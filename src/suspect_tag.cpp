#include "suspect.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/object.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/tag.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/visitor.hpp>

#include <arrow/api.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "h3_utils.hpp"
#include "suspect_tag_store.hpp"

namespace suspect {

namespace {

constexpr size_t kFlushThreshold = 1'000'000;  // pending rows per stage flush

// Builder Appends only fail on allocation; throw instead of ignoring the
// [[nodiscard]] Status.
template <typename B, typename V>
void append_checked(B& builder, V&& value) {
    if (!builder.Append(std::forward<V>(value)).ok()) {
        throw std::runtime_error("Failed to append value to Arrow builder");
    }
}

template <typename B>
void finish_checked(B& builder, std::shared_ptr<arrow::Array>* out) {
    if (!builder.Finish(out).ok()) {
        throw std::runtime_error("Failed to finalize Arrow builder");
    }
}

std::shared_ptr<arrow::Table> combine_chunks(const std::shared_ptr<arrow::Table>& table) {
    auto result = table->CombineChunks();
    if (!result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + result.status().ToString());
    }
    return *result;
}

// The single-chunk typed column of a combined table, by column name.
template <typename T>
const T* typed_column(const std::shared_ptr<arrow::Table>& table, const std::string& name) {
    const int idx = table->schema()->GetFieldIndex(name);
    if (idx < 0) {
        throw std::runtime_error("Table is missing the '" + name + "' column");
    }
    return static_cast<const T*>(table->column(idx)->chunk(0).get());
}

// Every .parquet file under `root` (recursively, forming per-diff stage
// groups), sorted for deterministic order.
std::vector<std::string> collect_parquet_recursive(const std::string& root) {
    std::vector<std::string> paths;
    if (!std::filesystem::is_directory(root)) return paths;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
            paths.push_back(entry.path().string());
        }
    }
    std::sort(paths.begin(), paths.end());
    return paths;
}

// The replication sequence of a counts stage file, taken from its seq_<n>
// parent directory (".../counts/seq_<n>/stage_*.parquet"); 0 when the path
// does not carry one.
uint64_t staged_seq(const std::string& path) {
    const std::string dir = std::filesystem::path(path).parent_path().filename().string();
    constexpr std::string_view kPrefix = "seq_";
    if (dir.compare(0, kPrefix.size(), kPrefix) != 0) return 0;
    try {
        return std::stoull(dir.substr(kPrefix.size()));
    } catch (const std::exception&) {
        return 0;
    }
}

// ---------------------------------------------------------------------------
// Filter 5 stage I/O (per-(uid, minute, tag_key) modified+deleted counts)
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> tags_stage_schema() {
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("minute", arrow::uint32(), false),
        arrow::field("tag_key", arrow::utf8(), false),
        arrow::field("count", arrow::uint32(), false),
    });
}

void write_tags_stage_file(
    const std::string& path,
    const std::unordered_map<UserMinuteTagKey, UserMinuteTagEntry, UserMinuteTagKeyHash>& rows) {
    if (rows.empty()) return;

    const int64_t n = static_cast<int64_t>(rows.size());
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder username_builder;
    arrow::UInt32Builder minute_builder;
    arrow::StringBuilder tag_key_builder;
    arrow::UInt32Builder count_builder;

    if (!uid_builder.Reserve(n).ok() || !username_builder.Reserve(n).ok() ||
        !minute_builder.Reserve(n).ok() || !tag_key_builder.Reserve(n).ok() ||
        !count_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while flushing suspect tag stage");
    }
    for (const auto& [key, entry] : rows) {
        append_checked(uid_builder, key.uid);
        append_checked(username_builder, entry.username);
        append_checked(minute_builder, key.minute);
        append_checked(tag_key_builder, key.tag_key);
        append_checked(count_builder, entry.count);
    }

    std::shared_ptr<arrow::Array> uid, username, minute, tag_key, count;
    finish_checked(uid_builder, &uid);
    finish_checked(username_builder, &username);
    finish_checked(minute_builder, &minute);
    finish_checked(tag_key_builder, &tag_key);
    finish_checked(count_builder, &count);

    arrow_table_io::write_table(path,
                                arrow::Table::Make(tags_stage_schema(),
                                                   {uid, username, minute, tag_key, count}));
}

}  // namespace

// ---------------------------------------------------------------------------
// MinuteTagStats: the pure per-(uid, minute, tag_key) counter
// ---------------------------------------------------------------------------

void MinuteTagStats::add_object(int64_t uid, std::string_view username, uint32_t minute,
                                const std::vector<std::string>& tag_keys,
                                bool visible, uint32_t version) {
    // Modified+deleted only (creates excluded), matching filter 2's scope
    if (visible && version == 1) return;
    // Deduplicate tag keys per object (defensive: OSM forbids duplicates, but be safe)
    std::vector<std::string> uniq = tag_keys;
    std::sort(uniq.begin(), uniq.end());
    uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());
    for (const std::string& tag_key : uniq) {
        UserMinuteTagEntry& e = tags_[UserMinuteTagKey{uid, minute, tag_key}];
        if (e.username.empty()) e.username = std::string(username);
        e.count++;
    }
}

const std::unordered_map<UserMinuteTagKey, UserMinuteTagEntry, UserMinuteTagKeyHash>& MinuteTagStats::tags() const {
    return tags_;
}

void MinuteTagStats::clear() { tags_.clear(); }

size_t MinuteTagStats::size() const { return tags_.size(); }

// ---------------------------------------------------------------------------
// Scan handler
// ---------------------------------------------------------------------------

namespace {

class SuspectTagScanHandler : public osmium::handler::Handler {
public:
    explicit SuspectTagScanHandler(std::string stage_dir)
        : stage_dir_(std::move(stage_dir)) {}

    void node(const osmium::Node& node) {
        add_object(static_cast<int64_t>(node.uid()), object_user(node),
                   node.timestamp(), node.visible(), static_cast<uint32_t>(node.version()),
                   node.tags());
    }

    void way(const osmium::Way& way) {
        add_object(static_cast<int64_t>(way.uid()), object_user(way),
                   way.timestamp(), way.visible(), static_cast<uint32_t>(way.version()),
                   way.tags());
    }

    void relation(const osmium::Relation& relation) {
        add_object(static_cast<int64_t>(relation.uid()), object_user(relation),
                   relation.timestamp(), relation.visible(), static_cast<uint32_t>(relation.version()),
                   relation.tags());
    }

    void finish() { flush_stage(); }

    // INSTR
    uint64_t objects() const { return objects_; }
    size_t stage_files() const { return stage_files_; }
    size_t stage_rows() const { return stage_rows_flushed_; }

private:
    static std::string object_user(const osmium::OSMObject& object) {
        const char* user = object.user();
        return std::string(user ? user : "");
    }

    // Extract all tag keys from an osmium TagList
    static std::vector<std::string> extract_tag_keys(const osmium::TagList& tags) {
        std::vector<std::string> keys;
        keys.reserve(tags.size());
        for (const osmium::Tag& tag : tags) {
            keys.push_back(tag.key());
        }
        return keys;
    }

    void add_object(int64_t uid, const std::string& username, const osmium::Timestamp& ts,
                    bool visible, uint32_t version, const osmium::TagList& tags) {
        objects_++;
        std::vector<std::string> tag_keys = extract_tag_keys(tags);
        if (tag_keys.empty()) return;
        stats_.add_object(uid, username, h3_utils::timestamp_to_utc_minute(ts.seconds_since_epoch()),
                          tag_keys, visible, version);
        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void flush_stage() {
        if (stats_.tags().empty()) return;
        char name[32];
        std::snprintf(name, sizeof(name), "stage_%05zu.parquet", stage_files_);
        write_tags_stage_file(stage_dir_ + "/" + name, stats_.tags());
        stage_rows_flushed_ += stats_.size();
        stats_.clear();
        stage_files_++;
    }

    std::string stage_dir_;
    MinuteTagStats stats_;

    // INSTR
    uint64_t objects_ = 0;
    size_t stage_files_ = 0;
    size_t stage_rows_flushed_ = 0;
};

}  // namespace

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

void run_scan_diff_tags(const std::string& diff_path, const std::string& stage_dir) {
    std::filesystem::remove_all(stage_dir);  // a rerun never reuses stale stage files
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(diff_path);  // .osc.gz -> format+compression from extension
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    SuspectTagScanHandler handler(stage_dir);
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();

    std::cerr << "[suspect] tag diff scan objects=" << handler.objects()
              << " stage_rows=" << handler.stage_rows()
              << " stage_files=" << handler.stage_files() << "\n";
}

void fold_tag_counts(const std::string& tags_root, const std::string& tags_path,
                     uint64_t applied_seq) {
    const std::vector<std::string> tag_stage = collect_parquet_recursive(tags_root);
    const bool have_base = std::filesystem::exists(tags_path);

    // A base store already stamped with a sequence >= the run's means a
    // previous run folded the same diffs; skip instead of double-counting.
    if (have_base && applied_seq > 0 &&
        suspect_tag_store::applied_seq_of(tags_path) >= applied_seq) {
        std::cerr << "[suspect] tag buckets already folded through " << applied_seq
                  << ", skipping\n";
        std::filesystem::remove_all(tags_root);
        return;
    }
    if (tag_stage.empty()) {
        std::cerr << "[suspect] no tag bucket stage files under " << tags_root
                  << ", nothing to fold\n";
        std::filesystem::remove_all(tags_root);
        return;
    }

    const uint64_t base_seq =
        have_base ? suspect_tag_store::applied_seq_of(tags_path) : 0;

    // Merge base + every newly staged diff, summing equal (uid, minute, tag_key) keys.
    // The map hands the writer its guaranteed (uid, minute, tag_key) ascending order.
    std::map<std::tuple<int64_t, uint32_t, std::string>, uint32_t> merged;
    if (have_base) {
        suspect_tag_store::Reader base(tags_path);
        for (size_t i = 0; i < base.size(); ++i) {
            merged[{base.uid_at(i), base.minute_at(i), base.tag_key_at(i)}] += base.count_at(i);
        }
    }
    for (const std::string& path : tag_stage) {
        if (base_seq > 0 && staged_seq(path) <= base_seq) {
            std::cerr << "[suspect] tag stage " << path
                      << " already folded (seq <= " << base_seq << "), skipping\n";
            continue;
        }
        const std::shared_ptr<arrow::Table> table = combine_chunks(
            arrow_table_io::read_table(path));
        const auto* uids = typed_column<arrow::Int64Array>(table, "uid");
        const auto* minutes = typed_column<arrow::UInt32Array>(table, "minute");
        const auto* tag_keys = typed_column<arrow::StringArray>(table, "tag_key");
        const auto* counts = typed_column<arrow::UInt32Array>(table, "count");
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            merged[{uids->Value(i), minutes->Value(i), tag_keys->GetString(i)}] += counts->Value(i);
        }
    }

    suspect_tag_store::Writer writer(tags_path);
    writer.set_applied_seq(applied_seq);
    for (const auto& [key, count] : merged) {
        writer.add(std::get<0>(key), std::get<1>(key), std::get<2>(key), count);
    }
    writer.finish();
    std::cerr << "[suspect] folded " << merged.size() << " tag buckets into "
              << tags_path << "\n";
    std::filesystem::remove_all(tags_root);
}

}  // namespace suspect