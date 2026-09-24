#include "users_history.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/object.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/tag.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/visitor.hpp>

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/compute/api.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "h3_utils.hpp"
#include "reputation.hpp"
#include "vandalism.hpp"

namespace users_history {

namespace {

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

// ---------------------------------------------------------------------------
// Stage I/O
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> stage_schema() {
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("change_date", arrow::uint16(), false),
    };
    for (const auto& c : kCounters) {
        fields.push_back(arrow::field(c.name, arrow::uint32(), false));
    }
    return arrow::schema(fields);
}

void write_stage_file(const std::string& path,
                      const std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash>& rows) {
    if (rows.empty()) return;

    const int64_t n = static_cast<int64_t>(rows.size());
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder username_builder;
    arrow::UInt16Builder date_builder;
    std::array<arrow::UInt32Builder, kCounterCount> counter_builders;

    if (!uid_builder.Reserve(n).ok() || !username_builder.Reserve(n).ok() ||
        !date_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while flushing users-history stage");
    }
    for (size_t i = 0; i < kCounterCount; ++i) {
        if (!counter_builders[i].Reserve(n).ok()) {
            throw std::runtime_error("Reserve() failed while flushing users-history stage");
        }
    }

    for (const auto& [key, entry] : rows) {
        append_checked(uid_builder, key.uid);
        append_checked(username_builder, entry.username);
        append_checked(date_builder, key.day);
        const DayRow& r = entry.row;
        for (size_t i = 0; i < kCounterCount; ++i) append_checked(counter_builders[i], r.*kCounters[i].member);
    }

    std::shared_ptr<arrow::Array> uid, username, date;
    std::array<std::shared_ptr<arrow::Array>, kCounterCount> counters;
    finish_checked(uid_builder, &uid);
    finish_checked(username_builder, &username);
    finish_checked(date_builder, &date);
    for (size_t i = 0; i < kCounterCount; ++i) finish_checked(counter_builders[i], &counters[i]);

    std::vector<std::shared_ptr<arrow::Array>> columns = {uid, username, date};
    columns.insert(columns.end(), counters.begin(), counters.end());
    arrow_table_io::write_table(path, arrow::Table::Make(stage_schema(), columns));
}

// ---------------------------------------------------------------------------
// Streaming scan handler over the history file. Every version is forwarded
// to the (uid, day) aggregation in UserEventStats, which needs no object
// run state (the counters are per (uid, day), not per object). The handler
// only tracks the current object to count distinct objects for INSTR.
// ---------------------------------------------------------------------------

class ScanHandler : public osmium::handler::Handler {
public:
    explicit ScanHandler(const std::string& stage_dir) : stage_dir_(stage_dir) {}

    void node(const osmium::Node& node) {
        begin_object(ObjectKind::Node, node.id());
        add_version(static_cast<int64_t>(node.uid()), object_user(node),
                    version_day(node.timestamp()), node.visible(),
                    static_cast<uint32_t>(node.version()), ObjectKind::Node,
                    created_tag_bits(node));
    }

    void way(const osmium::Way& way) {
        begin_object(ObjectKind::Way, way.id());
        add_version(static_cast<int64_t>(way.uid()), object_user(way),
                    version_day(way.timestamp()), way.visible(),
                    static_cast<uint32_t>(way.version()), ObjectKind::Way,
                    created_tag_bits(way));
    }

    // Relations feed the same per-(uid, day) counters as nodes/ways, so
    // created/modified/deleted changes all contribute to a day's activity
    // total. Only the created counter and its tags carry reputation value.
    void relation(const osmium::Relation& relation) {
        add_version(static_cast<int64_t>(relation.uid()), object_user(relation),
                    version_day(relation.timestamp()), relation.visible(),
                    static_cast<uint32_t>(relation.version()), ObjectKind::Relation,
                    created_tag_bits(relation));
        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void finish() { flush_stage(); }

    // INSTR
    uint64_t objects() const { return objects_; }
    uint64_t versions() const { return versions_; }
    size_t stage_files() const { return stage_files_; }
    size_t stage_rows() const { return stage_rows_flushed_; }

private:
    // Tracks the current object so the distinct-object INSTR count is
    // incremented once per (kind, id) run (versions arrive contiguous per
    // object). May be called on every version; it is a no-op within a run.
    void begin_object(ObjectKind kind, int64_t id) {
        if (has_current_ && kind == kind_ && id == current_id_) return;
        has_current_ = true;
        kind_ = kind;
        current_id_ = id;
        objects_++;
    }

    static std::string object_user(const osmium::OSMObject& object) {
        const char* user = object.user();
        return std::string(user ? user : "");
    }

    // Bitmask of which Top12 tag keys an object carries; tag aspect is
    // counted on created objects only (paper sec. 4), so bare objects pass 0.
    static uint32_t created_tag_bits(const osmium::OSMObject& object) {
        return object.visible() && object.version() == 1 ? top12_tag_bits(object.tags()) : 0;
    }

    static uint32_t top12_tag_bits(const osmium::TagList& tags) {
        uint32_t bits = 0;
        for (const osmium::Tag& tag : tags) {
            for (size_t i = 0; i < kTop12TagKeys.size(); ++i) {
                if (tag.key() == kTop12TagKeys[i]) {
                    bits |= 1u << i;
                    break;
                }
            }
        }
        return bits;
    }

    static uint16_t version_day(const osmium::Timestamp& ts) {
        return h3_utils::require_u16_day(
            h3_utils::timestamp_to_utc_day(ts.seconds_since_epoch()));
    }

    void add_version(int64_t uid, const std::string& username, uint16_t day, bool visible,
                     uint32_t version, ObjectKind kind, uint32_t created_tag_bits = 0) {
        versions_++;
        stats_.add_version(uid, username, day, visible, version, kind, created_tag_bits);
        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void flush_stage() {
        if (stats_.days().empty()) return;
        char name[32];
        std::snprintf(name, sizeof(name), "stage_%05zu.parquet", stage_files_);
        write_stage_file(stage_dir_ + "/" + name, stats_.days());
        stage_rows_flushed_ += stats_.size();
        stats_.days().clear();
        stage_files_++;
    }

    std::string stage_dir_;
    UserEventStats stats_;
    bool has_current_ = false;
    ObjectKind kind_ = ObjectKind::Node;
    int64_t current_id_ = 0;

    // INSTR
    uint64_t objects_ = 0;
    uint64_t versions_ = 0;
    size_t stage_files_ = 0;
    size_t stage_rows_flushed_ = 0;
};

// ---------------------------------------------------------------------------
// Finalize helpers
// ---------------------------------------------------------------------------

// Live per-day output counters: the six node/way change counters plus the
// three relation counters (created, modified, deleted). The per-day tag_*
// counters are not consumed by the users viewer (the reputation's tag aspects
// come from the per-user totals below), so they are aggregated internally but
// never written to the history file.
constexpr size_t kLiveCounterCount = 9;

// Concatenates the (single-chunk) columns of one stage table into `columns`.
void append_stage_columns(const std::shared_ptr<arrow::Table>& table,
                          std::vector<std::vector<std::shared_ptr<arrow::Array>>>& columns) {
    auto combined_result = table->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("CombineChunks failed on stage table: " +
                                 combined_result.status().ToString());
    }
    const auto& combined = *combined_result;
    for (int f = 0; f < combined->num_columns(); ++f) {
        columns[f].push_back(combined->column(f)->chunk(0));
    }
}

// The single-chunk typed column of a (combined) table, by column name.
template <typename T>
const T* typed_column(const std::shared_ptr<arrow::Table>& table, const std::string& name) {
    const int idx = table->schema()->GetFieldIndex(name);
    if (idx < 0) {
        throw std::runtime_error("Stage table is missing the '" + name + "' column");
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

// Reputation aspect layout: the three created-counter sums followed by the
// Top12 tag sums (kCounterCount - kTagCount .. kCounterCount - 1), aligned
// with the per-uid totals vectors.
constexpr size_t kRepAspectCount = 3 + kTagCount;
constexpr size_t kFirstTag = kCounterCount - kTagCount;
constexpr auto counter_index = [](std::string_view name) -> size_t {
    for (size_t i = 0; i < kCounterCount; ++i) {
        if (kCounters[i].name == name) return i;
    }
    return kCounterCount;
};

// Writes user_reputation.parquet (next to `history_path`) from per-uid
// identity, first-seen day and the 21 counter totals, ranked exactly over the
// whole contributor population by reputation::compute. The dataset-wide
// active/max stats land in the Parquet footer key_value_metadata; sorting is
// by (username, uid) so an exact username filter prunes to matching pages.
// Returns the reputation::Result, reused by the update finalize for the
// filter-1 flag on newly-written rows so the ranking is computed exactly once
// per finalize.
reputation::Result build_reputation_table(
    const std::vector<int64_t>& rep_uids,
    const std::vector<std::string>& rep_usernames,
    const std::vector<uint16_t>& rep_first_seen,
    const std::array<std::vector<uint64_t>, kCounterCount>& counter_sums,
    const std::string& history_path, int64_t reputation_group_rows) {
    std::array<size_t, kRepAspectCount> rep_counter_idx = {
        counter_index("node_created"), counter_index("way_created"),
        counter_index("relation_created")};
    for (size_t i = 0; i < kTagCount; ++i) rep_counter_idx[3 + i] = kFirstTag + i;
    if (rep_counter_idx[0] >= kCounterCount || rep_counter_idx[1] >= kCounterCount ||
        rep_counter_idx[2] >= kCounterCount) {
        throw std::runtime_error("Counter schema changed: reputation aspects missing");
    }

    // Exact reputation: each aspect is ranked over the whole contributor
    // population (no sampling), computed in C++ so clients need no
    // distribution file or ranking math.
    std::array<std::vector<uint64_t>, kRepAspectCount> rep_totals;
    for (size_t a = 0; a < kRepAspectCount; ++a) {
        rep_totals[a].resize(rep_uids.size());
        for (size_t i = 0; i < rep_uids.size(); ++i) {
            rep_totals[a][i] = counter_sums[rep_counter_idx[a]][i];
        }
    }
    const reputation::Result rep = reputation::compute(rep_uids, rep_totals);

    std::array<std::string, kRepAspectCount> rep_aspect_names = {"node", "way",
                                                                  "relation"};
    for (size_t i = 0; i < kTagCount; ++i) {
        rep_aspect_names[3 + i] = "tag_" + std::string(kTop12TagKeys[i]);
    }

    // Wide one-row-per-uid table (see the header for the column layout).
    arrow::Int64Builder rep_uid_builder;
    arrow::StringBuilder rep_username_builder;
    arrow::UInt16Builder rep_first_seen_builder;
    arrow::UInt8Builder rep_score_builder;
    std::array<arrow::UInt32Builder, kCounterCount> rep_counter_builders;
    std::array<arrow::DoubleBuilder, kRepAspectCount> rep_pct_builders;
    for (size_t i = 0; i < rep_uids.size(); ++i) {
        append_checked(rep_uid_builder, rep_uids[i]);
        append_checked(rep_username_builder, rep_usernames[i]);
        append_checked(rep_first_seen_builder, rep_first_seen[i]);
        append_checked(rep_score_builder, rep.reputation[i]);
        for (size_t c = 0; c < kCounterCount; ++c) {
            append_checked(rep_counter_builders[c], counter_sums[c][i]);
        }
        for (size_t a = 0; a < kRepAspectCount; ++a) {
            append_checked(rep_pct_builders[a], rep.pct[a][i]);
        }
    }

    std::vector<std::shared_ptr<arrow::Field>> rep_fields = {
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("first_seen_day", arrow::uint16(), false),
        arrow::field("reputation", arrow::uint8(), false),
    };
    for (const auto& c : kCounters) {
        rep_fields.push_back(arrow::field(c.name, arrow::uint32(), false));
    }
    for (size_t a = 0; a < kRepAspectCount; ++a) {
        rep_fields.push_back(
            arrow::field(rep_aspect_names[a] + "_pct", arrow::float64(), false));
    }

    std::vector<std::shared_ptr<arrow::Array>> rep_columns;
    rep_columns.reserve(4 + kCounterCount + kRepAspectCount);
    std::shared_ptr<arrow::Array> rep_uid, rep_username_arr, rep_first_seen_arr, rep_score;
    finish_checked(rep_uid_builder, &rep_uid);
    finish_checked(rep_username_builder, &rep_username_arr);
    finish_checked(rep_first_seen_builder, &rep_first_seen_arr);
    finish_checked(rep_score_builder, &rep_score);
    rep_columns.push_back(rep_uid);
    rep_columns.push_back(rep_username_arr);
    rep_columns.push_back(rep_first_seen_arr);
    rep_columns.push_back(rep_score);
    for (size_t c = 0; c < kCounterCount; ++c) {
        std::shared_ptr<arrow::Array> arr;
        finish_checked(rep_counter_builders[c], &arr);
        rep_columns.push_back(arr);
    }
    for (size_t a = 0; a < kRepAspectCount; ++a) {
        std::shared_ptr<arrow::Array> arr;
        finish_checked(rep_pct_builders[a], &arr);
        rep_columns.push_back(arr);
    }
    // The dataset-wide active/max aspect stats are the same value for every
    // row, so they are attached once as file-level key_value_metadata rather
    // than written as 30 repeated columns.
    std::vector<std::string> rep_meta_keys;
    std::vector<std::string> rep_meta_values;
    rep_meta_keys.reserve(2 * kRepAspectCount);
    rep_meta_values.reserve(2 * kRepAspectCount);
    for (size_t a = 0; a < kRepAspectCount; ++a) {
        rep_meta_keys.push_back(rep_aspect_names[a] + "_active");
        rep_meta_values.push_back(std::to_string(rep.active[a]));
        rep_meta_keys.push_back(rep_aspect_names[a] + "_max");
        rep_meta_values.push_back(std::to_string(rep.max[a]));
    }
    auto rep_meta = arrow::KeyValueMetadata::Make(rep_meta_keys, rep_meta_values);
    auto rep_table = arrow::Table::Make(arrow::schema(rep_fields), rep_columns);
    rep_table = arrow_table_io::sort_by_keys(
        rep_table, {arrow::compute::SortKey("username"), arrow::compute::SortKey("uid")});

    const std::filesystem::path history_parent =
        std::filesystem::path(history_path).parent_path();
    const std::string reputation_path =
        (history_parent / "user_reputation.parquet").string();
    const std::string reputation_tmp = reputation_path + ".tmp";
    // The viewer filters on username (exact); uid is kept too as the stable
    // identity key. Only those two columns keep row-group min/max statistics
    // in the footer.
    arrow_table_io::write_table(reputation_tmp, rep_table, rep_meta, reputation_group_rows,
                                {"username", "uid"});
    std::filesystem::rename(reputation_tmp, reputation_path);
    return rep;
}

}  // namespace

// ---------------------------------------------------------------------------
// Scan + finalize entry points
// ---------------------------------------------------------------------------

void run_scan(const std::string& input_path, const std::string& stage_dir) {
    std::filesystem::remove_all(stage_dir);
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(input_path);
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    ScanHandler handler(stage_dir);

    auto start = std::chrono::steady_clock::now();
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[users history] scan done in " << elapsed << "s\n";
    // INSTR
    std::cerr << "[users history] objects=" << handler.objects()
              << " versions=" << handler.versions()
              << " stage_rows=" << handler.stage_rows()
              << " stage_files=" << handler.stage_files() << "\n";
}

void run_finalize(const std::string& stage_dir, const std::string& history_path,
                  int64_t users_history_group_rows, int64_t reputation_group_rows) {
    // Registers Arrow's compute kernels (sort_indices, take), required even
    // when the finalize runs without any of passes 1-3 / a diff scan.
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize Arrow compute: " +
                                 init_status.ToString());
    }

    std::vector<std::string> stage_paths;
    if (std::filesystem::is_directory(stage_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(stage_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
                stage_paths.push_back(entry.path().string());
            }
        }
        std::sort(stage_paths.begin(), stage_paths.end());
    }
    if (stage_paths.empty()) {
        std::cerr << "[users history] no stage files under " << stage_dir
                  << ", nothing to finalize\n";
        return;
    }

    // Aggregate every stage file into one in-memory table (finalize performs
    // a full SortIndices on the combined rows).
    auto schema = stage_schema();
    std::vector<std::vector<std::shared_ptr<arrow::Array>>> column_lists(schema->num_fields());
    for (const std::string& path : stage_paths) {
        append_stage_columns(arrow_table_io::read_table(path), column_lists);
    }

    std::vector<std::shared_ptr<arrow::Array>> columns(schema->num_fields());
    for (size_t f = 0; f < column_lists.size(); ++f) {
        if (column_lists[f].empty()) {
            throw std::runtime_error("Stage column '" + schema->field(f)->name() +
                                     "' is missing; rerun import (or update) to "
                                     "regenerate the stage files");
        }
        if (column_lists[f].size() == 1) {
            columns[f] = column_lists[f][0];
        } else {
            auto concat_result =
                arrow::Concatenate(column_lists[f], arrow::default_memory_pool());
            if (!concat_result.ok()) {
                throw std::runtime_error("Failed to concatenate stage columns: " +
                                         concat_result.status().ToString());
            }
            columns[f] = *concat_result;
        }
    }
    auto combined = arrow_table_io::sort_by_keys(
        arrow::Table::Make(schema, columns),
        {arrow::compute::SortKey("uid"), arrow::compute::SortKey("change_date")});

    const auto* uid_array = static_cast<const arrow::Int64Array*>(combined->column(0)->chunk(0).get());
    const auto* user_array = static_cast<const arrow::StringArray*>(combined->column(1)->chunk(0).get());
    const auto* day_array = static_cast<const arrow::UInt16Array*>(combined->column(2)->chunk(0).get());

    std::array<std::shared_ptr<arrow::Array>, kCounterCount> counter_arrays;
    for (size_t i = 0; i < kCounterCount; ++i) {
        counter_arrays[i] = combined->column(3 + static_cast<int>(i))->chunk(0);
    }

    // Per-uid sums of all 21 history counters, in the (uid) order of the
    // sorted history table — an index-aligned walk over the combined rows that
    // feeds the reputation (and, in the update finalize, the filter-1 flag on
    // newly-written rows). The aspect mapping lives in
    // build_reputation_table; storing every counter total (not just the
    // reputation aspects) keeps the per-user totals complete in this file.
    std::vector<int64_t> rep_uids;
    std::vector<std::string> rep_usernames;
    std::vector<uint16_t> rep_first_seen;
    std::array<std::vector<uint64_t>, kCounterCount> counter_sums;
    {
        std::array<uint64_t, kCounterCount> acc{};
        int64_t cur = 0;
        bool in = false;
        uint16_t group_first_seen = 0;
        std::string group_username;
        const auto* uid_arr = static_cast<const arrow::Int64Array*>(uid_array);
        const auto flush = [&]() {
            if (!in) return;
            rep_uids.push_back(cur);
            if (group_username.empty()) group_username = "<" + std::to_string(cur) + ">";
            rep_usernames.push_back(group_username);
            rep_first_seen.push_back(group_first_seen);
            for (size_t c = 0; c < kCounterCount; ++c) {
                counter_sums[c].push_back(acc[c]);
            }
        };
        for (int64_t i = 0; i < uid_arr->length(); ++i) {
            const int64_t u = uid_arr->Value(i);
            const uint16_t day = day_array->Value(i);
            if (!in || u != cur) {
                if (in) flush();
                in = true;
                cur = u;
                acc.fill(0);
                group_first_seen = day;
                group_username.clear();
            }
            // Combined rows are index-aligned with the history rows, so the
            // row's username is the last (current) username seen for the uid.
            group_username = user_array->GetView(i);
            for (size_t c = 0; c < kCounterCount; ++c) {
                const auto* arr =
                    static_cast<const arrow::UInt32Array*>(counter_arrays[c].get());
                acc[c] += arr->Value(i);
            }
        }
        flush();
    }

    // Writes user_reputation.parquet for every contributor (identity, current
    // username, the exact per-aspect percentile scores).
    build_reputation_table(rep_uids, rep_usernames, rep_first_seen, counter_sums,
                           history_path, reputation_group_rows);

    // One derived pass over the (uid, change_date)-sorted rows computes the
    // history rows.
    arrow::Int64Builder ind_uid_builder;
    arrow::UInt16Builder ind_day_builder;
    arrow::UInt32Builder ind_count_builder;
    arrow::UInt8Builder ind_flag_builder;

    const int64_t n = combined->num_rows();
    for (int64_t i = 0; i < n; ++i) {
        const int64_t uid = uid_array->Value(i);

        const uint16_t day = day_array->Value(i);
        DayRow day_row;
        for (size_t c = 0; c < kCounterCount; ++c) {
            const auto* arr = static_cast<const arrow::UInt32Array*>(counter_arrays[c].get());
            day_row.*kCounters[c].member = arr->Value(i);
        }

        uint32_t count = 0;
        for (size_t c = 0; c < kLiveCounterCount; ++c) count += day_row.*kCounters[c].member;

        append_checked(ind_uid_builder, uid);
        append_checked(ind_day_builder, day);
        append_checked(ind_count_builder, count);
        // Import writes no vandalism flags: the object hours precede the
        // replication stream's minute buckets (filters 2/3), and filter 1 is
        // forward-only, marking rows written after a low reputation is
        // detected rather than retroactively over the whole history.
        append_checked(ind_flag_builder, 0);
    }

    std::shared_ptr<arrow::Array> ind_uid, ind_day, ind_count, ind_flag;
    finish_checked(ind_uid_builder, &ind_uid);
    finish_checked(ind_day_builder, &ind_day);
    finish_checked(ind_count_builder, &ind_count);
    finish_checked(ind_flag_builder, &ind_flag);

    std::vector<std::shared_ptr<arrow::Field>> history_fields = {
        arrow::field("uid", arrow::int64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("count", arrow::uint32(), false),
        arrow::field("vandalism_flag", arrow::uint8(), false),
    };
    std::vector<std::shared_ptr<arrow::Array>> history_columns = {
        ind_uid, ind_day, ind_count, ind_flag};
    auto history_schema = arrow::schema(history_fields);
    // The history rows were appended in (uid, change_date) order while
    // walking the sorted combined table, so no re-sort is needed.
    auto history_table = arrow::Table::Make(history_schema, history_columns);

    const std::string history_tmp = history_path + ".tmp";
    // The viewer filters on uid; only that column keeps row-group min/max
    // statistics in the footer.
    arrow_table_io::write_table(history_tmp, history_table, users_history_group_rows,
                                {"uid"});
    std::filesystem::rename(history_tmp, history_path);

    std::filesystem::remove_all(stage_dir);

    std::cerr << "[users history] finalized " << history_table->num_rows()
              << " history rows, " << rep_uids.size() << " reputation rows\n";
}

void run_scan_diff(const std::string& diff_path, const std::string& stage_dir) {
    std::filesystem::remove_all(stage_dir);  // a rerun never reuses stale stage files
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(diff_path);  // .osc.gz -> format+compression from extension
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    ScanHandler handler(stage_dir);
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();

    std::cerr << "[users history] diff scan objects=" << handler.objects()
              << " versions=" << handler.versions()
              << " stage_rows=" << handler.stage_rows()
              << " stage_files=" << handler.stage_files() << "\n";
}

void run_update_finalize(const std::string& stage_root, const std::string& history_path,
                         int64_t users_history_group_rows, int64_t reputation_group_rows,
                         const std::string& minutes_path,
                         const std::map<std::pair<int64_t, uint16_t>, uint8_t>& move_flags) {
    // Registers Arrow's compute kernels (sort_indices, take).
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize Arrow compute: " +
                                 init_status.ToString());
    }

    std::vector<std::string> stage_paths = collect_parquet_recursive(stage_root);
    if (stage_paths.empty()) {
        std::cerr << "[users history] no update stage files under " << stage_root
                  << ", nothing to finalize\n";
        std::filesystem::remove_all(stage_root);
        return;
    }

    // Every diff stage file (stage_root/seq_<n>/stage_*.parquet) carries the
    // full stage schema; aggregate them into one in-memory table.
    auto schema = stage_schema();
    std::vector<std::vector<std::shared_ptr<arrow::Array>>> column_lists(schema->num_fields());
    for (const std::string& path : stage_paths) {
        append_stage_columns(arrow_table_io::read_table(path), column_lists);
    }
    std::vector<std::shared_ptr<arrow::Array>> columns(schema->num_fields());
    for (size_t f = 0; f < column_lists.size(); ++f) {
        if (column_lists[f].empty()) {
            throw std::runtime_error("Update stage column '" + schema->field(f)->name() +
                                     "' is missing");
        }
        if (column_lists[f].size() == 1) {
            columns[f] = column_lists[f][0];
        } else {
            auto concat_result =
                arrow::Concatenate(column_lists[f], arrow::default_memory_pool());
            if (!concat_result.ok()) {
                throw std::runtime_error("Failed to concatenate update stage columns: " +
                                         concat_result.status().ToString());
            }
            columns[f] = *concat_result;
        }
    }
    std::shared_ptr<arrow::Table> combined = arrow::Table::Make(schema, columns);

    const auto* uids = typed_column<arrow::Int64Array>(combined, "uid");
    const auto* users = typed_column<arrow::StringArray>(combined, "username");
    const auto* days = typed_column<arrow::UInt16Array>(combined, "change_date");
    std::array<const arrow::UInt32Array*, kCounterCount> counters;
    for (size_t c = 0; c < kCounterCount; ++c) {
        counters[c] = typed_column<arrow::UInt32Array>(combined, kCounters[c].name);
    }

    // Delta per-(uid, day) live activity, per-uid counter sums and per-uid
    // identity, accumulated once over all the run's diffs.
    std::map<std::pair<int64_t, uint16_t>, uint32_t> delta_counts;
    std::unordered_map<int64_t, std::array<uint64_t, kCounterCount>> delta_totals;
    std::unordered_map<int64_t, std::string> delta_username;
    std::unordered_map<int64_t, uint16_t> delta_first_seen;
    const int64_t n = combined->num_rows();
    for (int64_t i = 0; i < n; ++i) {
        const int64_t uid = uids->Value(i);
        const uint16_t day = days->Value(i);
        uint32_t live = 0;
        for (size_t c = 0; c < kLiveCounterCount; ++c) live += counters[c]->Value(i);
        delta_counts[{uid, day}] += live;
        std::array<uint64_t, kCounterCount>& totals = delta_totals[uid];
        for (size_t c = 0; c < kCounterCount; ++c) totals[c] += counters[c]->Value(i);
        delta_username[uid] = users->GetString(i);
        auto it = delta_first_seen.find(uid);
        if (it == delta_first_seen.end() || day < it->second) delta_first_seen[uid] = day;
    }

    // Reputation: the existing per-uid totals (whose new first-seen day and
    // username, if touched by the diffs, are merged in) plus the diff totals,
    // then ranked exactly over the whole population again. The ranking is
    // computed here, before the history write, because it also yields the
    // filter-1 flag set that this run's newly-written rows are screened with.
    const std::filesystem::path history_parent =
        std::filesystem::path(history_path).parent_path();
    const std::string reputation_path =
        (history_parent / "user_reputation.parquet").string();
    std::vector<int64_t> rep_uids;
    std::vector<std::string> rep_usernames;
    std::vector<uint16_t> rep_first_seen;
    std::array<std::vector<uint64_t>, kCounterCount> counter_sums;
    std::unordered_map<int64_t, size_t> uid_index;
    bool have_base_rep = false;
    if (std::filesystem::exists(reputation_path)) {
        auto base_result = arrow_table_io::read_table(reputation_path)->CombineChunks();
        if (!base_result.ok()) {
            throw std::runtime_error("CombineChunks failed on " + reputation_path + ": " +
                                     base_result.status().ToString());
        }
        const std::shared_ptr<arrow::Table> base_rep = *base_result;
        const auto* r_uids = typed_column<arrow::Int64Array>(base_rep, "uid");
        const auto* r_users = typed_column<arrow::StringArray>(base_rep, "username");
        const auto* r_seen = typed_column<arrow::UInt16Array>(base_rep, "first_seen_day");
        for (int64_t i = 0; i < base_rep->num_rows(); ++i) {
            const int64_t uid = r_uids->Value(i);
            uid_index[uid] = rep_uids.size();
            rep_uids.push_back(uid);
            rep_usernames.push_back(r_users->GetString(i));
            rep_first_seen.push_back(r_seen->Value(i));
        }
        for (size_t c = 0; c < kCounterCount; ++c) {
            const auto* arr = typed_column<arrow::UInt32Array>(base_rep, kCounters[c].name);
            counter_sums[c].resize(rep_uids.size());
            for (size_t i = 0; i < rep_uids.size(); ++i) counter_sums[c][i] = arr->Value(i);
        }
        have_base_rep = true;
    }
    // Filter 1 (paper sec. 5): users whose current reputation is below
    // kFilter1ReputationThreshold. Unlike the diff-based screens this is a
    // forward-only flag: it is applied to the run's newly-written rows and
    // never re-derived over the base history. A contributor who created
    // nothing scores reputation 0, which covers the paper's "new users" half.
    std::unordered_map<int64_t, uint8_t> filter1_uid;
    if (have_base_rep || !delta_totals.empty()) {
        for (const auto& [uid, sum] : delta_totals) {
            auto it = uid_index.find(uid);
            if (it != uid_index.end()) {
                const size_t idx = it->second;
                for (size_t c = 0; c < kCounterCount; ++c) counter_sums[c][idx] += sum[c];
                const auto us = delta_username.find(uid);
                if (us != delta_username.end() && !us->second.empty()) rep_usernames[idx] = us->second;
                rep_first_seen[idx] = std::min(rep_first_seen[idx], delta_first_seen[uid]);
            } else {
                uid_index[uid] = rep_uids.size();
                rep_uids.push_back(uid);
                std::string name = delta_username[uid];
                if (name.empty()) name = "<" + std::to_string(uid) + ">";
                rep_usernames.push_back(std::move(name));
                rep_first_seen.push_back(delta_first_seen[uid]);
                for (size_t c = 0; c < kCounterCount; ++c) counter_sums[c].push_back(sum[c]);
            }
        }
        const reputation::Result rep = build_reputation_table(
            rep_uids, rep_usernames, rep_first_seen, counter_sums, history_path,
            reputation_group_rows);
        for (size_t i = 0; i < rep_uids.size() && i < rep.reputation.size(); ++i) {
            if (rep.reputation[i] < vandalism::kFilter1ReputationThreshold) {
                filter1_uid[rep_uids[i]] = vandalism::kFlagFilter1;
            }
        }
    }

    // Users history: base file plus deltas, summed per (uid, change_date)
    // and written sorted, once. All three filter bits are monotonic: the base
    // row's flags are carried forward unchanged, bits 0/1 are ORed with this
    // run's minute-store and move-flagged days, and bit 2 (filter 1) is set on
    // the run's newly-written rows whose user's current reputation is below
    // the threshold. Base rows are never masked or re-flagged, so a flag once
    // written persists and a reputation drop is never applied retroactively.
    std::map<std::pair<int64_t, uint16_t>, uint32_t> merged_counts;
    std::map<std::pair<int64_t, uint16_t>, uint8_t> merged_flags;
    std::set<std::pair<int64_t, uint16_t>> base_keys;
    if (std::filesystem::exists(history_path)) {
        auto base_result = arrow_table_io::read_table(history_path)->CombineChunks();
        if (!base_result.ok()) {
            throw std::runtime_error("CombineChunks failed on " + history_path + ": " +
                                     base_result.status().ToString());
        }
        const std::shared_ptr<arrow::Table> base_table = *base_result;
        const auto* b_uids = typed_column<arrow::Int64Array>(base_table, "uid");
        const auto* b_days = typed_column<arrow::UInt16Array>(base_table, "change_date");
        const auto* b_counts = typed_column<arrow::UInt32Array>(base_table, "count");
        const auto* b_flags = typed_column<arrow::UInt8Array>(base_table, "vandalism_flag");
        for (int64_t i = 0; i < base_table->num_rows(); ++i) {
            const auto key = std::make_pair(b_uids->Value(i), b_days->Value(i));
            merged_counts[key] += b_counts->Value(i);
            merged_flags[key] |= b_flags->Value(i);
            base_keys.insert(key);
        }
    }
    for (const auto& [key, count] : delta_counts) merged_counts[key] += count;

    // Rebuild the whole-history flags from the persisted sources: bit 0 from
    // the minute store, bit 1 from the run's folded move-flagged days. Every
    // flagged_days value already carries kFlagFilter2 and every move_flags
    // entry kFlagFilter3, so ORing them into the carried-forward base bits
    // yields the combined per-day field.
    for (const auto& [key, value] : vandalism::flagged_days(minutes_path)) {
        merged_flags[key] |= value;
    }
    for (const auto& [key, value] : move_flags) {
        merged_flags[key] |= value;
    }

    arrow::Int64Builder ind_uid_builder;
    arrow::UInt16Builder ind_day_builder;
    arrow::UInt32Builder ind_count_builder;
    arrow::UInt8Builder ind_flag_builder;
    // Vandalism export: the same merged flags, one (uid, change_date) row per
    // day carrying any bit, with the current username resolved inline.
    arrow::Int64Builder v_uid_builder;
    arrow::StringBuilder v_user_builder;
    arrow::UInt16Builder v_day_builder;
    arrow::UInt8Builder v_flag_builder;
    for (const auto& [key, count] : merged_counts) {
        append_checked(ind_uid_builder, key.first);
        append_checked(ind_day_builder, key.second);
        append_checked(ind_count_builder, count);
        // Forward-only filter 1: only the rows this run newly writes may pick
        // up the low-reputation bit; a day already present in the base file
        // keeps its carried flags untouched.
        const auto it = base_keys.count(key) ? filter1_uid.end() : filter1_uid.find(key.first);
        const uint8_t flag =
            merged_flags[key] | (it != filter1_uid.end() ? it->second : 0);
        append_checked(ind_flag_builder, flag);
        if (flag != 0) {
            append_checked(v_uid_builder, key.first);
            append_checked(v_day_builder, key.second);
            append_checked(v_flag_builder, flag);
            const auto ui = uid_index.find(key.first);
            append_checked(v_user_builder,
                           ui != uid_index.end()
                               ? rep_usernames[ui->second]
                               : "<" + std::to_string(key.first) + ">");
        }
    }
    std::shared_ptr<arrow::Array> ind_uid, ind_day, ind_count, ind_flag;
    finish_checked(ind_uid_builder, &ind_uid);
    finish_checked(ind_day_builder, &ind_day);
    finish_checked(ind_count_builder, &ind_count);
    finish_checked(ind_flag_builder, &ind_flag);
    // The merged map iterates in (uid, change_date) order, so no re-sort.
    auto history_table = arrow::Table::Make(
        arrow::schema({arrow::field("uid", arrow::int64(), false),
                       arrow::field("change_date", arrow::uint16(), false),
                       arrow::field("count", arrow::uint32(), false),
                       arrow::field("vandalism_flag", arrow::uint8(), false)}),
        {ind_uid, ind_day, ind_count, ind_flag});

    const std::string history_tmp = history_path + ".tmp";
    // The viewer filters on uid; only that column keeps row-group min/max
    // statistics in the footer.
    arrow_table_io::write_table(history_tmp, history_table, users_history_group_rows,
                                {"uid"});
    std::filesystem::rename(history_tmp, history_path);

    // The vandalism export: every (uid, change_date) carrying any flag bit,
    // with the current username, sorted by (change_date, uid). The flag set
    // is exactly the users-history flags (the merged state above), so the
    // two outputs always agree. The file is update-only: import writes no
    // flags and never produces it; an update with no flagged day writes an
    // empty file.
    std::shared_ptr<arrow::Array> v_uid, v_user, v_day, v_flag;
    finish_checked(v_uid_builder, &v_uid);
    finish_checked(v_user_builder, &v_user);
    finish_checked(v_day_builder, &v_day);
    finish_checked(v_flag_builder, &v_flag);
    auto vandalism_table = arrow::Table::Make(
        arrow::schema({arrow::field("uid", arrow::int64(), false),
                       arrow::field("username", arrow::utf8(), false),
                       arrow::field("change_date", arrow::uint16(), false),
                       arrow::field("vandalism_flag", arrow::uint8(), false)}),
        {v_uid, v_user, v_day, v_flag});
    // Day-first order makes change_date (the pruning column) the compact
    // footer statistics key.
    vandalism_table = arrow_table_io::sort_by_keys(
        vandalism_table,
        {arrow::compute::SortKey("change_date"), arrow::compute::SortKey("uid")});
    const std::string vandalism_path = (history_parent / "vandalism.parquet").string();
    const std::string vandalism_tmp = vandalism_path + ".tmp";
    arrow_table_io::write_table(vandalism_tmp, vandalism_table, reputation_group_rows,
                                {"change_date"});
    std::filesystem::rename(vandalism_tmp, vandalism_path);

    std::filesystem::remove_all(stage_root);

    std::cerr << "[users history] update finalized " << history_table->num_rows()
              << " history rows, " << rep_uids.size() << " reputation rows, "
              << vandalism_table->num_rows() << " vandalism rows\n";
}

}  // namespace users_history
