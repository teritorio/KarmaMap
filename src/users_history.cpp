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
#include "ranking.hpp"
#include "suspect.hpp"

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
    // total. Only the created counter and its tags carry ranking points.
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
// counters are not consumed by the users viewer (the ranking's tag aspects
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
// `what` names the file for the error, so a base file with a foreign schema
// does not read as a stage file.
template <typename T>
const T* typed_column(const std::shared_ptr<arrow::Table>& table, const std::string& name,
                      const std::string& what = "stage table") {
    const int idx = table->schema()->GetFieldIndex(name);
    if (idx < 0) {
        throw std::runtime_error(what + " is missing the '" + name + "' column");
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

// Ranking aspect layout: the three created-counter sums followed by the
// Top12 tag sums (kCounterCount - kTagCount .. kCounterCount - 1), aligned
// with the per-uid totals vectors.
constexpr size_t kRankAspectCount = 3 + kTagCount;
constexpr size_t kFirstTag = kCounterCount - kTagCount;
constexpr auto counter_index = [](std::string_view name) -> size_t {
    for (size_t i = 0; i < kCounterCount; ++i) {
        if (kCounters[i].name == name) return i;
    }
    return kCounterCount;
};

// Writes user_ranking.parquet (next to `history_path`) from per-uid
// identity, first-seen day and the 21 counter totals, ranked exactly over the
// whole contributor population by ranking::compute. The dataset-wide
// active/max stats land in the Parquet footer key_value_metadata; sorting is
// by (username, uid) so an exact username filter prunes to matching pages.
// Returns the ranking::Result, reused by the update finalize for the
// filter-1 flag on newly-written rows so the ranking is computed exactly once
// per finalize.
ranking::Result build_ranking_table(
    const std::vector<int64_t>& rank_uids,
    const std::vector<std::string>& rank_usernames,
    const std::vector<uint16_t>& rank_first_seen,
    const std::array<std::vector<uint64_t>, kCounterCount>& counter_sums,
    const std::string& history_path, int64_t ranking_group_rows) {
    std::array<size_t, kRankAspectCount> rank_counter_idx = {
        counter_index("node_created"), counter_index("way_created"),
        counter_index("relation_created")};
    for (size_t i = 0; i < kTagCount; ++i) rank_counter_idx[3 + i] = kFirstTag + i;
    if (rank_counter_idx[0] >= kCounterCount || rank_counter_idx[1] >= kCounterCount ||
        rank_counter_idx[2] >= kCounterCount) {
        throw std::runtime_error("Counter schema changed: ranking aspects missing");
    }

    // Exact ranking: each aspect is ranked over the whole contributor
    // population (no sampling), computed in C++ so clients need no
    // distribution file or ranking math.
    std::array<std::vector<uint64_t>, kRankAspectCount> rank_totals;
    for (size_t a = 0; a < kRankAspectCount; ++a) {
        rank_totals[a].resize(rank_uids.size());
        for (size_t i = 0; i < rank_uids.size(); ++i) {
            rank_totals[a][i] = counter_sums[rank_counter_idx[a]][i];
        }
    }
    const ranking::Result rank = ranking::compute(rank_uids, rank_totals);

    std::array<std::string, kRankAspectCount> rank_aspect_names = {"node", "way",
                                                                    "relation"};
    for (size_t i = 0; i < kTagCount; ++i) {
        rank_aspect_names[3 + i] = "tag_" + std::string(kTop12TagKeys[i]);
    }

    // Wide one-row-per-uid table (see the header for the column layout).
    arrow::Int64Builder rank_uid_builder;
    arrow::StringBuilder rank_username_builder;
    arrow::UInt16Builder rank_first_seen_builder;
    arrow::UInt8Builder rank_score_builder;
    std::array<arrow::UInt32Builder, kCounterCount> rank_counter_builders;
    std::array<arrow::DoubleBuilder, kRankAspectCount> rank_pct_builders;
    for (size_t i = 0; i < rank_uids.size(); ++i) {
        append_checked(rank_uid_builder, rank_uids[i]);
        append_checked(rank_username_builder, rank_usernames[i]);
        append_checked(rank_first_seen_builder, rank_first_seen[i]);
        append_checked(rank_score_builder, rank.ranking[i]);
        for (size_t c = 0; c < kCounterCount; ++c) {
            append_checked(rank_counter_builders[c], counter_sums[c][i]);
        }
        for (size_t a = 0; a < kRankAspectCount; ++a) {
            append_checked(rank_pct_builders[a], rank.pct[a][i]);
        }
    }

    std::vector<std::shared_ptr<arrow::Field>> rank_fields = {
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("first_seen_day", arrow::uint16(), false),
        arrow::field("ranking", arrow::uint8(), false),
    };
    for (const auto& c : kCounters) {
        rank_fields.push_back(arrow::field(c.name, arrow::uint32(), false));
    }
    for (size_t a = 0; a < kRankAspectCount; ++a) {
        rank_fields.push_back(
            arrow::field(rank_aspect_names[a] + "_pct", arrow::float64(), false));
    }

    std::vector<std::shared_ptr<arrow::Array>> rank_columns;
    rank_columns.reserve(4 + kCounterCount + kRankAspectCount);
    std::shared_ptr<arrow::Array> rank_uid, rank_username_arr, rank_first_seen_arr, rank_score;
    finish_checked(rank_uid_builder, &rank_uid);
    finish_checked(rank_username_builder, &rank_username_arr);
    finish_checked(rank_first_seen_builder, &rank_first_seen_arr);
    finish_checked(rank_score_builder, &rank_score);
    rank_columns.push_back(rank_uid);
    rank_columns.push_back(rank_username_arr);
    rank_columns.push_back(rank_first_seen_arr);
    rank_columns.push_back(rank_score);
    for (size_t c = 0; c < kCounterCount; ++c) {
        std::shared_ptr<arrow::Array> arr;
        finish_checked(rank_counter_builders[c], &arr);
        rank_columns.push_back(arr);
    }
    for (size_t a = 0; a < kRankAspectCount; ++a) {
        std::shared_ptr<arrow::Array> arr;
        finish_checked(rank_pct_builders[a], &arr);
        rank_columns.push_back(arr);
    }
    // The dataset-wide active/max aspect stats are the same value for every
    // row, so they are attached once as file-level key_value_metadata rather
    // than written as 30 repeated columns.
    std::vector<std::string> rank_meta_keys;
    std::vector<std::string> rank_meta_values;
    rank_meta_keys.reserve(2 * kRankAspectCount);
    rank_meta_values.reserve(2 * kRankAspectCount);
    for (size_t a = 0; a < kRankAspectCount; ++a) {
        rank_meta_keys.push_back(rank_aspect_names[a] + "_active");
        rank_meta_values.push_back(std::to_string(rank.active[a]));
        rank_meta_keys.push_back(rank_aspect_names[a] + "_max");
        rank_meta_values.push_back(std::to_string(rank.max[a]));
    }
    auto rank_meta = arrow::KeyValueMetadata::Make(rank_meta_keys, rank_meta_values);
    auto rank_table = arrow::Table::Make(arrow::schema(rank_fields), rank_columns);
    rank_table = arrow_table_io::sort_by_keys(
        rank_table, {arrow::compute::SortKey("username"), arrow::compute::SortKey("uid")});

    const std::filesystem::path history_parent =
        std::filesystem::path(history_path).parent_path();
    const std::string ranking_path = (history_parent / "user_ranking.parquet").string();
    const std::string ranking_tmp = ranking_path + ".tmp";
    // The viewer filters on username (exact); uid is kept too as the stable
    // identity key. Only those two columns keep row-group min/max statistics
    // in the footer.
    arrow_table_io::write_table(ranking_tmp, rank_table, rank_meta, ranking_group_rows,
                                {"username", "uid"});
    std::filesystem::rename(ranking_tmp, ranking_path);
    return rank;
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
                  int64_t users_history_group_rows, int64_t ranking_group_rows) {
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
    // feeds the ranking (and, in the update finalize, the filter-1 flag on
    // newly-written rows). The aspect mapping lives in
    // build_ranking_table; storing every counter total (not just the
    // ranking aspects) keeps the per-user totals complete in this file.
    std::vector<int64_t> rank_uids;
    std::vector<std::string> rank_usernames;
    std::vector<uint16_t> rank_first_seen;
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
            rank_uids.push_back(cur);
            if (group_username.empty()) group_username = "<" + std::to_string(cur) + ">";
            rank_usernames.push_back(group_username);
            rank_first_seen.push_back(group_first_seen);
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

    // Writes user_ranking.parquet for every contributor (identity, current
    // username, the exact per-aspect percentile scores).
    build_ranking_table(rank_uids, rank_usernames, rank_first_seen, counter_sums,
                        history_path, ranking_group_rows);

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
        // Import writes no suspect flags: the object hours precede the
        // replication stream's minute buckets (filters 2/3), and filter 1 is
        // forward-only, marking rows written after a low ranking is
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
        arrow::field("suspect_flag", arrow::uint8(), false),
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
              << " history rows, " << rank_uids.size() << " ranking rows\n";
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
                         int64_t users_history_group_rows, int64_t ranking_group_rows,
                         const std::string& minutes_path,
                         const std::map<std::pair<int64_t, uint16_t>, suspect::MoveDay>& move_flags) {
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

    const std::filesystem::path history_parent =
        std::filesystem::path(history_path).parent_path();
    const std::string ranking_path = (history_parent / "user_ranking.parquet").string();
    const std::string suspect_path = (history_parent / "suspect.parquet").string();

    // The suspect file freezes each flagged day's filter-1 ranking and
    // far-move count. suspect_base maps every previously flagged
    // (uid, change_date) to those carried values; the base read and the export
    // loop below that writes over it must stay in lockstep (far_move_count,
    // ranking_at_day are the only carried columns).
    struct SuspectBaseRow {
        uint32_t far_move_count = 0;
        uint8_t ranking_at_day = 0;
    };
    std::map<std::pair<int64_t, uint16_t>, SuspectBaseRow> suspect_base;
    if (std::filesystem::exists(suspect_path)) {
        auto base_result = arrow_table_io::read_table(suspect_path)->CombineChunks();
        if (!base_result.ok()) {
            throw std::runtime_error("CombineChunks failed on " + suspect_path + ": " +
                                     base_result.status().ToString());
        }
        const std::shared_ptr<arrow::Table> base_v = *base_result;
        const auto* bv_uids = typed_column<arrow::Int64Array>(base_v, "uid", suspect_path);
        const auto* bv_days =
            typed_column<arrow::UInt16Array>(base_v, "change_date", suspect_path);
        const auto* bv_moves =
            typed_column<arrow::UInt32Array>(base_v, "far_move_count", suspect_path);
        const auto* bv_rank =
            typed_column<arrow::UInt8Array>(base_v, "ranking_at_day", suspect_path);
        for (int64_t i = 0; i < base_v->num_rows(); ++i) {
            SuspectBaseRow& row = suspect_base[{bv_uids->Value(i), bv_days->Value(i)}];
            row.far_move_count = bv_moves->Value(i);
            row.ranking_at_day = bv_rank->Value(i);
        }
    }
    std::vector<int64_t> rank_uids;
    std::vector<std::string> rank_usernames;
    std::vector<uint16_t> rank_first_seen;
    std::array<std::vector<uint64_t>, kCounterCount> counter_sums;
    std::unordered_map<int64_t, size_t> uid_index;
    // The base ranking rows: identity columns and the 21 counter totals, which
    // the run's delta totals are added to below. Together with the diff totals
    // they are ranked exactly over the whole population again, before the
    // history write, because the ranking also yields the filter-1 flag set
    // that this run's newly-written rows are screened with.
    bool have_base_rank = false;
    if (std::filesystem::exists(ranking_path)) {
        auto base_result = arrow_table_io::read_table(ranking_path)->CombineChunks();
        if (!base_result.ok()) {
            throw std::runtime_error("CombineChunks failed on " + ranking_path + ": " +
                                     base_result.status().ToString());
        }
        const std::shared_ptr<arrow::Table> base_rank = *base_result;
        const auto* r_uids = typed_column<arrow::Int64Array>(base_rank, "uid", ranking_path);
        const auto* r_users =
            typed_column<arrow::StringArray>(base_rank, "username", ranking_path);
        const auto* r_seen =
            typed_column<arrow::UInt16Array>(base_rank, "first_seen_day", ranking_path);
        for (int64_t i = 0; i < base_rank->num_rows(); ++i) {
            const int64_t uid = r_uids->Value(i);
            uid_index[uid] = rank_uids.size();
            rank_uids.push_back(uid);
            rank_usernames.push_back(r_users->GetString(i));
            rank_first_seen.push_back(r_seen->Value(i));
        }
        for (size_t c = 0; c < kCounterCount; ++c) {
            const auto* arr =
                typed_column<arrow::UInt32Array>(base_rank, kCounters[c].name, ranking_path);
            counter_sums[c].resize(rank_uids.size());
            for (size_t i = 0; i < rank_uids.size(); ++i) counter_sums[c][i] = arr->Value(i);
        }
        have_base_rank = true;
    }

    // Users history base, read with the two bases above so that a base which
    // does not match this build aborts the run before its first file is
    // replaced. It is folded with this run's deltas and flag sources below
    // into one sorted rewrite. All three filter bits are monotonic: the base
    // row's flags are carried forward unchanged, bits 0/1 are ORed with this
    // run's minute-store and move-flagged days, and bit 2 (filter 1) is set on
    // the run's newly-written rows whose user's current ranking is below
    // the threshold. Base rows are never masked or re-flagged, so a flag once
    // written persists and a ranking drop is never applied retroactively.
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
        const auto* b_uids = typed_column<arrow::Int64Array>(base_table, "uid", history_path);
        const auto* b_days =
            typed_column<arrow::UInt16Array>(base_table, "change_date", history_path);
        const auto* b_counts =
            typed_column<arrow::UInt32Array>(base_table, "count", history_path);
        const auto* b_flags =
            typed_column<arrow::UInt8Array>(base_table, "suspect_flag", history_path);
        for (int64_t i = 0; i < base_table->num_rows(); ++i) {
            const auto key = std::make_pair(b_uids->Value(i), b_days->Value(i));
            merged_counts[key] += b_counts->Value(i);
            merged_flags[key] |= b_flags->Value(i);
            base_keys.insert(key);
        }
    }
    // Filter 1 (paper sec. 5): users whose current ranking is below
    // kFilter1RankingThreshold. Unlike the diff-based screens this is a
    // forward-only flag: it is applied to the run's newly-written rows and
    // never re-derived over the base history. A contributor who created
    // nothing ranks 0, which covers the paper's "new users" half.
    std::unordered_map<int64_t, uint8_t> filter1_uid;
    ranking::Result rank;
    if (have_base_rank || !delta_totals.empty()) {
        for (const auto& [uid, sum] : delta_totals) {
            auto it = uid_index.find(uid);
            if (it != uid_index.end()) {
                const size_t idx = it->second;
                for (size_t c = 0; c < kCounterCount; ++c) counter_sums[c][idx] += sum[c];
                const auto us = delta_username.find(uid);
                if (us != delta_username.end() && !us->second.empty()) rank_usernames[idx] = us->second;
                rank_first_seen[idx] = std::min(rank_first_seen[idx], delta_first_seen[uid]);
            } else {
                uid_index[uid] = rank_uids.size();
                rank_uids.push_back(uid);
                std::string name = delta_username[uid];
                if (name.empty()) name = "<" + std::to_string(uid) + ">";
                rank_usernames.push_back(std::move(name));
                rank_first_seen.push_back(delta_first_seen[uid]);
                for (size_t c = 0; c < kCounterCount; ++c) counter_sums[c].push_back(sum[c]);
            }
        }
        rank = build_ranking_table(
            rank_uids, rank_usernames, rank_first_seen, counter_sums, history_path,
            ranking_group_rows);
        for (size_t i = 0; i < rank_uids.size() && i < rank.ranking.size(); ++i) {
            if (rank.ranking[i] < suspect::kFilter1RankingThreshold) {
                filter1_uid[rank_uids[i]] = suspect::kFlagFilter1;
            }
        }
    }

    for (const auto& [key, count] : delta_counts) merged_counts[key] += count;

    // Rebuild the whole-history flags from the persisted sources: bit 0 from
    // the minute store, bit 1 from the run's folded move-flagged days. Every
    // filter2_days value is kFlagFilter2 and every move_flags entry carries
    // kFlagFilter3, so ORing them into the carried-forward base bits yields
    // the combined per-day field.
    const auto filter2_days = suspect::flagged_days(minutes_path);
    for (const auto& [key, flag] : filter2_days) {
        merged_flags[key] |= flag;
    }
    for (const auto& [key, value] : move_flags) {
        merged_flags[key] |= value.flags;
    }

    arrow::Int64Builder ind_uid_builder;
    arrow::UInt16Builder ind_day_builder;
    arrow::UInt32Builder ind_count_builder;
    arrow::UInt8Builder ind_flag_builder;
    // Suspect export: the same merged flags, one (uid, change_date) row per
    // day carrying any bit, plus the day's total change count (created +
    // modified + deleted, the same value as the count column written above),
    // the count of that day's staged moves beyond kFilter3Threshold and the
    // ranking frozen at the day's first flag, with the username resolved
    // inline.
    arrow::Int64Builder v_uid_builder;
    arrow::StringBuilder v_user_builder;
    arrow::UInt16Builder v_day_builder;
    arrow::UInt8Builder v_flag_builder;
    arrow::UInt32Builder v_changes_builder;
    arrow::UInt32Builder v_moves_builder;
    arrow::UInt8Builder v_rank_builder;
    for (const auto& [key, count] : merged_counts) {
        append_checked(ind_uid_builder, key.first);
        append_checked(ind_day_builder, key.second);
        append_checked(ind_count_builder, count);
        // Forward-only filter 1: only the rows this run newly writes may pick
        // up the low-ranking bit; a day already present in the base file
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
                               ? rank_usernames[ui->second]
                               : "<" + std::to_string(key.first) + ">");
            // changes is the day's own total re-derived from the merged counts
            // each run (an appended-to history yields the same sum, so the
            // value never drifts and never touches other days). far_move_count
            // is the day's count of moves beyond kFilter3Threshold: carried
            // from the base file when the day was already flagged (the move
            // stage is transient), taken from this run's folded staged moves
            // when the day is newly flagged. ranking_at_day is stamped once:
            // carried from the base file when the day was already flagged,
            // taken from this run's current ranking when the day is newly
            // flagged, and never recalculated.
            append_checked(v_changes_builder, count);
            const auto carried = suspect_base.find(key);
            const auto mv = move_flags.find(key);
            if (carried != suspect_base.end()) {
                append_checked(v_moves_builder, carried->second.far_move_count);
                append_checked(v_rank_builder, carried->second.ranking_at_day);
            } else {
                append_checked(v_moves_builder,
                               mv != move_flags.end() ? mv->second.far_move_count : 0);
                append_checked(v_rank_builder,
                               ui != uid_index.end() ? rank.ranking[ui->second] : 0);
            }
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
                       arrow::field("suspect_flag", arrow::uint8(), false)}),
        {ind_uid, ind_day, ind_count, ind_flag});

    const std::string history_tmp = history_path + ".tmp";
    // The viewer filters on uid; only that column keeps row-group min/max
    // statistics in the footer.
    arrow_table_io::write_table(history_tmp, history_table, users_history_group_rows,
                                {"uid"});
    std::filesystem::rename(history_tmp, history_path);

    // The suspect export: every (uid, change_date) carrying any flag bit,
    // with the username, the day's total change count, the count of that day's
    // far moves and the ranking frozen at the day's first flag, sorted by
    // (change_date, uid) with change_date descending (newest first) so a client
    // reading the 100 latest flagged days fetches only the leading row groups.
    // The flag set is exactly the users-history flags (the merged state above),
    // so the two outputs always agree. The file is update-only: import writes
    // no flags and never produces it; an update with no flagged day writes an
    // empty file.
    std::shared_ptr<arrow::Array> v_uid, v_user, v_day, v_flag, v_changes, v_moves, v_rank;
    finish_checked(v_uid_builder, &v_uid);
    finish_checked(v_user_builder, &v_user);
    finish_checked(v_day_builder, &v_day);
    finish_checked(v_flag_builder, &v_flag);
    finish_checked(v_changes_builder, &v_changes);
    finish_checked(v_moves_builder, &v_moves);
    finish_checked(v_rank_builder, &v_rank);
    auto suspect_table = arrow::Table::Make(
        arrow::schema({arrow::field("uid", arrow::int64(), false),
                       arrow::field("username", arrow::utf8(), false),
                       arrow::field("change_date", arrow::uint16(), false),
                       arrow::field("suspect_flag", arrow::uint8(), false),
                       arrow::field("changes", arrow::uint32(), false),
                       arrow::field("far_move_count", arrow::uint32(), false),
                       arrow::field("ranking_at_day", arrow::uint8(), false)}),
        {v_uid, v_user, v_day, v_flag, v_changes, v_moves, v_rank});
    // Newest-first order makes change_date (the pruning column) the compact
    // footer statistics key, with the most recent days' min/max in the first
    // row groups.
    suspect_table = arrow_table_io::sort_by_keys(
        suspect_table,
        {arrow::compute::SortKey("change_date", arrow::compute::SortOrder::Descending),
         arrow::compute::SortKey("uid")});
    const std::string suspect_tmp = suspect_path + ".tmp";
    arrow_table_io::write_table(suspect_tmp, suspect_table, ranking_group_rows,
                                {"change_date"});
    std::filesystem::rename(suspect_tmp, suspect_path);

    std::filesystem::remove_all(stage_root);

    std::cerr << "[users history] update finalized " << history_table->num_rows()
              << " history rows, " << rank_uids.size() << " ranking rows, "
              << suspect_table->num_rows() << " suspect rows\n";
}

}  // namespace users_history
