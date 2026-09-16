#include "user_indicators.hpp"

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
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "h3_utils.hpp"
#include "reputation.hpp"

namespace user_indicators {

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
        throw std::runtime_error("Reserve() failed while flushing user-indicator stage");
    }
    for (size_t i = 0; i < kCounterCount; ++i) {
        if (!counter_builders[i].Reserve(n).ok()) {
            throw std::runtime_error("Reserve() failed while flushing user-indicator stage");
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
// Streaming scan handler over the history file. Objects arrive sorted by
// (type, id, version), so a run of contiguous versions of one object is
// aggregated in UserEventStats and dropped as the stream moves on.
// ---------------------------------------------------------------------------

class ScanHandler : public osmium::handler::Handler {
public:
    explicit ScanHandler(const std::string& stage_dir, const Thresholds& thresholds)
        : stage_dir_(stage_dir), stats_(thresholds) {}

    void node(const osmium::Node& node) {
        begin_object(ObjectKind::Node, node.id());
        const osmium::Location& loc = node.location();
        // lat()/lon() throw osmium::invalid_location on a location-less node
        // (deleted history versions carry no coordinates); only evaluate them
        // behind the validity guard, as the node pass does. Location-less
        // versions pass an empty optional that never contributes a baseline.
        const bool has_coords = loc.valid();
        add_version(static_cast<int64_t>(node.uid()), object_user(node),
                    version_day(node.timestamp()), node.visible(),
                    static_cast<uint32_t>(node.version()), ObjectKind::Node,
                    has_coords
                        ? std::optional<std::pair<double, double>>(
                              std::make_pair(loc.lat(), loc.lon()))
                        : std::nullopt,
                    created_tag_bits(node));
    }

    void way(const osmium::Way& way) {
        begin_object(ObjectKind::Way, way.id());
        add_version(static_cast<int64_t>(way.uid()), object_user(way),
                    version_day(way.timestamp()), way.visible(),
                    static_cast<uint32_t>(way.version()), ObjectKind::Way, std::nullopt,
                    created_tag_bits(way));
    }

    // Relations feed only the created counter: the OSMPatrol reputation is
    // built from created objects, and relation runs must not touch the
    // node/way run state. Location-less by nature, so no coords handling.
    void relation(const osmium::Relation& relation) {
        if (relation.visible() && relation.version() == 1) {
            stats_.record_relation_created(static_cast<int64_t>(relation.uid()),
                                           object_user(relation),
                                           version_day(relation.timestamp()),
                                           created_tag_bits(relation));
            // Relation-only stretches never reach add_version()'s threshold
            // check, so keep the accumulator bounded here too.
            if (stats_.size() >= kFlushThreshold) flush_stage();
        }
    }

    void finish() {
        if (has_current_) stats_.end_object();
        flush_stage();
    }

    // INSTR
    uint64_t objects() const { return objects_; }
    uint64_t versions() const { return versions_; }
    size_t stage_files() const { return stage_files_; }
    size_t stage_rows() const { return stage_rows_flushed_; }

private:
    // Ends the previous object's run and starts a new one whenever the kind
    // or id changed (versions arrive contiguous per object). May be called
    // on every version; it is a no-op within a run.
    void begin_object(ObjectKind kind, int64_t id) {
        if (has_current_ && kind == kind_ && id == current_id_) return;
        if (has_current_) stats_.end_object();
        stats_.begin_object();
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
                     uint32_t version, ObjectKind kind,
                     std::optional<std::pair<double, double>> coords,
                     uint32_t created_tag_bits = 0) {
        versions_++;
        stats_.add_version(uid, username, day, visible, version, kind, std::move(coords),
                           created_tag_bits);
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

std::shared_ptr<arrow::Table> order_table(
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<arrow::compute::SortKey>& keys) {
    arrow::compute::SortOptions options(keys);
    auto indices_result = arrow::compute::SortIndices(arrow::Datum(table), options);
    if (!indices_result.ok()) {
        throw std::runtime_error("Failed to sort rows: " +
                                 indices_result.status().ToString());
    }
    const std::shared_ptr<arrow::Array> index_array = indices_result.ValueOrDie();

    std::vector<std::shared_ptr<arrow::Array>> sorted_columns;
    for (const auto& column : table->columns()) {
        if (column->num_chunks() != 1) {
            throw std::runtime_error("Unexpected multi-chunk column while sorting");
        }
        auto taken_result = arrow::compute::Take(*column->chunk(0), *index_array,
                                                 arrow::compute::TakeOptions::Defaults());
        if (!taken_result.ok()) {
            throw std::runtime_error("Failed to order rows: " +
                                     taken_result.status().ToString());
        }
        sorted_columns.push_back(taken_result.ValueOrDie());
    }
    return arrow::Table::Make(table->schema(), sorted_columns);
}

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

}  // namespace

// ---------------------------------------------------------------------------
// Scan + finalize entry points
// ---------------------------------------------------------------------------

void run_scan(const std::string& input_path, const std::string& stage_dir,
              const Thresholds& thresholds) {
    std::filesystem::remove_all(stage_dir);
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(input_path);
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    ScanHandler handler(stage_dir, thresholds);

    auto start = std::chrono::steady_clock::now();
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[user indicators] scan done in " << elapsed << "s\n";
    // INSTR
    std::cerr << "[user indicators] objects=" << handler.objects()
              << " versions=" << handler.versions()
              << " stage_rows=" << handler.stage_rows()
              << " stage_files=" << handler.stage_files() << "\n";
}

void run_finalize(const std::string& stage_dir, const std::string& indicators_path,
                  const Thresholds& thresholds) {
    // Registers Arrow's compute kernels (sort_indices, take), required even
    // when --user-indicators runs without any of passes 1-3.
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
        std::cerr << "[user indicators] no stage files under " << stage_dir
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
                                     "' is missing; rerun --user-indicators to "
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
    auto combined = order_table(arrow::Table::Make(schema, columns),
                                {arrow::compute::SortKey("uid"),
                                 arrow::compute::SortKey("change_date")});

    const auto* uid_array = static_cast<const arrow::Int64Array*>(combined->column(0)->chunk(0).get());
    const auto* user_array = static_cast<const arrow::StringArray*>(combined->column(1)->chunk(0).get());
    const auto* day_array = static_cast<const arrow::UInt16Array*>(combined->column(2)->chunk(0).get());

    std::array<std::shared_ptr<arrow::Array>, kCounterCount> counter_arrays;
    for (size_t i = 0; i < kCounterCount; ++i) {
        counter_arrays[i] = combined->column(3 + static_cast<int>(i))->chunk(0);
    }

    // One derived pass over the (uid, change_date)-sorted rows computes the
    // indicator rows.
    arrow::Int64Builder ind_uid_builder;
    arrow::UInt16Builder ind_day_builder;
    std::array<arrow::UInt32Builder, kCounterCount> ind_counter_builders;

    const int64_t n = combined->num_rows();
    for (int64_t i = 0; i < n; ++i) {
        const int64_t uid = uid_array->Value(i);

        const uint16_t day = day_array->Value(i);
        DayRow day_row;
        for (size_t c = 0; c < kCounterCount; ++c) {
            const auto* arr = static_cast<const arrow::UInt32Array*>(counter_arrays[c].get());
            day_row.*kCounters[c].member = arr->Value(i);
        }

        append_checked(ind_uid_builder, uid);
        append_checked(ind_day_builder, day);
        for (size_t c = 0; c < kCounterCount; ++c) {
            append_checked(ind_counter_builders[c], day_row.*kCounters[c].member);
        }
    }

    std::shared_ptr<arrow::Array> ind_uid, ind_day;
    std::array<std::shared_ptr<arrow::Array>, kCounterCount> ind_counters;
    finish_checked(ind_uid_builder, &ind_uid);
    finish_checked(ind_day_builder, &ind_day);
    for (size_t c = 0; c < kCounterCount; ++c) finish_checked(ind_counter_builders[c], &ind_counters[c]);

    std::vector<std::shared_ptr<arrow::Field>> indicator_fields = {
        arrow::field("uid", arrow::int64(), false),
        arrow::field("change_date", arrow::uint16(), false),
    };
    for (const auto& c : kCounters) {
        indicator_fields.push_back(arrow::field(c.name, arrow::uint32(), false));
    }
    auto indicator_schema = arrow::schema(indicator_fields);
    std::vector<std::shared_ptr<arrow::Array>> indicator_columns = {ind_uid, ind_day};
    indicator_columns.insert(indicator_columns.end(), ind_counters.begin(), ind_counters.end());
    // The indicator rows were appended in (uid, change_date) order while
    // walking the sorted combined table, so no re-sort is needed.
    auto indicator_table = arrow::Table::Make(indicator_schema, indicator_columns);

    const std::string indicators_tmp = indicators_path + ".tmp";
    arrow_table_io::write_table(indicators_tmp, indicator_table);
    std::filesystem::rename(indicators_tmp, indicators_path);

    // Per-uid sums of all 22 indicator counters, in the (uid) order of the
    // sorted indicator table. The three created-object aspects are bound to
    // their kCounters columns by name; the tag columns are always the
    // trailing kTagCount entries, in kTop12TagKeys order, so a schema change
    // cannot silently move an aspect. Storing every counter total (not just
    // the reputation aspects) lets the file double as the running per-user
    // totals for a later incremental update pass.
    constexpr size_t kRepAspectCount = 3 + kTagCount;
    constexpr auto counter_index = [](std::string_view name) -> size_t {
        for (size_t i = 0; i < kCounterCount; ++i) {
            if (kCounters[i].name == name) return i;
        }
        return kCounterCount;
    };
    constexpr size_t kFirstTag = kCounterCount - kTagCount;
    std::array<size_t, kRepAspectCount> rep_counter_idx = {
        counter_index("node_created"), counter_index("way_created"),
        counter_index("relation_created")};
    for (size_t i = 0; i < kTagCount; ++i) rep_counter_idx[3 + i] = kFirstTag + i;

    std::vector<int64_t> rep_uids;
    std::vector<std::string> rep_usernames;
    std::vector<uint16_t> rep_first_seen;
    std::vector<bool> rep_bulk;
    std::vector<uint32_t> rep_max_day_changes;
    std::array<std::vector<uint64_t>, kCounterCount> counter_sums;
    {
        std::array<uint64_t, kCounterCount> acc{};
        int64_t cur = 0;
        bool in = false;
        uint16_t group_first_seen = 0;
        uint32_t window_total = 0;
        uint32_t max_day_changes = 0;
        std::string group_username;
        const auto* uid_arr = static_cast<const arrow::Int64Array*>(ind_uid.get());
        const auto flush = [&]() {
            if (!in) return;
            rep_uids.push_back(cur);
            if (group_username.empty()) group_username = "<" + std::to_string(cur) + ">";
            rep_usernames.push_back(group_username);
            rep_first_seen.push_back(group_first_seen);
            rep_bulk.push_back(window_total >= static_cast<uint32_t>(thresholds.bulk_edit_min));
            rep_max_day_changes.push_back(max_day_changes);
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
                window_total = 0;
                max_day_changes = 0;
                group_username.clear();
            }
            // Combined rows are index-aligned with the indicator rows, so the
            // row's username is the last (current) username seen for the uid.
            group_username = user_array->GetView(i);
            uint64_t total_events = 0;
            for (size_t c = 0; c < kCounterCount; ++c) {
                const auto* arr =
                    static_cast<const arrow::UInt32Array*>(ind_counters[c].get());
                const uint32_t v = arr->Value(i);
                acc[c] += v;
                if (c < 6) total_events += v;  // node/way change counters
            }
            if (static_cast<int32_t>(day) - static_cast<int32_t>(group_first_seen) <
                thresholds.new_user_window_days) {
                window_total += static_cast<uint32_t>(total_events);
            }
            // Same per-day cutoff the edits viewer applies to flag mass edit
            // or delete bursts (node/way modified+deleted).
            const uint32_t day_changes =
                static_cast<const arrow::UInt32Array*>(ind_counters[1].get())->Value(i) +
                static_cast<const arrow::UInt32Array*>(ind_counters[2].get())->Value(i) +
                static_cast<const arrow::UInt32Array*>(ind_counters[4].get())->Value(i) +
                static_cast<const arrow::UInt32Array*>(ind_counters[5].get())->Value(i);
            max_day_changes = std::max(max_day_changes, day_changes);
        }
        flush();
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

    // Wide one-row-per-uid table: uid, current username (identity, so the
    // viewers can filter by exact username), the user's first-seen day, the
    // bulk-new-user flag, the busiest single-day node/way modified+deleted
    // count, reputation, the 22 indicator totals, and the per-aspect
    // percentile column, sorted by username (uid tie-break) so an exact
    // username filter prunes to the matching pages. The dataset-wide
    // active/max stats are written once as file key_value_metadata (_active
    // and _max keys) instead of repeated per-row columns, and the per-aspect
    // points derive client-side from pct and the constant paper caps.
    arrow::Int64Builder rep_uid_builder;
    arrow::StringBuilder rep_username_builder;
    arrow::UInt16Builder rep_first_seen_builder;
    arrow::BooleanBuilder rep_bulk_builder;
    arrow::UInt32Builder rep_max_day_changes_builder;
    arrow::UInt8Builder rep_score_builder;
    std::array<arrow::UInt32Builder, kCounterCount> rep_counter_builders;
    std::array<arrow::DoubleBuilder, kRepAspectCount> rep_pct_builders;
    for (size_t i = 0; i < rep_uids.size(); ++i) {
        append_checked(rep_uid_builder, rep_uids[i]);
        append_checked(rep_username_builder, rep_usernames[i]);
        append_checked(rep_first_seen_builder, rep_first_seen[i]);
        append_checked(rep_bulk_builder, rep_bulk[i]);
        append_checked(rep_max_day_changes_builder, rep_max_day_changes[i]);
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
        arrow::field("bulk_new_user", arrow::boolean(), false),
        arrow::field("max_day_changes", arrow::uint32(), false),
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
    rep_columns.reserve(6 + kCounterCount + kRepAspectCount);
    std::shared_ptr<arrow::Array> rep_uid, rep_username_arr, rep_first_seen_arr, rep_bulk_arr;
    std::shared_ptr<arrow::Array> rep_max_day_changes_arr, rep_score;
    finish_checked(rep_uid_builder, &rep_uid);
    finish_checked(rep_username_builder, &rep_username_arr);
    finish_checked(rep_first_seen_builder, &rep_first_seen_arr);
    finish_checked(rep_bulk_builder, &rep_bulk_arr);
    finish_checked(rep_max_day_changes_builder, &rep_max_day_changes_arr);
    finish_checked(rep_score_builder, &rep_score);
    rep_columns.push_back(rep_uid);
    rep_columns.push_back(rep_username_arr);
    rep_columns.push_back(rep_first_seen_arr);
    rep_columns.push_back(rep_bulk_arr);
    rep_columns.push_back(rep_max_day_changes_arr);
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
    // than written as 30 repeated columns. write_table puts the metadata into
    // the Parquet footer, which is what the viewer reads.
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
    rep_table = order_table(rep_table,
                            {arrow::compute::SortKey("username"),
                             arrow::compute::SortKey("uid")});

    const std::filesystem::path indicators_parent =
        std::filesystem::path(indicators_path).parent_path();
    const std::string reputation_path =
        (indicators_parent / "user_reputation.parquet").string();
    const std::string reputation_tmp = reputation_path + ".tmp";
    arrow_table_io::write_table(reputation_tmp, rep_table, rep_meta);
    std::filesystem::rename(reputation_tmp, reputation_path);

    std::filesystem::remove_all(stage_dir);

    std::cerr << "[user indicators] finalized " << indicator_table->num_rows()
              << " indicator rows, " << rep_uids.size() << " reputation rows\n";
}

}  // namespace user_indicators
