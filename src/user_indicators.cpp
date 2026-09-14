#include "user_indicators.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/object.hpp>
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

namespace user_indicators {

namespace {

// Column order mirrors DayRow's member order.
constexpr std::array<uint32_t DayRow::*, 9> kDayCounterMembers = {
    &DayRow::node_created, &DayRow::node_modified, &DayRow::node_deleted,
    &DayRow::way_created,  &DayRow::way_modified,  &DayRow::way_deleted,
    &DayRow::relocated,    &DayRow::short_lived,   &DayRow::rapid_edit,
};

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
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("node_created", arrow::uint32(), false),
        arrow::field("node_modified", arrow::uint32(), false),
        arrow::field("node_deleted", arrow::uint32(), false),
        arrow::field("way_created", arrow::uint32(), false),
        arrow::field("way_modified", arrow::uint32(), false),
        arrow::field("way_deleted", arrow::uint32(), false),
        arrow::field("relocated", arrow::uint32(), false),
        arrow::field("short_lived", arrow::uint32(), false),
        arrow::field("rapid_edit", arrow::uint32(), false),
    });
}

void write_stage_file(const std::string& path,
                      const std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash>& rows) {
    if (rows.empty()) return;

    const int64_t n = static_cast<int64_t>(rows.size());
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder username_builder;
    arrow::UInt16Builder date_builder;
    std::array<arrow::UInt32Builder, 9> counter_builders;

    if (!uid_builder.Reserve(n).ok() || !username_builder.Reserve(n).ok() ||
        !date_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while flushing user-indicator stage");
    }
    for (size_t i = 0; i < 9; ++i) {
        if (!counter_builders[i].Reserve(n).ok()) {
            throw std::runtime_error("Reserve() failed while flushing user-indicator stage");
        }
    }

    for (const auto& [key, entry] : rows) {
        append_checked(uid_builder, key.uid);
        append_checked(username_builder, entry.username);
        append_checked(date_builder, key.day);
        const DayRow& r = entry.row;
        for (size_t i = 0; i < 9; ++i) append_checked(counter_builders[i], r.*kDayCounterMembers[i]);
    }

    std::shared_ptr<arrow::Array> uid, username, date;
    std::array<std::shared_ptr<arrow::Array>, 9> counters;
    finish_checked(uid_builder, &uid);
    finish_checked(username_builder, &username);
    finish_checked(date_builder, &date);
    for (size_t i = 0; i < 9; ++i) finish_checked(counter_builders[i], &counters[i]);

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
                        : std::nullopt);
    }

    void way(const osmium::Way& way) {
        begin_object(ObjectKind::Way, way.id());
        add_version(static_cast<int64_t>(way.uid()), object_user(way),
                    version_day(way.timestamp()), way.visible(),
                    static_cast<uint32_t>(way.version()), ObjectKind::Way, std::nullopt);
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

    static uint16_t version_day(const osmium::Timestamp& ts) {
        return h3_utils::require_u16_day(
            h3_utils::timestamp_to_utc_day(ts.seconds_since_epoch()));
    }

    void add_version(int64_t uid, const std::string& username, uint16_t day, bool visible,
                     uint32_t version, ObjectKind kind,
                     std::optional<std::pair<double, double>> coords) {
        versions_++;
        stats_.add_version(uid, username, day, visible, version, kind, std::move(coords));
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
        throw std::runtime_error("Failed to sort user-indicator rows: " +
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
            throw std::runtime_error("Failed to order user-indicator rows: " +
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
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way);

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

void run_finalize(const std::string& stage_dir, const std::string& profiles_path,
                  const std::string& indicators_path, const Thresholds& thresholds) {
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

    std::array<std::shared_ptr<arrow::Array>, 9> counter_arrays;
    for (size_t i = 0; i < 9; ++i) {
        counter_arrays[i] = combined->column(3 + static_cast<int>(i))->chunk(0);
    }

    // One derived pass over the (uid, change_date)-sorted rows computes the
    // indicator rows, the per-uid first-seen day, the per-username first-edit
    // day, and the events within the new-user window.
    arrow::Int64Builder ind_uid_builder;
    arrow::UInt16Builder ind_day_builder;
    std::array<arrow::UInt32Builder, 9> ind_counter_builders;

    arrow::Int64Builder prof_uid_builder;
    arrow::StringBuilder prof_user_builder;
    arrow::UInt16Builder prof_first_edit_builder;
    arrow::UInt16Builder prof_first_seen_builder;
    arrow::BooleanBuilder prof_bulk_builder;

    std::unordered_map<std::string, uint16_t> username_first_day;
    int64_t current_uid = 0;
    bool in_group = false;
    uint16_t first_seen = 0;
    uint32_t window_total = 0;

    const int64_t n = combined->num_rows();
    auto flush_profiles_group = [&]() {
        const bool bulk_new_user = window_total >= static_cast<uint32_t>(thresholds.bulk_edit_min);
        for (const auto& [username, first_edit_day] : username_first_day) {
            append_checked(prof_uid_builder, current_uid);
            const std::string label = username.empty()
                                          ? "<" + std::to_string(current_uid) + ">"
                                          : username;
            append_checked(prof_user_builder, label);
            append_checked(prof_first_edit_builder, first_edit_day);
            append_checked(prof_first_seen_builder, first_seen);
            append_checked(prof_bulk_builder, bulk_new_user);
        }
        username_first_day.clear();
        window_total = 0;
        in_group = false;
    };

    for (int64_t i = 0; i < n; ++i) {
        const int64_t uid = uid_array->Value(i);
        if (!in_group) {
            current_uid = uid;
            in_group = true;
            first_seen = day_array->Value(i);
        } else if (uid != current_uid) {
            flush_profiles_group();
            current_uid = uid;
            in_group = true;
            first_seen = day_array->Value(i);
        }

        const uint16_t day = day_array->Value(i);
        DayRow day_row;
        for (size_t c = 0; c < 9; ++c) {
            const auto* arr = static_cast<const arrow::UInt32Array*>(counter_arrays[c].get());
            day_row.*kDayCounterMembers[c] = arr->Value(i);
        }

        const std::string username(user_array->GetView(i));
        const uint16_t day_int = day;
        auto it = username_first_day.find(username);
        if (it == username_first_day.end()) {
            username_first_day.emplace(username, day_int);
        } else if (day_int < it->second) {
            it->second = day_int;
        }

        if (static_cast<int32_t>(day_int) - static_cast<int32_t>(first_seen) <
            thresholds.new_user_window_days) {
            window_total += day_row.total_events();
        }

        append_checked(ind_uid_builder, uid);
        append_checked(ind_day_builder, day);
        for (size_t c = 0; c < 9; ++c) {
            append_checked(ind_counter_builders[c], day_row.*kDayCounterMembers[c]);
        }
    }
    if (in_group) flush_profiles_group();

    std::shared_ptr<arrow::Array> ind_uid, ind_day;
    std::array<std::shared_ptr<arrow::Array>, 9> ind_counters;
    finish_checked(ind_uid_builder, &ind_uid);
    finish_checked(ind_day_builder, &ind_day);
    for (size_t c = 0; c < 9; ++c) finish_checked(ind_counter_builders[c], &ind_counters[c]);

    auto indicator_schema = arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("node_created", arrow::uint32(), false),
        arrow::field("node_modified", arrow::uint32(), false),
        arrow::field("node_deleted", arrow::uint32(), false),
        arrow::field("way_created", arrow::uint32(), false),
        arrow::field("way_modified", arrow::uint32(), false),
        arrow::field("way_deleted", arrow::uint32(), false),
        arrow::field("relocated", arrow::uint32(), false),
        arrow::field("short_lived", arrow::uint32(), false),
        arrow::field("rapid_edit", arrow::uint32(), false),
    });
    std::vector<std::shared_ptr<arrow::Array>> indicator_columns = {ind_uid, ind_day};
    indicator_columns.insert(indicator_columns.end(), ind_counters.begin(), ind_counters.end());
    // The indicator rows were appended in (uid, change_date) order while
    // walking the sorted combined table, so no re-sort is needed.
    auto indicator_table = arrow::Table::Make(indicator_schema, indicator_columns);

    std::shared_ptr<arrow::Array> prof_uid, prof_user, prof_first_edit, prof_first_seen, prof_bulk;
    if (!prof_uid_builder.Finish(&prof_uid).ok() || !prof_user_builder.Finish(&prof_user).ok() ||
        !prof_first_edit_builder.Finish(&prof_first_edit).ok() ||
        !prof_first_seen_builder.Finish(&prof_first_seen).ok() ||
        !prof_bulk_builder.Finish(&prof_bulk).ok()) {
        throw std::runtime_error("Failed to finalize profile columns");
    }
    auto profile_table = arrow::Table::Make(
        arrow::schema({
            arrow::field("uid", arrow::int64(), false),
            arrow::field("username", arrow::utf8(), false),
            arrow::field("first_edit_day", arrow::uint16(), false),
            arrow::field("first_seen_day", arrow::uint16(), false),
            arrow::field("bulk_new_user", arrow::boolean(), false),
        }),
        {prof_uid, prof_user, prof_first_edit, prof_first_seen, prof_bulk});
    // First-edit day of each segment is what makes profile rows unique per
    // (uid, username) interval; ordering by it keeps the catalog stable.
    profile_table = order_table(profile_table,
                                {arrow::compute::SortKey("uid"),
                                 arrow::compute::SortKey("first_edit_day")});

    const std::string indicators_tmp = indicators_path + ".tmp";
    const std::string profiles_tmp = profiles_path + ".tmp";
    arrow_table_io::write_table(indicators_tmp, indicator_table);
    arrow_table_io::write_table(profiles_tmp, profile_table);
    std::filesystem::rename(indicators_tmp, indicators_path);
    std::filesystem::rename(profiles_tmp, profiles_path);
    std::filesystem::remove_all(stage_dir);

    std::cerr << "[user indicators] finalized " << indicator_table->num_rows()
              << " indicator rows, " << profile_table->num_rows() << " profile rows\n";
}

}  // namespace user_indicators