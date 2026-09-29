#include "suspect.hpp"
#include "suspect_store.hpp"
#include "suspect_tag_store.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/object.hpp>
#include <osmium/osm/relation.hpp>
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
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "h3_utils.hpp"
#include "suspect_store.hpp"
#include "suspect_cell_store.hpp"
#include "update.hpp"

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
// Filter 2 stage I/O (per-(uid, minute) modified+deleted counts)
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> counts_stage_schema() {
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("minute", arrow::uint32(), false),
        arrow::field("modified_deleted", arrow::uint32(), false),
    });
}

void write_counts_stage_file(
    const std::string& path,
    const std::unordered_map<UserMinuteKey, UserMinuteEntry, UserMinuteKeyHash>& rows) {
    if (rows.empty()) return;

    const int64_t n = static_cast<int64_t>(rows.size());
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder username_builder;
    arrow::UInt32Builder minute_builder;
    arrow::UInt32Builder count_builder;

    if (!uid_builder.Reserve(n).ok() || !username_builder.Reserve(n).ok() ||
        !minute_builder.Reserve(n).ok() || !count_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while flushing suspect stage");
    }
    for (const auto& [key, entry] : rows) {
        append_checked(uid_builder, key.uid);
        append_checked(username_builder, entry.username);
        append_checked(minute_builder, key.minute);
        append_checked(count_builder, entry.modified_deleted);
    }

    std::shared_ptr<arrow::Array> uid, username, minute, count;
    finish_checked(uid_builder, &uid);
    finish_checked(username_builder, &username);
    finish_checked(minute_builder, &minute);
    finish_checked(count_builder, &count);

    arrow_table_io::write_table(path,
                                arrow::Table::Make(counts_stage_schema(),
                                                   {uid, username, minute, count}));
}

// Streaming scan over one replication diff: classifies every object the same
// way the users-history diff scan does and feeds the per-(uid, minute)
// counter.
class SuspectScanHandler : public osmium::handler::Handler {
public:
    explicit SuspectScanHandler(std::string stage_dir)
        : stage_dir_(std::move(stage_dir)) {}

    void node(const osmium::Node& node) {
        add_object(static_cast<int64_t>(node.uid()), object_user(node),
                   node.timestamp(), node.visible(), static_cast<uint32_t>(node.version()));
    }

    void way(const osmium::Way& way) {
        add_object(static_cast<int64_t>(way.uid()), object_user(way),
                   way.timestamp(), way.visible(), static_cast<uint32_t>(way.version()));
    }

    void relation(const osmium::Relation& relation) {
        add_object(static_cast<int64_t>(relation.uid()), object_user(relation),
                   relation.timestamp(), relation.visible(),
                   static_cast<uint32_t>(relation.version()));
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

    void add_object(int64_t uid, const std::string& username, const osmium::Timestamp& ts,
                    bool visible, uint32_t version) {
        objects_++;
        stats_.add_object(uid, username, h3_utils::timestamp_to_utc_minute(
                                            ts.seconds_since_epoch()),
                          visible, version);
        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void flush_stage() {
        if (stats_.minutes().empty()) return;
        char name[32];
        std::snprintf(name, sizeof(name), "stage_%05zu.parquet", stage_files_);
        write_counts_stage_file(stage_dir_ + "/" + name, stats_.minutes());
        stage_rows_flushed_ += stats_.size();
        stats_.clear();
        stage_files_++;
    }

    std::string stage_dir_;
    MinuteStats stats_;

    // INSTR
    uint64_t objects_ = 0;
    size_t stage_files_ = 0;
    size_t stage_rows_flushed_ = 0;
};

// ---------------------------------------------------------------------------
// Filter 3 stage I/O (per-(uid, minute, meters) rows of moves > kFilter3Threshold)
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> moves_stage_schema() {
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("minute", arrow::uint32(), false),
        arrow::field("meters", arrow::uint32(), false),
    });
}

void write_moves_stage_file(const std::string& path, size_t n, const std::vector<int64_t>& uids,
                            const std::vector<uint32_t>& minutes,
                            const std::vector<uint32_t>& meters) {
    if (n == 0) return;
    arrow::Int64Builder uid_builder;
    arrow::UInt32Builder minute_builder;
    arrow::UInt32Builder meters_builder;

    if (!uid_builder.Reserve(static_cast<int64_t>(n)).ok() ||
        !minute_builder.Reserve(static_cast<int64_t>(n)).ok() ||
        !meters_builder.Reserve(static_cast<int64_t>(n)).ok()) {
        throw std::runtime_error("Reserve() failed while flushing move stage");
    }
    for (size_t i = 0; i < n; ++i) {
        append_checked(uid_builder, uids[i]);
        append_checked(minute_builder, minutes[i]);
        append_checked(meters_builder, meters[i]);
    }

    std::shared_ptr<arrow::Array> uid, minute, meters_array;
    finish_checked(uid_builder, &uid);
    finish_checked(minute_builder, &minute);
    finish_checked(meters_builder, &meters_array);

    arrow_table_io::write_table(
        path, arrow::Table::Make(moves_stage_schema(), {uid, minute, meters_array}));
}

// ---------------------------------------------------------------------------
// Filter 4 stage I/O (per-(uid, minute, h3_cell) modified+deleted counts)
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> cells_stage_schema() {
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("minute", arrow::uint32(), false),
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("modified_deleted", arrow::uint32(), false),
    });
}

void write_cells_stage_file(
    const std::string& path,
    const std::unordered_map<UserMinuteCellKey, UserMinuteCellEntry, UserMinuteCellKeyHash>& rows) {
    if (rows.empty()) return;

    const int64_t n = static_cast<int64_t>(rows.size());
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder username_builder;
    arrow::UInt32Builder minute_builder;
    arrow::UInt64Builder cell_builder;
    arrow::UInt32Builder count_builder;

    if (!uid_builder.Reserve(n).ok() || !username_builder.Reserve(n).ok() ||
        !minute_builder.Reserve(n).ok() || !cell_builder.Reserve(n).ok() ||
        !count_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while flushing suspect cell stage");
    }
    for (const auto& [key, entry] : rows) {
        append_checked(uid_builder, key.uid);
        append_checked(username_builder, entry.username);
        append_checked(minute_builder, key.minute);
        append_checked(cell_builder, key.h3_cell);
        append_checked(count_builder, entry.modified_deleted);
    }

    std::shared_ptr<arrow::Array> uid, username, minute, cell, count;
    finish_checked(uid_builder, &uid);
    finish_checked(username_builder, &username);
    finish_checked(minute_builder, &minute);
    finish_checked(cell_builder, &cell);
    finish_checked(count_builder, &count);

    arrow_table_io::write_table(path,
                                arrow::Table::Make(cells_stage_schema(),
                                                   {uid, username, minute, cell, count}));
}

}  // namespace

// ---------------------------------------------------------------------------
// NodeMoveSink
// ---------------------------------------------------------------------------

NodeMoveSink::NodeMoveSink(std::string stage_root) : stage_root_(std::move(stage_root)) {}

NodeMoveSink::~NodeMoveSink() {
    // Safety net only: run_update_mode calls finish_seq() after every diff, so
    // a pending flush here means an abnormal exit (the run's stage root is
    // wiped and re-scanned on the next run anyway).
    try {
        if (in_seq_) finish_seq();
    } catch (...) {
    }
}

void NodeMoveSink::start_seq(uint64_t seq) {
    if (in_seq_) finish_seq();
    in_seq_ = true;
    seq_ = seq;
    stage_files_ = 0;
    rows_in_seq_ = 0;
    // A rerun never reuses stale stage files for the same sequence.
    std::filesystem::remove_all(stage_root_ + "/seq_" + std::to_string(seq));
    std::filesystem::create_directories(stage_root_ + "/seq_" + std::to_string(seq));
}

void NodeMoveSink::record(int64_t uid, int64_t ts_seconds, uint64_t prev_cell,
                          double new_lat, double new_lon) {
    const auto [plat, plon] = h3_utils::cell_to_latlng(prev_cell);
    const double meters = h3_utils::haversine_m(plat, plon, new_lat, new_lon);
    if (meters <= kFilter3Threshold) return;
    uids_.push_back(uid);
    minutes_.push_back(h3_utils::timestamp_to_utc_minute(ts_seconds));
    meters_.push_back(static_cast<uint32_t>(meters));
    rows_in_seq_++;
    if (rows_in_seq_ >= kFlushThreshold) flush_pending();
}

void NodeMoveSink::finish_seq() {
    if (!in_seq_) return;
    flush_pending();
    in_seq_ = false;
}

void NodeMoveSink::flush_pending() {
    if (rows_in_seq_ == 0) return;
    char name[32];
    std::snprintf(name, sizeof(name), "stage_%05zu.parquet", stage_files_);
    write_moves_stage_file(stage_root_ + "/seq_" + std::to_string(seq_) + "/" + name,
                           rows_in_seq_, uids_, minutes_, meters_);
    stage_files_++;
    rows_in_seq_ = 0;
    uids_.clear();
    minutes_.clear();
    meters_.clear();
}

// ---------------------------------------------------------------------------
// Scan + finalize entry points
// ---------------------------------------------------------------------------

void run_scan_diff(const std::string& diff_path, const std::string& stage_dir) {
    std::filesystem::remove_all(stage_dir);  // a rerun never reuses stale stage files
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(diff_path);  // .osc.gz -> format+compression from extension
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    SuspectScanHandler handler(stage_dir);
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();

    std::cerr << "[suspect] diff scan objects=" << handler.objects()
              << " stage_rows=" << handler.stage_rows()
              << " stage_files=" << handler.stage_files() << "\n";
}

// ---------------------------------------------------------------------------
// Filter 4: Cell scan
// ---------------------------------------------------------------------------

namespace {

class SuspectCellScanHandler : public osmium::handler::Handler {
public:
    explicit SuspectCellScanHandler(std::string stage_dir, int h3_resolution,
                                    const ::update_pass::NodeState& node_state)
        : stage_dir_(std::move(stage_dir)), h3_resolution_(h3_resolution),
          node_state_(node_state) {}

    void node(const osmium::Node& node) {
        if (!node.location().valid()) return;
        uint64_t cell = h3_utils::location_to_cell(node.location().lat(), node.location().lon(), h3_resolution_);
        add_node(static_cast<int64_t>(node.uid()), object_user(node),
                 node.timestamp(), cell, node.visible(), static_cast<uint32_t>(node.version()));
    }

    void way(const osmium::Way& way) {
        // Collect distinct H3 cells of way's nodes via NodeState.
        // Visible ways use post() (new geometry), deleted ways use pre() (last known).
        std::vector<uint64_t> cells;
        cells.reserve(way.nodes().size());
        for (const auto& wn : way.nodes()) {
            uint64_t cell = way.visible() ? node_state_.post(wn.ref()) : node_state_.pre(wn.ref());
            if (cell == 0) continue;
            cells.push_back(cell);
        }
        // Deduplicate: way counts once per cell (sort+unique is O(n log n) vs O(n^2) for find)
        if (!cells.empty()) {
            std::sort(cells.begin(), cells.end());
            cells.erase(std::unique(cells.begin(), cells.end()), cells.end());
            add_way(static_cast<int64_t>(way.uid()), object_user(way),
                    way.timestamp(), cells, way.visible(), static_cast<uint32_t>(way.version()));
        }
    }

    void relation(const osmium::Relation&) {
        // Skip relations per Filter 4 design (relations don't have direct geometry)
        skipped_relations_++;
    }

    void finish() { flush_stage(); }

    // INSTR
    uint64_t objects() const { return objects_; }
    size_t stage_files() const { return stage_files_; }
    size_t stage_rows() const { return stage_rows_flushed_; }
    uint64_t skipped_relations() const { return skipped_relations_; }

private:
    static std::string object_user(const osmium::OSMObject& object) {
        const char* user = object.user();
        return std::string(user ? user : "");
    }

    void add_node(int64_t uid, const std::string& username, const osmium::Timestamp& ts,
                  uint64_t cell, bool visible, uint32_t version) {
        objects_++;
        stats_.add_node(uid, username, h3_utils::timestamp_to_utc_minute(ts.seconds_since_epoch()),
                        cell, visible, version);
        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void add_way(int64_t uid, const std::string& username, const osmium::Timestamp& ts,
                 const std::vector<uint64_t>& cells, bool visible, uint32_t version) {
        objects_++;
        uint32_t minute = h3_utils::timestamp_to_utc_minute(ts.seconds_since_epoch());
        stats_.add_way(uid, username, minute, cells, visible, version);
        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void flush_stage() {
        if (stats_.cells().empty()) return;
        char name[32];
        std::snprintf(name, sizeof(name), "stage_%05zu.parquet", stage_files_);
        write_cells_stage_file(stage_dir_ + "/" + name, stats_.cells());
        stage_rows_flushed_ += stats_.size();
        stats_.clear();
        stage_files_++;
    }

    std::string stage_dir_;
    int h3_resolution_;
    const ::update_pass::NodeState& node_state_;
    MinuteCellStats stats_;

    // INSTR
    uint64_t objects_ = 0;
    size_t stage_files_ = 0;
    size_t stage_rows_flushed_ = 0;
    uint64_t skipped_relations_ = 0;
};

}  // namespace

void run_scan_diff_cells(const std::string& diff_path, const std::string& stage_dir,
                         int h3_resolution, const ::update_pass::NodeState& node_state) {
    std::filesystem::remove_all(stage_dir);  // a rerun never reuses stale stage files
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    SuspectCellScanHandler handler(stage_dir, h3_resolution, node_state);
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();

    std::cerr << "[suspect] cell diff scan objects=" << handler.objects()
              << " stage_rows=" << handler.stage_rows()
              << " stage_files=" << handler.stage_files()
              << " skipped_relations=" << handler.skipped_relations() << "\n";
}

void fold_minute_counts(const std::string& counts_root, const std::string& minutes_path,
                        uint64_t applied_seq) {
    const std::vector<std::string> count_stage = collect_parquet_recursive(counts_root);
    const bool have_base = std::filesystem::exists(minutes_path);

    // A base store already stamped with a sequence >= the run's means a
    // previous run folded the same diffs (crash between rename and stage
    // cleanup); skip instead of double-counting. applied_seq == 0 has no
    // meaningful stamp.
    if (have_base && applied_seq > 0 &&
        suspect_store::applied_seq_of(minutes_path) >= applied_seq) {
        std::cerr << "[suspect] minute buckets already folded through " << applied_seq
                  << ", skipping\n";
        std::filesystem::remove_all(counts_root);
        return;
    }
    if (count_stage.empty()) {
        std::cerr << "[suspect] no minute bucket stage files under " << counts_root
                  << ", nothing to fold\n";
        std::filesystem::remove_all(counts_root);
        return;
    }

    // The store always holds a prefix of the replication stream: it is the
    // merge base of every fold, so any staged sequence already covered by the
    // store's stamp was folded by an earlier (possibly crashed-before-manifest)
    // run. Re-scanning those diffs re-stages them, so skip them here rather
    // than summing them into the store a second time. Merging then only
    // advances the store past its stamp, keeping it cumulative.
    const uint64_t base_seq =
        have_base ? suspect_store::applied_seq_of(minutes_path) : 0;

    // Merge base + every newly staged diff, summing equal (uid, minute) keys.
    // The map hands the writer its guaranteed (uid, minute) ascending order.
    std::map<std::pair<int64_t, uint32_t>, uint32_t> merged;
    if (have_base) {
        suspect_store::Reader base(minutes_path);
        for (size_t i = 0; i < base.size(); ++i) {
            merged[{base.uid_at(i), base.minute_at(i)}] += base.count_at(i);
        }
    }
    for (const std::string& path : count_stage) {
        if (base_seq > 0 && staged_seq(path) <= base_seq) {
            std::cerr << "[suspect] stage " << path
                      << " already folded (seq <= " << base_seq << "), skipping\n";
            continue;
        }
        const std::shared_ptr<arrow::Table> table = combine_chunks(
            arrow_table_io::read_table(path));
        const auto* uids = typed_column<arrow::Int64Array>(table, "uid");
        const auto* minutes = typed_column<arrow::UInt32Array>(table, "minute");
        const auto* counts = typed_column<arrow::UInt32Array>(table, "modified_deleted");
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            merged[{uids->Value(i), minutes->Value(i)}] += counts->Value(i);
        }
    }

    suspect_store::Writer writer(minutes_path);
    writer.set_applied_seq(applied_seq);
    for (const auto& [key, count] : merged) {
        writer.add(key.first, key.second, count);
    }
    writer.finish();
    std::cerr << "[suspect] folded " << merged.size() << " minute buckets into "
              << minutes_path << "\n";
    std::filesystem::remove_all(counts_root);
}

// ---------------------------------------------------------------------------
// Filter 4: Fold cell counts
// ---------------------------------------------------------------------------

void fold_cell_counts(const std::string& cells_root, const std::string& cells_path,
                      uint64_t applied_seq, int h3_resolution) {
    const std::vector<std::string> cell_stage = collect_parquet_recursive(cells_root);
    const bool have_base = std::filesystem::exists(cells_path);

    // A base store already stamped with a sequence >= the run's means a
    // previous run folded the same diffs; skip instead of double-counting.
    if (have_base && applied_seq > 0 &&
        suspect_cell_store::applied_seq_of(cells_path) >= applied_seq) {
        std::cerr << "[suspect] cell buckets already folded through " << applied_seq
                  << ", skipping\n";
        std::filesystem::remove_all(cells_root);
        return;
    }
    if (cell_stage.empty()) {
        std::cerr << "[suspect] no cell bucket stage files under " << cells_root
                  << ", nothing to fold\n";
        std::filesystem::remove_all(cells_root);
        return;
    }

    const uint64_t base_seq =
        have_base ? suspect_cell_store::applied_seq_of(cells_path) : 0;

    // Merge base + every newly staged diff, summing equal (uid, minute, h3_cell) keys.
    // The map hands the writer its guaranteed (uid, minute, h3_cell) ascending order.
    std::map<std::tuple<int64_t, uint32_t, uint64_t>, uint32_t> merged;
    if (have_base) {
        suspect_cell_store::Reader base(cells_path, h3_resolution);
        for (size_t i = 0; i < base.size(); ++i) {
            merged[{base.uid_at(i), base.minute_at(i), base.cell_at(i)}] += base.count_at(i);
        }
    }
    for (const std::string& path : cell_stage) {
        if (base_seq > 0 && staged_seq(path) <= base_seq) {
            std::cerr << "[suspect] cell stage " << path
                      << " already folded (seq <= " << base_seq << "), skipping\n";
            continue;
        }
        const std::shared_ptr<arrow::Table> table = combine_chunks(
            arrow_table_io::read_table(path));
        const auto* uids = typed_column<arrow::Int64Array>(table, "uid");
        const auto* minutes = typed_column<arrow::UInt32Array>(table, "minute");
        const auto* cells = typed_column<arrow::UInt64Array>(table, "h3_cell");
        const auto* counts = typed_column<arrow::UInt32Array>(table, "modified_deleted");
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            merged[{uids->Value(i), minutes->Value(i), cells->Value(i)}] += counts->Value(i);
        }
    }

    suspect_cell_store::Writer writer(cells_path, h3_resolution);
    writer.set_applied_seq(applied_seq);
    for (const auto& [key, count] : merged) {
        writer.add(std::get<0>(key), std::get<1>(key), std::get<2>(key), count);
    }
    writer.finish();
    std::cerr << "[suspect] folded " << merged.size() << " cell buckets into "
              << cells_path << "\n";
    std::filesystem::remove_all(cells_root);
}

std::map<std::pair<int64_t, uint16_t>, FilterDay> flagged_days(const std::string& minutes_path) {
    std::map<std::pair<int64_t, uint16_t>, FilterDay> days;
    if (!std::filesystem::exists(minutes_path)) return days;
    suspect_store::Reader reader(minutes_path);

    // The store is grouped by uid in (uid, minute) order, so each uid's series
    // is contiguous; hour_spans needs no more than that.
    std::vector<std::pair<uint32_t, uint32_t>> series;
    int64_t cur_uid = 0;
    bool have_uid = false;
    const auto flush = [&]() {
        if (!have_uid) return;
        for (const MinuteSpanRow& row : hour_spans(series)) {
            if (!row.flagged) continue;
            const int64_t day = static_cast<int64_t>(row.minute) / 1440;
            if (day > h3_utils::kMaxUint16Day) {
                throw std::runtime_error("Minute bucket day out of uint16 range");
            }
            FilterDay& entry = days[{cur_uid, static_cast<uint16_t>(day)}];
            entry.flags = kFlagFilter2;
            entry.max_edits_per_hour = std::max(entry.max_edits_per_hour, row.hour_span);
        }
        series.clear();
    };
    for (size_t i = 0; i < reader.size(); ++i) {
        const int64_t uid = reader.uid_at(i);
        if (have_uid && uid != cur_uid) flush();
        cur_uid = uid;
        have_uid = true;
        series.emplace_back(reader.minute_at(i), reader.count_at(i));
    }
    flush();
    return days;
}

std::map<std::pair<int64_t, uint16_t>, MoveDay> flagged_move_days(
    const std::string& stage_root) {
    std::map<std::pair<int64_t, uint16_t>, MoveDay> days;

    const std::string moves_root = stage_root + "/moves";
    const std::vector<std::string> move_stage = collect_parquet_recursive(moves_root);
    // A move flag is a monotonic day-level bit: staged rows can only turn it
    // on, so a rerun re-folding its regenerated stages ORs the same flags and
    // nothing needs the applied-sequence machinery of fold_minute_counts.
    for (const std::string& path : move_stage) {
        const std::shared_ptr<arrow::Table> table = combine_chunks(
            arrow_table_io::read_table(path));
        const auto* uids = typed_column<arrow::Int64Array>(table, "uid");
        const auto* minutes = typed_column<arrow::UInt32Array>(table, "minute");
        const auto* meters = typed_column<arrow::UInt32Array>(table, "meters");
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            const int64_t day = static_cast<int64_t>(minutes->Value(i)) / 1440;
            if (day > h3_utils::kMaxUint16Day) {
                throw std::runtime_error("Move minute day out of uint16 range");
            }
            MoveDay& entry = days[{uids->Value(i), static_cast<uint16_t>(day)}];
            entry.flags = static_cast<uint8_t>(entry.flags | kFlagFilter3);
            entry.far_move_count++;
            entry.max_move_meters = std::max(entry.max_move_meters, meters->Value(i));
        }
    }
    if (!move_stage.empty()) {
        std::cerr << "[suspect] folded " << days.size() << " move-flagged days\n";
    }

    // The run's staging is now folded into the day flags; the counts/ sub-tree
    // was already consumed by fold_minute_counts, so remove the whole root.
    std::filesystem::remove_all(stage_root);
    return days;
}

// ---------------------------------------------------------------------------
// Filter 4: cell spread flag
// ---------------------------------------------------------------------------

CellSpreadAccumulator::CellSpreadAccumulator(double cell_area_km2)
    : cell_area_km2_(cell_area_km2) {}

void CellSpreadAccumulator::evict(uint32_t minute) {
    // Evict object rows older than the window
    while (!object_deque_.empty() && object_deque_.front().first + kHourSpanMinutes <= minute) {
        object_total_ -= object_deque_.front().second;
        object_deque_.pop_front();
    }
    // Evict cell rows older than the window
    while (!cell_deque_.empty() && std::get<0>(cell_deque_.front()) + kHourSpanMinutes <= minute) {
        const auto& front = cell_deque_.front();
        uint64_t cell = std::get<1>(front);
        uint32_t count = std::get<2>(front);
        auto it = cell_counts_.find(cell);
        if (it != cell_counts_.end()) {
            if (it->second <= count) {
                cell_counts_.erase(it);
            } else {
                it->second -= count;
            }
        }
        cell_deque_.pop_front();
    }
}

void CellSpreadAccumulator::add_cell(uint32_t minute, uint64_t cell, uint32_t count) {
    cell_deque_.emplace_back(minute, cell, count);
    cell_counts_[cell] += count;
}

void CellSpreadAccumulator::add_object(uint32_t minute, uint32_t count) {
    object_deque_.emplace_back(minute, count);
    object_total_ += count;
}

CellSpreadRow CellSpreadAccumulator::evaluate(uint32_t minute) const {
    uint32_t distinct = static_cast<uint32_t>(cell_counts_.size());
    double spread = distinct * cell_area_km2_;
    uint8_t flagged = (object_total_ >= kFilter4MinCount &&
                       distinct >= kFilter4MinCells &&
                       spread >= kFilter4SpreadKm2) ? kFlagFilter4 : 0;
    return {minute, static_cast<uint32_t>(object_total_), distinct, spread, flagged};
}

std::map<std::pair<int64_t, uint16_t>, CellSpreadDay> flagged_cell_days(
    const std::string& cells_path, const std::string& minutes_path, int h3_resolution) {
    std::map<std::pair<int64_t, uint16_t>, CellSpreadDay> days;
    if (!std::filesystem::exists(cells_path) || !std::filesystem::exists(minutes_path)) {
        return days;
    }
    suspect_cell_store::Reader cell_reader(cells_path, h3_resolution);
    suspect_store::Reader minute_reader(minutes_path);

    // Both stores are sorted by (uid, minute, ...), so we can walk them in lockstep.
    // A uid must appear in BOTH stores to be evaluated: the cell store provides the
    // distinct H3 cell count for the spread metric, and the minute store provides
    // the true edit count for the gate. If a uid is missing from either store,
    // it cannot produce a Filter 4 flag and is skipped.
    size_t cell_i = 0, minute_i = 0;
    while (cell_i < cell_reader.size() && minute_i < minute_reader.size()) {
        int64_t cell_uid = cell_reader.uid_at(cell_i);
        int64_t minute_uid = minute_reader.uid_at(minute_i);
        if (cell_uid < minute_uid) {
            // Skip this uid's cell rows (no minute data to pair with)
            while (cell_i < cell_reader.size() && cell_reader.uid_at(cell_i) == cell_uid) {
                ++cell_i;
            }
            continue;
        }
        if (minute_uid < cell_uid) {
            // Skip this uid's minute rows (no cell data)
            while (minute_i < minute_reader.size() && minute_reader.uid_at(minute_i) == minute_uid) {
                ++minute_i;
            }
            continue;
        }
        // Same uid: collect all cell rows and minute rows for this uid.
        int64_t uid = cell_uid;
        std::vector<CellCountRow> cell_rows;
        std::vector<std::pair<uint32_t, uint32_t>> obj_rows;
        while (cell_i < cell_reader.size() && cell_reader.uid_at(cell_i) == uid) {
            cell_rows.push_back({cell_reader.minute_at(cell_i), cell_reader.cell_at(cell_i), cell_reader.count_at(cell_i)});
            ++cell_i;
        }
        while (minute_i < minute_reader.size() && minute_reader.uid_at(minute_i) == uid) {
            obj_rows.emplace_back(minute_reader.minute_at(minute_i), minute_reader.count_at(minute_i));
            ++minute_i;
        }
        double cell_area = h3_utils::average_hexagon_area_km2(h3_resolution);
        auto spreads = cell_spreads(cell_rows, obj_rows, cell_area);
        for (const auto& s : spreads) {
            if (s.flagged) {
                int64_t day = static_cast<int64_t>(s.minute) / 1440;
                if (day > h3_utils::kMaxUint16Day) {
                    throw std::runtime_error("Cell spread minute day out of uint16 range");
                }
                CellSpreadDay& entry = days[{uid, static_cast<uint16_t>(day)}];
                entry.flags = kFlagFilter4;
                if (s.spread_km2 > entry.spread_km2) entry.spread_km2 = s.spread_km2;
            }
        }
    }
    return days;
}

// ---------------------------------------------------------------------------
// Filter 5: tag coverage flag
// ---------------------------------------------------------------------------

TagCoverageAccumulator::TagCoverageAccumulator() = default;

void TagCoverageAccumulator::evict(uint32_t minute) {
    while (!object_deque_.empty() && object_deque_.front().first + kHourSpanMinutes <= minute) {
        object_total_ -= object_deque_.front().second;
        object_deque_.pop_front();
    }
    while (!tag_deque_.empty() && std::get<0>(tag_deque_.front()) + kHourSpanMinutes <= minute) {
        const auto& front = tag_deque_.front();
        const std::string& key = std::get<1>(front);
        uint32_t count = std::get<2>(front);
        auto it = key_counts_.find(key);
        if (it != key_counts_.end()) {
            if (it->second <= count) {
                key_counts_.erase(it);
            } else {
                it->second -= count;
            }
        }
        tag_deque_.pop_front();
    }
}

void TagCoverageAccumulator::add_tag(uint32_t minute, std::string_view tag_key, uint32_t count) {
    tag_deque_.emplace_back(minute, std::string(tag_key), count);
    key_counts_[std::string(tag_key)] += count;
}

void TagCoverageAccumulator::add_object(uint32_t minute, uint32_t count) {
    object_deque_.emplace_back(minute, count);
    object_total_ += count;
}

TagCoverageRow TagCoverageAccumulator::evaluate(uint32_t minute) const {
    // An unflagged row's keys are never read, so a window that cannot clear the
    // minimum object count skips the scan entirely; that is the common case on
    // a quiet day and this runs per minute over the whole persisted store.
    std::vector<std::string> keys;
    if (object_total_ >= kFilter5MinObjects) {
        // Every key over the coverage threshold is listed, not just the
        // strongest: a mass edit spreads across many similar keys and a day can
        // trip on any one of them. The candidates are collected as pointers
        // into key_counts_ so the ordering below moves no strings, then copied
        // out once; sorting also keeps the reported set independent of the
        // unordered_map's iteration order.
        std::vector<const std::string*> over;
        for (const auto& [key, count] : key_counts_) {
            if (static_cast<double>(count) / static_cast<double>(object_total_) >
                kFilter5TagCoverage) {
                over.push_back(&key);
            }
        }
        std::sort(over.begin(), over.end(),
                  [](const std::string* a, const std::string* b) { return *a < *b; });
        keys.reserve(over.size());
        for (const std::string* key : over) keys.push_back(*key);
    }
    uint8_t flagged = 0;
    if (object_total_ >= kFilter5MinObjects && !keys.empty()) {
        flagged = kFlagFilter5;
    }
    return {minute, static_cast<uint32_t>(object_total_), flagged, std::move(keys)};
}

std::vector<TagCoverageRow> tag_coverages(
    const std::vector<std::tuple<uint32_t, std::string, uint32_t>>& tag_rows,
    const std::vector<std::pair<uint32_t, uint32_t>>& object_counts) {
    std::vector<TagCoverageRow> out;
    out.reserve(tag_rows.size() + object_counts.size());
    TagCoverageAccumulator acc;
    size_t obj_idx = 0;
    size_t row_idx = 0;
    while (row_idx < tag_rows.size() || obj_idx < object_counts.size()) {
        uint32_t m;
        if (row_idx >= tag_rows.size()) {
            m = object_counts[obj_idx].first;
        } else if (obj_idx >= object_counts.size()) {
            m = std::get<0>(tag_rows[row_idx]);
        } else {
            m = std::min(std::get<0>(tag_rows[row_idx]), object_counts[obj_idx].first);
        }
        while (obj_idx < object_counts.size() && object_counts[obj_idx].first <= m) {
            acc.add_object(object_counts[obj_idx].first, object_counts[obj_idx].second);
            ++obj_idx;
        }
        while (row_idx < tag_rows.size() && std::get<0>(tag_rows[row_idx]) == m) {
            acc.add_tag(std::get<0>(tag_rows[row_idx]), std::get<1>(tag_rows[row_idx]),
                        std::get<2>(tag_rows[row_idx]));
            ++row_idx;
        }
        acc.evict(m);
        out.push_back(acc.evaluate(m));
    }
    return out;
}

std::vector<CellSpreadRow> cell_spreads(
    const std::vector<CellCountRow>& rows,
    const std::vector<std::pair<uint32_t, uint32_t>>& object_counts,
    double cell_area_km2) {
    std::vector<CellSpreadRow> out;
    out.reserve(rows.size() + object_counts.size());
    CellSpreadAccumulator acc(cell_area_km2);
    size_t obj_idx = 0;
    size_t row_idx = 0;
    while (row_idx < rows.size() || obj_idx < object_counts.size()) {
        uint32_t m;
        if (row_idx >= rows.size()) {
            m = object_counts[obj_idx].first;
        } else if (obj_idx >= object_counts.size()) {
            m = rows[row_idx].minute;
        } else {
            m = std::min(rows[row_idx].minute, object_counts[obj_idx].first);
        }
        // Feed every object row through minute m, then every cell row of minute
        // m, so the window state read below covers both through m inclusive.
        while (obj_idx < object_counts.size() && object_counts[obj_idx].first <= m) {
            acc.add_object(object_counts[obj_idx].first, object_counts[obj_idx].second);
            ++obj_idx;
        }
        while (row_idx < rows.size() && rows[row_idx].minute == m) {
            acc.add_cell(rows[row_idx].minute, rows[row_idx].h3_cell, rows[row_idx].modified_deleted);
            ++row_idx;
        }
        // Evict after the adds, so a row admitted during this step that predates
        // the window is dropped before the window state is read.
        acc.evict(m);
        out.push_back(acc.evaluate(m));
    }
    return out;
}

std::map<std::pair<int64_t, uint16_t>, TagCoverageDay> flagged_tag_days(
    const std::string& tags_path, const std::string& minutes_path) {
    std::map<std::pair<int64_t, uint16_t>, TagCoverageDay> days;
    if (!std::filesystem::exists(tags_path) || !std::filesystem::exists(minutes_path)) {
        return days;
    }
    suspect_tag_store::Reader tag_reader(tags_path);
    suspect_store::Reader minute_reader(minutes_path);

    // Both stores are sorted by (uid, minute, ...), walk them in lockstep.
    // A uid must appear in BOTH stores to be evaluated. If missing from either,
    // it cannot produce a Filter 5 flag and is skipped.
    size_t tag_i = 0, minute_i = 0;
    while (tag_i < tag_reader.size() && minute_i < minute_reader.size()) {
        int64_t tag_uid = tag_reader.uid_at(tag_i);
        int64_t minute_uid = minute_reader.uid_at(minute_i);
        if (tag_uid < minute_uid) {
            while (tag_i < tag_reader.size() && tag_reader.uid_at(tag_i) == tag_uid) {
                ++tag_i;
            }
            continue;
        }
        if (minute_uid < tag_uid) {
            while (minute_i < minute_reader.size() && minute_reader.uid_at(minute_i) == minute_uid) {
                ++minute_i;
            }
            continue;
        }
        // Same uid: collect all tag rows and minute rows for this uid.
        int64_t uid = tag_uid;
        std::vector<std::tuple<uint32_t, std::string, uint32_t>> tag_rows;
        std::vector<std::pair<uint32_t, uint32_t>> obj_rows;
        while (tag_i < tag_reader.size() && tag_reader.uid_at(tag_i) == uid) {
            tag_rows.emplace_back(tag_reader.minute_at(tag_i),
                                  tag_reader.tag_key_at(tag_i),
                                  tag_reader.count_at(tag_i));
            ++tag_i;
        }
        while (minute_i < minute_reader.size() && minute_reader.uid_at(minute_i) == uid) {
            obj_rows.emplace_back(minute_reader.minute_at(minute_i), minute_reader.count_at(minute_i));
            ++minute_i;
        }
        auto spreads = tag_coverages(tag_rows, obj_rows);
        for (const auto& s : spreads) {
            if (s.flagged) {
                int64_t day = static_cast<int64_t>(s.minute) / 1440;
                if (day > h3_utils::kMaxUint16Day) {
                    throw std::runtime_error("Tag coverage minute day out of uint16 range");
                }
                // A mass edit spreads its keys over many qualifying windows of
                // the same day, so the day collects their union rather than
                // one window's keys.
                TagCoverageDay& entry = days[{uid, static_cast<uint16_t>(day)}];
                entry.flags = kFlagFilter5;
                entry.tags.insert(s.keys.begin(), s.keys.end());
            }
        }
    }
    return days;
}

}  // namespace suspect