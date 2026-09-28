#include "suspect.hpp"

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
// Filter 3 stage I/O (per-(uid, minute) rows of moves > kFilter3Threshold)
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> moves_stage_schema() {
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("minute", arrow::uint32(), false),
    });
}

void write_moves_stage_file(const std::string& path, size_t n, const std::vector<int64_t>& uids,
                            const std::vector<uint32_t>& minutes) {
    if (n == 0) return;
    arrow::Int64Builder uid_builder;
    arrow::UInt32Builder minute_builder;

    if (!uid_builder.Reserve(static_cast<int64_t>(n)).ok() ||
        !minute_builder.Reserve(static_cast<int64_t>(n)).ok()) {
        throw std::runtime_error("Reserve() failed while flushing move stage");
    }
    for (size_t i = 0; i < n; ++i) {
        append_checked(uid_builder, uids[i]);
        append_checked(minute_builder, minutes[i]);
    }

    std::shared_ptr<arrow::Array> uid, minute;
    finish_checked(uid_builder, &uid);
    finish_checked(minute_builder, &minute);

    arrow_table_io::write_table(path,
                                arrow::Table::Make(moves_stage_schema(), {uid, minute}));
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
                           rows_in_seq_, uids_, minutes_);
    stage_files_++;
    rows_in_seq_ = 0;
    uids_.clear();
    minutes_.clear();
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
    explicit SuspectCellScanHandler(std::string stage_dir, int h3_resolution)
        : stage_dir_(std::move(stage_dir)), h3_resolution_(h3_resolution) {}

    void node(const osmium::Node& node) {
        if (!node.location().valid()) return;
        uint64_t cell = h3_utils::location_to_cell(node.location().lat(), node.location().lon(), h3_resolution_);
        add_node(static_cast<int64_t>(node.uid()), object_user(node),
                 node.timestamp(), cell, node.visible(), static_cast<uint32_t>(node.version()));
    }

    void way(const osmium::Way& way) {
        // Collect distinct H3 cells of way's nodes
        std::vector<uint64_t> cells;
        cells.reserve(way.nodes().size());
        for (const auto& wn : way.nodes()) {
            if (!wn.location().valid()) continue;
            uint64_t cell = h3_utils::location_to_cell(wn.location().lat(), wn.location().lon(), h3_resolution_);
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
    MinuteCellStats stats_;

    // INSTR
    uint64_t objects_ = 0;
    size_t stage_files_ = 0;
    size_t stage_rows_flushed_ = 0;
    uint64_t skipped_relations_ = 0;
};

}  // namespace

void run_scan_diff_cells(const std::string& diff_path, const std::string& stage_dir, int h3_resolution) {
    std::filesystem::remove_all(stage_dir);  // a rerun never reuses stale stage files
    std::filesystem::create_directories(stage_dir);

    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);

    SuspectCellScanHandler handler(stage_dir, h3_resolution);
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

std::map<std::pair<int64_t, uint16_t>, uint8_t> flagged_days(const std::string& minutes_path) {
    std::map<std::pair<int64_t, uint16_t>, uint8_t> days;
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
            days[{cur_uid, static_cast<uint16_t>(day)}] = kFlagFilter2;
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
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            const int64_t day = static_cast<int64_t>(minutes->Value(i)) / 1440;
            if (day > h3_utils::kMaxUint16Day) {
                throw std::runtime_error("Move minute day out of uint16 range");
            }
            MoveDay& entry = days[{uids->Value(i), static_cast<uint16_t>(day)}];
            entry.flags = static_cast<uint8_t>(entry.flags | kFlagFilter3);
            entry.far_move_count++;
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

}  // namespace suspect