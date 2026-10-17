#include "update.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/io/xml_output.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/visitor.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "h3_utils.hpp"
#include "object_tag_state.hpp"
#include "partitioned_parquet_writer.hpp"
#include "users_history.hpp"

namespace update_pass {

namespace {

// Collects the timestamps of every object of a diff buffer, feeding the
// batch's time-spread bound (kUpdateBatchMaxSeconds).
class TimestampTracker : public osmium::handler::Handler {
public:
    void node(const osmium::Node& n) { update(n); }
    void way(const osmium::Way& w) { update(w); }
    void relation(const osmium::Relation& r) { update(r); }

    bool any() const { return any_; }
    int64_t min_ts = std::numeric_limits<int64_t>::max();
    int64_t max_ts = std::numeric_limits<int64_t>::min();

private:
    template <typename T>
    void update(const T& object) {
        const int64_t ts = object.timestamp().seconds_since_epoch();
        if (ts < min_ts) min_ts = ts;
        if (ts > max_ts) max_ts = ts;
        any_ = true;
    }

    bool any_ = false;
};

class NodeUpdateHandler : public osmium::handler::Handler {
public:
    NodeUpdateHandler(NodeState* state,
                      parquet_out::PartitionedParquetWriter* parquet_writer,
                      int h3_resolution, suspect::NodeMoveSink* moves)
        : state_(state),
          parquet_writer_(parquet_writer),
          h3_resolution_(h3_resolution),
          moves_(moves) {}

    void node(const osmium::Node& n) {
        touched_++;  // INSTR
        const int64_t ts = n.timestamp().seconds_since_epoch();
        const int32_t day = h3_utils::timestamp_to_utc_day(ts);

        if (n.location().valid()) {
            const double lat = n.location().lat();
            const double lon = n.location().lon();
            const uint64_t cell = h3_utils::location_to_cell(lat, lon, h3_resolution_);

            // Suspect filter 3: a modification (version > 1) with a known
            // prior cell is a candidate move. Capture the pre-update cell
            // BEFORE set_position folds the new one into the overlay.
            if (moves_ && n.visible() && n.version() > 1) {
                const uint64_t prev = state_->pre(n.id());
                if (prev != 0) {
                    moves_->record(n.uid(), ts, prev, lat, lon);
                }
            }

            state_->set_position(n.id(), cell);
            parquet_writer_->increment(cell, day);
            positions_++;  // INSTR

        } else if (!n.visible()) {
            // Deletion: counted on the last known position.
            const uint64_t cell = state_->remove_node(n.id());
            if (cell != 0) {
                parquet_writer_->increment(cell, day);
            }
            deleted_++;  // INSTR
        }
    }

    // INSTR
    uint64_t touched() const { return touched_; }
    uint64_t positions() const { return positions_; }
    uint64_t deleted() const { return deleted_; }

private:
    NodeState* state_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;
    int h3_resolution_;
    suspect::NodeMoveSink* moves_;

    // INSTR: diagnostic counters.
    uint64_t touched_ = 0;
    uint64_t positions_ = 0;
    uint64_t deleted_ = 0;
};

// Resolves way node refs in batches: refs are accumulated until the batch
// exceeds `way_batch_bytes`, then resolved with a single forward sweep over
// the base cache (prefetch_base) instead of a random lookup per ref. A way
// never spans two batches, so counting stays identical to the streaming pass.
class WayUpdateHandler : public osmium::handler::Handler {
public:
    WayUpdateHandler(const NodeState* state,
                     parquet_out::PartitionedParquetWriter* parquet_writer,
                     size_t way_batch_bytes)
        : state_(state),
          parquet_writer_(parquet_writer),
          batch_max_refs_(std::max<size_t>(1, way_batch_bytes / sizeof(int64_t))) {}

    void way(const osmium::Way& w) {
        touched_++;  // INSTR
        const int64_t ts = w.timestamp().seconds_since_epoch();
        const uint16_t day = h3_utils::require_u16_day(h3_utils::timestamp_to_utc_day(ts));

        if (w.nodes().empty()) {
            return;
        }
        // Visible ways resolve against the post-update view; deleted ways
        // against the pre-update view (their last known geometry).
        way_days_.push_back(day);
        way_visible_.push_back(w.visible() ? 1u : 0u);
        ref_starts_.push_back(refs_.size());
        for (const auto& nr : w.nodes()) {
            refs_.push_back(nr.ref());
        }
        if (refs_.size() >= batch_max_refs_) {
            flush_batch();
        }
    }

    void finish() { flush_batch(); }

    // INSTR
    uint64_t touched() const { return touched_; }
    uint64_t points() const { return points_; }
    uint64_t batches() const { return batches_; }
    uint64_t scanned() const { return scanned_; }

private:
    void flush_batch() {
        if (refs_.empty()) {
            return;
        }

        std::vector<int64_t> nodes = refs_;
        std::sort(nodes.begin(), nodes.end());
        nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
        scanned_ += state_->prefetch_base(nodes);

        for (size_t s = 0; s < way_days_.size(); ++s) {
            const size_t begin = ref_starts_[s];
            const size_t end = (s + 1 < ref_starts_.size()) ? ref_starts_[s + 1] : refs_.size();
            const bool pre = way_visible_[s] == 0;

            std::vector<uint64_t> cells;
            cells.reserve(end - begin);
            for (size_t i = begin; i < end; ++i) {
                const uint64_t cell = pre ? state_->pre(refs_[i]) : state_->post(refs_[i]);
                if (cell != 0) cells.push_back(cell);
            }
            if (cells.empty()) {
                continue;
            }
            std::sort(cells.begin(), cells.end());
            cells.erase(std::unique(cells.begin(), cells.end()), cells.end());
            for (uint64_t cell : cells) {
                parquet_writer_->increment(cell, way_days_[s]);
                points_++;  // INSTR
            }
        }

        way_days_.clear();
        way_visible_.clear();
        ref_starts_.clear();
        refs_.clear();
        batches_++;  // INSTR
    }

    const NodeState* state_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;
    size_t batch_max_refs_;

    std::vector<uint16_t> way_days_;
    std::vector<uint8_t> way_visible_;
    std::vector<size_t> ref_starts_;
    std::vector<int64_t> refs_;

    // INSTR: diagnostic counters.
    uint64_t touched_ = 0;
    uint64_t points_ = 0;
    uint64_t batches_ = 0;
    uint64_t scanned_ = 0;
};

}  // namespace

DiffMerger::DiffMerger() = default;

DiffMerger::~DiffMerger() {
    try {
        finish();
    } catch (...) {
    }
}

void DiffMerger::open(const std::string& out_path) {
    finish();
    const std::filesystem::path dir = std::filesystem::path(out_path).parent_path();
    if (!dir.empty()) std::filesystem::create_directories(dir);
    osmium::io::File out_file(out_path);
    osmium::io::Header header;
    header.set("generator", "karmamap");
    writer_ = std::make_unique<osmium::io::Writer>(out_file, header,
                                                   osmium::io::overwrite::allow);
    min_ts_ = 0;
    max_ts_ = 0;
    has_ts_ = false;
}

void DiffMerger::add(const std::string& diff_path) {
    if (!writer_) {
        throw std::logic_error("DiffMerger::add called before open()");
    }
    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file);
    TimestampTracker tracker;
    while (osmium::memory::Buffer buffer = reader.read()) {
        osmium::apply(buffer, tracker);
        (*writer_)(std::move(buffer));
    }
    reader.close();
    if (tracker.any()) {
        if (!has_ts_ || tracker.min_ts < min_ts_) min_ts_ = tracker.min_ts;
        if (!has_ts_ || tracker.max_ts > max_ts_) max_ts_ = tracker.max_ts;
        has_ts_ = true;
    }
}

void DiffMerger::finish() {
    if (!writer_) return;
    writer_->close();
    writer_.reset();
}

void run_node_update(const std::string& diff_path, const std::string& changes_root,
                     uint64_t seq, int h3_resolution, NodeState* state,
                     suspect::NodeMoveSink* moves) {
    std::cerr << "[update node pass] " << diff_path << "\n";
    const auto t0 = std::chrono::steady_clock::now();  // INSTR

    parquet_out::PartitionedParquetWriter writer(changes_root,
                                                 "nodes." + std::to_string(seq) + ".parquet");
    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::node);
    NodeUpdateHandler handler(state, &writer, h3_resolution, moves);

    while (osmium::memory::Buffer buffer = reader.read()) {
        osmium::apply(buffer, handler);
    }
    reader.close();
    writer.finish();

    const double elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - t0)
                                  .count();  // INSTR
    std::cerr << "[update node pass] touched=" << handler.touched()
              << " positions=" << handler.positions() << " deletions=" << handler.deleted()
              << " incremental=" << static_cast<int64_t>(state->base_records())
                                      + static_cast<int64_t>(state->overlay_size())
                                      - static_cast<int64_t>(state->deleted_size())
              << " (" << elapsed_ms << "ms) overlay=" << state->overlay_size()
              << " deleted=" << state->deleted_size() << "\n";
}

void run_way_update(const std::string& diff_path, const std::string& changes_root,
                    uint64_t seq, const NodeState& state, size_t way_batch_bytes) {
    std::cerr << "[update way pass] " << diff_path << "\n";
    const auto t0 = std::chrono::steady_clock::now();  // INSTR

    parquet_out::PartitionedParquetWriter writer(changes_root,
                                                 "ways." + std::to_string(seq) + ".parquet");
    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::way);
    WayUpdateHandler handler(&state, &writer, way_batch_bytes);

    while (osmium::memory::Buffer buffer = reader.read()) {
        osmium::apply(buffer, handler);
    }
    reader.close();
    handler.finish();
    writer.finish();

    const double elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - t0)
                                  .count();  // INSTR
    std::cerr << "[update way pass] touched=" << handler.touched()
              << " points=" << handler.points() << " batches=" << handler.batches()
              << " scanned=" << handler.scanned() << " (" << elapsed_ms << "ms)\n";
}

void run_diff_scans(const std::string& change_path, const std::string& changes_root,
                    uint64_t seq, size_t way_batch_bytes, int h3_resolution,
                    const NodeState& state, suspect::ObjectTagState* tag_state,
                    const std::string& history_stage, const std::string& counts_stage,
                    const std::string& cells_stage, const std::string& tags_stage,
                    const std::string& langs_stage) {
    std::cerr << "[update way pass] " << change_path << "\n";

    // A rerun never reuses stale stage files.
    std::filesystem::remove_all(history_stage);
    std::filesystem::remove_all(counts_stage);
    std::filesystem::remove_all(cells_stage);
    std::filesystem::remove_all(tags_stage);
    std::filesystem::remove_all(langs_stage);
    std::filesystem::create_directories(history_stage);
    std::filesystem::create_directories(counts_stage);
    std::filesystem::create_directories(cells_stage);
    std::filesystem::create_directories(tags_stage);
    std::filesystem::create_directories(langs_stage);

    const auto t0 = std::chrono::steady_clock::now();  // INSTR (whole shared read)
    parquet_out::PartitionedParquetWriter ways_writer(changes_root,
                                                      "ways." + std::to_string(seq) + ".parquet");
    WayUpdateHandler way_handler(&state, &ways_writer, way_batch_bytes);
    std::unique_ptr<DiffScanStep> history_step = users_history::make_scan_step(history_stage);
    std::unique_ptr<DiffScanStep> counts_step = suspect::make_counts_scan_step(counts_stage);
    std::unique_ptr<DiffScanStep> cells_step =
        suspect::make_cells_scan_step(cells_stage, h3_resolution, state);
    std::unique_ptr<DiffScanStep> tags_step = suspect::make_tags_scan_step(tags_stage);
    std::unique_ptr<DiffScanStep> langs_step =
        suspect::make_langs_scan_step(langs_stage, tag_state);

    osmium::io::File input_file(change_path);
    osmium::io::Reader reader(input_file,
                              osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                  osmium::osm_entity_bits::relation);
    while (osmium::memory::Buffer buffer = reader.read()) {
        osmium::apply(buffer, way_handler);
        history_step->apply(buffer);
        counts_step->apply(buffer);
        cells_step->apply(buffer);
        tags_step->apply(buffer);
        langs_step->apply(buffer);
    }
    reader.close();

    way_handler.finish();
    ways_writer.finish();
    const double elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - t0)
                                  .count();  // INSTR
    std::cerr << "[update way pass] touched=" << way_handler.touched()
              << " points=" << way_handler.points() << " batches=" << way_handler.batches()
              << " scanned=" << way_handler.scanned() << " (shared read " << elapsed_ms << "ms)\n";
    history_step->finish();
    counts_step->finish();
    cells_step->finish();
    tags_step->finish();
    langs_step->finish();
}

}  // namespace update_pass
