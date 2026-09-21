#include "update.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/visitor.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

#include "h3_utils.hpp"
#include "partitioned_parquet_writer.hpp"

namespace update_pass {

namespace {

class NodeUpdateHandler : public osmium::handler::Handler {
public:
    NodeUpdateHandler(NodeState* state,
                      parquet_out::PartitionedParquetWriter* parquet_writer,
                      int h3_resolution)
        : state_(state), parquet_writer_(parquet_writer), h3_resolution_(h3_resolution) {}

    void node(const osmium::Node& n) {
        touched_++;  // INSTR
        const int64_t ts = n.timestamp().seconds_since_epoch();
        const int32_t day = h3_utils::timestamp_to_utc_day(ts);

        if (n.location().valid()) {
            const double lat = n.location().lat();
            const double lon = n.location().lon();
            const uint64_t cell = h3_utils::location_to_cell(lat, lon, h3_resolution_);

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

    // INSTR: diagnostic counters.
    uint64_t touched_ = 0;
    uint64_t positions_ = 0;
    uint64_t deleted_ = 0;
};

class WayUpdateHandler : public osmium::handler::Handler {
public:
    WayUpdateHandler(const NodeState* state,
                     parquet_out::PartitionedParquetWriter* parquet_writer)
        : state_(state), parquet_writer_(parquet_writer) {}

    void way(const osmium::Way& w) {
        touched_++;  // INSTR
        const int64_t ts = w.timestamp().seconds_since_epoch();
        const uint16_t day = h3_utils::require_u16_day(h3_utils::timestamp_to_utc_day(ts));

        if (w.nodes().empty()) {
            return;
        }
        // Visible ways resolve against the post-update view; deleted ways
        // against the pre-update view (their last known geometry).
        const bool pre = !w.visible();
        std::vector<uint64_t> cells;
        for (const auto& nr : w.nodes()) {
            const uint64_t cell = pre ? state_->pre(nr.ref()) : state_->post(nr.ref());
            if (cell != 0) cells.push_back(cell);
        }
        if (cells.empty()) {
            return;
        }
        std::sort(cells.begin(), cells.end());
        cells.erase(std::unique(cells.begin(), cells.end()), cells.end());
        for (uint64_t cell : cells) {
            parquet_writer_->increment(cell, day);
            points_++;  // INSTR
        }
    }

    // INSTR
    uint64_t touched() const { return touched_; }
    uint64_t points() const { return points_; }

private:
    const NodeState* state_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;

    // INSTR: diagnostic counters.
    uint64_t touched_ = 0;
    uint64_t points_ = 0;
};

}  // namespace

void run_node_update(const std::string& diff_path, const std::string& changes_root,
                     uint64_t seq, int h3_resolution, NodeState* state) {
    std::cerr << "[update node pass] " << diff_path << "\n";
    const auto t0 = std::chrono::steady_clock::now();  // INSTR

    parquet_out::PartitionedParquetWriter writer(changes_root,
                                                 "nodes." + std::to_string(seq) + ".parquet");
    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::node);
    NodeUpdateHandler handler(state, &writer, h3_resolution);

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
              << " incremental=" << state->base_records() + state->overlay_size() - state->deleted_size()
              << " (" << elapsed_ms << "ms) overlay=" << state->overlay_size()
              << " deleted=" << state->deleted_size() << "\n";
}

void run_way_update(const std::string& diff_path, const std::string& changes_root,
                    uint64_t seq, const NodeState& state) {
    std::cerr << "[update way pass] " << diff_path << "\n";
    const auto t0 = std::chrono::steady_clock::now();  // INSTR

    parquet_out::PartitionedParquetWriter writer(changes_root,
                                                 "ways." + std::to_string(seq) + ".parquet");
    osmium::io::File input_file(diff_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::way);
    WayUpdateHandler handler(&state, &writer);

    while (osmium::memory::Buffer buffer = reader.read()) {
        osmium::apply(buffer, handler);
    }
    reader.close();
    writer.finish();

    const double elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - t0)
                                  .count();  // INSTR
    std::cerr << "[update way pass] touched=" << handler.touched()
              << " points=" << handler.points() << " (" << elapsed_ms << "ms)\n";
}

}  // namespace update_pass
