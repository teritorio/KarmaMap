#pragma once

// Handler applied to the node stream of an .osh.pbf file (assumed sorted by
// (id, version) ascending). Nodes with coordinates are written to the node
// cache and counted; deletions are counted on the last known position.
// Counts go to the PartitionedParquetWriter; this handler only appends
// cache records.

#include <osmium/handler.hpp>
#include <osmium/osm/node.hpp>

#include "h3_utils.hpp"
#include "node_cache.hpp"
#include "partitioned_parquet_writer.hpp"

namespace node_pass {

class NodeCacheHandler : public osmium::handler::Handler {
public:
    NodeCacheHandler(node_cache::Writer* cache_writer,
                     parquet_out::PartitionedParquetWriter* parquet_writer, int h3_resolution)
        : cache_writer_(cache_writer), parquet_writer_(parquet_writer), h3_resolution_(h3_resolution) {}

    // Call after the full read to flush the writer (patching its header).
    void finish() { cache_writer_->finish(); }

    // INSTR
    uint64_t nodes() const { return nodes_; }
    uint64_t cache_writes() const { return cache_writes_; }

    void node(const osmium::Node& n) {
        nodes_++;  // INSTR
        if (n.id() != current_node_id_) {
            current_node_id_ = n.id();
            has_last_cell_ = false;
        }

        const int64_t ts = n.timestamp().seconds_since_epoch();
        const int32_t day = h3_utils::timestamp_to_utc_day(ts);
        const bool has_coords = n.location().valid();

        if (has_coords) {
            const double lat = n.location().lat();
            const double lon = n.location().lon();
            const uint64_t cell = h3_utils::location_to_cell(lat, lon, h3_resolution_);

            cache_writer_->add(n.id(), day, cell);
            cache_writes_++;  // INSTR

            last_cell_ = cell;
            has_last_cell_ = true;

            count_change(cell, day);

        } else if (!n.visible() && has_last_cell_) {
            // Deletion: counted on the last known position, nothing valid to store.
            count_change(last_cell_, day);
        }
    }

private:
    void count_change(uint64_t cell, int32_t day) {
        parquet_writer_->increment(cell, day);
    }

    node_cache::Writer* cache_writer_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;
    int h3_resolution_;

    // INSTR: diagnostic counters.
    uint64_t nodes_ = 0;
    uint64_t cache_writes_ = 0;

    int64_t current_node_id_ = -1;
    bool has_last_cell_ = false;
    uint64_t last_cell_ = 0;
};

}  // namespace node_pass
