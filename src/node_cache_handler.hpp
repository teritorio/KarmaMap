#pragma once

// Handler applied to the node stream of an .osh.pbf file (full history,
// assumed sorted by (id, version) ascending).
//
// Business rules:
//  - Invalid position (no coordinates) -> no RocksDB write.
//  - Deletion (visible=false) -> counted as a change, using the LAST known
//    position of that node. If no position was ever known -> nothing to
//    count.
//  - The guaranteed (id, version) ordering lets us reset the "last known
//    position" state on every node_id change with no extra checks.
//
// Counts are routed straight to a PartitionedParquetWriter (one Parquet
// file per calendar month); that writer owns its own per-partition
// buffering and flush thresholds, so this handler only tracks the RocksDB
// write-batch buffering.

#include <osmium/handler.hpp>
#include <osmium/osm/node.hpp>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include <stdexcept>

#include "h3_utils.hpp"
#include "partitioned_parquet_writer.hpp"
#include "rocks_codec.hpp"

namespace node_pass {

constexpr size_t kRocksBatchThreshold = 100'000;  // entries before RocksDB flush

class NodeCacheHandler : public osmium::handler::Handler {
public:
    NodeCacheHandler(rocksdb::DB* db, parquet_out::PartitionedParquetWriter* parquet_writer,
                      int h3_resolution)
        : db_(db), parquet_writer_(parquet_writer), h3_resolution_(h3_resolution) {}

    // Call after the full read to flush any remaining buffered RocksDB writes.
    void finish() { flush_rocksdb(); }

    void node(const osmium::Node& n) {
        if (n.id() != current_node_id_) {
            current_node_id_ = n.id();
            has_last_location_ = false;
        }

        const int64_t ts = n.timestamp().seconds_since_epoch();
        const bool has_coords = n.location().valid();

        if (has_coords) {
            const double lat = n.location().lat();
            const double lon = n.location().lon();

            write_batch_.Put(rocks_codec::encode_key(n.id(), n.version()),
                              rocks_codec::encode_value(ts, lat, lon));
            rocks_batch_count_++;

            last_lat_ = lat;
            last_lon_ = lon;
            has_last_location_ = true;

            count_change(lat, lon, ts);

        } else if (!n.visible() && has_last_location_) {
            // Deletion with a previously known position: counted, no new
            // RocksDB write (no new valid position to store).
            count_change(last_lat_, last_lon_, ts);
        }
        // Otherwise (no coordinates, no known position): skip.

        if (rocks_batch_count_ >= kRocksBatchThreshold) flush_rocksdb();
    }

private:
    void count_change(double lat, double lon, int64_t ts) {
        uint64_t cell = h3_utils::location_to_cell(lat, lon, h3_resolution_);
        int32_t day = h3_utils::timestamp_to_utc_day(ts);
        parquet_writer_->increment(cell, day);
    }

    void flush_rocksdb() {
        if (rocks_batch_count_ == 0) return;
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &write_batch_);
        if (!status.ok()) {
            throw std::runtime_error("RocksDB batch write failed: " + status.ToString());
        }
        write_batch_.Clear();
        rocks_batch_count_ = 0;
    }

    rocksdb::DB* db_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;
    int h3_resolution_;

    rocksdb::WriteBatch write_batch_;
    size_t rocks_batch_count_ = 0;

    int64_t current_node_id_ = -1;
    bool has_last_location_ = false;
    double last_lat_ = 0.0;
    double last_lon_ = 0.0;
};

}  // namespace node_pass
