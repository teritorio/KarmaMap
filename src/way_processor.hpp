#pragma once

// Handler applied to the way stream of an .osh.pbf file (full history,
// assumed sorted by (id, version) ascending). Requires a RocksDB cache
// already populated by the node pass (node_id, version -> position).
//
// Business rules:
//  - Position resolved via the LAST known version of the node whose
//    timestamp <= the timestamp of the way version being processed.
//  - A segment with an unresolved endpoint -> skipped.
//  - gridPathCells() failure -> segment skipped.
//  - No deduplication: every segment independently increments every cell
//    it crosses.
//  - Deleted way (visible=false) -> counted as a change, using the LAST
//    known geometry. If no geometry was ever known before deletion ->
//    nothing to count.
//  - Visible way with fewer than 2 nodes -> skipped.
//
// Counts are routed straight to a PartitionedParquetWriter (one Parquet
// file per calendar month); that writer owns its own per-partition
// buffering and flush thresholds, so this handler holds no counters of
// its own.

#include <h3api.h>
#include <osmium/handler.hpp>
#include <osmium/osm/way.hpp>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include "h3_utils.hpp"
#include "partitioned_parquet_writer.hpp"
#include "rocks_codec.hpp"

namespace way_pass {

// A resolved position, or "absent" if the node could not be found in the
// RocksDB cache at the target date.
struct ResolvedPoint {
    bool valid = false;
    double lat = 0.0;
    double lon = 0.0;
};

class WayProcessor : public osmium::handler::Handler {
public:
    WayProcessor(rocksdb::DB* db, parquet_out::PartitionedParquetWriter* parquet_writer,
                 int h3_resolution)
        : db_(db), parquet_writer_(parquet_writer), h3_resolution_(h3_resolution) {}

    void way(const osmium::Way& w) {
        if (w.id() != current_way_id_) {
            current_way_id_ = w.id();
            has_last_geometry_ = false;
            last_known_geometry_.clear();
        }

        const int64_t ts = w.timestamp().seconds_since_epoch();
        const int32_t day = h3_utils::timestamp_to_utc_day(ts);

        if (w.visible() && w.nodes().size() >= 2) {
            std::vector<ResolvedPoint> geometry;
            geometry.reserve(w.nodes().size());
            for (const auto& nr : w.nodes()) geometry.push_back(resolve_position(nr.ref(), ts));

            count_way_segments(geometry, day);

            last_known_geometry_ = std::move(geometry);
            has_last_geometry_ = true;

        } else if (!w.visible() && has_last_geometry_) {
            count_way_segments(last_known_geometry_, day);
        }
        // Otherwise (visible way with < 2 nodes, or deletion with no known
        // geometry): nothing to do.
    }

private:
    // Looks up, in RocksDB, the last known version of the node whose
    // timestamp is <= target_ts. Entries are sorted by (node_id, version)
    // ascending, and timestamp increases with version, so scanning in
    // order and stopping at the first timestamp > target_ts is correct.
    ResolvedPoint resolve_position(int64_t node_id, int64_t target_ts) {
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));

        ResolvedPoint result;
        const std::string prefix = rocks_codec::encode_prefix(node_id);

        for (it->Seek(prefix); it->Valid(); it->Next()) {
            rocksdb::Slice key = it->key();
            if (key.size() != rocks_codec::kKeySize) break;

            auto decoded_key = rocks_codec::decode_key(key.data(), key.size());
            if (decoded_key.node_id != node_id) break;  // left the prefix

            rocksdb::Slice value = it->value();
            auto record = rocks_codec::decode_value(value.data(), value.size());
            if (record.timestamp > target_ts) break;  // future versions, stop here

            result.valid = true;
            result.lat = record.lat();
            result.lon = record.lon();
        }

        return result;
    }

    void count_way_segments(const std::vector<ResolvedPoint>& geometry, int32_t day) {
        for (size_t i = 1; i < geometry.size(); ++i) {
            const auto& a = geometry[i - 1];
            const auto& b = geometry[i];
            if (!a.valid || !b.valid) continue;  // broken segment -> skip

            uint64_t c1 = h3_utils::location_to_cell(a.lat, a.lon, h3_resolution_);
            uint64_t c2 = h3_utils::location_to_cell(b.lat, b.lon, h3_resolution_);

            int64_t path_size = 0;
            H3Error size_err = gridPathCellsSize(static_cast<H3Index>(c1),
                                                  static_cast<H3Index>(c2), &path_size);
            if (size_err != E_SUCCESS || path_size <= 0) continue;  // segment skipped

            path_buffer_.resize(static_cast<size_t>(path_size));
            H3Error path_err = gridPathCells(static_cast<H3Index>(c1), static_cast<H3Index>(c2),
                                              path_buffer_.data());
            if (path_err != E_SUCCESS) continue;  // segment skipped

            for (int64_t k = 0; k < path_size; ++k) {
                uint64_t cell = static_cast<uint64_t>(path_buffer_[static_cast<size_t>(k)]);
                parquet_writer_->increment(cell, day);  // no dedup
            }
        }
    }

    rocksdb::DB* db_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;
    int h3_resolution_;

    std::vector<H3Index> path_buffer_;  // reused across segments

    int64_t current_way_id_ = -1;
    bool has_last_geometry_ = false;
    std::vector<ResolvedPoint> last_known_geometry_;
};

}  // namespace way_pass
