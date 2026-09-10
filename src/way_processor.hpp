#pragma once

// Handler applied to the way stream of an .osh.pbf file (assumed sorted by
// (id, version) ascending), resolving referenced node positions against an
// mmap node cache built by the node pass.
//
// A way is counted at the distinct cells of its resolved node positions
// (each cell once per version); unresolved nodes are skipped, deleted ways
// count on their last known geometry. A node's position is its last cache
// record with day <= the way's day.
//
// Lookups are batched (--way-batch-mb): a batch's refs are sorted by
// (node_id, day) and resolved with one forward-only sweep over the cache, so
// the page cache is read sequentially. A way never spans two batches;
// deleted-way geometry carries across batches via a cell list.

#include <osmium/handler.hpp>
#include <osmium/osm/way.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

#include "h3_utils.hpp"
#include "node_cache.hpp"
#include "partitioned_parquet_writer.hpp"

namespace way_pass {

struct NodeRef {
    int64_t node;
    uint16_t day;
    uint32_t slot;
};

struct ResolvedRef {
    uint32_t slot;
    uint64_t cell;
};

class WayProcessor : public osmium::handler::Handler {
public:
    WayProcessor(node_cache::Reader* cache_reader,
                 parquet_out::PartitionedParquetWriter* parquet_writer,
                 size_t batch_bytes)
        : cache_reader_(cache_reader),
          parquet_writer_(parquet_writer),
          batch_max_refs_(std::max<size_t>(1, batch_bytes / sizeof(NodeRef))) {}

    void way(const osmium::Way& w) {
        ways_++;  // INSTR
        const int64_t ts = w.timestamp().seconds_since_epoch();
        const uint16_t day = h3_utils::require_u16_day(h3_utils::timestamp_to_utc_day(ts));

        if (w.visible() && !w.nodes().empty()) {
            const uint32_t slot = static_cast<uint32_t>(slot_counter_++);
            if (slot_days_.size() <= slot) slot_days_.resize(slot + 1);
            if (slot_del_days_.size() <= slot) slot_del_days_.resize(slot + 1);
            slot_days_[slot] = day;

            for (const auto& nr : w.nodes()) {
                refs_.push_back({nr.ref(), day, slot});
            }

            // Flush only on way boundaries so a way is never split across
            // two batches (distinct-cell counting is per batch).
            if (refs_.size() >= batch_max_refs_) flush_batch();
        } else if (!w.visible()) {
            if (slot_counter_ > 0) {
                // Previous visible version of this way is in the current
                // batch: count it there, at this deletion's day.
                const size_t slot = slot_counter_ - 1;
                if (slot_del_days_.size() <= slot) slot_del_days_.resize(slot + 1);
                slot_del_days_[slot].push_back(day);
            } else if (has_carry_) {
                // Previous visible version ended in an earlier batch; its
                // geometry is carried, count it directly.
                for (const auto& cell : carry_cells_) {
                    parquet_writer_->increment(cell, day);
                    points_++;  // INSTR
                }
            }
        }
    }

    // Flushes the final batch; call after the full read.
    void finish() { flush_batch(); }

    // INSTR
    void print_stats() const {
        const double total_ns = static_cast<double>(resolve_ns_) + static_cast<double>(points_ns_);
        const double resolve_pct = total_ns > 0 ? 100.0 * resolve_ns_ / total_ns : 0.0;
        const double points_pct = total_ns > 0 ? 100.0 * points_ns_ / total_ns : 0.0;
        std::cerr << "[way pass] ways=" << ways_ << " batches=" << batches_
                  << " lookups=" << node_lookups_
                  << " scanned=" << records_scanned_
                  << " (" << records_scanned_ / static_cast<double>(node_lookups_ ? node_lookups_ : 1)
                  << "/lookup) misses=" << lookup_misses_
                  << " (" << 100.0 * lookup_misses_ / static_cast<double>(node_lookups_ ? node_lookups_ : 1)
                  << "%) points=" << points_ << "\n";
        std::cerr << "[way pass] resolve=" << resolve_ns_ / 1e6 << "ms (" << resolve_pct
                  << "%) points=" << points_ns_ / 1e6 << "ms (" << points_pct
                  << "%) instrumented_total=" << total_ns / 1e6 << "ms\n";
    }

private:
    void flush_batch() {
        if (refs_.empty() && slot_counter_ == 0) {
            return;
        }

        const auto t0 = std::chrono::steady_clock::now();

        // Sort refs by (node, day) and resolve with a forward-only sweep.
        std::sort(refs_.begin(), refs_.end(), [](const NodeRef& a, const NodeRef& b) {
            if (a.node != b.node) return a.node < b.node;
            if (a.day != b.day) return a.day < b.day;
            return a.slot < b.slot;
        });

        resolved_.clear();
        resolved_.reserve(refs_.size());
        if (!refs_.empty()) {
            size_t rec = cache_reader_->sweep_start(refs_.front().node);
            bool key_valid = false;
            int64_t key_node = 0;
            uint16_t key_day = 0;
            bool key_found = false;
            uint64_t key_cell = 0;

            for (const NodeRef& ref : refs_) {
                node_lookups_++;  // INSTR

                bool found;
                uint64_t cell;
                if (key_valid && ref.node == key_node && ref.day == key_day) {
                    // Identical key as the previous ref: reuse its result.
                    found = key_found;
                    cell = key_cell;
                } else {
                    found = false;
                    const size_t rec0 = rec;  // INSTR
                    while (rec < cache_reader_->size()) {
                        const int64_t rn = cache_reader_->node_at(rec);
                        if (rn < ref.node) {
                            rec++;
                            continue;
                        }
                        if (rn == ref.node) {
                            const uint16_t rd = cache_reader_->day_at(rec);
                            if (rd <= ref.day) {
                                found = true;
                                cell = cache_reader_->cell_at(rec);
                                rec++;
                                continue;
                            }
                            break;  // future version of this node
                        }
                        break;  // past this node
                    }
                    records_scanned_ += rec - rec0;  // INSTR
                    key_valid = true;
                    key_node = ref.node;
                    key_day = ref.day;
                    key_found = found;
                    key_cell = cell;
                }

                if (found) {
                    resolved_.push_back({ref.slot, cell});
                } else {
                    lookup_misses_++;  // INSTR
                }
            }
        }

        resolve_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - t0)
                           .count();  // INSTR
        const auto t1 = std::chrono::steady_clock::now();  // INSTR

        // Distinct cells per slot by sorted adjacency.
        if (!resolved_.empty() || slot_counter_ > 0) {
            std::sort(resolved_.begin(), resolved_.end(), [](const ResolvedRef& a,
                                                             const ResolvedRef& b) {
                if (a.slot != b.slot) return a.slot < b.slot;
                return a.cell < b.cell;
            });

            std::vector<uint64_t> new_carry;
            const size_t last_slot = slot_counter_ > 0 ? slot_counter_ - 1 : 0;

            size_t i = 0;
            while (i < resolved_.size()) {
                const uint32_t slot = resolved_[i].slot;
                size_t j = i;
                while (j < resolved_.size() && resolved_[j].slot == slot) j++;

                const uint16_t vis_day = slot_days_[slot];
                const std::vector<uint16_t>& del_days = slot_del_days_[slot];

                for (size_t k = i; k < j;) {
                    size_t m = k;
                    while (m < j && resolved_[m].cell == resolved_[k].cell) m++;

                    const uint64_t cell = resolved_[k].cell;
                    parquet_writer_->increment(cell, vis_day);
                    points_++;  // INSTR
                    for (uint16_t d : del_days) {
                        parquet_writer_->increment(cell, d);
                        points_++;  // INSTR
                    }
                    if (slot_counter_ > 0 && slot == last_slot) {
                        new_carry.push_back(resolved_[k].cell);
                    }
                    k = m;
                }
                i = j;
            }

            if (slot_counter_ > 0) {
                // Last rendered geometry, carried over to the next batch for
                // deleted ways that follow it there.
                has_carry_ = true;
                carry_cells_ = std::move(new_carry);
            }
        }

        points_ns_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now() - t1)
                          .count();  // INSTR

        refs_.clear();
        resolved_.clear();
        slot_days_.clear();
        slot_del_days_.clear();
        slot_counter_ = 0;
        batches_++;  // INSTR
    }

    node_cache::Reader* cache_reader_;
    parquet_out::PartitionedParquetWriter* parquet_writer_;
    size_t batch_max_refs_;

    std::vector<NodeRef> refs_;
    std::vector<ResolvedRef> resolved_;
    std::vector<uint16_t> slot_days_;
    std::vector<std::vector<uint16_t>> slot_del_days_;
    size_t slot_counter_ = 0;

    bool has_carry_ = false;
    std::vector<uint64_t> carry_cells_;

    // INSTR: diagnostic counters.
    uint64_t ways_ = 0;
    uint64_t batches_ = 0;
    uint64_t node_lookups_ = 0;
    uint64_t records_scanned_ = 0;
    uint64_t lookup_misses_ = 0;
    uint64_t points_ = 0;  // distinct node cells incremented
    int64_t resolve_ns_ = 0;
    int64_t points_ns_ = 0;
};

}  // namespace way_pass
