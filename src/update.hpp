#pragma once

// Update mode: applies osmosis replication diffs (.osc.gz change files) to an
// existing dataset built by "karmamap import" and given its incremental cache
// by "karmamap prepare-update". The first three passes run in update mode:
//
//   update node pass: counts the diff's node changes into nodes.<seq>.parquet
//     staging and folds created/modified positions into an in-memory overlay
//     over the flat incremental cache (--node-cache-last, <node-cache>.last),
//     tracking deletions separately.
//   update way pass: counts the diff's way changes into ways.<seq>.parquet,
//     resolving node refs against the overlay (post-update view for visible
//     ways, pre-update view for deleted ways = their last known geometry).
//   flat cache rebuild: once per "karmamap update" run the .last cache is
//     rewritten as base + overlay minus deletions (the incremental writer's
//     tmp+rename swap keeps it consistent across crashes).
//
// Each diff stages its own counts under a suffixed name (nodes.<seq>.parquet /
// ways.<seq>.parquet); sort_pass::merge_update_partitions folds them into
// data.parquet exactly once per run, so fetching N diffs never rewrites the
// dataset N times.

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "node_cache.hpp"

namespace update_pass {

// The per-run node position state: the flat incremental cache as a read-only
// base plus the diff batch's created/modified overlay and delete set, all in
// memory. Created/modified nodes overlay the base cell; deleted nodes are
// removed from the overlay and remembered so the cache rebuild drops them and
// deleted ways can resolve their pre-update geometry.
class NodeState {
public:
    NodeState(const std::string& node_cache_last_path, int h3_resolution)
        : base_(node_cache_last_path, h3_resolution), h3_resolution_(h3_resolution) {}

    NodeState(const NodeState&) = delete;
    NodeState& operator=(const NodeState&) = delete;

    // Cell of a node as of before this batch's deletions (overlay else base):
    // deleted ways resolve here, on their last known geometry.
    uint64_t pre(int64_t node) const {
        auto it = overlay_.find(node);
        if (it != overlay_.end()) return it->second;
        return base_.lookup(node);
    }

    // Cell of a node as of after the batch (overlay else base; deleted nodes
    // resolve to nothing): created/modified ways resolve here.
    uint64_t post(int64_t node) const {
        auto it = overlay_.find(node);
        if (it != overlay_.end()) return it->second;
        if (deleted_.count(node)) return 0;
        return base_.lookup(node);
    }

    // Folds in a node with a known position (created or modified).
    void set_position(int64_t node, uint64_t cell) {
        overlay_[node] = cell;
        deleted_.erase(node);
    }

    // Records a deletion: returns the node's last known cell (overlay else
    // base, 0 when unknown) and removes any overlay position.
    uint64_t remove_node(int64_t node) {
        const uint64_t cell = pre(node);
        overlay_.erase(node);
        deleted_[node] = 1;
        return cell;
    }

    size_t overlay_size() const { return overlay_.size(); }
    size_t deleted_size() const { return deleted_.size(); }

    // Rewrites the incremental cache as base + overlay, minus deletions. The
    // base records stream in sorted order with overlay overrides applied;
    // overlay nodes absent from the base are sorted and interleaved, so the
    // incremental writer sees strictly ascending node ids.
    void rebuild(const std::string& output_path, int h3_resolution) const {
        node_cache::incremental::Writer writer(output_path, h3_resolution);
        std::vector<std::pair<int64_t, uint64_t>> extra;
        for (const auto& [node, cell] : overlay_) {
            if (base_.lookup(node) == 0) extra.emplace_back(node, cell);
        }
        std::sort(extra.begin(), extra.end());

        size_t b = 0;
        const size_t n = base_.size();
        for (size_t i = 0; i < n; ++i) {
            const int64_t node = base_.node_at(i);
            if (deleted_.count(node)) continue;
            uint64_t cell = base_.cell_at(i);
            auto it = overlay_.find(node);
            if (it != overlay_.end()) cell = it->second;
            while (b < extra.size() && extra[b].first < node) {
                writer.add(extra[b].first, extra[b].second);
                ++b;
            }
            writer.add(node, cell);
        }
        while (b < extra.size()) {
            writer.add(extra[b].first, extra[b].second);
            ++b;
        }
        writer.finish();
    }

    // INSTR: flat cache records after the base sweep, for the run summary.
    uint64_t base_records() const { return base_.size(); }

private:
    node_cache::incremental::Reader base_;
    std::unordered_map<int64_t, uint64_t> overlay_;
    std::unordered_map<int64_t, uint8_t> deleted_;
    int h3_resolution_;
};

// Run the update node pass over one change file: counts node deltas into
// <changes_root>/year=YYYY/nodes.<seq>.parquet and folds the positions into
// `state` (shared across all diffs of the run).
void run_node_update(const std::string& diff_path, const std::string& changes_root,
                     uint64_t seq, int h3_resolution, NodeState* state);

// Run the update way pass over one change file: counts way deltas into
// <changes_root>/year=YYYY/ways.<seq>.parquet using `state`'s overlay.
void run_way_update(const std::string& diff_path, const std::string& changes_root,
                    uint64_t seq, const NodeState& state);

}  // namespace update_pass
