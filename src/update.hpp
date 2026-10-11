#pragma once

// Update mode: applies osmosis replication diffs (.osc.gz change files) to an
// existing dataset built by "karmamap import" and given its incremental cache
// by "karmamap prepare-update". The first three passes run in update mode:
//
//   update node pass: counts a batch's node changes into nodes.<seq>.parquet
//     staging and folds created/modified positions into an in-memory overlay
//     over the flat incremental cache (--node-cache-last, <node-cache>.last),
//     tracking deletions separately.
//   update way pass: counts a batch's way changes into ways.<seq>.parquet,
//     resolving node refs against the overlay (post-update view for visible
//     ways, pre-update view for deleted ways = their last known geometry).
//   flat cache persist: once per "karmamap update" run the accumulated overlay
//     and deletions are written to a small .last.delta sidecar (O(changes)),
//     leaving the .last base untouched. Only when the accumulated delta reaches
//     the compaction threshold is the base rewritten as base + overlay minus
//     deletions and the sidecar dropped (the incremental writer's tmp+rename
//     swap keeps it consistent across crashes).
//
// Consecutive diffs are merged into one change file per batch (the batching
// bound is kUpdateBatchMaxSeconds below); a batch stages its counts under the
// batch's last sequence (nodes.<seq>.parquet / ways.<seq>.parquet).
// sort_pass::merge_update_partitions folds the staged files into data.parquet
// once per run, so fetching N diffs never rewrites the dataset N times.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <osmium/memory/buffer.hpp>

#include "node_cache.hpp"
#include "node_delta.hpp"
#include "suspect.hpp"

namespace osmium {
namespace io {
class Writer;
}
}  // namespace osmium

namespace update_pass {

// Batch bound: diffs are merged into one change file once their combined
// object-timestamp span reaches kUpdateBatchMaxSeconds. The span comes from
// the diffs themselves, so the bound holds for any replication cadence.
inline constexpr int64_t kUpdateBatchMaxSeconds = 3600;  // 1 h

// Streams a batch of replication diffs (.osc.gz) into one osmChange file in
// sequence order: opens the writer on open(), streams each diff's buffers on
// add(), closes on finish(), and tracks the min/max object timestamps so the
// caller can bound the batch. Output is a plain .osc under the diffs dir.
class DiffMerger {
public:
    DiffMerger();
    ~DiffMerger();

    DiffMerger(const DiffMerger&) = delete;
    DiffMerger& operator=(const DiffMerger&) = delete;

    // Opens (or overwrites) the merged file at out_path and resets the batch
    // state. Truncates any file already there, so a leftover from a crashed
    // run can never poison a later one.
    void open(const std::string& out_path);

    // Streams one diff into the merged file. Called once per diff of the
    // batch, in sequence order. Throws on a missing open(), or on diff read
    // or write errors.
    void add(const std::string& diff_path);

    // Closes the merged file. Idempotent; called before the passes run.
    void finish();

    int64_t min_ts() const { return min_ts_; }
    int64_t max_ts() const { return max_ts_; }
    bool has_ts() const { return has_ts_; }
    bool is_open() const { return writer_ != nullptr; }

private:
    std::unique_ptr<osmium::io::Writer> writer_;
    int64_t min_ts_ = 0;
    int64_t max_ts_ = 0;
    bool has_ts_ = false;
};

// The per-run node position state: the flat incremental cache as a read-only
// base plus the accumulated created/modified overlay and delete set, all in
// memory. Created/modified nodes overlay the base cell; deleted nodes are
// removed from the overlay and remembered so the cache persist drops them and
// deleted ways can resolve their pre-update geometry. The overlay and delete
// set carry over between runs through a <base>.delta sidecar, so a run only
// rewrites the base when they grow past the compaction threshold.
class NodeState {
public:
    NodeState(const std::string& node_cache_last_path, int h3_resolution)
        : base_(node_cache_last_path, h3_resolution), h3_resolution_(h3_resolution) {
        node_cache::delta::State accumulated =
            node_cache::delta::load_or_empty(delta_path(node_cache_last_path),
                                             h3_resolution);
        overlay_ = std::move(accumulated.modified);
        deleted_ = std::move(accumulated.deleted);
    }

    NodeState(const NodeState&) = delete;
    NodeState& operator=(const NodeState&) = delete;

    static std::string delta_path(const std::string& node_cache_last_path) {
        return node_cache_last_path + ".delta";
    }

    // Cell of a node as of before this batch's deletions (overlay else base):
    // deleted ways resolve here, on their last known geometry.
    uint64_t pre(int64_t node) const {
        auto it = overlay_.find(node);
        if (it != overlay_.end()) return it->second;
        return base_cell(node);
    }

    // Cell of a node as of after the batch (overlay else base; deleted nodes
    // resolve to nothing): created/modified ways resolve here.
    uint64_t post(int64_t node) const {
        auto it = overlay_.find(node);
        if (it != overlay_.end()) return it->second;
        if (deleted_.count(node)) return 0;
        return base_cell(node);
    }

    // Resolves `nodes` that are neither in the overlay nor already memoized
    // with a single forward sweep over the base cache, memoizing the cells so
    // later pre()/post() calls serve from memory. The batched update way pass
    // calls this once per ref batch: sorting the batch's refs and walking the
    // cache forward avoids the per-ref block decompressions of random lookup().
    // Returns the number of base records read during the sweep (INSTR).
    size_t prefetch_base(const std::vector<int64_t>& nodes) const {
        std::vector<int64_t> pending;
        pending.reserve(nodes.size());
        for (int64_t node : nodes) {
            if (overlay_.count(node)) continue;
            if (base_memo_.count(node)) continue;
            pending.push_back(node);
        }
        if (pending.empty()) return 0;
        std::sort(pending.begin(), pending.end());

        size_t scanned = 0;
        size_t rec = base_.sweep_start(pending.front());
        size_t p = 0;
        while (p < pending.size() && rec < base_.size()) {
            const int64_t want = pending[p];
            const int64_t rn = base_.node_at(rec);
            scanned++;
            if (rn < want) {
                rec++;
                continue;
            }
            if (rn == want) {
                base_memo_[want] = base_.cell_at(rec);
                rec++;
            } else {
                base_memo_[want] = 0;  // past this node; absent
            }
            p++;
        }
        while (p < pending.size()) {
            base_memo_[pending[p++]] = 0;
        }
        return scanned;
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

    // Rewrites the incremental cache as base + overlay, minus deletions. Overlay
    // keys are sorted before base classification so the reader's single
    // decompressed block advances forward; base records then stream in sorted
    // order with overlay overrides applied and new nodes interleaved.
    void rebuild(const std::string& output_path, int h3_resolution) const {
        node_cache::incremental::Writer writer(output_path, h3_resolution);
        std::vector<int64_t> overlay_nodes;
        overlay_nodes.reserve(overlay_.size());
        for (const auto& entry : overlay_) {
            overlay_nodes.push_back(entry.first);
        }
        std::sort(overlay_nodes.begin(), overlay_nodes.end());

        std::vector<std::pair<int64_t, uint64_t>> extra;
        for (int64_t node : overlay_nodes) {
            if (base_cell(node) == 0) extra.emplace_back(node, overlay_.at(node));
        }

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

    // What persist() did with the run's accumulated changes.
    enum class PersistResult { no_change, delta_written, compacted };

    // Persists the run's accumulated overlay and deletions. Writes the delta
    // sidecar (O(changes)) unless the accumulated change count reaches
    // `changed_limit`, in which case the base is rewritten as base + overlay
    // minus deletions and the sidecar dropped. A run that changed nothing
    // leaves both the base and any existing sidecar untouched.
    PersistResult persist(const std::string& last_path, int h3_resolution,
                          size_t changed_limit) const {
        const size_t changed = overlay_.size() + deleted_.size();
        if (changed == 0) return PersistResult::no_change;
        const std::string sidecar = delta_path(last_path);
        if (changed >= changed_limit) {
            rebuild(last_path, h3_resolution);
            std::error_code ec;
            std::filesystem::remove(sidecar, ec);
            return PersistResult::compacted;
        }
        node_cache::delta::write(sidecar, h3_resolution, overlay_, deleted_);
        return PersistResult::delta_written;
    }

    // INSTR: flat cache records after the base sweep, for the run summary.
    uint64_t base_records() const { return base_.size(); }

private:
    // Base cache cell for `node`, memoized across the run so repeated
    // resolution (node pass move checks, way pass refs) decompresses each cell
    // at most once after prefetch_base has run.
    uint64_t base_cell(int64_t node) const {
        auto it = base_memo_.find(node);
        if (it != base_memo_.end()) return it->second;
        const uint64_t cell = base_.lookup(node);
        base_memo_.emplace(node, cell);
        return cell;
    }

    node_cache::incremental::Reader base_;
    std::unordered_map<int64_t, uint64_t> overlay_;
    std::unordered_map<int64_t, uint8_t> deleted_;
    mutable std::unordered_map<int64_t, uint64_t> base_memo_;
    int h3_resolution_;
};

// Run the update node pass over one change file: counts node deltas into
// <changes_root>/year=YYYY/nodes.<seq>.parquet and folds the positions into
// `state` (shared across all batches of the run). When `moves` is non-null,
// modified nodes with a known prior position are recorded into it before the
// position is updated (suspect filter 3; only moves beyond the threshold are
// staged, as per-(uid, minute) rows).
void run_node_update(const std::string& diff_path, const std::string& changes_root,
                     uint64_t seq, int h3_resolution, NodeState* state,
                     suspect::NodeMoveSink* moves = nullptr);

// Run the update way pass over one change file: counts way deltas into
// <changes_root>/year=YYYY/ways.<seq>.parquet using `state`'s overlay. Way
// refs are accumulated into batches of up to `way_batch_bytes` and resolved
// against the base cache with a single forward sweep per batch (see
// prefetch_base), mirroring the import pass; visible ways count their
// post-update cells, deleted ways their pre-update cells.
void run_way_update(const std::string& diff_path, const std::string& changes_root,
                    uint64_t seq, const NodeState& state, size_t way_batch_bytes);

// One scan pass of run_diff_scans' shared change-file read: apply() runs the
// wrapped subsystem handler over one buffer of the shared read, finish()
// flushes it and prints the pass's INSTR line.
class DiffScanStep {
public:
    virtual ~DiffScanStep() = default;
    virtual void apply(const osmium::memory::Buffer& buffer) = 0;
    virtual void finish() = 0;
};

// Runs the way, users-history, suspect counts (filter 2), suspect cells
// (filter 4) and suspect tag (filter 5) passes over one change file with a
// single shared read: the five handlers are driven from one reader, so a
// batch's change stream is parsed twice in total (the node pass plus this)
// instead of once per pass. The four stage dirs are wiped first so a rerun
// never reuses stale stage files; ways land in
// <changes_root>/year=YYYY/ways.<seq>.parquet. The node pass must fold all
// positions before ways and cells resolve node refs, so this runs after
// run_node_update.
void run_diff_scans(const std::string& change_path, const std::string& changes_root,
                    uint64_t seq, size_t way_batch_bytes, int h3_resolution,
                    const NodeState& state, const std::string& history_stage,
                    const std::string& counts_stage, const std::string& cells_stage,
                    const std::string& tags_stage);

}  // namespace update_pass
