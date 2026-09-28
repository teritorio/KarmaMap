#pragma once

// Suspect history following the OSMPatrol filters of Neis, Goetz & Zipf
// (2012) — see docs/osmpatrol-neis-2012.md — plus one local extension.
// All four filters collapse into the per-day `suspect_flag` bits field of
// users_history.parquet:
//
//   bit 0 (kFlagFilter2)  a day minute's trailing 60-minute modified+deleted
//                         span exceeds kFilter2Threshold (paper: "modified
//                         or deleted more than 500 objects within one hour")
//   bit 1 (kFlagFilter3)  any modified node moved more than kFilter3Threshold
//                         metres that day
//   bit 2 (kFlagFilter1)  the editing user's ranking is below
//                         kFilter1RankingThreshold (paper: "Show all edits
//                         of new users and/or users with a very low ranking
//                         (<5%)"); set forward-only on rows the users-history
//                         update finalize newly writes (0 on import).
//                         "New users" are covered implicitly: a contributor
//                         who created nothing ranks 0.
//   bit 3 (kFlagFilter4)  a local extension, not from the paper: a 60-minute
//                         window holds at least kFilter4MinCount modified+
//                         deleted objects touching at least kFilter4MinCells
//                         distinct H3 cells, whose combined cell area reaches
//                         kFilter4SpreadKm2 km².
//
//   suspect_minutes.bin per-(uid, minute) count of modified+deleted objects
//                       over the whole update period, persisted as a binary
//                       block store (suspect_store.hpp). fold_minute_counts()
//                       merges each update run's staged buckets into it,
//                       summed per (uid, minute), exactly once. It is the
//                       source of truth behind the bit-0 flag: flagged_days()
//                       reads it and the users-history update finalize
//                       writes its bits into users_history.parquet. It is also
//                       the object-count gate for bit 3.
//
//   suspect_cells.bin  per-(uid, minute, h3_cell) count of modified+deleted
//                       objects over the whole update period
//                       (suspect_cell_store.hpp), written by the same staging
//                       pass that writes suspect_minutes.bin. This is the
//                       distinct-cell input for bit 3. Ways count once per
//                       distinct cell their nodes fall in; relations are
//                       skipped. The store header pins the H3 resolution it
//                       was written at and is validated on read.
//
// Buckets are UTC minutes since the epoch. A minute's trailing 60-minute span
// is the sum of its modified_deleted plus the previous 59 minutes'. Folding is
// idempotent: the store header carries the applied replication sequence, and
// fold_minute_counts() skips a sequence that is already folded.
//
// Filters 2, 3 and 4 are loaded at ingest: flagged_days() recomputes the
// whole-history bit-0 set from the persisted store, flagged_cell_days() the
// bit-3 set from the two stores, and the finalize ORs them
// with the carried base bits, so a day's flag is monotonic (an update rerun
// re-setting an already-set bit is harmless; no move store is kept — every
// modified node's distance is thresholded and discarded). They only cover the
// period after the recorded replication sequence (the diffs applied by update
// runs); import writes them as 0.
//
// Bit 2 is likewise monotonic and forward-only, but not diff-based. It is
// derived from the user's current ranking (below
// kFilter1RankingThreshold): import writes it as 0, and the users-history
// update finalize sets it only on the run's newly-written rows for a
// below-threshold contributor. Base rows' bit-2 is carried unchanged, so a
// flag once written persists and a ranking drop never re-flags the past.
// The users-history finalize builds the bit from the same ranking::Result
// that writes user_ranking.parquet.
//
// Filter 3's prior position is the center of the node's last known H3 cell
// (NodeState overlay from an earlier diff of the run, else the flat
// incremental cache base), because osc diffs carry only the new coordinates.
// At --h3-resolution 9 the cell radius is ~175 m, so the computed distance
// equals |old cell center -> new point| and differs from the true
// |old point -> new point| move by up to one cell radius — adequate for the
// 500 m screen, not for the paper's finer 11 m edit-analysis flag.

#include <algorithm>
#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

// Forward declaration: run_scan_diff_cells takes a reference to NodeState
// to resolve way node cells. update.hpp includes this header, so we cannot
// include it here (would cycle). NodeState is defined in ::update_pass.
namespace update_pass { class NodeState; }

namespace suspect {

// Filter 2 flag threshold: "modified or deleted more than 500 objects within
// one hour" (paper sec. 5).
inline constexpr uint32_t kFilter2Threshold = 500;

// Filter 3 flag threshold: "nodes moved more than 500 metres" (paper sec. 5).
inline constexpr double kFilter3Threshold = 500.0;

// Filter 1 flag threshold: "users with a very low ranking (<5%)" (paper
// sec. 5). The ranking is stored as a 0-100 uint8, so a value below 5
// triggers the bit.
inline constexpr uint8_t kFilter1RankingThreshold = 5;

// Bits of the users_history.parquet suspect_flag column.
inline constexpr uint8_t kFlagFilter2 = 0x01;
inline constexpr uint8_t kFlagFilter3 = 0x02;
inline constexpr uint8_t kFlagFilter1 = 0x04;
// bit 3 (kFlagFilter4): distinct H3 cells in trailing 1h exceed a surface-area budget.
// Not from the original OSMPatrol paper; a local extension.
inline constexpr uint8_t kFlagFilter4 = 0x08;

// Trailing window width in minutes (inclusive of the current minute).
inline constexpr uint32_t kHourSpanMinutes = 60;

// Filter 4 thresholds.
inline constexpr uint32_t kFilter4MinCount  = 20;     // true edits in the 60-minute window
inline constexpr uint32_t kFilter4MinCells  = 3;      // distinct cells in the window
inline constexpr double   kFilter4SpreadKm2 = 20.0;   // area budget (distinct_cells * cell_area)

struct UserMinuteKey {
    int64_t uid;
    uint32_t minute;

    bool operator==(const UserMinuteKey& o) const { return uid == o.uid && minute == o.minute; }
};

struct UserMinuteKeyHash {
    size_t operator()(const UserMinuteKey& k) const noexcept {
        return std::hash<int64_t>()(k.uid) ^
               (std::hash<uint32_t>()(k.minute) + 0x9e3779b97f4a7c15ULL);
    }
};

struct UserMinuteEntry {
    std::string username;  // first username seen that minute for this uid
    uint32_t modified_deleted = 0;
};

// Pure, osmium-free per-(uid, minute) aggregation. The diff scan feeds every
// object to add_object(), which counts modifies and deletes only (creates are
// ignored, following filter 2). Unit-testable without a diff file.
class MinuteStats {
public:
    MinuteStats() = default;

    void add_object(int64_t uid, std::string_view username, uint32_t minute, bool visible,
                    uint32_t version) {
        if (visible && version == 1) return;  // created, not a mod/del
        UserMinuteEntry& e = minutes_[UserMinuteKey{uid, minute}];
        if (e.username.empty()) e.username = std::string(username);
        e.modified_deleted++;
    }

    const std::unordered_map<UserMinuteKey, UserMinuteEntry, UserMinuteKeyHash>& minutes() const {
        return minutes_;
    }

    void clear() { minutes_.clear(); }

    size_t size() const { return minutes_.size(); }

private:
    std::unordered_map<UserMinuteKey, UserMinuteEntry, UserMinuteKeyHash> minutes_;
};

// NEW: Filter 4 - per (uid, minute, h3_cell) modified+deleted counts
struct UserMinuteCellKey {
    int64_t uid;
    uint32_t minute;
    uint64_t h3_cell;  // packed 6-byte cell (resolution <= 13)

    bool operator==(const UserMinuteCellKey& o) const {
        return uid == o.uid && minute == o.minute && h3_cell == o.h3_cell;
    }
};

struct UserMinuteCellKeyHash {
    size_t operator()(const UserMinuteCellKey& k) const noexcept {
        return std::hash<int64_t>()(k.uid) ^
               (std::hash<uint32_t>()(k.minute) + 0x9e3779b97f4a7c15ULL) ^
               (std::hash<uint64_t>()(k.h3_cell) << 1);
    }
};

struct UserMinuteCellEntry {
    std::string username;  // first username seen for this (uid, minute, cell)
    uint32_t modified_deleted = 0;
};

// Pure, osmium-free per-(uid, minute, h3_cell) aggregation for Filter 4.
// Nodes: count in their H3 cell. Ways: count once per distinct cell of their nodes.
// Creates (visible version 1) are ignored. Relations are skipped.
class MinuteCellStats {
public:
    MinuteCellStats() = default;

    void add_node(int64_t uid, std::string_view username, uint32_t minute, uint64_t h3_cell,
                  bool visible, uint32_t version) {
        if (visible && version == 1) return;  // created, not a mod/del
        UserMinuteCellEntry& e = cells_[UserMinuteCellKey{uid, minute, h3_cell}];
        if (e.username.empty()) e.username = std::string(username);
        e.modified_deleted++;
    }

    void add_way(int64_t uid, std::string_view username, uint32_t minute,
                 const std::vector<uint64_t>& way_cells, bool visible, uint32_t version) {
        if (visible && version == 1) return;  // created, not a mod/del
        // Deduplicate cells (defensive: caller should deduplicate, but we don't trust it)
        if (way_cells.size() > 1) {
            std::vector<uint64_t> uniq = way_cells;
            std::sort(uniq.begin(), uniq.end());
            uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());
            for (uint64_t cell : uniq) {
                UserMinuteCellEntry& e = cells_[UserMinuteCellKey{uid, minute, cell}];
                if (e.username.empty()) e.username = std::string(username);
                e.modified_deleted++;
            }
        } else {
            for (uint64_t cell : way_cells) {
                UserMinuteCellEntry& e = cells_[UserMinuteCellKey{uid, minute, cell}];
                if (e.username.empty()) e.username = std::string(username);
                e.modified_deleted++;
            }
        }
    }

    const std::unordered_map<UserMinuteCellKey, UserMinuteCellEntry, UserMinuteCellKeyHash>& cells() const {
        return cells_;
    }

    void clear() { cells_.clear(); }

    size_t size() const { return cells_.size(); }

private:
    std::unordered_map<UserMinuteCellKey, UserMinuteCellEntry, UserMinuteCellKeyHash> cells_;
};

// --- Filter 4: cell spread flag ---

struct CellCountRow {
    uint32_t minute;
    uint64_t h3_cell;
    uint32_t modified_deleted;
};

struct CellSpreadRow {
    uint32_t minute;
    uint32_t object_count;
    uint32_t distinct_cells;
    double spread_km2;
    uint8_t flagged;
};

// Sliding-window evaluator for one uid. Feeds cell rows (minute, cell, count)
// and object rows (minute, count) in ascending minute order. The window is the
// trailing kHourSpanMinutes minutes inclusive of the current minute, matching
// the rule of Filter 2's hour_spans. Memory is bounded by the window
// (~60 min) rather than by the number of rows fed.
//
// Order of operations is load-bearing: add_cell()/add_object() only append, so
// a caller that feeds rows from before the current minute and then calls
// evict() will have those rows counted. Call evaluate() for a minute m only
// after feeding every row through m and calling evict(m) exactly once, which
// leaves the window holding exactly the rows in (m - kHourSpanMinutes, m].
class CellSpreadAccumulator {
public:
    explicit CellSpreadAccumulator(double cell_area_km2);

    void add_cell(uint32_t minute, uint64_t cell, uint32_t count);
    void add_object(uint32_t minute, uint32_t count);
    void evict(uint32_t minute);
    CellSpreadRow evaluate(uint32_t minute) const;

private:
    double cell_area_km2_;
    uint64_t object_total_ = 0;
    std::deque<std::pair<uint32_t, uint32_t>> object_deque_;  // (minute, count)
    std::deque<std::tuple<uint32_t, uint64_t, uint32_t>> cell_deque_;  // (minute, cell, count)
    std::unordered_map<uint64_t, uint32_t> cell_counts_;
};

// Thin wrapper around CellSpreadAccumulator for the vector-based API.
// Mirrors the shape of hour_spans: one CellSpreadRow per minute of the union
// of the two input streams, in ascending order. Both streams must be sorted
// ascending and non-decreasing in minute. Evaluating on the union rather than
// on the cell rows alone matters because the two streams are populated
// differently: relations, deleted nodes with no location, and ways whose refs
// all resolve to cell 0 reach the minute store but never the cell store, so a
// user can cross the object-count gate on a minute that carries no cell row.
inline std::vector<CellSpreadRow> cell_spreads(
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

// Reads the persisted cell store and the minute store, computes the Filter 4
// flag per (uid, day) and returns a map keyed by (uid, day) with value
// kFlagFilter4 (or 0 if not flagged). The minute store provides the true
// per-(uid, minute) modified+deleted count for the >= kFilter4MinCount gate.
std::map<std::pair<int64_t, uint16_t>, uint8_t> flagged_cell_days(
    const std::string& cells_path, const std::string& minutes_path, int h3_resolution);

// One merged (uid, minute) row plus its derived window. hour_span sums
// modified_deleted over the trailing kHourSpanMinutes minutes ending at
// minute; flagged = hour_span > kFilter2Threshold.
struct MinuteSpanRow {
    uint32_t minute;
    uint32_t modified_deleted;
    uint32_t hour_span;
    uint8_t flagged;
};

// Computes the trailing 1-hour span and flag for one uid's already-aggregated
// minutes (at most one row per minute, strictly ascending). Returns a row for
// every input minute. Non-empty `minute_counts` only.
inline std::vector<MinuteSpanRow> hour_spans(
    const std::vector<std::pair<uint32_t, uint32_t>>& minute_counts) {
    std::vector<MinuteSpanRow> out;
    out.reserve(minute_counts.size());
    size_t head = 0;
    uint64_t window = 0;
    for (const auto& [minute, count] : minute_counts) {
        window += count;
        while (minute_counts[head].first + kHourSpanMinutes <= minute) {
            window -= minute_counts[head].second;
            ++head;
        }
        out.push_back({minute, count, static_cast<uint32_t>(window),
                       static_cast<uint8_t>(window > kFilter2Threshold ? 1 : 0)});
    }
    return out;
}

// Collects modified-node moves during the update node pass (filter 3). Rows
// are written into per-diff stage dirs under <stage_root>/seq_<seq> and folded
// by flagged_move_days into the day-level bit-1 flag and far-move count.
class NodeMoveSink {
public:
    explicit NodeMoveSink(std::string stage_root);
    ~NodeMoveSink();

    NodeMoveSink(const NodeMoveSink&) = delete;
    NodeMoveSink& operator=(const NodeMoveSink&) = delete;

    // Begins collecting for one diff; stages go under <stage_root>/seq_<seq>.
    // Must be called before record() and once per diff of the run.
    void start_seq(uint64_t seq);

    // Records a visible version>1 node with a valid new location and a known
    // prior cell. The move distance is computed from the prior cell center and
    // only a move beyond kFilter3Threshold is staged, as a (uid, minute) row.
    // Callers must read the prior cell from NodeState::pre() before
    // set_position().
    void record(int64_t uid, int64_t ts_seconds, uint64_t prev_cell, double new_lat,
                double new_lon);

    // Flushes the current seq's pending rows to its stage dir.
    void finish_seq();

private:
    void flush_pending();

    std::string stage_root_;
    uint64_t seq_ = 0;
    bool in_seq_ = false;
    size_t rows_in_seq_ = 0;
    size_t stage_files_ = 0;
    std::vector<int64_t> uids_;
    std::vector<uint32_t> minutes_;
};

// Scans one replication diff (.osc.gz change file) into a fresh stage_dir,
// counting modified+deleted objects per (uid, minute) (filter 2). Mirrors the
// users-history diff classification (visible version 1 = created, later
// versions = modified, invisible = deleted).
void run_scan_diff(const std::string& diff_path, const std::string& stage_dir);

// Folds one update run's staged per-(uid, minute) count buckets under
// `counts_root` (one parquet file per diff flush) into the persisted binary
// minute store at `minutes_path`, summing equal (uid, minute) keys ("merge
// with the incoming update"). `applied_seq` is the last replication sequence
// folded this run: it is stamped into the store header and lets a rerun of an
// already-folded sequence (crash between rename and stage cleanup) skip the
// fold instead of double-counting. When the store already carries a stamp,
// staged sequences <= that stamp were folded by an earlier run and are
// skipped per-directory, so a rerun advancing past the stamp re-merges only
// the newly staged sequences. The staged buckets are always removed afterwards.
void fold_minute_counts(const std::string& counts_root, const std::string& minutes_path,
                        uint64_t applied_seq);

// Reads the persisted minute store and returns one (uid, day) -> flag entry
// for every day holding at least one minute whose trailing-hour span exceeds
// kFilter2Threshold; day = minute / 1440 (UTC) and the flag is kFlagFilter2.
// Feeds the suspect_flag bits the users-history update finalize writes into
// the daily history.
std::map<std::pair<int64_t, uint16_t>, uint8_t> flagged_days(const std::string& minutes_path);

// One (uid, day) filter-3 result: the day's flag plus the number of staged
// moves beyond kFilter3Threshold folded into that day.
struct MoveDay {
    uint8_t flags = 0;
    uint32_t far_move_count = 0;

    bool operator==(const MoveDay& o) const {
        return flags == o.flags && far_move_count == o.far_move_count;
    }
};

// Folds one update run's staged >kFilter3Threshold node moves under
// `stage_root` (moves/ sub-tree) into this run's (uid, day) -> MoveDay set:
// each staged (uid, minute) row becomes a flag and an added far-move count on
// day = minute / 1440. Removes the stage root (and therefore the counts/
// sub-tree already consumed by fold_minute_counts) afterwards. Flags are ORed
// by the caller, so folding a rerun's regenerated stages is a no-op.
std::map<std::pair<int64_t, uint16_t>, MoveDay> flagged_move_days(
    const std::string& stage_root);

// --- Filter 4: H3 cell activity tracking ---

// Scans one replication diff (.osc.gz change file) into a fresh stage_dir,
// counting modified+deleted objects per (uid, minute, h3_cell).
// Nodes are counted in their H3 cell. Ways are counted once per distinct
// cell of their nodes, resolved through NodeState (post() for visible ways,
// pre() for deleted ways). Relations are skipped.
void run_scan_diff_cells(const std::string& diff_path, const std::string& stage_dir,
                         int h3_resolution, const ::update_pass::NodeState& node_state);

// Folds one update run's staged per-(uid, minute, h3_cell) count buckets under
// `cells_root` (one parquet file per diff flush) into the persisted binary
// cell store at `cells_path`, summing equal (uid, minute, h3_cell) keys.
// `applied_seq` is the last replication sequence folded this run: it is stamped
// into the store header and lets a rerun of an already-folded sequence skip
// the fold instead of double-counting. `h3_resolution` is stored in the header
// and validated on read.
void fold_cell_counts(const std::string& cells_root, const std::string& cells_path,
                      uint64_t applied_seq, int h3_resolution);

}  // namespace suspect
