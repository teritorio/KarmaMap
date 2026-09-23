#pragma once

// Vandalism history computed from the osmosis replication diffs applied by
// "karmamap update", following two of the OSMPatrol filters of Neis, Goetz &
// Zipf (2012) — see docs/osmpatrol-neis-2012.md. Both filters collapse into
// the per-day `vandalism_flag` bits field of users_history.parquet:
//
//   bit 0 (kFlagFilter2)  a day minute's trailing 60-minute modified+deleted
//                         span exceeds kFilter2Threshold (paper: "modified
//                         or deleted more than 500 objects within one hour")
//   bit 1 (kFlagFilter3)  any modified node moved more than kFilter3Threshold
//                         metres that day
//
//   vandalism_minutes.bin per-(uid, minute) count of modified+deleted objects
//                         over the whole update period, persisted as a binary
//                         block store (vandalism_store.hpp). fold_minute_counts()
//                         merges each update run's staged buckets into it,
//                         summed per (uid, minute), exactly once. It is the
//                         source of truth behind the bit-0 flag: flagged_days()
//                         reads it and the users-history update finalize
//                         writes its bits into users_history.parquet.
//
// Buckets are UTC minutes since the epoch. A minute's trailing 60-minute span
// is the sum of its modified_deleted plus the previous 59 minutes'. Folding is
// idempotent: the store header carries the applied replication sequence, and
// fold_minute_counts() skips a sequence that is already folded.
//
// Both flags are loaded at ingest: flagged_days() recomputes the whole-history
// bit-0 set from the persisted store and the finalize ORs it with the carried
// base bits, so a day's flag is monotonic (an update rerun re-setting an
// already-set bit is harmless; no move store is kept — every modified node's
// distance is thresholded and discarded).
//
// Filter 1 (new users / reputation < 5%) is not extracted here: it joins
// user_reputation.parquet at query time.
//
// Filter 3's prior position is the center of the node's last known H3 cell
// (NodeState overlay from an earlier diff of the run, else the flat
// incremental cache base), because osc diffs carry only the new coordinates.
// At --h3-resolution 9 the cell radius is ~175 m, so the computed distance
// equals |old cell center -> new point| and differs from the true
// |old point -> new point| move by up to one cell radius — adequate for the
// 500 m screen, not for the paper's finer 11 m edit-analysis flag.
//
// The flags only cover the period after the recorded replication sequence
// (the diffs applied by update runs); import writes them as 0.

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace vandalism {

// Filter 2 flag threshold: "modified or deleted more than 500 objects within
// one hour" (paper sec. 5).
inline constexpr uint32_t kFilter2Threshold = 500;

// Filter 3 flag threshold: "nodes moved more than 500 metres" (paper sec. 5).
inline constexpr double kFilter3Threshold = 500.0;

// Bits of the users_history.parquet vandalism_flag column.
inline constexpr uint8_t kFlagFilter2 = 0x01;
inline constexpr uint8_t kFlagFilter3 = 0x02;

// Trailing window width in minutes (inclusive of the current minute).
inline constexpr uint32_t kHourSpanMinutes = 60;

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
// by flagged_move_days into the day-level bit-1 flag.
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
    // only a move beyond kFilter3Threshold is staged, as (uid, minute). Callers
    // must read the prior cell from NodeState::pre() before set_position().
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

// Reads the persisted minute store and returns one (uid, day) -> kFlagFilter2
// entry for every day holding at least one minute whose trailing-hour span
// exceeds kFilter2Threshold; day = minute / 1440 (UTC). Feeds the
// vandalism_flag bits the users-history update finalize writes into the
// daily history.
std::map<std::pair<int64_t, uint16_t>, uint8_t> flagged_days(const std::string& minutes_path);

// Folds one update run's staged >kFilter3Threshold node moves under
// `stage_root` (moves/ sub-tree) into this run's (uid, day) -> kFlagFilter3
// set: each staged (uid, minute) row becomes a flag on day = minute / 1440.
// Removes the stage root (and therefore the counts/ sub-tree already consumed
// by fold_minute_counts) afterwards. Flags are ORed by the caller, so folding
// a rerun's regenerated stages is a no-op.
std::map<std::pair<int64_t, uint16_t>, uint8_t> flagged_move_days(
    const std::string& stage_root);

}  // namespace vandalism