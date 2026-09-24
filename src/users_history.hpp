#pragma once

// Users-history computation: an optional, H3-independent mode that scores
// the OSM full history per contributing user and per UTC day. Two outputs:
//
//   users_history.parquet  per (uid, change_date) activity counter plus the
//                            vandalism flag bits:
//                            (uid, change_date, count = the six node/way
//                            change counters + relation_created +
//                            relation_modified + relation_deleted;
//                            vandalism_flag = bits of the day's vandalism
//                            screens — bit 0 (kFlagFilter2): any of the
//                            day's minutes had > 500 modified+deleted
//                            objects in a one-hour window; bit 1
//                            (kFlagFilter3): a modified node was moved
//                            > 500 m that day; bit 2 (kFlagFilter1): the day's
//                            user has reputation below 5% ("new users or low
//                            reputation"). Bits 0/1 are filled from
//                            the persisted minute store and the run's move
//                            stages by the update finalize, 0 on import;
//                            bit 2 is forward-only, set only on rows the
//                            update finalize newly writes for a below-threshold
//                            contributor (also 0 on import), and monotonic:
//                            base rows are carried unchanged, so a flag once
//                            written persists. The per-day
//                            tag_* counters are aggregated in finalize and
//                            surface only as reputation totals below)
//   user_reputation.parquet  per-uid reputation + full history totals
//                            (uid, username, first_seen_day, reputation,
//                             21 counter totals, per-aspect pct;
//                             active/max in file metadata)
//   vandalism.parquet  update-only; the flagged (uid, change_date) rows
//                      of users_history.parquet, re-joined with the current
//                      username (uid, username, change_date, vandalism_flag),
//                      sorted by (change_date, uid). Written by the update
//                      finalize from the same merged flag state as the
//                      history file, so the two always agree; a pure import
//                      writes no flags and produces no vandalism.parquet.
//
// users_history.parquet and user_reputation.parquet are non-partitioned,
// with users_history sorted by (uid, change_date) and user_reputation.parquet
// by username (uid tie-break), so an exact username filter in the users viewer
// prunes to the matching pages; the per-day history table still joins on uid
// for the timeline.
//
// The scan is a single streaming pass over the history (entity bits
// node|way|relation). OSM full-history files are sorted by (object id,
// version), so all versions of one object are contiguous: each object's run
// is scored and forgotten as the stream advances (O(1) object state), and
// every event is attributed to the editing (uid, day). No changeset
// metadata is required.
//
// Relations obey the same node/way classification (visible version 1 =
// created, visible later versions = modified, invisible = deleted). Following
// the OSMPatrol model (Neis, Goetz & Zipf 2012), the per-user reputation is
// built from the objects a contributor created, so relation modifies/deletes
// (and the node/way ones) feed only the per-day activity count, never the
// reputation.
//
// The reputation's tag aspect counts the "Top12" most-used tags (up to 4
// points each, paper sec. 4) on created objects, one counter per tag (see
// kTop12TagKeys below: bit i of a created object's tag mask maps to the
// i-th tag_* DayRow member and the i-th trailing entry in kCounters).
// The paper's "address" key is replaced by "place", since OSM address
// tagging uses the addr: prefix. Like relation_created, tag usage is
// reputation-only and never part of the node/way activity totals.

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace users_history {

constexpr size_t kFlushThreshold = 1'000'000;  // accumulator rows per stage flush

enum class ObjectKind { Node, Way, Relation };

// Per-(uid, change_date) counters. The tag_* counters mirror the Top12 tag
// key order in kTop12TagKeys; see apply_created_tags() and kCounters.
struct DayRow {
    uint32_t node_created = 0;
    uint32_t node_modified = 0;
    uint32_t node_deleted = 0;
    uint32_t way_created = 0;
    uint32_t way_modified = 0;
    uint32_t way_deleted = 0;
    uint32_t relation_created = 0;
    uint32_t relation_modified = 0;
    uint32_t relation_deleted = 0;
    uint32_t tag_amenity = 0;
    uint32_t tag_boundary = 0;
    uint32_t tag_building = 0;
    uint32_t tag_highway = 0;
    uint32_t tag_landuse = 0;
    uint32_t tag_leisure = 0;
    uint32_t tag_name = 0;
    uint32_t tag_natural = 0;
    uint32_t tag_place = 0;
    uint32_t tag_railway = 0;
    uint32_t tag_sport = 0;
    uint32_t tag_waterway = 0;
};

// Top12 tag keys in reputation-aspect order (bit i of a created object's
// tag mask ↔ kTop12TagKeys[i] ↔ the i-th trailing tag entry in kCounters).
// "address" from the paper is replaced by "place" (OSM addr: prefix).
constexpr size_t kTagCount = 12;
constexpr std::array<std::string_view, kTagCount> kTop12TagKeys = {
    "amenity", "boundary", "building", "highway", "landuse", "leisure",
    "name",    "natural",  "place",    "railway", "sport",   "waterway",
};

// Single source of truth for the Parquet column names, the stage/finalize
// loops and the created-tag bit spreading. The kTagCount trailing entries
// are in kTop12TagKeys order; DayRow's tag_* members must mirror them.
struct CounterSpec {
    const char* name;
    uint32_t DayRow::* member;
};

// 6 node/way change counters + 3 relation counters + kTagCount tag counters.
constexpr size_t kCounterCount = 9 + kTagCount;

constexpr std::array<CounterSpec, kCounterCount> kCounters = {{
    {"node_created",     &DayRow::node_created},
    {"node_modified",    &DayRow::node_modified},
    {"node_deleted",     &DayRow::node_deleted},
    {"way_created",      &DayRow::way_created},
    {"way_modified",     &DayRow::way_modified},
    {"way_deleted",      &DayRow::way_deleted},
    {"relation_created", &DayRow::relation_created},
    {"relation_modified",&DayRow::relation_modified},
    {"relation_deleted", &DayRow::relation_deleted},
    {"tag_amenity",      &DayRow::tag_amenity},
    {"tag_boundary",     &DayRow::tag_boundary},
    {"tag_building",     &DayRow::tag_building},
    {"tag_highway",      &DayRow::tag_highway},
    {"tag_landuse",      &DayRow::tag_landuse},
    {"tag_leisure",      &DayRow::tag_leisure},
    {"tag_name",         &DayRow::tag_name},
    {"tag_natural",      &DayRow::tag_natural},
    {"tag_place",        &DayRow::tag_place},
    {"tag_railway",      &DayRow::tag_railway},
    {"tag_sport",        &DayRow::tag_sport},
    {"tag_waterway",     &DayRow::tag_waterway},
}};

struct UserDayKey {
    int64_t uid;
    uint16_t day;

    bool operator==(const UserDayKey& o) const { return uid == o.uid && day == o.day; }
};

struct UserDayKeyHash {
    size_t operator()(const UserDayKey& k) const noexcept {
        return std::hash<int64_t>()(k.uid) ^
               (std::hash<uint16_t>()(k.day) + 0x9e3779b97f4a7c15ULL);
    }
};

struct UserDayEntry {
    std::string username;  // username seen first that day for this uid
    DayRow row;
};

// Pure, osmium-free aggregation. The scan handler feeds every version to
// add_version(), which scores it immediately and attributes it to the editing
// (uid, day). Object boundaries are irrelevant to the counters, so the class
// keeps no object run state. Unit-testable without a history file.
class UserEventStats {
public:
    UserEventStats() = default;

    void add_version(int64_t uid, const std::string& username, uint16_t day, bool visible,
                     uint32_t version, ObjectKind kind,
                     uint32_t created_tag_bits = 0) {
        UserDayEntry& e = days_[UserDayKey{uid, day}];
        if (e.username.empty()) e.username = username;
        DayRow& r = e.row;

        switch (kind) {
            case ObjectKind::Node:
                if (!visible) {
                    r.node_deleted++;
                } else if (version == 1) {
                    r.node_created++;
                    apply_created_tags(r, created_tag_bits);
                } else {
                    r.node_modified++;
                }
                break;
            case ObjectKind::Way:
                if (!visible) {
                    r.way_deleted++;
                } else if (version == 1) {
                    r.way_created++;
                    apply_created_tags(r, created_tag_bits);
                } else {
                    r.way_modified++;
                }
                break;
            case ObjectKind::Relation:
                if (!visible) {
                    r.relation_deleted++;
                } else if (version == 1) {
                    r.relation_created++;
                    // Reputation's tag aspect counts the Top12 tags used
                    // during the creation only (paper sec. 4).
                    apply_created_tags(r, created_tag_bits);
                } else {
                    r.relation_modified++;
                }
                break;
        }
    }

    const std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash>& days() const {
        return days_;
    }

    std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash>& days() { return days_; }

    size_t size() const { return days_.size(); }

private:
    // Spreads a created object's Top12 tag mask into the per-tag counters.
    // Bit i of the mask corresponds to kTop12TagKeys[i] and the i-th
    // trailing tag entry in kCounters.
    static void apply_created_tags(DayRow& r, uint32_t bits) {
        constexpr size_t first_tag = kCounterCount - kTagCount;
        for (uint32_t i = 0; i < kTagCount; ++i) {
            if (bits & (1u << i)) ++(r.*kCounters[first_tag + i].member);
        }
    }

    std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash> days_;
};

// One streaming scan + finalize of the stage data. See users_history.cpp.
void run_scan(const std::string& input_path, const std::string& stage_dir);

void run_finalize(const std::string& stage_dir, const std::string& history_path,
                  int64_t users_history_group_rows, int64_t reputation_group_rows);

// Update mode: scans one replication diff (an .osc.gz change file) into a
// fresh stage_dir. The scan handler classifies diff objects the same way the
// history scan does (visible version 1 = created, later versions = modified,
// invisible = deleted); the version_ == 1 test matches a diff, whose creates
// always carry version 1.
void run_scan_diff(const std::string& diff_path, const std::string& stage_dir);

// Merges the per-diff update stage dirs under `stage_root` (each
// stage_root/seq_<n>/stage_*.parquet) into the existing datasets: the per
// (uid, change_date) deltas are summed into users_history.parquet, and
// user_reputation.parquet is recomputed from the existing per-uid totals plus
// the diff totals (so newly appeared contributors join the ranking). The
// vandalism_flag bits are rebuilt over the whole history: bit 0 (kFlagFilter2)
// from the persisted minute store at `minutes_path` (vandalism::flagged_days),
// bit 1 (kFlagFilter3) from `move_flags`, the run's (uid, change_date) ->
// kFlagFilter3 set produced by vandalism::flagged_move_days, which the caller
// must have folded first. All bits are monotonic and forward-only: the
// existing file's flags are carried forward unchanged (import writes 0), this
// run's bit-0/1 sets are ORed in, and bit 2 (kFlagFilter1, low reputation) is
// set only on the run's newly-written rows (keys absent from the base
// history) whose user's current reputation is below the threshold. A
// reputation drop never re-flags the base rows and a once-set bit is never
// masked out. Called once per update run. The existing files must carry the
// schemas written by run_finalize (full-run datasets) or a previous update
// finalize. Alongside the history rewrite it also writes vandalism.parquet
// (one row per flagged (uid, change_date) with the current username, sorted
// by (change_date, uid)); a pure import never produces it.
void run_update_finalize(const std::string& stage_root, const std::string& history_path,
                         int64_t users_history_group_rows, int64_t reputation_group_rows,
                         const std::string& minutes_path,
                         const std::map<std::pair<int64_t, uint16_t>, uint8_t>& move_flags);

}  // namespace users_history
