#pragma once

// User-indicator computation: an optional, H3-independent mode that scores
// the OSM full history per contributing user and per UTC day. Two outputs:
//
//   user_indicators.parquet  per (uid, change_date) activity counters
//                            (uid, change_date, node/way/relation counters,
//                            tag_*)
//   user_reputation.parquet  per-uid reputation + full indicator totals
//                            (uid, username, first_seen_day, reputation,
//                             19 counter totals, per-aspect pct;
//                             active/max in file metadata)
//
// Both are non-partitioned, with user_indicators sorted by (uid,
// change_date) and user_reputation.parquet by username (uid tie-break), so
// an exact username filter in the users viewer prunes to the matching pages;
// the per-day indicator table still joins on uid for the timeline.
//
// The scan is a single streaming pass over the history (entity bits
// node|way|relation). OSM full-history files are sorted by (object id,
// version), so all versions of one object are contiguous: each object's run
// is scored and forgotten as the stream advances (O(1) object state), and
// every event is attributed to the editing (uid, day). No changeset
// metadata is required.
//
// Relations contribute only a created counter (record_relation_created).
// Following the OSMPatrol model (Neis, Goetz & Zipf 2012), the per-user
// reputation is built from the objects a contributor created. Relation
// modifies/deletes are therefore not counted, and total_events()
// deliberately excludes them too.
//
// The reputation's tag aspect counts the "Top12" most-used tags (up to 4
// points each, paper sec. 4) on created objects, one counter per tag (see
// kTop12TagKeys below: bit i of a created object's tag mask maps to the
// i-th tag_* DayRow member and the i-th trailing entry in kCounters).
// The paper's "address" key is replaced by "place", since OSM address
// tagging uses the addr: prefix. Like relation_created, tag usage is
// reputation-only and never part of total_events().

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

namespace user_indicators {

constexpr size_t kFlushThreshold = 1'000'000;  // accumulator rows per stage flush

enum class ObjectKind { Node, Way };

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

    // Only node/way events count as edits; relations are tracked for the
    // reputation's created-relations aspect, not for activity volume.
    uint32_t total_events() const {
        return node_created + node_modified + node_deleted + way_created +
               way_modified + way_deleted;
    }
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

// 6 node/way change counters + relation_created + kTagCount tag counters.
constexpr size_t kCounterCount = 7 + kTagCount;

constexpr std::array<CounterSpec, kCounterCount> kCounters = {{
    {"node_created",     &DayRow::node_created},
    {"node_modified",    &DayRow::node_modified},
    {"node_deleted",     &DayRow::node_deleted},
    {"way_created",      &DayRow::way_created},
    {"way_modified",     &DayRow::way_modified},
    {"way_deleted",      &DayRow::way_deleted},
    {"relation_created", &DayRow::relation_created},
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

// Pure, osmium-free aggregation. The scan handler groups an object's
// contiguous versions with begin_object()/add_version()/end_object();
// every version is scored immediately and attributed to the editing
// (uid, day). Unit-testable without a history file.
class UserEventStats {
public:
    UserEventStats() = default;

    void begin_object() { run_.active = true; }

    void add_version(int64_t uid, const std::string& username, uint16_t day, bool visible,
                     uint32_t version, ObjectKind kind,
                     uint32_t created_tag_bits = 0) {
        if (!run_.active) begin_object();

        UserDayEntry& e = days_[UserDayKey{uid, day}];
        if (e.username.empty()) e.username = username;
        DayRow& r = e.row;

        if (!visible) {
            if (kind == ObjectKind::Node) {
                r.node_deleted++;
            } else {
                r.way_deleted++;
            }
        } else if (version == 1) {
            if (kind == ObjectKind::Node) {
                r.node_created++;
            } else {
                r.way_created++;
            }
            // Reputation's tag aspect counts the Top12 tags used during the
            // creation only (paper sec. 4).
            apply_created_tags(r, created_tag_bits);
        } else {
            if (kind == ObjectKind::Node) {
                r.node_modified++;
            } else {
                r.way_modified++;
            }
        }
    }

    void end_object() { run_.active = false; }

    // Isolated relation-created accounting: relations arrive as their own
    // contiguous runs; only visible v1 versions count, and they must not
    // influence the node/way run state.
    void record_relation_created(int64_t uid, const std::string& username, uint16_t day,
                                 uint32_t created_tag_bits = 0) {
        UserDayEntry& e = days_[UserDayKey{uid, day}];
        if (e.username.empty()) e.username = username;
        DayRow& r = e.row;
        r.relation_created++;
        apply_created_tags(r, created_tag_bits);
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

    struct RunState {
        bool active = false;
    };

    RunState run_;
    std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash> days_;
};

// One streaming scan + finalize of the stage data. See user_indicators.cpp.
void run_scan(const std::string& input_path, const std::string& stage_dir);

void run_finalize(const std::string& stage_dir, const std::string& indicators_path);

}  // namespace user_indicators
