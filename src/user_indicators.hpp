#pragma once

// User-indicator computation: an optional, H3-independent mode that scores
// the OSM full history per contributing user and per UTC day. Two outputs:
//
//   user_indicators.parquet  per (uid, change_date) activity counters
//                            (uid, change_date, node/way/relation counters,
//                             relocated, short_lived, rapid_edit, tag_*)
//   user_reputation.parquet  per-uid reputation + full indicator totals
//                            (uid, username, first_seen_day, bulk_new_user,
//                             max_day_changes, reputation, 22 counter totals,
//                             per-aspect pct; active/max in file metadata)
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
// reputation is built from the objects a contributor created; modifications
// and deletions only feed the edit-level vandalism value (former-owner
// reputation, former version number, edit date), approximated here by the
// short_lived and rapid_edit counters. Relation modifies/deletes are
// therefore not counted, and total_events() deliberately excludes them too.
//
// The reputation's tag aspect counts the "Top12" most-used tags (up to 4
// points each, paper sec. 4) on created objects, one counter per tag (see
// kTop12TagKeys below: bit i of a created object's tag mask maps to the
// i-th tag_* DayRow member and the i-th trailing entry in kCounters).
// The paper's "address" key is replaced by "place", since OSM address
// tagging uses the addr: prefix. Like relation_created, tag usage is
// reputation-only and never part of total_events().

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace user_indicators {

constexpr size_t kFlushThreshold = 1'000'000;  // accumulator rows per stage flush

// Defaults for the rule thresholds, shared by Thresholds and the CLI
// Options surface (--relocate-meters, --short-life-days, ...).
constexpr double kDefaultRelocateMeters = 500.0;
constexpr int kDefaultShortLifeDays = 7;
constexpr int kDefaultRapidEditVersions = 5;
constexpr int kDefaultRapidEditWindowDays = 7;
constexpr int kDefaultNewUserWindowDays = 30;
constexpr int kDefaultBulkEditMin = 10;

// Rule thresholds. The values are documented starting points, not calibrated
// against ground truth (vandalism is only ever "suspicion" from history).
struct Thresholds {
    double relocate_meters = kDefaultRelocateMeters;       // node move distance that counts
    int short_life_days = kDefaultShortLifeDays;           // created+deleted within N days
    int rapid_edit_versions = kDefaultRapidEditVersions;   // >= K versions ...
    int rapid_edit_window_days = kDefaultRapidEditWindowDays;  // ... within N days
    int new_user_window_days = kDefaultNewUserWindowDays;  // account younger than N days
    int bulk_edit_min = kDefaultBulkEditMin;               // >= N events in a day to flag
};

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
    uint32_t relocated = 0;
    uint32_t short_lived = 0;
    uint32_t rapid_edit = 0;
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

// 6 node/way change counters + relocated/short_lived/rapid_edit +
// relation_created + kTagCount tag counters.
constexpr size_t kCounterCount = 10 + kTagCount;

constexpr std::array<CounterSpec, kCounterCount> kCounters = {{
    {"node_created",     &DayRow::node_created},
    {"node_modified",    &DayRow::node_modified},
    {"node_deleted",     &DayRow::node_deleted},
    {"way_created",      &DayRow::way_created},
    {"way_modified",     &DayRow::way_modified},
    {"way_deleted",      &DayRow::way_deleted},
    {"relocated",        &DayRow::relocated},
    {"short_lived",      &DayRow::short_lived},
    {"rapid_edit",       &DayRow::rapid_edit},
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

inline double haversine_meters(double lat1_deg, double lon1_deg, double lat2_deg,
                               double lon2_deg) {
    constexpr double kEarthRadius = 6371000.0;  // mean radius in meters
    const double lat1 = lat1_deg * M_PI / 180.0;
    const double lat2 = lat2_deg * M_PI / 180.0;
    const double dlat = (lat2_deg - lat1_deg) * M_PI / 180.0;
    const double dlon = (lon2_deg - lon1_deg) * M_PI / 180.0;
    const double a = std::sin(dlat / 2.0) * std::sin(dlat / 2.0) +
                     std::cos(lat1) * std::cos(lat2) * std::sin(dlon / 2.0) *
                         std::sin(dlon / 2.0);
    return kEarthRadius * 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

// Pure, osmium-free aggregation + rule state. The scan handler groups an
// object's contiguous versions with begin_object()/add_version()/end_object();
// every version is scored immediately and attributed to the editing
// (uid, day). Unit-testable without a history file.
class UserEventStats {
public:
    explicit UserEventStats(const Thresholds& thresholds) : t_(thresholds) {}

    void begin_object() {
        run_.active = true;
        run_.first_day = 0;
        run_.recent_days.clear();
        run_.last_coords.reset();
        run_.first_seen = false;
    }

    void add_version(int64_t uid, const std::string& username, uint16_t day, bool visible,
                     uint32_t version, ObjectKind kind,
                     std::optional<std::pair<double, double>> coords,
                     uint32_t created_tag_bits = 0) {
        if (!run_.active) begin_object();
        if (!run_.first_seen) {
            run_.first_day = day;
            run_.first_seen = true;
        }
        run_.kind = kind;

        UserDayEntry& e = days_[UserDayKey{uid, day}];
        if (e.username.empty()) e.username = username;
        DayRow& r = e.row;

        if (!visible) {
            if (kind == ObjectKind::Node) {
                r.node_deleted++;
            } else {
                r.way_deleted++;
            }
            if (run_.first_seen && day >= run_.first_day &&
                static_cast<int>(day - run_.first_day) <= t_.short_life_days) {
                r.short_lived++;
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

        // Relocation compares a node version's coordinates against the last
        // version that actually carried coordinates; location-less versions
        // (deletions) keep the previous baseline.
        if (kind == ObjectKind::Node && coords) {
            if (run_.last_coords) {
                if (haversine_meters(run_.last_coords->first, run_.last_coords->second,
                                     coords->first, coords->second) > t_.relocate_meters) {
                    r.relocated++;
                }
            }
            run_.last_coords = coords;
        }

        // Rapid re-edit: the editing version is flagged when the window
        // [day - N, day] already holds >= K versions (counting this one).
        run_.recent_days.push_back(day);
        while (!run_.recent_days.empty() &&
               static_cast<int>(day - run_.recent_days.front()) > t_.rapid_edit_window_days) {
            run_.recent_days.erase(run_.recent_days.begin());
        }
        if (static_cast<int>(run_.recent_days.size()) >= t_.rapid_edit_versions) {
            r.rapid_edit++;
        }
    }

    void end_object() { run_.active = false; }

    // Isolated relation-created accounting: relations arrive as their own
    // contiguous runs, but only visible v1 versions count, and they must not
    // influence the node/way totalled run state (short_lived, rapid_edit).
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
        bool first_seen = false;
        uint16_t first_day = 0;
        std::optional<std::pair<double, double>> last_coords;
        std::vector<uint16_t> recent_days;
        ObjectKind kind = ObjectKind::Node;
    };

    Thresholds t_;
    RunState run_;
    std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash> days_;
};

// One streaming scan + finalize of the stage data. See user_indicators.cpp.
void run_scan(const std::string& input_path, const std::string& stage_dir,
              const Thresholds& thresholds);

void run_finalize(const std::string& stage_dir, const std::string& indicators_path,
                  const Thresholds& thresholds);

}  // namespace user_indicators