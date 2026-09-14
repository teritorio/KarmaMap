#pragma once

// User-indicator computation: an optional, H3-independent mode that scores
// the OSM full history per contributing user and per UTC day. Two outputs:
//
//   user_profiles.parquet    per (uid, username) validity segment
//                            (uid, username, first_edit_day, first_seen_day,
//                             bulk_new_user)
//   user_indicators.parquet  per (uid, change_date) activity counters
//                            (uid, change_date, 9 counters)
//
// Both are non-partitioned, with user_profiles sorted by (uid,
// first_edit_day) and user_indicators by (uid, change_date), for cheap
// joins on uid.
//
// The scan is a single streaming pass over the history (entity bits
// node|way). OSM full-history files are sorted by (object id, version), so
// all versions of one object are contiguous: each object's run is scored and
// forgotten as the stream advances (O(1) object state), and every event is
// attributed to the editing (uid, day). No changeset metadata is required.

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace user_indicators {

constexpr size_t kFlushThreshold = 1'000'000;  // accumulator rows per stage flush

// Rule thresholds. The values are documented starting points, not calibrated
// against ground truth (vandalism is only ever "suspicion" from history).
struct Thresholds {
    double relocate_meters = 1000.0;        // node move distance that counts
    int short_life_days = 7;                // created+deleted within N days
    int rapid_edit_versions = 5;            // >= K versions ...
    int rapid_edit_window_days = 7;         // ... within N days
    int new_user_window_days = 30;          // account younger than N days
    int bulk_edit_min = 10;                 // >= N events in a day to flag
};

enum class ObjectKind { Node, Way };

// Per-(uid, change_date) counters.
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

    uint32_t total_events() const {
        return node_created + node_modified + node_deleted + way_created +
               way_modified + way_deleted;
    }
};

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
                     std::optional<std::pair<double, double>> coords) {
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

    const std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash>& days() const {
        return days_;
    }

    std::unordered_map<UserDayKey, UserDayEntry, UserDayKeyHash>& days() { return days_; }

    size_t size() const { return days_.size(); }

private:
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

void run_finalize(const std::string& stage_dir, const std::string& profiles_path,
                  const std::string& indicators_path, const Thresholds& thresholds);

}  // namespace user_indicators