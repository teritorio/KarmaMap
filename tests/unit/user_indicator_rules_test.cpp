#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "user_indicators.hpp"

namespace {

using user_indicators::ObjectKind;
using user_indicators::Thresholds;
using user_indicators::UserDayKey;
using user_indicators::UserEventStats;
using user_indicators::haversine_meters;

constexpr uint16_t day(int32_t d) { return static_cast<uint16_t>(d); }

// Entry-level helper returning the (uid, day) row, creating a throwaway
// entry if it was never touched.
auto row_of(UserEventStats& s, int64_t uid, uint16_t d) {
    return s.days().at(UserDayKey{uid, d});
}

TEST(UserIndicatorRules, CountsCreateModifyDeletePerUserAndDay) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.add_version(7, "alice", day(100), true, 2, ObjectKind::Node, std::nullopt);
    s.add_version(8, "bob", day(101), true, 3, ObjectKind::Node, std::nullopt);
    s.add_version(7, "alice", day(102), false, 4, ObjectKind::Node, std::nullopt);
    s.add_version(7, "alice", day(103), true, 1, ObjectKind::Way, std::nullopt);
    s.add_version(7, "alice", day(104), false, 2, ObjectKind::Way, std::nullopt);
    s.add_version(8, "bob", day(105), true, 2, ObjectKind::Way, std::nullopt);

    auto r100 = row_of(s, 7, day(100));
    EXPECT_EQ(r100.row.node_created, 1);
    EXPECT_EQ(r100.row.node_modified, 1);
    EXPECT_EQ(r100.row.node_deleted, 0);

    auto r102 = row_of(s, 7, day(102));
    EXPECT_EQ(r102.row.node_deleted, 1);

    auto r103 = row_of(s, 7, day(103));
    EXPECT_EQ(r103.row.way_created, 1);
    auto r104 = row_of(s, 7, day(104));
    EXPECT_EQ(r104.row.way_deleted, 1);
    auto r105 = row_of(s, 8, day(105));
    EXPECT_EQ(r105.row.way_modified, 1);
}

TEST(UserIndicatorRules, AggregatesSameUserAndDay) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();
    s.begin_object();
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();

    EXPECT_EQ(s.days().size(), 1);
    EXPECT_EQ(row_of(s, 7, day(100)).row.node_created, 2);
}

TEST(UserIndicatorRules, RelocationPastThresholdCounts) {
    Thresholds t;  // relocate_meters = 500
    UserEventStats s(t);
    s.begin_object();
    // ~157 km move (0,0) -> (1,1)
    s.add_version(5, "alice", day(100), true, 1, ObjectKind::Node, std::pair{0.0, 0.0});
    s.add_version(5, "alice", day(101), true, 2, ObjectKind::Node, std::pair{1.0, 1.0});
    // ~111 m move (1,1) -> (1.001,1): below 500 m, no flag
    s.add_version(5, "alice", day(102), true, 3, ObjectKind::Node, std::pair{1.001, 1.0});
    s.end_object();

    EXPECT_EQ(row_of(s, 5, day(100)).row.relocated, 0);
    EXPECT_EQ(row_of(s, 5, day(101)).row.relocated, 1);
    EXPECT_EQ(row_of(s, 5, day(102)).row.relocated, 0);

    EXPECT_GT(haversine_meters(0.0, 0.0, 1.0, 1.0), 500.0);
    EXPECT_LT(haversine_meters(1.0, 1.0, 1.001, 1.0), 500.0);
}

TEST(UserIndicatorRules, ShortLivedDelete) {
    Thresholds t;  // short_life_days = 7
    t.short_life_days = 7;
    UserEventStats s(t);

    s.begin_object();
    s.add_version(3, "alice", day(10), true, 1, ObjectKind::Node, std::nullopt);
    s.add_version(3, "alice", day(12), false, 2, ObjectKind::Node, std::nullopt);
    s.end_object();

    s.begin_object();
    s.add_version(4, "bob", day(10), true, 1, ObjectKind::Node, std::nullopt);
    s.add_version(4, "bob", day(40), false, 2, ObjectKind::Node, std::nullopt);
    s.end_object();

    EXPECT_EQ(row_of(s, 3, day(12)).row.short_lived, 1);
    EXPECT_EQ(row_of(s, 4, day(40)).row.short_lived, 0);
}

TEST(UserIndicatorRules, RapidEditWindow) {
    Thresholds t;  // rapid_edit_versions = 5, window = 7
    UserEventStats s(t);
    s.begin_object();
    s.add_version(2, "alice", day(10), true, 1, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(11), true, 2, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(12), true, 3, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(13), true, 4, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(14), true, 5, ObjectKind::Node, std::nullopt);
    s.end_object();

    EXPECT_EQ(row_of(s, 2, day(10)).row.rapid_edit, 0);
    EXPECT_EQ(row_of(s, 2, day(14)).row.rapid_edit, 1);
}

TEST(UserIndicatorRules, RapidEditBeyondWindowNotFlagged) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    // 5 versions spread 9+ days apart: the window never holds >= 5.
    s.add_version(2, "alice", day(10), true, 1, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(19), true, 2, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(28), true, 3, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(37), true, 4, ObjectKind::Node, std::nullopt);
    s.add_version(2, "alice", day(46), true, 5, ObjectKind::Node, std::nullopt);
    s.end_object();

    for (int d = 10; d <= 46; d += 9) {
        EXPECT_EQ(row_of(s, 2, day(d)).row.rapid_edit, 0);
    }
}

TEST(UserIndicatorRules, ObjectRunsAreSeparatedAcrossKinds) {
    Thresholds t;
    UserEventStats s(t);
    // Node id 5 created day 100 ...
    s.begin_object();
    s.add_version(9, "alice", day(100), true, 1, ObjectKind::Node, std::pair{0.0, 0.0});
    s.end_object();
    // ... but a Way with the same numeric id starts a fresh run on day 200:
    // its delete on day 203 must be short-lived relative to day 200, not 100.
    s.begin_object();
    s.add_version(9, "alice", day(200), true, 1, ObjectKind::Way, std::nullopt);
    s.add_version(9, "alice", day(203), false, 2, ObjectKind::Way, std::nullopt);
    s.end_object();

    EXPECT_EQ(row_of(s, 9, day(203)).row.way_deleted, 1);
    EXPECT_EQ(row_of(s, 9, day(203)).row.short_lived, 1);
}

TEST(UserIndicatorRules, DayRowKeepsFirstUsernameOfTheDay) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    s.add_version(5, "bob", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();
    s.begin_object();
    s.add_version(5, "carol", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();
    s.begin_object();
    s.add_version(5, "carol", day(101), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();

    EXPECT_EQ(row_of(s, 5, day(100)).username, "bob");
    EXPECT_EQ(row_of(s, 5, day(101)).username, "carol");
}

TEST(UserIndicatorRules, ZeroUidIsCounted) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    s.add_version(0, "", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();
    EXPECT_EQ(row_of(s, 0, day(100)).row.node_created, 1);
    EXPECT_EQ(row_of(s, 0, day(100)).username, "");
}

TEST(UserIndicatorRules, RelationCreatedIsIsolated) {
    Thresholds t;
    UserEventStats s(t);
    // Relations count only visible v1 versions and never touch the node/way
    // run state (no short_lived/rapid_edit, no total_events contribution).
    s.record_relation_created(6, "alice", day(100));
    s.record_relation_created(6, "alice", day(100));
    s.record_relation_created(6, "alice", day(101));

    EXPECT_EQ(row_of(s, 6, day(100)).row.relation_created, 2);
    EXPECT_EQ(row_of(s, 6, day(100)).row.total_events(), 0);
    EXPECT_EQ(row_of(s, 6, day(101)).row.relation_created, 1);
}

TEST(UserIndicatorRules, RelationCreatedKeepsNodeRunState) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Node, std::nullopt);
    s.end_object();
    // A relation creation does not disturb the node/way counters already
    // recorded, and shares the (uid, day) entry.
    s.record_relation_created(6, "alice", day(100));

    auto r = row_of(s, 6, day(100));
    EXPECT_EQ(r.row.node_created, 1);
    EXPECT_EQ(r.row.relation_created, 1);
    EXPECT_EQ(r.username, "alice");
}

TEST(UserIndicatorRules, CreatedTagsAccumulatePerDayAndObject) {
    Thresholds t;
    UserEventStats s(t);
    // amenity + building on one node, building on a way, same (uid, day).
    s.begin_object();
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, std::nullopt, 0b101);
    s.end_object();
    s.begin_object();
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Way, std::nullopt, 0b100);
    s.end_object();

    auto r = row_of(s, 7, day(100));
    EXPECT_EQ(r.row.tag_amenity, 1);    // bit 0
    EXPECT_EQ(r.row.tag_building, 2);   // bit 2: both objects
    EXPECT_EQ(r.row.tag_boundary, 0);
    EXPECT_EQ(r.row.tag_highway, 0);    // bit 3
    EXPECT_EQ(r.row.relation_created, 0);
}

TEST(UserIndicatorRules, TagsIgnoredOnModifyAndDelete) {
    Thresholds t;
    UserEventStats s(t);
    s.begin_object();
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, std::nullopt, 0b001);
    s.add_version(7, "alice", day(101), true, 2, ObjectKind::Node, std::nullopt, 0b001);
    s.add_version(7, "alice", day(102), false, 3, ObjectKind::Node, std::nullopt, 0b001);
    s.add_version(7, "alice", day(103), true, 1, ObjectKind::Way, std::nullopt, 0b001);
    s.end_object();

    // Tags count on the creation only; later versions of the same object add
    // nothing even if the caller passes a mask.
    EXPECT_EQ(row_of(s, 7, day(100)).row.tag_amenity, 1);
    EXPECT_EQ(row_of(s, 7, day(101)).row.tag_amenity, 0);
    EXPECT_EQ(row_of(s, 7, day(102)).row.tag_amenity, 0);
    EXPECT_EQ(row_of(s, 7, day(103)).row.tag_amenity, 1);
}

TEST(UserIndicatorRules, RelationCreatedTagsAccumulate) {
    Thresholds t;
    UserEventStats s(t);
    s.record_relation_created(6, "alice", day(100), 0b110);  // boundary + building
    s.record_relation_created(6, "alice", day(100), 0b100);  // building again

    auto r = row_of(s, 6, day(100));
    EXPECT_EQ(r.row.relation_created, 2);
    EXPECT_EQ(r.row.tag_boundary, 1);
    EXPECT_EQ(r.row.tag_building, 2);
    EXPECT_EQ(r.row.tag_waterway, 0);
}

TEST(UserIndicatorRules, TagBitsMapToIndependentCounters) {
    Thresholds t;
    UserEventStats s(t);
    // Bit i of a created object's mask touches exactly the i-th tag counter.
    for (std::uint32_t i = 0; i < user_indicators::kTagCount; ++i) {
        // One object per (uid, day), so each row shows a single isolated bit.
        s.begin_object();
        s.add_version(9, "alice", day(100 + i), true, 1, ObjectKind::Node, std::nullopt,
                      1u << i);
        s.end_object();
    }
    for (std::uint32_t i = 0; i < user_indicators::kTagCount; ++i) {
        const auto& row = row_of(s, 9, day(100 + i)).row;
        const std::array<std::uint32_t, user_indicators::kTagCount> tag_total = {
            row.tag_amenity,  row.tag_boundary,  row.tag_building, row.tag_highway,
            row.tag_landuse,  row.tag_leisure,   row.tag_name,     row.tag_natural,
            row.tag_place,    row.tag_railway,   row.tag_sport,    row.tag_waterway,
        };
        for (std::uint32_t j = 0; j < user_indicators::kTagCount; ++j) {
            EXPECT_EQ(tag_total[j], (i == j) ? 1u : 0u) << "bit " << i << " vs counter " << j;
        }
    }
}

}  // namespace
