#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>

#include "user_indicators.hpp"

namespace {

using user_indicators::ObjectKind;
using user_indicators::UserDayKey;
using user_indicators::UserEventStats;

constexpr uint16_t day(int32_t d) { return static_cast<uint16_t>(d); }

// Entry-level helper returning the (uid, day) row, creating a throwaway
// entry if it was never touched.
auto row_of(UserEventStats& s, int64_t uid, uint16_t d) {
    return s.days().at(UserDayKey{uid, d});
}

TEST(UserIndicatorRules, CountsCreateModifyDeletePerUserAndDay) {
    UserEventStats s;
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node);
    s.add_version(7, "alice", day(100), true, 2, ObjectKind::Node);
    s.add_version(8, "bob", day(101), true, 3, ObjectKind::Node);
    s.add_version(7, "alice", day(102), false, 4, ObjectKind::Node);
    s.add_version(7, "alice", day(103), true, 1, ObjectKind::Way);
    s.add_version(7, "alice", day(104), false, 2, ObjectKind::Way);
    s.add_version(8, "bob", day(105), true, 2, ObjectKind::Way);

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
    UserEventStats s;
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node);
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node);

    EXPECT_EQ(s.days().size(), 1);
    EXPECT_EQ(row_of(s, 7, day(100)).row.node_created, 2);
}

TEST(UserIndicatorRules, DayRowKeepsFirstUsernameOfTheDay) {
    UserEventStats s;
    s.add_version(5, "bob", day(100), true, 1, ObjectKind::Node);
    s.add_version(5, "carol", day(100), true, 1, ObjectKind::Node);
    s.add_version(5, "carol", day(101), true, 1, ObjectKind::Node);

    EXPECT_EQ(row_of(s, 5, day(100)).username, "bob");
    EXPECT_EQ(row_of(s, 5, day(101)).username, "carol");
}

TEST(UserIndicatorRules, ZeroUidIsCounted) {
    UserEventStats s;
    s.add_version(0, "", day(100), true, 1, ObjectKind::Node);
    EXPECT_EQ(row_of(s, 0, day(100)).row.node_created, 1);
    EXPECT_EQ(row_of(s, 0, day(100)).username, "");
}

TEST(UserIndicatorRules, RelationCreatedIsIsolated) {
    UserEventStats s;
    // Relations count only visible v1 versions and never touch the node/way
    // counters.
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Relation);
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Relation);
    s.add_version(6, "alice", day(101), true, 1, ObjectKind::Relation);

    EXPECT_EQ(row_of(s, 6, day(100)).row.relation_created, 2);
    const auto& r100 = row_of(s, 6, day(100)).row;
    EXPECT_EQ(r100.node_created + r100.node_modified + r100.node_deleted +
                  r100.way_created + r100.way_modified + r100.way_deleted,
              0);
    EXPECT_EQ(row_of(s, 6, day(101)).row.relation_created, 1);
}

TEST(UserIndicatorRules, RelationCreatedSharesUidDayWithNode) {
    UserEventStats s;
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Node);
    // A relation creation does not disturb the node/way counters already
    // recorded, and shares the (uid, day) entry.
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Relation);

    auto r = row_of(s, 6, day(100));
    EXPECT_EQ(r.row.node_created, 1);
    EXPECT_EQ(r.row.relation_created, 1);
    EXPECT_EQ(r.username, "alice");
}

TEST(UserIndicatorRules, CreatedTagsAccumulatePerDayAndObject) {
    UserEventStats s;
    // amenity + building on one node, building on a way, same (uid, day).
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, 0b101);
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Way, 0b100);

    auto r = row_of(s, 7, day(100));
    EXPECT_EQ(r.row.tag_amenity, 1);    // bit 0
    EXPECT_EQ(r.row.tag_building, 2);   // bit 2: both objects
    EXPECT_EQ(r.row.tag_boundary, 0);
    EXPECT_EQ(r.row.tag_highway, 0);    // bit 3
    EXPECT_EQ(r.row.relation_created, 0);
}

TEST(UserIndicatorRules, TagsIgnoredOnModifyAndDelete) {
    UserEventStats s;
    s.add_version(7, "alice", day(100), true, 1, ObjectKind::Node, 0b001);
    s.add_version(7, "alice", day(101), true, 2, ObjectKind::Node, 0b001);
    s.add_version(7, "alice", day(102), false, 3, ObjectKind::Node, 0b001);
    s.add_version(7, "alice", day(103), true, 1, ObjectKind::Way, 0b001);

    // Tags count on the creation only; later versions of the same object add
    // nothing even if the caller passes a mask.
    EXPECT_EQ(row_of(s, 7, day(100)).row.tag_amenity, 1);
    EXPECT_EQ(row_of(s, 7, day(101)).row.tag_amenity, 0);
    EXPECT_EQ(row_of(s, 7, day(102)).row.tag_amenity, 0);
    EXPECT_EQ(row_of(s, 7, day(103)).row.tag_amenity, 1);
}

TEST(UserIndicatorRules, RelationCreatedTagsAccumulate) {
    UserEventStats s;
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Relation, 0b110);  // boundary + building
    s.add_version(6, "alice", day(100), true, 1, ObjectKind::Relation, 0b100);  // building again

    auto r = row_of(s, 6, day(100));
    EXPECT_EQ(r.row.relation_created, 2);
    EXPECT_EQ(r.row.tag_boundary, 1);
    EXPECT_EQ(r.row.tag_building, 2);
    EXPECT_EQ(r.row.tag_waterway, 0);
}

TEST(UserIndicatorRules, TagBitsMapToIndependentCounters) {
    UserEventStats s;
    // Bit i of a created object's mask touches exactly the i-th tag counter.
    for (std::uint32_t i = 0; i < user_indicators::kTagCount; ++i) {
        // One object per (uid, day), so each row shows a single isolated bit.
        s.add_version(9, "alice", day(100 + i), true, 1, ObjectKind::Node, 1u << i);
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
