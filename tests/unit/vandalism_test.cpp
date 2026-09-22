#include <gtest/gtest.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "vandalism.hpp"

namespace {

// ---------------------------------------------------------------------------
// MinuteStats: the pure per-(uid, minute) modified+deleted counter
// ---------------------------------------------------------------------------

TEST(VandalismMinuteStats, CountsModifiesAndDeletesOnly) {
    vandalism::MinuteStats stats;
    // Create (visible version 1) is ignored by filter 2.
    stats.add_object(10, "alice", 1000, true, 1);
    // Modify (visible version > 1) counts.
    stats.add_object(10, "alice", 1000, true, 3);
    stats.add_object(10, "alice", 1000, true, 4);
    // Delete (invisible) counts regardless of version.
    stats.add_object(10, "alice", 1001, false, 7);
    EXPECT_EQ(stats.size(), 2);
}

TEST(VandalismMinuteStats, BucketsByUtcMinute) {
    vandalism::MinuteStats stats;
    // Two edits in minute 1 share a key; distinct minutes stay separate.
    stats.add_object(11, "bob", 0, true, 2);
    stats.add_object(11, "bob", 1, true, 2);
    stats.add_object(11, "bob", 1, true, 2);
    stats.add_object(11, "bob", 2, true, 2);
    ASSERT_EQ(stats.size(), 3);
    EXPECT_EQ(stats.minutes().count(vandalism::UserMinuteKey{11, 0}), 1);
    EXPECT_EQ(stats.minutes().at(vandalism::UserMinuteKey{11, 1}).modified_deleted, 2);
    EXPECT_EQ(stats.minutes().count(vandalism::UserMinuteKey{11, 2}), 1);
}

TEST(VandalismMinuteStats, DifferentUsersAndMinutesStaySeparate) {
    vandalism::MinuteStats stats;
    stats.add_object(10, "alice", 100, true, 2);
    stats.add_object(10, "alice", 100, true, 2);
    stats.add_object(11, "bob", 100, true, 2);
    stats.add_object(11, "bob", 101, true, 2);
    ASSERT_EQ(stats.size(), 3);
    EXPECT_EQ(stats.minutes().at(vandalism::UserMinuteKey{10, 100}).modified_deleted, 2);
    EXPECT_EQ(stats.minutes().at(vandalism::UserMinuteKey{11, 100}).modified_deleted, 1);
    EXPECT_EQ(stats.minutes().at(vandalism::UserMinuteKey{11, 101}).modified_deleted, 1);
}

TEST(VandalismMinuteStats, FirstUsernameSeenWins) {
    vandalism::MinuteStats stats;
    stats.add_object(10, "alice", 100, true, 2);
    stats.add_object(10, "alice_alias", 100, true, 3);
    stats.add_object(11, "bob", 100, true, 2);
    ASSERT_EQ(stats.size(), 2);
    EXPECT_EQ(stats.minutes().at(vandalism::UserMinuteKey{10, 100}).username, "alice");
    EXPECT_EQ(stats.minutes().at(vandalism::UserMinuteKey{11, 100}).username, "bob");
}

// ---------------------------------------------------------------------------
// hour_spans: the trailing 60-minute window and the filter-2 flag
// ---------------------------------------------------------------------------

TEST(VandalismHourSpans, SingleMinuteAboveThresholdFlags) {
    const auto spans = vandalism::hour_spans({{2000, 501}});
    ASSERT_EQ(spans.size(), 1);
    EXPECT_EQ(spans[0].hour_span, 501);
    EXPECT_EQ(spans[0].flagged, 1);
}

TEST(VandalismHourSpans, ExactlyThresholdIsNotFlagged) {
    const auto spans = vandalism::hour_spans({{100, 500}});
    ASSERT_EQ(spans.size(), 1);
    EXPECT_EQ(spans[0].hour_span, 500);
    EXPECT_EQ(spans[0].flagged, 0);
}

TEST(VandalismHourSpans, WindowDropsAntiqueMinutes) {
    // minute_counts: 5 -> 100, 35 -> 400 (500 together), 65 -> 150.
    const auto spans = vandalism::hour_spans({{5, 100}, {35, 400}, {65, 150}});
    ASSERT_EQ(spans.size(), 3);
    // minute 5: only its own count.
    EXPECT_EQ(spans[0].hour_span, 100);
    EXPECT_EQ(spans[0].flagged, 0);
    // minute 35: window [1..35] holds 100 + 400 = 500, not > 500.
    EXPECT_EQ(spans[1].hour_span, 500);
    EXPECT_EQ(spans[1].flagged, 0);
    // minute 65: window [6..65] drops minute 5; 400 + 150 = 550.
    EXPECT_EQ(spans[2].hour_span, 550);
    EXPECT_EQ(spans[2].flagged, 1);
}

TEST(VandalismHourSpans, GapEmptiesWindow) {
    // 120 minutes apart: the earlier count falls out entirely.
    const auto spans = vandalism::hour_spans({{100, 400}, {220, 200}});
    ASSERT_EQ(spans.size(), 2);
    EXPECT_EQ(spans[0].hour_span, 400);
    EXPECT_EQ(spans[1].hour_span, 200);  // minute 100 dropped (100 + 60 <= 220)
    EXPECT_EQ(spans[1].flagged, 0);
}

TEST(VandalismHourSpans, BoundaryWindowEdge) {
    // Consecutive minutes inside a 59-minute span keep everything; at a gap of
    // exactly 60 the oldest drops (window is [m-59, m] inclusive).
    const auto spans = vandalism::hour_spans({{10, 100}, {70, 250}});
    // 10 + 60 <= 70, so minute 10 is dropped even though 70 - 10 == 60.
    ASSERT_EQ(spans.size(), 2);
    EXPECT_EQ(spans[1].hour_span, 250);
}

}  // namespace