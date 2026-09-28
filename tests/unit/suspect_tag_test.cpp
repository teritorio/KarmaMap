#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "test_helpers.hpp"
#include "suspect.hpp"
#include "suspect_tag_store.hpp"

namespace {

using test_helpers::TempDir;

// ---------------------------------------------------------------------------
// MinuteTagStats: the pure per-(uid, minute, tag_key) counter
// ---------------------------------------------------------------------------

TEST(SuspectMinuteTagStats, CountsAllChangeTypes) {
    suspect::MinuteTagStats stats;
    // Create (visible version 1) is skipped: the minute store counts only
    // modified+deleted, so the numerator must use the same scope.
    stats.add_object(10, "alice", 1000, {"highway", "name"}, true, 1);
    // Modify (visible version > 1) counts
    stats.add_object(10, "alice", 1000, {"highway"}, true, 3);
    // Delete (invisible) counts regardless of version
    stats.add_object(10, "alice", 1001, {"building"}, false, 7);
    EXPECT_EQ(stats.size(), 2);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 1000, "highway"}).count, 1);
    EXPECT_EQ(stats.tags().count(suspect::UserMinuteTagKey{10, 1000, "name"}), 0);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 1001, "building"}).count, 1);
}

TEST(SuspectMinuteTagStats, BucketsByUtcMinuteAndTagKey) {
    suspect::MinuteTagStats stats;
    // Two edits in same minute, same tag share a key
    stats.add_object(11, "bob", 0, {"amenity"}, true, 2);
    stats.add_object(11, "bob", 0, {"amenity"}, true, 2);
    // Different minute, same tag
    stats.add_object(11, "bob", 1, {"amenity"}, true, 2);
    // Different tag, same minute
    stats.add_object(11, "bob", 0, {"shop"}, true, 2);
    ASSERT_EQ(stats.size(), 3);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{11, 0, "amenity"}).count, 2);
    EXPECT_EQ(stats.tags().count(suspect::UserMinuteTagKey{11, 1, "amenity"}), 1);
    EXPECT_EQ(stats.tags().count(suspect::UserMinuteTagKey{11, 0, "shop"}), 1);
}

TEST(SuspectMinuteTagStats, DifferentUsersAndMinutesAndTagsStaySeparate) {
    suspect::MinuteTagStats stats;
    stats.add_object(10, "alice", 100, {"highway"}, true, 2);
    stats.add_object(10, "alice", 100, {"building"}, true, 2);
    stats.add_object(11, "bob", 100, {"highway"}, true, 2);
    stats.add_object(11, "bob", 101, {"highway"}, true, 2);
    ASSERT_EQ(stats.size(), 4);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "highway"}).count, 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "building"}).count, 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{11, 100, "highway"}).count, 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{11, 101, "highway"}).count, 1);
}

TEST(SuspectMinuteTagStats, FirstUsernameSeenWins) {
    suspect::MinuteTagStats stats;
    stats.add_object(10, "alice", 100, {"highway"}, true, 2);
    stats.add_object(10, "alice_alias", 100, {"highway"}, true, 3);
    stats.add_object(11, "bob", 100, {"highway"}, true, 2);
    ASSERT_EQ(stats.size(), 2);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "highway"}).username, "alice");
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{11, 100, "highway"}).username, "bob");
}

TEST(SuspectMinuteTagStats, DeduplicatesTagKeysPerObject) {
    suspect::MinuteTagStats stats;
    // Same tag key multiple times in one object (should not happen in OSM, but be safe)
    stats.add_object(10, "alice", 100, {"highway", "highway", "name"}, true, 2);
    ASSERT_EQ(stats.size(), 2);  // highway and name
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "highway"}).count, 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "name"}).count, 1);
}

TEST(SuspectMinuteTagStats, EmptyTagListIgnored) {
    suspect::MinuteTagStats stats;
    stats.add_object(10, "alice", 100, {}, true, 2);
    EXPECT_EQ(stats.size(), 0);
}

TEST(SuspectMinuteTagStats, HandlesSpecialCharactersInTagKeys) {
    suspect::MinuteTagStats stats;
    stats.add_object(10, "alice", 100, {"addr:street", "name:en", "fixme"}, true, 2);
    ASSERT_EQ(stats.size(), 3);
    EXPECT_EQ(stats.tags().count(suspect::UserMinuteTagKey{10, 100, "addr:street"}), 1);
    EXPECT_EQ(stats.tags().count(suspect::UserMinuteTagKey{10, 100, "name:en"}), 1);
    EXPECT_EQ(stats.tags().count(suspect::UserMinuteTagKey{10, 100, "fixme"}), 1);
}

TEST(SuspectMinuteTagStats, SkipsCreates) {
    suspect::MinuteTagStats stats;
    stats.add_object(10, "alice", 100, {"highway"}, true, 1);
    EXPECT_EQ(stats.size(), 0);
    stats.add_object(10, "alice", 100, {"highway"}, true, 2);
    EXPECT_EQ(stats.size(), 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "highway"}).count, 1);
    stats.add_object(10, "alice", 101, {"building"}, false, 5);
    EXPECT_EQ(stats.size(), 2);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 101, "building"}).count, 1);
}

TEST(SuspectMinuteTagStats, CountsOnlyModifiesAndDeletes) {
    suspect::MinuteTagStats stats;
    stats.add_object(10, "alice", 100, {"highway"}, true, 1);
    stats.add_object(10, "alice", 100, {"highway"}, true, 2);
    stats.add_object(10, "alice", 100, {"name"}, true, 1);
    stats.add_object(10, "alice", 100, {"name"}, true, 3);
    stats.add_object(10, "alice", 100, {"building"}, false, 1);
    ASSERT_EQ(stats.size(), 3);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "highway"}).count, 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "name"}).count, 1);
    EXPECT_EQ(stats.tags().at(suspect::UserMinuteTagKey{10, 100, "building"}).count, 1);
}

// ---------------------------------------------------------------------------
// TagCoverageAccumulator: the sliding-window rule for Filter 5
// ---------------------------------------------------------------------------

TEST(TagCoverageAccumulator, BasicCoverage) {
    suspect::TagCoverageAccumulator acc;
    acc.add_object(100, 100);
    acc.add_tag(100, "highway", 95);
    acc.evict(100);
    auto row = acc.evaluate(100);
    EXPECT_EQ(row.object_total, 100);
    EXPECT_EQ(row.top_key_count, 95);
    EXPECT_DOUBLE_EQ(row.coverage, 0.95);
    EXPECT_EQ(row.flagged, suspect::kFlagFilter5);
}

TEST(TagCoverageAccumulator, BelowCoverageThreshold) {
    suspect::TagCoverageAccumulator acc;
    acc.add_object(100, 100);
    acc.add_tag(100, "highway", 89);
    acc.evict(100);
    auto row = acc.evaluate(100);
    EXPECT_EQ(row.top_key_count, 89);
    EXPECT_DOUBLE_EQ(row.coverage, 0.89);
    EXPECT_EQ(row.flagged, 0);
}

TEST(TagCoverageAccumulator, BelowObjectGate) {
    suspect::TagCoverageAccumulator acc;
    acc.add_object(100, 99);
    acc.add_tag(100, "highway", 99);
    acc.evict(100);
    auto row = acc.evaluate(100);
    EXPECT_EQ(row.top_key_count, 99);
    EXPECT_DOUBLE_EQ(row.coverage, 1.0);
    EXPECT_EQ(row.flagged, 0);
}

TEST(TagCoverageAccumulator, Exactly90PercentNoFlag) {
    suspect::TagCoverageAccumulator acc;
    acc.add_object(100, 100);
    acc.add_tag(100, "highway", 90);  // 90% exactly, not > 90%
    acc.evict(100);
    auto row = acc.evaluate(100);
    EXPECT_EQ(row.top_key_count, 90);
    EXPECT_DOUBLE_EQ(row.coverage, 0.90);
    EXPECT_EQ(row.flagged, 0);  // strictly greater than threshold
}

TEST(TagCoverageAccumulator, EvictionResetsWindow) {
    suspect::TagCoverageAccumulator acc;
    acc.add_object(100, 100);
    acc.add_tag(100, "highway", 95);
    acc.evict(100);
    // Now advance beyond the window (100 + 60 = 160)
    acc.add_object(160, 0);  // no new objects, just evict
    acc.evict(160);
    auto row = acc.evaluate(160);
    EXPECT_EQ(row.object_total, 0);
    EXPECT_EQ(row.top_key_count, 0);
    EXPECT_EQ(row.flagged, 0);
}

TEST(TagCoverageAccumulator, MultipleTagKeysTopWins) {
    suspect::TagCoverageAccumulator acc;
    acc.add_object(100, 100);
    acc.add_tag(100, "highway", 95);
    acc.add_tag(100, "name", 10);
    acc.add_tag(100, "building", 5);
    acc.evict(100);
    auto row = acc.evaluate(100);
    EXPECT_EQ(row.top_key_count, 95);
    EXPECT_DOUBLE_EQ(row.coverage, 0.95);
    EXPECT_EQ(row.flagged, suspect::kFlagFilter5);
}

TEST(TagCoverageAccumulator, TagRowsAtDifferentMinutes) {
    suspect::TagCoverageAccumulator acc;
    // Minute 100: 50 objects, 45 highway (90%)
    acc.add_object(100, 50);
    acc.add_tag(100, "highway", 45);
    acc.evict(100);
    // Minute 110: 60 objects, 55 highway (91.6%) -> window total 110, highway 100 (90.9%)
    acc.add_object(110, 60);
    acc.add_tag(110, "highway", 55);
    acc.evict(110);
    auto row = acc.evaluate(110);
    EXPECT_EQ(row.object_total, 110);
    EXPECT_EQ(row.top_key_count, 100);
    EXPECT_NEAR(row.coverage, 100.0 / 110.0, 0.001);
    EXPECT_EQ(row.flagged, suspect::kFlagFilter5);  // 100/110 = 90.9% > 90%, total 110 >= 100
}

// ---------------------------------------------------------------------------
// tag_coverages helper
// ---------------------------------------------------------------------------

TEST(TagCoveragesHelper, UnionOfMinutes) {
    using T = std::tuple<uint32_t, std::string, uint32_t>;
    std::vector<T> tag_rows = {
        {100, "highway", 95},
    };
    std::vector<std::pair<uint32_t, uint32_t>> obj_rows = {
        {100, 100},
    };
    auto rows = suspect::tag_coverages(tag_rows, obj_rows);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].minute, 100);
    EXPECT_EQ(rows[0].flagged, suspect::kFlagFilter5);
}

TEST(TagCoveragesHelper, ObjectMinuteWithoutTagRow) {
    using T = std::tuple<uint32_t, std::string, uint32_t>;
    std::vector<T> tag_rows = {};
    std::vector<std::pair<uint32_t, uint32_t>> obj_rows = {
        {100, 100},
    };
    auto rows = suspect::tag_coverages(tag_rows, obj_rows);
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].minute, 100);
    EXPECT_EQ(rows[0].object_total, 100);
    EXPECT_EQ(rows[0].top_key_count, 0);
    EXPECT_EQ(rows[0].flagged, 0);
}

// ---------------------------------------------------------------------------
// SuspectTagStore: roundtrip tests
// ---------------------------------------------------------------------------

TEST(SuspectTagStore, RoundtripSorted) {
    TempDir dir;
    const std::string path = dir.join("tags.bin");
    {
        suspect_tag_store::Writer w(path);
        w.add(3, 100, "highway", 5);
        w.add(3, 101, "building", 7);
        w.add(10, 1440, "amenity", 501);
        w.set_applied_seq(42);
        w.finish();
    }

    suspect_tag_store::Reader r(path);
    EXPECT_EQ(r.size(), 3);
    EXPECT_EQ(r.applied_seq(), 42);
    EXPECT_EQ(r.uid_at(0), 3);
    EXPECT_EQ(r.minute_at(0), 100);
    EXPECT_EQ(r.tag_key_at(0), "highway");
    EXPECT_EQ(r.count_at(0), 5);
    EXPECT_EQ(r.uid_at(1), 3);
    EXPECT_EQ(r.minute_at(1), 101);
    EXPECT_EQ(r.tag_key_at(1), "building");
    EXPECT_EQ(r.count_at(1), 7);
    EXPECT_EQ(r.uid_at(2), 10);
    EXPECT_EQ(r.minute_at(2), 1440);
    EXPECT_EQ(r.tag_key_at(2), "amenity");
    EXPECT_EQ(r.count_at(2), 501);
}

TEST(SuspectTagStore, LargeUidsAndNegativeOrdering) {
    TempDir dir;
    const std::string path = dir.join("tags.bin");
    {
        suspect_tag_store::Writer w(path);
        w.add(-1, 5, "highway", 3);
        w.add(0, 5, "building", 4);
        w.add(1, 0, "amenity", 1);
        w.add(1, 5, "shop", 2);
        w.add(INT64_MAX, 9, "name", 7);
        w.finish();
    }
    suspect_tag_store::Reader r(path);
    ASSERT_EQ(r.size(), 5);
    EXPECT_EQ(r.uid_at(0), -1);
    EXPECT_EQ(r.uid_at(1), 0);
    EXPECT_EQ(r.uid_at(2), 1);
    EXPECT_EQ(r.uid_at(3), 1);
    EXPECT_EQ(r.uid_at(4), INT64_MAX);
    EXPECT_EQ(r.minute_at(4), 9);
    EXPECT_EQ(r.tag_key_at(4), "name");
    EXPECT_EQ(r.count_at(4), 7);
}

TEST(SuspectTagStore, MultiBlockRoundtrip) {
    TempDir dir;
    const std::string path = dir.join("big.bin");
    {
        suspect_tag_store::Writer w(path);
        constexpr uint32_t kBlock = 1 << 18;
        for (uint32_t i = 0; i < kBlock + 7; ++i) {
            w.add(1, i, "tag" + std::to_string(i), i % 100);
        }
        w.finish();
    }
    suspect_tag_store::Reader r(path);
    EXPECT_EQ(r.size(), (1u << 18) + 7);
    for (size_t i = 0; i < r.size(); ++i) {
        EXPECT_EQ(r.uid_at(i), 1);
        EXPECT_EQ(r.minute_at(i), static_cast<uint32_t>(i));
        EXPECT_EQ(r.tag_key_at(i), "tag" + std::to_string(i));
        EXPECT_EQ(r.count_at(i), static_cast<uint32_t>(i % 100));
    }
}

TEST(SuspectTagStore, RejectsNonAscendingInput) {
    TempDir dir;
    const std::string path = dir.join("dup.bin");
    {
        suspect_tag_store::Writer w(path);
        w.add(1, 10, "highway", 1);
        EXPECT_THROW(w.add(1, 10, "highway", 2), std::runtime_error);  // duplicate key
    }
    {
        suspect_tag_store::Writer w(path);
        w.add(1, 10, "highway", 1);
        EXPECT_THROW(w.add(0, 5, "highway", 2), std::runtime_error);  // uid goes backwards
    }
    {
        suspect_tag_store::Writer w(path);
        w.add(1, 10, "building", 1);
        EXPECT_THROW(w.add(1, 10, "amenity", 2), std::runtime_error);  // tag_key goes backwards (lexicographic)
    }
    {
        suspect_tag_store::Writer w(path);
        w.add(1, 10, "highway", 1);
        EXPECT_THROW(w.add(1, 9, "highway", 2), std::runtime_error);  // minute goes backwards
    }
}

TEST(SuspectTagStore, FinishReplacesTheFileWhole) {
    TempDir dir;
    const std::string path = dir.join("r.bin");
    {
        suspect_tag_store::Writer w(path);
        w.add(7, 0, "highway", 1);
        w.finish();
    }
    const uint64_t first_size = static_cast<uint64_t>(std::filesystem::file_size(path));
    {
        suspect_tag_store::Writer w(path);
        w.add(7, 0, "highway", 1);
        w.add(9, 1, "building", 2);
        w.finish();
    }
    EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));
    EXPECT_NE(std::filesystem::file_size(path), 0);
    (void)first_size;
    suspect_tag_store::Reader r(path);
    EXPECT_EQ(r.size(), 2);
}

TEST(SuspectTagStore, MissingFileAppliedSeqIsZero) {
    TempDir dir;
    EXPECT_EQ(suspect_tag_store::applied_seq_of(dir.join("nope.bin")), 0);
}

TEST(SuspectTagStore, WriterOrderingAcrossRecordsPerUid) {
    TempDir dir;
    const std::string path = dir.join("o.bin");
    {
        suspect_tag_store::Writer w(path);
        w.add(5, 100, "highway", 1);
        EXPECT_THROW(w.add(5, 99, "highway", 2), std::runtime_error);
    }
    {
        suspect_tag_store::Writer w(path);
        w.add(5, 100, "building", 1);
        EXPECT_THROW(w.add(5, 100, "amenity", 2), std::runtime_error);
    }
}

TEST(SuspectTagStore, LongTagKeyRejected) {
    TempDir dir;
    const std::string path = dir.join("long.bin");
    {
        suspect_tag_store::Writer w(path);
        std::string long_key(256, 'a');  // > 255 chars
        EXPECT_THROW(w.add(1, 100, long_key, 1), std::runtime_error);
    }
}

// Pins the on-disk header so the format cannot drift from the one documented
// in API.md without a test failing: a changed byte here means every store
// written so far becomes unreadable.
TEST(SuspectTagStore, HeaderLayoutIsStable) {
    TempDir dir;
    const std::string path = dir.join("h.bin");
    {
        suspect_tag_store::Writer w(path);
        w.add(7, 100, "highway", 3);
        w.set_applied_seq(42);
        w.finish();
    }
    std::array<uint8_t, suspect_tag_store::kHeaderSize> header{};
    {
        std::ifstream in(path, std::ios::binary);
        in.read(reinterpret_cast<char*>(header.data()),
                static_cast<std::streamsize>(header.size()));
        ASSERT_EQ(in.gcount(), static_cast<std::streamsize>(header.size()));
    }
    EXPECT_EQ(suspect_tag_store::be32(header.data()), suspect_tag_store::kMagic);
    EXPECT_EQ(suspect_tag_store::be32(header.data() + 4), 1u);   // format version
    EXPECT_EQ(suspect_tag_store::be32(header.data() + 8), 0u);   // 0 = variable-length
    EXPECT_EQ(suspect_tag_store::be32(header.data() + 12), 0u);  // H3 resolution (N/A)
    EXPECT_EQ(suspect_tag_store::be64(header.data() + 16), 1u);  // record count
    EXPECT_EQ(suspect_tag_store::be64(header.data() + 24), 1u);  // block count
    EXPECT_EQ(suspect_tag_store::be64(header.data() + 32), 42u); // applied sequence
}

}  // namespace