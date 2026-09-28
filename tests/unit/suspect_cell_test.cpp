#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "test_helpers.hpp"
#include "suspect.hpp"
#include "suspect_cell_store.hpp"

namespace {

using test_helpers::TempDir;

// ---------------------------------------------------------------------------
// MinuteCellStats: the pure per-(uid, minute, h3_cell) modified+deleted counter
// ---------------------------------------------------------------------------

TEST(SuspectMinuteCellStats, CountsNodeModifiesAndDeletesOnly) {
    suspect::MinuteCellStats stats;
    // Create (visible version 1) is ignored by filter 4.
    stats.add_node(10, "alice", 1000, 0x12345678, true, 1);
    // Modify (visible version > 1) counts.
    stats.add_node(10, "alice", 1000, 0x12345678, true, 3);
    stats.add_node(10, "alice", 1000, 0x12345678, true, 4);
    // Delete (invisible) counts regardless of version.
    stats.add_node(10, "alice", 1001, 0x12345678, false, 7);
    EXPECT_EQ(stats.size(), 2);
}

TEST(SuspectMinuteCellStats, BucketsByUtcMinuteAndCell) {
    suspect::MinuteCellStats stats;
    // Two edits in same minute, same cell share a key
    stats.add_node(11, "bob", 0, 0x11111111, true, 2);
    stats.add_node(11, "bob", 0, 0x11111111, true, 2);
    // Different minute, same cell
    stats.add_node(11, "bob", 1, 0x11111111, true, 2);
    // Different cell, same minute
    stats.add_node(11, "bob", 0, 0x22222222, true, 2);
    ASSERT_EQ(stats.size(), 3);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{11, 0, 0x11111111}).modified_deleted, 2);
    EXPECT_EQ(stats.cells().count(suspect::UserMinuteCellKey{11, 1, 0x11111111}), 1);
    EXPECT_EQ(stats.cells().count(suspect::UserMinuteCellKey{11, 0, 0x22222222}), 1);
}

TEST(SuspectMinuteCellStats, DifferentUsersAndMinutesAndCellsStaySeparate) {
    suspect::MinuteCellStats stats;
    stats.add_node(10, "alice", 100, 0xAAAAAAAA, true, 2);
    stats.add_node(10, "alice", 100, 0xBBBBBBBB, true, 2);
    stats.add_node(11, "bob", 100, 0xAAAAAAAA, true, 2);
    stats.add_node(11, "bob", 101, 0xAAAAAAAA, true, 2);
    ASSERT_EQ(stats.size(), 4);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{10, 100, 0xAAAAAAAA}).modified_deleted, 1);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{10, 100, 0xBBBBBBBB}).modified_deleted, 1);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{11, 100, 0xAAAAAAAA}).modified_deleted, 1);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{11, 101, 0xAAAAAAAA}).modified_deleted, 1);
}

TEST(SuspectMinuteCellStats, FirstUsernameSeenWins) {
    suspect::MinuteCellStats stats;
    stats.add_node(10, "alice", 100, 0x12345678, true, 2);
    stats.add_node(10, "alice_alias", 100, 0x12345678, true, 3);
    stats.add_node(11, "bob", 100, 0x12345678, true, 2);
    ASSERT_EQ(stats.size(), 2);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{10, 100, 0x12345678}).username, "alice");
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{11, 100, 0x12345678}).username, "bob");
}

TEST(SuspectMinuteCellStats, WayCountsOncePerDistinctCell) {
    suspect::MinuteCellStats stats;
    std::vector<uint64_t> cells = {0x11111111, 0x22222222, 0x11111111};  // duplicate cell
    stats.add_way(10, "alice", 100, cells, true, 2);
    // Way should count once per distinct cell
    ASSERT_EQ(stats.size(), 2);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{10, 100, 0x11111111}).modified_deleted, 1);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{10, 100, 0x22222222}).modified_deleted, 1);
}

TEST(SuspectMinuteCellStats, WayCreateIgnored) {
    suspect::MinuteCellStats stats;
    std::vector<uint64_t> cells = {0x11111111};
    stats.add_way(10, "alice", 100, cells, true, 1);  // create
    EXPECT_EQ(stats.size(), 0);
}

TEST(SuspectMinuteCellStats, WayDeleteCounts) {
    suspect::MinuteCellStats stats;
    std::vector<uint64_t> cells = {0x11111111};
    stats.add_way(10, "alice", 100, cells, false, 5);  // delete
    ASSERT_EQ(stats.size(), 1);
    EXPECT_EQ(stats.cells().at(suspect::UserMinuteCellKey{10, 100, 0x11111111}).modified_deleted, 1);
}

// ---------------------------------------------------------------------------
// SuspectCellStore: roundtrip tests
// ---------------------------------------------------------------------------

TEST(SuspectCellStore, RoundtripSorted) {
    TempDir dir;
    const std::string path = dir.join("cells.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(3, 100, 0x11111111, 5);
        w.add(3, 101, 0x22222222, 7);
        w.add(10, 1440, 0x33333333, 501);
        w.set_applied_seq(42);
        w.finish();
    }

    suspect_cell_store::Reader r(path, 9);
    EXPECT_EQ(r.size(), 3);
    EXPECT_EQ(r.applied_seq(), 42);
    EXPECT_EQ(r.uid_at(0), 3);
    EXPECT_EQ(r.minute_at(0), 100);
    EXPECT_EQ(r.cell_at(0), 0x11111111);
    EXPECT_EQ(r.count_at(0), 5);
    EXPECT_EQ(r.uid_at(1), 3);
    EXPECT_EQ(r.minute_at(1), 101);
    EXPECT_EQ(r.cell_at(1), 0x22222222);
    EXPECT_EQ(r.count_at(1), 7);
    EXPECT_EQ(r.uid_at(2), 10);
    EXPECT_EQ(r.minute_at(2), 1440);
    EXPECT_EQ(r.cell_at(2), 0x33333333);
    EXPECT_EQ(r.count_at(2), 501);
}

TEST(SuspectCellStore, LargeUidsAndNegativeOrdering) {
    TempDir dir;
    const std::string path = dir.join("cells.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(-1, 5, 0x11111111, 3);
        w.add(0, 5, 0x22222222, 4);
        w.add(1, 0, 0x33333333, 1);
        w.add(1, 5, 0x44444444, 2);
        w.add(INT64_MAX, 9, 0x55555555, 7);
        w.finish();
    }
    suspect_cell_store::Reader r(path, 9);
    ASSERT_EQ(r.size(), 5);
    EXPECT_EQ(r.uid_at(0), -1);
    EXPECT_EQ(r.uid_at(1), 0);
    EXPECT_EQ(r.uid_at(2), 1);
    EXPECT_EQ(r.uid_at(3), 1);
    EXPECT_EQ(r.uid_at(4), INT64_MAX);
    EXPECT_EQ(r.minute_at(4), 9);
    EXPECT_EQ(r.cell_at(4), 0x55555555);
    EXPECT_EQ(r.count_at(4), 7);
}

TEST(SuspectCellStore, MultiBlockRoundtrip) {
    TempDir dir;
    const std::string path = dir.join("big.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        constexpr uint32_t kBlock = 1 << 18;
        for (uint32_t i = 0; i < kBlock + 7; ++i) {
            w.add(1, i, 0x10000000 + i, i % 100);
        }
        w.finish();
    }
    suspect_cell_store::Reader r(path, 9);
    EXPECT_EQ(r.size(), (1u << 18) + 7);
    for (size_t i = 0; i < r.size(); ++i) {
        EXPECT_EQ(r.uid_at(i), 1);
        EXPECT_EQ(r.minute_at(i), static_cast<uint32_t>(i));
        EXPECT_EQ(r.cell_at(i), 0x10000000 + static_cast<uint64_t>(i));
        EXPECT_EQ(r.count_at(i), static_cast<uint32_t>(i % 100));
    }
}

TEST(SuspectCellStore, RejectsNonAscendingInput) {
    TempDir dir;
    const std::string path = dir.join("dup.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(1, 10, 0x11111111, 1);
        EXPECT_THROW(w.add(1, 10, 0x11111111, 2), std::runtime_error);  // duplicate key
    }
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(1, 10, 0x11111111, 1);
        EXPECT_THROW(w.add(0, 5, 0x11111111, 2), std::runtime_error);  // uid goes backwards
    }
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(1, 10, 0x22222222, 1);
        EXPECT_THROW(w.add(1, 10, 0x11111111, 2), std::runtime_error);  // cell goes backwards
    }
}

TEST(SuspectCellStore, FinishReplacesTheFileWhole) {
    TempDir dir;
    const std::string path = dir.join("r.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(7, 0, 0x11111111, 1);
        w.finish();
    }
    const uint64_t first_size = static_cast<uint64_t>(std::filesystem::file_size(path));
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(7, 0, 0x11111111, 1);
        w.add(9, 1, 0x22222222, 2);
        w.finish();
    }
    EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));
    EXPECT_NE(std::filesystem::file_size(path), 0);
    (void)first_size;
    suspect_cell_store::Reader r(path, 9);
    EXPECT_EQ(r.size(), 2);
}

TEST(SuspectCellStore, MissingFileAppliedSeqIsZero) {
    TempDir dir;
    EXPECT_EQ(suspect_cell_store::applied_seq_of(dir.join("nope.bin")), 0);
}

TEST(SuspectCellStore, WriterOrderingAcrossRecordsPerUid) {
    TempDir dir;
    const std::string path = dir.join("o.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(5, 100, 0x11111111, 1);
        EXPECT_THROW(w.add(5, 99, 0x11111111, 2), std::runtime_error);
    }
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(5, 100, 0x22222222, 1);
        EXPECT_THROW(w.add(5, 100, 0x11111111, 2), std::runtime_error);
    }
}

TEST(SuspectCellStore, ResolutionMismatchThrows) {
    TempDir dir;
    const std::string path = dir.join("cells.bin");
    {
        suspect_cell_store::Writer w(path, 9);
        w.add(1, 100, 0x11111111, 5);
        w.finish();
    }
    // Reading with different resolution should throw
    EXPECT_THROW(suspect_cell_store::Reader r(path, 10), std::runtime_error);
}

}  // namespace