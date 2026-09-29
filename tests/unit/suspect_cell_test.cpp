#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "test_helpers.hpp"
#include "h3_utils.hpp"
#include "suspect.hpp"
#include "suspect_cell_store.hpp"
#include "suspect_store.hpp"

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

// ---------------------------------------------------------------------------
// Filter 4: cell_spreads (pure sliding window over cell + object counts)
// ---------------------------------------------------------------------------

TEST(SuspectCellSpreads, SingleCellBurstNotFlagged) {
    // One cell with 20 edits, but only 1 distinct cell -> below kFilter4MinCells (3)
    std::vector<suspect::CellCountRow> rows = {{100, 0x1111, 20}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 20}};
    auto out = suspect::cell_spreads(rows, obj, 0.1);  // 0.1 km²/cell
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].distinct_cells, 1);
    EXPECT_EQ(out[0].flagged, 0);
}

TEST(SuspectCellSpreads, ThreeCellsUnderAreaNotFlagged) {
    // 3 distinct cells, 20 edits total, but area = 3 * 0.1 = 0.3 < 20
    std::vector<suspect::CellCountRow> rows = {{100, 0x1111, 10}, {100, 0x2222, 5}, {100, 0x3333, 5}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 20}};
    auto out = suspect::cell_spreads(rows, obj, 0.1);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].distinct_cells, 3);
    EXPECT_DOUBLE_EQ(out[0].spread_km2, 0.3);
    EXPECT_EQ(out[0].flagged, 0);
}

TEST(SuspectCellSpreads, ThreeCellsOverAreaFlagged) {
    // 3 distinct cells, 20 edits, area = 3 * 10 = 30 >= 20
    std::vector<suspect::CellCountRow> rows = {{100, 0x1111, 10}, {100, 0x2222, 5}, {100, 0x3333, 5}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 20}};
    auto out = suspect::cell_spreads(rows, obj, 10.0);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].distinct_cells, 3);
    EXPECT_DOUBLE_EQ(out[0].spread_km2, 30.0);
    EXPECT_EQ(out[0].flagged, suspect::kFlagFilter4);
}

TEST(SuspectCellSpreads, EvictionDropsOldMinutes) {
    // Minute 100 has 3 cells, minute 160 has 3 cells. At minute 160,
    // the window [101..160] drops minute 100, so distinct goes from 6 to 3.
    std::vector<suspect::CellCountRow> rows = {
        {100, 0x1111, 5}, {100, 0x2222, 5}, {100, 0x3333, 5},  // 15 edits at 100
        {160, 0x4444, 5}, {160, 0x5555, 5}, {160, 0x6666, 5}   // 15 edits at 160
    };
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 15}, {160, 15}};
    auto out = suspect::cell_spreads(rows, obj, 10.0);
    ASSERT_EQ(out.size(), 2);
    // At minute 100: distinct=3, area=30, count=15 (<20) -> not flagged
    EXPECT_EQ(out[0].flagged, 0);
    // At minute 160: window contains only 160's rows, distinct=3, area=30, count=15 (<20) -> not flagged
    EXPECT_EQ(out[1].flagged, 0);
    // Now add more at 160 to hit the gate
    rows = {{100, 0x1111, 10}, {100, 0x2222, 10},
            {160, 0x4444, 10}, {160, 0x5555, 10}, {160, 0x6666, 10}};
    obj = {{100, 20}, {160, 30}};
    out = suspect::cell_spreads(rows, obj, 10.0);
    // At minute 100: distinct=2 (<3) -> not flagged
    EXPECT_EQ(out[0].flagged, 0);
    // At minute 160: window drops 100, distinct=3, area=30, count=30 (>=20) -> flagged
    EXPECT_EQ(out[1].flagged, suspect::kFlagFilter4);
}

TEST(SuspectCellSpreads, GateOnObjectCountNotCellCount) {
    // Many cells but few real edits (object counts from minute store)
    // 10 distinct cells, 100 cell-store counts, but only 15 object edits
    std::vector<suspect::CellCountRow> rows;
    for (int i = 0; i < 10; ++i) rows.push_back({100, static_cast<uint64_t>(0x1000 + i), 10});
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 15}};  // only 15 real edits
    auto out = suspect::cell_spreads(rows, obj, 2.0);  // area = 10 * 2 = 20 >= 20
    ASSERT_EQ(out.size(), 1);
    // distinct=10 (>=3), area=20 (>=20), but object_total=15 (<20) -> not flagged
    EXPECT_EQ(out[0].object_count, 15);
    EXPECT_EQ(out[0].flagged, 0);
}

TEST(SuspectCellSpreads, ObjectRowsOlderThanWindowDoNotCount) {
    // The minute store holds edits that produced no cell row: deleted nodes with
    // no location, relations, and ways whose refs resolve to cell 0. Such a row
    // at minute 100 is outside the 60-minute window ending at minute 1000, so
    // the 40 edits it records must not satisfy the >= 20 gate there.
    std::vector<suspect::CellCountRow> rows = {{1000, 0x1111, 1}, {1000, 0x2222, 1}, {1000, 0x3333, 1}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 40}, {1000, 3}};
    auto out = suspect::cell_spreads(rows, obj, 10.0);
    ASSERT_EQ(out.size(), 2);  // minute 100 has no cell row but is still evaluated
    EXPECT_EQ(out[0].minute, 100u);
    // At minute 1000: distinct=3 (>=3), area=30 (>=20), but only 3 edits are in
    // the window, so the gate does not fire.
    EXPECT_EQ(out[1].object_count, 3);
    EXPECT_EQ(out[1].distinct_cells, 3);
    EXPECT_EQ(out[1].flagged, 0);
}

TEST(SuspectCellSpreads, ObjectRowInsideWindowCountsWithoutCellRow) {
    // Same shape, but the cell-less minute is within the window, so its edits
    // must be counted even though the cell store has no row for it.
    std::vector<suspect::CellCountRow> rows = {{1000, 0x1111, 1}, {1000, 0x2222, 1}, {1000, 0x3333, 1}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{990, 40}, {1000, 3}};
    auto out = suspect::cell_spreads(rows, obj, 10.0);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[1].object_count, 43);
    EXPECT_EQ(out[1].flagged, suspect::kFlagFilter4);
}

TEST(SuspectCellSpreads, GateCrossedOnCellLessMinuteStillFlags) {
    // The cells were touched early and the bulk of the edits arrived later on a
    // minute with no cell row (relations, deletes without a location). The
    // window only qualifies at that later minute, so the union of the two
    // minute grids has to be evaluated for the day to be flagged.
    std::vector<suspect::CellCountRow> rows = {{100, 0x1111, 1}, {100, 0x2222, 1}, {100, 0x3333, 1}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 3}, {130, 17}};
    auto out = suspect::cell_spreads(rows, obj, 10.0);
    ASSERT_EQ(out.size(), 2);
    // Minute 100: 3 cells but only 3 edits, gate not met.
    EXPECT_EQ(out[0].object_count, 3);
    EXPECT_EQ(out[0].flagged, 0);
    // Minute 130: the 3 cells are still in the window and the count reaches 20.
    EXPECT_EQ(out[1].object_count, 20);
    EXPECT_EQ(out[1].distinct_cells, 3);
    EXPECT_EQ(out[1].flagged, suspect::kFlagFilter4);
}

TEST(SuspectCellSpreads, SameCountsDifferentCellArea) {
    // Same data, different cell_area_km2 -> different flag outcome
    std::vector<suspect::CellCountRow> rows = {{100, 0x1111, 10}, {100, 0x2222, 10}, {100, 0x3333, 10}};
    std::vector<std::pair<uint32_t, uint32_t>> obj = {{100, 30}};
    auto out_low = suspect::cell_spreads(rows, obj, 5.0);   // area = 15 < 20
    auto out_high = suspect::cell_spreads(rows, obj, 10.0);  // area = 30 >= 20
    EXPECT_EQ(out_low[0].flagged, 0);
    EXPECT_EQ(out_high[0].flagged, suspect::kFlagFilter4);
}

// ---------------------------------------------------------------------------
// Filter 4: flagged_cell_days end-to-end through real stores
// ---------------------------------------------------------------------------

TEST(SuspectCellFlaggedDays, EndToEndGateUsesTrueObjectCount) {
    // Both uids reach 3 cells and clear the area budget at res 6. Only uid 10
    // clears the >= 20 edit gate, so only uid 10 is flagged.
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;  // area ~36 km², 3 cells = 108 km² >= 20

    {
        suspect_cell_store::Writer w(cells_path, res);
        w.add(10, 100, 0x11111111, 10);
        w.add(10, 100, 0x22222222, 10);
        w.add(10, 100, 0x33333333, 10);
        w.add(20, 100, 0x44444444, 10);
        w.add(20, 100, 0x55555555, 10);
        w.add(20, 100, 0x66666666, 10);
        w.finish();
    }
    {
        suspect_store::Writer w(minutes_path);
        w.add(10, 100, 30);  // 30 >= 20 gate
        w.add(20, 100, 15);  // 15 < 20 gate
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    ASSERT_EQ(days.size(), 1);
    EXPECT_EQ(days.at({10, 0}).flags, suspect::kFlagFilter4);
    EXPECT_EQ(days.count({20, 0}), 0u);
}

TEST(SuspectCellFlaggedDays, SkipsUidMissingFromOneStore) {
    // A uid needs rows in both stores: the cell store supplies the distinct cell
    // count, the minute store the true edit count. uid 10 and uid 40 are in both
    // and flag; uid 20 has cell rows but no minute rows, uid 30 the reverse, so
    // both are skipped. The walk must skip past those two and still reach uid 40.
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;

    {
        // Cell store holds uids 10, 20 and 40: uid 30 is deliberately absent.
        suspect_cell_store::Writer w(cells_path, res);
        w.add(10, 100, 0x11111111, 10);
        w.add(10, 100, 0x22222222, 10);
        w.add(10, 100, 0x33333333, 10);
        w.add(20, 100, 0x44444444, 10);
        w.add(20, 100, 0x55555555, 10);
        w.add(20, 100, 0x66666666, 10);
        w.add(40, 100, 0xAAAAAAA1, 10);
        w.add(40, 100, 0xAAAAAAA2, 10);
        w.add(40, 100, 0xAAAAAAA3, 10);
        w.finish();
    }
    {
        // Minute store holds uids 10, 30 and 40: uid 20 is deliberately absent.
        suspect_store::Writer w(minutes_path);
        w.add(10, 100, 30);
        w.add(30, 100, 30);
        w.add(40, 100, 30);
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    ASSERT_EQ(days.size(), 2);
    EXPECT_EQ(days.at({10, 0}).flags, suspect::kFlagFilter4);
    EXPECT_EQ(days.count({20, 0}), 0u);
    EXPECT_EQ(days.count({30, 0}), 0u);
    EXPECT_EQ(days.at({40, 0}).flags, suspect::kFlagFilter4);
}

TEST(SuspectCellFlaggedDays, EndToEndWithFlagAtRes6) {
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;  // area ~36 km², 3 cells = 108 km² >= 20

    {
        suspect_cell_store::Writer w(cells_path, res);
        // uid 10: minute 100 -> 3 distinct cells, 10 edits each
        w.add(10, 100, 0x11111111, 10);
        w.add(10, 100, 0x22222222, 10);
        w.add(10, 100, 0x33333333, 10);
        w.finish();
    }
    {
        suspect_store::Writer w(minutes_path);
        w.add(10, 100, 30);  // 30 >= 20 gate
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    // Should flag uid 10 at day 0 (minute 100 / 1440 = 0)
    ASSERT_EQ(days.size(), 1);
    auto it = days.find({10, 0});
    EXPECT_TRUE(it != days.end());
    EXPECT_EQ(it->second.flags, suspect::kFlagFilter4);
}

TEST(SuspectCellFlaggedDays, MaxSpreadIsTheDayPeak) {
    // Two qualifying windows on day 0, the second reaching more cells. The day
    // reports the larger spread, not the one from the last window.
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;
    const double cell_km2 = h3_utils::average_hexagon_area_km2(res);

    {
        suspect_cell_store::Writer w(cells_path, res);
        w.add(10, 100, 0x11111111, 10);
        w.add(10, 100, 0x22222222, 10);
        w.add(10, 100, 0x33333333, 10);
        w.add(10, 900, 0x44444444, 10);
        w.add(10, 900, 0x55555555, 10);
        w.add(10, 900, 0x66666666, 10);
        w.add(10, 900, 0x77777777, 10);
        w.add(10, 900, 0x88888888, 10);
        w.finish();
    }
    {
        suspect_store::Writer w(minutes_path);
        w.add(10, 100, 30);
        w.add(10, 900, 50);
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    ASSERT_EQ(days.count({10, 0}), 1u);
    // The windows are 800 minutes apart, so they do not overlap: the peak is
    // the 5-cell window's area.
    EXPECT_DOUBLE_EQ(days.at({10, 0}).spread_km2, 5 * cell_km2);
}

TEST(SuspectCellFlaggedDays, DayBoundary) {
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;

    {
        suspect_cell_store::Writer w(cells_path, res);
        // Minute 1439 (day 0) and 1440 (day 1), both with 3 cells, 30 edits
        w.add(10, 1439, 0x11111111, 10);
        w.add(10, 1439, 0x22222222, 10);
        w.add(10, 1439, 0x33333333, 10);
        w.add(10, 1440, 0x44444444, 10);
        w.add(10, 1440, 0x55555555, 10);
        w.add(10, 1440, 0x66666666, 10);
        w.finish();
    }
    {
        suspect_store::Writer w(minutes_path);
        w.add(10, 1439, 30);
        w.add(10, 1440, 30);
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    // Should flag both day 0 and day 1
    ASSERT_EQ(days.size(), 2);
    EXPECT_EQ(days.at({10, 0}).flags, suspect::kFlagFilter4);
    EXPECT_EQ(days.at({10, 1}).flags, suspect::kFlagFilter4);
}

TEST(SuspectCellFlaggedDays, UnflaggedDay) {
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;

    {
        suspect_cell_store::Writer w(cells_path, res);
        // Only 2 distinct cells at minute 100 (< kFilter4MinCells=3)
        w.add(10, 100, 0x11111111, 15);
        w.add(10, 100, 0x22222222, 15);
        w.finish();
    }
    {
        suspect_store::Writer w(minutes_path);
        w.add(10, 100, 30);
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    EXPECT_TRUE(days.empty());
}

TEST(SuspectCellFlaggedDays, CellLessMinuteInsideWindowCountsTowardGate) {
    // The cell store has rows only for minute 100. The minute store also records
    // minute 105, where a relations-only burst has no cells of its own. Those
    // edits are inside the window ending at minute 105, so together the 105
    // minute clears the gate that minute 100 alone does not.
    TempDir dir;
    const std::string cells_path = dir.join("cells.bin");
    const std::string minutes_path = dir.join("minutes.bin");
    const int res = 6;

    {
        suspect_cell_store::Writer w(cells_path, res);
        w.add(10, 100, 0x11111111, 3);
        w.add(10, 100, 0x22222222, 3);
        w.add(10, 100, 0x33333333, 3);
        w.finish();
    }
    {
        suspect_store::Writer w(minutes_path);
        w.add(10, 100, 9);   // below the 20-edit gate on its own
        w.add(10, 105, 11);  // no cells of its own, but inside the window
        w.set_applied_seq(1);
        w.finish();
    }
    auto days = suspect::flagged_cell_days(cells_path, minutes_path, res);
    ASSERT_EQ(days.size(), 1);
    EXPECT_EQ(days.at({10, 0}).flags, suspect::kFlagFilter4);
}

}  // namespace