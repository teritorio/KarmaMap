#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "options.hpp"
#include "sort_pass.hpp"
#include "test_helpers.hpp"

namespace {

using test_helpers::TempDir;
using test_helpers::read_parquet;
using test_helpers::write_staging;

// Distinct H3-plausible cells (mode bit set, low sentinel 0x3F) that sort
// unambiguously by value.
constexpr uint64_t kLow = 0x0F0000000000003FULL;
constexpr uint64_t kMid = 0x1F0000000000003FULL;
constexpr uint64_t kHigh = 0x2F0000000000003FULL;

// Creates root/year=YYYY [no month subdirs].
void make_partitions(const std::string& root, const std::vector<std::string>& years) {
    for (const auto& year : years) {
        std::filesystem::create_directories(root + "/year=" + year);
    }
}

// Number of row groups in a freshly written Parquet file.
int num_row_groups(const std::string& path) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    if (!infile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + ": " +
                                 infile_result.status().ToString());
    }
    auto reader_result =
        parquet::arrow::OpenFile(*infile_result, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        throw std::runtime_error("Failed to open Parquet reader for " + path + ": " +
                                 reader_result.status().ToString());
    }
    return (*reader_result)->num_row_groups();
}

// Unpacks data.parquet (h3_cell, change_date, count).
struct MergedTable {
    std::vector<uint64_t> cells;
    std::vector<uint16_t> days;
    std::vector<uint32_t> counts;

    size_t size() const { return cells.size(); }

    size_t index_of(uint64_t cell, uint16_t day) const {
        for (size_t i = 0; i < cells.size(); ++i) {
            if (cells[i] == cell && days[i] == day) return i;
        }
        throw std::runtime_error("Row (cell, day) not found in merged table");
    }
};

MergedTable read_merged(const std::string& path) {
    auto combined_result = read_parquet(path)->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + combined_result.status().ToString());
    }
    const auto& table = *combined_result;

    MergedTable out;
    const auto* cells = static_cast<const arrow::UInt64Array*>(table->column(0)->chunk(0).get());
    const auto* days = static_cast<const arrow::UInt16Array*>(table->column(1)->chunk(0).get());
    const auto* counts = static_cast<const arrow::UInt32Array*>(table->column(2)->chunk(0).get());
    for (int64_t i = 0; i < table->num_rows(); ++i) {
        out.cells.push_back(cells->Value(i));
        out.days.push_back(days->Value(i));
        out.counts.push_back(counts->Value(i));
    }
    return out;
}

// Staging file whose change_date column is int32 (not uint16), to exercise
// sort_pass's schema guard.
void write_staging_int32_date(
    const std::string& path,
    const std::vector<std::tuple<uint64_t, int32_t, uint32_t>>& rows) {
    arrow::UInt64Builder cell_builder;
    arrow::Int32Builder date_builder;
    arrow::UInt32Builder count_builder;
    for (const auto& [cell, day, count] : rows) {
        if (!cell_builder.Append(cell).ok() || !date_builder.Append(day).ok() ||
            !count_builder.Append(count).ok()) {
            throw std::runtime_error("Failed to append staging row");
        }
    }

    std::shared_ptr<arrow::Array> cells, dates, counts;
    if (!cell_builder.Finish(&cells).ok() || !date_builder.Finish(&dates).ok() ||
        !count_builder.Finish(&counts).ok()) {
        throw std::runtime_error("Failed to finalize staging columns");
    }

    auto schema = arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("change_date", arrow::int32(), false),
        arrow::field("count", arrow::uint32(), false),
    });
    auto table = arrow::Table::Make(schema, {cells, dates, counts});

    test_helpers::write_table(path, table);
}

TEST(SortPass, MergesNodesAndWays) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    write_staging(year + "/nodes.parquet", {{kLow, 1, 3}, {kMid, 2, 5}});
    write_staging(year + "/ways.parquet", {{kLow, 1, 7}, {kHigh, 3, 2}});

    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    EXPECT_TRUE(std::filesystem::exists(year + "/data.parquet"));
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.parquet"));
    EXPECT_FALSE(std::filesystem::exists(year + "/ways.parquet"));

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 3);

    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 10);
    EXPECT_EQ(merged.counts[merged.index_of(kMid, 2)], 5);
    EXPECT_EQ(merged.counts[merged.index_of(kHigh, 3)], 2);
}

TEST(SortPass, NodesOnly) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";
    write_staging(year + "/nodes.parquet", {{kLow, 1, 4}});

    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged.counts[0], 4);
}

TEST(SortPass, WaysOnly) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";
    write_staging(year + "/ways.parquet", {{kLow, 1, 6}});

    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged.counts[0], 6);
}

TEST(SortPass, SortsByCellThenDay) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    // Descending (cell, day) on input; output must be ascending.
    write_staging(year + "/nodes.parquet", {{kHigh, 2, 1}, {kMid, 3, 1},
                                            {kMid, 1, 1}, {kLow, 4, 1}});

    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 4);
    const uint64_t expected_cells[] = {kLow, kMid, kMid, kHigh};
    const uint16_t expected_days[] = {4, 1, 3, 2};
    for (size_t i = 0; i < merged.size(); ++i) {
        EXPECT_EQ(merged.cells[i], expected_cells[i]);
        EXPECT_EQ(merged.days[i], expected_days[i]);
    }
}

TEST(SortPass, ReMergeFallsBackToDataForMissingStaging) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    write_staging(year + "/nodes.parquet", {{kLow, 1, 3}});
    write_staging(year + "/ways.parquet", {{kLow, 1, 7}});
    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    // Add only a new ways staging file; the already-merged count must be
    // carried over from the previous data.parquet rather than zeroed.
    write_staging(year + "/ways.parquet", {{kHigh, 2, 4}});
    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 10);
    EXPECT_EQ(merged.counts[merged.index_of(kHigh, 2)], 4);
}

TEST(SortPass, AlreadyMergedYearUntouched) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    write_staging(year + "/nodes.parquet", {{kLow, 1, 3}});
    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    const std::string data_path = year + "/data.parquet";
    const auto before = read_parquet(data_path);
    const uintmax_t size_before = std::filesystem::file_size(data_path);

    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    const auto after = read_parquet(data_path);
    EXPECT_EQ(after->num_rows(), before->num_rows());
    EXPECT_EQ(std::filesystem::file_size(data_path), size_before);
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.parquet"));
}

TEST(SortPass, MissingOrEmptyRootIsNoOp) {
    TempDir dir;
    EXPECT_NO_THROW(sort_pass::merge_and_sort_partitions(dir.join("nope"), 3));
    EXPECT_NO_THROW(sort_pass::merge_and_sort_partitions(dir.path(), 3));
}

TEST(SortPass, LargeYearWritesMultipleRowGroups) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    // 7 rows with a row-group budget of 3 -> row groups of 3, 3, 1.
    write_staging(year + "/nodes.parquet",
                  {{kLow, 1, 1}, {kLow, 2, 1}, {kLow, 3, 1},
                   {kLow, 4, 1}, {kLow, 5, 1}, {kLow, 6, 1}, {kLow, 7, 1}});

    sort_pass::merge_and_sort_partitions(dir.path(), /*change_group_rows=*/3);

    const std::string data_path = year + "/data.parquet";
    ASSERT_TRUE(std::filesystem::exists(data_path));
    EXPECT_EQ(num_row_groups(data_path), 3);
    EXPECT_EQ(read_merged(data_path).size(), 7);
}

TEST(SortPass, WrongChangeDateTypeThrows) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";
    write_staging_int32_date(year + "/nodes.parquet", {{kLow, 1, 3}});

    EXPECT_THROW(sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows), std::runtime_error);
    EXPECT_FALSE(std::filesystem::exists(year + "/data.parquet"));
}

// ---------------------------------------------------------------------------
// merge_update_partitions (update mode staging -> data.parquet)
// ---------------------------------------------------------------------------

TEST(UpdateMerge, FoldsStagedDiffsOverExistingData) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    write_staging(year + "/nodes.parquet", {{kLow, 1, 10}});
    write_staging(year + "/ways.parquet", {{kLow, 1, 7}});
    sort_pass::merge_and_sort_partitions(dir.path(), kDefaultChangeGroupRows);

    // Update run: a nodes-only diff for sequence 2847600 lands on top of the
    // merged data. The pre-existing (kLow,1) count must carry over.
    write_staging(year + "/nodes.2847600.parquet", {{kLow, 1, 3}, {kMid, 2, 5}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 2847600);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 20);
    EXPECT_EQ(merged.counts[merged.index_of(kMid, 2)], 5);
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.2847600.parquet"));
    EXPECT_EQ(sort_pass::read_source_sequence(year + "/data.parquet"), 2847600);
}

TEST(UpdateMerge, MultipleSequencesAreCumulative) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    write_staging(year + "/nodes.2847600.parquet", {{kLow, 1, 4}});
    write_staging(year + "/ways.2847600.parquet", {{kLow, 1, 9}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 2847600);
    EXPECT_EQ(read_merged(year + "/data.parquet").counts[0], 13);

    write_staging(year + "/nodes.2847601.parquet", {{kLow, 1, 2}});
    write_staging(year + "/ways.2847601.parquet", {{kMid, 2, 6}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 2847601);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 15);
    EXPECT_EQ(merged.counts[merged.index_of(kMid, 2)], 6);
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.2847601.parquet"));
    EXPECT_FALSE(std::filesystem::exists(year + "/ways.2847601.parquet"));
    EXPECT_EQ(sort_pass::read_source_sequence(year + "/data.parquet"), 2847601);
}

TEST(UpdateMerge, AlreadyAppliedStampDropsOrphanedStaging) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    write_staging(year + "/nodes.2847600.parquet", {{kLow, 1, 4}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 2847600);

    const std::string data_path = year + "/data.parquet";
    const uintmax_t size_before = std::filesystem::file_size(data_path);

    // A crash left the staging behind after a completed run of the same seq.
    write_staging(year + "/nodes.2847600.parquet", {{kHigh, 3, 99}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 2847600);

    const auto merged = read_merged(data_path);
    ASSERT_EQ(merged.size(), 1);  // orphan staging must NOT be re-merged
    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 4);
    EXPECT_EQ(std::filesystem::file_size(data_path), size_before);
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.2847600.parquet"));
}

TEST(UpdateMerge, MissingOrEmptyRootIsNoOp) {
    TempDir dir;
    EXPECT_NO_THROW(sort_pass::merge_update_partitions(dir.join("nope"), 3, 1));
    EXPECT_NO_THROW(sort_pass::merge_update_partitions(dir.path(), 3, 1));
}

TEST(UpdateMerge, SkipsAndDropsStagingNewerThanApplied) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    // 101 was staged by a crashed run that fetched further than this run
    // applies; it must not be folded (nor survive) at applied_seq 100.
    write_staging(year + "/nodes.100.parquet", {{kLow, 1, 4}});
    write_staging(year + "/nodes.101.parquet", {{kMid, 2, 9}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 100);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 4);
    EXPECT_EQ(sort_pass::read_source_sequence(year + "/data.parquet"), 100);
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.100.parquet"));
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.101.parquet"));

    // The run that reaches 101 re-stages and folds it.
    write_staging(year + "/nodes.101.parquet", {{kMid, 2, 9}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 101);
    const auto after = read_merged(year + "/data.parquet");
    ASSERT_EQ(after.size(), 2);
    EXPECT_EQ(after.counts[after.index_of(kLow, 1)], 4);
    EXPECT_EQ(after.counts[after.index_of(kMid, 2)], 9);
    EXPECT_EQ(sort_pass::read_source_sequence(year + "/data.parquet"), 101);
}

TEST(UpdateMerge, FullRunStagingWithoutSequenceIsAlwaysFolded) {
    TempDir dir;
    make_partitions(dir.path(), {"2024"});
    const std::string year = dir.path() + "/year=2024";

    // nodes.parquet carries no sequence (full-run staging) and is folded even
    // though it sits next to a newer update staging file that is dropped.
    write_staging(year + "/nodes.parquet", {{kLow, 1, 3}});
    write_staging(year + "/ways.101.parquet", {{kLow, 1, 2}});
    sort_pass::merge_update_partitions(dir.path(), kDefaultChangeGroupRows, 100);

    const auto merged = read_merged(year + "/data.parquet");
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged.counts[merged.index_of(kLow, 1)], 3);
    EXPECT_FALSE(std::filesystem::exists(year + "/nodes.parquet"));
    EXPECT_FALSE(std::filesystem::exists(year + "/ways.101.parquet"));
}

}  // namespace
