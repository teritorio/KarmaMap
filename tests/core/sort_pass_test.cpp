#include <gtest/gtest.h>

#include <arrow/api.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

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

// Creates root/year=YYYY/month=MM.
void make_partitions(const std::string& root, const std::vector<std::string>& yyyy_mm) {
    for (const auto& month : yyyy_mm) {
        const auto dash = month.find('-');
        std::filesystem::create_directories(
            root + "/year=" + month.substr(0, dash) + "/month=" + month.substr(dash + 1));
    }
}

// Unpacks data.parquet (h3_cell, change_date, node_count, way_count).
struct MergedTable {
    std::vector<uint64_t> cells;
    std::vector<uint16_t> days;
    std::vector<uint32_t> nodes;
    std::vector<uint32_t> ways;

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
    const auto* nodes = static_cast<const arrow::UInt32Array*>(table->column(2)->chunk(0).get());
    const auto* ways = static_cast<const arrow::UInt32Array*>(table->column(3)->chunk(0).get());
    for (int64_t i = 0; i < table->num_rows(); ++i) {
        out.cells.push_back(cells->Value(i));
        out.days.push_back(days->Value(i));
        out.nodes.push_back(nodes->Value(i));
        out.ways.push_back(ways->Value(i));
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
    make_partitions(dir.path(), {"2024-06"});
    const std::string month = dir.path() + "/year=2024/month=06";

    write_staging(month + "/nodes.parquet", {{kLow, 1, 3}, {kMid, 2, 5}});
    write_staging(month + "/ways.parquet", {{kLow, 1, 7}, {kHigh, 3, 2}});

    sort_pass::merge_and_sort_partitions(dir.path());

    EXPECT_TRUE(std::filesystem::exists(month + "/data.parquet"));
    EXPECT_FALSE(std::filesystem::exists(month + "/nodes.parquet"));
    EXPECT_FALSE(std::filesystem::exists(month + "/ways.parquet"));

    const auto merged = read_merged(month + "/data.parquet");
    ASSERT_EQ(merged.size(), 3);

    const size_t ia = merged.index_of(kLow, 1);
    EXPECT_EQ(merged.nodes[ia], 3);
    EXPECT_EQ(merged.ways[ia], 7);

    const size_t ib = merged.index_of(kMid, 2);
    EXPECT_EQ(merged.nodes[ib], 5);
    EXPECT_EQ(merged.ways[ib], 0);

    const size_t ic = merged.index_of(kHigh, 3);
    EXPECT_EQ(merged.nodes[ic], 0);
    EXPECT_EQ(merged.ways[ic], 2);
}

TEST(SortPass, NodesOnlyZeroesWays) {
    TempDir dir;
    make_partitions(dir.path(), {"2024-01"});
    const std::string month = dir.path() + "/year=2024/month=01";
    write_staging(month + "/nodes.parquet", {{kLow, 1, 4}});

    sort_pass::merge_and_sort_partitions(dir.path());

    const auto merged = read_merged(month + "/data.parquet");
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged.nodes[0], 4);
    EXPECT_EQ(merged.ways[0], 0);
}

TEST(SortPass, WaysOnlyZeroesNodes) {
    TempDir dir;
    make_partitions(dir.path(), {"2024-01"});
    const std::string month = dir.path() + "/year=2024/month=01";
    write_staging(month + "/ways.parquet", {{kLow, 1, 6}});

    sort_pass::merge_and_sort_partitions(dir.path());

    const auto merged = read_merged(month + "/data.parquet");
    ASSERT_EQ(merged.size(), 1);
    EXPECT_EQ(merged.nodes[0], 0);
    EXPECT_EQ(merged.ways[0], 6);
}

TEST(SortPass, SortsByCellThenDay) {
    TempDir dir;
    make_partitions(dir.path(), {"2024-01"});
    const std::string month = dir.path() + "/year=2024/month=01";

    // Descending (cell, day) on input; output must be ascending.
    write_staging(month + "/nodes.parquet", {{kHigh, 2, 1}, {kMid, 3, 1},
                                             {kMid, 1, 1}, {kLow, 4, 1}});

    sort_pass::merge_and_sort_partitions(dir.path());

    const auto merged = read_merged(month + "/data.parquet");
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
    make_partitions(dir.path(), {"2024-01"});
    const std::string month = dir.path() + "/year=2024/month=01";

    write_staging(month + "/nodes.parquet", {{kLow, 1, 3}});
    write_staging(month + "/ways.parquet", {{kLow, 1, 7}});
    sort_pass::merge_and_sort_partitions(dir.path());

    // Add only a new ways staging file; the node count must be carried over
    // from the previous data.parquet rather than zeroed.
    write_staging(month + "/ways.parquet", {{kHigh, 2, 4}});
    sort_pass::merge_and_sort_partitions(dir.path());

    const auto merged = read_merged(month + "/data.parquet");
    ASSERT_EQ(merged.size(), 2);
    EXPECT_EQ(merged.nodes[merged.index_of(kLow, 1)], 3);
    EXPECT_EQ(merged.ways[merged.index_of(kLow, 1)], 0);
    EXPECT_EQ(merged.nodes[merged.index_of(kHigh, 2)], 0);
    EXPECT_EQ(merged.ways[merged.index_of(kHigh, 2)], 4);
}

TEST(SortPass, AlreadyMergedMonthUntouched) {
    TempDir dir;
    make_partitions(dir.path(), {"2024-01"});
    const std::string month = dir.path() + "/year=2024/month=01";

    write_staging(month + "/nodes.parquet", {{kLow, 1, 3}});
    sort_pass::merge_and_sort_partitions(dir.path());

    const std::string data_path = month + "/data.parquet";
    const auto before = read_parquet(data_path);
    const uintmax_t size_before = std::filesystem::file_size(data_path);

    sort_pass::merge_and_sort_partitions(dir.path());

    const auto after = read_parquet(data_path);
    EXPECT_EQ(after->num_rows(), before->num_rows());
    EXPECT_EQ(std::filesystem::file_size(data_path), size_before);
    EXPECT_FALSE(std::filesystem::exists(month + "/nodes.parquet"));
}

TEST(SortPass, MissingOrEmptyRootIsNoOp) {
    TempDir dir;
    EXPECT_NO_THROW(sort_pass::merge_and_sort_partitions(dir.join("nope")));
    EXPECT_NO_THROW(sort_pass::merge_and_sort_partitions(dir.path()));
}

TEST(SortPass, WrongChangeDateTypeThrows) {
    TempDir dir;
    make_partitions(dir.path(), {"2024-01"});
    const std::string month = dir.path() + "/year=2024/month=01";
    write_staging_int32_date(month + "/nodes.parquet", {{kLow, 1, 3}});

    EXPECT_THROW(sort_pass::merge_and_sort_partitions(dir.path()), std::runtime_error);
    EXPECT_FALSE(std::filesystem::exists(month + "/data.parquet"));
}

}  // namespace