#include <gtest/gtest.h>

#include <arrow/api.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "date_utils.hpp"
#include "manifest.hpp"
#include "test_helpers.hpp"

namespace {

using test_helpers::TempDir;

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void make_partitions(const std::string& changes_root,
                     const std::vector<std::string>& years) {
    for (const auto& year : years) {
        std::filesystem::create_directories(changes_root + "/year=" + year);
    }
}

// Writes a real data.parquet (same schema as the sort pass output) so the
// manifest's date_range can be read from its footer statistics.
void write_data_parquet(const std::string& path,
                        const std::vector<std::pair<uint16_t, uint32_t>>& rows) {
    arrow::UInt64Builder cell_builder;
    arrow::UInt16Builder date_builder;
    arrow::UInt32Builder node_builder;
    arrow::UInt32Builder way_builder;
    for (const auto& [day, count] : rows) {
        cell_builder.Append(0);
        date_builder.Append(day);
        node_builder.Append(count);
        way_builder.Append(0);
    }

    std::shared_ptr<arrow::Array> cells, dates, nodes, ways;
    ASSERT_TRUE(cell_builder.Finish(&cells).ok());
    ASSERT_TRUE(date_builder.Finish(&dates).ok());
    ASSERT_TRUE(node_builder.Finish(&nodes).ok());
    ASSERT_TRUE(way_builder.Finish(&ways).ok());

    auto schema = arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("node_count", arrow::uint32(), false),
        arrow::field("way_count", arrow::uint32(), false),
    });
    arrow_table_io::write_table(path,
                                arrow::Table::Make(schema, {cells, dates, nodes, ways}));
}

TEST(Manifest, WritesEmptyManifest) {
    TempDir dir;
    manifest::write_manifest(dir.path(), 9);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"h3_resolution\": 9"), std::string::npos);
    EXPECT_EQ(json.find("\"date_range\""), std::string::npos);
    EXPECT_NE(json.find("\"partitions\": []"), std::string::npos);
    EXPECT_NE(json.find("\"path\": \"changes\""), std::string::npos);
}

TEST(Manifest, SortsYearPartitions) {
    TempDir dir;
    // Build in scrambled order; output must be sorted.
    make_partitions(dir.join("changes"), {"2025", "2023", "2024"});
    manifest::write_manifest(dir.path(), 12);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"h3_resolution\": 12"), std::string::npos);
    EXPECT_EQ(json.find("\"date_range\""), std::string::npos);
    EXPECT_NE(json.find("\"partitions\": [\"2023\", \"2024\", \"2025\"]"),
              std::string::npos);
}

TEST(Manifest, WritesDateRangeFromDataFiles) {
    TempDir dir;
    // 2026 has only a directory (no data file yet), so it must not widen the
    // bounds; the range is the merged extent of the files that exist.
    make_partitions(dir.join("changes"), {"2024", "2025", "2026"});
    write_data_parquet(dir.join("changes/year=2024/data.parquet"),
                       {{19723, 1}, {19753, 5}});  // 2024-01-01 .. 2024-01-31
    write_data_parquet(dir.join("changes/year=2025/data.parquet"),
                       {{20089, 2}, {20319, 4}});  // 2025-01-01 .. 2025-08-19

    manifest::write_manifest(dir.path(), 9);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"date_range\": { \"min_date\": \""
                            + date_utils::iso_date(19723)
                            + "\", \"max_date\": \""
                            + date_utils::iso_date(20319) + "\" }"),
              std::string::npos);
    EXPECT_NE(json.find("\"partitions\": [\"2024\", \"2025\", \"2026\"]"),
              std::string::npos);

    // partition_footer_sizes contains the years with readable data.parquet
    // and omits 2026 (directory without a data file).
    const std::string pfs = json.substr(json.find("\"partition_footer_sizes\":"));
    EXPECT_NE(pfs.find("\"2024\": "), std::string::npos);
    EXPECT_NE(pfs.find("\"2025\": "), std::string::npos);
    EXPECT_EQ(pfs.find("\"2026\""), std::string::npos);
}

TEST(Manifest, IgnoresForeignEntries) {
    TempDir dir;
    make_partitions(dir.join("changes"), {"2024"});
    std::filesystem::create_directories(dir.join("changes/some_other_dir"));
    std::filesystem::create_directories(dir.join("changes/year=2024/plain_month"));
    {
        std::ofstream(dir.join("changes/note.txt")) << "not a partition\n";
    }
    manifest::write_manifest(dir.path(), 4);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"partitions\": [\"2024\"]"), std::string::npos);
}

TEST(Manifest, WritesUserDatasetEntries) {
    TempDir dir;
    make_partitions(dir.join("changes"), {"2024"});
    {
        std::ofstream(dir.join("user_indicators.parquet")) << "x\n";
        std::ofstream(dir.join("user_reputation.parquet")) << "z\n";
    }
    manifest::write_manifest(dir.path(), 4);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"user_indicators\": { \"path\": \"user_indicators.parquet\", \"partitions\": [] }"),
              std::string::npos);
    EXPECT_NE(json.find("\"user_reputation\": { \"path\": \"user_reputation.parquet\", \"partitions\": [] }"),
              std::string::npos);
    // The fixture files are not parquet, so no footer_size is emitted.
    EXPECT_EQ(json.find("footer_size"), std::string::npos);
}

TEST(Manifest, WritesFooterSizeForUserDatasets) {
    TempDir dir;
    make_partitions(dir.join("changes"), {"2024"});

    // A real (small) parquet file so the manifest can read its footer size.
    arrow::UInt32Builder builder;
    ASSERT_TRUE(builder.Append(1).ok());
    std::shared_ptr<arrow::Array> values;
    ASSERT_TRUE(builder.Finish(&values).ok());
    auto schema = arrow::schema({arrow::field("v", arrow::uint32(), false)});
    arrow_table_io::write_table(dir.join("user_indicators.parquet"),
                                arrow::Table::Make(schema, {values}));

    manifest::write_manifest(dir.path(), 4);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"user_indicators\": { \"path\": \"user_indicators.parquet\", \"partitions\": [], \"footer_size\": "),
              std::string::npos);
}

TEST(Manifest, SkipsUserDatasetsWhenAbsent) {
    TempDir dir;
    make_partitions(dir.join("changes"), {"2024"});
    manifest::write_manifest(dir.path(), 4);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_EQ(json.find("\"user_indicators\""), std::string::npos);
    EXPECT_EQ(json.find("\"user_reputation\""), std::string::npos);
}

TEST(Manifest, MissingOutputDirThrows) {
    TempDir dir;
    EXPECT_THROW(manifest::write_manifest(dir.join("nonexistent"), 9),
                 std::runtime_error);
}

}  // namespace