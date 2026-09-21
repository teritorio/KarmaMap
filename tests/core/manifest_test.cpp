#include <gtest/gtest.h>

#include <arrow/api.h>
#include <parquet/file_reader.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
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
    arrow::UInt32Builder count_builder;
    for (const auto& [day, count] : rows) {
        ASSERT_TRUE(cell_builder.Append(0).ok());
        ASSERT_TRUE(date_builder.Append(day).ok());
        ASSERT_TRUE(count_builder.Append(count).ok());
    }

    std::shared_ptr<arrow::Array> cells, dates, counts;
    ASSERT_TRUE(cell_builder.Finish(&cells).ok());
    ASSERT_TRUE(date_builder.Finish(&dates).ok());
    ASSERT_TRUE(count_builder.Finish(&counts).ok());

    auto schema = arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("count", arrow::uint32(), false),
    });
    arrow_table_io::write_table(path,
                                arrow::Table::Make(schema, {cells, dates, counts}));
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

TEST(Manifest, WritesSourceProvenance) {
    TempDir dir;
    replication_state::State source;
    source.url = "https://x/canary-updates/";
    source.sequence_number = 2847632;
    source.timestamp = "2019-12-30T09\\:36\\:32Z";

    manifest::write_manifest(dir.path(), 9, source);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"source\": {\n"
                        "    \"url\": \"https://x/canary-updates/\",\n"
                        "    \"sequence_number\": 2847632,\n"
                        "    \"timestamp\": \"2019-12-30T09\\\\:36\\\\:32Z\"\n"
                        "  }"),
              std::string::npos);
}

TEST(Manifest, OmitsSourceWhenAbsent) {
    TempDir dir;
    manifest::write_manifest(dir.path(), 9);
    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_EQ(json.find("\"source\""), std::string::npos);
}

TEST(Manifest, EscapesSourceJsonQuotes) {
    TempDir dir;
    replication_state::State source;
    source.url = "https://x/update\"stream/";
    source.sequence_number = 7;
    source.timestamp = "a\"b";

    manifest::write_manifest(dir.path(), 9, source);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\\\"stream"), std::string::npos);
    EXPECT_NE(json.find("a\\\"b"), std::string::npos);
}

TEST(Manifest, MissingOutputDirThrows) {
    TempDir dir;
    EXPECT_THROW(manifest::write_manifest(dir.join("nonexistent"), 9),
                 std::runtime_error);
}

// Reads data.parquet's footer and asserts per-column row-group statistics
// presence. Real parquet I/O, so it also proves dropping statistics keeps the
// data readable.
void check_column_statistics(const std::string& path,
                             const std::vector<bool>& expect_stats) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    ASSERT_TRUE(infile_result.ok());
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::Open(infile_result.ValueOrDie());
    const std::shared_ptr<parquet::FileMetaData> meta = reader->metadata();
    ASSERT_GT(meta->num_row_groups(), 0);
    for (int c = 0; c < static_cast<int>(expect_stats.size()); ++c) {
        const std::string name = meta->schema()->Column(c)->name();
        const bool has_stats = meta->RowGroup(0)->ColumnChunk(c)->statistics() != nullptr;
        EXPECT_EQ(has_stats, expect_stats[c]) << "column " << name;
    }
}

TEST(ArrowTableIo, WritesFooterStatisticsOnlyForPrunedColumns) {
    TempDir dir;

    arrow::UInt64Builder cell_builder;
    arrow::UInt16Builder date_builder;
    arrow::UInt32Builder count_builder;
    for (uint16_t day = 19700; day < 19705; ++day) {
        ASSERT_TRUE(cell_builder.Append(day).ok());
        ASSERT_TRUE(date_builder.Append(day).ok());
        ASSERT_TRUE(count_builder.Append(day - 19700).ok());
    }
    std::shared_ptr<arrow::Array> cells, dates, counts;
    ASSERT_TRUE(cell_builder.Finish(&cells).ok());
    ASSERT_TRUE(date_builder.Finish(&dates).ok());
    ASSERT_TRUE(count_builder.Finish(&counts).ok());
    auto schema = arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("count", arrow::uint32(), false),
    });
    auto table = arrow::Table::Make(schema, {cells, dates, counts});

    // Only the pruning columns carry row-group statistics in the footer.
    const std::string pruned = dir.join("pruned.parquet");
    arrow_table_io::write_table(pruned, table, 1'000, {"h3_cell", "change_date"});
    check_column_statistics(pruned, {true, true, false});

    // The default keeps statistics on every column (staging files etc.).
    const std::string all = dir.join("all.parquet");
    arrow_table_io::write_table(all, table, 1'000);
    check_column_statistics(all, {true, true, true});

    // Dropping statistics must not change the stored values.
    auto roundtrip = arrow_table_io::read_table(pruned);
    EXPECT_TRUE(roundtrip->Equals(*table));
}

}  // namespace
