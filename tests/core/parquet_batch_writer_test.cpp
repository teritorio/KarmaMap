#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "parquet_batch_writer.hpp"
#include "test_helpers.hpp"

namespace {

using test_helpers::TempDir;
using test_helpers::read_parquet;
using test_helpers::sum_column;
using parquet_out::CountKey;
using parquet_out::CountMap;
using parquet_out::ParquetBatchWriter;

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

TEST(ParquetBatchWriter, SchemaAndRoundTrip) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");
    const uint64_t cell_value = 0x0800792A0000FFFFULL;  // H3-like 64-bit value
    {
        ParquetBatchWriter writer(path);
        CountMap counts;
        counts[{cell_value, 5}] = 3;
        writer.flush(counts);
        EXPECT_TRUE(counts.empty());
    }

    auto table = read_parquet(path);
    ASSERT_EQ(table->num_rows(), 1);

    const auto* cell_array = static_cast<const arrow::UInt64Array*>(
        table->column(0)->chunk(0).get());
    ASSERT_EQ(cell_array->Value(0), cell_value);

    EXPECT_EQ(table->schema()->GetFieldIndex("h3_cell"), 0);
    EXPECT_EQ(table->schema()->GetFieldIndex("change_date"), 1);
    EXPECT_EQ(table->schema()->GetFieldIndex("count"), 2);
    EXPECT_TRUE(table->schema()->field(0)->type()->Equals(*arrow::uint64()));
    EXPECT_TRUE(table->schema()->field(1)->type()->Equals(*arrow::uint16()));
    EXPECT_TRUE(table->schema()->field(2)->type()->Equals(*arrow::uint32()));
    EXPECT_FALSE(table->schema()->field(0)->nullable());
}

TEST(ParquetBatchWriter, MultipleFlushesAppendRowGroups) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");
    {
        ParquetBatchWriter writer(path);
        CountMap counts;
        counts[{10, 1}] = 5;
        writer.flush(counts);
        EXPECT_TRUE(counts.empty());

        counts[{20, 2}] = 7;
        counts[{30, 3}] = 9;
        writer.flush(counts);
        EXPECT_TRUE(counts.empty());
    }

    EXPECT_EQ(num_row_groups(path), 2);
    EXPECT_EQ(sum_column(read_parquet(path), "count"), 21);
}

TEST(ParquetBatchWriter, MapAccumulatesBeforeFlush) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");
    {
        ParquetBatchWriter writer(path);
        CountMap counts;
        counts[{42, 7}] += 2;
        counts[{42, 7}] += 3;
        writer.flush(counts);
    }

    auto table = read_parquet(path);
    ASSERT_EQ(table->num_rows(), 1);
    const auto* count_array = static_cast<const arrow::UInt32Array*>(
        table->column(2)->chunk(0).get());
    EXPECT_EQ(count_array->Value(0), 5);
}

TEST(ParquetBatchWriter, EmptyFlushIsNoOp) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");
    {
        ParquetBatchWriter writer(path);
        CountMap counts;
        writer.flush(counts);
        EXPECT_TRUE(counts.empty());
    }

    EXPECT_EQ(num_row_groups(path), 0);
    EXPECT_EQ(read_parquet(path)->num_rows(), 0);
}

TEST(ParquetBatchWriter, DayOutOfRangeThrowsAndKeepsMap) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");

    ParquetBatchWriter writer(path);
    CountMap counts;
    counts[{1, 65536}] = 1;
    EXPECT_THROW(writer.flush(counts), std::runtime_error);
    EXPECT_FALSE(counts.empty());

    counts.clear();
    counts[{1, -1}] = 1;
    EXPECT_THROW(writer.flush(counts), std::runtime_error);
    EXPECT_FALSE(counts.empty());
}

TEST(ParquetBatchWriter, CloseIsIdempotent) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");
    {
        ParquetBatchWriter writer(path);
        CountMap counts;
        counts[{7, 9}] = 1;
        writer.flush(counts);
        writer.close();
        writer.close();
    }
    EXPECT_EQ(sum_column(read_parquet(path), "count"), 1);
}

TEST(ParquetBatchWriter, DestructorCloses) {
    TempDir dir;
    const std::string path = dir.join("out.parquet");
    {
        ParquetBatchWriter writer(path);
        CountMap counts;
        counts[{7, 9}] = 4;
        writer.flush(counts);
    }
    EXPECT_EQ(sum_column(read_parquet(path), "count"), 4);
}

}  // namespace