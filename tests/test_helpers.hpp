#pragma once

// Shared helpers for the C++ test suite. Kept free of osmium includes so the
// unit_tests binary (which exercises only src/*.hpp pure logic) stays lean;
// osmium-backed builders arrive with the handler/CLI tests.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

namespace test_helpers {

// RAII temporary directory, removed on destruction.
class TempDir {
public:
    TempDir() {
        dir_ = std::filesystem::temp_directory_path() /
               ("karmamap_test_" +
                std::to_string(
                    std::chrono::high_resolution_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir_);
    }

    ~TempDir() { std::filesystem::remove_all(dir_); }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    std::string path() const { return dir_.string(); }

    std::string join(const std::string& name) const { return (dir_ / name).string(); }

private:
    std::filesystem::path dir_;
};

// Reads a whole Parquet file back into a table. Mirrors sort_pass's read path.
inline std::shared_ptr<arrow::Table> read_parquet(const std::string& path) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    if (!infile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + ": " +
                                 infile_result.status().ToString());
    }
    auto reader_result = parquet::arrow::OpenFile(*infile_result, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        throw std::runtime_error("Failed to open Parquet reader for " + path + ": " +
                                 reader_result.status().ToString());
    }
    auto table_result = (*reader_result)->ReadTable();
    if (!table_result.ok()) {
        throw std::runtime_error("Failed to read table from " + path + ": " +
                                 table_result.status().ToString());
    }
    return *table_result;
}

// Sums a numeric column (uint64/uint32/uint16/int32) for spot checks.
inline int64_t sum_column(const std::shared_ptr<arrow::Table>& table, const std::string& name) {
    int idx = table->schema()->GetFieldIndex(name);
    if (idx < 0) throw std::runtime_error("No column named " + name);
    auto combined_result = table->CombineChunks();
    if (!combined_result.ok()) throw std::runtime_error("CombineChunks failed");
    auto combined = *combined_result;
    auto array = combined->column(idx)->chunk(0);

    int64_t total = 0;
    switch (array->type_id()) {
        case arrow::Type::UINT64: {
            auto a = std::static_pointer_cast<arrow::UInt64Array>(array);
            for (int64_t i = 0; i < a->length(); ++i) total += static_cast<int64_t>(a->Value(i));
            break;
        }
        case arrow::Type::UINT32: {
            auto a = std::static_pointer_cast<arrow::UInt32Array>(array);
            for (int64_t i = 0; i < a->length(); ++i) total += a->Value(i);
            break;
        }
        case arrow::Type::UINT16: {
            auto a = std::static_pointer_cast<arrow::UInt16Array>(array);
            for (int64_t i = 0; i < a->length(); ++i) total += a->Value(i);
            break;
        }
        case arrow::Type::INT32: {
            auto a = std::static_pointer_cast<arrow::Int32Array>(array);
            for (int64_t i = 0; i < a->length(); ++i) total += a->Value(i);
            break;
        }
        default:
            throw std::runtime_error("Unsupported column type for sum_column: " + name);
    }
    return total;
}

// Writes a whole table to Parquet (ZSTD-compressed) and closes the file.
// Shared by the core tests that seed stage files for the sort, manifest and
// history passes.
inline void write_table(const std::string& path,
                        const std::shared_ptr<arrow::Table>& table) {
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    if (!outfile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + " for writing: " +
                                 outfile_result.status().ToString());
    }

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    auto write_status =
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outfile_result,
                                   /*chunk_size=*/table->num_rows(), props_builder.build());
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write file " + path + ": " +
                                 write_status.ToString());
    }
    if (!(*outfile_result)->Close().ok()) {
        throw std::runtime_error("Failed to finalize file " + path);
    }
}

// Writes a staging (h3_cell, change_date, count) Parquet file, the format
// produced by passes 1 and 2, used to seed sort/manifest tests.
inline void write_staging(
    const std::string& path,
    const std::vector<std::tuple<uint64_t, int32_t, uint32_t>>& rows) {
    arrow::UInt64Builder cell_builder;
    arrow::UInt16Builder date_builder;
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
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("count", arrow::uint32(), false),
    });
    auto table = arrow::Table::Make(schema, {cells, dates, counts});

    write_table(path, table);
}

}  // namespace test_helpers