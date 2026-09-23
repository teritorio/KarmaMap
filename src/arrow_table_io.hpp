#pragma once

// Shared Arrow/Parquet table I/O for the merge-style passes (sort pass and
// the users-history finalize), so identical open/write/finalize behaviour
// is not duplicated.

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace arrow_table_io {

// Default row-group size when a caller does not pass an explicit value.
inline constexpr int64_t kDefaultRowGroupRows = 500'000;

inline std::shared_ptr<arrow::Table> read_table(const std::string& path) {
    auto infile_result = arrow::io::ReadableFile::Open(path);
    if (!infile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + " for reading: " +
                                  infile_result.status().ToString());
    }

    auto reader_result = parquet::arrow::OpenFile(*infile_result, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        throw std::runtime_error("Failed to open Parquet reader for " + path + ": " +
                                  reader_result.status().ToString());
    }
    std::unique_ptr<parquet::arrow::FileReader> reader = std::move(*reader_result);

    // ReadTable() no-arg: the out-param overload is deprecated in Parquet 24.0.0.
    auto table_result = reader->ReadTable();
    if (!table_result.ok()) {
        throw std::runtime_error("Failed to read table from " + path + ": " +
                                  table_result.status().ToString());
    }
    return *table_result;
}

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table,
                        const std::shared_ptr<arrow::KeyValueMetadata>& file_metadata,
                        int64_t row_group_rows,
                        const std::vector<std::string>& statistics_columns) {
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    if (!outfile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + " for writing: " +
                                  outfile_result.status().ToString());
    }

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    // Only the columns the viewers prune on (`statistics_columns`) keep
    // row-group min/max statistics in the footer; the others are written
    // without chunk stats to keep the footer metadata compact.
    for (const auto& field : table->schema()->fields()) {
        const std::string& name = field->name();
        if (!statistics_columns.empty() &&
            std::find(statistics_columns.begin(), statistics_columns.end(), name) ==
                statistics_columns.end()) {
            props_builder.disable_statistics(name);
        }
    }
    auto writer_props = props_builder.build();

    // Row groups of at most `row_group_rows` rows.
    const int64_t chunk_size = table->num_rows() > 0 ? row_group_rows : 1;

    // FileWriter is used instead of the WriteTable convenience so caller
    // key_value_metadata lands in the Parquet footer, independent of whether
    // the writer also propagates the Arrow schema metadata.
    auto writer_result = parquet::arrow::FileWriter::Open(
        *table->schema(), arrow::default_memory_pool(), *outfile_result, writer_props);
    if (!writer_result.ok()) {
        throw std::runtime_error("Failed to open Parquet writer for " + path + ": " +
                                  writer_result.status().ToString());
    }
    std::unique_ptr<parquet::arrow::FileWriter> writer = std::move(*writer_result);

    auto write_status = writer->WriteTable(*table, chunk_size);
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write " + path + ": " + write_status.ToString());
    }
    if (file_metadata) {
        auto meta_status = writer->AddKeyValueMetadata(file_metadata);
        if (!meta_status.ok()) {
            throw std::runtime_error("Failed to add metadata to " + path + ": " +
                                      meta_status.ToString());
        }
    }
    auto close_status = writer->Close();
    if (!close_status.ok()) {
        throw std::runtime_error("Failed to finalize " + path + ": " +
                                  close_status.ToString());
    }

    auto close_sink_status = (*outfile_result)->Close();
    if (!close_sink_status.ok()) {
        throw std::runtime_error("Failed to close " + path + ": " +
                                  close_sink_status.ToString());
    }
}

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table,
                        const std::shared_ptr<arrow::KeyValueMetadata>& file_metadata,
                        int64_t row_group_rows) {
    write_table(path, table, file_metadata, row_group_rows, {});
}

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table,
                        int64_t row_group_rows,
                        const std::vector<std::string>& statistics_columns) {
    write_table(path, table, nullptr, row_group_rows, statistics_columns);
}

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table,
                        int64_t row_group_rows) {
    write_table(path, table, nullptr, row_group_rows);
}

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table,
                        const std::shared_ptr<arrow::KeyValueMetadata>& file_metadata) {
    write_table(path, table, file_metadata, kDefaultRowGroupRows);
}

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table) {
    write_table(path, table, nullptr);
}

// Orders a table by the given named-column keys (SortIndices + Take), so the
// row-group min/max ranges of a partition's file stay compact for bbox and
// date pruning. Every column must be single-chunk, as the merge-style passes
// build them.
inline std::shared_ptr<arrow::Table> sort_by_keys(
    const std::shared_ptr<arrow::Table>& table,
    const std::vector<arrow::compute::SortKey>& keys) {
    arrow::compute::SortOptions options(keys);
    auto indices_result = arrow::compute::SortIndices(arrow::Datum(table), options);
    if (!indices_result.ok()) {
        throw std::runtime_error("Failed to sort rows: " +
                                 indices_result.status().ToString());
    }
    const std::shared_ptr<arrow::Array> index_array = indices_result.ValueOrDie();

    std::vector<std::shared_ptr<arrow::Array>> sorted_columns;
    for (const auto& column : table->columns()) {
        if (column->num_chunks() != 1) {
            throw std::runtime_error("Unexpected multi-chunk column while sorting");
        }
        auto taken_result = arrow::compute::Take(*column->chunk(0), *index_array,
                                                 arrow::compute::TakeOptions::Defaults());
        if (!taken_result.ok()) {
            throw std::runtime_error("Failed to order rows: " +
                                     taken_result.status().ToString());
        }
        sorted_columns.push_back(taken_result.ValueOrDie());
    }
    return arrow::Table::Make(table->schema(), sorted_columns);
}

}  // namespace arrow_table_io
