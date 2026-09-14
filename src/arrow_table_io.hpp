#pragma once

// Shared Arrow/Parquet table I/O for the merge-style passes (sort pass and
// the user-indicator finalize), so identical open/write/finalize behaviour
// is not duplicated.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace arrow_table_io {

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

inline void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table) {
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    if (!outfile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + " for writing: " +
                                  outfile_result.status().ToString());
    }

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    auto writer_props = props_builder.build();

    // One row group per file, bounded by a single partition's data.
    const int64_t chunk_size = table->num_rows() > 0 ? table->num_rows() : 1;

    auto write_status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                     *outfile_result, chunk_size, writer_props);
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write " + path + ": " + write_status.ToString());
    }

    auto close_status = (*outfile_result)->Close();
    if (!close_status.ok()) {
        throw std::runtime_error("Failed to finalize " + path + ": " + close_status.ToString());
    }
}

}  // namespace arrow_table_io