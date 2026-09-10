#include "sort_pass.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <vector>

namespace sort_pass {

namespace {

std::shared_ptr<arrow::Table> read_table(const std::string& path) {
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

    // ReadTable(shared_ptr<Table>*) is deprecated as of Parquet 24.0.0 in
    // favor of a Result-returning overload; the exact signature below is
    // inferred from that deprecation message, not independently confirmed
    // against the installed header at the time this was written.
    auto table_result = reader->ReadTable();
    if (!table_result.ok()) {
        throw std::runtime_error("Failed to read table from " + path + ": " +
                                  table_result.status().ToString());
    }
    return *table_result;
}

// Sorts the table by h3_cell without using arrow::compute: on the Arrow
// build resolved at container-build time, the vector compute kernels
// (sort_indices, take) are not registered ("No function registered with
// name: sort_indices"), which this project has no control over (likely a
// trimmed Debian package build). Sorting is done manually instead: extract
// typed columns, sort a plain index vector with std::sort, then rebuild
// the table by appending values in that order.
std::shared_ptr<arrow::Table> sort_by_h3_cell(const std::shared_ptr<arrow::Table>& table) {
    // CombineChunks() merges every column down to a single chunk, so each
    // column below is a single typed array instead of a ChunkedArray with
    // an unknown number of pieces.
    auto combined_result = table->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + combined_result.status().ToString());
    }
    std::shared_ptr<arrow::Table> combined = *combined_result;

    int cell_idx = combined->schema()->GetFieldIndex("h3_cell");
    int date_idx = combined->schema()->GetFieldIndex("change_date");
    int count_idx = combined->schema()->GetFieldIndex("count");
    if (cell_idx < 0 || date_idx < 0 || count_idx < 0) {
        throw std::runtime_error("Unexpected schema: missing h3_cell/change_date/count column");
    }

    const int64_t n = combined->num_rows();
    if (n == 0) return combined;

    auto cell_array =
        std::static_pointer_cast<arrow::UInt64Array>(combined->column(cell_idx)->chunk(0));
    auto date_array =
        std::static_pointer_cast<arrow::Date32Array>(combined->column(date_idx)->chunk(0));
    auto count_array =
        std::static_pointer_cast<arrow::UInt32Array>(combined->column(count_idx)->chunk(0));

    std::vector<int64_t> indices(static_cast<size_t>(n));
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
        return cell_array->Value(a) < cell_array->Value(b);
    });

    arrow::UInt64Builder sorted_cell_builder;
    arrow::Date32Builder sorted_date_builder;
    arrow::UInt32Builder sorted_count_builder;

    if (!sorted_cell_builder.Reserve(n).ok() || !sorted_date_builder.Reserve(n).ok() ||
        !sorted_count_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while sorting");
    }

    for (int64_t idx : indices) {
        auto s1 = sorted_cell_builder.Append(cell_array->Value(idx));
        auto s2 = sorted_date_builder.Append(date_array->Value(idx));
        auto s3 = sorted_count_builder.Append(count_array->Value(idx));
        if (!s1.ok() || !s2.ok() || !s3.ok()) {
            throw std::runtime_error("Failed to append a sorted row");
        }
    }

    std::shared_ptr<arrow::Array> sorted_cell, sorted_date, sorted_count;
    auto f1 = sorted_cell_builder.Finish(&sorted_cell);
    auto f2 = sorted_date_builder.Finish(&sorted_date);
    auto f3 = sorted_count_builder.Finish(&sorted_count);
    if (!f1.ok() || !f2.ok() || !f3.ok()) {
        throw std::runtime_error("Failed to finalize sorted columns");
    }

    return arrow::Table::Make(combined->schema(), {sorted_cell, sorted_date, sorted_count});
}

void write_table(const std::string& path, const std::shared_ptr<arrow::Table>& table) {
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    if (!outfile_result.ok()) {
        throw std::runtime_error("Failed to open " + path + " for writing: " +
                                  outfile_result.status().ToString());
    }

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    auto writer_props = props_builder.build();

    // Row count is bounded by a single partition's data (one month), so a
    // single row group for the whole file is fine here.
    const int64_t chunk_size = table->num_rows() > 0 ? table->num_rows() : 1;

    auto write_status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                     *outfile_result, chunk_size, writer_props);
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write " + path + ": " + write_status.ToString());
    }
}

void sort_one_file(const std::string& original) {
    // "<name>.parquet" -> "<name>.sorted.parquet"
    const std::string suffix = ".parquet";
    const std::string sorted =
        original.substr(0, original.size() - suffix.size()) + ".sorted.parquet";

    std::cerr << "[sort pass] " << original << "\n";

    auto table = read_table(original);
    auto sorted_table = sort_by_h3_cell(table);
    write_table(sorted, sorted_table);

    // Only remove the original once the sorted file was written successfully.
    std::filesystem::remove(original);
    std::filesystem::rename(sorted, original);
}

}  // namespace

void sort_partitions(const std::string& root_dir) {
    if (!std::filesystem::exists(root_dir)) return;

    for (const auto& entry : std::filesystem::recursive_directory_iterator(root_dir)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".parquet") continue;

        sort_one_file(entry.path().string());
    }
}

}  // namespace sort_pass
