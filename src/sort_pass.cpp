#include "sort_pass.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "h3_utils.hpp"
#include "parquet_batch_writer.hpp"

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

    // ReadTable() no-arg: the out-param overload is deprecated in Parquet 24.0.0.
    auto table_result = reader->ReadTable();
    if (!table_result.ok()) {
        throw std::runtime_error("Failed to read table from " + path + ": " +
                                  table_result.status().ToString());
    }
    return *table_result;
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

    // One row group per file, bounded by a single partition's data.
    const int64_t chunk_size = table->num_rows() > 0 ? table->num_rows() : 1;

    auto write_status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                     *outfile_result, chunk_size, writer_props);
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write " + path + ": " + write_status.ToString());
    }

    // Fail fast on disk errors surfaced by closing/flushing the underlying
    // file (e.g. ENOSPC delayed until metadata is flushed on close), instead
    // of silently leaving a truncated partition behind.
    auto close_status = (*outfile_result)->Close();
    if (!close_status.ok()) {
        throw std::runtime_error("Failed to finalize " + path + ": " + close_status.ToString());
    }
}

// Orders a table by (h3_cell, change_date) with Arrow's compute kernels
// (SortIndices + Take), so row-group min/max ranges stay compact for bbox
// (h3_cell) and date (change_date) pruning.
std::shared_ptr<arrow::Table> sort_by_h3_day(const std::shared_ptr<arrow::Table>& table) {
    arrow::compute::SortOptions options({arrow::compute::SortKey("h3_cell"),
                                         arrow::compute::SortKey("change_date")});
    auto indices_result = arrow::compute::SortIndices(arrow::Datum(table), options);
    if (!indices_result.ok()) {
        throw std::runtime_error("Failed to sort merged rows: " +
                                 indices_result.status().ToString());
    }
    const std::shared_ptr<arrow::Array> index_array = indices_result.ValueOrDie();

    std::vector<std::shared_ptr<arrow::Array>> sorted_columns;
    for (const auto& column : table->columns()) {
        if (column->num_chunks() != 1) {
            throw std::runtime_error("Unexpected multi-chunk column while sorting merged rows");
        }
        auto taken_result = arrow::compute::Take(*column->chunk(0), *index_array,
                                                 arrow::compute::TakeOptions::Defaults());
        if (!taken_result.ok()) {
            throw std::runtime_error("Failed to order merged rows: " +
                                     taken_result.status().ToString());
        }
        sorted_columns.push_back(taken_result.ValueOrDie());
    }
    return arrow::Table::Make(table->schema(), sorted_columns);
}

struct MergedPair {
    uint32_t node = 0;
    uint32_t way = 0;
};

using MergedMap = std::unordered_map<parquet_out::CountKey, MergedPair, parquet_out::CountKeyHash>;

// Adds every row of one source table to `merged`, into the node_count or
// way_count column depending on `is_node`, so rows present in only one
// source keep 0 in the other column. `count_col` names the count column:
// "count" for the pass 1/pass 2 staging files, "node_count"/"way_count"
// when reading an existing data.parquet as a fallback source.
void merge_source(const std::shared_ptr<arrow::Table>& table, const char* count_col, bool is_node,
                  MergedMap& merged) {
    const int cell_idx = table->schema()->GetFieldIndex("h3_cell");
    const int date_idx = table->schema()->GetFieldIndex("change_date");
    const int count_idx = table->schema()->GetFieldIndex(count_col);
    if (cell_idx < 0 || date_idx < 0 || count_idx < 0) {
        throw std::runtime_error(std::string("Unexpected source schema: missing h3_cell/")
                                     .append("change_date/")
                                     .append(count_col)
                                     .append(" column"));
    }

    // change_date is a uint16 epoch-day count; reject any other type loudly
    // instead of silently re-decoding it as garbage.
    const std::shared_ptr<arrow::Field> date_field = table->schema()->field(date_idx);
    if (!date_field->type()->Equals(*arrow::uint16())) {
        throw std::runtime_error("Incompatible Parquet schema: change_date is '"
                                 + date_field->type()->ToString()
                                 + "', expected a uint16-day-count column; "
                                   "rerun with --pass all to rebuild the dataset");
    }

    // CombineChunks() so each column below is a single typed array.
    auto combined_result = table->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + combined_result.status().ToString());
    }
    std::shared_ptr<arrow::Table> combined = *combined_result;

    auto cell_array =
        std::static_pointer_cast<arrow::UInt64Array>(combined->column(cell_idx)->chunk(0));
    auto date_array =
        std::static_pointer_cast<arrow::UInt16Array>(combined->column(date_idx)->chunk(0));
    auto count_array =
        std::static_pointer_cast<arrow::UInt32Array>(combined->column(count_idx)->chunk(0));

    const int64_t n = combined->num_rows();
    for (int64_t i = 0; i < n; ++i) {
        parquet_out::CountKey key{cell_array->Value(i),
                                  static_cast<int32_t>(date_array->Value(i))};
        MergedPair& value = merged[key];
        uint32_t& target = is_node ? value.node : value.way;
        target += count_array->Value(i);
    }
}

void merge_one_month(const std::string& month_dir) {
    const std::string nodes_path = month_dir + "/nodes.parquet";
    const std::string ways_path = month_dir + "/ways.parquet";
    const std::string output_path = month_dir + "/data.parquet";
    const std::string tmp_path = month_dir + "/data.parquet.tmp";

    const bool has_nodes = std::filesystem::exists(nodes_path);
    const bool has_ways = std::filesystem::exists(ways_path);
    const bool has_data = std::filesystem::exists(output_path);
    if (!has_nodes && !has_ways) return;  // nothing new to merge (already merged, or empty)

    std::cerr << "[sort pass] " << month_dir << "\n";

    MergedMap merged;
    if (has_nodes) {
        merge_source(read_table(nodes_path), "count", /*is_node=*/true, merged);
    } else if (has_data) {
        // No nodes.parquet, but an earlier data.parquet is present. Its
        // node_count is the fallback so a re-merge never zeroes the column.
        merge_source(read_table(output_path), "node_count", /*is_node=*/true, merged);
    }
    if (has_ways) {
        merge_source(read_table(ways_path), "count", /*is_node=*/false, merged);
    } else if (has_data) {
        merge_source(read_table(output_path), "way_count", /*is_node=*/false, merged);
    }

    // Columns are built in map order; sort_by_h3_day reorders them by
    // (h3_cell, change_date) for compact row-group min/max ranges.
    const int64_t n = static_cast<int64_t>(merged.size());
    arrow::UInt64Builder cell_builder;
    arrow::UInt16Builder date_builder;
    arrow::UInt32Builder node_builder;
    arrow::UInt32Builder way_builder;
    if (!cell_builder.Reserve(n).ok() || !date_builder.Reserve(n).ok() ||
        !node_builder.Reserve(n).ok() || !way_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while merging");
    }

    for (const auto& [key, value] : merged) {
        auto s1 = cell_builder.Append(key.h3_cell);
        auto s2 = date_builder.Append(h3_utils::require_u16_day(key.day));
        auto s3 = node_builder.Append(value.node);
        auto s4 = way_builder.Append(value.way);
        if (!s1.ok() || !s2.ok() || !s3.ok() || !s4.ok()) {
            throw std::runtime_error("Failed to append a merged row");
        }
    }

    std::shared_ptr<arrow::Array> cells, dates, nodes, ways;
    auto f1 = cell_builder.Finish(&cells);
    auto f2 = date_builder.Finish(&dates);
    auto f3 = node_builder.Finish(&nodes);
    auto f4 = way_builder.Finish(&ways);
    if (!f1.ok() || !f2.ok() || !f3.ok() || !f4.ok()) {
        throw std::runtime_error("Failed to finalize merged columns");
    }

    auto schema = arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("node_count", arrow::uint32(), false),
        arrow::field("way_count", arrow::uint32(), false),
    });
    auto merged_table = sort_by_h3_day(arrow::Table::Make(schema, {cells, dates, nodes, ways}));

    // Write under a temp name, rename into place, and only then remove the
    // staging files. A re-run reads whatever staging files survived and, for
    // counts whose staging file was already removed, reuses data.parquet
    // (only ever created by a completed rename).
    write_table(tmp_path, merged_table);
    std::filesystem::rename(tmp_path, output_path);
    if (has_nodes) std::filesystem::remove(nodes_path);
    if (has_ways) std::filesystem::remove(ways_path);
}

}  // namespace

void merge_and_sort_partitions(const std::string& root_dir) {
    // Registers Arrow's compute kernels (e.g. sort_indices, take); without
    // this the functions are missing from the registry and sorting fails.
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize Arrow compute: " +
                                 init_status.ToString());
    }
    if (!std::filesystem::exists(root_dir)) return;

    for (const auto& year_entry : std::filesystem::directory_iterator(root_dir)) {
        if (!year_entry.is_directory()) continue;
        for (const auto& month_entry : std::filesystem::directory_iterator(year_entry.path())) {
            if (!month_entry.is_directory()) continue;
            merge_one_month(month_entry.path().string());
        }
    }
}

}  // namespace sort_pass
