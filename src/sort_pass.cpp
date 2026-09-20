#include "sort_pass.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "h3_utils.hpp"
#include "parquet_batch_writer.hpp"

namespace sort_pass {

namespace {

// Adds every row of one source table to `merged`, summing each row's count
// into the (h3_cell, change_date) accumulator. Both the pass 1/pass 2
// staging files and an existing data.parquet fallback carry a single
// `count` column, so the same read path serves all three sources.
void merge_source(std::shared_ptr<arrow::Table> table, parquet_out::CountMap& merged) {
    const int cell_idx = table->schema()->GetFieldIndex("h3_cell");
    const int date_idx = table->schema()->GetFieldIndex("change_date");
    const int count_idx = table->schema()->GetFieldIndex("count");
    if (cell_idx < 0 || date_idx < 0 || count_idx < 0) {
        throw std::runtime_error("Unexpected source schema: missing h3_cell/"
                                 "change_date/count column");
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
        merged[key] += count_array->Value(i);
    }
}

void merge_one_year(const std::string& year_dir, int64_t change_group_rows) {
    const std::string nodes_path = year_dir + "/nodes.parquet";
    const std::string ways_path = year_dir + "/ways.parquet";
    const std::string output_path = year_dir + "/data.parquet";
    const std::string tmp_path = year_dir + "/data.parquet.tmp";

    const bool has_nodes = std::filesystem::exists(nodes_path);
    const bool has_ways = std::filesystem::exists(ways_path);
    const bool has_data = std::filesystem::exists(output_path);
    if (!has_nodes && !has_ways) return;  // nothing new to merge (already merged, or empty)

    std::cerr << "[sort pass] " << year_dir << "\n";

    parquet_out::CountMap merged;
    if (has_nodes) {
        merge_source(arrow_table_io::read_table(nodes_path), merged);
    } else if (has_data) {
        // No nodes.parquet, but an earlier data.parquet is present. Its
        // total count is the fallback so a re-merge never zeroes it.
        merge_source(arrow_table_io::read_table(output_path), merged);
    }
    if (has_ways) {
        merge_source(arrow_table_io::read_table(ways_path), merged);
    } else if (has_data) {
        merge_source(arrow_table_io::read_table(output_path), merged);
    }

    // Columns are built in map order; sort_by_keys reorders them by
    // (h3_cell, change_date) for compact row-group min/max ranges.
    const int64_t n = static_cast<int64_t>(merged.size());
    arrow::UInt64Builder cell_builder;
    arrow::UInt16Builder date_builder;
    arrow::UInt32Builder count_builder;
    if (!cell_builder.Reserve(n).ok() || !date_builder.Reserve(n).ok() ||
        !count_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while merging");
    }

    for (const auto& [key, count] : merged) {
        auto s1 = cell_builder.Append(key.h3_cell);
        auto s2 = date_builder.Append(h3_utils::require_u16_day(key.day));
        auto s3 = count_builder.Append(count);
        if (!s1.ok() || !s2.ok() || !s3.ok()) {
            throw std::runtime_error("Failed to append a merged row");
        }
    }

    std::shared_ptr<arrow::Array> cells, dates, counts;
    auto f1 = cell_builder.Finish(&cells);
    auto f2 = date_builder.Finish(&dates);
    auto f3 = count_builder.Finish(&counts);
    if (!f1.ok() || !f2.ok() || !f3.ok()) {
        throw std::runtime_error("Failed to finalize merged columns");
    }

    auto schema = arrow::schema({
        arrow::field("h3_cell", arrow::uint64(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("count", arrow::uint32(), false),
    });
    auto merged_table = arrow_table_io::sort_by_keys(
        arrow::Table::Make(schema, {cells, dates, counts}),
        {arrow::compute::SortKey("h3_cell"), arrow::compute::SortKey("change_date")});

    // Write under a temp name, rename into place, and only then remove the
    // staging files. A re-run reads whatever staging files survived and, for
    // counts whose staging file was already removed, reuses data.parquet
    // (only ever created by a completed rename).
    // The viewers filter on h3_cell (bbox) and change_date (span); only those
    // columns keep row-group min/max statistics in the footer.
    arrow_table_io::write_table(tmp_path, merged_table, change_group_rows,
                                {"h3_cell", "change_date"});
    std::filesystem::rename(tmp_path, output_path);
    if (has_nodes) std::filesystem::remove(nodes_path);
    if (has_ways) std::filesystem::remove(ways_path);
}

}  // namespace

void merge_and_sort_partitions(const std::string& root_dir, int64_t change_group_rows) {
    // Registers Arrow's compute kernels (e.g. sort_indices, take); without
    // this the functions are missing from the registry and sorting fails.
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize Arrow compute: " +
                                 init_status.ToString());
    }
    if (!std::filesystem::exists(root_dir)) return;

    for (const auto& year_entry : std::filesystem::directory_iterator(root_dir)) {
        if (year_entry.is_directory()) {
            merge_one_year(year_entry.path().string(), change_group_rows);
        }
    }
}

}  // namespace sort_pass
