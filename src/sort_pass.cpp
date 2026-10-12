#include "sort_pass.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <parquet/file_reader.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
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

// Ends with helper avoiding std::string::ends_with (libstdc++ 11+ only).
bool ends_with(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

// Computes the weighted spatial center (lon, lat) and total weight from a
// merged CountMap. Weights are the total count per cell (summed across
// all dates). Returns nullopt if no data.
struct SpatialCenter {
    double lon;
    double lat;
    uint64_t weight;
};

std::optional<SpatialCenter> compute_spatial_center(const parquet_out::CountMap& merged) {
    double sum_lon = 0.0;
    double sum_lat = 0.0;
    uint64_t total = 0;
    for (const auto& [key, count] : merged) {
        auto [lat, lon] = h3_utils::cell_to_latlng(key.h3_cell);
        sum_lat += static_cast<double>(count) * lat;
        sum_lon += static_cast<double>(count) * lon;
        total += count;
    }
    if (total == 0) return std::nullopt;
    return SpatialCenter{sum_lon / total, sum_lat / total, total};
}

// The replication sequence of an update staging name (nodes.<seq>.parquet /
// ways.<seq>.parquet); nullopt for the full-run staging name (nodes.parquet /
// ways.parquet) and any name that does not carry a numeric sequence.
std::optional<uint64_t> staging_seq(const std::string& name) {
    const std::string suffix = ".parquet";
    if (!ends_with(name, suffix)) return std::nullopt;
    const std::string stem = name.substr(0, name.size() - suffix.size());
    const size_t dot = stem.find('.');
    if (dot == std::string::npos) return std::nullopt;  // full-run staging
    const std::string digits = stem.substr(dot + 1);
    if (digits.empty()) return std::nullopt;
    for (char c : digits) {
        if (c < '0' || c > '9') return std::nullopt;
    }
    return std::stoull(digits);
}

// The column indices of the (h3_cell, change_date, count) schema shared by the
// pass 1/pass 2 staging files, an existing data.parquet and the merged output.
struct SourceCols {
    int cell;
    int date;
    int count;
};

// Validates that shared schema, rejecting a non-uint16 change_date loudly
// instead of silently re-decoding it as garbage.
SourceCols validate_source_schema(const std::shared_ptr<arrow::Table>& table) {
    const int cell_idx = table->schema()->GetFieldIndex("h3_cell");
    const int date_idx = table->schema()->GetFieldIndex("change_date");
    const int count_idx = table->schema()->GetFieldIndex("count");
    if (cell_idx < 0 || date_idx < 0 || count_idx < 0) {
        throw std::runtime_error("Unexpected source schema: missing h3_cell/"
                                 "change_date/count column");
    }
    const std::shared_ptr<arrow::Field> date_field = table->schema()->field(date_idx);
    if (!date_field->type()->Equals(*arrow::uint16())) {
        throw std::runtime_error("Incompatible Parquet schema: change_date is '"
                                 + date_field->type()->ToString()
                                 + "', expected a uint16-day-count column; "
                                   "rerun with --pass all to rebuild the dataset");
    }
    return SourceCols{cell_idx, date_idx, count_idx};
}

// Adds every row of one source table to `merged`, summing each row's count
// into the (h3_cell, change_date) accumulator. Both the pass 1/pass 2
// staging files and an existing data.parquet fallback carry a single
// `count` column, so the same read path serves all three sources.
void merge_source(std::shared_ptr<arrow::Table> table, parquet_out::CountMap& merged) {
    const SourceCols cols = validate_source_schema(table);

    // CombineChunks() so each column below is a single typed array.
    auto combined_result = table->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + combined_result.status().ToString());
    }
    std::shared_ptr<arrow::Table> combined = *combined_result;

    auto cell_array =
        std::static_pointer_cast<arrow::UInt64Array>(combined->column(cols.cell)->chunk(0));
    auto date_array =
        std::static_pointer_cast<arrow::UInt16Array>(combined->column(cols.date)->chunk(0));
    auto count_array =
        std::static_pointer_cast<arrow::UInt32Array>(combined->column(cols.count)->chunk(0));

    const int64_t n = combined->num_rows();
    for (int64_t i = 0; i < n; ++i) {
        parquet_out::CountKey key{cell_array->Value(i),
                                  static_cast<int32_t>(date_array->Value(i))};
        merged[key] += count_array->Value(i);
    }
}

// A single (h3_cell, change_date, count) row collected for the update merge.
struct MergeRow {
    uint64_t cell;
    int32_t day;
    uint32_t count;
};

// Appends every row of a staging partition to `out`, validating the schema.
void collect_rows(const std::shared_ptr<arrow::Table>& table, std::vector<MergeRow>& out) {
    const SourceCols cols = validate_source_schema(table);
    auto combined_result = table->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + combined_result.status().ToString());
    }
    const std::shared_ptr<arrow::Table> combined = *combined_result;
    auto cell_array =
        std::static_pointer_cast<arrow::UInt64Array>(combined->column(cols.cell)->chunk(0));
    auto date_array =
        std::static_pointer_cast<arrow::UInt16Array>(combined->column(cols.date)->chunk(0));
    auto count_array =
        std::static_pointer_cast<arrow::UInt32Array>(combined->column(cols.count)->chunk(0));
    const int64_t n = combined->num_rows();
    out.reserve(out.size() + static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        out.push_back(MergeRow{cell_array->Value(i),
                               static_cast<int32_t>(date_array->Value(i)),
                               count_array->Value(i)});
    }
}

void append_merged(arrow::UInt64Builder& cells, arrow::UInt16Builder& dates,
                   arrow::UInt32Builder& counts, uint64_t cell, int32_t day, uint32_t count) {
    const auto s1 = cells.Append(cell);
    const auto s2 = dates.Append(h3_utils::require_u16_day(day));
    const auto s3 = counts.Append(count);
    if (!s1.ok() || !s2.ok() || !s3.ok()) {
        throw std::runtime_error("Failed to append a merged row");
    }
}

// Builds the sorted (h3_cell, change_date, count) table a merged CountMap
// becomes, with the columns built in map order and sort_by_keys reordering
// them by (h3_cell, change_date) for compact row-group min/max ranges.
std::shared_ptr<arrow::Table> build_merged_table(const parquet_out::CountMap& merged) {
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
    return arrow_table_io::sort_by_keys(
        arrow::Table::Make(schema, {cells, dates, counts}),
        {arrow::compute::SortKey("h3_cell"), arrow::compute::SortKey("change_date")});
}

std::optional<std::pair<std::pair<double, double>, uint64_t>> merge_one_year(const std::string& year_dir, int64_t change_group_rows) {
    const std::string nodes_path = year_dir + "/nodes.parquet";
    const std::string ways_path = year_dir + "/ways.parquet";
    const std::string output_path = year_dir + "/data.parquet";
    const std::string tmp_path = year_dir + "/data.parquet.tmp";

    const bool has_nodes = std::filesystem::exists(nodes_path);
    const bool has_ways = std::filesystem::exists(ways_path);
    const bool has_data = std::filesystem::exists(output_path);
    if (!has_nodes && !has_ways) return std::nullopt;  // nothing new to merge (already merged, or empty)

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

    auto center = compute_spatial_center(merged);

    // Columns are built in map order; sort_by_keys reorders them by
    // (h3_cell, change_date) for compact row-group min/max ranges.
    std::shared_ptr<arrow::Table> merged_table = build_merged_table(merged);

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

    if (center) {
        return std::make_pair(std::make_pair(center->lon, center->lat), center->weight);
    }
    return std::nullopt;
}

}  // namespace

std::optional<std::pair<double, double>> merge_and_sort_partitions(const std::string& root_dir, int64_t change_group_rows) {
    // Registers Arrow's compute kernels (e.g. sort_indices, take); without
    // this the functions are missing from the registry and sorting fails.
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize Arrow compute: " +
                                 init_status.ToString());
    }
    if (!std::filesystem::exists(root_dir)) return std::nullopt;

    double sum_lon = 0.0;
    double sum_lat = 0.0;
    uint64_t total_weight = 0;
    bool any = false;

    for (const auto& year_entry : std::filesystem::directory_iterator(root_dir)) {
        if (year_entry.is_directory()) {
            auto result = merge_one_year(year_entry.path().string(), change_group_rows);
            if (result) {
                any = true;
                const auto& center = result->first;
                uint64_t weight = result->second;
                sum_lon += center.first * weight;
                sum_lat += center.second * weight;
                total_weight += weight;
            }
        }
    }

    if (!any) return std::nullopt;
    return std::make_pair(sum_lon / total_weight, sum_lat / total_weight);
}

uint64_t read_source_sequence(const std::string& data_path) {
    const std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(data_path, false);
    const auto meta = reader->metadata();
    if (!meta->key_value_metadata()) return 0;
    const int idx = meta->key_value_metadata()->FindKey(karmamap_source_seq);
    if (idx == -1) return 0;
    try {
        return std::stoull(meta->key_value_metadata()->value(idx));
    } catch (const std::exception&) {
        return 0;
    }
}

std::optional<std::pair<double, double>> merge_update_partitions(const std::string& root_dir, int64_t change_group_rows,
                                                                 uint64_t applied_seq) {
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        throw std::runtime_error("Failed to initialize Arrow compute: " +
                                 init_status.ToString());
    }
    if (!std::filesystem::exists(root_dir)) return std::nullopt;

    double sum_lon = 0.0;
    double sum_lat = 0.0;
    uint64_t total_weight = 0;
    bool any = false;

    for (const auto& year_entry : std::filesystem::directory_iterator(root_dir)) {
        if (!year_entry.is_directory()) continue;
        const std::string year_dir = year_entry.path().string();

        // Collect the run's staging partitions for this year, in sequence
        // order. Staging carrying a sequence later than this run's applied
        // sequence (left by a crashed run that staged more than this run
        // applies) is never folded: it is dropped here and regenerated by the
        // run that reaches it.
        std::vector<std::string> node_staging, way_staging, stale_staging;
        for (const auto& file_entry : std::filesystem::directory_iterator(year_dir)) {
            if (!file_entry.is_regular_file()) continue;
            const std::string name = file_entry.path().filename().string();
            const bool is_nodes =
                name.compare(0, 6, "nodes.") == 0 && ends_with(name, ".parquet");
            const bool is_ways =
                name.compare(0, 5, "ways.") == 0 && ends_with(name, ".parquet");
            if (!is_nodes && !is_ways) continue;
            const std::string path = file_entry.path().string();
            const auto seq = staging_seq(name);
            if (seq && *seq > applied_seq) {
                stale_staging.push_back(path);
            } else if (is_nodes) {
                node_staging.push_back(path);
            } else {
                way_staging.push_back(path);
            }
        }
        std::sort(node_staging.begin(), node_staging.end());
        std::sort(way_staging.begin(), way_staging.end());

        const auto remove_staging = [&] {
            for (const auto& p : node_staging) std::filesystem::remove(p);
            for (const auto& p : way_staging) std::filesystem::remove(p);
            for (const auto& p : stale_staging) std::filesystem::remove(p);
        };
        if (node_staging.empty() && way_staging.empty()) {
            remove_staging();
            continue;
        }

        const std::string output_path = year_dir + "/data.parquet";
        const std::string tmp_path = year_dir + "/data.parquet.tmp";

        // A completed earlier run of the same sequence already rewrote the
        // partition (stamp >= applied_seq): drop the orphaned staging and
        // leave the merged data untouched (a re-merge would be idempotent but
        // wasteful).
        if (std::filesystem::exists(output_path) &&
            read_source_sequence(output_path) >= applied_seq) {
            std::cerr << "[update merge] " << year_dir << " already at seq "
                      << applied_seq << ", dropping staging\n";
            remove_staging();
            continue;
        }

        std::cerr << "[update merge] " << year_dir << "\n";

        // The base dataset is sorted by (h3_cell, change_date) and only ever
        // replaced by a completed rename. The run's staging is folded in with a
        // linear merge, so no hash table and no global re-sort is needed.
        std::shared_ptr<arrow::Table> base_combined;
        const arrow::UInt64Array* base_cells = nullptr;
        const arrow::UInt16Array* base_dates = nullptr;
        const arrow::UInt32Array* base_counts = nullptr;
        int64_t base_n = 0;
        if (std::filesystem::exists(output_path)) {
            auto base_result = arrow_table_io::read_table(output_path)->CombineChunks();
            if (!base_result.ok()) {
                throw std::runtime_error("CombineChunks failed on " + output_path + ": " +
                                         base_result.status().ToString());
            }
            base_combined = *base_result;
            const SourceCols bc = validate_source_schema(base_combined);
            base_cells = static_cast<const arrow::UInt64Array*>(
                base_combined->column(bc.cell)->chunk(0).get());
            base_dates = static_cast<const arrow::UInt16Array*>(
                base_combined->column(bc.date)->chunk(0).get());
            base_counts = static_cast<const arrow::UInt32Array*>(
                base_combined->column(bc.count)->chunk(0).get());
            base_n = base_combined->num_rows();
        }

        std::vector<MergeRow> delta;
        for (const auto& p : node_staging) collect_rows(arrow_table_io::read_table(p), delta);
        for (const auto& p : way_staging) collect_rows(arrow_table_io::read_table(p), delta);
        std::sort(delta.begin(), delta.end(), [](const MergeRow& a, const MergeRow& b) {
            return a.cell < b.cell || (a.cell == b.cell && a.day < b.day);
        });
        // Fold equal (cell, day) rows so the merge sees one entry per key, as
        // the previous hash-table accumulation did.
        size_t write = 0;
        for (size_t read = 0; read < delta.size(); ++read) {
            if (write > 0 && delta[write - 1].cell == delta[read].cell &&
                delta[write - 1].day == delta[read].day) {
                delta[write - 1].count += delta[read].count;
            } else {
                delta[write] = delta[read];
                ++write;
            }
        }
        delta.resize(write);

        arrow::UInt64Builder cell_builder;
        arrow::UInt16Builder date_builder;
        arrow::UInt32Builder count_builder;
        uint64_t total = 0;
        double center_lat = 0.0;
        double center_lon = 0.0;
        uint64_t cur_cell = 0;
        bool have_cell = false;
        double cur_lat = 0.0;
        double cur_lon = 0.0;

        size_t bi = 0;
        size_t di = 0;
        const size_t base_size = static_cast<size_t>(base_n);
        while (bi < base_size || di < delta.size()) {
            uint64_t cell;
            int32_t day;
            uint32_t count;
            if (bi < base_size && di < delta.size()) {
                const uint64_t bc = base_cells->Value(bi);
                const int32_t bd = static_cast<int32_t>(base_dates->Value(bi));
                if (bc < delta[di].cell || (bc == delta[di].cell && bd < delta[di].day)) {
                    cell = bc;
                    day = bd;
                    count = base_counts->Value(bi);
                    ++bi;
                } else if (bc == delta[di].cell && bd == delta[di].day) {
                    cell = bc;
                    day = bd;
                    count = base_counts->Value(bi) + delta[di].count;
                    ++bi;
                    ++di;
                } else {
                    cell = delta[di].cell;
                    day = delta[di].day;
                    count = delta[di].count;
                    ++di;
                }
            } else if (bi < base_size) {
                cell = base_cells->Value(bi);
                day = static_cast<int32_t>(base_dates->Value(bi));
                count = base_counts->Value(bi);
                ++bi;
            } else {
                cell = delta[di].cell;
                day = delta[di].day;
                count = delta[di].count;
                ++di;
            }

            append_merged(cell_builder, date_builder, count_builder, cell, day, count);

            // Rows arrive grouped by cell (the sort is (cell, day)), so the H3
            // centre is resolved once per cell and weighted by the cell's
            // summed count, matching compute_spatial_center over the whole year.
            if (!have_cell || cell != cur_cell) {
                const auto ll = h3_utils::cell_to_latlng(cell);
                cur_lat = ll.first;
                cur_lon = ll.second;
                cur_cell = cell;
                have_cell = true;
            }
            center_lat += static_cast<double>(count) * cur_lat;
            center_lon += static_cast<double>(count) * cur_lon;
            total += count;
        }

        if (total > 0) {
            any = true;
            sum_lon += center_lon;
            sum_lat += center_lat;
            total_weight += total;
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
        std::shared_ptr<arrow::Table> merged_table =
            arrow::Table::Make(schema, {cells, dates, counts});
        arrow_table_io::write_table(
            tmp_path, merged_table,
            arrow::KeyValueMetadata::Make({karmamap_source_seq},
                                           {std::to_string(applied_seq)}),
            change_group_rows, {"h3_cell", "change_date"});
        std::filesystem::rename(tmp_path, output_path);
        remove_staging();
    }

    if (!any) return std::nullopt;
    return std::make_pair(sum_lon / total_weight, sum_lat / total_weight);
}

}  // namespace sort_pass
