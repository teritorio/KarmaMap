#include "manifest.hpp"

#include <arrow/io/api.h>
#include <parquet/file_reader.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "date_utils.hpp"

namespace manifest {

namespace {

// Scans dataset_root/year=YYYY and returns the sorted list of "YYYY"
// partition strings actually present on disk.
std::vector<std::string> list_partitions(const std::string& dataset_root) {
    std::vector<std::string> partitions;
    if (!std::filesystem::exists(dataset_root)) return partitions;

    for (const auto& year_entry : std::filesystem::directory_iterator(dataset_root)) {
        if (!year_entry.is_directory()) continue;
        const std::string year_dir = year_entry.path().filename().string();  // "year=YYYY"
        const std::string year_prefix = "year=";
        if (year_dir.compare(0, year_prefix.size(), year_prefix) != 0) continue;
        partitions.push_back(year_dir.substr(year_prefix.size()));
    }

    std::sort(partitions.begin(), partitions.end());
    return partitions;
}

// Reads the data.parquet footer of one year partition and folds the
// change_date column's row-group min/max statistics into `bounds`. Empty
// files and row groups without statistics (no min/max written) contribute
// nothing; an unexpected schema or statistics type fails loudly.
bool read_year_bounds(const std::string& data_path,
                      std::pair<int32_t, int32_t>& bounds) {
    auto infile_result = arrow::io::ReadableFile::Open(data_path);
    if (!infile_result.ok()) {
        throw std::runtime_error("Failed to open " + data_path + " for reading: " +
                                 infile_result.status().ToString());
    }

    std::unique_ptr<parquet::ParquetFileReader> reader;
    try {
        reader = parquet::ParquetFileReader::Open(*infile_result);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to read Parquet footer of " + data_path + ": " +
                                 e.what());
    }

    const std::shared_ptr<parquet::FileMetaData> meta = reader->metadata();
    const int col_idx = meta->schema()->ColumnIndex("change_date");
    if (col_idx < 0) {
        throw std::runtime_error("Unexpected Parquet schema in " + data_path +
                                 ": missing change_date column");
    }

    bool found = false;
    for (int i = 0; i < meta->num_row_groups(); ++i) {
        std::unique_ptr<parquet::ColumnChunkMetaData> chunk =
            meta->RowGroup(i)->ColumnChunk(col_idx);
        if (!chunk) continue;
        const parquet::Statistics* stats = chunk->statistics().get();
        if (!stats || !stats->HasMinMax()) continue;

        int64_t lo = 0, hi = 0;
        switch (stats->physical_type()) {
            case parquet::Type::INT32: {
                const auto* typed =
                    static_cast<const parquet::TypedStatistics<parquet::Int32Type>*>(stats);
                lo = typed->min();
                hi = typed->max();
                break;
            }
            case parquet::Type::INT64: {
                const auto* typed =
                    static_cast<const parquet::TypedStatistics<parquet::Int64Type>*>(stats);
                lo = typed->min();
                hi = typed->max();
                break;
            }
            default:
                throw std::runtime_error("Unexpected change_date statistics type in " +
                                         data_path + ": " +
                                         parquet::TypeToString(stats->physical_type()));
        }

        if (!found) {
            bounds = {static_cast<int32_t>(lo), static_cast<int32_t>(hi)};
            found = true;
        } else {
            bounds.first = std::min(bounds.first, static_cast<int32_t>(lo));
            bounds.second = std::max(bounds.second, static_cast<int32_t>(hi));
        }
    }
    return found;
}

// Global min/max change_date across every data.parquet on disk, i.e. the
// exact date span a client can query. Nullopt when no row data exists
// (empty dataset, or staging-only years without pass 3).
std::optional<std::pair<int32_t, int32_t>> scan_date_range(
    const std::string& changes_root, const std::vector<std::string>& partitions) {
    int32_t lo = 0, hi = 0;
    bool found = false;
    for (const std::string& year : partitions) {
        const std::string data_path = changes_root + "/year=" + year + "/data.parquet";
        if (!std::filesystem::exists(data_path)) continue;
        std::pair<int32_t, int32_t> bounds;
        if (read_year_bounds(data_path, bounds)) {
            if (!found) {
                lo = bounds.first;
                hi = bounds.second;
                found = true;
            } else {
                lo = std::min(lo, bounds.first);
                hi = std::max(hi, bounds.second);
            }
        }
    }
    if (!found) return std::nullopt;
    return std::make_pair(lo, hi);
}

std::string json_string_array(const std::vector<std::string>& values) {
    std::string out = "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) out += ", ";
        out += "\"" + values[i] + "\"";
    }
    out += "]";
    return out;
}

}  // namespace

void write_manifest(const std::string& output_dir, int h3_resolution) {
    auto partitions = list_partitions(output_dir + "/changes");

    std::ofstream out(output_dir + "/manifest.json");
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open manifest.json for writing in " + output_dir);
    }

    // date_range bounds the exact queryable days, read from each
    // data.parquet footer's change_date statistics; it is omitted when no
    // row data exists (empty dataset or staging-only years).
    std::optional<std::pair<int32_t, int32_t>> bounds =
        scan_date_range(output_dir + "/changes", partitions);

    out << "{\n";
    out << "  \"h3_resolution\": " << h3_resolution << ",\n";

    if (bounds) {
        out << "  \"date_range\": { \"min_date\": \"" << date_utils::iso_date(bounds->first)
            << "\", \"max_date\": \"" << date_utils::iso_date(bounds->second) << "\" },\n";
    }

    out << "  \"datasets\": {\n";
    bool first_dataset = true;
    auto write_dataset = [&](const std::string& name, const std::string& path,
                             const std::vector<std::string>& parts) {
        if (!first_dataset) out << ",\n";
        first_dataset = false;
        out << "    \"" << name << "\": { \"path\": \"" << path << "\", \"partitions\": "
            << json_string_array(parts) << " }";
    };
    write_dataset("changes", "changes", partitions);
    // The user-indicator outputs are non-partitioned single files; the empty
    // partition list tells year-based query clients to skip them.
    if (std::filesystem::exists(output_dir + "/user_indicators.parquet")) {
        write_dataset("user_indicators", "user_indicators.parquet", {});
    }
    if (std::filesystem::exists(output_dir + "/user_reputation.parquet")) {
        write_dataset("user_reputation", "user_reputation.parquet", {});
    }
    out << "\n  }\n";
    out << "}\n";
}

}  // namespace manifest