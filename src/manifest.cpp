#include "manifest.hpp"

#include <arrow/io/api.h>
#include <parquet/file_reader.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "date_utils.hpp"

namespace manifest {

namespace {

// Minimal JSON helpers over the manifest text emitted by write_manifest.
// Keys are matched at token boundaries (the character before the opening
// quote must not be a word character, underscore or quote), so a quoted
// value that merely contains the key name cannot shadow the real key. Values
// are returned long-string first.

bool is_key_boundary(const std::string& text, size_t pos) {
    if (pos == 0) return true;
    const unsigned char prev = static_cast<unsigned char>(text[pos - 1]);
    return !(std::isalnum(prev) || prev == '_' || prev == '"');
}

std::optional<std::string> json_string_value(const std::string& text, const std::string& key) {
    const std::string token = "\"" + key + "\"";
    size_t pos = 0;
    while ((pos = text.find(token, pos)) != std::string::npos) {
        if (is_key_boundary(text, pos)) {
            const size_t colon = text.find(':', pos + token.size());
            if (colon != std::string::npos) {
                const size_t start = text.find_first_not_of(" \t\r\n", colon + 1);
                if (start != std::string::npos && text[start] == '"') {
                    const size_t quote = text.find('"', start + 1);
                    if (quote != std::string::npos && quote > start) {
                        std::string value = text.substr(start + 1, quote - start - 1);
                        // Undo the escapes write_manifest emits (`\"` and
                        // `\\`); URLs and timestamps carry no other escapes.
                        size_t out = 0;
                        for (size_t i = 0; i < value.size(); ++i) {
                            if (value[i] == '\\' && i + 1 < value.size() &&
                                (value[i + 1] == '"' || value[i + 1] == '\\')) {
                                ++i;
                            }
                            value[out++] = value[i];
                        }
                        value.resize(out);
                        return value;
                    }
                }
            }
        }
        pos += token.size();
    }
    return std::nullopt;
}

std::optional<uint64_t> json_number_value(const std::string& text, const std::string& key) {
    const std::string token = "\"" + key + "\"";
    size_t pos = 0;
    while ((pos = text.find(token, pos)) != std::string::npos) {
        if (is_key_boundary(text, pos)) {
            const size_t colon = text.find(':', pos + token.size());
            if (colon != std::string::npos) {
                const size_t start = text.find_first_not_of(" \t\r\n", colon + 1);
                if (start != std::string::npos && text[start] >= '0' && text[start] <= '9') {
                    size_t end = start;
                    while (end < text.size() && text[end] >= '0' && text[end] <= '9') ++end;
                    return std::stoull(text.substr(start, end - start));
                }
            }
        }
        pos += token.size();
    }
    return std::nullopt;
}

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

// Escapes a string for use inside a JSON double-quoted value (backslash and
// quote); URL and timestamp provenance fields can carry both.
std::string json_escape(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (char c : value) {
        if (c == '"' || c == '\\') out += '\\';
        out += c;
    }
    return out;
}

// Size in bytes of a parquet file's footer metadata, read from the 8 trailing
// bytes (uint32 little-endian metadata length followed by the "PAR1" magic).
// Nullopt when the file is missing or not a plain parquet file.
std::optional<uint32_t> footer_size(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::nullopt;
    if (!in.seekg(-8, std::ios::end)) return std::nullopt;
    char tail[8];
    if (!in.read(tail, sizeof(tail))) return std::nullopt;
    if (tail[4] != 'P' || tail[5] != 'A' || tail[6] != 'R' || tail[7] != '1') {
        return std::nullopt;
    }
    uint32_t length = static_cast<uint8_t>(tail[0]);
    length |= static_cast<uint32_t>(static_cast<uint8_t>(tail[1])) << 8;
    length |= static_cast<uint32_t>(static_cast<uint8_t>(tail[2])) << 16;
    length |= static_cast<uint32_t>(static_cast<uint8_t>(tail[3])) << 24;
    return length;
}

}  // namespace

void write_manifest(const std::string& output_dir, int h3_resolution,
                    const std::optional<replication_state::State>& source) {
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

    // Source provenance: the osmosis replication state of the snapshot, always
    // present after import (sequence and timestamp from the <base>.state.txt
    // sidecar, url from --update-url or empty without it) and after update
    // (the applied sequence, with the fetched state.txt timestamp).
    // prepare-update hands over the currently recorded state with only the URL
    // replaced, so its run never overwrites the recorded sequence/timestamp.
    if (source) {
        out << "  \"source\": {\n";
        out << "    \"url\": \"" << json_escape(source->url) << "\",\n";
        out << "    \"sequence_number\": " << source->sequence_number << ",\n";
        out << "    \"timestamp\": \"" << json_escape(source->timestamp) << "\"\n";
        out << "  },\n";
    }

    if (bounds) {
        out << "  \"date_range\": { \"min_date\": \"" << date_utils::iso_date(bounds->first)
            << "\", \"max_date\": \"" << date_utils::iso_date(bounds->second) << "\" },\n";
    }

    out << "  \"datasets\": {\n";
    bool first_dataset = true;
    auto write_dataset = [&](const std::string& name, const std::string& path,
                             const std::vector<std::string>& parts,
                             const std::string& footer_field) {
        if (!first_dataset) out << ",\n";
        first_dataset = false;
        out << "    \"" << name << "\": { \"path\": \"" << path << "\", \"partitions\": "
            << json_string_array(parts);
        if (!footer_field.empty()) out << ", " << footer_field;
        out << " }";
    };

    // Per-year parquet footer metadata sizes, so year-based query clients can
    // fetch exactly the footer bytes instead of the trailing 512 KB tail
    // window. Years whose data.parquet is missing or unreadable are omitted.
    std::string partition_footer_sizes;
    {
        std::string entries;
        for (const std::string& year : partitions) {
            auto size = footer_size(output_dir + "/changes/year=" + year + "/data.parquet");
            if (!size) continue;
            if (!entries.empty()) entries += ", ";
            entries += "\"" + year + "\": " + std::to_string(*size);
        }
        if (!entries.empty()) {
            partition_footer_sizes = "\"partition_footer_sizes\": { " + entries + " }";
        }
    }
    write_dataset("changes", "changes", partitions, partition_footer_sizes);

    // The users-history outputs are non-partitioned single files; the empty
    // partition list tells year-based query clients to skip them. footer_size
    // lets the users viewer read the exact footer window of these files.
    auto single_file_field = [&](const std::string& path) -> std::string {
        auto size = footer_size(output_dir + "/" + path);
        return size ? "\"footer_size\": " + std::to_string(*size) : std::string();
    };
    if (std::filesystem::exists(output_dir + "/users_history.parquet")) {
        write_dataset("users_history", "users_history.parquet", {},
                      single_file_field("users_history.parquet"));
    }
    if (std::filesystem::exists(output_dir + "/user_reputation.parquet")) {
        write_dataset("user_reputation", "user_reputation.parquet", {},
                      single_file_field("user_reputation.parquet"));
    }
    if (std::filesystem::exists(output_dir + "/vandalism.parquet")) {
        write_dataset("vandalism", "vandalism.parquet", {},
                      single_file_field("vandalism.parquet"));
    }
    out << "\n  }\n";
    out << "}\n";
}

std::optional<replication_state::State> read_source(const std::string& output_dir) {
    std::ifstream in(output_dir + "/manifest.json");
    if (!in) return std::nullopt;
    std::ostringstream ss;
    ss << in.rdbuf();
    const std::string text = ss.str();

    // Bound the key lookups to the "source" object: write_manifest emits it
    // with exactly one level of nesting (no objects inside), so its balanced
    // { } range is easy to carve out.
    const std::string token = "\"source\"";
    size_t pos = text.find(token);
    while (pos != std::string::npos && !is_key_boundary(text, pos)) {
        pos = text.find(token, pos + token.size());
    }
    if (pos == std::string::npos) return std::nullopt;

    const size_t open = text.find('{', pos + token.size());
    if (open == std::string::npos) return std::nullopt;
    size_t depth = 1;
    size_t end = open;
    while (depth > 0 && ++end < text.size()) {
        if (text[end] == '{') ++depth;
        else if (text[end] == '}') --depth;
    }
    if (depth != 0) return std::nullopt;
    const std::string block = text.substr(open, end - open);

    const auto url = json_string_value(block, "url");
    const auto seq = json_number_value(block, "sequence_number");
    if (!url || !seq) return std::nullopt;

    replication_state::State state;
    state.url = replication_state::normalize_update_url(*url);
    state.sequence_number = *seq;
    const auto ts = json_string_value(block, "timestamp");
    state.timestamp = ts.value_or("");
    return state;
}

}  // namespace manifest
