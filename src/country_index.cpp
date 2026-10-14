#include "country_index.hpp"

#include <arrow/api.h>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include "arrow_table_io.hpp"

namespace country_index {
namespace {

// Parses the flat {"<id>": "<ISO2>" | null} table written by
// country_s2_index.py. The file is machine-generated and tiny, so a tolerant
// scanner avoids pulling a JSON dependency into the build.
std::vector<std::string> parse_id_table(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("Failed to open country id table " + path);
    }
    std::ostringstream buffer;
    buffer << in.rdbuf();
    const std::string text = buffer.str();

    std::vector<std::string> table;
    size_t i = 0;
    while (i < text.size()) {
        if (text[i] != '"') {
            ++i;
            continue;
        }
        const size_t key_begin = ++i;
        while (i < text.size() && text[i] != '"') ++i;
        if (i >= text.size()) break;
        const std::string key = text.substr(key_begin, i - key_begin);
        ++i;  // closing quote
        while (i < text.size() && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n')) ++i;
        if (i >= text.size() || text[i] != ':') continue;
        ++i;
        while (i < text.size() && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n')) ++i;
        std::string value;
        if (i < text.size() && text[i] == '"') {
            const size_t value_begin = ++i;
            while (i < text.size() && text[i] != '"') ++i;
            value = text.substr(value_begin, i - value_begin);
            ++i;
        } else {
            // null (or any non-string): leave the value empty.
            while (i < text.size() && text[i] != ',' && text[i] != '}') ++i;
        }

        bool digits = !key.empty();
        for (char c : key) {
            if (!std::isdigit(static_cast<unsigned char>(c))) digits = false;
        }
        if (!digits) continue;
        const size_t id = std::stoul(key);
        if (table.size() <= id) table.resize(id + 1);
        table[id] = value;
    }
    return table;
}

uint64_t low_bit(uint64_t id) { return id & (~id + 1); }

}  // namespace

CountryIndex CountryIndex::load(const std::string& parquet_path,
                                const std::string& json_path) {
    CountryIndex index;
    index.id_to_iso_ = parse_id_table(json_path);

    auto combined_result = arrow_table_io::read_table(parquet_path)->CombineChunks();
    if (!combined_result.ok()) {
        throw std::runtime_error("Failed to combine chunks of " + parquet_path + ": " +
                                 combined_result.status().ToString());
    }
    const std::shared_ptr<arrow::Table> table = *combined_result;

    const int cell_idx = table->schema()->GetFieldIndex("s2_cell");
    const int id_idx = table->schema()->GetFieldIndex("country_id");
    if (cell_idx < 0 || id_idx < 0) {
        throw std::runtime_error("Country index " + parquet_path +
                                 " is missing s2_cell/country_id columns");
    }
    const int64_t rows = table->num_rows();
    if (rows == 0) return index;
    if (table->column(cell_idx)->num_chunks() != 1 || table->column(id_idx)->num_chunks() != 1) {
        throw std::runtime_error("Country index " + parquet_path + " has unsplit chunks");
    }

    const auto* cells =
        static_cast<const arrow::UInt64Array*>(table->column(cell_idx)->chunk(0).get());
    const auto* ids =
        static_cast<const arrow::UInt8Array*>(table->column(id_idx)->chunk(0).get());
    index.rows_.reserve(static_cast<size_t>(rows));
    for (int64_t i = 0; i < rows; ++i) {
        const uint64_t id = cells->Value(i);
        const uint64_t bit = low_bit(id);
        // A cell covers every leaf in [id - lsb + 1, id + lsb - 1].
        index.rows_.push_back({id - bit + 1, id + bit - 1, ids->Value(i)});
    }
    std::sort(index.rows_.begin(), index.rows_.end(),
              [](const Row& a, const Row& b) { return a.range_min < b.range_min; });
    return index;
}

std::optional<std::string> CountryIndex::lookup(double lat_deg, double lon_deg) const {
    if (rows_.empty()) return std::nullopt;

    const uint64_t leaf = S2CellId(S2LatLng::FromDegrees(lat_deg, lon_deg)).id();
    auto it = std::upper_bound(
        rows_.begin(), rows_.end(), leaf,
        [](uint64_t value, const Row& row) { return value < row.range_min; });
    if (it == rows_.begin()) return std::nullopt;
    --it;
    if (leaf > it->range_max) return std::nullopt;
    if (it->country_id == 0 || static_cast<size_t>(it->country_id) >= id_to_iso_.size()) {
        return std::nullopt;
    }

    const std::string& iso = id_to_iso_[it->country_id];
    if (iso.empty()) return std::nullopt;
    return iso;
}

}  // namespace country_index
