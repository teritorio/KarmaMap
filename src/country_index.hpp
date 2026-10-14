#pragma once

// Reads the S2 country index (country.parquet + country_ids.json) produced by
// country_s2_index.py and resolves a WGS84 coordinate to an ISO 3166-1
// alpha-2 code, or nullopt over ocean / unresolved cells.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace country_index {

class CountryIndex {
public:
    // Reads `parquet_path` (s2_cell, country_id, level) and the id -> ISO
    // lookup table at `json_path`. Throws std::runtime_error on a malformed
    // or missing file.
    static CountryIndex load(const std::string& parquet_path, const std::string& json_path);

    // The ISO code covering (lat_deg, lon_deg), or nullopt over ocean / when
    // the covering cell carries country_id 0.
    std::optional<std::string> lookup(double lat_deg, double lon_deg) const;

    bool empty() const { return rows_.empty(); }
    size_t size() const { return rows_.size(); }

private:
    struct Row {
        uint64_t range_min;
        uint64_t range_max;
        uint8_t country_id;
    };

    std::vector<Row> rows_;                 // ordered by range_min
    std::vector<std::string> id_to_iso_;    // indexed by country_id (0 = empty)
};

}  // namespace country_index
