#pragma once

// H3 / date helpers shared by the passes and the Parquet writers.

#include <h3api.h>

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace h3_utils {

// UTC days since the Unix epoch (1970-01-01), the value stored in the
// Parquet change_date column and the mmap node-cache records.
inline int32_t timestamp_to_utc_day(int64_t epoch_seconds) {
    return static_cast<int32_t>(epoch_seconds / 86400);
}

// UTC minutes since the Unix epoch, the value stored in the vandalism
// indicator minute columns.
inline uint32_t timestamp_to_utc_minute(int64_t epoch_seconds) {
    return static_cast<uint32_t>(epoch_seconds / 60);
}

// Upper bound of the 2-byte day encoding shared by the Parquet change_date
// column and the node-cache records: 65535 days after the epoch = 2149.
inline constexpr int32_t kMaxUint16Day = 65535;

inline uint16_t require_u16_day(int32_t day) {
    // OSM history never approaches the uint16 ceiling (year 2149); fail
    // loudly rather than silently wrapping and corrupting day ordering.
    if (day < 0 || day > kMaxUint16Day) {
        throw std::runtime_error("Day out of uint16 range (0..65535)");
    }
    return static_cast<uint16_t>(day);
}

inline uint64_t location_to_cell(double lat_deg, double lon_deg, int resolution) {
    LatLng geo;
    geo.lat = degsToRads(lat_deg);
    geo.lng = degsToRads(lon_deg);
    H3Index cell;
    H3Error err = latLngToCell(&geo, resolution, &cell);
    if (err != E_SUCCESS) {
        // Should not happen for valid coordinates (osmium guarantees
        // -90..90 / -180..180 via Location::valid()).
        throw std::runtime_error("latLngToCell failed unexpectedly");
    }
    return static_cast<uint64_t>(cell);
}

// Center of an H3 cell as (lat, lon) degrees. The prior-position approximation
// behind vandalism filter 3: osc diffs carry only the new coordinates, so the
// last known position is reconstructed from the incremental cache's cell.
inline std::pair<double, double> cell_to_latlng(uint64_t cell) {
    LatLng geo;
    H3Error err = cellToLatLng(cell, &geo);
    if (err != E_SUCCESS) {
        // Should not happen for cells produced by location_to_cell.
        throw std::runtime_error("cellToLatLng failed unexpectedly");
    }
    return {radsToDegs(geo.lat), radsToDegs(geo.lng)};
}

// Great-circle distance between two coordinates in meters (haversine, mean
// Earth radius 6 371 000 m). `a` is clamped to [0, 1] so near-antipodal float
// rounding cannot push sqrt(1 - a) past zero into NaN.
inline double haversine_m(double lat1_deg, double lon1_deg, double lat2_deg, double lon2_deg) {
    constexpr double kEarthRadiusM = 6371000.0;
    const double phi1 = degsToRads(lat1_deg);
    const double phi2 = degsToRads(lat2_deg);
    const double dphi = degsToRads(lat2_deg - lat1_deg);
    const double dlambda = degsToRads(lon2_deg - lon1_deg);
    double a = std::sin(dphi / 2) * std::sin(dphi / 2) +
               std::cos(phi1) * std::cos(phi2) * std::sin(dlambda / 2) *
                   std::sin(dlambda / 2);
    if (a < 0.0) a = 0.0;
    if (a > 1.0) a = 1.0;
    return 2.0 * kEarthRadiusM * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

// 6-byte encoding of an H3 cell at resolution <= 13. In H3 v4, the base
// cell + digits r=13..1 sit in bits 6..51 (46 contiguous bits); resolutions
// 14-15 need 49+ bits and are rejected. The resolution itself does not fit
// and is re-applied from the cache header on unpack.
inline constexpr int kMaxPackedCellResolution = 13;
inline constexpr uint64_t kPackedCellMask = (1ULL << 46) - 1;  // h3 bits 6..51

inline uint64_t pack_cell(uint64_t h3, int resolution) {
    if (resolution < 0 || resolution > kMaxPackedCellResolution) {
        throw std::runtime_error(
            "H3 resolution must be 0..13 (" +
            std::to_string(resolution) + " does not fit the 6-byte cell encoding)");
    }
    return (h3 >> 6) & kPackedCellMask;
}

inline uint64_t unpack_cell(uint64_t packed, int resolution) {
    // Bits 0–5 hold the unused digit-sentinel positions (res 14 and 15),
    // always 0x3F for any valid H3 cell at resolution ≤ 13.
    return (1ULL << 59) | (static_cast<uint64_t>(resolution) << 52) |
           (packed << 6) | 0x3Full;
}

}  // namespace h3_utils
