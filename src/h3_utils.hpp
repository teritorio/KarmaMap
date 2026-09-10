#pragma once

// H3 / date helpers shared between the node pass and the way pass.

#include <h3api.h>

#include <cstdint>
#include <stdexcept>
#include <string>

namespace h3_utils {

// UTC days since the Unix epoch (1970-01-01), the value stored in the
// Parquet change_date column and the mmap node-cache records.
inline int32_t timestamp_to_utc_day(int64_t epoch_seconds) {
    return static_cast<int32_t>(epoch_seconds / 86400);
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
