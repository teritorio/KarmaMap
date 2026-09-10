#pragma once

// H3 / date helpers shared between the node pass and the way pass.

#include <h3api.h>

#include <stdexcept>

namespace h3_utils {

// Days elapsed since epoch (UTC), consistent with arrow::date32.
inline int32_t timestamp_to_utc_day(int64_t epoch_seconds) {
    return static_cast<int32_t>(epoch_seconds / 86400);
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

}  // namespace h3_utils
