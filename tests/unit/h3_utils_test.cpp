#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>

#include "h3_utils.hpp"

namespace {

TEST(H3Utils, PackUnpackRoundTrip) {
    // Several lat/lon pairs across res 0..13: unpack(pack(cell)) must be identity.
    const double coords[][2] = {
        {0.0, 0.0},
        {37.3615593, -122.0553238},        // H3 upstream fixture
        {48.8566, 2.3522},                 // Paris
        {-33.8688, 151.2093},              // Sydney
        {71.2906, -156.7886}               // Utqiaġvik, Alaska
    };
    for (const auto& [lat, lon] : coords) {
        for (int res = 0; res <= h3_utils::kMaxPackedCellResolution; ++res) {
            const uint64_t cell = h3_utils::location_to_cell(lat, lon, res);
            const uint64_t packed = h3_utils::pack_cell(cell, res);
            EXPECT_EQ(h3_utils::unpack_cell(packed, res), cell) << "res=" << res;
        }
    }
}

TEST(H3Utils, PackCellRejectsResOutOfRange) {
    const uint64_t cell = h3_utils::location_to_cell(37.3615593, -122.0553238, 9);
    EXPECT_THROW(h3_utils::pack_cell(cell, 14), std::runtime_error);
    EXPECT_THROW(h3_utils::pack_cell(cell, -1), std::runtime_error);
}

TEST(H3Utils, LocationToCellGolden) {
    // Value produced by the pinned H3 v4.1.0 (CMakeLists.txt FetchContent tag).
    EXPECT_EQ(h3_utils::location_to_cell(37.3615593, -122.0553238, 9),
              0x89283470d93ffffULL);
}

TEST(H3Utils, TimestampToUtcDay) {
    EXPECT_EQ(h3_utils::timestamp_to_utc_day(0), 0);
    EXPECT_EQ(h3_utils::timestamp_to_utc_day(1704067200), 19723);  // 2024-01-01T00:00:00Z
    EXPECT_EQ(h3_utils::timestamp_to_utc_day(1704153599), 19723);  // 2024-01-01T23:59:59Z
    EXPECT_EQ(h3_utils::timestamp_to_utc_day(-1), 0);              // truncation toward zero
    EXPECT_EQ(h3_utils::timestamp_to_utc_day(-86400), -1);         // 1969-12-31
}

TEST(H3Utils, RequireU16DayBounds) {
    EXPECT_EQ(h3_utils::require_u16_day(0), 0);
    EXPECT_EQ(h3_utils::require_u16_day(65535), 65535);
    EXPECT_THROW(h3_utils::require_u16_day(-1), std::runtime_error);
    EXPECT_THROW(h3_utils::require_u16_day(65536), std::runtime_error);
}

TEST(H3Utils, TimestampToUtcMinute) {
    EXPECT_EQ(h3_utils::timestamp_to_utc_minute(0), 0);
    EXPECT_EQ(h3_utils::timestamp_to_utc_minute(59), 0);
    EXPECT_EQ(h3_utils::timestamp_to_utc_minute(60), 1);
    EXPECT_EQ(h3_utils::timestamp_to_utc_minute(1704067200), 28401120);  // 2024-01-01T00:00:00Z
}

TEST(H3Utils, CellToLatLngRoundTrip) {
    // The center of the cell holding a point stays within one cell radius of
    // it (res 9 radius ~175 m ~ 0.0016 deg lat); a loose 0.01 deg bound covers
    // the roundtrip without being resolution-fragile.
    const double coords[][2] = {
        {0.0, 0.0},
        {37.3615593, -122.0553238},  // H3 upstream fixture
        {48.8566, 2.3522},           // Paris
        {-33.8688, 151.2093},        // Sydney
        {28.9540973, -13.7811407},   // a sample-diff node
    };
    for (const auto& [lat, lon] : coords) {
        for (int res = 8; res <= 10; ++res) {
            const uint64_t cell = h3_utils::location_to_cell(lat, lon, res);
            const auto [clat, clon] = h3_utils::cell_to_latlng(cell);
            EXPECT_NEAR(clat, lat, 0.01) << "res=" << res << " lat";
            EXPECT_NEAR(clon, lon, 0.01) << "res=" << res << " lon";
        }
    }
}

TEST(H3Utils, HaversineKnownDistances) {
    // Same point.
    EXPECT_DOUBLE_EQ(h3_utils::haversine_m(52.5200, 13.4050, 52.5200, 13.4050), 0.0);
    // One degree along a meridian ~ 111.195 km (R = 6 371 000 m).
    EXPECT_NEAR(h3_utils::haversine_m(0.0, 0.0, 1.0, 0.0), 111195.0, 50.0);
    // Two degrees east along the equator ~ 222.389 km.
    EXPECT_NEAR(h3_utils::haversine_m(0.0, 0.0, 0.0, 2.0), 222389.0, 50.0);
    // The degenerate anti-podal case stays finite and non-negative.
    EXPECT_GT(h3_utils::haversine_m(0.0, 0.0, 0.0, 180.0), 1.0e6);
}

}  // namespace
