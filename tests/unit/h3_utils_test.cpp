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

}  // namespace