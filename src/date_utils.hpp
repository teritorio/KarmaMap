#pragma once

// Calendar helpers built on top of days-since-epoch (UTC), as already used
// for the change_date column (see h3_utils.hpp).

#include <cstdint>

namespace date_utils {

// Converts days since 1970-01-01 (UTC) into a (year, month) pair.
// Public-domain algorithm by Howard Hinnant:
// http://howardhinnant.github.io/date_algorithms.html#civil_from_days
inline void year_month_from_day(int32_t days_since_epoch, int* year, int* month) {
    int64_t z = days_since_epoch;
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int64_t doe = z - era * 146097;                                // [0, 146096]
    const int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;  // [0, 399]
    const int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);          // [0, 365]
    const int64_t mp = (5 * doy + 2) / 153;                               // [0, 11]
    const int64_t m = mp + (mp < 10 ? 3 : -9);                            // [1, 12]
    const int64_t y = yoe + era * 400 + (m <= 2 ? 1 : 0);

    *year = static_cast<int>(y);
    *month = static_cast<int>(m);
}

}  // namespace date_utils
