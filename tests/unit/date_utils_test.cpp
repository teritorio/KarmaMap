#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "date_utils.hpp"

namespace {

using YearMonth = std::pair<int, int>;

TEST(DateUtils, YearMonthFromDayGoldens) {
    // days -> (year, month), golden table including leap-year and negative days.
    const std::pair<int32_t, YearMonth> table[] = {
        {0, {1970, 1}},
        {30, {1970, 1}},
        {31, {1970, 2}},    // 1970-02-01
        {58, {1970, 2}},    // 1970-02-28 (last day of non-leap Feb)
        {59, {1970, 3}},    // 1970-03-01
        {365, {1971, 1}},   // 1971-01-01
        {19723, {2024, 1}}, // 2024-01-01
        {19753, {2024, 1}}, // 2024-01-31
        {19754, {2024, 2}}, // 2024-02-01 (leap year)
        {19782, {2024, 2}}, // 2024-02-29
        {19783, {2024, 3}}, // 2024-03-01
        {-1, {1969, 12}},
        {-31, {1969, 12}},  // 1969-12-01
        {-32, {1969, 11}},  // 1969-11-30
        {-334, {1969, 2}},  // 1969-02-01
        {-307, {1969, 2}},  // 1969-02-28 (last day of non-leap Feb)
        {-306, {1969, 3}},  // 1969-03-01
        {-365, {1969, 1}}, // 1969-01-01
        {-336, {1969, 1}}, // 1969-01-30
        {-366, {1968, 12}}, // 1968-12-31
    };
    for (const auto& [days, expected] : table) {
        int year = 0, month = 0;
        date_utils::year_month_from_day(days, &year, &month);
        EXPECT_EQ((YearMonth{year, month}), expected) << "days=" << days;
    }
}

TEST(DateUtils, IsoDateGoldens) {
    // days -> "YYYY-MM-DD", golden table including leap years and negatives.
    const std::pair<int32_t, std::string> table[] = {
        {0, "1970-01-01"},
        {30, "1970-01-31"},
        {31, "1970-02-01"},
        {58, "1970-02-28"},
        {59, "1970-03-01"},
        {364, "1970-12-31"},
        {365, "1971-01-01"},
        {19723, "2024-01-01"},
        {19753, "2024-01-31"},
        {19754, "2024-02-01"},
        {19782, "2024-02-29"},  // leap day
        {19783, "2024-03-01"},
        {20088, "2024-12-31"},
        {20089, "2025-01-01"},
        {-1, "1969-12-31"},
        {-31, "1969-12-01"},
        {-32, "1969-11-30"},
        {-34, "1969-11-28"},
    };
    for (const auto& [days, expected] : table) {
        EXPECT_EQ(date_utils::iso_date(days), expected) << "days=" << days;
    }
}

}  // namespace