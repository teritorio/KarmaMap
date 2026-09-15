#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include "reputation_distribution.hpp"

namespace {

TEST(ReputationDistribution, ExactWhenPopulationAtMostK) {
    const std::vector<uint64_t> totals = {1, 1, 2, 3, 7, 0, 0};
    const auto d = reputation_distribution::build(totals, 1024);
    EXPECT_EQ(d.active, 5);
    ASSERT_EQ(d.points.size(), 4);
    EXPECT_EQ(d.points[0].value, 1);
    EXPECT_EQ(d.points[0].less, 0);
    EXPECT_EQ(d.points[1].value, 2);
    EXPECT_EQ(d.points[1].less, 2);
    EXPECT_EQ(d.points[2].value, 3);
    EXPECT_EQ(d.points[2].less, 3);
    EXPECT_EQ(d.points[3].value, 7);
    EXPECT_EQ(d.points[3].less, 4);
}

TEST(ReputationDistribution, EmptyAndZeroTotals) {
    EXPECT_TRUE(reputation_distribution::build({}, 1024).empty());
    EXPECT_TRUE(reputation_distribution::build({0, 0, 0}, 1024).empty());
    EXPECT_EQ(reputation_distribution::build({0, 5, 0}, 1024).active, 1);
}

TEST(ReputationDistribution, SortedValuesAndMonotoneLess) {
    std::mt19937 rng(42);
    std::vector<uint64_t> totals(5000);
    std::generate(totals.begin(), totals.end(),
                  [&]() { return static_cast<uint64_t>(rng() % 1000) + 1; });
    totals.push_back(0);
    const auto d = reputation_distribution::build(totals, 256);
    ASSERT_FALSE(d.points.empty());
    for (size_t i = 1; i < d.points.size(); ++i) {
        EXPECT_GT(d.points[i].value, d.points[i - 1].value);
        EXPECT_GE(d.points[i].less, d.points[i - 1].less);
    }
}

TEST(ReputationDistribution, RankErrorBoundedByOneWindow) {
    const size_t active = 5000;
    const size_t k = 64;
    std::vector<uint64_t> totals(active);
    for (size_t i = 0; i < active; ++i) {
        totals[i] = static_cast<uint64_t>(i * 2 + 1);  // strictly increasing
    }
    std::shuffle(totals.begin(), totals.end(), std::mt19937(7));
    const auto d = reputation_distribution::build(totals, k);

    const size_t window = (active + k - 1) / k;
    for (uint64_t total : totals) {
        // Lookup mirrors web/users/query.js aspect(): last sample <= total,
        // or rank 0 when the total sits below the first sample.
        size_t lo = 0, hi = d.points.size();
        while (lo < hi) {
            const size_t mid = (lo + hi) / 2;
            if (d.points[mid].value <= total) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        const uint64_t less = lo == 0 ? 0 : d.points[lo - 1].less;
        const uint64_t true_less = total / 2;  // `total` = 2*rank + 1
        EXPECT_LE(less, true_less) << "sample must not over-seat " << total;
        EXPECT_LT(true_less - less, window) << "sample underestimates rank of " << total;
    }
}

}  // namespace