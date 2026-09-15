#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

#include "reputation.hpp"

namespace {

std::array<std::vector<uint64_t>, reputation::kAspectCount>
zero_totals(size_t n) {
    std::array<std::vector<uint64_t>, reputation::kAspectCount> totals;
    for (auto& v : totals) v.assign(n, 0);
    return totals;
}

TEST(Reputation, TiesShareRank) {
    const std::vector<int64_t> uids = {1, 2, 3, 4};
    auto totals = zero_totals(4);
    totals[0] = {5, 5, 7, 9};  // node aspect
    const reputation::Result res = reputation::compute(uids, totals);

    EXPECT_EQ(res.active[0], 4);
    EXPECT_EQ(res.max[0], 9);
    EXPECT_EQ(res.active[1], 0);  // way aspect untouched
    EXPECT_DOUBLE_EQ(res.points[0][0], 0.0);  // ties at the bottom share rank 0
    EXPECT_DOUBLE_EQ(res.points[0][1], 0.0);
    EXPECT_DOUBLE_EQ(res.pct[0][0], 0.0);
    EXPECT_DOUBLE_EQ(res.points[0][2], 20.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.pct[0][2], 100.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.points[0][3], 20.0);  // unique busiest
    EXPECT_DOUBLE_EQ(res.pct[0][3], 100.0);

    EXPECT_EQ(res.reputation[0], 0);
    EXPECT_EQ(res.reputation[1], 0);
    EXPECT_EQ(res.reputation[2], 13);  // round(20*2/3)
    EXPECT_EQ(res.reputation[3], 20);  // unique busiest on node only
}

TEST(Reputation, SoleContributorGetsFullCap) {
    const std::vector<int64_t> uids = {1, 2};
    auto totals = zero_totals(2);
    totals[0] = {4, 0};  // node
    const reputation::Result res = reputation::compute(uids, totals);

    EXPECT_EQ(res.active[0], 1);
    EXPECT_EQ(res.max[0], 4);
    EXPECT_DOUBLE_EQ(res.points[0][0], 20.0);
    EXPECT_DOUBLE_EQ(res.pct[0][0], 100.0);
    EXPECT_DOUBLE_EQ(res.points[0][1], 0.0);
    EXPECT_EQ(res.reputation[0], 20);
    EXPECT_EQ(res.reputation[1], 0);
}

TEST(Reputation, ZeroTotalsAreInactive) {
    const std::vector<int64_t> uids = {1, 2};
    const reputation::Result res = reputation::compute(uids, zero_totals(2));

    for (size_t a = 0; a < reputation::kAspectCount; ++a) {
        EXPECT_EQ(res.active[a], 0);
        EXPECT_EQ(res.max[a], 0);
        for (size_t i = 0; i < 2; ++i) {
            EXPECT_DOUBLE_EQ(res.points[a][i], 0.0);
            EXPECT_DOUBLE_EQ(res.pct[a][i], 0.0);
        }
    }
    EXPECT_EQ(res.reputation[0], 0);
    EXPECT_EQ(res.reputation[1], 0);
}

TEST(Reputation, UniqueBusiestAmongChecked) {
    const std::vector<int64_t> uids = {1, 2, 3, 4};
    auto totals = zero_totals(4);
    totals[0] = {0, 2, 2, 5};  // node: active 3
    const reputation::Result res = reputation::compute(uids, totals);

    EXPECT_EQ(res.active[0], 3);
    EXPECT_EQ(res.max[0], 5);
    EXPECT_DOUBLE_EQ(res.points[0][3], 20.0);
    EXPECT_DOUBLE_EQ(res.pct[0][3], 100.0);
    EXPECT_DOUBLE_EQ(res.points[0][1], 0.0);
    EXPECT_DOUBLE_EQ(res.points[0][2], 0.0);
    EXPECT_EQ(res.reputation[3], 20);
}

TEST(Reputation, RoundsToIntAndCapsAt100) {
    // uid 2 sits mid-rank on every aspect, so its rounded total exercises
    // rounding of fractional point sums.
    const std::vector<int64_t> uids = {1, 2, 3, 4};
    auto totals = zero_totals(4);
    for (size_t a = 0; a < 3; ++a) {  // node, way, relation
        totals[a] = {8, 7, 6, 5};
    }
    const reputation::Result res = reputation::compute(uids, totals);

    // uid 2 (7): each aspect has less = 2 of 3.
    EXPECT_DOUBLE_EQ(res.points[0][1], 20.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.points[1][1], 20.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.points[2][1], 12.0 * 2 / 3);
    EXPECT_EQ(res.reputation[1], 35);  // round(34.666...)

    // uid 1 (8): unique busiest on all three aspects.
    EXPECT_EQ(res.reputation[0], 52);
}

TEST(Reputation, FullCapsSumTo100Exactly) {
    const std::vector<int64_t> uids = {1, 2};
    auto totals = zero_totals(2);
    for (size_t a = 0; a < reputation::kAspectCount; ++a) {
        totals[a][0] = 100 + a;  // uid 1 alone is active on every aspect
    }
    const reputation::Result res = reputation::compute(uids, totals);

    EXPECT_EQ(res.reputation[0], 100);  // clamped at the cap total
    EXPECT_EQ(res.reputation[1], 0);
}

TEST(Reputation, EmptyInput) {
    const reputation::Result res =
        reputation::compute({}, std::array<std::vector<uint64_t>,
                                           reputation::kAspectCount>{});
    EXPECT_TRUE(res.reputation.empty());
    for (size_t a = 0; a < reputation::kAspectCount; ++a) {
        EXPECT_EQ(res.active[a], 0);
        EXPECT_TRUE(res.points[a].empty());
    }
}

}  // namespace