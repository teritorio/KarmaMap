#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

#include "ranking.hpp"

namespace {

std::array<std::vector<uint64_t>, ranking::kAspectCount>
zero_totals(size_t n) {
    std::array<std::vector<uint64_t>, ranking::kAspectCount> totals;
    for (auto& v : totals) v.assign(n, 0);
    return totals;
}

TEST(Ranking, TiesShareRank) {
    const std::vector<int64_t> uids = {1, 2, 3, 4};
    auto totals = zero_totals(4);
    totals[0] = {5, 5, 7, 9};  // node aspect
    const ranking::Result res = ranking::compute(uids, totals);

    EXPECT_EQ(res.active[0], 4);
    EXPECT_EQ(res.max[0], 9);
    EXPECT_EQ(res.active[1], 0);  // way aspect untouched
    // Ties at the bottom share rank 0; the pct mirrors the tied points
    // (points = cap * pct / 100, derived client-side).
    EXPECT_DOUBLE_EQ(res.pct[0][0], 0.0);
    EXPECT_DOUBLE_EQ(res.pct[0][1], 0.0);
    EXPECT_DOUBLE_EQ(res.pct[0][2], 100.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.pct[0][3], 100.0);  // unique busiest

    EXPECT_EQ(res.ranking[0], 0);
    EXPECT_EQ(res.ranking[1], 0);
    EXPECT_EQ(res.ranking[2], 13);  // round(20*2/3)
    EXPECT_EQ(res.ranking[3], 20);  // unique busiest on node only
}

TEST(Ranking, SoleContributorGetsFullCap) {
    const std::vector<int64_t> uids = {1, 2};
    auto totals = zero_totals(2);
    totals[0] = {4, 0};  // node
    const ranking::Result res = ranking::compute(uids, totals);

    EXPECT_EQ(res.active[0], 1);
    EXPECT_EQ(res.max[0], 4);
    EXPECT_DOUBLE_EQ(res.pct[0][0], 100.0);  // sole contributor: full cap, pct 100
    EXPECT_DOUBLE_EQ(res.pct[0][1], 0.0);
    EXPECT_EQ(res.ranking[0], 20);
    EXPECT_EQ(res.ranking[1], 0);
}

TEST(Ranking, ZeroTotalsAreInactive) {
    const std::vector<int64_t> uids = {1, 2};
    const ranking::Result res = ranking::compute(uids, zero_totals(2));

    for (size_t a = 0; a < ranking::kAspectCount; ++a) {
        EXPECT_EQ(res.active[a], 0);
        EXPECT_EQ(res.max[a], 0);
        for (size_t i = 0; i < 2; ++i) {
            EXPECT_DOUBLE_EQ(res.pct[a][i], 0.0);
        }
    }
    EXPECT_EQ(res.ranking[0], 0);
    EXPECT_EQ(res.ranking[1], 0);
}

TEST(Ranking, UniqueBusiestAmongChecked) {
    const std::vector<int64_t> uids = {1, 2, 3, 4};
    auto totals = zero_totals(4);
    totals[0] = {0, 2, 2, 5};  // node: active 3
    const ranking::Result res = ranking::compute(uids, totals);

    EXPECT_EQ(res.active[0], 3);
    EXPECT_EQ(res.max[0], 5);
    EXPECT_DOUBLE_EQ(res.pct[0][3], 100.0);  // unique busiest
    EXPECT_DOUBLE_EQ(res.pct[0][1], 0.0);
    EXPECT_DOUBLE_EQ(res.pct[0][2], 0.0);
    EXPECT_EQ(res.ranking[3], 20);
}

TEST(Ranking, RoundsToIntAndCapsAt100) {
    // uid 2 sits mid-rank on every aspect, so its rounded total exercises
    // rounding of fractional point sums.
    const std::vector<int64_t> uids = {1, 2, 3, 4};
    auto totals = zero_totals(4);
    for (size_t a = 0; a < 3; ++a) {  // node, way, relation
        totals[a] = {8, 7, 6, 5};
    }
    const ranking::Result res = ranking::compute(uids, totals);

    // uid 2 (7): each aspect has less = 2 of 3.
    EXPECT_DOUBLE_EQ(res.pct[0][1], 100.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.pct[1][1], 100.0 * 2 / 3);
    EXPECT_DOUBLE_EQ(res.pct[2][1], 100.0 * 2 / 3);
    EXPECT_EQ(res.ranking[1], 35);  // round(34.666...)

    // uid 1 (8): unique busiest on all three aspects.
    EXPECT_EQ(res.ranking[0], 52);
}

TEST(Ranking, FullCapsSumTo100Exactly) {
    const std::vector<int64_t> uids = {1, 2};
    auto totals = zero_totals(2);
    for (size_t a = 0; a < ranking::kAspectCount; ++a) {
        totals[a][0] = 100 + a;  // uid 1 alone is active on every aspect
    }
    const ranking::Result res = ranking::compute(uids, totals);

    EXPECT_EQ(res.ranking[0], 100);  // clamped at the cap total
    EXPECT_EQ(res.ranking[1], 0);
}

TEST(Ranking, EmptyInput) {
    const ranking::Result res =
        ranking::compute({}, std::array<std::vector<uint64_t>,
                                        ranking::kAspectCount>{});
    EXPECT_TRUE(res.ranking.empty());
    for (size_t a = 0; a < ranking::kAspectCount; ++a) {
        EXPECT_EQ(res.active[a], 0);
        EXPECT_TRUE(res.pct[a].empty());
    }
}

}  // namespace