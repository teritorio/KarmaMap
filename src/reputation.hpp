#pragma once

// Exact per-aspect percentile reputation over aligned per-uid totals, used by
// the --user-indicators finalize to write user_reputation.parquet (one row
// per user). Pure (no Arrow/osmium): the unit tests stay lean.
//
// Each aspect is capped at its paper weight (20/20/12 + 4 per Top12 tag) and
// scored by the user's percentile rank among the contributors active on that
// aspect (raw count > 0): a zero total scores 0, a sole contributor gets the
// full cap, and equal totals share the same rank. P(x) matches the exact
// full-table rank; nothing is sampled.

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <vector>

namespace reputation {

// Aspect layout: node(0), way(1), relation(2), then the 12 Top12 tags (in
// kTop12TagKeys order; totals[a][i] must align with uid[i]).
inline constexpr size_t kAspectCount = 15;
inline constexpr std::array<double, kAspectCount> kCaps = {
    20, 20, 12, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
};

struct Result {
    std::vector<uint8_t> reputation;                // aligned with uid (0..100)
    std::array<uint64_t, kAspectCount> active{};    // contributors with total > 0
    std::array<uint64_t, kAspectCount> max{};       // largest total, 0 when inactive
    std::array<std::vector<double>, kAspectCount> pct;  // aligned with uid (0..100)
};

inline Result compute(const std::vector<int64_t>& uid,
                      const std::array<std::vector<uint64_t>, kAspectCount>& totals) {
    Result res;
    const size_t n = uid.size();
    // Per-aspect points are cap * P, so the reputation sum accumulates inline
    // from the same rank math.
    std::vector<double> rep_sum(n, 0.0);
    for (size_t a = 0; a < kAspectCount; ++a) {
        res.pct[a].assign(n, 0.0);
    }
    res.reputation.assign(n, 0);
    if (n == 0) return res;

    std::vector<size_t> order;
    order.reserve(n);
    for (size_t a = 0; a < kAspectCount; ++a) {
        order.clear();
        uint64_t mx = 0;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t t = totals[a][i];
            if (t > 0) {
                order.push_back(i);
                if (t > mx) mx = t;
            }
        }
        res.active[a] = order.size();
        res.max[a] = mx;
        if (order.empty()) continue;

        std::sort(order.begin(), order.end(),
                  [&](size_t x, size_t y) { return totals[a][x] < totals[a][y]; });

        if (order.size() == 1) {
            const size_t i = order[0];
            rep_sum[i] += kCaps[a];
            res.pct[a][i] = 100.0;
            continue;
        }

        const double scale = 1.0 / static_cast<double>(res.active[a] - 1);
        size_t start = 0;
        while (start < order.size()) {
            const uint64_t v = totals[a][order[start]];
            size_t end = start;
            while (end < order.size() && totals[a][order[end]] == v) ++end;
            const double less = static_cast<double>(start);  // ties share this rank
            for (size_t k = start; k < end; ++k) {
                const size_t i = order[k];
                rep_sum[i] += kCaps[a] * less * scale;
                res.pct[a][i] = 100.0 * less * scale;
            }
            start = end;
        }
    }

    for (size_t i = 0; i < n; ++i) {
        res.reputation[i] =
            static_cast<uint8_t>(std::clamp(std::round(rep_sum[i]), 0.0, 100.0));
    }
    return res;
}

}  // namespace reputation