#pragma once

// Compact per-aspect CDF proxy used by the users viewer to rank a
// contributor against the per-aspect distribution of created objects / Top12
// tags, without downloading the full indicator table. Each aspect's
// contributor population (count > 0) is sampled with k equal-mass windows
// (ceil(active/k) contributors each); one sample per window records the
// largest count and the number of contributors strictly below it. Reading
// the nearest sample below a count underestimates the true rank by at most
// one window: percentile error <= 100/k. Populations <= k are kept exact.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace reputation_distribution {

// Default number of CDF samples per aspect.
inline constexpr size_t kSamplesPerAspect = 1024;

// One CDF sample: `less` = contributors with a total strictly below `value`.
struct Point {
    uint64_t value;
    uint64_t less;
};

struct Distribution {
    uint64_t active = 0;
    std::vector<Point> points;

    bool empty() const { return points.empty(); }
};

// Equal-mass CDF sampling of one aspect. `totals` holds per-contributor raw
// counts; zeros are dropped. Points come out in increasing `value` order.
inline Distribution build(const std::vector<uint64_t>& totals, size_t k) {
    Distribution d;
    if (k == 0) return d;

    std::vector<uint64_t> values;
    values.reserve(totals.size());
    for (uint64_t v : totals) {
        if (v > 0) values.push_back(v);
    }
    d.active = values.size();
    if (values.empty()) return d;
    std::sort(values.begin(), values.end());

    if (values.size() <= k) {
        size_t i = 0;
        while (i < values.size()) {
            const uint64_t v = values[i];
            const size_t less = i;
            while (i < values.size() && values[i] == v) ++i;
            d.points.push_back({v, less});
        }
        return d;
    }

    const size_t window = (d.active + k - 1) / k;
    size_t start = 0;
    while (start < d.active) {
        const size_t end = std::min(d.active, start + window);
        const uint64_t v = values[end - 1];
        const auto first = std::lower_bound(values.begin(), values.end(), v);
        const size_t less = static_cast<size_t>(first - values.begin());
        if (d.points.empty() || d.points.back().value != v) {
            d.points.push_back({v, less});
        }
        start = end;
    }
    return d;
}

}  // namespace reputation_distribution