#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

inline constexpr size_t kDefaultWayBatchBytes = 512ULL * 1024 * 1024;
inline constexpr int64_t kDefaultChangeGroupRows = 10'000;
inline constexpr int64_t kDefaultIndicatorsGroupRows = 10'000;
inline constexpr int64_t kDefaultReputationGroupRows = 1'000;

struct Options {
    std::string input_path;
    std::string node_cache_path;
    std::string incremental_cache_path;
    std::string output_dir;
    std::string update_url;
    std::string cookie_path;  // empty: use <output-dir>/.geofabrik.cookie
    bool update_mode = false;
    int64_t max_update_diffs = 0;  // 0: catch up to the current state txt
    int h3_resolution = 9;
    size_t way_batch_bytes = kDefaultWayBatchBytes;
    int64_t change_group_rows = kDefaultChangeGroupRows;
    int64_t indicators_group_rows = kDefaultIndicatorsGroupRows;
    int64_t reputation_group_rows = kDefaultReputationGroupRows;
    bool run_node_pass = true;
    bool run_way_pass = true;
    bool run_sort_pass = true;
    bool run_step4 = true;
    bool run_user_indicators = false;
    bool pass_given = false;  // whether --pass was passed (rejected with --update)
};

void print_usage(const char* argv0);

// Returns false when --help/-h was passed (caller prints usage and exits 0).
// Throws std::runtime_error on argument errors.
bool parse_args(int argc, char** argv, Options* opts);
