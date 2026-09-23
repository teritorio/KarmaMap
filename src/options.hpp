#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

inline constexpr size_t kDefaultWayBatchBytes = 512ULL * 1024 * 1024;
inline constexpr int64_t kDefaultChangeGroupRows = 10'000;
inline constexpr int64_t kDefaultUsersHistoryGroupRows = 10'000;
inline constexpr int64_t kDefaultReputationGroupRows = 1'000;

struct Options {
    enum class Stage { import, prepare_update, update };

    Stage stage = Stage::import;

    // import <planet.osh.pbf> (import stage positional).
    std::string input_path;
    // Node position cache (import/prepare-update; default <output-dir>/../node_positions.cache).
    std::string node_cache_path;
    // Last-known-position cache, <node-cache>.last by default; prepare-update
    // writes it, update reads and rebuilds it.
    std::string node_cache_last_path;
    // Diff download cache dir and persisted vandalism minute store; both live
    // next to the node caches (parent dir of the default node cache path).
    std::string diffs_dir;
    std::string vandalism_minutes_path;
    // Output directory for the Parquet datasets (default: $DATA_DIR/output,
    // i.e. data/output, or /data/output in the container where DATA_DIR=/data).
    std::string output_dir;
    std::string update_url;
    std::string cookie_path;  // empty: use <node-cache-parent>/.geofabrik.cookie
    int64_t max_update_diffs = 0;  // update [N], N>=0; 0: catch up to the current state text
    int h3_resolution = 9;
    size_t way_batch_bytes = kDefaultWayBatchBytes;
    int64_t change_group_rows = kDefaultChangeGroupRows;
    int64_t users_history_group_rows = kDefaultUsersHistoryGroupRows;
    int64_t reputation_group_rows = kDefaultReputationGroupRows;
    bool run_node_pass = true;
    bool run_way_pass = true;
    bool run_sort_pass = true;
    bool pass_given = false;      // whether --pass was passed (rejected with prepare-update/update)
    bool way_batch_mb_given = false;  // whether --way-batch-mb was passed (import only)
    bool node_cache_given = false;    // whether --node-cache was passed (update only)
};

void print_usage(const char* argv0);

// Returns false when --help/-h/help was passed (caller prints usage and exits 0).
// Throws std::runtime_error on argument errors.
bool parse_args(int argc, char** argv, Options* opts);
