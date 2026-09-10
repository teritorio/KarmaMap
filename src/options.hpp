#pragma once

#include <cstddef>
#include <string>

inline constexpr size_t kDefaultWayBatchBytes = 512ULL * 1024 * 1024;

struct Options {
    std::string input_path;
    std::string node_cache_path;
    std::string output_dir;
    int h3_resolution = 9;
    size_t way_batch_bytes = kDefaultWayBatchBytes;
    bool run_node_pass = true;
    bool run_way_pass = true;
    bool run_sort_pass = true;
};

void print_usage(const char* argv0);

// Returns false when --help/-h was passed (caller prints usage and exits 0).
// Throws std::runtime_error on argument errors.
bool parse_args(int argc, char** argv, Options* opts);
