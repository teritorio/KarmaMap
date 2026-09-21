#include <gtest/gtest.h>

#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include "options.hpp"

namespace {

// Builds a C-style argv from strings and runs parse_args.
bool parse(const std::vector<std::string>& args, Options* opts) {
    std::vector<char*> argv;
    for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
    // prog args plus the terminating null
    argv.push_back(nullptr);
    return parse_args(static_cast<int>(args.size()), argv.data(), opts);
}

TEST(Options, DefaultsPreserved) {
    Options opts;
    bool ok = parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                     "--output-dir", "out"},
                    &opts);
    ASSERT_TRUE(ok);
    EXPECT_EQ(opts.h3_resolution, 9);
    EXPECT_EQ(opts.way_batch_bytes, kDefaultWayBatchBytes);
    EXPECT_EQ(opts.change_group_rows, kDefaultChangeGroupRows);
    EXPECT_EQ(opts.indicators_group_rows, kDefaultIndicatorsGroupRows);
    EXPECT_TRUE(opts.run_node_pass);
    EXPECT_TRUE(opts.run_way_pass);
    EXPECT_TRUE(opts.run_sort_pass);
    EXPECT_TRUE(opts.run_step4);
    EXPECT_FALSE(opts.run_user_indicators);
    EXPECT_EQ(opts.incremental_cache_path, "cache.bin.last");
}

TEST(Options, UserIndicatorsFlagEnablesPass) {
    Options opts;
    bool ok = parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                     "--output-dir", "out", "--user-indicators"},
                    &opts);
    ASSERT_TRUE(ok);
    EXPECT_TRUE(opts.run_user_indicators);
}

TEST(Options, PassSelection) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--pass", "2"},
                      &opts));
    EXPECT_FALSE(opts.run_node_pass);
    EXPECT_TRUE(opts.run_way_pass);
    EXPECT_FALSE(opts.run_sort_pass);
    EXPECT_FALSE(opts.run_step4);
}

TEST(Options, PassFourOnly) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--pass", "4"},
                      &opts));
    EXPECT_FALSE(opts.run_node_pass);
    EXPECT_FALSE(opts.run_way_pass);
    EXPECT_FALSE(opts.run_sort_pass);
    EXPECT_TRUE(opts.run_step4);
}

TEST(Options, NoStep4Flag) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--no-step-4"},
                      &opts));
    EXPECT_FALSE(opts.run_step4);
}

TEST(Options, IncrementalCacheOverride) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--incremental-cache", "last.bin"},
                      &opts));
    EXPECT_EQ(opts.incremental_cache_path, "last.bin");
}

TEST(Options, MissingValueThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--way-batch-mb"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UnknownArgumentThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--nope"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, HelpReturnsFalse) {
    Options opts;
    EXPECT_FALSE(parse({"prog", "--help"}, &opts));
}

TEST(Options, UpdateUrlDefaultEmpty) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.update_url, "");
}

TEST(Options, UpdateUrlParsed) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--update-url",
                       "https://example.com/region-updates/"},
                      &opts));
    EXPECT_EQ(opts.update_url, "https://example.com/region-updates/");
}

TEST(Options, UpdateUrlMissingValueThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--update-url"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, CookieDefaultEmpty) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.cookie_path, "");
}

TEST(Options, CookieParsed) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--cookie", "jar.txt"},
                      &opts));
    EXPECT_EQ(opts.cookie_path, "jar.txt");
}

TEST(Options, CookieMissingValueThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--cookie"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, RequiredArgumentsMissingThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf"}, &opts), std::runtime_error);
}

TEST(Options, ChangeGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.change_group_rows, kDefaultChangeGroupRows);
}

TEST(Options, ChangeGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--change-group-rows", "123456"},
                      &opts));
    EXPECT_EQ(opts.change_group_rows, 123456);
}

TEST(Options, ChangeGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--change-group-rows", "500"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, IndicatorsGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.indicators_group_rows, kDefaultIndicatorsGroupRows);
}

TEST(Options, IndicatorsGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--indicators-group-rows", "1234"},
                      &opts));
    EXPECT_EQ(opts.indicators_group_rows, 1234);
}

TEST(Options, IndicatorsGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--indicators-group-rows", "999"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, ReputationGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.reputation_group_rows, kDefaultReputationGroupRows);
}

TEST(Options, ReputationGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--reputation-group-rows", "1234"},
                      &opts));
    EXPECT_EQ(opts.reputation_group_rows, 1234);
}

TEST(Options, ReputationGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--reputation-group-rows", "999"},
                       &opts),
                 std::runtime_error);
}

// ---------------------------------------------------------------------------
// --update
// ---------------------------------------------------------------------------

TEST(Options, UpdateModeWithoutUpdateUrlParses) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--update", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_TRUE(opts.update_mode);
    EXPECT_TRUE(opts.update_url.empty());  // resolved from manifest.json at runtime
}

TEST(Options, UpdateModeWithIncrementalCacheOnly) {
    Options opts;  // full history node cache not needed in update mode
    ASSERT_TRUE(parse({"prog", "--update", "--incremental-cache", "incr.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_TRUE(opts.update_mode);
    EXPECT_TRUE(opts.node_cache_path.empty());
    EXPECT_EQ(opts.incremental_cache_path, "incr.bin");
}

TEST(Options, UpdateModeWithoutCacheThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--update", "--output-dir", "out"}, &opts),
                 std::runtime_error);
}

TEST(Options, UpdateModeBare) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--update", "--update-url",
                       "https://example.com/region-updates/",
                       "--node-cache", "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_TRUE(opts.update_mode);
    EXPECT_EQ(opts.max_update_diffs, 0);  // catch up to the current state.txt
    EXPECT_TRUE(opts.input_path.empty());
}

TEST(Options, UpdateModeCountSeparateToken) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--update", "5", "--update-url",
                       "https://example.com/region-updates/",
                       "--node-cache", "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_TRUE(opts.update_mode);
    EXPECT_EQ(opts.max_update_diffs, 5);
}

TEST(Options, UpdateModeCountEquals) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--update=3", "--update-url",
                       "https://example.com/region-updates/",
                       "--node-cache", "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_TRUE(opts.update_mode);
    EXPECT_EQ(opts.max_update_diffs, 3);
}

TEST(Options, UpdateModeCountZeroIsBare) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--update", "0", "--update-url",
                       "https://example.com/region-updates/",
                       "--node-cache", "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_TRUE(opts.update_mode);
    EXPECT_EQ(opts.max_update_diffs, 0);
}

TEST(Options, UpdateModeInvalidCountThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--update", "abc", "--update-url",
                        "https://example.com/region-updates/",
                        "--node-cache", "cache.bin", "--output-dir", "out"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UpdateModeWithInputThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--update", "--update-url",
                        "https://example.com/region-updates/",
                        "--node-cache", "cache.bin", "--output-dir", "out"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UpdateModeWithPassThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--update", "--update-url",
                        "https://example.com/region-updates/",
                        "--node-cache", "cache.bin", "--output-dir", "out",
                        "--pass", "2"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, InputDownloadUrlIsFullMode) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--update-url",
                       "https://example.com/region-updates/",
                       "--node-cache", "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_FALSE(opts.update_mode);
}

}  // namespace
