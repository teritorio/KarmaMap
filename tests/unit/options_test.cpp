#include <gtest/gtest.h>

#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include "options.hpp"

namespace {

// Sets/unsets an env var for the test's scope, so a failing ASSERT midway
// cannot leak it into later tests.
class ScopedEnv {
public:
    ScopedEnv(const char* var, const char* val) : var_(var) {
        if (val) setenv(var, val, 1);
        else unsetenv(var);
    }
    ~ScopedEnv() { unsetenv(var_); }

private:
    const char* var_;
};

// Builds a C-style argv from strings and runs parse_args.
bool parse(const std::vector<std::string>& args, Options* opts) {
    std::vector<char*> argv;
    for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
    // prog args plus the terminating null
    argv.push_back(nullptr);
    return parse_args(static_cast<int>(args.size()), argv.data(), opts);
}

TEST(Options, ImportDefaultsPreserved) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.stage, Options::Stage::import);
    EXPECT_EQ(opts.input_path, "in.pbf");
    EXPECT_EQ(opts.node_cache_path, "cache.bin");
    EXPECT_EQ(opts.node_cache_last_path, "cache.bin.last");
    EXPECT_EQ(opts.h3_resolution, 9);
    EXPECT_EQ(opts.way_batch_bytes, kDefaultWayBatchBytes);
    EXPECT_EQ(opts.change_group_rows, kDefaultChangeGroupRows);
    EXPECT_EQ(opts.users_history_group_rows, kDefaultUsersHistoryGroupRows);
    EXPECT_EQ(opts.reputation_group_rows, kDefaultReputationGroupRows);
    EXPECT_TRUE(opts.run_node_pass);
    EXPECT_TRUE(opts.run_way_pass);
    EXPECT_TRUE(opts.run_sort_pass);
}

TEST(Options, ImportAloneDerivesDefaults) {
    ScopedEnv data_dir("DATA_DIR", nullptr);
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf"}, &opts));
    EXPECT_EQ(opts.output_dir, "data/output");
    EXPECT_EQ(opts.node_cache_path, "data/node_positions.cache");
    EXPECT_EQ(opts.node_cache_last_path, "data/node_positions.cache.last");
}

TEST(Options, DataDirEnvSetsDefaultOutputDir) {
    ScopedEnv data_dir("DATA_DIR", "/data");
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf"}, &opts));
    EXPECT_EQ(opts.output_dir, "/data/output");
    EXPECT_EQ(opts.node_cache_path, "/data/node_positions.cache");
    EXPECT_EQ(opts.node_cache_last_path, "/data/node_positions.cache.last");
}

TEST(Options, DataDirEnvWithTrailingSlash) {
    ScopedEnv data_dir("DATA_DIR", "/data/");
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf"}, &opts));
    EXPECT_EQ(opts.output_dir, "/data/output");
    EXPECT_EQ(opts.node_cache_path, "/data/node_positions.cache");
}

TEST(Options, ImportFileAfterFlags) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "--node-cache", "cache.bin", "in.pbf",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.input_path, "in.pbf");
}

TEST(Options, ImportMissingInputFileThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import"}, &opts), std::runtime_error);
}

TEST(Options, ImportMissingInputFileBeforeOutputThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "--output-dir", "out"}, &opts),
                 std::runtime_error);
}

TEST(Options, ImportUnexpectedArgumentThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "extra"}, &opts),
                 std::runtime_error);
}

TEST(Options, PassSelection) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--pass", "2"},
                      &opts));
    EXPECT_FALSE(opts.run_node_pass);
    EXPECT_TRUE(opts.run_way_pass);
    EXPECT_FALSE(opts.run_sort_pass);
}

TEST(Options, PassFourThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--pass", "4"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, PassInvalidThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--pass", "nope"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, MissingWayBatchValueThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--way-batch-mb"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, WayBatchOutsideRangeThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--way-batch-mb", "1",
                        "--output-dir", "out"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UnknownArgumentThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--nope"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UnknownVerbThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "sync"}, &opts), std::runtime_error);
}

TEST(Options, NoCommandThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog"}, &opts), std::runtime_error);
}

TEST(Options, HelpReturnsFalse) {
    Options opts;
    EXPECT_FALSE(parse({"prog", "--help"}, &opts));
    EXPECT_FALSE(parse({"prog", "-h"}, &opts));
    EXPECT_FALSE(parse({"prog", "help"}, &opts));
}

TEST(Options, ImportUpdateUrlParsed) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--update-url",
                       "https://example.com/region-updates/"},
                      &opts));
    EXPECT_EQ(opts.update_url, "https://example.com/region-updates/");
}

TEST(Options, UpdateUrlMissingValueThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--update-url"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, ImportAllowsUpdateUrl) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--update-url",
                       "https://example.com/region-updates/",
                       "--node-cache", "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.stage, Options::Stage::import);
}

TEST(Options, NodeCacheLastOverride) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--node-cache-last", "last.bin"},
                      &opts));
    EXPECT_EQ(opts.node_cache_last_path, "last.bin");
}

TEST(Options, CookieDefaultEmpty) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.cookie_path, "");
}

TEST(Options, CookieParsed) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--cookie", "jar.txt"},
                      &opts));
    EXPECT_EQ(opts.cookie_path, "jar.txt");
}

TEST(Options, CookieMissingValueThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--cookie"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, ChangeGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf"}, &opts));
    EXPECT_EQ(opts.change_group_rows, kDefaultChangeGroupRows);
}

TEST(Options, ChangeGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--change-group-rows", "123456"},
                      &opts));
    EXPECT_EQ(opts.change_group_rows, 123456);
}

TEST(Options, ChangeGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--change-group-rows", "500"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UsersHistoryGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf"}, &opts));
    EXPECT_EQ(opts.users_history_group_rows, kDefaultUsersHistoryGroupRows);
}

TEST(Options, UsersHistoryGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--users-history-group-rows", "1234"},
                      &opts));
    EXPECT_EQ(opts.users_history_group_rows, 1234);
}

TEST(Options, UsersHistoryGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--users-history-group-rows", "999"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, ReputationGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf"}, &opts));
    EXPECT_EQ(opts.reputation_group_rows, kDefaultReputationGroupRows);
}

TEST(Options, ReputationGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "import", "in.pbf", "--reputation-group-rows", "1234"},
                      &opts));
    EXPECT_EQ(opts.reputation_group_rows, 1234);
}

TEST(Options, ReputationGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "import", "in.pbf", "--reputation-group-rows", "999"},
                       &opts),
                 std::runtime_error);
}

// ---------------------------------------------------------------------------
// prepare-update
// ---------------------------------------------------------------------------

TEST(Options, PrepareUpdateRequiresUrlThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "prepare-update"}, &opts), std::runtime_error);
}

TEST(Options, PrepareUpdateWithUrlParses) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "prepare-update", "--update-url",
                       "https://example.com/region-updates/", "--node-cache",
                       "cache.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.stage, Options::Stage::prepare_update);
    EXPECT_EQ(opts.node_cache_last_path, "cache.bin.last");
    EXPECT_EQ(opts.max_update_diffs, 0);
}

TEST(Options, PrepareUpdateAloneDerivesDefaults) {
    ScopedEnv data_dir("DATA_DIR", nullptr);
    Options opts;
    ASSERT_TRUE(parse({"prog", "prepare-update", "--update-url",
                       "https://example.com/region-updates/"},
                      &opts));
    EXPECT_EQ(opts.output_dir, "data/output");
    EXPECT_EQ(opts.node_cache_path, "data/node_positions.cache");
    EXPECT_EQ(opts.node_cache_last_path, "data/node_positions.cache.last");
}

TEST(Options, PrepareUpdateNodeCacheLastOverride) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "prepare-update", "--update-url",
                       "https://example.com/region-updates/", "--node-cache-last",
                       "last.bin", "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.node_cache_last_path, "last.bin");
}

TEST(Options, PrepareUpdateRejectsInputThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "prepare-update", "--update-url",
                        "https://example.com/region-updates/", "in.pbf"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, PrepareUpdateRejectsPassThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "prepare-update", "--update-url",
                        "https://example.com/region-updates/", "--pass", "2"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, PrepareUpdateRejectsWayBatchThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "prepare-update", "--update-url",
                        "https://example.com/region-updates/", "--way-batch-mb", "64"},
                       &opts),
                 std::runtime_error);
}

// ---------------------------------------------------------------------------
// update
// ---------------------------------------------------------------------------

TEST(Options, UpdateBareParses) {
    ScopedEnv data_dir("DATA_DIR", nullptr);
    Options opts;
    ASSERT_TRUE(parse({"prog", "update"}, &opts));
    EXPECT_EQ(opts.stage, Options::Stage::update);
    EXPECT_TRUE(opts.update_url.empty());  // resolved from manifest.json at runtime
    EXPECT_EQ(opts.max_update_diffs, 0);   // catch up to the current state.txt
    EXPECT_TRUE(opts.input_path.empty());
    EXPECT_EQ(opts.node_cache_last_path, "data/node_positions.cache.last");
}

TEST(Options, UpdateCountSeparateToken) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "update", "5", "--update-url",
                       "https://example.com/region-updates/"},
                      &opts));
    EXPECT_EQ(opts.max_update_diffs, 5);
}

TEST(Options, UpdateCountZeroIsBare) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "update", "0", "--update-url",
                       "https://example.com/region-updates/"},
                      &opts));
    EXPECT_EQ(opts.max_update_diffs, 0);
}

TEST(Options, UpdateInvalidCountThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "update", "abc", "--update-url",
                        "https://example.com/region-updates/"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UpdateNodeCacheLastOverride) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "update", "--node-cache-last", "last.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.node_cache_last_path, "last.bin");
    EXPECT_EQ(opts.output_dir, "out");
}

TEST(Options, UpdateRejectsPassThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "update", "--update-url",
                        "https://example.com/region-updates/", "--pass", "2"},
                       &opts),
                 std::runtime_error);
}

TEST(Options, UpdateRejectsWayBatchThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "update", "--way-batch-mb", "64"}, &opts),
                 std::runtime_error);
}

TEST(Options, UpdateRejectsNodeCacheThrows) {
    // The full-history node cache is a pass 1-2 input, not used by update.
    Options opts;
    EXPECT_THROW(parse({"prog", "update", "--node-cache", "cache.bin"}, &opts),
                 std::runtime_error);
}

}  // namespace
