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
    EXPECT_EQ(opts.user_group_rows, kDefaultUserGroupRows);
    EXPECT_TRUE(opts.run_node_pass);
    EXPECT_TRUE(opts.run_way_pass);
    EXPECT_TRUE(opts.run_sort_pass);
    EXPECT_FALSE(opts.run_user_indicators);
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

TEST(Options, UserGroupRowsDefault) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out"},
                      &opts));
    EXPECT_EQ(opts.user_group_rows, kDefaultUserGroupRows);
}

TEST(Options, UserGroupRowsValid) {
    Options opts;
    ASSERT_TRUE(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                       "--output-dir", "out", "--user-group-rows", "1234"},
                      &opts));
    EXPECT_EQ(opts.user_group_rows, 1234);
}

TEST(Options, UserGroupRowsTooSmallThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--user-group-rows", "999"},
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

}  // namespace
