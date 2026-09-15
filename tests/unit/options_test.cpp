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
    EXPECT_TRUE(opts.run_node_pass);
    EXPECT_TRUE(opts.run_way_pass);
    EXPECT_TRUE(opts.run_sort_pass);
    EXPECT_FALSE(opts.run_user_indicators);
    EXPECT_DOUBLE_EQ(opts.thresholds.relocate_meters, user_indicators::kDefaultRelocateMeters);
    EXPECT_EQ(opts.thresholds.short_life_days, user_indicators::kDefaultShortLifeDays);
    EXPECT_EQ(opts.thresholds.rapid_edit_versions, user_indicators::kDefaultRapidEditVersions);
    EXPECT_EQ(opts.thresholds.rapid_edit_window_days, user_indicators::kDefaultRapidEditWindowDays);
    EXPECT_EQ(opts.thresholds.new_user_window_days, user_indicators::kDefaultNewUserWindowDays);
    EXPECT_EQ(opts.thresholds.bulk_edit_min, user_indicators::kDefaultBulkEditMin);
}

TEST(Options, UserIndicatorsFlagEnablesPass) {
    Options opts;
    bool ok = parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                     "--output-dir", "out", "--user-indicators"},
                    &opts);
    ASSERT_TRUE(ok);
    EXPECT_TRUE(opts.run_user_indicators);
}

TEST(Options, ThresholdFlagsOverride) {
    Options opts;
    bool ok = parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                     "--output-dir", "out", "--user-indicators",
                     "--relocate-meters", "25.5", "--short-life-days", "3",
                     "--rapid-edit-versions", "9", "--rapid-edit-window-days", "14",
                     "--new-user-window-days", "60", "--bulk-edit-min", "50"},
                    &opts);
    ASSERT_TRUE(ok);
    EXPECT_DOUBLE_EQ(opts.thresholds.relocate_meters, 25.5);
    EXPECT_EQ(opts.thresholds.short_life_days, 3);
    EXPECT_EQ(opts.thresholds.rapid_edit_versions, 9);
    EXPECT_EQ(opts.thresholds.rapid_edit_window_days, 14);
    EXPECT_EQ(opts.thresholds.new_user_window_days, 60);
    EXPECT_EQ(opts.thresholds.bulk_edit_min, 50);
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
                        "--output-dir", "out", "--relocate-meters"},
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

TEST(Options, InvalidRapidEditVersionsThrows) {
    Options opts;
    EXPECT_THROW(parse({"prog", "--input", "in.pbf", "--node-cache", "cache.bin",
                        "--output-dir", "out", "--rapid-edit-versions", "1"},
                       &opts),
                 std::runtime_error);
}

}  // namespace