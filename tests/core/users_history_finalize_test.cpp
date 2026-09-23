#include <gtest/gtest.h>

#include <arrow/api.h>

#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "options.hpp"
#include "test_helpers.hpp"
#include "users_history.hpp"
#include "vandalism.hpp"
#include "vandalism_store.hpp"

namespace {

using test_helpers::TempDir;
using test_helpers::read_parquet;

template <typename B, typename V>
void append_ok(B& builder, V&& value) {
    ASSERT_TRUE(builder.Append(std::forward<V>(value)).ok());
}
template <typename B>
void finish_ok(B& builder, std::shared_ptr<arrow::Array>* out) {
    ASSERT_TRUE(builder.Finish(out).ok());
}

struct StageRow {
    int64_t uid;
    std::string username;
    uint16_t day;
    uint32_t node_created;
    uint32_t node_modified;
    uint32_t node_deleted;
    uint32_t way_created;
    uint32_t way_modified;
    uint32_t way_deleted;
    uint32_t relation_created;
    uint32_t relation_modified;
    uint32_t relation_deleted;
    uint32_t tag_amenity;
    uint32_t tag_boundary;
    uint32_t tag_building;
    uint32_t tag_highway;
    uint32_t tag_landuse;
    uint32_t tag_leisure;
    uint32_t tag_name;
    uint32_t tag_natural;
    uint32_t tag_place;
    uint32_t tag_railway;
    uint32_t tag_sport;
    uint32_t tag_waterway;
};

// Writes one stage file with the exact schema the scan produces.
void write_stage(const std::string& path, const std::vector<StageRow>& rows) {
    arrow::Int64Builder uid;
    arrow::StringBuilder username;
    arrow::UInt16Builder day;
    std::vector<arrow::UInt32Builder*> counters;
    std::vector<std::unique_ptr<arrow::UInt32Builder>> owned_counters;
    for (size_t i = 0; i < users_history::kCounterCount; ++i) {
        owned_counters.push_back(std::make_unique<arrow::UInt32Builder>());
        counters.push_back(owned_counters.back().get());
    }

    // Non-const accessors for the counter fields in declaration order.
    uint32_t (StageRow::*members[users_history::kCounterCount]) = {
        &StageRow::node_created, &StageRow::node_modified, &StageRow::node_deleted,
        &StageRow::way_created,  &StageRow::way_modified,  &StageRow::way_deleted,
        &StageRow::relation_created,
        &StageRow::relation_modified,
        &StageRow::relation_deleted,
        &StageRow::tag_amenity,   &StageRow::tag_boundary,  &StageRow::tag_building,
        &StageRow::tag_highway,   &StageRow::tag_landuse,   &StageRow::tag_leisure,
        &StageRow::tag_name,      &StageRow::tag_natural,   &StageRow::tag_place,
        &StageRow::tag_railway,   &StageRow::tag_sport,     &StageRow::tag_waterway,
    };

    for (const auto& r : rows) {
        append_ok(uid, r.uid);
        append_ok(username, r.username);
        append_ok(day, r.day);
        for (size_t i = 0; i < users_history::kCounterCount; ++i) {
            append_ok(*counters[i], r.*members[i]);
        }
    }

    std::shared_ptr<arrow::Array> a_uid, a_user, a_day;
    std::vector<std::shared_ptr<arrow::Array>> a_counters(users_history::kCounterCount);
    finish_ok(uid, &a_uid);
    finish_ok(username, &a_user);
    finish_ok(day, &a_day);
    for (size_t i = 0; i < users_history::kCounterCount; ++i) {
        finish_ok(*counters[i], &a_counters[i]);
    }

    auto schema = arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("change_date", arrow::uint16(), false),
        arrow::field("node_created", arrow::uint32(), false),
        arrow::field("node_modified", arrow::uint32(), false),
        arrow::field("node_deleted", arrow::uint32(), false),
        arrow::field("way_created", arrow::uint32(), false),
        arrow::field("way_modified", arrow::uint32(), false),
        arrow::field("way_deleted", arrow::uint32(), false),
        arrow::field("relation_created", arrow::uint32(), false),
        arrow::field("relation_modified", arrow::uint32(), false),
        arrow::field("relation_deleted", arrow::uint32(), false),
        arrow::field("tag_amenity", arrow::uint32(), false),
        arrow::field("tag_boundary", arrow::uint32(), false),
        arrow::field("tag_building", arrow::uint32(), false),
        arrow::field("tag_highway", arrow::uint32(), false),
        arrow::field("tag_landuse", arrow::uint32(), false),
        arrow::field("tag_leisure", arrow::uint32(), false),
        arrow::field("tag_name", arrow::uint32(), false),
        arrow::field("tag_natural", arrow::uint32(), false),
        arrow::field("tag_place", arrow::uint32(), false),
        arrow::field("tag_railway", arrow::uint32(), false),
        arrow::field("tag_sport", arrow::uint32(), false),
        arrow::field("tag_waterway", arrow::uint32(), false),
    });
    std::vector<std::shared_ptr<arrow::Array>> columns = {a_uid, a_user, a_day};
    columns.insert(columns.end(), a_counters.begin(), a_counters.end());
    auto table = arrow::Table::Make(schema, columns);

    test_helpers::write_table(path, table);
}

// Reads back an output file into typed helper vectors.
struct HistoryRows {
    std::vector<int64_t> uid;
    std::vector<uint16_t> day;
    std::vector<uint32_t> count;
    std::vector<uint8_t> flag;
};

HistoryRows read_history(const std::string& path) {
    auto combined_result = read_parquet(path)->CombineChunks();
    EXPECT_TRUE(combined_result.ok()) << combined_result.status();
    if (!combined_result.ok()) return {};
    const auto& t = *combined_result;
    // uid, change_date, the day's total activity count (the six node/way
    // change counters plus the three relation counters), and the vandalism
    // flag.
    EXPECT_EQ(t->num_columns(), 4);
    HistoryRows out;
    const auto* uid = static_cast<const arrow::Int64Array*>(t->column(0)->chunk(0).get());
    const auto* day = static_cast<const arrow::UInt16Array*>(t->column(1)->chunk(0).get());
    const auto* count = static_cast<const arrow::UInt32Array*>(t->column(2)->chunk(0).get());
    const auto* flag = static_cast<const arrow::UInt8Array*>(t->column(3)->chunk(0).get());
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        out.uid.push_back(uid->Value(i));
        out.day.push_back(day->Value(i));
        out.count.push_back(count->Value(i));
        out.flag.push_back(flag->Value(i));
    }
    return out;
}

struct ReputationRows {
    std::vector<int64_t> uid;
    std::vector<std::string> username;
    std::vector<uint16_t> first_seen_day;
};

ReputationRows read_reputation(const std::string& path) {
    auto combined_result = read_parquet(path)->CombineChunks();
    EXPECT_TRUE(combined_result.ok()) << combined_result.status();
    if (!combined_result.ok()) return {};
    const auto& t = *combined_result;
    ReputationRows out;
    const auto* uid = static_cast<const arrow::Int64Array*>(t->column(0)->chunk(0).get());
    const auto* user = static_cast<const arrow::StringArray*>(t->column(1)->chunk(0).get());
    const auto* first_seen =
        static_cast<const arrow::UInt16Array*>(t->column(2)->chunk(0).get());
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        out.uid.push_back(uid->Value(i));
        out.username.push_back(user->GetString(i));
        out.first_seen_day.push_back(first_seen->Value(i));
    }
    return out;
}

TEST(UsersHistoryFinalize, ConcatenatesSortsAndDerives) {
    TempDir dir;
    const std::string stage = dir.join("stage");
    const std::string history = dir.join("users_history.parquet");

    std::filesystem::create_directories(stage);

    // Unsorted on purpose: uid 11's rows precede uid 10's in this file.
    write_stage(stage + "/stage_00000.parquet",
                {
                    {11, "bob", 2000, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {10, "alice", 1005, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {10, "alice", 1000, 5, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    // Second file: exercises multi-file concatenation.
    write_stage(stage + "/stage_00001.parquet",
                {
                    {10, "alice_alias", 1050, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
                    {12, "", 3000, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });

    users_history::run_finalize(stage, history, kDefaultUsersHistoryGroupRows,
                                kDefaultReputationGroupRows);

    EXPECT_FALSE(std::filesystem::exists(stage));
    EXPECT_TRUE(std::filesystem::exists(history));

    const auto ind = read_history(history);
    // Sorted by (uid, change_date).
    const std::vector<int64_t> exp_uid = {10, 10, 10, 11, 12};
    const std::vector<uint16_t> exp_day = {1000, 1005, 1050, 2000, 3000};
    ASSERT_EQ(ind.uid.size(), exp_uid.size());
    for (size_t i = 0; i < exp_uid.size(); ++i) {
        EXPECT_EQ(ind.uid[i], exp_uid[i]) << "row " << i;
        EXPECT_EQ(ind.day[i], exp_day[i]) << "row " << i;
    }
    // Each day's count sums the live counters that fired: the six node/way
    // change counters plus the three relation counters.
    EXPECT_EQ(ind.count[0], 8);  // uid 10 day 1000: 5 node_created + 3 relation_created
    EXPECT_EQ(ind.count[1], 6);  // uid 10 day 1005: 6 node_created
    EXPECT_EQ(ind.count[2], 2);  // uid 10 day 1050: 2 way_created
    EXPECT_EQ(ind.count[3], 4);  // uid 11 day 2000: 4 node_modified
    EXPECT_EQ(ind.count[4], 1);  // uid 12 day 3000: 1 node_created
    // Import fills the filter-1 bit from the current reputation: uid 11 (only
    // modified objects) scores 0, and uid 12 scores 4 (its node is outranked
    // by uid 10's, leaving just the building-tag cap) — both below the <5%
    // screen; uid 10 scores 64. The filter-2/3 bits stay 0 until an update
    // finalize.
    ASSERT_EQ(ind.flag.size(), 5);
    EXPECT_EQ(ind.flag[0], 0);
    EXPECT_EQ(ind.flag[1], 0);
    EXPECT_EQ(ind.flag[2], 0);
    EXPECT_EQ(ind.flag[3], vandalism::kFlagFilter1);
    EXPECT_EQ(ind.flag[4], vandalism::kFlagFilter1);

    const auto rep = read_reputation(dir.join("user_reputation.parquet"));
    // One row per uid, its current username (uid 10's "alice".."alice_alias"
    // span flattened to the last seen), sorted by username.
    ASSERT_EQ(rep.uid.size(), 3);
    EXPECT_EQ(rep.uid[0], 12);              // "<12>" sorts first
    EXPECT_EQ(rep.username[0], "<12>");
    EXPECT_EQ(rep.first_seen_day[0], 3000);

    EXPECT_EQ(rep.uid[1], 10);
    EXPECT_EQ(rep.username[1], "alice_alias");
    EXPECT_EQ(rep.first_seen_day[1], 1000);

    EXPECT_EQ(rep.uid[2], 11);
    EXPECT_EQ(rep.username[2], "bob");
    EXPECT_EQ(rep.first_seen_day[2], 2000);
}

TEST(UsersHistoryFinalize, NoStageIsNoOp) {
    TempDir dir;
    const std::string history = dir.join("users_history.parquet");

    EXPECT_NO_THROW(users_history::run_finalize(dir.join("nope"), history,
                                                kDefaultUsersHistoryGroupRows,
                                                kDefaultReputationGroupRows));
    EXPECT_FALSE(std::filesystem::exists(history));

    // An empty stage directory is equally a no-op.
    const std::string empty_stage = dir.join("empty_stage");
    std::filesystem::create_directories(empty_stage);
    EXPECT_NO_THROW(users_history::run_finalize(empty_stage, history,
                                                kDefaultUsersHistoryGroupRows,
                                                kDefaultReputationGroupRows));
    EXPECT_FALSE(std::filesystem::exists(history));
}

// Update mode: per-diff stage groups under stage_root/seq_<n>/ fold into the
// base users_history.parquet and user_reputation.parquet exactly once.
TEST(UsersHistoryFinalize, UpdateMergesDeltasIntoExistingFiles) {
    TempDir dir;
    const std::string history = dir.join("users_history.parquet");

    // Base dataset: one scan/finalize run producing the history and
    // reputation files.
    const std::string base_stage = dir.join("base_stage");
    std::filesystem::create_directories(base_stage);
    write_stage(base_stage + "/stage_00000.parquet",
                {
                    {10, "alice", 1000, 5, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0},
                    {11, "bob", 2000, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    users_history::run_finalize(base_stage, history, kDefaultUsersHistoryGroupRows,
                                kDefaultReputationGroupRows);

    // One update run: sequence 2847600 (nodes+ways) and 2847601 (ways only),
    // in separate stage groups under stage_root/seq_<n>.
    const std::string stage_root = dir.join("update_stage");
    const std::string group_a = stage_root + "/seq_2847600";
    const std::string group_b = stage_root + "/seq_2847601";
    std::filesystem::create_directories(group_a);
    std::filesystem::create_directories(group_b);
    write_stage(group_a + "/stage.parquet",
                {
                    {10, "alice", 1000, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {13, "carol", 1001, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    write_stage(group_b + "/stage.parquet",
                {
                    {11, "bob", 2001, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });

    users_history::run_update_finalize(stage_root, history,
                                       kDefaultUsersHistoryGroupRows,
                                       kDefaultReputationGroupRows,
                                         dir.join("vandalism_minutes.bin"), {});

    // Existing (uid,day) rows accumulate; new users are appended.
    const auto ind = read_history(history);
    const std::vector<int64_t> exp_uid = {10, 11, 11, 13};
    const std::vector<uint16_t> exp_day = {1000, 2000, 2001, 1001};
    ASSERT_EQ(ind.uid.size(), exp_uid.size());
    for (size_t i = 0; i < exp_uid.size(); ++i) {
        EXPECT_EQ(ind.uid[i], exp_uid[i]) << "row " << i;
        EXPECT_EQ(ind.day[i], exp_day[i]) << "row " << i;
    }
    // uid 10 day 1000: 8 from the base run (5 node_created + 3 relation_created)
    // plus 2 way_created from the diff.
    EXPECT_EQ(ind.count[0], 10);
    EXPECT_EQ(ind.count[1], 4);  // uid 11 day 2000 untouched
    EXPECT_EQ(ind.count[2], 1);  // uid 11 day 2001: 1 relation_created
    EXPECT_EQ(ind.count[3], 1);  // uid 13 day 1001: 1 node_created
    // No minute store existed, so nobody carries the filter-2 bit; the filter-1
    // bit is re-derived from the current reputation instead. uid 11 (its only
    // creation, 1 relation, ranks below uid 10's 3) and uid 13 (1 node, ranks
    // below uid 10's 5) score 0 -> flagged; uid 10 scores 56.
    ASSERT_EQ(ind.flag.size(), 4);
    EXPECT_EQ(ind.flag[0], 0);
    EXPECT_EQ(ind.flag[1], vandalism::kFlagFilter1);
    EXPECT_EQ(ind.flag[2], vandalism::kFlagFilter1);
    EXPECT_EQ(ind.flag[3], vandalism::kFlagFilter1);

    const auto rep = read_reputation(dir.join("user_reputation.parquet"));
    ASSERT_EQ(rep.uid.size(), 3);
    EXPECT_EQ(rep.uid[0], 10);
    EXPECT_EQ(rep.uid[1], 11);
    EXPECT_EQ(rep.uid[2], 13);
    EXPECT_EQ(rep.username[2], "carol");
    EXPECT_EQ(rep.first_seen_day[2], 1001);

    // Per-run staging is removed once folded in.
    EXPECT_FALSE(std::filesystem::exists(stage_root));
}

TEST(UsersHistoryFinalize, UpdateNoStageIsNoOp) {
    TempDir dir;
    const std::string history = dir.join("users_history.parquet");

    EXPECT_NO_THROW(users_history::run_update_finalize(
        dir.join("nope"), history, kDefaultUsersHistoryGroupRows,
        kDefaultReputationGroupRows, dir.join("vandalism_minutes.bin"), {}));
    EXPECT_FALSE(std::filesystem::exists(history));
}

// The vandalism filter-2 flag: with a persisted minute store present, the
// update finalize marks exactly the (uid, day) pairs that hold a minute whose
// trailing one-hour span exceeds the threshold.
TEST(UsersHistoryFinalize, UpdateFlagsVandalismDaysFromMinuteStore) {
    TempDir dir;
    const std::string history = dir.join("users_history.parquet");

    // Base dataset from import (day 1000 for uid 10, day 2000 for uid 11).
    const std::string base_stage = dir.join("base_stage");
    std::filesystem::create_directories(base_stage);
    write_stage(base_stage + "/stage_00000.parquet",
                {
                    {10, "alice", 1000, 5, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0},
                    {11, "bob", 2000, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    users_history::run_finalize(base_stage, history, kDefaultUsersHistoryGroupRows,
                                kDefaultReputationGroupRows);

    // Minute store whose (uid, minute) -> count flags:
    //   uid 10 day 1000: minute 1440*1000+30 carries 501 edits -> flagged.
    //   uid 11 day 2000: minute 1440*2000+500 carries 40 edits -> not flagged.
    //   uid 13 day 1001: minute 1440*1001+0 carries 600 edits -> flagged, and
    //   uid 13 appears in this run's diffs.
    const std::string minutes = dir.join("vandalism_minutes.bin");
    {
        vandalism_store::Writer w(minutes);
        w.add(10, 1440u * 1000u + 30, 501);
        w.add(11, 1440u * 2000u + 500, 40);
        w.add(13, 1440u * 1001u, 600);
        w.finish();
    }

    // One update run adding uid 13 on its first day.
    const std::string stage_root = dir.join("update_stage");
    std::filesystem::create_directories(stage_root + "/seq_2847600");
    write_stage(stage_root + "/seq_2847600/stage.parquet",
                {
                    {13, "carol", 1001, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });

    users_history::run_update_finalize(stage_root, history,
                                       kDefaultUsersHistoryGroupRows,
                                       kDefaultReputationGroupRows, minutes, {});

    const auto ind = read_history(history);
    // Rows sorted by (uid, change_date): uid 10 day 1000, uid 11 day 2000,
    // uid 13 day 1001.
    ASSERT_EQ(ind.uid.size(), 3);
    EXPECT_EQ(ind.uid[0], 10);
    EXPECT_EQ(ind.day[0], 1000);
    EXPECT_EQ(ind.flag[0], vandalism::kFlagFilter2);  // 501 > 500 in one hour; rep 56
    EXPECT_EQ(ind.uid[1], 11);
    EXPECT_EQ(ind.day[1], 2000);
    EXPECT_EQ(ind.flag[1], vandalism::kFlagFilter1);  // 40 < 500; rep 0
    EXPECT_EQ(ind.uid[2], 13);
    EXPECT_EQ(ind.day[2], 1001);
    EXPECT_EQ(ind.flag[2], vandalism::kFlagFilter2 | vandalism::kFlagFilter1);  // 600 > 500; rep 0
}

// The combined vandalism flag: base flags are carried forward, this run's
// filter-2 bits come from the minute store and filter-3 bits from the moves
// folding, so a day flagged by both screens carries 0x03.
TEST(UsersHistoryFinalize, UpdateFlagsCombineCarriedAndMoveDays) {
    TempDir dir;
    const std::string history = dir.join("users_history.parquet");

    // Base dataset from import for uid 10 day 1000 and uid 11 day 2000.
    const std::string base_stage = dir.join("base_stage");
    std::filesystem::create_directories(base_stage);
    write_stage(base_stage + "/stage_00000.parquet",
                {
                    {10, "alice", 1000, 5, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0},
                    {11, "bob", 2000, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    users_history::run_finalize(base_stage, history, kDefaultUsersHistoryGroupRows,
                                kDefaultReputationGroupRows);

    const std::string minutes = dir.join("vandalism_minutes.bin");
    {
        vandalism_store::Writer w(minutes);
        w.add(10, 1440u * 1000u + 30, 501);
        w.finish();
    }

    const std::string stage_root = dir.join("stage");
    const auto update_with = [&](const std::string& seq_hex, const std::string& username,
                                 uint16_t day,
                                 const std::map<std::pair<int64_t, uint16_t>, uint8_t>& move_flags,
                                 uint64_t seq) {
        const std::string stage_root = dir.join("seq_" + seq_hex);
        std::filesystem::create_directories(stage_root + "/seq_" + std::to_string(seq));
        write_stage(stage_root + "/seq_" + std::to_string(seq) + "/stage.parquet",
                    {
                        {90, username, day, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    });
        users_history::run_update_finalize(stage_root, history,
                                           kDefaultUsersHistoryGroupRows,
                                           kDefaultReputationGroupRows, minutes,
                                             move_flags);
    };

    // Run 1: uid 90 day 3000 staged; only the minute store carries the
    // filter-2 flag for uid 10 day 1000.
    update_with("1", "zoe", 3000, {}, 2847600);
    const auto ind1 = read_history(history);
    // (uid, day) sorted: uid 10 day 1000, uid 11 day 2000, uid 90 day 3000.
    ASSERT_EQ(ind1.uid.size(), 3);
    EXPECT_EQ(ind1.day[0], 1000);
    EXPECT_EQ(ind1.flag[0], vandalism::kFlagFilter2);  // rep 36, no filter 1
    EXPECT_EQ(ind1.flag[1], vandalism::kFlagFilter1);  // uid 11 scores 0
    EXPECT_EQ(ind1.flag[2], vandalism::kFlagFilter1);  // uid 90 scores 0

    // Run 2: uid 90 day 3001 staged; move-flagged existing days uid 10 day
    // 1000 (which already carries the filter-2 bit) and uid 11 day 2000.
    update_with("2", "zoe", 3001,
                std::map<std::pair<int64_t, uint16_t>, uint8_t>{
                    {{10, 1000}, vandalism::kFlagFilter3},
                    {{11, 2000}, vandalism::kFlagFilter3},
                },
                2847601);

    const auto ind = read_history(history);
    //   uid 10 day 1000: filter 2 carried + filter 3 added -> 0x03 (rep 36).
    //   uid 11 day 2000: move-flagged plus low reputation -> 0x02|0x04.
    //   uid 90 days 3000/3001: low reputation only -> 0x04.
    ASSERT_EQ(ind.uid.size(), 4);
    EXPECT_EQ(ind.day[0], 1000);
    EXPECT_EQ(ind.flag[0], vandalism::kFlagFilter2 | vandalism::kFlagFilter3);
    EXPECT_EQ(ind.uid[1], 11);
    EXPECT_EQ(ind.day[1], 2000);
    EXPECT_EQ(ind.flag[1], vandalism::kFlagFilter3 | vandalism::kFlagFilter1);
    EXPECT_EQ(ind.flag[2], vandalism::kFlagFilter1);
    EXPECT_EQ(ind.flag[3], vandalism::kFlagFilter1);
}

// The filter-1 bit is non-monotonic: every update recomputes it from the
// current reputation, masking the base rows' bit first, so a contributor whose
// standing rises loses it again and a sinking one gains it.
TEST(UsersHistoryFinalize, UpdateRecomputesFilter1FromCurrentReputation) {
    TempDir dir;
    const std::string history = dir.join("users_history.parquet");

    // Import: uid 1 creates 1 node (rep 0 -> flagged), uid 2 creates 10
    // (rep 20 -> clean).
    const std::string base_stage = dir.join("base_stage");
    std::filesystem::create_directories(base_stage);
    write_stage(base_stage + "/stage_00000.parquet",
                {
                    {1, "alice", 1000, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {2, "bob", 2000, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    users_history::run_finalize(base_stage, history, kDefaultUsersHistoryGroupRows,
                                kDefaultReputationGroupRows);
    {
        const auto ind = read_history(history);
        ASSERT_EQ(ind.flag.size(), 2);
        EXPECT_EQ(ind.flag[0], vandalism::kFlagFilter1);
        EXPECT_EQ(ind.flag[1], 0);
    }

    // Update: uid 1 creates 100 more nodes, outranking uid 2. The recomputed
    // reputation flips both users: uid 1's stale filter-1 bit is dropped
    // (non-monotonic), while uid 2's score drops to 0 and its day is flagged.
    const std::string stage_root = dir.join("update_stage");
    std::filesystem::create_directories(stage_root + "/seq_2847600");
    write_stage(stage_root + "/seq_2847600/stage.parquet",
                {
                    {1, "alice", 1001, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    users_history::run_update_finalize(stage_root, history,
                                       kDefaultUsersHistoryGroupRows,
                                       kDefaultReputationGroupRows,
                                       dir.join("vandalism_minutes.bin"), {});

    const auto ind = read_history(history);
    ASSERT_EQ(ind.uid.size(), 3);
    EXPECT_EQ(ind.uid[0], 1);  // day 1000: base bit masked, fresh rep 20 -> 0
    EXPECT_EQ(ind.day[0], 1000);
    EXPECT_EQ(ind.flag[0], 0);
    EXPECT_EQ(ind.uid[1], 1);  // day 1001
    EXPECT_EQ(ind.day[1], 1001);
    EXPECT_EQ(ind.flag[1], 0);
    EXPECT_EQ(ind.uid[2], 2);  // day 2000: fresh rep 0 -> flagged
    EXPECT_EQ(ind.day[2], 2000);
    EXPECT_EQ(ind.flag[2], vandalism::kFlagFilter1);
}

// The threshold boundary: reputation is a percentile over the contributors
// active on each aspect, so exact low scores are easy to fix. A score of 4 is
// flagged, a score of 5 (the kFilter1ReputationThreshold) is not.
TEST(UsersHistoryFinalize, Filter1FlagsReputationThresholdBoundary) {
    const auto scenario = [&](std::initializer_list<uint32_t> node_totals)
        -> std::map<int64_t, uint8_t> {
        TempDir dir;
        const std::string stage = dir.join("stage");
        const std::string history = dir.join("users_history.parquet");
        std::filesystem::create_directories(stage);
        std::vector<StageRow> rows;
        size_t i = 0;
        for (const uint32_t v : node_totals) {
            rows.push_back(StageRow{static_cast<int64_t>(i + 1),
                                    std::to_string(i + 1),
                                    static_cast<uint16_t>(100 + i),
                                    v, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                    0, 0, 0, 0, 0});
            ++i;
        }
        write_stage(stage + "/stage_00000.parquet", rows);
        users_history::run_finalize(stage, history, kDefaultUsersHistoryGroupRows,
                                    kDefaultReputationGroupRows);
        const auto ind = read_history(history);
        std::map<int64_t, uint8_t> flags;
        for (size_t r = 0; r < ind.uid.size(); ++r) flags[ind.uid[r]] |= ind.flag[r];
        return flags;
    };

    // Five contributors ranked 1..5 on node_created: uid 1 scores 0 (flagged),
    // uid 2 exactly 5, which is not "below the threshold".
    const auto f5 = scenario({1, 2, 3, 4, 5});
    EXPECT_EQ(f5.at(1), vandalism::kFlagFilter1);
    EXPECT_EQ(f5.at(2), 0);
    EXPECT_EQ(f5.at(3), 0);

    // Six contributors where the second-lowest scores exactly 4 -> flagged.
    const auto f6 = scenario({1, 2, 10, 11, 12, 13});
    EXPECT_EQ(f6.at(1), vandalism::kFlagFilter1);
    EXPECT_EQ(f6.at(2), vandalism::kFlagFilter1);
    EXPECT_EQ(f6.at(3), 0);
}

}  // namespace
