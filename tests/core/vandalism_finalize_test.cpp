#include <gtest/gtest.h>

#include <arrow/api.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "h3_utils.hpp"
#include "node_cache.hpp"
#include "test_helpers.hpp"
#include "update.hpp"
#include "vandalism.hpp"
#include "vandalism_store.hpp"

namespace {

using test_helpers::TempDir;
using test_helpers::read_parquet;
using test_helpers::write_table;

// ---------------------------------------------------------------------------
// Stage file builders with the exact schemas the scan/sink produce.
// ---------------------------------------------------------------------------

struct CountsRow {
    int64_t uid;
    std::string username;
    uint32_t minute;
    uint32_t modified_deleted;
};

void write_counts_stage(const std::string& path, const std::vector<CountsRow>& rows) {
    arrow::Int64Builder uid;
    arrow::StringBuilder username;
    arrow::UInt32Builder minute;
    arrow::UInt32Builder md;
    for (const auto& r : rows) {
        ASSERT_TRUE(uid.Append(r.uid).ok());
        ASSERT_TRUE(username.Append(r.username).ok());
        ASSERT_TRUE(minute.Append(r.minute).ok());
        ASSERT_TRUE(md.Append(r.modified_deleted).ok());
    }
    std::shared_ptr<arrow::Array> a_uid, a_user, a_min, a_md;
    ASSERT_TRUE(uid.Finish(&a_uid).ok());
    ASSERT_TRUE(username.Finish(&a_user).ok());
    ASSERT_TRUE(minute.Finish(&a_min).ok());
    ASSERT_TRUE(md.Finish(&a_md).ok());
    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("uid", arrow::int64(), false),
                       arrow::field("username", arrow::utf8(), false),
                       arrow::field("minute", arrow::uint32(), false),
                       arrow::field("modified_deleted", arrow::uint32(), false)}),
        {a_uid, a_user, a_min, a_md});
    write_table(path, table);
}

struct MoveStageRow {
    int64_t uid;
    uint32_t minute;
};

void write_moves_stage(const std::string& path, const std::vector<MoveStageRow>& rows) {
    arrow::Int64Builder uid;
    arrow::UInt32Builder minute;
    for (const auto& r : rows) {
        ASSERT_TRUE(uid.Append(r.uid).ok());
        ASSERT_TRUE(minute.Append(r.minute).ok());
    }
    std::shared_ptr<arrow::Array> a_uid, a_min;
    ASSERT_TRUE(uid.Finish(&a_uid).ok());
    ASSERT_TRUE(minute.Finish(&a_min).ok());
    auto table = arrow::Table::Make(
        arrow::schema({arrow::field("uid", arrow::int64(), false),
                       arrow::field("minute", arrow::uint32(), false)}),
        {a_uid, a_min});
    write_table(path, table);
}

// ---------------------------------------------------------------------------
// Read-back helpers.
// ---------------------------------------------------------------------------

struct StoreRow {
    int64_t uid;
    uint32_t minute;
    uint32_t count;
};

std::vector<StoreRow> read_minutes(const std::string& path) {
    vandalism_store::Reader reader(path);
    std::vector<StoreRow> out;
    out.reserve(reader.size());
    for (size_t i = 0; i < reader.size(); ++i) {
        out.push_back({reader.uid_at(i), reader.minute_at(i), reader.count_at(i)});
    }
    return out;
}

std::vector<MoveStageRow> read_move_stage(const std::string& path) {
    auto table = read_parquet(path);
    EXPECT_EQ(table->num_columns(), 2);
    if (table->num_columns() != 2) return {};
    auto combined = table->CombineChunks();
    EXPECT_TRUE(combined.ok());
    if (!combined.ok()) return {};
    const auto& t = *combined;
    const auto* uid = static_cast<const arrow::Int64Array*>(t->column(0)->chunk(0).get());
    const auto* minute = static_cast<const arrow::UInt32Array*>(t->column(1)->chunk(0).get());
    std::vector<MoveStageRow> out;
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        out.push_back({uid->Value(i), minute->Value(i)});
    }
    return out;
}

// ---------------------------------------------------------------------------
// fold_minute_counts
// ---------------------------------------------------------------------------

TEST(VandalismMinuteFold, FromScratchWritesSortedMergedStore) {
    TempDir dir;
    const std::string counts_root = dir.join("stage/counts");
    const std::string minutes = dir.join("vandalism_minutes.bin");

    // Unsorted on purpose across two sequence groups.
    std::filesystem::create_directories(counts_root + "/seq_1");
    write_counts_stage(counts_root + "/seq_1/stage.parquet",
                       {{10, "alice", 1040, 200}, {10, "alice", 1000, 300}});
    std::filesystem::create_directories(counts_root + "/seq_2");
    write_counts_stage(counts_root + "/seq_2/stage.parquet",
                       {{10, "alice", 1099, 1}, {11, "bob", 2000, 501}});

    // Equal (uid, minute) keys across stage files must sum into one record.
    std::filesystem::create_directories(counts_root + "/seq_3");
    write_counts_stage(counts_root + "/seq_3/stage.parquet",
                       {{10, "alice", 1000, 50}});

    vandalism::fold_minute_counts(counts_root, minutes, 42);

    EXPECT_FALSE(std::filesystem::exists(counts_root));
    ASSERT_TRUE(std::filesystem::exists(minutes));

    // Summed per (uid, minute), (uid, minute) sorted: 1000=350, 1040=200,
    // 1099=1, and uid 11's 2000=501.
    const auto rows = read_minutes(minutes);
    const std::vector<StoreRow> exp = {{10, 1000, 350}, {10, 1040, 200},
                                       {10, 1099, 1},  {11, 2000, 501}};
    ASSERT_EQ(rows.size(), exp.size());
    for (size_t i = 0; i < exp.size(); ++i) {
        EXPECT_EQ(rows[i].uid, exp[i].uid) << "row " << i;
        EXPECT_EQ(rows[i].minute, exp[i].minute) << "row " << i;
        EXPECT_EQ(rows[i].count, exp[i].count) << "row " << i;
    }

    vandalism_store::Reader reader(minutes);
    EXPECT_EQ(reader.applied_seq(), 42);
}

TEST(VandalismMinuteFold, MergesIncomingBucketsAndSkipsReruns) {
    TempDir dir;
    const std::string counts_root = dir.join("stage/counts");
    const std::string minutes = dir.join("vandalism_minutes.bin");

    // First update run: buckets for uid 10, stamped 50.
    {
        std::filesystem::create_directories(counts_root + "/seq_1");
        write_counts_stage(counts_root + "/seq_1/stage.parquet",
                           {{10, "alice", 1000, 300}, {10, "alice", 1040, 200}});
        vandalism::fold_minute_counts(counts_root, minutes, 50);
    }
    EXPECT_EQ(read_minutes(minutes).size(), 2);

    // Incoming update run: adds edits to uid 10's minute 1000 and a new user.
    {
        std::filesystem::create_directories(counts_root + "/seq_51");
        write_counts_stage(counts_root + "/seq_51/stage.parquet",
                           {{10, "alice", 1000, 50}, {12, "dave", 3000, 600}});
        vandalism::fold_minute_counts(counts_root, minutes, 51);
    }
    EXPECT_FALSE(std::filesystem::exists(counts_root));

    const auto rows = read_minutes(minutes);
    const std::vector<StoreRow> exp = {{10, 1000, 350}, {10, 1040, 200},
                                       {12, 3000, 600}};
    ASSERT_EQ(rows.size(), exp.size());
    for (size_t i = 0; i < exp.size(); ++i) {
        EXPECT_EQ(rows[i].uid, exp[i].uid) << "row " << i;
        EXPECT_EQ(rows[i].minute, exp[i].minute) << "row " << i;
        EXPECT_EQ(rows[i].count, exp[i].count) << "row " << i;
    }
    {
        vandalism_store::Reader reader(minutes);
        EXPECT_EQ(reader.applied_seq(), 51);
    }

    // Rerun of sequence 51: the stamp >= 51 skip must leave the store
    // untouched whatever the freshly regenerated stage content says.
    {
        std::filesystem::create_directories(counts_root + "/seq_51");
        write_counts_stage(counts_root + "/seq_51/stage.parquet",
                           {{10, "alice", 1000, 50000}, {12, "dave", 3000, 90000}});
        vandalism::fold_minute_counts(counts_root, minutes, 51);
    }
    EXPECT_FALSE(std::filesystem::exists(counts_root));
    const auto after = read_minutes(minutes);
    ASSERT_EQ(after.size(), exp.size());
    EXPECT_EQ(after[0].count, 350);   // unchanged
    EXPECT_EQ(after[2].count, 600);   // unchanged
}

TEST(VandalismMinuteFold, NoStageIsNoOp) {
    TempDir dir;
    const std::string minutes = dir.join("vandalism_minutes.bin");
    EXPECT_NO_THROW(vandalism::fold_minute_counts(dir.join("nope/counts"), minutes, 1));
    EXPECT_FALSE(std::filesystem::exists(minutes));

    const std::string empty = dir.join("empty/counts");
    std::filesystem::create_directories(empty);
    EXPECT_NO_THROW(vandalism::fold_minute_counts(empty, minutes, 1));
    EXPECT_FALSE(std::filesystem::exists(minutes));
    EXPECT_FALSE(std::filesystem::exists(empty));
}

// A crashed run stamped the store through seq 50 but died before the manifest
// write. The next run re-scans seqs 1..60 and re-stages the already-folded
// seq_50 alongside the new seq_60; only the sequences beyond the stamp may be
// re-folded, so uid 10's minute 1000 is not summed twice.
TEST(VandalismMinuteFold, CrashBeforeManifestThenAdvanceDoesNotDoubleCount) {
    TempDir dir;
    const std::string counts_root = dir.join("stage/counts");
    const std::string minutes = dir.join("vandalism_minutes.bin");

    {
        std::filesystem::create_directories(counts_root + "/seq_50");
        write_counts_stage(counts_root + "/seq_50/stage.parquet",
                           {{10, "alice", 1000, 300}});
        vandalism::fold_minute_counts(counts_root, minutes, 50);
    }

    {
        // seq_50 re-staged by the rerun (already in the store), seq_60 new.
        std::filesystem::create_directories(counts_root + "/seq_50");
        write_counts_stage(counts_root + "/seq_50/stage.parquet",
                           {{10, "alice", 1000, 300}});
        std::filesystem::create_directories(counts_root + "/seq_60");
        write_counts_stage(counts_root + "/seq_60/stage.parquet",
                           {{10, "alice", 1000, 50}, {12, "dave", 3000, 600}});
        vandalism::fold_minute_counts(counts_root, minutes, 60);
    }

    EXPECT_FALSE(std::filesystem::exists(counts_root));
    const auto rows = read_minutes(minutes);
    const std::vector<StoreRow> exp = {{10, 1000, 350}, {12, 3000, 600}};
    ASSERT_EQ(rows.size(), exp.size());
    for (size_t i = 0; i < exp.size(); ++i) {
        EXPECT_EQ(rows[i].uid, exp[i].uid) << "row " << i;
        EXPECT_EQ(rows[i].minute, exp[i].minute) << "row " << i;
        EXPECT_EQ(rows[i].count, exp[i].count) << "row " << i;
    }
    {
        vandalism_store::Reader reader(minutes);
        EXPECT_EQ(reader.applied_seq(), 60);
    }
}

// ---------------------------------------------------------------------------
// flagged_days
// ---------------------------------------------------------------------------

TEST(VandalismFlaggedDays, ThresholdAcrossDays) {
    TempDir dir;
    const std::string minutes = dir.join("vandalism_minutes.bin");
    {
        // Day 0 for uid 7 at minute 0: single 501 -> flagged. Day 1 at minute
        // 1440: single 500 -> not flagged. uid 8 has minutes 2880 (400) and
        // 2882 (120): the trailing span at 2882 is 520 and day 2882/1440 = 2
        // is flagged.
        vandalism_store::Writer w(minutes);
        w.add(7, 0, 501);
        w.add(7, 1440, 500);
        w.add(8, 2880, 400);
        w.add(8, 2882, 120);
        w.finish();
    }
    const std::map<std::pair<int64_t, uint16_t>, uint8_t> flags =
        vandalism::flagged_days(minutes);
    const std::map<std::pair<int64_t, uint16_t>, uint8_t> exp_flags = {
        {{7, 0}, 1}, {{8, 2}, 1},
    };
    EXPECT_EQ(flags, exp_flags);
}

TEST(VandalismFlaggedDays, GuardsDayUint16Range) {
    TempDir dir;
    const std::string minutes = dir.join("vandalism_minutes.bin");
    {
        vandalism_store::Writer w(minutes);
        // minute / 1440 = 65536 exceeds the uint16 change_date ceiling.
        w.add(5, 65536u * 1440u, 600);
        w.finish();
    }
    EXPECT_THROW(vandalism::flagged_days(minutes), std::runtime_error);
}

TEST(VandalismFlaggedDays, MissingStoreIsEmpty) {
    TempDir dir;
    EXPECT_EQ(vandalism::flagged_days(dir.join("nope.bin")).size(), 0);
}

// ---------------------------------------------------------------------------
// flagged_move_days (filter 3 -> day-level bit-1 flags).
// ---------------------------------------------------------------------------

TEST(VandalismMoveDays, FoldsStagedMovesIntoDayFlags) {
    TempDir dir;
    const std::string stage_root = dir.join("stage");

    // One run's staged per-(uid, minute) moves (> threshold, as the sink
    // writes them), spread over two seq groups.
    {
        std::filesystem::create_directories(stage_root + "/moves/seq_1");
        write_moves_stage(stage_root + "/moves/seq_1/stage.parquet",
                          {{10, 1440}, {10, 2880}});
        std::filesystem::create_directories(stage_root + "/moves/seq_2");
        write_moves_stage(stage_root + "/moves/seq_2/stage.parquet",
                          {{10, 2881}, {12, 1440 + 5}});
    }
    const auto flags = vandalism::flagged_move_days(stage_root);
    EXPECT_FALSE(std::filesystem::exists(stage_root));

    // day = minute / 1440: uid 10's 1440 -> day 1, 2880/2881 -> day 2; uid
    // 12's 1445 falls into day 1.
    const std::map<std::pair<int64_t, uint16_t>, uint8_t> exp = {
        {{10, 1}, vandalism::kFlagFilter3},
        {{10, 2}, vandalism::kFlagFilter3},
        {{12, 1}, vandalism::kFlagFilter3},
    };
    EXPECT_EQ(flags, exp);
}

TEST(VandalismMoveDays, RerunRefoldsSameFlags) {
    TempDir dir;
    const std::string stage_root = dir.join("stage");

    const auto run = [&] {
        std::filesystem::create_directories(stage_root + "/moves/seq_1");
        write_moves_stage(stage_root + "/moves/seq_1/stage.parquet",
                          {{10, 1440 * 1000 + 30}});
        return vandalism::flagged_move_days(stage_root);
    };
    EXPECT_EQ(run(), (std::map<std::pair<int64_t, uint16_t>, uint8_t>{
        {{10, 1000}, vandalism::kFlagFilter3}}));
    // A rerun regenerates the same staged rows; flags are monotonic ORs, so
    // the result is identical.
    EXPECT_EQ(run(), (std::map<std::pair<int64_t, uint16_t>, uint8_t>{
        {{10, 1000}, vandalism::kFlagFilter3}}));
}

TEST(VandalismMoveDays, NoStageIsEmptyAndRemovesRoot) {
    TempDir dir;
    EXPECT_EQ(vandalism::flagged_move_days(dir.join("nope")).size(), 0);
    EXPECT_FALSE(std::filesystem::exists(dir.join("nope")));

    const std::string empty = dir.join("empty_stage");
    std::filesystem::create_directories(empty);
    EXPECT_EQ(vandalism::flagged_move_days(empty).size(), 0);
    EXPECT_FALSE(std::filesystem::exists(empty));

    // A counts/ subtree without moves is folded to nothing here (its minutes
    // were consumed by fold_minute_counts) and still cleaned up.
    const std::string counts = dir.join("counts_only");
    std::filesystem::create_directories(counts + "/counts/seq_1");
    EXPECT_EQ(vandalism::flagged_move_days(counts).size(), 0);
    EXPECT_FALSE(std::filesystem::exists(counts));
}

// ---------------------------------------------------------------------------
// run_scan_diff over a real (plain) .osc change file.
// ---------------------------------------------------------------------------

const char* kOsc =
    "<?xml version='1.0' encoding='UTF-8'?>\n"
    "<osmChange version=\"0.6\" generator=\"test\">\n"
    "  <create>\n"
    "    <node id=\"1\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
    " uid=\"10\" user=\"alice\" lat=\"10.0\" lon=\"20.0\"/>\n"
    "    <way id=\"1\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
    " uid=\"11\" user=\"bob\"><nd ref=\"1\"/></way>\n"
    "  </create>\n"
    "  <modify>\n"
    "    <node id=\"2\" version=\"3\" timestamp=\"2024-01-01T00:01:00Z\""
    " uid=\"10\" user=\"alice\" lat=\"10.1\" lon=\"20.1\"/>\n"
    "    <way id=\"2\" version=\"4\" timestamp=\"2024-01-01T00:01:00Z\""
    " uid=\"10\" user=\"alice\"><nd ref=\"2\"/></way>\n"
    "  </modify>\n"
    "  <delete>\n"
    "    <node id=\"3\" version=\"5\" timestamp=\"2024-01-01T00:02:00Z\""
    " uid=\"12\" user=\"carol\" lat=\"11.0\" lon=\"21.0\"/>\n"
    "    <relation id=\"1\" version=\"2\" timestamp=\"2024-01-01T00:02:00Z\""
    " uid=\"12\" user=\"carol\"/>\n"
    "  </delete>\n"
    "</osmChange>\n";

TEST(VandalismScanDiff, ClassifiesAndBuckets) {
    TempDir dir;
    const std::string osc = dir.join("diff.osc");
    {
        std::ofstream out(osc);
        out << kOsc;
    }
    const std::string stage_dir = dir.join("stage");
    EXPECT_NO_THROW(vandalism::run_scan_diff(osc, stage_dir));

    // Creates ignored; the modify (node+way) and the delete (node+relation)
    // each land in their UTC minute buckets: ts 00:01 -> minute 28401121,
    // 00:02 -> 28401122.
    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(stage_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
            files.push_back(entry.path().string());
        }
    }
    ASSERT_EQ(files.size(), 1);
    auto table = read_parquet(files[0]);
    auto combined = table->CombineChunks();
    ASSERT_TRUE(combined.ok());
    const auto& t = *combined;
    ASSERT_EQ(t->num_rows(), 2);
    const auto* uid = static_cast<const arrow::Int64Array*>(t->column(0)->chunk(0).get());
    const auto* user = static_cast<const arrow::StringArray*>(t->column(1)->chunk(0).get());
    const auto* minute = static_cast<const arrow::UInt32Array*>(t->column(2)->chunk(0).get());
    const auto* md = static_cast<const arrow::UInt32Array*>(t->column(3)->chunk(0).get());
    // The stage rows are flushed from an unordered map, so only the multiset
    // of rows is stable; sort before comparing.
    std::vector<std::tuple<int64_t, std::string, uint32_t, uint32_t>> rows;
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        rows.emplace_back(uid->Value(i), user->GetString(i), minute->Value(i),
                          md->Value(i));
    }
    std::sort(rows.begin(), rows.end());
    EXPECT_EQ(std::get<0>(rows[0]), 10);
    EXPECT_EQ(std::get<1>(rows[0]), "alice");
    EXPECT_EQ(std::get<2>(rows[0]), 28401121);
    EXPECT_EQ(std::get<3>(rows[0]), 2);  // node modify + way modify
    EXPECT_EQ(std::get<0>(rows[1]), 12);
    EXPECT_EQ(std::get<1>(rows[1]), "carol");
    EXPECT_EQ(std::get<2>(rows[1]), 28401122);
    EXPECT_EQ(std::get<3>(rows[1]), 2);  // node delete + relation delete
}

// ---------------------------------------------------------------------------
// NodeMoveSink hooked into the update node pass (filter 3).
// ---------------------------------------------------------------------------

TEST(VandalismNodeMoves, UpdateNodePassStagesOnlyFarMoves) {
    TempDir dir;
    const std::string cache = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(cache, 9);
        // Node 2 previously near the origin (res-9 cell holding (0,0)).
        w.add(2, h3_utils::location_to_cell(0.0, 0.0, 9));
        w.add(5, h3_utils::location_to_cell(0.0, 0.0, 9));
        w.finish();
    }

    // Diff: node 2 modified ~157 km north-east (far move, staged); node 5
    // modified ~150 m from the same prior cell (sub-threshold, not staged);
    // node 3 created (version 1, never a move); node 4 deleted (no new
    // position, no move).
    const std::string osc = dir.join("diff2.osc");
    {
        std::ofstream out(osc);
        out << "<osmChange version=\"0.6\" generator=\"test\">\n"
            << "  <modify>\n"
            << "    <node id=\"2\" version=\"4\" timestamp=\"2024-01-01T01:00:00Z\""
            << " uid=\"10\" user=\"alice\" lat=\"1.0\" lon=\"1.0\"/>\n"
            << "    <node id=\"5\" version=\"2\" timestamp=\"2024-01-01T01:00:00Z\""
            << " uid=\"12\" user=\"carol\" lat=\"0.001\" lon=\"0.001\"/>\n"
            << "  </modify>\n"
            << "  <create>\n"
            << "    <node id=\"3\" version=\"1\" timestamp=\"2024-01-01T01:00:00Z\""
            << " uid=\"10\" user=\"alice\" lat=\"10.1\" lon=\"20.1\"/>\n"
            << "  </create>\n"
            << "  <delete>\n"
            << "    <node id=\"4\" version=\"6\" timestamp=\"2024-01-01T01:00:00Z\""
            << " uid=\"11\" user=\"bob\" lat=\"30.0\" lon=\"30.0\"/>\n"
            << "  </delete>\n"
            << "</osmChange>\n";
    }

    const std::string changes_root = dir.join("changes");
    std::filesystem::create_directories(changes_root);
    vandalism::NodeMoveSink sink(dir.join("moves"));
    {
        update_pass::NodeState state(cache, 9);
        sink.start_seq(1);
        update_pass::run_node_update(osc, changes_root, 1, 9, &state, &sink);
        sink.finish_seq();
    }

    // Exactly one staged row: node 2's ~157 km move as (uid, minute).
    const std::vector<std::string> files = [&] {
        std::vector<std::string> out;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(
                 dir.join("moves"))) {
            if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
                out.push_back(entry.path().string());
            }
        }
        return out;
    }();
    ASSERT_EQ(files.size(), 1);
    const auto mv = read_move_stage(files[0]);
    ASSERT_EQ(mv.size(), 1);
    EXPECT_EQ(mv[0].uid, 10);
    EXPECT_EQ(mv[0].minute, 28401180);  // 2024-01-01T01:00:00Z
}

}  // namespace