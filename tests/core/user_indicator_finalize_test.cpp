#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "test_helpers.hpp"
#include "user_indicators.hpp"

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
    uint32_t relocated;
    uint32_t short_lived;
    uint32_t rapid_edit;
    uint32_t relation_created;
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
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(outfile_result.ok()) << outfile_result.status();

    arrow::Int64Builder uid;
    arrow::StringBuilder username;
    arrow::UInt16Builder day;
    std::vector<arrow::UInt32Builder*> counters;
    std::vector<std::unique_ptr<arrow::UInt32Builder>> owned_counters;
    for (size_t i = 0; i < 22; ++i) {
        owned_counters.push_back(std::make_unique<arrow::UInt32Builder>());
        counters.push_back(owned_counters.back().get());
    }

    // Non-const accessors for the counter fields in declaration order.
    uint32_t (StageRow::*members[22]) = {
        &StageRow::node_created, &StageRow::node_modified, &StageRow::node_deleted,
        &StageRow::way_created,  &StageRow::way_modified,  &StageRow::way_deleted,
        &StageRow::relocated,    &StageRow::short_lived,   &StageRow::rapid_edit,
        &StageRow::relation_created,
        &StageRow::tag_amenity,   &StageRow::tag_boundary,  &StageRow::tag_building,
        &StageRow::tag_highway,   &StageRow::tag_landuse,   &StageRow::tag_leisure,
        &StageRow::tag_name,      &StageRow::tag_natural,   &StageRow::tag_place,
        &StageRow::tag_railway,   &StageRow::tag_sport,     &StageRow::tag_waterway,
    };

    for (const auto& r : rows) {
        append_ok(uid, r.uid);
        append_ok(username, r.username);
        append_ok(day, r.day);
        for (size_t i = 0; i < 22; ++i) append_ok(*counters[i], r.*members[i]);
    }

    std::shared_ptr<arrow::Array> a_uid, a_user, a_day;
    std::vector<std::shared_ptr<arrow::Array>> a_counters(22);
    finish_ok(uid, &a_uid);
    finish_ok(username, &a_user);
    finish_ok(day, &a_day);
    for (size_t i = 0; i < 22; ++i) finish_ok(*counters[i], &a_counters[i]);

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
        arrow::field("relocated", arrow::uint32(), false),
        arrow::field("short_lived", arrow::uint32(), false),
        arrow::field("rapid_edit", arrow::uint32(), false),
        arrow::field("relation_created", arrow::uint32(), false),
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

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::ZSTD);
    auto write_status =
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outfile_result,
                                   /*chunk_size=*/table->num_rows(), props_builder.build());
    ASSERT_TRUE(write_status.ok()) << write_status.ToString();
    ASSERT_TRUE((*outfile_result)->Close().ok());
}

// Reads back an output file into typed helper vectors.
struct IndicatorRows {
    std::vector<int64_t> uid;
    std::vector<uint16_t> day;
    std::vector<uint32_t> node_created;
    std::vector<uint32_t> relocated;
    std::vector<uint32_t> rapid_edit;
    std::vector<uint32_t> relation_created;
    std::vector<uint32_t> tag_amenity;
    std::vector<uint32_t> tag_building;
    std::vector<uint32_t> tag_highway;
    std::vector<uint32_t> tag_waterway;
};

IndicatorRows read_indicators(const std::string& path) {
    auto combined_result = read_parquet(path)->CombineChunks();
    EXPECT_TRUE(combined_result.ok()) << combined_result.status();
    if (!combined_result.ok()) return {};
    const auto& t = *combined_result;
    IndicatorRows out;
    const auto* uid = static_cast<const arrow::Int64Array*>(t->column(0)->chunk(0).get());
    const auto* day = static_cast<const arrow::UInt16Array*>(t->column(1)->chunk(0).get());
    const auto* node_created =
        static_cast<const arrow::UInt32Array*>(t->column(2)->chunk(0).get());
    const auto* relocated =
        static_cast<const arrow::UInt32Array*>(t->column(8)->chunk(0).get());
    const auto* rapid_edit =
        static_cast<const arrow::UInt32Array*>(t->column(10)->chunk(0).get());
    const auto* relation_created =
        static_cast<const arrow::UInt32Array*>(t->column(11)->chunk(0).get());
    const auto* tag_amenity =
        static_cast<const arrow::UInt32Array*>(t->column(12)->chunk(0).get());
    const auto* tag_building =
        static_cast<const arrow::UInt32Array*>(t->column(14)->chunk(0).get());
    const auto* tag_highway =
        static_cast<const arrow::UInt32Array*>(t->column(15)->chunk(0).get());
    const auto* tag_waterway =
        static_cast<const arrow::UInt32Array*>(t->column(23)->chunk(0).get());
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        out.uid.push_back(uid->Value(i));
        out.day.push_back(day->Value(i));
        out.node_created.push_back(node_created->Value(i));
        out.relocated.push_back(relocated->Value(i));
        out.rapid_edit.push_back(rapid_edit->Value(i));
        out.relation_created.push_back(relation_created->Value(i));
        out.tag_amenity.push_back(tag_amenity->Value(i));
        out.tag_building.push_back(tag_building->Value(i));
        out.tag_highway.push_back(tag_highway->Value(i));
        out.tag_waterway.push_back(tag_waterway->Value(i));
    }
    return out;
}

struct ProfileRows {
    std::vector<int64_t> uid;
    std::vector<std::string> username;
    std::vector<uint16_t> first_edit_day;
    std::vector<uint16_t> first_seen_day;
    std::vector<bool> bulk_new_user;
};

ProfileRows read_profiles(const std::string& path) {
    auto combined_result = read_parquet(path)->CombineChunks();
    EXPECT_TRUE(combined_result.ok()) << combined_result.status();
    if (!combined_result.ok()) return {};
    const auto& t = *combined_result;
    ProfileRows out;
    const auto* uid = static_cast<const arrow::Int64Array*>(t->column(0)->chunk(0).get());
    const auto* user = static_cast<const arrow::StringArray*>(t->column(1)->chunk(0).get());
    const auto* first_edit =
        static_cast<const arrow::UInt16Array*>(t->column(2)->chunk(0).get());
    const auto* first_seen =
        static_cast<const arrow::UInt16Array*>(t->column(3)->chunk(0).get());
    const auto* bulk = static_cast<const arrow::BooleanArray*>(t->column(4)->chunk(0).get());
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        out.uid.push_back(uid->Value(i));
        out.username.push_back(user->GetString(i));
        out.first_edit_day.push_back(first_edit->Value(i));
        out.first_seen_day.push_back(first_seen->Value(i));
        out.bulk_new_user.push_back(bulk->Value(i));
    }
    return out;
}

TEST(UserIndicatorFinalize, ConcatenatesSortsAndDerives) {
    TempDir dir;
    const std::string stage = dir.join("stage");
    const std::string profiles = dir.join("user_profiles.parquet");
    const std::string indicators = dir.join("user_indicators.parquet");

    std::filesystem::create_directories(stage);

    // Unsorted on purpose: uid 11's rows precede uid 10's in this file.
    write_stage(stage + "/stage_00000.parquet",
                {
                    {11, "bob", 2000, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {10, "alice", 1005, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {10, "alice", 1000, 5, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });
    // Second file: exercises multi-file concatenation.
    write_stage(stage + "/stage_00001.parquet",
                {
                    {10, "alice_alias", 1050, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
                    {12, "", 3000, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
                });

    user_indicators::Thresholds thresholds;  // defaults: window 30, bulk 10
    user_indicators::run_finalize(stage, profiles, indicators, thresholds);

    EXPECT_FALSE(std::filesystem::exists(stage));
    EXPECT_TRUE(std::filesystem::exists(profiles));
    EXPECT_TRUE(std::filesystem::exists(indicators));

    const auto ind = read_indicators(indicators);
    // Sorted by (uid, change_date).
    const std::vector<int64_t> exp_uid = {10, 10, 10, 11, 12};
    const std::vector<uint16_t> exp_day = {1000, 1005, 1050, 2000, 3000};
    ASSERT_EQ(ind.uid.size(), exp_uid.size());
    for (size_t i = 0; i < exp_uid.size(); ++i) {
        EXPECT_EQ(ind.uid[i], exp_uid[i]) << "row " << i;
        EXPECT_EQ(ind.day[i], exp_day[i]) << "row " << i;
    }
    // Counters survived per (uid, day).
    EXPECT_EQ(ind.node_created[0], 5);
    EXPECT_EQ(ind.node_created[1], 6);
    EXPECT_EQ(ind.node_created[4], 1);  // uid 12
    EXPECT_EQ(ind.rapid_edit[0], 1);    // uid 10 day 1000, padded in the staging row
    EXPECT_EQ(ind.relocated[0], 0);
    EXPECT_EQ(ind.relation_created[0], 3);       // uid 10 day 1000
    EXPECT_EQ(ind.relation_created[1], 0);       // uid 10 day 1005
    EXPECT_EQ(ind.relation_created[4], 0);       // uid 12
    // Top12 tag counters survive per (uid, day).
    EXPECT_EQ(ind.tag_amenity[0], 0);
    EXPECT_EQ(ind.tag_building[0], 2);       // uid 10 day 1000
    EXPECT_EQ(ind.tag_waterway[2], 1);       // uid 10 day 1050
    EXPECT_EQ(ind.tag_highway[4], 1);        // uid 12
    EXPECT_EQ(ind.tag_waterway[4], 0);

    const auto prof = read_profiles(profiles);
    // alice (bulk: 5+6 = 11 events within window >= 10), then alice_alias,
    // bob, and the anonymous uid facing "<12>".
    ASSERT_EQ(prof.uid.size(), 4);
    EXPECT_EQ(prof.uid[0], 10);
    EXPECT_EQ(prof.username[0], "alice");
    EXPECT_EQ(prof.first_edit_day[0], 1000);
    EXPECT_EQ(prof.first_seen_day[0], 1000);
    EXPECT_TRUE(prof.bulk_new_user[0]);

    EXPECT_EQ(prof.uid[1], 10);
    EXPECT_EQ(prof.username[1], "alice_alias");
    EXPECT_EQ(prof.first_edit_day[1], 1050);
    EXPECT_EQ(prof.first_seen_day[1], 1000);
    EXPECT_TRUE(prof.bulk_new_user[1]);  // uid-level flag, shared across segments

    EXPECT_EQ(prof.uid[2], 11);
    EXPECT_EQ(prof.username[2], "bob");
    EXPECT_EQ(prof.first_seen_day[2], 2000);
    EXPECT_FALSE(prof.bulk_new_user[2]);  // only 4 events in window

    EXPECT_EQ(prof.uid[3], 12);
    EXPECT_EQ(prof.username[3], "<12>");
    EXPECT_EQ(prof.first_seen_day[3], 3000);
    EXPECT_FALSE(prof.bulk_new_user[3]);
}

TEST(UserIndicatorFinalize, NoStageIsNoOp) {
    TempDir dir;
    const std::string profiles = dir.join("user_profiles.parquet");
    const std::string indicators = dir.join("user_indicators.parquet");

    user_indicators::Thresholds thresholds;
    EXPECT_NO_THROW(
        user_indicators::run_finalize(dir.join("nope"), profiles, indicators, thresholds));
    EXPECT_FALSE(std::filesystem::exists(profiles));
    EXPECT_FALSE(std::filesystem::exists(indicators));

    // An empty stage directory is equally a no-op.
    const std::string empty_stage = dir.join("empty_stage");
    std::filesystem::create_directories(empty_stage);
    EXPECT_NO_THROW(
        user_indicators::run_finalize(empty_stage, profiles, indicators, thresholds));
    EXPECT_FALSE(std::filesystem::exists(profiles));
    EXPECT_FALSE(std::filesystem::exists(indicators));
}

TEST(UserIndicatorFinalize, WindowAndBulkThresholdsApply) {
    TempDir dir;
    const std::string stage = dir.join("stage");
    const std::string profiles = dir.join("user_profiles.parquet");
    const std::string indicators = dir.join("user_indicators.parquet");
    std::filesystem::create_directories(stage);

    // uid 20: 3 events within 7 days are below bulk_edit_min even though its
    // timeline is early; uid 21: 12 events across 8 days exceed the window.
    write_stage(stage + "/stage_00000.parquet",
                {
                    {21, "mallory", 1000, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {20, "eve", 5000, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                    {21, "mallory", 1006, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                });

    user_indicators::Thresholds thresholds;
    thresholds.new_user_window_days = 7;
    thresholds.bulk_edit_min = 10;
    user_indicators::run_finalize(stage, profiles, indicators, thresholds);

    const auto prof = read_profiles(profiles);
    ASSERT_EQ(prof.uid.size(), 2);
    EXPECT_EQ(prof.uid[0], 20);
    EXPECT_FALSE(prof.bulk_new_user[0]);
    EXPECT_EQ(prof.uid[1], 21);
    EXPECT_TRUE(prof.bulk_new_user[1]);  // 6 + 6 within [1000, 1006]
}

}  // namespace