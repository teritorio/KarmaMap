#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
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

// Writes one stage file with the exact schema the scan produces: uid,
// username, change_date and the 22 uint32 counters.
void write_stage(const std::string& path,
                 const std::vector<std::pair<int64_t, std::vector<uint32_t>>>& rows) {
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(outfile_result.ok()) << outfile_result.status();

    arrow::Int64Builder uid;
    arrow::StringBuilder username;
    arrow::UInt16Builder day;
    std::vector<std::unique_ptr<arrow::UInt32Builder>> counters;
    for (size_t i = 0; i < 22; ++i) {
        counters.push_back(std::make_unique<arrow::UInt32Builder>());
    }
    for (size_t i = 0; i < rows.size(); ++i) {
        append_ok(uid, rows[i].first);
        append_ok(username, std::to_string(rows[i].first));
        append_ok(day, static_cast<uint16_t>(100 + i));
        ASSERT_EQ(rows[i].second.size(), 22);
        for (size_t c = 0; c < 22; ++c) append_ok(*counters[c], rows[i].second[c]);
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

TEST(ReputationFile, WritesExactWidePerUidTable) {
    TempDir dir;
    const std::string stage = dir.join("stage");
    const std::string profiles = dir.join("user_profiles.parquet");
    const std::string indicators = dir.join("user_indicators.parquet");
    std::filesystem::create_directories(stage);

    // Counter indices follow the stage schema: 0 node_created, 3 way_created,
    // 9 relation_created, 12 tag_building, 13 tag_highway.
    const uint32_t node_created = 0;
    const uint32_t way_created = 3;
    const uint32_t relation_created = 9;
    const uint32_t tag_building = 12;
    const uint32_t tag_highway = 13;
    std::vector<uint32_t> c(22, 0);
    auto row = [&](std::initializer_list<std::pair<uint32_t, uint32_t>> set) {
        auto r = c;
        for (const auto& [idx, v] : set) r[idx] = v;
        return r;
    };

    write_stage(stage + "/stage_00000.parquet",
                {
                    // uid 1: node_created 6 + 4 = 10, tag_building 3.
                    {1, row({{node_created, 6}, {tag_building, 3}})},
                    {1, row({{node_created, 4}})},
                    // uid 2: node_created 5, tag_highway 2.
                    {2, row({{node_created, 5}, {tag_highway, 2}})},
                    // uid 3: node_created 5, way_created 1.
                    {3, row({{node_created, 5}, {way_created, 1}})},
                    // uid 4: relation_created 7.
                    {4, row({{relation_created, 7}})},
                });

    user_indicators::Thresholds thresholds;
    user_indicators::run_finalize(stage, profiles, indicators, thresholds);

    const std::string rep = dir.join("user_reputation.parquet");
    EXPECT_TRUE(std::filesystem::exists(rep));
    EXPECT_FALSE(std::filesystem::exists(rep + ".tmp"));

    auto combined_result = read_parquet(rep)->CombineChunks();
    ASSERT_TRUE(combined_result.ok());
    const auto& t = *combined_result;
    ASSERT_EQ(t->num_columns(), 84);
    ASSERT_EQ(t->num_rows(), 4);

    const auto col = [&](const std::string& name) -> std::shared_ptr<arrow::Array> {
        const int idx = t->schema()->GetFieldIndex(name);
        EXPECT_GE(idx, 0) << "missing column " << name;
        return idx >= 0 ? t->column(idx)->chunk(0) : nullptr;
    };
    const auto uid_arr = std::static_pointer_cast<arrow::Int64Array>(col("uid"));
    const auto score_arr = std::static_pointer_cast<arrow::UInt8Array>(col("reputation"));
    const auto double_col = [&](const std::string& name) {
        return std::static_pointer_cast<arrow::DoubleArray>(col(name));
    };
    const auto uint64_col = [&](const std::string& name) {
        return std::static_pointer_cast<arrow::UInt64Array>(col(name));
    };
    const auto uint32_col = [&](const std::string& name) {
        return std::static_pointer_cast<arrow::UInt32Array>(col(name));
    };

    for (int64_t i = 0; i < 4; ++i) EXPECT_EQ(uid_arr->Value(i), i + 1);  // uid-sorted

    // Reputations: uid1 = node 20 + tag_building 4; uid3 takes the way cap;
    // uid4 takes the relation cap; uid2 only tag_highway 4.
    const std::vector<uint8_t> exp_score = {24, 4, 20, 12};
    for (int64_t i = 0; i < 4; ++i) EXPECT_EQ(score_arr->Value(i), exp_score[i]);

    // Per-uid counter totals (the wide file's running sums).
    const auto node_created_arr = uint32_col("node_created");
    const auto way_created_arr = uint32_col("way_created");
    const auto relation_created_arr = uint32_col("relation_created");
    const auto tag_building_arr = uint32_col("tag_building");
    const auto tag_highway_arr = uint32_col("tag_highway");
    EXPECT_EQ(node_created_arr->Value(0), 10);
    EXPECT_EQ(node_created_arr->Value(1), 5);
    EXPECT_EQ(node_created_arr->Value(2), 5);
    EXPECT_EQ(way_created_arr->Value(2), 1);
    EXPECT_EQ(relation_created_arr->Value(3), 7);
    EXPECT_EQ(tag_building_arr->Value(0), 3);
    EXPECT_EQ(tag_highway_arr->Value(1), 2);

    // node aspect: uid1 (10) is the unique busiest of {10, 5, 5}.
    const auto node_points = double_col("node_points");
    const auto node_pct = double_col("node_pct");
    const auto node_active = uint64_col("node_active");
    const auto node_max = uint64_col("node_max");
    EXPECT_DOUBLE_EQ(node_points->Value(0), 20.0);
    EXPECT_DOUBLE_EQ(node_pct->Value(0), 100.0);
    EXPECT_DOUBLE_EQ(node_points->Value(1), 0.0);
    EXPECT_DOUBLE_EQ(node_points->Value(2), 0.0);
    EXPECT_EQ(node_active->Value(0), 3);  // repeated per row
    EXPECT_EQ(node_active->Value(3), 3);
    EXPECT_EQ(node_max->Value(0), 10);

    // way aspect: uid3 alone is active.
    const auto way_points = double_col("way_points");
    const auto way_active = uint64_col("way_active");
    const auto way_max = uint64_col("way_max");
    EXPECT_DOUBLE_EQ(way_points->Value(2), 20.0);
    EXPECT_DOUBLE_EQ(way_points->Value(0), 0.0);
    EXPECT_EQ(way_active->Value(0), 1);
    EXPECT_EQ(way_max->Value(0), 1);

    // relation aspect: uid4 alone is active.
    const auto relation_points = double_col("relation_points");
    const auto relation_pct = double_col("relation_pct");
    EXPECT_DOUBLE_EQ(relation_points->Value(3), 12.0);
    EXPECT_DOUBLE_EQ(relation_pct->Value(3), 100.0);

    // tag_building / tag_highway, each a single contributor.
    const auto tag_building_points = double_col("tag_building_points");
    const auto tag_building_max = uint64_col("tag_building_max");
    const auto tag_highway_points = double_col("tag_highway_points");
    EXPECT_DOUBLE_EQ(tag_building_points->Value(0), 4.0);
    EXPECT_EQ(tag_building_max->Value(0), 3);
    EXPECT_DOUBLE_EQ(tag_highway_points->Value(1), 4.0);

    // Untouched tag aspects carry zeros.
    const auto tag_amenity_points = double_col("tag_amenity_points");
    EXPECT_DOUBLE_EQ(tag_amenity_points->Value(0), 0.0);
    const auto tag_amenity_active = uint64_col("tag_amenity_active");
    EXPECT_EQ(tag_amenity_active->Value(0), 0);
}

}  // namespace