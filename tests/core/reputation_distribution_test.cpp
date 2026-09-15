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

struct DistRow {
    std::string aspect;
    uint64_t value;
    uint64_t less;
    uint64_t active;
};

std::vector<DistRow> read_distribution(const std::string& path) {
    auto combined_result = read_parquet(path)->CombineChunks();
    EXPECT_TRUE(combined_result.ok()) << combined_result.status();
    if (!combined_result.ok()) return {};
    const auto& t = *combined_result;
    EXPECT_EQ(t->num_columns(), 4);
    if (t->num_columns() != 4) return {};
    const auto* aspect = static_cast<const arrow::StringArray*>(t->column(0)->chunk(0).get());
    const auto* value = static_cast<const arrow::UInt64Array*>(t->column(1)->chunk(0).get());
    const auto* less = static_cast<const arrow::UInt64Array*>(t->column(2)->chunk(0).get());
    const auto* active = static_cast<const arrow::UInt64Array*>(t->column(3)->chunk(0).get());
    std::vector<DistRow> out;
    for (int64_t i = 0; i < t->num_rows(); ++i) {
        out.push_back({aspect->GetString(i), value->Value(i), less->Value(i),
                       active->Value(i)});
    }
    return out;
}

TEST(ReputationDistributionFile, AggregatesAndWritesSiblingParquet) {
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

    const std::string dist = dir.join("user_reputation_distribution.parquet");
    EXPECT_TRUE(std::filesystem::exists(dist));
    EXPECT_FALSE(std::filesystem::exists(dist + ".tmp"));

    // Aspects appear in order: node_created, way_created, relation_created,
    // then tag_* in kTop12TagKeys order. node_created totals {10,5,5} are
    // sampled exactly (values 5 and 10); single-contributor aspects are exact.
    const auto rows = read_distribution(dist);
    const std::vector<DistRow> expected = {
        {"node_created", 5, 0, 3},
        {"node_created", 10, 2, 3},
        {"way_created", 1, 0, 1},
        {"relation_created", 7, 0, 1},
        {"tag_building", 3, 0, 1},
        {"tag_highway", 2, 0, 1},
    };
    ASSERT_EQ(rows.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(rows[i].aspect, expected[i].aspect) << "row " << i;
        EXPECT_EQ(rows[i].value, expected[i].value) << "row " << i;
        EXPECT_EQ(rows[i].less, expected[i].less) << "row " << i;
        EXPECT_EQ(rows[i].active, expected[i].active) << "row " << i;
    }
}

}  // namespace