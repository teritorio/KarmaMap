#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <string>
#include <tuple>
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
// username, change_date and the 19 uint32 counters.
void write_stage_named(const std::string& path,
                       const std::vector<std::tuple<int64_t, std::string, uint16_t,
                                                      std::vector<uint32_t>>>& rows) {
    auto outfile_result = arrow::io::FileOutputStream::Open(path);
    ASSERT_TRUE(outfile_result.ok()) << outfile_result.status();

    arrow::Int64Builder uid;
    arrow::StringBuilder username;
    arrow::UInt16Builder day;
    std::vector<std::unique_ptr<arrow::UInt32Builder>> counters;
    for (size_t i = 0; i < user_indicators::kCounterCount; ++i) {
        counters.push_back(std::make_unique<arrow::UInt32Builder>());
    }
    for (const auto& [u, name, d, cts] : rows) {
        append_ok(uid, u);
        append_ok(username, name);
        append_ok(day, d);
        ASSERT_EQ(cts.size(), user_indicators::kCounterCount);
        for (size_t c = 0; c < user_indicators::kCounterCount; ++c) {
            append_ok(*counters[c], cts[c]);
        }
    }

    std::shared_ptr<arrow::Array> a_uid, a_user, a_day;
    std::vector<std::shared_ptr<arrow::Array>> a_counters(user_indicators::kCounterCount);
    finish_ok(uid, &a_uid);
    finish_ok(username, &a_user);
    finish_ok(day, &a_day);
    for (size_t i = 0; i < user_indicators::kCounterCount; ++i) {
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

void write_stage(const std::string& path,
                 const std::vector<std::pair<int64_t, std::vector<uint32_t>>>& rows) {
    std::vector<std::tuple<int64_t, std::string, uint16_t, std::vector<uint32_t>>> named;
    named.reserve(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        named.emplace_back(rows[i].first, std::to_string(rows[i].first),
                           static_cast<uint16_t>(100 + i), rows[i].second);
    }
    write_stage_named(path, named);
}

TEST(ReputationFile, WritesExactWidePerUidTable) {
    TempDir dir;
    const std::string stage = dir.join("stage");
    const std::string indicators = dir.join("user_indicators.parquet");
    std::filesystem::create_directories(stage);

    // Counter indices follow the stage schema: 0 node_created, 3 way_created,
    // 6 relation_created, 9 tag_building, 10 tag_highway.
    const uint32_t node_created = 0;
    const uint32_t way_created = 3;
    const uint32_t relation_created = 6;
    const uint32_t tag_building = 9;
    const uint32_t tag_highway = 10;
    std::vector<uint32_t> c(user_indicators::kCounterCount, 0);
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

    user_indicators::run_finalize(stage, indicators);

    const std::string rep = dir.join("user_reputation.parquet");
    EXPECT_TRUE(std::filesystem::exists(rep));
    EXPECT_FALSE(std::filesystem::exists(rep + ".tmp"));

    auto combined_result = read_parquet(rep)->CombineChunks();
    ASSERT_TRUE(combined_result.ok());
    const auto& t = *combined_result;
    ASSERT_EQ(t->num_columns(), 38);
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
    const auto uint32_col = [&](const std::string& name) {
        return std::static_pointer_cast<arrow::UInt32Array>(col(name));
    };

    for (int64_t i = 0; i < 4; ++i) EXPECT_EQ(uid_arr->Value(i), i + 1);
    // Fixture usernames "1".."4" happen to sort in uid order; the table is
    // username-sorted (uid tie-break).

    // Identity columns: username matches the stage's std::to_string(uid);
    // first_seen_day is the group's first change_date (100 + first row index).
    const auto username_arr = std::static_pointer_cast<arrow::StringArray>(col("username"));
    const auto first_seen_arr = std::static_pointer_cast<arrow::UInt16Array>(col("first_seen_day"));
    for (int64_t i = 0; i < 4; ++i) {
        EXPECT_EQ(username_arr->GetString(i), std::to_string(i + 1));
    }
    const std::vector<uint16_t> exp_first_seen = {100, 102, 103, 104};
    for (int64_t i = 0; i < 4; ++i) EXPECT_EQ(first_seen_arr->Value(i), exp_first_seen[i]);

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

    // Only the per-aspect percentile is stored per row (the points derive
    // from pct and the constant paper caps client-side); the dataset-wide
    // active/max stats live in the Parquet file footer's key_value_metadata
    // (what the browser reads with parquetMetadataAsync), not in the Arrow
    // reader's reconstructed schema -- so check the file metadata directly.
    const auto node_pct = double_col("node_pct");
    const auto way_pct = double_col("way_pct");
    const auto relation_pct = double_col("relation_pct");
    const auto tag_building_pct = double_col("tag_building_pct");
    const auto tag_highway_pct = double_col("tag_highway_pct");
    const auto tag_amenity_pct = double_col("tag_amenity_pct");
    EXPECT_DOUBLE_EQ(node_pct->Value(0), 100.0);  // node: uid1 (10) unique busiest of {10, 5, 5}
    EXPECT_DOUBLE_EQ(node_pct->Value(1), 0.0);
    EXPECT_DOUBLE_EQ(node_pct->Value(2), 0.0);
    EXPECT_DOUBLE_EQ(way_pct->Value(2), 100.0);   // way: uid3 alone is active
    EXPECT_DOUBLE_EQ(relation_pct->Value(3), 100.0);  // relation: uid4 alone is active
    EXPECT_DOUBLE_EQ(tag_building_pct->Value(0), 100.0);
    EXPECT_DOUBLE_EQ(tag_highway_pct->Value(1), 100.0);
    EXPECT_DOUBLE_EQ(tag_amenity_pct->Value(0), 0.0);

    auto rep_file_result = arrow::io::ReadableFile::Open(rep);
    ASSERT_TRUE(rep_file_result.ok()) << rep_file_result.status();
    const auto file_meta = parquet::ReadMetaData(*rep_file_result);
    const auto meta = file_meta->key_value_metadata();
    ASSERT_NE(meta, nullptr);
    const auto meta_val = [&](const std::string& key) -> std::string {
        auto value = meta->Get(key);
        EXPECT_TRUE(value.ok()) << "missing metadata key " << key;
        return value.ValueOr("");
    };
    EXPECT_EQ(meta_val("node_active"), "3");
    EXPECT_EQ(meta_val("node_max"), "10");
    EXPECT_EQ(meta_val("way_active"), "1");
    EXPECT_EQ(meta_val("way_max"), "1");
    EXPECT_EQ(meta_val("relation_active"), "1");
    EXPECT_EQ(meta_val("relation_max"), "7");
    EXPECT_EQ(meta_val("tag_building_active"), "1");
    EXPECT_EQ(meta_val("tag_building_max"), "3");
    EXPECT_EQ(meta_val("tag_highway_active"), "1");
    EXPECT_EQ(meta_val("tag_highway_max"), "2");
    EXPECT_EQ(meta_val("tag_amenity_active"), "0");
    EXPECT_EQ(meta_val("tag_amenity_max"), "0");
}

TEST(ReputationFile, SortsByUsernameThenUid) {
    TempDir dir;
    const std::string stage = dir.join("stage");
    const std::string indicators = dir.join("user_indicators.parquet");
    std::filesystem::create_directories(stage);

    const uint32_t node_created = 0;
    std::vector<uint32_t> c(user_indicators::kCounterCount, 0);
    auto row = [&](uint32_t v) { auto r = c; r[node_created] = v; return r; };

    // Uid groups arrive in uid order; the usernames are scrambled on purpose
    // (uid 7's "bob" row is written before uid 2's), so only a real username
    // sort with an uid tie-break yields the expected order below.
    write_stage_named(stage + "/stage_00000.parquet",
                      {
                          {1, "carol", 101, row(1)},
                          {7, "bob", 100, row(1)},
                          {2, "bob", 102, row(1)},
                          {5, "alice", 103, row(1)},
                          {8, "dave", 104, row(1)},
                      });

    user_indicators::run_finalize(stage, indicators);

    const std::string rep = dir.join("user_reputation.parquet");
    EXPECT_TRUE(std::filesystem::exists(rep));
    auto combined_result = read_parquet(rep)->CombineChunks();
    ASSERT_TRUE(combined_result.ok());
    const auto& t = *combined_result;
    ASSERT_EQ(t->num_rows(), 5);

    const auto usernames = std::static_pointer_cast<arrow::StringArray>(t->column(1)->chunk(0));
    const auto uids = std::static_pointer_cast<arrow::Int64Array>(t->column(0)->chunk(0));
    const std::vector<std::string> exp_username = {"alice", "bob", "bob", "carol", "dave"};
    const std::vector<int64_t> exp_uid = {5, 2, 7, 1, 8};
    for (int64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(usernames->GetString(i), exp_username[i]);
        EXPECT_EQ(uids->Value(i), exp_uid[i]);
    }
}

}  // namespace
