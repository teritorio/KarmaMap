#include <gtest/gtest.h>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include <arrow/api.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "country_index.hpp"
#include "test_helpers.hpp"

namespace {

using test_helpers::TempDir;

// The S2 cell of `level` containing (lat, lon), the id shape country.parquet
// stores.
uint64_t covering_cell(double lat, double lon, int level) {
    return S2CellId(S2LatLng::FromDegrees(lat, lon)).parent(level).id();
}

// Writes a country.parquet (s2_cell, country_id, level) and the sibling
// country_ids.json, as country_s2_index.py does.
void write_index(const std::string& parquet_path, const std::string& json_path,
                 const std::vector<std::tuple<uint64_t, uint8_t, uint8_t>>& rows,
                 const std::string& json_text) {
    arrow::UInt64Builder cell_builder;
    arrow::UInt8Builder id_builder;
    arrow::UInt8Builder level_builder;
    for (const auto& [cell, id, level] : rows) {
        ASSERT_TRUE(cell_builder.Append(cell).ok());
        ASSERT_TRUE(id_builder.Append(id).ok());
        ASSERT_TRUE(level_builder.Append(level).ok());
    }
    std::shared_ptr<arrow::Array> cells, ids, levels;
    ASSERT_TRUE(cell_builder.Finish(&cells).ok());
    ASSERT_TRUE(id_builder.Finish(&ids).ok());
    ASSERT_TRUE(level_builder.Finish(&levels).ok());

    auto schema = arrow::schema({arrow::field("s2_cell", arrow::uint64(), false),
                                 arrow::field("country_id", arrow::uint8(), false),
                                 arrow::field("level", arrow::uint8(), false)});
    test_helpers::write_table(parquet_path,
                              arrow::Table::Make(schema, {cells, ids, levels}));

    std::ofstream out(json_path);
    ASSERT_TRUE(out.is_open());
    out << json_text;
}

TEST(CountryIndex, ResolvesCoveringCell) {
    TempDir dir;
    const double paris_lat = 48.8566;
    const double paris_lon = 2.3522;

    write_index(dir.join("country.parquet"), dir.join("country_ids.json"),
                {{covering_cell(paris_lat, paris_lon, 5), 1, 5}},
                "{\n \"0\": null,\n \"1\": \"FR\"\n}\n");

    const country_index::CountryIndex index =
        country_index::CountryIndex::load(dir.join("country.parquet"),
                                          dir.join("country_ids.json"));
    ASSERT_EQ(index.size(), 1u);
    auto iso = index.lookup(paris_lat, paris_lon);
    ASSERT_TRUE(iso.has_value());
    EXPECT_EQ(*iso, "FR");
}

TEST(CountryIndex, OceanAndOtherCellsResolveToNothing) {
    TempDir dir;
    const double paris_lat = 48.8566;
    const double paris_lon = 2.3522;

    write_index(dir.join("country.parquet"), dir.join("country_ids.json"),
                {{covering_cell(paris_lat, paris_lon, 5), 1, 5}},
                "{\n \"0\": null,\n \"1\": \"FR\"\n}\n");

    const country_index::CountryIndex index =
        country_index::CountryIndex::load(dir.join("country.parquet"),
                                          dir.join("country_ids.json"));
    // A point far outside the indexed cell (South Pacific) falls in no row.
    EXPECT_FALSE(index.lookup(-27.0, -109.0).has_value());
}

TEST(CountryIndex, RowsAreSortedByRangeNotFileOrder) {
    TempDir dir;
    // Two disjoint cells. Feed the larger cell id first so the on-disk order is
    // the reverse of the ascending range_min order load() sorts into.
    const uint64_t paris = covering_cell(48.8566, 2.3522, 5);
    const uint64_t berlin = covering_cell(52.5200, 13.4050, 5);
    ASSERT_NE(paris, berlin);

    const uint64_t hi = std::max(paris, berlin);
    const uint64_t lo = std::min(paris, berlin);
    write_index(dir.join("country.parquet"), dir.join("country_ids.json"),
                {{hi, hi == paris ? 1 : 2, 5}, {lo, lo == paris ? 1 : 2, 5}},
                "{\n \"0\": null,\n \"1\": \"FR\",\n \"2\": \"DE\"\n}\n");

    const country_index::CountryIndex index =
        country_index::CountryIndex::load(dir.join("country.parquet"),
                                          dir.join("country_ids.json"));
    ASSERT_TRUE(index.lookup(48.8566, 2.3522).has_value());
    EXPECT_EQ(*index.lookup(48.8566, 2.3522), "FR");
    ASSERT_TRUE(index.lookup(52.5200, 13.4050).has_value());
    EXPECT_EQ(*index.lookup(52.5200, 13.4050), "DE");
}

TEST(CountryIndex, CountryIdZeroResolvesToNothing) {
    TempDir dir;
    const double lat = 10.0;
    const double lon = 10.0;
    write_index(dir.join("country.parquet"), dir.join("country_ids.json"),
                {{covering_cell(lat, lon, 5), 0, 5}},
                "{\n \"0\": null,\n \"1\": \"FR\"\n}\n");

    const country_index::CountryIndex index =
        country_index::CountryIndex::load(dir.join("country.parquet"),
                                          dir.join("country_ids.json"));
    EXPECT_FALSE(index.lookup(lat, lon).has_value());
}

TEST(CountryIndex, EmptyIndexResolvesToNothing) {
    TempDir dir;
    write_index(dir.join("country.parquet"), dir.join("country_ids.json"), {},
                "{\n \"0\": null\n}\n");

    const country_index::CountryIndex index =
        country_index::CountryIndex::load(dir.join("country.parquet"),
                                          dir.join("country_ids.json"));
    EXPECT_TRUE(index.empty());
    EXPECT_FALSE(index.lookup(48.8566, 2.3522).has_value());
}

TEST(CountryIndex, MissingIdTableThrows) {
    TempDir dir;
    // main.cpp catches this so an update still runs with empty countries.
    EXPECT_THROW(country_index::CountryIndex::load(dir.join("country.parquet"),
                                                   dir.join("country_ids.json")),
                 std::runtime_error);
}

TEST(CountryIndex, AdjacentCellsResolveToTheirOwnCountry) {
    TempDir dir;
    // Two disjoint sibling cells, each carrying a different country.
    const S2CellId parent(covering_cell(0.0, 0.0, 2));
    const S2LatLng first_center = parent.child(0).ToLatLng();
    const S2LatLng second_center = parent.child(1).ToLatLng();

    write_index(dir.join("country.parquet"), dir.join("country_ids.json"),
                {{parent.child(0).id(), 1, 3}, {parent.child(1).id(), 2, 3}},
                "{\n \"0\": null,\n \"1\": \"AA\",\n \"2\": \"BB\"\n}\n");

    const country_index::CountryIndex index =
        country_index::CountryIndex::load(dir.join("country.parquet"),
                                          dir.join("country_ids.json"));
    EXPECT_EQ(*index.lookup(first_center.lat().degrees(), first_center.lng().degrees()),
              "AA");
    EXPECT_EQ(*index.lookup(second_center.lat().degrees(), second_center.lng().degrees()),
              "BB");
}

}  // namespace
