// Tests for suspect_atom::write_feed: an Atom 1.0 mirror of suspect.parquet,
// published next to the parquet on every update run.

#include "suspect_atom.hpp"

#include "date_utils.hpp"
#include "suspect.hpp"
#include "test_helpers.hpp"

#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/table.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace {

void require_ok(arrow::Status status) {
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

std::shared_ptr<arrow::Array> make_list_column(
    const std::vector<std::vector<std::string>>& rows) {
    arrow::Int32Builder offsets_builder;
    arrow::StringBuilder values_builder;
    require_ok(offsets_builder.Append(0));
    int32_t total = 0;
    for (const auto& row : rows) {
        for (const auto& value : row) require_ok(values_builder.Append(value));
        total += static_cast<int32_t>(row.size());
        require_ok(offsets_builder.Append(total));
    }
    std::shared_ptr<arrow::Array> offsets, values;
    require_ok(offsets_builder.Finish(&offsets));
    require_ok(values_builder.Finish(&values));
    auto result = arrow::ListArray::FromArrays(*offsets, *values);
    if (!result.ok()) {
        throw std::runtime_error("ListArray::FromArrays failed: " +
                                 result.status().ToString());
    }
    return *result;
}

std::shared_ptr<arrow::Table> make_suspect_table() {
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder user_builder;
    arrow::UInt16Builder day_builder;
    arrow::UInt8Builder flag_builder;
    arrow::UInt32Builder changes_builder, hour_builder, moves_builder,
        maxmove_builder, lang_events_builder;
    arrow::FloatBuilder spread_builder;
    arrow::UInt8Builder rank_builder;

    constexpr int64_t uids[] = {42, 7, 999};
    constexpr const char* users[] = {"alice", "b<script>b", "&bad<user"};
    constexpr uint16_t days[] = {20744, 20743, 20742};
    constexpr uint8_t flags[] = {suspect::kFlagFilter2 | suspect::kFlagFilter4,
                                 suspect::kFlagFilter5 | suspect::kFlagFilter3,
                                 0x00};
    constexpr uint32_t k_changes[] = {1234, 50, 5};
    constexpr uint32_t hours[] = {600, 0, 0};
    constexpr float spreads[] = {23.7f, 0.0f, 0.0f};
    constexpr uint32_t k_moves[] = {0, 3, 0};
    constexpr uint32_t maxmoves[] = {0, 1200, 0};
    constexpr uint8_t ranks[] = {7, 99, 3};
    constexpr uint32_t k_lang_events[] = {7, 20, 30};

    const std::vector<std::vector<std::string>> keys = {{}, {"building", "amenity"}, {}};
    const std::vector<std::vector<std::string>> countries = {{"US", "FR"}, {}, {}};

    for (size_t i = 0; i < 3; ++i) {
        require_ok(uid_builder.Append(uids[i]));
        require_ok(user_builder.Append(users[i]));
        require_ok(day_builder.Append(days[i]));
        require_ok(flag_builder.Append(flags[i]));
        require_ok(changes_builder.Append(k_changes[i]));
        require_ok(hour_builder.Append(hours[i]));
        require_ok(spread_builder.Append(spreads[i]));
        require_ok(moves_builder.Append(k_moves[i]));
        require_ok(maxmove_builder.Append(maxmoves[i]));
        require_ok(lang_events_builder.Append(k_lang_events[i]));
        require_ok(rank_builder.Append(ranks[i]));
    }

    std::shared_ptr<arrow::Array> uid, user, day, flag, changes, hour, spread,
        moves, maxmove, rank, lang_events, keys_arr, countries_arr;
    require_ok(uid_builder.Finish(&uid));
    require_ok(user_builder.Finish(&user));
    require_ok(day_builder.Finish(&day));
    require_ok(flag_builder.Finish(&flag));
    require_ok(changes_builder.Finish(&changes));
    require_ok(hour_builder.Finish(&hour));
    require_ok(spread_builder.Finish(&spread));
    require_ok(moves_builder.Finish(&moves));
    require_ok(maxmove_builder.Finish(&maxmove));
    require_ok(lang_events_builder.Finish(&lang_events));
    require_ok(rank_builder.Finish(&rank));
    keys_arr = make_list_column(keys);
    countries_arr = make_list_column(countries);

    return arrow::Table::Make(
        arrow::schema({
            arrow::field("uid", arrow::int64(), false),
            arrow::field("username", arrow::utf8(), false),
            arrow::field("change_date", arrow::uint16(), false),
            arrow::field("suspect_flag", arrow::uint8(), false),
            arrow::field("changes", arrow::uint32(), false),
            arrow::field("max_edits_per_hour", arrow::uint32(), false),
            arrow::field("peak_spread_km2", arrow::float32(), false),
            arrow::field("changed_keys", arrow::list(arrow::utf8()), false),
            arrow::field("countries", arrow::list(arrow::utf8()), false),
            arrow::field("far_move_count", arrow::uint32(), false),
            arrow::field("max_move_meters", arrow::uint32(), false),
            arrow::field("ranking_at_day", arrow::uint8(), false),
            arrow::field("name_lang_events", arrow::uint32(), false),
        }),
        std::vector<std::shared_ptr<arrow::Array>>{
            uid, user, day, flag, changes, hour, spread, keys_arr, countries_arr,
            moves, maxmove, rank, lang_events});
}

std::shared_ptr<arrow::Table> empty_suspect_table() {
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder user_builder;
    arrow::UInt16Builder day_builder;
    arrow::UInt8Builder flag_builder;
    arrow::UInt32Builder changes_builder, hour_builder, moves_builder,
        maxmove_builder, lang_events_builder;
    arrow::FloatBuilder spread_builder;
    arrow::UInt8Builder rank_builder;
    std::shared_ptr<arrow::Array> uid, user, day, flag, changes, hour, spread,
        moves, maxmove, rank, lang_events;
    require_ok(uid_builder.Finish(&uid));
    require_ok(user_builder.Finish(&user));
    require_ok(day_builder.Finish(&day));
    require_ok(flag_builder.Finish(&flag));
    require_ok(changes_builder.Finish(&changes));
    require_ok(hour_builder.Finish(&hour));
    require_ok(spread_builder.Finish(&spread));
    require_ok(moves_builder.Finish(&moves));
    require_ok(maxmove_builder.Finish(&maxmove));
    require_ok(lang_events_builder.Finish(&lang_events));
    require_ok(rank_builder.Finish(&rank));
    return arrow::Table::Make(
        arrow::schema({
            arrow::field("uid", arrow::int64(), false),
            arrow::field("username", arrow::utf8(), false),
            arrow::field("change_date", arrow::uint16(), false),
            arrow::field("suspect_flag", arrow::uint8(), false),
            arrow::field("changes", arrow::uint32(), false),
            arrow::field("max_edits_per_hour", arrow::uint32(), false),
            arrow::field("peak_spread_km2", arrow::float32(), false),
            arrow::field("changed_keys", arrow::list(arrow::utf8()), false),
            arrow::field("countries", arrow::list(arrow::utf8()), false),
            arrow::field("far_move_count", arrow::uint32(), false),
            arrow::field("max_move_meters", arrow::uint32(), false),
            arrow::field("ranking_at_day", arrow::uint8(), false),
            arrow::field("name_lang_events", arrow::uint32(), false),
        }),
        std::vector<std::shared_ptr<arrow::Array>>{
            uid, user, day, flag, changes, hour, spread, make_list_column({}),
            make_list_column({}), moves, maxmove, rank, lang_events});
}

std::string feed_text(const std::string& atom_path) {
    std::ifstream in(atom_path);
    EXPECT_TRUE(in.is_open()) << "cannot read " << atom_path;
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

int count_entries(std::string_view s) {
    int n = 0;
    size_t pos = 0;
    while ((pos = s.find("<entry>", pos)) != std::string::npos) {
        ++n;
        pos += 7;
    }
    return n;
}

}  // namespace

TEST(SuspectAtomTest, writesEntriesNewestFirst) {
    test_helpers::TempDir dir;
    const std::string atom_path = dir.join("suspect.atom");

    auto table = make_suspect_table();
    suspect_atom::write_feed(*table, atom_path);

    EXPECT_TRUE(std::filesystem::exists(atom_path));
    EXPECT_FALSE(std::filesystem::exists(atom_path + ".tmp"));

    const std::string feed = feed_text(atom_path);
    EXPECT_NE(feed.find("<?xml version='1.0' encoding='utf-8'?>"),
              std::string::npos);
    EXPECT_NE(feed.find("<title>KarmaMap - suspects</title>"), std::string::npos);
    EXPECT_NE(feed.find("<id>tag:karmamap,2026:suspects</id>"), std::string::npos);
    EXPECT_NE(feed.find("<link rel='self' href='/data/suspect.atom'/>"),
              std::string::npos);
    EXPECT_NE(feed.find("<link rel='alternate' href='/suspect/'/>"), std::string::npos);
    EXPECT_EQ(count_entries(feed), 3);

    const std::string d0 = date_utils::iso_date(20744);
    const std::string d1 = date_utils::iso_date(20743);
    const std::string d2 = date_utils::iso_date(20742);
    EXPECT_NE(feed.find("<updated>" + d0 + "T00:00:00Z</updated>"), std::string::npos);

    EXPECT_NE(feed.find("<title>alice - " + d0 + " (1234 changes)</title>"),
              std::string::npos);
    EXPECT_NE(feed.find("<id>tag:karmamap," + d0 + ":suspect/42</id>"), std::string::npos);
    EXPECT_NE(feed.find("<link rel='alternate' href='../users/#user=alice'/>"),
              std::string::npos);
    EXPECT_NE(feed.find("https://www.openstreetmap.org/user/alice"), std::string::npos);
    EXPECT_NE(feed.find("<category term='filter-2'/>"), std::string::npos);
    EXPECT_NE(feed.find("<category term='filter-4'/>"), std::string::npos);
    EXPECT_NE(feed.find("<category term='US'/>"), std::string::npos);
    EXPECT_NE(feed.find("<category term='FR'/>"), std::string::npos);
    EXPECT_NE(feed.find("Filter 2 (edit burst): peak 600 edits/hour"),
              std::string::npos);
    EXPECT_NE(feed.find("Filter 4 (spread): peak 24 km^2"), std::string::npos);
    EXPECT_NE(feed.find(std::string("Countries: \U0001F1FA\U0001F1F8 US, ") +
                            "\U0001F1EB\U0001F1F7 FR"),
              std::string::npos);

    // Entries are newest first: row 1 follows row 0, so its filter markers
    // must appear after row 0's in the serialized feed.
    const size_t row1 = feed.find("<title>b&lt;script&gt;b - " + d1 + " (50 changes)</title>");
    EXPECT_NE(row1, std::string::npos);
    EXPECT_NE(feed.find("Filter 5 (mass tag): building amenity", row1),
              std::string::npos);
    EXPECT_NE(feed.find("Filter 3 (node moves): 3 far moves, largest 1.2 km", row1),
              std::string::npos);
    EXPECT_EQ(feed.find("<category term='filter-2'/>", row1), std::string::npos);

    const size_t row2 = feed.find("<title>&amp;bad&lt;user - " + d2 + " (5 changes)</title>");
    EXPECT_NE(row2, std::string::npos);
    EXPECT_NE(feed.find("Ranking at day: #3", row2), std::string::npos);
}

TEST(SuspectAtomTest, emptyTable) {
    test_helpers::TempDir dir;
    const std::string atom_path = dir.join("suspect.atom");

    suspect_atom::write_feed(*empty_suspect_table(), atom_path);

    EXPECT_TRUE(std::filesystem::exists(atom_path));
    EXPECT_FALSE(std::filesystem::exists(atom_path + ".tmp"));

    const std::string feed = feed_text(atom_path);
    EXPECT_EQ(count_entries(feed), 0);
    EXPECT_NE(feed.find("<updated>1970-01-01T00:00:00Z</updated>"), std::string::npos);
    EXPECT_NE(feed.find("</feed>"), std::string::npos);
}
