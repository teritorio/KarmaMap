#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "state.hpp"

namespace {

using replication_state::normalize_update_url;
using replication_state::parse_state;
using replication_state::state_txt_url;

TEST(StateUrl, NormalizeUpdateUrl) {
    EXPECT_EQ(normalize_update_url("https://x/y-updates/"),
              "https://x/y-updates/");
    EXPECT_EQ(normalize_update_url("https://x/y-updates"),
              "https://x/y-updates/");
    EXPECT_EQ(normalize_update_url(""), "");
}

TEST(StateUrl, StateTxtUrlAppendsToNormalizedBase) {
    EXPECT_EQ(state_txt_url("https://x/y-updates/"),
              "https://x/y-updates/state.txt");
    EXPECT_EQ(state_txt_url("https://x/y-updates"),
              "https://x/y-updates/state.txt");
}

TEST(StateParse, ParsesOsmosisFormat) {
    const std::string content =
        "#Mon Dec 30 09:37:18 UTC 2019\n"
        "sequenceNumber=2847632\n"
        "timestamp=2019-12-30T09\\:36\\:32Z\n";
    const auto state = parse_state(content, "https://x/y-updates/");
    EXPECT_EQ(state.sequence_number, 2847632ULL);
    EXPECT_EQ(state.timestamp, "2019-12-30T09\\:36\\:32Z");
    EXPECT_EQ(state.url, "https://x/y-updates/");
}

TEST(StateParse, SkipsBlankLines) {
    const std::string content =
        "\nsequenceNumber=1\n\n  \ntimestamp=2020-01-01T00\\:00\\:00Z\n";
    const auto state = parse_state(content, "u");
    EXPECT_EQ(state.sequence_number, 1ULL);
    EXPECT_EQ(state.timestamp, "2020-01-01T00\\:00\\:00Z");
}

TEST(StateParse, HandlesCrlfLineEndings) {
    const std::string content =
        "sequenceNumber=2847632\r\ntimestamp=2019-12-30T09\\:36\\:32Z\r\n";
    const auto state = parse_state(content, "u");
    EXPECT_EQ(state.sequence_number, 2847632ULL);
    EXPECT_EQ(state.timestamp, "2019-12-30T09\\:36\\:32Z");
}

TEST(StateParse, MissingSequenceNumberThrows) {
    EXPECT_THROW(parse_state("timestamp=2020-01-01T00\\:00\\:00Z\n", "u"),
                 std::runtime_error);
}

TEST(StateParse, MissingTimestampThrows) {
    EXPECT_THROW(parse_state("sequenceNumber=1\n", "u"), std::runtime_error);
}

TEST(StateParse, MalformedSequenceNumberThrows) {
    EXPECT_THROW(
        parse_state("sequenceNumber=12a\ntimestamp=2020-01-01T00\\:00\\:00Z\n",
                    "u"),
        std::runtime_error);
}

TEST(StateParse, EmptyContentThrows) {
    EXPECT_THROW(parse_state("", "u"), std::runtime_error);
}

}  // namespace