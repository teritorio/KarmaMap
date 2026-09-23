#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#include "state.hpp"

namespace {

using replication_state::diff_url;
using replication_state::normalize_update_url;
using replication_state::parse_state;
using replication_state::read_state_file;
using replication_state::sidecar_state_path;
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

TEST(StateUrl, DiffUrlThreeThreeThreeLayout) {
    EXPECT_EQ(diff_url("https://x/y-updates/", 2847632ULL),
              "https://x/y-updates/002/847/632.osc.gz");
}

TEST(StateUrl, DiffUrlPadsLowSequences) {
    EXPECT_EQ(diff_url("https://x/y-updates/", 42ULL),
              "https://x/y-updates/000/000/042.osc.gz");
    EXPECT_EQ(diff_url("https://x/y-updates/", 0ULL),
              "https://x/y-updates/000/000/000.osc.gz");
}

TEST(StateUrl, DiffUrlNormalizesTrailingSlash) {
    EXPECT_EQ(diff_url("https://x/y-updates", 9999999ULL),
              "https://x/y-updates/009/999/999.osc.gz");
}

TEST(DiffFileName, ParsesSequence) {
    EXPECT_EQ(replication_state::diff_file_sequence("2847632.osc.gz").value(),
              2847632ULL);
    EXPECT_EQ(replication_state::diff_file_sequence("000000042.osc.gz").value(),
              42ULL);
}

TEST(DiffFileName, NulloptForOscGzOnlyName) {
    EXPECT_FALSE(replication_state::diff_file_sequence(".osc.gz").has_value());
}

TEST(DiffFileName, NulloptForTempFile) {
    EXPECT_FALSE(replication_state::diff_file_sequence("42.osc.gz.tmp").has_value());
}

TEST(DiffFileName, NulloptForNonNumericStem) {
    EXPECT_FALSE(replication_state::diff_file_sequence("abc.osc.gz").has_value());
    EXPECT_FALSE(replication_state::diff_file_sequence("42a.osc.gz").has_value());
    EXPECT_FALSE(replication_state::diff_file_sequence("-42.osc.gz").has_value());
}

TEST(DiffFileName, NulloptForUnrelatedName) {
    EXPECT_FALSE(replication_state::diff_file_sequence("notes.pdf").has_value());
}

TEST(SidecarStatePath, SwapsOshPbfExtension) {
    EXPECT_EQ(sidecar_state_path("data/region.osh.pbf"),
              "data/region.state.txt");
}

TEST(SidecarStatePath, SwapsOsmPbfExtension) {
    EXPECT_EQ(sidecar_state_path("canary-islands-260919.osm.pbf"),
              "canary-islands-260919.state.txt");
}

TEST(SidecarStatePath, SwapsPlainPbfExtension) {
    EXPECT_EQ(sidecar_state_path("data/region.pbf"), "data/region.state.txt");
}

TEST(SidecarStatePath, AppendsToExtensionlessPath) {
    EXPECT_EQ(sidecar_state_path("data/region"), "data/region.state.txt");
}

TEST(ReadStateFile, ParsesAFileFromDisk) {
    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / "karmamap_state_test";
    std::filesystem::create_directories(dir);
    const std::filesystem::path file = dir / "region.state.txt";
    {
        std::ofstream out(file);
        out << "sequenceNumber=2847632\n"
            << "timestamp=2019-12-30T09\\:36\\:32Z\n";
    }
    const auto state = read_state_file(file.string(), "https://x/y-updates/");
    std::filesystem::remove_all(dir);
    EXPECT_EQ(state.sequence_number, 2847632ULL);
    EXPECT_EQ(state.timestamp, "2019-12-30T09\\:36\\:32Z");
    EXPECT_EQ(state.url, "https://x/y-updates/");
}

TEST(ReadStateFile, MissingFileThrows) {
    EXPECT_THROW(read_state_file("/no/such/state.txt", "u"), std::runtime_error);
}

TEST(ReadStateFile, MalformedContentThrows) {
    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / "karmamap_state_test_malformed";
    std::filesystem::create_directories(dir);
    const std::filesystem::path file = dir / "region.state.txt";
    {
        std::ofstream out(file);
        out << "not a state file\n";
    }
    EXPECT_THROW(read_state_file(file.string(), "u"), std::runtime_error);
    std::filesystem::remove_all(dir);
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
