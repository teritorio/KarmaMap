#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "manifest.hpp"
#include "test_helpers.hpp"

namespace {

using test_helpers::TempDir;

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void make_partitions(const std::string& changes_root,
                     const std::vector<std::string>& yyyy_mm) {
    for (const auto& month : yyyy_mm) {
        const auto dash = month.find('-');
        const std::string year = month.substr(0, dash);
        const std::string mm = month.substr(dash + 1);
        std::filesystem::create_directories(
            changes_root + "/year=" + year + "/month=" + mm);
    }
}

TEST(Manifest, WritesEmptyManifest) {
    TempDir dir;
    manifest::write_manifest(dir.path(), 9);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"h3_resolution\": 9"), std::string::npos);
    EXPECT_NE(json.find("\"date_range\": null"), std::string::npos);
    EXPECT_NE(json.find("\"partitions\": []"), std::string::npos);
    EXPECT_NE(json.find("\"path\": \"changes\""), std::string::npos);
}

TEST(Manifest, SortsPartitionsAndRanges) {
    TempDir dir;
    // Build in scrambled order; output must be sorted.
    make_partitions(dir.join("changes"), {"2024-03", "2023-12", "2024-01"});
    manifest::write_manifest(dir.path(), 12);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"h3_resolution\": 12"), std::string::npos);
    EXPECT_NE(json.find("\"min_month\": \"2023-12\""), std::string::npos);
    EXPECT_NE(json.find("\"max_month\": \"2024-03\""), std::string::npos);
    EXPECT_NE(json.find("\"partitions\": [\"2023-12\", \"2024-01\", \"2024-03\"]"),
              std::string::npos);
}

TEST(Manifest, IgnoresForeignEntries) {
    TempDir dir;
    make_partitions(dir.join("changes"), {"2024-01"});
    std::filesystem::create_directories(dir.join("changes/some_other_dir"));
    std::filesystem::create_directories(dir.join("changes/year=2024/plain_month"));
    {
        std::ofstream(dir.join("changes/note.txt")) << "not a partition\n";
    }
    manifest::write_manifest(dir.path(), 4);

    const std::string json = read_file(dir.join("manifest.json"));
    EXPECT_NE(json.find("\"partitions\": [\"2024-01\"]"), std::string::npos);
}

TEST(Manifest, MissingOutputDirThrows) {
    TempDir dir;
    EXPECT_THROW(manifest::write_manifest(dir.join("nonexistent"), 9),
                 std::runtime_error);
}

}  // namespace