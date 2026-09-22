#include <gtest/gtest.h>

#include <arrow/api.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "h3_utils.hpp"
#include "node_cache.hpp"
#include "options.hpp"
#include "test_helpers.hpp"
#include "update.hpp"

namespace {

using test_helpers::TempDir;
using test_helpers::read_parquet;

const char* kOsc =
    "<?xml version='1.0' encoding='UTF-8'?>\n"
    "<osmChange version=\"0.6\" generator=\"test\">\n"
    "  <create>\n"
    "    <node id=\"20\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
    " lat=\"0.02\" lon=\"0.02\"/>\n"
    "    <way id=\"101\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
    " uid=\"1\" user=\"u\"><nd ref=\"10\"/><nd ref=\"20\"/></way>\n"
    "    <way id=\"103\" version=\"1\" timestamp=\"2024-01-03T00:00:00Z\""
    " uid=\"1\" user=\"u\"><nd ref=\"11\"/><nd ref=\"12\"/></way>\n"
    "    <way id=\"105\" version=\"1\" timestamp=\"2024-01-03T00:00:00Z\""
    " uid=\"1\" user=\"u\"><nd ref=\"999\"/></way>\n"
    "  </create>\n"
    "  <modify>\n"
    "    <node id=\"10\" version=\"2\" timestamp=\"2024-01-01T00:00:00Z\""
    " uid=\"2\" user=\"v\" lat=\"0.03\" lon=\"0.03\"/>\n"
    "    <way id=\"102\" version=\"2\" timestamp=\"2024-01-02T00:00:00Z\""
    " uid=\"1\" user=\"u\"><nd ref=\"12\"/><nd ref=\"13\"/></way>\n"
    "  </modify>\n"
    "  <delete>\n"
    "    <node id=\"11\" version=\"3\" timestamp=\"2024-01-01T00:00:00Z\""
    " uid=\"1\" user=\"u\"/>\n"
    "    <way id=\"104\" version=\"2\" timestamp=\"2024-01-04T00:00:00Z\""
    " uid=\"1\" user=\"u\"><nd ref=\"10\"/><nd ref=\"11\"/><nd ref=\"13\"/></way>\n"
    "  </delete>\n"
    "</osmChange>\n";

// Aggregated (cell, day) -> count of a ways.<seq>.parquet staging file.
using Aggregated = std::map<std::tuple<uint64_t, uint32_t>, uint32_t>;

std::string find_ways_file(const std::string& root, uint64_t seq) {
    const std::string want = "ways." + std::to_string(seq) + ".parquet";
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().filename() == want) {
            return entry.path().string();
        }
    }
    throw std::runtime_error("No " + want + " under " + root);
}

Aggregated read_ways(const std::string& root, uint64_t seq) {
    const std::string path = find_ways_file(root, seq);
    auto combined = read_parquet(path)->CombineChunks();
    if (!combined.ok()) throw std::runtime_error("CombineChunks failed");
    const auto& table = *combined;

    Aggregated out;
    const auto* cells = static_cast<const arrow::UInt64Array*>(table->column(0)->chunk(0).get());
    const auto* days = static_cast<const arrow::UInt16Array*>(table->column(1)->chunk(0).get());
    const auto* counts = static_cast<const arrow::UInt32Array*>(table->column(2)->chunk(0).get());
    for (int64_t i = 0; i < table->num_rows(); ++i) {
        out[{cells->Value(i), days->Value(i)}] += counts->Value(i);
    }
    return out;
}

// Runs the node pass then the way pass over `osc` against a fresh NodeState
// seeded from `cache`, exactly as the update stage does per diff.
void run_diffs(const std::string& osc, const std::string& cache,
               const std::string& changes_root, size_t way_batch_bytes) {
    update_pass::NodeState state(cache, 9);
    update_pass::run_node_update(osc, changes_root, 1, 9, &state);
    update_pass::run_way_update(osc, changes_root, 1, state, way_batch_bytes);
}

uint64_t cell(double lat, double lon) { return h3_utils::location_to_cell(lat, lon, 9); }

TEST(WayUpdatePass, ResolvesOverlayDeletedAndBaseRefs) {
    TempDir dir;
    const std::string cache = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(cache, 9);
        w.add(10, cell(0.0, 0.0));
        w.add(11, cell(0.0, 0.01));
        w.add(12, cell(0.01, 0.0));
        w.add(13, cell(0.01, 0.01));
        w.finish();
    }
    const std::string osc = dir.join("diff.osc");
    {
        std::ofstream out(osc);
        out << kOsc;
    }

    const std::string changes_root = dir.join("changes");
    std::filesystem::create_directories(changes_root);
    run_diffs(osc, cache, changes_root, kDefaultWayBatchBytes);

    // 2024-01-01..04 are UTC days 19723..19726.
    const uint32_t d1 = 19723, d2 = 19724, d3 = 19725, d4 = 19726;
    Aggregated want;
    // way 101 visible: node 20 in overlay pins its create position; node 10
    // was modified in the same diff, so post() returns its overlay cell.
    want[{cell(0.02, 0.02), d1}] += 1;
    want[{cell(0.03, 0.03), d1}] += 1;
    // way 102 visible: both refs resolve to base cells.
    want[{cell(0.01, 0.0), d2}] += 1;
    want[{cell(0.01, 0.01), d2}] += 1;
    // way 103 visible: node 11 was deleted (coordinate-less delete, so it
    // reaches remove_node's deleted set) -> skipped; node 12 base. way 105
    // references the unknown node 999 -> skipped.
    want[{cell(0.01, 0.0), d3}] += 1;
    // way 104 deleted: pre-update geometry. node 10's pre() is its overlay
    // cell (modified in this diff); node 11's pre() is its base cell;
    // node 13's is its base cell.
    want[{cell(0.03, 0.03), d4}] += 1;
    want[{cell(0.0, 0.01), d4}] += 1;
    want[{cell(0.01, 0.01), d4}] += 1;

    EXPECT_EQ(read_ways(changes_root, 1), want);
}

TEST(WayUpdatePass, BatchSizeDoesNotChangeCounts) {
    TempDir dir;
    const std::string cache = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(cache, 9);
        w.add(10, cell(0.0, 0.0));
        w.add(11, cell(0.0, 0.01));
        w.add(12, cell(0.01, 0.0));
        w.add(13, cell(0.01, 0.01));
        w.finish();
    }
    const std::string osc = dir.join("diff.osc");
    {
        std::ofstream out(osc);
        out << kOsc;
    }

    const std::string big_root = dir.join("changes_big");
    const std::string tiny_root = dir.join("changes_tiny");
    std::filesystem::create_directories(big_root);
    std::filesystem::create_directories(tiny_root);
    run_diffs(osc, cache, big_root, kDefaultWayBatchBytes);
    run_diffs(osc, cache, tiny_root, 1);  // one batch per way

    EXPECT_EQ(read_ways(tiny_root, 1), read_ways(big_root, 1));
}

}  // namespace