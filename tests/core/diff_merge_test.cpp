#include <gtest/gtest.h>

#include <osmium/handler.hpp>
#include <osmium/io/reader.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/visitor.hpp>

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

// One object as preserved through the merge, in file order.
struct ObjectKey {
    char type;  // 'n' | 'w' | 'r'
    int64_t id;
    uint32_t version;
    int64_t ts;
    bool operator==(const ObjectKey& other) const {
        return type == other.type && id == other.id && version == other.version &&
               ts == other.ts;
    }
};

class Collector : public osmium::handler::Handler {
public:
    void node(const osmium::Node& n) { add('n', n); }
    void way(const osmium::Way& w) { add('w', w); }
    void relation(const osmium::Relation& r) { add('r', r); }

    std::vector<ObjectKey> keys;

private:
    template <typename T>
    void add(char type, const T& object) {
        keys.push_back({type, object.id(), object.version(),
                        object.timestamp().seconds_since_epoch()});
    }
};

std::vector<ObjectKey> read_keys(const std::string& path) {
    osmium::io::File file(path);
    osmium::io::Reader reader(file);
    Collector collector;
    while (osmium::memory::Buffer buffer = reader.read()) {
        osmium::apply(buffer, collector);
    }
    reader.close();
    return collector.keys;
}

void write_osc(const std::string& path, const std::string& body) {
    std::ofstream out(path);
    out << body;
}

// Aggregated (cell, day) -> count across a root's ways.<seq>.parquet files.
std::map<std::tuple<uint64_t, uint32_t>, uint32_t> aggregate_ways(
    const std::string& root) {
    std::map<std::tuple<uint64_t, uint32_t>, uint32_t> out;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        const std::string name = entry.path().filename().string();
        if (name.rfind("ways.", 0) != 0 || entry.path().extension() != ".parquet") {
            continue;
        }
        auto combined = read_parquet(entry.path().string())->CombineChunks();
        if (!combined.ok()) throw std::runtime_error("CombineChunks failed");
        const auto& table = *combined;
        const auto* cells =
            static_cast<const arrow::UInt64Array*>(table->column(0)->chunk(0).get());
        const auto* days =
            static_cast<const arrow::UInt16Array*>(table->column(1)->chunk(0).get());
        const auto* counts =
            static_cast<const arrow::UInt32Array*>(table->column(2)->chunk(0).get());
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            out[{cells->Value(i), days->Value(i)}] += counts->Value(i);
        }
    }
    return out;
}

}  // namespace

TEST(DiffMerger, MergesDiffsInSequenceOrderAndTracksSpan) {
    TempDir dir;
    const std::string diff1 =
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<osmChange version=\"0.6\" generator=\"test\">\n"
        "  <create>\n"
        "    <node id=\"1\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " lat=\"0.02\" lon=\"0.02\"/>\n"
        "    <way id=\"100\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " uid=\"1\" user=\"u\"><nd ref=\"1\"/></way>\n"
        "  </create>\n"
        "  <modify>\n"
        "    <node id=\"2\" version=\"2\" timestamp=\"2024-01-01T00:01:00Z\""
        " uid=\"2\" user=\"v\" lat=\"0.03\" lon=\"0.03\"/>\n"
        "  </modify>\n"
        "</osmChange>\n";
    const std::string diff2 =
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<osmChange version=\"0.6\" generator=\"test\">\n"
        "  <modify>\n"
        "    <node id=\"2\" version=\"3\" timestamp=\"2024-01-01T00:02:00Z\""
        " uid=\"2\" user=\"v\" lat=\"0.04\" lon=\"0.04\"/>\n"
        "  </modify>\n"
        "  <delete>\n"
        "    <node id=\"1\" version=\"2\" timestamp=\"2024-01-01T00:03:00Z\""
        " uid=\"1\" user=\"u\"/>\n"
        "    <relation id=\"300\" version=\"1\" timestamp=\"2024-01-01T00:03:00Z\">"
        "<member type=\"way\" ref=\"100\" role=\"\"/></relation>\n"
        "  </delete>\n"
        "</osmChange>\n";
    write_osc(dir.join("1.osc"), diff1);
    write_osc(dir.join("2.osc"), diff2);

    const std::string merged = dir.join("merged.osc");
    update_pass::DiffMerger merger;
    merger.open(merged);
    merger.add(dir.join("1.osc"));
    merger.add(dir.join("2.osc"));
    EXPECT_TRUE(merger.has_ts());
    EXPECT_EQ(merger.min_ts(), 1704067200);  // 2024-01-01T00:00:00Z
    EXPECT_EQ(merger.max_ts(), 1704067380);  // 2024-01-01T00:03:00Z
    merger.finish();
    EXPECT_FALSE(merger.is_open());

    const std::vector<ObjectKey> want = {
        {'n', 1, 1, 1704067200},
        {'w', 100, 1, 1704067200},
        {'n', 2, 2, 1704067260},
        {'n', 2, 3, 1704067320},
        {'n', 1, 2, 1704067380},
        {'r', 300, 1, 1704067380},
    };
    EXPECT_EQ(read_keys(merged), want);
}

TEST(DiffMerger, OverwriteTruncatesLeftoverMergedFile) {
    TempDir dir;
    const std::string merged = dir.join("merged.osc");
    write_osc(merged, "garbage that is not a valid osmChange file\n");

    update_pass::DiffMerger merger;
    merger.open(merged);
    merger.finish();

    // open()+finish() with no diffs yields an empty-but-valid osmChange; the
    // leftover text must be gone (no throw on reparse).
    EXPECT_NO_THROW({ read_keys(merged); });
    EXPECT_TRUE(read_keys(merged).empty());
}

TEST(DiffMerger, BatchNodeCountsEqualSumOfPerDiff) {
    TempDir dir;
    const std::string cache = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(cache, 9);
        w.finish();
    }

    const std::string diff1 =
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<osmChange version=\"0.6\" generator=\"test\">\n"
        "  <create>\n"
        "    <node id=\"5\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " lat=\"0.02\" lon=\"0.02\"/>\n"
        "  </create>\n"
        "  <modify>\n"
        "    <node id=\"5\" version=\"2\" timestamp=\"2024-01-01T00:01:00Z\""
        " lat=\"0.03\" lon=\"0.03\"/>\n"
        "  </modify>\n"
        "</osmChange>\n";
    const std::string diff2 =
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<osmChange version=\"0.6\" generator=\"test\">\n"
        "  <delete>\n"
        "    <node id=\"5\" version=\"3\" timestamp=\"2024-01-01T00:02:00Z\""
        " uid=\"1\" user=\"u\"/>\n"
        "  </delete>\n"
        "</osmChange>\n";
    write_osc(dir.join("1.osc"), diff1);
    write_osc(dir.join("2.osc"), diff2);

    const std::string merged = dir.join("merged.osc");
    update_pass::DiffMerger merger;
    merger.open(merged);
    merger.add(dir.join("1.osc"));
    merger.add(dir.join("2.osc"));
    merger.finish();

    // Per-diff passes into one dataset.
    const std::string sep_root = dir.join("changes_sep");
    std::filesystem::create_directories(sep_root);
    {
        update_pass::NodeState state(cache, 9);
        update_pass::run_node_update(dir.join("1.osc"), sep_root, 1, 9, &state);
        update_pass::run_node_update(dir.join("2.osc"), sep_root, 2, 9, &state);
    }

    // One merged pass, staged under the batch's last sequence.
    const std::string batched_root = dir.join("changes_batch");
    std::filesystem::create_directories(batched_root);
    {
        update_pass::NodeState state(cache, 9);
        update_pass::run_node_update(merged, batched_root, 2, 9, &state);
    }

    // Aggregated (cell, day) -> count across a root's nodes.*.parquet files.
    const auto aggregate = [&](const std::string& root) {
        std::map<std::tuple<uint64_t, uint32_t>, uint32_t> out;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
            if (!entry.is_regular_file()) continue;
            const std::string name = entry.path().filename().string();
            if (name.rfind("nodes.", 0) != 0 || entry.path().extension() != ".parquet") {
                continue;
            }
            auto combined = read_parquet(entry.path().string())->CombineChunks();
            if (!combined.ok()) throw std::runtime_error("CombineChunks failed");
            const auto& table = *combined;
            const auto* cells =
                static_cast<const arrow::UInt64Array*>(table->column(0)->chunk(0).get());
            const auto* days =
                static_cast<const arrow::UInt16Array*>(table->column(1)->chunk(0).get());
            const auto* counts =
                static_cast<const arrow::UInt32Array*>(table->column(2)->chunk(0).get());
            for (int64_t i = 0; i < table->num_rows(); ++i) {
                out[{cells->Value(i), days->Value(i)}] += counts->Value(i);
            }
        }
        return out;
    };

    const auto want = aggregate(sep_root);
    ASSERT_EQ(want.size(), 2u);  // create cell; modify and delete share the last cell
    EXPECT_EQ(aggregate(batched_root), want);
}

// Documents the accepted end-of-batch way geometry: within one merged batch
// all ways resolve against the batch's final node positions, unlike per-diff
// processing.
TEST(DiffMerger, BatchWayGeometryResolvesAtEndOfBatch) {
    TempDir dir;
    const std::string cache = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(cache, 9);
        w.finish();
    }

    // diff 1: node 10 and way 100; node 11 and way 101.
    const std::string diff1 =
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<osmChange version=\"0.6\" generator=\"test\">\n"
        "  <create>\n"
        "    <node id=\"10\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " lat=\"0.02\" lon=\"0.02\"/>\n"
        "    <way id=\"100\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " uid=\"1\" user=\"u\"><nd ref=\"10\"/></way>\n"
        "    <node id=\"11\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " lat=\"0.04\" lon=\"0.04\"/>\n"
        "    <way id=\"101\" version=\"1\" timestamp=\"2024-01-01T00:00:00Z\""
        " uid=\"1\" user=\"u\"><nd ref=\"11\"/></way>\n"
        "  </create>\n"
        "</osmChange>\n";
    // diff 2: node 10 moves; node 11 is deleted, taking way 101 with it.
    const std::string diff2 =
        "<?xml version='1.0' encoding='UTF-8'?>\n"
        "<osmChange version=\"0.6\" generator=\"test\">\n"
        "  <modify>\n"
        "    <node id=\"10\" version=\"2\" timestamp=\"2024-01-01T00:30:00Z\""
        " lat=\"0.03\" lon=\"0.03\"/>\n"
        "  </modify>\n"
        "  <delete>\n"
        "    <node id=\"11\" version=\"2\" timestamp=\"2024-01-01T00:30:00Z\""
        " uid=\"1\" user=\"u\"/>\n"
        "    <way id=\"101\" version=\"2\" timestamp=\"2024-01-01T00:30:00Z\""
        " uid=\"1\" user=\"u\"><nd ref=\"11\"/></way>\n"
        "  </delete>\n"
        "</osmChange>\n";
    write_osc(dir.join("1.osc"), diff1);
    write_osc(dir.join("2.osc"), diff2);

    // Sequential per-diff passes with a shared state: the pre-batching view.
    const std::string seq_root = dir.join("changes_seq");
    std::filesystem::create_directories(seq_root);
    {
        update_pass::NodeState state(cache, 9);
        update_pass::run_node_update(dir.join("1.osc"), seq_root, 1, 9, &state);
        update_pass::run_way_update(dir.join("1.osc"), seq_root, 1, state,
                                    kDefaultWayBatchBytes);
        update_pass::run_node_update(dir.join("2.osc"), seq_root, 2, 9, &state);
        update_pass::run_way_update(dir.join("2.osc"), seq_root, 2, state,
                                    kDefaultWayBatchBytes);
    }

    // One merged batch: the whole batch's node positions fold in first.
    const std::string merged_root = dir.join("changes_merged");
    std::filesystem::create_directories(merged_root);
    const std::string merged = dir.join("merged.osc");
    {
        update_pass::DiffMerger merger;
        merger.open(merged);
        merger.add(dir.join("1.osc"));
        merger.add(dir.join("2.osc"));
        merger.finish();
        update_pass::NodeState state(cache, 9);
        update_pass::run_node_update(merged, merged_root, 2, 9, &state);
        update_pass::run_way_update(merged, merged_root, 2, state, kDefaultWayBatchBytes);
    }

    const uint64_t cell_a = h3_utils::location_to_cell(0.02, 0.02, 9);
    const uint64_t cell_b = h3_utils::location_to_cell(0.03, 0.03, 9);
    const uint64_t cell_c = h3_utils::location_to_cell(0.04, 0.04, 9);
    const uint32_t day1 = 19723;  // 2024-01-01 UTC

    // Sequential: way 100 counts node 10 at diff 1's position, way 101 at
    // diff 1's node 11.
    const std::map<std::tuple<uint64_t, uint32_t>, uint32_t> want_seq = {
        {{cell_a, day1}, 1}, {{cell_c, day1}, 1}};
    EXPECT_EQ(aggregate_ways(seq_root), want_seq);

    // Merged: way 100 sees node 10's end-of-batch position; way 101 is gone
    // (node 11 created then deleted in the batch).
    const std::map<std::tuple<uint64_t, uint32_t>, uint32_t> want_batch = {
        {{cell_b, day1}, 1}};
    EXPECT_EQ(aggregate_ways(merged_root), want_batch);
}