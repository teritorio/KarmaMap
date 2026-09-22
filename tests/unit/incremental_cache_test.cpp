#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "node_cache.hpp"
#include "test_helpers.hpp"
#include "update.hpp"

namespace {

using test_helpers::TempDir;

// A well-formed H3 cell at resolution 9 (same shape as node_cache_test).
uint64_t cell(uint64_t v) {
    return (1ULL << 59) | (9ULL << 52) | (v << 6) | 0x3Full;
}

uint32_t be32(const std::vector<uint8_t>& b, size_t off) {
    return (static_cast<uint32_t>(b[off]) << 24) | (static_cast<uint32_t>(b[off + 1]) << 16) |
           (static_cast<uint32_t>(b[off + 2]) << 8) | static_cast<uint32_t>(b[off + 3]);
}

uint64_t be64(const std::vector<uint8_t>& b, size_t off) {
    uint64_t v = 0;
    for (size_t i = 0; i < 8; ++i) v = (v << 8) | b[off + i];
    return v;
}

std::vector<uint8_t> read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    in.seekg(0, std::ios::end);
    const std::streamoff len = in.tellg();
    in.seekg(0);
    std::vector<uint8_t> out(static_cast<size_t>(len));
    in.read(reinterpret_cast<char*>(out.data()), len);
    return out;
}

TEST(IncrementalCache, LastVersionWins) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0x111111));
        w.add(10, cell(0x222222));  // same node, later version wins
        w.add(10, cell(0x333333));
        EXPECT_EQ(w.records(), 1);
        w.finish();
    }

    node_cache::incremental::Reader r(path, 9);
    EXPECT_EQ(r.size(), 1);
    EXPECT_EQ(r.node_at(0), 10);
    EXPECT_EQ(r.cell_at(0), cell(0x333333));
    EXPECT_EQ(r.lookup(10), cell(0x333333));
    EXPECT_EQ(r.lookup(999), 0);  // absent node
}

TEST(IncrementalCache, MultiNodeOrdering) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(1, cell(1));
        w.add(2, cell(2));
        w.add(2, cell(3));  // last version wins
        w.add(5, cell(4));
        EXPECT_EQ(w.records(), 3);
        w.finish();
    }

    node_cache::incremental::Reader r(path, 9);
    EXPECT_EQ(r.size(), 3);
    EXPECT_EQ(r.node_at(0), 1);
    EXPECT_EQ(r.node_at(1), 2);
    EXPECT_EQ(r.node_at(2), 5);
    EXPECT_EQ(r.lookup(1), cell(1));
    EXPECT_EQ(r.lookup(2), cell(3));
    EXPECT_EQ(r.lookup(5), cell(4));
    EXPECT_EQ(r.lookup(3), 0);  // between nodes sorts past the match
}

TEST(IncrementalCache, UnsortedInputThrows) {
    TempDir dir;
    node_cache::incremental::Writer w(dir.join("incr.bin"), 9);
    w.add(5, cell(1));
    EXPECT_THROW(w.add(3, cell(2)), std::runtime_error);
}

TEST(IncrementalCache, FinishPatchesAndRenames) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0x123456));
        w.add(20, cell(0xBEEF));
        w.finish();
    }

    // File is in place under the final name, tmp is gone.
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));

    const auto data = read_file(path);
    EXPECT_EQ(std::memcmp(data.data(), "INCC", 4), 0);
    EXPECT_EQ(be32(data, 4), node_cache::incremental::kVersion);
    EXPECT_EQ(be32(data, 8), node_cache::incremental::kRecordSize);
    EXPECT_EQ(be32(data, 12), 9);  // h3 resolution
    EXPECT_EQ(be64(data, 16), 2);  // record count
    EXPECT_EQ(be64(data, 24), 1);  // block count
    EXPECT_EQ(be32(data, 32), node_cache::incremental::kRecordsPerBlock);
    EXPECT_EQ(be32(data, 36), node_cache::incremental::kCompressionZstd);
}

TEST(IncrementalCache, ReplacesPreviousFile) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(1, cell(1));
        w.finish();
    }
    // A second run rebuilds over the same path without leaving the old one.
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(1, cell(2));
        w.add(2, cell(3));
        w.finish();
    }
    node_cache::incremental::Reader r(path, 9);
    EXPECT_EQ(r.size(), 2);
    EXPECT_EQ(r.lookup(1), cell(2));
    EXPECT_EQ(r.lookup(2), cell(3));
}

TEST(IncrementalCache, ReaderMissingFileThrows) {
    TempDir dir;
    EXPECT_THROW(node_cache::incremental::Reader(dir.join("nope.bin"), 9),
                 std::runtime_error);
}

// Heavy: fills a first block and spills a second; verifies lookup crosses the
// decompression-cache boundary and still resolves the last node.
TEST(IncrementalCache, MultiBlockRoundTrip) {
    TempDir dir;
    const std::string path = dir.join("big.bin");
    const size_t kRecords = (size_t{1} << 18) + 1;
    {
        node_cache::incremental::Writer w(path, 9);
        for (size_t i = 0; i < kRecords; ++i) {
            w.add(static_cast<int64_t>(i), cell(i));
        }
        w.finish();
        EXPECT_EQ(w.records(), kRecords);
    }

    node_cache::incremental::Reader r(path, 9);
    EXPECT_EQ(r.size(), kRecords);
    EXPECT_EQ(r.node_at(kRecords - 1), static_cast<int64_t>(kRecords - 1));
    EXPECT_EQ(r.cell_at(kRecords - 1), cell(kRecords - 1));
    EXPECT_EQ(r.lookup(static_cast<int64_t>(kRecords - 1)), cell(kRecords - 1));
    EXPECT_EQ(r.sweep_start(static_cast<int64_t>(kRecords - 1)), 0);
}

// Step 4's transform: feed every record of a (node_id, day)-sorted history
// cache into the collapsing writer; each node keeps only its last cell.
TEST(IncrementalCache, CollapsesHistoryCache) {
    TempDir dir;
    const std::string hist = dir.join("history.bin");
    const std::string incr = dir.join("incr.bin");
    {
        node_cache::Writer w(hist, 9);
        w.add(1, 5, cell(0xAAAA));
        w.add(1, 7, cell(0xBBBB));  // later day, last version
        w.add(2, 3, cell(0xCCCC));
        w.add(3, 1, cell(0xDDDD));
        w.add(3, 4, cell(0xEEEE));
        w.add(3, 9, cell(0xFFFF));
        w.finish();
        EXPECT_EQ(w.records(), 6);
    }

    {
        node_cache::Reader hist_reader(hist, 9);
        node_cache::incremental::Writer w(incr, 9);
        const size_t n = hist_reader.size();
        for (size_t i = 0; i < n; ++i) {
            w.add(hist_reader.node_at(i), hist_reader.cell_at(i));
        }
        EXPECT_EQ(w.records(), 3);
        w.finish();
    }

    node_cache::incremental::Reader r(incr, 9);
    EXPECT_EQ(r.size(), 3);
    EXPECT_EQ(r.lookup(1), cell(0xBBBB));
    EXPECT_EQ(r.lookup(2), cell(0xCCCC));
    EXPECT_EQ(r.lookup(3), cell(0xFFFF));
}

// ---------------------------------------------------------------------------
// update_pass::NodeState (diff overlay over the flat incremental cache)
// ---------------------------------------------------------------------------

TEST(NodeState, ResolvesAgainstBaseCache) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0xAAAA));
        w.add(20, cell(0xBBBB));
        w.finish();
    }
    update_pass::NodeState state(path, 9);
    EXPECT_EQ(state.pre(10), cell(0xAAAA));
    EXPECT_EQ(state.post(10), cell(0xAAAA));
    EXPECT_EQ(state.pre(20), cell(0xBBBB));
    EXPECT_EQ(state.pre(999), 0);  // unknown node
    EXPECT_EQ(state.overlay_size(), 0);
    EXPECT_EQ(state.deleted_size(), 0);
}

TEST(NodeState, CreatedNodeOverlaysBase) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0xAAAA));
        w.finish();
    }
    update_pass::NodeState state(path, 9);
    state.set_position(10, cell(0xCCCC));  // modification
    state.set_position(20, cell(0xDDDD));  // creation
    EXPECT_EQ(state.pre(10), cell(0xCCCC));
    EXPECT_EQ(state.post(10), cell(0xCCCC));
    EXPECT_EQ(state.post(20), cell(0xDDDD));
    EXPECT_EQ(state.overlay_size(), 2);
}

// Vandalism filter 3 must read a modified node's prior cell BEFORE
// set_position folds the new one into the overlay; this pins the ordering the
// update node handler relies on.
TEST(NodeState, PreBeforeSetPositionSeesOldCell) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0xAAAA));
        w.finish();
    }
    update_pass::NodeState state(path, 9);
    const uint64_t before = state.pre(10);  // must be the old cell
    state.set_position(10, cell(0xBBBB));
    EXPECT_EQ(before, cell(0xAAAA));
    EXPECT_EQ(state.pre(10), cell(0xBBBB));  // after: overlay wins
    EXPECT_EQ(state.post(10), cell(0xBBBB));
}

TEST(NodeState, DeletedNodePreKeepsLastCellPostIsZero) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0xAAAA));
        w.add(20, cell(0xBBBB));
        w.finish();
    }
    update_pass::NodeState state(path, 9);
    const uint64_t last = state.remove_node(10);
    EXPECT_EQ(last, cell(0xAAAA));  // pre-update view for deleted ways
    EXPECT_EQ(state.pre(10), cell(0xAAAA));  // last known cell kept for deleted-way resolution
    EXPECT_EQ(state.post(10), 0);
    EXPECT_EQ(state.deleted_size(), 1);
    EXPECT_EQ(state.overlay_size(), 0);
}

TEST(NodeState, SetPositionAfterDeleteReinstates) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(10, cell(0xAAAA));
        w.finish();
    }
    update_pass::NodeState state(path, 9);
    state.remove_node(10);
    state.set_position(10, cell(0xEEEE));  // deleted then recreated in the batch
    EXPECT_EQ(state.pre(10), cell(0xEEEE));
    EXPECT_EQ(state.post(10), cell(0xEEEE));
    EXPECT_EQ(state.deleted_size(), 0);
}

TEST(NodeState, RebuildMergesOverlayAndDropsDeleted) {
    TempDir dir;
    const std::string path = dir.join("incr.bin");
    const std::string out = dir.join("incr.last");
    {
        node_cache::incremental::Writer w(path, 9);
        w.add(1, cell(0xAAAA));
        w.add(2, cell(0xBBBB));
        w.add(5, cell(0xCCCC));
        w.finish();
    }
    {
        update_pass::NodeState state(path, 9);
        state.set_position(2, cell(0xDDDD));   // modify base node
        state.remove_node(5);                  // delete base node
        state.set_position(7, cell(0xEEEE));   // new node
        state.set_position(9, cell(0xFFFF));   // new node
        state.rebuild(out, 9);
    }
    node_cache::incremental::Reader r(out, 9);
    EXPECT_EQ(r.size(), 4);
    EXPECT_EQ(r.node_at(0), 1);
    EXPECT_EQ(r.node_at(1), 2);
    EXPECT_EQ(r.node_at(2), 7);
    EXPECT_EQ(r.node_at(3), 9);
    EXPECT_EQ(r.lookup(1), cell(0xAAAA));
    EXPECT_EQ(r.lookup(2), cell(0xDDDD));  // overridden
    EXPECT_EQ(r.lookup(5), 0);             // deleted
    EXPECT_EQ(r.lookup(7), cell(0xEEEE));  // interleaved sorted
    EXPECT_EQ(r.lookup(9), cell(0xFFFF));
}

}  // namespace
