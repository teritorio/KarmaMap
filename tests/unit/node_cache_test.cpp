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

namespace {

using test_helpers::TempDir;

// A well-formed H3 cell at resolution 9 whose lower 46 bits hold `v`;
// pack_cell()/unpack_cell() round-trip this form exactly (H3 mode bit 59 is
// set, resolution lives in bits 52..55, sentinel 0x3F fills unused digits
// in bits 0..5).
uint64_t cell(uint64_t v) {
    return (1ULL << 59) | (9ULL << 52) | (v << 6) | 0x3Full;
}

// Reads `len` bytes at `offset` from a raw file.
std::vector<uint8_t> read_bytes(const std::string& path, size_t offset, size_t len) {
    std::ifstream in(path, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(offset));
    std::vector<uint8_t> out(len);
    in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamoff>(len));
    if (in.gcount() < static_cast<std::streamoff>(len)) {
        throw std::runtime_error("Short read from " + path);
    }
    return out;
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

// ---------------------------------------------------------------------------
// Pure encode/decode helpers (all inline, no file IO).
// ---------------------------------------------------------------------------

TEST(NodeCache, EncodeNodeSignFlip) {
    EXPECT_EQ(node_cache::encode_node(-3), 0x7FFFFFFFFFFFFFFDULL);
    EXPECT_EQ(node_cache::encode_node(-1), 0x7FFFFFFFFFFFFFFFULL);
    EXPECT_EQ(node_cache::encode_node(0), 0x8000000000000000ULL);
    EXPECT_EQ(node_cache::encode_node(1), 0x8000000000000001ULL);
    EXPECT_EQ(node_cache::encode_node(INT64_MAX), 0xFFFFFFFFFFFFFFFFULL);
}

TEST(NodeCache, ByteOrderGoldens) {
    char buf[16] = {};
    node_cache::put_be16(buf, 0x1234);
    EXPECT_EQ(static_cast<uint8_t>(buf[0]), 0x12);
    EXPECT_EQ(static_cast<uint8_t>(buf[1]), 0x34);
    EXPECT_EQ(node_cache::decode_day(reinterpret_cast<const uint8_t*>(buf)), 0x1234);

    node_cache::put_be64(buf, 0x0123456789ABCDEFULL);
    const uint8_t expect_be64[8] = {0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF};
    EXPECT_EQ(std::memcmp(buf, expect_be64, 8), 0);
}

TEST(NodeCache, DecodeNodeRoundTrip) {
    const int64_t values[] = {INT64_MIN, -1, 0, 1, 7, INT64_MAX};
    for (int64_t v : values) {
        char rec[8];
        node_cache::put_be64(rec, node_cache::encode_node(v));
        EXPECT_EQ(node_cache::decode_node(reinterpret_cast<const uint8_t*>(rec)), v);
    }
}

TEST(NodeCache, Cell6RoundTrip) {
    // put_cell6 stores only the low 6 bytes; values must fit 48 bits.
    const uint64_t values[] = {0, 1, 0x1234567890ABULL, 0xFFFFFFFFFFFFULL};
    for (uint64_t v : values) {
        char buf[6] = {};
        node_cache::put_cell6(buf, v);
        EXPECT_EQ(node_cache::read_cell6(reinterpret_cast<const uint8_t*>(buf)), v);
    }
}

TEST(NodeCache, PutCell6LittleEndian) {
    char buf[6] = {};
    node_cache::put_cell6(buf, 0x010203040506ULL);
    const uint8_t expect[6] = {0x06, 0x05, 0x04, 0x03, 0x02, 0x01};
    EXPECT_EQ(std::memcmp(buf, expect, 6), 0);
}

// ---------------------------------------------------------------------------
// Writer -> Reader round-trips against a real file.
// ---------------------------------------------------------------------------

TEST(NodeCache, SingleBlockRoundTrip) {
    TempDir dir;
    const std::string path = dir.join("cache.bin");
    {
        node_cache::Writer w(path, 9);
        w.add(10, 5, cell(0x123456));  // day 5
        w.add(10, 7, cell(0xABCDEF));  // day 7 (same node, later day)
        w.add(20, 3, cell(0xBEEF));    // different node, earlier day
        w.finish();
        EXPECT_EQ(w.records(), 3);
    }

    node_cache::Reader r(path, 9);
    EXPECT_EQ(r.size(), 3);

    // Direct record access.
    EXPECT_EQ(r.node_at(0), 10);
    EXPECT_EQ(r.day_at(1), 7);
    EXPECT_EQ(r.cell_at(2), cell(0xBEEF));
}

TEST(NodeCache, DayCollapseLastWins) {
    TempDir dir;
    const std::string path = dir.join("cache.bin");
    {
        node_cache::Writer w(path, 9);
        w.add(1, 42, cell(0x111111));
        w.add(1, 42, cell(0x222222));  // same (node, day): last version wins
        w.finish();
        EXPECT_EQ(w.records(), 1);
    }

    node_cache::Reader r(path, 9);
    EXPECT_EQ(r.size(), 1);
    EXPECT_EQ(r.cell_at(0), cell(0x222222));
}

TEST(NodeCache, OrderingViolationsThrow) {
    TempDir dir;

    // day decreases for the same node.
    {
        node_cache::Writer w(dir.join("a.bin"), 9);
        w.add(1, 10, cell(1));
        EXPECT_THROW(w.add(1, 5, cell(2)), std::runtime_error);
    }

    // node id decreases.
    {
        node_cache::Writer w(dir.join("b.bin"), 9);
        w.add(5, 10, cell(1));
        EXPECT_THROW(w.add(3, 10, cell(2)), std::runtime_error);
    }

    // Day bounds within uint16 range are accepted.
    {
        node_cache::Writer w(dir.join("c.bin"), 9);
        w.add(1, 0, cell(1));
        w.add(1, 65535, cell(2));
        w.finish();
        EXPECT_EQ(w.records(), 2);
    }
}

TEST(NodeCache, FinishPatchesHeader) {
    TempDir dir;
    const std::string path = dir.join("cache.bin");
    uint64_t writer_bytes = 0;
    {
        node_cache::Writer w(path, 9);
        w.add(10, 5, cell(0x123456));
        w.add(20, 3, cell(0xBEEF));
        w.finish();
        EXPECT_EQ(w.records(), 2);
        writer_bytes = w.bytes();  // header + compressed blocks + directory
    }

    const auto header = read_bytes(path, 0, node_cache::kHeaderSize);
    EXPECT_EQ(std::memcmp(header.data(), "OSNC", 4), 0);
    EXPECT_EQ(be32(header, 4), node_cache::kVersion);
    EXPECT_EQ(be32(header, 8), node_cache::kRecordSize);
    EXPECT_EQ(be32(header, 12), 9);  // h3 resolution
    EXPECT_EQ(be64(header, 16), 2);  // record count
    EXPECT_EQ(be64(header, 24), 1);  // block count
    EXPECT_EQ(be32(header, 32), node_cache::kRecordsPerBlock);
    EXPECT_EQ(be32(header, 36), node_cache::kCompressionZstd);

    // Writer's byte accounting matches the file exactly.
    EXPECT_EQ(std::filesystem::file_size(path), writer_bytes);
}

TEST(NodeCache, CorruptionPathsThrow) {
    TempDir dir;

    // File does not exist.
    EXPECT_THROW(node_cache::Reader(dir.join("nope.bin"), 9), std::runtime_error);

    // Wrong magic.
    {
        const std::string path = dir.join("bad_magic.bin");
        {
            node_cache::Writer w(path, 9);
            w.add(1, 1, cell(1));
            w.finish();
        }
        std::ofstream out(path, std::ios::binary | std::ios::in | std::ios::ate);
        out.seekp(0);
        out.write("XXXX", 4);
        out.close();
        EXPECT_THROW(node_cache::Reader(path, 9), std::runtime_error);
    }

    // Header truncation.
    {
        const std::string path = dir.join("truncated.bin");
        {
            node_cache::Writer w(path, 9);
            w.add(1, 1, cell(1));
            w.add(2, 2, cell(2));
            w.finish();
        }
        const auto size = std::filesystem::file_size(path);
        std::filesystem::resize_file(path, size / 2);
        EXPECT_THROW(node_cache::Reader(path, 9), std::runtime_error);
    }

    // Resolution mismatch.
    {
        const std::string path = dir.join("res_mismatch.bin");
        {
            node_cache::Writer w(path, 7);
            w.add(1, 1, cell(1));
            w.finish();
        }
        EXPECT_THROW(node_cache::Reader(path, 9), std::runtime_error);
    }
}

// Heavy: writes 2^18+1 records so the first block fills (2^18 records) and a
// second partial block is flushed; verifies the sweep crossing + record
// access across the decompression-cache boundary.
TEST(NodeCache, MultiBlockRoundTrip) {
    TempDir dir;
    const std::string path = dir.join("big.bin");
    const size_t kRecords = (size_t{1} << 18) + 1;
    {
        node_cache::Writer w(path, 9);
        for (size_t i = 0; i < kRecords; ++i) {
            w.add(static_cast<int64_t>(i), 1, cell(i));
        }
        w.finish();
        EXPECT_EQ(w.records(), kRecords);
    }

    node_cache::Reader r(path, 9);
    EXPECT_EQ(r.size(), kRecords);

    // The sweep may begin anywhere before a node reached through a new
    // block; starting at record 0 crosses the block boundary to reach it.
    EXPECT_EQ(r.sweep_start(static_cast<int64_t>(kRecords - 1)), 0);
    // Forces decompression of block 1 through the caching record accessor.
    EXPECT_EQ(r.node_at(kRecords - 1), static_cast<int64_t>(kRecords - 1));
}

}  // namespace