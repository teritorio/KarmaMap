#pragma once

// The node-position delta sidecar: the accumulated changes an update run has
// applied on top of the flat incremental cache (its base) since the base was
// last rewritten. Persisting them here lets each run write O(changes) instead
// of rewriting the whole base; the base is rebuilt from base + delta and the
// sidecar cleared only when the delta grows past the compaction threshold.
//
//   header: [magic 4B][version 4B][h3 resolution 4B]
//           [modified count 8B][deleted count 8B]                  (28 bytes)
//   body:   modified_count * [node_id 8B + packed h3 cell 6B]      (14 bytes)
//           deleted_count  * [node_id 8B]                           (8 bytes)
//
// node_id is big-endian sign-flipped (node_cache::encode_node) so the records
// sort numerically; the cell is the 6-byte packed form (h3_utils::pack_cell).
// The two sets are disjoint (a node is either created/modified or deleted) and
// each is written in ascending node order. Written to <path>.tmp and renamed
// over <path>, so a crash never leaves a torn sidecar.

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "h3_utils.hpp"
#include "node_cache.hpp"

namespace node_cache {
namespace delta {

constexpr uint32_t kMagic = 0x494E4344;  // "INCD"
constexpr uint32_t kVersion = 1;
constexpr size_t kHeaderSize = 28;
constexpr size_t kModifiedRecordSize = 14;  // node_id (8) + packed cell (6)
constexpr size_t kDeletedRecordSize = 8;    // node_id (8)

inline void put_be32(char* dst, uint32_t v) {
    dst[0] = static_cast<char>(v >> 24);
    dst[1] = static_cast<char>(v >> 16);
    dst[2] = static_cast<char>(v >> 8);
    dst[3] = static_cast<char>(v & 0xFF);
}

inline uint32_t be32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

inline uint64_t be64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
    return v;
}

struct State {
    std::unordered_map<int64_t, uint64_t> modified;  // node id -> last known h3 cell
    std::unordered_map<int64_t, uint8_t> deleted;    // node id -> 1
};

// Writes the sidecar atomically (tmp + rename). `modified` and `deleted` keys
// must be disjoint.
inline void write(const std::string& path, int h3_resolution,
                  const std::unordered_map<int64_t, uint64_t>& modified,
                  const std::unordered_map<int64_t, uint8_t>& deleted) {
    const std::string tmp = path + ".tmp";
    const std::filesystem::path dir = std::filesystem::path(path).parent_path();
    if (!dir.empty()) std::filesystem::create_directories(dir);

    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to create node delta " + tmp);
    }
    try {
        char header[kHeaderSize] = {};
        put_be32(header, kMagic);
        put_be32(header + 4, kVersion);
        put_be32(header + 8, static_cast<uint32_t>(h3_resolution));
        put_be64(header + 12, static_cast<uint64_t>(modified.size()));
        put_be64(header + 20, static_cast<uint64_t>(deleted.size()));
        ensure_write(fd, header, sizeof(header), "node delta header");

        std::vector<int64_t> nodes;
        nodes.reserve(modified.size());
        for (const auto& entry : modified) nodes.push_back(entry.first);
        std::sort(nodes.begin(), nodes.end());
        for (int64_t node : nodes) {
            char rec[kModifiedRecordSize];
            put_be64(rec, encode_node(node));
            put_cell6(rec + 8, h3_utils::pack_cell(modified.at(node), h3_resolution));
            ensure_write(fd, rec, sizeof(rec), "node delta modified record");
        }

        nodes.clear();
        nodes.reserve(deleted.size());
        for (const auto& entry : deleted) nodes.push_back(entry.first);
        std::sort(nodes.begin(), nodes.end());
        for (int64_t node : nodes) {
            char rec[kDeletedRecordSize];
            put_be64(rec, encode_node(node));
            ensure_write(fd, rec, sizeof(rec), "node delta deleted record");
        }

        if (::fsync(fd) != 0) {
            throw std::runtime_error("Failed to fsync node delta");
        }
        ::close(fd);
    } catch (...) {
        ::close(fd);
        std::error_code ec;
        std::filesystem::remove(tmp, ec);
        throw;
    }
    std::filesystem::rename(tmp, path);
}

// Loads the sidecar, or returns an empty State when the path does not exist
// (the base then carries the whole node state). Throws on a malformed file or
// an h3-resolution mismatch.
inline State load_or_empty(const std::string& path, int expected_h3_resolution) {
    State state;
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) return state;

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open node delta " + path);
    }

    uint8_t header[kHeaderSize];
    in.read(reinterpret_cast<char*>(header),
            static_cast<std::streamsize>(sizeof(header)));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(header))) {
        throw std::runtime_error("Node delta " + path + " too small");
    }
    const uint32_t magic = be32(header);
    const uint32_t version = be32(header + 4);
    const int h3_resolution = static_cast<int>(be32(header + 8));
    const uint64_t modified_count = be64(header + 12);
    const uint64_t deleted_count = be64(header + 20);
    if (magic != kMagic || version != kVersion) {
        throw std::runtime_error("Incompatible node delta " + path);
    }
    if (h3_resolution != expected_h3_resolution) {
        throw std::runtime_error(
            "Node delta " + path + " was built at H3 resolution " +
            std::to_string(h3_resolution) + ", but " +
            std::to_string(expected_h3_resolution) + " is requested");
    }

    state.modified.reserve(modified_count);
    state.deleted.reserve(deleted_count);
    std::vector<uint8_t> rec(kModifiedRecordSize);
    for (uint64_t i = 0; i < modified_count; ++i) {
        in.read(reinterpret_cast<char*>(rec.data()),
                static_cast<std::streamsize>(kModifiedRecordSize));
        if (in.gcount() != static_cast<std::streamsize>(kModifiedRecordSize)) {
            throw std::runtime_error("Node delta " + path + " truncated");
        }
        const int64_t node = decode_node(rec.data());
        state.modified[node] =
            h3_utils::unpack_cell(read_cell6(rec.data() + 8), h3_resolution);
    }
    char nrec[kDeletedRecordSize];
    for (uint64_t i = 0; i < deleted_count; ++i) {
        in.read(nrec, static_cast<std::streamsize>(kDeletedRecordSize));
        if (in.gcount() != static_cast<std::streamsize>(kDeletedRecordSize)) {
            throw std::runtime_error("Node delta " + path + " truncated");
        }
        state.deleted[decode_node(reinterpret_cast<const uint8_t*>(nrec))] = 1;
    }
    return state;
}

}  // namespace delta
}  // namespace node_cache
