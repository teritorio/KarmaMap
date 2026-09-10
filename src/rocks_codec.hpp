#pragma once

// Compact encoding for the RocksDB entries of the node-position cache.
//
// Key   : node_id (int64, big-endian, 8B) + version (uint32, big-endian, 4B)
//         Big-endian byte order matches numeric order, which allows a
//         prefix scan on node_id with versions returned in increasing order.
//
// Value : timestamp (int64, 8B) + lat_e7 (int32, 4B) + lon_e7 (int32, 4B)
//         Coordinates stored as fixed-point integers (1e-7 degree, OSM's
//         native precision).

#include <cstdint>
#include <string>

namespace rocks_codec {

constexpr size_t kKeySize = 12;
constexpr size_t kValueSize = 16;

inline void put_be64(char* dst, uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        dst[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

inline void put_be32(char* dst, uint32_t v) {
    for (int i = 3; i >= 0; --i) {
        dst[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

inline uint64_t get_be64(const char* src) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(src[i]);
    return v;
}

inline uint32_t get_be32(const char* src) {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v = (v << 8) | static_cast<uint8_t>(src[i]);
    return v;
}

inline std::string encode_key(int64_t node_id, uint32_t version) {
    std::string out(kKeySize, '\0');
    put_be64(out.data(), static_cast<uint64_t>(node_id));
    put_be32(out.data() + 8, version);
    return out;
}

// Scan prefix: the node_id alone (version=0 acts as the lower bound).
inline std::string encode_prefix(int64_t node_id) { return encode_key(node_id, 0); }

struct DecodedKey {
    int64_t node_id;
    uint32_t version;
};

inline DecodedKey decode_key(const char* data, size_t /*size*/) {
    DecodedKey k;
    k.node_id = static_cast<int64_t>(get_be64(data));
    k.version = get_be32(data + 8);
    return k;
}

struct NodeVersionRecord {
    int64_t timestamp;  // epoch seconds, UTC
    int32_t lat_e7;
    int32_t lon_e7;

    double lat() const { return static_cast<double>(lat_e7) / 1e7; }
    double lon() const { return static_cast<double>(lon_e7) / 1e7; }
};

inline std::string encode_value(int64_t timestamp, double lat, double lon) {
    std::string out(kValueSize, '\0');
    put_be64(out.data(), static_cast<uint64_t>(timestamp));
    put_be32(out.data() + 8, static_cast<uint32_t>(static_cast<int32_t>(lat * 1e7)));
    put_be32(out.data() + 12, static_cast<uint32_t>(static_cast<int32_t>(lon * 1e7)));
    return out;
}

inline NodeVersionRecord decode_value(const char* data, size_t /*size*/) {
    NodeVersionRecord r;
    r.timestamp = static_cast<int64_t>(get_be64(data));
    r.lat_e7 = static_cast<int32_t>(get_be32(data + 8));
    r.lon_e7 = static_cast<int32_t>(get_be32(data + 12));
    return r;
}

}  // namespace rocks_codec
