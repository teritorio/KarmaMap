#pragma once

// The node-position cache: one file built by the node pass and read by the
// way pass through an mmap. 16-byte records sorted by (node_id, day)
// ascending, stored in ZSTD-compressed 2^18-record blocks (4 MiB raw)
// followed by a directory of per-block first node + compressed size:
//
//   record: [node_id 8B][day 2B][h3 cell 6B]        (16 bytes)
//   file:   [header 40B][block 0]...[block N-1][directory 12*N]
//
// node_id is big-endian with the sign bit flipped so the encoded bytes sort
// in numeric order, matching the (node_id, day) sort of the records. day is
// the uint16 UTC epoch-day value shared with change_date; the cell is packed
// 6-byte LE (h3_utils::pack_cell). Full layout details are in README "Node
// cache".

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <zstd.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "h3_utils.hpp"

namespace node_cache {

constexpr uint32_t kMagic = 0x4F534E43;  // "OSNC"
constexpr uint32_t kVersion = 1;
constexpr size_t kRecordSize = 16;  // node_id (8) + day (2) + h3 cell (6) packed
constexpr size_t kHeaderSize = 40;
constexpr size_t kRecordsPerBlock = 1 << 18;  // 262144 records = 4 MiB raw
constexpr size_t kLog2RecordsPerBlock = 18;
constexpr uint32_t kCompressionZstd = 1;
constexpr int kZstdLevel = 3;
constexpr size_t kDirectoryEntrySize = 12;  // first_node (8) + compressed_size (4)

inline bool ensure_write(int fd, const void* data, size_t len, const char* what) {
    const char* p = static_cast<const char*>(data);
    while (len > 0) {
        const ssize_t n = ::write(fd, p, len);
        if (n <= 0) {
            throw std::runtime_error(std::string("Failed to write ") + what);
        }
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Sign-flip + big-endian *decoded* compare helpers (the encoding itself is
// done inline when a record is laid out).
inline uint64_t encode_node(int64_t node_id) {
    return static_cast<uint64_t>(node_id) ^ (1ULL << 63);
}

inline int64_t decode_node(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
    return static_cast<int64_t>(v ^ (1ULL << 63));
}

inline uint16_t decode_day(const uint8_t* p) {
    return static_cast<uint16_t>(static_cast<uint16_t>(p[0]) << 8 | p[1]);
}

inline void put_be16(char* dst, uint16_t v) {
    dst[0] = static_cast<char>(v >> 8);
    dst[1] = static_cast<char>(v & 0xFF);
}

inline void put_be64(char* dst, uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        dst[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

inline void put_cell6(char* dst, uint64_t v) {
    for (int i = 0; i < 6; ++i) {
        dst[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

inline uint64_t read_cell6(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 5; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}

// Buffers records into 2^18-record blocks, ZSTD-compressed and written out
// as they fill. One pending record collapses consecutive same-(node_id, day)
// input (last version of a day wins) and enforces the ascending (node_id,
// day) order the sweep relies on; OSM full-history order makes days
// per node non-decreasing, so any violation aborts loudly.
class Writer {
public:
    Writer(const std::string& path, int h3_resolution)
        : h3_resolution_(h3_resolution) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to create node cache " + path);
        }
        char header[kHeaderSize] = {};
        put_be32(header, kMagic);
        put_be32(header + 4, kVersion);
        put_be32(header + 8, static_cast<uint32_t>(kRecordSize));
        put_be32(header + 12, static_cast<uint32_t>(h3_resolution));
        put_be64(header + 16, 0);  // record count, patched at finish()
        put_be64(header + 24, 0);  // block count, patched at finish()
        put_be32(header + 32, static_cast<uint32_t>(kRecordsPerBlock));
        put_be32(header + 36, kCompressionZstd);
        ensure_write(fd_, header, sizeof(header), "cache header");
    }

    ~Writer() {
        if (fd_ >= 0) ::close(fd_);
    }

    void add(int64_t node_id, int32_t day, uint64_t h3_cell) {
        const uint16_t day16 = h3_utils::require_u16_day(day);

        if (has_pending_ && pending_node_ == node_id) {
            if (day16 < pending_day_) {
                throw std::runtime_error(
                    "Node cache input not sorted: node " + std::to_string(node_id) +
                    " day decreased from " + std::to_string(pending_day_) + " to " +
                    std::to_string(day16) +
                    " (OSM full-history files are expected sorted by (id, version))");
            }
            if (day16 == pending_day_) {
                pending_cell_ = h3_cell;  // last version of the day wins
                return;
            }
        }
        if (has_pending_ && pending_node_ > node_id) {
            throw std::runtime_error(
                "Node cache input not sorted: node " + std::to_string(node_id) +
                " after node " + std::to_string(pending_node_));
        }

        flush_pending();
        pending_node_ = node_id;
        pending_day_ = day16;
        pending_cell_ = h3_cell;
        has_pending_ = true;
    }

    // Writes the last buffered record and partial block, then patches the
    // header counts and appends the block directory.
    void finish() {
        if (fd_ < 0) return;
        flush_pending();
        flush_block();  // last partial block (no-op if empty)

        // Patch record_count and block_count in place.
        char counts[16];
        put_be64(counts, records_);
        put_be64(counts + 8, blocks_);
        if (::pwrite(fd_, counts, sizeof(counts), 16) != static_cast<ssize_t>(sizeof(counts))) {
            throw std::runtime_error("Failed to finalize node cache header");
        }

        // Append the block directory (first node + compressed size per block).
        std::vector<uint8_t> dir(directory_.size() * kDirectoryEntrySize);
        for (size_t i = 0; i < directory_.size(); ++i) {
            char* p = reinterpret_cast<char*>(dir.data()) + i * kDirectoryEntrySize;
            put_be64(p, directory_[i].first_node);
            put_be32(p + 8, directory_[i].comp_size);
        }
        if (!dir.empty()) {
            ensure_write(fd_, dir.data(), dir.size(), "cache directory");
        }
        bytes_ += dir.size();

        if (::fsync(fd_) != 0) {
            throw std::runtime_error("Failed to fsync node cache");
        }
        ::close(fd_);
        fd_ = -1;
    }

    uint64_t records() const { return records_ + (has_pending_ ? 1 : 0); }
    uint64_t bytes() const { return bytes_; }

private:
    static void put_be32(char* dst, uint32_t v) {
        dst[0] = static_cast<char>(v >> 24);
        dst[1] = static_cast<char>(v >> 16);
        dst[2] = static_cast<char>(v >> 8);
        dst[3] = static_cast<char>(v & 0xFF);
    }

    void flush_pending() {
        if (!has_pending_) return;
        char rec[kRecordSize];
        put_be64(rec, encode_node(pending_node_));
        put_be16(rec + 8, pending_day_);
        put_cell6(rec + 10, h3_utils::pack_cell(pending_cell_, h3_resolution_));
        if (buffer_.empty()) block_first_node_ = encode_node(pending_node_);
        buffer_.insert(buffer_.end(), rec, rec + kRecordSize);
        records_++;
        has_pending_ = false;
        if (buffer_.size() >= kRecordsPerBlock * kRecordSize) flush_block();
    }

    void flush_block() {
        if (buffer_.empty()) return;
        const size_t max_comp = ZSTD_compressBound(buffer_.size());
        if (comp_scratch_.size() < max_comp) comp_scratch_.resize(max_comp);
        const size_t comp_size = ZSTD_compress(comp_scratch_.data(), max_comp,
                                               buffer_.data(), buffer_.size(),
                                               kZstdLevel);
        if (ZSTD_isError(comp_size)) {
            throw std::runtime_error(std::string("ZSTD_compress failed: ") +
                                     ZSTD_getErrorName(comp_size));
        }
        ensure_write(fd_, comp_scratch_.data(), comp_size, "cache block");
        directory_.push_back({block_first_node_, static_cast<uint32_t>(comp_size)});
        bytes_ += comp_size;
        blocks_++;
        buffer_.clear();
    }

    struct DirectoryEntry {
        uint64_t first_node;  // encoded (sign-flipped); decoded for the directory search
        uint32_t comp_size;
    };

    int fd_ = -1;
    int h3_resolution_;

    bool has_pending_ = false;
    int64_t pending_node_ = 0;
    uint16_t pending_day_ = 0;
    uint64_t pending_cell_ = 0;

    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> comp_scratch_;
    std::vector<DirectoryEntry> directory_;
    uint64_t block_first_node_ = 0;
    uint64_t records_ = 0;
    uint64_t blocks_ = 0;
    uint64_t bytes_ = kHeaderSize;
};

// Read-only mmap'd view over an existing cache file.
class Reader {
public:
    Reader(const std::string& path, int expected_h3_resolution) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Node cache " + path +
                                     " not found; run --pass 1 (or --pass all) first");
        }

        uint8_t header[kHeaderSize];
        ssize_t n = ::read(fd, header, sizeof(header));
        if (n != static_cast<ssize_t>(kHeaderSize)) {
            ::close(fd);
            throw std::runtime_error("Node cache " + path +
                                     " too small; rerun with --pass all to rebuild");
        }

        const uint32_t magic = static_cast<uint32_t>(be32(header));
        const uint32_t version = static_cast<uint32_t>(be32(header + 4));
        const uint32_t record_size = static_cast<uint32_t>(be32(header + 8));
        h3_resolution_ = static_cast<int>(be32(header + 12));
        count_ = be64(header + 16);
        block_count_ = be64(header + 24);
        const uint32_t records_per_block = be32(header + 32);
        const uint32_t compression = be32(header + 36);

        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            throw std::runtime_error("Failed to stat node cache " + path);
        }
        const uint64_t file_size = static_cast<uint64_t>(st.st_size);

        if (magic != kMagic || version != kVersion || record_size != kRecordSize) {
            ::close(fd);
            throw std::runtime_error(
                "Incompatible node cache " + path + " (wrong format or record "
                "size); rerun with --pass all to rebuild");
        }
        if (h3_resolution_ != expected_h3_resolution) {
            ::close(fd);
            throw std::runtime_error(
                "Node cache " + path + " was built at H3 resolution " +
                std::to_string(h3_resolution_) + ", but " +
                std::to_string(expected_h3_resolution) +
                " is requested; rerun with --pass all to rebuild");
        }
        // The compression and records-per-block fields are the format stamp:
        // a cache written by another record layout fails this check.
        if (compression != kCompressionZstd ||
            records_per_block != kRecordsPerBlock ||
            block_count_ != (count_ + kRecordsPerBlock - 1) / kRecordsPerBlock) {
            ::close(fd);
            throw std::runtime_error(
                "Incompatible node cache " + path +
                " (not a compressed block format); rerun with --pass all to rebuild");
        }

        const uint64_t dir_size = block_count_ * kDirectoryEntrySize;
        if (file_size < kHeaderSize + dir_size) {
            ::close(fd);
            throw std::runtime_error("Node cache " + path +
                                     " truncated or incompatible; rerun with --pass all to rebuild");
        }

        size_ = static_cast<size_t>(file_size);
        data_ = static_cast<const uint8_t*>(
            ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data_ == MAP_FAILED) {
            ::close(fd);
            throw std::runtime_error("Failed to mmap node cache " + path);
        }
        ::close(fd);

        // The way pass sweeps batches forward; tell the kernel to read ahead
        // rather than thrash random pages.
        ::madvise(const_cast<uint8_t*>(data_), size_, MADV_SEQUENTIAL);

        // Load the block directory and recompute cumulative byte offsets.
        const uint64_t dir_start = size_ - dir_size;
        directory_.reserve(static_cast<size_t>(block_count_));
        uint64_t offset = kHeaderSize;
        for (uint64_t i = 0; i < block_count_; ++i) {
            const uint8_t* p = data_ + dir_start + i * kDirectoryEntrySize;
            const uint32_t comp_size = be32(p + 8);
            directory_.push_back({decode_node(p), offset, comp_size});
            offset += comp_size;
        }
        if (offset != dir_start) {
            throw std::runtime_error("Node cache " + path +
                                     " corrupted block directory; rerun with --pass all to rebuild");
        }
    }

    ~Reader() {
        if (data_ && data_ != MAP_FAILED) ::munmap(const_cast<uint8_t*>(data_), size_);
    }

    size_t size() const { return static_cast<size_t>(count_); }

    // Record access decodes through the one-block decompression cache, so
    // the returned pointer is only valid until the next access of another
    // block.
    const uint8_t* record(size_t i) const {
        const size_t block = i >> kLog2RecordsPerBlock;
        ensure_block(block);
        return cached_buf_.data() + (i & (kRecordsPerBlock - 1)) * kRecordSize;
    }
    int64_t node_at(size_t i) const { return decode_node(record(i)); }
    uint16_t day_at(size_t i) const { return decode_day(record(i) + 8); }
    uint64_t cell_at(size_t i) const {
        return h3_utils::unpack_cell(read_cell6(record(i) + 10), h3_resolution_);
    }

    // Record index at which a forward sweep can start for node_id: the first
    // record of the last block whose first record is < node_id. Records
    // before it are strictly < node_id.
    size_t sweep_start(int64_t node_id) const {
        const size_t block = last_block(node_id);
        return block == kNoBlock ? 0 : block * kRecordsPerBlock;
    }

private:
    struct Block {
        int64_t first_node;
        uint64_t offset;
        uint32_t comp_size;
    };

    void ensure_block(size_t block) const {
        if (cached_block_ == block) return;
        if (block >= directory_.size()) {
            throw std::runtime_error("Node cache block index out of range");
        }
        const Block& b = directory_[block];
        const size_t raw_size =
            (block + 1 == directory_.size()
                 ? static_cast<size_t>(count_ - block * kRecordsPerBlock)
                 : kRecordsPerBlock) *
            kRecordSize;
        if (cached_buf_.size() < raw_size) cached_buf_.resize(raw_size);
        const size_t sz = ZSTD_decompress(cached_buf_.data(), raw_size,
                                          data_ + b.offset, b.comp_size);
        if (ZSTD_isError(sz) || sz != raw_size) {
            throw std::runtime_error(
                std::string("ZSTD_decompress failed: ") +
                (ZSTD_isError(sz) ? ZSTD_getErrorName(sz) : "size mismatch") +
                "; rerun with --pass all to rebuild");
        }
        cached_block_ = block;
    }

    // Index of the last block whose first record is < node_id; kNoBlock if
    // none qualifies.
    size_t last_block(int64_t node_id) const {
        size_t lo = 0, hi = directory_.size();
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (directory_[mid].first_node < node_id) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo > 0 ? lo - 1 : kNoBlock;
    }

    static uint32_t be32(const uint8_t* p) {
        return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
               (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
    }

    static uint64_t be64(const uint8_t* p) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
        return v;
    }

    static constexpr size_t kNoBlock = static_cast<size_t>(-1);

    int h3_resolution_ = 0;
    uint64_t count_ = 0;
    uint64_t block_count_ = 0;
    size_t size_ = 0;
    const uint8_t* data_ = nullptr;
    std::vector<Block> directory_;
    mutable std::vector<uint8_t> cached_buf_;
    mutable size_t cached_block_ = static_cast<size_t>(-1);
};

}  // namespace node_cache