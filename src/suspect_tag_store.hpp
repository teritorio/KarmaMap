#pragma once

// The persisted per-(uid, minute, tag_key) modified+deleted count store
// for Filter 5 (tag activity tracking). One shared, single-file store
// holds the merged per-(uid, minute, tag_key) counts for the whole update
// period; the update finalize sums the incoming diffs' staged buckets into
// it ("merge with the incoming update").
//
// Record format (variable length):
//   [uid:8][minute:4][tag_key_len:1][tag_key:N][count:4]
//
// File format (same block/compression scheme as suspect_store):
//   [header 40B][block 0]...[block N-1][directory 12*N]
//
// uid is big-endian with the sign bit flipped so the encoded bytes sort in
// numeric order; minute/count are plain big-endian; tag_key_len is 1 byte,
// tag_key is UTF-8. Records are unique per (uid, minute, tag_key) — the
// finalize sums before writing. The writer streams into ZSTD-compressed
// 2^18-record blocks and swaps the result into place with a rename. The header
// carries the applied-sequence stamp, making a rerun of an already-folded
// sequence a no-op.

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <zstd.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

namespace suspect_tag_store {

constexpr uint32_t kMagic = 0x53474154;  // "TAGS"
// The store holds modified+deleted counts only, so it shares scope with the
// minute store that supplies the coverage denominator.
constexpr uint32_t kVersion = 1;
constexpr size_t kHeaderSize = 40;
constexpr size_t kRecordsPerBlock = 1 << 18;  // 262144 records per block
constexpr size_t kLog2RecordsPerBlock = 18;
constexpr int kZstdLevel = 3;
constexpr size_t kDirectoryEntrySize = 12;  // first_uid (8) + compressed_size (4)

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

inline uint64_t encode_uid(int64_t uid) {
    return static_cast<uint64_t>(uid) ^ (1ULL << 63);
}

inline int64_t decode_uid(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
    return static_cast<int64_t>(v ^ (1ULL << 63));
}

inline void put_be64(char* dst, uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        dst[i] = static_cast<char>(v & 0xFF);
        v >>= 8;
    }
}

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

// Buffers records into ZSTD blocks and writes them out as they fill. Input
// must be strictly ascending by (uid, minute, tag_key) with no repeated key;
// the merge in fold_tag_counts guarantees that. Writes to <path>.tmp and
// renames over <path> at finish().
class Writer {
public:
    explicit Writer(const std::string& path)
        : final_path_(path), tmp_path_(path + ".tmp") {
        const std::filesystem::path dir = std::filesystem::path(final_path_).parent_path();
        if (!dir.empty()) std::filesystem::create_directories(dir);
        fd_ = ::open(tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to create tag store " + tmp_path_);
        }
        char header[kHeaderSize] = {};
        put_be32(header, kMagic);
        put_be32(header + 4, kVersion);
        put_be32(header + 8, 0);  // record_size = 0 means variable-length
        put_be32(header + 12, 0);  // h3_resolution = 0 (N/A for tag store)
        put_be64(header + 16, 0);  // record count, patched at finish()
        put_be64(header + 24, 0);  // block count, patched at finish()
        put_be64(header + 32, 0);  // applied sequence, patched at finish()
        ensure_write(fd_, header, sizeof(header), "tag store header");
    }

    ~Writer() {
        if (fd_ >= 0) ::close(fd_);
    }

    void add(int64_t uid, uint32_t minute, const std::string& tag_key, uint32_t count) {
        if (tag_key.size() > 255) {
            throw std::runtime_error("Tag key too long (max 255 chars per OSM): " + tag_key);
        }
        if (has_pending_) {
            if (pending_uid_ > uid ||
                (pending_uid_ == uid && pending_minute_ > minute) ||
                (pending_uid_ == uid && pending_minute_ == minute && pending_tag_key_ >= tag_key)) {
                throw std::runtime_error(
                    "Tag store input not sorted or duplicated: (uid=" +
                    std::to_string(uid) + ", minute=" + std::to_string(minute) +
                    ", tag=" + tag_key +
                    ") is not after (uid=" + std::to_string(pending_uid_) +
                    ", minute=" + std::to_string(pending_minute_) +
                    ", tag=" + pending_tag_key_ + ")");
            }
        }
        flush_pending();
        pending_uid_ = uid;
        pending_minute_ = minute;
        pending_tag_key_ = tag_key;
        pending_count_ = count;
        has_pending_ = true;
    }

    // Sets the source-sequence stamp written into the header.
    void set_applied_seq(uint64_t seq) { applied_seq_ = seq; }

    // Finalizes the header and directory on the tmp file, then renames it
    // over the final path so a previous store is only ever replaced whole.
    void finish() {
        if (fd_ < 0) return;
        flush_pending();
        flush_block();  // last partial block (no-op if empty)

        char counts[16];
        put_be64(counts, records_);
        put_be64(counts + 8, blocks_);
        if (::pwrite(fd_, counts, sizeof(counts), 16) != static_cast<ssize_t>(sizeof(counts))) {
            throw std::runtime_error("Failed to finalize tag store header");
        }
        char stamp[8];
        put_be64(stamp, applied_seq_);
        if (::pwrite(fd_, stamp, sizeof(stamp), 32) != static_cast<ssize_t>(sizeof(stamp))) {
            throw std::runtime_error("Failed to stamp tag store header");
        }

        std::vector<uint8_t> dir(directory_.size() * kDirectoryEntrySize);
        for (size_t i = 0; i < directory_.size(); ++i) {
            char* p = reinterpret_cast<char*>(dir.data()) + i * kDirectoryEntrySize;
            put_be64(p, directory_[i].first_uid);
            put_be32(p + 8, directory_[i].comp_size);
        }
        if (!dir.empty()) {
            ensure_write(fd_, dir.data(), dir.size(), "tag store directory");
        }
        bytes_ += dir.size();

        if (::fsync(fd_) != 0) {
            throw std::runtime_error("Failed to fsync tag store");
        }
        ::close(fd_);
        fd_ = -1;

        std::filesystem::rename(tmp_path_, final_path_);
    }

    uint64_t records() const { return records_ + (has_pending_ ? 1 : 0); }

private:
    void flush_pending() {
        if (!has_pending_) return;
        const size_t tag_len = pending_tag_key_.size();
        const size_t rec_size = 8 + 4 + 1 + tag_len + 4;  // uid + minute + tag_len + tag + count
        std::vector<char> rec_vec(rec_size);
        char* p = rec_vec.data();
        put_be64(p, encode_uid(pending_uid_));
        p += 8;
        put_be32(p, pending_minute_);
        p += 4;
        *p++ = static_cast<char>(tag_len);
        std::memcpy(p, pending_tag_key_.data(), tag_len);
        p += tag_len;
        put_be32(p, pending_count_);
        if (buffer_.empty()) block_first_uid_ = encode_uid(pending_uid_);
        buffer_.insert(buffer_.end(), rec_vec.begin(), rec_vec.end());
        records_++;
        records_in_block_++;
        has_pending_ = false;
        if (records_in_block_ >= kRecordsPerBlock) flush_block();
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
        ensure_write(fd_, comp_scratch_.data(), comp_size, "tag store block");
        directory_.push_back({block_first_uid_, static_cast<uint32_t>(comp_size)});
        bytes_ += comp_size;
        blocks_++;
        records_in_block_ = 0;
        buffer_.clear();
    }

    struct DirectoryEntry {
        uint64_t first_uid;  // encoded (sign-flipped)
        uint32_t comp_size;
    };

    std::string final_path_;
    std::string tmp_path_;
    int fd_ = -1;

    bool has_pending_ = false;
    int64_t pending_uid_ = 0;
    uint32_t pending_minute_ = 0;
    std::string pending_tag_key_;
    uint32_t pending_count_ = 0;

    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> comp_scratch_;
    std::vector<DirectoryEntry> directory_;
    uint64_t block_first_uid_ = 0;
    uint64_t records_ = 0;
    uint64_t records_in_block_ = 0;
    uint64_t blocks_ = 0;
    uint64_t bytes_ = kHeaderSize;
    uint64_t applied_seq_ = 0;
};

// Read-only mmap'd view of a tag store. Same block/directory access as the
// node cache; records are returned through a one-block decompression cache so
// the pointer is only valid until the next access of another block.
class Reader {
public:
    explicit Reader(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Tag store " + path + " not found");
        }

        uint8_t header[kHeaderSize];
        ssize_t n = ::read(fd, header, sizeof(header));
        if (n != static_cast<ssize_t>(kHeaderSize)) {
            ::close(fd);
            throw std::runtime_error("Tag store " + path + " too small");
        }

        const uint32_t magic = be32(header);
        const uint32_t version = be32(header + 4);
        const uint32_t record_size = be32(header + 8);
        count_ = be64(header + 16);
        block_count_ = be64(header + 24);
        applied_seq_ = be64(header + 32);

        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            throw std::runtime_error("Failed to stat tag store " + path);
        }
        const uint64_t file_size = static_cast<uint64_t>(st.st_size);

        if (magic != kMagic || version != kVersion || record_size != 0) {
            ::close(fd);
            throw std::runtime_error("Incompatible tag store " + path +
                                     " (wrong format or record size)");
        }
        if (block_count_ != (count_ + kRecordsPerBlock - 1) / kRecordsPerBlock) {
            ::close(fd);
            throw std::runtime_error("Incompatible tag store " + path +
                                     " (not a compressed block format)");
        }

        const uint64_t dir_size = block_count_ * kDirectoryEntrySize;
        if (file_size < kHeaderSize + dir_size) {
            ::close(fd);
            throw std::runtime_error("Tag store " + path + " truncated or incompatible");
        }

        size_ = static_cast<size_t>(file_size);
        data_ = static_cast<const uint8_t*>(
            ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data_ == MAP_FAILED) {
            ::close(fd);
            throw std::runtime_error("Failed to mmap tag store " + path);
        }
        ::close(fd);
        ::madvise(const_cast<uint8_t*>(data_), size_, MADV_SEQUENTIAL);

        const uint64_t dir_start = size_ - dir_size;
        directory_.reserve(static_cast<size_t>(block_count_));
        uint64_t offset = kHeaderSize;
        for (uint64_t i = 0; i < block_count_; ++i) {
            const uint8_t* p = data_ + dir_start + i * kDirectoryEntrySize;
            const uint32_t comp_size = be32(p + 8);
            directory_.push_back({decode_uid(p), offset, comp_size});
            offset += comp_size;
        }
        if (offset != dir_start) {
            throw std::runtime_error("Tag store " + path +
                                     " corrupted block directory");
        }
    }

    ~Reader() {
        if (data_ && data_ != MAP_FAILED) ::munmap(const_cast<uint8_t*>(data_), size_);
    }

    size_t size() const { return static_cast<size_t>(count_); }
    uint64_t applied_seq() const { return applied_seq_; }

    // The returned pointer is only valid until the next access of another
    // block (one-block decompression cache).
    const uint8_t* record(size_t i) const {
        const size_t block = i >> kLog2RecordsPerBlock;
        ensure_block(block);
        const Block& b = directory_[block];
        const size_t idx_in_block = i & (kRecordsPerBlock - 1);
        if (idx_in_block >= b.record_offsets.size()) {
            throw std::runtime_error("Tag store: record index out of range in block " + 
                                     std::to_string(block));
        }
        return cached_buf_.data() + b.record_offsets[idx_in_block];
    }

    // Variable-length record accessors
    int64_t uid_at(size_t i) const {
        const uint8_t* rec = record(i);
        return decode_uid(rec);
    }
    uint32_t minute_at(size_t i) const {
        const uint8_t* rec = record(i);
        return be32(rec + 8);
    }
    std::string tag_key_at(size_t i) const {
        const uint8_t* rec = record(i);
        const uint8_t tag_len = rec[12];
        return std::string(reinterpret_cast<const char*>(rec + 13), tag_len);
    }
    uint32_t count_at(size_t i) const {
        const uint8_t* rec = record(i);
        const uint8_t tag_len = rec[12];
        return be32(rec + 13 + tag_len);
    }

private:
    struct Block {
        int64_t first_uid;
        uint64_t offset;
        uint32_t comp_size;
        // Variable-length record offsets within the decompressed block
        mutable std::vector<size_t> record_offsets;
        mutable bool offsets_built = false;
        
        Block() = default;
        Block(int64_t uid, uint64_t off, uint32_t cs) 
            : first_uid(uid), offset(off), comp_size(cs) {}
    };

    void build_record_offsets(size_t block) const {
        Block& b = directory_[block];
        if (b.offsets_built) return;
        
        const uint8_t* buf = cached_buf_.data();
        size_t pos = 0;
        size_t records_in_block = (block + 1 == directory_.size())
            ? static_cast<size_t>(count_ - block * kRecordsPerBlock)
            : kRecordsPerBlock;
        
        b.record_offsets.clear();
        b.record_offsets.reserve(records_in_block);
        
        for (size_t i = 0; i < records_in_block; ++i) {
            b.record_offsets.push_back(pos);
            // uid:8 + minute:4 + tag_len:1 + tag:N + count:4
            if (pos + 13 > cached_buf_.size()) {
                throw std::runtime_error("Tag store: truncated record at block " + 
                                         std::to_string(block) + ", record " + std::to_string(i));
            }
            uint8_t tag_len = buf[pos + 12];
            size_t rec_size = 8 + 4 + 1 + tag_len + 4;
            if (pos + rec_size > cached_buf_.size()) {
                throw std::runtime_error("Tag store: record extends past buffer at block " + 
                                         std::to_string(block) + ", record " + std::to_string(i));
            }
            pos += rec_size;
        }
        b.offsets_built = true;
    }

    void ensure_block(size_t block) const {
        if (cached_block_ == block) return;
        if (block >= directory_.size()) {
            throw std::runtime_error("Tag store block index out of range");
        }
        const Block& b = directory_[block];
        // Use ZSTD_getFrameContentSize to get exact decompressed size
        unsigned long long frame_size = ZSTD_getFrameContentSize(data_ + b.offset, b.comp_size);
        if (frame_size == ZSTD_CONTENTSIZE_ERROR || frame_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            // Fallback to max record size if frame size unknown
            const size_t raw_size =
                (block + 1 == directory_.size()
                     ? static_cast<size_t>(count_ - block * kRecordsPerBlock)
                     : kRecordsPerBlock) *
                272;
            if (cached_buf_.size() < raw_size) cached_buf_.resize(raw_size);
        } else {
            if (cached_buf_.size() < frame_size) cached_buf_.resize(static_cast<size_t>(frame_size));
        }
        const size_t sz = ZSTD_decompress(cached_buf_.data(), cached_buf_.size(),
                                          data_ + b.offset, b.comp_size);
        if (ZSTD_isError(sz) || sz == 0) {
            throw std::runtime_error(std::string("ZSTD_decompress failed: ") +
                                     (ZSTD_isError(sz) ? ZSTD_getErrorName(sz) : "size mismatch"));
        }
        cached_block_ = block;
        build_record_offsets(block);
    }

    uint64_t count_ = 0;
    uint64_t block_count_ = 0;
    uint64_t applied_seq_ = 0;
    size_t size_ = 0;
    const uint8_t* data_ = nullptr;
    mutable std::vector<Block> directory_;
    mutable std::vector<uint8_t> cached_buf_;
    mutable size_t cached_block_ = static_cast<size_t>(-1);
};

// Reads only the applied-sequence stamp of an existing store (0 when the file
// does not exist or is too small to carry a header).
inline uint64_t applied_seq_of(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return 0;
    uint8_t header[kHeaderSize];
    const ssize_t n = ::read(fd, header, sizeof(header));
    ::close(fd);
    if (n != static_cast<ssize_t>(kHeaderSize)) return 0;
    const uint32_t magic = be32(header);
    const uint32_t version = be32(header + 4);
    const uint32_t record_size = be32(header + 8);
    if (magic != kMagic || version != kVersion || record_size != 0) return 0;
    return be64(header + 32);
}

}  // namespace suspect_tag_store