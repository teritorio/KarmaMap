#pragma once

// The persisted per-(uid, day, language) event count store for name-language
// (name-language abuse detection). One shared, single-file store holds the
// merged per-(uid, day, lang) counts for the whole update period; the update
// finalize sums the incoming diffs' staged buckets into it.
//
// Record format (variable length):
//   [uid:8][day:4][lang_len:1][lang:N][count:4]
//
// File format (same block/compression scheme as suspect_tag_store):
//   [header 40B][block 0]...[block N-1][directory 12*N]
//
// uid is big-endian with the sign bit flipped so the encoded bytes sort in
// numeric order; day/count are plain big-endian; lang_len is 1 byte, lang is
// UTF-8. Records are unique per (uid, day, lang) — the finalize sums before
// writing. The writer streams into ZSTD-compressed 2^18-record blocks and
// swaps the result into place with a rename. The header carries the
// applied-sequence stamp, making a rerun of an already-folded sequence a no-op.

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

namespace suspect_lang_store {

constexpr uint32_t kMagic = 0x4C414E47;  // "LANG"
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

class Writer {
public:
    explicit Writer(const std::string& path)
        : final_path_(path), tmp_path_(path + ".tmp") {
        const std::filesystem::path dir = std::filesystem::path(final_path_).parent_path();
        if (!dir.empty()) std::filesystem::create_directories(dir);
        fd_ = ::open(tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to create lang store " + tmp_path_);
        }
        char header[kHeaderSize] = {};
        put_be32(header, kMagic);
        put_be32(header + 4, kVersion);
        put_be32(header + 8, 0);  // record_size = 0 means variable-length
        put_be32(header + 12, 0);  // reserved
        put_be64(header + 16, 0);  // record count, patched at finish()
        put_be64(header + 24, 0);  // block count, patched at finish()
        put_be64(header + 32, 0);  // applied sequence, patched at finish()
        ensure_write(fd_, header, sizeof(header), "lang store header");
    }

    ~Writer() {
        if (fd_ >= 0) ::close(fd_);
    }

    void add(int64_t uid, uint32_t day, const std::string& lang, uint32_t count) {
        if (lang.size() > 255) {
            throw std::runtime_error("Lang code too long: " + lang);
        }
        if (has_pending_) {
            if (pending_uid_ > uid ||
                (pending_uid_ == uid && pending_day_ > day) ||
                (pending_uid_ == uid && pending_day_ == day && pending_lang_ >= lang)) {
                throw std::runtime_error(
                    "Lang store input not sorted or duplicated: (uid=" +
                    std::to_string(uid) + ", day=" + std::to_string(day) +
                    ", lang=" + lang + ") is not after (uid=" +
                    std::to_string(pending_uid_) + ", day=" +
                    std::to_string(pending_day_) + ", lang=" + pending_lang_ + ")");
            }
        }
        flush_pending();
        pending_uid_ = uid;
        pending_day_ = day;
        pending_lang_ = lang;
        pending_count_ = count;
        has_pending_ = true;
    }

    void set_applied_seq(uint64_t seq) { applied_seq_ = seq; }

    void finish() {
        if (fd_ < 0) return;
        flush_pending();
        flush_block();

        char counts[16];
        put_be64(counts, records_);
        put_be64(counts + 8, blocks_);
        if (::pwrite(fd_, counts, sizeof(counts), 16) != static_cast<ssize_t>(sizeof(counts))) {
            throw std::runtime_error("Failed to finalize lang store header");
        }
        char stamp[8];
        put_be64(stamp, applied_seq_);
        if (::pwrite(fd_, stamp, sizeof(stamp), 32) != static_cast<ssize_t>(sizeof(stamp))) {
            throw std::runtime_error("Failed to stamp lang store header");
        }

        std::vector<uint8_t> dir(directory_.size() * kDirectoryEntrySize);
        for (size_t i = 0; i < directory_.size(); ++i) {
            char* p = reinterpret_cast<char*>(dir.data()) + i * kDirectoryEntrySize;
            put_be64(p, directory_[i].first_uid);
            put_be32(p + 8, directory_[i].comp_size);
        }
        if (!dir.empty()) {
            ensure_write(fd_, dir.data(), dir.size(), "lang store directory");
        }
        bytes_ += dir.size();

        if (::fsync(fd_) != 0) {
            throw std::runtime_error("Failed to fsync lang store");
        }
        ::close(fd_);
        fd_ = -1;

        std::filesystem::rename(tmp_path_, final_path_);
    }

    uint64_t records() const { return records_ + (has_pending_ ? 1 : 0); }

private:
    void flush_pending() {
        if (!has_pending_) return;
        const size_t lang_len = pending_lang_.size();
        const size_t rec_size = 8 + 4 + 1 + lang_len + 4;
        std::vector<char> rec_vec(rec_size);
        char* p = rec_vec.data();
        put_be64(p, encode_uid(pending_uid_));
        p += 8;
        put_be32(p, pending_day_);
        p += 4;
        *p++ = static_cast<char>(lang_len);
        std::memcpy(p, pending_lang_.data(), lang_len);
        p += lang_len;
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
        ensure_write(fd_, comp_scratch_.data(), comp_size, "lang store block");
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
    uint32_t pending_day_ = 0;
    std::string pending_lang_;
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

class Reader {
public:
    explicit Reader(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Lang store " + path + " not found");
        }

        uint8_t header[kHeaderSize];
        ssize_t n = ::read(fd, header, sizeof(header));
        if (n != static_cast<ssize_t>(kHeaderSize)) {
            ::close(fd);
            throw std::runtime_error("Lang store " + path + " too small");
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
            throw std::runtime_error("Failed to stat lang store " + path);
        }
        const uint64_t file_size = static_cast<uint64_t>(st.st_size);

        if (magic != kMagic || version != kVersion || record_size != 0) {
            ::close(fd);
            throw std::runtime_error("Incompatible lang store " + path +
                                     " (wrong format or record size)");
        }
        if (block_count_ != (count_ + kRecordsPerBlock - 1) / kRecordsPerBlock) {
            ::close(fd);
            throw std::runtime_error("Incompatible lang store " + path +
                                     " (not a compressed block format)");
        }

        const uint64_t dir_size = block_count_ * kDirectoryEntrySize;
        if (file_size < kHeaderSize + dir_size) {
            ::close(fd);
            throw std::runtime_error("Lang store " + path + " truncated or incompatible");
        }

        size_ = static_cast<size_t>(file_size);
        data_ = static_cast<const uint8_t*>(
            ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data_ == MAP_FAILED) {
            ::close(fd);
            throw std::runtime_error("Failed to mmap lang store " + path);
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
            throw std::runtime_error("Lang store " + path +
                                     " corrupted block directory");
        }
    }

    ~Reader() {
        if (data_ && data_ != MAP_FAILED) ::munmap(const_cast<uint8_t*>(data_), size_);
    }

    size_t size() const { return static_cast<size_t>(count_); }
    uint64_t applied_seq() const { return applied_seq_; }

    const uint8_t* record(size_t i) const {
        const size_t block = i >> kLog2RecordsPerBlock;
        ensure_block(block);
        const Block& b = directory_[block];
        const size_t idx_in_block = i & (kRecordsPerBlock - 1);
        if (idx_in_block >= b.record_offsets.size()) {
            throw std::runtime_error("Lang store: record index out of range in block " +
                                     std::to_string(block));
        }
        return cached_buf_.data() + b.record_offsets[idx_in_block];
    }

    int64_t uid_at(size_t i) const {
        const uint8_t* rec = record(i);
        return decode_uid(rec);
    }
    uint32_t day_at(size_t i) const {
        const uint8_t* rec = record(i);
        return be32(rec + 8);
    }
    std::string lang_at(size_t i) const {
        const uint8_t* rec = record(i);
        const uint8_t lang_len = rec[12];
        return std::string(reinterpret_cast<const char*>(rec + 13), lang_len);
    }
    uint32_t count_at(size_t i) const {
        const uint8_t* rec = record(i);
        const uint8_t lang_len = rec[12];
        return be32(rec + 13 + lang_len);
    }

private:
    struct Block {
        int64_t first_uid;
        uint64_t offset;
        uint32_t comp_size;
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
            if (pos + 13 > cached_buf_.size()) {
                throw std::runtime_error("Lang store: truncated record at block " +
                                         std::to_string(block) + ", record " +
                                         std::to_string(i));
            }
            uint8_t lang_len = buf[pos + 12];
            size_t rec_size = 8 + 4 + 1 + lang_len + 4;
            if (pos + rec_size > cached_buf_.size()) {
                throw std::runtime_error("Lang store: record extends past buffer at block " +
                                         std::to_string(block) + ", record " +
                                         std::to_string(i));
            }
            pos += rec_size;
        }
        b.offsets_built = true;
    }

    void ensure_block(size_t block) const {
        if (cached_block_ == block) return;
        if (block >= directory_.size()) {
            throw std::runtime_error("Lang store block index out of range");
        }
        const Block& b = directory_[block];
        unsigned long long frame_size = ZSTD_getFrameContentSize(data_ + b.offset, b.comp_size);
        if (frame_size == ZSTD_CONTENTSIZE_ERROR || frame_size == ZSTD_CONTENTSIZE_UNKNOWN) {
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

}  // namespace suspect_lang_store
