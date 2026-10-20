#pragma once

// The persisted per-object prior tag store for name-language abuse detection.
// Uses dictionary encoding for tag keys: each unique key is assigned a 2-byte
// ID, and records store key IDs instead of repeated key strings.
//
// Record format (variable length):
//   [object_id:8 BE sign-flipped][n_tags:1][(key_id:2 BE, val_len:1, value:N) x n_tags]
//
// File format:
//   [header 40B][ZSTD block 0]...[ZSTD block N-1][dictionary][block directory 12*N]
//
// Dictionary format:
//   [dict_size:4][key_count:4][(key_len:1, key:N) x key_count]
//
// Header:
//   [0:4]   magic = TAGO
//   [4:8]   version = 2
//   [8:12]  record_size = 0 (variable-length)
//   [12:16] dictionary_size
//   [16:24] record count
//   [24:32] block count
//   [32:40] applied sequence

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
#include <unordered_map>
#include <utility>
#include <vector>

namespace object_tag_store {

constexpr uint32_t kMagic = 0x5441474F;  // "TAGO"
constexpr uint32_t kVersion = 2;
constexpr size_t kHeaderSize = 40;
constexpr size_t kRecordsPerBlock = 1 << 18;  // 262144 records per block
constexpr size_t kLog2RecordsPerBlock = 18;
constexpr int kZstdLevel = 3;
constexpr size_t kDirectoryEntrySize = 12;  // first_object_id (8) + compressed_size (4)
constexpr uint16_t kMaxDictKeys = 65535;  // max unique tag keys (IDs 0..65534)

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

inline uint64_t encode_id(int64_t id) {
    return static_cast<uint64_t>(id) ^ (1ULL << 63);
}

inline int64_t decode_id(const uint8_t* p) {
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

inline void put_be16(char* dst, uint16_t v) {
    dst[0] = static_cast<char>(v >> 8);
    dst[1] = static_cast<char>(v & 0xFF);
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

inline uint16_t be16(const uint8_t* p) {
    return (static_cast<uint16_t>(p[0]) << 8) | static_cast<uint16_t>(p[1]);
}

// A set of (key, value) tag pairs for one object, sorted by key.
using TagMap = std::vector<std::pair<std::string, std::string>>;

// Serialise one object's tags using dictionary key IDs.
inline std::vector<char> serialise_record_dict(int64_t object_id,
                                                const TagMap& tags,
                                                const std::unordered_map<std::string, uint16_t>& key_to_id) {
    size_t size = 10;  // object_id(8) + n_tags(2)  (2-byte count; format version stays 2)
    for (const auto& [key, value] : tags) {
        size += 2 + 1 + value.size();  // key_id(2) + val_len(1) + value
    }
    std::vector<char> buf(size);
    char* p = buf.data();
    put_be64(p, encode_id(object_id));
    p += 8;
    put_be16(p, static_cast<uint16_t>(tags.size()));
    p += 2;
    for (const auto& [key, value] : tags) {
        auto it = key_to_id.find(key);
        if (it == key_to_id.end()) {
            throw std::runtime_error("Object tag store: key not in dictionary: " + key);
        }
        put_be16(p, it->second);
        p += 2;
        *p++ = static_cast<char>(value.size());
        std::memcpy(p, value.data(), value.size());
        p += value.size();
    }
    return buf;
}

// Parse one record using dictionary key IDs.
inline std::pair<int64_t, TagMap> parse_record_dict(const uint8_t* p, const uint8_t* end,
                                                     const std::vector<std::string>& id_to_key) {
    if (static_cast<size_t>(end - p) < 10) {
        throw std::runtime_error("Object tag store: truncated record");
    }
    int64_t object_id = decode_id(p);
    p += 8;
    const uint16_t n_tags = be16(p);
    p += 2;
    TagMap tags;
    tags.reserve(n_tags);
    for (uint16_t t = 0; t < n_tags; ++t) {
        if (static_cast<size_t>(end - p) < 3) {
            throw std::runtime_error("Object tag store: truncated key id");
        }
        uint16_t key_id = be16(p);
        p += 2;
        if (static_cast<size_t>(end - p) < 1) {
            throw std::runtime_error("Object tag store: truncated value length");
        }
        const uint8_t val_len = *p++;
        if (static_cast<size_t>(end - p) < val_len) {
            throw std::runtime_error("Object tag store: truncated value");
        }
        std::string value(reinterpret_cast<const char*>(p), val_len);
        p += val_len;
        if (static_cast<size_t>(key_id) >= id_to_key.size()) {
            throw std::runtime_error("Object tag store: invalid key id");
        }
        tags.emplace_back(id_to_key[key_id], std::move(value));
    }
    return {object_id, tags};
}

// Buffers records into ZSTD blocks and writes them out as they fill. Input
// must be strictly ascending by object_id with no repeated id. Builds a
// dictionary of tag keys as records are added; writes the dictionary at the
// end of the file (before the block directory) at finish().
class Writer {
public:
    // `type`: 0=node, 1=way, 2=relation. Writes to
    // <parent>/object_tags.<nodes|ways|relations>.cache derived from `path`
    // by replacing the final filename's stem with "object_tags.<type>".
    explicit Writer(const std::string& path, uint8_t type)
        : type_(type) {
        const std::filesystem::path p(path);
        const std::string type_name = type_name_str(type);
        final_path_ = p.parent_path() / ("object_tags." + type_name + ".cache");
        tmp_path_ = final_path_.string() + ".tmp";
        const std::filesystem::path dir = final_path_.parent_path();
        if (!dir.empty()) std::filesystem::create_directories(dir);
        fd_ = ::open(tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to create object tag store " + tmp_path_);
        }
        char header[kHeaderSize] = {};
        put_be32(header, kMagic);
        put_be32(header + 4, kVersion);
        put_be32(header + 8, 0);  // record_size = 0 means variable-length
        put_be32(header + 12, 0);  // dictionary_size, patched at finish()
        put_be64(header + 16, 0);  // record count, patched at finish()
        put_be64(header + 24, 0);  // block count, patched at finish()
        put_be64(header + 32, 0);  // applied sequence, patched at finish()
        ensure_write(fd_, header, sizeof(header), "object tag store header");
    }

    ~Writer() {
        if (fd_ >= 0) ::close(fd_);
    }

    static std::string type_name_str(uint8_t type) {
        switch (type) {
            case 0: return "nodes";
            case 1: return "ways";
            case 2: return "relations";
            default: throw std::runtime_error("Invalid object tag store type: " + std::to_string(type));
        }
    }

    // Adds a record. object_id must be strictly ascending. type is fixed at
    // construction and not stored per-record.
    void add(int64_t object_id, const TagMap& tags) {
        if (tags.size() > 65535) {
            throw std::runtime_error(
                "Object tag store: too many tags per object (" +
                std::to_string(tags.size()) + " > 65535): object_id=" +
                std::to_string(object_id));
        }
        if (has_pending_ && pending_id_ >= object_id) {
            throw std::runtime_error(
                "Object tag store input not sorted or duplicated: object_id=" +
                std::to_string(object_id) +
                " is not after object_id=" + std::to_string(pending_id_));
        }
        // Assign key IDs for any new keys in this record
        for (const auto& [key, value] : tags) {
            if (key_to_id_.find(key) == key_to_id_.end()) {
                if (id_to_key_.size() >= kMaxDictKeys) {
                    throw std::runtime_error(
                        "Object tag store: too many unique tag keys (> " +
                        std::to_string(kMaxDictKeys) + ")");
                }
                key_to_id_[key] = static_cast<uint16_t>(id_to_key_.size());
                id_to_key_.push_back(key);
            }
        }
        flush_pending();
        pending_id_ = object_id;
        pending_tags_ = tags;
        has_pending_ = true;
    }

    void finish() {
        if (fd_ < 0) return;
        flush_pending();
        flush_block();  // last partial block (no-op if empty)

        // Write dictionary: [dict_size:4][key_count:4][(key_len:1, key:N) x key_count]
        std::vector<char> dict_buf;
        dict_buf.reserve(8 + id_to_key_.size() * 16);
        char tmp[8];
        put_be32(tmp, 0);  // dict_size placeholder, patched below
        put_be32(tmp + 4, static_cast<uint32_t>(id_to_key_.size()));
        dict_buf.insert(dict_buf.end(), tmp, tmp + 8);
        for (const auto& key : id_to_key_) {
            dict_buf.push_back(static_cast<char>(key.size()));
            dict_buf.insert(dict_buf.end(), key.begin(), key.end());
        }
        // Patch dict_size (first 4 bytes)
        put_be32(dict_buf.data(), static_cast<uint32_t>(dict_buf.size()));

        ensure_write(fd_, dict_buf.data(), dict_buf.size(), "object tag store dictionary");

        // Patch header: record count, block count, dictionary_size
        char counts[16];
        put_be64(counts, records_);
        put_be64(counts + 8, blocks_);
        if (::pwrite(fd_, counts, sizeof(counts), 16) != static_cast<ssize_t>(sizeof(counts))) {
            throw std::runtime_error("Failed to finalize object tag store header");
        }
        char dict_size[4];
        put_be32(dict_size, static_cast<uint32_t>(dict_buf.size()));
        if (::pwrite(fd_, dict_size, 4, 12) != 4) {
            throw std::runtime_error("Failed to write dictionary size");
        }

        // Write directory
        std::vector<uint8_t> dir(directory_.size() * kDirectoryEntrySize);
        for (size_t i = 0; i < directory_.size(); ++i) {
            char* p = reinterpret_cast<char*>(dir.data()) + i * kDirectoryEntrySize;
            put_be64(p, directory_[i].first_id);
            put_be32(p + 8, directory_[i].comp_size);
        }
        if (!dir.empty()) {
            ensure_write(fd_, dir.data(), dir.size(), "object tag store directory");
        }

        if (::fsync(fd_) != 0) {
            throw std::runtime_error("Failed to fsync object tag store");
        }
        ::close(fd_);
        fd_ = -1;
        std::filesystem::rename(tmp_path_, final_path_);
    }

    uint64_t records() const { return records_ + (has_pending_ ? 1 : 0); }

private:
    void flush_pending() {
        if (!has_pending_) return;
        std::vector<char> rec = serialise_record_dict(pending_id_, pending_tags_, key_to_id_);
        if (buffer_.empty()) block_first_id_ = encode_id(pending_id_);
        buffer_.insert(buffer_.end(), rec.begin(), rec.end());
        records_++;
        has_pending_ = false;
        if (records_ % kRecordsPerBlock == 0) flush_block();  // kRecordsPerBlock records per block
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
        ensure_write(fd_, comp_scratch_.data(), comp_size, "object tag store block");
        directory_.push_back({block_first_id_, static_cast<uint32_t>(comp_size)});
        bytes_ += comp_size;
        blocks_++;
        buffer_.clear();
    }

    struct DirectoryEntry {
        uint64_t first_id;  // encoded (sign-flipped)
        uint32_t comp_size;
    };

    std::filesystem::path final_path_;
    std::string tmp_path_;
    int fd_ = -1;

    bool has_pending_ = false;
    int64_t pending_id_ = 0;
    uint8_t type_ = 0;
    TagMap pending_tags_;

    std::vector<uint8_t> buffer_;
    std::vector<uint8_t> comp_scratch_;
    std::vector<DirectoryEntry> directory_;
    uint64_t block_first_id_ = 0;
    uint64_t records_ = 0;
    uint64_t blocks_ = 0;
    uint64_t bytes_ = kHeaderSize;

    // Dictionary: key -> ID and ID -> key
    std::unordered_map<std::string, uint16_t> key_to_id_;
    std::vector<std::string> id_to_key_;
};

// Read-only mmap'd view of an object tag store. Loads the dictionary from the
// end of the file and uses it to decode key IDs in records.
class Reader {
public:
    // `type`: 0=node, 1=way, 2=relation. Reads from
    // <parent>/object_tags.<nodes|ways|relations>.cache derived from `path`.
    explicit Reader(const std::string& path, uint8_t type)
        : type_(type) {
        const std::filesystem::path p(path);
        const std::string type_name = Writer::type_name_str(type);
        const std::string actual_path = p.parent_path() / ("object_tags." + type_name + ".cache");
        int fd = ::open(actual_path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Object tag store " + actual_path + " not found");
        }

        uint8_t header[kHeaderSize];
        ssize_t n = ::read(fd, header, sizeof(header));
        if (n != static_cast<ssize_t>(kHeaderSize)) {
            ::close(fd);
            throw std::runtime_error("Object tag store " + path + " too small");
        }

        const uint32_t magic = be32(header);
        const uint32_t version = be32(header + 4);
        const uint32_t record_size = be32(header + 8);
        const uint32_t dict_size = be32(header + 12);
        count_ = be64(header + 16);
        block_count_ = be64(header + 24);

        if (magic != kMagic || version != kVersion || record_size != 0) {
            ::close(fd);
            throw std::runtime_error("Incompatible object tag store " + path +
                                     " (wrong format or record size)");
        }
        if (block_count_ != (count_ + kRecordsPerBlock - 1) / kRecordsPerBlock) {
            ::close(fd);
            throw std::runtime_error("Incompatible object tag store " + path +
                                     " (not a compressed block format)");
        }

        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            throw std::runtime_error("Failed to stat object tag store " + path);
        }
        const uint64_t file_size = static_cast<uint64_t>(st.st_size);

        const uint64_t dir_size = block_count_ * kDirectoryEntrySize;
        const uint64_t dir_start = file_size - dir_size;
        const uint64_t dict_start = dict_size > 0 ? dir_start - dict_size : dir_start;

        if (file_size < kHeaderSize + dict_size + dir_size) {
            ::close(fd);
            throw std::runtime_error("Object tag store " + path + " truncated or incompatible");
        }

        size_ = static_cast<size_t>(file_size);
        data_ = static_cast<const uint8_t*>(
            ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0));
        if (data_ == MAP_FAILED) {
            ::close(fd);
            throw std::runtime_error("Failed to mmap object tag store " + path);
        }
        ::close(fd);
        ::madvise(const_cast<uint8_t*>(data_), size_, MADV_SEQUENTIAL);

        // Read block directory from dir_start
        directory_.reserve(static_cast<size_t>(block_count_));
        uint64_t offset = kHeaderSize;
        for (uint64_t i = 0; i < block_count_; ++i) {
            const uint8_t* p = data_ + dir_start + i * kDirectoryEntrySize;
            const uint32_t comp_size = be32(p + 8);
            directory_.push_back({decode_id(p), offset, comp_size});
            offset += comp_size;
        }
        if (offset != dict_start) {
            throw std::runtime_error("Object tag store " + path +
                                     " corrupted block directory");
        }

        // Read dictionary from dict_start
        if (dict_size > 0) {
            const uint8_t* dict_p = data_ + dict_start;
            const uint32_t dict_total = be32(dict_p);
            if (static_cast<uint64_t>(dict_total) != dict_size) {
                throw std::runtime_error("Object tag store " + path +
                                         " corrupted dictionary size");
            }
            const uint32_t key_count = be32(dict_p + 4);
            const uint8_t* key_p = dict_p + 8;
            id_to_key_.reserve(key_count);
            for (uint32_t i = 0; i < key_count; ++i) {
                if (static_cast<size_t>(dict_p + dict_size - key_p) < 1) {
                    throw std::runtime_error("Object tag store: truncated dictionary key");
                }
                const uint8_t key_len = *key_p++;
                if (static_cast<size_t>(dict_p + dict_size - key_p) < key_len) {
                    throw std::runtime_error("Object tag store: truncated dictionary key value");
                }
                id_to_key_.emplace_back(reinterpret_cast<const char*>(key_p), key_len);
                key_p += key_len;
            }
        }
    }

    ~Reader() {
        if (data_ && data_ != MAP_FAILED) ::munmap(const_cast<uint8_t*>(data_), size_);
    }

    size_t size() const { return static_cast<size_t>(count_); }

    const uint8_t* record(size_t i) const {
        const size_t block = i >> kLog2RecordsPerBlock;
        ensure_block(block);
        const Block& b = directory_[block];
        const size_t idx_in_block = i & (kRecordsPerBlock - 1);
        if (idx_in_block >= b.record_offsets.size()) {
            throw std::runtime_error("Object tag store: record index out of range in block " +
                                     std::to_string(block));
        }
        return cached_buf_.data() + b.record_offsets[idx_in_block];
    }

    int64_t object_id_at(size_t i) const {
        const uint8_t* rec = record(i);
        return decode_id(rec);
    }

    // Returns the tag map for record i, using the dictionary to resolve key IDs.
    TagMap tags_at(size_t i) const {
        const uint8_t* rec = record(i);
        const uint8_t* p = rec + 8;  // skip object_id
        const uint16_t n_tags = be16(p);
        p += 2;
        TagMap tags;
        tags.reserve(n_tags);
        for (uint16_t t = 0; t < n_tags; ++t) {
            if (static_cast<size_t>(end_of_block() - p) < 3) {
                throw std::runtime_error("Object tag store: truncated key id");
            }
            uint16_t key_id = be16(p);
            p += 2;
            if (static_cast<size_t>(key_id) >= id_to_key_.size()) {
                throw std::runtime_error("Object tag store: invalid key id");
            }
            const std::string& key = id_to_key_[key_id];
            const uint8_t val_len = *p++;
            std::string value(reinterpret_cast<const char*>(p), val_len);
            p += val_len;
            tags.emplace_back(key, std::move(value));
        }
        return tags;
    }

private:
    // Returns a pointer one past the end of the current block's decompressed data.
    const uint8_t* end_of_block() const { return cached_buf_.data() + cached_buf_.size(); }

    struct Block {
        int64_t first_id;
        uint64_t offset;
        uint32_t comp_size;
        mutable std::vector<size_t> record_offsets;
        mutable bool offsets_built = false;

        Block() = default;
        Block(int64_t id, uint64_t off, uint32_t cs)
            : first_id(id), offset(off), comp_size(cs) {}
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
            if (pos + 10 > cached_buf_.size()) {  // object_id(8) + n_tags(2) minimum
                throw std::runtime_error("Object tag store: truncated record at block " +
                                         std::to_string(block) + ", record " +
                                         std::to_string(i));
            }
            const uint16_t n_tags = be16(buf + pos + 8);
            size_t rec_size = 10;  // object_id(8) + n_tags(2)
            for (uint16_t t = 0; t < n_tags; ++t) {
                // key_id(2) + val_len(1) + value
                if (pos + rec_size + 3 > cached_buf_.size()) {
                    throw std::runtime_error("Object tag store: truncated key id at block " +
                                             std::to_string(block) + ", record " +
                                             std::to_string(i));
                }
                rec_size += 2;  // key_id
                const uint8_t val_len = buf[pos + rec_size];
                rec_size += 1;  // val_len
                rec_size += val_len;
            }
            pos += rec_size;
        }
        b.offsets_built = true;
    }

    void ensure_block(size_t block) const {
        if (cached_block_ == block) return;
        if (block >= directory_.size()) {
            throw std::runtime_error("Object tag store block index out of range");
        }
        const Block& b = directory_[block];
        unsigned long long frame_size = ZSTD_getFrameContentSize(data_ + b.offset, b.comp_size);
        if (frame_size == ZSTD_CONTENTSIZE_ERROR || frame_size == ZSTD_CONTENTSIZE_UNKNOWN) {
            const size_t raw_size =
                (block + 1 == directory_.size()
                     ? static_cast<size_t>(count_ - block * kRecordsPerBlock)
                     : kRecordsPerBlock) *
                512;
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
        cached_buf_.resize(sz);
        cached_block_ = block;
        build_record_offsets(block);
    }

    uint64_t count_ = 0;
    uint64_t block_count_ = 0;
    size_t size_ = 0;
    const uint8_t* data_ = nullptr;
    uint8_t type_ = 0;
    mutable std::vector<Block> directory_;
    mutable std::vector<uint8_t> cached_buf_;
    mutable size_t cached_block_ = static_cast<size_t>(-1);

    // Dictionary: key_id -> key string
    std::vector<std::string> id_to_key_;
};

}  // namespace object_tag_store
