#include "object_tag_state.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace object_tag_delta {

void write_delta(const std::string& path,
                 const std::unordered_map<int64_t, suspect::TagOverlayEntry>& overlay,
                 const std::unordered_set<int64_t>& deleted) {
    const std::string tmp = path + ".tmp";
    const std::filesystem::path dir = std::filesystem::path(path).parent_path();
    if (!dir.empty()) std::filesystem::create_directories(dir);

    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to create object tag delta " + tmp);
    }

    // Collect all entries: overlay (non-deleted) + deleted set.
    std::vector<std::pair<int64_t, const suspect::TagOverlayEntry*>> entries;
    entries.reserve(overlay.size() + deleted.size());
    for (const auto& [id, entry] : overlay) {
        entries.emplace_back(id, &entry);
    }
    for (int64_t id : deleted) {
        entries.emplace_back(id, nullptr);  // nullptr = tombstone
    }
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    char header[kHeaderSize] = {};
    object_tag_store::put_be32(header, kMagic);
    object_tag_store::put_be32(header + 4, kVersion);
    object_tag_store::put_be64(header + 8, static_cast<uint64_t>(entries.size()));
    object_tag_store::ensure_write(fd, header, sizeof(header), "object tag delta header");

    for (const auto& [id, entry] : entries) {
        // [object_id:8][n_tags:1][deleted:1][pairs...]
        std::vector<char> rec;
        rec.reserve(64);
        char id_buf[8];
        object_tag_store::put_be64(id_buf, static_cast<uint64_t>(id) ^ (1ULL << 63));
        rec.insert(rec.end(), id_buf, id_buf + 8);

        uint8_t n_tags = 0;
        uint8_t deleted_flag = (entry == nullptr) ? 1 : 0;
        if (entry != nullptr && !entry->deleted) n_tags = static_cast<uint8_t>(entry->tags.size());
        rec.push_back(static_cast<char>(n_tags));
        rec.push_back(static_cast<char>(deleted_flag));

        if (entry != nullptr && !entry->deleted) {
            for (const auto& [key, value] : entry->tags) {
                rec.push_back(static_cast<char>(key.size()));
                rec.insert(rec.end(), key.begin(), key.end());
                rec.push_back(static_cast<char>(value.size()));
                rec.insert(rec.end(), value.begin(), value.end());
            }
        }
        object_tag_store::ensure_write(fd, rec.data(), rec.size(), "object tag delta record");
    }

    if (::fsync(fd) != 0) {
        ::close(fd);
        throw std::runtime_error("Failed to fsync object tag delta");
    }
    ::close(fd);
    std::filesystem::rename(tmp, path);
}

bool read_delta(const std::string& path,
                std::unordered_map<int64_t, suspect::TagOverlayEntry>& overlay,
                std::unordered_set<int64_t>& deleted) {
    if (!std::filesystem::exists(path)) return false;

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;

    uint8_t header[kHeaderSize];
    ssize_t n = ::read(fd, header, sizeof(header));
    if (n != static_cast<ssize_t>(kHeaderSize)) {
        ::close(fd);
        return false;
    }
    const uint32_t magic = object_tag_store::be32(header);
    const uint32_t version = object_tag_store::be32(header + 4);
    const uint64_t count = object_tag_store::be64(header + 8);

    if (magic != kMagic || version != kVersion) {
        ::close(fd);
        return false;
    }

    for (uint64_t i = 0; i < count; ++i) {
        uint8_t id_buf[8];
        if (::read(fd, id_buf, 8) != 8) break;
        uint64_t enc_id = 0;
        for (int j = 0; j < 8; ++j) enc_id = (enc_id << 8) | id_buf[j];
        int64_t id = static_cast<int64_t>(enc_id ^ (1ULL << 63));

        uint8_t n_tags = 0, deleted_flag = 0;
        if (::read(fd, &n_tags, 1) != 1) break;
        if (::read(fd, &deleted_flag, 1) != 1) break;

        suspect::TagOverlayEntry entry;
        entry.deleted = (deleted_flag != 0);

        for (uint8_t t = 0; t < n_tags; ++t) {
            uint8_t key_len = 0;
            if (::read(fd, &key_len, 1) != 1) break;
            std::string key(key_len, '\0');
            if (key_len > 0 && ::read(fd, &key[0], key_len) != static_cast<ssize_t>(key_len)) break;
            uint8_t val_len = 0;
            if (::read(fd, &val_len, 1) != 1) break;
            std::string value(val_len, '\0');
            if (val_len > 0 && ::read(fd, &value[0], val_len) != static_cast<ssize_t>(val_len)) break;
            entry.tags.emplace_back(std::move(key), std::move(value));
        }

        if (entry.deleted) {
            deleted.insert(id);
        } else {
            overlay[id] = std::move(entry);
        }
    }

    ::close(fd);
    return true;
}

}  // namespace object_tag_delta
