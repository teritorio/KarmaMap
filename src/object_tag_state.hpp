#pragma once

// ObjectTagState: per-object prior-tag cache with base + overlay + delta
// sidecar, mirroring update_pass::NodeState. Holds the name-family tags of
// every object that currently carries at least one language variant
// (name:xx / wikipedia:xx). Used by name-language to compare before/after tags
// across diff batches.
//
// Base file: <path>        (mmap'd, read-only, sorted by object id)
// Delta sidecar: <path>.delta (overlay accumulated during a run)

#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "object_tag_store.hpp"

namespace suspect {

// In-memory overlay entry: the new tag set for an object, or a tombstone.
struct TagOverlayEntry {
    bool deleted = false;
    object_tag_store::TagMap tags;
};

// Loads/saves the delta sidecar. Declared in object_tag_delta.cpp.
}  // namespace suspect

namespace object_tag_delta {
constexpr uint32_t kMagic = 0x44454C54;  // "DELT"
constexpr uint32_t kVersion = 1;
constexpr size_t kHeaderSize = 16;

void write_delta(const std::string& path,
                 const std::unordered_map<int64_t, suspect::TagOverlayEntry>& overlay,
                 const std::unordered_set<int64_t>& deleted);
bool read_delta(const std::string& path,
                std::unordered_map<int64_t, suspect::TagOverlayEntry>& overlay,
                std::unordered_set<int64_t>& deleted);
}  // namespace object_tag_delta

namespace suspect {

class ObjectTagState {
public:
    // `base_path` is the base store file. The delta sidecar is
    // `base_path + ".delta"`. The entire base is loaded into an in-memory
    // hash map at construction (only objects with language tags are stored,
    // so the working set is small).
    explicit ObjectTagState(const std::string& base_path)
        : base_path_(base_path), delta_path_(base_path + ".delta") {
        if (std::filesystem::exists(base_path_)) {
            object_tag_store::Reader base(base_path_);
            for (size_t i = 0; i < base.size(); ++i) {
                base_tags_[base.object_id_at(i)] = base.tags_at(i);
            }
        }
        // Load any existing delta sidecar as the initial overlay.
        object_tag_delta::read_delta(delta_path_, overlay_, deleted_);
    }

    ObjectTagState(const ObjectTagState&) = delete;
    ObjectTagState& operator=(const ObjectTagState&) = delete;

    ~ObjectTagState() = default;

    // Tags before this run's changes for `id` (overlay else base; deleted or
    // absent → empty).
    const object_tag_store::TagMap& pre(int64_t id) const {
        auto it = overlay_.find(id);
        if (it != overlay_.end()) {
            if (it->second.deleted) {
                static const object_tag_store::TagMap empty;
                return empty;
            }
            return it->second.tags;
        }
        if (deleted_.count(id)) {
            static const object_tag_store::TagMap empty;
            return empty;
        }
        auto it2 = base_tags_.find(id);
        if (it2 != base_tags_.end()) return it2->second;
        static const object_tag_store::TagMap empty;
        return empty;
    }

    // Records that `id` now carries `tags` (replaces prior). Called during
    // the scan after classifying the object.
    void set_tags(int64_t id, object_tag_store::TagMap tags) {
        deleted_.erase(id);
        overlay_[id] = TagOverlayEntry{false, std::move(tags)};
    }

    // Records that `id` was deleted (no longer exists). Returns its prior
    // tags (for the caller to classify).
    const object_tag_store::TagMap& remove_tags(int64_t id) {
        const object_tag_store::TagMap& prior = pre(id);
        overlay_[id] = TagOverlayEntry{true, {}};
        deleted_.insert(id);
        return prior;
    }

    // Number of overlay entries plus deletions (for compaction threshold).
    // Deleted objects are tombstones in overlay_ and also in deleted_, so
    // count live overlay entries plus distinct deletions.
    size_t changed_count() const {
        size_t live_overlay = 0;
        for (const auto& [_, entry] : overlay_) {
            if (!entry.deleted) ++live_overlay;
        }
        return live_overlay + deleted_.size();
    }

    // Number of objects in the base store (for compaction threshold).
    size_t base_size() const { return base_tags_.size(); }

    // Phase 1: durably writes overlay + deletions to the delta sidecar.
    void persist_delta() const {
        if (changed_count() == 0) return;
        object_tag_delta::write_delta(delta_path_, overlay_, deleted_);
    }

    // Phase 2: rewrites the base as base + overlay minus deletions, then
    // drops the delta sidecar. Only called after persist_delta() reported
    // enough changes.
    void compact() {
        // Collect all live object ids with their final tags.
        std::map<int64_t, object_tag_store::TagMap> merged(base_tags_.begin(),
                                                           base_tags_.end());
        for (const auto& [id, entry] : overlay_) {
            if (!entry.deleted && !deleted_.count(id)) {
                merged[id] = entry.tags;
            }
        }
        for (int64_t id : deleted_) {
            merged.erase(id);
        }

        // Rewrite base tmp+rename.
        const std::string tmp = base_path_ + ".tmp";
        {
            object_tag_store::Writer writer(tmp);
            for (const auto& [id, tags] : merged) {
                writer.add(id, tags);
            }
            writer.finish();
        }
        std::filesystem::rename(tmp, base_path_);

        // Re-load base from the new file.
        base_tags_.clear();
        if (std::filesystem::exists(base_path_)) {
            object_tag_store::Reader base(base_path_);
            for (size_t i = 0; i < base.size(); ++i) {
                base_tags_[base.object_id_at(i)] = base.tags_at(i);
            }
        }

        // Drop delta sidecar and clear overlay.
        std::error_code ec;
        std::filesystem::remove(delta_path_, ec);
        overlay_.clear();
        deleted_.clear();
    }

private:
    std::string base_path_;
    std::string delta_path_;
    std::unordered_map<int64_t, object_tag_store::TagMap> base_tags_;
    std::unordered_map<int64_t, TagOverlayEntry> overlay_;
    std::unordered_set<int64_t> deleted_;
};

}  // namespace suspect
