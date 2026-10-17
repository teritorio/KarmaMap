#include "suspect.hpp"
#include "suspect_lang_store.hpp"

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/object.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/tag.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/visitor.hpp>

#include <arrow/api.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow_table_io.hpp"
#include "h3_utils.hpp"
#include "object_tag_state.hpp"
#include "update.hpp"

namespace suspect {

namespace {

constexpr size_t kFlushThreshold = 1'000'000;  // pending rows per stage flush

template <typename B, typename V>
void append_checked(B& builder, V&& value) {
    if (!builder.Append(std::forward<V>(value)).ok()) {
        throw std::runtime_error("Failed to append value to Arrow builder");
    }
}

template <typename B>
void finish_checked(B& builder, std::shared_ptr<arrow::Array>* out) {
    if (!builder.Finish(out).ok()) {
        throw std::runtime_error("Failed to finalize Arrow builder");
    }
}

std::shared_ptr<arrow::Table> combine_chunks(const std::shared_ptr<arrow::Table>& table) {
    auto result = table->CombineChunks();
    if (!result.ok()) {
        throw std::runtime_error("CombineChunks failed: " + result.status().ToString());
    }
    return *result;
}

template <typename T>
const T* typed_column(const std::shared_ptr<arrow::Table>& table, const std::string& name) {
    const int idx = table->schema()->GetFieldIndex(name);
    if (idx < 0) {
        throw std::runtime_error("Table is missing the '" + name + "' column");
    }
    return static_cast<const T*>(table->column(idx)->chunk(0).get());
}

std::vector<std::string> collect_parquet_recursive(const std::string& root) {
    std::vector<std::string> paths;
    if (!std::filesystem::is_directory(root)) return paths;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_regular_file() && entry.path().extension() == ".parquet") {
            paths.push_back(entry.path().string());
        }
    }
    std::sort(paths.begin(), paths.end());
    return paths;
}

uint64_t staged_seq(const std::string& path) {
    const std::string dir = std::filesystem::path(path).parent_path().filename().string();
    constexpr std::string_view kPrefix = "seq_";
    if (dir.compare(0, kPrefix.size(), kPrefix) != 0) return 0;
    try {
        return std::stoull(dir.substr(kPrefix.size()));
    } catch (const std::exception&) {
        return 0;
    }
}

// ---------------------------------------------------------------------------
// Name-language stage I/O (per-(uid, day, lang) event counts)
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> langs_stage_schema() {
    return arrow::schema({
        arrow::field("uid", arrow::int64(), false),
        arrow::field("username", arrow::utf8(), false),
        arrow::field("day", arrow::uint32(), false),
        arrow::field("lang", arrow::utf8(), false),
        arrow::field("count", arrow::uint32(), false),
    });
}

void write_langs_stage_file(
    const std::string& path,
    const std::unordered_map<UserMinuteLangKey, UserMinuteLangEntry, UserMinuteLangKeyHash>&
        rows) {
    if (rows.empty()) return;

    const int64_t n = static_cast<int64_t>(rows.size());
    arrow::Int64Builder uid_builder;
    arrow::StringBuilder username_builder;
    arrow::UInt32Builder day_builder;
    arrow::StringBuilder lang_builder;
    arrow::UInt32Builder count_builder;

    if (!uid_builder.Reserve(n).ok() || !username_builder.Reserve(n).ok() ||
        !day_builder.Reserve(n).ok() || !lang_builder.Reserve(n).ok() ||
        !count_builder.Reserve(n).ok()) {
        throw std::runtime_error("Reserve() failed while flushing suspect lang stage");
    }
    for (const auto& [key, entry] : rows) {
        append_checked(uid_builder, key.uid);
        append_checked(username_builder, entry.username);
        append_checked(day_builder, key.day);
        append_checked(lang_builder, key.lang);
        append_checked(count_builder, entry.count);
    }

    std::shared_ptr<arrow::Array> uid, username, day, lang, count;
    finish_checked(uid_builder, &uid);
    finish_checked(username_builder, &username);
    finish_checked(day_builder, &day);
    finish_checked(lang_builder, &lang);
    finish_checked(count_builder, &count);

    arrow_table_io::write_table(path,
                                arrow::Table::Make(langs_stage_schema(),
                                                   {uid, username, day, lang, count}));
}

// ---------------------------------------------------------------------------
// Name-language classifier
// ---------------------------------------------------------------------------

static std::vector<std::pair<std::string, std::string>> extract_tag_pairs(
    const osmium::TagList& tags) {
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(tags.size());
    for (const osmium::Tag& tag : tags) {
        out.emplace_back(tag.key(), tag.value());
    }
    return out;
}

static std::string_view find_tag_value(
    const std::vector<std::pair<std::string, std::string>>& tags,
    std::string_view key) {
    for (const auto& [k, v] : tags) {
        if (k == key) return v;
    }
    return {};
}

static bool any_name_lang_has_value(
    const std::vector<std::pair<std::string, std::string>>& tags,
    std::string_view value) {
    for (const auto& [k, v] : tags) {
        std::string lang;
        if (is_name_lang_key(k, &lang) && v == value) return true;
    }
    return false;
}

NameLanguageResult classify_name_language(
    const object_tag_store::TagMap& prev_tags,
    const std::vector<std::pair<std::string, std::string>>& new_tags,
    bool has_bare_name, std::string_view new_name_value) {
    NameLanguageResult result;

    std::map<std::string, std::string> prev_map, new_map;
    for (const auto& [k, v] : prev_tags) prev_map[k] = v;
    for (const auto& [k, v] : new_tags) new_map[k] = v;

    std::set<std::string> prev_langs, new_langs;
    for (const auto& [k, v] : prev_tags) {
        std::string lang;
        if (is_name_lang_key(k, &lang)) prev_langs.insert(lang);
    }
    for (const auto& [k, v] : new_tags) {
        std::string lang;
        if (is_name_lang_key(k, &lang)) new_langs.insert(lang);
    }

    // Wikipedia: map value -> lang for lang-suffixed keys.
    std::map<std::string, std::string> prev_wiki_by_val, new_wiki_by_val;
    for (const auto& [k, v] : prev_tags) {
        std::string lang;
        if (is_wikipedia_key(k, &lang) && !lang.empty()) prev_wiki_by_val[v] = lang;
    }
    for (const auto& [k, v] : new_tags) {
        std::string lang;
        if (is_wikipedia_key(k, &lang) && !lang.empty()) new_wiki_by_val[v] = lang;
    }

    // (e) Wikipedia language prefix swap: same value moved xx -> yy.
    for (const auto& [value, prev_lang] : prev_wiki_by_val) {
        auto it = new_wiki_by_val.find(value);
        if (it != new_wiki_by_val.end() && it->second != prev_lang) {
            result.event = NameLanguageEvent::kWikipediaSwap;
            result.lang = prev_lang;
            return result;
        }
    }

    // (a) name:xx removed: a lang present before but not after.
    for (const std::string& lang : prev_langs) {
        if (!new_langs.count(lang)) {
            result.event = NameLanguageEvent::kNameLangRemoved;
            result.lang = lang;
            return result;
        }
    }

    // (b) bare name changed, old value not kept in name:xx or old_name.
    if (has_bare_name && !new_name_value.empty()) {
        auto prev_name_it = prev_map.find("name");
        if (prev_name_it != prev_map.end() && prev_name_it->second != new_name_value) {
            const std::string& old_name = prev_name_it->second;
            bool preserved = any_name_lang_has_value(new_tags, old_name) ||
                             (new_map.count("old_name") > 0 &&
                              new_map.at("old_name") == old_name);
            if (!preserved && !is_typo_fix(old_name, std::string(new_name_value))) {
                if (!new_langs.empty()) {
                    result.event = NameLanguageEvent::kNameChanged;
                    result.lang = *new_langs.begin();
                    return result;
                }
            }
        }
    }

    // (d) name:xx overwritten with a copy of bare name.
    if (has_bare_name && !new_name_value.empty()) {
        for (const auto& [k, v] : new_tags) {
            std::string lang;
            if (is_name_lang_key(k, &lang) && v == new_name_value) {
                auto prev_it = prev_map.find(k);
                if (prev_it == prev_map.end() || prev_it->second != new_name_value) {
                    result.event = NameLanguageEvent::kNameLangOverwrite;
                    result.lang = lang;
                    return result;
                }
            }
        }
    }

    // (f) bare name deleted entirely.
    if (prev_map.count("name") > 0 && !new_map.count("name")) {
        if (!new_langs.empty()) {
            result.event = NameLanguageEvent::kNameDeleted;
            result.lang = *new_langs.begin();
            return result;
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// Scan handler
// ---------------------------------------------------------------------------

class SuspectLangScanHandler : public osmium::handler::Handler {
public:
    SuspectLangScanHandler(std::string stage_dir, ObjectTagState* tag_state)
        : stage_dir_(std::move(stage_dir)), tag_state_(tag_state) {}

    void node(const osmium::Node& node) {
        process(static_cast<int64_t>(node.id()), static_cast<int64_t>(node.uid()),
                object_user(node), node.timestamp(), extract_tag_pairs(node.tags()),
                node.visible(), static_cast<uint32_t>(node.version()));
    }

    void way(const osmium::Way& way) {
        process(static_cast<int64_t>(way.id()), static_cast<int64_t>(way.uid()),
                object_user(way), way.timestamp(), extract_tag_pairs(way.tags()),
                way.visible(), static_cast<uint32_t>(way.version()));
    }

    void relation(const osmium::Relation& relation) {
        process(static_cast<int64_t>(relation.id()),
                static_cast<int64_t>(relation.uid()), object_user(relation),
                relation.timestamp(), extract_tag_pairs(relation.tags()),
                relation.visible(), static_cast<uint32_t>(relation.version()));
    }

    void finish() { flush_stage(); }

    uint64_t objects() const { return objects_; }
    size_t stage_files() const { return stage_files_; }
    size_t stage_rows() const { return stage_rows_flushed_; }

private:
    static std::string object_user(const osmium::OSMObject& object) {
        const char* user = object.user();
        return std::string(user ? user : "");
    }

    static uint32_t day_from_ts(const osmium::Timestamp& ts) {
        return static_cast<uint32_t>(
            h3_utils::timestamp_to_utc_day(ts.seconds_since_epoch()));
    }

    void process(int64_t object_id, int64_t uid, const std::string& username,
                 const osmium::Timestamp& ts,
                 const std::vector<std::pair<std::string, std::string>>& new_tags,
                 bool visible, uint32_t version) {
        objects_++;
        if (!visible) {
            // Whole-object deleted: clear prior tags from the cache so a
            // later recreation doesn't compare against stale state.
            tag_state_->remove_tags(object_id);
            return;
        }
        if (version == 1) return;  // create

        uint32_t day = day_from_ts(ts);

        const object_tag_store::TagMap& prev_tags = tag_state_->pre(object_id);

        // Determine whether bare name / wikipedia are stored in the new
        // version (stored only when a language variant also exists).
        bool has_name_lang = false;
        bool has_wiki_lang = false;
        for (const auto& [k, v] : new_tags) {
            std::string l;
            if (is_name_lang_key(k, &l)) has_name_lang = true;
            if (is_wikipedia_key(k, &l) && !l.empty()) has_wiki_lang = true;
        }
        // Bare name is stored only when a name:* also exists.
        std::string new_name_value;
        if (has_name_lang) {
            new_name_value = std::string(find_tag_value(new_tags, "name"));
        }
        bool has_bare_name = has_name_lang && !new_name_value.empty();

        NameLanguageResult res =
            classify_name_language(prev_tags, new_tags, has_bare_name, new_name_value);
        if (res.event == NameLanguageEvent::kNone || res.lang.empty()) return;

        stats_.add_object(uid, username, day, res.lang, visible, version);

        // Update the state cache with the new tracked tags.
        object_tag_store::TagMap new_tag_map;
        for (const auto& [k, v] : new_tags) {
            std::string l;
            bool store = is_name_lang_key(k, &l) ||
                         (is_wikipedia_key(k, &l) && !l.empty()) ||
                         (k == "name" && has_name_lang) ||
                         (k == "wikipedia" && has_wiki_lang);
            if (store) new_tag_map.emplace_back(k, v);
        }
        std::sort(new_tag_map.begin(), new_tag_map.end());
        tag_state_->set_tags(object_id, std::move(new_tag_map));

        if (stats_.size() >= kFlushThreshold) flush_stage();
    }

    void flush_stage() {
        if (stats_.langs().empty()) return;
        char name[32];
        std::snprintf(name, sizeof(name), "stage_%05zu.parquet", stage_files_);
        write_langs_stage_file(stage_dir_ + "/" + name, stats_.langs());
        stage_rows_flushed_ += stats_.size();
        stats_.clear();
        stage_files_++;
    }

    std::string stage_dir_;
    ObjectTagState* tag_state_;
    MinuteLangStats stats_;

    uint64_t objects_ = 0;
    size_t stage_files_ = 0;
    size_t stage_rows_flushed_ = 0;
};

// ---------------------------------------------------------------------------
// Step factory for update_pass::run_diff_scans
// ---------------------------------------------------------------------------

class LangsScanStep : public update_pass::DiffScanStep {
public:
    LangsScanStep(const std::string& stage_dir, ObjectTagState* tag_state)
        : handler_(stage_dir, tag_state) {}

    void apply(const osmium::memory::Buffer& buffer) override {
        osmium::apply(buffer, handler_);
    }

    void finish() override {
        handler_.finish();
        std::cerr << "[suspect] lang diff scan objects=" << handler_.objects()
                  << " stage_rows=" << handler_.stage_rows()
                  << " stage_files=" << handler_.stage_files() << "\n";
    }

private:
    SuspectLangScanHandler handler_;
};

}  // namespace

std::unique_ptr<update_pass::DiffScanStep> make_langs_scan_step(
    const std::string& stage_dir, suspect::ObjectTagState* tag_state) {
    return std::make_unique<LangsScanStep>(stage_dir, tag_state);
}

// ---------------------------------------------------------------------------
// Fold lang counts into the persisted binary store
// ---------------------------------------------------------------------------

void fold_lang_counts(const std::string& langs_root, const std::string& langs_path,
                      uint64_t applied_seq) {
    const std::vector<std::string> lang_stage = collect_parquet_recursive(langs_root);
    const bool have_base = std::filesystem::exists(langs_path);

    // Rerun-skip: base already stamped with a sequence >= the run's.
    if (have_base && applied_seq > 0 &&
        suspect_lang_store::applied_seq_of(langs_path) >= applied_seq) {
        std::cerr << "[suspect] lang buckets already folded through " << applied_seq
                  << ", skipping\n";
        std::filesystem::remove_all(langs_root);
        return;
    }
    if (lang_stage.empty()) {
        std::cerr << "[suspect] no lang bucket stage files under " << langs_root
                  << ", nothing to fold\n";
        std::filesystem::remove_all(langs_root);
        return;
    }

    const uint64_t base_seq = have_base ? suspect_lang_store::applied_seq_of(langs_path) : 0;

    // Merge base + staged diffs, summing equal (uid, day, lang) keys.
    std::map<std::tuple<int64_t, uint32_t, std::string>, uint32_t> merged;
    if (have_base) {
        suspect_lang_store::Reader base(langs_path);
        for (size_t i = 0; i < base.size(); ++i) {
            merged[{base.uid_at(i), base.day_at(i), base.lang_at(i)}] += base.count_at(i);
        }
    }
    for (const std::string& path : lang_stage) {
        if (base_seq > 0 && staged_seq(path) <= base_seq) {
            std::cerr << "[suspect] lang stage " << path
                      << " already folded (seq <= " << base_seq << "), skipping\n";
            continue;
        }
        const std::shared_ptr<arrow::Table> table = combine_chunks(
            arrow_table_io::read_table(path));
        const auto* uids = typed_column<arrow::Int64Array>(table, "uid");
        const auto* days = typed_column<arrow::UInt32Array>(table, "day");
        const auto* langs = typed_column<arrow::StringArray>(table, "lang");
        const auto* counts = typed_column<arrow::UInt32Array>(table, "count");
        for (int64_t i = 0; i < table->num_rows(); ++i) {
            merged[{uids->Value(i), days->Value(i), langs->GetString(i)}] +=
                counts->Value(i);
        }
    }

    suspect_lang_store::Writer writer(langs_path);
    writer.set_applied_seq(applied_seq);
    for (const auto& [key, count] : merged) {
        writer.add(std::get<0>(key), std::get<1>(key), std::get<2>(key), count);
    }
    writer.finish();
    std::cerr << "[suspect] folded " << merged.size() << " lang buckets into "
              << langs_path << "\n";
    std::filesystem::remove_all(langs_root);
}

// ---------------------------------------------------------------------------
// Flag lang days
// ---------------------------------------------------------------------------

std::map<std::pair<int64_t, uint16_t>, NameLanguageDay> flagged_lang_days(
    const std::string& lang_path) {
    std::map<std::pair<int64_t, uint16_t>, NameLanguageDay> days;
    if (!std::filesystem::exists(lang_path)) return days;
    suspect_lang_store::Reader reader(lang_path);

    // The store is sorted by (uid, day, lang). Group by (uid, day).
    std::vector<std::tuple<uint32_t, std::string, uint32_t>> series;
    int64_t cur_uid = 0;
    uint32_t cur_day = 0;
    bool have_uid = false;
    const auto flush = [&]() {
        if (!have_uid) return;
        // Find the max-count language for this uid/day.
        uint32_t best_count = 0;
        std::string best_lang;
        uint32_t total = 0;
        for (const auto& [day, lang, count] : series) {
            total += count;
            if (count > best_count) {
                best_count = count;
                best_lang = lang;
            }
        }
        // Flag if any bucket clears the min-events gate.
        if (best_count >= kNameLanguageMinEvents) {
            if (cur_day > h3_utils::kMaxUint16Day) {
                throw std::runtime_error("Lang bucket day out of uint16 range");
            }
            NameLanguageDay& entry = days[{cur_uid, static_cast<uint16_t>(cur_day)}];
            entry.flags = kFlagNameLanguage;
            entry.event_count = total;
            entry.primary_lang = best_lang;
            entry.primary_count = best_count;
        }
        series.clear();
    };
    for (size_t i = 0; i < reader.size(); ++i) {
        const int64_t uid = reader.uid_at(i);
        const uint32_t day = reader.day_at(i);
        if (have_uid && (uid != cur_uid || day != cur_day)) flush();
        cur_uid = uid;
        cur_day = day;
        have_uid = true;
        series.emplace_back(day, reader.lang_at(i), reader.count_at(i));
    }
    flush();
    return days;
}

}  // namespace suspect
