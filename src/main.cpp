// karmamap: daily count of OSM changes per H3 cell.
//
// Command verbs:
//   import <planet.osh.pbf>   build a partitioned Parquet dataset under
//     changes/year=YYYY/data.parquet  (h3_cell, change_date, count)
//     by running the node pass (writes the mmap node-position cache + counts
//     nodes into .../nodes.parquet), way pass (resolves node positions by a
//     batched sweep over the cache, counts ways at the distinct cells of
//     their node positions into .../ways.parquet), and the merge pass
//     (merges each year's node and way counts into a single count column of
//     data.parquet, sorted by (h3_cell, change_date) so that Parquet row
//     group min/max statistics become useful for bbox and date-range
//     pruning), plus the per-user/per-day history pass.
//   prepare-update            build the .last incremental cache (one record per
//     node, last known h3 cell, no day) from the node cache and record the
//     update stream URL in manifest.json (the recorded sequence and timestamp
//     are kept; no state.txt is fetched).
//   update [N]                advance the dataset along its replication diff
//     stream (see run_update_mode).

#include <osmium/handler.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/osm/timestamp.hpp>
#include <osmium/visitor.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

#include "country_index.hpp"
#include "file_lock.hpp"
#include "manifest.hpp"
#include "object_tag_state.hpp"
#include "object_tag_store.hpp"
#include "geofabrik_cookie.hpp"
#include "node_cache.hpp"
#include "node_cache_handler.hpp"
#include "options.hpp"
#include "partitioned_parquet_writer.hpp"
#include "sort_pass.hpp"
#include "state.hpp"
#include "update.hpp"
#include "users_history.hpp"
#include "suspect.hpp"
#include "way_processor.hpp"

namespace {

int64_t elapsed_ms(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

// Times one finalize step and logs its wall-clock duration, matching the
// pass-level "done in" lines. Returns the step's result, or void.
template <typename F>
auto timed_step(const char* label, F fn) {
    auto start = std::chrono::steady_clock::now();
    if constexpr (std::is_void_v<std::invoke_result_t<F>>) {
        fn();
        std::cerr << label << " done in " << elapsed_ms(start) << "ms\n";
    } else {
        auto result = fn();
        std::cerr << label << " done in " << elapsed_ms(start) << "ms\n";
        return result;
    }
}

// Set to true when SIGINT or SIGTERM is received. The handler only touches an
// atomic flag (async-signal-safe); all cleanup happens in the read loops below.
std::atomic<bool> g_stop_requested{false};

void signal_handler(int sig) {
    g_stop_requested.store(true, std::memory_order_relaxed);
}

// Wipes a dataset root from a previous run, so a re-run never leaves stale
// year=YYYY partition files behind. No-op if absent.
void reset_dataset_root(const std::string& root) {
    std::filesystem::remove_all(root);
}

// Writes output-dir/manifest.json and logs the step. Update mode calls this
// inline (before the deferred base rebuild); every other stage calls it from
// main once its passes are done.
void write_manifest_output(const Options& opts,
                           const std::optional<replication_state::State>& source,
                           const std::optional<std::pair<double, double>>& spatial_center) {
    std::cerr << "[manifest] writing " << opts.output_dir << "/manifest.json\n";
    timed_step("[manifest] write", [&] {
        manifest::write_manifest(opts.output_dir, opts.h3_resolution, source, spatial_center);
    });
}

// Deletes a freshly applied diff file. The removal is the last step of a
// sequence: a failure here aborts the run loudly instead of continuing into
// the merge while believing the file was consumed.
void remove_applied_diff(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        throw std::runtime_error("Failed to remove applied diff " + path + ": " +
                                 ec.message());
    }
}

// The cookie jar used for the update stream, or "" when the update URL does
// not sit behind the authenticated Geofabrik internal server. For the
// internal host the OSM-session jar is obtained or refreshed up front (from
// OSM_GEOFABRIK_USER/OSM_GEOFABRIK_PASSWORD in .env) so both the state fetch
// and every diff download can use it.
std::string resolve_update_cookie(const Options& opts) {
    if (!geofabrik_cookie::requires_auth(opts.update_url)) return "";
    std::string jar =
        opts.cookie_path.empty()
            ? geofabrik_cookie::default_cookie_path(
                  std::filesystem::path(opts.node_cache_path).parent_path().string())
            : opts.cookie_path;
    if (!geofabrik_cookie::has_credentials() && !std::filesystem::exists(jar)) {
        throw std::runtime_error(
            "The update URL points at the Geofabrik internal server "
            "(osm-internal.download.geofabrik.de), which requires an OSM "
            "account; set OSM_GEOFABRIK_USER/OSM_GEOFABRIK_PASSWORD in .env "
            "(or pass --cookie with an existing jar)");
    }
    if (geofabrik_cookie::has_credentials()) {
        std::cerr << "[auth] ensuring Geofabrik cookie at " << jar << "\n";
        geofabrik_cookie::ensure_valid_cookie(jar);
    }
    return jar;
}

// Collect the name-family tags worth caching for the prior-tag cache: every
// name:* and wikipedia:* tag, plus the bare "name"/"wikipedia" keys only when
// a language variant of the same family is also present. Sorted by key, the
// order ObjectTagState::compact()'s base build expects. (n_tags is stored as
// a 2-byte field, so objects with more than 255 name tags are supported.)
object_tag_store::TagMap extract_name_tags(const osmium::TagList& tags) {
    bool has_name_lang = false;
    bool has_wiki_lang = false;
    for (const osmium::Tag& tag : tags) {
        std::string lang;
        if (suspect::is_name_lang_key(tag.key(), &lang)) has_name_lang = true;
        if (suspect::is_wikipedia_key(tag.key(), &lang) && !lang.empty()) has_wiki_lang = true;
    }
    object_tag_store::TagMap out;
    for (const osmium::Tag& tag : tags) {
        std::string lang;
        const bool store =
            suspect::is_name_lang_key(tag.key(), &lang) ||
            (suspect::is_wikipedia_key(tag.key(), &lang) && !lang.empty()) ||
            (tag.key() == "name" && has_name_lang) ||
            (tag.key() == "wikipedia" && has_wiki_lang);
        if (store) {
            out.emplace_back(std::string(tag.key()), tag.value());
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Streams every object of the full-history input and records its
// name-family tags. Only the last version of each object is kept; the
// previous base is never reused, so nothing is accumulated in RAM beyond
// the current object's tags.
//
// Writes to three separate files (object_tags.nodes/.ways/.relations.cache),
// one per object type, so object_id needs only to be ascending within each
// type. Input must follow OSM convention: nodes (ascending id), then ways,
// then relations.
class TagCacheHandler : public osmium::handler::Handler {
public:
    TagCacheHandler(object_tag_store::Writer& node_writer,
                    object_tag_store::Writer& way_writer,
                    object_tag_store::Writer& relation_writer) {
        writers_[0] = &node_writer;
        writers_[1] = &way_writer;
        writers_[2] = &relation_writer;
    }

    void node(const osmium::Node& node) {
        process(ObjType::node, node.id(), node.tags(), node.visible());
    }

    void way(const osmium::Way& way) {
        process(ObjType::way, way.id(), way.tags(), way.visible());
    }

    void relation(const osmium::Relation& relation) {
        process(ObjType::relation, relation.id(), relation.tags(), relation.visible());
    }

    void finish() {
        flush_current();
    }

    uint64_t objects() const { return objects_; }
    uint64_t cached() const { return cached_; }

private:
    enum class ObjType { node, way, relation };

    static const char* type_name(ObjType t) {
        switch (t) {
            case ObjType::node: return "node";
            case ObjType::way: return "way";
            case ObjType::relation: return "relation";
        }
        return "unknown";
    }

    void process(ObjType type, int64_t object_id, const osmium::TagList& tags, bool visible) {
        objects_++;
        if (type != current_type_) {
            flush_current();
            current_type_ = type;
            current_id_ = 0;  // reset id check for the new type
        }
        if (object_id != current_id_) {
            flush_current();
            current_id_ = object_id;
        }
        if (!visible) {
            current_tags_.clear();  // deletion wins
        } else {
            current_tags_ = extract_name_tags(tags);  // last version wins
        }
    }

    void flush_current() {
        if (!current_tags_.empty()) {
            try {
                writers_[static_cast<int>(current_type_)]->add(current_id_, current_tags_);
                ++cached_;
            } catch (const std::runtime_error& e) {
                throw std::runtime_error(
                    "Object tag store input not sorted or duplicated: " +
                    std::string(type_name(current_type_)) + " object_id=" +
                    std::to_string(current_id_) +
                    " error: " + e.what());
            }
        }
        current_tags_.clear();
    }

    object_tag_store::Writer* writers_[3];
    ObjType current_type_ = ObjType::node;
    int64_t current_id_ = 0;
    object_tag_store::TagMap current_tags_;
    uint64_t objects_ = 0;
    uint64_t cached_ = 0;
};

// Builds the name tag cache (object_tags.cache) from the full-history input,
// mirroring how the node pass builds the node position cache: wiped and
// rebuilt from scratch so the base reflects the current state of the input.
// Writes three type-specific files: object_tags.nodes/.ways/.relations.cache.
void build_tag_cache(const Options& opts) {
    const std::filesystem::path cache_parent =
        std::filesystem::path(opts.node_cache_path).parent_path();
    const std::string tag_cache_path =
        (cache_parent / "object_tags.cache").lexically_normal().string();

    // Wipe any previous base files; the cache is rebuilt from scratch, never reused.
    std::filesystem::remove(cache_parent / "object_tags.nodes.cache");
    std::filesystem::remove(cache_parent / "object_tags.nodes.cache.delta");
    std::filesystem::remove(cache_parent / "object_tags.ways.cache");
    std::filesystem::remove(cache_parent / "object_tags.ways.cache.delta");
    std::filesystem::remove(cache_parent / "object_tags.relations.cache");
    std::filesystem::remove(cache_parent / "object_tags.relations.cache.delta");
    std::filesystem::create_directories(cache_parent);

    object_tag_store::Writer node_writer(tag_cache_path, 0);
    object_tag_store::Writer way_writer(tag_cache_path, 1);
    object_tag_store::Writer relation_writer(tag_cache_path, 2);

    osmium::io::File input_file(opts.input_path);
    osmium::io::Reader reader(input_file,
                             osmium::osm_entity_bits::node |
                                 osmium::osm_entity_bits::way |
                                 osmium::osm_entity_bits::relation);
    TagCacheHandler handler(node_writer, way_writer, relation_writer);
    while (osmium::memory::Buffer buf = reader.read()) {
        if (g_stop_requested.load(std::memory_order_relaxed)) {
            break;
        }
        osmium::apply(buf, handler);
    }
    reader.close();
    handler.finish();
    node_writer.finish();
    way_writer.finish();
    relation_writer.finish();

    if (g_stop_requested.load(std::memory_order_relaxed)) {
        std::cerr << "[tag cache] interrupted by signal\n";
        throw std::runtime_error("Interrupted by signal");
    }

    std::cerr << "[tag cache] wrote " << tag_cache_path
              << " objects=" << handler.objects()
              << " cached=" << handler.cached() << "\n";
}

void run_node_pass(const Options& opts) {
    const std::string root = opts.output_dir + "/changes";
    std::cerr << "[node pass] -> " << root << "\n";
    reset_dataset_root(root);  // wipe any stale partition files from an earlier run

    // The cache is derived data with a private format; wipe it before
    // rebuilding so a stale cache is never reused. No-op if absent.
    std::filesystem::remove_all(opts.node_cache_path);
    const std::filesystem::path cache_dir =
        std::filesystem::path(opts.node_cache_path).parent_path();
    if (!cache_dir.empty()) std::filesystem::create_directories(cache_dir);

    parquet_out::PartitionedParquetWriter parquet_writer(root, "nodes.parquet");
    node_cache::Writer cache_writer(opts.node_cache_path, opts.h3_resolution);

    osmium::io::File input_file(opts.input_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::node);

    node_pass::NodeCacheHandler handler(&cache_writer, &parquet_writer, opts.h3_resolution);

    auto start = std::chrono::steady_clock::now();
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();

    handler.finish();  // also finishes the cache writer (patches its header)
    parquet_writer.finish();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[node pass] done in " << elapsed << "s\n";
    std::cerr << "[node pass] nodes=" << handler.nodes()
              << " cache_writes=" << handler.cache_writes()
              << " cache_records=" << cache_writer.records()
              << " cache_bytes=" << cache_writer.bytes() << "\n";
}

void run_way_pass(const Options& opts) {
    const std::string root = opts.output_dir + "/changes";
    std::cerr << "[way pass] -> " << root << "\n";

    // Writes into pass 1's existing changes/ root so both staging files
    // share the year directories; does not wipe it (pass 2 may run alone).
    parquet_out::PartitionedParquetWriter parquet_writer(root, "ways.parquet");

    node_cache::Reader cache_reader(opts.node_cache_path, opts.h3_resolution);

    osmium::io::File input_file(opts.input_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::way);

    way_pass::WayProcessor handler(&cache_reader, &parquet_writer, opts.way_batch_bytes);

    auto start = std::chrono::steady_clock::now();
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();

    handler.finish();
    parquet_writer.finish();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[way pass] done in " << elapsed << "s\n";
    handler.print_stats();
}

std::optional<std::pair<double, double>> run_sort_pass(const Options& opts) {
    std::cerr << "[sort pass] merging and sorting partitions under " << opts.output_dir << "/changes\n";

    auto start = std::chrono::steady_clock::now();

    auto center = sort_pass::merge_and_sort_partitions(opts.output_dir + "/changes",
                                                       opts.change_group_rows);

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[sort pass] done in " << elapsed << "s\n";

    return center;
}

void run_step4_pass(const Options& opts) {
    std::cerr << "[step 4] incremental cache -> " << opts.node_cache_last_path << "\n";

    // Derived from the node cache: records sorted by (node_id, day) mean the
    // last record per node_id is its last known position, so a straight sweep
    // feeding the collapsing writer drops the day column for free.
    node_cache::Reader history_reader(opts.node_cache_path, opts.h3_resolution);
    node_cache::incremental::Writer cache_writer(opts.node_cache_last_path,
                                                 opts.h3_resolution);

    auto start = std::chrono::steady_clock::now();
    const size_t history_records = history_reader.size();
    for (size_t i = 0; i < history_records; ++i) {
        cache_writer.add(history_reader.node_at(i), history_reader.cell_at(i));
    }
    const uint64_t records = cache_writer.records();
    cache_writer.finish();

    // The history cache was only an intermediate from which the incremental
    // cache was collapsed; it is never read again (import wipes it, update and
    // later prepare-update runs use the .last cache). Free its space once the
    // new cache is final, unless the two paths coincide by user error.
    if (opts.node_cache_path != opts.node_cache_last_path) {
        std::filesystem::remove(opts.node_cache_path);
    }
    // The freshly collapsed base carries every node, so any delta sidecar an
    // earlier update run left is now stale and must not re-apply.
    std::filesystem::remove(update_pass::NodeState::delta_path(opts.node_cache_last_path));

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[step 4] done in " << elapsed << "s\n";
    std::cerr << "[step 4] history_records=" << history_records
              << " nodes=" << records
              << " cache_bytes=" << cache_writer.bytes() << "\n";
}

void run_users_history_pass(const Options& opts) {
    const std::string stage_dir = opts.output_dir + "/users_history_stage";
    const std::string history_path = opts.output_dir + "/users_history.parquet";

    // A re-run never reuses stale outputs: wipe stage + final files before
    // scanning, so an empty scan cannot leave last run's rows behind.
    std::filesystem::remove_all(stage_dir);
    std::filesystem::remove(history_path);
    std::filesystem::remove(opts.output_dir + "/user_ranking.parquet");
    std::filesystem::remove(opts.output_dir + "/suspect.parquet");

    users_history::run_scan(opts.input_path, stage_dir);
    users_history::run_finalize(stage_dir, history_path, opts.users_history_group_rows,
                                opts.ranking_group_rows);
}

// Update mode: advances an existing dataset along its replication diff stream
// (see options.cpp, the "update" command). Starting from the sequence recorded
// in manifest.json's source block, every diff up to the target (the current
// state.txt, capped by "update [N]") is downloaded to the diffs dir (next to
// the node caches) and applied by the update node and way passes into
// per-sequence staging
// partitions (nodes.<seq>.parquet / ways.<seq>.parquet). Diffs already
// committed by earlier runs (at or below the recorded base sequence) are
// purged on start, and each freshly downloaded diff is removed once every
// pass over it succeeded. Once after all diffs the flat incremental cache is
// rebuilt (base + overlay minus deletions) and the staging partitions are
// merged into data.parquet, so N diffs never rewrite the dataset N times.
// A diff download failure stops the run but does not discard progress: the
// already downloaded diffs are applied and the same commit runs for the
// resulting prefix, so the next run resumes from there. Only a failure before
// the first diff is applied aborts without committing.
// Returns the provenance to write to manifest (the applied sequence, with
// the fetched state.txt timestamp, or the last applied diff's timestamp when a
// download failure stops the run early), sets spatial_center if computed, and
// sets stopped_early_out when the run committed a partial prefix.
std::optional<replication_state::State> run_update_mode(
    const Options& opts, const replication_state::State& current,
    std::optional<std::pair<double, double>>& spatial_center,
    bool& stopped_early_out) {
    stopped_early_out = false;
    std::cerr << "[update] fetching diff stream state below " << current.url << "\n";
    const std::string cookie = resolve_update_cookie(opts);

    const std::optional<replication_state::State> dataset_source =
        manifest::read_source(opts.output_dir);
    if (!dataset_source) {
        throw std::runtime_error(
            "karmamap update needs the sequence the dataset is at, but no "
            "source provenance block exists in " +
            opts.output_dir +
            "/manifest.json; build the dataset first with 'karmamap import "
            "<file> --update-url <url>'");
    }
    if (replication_state::normalize_update_url(opts.update_url) != dataset_source->url) {
        throw std::runtime_error("--update-url " + opts.update_url +
                                 " does not match the dataset's source " + dataset_source->url +
                                 "; update only advances a dataset along its "
                                 "originating diff stream");
    }

    const uint64_t base_seq = dataset_source->sequence_number;
    const uint64_t first = base_seq + 1;
    const uint64_t target =
        opts.max_update_diffs > 0
            ? std::min(current.sequence_number,
                       base_seq + static_cast<uint64_t>(opts.max_update_diffs))
            : current.sequence_number;
    // Purge diffs already committed by earlier runs: the manifest's recorded
    // sequence only advances after the merge, so any <seq>.osc.gz at or below
    // it was fully applied and this run never re-fetches it. Best-effort: a
    // leftover here is an inert download cache, so a removal failure warns
    // instead of aborting the update.
    const std::string diffs_dir = opts.diffs_dir;
    if (std::filesystem::is_directory(diffs_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(diffs_dir)) {
            if (!entry.is_regular_file()) continue;
            const auto seq =
                replication_state::diff_file_sequence(entry.path().filename().string());
            if (!seq || *seq > base_seq) continue;
            const std::string path = entry.path().string();
            std::error_code ec;
            std::filesystem::remove(path, ec);
            if (ec) {
                std::cerr << "[update] warning: failed to purge committed diff "
                          << path << ": " << ec.message() << "\n";
            }
        }
    }
    if (first > target) {
        std::cerr << "[update] already at sequence " << base_seq
                  << " (state.txt=" << current.sequence_number << "); nothing to apply\n";
        // Nothing was applied, but the manifest is still refreshed (footer
        // sizes and date range from the existing files), as before.
        write_manifest_output(opts, current, spatial_center);
        return current;
    }

    const std::string changes_root = opts.output_dir + "/changes";
    if (!std::filesystem::is_directory(changes_root)) {
        throw std::runtime_error("karmamap update needs an existing dataset, but " +
                                 changes_root + " was not found");
    }
    if (!std::filesystem::exists(opts.node_cache_last_path)) {
        throw std::runtime_error("karmamap update needs the incremental cache " +
                                 opts.node_cache_last_path +
                                 "; build it first with 'karmamap prepare-update "
                                 "--update-url <url>'");
    }

    std::cerr << "[update] applying sequences " << first << ".." << target << "\n";

    update_pass::NodeState node_state(opts.node_cache_last_path, opts.h3_resolution);
    const std::string history_stage_root =
        opts.output_dir + "/users_history_update_stage";
    const std::string suspect_stage_root = opts.output_dir + "/suspect_update_stage";
    // Stage groups from a crashed earlier run may cover sequences this run
    // does not re-scan; drop them so finalize only folds what was applied.
    std::filesystem::remove_all(history_stage_root);
    std::filesystem::remove_all(suspect_stage_root);
    // Filter 2 stages go under suspect_update_stage/counts/seq_<n>; the
    // move sink stages filter 3 under suspect_update_stage/moves/seq_<n>;
    // Filter 4 stages go under suspect_update_stage/cells/seq_<n>;
    // Filter 5 stages go under suspect_update_stage/tags/seq_<n>.
    suspect::NodeMoveSink move_sink(suspect_stage_root + "/moves");
    // Name-language: persistent prior-tag cache (object → tags), base + delta
    // sidecar + rebuild, mirroring the incremental node cache strategy.
    // Lives next to the node caches, like suspect_minutes.bin etc.
    const std::filesystem::path cache_parent =
        std::filesystem::path(opts.node_cache_path).parent_path();
    const std::string tag_cache_path =
        (cache_parent / "object_tags.cache").lexically_normal().string();
    suspect::ObjectTagState node_tag_state(tag_cache_path, 0);
    suspect::ObjectTagState way_tag_state(tag_cache_path, 1);
    suspect::ObjectTagState relation_tag_state(tag_cache_path, 2);

    // Diffs are merged into diffs_dir/merged.osc between flushes (a batch of a
    // single trailing diff skips the merge); the merged file and the batch's
    // diff files are removed once every pass over them ran.
    const std::string merged_path = diffs_dir + "/merged.osc";
    std::error_code ignore_ec;
    std::filesystem::remove(merged_path, ignore_ec);  // stale from a crashed run

    uint64_t applied = base_seq;
    update_pass::DiffMerger merger;
    std::vector<std::string> batch_diffs;
    uint64_t batch_first_seq = 0;
    // Set when a diff download fails: the run stops and commits the diffs
    // applied so far (see below) instead of discarding them. `applied_ts` is
    // the newest object timestamp of the last applied batch, recorded as the
    // partial run's provenance timestamp.
    bool stopped_early = false;
    uint64_t stopped_seq = 0;
    int64_t applied_ts = 0;
    const auto apply_batch = [&](const std::string& change_path, uint64_t seq) {
        // The node pass folds the whole batch's positions in before any way
        // is resolved, so way and filter-4 cells see the end-of-batch geometry.
        move_sink.start_seq(seq);
        update_pass::run_node_update(change_path, changes_root, seq, opts.h3_resolution,
                                     &node_state, &move_sink);
        update_pass::run_diff_scans(
            change_path, changes_root, seq, opts.way_batch_bytes, opts.h3_resolution,
            node_state, &node_tag_state, &way_tag_state, &relation_tag_state,
            history_stage_root + "/seq_" + std::to_string(seq),
            suspect_stage_root + "/counts/seq_" + std::to_string(seq),
            suspect_stage_root + "/cells/seq_" + std::to_string(seq),
            suspect_stage_root + "/tags/seq_" + std::to_string(seq),
            suspect_stage_root + "/langs/seq_" + std::to_string(seq));
        move_sink.finish_seq();
    };
    const auto flush_batch = [&](uint64_t seq) {
        merger.finish();
        std::cerr << "[update] merged batch seq " << batch_first_seq << ".." << seq
                  << " (" << batch_diffs.size() << " diffs)";
        if (merger.has_ts()) {
            applied_ts = merger.max_ts();
            std::cerr << " ts " << osmium::Timestamp(merger.min_ts()).to_iso_all()
                      << ".." << osmium::Timestamp(merger.max_ts()).to_iso_all();
        }
        std::cerr << "\n";
        apply_batch(merged_path, seq);
        // `merged_path` and the batch's diff files are provably applied after
        // every pass over the batch succeeded; a throw anywhere above leaves
        // them for fetch_diff to reuse on a re-run.
        for (const std::string& diff_path : batch_diffs) {
            remove_applied_diff(diff_path);
        }
        std::filesystem::remove(merged_path, ignore_ec);
        applied = seq;
        batch_diffs.clear();
    };

    for (uint64_t seq = first; seq <= target; ++seq) {
        std::string diff_path;
        try {
            diff_path = replication_state::fetch_diff(
                current.url, seq, cookie, diffs_dir + "/" + std::to_string(seq) + ".osc.gz");
        } catch (const std::exception& e) {
            // Guards the fetch_diff call only: a corrupt diff (a read failure
            // in merger.add) or a pass failure still aborts hard below rather
            // than being checkpointed around. A local write error in the diffs
            // dir lands here too and is treated like a transient download
            // failure, which is still a consistent prefix.
            std::cerr << "[update] download of sequence " << seq << " failed: "
                      << e.what() << "\n";
            stopped_early = true;
            stopped_seq = seq;
            break;
        }
        // A batch of a single trailing diff runs the passes on the diff
        // directly instead of re-serializing it into the merged file.
        if (batch_diffs.empty() && seq == target) {
            std::cerr << "[update] merged batch seq " << seq << ".." << seq
                      << " (1 diff)\n";
            apply_batch(diff_path, seq);
            remove_applied_diff(diff_path);
            applied = seq;
            continue;
        }
        if (!merger.is_open()) {
            merger.open(merged_path);
            batch_first_seq = seq;
        }
        merger.add(diff_path);
        batch_diffs.push_back(diff_path);
        const bool time_batch_full =
            merger.has_ts() &&
            (merger.max_ts() - merger.min_ts() >=
             static_cast<int64_t>(update_pass::kUpdateBatchMaxSeconds));
        if (time_batch_full || seq == target) {
            flush_batch(seq);
        }
    }

    // A failed download stops the run, but the diffs applied so far are a
    // consistent prefix of the stream: apply the current batch's already
    // downloaded diffs, then fall through to the commit below. Only a failure
    // before the first diff was applied has nothing to checkpoint and aborts.
    if (stopped_early) {
        if (!batch_diffs.empty()) {
            flush_batch(stopped_seq - 1);
        } else {
            merger.finish();
            std::filesystem::remove(merged_path, ignore_ec);
        }
        if (applied == base_seq) {
            throw std::runtime_error(
                "diff download for sequence " + std::to_string(stopped_seq) +
                " failed before any diff was applied; the dataset is unchanged");
        }
        std::cerr << "[update] stopping early at sequence " << stopped_seq
                  << "; committing through " << applied << "\n";
    }

    // Name-language: persist the prior-tag cache delta (overlay + deletions).
    node_tag_state.persist_delta();
    way_tag_state.persist_delta();
    relation_tag_state.persist_delta();

    std::cerr << "[update] merging staging partitions into " << changes_root << "\n";
    auto center = timed_step("[update] merge staging partitions", [&] {
        return sort_pass::merge_update_partitions(changes_root, opts.change_group_rows, applied);
    });

    // Filter 2: fold this run's staged minute buckets into the persisted
    // binary minute store first, so the daily suspect flag written below
    // already reflects every diff of this run. Filter 3 (any node moved
    // > 500 m) folds into per-day flags the same way, and both staging roots
    // are consumed here. Filter 4: fold cell buckets into suspect_cells.bin.
    // Filter 5: fold tag buckets into suspect_tags.bin.
    const std::string minutes_path = opts.suspect_minutes_path;
    timed_step("[suspect] fold minute counts", [&] {
        suspect::fold_minute_counts(suspect_stage_root + "/counts", minutes_path, applied);
    });
    const std::string cells_path = opts.suspect_cells_path;
    timed_step("[suspect] fold cell counts", [&] {
        suspect::fold_cell_counts(suspect_stage_root + "/cells", cells_path, applied,
                                  opts.h3_resolution);
    });
    const std::string tags_path = opts.suspect_tags_path;
    timed_step("[suspect] fold tag counts", [&] {
        suspect::fold_tag_counts(suspect_stage_root + "/tags", tags_path, applied);
    });
    const std::map<std::pair<int64_t, uint16_t>, suspect::MoveDay> move_flags =
        timed_step("[suspect] flag move days", [&] {
            return suspect::flagged_move_days(suspect_stage_root);
        });
    // Filter 4: compute spatial spread flags from the cell and minute stores.
    const std::map<std::pair<int64_t, uint16_t>, suspect::CellSpreadDay> filter4_days =
        timed_step("[suspect] flag cell days", [&] {
            return suspect::flagged_cell_days(cells_path, minutes_path, opts.h3_resolution);
        });
    // Filter 5: compute tag coverage flags from the tag and minute stores.
    const std::map<std::pair<int64_t, uint16_t>, suspect::TagCoverageDay> filter5_days =
        timed_step("[suspect] flag tag days", [&] {
            return suspect::flagged_tag_days(tags_path, minutes_path);
        });
    // Name-language: fold lang buckets and compute name-language abuse flags.
    const std::string langs_path =
        (cache_parent / "suspect_lang.bin").lexically_normal().string();
    timed_step("[suspect] fold lang counts", [&] {
        suspect::fold_lang_counts(suspect_stage_root + "/langs", langs_path, applied);
    });
    const std::map<std::pair<int64_t, uint16_t>, suspect::NameLanguageDay> name_language_days =
        timed_step("[suspect] flag lang days", [&] {
            return suspect::flagged_lang_days(langs_path);
        });

    // A missing or unreadable country index leaves the countries lists empty
    // rather than failing the update.
    std::unique_ptr<country_index::CountryIndex> country_idx;
    if (std::filesystem::exists(opts.country_index_path)) {
        const std::filesystem::path idx_path = opts.country_index_path;
        const std::string ids_path = (idx_path.parent_path() / "country_ids.json").string();
        try {
            country_idx = std::make_unique<country_index::CountryIndex>(
                country_index::CountryIndex::load(opts.country_index_path, ids_path));
            std::cerr << "[suspect] country index: " << country_idx->size() << " cells from "
                      << opts.country_index_path << "\n";
        } catch (const std::exception& e) {
            std::cerr << "[suspect] could not load country index " << opts.country_index_path
                      << " (" << e.what() << "); suspect countries left empty\n";
            country_idx.reset();
        }
    } else {
        std::cerr << "[suspect] country index " << opts.country_index_path
                  << " not found; suspect countries left empty\n";
    }

    timed_step("[users history] update finalize", [&] {
        users_history::run_update_finalize(history_stage_root,
                                           opts.output_dir + "/users_history.parquet",
                                           opts.users_history_group_rows,
                                           opts.ranking_group_rows, minutes_path,
                                           move_flags, filter4_days, filter5_days,
                                           name_language_days, cells_path, opts.h3_resolution,
                                           country_idx.get());
    });

    // Provenance records the applied state: the sequence is the last applied
    // diff (indexed by "update N"), the timestamp is the fetched state.txt's
    // (the newest applied day's). A partial run stopped below the fetched
    // target, so its timestamp is taken from the last applied diff instead.
    replication_state::State new_source = current;
    new_source.sequence_number = applied;
    if (stopped_early && applied > base_seq && applied_ts > 0) {
        new_source.timestamp = osmium::Timestamp(applied_ts).to_iso_all();
    }
    stopped_early_out = stopped_early;
    spatial_center = center;

    // The incremental cache is written in two phases so the expensive base
    // rebuild runs last, after every user-visible output (data.parquet, the
    // suspect/history files and the manifest) is durable. Phase 1 writes the
    // O(changes) delta sidecar, so a crash during the rebuild still resumes
    // from a consistent cache; phase 2 rewrites the base and drops the sidecar.
    const size_t compact_limit =
        std::max<size_t>(1, static_cast<size_t>(node_state.base_records()) / 10);
    const update_pass::NodeState::PersistResult cache_result =
        timed_step("[update] persist incremental cache delta", [&] {
            return node_state.persist_delta(opts.node_cache_last_path, opts.h3_resolution,
                                            compact_limit);
        });
    std::cerr << "[update] incremental cache delta "
              << (cache_result == update_pass::NodeState::PersistResult::rebuild_due
                      ? "written (rebuild due)"
                      : cache_result ==
                                update_pass::NodeState::PersistResult::delta_written
                            ? "written"
                            : "unchanged")
              << " overlay=" << node_state.overlay_size()
              << " deleted=" << node_state.deleted_size() << "\n";

    write_manifest_output(opts, new_source, center);

    if (cache_result == update_pass::NodeState::PersistResult::rebuild_due) {
        std::cerr << "[update] compacting incremental cache " << opts.node_cache_last_path
                  << "\n";
        timed_step("[update] compact incremental cache", [&] {
            node_state.compact(opts.node_cache_last_path, opts.h3_resolution);
        });
    }
    // Name-language: compact the prior-tag cache when accumulated changes
    // reach 1/10 of the base size (same threshold as the node cache).
    struct TagStateInfo {
        suspect::ObjectTagState* state;
        uint8_t type;
    };
    for (auto& tsi : {TagStateInfo{&node_tag_state, 0},
                      TagStateInfo{&way_tag_state, 1},
                      TagStateInfo{&relation_tag_state, 2}}) {
        if (tsi.state->changed_count() > 0 &&
            tsi.state->changed_count() >= tsi.state->base_size() / 10 + 1) {
            std::cerr << "[update] compacting prior-tag cache ("
                      << object_tag_store::Writer::type_name_str(tsi.type) << ")\n";
            timed_step("[update] compact prior-tag cache", [&] {
                tsi.state->compact();
            });
        }
    }

    return new_source;
}

}  // namespace

int main(int argc, char** argv) {
    Options opts;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        if (!parse_args(argc, argv, &opts)) {
            print_usage(argv[0]);
            return 0;
        }
    } catch (const std::exception& e) {
        std::cerr << "Argument error: " << e.what() << "\n\n";
        print_usage(argv[0]);
        return 1;
    }

    bool update_incomplete = false;
    try {
        // Serialise runs against the shared data tree (caches, diffs, suspect
        // stores and output): one advisory flock next to the node caches,
        // taken before any file is touched. A concurrent run (e.g. a cron
        // update overlapping a long catch-up) fails fast instead of corrupting
        // the dataset.
        const std::filesystem::path cache_dir =
            std::filesystem::path(opts.node_cache_path).parent_path();
        if (!cache_dir.empty()) std::filesystem::create_directories(cache_dir);
        const std::string lock_path =
            (cache_dir / ".karmamap.lock").lexically_normal().string();
        auto lock = FileLock::acquire(lock_path, /*block=*/false);
        if (!lock) {
            std::cerr << "Another karmamap run holds " << lock_path
                      << "; refusing to modify the data concurrently\n";
            return 1;
        }

        std::filesystem::create_directories(opts.output_dir);

        // Source provenance. Fail fast before the passes run: import requires
        // the sidecar <base>.state.txt next to the osh (downloaded manually
        // with wget on the snapshot's day) and records its sequence and
        // timestamp; update fetches the live state.txt up front, so a bad
        // update URL aborts immediately. prepare-update never fetches: it
        // reads the source block recorded in manifest.json and only overwrites
        // the URL with --update-url (rejecting a mismatch with a URL that is
        // already recorded), keeping the recorded sequence and timestamp.
        // Unless --update-url is given, update derives the update stream from
        // the dataset's recorded source URL in manifest.json (the URL its data
        // was built from).
        std::optional<replication_state::State> source;
        if (opts.stage == Options::Stage::update && opts.update_url.empty()) {
            const std::optional<replication_state::State> dataset_source =
                manifest::read_source(opts.output_dir);
            if (!dataset_source || dataset_source->url.empty()) {
                throw std::runtime_error(
                    "karmamap update needs the dataset's update stream, but " +
                    opts.output_dir +
                    "/manifest.json carries no source URL; rebuild the dataset "
                    "with 'karmamap import <file> --update-url <url>', or pass "
                    "--update-url");
            }
            opts.update_url = dataset_source->url;
        }
        if (opts.stage == Options::Stage::import) {
            const std::string state_path =
                replication_state::sidecar_state_path(opts.input_path);
            std::cerr << "[source] reading " << state_path << "\n";
            source = replication_state::read_state_file(
                state_path, replication_state::normalize_update_url(opts.update_url));
            std::cerr << "[source] sequence_number=" << source->sequence_number
                      << " timestamp=" << source->timestamp << "\n";
        } else if (opts.stage == Options::Stage::update) {
            const std::string cookie_file = resolve_update_cookie(opts);
            std::cerr << "[source] fetching " << opts.update_url << "state.txt\n";
            source = replication_state::fetch(opts.update_url, cookie_file);
            std::cerr << "[source] sequence_number=" << source->sequence_number
                      << " timestamp=" << source->timestamp << "\n";
        } else if (opts.stage == Options::Stage::prepare_update) {
            const std::optional<replication_state::State> recorded =
                manifest::read_source(opts.output_dir);
            if (!recorded) {
                throw std::runtime_error(
                    "karmamap prepare-update needs the dataset's source "
                    "provenance, but " +
                    opts.output_dir +
                    "/manifest.json carries no source block; run 'karmamap "
                    "import' first");
            }
            const std::string normalized =
                replication_state::normalize_update_url(opts.update_url);
            if (!recorded->url.empty() && recorded->url != normalized) {
                throw std::runtime_error(
                    "--update-url " + opts.update_url +
                    " does not match the dataset's recorded source " +
                    recorded->url +
                    "; re-import with --update-url to switch streams");
            }
            replication_state::State preserved = *recorded;
            preserved.url = normalized;
            std::cerr << "[source] recording " << preserved.url
                      << " (sequence " << preserved.sequence_number
                      << " and timestamp from manifest.json; no state.txt fetch)\n";
            source = preserved;
        }

        std::optional<std::pair<double, double>> spatial_center;

        switch (opts.stage) {
            case Options::Stage::prepare_update:
                run_step4_pass(opts);
                break;
            case Options::Stage::import:
                if (opts.run_node_pass) {
                    run_node_pass(opts);
                }
                if (opts.run_way_pass) {
                    run_way_pass(opts);
                }
                if (opts.run_sort_pass) {
                    spatial_center = run_sort_pass(opts);
                }
                build_tag_cache(opts);
                run_users_history_pass(opts);
                break;
            case Options::Stage::update:
                source = run_update_mode(opts, *source, spatial_center, update_incomplete);
                break;
        }

    // Update mode writes its own manifest inside run_update_mode, before the
    // deferred base rebuild; every other stage writes it once here.
    if (opts.stage != Options::Stage::update) {
        write_manifest_output(opts, source, spatial_center);
    }

    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }

    return update_incomplete ? 1 : 0;
}
