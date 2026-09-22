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
//     pruning), plus the per-user/per-day indicator pass.
//   prepare-update            build the .last incremental cache (one record per
//     node, last known h3 cell, no day) from the node cache and record the
//     update stream provenance in manifest.json.
//   update [N]                advance the dataset along its replication diff
//     stream (see run_update_mode).

#include <osmium/io/any_input.hpp>
#include <osmium/visitor.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "manifest.hpp"
#include "geofabrik_cookie.hpp"
#include "node_cache.hpp"
#include "node_cache_handler.hpp"
#include "options.hpp"
#include "partitioned_parquet_writer.hpp"
#include "sort_pass.hpp"
#include "state.hpp"
#include "update.hpp"
#include "user_indicators.hpp"
#include "vandalism.hpp"
#include "way_processor.hpp"

namespace {

// Wipes a dataset root from a previous run, so a re-run never leaves stale
// year=YYYY partition files behind. No-op if absent.
void reset_dataset_root(const std::string& root) {
    std::filesystem::remove_all(root);
}

// The cookie jar used for the update stream, or "" when the update URL does
// not sit behind the authenticated Geofabrik internal server. For the
// internal host the OSM-session jar is obtained or refreshed up front (from
// OSM_GEOFABRIK_USER/OSM_GEOFABRIK_PASSWORD in .env) so both the state fetch
// and every diff download can use it.
std::string resolve_update_cookie(const Options& opts) {
    if (!geofabrik_cookie::requires_auth(opts.update_url)) return "";
    std::string jar =
        opts.cookie_path.empty() ? geofabrik_cookie::default_cookie_path(opts.output_dir)
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

void run_sort_pass(const Options& opts) {
    std::cerr << "[sort pass] merging and sorting partitions under " << opts.output_dir << "/changes\n";

    auto start = std::chrono::steady_clock::now();

    sort_pass::merge_and_sort_partitions(opts.output_dir + "/changes",
                                         opts.change_group_rows);

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[sort pass] done in " << elapsed << "s\n";
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

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[step 4] done in " << elapsed << "s\n";
    std::cerr << "[step 4] history_records=" << history_records
              << " nodes=" << records
              << " cache_bytes=" << cache_writer.bytes() << "\n";
}

void run_user_indicator_pass(const Options& opts) {
    const std::string stage_dir = opts.output_dir + "/user_indicator_stage";
    const std::string indicators_path = opts.output_dir + "/user_indicators.parquet";

    // A re-run never reuses stale outputs: wipe stage + final files before
    // scanning, so an empty scan cannot leave last run's rows behind.
    std::filesystem::remove_all(stage_dir);
    std::filesystem::remove(indicators_path);
    std::filesystem::remove(opts.output_dir + "/user_reputation.parquet");

    user_indicators::run_scan(opts.input_path, stage_dir);
    user_indicators::run_finalize(stage_dir, indicators_path, opts.indicators_group_rows,
                                  opts.reputation_group_rows);
}

// Update mode: advances an existing dataset along its replication diff stream
// (see options.cpp, the "update" command). Starting from the sequence recorded
// in manifest.json's source block, every diff up to the target (the current
// state.txt, capped by "update [N]") is downloaded to <output-dir>/diffs and
// applied by the update node and way passes into per-sequence staging
// partitions (nodes.<seq>.parquet / ways.<seq>.parquet). Once after all diffs
// the flat incremental cache is rebuilt (base + overlay minus deletions) and
// the staging partitions are merged into data.parquet, so N diffs never
// rewrite the dataset N times. Returns the provenance to write to manifest
// (the applied sequence, with the fetched state.txt timestamp).
std::optional<replication_state::State> run_update_mode(
    const Options& opts, const replication_state::State& current) {
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
    if (first > target) {
        std::cerr << "[update] already at sequence " << base_seq
                  << " (state.txt=" << current.sequence_number << "); nothing to apply\n";
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
    const std::string diffs_dir = opts.output_dir + "/diffs";
    const std::string indicator_stage_root =
        opts.output_dir + "/user_indicator_update_stage";
    const std::string vandalism_stage_root = opts.output_dir + "/vandalism_update_stage";
    // Stage groups from a crashed earlier run may cover sequences this run
    // does not re-scan; drop them so finalize only folds what was applied.
    std::filesystem::remove_all(indicator_stage_root);
    std::filesystem::remove_all(vandalism_stage_root);
    // Filter 2 stages go under vandalism_update_stage/counts/seq_<n>; the
    // move sink stages filter 3 under vandalism_update_stage/moves/seq_<n>.
    vandalism::NodeMoveSink move_sink(vandalism_stage_root + "/moves");
    uint64_t applied = base_seq;
    for (uint64_t seq = first; seq <= target; ++seq) {
        const std::string diff_path = replication_state::fetch_diff(
            current.url, seq, cookie, diffs_dir + "/" + std::to_string(seq) + ".osc.gz");
        move_sink.start_seq(seq);
        update_pass::run_node_update(diff_path, changes_root, seq, opts.h3_resolution,
                                     &node_state, &move_sink);
        update_pass::run_way_update(diff_path, changes_root, seq, node_state,
                                    opts.way_batch_bytes);
        user_indicators::run_scan_diff(
            diff_path, indicator_stage_root + "/seq_" + std::to_string(seq));
        vandalism::run_scan_diff(
            diff_path, vandalism_stage_root + "/counts/seq_" + std::to_string(seq));
        move_sink.finish_seq();
        applied = seq;
    }

    std::cerr << "[update] rebuilding incremental cache " << opts.node_cache_last_path
              << "\n";
    node_state.rebuild(opts.node_cache_last_path, opts.h3_resolution);

    std::cerr << "[update] merging staging partitions into " << changes_root << "\n";
    sort_pass::merge_update_partitions(changes_root, opts.change_group_rows, applied);

    // Filter 2: fold this run's staged minute buckets into the persisted
    // binary minute store first, so the daily vandalism flag written below
    // already reflects every diff of this run. Filter 3 (any node moved
    // > 500 m) folds into per-day flags the same way, and both staging roots
    // are consumed here.
    const std::string minutes_path = opts.output_dir + "/vandalism_minutes.bin";
    vandalism::fold_minute_counts(vandalism_stage_root + "/counts", minutes_path, applied);
    const std::map<std::pair<int64_t, uint16_t>, uint8_t> move_flags =
        vandalism::flagged_move_days(vandalism_stage_root);

    user_indicators::run_update_finalize(indicator_stage_root,
                                         opts.output_dir + "/user_indicators.parquet",
                                         opts.indicators_group_rows,
                                         opts.reputation_group_rows, minutes_path,
                                         move_flags);

    // Provenance now reflects the applied state: the sequence is the last
    // applied diff (indexed by "update N"), the timestamp is the fetched
    // state.txt's (the newest applied day's).
    replication_state::State new_source = current;
    new_source.sequence_number = applied;
    return new_source;
}

}  // namespace

int main(int argc, char** argv) {
    Options opts;

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

    try {
        std::filesystem::create_directories(opts.output_dir);

        // Source provenance. Fail fast before the passes run: import requires
        // the sidecar <base>.state.txt next to the osh (downloaded manually
        // with wget on the snapshot's day) and records its sequence and
        // timestamp; prepare-update and update fetch the live state.txt up
        // front, so a bad update URL aborts immediately. Unless --update-url
        // is given, update derives the update stream from the dataset's
        // recorded source URL in manifest.json (the URL its data was built
        // from).
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
        } else if (!opts.update_url.empty()) {
            const std::string cookie_file = resolve_update_cookie(opts);
            std::cerr << "[source] fetching " << opts.update_url << "state.txt\n";
            source = replication_state::fetch(opts.update_url, cookie_file);
            std::cerr << "[source] sequence_number=" << source->sequence_number
                      << " timestamp=" << source->timestamp << "\n";
        }

        switch (opts.stage) {
            case Options::Stage::prepare_update:
                run_step4_pass(opts);
                break;
            case Options::Stage::update:
                source = run_update_mode(opts, *source);
                break;
            case Options::Stage::import:
                if (opts.run_node_pass) {
                    run_node_pass(opts);
                }
                if (opts.run_way_pass) {
                    run_way_pass(opts);
                }
                if (opts.run_sort_pass) {
                    run_sort_pass(opts);
                }
                run_user_indicator_pass(opts);
                break;
        }

        std::cerr << "[manifest] writing " << opts.output_dir << "/manifest.json\n";
        manifest::write_manifest(opts.output_dir, opts.h3_resolution, source);

    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
