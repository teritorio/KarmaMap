// karmamap: daily count of OSM changes per H3 cell.
//
// Reads an OSM full-history file (.osh.pbf) and produces one partitioned
// Parquet dataset under --output-dir:
//   changes/year=YYYY/month=MM/data.parquet  (h3_cell, change_date, node_count, way_count)
//
// Three stages: node pass (writes the mmap node-position cache + counts
// nodes into .../nodes.parquet), way pass (resolves node positions by a
// batched sweep over the cache, counts ways at the distinct cells of their
// node positions into .../ways.parquet, no segment path tracing), merge
// pass (merges each month's node and way counts into node_count/way_count
// columns of data.parquet, sorted by (h3_cell, change_date) so that Parquet
// row group min/max statistics become useful for bbox and date-range
// pruning).

#include <osmium/io/any_input.hpp>
#include <osmium/visitor.hpp>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "manifest.hpp"
#include "node_cache.hpp"
#include "node_cache_handler.hpp"
#include "options.hpp"
#include "partitioned_parquet_writer.hpp"
#include "sort_pass.hpp"
#include "user_indicators.hpp"
#include "way_processor.hpp"

namespace {

// Wipes a dataset root from a previous run, so a re-run never leaves stale
// year=YYYY/month=MM partition files behind. No-op if absent.
void reset_dataset_root(const std::string& root) {
    std::filesystem::remove_all(root);
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
    // INSTR
    std::cerr << "[node pass] nodes=" << handler.nodes()
              << " cache_writes=" << handler.cache_writes()
              << " cache_records=" << cache_writer.records()
              << " cache_bytes=" << cache_writer.bytes()
              << " writer increments=" << parquet_writer.increments()
              << " flushes=" << parquet_writer.flushes()
              << " open_partitions=" << parquet_writer.open_partitions() << "\n";
}

void run_way_pass(const Options& opts) {
    const std::string root = opts.output_dir + "/changes";
    std::cerr << "[way pass] -> " << root << "\n";

    // Writes into pass 1's existing changes/ root so both staging files
    // share the month directories; does not wipe it (pass 2 may run alone).
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
    // INSTR
    handler.print_stats();
    std::cerr << "[way pass] writer increments=" << parquet_writer.increments()
              << " flushes=" << parquet_writer.flushes()
              << " open_partitions=" << parquet_writer.open_partitions() << "\n";
}

void run_sort_pass(const Options& opts) {
    std::cerr << "[sort pass] merging and sorting partitions under " << opts.output_dir << "/changes\n";

    auto start = std::chrono::steady_clock::now();

    sort_pass::merge_and_sort_partitions(opts.output_dir + "/changes");

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[sort pass] done in " << elapsed << "s\n";
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
    user_indicators::run_finalize(stage_dir, indicators_path);
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

        if (opts.run_node_pass) {
            run_node_pass(opts);
        }
        if (opts.run_way_pass) {
            run_way_pass(opts);
        }
        if (opts.run_sort_pass) {
            run_sort_pass(opts);
        }
        if (opts.run_user_indicators) {
            run_user_indicator_pass(opts);
        }

        std::cerr << "[manifest] writing " << opts.output_dir << "/manifest.json\n";
        manifest::write_manifest(opts.output_dir, opts.h3_resolution);

    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
