// osh_change_index: daily count of OSM changes per H3 cell.
//
// Reads an OSM full-history file (.osh.pbf) and produces, by default, two
// partitioned Parquet datasets under --output-dir:
//   nodes_changes/year=YYYY/month=MM.parquet  (h3_cell, change_date, count)
//   ways_changes/year=YYYY/month=MM.parquet   (h3_cell, change_date, count)
//
// The pipeline runs in three stages:
//   1. node pass: reads every node, builds a RocksDB cache
//      (node_id, version -> position), counts node changes.
//   2. way pass: reads every way, resolves referenced node positions via
//      the RocksDB cache, traces H3 cells crossed by every segment, counts
//      way changes. Depends on the cache built by the node pass.
//   3. sort pass: rewrites every partition file sorted by h3_cell, so that
//      Parquet row group statistics become useful for bbox pruning.
//
// Usage:
//   osh_change_index --input <planet.osh.pbf> --rocksdb <dir> --output-dir <dir>
//                     [--resolution N] [--pass 1|2|3|all]
//
// Examples:
//   osh_change_index --input region.osh.pbf --rocksdb ./cache --output-dir ./out
//   osh_change_index --input region.osh.pbf --rocksdb ./cache --output-dir ./out --pass 1
//   osh_change_index --input region.osh.pbf --rocksdb ./cache --output-dir ./out --pass 3

#include <osmium/io/any_input.hpp>
#include <osmium/visitor.hpp>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "manifest.hpp"
#include "node_cache_handler.hpp"
#include "partitioned_parquet_writer.hpp"
#include "sort_pass.hpp"
#include "way_processor.hpp"

namespace {

struct Options {
    std::string input_path;
    std::string rocksdb_path;
    std::string output_dir;
    int h3_resolution = 9;
    bool run_node_pass = true;
    bool run_way_pass = true;
    bool run_sort_pass = true;
};

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0
        << " --input <planet.osh.pbf> --rocksdb <dir> --output-dir <dir> "
           "[--resolution N] [--pass 1|2|3|all]\n\n"
        << "  --input        OSM full-history file (.osh.pbf)\n"
        << "  --rocksdb      RocksDB cache directory (created by pass 1, read by pass 2)\n"
        << "  --output-dir   Output directory for the Parquet datasets\n"
        << "  --resolution   H3 resolution, 0-15 (default: 9)\n"
        << "  --pass         1 (nodes only), 2 (ways only, requires an already\n"
        << "                 populated RocksDB cache), 3 (sort only, requires\n"
        << "                 passes 1 and 2 to have already run), or all (default)\n";
}

bool parse_args(int argc, char** argv, Options* opts) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string("Missing value for ") + flag);
            }
            return argv[++i];
        };

        if (arg == "--input") {
            opts->input_path = next_value("--input");
        } else if (arg == "--rocksdb") {
            opts->rocksdb_path = next_value("--rocksdb");
        } else if (arg == "--output-dir") {
            opts->output_dir = next_value("--output-dir");
        } else if (arg == "--resolution") {
            opts->h3_resolution = std::stoi(next_value("--resolution"));
        } else if (arg == "--pass") {
            std::string v = next_value("--pass");
            opts->run_node_pass = (v == "1" || v == "all");
            opts->run_way_pass = (v == "2" || v == "all");
            opts->run_sort_pass = (v == "3" || v == "all");
            if (v != "1" && v != "2" && v != "3" && v != "all") {
                throw std::runtime_error("--pass must be 1, 2, 3 or all (got: " + v + ")");
            }
        } else if (arg == "--help" || arg == "-h") {
            return false;
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }

    if (opts->input_path.empty() || opts->rocksdb_path.empty() || opts->output_dir.empty()) {
        throw std::runtime_error("--input, --rocksdb and --output-dir are required");
    }
    // Way pass alone: the RocksDB cache must already exist. Not checked
    // here - `OpenForReadOnly` fails cleanly further down if it is absent.
    return true;
}

std::unique_ptr<rocksdb::DB> open_rocksdb(const std::string& path, bool writable) {
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status status;

    if (writable) {
        rocksdb::Options options;
        options.create_if_missing = true;
        options.write_buffer_size = 256ULL * 1024 * 1024;
        options.max_write_buffer_number = 4;
        options.min_write_buffer_number_to_merge = 1;
        options.compression = rocksdb::kZSTD;
        options.IncreaseParallelism();
        status = rocksdb::DB::Open(options, path, &raw_db);
    } else {
        rocksdb::Options options;
        options.max_open_files = -1;
        status = rocksdb::DB::OpenForReadOnly(options, path, &raw_db);
    }

    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB (" + path + "): " + status.ToString());
    }
    return std::unique_ptr<rocksdb::DB>(raw_db);
}

void run_node_pass(const Options& opts, rocksdb::DB* db) {
    const std::string root = opts.output_dir + "/nodes_changes";
    std::cerr << "[node pass] -> " << root << "\n";

    parquet_out::PartitionedParquetWriter parquet_writer(root);

    osmium::io::File input_file(opts.input_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::node);

    node_pass::NodeCacheHandler handler(db, &parquet_writer, opts.h3_resolution);

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
    std::cerr << "[node pass] done in " << elapsed << "s\n";
}

void run_way_pass(const Options& opts, rocksdb::DB* db) {
    const std::string root = opts.output_dir + "/ways_changes";
    std::cerr << "[way pass] -> " << root << "\n";

    parquet_out::PartitionedParquetWriter parquet_writer(root);

    osmium::io::File input_file(opts.input_path);
    osmium::io::Reader reader(input_file, osmium::osm_entity_bits::way);

    way_pass::WayProcessor handler(db, &parquet_writer, opts.h3_resolution);

    auto start = std::chrono::steady_clock::now();
    while (osmium::memory::Buffer buf = reader.read()) {
        osmium::apply(buf, handler);
    }
    reader.close();

    parquet_writer.finish();

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[way pass] done in " << elapsed << "s\n";
}

void run_sort_pass(const Options& opts) {
    std::cerr << "[sort pass] sorting partitions under " << opts.output_dir << "\n";

    auto start = std::chrono::steady_clock::now();

    sort_pass::sort_partitions(opts.output_dir + "/nodes_changes");
    sort_pass::sort_partitions(opts.output_dir + "/ways_changes");

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start)
                       .count();
    std::cerr << "[sort pass] done in " << elapsed << "s\n";
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

        // RocksDB is opened writable as soon as the node pass runs (it is
        // the only stage that populates the cache); read-only if only the
        // way pass runs against an already existing cache. Not needed at
        // all for the sort pass alone, but opening it read-only is cheap
        // and keeps this function simple.
        if (opts.run_node_pass || opts.run_way_pass) {
            auto db = open_rocksdb(opts.rocksdb_path, /*writable=*/opts.run_node_pass);
            if (opts.run_node_pass) run_node_pass(opts, db.get());
            if (opts.run_way_pass) run_way_pass(opts, db.get());
        }

        if (opts.run_sort_pass) run_sort_pass(opts);

        std::cerr << "[manifest] writing " << opts.output_dir << "/manifest.json\n";
        manifest::write_manifest(opts.output_dir, opts.h3_resolution);

    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
