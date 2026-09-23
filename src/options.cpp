#include "options.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "h3_utils.hpp"

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " <command> [options]\n\n"
        << "Commands:\n"
        << "  import <planet.osh.pbf> Build a dataset from an OSM full-history\n"
        << "                          snapshot (passes 1-3). Reads the state.txt\n"
        << "                          sidecar next to the snapshot (download it\n"
        << "                          with wget on the snapshot's day) for its\n"
        << "                          replication provenance. Never builds the\n"
        << "                          .last cache; run prepare-update for that.\n"
        << "  prepare-update          Build the .last incremental cache from the\n"
        << "                          node cache and record the update stream URL\n"
        << "                          in manifest.json. No dataset changes.\n"
        << "  update [N]              Apply osmosis replication diffs (.osc.gz,\n"
        << "                          3/3/3 layout) to an existing dataset. The\n"
        << "                          starting sequence is read from manifest.json's\n"
        << "                          source block, whose URL is the update stream\n"
        << "                          (overridable with --update-url; an explicit URL\n"
        << "                          must match the recorded one). N caps the number\n"
        << "                          of diffs fetched (default: catch up to the\n"
        << "                          current state.txt). Each applied diff is\n"
        << "                          removed from <output-dir>/diffs once every\n"
        << "                          pass over it succeeded. Reads and rebuilds the\n"
        << "                          --node-cache-last cache; the full history node\n"
        << "                          cache is not used.\n"
        << "  help / --help / -h      Show this help.\n\n"
        << "Options (defaults in [brackets], all optional unless noted):\n"
        << "  --node-cache <file>       Node position cache (import and\n"
        << "                            prepare-update). [<output-dir>/../node_positions.cache]\n"
        << "  --node-cache-last <file>  Last-known-position cache, written by\n"
        << "                            prepare-update, read/rebuild by update.\n"
        << "                            [<node-cache>.last]\n"
        << "  --output-dir <dir>        Output directory for the Parquet datasets.\n"
        << "                            [$DATA_DIR/output, default data/output]\n"
        << "  --update-url <url>        Osmosis replication update URL (e.g.\n"
        << "                            https://.../canary-islands-updates/). At\n"
        << "                            import it only records the update stream URL\n"
        << "                            (sequence and timestamp come from the\n"
        << "                            snapshot's sidecar state file).\n"
        << "  --cookie <jar>            Netscape cookie jar for the Geofabrik internal\n"
        << "                            server (osm-internal.download.geofabrik.de);\n"
        << "                            default: <output-dir>/.geofabrik.cookie. When the\n"
        << "                            update URL points at the internal host, karmamap\n"
        << "                            obtains and refreshes the jar itself from the OSM\n"
        << "                            account in OSM_GEOFABRIK_USER/OSM_GEOFABRIK_PASSWORD\n"
        << "                            and sends it on update's state.txt fetch\n"
        << "  --pass 1|2|3|all          Import only: run only the node (1), way (2) or\n"
        << "                            merge (3) pass, or all three (default). Use\n"
        << "                            prepare-update for the incremental cache.\n"
        << "  --h3-resolution <r>       Resolution of the data cells, 0-13 (default: 9);\n"
        << "                            must match between import, prepare-update and\n"
        << "                            update (the caches encode cells at this\n"
        << "                            resolution)\n"
        << "  --way-batch-mb <mb>       Import only, way-pass lookup batch budget in MiB\n"
        << "                            (default: 512)\n"
        << "  --change-group-rows <n>   Target rows per Parquet row group of the changes\n"
        << "                            dataset (default: 10000); smaller row groups keep\n"
        << "                            h3_cell/change_date min-max compact so range-pruning\n"
        << "                            clients download only the pages they need\n"
        << "  --users-history-group-rows <n>\n"
        << "                            Target rows per Parquet row group of\n"
        << "                            users_history.parquet (default: 10000)\n"
        << "  --reputation-group-rows <n> Target rows per Parquet row group of\n"
        << "                            user_reputation.parquet (default: 1000)\n";
}

bool parse_args(int argc, char** argv, Options* opts) {
    auto is_count = [](const std::string& token) {
        if (token.empty()) return false;
        for (char c : token) {
            if (c < '0' || c > '9') return false;
        }
        return true;
    };
    auto parse_count = [&](const std::string& token, const char* flag) {
        if (!is_count(token)) {
            throw std::runtime_error(std::string("Invalid value for ") + flag + ": " + token);
        }
        return std::stoll(token);
    };

    int i = 1;
    if (i >= argc) {
        throw std::runtime_error("no command given; expected import, prepare-update or update");
    }
    const std::string verb = argv[i++];
    if (verb == "help" || verb == "--help" || verb == "-h") {
        return false;
    }
    if (verb == "import") {
        opts->stage = Options::Stage::import;
    } else if (verb == "prepare-update") {
        opts->stage = Options::Stage::prepare_update;
    } else if (verb == "update") {
        opts->stage = Options::Stage::update;
    } else {
        throw std::runtime_error("unknown command: " + verb +
                                 " (expected import, prepare-update or update)");
    }

    bool update_count_given = false;  // update [N] positional
    auto next_value = [&](const char* flag) -> std::string {
        if (i + 1 >= argc) {
            throw std::runtime_error(std::string("Missing value for ") + flag);
        }
        return argv[++i];
    };

    for (; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            return false;
        }
        if (arg.empty() || arg[0] != '-') {
            // Positional arguments: import <file>, update [N].
            if (opts->stage == Options::Stage::import && opts->input_path.empty()) {
                opts->input_path = arg;
            } else if (opts->stage == Options::Stage::update && !update_count_given &&
                       is_count(arg)) {
                opts->max_update_diffs = parse_count(arg, "update");
                update_count_given = true;
            } else {
                throw std::runtime_error("unexpected argument: " + arg);
            }
            continue;
        }
        if (arg == "--node-cache") {
            opts->node_cache_path = next_value("--node-cache");
            opts->node_cache_given = true;
        } else if (arg == "--node-cache-last") {
            opts->node_cache_last_path = next_value("--node-cache-last");
        } else if (arg == "--output-dir") {
            opts->output_dir = next_value("--output-dir");
        } else if (arg == "--update-url") {
            opts->update_url = next_value("--update-url");
        } else if (arg == "--cookie") {
            opts->cookie_path = next_value("--cookie");
        } else if (arg == "--h3-resolution") {
            opts->h3_resolution = std::stoi(next_value("--h3-resolution"));
        } else if (arg == "--way-batch-mb") {
            const unsigned long mb = std::stoul(next_value("--way-batch-mb"));
            if (mb < 16 || mb > 1ULL << 20) {  // sanity range 16 MiB .. 1 TiB
                throw std::runtime_error("--way-batch-mb must be in 16..1048576");
            }
            opts->way_batch_bytes = mb * 1024ULL * 1024ULL;
            opts->way_batch_mb_given = true;
        } else if (arg == "--change-group-rows") {
            const long long rows = std::stoll(next_value("--change-group-rows"));
            if (rows < 1'000) {
                throw std::runtime_error("--change-group-rows must be at least 1000");
            }
            opts->change_group_rows = rows;
        } else if (arg == "--users-history-group-rows") {
            const long long rows = std::stoll(next_value("--users-history-group-rows"));
            if (rows < 1'000) {
                throw std::runtime_error("--users-history-group-rows must be at least 1000");
            }
            opts->users_history_group_rows = rows;
        } else if (arg == "--reputation-group-rows") {
            const long long rows = std::stoll(next_value("--reputation-group-rows"));
            if (rows < 1'000) {
                throw std::runtime_error("--reputation-group-rows must be at least 1000");
            }
            opts->reputation_group_rows = rows;
        } else if (arg == "--pass") {
            const std::string v = next_value("--pass");
            opts->pass_given = true;
            opts->run_node_pass = (v == "1" || v == "all");
            opts->run_way_pass = (v == "2" || v == "all");
            opts->run_sort_pass = (v == "3" || v == "all");
            if (v != "1" && v != "2" && v != "3" && v != "all") {
                throw std::runtime_error("--pass must be 1, 2, 3 or all (got: " + v + ")");
            }
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }

    if (opts->stage == Options::Stage::import && opts->input_path.empty()) {
        throw std::runtime_error("karmamap import <planet.osh.pbf>: missing input file");
    }
    switch (opts->stage) {
        case Options::Stage::prepare_update:
            if (opts->update_url.empty()) {
                throw std::runtime_error(
                    "karmamap prepare-update needs the update stream URL; pass "
                    "--update-url <url> (it is recorded in manifest.json, keeping "
                    "the recorded sequence and timestamp)");
            }
            if (opts->pass_given) {
                throw std::runtime_error("--pass is not used with prepare-update");
            }
            if (opts->way_batch_mb_given) {
                throw std::runtime_error("--way-batch-mb is only used by import");
            }
            break;
        case Options::Stage::update:
            if (opts->pass_given) {
                throw std::runtime_error(
                    "--pass is not used with update: update always runs the node, "
                    "way and merge passes and rebuilds the incremental cache");
            }
            if (opts->way_batch_mb_given) {
                throw std::runtime_error("--way-batch-mb is only used by import");
            }
            if (opts->node_cache_given) {
                throw std::runtime_error(
                    "--node-cache is not used with update: update reads only the "
                    "incremental cache (--node-cache-last)");
            }
            break;
        case Options::Stage::import:
            break;
    }

    if (opts->output_dir.empty()) {
        // Default <output-dir> is $DATA_DIR/output (relative to the working
        // directory); docker-compose sets DATA_DIR=/data so container runs
        // default to /data/output instead of /data/data/output.
        std::string base = "data";
        if (const char* data_dir = std::getenv("DATA_DIR")) {
            if (data_dir[0] != '\0') base = data_dir;
        }
        opts->output_dir = (std::filesystem::path(base) / "output").string();
    }
    if (opts->node_cache_path.empty()) {
        opts->node_cache_path =
            (std::filesystem::path(opts->output_dir) / ".." / "node_positions.cache")
                .lexically_normal()
                .string();
    }
    if (opts->node_cache_last_path.empty()) {
        opts->node_cache_last_path = opts->node_cache_path + ".last";
    }
    if (opts->h3_resolution < 0 ||
        opts->h3_resolution > h3_utils::kMaxPackedCellResolution) {
        throw std::runtime_error(
            "H3 resolution must be in range 0-13 (the 6-byte cache cell "
            "encoding holds at most 13 digits)");
    }
    // For a way-pass-only run the cache must already exist; not checked
    // here - opening read-only fails cleanly if it is absent.
    return true;
}
