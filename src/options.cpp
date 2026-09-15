#include "options.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

#include "h3_utils.hpp"

void print_usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0
        << " --input <planet.osh.pbf> --node-cache <file> --output-dir <dir> "
           "[core options] [fine-tuning options]\n\n"
        << "Core options:\n"
        << "  --input                OSM full-history file (.osh.pbf)\n"
        << "  --node-cache           Node position cache file (wiped and rebuilt by pass 1, read by pass 2)\n"
        << "  --output-dir           Output directory for the Parquet datasets\n"
        << "  --pass                 1 (nodes only), 2 (ways only, requires an already\n"
        << "                         populated node cache), 3 (merge + sort only,\n"
        << "                         requires passes 1 and 2 to have already run),\n"
        << "                         or all (default)\n"
        << "  --way-batch-mb         Way-pass lookup batch budget in MiB (default: 512)\n\n"
        << "Fine-tuning options:\n"
        << "  h3 cell:\n"
        << "    --h3-resolution      Resolution of the data cells, 0-13 (default: 9)\n"
        << "  user stats (--user-indicators):\n"
        << "    --user-indicators    Additionally score history per user and per UTC day,\n"
        << "                         writing user_profiles.parquet and\n"
        << "                         user_indicators.parquet (non-partitioned single\n"
        << "                         files, independent of passes 1-3); nodes, ways,\n"
        << "                         relation creations and Top12 tag usage are counted\n"
        << "    --relocate-meters    Node move distance (meters) that counts as a\n"
        << "                         relocation (default: " << user_indicators::kDefaultRelocateMeters << ")\n"
        << "    --short-life-days    A delete counts as short-lived if the object was\n"
        << "                         created within this many days (default: " << user_indicators::kDefaultShortLifeDays << ")\n"
        << "    --rapid-edit-versions  At least this many object versions within the\n"
        << "                         rapid-edit window flag rapid editing (default: " << user_indicators::kDefaultRapidEditVersions << ")\n"
        << "    --rapid-edit-window-days  Rolling window for rapid-edit counting\n"
        << "                         (default: " << user_indicators::kDefaultRapidEditWindowDays << ")\n"
        << "    --new-user-window-days  A user is a new user within this many days of\n"
        << "                         their first edit (default: " << user_indicators::kDefaultNewUserWindowDays << ")\n"
        << "    --bulk-edit-min       A new user is bulk-editing when making at least\n"
        << "                         this many edits within the new-user window\n"
        << "                         (default: " << user_indicators::kDefaultBulkEditMin << ")\n";
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
        } else if (arg == "--node-cache") {
            opts->node_cache_path = next_value("--node-cache");
        } else if (arg == "--output-dir") {
            opts->output_dir = next_value("--output-dir");
        } else if (arg == "--h3-resolution") {
            opts->h3_resolution = std::stoi(next_value("--h3-resolution"));
        } else if (arg == "--way-batch-mb") {
            const unsigned long mb = std::stoul(next_value("--way-batch-mb"));
            if (mb < 16 || mb > 1ULL << 20) {  // sanity range 16 MiB .. 1 TiB
                throw std::runtime_error("--way-batch-mb must be in 16..1048576");
            }
            opts->way_batch_bytes = mb * 1024ULL * 1024ULL;
        } else if (arg == "--pass") {
            std::string v = next_value("--pass");
            opts->run_node_pass = (v == "1" || v == "all");
            opts->run_way_pass = (v == "2" || v == "all");
            opts->run_sort_pass = (v == "3" || v == "all");
            if (v != "1" && v != "2" && v != "3" && v != "all") {
                throw std::runtime_error("--pass must be 1, 2, 3 or all (got: " + v + ")");
            }
        } else if (arg == "--user-indicators") {
            opts->run_user_indicators = true;
        } else if (arg == "--relocate-meters") {
            opts->thresholds.relocate_meters = std::stod(next_value("--relocate-meters"));
        } else if (arg == "--short-life-days") {
            opts->thresholds.short_life_days = std::stoi(next_value("--short-life-days"));
        } else if (arg == "--rapid-edit-versions") {
            opts->thresholds.rapid_edit_versions = std::stoi(next_value("--rapid-edit-versions"));
        } else if (arg == "--rapid-edit-window-days") {
            opts->thresholds.rapid_edit_window_days = std::stoi(next_value("--rapid-edit-window-days"));
        } else if (arg == "--new-user-window-days") {
            opts->thresholds.new_user_window_days = std::stoi(next_value("--new-user-window-days"));
        } else if (arg == "--bulk-edit-min") {
            opts->thresholds.bulk_edit_min = std::stoi(next_value("--bulk-edit-min"));
        } else if (arg == "--help" || arg == "-h") {
            return false;
        } else {
            throw std::runtime_error("Unknown argument: " + arg);
        }
    }

    if (opts->input_path.empty() || opts->node_cache_path.empty() || opts->output_dir.empty()) {
        throw std::runtime_error("--input, --node-cache and --output-dir are required");
    }
    if (opts->h3_resolution < 0 ||
        opts->h3_resolution > h3_utils::kMaxPackedCellResolution) {
        throw std::runtime_error(
            "H3 resolution must be in range 0-13 (the 6-byte cache cell "
            "encoding holds at most 13 digits)");
    }
    if (opts->thresholds.relocate_meters < 0.0 || opts->thresholds.short_life_days < 1 ||
        opts->thresholds.rapid_edit_versions < 2 || opts->thresholds.rapid_edit_window_days < 1 ||
        opts->thresholds.new_user_window_days < 1 || opts->thresholds.bulk_edit_min < 1) {
        throw std::runtime_error(
            "User-indicator thresholds must be positive "
            "(rapid_edit_versions >= 2)");
    }
    // For a way-pass-only run the cache must already exist; not checked
    // here - opening read-only fails cleanly if it is absent.
    return true;
}
