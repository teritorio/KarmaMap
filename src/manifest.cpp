#include "manifest.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace manifest {

namespace {

// Scans dataset_root/year=YYYY/month=MM.parquet and returns the sorted list
// of "YYYY-MM" partition strings actually present on disk.
std::vector<std::string> list_partitions(const std::string& dataset_root) {
    std::vector<std::string> partitions;
    if (!std::filesystem::exists(dataset_root)) return partitions;

    for (const auto& year_entry : std::filesystem::directory_iterator(dataset_root)) {
        if (!year_entry.is_directory()) continue;
        const std::string year_dir = year_entry.path().filename().string();  // "year=YYYY"
        const std::string year_prefix = "year=";
        if (year_dir.compare(0, year_prefix.size(), year_prefix) != 0) continue;
        const std::string year = year_dir.substr(year_prefix.size());

        for (const auto& month_entry : std::filesystem::directory_iterator(year_entry.path())) {
            if (!month_entry.is_regular_file()) continue;
            if (month_entry.path().extension() != ".parquet") continue;

            const std::string month_file = month_entry.path().stem().string();  // "month=MM"
            const std::string month_prefix = "month=";
            if (month_file.compare(0, month_prefix.size(), month_prefix) != 0) continue;
            const std::string month = month_file.substr(month_prefix.size());

            partitions.push_back(year + "-" + month);
        }
    }

    std::sort(partitions.begin(), partitions.end());
    return partitions;
}

std::string json_string_array(const std::vector<std::string>& values) {
    std::string out = "[";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) out += ", ";
        out += "\"" + values[i] + "\"";
    }
    out += "]";
    return out;
}

}  // namespace

void write_manifest(const std::string& output_dir, int h3_resolution) {
    auto node_partitions = list_partitions(output_dir + "/nodes_changes");
    auto way_partitions = list_partitions(output_dir + "/ways_changes");

    std::vector<std::string> all_partitions;
    all_partitions.insert(all_partitions.end(), node_partitions.begin(), node_partitions.end());
    all_partitions.insert(all_partitions.end(), way_partitions.begin(), way_partitions.end());
    std::sort(all_partitions.begin(), all_partitions.end());
    all_partitions.erase(std::unique(all_partitions.begin(), all_partitions.end()),
                          all_partitions.end());

    std::ofstream out(output_dir + "/manifest.json");
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open manifest.json for writing in " + output_dir);
    }

    out << "{\n";
    out << "  \"h3_resolution\": " << h3_resolution << ",\n";

    if (all_partitions.empty()) {
        out << "  \"date_range\": null,\n";
    } else {
        out << "  \"date_range\": { \"min_month\": \"" << all_partitions.front()
            << "\", \"max_month\": \"" << all_partitions.back() << "\" },\n";
    }

    out << "  \"datasets\": {\n";
    out << "    \"nodes_changes\": { \"path\": \"nodes_changes\", \"partitions\": "
        << json_string_array(node_partitions) << " },\n";
    out << "    \"ways_changes\": { \"path\": \"ways_changes\", \"partitions\": "
        << json_string_array(way_partitions) << " }\n";
    out << "  }\n";
    out << "}\n";
}

}  // namespace manifest
