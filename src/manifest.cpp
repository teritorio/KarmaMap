#include "manifest.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace manifest {

namespace {

// Scans dataset_root/year=YYYY/month=MM and returns the sorted list of
// "YYYY-MM" partition strings actually present on disk.
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
            if (!month_entry.is_directory()) continue;
            const std::string month_dir = month_entry.path().filename().string();  // "month=MM"
            const std::string month_prefix = "month=";
            if (month_dir.compare(0, month_prefix.size(), month_prefix) != 0) continue;
            const std::string month = month_dir.substr(month_prefix.size());

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
    auto partitions = list_partitions(output_dir + "/changes");

    std::ofstream out(output_dir + "/manifest.json");
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open manifest.json for writing in " + output_dir);
    }

    out << "{\n";
    out << "  \"h3_resolution\": " << h3_resolution << ",\n";

    if (partitions.empty()) {
        out << "  \"date_range\": null,\n";
    } else {
        out << "  \"date_range\": { \"min_month\": \"" << partitions.front()
            << "\", \"max_month\": \"" << partitions.back() << "\" },\n";
    }

    out << "  \"datasets\": {\n";
    bool first_dataset = true;
    auto write_dataset = [&](const std::string& name, const std::string& path,
                             const std::vector<std::string>& parts) {
        if (!first_dataset) out << ",\n";
        first_dataset = false;
        out << "    \"" << name << "\": { \"path\": \"" << path << "\", \"partitions\": "
            << json_string_array(parts) << " }";
    };
    write_dataset("changes", "changes", partitions);
    // The user-indicator outputs are non-partitioned single files; the empty
    // partition list tells month-based query clients to skip them.
    if (std::filesystem::exists(output_dir + "/user_indicators.parquet")) {
        write_dataset("user_indicators", "user_indicators.parquet", {});
    }
    if (std::filesystem::exists(output_dir + "/user_reputation.parquet")) {
        write_dataset("user_reputation", "user_reputation.parquet", {});
    }
    out << "\n  }\n";
    out << "}\n";
}

}  // namespace manifest