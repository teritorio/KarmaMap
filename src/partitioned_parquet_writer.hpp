#pragma once

// Routes (h3_cell, day, count) increments to one Parquet file per calendar
// month under root_dir/year=YYYY/month=MM/<file>. Pass 1 writes
// nodes.parquet, pass 2 ways.parquet; pass 3 merges both into data.parquet.
// Each partition flushes independently once its pending rows exceed a
// threshold. Files here are NOT sorted - see sort_pass.hpp.

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "date_utils.hpp"
#include "parquet_batch_writer.hpp"

namespace parquet_out {

constexpr size_t kFlushThreshold = 500'000;

class PartitionedParquetWriter {
public:
    explicit PartitionedParquetWriter(std::string root_dir, std::string output_file_name)
        : root_dir_(std::move(root_dir)), output_file_name_(std::move(output_file_name)) {
        std::filesystem::create_directories(root_dir_);
    }

    void increment(uint64_t h3_cell, int32_t day) {
        increments_++;  // INSTR
        int year = 0, month = 0;
        date_utils::year_month_from_day(day, &year, &month);

        Partition& p = get_partition(year, month);
        p.pending[{h3_cell, day}]++;
        if (p.pending.size() >= kFlushThreshold) {
            p.writer->flush(p.pending);
            flushes_++;  // INSTR
        }
    }

    // Flushes and closes every partition opened so far.
    void finish() {
        for (auto& [key, p] : partitions_) {
            if (!p.pending.empty()) {
                p.writer->flush(p.pending);
                flushes_++;  // INSTR
            }
            p.writer->close();
        }
    }

    // INSTR
    uint64_t increments() const { return increments_; }
    uint64_t flushes() const { return flushes_; }
    uint64_t open_partitions() const { return open_partitions_; }

private:
    struct Partition {
        std::unique_ptr<ParquetBatchWriter> writer;
        CountMap pending;
    };

    Partition& get_partition(int year, int month) {
        auto key = std::make_pair(year, month);
        auto it = partitions_.find(key);
        if (it != partitions_.end()) return it->second;
        open_partitions_++;  // INSTR

        std::string year_dir = root_dir_ + "/year=" + std::to_string(year);
        std::filesystem::create_directories(year_dir);

        char month_str[3];
        std::snprintf(month_str, sizeof(month_str), "%02d", month);
        std::string month_dir = year_dir + "/month=" + month_str;
        std::filesystem::create_directories(month_dir);

        Partition p;
        p.writer = std::make_unique<ParquetBatchWriter>(month_dir + "/" + output_file_name_);
        auto [inserted_it, _] = partitions_.emplace(key, std::move(p));
        return inserted_it->second;
    }

    std::string root_dir_;
    std::string output_file_name_;
    std::map<std::pair<int, int>, Partition> partitions_;

    // INSTR: diagnostic counters.
    uint64_t increments_ = 0;
    uint64_t flushes_ = 0;
    uint64_t open_partitions_ = 0;
};

}  // namespace parquet_out