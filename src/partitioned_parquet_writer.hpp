#pragma once

// Routes (h3_cell, day, count) increments to one Parquet file per calendar
// year under root_dir/year=YYYY/<file>. Pass 1 writes nodes.parquet, pass 2
// ways.parquet; pass 3 merges both into data.parquet. Each partition flushes
// independently once its pending rows exceed a threshold. Files here are NOT
// sorted - see sort_pass.hpp.

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
        int year = 0, month = 0;
        date_utils::year_month_from_day(day, &year, &month);

        Partition& p = get_partition(year);
        p.pending[{h3_cell, day}]++;
        if (p.pending.size() >= kFlushThreshold) {
            p.writer->flush(p.pending);
        }
    }

    // Flushes and closes every partition opened so far.
    void finish() {
        for (auto& [year, p] : partitions_) {
            if (!p.pending.empty()) {
                p.writer->flush(p.pending);
            }
            p.writer->close();
        }
    }

private:
    struct Partition {
        std::unique_ptr<ParquetBatchWriter> writer;
        CountMap pending;
    };

    Partition& get_partition(int year) {
        auto it = partitions_.find(year);
        if (it != partitions_.end()) return it->second;

        std::string year_dir = root_dir_ + "/year=" + std::to_string(year);
        std::filesystem::create_directories(year_dir);

        Partition p;
        p.writer = std::make_unique<ParquetBatchWriter>(year_dir + "/" + output_file_name_);
        auto [inserted_it, _] = partitions_.emplace(year, std::move(p));
        return inserted_it->second;
    }

    std::string root_dir_;
    std::string output_file_name_;
    std::map<int, Partition> partitions_;
};

}  // namespace parquet_out