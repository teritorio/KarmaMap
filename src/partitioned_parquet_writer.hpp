#pragma once

// Routes (h3_cell, day, count) increments to one Parquet file per calendar
// month, under root_dir/year=YYYY/month=MM.parquet. Each partition flushes
// independently once its own pending-row threshold is reached.
//
// Files written here are NOT sorted internally - see sort_pass.hpp for the
// separate pass that sorts each partition file by h3_cell after both the
// node pass and the way pass have completed.

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

constexpr size_t kPartitionFlushThreshold = 500'000;  // pending rows before a partition flushes

class PartitionedParquetWriter {
public:
    explicit PartitionedParquetWriter(std::string root_dir) : root_dir_(std::move(root_dir)) {
        std::filesystem::create_directories(root_dir_);
    }

    void increment(uint64_t h3_cell, int32_t day) {
        int year = 0, month = 0;
        date_utils::year_month_from_day(day, &year, &month);

        Partition& p = get_partition(year, month);
        p.pending[{h3_cell, day}]++;
        if (p.pending.size() >= kPartitionFlushThreshold) {
            p.writer->flush(p.pending);
        }
    }

    // Flushes and closes every partition opened so far.
    void finish() {
        for (auto& [key, p] : partitions_) {
            if (!p.pending.empty()) p.writer->flush(p.pending);
            p.writer->close();
        }
    }

private:
    struct Partition {
        std::unique_ptr<ParquetBatchWriter> writer;
        CountMap pending;
    };

    Partition& get_partition(int year, int month) {
        auto key = std::make_pair(year, month);
        auto it = partitions_.find(key);
        if (it != partitions_.end()) return it->second;

        std::string year_dir = root_dir_ + "/year=" + std::to_string(year);
        std::filesystem::create_directories(year_dir);

        char month_str[3];
        std::snprintf(month_str, sizeof(month_str), "%02d", month);
        std::string path = year_dir + "/month=" + month_str + ".parquet";

        Partition p;
        p.writer = std::make_unique<ParquetBatchWriter>(path);
        auto [inserted_it, _] = partitions_.emplace(key, std::move(p));
        return inserted_it->second;
    }

    std::string root_dir_;
    std::map<std::pair<int, int>, Partition> partitions_;
};

}  // namespace parquet_out
