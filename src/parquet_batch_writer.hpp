#pragma once

// Incrementally writes batches of counts (h3_cell, date, count) to a
// Parquet file as successive row groups, avoiding accumulating the whole
// dataset in memory before writing.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace parquet_out {

// In-memory aggregation key: (H3 cell, UTC day since epoch).
struct CountKey {
    uint64_t h3_cell;
    int32_t day;

    bool operator==(const CountKey& o) const {
        return h3_cell == o.h3_cell && day == o.day;
    }
};

struct CountKeyHash {
    size_t operator()(const CountKey& k) const noexcept {
        return std::hash<uint64_t>()(k.h3_cell) ^
               (std::hash<int32_t>()(k.day) + 0x9e3779b97f4a7c15ULL);
    }
};

using CountMap = std::unordered_map<CountKey, uint32_t, CountKeyHash>;

class ParquetBatchWriter {
public:
    explicit ParquetBatchWriter(const std::string& output_path);
    ~ParquetBatchWriter();

    // Writes the content of `counts` as a new row group, then clears the
    // map so the caller can reuse it immediately.
    void flush(CountMap& counts);

    void close();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace parquet_out
