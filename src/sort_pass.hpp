#pragma once

// Pass 3: for every year partition under root_dir, full-outer-joins the
// staging files (nodes.parquet, ways.parquet) on (h3_cell, change_date) and
// writes the result to data.parquet, sorted by (h3_cell, change_date) so
// row-group min/max become useful for bbox and date-range pruning. The
// result is written under a temp name and renamed into place.

#include <cstdint>
#include <string>

namespace sort_pass {

// Merges every year partition found under root_dir (a single changes/
// dataset root). Year directories that already hold only data.parquet
// (both nodes.parquet and ways.parquet absent) are left untouched.
// `change_group_rows` bounds the size of each data.parquet row group.
void merge_and_sort_partitions(const std::string& root_dir, int64_t change_group_rows);

}  // namespace sort_pass
