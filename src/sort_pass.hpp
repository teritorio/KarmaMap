#pragma once

// Pass 3: sorts every partition file produced by the node pass / way pass
// (root_dir/year=YYYY/month=MM.parquet) by h3_cell, so that Parquet row
// group min/max statistics become useful for bbox pruning.
//
// Files are rewritten under a different name first (<name>.sorted.parquet),
// and the original is only removed once the sorted file has been written
// successfully. No mid-run resume support: if the process is interrupted
// between removing the original and the final rename, a stray
// "<name>.sorted.parquet" can be left next to a missing original - not
// handled automatically (consistent with the rest of the project, which
// has no checkpoint/resume support elsewhere either).

#include <string>

namespace sort_pass {

// Recursively finds every *.parquet file under root_dir and rewrites it
// sorted by h3_cell, in place (same final path).
void sort_partitions(const std::string& root_dir);

}  // namespace sort_pass
