#pragma once

// Writes output-dir/manifest.json, describing what a client (e.g. a
// browser-side hyparquet consumer) needs to know without guessing: the H3
// resolution used, the list of partitions actually present for each
// dataset, and the exact change_date span (min_date/max_date) of the
// changes dataset.
//
// Built from a year=YYYY directory scan plus the change_date min/max read
// from each data.parquet footer - cheap, and correct regardless of which
// --pass combination produced the files on disk.

#include <string>

namespace manifest {

void write_manifest(const std::string& output_dir, int h3_resolution);

}  // namespace manifest