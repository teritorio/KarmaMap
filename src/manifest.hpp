#pragma once

// Writes output-dir/manifest.json, describing what a client (e.g. a
// browser-side hyparquet consumer) needs to know without guessing:
// H3 resolution used, overall month coverage, and the list of partitions
// actually present for each dataset.
//
// Built purely from a directory scan (year=YYYY/month=MM layout), not
// from Parquet file contents - cheap, and correct regardless of which
// --pass combination produced the files on disk.

#include <string>

namespace manifest {

void write_manifest(const std::string& output_dir, int h3_resolution);

}  // namespace manifest