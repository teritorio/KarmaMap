#pragma once

// Pass 3: for every year partition under root_dir, sums the staging files'
// (nodes.parquet, ways.parquet) counts per (h3_cell, change_date) into a
// single `count` column and writes the result to data.parquet, sorted by
// (h3_cell, change_date) so row-group min/max become useful for bbox and
// date-range pruning. The result is written under a temp name and renamed
// into place.

#include <cstdint>
#include <string>

namespace sort_pass {

// Footer metadata key of the data partitions written by update mode: the
// highest replication sequence whose deltas are folded into the counts. An
// update merge that starts from a sequence already stamped into every
// partition removes the leftover staging files instead of re-merging.
inline constexpr const char* karmamap_source_seq = "karmamap_source_seq";

// Merges every year partition found under root_dir (a single changes/
// dataset root). Year directories that already hold only data.parquet
// (both nodes.parquet and ways.parquet absent) are left untouched.
// `change_group_rows` bounds the size of each data.parquet row group.
void merge_and_sort_partitions(const std::string& root_dir, int64_t change_group_rows);

// Merges base data.parquet with the update staging partitions of a run into
// one merged table, sorted and stamped with `applied_seq` under the
// karmamap_source_seq footer key. Staging partitions are named
// nodes.<seq>.parquet / ways.<seq>.parquet (from the update node/way passes)
// and are removed once folded in; a partition already stamped with a sequence
// >= applied_seq has its orphaned staging removed and is left untouched.
// Only year directories that carry staging partitions are rewritten.
void merge_update_partitions(const std::string& root_dir, int64_t change_group_rows,
                             uint64_t applied_seq);

// Reads the karmamap_source_seq footer stamp of a data partition (0 when the
// file carries no such metadata or does not exist).
uint64_t read_source_sequence(const std::string& data_path);

}  // namespace sort_pass
