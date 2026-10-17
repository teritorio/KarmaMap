#pragma once

// Atom 1.0 feed mirroring a suspect.parquet table: one entry per flagged
// (uid, change_date), newest first, published next to the parquet file so the
// feed and the data ship together on every update run.
#include <arrow/api.h>
#include <string>

namespace suspect_atom {

// Writes the feed to `atom_path + ".tmp"` then renames it into place, so a
// reader never sees a partially written feed. Throws on failure.
void write_feed(const arrow::Table& suspect_table, const std::string& atom_path);

}  // namespace suspect_atom
