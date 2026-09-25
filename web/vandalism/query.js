// Vandalism query: the N latest flagged (uid, change_date) rows of
// vandalism.parquet. That file is written by the update finalize sorted
// newest-first (change_date descending, uid tie-break), so the most recent
// flagged days live in the leading row groups and a rowEnd cap reads just
// their pages — no date filter or whole-file scan needed.

import { queryRows } from '../lib/parquet.js'

// The vandalism file's full column set (uid, username, change_date,
// vandalism_flag, changes, far_move_count, reputation_at_day).
const VANDALISM_COLUMNS = [
  'uid',
  'username',
  'change_date',
  'vandalism_flag',
  'changes',
  'far_move_count',
  'reputation_at_day',
]

// Returns the newest `limit` vandalism rows (default 100), one row per
// flagged (uid, change_date), in file order (newest first).
export async function queryVandalismLatest(baseUrl, path, footerSize, limit = 100) {
  return queryRows(baseUrl, path, undefined, VANDALISM_COLUMNS, footerSize, limit)
}