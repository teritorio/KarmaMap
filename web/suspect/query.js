// Suspect query: the N latest flagged (uid, change_date) rows of
// suspect.parquet. That file is written by the update finalize sorted
// newest-first (change_date descending, uid tie-break), so the most recent
// flagged days live in the leading row groups and a rowEnd cap reads just
// their pages — no date filter or whole-file scan needed.

import { queryRows } from '../lib/parquet.js'

// The suspect file's full column set (uid, username, change_date,
// suspect_flag, changes, max_edits_per_hour, peak_spread_km2, changed_keys,
// far_move_count, max_move_meters, ranking_at_day).
const SUSPECT_COLUMNS = [
  'uid',
  'username',
  'change_date',
  'suspect_flag',
  'changes',
  'max_edits_per_hour',
  'peak_spread_km2',
  'changed_keys',
  'far_move_count',
  'max_move_meters',
  'ranking_at_day',
]

// Returns the newest `limit` suspect rows (default 100), one row per
// flagged (uid, change_date), in file order (newest first).
export async function querySuspectLatest(baseUrl, path, footerSize, limit = 100) {
  return queryRows(baseUrl, path, undefined, SUSPECT_COLUMNS, footerSize, limit)
}