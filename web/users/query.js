// User queries across the non-partitioned Parquet files written by the
// users-history pass, served from data/ one directory above the viewers. Username
// matching uses a direct exact-username lookup on
// user_reputation.parquet (the pipeline stamps the current username per uid,
// and that file is username-sorted with a uid tie-break, so the exact filter
// prunes straight to the matching pages); the reputation and per-uid counter
// totals come from the same row. Only the per-day activity timeline still
// comes from users_history.parquet (uid-sorted, so a range filter prunes
// pages, with exact membership kept client-side); the timeline's per-day
// counts are read from the single `count` column (the day's total activity:
// the six node/way change counters plus the three relation counters). The
// dataset-wide `active`/`max` stats are read once from the file's
// key_value_metadata footer instead of repeated per-row columns.

import { queryRows, queryRowsWithMetadata } from '../lib/parquet.js'
import { ALL_COUNTERS, TAG_COUNTERS } from './reputation.js'

// The reputation aspects key the file-level key_value_metadata: one
// <aspect>_active/_max pair per aspect, matching the C++ writer's stat keys.
const ASPECT_KEYS = ['node', 'way', 'relation', ...TAG_COUNTERS]

// Dataset-wide aspect stats (contributors active on an aspect and its largest
// per-user total) are the same value for every row, so the pipeline writes
// them once as Parquet file-level key_value_metadata (<aspect>_active/_max)
// instead of 30 repeated columns. Read from the metadata the query already
// parsed, so the footer is fetched a single time.
function readAspectStats(metadata) {
  const kv = new Map((metadata.key_value_metadata ?? []).map((e) => [e.key, e.value]))
  const stats = {}
  for (const key of ASPECT_KEYS) {
    stats[key] = {
      active: Number(kv.get(`${key}_active`) ?? 0),
      max: Number(kv.get(`${key}_max`) ?? 0),
    }
  }
  return stats
}

// Exact username match on user_reputation.parquet (the current username is
// stamped per uid), returning the whole wide per-uid row -- identity columns,
// reputation and history totals -- plus the dataset-wide active/max stats
// read from the file footer. uid and the day columns are small integers
// (int64/uint16), so Number() conversion is lossless.
export async function queryReputationByUsername(baseUrl, path, username, footerSize) {
  const res = await queryRowsWithMetadata(baseUrl, path, { username: { $eq: username } }, undefined, footerSize)
  if (!res) return { rows: [], stats: {} }
  const stats = readAspectStats(res.metadata)
  return {
    rows: res.rows.map((row) => {
      const counters = {}
      for (const key of ALL_COUNTERS) counters[key] = Number(row[key] ?? 0)
      return {
        uid: Number(row.uid),
        username: row.username,
        first_seen_day: Number(row.first_seen_day),
        counters,
        reputation: row,
      }
    }),
    stats,
  }
}

// uid-sorted file: a [min, max] range filter prunes pages, and the exact
// uid set is applied client-side (the same pattern the changes viewer uses
// for its non-contiguous H3 cell set). Used for the per-day timeline only.
export async function queryHistory(baseUrl, path, uids, footerSize) {
  if (uids.length === 0) return []
  const minUid = Math.min(...uids)
  const maxUid = Math.max(...uids)
  const uidSet = new Set(uids)
  // The history file holds exactly the timeline's 3 columns (uid,
  // change_date, count), so no projection is needed.
  const rows = await queryRows(baseUrl, path, { uid: { $gte: minUid, $lte: maxUid } }, undefined, footerSize)
  return rows.filter((row) => uidSet.has(Number(row.uid)))
}
