// User-profile + user-indicator queries across the two non-partitioned
// Parquet files written by --user-indicators, served by the :8080 data
// server. user_profiles.parquet is matched by exact username; the matching
// uid(s) then select rows from user_indicators.parquet (uid-sorted, so a
// range filter prunes pages, with exact membership kept client-side) and
// are aggregated into raw per-indicator totals plus a per-day edit timeline.

import { parquetQuery, asyncBufferFromUrl } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

const CHANGE_COUNTERS = [
  'node_created', 'node_modified', 'node_deleted',
  'way_created', 'way_modified', 'way_deleted',
]
const ALL_COUNTERS = [...CHANGE_COUNTERS, 'relocated', 'short_lived', 'rapid_edit']

export function dayKey(changeDate) {
  return new Date(Number(changeDate) * 86400000).toISOString().slice(0, 10)
}

async function queryRows(baseUrl, path, filter) {
  const url = `${baseUrl}/${path}`
  let file
  try {
    file = await asyncBufferFromUrl({ url })
  } catch (err) {
    // A fetch failure means the file isn't there, not a fatal query error.
    console.warn(`Skipping ${url}: ${err.message}`)
    return []
  }
  return parquetQuery({ file, compressors, filter })
}

// Exact username match on user_profiles.parquet. uid and the day columns
// are small integers (int64/uint16), so Number() conversion is lossless.
export async function queryProfiles(baseUrl, path, username) {
  const rows = await queryRows(baseUrl, path, { username: { $eq: username } })
  return rows.map((row) => ({
    uid: Number(row.uid),
    username: row.username,
    first_edit_day: Number(row.first_edit_day),
    first_seen_day: Number(row.first_seen_day),
    bulk_new_user: row.bulk_new_user === true,
  }))
}

// uid-sorted file: a [min, max] range filter prunes pages, and the exact
// uid set is applied client-side (the same pattern the changes viewer uses
// for its non-contiguous H3 cell set).
export async function queryIndicators(baseUrl, path, uids) {
  if (uids.length === 0) return []
  const minUid = Math.min(...uids)
  const maxUid = Math.max(...uids)
  const uidSet = new Set(uids)
  const rows = await queryRows(baseUrl, path, { uid: { $gte: minUid, $lte: maxUid } })
  return rows.filter((row) => uidSet.has(Number(row.uid)))
}

// Raw totals across the user's full timeline: every counter summed, total
// edits, the activity-by-day timeline, and the profile summary values.
export function computeScores(profiles, indicatorRows) {
  const counters = {}
  for (const key of ALL_COUNTERS) counters[key] = 0

  const byDay = new Map()
  for (const row of indicatorRows) {
    for (const key of ALL_COUNTERS) counters[key] += Number(row[key] ?? 0)

    const dayCount = CHANGE_COUNTERS.reduce((sum, k) => sum + Number(row[k] ?? 0), 0)
    const day = dayKey(row.change_date)
    byDay.set(day, (byDay.get(day) ?? 0) + dayCount)
  }

  const totalEdits = CHANGE_COUNTERS.reduce((sum, k) => sum + counters[k], 0)
  return {
    uid: profiles.length ? Math.min(...profiles.map((p) => p.uid)) : null,
    firstSeenDay: profiles.length ? Math.min(...profiles.map((p) => p.first_seen_day)) : null,
    bulkNewUser: profiles.some((p) => p.bulk_new_user),
    counters,
    totalEdits,
    byDay,
  }
}