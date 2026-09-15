// User-profile + user-indicator queries across the two non-partitioned
// Parquet files written by --user-indicators, served by the :8080 data
// server. user_profiles.parquet is matched by exact username; the matching
// uid(s) then select rows from user_indicators.parquet (uid-sorted, so a
// range filter prunes pages, with exact membership kept client-side) and
// are aggregated into raw per-indicator totals, an OSMPatrol reputation
// (created objects only, per the paper), edit-suspicion signals, and a
// per-day edit timeline.

import { parquetQuery, asyncBufferFromUrl } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

const CHANGE_COUNTERS = [
  'node_created', 'node_modified', 'node_deleted',
  'way_created', 'way_modified', 'way_deleted',
]

// Top12 tag aspect of the reputation (paper sec. 4, with "address" replaced
// by "place": OSM address tagging uses the addr: prefix).
const TOP12_TAGS = [
  'amenity', 'boundary', 'building', 'highway', 'landuse', 'leisure',
  'name', 'natural', 'place', 'railway', 'sport', 'waterway',
]
export const TAG_COUNTERS = TOP12_TAGS.map((key) => `tag_${key}`)

const ALL_COUNTERS = [...CHANGE_COUNTERS, 'relocated', 'short_lived', 'rapid_edit', 'relation_created', ...TAG_COUNTERS]

// OSMPatrol reputation caps (Neis, Goetz & Zipf 2012, §4). The reputation is
// built only from *created* objects and Top12 tag usage; modifications and
// deletions feed the edit-level vandalism value instead. Each of the 12 tags
// is worth up to 4 points, so the computable maximum is 20+20+12+48 = 100.
export const REP_CAPS = { node: 20, way: 20, relation: 12 }
export const REP_TAG_CAP = 4
const REP_MAX =
  REP_CAPS.node + REP_CAPS.way + REP_CAPS.relation + TOP12_TAGS.length * REP_TAG_CAP
const REP_NOTE = `Reputation 0-100: created nodes ${REP_CAPS.node}, ways ${REP_CAPS.way}, relations ${REP_CAPS.relation} and ${TOP12_TAGS.length} top tags x ${REP_TAG_CAP}; each aspect is capped at its weight and scored by the user's percentile rank among the dataset's contributors active on that aspect`
export const REP_FORMULA =
  `R = min(${REP_CAPS.node}, ${REP_CAPS.node}\u00b7P(n)) + min(${REP_CAPS.way}, ${REP_CAPS.way}\u00b7P(w)) + ` +
  `min(${REP_CAPS.relation}, ${REP_CAPS.relation}\u00b7P(r)) + \u03a3\u1d62 min(${REP_TAG_CAP}, ${REP_TAG_CAP}\u00b7P(t\u1d62))`

// Edit-level suspicion thresholds from the paper (sec. 5 filters): more than
// 500 modified/deleted objects within one hour, node moved over 500 m, and
// new/low-reputation (reputation < 5%) users. Daily counters approximate the
// hourly rule with a per-day cutoff; the pipeline flags relocation at 500 m.
const SUSPICION_CHANGES_PER_DAY = 500

// One pass over user_reputation_distribution.parquet yields the per-aspect
// equal-mass CDF samples used to rank each contributor's percentile. The file
// is written by run_finalize (C++) and is small enough to cache once per
// page-session. A missing file degrades each aspect to 0.
let distCache = null
let distCachePath = null

// The per-aspect shape consumed by computeReputation; an empty instance
// degrades every aspect to 0 (used when the distribution file is absent).
export function emptyDistribution() {
  const newDist = () => ({ entries: [], active: 0, max: 0 })
  return {
    nodes: newDist(),
    ways: newDist(),
    relations: newDist(),
    tags: Array.from({ length: TOP12_TAGS.length }, newDist),
  }
}

export async function datasetDistribution(baseUrl, path) {
  if (distCache && distCachePath === path) return distCache
  const rows = await queryRows(baseUrl, path, () => true)
  const distribution = emptyDistribution()
  const map = new Map([
    ['node_created', distribution.nodes],
    ['way_created', distribution.ways],
    ['relation_created', distribution.relations],
    ...TOP12_TAGS.map((key, i) => [`tag_${key}`, distribution.tags[i]]),
  ])
  for (const row of rows) {
    const dist = map.get(row.aspect)
    if (!dist) continue
    dist.entries.push({ value: Number(row.value), less: Number(row.less) })
    dist.active = Number(row.active)
  }
  for (const dist of map.values()) {
    if (dist.entries.length > 0) dist.max = dist.entries[dist.entries.length - 1].value
  }
  distCache = distribution
  distCachePath = path
  return distribution
}

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

// OSMPatrol reputation (0..100): each created-object and Top12-tag aspect is
// capped at its paper weight and scored by the user's percentile rank among
// the dataset's contributors active on that aspect (raw count > 0): no
// activity scores 0, a unique busiest contributor scores the full weight, and
// equal counts share the same rank.
export function computeReputation(counters, distribution) {
  const rankPoint = (total, cap, dist) => {
    if (total <= 0 || dist.active === 0) return { points: 0, pct: 0 }
    if (dist.active === 1) return { points: cap, pct: 100 }
    const entries = dist.entries
    let lo = 0
    let hi = entries.length
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      if (entries[mid].value <= total) lo = mid + 1
      else hi = mid
    }
    const less = lo === 0 ? 0 : entries[lo - 1].less
    const pct = (100 * less) / (dist.active - 1)
    return { points: (cap * less) / (dist.active - 1), pct }
  }
  const round = (v) => Math.round(v * 100) / 100
  const node = rankPoint(counters.node_created, REP_CAPS.node, distribution.nodes)
  const way = rankPoint(counters.way_created, REP_CAPS.way, distribution.ways)
  const relation = rankPoint(counters.relation_created, REP_CAPS.relation, distribution.relations)
  const tags = TOP12_TAGS.map((_, i) =>
    rankPoint(counters[TAG_COUNTERS[i]], REP_TAG_CAP, distribution.tags[i]))
  const detail = (key, label, cap, raw, dist, points, pct) => ({
    key, label, cap,
    raw: Number(raw ?? 0),
    max: dist.max,
    active: dist.active,
    points: round(points),
    pct,
  })
  const objectDetails = [
    detail('node', 'Created nodes', REP_CAPS.node, counters.node_created, distribution.nodes, node.points, node.pct),
    detail('way', 'Created ways', REP_CAPS.way, counters.way_created, distribution.ways, way.points, way.pct),
    detail('relation', 'Created relations', REP_CAPS.relation, counters.relation_created, distribution.relations, relation.points, relation.pct),
  ]
  const tagDetails = TOP12_TAGS.map((key, i) =>
    detail(TAG_COUNTERS[i], `Tag ${key}`, REP_TAG_CAP, counters[TAG_COUNTERS[i]],
      distribution.tags[i], tags[i].points, tags[i].pct))
  const tagPoints = tags.reduce((sum, t) => sum + t.points, 0)
  const details = [...objectDetails, ...tagDetails]
  return {
    value: Math.round(node.points + way.points + relation.points + tagPoints),
    max: REP_MAX,
    note: REP_NOTE,
    details,
  }
}

// Edit-level vandalism signals (paper sec. 4-5): object modifications and
// deletions, geometry moves, rapid re-edits/short-lived objects, and the
// new/low-reputation factors. These support the reputation; the paper fuses
// reputation and edit value with patrol-defined weights, so no single
// combined number is produced.
export function computeSuspicion(counters, bulkNewUser, maxDayChanges, reputation) {
  return {
    delWild: maxDayChanges > SUSPICION_CHANGES_PER_DAY,
    moved: counters.relocated > 0,
    nearRepeats: counters.short_lived > 0 || counters.rapid_edit > 0,
    newUser: bulkNewUser,
    lowReputation: reputation < 5,
  }
}

// Raw totals across the user's full timeline: every counter summed, total
// edits, the activity-by-day timeline, and the profile summary values,
// together with the OSMPatrol reputation and edit-suspicion signals.
export function computeScores(profiles, indicatorRows, distribution) {
  const counters = {}
  for (const key of ALL_COUNTERS) counters[key] = 0

  const byDay = new Map()
  let maxDayChanges = 0
  for (const row of indicatorRows) {
    for (const key of ALL_COUNTERS) counters[key] += Number(row[key] ?? 0)

    // Paper filter 2: more than 500 modified or deleted objects within one
    // hour, approximated per day across node+way events.
    const dayChanges = Number(row.node_modified ?? 0) + Number(row.node_deleted ?? 0) +
      Number(row.way_modified ?? 0) + Number(row.way_deleted ?? 0)
    if (dayChanges > maxDayChanges) maxDayChanges = dayChanges

    const dayCount = CHANGE_COUNTERS.reduce((sum, k) => sum + Number(row[k] ?? 0), 0)
    const day = dayKey(row.change_date)
    byDay.set(day, (byDay.get(day) ?? 0) + dayCount)
  }

  const totalEdits = CHANGE_COUNTERS.reduce((sum, k) => sum + counters[k], 0)
  const bulkNewUser = profiles.some((p) => p.bulk_new_user)
  const reputation = computeReputation(counters, distribution)
  return {
    uid: profiles.length ? Math.min(...profiles.map((p) => p.uid)) : null,
    firstSeenDay: profiles.length ? Math.min(...profiles.map((p) => p.first_seen_day)) : null,
    bulkNewUser,
    counters,
    totalEdits,
    byDay,
    maxDayChanges,
    reputation,
    suspicion: computeSuspicion(counters, bulkNewUser, maxDayChanges, reputation.value),
  }
}
