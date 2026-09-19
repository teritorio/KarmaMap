// User queries across the non-partitioned Parquet files written by
// --user-indicators, served from data/ one directory above the viewers. Username
// matching uses a direct exact-username lookup on
// user_reputation.parquet (the pipeline stamps the current username per uid,
// and that file is username-sorted with a uid tie-break, so the exact filter
// prunes straight to the matching pages); the reputation and per-uid counter
// totals come from the same row. Only the per-day activity timeline still
// comes from user_indicators.parquet (uid-sorted, so a range filter prunes
// pages, with exact membership kept client-side); its day counts add
// relation creations to the six node/way change counters. The per-aspect
// `points` are recomputed from the stored `pct` and the paper's constant
// caps, and the dataset-wide `active`/`max` stats are read once from the
// file's key_value_metadata footer instead of repeated per-row columns. No
// ranking or percentile math runs in the browser.

import { parquetQuery, asyncBufferFromUrl, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

const realFetch = window.fetch.bind(window)
export const fetchLog = []
let fetchSeq = 0

function countingFetch(url, options = {}) {
  const requestId = ++fetchSeq
  const start = performance.now()
  return realFetch(url, options).then((resp) => {
    if (!resp.ok) return resp
    const headers = options.headers
    const range = typeof headers?.get === 'function' ? headers.get('Range') : headers?.Range
    const req = typeof range === 'string' ? /bytes=(\d+)-(\d*)/.exec(range) : null
    const cr = resp.headers.get('Content-Range')
    const res = cr ? /bytes (\d+)-(\d+)\/(\d+)/.exec(cr) : null
    fetchLog.push({
      id: requestId,
      file: String(url).split('?')[0].split('/').pop(),
      range: range ?? '-',
      start: res ? Number(res[1]) : (req ? Number(req[1]) : 0),
      end: res ? Number(res[2]) : null,
      total: res ? Number(res[3]) : Number(resp.headers.get('Content-Length') ?? 0),
      size: Number(resp.headers.get('Content-Length') ?? 0),
      status: resp.status,
      ms: Math.round(performance.now() - start),
    })
    return resp
  })
}

export function enableFetchLogging() {
  window.fetch = countingFetch
  return countingFetch
}

export function resetFetchLog() {
  fetchLog.length = 0
}

export function logFetchDetails(label) {
  if (fetchLog.length === 0) return
  console.group(`[fetch] ${label}`)
  console.table(fetchLog)
  console.log(`[fetch] ${label}: ${fetchLog.length} request(s), ${fetchLog.reduce((sum, r) => sum + r.size, 0)} bytes`)
  console.groupEnd()
}

const fetch = enableFetchLogging()

const CHANGE_COUNTERS = [
  'node_created', 'node_modified', 'node_deleted',
  'way_created', 'way_modified', 'way_deleted',
]

// Day counters of the activity timeline: the six node/way change counters
// plus relation creations. The per-uid edit *total* stays node/way-only
// (relations are reputation-only, see dayCount vs totalEdits below).
const DAY_COUNTERS = [...CHANGE_COUNTERS, 'relation_created']

// Top12 tag aspect of the reputation (paper sec. 4, with "address" replaced
// by "place": OSM address tagging uses the addr: prefix).
const TOP12_TAGS = [
  'amenity', 'boundary', 'building', 'highway', 'landuse', 'leisure',
  'name', 'natural', 'place', 'railway', 'sport', 'waterway',
]
export const TAG_COUNTERS = TOP12_TAGS.map((key) => `tag_${key}`)

const ALL_COUNTERS = [...CHANGE_COUNTERS, 'relation_created', ...TAG_COUNTERS]

// The 15 reputation aspects, matching the pipeline's parquet column/stat keys.
const ASPECT_KEYS = ['node', 'way', 'relation', ...TAG_COUNTERS]

// OSMPatrol reputation caps (Neis, Goetz & Zipf 2012, §4). The reputation is
// built only from *created* objects and Top12 tag usage; modifications and
// deletions carry no reputation value. Each of the 12 tags is worth up to 4
// points, so the computable maximum is 20+20+12+48 = 100.
export const REP_CAPS = { node: 20, way: 20, relation: 12 }
const REP_TAG_CAP = 4
const REP_MAX =
  REP_CAPS.node + REP_CAPS.way + REP_CAPS.relation + TOP12_TAGS.length * REP_TAG_CAP
const REP_NOTE = `Reputation 0-100: created nodes ${REP_CAPS.node}, ways ${REP_CAPS.way}, relations ${REP_CAPS.relation} and ${TOP12_TAGS.length} top tags x ${REP_TAG_CAP}; each aspect is capped at its weight and scored by the user's percentile rank among the dataset's contributors active on that aspect`
export const REP_FORMULA =
  `R = min(${REP_CAPS.node}, ${REP_CAPS.node}\u00b7P(n)) + min(${REP_CAPS.way}, ${REP_CAPS.way}\u00b7P(w)) + ` +
  `min(${REP_CAPS.relation}, ${REP_CAPS.relation}\u00b7P(r)) + \u03a3\u1d62 min(${REP_TAG_CAP}, ${REP_TAG_CAP}\u00b7P(t\u1d62))`

export function dayKey(changeDate) {
  return new Date(Number(changeDate) * 86400000).toISOString().slice(0, 10)
}

async function probeTotal(url) {
  const head = await fetch(url, { method: 'HEAD' })
  const contentLength = Number(head.headers.get('Content-Length') ?? 0)
  if (head.ok && contentLength > 0) return contentLength
  const range = await fetch(url, { headers: { Range: 'bytes=0-0' } })
  const m = /bytes \d+-\d+\/(\d+)/.exec(range.headers.get('Content-Range') ?? '')
  return m ? Number(m[1]) : 0
}

async function fetchParquet(baseUrl, path, footerSize) {
  const url = `${baseUrl}/${path}`
  try {
    if (!footerSize) return await asyncBufferFromUrl({ url, fetch })
    const totalBytes = await probeTotal(url)
    if (!totalBytes) return await asyncBufferFromUrl({ url, fetch })
    return await asyncBufferFromUrl({
      url,
      totalBytes,
      rangeStart: totalBytes - footerSize - 8,
      rangeEnd: totalBytes - 1,
      fetch,
    })
  } catch (err) {
    // A fetch failure means the file isn't there, not a fatal query error.
    console.warn(`Skipping ${url}: ${err.message}`)
    return null
  }
}

async function queryRows(baseUrl, path, filter, columns, footerSize) {
  const file = await fetchParquet(baseUrl, path, footerSize)
  if (!file) return []
  if (!footerSize) return parquetQuery({ file, compressors, filter, columns, fetch })
  // footer_size (from the manifest) points exactly at the parquet footer, so
  // the initial fetch reads just those bytes instead of the 512 KB tail
  // window hyparquet uses by default.
  const metadata = await parquetMetadataAsync(file, { initialFetchSize: footerSize + 8, fetch })
  return parquetQuery({ file, compressors, filter, columns, metadata, fetch })
}

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
// reputation and indicator totals -- plus the dataset-wide active/max stats
// read from the file footer. uid and the day columns are small integers
// (int64/uint16), so Number() conversion is lossless.
export async function queryReputationByUsername(baseUrl, path, username, footerSize) {
  const file = await fetchParquet(baseUrl, path, footerSize)
  if (!file) return { rows: [], stats: {} }
  const metadata = await parquetMetadataAsync(
    file, footerSize ? { initialFetchSize: footerSize + 8, fetch } : { fetch })
  const stats = readAspectStats(metadata)
  const rows = await parquetQuery({ file, compressors, filter: { username: { $eq: username } }, metadata, fetch })
  return {
    rows: rows.map((row) => {
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
export async function queryIndicators(baseUrl, path, uids, footerSize) {
  if (uids.length === 0) return []
  const minUid = Math.min(...uids)
  const maxUid = Math.max(...uids)
  const uidSet = new Set(uids)
  // The indicator file holds exactly the timeline's 9 columns (uid,
  // change_date and the seven day counters), so no projection is needed.
  const rows = await queryRows(baseUrl, path, { uid: { $gte: minUid, $lte: maxUid } }, undefined, footerSize)
  return rows.filter((row) => uidSet.has(Number(row.uid)))
}

// OSMPatrol reputation (0..100): taken verbatim from the exact per-uid row of
// user_reputation.parquet, whose per-aspect pct, active and max were computed
// in C++ (aspect capped at its paper weight, scored by the user's percentile
// rank among the contributors active on that aspect: no activity scores 0, a
// unique busiest contributor scores the full weight, and equal counts share
// the same rank). The per-aspect points are cap * pct / 100, recomputed here
// from the stored pct and the constant paper caps; the dataset-wide active/max
// stats come from the file's key_value_metadata. `row` is one wide per-uid
// row as produced by queryReputationByUsername().
function computeReputation(row, stats) {
  const round = (v) => Math.round(Number(v) * 100) / 100
  const detail = (key, label, cap, counter) => {
    const pct = Number(row[`${key}_pct`] ?? 0)
    const stat = stats[key]
    return {
      key,
      label,
      cap,
      raw: Number(row[counter] ?? 0),
      max: stat.max,
      active: stat.active,
      points: round(pct * cap / 100),
      pct,
    }
  }
  return {
    value: Number(row.reputation),
    max: REP_MAX,
    note: REP_NOTE,
    details: [
      detail('node', 'Created nodes', REP_CAPS.node, 'node_created'),
      detail('way', 'Created ways', REP_CAPS.way, 'way_created'),
      detail('relation', 'Created relations', REP_CAPS.relation, 'relation_created'),
      ...TAG_COUNTERS.map((key) => detail(key, `Tag ${key.slice(4)}`, REP_TAG_CAP, key)),
    ],
  }
}

// Totals from the exact user_reputation.parquet row (all 19 indicator sums
// and the identity columns are stored per uid by the pipeline), plus the
// per-day timeline from user_indicators.parquet and the activity-by-day edit
// counts that feed the history graph.
export function computeScores(reputationRows, indicatorRows, stats) {
  const repRow = reputationRows[0]
  const counters = { ...repRow.counters }

  const byDay = new Map()
  for (const row of indicatorRows) {
    const dayCount = DAY_COUNTERS.reduce((sum, k) => sum + Number(row[k] ?? 0), 0)
    const day = dayKey(row.change_date)
    byDay.set(day, (byDay.get(day) ?? 0) + dayCount)
  }

  const totalEdits = CHANGE_COUNTERS.reduce((sum, k) => sum + counters[k], 0)
  const reputation = computeReputation(repRow.reputation, stats)
  return {
    uid: repRow.uid,
    firstSeenDay: repRow.first_seen_day,
    counters,
    totalEdits,
    byDay,
    reputation,
  }
}
