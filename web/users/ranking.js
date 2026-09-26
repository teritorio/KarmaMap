// OSMPatrol-style ranking domain logic (Neis, Goetz & Zipf 2012, §4).
// Pure computation over the rows fetched by query.js: no parquet or network
// access. The per-aspect `points` are recomputed from the stored `pct` and
// the paper's constant caps, and the dataset-wide `active`/`max` stats come
// from the file's key_value_metadata. No percentile math runs in the browser.

import { dayKey } from '../lib/api.js'

const CHANGE_COUNTERS = [
  'node_created', 'node_modified', 'node_deleted',
  'way_created', 'way_modified', 'way_deleted',
]

// Top12 tag aspect of the ranking (paper sec. 4, with "address" replaced
// by "place": OSM address tagging uses the addr: prefix).
const TOP12_TAGS = [
  'amenity', 'boundary', 'building', 'highway', 'landuse', 'leisure',
  'name', 'natural', 'place', 'railway', 'sport', 'waterway',
]
export const TAG_COUNTERS = TOP12_TAGS.map((key) => `tag_${key}`)

// All counter columns of user_ranking.parquet (changes + relation
// creations + tags), used to project a wide per-uid row into a counters map.
export const ALL_COUNTERS = [...CHANGE_COUNTERS, 'relation_created', ...TAG_COUNTERS]

// Bits of the daily vandalism_flag column of users_history.parquet,
// mirroring src/vandalism.hpp. Filter 2/3 are per-day occurrences; filter 1
// is forward-only, set on new rows for a below-threshold contributor.
export const FLAG_FILTER_2 = 0x01
export const FLAG_FILTER_3 = 0x02
export const FLAG_FILTER_1 = 0x04

export const FLAG_LABELS = [
  { mask: FLAG_FILTER_2, label: 'Filter 2: >500 modified/deleted in one hour' },
  { mask: FLAG_FILTER_3, label: 'Filter 3: node moved >500 m' },
  { mask: FLAG_FILTER_1, label: 'Filter 1: ranking <5% (set on new edits)' },
]

// OSMPatrol ranking caps (Neis, Goetz & Zipf 2012, §4). The ranking is
// built only from *created* objects and Top12 tag usage; modifications and
// deletions carry no ranking points. Each of the 12 tags is worth up to 4
// points, so the computable maximum is 20+20+12+48 = 100.
export const RANK_CAPS = { node: 20, way: 20, relation: 12 }
const RANK_TAG_CAP = 4
const RANK_MAX =
  RANK_CAPS.node + RANK_CAPS.way + RANK_CAPS.relation + TOP12_TAGS.length * RANK_TAG_CAP
const RANK_NOTE = `Ranking 0-100: created nodes ${RANK_CAPS.node}, ways ${RANK_CAPS.way}, relations ${RANK_CAPS.relation} and ${TOP12_TAGS.length} top tags x ${RANK_TAG_CAP}; each aspect is capped at its weight and scored by the user's percentile rank among the dataset's contributors active on that aspect`
export const RANK_FORMULA =
  `Ranking = min(${RANK_CAPS.node}, ${RANK_CAPS.node}\u00b7P(n)) + min(${RANK_CAPS.way}, ${RANK_CAPS.way}\u00b7P(w)) + ` +
  `min(${RANK_CAPS.relation}, ${RANK_CAPS.relation}\u00b7P(r)) + \u03a3\u1d62 min(${RANK_TAG_CAP}, ${RANK_TAG_CAP}\u00b7P(t\u1d62))`

// OSMPatrol ranking (0..100): taken verbatim from the exact per-uid row of
// user_ranking.parquet, whose per-aspect pct, active and max were computed
// in C++ (aspect capped at its paper weight, scored by the user's percentile
// rank among the contributors active on that aspect: no activity scores 0, a
// unique busiest contributor scores the full weight, and equal counts share
// the same rank). The per-aspect points are cap * pct / 100, recomputed here
// from the stored pct and the constant paper caps; the dataset-wide active/max
// stats come from the file's key_value_metadata. `row` is one wide per-uid
// row as produced by queryRankingByUsername().
function computeRanking(row, stats) {
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
    value: Number(row.ranking),
    max: RANK_MAX,
    note: RANK_NOTE,
    details: [
      detail('node', 'Created nodes', RANK_CAPS.node, 'node_created'),
      detail('way', 'Created ways', RANK_CAPS.way, 'way_created'),
      detail('relation', 'Created relations', RANK_CAPS.relation, 'relation_created'),
      ...TAG_COUNTERS.map((key) => detail(key, `Tag ${key.slice(4)}`, RANK_TAG_CAP, key)),
    ],
  }
}

// Totals from the exact user_ranking.parquet row (all 21 history sums
// and the identity columns are stored per uid by the pipeline), plus the
// per-day timeline from users_history.parquet: the activity-by-day edit
// counts and per-day vandalism_flag masks that feed the history graph.
export function computeScores(rankingRows, historyRows, stats) {
  const rankRow = rankingRows[0]
  const counters = { ...rankRow.counters }

  const byDay = new Map()
  const flagByDay = new Map()
  for (const row of historyRows) {
    const dayCount = Number(row.count ?? 0)
    const day = dayKey(row.change_date)
    byDay.set(day, (byDay.get(day) ?? 0) + dayCount)
    flagByDay.set(day, (flagByDay.get(day) ?? 0) | Number(row.vandalism_flag ?? 0))
  }

  const totalEdits = CHANGE_COUNTERS.reduce((sum, k) => sum + counters[k], 0)
  const ranking = computeRanking(rankRow.ranking, stats)
  return {
    uid: rankRow.uid,
    firstSeenDay: rankRow.first_seen_day,
    counters,
    totalEdits,
    byDay,
    flagByDay,
    ranking,
  }
}
