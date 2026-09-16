# Data API

The pipeline writes a self-describing `output-dir/` that any HTTP range-capable
client reads directly: the files are plain Parquet over plain HTTP, so they
work with DuckDB, a browser-side reader such as hyparquet, or the bundled web
viewers — no application backend, no custom protocol. The two access parts:

- **Changes** — `changes/`: monthly partitioned `(h3_cell, change_date,
  node_count, way_count)` change counts.
- **Users** (only with `--user-indicators`) — `user_indicators.parquet` and
  `user_reputation.parquet`: per-user, per-day activity and reputation.

## Output layout

```
output-dir/
├── manifest.json
├── user_indicators.parquet    # only with --user-indicators
├── user_reputation.parquet    # only with --user-indicators
└── changes/
    └── year=2025/
        ├── month=01/
        │   ├── data.parquet      # (h3_cell, change_date, node_count, way_count)
        │   ├── nodes.parquet     # staging, merged and removed by pass 3
        │   └── ways.parquet      # staging, merged and removed by pass 3
        ├── month=02/
        │   └── data.parquet
        └── ...
```

Files are ZSTD-compressed. `manifest.json` is rewritten at the end of every
run from a directory scan of the `changes/` root, so clients know the H3
resolution, which month partitions exist, and the overall date range:

```json
{
  "h3_resolution": 9,
  "date_range": { "min_month": "2005-01", "max_month": "2026-08" },
  "datasets": {
    "changes": {
      "path": "changes",
      "partitions": ["2005-01", "2005-02", "..."]
    },
    "user_indicators": { "path": "user_indicators.parquet", "partitions": [] },
    "user_reputation": { "path": "user_reputation.parquet", "partitions": [] }
  }
}
```

`date_range` is `null` when the dataset is empty. The user-indicator entries
appear only when their files exist; an empty partition list signals a
non-partitioned single file, which month-based query clients (the web
frontend) skip.

## Changes part

Each month's `data.parquet` holds:

| Column | Type | Meaning |
|---|---|---|
| `h3_cell` | `uint64` | H3 index of the cell, at the dataset's `h3_resolution` |
| `change_date` | `uint16` | UTC day count since the Unix epoch (1970-01-01) |
| `node_count` | `uint32` | Node changes in that cell on that day |
| `way_count` | `uint32` | Way changes in that cell on that day |

`change_date` is stored as a `uint16` count of UTC days instead of Parquet's
native `DATE` type, cutting the column from 4 to 2 bytes per row. Reconstruct
the date in a query with `DATE '1970-01-01' + change_date`.

Rows are sorted by `(h3_cell, change_date)` after the merge pass, so
row-group min/max statistics support both bbox pruning and date pruning
within each month file. The calendar-month partitioning bounds the number of
concurrently open Parquet writers by the number of months that contain data —
independent of extract size or H3 resolution.

### Querying with DuckDB

```sql
SELECT DATE '1970-01-01' + change_date AS change_date,
       SUM(node_count) + SUM(way_count) AS total
FROM read_parquet('data/output/changes/year=*/month=*/data.parquet', hive_partitioning = true)
GROUP BY change_date
ORDER BY change_date
LIMIT 20;
```

### Web access

`web/changes/query.js` queries bbox + date range. The date range selects the
month partitions (intersected with the manifest's partition list); each
distinct file is queried once via `parquetQuery`, which prunes row groups on
`h3_cell` and `change_date`, then aggregates `node_count + way_count`
client-side per cell and per day. The non-contiguous H3 cell set of the
viewport bbox is applied as a coarse `[min, max]` range filter first, then
exact membership is checked client-side. `web/changes/app.js` triggers a new
query on pan/zoom (debounced) and takes its `BASE_URL` from the
`BASE_URL` constant (see HTTP transport below).

## Users part

Both files exist only when the pipeline ran with `--user-indicators`. They
are non-partitioned single files, kept that way so the `uid` join stays cheap
and the numerics-only indicators file stays small.

- `user_indicators.parquet` — one row per `(uid, change_date)`, sorted by
  `(uid, change_date)`:

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `change_date` | `uint16` | UTC day (same encoding as `changes/`) |
  | `node_created`, `node_modified`, `node_deleted` | `uint32` | Node change counters |
  | `way_created`, `way_modified`, `way_deleted` | `uint32` | Way change counters |
  | `relation_created` | `uint32` | Relation creations (reputation only, but counted in the viewer's day timeline) |

  The per-day `tag_*` counters are aggregated during finalize and only their
  per-user sums are written (in `user_reputation.parquet`), so they never
  appear per day.

- `user_reputation.parquet` — one row per user, sorted by `username` (ties
  broken by `uid`, so an exact username lookup prunes straight to the
  matching pages):

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `username` | `utf8` | The user's current username (identity, for direct lookup) |
  | `first_seen_day` | `uint16` | Day the user's first watched change appears |
  | `reputation` | `uint8` | Exact current score 0-100 (sum of the per-aspect points) |
  | `node_created` … `tag_waterway` | `uint32` | Per-user sums of the 19 indicator counters |

  Then, for each aspect `node`, `way`, `relation` and `tag_<key>` (each
  Top12 tag):

  | Column | Type | Meaning |
  |---|---|---|
  | `<aspect>_pct` | `float64` | Percentile rank `100 · P` |

  (38 columns total.) Every aspect is computed exactly in C++: `P` is the
  user's rank among the contributors active on that aspect (raw count > 0),
  with equal totals sharing the same rank, a sole contributor at the full
  cap `cap` (=20/20/12 and 4 per tag) and a zero total at 0; nothing is
  sampled and the percentages match the full-table rank exactly. The points a
  user earns on an aspect (`cap · P`) derive from that `pct` and the constant
  paper caps, so only the percentile is stored per row. The dataset-wide
  stats behind the tooltips — the contributors active on each aspect and its
  largest per-user total — have the same value for every user, so the
  pipeline writes them once as file-level Parquet `key_value_metadata`
  (`<aspect>_active`, `<aspect>_max` keys) instead of 30 repeated columns.

### Derivation notes

- Every version event is attributed to the editing `(uid, day)`; day totals
  are sums of the six node/way change counters (the viewer's activity
  timeline adds `relation_created` to each day's count).
- `relation_created` is counted for visible version-1 relations only.
  Relation creations feed the OSMPatrol reputation (built from *created*
  objects only); modifies/deletes are not counted, and relations are
  excluded from the node/way day-total. The heuristic follows
  Neis, Goetz & Zipf, *ISPRS Int. J. Geo-Inf.* 2012, 1(3), 315-332.
- The `tag_*` counters mirror the paper's "Top12" most-used tags, one counter
  per tag (12 × 4 = 48 reputation points), counted only at object creation.
  The paper's `address` key is replaced by `place`, as OSM address tagging
  uses the `addr:` prefix. Like relations, tag usage is reputation-only and
  excluded from the day totals, so only the per-user sums are stored (in
  `user_reputation.parquet`); the per-day file keeps just the change counters
  and `relation_created`.

### Scaling

OSM full history is `(id, version)`-sorted, so the scan is a running pass
with O(1) object state, writing day-aggregates to a staged
`user_indicator_stage/stage_*.parquet` directory that finalize merges, sorts
by `(uid, change_date)`, derives the reputation rows, and removes.
`user_reputation.parquet` is a pure derived view of that data: one row per
user, so it grows with new users, not new edits, and can be rebuilt from the
per-user totals without re-reading history.

### Querying with DuckDB

Join the files on `uid` to examine users:

```sql
SELECT r.username,
       DATE '1970-01-01' + i.change_date AS change_date,
       i.node_created + i.node_modified + i.node_deleted +
       i.way_created + i.way_modified + i.way_deleted AS edits,
       i.relation_created
FROM read_parquet('output-dir/user_reputation.parquet') r
JOIN read_parquet('output-dir/user_indicators.parquet') i USING (uid)
ORDER BY edits DESC
LIMIT 20;
```

### Web access

`web/users/query.js` looks a user up by exact username on
`user_reputation.parquet` (the pipeline stamps the current username per uid,
and the file is username-sorted with a uid tie-break, so the exact filter
prunes straight to the matching pages). The reputation, identity fields and
per-indicator totals all come from that same user row; the dataset-wide
`active`/`max` aspect stats are read once from the file's
`key_value_metadata` footer instead of repeated per-row columns, and the
per-aspect points are recomputed from the stored `pct` and the constant paper
caps — no ranking or percentile math runs in the browser.

Only the per-day activity timeline is then read from
`user_indicators.parquet`: a `uid` `[min, max]` range filter prunes the
uid-sorted file to the pages holding that user, with exact membership kept
client-side. The day counts add `relation_created` to the six node/way change
counters; the per-uid edit total stays node/way-only (relations are
reputation-only).

## HTTP transport

Clients read the files with byte-range requests: hyparquet's
`asyncBufferFromUrl` opens each file and fetches the footer, row-group
metadata and pages it needs, so a large file is never downloaded in full. The
server must therefore support `Range` (`206 Partial Content`,
`Accept-Ranges: bytes`). When the frontend and the data are served from
different origins, the data host must also set permissive CORS headers —
the `Caddyfile` used by the bundled `caddy` service sets both:

```
header Access-Control-Allow-Origin "*"
header Access-Control-Allow-Methods "GET, HEAD, OPTIONS"
header Access-Control-Allow-Headers "Range"
header Access-Control-Expose-Headers "Content-Range, Content-Length, Accept-Ranges"
```

The web viewers take their data root from the `BASE_URL` constant in
`web/changes/app.js` and `web/users/app.js` (default `http://localhost:8080/data`,
serving `output-dir/` at the `/data/` path and the frontend at the root).