# Data API

This document is the data contract of the Parquet files written by the
pipeline: their layout, schemas and encodings, for any tool that wants to
read them — DuckDB, a Parquet library, or a browser-side reader. The files
are plain Parquet (ZSTD-compressed); nothing else is needed to consume them.
The two access parts:

- **Changes** — `changes/`: yearly partitioned `(h3_cell, change_date,
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
        ├── data.parquet      # (h3_cell, change_date, node_count, way_count)
        ├── nodes.parquet     # staging, merged and removed by pass 3
        └── ways.parquet      # staging, merged and removed by pass 3
```

Files are ZSTD-compressed. `manifest.json` is rewritten at the end of every
run from a directory scan of the `changes/` root plus each `data.parquet`'s
footer statistics, so clients know the H3 resolution, which year partitions
exist, and the exact first/last day with data (`date_range`; omitted when no
row data exists yet):

```json
{
  "h3_resolution": 9,
  "date_range": { "min_date": "2005-01-01", "max_date": "2026-08-19" },
  "datasets": {
    "changes": {
      "path": "changes",
      "partitions": ["2005", "2006", "..."],
      "partition_footer_sizes": { "2005": 41298, "2006": 39807, "..." }
    },
    "user_indicators": { "path": "user_indicators.parquet", "partitions": [], "footer_size": 35112 },
    "user_reputation": { "path": "user_reputation.parquet", "partitions": [], "footer_size": 40894 }
  }
}
```

`partition_footer_sizes` (one entry per readable `data.parquet` year) and
`footer_size` (non-partitioned user files) give the byte length of each
file's footer metadata, so browser readers fetch exactly the footer instead
of the trailing 512 KB tail window. They are omitted when the file is
missing or not a plain Parquet file.

`date_range` is the merged `change_date` min/max read from each year's
`data.parquet` footer (column statistics), so date pickers can bound their
inputs to the actual data span rather than the edge whole years. The
user-indicator entries appear only when their files exist; an empty
partition list signals a non-partitioned single file, which year-based
query clients skip.

## Changes part

Each year's `data.parquet` holds:

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
within each year file.

### Querying with DuckDB

```sql
SELECT DATE '1970-01-01' + change_date AS change_date,
       SUM(node_count) + SUM(way_count) AS total
FROM read_parquet('data/output/changes/year=*/data.parquet', hive_partitioning = true)
GROUP BY change_date
ORDER BY change_date
LIMIT 20;
```

## Users part

Both files exist only when the pipeline ran with `--user-indicators`. They
are two non-partitioned single files.

- `user_indicators.parquet` — one row per `(uid, change_date)`, sorted by
  `(uid, change_date)`:

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `change_date` | `uint16` | UTC day (same encoding as `changes/`) |
  | `node_created`, `node_modified`, `node_deleted` | `uint32` | Node change counters |
  | `way_created`, `way_modified`, `way_deleted` | `uint32` | Way change counters |
  | `relation_created` | `uint32` | Relation creations (reputation only; part of a day's activity total) |

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
  stats — the contributors active on each aspect and its largest per-user
  total — have the same value for every user, so the pipeline writes them
  once as file-level Parquet `key_value_metadata` (`<aspect>_active`,
  `<aspect>_max` keys) instead of 30 repeated columns.

### Derivation notes

- Every version event is attributed to the editing `(uid, day)`; day totals
  are sums of the six node/way change counters; a day's activity totals add
  `relation_created` to that sum.
- `relation_created` is counted for visible version-1 relations only.
  Relation creations feed the OSMPatrol reputation (built from *created*
  objects only); modifies/deletes are not counted, and relations are
  excluded from the node/way day-total.
- The `tag_*` counters mirror the paper's "Top12" most-used tags, one counter
  per tag (12 × 4 = 48 reputation points), counted only at object creation.
  The paper's `address` key is replaced by `place`, as OSM address tagging
  uses the `addr:` prefix. Like relations, tag usage is reputation-only and
  excluded from the day totals, so only the per-user sums are stored (in
  `user_reputation.parquet`); the per-day file keeps just the change counters
  and `relation_created`.

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