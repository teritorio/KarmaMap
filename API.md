# Data API

This document is the data contract of the Parquet files written by the
pipeline: their layout, schemas and encodings, for any tool that wants to
read them — DuckDB, a Parquet library, or a browser-side reader. The files
are plain Parquet (ZSTD-compressed); nothing else is needed to consume them.
The two access parts:

- **Changes** — `changes/`: yearly partitioned `(h3_cell, change_date,
  count)` change counts (node + way changes merged per cell per day).
- **Users** — `users_history.parquet` and
  `user_reputation.parquet`: per-user, per-day activity and reputation.
- **Vandalism** — `vandalism_minutes.bin` (binary, not Parquet): the
  update-only OSMPatrol filter-2 source; filter-3 fold stages are transient.
  The per-day flags (bits for filters 2 and 3) land in
  `users_history.parquet`.

## Output layout

```
output-dir/
├── manifest.json
├── users_history.parquet
├── user_reputation.parquet
└── changes/
    └── year=2025/
        ├── data.parquet      # (h3_cell, change_date, count)
        ├── nodes.parquet     # staging, merged and removed by pass 3
        └── ways.parquet      # staging, merged and removed by pass 3

# Next to the node caches (output-dir's parent by default):
node_positions.cache      # full-history node cache (import/prepare-update)
node_positions.cache.last  # incremental cache (prepare-update/update)
vandalism_minutes.bin     # update-only; binary block store
diffs/                    # update-only; in-flight diff download cache
```

Files are ZSTD-compressed. `manifest.json` is rewritten at the end of every
run from a directory scan of the `changes/` root plus each `data.parquet`'s
footer statistics, so clients know the H3 resolution, which year partitions
exist, and the exact first/last day with data (`date_range`; omitted when no
row data exists yet):

```json
{
  "h3_resolution": 9,
  "source": {
    "url": "https://osm-internal.download.geofabrik.de/africa/canary-islands-updates/",
    "sequence_number": 2847632,
    "timestamp": "2019-12-30T09\\:36\\:32Z"
  },
  "date_range": { "min_date": "2005-01-01", "max_date": "2026-08-19" },
  "datasets": {
    "changes": {
      "path": "changes",
      "partitions": ["2005", "2006", "..."],
      "partition_footer_sizes": { "2005": 41298, "2006": 39807, "..." }
    },
    "users_history": { "path": "users_history.parquet", "partitions": [], "footer_size": 35112 },
    "user_reputation": { "path": "user_reputation.parquet", "partitions": [], "footer_size": 40894 }
  }
}
```

`source` records the input snapshot's osmosis replication provenance: the
normalized update URL (trailing `/` guaranteed; empty when import got no
`--update-url`), and the replication `sequence_number` and `timestamp` read
from the snapshot's `<base>.state.txt` sidecar (that day's state, downloaded
with wget alongside the osh). `sequence_number` is a JSON number; `url` and
`timestamp` are strings, with the osmosis `\:` timestamp escaping preserved.
`prepare-update` rewrites only the URL (keeping the recorded sequence and
timestamp, and requiring an already recorded URL to match).

`partition_footer_sizes` (one entry per readable `data.parquet` year) and
`footer_size` (non-partitioned user files) give the byte length of each
file's footer metadata, so browser readers fetch exactly the footer instead
of the trailing 512 KB tail window. They are omitted when the file is
missing or not a plain Parquet file.

`date_range` is the merged `change_date` min/max read from each year's
`data.parquet` footer (column statistics), so date pickers can bound their
inputs to the actual data span rather than the edge whole years. The
users-history entries appear only when their files exist; an empty
partition list signals a non-partitioned single file, which year-based
query clients skip.

## Changes part

Each year's `data.parquet` holds:

| Column | Type | Meaning |
|---|---|---|
| `h3_cell` | `uint64` | H3 index of the cell, at the dataset's `h3_resolution` |
| `change_date` | `uint16` | UTC day count since the Unix epoch (1970-01-01) |
| `count` | `uint32` | Node + way changes in that cell on that day |

`change_date` is stored as a `uint16` count of UTC days instead of Parquet's
native `DATE` type, cutting the column from 4 to 2 bytes per row. Reconstruct
the date in a query with `DATE '1970-01-01' + change_date`.

Rows are sorted by `(h3_cell, change_date)` after the merge pass, so
row-group min/max statistics support both bbox pruning and date pruning
within each year file. Only `h3_cell` and `change_date` carry footer
row-group statistics; `count` is written without them
to keep the footer metadata compact.

### Querying with DuckDB

```sql
SELECT DATE '1970-01-01' + change_date AS change_date,
       SUM(count) AS total
FROM read_parquet('data/output/changes/year=*/data.parquet', hive_partitioning = true)
GROUP BY change_date
ORDER BY change_date
LIMIT 20;
```

## Update mode

`karmamap update` downloads osmosis replication diffs (`.osc.gz`, one per
sequence), folds them into the existing dataset, and removes each diff file
once its passes succeeded. Each rewritten year's
`data.parquet` footer carries a `karmamap_source_seq` key_value_metadata
entry holding the highest applied sequence number as a string: the merge is
apply-once, so a partition stamped at or beyond the run's sequence is never
rewritten again (orphaned per-sequence staging files under that year, plus
any staging from sequences beyond the run's applied one, are simply
deleted). The `changes/` staging files from an update run are named
`nodes.<seq>.parquet`/`ways.<seq>.parquet` (vs. the full-run `nodes.parquet`/
`ways.parquet`), describing the sequence each delta came from; they exist
only until the first merge after their fetch.

## Users part

Both files are written by every import and update run. They
are two non-partitioned single files.

- `users_history.parquet` — one row per `(uid, change_date)`, sorted by
  `(uid, change_date)`:

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `change_date` | `uint16` | UTC day (same encoding as `changes/`) |
  | `count` | `uint32` | Total activity that day: the six node/way change counters plus the three relation counters (created, modified, deleted) |
  | `vandalism_flag` | `uint8` | Per-day OSMPatrol flag: bit 0 (`0x01`) = any of the day's minutes had > 500 modified+deleted objects within a one-hour window; bit 1 (`0x02`) = a modified node moved more than 500 m that day; bit 2 (`0x04`) = the day's user has reputation < 5% (filter 1, "new users or low reputation"; a contributor who created nothing ranks 0). All bits are monotonic and forward-only: import writes 0; bits 0/1 are ORed by every update finalize from `vandalism_minutes.bin` plus the run's move-flagged days, and bit 2 is set only on the rows that update run newly writes for a below-threshold contributor. Base rows are carried unchanged, so once set a bit persists and a reputation drop never re-flags the past |

  The per-day `tag_*` counters are aggregated during finalize and only their
  per-user sums are written (in `user_reputation.parquet`), so they never
  appear per day.

  Only `uid` carries footer row-group statistics in `users_history.parquet`
  (it is the sole pruning column); in `user_reputation.parquet` both `username`
  (exact filter) and `uid` (stable identity key) do. Every other column is
  written without them to keep the footer metadata compact.

- `user_reputation.parquet` — one row per user, sorted by `username` (ties
  broken by `uid`, so an exact username lookup prunes straight to the
  matching pages):

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `username` | `utf8` | The user's current username (identity, for direct lookup) |
  | `first_seen_day` | `uint16` | Day the user's first watched change appears |
  | `reputation` | `uint8` | Exact current score 0-100 (sum of the per-aspect points) |
  | `node_created` … `tag_waterway` | `uint32` | Per-user sums of the 21 history counters |

  Then, for each aspect `node`, `way`, `relation` and `tag_<key>` (each
  Top12 tag):

  | Column | Type | Meaning |
  |---|---|---|
  | `<aspect>_pct` | `float64` | Percentile rank `100 · P` |

  (40 columns total.) Every aspect is computed exactly in C++: `P` is the
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

- Every version event is attributed to the editing `(uid, day)`. The stored
  per-day `count` is the total activity that day: the six node/way change
  counters plus the three relation counters (created, modified, deleted).
- Only visible version-1 relation creations feed the OSMPatrol reputation
  (built from *created* objects only); relation modifies/deletes add to a
  day's `count` but carry no reputation value.
- The `tag_*` counters mirror the paper's "Top12" most-used tags, one counter
  per tag (12 × 4 = 48 reputation points), counted only at object creation.
  The paper's `address` key is replaced by `place`, as OSM address tagging
  uses the `addr:` prefix. Like relations, tag usage is reputation-only and
  excluded from the day totals, so only the per-user sums are stored (in
  `user_reputation.parquet`); the per-day file keeps just `count`.

### Querying with DuckDB

Join the files on `uid` to examine users:

```sql
SELECT r.username,
       DATE '1970-01-01' + i.change_date AS change_date,
       i.count AS edits,
       r.relation_created
FROM read_parquet('output-dir/user_reputation.parquet') r
JOIN read_parquet('output-dir/users_history.parquet') i USING (uid)
ORDER BY edits DESC
LIMIT 20;
```

## Vandalism part

The vandalism outputs are update-only: they cover the period after the
recorded replication sequence and are absent after a pure import. They
implement the OSMPatrol filters 2 (`> 500 modified/deleted in one hour`) and 3
(node moved beyond 500 m); filter 1 (new users / reputation < 5%) is a
forward-only bit (bit 2 of `vandalism_flag`) that the users-history update
finalize sets on the rows it newly writes. Filters 2/3 fold into the per-day
`vandalism_flag` bits of `users_history.parquet`; filter 2 draws on one binary
store and filter 3's move staging is transient.

- `vandalism_minutes.bin` — the **binary** per-`(uid, minute)` modified+
  deleted counts behind the filter-2 flag (bit 0 of `vandalism_flag`); it is a
  binary store, not part of `manifest.json` or the Parquet contract. Format (see
  `src/vandalism_store.hpp`): a 40-byte header (magic `VMIN`, version, record
  size, record/block counts, the `karmamap_source_seq`-equivalent
  applied-sequence stamp), followed by ZSTD blocks of `2^18` 16-byte records
  `(uid, minute, count)` and a per-block directory of `(first_uid, compressed
  size)`. `uid` is big-endian with the sign bit flipped,
  `minute`/`count` big-endian, all strictly ascending and unique per
  `(uid, minute)`. Each finalize run merges its staged buckets into this store
  and stamps it, so it is the flag's complete source of truth.

- Filter 3 stages the run's detected node moves (`> 500` m, as `(uid, minute)`
  rows, gated by the sink) under the update stage root. `flagged_move_days`
  folds them into `(uid, day) -> bit-1` flags for `users_history.parquet`
  and removes the root; the base file's flags are carried forward and ORed, so
  the combination is monotonic across reruns. The distance is measured from
  the old H3 cell center to the new point: off by up to one res-9 cell radius
  (~175 m), fine for the 500 m screen, not for the finer 11 m edit-analysis
  flag.
