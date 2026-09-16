# osh_change_index

Reads an OSM full-history file (`.osh.pbf`) and produces one partitioned
Parquet dataset — `changes/` — with `(h3_cell, change_date, node_count,
way_count)` rows, partitioned by calendar month
(`year=YYYY/month=MM/data.parquet`, standard hive partitioning), for
bbox + date-range queries (e.g. with DuckDB or the included web frontend).
`node_count` counts node changes, `way_count` way changes, in the same file
per month so a client reads one dataset per month. `change_date` is a raw
UTC day count since 1970-01-01 stored as `uint16` — the same value the
node-cache records use.

A single binary, `osh_change_index`, runs three stages by default:

1. Node pass: builds the mmap node cache `(node_id, day) -> position` and
   counts node changes into `changes/year=YYYY/month=MM/nodes.parquet`.
2. Way pass: resolves each way's node positions via the cache and counts
   way changes at the distinct cells of those positions into
   `changes/year=YYYY/month=MM/ways.parquet`.
3. Merge pass: full-outer-joins each month's `nodes.parquet` and
   `ways.parquet` on `(h3_cell, change_date)` into `data.parquet`, sorted by
   `(h3_cell, change_date)` so row-group min/max support bbox and
   date-range pruning. Idempotent: an existing `data.parquet` supplies
   whichever count's staging file is already gone, so re-merging never
   zeroes it.

Passes 1-3 are independent of the optional user-indicator pass described
below.

## User indicators (`--user-indicators`)

An optional, H3-independent pass that scores history **per user and per UTC
day** with cheap OSMPatrol-style heuristics, targeting vandalism/bulk
editors. It is a single streaming scan over the node and way history plus
relation creations (no changeset metadata is needed) and one in-memory
finalize, and is independent of passes 1-3. It writes two non-partitioned
Parquet files:

```
output-dir/
├── user_indicators.parquet  # daily counters + flags, one row per (uid, change_date)
└── user_reputation.parquet  # exact per-uid reputation + all indicator totals
```

- `user_indicators.parquet` — one row per `(uid, change_date)`:

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `change_date` | `uint16` | UTC day (same encoding as `changes/`) |
  | `node_created`, `node_modified`, `node_deleted` | `uint32` | Node change counters |
  | `way_created`, `way_modified`, `way_deleted` | `uint32` | Way change counters |
  | `relocated` | `uint32` | Node versions moved > `--relocate-meters` |
  | `short_lived` | `uint32` | Deletes of objects created ≤ `--short-life-days` earlier |
  | `rapid_edit` | `uint32` | Object versions arriving with ≥ `--rapid-edit-versions` versions within `--rapid-edit-window-days` |
  | `relation_created` | `uint32` | Relation creations (reputation only, see below) |
  | `tag_amenity` … `tag_waterway` | `uint32` | Top12 tag usage on created objects (reputation, see below) |

  Sorted by `(uid, change_date)`.

- `user_reputation.parquet` — one row per user, sorted by `username`
  (ties broken by `uid`, so an exact username lookup prunes straight to the
  matching pages):

  | Column | Type | Meaning |
  |---|---|---|
  | `uid` | `int64` | OSM user id |
  | `username` | `utf8` | The user's current username (identity, for direct lookup) |
  | `first_seen_day` | `uint16` | Day the user's first watched change appears |
  | `bulk_new_user` | `bool` | New-user-window bulk upload flag (see below) |
  | `max_day_changes` | `uint32` | Most node/way modified+deleted events in a single day |
  | `reputation` | `uint8` | Exact current score 0-100 (sum of the per-aspect points) |
  | `node_created` … `tag_waterway` | `uint32` | Per-user sums of the 22 indicator counters |

  Then, for each aspect `node`, `way`, `relation` and `tag_<key>` (each
  Top12 tag):

  | Column | Type | Meaning |
  |---|---|---|
  | `<aspect>_pct` | `float64` | Percentile rank `100 · P` |

  (43 columns total.) Every aspect is computed exactly in C++: `P` is the
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
  The viewer fetches a user by exact username from this file (the pipeline
  stamps the current username per uid, so the identity columns land on the
  same row) and reads the per-day timeline from `user_indicators.parquet`.

Derivation notes:

- Every version event is attributed to the editing `(uid, day)`; day totals
  are sums of the six node/way change counters. `relocated` compares
  consecutive versions of the same node (version-to-version move),
  `short_lived` counts an object's delete relative to its own first version,
  and `rapid_edit` counts each version that brings its object's rolling
  window up to threshold.
- `relation_created` is counted for visible version-1 relations only.
  Relation creations feed the OSMPatrol reputation (built from *created*
  objects only); modifies/deletes are not counted, and relations are
  excluded from day totals and `bulk_new_user`. The heuristic follows
  Neis, Goetz & Zipf, *ISPRS Int. J. Geo-Inf.* 2012, 1(3), 315-332.
- The `tag_*` columns mirror the paper's "Top12" most-used tags, one counter
  per tag (12 × 4 = 48 reputation points), counted only at object creation.
  The paper's `address` key is replaced by `place`, as OSM address tagging
  uses the `addr:` prefix. Like relations, tag usage is reputation-only and
  excluded from day totals.
- `bulk_new_user` is derived in finalize from the daily rows themselves
  (the events within `new_user_window_days` of the user's first seen day),
  so it needs no extra history scan.
- Rule thresholds (defaults in parentheses) come from `--` flags:

  | Flag | Default | Role |
  |---|---|---|
  | `--relocate-meters` | `500` | Node move distance counting as a relocation |
  | `--short-life-days` | `7` | Max age (days) of a created+deleted object |
  | `--rapid-edit-versions` | `5` | Versions that trigger the rapid-edit flag |
  | `--rapid-edit-window-days` | `7` | Rolling window for rapid-edit counting |
  | `--new-user-window-days` | `30` | Days after first seen edit a user counts as new |
  | `--bulk-edit-min` | `10` | Edits within the window that flag `bulk_new_user` |

These are documented starting points, not calibrated against ground truth:
suspicion only. Join the files on `uid` to prioritize users, e.g. with
DuckDB:

```sql
SELECT r.username,
       DATE '1970-01-01' + i.change_date AS change_date,
       i.node_created + i.node_modified + i.node_deleted +
       i.way_created + i.way_modified + i.way_deleted AS edits,
       i.relocated, i.short_lived, i.rapid_edit, i.relation_created,
       i.tag_highway, i.tag_building
FROM read_parquet('output-dir/user_reputation.parquet') r
JOIN read_parquet('output-dir/user_indicators.parquet') i USING (uid)
WHERE r.bulk_new_user
ORDER BY edits DESC
LIMIT 20;
```

Scaling: OSM full history is `(id, version)`-sorted, so the scan is a
running pass with O(1) object state, writing day-aggregates to a staged
`user_indicator_stage/stage_*.parquet` directory that finalize merges,
sorts by `(uid, change_date)`, derives the reputation rows, and removes.
`user_reputation.parquet` is a pure derived view of that data: one row per
user, so it grows with new users, not new edits, and a later update pass can
rebuild it from the per-user totals without re-reading history.
Non-partitioned single files keep the join cheap and the numerics-only
indicators file small.

## Business rules

| Case | Behavior |
|---|---|
| Node with valid coordinates | Written to the node cache + counted |
| Deleted node with a previously known position | Counted on the last known position, no cache write |
| Node with no coordinates and no previously known position | Skipped |
| Way node with an unresolved position | The node is skipped |
| Deleted way with a previously known geometry | Counted on the last known geometry |
| Deleted way with no previously known geometry | Skipped |
| Visible way with no nodes | Skipped |
| Relations | Out of scope for the change-counting passes; the `--user-indicators` pass counts relation creations only |
| Node cells of a way | Each distinct node cell counted once per way version |
| Time zone | Strict UTC |
| Source file ordering | Assumed sorted by `(id, version)` ascending, as documented for OSM full-history files |

## Resolution

The pipeline uses a single H3 resolution:

- **`--h3-resolution`** (default 9): the resolution of the cells stored in
  the Parquet `h3_cell` column and used across all passes. Range 0-13 (the
  node cache packs at most 13 H3 digits into 6 bytes).

Rows are partitioned by the calendar month of `change_date`, not by H3
cell, so the number of concurrently open Parquet writers is bounded by the
number of months that contain data — independent of extract size or H3
resolution.

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

`change_date` is stored as a `uint16` count of UTC days since the Unix
epoch (1970-01-01) instead of Parquet's native `DATE` type, cutting the
column from 4 to 2 bytes per row. Reconstruct the date in a query with
`DATE '1970-01-01' + change_date`.

`data.parquet` rows are sorted by `(h3_cell, change_date)` after the merge
pass, so row-group min/max statistics are useful for both bbox pruning and
date pruning within each month file.

`manifest.json` is rewritten at the end of every run from a directory scan
of the `changes/` root, so clients know the H3 resolution, which month
partitions exist, and the overall date range:

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

`date_range` is `null` when the dataset is empty. The user-indicator
entries appear only when their files exist; an empty partition list signals
a non-partitioned single file, which month-based query clients (the web
frontend) skip.

## Node cache

The `--node-cache` argument points at a single file that pass 1 builds and
pass 2 reads (pass 1 deletes and recreates it):

```
record : [node_id 8B][day 2B][h3 cell 6B]   (16 bytes)
file   : [header 40B][block 0]...[block N-1][directory 12*N]
header : magic "OSNC", version, record_size, h3_resolution,
         record count, block count, records per block, compression
```

Records are sorted by `(node_id, day)` ascending. `node_id` is stored
big-endian with the sign bit flipped so byte order equals numeric order;
`day` is the same uint16 UTC epoch-day value as `change_date`; the cell is
the node's H3 index at `--h3-resolution` (0-13) packed into 6 little-endian
bytes (`h3_utils::pack_cell`), with the resolution re-applied from the
header on read. The writer
enforces the ascending order while building (a decreasing node id or day
aborts loudly), so the binary search and the way pass's sweep are safe. A
node edited several times in one day is stored once (last position wins).
Node deletions are not stored: resolving a deleted node returns its last
known position — an accepted approximation.

Records are grouped into blocks of 2¹⁸ (4 MiB raw) that are ZSTD-compressed
on write and appended as-is; the block and record counts are patched into
the header at finish, and a trailing directory holds each block's first
node and compressed size (offsets cumulative, recomputed in RAM at open).
The `compression`/`records-per-block` header fields act as a format stamp:
a cache written by another record layout, or a truncated file, aborts with
rebuild instructions. Worst case (incompressible data) ZSTD stores a block
raw, so the file is never meaningfully larger than the uncompressed
layout; the cost is one decompression per block on first access of each
pass.

The reader mmaps the file read-only and decompresses one block at a time
into a 4 MiB cache. The per-block first keys narrow single lookups and
reposition the way pass's sweep cursor between batches; no RAM sample index
is kept.

## Configuration

Host paths are read from a `.env` file (see `.env.template`) and used by
`docker-compose.yml`. `DATA_DIR` is mounted at `/data` for the
`osh_change_index` service — `--input`, `--node-cache` and `--output-dir`
are absolute paths under it — and `OUTPUT_DIR` is served read-only by the
`caddy` service.

```bash
cp .env.template .env
```

```
DATA_DIR=./data/
```

Without a `.env`, the compose defaults `DATA_DIR=./data/` (mounted at `/data`)
and `OUTPUT_DIR=./data/output` apply, so the examples in the next section
work as written.

## Build

Base image: Debian (`debian:bookworm` build stage,
`debian:bookworm-slim` runtime stage).

```bash
docker compose --profile=* build
```

## Tests

The C++ tests use Google Test (Debian `libgtest-dev`) and run through CTest.
They are compiled and executed as part of the Docker build stage, so a
failing test fails the image build. Run them manually with:

```bash
docker build --target build -t osh_change_index:build . \
  && docker run --rm osh_change_index:build bash -c "ctest --test-dir build --output-on-failure"
```

Build without tests by configuring with `-DOSH_ENABLE_TESTS=OFF`.

## Usage

```
osh_change_index --input <planet.osh.pbf> --node-cache <file> --output-dir <dir> [core options] [fine-tuning options]
```

### Core options

| Option | Description |
|---|---|
| `--input` | OSM full-history file (`.osh.pbf`), required |
| `--node-cache` | Node position cache file (wiped and rebuilt by pass 1, read by pass 2), required |
| `--output-dir` | Output directory for the Parquet datasets, required (created if missing) |
| `--pass` | `1` (nodes only), `2` (ways only, requires an already populated node cache), `3` (merge + sort only, requires passes 1 and 2 to have already run), or `all` (default) |
| `--way-batch-mb` | Way-pass lookup batch budget in MiB (default: `512`) |

### Fine-tuning options

H3 cell:

| Option | Description |
|---|---|
| `--h3-resolution` | Resolution of the data cells, 0-13 (default: `9`) |

User stats (`--user-indicators`):

| Option | Description |
|---|---|
| `--user-indicators` | Also run the user-indicator pass (see above); independent of passes 1-3 |
| `--relocate-meters` | Relocation threshold in meters (default: `500`) |
| `--short-life-days` | Short-lived delete window in days (default: `7`) |
| `--rapid-edit-versions` | Rapid-edit version threshold (default: `5`) |
| `--rapid-edit-window-days` | Rapid-edit rolling window in days (default: `7`) |
| `--new-user-window-days` | New-user window after first edit in days (default: `30`) |
| `--bulk-edit-min` | Edits within the window that flag `bulk_new_user` (default: `10`) |

## Running

Place the input file under `DATA_DIR/input` (default `data/input/`), then:

```bash
docker compose --profile=build run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output
```

To resume after an earlier stage, run the passes one at a time (a way-only
fix-up after a completed run re-runs `--pass 2` — which puts `ways.parquet`
back next to the already-removed `nodes.parquet` — then `--pass 3`,
sourcing node counts from the existing `data.parquet`):

```bash
docker compose --profile=build run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output --pass 1

docker compose --profile=build run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output --pass 2

docker compose --profile=build run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output --pass 3
```

Pass 1 wipes `changes/` (so a re-run never leaves stale partitions) and the
node cache file before rebuilding; pass 2 does not wipe the root — it adds
`ways.parquet` staging files next to `nodes.parquet`, so any pass can run
alone. The manifest is rebuilt at the end of every run.

## Serving the output and the web frontend

The `caddy` service serves everything from a single port `8080`: the web
frontend at the root and `OUTPUT_DIR` (Parquet partitions + `manifest.json`)
under `/data/`, with range requests and permissive CORS on the data path —
what the browser-side Parquet reader (hyparquet) needs to query data
directly, no application backend.

```bash
docker compose up caddy
```

Then open `http://localhost:8080/`.

- **`/changes/`** — the changes viewer: a map of aggregated H3 cells and a
  day-by-day histogram. Pan/zoom and the date range re-query automatically
  (debounced). Data is fetched from `http://localhost:8080/data/` (`BASE_URL`
  in `app.js`).
- **`/users/`** — the users viewer: look up an OSM username to see their
  OSMPatrol reputation (0-100, built from created nodes/ways/relations and
  capped at the paper's per-aspect weights, with edit-suspicion chips), raw
  indicator totals (relocated, short-lived, rapid-edits, etc.) and an
  edit-activity timeline over the user-indicator files generated by
  `--user-indicators`. The username is matched exactly on
  `user_reputation.parquet` — the pipeline stamps the current username per
  uid, so the reputation, identity fields and indicator totals all come from
  the same uid-row; only the per-day timeline is then read from
  `user_indicators.parquet` (uid-range filter), so a planet-scale index loads
  in the browser without a full download of the indicator table.

## Testing on a small region before the full planet

Never run directly on `planet-latest.osh.pbf` (~150 GB) without first
validating the pipeline on a small extract.

```bash
wget -O data/input/region.osh.pbf \
  https://download.geofabrik.de/europe/malta-updates.osh.pbf
```

Check the exact URL on https://download.geofabrik.de/ — look for files
suffixed `-internal.osh.pbf` or `-updates.osh.pbf` (not every export
includes full history). Then run the full pipeline and sanity-check with
DuckDB:

```bash
docker compose run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output
```

```sql
SELECT DATE '1970-01-01' + change_date AS change_date,
       SUM(node_count) + SUM(way_count) AS total
FROM read_parquet('data/output/changes/year=*/month=*/data.parquet', hive_partitioning = true)
GROUP BY change_date
ORDER BY change_date
LIMIT 20;
```

```
du -h data/node_positions.cache
```

Use the cache size to extrapolate disk needs for a full planet run, then
scale up gradually (a whole country before the planet) to validate
processing time and stability.
