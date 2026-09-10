# osh_change_index

Reads an OSM full-history file (`.osh.pbf`) and produces two partitioned
Parquet datasets — `nodes_changes/` and `ways_changes/` — each containing
`(h3_cell, change_date, count)` rows, laid out as
`year=YYYY/month=MM.parquet` and sorted by `h3_cell` within each file, for
efficient bbox + date-range queries (e.g. with DuckDB).

A single binary, `osh_change_index`, runs three stages by default:

1. Node pass: reads every node, writes a RocksDB cache
   `(node_id, version) -> position`, counts node changes into
   `nodes_changes/year=YYYY/month=MM.parquet`.
2. Way pass: reads every way, resolves the position of each referenced
   node via the RocksDB cache, traces the H3 cells crossed by every
   segment, counts way changes into `ways_changes/year=YYYY/month=MM.parquet`.
   Depends on the cache built by the node pass.
3. Sort pass: rewrites every partition file sorted by `h3_cell`, so that
   Parquet row group min/max statistics become useful for bbox pruning.
   Depends on the files produced by passes 1 and 2.

## Business rules

| Case | Behavior |
|---|---|
| Node with valid coordinates | Written to the RocksDB cache + counted |
| Deleted node with a previously known position | Counted on the last known position, no RocksDB write |
| Node with no coordinates and no previously known position | Skipped |
| Way segment with an unresolved endpoint | Skipped |
| Cells too far apart to trace (`gridPathCells` fails) | Segment skipped |
| Deleted way with a previously known geometry | Counted on the last known geometry |
| Deleted way with no previously known geometry | Skipped |
| Visible way with fewer than 2 nodes | Skipped |
| Relations | Out of scope, ignored |
| Cells crossed by a segment | All counted, no deduplication |
| Time zone | Strict UTC |
| Source file ordering | Assumed sorted by `(id, version)` ascending, confirmed by OSM's documented full-history format — not re-verified by the code |

## Output layout

```
output-dir/
├── manifest.json
├── nodes_changes/
│   ├── year=2005/
│   │   ├── month=01.parquet
│   │   └── ...
│   └── year=2026/
│       └── ...
└── ways_changes/
    ├── year=2005/
    │   └── ...
```

Partitioning by month, rather than by year or by day, is a deliberate
trade-off: fine enough that a small-time-range query only touches one or
two files, coarse enough that the number of partition files stays in the
hundreds (not the thousands), which matters because the source `.osh.pbf`
file is not sorted by date — partitions can all be active at once during
the node/way passes.

`manifest.json` is (re)written at the end of every run, from a directory
scan of the two dataset roots above — not from Parquet file contents. It
exists so a client (e.g. a browser reading these files directly with
hyparquet) doesn't have to guess the H3 resolution used or which months
are actually available:

```json
{
  "h3_resolution": 9,
  "date_range": { "min_month": "2005-01", "max_month": "2026-08" },
  "datasets": {
    "nodes_changes": { "path": "nodes_changes", "partitions": ["2005-01", "2005-02", "..."] },
    "ways_changes":  { "path": "ways_changes",  "partitions": ["2005-01", "..."] }
  }
}
```

## Configuration

Host paths are read from a `.env` file (see `.env.template`) and used by
`docker-compose.yml` to mount `--input`, `--rocksdb` and `--output-dir`
directories into the container.

### Setup

```bash
cp .env.template .env
```

Then edit `.env` if the defaults don't fit:

```
INPUT_DIR=./data/input
ROCKSDB_DIR=./data/rocksdb
OUTPUT_DIR=./data/output
```

These default to `./data/...` under the current directory, so `.env` can be
skipped entirely if that layout works.

## Build

Base image: Debian (`debian:bookworm` for the build stage,
`debian:bookworm-slim` for the runtime stage).

```bash
docker compose build
```

### Known build caveats

The runtime stage does not install Arrow/Parquet/RocksDB via apt package
names (those turned out to be fragile across version bumps). Instead, the
build stage copies the compiled binary together with every shared library
it actually links against, resolved via `ldd`. This means the runtime
image only ever needs whatever the build stage really produced, with
nothing to keep in sync manually.

`doxygen` and `graphviz` are installed in the build stage because H3's
CMake build looks for Doxygen (including its `dot` component, provided by
graphviz) regardless of the `ENABLE_DOCS` option in this setup; this
avoids a configure-time failure.

`src/sort_pass.cpp` sorts each partition manually with `std::sort` instead
of `arrow::compute::SortIndices`/`Take`: on a real run, the Arrow build
resolved via the `apache-arrow-apt-source` package raised `No function
registered with name: sort_indices` — the vector compute kernels are not
registered in that build (likely a trimmed package). Sorting is done by
extracting typed columns, sorting a plain index vector, and rebuilding the
table, which sidesteps the compute function registry entirely.

The Parquet version resolved at build time was confirmed to be 24.0.0. Its
`parquet::arrow::OpenFile` no longer has an out-parameter overload (only
`arrow::Result`-returning), and `FileReader::ReadTable(shared_ptr<Table>*)`
is deprecated in favor of a `Result`-returning overload — `sort_pass.cpp`
was fixed for the former based on the compiler error, and updated for the
latter based on the deprecation message rather than a confirmed successful
build; watch for a similar error there on the next build attempt.

## Usage

```
osh_change_index --input <planet.osh.pbf> --rocksdb <dir> --output-dir <dir> [--resolution N] [--pass 1|2|3|all]
```

| Option | Description |
|---|---|
| `--input` | OSM full-history file (`.osh.pbf`), required |
| `--rocksdb` | RocksDB cache directory (created by pass 1, read by pass 2), required |
| `--output-dir` | Output directory for the Parquet datasets, required (created if missing) |
| `--resolution` | H3 resolution, 0-15 (default: `9`) |
| `--pass` | `1` (nodes only), `2` (ways only, requires an already populated RocksDB cache), `3` (sort only, requires passes 1 and 2 to have already run), or `all` (default) |

## Running

Place the input file under `INPUT_DIR` (default `data/input/`), then:

```bash
docker compose run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --rocksdb /data/rocksdb --output-dir /data/output
```

### Splitting into separate steps

Useful to resume after an earlier stage already completed.

```bash
docker compose run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --rocksdb /data/rocksdb --output-dir /data/output --pass 1

docker compose run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --rocksdb /data/rocksdb --output-dir /data/output --pass 2

docker compose run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --rocksdb /data/rocksdb --output-dir /data/output --pass 3
```

## Serving the output for browser-side use

The `caddy` service in `docker-compose.yml` serves `OUTPUT_DIR` as plain
static files over HTTP, with range-request support and permissive CORS —
what a browser-side Parquet reader (e.g. hyparquet) needs to query the
Parquet partitions and `manifest.json` directly.

```bash
docker compose up caddy
```

Files become available at `http://localhost:8080/`, e.g.
`http://localhost:8080/manifest.json` or
`http://localhost:8080/nodes_changes/year=2024/month=03.parquet`.

## Testing on a small region before the full planet

Never run directly on `planet-latest.osh.pbf` (~150 GB) without first
validating the pipeline on a small extract.

### 1. Get a regional extract with full history

```bash
wget -O data/input/region.osh.pbf \
  https://download.geofabrik.de/europe/malta-updates.osh.pbf
```

Check the exact URL on https://download.geofabrik.de/ — look for files
suffixed `-internal.osh.pbf` or `-updates.osh.pbf` depending on the region
(not every export includes full history).

### 2. Run the full pipeline

```bash
docker compose run --rm osh_change_index \
  osh_change_index --input /data/input/region.osh.pbf --rocksdb /data/rocksdb --output-dir /data/output
```

### 3. Check the output with DuckDB

```sql
SELECT change_date, SUM(count) AS total
FROM read_parquet(
  ['data/output/nodes_changes/*/*.parquet', 'data/output/ways_changes/*/*.parquet'],
  hive_partitioning = true
)
GROUP BY change_date
ORDER BY change_date
LIMIT 20;

SELECT COUNT(*) AS distinct_cell_days, SUM(count) AS total_changes
FROM read_parquet('data/output/nodes_changes/*/*.parquet', hive_partitioning = true);
```

### 4. Check the RocksDB cache size

```bash
du -sh data/rocksdb/
```

Use this as a reference to extrapolate disk space needs for a full planet
run.

### 5. Scale up gradually

Once validated, re-run on a larger extract (a whole country) before the
full planet, to validate processing time and stability.

## Known caveats

### RocksDB cache does not store node deletions

If a node is deleted and a way still references it after that date,
position resolution will still return its last known position before
deletion. This is an accepted approximation of the design — keep it in
mind if validation shows discrepancies.

### Sort pass reads each partition file fully into memory

`sort_pass.cpp` loads a whole partition file into an Arrow table before
sorting it — no external/chunked sort. Monthly partitions are expected to
stay small enough for this in practice, but this has not been measured on
a full planet run. If a given month turns out too large, this would need
revisiting.

### Sort pass runs sequentially

Each partition file is sorted independently and is trivially
parallelizable, but the current implementation processes them one at a
time. Left as a possible follow-up if the sort pass turns out to dominate
total run time.

### Sort pass has a narrow non-atomic window

Each file is rewritten as `<name>.sorted.parquet`, and the original is
only removed once that write succeeds; the file is then renamed to its
final name. If the process is interrupted between removing the original
and the final rename, a stray `<name>.sorted.parquet` can be left next to
a missing original. Not handled automatically — consistent with the rest
of the project, which has no checkpoint/resume support elsewhere either.

### No mid-pass resume

`--pass` lets you rerun a whole stage, not resume partway through one.

### Way pass I/O pattern

Heavy random RocksDB reads during the way pass (one `Seek` per referenced
node): a likely bottleneck on the full planet — measure this on the test
extract before considering parallelization.
