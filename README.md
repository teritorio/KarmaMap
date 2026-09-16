# osh_change_index

Reads an OSM full-history file (`.osh.pbf`) and produces one partitioned
Parquet dataset — `changes/` — with `(h3_cell, change_date, node_count,
way_count)` rows, partitioned by calendar month
(`year=YYYY/month=MM/data.parquet`, standard hive partitioning), for
bbox + date-range queries (e.g. with DuckDB or the included web frontend).
`node_count` counts node changes, `way_count` way changes, in the same file
per month so a client reads one dataset per month. The optional
`--user-indicators` pass adds two non-partitioned user datasets.

The output is queried directly in the browser by two static viewers shipped
in `web/`: a changes map + histogram, and a per-user OSMPatrol reputation
viewer.

## What it produces

- `changes/` — `(h3_cell, change_date, node_count, way_count)` counts,
  partitioned by calendar month.
- `user_indicators.parquet` and `user_reputation.parquet` — per-user, per-day
  activity and reputation, only with `--user-indicators`.

## Documentation

- [HOW_IT_WORKS.md](HOW_IT_WORKS.md) — the internal pipeline: passes,
  business rules, resolution, node cache and the optional user-indicator
  pass, plus how the bundled web viewers query the data.
- [API.md](API.md) — the Parquet data contract (layout, schemas, encodings)
  for reusers.

## Setup

### Requirements

- Docker with Compose.
- An OSM full-history file (`.osh.pbf`).
- Disk space for the node cache and the Parquet output.

### Configuration

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
and `OUTPUT_DIR=./data/` apply, so the examples below work as written.

### Build

Base image: Debian (`debian:bookworm` build stage,
`debian:bookworm-slim` runtime stage).

```bash
docker compose --profile=* build
```

### Tests

The C++ tests use Google Test (Debian `libgtest-dev`) and run through CTest.
They are compiled and executed as part of the Docker build stage, so a
failing test fails the image build. Run them manually with:

```bash
docker build --target build -t osh_change_index:build . \
  && docker run --rm osh_change_index:build bash -c "ctest --test-dir build --output-on-failure"
```

Build without tests by configuring with `-DOSH_ENABLE_TESTS=OFF`.

### Usage

```
osh_change_index --input <planet.osh.pbf> --node-cache <file> --output-dir <dir> [core options]
```

#### Core options

| Option | Description |
|---|---|
| `--input` | OSM full-history file (`.osh.pbf`), required |
| `--node-cache` | Node position cache file (wiped and rebuilt by pass 1, read by pass 2), required |
| `--output-dir` | Output directory for the Parquet datasets, required (created if missing) |
| `--pass` | `1` (nodes only), `2` (ways only, requires an already populated node cache), `3` (merge + sort only, requires passes 1 and 2 to have already run), or `all` (default) |
| `--way-batch-mb` | Way-pass lookup batch budget in MiB (default: `512`) |
| `--h3-resolution` | Resolution of the data cells, 0-13 (default: `9`) |
| `--user-indicators` | Also run the user-indicator pass (see [HOW_IT_WORKS.md](HOW_IT_WORKS.md)); independent of passes 1-3 |

### Running

Place the input file under `DATA_DIR/input` (default `data/`), then:

```bash
docker compose --profile=build run --rm osh_change_index osh_change_index --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/
```

To resume after an earlier stage, run the passes one at a time (a way-only
fix-up after a completed run re-runs `--pass 2` — which puts `ways.parquet`
back next to the already-removed `nodes.parquet` — then `--pass 3`,
sourcing node counts from the existing `data.parquet`):

```bash
docker compose --profile=build run --rm osh_change_index osh_change_index --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/ --pass 1

docker compose --profile=build run --rm osh_change_index osh_change_index --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/ --pass 2

docker compose --profile=build run --rm osh_change_index osh_change_index --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/ --pass 3
```

### Serving the web frontend

The `caddy` service serves everything from a single port `8080`: the web
frontend at the root and `OUTPUT_DIR` (Parquet partitions + `manifest.json`)
under `/data/`, with range requests and permissive CORS on the data path.

```bash
docker compose up
```

Then open `http://localhost:8080/`.

- **`/changes/`** — the changes viewer: a map of aggregated H3 cells and a
  day-by-day histogram. Pan/zoom and the date range re-query automatically
  (debounced).
- **`/users/`** — the users viewer: look up an OSM username to see their
  OSMPatrol reputation (0-100), per-user indicator totals and an edit-activity
  timeline; requires `--user-indicators` (the user datasets).

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

How each viewer queries the data is documented in
[HOW_IT_WORKS.md](HOW_IT_WORKS.md).

## Testing on a small region before the full planet

Never run directly on `planet-latest.osh.pbf` (~150 GB) without first
validating the pipeline on a small extract.

```bash
wget -O data/canary-islands-internal.osh.pbf https://osm-internal.download.geofabrik.de/africa/canary-islands-latest-internal.osm.pbf
```

```bash
docker compose run --rm osh_change_index \
  osh_change_index --input /data/canary-islands-internal.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/
```

Input and Output files size
```bash
124M data/canary-islands-internal.osh.pbf
 23M data/node_positions.cache
  4M data/output
```
