# KarmaMap

KarmaMap reads an OSM full-history file (`.osh.pbf`) and produces one
partitioned Parquet change map - `changes/` - with `(h3_cell, change_date,
count)` rows (node + way changes merged per cell per day), partitioned by
calendar year
(`year=YYYY/data.parquet`, standard hive partitioning), for
bbox + date-range queries (e.g. with DuckDB or the included web frontend).
The `count` of a cell on a day sums the node changes and the way changes, in
the same file
per year so a client reads one dataset per year. The optional
`--user-indicators` pass adds the karma layer: per-user, per-day activity
and a 0-100 reputation per contributor.

The output is queried directly in the browser by two static viewers shipped
in `web/`: a changes map + histogram, and a per-user reputation viewer. The
reputation scoring follows [Neis, Goetz & Zipf, *ISPRS Int. J. Geo-Inf.*
2012, 1(3), 315-332](https://www.mdpi.com/2220-9964/1/3/315).

## What it produces

- `changes/` — `(h3_cell, change_date, count)` counts (node + way changes
  merged per cell per day), partitioned by calendar year.
- `user_indicators.parquet` and `user_reputation.parquet` — per-user, per-day
  activity and reputation, only with `--user-indicators`.

![Changes H3](changes-h3.webp)

![Users](users.webp)

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
`karmamap` service — `--input`, `--node-cache` and `--output-dir`
are absolute paths under it — and `OUTPUT_DIR` is served read-only by the
`caddy` service.

```bash
cp .env.template .env
```

```
DATA_DIR=./data/
```

Without a `.env`, the compose defaults `DATA_DIR=./data/` (mounted at `/data`)

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
docker build --target build -t karmamap:build . \
  && docker run --rm karmamap:build bash -c "ctest --test-dir build --output-on-failure"
```

Build without tests by configuring with `-DOSH_ENABLE_TESTS=OFF`.

### Usage

```
karmamap --input <planet.osh.pbf> --node-cache <file> --output-dir <dir> [core options]
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
| `--change-group-rows` | Target rows per Parquet row group of the changes dataset (`changes/*/year=*/data.parquet`) (default: `10000`); smaller row groups keep `h3_cell`/`change_date` min-max compact so range-pruning clients download only the pages they need |
| `--indicators-group-rows` | Target rows per Parquet row group of `user_indicators.parquet` (default: `10000`) |
| `--reputation-group-rows` | Target rows per Parquet row group of `user_reputation.parquet` (default: `1000`) |
| `--user-indicators` | Also run the user-indicator pass (see [HOW_IT_WORKS.md](HOW_IT_WORKS.md)); independent of passes 1-3 |

### Running

Place the input file under `DATA_DIR` (default `data/`), then:

```bash
docker compose --profile=build run --rm karmamap karmamap --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/
```

To resume after an earlier stage, run the passes one at a time (a way-only
fix-up after a completed run re-runs `--pass 2` — which puts `ways.parquet`
back next to the already-removed `nodes.parquet` — then `--pass 3`,
sourcing node counts from the existing `data.parquet`):

```bash
docker compose --profile=build run --rm karmamap karmamap --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output --pass 1

docker compose --profile=build run --rm karmamap karmamap --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output --pass 2

docker compose --profile=build run --rm karmamap karmamap --input /data/region.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output --pass 3
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

The web viewers resolve their data root relative to the page URL: the
`data/` directory one level above `changes/` and `users/`. Frontend and
data share one base path, and in the bundled compose `caddy` serves the
frontend at `/` and `output-dir/` at `/data`.

How each viewer queries the data is documented in
[HOW_IT_WORKS.md](HOW_IT_WORKS.md).

## Testing on a small region before the full planet

Never run directly on `planet-latest.osh.pbf` (~150 GB) without first
validating the pipeline on a small extract.

```bash
wget -O data/canary-islands-internal.osh.pbf https://osm-internal.download.geofabrik.de/africa/canary-islands-latest-internal.osm.pbf
```

```bash
docker compose run --rm karmamap karmamap --user-indicators --input /data/canary-islands-internal.osh.pbf --node-cache /data/node_positions.cache --output-dir /data/output
```

Input and Output files size
```bash
124M data/canary-islands-internal.osh.pbf
 23M data/node_positions.cache
  4M data/output
```
