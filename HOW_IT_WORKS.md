# How it works

The internal processing pipeline of the `osh_change_index` binary. The
external data contract — output layout, schemas, and how clients access the
data — lives in [API.md](API.md); this file documents how the binary produces
it.

## The passes

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

Passes 1-3 are independent of the optional user-indicator pass (see below).

### Incremental runs

Pass 1 wipes `changes/` (so a re-run never leaves stale partitions) and the
node cache file before rebuilding; pass 2 does not wipe the root — it adds
`ways.parquet` staging files next to `nodes.parquet`, so any pass can run
alone. The manifest is rebuilt at the end of every run.

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
aborts loudly), so the way pass's sweep is safe. A node edited several times
in one day is stored once (last position wins).
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
into a 4 MiB cache. The per-block first keys seed the way pass's sweep cursor
between batches (a binary search over the directory, then one forward-only
scan); no RAM sample index is kept.

## User-indicator pass

The optional `--user-indicators` pass scores history **per user and per UTC
day** with cheap OSMPatrol-style heuristics. It is a single streaming scan
over the node and way history plus relation creations (no changeset metadata
is needed) and one in-memory finalize. OSM full history is
`(id, version)`-sorted, so the scan is a running pass with O(1) object
state, writing day-aggregates to a staged
`user_indicator_stage/stage_*.parquet` directory that finalize merges, sorts
by `(uid, change_date)`, derives the reputation rows, and removes.
`user_reputation.parquet` is a pure derived view of that data: one row per
user, so it grows with new users, not new edits, and can be rebuilt from the
per-user totals without re-reading history. The non-partitioned single files
keep the `uid` join cheap and the numerics-only indicators file small.
The output schemas and access patterns are documented in [API.md](API.md).