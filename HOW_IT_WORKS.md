# How it works

The internal processing pipeline of the `karmamap` binary, plus how
the output it produces is queried by the bundled web frontend. The Parquet
data contract — layout, schemas, encodings — lives in [API.md](API.md).

## The passes

A single binary, `karmamap`, with four pipeline stages — passes 1-3 run
under `karmamap import`, step 4 under `karmamap prepare-update`:

1. Node pass: builds the mmap node cache `(node_id, day) -> position` and
   counts node changes into `changes/year=YYYY/nodes.parquet`.
2. Way pass: resolves each way's node positions via the cache and counts
   way changes at the distinct cells of those positions into
   `changes/year=YYYY/ways.parquet`.
3. Merge pass: merges each year's `nodes.parquet` and `ways.parquet`
   counts per `(h3_cell, change_date)` into the single `count` column of
   `data.parquet`, sorted by
   `(h3_cell, change_date)` so row-group min/max support bbox and
   date-range pruning. Idempotent: an existing `data.parquet` supplies the
   merged total of any count whose staging file is already gone, so
   re-merging never zeroes it.
4. Step 4 (incremental cache): collapses the node cache into a second cache
   holding only the last known h3 cell per node (day dropped), written to
   `<node-cache>.last` by default (see "Incremental cache" below). Step 4 is
   not part of import: run `karmamap prepare-update` for it.

Passes 1-3 and the users-history pass run as part of `karmamap import`
(step 4 lives in `karmamap prepare-update`; see below).

Import records the snapshot's osmosis replication provenance in
`manifest.json` from a `<base>.state.txt` sidecar next to the osh, downloaded
manually with wget on the snapshot's day: upstream `state.txt` is always the
current state and would be too new for an older snapshot. `prepare-update`
records only the update stream URL, keeping the recorded sequence and
timestamp; `update` fetches the live `state.txt` from the update URL.

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
| Relations | Out of scope for the change-counting passes; the users-history pass counts relation created/modified/deleted in a day's activity (relations feed the reputation only via creations) |
| Node cells of a way | Each distinct node cell counted once per way version |
| Time zone | Strict UTC |
| Source file ordering | Assumed sorted by `(id, version)` ascending, as documented for OSM full-history files |

## Resolution

The pipeline uses a single H3 resolution:

- **`--h3-resolution`** (default 9): the resolution of the cells stored in
  the Parquet `h3_cell` column and used across all passes. Range 0-13 (the
  node cache packs at most 13 H3 digits into 6 bytes).

Rows are partitioned by the calendar year of `change_date`, not by H3
cell, so the number of concurrently open Parquet writers is bounded by the
number of years that contain data — independent of extract size or H3
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

## Incremental cache

Step 4 derives a second, history-less cache from the node cache. The node
cache holds every `(node_id, day)` version, so its last record per node_id is
that node's last known position; the incremental cache keeps exactly one
14-byte record per node — `[node_id 8B][h3 cell 6B]`, the day column dropped,
`node_id -> h3_cell`. Same block/directory scheme as the node cache
(a `"INCC"` magic, 2¹⁸ records per ZSTD block, trailing directory of first
key + compressed size), built by one streaming sweep of the node cache fed
into a collapsing writer.

Because the record layout differs from the node cache, blocks are never
reused across runs: the writer appends fresh blocks to `<path>.tmp` and swaps
it over the final path with a rename once the header and directory are
finalized, so a crash leaves either the old cache or only the tmp file,
never a torn cache. The `--node-cache-last` flag sets the output path
(default `<node-cache>.last`); the build is the whole of
`karmamap prepare-update`, import never runs it.

A reader (`node_cache::incremental::Reader`) mmaps the file and looks up a
node's last known cell via the block-directory binary search plus a linear
scan (returning 0 for nodes absent from the cache).

## Update mode

`karmamap update` applies osmosis replication diffs to an existing dataset
whose `manifest.json` recorded an update URL (an `--update-url` passed to
`karmamap import`, or the one recorded by `karmamap prepare-update`, which
never fetches state.txt). The
update stream is that recorded source URL unless `--update-url` is given
explicitly (which must then match), and update runs against the `.last`
incremental cache built by `prepare-update`. The starting
sequence is the recorded source sequence; each
`.osc.gz` diff (URL `AAA/BBB/CCC.osc.gz`, where N = AAA*1000000 + BBB*1000 +
CCC from the 3/3/3 split of the sequence) is downloaded to the diffs dir next
to the node caches — a file
already present is reused, a partial download is removed on failure — and
applied. Once every pass over a diff succeeded its file is removed, so
the diffs dir only ever holds in-flight diffs; diffs committed by
earlier runs are purged at update start. Bare `update` (or `update 0`) fetches
every diff up to the current
`state.txt`; `update N` stops after N.

Each diff invokes update-mode passes 1 and 2:

- **Update node pass**: counts the diff's node changes into
  `changes/year=YYYY/nodes.<seq>.parquet` and folds created/modified
  positions into an in-memory overlay over the flat incremental cache
  (`--node-cache-last`, default `<node-cache>.last`), tracking deletions
  separately.
- **Update way pass**: counts the diff's way changes into
  `changes/year=YYYY/ways.<seq>.parquet`, resolving node refs against the
  overlay — post-update view for visible ways (overlay else base, deleted
  resolves to nothing), pre-update view for deleted ways (overlay else base
  = their last known geometry). Like the import way pass, refs are resolved
  in batches (bounded by `--way-batch-mb`'s default) with one forward-only
  sweep over the base cache per batch instead of a random lookup per ref,
  so a diff's way pass decompresses only the cache blocks its refs touch.

Once per run the `.last` incremental cache is rebuilt as base + overlay minus
deletions (the incremental writer's tmp+rename swap keeps it consistent), and
the staged per-sequence counts are folded into each year's `data.parquet` by
`merge_update_partitions` — base `data.parquet` plus its `nodes.<seq>` /
`ways.<seq>` staging per year, one rename per year. Fetching N diffs never
rewrites the dataset N times.

Apply-once semantics: the merged `data.parquet` footer carries a
`karmamap_source_seq` key stamped with the highest sequence folded in. A
partition whose stamp is already >= the applied sequence drops any orphaned
staging and leaves the merged data untouched, so a crash after the merge
rename (or a rerun of the same diff) is a no-op. Staging carrying a sequence
later than the applied one (a crashed run that fetched further than a capped
rerun applies) is dropped rather than folded. Years holding no staging are
never rewritten.

Each update diff additionally runs the users-history scan
into `users_history_update_stage/seq_<n>/`; one finalize pass folds the
per-`(uid, change_date)` activity deltas and per-uid counter totals into
`users_history.parquet` (in place, sorted) and rebuilds
`user_reputation.parquet` from the combined existing + delta totals. The
manifest's source block is updated to reflect the highest applied sequence
and its timestamp once per run.

The vandalism engine (OSMPatrol filters 2 and 3, see
`docs/osmpatrol-neis-2012.md`) runs in the same loop. Every diff is scanned
into per-`(uid, minute)` modified+deleted buckets under
`vandalism_update_stage/counts/seq_<n>/`, and modified nodes with a known
prior position are recorded by the update node pass into
`vandalism_update_stage/moves/seq_<n>/`. The finalize three-step ordering is
load-bearing:

1. `fold_minute_counts` merges the run's staged buckets into the persisted
   binary store `vandalism_minutes.bin` (`vandalism_store.hpp`), summing
   equal `(uid, minute)` keys — the store is the merge base for the next run
   and is stamped with the applied sequence so a rerun is a no-op.
2. `users_history::run_update_finalize` reads it through
   `vandalism::flagged_days` plus this run's `vandalism::flagged_move_days`
   and recomputes the `vandalism_flag` column of `users_history.parquet`
   (bits 0/1 base flags carried forward, ORed with this run's filter-2 and
   filter-3 bits; bit 2, the reputation-based filter 1, is forward-only —
   set only on the rows this run newly writes, see the users-history pass
   below).
3. `vandalism::flagged_move_days` folds the staged node moves (> 500 m) into
   those same per-day bits (filter 3); its stage is transient and removed, so
   every finalize is idempotent. `vandalism_minutes.bin` likewise folds
   minutes (filter 2). There is no persisted move dataset — both filters land
   only in `users_history.parquet`'s `vandalism_flag` bits.

## Users-history pass

The users-history pass scores history **per user and per UTC
day** with cheap OSMPatrol-style heuristics. It is a single streaming scan
over the node and way history plus relation creations (no changeset metadata
is needed) and one in-memory finalize. OSM full history is
`(id, version)`-sorted, so the scan is a running pass with O(1) object
state, writing day-aggregates to a staged
`users_history_stage/stage_*.parquet` directory that finalize merges, sorts
by `(uid, change_date)`, derives the reputation rows, and removes.
`user_reputation.parquet` is a pure derived view of that data: one row per
user, so it grows with new users, not new edits, and can be rebuilt from the
per-user totals without re-reading history. The non-partitioned single files
keep the `uid` join cheap and the numerics-only history file small.

The `vandalism_flag` of each history row is a bit field. Bits 0 and 1
(`kFlagFilter2`, `kFlagFilter3`) stay 0 on import and are monotonically ORed
by every update finalize from the persisted minute store and the run's
move-flagged days. Bit 2 (`kFlagFilter1`, the paper's "new users or
reputation < 5%" filter) differs: import fills it too, and every finalize
masks it out of the base rows and re-ORs it from the freshly ranked
reputation, so a contributor who climbs above the threshold loses the bit
again. Both finalizes derive it from the same `reputation::Result` written to
`user_reputation.parquet`; a contributor who created nothing ranks 0, which
covers the "new users" half of the screen without a separate rule.

## Vandalism pass

The vandalism engine (OSMPatrol filters 2 and 3 of Neis, Goetz & Zipf 2012)
watches the diff stream, not the full history — it runs `update`-only. The
diff scan (`vandalism::run_scan_diff`) reuses the
users-history classification (visible version 1 = created, later = modified,
invisible = deleted) and counts **modified + deleted** objects per
`(uid, minute)` into `vandalism_update_stage/counts/seq_<n>/` (UTC minutes
since the epoch; creates are ignored).

The minute buckets are persisted as the binary block store
`vandalism_minutes.bin` next to the node caches (see `vandalism_store.hpp` for
the on-disk format: one 16-byte record per `(uid, minute)`, sorted, keyed as
the update finalize's merge base). `fold_minute_counts` reads the store plus
the run's staged buckets, sums equal `(uid, minute)` keys (so a minute that
gains edits in an incoming diff amends its record), rewrites the store with a
tmp+rename swap and stamps its header with the applied sequence. A rerun of
an already-folded sequence (crash between the rename and stage cleanup) is
skipped.

`vandalism::flagged_days` reads the store back into
`(uid, day) -> flag`: each uid's contiguous minute series is run through
`hour_spans`, the trailing 60-minute window (the minute's count plus the
previous 59), and any day holding a minute whose span is strictly above 500
is flagged (`day = minute / 1440`, so a burst crossing midnight still lands
on the day of its peak minute). The users-history update finalize merges
those flags into `users_history.parquet`'s `vandalism_flag` column for the
whole history. Import writes the column as 0: the full-history scan precedes
the replication stream, so no minute buckets exist for it.

Filter 3 records modified-node moves (> 500 m) as the same updates apply:
the update node pass already tracks each node's last known H3 cell
(`node_cache::incremental` base plus this run's overlay), and it hands every
detected move beyond the 500 m screen to `vandalism::NodeMoveSink`, which
stages `(uid, minute)` rows under
`vandalism_update_stage/moves/seq_<n>/` (rows that do not clear the screen
are dropped before staging). `flagged_move_days` folds them straight into
this run's `(uid, day) -> filter-3 bit` flags used by the users-history
finalize and removes the stage root, so there is **no persisted move
dataset** — filter 3 survives only as the carried/ORed day bit in
`users_history.parquet`. The distance is measured from the prior cell
center to the new point, within one res-9 cell radius (~175 m) of the true
prior: fine for the 500 m screen, not for the paper's finer 11 m
edit-analysis flag. Filter 1 (new users / reputation < 5%) is a reputation
bit (`kFlagFilter1`) that the users-history update finalize sets forward-only
on the rows it newly writes, from `user_reputation.parquet`'s ranking,
instead of a diff-based screen; base rows are never re-flagged, so the bit
is monotonic.

## Web viewer queries

### Changes viewer

`web/changes/query.js` queries bbox + date range. The date range selects the
year partitions (intersected with the manifest's partition list); each
distinct file is queried once via `parquetQuery`, which prunes row groups on
`h3_cell` and `change_date`, then aggregates the merged `count`
client-side per cell and per day. The manifest's `date_range` (read from the
data.parquet footer stats) bounds the date pickers and histogram axis to the
exact days that hold data. The non-contiguous H3 cell set of the
viewport bbox is applied as a coarse `[min, max]` range filter first, then
exact membership is checked client-side. `web/changes/app.js` triggers a new
query on pan/zoom (debounced) and resolves its data root relative to the
page URL (`../data`, see README, "Serving the web frontend").

### Users viewer

`web/users/query.js` looks a user up by exact username on
`user_reputation.parquet` (the pipeline stamps the current username per uid,
and the file is username-sorted with a uid tie-break, so the exact filter
prunes straight to the matching pages). The reputation, identity fields and
per-history totals all come from that same user row; the dataset-wide
`active`/`max` aspect stats are read once from the file's
`key_value_metadata` footer instead of repeated per-row columns, and the
per-aspect points are recomputed from the stored `pct` and the constant paper
caps — no ranking or percentile math runs in the browser.

Only the per-day activity timeline is then read from
`users_history.parquet`: a `uid` `[min, max]` range filter prunes the
uid-sorted file to the pages holding that user, with exact membership kept
client-side. Each day's `count` column already totals the six node/way change
counters plus the three relation counters; the per-uid edit total stays
node/way-only (relations are reputation-only).