# Operations

Ongoing system behaviour: S3 upload, garbage collection, repacking, and filesystem metadata awareness.

## WAL Promotion

The WAL is promoted to a `pending/` segment in two ways, both handled by the `VolumeActor`:

**Size threshold (32 MB):** after every write reply is sent, the actor checks `Volume::needs_promote()`. If the WAL has reached 32 MB, `flush_wal()` is called before the next queued message is processed. This is a soft cap — a single write larger than 32 MB will still succeed, producing an oversized segment. Crucially, the write caller receives its reply before the promote runs; the cost is borne by the next queued operation, not the caller that crossed the threshold.

**Idle flush:** the actor run loop selects on a 10-second tick alongside the request channel. When the tick fires and the WAL is non-empty, `flush_wal()` is called. This ensures that a short burst of writes — e.g. writing a few files from a VM — lands in `pending/` without requiring an explicit `snapshot-volume` call. The interval is 10 seconds (conservative for development observability). The idle tick is WAL-flush only — compaction (`sweep_pending`, `repack`) is not triggered here. All compaction is coordinator-driven, as part of the drain loop before upload.

Both triggers call the same `flush_wal()` → `promote()` path, producing an identical segment format. Neither is on the guest-fsync critical path — `NBD_CMD_FLUSH` sends an explicit `Flush` message through the actor channel and blocks until it completes.

## Coordinator Daemon

The coordinator is a long-running daemon that runs on each host and manages all volumes on that host. It is the exclusive owner of all S3 mutations — upload, delete, and GC rewrites. The volume process holds read-only S3 credentials (for demand-fetch only); it never writes to or deletes from S3.

**Coordinator configuration (`coordinator.toml`):** loaded from the current directory by default; location overridable with `--config`. All fields optional — defaults work for local development with `./elide_data` and `./elide_store`.

```toml
data_dir = "elide_data"              # directory containing by_id/ and by_name/; default: ./elide_data

[store]
# local_path = "elide_store"        # local filesystem store (default if no bucket set)
# bucket = "my-elide-bucket"        # S3 bucket
# endpoint = "https://..."          # S3 endpoint override (MinIO, Tigris, etc.)
# region = "us-east-1"              # AWS region (falls back to AWS_DEFAULT_REGION)

[drain]
interval_secs      = 5              # drain pending/ every N seconds
scan_interval_secs = 30             # rescan by_id/ for new volumes every N seconds

[gc]
density_threshold   = 0.70          # compact segments when live_bytes/total_bytes < threshold
small_segment_bytes = 8_388_608     # also compact segments smaller than this (8 MiB)
interval_secs       = 300           # run GC pass every N seconds (5 minutes)
```

**Volume discovery:** the coordinator scans `<data_dir>/by_id/` for ULID-named subdirectories. Each discovered fork gets its own `fork_loop` task. The coordinator only starts supervision (spawning a volume process) for writable volumes — readonly volumes (imported bases, pulled snapshots) are discovered for drain and prefetch only.

**Per-fork responsibilities:**

| Trigger | Action |
|---|---|
| Drain tick fires | Compact `pending/` (sweep + repack via volume IPC), then upload to S3 |
| GC tick fires | Repack sparse `segments/` (coordinator GC, fresh ULIDs, handoff protocol) |
| New fork opened (cold-start) | `prefetch-indexes`: download `.idx` files for ancestor segments |

The volume process is a pure data servant: it accepts NBD reads and writes, flushes the WAL to `pending/` on a size threshold or idle tick, applies GC handoffs from the coordinator, and demand-fetches segments from S3 on cache miss. It does not initiate compaction or upload — those are exclusively coordinator responsibilities.

**`fetched/` is the volume's concern, not the coordinator's.** The volume creates and manages the `fetched/` cache directory — writing triplets on demand-fetch, promoting fully-populated triplets to `segments/`, and evicting LRU entries when the cache exceeds capacity. The coordinator does not read or write `fetched/`.

### S3 Upload

Segments accumulate in `pending/` after WAL promotion (see [formats.md](formats.md) for the promotion commit sequence). The coordinator uploads them to S3 and moves them to `segments/` on success. Each segment is handled independently — a failure on one does not block the others.

The `object_store` crate is used for all store access, providing a uniform interface across local filesystem, S3, GCS, and Azure backends.

**Upload commit sequence per segment:**
1. Read `pending/<ulid>` into memory
2. PUT to object store at key `by_id/<volume_ulid>/YYYYMMDD/<segment_ulid>`
3. On first drain: also PUT `by_id/<volume_ulid>/manifest.toml`, `by_id/<volume_ulid>/volume.pub`, and `names/<volume_name>` (idempotent; same content on every retry)
4. On success: rename `pending/<ulid>` → `segments/<ulid>` (atomic commit)
5. On failure: leave in `pending/`, record error, continue with remaining segments

The rename in step 3 is the local commit point. If the coordinator crashes between steps 2 and 3, the object is in S3 but the segment is still in `pending/` — a retry will re-PUT (idempotent) and then rename. No ledger file is needed.

**Drain loop sequencing:** the coordinator's per-fork drain loop (`fork_loop`) runs the following steps sequentially on each tick:

1. Flush WAL — call `flush` on the volume via `control.sock` to push any pending WAL data to `pending/`
2. Sweep — call `sweep_pending` on the volume via `control.sock`, merging small `pending/` segments
3. Repack — call `repack` on the volume via `control.sock`, compacting sparse `pending/` segments
4. Upload — read each `pending/` file, PUT to S3, rename to `segments/` on success
5. GC — if the GC interval has elapsed, call `gc_checkpoint` to get two output ULIDs, run a GC pass on `segments/`, and apply any completed handoffs (see Coordinator-driven segment GC below)

Steps 1–4 run every tick; step 5 is rate-limited to a configurable `gc_interval` (default 5 minutes). All five steps run sequentially within a single task per fork. Because the coordinator is the only caller of sweep and repack, and upload follows immediately in the same task, there is no concurrent access to `pending/` and no race between compaction and upload.

Steps 1–3 require `control.sock` to be present (volume running). If the socket is absent, those steps are skipped silently and the drain proceeds with upload only. Step 5 (GC) also requires the socket; if absent, the GC tick is skipped and retried next interval.

Segments that are dense at upload time will not need coordinator GC later — the S3 round-trip cost is only paid for extents whose LBAs are overwritten *after* upload.

## Demand-fetch

Segments in `segments/` are S3-backed and evictable. When `find_segment_file` is called during a read and the segment file is absent locally, the volume delegates to an optional `SegmentFetcher`. If a fetcher is configured, it downloads the segment from the object store, writes the three-file fetched format to `fetched/`, and the read proceeds normally. If no fetcher is configured, the read fails with "segment not found".

**Rebuild vs. runtime:** demand-fetch operates at read time, after the volume is open. The LBA map and extent index are rebuilt at `Volume::open` by scanning `pending/`, `segments/`, and `fetched/*.idx` on local disk. If none of these are present for an ancestor segment, that data will appear as zeros at open time rather than triggering a fetch. This is the **cold-start problem**.

**Cold-start prefetch solves the cold-start problem.** When the coordinator discovers a new fork on a host with no local ancestor segments, it automatically walks the fork's ancestry chain, lists S3 objects for each ancestor fork, and downloads the header+index portion (`[0, body_section_start)`) of each segment not already present locally, writing it as `fetched/<ulid>.idx` in the ancestor's fork directory. Body bytes are not downloaded. After this, `Volume::open` rebuilds the full LBA map from `fetched/*.idx`, and individual reads demand-fetch body bytes on first access. This happens automatically as part of fork discovery — no manual invocation required.

**Configuration — `fetch.toml`** in the volume root directory:

```toml
# S3 / S3-compatible (MinIO, Tigris, etc.)
bucket   = "my-elide-bucket"
endpoint = "https://s3.amazonaws.com"   # omit for AWS default
region   = "us-east-1"                  # optional; falls back to AWS_DEFAULT_REGION

# Local filesystem — for testing without a real object store
# local_path = "/tmp/elide-store"
```

If `fetch.toml` is absent, the following sources are tried in order:
1. Env vars: `ELIDE_S3_BUCKET` (required), `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION`
2. `./elide_store` — the coordinator's default local store location (auto-detected if it exists)

If none of these are present, demand-fetch is disabled and reads of missing segment bodies fail with "segment not found".

**Current implementation note:** demand-fetch currently downloads the entire segment body in one GET. The `.present` bitset is written with all bits set (full body present). Partial fetching (range-GETs for individual extents) is a future optimisation.


**Next step: eviction.** Track segment access time (mtime touch on fetch or read), enforce a configurable `max_cache_bytes` in `fetch.toml`, and evict least-recently-used segments from `segments/` and `fetched/` (never from `pending/`). Eviction + demand-fetch together make `segments/` a transparent cache tier.

## Bootstrap from the Store

A volume can be reconstructed on any host that has access to the object store, without copying local data.

### `volume remote list`

```
elide volume remote list
```

Issues a `LIST names/` against the store and prints all named volumes:

```
ubuntu-22.04    01JQAAAAAAAAAAAAAAAAAAAAAA
server-base     01JQBBBBBBBBBBBBBBBBBBBBBB
```

Uses the same store configuration as demand-fetch (`fetch.toml`, env vars, or `./elide_store` fallback).

### `volume remote pull <name>`

```
elide volume remote pull ubuntu-22.04
```

Reconstructs a local volume skeleton from the store:

1. Resolve `names/<name>` → volume ULID
2. Download `by_id/<ulid>/manifest.toml` (size, readonly flag, OCI source)
3. Download `by_id/<ulid>/volume.pub` (Ed25519 public key)
4. Create `<data_dir>/by_id/<ulid>/` with `volume.name`, `volume.size`, `volume.readonly`, `volume.pub`, `manifest.toml`, and an empty `segments/`
5. Create `by_name/<name>` symlink
6. Send a rescan request to the coordinator

**After the pull:** the coordinator discovers the new volume on the next scan (the empty `segments/` triggers prefetch). It downloads the index section (`.idx`) of every segment from the store into `fetched/`, then `Volume::open` rebuilds the full LBA map. Subsequent reads demand-fetch body bytes on first access.

The volume is readable (via `volume ls`, `volume serve --readonly`) as soon as the coordinator's prefetch pass completes. No full body download is required upfront.

### Full bootstrap sequence

```
# On a fresh host with access to the same store:
elide volume remote list                    # discover available volumes
elide volume remote pull ubuntu-22.04       # reconstruct skeleton + trigger prefetch
# coordinator prefetches .idx files automatically
elide volume ls ubuntu-22.04                # readable; first access demand-fetches bodies
```

## Diagnostic tools

Two commands inspect the raw binary file formats written by `elide`. Both are read-only.

**`inspect-segment <path>`** — prints the header and index entries of a segment file or a fetched `.idx` file:

```
elide inspect-segment volumes/myvm/forks/default/pending/01JQEXAMPLE...
elide inspect-segment volumes/myvm/forks/vm2/fetched/01JQEXAMPLE....idx
```

Output includes: file kind (full segment vs index-only), entry counts (data / dedup_ref), a table of data entries sorted by body offset (LBA range, body offset, stored length, compression flag), and total body utilisation. Entries that would read past the end of the body file are flagged `OVERFLOW` — this indicates a segment corruption or a flag-translation bug.

**`inspect-wal <path>`** — prints every record in a WAL file:

```
elide inspect-wal volumes/myvm/forks/default/wal/01JQEXAMPLE...
```

Output includes: record counts (data / dedup_ref), a table of records (type, LBA range, body offset within the WAL file, payload size, compression flag). Truncated tail records (from a crash mid-write) are reported but not an error — `Volume::open` handles them during recovery.

Both commands are useful when debugging read failures: `inspect-segment` surfaces the compressed/uncompressed flag and stored lengths that the read path relies on; `inspect-wal` shows what a WAL contains before it is promoted.

## GC and Repacking

### Background pending compaction

`serve-volume` runs a compaction pass on `pending/` automatically, triggered by the idle flush. After `flush_wal()` succeeds in the idle arm (no write is in flight), the pass runs immediately. Because compaction only runs during the idle window, it never delays a write.

**What the pass does:**

1. Scan all segments in `pending/`
2. Cross-reference against the live LBA map to identify dead extents (LBAs since overwritten)
3. Identify candidates: any segment with at least one dead extent, or any segment below 8 MB
4. Skip if the only candidate is a single small segment with no dead extents — rewriting it produces an identical output (see below)
5. Collect all live extents from every candidate segment
6. Write one or more new `pending/<ulid>` segments containing the merged live extents, split at 32 MB
7. Update the in-memory extent index to point to the new segments
8. Delete the original candidate segments

**Write-path isolation:** neither promotion trigger (32 MB WAL threshold or idle flush) blocks on compaction. After any promotion, a new empty WAL is immediately available for writes. Compaction catches up in the next idle window.

**Snapshot floor:** segments at or below the latest snapshot ULID are frozen and are never touched, even if they are in `pending/`.

**Single-candidate guard:** a single small segment with no dead extents is not a candidate. Rewriting it would produce an output with identical content — a pointless write amplification. The pass only proceeds for a single segment when it has dead extents to reclaim. When two or more small segments are candidates, they are always merged (reducing segment count from N to 1 is worthwhile even if all extents are live).

**Key property:** data written then deleted before `drain-pending` runs is never uploaded to S3. The compaction pass removes it from `pending/` entirely. This is the primary economic argument for compacting before upload — the local I/O cost of compaction is much cheaper than uploading dead bytes to S3 and paying for coordinator GC later.

---

**GC has two distinct scopes with different constraints:**

*Local GC* (space reclamation on disk) operates only on live leaf nodes — those containing `wal/`. Frozen ancestor nodes are structurally immutable and shared by all their descendants; their local segments cannot be touched while any live descendant exists. This matches the lsvd reference implementation's approach: `removeSegmentIfPossible()` refuses to delete a segment referenced by any volume. In the directory model this is structural: absence of `wal/` means no local GC.

To reclaim local space from a frozen ancestor, all its live descendants must first be deleted or re-based. This constraint is intentional: it makes the invariant ("ancestor segments are immutable") enforceable without any reference counting. A practical consequence: a host running many long-lived VMs all forked from the same ancestor will accumulate unreclaimable local disk usage in that ancestor's `segments/` until the VMs are deleted or re-based onto a newer snapshot. S3 repacking is not subject to this constraint and can consolidate or remove ancestor data regardless of live descendants.

Within a live leaf node there is a clean ownership split by directory: `pending/` belongs to the volume; `segments/` belongs to the coordinator.

- Volume GC (`repack`, `sweep_pending`) operates on `pending/` only. Ancestor directories are never scanned or modified.
- Coordinator GC operates on `segments/` — it reads segment index files from both `pending/` and `segments/` to determine liveness, but only writes and deletes within `segments/` (via the handoff protocol).

This split ensures the `segments/` invariant ("file present ↔ confirmed in S3") is never violated by the volume, and eliminates ULID-reuse races between local compaction and S3 upload. See *Open questions* below for details.

### Coordinator-driven segment GC

Segment GC — reclaiming space from already-uploaded `segments/` files — is a coordinator operation. The volume process is not taken offline; it remains available for reads and writes throughout.

**Why the coordinator, not the volume:** segment GC requires S3 mutations (uploading replacement segments, deleting old ones). The volume holds read-only S3 credentials; all S3 writes go through the coordinator.

**Two strategies, run per fork on a configurable interval (default: every 5 minutes):**

*Repack pass* (mirrors lsvd `StartGC` and volume `repack()`):
- Reconstruct the extent index from this fork's `segments/` and `pending/` index files
- Find the single least-dense segment: lowest `live_bytes / file_bytes` ratio
- If its density is below `density_threshold` (default: 0.70), compact it → one output segment
- Return immediately after one segment; next tick handles the next candidate

*Sweep* (mirrors lsvd `SweepSmallSegments` and volume `sweep_pending()`):
- Collect segments below `small_segment_bytes` (default: 8 MiB) that have density ≥ `density_threshold` (lower-density small segments are owned by repack), oldest-first, up to 32 MiB total live bytes
- Skip if fewer than 2 candidates — a single small segment with density ≥ threshold has no meaningful dead space
- Merge all candidates → one output segment

Both passes run in the same tick if both find candidates; they operate on disjoint input sets (repack removes its candidate from the stats before sweep selects). Each produces an independent output segment with its own ULID, obtained via a separate `gc_checkpoint` call (the second WAL flush is a no-op). Per-tick work remains bounded: repack processes one segment; sweep is capped at 32 MiB of live data.

**Liveness:** an extent entry is live only if it passes two checks:

1. **Extent-live:** the reconstructed extent index still points to the input segment for that hash. Entries overwritten by a newer segment in the same fork are extent-dead and excluded.
2. **LBA-live:** the LBA map still maps the original LBA to that hash (`lbamap.hash_at(lba) == Some(hash)`). If the LBA has since been overwritten with different data, the entry is LBA-dead even though the extent index still references it. LBA-dead entries are recorded as *removed entries* in the handoff file so the volume can clean the stale extent index reference when it applies the handoff.

The coordinator rebuilds from on-disk files only — in-memory WAL entries are not visible. This means a WAL entry not yet flushed to `pending/` may cause the coordinator to treat a soon-to-be-dead extent as live. The worst case is a small space leak (the compacted output carries an extent whose LBA will be dead after the WAL flushes). This does not cause data corruption or stale reads — see *Output ULID assignment* below.

**Dedup-ref entries during compaction:** dedup-ref segment entries carry an LBA mapping but no body data. All three compaction paths (volume `repack`, `sweep_pending`, and coordinator GC) carry a dedup-ref entry only if `lbamap.hash_at(start_lba) == Some(hash)`. Unconditionally carrying a dedup-ref is wrong because a stale ref (LBA since overwritten with different data) reintroduces the old LBA mapping into the output segment, corrupting reads after a crash+rebuild. Unconditionally dropping is also wrong because a live ref (LBA still maps to that hash) loses its mapping, causing "segment not found" errors after the input segment is deleted.

**Snapshot floor:** segments at or below the latest snapshot ULID are frozen and skipped. They may be referenced by child forks.

**Output placement and the `segments/` invariant:**

`segments/` files are always signed with `volume.key`. The coordinator does not hold the volume's private key, so it cannot write directly to `segments/`. Instead it stages the compacted segment in `gc/` using an ephemeral key, and the volume re-signs it before moving it into `segments/`:

1. Coordinator writes `gc/<new-ulid>` (ephemeral-signed segment, via tmp-rename)
2. Coordinator writes `gc/<new-ulid>.pending` (handoff entries, via tmp-rename); both files are now visible
3. Volume re-signs `gc/<new-ulid>` with `volume.key`, writes `segments/<new-ulid>`, deletes `gc/<new-ulid>`
4. Volume applies extent index patches and renames `gc/<new-ulid>.pending` → `gc/<new-ulid>.applied`
5. Coordinator uploads `segments/<new-ulid>` (volume-signed) to S3, deletes old files, renames to `.done`

Only after step 3 is the file visible in `segments/`, and it is always volume-signed. A crash at any step leaves recoverable file state: `gc/<new-ulid>` and/or `.pending` remain present and are re-processed on the next tick.

**Output ULID assignment:** the compacted segment is assigned `max(input ULIDs).increment()` — one step ahead of the newest input in the total ULID order. This gives three properties:

1. **Supersedes inputs in rebuild.** The output ULID is strictly greater than all inputs, so `extentindex::rebuild` uses the compacted segment in preference to the originals during the transition period (before the `.applied` handoff redirects the extent index).

2. **Concurrent writes always win.** Input segments have already passed through the drain/upload pipeline, so their timestamps are seconds to minutes behind the current wall clock. Any write that occurs during compaction gets a ULID from the current time, which is far ahead of `max(inputs)`. Concurrent writes therefore always produce higher ULIDs than the compacted output and win in ULID-ordered rebuild — no locking required.

3. **Worst case is a bounded space leak, not data corruption.** If the liveness check misses a concurrent write (WAL not yet flushed), the compacted output carries that extent unnecessarily. The concurrent write's ULID is higher, so it wins for the LBA in rebuild; the orphaned extent in the compacted output is never read. LBA-dead extents are caught by the LBA-level liveness check (see above) when their source LBA has already been flushed; the remaining gap is only extents whose overwriting write is still in the WAL at the moment the coordinator runs.

The `increment()` overflow case (all 80 random bits set) is effectively unreachable; the fallback is `Ulid::from_parts(max_timestamp + 1ms, 0)`, which preserves the same ordering guarantee.

**At most one outstanding GC pass per fork:** the coordinator defers a new pass if any `gc/*.pending` files exist. This prevents accumulating multiple overlapping handoffs before the volume has applied the first.

**`gc_checkpoint`:** before running a GC pass, the coordinator calls `gc_checkpoint` on the volume via `control.sock`. This flushes the volume's WAL (ensuring all in-flight writes are in `pending/` where the coordinator can see them), then mints two ULIDs 2ms apart using the volume's own clock, and returns them as `(repack_ulid, sweep_ulid)`. The coordinator uses `repack_ulid` as the output ULID for any density-repack pass and `sweep_ulid` for any sweep pass in the same GC tick. Two ULIDs are returned in a single round trip so the coordinator can run both strategies in one tick without a second IPC call. The 2ms gap ensures they are distinct even on systems where `Ulid::new()` has millisecond resolution.

Because the WAL was flushed before minting, no in-flight WAL segment can have a ULID above either returned value — both GC outputs are guaranteed to sort before any subsequent write. This closes the rebuild-time ordering race described in the design notes.

ULIDs are always minted by the volume process, never by the coordinator. This is deliberate: generating them on the coordinator would be unsafe if the coordinator's clock is ahead of the volume's, as the GC output ULID could then exceed a future write's ULID and corrupt rebuild ordering. By sourcing both ULIDs from the volume's clock, clock skew between hosts is not a concern.

**GC result file format (`gc/<result-ulid>.pending`):**

The result file describes extent index patches. The LBA map does not need patching — it maps `LBA → hash`, and hashes do not change when data is moved.

```
# plain text, one entry per line

# 4-field: carried entry — extent moved to new segment
<hash_hex> <old_segment_ulid> <new_segment_ulid> <new_absolute_body_offset>

# 2-field: removed entry — extent-live but LBA-dead; remove from extent index
<hash_hex> <old_segment_ulid>
```

A handoff file containing only 2-field lines is a *removal-only handoff*. It has no associated output segment (nothing was carried). A handoff file may also have a mix of both line types.

**GC handoff protocol:**

1. Coordinator calls `gc_checkpoint` → flushes WAL, receives `(repack_ulid, sweep_ulid)` minted by the volume
2. Coordinator compacts input segments using the appropriate ULID (`repack_ulid` for density repack, `sweep_ulid` for sweep), stages the compacted segment (if any) in `gc/<result-ulid>` (signed with an ephemeral key), then writes `gc/<result-ulid>.pending` (handoff entries)
3. Volume applies in idle arm:
   - Re-signs `gc/<result-ulid>` with `volume.key`, writes `segments/<result-ulid>`, deletes `gc/<result-ulid>`
   - For each 4-field entry: if extent index still points to `old_segment_ulid` for this hash, update to `new_segment_ulid + new_body_offset`; otherwise skip (hash was rewritten by a newer write — stale coordinator snapshot)
   - For each 2-field entry: if extent index still points to `old_segment_ulid` for this hash, remove the entry (LBA has been overwritten; the reference is dangling)
   - Removal-only handoffs (all 2-field lines) are applied immediately — no output segment or re-signing needed. Handoffs with 4-field entries complete the re-sign step before applying carried entries
   - Rename `gc/<result-ulid>.pending` → `gc/<result-ulid>.applied`
4. Coordinator (on next poll): sees `.applied`, uploads `segments/<result-ulid>` (volume-signed) to S3, deletes old S3 objects, removes old local `segments/<old-ulid>` files, renames to `gc/<result-ulid>.done`
5. Coordinator (periodic cleanup): deletes `gc/*.done` files older than 7 days

Old local `segments/<old-ulid>` files are left in place until step 4. They remain readable by the volume until then; after deletion reads route to the new segment via the patched extent index (for carried entries) or fail-fast on dangling references that have been cleaned (for removed entries).

This protocol is crash-safe: if either process restarts mid-handoff, the `.pending`/`.applied` state is re-read on next tick and the appropriate step retried. No data is lost.

**`.done` file accumulation:** at the default 5-minute GC interval, a fork accumulates ~288 `.done` files per day. Each file is small (one line per moved or removed extent, ~100–130 bytes each), but directory inode count grows unboundedly without cleanup. The coordinator runs a TTL cleanup pass each tick, deleting `.done` files whose mtime is older than 7 days. This retains a recent window useful for post-mortem debugging (which segments were compacted, when) without unbounded growth. Deletion is safe because by the time a handoff reaches `.done` the old input segments are already removed; `sort_for_rebuild` only classifies `.pending` and `.applied` sidecars as in-flight GC outputs.

*S3 repacking* (locality optimisation in object storage) is a coordinator-level operation and is **not subject to the leaf-only constraint**. The coordinator can read extents from any node's local segments or from S3, create new S3 objects with better layout, and update the extent index to point to them. Local files are caches — the coordinator does not modify them to repack at the S3 level.

The unit of an S3 read is a byte-range GET covering a contiguous chunk of a segment body. The goal of repacking is to minimise the number of such GETs needed to serve a given access pattern. By co-locating extents that are accessed together (e.g. during VM boot) into a contiguous region of the same S3 object, all of them can be retrieved in a single byte-range GET — or at most a small number of adjacent chunk fetches. Extents scattered across many segments, or far apart within a segment, require a separate GET per chunk.

**Standard local GC within a live node:** walk the live node's LBA map, identify extents no longer referenced by any LBA range (overwritten or deleted), remove them from local segments after a grace period. Compact sparse segments by merging live extents into fresh, denser segments and updating the extent index.

**Delta dependency handling:** when a source extent is about to be removed and a live delta in S3 depends on it, materialise the delta first (fetch source + delta → full extent, write full extent to S3, update extent index). Then remove the source. The dependency map is derived fresh each GC sweep from the extent index — no persistent reverse index needed.

**Access-pattern-driven repacking:** the coordinator observes which extents are accessed during VM boot and co-locates them contiguously in a dedicated S3 segment. A cold VM boot then requires a small number of byte-range GETs — ideally one — to satisfy the entire boot sequence, rather than one GET per chunk scattered across many segments. Because the extent index covers the full snapshot tree, the repacker sees all boot extents in one place regardless of which ancestor node's segment originally held them.

**Boot hint accumulation:** every VM boot records which extent hashes were accessed during the boot phase (identified by time window after volume attach, or explicit VM lifecycle signals from the hypervisor). Observations accumulate per snapshot node. With the snapshot tree, multiple VMs forked from the same ancestor all boot the same base extents — their observations aggregate for the same image, accelerating convergence. After sufficient boots (converges quickly at scale — 500 VMs/day = 500 observations/day), the hint set is stable enough to drive repacking.

**Continuous improvement:** first boot is cold; boot access patterns are recorded; next repack co-locates those extents in S3; subsequent boots are faster. The feedback loop strengthens with scale.

**Snapshot-aligned repacking:** the snapshot tree makes the two-tier S3 layout structurally explicit rather than heuristically inferred. Extents that live in ancestor nodes are by definition shared across all descendants — they are natural candidates for base segments. Extents in recent leaf nodes are unique to that branch — they belong in snapshot-specific segments. Directory depth is a direct proxy for sharing depth; no similarity analysis needed.

```
s3://bucket/segments/base-<hash>     — extents from ancestor nodes; shared across all descendants
s3://bucket/segments/snap-<id>-N     — extents unique to a specific snapshot node
```

Shared extents (e.g. the ~84% identical between Ubuntu 22.04 point releases) are consolidated into base segments. A new host serving any snapshot from the same family fetches these once. Snapshot-specific segments contain only the changed extents, stored as deltas against their counterparts in the base segments. When a repacked base segment supersedes extents previously stored in per-node S3 objects, the extent index is updated to point to the repacked location; the per-node objects can then be removed once no extent index entry references them.

**Ext4 re-alignment during GC:** GC is a natural point to perform or improve extent re-alignment, not just snapshot time.

---

- **Snapshot nodes:** safe and clean — a snapshot is frozen, the filesystem state is fixed. GC can parse ext4 metadata and re-align any snapshot that was not aligned at creation time.
- **Live nodes:** the filesystem is in flux. GC uses the most recent frozen ancestor's ext4 metadata as a proxy. Re-alignment is approximate but safe — dedup quality improves progressively without risk of data corruption.

## Filesystem Metadata Awareness

Since the system controls the underlying block device, it sees every write — including writes to ext4 metadata structures (superblock, group descriptors, inode tables, extent trees, journal). This visibility is an opportunity to handle metadata blocks smarter than opaque data blocks.

**Metadata extent tagging:** once metadata LBAs are identified from the superblock (all at well-defined offsets), those extents can be tagged in the LBA map. Tagged metadata extents receive special treatment:
- Skip dedup — inode tables and group descriptors are volume-specific (unique inode numbers, volume-specific block addresses) and will never match across snapshots
- Cache aggressively — metadata blocks are hot; every filesystem operation reads them

**Incremental shadow filesystem view:** because every write to a known metadata LBA is visible, the system could maintain a continuously-updated internal view of the filesystem layout — which LBA ranges belong to which files, as files are created, deleted, and modified. At snapshot time, the shadow view is already current: no parse-from-scratch, re-alignment is essentially free.

**The journal problem:** ext4 metadata does not go directly to its final LBA. It is written to the jbd2 journal first (write → journal commit → checkpoint to final location). There are three levels of journal handling with very different complexity profiles:

- **Level 1 — detect that metadata changed (trivial):** writes to journal LBAs are visible at the block device level. We know a transaction is in flight but not what changed. Useful only for invalidating the shadow view ("metadata changed, re-parse at next opportunity"). No journal parsing required.

- **Level 2 — parse committed transactions at snapshot/GC time (moderate):** at a known-clean point (snapshot or GC checkpoint), read the journal, walk committed transactions, and replay them to recover current metadata state. This is what `e2fsck` does. The jbd2 format is well-documented with reference implementations in `e2fsprogs` and the kernel. A few hundred lines of careful Rust. Risk is low — we are parsing a frozen, consistent state. This is sufficient for snapshot and GC re-alignment.

- **Level 3 — live transaction tracking (high):** intercept journal writes in real time, parse each transaction as it commits, and update the shadow view incrementally. Requires recognising journal LBAs in the write stream, parsing jbd2 descriptor blocks to correlate data blocks with their final destinations, and correctly handling the circular log structure, transaction abort/rollback, and journal checkpointing. Getting this wrong silently produces an incorrect shadow view. The kernel's jbd2 module is the authoritative reference and is non-trivial.

One simplifying factor across all levels: ext4's default journaling mode is `data=ordered` — only metadata goes through the journal; data blocks are written directly to their final locations. Journal handling is therefore scoped to metadata only, not the full write stream.

**Recommended approach:** implement Level 2 first — sufficient for snapshot/GC re-alignment and well-understood. Level 3 (live shadow view) is only needed for real-time file-identity-aware dedup decisions and should be deferred until Level 2 is working well.

**Future potential:** a live shadow filesystem view would enable real-time dedup decisions informed by file identity — knowing that a write is to a known shared library vs. a per-VM log file, for example, without waiting for snapshot time. This is a significant capability that falls out naturally from controlling the block device, and is worth designing toward even if not implemented immediately.
