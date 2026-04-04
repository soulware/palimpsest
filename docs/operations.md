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
interval_secs       = 30            # run GC pass every N seconds
```

**Volume discovery:** the coordinator scans `<data_dir>/by_id/` for ULID-named subdirectories. Each discovered fork gets its own `fork_loop` task. The coordinator only starts supervision (spawning a volume process) for writable volumes — readonly volumes (imported bases, pulled snapshots) are discovered for drain and prefetch only.

**Per-fork responsibilities:**

| Trigger | Action |
|---|---|
| Drain tick fires | Compact `pending/` (sweep + repack via volume IPC), then upload to S3 |
| GC tick fires | Repack sparse `segments/` (coordinator GC, fresh ULIDs, handoff protocol) |
| New fork opened (cold-start) | `prefetch-indexes`: download `.idx` files for ancestor segments |

The volume process is a pure data servant: it accepts NBD reads and writes, flushes the WAL to `pending/` on a size threshold or idle tick, applies GC handoffs from the coordinator, and demand-fetches segments from S3 on cache miss. It does not initiate compaction or upload — those are exclusively coordinator responsibilities.

**Directory ownership split.** Two directories under each fork have distinct ownership:

- **`index/`** (coordinator-written): the coordinator writes `index/<ulid>.idx` after confirmed S3 upload, and never reads it back. These files are the permanent LBA index — they survive eviction and allow `Volume::open` to rebuild the full LBA map even when segment bodies are absent locally. The volume and evict never write to `index/`.
- **`cache/`** (volume-managed): the volume writes `.body` and `.present` files here on demand-fetch. The coordinator does not read or write `cache/`.

### S3 Upload

Segments accumulate in `pending/` after WAL promotion (see [formats.md](formats.md) for the promotion commit sequence). The coordinator uploads them to S3 and moves them to `segments/` on success. Each segment is handled independently — a failure on one does not block the others.

The `object_store` crate is used for all store access, providing a uniform interface across local filesystem, S3, GCS, and Azure backends.

**Upload commit sequence per segment:**
1. Read `pending/<ulid>` into memory
2. PUT to object store at key `by_id/<volume_ulid>/YYYYMMDD/<segment_ulid>`
3. On first drain: also PUT `by_id/<volume_ulid>/manifest.toml`, `by_id/<volume_ulid>/volume.pub`, and `names/<volume_name>` (idempotent; same content on every retry)
4. Write `index/<ulid>.idx` — header+index section extracted from `pending/<ulid>` (S3-confirmation marker; written **before** rename)
5. On success: rename `pending/<ulid>` → `segments/<ulid>` (atomic local commit)
6. On failure at any step: leave in `pending/`, record error, continue with remaining segments

**Ordering invariant: `.idx` is written before rename.**  Steps 4 and 5 must happen in this order to ensure every file ever present in `segments/` has a corresponding `index/<ulid>.idx`.  This is the precondition for safe eviction: once a body is evicted from `segments/`, the only path back is `index/<ulid>.idx` → demand-fetch from S3.

The crash recovery argument depends on this order:
- Crash before step 4: file still in `pending/`, drain retries on next start ✓
- Crash between steps 4 and 5: `.idx` written, file still in `pending/`, drain retries rename on next start (PUT and `extract_idx` are idempotent) ✓
- Crash after step 5: both `.idx` and `segments/<ulid>` exist ✓

If `.idx` were written **after** the rename (the previous incorrect ordering), a crash between rename and `.idx` write would leave `segments/<ulid>` stranded without `.idx`.  Drain only scans `pending/` for work, so it would never retry the `.idx` write.  A subsequent `volume evict` would then delete the body, making the data permanently inaccessible even though S3 still holds it.

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

Segments in `segments/` are S3-backed and evictable. When `find_segment_file` is called during a read and the segment file is absent locally, the volume delegates to an optional `SegmentFetcher`. If a fetcher is configured, it downloads the segment from the object store, writes the three-file cache format to `cache/`, and the read proceeds normally. If no fetcher is configured, the read fails with "segment not found".

**Rebuild vs. runtime:** demand-fetch operates at read time, after the volume is open. The LBA map and extent index are rebuilt at `Volume::open` by scanning `pending/`, `segments/`, and `index/*.idx` on local disk. If none of these are present for an ancestor segment, that data will appear as zeros at open time rather than triggering a fetch. This is the **cold-start problem**.

**Cold-start prefetch solves the cold-start problem.** When the coordinator discovers a new fork on a host with no local ancestor segments, it automatically walks the fork's ancestry chain, lists S3 objects for each ancestor fork, and downloads the header+index portion (`[0, body_section_start)`) of each segment not already present locally, writing it as `index/<ulid>.idx` in the ancestor's fork directory. Body bytes are not downloaded. After this, `Volume::open` rebuilds the full LBA map from `index/*.idx`, and individual reads demand-fetch body bytes on first access. This happens automatically as part of fork discovery — no manual invocation required.

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

**Fetch granularity:** demand-fetch issues a range-GET for only the extents needed, not the full segment body. When a specific extent is required, the fetcher scans forward from that entry collecting contiguous, not-yet-present adjacent entries into a batch (up to 256 KiB by default, configurable via `fetch_batch_bytes` in `fetch.toml`). A single range-GET covers the batch; bytes are written into `.body` at the correct offset and the `.present` bitset is updated for all fetched entries. The `.body` file may appear as large as the full segment because it is written as a sparse file at the extent's body offset — only the fetched regions contain actual data.


**Next step: automatic eviction.** Track segment access time (mtime touch on fetch or read), enforce a configurable `max_cache_bytes` in `fetch.toml`, and evict least-recently-used segments from `segments/` and `cache/` (never from `pending/`). Eviction + demand-fetch together make `segments/` a transparent cache tier.

## Manual Eviction

```
elide volume evict <vol>
```

Deletes all evictable segment files from `segments/` to reclaim local disk space. Safe to run on any volume that has S3 backing — evicted segments are demand-fetched on next access.

**Eviction always succeeds.** Segments that cannot safely be evicted are skipped silently; the command reports only the count of files actually deleted. `pending/` files are never touched — they have not yet been uploaded to S3 and are not part of `segments/`.

**No GC protection logic needed.**

Because `segments/<ulid>` is present only after confirmed S3 upload, every file in `segments/` is safe to evict (subject to the `index/<ulid>.idx` check below). In-progress GC handoffs keep their new output body in `gc/` until the coordinator completes the upload and moves it to `segments/` — evict never touches `gc/`.

**Crash safety.**  Every file in `segments/` is structurally guaranteed to be S3-confirmed — the coordinator is the only process that moves bodies there, and does so only after upload and `.idx` write (see Upload commit sequence above).  Evict therefore skips any `segments/<ulid>` body that has no corresponding `index/<ulid>.idx`; such a body cannot have been fully committed.  After a crash+reopen, `Volume::open` rebuilds the LBA map from `index/*.idx`, and subsequent reads fall through to the `SegmentFetcher` for body bytes.

**S3 dependency after eviction.**  Before eviction, `segments/<ulid>` is
redundant with S3 — that is why it is safe to delete.  After eviction,
`index/<ulid>.idx` is the sole local record that those LBAs exist.  If an
`index/` entry is deleted or corrupted, the coordinator will regenerate it
automatically on startup by running prefetch against S3 (triggered whenever
`segments/` is empty and `index/` is empty).  Eviction is therefore only safe
on volumes with S3 backing; running it on a volume without a reachable store
risks permanent LBA map loss.

**Evicting demand-fetched body data.**  `evict` also reclaims space used by
previously demand-fetched body bytes: it deletes `cache/<ulid>.body` and
`cache/<ulid>.present` for any segment whose body is locally cached.  The
`index/<ulid>.idx` entry is preserved, so the LBA map survives; subsequent
reads re-fetch body bytes from S3 on demand.

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

**After the pull:** the coordinator discovers the new volume on the next scan (the empty `segments/` triggers prefetch). It downloads the index section (`.idx`) of every segment from the store into `index/`, then `Volume::open` rebuilds the full LBA map. Subsequent reads demand-fetch body bytes on first access.

The volume is readable (via `volume ls`, `volume serve --readonly`) as soon as the coordinator's prefetch pass completes. No full body download is required upfront.

### Full bootstrap sequence

```
# On a fresh host with access to the same store:
elide volume remote list                    # discover available volumes
elide volume remote pull ubuntu-22.04       # reconstruct skeleton + trigger prefetch
# coordinator prefetches .idx files automatically
elide volume ls ubuntu-22.04                # readable; first access demand-fetches bodies
```

## Disaster recovery

### Disk loss on a live volume

**What is recoverable:** all data that was in segments fully uploaded to S3.

**What is lost:** the contents of `pending/` (WAL segments not yet uploaded) and any writes buffered in the current in-memory WAL.  The recovery point is the last segment the coordinator successfully uploaded.

**Recovery:** `volume remote pull <name>` reconstructs the directory skeleton from S3 — `manifest.toml`, `volume.pub`, an empty `segments/`.  The coordinator then runs prefetch automatically (empty `segments/` and empty `index/` is the trigger) and downloads `.idx` files for every uploaded segment into `index/`.  The volume is readable once prefetch completes.

The recovered volume is permanently readonly: `volume.key` was never uploaded to S3 and cannot be reconstructed.  To continue writing, fork from a snapshot (see below).

### Lost private key

All data in S3 remains readable — signature verification uses `volume.pub`, which is stored in S3.  However, new writes are impossible without `volume.key`.

**If snapshots exist in S3:** fork from the latest snapshot.  The fork creates a new volume with a new key pair; the parent's segments are read through the parent's `volume.pub`.

**If no snapshots exist:** snapshot markers are intentionally unsigned empty files.  Any party with write access to the bucket can create one — this is by design: a volume originally created on one host should be snapshotable and forkable from a different host using a different keypair.  Segment content integrity is enforced by the segment signatures regardless.  To create an emergency branch point: upload an empty file to `by_id/<volume_ulid>/snapshots/<ulid>` using any S3 client, with a ULID equal to the last existing segment ULID (so the branch point covers all uploaded data).  Once the snapshot is visible in S3, `volume fork` can branch from it.  There is no `elide` command for this today — it requires direct S3 manipulation.

### Accidental local deletion with the private key intact

If `segments/`, `index/`, and `cache/` are deleted locally but `volume.key` remains, recovery is fully automatic.  On startup, the coordinator's `fork_loop` detects that `segments/` and `index/` are both empty and runs prefetch against S3, downloading `.idx` files for every uploaded segment into `index/`.  The volume reopens with a complete LBA map and is immediately writable.

**Partially deleting `segments/` is not the same as emptying it.**  The prefetch trigger fires when `segments/` is completely empty *and* `index/` is completely empty.  If some segment files are deleted by hand and others remain (or if `index/*.idx` files still exist), the coordinator sees locally present data and does not run prefetch.  The deleted segments' LBAs are absent from the rebuilt LBA map unless `index/<ulid>.idx` files exist for them.  Reads to those LBAs return zeros with no error — silent data loss.  The only safe ways to remove individual segment bodies are `elide volume evict` (which relies on coordinator-written `index/*.idx`) or ensuring the deleted segment's LBAs are fully covered by newer entries in `pending/` (i.e. they have all been overwritten).

`discover_volumes` skips ULID directories that have neither a `segments/` nor a `pending/` subdirectory — this guards against partially-created volume shells during `volume fork`.  The check is on directory *existence*, not contents, so:

- Delete `segments/` contents only → still discovered (`segments/` dir exists); prefetch triggers (empty dir + empty `index/`).
- Delete `segments/` directory entirely → still discovered via `pending/`; prefetch triggers.
- Delete `pending/` directory entirely → still discovered via `segments/`; prefetch triggers if `segments/` and `index/` are both empty.
- Delete both directories entirely → **not discovered**.  Fix: `mkdir <vol_dir>/segments`.

## Diagnostic tools

Two commands inspect the raw binary file formats written by `elide`. Both are read-only.

**`inspect-segment <path>`** — prints the header and index entries of a segment file or a cached `.idx` file:

```
elide inspect-segment volumes/myvm/forks/default/pending/01JQEXAMPLE...
elide inspect-segment volumes/myvm/forks/vm2/index/01JQEXAMPLE....idx
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

**Key property:** data written then deleted before the drain loop runs is never uploaded to S3. The compaction pass removes it from `pending/` entirely. This is the primary economic argument for compacting before upload — the local I/O cost of compaction is much cheaper than uploading dead bytes to S3 and paying for coordinator GC later.

---

**GC has two distinct scopes with different constraints:**

*Local GC* (space reclamation on disk) operates only on live leaf nodes — those containing `wal/`. Frozen ancestor nodes are structurally immutable and shared by all their descendants; their local segments cannot be touched while any live descendant exists. This matches the lsvd reference implementation's approach: `removeSegmentIfPossible()` refuses to delete a segment referenced by any volume. In the directory model this is structural: absence of `wal/` means no local GC.

To reclaim local space from a frozen ancestor, all its live descendants must first be deleted or re-based. This constraint is intentional: it makes the invariant ("ancestor segments are immutable") enforceable without any reference counting. A practical consequence: a host running many long-lived VMs all forked from the same ancestor will accumulate unreclaimable local disk usage in that ancestor's `segments/` until the VMs are deleted or re-based onto a newer snapshot. S3 repacking is not subject to this constraint and can consolidate or remove ancestor data regardless of live descendants.

Within a live leaf node there is a clean ownership split by directory: `pending/` belongs to the volume; `segments/` belongs to the coordinator.

- Volume GC (`repack`, `sweep_pending`) operates on `pending/` only. Ancestor directories are never scanned or modified.
- Coordinator GC operates on `segments/` — it reads segment index files from both `pending/` and `segments/` to determine liveness; writes and deletes within `segments/` via the handoff protocol (the coordinator is the only process that moves bodies into `segments/`).

This split structurally enforces the `segments/` invariant ("file present ↔ confirmed in S3"): neither the volume nor evict ever write to `segments/`. The coordinator performs the `pending/` → `segments/` rename after upload on the drain path, and the `gc/` → `segments/` move after upload on the GC handoff path. This eliminates any need for eviction to inspect in-flight GC state and eliminates ULID-reuse races between local compaction and S3 upload. See *Open questions* below for details.

### Coordinator-driven segment GC

Segment GC — reclaiming space from already-uploaded `segments/` files — is a coordinator operation. The volume process is not taken offline; it remains available for reads and writes throughout.

**Why the coordinator, not the volume:** segment GC requires S3 mutations (uploading replacement segments, deleting old ones). The volume holds read-only S3 credentials; all S3 writes go through the coordinator.

**Two strategies, run per fork on a configurable interval (default: every 30 seconds):**

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

`segments/<ulid>` present ↔ confirmed in S3. The coordinator is the sole process that moves bodies into `segments/`, and does so only after confirmed S3 upload. For GC handoffs this means the coordinator stages the compacted segment in `gc/`, the volume re-signs it in-place within `gc/`, the coordinator uploads it and writes `index/<new-ulid>.idx`, then moves it to `segments/`. The `segments/` move is the final step — it is the S3-confirmation marker for the body itself. Eviction therefore requires no GC protection checks: every file already in `segments/` is redundant with S3.

1. Coordinator writes `gc/<new-ulid>` (ephemeral-signed segment, via tmp-rename) — only for Repack handoffs; Dead/Remove handoffs skip this step (no new body)
2. Coordinator writes `gc/<new-ulid>.pending` (handoff entries, via tmp-rename); both files (if applicable) are now visible
3. Volume re-signs `gc/<new-ulid>` in-place with `volume.key` (writes to `gc/<new-ulid>.tmp`, then renames over `gc/<new-ulid>`). The body remains in `gc/` — it is not yet moved to `segments/`.
4. Volume applies extent index patches and renames `gc/<new-ulid>.pending` → `gc/<new-ulid>.applied`
5. Coordinator uploads `gc/<new-ulid>` (now volume-signed) to S3
6. Coordinator writes `index/<new-ulid>.idx` — only when a new segment was produced (step 1 ran); this is the S3-confirmation marker
7. Coordinator moves `gc/<new-ulid>` → `segments/<new-ulid>` — the final confirmation step; `segments/` now reflects that the segment is S3-backed
8. Coordinator deletes old S3 objects and local `segments/<old-ulid>` files
9. Coordinator renames `gc/<new-ulid>.applied` → `gc/<new-ulid>.done`

The `segments/<ulid>` invariant — "present ↔ confirmed in S3" — is maintained because the coordinator (not the volume) performs the move into `segments/` only after upload and `index/` write. During the `.applied` window (steps 5–7), the volume-signed body lives in `gc/<new-ulid>`; reads for LBAs covered by this segment fall back to `gc/<new-ulid>` when `segments/<new-ulid>` is absent. The `index/` write at step 6 is omitted for Dead/Remove handoffs (no new segment body). A crash at any step leaves recoverable file state: `gc/<new-ulid>` and/or `.pending`/`.applied` remain present and are re-processed on the next tick.

**Output ULID assignment:** the compacted segment is assigned `max(input ULIDs).increment()` — one step ahead of the newest input in the total ULID order. This gives three properties:

1. **Supersedes inputs in rebuild.** The output ULID is strictly greater than all inputs, so `extentindex::rebuild` uses the compacted segment in preference to the originals during the transition period (before the `.applied` handoff redirects the extent index).

2. **Concurrent writes always win.** Input segments have already passed through the drain/upload pipeline, so their timestamps are seconds to minutes behind the current wall clock. Any write that occurs during compaction gets a ULID from the current time, which is far ahead of `max(inputs)`. Concurrent writes therefore always produce higher ULIDs than the compacted output and win in ULID-ordered rebuild — no locking required.

3. **Worst case is a bounded space leak, not data corruption.** If the liveness check misses a concurrent write (WAL not yet flushed), the compacted output carries that extent unnecessarily. The concurrent write's ULID is higher, so it wins for the LBA in rebuild; the orphaned extent in the compacted output is never read. LBA-dead extents are caught by the LBA-level liveness check (see above) when their source LBA has already been flushed; the remaining gap is only extents whose overwriting write is still in the WAL at the moment the coordinator runs.

The `increment()` overflow case (all 80 random bits set) is effectively unreachable; the fallback is `Ulid::from_parts(max_timestamp + 1ms, 0)`, which preserves the same ordering guarantee.

**At most one outstanding GC pass per fork:** the coordinator defers a new pass if any `gc/*.pending` files exist. This prevents accumulating multiple overlapping handoffs before the volume has applied the first.

**`gc_checkpoint`:** before running a GC pass, the coordinator calls `gc_checkpoint` on the volume via `control.sock`. The volume mints three ULIDs from its monotonic clock — `u_repack`, `u_sweep`, and `u_wal` — flushes the current WAL to `pending/` under the name `u_wal`, and opens a fresh WAL. The coordinator receives `(u_repack, u_sweep)` and uses `u_repack` as the output ULID for any density-repack pass and `u_sweep` for any sweep pass in the same GC tick. Two ULIDs are returned in a single round trip so the coordinator can run both strategies in one tick without a second IPC call.

**Why three ULIDs, minted first — the pre-mint pattern.**

The monotonic mint is a logical clock. Pulling all three identifiers from it *before* any I/O encodes the required ordering constraint in advance:

```
u_repack < u_sweep < u_wal < new_wal_ulid
```

The I/O steps then execute in the pre-determined logical order without requiring any coordination after the fact. This is a general technique: when several operations must be strictly ordered but execute at different times or on different systems, pre-minting their identifiers from a single monotonic source establishes the ordering before anything runs.

Without pre-minting `u_wal`, the WAL segment flushed at checkpoint time carries the WAL's *existing* ULID — assigned when the WAL was opened, before the GC ULIDs were minted. That ULID is lower than `u_sweep`. After the segment is drained from `pending/` to `segments/`, crash-recovery rebuild applies segments in ULID order: the GC output (higher ULID) overwrites the WAL segment (lower ULID) for any shared LBAs, returning stale data.

When the WAL is empty at checkpoint time, the WAL file is deleted and `u_wal` is unused (no segment produced). The fresh WAL opened after minting carries a ULID > `u_sweep`, so subsequent writes are always ordered correctly.

ULIDs are always minted by the volume process, never by the coordinator. This is deliberate: generating them on the coordinator would be unsafe if the coordinator's clock is ahead of the volume's, as the GC output ULID could then exceed a future write's ULID and corrupt rebuild ordering. By sourcing all ULIDs from the volume's clock, coordinator clock skew cannot affect segment ordering.

**GC result file format (`gc/<result-ulid>.pending`):**

The result file describes extent index patches. The LBA map does not need patching — it maps `LBA → hash`, and hashes do not change when data is moved.

```
# plain text, one entry per line; first token is the line type

# repack — extent moved to new segment (formerly "4-field")
repack <hash_hex> <old_segment_ulid> <new_segment_ulid> <new_absolute_body_offset>

# remove — extent-live but LBA-dead; remove from extent index (formerly "2-field")
remove <hash_hex> <old_segment_ulid>

# dead — entire segment is all-dead; no output segment produced
dead <old_segment_ulid>
```

Explicit type prefixes replace the previous implicit word-count format (4-field / 2-field). This makes the format self-documenting, unambiguous to parse, and extensible.

A handoff file may mix `repack` and `remove` lines freely. A file containing only `remove` lines is a *removal-only handoff* — no output segment is produced and no re-signing step is needed. A file containing only `dead` lines is a *tombstone handoff* — see the *Coordinator deletion invariant* section below.

**GC handoff file lifecycle:**

Each handoff file in `gc/` progresses through three states, represented by the typed `GcHandoffState` enum in `elide-core/src/gc.rs`:

| File | State | Meaning |
|------|-------|---------|
| `gc/<ulid>.pending` | `Pending` | Coordinator staged the handoff; volume has not yet applied it |
| `gc/<ulid>.applied` | `Applied` | Volume applied the handoff; coordinator has not yet uploaded or cleaned up |
| `gc/<ulid>.done` | `Done` | Coordinator completed upload and cleanup; retained for 7 days, then pruned |

`GcHandoff::from_filename` parses any `gc/` directory entry into a `(ulid, state)` pair. All code that inspects handoff file state goes through this parser rather than matching raw filename suffixes.

**GC handoff protocol:**

1. Coordinator calls `gc_checkpoint` → flushes WAL, receives `(repack_ulid, sweep_ulid)` minted by the volume
2. Coordinator compacts input segments using the appropriate ULID (`repack_ulid` for density repack, `sweep_ulid` for sweep), stages the compacted segment (if any) in `gc/<result-ulid>` (signed with an ephemeral key), then writes `gc/<result-ulid>.pending` (handoff entries)
3. Volume applies in idle arm:
   - Re-signs `gc/<result-ulid>` with `volume.key`, writes `segments/<result-ulid>`, deletes `gc/<result-ulid>`
   - For each `repack` entry: if extent index still points to `old_segment_ulid` for this hash, update to `new_segment_ulid + new_body_offset`; otherwise skip (hash was rewritten by a newer write — stale coordinator snapshot)
   - For each `remove` entry: if extent index still points to `old_segment_ulid` for this hash, remove the entry (LBA has been overwritten; the reference is dangling)
   - For each `dead` entry: verify no LBA map or extent index entry references `old_segment_ulid`; this is expected and is a no-op from the volume's perspective — its purpose is to obtain the volume's acknowledgment before deletion
   - Removal-only handoffs (only `remove` lines) are applied immediately — no output segment or re-signing needed. Handoffs with `repack` entries complete the re-sign step before applying carried entries. Tombstone handoffs (only `dead` lines) are also applied without a re-sign step
   - Rename `gc/<result-ulid>.pending` → `gc/<result-ulid>.applied`
4. Coordinator (on next poll): sees `.applied`, uploads `segments/<result-ulid>` (volume-signed) to S3, deletes old S3 objects, removes old local `segments/<old-ulid>` files, renames to `gc/<result-ulid>.done`
5. Coordinator (periodic cleanup): deletes `gc/*.done` files older than 7 days

Old local `segments/<old-ulid>` files are left in place until step 4. They remain readable by the volume until then; after deletion reads route to the new segment via the patched extent index (for carried entries) or fail-fast on dangling references that have been cleaned (for removed entries).

This protocol is crash-safe: if either process restarts mid-handoff, the `.pending`/`.applied` state is re-read on next tick and the appropriate step retried. No data is lost.

**Coordinator deletion invariant — no segment is ever deleted without the volume's acknowledgment:**

The coordinator must never directly delete a `segments/` file or its S3 counterpart. All deletions, including segments the coordinator believes to be entirely unreferenced, must go through the handoff protocol (`.pending` → `.applied` → coordinator deletes). This invariant holds even when both `repack` and `remove` sets are empty — i.e. when the coordinator's liveness analysis says a segment has no live extents and no extent index entries at all.

**Why direct deletion is unsafe.** The coordinator rebuilds liveness from on-disk index files. The volume's in-memory LBA map may be ahead of those files: writes that arrived between the last `gc_checkpoint` WAL flush and the end of the GC pass are not yet visible to the coordinator. If the coordinator deletes a segment based on a stale liveness view, the volume may still hold a live reference to it, producing a "segment not found" error on the next read.

**The tombstone handoff.** For segments the coordinator determines to be all-dead (no extent index or LBA map entries visible in on-disk state), a *tombstone handoff* is written: a `.pending` file containing only `dead <old_segment_ulid>` lines, with no associated output segment file. The volume applies the tombstone (verifying from its own perspective that no references exist — a no-op in the normal case), writes `.applied`, and the coordinator proceeds with deletion as in step 4 above.

This design was validated in the TLA+ model (`specs/HandoffProtocol.tla`): `CoordApplyDone` (the only deletion action) requires `handoff = "applied"`, and the `NoSegmentNotFound` and `NoLostData` invariants are checked exhaustively across all crash and concurrent-write interleavings. The all-dead direct deletion path that previously existed in the coordinator was not modelled in the spec — an inconsistency that reflected a real safety violation in the code.

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
