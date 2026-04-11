# Operations

Ongoing system behaviour: WAL promotion, S3 upload, garbage collection, repacking, and filesystem metadata awareness.

## WAL promotion

The WAL is promoted to a `pending/` segment in two ways, both handled by `VolumeActor`:

- **Size threshold (32 MB):** after every write reply, the actor checks `Volume::needs_promote()` and calls `flush_wal()` before the next queued message. The write caller already has its reply — the cost is borne by the next operation. Oversized writes succeed (soft cap).
- **Idle flush (10s tick):** a tick runs alongside the request channel; when it fires with a non-empty WAL, `flush_wal()` is called.

Both paths produce identical segments. `NBD_CMD_FLUSH` sends an explicit `Flush` message through the actor channel and blocks until it completes — neither promotion path is on the guest-fsync critical path. The idle tick is WAL-flush only; all compaction is coordinator-driven.

## Coordinator daemon

The coordinator is a long-running per-host daemon that owns all S3 mutations (upload, delete, GC rewrites). The volume holds read-only S3 credentials for demand-fetch and never writes to S3.

**Configuration (`coordinator.toml`):** loaded from the current directory; overridable with `--config`. All fields optional.

```toml
data_dir = "elide_data"              # by_id/ and by_name/

[store]
# local_path = "elide_store"         # default if no bucket set
# bucket = "my-elide-bucket"
# endpoint = "https://..."           # MinIO, Tigris, etc.
# region = "us-east-1"

[drain]
interval_secs      = 5
scan_interval_secs = 30

[gc]
density_threshold   = 0.70           # compact segments below this density
small_segment_bytes = 8_388_608      # also compact segments smaller than this
interval_secs       = 30
```

The coordinator scans `<data_dir>/by_id/` for ULID subdirectories and spawns a `fork_loop` task per fork; supervision runs for writable volumes only (readonly volumes are discovered for drain and prefetch).

**Directory ownership within a fork.** `index/<ulid>.idx` is the S3-confirmation marker — written by the volume at promote time *after* the coordinator confirms S3 upload via IPC, and the sole survivor of body eviction. `cache/<ulid>.body` + `cache/<ulid>.present` are the evictable local bodies, written on promote or demand-fetch. The coordinator never writes to `cache/` — bodies always route through the volume's promote IPC, keeping the signing key on the volume host.

## CLI operations and the IPC boundary

Any CLI operation that mutates a live volume's directory must go through coordinator or volume IPC; direct filesystem mutation would race with the in-memory state owned by the volume and coordinator.

| Operation | Route | Status |
|---|---|---|
| `volume create` / `fork` / `remote pull` | Direct fs (+ optional IPC) | Safe — creates a new volume the coordinator picks up on rescan |
| `volume update` | Direct fs + IPC restart | Safe (config only read at `Volume::open`) |
| `volume snapshot` | IPC (primary) / offline fallback | Offline fallback races with coordinator drain — proposed: always route through coordinator |
| `volume evict` | Direct fs | **Unsafe** — proposed: route through coordinator IPC; concurrent GC can read a body between `collect_stats` and `compact_segments` |
| `volume import`/`delete`/`up`/`down`/`status` | Coordinator IPC | Safe |
| `volume info`/`ls` | Direct fs (read-only) | Safe |

## S3 upload

Segments accumulate in `pending/` after WAL promotion. The coordinator's per-fork drain loop runs these steps sequentially on each tick:

1. **Flush WAL** via `flush` IPC
2. **Sweep** via `sweep_pending` IPC (merges small `pending/` segments)
3. **Repack** via `repack` IPC (compacts sparse `pending/` segments)
4. **Upload** — PUT each `pending/` file, send `promote <ulid>` IPC on success; the volume writes `index/<ulid>.idx` + `cache/<ulid>.body` + `cache/<ulid>.present` and removes `pending/<ulid>`.
5. **GC** — rate-limited to `gc_interval` (default 10s); see below.

All store access uses the `object_store` crate (uniform local / S3 / GCS / Azure). On first drain the coordinator also uploads `manifest.toml`, `volume.pub`, and `names/<name>`. The volume's promote handler writes `index/` before the body files, and body files before removing `pending/` — a mid-promote crash leaves idempotent retry state.

Steps 1–3 and 5 require `control.sock` (volume running). If absent, those steps are skipped and upload proceeds alone. The drain loop skips a fork entirely when `import.lock` is present and `control.sock` is absent — the import process is still writing segments.

Segments dense at upload time will never need coordinator GC — the S3 round-trip cost is only paid for extents overwritten *after* upload, which is the primary argument for compacting `pending/` before upload.

## Demand-fetch

S3-uploaded segment bodies live in `cache/<ulid>.body`. On a read with the body absent (or the entry's present-bit clear), the volume delegates to an optional `SegmentFetcher` which range-GETs the needed extents into `cache/`. Without a configured fetcher the read fails with "segment not found".

A single range-GET covers a batch of contiguous, not-yet-present entries (default 256 KiB, `fetch_batch_bytes`). Bytes are written into the sparse `.body` file at the correct offset; the `.present` bitset is updated.

**Cold-start prefetch.** `Volume::open` rebuilds the LBA map from *on-disk* `pending/` and `index/*.idx` — if an ancestor's `.idx` is absent those LBAs read as zeros rather than triggering a fetch. When the coordinator discovers a fork with no local ancestor segments, it walks the ancestry, lists S3 objects, and downloads each missing segment's header+index section as `index/<ulid>.idx`. Body bytes are not downloaded — individual reads demand-fetch them.

**Configuration — `fetch.toml`** in the volume root. If absent, fallbacks are env vars (`ELIDE_S3_BUCKET`, `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION`) then `./elide_store`.

```toml
bucket   = "my-elide-bucket"
endpoint = "https://s3.amazonaws.com"
region   = "us-east-1"
# local_path = "/tmp/elide-store"   # for testing without a real store
```

**Automatic eviction (future).** Track mtime on fetch/read, enforce `max_cache_bytes`, evict LRU `cache/<ulid>.body` files. Must run inside the coordinator for the same sequencing reason as manual eviction.

## Manual eviction

```
elide volume evict <vol>
```

Deletes `cache/<ulid>.body` and `cache/<ulid>.present` to reclaim local disk; evicted bodies are demand-fetched on next access. Evictable only if `index/<ulid>.idx` exists (S3 confirmation). `pending/` and `gc/` are never touched; unsafe segments are skipped silently.

Eviction is only safe on volumes with reachable S3 backing. The current CLI implementation is **unsafe** — it deletes files directly, racing with coordinator GC's `collect_stats` → `compact_segments`. Fix: route through a coordinator IPC that sequences deletion between ticks.

## Bootstrap from the store

```
elide volume remote list                  # LIST names/
elide volume remote pull <name>           # create skeleton + rescan
elide volume ls <name>                    # readable once prefetch completes
```

`remote pull` resolves `names/<name>` → volume ULID, downloads `manifest.toml` and `volume.pub`, creates `<data_dir>/by_id/<ulid>/` with an empty `index/`, creates the `by_name/` symlink, and sends a rescan IPC. The coordinator's prefetch trigger (empty `index/` + no `pending/`) fires, downloading every segment's `.idx`; subsequent reads demand-fetch bodies.

## Post-import workflows

An imported volume is readonly. Two ways to handle writes (not mutually exclusive):

- **External write layer** — serve `--readonly`, let the hypervisor or guest overlay writes. Writes are opaque to Elide. Good for ephemeral.
- **Elide-managed writable fork** — `volume fork <base> <child>`; writes go through the content-addressed store. Good for long-lived VMs.

## Disaster recovery

- **Disk loss on a live volume.** Recoverable: everything in fully-uploaded segments. Lost: `pending/` and the in-memory WAL tail. Recover with `volume remote pull <name>`. The recovered volume is permanently readonly (`volume.key` was never uploaded); fork from a snapshot to continue writing.
- **Lost private key.** S3 data remains readable (signatures verified with `volume.pub` in S3), but new writes are impossible. If snapshots exist, fork from the latest. Snapshot markers are intentionally unsigned empty files, so an emergency branch point can be created by uploading an empty file to `by_id/<vol>/snapshots/<ulid>` with a ULID ≥ the latest segment.
- **Local `cache/` deletion.** Automatic recovery — `Volume::open` rebuilds from `index/*.idx` and reads demand-fetch bodies.
- **Local `cache/` + `index/` deletion.** Prefetch re-runs on startup (empty `index/` + empty `pending/`) and the volume reopens writable.
- **Partial `index/` deletion is silent data loss.** The prefetch trigger requires `index/` to be *completely* empty; a partial delete leaves the coordinator seeing locally-present data. Missing segments' LBAs read as zeros. The only safe way to remove individual body files is `elide volume evict`, which preserves `.idx`.

`discover_volumes` skips ULID directories with neither `pending/` nor `index/`. Deleting both entirely hides the volume — fix with `mkdir <vol_dir>/index`.

## Diagnostic tools

Two read-only commands inspect raw on-disk formats:

- **`inspect-segment <path>`** — header, entry counts, data entries (LBA range, body offset, stored length, compression flag), body utilisation. Entries reading past body EOF are flagged `OVERFLOW`.
- **`inspect-wal <path>`** — every WAL record (type, LBA range, body offset, payload size, compression flag). Truncated tails from crashes are reported, not errored — `Volume::open` handles them on recovery.

## GC and repacking

### Pending compaction (volume, automatic)

The `serve-volume` actor runs a compaction pass on `pending/` in its idle arm after `flush_wal()` — never delaying a write. Candidates are segments with any dead extents, or any segment below 8 MB; the pass merges their live extents into new `pending/` segments split at 32 MB and deletes the originals. A single small segment with no dead extents is skipped (rewriting would produce identical content).

Snapshot-floor segments (at or below the latest snapshot ULID) are frozen and never touched. Data written then deleted before the drain runs never hits S3 — compacting locally is much cheaper than uploading dead bytes and paying coordinator GC later.

### Scope: live leaves only

Local GC operates only on live leaf nodes (those with `wal/`). Frozen ancestors are structurally immutable and shared across descendants; their local segments cannot be touched while any descendant exists. A host running many long-lived forks from a common ancestor accumulates unreclaimable disk in that ancestor until the forks are deleted or re-based. S3 repacking is not subject to this constraint.

### Coordinator-driven segment GC

Segment GC reclaims space from already-uploaded segments while the volume stays online. The coordinator owns this because it requires S3 mutations.

Each GC tick runs three passes per fork:

- **Dead pre-pass** — segments with no live entries are tombstoned in a `dead`-only handoff. No fetch, no write, just DELETE. If it fires, repack is skipped this tick (shares `repack_ulid`).
- **Repack** — finds the single least-dense segment; if its density is below `density_threshold` (default 0.70) it is compacted to one output.
- **Sweep** — merges segments below `small_segment_bytes` (default 8 MiB) with density ≥ threshold, oldest-first, up to 32 MiB total live bytes. Skipped with fewer than 2 candidates.

Repack and sweep run on disjoint inputs. Per-tick work is bounded: dead pre-pass is O(1) per segment, repack processes one, sweep caps at 32 MiB.

**Liveness** — an entry is live only if both (a) the reconstructed extent index still points to this input segment for that hash, and (b) `lbamap.hash_at(lba) == Some(hash)`. LBA-dead entries are recorded as `remove` lines in the handoff. The coordinator rebuilds liveness from on-disk files only — in-memory WAL entries are not visible. Worst case is a small space leak (an extent carried into the output that will be dead once the WAL flushes), never corruption. Dedup-ref entries are carried only if the start LBA still maps to the hash — unconditional carry or drop both corrupt data (see `docs/testing.md` bug E). Snapshot-floor segments are skipped.

### `gc_checkpoint` — the pre-mint pattern

Before each GC pass the coordinator calls `gc_checkpoint` on the volume. The volume mints three ULIDs in one shot from its monotonic clock — `u_repack`, `u_sweep`, `u_wal` — flushes the current WAL under `u_wal`, and opens a fresh WAL. The coordinator receives `(u_repack, u_sweep)` as the output ULIDs for the corresponding passes. Pre-minting all three together encodes the required ordering (`u_repack < u_sweep < u_wal < new_wal_ulid`) in advance and is essential for crash-recovery correctness — without it, a WAL flushed at checkpoint time would keep its older ULID and get shadowed by the GC output on rebuild. ULIDs are always minted by the volume, so coordinator clock skew cannot corrupt segment ordering. See `docs/testing.md` bugs C and D for regressions.

The GC output ULID is `max(inputs).increment()`. Because input segments have already drained (timestamps seconds to minutes behind wall-clock), any concurrent write gets a ULID far ahead of the output and wins at rebuild — no locking needed, and a missed concurrent write is at worst a space leak.

### Handoff file format

`gc/<result-ulid>.pending` holds extent index patches, one per line:

```
repack <hash_hex> <old_ulid> <new_ulid> <new_body_offset>
remove <hash_hex> <old_ulid>
dead   <old_ulid>
```

Only-`remove` is a *removal-only handoff* (no output segment, no re-sign). Only-`dead` is a *tombstone handoff*. Files may mix `repack` and `remove`.

Each handoff progresses through three states (`GcHandoffState` enum; `GcHandoff::from_filename` parses directory entries):

| File | State | Meaning |
|---|---|---|
| `gc/<ulid>.pending` | `Pending` | Staged; volume has not applied |
| `gc/<ulid>.applied` | `Applied` | Volume applied; coordinator has not cleaned up |
| `gc/<ulid>.done` | `Done` | Done; retained 7 days then pruned |

### Handoff protocol

1. Coordinator `gc_checkpoint` → `(u_repack, u_sweep)`.
2. Coordinator stages `gc/<result-ulid>` (ephemeral-signed, repack only) and `gc/<result-ulid>.pending` via tmp-rename.
3. Volume (idle arm) re-signs `gc/<result-ulid>` with `volume.key`, writes `index/<result-ulid>.idx`, applies the patches (`repack` updates the extent location, `remove` deletes the reference, `dead` is a no-op verify), evicts old `cache/<old-ulid>.*`, renames `.pending → .applied`.
4. Coordinator (next tick) uploads `gc/<result-ulid>` (volume-signed), deletes old S3 objects, removes old local segment files, `promote` IPC → volume writes `cache/<new>.*` and removes `gc/<new-ulid>`, then renames `.applied → .done`.
5. Periodic TTL cleanup deletes `.done` files older than 7 days.

A crash at any step leaves recoverable state — `.pending`/`.applied` re-process on the next tick.

**Restart safety.** On restart the extent index is rebuilt from on-disk `.idx` files, which still point to old segments until `apply_done_handoffs` removes them. Two mechanisms close the gap: `apply_gc_handoffs` also processes `.applied` files (re-applying the extent-index updates idempotently with the same `still_at_old` check), and the coordinator calls `apply_gc_handoffs` IPC immediately before `apply_done_handoffs` on every GC tick.

**Coordinator deletion invariant — no segment is deleted without the volume's acknowledgment.** Every deletion, including all-dead segments, goes through the handoff protocol (tombstone for the all-dead case). Direct coordinator deletion is unsafe because its liveness view is rebuilt from on-disk files, which may be behind the volume's in-memory LBA map. Verified in `specs/HandoffProtocol.tla`.

Only one outstanding pass per fork: new passes are deferred while any `gc/*.pending` files exist.

### S3 repacking (future, not implemented)

S3 repacking is a locality optimisation and is not subject to the leaf-only constraint — the coordinator can read any node's segments (local or S3) and rewrite them to co-locate extents accessed together, so a boot sequence can be served by one range-GET. The snapshot tree makes the two-tier layout structurally explicit: ancestor-node extents belong in shared base segments; leaf-node extents belong in snapshot-specific objects. Boot-hint accumulation (per-boot access observations, aggregated per snapshot node) would drive the decisions.

## Filesystem metadata awareness (future)

Because Elide controls the block device, it sees every write including ext4 metadata (superblock, group descriptors, inode tables, extent trees, journal). Two concrete opportunities:

- **Metadata extent tagging** (near-term). Identify metadata LBAs from the superblock, tag them in the LBA map, skip dedup and cache aggressively.
- **Shadow filesystem view** (deferred). Maintain a continuously-updated view of ext4 layout for free snapshot-time re-alignment. Complicated by jbd2: metadata goes to the journal first, not its final LBA. Level 2 (parse committed transactions at snapshot/GC time, `e2fsck`-style) is sufficient for re-alignment; Level 3 (live transaction tracking) is only needed for real-time file-identity-aware dedup and should stay deferred.
