# Plan: offload heavy work from the volume actor

**Status:** Proposal.

## Problem

`VolumeActor` (`elide-core/src/actor.rs`) owns the `Volume` exclusively and processes all mutating requests on a single thread. Reads bypass the actor entirely via an `ArcSwap<ReadSnapshot>` and never contend. Writes, flushes, compaction, GC-handoff application, delta-repack, snapshot, and `promote_segment` all run on the actor thread and serialize against each other through the crossbeam channel.

This is a clean design for correctness but creates write-tail latency under maintenance: a slow op on the actor blocks every queued write behind it. The worst offenders are in the 100 ms – seconds range.

| Op | Typical cost | Frequency |
|---|---|---|
| `flush_wal` / promote | 10–500 ms | every 32 MiB written or 10 s idle |
| `apply_gc_handoffs` re-sign | 10s–100s ms per handoff | idle tick + explicit |
| `sweep_pending` / `repack` | 100s ms – seconds | explicit |
| `delta_repack_post_snapshot` | seconds | explicit, post-snapshot |
| `snapshot` (full) | seconds | explicit |
| `promote_segment` | 10s–100s ms | every confirmed S3 upload |

Reads are unaffected throughout. The goal of this plan is to unblock **writes** during maintenance, not to speed up maintenance itself.

## The shape every blocking op shares

Each maintenance op decomposes cleanly into three phases:

1. **Prep** (on actor): snapshot the inputs — live-hash set, `Arc<ExtentIndex>`, floor ULID, owned `pending_entries`, file paths.
2. **Heavy middle** (pure, off actor): read segments, verify signatures, decompress, write new segments, fsync, compute deltas, sign manifests. A pure function of the prep snapshot plus filesystem paths; touches no mutable `Volume` state.
3. **Apply** (on actor): update `extent_index`, evict `file_cache`, publish snapshot, rename/delete files.

Only (1) and (3) need the actor. (2) can run on a dedicated flusher/worker thread and deliver its result back through a new `VolumeRequest::*Complete` variant that the actor handles on its next turn. No new locks, no new synchronization primitives — just more message kinds on the existing channel.

## Prerequisite: conditional inserts on `extent_index`

While (2) runs, concurrent writes may supersede entries the worker is operating on. The apply phase must tolerate this: every `extent_index.insert(hash, loc_new)` becomes a CAS — *only* replace the entry if it still points at the source segment the worker consumed.

Today's code already does this check on the **remove** path:

- `volume.rs:1182` — `sweep_pending` dead-entry removal
- `volume.rs:843` — `repack` dead-entry removal
- `volume.rs:1622` — `apply_gc_handoffs` index update
- `volume.rs:1888` — `promote_segment` index update

But the **insert** path in `sweep_pending` (`volume.rs:1254`, `1268`) and `repack` (`volume.rs:892`, `906`) and `delta_repack_post_snapshot` (`volume.rs:1031`, `1045`) unconditionally overwrites. This is safe today only because everything runs atomically under the actor lock. Making these inserts conditional is a small, local change and is the first landing piece.

## Flush vs promote

Today `flush_wal()` conflates two distinct operations:

- **Flush** = `wal.fsync()`. Durability barrier. The WAL is the durability boundary — after fsync, data survives a crash. This is what `NBD_CMD_FLUSH` requires.
- **Promote** = serialize WAL entries into a `pending/<ulid>` segment, update the extent index, delete the old WAL, open a fresh WAL. Housekeeping for the segment lifecycle.

The offload separates these. `VolumeRequest::Flush` becomes a WAL fsync + immediate reply. Promotion is triggered asynchronously by the threshold check and idle tick. Callers that need data in `pending/` (snapshot, `gc_checkpoint`) wait for promote completion via their own IPC verbs.

See [promote-offload-plan.md](promote-offload-plan.md) for the full separation.

## Op-by-op analysis

### 1. WAL promote — `volume.rs:2090` *(biggest win)*

Heavy bit: `segment::promote` at `2111` serialises the in-memory `pending_entries` into a signed segment file and fsyncs.

**Why it's the biggest win:** highest frequency by far, and the data is already in memory — the worker doesn't even need to re-read the WAL.

**Offload shape:**
- Actor: `wal.fsync()`, `std::mem::take(&mut self.pending_entries)`, open the fresh WAL (new writes start flowing immediately), send `PromoteJob { ulid, entries, signer }` to the flusher thread.
- Worker: write `pending/<ulid>` + fsync.
- Actor on completion: flip `extent_index` entries from WAL-relative to segment-relative offsets, bump `flush_gen`, publish snapshot, delete old WAL file.

**Correctness wrinkle:** during the window between "fresh WAL opened" and "apply phase runs," readers may resolve `extent_index.segment_id == wal_ulid` and open the old WAL file. The old WAL must stay on disk until the apply phase — at the cost of transient double-storage. `flush_gen` bump at apply time evicts reader `file_cache` entries so the next read picks up the new segment.

### 2. `apply_gc_handoffs` — `volume.rs:1385`

Heavy bits: the re-sign at `1449-1462` (read the coordinator-staged body, rewrite with the volume key) and the segment-index verify at `1501`.

**Offload shape:** per-handoff job. Worker input is `(gc_seg_path, signer, verifying_key)`; worker output is `(new_entries, body_section_start, inline_bytes)`. Apply phase does extent-index updates (already conditional at `1625`) and the file rename/cleanup at `1705-1729`.

### 3. `delta_repack_post_snapshot` — `volume.rs:955`

Heavy bit: `delta_compute::rewrite_post_snapshot_with_prior` per segment at `993` — zstd-dict delta computation against the prior snapshot. Seconds on large volumes.

**Offload shape:** each segment is independent, so this is a candidate for **per-segment parallelism on a worker pool**, not just serialization. Inputs are `(seg_path, &BlockReader, signer, verifying_key)`; the `BlockReader` is already a pinned-snapshot read-only view with no shared mutable state. Apply phase walks the converted entries and issues conditional inserts.

### 4. `sweep_pending` / `repack` — `volume.rs:1112` / `766`

Heavy bits: `read_and_verify_segment_index`, `read_inline_section`, `read_extent_bodies`, `write_segment`, `rename`, `fsync_dir`, looped over N segments.

**Offload shape:** prep snapshot is `(live_set, floor, pending_dir, signer, verifying_key)`. Worker output is `(merged_live_entries, new_ulid, body_section_start, candidate_paths_to_delete)`. Apply phase does conditional inserts, `file_cache` eviction, candidate deletion.

The liveness set is computed at prep time and is necessarily a *subset* of true liveness at apply time (concurrent writes can only add references). This is conservative in the safe direction — we might keep a now-dead hash alive for one more cycle, never the reverse.

### 5. `promote_segment` — `volume.rs:1814`

Heavy bits: `extract_idx` at `1847` and `promote_to_cache` at `1854` are both pure file-to-file copies. There's also a wasteful re-read at `1874` — the parsing done there was already done by whatever produced the segment.

**Offload shape:** trivially hoistable. **Free work reduction available first:** plumb the parsed segment index through the producing path so `promote_segment` doesn't re-parse and re-verify.

### 6. `snapshot` — `volume.rs:2210`

Combined op: promote + loop of `promote_segment` + directory enumerations + manifest signing + marker write. Requires all WAL data in `pending/` before proceeding, so it waits for any in-flight promote to complete before starting.

Falls out of the other offloads: the promote phase reuses (1), the promote loop reuses (5), and the manifest signing at `2323` is pure crypto on in-memory content — the simplest possible worker job.

## Shared optimization: segment-index cache

Five of these ops call `segment::read_and_verify_segment_index` on segments they almost certainly parsed moments earlier (sweep, repack, delta_repack, apply_gc, promote). A small LRU cache keyed by `(ulid, file_len)` holding the parsed `(body_section_start, entries)` would let workers skip both the disk read and the signature verify on the hot retry paths. Orthogonal to offloading but probably worth more than any single hoist in absolute CPU terms.

## Sequencing

1. **Make `extent_index` inserts conditional.** Smallest change, benefits nothing on its own but unblocks everything else. Add a proptest that writes while a repack/sweep is mid-flight (once the infrastructure from step 2 exists).
2. **Introduce a single flusher thread and a `VolumeRequest::PromoteComplete` message.** Offload `flush_wal_to_pending` — this is the one op where the write data is already in memory, the one op whose offload directly reduces write tail latency, and the one op that lets us validate the three-phase pattern end-to-end.
3. **Offload `apply_gc_handoffs`.** Reuses the flusher thread with a new job variant. Lowest risk after promote.
4. **Land the segment-index cache.** Orthogonal but by this point it's clearly paying for itself across several call sites.
5. **Offload `sweep_pending`, `repack`, `delta_repack_post_snapshot`.** In that order — sweep is the simplest, delta_repack is the largest payoff. Delta_repack is the first op where per-segment parallelism (a small worker pool rather than a single flusher) is clearly worth it.
6. **Offload `promote_segment` (and fix the re-parse).** Snapshot falls out as a composite of the pieces above.

After step 2 every subsequent step reuses the same infrastructure — a crossbeam channel, a worker thread (or pool), and a set of `*Complete` message variants. No new primitives, no new locks, no read-path changes.

## Non-goals

- Speeding up maintenance itself. The goal is latency isolation for writes, not throughput.
- Any change to the read path. Reads are already lock-free.
- Parallelising the actor. There is still exactly one actor thread applying mutations; the offload is into stateless workers whose results are re-applied serially on the actor.
- Persistent job state across crashes. Every offloaded job is idempotent from on-disk state — if a worker is mid-flight when the process dies, the next startup re-scans `pending/` / `gc/` and retries from scratch.
