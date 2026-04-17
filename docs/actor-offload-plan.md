# Plan: offload heavy work from the volume actor

**Status:** Partially landed. Steps 1–4 (CAS inserts, WAL promote offload, `apply_gc_handoffs` offload, `promote_segment` offload) are merged on `main`. Step 4 (`promote_segment`) landed out-of-sequence with the original plan because write-tail latency on a live dd workload prioritised the per-upload actor stalls over the segment-index cache. Step 5 (segment-index cache) is now fully landed — the cache covers the worker-thread `promote_segment` path, the actor-thread sweep/repack loops, and `delta_compute::rewrite_post_snapshot_with_prior` (plumbed as part of step 6c). Steps 6a (`sweep_pending` offload), 6b (`repack` offload), and 6c (`delta_repack_post_snapshot` offload) have all landed. Step 7 (snapshot offload) is planned — scope narrowed to `sign_snapshot_manifest` only, since the rest of the coordinator-driven snapshot sequence already routes through offloaded paths; see [snapshot-offload-plan.md](snapshot-offload-plan.md).

The flusher thread introduced in step 2 has since been generalized into a single long-lived **worker thread** that dispatches jobs via a `WorkerJob` enum (currently `Promote`, `GcHandoff`, `PromoteSegment`, `Sweep`, `Repack`, and `DeltaRepack`). Further offloads add new `WorkerJob` variants rather than spawning new threads.

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

Only (1) and (3) need the actor. (2) can run on the shared worker thread and deliver its result back through a channel. No new locks, no new synchronization primitives.

## Prerequisite: conditional inserts on `extent_index` — LANDED

**Landed in PR #51 (Landing 1 of the promote offload).**

While (2) runs, concurrent writes may supersede entries the worker is operating on. The apply phase must tolerate this: every `extent_index.insert(hash, loc_new)` becomes a CAS — *only* replace the entry if it still points at the source segment the worker consumed.

The CAS primitive is `ExtentIndex::replace_if_matches(hash, expected_segment_id, expected_body_offset, new_location)` in `extentindex.rs`. It is used by:

- `flush_wal_to_pending_as` — inline promote path
- `apply_promote` — off-actor promote apply phase
- Recovery-time promote during `Volume::open_impl`

`ExtentIndex::remove_if_matches` was added alongside the sweep/repack offloads so dead-entry removals can also be conditional on the source `(segment_id, body_offset)`. All three maintenance offloads (sweep, repack, delta_repack) now use the CAS primitives unconditionally — no direct `insert` / `remove` calls remain in their apply phases.

## Flush vs promote — LANDED

The flush/promote separation is fully implemented. See [promote-offload-plan.md](promote-offload-plan.md) for the complete design and implementation details.

Summary: `VolumeRequest::Flush` is now a WAL fsync + immediate reply. Promotion is triggered asynchronously by the threshold check and idle tick, dispatched to the worker thread as a `WorkerJob::Promote`. Results arrive on a dedicated bounded crossbeam channel (not a `VolumeRequest` variant). The actor's `select!` loop has three arms: `VolumeRequest`, worker results, and idle tick.

## Op-by-op analysis

### 1. WAL promote *(LANDED)*

**Landed in PRs #51, #55, #56, #57.** See [promote-offload-plan.md](promote-offload-plan.md).

The worker is a single long-lived thread with bounded channels (capacity 4). Multiple promotes can be in flight. `gc_checkpoint` routes through the same worker with a parked reply. The apply phase uses CAS (`replace_if_matches`) to handle concurrent writes.

### 2. `apply_gc_handoffs` *(LANDED)*

**Landed in PR #58.** The re-sign (read the coordinator-staged body, read the inputs' `.idx` files, rewrite with the volume key) now runs on the worker thread as `WorkerJob::GcHandoff`.

Prep phase on the actor enumerates `gc/*.staged` files and builds a `GcHandoffJob` per entry. The worker reads the staged segment, collects body-owning entries from each input's `.idx`, reads inline + body data, re-signs, and writes `gc/<ulid>.tmp`. Apply phase on the actor runs `Volume::apply_gc_handoff_result`: conditional extent-index rewrites (CAS-compatible), rename `.tmp` → `.applied`, `publish_snapshot`. Batches dispatch one handoff at a time; the next is sent after the previous result applies.

### 3a. `sweep_pending` *(LANDED)*

Heavy bits: `read_and_verify_segment_index`, `read_inline_section`, `read_extent_bodies`, `write_segment`, `rename`, `fsync_dir`, looped over N segments.

Prep on the actor (`Volume::prepare_sweep`) snapshots `Arc<LbaMap>`, the snapshot floor, and signer/key state into a `SweepJob`. Worker (`actor::execute_sweep`) does the per-segment loop, partitioning, body reads, and the merged `write_segment` + rename + `fsync_dir`. The result carries each surviving live entry and each dead entry paired with its source `(segment_id, body_offset)` CAS preconditions, captured before `write_segment` reassigns offsets. Apply (`Volume::apply_sweep_result`) drops dead entries via `remove_if_matches` and re-points carried-live entries via `replace_if_matches` — concurrent overwrites between prep and apply naturally lose the CAS and survive untouched.

The liveness set is computed at prep time and is necessarily a *subset* of true liveness at apply time (concurrent writes can only add references). This is conservative in the safe direction — we might keep a now-dead hash alive for one more cycle, never the reverse.

`Volume::sweep_pending(&mut self)` is preserved as a thin synchronous wrapper (`prepare_sweep` + `execute_sweep` + `apply_sweep_result`) so existing tests and any inline callers still compile against the same signature; the actor uses the offload path directly with parked replies and a `parked_sweep` slot that rejects concurrent requests.

**Follow-up: candidate selection.** The current `size < 8 MiB` test produces a long tail of slightly-too-large segments (two 7 MiB inputs → one 14 MiB output → permanently ineligible). Switch to an opportunistic-packing rule that targets *filling* output segments toward 32 MiB rather than just evicting the very-small ones. See the TODO under "Pending compaction" in `operations.md`.

### 3b. `repack` *(LANDED)*

Per-segment rewrite: each eligible segment is rewritten in place, reusing its own ULID. Prep on the actor (`Volume::prepare_repack`) snapshots `Arc<LbaMap>`, the snapshot floor, and signer/key state into a `RepackJob`. Worker (`actor::execute_repack`) iterates every non-floor segment, computes live/total bytes against the captured `lbamap`, partitions entries with the same predicate as today, reads bodies for the live set, and writes a fresh `.tmp` that renames over the original path (output ULID = input ULID). Segments with zero live entries are deleted outright. Apply (`Volume::apply_repack_result`) drops dead entries via `remove_if_matches` and re-points carried-live entries via `replace_if_matches` — the CAS precondition is `(seg_id, source_body_offset)` captured before `write_segment` reassigned offsets, so concurrent writes between prep and apply lose the CAS and survive untouched.

Unlike sweep, each segment produces its own output; the result carries a `Vec<RepackedSegment>` with per-segment CAS payloads. Apply walks that list serially — no per-segment parallelism is attempted yet; that's the delta_repack concern (step 3c / 6c).

`Volume::repack(&mut self, min_live_ratio)` is preserved as a thin synchronous wrapper (`prepare_repack` + `execute_repack` + `apply_repack_result`) so existing tests still compile against the same signature; the actor uses the offload path directly with a `parked_repack` slot that rejects concurrent requests.

### 3c. `delta_repack_post_snapshot` *(LANDED)*

Heavy bit: `delta_compute::rewrite_post_snapshot_with_prior` per segment — zstd-dict delta computation against the prior snapshot. Seconds on large volumes.

Prep on the actor (`Volume::prepare_delta_repack`) resolves the latest sealed snapshot and packages the signer, verifying key, and segment-cache handle into a `DeltaRepackJob`. Worker (`actor::execute_delta_repack`) constructs the snapshot-pinned `BlockReader` off the actor, iterates every post-snapshot segment in `pending/`, and invokes `rewrite_post_snapshot_with_prior` — which now takes the shared `SegmentIndexCache` so the parse is shared with the sweep/repack workers. The result carries a `RewrittenSegment` per rewritten input, each entry paired with its pre-rewrite `(kind, stored_offset)` so apply can CAS against the source location.

Apply (`Volume::apply_delta_repack_result`) runs on the actor: evicts the fd cache, then walks entries with `replace_if_matches` for Data/Inline carried through, `remove_if_matches` + `insert_delta_if_absent` for Data→Delta conversions, and `insert_delta_if_absent` (no-op when the slot exists) for pre-existing Delta entries — matching the pre-offload behaviour. Concurrent overwrites between prep and apply naturally lose the CAS and survive untouched.

`Volume::delta_repack_post_snapshot(&mut self)` is preserved as a thin synchronous wrapper (`prepare_delta_repack` + `execute_delta_repack` + `apply_delta_repack_result`) so existing tests and inline callers still compile; the actor uses the offload path directly with a `parked_delta_repack` slot that rejects concurrent requests.

Per-segment parallelism (a small worker pool rather than a single worker) remains the obvious next-step optimisation — delta computation runs into seconds per segment — but is out of scope for the single-worker offload. The pre-offload behaviour is single-threaded already; this step is pure latency isolation for writes.

### 4. `promote_segment` *(LANDED)*

**Landed in PR #59.** See [promote-segment-offload-plan.md](promote-segment-offload-plan.md) for the full design and landing breakdown.

Prep phase on the actor picks the source (`pending/<ulid>` > `gc/<ulid>` > body-exists early-return) and builds a `PromoteSegmentJob`. Worker thread reads + verifies the index once, handles the GC tombstone shortcut, writes `index/<ulid>.idx` and `cache/<ulid>.{body,present}` (both idempotent on retry), and returns parsed entries + bss + inline bytes. Apply phase on the actor runs the extent-index CAS (Local → Cached) + `pending/<ulid>` delete for the drain path, or input-idx cleanup for the GC path. `WorkerResult::PromoteSegment` carries the ULID out-of-band so errors map to the right parked caller. Single-parse collapse folded in — the previous triple parse is now one `read_and_verify_segment_index` shared across tombstone check, idx extract, body copy, and apply-phase CAS.

### 5. `snapshot` *(planned — scope: `sign_snapshot_manifest` only)*

The original framing ("composite of the pieces above") assumed `Volume::snapshot()` was still the hot production path. It isn't: the coordinator decomposes snapshot into `promote_wal` + `drain_pending` (per-segment `promote`) + `apply_gc_handoffs` + `sign_snapshot_manifest`, and the first three steps already route through the worker. Only `sign_snapshot_manifest` still blocks the actor — `index/` enumeration + Ed25519 sign + atomic manifest write (fsync) + marker write.

The dead `VolumeRequest::Snapshot` arm (no wire sender; `src/control.rs:200` is the only handler and the CLI now goes through the coordinator) is deleted in the same landing. `Volume::snapshot()` the method stays for in-process tests and never runs on the actor.

See [snapshot-offload-plan.md](snapshot-offload-plan.md) for the design.

## Shared optimization: segment-index cache *(LANDED)*

`elide_core::segment_cache::SegmentIndexCache` is an `Arc`-shared LRU (capacity 64) keyed by `(path, file_len)` over `Arc<ParsedIndex>`, with a `VerifyingKey` hash discriminator so a stale entry from a different key never hits. The `Volume` owns one; worker jobs (`GcHandoffJob`, `PromoteSegmentJob`, `SweepJob`, `RepackJob`, `DeltaRepackJob`) carry cloned `Arc`s so the actor thread and the worker thread share cache slots.

The following hot paths are routed through the cache:

- `actor::execute_promote_segment` (worker thread; every confirmed S3 upload)
- `actor::execute_sweep` (worker thread; sweep loop)
- `actor::execute_repack` (worker thread; repack loop)
- `actor::execute_delta_repack` → `delta_compute::rewrite_post_snapshot_with_prior` (worker thread; delta repack loop)
- `Volume::evict_dead_segments` / compact_pending scan (actor thread)

Still direct-call (low win or out-of-scope for this pass):

- `Volume::apply_gc_handoffs` post-write re-parse of the tmp file (file just written; no prior parse to hit)
- `Volume::delete_cached_inputs` scan of `gc/<ulid>` (rare)
- `Volume::redact_segment` (rare, in-place)
- `delta_compute::maybe_rewrite_segment` (runs in `elide-import` one-shot, not in the actor path)
- `extentindex`/`lbamap` rebuild paths (boot-time; cache is empty then)

Cache invalidation is file-length-based: tmp+rename rewrites change the entry count → the index section length → the file length, so a stale entry naturally misses. `redact_segment` punches body holes but preserves the index section and file length, so a cache hit returns the same parsed index. Cross-fork correctness is guarded by the verifying-key hash on each cached entry.

## Sequencing

1. **Make `extent_index` inserts conditional.** LANDED (PR #51). `replace_if_matches` on `ExtentIndex`.
2. **Introduce a single worker thread and offload WAL promote.** LANDED (PRs #55, #56, #57). Dedicated bounded channels, three-arm `select!`, multi-promote queuing, GC checkpoint routing. See [promote-offload-plan.md](promote-offload-plan.md).
3. **Offload `apply_gc_handoffs`.** LANDED (PR #58). Generalized the worker to carry a `WorkerJob` enum and added the `GcHandoff` variant.
4. **Offload `promote_segment` (and fix the re-parse).** LANDED (PR #59, out-of-sequence with the original plan). Added `WorkerJob::PromoteSegment` + parked-reply dispatch. See [promote-segment-offload-plan.md](promote-segment-offload-plan.md).
5. **Land the segment-index cache.** LANDED. `SegmentIndexCache` covers the worker-thread `promote_segment` / sweep / repack / delta_repack paths and the actor-thread `compact_pending` loop. `delta_compute::rewrite_post_snapshot_with_prior` was plumbed through in step 6c.
6. **Offload `sweep_pending`, `repack`, `delta_repack_post_snapshot`.** In that order — sweep is the simplest, delta_repack is the largest payoff. Per-segment parallelism (a small worker pool rather than a single worker) remains a future optimisation for delta_repack.
   - **6a (LANDED):** `sweep_pending`. Added `WorkerJob::Sweep` + parked-reply dispatch. Apply uses `replace_if_matches` for carried-live entries and the new `remove_if_matches` for dead entries, both keyed on the source `(segment_id, body_offset)`.
   - **6b (LANDED):** `repack`. Added `WorkerJob::Repack` + `parked_repack` slot. Per-segment rewrite with output ULID = input ULID; apply uses `replace_if_matches` / `remove_if_matches` keyed on the source `(seg_id, body_offset)` captured before `write_segment` reassigned offsets.
   - **6c (LANDED):** `delta_repack_post_snapshot`. Added `WorkerJob::DeltaRepack` + `parked_delta_repack` slot. Worker constructs the snapshot-pinned `BlockReader` off the actor; `rewrite_post_snapshot_with_prior` now takes the shared `SegmentIndexCache`. Apply pairs each rewritten entry with its pre-rewrite `(kind, stored_offset)` and CAS-updates the extent index via `replace_if_matches` / `remove_if_matches` / `insert_delta_if_absent` depending on the kind transition.
7. **Offload `sign_snapshot_manifest` + delete dead `VolumeRequest::Snapshot`.** See [snapshot-offload-plan.md](snapshot-offload-plan.md). Production's coordinator-driven snapshot already decomposes into `promote_wal` / per-segment `promote` / `apply_gc_handoffs` / `sign_snapshot_manifest`, and the first three are already offloaded. Only the manifest signing remains on the actor, and the legacy single-shot `VolumeRequest::Snapshot` arm is dead wire.

After step 2 every subsequent step reuses the same infrastructure — bounded crossbeam channels, a worker thread (or pool), and result handling in the worker `select!` arm. No new primitives, no new locks, no read-path changes.

## Non-goals

- Speeding up maintenance itself. The goal is latency isolation for writes, not throughput.
- Any change to the read path. Reads are already lock-free.
- Parallelising the actor. There is still exactly one actor thread applying mutations; the offload is into stateless workers whose results are re-applied serially on the actor.
- Persistent job state across crashes. Every offloaded job is idempotent from on-disk state — if a worker is mid-flight when the process dies, the next startup re-scans `pending/` / `gc/` and retries from scratch.

## Related: coordinator-side blocking GC *(out of scope, follow-up)*

The coordinator's per-fork GC tick (`elide-coordinator/src/gc.rs::gc_fork`) is `async fn` but performs blocking IO inline (index rebuild, segment reads, S3 round-trips) on the per-fork tokio task — *not* via `spawn_blocking`. The TODO at `gc.rs:55-57` notes this is fine while passes stay short and to switch when they grow long enough to stall other coordinator tasks.

This is a separate concern from the volume-actor offload tracked above:

- The volume actor's offload protects **writes**. The coordinator's `gc_fork` never serves writes; its latency concern is starving the runtime / other forks scheduled on the same task pool.
- It is structurally already isolated from the volume — coordinator and volume are different processes.

When this needs work the fix is mechanical (`tokio::task::spawn_blocking` around the synchronous body of `gc_fork`), not a worker-thread architecture like this plan describes.
