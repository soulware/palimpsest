# Plan: offload heavy work from the volume actor

**Status:** Partially landed. Steps 1–3 (CAS inserts + WAL promote offload + `apply_gc_handoffs` offload) are merged. Steps 4–6 are not started.

The flusher thread introduced in step 2 has since been generalized into a single long-lived **worker thread** that dispatches jobs via a `WorkerJob` enum (currently `Promote` and `GcHandoff`). Further offloads add new `WorkerJob` variants rather than spawning new threads.

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

The **insert** path in `sweep_pending`, `repack`, and `delta_repack_post_snapshot` still unconditionally overwrites. This is safe today because those ops still run atomically on the actor. When those ops are offloaded (steps 4–5), their inserts must also become conditional.

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

### 3. `delta_repack_post_snapshot` *(not started)*

Heavy bit: `delta_compute::rewrite_post_snapshot_with_prior` per segment — zstd-dict delta computation against the prior snapshot. Seconds on large volumes.

**Offload shape:** each segment is independent, so this is a candidate for **per-segment parallelism on a worker pool**, not just serialization. Inputs are `(seg_path, &BlockReader, signer, verifying_key)`; the `BlockReader` is already a pinned-snapshot read-only view with no shared mutable state. Apply phase walks the converted entries and issues conditional inserts.

### 4. `sweep_pending` / `repack` *(not started)*

Heavy bits: `read_and_verify_segment_index`, `read_inline_section`, `read_extent_bodies`, `write_segment`, `rename`, `fsync_dir`, looped over N segments.

**Offload shape:** prep snapshot is `(live_set, floor, pending_dir, signer, verifying_key)`. Worker output is `(merged_live_entries, new_ulid, body_section_start, candidate_paths_to_delete)`. Apply phase does conditional inserts, `file_cache` eviction, candidate deletion.

The liveness set is computed at prep time and is necessarily a *subset* of true liveness at apply time (concurrent writes can only add references). This is conservative in the safe direction — we might keep a now-dead hash alive for one more cycle, never the reverse.

### 5. `promote_segment` *(not started)*

Heavy bits: `extract_idx` and `promote_to_cache` are both pure file-to-file copies. There's also a potentially wasteful re-read — the parsing done in promote_segment may duplicate work already done by whoever produced the segment.

**Offload shape:** trivially hoistable. **Free work reduction available first:** plumb the parsed segment index through the producing path so `promote_segment` doesn't re-parse and re-verify.

### 6. `snapshot` *(not started)*

Combined op: promote + loop of `promote_segment` + directory enumerations + manifest signing + marker write. Requires all WAL data in `pending/` before proceeding, so it waits for any in-flight promote to complete before starting.

Falls out of the other offloads: the promote phase reuses (1), the promote loop reuses (5), and the manifest signing is pure crypto on in-memory content — the simplest possible worker job.

## Shared optimization: segment-index cache *(not started)*

Several of these ops call `segment::read_and_verify_segment_index` on segments they almost certainly parsed moments earlier (sweep, repack, delta_repack, apply_gc, promote). A small LRU cache keyed by `(ulid, file_len)` holding the parsed `(body_section_start, entries)` would let workers skip both the disk read and the signature verify on the hot retry paths. Orthogonal to offloading but probably worth more than any single hoist in absolute CPU terms.

## Sequencing

1. **Make `extent_index` inserts conditional.** LANDED (PR #51). `replace_if_matches` on `ExtentIndex`.
2. **Introduce a single worker thread and offload WAL promote.** LANDED (PRs #55, #56, #57). Dedicated bounded channels, three-arm `select!`, multi-promote queuing, GC checkpoint routing. See [promote-offload-plan.md](promote-offload-plan.md).
3. **Offload `apply_gc_handoffs`.** LANDED (PR #58). Generalized the worker to carry a `WorkerJob` enum and added the `GcHandoff` variant.
4. **Land the segment-index cache.** Orthogonal but by this point it's clearly paying for itself across several call sites.
5. **Offload `sweep_pending`, `repack`, `delta_repack_post_snapshot`.** In that order — sweep is the simplest, delta_repack is the largest payoff. Delta_repack is the first op where per-segment parallelism (a small worker pool rather than a single worker) is clearly worth it.
6. **Offload `promote_segment` (and fix the re-parse).** Snapshot falls out as a composite of the pieces above.

After step 2 every subsequent step reuses the same infrastructure — bounded crossbeam channels, a worker thread (or pool), and result handling in the worker `select!` arm. No new primitives, no new locks, no read-path changes.

## Non-goals

- Speeding up maintenance itself. The goal is latency isolation for writes, not throughput.
- Any change to the read path. Reads are already lock-free.
- Parallelising the actor. There is still exactly one actor thread applying mutations; the offload is into stateless workers whose results are re-applied serially on the actor.
- Persistent job state across crashes. Every offloaded job is idempotent from on-disk state — if a worker is mid-flight when the process dies, the next startup re-scans `pending/` / `gc/` and retries from scratch.
