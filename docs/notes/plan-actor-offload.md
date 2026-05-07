---
status: landed
related: [plan-promote-offload.md, plan-promote-segment-offload.md, plan-snapshot-offload.md]
landed_in: ../architecture.md
---

# Offload heavy work from the volume actor

`VolumeActor` (`elide-core/src/actor.rs`) processes all mutating requests on a single thread. A clean design for correctness, but slow ops (`flush_wal`, `apply_gc_handoffs`, `sweep_pending`, `repack`, `delta_repack_post_snapshot`, `snapshot`, `promote_segment`) blocked queued writes behind them — 100 ms to seconds of write-tail latency under maintenance.

The seven-step plan offloaded each of these onto a single long-lived **worker thread** dispatching `WorkerJob` variants, keeping write throughput unaffected during maintenance. Reads were already lock-free and stayed so.

## The shape every offload shared

1. **Prep** (on actor): snapshot inputs — live-hash set, `Arc<ExtentIndex>`, floor ULID, owned `pending_entries`, file paths.
2. **Heavy middle** (pure, off actor): read segments, verify, decompress, write new segments, fsync, compute deltas. A pure function of the prep snapshot plus filesystem paths; touches no mutable `Volume` state.
3. **Apply** (on actor): update `extent_index` via CAS primitives, evict `file_cache`, publish snapshot, rename/delete files.

Only (1) and (3) need the actor. (2) runs on the worker thread and delivers its result back through a channel. No new locks, no new synchronization primitives.

## Prerequisite: conditional inserts

`ExtentIndex::replace_if_matches` (and later `remove_if_matches`) makes apply phases CAS-keyed on the source `(segment_id, body_offset)`. Concurrent overwrites between prep and apply naturally lose the CAS and survive untouched — conservative in the safe direction, never the reverse.

## Steps

| # | Op | PR | Sub-plan |
|---|---|---|---|
| 1 | `extent_index` CAS primitives | #51 | — |
| 2 | WAL promote | #55, #56, #57 | [plan-promote-offload.md](plan-promote-offload.md) |
| 3 | `apply_gc_handoffs` | #58 | — |
| 4 | `promote_segment` | #59 | [plan-promote-segment-offload.md](plan-promote-segment-offload.md) |
| 5 | `SegmentIndexCache` (Arc-shared LRU, key = `(path, file_len)` + verifying-key hash) | #61 | — |
| 6a/b/c | `sweep_pending` / `repack` / `delta_repack_post_snapshot` | #62, #64, #65 | — |
| 7 | `sign_snapshot_manifest` + delete dead `VolumeRequest::Snapshot` | #68 | [plan-snapshot-offload.md](plan-snapshot-offload.md) |

Step 4 landed out-of-sequence to prioritise per-upload actor stalls observed under a live `dd` workload. The flusher thread introduced in step 2 was generalised into the long-lived worker; further offloads add `WorkerJob` variants, not threads.

Each step's `Volume::<op>(&mut self)` method survives as a thin synchronous wrapper (`prepare_*` + `execute_*` + `apply_*`) so existing tests still compile against the same signature; the actor uses the offload path directly.

## Non-goals (kept)

- Not speeding up maintenance — only isolating writes from it.
- No read-path change.
- No persistent job state — every offloaded job is idempotent from on-disk state; mid-flight crashes resume from scratch.

## Open follow-ups

- Per-segment parallelism for `delta_repack_post_snapshot` (single-threaded today; pre-offload was also single-threaded — pure latency isolation).
- Coordinator-side `gc_fork` does blocking I/O on its tokio task. Out of scope for this plan: when it needs work, the fix is `tokio::task::spawn_blocking`, not the worker-thread architecture above.
