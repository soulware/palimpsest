# Plan: offload `promote_segment` off the volume actor

**Status:** Landed on branch `promote-segment-offload`. Step 6 of [actor-offload-plan.md](actor-offload-plan.md). Reuses the worker-thread infrastructure established by [promote-offload-plan.md](promote-offload-plan.md) and extended by PR #58.

## Goal

Move the heavy middle of `Volume::promote_segment` — file I/O and signature verification — off the actor thread. The coordinator issues a `promote <ulid>` IPC for every segment it confirms in S3 (once per pending segment produced by drain, and once per GC output). Under a write burst this fires repeatedly during the drain tick and each invocation blocks every queued write for 10–100 ms.

Folded into the same landing: collapse the current **triple re-parse** of the segment index inside one call down to a single parse.

**Non-goals.** Changing the IPC protocol. Changing the on-disk lifecycle of `pending/` / `gc/` / `cache/` / `index/`. Running multiple promotes in parallel on a pool. Offloading `redact_segment` (which runs on the actor just before upload; separate concern).

## Current flow

`VolumeHandle::promote_segment(ulid)` → `VolumeRequest::Promote` → actor thread runs `Volume::promote_segment(ulid)` (`elide-core/src/volume.rs:2438`):

1. Decide source: `pending/<ulid>` (drain) or `gc/<ulid>` (GC). Idempotent early-return if `cache/<ulid>.body` already exists.
2. GC tombstone shortcut: if the source is a GC output with zero entries and non-empty inputs, **parse + verify the index**, delete input `.idx` files, return.
3. `segment::extract_idx(src, idx_path)` — **reads header + index section**, writes `index/<ulid>.idx`.
4. `segment::promote_to_cache(src, body_path, present_path)` — **reads header + parses index** internally, copies body bytes via `io::copy` (sparse), writes `.present`, optionally writes `.delta`.
5. Evict cached fd for this segment.
6. Drain path: **`read_and_verify_segment_index` again** (third parse, plus signature verify), read inline section if any inline entries, CAS extent-index rewrites (`BodySource::Local` → `BodySource::Cached(n)`), delete `pending/<ulid>`, delete `pending/<ulid>.delta`.
7. GC path (non-tombstone): **`read_and_verify_segment_index` again** (second parse), delete `index/<old>.idx` for every consumed input.

Every step runs on the actor. Steps 3–4 are bulk file I/O. Step 6's re-parse duplicates work already done by steps 3+4.

## Proposed flow

### Phase shape (same three-phase pattern as other offloads)

**Prep (actor):** decide drain vs GC, early-return if already promoted, build `PromoteSegmentJob`.

**Heavy middle (worker):** open the source segment **once**, parse + verify the index **once**, emit idx file, copy body to cache, build the apply-phase inputs (entries, bss, inline bytes, inputs list for GC cleanup).

**Apply (actor):** conditional extent-index CAS, directory mutations (`pending/<ulid>` delete, `pending/<ulid>.delta` delete, input `.idx` deletes), evict fd cache, publish snapshot, reply to the parked caller.

### Data structures

```rust
pub struct PromoteSegmentJob {
    pub ulid: Ulid,
    pub src_path: PathBuf,
    pub is_drain: bool,              // pending/ vs gc/
    pub body_path: PathBuf,
    pub present_path: PathBuf,
    pub idx_path: PathBuf,
    pub verifying_key: VerifyingKey, // for signature verify
}

pub struct PromoteSegmentResult {
    pub ulid: Ulid,
    pub is_drain: bool,
    pub src_path: PathBuf,
    pub body_section_start: u64,
    pub entries: Vec<SegmentEntry>,
    pub inputs: Vec<Ulid>,           // consumed inputs for GC path
    pub inline: Vec<u8>,             // empty if no inline entries
    pub tombstone: bool,             // GC-tombstone shortcut taken
}
```

`WorkerJob` and `WorkerResult` grow a `PromoteSegment(..)` variant each. No new thread. No new channel.

### Worker

One function — `execute_promote_segment(job: PromoteSegmentJob) -> io::Result<PromoteSegmentResult>`:

1. If `body_path` already exists → idempotent success. Open nothing, return a result with empty entries and `tombstone = false` (apply phase no-ops on empty entries; see below).
2. Open `src_path`. `read_and_verify_segment_index` once — get `(bss, entries, inputs)`.
3. If `!is_drain` and `entries.is_empty()` and `!inputs.is_empty()`: this is a GC tombstone. Return with `tombstone = true`, `entries = []`, `inputs = <read list>`. Apply phase handles input-idx cleanup and stops.
4. Write `idx_path` from the already-parsed header + index-section bytes (no re-read). `write_file_atomic`.
5. Copy body bytes to `body_path` via the sparse-copy walk over Data entries (same as today, but using the already-parsed `entries` — no second parse). Write `.present`, optionally `.delta`. Commit `.body` last.
6. If any entry is `Inline`, read the inline section bytes. (Small: ~tens of KB.)
7. Return `PromoteSegmentResult { .. }`.

The heavy I/O — read the segment, signature-verify, write the idx, copy the body, write present — all happens here. The actor is free to process writes throughout.

### Apply phase (actor)

On `WorkerResult::PromoteSegment(Ok(result))`:

1. If `result.tombstone`: delete `index/<old>.idx` for each consumed input. **Do not** touch the extent index. Done.
2. If `result.entries.is_empty()` and not tombstone: this was the idempotent-early-return case. Reply `Ok(())`. Done.
3. Otherwise: loop over `entries`, CAS the extent-index entry from `(segment_id == ulid, body_source: Local)` to `Cached(i)` (same code as today, but from the pre-parsed `entries` and `inline`).
4. Drain-specific: `evict_cached_segment(ulid)`, delete `pending/<ulid>`, delete `pending/<ulid>.delta`.
5. GC-specific: delete `index/<old>.idx` for each consumed input.
6. `publish_snapshot()`, reply `Ok(())` to the parked caller.

### Parking

```rust
struct ParkedPromoteSegment {
    ulid: Ulid,
    reply: Sender<io::Result<()>>,
}

parked_promote_segments: Vec<ParkedPromoteSegment>,
```

On `VolumeRequest::Promote { ulid, reply }`:
- Actor calls `Volume::prepare_promote_segment(ulid)` which returns `PromoteSegmentPrep`: either a `PromoteSegmentJob`, or `PrepOutcome::AlreadyPromoted`, or an error.
- If `AlreadyPromoted`: reply `Ok(())` immediately.
- Otherwise: park the reply, dispatch `WorkerJob::PromoteSegment(job)`.

Multiple parked replies are supported (coordinator may fire promote_segment for several ULIDs in the same drain tick; the bounded worker channel already throttles).

On worker result: match by ULID, pop and reply.

### Concurrency with other worker jobs

The worker thread is single-threaded and processes FIFO. Promotes, GC handoffs, and promote_segments serialize through it. This is the correct default — per-job parallelism is a future optimization.

Order-sensitive interactions we need to verify are still safe:

| Interleaving | Safe? |
|---|---|
| `WorkerJob::Promote` (WAL) completes → apply → actor processes a later `WorkerJob::PromoteSegment` for a segment the WAL promote superseded | Safe. Apply phase CAS skips entries whose extent-index entry no longer points at this segment. |
| `WorkerJob::GcHandoff` writes `gc/<ulid>.tmp` → rename `.applied` → coordinator later dispatches `PromoteSegment(ulid)` for the `gc/<ulid>` body | Safe. The GC handoff renames `.tmp` → `.applied` → (finalize) `gc/<ulid>`. `PromoteSegment` only reads the bare `gc/<ulid>` path, which exists by the time the coordinator calls `promote_segment`. |
| Two `PromoteSegment` for the same ULID (coordinator retry) | First completes; second sees `body_path` exists in the prep phase and replies immediately. |
| `PromoteSegment` drain path runs while a write for the same hash lands on the fresh WAL | Safe. The new write inserts an extent-index entry pointing at the *new* WAL; the apply phase's CAS leaves it alone. |

### Crash semantics

Step-by-step on the worker:

| Crash point | State | Recovery |
|---|---|---|
| Before idx write | Unchanged | Coordinator retries promote on next tick (segment still in `pending/` or `gc/`). |
| After idx write, before body commit | `index/<ulid>.idx` exists, `cache/<ulid>.body` missing | `extract_idx` no-ops (idx exists); `promote_to_cache` writes body. Replay idempotent. |
| After body commit, before apply phase | `cache/<ulid>.body` exists, `pending/<ulid>` still exists, extent-index still `BodySource::Local` for that ULID | Coordinator retries. Prep picks `pending/<ulid>` as source (not the idempotent body-exists early-return — that fires only when pending/ and gc/ are both absent). Worker re-parses, inner `extract_idx` / `promote_to_cache` no-op on their committed outputs, apply phase runs normally: CAS, delete pending. |

The prep logic below preserves the invariant that the idempotent early-return fires **only when the source file is absent**. Regression guard: `promote_segment_recovers_mid_apply_crash` in `volume.rs`, and the `HalfPromotePending` SimOp with the `assert_promote_recovery` invariant in the Crash handler of `volume_proptest.rs`. Both exercise the state "cache body committed, apply phase not run, source still present" and assert the retry completes.

Proposed prep logic:

```rust
fn prepare_promote_segment(&self, ulid: Ulid) -> io::Result<PromoteSegmentPrep> {
    let body_path = /* cache/<ulid>.body */;
    let pending_path = /* pending/<ulid> */;
    let gc_path = /* gc/<ulid> */;

    // Source selection matches today's inline code: pending/ > gc/ > body-exists
    // early-return. The ordering is what closes the mid-apply crash window —
    // if the source file survived, the retry must re-run the apply phase.
    let (src_path, is_drain) = if pending_path.try_exists()? {
        (pending_path, true)
    } else if gc_path.try_exists()? {
        (gc_path, false)
    } else if body_path.try_exists()? {
        return Ok(PromoteSegmentPrep::AlreadyPromoted);
    } else {
        return Err(io::Error::other("promote: segment not found"));
    };

    Ok(PromoteSegmentPrep::Job(PromoteSegmentJob { .. }))
}
```

`promote_to_cache` already guards on `body_path.exists()` and `extract_idx` guards on `idx_path.exists()`, so the worker does effectively zero redundant I/O on the retry — it still parses the index once (for the apply phase), but the file copies are no-ops.

### Failure handling

If the worker returns an error:
- Log, pop the parked reply, forward the error to the coordinator.
- On-disk state may have the idx committed and the body missing; the next drain tick's retry is idempotent.

## Free wins folded in

1. **Single parse.** Today: `extract_idx` parses header; `promote_to_cache` re-reads and re-parses; drain-path apply re-reads, re-parses, and verifies. One parse+verify covers all three.
2. **Single body read.** `promote_to_cache` reads bytes via `io::copy` already (uses `copy_file_range` on Linux). We don't change that; we just avoid re-opening the file afterwards for inline section reads by having the worker read inline once if needed.
3. **GC tombstone early-return stays on the worker.** Today the tombstone shortcut parses + verifies on the actor. Moving it onto the worker is a pure win.

## Testing

1. **Unit test: mid-apply crash recovery.** **Landed.** `promote_segment_recovers_mid_apply_crash` in `volume.rs`. Constructs the "cache body committed, apply phase not run" state, drops the volume, reopens, calls `promote_segment`, asserts pending gone + extent-index entry is `Cached` + data read-back unchanged. Confirms the inline code already handles the window; guards against a regression where the offload's prep accidentally early-returns on body-exists.
2. **Proptest: `HalfPromotePending` + `assert_promote_recovery`.** **Landed.** New SimOp runs `extract_idx` + `promote_to_cache` on one pending segment without the apply phase. The `Crash` handler in `crash_recovery_oracle` and `gc_interleaved_oracle` calls `assert_promote_recovery` to retry `promote_segment` on every ULID whose body exists alongside a surviving pending, and asserts the pending is gone after the retry. Interleaved with every other op (writes, GC, sweep/repack).
3. **Unit test: GC tombstone short-circuit.** Verify tombstone handling on the worker: no body/idx written, input `.idx` files deleted on apply.
4. **Unit test: CAS skips superseded entries.** Construct a state where a write supersedes a hash between prep and apply. Verify apply leaves the newer entry intact.
5. **Integration test: concurrent writes during promote_segment.** Proptest / simulation — writes continue to land while worker is mid-promote. Read-your-writes holds throughout.
6. **Bench: write latency during drain tick with multiple pending segments.** Compare p99 write latency before/after for a dd-style burst that produces 4× pending segments.

### TLA+ gap

`HandoffProtocol.tla` collapses `CoordPromote` into one atomic step (line 371–372: *"Modelled as one atomic step: in production the body of promote_segment runs start-to-end on a single actor message"*). Under the offload the sub-steps become observable and the model's abstraction no longer holds. Updating the spec to split `CoordPromote` into `CoordPromoteWriteCache` (worker) and `CoordPromoteApply` (actor), with crash actions between them, is a follow-up — not blocking this landing because the two Rust tests above cover the concrete invariant.

## Landing sequence

All landed on branch `promote-segment-offload`:

1. **Landed (`d861b21`).** Two tests: `promote_segment_recovers_mid_apply_crash` unit test + `HalfPromotePending` SimOp with `assert_promote_recovery` invariant. Regression guards.
2. **Landed (`63f7db1`, combined with step 3).** `WorkerJob::PromoteSegment` / `WorkerResult::PromoteSegment` variants and `PromoteSegmentJob` / `PromoteSegmentResult` / `PromoteSegmentPrep` types.
3. **Landed (`63f7db1`).** Split `Volume::promote_segment` into `prepare_promote_segment` + `actor::execute_promote_segment` + `apply_promote_segment_result`. The inline method still calls all three in sequence on the actor; the worker fn is exercised identically whether it runs inline (direct-Volume callers) or on the worker thread (offload). Single-parse collapse folded in — one `read_and_verify_segment_index` covers the tombstone check, idx extract, body copy, and apply-phase CAS, replacing the previous triple parse.
4. **Landed (this landing).** Route `VolumeRequest::Promote` through the worker: prep on the actor, dispatch `WorkerJob::PromoteSegment` with a parked reply, apply on result receipt, match reply by ULID. `WorkerResult::PromoteSegment` carries the ULID out-of-band so errors also map to the right parked caller (FIFO fallback would mis-attribute under concurrent promotes). `Volume::promote_segment` inline is retained as the direct-Volume entry point for tests that bypass the actor.

## Open questions

- **Signature verify on the hot path.** The current inline code verifies on the actor. The worker path keeps it on the worker — the cost moves, but Ed25519 verify is single-digit ms and not worth caching across calls. Revisit only if profiles say otherwise.
- **Do we want the segment-index cache first?** Step 4 of the actor-offload-plan. Would make the apply-phase CAS cheaper but does not eliminate the actor stall. Tackling promote_segment first is a bigger per-landing win for tail latency; the cache remains a follow-up.
- **Parallel worker pool.** Four pending segments in a drain tick could run concurrently. Deferred until we have numbers that say the single-worker serialization is the next bottleneck.
