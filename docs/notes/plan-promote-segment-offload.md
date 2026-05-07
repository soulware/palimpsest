---
status: landed
related: [plan-actor-offload.md, plan-promote-offload.md]
---

# Offload `promote_segment` off the volume actor

Step 4 of [plan-actor-offload.md](plan-actor-offload.md). Landed on branch `promote-segment-offload`. The coordinator issues `promote <ulid>` IPC for every confirmed S3 upload (drain output) and every GC output, so this fired repeatedly during drain ticks and blocked queued writes for 10–100 ms each.

## Decision

Same three-phase shape as the other offloads. `WorkerJob::PromoteSegment` carries the source path and verifying key; the worker reads + verifies the index **once**, emits `index/<ulid>.idx`, copies body to `cache/<ulid>.{body,present}`, optionally writes `.delta`, and returns parsed entries + body-section start + inline bytes. Apply phase on the actor runs the extent-index CAS (`Local → Cached`) plus `pending/<ulid>` delete (drain) or input `.idx` cleanup (GC).

Folded into the same landing: collapse the previous triple re-parse of the segment index down to one `read_and_verify_segment_index` shared across tombstone check, idx extract, body copy, and apply-phase CAS.

## Crash recovery — load-bearing detail

The idempotent body-exists early-return must fire **only when the source file is absent**. Otherwise a crash between body commit and apply phase would leave the extent index pointing at `BodySource::Local` for the WAL ULID, the cache body committed, and the source `pending/<ulid>` still present — the retry would skip the apply phase.

Source selection: `pending/<ulid>` > `gc/<ulid>` > body-exists early-return. If the source survived, the retry must re-run apply.

Regression guards: `promote_segment_recovers_mid_apply_crash` (unit) and `HalfPromotePending` SimOp + `assert_promote_recovery` invariant in the proptest crash handler.

## Apply-phase CAS

`WorkerResult::PromoteSegment` carries the ULID out-of-band so errors map to the right parked caller (FIFO fallback would mis-attribute under concurrent promotes). The CAS skips entries whose extent-index entry no longer points at this segment — concurrent writes between prep and apply are safe.

## Open follow-ups (not blocking)

- TLA+ spec collapses `CoordPromote` into one atomic step. Under the offload the sub-steps are observable; updating the spec to split into `CoordPromoteWriteCache` (worker) + `CoordPromoteApply` (actor) with crash actions between them is a follow-up.
- Parallel worker pool — multiple pending segments in a drain tick could run concurrently. Deferred until there's evidence single-worker serialisation is the bottleneck.
