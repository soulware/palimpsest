# Pending compaction unification

Collapse `sweep_pending` and `repack` into one drain-phase pass. Status:
proposal.

## Motivation

After the redact→repack fold (#287), `pending/`-side compaction has two
passes that differ only in **bucketing policy**:

|              | Selection                                  | Output count | Side effects                   |
|--------------|--------------------------------------------|--------------|--------------------------------|
| **Sweep**    | `live_bytes ≤ 16 MiB`, ≥2 inputs in bucket | 1            | Drops dead during rewrite      |
| **Repack**   | any hash-dead body                         | N (per-input)| Drops dead by construction     |

Both:
- Pre-mint `u_op_i < u_flush`, flush WAL into `pending/<u_flush>`.
- Snapshot `lbamap` + `extent_index` into an `Arc`.
- Reuse the same `segment_classify::classify_entry` and
  `rewrite_plan::PlanOutput` + `rewrite_apply::materialise_plan`
  infrastructure.
- Use the same `insert_if_newer` lbamap apply path (since #283).

Per drain tick, the coordinator currently issues two IPC calls
(`sweep_pending` then `repack`), each running its own
prepare/execute/apply trio. This pays for two WAL flushes, two
classifier passes per pending segment, and two snapshot clones for what
is structurally one operation.

There is also a corner case neither pass cleanly handles today: a
medium dense segment (live_bytes > 16 MiB but still < 32 MiB) gets
solo-rewritten by repack instead of bin-packed. A unified pass with a
single bucketing strategy can pack this case.

## Proposal

One pass — call it **compact** — that:

1. Selects every pending segment whose classification produces a
   non-trivial output (i.e. anything other than every entry classifying
   `FullyLive`), **or** which is small enough to pack with at least one
   other input.
2. Bin-packs candidates into output buckets sized to 32 MiB live and
   8192 entries (the existing sweep target / WAL flush thresholds),
   spilling oversize candidates to their own solo bucket.
3. Materialises each bucket as a separate output segment under a fresh
   ULID.
4. Applies all outputs through the same incremental
   `insert_if_newer`/`replace_if_matches` path that repack already uses.

The drain loop becomes `flush → compact → upload → gc` — one IPC where
there are two today.

## Algorithm

Proposed prep+execute on the actor/worker:

```
prepare_compact:
  segs     = collect_segment_files(pending_dir)
  if segs.is_empty(): return None
  output_ulids = [mint.next() for _ in 0..segs.len() + 1]   # +1 for u_flush peer
  u_flush      = mint.next()
  flush_wal_to_pending_as(u_flush)
  return CompactJob { output_ulids, u_flush, snapshots… }

execute_compact:
  classify every input against the snapshots
  skip inputs whose classification is all-FullyLive and live_bytes > SWEEP_SMALL_THRESHOLD
  bucket the rest:
    1. small set := { c | live_bytes ≤ SWEEP_SMALL_THRESHOLD }
       sort by live_bytes ascending; greedy-fill buckets up to
       (SWEEP_TARGET_LIVE bytes, SWEEP_ENTRY_CAP entries)
    2. solo set := the remainder (large with non-trivial classification)
       each becomes its own single-input bucket
  assign output_ulids[i] to bucket i
  materialise each bucket via materialise_plan
```

Skip rule details:
- `live_bytes == total_bytes` AND `live_bytes > SWEEP_SMALL_THRESHOLD`
  → skip (no dead to drop, no peer to combine with).
- `live_bytes == total_bytes` AND `live_bytes ≤ SWEEP_SMALL_THRESHOLD`
  → eligible for the small set (today's sweep behaviour).
- `live_bytes < total_bytes` → eligible regardless of size.

A single-segment small bucket reduces to a no-op rewrite (today's
sweep skips it explicitly). Same exit condition in unified.

## Apply path

Today:
- `apply_sweep_result` consumes `{ new_ulid: Option<Ulid>,
  out_entries, inputs }` (one output, many inputs).
- `apply_repack_result` consumes `Vec<RepackedSegment>` (many outputs,
  one input each).

Proposed: keep repack's shape — `Vec<CompactedBucket>` where each
bucket carries its own output ULID, output entries, and the list of
input segments it consumed. Sweep's single-output shape becomes the
1-element case of this.

`insert_if_newer` already keys on the claimant ULID so the per-input
CAS gating that sweep and repack use today carries over unchanged.

## What gets removed

- `VolumeRequest::SweepPending`, `SweepJob`, `SweepResult`,
  `SweptInput` types
- `prepare_sweep`, `execute_sweep`, `apply_sweep_result`,
  `Volume::sweep_pending`
- `start_sweep`, `parked_sweep`, `WorkerJob::Sweep`,
  `WorkerResult::Sweep` in the actor
- The coordinator's `sweep_pending` IPC client + drain-step 2 call

What replaces them:
- `prepare_compact` / `execute_compact` / `apply_compact_result`
  (probably built by extending `repack` rather than as new files)
- `Volume::compact_pending` synchronous wrapper for tests
- `VolumeRequest::Compact` IPC (renamed from Repack, or kept as Repack
  with broader semantics — open question)

## Tradeoffs

**Pro**
- One pass type, one classifier scan per segment per drain, one WAL
  flush, one snapshot clone.
- Closes the "dense, mid-sized, has-dead" packing hole today's
  sweep+repack split misses.
- Smaller surface area: tests against one IPC, one apply path.

**Con**
- Bigger pass: a single bug propagates further. Mitigated by the fact
  that all the primitives are already shared infrastructure.
- The bucketer logic gets one more case (today: pack smalls, treat
  large/dense as filler if it fits). Unified bucketer also has to
  handle "large with dead → solo bucket."

## Open questions

1. **Naming.** Keep `repack` as the broader name, or rename to `compact`?
   - `repack` already in use across CLI, IPC, docs.
   - `compact` aligns with the verb the docs use for the umbrella
     ("pending compaction").
   - Could keep `repack` and just broaden it; rename is a separate concern.

2. **Sweep's filler rule.** Today sweep takes one filler from the
   `live_bytes > 16 MiB` set if budget remains. Should unified preserve
   that? It's a small win on packing density that comes nearly for
   free.

3. **WAL flush peer ULID count.** PR-side fix already pre-mints
   `segs.len() + 1` for the WAL-flush peer. Same story here.

4. **Coordinator drain order.** Currently
   `flush → sweep → repack → upload → gc`. Becomes
   `flush → compact → upload → gc`. Verify that the `flush` IPC step
   is still needed given that `prepare_compact` flushes the WAL itself
   (it is — the explicit `flush` IPC is independent of compaction; it
   covers the readonly-volume case where sweep/repack are skipped).

5. **Per-bucket parallelism.** Out of scope for this proposal but
   worth a note: with N independent output buckets the worker could
   materialise them in parallel. Today sweep is single-output so the
   question doesn't arise; unified makes it natural.

## Out of scope

- `delta_repack_post_snapshot` (different selection criterion,
  snapshot-pinned reader; stays its own pass).
- Coordinator-side GC (operates on already-uploaded S3 segments).
- The `density_threshold` config knob (already removed for repack
  in #287; coordinator GC retains its own).

## Sketch of the diff

Approximate, for sizing:
- Delete `prepare_sweep` / `execute_sweep` / `apply_sweep_result`
  and the `Sweep*` types: ~600 lines.
- Add bucketing branch to `execute_repack` (or rename to
  `execute_compact`): ~150 lines.
- Update coordinator drain loop to drop the `sweep_pending` IPC call:
  ~10 lines.
- Update tests: replace `vol.sweep_pending()` calls with
  `vol.compact_pending()` (or `vol.repack()` if we keep the name);
  test fixtures stay essentially unchanged because both passes
  already produce the same shape on disk.
- Doc updates across architecture.md, formats.md, operations.md.

Net likely negative — sweep's selection + bucketing logic is the
larger of the two and absorbs cleanly into the broadened repack.
