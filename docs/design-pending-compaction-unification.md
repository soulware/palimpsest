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

Broaden `repack` to absorb sweep:

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

The drain loop becomes `flush → repack → upload → gc` — one IPC where
there are two today.

## Algorithm

Proposed prep+execute on the actor/worker:

```
prepare_repack:
  segs     = collect_segment_files(pending_dir)
  if segs.is_empty(): return None
  output_ulids = [mint.next() for _ in 0..segs.len() + 1]   # +1 for u_flush peer
  u_flush      = mint.next()
  flush_wal_to_pending_as(u_flush)
  return RepackJob { output_ulids, u_flush, snapshots… }

execute_repack:
  classify every input against the snapshots
  skip inputs that are fully live AND larger than SWEEP_SMALL_THRESHOLD
    (no dead to drop, no peer to combine with)
  bin-pack the rest into 32 MiB live / 8192-entry buckets:
    sort candidates by live_bytes (first-fit-decreasing)
    for each candidate:
      place in the first existing bucket with enough remaining budget
      else start a new bucket
  assign output_ulids[i] to bucket i
  materialise each bucket via materialise_plan
```

Skip rule details:
- `live_bytes == total_bytes` AND `live_bytes > SWEEP_SMALL_THRESHOLD`
  → skip (no dead to drop, no peer to combine with).
- `live_bytes == total_bytes` AND `live_bytes ≤ SWEEP_SMALL_THRESHOLD`
  → eligible (today's sweep behaviour).
- `live_bytes < total_bytes` → eligible regardless of size.

A bucket with exactly one fully-live small input would be a no-op
rewrite; the bucketer drops it the same way today's sweep skips
single-segment buckets.

### Why `segs.len() + 1` output ULIDs

Bin-packing is at most N→N: every input is placed in exactly one
bucket, and we never split an input across buckets. The worst case
(every input becomes a solo bucket) gives exactly N output segments,
not more.

The +1 covers the WAL-flush peer. `prepare_repack` mints output ULIDs
*before* it flushes the open WAL into `pending/<u_flush>`, because the
ordering invariant `u_repack_i < u_flush < next WAL ULID` requires
output ULIDs to sort below `u_flush` — which means they must be minted
first. The flush may then add one new pending file, and that file can
itself be eligible for repack (e.g. a WAL containing multiple writes
to the same LBA produces a flushed segment with hash-dead bodies).
Pre-mint upper bound: `pre_flush_segs + 1`.

The actor is single-threaded between prep and the worker run, so no
new pending segments can appear; N+1 is a tight bound, not a guess.
ULID minting advances a `u128` counter — unused ULIDs cost nothing
beyond a `Vec` slot.

There is **no separate "filler" rule**. Today's sweep needs one
because its selection is narrow (`live_bytes ≤ 16 MiB` only) — the
filler grabs one larger eligible segment to top up otherwise-underfilled
outputs. In the unified pass every dead-bearing segment is already in
the candidate set, so plain best-fit bin-packing covers all the cases:
- Multiple smalls → one packed bucket
- One mid-sized → its own solo bucket
- Small + mid-sized that fit together → one packed bucket

## Apply path

Today:
- `apply_sweep_result` consumes `{ new_ulid: Option<Ulid>,
  out_entries, inputs }` (one output, many inputs).
- `apply_repack_result` consumes `Vec<RepackedSegment>` (many outputs,
  one input each).

Proposed: keep repack's shape — `Vec<RepackedBucket>` where each
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
- Broadened `prepare_repack` / `execute_repack` / `apply_repack_result`
  with the bucketer absorbing sweep's selection + packing.
- `Volume::repack` keeps its current signature; semantics broaden.
- `VolumeRequest::Repack` keeps its name and shape (no payload, returns
  `CompactionStats`).

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

## Decisions

- **Naming**: keep `repack`. Already in use across CLI, IPC, docs;
  the broader semantics fit cleanly without a rename.
- **WAL-flush peer ULID count**: pre-mint `segs.len() + 1`, same
  pattern that landed in #287's follow-up fix. ULID minting is cheap.
- **Coordinator drain order**:
  `flush → repack → upload → gc`. The explicit `flush` IPC stays
  separate from `repack`'s internal flush — `flush` runs on its own
  cadence and covers the readonly-volume case where `repack` is
  skipped.

## Open

- **Per-bucket parallelism**. With N independent output buckets the
  worker could materialise them in parallel. Out of scope for this
  proposal; revisit once the unification lands.

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
- Add bucketing branch to `execute_repack`: ~150 lines.
- Update coordinator drain loop to drop the `sweep_pending` IPC call:
  ~10 lines.
- Update tests: replace `vol.sweep_pending()` calls with
  `vol.repack()`. Test fixtures stay essentially unchanged because both
  passes already produce the same shape on disk.
- Doc updates across architecture.md, formats.md, operations.md.

Net likely negative — sweep's selection + bucketing logic is the
larger of the two and absorbs cleanly into the broadened repack.
