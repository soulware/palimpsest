# Finding: sweep loses partial-death output to lbamap claimant guard

## Symptom

`gc_interleaved_oracle` proptest fails on plain `main` (`06c0881`) with a
shrunk sequence whose minimal isolated form is:

```text
Write LBA 0; flush_wal           // seeds pending with one segment
WriteMulti(43..47, seed=0)       // wm1 — 4 blocks, lbamap[43..47]=wm1_hash
WriteMulti(42..44, seed=0)       // wm2 — 2 blocks, overwrites lbamap[43]
sweep_pending
repack
crash + reopen
read LBA 45                       // returns all zeros instead of wm1 block 2
```

Reproducers on this branch:

- `writemulti_overlap_sweep_then_repack_regression` (minimal, ~120 LoC)
- `gc_interleaved_writemulti_overlap_regression` (matches the proptest
  shrunk sequence verbatim)

Both fail when sweep then repack run in that order. Sweep alone passes;
repack alone passes; together fails.

## Root cause

`apply_sweep_result` cannot install its own output into the lbamap
because the existing claimant on those LBAs is *already* `u_flush` —
the WAL-flush peer that `prepare_sweep` itself just minted and consumed
as a sweep input.

Walk:

1. `prepare_sweep` mints `u_sweep`, then `u_flush` (`u_sweep < u_flush`),
   then calls `flush_wal_to_pending_as(u_flush)`. That flush bumps lbamap
   claimants for every entry it owns from `wal_ulid` → `u_flush` via
   `set_claimant_if_matches` (mod.rs:2122).
2. `execute_sweep` classifies the flushed segment. wm1 has 3 of 4 blocks
   live (LBA 43 was overwritten by wm2) → `PartialDeath`. The output
   plan emits a Canonical record for wm1 (preserves the body for dedup)
   and a `Run` record for the live sub-run (LBA 44..47, payload offset
   1). Materialisation slices wm1's body into a fresh Data entry with a
   new hash (`acce0fb0`) at LBA 44..47.
3. `apply_sweep_result` calls
   `lbamap.insert_if_newer(44, 3, acce0fb0_hash, u_sweep)`.
   `insert_if_newer` is the protection added by PR #277
   (`docs/design-lbamap-claimant-tracking.md`): it refuses to install
   when an existing entry has `claimant_ulid >= caller's claimant`. The
   existing claimant is `u_flush > u_sweep`, so the install is blocked.
4. lbamap[44..47] continues to point at `(wm1_hash, claimant=u_flush)`.
   Reads still resolve correctly at this point because
   `extent_index[wm1_hash]` was repointed at `u_sweep_output` (the
   merged segment) via the Canonical entry, and the lbamap entry tracks
   `payload_block_offset=1` so block math still lands on the right
   bytes.
5. `repack` runs next. The merged segment has a Canonical wm1 entry
   whose hash isn't in `lba_referenced_hashes` (lbamap points at
   `wm1_hash` but the live-set is built from current claimants, so this
   hash is dead by repack's accounting), so
   `live_bytes_estimate < total_bytes` and repack rewrites the segment.
6. While rewriting, repack classifies the sliced Data entry
   (`acce0fb0`, LBA 44..47): lbamap[44..47] reports `wm1_hash`, not
   `acce0fb0`. `matching_blocks == 0` → `DropAndRemoveHash`.
7. The new segment loses the Data LBA 44..47 entry. lbamap rebuild from
   disk after crash has no entry for LBAs 44, 45, 46 → reads return
   zero.

Confirmation via segment dumps mid-sequence:

```text
AFTER_SWEEP <u_sweep_output>: 4 entries
  [0] Inline lba=0..1 hash=b6fb73fc          (zero)
  [1] CanonicalData hash=366db958 stored=16384  (wm1 body preserved)
  [2] Data lba=44..47 hash=acce0fb0 stored=12288  (sliced live sub-run) ← LOST
  [3] Data lba=42..44 hash=15843321 stored=8192   (wm2 head)

AFTER_REPACK <u_repack_output>: 3 entries
  [0] Inline lba=0..1 hash=b6fb73fc
  [1] CanonicalData hash=366db958 stored=16384
  [2] Data lba=42..44 hash=15843321 stored=8192
```

## Why repack-alone doesn't trigger this

In SKIP_SWEEP, `prepare_repack` flushes the WAL into a pending segment
just like `prepare_sweep` does, and the same claimant-bump occurs. But
repack iterates per-segment and that flushed segment has *no hash-dead
entries* (wm1 is fully live in lbamap, since the partial-overwrite by
wm2 left lbamap[44..47] correctly pointing at wm1 with
`payload_block_offset=1`), so `live_bytes_estimate == total_bytes` and
the segment is skipped. No slicing, no
insert_if_newer call to be rejected.

The sliced output is only produced when sweep is forced to merge two
inputs (≥2 candidates), which gives it both the prior pending segment
and the just-flushed WAL peer to bin-pack together.

## What's wrong with the design here

`insert_if_newer`'s strict-newer guard (PR #277) is correct against
*concurrent* writers: if a live writer re-claimed an LBA between the
structural op's snapshot and apply, the live writer must win. The
guard implements that by demanding the new claimant be strictly newer.

But sweep's `u_sweep_output < u_flush` is intentional — the output ULID
is meant to sort below the WAL-flush peer so on rebuild, sweep is
applied before the flush peer's leftovers (if any survive). The guard
treats `u_flush` (a sweep input that sweep is consuming and deleting on
disk) as a "concurrent claimant that must be preserved", which is the
opposite of what's true: that input is being *replaced* by the sweep
output.

The semantic gap: `insert_if_newer` doesn't know which existing
claimants belong to the sweep's own input set. A claimant that matches
a consumed input is not a concurrent writer — it's a stale anchor for
data the sweep is rewriting under a fresh ULID.

## Proposed fix shape (open for discussion)

Add a sibling op that takes the consumed-inputs set explicitly:

```rust
lbamap.insert_consuming_inputs(start, len, hash, new_claimant, &input_ulids)
```

Semantics: behave as `insert_if_newer`, but treat any existing entry
whose `claimant_ulid` is in `input_ulids` as if it were absent
(unconditionally overridable). Concurrent writers (claimants outside
the input set) still win as before.

Both `apply_sweep_result` and `apply_repack_result` already build an
`input_ulids: HashSet<Ulid>` for their CAS gates on the extent index
— the same set is the right one to pass to lbamap.

`apply_repack_result` should adopt the same op even though our test
doesn't surface a failure there: the same WAL-flush-peer setup applies
(prepare_repack mints output ULIDs *then* flushes WAL into a higher
`u_flush`), and a future reproducer with a hash-dead WAL flush (multiple
writes to the same LBA in one open WAL) would hit the same bug in
repack alone.

`apply_plan_apply_result` (GC apply) is also vulnerable for the same
structural reason: `gc_checkpoint` mints `u_gc < u_flush`, the flush
bumps lbamap claimants to `u_flush`, and a GC plan output with a fresh
sliced hash gets blocked by the strict-newer guard. The latent bug
surfaces only when a downstream structural pass classifies against the
stale lbamap, which is why existing GC tests didn't catch it — the
in-memory drift was even explicitly documented as "benign" in
`assert_lbamap_consistent`. With the override threaded through, the
divergence no longer occurs.

## Pre-existing nature

Reproduced on `06c0881` (current main). Also reproduces at `015d38e`
(#287, fold-redact merge); did not bisect further because pre-#287
`repack` had a `min_live_ratio: f64` parameter and would need API
shimming. Not introduced by the recent perf PR stack (#293–#296).
