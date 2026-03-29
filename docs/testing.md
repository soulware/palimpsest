# Testing

## Property-based tests

The correctness of the volume's crash-recovery model is verified with
property-based tests using [proptest](https://proptest-rs.github.io/proptest/).
These live in `elide-core/tests/volume_proptest.rs`.

Proptest generates random sequences of operations, runs them against the real
volume implementation, and checks invariants after each relevant step.  When a
test fails it automatically shrinks the input to the shortest sequence that
still reproduces the failure — typically a handful of operations.

### Correctness invariants

The tests are designed to protect these invariants:

1. **ULID total order is correctness.** `rebuild_segments` applies segments
   oldest-first by ULID.  Any segment with a ULID that violates monotonicity
   can silently shadow a newer write.  This is not a cleanliness property — it
   is the reason crash recovery is correct.

2. **WAL ULID is pre-assigned at WAL creation.** When `compact_pending` or
   coordinator GC runs, the current WAL already has a fixed ULID.  Any new
   segment produced by those operations must have a ULID `< wal_ulid`, not
   `> wal_ulid`.  The mechanism: `compact_pending` uses `max(candidate_ULIDs)`
   as output; coordinator GC uses `max(inputs).increment()`.  Both are
   guaranteed to be below the current WAL ULID because all `pending/` and
   `segments/` files were created before the current WAL was opened.

3. **`segments/` ↔ S3 invariant.** The coordinator only touches `segments/`,
   never `pending/`.  This boundary is what makes invariant 2 hold: coordinator
   inputs are always from a prior write epoch.

4. **Snapshot floor.** Segments at or below the latest snapshot ULID are
   frozen — `compact_pending` and `compact_volume` must never modify or delete
   them.

### The two properties

**ULID monotonicity** (`ulid_monotonicity`)

Every segment file produced by a volume operation must have a ULID strictly
greater than all segment ULIDs that existed before the operation:

- `flush_wal` → new segment ULID > all pre-existing ULIDs
- `compact_pending` → output ULID > all pre-existing ULIDs (including the
  segments it consumed)
- Simulated coordinator GC → output ULID > max(consumed input ULIDs)

This property is the reason crash recovery is safe: `rebuild_segments` applies
segments in ULID order oldest-first, so a higher ULID always wins for any given
LBA.  Violating it means an older compacted segment can shadow a newer write.

**Crash-recovery oracle** (`crash_recovery_oracle`)

Maintains a ground-truth `HashMap<lba, data>` tracking the most recent write
to each LBA.  After every `Crash` operation (drop + reopen), every LBA in the
oracle is read back and compared to the expected value.

This directly tests end-to-end correctness rather than the mechanism.  It
catches any scenario where a combination of operations produces a stale or
missing read after recovery, regardless of whether the ULID ordering invariant
looks intact from the outside.

### The simulation model

Each test runs a random sequence of `SimOp` values against a single fork
directory:

| Op | What it does | Oracle effect |
|----|-------------|---------------|
| `Write { lba, seed }` | `vol.write(lba, [seed; 4096])` | `oracle.insert(lba, [seed; 4096])` |
| `Flush` | `vol.flush_wal()` — promotes WAL to `pending/` | none (write already recorded) |
| `CompactPending` | `vol.compact_pending()` — merges/deduplicates `pending/` segments | none (no data change) |
| `DrainLocal` | Moves all `pending/` files to `segments/` (simulates coordinator upload) | none |
| `CoordGcLocal` | Runs a coordinator-style GC pass on `segments/` in-process | none (no data change) |
| `Crash` | Drops the `Volume` and reopens it (full rebuild from disk) | assert all oracle LBAs match |

`DrainLocal` is needed before `CoordGcLocal` has material to work with, just
as in production the coordinator only compacts segments that have been
uploaded.  The proptest engine discovers on its own which interleavings are
interesting.

`CoordGcLocal` picks the two oldest segments in `segments/`, merges their
entries, writes an output with `ULID = max(inputs).increment()`, and deletes
the inputs — the same algorithm as the real coordinator GC in
`elide-coordinator/src/gc.rs`.

### Bug found by these tests

During initial implementation, `crash_recovery_oracle` immediately found a bug
in `compact_pending`.

**Failing sequence (shrunk by proptest):**

```
Write(lba=0, seed=0)   -- H0
Write(lba=2, seed=1)   -- H1
Flush                  -- S1: DATA(lba=0,H0), DATA(lba=2,H1)
Write(lba=0, seed=1)   -- H1 again → dedup REF in WAL
CompactPending         -- S1' created with mint.next() = U3
Write(lba=2, seed=2)   -- H2
Flush                  -- S2: WAL ULID = U2 (pre-assigned at WAL creation)
Crash                  -- rebuild: S2(U2) then S1'(U3) → lba=2 returns [1] not [2]
```

**Root cause:** `compact_pending` used `mint.next()` to name its output
segment.  Because the current WAL's ULID (U2) was pre-assigned at WAL creation
time, `mint.next()` during compaction produced U3 > U2.  When the WAL was
later flushed, it inherited U2.  Rebuild processes in ULID order, so the
compact output (U3) was applied after the WAL flush segment (U2) and silently
overwrote lba=2 with stale data.

**Fix:** `compact_pending` now uses `max(candidate_ULIDs)` as the output ULID,
atomically replacing the max-ULID candidate file via `.tmp` rename.  Since all
`pending/` segments were created before the current WAL was opened, their ULIDs
are strictly less than the WAL ULID.  The output therefore always sorts below
the eventual WAL flush segment.

### Extending the tests

To add a new operation:

1. Add a variant to `SimOp` in `tests/volume_proptest.rs`.
2. Add it to `arb_sim_op()` with an appropriate weight.
3. Handle it in **both** proptest blocks:
   - In `ulid_monotonicity`: capture `ulids_before`, then after the op check
     that every ULID in `after.difference(&ulids_before)` is `> max_before`.
   - In `crash_recovery_oracle`: update `oracle` if the op changes visible LBA
     state; otherwise just execute.
4. If the op can produce no-op results (nothing to compact, no candidates,
   etc.), make sure the no-op path is handled without panicking.

**What to assert for each operation type:**

- **State-changing** (e.g. Write): update the oracle immediately.
- **Structural** (e.g. Flush, Compact, Drain, GC): no oracle update, but assert
  ULID ordering in `ulid_monotonicity`.
- **Recovery** (Crash): assert full oracle match after reopen.
- **New feature ops**: ask two questions — does this op change which data is
  visible? If yes, update the oracle.  Does it produce new segment files?  If
  yes, add a ULID ordering assertion.

To increase confidence after a bug fix, add the minimal failing sequence as a
deterministic regression test in `elide-core/src/volume.rs` before verifying
that the proptest also passes.

### Future dimensions

The current tests focus on crash-recovery correctness for a single fork.
Other dimensions worth adding:

**Live read-your-writes oracle.** The crash-recovery oracle only asserts after
`Crash`.  Asserting `vol.read(lba) == last_write` after every `Write` would
catch in-memory state bugs (lbamap / extent_index corruption, wrong WAL offset
tracking).  The marginal value over the existing unit tests is lower here, but
it would close the gap for unusual sequences that unit tests don't cover.

**Fork ancestry isolation oracle.** The most compelling gap.  The layered read
path with ULID cutoffs is the most complex logic in the volume and is not
exercised by the current proptest at all.  A fork oracle would run sequences
like:

```
BaseWrite, BaseWrite, Snapshot, ForkFromBase,
ChildWrite, ChildWrite, Crash(child), ...
```

...and maintain two independent `HashMap<lba, data>` views — the base's state
at snapshot time and the child's own writes on top — asserting after every
`Crash` that:
- ancestral LBAs not overwritten by the child read back base data
- child writes shadow base data at the same LBA
- post-branch base writes are invisible to the child

**Snapshot floor invariant.**  Adding a `Snapshot` op and asserting that
`compact_pending` / `compact` never modifies segments at or below the snapshot
ULID would cover that invariant beyond the two fixed-sequence unit tests that
exist today.

**Coordinator GC interleaved with live writes.**  The current `CoordGcLocal`
only runs after `DrainLocal` has moved segments out of `pending/`.  A more
realistic simulation would interleave GC with live `Flush` operations while
segments span both `pending/` and `segments/`, stressing the boundary that the
`max(inputs).increment() < new volume ULIDs` ordering invariant is designed to
protect.
