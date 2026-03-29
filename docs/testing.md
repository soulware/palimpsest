# Testing

## Property-based tests

The correctness of the volume's crash-recovery model is verified with
property-based tests using [proptest](https://proptest-rs.github.io/proptest/).
These live in `elide-core/tests/volume_proptest.rs`.

Proptest generates random sequences of operations, runs them against the real
volume implementation, and checks invariants after each relevant step.  When a
test fails it automatically shrinks the input to the shortest sequence that
still reproduces the failure — typically a handful of operations.

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

| Op | What it does |
|----|-------------|
| `Write { lba, seed }` | `vol.write(lba, [seed; 4096])` |
| `Flush` | `vol.flush_wal()` — promotes WAL to `pending/` |
| `CompactPending` | `vol.compact_pending()` — merges/deduplicates `pending/` segments |
| `DrainLocal` | Moves all `pending/` files to `segments/` (simulates coordinator upload) |
| `CoordGcLocal` | Runs a coordinator-style GC pass on `segments/` in-process |
| `Crash` | Drops the `Volume` and reopens it (full rebuild from disk) |

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
2. Add it to `arb_sim_op()`.
3. Handle it in both `proptest!` blocks — `ulid_monotonicity` should assert any
   new ULID ordering guarantees; `crash_recovery_oracle` should update the
   oracle if the operation changes visible state.

To increase confidence after a bug fix, add the minimal failing sequence as a
deterministic regression test in `elide-core/src/volume.rs` before verifying
that the proptest also passes.
