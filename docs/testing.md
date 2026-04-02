# Testing

## Property-based tests

The correctness of the volume's crash-recovery model is verified with
property-based tests using [proptest](https://proptest-rs.github.io/proptest/).
The main suite lives in `elide-core/tests/volume_proptest.rs`; fork ancestry
isolation is in `elide-core/tests/fork_proptest.rs`; deterministic integration
tests for GC ordering and `ReadonlyVolume` live in `gc_ordering_test.rs` and
`readonly_volume_test.rs` respectively.

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

2. **WAL ULID is pre-assigned at WAL creation.** When `sweep_pending` or
   coordinator GC runs, the current WAL already has a fixed ULID.  Any new
   segment produced by those operations must have a ULID `< wal_ulid`, not
   `> wal_ulid`.  The mechanism: `sweep_pending` uses `max(candidate_ULIDs)`
   as output; coordinator GC uses `max(inputs).increment()`.  Both are
   guaranteed to be below the current WAL ULID because all `pending/` and
   `segments/` files were created before the current WAL was opened.

3. **`segments/` ↔ S3 invariant.** The coordinator only touches `segments/`,
   never `pending/`.  This boundary is what makes invariant 2 hold: coordinator
   inputs are always from a prior write epoch.

4. **Snapshot floor.** Segments at or below the latest snapshot ULID are
   frozen — `sweep_pending` and `repack` must never modify or delete
   them.  Covered by the `Snapshot` SimOp in `ulid_monotonicity`: after
   every `Snapshot` the test records the floor ULID and asserts after every
   subsequent `SweepPending` or `Repack` that no frozen segment was deleted.

### The two properties

**ULID monotonicity** (`ulid_monotonicity`)

Every segment file produced by a volume operation must have a ULID strictly
greater than all segment ULIDs that existed before the operation:

- `flush_wal` → new segment ULID > all pre-existing ULIDs
- `sweep_pending` → output ULID > all pre-existing ULIDs (including the
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
| `SweepPending` | `vol.sweep_pending()` — merges/deduplicates `pending/` segments | none (no data change) |
| `Repack` | `vol.repack(0.9)` — density pass on `pending/` + `segments/` | none (no data change) |
| `DrainLocal` | Moves all `pending/` files to `segments/` (simulates coordinator upload) | none |
| `CoordGcLocal { n }` | Runs a coordinator-style GC pass on `segments/` in-process, merging `n` segments (2–5) | none (no data change) |
| `Crash` | Drops the `Volume` and reopens it (full rebuild from disk) | assert all oracle LBAs match |
| `Snapshot` | `vol.snapshot()` — records branch point; sets snapshot floor for sweep/repack assertions | tracks floor ULID |
| `ReadUnwritten` | Reads LBA 64 (always outside write range) | assert all-zero bytes |

`DrainLocal` is needed before `CoordGcLocal` has material to work with, just
as in production the coordinator only compacts segments that have been
uploaded.  The proptest engine discovers on its own which interleavings are
interesting.

`CoordGcLocal { n }` picks the `n` oldest segments in `segments/` (proptest
generates `n` in the range 2–5), merges their entries, writes an output with
`ULID = max(inputs).increment()`, and deletes the inputs — the same algorithm
as the real coordinator GC in `elide-coordinator/src/gc.rs`.

### Bug found by these tests

During initial implementation, `crash_recovery_oracle` immediately found a bug
in `sweep_pending`.

**Failing sequence (shrunk by proptest):**

```
Write(lba=0, seed=0)   -- H0
Write(lba=2, seed=1)   -- H1
Flush                  -- S1: DATA(lba=0,H0), DATA(lba=2,H1)
Write(lba=0, seed=1)   -- H1 again → dedup REF in WAL
SweepPending         -- S1' created with mint.next() = U3
Write(lba=2, seed=2)   -- H2
Flush                  -- S2: WAL ULID = U2 (pre-assigned at WAL creation)
Crash                  -- rebuild: S2(U2) then S1'(U3) → lba=2 returns [1] not [2]
```

**Root cause:** `sweep_pending` used `mint.next()` to name its output
segment.  Because the current WAL's ULID (U2) was pre-assigned at WAL creation
time, `mint.next()` during compaction produced U3 > U2.  When the WAL was
later flushed, it inherited U2.  Rebuild processes in ULID order, so the
compact output (U3) was applied after the WAL flush segment (U2) and silently
overwrote lba=2 with stale data.

**Fix:** `sweep_pending` now uses `max(candidate_ULIDs)` as the output ULID,
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
- **Structural** (e.g. Flush, SweepPending, Repack, Drain, GC): no oracle update, but assert
  ULID ordering in `ulid_monotonicity`.
- **Recovery** (Crash): assert full oracle match after reopen.
- **New feature ops**: ask two questions — does this op change which data is
  visible? If yes, update the oracle.  Does it produce new segment files?  If
  yes, add a ULID ordering assertion.

To increase confidence after a bug fix, add the minimal failing sequence as a
deterministic regression test in `elide-core/src/volume.rs` before verifying
that the proptest also passes.

### Known gaps

These are gaps in the current simulation model that could allow bugs to go
undetected.  They are documented here so they are not forgotten.

**`crash_recovery_oracle` does not delete consumed GC segments.**  *(Fixed.)*
After `CoordGcLocal`, the helper returns the paths of old input segments to be
deleted.  `ulid_monotonicity` collects and removes them correctly.
`crash_recovery_oracle` was discarding the returned paths (`let _ = ...`), so
consumed segment files accumulated indefinitely and were never removed.  This
meant the oracle never ran in the post-GC steady state — where the old segments
are gone and reads must go entirely through the new compacted output — which is
exactly the state the handoff protocol is designed to make safe.  Fixed by
collecting and deleting the consumed paths after `apply_gc_handoffs`, matching
the real coordinator protocol.

**`Repack` SimOp is entirely absent.**  *(Fixed, two bugs found.)*
`vol.repack(min_live_ratio)` is the volume-level density pass.  It iterates
both `pending/` and `segments/`, rewrites sparse segments, and deletes the
originals.  Adding `Repack` to the simulation immediately found two bugs:

1. `repack()` used `mint.next()` for output ULIDs, producing values above the
   current WAL ULID.  On rebuild the repack output sorted after the WAL flush
   segment and overwrote newer data with stale data.  Fixed by reusing
   the source segment's own ULID as the output ULID — the same approach as
   `sweep_pending`.

2. When the source segment is in `pending/`, the output path equals the source
   path.  The `rename(.tmp → pending/<ulid>)` atomically replaced the source,
   but the subsequent `remove_file(seg_path)` deleted the newly-written repack
   output.  Fixed by skipping the delete when the source is in `pending/` (the
   rename already replaced it in-place).

**Coordinator multi-segment sweep not modelled.**  *(Fixed.)*  `CoordGcLocal`
now takes an `n: usize` parameter; the proptest generates `n` in the range 2–5.
A GC merge ordering bug was found and fixed during the implementation: without
sorting GC-output segments before regular segments, a higher-ULID GC output
containing older writes could shadow newer data from a lower-ULID regular
segment during rebuild.

**Multi-LBA writes and reads never issued.**  Every `Write` SimOp writes exactly
4096 bytes (one LBA) and every `ReadUnwritten` reads one LBA.  `vol.write()` and
`vol.read()` accept any non-zero multiple of 4096, and the `read_extents` path
has distinct logic for multi-LBA extents: it iterates `lbamap.extents_in_range()`
and computes `payload_block_offset` to seek into a compressed or uncompressed
body.  That arithmetic and the partial-range read path are never exercised.  A
`WriteMulti { lba, count, seed }` SimOp would cover it and would also surface any
mixed live/dead partial-extent accounting errors in `sweep_pending` and `repack`.

**Dedup path not reliably triggered.**  The DEDUP_REF write path fires only when
two LBAs hold identical data.  With the current `lba: 0..8, seed: any::<u8>()`
strategy the dedup path may or may not be reached on any given run.  The
corruption bug fixed in `sweep_pending` (dead DEDUP_REF incorrectly evicting a
live DATA extent) was found by chance rather than by design.  A `DedupWrite`
SimOp that explicitly writes the same seed to two different LBAs would guarantee
the dedup and dead-REF paths are exercised on every run.

**Actor `Snapshot` op absent.**  `VolumeHandle::snapshot()` sends a
`VolumeRequest::Snapshot` through the actor channel, which flushes the WAL,
opens a new one, and republishes the snapshot.  The actor proptest has no
`ActorOp::Snapshot` variant, so the read-your-writes guarantee after a snapshot
— and the interaction between snapshot, handle file-descriptor cache eviction,
and subsequent writes — is never tested through the actor layer.

**`ReadonlyVolume` has no property-based test.**  The five scenarios in
`readonly_volume_test.rs` are fixed sequences.  A proptest that opens a
`ReadonlyVolume` after an arbitrary sequence of writes, flushes, drains, and GC
passes would explore cases the fixed tests cannot, particularly: GC running after
the `ReadonlyVolume` was opened (the `ReadonlyVolume` has no `apply_gc_handoffs`
path, so its extent index may reference a deleted segment on the next read).

**WAL truncated-tail recovery not triggered by proptest.**  `recover_wal()`
truncates any partial tail record and replays the rest.  The `Crash` SimOp always
drops a clean `Volume` (full records only), so the truncation branch is never
exercised at this level.  It is covered by WAL unit tests in `writelog.rs` but
not by any cross-layer proptest sequence.

### Future dimensions

The current tests focus on crash-recovery correctness for a single fork.
Other dimensions worth adding:

**Fork ancestry isolation oracle** (now implemented).
`elide-core/tests/fork_proptest.rs` covers the layered read path with ULID
cutoffs.  The test runs two phases: random pre-fork base ops (Write/Flush/Drain)
followed by random post-fork mixed ops (BaseWrite/BaseFlush/BaseDrain/
ChildWrite/ChildFlush/ChildDrain/ChildCrash/BaseCrash).  Two oracles are
maintained — `base_oracle` and `child_oracle` (snapshot of base at fork time,
updated only by child writes).  After every `ChildCrash`:

- ancestral LBAs not overwritten by the child read back base data
- child writes shadow base data at the same LBA
- post-branch base writes to new LBAs are invisible to the child (read zero)

**Snapshot floor invariant** (now implemented).  The `Snapshot` SimOp in
`ulid_monotonicity` tracks the floor ULID and asserts after every
`SweepPending` or `Repack` that no frozen segment was deleted, covering
this invariant with the proptest engine beyond the two fixed-sequence unit
tests.

**Coordinator GC interleaved with live writes** (now implemented).
`gc_interleaved_oracle` uses a `prop_oneof!` strategy across six starting
states (cold start, two drained segments, snapshot in place, pending-only,
three segments, post-GC) to ensure `CoordGcLocal` fires reliably.
`elide-core/tests/gc_ordering_test.rs` adds two deterministic integration
tests covering the two key interleaving scenarios: live write before GC
(stale-entry filtering) and live write after GC (sort_for_rebuild priority).

**GC handoff coverage** (now implemented): `CoordGcLocal` now goes through the
full handoff protocol — it writes `gc/<new_ulid>.pending` before deleting the
old input segments, then calls `vol.apply_gc_handoffs()` to exercise the volume's
handoff path.  A `Crash` after the deletion but before `apply_gc_handoffs` is
automatically covered: the rebuilt volume reads from the new segment (which
survived), and the pending handoff is applied on the next `CoordGcLocal`.

**`ReadonlyVolume`** (now implemented).
`elide-core/tests/readonly_volume_test.rs` covers the five key behaviours:
unwritten LBA returns zeros; flushed `pending/` data is visible; WAL-only
writes (not yet flushed) are invisible; drained `segments/` data is visible;
and data remains correct after a coordinator GC pass.

---

## Actor-layer property tests

`elide-core/tests/actor_proptest.rs` tests the concurrency layer — `VolumeActor`,
`VolumeHandle`, and `ReadSnapshot` — rather than `Volume` directly.  This matters
because the actor introduces objects that `Volume` doesn't know about: a per-handle
file-handle cache and an `ArcSwap`-published snapshot.  Bugs in the interaction
between these objects and the Volume's internal state are invisible to the
volume-level proptest.

### What is different at this layer

The volume-level proptest calls `Volume` methods directly in a single thread.
The actor-layer proptest:

- Spawns a real `VolumeActor` thread and communicates through the channel
- Uses `VolumeHandle` for all reads and writes (the production code path)
- Asserts **read-your-writes** after every write — not just after crash

The read-your-writes assertion is the key addition: after `handle.write()` returns
`Ok`, `handle.read()` of the same LBA must immediately return the written data,
without any flush.  This exercises the `ArcSwap` snapshot publication path.

### The simulation model

| Op | Action | Assertion |
|----|--------|-----------|
| `Write { lba, seed }` | `handle.write(lba, [seed; 4096])` | immediately read back same LBA — must match |
| `Flush` | `handle.flush()` — promotes WAL to `pending/` | none |
| `DrainLocal` | moves all `pending/` to `segments/` (simulates coordinator upload) | none |
| `CoordGcLocal { n }` | coordinator-style GC on `segments/`, merges `n` segments (2–5), applies handoff | assert full oracle after handoff |
| `SweepPending` | `handle.sweep_pending()` via actor channel — merges small `pending/` segments | assert full oracle (old files deleted; `publish_snapshot()` must evict handle fd cache) |
| `Repack` | `handle.repack(0.5)` via actor channel — density pass on `pending/` | assert full oracle (same stale-fd invariant as `SweepPending`) |
| `Crash` | shutdown actor + join thread + reopen Volume + new actor | assert full oracle on reopen |

`SweepPending` and `Repack` cover the invariant that `publish_snapshot()` is
called after any compaction that deletes old segment files.  Without the snapshot
republication, a handle with a cached file descriptor to a deleted segment would
get `ENOENT` or read from a wrong offset on the next read.  This bug is invisible
at the `Volume` level because `Volume` serialises its own mutations; only the
actor/handle split exposes it.

`Crash` is a clean shutdown (`Shutdown` message + thread join) followed by
`Volume::open()`, which triggers WAL recovery.  The oracle covers all writes —
including those never explicitly flushed — because WAL recovery makes them
readable.

### Bug found immediately on first run

The proptest found a stale file-handle cache bug on its first run.  Proptest
automatically shrunk the failure to three operations:

```
Write { lba: 0, seed: 50 }
Flush
Write { lba: 0, seed: 50 }   ← same data as first write
```

**What happened:**

1. First `Write`: data written to WAL.  Extent index: `hash → {wal/W1, WAL_OFFSET}`.  Handle file cache populated with an open fd to `wal/W1`.
2. `Flush`: WAL promoted to `pending/W1`.  Extent index updated: `hash → {W1, SEGMENT_OFFSET}` (segment-format absolute offset, a different number).  WAL file deleted — but the open fd in the handle's cache remains valid (Unix keeps the inode alive).
3. Second `Write` (same data): dedup path — the hash is already in the extent index, so a REF record is written.  Extent index unchanged (still `SEGMENT_OFFSET`).  Snapshot published with `SEGMENT_OFFSET`.
4. Read-your-writes check: handle loads snapshot (`SEGMENT_OFFSET`), hits the cached fd (still pointing at the deleted WAL inode), seeks to `SEGMENT_OFFSET` in the WAL file — past the end of the file — and gets `UnexpectedEof`.

**Why it was invisible before:**

The `Volume`-level proptest never exercises this because `Volume` serialises its
own mutations and file cache.  Only the actor/handle split — where the snapshot
and file cache live in a separate object from `Volume` — creates the exposure.
In production this would have triggered on any VM workload that writes a block,
issues a sync, and writes the same block again (a normal pattern for many
filesystems and databases).

**Fix:** a `flush_gen: Arc<AtomicU64>` is shared between the actor and all
handles.  The actor increments it after every WAL promotion and republishes the
snapshot with post-promote offsets.  `VolumeHandle::read()` compares its cached
generation against the current value; if they differ it evicts the file cache
before loading the snapshot.  `flush_wal_to_pending` also evicts `Volume`'s own
file cache for the promoted WAL ULID.

### Second bug found: dead DEDUP_REF removes live DATA extent

`actor_correctness` found a data-corruption bug in `sweep_pending`.  Proptest
shrunk the failure to six operations:

```
Write { lba: 0, seed: 1 }   -- content H1
Write { lba: 3, seed: 1 }   -- same content H1 → DEDUP_REF in WAL
Flush                        -- S1: DATA(lba=0, H1), DEDUP_REF(lba=3, H1)
Write { lba: 3, seed: 2 }   -- lba 3 overwritten in WAL (seed 2 ≠ seed 1)
SweepPending
CoordGcLocal { n: 2 }
```

After the GC handoff, `handle.read(0, 1)` returned all zeros instead of `[1; 4096]`.

**What happened:**

1. `S1` contains `DATA(lba=0, H1)` and `DEDUP_REF(lba=3, H1)`.
2. After `Write(lba=3, seed=2)`, lba 3 is live with hash H2.  The DEDUP_REF for
   lba=3 in S1 is therefore dead.
3. `sweep_pending` classifies `DEDUP_REF(lba=3, H1)` as a dead entry.
4. The dead-entry loop then called `extent_index.remove(H1)` — evicting the
   DATA body location for lba=0, which legitimately has hash H1.
5. Because the dead entry was a DEDUP_REF (no body bytes), `any_dead` was not
   set.  The `candidate_paths.len() == 1 && !any_dead` early-return path fired,
   exiting without rewriting S1 and without re-inserting H1.
6. Subsequent reads of lba=0 found no extent_index entry for H1 and returned zeros.

**Why `any_dead` doesn't cover DEDUP_REFs:**  `any_dead` is only set by dead
non-dedup-ref entries (those with actual body bytes worth reclaiming).  A dead
dedup ref carries no bytes, so it does not contribute to `dead_bytes` and was
never intended to trigger a rewrite — but the removal loop iterated over all
dead entries regardless.

**Fix:** skip DEDUP_REF entries at the top of the dead-entry extent_index
removal loop.  Dedup refs have no body; the extent_index tracks DATA body
locations only.  A dead dedup ref means the LBA has been overwritten, but the
DATA body for the shared hash is still live (referenced by whichever LBA owns
the DATA entry).

### Third bug found: `sweep_pending` non-deterministic merge order corrupts LBA after crash

`actor_correctness` found a data-corruption bug where `sweep_pending` produced
a merged segment with stale LBA data surviving crash+rebuild.  Proptest shrunk
the failure to this sequence:

```
Write { lba: 1, seed: 10 }   -- H10 → pending/W_a
Flush
Write { lba: 1, seed: 123 }  -- H123; overwrites lba 1
Write { lba: 2, seed: 235 }
Flush                         -- pending/W_b: DATA(lba=1, H123), DATA(lba=2, H235)
Flush                         -- pending/W_c: DATA(lba=0, H206)
Write { lba: 7, seed: 10 }   -- WAL; H10 is now live again (different LBA)
SweepPending                  -- merges W_a + W_b + W_c → W_c
Crash                         -- rebuild: lba 1 returns [10; 4096] not [123; 4096]
```

**What happened:**

1. After the second `Write { lba: 7, seed: 10 }`, hash H10 is in `live_hashes`
   (because lba 7 maps to H10).
2. `sweep_pending` uses hash-based liveness: it classifies segment W_a's
   `DATA(lba=1, H10)` as **live** because H10 is in `live_hashes`, even though
   lba 1 was overwritten and no longer maps to H10.
3. Both the stale entry `DATA(lba=1, H10)` (from W_a) and the correct entry
   `DATA(lba=1, H123)` (from W_b) end up in `merged_live` and are written into
   the merged output segment.
4. `collect_segment_files` returns files in OS/filesystem order, which is not
   guaranteed to be ULID order.  On this run, W_b's entries were written before
   W_a's entries in the merged segment.
5. `rebuild_segments` applies entries in file-sequential order with last-write-wins
   semantics.  With the stale entry appearing last (`[10; 4096]` after `[123; 4096]`),
   lba 1 was set to the wrong value.

**Why the tombstone change exposed it:**  the bug predates tombstone support.
Adding tombstone writes to the simulation helper changed the timing of filesystem
operations (a `create_dir_all` and a file write per GC pass), which shifted the
OS `read_dir` ordering just enough that the same proptest regression seeds now
produced the wrong merge order consistently.  Without the tombstone change the
same sequences happened to produce a safe ordering on this filesystem.

**Fix:** sort `seg_paths` by filename (ULID) ascending in `sweep_pending` before
the merge loop.  Since ULIDs encode write order, this guarantees oldest entries
appear first in `merged_live` and therefore first in the output segment.
`rebuild_segments` then always applies the most-recent write last — the correct
last-write-wins result — regardless of how the OS orders `read_dir` results.

---

## Concurrent integration test

`elide-core/tests/concurrent_test.rs` tests the ordering invariant between
the coordinator and the volume that neither proptest can cover: a live reader
must never observe a `segment not found` error during a concurrent GC pass.

The proptest suites are single-threaded by design — they call
`simulate_coord_gc_local` and `apply_gc_handoffs` sequentially on the test
thread, so the window between file deletion and extent-index update never opens.
A dedicated concurrent test is required.

**The invariant:** the coordinator must not delete old local segment files until
after the volume has acknowledged the GC handoff (renamed `gc/<ulid>.pending`
to `gc/<ulid>.applied`).  That rename is the volume's signal that its extent
index now points at the new compacted segment and the old files are safe to
delete.  Violating the ordering — deleting before `apply_gc_handoffs` — leaves
a window where reads of cold LBAs fail with `segment not found`.

**Test structure:** seeds two segments of data (LBAs 0–7), then runs a reader
thread (500 iterations of reading all LBAs) concurrently with a coordinator
thread (one GC pass).  The coordinator applies the handoff first, then deletes
the old segment files.  The reader records any error; the test asserts the error
list is empty.

**How the failure mode was confirmed:** the test was run with
`simulate_coord_gc_local` deleting files inline (before returning), reproducing
the original bug.  The test failed immediately with `segment not found` errors
for LBAs 0–3.  The fix — returning paths to the caller for deferred deletion —
made the test pass.

---

## Formal model: TLA+ handoff protocol

`specs/HandoffProtocol.tla` is a TLA+ formal model of the GC handoff protocol,
verified with the TLC model checker.

The handoff is a three-state file lifecycle (`absent → pending → applied →
done`) driven by two independent actors (coordinator and volume) that can crash
and restart at any point.  The proptest suite exercises this path in-process and
single-threaded; TLA+ covers the orthogonal concern of correctness under all
possible interleavings and crash points, including concurrent writes that
supersede GC results.

### What the model checks

**State variables** are the minimal state needed to reconstruct the protocol at
any point: the handoff file state, the extent index entry for each hash (pre-GC,
updated to GCOutput, removed, or superseded by a newer write), segment presence,
and the up/down state of each actor.

**Actions** are atomic steps: coordinator writes `.pending` (with GCOutput),
volume applies each entry with its per-entry guard, volume renames to
`.applied`, coordinator deletes old segments and renames to `.done`, and
`NewerWrite` — an unconstrained action that can supersede any entry at any time,
modelling concurrent writes.

**Safety invariants** (checked as `INVARIANTS` — must hold in every reachable state):

- `NoSegmentNotFound` — the extent index never references a segment that is not
  present.  Catches any ordering where the volume applies the handoff before
  GCOutput is accessible, or where the coordinator deletes a segment that the
  extent index still points to.

- `NoLostData` — segments are only removed after no extent index entry
  references them.

**Liveness property** (checked as a `PROPERTY` — must hold in every infinite execution):

- `EventuallyDone` — `<>(handoff = "done")`.  The handoff eventually completes
  given fair scheduling.

### Key findings from model-checking

TLC explored 204 distinct states in under 400ms and found no safety violations.
The liveness check required working through the correct fairness conditions:

- **WF (weak fairness)** for restart actions: if an actor is down, restart is
  continuously enabled, so WF suffices.
- **SF (strong fairness)** for all progress actions: crashes temporarily disable
  any action that requires an actor to be UP.  WF fires only when continuously
  enabled; SF fires when enabled *infinitely often*.  Because restarts bring
  actors back up, each progress action is enabled in every restart window —
  infinitely often — so SF guarantees it eventually fires in some window.

TLC surfaced two liveness counterexamples (lassos) that required SF rather than
WF: one where the coordinator stayed permanently down while the volume
crash-looped, and one where the coordinator restarted but immediately crashed
again before completing cleanup.  Both are valid model-checker findings: they
identify the weakest fairness assumption needed to express the liveness claim,
not bugs in the protocol.

### Running the model

```
tlc specs/HandoffProtocol.tla -config specs/HandoffProtocol.cfg
```

Or via the VS Code TLA+ extension (`tlaplus.tlaplus`): open the `.tla` file and
`Cmd+Shift+P → TLA+: Check Model`.  Requires a JRE (`brew install --cask
temurin`).

The `.cfg` uses two carried hashes and one removed hash — enough to exercise all
protocol branches while keeping the state space small.  Scaling up the constants
increases state count polynomially but does not change the result.

---

## Future: deeper concurrency verification

The concurrent integration test validates the ordering invariant under a fixed
workload and real OS thread scheduling.  **`loom`**
([tokio-rs/loom](https://github.com/tokio-rs/loom)) would give stronger
guarantees: it replaces the standard library's atomics, mutexes, and thread APIs
with instrumented versions and exhaustively explores all possible thread
interleavings.  Applied here, it could prove that no scheduling of the
coordinator and reader threads produces a `segment not found` error.  The cost
is rewriting the concurrent paths to use `loom`-aware primitives within the
test, which requires some test-specific abstraction.
