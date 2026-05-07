# Testing

## Test file conventions

Each crate's `tests/` directory uses a two-suffix convention:

| Suffix | Purpose |
|--------|---------|
| `*_proptest.rs` | Proptest suites — random operation sequences, shrinking, regression seeds |
| `*_test.rs` | Deterministic tests — fixed sequences, regression reproductions, scenario tests |

Current inventory:

```
elide-core/tests/
  volume_proptest.rs      proptest: crash-recovery oracle + ULID monotonicity
  fork_proptest.rs        proptest: fork ancestry isolation oracle
  actor_proptest.rs       proptest: actor-layer read-your-writes oracle
  fork_test.rs            deterministic: fork ancestry fixed scenarios
  gc_ordering_test.rs     deterministic: GC interleaving fixed scenarios
  gc_index_test.rs        deterministic: GC index rebuild scenarios
  readonly_volume_test.rs deterministic: ReadonlyVolume fixed scenarios
  evict_test.rs           deterministic: segment eviction scenarios
  concurrent_test.rs      deterministic: concurrent GC + reader ordering

elide-coordinator/tests/
  gc_proptest.rs          proptest: coordinator GC oracle (segment cleanup + data)
  gc_test.rs              deterministic: coordinator GC regression reproductions
```

The split is enforced by naming. When adding a new test file, pick the right suffix and add it to the inventory above.

## Property-based tests

The volume's crash-recovery model is verified with [proptest](https://proptest-rs.github.io/proptest/). Proptest generates random operation sequences, runs them against the real volume implementation, checks invariants, and shrinks failing inputs to a minimal reproducer.

### Correctness invariants

- **ULID total order is correctness.** `rebuild_segments` applies segments oldest-first by ULID; a violation lets an older segment silently shadow a newer write.
- **WAL ULID is pre-assigned at WAL creation.** Any segment produced by `sweep_pending` or coordinator GC must have a ULID `< wal_ulid`. `sweep_pending` uses `max(candidate_ULIDs)`; coordinator GC uses `UlidMint::next` (clock-if-ahead-else-`last.increment()`). Both are guaranteed below the current WAL ULID because all `pending/` and S3-confirmed segments — and the mint's last-seen ULID — were created before the current WAL was opened. See `docs/operations.md` *gc_checkpoint — the pre-mint pattern* for why GC ULID timestamps tracking wall-clock-at-handoff is also load-bearing for retention.
- **`index/` ↔ S3 invariant.** The coordinator reads `index/` to find GC candidates but never writes into `cache/`. The volume's promote IPC handler is the sole path that moves bodies into `cache/`, and only after S3 upload.
- **Snapshot floor.** Segments at or below the latest snapshot ULID are frozen — `sweep_pending` and `repack` must never modify or delete them.

### Simulation model

`volume_proptest.rs` runs a random sequence of `SimOp` values against a single fork directory. Two proptest blocks share the same ops:

- `ulid_monotonicity` — every new segment's ULID must be `> max(pre-existing ULIDs)`; snapshot-floor segments must not be deleted.
- `crash_recovery_oracle` — maintains a ground-truth `HashMap<lba, data>`; after every `Crash` (drop + reopen), reads every LBA back and compares.

| Op | What it does |
|----|-------------|
| `Write { lba, seed }` | `vol.write(lba, [seed; 4096])` |
| `Flush` | `vol.flush_wal()` — promotes WAL to `pending/` |
| `SweepPending` | merges/deduplicates `pending/` segments |
| `Repack` | `vol.repack(0.9)` density pass on `pending/` + S3-confirmed |
| `DrainLocal` | simulates coordinator upload: `pending/` → `index/` + `cache/` |
| `CoordGcLocal { n }` | coordinator-style GC on S3-confirmed segments, `n` in 2–5 |
| `Crash` | drops the `Volume` and reopens it (full rebuild) |
| `Snapshot` | `vol.snapshot()` — sets the snapshot floor |
| `PopulateFetched` | writes 3-file demand-fetch cache format (`.idx` + `.body` + `.present`) |
| `ReadUnwritten` | reads LBA 64 (always outside write range); must be zero |

`CoordGcLocal` picks the `n` oldest S3-confirmed segments, writes a self-describing GC output to `gc/<ulid>.staged` (with the consumed input ULID list embedded in the segment header, no manifest sidecar), and calls `vol.apply_gc_handoffs()` to exercise the volume's handoff path — the same algorithm as `elide-coordinator/src/gc.rs`.

### Extending the tests

To add a new operation:

1. Add a `SimOp` variant in `tests/volume_proptest.rs` and wire it into `arb_sim_op()`.
2. Handle it in **both** proptest blocks:
   - `ulid_monotonicity`: assert every new ULID is `> max_before`, and no frozen (≤ snapshot floor) segment was deleted.
   - `crash_recovery_oracle`: update the oracle if the op changes visible LBA state; otherwise just execute.
3. Handle no-op results without panicking.

For design guidance when extending simulation coverage, see `.claude/agents/proptest-guardian.md`.

### Materializing proptest failures as deterministic tests

When proptest finds a meaningful bug (not a trivial off-by-one), promote the shrunk sequence to a named `#[test]` alongside the proptest, with a comment explaining the invariant violated and the fix. The deterministic test should fail on the unfixed code and pass after the fix. Keep the proptest regression seed too — the seed replays the failure but the deterministic test documents *why* the bug occurred and survives proptest version upgrades.

Good candidates for promotion: concurrency/ordering bugs, bugs whose fix involves a non-obvious invariant, or any bug where diagnosis required real analysis. Simple shape bugs can stay as just a seed.

### Proptest regression files

Each proptest suite has a `*.proptest-regressions` file. Keep these committed — they are the suite's memory of previously-found bugs. Deduplicate periodically: proptest appends a new seed on every failure even if it shrinks to the same sequence.

### Historical bugs caught

Each entry below has a named deterministic regression test; see the listed file for the reproduction and fix detail.

**Volume layer** (`volume_proptest.rs`, `actor_proptest.rs`):

- `sweep_pending` used `mint.next()` for output ULIDs, producing values above the current WAL ULID. Fix: use `max(candidate_ULIDs)`.
- `repack` had the same `mint.next()` bug, plus a delete-after-rename bug when the source was in `pending/`.
- Actor-layer stale file-handle cache: after a flush promoted WAL to `pending/`, handles held open fds to the deleted WAL inode. Fix: shared `flush_gen` atomic; handles evict the fd cache when the generation bumps.
- `sweep_pending` dead-entry loop removed the extent-index entry for a live DATA body when processing a dead DEDUP_REF sharing its hash. Fix: skip DEDUP_REFs in the removal loop.
- `sweep_pending` non-deterministic merge order (from OS `read_dir` order) could leave stale entries last in the output. Fix: sort candidates by ULID before merging.

**Coordinator layer** (`gc_proptest.rs`, `gc_test.rs`):

- Bug A: DEDUP_REF-only input segments were never deleted (no handoff line named them). Fix in the manifest era: emit a `Dead` line for every consumed input. Under the self-describing protocol every input ULID is in the new segment's header so this class of bug cannot recur.
- Bug B: a write between `gc_checkpoint()` and `apply_gc_handoffs()` could revive a hash GC had marked dead, and `apply_gc_handoffs` would then remove the now-live hash. Fix: re-check `lba_referenced_hashes()` inside `apply_gc_handoffs` and cancel the pass if any not-carried hash is live.
- Bug C: `gc_checkpoint` did not open a new WAL when the existing WAL was empty, so its ULID stayed below the freshly-minted GC output ULIDs. Superseded by the lazy-WAL design: on idle, `gc_checkpoint` deletes the empty WAL file and leaves the volume in a no-WAL state; the next write opens a fresh WAL at `mint.next()` (monotonically above the GC output ULIDs), so no reserved `u_wal` is needed.
- Bug D: `gc_checkpoint` flushed a non-empty WAL under its old (pre-mint) ULID. Fix: pre-mint `u_gc` and `u_flush` before any I/O and flush the WAL under `u_flush` (originally three ULIDs `u_repack < u_sweep < u_flush` when the dead/repack/sweep passes were separate; collapsed to two under the unified GC pass).
- Bug E: on restart, the extent index was rebuilt from on-disk `.idx` files which still pointed at old segments, leaving the system stale once `apply_done_handoffs` removed them. Fix in the manifest era: re-apply `.applied` handoffs on restart. Under the self-describing protocol the bare `gc/<new>` body is picked up by `collect_gc_applied_segment_files` during rebuild and feeds the extent index at low priority — so the bug is resolved structurally and no explicit re-apply is needed.
- GC compactor converted DEDUP_REF entries to DATA entries with `stored_length=0`, producing a zero-length body record that caused EIO on rebuild. Fix: check `is_dedup_ref` and emit `new_dedup_ref` in the coordinator GC path. This bug was invisible to `elide-core`'s proptest because the test helper was a *separate reimplementation* of GC that happened to handle DEDUP_REFs correctly — which is why `gc_proptest.rs` exists in the coordinator crate, calling the real code.

### Runtime invariants gated behind `--features volume-invariants`

Some bugs are easier to catch at the introducing call site than at the eventual symptom. For those, the volume layer keeps "is the in-memory state consistent with disk?" assertions that fire at the end of structural state-mutating methods and panic on divergence with a labelled message identifying the calling method.

These checks are **not free** — each call rebuilds an in-memory data structure from disk. To keep the everyday `cargo test -p elide-core` suite fast and to avoid slowing down elide-core's own proptest binaries (which don't exercise the coordinator-layer paths these invariants protect), the invariants are gated behind a separate `volume-invariants` feature. The rebuild path additionally **skips ed25519 signature verification** (`lbamap::rebuild_segments_unverified`) since `Volume::open` already verifies signatures at startup and on-disk segments are immutable thereafter — the per-segment verify dominates the assertion cost.

#### Wiring

- `elide-core` declares two independent features: `proptest` (its own proptest suites) and `volume-invariants` (the runtime drift detection).
- `elide-coordinator/proptest` propagates **both** to elide-core: `proptest = ["elide-core/proptest", "elide-core/volume-invariants"]`. Running `cargo test -p elide-coordinator --features proptest` enables the invariants because elide-coordinator's coordinator-layer stress (gc_proptest) is exactly where the drift bugs live.
- Running `cargo test -p elide-core --features proptest` does **not** enable `volume-invariants` — elide-core's volume_proptest / fork_proptest / actor_proptest stay fast.
- CI **must split the workspace command per crate** rather than running `cargo test --workspace --features proptest` — Cargo's resolver-2 unifies features across a workspace build, which would activate `volume-invariants` for elide-core's test binaries via the elide-coordinator dependency. The split is in `.github/workflows/ci.yml` under the `host-proptests` job.
- The invariant function is split into two definitions: a heavy implementation under `#[cfg(feature = "volume-invariants")]`, and an `#[inline]` no-op stub under `#[cfg(not(feature = "volume-invariants"))]`. Callers always invoke the same name; the compiler picks the right one. This avoids `#[cfg]` clutter at every call site.

```rust
#[cfg(feature = "volume-invariants")]
pub(in crate::volume) fn assert_lbamap_consistent(&self, caller: &'static str) {
    // ... rebuild from disk + WAL, compare to self.lbamap, panic on diverge.
}

#[cfg(not(feature = "volume-invariants"))]
#[inline]
pub(in crate::volume) fn assert_lbamap_consistent(&self, _caller: &'static str) {}
```

#### Calling convention

Each call passes a static `&'static str` identifying the caller — e.g. `"apply_promote_segment_result_drain"`. The label appears in the panic message so a failing test points at the exact mutating method that introduced the drift, without walking a backtrace through release-mode inlining. This matters in proptest output where threads and panics interleave.

Call at the **end** of every method that may mutate the asserted state, on every successful exit path (early returns included). Putting the assert at the entry would fire on a precondition violation rather than the responsible operation.

#### Active invariants

Call sites use the umbrella `Volume::assert_volume_invariants(caller)` rather than the individual asserts; adding a new invariant is a single edit there and every existing call site picks it up. Called from structural ops only — `Volume::open`, `gc_checkpoint_for_test`, `apply_redact_result`, `apply_plan_apply_result`, `apply_promote_segment_result`. **Not** called from `write` / `write_zeroes` (high-frequency incremental updates whose drift would still be caught at the next structural op).

Currently in the umbrella:

- **`assert_lbamap_consistent`** — rebuilds the lbamap from `discover_fork_segments` + WAL replay and compares to `self.lbamap` per LBA. Catches the class of bug where a state-mutating op moves disk state without updating the in-memory mirror (e.g. promoting a pending segment changes its tier in the walk order, which can flip the per-LBA winner).

- **`assert_pending_above_committed`** — `max(committed_ulid) < min(pending_ulid)`. Structural form of the drain-ordering invariant `discover_fork_segments` walks under (committed first, then pending; pending wins last). Catches drain-ordering regressions before they surface as lbamap drift.

- **`assert_extent_index_consistent`** — every hash in `self.extent_index` must point at a segment that exists somewhere on disk. Catches phantom entries (e.g. inserting `ZERO_HASH` by mistake) and entries pointing at deleted segments. Deliberately doesn't enforce specific `segment_id` agreement (in-memory and disk-rebuild legitimately diverge there because some apply paths use unconditional `insert` while rebuild uses `insert_if_absent`) and doesn't fire on "disk has more than memory" (pre-existing redact/GC-prune-ancestor behaviour, out of scope).

- **`assert_lbamap_hashes_resolvable`** — every non-zero hash in `self.lbamap` must be resolvable through `self.extent_index` (either as DATA-canonical or Delta-canonical). Pure in-memory check, linear in lbamap entry count with two HashMap lookups per entry — cheap relative to the rebuild-from-disk invariants. Catches the bug class "lbamap retains a hash claim while extent_index lost the body location": a future read of that LBA would fail at the extent_index lookup, silent data unavailability waiting to surface.

#### When tests trip the invariant

Two recurring shapes, both test-setup artifacts that don't reflect production paths:

1. **Hand-crafted segment files.** Tests that lay down a segment directly (e.g. `write_segment_with_delta_body` for a Delta entry, or `populate_cache` for the 3-file demand-fetch format) bypass `Volume::write` → WAL → flush. `self.lbamap` never sees those entries; the next mutating op trips the invariant.

   *Resolution:* drop and reopen the `Volume` between the hand-craft and the next mutating call. `Volume::open`'s rebuild folds the new segment in.

2. **Promoting only the highest-ULID pending segment.** Tests that promote one pending segment while lower-ULID pending peers still claim overlapping LBAs flip the lbamap walk-order winner: pending always wins last, so once the high-ULID segment crosses into committed, a lower-ULID pending peer takes over for any overlapping LBA — but `self.lbamap` doesn't reflect the flip. Same drain-bug class as the one fixed for production drain in #265.

   *Resolution:* promote all pending segments in ULID-ascending order via `segment::read_ulid_dir_sorted`. The just-promoted ULID then stays below every remaining pending ULID and no overlapping pending peer can take over.

Production code paths don't hit either shape — `Volume::write` populates lbamap incrementally and the production drain (`coordinator/upload.rs`) sorts by ULID.

### Known gaps

Open gaps in simulation coverage, documented so they are not forgotten:

- **`ReadonlyVolume` proptest.** Only fixed-sequence tests exist. A proptest opening a `ReadonlyVolume` after arbitrary writes/flushes/drains/GC would explore GC-after-open (the `ReadonlyVolume` has no `apply_gc_handoffs` path, so its extent index can reference a deleted segment).
- **Concurrent `.present` RMW.** `FetchCoalescer` serialises the read-modify-write of `<seg>.present` via a per-segment `Mutex<()>` (PR #226) — the bug was lost bits when concurrent disjoint coalescer leases on the same segment ORed against a stale on-disk image. The deterministic reproducer landed alongside #226. The single-threaded proptest cannot exercise interleavings; the appropriate vehicle is a dedicated multi-threaded test or a loom model. (The cleared-bit fall-through to the `SegmentFetcher` *is* now covered by `SimOp::EvictCacheBody` paired with `CapturedBodyFetcher`.)
- **WAL truncated-tail recovery.** The `Crash` SimOp always drops a clean `Volume`, so `recover_wal()`'s partial-tail truncation branch is never triggered at the cross-layer level. Covered by unit tests in `writelog.rs` only.

## Actor-layer proptest

`elide-core/tests/actor_proptest.rs` tests `VolumeActor`, `VolumeClient`/`VolumeReader`, and `ReadSnapshot` instead of `Volume` directly. The actor layer introduces a per-reader file-handle cache and an `ArcSwap`-published snapshot that `Volume`-level tests cannot see.

Key additions over the volume-level proptest:

- Spawns a real actor thread; all reads/writes go through `VolumeClient`/`VolumeReader`.
- Asserts **read-your-writes** after every write — no flush needed. This exercises the `ArcSwap` snapshot publication path and is where the stale file-handle bug was caught.
- `SweepPending` and `Repack` assert the full oracle after return, covering the invariant that `publish_snapshot()` is called after any compaction that deletes old segment files (without it, handles with cached fds to deleted segments read from stale offsets).

## Fork ancestry proptest

`elide-core/tests/fork_proptest.rs` covers the layered read path with ULID cutoffs. Two phases: pre-fork base ops, then post-fork mixed ops on base and child. Two oracles (`base_oracle` and `child_oracle`, with child initialised as a snapshot of base at fork time) check the three ancestry properties after every `ChildCrash`/`BaseCrash`:

- Ancestral LBAs not overwritten by the child read back base data.
- Child writes shadow base data at the same LBA.
- Post-branch base writes to new LBAs are invisible to the child.

## Coordinator GC proptest

`elide-coordinator/tests/gc_proptest.rs` calls the **real** `gc_fork()` + `apply_gc_handoffs()` + `apply_done_handoffs()` path. This closes the structural gap where `elide-core`'s simulation helper could be correct while production coordinator code had a bug (exactly what happened with the DEDUP_REF → DATA conversion bug).

`GcSweep` flushes the WAL first (matching `gc_checkpoint()` in production), runs the full coordinator round-trip, then asserts every oracle LBA reads back its last-written value. A `Restart` SimOp exercises `GcSweep → Restart → GcSweep` sequences for the restart-safety path.

## Concurrent integration test

`elide-core/tests/concurrent_test.rs` enforces the ordering invariant between the coordinator and volume that neither proptest can cover: a live reader must never observe `segment not found` during a concurrent GC pass. The proptest suites are single-threaded by design, so the window between file deletion and extent-index update never opens there.

**The invariant:** the coordinator must not delete old local segment files until the volume has renamed `gc/<ulid>.staged` to bare `gc/<ulid>` (the atomic commit point of the self-describing handoff). The bare file's appearance is the volume's signal that its extent index now points at the new segment, and the bare file itself is what the coordinator's `apply_done_handoffs` walks.

The test seeds two segments, then runs a reader thread (500 read-all iterations) concurrently with a coordinator thread running one GC pass, asserting the reader's error list stays empty. The failure mode was confirmed by running once with inline deletion (before handoff apply), which reproduced `segment not found` for cold LBAs; the fix — returning paths for deferred deletion — made it pass.

## Formal model: TLA+ specs

Two TLA+ models checked with TLC:

- `specs/HandoffProtocol.tla` — GC handoff protocol.
- `specs/GCCheckpointOrdering.tla` — `u_gc < u_flush < next_write_wal_ulid` ordering invariant enforced by the two-ULID pre-mint in `gc_checkpoint`. The post-checkpoint WAL is opened lazily on the next write; its ULID comes from the monotonic mint so it automatically exceeds `u_flush`.
- `specs/WorkerOffload.tla` — actor ↔ worker offload protocol. Models the prep/middle/apply three-phase shape every offloaded maintenance op shares (sweep, repack, delta_repack, promote, gc_handoff, sign_snapshot_manifest) using one canonical op. Checks the CAS-loser survival invariant that proptest cannot cover (a single-threaded test cannot interleave a write between prep and apply on the same actor), and the post-crash no-permanent-park invariant that is structurally invisible to proptest (the actor is dropped along with its parked slots).

### HandoffProtocol

The model still describes the pre-self-describing three-state lifecycle (`absent → pending → applied → done`) and remains an accurate model of the safety invariants — the new on-disk shape is `absent → staged → bare → absent` but the same correctness properties hold (no segment is deleted without acknowledgment, no extent references a missing segment). Updating the TLA+ model to the new lifecycle is tracked as an open item.

**Safety invariants:**

- `NoSegmentNotFound` — the extent index never references an absent segment.
- `NoLostData` — segments are only removed once no extent index entry references them.

**Liveness:** `EventuallyDone` — `<>(handoff = "done")`. Requires strong fairness on progress actions (crashes temporarily disable them, so WF is insufficient) and weak fairness on restarts.

**Running:**

```
tlc specs/HandoffProtocol.tla -config specs/HandoffProtocol.cfg
```

Or via VS Code (`tlaplus.tlaplus`). Requires a JRE. The `.cfg` uses two carried hashes and one removed hash — enough to exercise all branches while keeping the state space small.

### WorkerOffload

```
tlc specs/WorkerOffload.tla -config specs/WorkerOffload.cfg
```

`MAX_WRITES = 3` is the default bound — every state-relevant interleaving is reachable at that size. Invariants checked: `NoCorruption`, `NoPermanentPark`, `OneInFlight`, and the temporal property `CasLoserSurvives`. To observe the bug the CAS guards against, drop the `extent_index = worker_job.src` check from the `Apply` action; TLC produces a `Write → StartPromote → Write → WorkerProduce → Apply` counterexample where the concurrent Write is clobbered.

## Future: deeper concurrency verification

The concurrent integration test validates the ordering invariant under a fixed workload and real OS scheduling. [`loom`](https://github.com/tokio-rs/loom) would give stronger guarantees by exhaustively exploring all thread interleavings, but requires rewriting the concurrent paths to use `loom`-aware primitives within the test.
