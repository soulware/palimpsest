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
- **WAL ULID is pre-assigned at WAL creation.** Any segment produced by `sweep_pending` or coordinator GC must have a ULID `< wal_ulid`. `sweep_pending` uses `max(candidate_ULIDs)`; coordinator GC uses `max(inputs).increment()`. Both are guaranteed below the current WAL ULID because all `pending/` and S3-confirmed segments were created before the current WAL was opened.
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
- Bug D: `gc_checkpoint` flushed a non-empty WAL under its old (pre-mint) ULID. Fix: pre-mint `u_repack`, `u_sweep`, `u_flush` before any I/O and flush the WAL under `u_flush`.
- Bug E: on restart, the extent index was rebuilt from on-disk `.idx` files which still pointed at old segments, leaving the system stale once `apply_done_handoffs` removed them. Fix in the manifest era: re-apply `.applied` handoffs on restart. Under the self-describing protocol the bare `gc/<new>` body is picked up by `collect_gc_applied_segment_files` during rebuild and feeds the extent index at low priority — so the bug is resolved structurally and no explicit re-apply is needed.
- GC compactor converted DEDUP_REF entries to DATA entries with `stored_length=0`, producing a zero-length body record that caused EIO on rebuild. Fix: check `is_dedup_ref` and emit `new_dedup_ref` in the coordinator GC path. This bug was invisible to `elide-core`'s proptest because the test helper was a *separate reimplementation* of GC that happened to handle DEDUP_REFs correctly — which is why `gc_proptest.rs` exists in the coordinator crate, calling the real code.

### Known gaps

Open gaps in simulation coverage, documented so they are not forgotten:

- **Multi-LBA writes and reads.** Every `Write` is one LBA. The multi-LBA `read_extents` path (`payload_block_offset` arithmetic, partial-range reads) is not exercised. A `WriteMulti` SimOp would cover it and also surface any mixed live/dead partial-extent accounting errors in `sweep_pending` and `repack`.
- **Dedup path not reliably triggered.** DEDUP_REF writes fire only when two LBAs hold identical data; with the current strategy that is by chance. A `DedupWrite` SimOp writing the same seed to two LBAs would guarantee the dedup and dead-REF paths run on every case. (Multiple historical bugs were found in this area only after it was specifically targeted.)
- **`ReadonlyVolume` proptest.** Only fixed-sequence tests exist. A proptest opening a `ReadonlyVolume` after arbitrary writes/flushes/drains/GC would explore GC-after-open (the `ReadonlyVolume` has no `apply_gc_handoffs` path, so its extent index can reference a deleted segment).
- **Partial-fetch `.present` bit gate.** Entries with a clear present bit must fall through to the `SegmentFetcher`; entries with the bit set must be served from the body file. `populate_cache` always sets all bits, so the cleared-bit path is not covered. Closing requires a test `SegmentFetcher` and manual bit clearing.
- **WAL truncated-tail recovery.** The `Crash` SimOp always drops a clean `Volume`, so `recover_wal()`'s partial-tail truncation branch is never triggered at the cross-layer level. Covered by unit tests in `writelog.rs` only.

## Actor-layer proptest

`elide-core/tests/actor_proptest.rs` tests `VolumeActor`, `VolumeHandle`, and `ReadSnapshot` instead of `Volume` directly. The actor layer introduces a per-handle file-handle cache and an `ArcSwap`-published snapshot that `Volume`-level tests cannot see.

Key additions over the volume-level proptest:

- Spawns a real actor thread; all reads/writes go through `VolumeHandle`.
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
- `specs/GCCheckpointOrdering.tla` — `u_repack < u_sweep < u_flush < next_write_wal_ulid` ordering invariant enforced by the three-ULID pre-mint in `gc_checkpoint`. The post-checkpoint WAL is opened lazily on the next write; its ULID comes from the monotonic mint so it automatically exceeds `u_flush`.
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
