# Plan: offload WAL promotion off the volume actor

**Status:** Proposal. Depends on [actor-offload-plan.md](actor-offload-plan.md) — this is the first landing step for that broader plan.

## Goal

Move the expensive part of `Volume::flush_wal_to_pending` off the actor thread onto a dedicated flusher thread, so that a write burst large enough to trigger promotion does not stall subsequent writes for the 10–500 ms it takes to serialize a 32 MiB segment and fsync it.

The same offload path is used by `gc_checkpoint`, which today synchronously promotes the current WAL on the actor and blocks every queued write for the duration. Routing GC through the flusher means one machinery serves both latency-relevant callers.

**Non-goals.** Speeding up promotion itself. Offloading any other maintenance op (covered by the parent plan). Changing the read path.

## Flush vs promote

Today `flush_wal()` conflates two operations that have different purposes:

- **Flush** = `wal.fsync()`. Durability barrier. The WAL is the durability boundary — after fsync, data survives a crash. This is what `NBD_CMD_FLUSH` requires.
- **Promote** = serialize WAL entries into a `pending/<ulid>` segment, update the extent index to segment-relative offsets, delete the old WAL, open a fresh WAL. Housekeeping for the segment lifecycle.

The offload separates these. `VolumeRequest::Flush` becomes a WAL fsync + immediate reply — no segment I/O, no blocking on the flusher. Promotion is triggered independently by the threshold check (`needs_promote()`) and the idle tick, dispatched asynchronously to the flusher thread.

Callers that need data in `pending/` before proceeding (the coordinator's snapshot path, `gc_checkpoint`) depend on promote completion, not flush. Those callers use `Snapshot` or `GcCheckpoint` IPC, which wait for any in-flight promote before proceeding.

## Current flow

`VolumeActor::run` at `actor.rs:195` on a successful write:

1. `volume.write(&data)` — WAL append + `extent_index.insert(hash, { segment_id: wal_ulid, body_offset: wal_relative_offset, .. })`.
2. Publish a new `ReadSnapshot` (no `flush_gen` bump).
3. Reply to the caller.
4. `if volume.needs_promote()` → `volume.flush_wal()` → `flush_wal_to_pending_as(self.wal_ulid)`.

`flush_wal_to_pending_as` at `volume.rs:2103`:

1. `wal.fsync()`.
2. `segment::promote(wal_path, segment_ulid, pending_dir, &mut pending_entries, signer)` — writes `pending/<ulid>.tmp` via `write_segment`, renames to `pending/<ulid>`, fsyncs the directory, **deletes the WAL file**.
3. Loops over `pending_entries` and rewrites `extent_index` entries to segment-relative offsets via `Arc::make_mut`.
4. `evict_cached_segment(wal_ulid)` on the volume's internal `file_cache`.
5. Returns; caller (`actor.rs:214`) calls `publish_snapshot` which bumps `flush_gen`.

The segment file reuses the WAL's ULID (`segment_ulid == self.wal_ulid`). `find_segment_in_dirs` at `volume.rs:2921` searches `wal/` before `pending/`, so during the promote window stale readers find the WAL file at its original path. After the delete, stale readers with cached fds continue reading via the deleted inode (Linux keeps it alive); stale readers without a cached fd fall through to `pending/<same_ulid>` and could read **wrong bytes** because their extent index entries still hold WAL-relative offsets. **This is a latent race today**, contingent on cold per-handle caches. The offload must not regress it, and ideally fixes it.

`gc_checkpoint` at `volume.rs:1333` is the one existing caller that already breaks the WAL-ULID-reuse convention: it pre-mints `u_repack < u_sweep < u_flush < u_wal` from the mint, calls `flush_wal_to_pending_as(u_flush)` (so the segment lands at `pending/<u_flush>`, not at the WAL's original ULID), then opens a fresh WAL with `u_wal > u_flush`. The offload inherits this pattern and extends it to the common case.

## Proposed flow

### Data structures

```rust
struct PromoteJob {
    segment_ulid: Ulid,                  // fresh, distinct from the WAL ulid
    old_wal_ulid: Ulid,
    old_wal_path: PathBuf,
    entries: Vec<SegmentEntry>,          // moved out of pending_entries
    pre_promote_offsets: Vec<Option<u64>>, // CAS tokens snapshotted at prep time
    signer: Arc<dyn SegmentSigner>,
    pending_dir: PathBuf,
}

struct PromoteResult {
    segment_ulid: Ulid,
    old_wal_ulid: Ulid,
    old_wal_path: PathBuf,
    body_section_start: u64,
    entries: Vec<SegmentEntry>,          // returned; `stored_offset` now segment-relative
    pre_promote_offsets: Vec<Option<u64>>,
}
```

The flusher is a single long-lived thread owning one crossbeam receiver for `PromoteJob`. The actor owns the sender. On shutdown the actor drops its sender; the flusher exits when `recv()` returns `Disconnected`.

Results come back on a **separate dedicated channel** (`Sender<io::Result<PromoteResult>>`), not the actor's main `VolumeRequest` channel. The actor's `select!` loop gains a third arm for this channel. This avoids the flusher holding a clone of the actor's sender, which would prevent the main channel from disconnecting when all `VolumeHandle`s are dropped — the actor relies on that disconnection for shutdown detection.

Multiple promotes can be in flight simultaneously. Each `PromoteJob` carries all the data the flusher needs; the flusher processes jobs in FIFO order and sends results back in the same order. The actor applies completions in arrival order.

### Sequencing: normal write-path promote

#### Actor: prep phase (triggered when `needs_promote()` fires after a write reply, or on idle tick)

1. `volume.wal.fsync()` — durability barrier for WAL data.
2. Snapshot CAS tokens: `pre_promote_offsets` from the extent index for each Data/Inline entry (same as the inline path does today).
3. `let old_wal_ulid = volume.wal_ulid;`
4. `let old_wal_path = volume.wal_path.clone();`
5. `let entries = std::mem::take(&mut volume.pending_entries);`
6. `let segment_ulid = volume.mint.next();` — **fresh ULID, not the WAL ULID**.
7. Open fresh WAL: `create_fresh_wal(&wal_dir, volume.mint.next())`. Volume's `wal_ulid` / `wal_path` / `pending_entries` are now the fresh WAL's. New writes start flowing immediately.
8. Send `PromoteJob { segment_ulid, old_wal_ulid, old_wal_path, entries, pre_promote_offsets, signer, pending_dir, reply_tx }` to the flusher.
9. Return to the select loop.

The actor hands off ownership of `entries` by move. It never touches the old batch again until the completion message arrives. If `needs_promote()` fires again before the first promote completes, the actor preps and dispatches another job — multiple promotes can be queued on the flusher channel.

#### Flusher: heavy middle

1. `let body_section_start = segment::write_and_commit(&pending_dir, segment_ulid, &mut entries, signer.as_ref())?;`
   - Writes `pending/<segment_ulid>.tmp` via `write_segment`. `write_segment` reassigns each entry's `stored_offset` to segment-relative.
   - Renames to `pending/<segment_ulid>`.
   - `fsync_dir`.
   - **Does not delete the old WAL** — that is the actor's responsibility in the apply phase.
2. Send `VolumeRequest::PromoteComplete(PromoteResult { .. })` back via the actor's main channel.

`segment::write_and_commit` already exists (Landing 1): `segment::promote` minus the final `fs::remove_file(wal_path)`.

#### Actor: apply phase (on `PromoteComplete`)

1. If `result.result` is an error: log, **do not touch** the old WAL or extent index (the old WAL still exists on disk, its entries still point at it at WAL-relative offsets, so reads continue to work). Return to the select loop. The old WAL is drained on restart via the multi-WAL recovery path.
2. On success, update `extent_index`. For each entry in `result.entries`, paired with the corresponding `pre_promote_offsets` token:
   - Skip `DedupRef` / `Zero` / `Delta` as today.
   - **Conditional replace** — only rewrite the entry if it currently matches `(segment_id == old_wal_ulid && body_offset == pre_promote_offset)`. If a concurrent write, a GC handoff, or a later promote's apply has already superseded it, leave it alone.
   - Otherwise `Arc::make_mut(&mut self.extent_index).insert(...)` with the new `(segment_ulid, stored_offset, body_section_start, BodySource::Local)`.
3. `self.publish_snapshot()` — bumps `flush_gen`. Readers loading the new snapshot see segment-relative offsets pointing at `pending/<segment_ulid>` and flush their file caches.
4. **Only now** delete the old WAL: `fs::remove_file(&old_wal_path)`. Stale readers that loaded the pre-apply snapshot still have entries with `segment_id = old_wal_ulid`; as long as they're still referencing that snapshot, the WAL file stays reachable via its original path or via their cached fd.

The "delete after publish" ordering is stricter than the current code (which deletes before updating the extent index) and, together with the fresh segment ULID, fixes the latent cold-cache race: after step 4, the only code paths that could still reach the old WAL file are readers holding a pre-apply snapshot whose entries use WAL-relative offsets that only make sense against the WAL file. Post-apply readers use segment-relative offsets pointing at `pending/<segment_ulid>`, a path that never collides with the WAL's ULID.

### Sequencing: gc_checkpoint through the flusher

`gc_checkpoint` needs all prior promotes applied before it can mint its ULIDs and dispatch its own promote. This is the one operation that must wait for the flusher queue to drain.

**Deferred until the flusher queue is empty.** When `GcCheckpoint` arrives and promotes are in flight, the actor stashes the reply and processes it once the last pending `PromoteComplete` has been applied. Writes continue normally during this wait — the active WAL is always open.

Once the queue is empty:

1. Mint `u_repack`, `u_sweep`, `u_flush`, `u_wal` in order.
2. Run the normal prep phase using `u_flush` as the segment ULID. **Do not open a fresh WAL** — the fresh WAL must use the pre-minted `u_wal` which sorts above `u_flush`.
3. Dispatch the promote job to the flusher. Defer writes until the completion arrives (no active WAL in this window).
4. On `PromoteComplete`: run the normal apply phase, then open the fresh WAL at `wal/<u_wal>`, send `Ok((u_repack, u_sweep))` on the stashed reply, unpark deferred writes.

The write deferral window is bounded by the flusher's segment write time (10–100 ms). GC checkpoints are rare (coordinator-driven), so the brief stall is acceptable.

#### Concurrent GcCheckpoint is an error

If a second `GcCheckpoint` request arrives while one is already parked, the actor replies with an error. The coordinator is expected to serialize its own checkpoints; a concurrent one signals a coordinator bug and should be loud.

## Fresh segment ULID

Minting a fresh ULID for the promoted segment (distinct from the WAL's own filename ULID) is not novel — `gc_checkpoint` already does this with `u_flush`. The offload plan extends the pattern to the common path.

Two benefits:

1. **No same-ULID path collision.** `find_segment_in_dirs` searches `wal/` then `pending/`, and today relies on the WAL file being deleted before the pending segment becomes lookup-reachable by the same ULID. Under the offload the old WAL survives across the rename-before-apply window. Using a fresh segment ULID means stale-snapshot lookups for `old_wal_ulid` only resolve to the WAL file; a cold-cache reader that can't find `wal/<old_wal_ulid>` gets `NotFound` — an error, not silent wrong bytes.
2. **Crash recovery has two distinct ULIDs to reason about** rather than one overloaded one.

### Interaction with the mint and with GC ULID ordering

The mint is a single monotonic counter on `Volume`, advanced by `mint.next()`. All minting happens on the actor thread; the flusher never touches it.

Under the plan, each promote advances the mint by:

| Context | Mint advances |
|---|---|
| Write-path / idle-tick promote | 2 (`segment_ulid`, `new_wal_ulid`) |
| `gc_checkpoint` promote | 4 (`u_repack`, `u_sweep`, `u_flush`, `u_wal`) — unchanged from today |

All minting is single-threaded on the actor. Even with multiple promotes in flight, the segment ULIDs are minted in dispatch order and form a strict sequence. The flusher never mints.

`GcCheckpoint` waits for the flusher queue to drain before minting its four ULIDs, so `u_repack < u_sweep < u_flush < u_wal` all sort above every previously dispatched segment ULID. The GC ULID-ordering invariant from `volume.rs:1313` is preserved: `(existing_segments) < u_repack < u_sweep < u_flush < u_wal < (fresh_wal_segments)`.

## Recovery semantics

The existing recovery path at `Volume::open_impl` makes two assumptions that the offload plan breaks:

1. **Exactly one WAL file exists.** `volume.rs:498` replays only `wal_files.into_iter().last()` — the highest-ULID WAL — and silently ignores any earlier ones.
2. **If `pending/<ulid>` and `wal/<ulid>` coexist, they share a ULID** (`volume.rs:425-438`). The dedup-retain loop treats this as "promote rename succeeded, WAL delete interrupted" and deletes the stale WAL.

Under the offload, crash windows can leave the volume with **multiple WAL files** and with **different ULIDs between the old WAL and the in-flight segment**. The recovery path must change.

### New recovery rules

1. **Replay all WAL files in ULID order**, not just the latest.
2. **Promote every non-latest WAL to a segment** during recovery, so the volume returns to its single-active-WAL invariant before `Volume::open` completes.
3. **Keep the legacy same-ULID dedup-retain loop** as a degenerate-state cleanup for volumes left over from pre-offload versions of the code. It no longer fires in normal operation but is cheap and handles on-disk state from older releases.

The revised recovery sequence, replacing lines 417–502 of `Volume::open_impl`:

```text
1. List wal_files in the wal directory, sort ascending by ULID.
2. Apply the legacy dedup-retain loop (same-ULID wal vs pending) — a no-op
   for volumes written by the offload path, handles legacy state.
3. open_read_state rebuilds the ancestor chain + self's pending/ + index/,
   populating lbamap and extent_index with segment-relative offsets.
4. For each wal file except the last (in ULID ascending order):
     - recover_wal(wal_path, &mut lbamap, &mut extent_index)
       (reads records, populates both maps with WAL-relative offsets at
       segment_id = this wal's ULID.)
     - mint a fresh segment_ulid = mint.next()
     - segment::write_and_commit(pending_dir, segment_ulid, &mut entries,
       signer) — writes pending/<segment_ulid>.
     - For each entry in this batch, conditionally replace the extent
       index location from (this wal_ulid, wal_offset) to (segment_ulid,
       segment_offset). Same CAS semantics as the online apply phase.
     - Delete wal_path.
5. For the last wal file, recover_wal sets up the active WAL: returns the
   WriteLog handle, ulid, path, and in-memory pending_entries that the
   fresh VolumeActor will own.
6. If there were no wal files, create a fresh one as today.
```

The mint floor computation (`volume.rs:483-493`) still happens before step 4 so that the fresh segment ULIDs minted during recovery are strictly above any existing segment ULIDs.

**Bookkeeping: `last_segment_ulid` must be updated for every recovery-promoted segment.** `Volume::last_segment_ulid` feeds the `snapshot()` path at `volume.rs:2231`, which uses it as the snapshot marker ULID and requires `snap_ulid >= every DedupRef target segment_id` (the first-snapshot pinning invariant at `volume.rs:2233-2241`). Under normal operation `flush_wal_to_pending_as` bumps this field on every successful promote; the recovery loop must do the same. One line per recovered-promoted WAL: `last_segment_ulid = last_segment_ulid.max(Some(fresh_segment_ulid))`.

### Why this works

Each WAL file is self-contained: its records describe a batch of writes in an internally consistent state. Replaying them in ULID order reproduces the logical write order exactly, and same-LBA conflicts resolve with last-write-wins (higher ULID wins). The conditional replace during recovery-time promote ensures that a hash superseded by a later WAL's entries doesn't get clobbered when an earlier WAL is promoted — identical to the online apply phase.

Orphaned pending segments (the "flusher wrote, actor never applied" case) are absorbed by the replay: the orphan segment's entries land in extent_index during step 3, then get overwritten by the old WAL's entries in step 4 (pointing at the WAL at WAL-relative offsets), then get overwritten again by the recovery-time promote (pointing at a freshly-minted segment). The original orphan is left on disk with no extent index references — wasted space, cleaned up by a later `sweep_pending`. This is the price of not writing a commit marker linking old WAL to in-flight segment; see *Alternatives considered*.

### Crash windows covered

| Crash point | On disk after crash | Recovery action | Data loss? |
|---|---|---|---|
| Prep: before the fresh WAL is opened | `wal/<old>`; no fresh WAL; no segment | Replay old WAL as the active WAL. Identical to today's single-WAL path. | No |
| Prep: after fresh WAL opened, before PromoteJob dispatched | `wal/<old>`, `wal/<new>` (empty); no segment | Replay old WAL, promote to fresh segment, delete old WAL. Replay new WAL as the active WAL. | No |
| Flusher: mid-write (`.tmp` exists) | `wal/<old>`, `wal/<new>`, `pending/<seg>.tmp` | Drop `.tmp` (existing cleanup at `volume.rs:404-410`). Treat as the prep-phase-dispatched case. | No |
| Flusher: after rename, before PromoteComplete | `wal/<old>`, `wal/<new>`, `pending/<seg>` | Segment rebuild picks up `pending/<seg>`. WAL replay overwrites those entries with WAL-relative offsets at `old_wal_ulid`. Recovery-time promote writes a second segment at a fresh ULID and rewrites entries to it. `pending/<seg>` is orphaned; sweep_pending cleans it up later. | No (wasted disk, cleaned up later) |
| Apply: mid extent-index update | Same as above | Same as above — in-memory state is discarded. | No |
| Apply: after update, before WAL delete | Same as above | Same as above. | No |
| Apply: WAL deleted | `wal/<new>`, `pending/<seg>` | Normal recovery. Segment rebuild populates extent index. WAL replay adds only fresh-WAL writes. | No |

Every crash window either loses no data or leaks a duplicate segment that sweep cleans up. No window loses data.

### Alternatives considered

- **Commit marker file linking `old_wal_ulid → segment_ulid`.** A sidecar file (e.g. `pending/<segment_ulid>.from-<old_wal_ulid>`) fsynced before the segment rename would let recovery detect the "flusher succeeded, apply crashed" case without duplicating the segment. **Rejected for the first landing** — the duplicate-segment leak is rare, sweep cleans it up, and adding a new on-disk format shape should not be conflated with the offload change. Can be added later if the leak shows up in benchmarks or production.
- **Rename-based link** (e.g. `pending/<segment_ulid>.from-<old_wal_ulid>` staged name, renamed to `pending/<segment_ulid>` by the apply phase). Same tradeoff as the sidecar marker — moves the link into the filename but keeps the orphan case if the rename doesn't happen. No clear win over the sidecar.
- **Recover multiple WALs by in-memory merge without promoting them.** Keeps the old WAL files around after recovery, referenced by extent_index at WAL-relative offsets. Avoids the duplicate-segment issue but leaves the volume in a multi-WAL state that violates the "one active WAL" invariant and complicates the actor's mental model. Rejected.

## Apply-phase correctness

The flusher runs concurrently with new writes on the actor. Multiple promotes can be in flight. We need to verify nothing conflicts.

| Concern | Analysis |
|---|---|
| Writer calls `extent_index.insert(hash, ..)` for a hash already in a flusher batch | Writer sees the old entry (points at the old WAL) in the extent index, writes a `DedupRef` — no insert. No conflict. The apply phase later updates the entry to point at the new segment; the writer's `DedupRef` in a later WAL resolves through the updated entry. |
| Writer calls `extent_index.insert(hash, ..)` for a hash not in any flusher batch | Points at the current WAL. Never touches old-batch entries. No conflict. |
| GC handoff (`apply_gc_handoffs`) runs during an offload window | GC handoffs run on the actor, serialized with apply phases. A handoff that supersedes an old-batch hash runs before or after the apply phase. The CAS correctly leaves the GC-placed entry alone in either case. |
| Multiple promotes complete out of order | Cannot happen — the flusher is a single thread processing a FIFO channel. Completions arrive in dispatch order. |
| `needs_promote()` fires while promotes are already queued | A new prep + dispatch proceeds normally. Each job operates on a different WAL's entries with a different segment ULID. No shared mutable state between jobs. |

The conditional-replace check is the load-bearing invariant. Its precondition — that `extent_index.lookup(&hash)` returns the exact `(segment_id, body_offset)` captured at prep time — is checked per entry. Cost is one hash-map lookup and two integer compares, negligible.

## Flow control

- Multiple `PromoteJob`s can be queued on the flusher channel. Each job is a bounded ~32 MiB segment write.
- The flusher is a single thread — jobs are processed in FIFO order, completions arrive in dispatch order.
- `VolumeRequest::Flush` fsyncs the WAL and replies immediately. It does not interact with the flusher.
- `GcCheckpoint` and `Snapshot` wait for the flusher queue to drain before proceeding.
- At most one `ParkedGcCheckpoint` at a time (second one errors).
- While a GC checkpoint is parked and its promote is in flight, writes are deferred onto an internal FIFO queue (no active WAL in this window).

Under sustained write load, the flusher queue depth is bounded by `write_throughput / flusher_throughput`. If writes consistently outpace the flusher, the queue grows and old WAL files accumulate on disk. This is a capacity problem — the correct response is backpressure (future work) or a second flusher thread, not protocol complexity.

## Failure handling

If the flusher reports an error:

- The old WAL is still on disk, fsynced, and still referenced by extent index entries at WAL-relative offsets. Reads against the old hashes continue to work — the WAL file is still reachable at its path.
- The pending segment is either absent, a stale `.tmp` (swept on startup), or renamed. A renamed segment at a fresh ULID with no extent index references is a wasted segment that sweep will clean up.
- `pending_entries` for the batch have been moved into the job and are gone from memory.
- Writes continue on the current active WAL.

**Don't retry in-process.** Log loudly, continue taking writes, and rely on restart + recovery to drain the old WAL. The multi-WAL recovery path replays and promotes it. Disk-full and signing errors are rare and typically persistent; retrying in-process adds code paths for negligible benefit.

For the GC checkpoint case, a flusher error means the parked state is popped and the error is returned to the GC caller. The coordinator retries on its next tick.

## Testing

1. **Unit test: conditional replace.** A constructed extent index where one entry has been superseded by a later writer; the apply phase leaves it alone. Regression coverage for the CAS semantics.
2. **Unit test: recovery replay of two WAL files.** Construct a volume directory with two WAL files at known ULIDs, invoke `Volume::open`, assert the lower WAL is promoted to a fresh segment and deleted, the higher WAL is the active WAL, and the LBA map matches a ground-truth oracle.
3. **Proptest: interleaved writes and promotes.** Existing simulation model needs a new op: "schedule a promote to complete after N writes." Invariant: every `read(lba)` returns the most recent `write(lba, ..)` observed by the caller. Checks read-your-writes across the offload window.
4. **Proptest: crash between rename and PromoteComplete.** Extend the crash-recovery oracle to fault-inject at this point. Invariant: rebuilt LBA map equals a fault-free run.
5. **Proptest: interleaved writes and gc_checkpoint.** Same simulation model. Invariant: writes submitted before `gc_checkpoint` returns are visible in the post-checkpoint state; writes submitted after use the fresh WAL.
6. **Unit test: concurrent GcCheckpoint rejected.** Queue two `GcCheckpoint` requests; the second errors before the first completes.
7. **Unit test: write deferred during phase 1→2.** Dispatch a `GcCheckpoint` with a slow flusher; submit a Write during the window; assert the Write completes after phase 2 without an error.
8. **Bench: write tail latency under sustained load.** Compare current code to offloaded code under a workload that triggers promote every 8 MiB. Target: p99 write latency drops by at least 10× during sustained writes.
9. **Bench: GC latency impact on writes.** With the coordinator running GC every 60 s, measure per-write p99 during a 10-minute sustained write workload. Target: no visible correlation between GC ticks and write latency spikes.
10. **Manual: large mkfs on NBD.** The `mkfs.ext4` path is a natural stress test — the write pattern triggers multiple promotes back-to-back. Success if total mkfs time does not regress and per-write latency becomes visibly smoother.

## Landing sequence

The work breaks into roughly three landings. Within each landing the steps are small and can be PR'd independently.

### Landing 1 — prerequisites, no behavior change

1. **Make extent-index inserts in `flush_wal_to_pending_as` conditional on the current location.** No behavioral change (still runs on actor), wires in the CAS semantics. Covered by unit test 1.
2. **Split `segment::promote` into `write_and_commit` (no WAL delete) + a wrapper that still deletes the WAL** for in-process callers and offline tooling.
3. **Convert `signer` from `Box<dyn SegmentSigner>` to `Arc<dyn SegmentSigner>`** on `Volume` so it can be shared with the flusher thread.

### Landing 2 — recovery replay of all WALs

4. **Replay all WAL files in `Volume::open_impl`**, promoting non-latest WALs to fresh segments. Keeps the legacy same-ULID dedup-retain loop. Lands independently — no offload yet, no concurrency, but future-proofs recovery for the offload work. Covered by unit test 2.
5. **Switch the in-process `flush_wal_to_pending` to mint a fresh segment ULID.** Still runs on the actor, still synchronous. Validates the fresh-ULID pattern under the existing concurrency model. Covered by unit test 1 and existing tests.

**Deferring the WAL delete — not in Landing 2.** The earlier draft of step 5 also required deferring the old-WAL unlink until after `publish_snapshot`, so that a pre-publish reader holding a stale extent index entry could still cold-open `wal/<old_wal_ulid>` during the publish window. That deferral is the natural shape of Landing 3 (the flusher's apply phase runs after publish by construction), but retrofitting it onto the inline actor path required (a) a persistent `pending_wal_cleanup` stash on `Volume`, (b) a public `finalize_wal_cleanup` entry point for the actor to call after its `publish_snapshot`, and (c) either changing the public `flush_wal` / `gc_checkpoint` / `snapshot` contract or threading a defer-vs-finalize flag through them — the latter touching ~100 test sites across `elide-core` and `elide-coordinator`.

The correctness benefit of deferring on the inline path is narrow: with the fresh segment ULID alone, the cold-cache race turns from "silent wrong bytes via `pending/<same_ulid>`" into "`NotFound` during the brief delete window" — an error, not corruption. Deferring the unlink further shrinks that `NotFound` window but does not close it, because a reader that loaded the pre-publish snapshot and opens the file *after* the eventual unlink still gets `NotFound`. We accept the narrow window for Landing 2; Landing 3 fixes it for free via the off-actor structure. The comment on the inline unlink at the end of `flush_wal_to_pending_as` calls this out so the next reader knows where the deferral will slot in.

Landing 2 is self-contained: it fixes the latent cold-cache race (wrong-bytes → error) and the recovery assumption without introducing any cross-thread coordination. Could be released on its own.

### Landing 3 — the offload itself

6. **Introduce the flusher thread** and wire `PromoteJob` / `PromoteComplete` plumbing (including the shutdown path). No job types yet — just a thread that receives `PromoteJob`s and acks.
7. **Route write-path and idle-tick promotes through the flusher.** Split `VolumeRequest::Flush` into WAL fsync (no promote). Multiple promotes can be queued. Land proptest 3 and 4, bench 8.
8. **Add the deferred-write queue and route `gc_checkpoint` through the flusher** with the parked-reply state machine, the drain-before-GC gate, and the concurrent-GC-error check. Land proptest 5, unit tests 6–7, bench 9.

Landing 3 is where the actual concurrency work lives and deserves the most careful review.

## Resolved questions

- **Mint state across the recovery-time promote.** Verified safe. The mint has no invariant beyond monotonicity (`ulid_mint.rs`) and no code reads `mint.last`. The related concern — `last_segment_ulid` — is called out above in *Recovery semantics*: the recovery loop must bump it for every freshly-minted segment ULID so that later `snapshot()` calls preserve the first-snapshot pinning invariant at `volume.rs:2233-2241`.
- **`gc_checkpoint` return latency / coordinator assumption.** Verified safe. The coordinator at `elide-coordinator/src/tasks.rs:309` awaits `gc_checkpoint` as opaque async IPC. Its only post-conditions are (1) `pending/<u_flush>` exists on disk — used by `gc_fork` at `elide-coordinator/src/gc.rs:195` which rebuilds from `pending/` + `index/`; (2) the fresh WAL is open; (3) `(u_repack, u_sweep)` are returned. The offloaded path preserves all three: the flusher queue is drained, the GC promote is applied, the fresh WAL is opened, and the reply is sent. Coordinator observes identical post-conditions.

## Open questions

- **Metrics.** `flusher_queue_depth`, `promote_duration`, `deferred_write_queue_depth`, `parked_gc_checkpoint_duration`. All are directly useful for operators and fall out of the implementation naturally.
- **Backpressure.** Under sustained writes that outpace the flusher, the queue grows and old WAL files accumulate. Future work: when queue depth exceeds a threshold, the actor could slow or block writes. Not in this landing — wait for evidence from benchmarks.
- **Deferred-queue ordering during GC.** After the GC checkpoint completes, the actor drains the deferred-write queue before pulling from the crossbeam channel. This means the coordinator's next IPC (`apply_gc_handoffs`) queues behind any deferred writes. Correct semantically (those writes were accepted before `apply_gc_handoffs` arrived), but worth noting because it extends `apply_gc_handoffs` latency by "time to drain deferred writes." Under normal load the deferred queue is small (bounded by flusher time × arrival rate) so this is negligible.
