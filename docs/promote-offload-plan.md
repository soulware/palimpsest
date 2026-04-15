# Plan: offload WAL promotion off the volume actor

**Status:** Proposal. Depends on [actor-offload-plan.md](actor-offload-plan.md) — this is the first landing step for that broader plan.

## Goal

Move the expensive part of `Volume::flush_wal_to_pending` off the actor thread onto a dedicated flusher thread, so that a write burst large enough to trigger promotion does not stall subsequent writes for the 10–500 ms it takes to serialize a 32 MiB segment and fsync it.

The same offload path is used by `gc_checkpoint`, which today synchronously promotes the current WAL on the actor and blocks every queued write for the duration. Routing GC through the flusher means one machinery serves both latency-relevant callers.

**Non-goals.** Speeding up promotion itself. Offloading any other maintenance op (covered by the parent plan). Changing the read path.

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
enum VolumeRequest {
    // ... existing variants ...
    PromoteComplete(PromoteResult),
}

struct PromoteJob {
    segment_ulid: Ulid,                  // fresh, distinct from the WAL ulid
    old_wal_ulid: Ulid,
    old_wal_path: PathBuf,
    entries: Vec<SegmentEntry>,          // moved out of pending_entries
    signer: Arc<dyn SegmentSigner>,
    pending_dir: PathBuf,
    continuation: PromoteContinuation,
}

enum PromoteContinuation {
    /// Normal write-path / idle-tick promote. Apply phase publishes the
    /// snapshot and returns to the select loop.
    Normal,
    /// Dispatched by gc_checkpoint. Apply phase publishes the snapshot,
    /// then consumes the actor's `parked_gc_checkpoint` state, opens a
    /// fresh WAL with the pre-minted `u_wal`, and sends (u_repack,
    /// u_sweep) back on the parked reply channel.
    GcCheckpoint,
}

struct PromoteResult {
    segment_ulid: Ulid,
    old_wal_ulid: Ulid,
    old_wal_path: PathBuf,
    body_section_start: u64,
    entries: Vec<SegmentEntry>,          // returned; `stored_offset` now segment-relative
    continuation: PromoteContinuation,
    result: io::Result<()>,
}
```

Actor state additions:

```rust
struct VolumeActor {
    // ... existing fields ...
    promote_in_flight: bool,
    parked_gc_checkpoint: Option<ParkedGcCheckpoint>,
}

struct ParkedGcCheckpoint {
    u_repack: Ulid,
    u_sweep: Ulid,
    u_wal: Ulid,                         // for the fresh WAL opened in phase 2
    reply: Sender<io::Result<(Ulid, Ulid)>>,
}
```

The flusher is a single long-lived thread owning one crossbeam receiver for `PromoteJob`. The actor owns the sender. On shutdown the actor drops its sender; the flusher exits when `recv()` returns `Disconnected`. `PromoteComplete` messages come back on the actor's main `VolumeRequest` channel so they're processed inline with writes and other requests.

The invariant: **`promote_in_flight` is true iff exactly one `PromoteJob` has been dispatched and the corresponding `PromoteComplete` has not yet been processed.** Only one promote is ever in flight.

### Sequencing: normal write-path promote

#### Actor: prep phase (triggered from `actor.rs:210` when `needs_promote()` fires)

1. If `promote_in_flight` → skip. The current write's reply has already been sent; the fresh WAL grows past its soft threshold until the in-flight job drains.
2. `volume.wal.fsync()` — durability barrier for WAL data.
3. `let old_wal_ulid = volume.wal_ulid;`
4. `let old_wal_path = volume.wal_path.clone();`
5. `let entries = std::mem::take(&mut volume.pending_entries);`
6. `let segment_ulid = volume.mint.next();` — **fresh ULID, not the WAL ULID**.
7. `volume.open_fresh_wal()` — new `wal/<new_wal_ulid>` ready to accept writes. Volume's `wal_ulid` / `wal_path` / `pending_entries` are now the fresh WAL's.
8. `self.promote_in_flight = true;`
9. Send `PromoteJob { segment_ulid, old_wal_ulid, old_wal_path, entries, signer: Arc::clone(&signer), pending_dir, continuation: PromoteContinuation::Normal }` to the flusher.
10. Return to the select loop. Immediately ready to service the next write.

The actor hands off ownership of `entries` by move. It never touches the old batch again until the completion message arrives.

#### Flusher: heavy middle

1. `let body_section_start = segment::write_and_commit(&pending_dir, segment_ulid, &mut entries, signer.as_ref())?;`
   - Writes `pending/<segment_ulid>.tmp` via `write_segment`. `write_segment` reassigns each entry's `stored_offset` to segment-relative.
   - Renames to `pending/<segment_ulid>`.
   - `fsync_dir`.
   - **Does not delete the old WAL** — that is the actor's responsibility in the apply phase.
2. Send `VolumeRequest::PromoteComplete(PromoteResult { .. })` back via the actor's main channel. The `continuation` field is carried through unchanged.

`segment::write_and_commit` is a new helper: `segment::promote` minus the final `fs::remove_file(wal_path)`. Existing `segment::promote` is rewritten to call `write_and_commit` then delete the WAL, preserving its API for in-process tests and offline tooling.

#### Actor: apply phase (on `PromoteComplete`)

Runs at the top of the select loop, before any write queued after the job was dispatched.

1. Assert `self.promote_in_flight == true`.
2. If `result.result` is an error: log, set `promote_in_flight = false`, **do not touch** the old WAL or extent index (the old WAL still exists on disk, its entries still point at it at WAL-relative offsets, so reads continue to work). If `continuation == GcCheckpoint`, pop `parked_gc_checkpoint` and send the error back to the parked reply. Return to the select loop. The idle tick or the next write-path threshold will retry the promote.
3. On success, update `extent_index`. For each entry in `result.entries`:
   - Skip `DedupRef` / `Zero` / `Delta` as today (`volume.rs:2126`).
   - Look up current location: `self.extent_index.lookup(&entry.hash)`.
   - **Conditional replace** — only rewrite the entry if it currently matches `(segment_id == old_wal_ulid && body_offset == entry.wal_offset_before_promote)`. If a concurrent write or a GC handoff has already superseded it, leave it alone.
   - Otherwise `Arc::make_mut(&mut self.extent_index).insert(...)` with the new `(segment_ulid, stored_offset, body_section_start, BodySource::Local)`.
4. `self.publish_snapshot()` — bumps `flush_gen`. Readers loading the new snapshot see segment-relative offsets pointing at `pending/<segment_ulid>` and flush their file caches.
5. **Only now** delete the old WAL: `fs::remove_file(&old_wal_path)`. Stale readers that loaded the pre-apply snapshot still have entries with `segment_id = old_wal_ulid`; as long as they're still referencing that snapshot, the WAL file stays reachable via its original path or via their cached fd.
6. `self.promote_in_flight = false;`
7. If `continuation == GcCheckpoint`, run the GC continuation (next section). Otherwise return to the select loop.

The "delete after publish" ordering is stricter than the current code (which deletes before updating the extent index) and, together with the fresh segment ULID, fixes the latent cold-cache race: after step 5, the only code paths that could still reach the old WAL file are readers holding a pre-apply snapshot whose entries use WAL-relative offsets that only make sense against the WAL file. Post-apply readers use segment-relative offsets pointing at `pending/<segment_ulid>`, a path that never collides with the WAL's ULID.

### Sequencing: gc_checkpoint through the flusher (continuation pattern)

`gc_checkpoint` becomes a two-phase operation spanning the promote round-trip. Phase 1 runs when the actor receives `VolumeRequest::GcCheckpoint`; phase 2 runs when the corresponding `PromoteComplete` lands.

#### Phase 1 (on `GcCheckpoint` message)

1. If `self.parked_gc_checkpoint.is_some()` → error back on the reply channel with "concurrent gc_checkpoint not allowed." See *Concurrent GcCheckpoint* below for rationale.
2. If `self.promote_in_flight` → defer. The simplest way to defer is to push the message back onto the select loop: send `VolumeRequest::GcCheckpoint { reply }` from the actor to itself (or equivalently, stash the reply in a small "deferred requests" queue that is drained when `promote_in_flight` becomes false). Either mechanism lets subsequent writes continue to be serviced while the flusher drains.
3. Otherwise:
   - `let u_repack = volume.mint.next();`
   - `let u_sweep = volume.mint.next();`
   - `let u_flush = volume.mint.next();` — the segment ULID for this promote.
   - `let u_wal = volume.mint.next();` — the fresh WAL ULID after the promote.
   - `volume.wal.fsync();`
   - `let old_wal_ulid = volume.wal_ulid;`
   - `let old_wal_path = volume.wal_path.clone();`
   - `let entries = std::mem::take(&mut volume.pending_entries);`
   - **Do not open the fresh WAL here.** The fresh WAL must be opened in phase 2 with the pre-minted `u_wal`, because `u_wal` was minted after `u_flush` specifically to sort above it for crash recovery. Opening it earlier would allow a write to sneak in between `u_flush`'s dispatch and the fresh WAL's activation, and that write's WAL entries would have a ULID below `u_wal`. To prevent writes in this window, the actor temporarily parks writes the same way it parks `GcCheckpoint`: if `parked_gc_checkpoint.is_some()` at the top of a Write message, push the Write back for later processing, or reply with `io::ErrorKind::Interrupted`. See *Handling writes during phase 1→2* below.
   - `self.parked_gc_checkpoint = Some(ParkedGcCheckpoint { u_repack, u_sweep, u_wal, reply });`
   - `self.promote_in_flight = true;`
   - Dispatch `PromoteJob { segment_ulid: u_flush, old_wal_ulid, old_wal_path, entries, signer, pending_dir, continuation: GcCheckpoint }` to the flusher.
   - Return to the select loop without replying.

#### Phase 2 (on `PromoteComplete` with `continuation == GcCheckpoint`)

Runs after the normal apply phase (steps 1–6 above). At this point the extent index is up to date, the snapshot is published, and the old WAL has been deleted — exactly the same state as a successful normal promote.

1. `let parked = self.parked_gc_checkpoint.take().expect("continuation tag without parked state");`
2. Open the fresh WAL at `wal/<parked.u_wal>`:
   - `let (wal, wal_ulid, wal_path, pending_entries) = create_fresh_wal(&wal_dir, parked.u_wal)?;`
   - Assign back to `volume.wal`, `volume.wal_ulid`, `volume.wal_path`, `volume.pending_entries`.
3. Send `Ok((parked.u_repack, parked.u_sweep))` on `parked.reply`.
4. Unpark any writes that were deferred during phase 1→2 (see below).

If opening the fresh WAL fails, send the error on `parked.reply` and leave the volume in a no-WAL state. The next Write request will fail because `volume.wal` is invalid; the actor can try to recover by minting yet another ULID. This is a degenerate case — disk full — and we already accept the same failure mode in `create_fresh_wal` today.

#### Handling writes during phase 1→2

Between `GcCheckpoint` phase 1 and phase 2, the volume has no active WAL. Writes cannot proceed. We have two options:

- **Defer writes**, the same way `GcCheckpoint` itself is deferred while a promote is in flight. A Write arriving during this window is pushed back onto the deferred queue and retried when the queue is drained (on `PromoteComplete` phase 2 completion). This imposes a brief write stall — bounded by the flusher's segment write time, typically 10–100 ms — but no write is rejected.
- **Reject writes** with `io::ErrorKind::Interrupted`. The caller retries. Simpler to implement but visible to the writer.

GC checkpoints are rare (idle-tick-driven, coordinator-driven) and the write stall is short. **Go with defer.** The deferred queue is a `VecDeque<VolumeRequest>` on the actor and is drained in FIFO order when the parked GC state is cleared. Since GC is already expected to cause a brief pause from the caller's perspective (the point of this whole plan is to shorten it, not eliminate it entirely), the bounded-stall semantics are the right fit.

With the deferred queue in place, the GC-in-flight-while-another-promote-is-in-flight case from phase 1 step 2 can be handled uniformly: `GcCheckpoint` is just another message that gets deferred while `promote_in_flight`.

#### Concurrent GcCheckpoint is an error

If a second `GcCheckpoint` request arrives while `parked_gc_checkpoint.is_some()`, the actor replies with an error. Rationale: `gc_checkpoint` is coordinator-driven, and the coordinator is expected to serialize its own checkpoints. A concurrent checkpoint signals a coordinator bug and should be loud. Queueing it silently would mask the bug and delay the noise.

This is the only message that explicitly errors on queue; all other messages (Write, Flush, idle-tick work) are processed normally during the phase 1→2 window, with Writes specifically using the defer queue.

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

Because all four GC ULIDs are pre-minted before any I/O, and because the actor's main loop is single-threaded, the mint ordering `u_repack < u_sweep < u_flush < u_wal` is guaranteed even though the segment write happens concurrently on the flusher. The flusher never mints.

No collision with concurrent writes is possible:

- A write-path promote mints `segment_ulid` and `new_wal_ulid` on the actor. Any concurrent `GcCheckpoint` is deferred until after the promote's apply phase, so GC ULIDs are minted strictly after `new_wal_ulid` and sort above it.
- `gc_checkpoint` mints all four ULIDs on the actor in phase 1. Any concurrent write-path promote is deferred for the same reason.
- No two promotes are ever in flight simultaneously, so no two segments are ever minting distinct `segment_ulid`s at the same time.

The GC ULID-ordering invariant from `volume.rs:1313` is preserved end-to-end: segments from a `gc_checkpoint` sort in crash-recovery rebuild order as `(existing_segments) < u_repack < u_sweep < u_flush < u_wal < (fresh_wal_segments)`.

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

## Apply-phase correctness without the actor lock

The "heavy middle" runs concurrently with new writes on the fresh WAL. We need to verify nothing the flusher does can conflict with a concurrent writer.

| Concern | Analysis |
|---|---|
| Writer calls `extent_index.insert(hash, ..)` for a hash already in the flusher's batch | Writer sees the old entry (points at old WAL) in the extent index, writes a `DedupRef` — no insert. No conflict. The apply phase later updates the entry to point at the new segment; the writer's `DedupRef` in the fresh WAL resolves through the updated entry. |
| Writer calls `extent_index.insert(hash, ..)` for a hash not in the flusher's batch | Points at the fresh WAL. Never touches old-batch entries. No conflict. |
| GC handoff (`apply_gc_handoffs`) runs during the offload window | GC handoffs run on the actor, serialized with the apply phase. A handoff that supersedes an old-batch hash runs before the apply phase can process it, and the apply phase's conditional replace correctly leaves the GC-placed entry alone. |
| A second `GcCheckpoint` arrives | Errored (*Concurrent GcCheckpoint is an error*). |
| Another write-path promote is triggered | Deferred via `promote_in_flight`. The fresh WAL grows past the soft threshold until the in-flight job drains. |

The conditional-replace check is the load-bearing invariant. Its precondition — that `extent_index.lookup(&hash)` returns the exact `(segment_id, body_offset)` the entry had at prep time — is checked per entry. Cost is one hash-map lookup and two integer compares, negligible.

## Flow control

- At most one `PromoteJob` is in flight at a time (`promote_in_flight` flag).
- At most one `ParkedGcCheckpoint` at a time (second one errors).
- While `promote_in_flight`, threshold-triggered promotes are skipped and the fresh WAL grows past its soft threshold.
- While `parked_gc_checkpoint.is_some()` and phase 2 hasn't run, writes are deferred onto an internal FIFO queue on the actor.

The soft WAL cap is not a correctness issue — the WAL has no hard upper bound other than free disk space. In the common case the promote takes 10s–100s of ms and the fresh WAL grows by at most a few MiB past the threshold. If the offload turns out to be slow enough that the fresh WAL grows unboundedly under sustained write load, the solution is a worker pool (one additional flusher thread), not more single-flusher protocol complexity.

## Failure handling

If the flusher reports an error:

- The old WAL is still on disk, fsynced, and still referenced by extent index entries at WAL-relative offsets. Reads against the old hashes continue to work — the WAL file is still reachable at its path.
- The pending segment is either absent, a stale `.tmp` (swept on startup), or renamed. A renamed segment at a fresh ULID with no extent index references is a wasted segment that sweep will clean up.
- `pending_entries` for the batch have been moved into the job and are gone from memory.
- The fresh WAL continues to collect new writes.
- `promote_in_flight` is cleared; the next threshold or idle tick will try again. Because the old WAL is still active (referenced by extent index at WAL-relative offsets, its file present), the retry path has to work off the extent index + WAL file rather than in-memory `pending_entries`. The simplest option is to not retry in-process at all: log the error, let the next startup re-run the promote via the recovery path (which replays the old WAL into a fresh segment). The in-memory state is slightly inconsistent (extent index points at the old WAL, but the actor thinks the active WAL is the fresh one) until the next restart.

**Recommendation: don't retry in-process on flusher error.** Log loudly, continue taking writes on the fresh WAL, and rely on restart + recovery to drain the old WAL. Disk-full and signing errors are rare and typically persistent; retrying in-process adds code paths for negligible benefit.

For the GC-continuation case, a flusher error means phase 2's `parked_gc_checkpoint` is popped and the error is returned to the GC caller. No lasting state change — the coordinator retries on its next tick.

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
7. **Route write-path and idle-tick promotes through the flusher** with `continuation: Normal`. Add `promote_in_flight` flag. Land proptest 3 and 4, bench 8.
8. **Add the deferred-write queue and route `gc_checkpoint` through the flusher** with `continuation: GcCheckpoint`, the parked-reply state machine, and the concurrent-GC-error check. Land proptest 5, unit tests 6–7, bench 9.

Landing 3 is where the actual concurrency work lives and deserves the most careful review.

## Resolved questions

- **Mint state across the recovery-time promote.** Verified safe. The mint has no invariant beyond monotonicity (`ulid_mint.rs`) and no code reads `mint.last`. The related concern — `last_segment_ulid` — is called out above in *Recovery semantics*: the recovery loop must bump it for every freshly-minted segment ULID so that later `snapshot()` calls preserve the first-snapshot pinning invariant at `volume.rs:2233-2241`.
- **`gc_checkpoint` return latency / coordinator assumption.** Verified safe. The coordinator at `elide-coordinator/src/tasks.rs:309` awaits `gc_checkpoint` as opaque async IPC. Its only post-conditions are (1) `pending/<u_flush>` exists on disk — used by `gc_fork` at `elide-coordinator/src/gc.rs:195` which rebuilds from `pending/` + `index/`; (2) the fresh WAL is open; (3) `(u_repack, u_sweep)` are returned. Option Y preserves all three: phase 2 runs the apply phase (segment + extent index + snapshot publish + WAL delete) and opens the fresh WAL before sending the reply. Coordinator observes identical post-conditions.

## Open questions

- **Metrics.** `promote_in_flight_duration`, `deferred_write_queue_depth`, `parked_gc_checkpoint_duration`. All three are directly useful for operators and fall out of the implementation naturally.
- **Deferred-queue ordering during GC.** After phase 2 sends the reply, the actor drains the deferred-write queue before pulling from the crossbeam channel. This means the coordinator's next IPC (`apply_gc_handoffs`) queues behind any deferred writes. Correct semantically (those writes were accepted before `apply_gc_handoffs` arrived), but worth noting because it extends `apply_gc_handoffs` latency by "time to drain deferred writes." Under normal load the deferred queue is small (bounded by flusher time × arrival rate) so this is negligible. If benchmarks show pathological cases, we could let the coordinator's messages skip the deferred queue — but I'd wait to see evidence before adding that complexity.
