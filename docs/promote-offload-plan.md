# Plan: offload WAL promotion off the volume actor

**Status:** Landed. All three landings merged. Depends on [actor-offload-plan.md](actor-offload-plan.md) — this was the first landing step for that broader plan.

**Post-landing evolution.** PR #58 generalized the flusher thread into a shared worker thread that dispatches jobs via a `WorkerJob` enum (currently `Promote` and `GcHandoff`). No behavior change for promotes. Terminology throughout this doc uses "flusher" for historical continuity with the PRs it describes; the current code calls it `worker_thread` and the channels carry `WorkerJob` / `WorkerResult`.

## Goal

Move the expensive part of `Volume::flush_wal_to_pending` off the actor thread onto a dedicated flusher thread, so that a write burst large enough to trigger promotion does not stall subsequent writes for the 10–500 ms it takes to serialize a 32 MiB segment and fsync it.

The same offload path is used by `gc_checkpoint`, which previously synchronously promoted the current WAL on the actor and blocked every queued write for the duration. Routing GC through the flusher means one machinery serves both latency-relevant callers.

**Non-goals.** Speeding up promotion itself. Offloading any other maintenance op (covered by the parent plan). Changing the read path.

## Flush vs promote

`flush_wal()` historically conflated two operations with different purposes:

- **Flush** = `wal.fsync()`. Durability barrier. The WAL is the durability boundary — after fsync, data survives a crash. This is what `NBD_CMD_FLUSH` requires.
- **Promote** = serialize WAL entries into a `pending/<ulid>` segment, update the extent index to segment-relative offsets, delete the old WAL, open a fresh WAL. Housekeeping for the segment lifecycle.

The offload separates these. `VolumeRequest::Flush` is a WAL fsync of the current WAL plus (if a promote is in flight) a park on the promote generation counter. No segment I/O, no *actor* blocking on the worker — the FLUSH caller waits, but new writes queued on the channel are still served onto the fresh WAL. Promotion itself is triggered independently by the threshold check (`needs_promote()`) and the idle tick, dispatched asynchronously to the flusher thread, and the *old* WAL's `fsync()` runs as the first step of the worker job rather than on the actor.

Callers that need data in `pending/` before proceeding (the coordinator's snapshot path, `gc_checkpoint`) depend on promote completion, not flush. Those callers use `Snapshot` or `GcCheckpoint` IPC, which wait for any in-flight promote before proceeding.

## Prior flow (pre-offload)

`VolumeActor::run` on a successful write:

1. `volume.write(&data)` — WAL append + `extent_index.insert(hash, { segment_id: wal_ulid, body_offset: wal_relative_offset, .. })`.
2. Publish a new `ReadSnapshot` (no `flush_gen` bump).
3. Reply to the caller.
4. `if volume.needs_promote()` → `volume.flush_wal()` → `flush_wal_to_pending_as(self.wal_ulid)`.

`flush_wal_to_pending_as`:

1. `wal.fsync()`.
2. `segment::promote(wal_path, segment_ulid, pending_dir, &mut pending_entries, signer)` — writes `pending/<ulid>.tmp` via `write_segment`, renames to `pending/<ulid>`, fsyncs the directory, **deletes the WAL file**.
3. Loops over `pending_entries` and rewrites `extent_index` entries to segment-relative offsets via `Arc::make_mut`.
4. `evict_cached_segment(wal_ulid)` on the volume's internal `file_cache`.
5. Returns; actor calls `publish_snapshot` which bumps `flush_gen`.

The segment file reused the WAL's ULID (`segment_ulid == self.wal_ulid`). `find_segment_in_dirs` searches `wal/` before `pending/`, so during the promote window stale readers found the WAL file at its original path. After the delete, stale readers with cached fds continued reading via the deleted inode (Linux keeps it alive); stale readers without a cached fd fell through to `pending/<same_ulid>` and could read **wrong bytes** because their extent index entries still held WAL-relative offsets. **This was a latent race**, fixed by the fresh-ULID change in Landing 2.

`gc_checkpoint` was the one existing caller that already broke the WAL-ULID-reuse convention: it pre-mints `u_repack < u_sweep < u_flush` from the mint and calls `flush_wal_to_pending_as(u_flush)` so the segment lands at `pending/<u_flush>`, not at the WAL's original ULID. The offload inherited this pattern and extended it to the common case. No `u_wal` is reserved — the post-checkpoint WAL is opened lazily on the next write (see "Lazy WAL" below).

## Current flow

### Data structures

```rust
pub struct PromoteJob {
    pub segment_ulid: Ulid,               // fresh, distinct from the WAL ulid
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub entries: Vec<SegmentEntry>,       // moved out of pending_entries
    pub pre_promote_offsets: Vec<Option<u64>>, // CAS tokens snapshotted at prep time
    pub signer: Arc<dyn SegmentSigner>,
    pub pending_dir: PathBuf,
}

pub struct PromoteResult {
    pub segment_ulid: Ulid,
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub body_section_start: u64,
    pub entries: Vec<SegmentEntry>,       // stored_offset now segment-relative
    pub pre_promote_offsets: Vec<Option<u64>>,
}

pub struct GcCheckpointPrep {
    pub u_repack: Ulid,
    pub u_sweep: Ulid,
    pub u_flush: Ulid,                    // segment ULID for the GC promote
    pub job: Option<PromoteJob>,          // None if WAL was empty
}
```

The flusher is a single long-lived thread. Two bounded crossbeam channels connect it to the actor:

- **Job channel**: `bounded::<PromoteJob>(4)` — actor sends, flusher receives. Capacity 4 (~128 MiB of WAL data at 32 MiB per segment).
- **Result channel**: `bounded::<io::Result<PromoteResult>>(4)` — flusher sends, actor receives.

Results arrive on a **dedicated channel**, not as a `VolumeRequest` variant. The actor's `select!` loop has three arms: the main `VolumeRequest` channel, the flusher result channel, and a 10-second idle tick.

On shutdown the actor drops its job sender (`flusher_tx.take()`); the flusher exits when `recv()` returns `Disconnected`. The actor then drains any remaining results (applying successful promotes) and joins the thread.

The actor tracks `promotes_in_flight: usize` — incremented on dispatch, decremented on every result (success or error).

### Parking structures

Two types of caller can be waiting for a specific promote to complete:

```rust
struct ParkedGcCheckpoint {
    u_repack: Ulid,
    u_sweep: Ulid,
    u_flush: Ulid,       // matched against the completed segment ULID
    reply: Sender<io::Result<(Ulid, Ulid)>>,
}

struct ParkedPromoteWal {
    segment_ulid: Ulid,  // matched against the completed segment ULID
    reply: Sender<io::Result<()>>,
}
```

- `parked_gc: Option<ParkedGcCheckpoint>` — at most one.
- `parked_promote_wal: Vec<ParkedPromoteWal>` — multiple can be parked if several `PromoteWal` requests arrive while the flusher is busy.

When a promote completes, the actor walks both structures and resolves any reply whose ULID matches.

### Sequencing: normal write-path promote

#### Actor: prep phase (triggered when `needs_promote()` fires after a write reply, or on idle tick)

`dispatch_promote()` calls `Volume::prepare_promote()`:

1. Return `Ok(None)` immediately if `pending_entries` is empty.
2. Snapshot CAS tokens: `pre_promote_offsets` from the extent index for each Data/Inline entry.
3. `let old_wal_ulid = self.wal_ulid;`
4. `let old_wal_path = self.wal_path.clone();`
5. `let entries = std::mem::take(&mut self.pending_entries);`
6. `let segment_ulid = self.mint.next();` — **fresh ULID, not the WAL ULID**.
7. Open fresh WAL: `create_fresh_wal(&wal_dir, self.mint.next())`. Volume's `wal_ulid` / `wal_path` / `pending_entries` are now the fresh WAL's. New writes start flowing immediately.
8. Return `Ok(Some(PromoteJob { .. }))`.

The actor sends the job to the flusher channel, increments `promotes_in_flight`, bumps `promote_gen`, pushes `old_wal_path` onto `inflight_old_wals`, and returns to the select loop. If `needs_promote()` fires again before the first promote completes, the actor preps and dispatches another job — multiple promotes can be queued on the flusher channel.

**The old WAL's `fsync()` no longer runs on the actor.** It is now the first step of the worker's `execute_promote` below. This matches the way a real block device keeps accepting commands while a FLUSH is in flight at the controller: the volume actor keeps processing writes onto the fresh WAL while the worker makes the old one durable in parallel. NBD `Flush` parks on `promote_gen` / `completed_gen` so the client still sees a strict durability barrier (see *Flush parking* below).

#### Flusher: heavy middle

1. `std::fs::File::open(&old_wal_path)?.sync_data()?` — the old-WAL durability barrier that `prepare_promote` used to run on the actor.
2. `let body_section_start = segment::write_and_commit(&pending_dir, segment_ulid, &mut entries, signer)?;`
   - Writes `pending/<segment_ulid>.tmp` via `write_segment`. `write_segment` reassigns each entry's `stored_offset` to segment-relative.
   - Renames to `pending/<segment_ulid>`.
   - `fsync_dir`.
   - **Does not delete the old WAL** — that is the actor's responsibility in the apply phase.
3. Send `Ok(PromoteResult { .. })` on the result channel.

`segment::write_and_commit` (Landing 1): `segment::promote` minus the final `fs::remove_file(wal_path)`.

#### Actor: apply phase (on flusher result channel)

1. If the result is an error: log, decrement `promotes_in_flight`, pop the FIFO head of `inflight_old_wals`, and perform a fallback `sync_data()` on that path on the actor thread — the worker may have failed before or after its own fsync ran, so the actor guarantees durability itself before bumping `completed_gen` and resolving any parked flushes. **Do not touch** the old WAL or extent index otherwise (the old WAL still exists on disk, its entries still point at it at WAL-relative offsets, so reads continue to work). Return to the select loop. The old WAL is drained on restart via the multi-WAL recovery path.
2. On success, pop the FIFO head of `inflight_old_wals`, bump `completed_gen`, resolve any parked flushes whose `needed_gen <= completed_gen`, and call `Volume::apply_promote(&result)`:
   a. Set `has_new_segments = true`, `last_segment_ulid = Some(result.segment_ulid)`.
   b. **CAS loop** — for each Data/Inline entry, paired with the corresponding `pre_promote_offsets` token: call `extent_index.replace_if_matches(hash, old_wal_ulid, old_wal_offset, new_location)`. Only rewrites the entry if it currently matches `(segment_id == old_wal_ulid && body_offset == pre_promote_offset)`. If a concurrent write, a GC handoff, or a later promote's apply has already superseded it, leave it alone.
   c. Log entry counts.
   d. Delete the old WAL: `fs::remove_file(&old_wal_path)`. Evict the cached file descriptor.
3. `publish_snapshot()` — bumps `flush_gen`. Readers loading the new snapshot see segment-relative offsets pointing at `pending/<segment_ulid>`.
4. Resolve any parked operations waiting for this segment ULID (see Parking structures above).

#### Actor: NBD `Flush` parking

`VolumeRequest::Flush` is split into two phases on the actor:

1. `self.volume.wal_fsync()` — fsync the *current* (fresh) WAL. This is identical to the previous behaviour: `Flush` on its own never blocks on the worker's promote.
2. `park_or_resolve_flush(reply)` — if `completed_gen >= promote_gen`, reply `Ok` immediately (no promote is in flight). Otherwise push a `ParkedFlush { needed_gen: promote_gen, reply }` onto `parked_flushes` and return to the select loop. The actor keeps processing writes and other requests while the flush is parked.

Every `WorkerResult::Promote` (success or error) bumps `completed_gen` and runs `resolve_parked_flushes`, which drains any entry whose `needed_gen <= completed_gen` and sends `Ok` (or the error from the fallback fsync on the error path). On shutdown, `shutdown_worker` drains the worker results the same way, so parked flushes resolve as long as their promote has a result to report; any flushes still parked after the channel closes have their reply senders dropped (caller sees `RecvError`).

This preserves NBD's FLUSH contract — every write ack'd before the flush is durable by the time the flush reply arrives — without blocking the actor on disk I/O. The analogy is a real block device: the controller keeps accepting commands while a FLUSH is in flight; only the caller of FLUSH waits.

### Sequencing: `PromoteWal` (explicit promote)

`VolumeRequest::PromoteWal` is an explicit "promote the current WAL now and tell me when it's done" request, used by the coordinator before snapshot operations. The flow is identical to the normal promote except the reply is parked in `parked_promote_wal` rather than fire-and-forget. If the WAL is empty, the reply is sent immediately.

### Sequencing: gc_checkpoint through the flusher

`gc_checkpoint` leaves the WAL closed after flushing — the next write lazily opens a fresh WAL via `ensure_wal_open`. Writes never pause; the reply is parked until the GC promote lands on disk.

When `GcCheckpoint` arrives:

1. If `parked_gc` is already `Some`, return an error (concurrent GC checkpoint).
2. Call `Volume::prepare_gc_checkpoint()`:
   a. Mint `u_repack`, `u_sweep`, `u_flush` in order.
   b. If `pending_entries` is empty: delete any lingering WAL file, leave `self.wal = None`, return `GcCheckpointPrep { job: None }`. No fsync — an empty WAL is just a MAGIC header being deleted.
   c. Otherwise: take ownership of the open WAL and `pending_entries`, snapshot CAS tokens, return `GcCheckpointPrep { job: Some(PromoteJob { segment_ulid: u_flush, .. }) }` with `self.wal = None`. The old WAL's `fsync()` runs on the worker as the first step of `execute_promote`, identical to the write-path promote — the parked GC reply won't resolve until the worker finishes, so the coordinator still observes a durable old WAL before acting on `(u_repack, u_sweep)`.
3. If `job` is `Some`: dispatch to flusher, park the reply in `parked_gc`.
4. If `job` is `None`: `publish_snapshot()`, send `Ok((u_repack, u_sweep))` immediately.

On `PromoteComplete` for `u_flush`: the actor applies the promote, publishes the snapshot, takes the parked reply, and sends `Ok((u_repack, u_sweep))`.

The key simplification relative to the original plan: there is **no drain-before-GC gate** and **no deferred-write queue**. Writes that arrive after the checkpoint call `ensure_wal_open`, which mints a fresh ULID from the monotonic mint and opens a new WAL file. Because the mint is strictly monotonic, that ULID is always > `u_flush` > `u_sweep` > `u_repack`, so the ordering invariant holds without pre-reserving a fourth `u_wal`: `(existing_segments) < u_repack < u_sweep < u_flush < (next_write_wal_ulid)`.

This works because the coordinator's post-condition is simply "pending/<u_flush> is on disk and (u_repack, u_sweep) are returned." The parked reply ensures that. Writes that land on the freshly-opened lazy WAL sort above all three GC ULIDs — they are invisible to the GC pass by construction. And an idle volume that sees many consecutive GC ticks with no intervening writes never opens a WAL at all, eliminating per-tick WAL churn.

#### Concurrent GcCheckpoint is an error

If a second `GcCheckpoint` request arrives while one is already parked, the actor replies with an error. The coordinator is expected to serialize its own checkpoints; a concurrent one signals a coordinator bug and should be loud.

## Fresh segment ULID

Minting a fresh ULID for the promoted segment (distinct from the WAL's own filename ULID) is not novel — `gc_checkpoint` already did this with `u_flush`. The offload extended the pattern to the common path.

Two benefits:

1. **No same-ULID path collision.** `find_segment_in_dirs` searches `wal/` then `pending/`, and previously relied on the WAL file being deleted before the pending segment became lookup-reachable by the same ULID. Under the offload the old WAL survives across the rename-before-apply window. Using a fresh segment ULID means stale-snapshot lookups for `old_wal_ulid` only resolve to the WAL file; a cold-cache reader that can't find `wal/<old_wal_ulid>` gets `NotFound` — an error, not silent wrong bytes.
2. **Crash recovery has two distinct ULIDs to reason about** rather than one overloaded one.

### Interaction with the mint and with GC ULID ordering

The mint is a single monotonic counter on `Volume`, advanced by `mint.next()`. All minting happens on the actor thread; the flusher never touches it.

Under the offload, each promote advances the mint by:

| Context | Mint advances |
|---|---|
| Write-path / idle-tick promote | 1 (`segment_ulid`) — plus 1 more when the next write calls `ensure_wal_open` |
| `gc_checkpoint` promote | 3 (`u_repack`, `u_sweep`, `u_flush`) — plus 1 more when the next write calls `ensure_wal_open` |

All minting is single-threaded on the actor. Even with multiple promotes in flight, the segment ULIDs are minted in dispatch order and form a strict sequence. The flusher never mints.

`GcCheckpoint` mints its three ULIDs during `prepare_gc_checkpoint`. Because the mint is monotonic and all minting is single-threaded, the three ULIDs sort above every previously minted segment ULID. Any subsequent `ensure_wal_open` mints a ULID strictly above `u_flush`. The GC ULID-ordering invariant is preserved: `(existing_segments) < u_repack < u_sweep < u_flush < (next_write_wal_ulid)`.

## Recovery semantics

The pre-offload recovery path made two assumptions that the offload breaks:

1. **Exactly one WAL file exists.** The old code replayed only the highest-ULID WAL and silently ignored any earlier ones.
2. **If `pending/<ulid>` and `wal/<ulid>` coexist, they share a ULID.** The dedup-retain loop treated this as "promote rename succeeded, WAL delete interrupted" and deleted the stale WAL.

Under the offload, crash windows can leave the volume with **multiple WAL files** and with **different ULIDs between the old WAL and the in-flight segment**. The recovery path was updated accordingly.

### Recovery rules

1. **Replay all WAL files in ULID order**, not just the latest.
2. **Promote every non-latest WAL to a segment** during recovery, so the volume returns to its single-active-WAL invariant before `Volume::open` completes.
3. **Keep the legacy same-ULID dedup-retain loop** as a degenerate-state cleanup for volumes left over from pre-offload versions of the code. It no longer fires in normal operation but is cheap and handles on-disk state from older releases.

### Recovery sequence (implemented in `Volume::open_impl`)

```text
1. Clean up pending/*.tmp — incomplete promotes from a previous crash.
2. open_read_state rebuilds the ancestor chain + self's pending/ + index/,
   populating lbamap and extent_index with segment-relative offsets.
3. List wal_files in the wal directory, sort ascending by ULID.
4. Apply the legacy dedup-retain loop (same-ULID wal vs pending) — a no-op
   for volumes written by the offload path, handles legacy state.
5. Compute last_segment_ulid from pending/, index/*.idx, bare gc/<id>.
6. Compute mint floor = max(segment_floor, wal_floor). Seed UlidMint.
7. For each wal file except the last (in ULID ascending order):
     - replay_wal_records(wal_path, &mut lbamap, &mut extent_index)
       (reads records, populates both maps with WAL-relative offsets at
       segment_id = this wal's ULID.)
     - If entries is empty, delete the WAL and continue.
     - Snapshot pre_promote_offsets (CAS tokens).
     - Mint a fresh segment_ulid = mint.next().
     - segment::write_and_commit(pending_dir, segment_ulid, &mut entries,
       signer) — writes pending/<segment_ulid>.
     - CAS apply: for each entry, conditionally replace the extent
       index location from (this wal_ulid, wal_offset) to (segment_ulid,
       segment_offset). Same CAS semantics as the online apply phase.
     - Bump last_segment_ulid.
     - Delete wal_path.
8. For the last wal file, recover_wal sets up the active WAL: returns the
   WriteLog handle, ulid, path, and in-memory pending_entries that the
   fresh VolumeActor will own.
9. If there were no wal files, create a fresh one.
```

**Bookkeeping: `last_segment_ulid` must be updated for every recovery-promoted segment.** `Volume::last_segment_ulid` feeds the `snapshot()` path, which uses it as the snapshot marker ULID and requires `snap_ulid >= every DedupRef target segment_id` (the first-snapshot pinning invariant). The recovery loop bumps it for every freshly-minted segment ULID.

### Why this works

Each WAL file is self-contained: its records describe a batch of writes in an internally consistent state. Replaying them in ULID order reproduces the logical write order exactly, and same-LBA conflicts resolve with last-write-wins (higher ULID wins). The conditional replace during recovery-time promote ensures that a hash superseded by a later WAL's entries doesn't get clobbered when an earlier WAL is promoted — identical to the online apply phase.

Orphaned pending segments (the "flusher wrote, actor never applied" case) are absorbed by the replay: the orphan segment's entries land in extent_index during step 2, then get overwritten by the old WAL's entries in step 7 (pointing at the WAL at WAL-relative offsets), then get overwritten again by the recovery-time promote (pointing at a freshly-minted segment). The original orphan is left on disk with no extent index references — wasted space, cleaned up by a later `sweep_pending`. This is the price of not writing a commit marker linking old WAL to in-flight segment; see *Alternatives considered*.

### Crash windows covered

| Crash point | On disk after crash | Recovery action | Data loss? |
|---|---|---|---|
| Prep: before the fresh WAL is opened | `wal/<old>`; no fresh WAL; no segment | Replay old WAL as the active WAL. Identical to the single-WAL path. | No |
| Prep: after fresh WAL opened, before PromoteJob dispatched | `wal/<old>`, `wal/<new>` (empty); no segment | Replay old WAL, promote to fresh segment, delete old WAL. Replay new WAL as the active WAL. | No |
| Flusher: mid-write (`.tmp` exists) | `wal/<old>`, `wal/<new>`, `pending/<seg>.tmp` | Drop `.tmp` (existing cleanup). Treat as the prep-phase-dispatched case. | No |
| Flusher: after rename, before apply | `wal/<old>`, `wal/<new>`, `pending/<seg>` | Segment rebuild picks up `pending/<seg>`. WAL replay overwrites those entries with WAL-relative offsets at `old_wal_ulid`. Recovery-time promote writes a second segment at a fresh ULID and rewrites entries to it. `pending/<seg>` is orphaned; sweep_pending cleans it up later. | No (wasted disk, cleaned up later) |
| Apply: mid extent-index update | Same as above | Same as above — in-memory state is discarded. | No |
| Apply: after update, before WAL delete | Same as above | Same as above. | No |
| Apply: WAL deleted | `wal/<new>`, `pending/<seg>` | Normal recovery. Segment rebuild populates extent index. WAL replay adds only fresh-WAL writes. | No |

Every crash window either loses no data or leaks a duplicate segment that sweep cleans up. No window loses data.

### Alternatives considered

- **Commit marker file linking `old_wal_ulid → segment_ulid`.** A sidecar file (e.g. `pending/<segment_ulid>.from-<old_wal_ulid>`) fsynced before the segment rename would let recovery detect the "flusher succeeded, apply crashed" case without duplicating the segment. **Rejected for the first landing** — the duplicate-segment leak is rare, sweep cleans it up, and adding a new on-disk format shape should not be conflated with the offload change. Can be added later if the leak shows up in benchmarks or production.
- **Rename-based link** (e.g. staged name renamed to final by the apply phase). Same tradeoff as the sidecar marker. No clear win.
- **Recover multiple WALs by in-memory merge without promoting them.** Keeps the old WAL files around after recovery, referenced by extent_index at WAL-relative offsets. Avoids the duplicate-segment issue but leaves the volume in a multi-WAL state that violates the "one active WAL" invariant and complicates the actor's mental model. Rejected.
- **Drain-before-GC and deferred-write queue.** The original plan proposed that `gc_checkpoint` wait for all in-flight promotes to drain, then defer writes until its own promote completed (no active WAL during the GC window). **Rejected during implementation** — the simpler design of leaving the WAL closed post-checkpoint (next write lazily opens a fresh one at `mint.next()`) achieves the same ULID-ordering guarantee without deferring writes or draining the flusher queue. The coordinator's post-condition ("pending/<u_flush> on disk") is satisfied by the parked reply; writes on the freshly-opened lazy WAL sort above all GC ULIDs by construction.

## Apply-phase correctness

The flusher runs concurrently with new writes on the actor. Multiple promotes can be in flight. Nothing conflicts:

| Concern | Analysis |
|---|---|
| Writer calls `extent_index.insert(hash, ..)` for a hash already in a flusher batch | Writer sees the old entry (points at the old WAL) in the extent index, writes a `DedupRef` — no insert. No conflict. The apply phase later updates the entry to point at the new segment; the writer's `DedupRef` in a later WAL resolves through the updated entry. |
| Writer calls `extent_index.insert(hash, ..)` for a hash not in any flusher batch | Points at the current WAL. Never touches old-batch entries. No conflict. |
| GC handoff (`apply_gc_handoffs`) runs during an offload window | GC handoffs run on the actor, serialized with apply phases. A handoff that supersedes an old-batch hash runs before or after the apply phase. The CAS correctly leaves the GC-placed entry alone in either case. |
| Multiple promotes complete out of order | Cannot happen — the flusher is a single thread processing a FIFO channel. Completions arrive in dispatch order. |
| `needs_promote()` fires while promotes are already queued | A new prep + dispatch proceeds normally. Each job operates on a different WAL's entries with a different segment ULID. No shared mutable state between jobs. |

The conditional-replace check is the load-bearing invariant. Its precondition — that `extent_index.lookup(&hash)` returns the exact `(segment_id, body_offset)` captured at prep time — is checked per entry. Cost is one hash-map lookup and two integer compares, negligible.

## Flow control

- Multiple `PromoteJob`s can be queued on the bounded flusher channel (capacity 4).
- The flusher is a single thread — jobs are processed in FIFO order, completions arrive in dispatch order.
- `VolumeRequest::Flush` fsyncs the current WAL and parks the reply on `promote_gen`/`completed_gen` if a promote dispatched before the flush has not yet completed. The actor returns to the select loop immediately — queued writes continue to be processed onto the fresh WAL while the FLUSH caller waits for the worker's old-WAL fsync.
- `VolumeRequest::PromoteWal` dispatches a promote and parks the reply until the segment is on disk. Multiple can be parked simultaneously.
- `GcCheckpoint` opens a fresh WAL immediately (no write pause), dispatches the GC promote, and parks the reply until `u_flush` completes.
- The legacy single-shot `VolumeRequest::Snapshot` has been retired (no live wire sender); the coordinator drives snapshot end-to-end via `PromoteWal` / per-segment `Promote` / `ApplyGcHandoffs` / `SignSnapshotManifest`, all offloaded. Direct-`Volume::snapshot()` survives for in-process tests and never runs on the actor.
- At most one `ParkedGcCheckpoint` at a time (second one errors).

Under sustained write load, the flusher queue depth is bounded by `write_throughput / flusher_throughput`. If writes consistently outpace the flusher, the queue grows and old WAL files accumulate on disk. This is a capacity problem — the correct response is backpressure (future work) or a second flusher thread, not protocol complexity.

## Failure handling

If the flusher reports an error:

- The old WAL is still on disk and still referenced by extent index entries at WAL-relative offsets. Reads against the old hashes continue to work — the WAL file is still reachable at its path.
- The actor performs a fallback `sync_data()` on the old WAL on the error path before bumping `completed_gen`, so any parked NBD `Flush` waiting on this promote's generation still receives a correct durability guarantee — either `Ok` (fallback fsync succeeded; the worker's attempt may or may not have landed, but the actor's did) or `Err` (fallback fsync also failed; caller must retry).
- The pending segment is either absent, a stale `.tmp` (swept on startup), or renamed. A renamed segment at a fresh ULID with no extent index references is a wasted segment that sweep will clean up.
- `pending_entries` for the batch have been moved into the job and are gone from memory.
- Writes continue on the current active WAL.

**Don't retry in-process.** Log loudly, continue taking writes, and rely on restart + recovery to drain the old WAL. The multi-WAL recovery path replays and promotes it. Disk-full and signing errors are rare and typically persistent; retrying in-process adds code paths for negligible benefit.

For the GC checkpoint case, if the flusher channel is closed during dispatch, the parked reply is popped and the error is returned immediately. If the flusher processes the job and returns an error, parked callers waiting on that ULID currently receive no reply — they time out on the coordinator side. This is a known gap; the coordinator retries on its next tick.

## Testing

1. **Unit test: conditional replace (CAS).** Four tests in `extentindex.rs` exercise `replace_if_matches`: exact-match rewrite, superseded-entry skip, offset-mismatch rejection, missing-entry no-op. **Landed.**
2. **Unit test: recovery replay of two WAL files.** `recovery_replays_all_wals_promoting_non_latest` in `volume.rs` constructs a two-WAL state, opens the volume, and asserts the lower WAL is promoted to a fresh segment, the higher WAL is the active WAL, and reads match an oracle. **Landed.**
3. **Proptest: interleaved writes and promotes.** Extending the simulation model with an off-actor promote op that completes asynchronously relative to subsequent writes. **Not yet implemented.**
4. **Proptest: crash between rename and apply.** Fault-inject at the "flusher has renamed `pending/<seg>` but apply has not yet run" point. **Not yet implemented.**
5. **Proptest: interleaved writes and gc_checkpoint.** The existing `SimOp::GcCheckpoint` covers the inline volume path. Coverage of the actor-level async flow (parked reply, concurrent writes on fresh WAL) is **not yet implemented.**
6. **Unit test: concurrent GcCheckpoint rejected.** The rejection logic is at `actor.rs:470`. **No test yet.**
7. **Unit test: PromoteWal reply after flusher completes.** Verifying that a `PromoteWal` request is parked and resolved once the flusher finishes. **No test yet.**
8. **Bench: write tail latency under sustained load.** Compare p99 write latency with and without the offload under a workload that triggers promote every 8 MiB. **Not yet implemented.**
9. **Bench: GC latency impact on writes.** With the coordinator running GC every 60 s, measure per-write p99 during a 10-minute sustained write workload. **Not yet implemented.**
10. **Manual: large mkfs on NBD.** The `mkfs.ext4` path is a natural stress test — the write pattern triggers multiple promotes back-to-back. **Run manually during development; no automated harness.**

## Landing sequence

### Landing 1 — prerequisites, no behavior change (PR #51, merged)

1. **CAS apply for extent-index inserts** in `flush_wal_to_pending_as`. Wired in conditional replace via `replace_if_matches`. Covered by unit tests 1.
2. **Split `segment::promote` into `write_and_commit` + wrapper.** `write_and_commit` writes and commits the segment without deleting the WAL. `promote` wraps it and adds the delete.
3. **Convert `signer` from `Box<dyn SegmentSigner>` to `Arc<dyn SegmentSigner>`** on `Volume` so it can be shared with the flusher thread.

### Landing 2 — recovery replay + fresh ULID (PR #55, merged)

4. **Replay all WAL files in `Volume::open_impl`**, promoting non-latest WALs to fresh segments. Keeps the legacy same-ULID dedup-retain loop. Covered by unit test 2.
5. **Switch the in-process `flush_wal_to_pending` to mint a fresh segment ULID.** Still runs on the actor, still synchronous. Validates the fresh-ULID pattern under the existing concurrency model.

### Landing 3 — the offload itself (PRs #56 + #57, merged)

6. **Introduce the flusher thread** and wire `PromoteJob` / `PromoteResult` plumbing (including the shutdown path). Bounded channels, `promotes_in_flight` counter, `ParkedPromoteWal` / `ParkedGcCheckpoint` structures.
7. **Route write-path and idle-tick promotes through the flusher.** Split `VolumeRequest::Flush` into WAL fsync only. Added `VolumeRequest::PromoteWal` for explicit promote-and-wait.
8. **Route `gc_checkpoint` through the flusher** with parked-reply dispatch, the concurrent-GC-error check, and the simplified no-drain/no-defer design.

### Post-landing fix (308f778)

**Replay WAL into coordinator LBA map** to prevent stale-liveness loop. The coordinator's GC liveness check was not accounting for WAL entries that had been replayed but not yet promoted, leading to a cycle where GC thought segments were still live.

## Resolved questions

- **Mint state across the recovery-time promote.** Verified safe. The mint has no invariant beyond monotonicity and no code reads `mint.last`. The related concern — `last_segment_ulid` — is called out in *Recovery semantics*: the recovery loop bumps it for every freshly-minted segment ULID so that later `snapshot()` calls preserve the first-snapshot pinning invariant.
- **`gc_checkpoint` return latency / coordinator assumption.** Verified safe. The coordinator awaits `gc_checkpoint` as opaque async IPC. Its only post-conditions are (1) `pending/<u_flush>` exists on disk, (2) the fresh WAL is open, (3) `(u_repack, u_sweep)` are returned. The offloaded path preserves all three.
- **Drain-before-GC complexity.** The original plan required draining in-flight promotes and deferring writes during `gc_checkpoint`. During implementation this was simplified away: opening the fresh WAL at `u_wal` immediately achieves the same ULID-ordering guarantee without any write pause. The deferred-write queue was never needed.
- **WAL delete ordering relative to publish.** The current implementation deletes the old WAL inside `apply_promote` (before `publish_snapshot`), not after. This is safe because the WAL delete happens after the extent index CAS is complete: at that point, no extent index entry references the old WAL. A reader that loaded the pre-apply snapshot still has entries pointing at `old_wal_ulid`, but these resolve to the WAL file which is still open (Linux keeps the inode alive via cached fds) or produce `NotFound` (cold-cache edge case, error not corruption). Post-apply readers see segment-relative offsets at the fresh ULID.

## Open questions

- **Metrics.** `flusher_queue_depth`, `promote_duration`, `parked_gc_checkpoint_duration`. All are directly useful for operators and fall out of the implementation naturally.
- **Backpressure.** Under sustained writes that outpace the flusher, the queue grows and old WAL files accumulate. The bounded channel (capacity 4) provides implicit backpressure — `send()` blocks when full. Future work: explicit backpressure signal to the write path if this proves insufficient.
- **Flusher error handling for parked callers.** If the flusher returns an error for a promote, the corresponding `ParkedGcCheckpoint` or `ParkedPromoteWal` reply is never resolved. The caller times out. A future improvement could pop the parked reply and forward the error.
