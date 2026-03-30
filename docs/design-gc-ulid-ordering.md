# Design: GC ULID ordering and the single-mint invariant

Status: **open** (stashed WIP in git stash, needs design before implementation)

Date: 2026-03-30

---

## Context

While implementing `.applied` -> `.done` cleanup for the GC handoff protocol,
proptest found a ULID ordering bug that was previously masked by timing.

## The invariant

**Every segment ULID in a fork must come from a single monotonic source.**
Without this, ULID ordering — which is the foundation of crash-recovery
correctness — can be violated.

Currently the volume and coordinator generate ULIDs independently:
- Volume: `UlidMint` seeded from `max(existing ULIDs)` on open
- Coordinator GC: `max(inputs).increment()`

These can collide when two ULIDs share the same millisecond timestamp, because
the 80-bit random portion determines sort order — a coin flip.

## How proptest found it

In production, GC inputs are segments that went through the drain pipeline —
their ULIDs are seconds to minutes old.  `max(inputs).increment()` produces a
ULID from that old timestamp, which is far below the current WAL ULID.  The
invariant holds by time alone.

Proptest collapses time to zero.  Operations that take minutes in production
happen within the same millisecond.  `DrainLocal` -> `CoordGcLocal` back-to-back
means the GC inputs have ULIDs from the same millisecond as the current WAL.
`max(inputs).increment()` lands at or above the WAL ULID roughly 50% of the
time (depending on random bits).

After multiple GC rounds, the effect accumulates: each GC output goes into
`segments/`, gets picked up by the next GC, and `increment()` deterministically
marches forward through the ULID space while the volume's mint draws random
positions.

## Three bugs found

### 1. Extent index guard (TOCTOU between GC liveness check and handoff application)

`apply_gc_handoffs()` unconditionally overwrites extent index entries from the
GC output segment's index.  If a newer write has landed between the
coordinator's liveness check and the volume applying the handoff, the GC
output's stale entry overwrites the correct one.

**Fix:** parse the `.pending` file's `old_ulid` per entry, and only update the
extent index if `extent_index[hash].segment_id == old_ulid`.  Uses
`blake3::Hash::from_hex()` (available in the `blake3` crate, no new deps).

**Status:** implemented in stash, tested, correct.

### 2. ULID source unification (two-source ordering race)

The coordinator's `compaction_ulid(max_input)` computes ULIDs independently
from the volume's mint.  When ULIDs collide in the same millisecond, the sort
order is determined by random bits — a coin flip.  After a crash, the LBA map
is rebuilt from segment files in ULID order.  If the GC output sorts after a
newer segment, its stale LBA entries shadow the correct ones.

**Fix:** the coordinator must request GC output ULIDs from the volume's mint
via `VolumeRequest::MintUlid`.  This ensures all ULIDs in a fork come from a
single monotonic source.  `compaction_ulid()` is removed.

**Status:** partially implemented in stash.  Design issues remain:
- `mint_ulid()` must not be called speculatively — only after confirming GC
  will proceed (candidates exist).  Otherwise wasted ULIDs advance the mint
  unnecessarily.
- The coordinator needs a `mint_ulid` callback plumbed through `gc_fork` and
  `compact_segments`.  For production, this will eventually be an IPC call to
  the volume process.  For now, the daemon can use `Ulid::new()` as a
  placeholder (safe in production where GC inputs are old).
- The test helper `simulate_coord_gc_local` needs the same parameter.

### 3. `.pending` file per-entry old_ulid (test helper bug)

The test helper `simulate_coord_gc_local` used `max_input` as the `old_ulid`
for ALL entries in the `.pending` file.  But entries come from two different
input segments.  The correct `old_ulid` is the specific source segment each
entry came from — required for the extent index guard (bug 1) to work.

**Fix:** track per-entry source segment ULID in the test helper.

**Status:** implemented in stash.

## Additional findings

### Atomic `.pending` write

The `.pending` file was written with `fs::write()` (non-atomic).  A crash
mid-write leaves a partial file.  `apply_done_handoffs` parses this to find
old segment ULIDs — a partial file means some old ULIDs are missed, leaking
S3 objects.

**Fix:** write via tmp + rename.

**Status:** implemented in stash.

### NotFound tolerance in rebuild

`lbamap::rebuild` and `extentindex::rebuild` fail hard if a segment file
disappears between path collection and the read (e.g. coordinator GC deleting
a segment file).  Should skip with a warning — the new compacted segment
(higher ULID) provides the correct entries.  Same fix needed in
`Volume::compact()`.

**Fix:** match on `ErrorKind::NotFound` and continue with a `warn!()`.

**Status:** implemented in stash.

### All-dead segment case

When all extents in GC candidates are dead, no handoff is needed (no extent
index entries reference the segments).  The coordinator should delete S3
objects and local files directly, skipping the handoff protocol.

**Fix:** direct delete in the all-dead branch of `compact_segments`.

**Status:** implemented in stash.

### `.applied` -> `.done` cleanup

The coordinator polls for `.applied` files at the start of each GC tick,
deletes old S3 objects and local segment files, then renames to `.done`.
S3 404 on delete is treated as success (idempotent across coordinator crashes).

**Fix:** `apply_done_handoffs()` in coordinator `gc.rs`.

**Status:** implemented in stash with unit tests.

## Proposed implementation plan

1. Extract the non-controversial fixes from the stash:
   - NotFound tolerance in rebuild (lbamap, extentindex, volume::compact)
   - Atomic `.pending` write
   - All-dead segment direct cleanup
   - `.applied` -> `.done` cleanup with unit tests

2. Design and implement the single-mint fix:
   - Add `Volume::mint_ulid()` and `VolumeRequest::MintUlid`
   - Add `mint_ulid` callback to coordinator's `gc_fork` / `compact_segments`
   - Call `mint_ulid` only after confirming candidates exist (not speculatively)
   - Remove `compaction_ulid()`
   - Update test helpers

3. Add the extent index guard:
   - Parse `.pending` for per-entry `old_ulid`
   - Guard on `extent_index[hash].segment_id == old_ulid`
   - Fix test helper to write per-entry source segment ULIDs

4. Update docs and tests

## Stash reference

```
git stash list   # find "WIP: apply_done_handoffs + ULID ordering fixes"
git stash show   # see the diff
```
