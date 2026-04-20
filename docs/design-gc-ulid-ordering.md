# Design: GC ULID ordering and the single-mint invariant

Status: **resolved** (extent index guard + per-entry tracking committed;
gc_checkpoint closes rebuild-time ordering gap; removal-only handoffs handle
LBA-dead extent cleanup; actor GcCheckpoint handler fixed to use volume mint)

Date: 2026-03-30

> **Note (April 2026):** the historical handoff filename references in
> this document — `.pending`, `.applied`, `.done`, removal-only handoffs
> as plaintext manifest lines — are out of date. The self-describing GC
> handoff protocol (see `docs/design-gc-self-describing-handoff.md`)
> replaces the manifest sidecar entirely; the on-disk lifecycle is now
> `gc/<ulid>.staged` → bare `gc/<ulid>` → deleted, with the consumed
> input ULID list carried in the segment header. The ULID ordering
> invariants this document records are unchanged: GC outputs still get
> `max(inputs).increment()` and `gc_checkpoint` still pre-mints
> `(u_repack, u_sweep, u_flush)` in one shot for crash-recovery
> correctness. The post-checkpoint WAL is opened lazily on the next
> write rather than pre-minted as a fourth `u_wal`, because
> `mint.next()` is strictly monotonic — any future mint is already
> above `u_flush`, so no reservation is needed.

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
`cache/`, gets picked up by the next GC, and `increment()` deterministically
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

**Status:** committed (07828b2).

### 2. ULID source unification (two-source ordering race)

The coordinator's `compaction_ulid(max_input)` computes ULIDs independently
from the volume's mint.  When ULIDs collide in the same millisecond, the sort
order is determined by random bits — a coin flip.  After a crash, the LBA map
is rebuilt from segment files in ULID order.  If the GC output sorts after a
newer segment, its stale LBA entries shadow the correct ones.

**Attempted fix: single-mint.**  The coordinator requests GC output ULIDs from
the volume's mint via `VolumeRequest::MintUlid`.  This ensures all ULIDs in a
fork come from a single monotonic source.  `compaction_ulid()` is removed.

**Why it was reverted:** the single-mint approach fundamentally breaks rebuild
ordering.  The volume's mint tracks the highest ULID issued.  When a WAL
segment is opened (step N), it gets ULID W from the mint.  When the coordinator
later calls `mint_ulid()` (step N+K), it gets ULID M > W.  The GC output is
named M.  If the WAL is flushed to pending/ and then a crash occurs, rebuild
processes segments in ULID order: W first, then M.  M's stale entries overwrite
W's correct entries — the exact bug we're trying to fix.

The `increment()` approach avoids this because `max(old inputs).increment()`
produces a ULID from the old inputs' timestamp, which is far below any
concurrent WAL ULID.  The GC output sorts before the WAL segment, so the WAL's
entries win on rebuild.

**Fix: `gc_checkpoint` (option A — WAL flush before mint).**  Before running a
GC pass the coordinator calls `gc_checkpoint` on the volume via the actor
channel.  The handler flushes the in-flight WAL to `pending/` and then mints
a fresh ULID from the volume's own generator.  The coordinator uses this ULID
as the output segment name.

Because the WAL is flushed *before* minting, no in-flight WAL segment can have
a ULID below the returned value — the volume's mint advances past the minted
ULID, so all subsequent WAL segments sort above the GC output.  The GC output
therefore always sorts below any concurrent or future write, and the WAL's
entries win on rebuild.  The two-source ordering race is closed.

**Status:** committed (b3f9244 + 23fd569).

### 3. `.pending` file per-entry old_ulid (test helper bug)

The test helper `simulate_coord_gc_local` used `max_input` as the `old_ulid`
for ALL entries in the `.pending` file.  But entries come from two different
input segments.  The correct `old_ulid` is the specific source segment each
entry came from — required for the extent index guard (bug 1) to work.

**Fix:** track per-entry source segment ULID in the test helper.

**Status:** committed (07828b2).

### 4. Actor GcCheckpoint handler used system clock instead of volume mint

The actor's `GcCheckpoint` message handler called `volume.gc_checkpoint()` to
flush the WAL (correct), but then **discarded the returned ULID** and generated
u1 and u2 via `ulid::Ulid::new()` — the system clock, independent of the
volume's monotonic mint:

```rust
// Broken code (before fix):
let result = self.volume.gc_checkpoint();  // flushes WAL, returns ULID G
let pair = result.map(|_| {               // G is silently discarded
    let u1 = ulid::Ulid::new().to_string(); // system clock — unrelated to mint
    std::thread::sleep(Duration::from_millis(2));
    let u2 = ulid::Ulid::new().to_string(); // system clock — unrelated to mint
    (u1, u2)
});
```

The volume mint after the flush is at G.  The next WAL segment uses
`self.mint.next()` = G+1.  But u1 and u2 are from the system clock and can
land above G+1 (they almost always do — they are generated *after* G+1 in
wall-clock time).  On rebuild the GC compact (named u2) sorts after the WAL
segment (G+1) and wins, serving stale data.

This is what caused the observed failure: after `volume evict` + coordinator
restart, LBA 0 mapped to the GC compact's stale zeros rather than the correct
superblock written by the subsequent WAL flush.

**Fix:** `volume.gc_checkpoint()` now returns `(String, String)` — flush WAL
once, then two successive `self.mint.next()` calls (2ms apart for distinct
timestamps).  The actor passes the pair through unchanged.  The mint advances
past both ULIDs, guaranteeing all subsequent WAL segments sort above both GC
outputs.

**Status:** committed (a9d0488).

## Additional findings

### Atomic `.pending` write

The `.pending` file was written with `fs::write()` (non-atomic).  A crash
mid-write leaves a partial file.  `apply_done_handoffs` parses this to find
old segment ULIDs — a partial file means some old ULIDs are missed, leaking
S3 objects.

**Fix:** write via tmp + rename.

**Status:** committed (7e8eeec).

### NotFound tolerance in rebuild

`lbamap::rebuild` and `extentindex::rebuild` fail hard if a segment file
disappears between path collection and the read (e.g. coordinator GC deleting
a segment file).  Should skip with a warning — the new compacted segment
(higher ULID) provides the correct entries.  Same fix needed in
`Volume::repack()`.

**Fix:** match on `ErrorKind::NotFound` and continue with a `warn!()`.

**Status:** committed (7e8eeec).

### All-dead segment case

When all extents in GC candidates are truly extent-dead (no extent index entry
references them), the coordinator deletes S3 objects and local files directly —
no handoff is needed.

However, extents that are *extent-live but LBA-dead* (the extent index still
references them, but the LBA has since been overwritten with different data)
require a removal-only handoff: a `.pending` file containing only 2-field lines
(`<hash> <old_ulid>`).  Without this, deleting the source segments leaves
dangling extent index references that cause "segment not found" errors on reads
(particularly via dedup-ref entries that resolve through the stale entry).

**Fix:** direct delete in the truly-all-dead branch; removal-only handoff when
extent-live/LBA-dead entries exist.

**Status:** initial direct-delete committed (7e8eeec); removal-only handoff
committed (2d0a6ce).

### `.applied` -> `.done` cleanup

The coordinator polls for `.applied` files at the start of each GC tick,
deletes old S3 objects and local segment files, then renames to `.done`.
S3 404 on delete is treated as success (idempotent across coordinator crashes).

**Fix:** `apply_done_handoffs()` in coordinator `gc.rs`.

**Status:** committed (7e8eeec) with 8 unit tests.

## Implementation status

| Fix | Status | Commit |
|-----|--------|--------|
| Extent index guard (bug 1) | Done | 07828b2 |
| Per-entry old_ulid (bug 3) | Done | 07828b2 |
| NotFound tolerance | Done | 7e8eeec |
| Atomic `.pending` write | Done | 7e8eeec |
| All-dead direct cleanup | Done | 7e8eeec |
| `.applied` -> `.done` | Done | 7e8eeec |
| gc_checkpoint / rebuild-time ordering (bug 2) | Done | b3f9244 + 23fd569 |
| LBA-level liveness filtering | Done | b3f9244 + 23fd569 |
| Removal-only handoffs (LBA-dead cleanup) | Done | 2d0a6ce |
| Dedup-ref liveness in all compaction paths | Done | d71b084 |
| Actor GcCheckpoint used system clock not volume mint (bug 4) | Done | a9d0488 |
