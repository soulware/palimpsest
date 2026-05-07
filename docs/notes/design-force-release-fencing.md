# Force-release fencing

**Status:** Proposed (2026-04-29). Companion to
[`design-portable-live-volume.md`](design-portable-live-volume.md) §
*`volume release --force`*. The recovery side of `--force` is already
specified there; this doc covers the **previous owner** side: what
must happen on coordinator A's host when A is alive and a peer has
force-released A's volume out from under it.

## Problem

`volume release --force` exists for the case "the previous owner is
unreachable and not coming back." Coordinator B unconditionally
rewrites `names/<name>` to `Released` after synthesising a handoff
snapshot from segments observable in S3. A new claimant C forks
from that synthesised snapshot.

The verb's safety contract — "the dead owner's writes that never
reached S3 are lost; nothing else" — assumes A is in fact dead. If A
is **alive** when `--force` runs (a partition that resolves, or a
mistakenly-issued `--force`), nothing in today's code stops A from
continuing to mutate `by_id/<V1>/...` in S3. The dangerous mutations
are not new writes (those become orphans, harmless), but **A's
reaper deleting segments named in B's synthesised manifest**. Once a
named segment is gone from S3, C's reads return 404.

## What "one owner" rests on, without `--force`

Today the "one mutating coordinator per S3 prefix" property is not
enforced by `names/<name>` checks on A's data path. It holds **by
construction**:

1. **Directory locality.** `<data_dir>/by_id/<V1>/` exists on
   exactly one host — the one that created or claimed it. Claims
   from other coordinators mint a fresh `vol_ulid`, so V1 is only
   mutated by A.
2. **Local volume halt.** `volume release` requires
   `volume.stopped` locally before the bucket flip. No new WAL, no
   new pending segments after release.
3. **Local snapshot floor.** A's GC respects
   `latest_snapshot(fork_dir)` as a floor: segments with ULID below
   it are not touched. The handoff snapshot in normal release is
   exactly A's most recent local snapshot, so segments named in the
   handoff are always at-or-below A's floor and pinned implicitly.
4. **Disjoint write prefixes.** Claimants always mint a fresh
   `vol_ulid`; their writes go to V2's prefix, never V1's.

`--force` violates 2 and 3 when A is alive: A's volume is still
running, and B's synthesised snapshot exists in S3 only — A's local
floor is unaware of it.

## The invariant `--force` must preserve

B's synthesised handoff manifest is a list of specific segment
ULIDs. C's read path resolves those ULIDs by GET against
`by_id/<V1>/segments/<ulid>`. **Every ULID named in the manifest
must remain in S3 for as long as any descendant of V1's
synthesised snap is alive.**

Set membership at the manifest level: whether a later GC output
preserves the bytes is irrelevant, because C resolves by ULID, not
by content. A doesn't know which ULIDs B chose, so A must stop
reaping anything in V1's prefix once dispossessed.

## The mechanism

The existing snapshot-floor rule already says exactly what we
want: **a snapshot at ULID S means GC does not operate on segments
with ULID < S.** B's synthesised snapshot has a freshly-minted ULID
greater than every segment it names, so making A's local floor
match B's snapshot pins B's entire set automatically.

Three pieces:

1. **A polls `names/<name>` for each owned volume per background
   tick.** On observing `coordinator_id != self` (or
   `state ∉ {Live, Stopped}`), A pulls the bucket's
   `handoff_snapshot` for V1 into A's local `<fork_dir>/snapshots/`.
   `latest_snapshot()` now returns S'; A's floor advances.
2. **GC respects the floor (existing).** With S' at A's local
   floor, every segment B named is below the floor, hence never
   selected for compaction. A's GC continues to operate freely
   *above* the floor — those segments are A's post-displacement
   orphans, never referenced by C's manifest, harmless to compact.
3. **Reaper respects the floor (new).** Before each
   `store.delete(target)`, the reaper reads `latest_snapshot()` and
   refuses to delete any segment with ULID below the floor. One
   local fs read per delete; cheap.

That's the whole mechanism. No `volume.retired` marker, no
"handoff-vs-user-snapshot" distinction, no separate fencing flag,
no per-op `names/<name>` reads.

## Why this works

- **A's pre-existing retention markers self-disarm.** Markers under
  `by_id/<V1>/retention/` reference inputs from past GC compactions.
  Once A pulls S' and the local floor advances, those inputs are
  below the floor. The reaper's per-op floor check refuses to
  process them. The markers remain in S3 until separately cleaned
  up (orthogonal hygiene).
- **A's post-displacement GC writes are scoped to orphans.** GC
  operates above the floor. Anything A compacts above S' is by
  definition not in B's manifest. Compacting orphans is harmless to
  C; the resulting retention markers reference orphan inputs and
  fire harmlessly when their deadlines elapse.
- **The artefact is its own persistence.** If A crashes between
  pulling S' and re-running, the snapshot is still on disk. On
  restart, A's first GC/reaper invocation observes the new floor.
  No separate fence flag to write or recover.

## Failure-mode walkthroughs

**A is partitioned from S3, B runs `--force`, A reconnects later.**

During partition: A's drain fails, segments accumulate locally. A's
GC may compute plans but cannot publish retention markers. A's
reaper cannot list S3. Zero mutations to V1's prefix.

A reconnects: A's first tick polls `names/<V1>` and observes the
flip. A pulls S' to local `snapshots/`. Floor advances to S'. A's
GC and reaper, on the same tick, see the new floor and treat
everything in B's pinned set as untouchable. No deletes are issued.

**A is healthy and `--force` is mistakenly issued.**

A's per-tick poll catches the flip on its next tick (≤ tick
cadence, default ≤ 10s for GC). The reaper's per-op floor check
absorbs anything mid-tick: any in-flight DELETE for an
above-old-floor input is now below the new floor → refused.

**Operator-initiated graceful retire (no claimant in mind yet).**

A snapshots its current state (the "now" case), halts the volume,
flips the name to Released. The fresh snapshot is the floor; GC and
reaper continue to operate freely above it but never below. Any
later claimant claims the released name and forks from this
snapshot. Symmetric with the `--force` recovery flow.

## What's not addressed by this mechanism

Three concerns remain orthogonal:

- **Tightening `--force`'s precondition.** Refusing `--force` when
  A's heartbeat is recent reduces the rate of stray triggers but is
  independent of the fence. Belongs in
  [`design-portable-live-volume.md`](design-portable-live-volume.md).
- **Cleanup of orphan state under V1.** Retention markers that
  self-disarm, segments A wrote post-displacement, and the synthesised
  snapshot manifest itself all accumulate under V1's prefix until a
  "retire-`vol_ulid`" path runs once no living fork pins V1's snap.
  Out of scope for this doc.
- **Surfacing dispossession to A's app.** Today A's volume IPC
  accepts writes regardless of name-record state. Plumbing the
  poll's result through to the volume process would convert silent
  post-`--force` writes into immediate errors. UX improvement, not
  a correctness change.

## Open questions

- **Poll cadence.** Default GC tick is 10s; the fence latency
  matches that. Reaper cadence is `retention_window/10` (default
  1 minute). Should both share a name-record poll, or do their own?
- **Sign `names/<name>` records.** Defence in depth against
  non-coordinator bucket writers. Not load-bearing for the fence.
- **Per-tick vs per-op poll.** Per-tick is sufficient given per-op
  floor check in the reaper. Per-op `names/<name>` polling adds
  cost without changing safety.
