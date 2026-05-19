# Segment tail-delta manifest

**Status:** Proposed.

P3 (folding in P4) of [`list-elimination-plan.md`](list-elimination-plan.md):
remove the remaining per-volume `segments/` and `retention/` prefix
LISTs from the coordinator runtime. This document specifies the
replacement and the invariant that makes it correct.

## The set we need, and the set we already have

The runtime never needs "every object under `by_id/<vol>/segments/`".
It needs **the live segment set of a `vol_ulid`** — and most of that
set is already enumerated, signed, and reachable without a LIST:

- The signed `SnapshotManifest`
  (`by_id/<vol>/snapshots/<date>/<snap>.manifest`) carries
  `segment_ulids: Vec<Ulid>` — the *full*, sorted, Ed25519-signed set
  of segments live as of that snapshot. It is a full manifest, not a
  delta over the prior snapshot.
- After P2, the latest manifest is reachable by a single GET through
  the `by_id/<vol>/snapshots/LATEST` pointer — no LIST.

The code is already split along this line. `prefetch_indexes` (the
warm-start claim path) *"anchors on a signed manifest and never lists
segments/ or retention/"*. The LIST path, `pull_indexes_via_list`,
exists for exactly one reason, stated in its own doc comment:
*"segments past the latest snapshot"* — the WAL-drain tail, plus GC
outputs and reaper deletions in that same post-snapshot window. The
three runtime LIST sites in scope (`prefetch.rs:442`, `fork.rs:670`,
`recovery.rs:165`) and the two retention sites (`prefetch.rs:643`,
`reaper.rs:80`) all derive only this post-snapshot remainder.

So the maintained object is not a parallel full segment index that
duplicates the manifest. It is a small **per-vol delta over the latest
signed manifest**, covering only the post-snapshot window. The
manifest stays the authoritative full set; the delta carries what has
happened since it was sealed.

## Shape: a second manifest, not a log

The post-snapshot set is the *high-cardinality per-write set* — a
write-heavy volume drains thousands of segments between snapshots. That
rules out an event-log shape. The event log's windowed-`HEAD` +
`prev`-chain works only because a name sees *dozens* of events over its
life; a fixed-N window over thousands of segments misses almost
everything and the `prev`-walk fallback is thousands of sequential
GETs — worse than the LIST it replaces.

The right primitive is the one the system already proves works for
"enumerate the whole segment set in one object, no LIST": **the
snapshot manifest**. The tail is simply a *second* manifest for the
not-yet-sealed delta — a single object, whole-object overwrite, **one
GET reads the entire tail at any cardinality**:

```
by_id/<vol_ulid>/tail        (single fixed key, like snapshots/LATEST)
```

`coord-data`, same axis as the data it indexes. Body: the current
delta — the set of `Added` segment ULIDs, the set of `Superseded`
edges (each with the wall-clock instant it was recorded), and the set
of `Tombstoned` ULIDs. Sorted; no signature (see *Derived state*).

**Single writer, no lock, no CAS.** The per-volume tick loop
(`tasks.rs`) already runs interval-gated sub-steps — drain every tick,
GC when `gc_interval` has elapsed. P3 **folds the reaper in as a third
gated step** (reap when `reaper_interval` has elapsed) and deletes the
separate coordinator-wide reaper task. The tick loop is then the
*sole* writer of the tail for its volume, and drain → GC → reap run
**sequentially within one iteration**. So a single whole-object PUT of
the tail at the end of each iteration — after that iteration's segment
PUTs/DELETEs — suffices. "Single-writer-per-vol-epoch" stops being a
careful argument and becomes a structural fact (matching P2's
`LATEST`, also a single-writer per-vol overwrite). Folding reap in
only changes its cadence from a private timer to a tick gate;
retention deletes *after* a multi-minute window, so tick-granularity
lateness is immaterial — the same trade GC's gate already makes.

**Write cost is per-tick, not per-segment.** The drain uploads *all*
of `pending/` in one tick (`upload.rs`), so a tick that drains
thousands of segments still produces exactly **one** tail overwrite.
Cumulative write amplification is O(ticks between snapshots), not
O(segments) — a handful of low-MB PUTs, far cheaper than thousands of
per-record objects *or* a chain walk.

**Why a separate object, not the segment headers.** An enumeration
object must outlive what it enumerates, so it cannot *be* the
segments: the reaper deletes them. `Tombstoned`/`Superseded` are facts
*about* absent objects with nowhere to live inside them. And the
ordering/aggregation point is the single-writer tick loop — the same
place that already sequences this volume's `index/` — not a per-object
field that concurrent writers could never agree on. Unlike the event
log the tail keeps no history: it is *only* the delta over the current
manifest anchor (the authority is the manifest, not accumulated
records), so it is reset wholesale at each seal, not compacted.

Retention/supersession is **folded into this same object** (P3+P4
collapsed, per the plan's "may collapse into one"): a snapshot
manifest already reflects all GC that completed before it, so — like
segments — supersession only matters in the post-snapshot window. One
object, one truncation event, no separate `retention/` prefix.

## Body format

Plaintext, line-oriented, sectioned by entry kind. Direct precedent:
`SnapshotManifest` (`signing.rs`, `segments:` header + one ULID per
line, sorted lex) and the retention marker (`retention.rs`, *"one
input segment ULID per line, plain text"*). Matches both, matches the
"on-disk layout inspectable with standard tools" principle, and at
thousands of entries is still tens of KB.

```
anchor: <snap_ulid|nil>
added:
  <ulid>
  ...
superseded:
  <input-ulid> <output-ulid> <since-rfc3339>
  ...
tombstoned:
  <ulid>
  ...
```

- Crockford-Base32 ULIDs throughout; parsed through `Ulid::from_string`
  (parse-don't-validate).
- **Sorted lex within each section** — chronological for ULIDs;
  matches the manifest convention; gives the rebuild invariant a
  deterministic canonical form.
- Empty sections present-but-empty regardless of state (canonical form).
- `anchor:` names the manifest this tail is a delta of (`nil` on a
  fresh volume). Self-describing for operators and lets the proptest
  assert anchor equality; not load-bearing for correctness — the
  manifest set is the arbiter regardless.
- `since` is RFC3339, matching the manifest's `recovered_at`. Replaces
  the role the retention marker's filename ULID plays today.
- **No `sig:`** — derived, unsigned state (see *Derived state*).
- **No hard cap.** Truncation at seal bounds the body; a cap would
  contradict the "thousands is fine" premise.

Parser/writer follows the `retention.rs` pattern — a dedicated module
exposing `render` / `parse` with explicit error types.

## Entry kinds

The tail body carries three entry kinds:

- **`Added{seg_ulid}`** — a segment was uploaded (drain) or produced
  (GC output) and is durable in S3.
- **`Superseded{input_ulid, output_ulid, since}`** — GC published
  `output` and `input` is now dead; `since` is the wall-clock instant
  the supersession was recorded. The supersession edge is **already
  self-describing from `output`'s signed header**: a GC output carries
  an authenticated `inputs` table (the consumed ULIDs, covered by the
  segment signature — `segment.rs`). So the entry is redundant **only
  as edge-authority** — a lost/corrupt entry re-derives from
  `output`'s signed header, the real authority. It is **not** redundant
  otherwise: it is the **LIST-free discovery index** (recovering all
  edges from inputs tables alone means fetching every GC output
  header) and the **retention-timing carrier** — the reaper deletes
  `input` once `since + retention < now`. `since` must be carried
  explicitly because the GC *output* ULID is history-derived
  (`max(inputs).increment()`, `design-gc-ulid-ordering.md`), not
  wall-clock. This is why `retention/` folds *into* the tail rather
  than being dropped in favour of the inputs tables.
- **`Tombstoned{seg_ulid}`** — the reaper deleted `seg_ulid` from S3.

## Derived state

The tail is **derived, unsigned state**. It is not an authenticity
root: every segment carries its own Ed25519 signature, verified at
fetch time on both the manifest and the LIST paths today. A forged or
corrupt tail can only point at a segment that then fails its own
signature check, or 404s — and readers already tolerate a 404 on
segment fetch (`list_supersessions` explicitly does). The tail is an
availability/enumeration hint over a signed substrate, not a trusted
set; its correctness floor is the rebuild (below), not a signature.

## Read path

A reader resolves the live set with two GETs and one more:

```
anchor   = LATEST -> manifest          (one GET each, P2)
tail     = GET by_id/<vol>/tail        (one GET, whole object)
live     = manifest.segment_ulids
         ∪ { e.seg_ulid   : Added e }
         − { e.input_ulid : Superseded e }
         − { e.seg_ulid   : Tombstoned e }
```

The **manifest's `segment_ulids` is authoritative for the
snapshot/tail boundary**: the tail is a delta over *that* set.
`Superseded` is applied over the manifest set too, not just over
`Added` — a pre-snapshot input that GC superseded *after* the snapshot
is in `manifest.segment_ulids` but must still be skipped, and its
`Superseded` entry lives in the tail.

### The tail is cross-coordinator only

The tail, like the manifest, exists for *other* readers — never the
writer. A coordinator wrote every segment it drained, so its local
`index/` is the authoritative complete set; the normal
`snapshot_take_new` seal just enumerates local `index/` (LIST-free,
not a tail consumer). **A host never reads the tail for a volume it
owns in the current epoch.** The read happens only for
segments/supersessions a *different or prior* coordinator produced
that this host lacks locally:

- **Force-release recovery** (`lifecycle.rs:301` →
  `recovery::list_and_verify_segments`) — recovering a dead *other*
  coordinator's volume; no local `index/` for it at all.
- **`force_snapshot_now` on a readonly source** (`fork.rs:367` →
  `force_snapshot_now_op`) — the source's local `index/` was hydrated
  manifest-only by `prefetch_indexes`; its owner has the tail, this
  host does not. The pinned-snapshot fork branch beside it never reads
  the tail.
- **Post-handoff reap** — a new owner's tick-loop reap step must
  reclaim inputs a *prior* owner superseded. Same-epoch the reap step
  needs no tail read (it just wrote the tail itself this iteration);
  only across a handoff is the prior owner's tail consulted.

Consequently the steady-state hot path (`prefetch_indexes` for
claim/start, pinned-snapshot fork, ancestor reads) touches neither
tail nor S3 LIST, and the host that pays the tail *write* never pays a
*read*.

## Writers and crash ordering

The per-volume tick loop is the single writer. The correctness rule is
unconditional: **all S3 segment object operations happen first, then
the single tail overwrite, then the iteration reports success.**

- **Drain (`upload.rs`)** — PUT every `segments/<seg>` for the tick,
  then overwrite the tail to include them. Crash before the tail PUT:
  segments exist with no tail entry — simply not consumed (a
  reclaimable space leak, never a correctness loss; an un-indexed
  segment cannot corrupt a read).
- **GC** — PUT `segments/<output>`, then the same tick's tail PUT adds
  `Added{output}` and `Superseded{input, output, since}`. Output
  segment is durable before any reader can see the `Superseded` that
  depends on it.
- **Reap** — DELETE the superseded input objects, then the tail PUT
  drops them / records `Tombstoned`. Crash after DELETE, before the
  tail PUT: object gone but tail still lists it — the reader gets a
  404 it already tolerates, never a skipped-live segment.

The forbidden direction — the tail asserting a segment is live with no
object behind it on a path that matters — cannot arise: objects are
always durable before the tail naming them, and the only
object-without-tail and tail-without-object windows are both
404-tolerant or benign-leak by construction. Because one iteration's
drain/GC/reap are sequential and collapse into one tail PUT, there is
no intra-host ordering between writers to reason about.

## Truncation

Truncation is trivial because the writer is also the sealer. When the
tick loop seals a new snapshot it writes the manifest, bumps `LATEST`
(P2), and — in the same sequential iteration — **overwrites the tail
to empty**. The new manifest's `segment_ulids` already absorbs every
live segment (the post-previous-snapshot drain and all completed GC),
so the emptied tail plus the new anchor still computes the identical
`live` set. No lazy reclamation, no compaction heuristic, no
cross-task coordination: a single writer resets a single object as
part of the seal it is already performing.

A reader that races the seal (reads old `LATEST` + new empty tail, or
new `LATEST` + old tail) still computes correct `live`: the manifest
set is the arbiter and the tail is a pure delta over whichever anchor
the reader resolved. The two are never required to be mutually
consistent at an instant.

## Rebuild defines correctness

Per the project invariant for derived state with rebuild + incremental
paths (`feedback_rebuild_is_invariant`), the **rebuild is the
specification** and the incremental tick-loop updates must
*structurally match what the rebuild would produce* — asserted in the
proptest model, not by convention.

The rebuild is small and anchor-bounded: an elevated, offline,
privileged LIST of `by_id/<vol>/segments/` **restricted to objects not
in the latest manifest**, reconstructing the delta. The invariant the
proptest asserts after every op (including a crash injected between
the segment object ops and the tail PUT):

> `live(manifest, tail)` ≡ `live(manifest, rebuilt-tail-from-LIST)`

Crash injection is expected to produce only the benign one-directional
divergence above (un-indexed object / tolerated 404), never a live-set
difference. This is `proptest-guardian` scope: the existing simulation
already drives drain/GC/reap; it is extended so the *tail object* —
not a LIST — is the queried set, with the reap step now inside the
tick loop.

## Reconcile and orphan reclamation

LIST is today's implicit "what is really in the bucket" self-heal.
Removing it from the runtime does not remove the need:

- The **manifest spine has no reconcile to add** — it is signed and
  durable on its own (P1/P2, `design-volume-event-log.md`).
- The **tail is authoritative for the runtime**; readers trust it.
  Bounded one-directional divergence is by construction (above), and a
  lost tail self-heals: the owning tick loop rewrites it whole next
  iteration from local `index/` + GC/reap state.
- **Orphan reclamation** (un-indexed objects from a crash between the
  segment PUT and the tail PUT) is an *explicit operator maintenance
  pass* that may use a privileged LIST under a separate elevated
  credential — deliberately not the coordinator runtime and not the
  exposed surface. Runtime stays LIST-free; repair is explicit and
  privileged, not a silent fallback, preserving "no optional
  correctness path in runtime".

## Consumer migration

| Site | Today | After |
|---|---|---|
| `prefetch.rs:442` (`pull_indexes_via_list`) | LIST `segments/` | manifest ∪ tail |
| `prefetch.rs:643` (`list_supersessions`) | LIST `retention/` | `Superseded` entries in tail |
| `fork.rs:670` | LIST `segments/` to verify pins present | manifest ∪ tail membership check |
| `recovery.rs:165` (force-release synthesis) | LIST `segments/` to re-sign | manifest ∪ tail; synthesised manifest absorbs the tail |
| `reaper.rs:80` | LIST `retention/`, separate task | `Superseded` entries in tail; reap step folded into the per-volume tick loop |

`pull_indexes_via_list` and `list_supersessions` collapse into one
manifest-anchored tail GET; `force_snapshot_now`'s "synthesise from
whatever is in S3 including segments past the latest snapshot" becomes
"latest manifest ∪ tail", and the synthesised handoff manifest it
writes absorbs the tail by the same truncation rule as any seal. The
coordinator-wide reaper task is removed; `reap_volume` becomes a
gated step in `tasks.rs`'s per-volume loop.

## Open questions

- **Reaper fold mechanics.** Folding `reap_volume` into the per-volume
  tick loop as a `reaper_interval`-gated step is the design above;
  the implementation must confirm a paused/not-running volume's loop
  still ticks often enough to honour the retention SLA (it does today
  for drain/GC; reap inherits the same gate).
- **Same-epoch reap without a tail read.** In-epoch the reap step
  needs no tail GET — it has the `Superseded`/`since` state it wrote
  this iteration and local `index/`. Only post-handoff must it read a
  prior owner's tail. Whether to special-case the same-epoch path or
  keep one uniform "read tail" path is an implementation call;
  correctness is identical (the durability-ordering delete gate is
  unchanged).
