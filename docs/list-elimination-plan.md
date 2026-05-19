# LIST-elimination plan

Remove every `s3:ListBucket` use from the coordinator runtime. Each
prefix LIST becomes a deterministic GET; then the `ListBucket`
statement is deleted from `coord-writer`'s role template and from
`design-mint.md` (resolves open question #12). The decision stands
independent of whether Tigris can prefix-scope `ListBucket` — that is
parked (`design-mint.md` #12); this work is the backend-portable answer
and a long-wanted perf win regardless.

Authority: `design-mint.md` § *`coord-writer`* / open question #12.
Related: `design-volume-event-log.md` (event HEAD pointer),
`design-peer-segment-fetch.md`.

## The LIST surface (from a full sweep of `elide-coordinator/src`)

| Prefix LISTed | Call sites | What it derives | Role today |
|---|---|---|---|
| `by_id/<vol>/snapshots/` | `fetch.rs:325`, `fork.rs:561`, `prefetch.rs:949`, `start_remote.rs:147` | max snapshot ULID + its dated `.manifest` key | `coord-data` |
| `by_id/<vol>/snapshots/` | `inbound/lifecycle.rs:777`, `inbound/lifecycle.rs:1502` | the *set* of snapshots, for cleanup/delete | `coord-data`/`writer` |
| `by_id/<vol>/segments/` | `prefetch.rs:442`, `fork.rs:670`, `recovery.rs:165` | the live segment-ULID set for the volume | `coord-data` |
| `by_id/<vol>/retention/` | `prefetch.rs:643` (`list_supersessions`), `reaper.rs:80` | GC supersession markers (input→output) | `coord-data` |
| `events/<name>/` | `peer_discovery.rs:171`, `volume_event_store.rs:155/253` | the event-record set / head for a name | `coord-writer` |

`config.rs:289` (`probe`, bare `by_id/`) is the *non*-mint passthrough
reachability check — not on the mint path, out of scope.

Keys are date-partitioned (`…/snapshots/YYYYMMDD/<ulid>.manifest`,
`…/segments/YYYYMMDD/<ulid>`), so today's LIST is a recursive prefix
scan; the substitutes below do not need the date partition.

## Substitution design

### Identity axes — why snapshots split cleanly

`name` and `vol_ulid` are two identity axes, instantaneously
bijective for a live name but **not stable over time**:

- **`names/<name>`** — stable ownership identity. Its lifecycle
  (Created/Claimed/Released/ForceReleased/Renamed/ForkedFrom) is
  intrinsically per-*name* — ownership CAS, cross-host handoff
  rendezvous, rename — and *cannot* move to per-vol: a claiming
  coordinator finds the volume **through the name**; it does not know
  the new `vol_ulid` until it reads the name. This is the event log,
  `events/<name>/`, under `coord-writer`.
- **`by_id/<vol_ulid>/`** — the data (segments/snapshots/retention),
  per-*vol_ulid*, under `coord-data`. A name walks through a sequence
  of `vol_ulid`s over its life (fork lineage); a `vol_ulid`'s data
  outlives the instant (ancestors are read by descendants).

Role restriction did not create this coupling — it made a pre-existing
axis crossing legible. Snapshots split along it exactly, and
respecting the split (rather than forcing a cross-axis write) is what
keeps the design simple:

- **Per-vol "latest snapshot in *this* `vol_ulid`"** — a
  `by_id/<vol>/snapshots/LATEST` pointer (per kind:
  `snapshot_take_new`, stable vs `-stop`), written conditional-PUT at
  publish by the per-vol path, **under `coord-data` only — no
  cross-role write, no new event**. Migrates `fetch.rs:325`,
  `fork.rs:561`, `prefetch.rs:949`, `start_remote.rs:147`,
  `lifecycle.rs:777`. Reconstructable from local volume state / the
  next publish (a lost pointer self-heals; not a correctness datum).
- **Per-name "the handoff/fork point to consume"** — *already* on the
  event log: `Released`/`ForceReleased` carry `handoff_snapshot`,
  `ForkedFrom` carries `source_snap_ulid`. The claimant reads it off
  the chain it is already walking. Covers `lifecycle.rs:560/707` and
  the claim path. `latest_release_handoff_snapshot`'s LIST
  (`lifecycle.rs:560/707`) is *pure redundancy today* — it LISTs to
  recompute a ULID the `Released` event already records — and on the
  release side the coordinator just sealed the snapshot, so it knows
  the ULID directly without LIST or event read.
- **Leftover `-stop` cleanup** (`lifecycle.rs:1502`) — the `-stop`
  being swept *is* the handoff snapshot; its ULID is in the per-name
  `Released` event. Delete by known key; no per-vol enumeration.

A full sweep confirms **no consumer needs a per-vol snapshot *set***:
there is no stable-snapshot retention/GC enumerator, and `prefetch`
resolves ancestor snapshots from the *branch ULID in signed
provenance* (name-axis lineage), only the writable head wanting the
per-vol `LATEST`. So no `SnapshotPublished`/`SnapshotDeleted` events,
no snapshot projection, no snapshot index — snapshots are a per-vol
pointer plus references the per-name log already carries.

### The event-log spine (existing events, existing back-links)

- **`events/<name>/HEAD` — a bounded window, not a scalar pointer.**
  `HEAD` carries the **last *N* events as full signed records**,
  rebuilt on every append as `new_HEAD = (new_event :: old_HEAD)[..N]`
  — one conditional-PUT, the same write that advances ordering today,
  just carrying *N* compact entries instead of one ULID. Events are
  tiny (ULID + enum + coord id + sig); N≈16 is a few KB.

  **`prev_event_ulid` is already on every record** (`volume_event.rs`,
  written by `emit_event`), so the authoritative chain already exists;
  what is LIST-based today is only latest-ULID *discovery*
  (`latest_event_ulid`) and the two walkers choosing LIST+sort over
  the link that is already there. **No event-format change, no new
  event kinds.**

  **HEAD is a cache, never a trust root.** Each entry is the
  already-individually-signed event record, so a tampered or dropped
  entry fails the existing per-event signature check or simply falls
  back to the prev-walk; correctness rests on the per-event
  signatures + the authoritative `prev_event_ulid` chain, exactly as
  today. HEAD's "rebuild" *is* the prev-walk (the project invariant
  for derived state: the rebuild defines correctness).

  **Crash ordering:** event record PUT *before* the HEAD rewrite (same
  discipline as the segment index). A crash between leaves HEAD
  stale-by-one; the next reader sees the newest record's `prev` ≠
  HEAD's top and repairs by one hop, or prev-walks. Staleness is
  bounded and self-healing.

  Replaces `peer_discovery.rs:171`, `volume_event_store.rs:155/253`.
  Key shape coordinated with `design-volume-event-log.md`. Runs under
  `coord-writer`. *N* is a tuning param (default ≈16), not pinned by
  the design.

#### Access patterns (0 hops common; bounded fallback; `--all` opt-in)

1. **Append** — `GET HEAD`, write the record (`prev =` HEAD's top),
   rewrite `HEAD`. O(1), no walk. The common write path
   (`Created`/`Claimed`/`Released`/`Renamed`); replaces
   `latest_event_ulid`'s LIST.
2. **Claim / peer-discovery** — the decisive event
   (`Released`/`ForceReleased`/`ForkedFrom`) and its payload are
   almost always within the last *N*, so they are **in the HEAD GET
   itself — zero extra hops**. Only a pathological tail (>*N* events
   since the last `Released`) falls back to the bounded
   `prev_event_ulid` walk. Subsumes the redundant
   `latest_release_handoff_snapshot` LIST. (Peer-discovery still does
   one *keyed* GET for the releaser's `coordinators/<id>/peer-endpoint`
   — not a walk, unavoidable.)
3. **Operator `volume events`** — bounded **recent-N**: served
   entirely from the HEAD window when the CLI default ≤ *N* (**zero
   walk**); larger windows or `--all` fall back to the prev-walk
   (`--all` = full to-genesis incl. the `inherits_log_from` rename
   crossing, still LIST-free). Removes the unbounded default walker;
   `list_events`' whole-prefix LIST goes away.

So at runtime the chain is essentially never walked: appends are a
single GET+PUT, the common claim/peer-discovery and the default
history view are answered from the one HEAD GET, and a `prev` walk
happens only on a long unclaimed tail or an explicit `--all`.

### Maintained index (`segments`, `retention` only)

The genuinely high-cardinality per-write sets — accreted by the WAL
drain and GC, pruned by the reaper — are too large to fold from a
chain on every read, so they keep a dedicated per-volume index object
(`coord-data`, same axis as the data):

- **segment index** — appended by the drain (`upload.rs`) as each
  segment is uploaded and by GC as it writes outputs; the reaper
  tombstones entries it deletes. Replaces `prefetch.rs:442`,
  `fork.rs:670`, `recovery.rs:165`.
- **retention index** — appended by GC with each supersession marker.
  Replaces `prefetch.rs:643`, `reaper.rs:80`.

These two may collapse into one per-volume append-only "manifest
delta log" — an implementation choice deferred to its phase, but
constrained by the next section. Snapshots are deliberately **not**
here (per-vol pointer + per-name handoff references, above).

### Worked example — a release/claim cycle

Coordinator **A** owns `myvol`; **B** later claims it. Every step is a
GET or a known-key PUT/DELETE — no LIST.

1. **Steady state (A).** A seals snapshot `S2`: writes
   `by_id/<vol>/snapshots/<date>/S2.manifest` and bumps
   `by_id/<vol>/snapshots/LATEST → (S2,Stable)`. Both writes are
   per-vol, **`coord-data` only** — no event, no `coord-writer`.
2. **Release (A).** A seals the handoff/stop snapshot `Sh` (it knows
   `Sh`'s ULID directly — it just minted it), CASes `names/myvol`
   Live→Released, appends `Released{handoff_snapshot: Sh}` to
   `events/myvol/`, advances `HEAD`. This event **already exists
   today**; nothing new on the name axis.
3. **Claim (B).** B CASes `names/myvol` Released→Claimed. It learns
   the fork point from the **single `HEAD` GET** — `Released{handoff:
   Sh}` is in the window (no `prev` walk in the common case;
   replacing the redundant `latest_release_handoff_snapshot` LIST). B
   appends `Claimed` and rewrites `HEAD`.
4. **Hydrate (B).** From `Sh.manifest` (a GET, key known from step 3)
   B gets the segment ULID set — the manifest already enumerates
   segments, so no LIST; any segment not local is range-GET by
   deterministic key.
5. **Stop-snapshot cleanup (B).** Today `lifecycle.rs:1502` LISTs the
   snapshot prefix to find leftover `-stop` objects. `Sh` is already
   known from step 3's event walk: B `DELETE`s `Sh` by known key. No
   LIST, no new event (the name log already recorded `Sh` via
   `Released`; its consumption needs no separate marker).

The invariant the example illustrates: **the name axis (event log)
carries ownership and the cross-epoch handoff/fork snapshot
references; the vol axis carries the data and a per-vol `LATEST`
pointer. Neither writes across to the other; every read is a chain
walk from `HEAD` or a known-key GET.**

### Reconcile/repair without LIST

LIST is today's implicit source of truth ("what is actually in the
bucket"). Removing it removes that self-heal, so the plan must replace
it, not merely delete it:

- **Snapshots / event spine have no LIST reconcile to add.** The
  per-name handoff/fork references live in events that already exist
  and are durable on the chain (`HEAD` durability is
  `design-volume-event-log.md`'s concern, not redesigned here). The
  per-vol `snapshots/LATEST` pointer is not a correctness datum: it is
  reconstructable from local volume state and is overwritten by the
  next publish, so a lost/stale pointer self-heals — a perf event, not
  a correctness one.
- **The segment/retention index is authoritative for the runtime**;
  readers trust it. Divergence is bounded and one-directional by
  construction if the
  index entry is written *after* the object PUT and *before* the
  operation reports success: a crash can leave an object with no index
  entry (a reclaimable space leak — never a correctness loss, since an
  un-indexed segment is simply not consumed), never an index entry
  with no object on a path that matters (readers already tolerate a
  `404` on segment fetch — `list_supersessions` explicitly does).
- The **rebuild defines correctness** (cf. the project invariant for
  derived state with rebuild + incremental paths): the
  segment/retention index's authoritative regeneration is a one-time
  elevated LIST, and the incremental drain/GC/reaper updates must
  structurally match what that rebuild would produce — asserted in the
  proptest model (below), not by convention.
- Orphan reclamation (un-indexed objects) is an **explicit operator
  maintenance pass** that may use a privileged LIST under a separate
  elevated credential — deliberately *not* the coordinator runtime or
  the exposed surface. Runtime stays LIST-free; this keeps the "no
  optional correctness path in runtime" principle intact (repair is
  explicit and privileged, not a silent fallback).

## Phasing

Each phase is independently shippable and leaves the tree green; no
phase introduces a dual LIST+index runtime fallback (that would defeat
the purpose and is itself an optional-correctness path).

Ordered so each phase builds on the prior.

- **P1 — event-log spine: windowed `events/<name>/HEAD`.** Add the
  `HEAD` object carrying the last *N* signed records (rebuilt on
  append; record PUT before HEAD rewrite). `emit_event` reads/advances
  `HEAD` instead of LIST-max; `peer_discovery` and
  `volume_event_store` read the window, falling back to the
  **already-present** `prev_event_ulid` walk only on a long tail.
  **No event-format change, no new event kinds.** Change `volume
  events` to bounded recent-N (served from the window; `--all` =
  explicit to-genesis prev-walk). Align key/shape with
  `design-volume-event-log.md`. Also gives the claim path its handoff
  from the HEAD window via the existing `Released`/`ForkedFrom`
  events.
- **P2 — per-vol `snapshots/LATEST` pointer.** Write it (per kind) at
  publish under `coord-data`; migrate the latest-snapshot consumers
  (`fetch.rs:325`, `fork.rs:561`, `prefetch.rs:949`,
  `start_remote.rs:147`, `lifecycle.rs:777`). Repoint the handoff /
  cleanup consumers (`lifecycle.rs:560/707/1502`) at the P1 chain
  walk / known-key delete. Removes every snapshot LIST; no new events,
  no cross-role write.
- **P3 — segment index.** Drain + GC maintenance, crash-ordering as
  above; migrate `prefetch`/`recovery`/`fork-verify`; define + test
  the reconcile invariant.
- **P4 — retention index** (or fold into P3's delta log); migrate
  `prefetch` supersession and `reaper`.
- **P5 — drop the grant.** Delete `s3:ListBucket` from
  `mint/examples/elide_roles/coord-writer.json`, the §*`coord-writer`*
  policy, and the role-inventory table in `design-mint.md`; add a CI
  grep guard that no `.list(` reaches a mint-backed store. End state:
  no role carries `ListBucket`.

## Back-compat

Clean break (project default). Indexes/pointers are derived state,
regenerated once by an elevated offline migration (or by republishing
a snapshot). No on-disk format negotiation, no runtime dual path.

## Validation

- Per phase: targeted unit/integration tests.
- The proptest simulation already drives drain/GC/reaper; extend it so
  the index — not a LIST — is the queried set, and assert the
  index ≡ object-set invariant after every op, including crash
  injection between object PUT and index append (proptest-guardian
  scope).
- End-to-end on the Tigris VM with `coord-data` carrying no
  `ListBucket` and `coord-writer`'s `ListBucket` removed.

## Out of scope / revisit later

- Whether Tigris honours prefix-scoped `ListBucket` (`design-mint.md`
  #12). If it does, this work still stands (perf + portability); it
  only relaxes the security urgency.
- `volume list --remote` and any operator-facing bucket enumeration —
  these legitimately enumerate and run under an explicit elevated
  credential, not the coordinator runtime; they are not in this
  removal.
- The interim credential posture before P5 lands (the per-volume LIST
  paths fail on Tigris under `coord-data` until then) — a separate
  decision, tracked with the mint cutover, not here.
