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

Two classes:

### Class 1 — latest-pointer (single well-known key)

A monotonic pointer object written by conditional-PUT at publish; the
reader GETs it instead of listing.

- **`by_id/<vol>/snapshots/LATEST`** — content: latest snapshot ULID
  and its full dated manifest key. Written at snapshot publish
  (`upload.rs` snapshot path). Migrates the four "latest snapshot"
  consumers (`fetch.rs:325`, `fork.rs:561`, `prefetch.rs:949`,
  `start_remote.rs:147`) from LIST→max to GET-pointer.
- **`events/<name>/HEAD`** — the pointer already proposed in
  `design-volume-event-log.md`. Migrates `peer_discovery.rs:171` and
  `volume_event_store.rs:155/253`. This path runs under `coord-writer`
  today; coordinate the key shape with that doc rather than inventing a
  second one.

### Class 2 — maintained index (the live set)

`snapshots/` *enumeration* (cleanup/GC, `lifecycle.rs:777/1502`),
`segments/`, and `retention/` are intrinsically dynamic — accreted by
the WAL drain and GC, pruned by the reaper — so a pointer cannot
represent them. Each becomes a per-volume index object the *writer of
the underlying objects also maintains*:

- **segment index** — appended by the drain (`upload.rs`) as each
  segment is uploaded and by GC as it writes outputs; the reaper
  tombstones entries it deletes. Replaces `prefetch.rs:442`,
  `fork.rs:670`, `recovery.rs:165`.
- **retention index** — appended by GC with each supersession marker.
  Replaces `prefetch.rs:643`, `reaper.rs:80`.
- **snapshot index** — the set side of snapshots (distinct from
  `LATEST`), for snapshot GC/cleanup. Replaces `lifecycle.rs:777/1502`.

These may collapse into one per-volume append-only "manifest delta
log" rather than three objects — an implementation choice deferred to
P3, but constrained by the next section.

### Reconcile/repair without LIST

LIST is today's implicit source of truth ("what is actually in the
bucket"). Removing it removes that self-heal, so the plan must replace
it, not merely delete it:

- The index is **authoritative** for the runtime; readers trust it.
- Divergence is bounded and one-directional by construction if the
  index entry is written *after* the object PUT and *before* the
  operation reports success: a crash can leave an object with no index
  entry (a reclaimable space leak — never a correctness loss, since an
  un-indexed segment is simply not consumed), never an index entry
  with no object on a path that matters (readers already tolerate a
  `404` on segment fetch — `list_supersessions` explicitly does).
- The **rebuild defines correctness** (cf. the project invariant for
  derived state with rebuild + incremental paths): the index's
  authoritative regeneration is a one-time elevated LIST, and the
  incremental drain/GC/reaper updates must structurally match what
  that rebuild would produce. This is asserted in the proptest model
  (below), not just by convention.
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

- **P1 — snapshots `LATEST` pointer.** Write at publish; migrate the
  four latest-snapshot consumers. Smallest, self-contained, removes
  the most frequent per-volume LIST (prefetch warm-start).
- **P2 — `events/<name>/HEAD` pointer.** Migrate `peer_discovery` and
  `volume_event_store`; align with `design-volume-event-log.md`.
- **P3 — segment index.** Drain + GC maintenance, crash-ordering as
  above; migrate `prefetch`/`recovery`/`fork-verify`; define + test
  the reconcile invariant.
- **P4 — retention + snapshot-set indexes** (or fold into P3's log);
  migrate `prefetch` supersession, `reaper`, and snapshot
  cleanup/GC (`lifecycle.rs:777/1502`).
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
