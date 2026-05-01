# Per-name event log

**Status:** Proposed.

A volume's *name* is the user-facing identifier that persists across
forks, host transfers, claim/release cycles, and snapshots. Today the
authoritative state of a name lives in a single S3 object —
`names/<name>` — updated by conditional PUT
(see [`design-portable-live-volume.md`](design-portable-live-volume.md)).
That object captures **what is true now**, but throws away **how we
got here**. The fork chain (`vol_ulid` ancestry) carries some of the
history, but only for episodes that produced a new fork; it does not
record stops/starts, force-releases, or renames, and it cannot be
walked from the human-facing name without already knowing the current
`vol_ulid`.

This proposal adds a per-name event log under `events/<name>/`,
written append-only as a coordinator transitions a name's state. The
log is the durable, ordered, signed history of every operation that
has touched the name. The pointer at `names/<name>` remains
canonical for "now"; the event log is canonical for "ever".

## Why a separate log

The current pointer object can't double as a history record:

- **Conditional PUT clobbers the previous state.** Each transition
  overwrites `names/<name>`. By the time anyone reads it, prior
  states are gone.
- **The fork chain is incomplete.** Stops, starts, and force-
  releases don't mint a new fork. Renames don't either (see below).
  None of these appear in `walk_ancestors`.
- **Cross-host audit needs a shared artefact.** Every coordinator
  that has ever held a name should leave a trace that any other
  coordinator can read, without RPC and without local caches
  agreeing.

A separate prefix is the natural shape: object-per-event, ULID-named,
write-once, conflict-free. Listing the prefix gives the ordered
history. No append protocol, no shared mutable state beyond the
pointer that already exists.

## Layout

```
names/<name>                              — current state pointer (existing)
events/<name>/<event_ulid>.toml           — append-only journal (new)
```

Events live under a separate top-level prefix from the pointer.
The pointer key (`names/<name>`) and the event prefix
(`events/<name>/`) never share a parent, so `aws s3 ls names/`
returns names and only names; `aws s3 ls events/` returns event
logs and only event logs. Operators inspecting the bucket never
have to mentally separate "is this key the pointer or a prefix
for events?". A "delete a name" verb (not yet a verb in elide)
would walk both prefixes, which is the same cost as one prefix
plus a single key delete.

Each event is a small TOML object named by its ULID. The ULID
provides:

- **Total order.** Listing `events/<name>/` and sorting
  lexicographically yields the canonical history.
- **Origin time.** The ULID's millisecond prefix records when the
  emitting coordinator wrote the event (advisory; not a fence).
- **Conflict-free naming.** Two coordinators emitting concurrent
  events produce distinct keys; both writes succeed without
  coordination.

## Event schema

```toml
version = 1
event_ulid = "<ulid>"
kind = "claimed"                          # see catalogue below
at = "2026-04-30T12:34:56Z"               # rfc3339, advisory
name = "<volume-name>"                    # duplicates the path segment; signed
coordinator_id = "<emitting-coordinator>"
hostname = "<emitter-host>"               # advisory; absent if gethostname() failed
vol_ulid = "<vol_ulid_at_this_event>"     # current fork at emit time
prev_event_ulid = "<ulid>"                # last event this emitter saw; absent on first event
signature = "<ed25519-sig-over-canonical-form>"
# kind-specific fields below
```

- **`signature`** is over the canonicalised TOML (sorted keys, fixed
  whitespace), prefixed with the fixed domain tag
  `"elide volume-event v1\0"`, using the emitter's `coordinator.key`
  (the same identity introduced in
  `design-portable-live-volume.md`). Verification is by lookup at
  `coordinators/<coordinator_id>/coordinator.pub`. This makes the
  log tamper-evident: a malicious bucket-writer can drop or reorder
  events but cannot forge them under another coordinator's
  identity. The in-message domain tag prevents signature
  substitution between events and other coordinator-signed
  artefacts (e.g. synthesised handoff snapshots) without requiring
  a second key.
- **`name`** duplicates the `<name>` segment of the on-disk key
  (`events/<name>/<event_ulid>.toml`). The path is already
  authoritative for "which name does this event belong to"; the
  in-body field exists so a raw `cat` of an event file is
  self-describing, and so the signature binds the event to a
  specific name. Without it, a bucket-rewriter could copy a signed
  event under `events/<other>/` and the signature would still
  verify — the field closes that gap by including the name in the
  canonical signing payload.
- **`hostname`** mirrors `NameRecord.hostname`: a human-meaningful
  host name captured by the emitting coordinator, advisory only,
  never compared for ownership. Stamped on every event so an audit
  trail attributes each transition to a host name in addition to
  the opaque `coordinator_id`. Read once from `gethostname()` at
  coordinator startup and cached; absent if the syscall failed or
  returned non-UTF-8 bytes. Included in the canonical signing
  payload — a bucket-rewriter cannot substitute a forged hostname
  under the original signature.
- **`prev_event_ulid`** is the writer's view of "the previous event
  on this name's log". It is *not* a strict hash chain — concurrent
  writers may both name the same `prev_event_ulid` — but it lets a
  reader detect when an emitter was unaware of a concurrent event,
  which is useful provenance for force-release and rename
  reconciliation. Emitters source this value from in-memory state in
  the warm case ("the event I just emitted" or "the latest I observed
  while watching this name"); a LIST of `events/<name>/` is only
  required on cold paths — fresh claim of a name the coordinator has
  never owned, or recovery after restart with no cached state.
  Steady-state event emission does not pay LIST cost. Backward
  chaining via `prev_event_ulid` lets readers verify history once they
  have an entry point but does not help locate the head of the log.

## Event kinds

The catalogue tracks **transitions of the name's identity or
ownership**, not every internal volume operation. Segment
publication, ordinary snapshots, and pure daemon-state changes
(`start`/`stop` flipping `Live ↔ Stopped` under the same owner)
stay out — they belong to the fork's local history, not the name's.
Adding them later is additive; starting narrow keeps the log
readable.

| Kind | Emitter | When |
|---|---|---|
| `created` | first creator | initial `names/<name>` write (writable or readonly) |
| `claimed` | acquiring coordinator | after `released → stopped/live` CAS succeeds |
| `released` | releasing coordinator | after `live/stopped → released` CAS succeeds |
| `force_released` | recovering coordinator | after `release --force` rewrites the pointer |
| `forked_from` | new fork's coordinator | when this name was created via `volume create --from` (emitted on the *new* name's log only — see open question 1) |
| `renamed_to` | owner | terminal event when this name is renamed away (see Rename) |
| `renamed_from` | owner | opening event when this name was just renamed in (see Rename) |

Daemon `start`/`stop` are deliberately absent: they don't move
ownership, don't change `vol_ulid`, and their durable record is
already "this coordinator is the listed owner". Surfacing them as
events would multiply the log volume for no auditable signal that
isn't already in the pointer.

Snapshot publication is also absent. Handoff snapshots are embedded
in `released` / `force_released` events; user snapshots are a
fork-local concern. If we later want a richer log, we add events;
old readers that don't recognise a kind treat it as opaque.

## Rename

Rename is the operation that motivated thinking about an event log
shape rather than treating each name as a disconnected pointer. The
fork chain doesn't help: rename does not mint a new fork. Without
the log, the human-facing identifier "foo → bar" is invisible.

**Proposed:** rename is a two-event boundary that ties the two name
logs together without copying history.

Preconditions: the name is `stopped` and held by the renaming
coordinator. (Renaming a `live` name racing with claim/release is
disallowed; rename a `released` name is also disallowed — claim it
first.)

Steps:

1. Conditional PUT to `names/<old>` flipping `state` to a new
   terminal `renamed` value, recording `renamed_to = "<new>"`. The
   pointer is *kept*, not deleted: it acts as a tombstone preventing
   re-creation of `<old>` and as a forward link for log walkers.
2. Conditional create (`If-None-Match: *`) of `names/<new>` with the
   same `vol_ulid`, `coordinator_id`, and `state = stopped`.
3. Emit `renamed_to` into `events/<old>/`.
4. Emit `renamed_from` into `events/<new>/`, with
   `inherits_log_from = "<old>"`.

Walking the log forward across renames: follow `renamed_to`. Walking
backward: follow `renamed_from` / `inherits_log_from`. Both
directions terminate at the original `created` event. Each `<name>`
in the chain has its own `events/<name>/` prefix; the walk
crosses between sibling prefixes, never up and down a hierarchy.

The fork chain (`vol_ulid` ancestry) is unchanged. Rename is purely a
human-facing rebinding; the underlying volume identity is untouched.

**Decision:** `renamed` is a first-class pointer state alongside
`live`/`stopped`/`released`/`readonly`. It is terminal for the old
name (no further transitions; the pointer is a tombstone forwarding
to `<new>`) and lifecycle verbs (`claim`, `start`, `stop`,
`release`) all refuse it cleanly with an error pointing at
`<new>`. Encoding it as `released + renamed_to` would let an
unaware claim path treat the old name as available; a dedicated
state makes the refusal table obvious.

## How claim/release consult the log

The pointer at `names/<name>` remains the single CAS-protected
source of truth for transitions. The log is **journal, not protocol**:

- Every successful CAS on `names/<name>` is followed by a best-
  effort PUT into `events/<name>/`. If the emitter crashes
  between the two writes, the log has a gap; the pointer is still
  authoritative. The next emitter's `prev_event_ulid` will skip the
  gap, leaving an auditable hole.
- `volume claim` and `volume release` decide based on the pointer
  + CAS, exactly as today. They may consult the log for two soft
  purposes:
  1. **Operator display.** `elide name log <name>` renders the
     events for human review before action.
  2. **Force-release context.** When B is recovering name from a
     dead coordinator A, B may inspect the log to confirm A's last
     observed `vol_ulid` matches what segment listing produces. A
     mismatch is a warning, not a hard refusal — segments are the
     authoritative material.
- The log is not a fencing token. It is not on the critical path of
  any transition. Removing it would not change the correctness of
  claim/release; it would only erase history.

This keeps the log light: we add audit and observability without
adding a new consistency requirement to an already load-bearing
pointer.

## Retention

**For now: forever.** The log is small (a TOML file per lifecycle
transition is at most a few KB), and a name that lives for years
producing a few hundred events is well below any operationally
interesting threshold. Archival — moving old events under
`events/<name>/archived/<year>/...` or rolling them into a
single signed manifest — is deferred until we have a concrete
reason. Until then, "list and sort" stays the canonical reader.

Deletion of a name (not yet a verb in elide) would emit a `deleted`
event and tombstone the pointer; the log would persist alongside it.
That's consistent with the rest of the system, where ULID-named
artefacts are never reused.

## Object store requirements

Same as `design-portable-live-volume.md`:

- Strong read-after-write consistency on individual keys (already
  required for the pointer itself).
- Conditional writes for the pointer (already required).

The event objects themselves need only **strong read-after-write**,
which every supported backend already gives. They are write-once
under unique ULID keys; no `If-Match`/`If-None-Match` is needed for
event PUTs (a duplicate ULID would be a bug, not a race).

On backends without conditional PUT, the pointer falls back to
host-local semantics (per the portability doc) and the event log
follows: it remains a per-host best-effort journal, identical in
shape but without the global-truth property.

## Open questions

1. **Should `forked_to` events appear in the source name's log, or
   only in the new name's log as `forked_from`?** Both is symmetric
   but doubles the cost. One-sided keeps the log tighter; either
   direction is recoverable via the pointer in the new name. Lean:
   `forked_from` only, on the new name; the source name learns of
   forks through replica/snapshot listings, not its own log.
2. **Per-coordinator emitter vs name-owner only.** Should *any*
   observer be able to emit events (e.g. a recovering coordinator
   logging "I observed the dead owner's last segments")? Or only
   the current pointer-holder? Restricting to the current holder is
   simpler; allowing observers makes force-release and migration
   more inspectable. Lean: holder only for the lifecycle events;
   force-release and rename are special cases the doc already
   covers.
3. **Surface in CLI.** `elide name log <name>` is the obvious read
   verb. Worth confirming whether this lives under a new `name`
   noun (`elide name log`, `elide name list`) or extends
   `volume status` (`volume status --remote --history`).

## Why we might not want this

- **A second mutable surface, in spirit.** The pointer is mutable;
  the event log is append-only but lives next to it. Operators now
  have two artefacts to reason about for one logical name. We keep
  the pointer authoritative to mitigate this, but the conceptual
  surface area still grows.
- **Best-effort writes hide gaps.** A coordinator that crashes
  between pointer-CAS and event-write leaves a silent gap. The log
  is detectably incomplete (`prev_event_ulid` skip), but only if
  someone actually looks. Treating the log as authoritative would
  require the two-phase write to be atomic, which would either
  burden the pointer protocol or require a transactional bucket
  primitive we don't have.
- **The fork chain already covers some of this.** For volumes that
  migrate via fork, `walk_ancestors` already produces a coordinator-
  by-coordinator history. The event log adds the *non-fork*
  transitions (stops, starts, force-releases, renames), which is
  real value, but it overlaps with what's already there. Worth
  confirming the event log's incremental information justifies the
  extra prefix.
