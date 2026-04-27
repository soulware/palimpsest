# Portable live volume

**Status:** Accepted. Implementation plan in
[`portable-live-volume-plan.md`](portable-live-volume-plan.md).

The proposal gives a conceptually cleaner naming and lifecycle model
than today's "live = host-pinned, fork to relocate" pattern, by
reusing the existing fork mechanism rather than introducing new
share-state-across-hosts machinery. Decision recorded 2026-04-27;
fresh-bucket-only (no migration), clean break for `volume remote`
(no aliases).

## The current model, in one paragraph

A volume's identity is its `<vol_ulid>`. The S3 prefix
`by_id/<vol_ulid>/` holds its segments, signed manifests, public
key, and provenance. A *live* volume is the one host that has its
WAL, signing key, and is producing new segments. Other hosts can
read its S3 prefix but cannot become it. The escape hatch for "I
want this elsewhere" is `volume create --from
<vol_ulid>/<snap_ulid>` — that produces a **new** volume with a new
ULID, new signing key, and a fork relationship to the original. This
is the right shape for divergence, but a heavy shape for relocation:
a host swap (rebalancing, hardware replacement, planned downtime)
either loses identity or pins the workload to one machine forever.

## The proposal

A **named volume** is portable across hosts. Its name is stable;
the underlying `<vol_ulid>` changes every time ownership transfers.
Each ownership episode is a fork of the previous one. At any moment
the name resolves to exactly one coordinator's fork.

The on-disk picture:

- `<vol_ulid_1>` — first owner's fork. Frozen after `stop`.
- `<vol_ulid_2>` — second owner's fork, parent = `<vol_ulid_1>` at
  its stop snapshot. Frozen after `stop`.
- `<vol_ulid_3>` — third owner's fork, parent = `<vol_ulid_2>` at its
  stop snapshot. Currently live.
- `names/<name>` → `<vol_ulid_3>` (with the current owner's
  coordinator id).

Each `<vol_ulid_i>` is written by exactly one coordinator and never
mutated by anyone else. The only mutable thing in the bucket is
`names/<name>`.

## Why this works

- **Signing-key portability is solved by not needing it.** Each fork
  has its own per-volume Ed25519 key, generated locally by the
  acquiring coordinator and never leaving that host. Verifiers verify
  segments against their own fork's `volume.pub`. The "key history" is
  the fork chain — which `walk_ancestors` already produces.
- **No shared mutable prefix.** Two coordinators never write to the
  same `<vol_ulid>/` prefix. There is no fencing token to plumb into
  segment manifests, no race for ETag-conditional writes on a marker
  inside a contested directory.
- **Audit trail is provenance.** Walking the fork chain reveals
  every coordinator that has ever owned the name, the snapshot at
  each handoff, and the current owner. No parallel audit log.
- **Names are globally consistent.** Today volume names are
  per-host (`by_name/` is local; the same name can refer to
  different `<vol_ulid>`s on different hosts — see
  `project_volume_naming`). Promoting `names/<name>` to the
  authoritative record fixes that: the first creator establishes
  the name, every subsequent operation refers to the same name on
  every host, and a name collision on `create` is a hard error
  enforced by the conditional PUT. Forks (`volume create --from
  <vol_ulid>/<snap_ulid> <new-name>`) still take a fresh name —
  divergence stays explicit, and the fresh name is itself globally
  unique.
- **Replica references stay valid forever.** Anyone who pinned
  `<vol_ulid_i>/<snap_ulid>` for a replica keeps a valid pin: the
  prefix is frozen, not deleted.

## Mechanism: `names/<name>` as the owner pointer

`names/<name>` becomes a small structured object:

```toml
vol_ulid = "<current_fork_ulid>"
coordinator_id = "<owner-coordinator-id>"
state = "live"            # or "stopped"
parent = "<prev_ulid>/<prev_snap_ulid>"   # absent on the root
acquired_at = "<rfc3339>"
hostname = "<owner-host-at-acquire-time>"  # advisory only
```

All transitions are conditional PUTs (`If-Match` on ETag, supported
by S3 and Tigris). Conditional-write atomicity on this single object
is the entire ownership protocol.

### Object store requirements

The whole proposal rests on two primitives, applied only to
`names/<name>`:

1. **Strong read-after-write consistency** on individual keys.
2. **Conditional writes** — `If-None-Match: *` for create-if-absent,
   `If-Match: <etag>` for compare-and-swap on existing keys.

Combined, these give a global, linearizable, single-key
compare-and-swap. That one primitive carries name uniqueness,
ownership transfer, and the explicit-skip semantics of
`--force-takeover`. Everything else in the system is append-only or
immutable — `<vol_ulid>/` prefixes are written by one coordinator
and never touched again, segments are content-addressed, snapshots
and provenance are signed and frozen — so no further coordination is
needed. Every reader independently reaches the same conclusion about
what is there.

This makes the portability story compatible with: S3 (current
post-2020 consistency model), Tigris, R2, MinIO, and Ceph RGW. It is
**not** compatible with eventually-consistent stores, last-write-wins
configurations, or any backend that lacks conditional PUT. The system
already requires strong consistency for snapshot publication; adding
conditional PUT is an incremental requirement, not a new dependency
class.

### Graceful degradation when the bucket doesn't support it

Lack of conditional PUT must not break Elide; it just means
portability is unavailable on that backend. The OSS path stays
intact:

- **Detection.** On first connection to a bucket, the coordinator
  probes by attempting a conditional PUT on a throwaway probe key
  (e.g. `_capabilities/conditional-put-probe-<ulid>`). The result is
  cached per-bucket. Optionally a `_capabilities/` declaration file
  can override the probe.
- **What stays available without conditional PUT:** all current
  Elide functionality. `volume create`, `volume create --from
  <vol_ulid>/<snap_ulid>`, segment upload, snapshot publication,
  demand-fetch, replicas. Names are per-host (today's behaviour) —
  `names/<name>` is either advisory or omitted entirely.
- **What gets disabled:** `volume start <name>` for a name not held
  by this coordinator. The error points the user at the supported
  alternative:

  ```
  $ elide volume start mydb
  error: portable volumes require conditional PUT support, which
         your object store does not provide.
         use `elide volume create <new-name> --from <vol_ulid>/<snap_ulid>`
         to fork the volume into a new local volume.
  ```

- **What `create` does on a non-portable backend:** since name
  uniqueness can't be enforced across coordinators, the name is
  treated as host-local (current behaviour). Two coordinators that
  independently `create` the same name produce two distinct volumes,
  same as today.

The fallback for any portability-needing user is `create --from`,
which never needs conditional PUT (it produces a fresh ULID and
prefix, no shared-key contention) and gives them divergence-as-
relocation — which is exactly today's escape hatch. This is honest:
on a backend that can't carry portability, we don't pretend it can;
we point users at the verb that actually works.

### `coordinator_id` derivation

`coordinator_id` derives from the existing
`<data_dir>/coordinator.root_key`:

```rust
let coordinator_id = blake3::derive_key(
    "elide coordinator-id v1",
    &root_key,
);
```

Domain-separated, no new on-disk state, raw key never leaves the
coordinator. Two coordinators on the same machine are distinct
identities; one coordinator moved between machines is the same
identity. Hostname is recorded as a debugging hint only — never
compared for ownership decisions.

**Operational consequence:** deleting `coordinator.root_key` (or
losing the data dir) ends that coordinator's identity. Volumes whose
`names/<name>` pointer names the old coordinator can only be
reclaimed via `--force-takeover`. This is the right behaviour and
doubles as an explicit escape hatch for a misbehaving coordinator —
delete the root key, restart, takeover from a clean identity. The
old key's macaroons become unverifiable at the same moment, so
clients re-auth anyway.

## Flows

### `volume stop <name>` — explicit relinquish

1. Stop accepting client writes (close NBD/ublk transport).
2. Drain WAL: promote pending records into segments, finish in-flight
   uploads.
3. Publish a handoff snapshot covering everything published. Record
   handoff metadata: outgoing `coordinator_id`, hostname,
   `acquired_at` of the current episode.
4. Conditional PUT to `names/<name>` setting `state = "stopped"`,
   keeping `vol_ulid` pointing at the now-frozen fork and
   recording the handoff snapshot.
5. The local `by_id/<vol_ulid>/` directory may be discarded (it's
   reproducible from S3) or kept as cache for fast reacquisition by
   the same coordinator. **Never** keep the WAL — there is none past
   the published snapshot, by construction.

### `volume start <name>` — claim ownership

1. Read `names/<name>`. If `state = "live"` and `coordinator_id != self`,
   refuse (use `--force-takeover` to override).
2. Mint a fresh `<new_ulid>` and generate a fresh Ed25519 keypair.
3. Create `by_id/<new_ulid>/` locally, with provenance pointing at
   the previous fork's `<vol_ulid>/<handoff_snap_ulid>`. Publish the
   new `volume.pub` and signed provenance.
4. Conditional PUT to `names/<name>`: `vol_ulid = <new_ulid>`,
   `coordinator_id = self`, `state = "live"`, `parent = <previous>`.
   This is the atomic ownership claim.
5. Begin serving. Reads fall through to the parent fork's prefix as
   normal fork reads do.

### `volume stop` is not coordinator shutdown

`volume stop` is an **explicit relinquish of ownership**, not what
happens when the daemon exits. The two paths are very different:

| Event | `names/<name>` | WAL | Snapshot taken | Recovery path |
|---|---|---|---|---|
| `volume stop <name>` | rewritten to `state=stopped` | drained, then discarded | yes — handoff snapshot | another coordinator may `volume start` |
| Coordinator graceful shutdown (SIGTERM, Ctrl-C) | unchanged — still names this coordinator | fsynced, retained on disk | no | this coordinator restarts, sees its own `coordinator_id`, replays WAL, resumes |
| Coordinator crash (SIGKILL, hardware) | unchanged | retained, possibly with unsynced tail | no | same as graceful — restart replays WAL. Unsynced tail is lost (matches the existing crash-recovery contract) |
| `--force-takeover` from elsewhere | rewritten by new coordinator without conditional check | abandoned (the dead coordinator's WAL is unreachable) | no — takeover forks from the previous handoff snapshot | new coordinator serves; any post-snapshot writes from the old owner that didn't reach S3 are lost |

A coordinator that is coming back keeps its volumes and its WAL.
Snapshots happen on explicit `stop` (and on the existing snapshot
cadence), not on every daemon restart. Otherwise every Ctrl-C would
mint N handoff snapshots for N live volumes — slow, wasteful, and
noisy in the audit history.

This matches the shape of `design-ublk-shutdown-park.md`: graceful
exit fsyncs and parks state for the same daemon to resume; deletion
is a separate, explicit verb.

### `--force-takeover`

Used when the previous owner is not coming back (machine gone,
`root_key` deleted, partition with no expected recovery). Skips the
conditional check on `names/<name>` — the new coordinator forks from
the last published snapshot, mints its own ULID and key, and
overwrites the name pointer. Any writes the previous owner accepted
after that snapshot but didn't publish are lost. The operator is
asserting that loss is acceptable.

## Snapshots: user vs handoff

The stop/start protocol works because every `stop` publishes a
snapshot — the receiving fork pins to it as its parent. So
portability does not introduce a new state-transfer mechanism; it
reuses snapshots, and the user just sees `stop` / `start`.

Two kinds of snapshot, one on-disk shape:

- **User snapshots** — minted by an explicit user/coordinator
  action. Pin retention, anchor replicas (`create --from
  <vol_ulid>/<snap_ulid>`), bound catchup windows.
- **Handoff snapshots** — minted automatically by `volume stop` to
  give the next fork a parent pin. Carry extra metadata: the
  releasing `coordinator_id`, hostname, episode `acquired_at`, and
  the snapshot ULID itself. Anchored under the *outgoing* fork's
  prefix.

Decisions for now:

- **Handoff snapshots are retained forever.** They give the volume
  a complete, audit-friendly history of where it has been; until we
  have a concrete reason to GC them, the simpler rule is "keep them
  all".
- **Replica `--from` against a handoff snapshot** is allowed but
  flagged in the snapshot record so tooling can warn or distinguish
  the two when surfacing snapshot lists. There is no correctness
  reason to disallow it — a handoff snapshot is as durable a pin as
  a user one.

## What changes elsewhere

### `by_name/` semantics

`by_name/<name>` is a local symlink today. Different hosts may use
different names for the same `<vol_ulid>` (`project_volume_naming`).
Portability does not change the local symlink shape, but the
*authoritative* mapping is now `names/<name>` in S3, not the local
view. `by_name/` becomes a per-host cache of "names this host knows
about and currently has materialised data for".

`volume list` gains an `owner` column with values like `self`,
`<other-coordinator-id> (host: <hostname>)`, `stopped`,
`stale (last seen <coordinator-id>)`.

### `volume remote` collapses into `volume`

Today the CLI has a separate `volume remote` namespace
(`remote list`, `remote pull`) for operations against volumes that
exist in the bucket but not on this host. With portability,
"remote" stops being a useful axis: every named volume lives at
`names/<name>` in S3, and any of them is a candidate for
`volume start <name>` regardless of whether this coordinator has
ever materialised it locally.

The mapping:

- `volume remote list` → folds into `volume list`, but the default
  stays **local-only**: only volumes this coordinator currently owns
  or has local data for. A flag (e.g. `--all` or `--remote`)
  expands the view to **every** name in `names/`, including ones
  currently held by another coordinator. Eligibility is shown as
  a column, not used to filter — operators want visibility into
  "host-B currently holds this name" for diagnostics, even though
  they couldn't `start` it without `--force-takeover`. Columns
  apply to both views: `local data: yes/no`, `owner`, `state`, and
  `eligible: yes/no`.
- `volume remote pull` → no longer a distinct verb. `volume start
  <name>` is sufficient: it forks from the most recent published
  snapshot, fetches what it needs lazily through the existing
  demand-fetch path, and warms the local cache as reads happen.
  Operators who want eager hydration can compose with `volume
  materialize` (already in the replica model) — orthogonal to
  start.
- `volume create --from <vol_ulid>/<snap_ulid>` is unchanged: the
  way to fork a *new logical volume* off a snapshot stays distinct
  from claiming an existing name.

What's left as a real distinction is **eligibility**, not
locality:

- `state = stopped` → eligible to start.
- `state = live, coordinator_id = self` → already running here.
- `state = live, coordinator_id = other` → not eligible without
  `--force-takeover`.

`volume list` surfaces this directly. There is no second namespace
to learn.

### Replica model

Portability is **orthogonal** to the replica model
(`design-replica-model.md`). `volume create --from
<vol_ulid>/<snap_ulid>` remains the way to *fork off* a logically
independent volume (new name, new identity, divergence). `volume
stop` + `volume start` is how to *relocate* a name (same name, fresh
fork inheriting from the previous tail). A user who wants to diverge
*and* relocate does each step explicitly.

The two share the same on-disk fork machinery; only the lifecycle
shape (which name they bind to) differs.

### Per-host coordinator model

Coordinators do not coordinate with each other
(`project_coordinator_per_host`). Each host's coordinator scans its
local `by_id/`. With portability, a coordinator that observes a
local `by_id/<vol_ulid>/` directory whose entry in `names/<name>` is
held by a different `coordinator_id` must not start serving it. The
local directory becomes a cache of S3 state — possibly the
parent-fork cache for a future re-acquisition by this coordinator,
or a stale leftover.

### Chain length

Long-lived volumes with frequent migrations grow long fork chains.
Read path traversal scales with chain depth. Two existing primitives
already handle this:

- **Handoff snapshots flatten the read path *within* a fork** — a
  fresh fork starts by reading at the parent's snapshot, not by
  walking arbitrarily deep.
- **`materialize` flattens the chain itself** — periodic compaction
  is a normal operation, copying everything reachable from the
  current state into a fresh root fork (new ULID, no parent).
  `materialize` already exists in `design-replica-model.md` for
  exactly this shape.

A volume that migrates daily for a year produces a 365-link chain,
but the read cost is bounded by the most recent handoff snapshot;
`materialize` on demand collapses the chain when the operator (or
automation) decides the chain has grown unwieldy.

## Open questions

1. **Naming clarity.** "Live" today means "host-pinned and serving".
   With portability, "live" becomes a property of *the name* (which
   coordinator currently holds it), not of an individual ULID.
   Worth picking the vocabulary deliberately before exposing it in
   CLI output.
2. **`names/<name>` write authority.** Today `names/` is written at
   import. With portability it becomes the most contentious object
   in the bucket. Bucket IAM is what gates who can write to it — we
   need to confirm the credential model already gives each
   coordinator write access scoped to `names/` (or design it in).
3. **GC across a fork chain.** Deduplication and the lowest-ULID-
   wins extent index policy (`project_lowest_ulid_canonical`) already
   work across fork ancestry. But a chain that gets long *because of
   migrations* is a new traffic pattern; worth a focused look before
   committing to portability as a default.

## Why we might not want this

- **`names/` becomes load-bearing.** Today it is a convenience
  index; with portability it is the source of truth for ownership.
  If the credential model doesn't already cleanly grant
  per-coordinator write scope on `names/`, this proposal pulls that
  into scope.
- **Chains accumulate.** A volume that migrates frequently produces
  a long fork chain. The mitigations exist (`materialize`,
  handoff-snapshot read path), but operating a long-chain volume is
  more demanding than operating a single-fork volume — there is more
  to monitor, more decisions about when to flatten.
- **The fork-as-relocation pattern works today.** Verbose but
  honest: every relocation produces a new ULID with provenance,
  which is auditable. Portability is conceptual cleanup over an
  already-working escape hatch — worth doing if and only if the
  cleanup pays for the new operational surface.
