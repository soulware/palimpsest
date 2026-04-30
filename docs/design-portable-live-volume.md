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
state = "live"            # or "stopped" or "released"
parent = "<prev_ulid>/<prev_snap_ulid>"   # absent on the root
claimed_at = "<rfc3339>"
hostname = "<owner-host-at-claim-time>"  # advisory only
```

### Five states, three intents

The lifecycle distinguishes:

1. "I want this volume's process down for a bit" (host maintenance,
   daemon restart) from "I'm done; someone else can have this name".
   A coordinator that goes into maintenance and writes
   `state=stopped` should not be racing other coordinators for its
   own volume on the way back up.
2. "Anyone may claim this" (open release) from "this is being
   handed to a specific coordinator" (targeted release). The latter
   removes the post-release race window where a third coordinator
   could grab the name.
3. **A name that *can* have an exclusive owner** (a writable volume
   — only one coordinator may serve it at a time) from **a name
   that points at immutable content** (an import — multiple
   coordinators may serve it concurrently with no coordination,
   because there is nothing to coordinate).

Five states make these intents explicit:

| State | `coordinator_id` | `claimed_at` / `hostname` | Mutable? | Who can claim |
|---|---|---|---|---|
| `live` | current owner | populated | yes | already running here; others need `release --force` first |
| `stopped` | current owner | populated | yes | this coordinator (local resume); others need `release --force` first |
| `released` | empty | empty | yes (after claim) | any coordinator |
| `reserved` | intended claimer | empty | yes (after claim) | only the named coordinator |
| `readonly` | empty | empty | **no** | n/a (no daemon, multiple readers OK) |

Each state has a coherent field shape: `coordinator_id` consistently
means "the coordinator this record is associated with"
(current owner on live/stopped, intended claimer on reserved, no
one on released/readonly); `claimed_at` and `hostname` consistently
mean "when/where the *current* owner claimed" — populated only when
there is a current owner.

The first four are the writable lifecycle. `readonly` is the
published-handle case: the name is bound permanently to immutable
content, no exclusive owner is needed, and lifecycle verbs
(`stop` / `release` / `start`) all refuse it cleanly. See §
"Readonly names" below.

Verbs and their state transitions:

- **`volume stop`** — local stop, retain ownership. `live → stopped`.
  WAL is fsynced; daemon halts; `state` flips to `stopped` via
  conditional PUT. **No handoff snapshot.** Other coordinators are
  refused.
- **`volume release`** — relinquish ownership. `live → released` or
  `stopped → released`. Drains WAL → publishes handoff snapshot →
  flips `state` to `released` via conditional PUT, clearing
  `coordinator_id`, `claimed_at`, and `hostname`. Any coordinator
  may now `start`. Refuses on foreign ownership unless `--force` is
  passed.
- **`volume release --to <coordinator_id>`** — targeted handoff.
  Same drain + handoff-snapshot path as `release`, but flips state
  to `reserved` and writes `coordinator_id = <X>` (the intended
  claimer). `claimed_at` and `hostname` stay empty — X has not
  claimed yet. Only X may `start --remote` against the resulting
  record; other coordinators are refused before the conditional
  PUT. Closes the post-release race window: there is no period where
  the name is openly claimable by anyone other than X.
- **`volume release --force`** — release a name held by **another
  coordinator**. The override path for "the previous owner is gone
  and not coming back". Skips the `If-Match` precondition on the
  `names/<name>` PUT and does **not** drain the previous owner's WAL
  (it isn't reachable). *(Proposed)* the recovering coordinator
  synthesises a fresh handoff snapshot from segments observable in
  S3, signed by its own `coordinator.key`. The data-loss boundary is
  "writes the dead owner accepted but never promoted to S3" — same
  as the crash-recovery contract. After `release --force`, a normal
  `volume start --remote <name>` claims the now-released name via
  the conditional-PUT path; concurrent claimers race cleanly. See
  the dedicated section below. The previous-owner side (split-brain
  safety when `--force` is run against a live coordinator) is
  covered in
  [`design-force-release-fencing.md`](design-force-release-fencing.md).
- **`volume release --force --to <coordinator_id>`** — composes the
  two: unconditional override of foreign ownership *and* targeted
  reservation in a single PUT. The post-force record is `reserved`,
  not `released`; only X may claim. Useful for "the previous host
  is gone, hand the volume directly to a known recipient" without
  a race window.
- **`volume stop --release`** — convenience for `stop` then
  `release` in one verb. `--release` accepts the same `--to` and
  `--force` flags as standalone `release`.
- **`volume start <name>`** — claim. Defaults to **local-only**:
  bare `start` may resume a `stopped` fork this coordinator owns,
  but never changes the bucket-side ownership record. It refuses
  in two cases that both require `--remote`:
  (a) `<name>` has no local state (no `by_name/<name>` symlink),
  (b) the bucket-side record is `released` (even if a stale local
  fork from a previous life is still on disk — re-acquiring a name
  the operator handed back to the pool is an explicit decision).
  **`--remote`** opts into the S3 claim path. Two sub-cases route
  internally: in-place reclaim when the local fork's ULID matches
  the released ULID (cheap, no pull, same fork kept) vs cross-
  coordinator claim otherwise (pull only the delta since the last
  local ancestor, mint a fresh fork, atomically rebind). Any prior
  local fork stays on disk and serves as ancestor cache where
  applicable. `volume start` never overrides another coordinator's
  ownership — recovery of a name held by an unreachable peer is
  `volume release --force` first, then `volume start --remote`.
- **Coordinator graceful shutdown / crash** — does not change
  `state`. A coordinator coming back up sees its own
  `coordinator_id` in `live` or `stopped` records and resumes; no
  `release` happens implicitly.

All transitions are conditional PUTs (`If-Match` on ETag, supported
by S3 and Tigris). Conditional-write atomicity on this single object
is the entire ownership protocol.

### Readonly names

A name in `state=readonly` points at immutable content (today: an
imported OCI image). The on-wire shape:

```toml
version = 1
vol_ulid = "<import_fork_ulid>"
state = "readonly"
```

`coordinator_id`, `claimed_at`, and `hostname` are all empty —
**there is no exclusive owner, by design**. Multiple coordinators
may pull and serve the same readonly name concurrently with no
coordination, because the underlying content cannot diverge.

Properties:

- **Conditional create still gives uniqueness.** `mark_initial`
  uses `If-None-Match: *` on creation, so two coordinators racing
  to import the same name resolve cleanly: one writes the record,
  the other sees `AlreadyExists` and refuses. The name is
  exclusively bound to one immutable artefact, even though no
  coordinator owns it.
- **Lifecycle verbs refuse readonly cleanly.** `mark_stopped`,
  `mark_released`, `mark_live`, and `mark_claimed` all return
  `InvalidTransition` (or `NotReleased`, for `mark_claimed`) on
  observing `state=readonly`. The local IPC layer also short-
  circuits earlier when it sees a `volume.readonly` marker on
  disk, so these are defence-in-depth rather than the primary
  refusal point.
- **No handoff snapshot.** A readonly name is its own handoff: any
  coordinator may pull `by_id/<vol_ulid>/` from the bucket and
  serve the name without touching `names/<name>`.
- **Re-import semantics:** rejected today (any existing record
  causes `AlreadyExists`). A future refinement could allow
  idempotent re-import when the supplied `vol_ulid` matches the
  existing one; out of scope for the initial landing.

When a writable volume might want a readonly handle (e.g. publish
a snapshot as a stable tag), the same state can carry that intent
— it is not import-specific. The writable lifecycle (`Live` →
`Stopped` ↔ `Released`) and the published-handle lifecycle
(`Readonly`, terminal) are intentionally separate so a name
cannot accidentally cross between them.

### Object store requirements

The whole proposal rests on two primitives, applied only to
`names/<name>`:

1. **Strong read-after-write consistency** on individual keys.
2. **Conditional writes** — `If-None-Match: *` for create-if-absent,
   `If-Match: <etag>` for compare-and-swap on existing keys.

Combined, these give a global, linearizable, single-key
compare-and-swap. That one primitive carries name uniqueness,
ownership transfer, and the explicit-skip semantics of
`volume release --force`. Everything else in the system is append-only or
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

### Coordinator identity *(Proposed)*

A coordinator's identity is rooted in a single Ed25519 keypair held
on the coordinator host. Mirroring the per-volume key convention:

- `<data_dir>/coordinator.key` — Ed25519 private key (32-byte seed),
  mode 0600. Generated on first start if absent. Never leaves the
  host.
- `<data_dir>/coordinator.pub` — Ed25519 public key. Mirrored to S3
  at `coordinators/<coordinator_id>/coordinator.pub` on coordinator
  startup so any other coordinator can verify signatures by
  `coordinator_id` lookup.

Everything else — `coordinator_id`, the macaroon MAC root — derives
from the keypair via domain-separated `blake3::derive_key`. There is
no separate `coordinator.root_key` file; that secret is derived
in-memory at startup.

```rust
// coordinator_id is a stable function of the *public* key:
// self-authenticating against the published coordinator.pub.
let coordinator_id = blake3::derive_key(
    "elide coordinator-id v1",
    coord_pub.as_bytes(),
);

// macaroon MAC root is derived in-memory from the *private* key
// at startup; never written to disk.
let macaroon_root = blake3::derive_key(
    "elide macaroon-root v1",
    coord_priv.as_bytes(),
);
```

Properties:

- **Self-authenticating identity.** Anyone fetching
  `coordinators/<coordinator_id>/coordinator.pub` can recompute
  `derive_key("elide coordinator-id v1", pub) == coordinator_id` and
  refuse a record where the pub doesn't match the id. The binding
  between id and pubkey is intrinsic; a bucket-writer cannot pose
  as `coordinator_id = X` while publishing a pubkey they actually
  control.
- **Single on-disk secret.** `coordinator.key` is the sole root —
  one artefact to protect, one to back up, one to lose.
- **Domain-separated reuse.** Sharing seed material between Ed25519
  signing and BLAKE3 MAC is safe because the derivation contexts
  are distinct; standard cryptographic hygiene.
- **No independent rotation, by design.** The signing key, the
  macaroon root, and `coordinator_id` rotate together (which is to
  say: they don't rotate today). Rotation would require a
  generational suffix and a retention story for old keys; defer
  until a concrete need appears.

**Operational consequence:** deleting `coordinator.key` (or losing
the data dir) ends that coordinator's identity. A new keypair gives
a new `coordinator_id`. Volumes whose `names/<name>` pointer names
the old coordinator can only be reclaimed via
`volume release --force`. This is the right behaviour and doubles
as an explicit escape hatch for a misbehaving coordinator — delete
the keypair, restart, then `release --force` + `start --remote`
from a clean identity. The old key's macaroons become unverifiable
at the same moment, so clients re-auth anyway.

**Trust model:** the `coordinators/<coordinator_id>/coordinator.pub`
write is gated by S3 write ACL — the same boundary that already
gates `release --force` itself. Verification of a synthesised
handoff snapshot: read the snapshot, look up
`coordinators/<recovering_coordinator_id>/coordinator.pub`, verify
the Ed25519 signature, recompute `coordinator_id` from the pub and
confirm it matches the path. A malicious party with bucket-write
could publish their own coordinator pubkey, but at that point they
already have full bucket-mutation authority and the trust story is
the bucket's, not Elide's.

**Migration from existing deployments.** Previous versions stored a
symmetric `coordinator.root_key` on disk and derived
`coordinator_id` from it. Per the project's "no backward compat by
default" stance, the upgrade path is a clean break: on first start
of new code, the coordinator generates `coordinator.key` /
`coordinator.pub` if absent, derives a new `coordinator_id` from
the new pub, and stops trusting any existing `coordinator.root_key`
file. Volumes whose `names/<name>` pointed at the old coord_id
recover via `volume release --force`. Outstanding macaroons issued
under the old root are invalidated; clients re-auth.

Future use cases for `coordinator.key` are likely (signed lifecycle
events, coordinator-attested operations) but the initial scope is
just synthesised handoff snapshots.

## Flows

### `volume stop <name>` — local stop, retain ownership

1. Refuse if a client (NBD/ublk) is connected.
2. Issue the existing `shutdown` RPC: WAL fsync, daemon halts.
3. Conditional PUT to `names/<name>` flipping `state` from `live`
   to `stopped`. `vol_ulid` and `coordinator_id` unchanged.
4. Local artefacts (cache, index, WAL, signing key) **stay in
   place**. The same coordinator can `volume start` later and
   resume from local state.

No handoff snapshot, no fork. The volume is reserved for this
coordinator. Other coordinators are refused (recovery requires
`volume release --force` from another host, then a normal
`volume start --remote`).

### `volume release <name>` — relinquish ownership

1. If currently `live`, do everything `volume stop` does first
   (refuse if client connected, shutdown).
2. Drain WAL: promote pending records into segments, finish
   in-flight uploads.
3. Publish a handoff snapshot covering everything published.
4. Conditional PUT to `names/<name>` setting `state = "released"`,
   keeping `vol_ulid` pointing at the now-frozen fork and recording
   the handoff snapshot. The owner-identity fields
   (`coordinator_id`, `claimed_at`, `hostname`) are **cleared** so
   the populated fields agree with the state: `Released` means
   "no current owner". The next claimant repopulates them via
   `mark_claimed`.
5. The local `by_id/<vol_ulid>/` directory may be discarded (it's
   reproducible from S3) or kept as cache for fast reacquisition.
   **Never** keep the WAL — there is none past the published
   snapshot, by construction.

`volume stop --release` is the convenience composition.

### `volume start <name>` — claim ownership

`volume start` is always safe: it never overrides another
coordinator's ownership. Defaults to **local-only**, where
"local-only" means *no bucket-side state change*. It will not
reach into S3 unless `--remote` is passed. Defaulting local avoids
surprising network pulls; refusing to override foreign ownership
forces operators through the explicit two-step recovery flow
(`volume release --force` + `volume start --remote`).

1. **Bare `start` (local-only).** The coordinator reads
   `names/<name>` to verify ownership but performs no bucket-side
   write. Allowed:
   - already running here (`state=live`, `coordinator_id=self`) —
     idempotent no-op,
   - local resume (`state=stopped`, `coordinator_id=self`) — reuse
     the existing fork; flip `state` back to `live` via conditional
     PUT; restart the daemon. No new ULID, no snapshot, no fork.
     (This *is* a bucket write — a CAS that flips
     `stopped → live` keeping ownership; "local-only" describes
     the *ownership* surface, not whether the record is touched.)

   Refused (each routed to `--remote`):
   - `<name>` has no local state — refuse with:
     ```
     error: volume 'mydb' not found locally.
       to claim it from the bucket, run: elide volume start --remote mydb
     ```
   - `state == "released"` (even when a stale local fork exists
     from a previous life this host owned) — refuse with:
     ```
     error: name 'mydb' is Released; reclaim with: elide volume start --remote mydb
     ```
     Reclaiming a name handed back to the pool is an explicit
     operator decision; bare `start` never silently flips the
     bucket-side ownership.

2. **`--remote` (claim).** Read `names/<name>`.
   - `state == "released"` → claim allowed for any coordinator.
     Two sub-cases route automatically:
     - **In-place reclaim** when the released `vol_ulid` matches a
       local fork this host still has: `mark_reclaimed_local` flips
       `released → live` keeping the same ULID. No pull, no fork
       mint. Reported as `<name>: reclaimed and started`.
     - **Cross-coordinator claim** otherwise: pull from the released
       ancestor, mint a fresh `<new_ulid>`, generate a fresh
       Ed25519 keypair, create `by_id/<new_ulid>/` with provenance
       pointing at the previous fork's
       `<vol_ulid>/<handoff_snap_ulid>`, publish `volume.pub` and
       signed provenance. Conditional PUT to `names/<name>`:
       `vol_ulid = <new_ulid>`, `coordinator_id = self`,
       `state = "live"`, `parent = <previous>`. Begin serving.
       Reported as `<name>: claimed and started`. Any prior local
       fork stays on disk and is reused as ancestor cache by the
       chain walk in `remote_pull` if it's part of the claimed
       chain (so the network fetch only covers the delta since
       the fork point).
   - `state == "reserved"` → claim allowed only when
     `coordinator_id == self`. Otherwise refuse with a clear error
     naming the intended claimer.
   - Anything else → refuse with a pointer at
     `volume release --force <name>` (followed by another
     `start --remote`) for the unreachable-owner recovery path.

One verb, two intents made explicit through a single flag: bare =
no ownership change, `--remote` = (re)claim a name from the
bucket. The override path lives on `volume release --force`, not
here.

### `volume stop` and `volume release` are not coordinator shutdown

The lifecycle verbs are explicit operator intent. Coordinator
process exit (graceful or crash) is something else entirely:

| Event | `names/<name>` | WAL | Snapshot taken | Recovery path |
|---|---|---|---|---|
| `volume stop <name>` | flipped to `state=stopped`, same `coordinator_id` | fsynced, retained on disk | no | this coordinator restarts and `volume start`s; other coordinators refused |
| `volume release <name>` | flipped to `state=released`; `coordinator_id`, `claimed_at`, `hostname` cleared | drained, then discarded | yes — handoff snapshot | any coordinator may `volume start --remote` |
| `volume release --to <X> <name>` | flipped to `state=reserved`; `coordinator_id=<X>`; `claimed_at`/`hostname` cleared | drained, then discarded | yes — handoff snapshot | only `<X>` may `volume start --remote`; others refused |
| Coordinator graceful shutdown (SIGTERM, Ctrl-C) | unchanged | fsynced, retained on disk | no | this coordinator restarts, sees its own `coordinator_id`, replays WAL, resumes serving — `state` was never flipped to `stopped`, so volumes that were `live` come back `live` |
| Coordinator crash (SIGKILL, hardware) | unchanged | retained, possibly with unsynced tail | no | same as graceful — restart replays WAL. Unsynced tail is lost (matches the existing crash-recovery contract) |
| `volume release --force` from elsewhere | rewritten without conditional check; flipped to `released`, identity cleared | abandoned (the dead coordinator's WAL is unreachable) | yes — synthesised handoff snapshot covering all S3-visible signature-valid segments, signed by the recovering coordinator's `coordinator.key` *(Proposed)* | a subsequent `volume start --remote` claims it normally; any writes the old owner accepted but never promoted to S3 are lost |

A coordinator that is coming back keeps its volumes, its WAL, and
its `state=live` records. The only implicit transitions are
operator-driven (`stop`, `release`, `start`) or operator-explicit
(`release --force`). Daemon lifecycle does not move state.

This matches the shape of `design-ublk-shutdown-park.md`: graceful
exit fsyncs and parks state for the same daemon to resume; deletion
is a separate, explicit verb.

### `volume release --force`

Used when the previous owner is not coming back (machine gone,
`coordinator.key` deleted, partition with no expected recovery).
Skips the
"who owns this name" check on `volume release` — the unconditional
PUT proceeds even when the record says `live` or `stopped` and
`coordinator_id != self`. No drain happens (the dead owner's WAL is
unreachable).

**Proposed:** the handoff snapshot is **synthesised at force-release
time from the segments observable in S3**, not pinned to the previous
fork's last published handoff snapshot. The recovering coordinator B:

1. Lists `by_id/<dead_vol_ulid>/segments/` (or the date-sharded
   layout under that prefix).
2. Fetches each segment header + index section (cheap; not the body).
3. Verifies each segment's Ed25519 signature against the dead fork's
   `volume.pub` from S3. Segments that fail verification (partial
   uploads, torn objects) are dropped.
4. Mints a synthesised handoff snapshot at
   `by_id/<dead_vol_ulid>/snapshots/<new_snap_ulid>` naming the
   verified segment set, with metadata fields:
   - `synthesised_from_recovery = true`
   - `recovering_coordinator_id = <B>`
   - `recovered_at = <timestamp>`
5. Signs the synthesised snapshot with B's **coordinator signing
   key** (`coordinator.key`; see "Coordinator signing key" below).
6. Unconditional PUT to `names/<name>` flipping to `released` (or
   `reserved` with `--to`), recording the synthesised snapshot ULID.

The data-loss boundary is "writes that were fsync'd locally on the
dead owner but never made it to S3" — identical to the crash-
recovery contract elsewhere. Strictly better than pinning to the
last handoff snapshot. Works equally well when the dead owner never
published a snapshot at all, because segments are self-describing
(see `docs/overview.md`: "the manifest is always derivable from the
segments"). The snapshot manifest is an optimisation, not a
correctness requirement.

After `release --force`, the name is in the normal `released` state
and the next claimant — including this same coordinator — runs
`volume start --remote <name>` through the standard conditional-PUT
path. The claimant verifies the synthesised handoff snapshot using
B's published coordinator pubkey before forking from it. Concurrent
`start --remote` callers race cleanly through `If-Match` on the
released etag; the loser sees a clean error.

Why split it into two verbs (`release --force` then `start --remote`)
instead of folding the override into `start`?

- **`volume start` stays always-safe**: it never overrides another
  coordinator. Bare `start` is local-only; `start --remote` only
  claims `released` names. No flag combination on `start` can
  override a foreign owner.
- **The dangerous step is auditable on its own**: the operator
  explicitly invokes `release --force` and explicitly accepts the
  data-loss tradeoff. The subsequent `start --remote` is a normal
  claim that any coordinator could perform — there's nothing
  privileged about being the host that did the `release --force`.
- **Two coordinators can race the post-`release --force` claim**:
  if both observe the now-`released` record at roughly the same
  time, conditional PUT picks one. That's the standard claim path,
  not a special override path.

## Snapshots: user vs handoff

The release/start protocol works because every `volume release`
publishes a snapshot — the next fork pins to it as its parent. So
portability does not introduce a new state-transfer mechanism; it
reuses snapshots, and the user just sees `release` / `start`.

Two kinds of snapshot, one on-disk shape:

- **User snapshots** — minted by an explicit user/coordinator
  action. Pin retention, anchor replicas (`create --from
  <vol_ulid>/<snap_ulid>`), bound catchup windows.
- **Handoff snapshots** — minted automatically by `volume release`
  to give the next fork a parent pin. Carry extra metadata: the
  releasing `coordinator_id`, hostname, episode `claimed_at`, and
  the snapshot ULID itself. Anchored under the *outgoing* fork's
  prefix. **`volume stop` does not mint a handoff snapshot** — it
  retains ownership locally, no fork is needed.

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

`volume list` stays a local-only view — it does not reach S3.
Per-name authoritative state lives in `names/<name>` and is
queried via `volume status --remote <name>` (see below).

### `volume remote` goes away

Today the CLI has a separate `volume remote` namespace
(`remote list`, `remote pull`) for operations against volumes that
exist in the bucket but not on this host. With portability,
"remote" stops being a useful axis: every named volume lives at
`names/<name>` in S3, and any of them is a candidate for
`volume start <name>` regardless of whether this coordinator has
ever materialised it locally.

The mapping:

- `volume remote list` → **removed**, not folded in. Listing every
  name in `names/` does not scale (a bucket may hold thousands of
  named volumes), and an unbounded enumeration is not a useful
  default. `volume list` stays strictly local: names this
  coordinator currently owns or has local data for. The `--all`
  flag keeps its existing local-only meaning (include ancestor
  forks); it does **not** reach S3.
- `volume status <name>` is the per-name query. The default is
  local: it reports what this coordinator knows about `<name>` from
  its own state. With `--remote`, it fetches `names/<name>` from S3
  and reports the authoritative record (`vol_ulid`, `state`,
  `coordinator_id`, `hostname`, `claimed_at`, eligibility for this
  coordinator). The user must already know the name — there is no
  discovery path through the CLI.
- `volume remote pull` → removed. `volume start <name>` is
  sufficient: it forks from the most recent published snapshot,
  fetches what it needs lazily through the existing demand-fetch
  path, and warms the local cache as reads happen. Operators who
  want eager hydration can compose with `volume materialize`
  (already in the replica model) — orthogonal to start.
- `volume create --from <vol_ulid>/<snap_ulid>` is unchanged: the
  way to fork a *new logical volume* off a snapshot stays distinct
  from claiming an existing name.

What's left as a real distinction is **eligibility**, not
locality:

- `state = released` → eligible to start (any coordinator).
- `state = stopped, coordinator_id = self` → eligible to start
  (local resume).
- `state = live, coordinator_id = self` → already running here.
- `state ∈ {live, stopped}, coordinator_id = other` → not eligible;
  recover via `volume release --force` followed by
  `volume start --remote`.

`volume status --remote <name>` surfaces this for a single name.
There is no second namespace to learn, and no unbounded listing.

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
