# Replica model

**Status:** Proposed framing; landed in stages.

- **Landed now:** `volume fork` is retired. The equivalent is `volume create
  <name> --from <source>` — the two commands were already semantically
  identical, so this is a pure cleanup.
- **Deferred:** Tightening `--from` to require an explicit
  `<vol_ulid>/<snap_ulid>` pin, and introducing `volume materialize`, both
  wait on TTL specifics for the object store (see below). The existing
  `--force-snapshot` flag on `create --from` stays in place as the
  interim mechanism for branching past the most recent snapshot.

## The framing

Every volume is a replica of some upstream, or is a root. "Consumer" and
"owner" are not asymmetric roles — both are just writers of their own S3
prefix under `by_id/<vol_ulid>/`. The optional relationship "I'm following
upstream X" is a property of the replica, not of the upstream. An upstream
volume owes its replicas nothing; replicas either keep up with the upstream's
retention or they fall off.

A **fork** in this model is a replica that stopped following and started
writing its own tail. A **snapshot** is a checkpoint used by either role —
it pins retention below that point on the upstream, and it bounds the
catchup window above that point for any replica trying to reach current
state.

## Two ways to materialise a replica

| Mode | CLI | Cost | Dependency on upstream |
|---|---|---|---|
| **Cheap reference** | `volume create <name> --from <vol_ulid>/<snap_ulid>` | Fast: pull `.idx` files, start reading. Segment bodies demand-fetched lazily. | Ongoing. Reads traverse upstream's S3 prefix via the ancestor chain. Upstream deletion or ACL change breaks the replica. |
| **Materialising detach** | `volume materialize <name> --from <vol_ulid>` | Expensive: copy every segment the source's current state needs into the new volume's own prefix. | None after completion. The new volume is a root with no parent. |

`create --from` is the git-branch shape. `materialize` is the git-clone
shape. Both produce writable volumes.

## Why `create --from` requires an explicit `<vol_ulid>/<snap_ulid>` pin

A replica of the upstream at snapshot Sn is a well-defined object: all
segments ≤ Sn are pinned by the upstream's own GC floor (`latest_snapshot`
on the upstream acts as the retention barrier), so the cheap-reference path
is durable without any coordination.

Bare-volume-ULID or bare-name references, and the old `--force-snapshot`
flag, were ways to say "branch from *now*" — the current state past the
upstream's most recent snapshot. That state is inherently unpinned and the
replica would be racing the upstream's GC for every read. There is no
correct cheap-reference form of "now": if the caller wants current state
self-contained, they want `materialize`. If they want a pinned point, they
need a snapshot to pin to.

Forcing `<vol_ulid>/<snap_ulid>` at the CLI reflects this: every
cheap-reference replica is anchored at a durable pin that both sides agree
on.

## Why `materialize` does not take a snapshot pin

Materialize is inherently a "now" operation. Its purpose is to capture the
upstream's *current* state — including the post-snapshot tail the upstream
has written since `latest_snapshot` — into a self-contained new volume.

If the caller wants a pinned historical state self-contained, they can
`create --from <vol>/<snap>` and then materialize from that replica. The
flattening is orthogonal to which state is captured.

Internally materialize decomposes:

1. Flatten the base ≤ `latest_snapshot(upstream)` — copy all segments into
   the new volume's own `segments/` prefix. No TTL pressure: the upstream's
   own GC floor pins these.
2. Race-copy the tail > `latest_snapshot(upstream)` — the segments written
   since the last snapshot. These are the only ones the upstream's GC is
   free to delete, so this portion is TTL-bounded.

The post-snapshot tail is also the part the upstream is *most* likely to
GC (sweep/repack preferentially target small recent segments), so the race
is real. Its practical tolerance depends on the object store's retention
behaviour on delete (see below) and the upstream's snapshot cadence.

## Snapshot cadence as a retention SLA

Under this model, snapshot cadence on the upstream becomes a first-class
operational knob. Tight cadence → small tail → small materialisation race
window, small catchup cost for replicas. No snapshots at all (the "dead
host, never snapshotted" degenerate case) → tail equals the whole volume.

This gives upstreams a concrete policy: "tail ≤ X GB at any moment"
translates to "any replica can catch up within a bounded download." It's
also a defence against the dead-host case: an upstream that auto-snapshots
at any cadence can never be in the "no snapshots" state.

## Live replicas

A replica that keeps polling the upstream's post-snapshot segments is
structurally identical to a materialise-once operation run on a loop. From
the replica's perspective, the upstream's post-snapshot tail is a
write-ahead log; upstream compactions are log compactions; the replica
replays either originals or compaction outputs, whichever is available.

Fork-from-remote, live replication, and cold recovery collapse into the
same mechanism separated only by polling frequency:

| Role | Polling | TTL dependency |
|---|---|---|
| Live replica | Continuous | None (fresh listing each poll gives fresh closure) |
| Cold recovery from recent snapshot | One-shot | Tail is small; TTL covers it easily |
| Cold recovery with no snapshot (PR #98 case) | One-shot | Tail ≈ whole volume; TTL is load-bearing |

The first two modes don't need TTL at all. TTL is only load-bearing when a
replica is catching up from farther back than the upstream's current
working set.

## What TTL buys us and what is still open

The object store's deletion semantics determine what happens in the race
window between an upstream GC deleting a segment and a replica reading it.
The clean model is:

- A deletion creates a tombstone with a bounded retention window T.
- Within T, `GET key` still returns bytes.
- After T, the object is unreachable.
- Listings can optionally expose tombstones (for replica-side race
  detection) or hide them (for upstream-side cleanliness).

This needs to be pinned down against Tigris's actual behaviour before the
`materialize` implementation can commit to error semantics. Open questions:

- Is deletion soft (object-lock / lifecycle-delay) or hard (immediate),
  configurable per-bucket?
- Does `GET` on a soft-deleted object succeed, or does the replica need
  version-aware reads? If version-aware, the manifest needs to record
  version IDs.
- What T is available, and is it configurable?
- How does this interact with S3 versioning semantics on Tigris
  specifically?

Until those are answered, `materialize` is a designed but unimplemented
command. PR #98/#99's `--force-snapshot` flag and its supporting
machinery (`create_readonly_snapshot_now`, the attestation keypair, the
`ParentRef.manifest_pubkey` field) are left in place as the interim
mechanism for the "branch past the latest snapshot" case. The intent is
to retire all of that when `materialize` lands, but not before — the
alternative is a shipped binary that can't cover the dead-host recovery
case at all.

## Proposed: Tigris snapshots as the materialize read source

Tigris exposes point-in-time bucket snapshots as a native primitive: a
snapshot is O(1) to take, references (not copies) existing object
versions, and is readable from the live bucket by passing the returned
`X-Tigris-Snapshot-Version` header on GETs. This is a candidate stable
read source for the `materialize` implementation — it eliminates the
upstream-GC race for the duration of the operation without requiring a
scratch bucket or a writable fork.

Rough shape:

1. Take a snapshot of the source bucket at the start of materialize.
2. Read all source extents through the snapshot (version-pinned GETs).
3. Re-emit new segments, owned solely by the target volume, into the
   live bucket.
4. Release the snapshot on completion.

Open questions / things to experiment with:

- Does the Rust S3 SDK we use permit injecting the
  `X-Tigris-Snapshot-Version` header per-request cleanly, or does it
  require a custom signer / middleware layer?
- Snapshot scope: bucket-wide only, or can it be scoped to a prefix?
  Cost and retention implications if it has to be bucket-wide.
- Is an intra-Tigris `CopyObject` from a snapshot-pinned source key to
  a live-bucket destination key metadata-only (reference bump) or a
  full byte copy? If the former, whole-segment re-parenting is
  effectively free at the storage layer and changes the shape of
  materialize from "re-emit segments" to "retarget object pointers".
- How does snapshot release interact with segments that have been
  copied out into the live bucket under new keys — is there any
  reference-count coupling we need to understand?

This is Tigris-specific. A plain-S3 `materialize` path is still
required and would rely on either (a) the coordinator pinning segment
deletes for the duration of the operation, or (b) S3 object-versioning
semantics where the backend supports them. The object-store trait
should keep snapshot-based and pin-based strategies behind the same
interface so the choice is per-backend and doesn't leak into the
materialize driver.

## Proposed: historical materialize via Tigris versioning

Tigris's always-on per-object versioning plus explicit bucket snapshots
give two tiers of time-travel read:

- **Bucket snapshots** — explicit, named, O(1), coarse granularity
  (whatever cadence we run them at). Manifest and segments are captured
  coherently, so a snapshot is a single-read-view of a volume's past
  state.
- **Per-object versions** — implicit and always-on (append-only data
  model); every write creates a new version, enumerable via
  `ListObjectVersions` + `versionId`. Fine granularity.

Together these make a stronger form of `materialize` possible:
`volume materialize <new> --from <vol>@T` where T is any retained
historical point, not just the source's current state. Host B can
resurrect host A's volume as it existed at T even if host A has since
mutated, GC'd, or deleted the underlying segments.

Implications:

- **Implicit backup.** Tigris retention policy becomes RPO. Snapshot
  cadence becomes recovery granularity. No separate backup system
  required for Tigris-backed deployments.
- **Forensic use.** Cherry-pick a single historical manifest or segment
  into the live bucket without restoring an entire volume.
- **Cross-host DR.** "Host A's bucket is in a bad state" becomes
  "materialize host A's last good snapshot onto host B" — one-shot
  read-through-snapshot.

Open questions:

- Retention cost. Non-current versions and snapshots consume storage;
  "keep a year of history" has a real bill. Lifecycle policy has to be
  explicit and operator-visible.
- Addressing. `<vol>@<snapshot-name>` is easy; arbitrary wall-clock T
  between snapshots requires navigating `versionId` by timestamp — need
  to decide what forms the CLI accepts.
- Whose retention pins whose segments when volumes share history.
- Plain-S3 deployments get none of this unless we build it, which we
  aren't. Worth deciding whether historical materialize is a
  Tigris-deployment feature or an aspired capability of the trait.

## Relationship to existing CLI

**Landed:**

- `volume fork` is retired. It was already semantically identical to
  `volume create --from`, so this is a pure CLI cleanup.
- `volume create` without `--from` stays as-is: a fresh empty volume.
- `volume create --from <source>` continues to accept the same three forms
  it accepts today — a volume name, a bare volume ULID, or an explicit
  `<vol_ulid>/<snap_ulid>` pin — and `--force-snapshot` stays in place.

**Pending (blocked on TTL resolution):**

- Tighten `--from` to require the explicit `<vol_ulid>/<snap_ulid>` pin
  form. Bare name and bare ULID become errors. This change waits on
  `materialize` because it removes the existing "branch from now" paths
  that `materialize` is the replacement for.
- Introduce `volume materialize <new-name> --from <vol_ulid>`.
- Remove `--force-snapshot` and its supporting machinery
  (`create_readonly_snapshot_now`, `FORCE_SNAPSHOT_KEY_FILE`,
  `ParentRef.manifest_pubkey`, `fork_volume_at_with_manifest_key`).

## Glossary changes

"Fork" remains useful as a lineage term (a replica that diverged) but is
no longer a CLI command. Documentation should distinguish the concept
(describing lineage) from the retired command (`volume fork`).
