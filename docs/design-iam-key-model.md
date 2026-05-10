# Per-volume IAM key model

## Status

Proposed. Extracted from `architecture.md` for focused iteration.

Applies to backends with per-access-key IAM policies — Tigris is the
motivating case. AWS STS deployments use a different mechanism
(session policies on `AssumeRole`) and are out of scope for this doc.
Plain-S3 / no-IAM deployments fall back to the shared-key downgrade
documented in *Credential backends* in `architecture.md`.

## Why

The coordinator currently holds a single full-access key in env and
signs every operation with it: uploads, GC, name claims, deletes. The
read-only credentials it vends to volume processes are also the
operator's own configured key. Two consequences follow:

- **Blast radius.** A bug in any actor that holds a key can touch any
  S3 prefix. There is no IAM-layer scoping per volume.
- **Audit ambiguity.** IAM logs cannot distinguish "coordinator
  uploaded for volume X" from "coordinator uploaded for volume Y" —
  both are signed by the same key.

Per-key IAM policies (Tigris-style: `CreateAccessKey` + `CreatePolicy`
+ `AttachUserPolicy` + `DateLessThan` conditions) let us split the key
inventory by purpose and scope, addressing both concerns without
requiring STS.

## Top-level bucket layout

The bucket has four top-level prefixes. Production users of each:

| Prefix | Purpose | PUT | GET | LIST | DELETE |
|---|---|---|---|---|---|
| `by_id/<vol-ulid>/` | Per-volume data: `volume.pub`, `volume.provenance`, `segments/`, `snapshots/`, `retention/` | coord (upload, identity, reaper) | coord (pull, prefetch, fork), volume process (segment fetch), peer-fetch (auth) | coord (snapshot/segment/retention enumeration) | coord (reaper) |
| `names/<name>` | Name → vol_ulid claim records | coord (claim, fork, force-release) | coord (resolve, verify), peer-fetch (auth) | coord (`volume list --remote`) | coord (rollback) |
| `events/<name>/<event-ulid>.toml` | Append-only per-name event journal | coord (event store, peer-discovery) | coord (event read, peer-discovery scan) | coord (event log enumeration) | **none** |
| `coordinators/<coord-ulid>/coordinator.pub` | Per-coordinator identity record | coord (identity publish, once) | coord (peer identity verify), peer-fetch (auth) | none observed | **none** |

Volume processes only touch `by_id/<self>/` and `by_id/<ancestor>/`.
They never read `names/`, `events/`, or `coordinators/` — those are
coordinator-side concerns.

`events/` and `coordinators/` are append-only / immutable in the
current design. Compacting the event log and decommissioning a
coordinator are deferred features; the IAM model locks both prefixes
against deletion by default to enforce that boundary at the IAM
layer.

## Key classes

The coordinator manages four classes of key, plus a transient fifth.

### 1. Admin key

Operator-configured, never minted by the coordinator. Holds the IAM
management actions needed to mint and rotate every other key. Never
used for S3 data operations. Rotated rarely, by the operator.

The admin key is **root-equivalent**: with `CreateAccessKey` +
`CreatePolicy` + `AttachUserPolicy`, it can mint a new key with any
policy it wants. Operators must guard it accordingly.

### 2. Coordinator writer key

Minted by the admin key at first coordinator start, persisted in IAM,
reused across coordinator restarts. Holds the S3 mutation rights the
coordinator needs across all four top-level prefixes — but with
`DeleteObject` denied on `events/` (append-only) and `coordinators/`
(immutable identity).

All coordinator-side mutations route through this key:

- Segment upload, snapshot manifest/marker/filemap upload, retention
  marker writes (`by_id/`).
- `names/<name>` claim, rename, force-release.
- Event journal appends (`events/<name>/<event-ulid>.toml`).
- One-time coordinator identity publish (`coordinators/<coord-ulid>/
  coordinator.pub`).

One key per coordinator, **not** per volume — see *Per-volume
scoping for writes (rejected)* below.

### 3. Per-volume read-only key

Minted at `volume create` / `volume fork`, deleted on `volume delete`.
Held by the coordinator and vended to the corresponding volume
process via the macaroon handshake. Scope: `s3:GetObject` on the
volume's own prefix and the prefixes of every ancestor in its
lineage. The ancestor list is fixed at fork time and immutable for
the volume's lifetime — the policy is stable apart from
`DateLessThan` refresh.

### 4. Peer-fetch key

Minted by the admin key at peer-fetch enable, deleted on disable.
One per coordinator. Held by the peer-fetch service for **auth-
verification reads only**: it reads `volume.pub`, `volume.provenance`,
`names/<name>`, and `coordinators/<coord-id>/coordinator.pub` to
verify incoming requests, then serves segment bytes out of the local
cache.

Peer-fetch is internet- or LAN-exposed and so deserves a key separate
from the writer key. It must not be able to reach segment bodies in
S3 — if a segment isn't in the local cache, peer-fetch reports "not
here" and the requester falls back to its own S3 path. Granting S3
segment-body access would defeat the cache-tier model.

### 5. Ephemeral fetch keys (transient)

Minted by the admin key at the start of `elide volume fetch`, deleted
when the fetch worker exits or when `DateLessThan` expires. Scope
mirrors the per-volume RO key (target volume + ancestors). Used by
both the coordinator front-half and the spawned `elide fetch-volume`
worker. Name resolution and any `coordinators/` reads needed for
ancestor verification go through the **writer key**, not the fetch
key — keeping the fetch key purely for `by_id/` data so the spawned
worker has no path to the names index or coordinator identity
records.

## Per-volume scoping for writes (rejected)

A natural extension would be to mint per-volume *writer* keys for the
coordinator's own use — one per volume, scoped to that volume's
prefix. We are not doing this.

The argument for it is "catch confused-deputy bugs at the IAM layer":
a logic error inside the coordinator that targets the wrong vol_ulid
would be rejected by IAM. That benefit is real but modest. The upper
layers (`volume_event_store`, claim records, the per-volume directory
structure) are where volume-identity correctness actually lives;
IAM-level enforcement here would be a redundant belt over a working
pair of suspenders.

What we give up is per-volume audit trails on writes. We accept
that — coordinator logs can tie a write to a request ID and causal
chain, which IAM logs cannot.

## IAM-layer invariants

Splitting keys by purpose lets us encode design invariants directly
in the IAM policies, where they hold even against coordinator bugs:

- **Event log is append-only.** No key in the model holds
  `s3:DeleteObject` on `events/*`. A coordinator bug or compromised
  process cannot rewrite or erase event-log history.
- **Coordinator identity is immutable.** No key holds
  `s3:DeleteObject` on `coordinators/*`. A coordinator pub-key once
  published cannot be retracted at the IAM layer.
- **Volume processes are read-only.** Per-volume RO keys hold only
  `s3:GetObject`, scoped to the volume's lineage. No bug in a volume
  process can cause an S3 mutation, regardless of what code paths it
  reaches.
- **Peer-fetch cannot read segment bodies.** Peer-fetch keys hold
  `s3:GetObject` only on auth-artefact suffixes (`volume.pub`,
  `volume.provenance`, `names/*`, `coordinators/*/coordinator.pub`),
  not on segment paths. Compromised peer-fetch cannot bypass the
  cache tier to drain segments from S3.

Lifting any of these requires a deliberate policy change, not a code
change.

## Identity and host locality

Tigris does **not** support tagging access keys, so identity is
encoded in policy names instead. Each coordinator-managed policy is
named with the coordinator's identity prefix:

```
elide-<coordinator-ulid>-writer
elide-<coordinator-ulid>-peerfetch
elide-<coordinator-ulid>-<volume-ulid>-ro
elide-<coordinator-ulid>-fetch-<fetch-ulid>
```

Coordinator identity already exists (`coordinator.root_key`, public
signing key in `crate::identity`); the public part is the
coordinator-ulid in policy names.

Consequences:

- **Reconciliation is host-local.** On startup each coordinator runs
  `ListPolicies`, filters by its own `elide-<coordinator-ulid>-`
  name prefix, cross-references against its local `by_id/` and fetch
  registry, and reaps orphans only among its own policies (and the
  keys those policies attach to). Other coordinators' policies are
  invisible to this one.
- **No key material crosses host boundaries.** A volume released
  from host A and claimed on host B causes A to delete its keys for
  that volume; B mints fresh keys against its own identity. The
  vol_ulid is the only artefact both sides agree on.
- **Dead-host orphans need operator action.** If a coordinator's
  host is destroyed permanently, its policies and keys outlive it.
  No other coordinator reaps them. The intended cleanup is an
  explicit operator command (`elide admin reap-host
  <coordinator-ulid>`). Automatic cross-host reaping is out of
  scope.
- **Cross-host fork is fine.** A child volume on host B with a
  parent on host A has a policy referencing `by_id/<parent>/*`. IAM
  doesn't care that host A "owns" that prefix — the policy is on
  host B's key, scoping host B's reads. Reconciliation must not
  interpret "this key references prefix P" as "this key belongs to
  whoever owns P".

Single-writer enforcement across hosts is **not** an IAM property —
IAM will happily issue valid write keys to two coordinators for the
same volume. Single-writer stays at the claim layer (`names/<name>`
records, fork lock files; see `design-volume-event-log.md`).

## Lifecycle

| Key class       | Minted by                         | Deleted on                       | Refresh                              |
|-----------------|-----------------------------------|----------------------------------|--------------------------------------|
| Admin           | Operator                          | Operator                         | Operator                             |
| Writer          | Admin, on first coord start       | Coordinator decommission         | Detach + create + attach (see below) |
| Peer-fetch      | Admin, on peer-fetch enable       | Peer-fetch disable               | Detach + create + attach             |
| Per-volume RO   | Admin, on volume create / fork    | `volume delete`                  | Detach + create + attach             |
| Ephemeral fetch | Admin, on `start_fetch`           | Fetch worker exit, or expiry     | None — short-lived                   |

`DateLessThan` is the safety net for every class except the admin
key. Tigris's policy-update path may not support in-place mutation;
refresh is implemented as create-new-policy → attach → detach-old →
delete-old. The credential material itself is unchanged.

## Inventory

The coordinator persists per-volume key metadata at
`<data_dir>/by_id/<vol-ulid>/iam.json`:

```json
{
  "ro_access_key_id": "tid_…",
  "policy_name": "elide-<coordinator-ulid>-<volume-ulid>-ro",
  "policy_expiry_unix": 1717200000,
  "ancestor_chain": ["<parent-ulid>", "<grandparent-ulid>"]
}
```

Inspectable with `ls` and `cat`, satisfying the
on-disk-state-is-inspectable invariant. Secret material is **not**
persisted; the coordinator caches it in memory for the volume's
lifetime and re-mints (rotating the access key) if it loses it.

The writer key's metadata lives at
`<data_dir>/coordinator/iam.json`, and the peer-fetch key's at
`<data_dir>/coordinator/iam-peerfetch.json`, in the same shape.
Fetch keys are not persisted — their lifetime is shorter than a
coordinator restart; if the coordinator crashes mid-fetch, recovery
deletes any policies matching the fetch-name prefix that don't
correspond to an active fetch in the registry.

## Policy sketches

### Admin key (operator-attached)

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ElideAdminKeyManagement",
    "Effect": "Allow",
    "Action": [
      "iam:CreateAccessKey",
      "iam:DeleteAccessKey",
      "iam:UpdateAccessKey",
      "iam:ListAccessKeys",
      "iam:CreatePolicy",
      "iam:DeletePolicy",
      "iam:GetPolicy",
      "iam:ListPolicies",
      "iam:AttachUserPolicy",
      "iam:DetachUserPolicy",
      "iam:ListUserPolicies"
    ],
    "Resource": "*"
  }]
}
```

No S3 access. Operator attaches; coordinator never modifies.

### Coordinator writer key

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VolumeData",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::<bucket>/by_id/*"
    },
    {
      "Sid": "Names",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::<bucket>/names/*"
    },
    {
      "Sid": "EventLogAppendOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::<bucket>/events/*"
    },
    {
      "Sid": "CoordinatorIdentityImmutable",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::<bucket>/coordinators/*"
    },
    {
      "Sid": "BucketList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::<bucket>"
    }
  ]
}
```

`s3:ListBucket` is bucket-scoped — Tigris has no `s3:prefix`
condition, so listing is all-or-nothing. Acceptable for the writer.
No `DateLessThan`; rotation is operator-driven.

### Peer-fetch key

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AuthArtefactsOnly",
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::<bucket>/by_id/*/volume.pub",
      "arn:aws:s3:::<bucket>/by_id/*/volume.provenance",
      "arn:aws:s3:::<bucket>/names/*",
      "arn:aws:s3:::<bucket>/coordinators/*/coordinator.pub"
    ]
  }]
}
```

No segment-body access. No write actions. The wildcard suffixes
depend on Tigris's S3-compatible IAM supporting `*` mid-resource —
verify before relying on it; if not supported, fall back to listing
volumes explicitly or scoping to `by_id/*` for `s3:GetObject` (with
the loss that peer-fetch could then read segment bodies it shouldn't).

### Per-volume read-only key

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ReadVolumeAndAncestors",
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::<bucket>/by_id/<self-ulid>/*",
      "arn:aws:s3:::<bucket>/by_id/<ancestor_1-ulid>/*",
      "arn:aws:s3:::<bucket>/by_id/<ancestor_N-ulid>/*"
    ],
    "Condition": {
      "DateLessThan": {"aws:CurrentTime": "<expiry-iso8601>"}
    }
  }]
}
```

No `s3:ListBucket`. Volume processes fetch by ULID via the snapshot
manifest; they never enumerate. No write actions. Optional
hardening: add an `IpAddress` condition pinning the host's egress IP.

### Ephemeral fetch key

Same shape as per-volume RO, with a shorter `DateLessThan` (e.g.
fetch-start + 24h). The coordinator front-half uses the **writer
key** for `names/<name>` resolution and any `coordinators/*`
verification reads, so the fetch key remains purely volume-data.

## GC / ancestor pruning

When a child's ancestor is fully consumed by GC into the child, the
child's policy still lists the now-empty ancestor prefix. We do not
prune. Stale entries are harmless extra read scope — they cost a
small amount of policy size and nothing else.

## Operational notes

- **Tigris quota.** Tigris does not document a per-organization
  access-key cap. With per-volume + per-fetch keys, a coordinator
  with N volumes and F active fetches uses 3 + N + F keys (admin +
  writer + peer-fetch + N volume + F fetch). Verify before relying
  on per-volume keys at scales of thousands of concurrent volumes
  per coordinator.
- **Optional `IpAddress` condition.** Tigris IAM supports
  `IpAddress` and `NotIpAddress` in policy conditions. Binding
  worker-class keys (RO, fetch, peer-fetch) to the host's egress IP
  gives defense-in-depth — a leaked key cannot be used off-host —
  but is brittle (NAT, IP changes, multi-homed). Off by default;
  available as a hardening option for deployments with stable
  egress.
- **Coordinator privilege ceiling.** The admin key is the only
  privileged credential; a host running the coordinator must guard
  it accordingly. The writer, peer-fetch, and per-volume keys are
  individually scoped, and a leak of any one bounds the damage to
  that key's policy.
- **Wildcard resource patterns.** Several policies above (peer-fetch
  in particular) rely on mid-resource wildcards
  (`by_id/*/volume.pub`). Verify Tigris's S3 IAM supports this
  pattern before relying on it.

## Volume fetch

`elide volume fetch <name>` resolves a remote volume name, pulls the
ancestor chain, downloads the manifest and per-segment idx files,
and spawns `elide fetch-volume` to body-warm the cache. Two actors
read S3.

The split:

- **Coordinator front-half** uses the **writer key** for name
  resolution (`names/<name>`) and any coordinator-pub reads
  (`coordinators/<id>/coordinator.pub`) needed for ancestor
  verification.
- **Ephemeral fetch key** is minted at `start_fetch`, scoped to
  `[target, ancestors…]` on `by_id/`, with `DateLessThan` set to
  fetch-start + a generous bound (e.g. 24 h) as a safety net for
  orphaned keys. The coordinator front-half uses it for manifest
  and idx fetches; the spawned `elide-fetch-volume` worker
  authenticates via the macaroon handshake (same path as a volume
  process) and receives the same key as its credentials. On worker
  exit, the coordinator deletes the policy and key.

If `volume fetch` is followed by `volume claim`, the claim flow
mints the volume's durable per-volume RO key. The fetch key and the
post-claim RO key are **separate**; the fetch key does not become
the claim key.

## Open questions

- **Tigris policy size limit** under deep ancestor chains. Likely
  not a concern in practice (fork depth ≤ small numbers) but
  unverified.
- **Tigris wildcard semantics** in resource ARNs (mid-resource `*`).
  The peer-fetch policy depends on this; verify before relying.
- **Writer key rotation cadence.** `DateLessThan` is not used; a
  scheduled rotation flow (operator-triggered or time-based) is
  open.
- **First start after upgrade** from a coordinator that wasn't
  using policy-name-based identity. Existing keys won't match the
  filter; they need a one-time migration step or operator opt-in.
  Spec'd separately when the upgrade path is needed.

## Non-goals

- The AWS STS path (different mechanism — session policies on
  `AssumeRole`, not per-key IAM policies).
- The shared-key downgrade for backends without per-key IAM policies.
- The macaroon protocol itself — covered in `architecture.md`.
- Cross-host coordination of any kind beyond what already exists in
  the claim layer.
- Event-log compaction and coordinator decommissioning. Both are
  deferred; the IAM model locks `events/` and `coordinators/`
  against deletion to enforce that boundary.
