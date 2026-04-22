# Elide as a Tigris-native system

## Status

Exploration. Not a committed direction. This doc thinks through what
Elide looks like if Tigris is treated as a first-class backend rather
than as one of several S3-compatible object stores sitting behind a
common trait.

## Premise

The current design treats Tigris as *an* S3-compatible backend. This
is defensible — it keeps Elide portable across AWS S3, MinIO, R2, and
Tigris — but it also means every Tigris-native feature we care about
(bucket snapshots, instant forks, always-on versioning, probably
metadata-only intra-bucket `CopyObject`) is either unused or has to be
reinvented in the Elide layer on top of a lowest-common-denominator
interface.

The question is whether to flip the default: design Elide *as* a
Tigris-native system, with a plain-S3 path that is understood to be a
degraded mode missing the cheap primitives Tigris gives us.

## What Tigris provides natively

- **Bucket snapshots** — O(1), reference-based, named, immutable.
  Readable directly from the live bucket by passing
  `X-Tigris-Snapshot-Version` on GETs; no separate endpoint.
- **Bucket forks** — instant writable clones seeded from a snapshot.
- **Per-object versioning** — always-on. The data model is append-only,
  so every write creates a new version, regardless of whether a user
  opts into versioning. Enumerable via `ListObjectVersions` +
  `versionId`.
- **Probable: metadata-only intra-bucket `CopyObject`** — unverified.
  If true, whole-segment re-parenting is effectively free at the
  storage layer.
- **Global distribution** with claimed zero egress.
- **Object lifecycle rules** for non-current-version and snapshot TTL.

Several of these have no direct S3 equivalent; a few map onto S3
features (object versioning, lifecycle rules) but Tigris's
implementation choices differ.

## What Elide implements today on top of S3

- WAL for the durable-write tail (local).
- Segment format (packed extents).
- LBA map / extent index per volume.
- Content-addressed dedup within and across volumes.
- Per-volume snapshots via manifest versioning
  (`<vol_ulid, snap_ulid>`).
- Volume forks with an attestation keypair to prove lineage.
- GC with segment compaction.
- Coordinator: owns S3 mutations, manages TTL/GC/lineage.

Some of these become redundant, partially redundant, or different in
shape once Tigris-native features are taken as given.

## Two mappings

The first architectural choice is how Elide volumes correspond to
Tigris buckets. The subsequent subsystem implications depend on this
choice.

**(A) One volume per bucket.** Each Tigris-native primitive maps 1:1
onto an Elide volume primitive: Tigris snapshot = Elide snapshot,
Tigris fork = Elide fork, `volume materialize` = intra-bucket copy
from a snapshot. No application-level snapshot/fork machinery is
needed.

- *Pros:* Maximum reuse of native primitives. Clean mental model.
  DR / lifecycle / retention become per-volume knobs. Cross-volume
  operations (e.g. "share my dataset with another account") become
  standard Tigris bucket operations.
- *Cons:* N buckets for N volumes. Unknown cost and operational feel
  at realistic scale. Per-bucket account limits. Potentially noisy
  for multi-tenant operators managing many volumes.

**(B) Multi-volume bucket.** Buckets group volumes (per tenant, per
project, per host). Tigris snapshots are cross-volume. Elide still
maintains per-volume manifest/index, but several subsystems simplify
because Tigris handles object lifecycle and retention.

- *Pros:* Fewer buckets. Better fit for multi-tenant deployments.
  Cross-volume atomicity is possible (e.g. snapshot a whole tenant at
  once).
- *Cons:* Granularity mismatch: per-volume operations still need an
  application layer on top of coarse Tigris primitives. Some of the
  cleanliness of mapping (A) is lost.

Either mapping is compatible with the underlying proposal; the choice
is about operational shape, not correctness. Both are worth sketching
through before picking.

## Subsystem implications

### Snapshots

Today an Elide snapshot is a `<vol_ulid, snap_ulid>` manifest pointer.
The manifest bookkeeping exists to give volume-level atomicity on top
of individually-versioned S3 objects.

- Under mapping (A), `Elide snapshot := Tigris snapshot`. Single
  primitive. No manifest versioning; the manifest is simply "inside"
  the snapshot.
- Under mapping (B), Elide snapshots still exist as manifest
  pointers, but their retention is delegated to Tigris snapshots at
  coarser granularity.

### Forks

Today `volume create --from` creates a new manifest that points at
the parent, with an attestation keypair used to prove lineage during
GC.

- Under mapping (A), `volume create --from` maps directly onto
  `tigris forks create` on the parent's bucket. No attestation key
  needed: the fork lineage is inherent to Tigris's data model and
  cannot be forged.
- Under mapping (B), the application-level fork machinery stays but
  can lean on snapshot-pinned reads for race-free operation.

### Materialize

Today `volume materialize` is designed but unimplemented. Planned
shape: re-emit all of the target volume's extents as new segments
owned solely by that volume, breaking shared-segment references.

With Tigris:

- If intra-bucket `CopyObject` is metadata-only (to be verified),
  materialize is a pointer retarget rather than a byte move — nearly
  free at the storage layer.
- Reading through a snapshot makes materialize race-free against
  upstream GC without any coordinator coordination.
- With historical versioning, `materialize --from <vol>@T` for any
  retained T is available essentially for free on top of the same
  mechanism.

### Dedup

Unaffected. Tigris's versioning deduplicates within a single object's
version chain; Elide's content-addressed dedup operates across extents
within segments. They are orthogonal and complementary. Dedup stays.

### GC

Today the coordinator runs per-segment liveness analysis across
volumes, compacts partially-live segments, deletes unreferenced
segments, and manages TTL.

With Tigris native:

- *Retention* of whole segments can be delegated to Tigris lifecycle
  rules: snapshots pin the versions they reference; non-pinned
  versions expire on a configured schedule. The application-level
  cross-volume refcounting layer can go away.
- *Compaction* (packing partially-live extents into new segments) has
  to stay in Elide — Tigris has no visibility into extent-level
  liveness.

Net effect: the GC story splits cleanly into "retention" (Tigris) and
"packing" (Elide). This is meaningfully simpler than today's
refcount-and-delete-under-TTL shape.

### Coordinator

Today the coordinator owns all S3 mutations, manages fork attestation
keys, runs GC, and enforces TTL-bounded races.

With Tigris native:

- Lineage proof via attestation key is probably redundant: Tigris
  snapshot identity is unforgeable and provides the same guarantee.
- TTL bookkeeping moves to Tigris lifecycle rules.
- The read-only-volume / mutating-coordinator credential separation
  still applies — it's an authorization property, not a backend
  property.

### Disaster recovery

Today DR is "bring your own backup strategy" — the S3 bucket is the
only artefact.

With Tigris native, DR is implicit:

- Retention policy = RPO.
- Snapshot cadence = recovery granularity.
- `materialize --from <vol>@<snapshot>` is the restore command.

No separate DR system is needed for Tigris deployments. For plain-S3
deployments this remains a gap unless the operator layers their own.

### Credentials

Unaffected in shape. The volume has read-only credentials, the
coordinator holds mutating credentials. Tigris supports the same
scoped access-key model.

## Risks

- **Lock-in.** Plain-S3 (AWS S3, MinIO, R2) becomes a second-class
  path with materially fewer features: no historical materialize, no
  O(1) snapshots, coordinator-side retention bookkeeping stays.
  Whether this is acceptable depends on the product positioning.
- **Cost opacity.** Tigris's always-on versioning isn't free at scale.
  Users who don't tune lifecycle rules can hit surprising bills.
  Defaults have to be sensible and visible.
- **Untested assumptions.** Several design choices depend on Tigris
  behaviour we haven't benchmarked (`CopyObject` semantics, SDK
  header injection, bucket count limits, snapshot-pinned read path).
- **Test infrastructure.** CI needs Tigris — or a compatible
  emulator — to exercise native paths. Today our local story is
  plain-S3 via MinIO.

## What needs experimenting before committing

- **Mapping choice** (one-volume-per-bucket vs grouped): cost, account
  limits, operator ergonomics.
- **`CopyObject` semantics** within a Tigris bucket — metadata-only or
  full byte copy?
- **Snapshot scoping** — bucket-wide only, or prefix-scoped? Retention
  cost implications.
- **Rust S3 SDK header injection** for `X-Tigris-Snapshot-Version`
  (already on the list in `design-replica-model.md`).
- **Per-volume bucket scaling** — if mapping (A) is viable, at what
  volume count does it break?
- **Lifecycle policy defaults** — what retention should ship out of
  the box? What knobs should be operator-visible?

## Non-goals for this doc

- Deciding between mapping (A) and (B).
- Committing to abandoning the plain-S3 path.
- Re-designing the WAL, segment format, LBA map, or extent index —
  those are backend-independent and unaffected by this question.
