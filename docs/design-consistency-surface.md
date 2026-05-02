# Consistency surface and bucket separation

## Status

Exploratory. Triggered by realising that Tigris's default bucket
location type ("Global") is **eventually consistent globally** —
which is exactly the regime that
[`design-portable-live-volume.md`](design-portable-live-volume.md)
explicitly rules out for the `names/<name>` CAS. This document
catalogues which Elide operations actually require strong consistency
and which can tolerate eventual consistency, and sketches a two-bucket
deployment that aligns each requirement with the cheapest backend
configuration that satisfies it.

## Tigris bucket location types

From the Tigris documentation:

| Location type | Consistency wording |
|---|---|
| Global (default) | Strong same-region, **eventual globally** |
| Dual-Region | Strong same-region, **eventual globally** |
| Multi-Region | **Strong globally** |
| Single-Region | **Strong globally** |

The Tigris docs do not currently describe a separate consistency
guarantee for conditional writes (`If-None-Match`, `If-Match`) —
they appear to inherit the bucket's general consistency property.
This is an open question worth confirming with Tigris support; the
rest of this document assumes the conservative reading that
conditional writes on a Global bucket are not globally linearizable.

## Catalogue: S3 access patterns

Each row is a distinct object class and how it is accessed. The last
column states the weakest consistency property the operation needs to
remain correct.

| Object class | Mutation shape | Readers | Consistency need |
|---|---|---|---|
| `by_id/<vol>/YYYYMMDD/<segment>` | Write-once PUT, ULID-named. Coordinator-only DELETE during GC, after the volume's signed handoff. Idempotent re-PUT. | Volume that owns the prefix (own host); replicas/forkers (any host) on demand-fetch | **Eventual-tolerant.** Cross-region readers may see 404 briefly after upload; the demand-fetch retry path absorbs this. ULID naming makes PUT a no-CAS operation — uniqueness is by name, not by check-then-write. |
| `by_id/<vol>/snapshots/<snap_ulid>` (marker) | Write-once empty object | Forkers, replicas | **Eventual-tolerant.** A reader that sees a marker before its `.manifest` / `.filemap` retries; a reader that sees the marker before the underlying segments retries on demand-fetch. |
| `by_id/<vol>/snapshots/<snap_ulid>.manifest` / `.filemap` | Write-once, signed | Forkers, replicas | **Eventual-tolerant.** Same as the marker — ordered as marker-last, but readers tolerate any visibility order via retry. |
| `by_id/<vol>/volume.pub`, `volume.provenance` | Write-once at create/import; immutable | Replicas (signature verification, OCI-source inspection on imported roots) | **Eventual-tolerant.** Stable; only published once. |
| `coordinators/<coord_id>/coordinator.pub` | Write-once at first coordinator startup; immutable | Other coordinators verifying segment signatures | **Eventual-tolerant.** |
| **`names/<name>`** | **Conditional PUT** — `If-None-Match: *` for create-if-absent on claim, `If-Match: <etag>` for ownership transfer / release | All coordinators that may host the name | **Globally linearizable single-key CAS.** Cannot tolerate eventual consistency without breaking the name-uniqueness invariant. |
| `_capabilities/conditional-put-probe-<ulid>` | Single conditional PUT (capability detection) | Same coordinator | Strong on the bucket where it lives — same regime as `names/`. |

## What requires strong consistency

Just one pattern: the `names/<name>` CAS lifecycle (claim, transfer,
release) defined in
[`design-portable-live-volume.md`](design-portable-live-volume.md).

That single primitive carries:

- Name uniqueness (no two coordinators believe they own the same name).
- Ownership transfer (`stopped → released → live` chain).
- The explicit-skip semantics of `volume release --force`.

Lose linearizability on `names/<name>` and you can construct a
two-coordinator split-brain over a single volume identity, which the
rest of the design assumes cannot happen.

The capability probe (`_capabilities/conditional-put-probe-<ulid>`)
inherits the same requirement, but is a single-shot detector — it
either works on this bucket or doesn't.

## What tolerates eventual consistency

Every other S3 access pattern in Elide:

- **Segments are write-once and ULID-named.** Uniqueness comes from
  the ULID, not from any check-then-write. Idempotent re-upload is
  the recovery story for any uncertainty about whether a previous PUT
  landed. Cross-region readers tolerate the eventual-visibility
  window because the read path is already retry-driven (a
  demand-fetch 404 falls through to the existing retry / refresh
  path).

- **Snapshots are published as ordered, write-once objects** —
  segments, then `.manifest`, then the empty marker. A replica in
  another region that observes objects out of order simply retries
  the missing piece; nothing is corrupted. The "snapshot publication
  is the linearization point" framing in the existing docs is
  load-bearing only within a single region; cross-region replicas
  already have to wait for replication and already retry.

- **Volume metadata (`volume.pub`, `volume.provenance`)** is
  effectively immutable. Eventually-consistent reads are fine.

- **Coordinator public keys** are the same: write-once, immutable.

The `volume.pub` and `volume.provenance` claims here assume we keep
them immutable. Key rotation would change this; it would also need to
be designed against a CAS primitive, at which point it joins
`names/<name>` in the strong-consistency tier.

## Two-bucket split

Because exactly one pattern needs strong global consistency, and
strong-globally bucket configurations (Multi-Region, Single-Region)
are typically more expensive than Global, a deployment can split:

- **Coordination bucket** — small, configured for **strong global
  consistency** (Multi-Region or Single-Region on Tigris; standard
  S3 with conditional-PUT in any region; etc). Holds:
  - `names/<name>`
  - `coordinators/<coord_id>/coordinator.pub`
  - `_capabilities/`

- **Data bucket** — large, configured for the cheapest acceptable
  tier (Tigris Global is now in scope). Holds everything else:
  - `by_id/<vol>/...` (segments, snapshot markers/manifests/filemaps,
    `volume.pub`, `volume.provenance`)

The coordination bucket carries kilobytes per name; the data bucket
carries the entire working set. The cost ratio is what makes the
split worth considering on Tigris specifically — paying for
strongly-globally-consistent storage on every segment byte is
wasteful if all that ever lands there is the name pointer.

### How `names/<name>` references the data bucket

`names/<name>` is already a structured object (see
`design-portable-live-volume.md` §"Mechanism: `names/<name>` as the
owner pointer"). The minimal change is to extend its body with a
data-bucket descriptor — bucket URL plus prefix — so that resolving a
name yields both "who owns it" and "where the bytes live". This keeps
`names/<name>` the single source of truth at lookup time.

Today, the bucket is implicit per-coordinator. Making the data
location explicit per-name is also independently useful: it leaves
the door open to per-volume bucket placement (geo, cost class) and
to migration between data buckets without changing the name.

### Cross-bucket ordering

There is one new ordering subtlety: the snapshot publication path
writes to the data bucket (segments, marker) and may update
`names/<name>` in the coordination bucket to advertise the new
latest snapshot. A cross-region reader that resolves
`names/<name>` linearly (coordination bucket, strong) and then
GETs the marker (data bucket, eventual) may see the marker before
its segments are visible. This is no worse than today's single-bucket
Global behaviour — it's the same retry window — but it's worth being
explicit that the strong-consistency property of `names/<name>` does
**not** transitively make the data it points at strongly consistent.

`names/<name>` could also embed only a coordinator hint and not the
latest snapshot; that keeps `names/<name>` writes minimal (claim /
release / transfer only) and pushes "what's the latest snapshot" back
to a LIST against the data bucket, which is already eventually
consistent and already what the design assumes today.

### What the split does not solve

- **Replica freshness on Global data buckets.** If the snapshot
  marker for region A's latest write hasn't replicated to region B
  yet, region B's replica is behind. This was already true; the
  split does not change it. It is the user's deployment choice to
  accept that window in exchange for Global pricing on the data
  bucket.

- **GC visibility.** Coordinator GC deletes from the data bucket
  after the volume's signed handoff. A reader in another region may
  briefly see deleted objects (LIST drift) or briefly miss
  newly-uploaded ones. The handoff protocol already tolerates this:
  no segment is deleted without the volume's acknowledgment, and
  readers retry.

- **Capability-probing complexity.** Each bucket needs its own
  conditional-PUT probe; the data bucket may legitimately be unable
  to do conditional PUT (and that's fine, because we don't need it
  there).

## Trade-offs of the split

For:

- Aligns each operation with the cheapest backend that satisfies it.
- Makes the consistency requirement of each subsystem explicit at the
  layout level — easier to reason about, easier to spot accidental
  upgrades to the strong tier.
- Per-name data placement opens up future geo / cost-class strategies.

Against:

- Two sets of credentials, two IAM scopes, two endpoints to
  configure. Volume-level credential issuance via macaroons (see
  `architecture.md` §"S3 credential distribution via macaroons") gets
  more complicated — a volume needs read on the data bucket but not
  the coordination bucket; a coordinator needs CAS on coordination
  and read-write on data.
- More moving parts in the operator-facing config. A single-bucket
  deployment still needs to be the default; the split is an
  optimisation, not a requirement.
- The `names/<name>` body now references the data bucket's URL,
  coupling the name pointer to bucket configuration. Migrating a
  volume between data buckets becomes a `names/<name>` rewrite plus
  a copy.

## Failure modes under eventual consistency

This section walks through what concretely breaks if `names/<name>`
is hosted on an eventually-consistent bucket — i.e. if we ignore the
strong-consistency requirement and run portable-live-volume on a
Tigris Global bucket as configured today. The shape of each failure
matters for thinking about whether any partial mitigation is
plausible, and for understanding the recovery story.

The failures cluster into four severity tiers.

### Tier 1: catastrophic — split-brain on `names/<name>`

These are the cases where two coordinators in different regions both
believe they own the same name simultaneously. Each spins up a
volume process with its own freshly-minted `vol_ulid`, accepts client
writes, publishes snapshots, and uploads segments to its own data
prefix.

**(a) Concurrent fresh claim.** Coordinators A (region X) and B
(region Y) both run `volume start --remote <name>` on a name that
does not yet exist. Each issues `If-None-Match: *` PUT on
`names/<name>`. Each replica sees the key as absent locally; both
PUTs succeed. After replication converges, one PUT wins (typically
last-write-wins by clock); the other is silently overwritten. Both
coordinators believe they hold the name and have already started
serving I/O. Each has minted its own `vol_ulid` and is writing
segments to a disjoint prefix.

End state: two parallel volume timelines exist in the data bucket
under different `vol_ulid`s. The losing coordinator's writes are
durable but invisible — `names/<name>` no longer points at them.
Clients that connected to the losing coordinator have lost every
write since the claim. The winning coordinator's volume looks
healthy.

**(b) Concurrent ownership transfer.** A holds the name, B attempts
`volume start --remote` to claim it (cooperative or `--force`). B
does an `If-Match: <etag>` PUT to flip `live → released → live`
re-bound to itself. Meanwhile A is doing its own concurrent `If-Match`
write — e.g. publishing a new snapshot pointer that lives in
`names/<name>`. Both `If-Match`es see the same etag in their local
replica and both succeed locally. After convergence, one wins.

End state: same as (a). The variant is that A's losing writes may
include a snapshot publication, so the losing timeline may be
"further along" than in (a).

**(c) Coordinator restart with stale view.** A crashes; restarts;
reads `names/<name>` from the local replica and sees itself as
owner. In the global state, B has already taken over. A re-asserts:
re-fences via `If-Match`, takes writes, publishes. Eventually A's
view converges to "B owns this name", but A has already lost data on
its own timeline.

End state: A wrote segments under its own `vol_ulid` for a window
between restart and convergence. Those segments are orphaned.

#### Structural mitigations that already help

ULID-prefixed segment paths mean the two writers never collide at the
object-key level — each writer fills its own `by_id/<vol_ulid>/...`
prefix. There is no byte-level corruption of segment files, and no
ambiguity about who wrote what (segments are signed by per-volume
keys minted at fork time, so a forensic walk of the bucket can always
attribute each segment to a specific `vol_ulid`).

What is corrupted is **the binding from the name to the timeline**:
clients believed they were writing to `mydb`; their writes are
durable somewhere; but `names/mydb` may not point at the timeline
that received them.

#### Recovery shape

Recovery is manual: enumerate orphan `vol_ulid`s under the bucket,
inspect each, choose a winner (or merge timelines via `volume
materialize`), rewrite `names/<name>` to point at the chosen ULID.
There is no automatic merge — the two timelines can have semantically
contradictory writes at the same LBA.

This is a property of the design, not a property of the bucket: any
system that allows two writers to fork from the same point will hit
this. The bucket choice determines whether *the system permits* the
split-brain in the first place.

### Tier 2: ghost transfers

A flips `live → released`. B reads `released` and claims `live`. From
the perspective of a third reader in a region where neither write has
yet replicated, the resolution sequence is:

1. Reader sees `state=live, owner=A` (pre-release view).
2. Replication delivers A's release. Reader sees `state=released`.
3. Replication delivers B's claim. Reader sees `state=live, owner=B`.

Each step is internally consistent; the ordering is preserved by the
underlying replication. The harm is **temporal**: a third party
making a routing or credential decision during the window may pick
the wrong owner. If the reader is itself a coordinator that proceeds
to claim or write based on the stale view, it cascades into Tier 1.

If the reader is purely an observer (CLI listing, monitoring), the
harm is operational confusion only.

### Tier 3: stale data, recoverable via retry

These are the routine cross-region eventual-consistency failures
that already exist on a single Global bucket and will continue to
exist on the data bucket of a two-bucket split. They are listed for
completeness; none of them corrupt anything.

- **Cold-read 404 on segments.** A replica reads a snapshot manifest
  from the data bucket and immediately GETs a referenced segment. The
  segment hasn't replicated yet. The fetcher's retry path absorbs the
  404. Window: replication lag, typically sub-second.

- **Stale snapshot LIST.** A replica enumerating
  `snapshots/<vol_ulid>/...` sees an older set than the authoritative
  region. Replica boots from a slightly older snapshot than is
  available. Lag, not loss.

- **GC delete visibility skew.** Coordinator A deletes input segment
  N after a successful handoff. A reader in another region still
  sees N in `LIST`. The reader's local index for some hash points at
  N. The reader fetches N: succeeds (delete hasn't replicated) or
  404s (delete has replicated). On 404, the demand-fetch path
  refreshes the index and retries against the new GC output. Window:
  replication lag.

- **Provenance read drift.** Volume metadata (`volume.pub`,
  `volume.provenance`) is effectively immutable, so most reads
  tolerate eventual consistency trivially. A `remote pull`
  immediately after `volume create` may briefly 404 on
  `volume.provenance` and retry.

These are the failures the existing design already accepts. Putting
the data bucket on Tigris Global doesn't change them.

### Tier 4: cosmetic — operational view drift

`volume remote list` (a `LIST names/`) returns slightly different
results in different regions during a window after any
`names/<name>` write. Operators see different "truths" briefly. No
tooling that bases routing decisions on `LIST names/` will be
correct globally; the contract was already that authoritative
ownership is the per-key state, not the listing.

### Summary table

| Tier | Class | Where it lives | Worst case | Recoverable? |
|---|---|---|---|---|
| 1 | Split-brain on `names/<name>` | Coordination tier — requires strong consistency | Two timelines under one name; client writes durable but unattributed | Manual: enumerate orphan ULIDs and re-bind |
| 2 | Ghost transfer | Coordination tier | Reader picks wrong owner during convergence; cascades to Tier 1 if the reader is itself a writer | Self-resolves on convergence unless cascaded |
| 3 | Cold-read 404 / stale list / GC delete race | Data tier — eventual is fine | Brief 404, retry, succeed | Automatic via retry |
| 4 | View drift | Either tier | Operator sees slightly different listings per region | Self-resolves on convergence |

The tiers correspond directly to the catalogue: Tier 1–2 are
`names/<name>`; Tier 3 is everything that is already write-once or
ULID-named; Tier 4 is LIST-based discovery. The two-bucket split
preserves Tier 3 as eventual (the cheap, correct choice) and lifts
Tier 1–2 onto a strongly-consistent backend.

### What "running on eventual" looks like in practice

If we shipped portable-live-volume on a Tigris Global bucket without
changes:

- Single-region deployments — every coordinator is in the same Tigris
  region — would be **fine**. Within a region the consistency is
  strong; the failure modes above all require cross-region
  participation.
- Cross-region deployments would expose Tier 1 split-brain on every
  contested claim or transfer. The frequency depends on the
  application: a name that is rarely transferred and never raced
  might run for a long time without an incident; a name that
  multiple regions actively try to claim (e.g. a regional failover
  scenario) would corrupt on roughly the first race.

This is what makes the failure dangerous: it doesn't show up in
single-region testing, and it doesn't show up immediately when the
deployment goes multi-region. It shows up the first time two
operators race over the same name.

## Open questions

- **Tigris semantics for conditional writes on a Global bucket.**
  Are `If-None-Match` and `If-Match` evaluated linearizably across
  regions, or do they inherit the bucket's eventual-globally
  property? The conservative reading (the latter) is what this
  document assumes. Worth confirming with Tigris before committing
  to the split.

- **Default deployment.** Should single-bucket Multi-Region remain
  the default and the two-bucket split be a documented optimisation,
  or should the split be the recommended shape for Tigris
  specifically? The first is conservative; the second matches actual
  cost incentives on Tigris.

- **Name-pointer schema evolution.** Adding the data-bucket
  descriptor to `names/<name>` is a schema change. Whether it
  belongs in the initial portable-volume schema (rather than tacked
  on later) is a sequencing call — see the body shape in
  `design-portable-live-volume.md`.

- **Capability detection on the data bucket.** Today the
  conditional-PUT probe runs against whatever bucket the coordinator
  is configured with. With a split, the probe target is the
  coordination bucket; the data bucket's lack of conditional PUT is
  fine and need not be probed at all.

## Cross-references

- [`design-portable-live-volume.md`](design-portable-live-volume.md)
  §"Object store requirements" — the only place in the design that
  asserts a strong-consistency requirement.
- [`design-replica-model.md`](design-replica-model.md) — the
  cross-region reader path that already tolerates eventual
  consistency on the data bucket.
- [`design-tigris-native.md`](design-tigris-native.md) — the
  Tigris-native exploration; this document is its consistency
  counterpart.
- [`architecture.md`](architecture.md) §"S3 credential distribution
  via macaroons" — credential issuance, which a two-bucket split
  would extend.
