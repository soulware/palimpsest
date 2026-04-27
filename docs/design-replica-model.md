# Replica model

**Status:** Proposed framing; landed in stages.

- **Landed now:** `volume fork` is retired. The equivalent is `volume create
  <name> --from <source>` — the two commands were already semantically
  identical, so this is a pure cleanup.
- **Deferred:** Tightening `--from` to require an explicit
  `<vol_ulid>/<snap_ulid>` pin, and introducing `volume materialize`. These
  previously waited on TTL specifics for the object store; that gap is now
  resolved by application-managed retention markers (see below), so
  the blocker is implementation, not design. The existing `--force-snapshot`
  flag on `create --from` stays in place as the interim mechanism for
  branching past the most recent snapshot.

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

## What TTL buys us

The object store's deletion semantics determine what happens in the race
window between an upstream GC deleting a segment and a replica reading it.
The required model is:

- A deletion creates a **retention markers** with a bounded retention
  window T.
- Within T, `GET key` on the canonical object still returns bytes.
- After T, a reaper physically deletes the canonical object (and the
  marker); subsequent `GET`s 404.
- Readers never consult the `retention/` prefix — they read canonical
  keys only, and treat 404 as "past T, gone."

Rather than depending on any backend's native lifecycle, versioning, or
snapshot features, Elide owns this mechanism directly in the coordinator.
This keeps all backends symmetric (Tigris, R2, AWS S3, MinIO), avoids a
per-backend retention API, and lets us define the semantics precisely —
what T is, when it starts, how restart works. See *Marker record*
below.

`--force-snapshot` and its supporting machinery
(`create_readonly_snapshot_now`, the attestation keypair, the
`ParentRef.manifest_pubkey` field) stay in place until `materialize`
lands; they're then retired.

## Proposed: application-managed retention markers

Elide adds a `retention/` prefix under each volume's S3 root. When
the coordinator's handoff protocol would physically delete an S3 object
(GC input deletion, per the coordinator-driven handoff described in
`operations.md` and `architecture.md`), it instead writes a
retention marker and leaves the canonical object in place. A
periodic reaper on the owning coordinator physically deletes targets
(and their markers) whose retention window has elapsed.

### Marker record

Path: `by_id/<vol_ulid>/retention/<gc_output_ulid>`

The marker filename **is** the GC output ULID — the segment whose
handoff produced this marker. Cross-reference: `segments/<date>/<gc_output_ulid>`
(the GC output itself, in S3) and `retention/<gc_output_ulid>`
(its retention record) name each other. Re-running the upload of the
same GC handoff re-PUTs the marker key with the same content — fully
idempotent.

Content: plain text, one input segment ULID per line, no trailing
metadata.

```
01K7QXAB1JNZSCK4KKY5888AJ
01K7QXAB1JNZSCK4KKY5888BR
01K7QXAB1JNZSCK4KKY5888CW
```

That's the whole record. No retention field, no reason field, no
schema version, no volume ULID:

- **Retention is derived, not stamped.** Deadline =
  `ulid_timestamp(<gc_output_ulid>) + current_config.T`. This relies
  on the load-bearing property documented in *operations.md* under
  *gc_checkpoint — the pre-mint pattern*: GC output ULIDs come from
  `UlidMint::next` (clock-if-ahead-else-`last.increment()`), so their
  timestamps track wall-clock at handoff. Operator changes to T apply
  to all markers immediately — the intuitive behavior for an
  operational knob.
- **Reason is implicit.** v1 has one writer (the GC handoff), and the
  GC output's segment header already identifies it as a GC output
  (non-empty `inputs_length`). A diagnostic field would duplicate
  that.
- **Volume scope is structural.** The volume ULID is in the path; the
  marker content is a list of input ULIDs that the reaper reconstructs
  into S3 keys *under the invocation volume*. There's no shape an
  input ULID can take that escapes the volume's prefix.

### Reader semantics

Readers never list or open `retention/`. They read canonical paths
and treat 404 as the authoritative "reaped past T" signal. Replicas
that want to guarantee success discipline their own reads: capture the
upstream's latest `.manifest` as the reference set, then complete all
referenced segment fetches within T of that capture.

### Rebuild semantics

Unaffected. Local rebuild reads `index/*.idx` — retention markers
live in a different S3 prefix and are invisible. If a from-S3 rebuild
codepath is ever added, it can scan canonical `segments/` / `index/`
keys and ignore `retention/` entirely: any GC input still
physically present during its retention window is correctly superseded
by the higher-ULID GC output's `inputs` list, so its presence is
harmless to the extent-index view.

### Reaper

Runs as a periodic task on the coordinator that owns the volume. Lists
`retention/`, decides which markers are past T using their
creation ULID, then for each expired marker:

1. Parse and validate every target key (see *Target validation* below).
   Reject the entire marker on any failure — log loudly, leave the
   marker in place, do not partially reap.
2. Delete each listed target key (idempotent; a 404 is fine).
3. Delete the marker.

Order matters: targets first, marker second. A restarted reaper
re-listing the prefix will see any marker whose targets are already
gone, 404 its way through them, and finish by deleting the marker.
Reversing the order would leave orphaned targets that no reaper ever
revisits.

The owning coordinator is the sole writer to its volume's S3 prefix;
replicas reading from this bucket have no write authority and do not
participate in reaping.

#### Scope

The reaper runs only for volumes the local coordinator owns as a
writer — i.e. volumes the coordinator is currently driving GC and
drain for. Read-only references (cheap-reference replicas of an
upstream) have no reaper because they have no S3 write authority on
the upstream's prefix; the upstream's own coordinator is responsible
for reaping there.

#### Cadence and dispatch

A single coordinator-wide ticker fires on the cadence
`max(retention / 10, 1s)`. On each tick, the coordinator iterates the
volumes it owns and spawns a non-blocking reap operation per volume.
Operations are independent: a slow or failing S3 call on one volume
does not delay the others. One global ticker plus per-volume spawns
keeps the resource shape simple at the small volume counts a single
coordinator manages.

The `1s` floor exists for tests with short retention values; in
production T is on the order of hours, so the cadence floor never
binds.

#### Target validation

The volume identity flows through the reaper as a ground-truth input,
not as a parsed value. Three independent checkpoints must all agree on
the same `vol_ulid` before any deletion happens:

1. **Invocation.** The reaper is invoked per-volume. The caller passes
   the volume's ULID explicitly; the reaper lists
   `by_id/<vol_ulid>/retention/` under that ULID. This ULID is the
   ground truth for the run.
2. **Marker path.** For each listed marker, the reaper parses the
   volume component out of the marker's own key and asserts it equals
   the invocation ULID. A mismatch means the listing returned a key
   from a different prefix — log loudly and skip.
3. **Input ULID.** Each line of the marker body is parsed as a ULID
   via `Ulid::from_string`. The reaper reconstructs the segment key as
   `by_id/<invocation_vol>/segments/<YYYYMMDD>/<input_ulid>`. There is
   no shape an input ULID can take that escapes the invocation
   volume's prefix — the prefix is constructed from the trusted
   invocation ULID, not from anything in the marker.

A line that doesn't parse as a ULID is rejected. In particular:

- **Bounded line count.** A hard cap (e.g. 1024) on input lines
  rejects malformed or runaway markers before any delete fires.
  Realistic GC handoffs sit well below the cap; anything near it is a
  bug or tampering signal.
- **No empty lines, no whitespace, no trailing data on the line.** A
  malformed line aborts the whole marker — log loudly, skip, do not
  partially reap.

A rejected marker becomes an operator signal, not silent data loss.

This three-checkpoint flow is deliberate. A parser bug in any single
layer cannot cause cross-volume reaping on its own — the bug would
have to corrupt the invocation ULID, the marker-path parse, and the
input-line parse identically. The dominant risk we're defending
against isn't a sophisticated attacker; it's an internal bug that
misroutes a delete to the wrong volume's data. Three independent
agreements on the same ULID make that class of bug structurally hard
to trigger.

Volume scope was previously enforced via a typed `ReapTarget` enum
that pattern-matched the full S3 key. Under the simplified
ULID-per-line shape, the enum collapses to "is this a valid ULID?";
volume scope and key shape both come from the invocation. This is
strictly safer — a malformed marker can produce at worst an extra 404
DELETE under the invocation volume's prefix, never a delete in
another volume's prefix.

The validation step also defends against the narrow split-IAM threat
model from *Authenticity* above: an attacker with PutObject but not
DeleteObject who plants a marker pointing at non-segment ULIDs
produces 404s on DELETE (those keys aren't real segments), not silent
deletion of unrelated data.

### The invariant

**Every physical delete of a canonical S3 object goes through a
retention markers.** This is what lets a replica trust "present now
⟹ present for at least T." Breaking this invariant even once breaks
the replica guarantee, silently.

Whole-volume deletion is the one exception: when a volume is being
torn down entirely, a replica following it has no contract that
survives the teardown. Volume delete can drop the prefix directly.

### Authenticity

Markers are written exclusively by the volume's owning coordinator —
the same actor that already holds write authority on the prefix and
drives all S3 deletions through the GC handoff. They are unsigned;
authenticity derives from the sole-writer ACL on the prefix. An
attacker able to write a marker is by construction also able to delete
canonical objects directly, so signing would not raise the bar.

### Retention window T

T is a deployment parameter, not a hardcoded constant. Sizing depends
on:

- Realistic time to copy the upstream's post-snapshot tail across a
  WAN (seconds to hours depending on size).
- Upstream snapshot cadence — a tight cadence shrinks the tail,
  making T less load-bearing.
- Storage cost of the retained canonical bytes during T.

Default TBD. An operator knob exposed through the volume (or
coordinator) config is the right shape; the coordinator applies it
when minting markers and when reaping.

### Retention economics

A counterintuitive consequence: GC during the retention window
*defers* space reclamation rather than driving it. An input segment
compacted at time t becomes reapable at t + T, not t. Throughout
[t, t+T] the input exists alongside its compacted output, so total
storage briefly carries both.

**Steady state.** If GC consumes input bytes at rate C (bytes/sec),
there is `C × T` bytes of in-flight retained inputs at any moment.
This term is invariant to GC frequency: doubling the frequency at
the same per-batch size doubles C and the retention term; halving
per-batch size at the same frequency keeps C constant. Total storage
is approximately:

```
live_data + post_compaction_outputs + (C × T)
```

**How the knobs interact:**

| Knob | Effect on retention overhead |
|---|---|
| `retention_window` (T) | Linear. Halving T halves the in-flight term. Lower bound is whatever replicas need to catch up; below that the guarantee is meaningless. |
| `gc.density_threshold` | Indirect, via what's worth compacting. A lower threshold (compact only mostly-dead) means input bytes >> output bytes — savings dominate retention overhead. A higher threshold (compact mostly-live) means input ≈ output — retention overhead is ~2× the throughput for marginal savings. |
| `gc.interval` | Batch granularity only. Steady-state retention overhead is unchanged. |

**Tombstone handoffs are the optimal case.** A 100%-dead input
compacted to a zero-entry GC output has input bytes ≈ retention
overhead and post-T reclamation = 100% of the input. The bytes that
would have been retained anyway are retained *explicitly*, with no
new compaction-output cost added.

**The small-segment branch** of the current GC strategy
(`docs/operations.md` *Per-tick selection*) admits small segments at
any density to consolidate into 32 MiB outputs. Under retention
semantics, that pays retention overhead for marginal compaction
savings when the small segments are dense. The tradeoff is workload-
dependent — not a correctness issue, just economics.

**The intuitive "GC frees space" model is wrong** during the retention
window. Under retention, GC's role is more like a smoothing filter
between dead-byte accumulation and physical deletion: dead bytes
accumulate at rate D, GC moves them through the retention pipeline at
rate C ≥ D, the pipeline holds `C × T` bytes, and reclamation happens
at rate C lagged by T. Tuning is about balancing the pipeline depth
against the volume's tolerance for dead-byte accumulation in `live_data`.

### Naming clarification

"Tombstone" already means something specific in Elide: a zero-entry GC
output carrying only an `inputs` list, used to supersede fully-dead
segments without producing new extents (see `operations.md`). These
GC-layer tombstones are orthogonal to the S3-layer retention markers
described here. When a GC tombstone handoff retires its input
segments, the coordinator mints a retention marker for those
inputs as part of the same handoff — two different mechanisms
cooperating, not the same one named twice.

## Rejected: Tigris snapshots as the materialize read source

**Status: rejected (2026-04-24).** Tigris bucket snapshots retain full
write history with no physical deletion — storage grows monotonically
with write volume. That defeats Elide's GC / repack cost model: every
superseded segment and every GC compaction input would accrue
permanent storage cost, not bounded cost. The retention-window problem
this section tried to solve is instead addressed by application-managed
retention markers (see *What TTL buys us* and *Proposed:
application-managed retention markers* above); those are
backend-agnostic and don't bill for history in perpetuity. Kept below
for historical context.

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

## Rejected: historical materialize via Tigris versioning

**Status: rejected (2026-04-24).** This builds on the same always-on
versioning that makes Tigris bucket snapshots unbounded — the same
monotonic-growth cost problem at per-object granularity. PITR between
Elide-level snapshots is a non-goal; if it ever becomes a goal, a
longer retention window T on retention markers gives a cost-bounded
(if coarser) path. Kept below for historical context.

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

**Pending (blocked on implementation of retention markers + `materialize`):**

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
