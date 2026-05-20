# Domain-typed store layer

**Status:** Proposed.

This document fixes the cut for the "domain-typed S3 layer" sketched
as future direction in
[`design-mint.md`](design-mint.md) § *Coordinator store architecture*.
Memory `project_objectstore_trait_overreach` is the same item from the
debt side. The goal is to remove `Arc<dyn ObjectStore>` from
coordinator function signatures and replace it with operation-typed
handles that speak in elide nouns (segments, snapshots, names, events)
rather than `object_store`'s generic bytes-and-paths verbs.

The work is independent of `[mint]`: it pays off equally under
`AWS_*`.

## What is wrong with the current shape

`ScopedStores` (`elide-coordinator/src/stores.rs`) returns
credential-scoped store handles:

```rust
pub trait ScopedStores {
    fn base_ro(&self)                   -> Arc<dyn ReadStore>;      // coord-base
    fn writer(&self)                    -> Arc<dyn ObjectStore>;    // coord-writer
    fn data_for_volume(&self, v: &Ulid) -> Arc<dyn ObjectStore>;    // coord-data
}
```

`base_ro` is already narrow (the exposed-verifier boundary justified
the surface reduction). `writer` and `data_for_volume` keep the full
`object_store::ObjectStore` surface. The consequences:

- **215 `Arc<dyn ObjectStore>` occurrences across 23 files in
  `elide-coordinator/src/`** (top offenders: `recovery.rs` 28,
  `inbound/lifecycle.rs` 26, `prefetch.rs` 17, `upload.rs` 15,
  `segment_head.rs` 14, `stores.rs` 13, `inbound/mod.rs` 13).
  Every helper takes `&Arc<dyn ObjectStore>` as a parameter, so any
  narrowing — for example dropping `list` from the mint-backed write
  handle once `s3:ListBucket` leaves the role policy — cascades
  through every signature.
- **No domain vocabulary.** A call site speaks `put_opts` / `get_opts`
  / `delete` / `list_with_delimiter` / `put_multipart` even when its
  operation is specific ("put this segment body", "head this
  manifest", "delete the superseded retention marker", "advance the
  per-vol HEAD object"). The abstraction layer the coordinator owns
  ends at the credential boundary; the *operation* layer is foreign.
- **Key layout lives in `format!` calls scattered across the
  codebase** (`by_id/{vol}/segments/{date}/{ulid}`,
  `by_id/{vol}/snapshots/{date}/{snap}.manifest`,
  `events/{name}/HEAD`, `names/{name}`, `coordinators/{sub}/...`).
  Wrong-prefix keys are constructable; the IAM-layer invariant (this
  role only writes that prefix) is enforced by `debug_assert!` plus
  runtime IAM, not the type system.

The fix is to put a domain operation layer *between* the credential
boundary and the call sites, so callers receive a handle that vends
named operations and the key formatting + verb-shape decisions move
to one place per object.

## Shape: object-typed handles, vended by role

Roles still define what credentials exist. Each role hands back
**object-typed handles**; each handle exposes only the operations its
policy authorises. The role boundary stays one trait
(`DomainStores` — successor to `ScopedStores`); call sites take the
narrow handle they actually need.

```rust
pub trait DomainStores: Send + Sync {
    // coord-writer-backed (write side); coord-base-backed reads
    // inherited via the reader supertrait
    fn name_claims(&self)        -> Arc<dyn NameClaims>;
    fn event_journal(&self)      -> Arc<dyn EventJournal>;
    fn own_identity(&self)       -> Arc<dyn OwnIdentity>;

    // coord-base-backed (read only)
    fn event_journal_ro(&self)   -> Arc<dyn EventJournalReader>;

    // coord-data-backed
    fn volume_data(&self, v: &Ulid) -> Arc<dyn VolumeData>;

    // coord-base-backed
    fn control_reader(&self)     -> Arc<dyn ControlPlaneReader>;
}
```

**Read-only sub-traits for the mutate-side handles.** Each write-side
handle (`NameClaims`, `EventJournal`, `OwnIdentity`) extends a
*reader* supertrait that holds only the read methods. The mutate
trait adds the writes. Pure-read call sites take `&dyn FooReader`
and **cannot** invoke a write at the type level — the same shape
`ReadStore` vs `ObjectStore` already uses for the role boundary,
applied to the operation boundary too. Concretely for events:

```rust
#[async_trait]
pub trait EventJournalReader: Send + Sync {
    async fn read_head(&self, name: &str) -> Result<Option<(EventHead, EventHeadToken)>, EventError>;
    async fn recent(&self, name: &str, limit: usize) -> Result<Vec<VolumeEvent>, EventError>;
    async fn list_and_verify(&self, name: &str, limit: usize) -> Result<Vec<VolumeEventEntry>, EventError>;
}

#[async_trait]
pub trait EventJournal: EventJournalReader {
    async fn emit(&self, identity: &CoordinatorIdentity, name: &str, kind: EventKind, vol_ulid: Ulid) -> Result<VolumeEvent, EventError>;
    // emit_best_effort default-method on top
}
```

Why split: a read-only path needs `coord-base` (the only role whose
policy grants `coordinators/<other>/*` reads, required by
`list_and_verify`'s pubkey lookup); the emit CAS needs `coord-writer`
end-to-end (the `docs/design-mint.md` "one credential per mutation"
rule). A single handle that hides the fan-out internally
(`BucketEventJournal` holds both stores and routes reads to base,
writes to writer) is the impl shape; the trait split lets a read-only
caller carry a strictly narrower type.

The same shape applies to `NameClaims` (`read` is `coord-base`-fit;
`try_claim` / `release` / `force_release` need `coord-writer`) and
`OwnIdentity` (other coordinators' identity material is reader-only;
self-publish is writer).

Each handle is the *full* operation surface for that object class —
it does **not** further fan out into sub-handles per verb. The cut is
deliberately at the noun, not at the verb: a call site that publishes
a snapshot also writes a HEAD and may delete a superseded retention
marker, and we want those to land on one handle, not three.

`VolumeData` returns sub-accessors *only* where the object class has
internal structure with its own invariants — currently just
`segments()`, `snapshots()`, `retention()`, `head()`. These are
non-`async` accessors on the same handle, not separate `Arc`s; they
exist so callers state which sub-object they are touching.

```rust
trait VolumeData {
    fn segments(&self)  -> Segments<'_>;
    fn snapshots(&self) -> Snapshots<'_>;
    fn retention(&self) -> Retention<'_>;
    fn head(&self)      -> SegmentHead<'_>;
    fn metadata(&self)  -> VolumeMetadata<'_>;   // volume.pub, volume.provenance
}
```

The borrow is from the `VolumeData` itself; sub-accessors are
zero-cost views (they hold a `&self` reference to the parent handle
plus the `vol_ulid`).

## Object catalogue

The complete coordinator-side object surface, by handle. Each row is
"what the call sites already do today, restated as a domain op".

### `NameClaims` (`coord-writer` for mutations, `coord-base` for reads)

The `names/<name>` record carries a small state machine
(`Live`/`Stopped`/`Released`/`Readonly`) plus ownership identity.
The trait's mutation surface is the set of typed state-transition
verbs (the existing `lifecycle::mark_*` ops), not an untyped
update/overwrite. The trait deliberately exposes no general `update`
or `overwrite`: every state change folds the lifecycle invariants
(owner check, valid-transition check, idempotency) into the store
call so they are impossible to bypass at the call site.

| Op | Verb today | Domain shape | Trait |
|---|---|---|---|
| Read record | `GET names/<name>` | `read(name) -> Option<NameRecord>` | `NameClaimsReader` |
| Initial claim (writable) | `mark_initial` — `If-None-Match: *` | `mark_initial(name, coord, host, vol_ulid, size) -> MarkInitialOutcome` | `NameClaims` |
| Initial claim (readonly import) | `mark_initial_readonly` — `If-None-Match: *` | `mark_initial_readonly(name, vol_ulid, size) -> MarkInitialOutcome` | `NameClaims` |
| Stop (Live → Stopped) | `mark_stopped` — read + `If-Match` CAS | `mark_stopped(name, coord, host) -> MarkStoppedOutcome` | `NameClaims` |
| Release (Live/Stopped → Released) | `mark_released` — read + `If-Match` CAS | `mark_released(name, coord, handoff) -> MarkReleasedOutcome` | `NameClaims` |
| Force-release (unconditional → Released) | `mark_released_force` — unconditional `PUT` | `mark_released_force(name, handoff) -> ForceReleaseOutcome` | `NameClaims` |
| Resume (Stopped → Live, local) | `mark_live` — read + `If-Match` CAS | `mark_live(name, coord, host) -> MarkLiveOutcome` | `NameClaims` |
| Claim Released (cross-coord) | `mark_claimed` — read + `If-Match` CAS, rebinds vol_ulid | `mark_claimed(name, coord, host, new_vol_ulid, target_state) -> MarkClaimedOutcome` | `NameClaims` |
| In-place reclaim of own Released | `mark_reclaimed_local` — read + `If-Match` CAS, keeps vol_ulid | `mark_reclaimed_local(name, coord, host, local_vol_ulid, target_state) -> MarkReclaimedLocalOutcome` | `NameClaims` |
| Reconcile local park markers | `reconcile_marker` | `reconcile_marker(vol_dir, name, coord)` | `NameClaims` |

Same reader/writer split as `EventJournal`: pure-read sites
(`Request::ResolveName`, the read for `size` after rebind) take
`&dyn NameClaimsReader` and cannot mutate at the type level. Each
`mark_*` runs its full read-modify-write on `coord-writer` (the
"one credential per mutation" rule); the inherited `read` goes on
`coord-base`.

### `EventJournal` (`coord-writer` for emit, `coord-base` for reads)

| Op | Today | Domain shape | Trait |
|---|---|---|---|
| Read HEAD | `GET events/<name>/HEAD` | `read_head(name) -> Option<(EventHead, EventHeadToken)>` | `EventJournalReader` |
| Read recent | `GET HEAD` + walk `prev_event_ulid` via `GET` | `recent(name, n) -> Vec<VolumeEvent>` | `EventJournalReader` |
| Read + verify | recent + per-event sig check | `list_and_verify(name, n) -> Vec<VolumeEventEntry>` | `EventJournalReader` |
| Emit (mint+sign+CAS append) | CAS HEAD then `PUT events/<name>/<ulid>` | `emit(identity, name, kind, vol_ulid) -> Result<VolumeEvent, EventError>` | `EventJournal` |

There is deliberately **no `delete` method on `EventJournal`** (nor on
its reader supertrait). The `events/` append-only invariant becomes a
type-level property: a caller holding either trait cannot delete an
event because the operation does not exist. The privileged offline
reaper (if/when introduced) is a separate handle wielding an elevated
credential, not a method here.

### `OwnIdentity` (`coord-writer`, `coordinators/<sub>/...`)

The coordinator's own published identity material (pubkey, peer
endpoint records). One handle per coordinator; no per-key parameters
above `sub`.

| Op | Today | Domain shape |
|---|---|---|
| Publish pubkey | `PUT coordinators/<sub>/pubkey` | `publish_pubkey(VerifyingKey)` |
| Publish endpoint | `PUT coordinators/<sub>/peer-endpoint.toml` | `publish_endpoint(PeerEndpoint)` |
| Read others' pubkey | (cross-coord) | this is a *reader* op — see `ControlPlaneReader` |

### `VolumeData::segments()` (`coord-data`, `by_id/<vol>/segments/<date>/<ulid>`)

| Op | Today | Domain shape |
|---|---|---|
| Put body | `put_opts` with content-type, multipart for large | `put(SegmentId, body) -> Result<()>`, internally chooses single-PUT vs multipart by size |
| Get body (range) | `get_opts` with `range` | `get_range(SegmentId, Range<u64>) -> impl Stream<Bytes>` |
| Head | `head` | `head(SegmentId) -> Option<SegmentMeta>` |
| Delete | `delete` | `delete(SegmentId) -> Result<()>` |
| Pull readonly (ancestor) | `get` whole object | covered by `get_range(.., 0..size)` once `size` is known via `head` |

`SegmentId` is `Ulid` today; the parser is `Ulid::from_string`
(memory `feedback_parse_dont_validate`). The date partition is
computed inside the handle from the `Ulid` timestamp.

### `VolumeData::snapshots()` (`coord-data`, `by_id/<vol>/snapshots/`)

| Op | Today | Domain shape |
|---|---|---|
| Put manifest | `put_opts` (signed body) | `put_manifest(SnapshotId, SignedManifest)` |
| Get manifest | `get` | `get_manifest(SnapshotId) -> SignedManifest` |
| Head | `head` | `head(SnapshotId) -> Option<SnapshotMeta>` |
| Delete | `delete` | `delete(SnapshotId)` |
| Read LATEST pointer | `GET snapshots/LATEST` | `read_latest() -> Option<LatestPointer>` |
| CAS LATEST pointer | conditional `PUT` | `advance_latest(prev, new) -> Result<LatestPointer, LatestConflict>` |

### `VolumeData::retention()` (`coord-data`, `by_id/<vol>/retention/`)

| Op | Today | Domain shape |
|---|---|---|
| Record supersession | `put_opts` | `record_supersession(inputs, output, ts)` |
| Read supersessions | `GET by_id/<vol>/HEAD` | enumerated from the per-vol HEAD; no separate retention read op |
| Delete (reaper) | `delete` | `delete_supersession(ulid)` |

### `VolumeData::head()` (`coord-data`, `by_id/<vol>/HEAD`)

The post-snapshot delta from `design-segment-index.md`. Sole writer
is the per-volume tick loop; readers are warm-start claim,
recovery, fork.

| Op | Today | Domain shape |
|---|---|---|
| Read | `GET by_id/<vol>/HEAD` | `read() -> Option<HeadBody>` |
| Write | unconditional `PUT` | `put(HeadBody)` |
| Delete | `delete` | `delete()` *(volume teardown only)* |

No CAS — the document is justified in `design-segment-index.md`.

### `VolumeData::metadata()` (`coord-data`, `by_id/<vol>/volume.{pub,provenance}`)

| Op | Today | Domain shape |
|---|---|---|
| Read pubkey | `get volume.pub` | `read_pubkey() -> VerifyingKey` |
| Write pubkey | `put volume.pub` | `write_pubkey(VerifyingKey)` |
| Read provenance | `get volume.provenance` | `read_provenance() -> Provenance` |
| Write provenance | `put volume.provenance` | `write_provenance(Provenance)` |

Two fixed keys per volume — small enough to be flat methods.

### `ControlPlaneReader` (`coord-base`)

Existing `ReadStore`, retyped. The peer-fetch verifier still needs
the cross-crate `Arc<dyn ObjectStore>` escape hatch
(`peer_verifier_store()` today); that one method stays on
`DomainStores` as a deliberate exception until `elide-peer-fetch` can
depend on the domain trait.

| Op | Today | Domain shape |
|---|---|---|
| Read others' pubkey | `get coordinators/<sub>/pubkey` | `read_coord_pubkey(sub)` |
| Read peer endpoint | `get coordinators/<sub>/peer-endpoint.toml` | `read_peer_endpoint(sub)` |
| Read name (cross-coord context) | `get names/<name>` | `read_name(name) -> Option<NameClaim>` (no token; pure read) |

## Verb-shape decisions

The unavoidable design pass: how `object_store`'s generic verbs
surface as domain operations without leaking the foreign type.

**Conditional writes** become typed CAS:
- `If-None-Match: *` → `try_claim` / `try_create` — distinct verb,
  named for the intent.
- `If-Match: <etag>` → take a typed token returned by the prior
  read. `ClaimToken`, `EventHeadToken`, `LatestPointerToken`. The
  token is `#[non_exhaustive]` and carries the ETag privately; it is
  unforgeable outside the handle. A holder of the token has *read*
  this object's current state.
- Conflict surfaces as a dedicated error per object
  (`ClaimConflict`, `EventConflict`, `LatestConflict`) — not a
  generic `PreconditionFailed`. Each enum carries the variants
  callers actually distinguish (e.g. `ClaimConflict::AlreadyHeld { by
  }`, `ClaimConflict::EtagMismatch`).

**Range reads** become a streaming op:
- `get_range(id, Range<u64>) -> impl Stream<Item = Result<Bytes>>`
  where the only escaping types are `bytes::Bytes` (already an elide
  dep) and our own `StoreError`.

**Multipart** is hidden:
- `Segments::put(id, body)` chooses single-PUT vs multipart based on
  the size of `body` and the configured threshold. The threshold is
  set once at daemon startup (`StoreSection::multipart_part_size_bytes`,
  already centralised in `upload.rs`) and read by the handle impl,
  not by callers. Callers never see a `MultipartUpload` value.
- For large segments that are produced as a stream (GC compaction
  output), the handle exposes `put_streaming(id) -> SegmentWriter`
  where `SegmentWriter` is an opaque-by-default sink with `write(&mut
  self, &[u8])` and `finish(self) -> Result<()>`. The implementation
  drives `object_store`'s multipart internally.

**Content type** is per-object, not per-call:
- `manifest` → `application/octet-stream` (signed, opaque bytes),
- `peer-endpoint.toml` → `application/toml`,
- `volume.pub` → `application/octet-stream`,
- and so on. The handle sets it; callers don't pass it.

**Errors** are a small enum per handle, not `object_store::Error`.
The transport-failure tail (TCP reset, 503, …) collapses into
`StoreError::Transport(io::Error)` so callers can match the
domain-meaningful cases without a 30-arm match on
`object_store::Error`.

## Where keys live

The full S3 key layout moves into one module:
`elide-coordinator/src/domain_store/keys.rs`. Every `format!("by_id/…")`
or `format!("events/…")` in the codebase is deleted; the handle impl
calls `keys::segment(vol, id)` etc. Wrong-prefix keys become
unconstructable in call-site code because the `keys::` functions are
not public outside the `domain_store` module.

This is the structural payoff the IAM layer cannot give us: the
`debug_assert!`-on-prefix tripwires in `mint_stores.rs` become dead
code, because the *type* of the handle determines the prefix.

## Cascade containment

The new traits are introduced **alongside** `ScopedStores`, not in
place of it. `DomainStores` is implemented by the same concrete types
(`PassthroughStores`, mint-backed); call sites migrate one operation
class at a time. `writer()` and `data_for_volume()` remain until
their last caller is gone, then disappear.

This is the only viable cascade strategy at 221 occurrences: an
atomic swap would be a multi-thousand-line PR with no incremental
value. The migration is per-object:

1. `EventJournal` first — smallest surface (~3 functions), strictest
   invariant payoff (`no delete` as a type), and a known-good test
   harness (`volume_event_store.rs`). Validates the verb-shape
   choices on a low-blast-radius slice.
2. `NameClaims` — next smallest, exercises the `ClaimToken` /
   conditional-write shape that's reused everywhere.
3. `VolumeData::head()` and `VolumeData::metadata()` — fixed keys,
   tiny surface; clears the `recovery.rs` and `segment_head.rs`
   cluster.
4. `VolumeData::segments()` and `::snapshots()` — the bulk of the
   `upload.rs`, `prefetch.rs`, `gc_cycle.rs`, `inbound/lifecycle.rs`
   churn.
5. `VolumeData::retention()` — small; can land with `segments()` or
   later.
6. `OwnIdentity` and `ControlPlaneReader` — finish, then delete
   `ScopedStores` and the `Arc<dyn ObjectStore>` accessors.

Each step leaves the tree green and reduces the
`Arc<dyn ObjectStore>` parameter count strictly. No phase introduces
a dual-surface fallback (the typed handle is the only way to do
that operation once its phase lands).

## Testing

Each handle gets unit tests against an in-memory `object_store`
backend, same pattern as `volume_event_store.rs` tests today. The
trait is mockable; complex flows that today use
`object_store::memory::InMemory` directly switch to a typed in-memory
impl of the domain trait, which is strictly simpler (fewer methods,
domain-meaningful errors).

Property tests that need to cover concurrent CAS (claim races, event
append races, LATEST pointer races) become trivial to write because
the conflict cases are typed: a proptest can assert *which* variant
of `ClaimConflict` should arise from a given interleaving, not just
that *some* `PreconditionFailed` happens.

## Non-goals

- **Replacing `object_store` as the underlying crate.** The domain
  layer is built on top of `object_store`; the crate stays. If we
  later swap to `rust-s3` or hand-rolled, the domain trait is the
  seam that makes that local.
- **Per-verb roles.** A handle does not split further along verbs
  (e.g. `SegmentReader` vs `SegmentWriter`). The credential boundary
  is at the role; *within* a role, all operations a call site uses
  are vended together. Splitting further would re-introduce the
  cascade in miniature.
- **Surfacing `object_store::Path` to callers.** The path type does
  not appear in any `pub fn` signature outside `domain_store/`.
- **Touching the volume process.** This document is coordinator-side.
  The volume's S3 reads (demand-fetch) are already moving to
  `rust-s3` per `docs/architecture.md` § *Async confinement*, and
  that work has its own trait (`RangeFetcher`); it does not need
  domain typing because the volume process speaks one verb (range
  read) against one prefix (its own `by_id/<vol>/segments/`).

## Open questions

- **`peer_verifier_store()` escape hatch lifetime.** Currently a
  cross-crate workaround. Removing it requires either letting
  `elide-peer-fetch` depend on `DomainStores`, or vending the
  verifier a typed `CoordinatorPubkeyReader` trait it can satisfy.
  Decide before the final `ScopedStores` deletion.
- **GC compaction segment writes.** Multi-segment compaction output
  today uses streaming multipart. The `SegmentWriter` sink as
  proposed is fine, but the *plan* object (`design-gc-plan-handoff.md`)
  may want its own typed handle rather than reusing `Segments::put`.
  Defer until the segments migration is in flight and the shape is
  obvious from the call sites.
