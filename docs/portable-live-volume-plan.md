# Portable live volume — implementation plan

Implements [`design-portable-live-volume.md`](design-portable-live-volume.md).

**Decisions locked in 2026-04-27:**

- **Fresh buckets only.** No migration of existing `names/` entries
  or volumes. New buckets created after this lands use the new
  schema; pre-existing buckets stay on the old schema and never get
  portability features.
- **`volume remote` is a clean break.** No aliases. The new
  shape (`volume list --all`, `volume start <name>`) replaces it
  entirely.

**Decisions locked in 2026-04-28:**

- **Force-release recovers to "now", not the last handoff snapshot.**
  The recovering coordinator lists S3 segments under the dead fork's
  prefix, verifies each segment's signature against the dead fork's
  `volume.pub`, and synthesises a fresh handoff snapshot covering
  the verified set. Data-loss boundary is "writes the dead owner
  accepted but never promoted to S3" — same as the crash-recovery
  contract. Works even if the dead owner never published a snapshot.
  Closes Phase 3's prior open question on fallback semantics.
- **Coordinator identity is an Ed25519 keypair.** `coordinator.key`
  / `coordinator.pub` (mirroring `volume.key` / `volume.pub`) are
  the sole on-disk identity artefacts. `coordinator_id` derives
  from the **public** key (self-authenticating), and the macaroon
  MAC root derives from the private key in-memory at startup —
  there is no separate `coordinator.root_key` file. Existing
  deployments take a clean break: new keypair, new `coordinator_id`,
  affected volumes recover via `volume release --force`.

## How it fits with existing `volume stop`/`volume start`

Today (`elide-coordinator/src/inbound.rs:1627–1710`), `volume
stop`/`volume start` are **local-only lifecycle** verbs: they bring
the volume daemon up/down on this host, gated by a `volume.stopped`
marker file. No S3 mutation, no ownership transfer.

This plan generalises the verbs without renaming them:

- For a name this coordinator owns (`names/<name>.coordinator_id ==
  self`), `stop`/`start` do today's local up/down — plus, on `stop`,
  drain WAL → publish handoff snapshot → release the name in S3.
- For a name held by another coordinator, `start` triggers the
  fork-on-claim path: read `names/<name>`, mint a fresh ULID and
  keypair, create a local fork directory with provenance pointing at
  the previous fork's handoff snapshot, conditionally claim the name.
- The `volume.stopped` marker becomes a local cache of
  `names/<name>.state`. Consistency rule: on coordinator startup,
  reconcile each local volume directory against `names/<name>` —
  if the marker disagrees, the S3 record wins.

## Phases

The phases are sequenced for shippability: each phase ends with a
working tree where existing functionality is preserved. Phases 0 and
1 are foundational; Phases 2–4 are the user-visible work; Phase 5
is consolidation.

---

### Phase 0 — Foundations

Internal additions; no behaviour change. **Landed** in
`elide-coordinator/src/portable.rs`.

- [x] **Conditional PUT helpers.** `put_if_absent(store, key, body)`
  and `put_with_match(store, key, body, expected)` wrap
  `ObjectStore::put_opts` with `PutMode::Create` and
  `PutMode::Update` respectively. The wrappers translate
  `Error::AlreadyExists` and `Error::Precondition` into a typed
  `ConditionalPutError::PreconditionFailed`. *Note:* `object_store`
  0.11 already exposes conditional PUT on the trait, so this is a
  thin typed-error wrapper rather than a downcast/extension layer.
- [x] **Bucket capability probe.** `probe_capabilities(store,
  probe_key)` does a `Create` then a second `Create` and reads
  `AlreadyExists` as "supports conditional PUT". A LWW backend that
  silently overwrites is detected by the second `Create` succeeding.
  Returns `BucketCapabilities { conditional_put: bool }`. Idempotent.
- [x] **`coordinator_id()` derivation.**
  `coordinator_id(&root_key)` returns
  `blake3::derive_key("elide coordinator-id v1", &root_key)`.
  `format_coordinator_id(&id)` formats the lower 16 bytes as a
  ULID-shaped Crockford-Base32 string (26 chars).
- [x] **Unit tests:** 10 tests in `portable::tests` covering
  determinism, domain separation, formatting round-trip, conditional
  PUT happy/sad paths, and probe idempotency. All against
  `InMemory` (the object_store reference impl).
- [ ] **Open:** test the probe's "not supported" path against a
  hand-rolled LWW mock impl of `ObjectStore`. The trait surface is
  large (~10 methods) so this is deferred until we have an actual
  non-conformant backend in CI to validate against. Real-bucket
  validation against Tigris/MinIO lives in Phase 5.
- [x] **`coordinator_id()` wired to `daemon::run()`.** Logged at
  startup (`info!("[coordinator] coordinator_id: {id}")`) and
  threaded through `IpcContext` for inbound handlers; landed as
  part of Phase 1.5 (`d1a9364`).

**Phase exit criteria:** met. New APIs land in `portable` module;
no caller uses them yet; full coordinator builds clean and the new
tests pass.

---

### Phase 1 — `names/<name>` schema

Promote `names/<name>` from a plain-text ULID to structured TOML:

```toml
version = 1
vol_ulid = "<current_fork_ulid>"
coordinator_id = "<owner-coordinator-id>"        # optional in Phase 1
state = "live"                                    # "live" | "stopped" | "released" | "reserved" | "readonly"
parent = "<prev_ulid>/<prev_snap_ulid>"           # absent on the root
claimed_at = "<rfc3339>"                          # optional in Phase 1
hostname = "<owner-host-at-claim-time>"           # advisory only
```

`state` is **five-valued** (see design doc § "Five states, three
intents"):

- `live` — held by `coordinator_id`, daemon serving.
- `stopped` — held by `coordinator_id`, daemon down on this host.
  Other coordinators cannot claim until the name is released
  (`volume release --force` from another host).
- `released` — no current owner; any coordinator may
  `volume start --remote`. `coordinator_id`, `claimed_at`, and
  `hostname` are cleared on release.
- `reserved` — released to a specific coordinator (`--to` flag).
  `coordinator_id` carries the intended claimer; `claimed_at` and
  `hostname` stay empty. Only the named coordinator may
  `volume start --remote`; others refuse before the conditional PUT.
  Closes the post-release race window for targeted handoff.
- `readonly` — name points at immutable content (e.g. an imported
  OCI image). No exclusive owner, no daemon to start; lifecycle
  verbs refuse it cleanly. Multiple coordinators may pull and
  serve the same name concurrently. See design doc § "Readonly
  names".

Phase 1 scope:

- [x] Define a `NameRecord` type in `elide-core::name_record` with
  parse/serialise (TOML), explicit `version` field, and a
  `NameState` enum covering `Live` / `Stopped` / `Released`.
  `from_toml` rejects unknown versions; `live_minimal()` constructor
  for Phase 1 writers.
- [x] Replace `names/<name>` readers in `src/main.rs` (`remote_list`,
  `resolve_pull_spec`) to parse `NameRecord` and extract
  `record.vol_ulid` instead of treating the body as plain ULID text.
- [x] Update `upload_volume_metadata` in
  `elide-coordinator/src/upload.rs` to write
  `NameRecord::live_minimal(vol_ulid)` serialised as TOML with
  content-type `application/toml`. Optional fields
  (`coordinator_id`, `claimed_at`, `hostname`) stay unpopulated;
  Phase 2 lifecycle verbs will populate them on state transitions.
- [x] Enable `ulid` `serde` feature in `elide-core/Cargo.toml` so
  `Ulid` round-trips through TOML directly.
- [x] 7 unit tests in `name_record::tests` cover round-trip
  (minimal + full), each `NameState` variant, version rejection,
  malformed TOML, missing required fields, and lowercase wire
  format.

**Phase exit criteria:** met. `names/<name>` reads and writes go
through `NameRecord`; bucket schema is fresh-bucket-only — old
plain-text records are not parseable and are not migrated.

---

### Phase 1.5 — Coordinator identity keypair

Replace the symmetric `coordinator.root_key` foundation with an
Ed25519 keypair held on the coordinator host. Required by Phase 3
(synthesised handoff snapshots are signed by `coordinator.key`).
Also retrofits Phase 0's `coordinator_id` derivation to derive from
the public key.

- [x] **Generate / load `coordinator.key` + `coordinator.pub`.**
  Landed in `elide-coordinator/src/identity.rs` (`d1a9364`).
- [x] **Re-root `coordinator_id` on the public key.**
- [x] **Derive macaroon root from the private key in-memory.**
- [x] **Publish `coordinator.pub` to S3 on startup**
  (`identity::publish_pub`).
- [x] **Pubkey fetcher** (`identity::fetch_coordinator_pub`) —
  reads the bucket pub, recomputes `coordinator_id` from the bytes
  and refuses on mismatch.
- [x] **Migration / clean-break handling** — on first start of new
  code, the coordinator generates `coordinator.key` /
  `coordinator.pub` if absent, derives a new `coordinator_id` from
  the new pub, and stops trusting any existing
  `coordinator.root_key` file.
- [x] **Tests** — round-trip, derivation determinism across
  restarts, pub publish idempotency, tampered-pub rejection.

**Phase exit criteria:** every coordinator has a verifiable Ed25519
identity reachable from the bucket. `coordinator_id` is
self-authenticating against the published pub. The legacy
`coordinator.root_key` file is no longer consulted.

---

### Phase 2 — Lifecycle verbs (cross-coordinator)

This is where portability becomes real. Gate the new verb behaviour
on the bucket-capability probe from Phase 0 — see Phase 4.

#### `volume stop` (local stop, retain ownership) — landed

- [x] Refuse if a client (NBD/ublk) is connected.
- [x] Issue the existing `shutdown` RPC: WAL fsync, daemon halts.
- [x] Conditional PUT to `names/<name>` flipping `state` from `live`
  to `stopped` via `lifecycle::mark_stopped`. `vol_ulid` and
  `coordinator_id` unchanged; backfills the latter (and
  `claimed_at`, `hostname`) on Phase 1 records that left these
  unset. **No handoff snapshot.**
- [x] Local artefacts stay in place.

#### `volume release` (relinquish ownership) — landed

- [x] Refuse if a client is connected, the volume is readonly,
  already stopped, or has no S3 record yet.
- [x] Verify ownership in S3 *before* the expensive drain.
- [x] Drain WAL → publish handoff snapshot via the existing
  `snapshot_volume` path; capture the snapshot ULID.
- [x] Halt the daemon (write `volume.stopped`, send `shutdown`).
- [x] Conditional PUT to `names/<name>` via
  `lifecycle::mark_released`: `state = "released"`,
  `handoff_snapshot = <snap_ulid>`, `coordinator_id` preserved as
  historical.
- [x] CLI: `elide volume release <name>`.
- [x] `volume stop --release` convenience composition (landed in
  `41f73d3`; the `--to <coord_id>` form composes with `--release` for
  targeted handoff).

#### `volume start <name>` — claim ownership

`volume start` is always safe: it never overrides another
coordinator. Defaults to **local-only**; the bucket is only
consulted when `--remote` is passed. The override path lives on
`volume release --force` (Phase 3), not on `start`.

- [x] **Local-resume path** — `state == "stopped"` and
  `coordinator_id == self`. `lifecycle::mark_live` flips `state` back
  to `live`; existing local-restart logic clears the marker and
  notifies the supervisor. No new ULID, no fork, no snapshot.
- [x] **Already-live path** — idempotent OK if record says we
  already own it as Live (covers daemon-restart races).
- [x] **In-place reclaim** — `state == "released"` and the released
  `vol_ulid` matches a local fork still on this host;
  `mark_reclaimed_local` flips back to `live` keeping the same ULID.
- [x] **Refusal paths** — foreign-owner records refuse with a
  pointer at `volume release --force` (Phase 3).
- [x] **`start --remote` against `Reserved` records.** When
  `start_volume_op` reads a `Reserved` record with no local fork:
  if `coordinator_id == self`, route through the same claim flow
  as `Released` (returns `released <vol_ulid> <snap>` so the CLI
  uses the existing `claim_released_name` path). The
  `mark_claimed` lifecycle helper's `Reserved → Live` arm
  (cb6b099) handles the actual flip atomically; non-target
  claimants get `OwnershipConflict { held_by: <target> }` before
  the conditional PUT. Foreign-target Reserved records refuse with
  a clear error naming the intended claimer.
- [x] **Claim-from-released path** — `state == "released"`,
  no local data. Currently runs on bare `volume start`; **needs to
  move behind `--remote`**. Coordinator's `start` op returns
  `released <vol_ulid> <snap_ulid>`; the CLI orchestrates the claim:
  1. Pull the released ancestor if not local (via existing
     `remote_pull`).
  2. Mint a fresh local fork via the existing `fork-create` IPC.
  3. Issue new `claim <name> <new_vol_ulid>` IPC →
     `lifecycle::mark_claimed` does a conditional PUT rebinding
     `names/<name>` to the new fork.
  Concurrent claims resolve cleanly via the conditional-PUT race;
  the loser's local fork is left as a usable orphan with a clear
  error message.
- [x] **Stale-symlink recovery for re-claim** — `claim_released_name`
  inspects an existing `by_name/<name>` and removes it when it
  points at the released ancestor (we previously owned and
  released this name) or is dangling. Refuses cleanly when the
  symlink targets an unrelated local ULID.
- [x] **Gate claim-from-released behind `--remote`.** Bare
  `volume start <name>` with no local data refuses with
  `volume 'mydb' not found locally; to claim it from the bucket,
  run: elide volume start --remote mydb`. The `--remote` flag is
  parsed in the IPC verb (`start <name> [--remote]`) and only when
  set does `start_volume_op` reach into S3 to read `names/<name>`.
  Without it, the coordinator never touches the bucket — surprise
  S3 pulls are now operator-explicit.

#### Other Phase 2 work

- [x] **`volume.stopped` marker rule.** `lifecycle::reconcile_marker`
  runs in the coordinator's discovery pass (`daemon.rs`):
  `state == Stopped` and marker absent → write the marker;
  `state == Live` and marker present → remove. Best-effort, scoped to
  records this coordinator owns, ignores foreign-owned records.
- [x] **Inbound-op composer tests.** 11 unit tests for the IPC
  verb dispatchers — `force_release_volume_op` (5),
  `claim_volume_op` (4), `start_volume_op --remote` routing (7) —
  exercising the composition of `recovery::` + `lifecycle::` +
  `name_store::` end-to-end against `InMemory`.
- [x] **Two-coordinator state-machine proptest** at
  `elide-coordinator/tests/portable_proptest.rs`: random sequences
  drawn from `{Create, Release, ReleaseTo, ForceRelease,
  ClaimReleased}` between two simulated coordinators sharing an
  `InMemory` bucket. 256 cases. Asserts six bucket-level invariants
  including signature verification of every handoff snapshot
  (volume.pub for normal manifests, recovering coordinator's
  `coordinator.pub` for synthesised handoff snapshots).
- [ ] **Real-bucket integration test** exercising stop-on-A →
  start-on-A (local resume) and release-on-A → start-on-B
  (cross-coordinator) against Tigris in CI. Phase 5.

**Phase exit criteria:** a name can move cleanly between two
coordinators when explicitly released, and stays put across a stop
on its current owner. Crash recovery for the same coordinator still
works as today.

---

### Phase 3 — Force takeover and targeted handoff

The override path is split into two normal verbs:

1. `volume release --force <name>` — unconditionally flip foreign
   `live`/`stopped` records to `released`. Synthesises a fresh
   handoff snapshot covering all S3-visible signature-valid segments
   under the dead fork's prefix, signed by the recovering
   coordinator's `coordinator.key`. No drain (the dead owner's WAL
   is unreachable).
2. `volume start --remote <name>` — claim the now-released name
   through the standard conditional-PUT path. Verifies the
   synthesised handoff snapshot's signature against the recovering
   coordinator's published pubkey before forking from it.

This keeps `volume start` always-safe (never overrides another
coordinator) and makes the dangerous step explicit and auditable.

`--to <coordinator_id>` adds a targeted-handoff variant that closes
the post-release race window: instead of `released` (anyone may
claim), the record goes to `reserved` (only the named coordinator
may claim). Composes with `--force`.

- [x] **`Reserved` state in `NameRecord`.** New variant in
  `NameState`; round-trip + lowercase wire tests landed.
  `mark_released_to(name, releasing_coord, target_coord, snap)`
  flips `Live`/`Stopped` → `Reserved` with the target written into
  `coordinator_id` (intended claimer). `mark_claimed` gains a
  `Reserved → Live` arm gated on `coordinator_id == self`; foreign
  claimants surface `OwnershipConflict { held_by: <target> }` before
  the conditional PUT, closing the post-release race window.
  Reserved is refused cleanly by `mark_stopped`, `mark_live`, and
  `mark_reclaimed_local`. Reader changes in `start`/`status`
  deferred to the `--remote` wiring task.
- [x] **Synthesised handoff snapshot record shape.** Extended the
  snapshot manifest format in `elide-core::signing` with three optional
  fields populated only on the synthesised path:
  - `synthesised_from_recovery: true`
  - `recovering_coordinator_id: <coord_id>`
  - `recovered_at: <rfc3339>`

  `write_snapshot_manifest` gained an `Option<&SnapshotManifestRecovery>`
  parameter; `read_snapshot_manifest` now returns a `SnapshotManifest`
  struct carrying `segment_ulids` plus optional `recovery`. The signing
  input is **domain-separated** when recovery metadata is present —
  `"elide-snapshot-recovery-v1\0"` prefix + `coord_id\0recovered_at\0\0` —
  so a non-recovery sig can't validate a manifest mutated to claim
  recovery, and vice versa. 5 new tests cover round-trip, empty
  segments, cross-class tamper, stripped-fields tamper, and partial
  metadata. Existing 4 writers and 3 readers updated. Tooling display
  of the flag is deferred to the verification-on-`start --remote`
  task.
- [x] **Segment-listing replay.** Landed in
  `elide-coordinator/src/recovery.rs`. `list_and_verify_segments`
  lists `by_id/<vol_ulid>/segments/`, range-fetches each segment's
  `header + [0, body_section_start)` (matching `prefetch::fetch_idx`),
  and verifies the Ed25519 signature against the dead fork's
  `volume.pub` via `elide_core::segment::verify_segment_bytes`.
  Returns `RecoveredSegments { segments: Vec<VerifiedSegment>,
  dropped: usize }` where `VerifiedSegment` carries
  `(segment_ulid, etag, size)` and the list is sorted by ULID.
  Segments failing verification (bad magic, invalid signature,
  truncated, foreign-signed) are dropped with a per-segment
  `warn!`. Non-ULID keys (stray `.tmp` uploads) are silently
  skipped, not counted as drops. `fetch_volume_pub` is the
  companion helper for pulling the dead fork's pubkey from S3.
  8 unit tests against `InMemory`.
- [x] **Synthesise + sign the handoff snapshot.** Landed as
  `recovery::mint_and_publish_synthesised_snapshot`. Mints a fresh
  ULID, builds the manifest via the new
  `elide_core::signing::build_snapshot_manifest_bytes` (extracted
  from `write_snapshot_manifest` so callers can publish the bytes
  somewhere other than a local volume directory), signs with the
  caller's `SegmentSigner` (the recovering coordinator's
  `coordinator.key`), and conditionally PUTs to
  `by_id/<dead_vol_ulid>/snapshots/YYYYMMDD/<snap>.manifest` via
  `portable::put_if_absent`. Returns
  `PublishedSynthesisedSnapshot { snap_ulid, key }` on success;
  `PublishSnapshotError::AlreadyExists { key }` on the (vanishingly
  improbable) ULID-collision path. 3 new tests verify the round
  trip via the existing `read_snapshot_manifest`, cross-class key
  rejection, and the conditional-create precondition.
- [x] **`--force` flag on `volume release`.** Wired through the
  CLI (`volume release <name> --force`), the IPC verb (`release
  <name> --force`), and a new `force_release_volume_op` in
  `inbound.rs`. The op composes the existing recovery primitives:
  read current `names/<name>` for the dead `vol_ulid`, fetch the
  dead fork's `volume.pub`, run `list_and_verify_segments`, mint
  the synthesised handoff snapshot via
  `mint_and_publish_synthesised_snapshot` (signed with the local
  `CoordinatorIdentity`, which now `impl SegmentSigner`), then
  unconditionally rewrite `names/<name>` via
  `lifecycle::mark_released_force` (built on a new
  `name_store::overwrite_name_record` helper). No local symlink,
  drain, or daemon manipulation. 7 new lifecycle tests cover the
  force/force-to transitions including `Reserved`/`Released`/`Readonly`
  refusal arms.
- [x] **`--to <coordinator_id>` flag on `volume release`.** Wired
  through the CLI (`volume release <name> --to <id>` and
  `volume stop --release --to <id>`), the IPC verb (`release <name>
  --to <id>`), and a new `release_to_volume_op` in `inbound.rs`.
  The drain → snapshot → halt path is shared with bare `release`
  via a refactored `release_with_final_flip` helper; only the final
  state-flip step differs (`mark_released_to` vs `mark_released`).
  Composes with `--force` for the unreachable-owner case, where the
  unconditional rewrite produces a `Reserved` record bound to the
  named target.
- [x] **Synthesised-snapshot verification on `start --remote`.**
  Three layers:
  - `elide_core::signing::peek_snapshot_manifest_recovery` and
    `read_snapshot_manifest_from_bytes` — bytes-based helpers so
    callers can detect synthesis and verify without going via a
    file. 5 new tests cover round-trip, peek, and wrong-key rejection.
  - `recovery::resolve_handoff_verifier(store, vol_ulid, snap_ulid)`
    — fetches the manifest, peeks for recovery metadata, fetches
    the recovering coordinator's `coordinator.pub` via the existing
    `identity::fetch_coordinator_pub` (which path-binds the pub to
    its derived id), and re-verifies the signature under that pub.
    Returns `HandoffVerifier::Normal` or `Synthesised { ..,
    manifest_pubkey }`. 4 new tests including pub-missing and
    manifest-missing refusal paths.
  - `resolve-handoff-key` IPC verb + `coordinator_client::resolve_handoff_key`
    + `claim_released_name` plumbing: when the resolver returns
    `Recovery { manifest_pubkey_hex }`, the CLI passes it to
    `fork-create` as `parent-key=<hex>`. The new fork's open-time
    ancestor walk (`block_reader.rs`) then verifies the synthesised
    manifest under the recovering coordinator's pubkey via the
    existing `parent_manifest_pubkey` provenance field.

  `identity` module promoted to the library (`pub mod identity` in
  `lib.rs`) so `recovery` can call `fetch_coordinator_pub`. Callers
  in binary-only files updated from `crate::identity::...` to
  `elide_coordinator::identity::...`.
- [x] **Tests at the helper / inbound-op layer:** tampered-segment
  drop (`force_release_op_drops_tampered_segment_but_succeeds`);
  `release --to <X>` then `start --remote` on X
  (`start_remote_against_reserved_for_self_returns_claim_pin`) and
  on Y (`start_remote_against_reserved_for_other_refuses`,
  `claim_op_refuses_reserved_for_another_coordinator`);
  synthesised-snapshot mismatched-pubkey and missing-pub refusal
  paths covered at the `recovery::resolve_handoff_verifier` layer;
  empty-segment-list recovery exercised in `recovery::tests` and
  the two-coordinator proptest.
- [ ] **Process-level recovery test:** force-release after a real
  simulated coordinator death (kill the writing process between
  segment uploads and `names/<name>` rewrite, then
  `release --force` + `start --remote` from a second coordinator).
  Belongs alongside the kernel-lane CI tests; heavier than the
  inbound-op coverage above.

**Phase exit criteria:** an operator can recover a name from a dead
coordinator and the recovered fork includes every segment the dead
owner successfully promoted to S3. The synthesised handoff snapshot
is independently verifiable from bucket-public material.

---

### Phase 4 — CLI surface unification

The "feels like magic" UX work. Depends on Phase 0–3.

- [x] **`volume list` stays local.** Already implemented this way:
  reads `by_name/` and (with `--all`) `by_id/`, never reaches S3.
  Columns are `name`, `vol_ulid`, `mode`, `state`, `transport`,
  `pid` per the existing shape.
- [x] **`volume status <name>` gains `--remote`.** Local-only by
  default; with `--remote` the new `status-remote` IPC verb fetches
  `names/<name>` from the bucket and returns a TOML body carrying
  `state`, `vol_ulid`, `coordinator_id`, `hostname`, `claimed_at`,
  `parent`, `handoff_snapshot`, plus an `eligibility` field
  (`owned` / `foreign` / `released-claimable` / `reserved-for-self`
  / `reserved-for-other` / `readonly`) computed against this
  coordinator's id. Absent records surface a clear error before any
  parsing. 6 new unit tests against `InMemory` cover each
  eligibility class plus the absent-name path.
- [x] **Hard-remove `volume remote`.** Deleted the `Remote` subcommand
  and `RemoteCommand` enum from `src/main.rs`, the dispatch arm, and
  `remote_list`. **`remote_pull` is retained** — it is the canonical
  helper used by `fork --from` (`FromSpec::ExplicitPin` /
  `FromSpec::BareUlid` / `FromSpec::Name`) and the
  `claim_released_name` path; deleting it would break shipped
  functionality. Doc surface updated in `docs/operations.md`,
  `docs/architecture.md`, `docs/formats.md` (the `volume remote list`
  / `volume remote pull` rows are replaced by `volume status --remote`
  and `volume start --remote` respectively).
- [ ] **Error path for non-portable buckets.** When `volume start
  <name>` is invoked against a name not held by this coordinator,
  *and* the bucket-capability probe says no conditional PUT,
  refuse with the documented error pointing the user at `volume
  create --from <vol_ulid>/<snap_ulid>`. (Phase 0 wiring; surfaced
  here.)
- [ ] **Tests:** golden-file tests for `volume list` (local) and
  `volume status --remote` output; integration test for the
  non-portable-bucket error path.

**Phase exit criteria:** the CLI is unified. `volume remote` no
longer exists. `volume list` is fast and offline; per-name S3
lookups go through `volume status --remote`.

---

### Phase 5 — Tests, docs, status

- [ ] **End-to-end proptest.** Random sequences of {`create`,
  `stop`, `start`, `release`, `release --force`, simulated crash}
  across two simulated coordinators sharing a fake bucket; assert
  invariants (single live owner per name; chain validity; provenance
  signatures verify; no orphan ULIDs in `names/`).
- [ ] **Real-bucket integration test.** Tigris-backed test exercising
  stop-on-A, start-on-B, repeated migration, force-release recovery.
  Probably gated by `ELIDE_S3_BUCKET` env var, similar to existing
  store tests.
- [ ] **Documentation updates.**
  - `docs/architecture.md`: replace the "name lives in `by_name/`"
    section with the new authoritative-`names/` model.
  - `docs/operations.md`: add a "moving a volume between hosts"
    runbook (`stop` on A → `release` on A → `start --remote` on B;
    `release --force` semantics for unreachable owners).
  - `docs/quickstart-tigris.md`: add a section showing two-host
    migration end to end.
  - `docs/status-2026-MM-DD.md`: a new status doc capturing the
    landed work.
- [ ] **Memory updates.** Update `project_volume_naming.md` to note
  the concern is resolved by the portable-volume work; remove or
  archive `project_next_step.md` references to `volume stop/start`
  pending work (now superseded).

**Phase exit criteria:** documentation and tests reflect the landed
state.

---

## Cross-cutting open items

These don't block any single phase but want a tracked answer before
the work fully closes:

1. **Garbage collection across migration-driven fork chains.** The
   existing GC and dedup paths handle ancestry; whether they handle
   the *traffic pattern* of frequent migrations (long thin chains
   with small per-fork tails) is worth a focused look. Probably a
   real exercise in Phase 5 testing.
2. **`volume materialize` ergonomics for chain compaction.** Already
   exists (`design-replica-model.md`) but is "deferred". Frequent
   migrations will accelerate the case for landing it; track
   separately.
3. **Bucket-capability probe failure modes.** If the probe itself
   fails (transient network error, unrelated S3 error), do we fail
   closed (refuse portable verbs) or fail open? Probably fail closed
   with a clear "couldn't determine bucket capabilities" error.
   Decide before Phase 4 ships.
4. **Coordinator pubkey caching / refresh.** The
   `coordinators/<coord_id>/coordinator.pub` write is one-shot at
   coordinator startup; readers fetch fresh on each verification.
   Decide whether claimants should cache pubs locally and how often
   they refresh. Not blocking — fresh-fetch on every verification
   is correct and the cost is one small GET per `start --remote`
   against a synthesised snapshot.
5. **~~ublk volume-open retry.~~ Done.** ublk now goes through
   `crate::volume_open::open_volume_with_retry` like NBD. That helper
   was also extended with a synchronous coordinator handshake
   (`await-prefetch <vol_ulid>` IPC) so both transports block on actual
   prefetch completion — a strong signal — instead of just retrying on
   the symptom (NotFound during open). The retry loop is kept as a
   second line of defence for untracked forks and filesystem-visibility
   latency. Volume-side budget is 60s with a clear timeout error.
