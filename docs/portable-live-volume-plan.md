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
- [ ] **Open:** wire `coordinator_id()` into `daemon::run()` so
  diagnostic output (`elide coordinator status` or similar) can show
  the id. Pure plumbing; deferred until a caller needs it.

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

- [ ] **Generate / load `coordinator.key` + `coordinator.pub`.** New
  module `elide-coordinator/src/identity.rs` (or extend
  `credential.rs`): on first start, generate a fresh Ed25519 keypair
  with `ed25519_dalek::SigningKey::generate`, write
  `<data_dir>/coordinator.key` (mode 0600) and
  `<data_dir>/coordinator.pub` (mode 0644). On subsequent starts,
  load from disk and verify the pub matches the key.
- [ ] **Re-root `coordinator_id` on the public key.** Change
  `portable::coordinator_id` to take the pubkey bytes and derive via
  `blake3::derive_key("elide coordinator-id v1", &pub_bytes)`.
  Update `format_coordinator_id` callers; update unit tests.
- [ ] **Derive macaroon root from the private key in-memory.**
  In `daemon::run`, replace `load_or_generate_root_key` with
  `load_or_generate_keypair` + an in-memory derivation:
  `let macaroon_root = blake3::derive_key("elide macaroon-root v1",
  &priv_bytes);`. The 32-byte result feeds the existing
  `macaroon::mint` / `macaroon::verify` callers unchanged.
- [ ] **Publish `coordinator.pub` to S3 on startup.** Conditional
  PUT to `coordinators/<coordinator_id>/coordinator.pub` using the
  `put_if_absent` helper from Phase 0. If a record already exists,
  read it and compare against the local pub: equal → no-op, mismatch
  → fail startup with a clear error (different coordinator already
  claimed this id, which only happens if `coordinator_id` derivation
  collides — vanishingly improbable for a 32-byte derivation, but
  fail loudly rather than silently overwrite).
- [ ] **Pubkey fetcher.** `fetch_coordinator_pub(store,
  coordinator_id) -> Result<VerifyingKey>` reads the bucket pub,
  verifies the embedded pub really derives to that `coordinator_id`
  (recompute and compare), returns the `VerifyingKey`. Refuse on
  mismatch.
- [ ] **Migration / clean-break handling.** On startup, if
  `coordinator.root_key` exists but `coordinator.key` does not,
  generate the new keypair and **leave** the old root_key file in
  place untouched (it's no longer read). Log a one-line warning
  noting the file is now ignored. The new `coordinator_id` will
  differ from any old one — Phase 2/3 lifecycle verbs handle stale
  pointers via `release --force`.
- [ ] **Tests:** keypair round-trip; `coordinator_id` derives
  identically across restarts of the same install; pub publishing
  is idempotent; pubkey fetcher rejects a tampered pub whose
  derivation doesn't match the path; macaroon mint/verify still
  work end-to-end against the in-memory-derived root.

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
- [ ] `volume stop --release` convenience composition (deferred to
  a follow-up commit; the two underlying verbs already exist).

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
- [ ] **Gate claim-from-released behind `--remote`.** Bare
  `volume start <name>` with no local data refuses with
  `volume 'mydb' not found locally; to claim from bucket, run: elide
  volume start --remote mydb`. The `--remote` flag opts into the
  S3 path above.

#### Other Phase 2 work

- [ ] **`volume.stopped` marker rule.** On coordinator startup,
  reconcile each local volume directory against `names/<name>`. If
  the local marker disagrees with S3, S3 wins; update the marker to
  match.
- [ ] **Tests:** unit tests for each state transition (15 lifecycle
  tests landed); integration test exercising stop-on-A → start-on-A
  (local resume) and release-on-A → start-on-B (cross-coordinator)
  against a real bucket (Tigris in CI); proptest covering
  interleaved stop/start/release/concurrent-claim sequences.

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
- [ ] **Segment-listing replay.** New helper in
  `elide-coordinator/src/recovery.rs` (or similar): given a dead
  fork's `vol_ulid`, list all segment objects under
  `by_id/<vol_ulid>/...`, fetch each segment's header + index
  section (not the body), verify the Ed25519 signature against the
  dead fork's `volume.pub` (also fetched from S3), and return a
  sorted-by-ULID list of `(segment_ulid, etag, segment_size)` for
  segments that pass verification. Segments failing signature check
  are dropped with a per-segment warning.
- [ ] **Synthesise + sign the handoff snapshot.** Mint a fresh
  ULID, build the snapshot record naming the verified segment set
  with the recovery metadata fields populated, sign with
  `coordinator.key`, write to
  `by_id/<dead_vol_ulid>/snapshots/<new_snap_ulid>` via conditional
  PUT (the path is fresh, so `put_if_absent`).
- [ ] **`--force` flag on `volume release`.** Skip the foreign-owner
  refusal. Run segment-listing replay → mint synthesised handoff
  snapshot → unconditional Overwrite of `names/<name>` flipping to
  `released` with the synthesised snapshot ULID recorded in
  `handoff_snapshot`. Do **not** drain the previous owner's WAL.
- [ ] **`--to <coordinator_id>` flag on `volume release`.** Same
  drain + handoff path as bare `release`, but writes
  `state=reserved` with `coordinator_id=<X>` instead of `released`
  with cleared identity. Composes with `--force` (which then skips
  the foreign-owner check too and uses the synthesised snapshot
  path). `mark_claimed` gains a `Reserved → Live` arm that requires
  `coordinator_id == self`; refusal for foreign-target reservations
  happens before the conditional PUT, with an error naming the
  intended claimer.
- [ ] **Synthesised-snapshot verification on `start --remote`.**
  When a `released` / `reserved` record's `handoff_snapshot` points
  at a snapshot record carrying `synthesised_from_recovery=true`,
  the claiming coordinator: (i) fetches the recovering coordinator's
  pubkey from `coordinators/<recovering_coordinator_id>/coordinator.pub`,
  (ii) verifies the snapshot's Ed25519 signature against it,
  (iii) recomputes `coordinator_id` from the fetched pub and confirms
  the path matches. Any failure refuses the claim with a clear error.
  Non-synthesised handoff snapshots use the existing per-volume
  signature path unchanged.
- [ ] **Tests:** force-release after simulated coordinator death
  (kill the writing process between segment uploads and
  `names/<name>` rewrite, then `release --force` + `start --remote`
  from a second coordinator); force-release when the dead fork
  never published a snapshot (segment-listing replay covers it);
  force-release skips a tampered segment whose signature fails
  verification; `release --to <X>` followed by `start --remote` on
  X (succeeds) and on Y (refused before conditional PUT);
  `start --remote` against a synthesised snapshot signed by a
  pubkey whose derivation doesn't match the bucket path (refused);
  `start --remote` against a synthesised snapshot whose signing
  coordinator's pub is missing from the bucket (refused with a
  clear error).

**Phase exit criteria:** an operator can recover a name from a dead
coordinator and the recovered fork includes every segment the dead
owner successfully promoted to S3. The synthesised handoff snapshot
is independently verifiable from bucket-public material.

---

### Phase 4 — CLI surface unification

The "feels like magic" UX work. Depends on Phase 0–3.

- [ ] **`volume list` stays local.**
  - View: names this coordinator owns or has materialised data for.
    Never lists `names/` in S3 — bucket may hold thousands of names
    and unbounded enumeration is not a useful default.
  - `--all` keeps its existing local-only meaning (include ancestor
    forks); it does **not** reach S3.
  - Columns: `name`, `vol_ulid`, `mode`, `state`, `transport`,
    `pid` (current shape).
- [ ] **`volume status <name>` gains `--remote`.**
  - Default: local — what this coordinator knows about `<name>`.
  - `--remote`: fetches `names/<name>` from S3 and prints the
    authoritative record (`vol_ulid`, `state`, `coordinator_id`,
    `hostname`, `claimed_at`) plus eligibility for this
    coordinator. Errors clearly if `<name>` is not present in the
    bucket.
- [ ] **Hard-remove `volume remote`.** Delete the `Remote`
  subcommand and `RemoteCommand` enum from `src/main.rs`. Delete
  `remote_list` and `remote_pull` helpers and their callers. Remove
  the `volume remote` documentation surface. Pre-existing scripts
  break — clean break per decision 2.
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
