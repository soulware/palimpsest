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
- **Force-takeover fallback semantics stay open** — see Phase 3 and
  related recovery questions about implicit "now" snapshots.

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
state = "live"                                    # "live" | "stopped" | "released" | "readonly"
parent = "<prev_ulid>/<prev_snap_ulid>"           # absent on the root
claimed_at = "<rfc3339>"                          # optional in Phase 1
hostname = "<owner-host-at-claim-time>"           # advisory only
```

`state` is **three-valued** (see design doc § "Three states, two
intents"):

- `live` — held by `coordinator_id`, daemon serving.
- `stopped` — held by `coordinator_id`, daemon down on this host.
  Other coordinators cannot claim until the name is released
  (`volume release --force` from another host).
- `released` — no current owner; any coordinator may `volume start`.
  `coordinator_id`, `claimed_at`, and `hostname` are cleared on
  release so the populated fields agree with the state.
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

### Phase 3 — Force takeover

The override path is split into two normal verbs:

1. `volume release --force <name>` — unconditionally flip foreign
   `live`/`stopped` records to `released`, no drain, handoff pinned
   to the previous fork's last published snapshot.
2. `volume start --remote <name>` — claim the now-released name
   through the standard conditional-PUT path.

This keeps `volume start` always-safe (never overrides another
coordinator) and makes the dangerous step explicit and auditable.

- [ ] **`--force` flag on `volume release`.** Skip the foreign-owner
  refusal. Skip the `If-Match` precondition on the `names/<name>`
  PUT (unconditional Overwrite). Do **not** drain the previous
  owner's WAL — it's unreachable. The `handoff_snapshot` field on
  the released record is set to the previous fork's last published
  handoff snapshot (walk back through `parent` if the current fork
  never published).
- [ ] **Open question (carried, not resolved by this plan):**
  what's the parent pin when the current `names/<name>` value points
  at a fork that exists in S3 but has *never published a handoff
  snapshot* (coordinator crashed mid-`start`)? Two candidates:
  (a) walk back to the *grandparent* — i.e. the last fork that did
  publish a handoff snapshot — losing the orphan's writes;
  (b) treat the orphan as live and require the operator to specify
  an explicit pin.
  This connects to the broader recovery question of an implicit
  "now" snapshot. Track separately; do not block Phase 3 on it —
  ship with option (a) as the default and document it.
- [ ] **Tests:** force-release after simulated coordinator death
  (kill the writing process between snapshot publication and
  `names/<name>` rewrite, then `release --force` + `start --remote`
  from a second coordinator); force-release when the previous fork
  has no published snapshots
  (option (a) fallback path).

**Phase exit criteria:** an operator can recover a name from a dead
coordinator. The "no published snapshot" fallback behaviour is
documented.

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

1. **Force-takeover fallback semantics** (Phase 3 open question).
   Connects to broader recovery questions about implicit "now"
   snapshots.
2. **Garbage collection across migration-driven fork chains.** The
   existing GC and dedup paths handle ancestry; whether they handle
   the *traffic pattern* of frequent migrations (long thin chains
   with small per-fork tails) is worth a focused look. Probably a
   real exercise in Phase 5 testing.
3. **`volume materialize` ergonomics for chain compaction.** Already
   exists (`design-replica-model.md`) but is "deferred". Frequent
   migrations will accelerate the case for landing it; track
   separately.
4. **Bucket-capability probe failure modes.** If the probe itself
   fails (transient network error, unrelated S3 error), do we fail
   closed (refuse portable verbs) or fail open? Probably fail closed
   with a clear "couldn't determine bucket capabilities" error.
   Decide before Phase 4 ships.
