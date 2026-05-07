---
status: landed
related: [plan-portable-live-volume.md, design-volume-event-log.md, design-force-release-fencing.md, design-volume-size-ownership.md]
landed_in: ../architecture.md
---

# Portable live volume

A **named volume** is portable across hosts. Its name is stable; the underlying `<vol_ulid>` changes every time ownership transfers. Each ownership episode is a fork of the previous one. At any moment the name resolves to exactly one coordinator's fork.

```
<vol_ulid_1>  — first owner's fork. Frozen after release.
<vol_ulid_2>  — second owner's fork, parent = <vol_ulid_1> at its handoff snapshot. Frozen after release.
<vol_ulid_3>  — third owner's fork, parent = <vol_ulid_2> at its handoff snapshot. Currently live.
names/<name>  → <vol_ulid_3> (with current owner's coordinator id)
```

Each `<vol_ulid_i>` is written by exactly one coordinator and never mutated by anyone else. The only mutable thing in the bucket is `names/<name>`.

## Why this works

- **Signing-key portability is solved by not needing it.** Each fork has its own per-volume Ed25519 key, generated locally by the acquiring coordinator and never leaving that host. Verifiers verify segments against their own fork's `volume.pub`. The "key history" is the fork chain.
- **No shared mutable prefix.** Two coordinators never write to the same `<vol_ulid>/` prefix. No fencing token to plumb into segment manifests; no race for ETag-conditional writes inside contested directories.
- **Audit trail is provenance.** Walking the fork chain reveals every coordinator that has owned the name, the snapshot at each handoff, and the current owner.
- **Names are globally consistent.** The first creator establishes the name; subsequent operations refer to the same name on every host.
- **Replica references stay valid forever.** Pinning `<vol_ulid_i>/<snap_ulid>` for a replica keeps a valid pin: the prefix is frozen, not deleted.

## `names/<name>` — the owner pointer

```toml
vol_ulid = "<current_fork_ulid>"
coordinator_id = "<owner-coordinator-id>"
state = "live"            # or "stopped" or "released" or "readonly"
parent = "<prev_ulid>/<prev_snap_ulid>"   # absent on root
claimed_at = "<rfc3339>"
hostname = "<owner-host-at-claim-time>"   # advisory only
```

| State | `coordinator_id` | `claimed_at` / `hostname` | Mutable? | Who can claim |
|---|---|---|---|---|
| `live` | current owner | populated | yes | running here; others need `release --force` |
| `stopped` | current owner | populated | yes | this coordinator (local resume); others need `release --force` |
| `released` | empty | empty | yes (after claim) | any coordinator |
| `readonly` | empty | empty | **no** | n/a (no daemon, multiple readers OK) |

Field shape is consistent: `coordinator_id` means "this record's current owner" (current owner on live/stopped, no one on released/readonly); `claimed_at`/`hostname` mean "when/where the *current* owner claimed", populated only when there is a current owner.

## Verbs

- **`volume stop`** — `live → stopped`. Local stop, retain ownership. WAL fsynced; daemon halted; conditional PUT flips state. **No handoff snapshot.** Other coordinators refused.
- **`volume release`** — `live → released` or `stopped → released`. Drains WAL → publishes handoff snapshot → flips state, clearing identity fields. Refuses on foreign ownership unless `--force`.
- **`volume release --force`** — release a name held by another coordinator. Skips `If-Match`, does not drain (the dead owner's WAL is unreachable). The recovering coordinator synthesises a fresh handoff snapshot from S3-observable verified segments.
- **`volume stop --release`** — convenience for stop then release.
- **`volume start`** — `stopped → live`. Pure local resume. Refuses if no local state, or if the bucket says `released` (route to `claim`).
- **`volume claim`** — `released → stopped`. In-place reclaim when local fork ULID matches; otherwise pull from the released ancestor, mint a fresh fork descending from the handoff snapshot, atomically rebind. Always CAS-protected. Daemon not started — use `volume start` afterwards (or compose with `--claim`).
- **Coordinator graceful shutdown / crash** — never changes `state`. A coordinator coming back keeps its volumes, its WAL, and its `state=live` records.

All transitions are conditional PUTs (`If-Match` on ETag; supported by S3, Tigris, R2, MinIO, Ceph RGW). Conditional-write atomicity on this single object is the entire ownership protocol.

### Verb flows

**`volume stop <name>`** (local stop, retain ownership):
1. Refuse if a client (NBD/ublk) is connected.
2. Existing `shutdown` RPC: WAL fsync, daemon halts.
3. Conditional PUT to `names/<name>` flipping `state` from `live` to `stopped`. `vol_ulid`/`coordinator_id` unchanged.
4. Local artefacts (cache, index, WAL, signing key) **stay in place** for the same coordinator's later `start`.

**`volume release <name>`** (relinquish ownership):
1. If currently `live`, do everything `stop` does first.
2. Drain WAL: promote pending records, finish in-flight uploads.
3. Publish a handoff snapshot covering everything published.
4. Conditional PUT setting `state = "released"`, keeping `vol_ulid` pointing at the now-frozen fork. Owner-identity fields (`coordinator_id`, `claimed_at`, `hostname`) are **cleared**.
5. Local `by_id/<vol_ulid>/` may be discarded (reproducible from S3) or kept as cache for fast reacquisition. **Never** keep the WAL.

**`volume start <name>`** (pure local resume — never reaches into the bucket to change ownership):
- Already `live` and ours → idempotent no-op.
- `state=stopped`, `coordinator_id=self` → reuse the existing fork; flip `state` back to `live` via conditional PUT; restart the daemon. No new ULID, no snapshot, no fork.
- No local state, or `state == "released"` → refuse with an error pointing at `volume claim`.

**`volume claim <name>`**:
- `state == "released"`: claim allowed for any coordinator. Two sub-cases route automatically — **in-place reclaim** when the released `vol_ulid` matches a local fork (no pull, no fork mint), **cross-coordinator claim** otherwise (pull from the released ancestor, mint a fresh ULID + keypair, create `by_id/<new_ulid>/` with provenance pointing at the previous fork's `<vol_ulid>/<handoff_snap_ulid>`, conditional PUT to `names/<name>`). Result is `Stopped`; daemon is **not** started.
- `state == "live"` or `"stopped"` (foreign-owned): refuse unless `--force` is passed. With `--force`, the verb internally synthesises a handoff snapshot for the dead fork (same path as `release --force`) and then claims the now-Released name in one shot.

### Lifecycle vs coordinator process exit

| Event | `names/<name>` | WAL | Snapshot | Recovery |
|---|---|---|---|---|
| `volume stop` | `state=stopped`, same `coordinator_id` | fsynced, retained | no | this coordinator restarts and `start`s; others refused |
| `volume release` | `state=released`; identity cleared | drained, then discarded | yes — handoff | any coordinator may `claim` |
| Coordinator graceful shutdown | unchanged | fsynced, retained | no | restarts, sees own `coordinator_id`, replays WAL, resumes — `state` was never flipped |
| Coordinator crash | unchanged | retained, possibly torn tail | no | same as graceful — restart replays WAL; unsynced tail lost (existing crash contract) |
| `volume release --force` from elsewhere | rewritten unconditionally; `released`, identity cleared | abandoned (dead WAL unreachable) | yes — synthesised | a subsequent `claim` claims it normally; data loss boundary = "writes the dead owner accepted but never promoted to S3" |

The lifecycle verbs are explicit operator intent. Coordinator process exit (graceful or crash) does **not** flip state — a coordinator coming back keeps its volumes, its WAL, and its `state=live` records.

## Readonly names

A name in `state=readonly` points at immutable content (today: an imported OCI image). `coordinator_id`/`claimed_at`/`hostname` are empty — **no exclusive owner, by design**. Multiple coordinators may pull and serve the same readonly name concurrently.

- `mark_initial` uses `If-None-Match: *` so two concurrent imports of the same name resolve cleanly: one writes, the other sees `AlreadyExists`.
- Lifecycle verbs (`stop`/`release`/`start`/`claim`) all refuse on `readonly` with `InvalidTransition`.
- Re-import is rejected today; idempotent re-import when supplied `vol_ulid` matches is a future refinement.

The writable lifecycle and the published-handle lifecycle are intentionally separate so a name cannot accidentally cross between them.

## Object store requirements

The whole proposal rests on two primitives applied only to `names/<name>`:

1. **Strong read-after-write consistency** on individual keys.
2. **Conditional writes** — `If-None-Match: *` for create-if-absent; `If-Match: <etag>` for CAS on existing keys.

Combined: a global, linearisable, single-key compare-and-swap. That one primitive carries name uniqueness, ownership transfer, and `release --force` semantics. Everything else is append-only or immutable.

**Graceful degradation.** Lack of conditional PUT must not break Elide. The coordinator probes per-bucket on first connect; if absent, `volume start <name>` for a name not held by this coordinator is disabled. The fallback is `volume create --from <vol_ulid>/<snap_ulid>` — fork-as-relocation, today's escape hatch, no shared-key contention.

## Coordinator identity

A coordinator's identity is rooted in a single Ed25519 keypair on the coordinator host:

- `<data_dir>/coordinator.key` — Ed25519 private key (32-byte seed), mode 0600. Never leaves the host.
- `<data_dir>/coordinator.pub` — public key. Mirrored to S3 at `coordinators/<coordinator_id>/coordinator.pub`.

`coordinator_id` is `blake3::derive_key("elide coordinator-id v1", coord_pub.as_bytes())` — **self-authenticating**: anyone can recompute and refuse a record where the pub doesn't match the id. The macaroon MAC root is `blake3::derive_key("elide macaroon-root v1", coord_priv.as_bytes())`, in-memory only — domain separation makes the seed reuse safe.

Single on-disk secret. No independent rotation: signing key, macaroon root, and `coordinator_id` rotate together (i.e. they don't, today). Deleting `coordinator.key` ends the coordinator's identity; volumes whose `names/<name>` points at it can only be reclaimed via `volume release --force` from a clean identity.

## `volume release --force` — synthesised handoff snapshot

The handoff snapshot is **synthesised at force-release time from S3-observable segments**, not pinned to the dead fork's last published handoff. Recovering coordinator B:

1. Lists `by_id/<dead_vol_ulid>/segments/`.
2. Fetches each segment header + index section (cheap; not the body).
3. Verifies Ed25519 signature against the dead fork's `volume.pub`. Failures (partial uploads, torn objects) are dropped.
4. Mints a synthesised snapshot at `by_id/<dead_vol_ulid>/snapshots/<new_snap_ulid>` naming the verified segment set, with `synthesised_from_recovery = true` and `recovering_coordinator_id = <B>`.
5. Signs it with B's coordinator key.
6. Unconditional PUT to `names/<name>` flipping to `released`, recording the synthesised snapshot ULID.

Data-loss boundary is "writes fsync'd locally on the dead owner but never made it to S3" — the existing crash-recovery contract. Strictly better than pinning to the last handoff. Works even when the dead owner never published a snapshot at all (segments are self-describing — the snapshot manifest is an optimisation, not a correctness requirement).

After `--force`, the name is normal `released` and the next claimant runs `volume claim <name>` through the standard CAS path. Splitting `release --force` from `claim` keeps `start` always-safe (no flag combination overrides a foreign owner) and makes the dangerous step independently auditable. The previous-owner side of split-brain is in [design-force-release-fencing.md](design-force-release-fencing.md).

## Snapshots: user vs handoff

User snapshots and handoff snapshots share the on-disk shape. Handoff snapshots are minted automatically by `volume release` and carry the releasing `coordinator_id`/hostname/episode `claimed_at`. **`volume stop` does not mint a handoff snapshot** — it retains ownership locally.

Handoff snapshots are retained forever for now (audit-friendly history; no concrete reason to GC). Replicas via `--from <vol_ulid>/<handoff_snap>` are allowed and flagged in the snapshot record so tooling can distinguish them.

## What changes elsewhere

**`by_name/`** stays a local symlink cache; the authoritative mapping is `names/<name>` in S3.

**`volume remote` goes away.** `volume remote list` is removed (unbounded enumeration); `volume status --remote <name>` is the per-name authoritative query (the user must already know the name; no discovery path through the CLI). `volume remote pull` is removed — `volume claim` + lazy demand-fetch covers it; eager hydration composes with `volume materialize`.

**Chain length.** Long-lived volumes with frequent migrations grow long fork chains, but read cost is bounded by the most recent handoff snapshot. `materialize` collapses chains on demand.

**Skip empty intermediates on claim.** Release/claim cycles that produce no writes (two coordinators ping-ponging, or stopped-on-A-started-on-B without modifying) currently add no-op forks. The rule: on claim, if `max(handoff_manifest.segments) < parent.snapshot_ulid` (released fork is empty by ULID monotonicity), set the new fork's `parent` to the released fork's own parent. Keeps the chain shallow by induction.

## Open questions

- **Naming clarity.** "Live" today means "host-pinned and serving". With portability it's a property of the name, not an individual ULID. Worth picking vocabulary deliberately for CLI output.
- **`names/<name>` write authority.** With portability `names/` becomes the most contentious object in the bucket. Confirm the credential model already grants per-coordinator write scope (or design it in).
- **GC across a fork chain.** Lowest-ULID-wins extent-index policy works across ancestry, but long migration chains are a new traffic pattern worth a focused look.
