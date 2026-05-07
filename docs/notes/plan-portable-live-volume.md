---
status: landed
related: [design-portable-live-volume.md, design-force-release-fencing.md]
---

# Portable live volume — implementation plan

Implements [design-portable-live-volume.md](design-portable-live-volume.md). Phases 0–4 landed; Phase 5 (additional proptest/integration coverage and docs) is the remaining tail.

## Decisions locked in during implementation

**2026-04-27 — Fresh buckets only.** No migration of existing `names/` entries or volumes. New buckets created after the landing use the new schema; pre-existing buckets stay on the old schema and never get portability features. `volume remote` is a clean break — no aliases.

**2026-04-28 — Force-release recovers to "now".** The recovering coordinator lists S3 segments under the dead fork's prefix, verifies each segment's signature against the dead fork's `volume.pub`, and synthesises a fresh handoff snapshot covering the verified set. Data-loss boundary is "writes the dead owner accepted but never promoted to S3" — same as the crash-recovery contract. Works even if the dead owner never published a snapshot.

**2026-04-28 — Coordinator identity is an Ed25519 keypair.** `coordinator.key` / `coordinator.pub` are the sole on-disk identity artefacts. `coordinator_id` derives from the **public** key (self-authenticating); the macaroon MAC root derives from the private key in-memory at startup. No separate `coordinator.root_key` file. Existing deployments take a clean break: new keypair, new `coordinator_id`, affected volumes recover via `volume release --force`.

## How it fits with existing `volume stop`/`volume start`

The verbs are generalised, not renamed:

- For a name this coordinator owns (`names/<name>.coordinator_id == self`), `stop`/`start` do today's local up/down — plus, on `stop`, drain WAL → publish handoff snapshot → release the name in S3.
- For a name held by another coordinator, `start` triggers the fork-on-claim path: read `names/<name>`, mint a fresh ULID and keypair, create a local fork directory with provenance pointing at the previous fork's handoff snapshot, conditionally claim the name.
- The `volume.stopped` marker becomes a local cache of `names/<name>.state`. On coordinator startup, reconcile each local volume directory against `names/<name>` — if the marker disagrees, the S3 record wins.

## Phases (landing record)

**Phase 0 — Foundations** (landed). `elide-coordinator/src/portable.rs`: conditional-PUT helpers (`put_if_absent`, `put_with_match`) wrapping `ObjectStore::put_opts` with typed errors; per-bucket capability probe.

**Phase 1 — `names/<name>` schema** (landed). New schema with `state`, `vol_ulid`, `coordinator_id`, `parent`, `claimed_at`, `hostname`. CAS-protected mutations: `mark_initial`, `mark_initial_readonly`, `mark_stopped`, `mark_released`, `mark_live`, `mark_claimed`. **Phase 1.5 — `coordinator.key`/`coordinator.pub`** (landed): `elide-coordinator/src/identity.rs`; coordinator pub mirrored to S3 at `coordinators/<coord_id>/coordinator.pub` on startup.

**Phase 2 — Lifecycle verbs and IPC** (landed). `volume stop` / `release` / `release --release` / `start` / `claim` / `start --claim` / `start --remote` wired through inbound IPC.

**Phase 3 — `release --force` recovery** (landed). `elide-coordinator/src/recovery.rs` lists segments under the dead fork's prefix, verifies signatures, mints a synthesised handoff snapshot signed by the recovering coordinator's key. Tests cover tampered-segment drop, claim-races against `reserved-for-other`, mismatched-pubkey refusal, empty-segment-list recovery, and a two-coordinator proptest.

**Phase 4 — CLI surface** (landed). `volume list` is local-only (`by_name/` + `--all` for `by_id/`, never reaches S3). `volume status <name> --remote` is the per-name authoritative query, returning eligibility (`owned` / `foreign` / `released-claimable` / `reserved-for-self` / `reserved-for-other` / `readonly`). `volume remote` deleted; `remote_pull` retained as the helper used by `fork --from` and `claim_released_name`.

**Phase 5 — outstanding.** End-to-end proptest covering random sequences across two simulated coordinators sharing a fake bucket; real-bucket Tigris integration; runbook in `docs/operations.md`; process-level recovery test alongside the kernel-lane CI.

## Cross-cutting open items

- **GC across migration-driven fork chains.** Existing GC handles ancestry; whether it handles the *traffic pattern* of frequent migrations (long thin chains with small per-fork tails) wants a focused look. Probably a real exercise in Phase 5 testing.
- **`volume materialize` for chain compaction.** Already exists in [design-replica-model.md](design-replica-model.md) but is deferred. Frequent migrations accelerate the case for landing it.
- **Bucket-capability probe failure modes.** If the probe itself fails (transient network error), default to fail-closed with a clear "couldn't determine bucket capabilities" error.
- **Coordinator pubkey caching.** Readers fetch fresh on each verification (one small GET per `start --remote` against a synthesised snapshot). Local caching is an optimisation, not a correctness need.
