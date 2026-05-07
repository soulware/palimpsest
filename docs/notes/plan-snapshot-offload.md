---
status: landed
related: [plan-actor-offload.md]
---

# Offload `sign_snapshot_manifest` off the volume actor

Step 7 (final) of [plan-actor-offload.md](plan-actor-offload.md). Landed in PR #68 (`edea025`).

## Decision

Same three-phase shape. Prep snapshots the signer `Arc` and base dir into a `SignSnapshotManifestJob`; worker enumerates `index/`, signs, atomic-writes the manifest, writes the marker; apply flips `has_new_segments = false`. Single `parked_sign_snapshot_manifest` slot rejects concurrent requests, matching the sweep/repack pattern. The coordinator's per-volume snapshot lock already serialises `snapshot_volume` end-to-end, so concurrent requests don't arise in practice.

## Folded into the same landing

`VolumeRequest::Snapshot`, `VolumeHandle::snapshot()`, and the `snapshot` arm in `src/control.rs` were deleted — no live wire-level sender. The production snapshot path goes through the decomposed sequence (`promote_wal` / `promote` / `apply_gc_handoffs` / `sign_snapshot_manifest`), and `Volume::snapshot()` the method survives only as an in-process entry point for tests.

## Crash semantics (already correct pre-offload)

The "marker written last" rule on `snapshots/<ulid>` makes the manifest atomic from a reader's perspective.

| Crash point | State | Recovery |
|---|---|---|
| Before manifest tmp-write completes | no manifest, no marker | coordinator retries; no visible partial snapshot |
| After manifest rename, before marker | `.manifest` exists, no marker | next attempt overwrites via tmp+rename then writes marker. Without a marker no reader saw the orphan |
| After marker, before apply | manifest + marker on disk; `has_new_segments` still true in memory | process is dying; in-memory flag is moot. Next open derives `has_new_segments` from WAL/`pending/`, not persisted |

No on-disk invariant changes under the offload.

## Worker concurrency

The worker is FIFO single-threaded. Within one snapshot the coordinator's snapshot lock plus the FIFO guarantee that `index/` enumeration sees state exactly after the preceding `promote`s; writes during the manifest sign land on the fresh WAL opened by the prior `promote_wal` and don't appear in `index/` until a future drain. Sweep / repack / delta-repack rewrite `pending/`, not `index/`, so they don't affect the manifest's enumeration.
