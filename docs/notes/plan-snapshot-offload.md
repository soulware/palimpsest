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

The "marker written last" rule on `snapshots/<ulid>` makes the manifest atomic from a reader's perspective. A crash between manifest rename and marker write leaves an orphan `.manifest` invisible to any reader; the next snapshot attempt overwrites via tmp+rename then writes the marker. No on-disk invariant changes under the offload.
