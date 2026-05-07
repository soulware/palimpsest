# docs/notes/

Working record of design discussions, implementation plans, dated status
snapshots, and prior-art notes. **LLM-targeted**: written and maintained for
Claude consumption. Humans usually want [`../quickstart.md`](../quickstart.md)
or [`../overview.md`](../overview.md) instead.

This directory is intentionally not indexed from `README.md`.

## Conventions

New notes should carry YAML frontmatter:

```yaml
---
status: proposed | accepted | landed | superseded | exploration
supersedes: <other-note.md>   # if applicable
related: [<other-note.md>, ...]
landed_in: <synthesis-section>  # e.g. ../architecture.md#dedup — required when status: landed
---
```

When a design lands, set `status: landed` and `landed_in:` to the canonical
synthesis-doc section, then stop editing the note. The design history is the
record of how the decision was reached; the synthesis doc is the current truth.

## Designs

| Doc | Summary |
|---|---|
| [design-gc-ulid-ordering.md](design-gc-ulid-ordering.md) | GC ULID ordering race, single-mint invariant, proptest findings |
| [design-gc-overlap-correctness.md](design-gc-overlap-correctness.md) | GC skips partial-LBA-death entries to avoid shadow/loss on rebuild when multi-LBA entries have been partially overwritten |
| [design-gc-partial-death-compaction.md](design-gc-partial-death-compaction.md) | Decouple composite body from surviving sub-runs of partial-LBA-death entries so normal GC can subsequently reclaim each piece independently |
| [design-gc-plan-handoff.md](design-gc-plan-handoff.md) | Coordinator emits a plaintext plan; volume materialises bodies via BlockReader and signs the output — single source of truth for body resolution |
| [design-gc-self-describing-handoff.md](design-gc-self-describing-handoff.md) | Self-describing GC handoff records — older `applied_gc/<ulid>` markers replaced |
| [design-extent-reclamation.md](design-extent-reclamation.md) | Extent reclamation strategy and measurement-before-mechanism rationale |
| [design-delta-compression.md](design-delta-compression.md) | Delta compression via file-path matching, file-aware import, snapshot filemaps |
| [design-delta-materialisation.md](design-delta-materialisation.md) | Local-only `cache/<ULID>.dmat` sidecar caches lz4-compressed materialised delta-entry bytes after first read; WAL-shaped append-only with hash-verified tail recovery |
| [design-noop-write-skip.md](design-noop-write-skip.md) | Skip writes that would not change LBA-mapped state; rationale and tier choices |
| [design-replica-model.md](design-replica-model.md) | Replica-based model for forks and recovery; retires `volume fork`, adds `volume materialize`, frames snapshot cadence as a retention SLA |
| [design-portable-live-volume.md](design-portable-live-volume.md) | Named volumes become portable across hosts; each `volume start` is a fresh fork inheriting from the previous tail, so each ownership episode has its own ULID and signing key. The only shared mutable thing is `names/<name>` |
| [design-volume-size-ownership.md](design-volume-size-ownership.md) | `size` lives on the `names/<name>` claim record (single owner, CAS-protected, signed in event log), not on the unsigned `manifest.toml`. Resize is a CAS + `UBLK_U_CMD_UPDATE_SIZE` (Linux 6.16+) |
| [design-manifest-toml-removal.md](design-manifest-toml-removal.md) | Dropped `manifest.toml` entirely. `name` / `readonly` / `origin` were redundant with existing surfaces; OCI `source` migrated onto signed `volume.provenance` as `oci_source` |
| [design-volume-event-log.md](design-volume-event-log.md) | Per-name append-only event log under `events/<name>/<ulid>.toml` recording lifecycle transitions. Pointer stays canonical for "now"; log is canonical for "ever" |
| [design-force-release-fencing.md](design-force-release-fencing.md) | Split-brain safety for `volume release --force`. Previous owner pulls the synthesised handoff snapshot; existing snapshot-floor rule (extended to the reaper) pins B's set automatically |
| [design-consistency-surface.md](design-consistency-surface.md) | Which Elide operations require strong consistency vs. which tolerate eventual; failure-mode walkthrough; sketch of a two-bucket split |
| [design-tigris-native.md](design-tigris-native.md) | What Elide looks like if designed Tigris-native (bucket snapshots, forks, versioning as first-class primitives) rather than as a portable S3 consumer |
| [design-oci-export.md](design-oci-export.md) | Squashed OCI export, dual publish via referrers, elide-snapshotter for containerd |
| [design-ublk-transport.md](design-ublk-transport.md) | ublk as preferred host-local transport alongside NBD — multi-queue async handler, USER_RECOVERY_REISSUE crash recovery, phased rollout |
| [design-ublk-shutdown-park.md](design-ublk-shutdown-park.md) | Shutdown leaves ublk device QUIESCED for recovery; deletion becomes an explicit verb. Makes `stop → start` reliable while a filesystem is still mounted |
| [design-peer-segment-fetch.md](design-peer-segment-fetch.md) | Opportunistic LAN peer-fetch tier in front of S3 for index/body bytes. Targets cross-host handoff (release → claim) and large-fleet image pull |

## Plans

| Doc | Summary |
|---|---|
| [actor-offload-plan.md](actor-offload-plan.md) | Offload heavy maintenance work off the volume actor to isolate write tail latency |
| [promote-offload-plan.md](promote-offload-plan.md) | Offload WAL promotion onto the worker thread (first step of actor-offload-plan) |
| [promote-segment-offload-plan.md](promote-segment-offload-plan.md) | Offload `promote_segment` IPC handler to the worker thread (step 6 of actor-offload-plan) |
| [snapshot-offload-plan.md](snapshot-offload-plan.md) | Offload snapshot work off the volume actor |
| [coordinator-driven-snapshot-plan.md](coordinator-driven-snapshot-plan.md) | Coordinator-driven snapshot flow |
| [portable-live-volume-plan.md](portable-live-volume-plan.md) | Phased implementation of portable live volumes |
| [fork-from-remote-plan.md](fork-from-remote-plan.md) | Auto-pull source + ancestor chain from S3 when forking from a name not local |
| [peer-segment-fetch-v1-plan.md](peer-segment-fetch-v1-plan.md) | v1 implementation of peer-fetch — `.idx`-only, coordinator-driven, opt-in via coordinator config |

## Status snapshots

Point-in-time waypoints. Latest first. Snapshots are *frozen* — never edit a
status doc after its date; write a new one or update the synthesis docs.

| Date | Doc |
|---|---|
| 2026-04-27 | [status-2026-04-27.md](status-2026-04-27.md) |
| 2026-04-20 | [status-2026-04-20.md](status-2026-04-20.md) |
| 2026-04-09 | [status-2026-04-09.md](status-2026-04-09.md) |
| 2026-03-30 | [status-2026-03-30.md](status-2026-03-30.md) |

## Prior art and references

| Doc | Summary |
|---|---|
| [reference.md](reference.md) | Implementation notes, open questions, and index of prior art docs |
| [reference-lsvd.md](reference-lsvd.md) | lab47/lsvd Go reference: directory layout, design decisions, and how they influenced Elide |
| [reference-nydus.md](reference-nydus.md) | containerd/nydus-snapshotter: lazy-loading container images, RAFS format, NRI optimizer plugin, boot hints |
