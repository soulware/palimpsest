# Elide

Elide is a log-structured block storage system combining demand-fetch, content-addressed dedup, and delta compression, designed for running many VMs efficiently on shared infrastructure.

## Documentation

| Document | Contents |
|---|---|
| [docs/quickstart.md](docs/quickstart.md) | Import an OCI image, branch a writable replica, and serve it over NBD |
| [docs/quickstart-data-volume.md](docs/quickstart-data-volume.md) | Create an empty data volume, mount from a Lima VM, write data, upload segments |
| [docs/quickstart-tigris.md](docs/quickstart-tigris.md) | Run against a real S3-compatible backend (Tigris); covers AWS S3, MinIO, R2, etc. |
| [docs/overview.md](docs/overview.md) | Problem statement, key concepts, operation modes, empirical findings |
| [docs/findings.md](docs/findings.md) | Empirical measurements: dedup rates, demand-fetch patterns, delta compression data, write amplification |
| [docs/architecture.md](docs/architecture.md) | System architecture, directory layout, write/read paths, LBA map, extent index, dedup, snapshots |
| [docs/formats.md](docs/formats.md) | WAL format, segment file format (header + index + inline + body + delta), S3 retrieval strategies |
| [docs/operations.md](docs/operations.md) | GC, repacking, boot hints, filesystem metadata awareness |
| [docs/testing.md](docs/testing.md) | Property-based tests: ULID monotonicity and crash-recovery oracle |
| [docs/reference.md](docs/reference.md) | Implementation notes, open questions, and index of prior art docs |
| [docs/reference-lsvd.md](docs/reference-lsvd.md) | lab47/lsvd design decisions and comparison to Elide |
| [docs/reference-nydus.md](docs/reference-nydus.md) | nydus-snapshotter: lazy loading, RAFS format, NRI optimizer, boot hints |
| [docs/vm-boot.md](docs/vm-boot.md) | Booting a VM from an Elide volume with QEMU direct kernel boot |
| [docs/design-gc-ulid-ordering.md](docs/design-gc-ulid-ordering.md) | Open design: GC ULID ordering race, single-mint invariant, proptest findings |
| [docs/design-gc-overlap-correctness.md](docs/design-gc-overlap-correctness.md) | Design: GC skips partial-LBA-death entries to avoid shadow/loss on rebuild when multi-LBA entries have been partially overwritten |
| [docs/design-gc-partial-death-compaction.md](docs/design-gc-partial-death-compaction.md) | Design: decouple composite body from surviving sub-runs of partial-LBA-death entries so normal GC can subsequently reclaim each piece independently |
| [docs/design-gc-plan-handoff.md](docs/design-gc-plan-handoff.md) | Design: coordinator emits a plaintext plan; volume materialises bodies via BlockReader and signs the output — single source of truth for body resolution |
| [docs/design-delta-compression.md](docs/design-delta-compression.md) | Design: delta compression via file-path matching, file-aware import, snapshot filemaps |
| [docs/design-replica-model.md](docs/design-replica-model.md) | Proposed: replica-based model for forks and recovery; retires `volume fork`, adds `volume materialize`, frames snapshot cadence as a retention SLA |
| [docs/design-portable-live-volume.md](docs/design-portable-live-volume.md) | Accepted: named volumes become portable across hosts; each `volume start` is a fresh fork inheriting from the previous tail, so each ownership episode has its own ULID and signing key. The only shared mutable thing is `names/<name>` |
| [docs/portable-live-volume-plan.md](docs/portable-live-volume-plan.md) | Plan: phased implementation of portable live volumes (foundations → schema → lifecycle verbs → `release --force` recovery → CLI unification → tests/docs). Fresh-bucket-only; clean break for `volume remote` |
| [docs/design-tigris-native.md](docs/design-tigris-native.md) | Exploration: what Elide looks like if designed Tigris-native (bucket snapshots, forks, versioning as first-class primitives) rather than as a portable S3 consumer |
| [docs/integrations.md](docs/integrations.md) | Integration targets: Docker, Firecracker, Cloud Hypervisor, Kubernetes — architecture, sequencing, open work |
| [docs/design-oci-export.md](docs/design-oci-export.md) | Exploration: squashed OCI export, dual publish via referrers, elide-snapshotter for containerd |
| [docs/actor-offload-plan.md](docs/actor-offload-plan.md) | Plan: offload heavy maintenance work off the volume actor to isolate write tail latency |
| [docs/promote-offload-plan.md](docs/promote-offload-plan.md) | Plan: offload WAL promotion onto the worker thread (first step of actor-offload-plan) |
| [docs/promote-segment-offload-plan.md](docs/promote-segment-offload-plan.md) | Plan: offload `promote_segment` IPC handler to the worker thread (step 6 of actor-offload-plan) |
| [docs/design-ublk-transport.md](docs/design-ublk-transport.md) | Design: ublk as preferred host-local transport alongside NBD — multi-queue async handler, USER_RECOVERY_REISSUE crash recovery, phased rollout (steps 1–3 landed, 4–5 open) |
| [docs/design-ublk-shutdown-park.md](docs/design-ublk-shutdown-park.md) | Design (proposed): shutdown leaves ublk device QUIESCED for recovery; deletion becomes an explicit verb. Makes `stop → start` reliable while a filesystem is still mounted |

## Status updates

Dated waypoints — each file summarises major changes, bug fixes, and
remaining work relative to the previous status.  Latest first.

| Date | Document |
|---|---|
| 2026-04-27 | [docs/status-2026-04-27.md](docs/status-2026-04-27.md) |
| 2026-04-20 | [docs/status-2026-04-20.md](docs/status-2026-04-20.md) |
| 2026-04-09 | [docs/status-2026-04-09.md](docs/status-2026-04-09.md) |
| 2026-03-30 | [docs/status-2026-03-30.md](docs/status-2026-03-30.md) |

Start with [docs/overview.md](docs/overview.md).

## Continuous integration

Two lanes run unconditionally on every pull request and every push to `main`:

- `ci` — build, clippy, and userspace tests.
- `ci-kernel` — kernel-dependent features (`ublk::`, `nbd::`) exercised inside
  a nested KVM VM on the GitHub runner. Host builds the test binary; the guest
  runs it via a 9p share. Blocking, not advisory.
