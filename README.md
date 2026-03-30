# Elide

Elide is a log-structured block storage system combining demand-fetch, content-addressed dedup, and delta compression, designed for running many VMs efficiently on shared infrastructure.

## Documentation

| Document | Contents |
|---|---|
| [docs/quickstart.md](docs/quickstart.md) | Import an OCI image, fork it, and serve it over NBD |
| [docs/quickstart-data-volume.md](docs/quickstart-data-volume.md) | Create an empty data volume, mount from a Multipass VM, write data, upload segments |
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
| [docs/status-2026-03-30.md](docs/status-2026-03-30.md) | Implementation status snapshot: what's built, remaining work, lsvd deviations (2026-03-30) |
| [docs/design-gc-ulid-ordering.md](docs/design-gc-ulid-ordering.md) | Open design: GC ULID ordering race, single-mint invariant, proptest findings |

Start with [docs/overview.md](docs/overview.md).
