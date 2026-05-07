# Elide

Elide is a log-structured block storage system combining demand-fetch, content-addressed dedup, and delta compression, designed for running many VMs efficiently on shared infrastructure.

## Documentation

Start with [docs/quickstart.md](docs/quickstart.md), then [docs/overview.md](docs/overview.md).

| Document | Contents |
|---|---|
| [docs/quickstart.md](docs/quickstart.md) | Import an OCI image, branch a writable replica, and serve it over NBD |
| [docs/quickstart-data-volume.md](docs/quickstart-data-volume.md) | Create an empty data volume, mount from a Lima VM, write data, upload segments |
| [docs/quickstart-tigris.md](docs/quickstart-tigris.md) | Run against a real S3-compatible backend (Tigris); covers AWS S3, MinIO, R2, etc. |
| [docs/overview.md](docs/overview.md) | Problem statement, key concepts, operation modes, empirical findings |
| [docs/architecture.md](docs/architecture.md) | System architecture, directory layout, write/read paths, LBA map, extent index, dedup, snapshots |
| [docs/formats.md](docs/formats.md) | WAL format, segment file format, S3 retrieval strategies |
| [docs/operations.md](docs/operations.md) | GC, repacking, boot hints, filesystem metadata awareness |
| [docs/findings.md](docs/findings.md) | Empirical measurements: dedup rates, demand-fetch patterns, delta compression data, write amplification |
| [docs/testing.md](docs/testing.md) | Property-based tests: ULID monotonicity and crash-recovery oracle |
| [docs/integrations.md](docs/integrations.md) | Integration targets: Docker, Firecracker, Cloud Hypervisor, Kubernetes |
| [docs/vm-boot.md](docs/vm-boot.md) | Booting a VM from an Elide volume with QEMU direct kernel boot |

`docs/notes/` holds design discussions, implementation plans, and dated status
snapshots — a working record maintained for LLM context, not human-facing
documentation. See [docs/notes/INDEX.md](docs/notes/INDEX.md) if you want to
trace why a particular decision was made.

## Continuous integration

Two lanes run unconditionally on every pull request and every push to `main`:

- `ci` — build, clippy, and userspace tests.
- `ci-kernel` — kernel-dependent features (`ublk::`, `nbd::`) exercised inside
  a nested KVM VM on the GitHub runner. Host builds the test binary; the guest
  runs it via a 9p share. Blocking, not advisory.
