# Elide

Elide is a log-structured block storage system combining demand-fetch, content-addressed dedup, and delta compression, designed for running many VMs efficiently on shared infrastructure.

## Documentation

| Document | Contents |
|---|---|
| [docs/overview.md](docs/overview.md) | Problem statement, key concepts, operation modes, empirical findings |
| [docs/findings.md](docs/findings.md) | Empirical measurements: dedup rates, demand-fetch patterns, delta compression data, write amplification |
| [docs/architecture.md](docs/architecture.md) | System architecture, directory layout, write/read paths, LBA map, extent index, dedup, snapshots |
| [docs/formats.md](docs/formats.md) | WAL format, segment file format (header + index + inline + body + delta), S3 retrieval strategies |
| [docs/operations.md](docs/operations.md) | GC, repacking, boot hints, filesystem metadata awareness |
| [docs/reference.md](docs/reference.md) | lsvd reference implementation comparison, implementation notes, open questions |

Start with [docs/overview.md](docs/overview.md).
