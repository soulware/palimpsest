# Reference

Background reading, implementation notes, and open questions.

## Prior art and related systems

- [reference-lsvd.md](reference-lsvd.md) — lab47/lsvd Go reference implementation: directory layout, design decisions, and how they influenced Elide
- [reference-nydus.md](reference-nydus.md) — containerd/nydus-snapshotter: lazy-loading container images, RAFS format, NRI optimizer plugin, boot hints, failure modes

---

## Implementation Notes

**The NBD server is a development and testing tool.** The production block device frontend will be ublk (Linux, io_uring-based). NBD is kept for development convenience and macOS compatibility during local testing. This is architecturally identical to the lab47/lsvd reference implementation, which also exposes an NBD device. Elide's NBD server listens on TCP rather than a Unix socket purely for convenience during dev/test (e.g. connecting a VM running under Multipass or QEMU without configuring shared sockets). No design decisions should be made to optimise the NBD path at the expense of the ublk path.

**S3 is intentionally deferred.** The system can be developed and validated end-to-end using local storage only — `pending/` and `segments/` act as local-only segment stores without any upload step. This covers the full write path, promotion pipeline, LBA map, crash recovery, and read path. S3 hookup comes later.

A clean progression for introducing S3:
1. **Local only** — `pending/` and `segments/` as local stores, no upload step
2. **Local S3-compatible service** (MinIO, LocalStack) — write the S3 client code against a local service; real upload/download paths exercised without cloud dependency
3. **Real S3** — swap in credentials, no code changes needed

Constraints to keep in mind so S3 integration stays straightforward:
- Segment IDs (ULIDs) are already globally unique and suitable as S3 object keys
- The `pending/` → `segments/` transition maps cleanly to "upload to S3, then rename locally"
- Persistent structures (manifests, segment files) reference segment IDs only — local paths are derived at runtime, never stored

---

## Open Questions

- **Hash output size:** BLAKE3 at full 256-bit is the current choice — collision probability is negligible (~2^-128 birthday bound) at any realistic extent count, and speed is equivalent to non-cryptographic hashes on AVX2/NEON hardware. A truncated 128-bit output would halve the per-entry cost in the extent index while keeping collision probability effectively zero at practical scales. Worth revisiting once the index size and memory pressure are measured empirically.
- **Inline extent threshold:** extents below this size are stored inline in the segment's inline section rather than referenced by body offset. Needs empirical validation against the actual extent size distribution in target images.
- **Entropy threshold:** 7.0 bits used in experiments, taken from the lab47/lsvd reference implementation. Optimal value depends on workload mix.
- **Segment size:** ~32MB soft threshold, taken from the lab47/lsvd reference implementation (`FlushThreshHold = 32MB`). Not a hard maximum — a segment closes when it exceeds the threshold. Optimal value depends on S3 request cost vs read amplification tradeoff.
- **Extent index implementation:** sled, rocksdb, or custom. Needs random reads and range scans.
- **Pre-log coalescing block limit:** lsvd uses 20 blocks. The right value for Elide depends on typical write burst sizes, acceptable memory footprint between fsyncs, and worst-case read amplification when compression is enabled (see reference-lsvd.md). The limit must be enforced at the write path even when the NBD layer delivers larger contiguous writes — splitting oversized writes into capped extents is preferable to unbounded amplification.
- **LBA map cache invalidation:** validate the cached `lba.map` against a hash of the current segment IDs across the full ancestor tree, not just the live node.
- **Delta segment threshold:** not every segment needs a delta body — only useful when changed extents have known prior versions in the ancestor tree. Criteria for when to compute and upload a delta body need empirical validation.
- **Boot hint persistence:** where are hint sets stored, how are they distributed across hosts? See reference-nydus.md for how nydus approaches this (embedded in image bootstrap at build time).
- **Boot hint ordering:** record LBA access sequence with timestamps (not just a set) to enable priority-ordered prefetch — see nydus NRI optimizer elapsed-time approach.
- **Empirical validation of repacking benefit:** measure segment fetch count before and after access-pattern-driven repacking.
- **ublk integration:** Linux-only, io_uring-based. NBD is the dev/test frontend; ublk is the production target.
