# Reference

Background reading, implementation notes, and open questions.

## Prior art and related systems

- [reference-lsvd.md](reference-lsvd.md) — lab47/lsvd Go reference implementation: directory layout, design decisions, and how they influenced Elide
- [reference-nydus.md](reference-nydus.md) — containerd/nydus-snapshotter: lazy-loading container images, RAFS format, NRI optimizer plugin, boot hints, failure modes

---

## Implementation Notes

**The NBD server is a development and testing tool.** The production block device frontend will be ublk (Linux, io_uring-based). NBD is kept for development convenience and macOS compatibility during local testing. This is architecturally identical to the lab47/lsvd reference implementation, which also exposes an NBD device. Elide's NBD server listens on TCP rather than a Unix socket purely for convenience during dev/test (e.g. connecting a VM running under Multipass or QEMU without configuring shared sockets). No design decisions should be made to optimise the NBD path at the expense of the ublk path.

**S3 upload is implemented via `elide-coordinator`.** The `drain-pending` subcommand uploads all segments from `pending/` to an object store and renames each to `segments/` on success. Both local (`--local <path>`, using `object_store::LocalFileSystem`) and real S3-compatible stores are supported with no code change. The upload path is validated end-to-end against a Multipass VM. What remains is demand-fetch: pulling segments back from S3 on a cache miss.

**Fork ownership is share-nothing by design.** Each fork is fully self-contained on its host — no distributed locks, leases, or coordination between hosts. The only inter-host communication is one-directional: segments flow from local disk to S3 via the coordinator. Forking to a new host is a clean handoff via snapshot, not a live migration. Each fork has an Ed25519 keypair (`fork.key`, `fork.pub`) generated at creation; every promoted segment is signed. On `serve-volume` open, `fork.origin` (hostname + canonical path + signature) is verified as a sanity check against accidental copies. This is not a key management system — if `fork.key` is copied to another host, both become valid signers. The guarantee is narrow: it catches misconfigured coordinators writing to the wrong fork and provides per-segment integrity at demand-fetch time. Full write access control is a bucket-level IAM concern.

**The imported base volume also has its own keypair.** `elide-import` generates `base.key` / `base.pub` / `base.origin` at the volume root before writing any segments, so every imported segment is signed. This prevents a second import from silently writing to the same volume root. The signing logic is shared via the `elide-signing` crate, which both `elide` and `elide-import` depend on.

**Demand-fetch is implemented (phase 1).** Segments in `segments/` are S3-backed and evictable; segments in `pending/` are local-only and must not be evicted. The hook point is `find_segment_file()` in `elide-core/src/volume.rs` — after all local directory checks fail, it delegates to an optional `SegmentFetcher` trait object, which downloads the segment and caches it in `segments/`. The fetcher is implemented in `elide/src/fetcher.rs` using `object_store` with a `tokio::runtime::Runtime::block_on` wrapper to satisfy the sync interface.

Key design decisions:

1. **`SegmentFetcher` trait in `elide-core`** (mirrors `SegmentSigner`): sync interface, `fn fetch(&self, segment_id: &str, dest: &Path) -> io::Result<()>`. `elide-core` stays async-free; async fetcher wraps `block_on`.
2. **Eviction deferred (phase 1)**: `segments/` grows unbounded. Eviction (LRU size cap) is the natural next step once fetch is validated in production.
3. **Fetch config via `fetch.toml`** in the volume directory; env var fallback (`ELIDE_S3_BUCKET`, `AWS_ENDPOINT_URL`). A `local_path` key enables local-filesystem testing with no object store server.
4. **Ancestor public keys via coordinator upload**: `drain_pending` uploads `fork.pub` (or `base.pub`) to `<volume_id>/<fork_name>/fork.pub` *before* any segments, so the key is always present when segments are fetchable. Signature verification at fetch time (TOFU) is not yet wired — the segment signature is stored in the file and can be verified in a future pass.

**Next: segment eviction.** `segments/` is now a cache tier but has no size cap. A simple policy: scan `segments/` by `mtime` (touched on fetch), evict oldest when total size exceeds a configured cap. Only `segments/` files are evictable — `pending/` files must never be removed (they are not yet in S3).

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
