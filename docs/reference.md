# Reference

Background reading, lsvd comparison, implementation notes, and open questions.

## lsvd Reference Implementation Notes

The [lab47/lsvd](https://github.com/lab47/lsvd) Go implementation is the primary reference. Key design decisions we studied and how they influenced palimpsest:

### lsvd local directory layout

```
<volume-dir>/
├── head.map                          — persisted LBA→PBA map (CBOR-encoded,
│                                       SHA-256 of segment list as freshness guard)
├── writecache.<ULID>                 — active WAL (receiving writes)
├── writecache.<ULID>                 — old WAL(s) queued for promotion (up to 20)
├── segments/
│   ├── segment.<ULID>               — single-file segment:
│   │                                   [SegmentHeader 8B][varint index][data body]
│   └── segment.<ULID>.complete      — transient during Flush(); renamed to final
└── volumes/
    └── <volName>/
        └── segments                 — binary: appended 16-byte ULIDs, one per segment
```

WAL files live at the root alongside `head.map`. There is no upload-state distinction in the directory layout because S3 upload is synchronous within the background promotion goroutine — by the time a WAL is deleted, its segment is already in S3 and `volumes/<vol>/segments` has been updated. Everything in `segments/` is guaranteed to be in S3.

### Palimpsest local directory layout (comparison)

```
<live-node-dir>/
├── lba.map                          — optional persisted LBA map (rebuilt from segment headers if stale)
├── wal/
│   └── <ULID>                      — WAL file(s): active or awaiting promotion
├── pending/
│   └── <ULID>                      — segment file committed locally, S3 upload pending
├── segments/
│   └── <ULID>                      — segment file confirmed uploaded to S3 (evictable)
└── children/                        — snapshot and fork children (absent until first snapshot)
    └── <ULID>/                      — child node directory

<parent-node-dir>/                   — frozen; no wal/ or pending/
├── segments/
│   └── <ULID>                      — segment file (read-only)
└── children/
    └── <ULID>/                      — child node directory
```

The `pending/` directory exists because palimpsest decouples local promotion from S3 upload. lsvd has no equivalent — it never has locally-committed segments that aren't yet in S3. The three-directory structure makes the full lifecycle visible via `ls`: `wal/` = in flight, `pending/` = local only, `segments/` = safely in S3.

| | lsvd | Palimpsest |
|---|---|---|
| WAL location | Root-level `writecache.<ULID>` | `wal/` subdir |
| Segment format | Single file (index embedded in body) | Single file (header + index + inline + body + delta) |
| Upload tracking | Not needed (S3 sync in promotion) | `pending/` vs `segments/` dirs |
| Temp files | `segment.<ULID>.complete` | `pending/<ULID>.tmp` |
| LBA map | `head.map` (CBOR, SHA-256 guard) | `lba.map` (optional; rebuilt from segment index sections) |
| Eviction policy | Not applicable | `segments/` evictable; `pending/` never |
| Snapshot model | `lowers` array (read-only lower disks) | Directory tree (ancestors are frozen nodes) |
| Dedup | Not implemented | Opportunistic on write path; local tree + best-effort cross-volume via shared root |

**Segment format:** lsvd uses a single file per segment: `[SegmentHeader (8 bytes)][ExtentHeaders (varint-encoded)][body data]` — all metadata embedded in the body. Palimpsest also uses a single file, but with a four-section layout (header + index + inline + body + delta) that allows the index section to be fetched independently via byte-range GET, avoiding retrieval of body data that isn't needed. The local file and S3 object use the same format; the S3 object may additionally carry a delta body computed at upload time.

**Snapshot / lower-disk model:** lsvd implements layering via a `lowers` parameter — an array of read-only disk handles that the read path falls through. Palimpsest encodes the same relationship in the directory tree: ancestor directories are the "lower disks", their absence of `wal/` enforces read-only semantics, and the ancestry is directly inspectable via `ls`.

**GC asymmetry:** in lsvd, `removeSegmentIfPossible()` prevents deleting a segment referenced by any volume, making lower-disk segments effectively immutable while any volume uses them. Palimpsest enforces this structurally: GC only targets nodes containing `wal/`; frozen nodes are never selected.

**Write log format:** single append-only file, one record per finalised extent. Palimpsest's write log follows this shape, adding the BLAKE3 hash and a `FLAG_DEDUP_REF` record type (absent in lsvd which is LBA-addressed, not content-addressed). Unlike lsvd, palimpsest writes a **clean segment body** at promotion time rather than renaming the WAL directly — the WAL format includes recovery headers that are not part of the segment format.

**Async promotion:** lsvd's `closeSegmentAsync()` sends to a background goroutine, but within that goroutine `Flush()` calls `UploadSegment()` synchronously — the LBA map is not updated until after S3 upload. Palimpsest decouples local promotion from S3 upload entirely: the WAL is committed as a local segment and the LBA map is updated without waiting for S3.

**Durability equivalence:** both have the same fundamental durability guarantee — local NVMe is the boundary, not S3. At any moment lsvd has one active WAL + up to 20 queued old WALs not yet in S3. Palimpsest has one active WAL + some number of promoted-but-not-yet-uploaded local segments. In both cases, total local disk failure loses data that the guest's fsync acknowledged.

**LBA map vs content-addressed:** lsvd's `lba2pba` maps `LBA → segment+offset` (physical). GC repacking must update it for every moved extent. Palimpsest's LBA map is `LBA → hash` (logical); physical location is tracked separately in the extent index. GC repacking updates only the extent index; the LBA map is unaffected.

**Pre-log coalescing:** lsvd's `nbdWrapper` buffers up to 20 contiguous blocks in a `pendingWrite` buffer. Palimpsest follows the same pattern; the count limit is a tuning parameter.

**Fsync handling:** `writeLog()` calls `bufio.Flush()` after each extent (OS buffer, not disk). The actual fsync happens only when the guest issues a flush. Palimpsest's `WriteLog::fsync()` follows this exactly.

**Compression:** lsvd uses LZ4 with an entropy threshold of 7.0 bits/byte and a minimum compression ratio of 1.5×. Palimpsest uses zstd (already a dependency) with the same 7.0-bit entropy threshold as a starting point.

**S3 read path — chunk-range reads, not full segment downloads:** lsvd never downloads entire segment files from S3. Instead it issues byte-range `GetObject` requests for chunk-sized slices of segment bodies — the chunk covering the needed extent(s) plus spatial neighbours. A two-level cache sits in front of S3: a 1GB mmap'd disk cache (1MB chunks, LRU eviction) that stores recently-fetched chunks so nearby extents avoid a second round-trip, and a 256-entry LRU of open `SegmentReader` handles (either local file handles or live S3 object reader connections) to avoid re-establishing connections. For palimpsest, the single-entry file handle cache we maintain today maps to lsvd's `LocalFile` reader. When S3 is added, the equivalent of lsvd's 1MB chunk disk cache is the right model for the cold-fetch path — not downloading whole segments.

**Compression and read amplification:** extents are compressed as a unit. A read of a single block from a multi-block compressed extent requires decompressing the entire extent — the stored bytes cannot be seeked into at block granularity. This means the maximum extent size directly bounds worst-case read amplification: a 20-block cap means at most 80KB decompressed to serve a 4KB read (20×). The pre-log coalescing block limit therefore doubles as a read amplification cap and must be chosen with both concerns in mind.

---

## Implementation Notes

**The NBD server is a development and testing tool.** The production block device frontend will be ublk (Linux, io_uring-based). NBD is kept for development convenience and macOS compatibility during local testing. This is architecturally identical to the lab47/lsvd reference implementation, which also exposes an NBD device. Palimpsest's NBD server listens on TCP rather than a Unix socket purely for convenience during dev/test (e.g. connecting a VM running under Multipass or QEMU without configuring shared sockets). No design decisions should be made to optimise the NBD path at the expense of the ublk path.

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
- **Pre-log coalescing block limit:** lsvd uses 20 blocks. The right value for palimpsest depends on typical write burst sizes, acceptable memory footprint between fsyncs, and worst-case read amplification when compression is enabled (see above). The limit must be enforced at the write path even when the NBD layer delivers larger contiguous writes — splitting oversized writes into capped extents is preferable to unbounded amplification.
- **LBA map cache invalidation:** validate the cached `lba.map` against a hash of the current segment IDs across the full ancestor tree, not just the live node.
- **Delta segment threshold:** not every segment needs a delta body — only useful when changed extents have known prior versions in the ancestor tree. Criteria for when to compute and upload a delta body need empirical validation.
- **Boot hint persistence:** where are hint sets stored, how are they distributed across hosts?
- **Empirical validation of repacking benefit:** measure segment fetch count before and after access-pattern-driven repacking.
- **ublk integration:** Linux-only, io_uring-based. NBD is the dev/test frontend; ublk is the production target.
