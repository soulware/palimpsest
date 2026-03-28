# lsvd Reference Implementation Notes

The [lab47/lsvd](https://github.com/lab47/lsvd) Go implementation is the primary reference. Key design decisions we studied and how they influenced Elide:

## lsvd local directory layout

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

## Elide local directory layout (comparison)

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

The `pending/` directory exists because Elide decouples local promotion from S3 upload. lsvd has no equivalent — it never has locally-committed segments that aren't yet in S3. The three-directory structure makes the full lifecycle visible via `ls`: `wal/` = in flight, `pending/` = local only, `segments/` = safely in S3.

| | lsvd | Elide |
|---|---|---|
| WAL location | Root-level `writecache.<ULID>` | `wal/` subdir |
| Segment format | Single file (index embedded in body) | Single file (header + index + inline + body + delta) |
| Upload tracking | Not needed (S3 sync in promotion) | `pending/` vs `segments/` dirs |
| Temp files | `segment.<ULID>.complete` | `pending/<ULID>.tmp` |
| LBA map | `head.map` (CBOR, SHA-256 guard) | `lba.map` (optional; rebuilt from segment index sections) |
| Eviction policy | Not applicable | `segments/` evictable; `pending/` never |
| Snapshot model | `lowers` array (read-only lower disks) | Directory tree (ancestors are frozen nodes) |
| Dedup | Not implemented | Opportunistic on write path; local tree + best-effort cross-volume via shared root |

## Design decision comparisons

**Segment format:** lsvd uses a single file per segment: `[SegmentHeader (8 bytes)][ExtentHeaders (varint-encoded)][body data]` — all metadata embedded in the body. Elide also uses a single file, but with a four-section layout (header + index + inline + body + delta) that allows the index section to be fetched independently via byte-range GET, avoiding retrieval of body data that isn't needed. The local file and S3 object use the same format; the S3 object may additionally carry a delta body computed at upload time.

**Snapshot / lower-disk model:** lsvd implements layering via a `lowers` parameter — an array of read-only disk handles that the read path falls through. Elide encodes the same relationship in the directory tree: ancestor directories are the "lower disks", their absence of `wal/` enforces read-only semantics, and the ancestry is directly inspectable via `ls`.

**GC asymmetry:** in lsvd, `removeSegmentIfPossible()` prevents deleting a segment referenced by any volume, making lower-disk segments effectively immutable while any volume uses them. Elide enforces this structurally: GC only targets nodes containing `wal/`; frozen nodes are never selected.

**Write log format:** single append-only file, one record per finalised extent. Elide's write log follows this shape, adding the BLAKE3 hash and a `FLAG_DEDUP_REF` record type (absent in lsvd which is LBA-addressed, not content-addressed). Unlike lsvd, Elide writes a **clean segment body** at promotion time rather than renaming the WAL directly — the WAL format includes recovery headers that are not part of the segment format.

**Async promotion:** lsvd's `closeSegmentAsync()` sends to a background goroutine, but within that goroutine `Flush()` calls `UploadSegment()` synchronously — the LBA map is not updated until after S3 upload. Elide decouples local promotion from S3 upload entirely: the WAL is committed as a local segment and the LBA map is updated without waiting for S3.

**Durability equivalence:** both have the same fundamental durability guarantee — local NVMe is the boundary, not S3. At any moment lsvd has one active WAL + up to 20 queued old WALs not yet in S3. Elide has one active WAL + some number of promoted-but-not-yet-uploaded local segments. In both cases, total local disk failure loses data that the guest's fsync acknowledged.

**LBA map vs content-addressed:** lsvd's `lba2pba` maps `LBA → segment+offset` (physical). GC repacking must update it for every moved extent. Elide's LBA map is `LBA → hash` (logical); physical location is tracked separately in the extent index. GC repacking updates only the extent index; the LBA map is unaffected.

**Pre-log coalescing:** lsvd's `nbdWrapper` buffers up to 20 contiguous blocks in a `pendingWrite` buffer. Elide follows the same pattern; the count limit is a tuning parameter.

**Fsync handling:** `writeLog()` calls `bufio.Flush()` after each extent (OS buffer, not disk). The actual fsync happens only when the guest issues a flush. Elide's `WriteLog::fsync()` follows this exactly.

**Compression:** lsvd uses LZ4 with an entropy threshold of 7.0 bits/byte and a minimum compression ratio of 1.5×. Elide uses zstd (already a dependency) with the same 7.0-bit entropy threshold as a starting point.

**S3 read path — chunk-range reads, not full segment downloads:** lsvd never downloads entire segment files from S3. Instead it issues byte-range `GetObject` requests for chunk-sized slices of segment bodies — the chunk covering the needed extent(s) plus spatial neighbours. A two-level cache sits in front of S3: a 1GB mmap'd disk cache (1MB chunks, LRU eviction) that stores recently-fetched chunks so nearby extents avoid a second round-trip, and a 256-entry LRU of open `SegmentReader` handles (either local file handles or live S3 object reader connections) to avoid re-establishing connections. For Elide, the single-entry file handle cache we maintain today maps to lsvd's `LocalFile` reader. When S3 is added, the equivalent of lsvd's 1MB chunk disk cache is the right model for the cold-fetch path — not downloading whole segments.

**Compression and read amplification:** extents are compressed as a unit. A read of a single block from a multi-block compressed extent requires decompressing the entire extent — the stored bytes cannot be seeked into at block granularity. This means the maximum extent size directly bounds worst-case read amplification: a 20-block cap means at most 80KB decompressed to serve a 4KB read (20×). The pre-log coalescing block limit therefore doubles as a read amplification cap and must be chosen with both concerns in mind.
