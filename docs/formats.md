# Formats

On-disk and on-wire formats for the write log (WAL) and segment files.

## Write Log

The write log is the local durability boundary. Writes land here on fsync; the log is promoted to a segment in the background.

### File format

A single append-only file per in-progress segment, living at `wal/<ULID>`. One file, records appended sequentially, no separate index.

**Magic header:** `ELIDWAL\x01` (8 bytes)

**ZERO_HASH sentinel:** `[0x00; 32]` — 32 zero bytes. This is a reserved constant used in WAL and segment entries to represent a zero extent (see below). It is safe as a sentinel because finding a BLAKE3 preimage that produces all-zero output is computationally infeasible; no real extent hash will ever equal it.

**Record types:**

*DATA record* — a new extent with its payload:
```
hash        (32 bytes)    BLAKE3 extent hash
start_lba   (u64 varint)  first logical block address
lba_length  (u32 varint)  extent length in 4KB blocks
flags       (u8)          see flag bits below
data_length (u32 varint)  byte length of payload (compressed size if FLAG_COMPRESSED)
data        (data_length bytes)
```

*REF record* — a thin dedup reference; no data payload:
```
hash         (32 bytes)    BLAKE3 hash of the extent
start_lba    (u64 varint)
lba_length   (u32 varint)
flags        (u8)          FLAG_DEDUP_REF set; no further fields
```

REF records carry no body bytes anywhere — not in the WAL, not in `pending/`, not in `cache/`, not in the uploaded S3 object. The hash is a key into the extent index (`hash → canonical segment ULID + body offset`), which is used to serve reads from the canonical body. The DedupRef index entry has `stored_offset = 0` and `stored_length = 0`; it reserves no body space and the segment's `body_length` excludes it entirely. See § Segment File Format — FLAG_DEDUP_REF for the two invariants (first-snapshot pinning and canonical presence) that make thin DedupRef sound.

*ZERO record* — a zero extent; no data payload, maps an LBA range to zeros:
```
hash        (32 bytes)    ZERO_HASH ([0x00; 32])
start_lba   (u64 varint)
lba_length  (u32 varint)
flags       (u8)          FLAG_ZERO set; no further fields
```

Zero extents differ from unwritten regions in one important way: an unwritten LBA range in a descendant falls through to the ancestor layer during LBA map reconstruction. A zero extent explicitly overrides the ancestor — any ancestor data at those LBAs is masked.

**Flag bits:**
- `0x01` `FLAG_COMPRESSED` — payload is zstd-compressed; `data_length` is compressed size
- `0x02` `FLAG_DEDUP_REF` — REF record; no data payload (thin; body lives in canonical segment)
- `0x04` `FLAG_ZERO` — ZERO record; no data payload; hash field is ZERO_HASH

**Flag namespace note:** WAL flag bits and segment index flag bits are **distinct namespaces with different values**. When promoting WAL records to segment entries, `recover_wal` must translate between them:

| Meaning | WAL bit | Segment bit |
|---|---|---|
| `FLAG_COMPRESSED` | `0x01` | `0x04` |
| `FLAG_DEDUP_REF` | `0x02` | `0x08` |
| `FLAG_ZERO`      | `0x04` | `0x10` |

The segment format also has `FLAG_INLINE` (`0x01`) and `FLAG_HAS_DELTAS` (`0x02`), which have no WAL equivalents. Never copy a WAL `flags` byte directly into a segment index entry.

For DATA and REF records, the hash is computed before the dedup check and stored in the log record. Recovery can reconstruct the LBA map without re-reading or re-hashing the data. ZERO records carry ZERO_HASH as a fixed sentinel — no hash computation is performed.

### Pre-log coalescing

Contiguous LBA writes are merged in memory before they reach the write log — in the NBD/ublk handler, not in the log itself. This mirrors lsvd's `pendingWrite` buffer. The coalescing window is bounded by both a block count limit (to prevent unbounded memory accumulation between fsyncs) and the fsync boundary (a guest fsync flushes any pending buffer). The write log only ever sees finalised, already-coalesced extents.

### Durability model

```
write arrives → in-memory coalescing buffer
                        │
               count limit or fsync
                        │
                        ▼
               hash → local dedup check → append_data / append_ref / append_zero → bufio (OS buffer)
                        │
                    guest fsync
                        │
                        ▼
               logF.sync_data() ← write log durable on local disk; reply sent to guest
                        │
                [background, async]
                        │
                        ▼
               segment close → clean segment file written → S3 upload
```

After a guest fsync returns, all prior writes are durable in the write log on local NVMe. S3 upload is asynchronous and not on the fsync critical path.

### Crash recovery

On startup, if a write log file exists, `scan()` reads it sequentially. If a partial record is found at the tail (power loss mid-write), the file is truncated to the last complete record. All complete records are replayed to reconstruct the in-memory LBA map. The write log is then reopened for continued appending.

**Failure scenarios:**

| Scenario | State on restart | Recovery |
|---|---|---|
| Crash mid-write (before fsync) | WAL tail partial | Truncate WAL to last complete record; replay |
| Crash after fsync, before promotion starts | `wal/<ULID>` intact; nothing in `pending/` | Replay WAL; promote normally |
| Crash during segment file write (steps 1–2) | `pending/<ULID>.tmp` may exist; WAL intact | Delete `.tmp`; replay WAL |
| Crash after rename, before WAL delete (steps 3–4) | Both `pending/<ULID>` and `wal/<ULID>` exist | Delete WAL; use pending segment |
| Crash after WAL delete, before LBA map update (steps 4–5) | `pending/<ULID>` present; no WAL | Rebuild LBA map from pending segment header + index |
| Crash mid-upload or after upload before rename (steps 6–8) | Segment still in `pending/`; may be in S3 already | Retry upload (idempotent); rename on success |
| Total local disk loss | All local state gone | Data loss bounded to writes not yet in S3 — same guarantee as a local SSD |

The final row is an intentional design choice: local NVMe is the durability boundary, matching the stated goal of "durability semantics similar to a local SSD". S3 is async offload, not the primary durability mechanism.

### Promotion to segment

When the write log reaches the 32MB threshold (or on an explicit flush), the background promotion task converts the WAL into a committed local segment. The WAL is assigned a ULID at creation time; that same ULID becomes the segment ID.

**The WAL ULID marks the start of a write epoch, not the time data was written.**  All writes accepted while the WAL is open belong to that epoch and inherit its ULID when promoted.  This pre-assignment is what makes compaction ordering safe: every segment in `pending/` was produced in an earlier epoch, so `max(pending ULIDs)` is always strictly less than the running WAL's ULID — there is no need to coordinate with the live WAL during compaction.

**Promotion writes a clean segment file.** The WAL format includes per-record headers that are useful for recovery but should not be part of the permanent segment format. Promotion reads the WAL sequentially and writes the raw extent data bytes (no headers) to a clean body section. DATA records contribute body bytes; DedupRef and ZERO records contribute no bytes and reserve no body space (`stored_offset = 0`, `stored_length = 0`). All segments — freshly promoted or GC-repacked — have the same uniform 64-byte index entry format.

**`redact_segment` (in-place dead-data hole-punching).** Before the coordinator reads a `pending/<ULID>` segment for S3 upload, it calls `redact_segment(ulid)` IPC on the volume. The volume hole-punches any hash-dead DATA entries **in place on the original `pending/<ULID>` file**: no sidecar, no copy, no rename, no layout change — only the physical byte ranges of dead DATA regions are freed. Hash-dead means: the LBA no longer maps to this hash, and no other live LBA anywhere in the volume references this hash (so there is no concurrent reader for those bytes). DedupRef entries are not touched: the thin format carries no DedupRef body bytes in the first place. The `body_length`, index section, signature, and file size are all unchanged. The operation is idempotent — re-running redact on a segment with no hash-dead entries is a no-op. Fast-path: segments with zero hash-dead entries return immediately.

The name `redact` reflects the intent: the sole purpose of this step is to ensure deleted data never leaves the host via S3 upload. Deferring it to upload time (rather than doing it at overwrite time) lets multiple dead hashes accumulate in a single `redact` pass and avoids coupling the write path to hash-liveness bookkeeping.

**WAL-to-segment flag translation:** WAL and segment index use different bit values for `FLAG_COMPRESSED` and `FLAG_DEDUP_REF` (see the WAL flag namespace note above). `recover_wal` translates WAL flags to segment flags before constructing `SegmentEntry` values — never copy a WAL `flags` byte directly into a segment index entry.

**Directory layout within a live node:**

```
wal/<ULID>              — WAL file (active or awaiting promotion)
pending/<ULID>          — segment file committed locally, S3 upload pending
index/<ULID>.idx        — header + index + inline; written on S3 confirmation; always retained
cache/<ULID>.body       — sparse body file; body-relative byte offsets; evictable
cache/<ULID>.present    — presence bitset; one bit per index entry; evictable with .body
```

`pending/` is the write-path holding area; `index/` and `cache/` together are the canonical post-upload local form, used by both self-written and demand-fetched segments:

```
wal/<ULID>  →  pending/<ULID>  →  S3 upload  →  index/<ULID>.idx + cache/<ULID>.{body,present}
                                                                        ↑
                                                   demand-fetch from S3 ┘
```

A `pending/<ULID>` file holds the full segment in the same format used in S3 minus the delta body (which the coordinator appends at upload time). Once the coordinator confirms upload, the volume writes `index/<ULID>.idx` plus `cache/<ULID>.body` + `.present` (with all bits set, since the full body is on hand) and then deletes `pending/<ULID>`. Demand-fetched segments arrive in the same shape: `index/<ULID>.idx` written first, body bytes populated incrementally into `cache/<ULID>.body` with corresponding bits in `.present`. There is no separate "uploaded full-file" local state — the cache triplet is the only post-upload form.

`wal/` normally contains one entry — the active WAL — but can contain two during the brief promotion window. On crash recovery all files in `wal/` are treated identically: scan, truncate partial tail, promote.

`pending/` segments are the only local copy of their data; they must not be evicted. `cache/<ULID>.body` and `.present` are S3-backed and freely evictable under space pressure; `index/<ULID>.idx` is the S3-confirmation marker and must be retained even after body eviction. No list files are needed — the filesystem is the index.

**Commit ordering:**

```
1. Build index section in memory from WAL extent list
2. sync_data() on WAL file
3. Write pending/<ULID>.tmp: header + index + inline + body (DATA extents only, no headers)
4. sync_data() on pending/<ULID>.tmp
5. Rename pending/<ULID>.tmp → pending/<ULID>            ← COMMIT POINT
6. fsync() on pending/ directory                         ← makes rename durable
7. Delete wal/<ULID>
8. Update LBA map in memory
```

Step 5 is the commit point — a complete segment file at `pending/<ULID>` means promotion is done. The file is written atomically via rename; there is no window where a partial file is visible as the committed name.

Step 6 — the directory fsync — is required because `rename()` updates the directory entry atomically in the VFS but the entry is only written to disk when the parent directory is fsynced. Without step 6, a machine crash immediately after step 5 could leave the rename uncommitted in the journal; on recovery the `.tmp` file would be the visible state and `pending/<ULID>` would not exist. Step 2 ensures the WAL is intact as a fallback in this case (recovery would replay from the WAL), but the directory fsync closes the gap and ensures the segment is the recovery path rather than the WAL.

The same `rename + fsync_dir` pattern applies to all segment-creating renames: WAL promotion, repack, sweep, and import.

**S3 upload completion:**

```
6. Read pending/<ULID>; choose S3 reduction strategy (if applicable):
   a. Delta compression: compute delta body against ancestor segments;
      S3 object = local file + appended delta body (header and index updated with delta offsets)
   b. Sparse: compare extents block-by-block against ancestor; build a fresh S3 object
      containing only changed-block extents; S3 manifest reflects the sparse LBA map
   c. Neither: upload local file as-is. The local file is already thin:
      DedupRef entries contribute no body bytes (stored_length=0),
      and hash-dead DATA entries have been hole-punched in place by redact_segment.
7. Upload S3 object
8. Volume writes index/<ULID>.idx and cache/<ULID>.{body,present} (full body, all bits set); deletes pending/<ULID>
```

The local `pending/<ulid>` file and the uploaded S3 object share the **same format-level thin layout**: `body_length` is the sum of DATA and INLINE `stored_length` only; DedupRef entries carry no body bytes anywhere. Dead-DATA body regions are hole-punched by `materialise_segment` on Linux (zero-written on other platforms) before upload. There is no "local optimisation vs S3" asymmetry — the thin layout is the only layout. See § Segment File Format — FLAG_DEDUP_REF for the invariants that make this sound.

Under **delta compression** the S3 object is derived from the local file (body section identical, delta body appended, header/index updated). The local file can be streamed directly.

Under the **sparse** strategy the S3 object diverges structurally from the local file: its index section contains entries for changed blocks only, not the original full-extent entries. The coordinator builds the S3 object fresh. H_new (the full extent hash) is not registered in the S3 extent index — only the changed block hashes are. The local segment retains H_new as a full DATA record; local reads are unaffected.

The two strategies are not mutually exclusive: sparse can be applied first (skip unchanged blocks), and delta compression applied to the changed blocks that remain. See [architecture.md](architecture.md) for the trade-off comparison.

**On startup:** scan all three directories within the live node. Each maps to one recovery action:
- `wal/` — replay (truncate partial tail if needed) and promote
- `pending/` — read header + index section for LBA map rebuild; queue S3 upload
- `index/*.idx` — read header + index section for LBA map rebuild

Then scan ancestor nodes' `index/` directories (no `wal/` or `pending/` — they are frozen), oldest ancestor first, to build the full merged LBA map.

---

## Segment File Format

Each segment is a **single file** both locally and in S3. The same format is used throughout — the local `pending/<ULID>` file is the S3 object minus the delta body, which the coordinator appends at upload time.

### File layout

```
[Header: 100 bytes]
  magic          (8 bytes)  — "ELIDSEG\x05"
  entry_count    (4 bytes)  — number of index entries (u32 le)
  index_length   (4 bytes)  — byte length of index section (u32 le)
  inline_length  (4 bytes)  — byte length of inline section (u32 le); 0 if none
  body_length    (8 bytes)  — byte length of full extent body (u64 le)
  delta_length   (4 bytes)  — byte length of delta body (u32 le); 0 if no deltas
  inputs_length  (4 bytes)  — byte length of inputs table (u32 le); multiple of 16
  signature      (64 bytes) — Ed25519 signature (see Fork ownership and signing below)

[Index section]             — starts at byte 100; length = index_length
[Inline section]            — starts at byte 100 + index_length; length = inline_length
[Full body]                 — starts at byte 100 + index_length + inline_length; length = body_length
[Delta body]                — starts at byte 100 + index_length + inline_length + body_length; length = delta_length
```

Derived section offsets (computable from the header alone):
```
index_offset  = 100
inline_offset = 100 + index_length
body_offset   = 100 + index_length + inline_length
delta_offset  = 100 + index_length + inline_length + body_length
```

**The full body** is raw concatenated extent data — DATA-record and REF-record extents, clean bytes, no framing. ZERO-record extents contribute nothing to the body (reads return zeros directly). All navigation is via the index section.

**The delta body** is raw concatenated delta blobs, referenced by byte offset from the index section. Absent on locally-stored segment files (`delta_length = 0`); present on S3 objects when the coordinator has computed deltas against ancestor segments.

**The inline section** holds raw bytes for inlined extents and inlined delta blobs. It is placed before the full body so a single `GET [0, body_offset)` retrieves the header, index, and all inline data together — sufficient for a warm-start client to serve all small extents without fetching the body at all.

### Index section entry format

**Flag bits** (1 byte per entry):
- `0x01` `FLAG_INLINE` — extent data is in the inline section; no body fetch needed
- `0x02` `FLAG_HAS_DELTAS` — one or more delta options follow
- `0x04` `FLAG_COMPRESSED` — stored data is compressed; lengths are compressed sizes
- `0x08` `FLAG_DEDUP_REF` — dedup reference; no body bytes, no body reservation (`stored_offset = 0`, `stored_length = 0`). Entry layout is the same 64 bytes as DATA. See § Segment File Format — FLAG_DEDUP_REF below.
- `0x10` `FLAG_ZERO` — zero extent; hash field is ZERO_HASH; no body in this segment; reads as zeros
- `0x20` `FLAG_DELTA` — thin delta entry; no body bytes, no body reservation (`stored_offset = 0`, `stored_length = 0`). Entry implies `FLAG_HAS_DELTAS` and must have at least one delta option; the content is served by fetching a delta blob from the segment's delta body section and decompressing it against the `source_hash` extent body located via the extent index. See § Segment File Format — FLAG_DELTA below.
- `0x40` `FLAG_CANONICAL_ONLY` — canonical-body-only entry. Co-exists with DATA (body in body section) or `FLAG_INLINE` (body in inline section); incompatible with `FLAG_DEDUP_REF`, `FLAG_ZERO`, `FLAG_DELTA`. The entry carries body bytes (its hash is canonical in this segment, resolvable via `extent_index.lookup`) but makes **no LBA claim**: `start_lba` and `lba_length` are serialised as zero and the entry is skipped by `lbamap::rebuild_segments`. Emitted by GC when a DATA/INLINE entry's original LBA has been overwritten (LBA-dead) but its hash is still referenced elsewhere via a DedupRef. Preserves the canonical body for dedup resolution without re-asserting the stale LBA→hash binding on rebuild. See § Segment File Format — FLAG_CANONICAL_ONLY below.

**Compression algorithm:** lz4_flex (LZ4) is used for all locally-written body extents. LZ4 decompresses at ~4 GB/s on modern hardware, well above local disk bandwidth, so the decompression cost per read is negligible relative to the I/O. This matches the lsvd reference implementation, which uses LZ4 for the same reason.

**Delta bodies** use zstd dictionary compression (`FLAG_HAS_DELTAS` option entries). The source extent is used as the zstd dictionary; the delta blob is much smaller than the full extent for in-place file updates. Delta blobs are computed in-process by `elide-import` after pending segments are written but before `serve_promote` publishes the control socket, and appended to the segment's delta body section. `FLAG_COMPRESSED` continues to apply uniformly to full-body entries (LZ4); the algorithm is implied by context (full-body entry = LZ4, delta option = zstd).

Both algorithms apply the same entropy gate (≥ 7.0 bits/byte skips compression) and minimum ratio threshold (< 1.5× skips storage).

**Compression granularity:** `FLAG_COMPRESSED` applies to the full stored payload of an entry — the entire extent is compressed as a unit. There is no sub-extent compression granularity. A read of any portion of a compressed extent must decompress the full payload. This matches lsvd. The practical impact is bounded by the pre-log coalescing block limit, which caps maximum extent size at write time.

The index section has two parts: fixed-size base entries, followed by a delta table.

**Base entries** (`entry_count × 64 bytes`):

```
For each extent (64 bytes, fixed-size):
  hash            (32 bytes) — BLAKE3 extent hash
  start_lba       (8 bytes)  — first logical block address (u64 le)
  lba_length      (4 bytes)  — extent length in 4KB blocks (u32 le)
  flags           (1 byte)   — flag bits above
  stored_offset   (8 bytes)  — byte offset within body or inline section (u64 le)
  stored_length   (4 bytes)  — byte length of stored data (u32 le)
  reserved        (7 bytes)  — must be zero
```

All entry kinds (DATA, DEDUP_REF, ZERO, INLINE, and DELTA) use the same 64-byte layout. `stored_offset` and `stored_length` are interpreted per kind: body-section-relative for DATA, inline-section-relative for INLINE, zero for DEDUP_REF, ZERO, and DELTA.

**Delta table** (appended after base entries, variable-length):

```
Per entry with deltas:
  entry_index     (4 bytes)  — index of the base entry (u32 le)
  delta_count     (1 byte)   — number of delta options (≥1)
  per delta option (77 bytes):
    source_hash   (32 bytes) — BLAKE3 hash of the source extent
    option_flags  (1 byte)   — bit 0: FLAG_DELTA_INLINE (reserved)
    delta_offset  (8 bytes)  — byte offset within delta body section (u64 le)
    delta_length  (4 bytes)  — byte length in delta body (u32 le)
    delta_hash    (32 bytes) — BLAKE3 hash of the compressed delta blob;
                               authenticates the delta bytes at
                               [delta_offset, delta_offset + delta_length)
                               (the delta body section is outside the segment
                               signature, so each option's delta_hash is the
                               sole authentication of its blob, verified on
                               demand-fetch before the blob is cached)
```

The delta table is only present when at least one entry has `FLAG_HAS_DELTAS` set. Its total length is `index_length - (entry_count × 64) - inputs_length`.

**Inputs table** (appended after the delta table, at the tail of the index section):

```
inputs_length / 16 entries, 16 bytes each:
  input_ulid    (16 bytes) — ULID of a source segment consumed to produce this output
```

Populated only for GC outputs (the coordinator's `compact_segments` writes the sorted list of swept input ULIDs into this region). Empty for normal WAL-promote segments and demand-fetch results. The volume's apply path reads this field to derive extent-index updates from the segment itself, without consulting any sidecar manifest — the whole self-describing GC handoff protocol is built on top of this. Because the inputs table is the tail of the index section, the existing signature `Ed25519(BLAKE3(header[0..36] || index_bytes))` authenticates it without a second hash region.

`lba_length × 4096` always gives the uncompressed extent size. `stored_length` gives the stored (possibly compressed) size.

**FLAG_DEDUP_REF entries** use the same 64-byte index layout as DATA entries but carry no body bytes and reserve no body space. `stored_offset` and `stored_length` are both zero. The segment's `body_length` is the sum of `stored_length` over DATA entries only — DedupRefs contribute nothing. The write path never seeks past a DedupRef and never allocates a sparse hole for one.

This layout is **unified across `pending/`, `cache/`, and S3**. The local `pending/<ULID>` file, the three-file cache (`.idx` + `.body` + `.present`), and the uploaded S3 object all share the same thin body section. Thin DedupRef delivers S3 storage savings directly; no materialisation or compaction step is required.

Reads of a DedupRef-mapped LBA go through the extent index: `extent_index.lookup(hash)` returns the canonical DATA location, and the read is served from there. The DedupRef entry's own offset and length fields are never consulted by the read path. This holds for local reads, cache reads, and cold-path demand-fetches against S3.

Hash-dead DATA entries (LBA overwritten and hash no longer referenced by any live LBA in the volume) are hole-punched in place on `pending/<ULID>` by `redact_segment` just before upload, so deleted data is never transmitted to S3. DedupRef entries are skipped by redact — they contribute zero body bytes in the thin format and there is nothing to punch.

**Two invariants make thin DedupRef sound:**

1. **Pinning.** Every live DedupRef target is a segment GC cannot rewrite or remove. Snapshot floor + first-snapshot pinning; see `architecture.md § Dedup`.
2. **Canonical presence.** For every live DedupRef hash H, `extent_index.lookup(H)` returns a DATA entry. Maintained by GC's LBA-referenced-hash liveness rule.

In `cache/`, `promote_to_cache` writes a `.body` file sized to the thin `body_length` (no DedupRef holes) and a `.present` bitset marking only DATA entries as present. DedupRef entries have their `.present` bit unset because their canonical bytes are served from a different segment entirely, not from this cache body.

**FLAG_DELTA entries (thin delta).** Same 64-byte index layout as DATA. `stored_offset` and `stored_length` are both zero — a Delta entry reserves no space in the body section of any segment (`pending/`, `cache/`, or S3). The segment's `body_length` excludes Delta entries alongside DedupRef entries; only DATA and INLINE contribute to `body_length`.

A Delta entry implicitly carries `FLAG_HAS_DELTAS` and must have **at least one** delta option in the delta table. Each option records a `source_hash` + `delta_offset` + `delta_length` pointing into this segment's delta body section (or the inline section, with `FLAG_DELTA_INLINE`). The reader picks the first option whose `source_hash` resolves via the local extent index, fetches the delta blob, and decompresses it using the source extent body as the zstd dictionary.

**Delta source must be a DATA entry.** A Delta entry's `source_hash` always resolves via `extent_index.lookup(source_hash)` to a DATA entry — never to another Delta. No delta-of-delta chains. This bounds decompression cost to a single dictionary apply and keeps GC liveness reasoning linear.

**Unified across `pending/`, `cache/`, and S3**, same as DedupRef: the local `pending/<ULID>` file, the three-file cache format, and the uploaded S3 object all agree that Delta entries carry no body bytes. Delta blobs live in the segment's delta body section, which *is* uploaded to S3 and *is* present in the cache file.

Reads of a Delta-mapped LBA:
1. Scan the entry's delta options in order (earliest-source preference; see § Read path in `design-delta-compression.md`).
2. For each option, check `extent_index.lookup(source_hash)`. First hit wins.
3. Fetch the source extent body (local or via demand-fetch).
4. Fetch the delta blob from the segment's delta body section (or inline).
5. Decompress with source as zstd dictionary → materialised extent bytes.

If no option's source resolves, the fetch fails. Unlike entries with `FLAG_HAS_DELTAS` on top of a DATA body, a Delta entry has no full-body fallback in the same segment — this is the price of thin delta, symmetric with thin DedupRef's dependence on `extent_index.lookup()`.

**Two invariants make thin Delta sound**, mirroring the thin DedupRef invariants:

1. **Pinning.** Every live Delta's source extent lives in a segment GC cannot rewrite or remove. Cross-import delta sources come from `extent_index` provenance lineage, which names only fully-snapshotted ancestors (enforced at import time).
2. **Canonical presence.** For every live Delta with source hash H, `extent_index.lookup(H)` returns a DATA entry. Maintained by GC's liveness rule, extended: a DATA entry is kept alive if any live LBA in the volume references its hash directly (Data/DedupRef) or indirectly as a Delta's `source_hash`.

The **`lba_referenced_hashes()`** set is extended to include delta source hashes for every live Delta LBA. GC's existing rule ("keep DATA alive iff any live LBA references its hash") then covers delta sources with no new code path.

**GC** carries Delta entries through `compact_candidates_inner` unchanged, same as DedupRef: a match arm that debug-asserts `stored_offset == 0, stored_length == 0`, copies the entry through, and skips `fetch_live_bodies` (it contributes no body bytes). The delta blob stays in the source segment's delta body section; GC does not move delta blobs between segments. The segment's delta body section is carried into the GC output as-is.

In `cache/`, `promote_to_cache` treats Delta entries the same as DedupRef entries: `.present` bit unset (delta blob is served from the delta body section, not the `.body` file), no contribution to `.body` size.

**FLAG_ZERO entries** carry only the LBA mapping with ZERO_HASH. No extent index lookup is performed for these entries — the read path returns zeros directly. Zero entries must be present in the segment index (and in the serialised manifest) to correctly mask ancestor data; they are never omitted even though they have no body bytes.

**FLAG_CANONICAL_ONLY entries (canonical body without an LBA claim).** Same 64-byte index layout as DATA/INLINE and the body is stored the same way — in the body section (bare `FLAG_CANONICAL_ONLY`) or the inline section (`FLAG_CANONICAL_ONLY | FLAG_INLINE`). The only differences are:

- `start_lba` and `lba_length` are serialised as **zero** (enforced on write).
- `lbamap::rebuild_segments` **skips** the entry: it contributes nothing to the LBA map.
- `extent_index::rebuild` treats it exactly like DATA/INLINE: the entry *is* canonical for its hash, and `extent_index.lookup(hash)` resolves to its location in this segment.

Emitted only by GC, never by the write path. The trigger: `collect_stats` encounters a DATA/INLINE entry whose original `start_lba` is LBA-dead (`lbamap[start_lba] != hash`) but whose hash is still referenced elsewhere via a DedupRef (`live_hashes.contains(&hash)`). Preserving the entry at its original LBA would re-assert the stale LBA→hash binding on rebuild with a higher ULID than whatever wrote the live content — silently shadowing the correct mapping (bug H; see `elide-coordinator/src/gc.rs::tests::collect_stats_preserves_dead_lba_entry_when_hash_live_elsewhere` for the regression test). Demoting to `FLAG_CANONICAL_ONLY` carries the canonical body forward for DedupRef / Delta source resolution while cleanly dropping the dead LBA claim.

**Compatible with**: DATA (body in body section) or INLINE (body in inline section), with optional `FLAG_COMPRESSED`. **Incompatible with**: `FLAG_DEDUP_REF` (thin, no body), `FLAG_ZERO` (sentinel hash), `FLAG_DELTA` (thin, no body).

DedupRef and Delta consumers are kind-agnostic: `extent_index.lookup(hash)` resolves to a canonical entry regardless of whether it's a plain DATA/INLINE or a `CANONICAL_ONLY`-demoted one.

**FLAG_INLINE extents** store their full data in the inline section. Particularly effective for the boot path: small config files, scripts, and locale data appear frequently during boot and are naturally small. A warm-start client that fetches `[0, body_offset)` gets all inline extents with no further requests.

**Multiple delta options** allow an extent to have deltas against several source extents (e.g. against the immediately prior snapshot and an earlier one). The client picks the first option whose `source_hash` is in its local extent index. If no source is available, the full extent is fetched from the body instead. This provides graceful degradation across skipped releases.

**FLAG_DELTA_INLINE** applies the same logic to delta blobs: a small delta is stored in the inline section, avoiding a byte-range fetch into the delta body.

**Index entries serve two purposes with a single scan:** LBA map reconstruction (`start_lba + lba_length + hash`) and extent index population (`hash → ULID + body_offset + body_length`). No separate pass needed.

### Typical segment file sizes (~1000 extents, ~32MB body)

| Configuration | Index section | Notes |
|---|---|---|
| No deltas | ~64KB | Base case (1000 × 64B) |
| 3 delta options, 16% of extents | ~77KB | Realistic point-release update |
| 3 delta options, all extents | ~204KB | Worst case |

Inline section size depends on the inline threshold (256 bytes stored size) and extent size distribution. Only genuinely tiny compressed extents inline — mostly-zero blocks, small config files. The threshold is deliberately low: at higher thresholds (e.g. 4096), compressed 4 KiB blocks from NBD writes would all inline, bloating the `.idx` and defeating demand-fetch.

### S3 object layout

The store uses two top-level prefixes:

```
by_id/    — one directory per volume ULID; mirrors the local by_id/ layout
names/    — one tiny file per named volume; the name→ULID index
```

**Segment key:**
```
by_id/<volume-ulid>/YYYYMMDD/<segment-ulid>
```

The date is extracted from the segment ULID's embedded millisecond timestamp and formatted as `YYYYMMDD`. Using the ULID timestamp (creation time) rather than upload time makes keys stable and deterministic regardless of when the coordinator drain loop runs.

Example segment key:
```
by_id/01KN4Q7WCJNQ9SCK4KKY5888AJ/20260401/01KN4Q887YGPWMG4CBHCZPZN4Q
```

**Volume public key:**
```
by_id/<volume-ulid>/volume.pub
```

Uploaded once at first drain. Enables segment signature verification on any host (trust-on-first-use).

**Volume provenance:**
```
by_id/<volume-ulid>/volume.provenance
```

Written at import, fork, and create time and uploaded with `volume.pub` so any host can verify lineage and segment signatures locally. The file is a custom line-oriented format (not TOML), Ed25519-signed by the volume's own key. Fields:

- `parent: <parent-ulid>/<snapshot-ulid>` — present on forks only.
- `parent_pubkey: <hex>` — embedded parent verifying key, paired with `parent`.
- `parent_manifest_pubkey: <hex>` — optional override for `.manifest` verification (force-snapshot path).
- `extent_index:` — flat list of `<source-ulid>/<snapshot-ulid>` entries for hash-pool ancestors (delta compression).
- `oci_image:` / `oci_digest:` / `oci_arch:` — present together iff this volume is an OCI-imported root. Forks of an imported volume don't inherit them.
- `sig:` — 64-byte Ed25519 signature over the canonical signing input.

`size` is **not** here — it lives on the `names/<name>` claim record (see [design-volume-size-ownership.md](design-volume-size-ownership.md)). `name` is **not** here either — `names/<name>` is the canonical name→ulid index.

**Snapshot markers:**
```
by_id/<volume-ulid>/snapshots/YYYYMMDD/<snapshot-ulid>
```

Uploaded eagerly after each `volume snapshot` and at the end of import. The date prefix matches the snapshot ULID timestamp, consistent with segment keys. Enables a pulling host to enumerate valid fork branch points.

**Name index entry:**
```
names/<name>
```

Content: the volume ULID, plain text. Written (or atomically overwritten) at import, fork, and create time. A single `LIST names/` returns all named volumes in the store regardless of how many ULIDs exist — no per-volume GETs needed.

Benefits of this layout:
- `LIST by_id/<ulid>/YYYYMMDD/` returns all segments for a volume on a given day — useful for GC audit
- `LIST by_id/<ulid>/snapshots/` returns all branch points — used during the `volume claim` ancestry walk
- `LIST names/` returns all named volumes (operator-side enumeration; the CLI no longer exposes a list verb after the `volume remote` removal)
- `by_id/` and `names/` are distinct top-level prefixes; ULIDs and human names can never collide

### Retrieval strategies

The header is 100 bytes; all section offsets are computable from it. This drives three distinct retrieval patterns:

**Cold start** (no local data — cannot use deltas):
```
Single GET of the entire file.
Delta body is at the end; the extra bytes are the cost of one request instead of two.
Parse index section → materialise all extents from body.
```

**Warm start** (some local data):
```
1. GET [0, body_offset)         — header + index + inline; make all fetch decisions
2. GET byte-ranges within body  — full extents needed (ranges coalesced)
3. GET byte-ranges within delta — delta blobs where source is available locally (ranges coalesced)
```

Steps 2 and 3 are independent and can be issued in parallel. Byte ranges within each section are sorted and nearby ranges merged into single GETs before issuing.

**Index-only** (startup LBA map and extent index rebuild):
```
GET [0, inline_offset)          — header + index section only; skip inline, body, delta
```

**Adaptive full-body fetch:** when the ratio of needed body bytes to `body_length` exceeds a threshold, a single GET of the body section is cheaper than many byte-range GETs. Threshold is byte-ratio based (not count-based) since extents are variable size.

### Snapshot indexes

Snapshot indexes are consolidated index-section views written at snapshot time, covering all extents reachable from that snapshot. They are smaller than the full set of per-segment index sections and remain immutable. A snapshot index enables fast cold startup on a new host: download the snapshot index, then download index sections for segments written since the snapshot, union to get the full extent index.

**Index recovery flow:**
```
1. GET snapshot index for the relevant snapshot (if available)
2. GET [0, inline_offset) for each segment written since that snapshot
3. Union → full extent index
```

**Segment files are the ground truth.** All derived structures (in-memory extent index, optional manifest) are caches reconstructible from segment files. On cold start or after index loss, reconstruction is: download index sections of all segment files (fast, small) rather than full segment bodies.

---

## Cache Segment Format

Segments fetched from S3 are **inherently partial**: a newly-arrived segment has its header and index available immediately, but body bytes arrive on demand as specific extents are read. The locally-written form in `pending/` is always complete by construction (written atomically via tmp→rename). Post-upload — whether self-written or demand-fetched — segments live in `index/` (for the `.idx` file) and `cache/` (for `.body` and `.present`) and use a three-file representation.

### Format comparison

| Property | Pre-upload (`pending/`) | Post-upload (`index/` + `cache/`) |
|---|---|---|
| File count | 1 per segment | 3 per segment (`.idx`, `.body`, `.present`) |
| Completeness | Always complete (atomic tmp→rename) | Self-written: full body; demand-fetched: populated incrementally |
| Format | Header + index + inline + body (+ delta on S3) | `.idx`: header + index + inline; `.body`: sparse body bytes |
| Delta section | `delta_length = 0` locally; appended by coordinator at upload | Never stored; materialised into `.body` if a delta path is taken |
| `body_offset` reference point | File-relative (as written) | Body-relative (0 = first byte of body section) — matches `entry.body_offset` directly |
| Presence tracking | N/A — always 100% present | `.present` bitset; one bit per index entry |
| Signature location | `header[32..96]` in the single file | `header[32..96]` in `.idx`; verified before any body fetch |
| Eviction unit | Not evictable | `.body` + `.present` (the body files); `.idx` is retained as the S3-confirmation marker |
| Lifecycle transition | `pending/` → `index/` + `cache/` (after S3 upload; volume writes triplet, deletes pending) | terminal form (no further promotion) |

### File details

**`<ulid>.idx` — header + index + inline**

Exactly the bytes `[0, body_section_start)` of the S3 object, where `body_section_start = 96 + index_length + inline_length`. This is the same slice retrieved by an index-only GET (`GET [0, inline_offset)` from the retrieval strategies section, extended to include the inline section). The existing segment reader parses this unchanged — header fields and index entries are at their normal byte offsets. The `body_length` and `delta_length` fields in the header reflect the full S3 object; they are valid metadata even though the body is not stored in this file.

`.idx` is always fetched first, before any body bytes. It contains everything needed for LBA map and extent index rebuild at `Volume::open`, so a host can open a volume with partial body coverage as long as all `.idx` files are present.

**`<ulid>.body` — sparse body file**

An OS sparse file of `body_length` bytes. Byte offsets within the file are **body-relative**: offset 0 corresponds to the first byte of the body section of the S3 object. This matches `entry.body_offset` directly — no arithmetic is needed to locate an extent in the body file.

Each fetched extent is written at its exact `body_offset` within this file. OS holes represent unfetched ranges. The `.present` bitset is the authoritative record of what is present; OS zeros in a hole are not distinguishable from legitimate zero-filled extent data without it.

DedupRef entries contribute no bytes and no byte ranges to `.body` — they are invisible at the file level. The `.present` bit for a DedupRef index entry is always unset, but this is vestigial: the read path resolves DedupRefs through the extent index, not through the bitset.

Extents materialised from a delta (delta bytes fetched, applied against a local source extent, result written) are stored at the same `body_offset` as the full-body version. From the read path's perspective, materialised delta output is identical to a directly-fetched body extent.

**`<ulid>.present` — presence bitset**

A packed bitset with one bit per index entry (entry N → bit N). Size: `ceil(entry_count / 8)` bytes. A set bit means the bytes `[body_offset, body_offset + body_length)` for that entry are present in `.body` and ready to serve.

Entries that have no locally-available body bytes are not marked present:
- `FLAG_DEDUP_REF` — body region is zero-filled locally; reads resolve through the extent index to the canonical segment. The present bit is unset. If the canonical is evicted, a demand-fetch retrieves the S3 segment (which has the body filled).
- `FLAG_INLINE` — data is in the inline section of `.idx`; no body fetch needed
- `FLAG_ZERO` — zero extent; no bytes in this segment's body; reads return zeros directly

`promote_to_cache` writes the `.present` bitset with bits set only for DATA entries. DedupRef entries start unset because their body bytes are served from the canonical segment, not from the cache body file.

All other entries (standard DATA entries with body bytes) start with their bit unset and are set when the extent is fetched and written to `.body`.

The bitset is written atomically per extent: write the bytes to `.body`, fsync (or at minimum ensure the write is visible), then set the bit. This ordering ensures a set bit is never visible before the corresponding body bytes.

### Fetch sequence

For a read that hits a missing body extent:

```
1. Check cache/<ulid>.present    — check bit N for this extent
   - Set → read from cache/<ulid>.body at entry.body_offset
   - Unset → proceed to step 2
2. Verify signature in index/<ulid>.idx (if not already verified this session)
3. Issue byte-range GET to S3: [body_section_start + entry.body_offset,
                                body_section_start + entry.body_offset + entry.body_length)
   - If a delta option is available and its source_hash is in the local extent index:
     fetch delta bytes instead; apply against local source; write materialised result
4. Write bytes to cache/<ulid>.body at entry.body_offset
5. Set bit N in cache/<ulid>.present
6. Serve from cache/<ulid>.body
```

Coalescing: before issuing GETs, collect all absent extents required for the current read and merge adjacent or nearby `(body_offset, body_length)` ranges into a minimal set of byte-range requests. This is the same coalescing described in the warm-start retrieval strategy.

---

## Fork Ownership and Signing

Each fork has exactly one owner: the host that holds its private key. This is a convention enforced at consumption time — signing does not prevent an unauthorised client from uploading bytes to S3 (that is an access-control concern handled at the object-store level), but it does mean any such upload will be **detected and rejected** when a demand-fetch client verifies the segment.

The single-owner property is also a correctness invariant for ULID-ordered rebuild and GC: because only one process writes new segments into a fork directory, ULID timestamps form an unambiguous total order over the write history with no risk of a second writer injecting segments at an arbitrary position in the sequence. See [architecture.md](architecture.md) — *Single-writer invariant*.

**This is not a key management system.** Elide generates an Ed25519 keypair when a writable volume is created and stores the private key in `volume.key`. There is no key escrow, rotation, revocation, or HSM integration. The guarantee is simple: a client without the private key cannot produce a valid segment signature for that volume. If `volume.key` is copied to another host, that host becomes an equally valid signer — the single-owner property then depends entirely on the operator keeping the key on one machine.

The practical value is narrow but real: it catches the common misconfiguration case where a coordinator is pointed at the wrong fork, and it provides per-segment integrity at demand-fetch time. It does not replace proper access control on the S3 bucket.

"Moving" a fork to a different host is done by creating a new fork from a snapshot on the destination host; the new fork gets a fresh keypair. The original fork's private key is not transferred — that would make two valid signers for the same fork.

### Key files

All volumes use a flat layout with fixed filenames directly in the volume directory. Readonly (imported) volumes have a public key only; writable (forked) volumes have both:

```
<vol_dir>/volume.pub        — Ed25519 public key (32 bytes; uploaded to S3; present on all volumes)
<vol_dir>/volume.provenance — Signed lineage (parent + extent_index) + Ed25519 signature (uploaded to S3; present on all volumes)
<vol_dir>/volume.key        — Ed25519 private key (32 bytes; never uploaded; absent on readonly volumes)
```

S3 locations:
```
by_id/<volume-ulid>/volume.pub
by_id/<volume-ulid>/volume.provenance
```

**Readonly volumes have no private key.** `elide-import` generates an ephemeral keypair in memory, uses it to sign all segments and the provenance file, writes `volume.pub` and `volume.provenance` to disk, then discards the private key. `volume.key` is never written. Since a readonly volume can never accept new writes, the private key has no use after import completes.

**`volume.provenance`** is the signed lineage statement for the volume: which fork parent (if any) it branched from, and which snapshots feed its extent index for dedup / delta compression. Signed by the private key (or ephemeral key for imports) at creation/import time. Uploaded to S3 alongside `volume.pub` so that `remote pull` can materialise the ancestor chain on another host. `serve-volume` verifies the signature against `volume.pub` on every open — readonly and writable alike.

The S3 copies of `volume.pub` and `volume.provenance` enable a pulling host to verify ancestor segments and walk the lineage chain for volumes it does not own. They are not authoritative against a compromised S3 bucket — locally-pinned public keys (see Verification below) are more trustworthy.

### Signing

Every segment is signed. There are no unsigned segments.

**Writable volumes:** signed by the volume's persistent private key (`volume.key`) at **promotion time** — when the WAL is promoted to `pending/<ulid>`. The segment file is complete and signed before it leaves the host.

**Imported readonly volumes:** signed by an ephemeral private key generated in memory by `elide-import`. The key is created before the first segment is written, so all import segments carry a valid signature. The key is discarded after import; `volume.key` is never written to disk.

**Signing input:** `BLAKE3(header[0..32] || index_section_bytes)`

The first 32 bytes of the header (all fields except the 64-byte signature field) are hashed together with the raw bytes of the index section. The signature field at `header[32..96]` is not included — it holds the output.

Signing at promotion/import rather than upload means every copy of the segment (`pending/<ulid>`, the post-upload `index/<ulid>.idx`, and the S3 object) carries the same signature. The coordinator uploads files unchanged; no re-signing step.

Because the private key never leaves the host, **all segment writes for a fork — including GC-compacted and S3-repacked segments — happen on the fork's host**. GC always produces new segments (new ULIDs) rather than modifying existing ones; signed segments are read-only once written.

The coordinator does not hold fork private keys. For S3 repacking, the coordinator acts as an orchestrator: it identifies which extents to consolidate and describes the desired layout, but delegates the actual segment creation and signing to the elide instance that owns the fork. The elide instance writes, signs, and either returns or directly uploads the new segment. This keeps the private key exclusively on the host and allows the elide instance to validate proposed content before signing it.

### Verification

Signature verification happens at **two points**:

**1. Local segment open (LBA map rebuild)**

Every time a volume is opened — by `Volume::open`, `ReadonlyVolume::open`, or either `serve-volume` path — all local segments are read to rebuild the LBA map. Each segment index is verified against `volume.pub` before its entries are accepted into the map:

```
verify Ed25519(sig, BLAKE3(header[0..32] || index_bytes), volume.pub)
```

A segment with an invalid or missing signature causes the open to fail hard (`InvalidData`). There is no skip or warn path. The only valid open states are: signature present and correct, or segment absent.

**2. Demand-fetch from S3**

Verification at demand-fetch time requires only the header and index section — the same bytes retrieved in an index-only fetch (`GET [0, inline_offset)`):

1. Verify `sig` against `BLAKE3(header[0..32] || index_bytes)` using the volume's locally-pinned public key — confirms the index was written by the keyholder
2. On each subsequent byte-range fetch from the body: verify `BLAKE3(fetched_bytes) == entry.hash` against the signed index — confirms the body bytes are what the keyholder wrote

A segment with a missing, malformed, or invalid signature is rejected: not cached, not served.

**Public key trust:** both verification paths use the locally-pinned `volume.pub` from disk. For ancestor volumes (segments written by a parent that this host does not own), the public key is fetched from S3 once and pinned locally before demand-fetch is enabled for that ancestor. Trust-on-first-use from S3 is a known limitation — it is weaker than out-of-band key distribution but sufficient for the misconfiguration-detection use case.
