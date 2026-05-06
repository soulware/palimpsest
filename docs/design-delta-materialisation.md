# Design: Local materialisation cache for delta entries (`.dmat`)

Status: **Proposed.**

Date: 2026-05-06

---

## Context

Delta entries (`FLAG_DELTA`, see [formats.md](formats.md) — *FLAG_DELTA entries (thin delta)*) carry no body bytes. Reads currently re-derive the materialised extent on every access:

1. Read the source extent body (full extent, lz4-decompressed if needed).
2. Read the delta blob from `cache/<ULID>.delta` (or the segment's delta body section in `pending/`).
3. zstd-dict-decompress the delta against the source body.
4. Slice out the requested LBA range.

Steps 1 and 2 are I/O; step 3 is the dominant CPU cost (zstd-dict-decompress runs ~500 MB/s on extent payloads, vs lz4's ~4 GB/s for `.body` reads). Every read of the same delta-mapped LBA pays the full cost again — no caching anywhere on the path.

Empirical findings ([findings.md](findings.md)) show delta compression saves ~94% of S3 storage on point-release upgrades. That same ratio implies most of an updated image's *content* is served via delta entries. Repeatedly decompressing them on every read leaves CPU and IOPS on the table.

This document specifies a local-only sidecar — `cache/<ULID>.dmat` — that holds materialised delta-entry bytes after their first read, so subsequent reads serve from a regular file with at most an lz4 decompress.

## Goals

- Materialise each delta entry **at most once** per host between cache evictions.
- Survive crash and restart: a successfully written record stays valid until the segment is evicted.
- Keep the format **inspectable with standard tools** — `ls -l` shows the real on-disk size; an `inspect-dmat` command can enumerate records.
- Authenticate materialised bytes against the segment's signed index — `.dmat` corruption cannot poison a read.
- Local-only: never uploaded to S3, never required for correctness, fully reconstructible from `.delta` plus the source extent.

## Non-goals

- **Eager materialisation at promote.** Entries that are never read should not consume disk. We materialise lazily, on first read.
- **Cross-segment content addressing.** The cache is keyed per-segment by `entry_idx`. The dedup-across-segments win exists but is rare in practice; the simpler scheme covers the dominant case.
- **Replacing `.delta`.** The delta blob is the canonical compressed form and is what S3 holds. `.dmat` is purely a local read-side cache.

## Why not reuse `.body`?

`FLAG_DELTA` entries contribute zero to `body_length` and reserve no range in `.body` (see [formats.md](formats.md) — base entries / Delta). Reusing `.body` for materialised bytes would require either (a) pre-allocating body slots for delta entries at write time, defeating the on-disk savings, or (b) appending past `body_length` with side-state recording entry → offset, which is a sidecar in everything but name. A separate file with its own discipline is simpler and keeps `.body`'s invariants (`set_len(body_length)`, exact size) intact.

## Why not atomic-rename, like `.body`?

`.body` is built once at promote and rename-into-place — the discipline matches its lifecycle (write once, read forever). `.dmat` is the opposite shape: append on first read, grow over time, recover from torn writes. Atomic-rename per materialisation costs O(N²) bytes written for N delta entries (the whole file is rewritten each time). Append-only with tail recovery is the natural fit, and it parallels the WAL.

## File format

```
cache/<ULID>.dmat

Magic header (8 bytes):  "ELIDMAT\x01"

Then a sequence of self-contained records, appended:

Record:
  entry_idx      (u32 le)         index of base entry in the segment's index section
  flags          (u8)             bit 0: FLAG_COMPRESSED (lz4_flex)
  stored_length  (u32 le)         length of `data` field below
  data           (stored_length bytes)
                                   if FLAG_COMPRESSED: lz4-compressed payload
                                                       (uncompressed size = lba_length × 4096)
                                   else:               raw materialised extent bytes
```

`stored_length` is the byte length present in the file — the same convention used by WAL DATA records and segment index entries. The uncompressed size is always derivable from the segment index (`entry.lba_length × 4096`), so it is not stored in the record.

The compression algorithm and entropy gate match `.body`: lz4_flex, skip compression if entropy ≥ 7.0 bits/byte or the ratio is below the minimum threshold. No new compression decision.

## Lifecycle

### Materialise (first read of a delta entry)

1. Decompress the delta as today (see [formats.md](formats.md) — Reads of a Delta-mapped LBA, steps 1–5).
2. Apply the standard entropy gate to the uncompressed bytes.
3. Build a record: `entry_idx | flags | stored_length | data`.
4. Append it to `cache/<ULID>.dmat`. Create the file with the magic header if absent.
5. `sync_data()` on the file.
6. Insert `(entry_idx → file_offset)` into the in-memory map for this segment.

The materialised bytes are returned to the caller from the in-memory result — no extra read of the just-written file.

### Read (subsequent reads)

1. In-memory map lookup: `entry_idx → file_offset`.
   - Miss → fall through to the existing delta path (and re-materialise on this read).
   - Hit → continue.
2. Read `stored_length` bytes from the file at `file_offset + record_header_size`.
3. lz4-decompress if `FLAG_COMPRESSED`.
4. Slice out the requested LBA range from the materialised bytes.

No per-read hash verification: the file was authenticated end-to-end on open.

### Open / recovery (parallel to WAL `scan()`)

1. If the file does not exist or is shorter than the magic header, treat as empty. The map starts empty.
2. Verify the magic. On mismatch, log and remove the file (it was written by a different format version — abandon the cache for this segment).
3. Walk records sequentially from byte 8:
   - If the remaining bytes are too few for a record header, stop here (partial tail).
   - Read `entry_idx`, `flags`, `stored_length`. If `stored_length` would extend past EOF, stop here (partial body).
   - Look up `segment.entry[entry_idx]`. If `entry_idx` is out of range or not a Delta entry, stop here (poisoned record).
   - Read `data`, lz4-decompress if needed. Verify `decompressed.len() == lba_length × 4096`. Verify `BLAKE3(decompressed) == entry.hash`. On any failure, stop here.
   - Insert `(entry_idx → record_start_offset)` into the in-memory map.
4. If we stopped before EOF, truncate the file to the last validated record.

The hash check is the authentication boundary: it anchors the record to the (signed) segment index. Any tamper with `.dmat` — bit flip, partial write, malicious overwrite — fails verification and is truncated. Re-materialisation from `.delta` then refills on demand.

### Eviction

`.dmat` is removed alongside `.body` and `.delta` whenever a segment's body cache is evicted. It contains no state that is unique or unrecoverable — re-materialisation produces identical bytes from `.delta` plus the source extent. The set of files cleared on segment eviction becomes:

```
cache/<ULID>.body
cache/<ULID>.present
cache/<ULID>.delta
cache/<ULID>.dmat        ← new
```

`.dmat` is a strictly local artefact; manifests, snapshots, and the `index/<ULID>.idx` retention rule are unaffected. It is never uploaded to S3, never enumerated in any signed structure, and never referenced by any cross-host protocol.

## Properties

| Property | Mechanism |
|---|---|
| At-most-once materialisation per entry per host (between evictions) | First read writes the record; subsequent reads hit the in-memory map. |
| Crash-safe | Append-only with tail truncation on open. No torn-record window — partial records fail length or hash verification and are truncated. |
| Authenticated | Each record's bytes are BLAKE3-verified against the segment's signed `entry.hash` at open time. |
| Bounded write amplification | One `sync_data()` per materialisation. No file rewrites, no rename, no O(N²) growth. |
| Inspectable | `ls -l` shows the actual file size. An `inspect-dmat` (or `inspect-segment` extension) can enumerate `(entry_idx, stored_length, FLAG_COMPRESSED)` per record. |
| Reconstructible | Deleting `.dmat` reverts to the existing on-the-fly delta path. No persistent state lives only in `.dmat`. |
| Local-only | Not uploaded, not in manifests, not in liveness predicates. |

## Read-path integration

In `try_read_delta_extent` (elide-core/src/volume/read.rs):

1. **Before** picking a delta option and reading the source body: check the per-segment in-memory `.dmat` map for `entry_idx`. On hit, read and decompress from `.dmat`, slice, return.
2. **After** successful zstd-dict-decompress (current code path): write the materialised bytes to `.dmat` (with the entropy gate), update the in-memory map, then slice and return.

Both paths share the post-decompression slice logic; only the bytes' provenance differs.

## Instrumentation

To validate that the cache is doing useful work, the following counters are added (per-volume, exposed via the existing telemetry surface):

| Counter | Increment when |
|---|---|
| `delta_materialise_lookup_total` | Any read enters the delta path. |
| `delta_materialise_hit_total` | The `.dmat` in-memory map resolves the read. |
| `delta_materialise_miss_total` | The `.dmat` map does not have the entry; we fall through to zstd-dict-decompress. |
| `delta_materialise_write_total` | A record is appended to `.dmat`. |
| `delta_materialise_write_bytes_total` | Bytes written to `.dmat` (post-compression, per record). |
| `delta_materialise_open_truncate_total` | Open-time scan truncated a partial or invalid tail. |
| `delta_materialise_open_invalid_total` | Open-time scan rejected a record by hash mismatch (corruption signal). |

Hit / miss together quantify how often a delta entry is read more than once between cache evictions — this is the only metric that says whether materialisation pays for itself. If hit-rate is low across realistic workloads, the strategy may want to be revisited (e.g. skip `.dmat` for one-shot reads). The compression-byte counter quantifies the on-disk cost of the decision.

## Open questions

1. **In-memory map size on hosts with very large segments.** A segment with 50 000 delta entries needs a 50k-entry `HashMap<u32, u64>` (~600 KB) per loaded segment. Per-host scale: load is bounded by the number of currently-mapped segments, not the cumulative segment count. Likely fine, but worth measuring on a large fork.

2. **Boot-time scan cost.** Open-time hash verification reads and decompresses every record. On a host with hundreds of partially-materialised segments, the aggregate scan could add measurable open-time latency. A future optimisation could write a footer at clean shutdown that vouches for the file's integrity, allowing the scan to skip per-record verification when the footer is present and matches. Not in v1.

3. **Eager pre-materialisation as a future option.** If telemetry shows that almost every delta entry in a segment is read shortly after promote, an eager mode (materialise all delta entries at promote time) becomes attractive. The format above accommodates it without change — the only difference is *when* records are appended. This is a tuning knob, not a format question.
