# Design: Delta compression via file-path matching

Status: **proposed**

Date: 2026-04-09

---

## Context

Empirical measurements (see [findings.md](findings.md)) show that delta compression using zstd with the prior file version as a dictionary achieves 94% marginal S3 fetch savings between Ubuntu 22.04 point releases. The segment format already supports delta entries (`source_hash`, delta body section, multiple delta options per extent — see [formats.md](formats.md)). What's missing is the pipeline that produces them.

The core problem is **source selection**: given a changed extent in a new snapshot, which extent from the prior snapshot should be used as the zstd dictionary? LBA matching doesn't work — ext4 can relocate file data across updates, so the same LBA may hold a completely different file.

## Approach: file-path matching across snapshots

Two snapshots of the same volume (or parent → child in the snapshot tree) represent two states of the same ext4 filesystem. The same file path in both snapshots is overwhelmingly likely to be the same logical file — the natural delta source.

This requires:
1. Knowing which file paths exist at each snapshot and which extents they map to
2. File-aligned extents (one extent ≈ one file) so the dictionary is the whole prior file

Both can be achieved at import time by parsing the ext4 image before writing segments.

## File-aware import

Today's import path (`elide-core/src/import.rs`) reads the ext4 image block-by-block at 4 KiB LBA granularity. Each block becomes one `SegmentEntry`. This is filesystem-unaware — extents don't correspond to files.

**Proposed: parse ext4 metadata first, then iterate files instead of blocks.**

The ext4 parsing infrastructure already exists in `src/extents.rs`:
- Superblock, block group descriptor, inode table parsing
- Extent tree traversal (multi-level index nodes)
- `enumerate_file_hashes()` — walks the directory tree, returns `path → (hash, byte_count)`
- `scan_file_extents_with_full_hash()` — reads file data in logical block order, computes per-extent and whole-file hashes

The file-aware import path:

1. Parse the ext4 superblock and inode tables to enumerate all regular files
2. For each file: collect its ext4 extent tree, read physical blocks in logical order, hash the concatenated file data as a single extent
3. Map the file's physical block ranges back to LBA ranges — the segment entry still needs `(lba, block_count)` so the read path can serve block I/O
4. Write a **filemap** alongside the snapshot marker (see below)

### Multi-extent files and LBA mapping

A single file may span non-contiguous physical blocks (multiple ext4 extents). The Elide extent — one hash, one contiguous data blob — represents the entire file's content. But the LBA map needs to know which LBAs this extent covers.

Each file produces one logical extent (one hash of the full file data) that may map to multiple discontiguous LBA ranges. The import emits one DATA entry for the first contiguous LBA range (carrying the actual file data), and DedupRef entries for the remaining ranges (pointing back to the same content hash at the appropriate byte offset within the extent). This reuses the existing segment format with no changes — DedupRef already exists for exactly this purpose.

### Metadata and free-space blocks

File-aware import only covers regular file data. The image also contains:
- ext4 metadata (superblock, group descriptors, inode tables, journal)
- Directory data blocks
- Free space (zeros or garbage)

Metadata and directory blocks should still be imported block-by-block at 4 KiB granularity — they're small, not candidates for dedup or delta, and the read path needs them at their exact LBAs. Free-space blocks (all zeros) are already skipped.

## Snapshot filemap

A new file per snapshot: `snapshots/<ulid>.filemap`

Maps file path → content hash for every regular file at that snapshot point. Written once at import time, immutable thereafter.

**Format (line-oriented text, one entry per line):**

```
# elide-filemap v1
<path>\t<blake3-hex>\t<byte_count>
<path>\t<blake3-hex>\t<byte_count>
...
```

Keyed by path because the coordinator's workflow is path-driven: iterate the child filemap by path, look up the same path in the parent filemap, compare hashes. If different, both hashes are known and the coordinator can fetch both extents to compute the delta.

Byte count is included so the coordinator can make size-aware decisions (e.g. skip delta for tiny files where the overhead isn't worth it).

**Properties:**
- Written once at import time, never modified
- Uploaded to S3 alongside segments for readonly volumes — this ensures the filemap survives local disk loss and is available when a new host imports the same volume
- Only produced for ext4 volumes; non-ext4 volumes simply don't have a filemap (delta compression degrades gracefully to full-extent storage)
- One filemap per snapshot, not per segment

**Why not in the segment format?** File paths are a snapshot-level concept — "what files exist at this point in time." A segment is a bag of extents that may span multiple files and multiple snapshots. Embedding paths in the segment index would couple the storage format to filesystem awareness and add per-entry overhead for data that's only used once (at delta computation time).

## Delta computation at upload time

When the coordinator uploads a segment from a child snapshot, it:

1. Loads the child's filemap (`path→hash`) and the parent's filemap (`path→hash`)
2. Iterates the child filemap by path:
   a. Look up the same path in the parent filemap
   b. If path absent in parent: new file, no delta candidate
   c. If parent hash == child hash: exact match, dedup handles it, no delta needed
   d. If parent hash != child hash (file changed) and parent extent data is available locally: compute `zstd::compress_with_dictionary(child_data, parent_data)` → delta blob
3. For each delta blob produced, append it to the segment's delta body section and record `source_hash` (the parent's extent hash) in the index entry's delta options

After this step, the filemap's job is done. The segment on S3 contains delta entries with `source_hash` references — the file path is not stored in the segment and is never needed again by the read path.

### Source data availability

The coordinator needs the actual byte content of both the child extent (which it has — it's in the segment being uploaded) and the parent extent (which it needs to fetch from a local segment or cache file). This is the same data path used for GC compaction — read extent data by hash from the extent index.

### Skipping delta for poor candidates

Not every changed file benefits from delta compression. The coordinator should skip delta when:
- The file is small (< some threshold, e.g. 4 KiB — delta overhead exceeds savings)
- The resulting delta is larger than the standalone zstd-compressed extent (delta made it worse)
- The source extent is not locally available (don't fetch from S3 just to compute a delta)

When delta is skipped, the extent is stored as a normal full-body entry. The read path handles this transparently.

## Read path: unchanged

The demand-fetch path already handles the delta format as designed in [formats.md](formats.md):

1. Fetch segment header + index
2. For each needed extent, check if a delta option's `source_hash` is in the local extent index
3. If yes: fetch delta bytes, decompress with source as dictionary, write materialised result to cache
4. If no: fetch full extent bytes from body section

No file paths, no filemaps, no filesystem awareness. The read path is purely content-addressed.

## Scope and sequencing

**Phase 1 (this design):** file-aware import + filemap + delta at upload for readonly imported volumes. This covers the primary use case: importing two point releases of the same Ubuntu image and achieving the 94% S3 savings measured in findings.md.

**Phase 2 (deferred):** snapshot-time filemap generation for live write paths. Deferred — the primary use case (imported readonly images) doesn't need it.

**Phase 3 (deferred):** content-similarity-based source selection for filesystem-agnostic delta. Deferred — marginal benefit over path matching for the primary workload, significantly more complex.

## Non-ext4 volumes

Volumes without an ext4 filesystem (data volumes, XFS, Windows/NTFS, raw block usage) simply don't get a filemap. The import path falls back to the existing block-by-block import. Delta compression is not available — segments are stored with full extents only. This is not a degraded mode that needs handling; it's the absence of an optimisation. The read path, segment format, and GC all work identically regardless of whether deltas exist.

## Open questions

- **Symlinks and hardlinks:** the filemap maps paths to content hashes. Symlinks are not regular files and are skipped. Hardlinks (multiple paths, same inode) will produce duplicate filemap entries with different paths but the same hash — harmless, but worth deduplicating in the filemap to keep it compact.
