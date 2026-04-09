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

There are two source-selection strategies, used in different scenarios. Both produce the same output: delta options appended to the segment's delta section with `source_hash` references in the index entries.

### LBA-based delta (live writes against a parent snapshot)

When a writable fork produces segments (WAL → pending), each DATA entry records the LBA it was written to. The parent snapshot's LBA map tells the coordinator what hash previously occupied that LBA. If the hash differs, the old extent is a natural delta source — the user modified data in-place.

The coordinator already builds an LBA map during GC via `lbamap::rebuild_segments()` (see `gc.rs`). The same rebuild is used here, scoped to the parent snapshot's segments (everything up to the branch-point ULID).

**Steps (per pending segment from a writable fork):**

1. Rebuild the parent snapshot's LBA map from ancestor `index/*.idx` files (same `rebuild_segments` call GC uses, with the ancestor chain truncated at the branch-point ULID)
2. Rebuild the extent index for the same ancestor chain (needed to locate source extent bodies)
3. For each DATA entry in the pending segment:
   a. Look up `entry.start_lba` in the parent LBA map → `old_hash`
   b. If no previous hash (new allocation): no delta candidate
   c. If `old_hash == entry.hash`: dedup already handled this, no delta needed
   d. If `old_hash != entry.hash` (data changed): look up `old_hash` in the extent index → read the old extent body → use it as zstd dictionary to compress the new extent body → delta blob
4. For each delta blob produced, append it to the segment's delta body section and record `source_hash = old_hash` in the index entry's delta options

The segment on S3 remains self-contained: the full extent body stays in the body section. The delta section is additive — an additional fetch option for readers that already have the source extent cached locally.

### Filemap-based delta (imported snapshot vs imported snapshot)

When the coordinator uploads segments from an imported snapshot that has a parent snapshot (future: sequential imports into one volume), it uses filemap path-matching instead of LBA matching. This handles file relocation across ext4 updates — the same file path in both snapshots is the natural delta source even if ext4 moved it to different LBAs.

1. Load the child's filemap (`path→hash`) and the parent's filemap (`path→hash`)
2. Iterate the child filemap by path:
   a. Look up the same path in the parent filemap
   b. If path absent in parent: new file, no delta candidate
   c. If parent hash == child hash: exact match, dedup handles it, no delta needed
   d. If parent hash != child hash (file changed) and parent extent data is available locally: compute `zstd::compress_with_dictionary(child_data, parent_data)` → delta blob
3. For each delta blob produced, append it to the segment's delta body section and record `source_hash` (the parent's extent hash) in the index entry's delta options

After this step, the filemap's job is done. The segment on S3 contains delta entries with `source_hash` references — the file path is not stored in the segment and is never needed again by the read path.

### Source data availability

The coordinator needs the actual byte content of both the new extent (which it has — it's in the segment being uploaded) and the source extent (which it needs to read from a local segment or cache file). This is the same data path used for GC compaction and materialisation — read extent data by hash from the extent index.

### Skipping delta for poor candidates

Not every changed extent benefits from delta compression. The coordinator should skip delta when:
- The extent is small (< some threshold, e.g. 4 KiB — delta overhead exceeds savings)
- The resulting delta is larger than the raw extent (delta made it worse — e.g. high-entropy data replacing low-entropy data)
- The source extent is not locally available (don't fetch from S3 just to compute a delta)

When delta is skipped, the extent is stored as a normal full-body entry. The read path handles this transparently.

### Where in the upload pipeline

Delta computation slots in after materialisation in `drain_pending()`:

1. **Materialise** — fill DedupRef body holes with canonical extent data (existing)
2. **Compute deltas** — for each DATA entry with an LBA-based (or filemap-based) source, compress with zstd dictionary and append to the delta section (new)
3. **Upload** — PUT the segment to S3 (existing)
4. **Promote** — IPC to volume: write index/, cache/, delete pending/ (existing)

Both materialisation and delta computation are additive operations: the segment gets larger, not smaller. Materialisation adds body bytes for DedupRef entries; delta computation adds an additional delta section. The savings come at read time when demand-fetch can pull the small delta instead of the full body.

## Read path: unchanged

The demand-fetch path already handles the delta format as designed in [formats.md](formats.md):

1. Fetch segment header + index
2. For each needed extent, check if a delta option's `source_hash` is in the local extent index
3. If yes: fetch delta bytes, decompress with source as dictionary, write materialised result to cache
4. If no: fetch full extent bytes from body section

No file paths, no filemaps, no filesystem awareness. The read path is purely content-addressed.

## Scope and sequencing

**Phase 1 (this design):** LBA-based delta for live writes. Import an image, fork it, make changes via NBD, and the coordinator delta-compresses changed blocks at upload time using the parent snapshot's LBA state as dictionary source. This exercises the full delta pipeline (computation, segment format, read path) as a proof-of-concept. The benefit is modest for small manual changes (most unchanged blocks are already deduped), but it validates the end-to-end machinery.

**Phase 2:** filemap-based delta for imported snapshots. Sequential import of point releases into one volume, with filemap path-matching for source selection. This is where the 94% S3 fetch savings from findings.md are realised.

**Phase 3 (deferred):** content-similarity-based source selection for filesystem-agnostic delta. Marginal benefit over path matching for the primary workload, significantly more complex.

## Non-ext4 volumes

For phase 1 (LBA-based delta), filesystem type doesn't matter — the delta source is selected purely by LBA identity, so it works for any volume with a parent snapshot.

For phase 2 (filemap-based delta), volumes without an ext4 filesystem simply don't get a filemap. The import path falls back to the existing block-by-block import. Delta compression is not available — segments are stored with full extents only. This is not a degraded mode that needs handling; it's the absence of an optimisation. The read path, segment format, and GC all work identically regardless of whether deltas exist.

## Open questions

- **Symlinks and hardlinks:** the filemap maps paths to content hashes. Symlinks are not regular files and are skipped. Hardlinks (multiple paths, same inode) will produce duplicate filemap entries with different paths but the same hash — harmless, but worth deduplicating in the filemap to keep it compact.
- **Multi-block writes:** the write path may coalesce contiguous blocks into a single extent covering multiple LBAs. When looking up the parent LBA map, each LBA within the extent may have a different old hash (or some may be new allocations). The simplest approach: only attempt delta when all LBAs in the extent map to a single contiguous source extent in the parent. If the mapping is fragmented, skip delta for that entry.
