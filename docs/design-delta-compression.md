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

## NBD fragmentation and the inline threshold

NBD is a block protocol. Every write through ext4 → NBD arrives as one or more 4 KiB blocks, regardless of the logical file size. A 200 KiB file written by the kernel becomes ~50 individual 4 KiB block writes in the WAL. This fragmentation has two consequences:

1. **Every extent is exactly one block.** There are no file-sized extents from NBD writes — only 4 KiB chunks (or small coalesced runs of contiguous blocks).

2. **Compressed block size determines inlining.** A 4 KiB block containing "hello" + zeros compresses to ~20 bytes via lz4. The inline threshold determines whether this goes in the `.idx` (fetched by every host) or the body section (demand-fetched when needed).

The inline threshold must be low (256 bytes) to avoid bloating the `.idx` with compressed block data. At the previous threshold (4096), virtually every compressed 4 KiB block was inlined — the `.idx` contained all the data, defeating demand-fetch and delta compression entirely.

At 256 bytes, only genuinely tiny data inlines (mostly-zero blocks, sparse metadata). Real data blocks go to the body section, where they benefit from demand-fetch and are eligible for delta compression. For file-aware import (phase 2), the stored size naturally reflects the real file size — a 5-byte config file stores as ~5 bytes (inline), a 200 KiB library stores as ~150 KiB compressed (body).

## Delta integration points

Delta compression fires at two points — both explicit user actions on a consistent filesystem state. The regular `drain_pending()` upload path ships raw segments as-is with no delta attempt.

### Import (file-aware, phase 2)

File-aware import reads the ext4 image directly (not via NBD), producing file-sized extents. The filemap enables path-matching against a parent import. This is where the 94% S3 fetch savings from findings.md are realised. See "Phase 2: import-time parent association" below.

### Snapshot (coalescing, future)

NBD writes fragment files into 4 KiB blocks. At snapshot time, the filesystem is in a consistent state — Elide can parse the ext4 metadata from its own block data and reconstruct file-level extents:

1. Parse the ext4 superblock, inode tables, and extent trees from the volume's current LBA state
2. Identify which 4 KiB blocks belong to the same file
3. Coalesce them into a single file-sized extent with one content hash
4. Emit a filemap for the snapshot (`path → hash`)
5. Compute deltas against the prior snapshot's filemap

This is "retroactive file-aware import" applied to live-write data. The ext4 parsing infrastructure already exists in `src/extents.rs`. The snapshot is the natural integration point because it represents a user-declared consistent state — unlike GC, which may see in-flight writes with uncommitted journal state.

This also means NBD-written volumes get filemaps at snapshot time, enabling filemap-based delta for subsequent snapshots — not just for imported volumes.

### Why not at drain time?

The current implementation includes an LBA-based delta step in `drain_pending()` as a proof-of-concept. In practice this has very limited value for NBD writes:

- Every pending segment contains fragmented 4 KiB blocks, not file-sized extents
- LBA-based source selection works but produces small deltas of small extents — marginal savings
- Rebuilding the parent LBA map and extent index per segment is expensive relative to the benefit

The drain-time delta path validates the end-to-end machinery (computation, segment format, serialization) but is not the intended production integration point.

## Scope and sequencing

**Phase 1 (done):** Delta format and machinery. Segment format v3 with delta table in the index, `rewrite_with_deltas()`, `compute_deltas()` with LBA-based source selection, integration into `drain_pending()` for validation. Proof-of-concept only — the real savings come from phases 2 and 3.

**Phase 2a (this change):** Import-time parent linkage. New file `volume.extent_index` names a parent snapshot whose extent index is merged into the child's hash pool (but not into its LBA map). `elide volume import --parent` wires this up; blocks whose hash already exists in the parent pool are written as `DedupRef` entries during the import block loop. Delivers cross-import dedup; lays the groundwork for filemap-based delta.

**Phase 2b:** File-aware import with filemap-based delta on top of the linkage from 2a. The coordinator loads both filemaps, matches paths, and computes zstd deltas against parent extent data for changed files. See "Phase 2: import-time parent association" below.

**Phase 3:** Snapshot-time coalescing. Parse ext4 metadata at snapshot time to reconstruct file-level extents from fragmented NBD blocks, emit filemaps, and compute deltas against prior snapshots. This extends filemap-based delta to live-write volumes.

**Phase 4 (deferred):** Content-similarity-based source selection for filesystem-agnostic delta. Marginal benefit over path matching for the primary workload, significantly more complex.

## Non-ext4 volumes

For phase 2 (filemap-based delta at import) and phase 3 (snapshot coalescing), volumes without an ext4 filesystem simply don't get a filemap. The import path falls back to the existing block-by-block import. Delta compression is not available — segments are stored with full extents only. This is not a degraded mode that needs handling; it's the absence of an optimisation. The read path, segment format, and GC all work identically regardless of whether deltas exist.

## Phase 2: import-time parent association

OCI images are opaque blobs — Ubuntu 22.04.1 and 22.04.2 are published as independent images with no parent/child relationship at the image level. The association between two versions of the same system is **operational knowledge**, not image metadata.

Elide makes this explicit: the operator declares the relationship at import time.

```
elide volume import ubuntu-22.04.1 ubuntu-22.04.1.img
elide volume import --extents-from ubuntu-22.04.1 ubuntu-22.04.2 ubuntu-22.04.2.img

# Union multiple hash sources:
elide volume import --extents-from ubuntu-22.04.2 --extents-from debian-12 \
    hybrid-release hybrid.img
```

The `--extents-from` flag (repeatable) contributes a volume's extent index to the new volume's hash pool. For each source the coordinator:
1. Resolves the source volume and its latest snapshot
2. Reads the source's own `volume.extent_index` (if present) and copies every entry into the new volume's file
3. Appends the source itself at its latest snapshot
4. Dedupes by ULID and truncates to `MAX_EXTENT_INDEX_SOURCES` entries (oldest-added dropped, with a warning)

On the import path:
1. `elide-import` reads `volume.extent_index` (already flat — no traversal), rebuilds a hash-only `ExtentIndex` over the listed sources, and passes it into `import_image`
2. During the block loop, any block whose hash is already present in the pool is written as a `DedupRef` instead of a fresh `Data` entry (automatic dedup across imports)
3. **Follow-up** (separate change): the coordinator's upload pipeline loads the child's and sources' filemaps, matches paths, and appends zstd delta options to changed extents using a source extent as the dictionary

`volume.extent_index` is deliberately **not** the same file as `volume.parent`. A fork (`volume.parent`) merges the parent's segments into the child's LBA map so the child can read parent data through CoW fall-through. An extent-index source (`volume.extent_index`) contributes only to the extent index, never to the LBA map: the child's reads only return hashes the child itself wrote, so no source data leaks even though source segments are used as a hash pool. The file is **flat, not hierarchical** — unlike fork ancestry, which chains through `walk_ancestors`, extent-index sources are computed once at import time and written as a pre-expanded union, so attach-time resolution is a single file read.

Multiple sources are a first-class feature: the operator can union several prior releases (e.g. the last few point releases plus a security-patched build) into the hash pool of a new import. Each source contributes its own pre-flattened list, and the coordinator dedupes across them. The `MAX_EXTENT_INDEX_SOURCES` cap bounds the list at 32 entries so attach cost stays predictable as operators chain imports over release trains; once the cap is hit, the coordinator logs a warning and drops the oldest-added entries.

See [architecture.md](architecture.md) for the layout and invariants around both files.

### Relationship to OCI layers

OCI images are built from stacked layers — each layer is a tar of filesystem diffs from the Dockerfile step that produced it. Elide imports the **flattened** ext4 root partition (all layers applied), not the individual layers. The OCI layer structure is discarded at import time.

This is deliberate. OCI layers answer "what common build steps do these images share?" — a build-time concern. Elide's ancestor chain answers "what has changed between version N and version N+1 of this system?" — a deployment-time concern. These are orthogonal:

- Two point releases (22.04.1 → 22.04.2) may share **zero** OCI layers if the base layer was rebuilt, yet 95% of their files are byte-identical. OCI layer sharing sees no overlap; Elide's filemap path-matching sees nearly everything.
- Two unrelated images may share every OCI layer except one, but if the differing layer touches a widely-referenced library, OCI layer sharing captures most of the overlap while the actual block-level delta between the two flattened images is small. Elide handles this naturally through dedup and delta.

The two approaches compose: OCI layer sharing optimises image **distribution** (pull only new layers to the import host). Elide's ancestor chain optimises block-level **serving** (fetch only delta bytes when a host already has the prior version cached). Neither needs awareness of the other.

### Why LBA-based delta doesn't help for imports

NBD live writes are block-granular (4 KiB per write). Each block becomes one segment entry. Delta works when the same LBA is overwritten with similar content.

Import, by contrast, reads the ext4 image and writes file-level extents — one hash for an entire file's content. Two imports of different image versions produce entries at potentially different LBAs (ext4 may lay out files differently between builds). LBA matching finds nothing; file-path matching finds everything.

This is why phase 2 requires filemaps and path-matching rather than LBA-based delta.

## Read-path delta: warm hosts, not cold starts

Delta options in S3 segments benefit **warm hosts** — hosts that already have the parent version's extents cached locally. The flow:

1. Host runs version N — demand-fetches extents as needed, caches them in `cache/`
2. Operator publishes version N+1 (imported with `--parent` pointing at version N)
3. Host pulls version N+1 via `volume remote pull` — the `volume.parent` pointer is preserved in the manifest, so the ancestor chain resolves to the local version N directory
4. Volume open rebuilds the extent index across the ancestor chain — version N's cached extents are visible
5. Demand-fetch for version N+1's changed extents checks delta options: if `source_hash` is in the local extent index (from version N's cache), fetch the delta blob (small) instead of the full body

On a **cold host** with no prior version cached, delta options are useless — no source extent is available locally, so the full body is fetched. This is correct and transparent; the delta path is purely an optimisation, never a requirement.

The `prefetch_indexes` coordinator task downloads `.idx` files for all ancestors on volume discovery, so the LBA map and extent index are always complete across the ancestry chain. Only body data is demand-fetched.

## Open questions

- **Symlinks and hardlinks:** the filemap maps paths to content hashes. Symlinks are not regular files and are skipped. Hardlinks (multiple paths, same inode) will produce duplicate filemap entries with different paths but the same hash — harmless, but worth deduplicating in the filemap to keep it compact.
- **Multi-block writes:** the write path may coalesce contiguous blocks into a single extent covering multiple LBAs. When looking up the parent LBA map, each LBA within the extent may have a different old hash (or some may be new allocations). The simplest approach: only attempt delta when all LBAs in the extent map to a single contiguous source extent in the parent. If the mapping is fragmented, skip delta for that entry.
