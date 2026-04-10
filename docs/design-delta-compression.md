# Design: Delta compression via file-path matching

Status: **phase 2b + thin delta in progress** (Phase 1 + 2a landed; see § Scope and sequencing)

Date: 2026-04-09 (updated 2026-04-10)

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

A single file may span non-contiguous physical blocks (multiple ext4 extents). Each such contiguous LBA range is a **fragment** of the file. For each fragment, the import emits one DATA entry covering that LBA range, with the fragment's own hash (BLAKE3 of the fragment's bytes) and the fragment's bytes as the body.

A **contiguous file** (the overwhelming common case for fresh `mkfs.ext4` + extract) produces exactly one DATA entry whose hash equals the whole-file hash and whose block count covers the whole file. A **fragmented file** produces one DATA entry per contiguous LBA range, each with its own fragment-scoped hash.

The segment format is unchanged — these are all normal DATA entries. The knowledge that multiple entries belong to the same file lives only in the filemap (see below); the LBA map and the extent index treat each fragment as an independent extent.

**Delta compression on fragmented files is best-effort.** The filemap records `(path, file_offset, byte_count, hash)` per fragment. At delta-compute time, the coordinator walks the child filemap grouped by path and looks up the same path in the source filemap. If the two fragment layouts match exactly — same set of `(file_offset, byte_count)` tuples — the coordinator pairs fragments by offset and computes per-fragment deltas. If layouts differ, **no delta for this file**; the child fragments are uploaded as full DATA entries with no delta options. Exact dedup still applies opportunistically (any fragment whose hash matches an existing extent in the source pool becomes a DedupRef via the normal Phase 2a path). This is an accepted degradation for an edge case — fragmented layouts are rare for fresh imports, and when they do differ across releases it's usually on large binaries where delta benefit is already marginal.

### Metadata and free-space blocks

File-aware import only covers regular file data. The image also contains:
- ext4 metadata (superblock, group descriptors, inode tables, journal)
- Directory data blocks
- Free space (zeros or garbage)

Metadata and directory blocks should still be imported block-by-block at 4 KiB granularity — they're small, not candidates for dedup or delta, and the read path needs them at their exact LBAs. Free-space blocks (all zeros) are already skipped.

## Snapshot filemap

A new file per snapshot: `snapshots/<ulid>.filemap`

Maps file path → list of fragment records for every regular file at that snapshot point. Written once at import time, immutable thereafter.

**Format (line-oriented text, one entry per fragment):**

```
# elide-filemap v2
<path>\t<file_offset>\t<blake3-hex>\t<byte_count>
<path>\t<file_offset>\t<blake3-hex>\t<byte_count>
...
```

A **contiguous file** (the common case) appears as a single line with `file_offset = 0` and `byte_count` = file size; the hash is the whole-file hash and equals the segment DATA entry's hash. A **fragmented file** appears as multiple lines with the same path and ascending `file_offset` values covering the file end-to-end; each line's hash is the fragment's own hash (BLAKE3 of just that fragment's bytes) and equals the corresponding segment DATA entry's hash. The fragmentation is determined entirely by how ext4 laid the file out; the filemap records it as-is.

Keyed by path because the coordinator's workflow is path-driven: iterate the child filemap by path, look up the same path in the parent filemap, compare fragment tuples. If both sides have the same fragment layout, pair by offset and compute per-fragment deltas where hashes differ.

The `file_offset` column makes fragment identity explicit: two fragments at the same path are "the same fragment" if and only if they share `(file_offset, byte_count)`. This is what allows the coordinator to match child and source fragments deterministically without any cross-referencing of ext4 layout metadata.

Byte count is included so the coordinator can make size-aware decisions (e.g. skip delta for tiny fragments where the overhead isn't worth it).

**Properties:**
- Written once at import time, never modified
- Uploaded to S3 alongside segments for readonly volumes — this ensures the filemap survives local disk loss and is available when a new host imports the same volume
- Only produced for ext4 volumes; non-ext4 volumes simply don't have a filemap (delta compression degrades gracefully to full-extent storage)
- One filemap per snapshot, not per segment

**Why not in the segment format?** File paths are a snapshot-level concept — "what files exist at this point in time." A segment is a bag of extents that may span multiple files and multiple snapshots. Embedding paths in the segment index would couple the storage format to filesystem awareness and add per-entry overhead for data that's only used once (at delta computation time).

## Delta computation at upload time

The production source-selection strategy is **filemap path-matching** across imported snapshots linked via `extent_index` provenance. This is the path that realises the 94% S3 fetch savings from `findings.md` and is load-bearing on Phase 2a's cross-import extent_index lineage.

### Removed: LBA-based delta (previously a PoC at drain time)

**Proposed:** remove the existing LBA-based delta path in `elide-coordinator/src/delta.rs` and its hook in `drain_pending()`. Rationale:

- NBD live writes are block-granular — each pending segment is a bag of fragmented 4 KiB blocks, not file-sized extents. LBA-based matching produces tiny deltas of tiny extents, with rebuild cost per segment greater than the marginal saving.
- The design always called this out as "PoC only; validates machinery; not the intended production integration point" (this doc, §"Why not at drain time?").
- Keeping it alongside filemap-based delta would create two delta paths racing for the same entries, each with different source-selection rules — unnecessary complexity.
- The delta format machinery it validated (segment format, delta options table, zstd dictionary compression, read-path decompression) is unaffected and remains in use.

Snapshot-time coalescing (design doc §"Snapshot (coalescing, future)") is the eventual replacement for LBA-based delta on live-write volumes, but is deferred. Until then, NBD-written volumes get filemaps only via Phase 3; delta at drain time is **gone**, not degraded.

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

**Proposed (Phase B):** delta computation runs only for segments produced by an **import** whose volume provenance lists one or more `extent_index` sources with filemaps available locally. Writable-fork drain segments bypass the delta stage entirely.

1. **Redact** — hole-punch dead DATA body regions in place (existing)
2. **Compute deltas** (Proposed, import-only) — load child + source filemaps, path-match changed files, compress with zstd dictionary using the source extent body, append to the delta section
3. **Upload** — PUT the segment to S3 (existing)
4. **Promote** — IPC to volume: write index/, cache/, delete pending/ (existing)

Delta computation under Phase B is **additive**: the segment's delta section grows, the full body section is untouched. Phase C (thin delta entry kind) then makes the body itself optional when a delta is produced — see below.

### Proposed: thin delta entry kind (Phase C)

Phase B's additive delta stores the full extent body *and* the delta blob, which is wasteful when a delta is produced: cold hosts pay for body bytes they would never fetch, and warm hosts pay for body bytes they never need. This mirrors the problem the thin DedupRef PR (#36) solved for dedup references.

**Proposed:** a new `EntryKind::Delta` that mirrors the format-level shape of `DedupRef`:

- `stored_offset = 0`, `stored_length = 0` — the entry reserves no body space. Its own content is not present in the body section of any segment (local, cache, or S3).
- The entry's content is served by fetching the delta blob from the segment's delta body section (or inline, via `FLAG_DELTA_INLINE`) and decompressing it against the source extent body, which is located via `extent_index.lookup(source_hash)`.
- One or more delta options per entry (`delta_count ≥ 1`) act as **hints**: the reader picks the first `source_hash` it can resolve locally. More than one option is a graceful-degradation mechanism across skipped releases.
- **Delta source must resolve to a DATA entry.** A Delta's `source_hash` never targets another Delta — no delta-of-delta chains. This bounds decompression cost to a single dictionary apply and keeps GC liveness reasoning linear.
- **Producer:** the Phase B upload stage emits `Delta` instead of `Data` when `delta_length < body_length` and the source is confirmed locally available. The body bytes are dropped from the segment entirely; only the delta blob is written.
- **Reader:** the demand-fetch path (`elide-fetch`) already handles delta options on Data entries. A Delta entry uses the same decompression step, with the only difference being that there is no full-body fallback in the same segment — if no option's source resolves, the fetch fails rather than falling back. This is the price of thin delta and is symmetric with thin DedupRef's dependence on `extent_index.lookup()`.

**Invariants** (mirroring the two DedupRef invariants):

1. **Pinning.** Every live Delta's source extent lives in a segment GC cannot rewrite or remove. The snapshot floor + first-snapshot pinning rules already provide this for cross-import delta sources, because `extent_index` lineage sources are always fully-snapshotted ancestors (checked at import time by Phase 2a).
2. **Canonical presence.** For every live Delta with source hash H, `extent_index.lookup(H)` returns a DATA entry. Maintained by GC's liveness rule, extended: a DATA entry is kept alive if any live LBA in the volume references its hash *either directly (Data or DedupRef) or indirectly via a Delta's source_hash*.

**`lba_referenced_hashes` extension.** The LBA map's `lba_referenced_hashes()` set already includes the hash reached by each live LBA. A Delta LBA reaches *two* hashes that must stay alive: the Delta's own content hash (for extent index visibility, though Delta entries are not themselves registered in the extent index) and the source hash (for decompression). The source hash is the load-bearing one — it must be folded into the same `lba_referenced_hashes()` set that PR #36 introduced, so GC's existing "keep DATA alive iff any live LBA references its hash" rule covers delta sources with no special-case code path.

**GC preservation.** GC carries Delta entries through `compact_candidates_inner` unchanged, same pattern as DedupRef: a new match arm that asserts `stored_offset == 0, stored_length == 0` and copies the entry through to the output, skipping `fetch_live_bodies` entirely (it contributes no body bytes). The delta blob itself stays in the source segment's delta section; GC does not move delta blobs between segments.

**Regression tests** (mirroring `gc_ordering_test.rs` from PR #36):

- A segment containing a Delta entry whose source lives in an ancestor segment passes through GC compaction unchanged.
- A segment containing a Delta entry whose source lives in the *same segment* as a Data entry (variant-b-analogue) is compacted correctly: overwriting the Delta's LBA before GC must still keep the source DATA alive if any other LBA references either hash.
- Crash recovery mid-compaction with Delta entries present.
- Rejecting an old format version that encoded delta-of-delta chains (format version bump signals the new invariant).

### Read path: delta decompression and source selection (part of Phase C)

`elide-fetch/src/lib.rs` currently has **no delta decompression path at all**: `fetch_one_extent` ignores `delta_options` entirely and always fetches full body bytes. This is correct for the current state (delta options are produced by the PoC but never consumed), but becomes wrong as soon as Phase B starts writing meaningful deltas, and is a hard blocker for Phase C's thin Delta entries (which have no full body to fall back on).

Phase C wires up delta decompression for the first time. The read path for an entry with one or more delta options:

1. Scan delta options in the order they appear in the entry. For each option:
   a. Look up `source_hash` in the local extent index.
   b. If the source segment's body is **already present in local cache** (`.present` bit set for that entry, or source lives in `pending/`/`segments/`), pick this option and stop scanning.
2. If no option's source was cached, scan again and pick the option whose source segment has the **earliest ULID** — oldest bases are most reusable across future deltas and most likely to be shared with other hosts, giving deterministic cross-host cache-reuse.
3. Fetch the source body (local read or demand-fetch), fetch the delta blob, zstd-dict decompress → materialised extent bytes → write to `.body` and set the `.present` bit.
4. If no option resolves at all: for a Data entry with delta options, fall back to fetching the full body from the segment's body section; for a thin Delta entry, return a fetch error (there is no fallback — this is the price of thin delta, and is symmetric with thin DedupRef's dependence on `extent_index.lookup()`).

Already-cached wins trivially (no fetch cost). Earliest-ULID as the uncached tiebreaker is the host-stable choice that maximises shared-base reuse across the fleet: any two hosts fetching the same child extent will pick the same source, so their caches converge on the same set of source bodies rather than fragmenting across many bases.

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

**Phase 1 (done):** Delta format and machinery. Segment format v3 with delta table in the index, `rewrite_with_deltas()`, `compute_deltas()` with LBA-based source selection, integration into `drain_pending()` for validation. Proof-of-concept only — the real savings come from later phases.

**Phase 2a (done):** Import-time extent-source linkage. The `extent_index` field in `volume.provenance` names one or more snapshots whose extents are merged into the child's hash pool (but not into its LBA map). `elide volume import --extents-from <name>` (repeatable) wires this up; blocks whose hash already exists in any listed source are written as `DedupRef` entries during the import block loop. Delivers cross-import dedup; lays the groundwork for filemap-based delta. All lineage is carried in the signed provenance file — no standalone `volume.extent_index` file exists.

**Phase B1 (Proposed, next):** File-aware import. Rewrite `elide-core/src/import.rs` to parse ext4 metadata and iterate files instead of blocks: regular file data becomes one file-sized DATA extent per file (with DedupRef entries for any discontiguous LBA ranges, per §"Multi-extent files and LBA mapping"); ext4 metadata (superblock, group descriptors, inode tables, directory blocks, journal) stays block-granular. Free space is skipped as today. B1 has no delta code; its value is granularity (dedup becomes file-level) and making filemap hashes match segment entry hashes — a hard prerequisite for every later step.

This is a clean break in the import output format. No compatibility shim for existing imported volumes — they remain valid and readable, but any new import produces the file-aware layout.

**Phase C (Proposed, after B1):** Thin delta entry kind + delta-aware demand-fetch reader. Two pieces that land together:

1. **Format:** new `EntryKind::Delta` with `stored_offset = 0, stored_length = 0`, mirroring the shape of thin DedupRef. GC preservation and `lba_referenced_hashes` folding follow the DedupRef pattern. No producer in the coordinator yet — the format is verified against synthetic hand-crafted fixtures in unit tests.
2. **Reader:** first-time implementation of delta decompression in `elide-fetch`. Scan an entry's delta options, pick the first `source_hash` that resolves locally (already-cached preferred; earliest ULID as tiebreaker among uncached candidates), fetch the source body, fetch the delta blob, zstd-dict decompress.

C lands before B2 so that when B2's producer lands, its output is the final thin format directly, not an intermediate "Data + delta_options" form. The existing `FLAG_HAS_DELTAS` mechanism on Data entries is format-level dead code after C: the delta options table exists (used by Delta entries) but no producer will ever set `FLAG_HAS_DELTAS` on a Data entry.

The earliest-source preference was originally scoped as a standalone Phase A. Its natural home is inside the Phase C reader's source-selection loop — there is no pre-existing delta-decompression path in `elide-fetch` to add a preference to — so it is absorbed into C.

**Phase B2 (Proposed, after C):** Filemap-based delta at coordinator upload. Load child + source volume filemaps via provenance lineage, path-match changed files, compute zstd deltas against locally-available source extent bodies, emit `EntryKind::Delta` entries using the format from C (not additive `Data + delta_options`). Simultaneously **removes** the existing LBA-based PoC delta path in `elide-coordinator/src/delta.rs` and its hook in `drain_pending()`. B2 is the first real producer of Delta entries; end-to-end verification runs through B1's file-aware import + C's reader.

**Phase 3 (deferred):** Snapshot-time coalescing. Parse ext4 metadata at snapshot time to reconstruct file-level extents from fragmented NBD blocks, emit filemaps, and compute deltas against prior snapshots. Extends filemap-based delta to live-write volumes — until this lands, delta is import-only.

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
2. Reads the source's **signed `volume.provenance`**, verifies the signature, and inherits every entry from the source's `extent_index` field
3. Appends the source itself at its latest snapshot
4. Dedupes by source ULID and applies the eviction rule described below

On the import path:
1. `elide-import` receives the resolved entries as repeatable `--extent-source` CLI args from the coordinator
2. `setup_readonly_identity` generates an ephemeral keypair and writes them into the new volume's `volume.provenance` under `extent_index`, all signed together
3. The import then walks its own (just-written) provenance via `walk_extent_ancestors` to rebuild a hash-only `ExtentIndex` over the listed sources, and passes it into `import_image`
4. During the block loop, any block whose hash is already present in the pool is written as a `DedupRef` instead of a fresh `Data` entry (automatic dedup across imports)
5. **Follow-up** (separate change): the coordinator's upload pipeline loads the child's and sources' filemaps, matches paths, and appends zstd delta options to changed extents using a source extent as the dictionary

The `extent_index` lineage is deliberately **not** the same as the `parent` lineage. A fork (`parent`) merges the parent's segments into the child's LBA map so the child can read parent data through CoW fall-through. An extent-index source (`extent_index`) contributes only to the extent index, never to the LBA map: the child's reads only return hashes the child itself wrote, so no source data leaks even though source segments are used as a hash pool. The list is **flat, not hierarchical** — unlike fork ancestry, which chains through `walk_ancestors`, extent-index sources are computed once at import time and written as a pre-expanded union inside the signed provenance, so attach-time resolution is a single read.

Multiple sources are a first-class feature: the operator can union several prior releases (e.g. the last few point releases plus a security-patched build) into the hash pool of a new import. Each source contributes its own pre-flattened list, and the coordinator dedupes across them.

### Eviction rule when the cap is hit

`MAX_EXTENT_INDEX_SOURCES = 32` bounds the list so attach cost stays predictable as operators chain imports over release trains. When the expanded union exceeds the cap:

- **Explicit sources** (the `--extents-from` arguments on this specific import) are sacred and kept in full. If the explicit count alone exceeds the cap, the import is rejected with a clear error — silently dropping operator intent is worse than a clean failure.
- **Inherited entries** (copied from explicit sources' own `extent_index` lists) fill the remaining slots via "oldest + most recent" pruning: the first-added entry is always kept (it's typically the base image, which holds the largest reusable pool), and the rest of the slots are filled with the most recently added inherited entries. Middle entries are dropped with a warning logged to the import job.

The rationale: for a release train like `v1 → v2 → … → v33`, the base `v1` holds the bulk of the unchanged content and must not be evicted, while the most recent releases hold the content most similar to what's being imported. Middle versions matter for delta compression between specific version pairs but contribute little marginal dedup. When smarter heuristics become worthwhile (e.g. filemap path-overlap scoring before import), they can replace this rule without changing the on-disk format.

See [architecture.md](architecture.md) for the provenance file format and invariants.

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

Delta options in S3 segments benefit **warm hosts** — hosts that already have a prior version's extents cached locally. The flow:

1. Host runs version N — demand-fetches extents as needed, caches them in `cache/`
2. Operator publishes version N+1 (imported with `--extents-from <version N>`)
3. Host pulls version N+1 via `volume remote pull` — the signed `volume.provenance` is preserved, so the walkers resolve the fork parent and extent-index sources to local `by_id/<ulid>/` directories
4. Volume open rebuilds the extent index across the signed lineage — version N's cached extents are visible via the `extent_index` entry
5. Demand-fetch for version N+1's changed extents checks delta options: if `source_hash` is in the local extent index (from version N's cache), fetch the delta blob (small) instead of the full body

On a **cold host** with no prior version cached, delta options are useless — no source extent is available locally, so the full body is fetched. This is correct and transparent; the delta path is purely an optimisation, never a requirement.

The `prefetch_indexes` coordinator task downloads `.idx` files for every source in the signed lineage on volume discovery (both `parent` fork chain and `extent_index` flat list), so the LBA map and extent index are always complete. Only body data is demand-fetched.

## Open questions

- **Symlinks and hardlinks:** the filemap maps paths to content hashes. Symlinks are not regular files and are skipped. Hardlinks (multiple paths, same inode) will produce duplicate filemap entries with different paths but the same hash — harmless, but worth deduplicating in the filemap to keep it compact.
- **Multi-block writes:** the write path may coalesce contiguous blocks into a single extent covering multiple LBAs. When looking up the parent LBA map, each LBA within the extent may have a different old hash (or some may be new allocations). The simplest approach: only attempt delta when all LBAs in the extent map to a single contiguous source extent in the parent. If the mapping is fragmented, skip delta for that entry.
