# Design: Delta compression via file-path matching

Status: **phase 2b + thin delta in progress** (Phase 1 + 2a landed; see § Scope and sequencing)

Date: 2026-04-09 (updated 2026-04-12)

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

### Snapshot (filemap generation, phase 3)

A coordinator-side background task parses ext4 metadata from the frozen snapshot's segments and emits `snapshots/<ulid>.filemap` asynchronously after the synchronous snapshot operation returns. Scope is filemap-only: no body rehashing, no new segment writes — the filemap records paths and fragment layouts exactly as the existing DATA entries describe them.

This extends filemap coverage to NBD-written volumes. Phase 3 does not itself perform delta compression for writable volumes — that's Phase 4's job — but the filemap it produces is the structural input Phase 4 needs to path-match LBAs after the fact.

See §"Phase 3: Snapshot-time filemap generation" for the full design.

### GC repack delta (phase 4)

Phase 4 extends the existing GC repack pass with a delta step, operating on the segments GC already owns — **post-snapshot-floor segments, above the most recent sealed snapshot**. For each LBA in a post-floor segment, the prior snapshot's filemap (produced by Phase 3) is consulted as a **heuristic hint**: if the LBA used to belong to some file, its prior extent hash is tried as a zstd dictionary against the post-floor 4 KiB target. The hint is right for in-place file modification (the dominant case for package upgrades and config edits) and silently wrong for delete-then-reallocate, metadata blocks, and fresh allocations — the existing size check catches every miss by emitting a raw DATA entry when `delta_length >= body_length`. zstd dictionary compression is always correct for any target against any dictionary, so the heuristic only affects compression ratio, never reconstructed bytes. Repacked segments supersede the originals through the existing GC replacement path; the sealed snapshot below the floor is never touched.

See §"Phase 4: GC repack with delta" for the full design (future).

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

**Phase 3 (Proposed):** Snapshot-time filemap generation. A coordinator-side background task parses ext4 metadata from the frozen snapshot's segment data and emits `snapshots/<ulid>.filemap` for writable-volume snapshots. Metadata-only (no body reads, no rehashing); best-effort (non-ext4 volumes and parse failures skip cleanly); crash-safe via tmp+fsync+upload+rename with a filesystem-as-queue restart scan. Extends filemap coverage to NBD-written volumes, serves as the structural input Phase 4 needs, and unlocks the "unfragmented entries as delta sources" exception as a side-effect for file-aware imports. See §"Phase 3: Snapshot-time filemap generation" for the full design.

**Phase 4 (deferred):** GC repack delta. Extend the existing GC repack/sweep pass with a delta step that operates on post-snapshot-floor segments (segments drained since the most recent sealed snapshot). For each LBA in a post-floor segment, consult the prior snapshot's filemap as a **heuristic hint** for which file used to live at that LBA, and try the prior file's extent hash as a zstd dictionary against the 4 KiB target fragment. The heuristic is right for in-place file modification (the dominant case) and silently wrong for delete-then-reallocate and metadata blocks — the existing `delta_length >= body_length` size check catches every miss by emitting a raw DATA entry. zstd is correct regardless of dictionary, so the heuristic only affects compression ratio, never reconstructed bytes. No new lifecycle machinery: post-floor segments are already GC-rewriteable, repacked outputs supersede the originals through GC's existing cleanup path, and the sealed snapshot providing the source filemap is read-only — published snapshots remain immutable. This is the step that delivers filemap-based delta for writable volumes and closes the drain-time LBA→path knowledge gap.

**Phase 5 (deferred):** Content-similarity-based source selection for filesystem-agnostic delta. Marginal benefit over path matching for the primary workload, significantly more complex.

## Pull-host demand-fetch of the delta body section

A **pull host** (a host that never ran the import, and obtains the segment by pulling from S3) reads Delta LBAs by demand-fetching the segment's delta body section on first access. The delta body lives in its own local file, `cache/<id>.delta`, parallel to `cache/<id>.body` and `cache/<id>.present`:

```
cache/<id>.idx     — header + index + inline (coordinator-written, permanent)
cache/<id>.body    — sparse body section (size = body_length); Data entries at their
                     stored_offset, holes elsewhere
cache/<id>.present — per-entry bitset for .body
cache/<id>.delta   — exactly the delta body region (size = delta_length), starting at
                     delta-region byte 0; absent iff the segment has no deltas or the
                     host hasn't fetched them yet
```

Keeping the delta region out of `.body` means `.body` has a single unambiguous shape and the "do I have the delta body locally?" question is answered by `.delta`'s existence, not by comparing file sizes against header fields.

### Rebuild semantics

`extent_index::rebuild` registers every Delta entry it encounters in the `.idx`, unconditionally, with `DeltaBodySource::Cached`. Rebuild does **not** stat `.delta`. The reader is responsible for handling the missing-file case.

For segments still in `pending/` (not yet promoted), Delta entries register with `DeltaBodySource::Full { body_section_start, body_length }` because the delta body lives inline at the end of the pending file. These are mutually exclusive — a segment is either still in `pending/` or has been promoted to the cache shape; rebuild sees exactly one of the two at any point in time.

### Read path

`try_read_delta_extent` dispatches on `DeltaBodySource`:

- `Full`: open the segment file via `find_segment` (unchanged path), seek to `body_section_start + body_length + opt.delta_offset`, read `opt.delta_length` bytes.
- `Cached`: call `open_delta_body_in_dirs` → open `cache/<id>.delta`; on miss, invoke the volume's attached `SegmentFetcher::fetch_delta_body`, which downloads and atomically writes (`tmp` + `rename`) the file. Then seek to `opt.delta_offset`, read `opt.delta_length` bytes.

`fetch_delta_body` issues a single range-GET for `[body_section_start + body_length, body_section_start + body_length + delta_length)` — the exact bounds, derived from the local `.idx` header. One request covers every Delta entry in the segment.

### Promote and the import host

`promote_to_cache` writes `.body` and `.delta` as two tmp+rename operations against distinct target files. The import host's Delta reads take the `Cached` path like any other host, except `.delta` is always present locally from promote onward.

### Crash safety

Both `promote_to_cache` and `fetch_delta_body` write `.delta` via tmp+rename, so a partial write never produces a visible `.delta` file. A rebuild after an interrupted fetch sees `.delta` as absent and the next read re-triggers the fetch.

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

## Phase 3: Snapshot-time filemap generation

Today, filemaps exist only for imported snapshots — they are written by `elide-core/src/import.rs` in the same pass that emits segment DATA entries from an ext4 image. Snapshots produced by `elide volume snapshot` on a writable (NBD-written) volume get only a signed manifest and a marker file; no filemap.

Phase 3 generates a filemap for such snapshots after they are written, asynchronously and best-effort. Scope is deliberately narrow: **filemap only, no coalescing of NBD-fragmented extents into file-sized extents.** The filemap records paths and fragment layouts exactly as the existing DATA entries describe them — one row per fragment. No body reads, no rehashing, no new segment writes.

### What Phase 3 delivers

- Filemap coverage for NBD-written volumes (previously: imported only).
- Path-level inspection of any snapshot — "what files exist here?", "what changed between these two snapshots?" — for debugging, fork-lineage analysis, and operator tooling.
- A prerequisite for every later filemap-consuming feature. Phase 4 in particular has nothing to consume without Phase 3.

### What Phase 3 does **not** deliver

**Delta compression for NBD-written volumes as the delta target.** (Throughout this section, *source* is the older extent used as the zstd dictionary — the same meaning as the `source_hash` field in the segment format — and *target* is the new extent being compressed against it.) Phase 3 generates a filemap but does not itself compress anything. The reason is a knowledge-gap issue in the upload pipeline:

- The coordinator's upload stage runs at **drain time**, when a pending segment is promoted to S3. At that moment, the coordinator knows only LBAs for the fragments in the segment — there is no LBA → filesystem-path mapping available anywhere.
- The filemap is the only structure that carries LBA → path, and Phase 3 generates it **after** the snapshot is sealed.
- The current design already encodes this constraint: §"Where in the upload pipeline" restricts drain-time delta to import-produced segments, because imports are the only code path where filesystem knowledge is available while segments are being written. Writable-volume drain bypasses delta entirely.

Closing this gap is the domain of **Phase 4**, which is not a new subsystem but an extension to the existing GC repack pass. GC already operates on post-snapshot-floor segments (segments above the most recent sealed snapshot, which are the segments drained since that snapshot) and rewrites them in place — repacked segments supersede the originals through the same cleanup path GC uses today. Phase 4 adds a delta step to that rewrite: for each LBA in a post-floor segment, consult the prior snapshot's filemap as a **heuristic hint** for which file used to live at that LBA, and try the prior file's extent hash as a zstd dictionary against the 4 KiB target fragment. The sealed snapshot providing the source filemap is never modified — Phase 4 only reads from it.

The "LBA → path" heuristic holds in the dominant case (in-place file modification — package upgrades, config edits, log writes at fixed offsets) and degrades gracefully elsewhere:

- **In-place modification:** prior LBA and current LBA both belong to the same file. The heuristic is right, the delta is tight.
- **New allocation / file growth:** post-floor LBA didn't belong to any file in the prior snapshot. Lookup returns nothing → raw body, no delta attempted.
- **ext4 metadata (inode tables, journal, group descriptors):** same as new allocation — not in the filemap → raw body.
- **Delete-then-reallocate:** prior file A was freed, ext4 reused the LBAs for file B. The heuristic hands zstd A's content as a dictionary for B's bytes. zstd still compresses correctly (dictionary compression is always correct regardless of dictionary), just with a useless ratio. **The existing size-check catches this unconditionally**: if `delta_length >= body_length * safety_margin`, emit a raw DATA entry and discard the delta. No corruption, just wasted compute.
- **Extent migration / defrag:** file moved to new LBAs. The new LBAs are treated as new allocations (lookup misses); the old LBAs have nothing writing to them.

So the correctness story is **always correct, sometimes no benefit**, with the size check as the backstop. Phase 4 may additionally short-circuit known-miss cases (e.g. ext4 metadata regions identified from the superblock) to avoid paying compression cost on LBAs that provably can't benefit, but correctness does not depend on it.

This approach works for Phase 4 because:

- **Post-floor segments are already rewriteable.** GC's existing machinery is the state machine we need. No new lifecycle, no sidecar manifests, no amendment of published snapshots.
- **The source filemap we need is the *prior* snapshot's, not the current one's.** Phase 3 is exactly what ensures that filemap exists for NBD-written volumes.
- **Published snapshots remain immutable.** Forks, remote pulls, and mirrors that committed to a historical snapshot's segment list see no change — Phase 4 never crosses the snapshot floor.

The two fragmentation shapes both reduce to the same Phase 4 operation:

- **Prior source whole** (imported prior snapshot): lookup returns a single filemap row whose hash covers the whole source file — the cleanest case. zstd gets a file-sized dictionary against a 4 KiB target and finds matches anywhere in the file.
- **Prior source fragmented** (NBD-written prior snapshot): handled by the single-row predicate from §"Narrow exception: unfragmented entries as delta sources" applied to the prior filemap — source files that appear as a single row in the prior snapshot's filemap participate; fragmented source files are skipped until a richer source-reconstruction design lands.

Phase 3 also does not change the filemap format, the segment format, or the synchronous snapshot operation. It is strictly additive — it produces a new file that some consumers can use if it exists.

The absence of immediate delta benefit is the main cost-benefit question for Phase 3. It lands anyway because the cost is modest (bounded by ext4 metadata size, not volume size) and because every later filemap-consuming feature — Phase 4 most of all — assumes snapshot filemaps exist.

### Narrow exception: unfragmented entries as delta sources

The drain-time knowledge gap above applies to the **target** side of delta compression — the new bytes being compressed. It does not apply to the **source** side. Phase 3 does unlock one small but genuine delta path without any Phase 4 infrastructure.

**Flow.** File-aware import runs with `--extents-from <writable-volume>`. The new import is the target; the writable volume's most recent snapshot is the source. The import walks ext4 directly, so it has filesystem knowledge on the target side. Phase 3 has already generated the source snapshot's filemap by the time the import runs, so it has filesystem knowledge on the source side. Neither side needs drain-time path information.

**Predicate.** For each new file, the import looks up the same path in the source filemap and checks whether it appears on **exactly one line**. A single-line entry means the source file is contiguous — one hash covers the whole file — and that hash plugs straight into the existing `source_hash` field. (A `file_offset = 0` row is necessary but not sufficient: every fragmented file also has a row at offset 0. The filemap is sorted by path, so the single-line check is a scan of adjacent rows.)

**Producer.** Reuses the Phase 2b filemap-based upload-time delta pass almost unchanged: the pass already accepts a source filemap and path-matches against it. The only change is that the source filemap may now be one Phase 3 generated for a writable-volume snapshot, rather than strictly one written by the import path. Source rows that are not single-line are skipped (existing best-effort rule for fragmented sources).

**Expected value profile.** The volume write path coalesces contiguous 4 KiB writes (§"Multi-block writes"), so small files flushed in one writeback tend to land as a single extent. Large files written across many flushes tend to fragment. The practical consequence:

- **Delta coverage** (files matched) is likely substantial for the long tail of small files — `/etc`, scripts, small binaries, interpreted source trees.
- **Delta savings** (bytes compressed) is likely modest, because the bytes that dominate total volume size live in large files, and those are the files most likely to fragment and be skipped.

This is the opposite of Phase 4's expected profile: Phase 4 coalescing would preferentially benefit the large-file cases this exception skips. The two are complementary — the narrow exception ships as a side-effect of Phase 3, and Phase 4 picks up the rest later.

### Worker placement and scheduling

A **coordinator-side background task**, enqueued after the synchronous `snapshot_volume()` returns to the caller. The synchronous path (flush WAL → drain pending → sign manifest → write marker → upload manifest + marker to S3) is unchanged and runs at full speed. The caller sees the snapshot as complete the moment the manifest and marker are published; the filemap appears some seconds later.

Failure of the worker — for any reason — is logged but does not affect snapshot validity. Consumers already tolerate missing filemaps (old snapshots, non-ext4 volumes, never-generated); the code path is exercised today by every writable-volume snapshot.

### Reading the frozen snapshot

The worker reads block state through **local `index/` + `segments/`**, using the **snapshot manifest's segment list** as the universe. This freezes the worker's view at snapshot time — writes that land on the volume after the snapshot was taken do not enter it, and no coordination with live I/O is required.

A `SnapshotBlockReader` presents a `read_block(lba) -> Option<Bytes>` interface backed by:

1. The manifest's segment list (frozen at snapshot creation)
2. A rebuilt LBA map over only those segments, using the same last-writer-wins rules as the volume's own rebuild path
3. The existing extent index for `hash → segment + offset` resolution
4. Direct reads from `segments/<ulid>` body files; `DedupRef` and `Delta` entries are resolved transparently through the normal read path

The reader is **metadata-only**: the ext4 walker requests only the blocks it actually needs (superblock, group descriptors, inode tables, directory blocks, extent tree nodes). File data blocks are **never read** — the filemap records their hashes by looking up the existing LBA map, not by rehashing bytes.

### ext4 detection

The ext4 magic `0xEF53` lives at byte 56 of the superblock, which starts at byte 1024 of the volume. One block read through the snapshot block reader decides whether to proceed. No magic → exit cleanly with no filemap; no error, no retry.

### Filemap construction

With a `SnapshotBlockReader` in hand, the existing ext4 walker in `src/extents.rs` is adapted to consume a `&dyn BlockReader` rather than a `&File`. The walk produces `path → Vec<FileFragment>`, the same structure the import path produces, with one difference: each fragment's hash comes from the **existing extent index** looked up by LBA range, not from rehashing the fragment bytes.

If a fragment's LBA range resolves to multiple distinct hashes (the range spans a boundary where a partial rewrite happened), it is emitted as multiple finer-grained filemap rows — each contiguous run of identical-hash blocks becomes one row. The v2 format already accommodates arbitrarily fine fragmentation.

### Output: tmp + fsync + upload + rename

Filemap output follows a write-temp, fsync, upload, rename discipline. The on-disk state unambiguously reflects work progress:

1. Write `snapshots/<snap_ulid>.filemap.tmp` with the generated rows, then `fsync` the file and its parent directory. A valid filemap now exists on disk.
2. PUT the file to S3 under the same prefix as the snapshot marker and manifest (`.../snapshots/<snap_ulid>.filemap`). Upload is idempotent.
3. Rename `.tmp` to `snapshots/<snap_ulid>.filemap` and fsync the parent directory.
4. Log success.

Any failure before step 3 leaves `.tmp` on disk. Any success leaves `.filemap` on disk in both local and S3 state. There is no ambiguous half-state.

### Restart recovery

On coordinator startup, a scan walks `snapshots/` in every locally-present volume and classifies each snapshot marker using three mutually exclusive disk states:

| State | Meaning | Action |
|---|---|---|
| Marker only, no `.tmp`, no `.filemap` | Never generated | Enqueue generation |
| `.tmp` present (with or without marker) | Upload pending or failed | Re-upload from `.tmp` (idempotent PUT), then rename |
| `.filemap` present | Complete | Skip |

No persistent job queue is needed — **the filesystem is the queue**. Each action converges to the terminal state. The scan is one `readdir` per volume; the system is robust to an arbitrary number of crashes, and a half-done filemap is never regenerated from scratch.

### Failure and best-effort semantics

- **Non-ext4 volume** (magic check fails): exit cleanly. Debug-level log only.
- **ext4 parse error** (corrupt superblock, unreadable inode table, etc.): warn-level log with the snapshot ULID and the first error encountered; delete `.tmp` if present; do not retry. The snapshot is still valid.
- **Local I/O error during block reads**: same as parse error — warn, delete `.tmp`, exit.
- **S3 upload failure**: leave `.tmp` in place. The next coordinator restart re-uploads from `.tmp`; the ext4 walk is not repeated. (A periodic in-process retry pass can be added later if restart is too coarse; restart alone is sufficient for a first implementation.)
- **Rename failure**: log and leave `.tmp` in place; restart recovery handles it.

### Cost model

For a 2.1 GB Ubuntu cloud image (findings.md: ~33,700 extents, ~100K–200K inodes):

- **Metadata block reads:** ~10–50 MB, served entirely from local `segments/`.
- **Walk:** linear in file count; `ext4_scan` already handles volumes of this size in well under a second on typical hardware.
- **Hash lookups:** O(1) per filemap row against the existing extent index.
- **Output write + S3 upload:** ~50 bytes per row (~2 MB for 33K fragments); single PUT.

**Expected wall-clock time:** single-digit seconds for a 2.1 GB volume, dominated by metadata reads and S3 upload latency, not CPU. The worker runs on a bounded task pool; snapshots on different volumes process in parallel without interference.

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
