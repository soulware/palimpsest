# Design: Delta compression via file-path matching

Status: **Phases 1–5 Tier 1 landed; Tier 2 deferred.** See § Phases.

Date: 2026-04-09 (updated 2026-04-13)

---

## Context

Empirical measurements ([findings.md](findings.md)) show zstd-with-dictionary delta compression achieves 94% marginal S3 fetch savings between Ubuntu 22.04 point releases when the prior file version is used as the dictionary. The segment format already supports delta entries (`source_hash`, delta body section, multiple delta options per extent — see [formats.md](formats.md)). The missing piece is the pipeline that produces them.

The core problem is **source selection**: given a changed extent in a new snapshot, which extent from the prior snapshot should be the zstd dictionary?

LBA matching doesn't work — ext4 can relocate file data across updates, so the same LBA may hold a completely different file. **File-path matching** does: two snapshots of the same volume represent two states of the same ext4 filesystem, and the same file path in both is overwhelmingly likely to be the same logical file.

This requires:
1. A `path → hash` map at each snapshot (the **filemap**)
2. File-aligned extents (one extent ≈ one file), so a delta's dictionary is the whole prior file

Both are produced at import time by parsing the ext4 image before writing segments.

NBD live writes cannot meet these preconditions — every write lands as a fragmented 4 KiB block, and the drain-time coordinator has no LBA → path information. NBD-written volumes therefore get delta compression via the post-snapshot delta repack path (Phase 5), which runs before drain on pending segments above the latest snapshot floor and consults the prior sealed snapshot's LBA map directly — no filemap needed for Tier 1. A filemap-driven Tier 2 for LBAs that shifted between snapshots is scoped but deferred.

## Filemap format

A new file per snapshot: `snapshots/<ulid>.filemap`. Line-oriented text, one entry per file fragment:

```
# elide-filemap v2
<path>\t<file_offset>\t<blake3-hex>\t<byte_count>
```

A **contiguous file** (the common case) appears as a single line with `file_offset = 0`; its hash equals the whole-file hash and matches the segment DATA entry's hash. A **fragmented file** appears as multiple lines with the same path, ascending `file_offset`s covering the file end-to-end, and per-fragment hashes.

Properties:
- Written once at filemap-generation time, never modified
- Uploaded to S3 alongside segments so the filemap survives local disk loss
- Only produced for ext4 volumes; non-ext4 volumes have no filemap
- One filemap per snapshot, not per segment

Keyed by path because the delta producer's workflow is path-driven: iterate the child filemap, look up the same path in the source filemap, compare fragment tuples. Two fragments are "the same fragment" iff they share `(file_offset, byte_count)`.

**Not in the segment format.** File paths are a snapshot-level concept. A segment is a bag of extents spanning multiple files and snapshots. Embedding paths in the segment index would couple the storage format to filesystem awareness and add per-entry overhead for data that's only used once, at delta-compute time.

## File-aware import

The import path parses the ext4 image and iterates files instead of blocks:

1. Parse the ext4 superblock and inode tables, enumerate regular files
2. For each file, collect its ext4 extent tree, read physical blocks in logical order, hash the concatenated bytes as a single extent
3. Emit one DATA entry per contiguous LBA range (fragment) covering the file, with the fragment's own BLAKE3 as the hash
4. Write the filemap alongside the snapshot marker

The ext4 parsing infrastructure lives in `elide-core/src/ext4_scan.rs`: superblock walkers, inode tables, extent tree traversal, file-fragment enumeration. The filemap is written as a side effect of the import walk — there is no separate filemap generation step.

**Contiguous files** (the common case for fresh `mkfs.ext4` + extract) produce exactly one DATA entry whose hash matches the filemap row. **Fragmented files** produce one DATA entry per contiguous LBA range; the segment format is unchanged. The knowledge that multiple entries belong to the same file lives only in the filemap.

**Delta compression on fragmented files is best-effort.** If the child and source filemaps agree on the fragment layout for a given path, the producer pairs fragments by offset and computes per-fragment deltas. If layouts differ, no delta for this file — the fragments are stored as full DATA entries. Exact dedup still applies opportunistically. This is an accepted degradation: fragmented layouts are rare for fresh imports, and the skipped files are typically large binaries where delta benefit is already marginal.

**Metadata and free space.** ext4 metadata (superblock, group descriptors, inode tables, directory blocks, journal) is imported block-by-block at 4 KiB granularity — small, not a dedup or delta candidate, and the read path needs it at exact LBAs. Free-space blocks (all zeros) are skipped.

This was a clean break in the import output format. Previously imported volumes remain valid and readable; new imports produce the file-aware layout unconditionally.

## Thin `Delta` entry kind

Naively storing the full extent body *and* the delta blob is wasteful: cold hosts pay for body bytes they would never fetch, warm hosts pay for body bytes they never need. `EntryKind::Delta` mirrors thin `DedupRef` (PR #36):

- `stored_offset = 0, stored_length = 0` — the entry reserves no body space
- Content is served by fetching the delta blob from the segment's delta section (or inline, via `FLAG_DELTA_INLINE`) and zstd-dict-decompressing against the source extent body, located via `extent_index.lookup(source_hash)`
- One or more delta options per entry (`delta_count ≥ 1`) act as **hints**: the reader picks the first `source_hash` it can resolve locally. Multiple options give graceful degradation across skipped releases.
- **Delta source must resolve to a DATA entry.** No delta-of-delta chains. This bounds decompression cost to a single dictionary apply and keeps GC liveness reasoning linear.

### Invariants

1. **Pinning.** Every live Delta's source extent lives in a segment GC cannot rewrite or remove. The snapshot floor + first-snapshot pinning rules already provide this for cross-import delta sources: `extent_index` lineage sources are always fully-snapshotted ancestors, checked at import time by Phase 2.
2. **Canonical presence.** For every live Delta with source hash H, `extent_index.lookup(H)` returns a DATA entry. Maintained by extending GC's existing rule: a DATA entry stays alive if any live LBA references its hash *either directly (Data or DedupRef) or indirectly via a Delta's `source_hash`*.

**`lba_referenced_hashes` extension.** A Delta LBA reaches two hashes that must stay alive: its own content hash and the source hash. The source hash is the load-bearing one — it folds into the same `lba_referenced_hashes()` set PR #36 introduced, so GC's "keep DATA alive iff any live LBA references its hash" rule covers delta sources with no special-case code path.

**GC preservation.** GC carries Delta entries through `compact_candidates_inner` unchanged, same pattern as DedupRef: a match arm that asserts `stored_offset == 0, stored_length == 0` and copies the entry through, skipping `fetch_live_bodies` entirely. The delta blob itself stays in the source segment's delta section; GC does not move delta blobs between segments.

**Regression tests** (mirroring `gc_ordering_test.rs` from PR #36):
- Segment containing a Delta whose source lives in an ancestor segment passes GC unchanged.
- Segment containing a Delta whose source lives in the *same* segment as a Data entry is compacted correctly when the Delta's LBA is overwritten pre-GC.
- Crash recovery mid-compaction with Delta entries present.
- Format-version bump rejects old fixtures encoding delta-of-delta chains.

## Read path

Delta decompression lives in `elide-core::volume::try_read_delta_extent` with body fetching in `elide-fetch::fetch_one_delta_body`.

### Source selection

For an entry with one or more delta options:

1. Scan delta options in order. Look up each `source_hash` in the local extent index. If the source segment's body is **already present in local cache** (`.present` bit set, or source lives in `pending/`/`segments/`), pick this option and stop.
2. If nothing was already cached, scan again and pick the option whose source segment has the **earliest ULID** — oldest bases are most reusable across future deltas and give deterministic cross-host cache reuse: any two hosts fetching the same child extent pick the same source, so their caches converge rather than fragmenting across many bases.
3. Fetch the source body (local read or demand-fetch), fetch the delta blob, zstd-dict decompress → materialised bytes → write to `.body`, set the `.present` bit.
4. If no option resolves: for a Data entry with delta options, fall back to the full body in the segment's body section; for a thin Delta entry, return a fetch error. No fallback is the price of thin delta, symmetric with thin DedupRef's dependence on `extent_index.lookup()`.

### Delta body cache file

A **pull host** (never ran the import, pulls the segment from S3) reads Delta LBAs by demand-fetching the segment's delta body section on first access. The delta body lives in its own local file, parallel to `.body`:

```
cache/<id>.idx     — header + index + inline (permanent, coordinator-written)
cache/<id>.body    — sparse body section; Data entries at stored_offset, holes elsewhere
cache/<id>.present — per-entry bitset for .body
cache/<id>.delta   — delta body region, starting at byte 0; absent iff no deltas or not yet fetched
```

Keeping the delta region out of `.body` means `.body` has a single unambiguous shape; "do I have the delta body locally?" is answered by `.delta`'s existence, not by comparing sizes against header fields.

`extent_index::rebuild` registers every Delta entry it encounters with `DeltaBodySource::Cached` unconditionally — it does not stat `.delta`; the reader handles the missing-file case. Segments still in `pending/` register as `DeltaBodySource::Full { body_section_start, body_length }` because the delta body lives inline at the end of the pending file. The two variants are mutually exclusive: a segment is either in `pending/` or has been promoted.

`try_read_delta_extent` dispatches on `DeltaBodySource`:
- `Full`: seek inside the segment file to `body_section_start + body_length + opt.delta_offset`, read `opt.delta_length` bytes.
- `Cached`: open `cache/<id>.delta`; on miss, call `SegmentFetcher::fetch_delta_body`, which issues a single range-GET for the entire delta region and tmp+renames the file. `promote_to_cache` writes `.body` and `.delta` as two tmp+rename operations against distinct target files.

### Warm hosts, not cold starts

Delta options benefit **warm hosts** — hosts that already have a prior version's extents cached. Flow:

1. Host runs version N, cached in `cache/`
2. Operator publishes version N+1 imported with `--extents-from N`
3. Host pulls version N+1; signed provenance preserves the lineage
4. Volume open rebuilds the extent index across the signed lineage — N's cached extents are visible
5. Demand-fetch for N+1's changed extents checks delta options; if `source_hash` resolves locally (from N's cache), fetch the small delta blob instead of the full body

On a **cold host** (no prior version cached), no source is available locally, so the full body is fetched. This is correct and transparent; delta is an optimisation, never a requirement.

The `prefetch_indexes` coordinator task downloads `.idx` files for every source in the signed lineage on volume discovery, so the LBA map and extent index are always complete without body data.

## Non-ext4 volumes

No filemap. No delta. Full extents only. The read path, segment format, and GC all work identically. This is the absence of an optimisation, not a degraded mode that needs handling.

## Phases

| # | Name | Status |
|---|---|---|
| 1 | Delta format + PoC drain-time producer | **done** (superseded) |
| 2 | `extent_index` lineage + cross-import dedup | **done** |
| 3 | File-aware import + thin Delta + filemap producer | **done** |
| 4 | Snapshot-time filemap generation | **done** |
| 5 Tier 1 | Pre-drain delta (same-LBA prior extent) | **done** |
| 5 Tier 2 | Filemap-driven cross-LBA source selection | deferred |
| 6 | Content-similarity source selection | deferred |

**Phase 1 — delta format + PoC.** Segment format v3 with delta table in the index and a coordinator-side `compute_deltas()` hooked into `drain_pending()`. Proof-of-concept that validated the end-to-end machinery; removed in Phase 3. The format itself is unchanged and still in use.

**Phase 2 — `extent_index` lineage.** `elide volume import --extents-from <name>` (repeatable) contributes one or more existing volumes' extents to the new volume's hash pool, carried in the signed `volume.provenance`. Blocks whose hash already exists in any source are written as `DedupRef` during the import block loop. Delivers cross-import dedup and lays the groundwork for filemap delta. Details — eviction rule, `MAX_EXTENT_INDEX_SOURCES = 32`, OCI-layer relationship — live in [architecture.md](architecture.md).

**Phase 3 — file-aware import, thin Delta, and filemap producer.** Three pieces that only make sense together, landed as one unit:

1. **File-aware import** (§ File-aware import). `elide-core/src/import.rs` parses ext4 metadata, iterates files, and emits one DATA entry per contiguous fragment. The filemap is written as a side effect of the import walk.
2. **Thin `EntryKind::Delta`** (§ Thin Delta entry kind). `stored_offset = 0, stored_length = 0`; GC preservation and `lba_referenced_hashes` folding mirror DedupRef; delta source must resolve to a DATA entry (no delta-of-delta). `FLAG_HAS_DELTAS` on Data entries is now format-level dead code — the table still exists but no producer sets the flag. The delta-aware reader lives in `try_read_delta_extent` (`elide-core/src/volume.rs`) and `fetch_one_delta_body` (`elide-fetch/src/lib.rs`), with `cache/<id>.delta` as the per-segment delta body file.
3. **Filemap delta producer** (`elide-core/src/delta_compute.rs`). Runs inside `elide-import` after all pending segments are written but before `serve_promote` publishes the control socket. Loads child + source filemaps via provenance lineage, path-matches changed files, computes `zstd::compress_with_dictionary(child_bytes, source_bytes)` against locally-available source extent bodies, and rewrites pending segments so matching DATA entries become thin Delta entries. Skip heuristics: extent below a fixed threshold, `delta_length >= body_length`, or source body not locally available. Skipped extents remain as normal Data entries.

The producer runs **in-process in `elide-import`** rather than in the coordinator upload stage: the signer stays inside the volume process, no key material crosses a boundary, and the delta work is complete before the segments are promoted to the coordinator for upload. Writable-fork drain segments bypass delta entirely — they have no filemap and no path knowledge.

Phase 3 also removed the Phase 1 PoC path at `elide-coordinator/src/delta.rs` and its `drain_pending()` hook. Two producers racing for the same entries with different source-selection rules was unnecessary complexity once the real path landed.

**Phase 4 — snapshot-time filemap generation.** Runs inline as step 4b of the coordinator's snapshot sequence in `elide-coordinator/src/inbound.rs`, after `sign_snapshot_manifest` and before the S3 upload — under the per-volume snapshot lock, so it cannot race with drain, GC, or eviction. Parses ext4 metadata from the frozen snapshot's segments via `BlockReader::open_snapshot` (manifest's segment list + rebuilt LBA map + extent index) and emits `snapshots/<ulid>.filemap` using the existing hashes looked up by LBA — no body reads, no rehashing. Filemap generation failures are logged as warnings and do not fail the snapshot: Phase 4 is strictly additive, producing a new file that consumers may use if it exists.

Scope is deliberately narrow: **filemap only, no coalescing of NBD-fragmented extents into file-sized extents.** The filemap records paths and fragment layouts exactly as the existing DATA entries describe them. Non-ext4 volumes and parse failures skip cleanly. Output uses write-tmp, fsync, upload, rename — the filesystem is the queue, and restart recovery re-uploads any leftover `.tmp`.

Extends filemap coverage to NBD-written volumes. Does not itself compress anything — the drain-time upload stage has no LBA → path mapping, and by the time Phase 4 runs, the segments are already sealed. Phase 5 picks up the delta opportunity.

One small exception lands as a side-effect: after Phase 4, a new import using `--extents-from <writable-volume>` can use the writable volume's snapshot filemap as a delta source. The import walks ext4 directly (target-side knowledge), Phase 4 has already generated the source filemap (source-side knowledge), and the Phase 3 producer works unchanged. Source files appearing as a single filemap row (contiguous) participate; fragmented sources are skipped.

**Phase 5 — post-snapshot delta repack.** Rewrites **post-snapshot-floor segments** (drained since the most recent sealed snapshot, not yet uploaded) with zstd-dictionary deltas against same-LBA extents in the prior snapshot. Two source-selection tiers:

1. **Tier 1 — same-LBA prior extent (landed).** Open a snapshot-pinned `BlockReader` on the prior sealed snapshot. For each 4 KiB target LBA, look up the fragment that occupied that LBA in the prior snapshot — `lba_map.lookup(lba) → hash → extent_index.lookup(hash)` returns the **whole fragment**, not a 4 KiB slice, so the dictionary is automatically the full containing extent (typically a multi-block file fragment). Needs nothing beyond the two in-memory rebuilds `BlockReader::open_snapshot` already does; no filemap lookup.
2. **Tier 2 — same-path cross-LBA (deferred).** Current and prior filemaps (from Phase 4) provide path → fragment mapping. When an LBA moved between snapshots (defrag, rename-with-realloc, logrotate), Tier 1 misses: the prior snapshot's LBA map has a different or absent hash at that LBA. Tier 2 would resolve the current LBA → path via the current filemap, look the same path up in the prior filemap, and use that fragment as the dictionary.

Tier 1 covers the dominant case — in-place file modification (package upgrades, config edits, log writes at fixed offsets) — and catches it cheaply with no filemap involvement on the read side. Tier 2 is the cleanup for the long tail where LBAs shifted; it can land later without any format change. zstd dictionary compression is always correct regardless of dictionary; the heuristic only affects compression ratio. The existing `delta_length >= body_length` size check catches every miss by emitting a raw DATA entry. Correctness story: **always correct, sometimes no benefit.**

**Pipeline position: pre-drain, not GC repack.** Tier 1 lives in `Volume::delta_repack_post_snapshot` (elide-core/src/volume.rs) and runs from the per-volume coordinator tick **before** `drain_pending` uploads to S3. Earlier drafts of this doc described it as an extension of the GC repack pass; the shipping implementation took a simpler position — the work is the same (rewrite post-floor pending segments in place) but it does not piggy-back on GC's liveness analysis. Rationale: delta repack operates on sealed segment files below the flush threshold, not on a liveness picture; sharing GC's machinery would have added coupling without benefit. The tick-loop call sits between `repack` and `drain_pending` so converted segments reach S3 as thin Delta entries on their first upload rather than going up as full bodies and being re-uploaded later.

**Implementation details.** The walk iterates `pending/` segment files whose ULIDs sit above the latest sealed snapshot's ULID, skipping snapshot-frozen segments. For each post-floor segment, `delta_compute::rewrite_post_snapshot_with_prior` (elide-core/src/delta_compute.rs) walks Data entries, queries the prior snapshot's `BlockReader` for a same-LBA source fragment, computes `zstd::compress_with_dictionary(child_bytes, source_bytes)`, and rewrites the segment body in place with converted entries moved into the delta section and the body section shrunk accordingly. The extent index is refreshed for every entry in the rewritten segment (Data, Inline, and Delta) so that the actor's in-memory view matches the new on-disk layout before the next read. The source fetcher passed to `BlockReader::open_snapshot` is `Box::new(|_| None)` — delta repack is best-effort, and if a source body is evicted locally the entry is left as a Data entry rather than pulled from S3 purely to seed a dictionary.

No new lifecycle: post-floor segments are already pending and will be uploaded by the next drain, the rewritten segments supersede the originals on disk via in-place replacement, and the sealed snapshot providing the source state is read-only. Published snapshots remain immutable — Phase 5 never crosses the snapshot floor. This is the step that delivers delta for writable volumes and closes the drain-time LBA → path knowledge gap.

**Snapshot integrity prerequisite.** Tier 1 was the first real consumer of `BlockReader::open_snapshot` in the live coordinator loop and surfaced a latent bug: the snapshot manifest was being signed over `index/` before any in-flight GC handoffs had been applied, so volume-applied handoffs from the prior tick could still reference segments that `promote_segment` was about to delete. The next tick's delta_repack would open the manifest and fail with ENOENT on the vanished `.idx`. Fix: drain GC handoffs (`apply_gc_handoffs` IPC + `apply_done_handoffs`) inside `snapshot_volume` before `sign_snapshot_manifest`, under the snapshot lock. Same root cause reached further: the structural fix for `gc/` single-writer semantics (routing every `gc/` mutation through the volume actor via `promote_segment` and `finalize_gc_handoff` IPCs) landed alongside Tier 1, and the self-describing handoff (April 2026) replaced the manifest sidecar with a header field — see `docs/design-gc-self-describing-handoff.md`.

**Phase 6 — content-similarity source selection.** Filesystem-agnostic delta via content fingerprinting. Marginal benefit over path matching for the primary workload; significantly more complex. Deferred.

## Open questions

- **Symlinks and hardlinks.** The filemap maps paths to content hashes. Symlinks are skipped (not regular files). Hardlinks (multiple paths, same inode) produce duplicate filemap entries with different paths but the same hash — harmless, but worth deduplicating to keep the filemap compact.
