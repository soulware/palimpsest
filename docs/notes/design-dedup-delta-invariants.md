---
status: descriptive
related: [design-delta-compression.md, design-gc-overlap-correctness.md]
---

# Dedup and delta correctness — worked examples and invariants

Architecture-level summary lives in `../architecture.md`. This note captures the edge cases that make the canonical-presence and pinning invariants concrete, and explains why `lba_referenced_hashes()` is sourced from the LBA map (not extent-index keys).

## Two DedupRefs with the same hash in one segment

Two edge cases worth making explicit, since they test both invariants simultaneously.

**Variant (a) — both DedupRefs to an external canonical.** A segment S contains two index entries with the same hash H, both DedupRef. The canonical DATA for H lives in some other segment S_canon (an ancestor segment, a prior segment in this volume, or a segment in another volume under the same dedup scope).

- Format: both entries have `stored_offset = 0, stored_length = 0`. Neither contributes to S's `body_length`.
- LBA map rebuild: both entries contribute `(lba → H)`. Two distinct LBAs both resolve to H.
- Extent index rebuild: both are skipped (DedupRef never enters the extent index). The extent index maps `H → S_canon` from S_canon's own scan.
- Reads of either LBA → `extent_index.lookup(H) → S_canon` → serve from S_canon's body. Correct.
- GC of S: both DedupRef entries kept alive iff their LBAs are live. DedupRef passes through to the GC output unchanged. `H`'s canonical stays in S_canon (or is Repacked there by a separate GC pass).

**Variant (b) — one DATA + one DedupRef to the sibling DATA, same segment.** A segment S contains a DATA entry for H at LBA1 and a DedupRef for H at LBA2. The DATA entry in S is the canonical for H.

- Format: the DATA entry has `stored_offset = X, stored_length = L, body_length += L`. The DedupRef has `stored_offset = 0, stored_length = 0, body_length += 0`.
- LBA map rebuild: both contribute: `LBA1 → H` and `LBA2 → H`.
- Extent index rebuild: DATA inserts `H → {segment=S, body_offset=X, body_length=L}`. DedupRef is skipped (order-independent).
- Reads of LBA1 and LBA2 both resolve through `extent_index.lookup(H)` back into S at offset X. Correct.
- GC of S, with LBA1 overwritten (now maps to some other hash H'): the DATA at LBA1 has `lba_live = false` but `extent_live = true` (it is canonical for H) and `live_hashes.contains(H) = true` (because LBA2 still references H via the DedupRef). The DATA is therefore kept alive and carried into the GC output — the sibling DedupRef at LBA2 is what keeps it alive.

**Why `live_hashes()` must be sourced from the LBA map, not from extent_index keys.** Variant (b) only works because `lba_map::live_hashes()` is `self.inner.values().map(|e| e.hash).collect()` — every hash in the LBA map contributes, whether the LBA was populated via a DATA write or a DedupRef write. If `live_hashes()` were ever "optimised" to a DATA-only filter, the DATA at LBA1 in variant (b) would become invisible to GC (not in live_hashes, not lba_live) and would be dropped as dead. The sibling DedupRef at LBA2 would then lose its canonical, violating the canonical-presence invariant. The function is named `lba_referenced_hashes()` for precisely this reason — the name is hostile to the dangerous misreading.

## Delta entries: format-level shape

`EntryKind::Delta` mirrors thin DedupRef: `stored_offset = 0`, `stored_length = 0`, no body bytes. The entry's content is materialised by fetching a delta blob from the segment's delta body section and decompressing it against a source extent body located via `extent_index.lookup(source_hash)`. Multiple delta options per entry act as hints; the reader picks the first whose source resolves (earliest-source preference).

- **Delta source must be a DATA entry** — never another Delta. No delta-of-delta chains.
- **Unified across `pending/`, `cache/`, and S3**, same as DedupRef. The delta body section is preserved on disk and in S3; Delta entries contribute nothing to `body_length`.
- **Read path:** `extent_index.lookup(source_hash)` → fetch source body → fetch delta blob → zstd-dict decompress. If no option's source resolves, the fetch fails; there is no full-body fallback in the same segment (the price of thin delta).

## Two invariants make thin Delta sound

Mirroring the DedupRef invariants:

1. **Pinning invariant.** Every live Delta's source extent lives in a segment GC cannot rewrite or remove. Cross-import delta sources come from `extent_index` provenance lineage, which names only fully-snapshotted ancestors (enforced at import time). Combined with the snapshot floor + first-snapshot pinning rules, the source segment is GC-stable for the lifetime of any Delta referencing it.
2. **Canonical-presence invariant.** For every live Delta with source hash H, `extent_index.lookup(H)` returns a DATA entry. Maintained by the `lba_referenced_hashes()` extension below.

## `lba_referenced_hashes()` extension

The set is extended to include delta source hashes for every live Delta LBA. With this folding, GC's existing rule — "keep DATA alive iff any live LBA references its hash" — automatically covers delta sources with no new code path. The rule's load-bearing property (variant (b) DedupRef correctness) is unchanged; the set simply contains more hashes per live LBA in the Delta case.

GC carries Delta entries through `compact_candidates_inner` unchanged, same pattern as DedupRef: a match arm that debug-asserts `stored_offset == 0, stored_length == 0`, copies the entry through to the output, and skips `fetch_live_bodies`. The delta blob stays in its source segment's delta body section across GC runs — GC does not move delta blobs between segments. Repack handoff lines are emitted only for DATA entries.

Historical note: an earlier design considered delta compression as purely S3-side, with local segments carrying full extents and the coordinator computing deltas fresh at upload. Thin delta collapses this — the producer writes the delta entry thin in-process during import, so local and S3 layouts stay unified (the same "no local/S3 asymmetry" property that thin DedupRef establishes).
