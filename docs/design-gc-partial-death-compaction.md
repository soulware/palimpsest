# Design: compacting partial-LBA-death segments

**Status:** Proposed.

## Motivation

[`docs/design-gc-overlap-correctness.md`](design-gc-overlap-correctness.md) established correctness by *skipping* any segment containing a partial-LBA-death entry.

This doc describes a compaction path that **decouples the composite body from the surviving sub-runs**, so each can subsequently be handled by normal GC independently. It preserves the correctness property of the skip rule — every emitted LBA claim agrees with the live `lbamap` across its full range — while breaking the coupling that kept them pinned together.

The compaction output for a partial-death entry:
- `canonical_only` (when needed to preserve the composite hash — see step 2 below) holding the full composite body (all original bytes, live and dead), **plus**
- a new entry for each live sub-run.

What we gain in the general case is *separability*: after compaction,

- the composite body is referenced only by whatever external DedupRefs/Deltas still point at it;
- each surviving sub-run is a first-class entry with its own hash and its own LBA claim;
- normal GC can reclaim either piece on its own — the composite once its external refs go away, a sub-run once overwritten.

## Design

Partial-death is a fourth outcome of `collect_stats`'s per-entry classifier, alongside fully-alive, fully-dead, and `canonical_only`. The segment is no longer partitioned out of the GC pipeline (as PR #77 did); it flows through normal GC, with partial-death entries processed per the steps below and non-partial-death entries handled by the existing paths in the same pass.

Partial-death compaction runs **per entry**, not per segment. For each partial-death entry encountered by `collect_stats`:

1. **Resolve the body.** Depending on entry shape:
   - `Data` / `Inline` → read the inline body.
   - `DedupRef` → look up `extent_index[hash]` and read the referenced body.
   - `Delta` → resolve `base_hash`, apply the delta to reconstruct the body.

2. **Handle the composite body.** The action depends on the original entry's type:
   - **Data / Inline**: the source segment owns the composite body. If the hash is externally referenced (any `dedup_hash == entry.hash` or `base_hash == entry.hash` — see the External reference check section), emit `canonical_only` in the compaction output preserving the composite body. Otherwise drop the composite body entirely.
   - **DedupRef**: the source segment does not own the composite body — it lives in the canonical segment pointed at by `extent_index[entry.hash]`, which is untouched by this compaction. **Skip this step entirely.** No `canonical_only` is needed: the canonical body remains resolvable via its existing location, whether or not external references exist.
   - **Delta**: the source segment owns the delta payload, not the reconstructed body. If the hash is externally referenced, emit `canonical_only` holding the **reconstructed composite body inlined as a full body** (not as a Delta). This keeps dedup reads O(1) rather than forcing per-read reconstruction. Otherwise drop the delta payload entirely.

3. **Slice into live sub-runs.** Use `lba_map.extents_in_range(start, end)` and filter to runs where `r.hash == entry.hash`. Each such run is a surviving slice of the composite body. There can be one (head / tail) or more (interior, or multiple disjoint overwriters) — emit one sub-run entry per run.

4. **Emit each sub-run through the normal write path** into the compaction output segment. For each sub-run bytes `B_i`:
   - Compute `hash(B_i)`.
   - If `extent_index` already has that hash → emit a whole-body `DedupRef`.
   - Otherwise → emit a fresh `Data` entry containing `B_i`.
   - In both cases the emitted entry's `(start_lba, lba_length)` matches the surviving sub-run exactly.

## Why this is correct

Rebuild applies segments in ULID order. The compaction output segment has ULID > all source ULIDs. After rebuild:

- Every surviving sub-run is claimed by a first-class entry in the compaction output, with `(start_lba, lba_length)` matching exactly.
- The composite body remains resolvable via its hash if and only if something still refers to it (DedupRef or Delta). `canonical_only` preserves it when needed; dropping it when not needed is safe because no reader path can reach it.
- The source segment is deleted because nothing on disk still depends on it: its live sub-runs have been re-emitted as first-class entries, and its composite body has either been carried forward as `canonical_only` or dropped as unreachable.

## External reference check

The check covers **both** resolution paths that can pin a body by hash:

- `DedupRef.hash == H_composite` — a DedupRef in any segment resolves by hash lookup.
- `Delta.base_hash == H_composite` — a Delta entry's base is resolved by hash lookup.

This is the same predicate that normal GC's `canonical_only` emission must already be evaluating for whole-entry LBA-dead cases. If the existing check covers only `DedupRef.hash` and not `Delta.base_hash`, that is a pre-existing correctness gap in normal GC — not a gap introduced by this design — and should be fixed alongside this work.

## Scope and notes

- This path handles only partial-LBA-death entries. Fully alive and fully dead entries are untouched; normal GC handles them.
- No changes to `SegmentEntry` or `ExtentLocation` on-disk shapes.
- **Hash collision (overwriter's hash happens to equal `entry.hash`).** `extents_in_range` reports the whole range as matching — `matching_bytes == total_bytes` — classified as fully-alive. The entry is not partial-death and never enters this path.
- **Post-GC `extent_index` ordering.** The compaction output's `canonical_only` entry has a higher ULID than any pre-existing DedupRef to the same hash. Normal GC already produces this "DedupRef precedes its canonical" state post-GC (the "DedupRef → lower ULID" invariant holds at write time but not after GC, per PR #23); this design inherits whatever rebuild resolution the existing GC relies on and does not introduce a novel ordering problem.

## Testing

Extend the tests in [`design-gc-overlap-correctness.md`](design-gc-overlap-correctness.md):

- For each of the three wrong shapes (head / tail / interior), after partial-death compaction runs:
  - The source segment is absent.
  - The compaction output segment contains first-class entries covering exactly the surviving sub-runs.
  - If the composite hash had no external refs, the composite body is absent.
  - If the composite hash had external refs (add a DedupRef or Delta-base variant to the fixture), a `canonical_only` entry in the output preserves the composite body.
- The `MultiLbaWriteThenOverwrite` proptest SimOp continues to satisfy `gc_oracle`, and additionally asserts that the source segment is gone after GC.
