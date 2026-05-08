# LBA map claimant tracking

## Summary

Today the LBA map is a `LBA → hash` map. This proposal adds a per-entry
`segment_id`: `LBA → (hash, segment_id)`.

The new field doesn't change *what* a read returns (the hash already
identifies the body via `extent_index`); it changes *how cheaply* the
in-memory lbamap can be kept in sync with disk after a structural op
(GC apply, redact apply, repack apply, …).

## What we'd gain

Three concrete sites in `elide-core/src/volume/` currently call
`rebuild_lbamap_from_disk()` after a structural commit:

- `apply_plan_apply_result` (applied branch) — after a GC commit.
- `apply_redact_result` (rewritten branch) — after a redact commit.
- `apply_plan_apply_result` (cancelled branch) — defence-in-depth.

Each rebuild walks every segment file in the fork (and ancestor chain),
parses headers, and inserts every entry. With the
`rebuild_segments_unverified` optimisation the per-segment cost is just
file read + parse, but it's still O(all_segments × entries_per_segment)
on every structural commit.

With a `segment_id` per lbamap entry, those rebuilds collapse to a
per-output-entry incremental update:

```rust
for e in &output_entries {
    // Only override if our claim is newer than what's already there.
    if let Some(current) = lbamap.get(e.start_lba)
        && current.segment_id > new_ulid
    {
        continue;
    }
    lbamap.insert(e.start_lba, e.lba_length, e.hash, new_ulid);
}
```

O(output_entries × log N) instead of O(all_segments). Cost difference
matters for volumes with many committed segments.

## Why it's counter-intuitive

The reflexive question is: "if two segments both claim LBA L → H_x with
the same hash, do they really have different *claimants*? They produce
identical reads."

The answer is yes, and the case where it matters is exactly the
walk-order precedence that the current full-rebuild gets right by
accident of traversal order:

- Segment `u_input` writes Data(H_old, [0..4)) — claim 1.
- GC processes it; `u_gc < u_live`, GC's plan output also claims
  Data(H_old, [0..4)) under `u_gc`.
- Live write `u_live > u_gc` overwrites [0..4) with `H_live`.
- Apply commits the GC output. Disk now has both `gc/<u_gc>` (claim
  H_old) and the live writer (claim H_live).

What should `lbamap[0..4)` be?

Walk order says: highest-ULID claimant wins → `H_live`. The full rebuild
walks `u_gc` then `u_live` and the second insert overrides the first.

But an *unconditional* per-output-entry incremental update doesn't know
that. The GC commit's `lbamap.insert(0..4, H_old)` clobbers the live
writer's claim — silent data corruption.

This is exactly the regression the
`gc_output_loses_to_live_write_applied_after_gc` test caught. See PR
#277 for the inline comments documenting why the rebuild can't be
naively incrementalised today.

The structural fix is: track the claimant ULID per lbamap entry, then
let the incremental update conditional-insert based on it.

## The body-owner / LBA-claimant distinction

A subtler reason `extent_index` alone can't fill this gap:

- **`extent_index[hash] → segment_id`** means "the body for this hash
  lives in this segment." It's the *body owner*. There's exactly one,
  by construction — extent_index is a hash-keyed map.

- **`lbamap[L] → hash`** means "this LBA reads as this hash." The
  segment that *claimed* this LBA — the one that wrote it or wrote the
  DedupRef pointing at it — isn't recorded.

These can be different segments. A `DedupRef(H, [200..300))` entry in
segment `u_dr` claims LBA 200..300 → H, but H's body lives in some
other `u_owner`. `extent_index[H].segment_id == u_owner`, not `u_dr`.

So if the lbamap currently shows `lbamap[200] = H_x` and we want to ask
"what's the ULID of the segment that gave us this claim?", looking up
`extent_index[H_x].segment_id` answers a different question — it tells
us where H_x's body is, not who staked the claim on LBA 200.

That's the gap that adding a `segment_id` to lbamap entries closes.

## Cost

- LbaMap entry size grows by 16 bytes (a Ulid). For a fork with N
  distinct LBA ranges in the map, that's 16·N bytes resident.
- Insert/lookup cost unchanged.
- Rebuild cost unchanged.

For typical workloads N is bounded by the volume's logical block count,
not its segment count, so this is small relative to the main map's
existing memory footprint.

## Status

Implemented. `MapEntry` now carries `claimant_ulid`. New
`LbaMap::insert_if_newer` / `insert_delta_if_newer` install only on
sub-ranges where no overlapping current entry has a claimant `>=` the
caller's, splitting the incoming range around higher-claimant overlaps.

Five apply paths previously calling `rebuild_lbamap_from_disk()` now
use the incremental merge:

- `apply_plan_apply_result` (applied) — GC commit.
- `apply_redact_result` (rewritten) — redact commit.
- `apply_plan_apply_result` (cancelled) — defence-in-depth rebuild
  removed; cancel performs no lbamap mutation, and the stress
  invariant `assert_lbamap_consistent` catches divergence at the
  introducing site.
- `apply_sweep_result` — pending-segment compaction commit.
- `apply_repack_result` — per-segment repack commit.

`delta_repack` apply still rebuilds from disk: it converts entries
in-place and may split a single entry into sub-runs with different
post hashes, which interacts with the existing
`set_delta_sources_if_matches` guard. Porting it would mean replacing
that guard with claimant-comparison semantics — possible but
non-mechanical, deferred to a separate change.
