# Design: GC correctness for overlapped multi-LBA entries

**Status:** Proposed.

## Summary

A multi-LBA segment entry whose LBA range has been partially overwritten by a later write can either shadow or erase the overwriter when the coordinator's GC compacts the old segment. The bug is in `collect_stats` in `elide-coordinator/src/gc.rs`: it decides liveness with a single-LBA point query, which misses what is happening at the other LBAs in the entry's range. This doc describes the bug and a fix that skips compaction of any entry whose range disagrees with the live LBA map.

## Scenario that fails today

Two writes land in sequence:

- `S1` (ULID `U1`): `vol.write(100, 16 KiB)` → one segment entry `(start_lba=100, lba_length=4, hash=H)`. `lbamap` has `[100, 4, H, offset=0)`.
- `S2` (ULID `U2 > U1`): `vol.write(102, 4 KiB)` → segment entry `(102, 1, W)`. `lbamap` splits into `[100, 2, H, 0), [102, 1, W, 0), [103, 1, H, 3)`.

No segment on disk ever records the surviving `[100, 2, H)` or `[103, 1, H)` claims as first-class entries. They exist only at runtime, as `payload_block_offset` aliases inside `S1`'s original 4-LBA entry (see `elide-core/src/lbamap.rs::insert` for the split logic).

Now GC runs on `S1`. `collect_stats` inspects `S1`'s entry:

```rust
let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
// hash_at(100) == H → lba_live = true
```

The point query only looks at LBA 100. It does not see that LBAs 102-103 belong to `W` and `H`-at-offset-3 respectively. The entry is marked live, kept intact, and copied into the GC output `C` at ULID `U3 > U2`.

On rebuild:

1. `S2` applies: `lbamap[102] = W`.
2. `C` applies: inserts `(100, 4, H)`, which overwrites `lbamap[102]` with `H`.

Reads at LBA 102 now return `H`'s bytes instead of `W`. Silent data loss.

## The five overlap shapes

Using **head / tail / interior** anchored to the existing entry (not the later write):

| Shape | `hash_at(start_lba)` | `collect_stats` today | Correct? |
|---|---|---|---|
| Disjoint (no overlap) | entry.hash | kept intact | yes |
| Whole entry overwritten | different | `canonical_only` (if hash live elsewhere) or dropped | yes |
| **Head** (first LBA overwritten, tail survives) | different | `canonical_only`; surviving tail's binding lost on rebuild | **no** |
| **Tail** (last LBA overwritten, start survives) | entry.hash | kept intact; dead tail shadows overwriter on rebuild | **no** |
| **Interior** (both ends survive, middle overwritten) | entry.hash | kept intact; dead middle shadows overwriter on rebuild | **no** |

Three of five shapes are wrong. The shadow failure (tail, interior) and the loss failure (head) are the same root cause: the emitted entry's `(start_lba, lba_length)` tuple disagrees with the live `lbamap` across its range.

## Why `canonical_only` doesn't handle this

`canonical_only` (commit `73e43ff`) fires when `lba_live = false` and the entry's hash is still live elsewhere (via a DedupRef at another LBA). It demotes the entry: zeros `start_lba` / `lba_length`, preserves the hash and body. `lbamap::rebuild_segments` skips it. That is correct when the entry is `lba_length = 1` — the only shape existing tests exercise.

For `lba_length > 1` with a head overwrite, `lba_live = false` still fires (because LBA `start_lba` is dead), but the entry has a *surviving tail*. Demoting to `canonical_only` drops the tail's LBA claim. No other segment on disk re-asserts it. Rebuild loses the binding.

For tail and interior overwrites, `lba_live = true` because LBA `start_lba` is still the entry's hash. The demotion arm never fires. The entry is kept intact and re-emitted at a higher ULID, shadowing the overwriter.

In all three cases, the fundamental issue is the same: `collect_stats` is making a claim about the full range based on a single-LBA sample. `canonical_only` solves a different problem (preserving a body when its LBA binding is entirely dead) and happens to reuse the same code path, which is why it fires incorrectly on head overlaps.

## Fix: skip partial-LBA-death in `collect_stats`

For any body-bearing entry (Data, Inline, Delta), use a range scan instead of a point query:

```rust
let end = entry.start_lba + entry.lba_length as u64;
let runs = lba_map.extents_in_range(entry.start_lba, end);

// Count how many of the entry's claimed LBAs still map to entry.hash.
let matching_bytes: u64 = runs.iter()
    .filter(|r| r.hash == entry.hash)
    .map(|r| (r.range_end - r.range_start) * BLOCK_BYTES)
    .sum();
let total_bytes = entry.lba_length as u64 * BLOCK_BYTES;

match matching_bytes {
    n if n == total_bytes => {
        // fully alive: keep the entry intact (current lba_live path)
    }
    0 => {
        // fully dead: canonical_only if hash still externally referenced,
        // else removed. Current logic.
    }
    _ => {
        // partially alive: skip compaction of this segment this round.
        // The bloated entry stays on disk at its original (low) ULID.
        // Rebuild applies segments in ULID order; lbamap::insert's split
        // logic correctly handles the overlap. No shadow, no loss.
    }
}
```

### What "skip" means in practice

`SegmentStats` gains a flag indicating the segment has at least one partially-alive entry. `find_least_dense` and the sweep partition exclude such segments from this GC pass. The segment remains in `index/`. Its body and entries are untouched.

### Why this is correct

The segment's bloated entry at ULID `U_original` stays on disk. On rebuild:

1. Older segments apply first.
2. The bloated segment applies: `lbamap.insert(start, len, hash)` claims the full original range.
3. The overwriter at `U > U_original` applies: its insert splits the bloated claim. `lbamap` now reflects both the overwriter and the surviving `payload_block_offset`-aliased tail/head.
4. Later segments apply on top.

The in-memory runtime `lbamap` and the rebuild-from-disk `lbamap` now produce the same result. The coordinator's liveness view matches the volume's, so stale-liveness cancels stop firing.

### What this does not do

A segment with a partial-LBA-death entry stays on disk indefinitely. It carries its dead bytes with it until either:

- Later writes happen to overwrite the surviving sub-ranges, making the entry fully dead — at which point GC compacts it normally.
- The segment is rewritten out of existence by some future cleanup pass.

There is no cleanup pass today. Reclaiming the dead storage in such segments is a separate, unplanned problem. Whether it needs a solution depends on how often the shape occurs on real workloads — measurement first, before any mechanism.

## How `canonical_only` narrows

With the skip rule in place, `canonical_only` is only reached when `matching_bytes == 0` — the entire entry range is LBA-dead. That is the shape it was designed for: whole entry overwritten while the hash remains live via a DedupRef at some other LBA. The incorrect head-overlap demotion never triggers because the skip rule catches partial-LBA-death first.

## Testing

Unit tests at the `collect_stats` level exercise each shape:

- **Tail overwrite**: 4-LBA Data entry at `[100, 4)`, 1-LBA write at `LBA 103`. Assert the segment is marked partial-death and excluded from GC compaction. Before the fix, the shadow invariant (every emitted LBA claim agrees with the current `lbamap` over its full range) fails at `LBA 103`.
- **Interior overwrite**: same, but the 1-LBA write hits `LBA 102`.
- **Head overwrite**: same, but the 1-LBA write hits `LBA 100`. Assert the loss invariant (every LBA within the original range that still resolves to `H` is claimed by some emitted non-canonical-only entry). Before the fix, `LBA 101` fails the loss invariant.

A proptest SimOp variant `MultiLbaWriteThenOverwrite { start, len, overlap_off, seed }` drives the full GC round-trip; `gc_oracle` holds because the bloated segment stays put and `lbamap` rebuilds correctly.
