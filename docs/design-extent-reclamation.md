# Extent reclamation

**Status:** Work in progress. The code described below is a proof-of-concept, not a finished feature. Do not cite anything in this doc when solving an unrelated correctness problem.

## What exists today

A volume-side primitive that rewrites bloated multi-LBA extent bodies into compact new-hash entries, and a test hook to drive it.

### Why anything needs rewriting

`Volume::write()` produces one `SegmentEntry` per write, with `lba_length` equal to the inbound size. Later partial overwrites split the LBA map entry via `payload_block_offset` aliasing (`elide-core/src/lbamap.rs::insert`) without touching the original stored body. Reads remain correct — each surviving sub-range resolves through the original compressed payload — but the body may end up with many dead interior blocks that every read still has to decompress past. The alias-merge primitive rewrites those surviving sub-ranges into fresh compact payloads and leaves the original hash fully orphaned, which lets a later GC pass reclaim it.

### The primitive

Three phases, of which only the middle is heavy. See `docs/design-noop-write-skip.md` for why rewrite writes pass cleanly through the no-op skip path.

1. **Snapshot** (`Volume::reclaim_snapshot`). Under the actor lock, clone `Arc<LbaMap>` and capture the extents over the target LBA range. Cheap — O(log n) range query plus an Arc bump. Returns a `ReclaimPlan`.

2. **Compute rewrites** (`ReclaimPlan::compute_rewrites`). Runs without the actor lock. For each non-zero-hash extent in the plan, two gates decide whether it's worth rewriting:
   - **Containment.** Every run of the hash in the current lbamap must sit inside the target range. Rewriting a hash whose body is referenced outside the target would leave those outside references pointing at the now-bloated body and make things worse.
   - **Bloat.** At least one run of the hash must have `payload_block_offset != 0`, indicating a prior split that left dead bytes inside the stored body.
   Extents that pass both gates have their live bytes read through the normal read path, re-hashed, and compressed. The output is a list of `ReclaimProposed { start_lba, data, hash }`.

3. **Commit** (`Volume::reclaim_commit`). Under the actor lock, check `Arc::ptr_eq(plan.lbamap_snapshot, self.lbamap)`. If the pointers differ, something mutated the lbamap between phase 1 and now — return `ReclaimOutcome { discarded: true, .. }` without doing anything. Otherwise apply each proposal through `Volume::write_with_hash`, which routes through the same WAL → pending → promote pipeline as any other write.

### The test hook

`VolumeHandle::reclaim_alias_merge(start_lba, lba_length)` ties the three phases together in the simplest possible way: snapshot, compute, commit, return the outcome.

`scan_reclaim_candidates` walks the live lbamap and extent index and produces `ReclaimCandidate { start_lba, lba_length, dead_blocks, live_blocks, stored_bytes }` entries for hashes with detectable bloat (controlled by `ReclaimThresholds`, all defaults placeholder values).

`elide volume reclaim <name>` calls the scanner, then calls the primitive once per candidate. It exists so the primitive can be exercised end-to-end — this is not a customer-facing operation.

## What's not here

Everything about when or how reclamation should happen in production:

- No scheduler. No coordinator-driven invocation. No drain loop.
- No hint stream. No hint durability model.
- No interaction with snapshot, fork, or evict.
- No tuning for the thresholds — the defaults in `ReclaimThresholds` are placeholders.
- No measurement infrastructure to tell us whether any of this matters on real workloads.

These are open questions, not a plan. They should only be answered if and when evidence says reclamation needs automation.
