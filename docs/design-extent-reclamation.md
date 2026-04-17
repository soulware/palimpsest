# Design: extent reclamation

**Status:** Proposed, not implemented.

## What this does and doesn't do

The pass described here **does not itself reclaim any storage**. It rewrites selected LBA map entries so that previously-bloated stored payloads become fully unreferenced, at which point the next ordinary coordinator GC pass drops them during segment repack.

That is a two-step reclamation chain:

1. **This pass** — rewrites LBA map entries to point at fresh compact bodies, leaving the old bodies orphaned. Runs volume-side during quiet periods.
2. **Coordinator GC** — already existing; reclaims segments whose live fraction has dropped below the repack threshold. Now has something to actually reclaim that it didn't before.

"Reclamation" is a slight abuse of terminology — this pass is really a *map-and-body rewrite* that enables reclamation to happen later. The distinction matters because the pass neither frees space on its own nor blocks on GC; it produces new dead weight in pre-floor segments and relies on the normal GC scheduler to act on it. Lock hold time, write amplification, and convergence are local to this pass; physical storage recovery is not.

## Problem

The write path never coalesces. Every `Volume::write()` produces exactly one `SegmentEntry` whose `lba_length` equals the inbound write size; flush, sweep, GC, delta repack, and snapshot promote all preserve extent boundaries (`elide-core/src/volume.rs:539`, `elide-coordinator/src/gc.rs:1081`). For an imported volume the first segments cleanly mirror file layout. Subsequent NBD traffic is the opposite: 4 KiB writes punch single-block extents into the LBA map. Because nothing recoalesces, fragmentation is monotonically non-decreasing.

Fragmentation has two distinct cost metrics:

- **LBA-range fragmentation.** Across a given LBA range, how many distinct map entries cover it. High fragmentation means per-LBA lookup overhead, loss of readahead coalescing, loss of contiguity for delta repack.
- **Body live-fraction.** Of the N blocks in a stored payload, how many are still referenced by any LBA map entry. Low live-fraction means wasted storage inside segments and decompress-to-discard waste on every read that touches the entry.

The two are correlated but distinct. A bloated 100-block body with 1 live block implies its original range was overwritten by ~99 other entries (high range fragmentation). But a range can be fragmented without any body becoming bloated (many small writes, each producing a compact entry). They suggest different remedies.

## What free-aliasing already buys

`LbaMap::insert` splits overlapping entries and uses `payload_block_offset` to alias the tail of a split into the middle of the original payload without rewriting it (`elide-core/src/lbamap.rs:100-170`). Light fragmentation is therefore essentially free:

Write `[100, 200)` as one 100-block entry H_A. Later overwrite `[150, 160)` with a 10-block entry H_B. The map splits into:

```
[100, 150) → H_A/offset=0    (50 blocks)
[150, 160) → H_B/offset=0    (10 blocks)
[160, 200) → H_A/offset=50   (40 blocks)
```

H_A's body is never touched. Reads of the surviving sub-ranges resolve through the original compressed payload, paying one decompression per request that serves 90 live blocks out of 100. Storage waste is 10%. The cost of rewriting exceeds the cost of the waste.

The pathological case is the opposite: a 100-block body with 1 live block. Every read pays full-frame decompression to discard 99 blocks, and 99% of the body is dead storage waste. The aliasing trick buys time but does not avoid the rewrite forever — it defers it to when cost-benefit crosses over.

Coordinator GC at segment level handles *whole-entry* death: an entry with zero live blocks is dropped during repack (`elide-coordinator/src/gc.rs:782`, `gc.rs:1098`). An entry with at least one live block is copied through in full, dead interior bytes and all. A single bloated entry inside an otherwise-healthy segment never trips segment-level repack. Something else has to produce "fully dead" entries for GC to act on. That something is this pass.

## Two primitives

Reclamation splits into two operations with different safety contracts and different knowledge requirements. They share the execution pipeline but target different situations.

### 1. Alias-merge (universal)

Rewrites aliased runs of a single content hash within a target LBA range as compact standalone entries. Preserves content boundaries.

Given the example map state above, alias-merge walks the range, groups by hash, and rewrites each hash's runs:

```
[100, 150) → H_A_prefix_new/offset=0   (50-block fresh body)
[150, 160) → H_B/offset=0              (unchanged — already optimal)
[160, 200) → H_A_suffix_new/offset=0   (40-block fresh body)
```

Same entry count, but each body is compact (no interior dead weight), every map entry has `payload_block_offset=0`, and H_A's bloated body is now fully orphaned — on the next segment GC pass it becomes dead weight and eventually its containing segment crosses the repack threshold.

**Safety.** Alias-merge never crosses content boundaries. Every reader sees identical bytes at identical LBAs, every DedupRef against H_B still resolves correctly, observable volume state is unchanged. Safe on any volume regardless of filesystem awareness.

**Knowledge required.** Just LBA coordinates. Any nominator — coordinator, CLI, test harness — can emit alias-merge hints.

### 2. Cross-content consolidation (file-aware)

Rewrites an entire LBA range as one contiguous entry, merging *across* content-hash boundaries:

```
[100, 200) → H_file/offset=0   (100-block fresh body covering the whole range)
```

H_A and H_B both become orphaned.

**Safety argument.** Consolidation is only valid when the caller can assert the range is one semantic unit — a single file — because:

- **Dedup is lost at the old granularity.** If H_B was shared (zero page, library blob, ELF padding), the new aggregate hash H_file won't dedup with anything that previously referenced H_B. Gains contiguity, loses potential sharing. For a file of unique content, net win; for a file of shared blocks, net loss.
- **Future partial writes go back to square one.** A later guest write to any block in the range splits the merged entry. Consolidation is a bet that the range is read-mostly.
- **Lifetimes become coupled.** Two LBAs can be physically adjacent but belong to unrelated content. Merging across the boundary couples their eviction, delta-repack, and dedup decisions. Inside a file this is fine; across a file boundary it is wrong.

**Knowledge required.** File identity. Today only the snapshot-time filemap walker has this — it parses ext4 metadata from frozen segments and records per-file fragment lists (`elide-coordinator/src/inbound.rs`, `elide-core/src/import.rs`). Consolidation can only live where the filemap lives.

### Layering

Alias-merge is the default and always safe. Consolidation is an optional stronger layer that requires filemap knowledge. Raw block volumes use only alias-merge and degrade gracefully. Filesystem-aware volumes use both — alias-merge for anything the coordinator detects from per-entry liveness, consolidation for same-file ranges the filemap walker identifies as fragmented.

## Execution: volume-side internal write

Both primitives execute the same way: read live bytes through the volume's normal read path, hash the materialised content, append new entries via the regular write pipeline, update the LBA map. The heavy work (fetch, decompress, hash, compress) runs **outside** the single-writer lock; the lock is taken only for a short snapshot at the start and a short commit-with-precondition at the end.

**This must be a volume operation, not a coordinator/GC operation.** Coordinator GC today preserves the hash-content relationship and only redirects existing hashes to new physical locations via the handoff protocol (`elide-coordinator/src/gc.rs:1206`). This pass has to *retire* old hashes, compute *new* hashes, append new WAL entries, and rewrite LBA map entries — all of which require:

- **The single-writer lock (briefly).** Only the volume mutates the LBA map and extent index. The commit step takes the lock; the heavy work does not.
- **The volume's read path.** Reading live sub-ranges of a bloated entry needs `payload_block_offset`-aware resolution (`read_extents`), which the coordinator's whole-body `fetch_live_bodies` does not know about.
- **The WAL → pending → promote pipeline.** New entries must crash-recover the same way NBD writes do.
- **Dedup participation.** The new hash might already exist in the extent index, in which case the pass should emit a `DedupRef` instead of a fresh `Data` entry. That is a write-path decision only the volume makes.

### Optimistic commit structure

The natural-looking implementation would hold the lock for the whole operation — snapshot, read, decompress, hash, compress, commit — and bound work per lock acquisition by a per-batch byte budget. That works, but it pessimises lock contention for no real benefit, because the heavy work **is itself discardable**: if a concurrent NBD write invalidates the target range mid-batch, there is no staged state to roll back (nothing has been written to the WAL, extent index, or LBA map) and the cost of aborting is just the CPU already spent.

That observation unlocks a two-phase structure with two short critical sections bracketing the heavy work:

```text
# Phase 1: capture snapshot (short critical section)
lock()
    snapshot = lbamap.entries_in_range(target)    # (lba, len, hash, offset) tuples
    locations = extent_index.resolve(snapshot)    # body positions per hash
unlock()

# Phase 2: heavy work (outside lock — ms-scale)
bytes = fetch_bodies(locations)
decompressed = decompress(bytes)
live_bytes = slice(decompressed, live_sub_ranges)
new_hash = blake3::hash(live_bytes)
compressed = lz4::compress(live_bytes)

# Phase 3: commit with precondition check (short critical section)
lock()
    current = lbamap.entries_in_range(target)
    if current != snapshot {
        unlock()
        return  # discard — no rollback needed, nothing was committed
    }
    wal.append(new_entry(new_hash, compressed))
    extent_index.insert(new_hash, location)
    lbamap.insert(target_start, target_len, new_hash)
unlock()
```

Total lock hold per hint drops from the duration of the full batch (several ms) to two short map lookups plus the WAL/map commit (tens of µs). That is roughly two orders of magnitude less contention against the NBD write path.

**Precondition check.** "Does `lbamap.entries_in_range(target)` produce the same sequence of `(lba, length, hash, payload_block_offset)` tuples as phase 1?" Purely local to the target range; cost is O(entries in range), which for realistic hints is a handful of comparisons. A concurrent NBD write to an unrelated range doesn't invalidate anything. A concurrent coordinator GC handoff that relocates a hash's body to a new physical location doesn't invalidate anything either — the handoff is a pure physical redirect; the map still lists the same hash with the same offset, which is all phase 3 compares. Only a mutation to the target range itself (exactly the concurrent client write we need to defer to) causes a discard.

**Discard is free-ish.** The wasted work on a discard is one batch's worth of read + decompress + hash + compress — a few ms of CPU and some allocations, no I/O, no state to undo. At a reasonable batch budget and the expected discard rate (well under 10% during quiet periods, higher if the drain loop is running against active workload) the amortised cost is marginal. No retry logic is needed: if the hint is still valid, the coordinator will re-emit it on its next scan, and the next quiet window will pick it up.

**Why no generation counter.** A conventional optimistic-concurrency scheme needs a version identifier so the commit step can detect "something changed." Here the LBA map entries themselves serve that role — each entry is an immutable tuple of `(lba, length, hash, payload_block_offset)`, and "the entries are the same sequence" is a stronger invariant than any counter would give us. It also means the check can be done without touching any non-map data structures.

### Bytes do not cross the coordinator/volume boundary

A subtle but important property: messages from coordinator to volume carry *no bytes* — only LBA coordinates. The volume assembles the data from its own local state at execution time.

This dodges the concurrency window problem entirely at the coordinator/volume level. The coordinator's scan can be arbitrarily stale by the time the volume processes the hint, and correctness still holds, because the volume always reads state under its own lock (phase 1) and verifies it has not changed before committing (phase 3). An NBD write arriving between hint generation and execution just changes what the volume sees in phase 1; the resulting entry reflects current observable content. Stale hints don't become wrong — they become unnecessary, and the noop-skip path handles that cleanly.

### Interaction with the no-op write skip path

These writes carry bytes that by construction equal the current observable content at the target LBAs — that is precisely why they are representation changes, not content changes. The noop skip path (`design-noop-write-skip.md`) interacts cleanly:

The skip fires only when the LBA map already binds the incoming hash directly to the target LBAs with `payload_block_offset=0`. For a reclamation pass that is the *post-rewrite* steady state, not the pre-rewrite one. The skip never fires on a first pass but reliably fires on subsequent passes, **giving idempotent convergence for free** without any termination tracking.

Rewrite writes flow through `Volume::write_with_hash`, which is the same entry point used by reclamation to reuse a precomputed phase-2 hash. It runs the same hash-based skip check as `Volume::write`; nothing about the skip needs to be bypassed. The pipeline downstream of the check (compression, dedup lookup, WAL append, LBA map update) is shared between client and reclamation writes.

(An earlier two-tier design added a body byte-compare tier that would have classified every reclamation rewrite as a no-op and silently dropped it, forcing an `internal-origin` API split. Tier 2 has been removed; that complication is gone with it. See `design-noop-write-skip.md § Why no byte-compare tier`.)

### Comparison: lsvd concurrency model

LSVD's GC (`refs/lsvd/gc.go:67-183`) runs in its own goroutine and does the equivalent heavy work — fetch extents, filter live bytes via `SubRange`, emit a new GC segment — without coordinating with the write path at all. The result lands atomically via an extent-map pointer swap; client writes in flight during GC do not wait for GC and GC does not wait for them.

Elide with the optimistic commit structure reaches the same end property — heavy reclamation work does not block client I/O — through a different mechanism. LSVD can rewrite extents freely because its identity is physical: nothing else in the system references the specific byte layout of an old extent, so the extent map is the sole source of truth and updating it is a single atomic swap. Elide's identity is content-derived (BLAKE3 hashes, dedup references, delta sources), so an in-flight rewrite has to verify that the target range still represents the same content at commit time. Phase 1 + phase 3 is how we do that verification without holding the lock during the heavy work.

Neither system is strictly lock-free in the theoretical sense — both have short critical sections for the map mutation itself. The practical property they share is that lock hold is proportional to the *commit* step, not the *work* step. That is the property that matters for coexistence with client writes.

The mechanism difference is not incidental; it is a direct consequence of the identity model. It is what it costs us to keep hash-as-identity while still achieving LSVD-level write-path non-interference.

## Hints: advisory, not durable

The pass is driven by **advisory hints** carrying LBA coordinates and a hint variant:

```rust
enum ReclaimHint {
    /// Safe universal: rewrite aliased runs within this range as compact
    /// standalone entries. Preserves content boundaries.
    MergeAliases { start_lba: u64, lba_length: u32 },
    /// File-aware: rewrite this range as one contiguous entry, even
    /// across content-hash boundaries. Caller asserts the range is
    /// one semantic unit.
    ConsolidateFile { start_lba: u64, lba_length: u32 },
}
```

**Hints are cache, not state.** Executing a hint is idempotent (the noop-skip hash check makes a re-run a no-op), and dropping a hint is a no-op (worst case: the volume stays sub-optimally fragmented, same as the baseline). Therefore:

- No fsync discipline around hints.
- No WAL for them.
- No crash-recovery semantics.
- If the hint file is lost, the coordinator re-scans and re-emits on its next pass.
- If a hint is processed twice, the noop-skip catches the second attempt.

This is a qualitatively different contract from anything else the coordinator tells the volume: GC handoffs (`gc/<ulid>.staged` → bare → finalised) are durability-critical state. Hints sit in a new, looser bucket.

**Hint sources.**

| Source | Emits | Knowledge |
|---|---|---|
| Coordinator per-segment liveness scan | `MergeAliases` | LBA map + per-entry live-fraction (computed during GC scan) |
| Snapshot-time filemap walker | `MergeAliases`, `ConsolidateFile` | File identity, per-file fragment list |
| CLI diagnostic | either | Explicit user-supplied coordinates |
| Proptest / simulation model | either | `SimOp::ReclaimRange { ... }` variant |

The coordinator is the primary nominator but not the only one. The pass is a volume primitive exposed on the IPC surface; the coordinator is one of several callers.

## Scheduling: quiet-period drain loop

The pass is **not invoked at a fixed time** — it is a **drain loop** over the hint stream, running at the volume's own pace. With the optimistic commit structure above, lock contention is no longer the reason to defer it to quiet periods — each hint only touches the lock for tens of µs. The reason to gate it on quietness is different: **minimising wasted CPU on discards**.

During a quiet window the probability that phase 3's precondition check fails is near zero, and every hint makes forward progress. During active client workload the precondition check may fail often enough that most batches burn CPU for nothing. Quiet-period scheduling is therefore about *amortised efficiency*, not about protecting NBD latency — the latency protection is already structural, built into the commit shape.

**Quietness signals** the volume observes locally without coordinating with anyone:

- No recent client NBD writes.
- WAL buffer is drained.
- `pending/` has been promoted; no post-floor writes awaiting seal.
- No snapshot operation in progress.
- No inbound control IPC pending.

None of these are expensive to check. A single quiet tick at some cadence gates the drain.

**Back-pressure is structural.** Between hints the drain loop rechecks quietness. An NBD write arriving during a quiet window just takes the lock first — phase 1 of the next hint will see the mutation and either discard or proceed based on whether it touched the target range. The drain loop notices on its next tick that the volume is no longer quiet and sleeps. No priority scheduling, no lock-ordering gymnastics — execute one hint, check, repeat until not quiet or hints exhausted.

**Per-batch byte budget.** One hint can nominate a very large range. The budget caps how much work a single batch does (e.g. 1 MiB) — not because the lock needs it (the lock is held only briefly at phase 1 and phase 3), but because a large batch has a bigger discard cost if phase 3 fails. Smaller batches fail cheaper. Large hints turn into multi-batch operations, each with an independent snapshot/commit cycle.

**Hint aging.** Stale hints waste scheduler capacity. A simple age bound ("drop hints older than N ticks") plus coordinator re-emission on its next scan gives self-healing without any explicit invalidation protocol.

**Snapshot-time as a special case.** Filemap-generated hints are produced under the snapshot lock as a side effect of filemap walking. Snapshot sealing *optionally* processes some inline, bounded by a tight byte budget, and defers the rest to the post-snapshot quiet drain. The snapshot seals as soon as its mandatory work is done; this pass is always best-effort.

## Selection heuristics

**Alias-merge candidate predicate** (coordinator liveness scan):

- `entry_dead_blocks >= MIN_DEAD_BLOCKS` (e.g. 8) — small waste isn't worth touching.
- `entry_dead_ratio >= MIN_DEAD_RATIO` (e.g. 0.3) — `payload_block_offset` aliasing is cheap below this.
- `entry_bytes >= MIN_ENTRY_BYTES` (e.g. 64 KiB) — tiny entries don't benefit from compaction.

**Cross-content consolidation candidate predicate** (filemap walker):

- `fragment_count >= MIN_FRAGMENTS` (e.g. 8).
- `total_bytes / fragment_count <= MAX_AVG_FRAGMENT_BYTES` (e.g. 16 KiB).
- `total_bytes >= MIN_FILE_BYTES` (e.g. 64 KiB).
- `dedup_ref_ratio < MAX_DEDUP_RATIO` (e.g. 0.25) — files dominated by DedupRefs would lose more than they gain.
- `delta_entry_count == 0` (or partial-consolidation supported) — merging cannot cross encoding-kind boundaries.

All thresholds are placeholders pending measurement.

## Tradeoffs

**Dedup conflict** (consolidation only). Consolidation creates a new aggregate hash that doesn't dedup with whatever previously referenced the original per-fragment hashes. Alias-merge is immune — it preserves content boundaries and therefore preserves dedup.

A possible refinement for consolidation: skip merging through DedupRef runs, producing two merged extents bracketing the dedup'd region. Adds complexity but preserves dedup wins where they exist.

**Delta extent conflict** (both). Phase 5 converts post-floor `Data` entries to thin `Delta` entries with a different encoding. Consequences:

- This pass must run *before* Phase 5 in any tick where both fire.
- This pass cannot touch entries already converted to Delta. Either skip or accept partial merge around delta regions.

There is also a synergy: merged entries are exactly the granularity Phase 5 Tier 2 wants as delta targets. Tier 1 (same-LBA prior fragment) returns one fragment for the whole merged range instead of N independent lookups. The two passes reinforce each other when sequenced correctly.

**Pre-floor fragment liveness** (both). The pass produces dead bytes inside pre-floor immutable segments. Those cannot be rewritten until ordinary GC visits them, and the leaf-only constraint means ancestors with active descendants can never be reclaimed. For fork-heavy workloads, the pass adds permanent dead weight to ancestors. Either accept this (fork lifetimes are typically bounded) or restrict the pass to leaves with no live descendants.

**Write amplification** (both). Every pass rewrites bytes that were already on disk. The amplification budget needs to be set against the read-amplification savings — for read-heavy workloads it pays back quickly; for write-heavy workloads it might not.

**Discard cost** (both). Under the optimistic commit structure, any batch whose target range is mutated by a concurrent client write discards its staged bytes and returns without committing. The wasted work is one batch's worth of fetch + decompress + hash + compress — a few ms of CPU and some allocations, no I/O, no rollback. Quiet-period scheduling keeps the discard rate low in expectation, and the per-batch byte budget bounds the worst-case waste per discard. At expected rates this is well under 1% of volume CPU.

**Lock contention** (both). Lock hold per hint is tens of µs (two short map lookups plus the commit), not the duration of a full batch. NBD writes racing against an in-flight reclamation operation wait at most that long before proceeding; any mutation that wins the race causes phase 3 to discard cleanly. This is explicit in the commit structure and does not need to be mitigated further.

**Delayed physical reclamation** (both). The whole point of the "What this does and doesn't do" section at the top. The pass enables future GC to reclaim; it does not itself free any bytes. If the coordinator's GC scheduler is slow or if the target segment is an ancestor with live descendants, the dead weight persists indefinitely. The pass is worth it only when the downstream GC path is expected to act.

## Measurement before mechanism

Neither cost metric is observable today. Before implementing any pass, the volume needs to expose:

- **Per-entry live-fraction** — derivable from an LBA map traversal; could live in a new `elide volume inspect --entry-liveness` subcommand.
- **Per-file fragment count** — derivable from the filemap; could live in `elide volume inspect --fragmentation` or `inspect-filemap`.

Running both against real aged volumes would show whether either metric is actually dominant on representative workloads before we pay the implementation cost. This is a prerequisite to choosing thresholds and to deciding whether the pass is worth implementing at all.

## Open questions

- **Threshold defaults.** Need empirical data on real fragmented volumes.
- **Snapshot-time inline budget.** How much (if any) work should happen synchronously under the snapshot lock vs deferred to the post-snapshot quiet drain? Probably tunable, probably small by default.
- **DedupRef-aware consolidation.** Is the complexity of bracketing DedupRef runs during consolidation justified, or is "skip files above a dedup threshold" sufficient?
- **Hint persistence.** Do hints survive volume restart? The advisory-cache framing suggests no — the coordinator re-scans and re-emits. But losing the entire queue on restart for a rarely-quiet volume could prevent convergence. Worth exercising on a long-running workload.
- **Filemap regeneration.** Consolidation under the snapshot lock must either rewrite filemap rows in place after merging or re-run filemap generation. In-place is cheaper but couples the pass to filemap format internals.
- **Coordinator detection cost.** Computing per-entry live-fraction across all segments is O(N) in extent count per GC scan. Worth measuring on a large volume before committing to it as a standing part of the scan.
- **Interaction with ancestor GC.** The pass creates dead weight in pre-floor segments that may belong to ancestors. If those ancestors are pinned by active descendants, the dead weight is permanent. Should the coordinator refuse to emit hints targeting entries inside pinned ancestors, or accept the leak as a cost of fork lifetimes?

## Relationship to existing passes

| Pass | Operates on | Granularity | Crosses content boundary? | Writer |
|---|---|---|---|---|
| `sweep_pending` | `pending/` segments | extent | no | volume |
| Coordinator GC repack | uploaded segments | extent | no | coordinator |
| Phase 5 delta repack | post-floor `pending/` | entry kind | no | volume |
| Snapshot promote | source segment | segment | no | volume |
| **Alias-merge (proposed)** | **any LBA range** | **hash run within range** | **no** | **volume** |
| **Cross-content consolidation (proposed)** | **file LBA range** | **file** | **yes (within file only)** | **volume** |

This pass is the first in the system that rewrites LBA map entries to point at new hashes rather than redirecting existing hashes to new physical locations. That is why it has to be a volume operation: the coordinator's handoff protocol is deliberately narrow and cannot express a hash-graph mutation. It is also the first pass that uses *file identity* (when available) as a structuring principle for extent layout, which is why the file-aware variant can only live where the filemap lives.
