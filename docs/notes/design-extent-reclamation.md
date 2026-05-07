# Extent reclamation

**Status:** Primitive, scanner, CLI, and per-tick coordinator scheduling are landed. Threshold tuning is empirical work still pending. Do not cite anything in this doc when solving an unrelated correctness problem.

## What exists today

A volume-side primitive that rewrites bloated multi-LBA extent bodies into compact new-hash entries, and a test hook to drive it.

### Why anything needs rewriting

`Volume::write()` produces one `SegmentEntry` per write, with `lba_length` equal to the inbound size. Later partial overwrites split the LBA map entry via `payload_block_offset` aliasing (`elide-core/src/lbamap.rs::insert`) without touching the original stored body. Reads remain correct — each surviving sub-range resolves through the original compressed payload — but the body may end up with many dead interior blocks that every read still has to decompress past. The alias-merge primitive rewrites those surviving sub-ranges into fresh compact payloads and leaves the original hash fully orphaned, which lets a later GC pass reclaim it.

### The primitive

Three phases, of which only the middle is heavy. See `docs/design-noop-write-skip.md` for why rewrite writes pass cleanly through the no-op skip path.

1. **Prepare** (`Volume::prepare_reclaim`). Under the actor lock, clone `Arc<LbaMap>` + `Arc<ExtentIndex>`, capture the extents over the target LBA range, mint the output segment ULID, and package the directory + signer state into a `ReclaimJob`. Cheap — O(log n) range query plus two Arc bumps plus a mint.

2. **Execute** (`actor::execute_reclaim` on the worker thread). Walks the plan off-actor. For each non-zero-hash extent in the plan, two gates decide whether it's worth rewriting:
   - **Containment.** Every run of the hash in the current lbamap must sit inside the target range. Rewriting a hash whose body is referenced outside the target would leave those outside references pointing at the now-bloated body and make things worse.
   - **Bloat.** At least one run of the hash must have `payload_block_offset != 0`, indicating a prior split that left dead bytes inside the stored body.
   Hashes that pass both gates have their stored body fetched via the captured extent-index snapshot (no actor round-trips), sliced to the live sub-range, re-hashed, and compressed. The output is a list of `SegmentEntry` values — Data/Inline for fresh hashes, DedupRef when the new hash is already indexed — written as a single signed segment in `pending/<segment_ulid>` via `segment::write_and_commit`. Delta-bodied hashes are skipped (see open questions).

3. **Apply** (`Volume::apply_reclaim_result`). Back on the actor, check `Arc::ptr_eq(result.lbamap_snapshot, self.lbamap)`. If the pointers differ, something mutated the lbamap while the worker was running — delete the orphan `pending/<segment_ulid>` and return `ReclaimOutcome { discarded: true, .. }`. Otherwise splice the worker's entries into the live lbamap + extent index. The segment rename was the durability commit point, same pattern `repack` uses. The WAL is not touched: reclaim's output is fully derivable from durable state, so a crash before rename leaves nothing to recover and a crash between rename and apply leaves an orphan segment that GC classifies as all-dead on the next pass.

### Entry points

`VolumeClient::reclaim_alias_merge(start_lba, lba_length)` is the production API: one actor round-trip, the heavy middle runs on the worker, the apply parks the reply until the worker returns.

`Volume::reclaim_alias_merge(start_lba, lba_length)` is the synchronous in-process wrapper (`prepare → execute → apply` on the calling thread), used by tests and offline tooling.

`scan_reclaim_candidates` walks the live lbamap and extent index and produces `ReclaimCandidate { start_lba, lba_length, dead_blocks, live_blocks, stored_bytes }` entries for hashes with detectable bloat (controlled by `ReclaimThresholds`, all defaults placeholder values).

`elide volume reclaim <name>` calls the scanner, then calls the primitive once per candidate. It exists so the primitive can be exercised end-to-end — this is not a customer-facing operation.

### Why bypassing the WAL is safe

WAL records exist to make writes crash-replayable before they reach a segment. Reclaim rewrites are *derivable* from already-durable state: the source hashes are live in existing segments, and the lbamap change is a pure remap onto a fresh body. If the actor crashes before the rename, there's nothing to recover — the old entries are still there, unchanged. If we crash between rename and lbamap splice, the new segment is an orphan that GC will classify as all-dead and sweep. Same property repack relies on.

### Discard window

`Arc::ptr_eq(result.lbamap_snapshot, self.lbamap)` spans (prepare → worker done → apply). Any concurrent mutation voids the plan and the worker's orphan segment is deleted. Repack has the same property and it hasn't been a problem; the fallback is a clean discard, not a retry-in-place, so the next scheduled pass picks up whatever state now exists.

### Space reclamation is deferred to GC

A successful reclaim pass updates the lbamap to point at fresh compact entries and leaves the original hash's body in its source segment as an LBA-dead entry. The body is not deleted at apply time. GC reclaims it later when the containing segment crosses an eligibility threshold (dead-input, ≤ 16 MiB live, or density < 0.70). A dead body in an otherwise-dense segment stays on disk until enough surrounding entries also die — so `runs_rewritten > 0` from `elide volume reclaim` means "scheduled", not "freed on disk".

## Coordinator wiring

Wired in `elide-coordinator/src/tasks.rs` alongside `sweep_pending` / `repack` / `delta_repack_post_snapshot`. Each drain tick calls `control::reclaim(&fork_dir, Some(1))`: the volume runs the scanner, takes the top-scoring candidate, and reclaims it. The cap of 1 per tick bounds per-tick latency; sustained bloat converges across ticks because the scanner sorts most-wasteful-first. Orphaned bodies from the reclaim output become ordinary sparse-segment candidates on the next GC tick. Skipped for readonly volumes and during import-serve phases (control.sock owned by the import process).

The pre-drain placement was picked over a post-snapshot hook because it needs no new event path, is symmetric with existing maintenance ops, and a single per-tick knob controls cadence.

### Concurrency constraint

Like repack and delta_repack, reclaim takes a `parked_reclaim: Option<Sender<...>>` slot on the actor. Concurrent IPC calls return `err concurrent reclaim not allowed`. Callers (coordinator tick loop) serialise naturally.

## Open questions

- **Threshold shape.** `ReclaimThresholds` defaults are placeholders. The scanner-gated trigger needs a single-number "worth firing" signal — likely `top_candidate.dead_blocks × dead_ratio` above a bar, but empirical work on an aged volume is needed before picking it.
- **Proposal cap per pass.** Scanner already sorts by `dead_blocks` desc; a hard cap on proposals kept per pass bounds the worker's CPU and the single-apply lock-hold duration. What cap?
- **Interaction with CANONICAL_ONLY demotion for hashes used as dedup/delta sources.** When the rewritten hash H has DedupRef or Delta entries elsewhere pointing at it, GC partial-death demotes H to `CANONICAL_ONLY` (body preserved, no LBA claim) rather than dropping it. This is a clean separation of concerns — H' serves this volume's LBA reads with no aliasing overhead; H serves remote dedup/delta consumers with the full body they hashed against — not a failure mode. But it does change the cost accounting: when H has outbound dedup/delta refs, reclaim adds `sizeof(H')` of new body on top of the preserved H; when H has none, reclaim is essentially free (H is orphaned and GC drops it entirely). The scanner threshold may want to factor in outbound-ref presence so the "worth firing" bar is stricter for hashes whose old body will survive.

  The signals are already available on `LbaMap`:

  - **Delta sources:** `delta_source_refcount(H)` returns the count of live Delta LBAs whose source is H. Already maintained via `incref`/`decref` on every lbamap mutation; currently marked "diagnostics" but the data is load-bearing (`lba_referenced_hashes()` depends on it). Just expose it as production API.
  - **DedupRef references within this volume:** no separate refcount needed. A DedupRef LBA contributes `X → H` to the lbamap with `entry.hash = H`, so the existing containment gate (every run of H sits inside the target range) already sweeps in any in-volume DedupRef for H. A DedupRef at an LBA outside the range fails containment and reclaim skips H.
  - **Cross-volume DedupRef/Delta:** not tracked by any refcount. Also not relevant to reclaim's cost accounting — reclaim mutates only this volume's lbamap and writes a new segment; other volumes' consumers pin their sources via their own `extent_index` against S3 segments, which reclaim doesn't touch.

  Translation to threshold logic: `delta_source_refcount(H) > 0` ⇒ H will survive demotion, apply a stricter cost-benefit bar (need sufficiently high dead-ratio to offset `sizeof(H')`). Otherwise H is orphaned post-reclaim and dropped next GC, and the looser bar is correct.

  **Output-shape optimisation.** A per-entry decision inside `execute_reclaim` picks the cheapest output for each reclaimed sub-range. Two independent signals prove "H's body will stick around regardless of this reclaim" and let us emit a thin Delta against H instead of a fresh body:

  1. **Snapshot pin.** `H.segment_id <= snapshot_floor_ulid` — H lives in a segment referenced by the current snapshot. Snapshot-referenced segments cannot be rewritten or dropped for the lifetime of the snapshot; the body is permanent. This is the stickier of the two signals — a snapshot typically outlives any individual Delta's LBA lifecycle.

  2. **Delta-source pin.** `delta_source_refcount(H) > 0` — H is already serving as a delta source for at least one other live entry. `lba_referenced_hashes` keeps H alive as long as any such Delta remains on the volume.

  When either holds, the sliced sub-range is emitted as `Delta { source_hash: H }`: zstd-dict-compressing a literal substring of H against H itself resolves to a few-hundred-byte dictionary reference, dramatically smaller than a fresh lz4'd body. When neither holds, H would be orphaned by the reclaim and dropped on the next GC pass — we emit fresh Data so GC can actually reclaim the body (pinning H via our own Delta would trade "drop H" for "keep forever"). A delta blob larger than the raw sub-range falls back to Data regardless. DedupRef still beats both shapes when the new hash is already canonical. The Delta-in branch (when the reclaimed hash is itself a Delta) is unaffected — its source's status is identical pre- and post-reclaim because `lbamap.insert_delta` propagates source refcounts across splits.
- **Reclaiming a Delta-bodied hash.** Distinct from the source-hash case above: when H itself is a `Delta` entry (not DATA), rewriting it naively as DATA would lose the delta saving on this volume. `execute_reclaim` preserves the delta shape: for each bloated Delta run it decompresses the full fragment via the same first-resolvable-option rule the reader uses (`try_read_delta_extent`), slices the live sub-range, and re-compresses against the same source with zstd-dict to produce a fresh thin Delta carrying one option for that source. The per-entry `delta_blob >= sub_range_len` check falls through to "skip this entry" on net-loss deltas, matching `delta_compute::rewrite_pending_with_deltas`. Sources that don't resolve locally (no option's `source_hash` in the extent index, or source body / delta blob missing from all search dirs) cause the entry to be skipped — reclaim never demand-fetches to seed a dictionary and never rehydrates a Delta as DATA.

### Bloat gate: scanner and primitive agree

Both `scan_reclaim_candidates` and `execute_reclaim` now use the same criterion: `live_blocks < logical_blocks`. `live_blocks` is the sum of run lengths. `logical_blocks` is `body_length / 4096` for uncompressed Data (exact) and `max_offset_end` for compressed Data, Inline, and Delta (a lower bound). Catches both middle splits and pure tail overwrites. The earlier narrower primitive gate (`any run with payload_block_offset != 0`) silently rejected tail-overwrite candidates that the scanner flagged; reproducing it end-to-end was `dd bs=128k count=1; dd bs=64k count=1 seek=1` on a mounted ext4 → `elide volume reclaim` reported `0 runs from 1 candidate`. No longer.

### Scanner surfaces Delta-backed bloat

`scan_reclaim_candidates` walks both the Data and Delta tables. A Delta fragment split by a partial overwrite is flagged with `dead_count_is_lower_bound = true` and `stored_bytes = max_offset_end * 4096` (the Delta entry itself carries no `body_length` on disk — the delta blob's size is unrelated to the logical fragment size). The primitive's Delta branch then handles the candidate as described above: preserve-shape re-delta against the same source, skip if no source resolves locally.
