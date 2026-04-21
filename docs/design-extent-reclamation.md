# Extent reclamation

**Status:** Work in progress. The code described below is a proof-of-concept, not a finished feature. Do not cite anything in this doc when solving an unrelated correctness problem.

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

`VolumeHandle::reclaim_alias_merge(start_lba, lba_length)` is the production API: one actor round-trip, the heavy middle runs on the worker, the apply parks the reply until the worker returns.

`Volume::reclaim_alias_merge(start_lba, lba_length)` is the synchronous in-process wrapper (`prepare → execute → apply` on the calling thread), used by tests and offline tooling.

`scan_reclaim_candidates` walks the live lbamap and extent index and produces `ReclaimCandidate { start_lba, lba_length, dead_blocks, live_blocks, stored_bytes }` entries for hashes with detectable bloat (controlled by `ReclaimThresholds`, all defaults placeholder values).

`elide volume reclaim <name>` calls the scanner, then calls the primitive once per candidate. It exists so the primitive can be exercised end-to-end — this is not a customer-facing operation.

### Why bypassing the WAL is safe

WAL records exist to make writes crash-replayable before they reach a segment. Reclaim rewrites are *derivable* from already-durable state: the source hashes are live in existing segments, and the lbamap change is a pure remap onto a fresh body. If the actor crashes before the rename, there's nothing to recover — the old entries are still there, unchanged. If we crash between rename and lbamap splice, the new segment is an orphan that GC will classify as all-dead and sweep. Same property repack relies on.

### Discard window

`Arc::ptr_eq(result.lbamap_snapshot, self.lbamap)` spans (prepare → worker done → apply). Any concurrent mutation voids the plan and the worker's orphan segment is deleted. Repack has the same property and it hasn't been a problem; the fallback is a clean discard, not a retry-in-place, so the next scheduled pass picks up whatever state now exists.

### Space reclamation is deferred to GC

A successful reclaim pass updates the lbamap to point at fresh compact entries and leaves the original hash's body in its source segment as an LBA-dead entry. The body is not deleted at apply time. GC reclaims it later when the containing segment crosses an eligibility threshold (dead-input, ≤ 16 MiB live, or density < 0.70). A dead body in an otherwise-dense segment stays on disk until enough surrounding entries also die — so `runs_rewritten > 0` from `elide volume reclaim` means "scheduled", not "freed on disk".

## Coordinator wiring (open)

Two options once the primitive is worker-offloaded:

1. **Pre-drain, scanner-gated** — in `tasks.rs`, alongside `sweep_pending` / `repack` / `delta_repack_post_snapshot`. Each tick: call a cheap `control::reclaim_scan` IPC that returns the top candidate's score; if it clears a configured bar, call `control::reclaim`. Symmetric with how `repack` is wired. Orphaned bodies from the reclaim output become ordinary sparse-segment candidates on the next GC tick.

2. **Post-snapshot** — trigger once after `sign_snapshot_manifest`. Rationale: bloat accumulates across a snapshot's lifetime, and the snapshot floor is what lets GC actually reclaim the resulting orphans. Sharper knife; requires an explicit event hook.

Lean is (1) as the first wiring: no new event path, symmetric with existing maintenance ops, and a single per-tick knob controls cadence.

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
- **Reclaiming a Delta-bodied hash.** Distinct from the source-hash case above: if H itself is a `Delta` entry (not DATA), rewriting it would re-materialise it as DATA and lose the delta saving on this volume. `execute_reclaim` currently skips Delta-bodied hashes via `read_reclaim_extent_body` returning `Ok(None)` on `lookup_delta` hits — pragmatic for a first wiring, but if Delta bloat ever turns out to be significant on real aged volumes the rewrite path needs to preserve the delta shape rather than skip.

- **Scanner and primitive disagree on "bloat".** `scan_reclaim_candidates` uses the general criterion `live_blocks < body_length` (i.e., "the hash has dead bytes somewhere in its stored body"). `execute_reclaim`'s gate is narrower: it requires at least one run with `payload_block_offset != 0`, which only fires for **middle-overwrite** splits. A **tail-overwrite** leaves the surviving run(s) with `payload_block_offset = 0` even though the body has a dead tail — the scanner flags these, the primitive silently rejects them, and the caller sees "0 runs rewritten from N candidates". Confirmed end-to-end via `dd bs=128k count=1; dd bs=64k count=1 seek=1` on a mounted ext4 → `elide volume reclaim` reports `0 runs from 1 candidate`.

  Reclaim *can* fix tail-overwrite bloat — rewriting the surviving run with a new compact hash orphans the old body just as effectively as the middle-split case. The primitive's gate is overly narrow: it should match the scanner's criterion (`live_blocks < body_length` against the captured `extent_index` snapshot). Fix lives in `execute_reclaim`: replace `runs.iter().any(|(_, _, offset)| *offset != 0)` with a check that folds `sum(run.length)` and compares to the `extent_index.lookup(h).body_length`-derived logical length (same shape `scan_reclaim_candidates` already computes).
