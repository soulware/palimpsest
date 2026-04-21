// Coordinator-side S3 segment GC.
//
// Single per-tick selection that bin-packs eligible segments into one output
// plan (≤ SWEEP_LIVE_CAP = 32 MiB live bytes rewritten per tick). Replaces
// the former dead/repack/sweep three-pass split with one rule:
//
//   Eligibility:
//     * dead input — live_lba_bytes == 0, includable at zero rewrite cost
//       (contributes to the output's `inputs` list, not its body).
//     * small     — live_lba_bytes ≤ SWEEP_SMALL_THRESHOLD (16 MiB), any
//       density. Consolidation candidate.
//     * sparse    — density < density_threshold, any size. Reclamation
//       candidate.
//   Snapshot-floor segments are ineligible.
//
//   Packing:
//     1. All tombstone-like inputs fold in for free.
//     2. Smalls sort ascending by live_lba_bytes and greedy-fill the 32 MiB
//        live budget.
//     3. One filler may top up the remaining headroom — chosen from the
//        sparse-large set (live_lba_bytes > 16 MiB AND density < threshold).
//        Among candidates that fit, prefer lowest density (maximum dead-
//        byte reclamation); ties broken by largest live bytes (best fit).
//
// Produces one output segment per tick under `u_gc`, or nothing if no
// eligible input exists. Per-tick work is bounded by the 32 MiB live cap
// plus O(1)-per-input tombstone bookkeeping on the apply side.
//
// Handoff protocol (plan-based, crash-safe, filesystem-only coordination —
// see docs/design-gc-plan-handoff.md for the full design):
//
//   1. Coordinator writes a plaintext plan to gc/<new-ulid>.plan via
//      tmp+rename. The plan lists the transformation for each input's
//      entries (Keep / Canonical / ZeroSplit / Run / Drop).
//
//   2. Volume (idle tick) reads the plan, resolves bodies via its own
//      BlockReader, assembles the output segment signed with the volume's
//      key, writes gc/<new-ulid>.tmp, renames .tmp → bare gc/<new-ulid>
//      (the atomic commit point), removes the plan.
//
//   3. Coordinator (next GC tick) sees the bare file: uploads it to S3, sends
//      promote_segment IPC (volume writes index/<new>.idx + cache/<new>.body
//      and deletes index/<input>.idx for each input), deletes old S3 objects,
//      sends finalize_gc_handoff IPC (volume deletes the bare body).
//
//   All-dead and removal-only handoffs collapse into a zero-entry GC output
//   with a non-empty inputs list. promote_segment recognises this shape and
//   skips writing index/<new>.idx / cache/<new>.body — the bare file is then
//   deleted via finalize_gc_handoff the same as a live output.
//
// A pass is deferred if any .plan or bare gc/<ulid> files already exist
// (at most one outstanding GC result per fork at a time).
//
// Blocking IO note: index rebuild and segment reads are synchronous. For the
// first pass these are called on the async task thread; move to spawn_blocking
// if GC passes become long enough to stall other coordinator tasks.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, warn};

use anyhow::{Context, Result};
use object_store::ObjectStore;
use ulid::Ulid;

use elide_core::extentindex::{self, ExtentIndex};
use elide_core::gc_plan::{GcPlan, PlanOutput};
use elide_core::lbamap::{self, LbaMap};
use elide_core::segment::{self, EntryKind, SegmentEntry};
use elide_core::volume::{ZERO_HASH, latest_snapshot};

use crate::config::GcConfig;
use crate::upload::segment_key;

/// Maximum total live bytes included in one small-segment sweep pass.
/// Matches the volume's FLUSH_THRESHOLD to keep output segment size bounded.
///
/// `compact_segments` writes a single output segment with no internal
/// splitting, so this cap is the only bound on output size.
const SWEEP_LIVE_CAP: u64 = 32 * 1024 * 1024;

/// Live-bytes threshold at or below which a segment is treated as a "small"
/// sweep candidate. Set at half of [`SWEEP_LIVE_CAP`] so two smalls always
/// combine to fit, and the merged output exits the small set permanently —
/// no infinite re-pack loop.
const SWEEP_SMALL_THRESHOLD: u64 = SWEEP_LIVE_CAP / 2;

/// Entry-count cap on the merged GC output. Mirrors the WAL's
/// `FLUSH_ENTRY_THRESHOLD` (volume.rs) so GC outputs sit at the same scale
/// as freshly-flushed segments — packing stops when either this cap or
/// [`SWEEP_LIVE_CAP`] would be exceeded. Bounds the index region for
/// dedup-heavy workloads where many thin entries would otherwise pack
/// without advancing the byte budget.
const SWEEP_ENTRY_CAP: usize = 8192;

/// Which GC strategy was executed.
#[derive(Debug, PartialEq)]
pub enum GcStrategy {
    /// No work done this pass; see [`NoneReason`] for why.
    None(NoneReason),
    /// Compacted one or more eligible segments into a single output.
    Compact,
}

/// Why a pass returned without compacting any segment.
///
/// Distinguishes the three early-exit paths so the coordinator can log
/// them differently: `NoIndex` / `PendingHandoffs` are transient bail-outs
/// where `total_segments` is not a real count, while `NoCandidates` is
/// the genuine "nothing eligible this tick" idle.
#[derive(Debug, PartialEq)]
pub enum NoneReason {
    /// `index/` dir does not exist yet (volume has no segments).
    NoIndex,
    /// A prior pass's `.plan` or bare handoff has not cleared yet;
    /// `gc_fork` bails before `collect_stats` runs.
    PendingHandoffs,
    /// Selection ran; no segment met the eligibility rule.
    /// `total_segments` is the real count here.
    NoCandidates,
}

/// Results from one GC pass.
pub struct GcStats {
    pub strategy: GcStrategy,
    /// Number of input segments compacted.
    pub candidates: usize,
    /// Estimated bytes freed (dead bytes removed from old segments).
    pub bytes_freed: u64,
    /// Total segments considered during this pass (above the snapshot floor).
    pub total_segments: usize,
    /// Number of fully-dead segments cleaned up in the pre-pass.
    pub dead_cleaned: usize,
    /// Number of segments held back because they have at least one
    /// partially-LBA-dead body-bearing entry. See
    /// `docs/design-gc-overlap-correctness.md`.
    pub deferred: usize,
}

impl GcStats {
    fn none(reason: NoneReason, total_segments: usize) -> Self {
        Self {
            strategy: GcStrategy::None(reason),
            candidates: 0,
            bytes_freed: 0,
            dead_cleaned: 0,
            deferred: 0,
            total_segments,
        }
    }
}

/// Run one GC pass for a single fork.
///
/// Selects up to one bucket of eligible segments (dead | small | sparse)
/// via the rules documented at the top of this module, and emits a single
/// plan under `u_gc`. Returns `GcStrategy::None(reason)` if nothing was
/// eligible; the reason distinguishes genuine idle (`NoCandidates`) from
/// transient bail-outs (`NoIndex`, `PendingHandoffs`).
///
/// `u_gc` must be pre-resolved via `gc_checkpoint` IPC before calling this
/// function — it originates from the volume process so that ULID ordering
/// is consistent with the volume's write clock.
///
/// `by_id_dir` is the data-dir root that houses `<volume_ulid>/` fork
/// directories (plus an optional `readonly/` sibling tree). Passed so that
/// `walk_ancestors` can resolve the fork chain — the liveness rebuild must
/// include ancestor layers, otherwise hashes live only via an ancestor LBA
/// look dead to the coordinator and trip stale-liveness on apply.
pub fn gc_fork(
    fork_dir: &Path,
    by_id_dir: &Path,
    config: &GcConfig,
    u_gc: Ulid,
) -> Result<GcStats> {
    let index_dir = fork_dir.join("index");
    if !index_dir.exists() {
        return Ok(GcStats::none(NoneReason::NoIndex, 0));
    }

    let gc_dir = fork_dir.join("gc");
    if has_pending_results(&gc_dir)? {
        return Ok(GcStats::none(NoneReason::PendingHandoffs, 0));
    }

    // Clean up any stale .fetch files left by a coordinator crash mid-compaction.
    // .fetch files are transient S3 downloads; the source is always in S3, so
    // deleting them unconditionally is safe.
    cleanup_fetch_files(&gc_dir);

    // Clean up stale `<ulid>.staged.tmp` scratch from a prior crashed
    // compaction. Safe to remove unconditionally: the current pass mints
    // a fresh ULID via `gc_checkpoint`, so no in-flight write targets
    // any existing stale scratch.
    cleanup_staging_files(&gc_dir);

    // Pending segments created by WAL auto-flush during drain are safe to
    // ignore here: collect_stats (below) only considers index/<ulid>.idx files,
    // so un-promoted segments (no .idx yet) are never GC candidates.
    // rebuild_segments includes pending/ with highest priority, so the LBA map
    // correctly reflects those writes and their LBAs are not included in
    // older-segment candidates.

    let vk =
        elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
            .context("loading volume verifying key")?;

    // Walk the ancestor chain so the liveness view matches what the volume
    // uses at apply time. Without this, a hash that lives only via an
    // ancestor-mapped LBA would look dead from the coordinator, get omitted
    // from the plan, and trip the volume's stale-liveness check on apply.
    // Mirrors `volume::open_read_state`.
    let ancestor_layers = elide_core::volume::walk_ancestors(fork_dir, by_id_dir)
        .context("walking fork ancestor chain")?;
    let rebuild_chain: Vec<(std::path::PathBuf, Option<String>)> = ancestor_layers
        .iter()
        .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
        .chain(std::iter::once((fork_dir.to_path_buf(), None)))
        .collect();
    let index = extentindex::rebuild(&rebuild_chain).context("rebuilding extent index")?;
    let mut lbamap = lbamap::rebuild_segments(&rebuild_chain).context("rebuilding lba map")?;

    // Replay any in-progress WAL files into the LBA map so that hashes
    // referenced by post-checkpoint writes are visible to the liveness
    // check.  Without this, a DedupRef written to the WAL after
    // gc_checkpoint would be invisible to the coordinator — the hash
    // would appear dead, be excluded from the GC output, and the
    // volume's stale-liveness check would reject the handoff in a loop.
    replay_wal_into_lbamap(&fork_dir.join("wal"), &mut lbamap)?;

    let live_hashes = lbamap.lba_referenced_hashes();

    let floor: Option<Ulid> = latest_snapshot(fork_dir)?;

    let all_stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, floor)
        .context("collecting segment stats")?;
    let total_segments = all_stats.len();

    // Segments with a partial-LBA-death Delta entry whose sources don't
    // resolve this pass are ineligible for compaction: there's no base
    // body to reconstruct the composite against. They sit out this pass
    // and retry next tick, when later writes may re-establish a source.
    // Data/Inline/DedupRef partial death, and Delta with at least one
    // resolvable source, are all handled in-band via
    // `expand_partial_death`. See
    // `docs/design-gc-partial-death-compaction.md`.
    let (deferred, all_stats): (Vec<SegmentStats>, Vec<SegmentStats>) =
        all_stats.into_iter().partition(|s| s.has_partial_death);
    let deferred_count = deferred.len();
    if !deferred.is_empty() {
        let deferred_ulids: Vec<String> = deferred
            .iter()
            .map(|s| s.ulid_str[..8].to_string())
            .collect();
        tracing::info!(
            "[gc] deferring {} segment(s) with unresolvable partial-LBA-death Delta entries: [{}]",
            deferred_count,
            deferred_ulids.join(", ")
        );
    }

    // Partition the stats into three disjoint buckets by shape:
    //   * dead  — no live entries, no removed hashes (tombstone-only input).
    //   * small — live_lba_bytes ≤ SWEEP_SMALL_THRESHOLD.
    //   * large — everything else above the snapshot floor.
    //
    // Dead inputs are "free" packing candidates: they contribute to the
    // output's inputs list and let promote_segment drop the stale .idx at
    // apply time, without writing any body bytes for them.
    //
    // Smalls admit any entry kind (DATA, DEDUP_REF, Delta, Zero) — under
    // the plan-handoff protocol the volume materialises the output, so
    // consolidating e.g. several DEDUP_REF-only segments into one is a
    // legitimate reclamation of S3 object count even without any physical
    // body bytes to rewrite.
    let mut dead_inputs: Vec<SegmentStats> = Vec::new();
    let mut small_inputs: Vec<SegmentStats> = Vec::new();
    let mut large_inputs: Vec<SegmentStats> = Vec::new();
    for s in all_stats {
        if s.live_entries.is_empty() && s.removed_hashes.is_empty() {
            dead_inputs.push(s);
        } else if s.live_lba_bytes <= SWEEP_SMALL_THRESHOLD {
            small_inputs.push(s);
        } else {
            large_inputs.push(s);
        }
    }

    // Sparse-large candidates eligible to fill remaining headroom. Both
    // density and dead-bytes-present gates mirror what the old per-pass
    // repack enforced so a dense-large segment is never rewritten solely
    // for consolidation — the only benefit would be one fewer S3 object,
    // at the cost of rewriting ~16+ MiB of live bytes. `has_data_content`
    // excludes DEDUP_REF-only / Zero-only segments where rewriting
    // reclaims no physical body.
    let density_threshold = config.density_threshold;
    let sparse_large: Vec<SegmentStats> = large_inputs
        .into_iter()
        .filter(|s| {
            s.density() < density_threshold && s.dead_lba_bytes() > 0 && s.has_data_content()
        })
        .collect();

    // Smalls sorted ascending by live bytes for tightest bin-pack.
    small_inputs.sort_by_key(|s| s.live_lba_bytes);

    let mut bucket: Vec<SegmentStats> = Vec::new();
    let mut budget = SWEEP_LIVE_CAP;
    let mut entry_budget = SWEEP_ENTRY_CAP;

    // Tier 0 — tombstone inputs. Free in both body bytes and output entries
    // (they emit `Drop` records, not fresh entries). No budget effect.
    bucket.append(&mut dead_inputs);

    // Tier 1 — smalls, ascending. Greedy until the next would exceed either
    // the live-bytes budget or the output-entry budget.
    for s in small_inputs {
        let s_entries = s.estimated_output_entries();
        if s.live_lba_bytes <= budget && s_entries <= entry_budget {
            budget -= s.live_lba_bytes;
            entry_budget -= s_entries;
            bucket.push(s);
        }
    }

    // Tier 2 — at most one sparse-large filler. Prefer lowest density
    // (maximum dead-byte reclamation per rewritten byte); tie-break by
    // largest live_lba_bytes (best fit for the remaining headroom).
    // Filler must also fit the remaining entry budget.
    if budget > 0
        && entry_budget > 0
        && let Some((pos, _)) = sparse_large
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                s.live_lba_bytes <= budget && s.estimated_output_entries() <= entry_budget
            })
            .min_by(|(_, a), (_, b)| {
                a.density()
                    .partial_cmp(&b.density())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.live_lba_bytes.cmp(&a.live_lba_bytes))
            })
    {
        let mut sparse_large = sparse_large;
        bucket.push(sparse_large.remove(pos));
    }

    // Skip a pass that has no reclamation benefit. A bucket is worth
    // emitting only if:
    //   * it folds in at least one tombstone (a DELETE either way), or
    //   * it folds in a sparse segment (rewrite reclaims dead bytes), or
    //   * it consolidates ≥ 2 inputs (one fewer S3 object per pack).
    // A single dense small input on its own is a pointless rewrite.
    let has_dead = bucket
        .iter()
        .any(|s| s.live_entries.is_empty() && s.removed_hashes.is_empty());
    let has_sparse = bucket.iter().any(|s| s.density() < density_threshold);
    if !has_dead && !has_sparse && bucket.len() < 2 {
        if deferred_count > 0 {
            return Ok(GcStats {
                strategy: GcStrategy::None(NoneReason::NoCandidates),
                candidates: 0,
                bytes_freed: 0,
                dead_cleaned: 0,
                deferred: deferred_count,
                total_segments,
            });
        }
        return Ok(GcStats::none(NoneReason::NoCandidates, total_segments));
    }

    // Sort the bucket back into ULID order before handing it to
    // `compact_segments` so its rebuild-order semantics hold (when two
    // input segments cover the same LBA the latest source wins).
    bucket.sort_by(|a, b| a.ulid_str.cmp(&b.ulid_str));

    let candidates = bucket.len();
    let bytes_freed: u64 = bucket.iter().map(|s| s.dead_lba_bytes()).sum();
    let live_bytes: u64 = bucket.iter().map(|s| s.live_lba_bytes).sum();
    let live_entries: usize = bucket.iter().map(|s| s.live_entries.len()).sum();
    let removed_hashes: usize = bucket.iter().map(|s| s.removed_hashes.len()).sum();
    let dead_cleaned = bucket
        .iter()
        .filter(|s| s.live_entries.is_empty() && s.removed_hashes.is_empty())
        .count();
    let input_ulids: Vec<&str> = bucket.iter().map(|s| s.ulid_str.as_str()).collect();
    tracing::info!(
        "[gc] compact: [{}] → {u_gc} inputs={} live_lba={} dead_lba={} \
         live_entries={} removed_hashes={} dead_folded={}",
        input_ulids.join(", "),
        candidates,
        live_bytes,
        bytes_freed,
        live_entries,
        removed_hashes,
        dead_cleaned,
    );
    compact_segments(bucket, &gc_dir, u_gc, &live_hashes).context("gc compaction")?;

    Ok(GcStats {
        strategy: GcStrategy::Compact,
        candidates,
        bytes_freed,
        dead_cleaned,
        deferred: deferred_count,
        total_segments,
    })
}

/// Process volume-applied GC handoffs: walk bare `gc/<ulid>` files, upload
/// each to S3, send `promote_segment` IPC, delete the corresponding old S3
/// objects, then send `finalize_gc_handoff` IPC so the volume removes the
/// bare body.
///
/// Called at the start of every `gc_loop` tick so that old S3 objects are
/// cleaned up promptly after the volume acknowledges each handoff. Any bare
/// files that survive a coordinator crash are processed on the next startup
/// tick — every step is idempotent (S3 PUT is idempotent; 404 on delete is
/// treated as success; `finalize_gc_handoff` is a no-op if the bare file is
/// already gone).
///
/// Returns the number of handoffs completed.
pub async fn apply_done_handoffs(
    fork_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
) -> Result<usize> {
    let gc_dir = fork_dir.join("gc");
    if !gc_dir.try_exists().context("checking gc dir")? {
        return Ok(0);
    }

    // Bare-named `gc/<ulid>` files are volume-applied handoffs awaiting S3
    // upload. Under the self-describing protocol, a bare file's `inputs`
    // header field lists the consumed source ULIDs — everything needed to
    // finish the handoff (upload → promote → S3 delete → local delete).
    let mut bare: Vec<fs::DirEntry> = fs::read_dir(&gc_dir)
        .context("reading gc dir")?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let Some(name) = name.to_str() else {
                return false;
            };
            // No extension; ULID-parseable stem; no sibling `.plan`
            // (would indicate mid-apply crash state that volume will
            // resolve on its next apply tick).
            if name.contains('.') {
                return false;
            }
            if Ulid::from_string(name).is_err() {
                return false;
            }
            !gc_dir.join(format!("{name}.plan")).exists()
        })
        .collect();

    if bare.is_empty() {
        return Ok(0);
    }

    bare.sort_by_key(|e| e.file_name());
    let cache_dir = fork_dir.join("cache");
    let mut count = 0;

    for entry in &bare {
        let filename = entry.file_name();
        let name = filename
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("gc filename is not valid UTF-8"))?;
        let new_ulid =
            Ulid::from_string(name).map_err(|e| anyhow::anyhow!("invalid gc filename: {e}"))?;
        let new_ulid_str = new_ulid.to_string();
        let gc_body = entry.path();
        let cache_body = cache_dir.join(format!("{new_ulid_str}.body"));

        // Verify signature and extract the inputs list. Loading the volume
        // key here (rather than at the top) keeps this function usable for
        // empty-gc-dir cases without requiring volume.pub.
        let vk =
            elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
                .context("loading volume verifying key")?;
        let (_, gc_entries, inputs) = segment::read_and_verify_segment_index(&gc_body, &vk)
            .with_context(|| {
                format!("signature verification failed for compacted segment {new_ulid_str}")
            })?;

        // Sanity check: GC output is well-formed.
        debug_assert!(
            gc_entries.iter().all(|e| match e.kind {
                EntryKind::DedupRef => e.stored_length == 0 && e.stored_offset == 0,
                EntryKind::Zero => e.stored_length == 0,
                _ => true,
            }),
            "compacted segment {new_ulid_str}: malformed DedupRef/Zero entry"
        );

        // Upload + promote are idempotent: if a previous pass already
        // uploaded and promoted, the store PUT is a re-PUT of the same
        // bytes and `promote_segment` short-circuits on cache body presence.
        crate::upload::upload_segment_file(
            &gc_body,
            &new_ulid_str,
            volume_id,
            store,
            part_size_bytes,
        )
        .await
        .with_context(|| format!("uploading compacted segment {new_ulid_str}"))?;

        // Promote IPC: volume writes index/<new>.idx, copies body to cache,
        // deletes stale index/<old>.idx for each input. The gc body stays
        // in place — we delete it at the end via `finalize_gc_handoff`.
        if !crate::control::promote_segment(fork_dir, new_ulid).await {
            warn!("[gc] promote {new_ulid_str}: volume not running; will retry next tick");
            continue;
        }

        // Delete old S3 objects for each consumed input. 404 means the
        // object is already gone (idempotent across restart).
        for old_ulid in &inputs {
            let old_ulid_str = old_ulid.to_string();
            let key = segment_key(volume_id, &old_ulid_str)
                .with_context(|| format!("building key for {old_ulid_str}"))?;
            match store.delete(&key).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => {
                    return Err(
                        anyhow::anyhow!(e).context(format!("deleting S3 object {old_ulid_str}"))
                    );
                }
            }
        }

        // Drop each consumed input's local cache. Safe here: promote_segment
        // already published cache/<new>.body and index/<new>.idx, and deleted
        // index/<input>.idx, so reads for input LBAs resolve through the new
        // segment. The volume never deletes cache/ — the coordinator is the
        // sole deleter, so this does not race the volume's apply path.
        for old_ulid in &inputs {
            let old_ulid_str = old_ulid.to_string();
            let _ = fs::remove_file(cache_dir.join(format!("{old_ulid_str}.body")));
            let _ = fs::remove_file(cache_dir.join(format!("{old_ulid_str}.present")));
        }

        // Finalize: volume deletes bare `gc/<new>` inside the actor, under
        // the same lock as `apply_gc_handoffs`. This is the "done" signal —
        // no more retries for this handoff.
        if !crate::control::finalize_gc_handoff(fork_dir, new_ulid).await {
            warn!("[gc] finalize {new_ulid_str}: volume not running; will retry next tick");
            continue;
        }

        let _ = cache_body; // previously used for seg_promoted detection
        count += 1;
    }

    Ok(count)
}

/// Delete any `gc/<ulid>.fetch` files left by a coordinator crash.
///
/// `.fetch` files are transient S3 downloads written by `compact_segments`
/// and deleted immediately after the body is read.  If the coordinator crashes
/// between the write and the delete, the file is left behind.  It is always
/// safe to remove unconditionally — the full segment remains in S3.
fn cleanup_fetch_files(gc_dir: &Path) {
    let Ok(entries) = fs::read_dir(gc_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.ends_with(".fetch")
            && let Err(e) = fs::remove_file(entry.path())
        {
            error!("[gc] failed to delete stale fetch file {name_str}: {e}");
        }
    }
}

/// Delete any `gc/<ulid>.plan.tmp` scratch files left by a crashed
/// `compact_segments` between the plan write and the rename to `<ulid>.plan`.
/// The next pass mints a fresh ULID so the stale scratch is inert, but it
/// must be removed to avoid unbounded disk growth and to keep subsequent
/// plan writes free of stale interlopers.
///
/// Coordinator-owned — the volume's apply-path sweeper deliberately skips
/// this suffix to avoid racing an in-flight write.
fn cleanup_staging_files(gc_dir: &Path) {
    let Ok(entries) = fs::read_dir(gc_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.ends_with(".plan.tmp")
            && let Err(e) = fs::remove_file(entry.path())
        {
            error!("[gc] failed to delete stale plan scratch file {name_str}: {e}");
        }
    }
}

// --- internals ---

/// Logical block size: all LBA lengths are in units of this many bytes.
const BLOCK_BYTES: u64 = 4096;

/// Per-segment stats computed during the scan phase.
struct SegmentStats {
    ulid_str: String,
    /// Logical live bytes: lba_length * BLOCK_BYTES summed over all live entries
    /// (DATA, dedup_ref, zero_extent).  Dedup refs and zero extents are included
    /// so that a segment full of live dedup refs is not treated as density=0.
    live_lba_bytes: u64,
    /// Logical total bytes: lba_length * BLOCK_BYTES summed over all entries.
    total_lba_bytes: u64,
    /// True if the segment contains at least one DATA entry (live, stale, or fully dead).
    /// Used to distinguish zero-only segments (no physical body to reclaim) from
    /// segments with DATA or REF entries that warrant GC even when all entries are dead.
    has_body_entries: bool,
    /// Live entries — already classified by `collect_stats` (Zero sub-splits
    /// expanded, fully-LBA-dead-hash-live entries demoted via `into_canonical`).
    /// The coordinator uses these plus `partial_death_runs` to emit a
    /// [`elide_core::gc_plan::GcPlan`]; the volume materialises bodies on apply.
    live_entries: Vec<SegmentEntry>,
    /// Per-entry position in the source segment's index section, parallel to
    /// `live_entries`. Referenced by plan outputs so the volume's materialise
    /// path can address the original input-segment entry.
    live_entry_indices: Vec<u32>,
    /// Hashes that are in the extent index but not reachable from the LBA map.
    /// The volume's apply path derives removals from its input-`.idx`-vs-
    /// materialised-output diff, so this list is retained for stats/logging
    /// only, not for handoff transport.
    removed_hashes: Vec<blake3::Hash>,
    /// Parallel to `live_entries`: for Data/Inline/DedupRef/Delta entries with
    /// partial LBA death, the list of live sub-runs (filtered to `r.hash ==
    /// entry.hash`). The coordinator encodes these as `PlanOutput::Partial`
    /// runs; the volume materialises by slicing the composite body. `None`
    /// for non-partial entries. See `docs/design-gc-partial-death-compaction.md`.
    partial_death_runs: Vec<Option<Arc<[lbamap::ExtentRead]>>>,
    /// True if at least one partial-LBA-death entry in this segment cannot
    /// be expanded this pass. In the current implementation this is only
    /// set for Delta entries where none of the `delta_options[i]
    /// .source_hash`es resolve via the extent index — there is no base
    /// body to reconstruct against, so the segment is deferred via the
    /// same skip rule PR #77 introduced. Retry next pass when a later
    /// write may re-establish a source. Data/Inline/DedupRef partial-
    /// death is always handled in-band via `partial_death_runs` and does
    /// not set this flag.
    has_partial_death: bool,
}

impl SegmentStats {
    fn dead_lba_bytes(&self) -> u64 {
        self.total_lba_bytes.saturating_sub(self.live_lba_bytes)
    }

    fn density(&self) -> f64 {
        if self.total_lba_bytes > 0 {
            self.live_lba_bytes as f64 / self.total_lba_bytes as f64
        } else {
            0.0
        }
    }

    /// True if this segment contains at least one DATA entry (regardless of
    /// liveness state).  Only DATA entries have physical body bytes to reclaim
    /// from S3.  A segment of only dedup refs / zero extents — whether live or
    /// dead — has no physical body; compacting it produces no storage savings
    /// and should be skipped.
    fn has_data_content(&self) -> bool {
        self.has_body_entries
    }

    /// Upper bound on the number of entries this input contributes to the
    /// merged GC output. Each non-partial live entry produces one output
    /// (Keep / Canonical / ZeroSplit). A partial-LBA-death entry expands
    /// into one Run per surviving sub-run, plus an optional Canonical for
    /// owned-body kinds (Data / Inline / Delta) — we treat that Canonical
    /// as always present, which overestimates by at most one per partial
    /// DedupRef. Tombstone-only inputs (no live entries) contribute zero —
    /// they emit a Drop record on the plan, not a fresh output entry.
    fn estimated_output_entries(&self) -> usize {
        let mut n = 0usize;
        for runs in &self.partial_death_runs {
            match runs {
                Some(r) => n += r.len() + 1,
                None => n += 1,
            }
        }
        n
    }
}

/// Scan `index/` and compute liveness stats for each committed segment.
/// Returns segments in ULID (chronological) order; snapshot-frozen segments
/// are excluded.
///
/// Segments are sorted using `sort_for_rebuild` semantics: GC outputs (those
/// with an in-flight `.staged` file or a bare `gc/<ulid>` file) come first
/// (lower priority); regular segments come last (higher priority). This is
/// critical for
/// `compact_segments`: when entries for the same LBA appear in multiple input
/// segments, the last-processed segment's entry wins in the output.  Using
/// sort_for_rebuild order ensures newer regular segments (even if they have a
/// lower ULID than an older GC output) override stale GC-output entries.
fn collect_stats(
    fork_dir: &Path,
    vk: &elide_core::signing::VerifyingKey,
    index: &ExtentIndex,
    live_hashes: &HashSet<blake3::Hash>,
    lba_map: &LbaMap,
    floor: Option<Ulid>,
) -> io::Result<Vec<SegmentStats>> {
    let index_dir = fork_dir.join("index");
    let mut idx_files = segment::collect_idx_files(&index_dir)?;
    segment::sort_for_rebuild(fork_dir, &mut idx_files);

    let mut result = Vec::new();
    for idx_path in idx_files {
        let Some(ulid_str) = idx_path
            .file_stem()
            .and_then(|n| n.to_str())
            .map(str::to_owned)
        else {
            continue;
        };
        let seg_ulid = Ulid::from_string(&ulid_str).map_err(|e| io::Error::other(e.to_string()))?;

        if let Some(f) = floor
            && seg_ulid <= f
        {
            continue;
        }

        // Read index from .idx — the only local file we need for stats.
        // Presence of .idx means the segment is confirmed in S3 (invariant:
        // .idx is written inside promote_segment IPC, after confirmed S3 PUT).
        let (_, entries, _) = segment::read_and_verify_segment_index(&idx_path, vk)?;

        // Read inline section from .idx for any inline entries.
        let has_inline = entries
            .iter()
            .any(|e| matches!(e.kind, EntryKind::Inline | EntryKind::CanonicalInline));
        let inline_bytes = if has_inline {
            segment::read_inline_section(&idx_path)?
        } else {
            Vec::new()
        };

        let mut live_lba_bytes: u64 = 0;
        let mut total_lba_bytes: u64 = 0;
        let mut physical_body_bytes: u64 = 0;
        let mut live_entries: Vec<SegmentEntry> = Vec::new();
        let mut live_entry_indices: Vec<u32> = Vec::new();
        let mut partial_death_runs: Vec<Option<Arc<[lbamap::ExtentRead]>>> = Vec::new();
        let mut removed_hashes: Vec<blake3::Hash> = Vec::new();
        let mut has_partial_death = false;
        // Capture body-owning entries that this pass classifies as fully dead
        // so we can log the coordinator's LBA-map view at classification time.
        // This pairs with the volume's `diagnose_stale_cancel_legacy` — if a
        // later apply trips stale-liveness on one of these hashes, the two
        // views can be diffed directly from the logs.
        struct DeadEntry {
            hash: blake3::Hash,
            start_lba: u64,
            lba_length: u32,
            per_lba: Vec<(u64, Option<blake3::Hash>)>,
        }
        let mut dead_diag: Vec<DeadEntry> = Vec::new();

        for (entry_idx, mut entry) in entries.into_iter().enumerate() {
            let entry_idx = entry_idx as u32;
            // Pre-populate inline entry data from the .idx inline section.
            // compact_segments needs this data to write the output segment.
            if entry.kind.is_inline() {
                let start = entry.stored_offset as usize;
                let end = start + entry.stored_length as usize;
                if end <= inline_bytes.len() {
                    entry.data = Some(inline_bytes[start..end].to_vec());
                }
            }
            let lba_bytes = entry.lba_length as u64 * BLOCK_BYTES;
            total_lba_bytes += lba_bytes;

            // Zero extents have no body bytes and use ZERO_HASH as a sentinel
            // (never in the extent index). Liveness is LBA-map ownership: a
            // sub-range of the entry is live iff the lbamap still maps it to
            // ZERO_HASH. A Zero entry can span thousands of LBAs (e.g. whole
            // mkfs TRIM), any subset of which may have since been overwritten
            // by later writes. Re-emitting the original span at the GC-output
            // ULID would shadow those later writes on rebuild — so split the
            // entry into sub-Zeros covering only the surviving ZERO_HASH runs.
            if entry.kind == EntryKind::Zero {
                let end = entry.start_lba + entry.lba_length as u64;
                for ext in lba_map.extents_in_range(entry.start_lba, end) {
                    if ext.hash != ZERO_HASH {
                        continue;
                    }
                    let run_len = (ext.range_end - ext.range_start) as u32;
                    let run_bytes = run_len as u64 * BLOCK_BYTES;
                    live_lba_bytes += run_bytes;
                    let mut live = entry.clone();
                    live.start_lba = ext.range_start;
                    live.lba_length = run_len;
                    live_entries.push(live);
                    live_entry_indices.push(entry_idx);
                    partial_death_runs.push(None);
                }
                continue;
            }
            // Inline/CanonicalInline entries have stored bytes in the inline
            // section (part of .idx, not S3 body). They do not contribute to
            // physical_body_bytes but do participate in liveness.
            if !entry.kind.is_inline() {
                physical_body_bytes += entry.stored_length as u64;
            }
            // DATA / Inline / DedupRef / Delta entries all carry both a hash
            // and an LBA claim. Liveness classification uses a full-range
            // scan against the lbamap, not a point query at `start_lba`,
            // because a multi-LBA entry's head, tail, or interior may have
            // been overwritten by later writes while `start_lba` is
            // unaffected (and vice versa). See
            // `docs/design-gc-overlap-correctness.md`.
            //
            // DedupRef is a thin record (`stored_length=0`, no body bytes),
            // so it contributes nothing to `physical_body_bytes` (the
            // accumulator above already zero-passes it). For the canonical-
            // emit path it's a no-op: DedupRef's canonical body lives at
            // `extent_index[entry.hash].segment_id`, a segment that's not
            // being compacted, so no body needs to be preserved in the
            // compacted output.
            //
            // Outcomes:
            //   - **fully alive** (every LBA in the range maps to
            //     `entry.hash`): keep the entry intact. Its LBA binding is
            //     authoritative and compacts cleanly.
            //   - **fully dead** (no LBA in the range maps to `entry.hash`):
            //     * owned-body kinds (Data/Inline) with hash still
            //       referenced elsewhere: demote to `canonical_only`. Body
            //       survives for extent-index resolution; no LBA claim on
            //       rebuild.
            //     * DedupRef: drop the entry. (No body to demote; if the
            //       segment happens to own the hash in the extent index,
            //       also schedule removal via `removed_hashes`.)
            //     * hash orphaned entirely: record for extent-index removal
            //       and drop.
            //   - **partially alive** (some LBAs still live at `entry.hash`,
            //     others overwritten): route into `partial_death_runs` for
            //     expansion in `compact_segments`. Delta with no
            //     resolvable `source_hash` is the sole remaining defer
            //     case — no base body means no reconstruction this pass.
            let end_lba = entry.start_lba + entry.lba_length as u64;
            let runs = lba_map.extents_in_range(entry.start_lba, end_lba);
            let matching_blocks: u64 = runs
                .iter()
                .filter(|r| r.hash == entry.hash)
                .map(|r| r.range_end - r.range_start)
                .sum();
            let total_blocks = entry.lba_length as u64;
            let extent_live = index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == seg_ulid);

            if matching_blocks == total_blocks {
                // Fully alive.
                live_lba_bytes += lba_bytes;
                live_entries.push(entry);
                live_entry_indices.push(entry_idx);
                partial_death_runs.push(None);
            } else if matching_blocks == 0 {
                match entry.kind {
                    EntryKind::Data | EntryKind::Inline | EntryKind::Delta
                        if extent_live && live_hashes.contains(&entry.hash) =>
                    {
                        // Fully LBA-dead but hash still externally live.
                        // Demote the source entry into its canonical variant —
                        // body preserved for dedup resolution, LBA claim
                        // dropped.
                        live_entries.push(entry.into_canonical());
                        live_entry_indices.push(entry_idx);
                        partial_death_runs.push(None);
                    }
                    _ if extent_live => {
                        // Fully dead and this segment owns the hash in the
                        // extent index — schedule removal.
                        removed_hashes.push(entry.hash);
                        if entry.kind.has_body_bytes() && dead_diag.len() < 8 {
                            let per_lba: Vec<(u64, Option<blake3::Hash>)> = (entry.start_lba
                                ..entry.start_lba + entry.lba_length as u64)
                                .map(|lba| (lba, lba_map.hash_at(lba)))
                                .collect();
                            dead_diag.push(DeadEntry {
                                hash: entry.hash,
                                start_lba: entry.start_lba,
                                lba_length: entry.lba_length,
                                per_lba,
                            });
                        }
                    }
                    _ => {
                        // Fully dead with no extent-index ownership here.
                        // Nothing to preserve or remove — drop silently.
                    }
                }
            } else {
                // Partially alive. For Data/Inline/DedupRef the composite body
                // can be resolved at expand time (from entry.data for owned
                // bodies, or via extent_index lookup for DedupRef — see
                // `expand_partial_death`), so `compact_segments` can slice it
                // into live sub-runs without deferring the segment.
                //
                // Delta routes through the same path provided at least one
                // `delta_options[i].source_hash` resolves via `extent_index`
                // (the Data-body map). If none resolve, there is no base
                // body to reconstruct against — fall back to the segment-
                // level defer (`has_partial_death = true`) and retry next
                // pass, when later writes may have re-established a source.
                live_lba_bytes += matching_blocks * BLOCK_BYTES;
                match entry.kind {
                    EntryKind::Data | EntryKind::Inline | EntryKind::DedupRef => {
                        let live_runs: Arc<[lbamap::ExtentRead]> =
                            runs.into_iter().filter(|r| r.hash == entry.hash).collect();
                        live_entries.push(entry);
                        live_entry_indices.push(entry_idx);
                        partial_death_runs.push(Some(live_runs));
                    }
                    EntryKind::Delta => {
                        let has_resolvable_source = entry
                            .delta_options
                            .iter()
                            .any(|opt| index.lookup(&opt.source_hash).is_some());
                        if has_resolvable_source {
                            let live_runs: Arc<[lbamap::ExtentRead]> =
                                runs.into_iter().filter(|r| r.hash == entry.hash).collect();
                            live_entries.push(entry);
                            live_entry_indices.push(entry_idx);
                            partial_death_runs.push(Some(live_runs));
                        } else {
                            live_entries.push(entry);
                            live_entry_indices.push(entry_idx);
                            partial_death_runs.push(None);
                            has_partial_death = true;
                        }
                    }
                    _ => {
                        // Zero is pre-split above; CanonicalData/CanonicalInline
                        // never reach this loop (they carry no LBA claim, so
                        // their `lba_length` is 0 and the earlier `total_lba_bytes`
                        // accounting zero-passes them). Treat as fully alive
                        // to stay safe.
                        live_entries.push(entry);
                        live_entry_indices.push(entry_idx);
                        partial_death_runs.push(None);
                    }
                }
            }
        }

        if !dead_diag.is_empty() {
            use std::fmt::Write as _;

            // An entry is "anomalous" if its per-LBA lbamap view contains
            // something that shouldn't happen for a cleanly-dead entry:
            //   - `None`: gap in the lbamap for a range the entry claimed
            //     (rebuild bug or missing replay)
            //   - `Some(h) == entry.hash`: classification said dead but the
            //     LBA still maps to this hash (classification bug)
            // Healthy cases (ZERO_HASH replacement, single or multiple
            // replacement hashes) render at debug with RLE-collapsed runs.
            let is_anomalous = |d: &DeadEntry| {
                d.per_lba.iter().any(|(_, h)| match h {
                    None => true,
                    Some(h) => *h == d.hash,
                })
            };

            let render_runs = |d: &DeadEntry| -> String {
                let mut out = String::new();
                let mut iter = d.per_lba.iter().peekable();
                while let Some((_, head)) = iter.next() {
                    let mut count: u64 = 1;
                    while let Some((_, next)) = iter.peek() {
                        if *next == *head {
                            iter.next();
                            count += 1;
                        } else {
                            break;
                        }
                    }
                    if !out.is_empty() {
                        out.push(',');
                    }
                    match head {
                        Some(h) => {
                            let _ = write!(out, "{count}×{}", h.to_hex());
                        }
                        None => {
                            let _ = write!(out, "{count}×None");
                        }
                    }
                }
                out
            };

            let render_per_lba = |d: &DeadEntry| -> String {
                let mut out = String::new();
                for (j, (lba, h)) in d.per_lba.iter().enumerate() {
                    if j > 0 {
                        out.push(',');
                    }
                    match h {
                        Some(h) => {
                            let _ = write!(out, "{lba}={}", h.to_hex());
                        }
                        None => {
                            let _ = write!(out, "{lba}=None");
                        }
                    }
                }
                out
            };

            let (anomalous, healthy): (Vec<&DeadEntry>, Vec<&DeadEntry>) =
                dead_diag.iter().partition(|d| is_anomalous(d));

            if !anomalous.is_empty() {
                let mut rendered = String::new();
                for (i, d) in anomalous.iter().enumerate() {
                    if i > 0 {
                        rendered.push_str("; ");
                    }
                    let _ = write!(
                        rendered,
                        "hash={} lba={}+{} lbamap=[{}]",
                        d.hash.to_hex(),
                        d.start_lba,
                        d.lba_length,
                        render_per_lba(d),
                    );
                }
                tracing::info!(
                    "[gc {ulid_str}] body-owning entries classified dead with anomalous lbamap \
                     ({} of {}): {}",
                    anomalous.len(),
                    dead_diag.len(),
                    rendered,
                );
            }

            if !healthy.is_empty() {
                let mut rendered = String::new();
                for (i, d) in healthy.iter().enumerate() {
                    if i > 0 {
                        rendered.push_str("; ");
                    }
                    let _ = write!(
                        rendered,
                        "hash={} lba={}+{} → {}",
                        d.hash.to_hex(),
                        d.start_lba,
                        d.lba_length,
                        render_runs(d),
                    );
                }
                tracing::debug!(
                    "[gc {ulid_str}] body-owning entries classified dead ({} of {}): {}",
                    healthy.len(),
                    dead_diag.len(),
                    rendered,
                );
            }
        }

        result.push(SegmentStats {
            ulid_str,
            has_body_entries: physical_body_bytes > 0 || has_inline,
            live_lba_bytes,
            total_lba_bytes,
            live_entries,
            live_entry_indices,
            partial_death_runs,
            removed_hashes,
            has_partial_death,
        });
    }
    Ok(result)
}

/// Classify each candidate's live entries and emit a `gc/<new-ulid>.plan`
/// file describing the desired output. The volume picks up the plan on its
/// next idle tick, resolves bodies through its own ancestor-aware BlockReader,
/// assembles and signs the output segment, and renames tmp → bare. See
/// `docs/design-gc-plan-handoff.md`.
///
fn compact_segments(
    candidates: Vec<SegmentStats>,
    gc_dir: &Path,
    new_ulid: Ulid,
    live_hashes: &HashSet<blake3::Hash>,
) -> Result<()> {
    let new_ulid_str = new_ulid.to_string();
    fs::create_dir_all(gc_dir).context("creating gc dir")?;

    let mut outputs: Vec<PlanOutput> = Vec::new();
    let mut total_inputs = 0usize;

    // Per-kind counters for the visibility log line.
    let mut keep_count = 0usize;
    let mut zero_split_count = 0usize;
    let mut canonical_count = 0usize;
    let mut run_count = 0usize;
    let mut drop_count = 0usize;

    for candidate in &candidates {
        let input_ulid = Ulid::from_string(&candidate.ulid_str)
            .with_context(|| format!("parsing candidate ulid {}", candidate.ulid_str))?;
        total_inputs += 1;

        // Fully-dead input with nothing to preserve and nothing explicitly
        // removed — it still needs to be consumed so its .idx is walked at
        // apply time. Emit a `drop` record so the input appears in the plan's
        // derived inputs list.
        let has_any_live = !candidate.live_entries.is_empty();
        let has_any_removed = !candidate.removed_hashes.is_empty();
        if !has_any_live && !has_any_removed {
            outputs.push(PlanOutput::Drop { input: input_ulid });
            drop_count += 1;
            continue;
        }
        // Tombstone-style input: no kept entries but hashes to remove.
        // Signal with a Drop so the input is listed; the apply path
        // derives the removals from the input's .idx.
        if !has_any_live {
            outputs.push(PlanOutput::Drop { input: input_ulid });
            drop_count += 1;
            continue;
        }

        for (i, entry) in candidate.live_entries.iter().enumerate() {
            let entry_idx = candidate.live_entry_indices[i];
            let sub_runs = &candidate.partial_death_runs[i];

            match (sub_runs, entry.kind) {
                (Some(runs), _) => {
                    // Partial-LBA-death: one optional `canonical` + one
                    // `run` per surviving sub-run. `emit_canonical` mirrors
                    // the former `expand_partial_death` decision — owned-
                    // body kinds (Data / Inline / Delta) emit a canonical
                    // when the composite hash is still LBA-live, preserving
                    // the composite body for dedup resolution. DedupRef
                    // never emits a canonical (body lives in a segment this
                    // pass doesn't touch).
                    let emit_canonical = matches!(
                        entry.kind,
                        EntryKind::Data | EntryKind::Inline | EntryKind::Delta
                    ) && live_hashes.contains(&entry.hash);
                    if emit_canonical {
                        outputs.push(PlanOutput::Canonical {
                            input: input_ulid,
                            entry_idx,
                        });
                        canonical_count += 1;
                    }
                    for r in runs.iter() {
                        outputs.push(PlanOutput::Run {
                            input: input_ulid,
                            entry_idx,
                            payload_block_offset: r.payload_block_offset,
                            start_lba: r.range_start,
                            lba_length: (r.range_end - r.range_start) as u32,
                        });
                        run_count += 1;
                    }
                }
                (None, EntryKind::Zero) => {
                    // `collect_stats` pre-splits multi-LBA Zero entries into
                    // surviving sub-runs by cloning the entry with adjusted
                    // `(start_lba, lba_length)`. Emit a `ZeroSplit` for every
                    // such sub-run — the materialise path discards the source
                    // entry's original span and honours the planned span.
                    outputs.push(PlanOutput::ZeroSplit {
                        input: input_ulid,
                        entry_idx,
                        start_lba: entry.start_lba,
                        lba_length: entry.lba_length,
                    });
                    zero_split_count += 1;
                }
                (None, EntryKind::CanonicalData | EntryKind::CanonicalInline) => {
                    // `collect_stats` demotes fully-LBA-dead hash-live entries
                    // by calling `into_canonical()`, which flips the kind.
                    // Emit as `Canonical` — the materialise side reads the
                    // stored bytes and writes a `Canonical*` in the output.
                    outputs.push(PlanOutput::Canonical {
                        input: input_ulid,
                        entry_idx,
                    });
                    canonical_count += 1;
                }
                (None, _) => {
                    // Fully-alive Data / Inline / DedupRef / Delta — pass
                    // through unchanged. For Delta, the materialise path will
                    // also carry the delta blob across into the output's
                    // delta body section.
                    outputs.push(PlanOutput::Keep {
                        input: input_ulid,
                        entry_idx,
                    });
                    keep_count += 1;
                }
            }
        }
    }

    let plan = GcPlan { new_ulid, outputs };
    let plan_path = gc_dir.join(format!("{new_ulid_str}.plan"));
    plan.write_atomic(&plan_path)
        .context("writing GC plan file")?;

    tracing::info!(
        "[gc] plan emitted → {new_ulid_str}: keep={keep_count} zero_split={zero_split_count} \
         canonical={canonical_count} run={run_count} drop={drop_count} \
         records={} inputs={total_inputs}",
        plan.outputs.len(),
    );

    Ok(())
}

/// Returns true if any in-flight GC handoffs exist in `gc_dir`.
///
/// An in-flight handoff is either:
/// - a `.plan` file (coordinator emitted it, volume has not yet materialised), or
/// - a bare `gc/<ulid>` file (volume materialised + signed, coordinator upload pending).
///
/// A new GC pass is deferred while any of these exist — the coordinator
/// should finish the current batch before emitting another plan.
fn has_pending_results(gc_dir: &Path) -> Result<bool> {
    if !gc_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(gc_dir).context("reading gc dir")? {
        let entry = entry.context("reading gc dir entry")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(".plan") {
            return Ok(true);
        }
        if !name.contains('.') && Ulid::from_string(name).is_ok() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Replay WAL records into `lbamap` so that the coordinator's liveness
/// view includes writes that arrived after `gc_checkpoint`.
///
/// `gc_checkpoint` flushes the WAL to `pending/`, but new writes can
/// arrive to a fresh WAL before the coordinator runs GC.  If those
/// writes include DedupRef entries pointing to hashes in GC input
/// segments, the coordinator would consider those hashes dead (they're
/// not in `pending/` or `index/`).  The volume's stale-liveness check
/// then rejects the GC output, causing an infinite retry loop.
///
/// Reading the WAL here closes that gap: the coordinator sees all
/// writes the volume has accepted, including in-flight WAL data.
fn replay_wal_into_lbamap(wal_dir: &Path, lbamap: &mut LbaMap) -> Result<()> {
    let entries = match fs::read_dir(wal_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let (records, _complete) = match elide_core::writelog::scan_readonly(&path) {
            Ok(r) => r,
            Err(e) => {
                warn!("skipping unreadable WAL {}: {e}", path.display());
                continue;
            }
        };
        for record in records {
            match record {
                elide_core::writelog::LogRecord::Data {
                    hash,
                    start_lba,
                    lba_length,
                    ..
                }
                | elide_core::writelog::LogRecord::Ref {
                    hash,
                    start_lba,
                    lba_length,
                } => {
                    lbamap.insert(start_lba, lba_length, hash);
                }
                elide_core::writelog::LogRecord::Zero {
                    start_lba,
                    lba_length,
                } => {
                    lbamap.insert(start_lba, lba_length, elide_core::volume::ZERO_HASH);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    struct MockSocket(tokio::task::JoinHandle<()>);
    impl Drop for MockSocket {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    /// Spawn a mock volume control socket at `<fork_dir>/control.sock`.
    ///
    /// Responds "ok" to any command, and for "promote <ulid>" also performs
    /// the volume's promote behaviour: copies the segment body from gc/ or
    /// pending/ into cache/, and deletes the pending/ file on the drain path.
    async fn spawn_mock_socket(fork_dir: std::path::PathBuf) -> MockSocket {
        let socket_path = fork_dir.join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let dir = fork_dir.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
                    let (r, mut w) = tokio::io::split(stream);
                    let mut buf = BufReader::new(r);
                    let mut line = String::new();
                    let _ = buf.read_line(&mut line).await;
                    let line = line.trim().to_owned();
                    if let Some(ulid_str) = line.strip_prefix("promote ") {
                        let ulid_str = ulid_str.to_owned();
                        let gc_src = dir.join("gc").join(&ulid_str);
                        let pending_src = dir.join("pending").join(&ulid_str);
                        let (src, is_drain) = if gc_src.exists() {
                            (gc_src, false)
                        } else {
                            (pending_src, true)
                        };
                        if src.exists() {
                            let cache = dir.join("cache");
                            std::fs::create_dir_all(&cache).ok();
                            let body = cache.join(format!("{ulid_str}.body"));
                            let present = cache.join(format!("{ulid_str}.present"));
                            elide_core::segment::promote_to_cache(&src, &body, &present).ok();
                            // Real volume deletes src after promote in both
                            // drain and GC paths (cache holds the body now).
                            std::fs::remove_file(&src).ok();
                        }
                        let _ = is_drain;
                    } else if let Some(ulid_str) = line.strip_prefix("finalize_gc_handoff ") {
                        let ulid_str = ulid_str.to_owned();
                        let applied = dir.join("gc").join(format!("{ulid_str}.applied"));
                        let done = dir.join("gc").join(format!("{ulid_str}.done"));
                        std::fs::rename(&applied, &done).ok();
                    }
                    w.write_all(b"ok\n").await.ok();
                });
            }
        });
        MockSocket(handle)
    }

    #[test]
    fn no_pending_results_when_gc_dir_absent() {
        let tmp = TempDir::new().unwrap();
        assert!(!has_pending_results(&tmp.path().join("gc")).unwrap());
    }

    #[test]
    fn no_pending_results_when_gc_dir_empty() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        assert!(!has_pending_results(&gc_dir).unwrap());
    }

    #[test]
    fn detects_plan_result_file() {
        // `has_pending_results` returns true while any handoff is in flight:
        // either a coordinator-emitted `.plan` file or a volume-materialised
        // bare `gc/<ulid>` waiting for upload.
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.plan"), "").unwrap();
        assert!(has_pending_results(&gc_dir).unwrap());
    }

    #[test]
    fn detects_bare_result_file() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV"), "").unwrap();
        assert!(has_pending_results(&gc_dir).unwrap());
    }

    #[test]
    fn ignores_unknown_suffixes_in_gc_dir() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.tmp"), "").unwrap();
        assert!(!has_pending_results(&gc_dir).unwrap());
    }

    /// Redact, upload to store, and promote all pending segments.
    /// Mirrors the real coordinator path: redact → S3 PUT → promote.
    async fn drain_with_redact(
        vol: &mut elide_core::volume::Volume,
        dir: &Path,
        volume_id: &str,
        store: &Arc<dyn ObjectStore>,
    ) {
        let pending_dir = dir.join("pending");
        let mut ulids = Vec::new();
        for entry in fs::read_dir(&pending_dir).unwrap().flatten() {
            let name = entry.file_name();
            let Some(s) = name.to_str() else { continue };
            if s.contains('.') {
                continue;
            }
            if let Ok(ulid) = Ulid::from_string(s) {
                ulids.push(ulid);
            }
        }
        for &ulid in &ulids {
            vol.redact_segment(ulid).unwrap();
            // Upload the pending segment directly — redact hole-punches in
            // place, so there is no sidecar. The in-place file is the upload.
            let ulid_str = ulid.to_string();
            let seg_path = pending_dir.join(&ulid_str);
            let data = fs::read(&seg_path).unwrap();
            let key = segment_key(volume_id, &ulid_str).unwrap();
            store
                .put(&key, bytes::Bytes::from(data).into())
                .await
                .unwrap();
            vol.promote_segment(ulid).unwrap();
        }
    }

    // --- apply_done_handoffs tests ---

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn done_no_gc_dir() {
        let tmp = TempDir::new().unwrap();
        let store = make_store();
        let n = apply_done_handoffs(
            tmp.path(),
            "vol",
            &store,
            crate::upload::DEFAULT_PART_SIZE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn done_empty_gc_dir() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join("gc")).unwrap();
        let store = make_store();
        let n = apply_done_handoffs(
            tmp.path(),
            "vol",
            &store,
            crate::upload::DEFAULT_PART_SIZE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn done_ignores_non_applied_files() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        // .pending and .done files should be ignored.
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.pending"), "").unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.done"), "").unwrap();
        let store = make_store();
        let n = apply_done_handoffs(
            tmp.path(),
            "vol",
            &store,
            crate::upload::DEFAULT_PART_SIZE_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(n, 0);
        // Files should be untouched.
        assert!(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.pending").exists());
        assert!(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.done").exists());
    }

    // --- DEDUP_REF regression test ---

    /// Regression: GC compactor must preserve DEDUP_REF entries from source
    /// segments as DEDUP_REF in the output, not convert them to DATA with
    /// stored_length=0.
    ///
    /// Bug: `compact_candidates_inner` called `SegmentEntry::new_data(...,
    /// Vec::new())` for every live entry including dedup-refs.
    /// `read_extent_bodies` skips dedup-refs so their `data` field stays
    /// `Vec::new()`, and `new_data` then set `stored_length = 0`.  On rebuild
    /// after restart, `extentindex::rebuild` inserted this zero-length DATA
    /// entry for the hash, corrupting the extent index.  Subsequent reads
    /// sought to `bss + body_offset + payload_block_offset * 4096` — past EOF
    /// of the actual segment — and returned EIO.
    ///
    /// Sequence that triggers the bug:
    ///   1. Write content H to lba 0 → flush → drain: segment S1 DATA(lba=0, H)
    ///   2. Write content H to lba 1 → flush → drain: segment S2 DEDUP_REF(lba=1, H)
    ///   3. GC compacts S1 + S2: lba 1's DEDUP_REF was mis-converted to DATA(len=0)
    ///   4. Crash + reopen: extent index holds zero-length DATA entry for H
    ///   5. Read lba 0 or lba 1 → EIO
    #[tokio::test]
    async fn gc_dedup_ref_preserved_across_compaction() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        // Set up volume keypair (needed by Volume::open and apply_done_handoffs).
        elide_core::signing::generate_keypair(
            dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();

        let content = [0xAAu8; 4096];

        // Step 1: write [0xAA; 4096] to lba 0, flush, redact, drain.
        // Produces S1: DATA(lba=0, hash=H_aa, body=[0xAA; 4096]).
        vol.write(0, &content).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        // Step 2: write the same content to lba 1, flush, redact, drain.
        // Same hash H_aa → the write path emits DedupRef(lba=1, H_aa) in S2,
        // carried through unchanged by the thin-DedupRef format.
        vol.write(1, &content).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        drop(vol);

        // Step 3: run the real coordinator GC.
        // density_threshold=0.0 admits all segments to sweep; the test
        // segments are well below SWEEP_SMALL_THRESHOLD so they pack into
        // tier 1 directly.
        let config = crate::config::GcConfig {
            density_threshold: 0.0,
            interval_secs: 0,
        };
        let u_gc = Ulid::new();
        let stats = gc_fork(dir, dir.parent().unwrap(), &config, u_gc).unwrap();
        assert!(
            stats.candidates >= 2,
            "GC should have compacted at least 2 segments (S1 + S2), got {}",
            stats.candidates
        );

        // Step 4: volume applies the handoff — re-signs the GC segment and
        // updates the in-memory extent index.
        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();
        let applied = vol.apply_gc_handoffs().unwrap();
        assert!(applied > 0, "GC handoff should have been applied");
        drop(vol);

        // Step 5: coordinator completes the handoff — uploads to S3, sends
        // promote IPC to volume (mock), deletes gc/<new>, renames .applied → .done.
        let _mock = spawn_mock_socket(dir.to_owned()).await;
        let done = apply_done_handoffs(
            dir,
            "test-vol",
            &store,
            crate::upload::DEFAULT_PART_SIZE_BYTES,
        )
        .await
        .unwrap();
        assert!(
            done > 0,
            "apply_done_handoffs should have processed the handoff"
        );

        // Step 6: crash + reopen — rebuild lbamap and extent index from disk.
        // Before the fix, extentindex::rebuild inserted a zero-length DATA entry
        // for H_aa (from the mis-converted DEDUP_REF in the GC output), causing
        // reads to seek past EOF with EIO.
        let vol = elide_core::volume::Volume::open(dir, dir).unwrap();

        assert_eq!(
            vol.read(0, 1).unwrap().as_slice(),
            &content,
            "lba 0 must read back [0xAA; 4096] after GC + crash + reopen"
        );
        assert_eq!(
            vol.read(1, 1).unwrap().as_slice(),
            &content,
            "lba 1 must read back [0xAA; 4096] after GC + crash + reopen"
        );
    }

    /// Step 2 of the self-describing GC handoff: the compacted segment's
    /// index section must carry the sorted list of source segment ULIDs that
    /// fed this output, so the volume can later derive the apply set from
    /// the segment itself without consulting a sidecar manifest.
    #[tokio::test]
    async fn gc_output_records_input_ulids() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        elide_core::signing::generate_keypair(
            dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();
        // Two distinct payloads so each drain produces its own segment.
        vol.write(0, &[0x11u8; 4096]).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        vol.write(1, &[0x22u8; 4096]).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;
        drop(vol);

        // Capture the input segment ULIDs from index/ before GC runs — those
        // are the candidates gc_fork will sweep.
        let index_dir = dir.join("index");
        let mut expected_inputs: Vec<Ulid> = fs::read_dir(&index_dir)
            .unwrap()
            .flatten()
            .filter_map(|e| {
                let name = e.file_name();
                let name = name.to_str()?;
                let stem = name.strip_suffix(".idx")?;
                Ulid::from_string(stem).ok()
            })
            .collect();
        expected_inputs.sort();
        assert!(
            expected_inputs.len() >= 2,
            "two drained segments expected, got {}",
            expected_inputs.len()
        );

        // Sweep both under a permissive density threshold.
        let config = crate::config::GcConfig {
            density_threshold: 0.0,
            interval_secs: 0,
        };
        let u_gc = Ulid::new();
        gc_fork(dir, dir.parent().unwrap(), &config, u_gc).unwrap();

        // Find the emitted GC plan in gc/. Under the plan handoff protocol
        // the coordinator writes `gc/<ulid>.plan` instead of a signed
        // staged segment.
        let gc_dir = dir.join("gc");
        let plan_path = fs::read_dir(&gc_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|s| s.to_str()) == Some("plan"))
            .expect("gc/ must contain a .plan file");
        let plan = elide_core::gc_plan::GcPlan::read(&plan_path).unwrap();

        assert_eq!(
            plan.inputs(),
            expected_inputs,
            "gc plan must list all swept source ulids in sorted order"
        );
    }

    /// Bug F: DATA entry at a non-canonical extent location must be kept when
    /// its LBA mapping is live.
    ///
    /// Two segments with the same hash for different LBAs: S1 has DATA(LBA 0→H),
    /// S2 has DedupRef(LBA 1→H).  The extent_index maps H→S2 (S2 processed
    /// last).  collect_stats for S1 should still keep the DATA entry via lba_live.
    #[test]
    fn collect_stats_keeps_data_entry_when_lba_live_but_not_extent_canonical() {
        let dir = TempDir::new().unwrap();
        let fork_dir = dir.path();

        elide_core::signing::generate_keypair(
            fork_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        let vk =
            elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
                .unwrap();

        let mut vol = elide_core::volume::Volume::open(fork_dir, fork_dir).unwrap();

        // S1: DATA(LBA 0→H101)
        vol.write(0, &[101u8; 4096]).unwrap();
        vol.flush_wal().unwrap();

        // S2: DedupRef(LBA 1→H101) — same hash, write-path dedup
        vol.write(1, &[101u8; 4096]).unwrap();
        vol.flush_wal().unwrap();

        // Drain both: redact in place, then promote.
        let pending_dir = fork_dir.join("pending");
        let mut ulids: Vec<ulid::Ulid> = Vec::new();
        for entry in fs::read_dir(&pending_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name_str = name.to_str().unwrap();
            if name_str.contains('.') {
                continue;
            }
            if let Ok(ulid) = ulid::Ulid::from_string(name_str) {
                ulids.push(ulid);
            }
        }
        ulids.sort();
        for ulid in &ulids {
            vol.redact_segment(*ulid).unwrap();
            vol.promote_segment(*ulid).unwrap();
        }

        // Rebuild from disk — extent_index has H101→S2 (S2 processed last).
        let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
        let index = extentindex::rebuild(&rebuild_chain).unwrap();
        let lbamap = lbamap::rebuild_segments(&rebuild_chain).unwrap();
        let live_hashes = lbamap.lba_referenced_hashes();

        let stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, None).unwrap();

        // Both segments should have 1 live entry each.
        // S1: DATA(LBA 0→H101) — not extent-canonical (H101→S2), but lba_live.
        // S2: DedupRef(LBA 1→H101) — lba_live.
        let total_live: usize = stats.iter().map(|s| s.live_entries.len()).sum();
        assert_eq!(
            total_live,
            2,
            "both LBA mappings must survive: got {} live entries across {} segments \
             (segments: {:?})",
            total_live,
            stats.len(),
            stats
                .iter()
                .map(|s| format!(
                    "{}:live={},removed={}",
                    &s.ulid_str[..8],
                    s.live_entries.len(),
                    s.removed_hashes.len()
                ))
                .collect::<Vec<_>>()
        );
    }

    /// Bug H reproduction (DIRECT collect_stats level):
    ///
    /// `collect_stats` keeps a DATA entry at its original `start_lba` when
    /// the LBA has been overwritten with a different hash *and* the entry's
    /// hash is still live somewhere else (via a DedupRef at a different
    /// LBA). The preserved entry then carries a spurious LBA→hash binding
    /// forward into the GC output; when the output lands in `index/` with
    /// a ULID higher than whichever segment currently writes the correct
    /// LBA binding, the output shadows it on rebuild — silent data loss.
    ///
    /// Scenario:
    ///   - S1: DATA h2 at LBA 0. Canonical for h2.
    ///   - S2: DedupRef h2 at LBA 1. Keeps h2 in `live_hashes`.
    ///   - S3: DATA h1 at LBA 0 (new hash, distinct 4 KiB). Overwrites the
    ///     LBA mapping so `lbamap[0] = h1`. S1's entry at LBA 0 is now
    ///     **LBA-dead**.
    ///
    /// Expected behaviour (post-fix): S1's GC stats either:
    ///   (a) do not include a live entry at LBA 0 with hash h2, or
    ///   (b) include one demoted to a kind that makes no LBA claim on
    ///       rebuild (`EntryKind::CanonicalBody`).
    ///
    /// Current (buggy) behaviour: S1's `live_entries` contains the DATA
    /// entry at `start_lba=0` with hash h2. Test asserts the entry does
    /// not carry an LBA-claiming shape — which fails today and will pass
    /// once the fix lands.
    #[test]
    fn collect_stats_preserves_dead_lba_entry_when_hash_live_elsewhere() {
        let dir = TempDir::new().unwrap();
        let fork_dir = dir.path();

        elide_core::signing::generate_keypair(
            fork_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        let vk =
            elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
                .unwrap();

        let mut vol = elide_core::volume::Volume::open(fork_dir, fork_dir).unwrap();

        // Use high-entropy content so writes land as DATA (not INLINE).
        // Two distinct blocks whose hashes differ.
        let mut h2_bytes = [0u8; 4096];
        for (i, b) in h2_bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(5);
        }
        let mut h1_bytes = [0u8; 4096];
        for (i, b) in h1_bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(59).wrapping_add(11);
        }
        assert_ne!(
            blake3::hash(&h1_bytes),
            blake3::hash(&h2_bytes),
            "test precondition"
        );

        // S1: DATA h2 at LBA 0. Canonical for h2.
        vol.write(0, &h2_bytes).unwrap();
        vol.flush_wal().unwrap();

        // S2: DedupRef h2 at LBA 1. (write_commit takes the REF path since
        // h2 is already in extent_index from S1.)
        vol.write(1, &h2_bytes).unwrap();
        vol.flush_wal().unwrap();

        // S3: DATA h1 at LBA 0 (distinct hash). Overwrites lbamap[0].
        // S1's entry at LBA 0 is now LBA-dead.
        vol.write(0, &h1_bytes).unwrap();
        vol.flush_wal().unwrap();

        // Drain: promote each pending/<ulid> to index/+cache/.
        let pending_dir = fork_dir.join("pending");
        let mut ulids: Vec<ulid::Ulid> = Vec::new();
        for entry in fs::read_dir(&pending_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name_str = name.to_str().unwrap();
            if name_str.contains('.') {
                continue;
            }
            if let Ok(ulid) = ulid::Ulid::from_string(name_str) {
                ulids.push(ulid);
            }
        }
        ulids.sort();
        for ulid in &ulids {
            vol.redact_segment(*ulid).unwrap();
            vol.promote_segment(*ulid).unwrap();
        }
        let s1_ulid = ulids[0].to_string();

        // Rebuild state + compute stats.
        let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
        let index = extentindex::rebuild(&rebuild_chain).unwrap();
        let lbamap = lbamap::rebuild_segments(&rebuild_chain).unwrap();
        let live_hashes = lbamap.lba_referenced_hashes();

        // Preconditions — match the scenario assumptions.
        let h1 = blake3::hash(&h1_bytes);
        let h2 = blake3::hash(&h2_bytes);
        assert_eq!(lbamap.hash_at(0), Some(h1), "LBA 0 must be h1");
        assert_eq!(lbamap.hash_at(1), Some(h2), "LBA 1 must be h2");
        assert!(live_hashes.contains(&h2), "h2 must be live (via LBA 1)");

        let stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, None).unwrap();

        // Find S1's stats.
        let s1_stats = stats
            .iter()
            .find(|s| s.ulid_str == s1_ulid)
            .expect("S1 must be in collect_stats output");

        // Core assertion — for every live entry in S1, check whether it
        // spuriously re-asserts an LBA mapping that the current lbamap
        // doesn't agree with.  If yes, the entry would poison rebuild.
        //
        // Before fix: this assertion fails.  S1's live_entries contains
        // a DATA entry with start_lba=0, lba_length=1, hash=h2, but
        // lbamap[0]=h1 — the mapping shadows the correct one.
        //
        // After fix: either the entry is absent from live_entries, or
        // it survives with a kind that rebuild skips (e.g. CanonicalBody,
        // once added).
        for entry in &s1_stats.live_entries {
            if matches!(entry.kind, EntryKind::Data | EntryKind::Inline) && entry.lba_length > 0 {
                let lba_hash = lbamap.hash_at(entry.start_lba);
                assert_eq!(
                    lba_hash,
                    Some(entry.hash),
                    "S1 live entry {:?} at start_lba={} hash={} claims an LBA \
                     whose current mapping is {:?} — would shadow the correct \
                     binding on rebuild (bug H).",
                    entry.kind,
                    entry.start_lba,
                    entry.hash.to_hex(),
                    lba_hash.map(|h| h.to_hex().to_string()),
                );
            }
        }
    }

    /// Bug I reproduction (DIRECT collect_stats level):
    ///
    /// `collect_stats` samples Zero-entry liveness with a single
    /// `hash_at(start_lba)` point query. A Zero entry written as one large
    /// TRIM (e.g. 4096 blocks) can have its **middle** overwritten by a
    /// later Data write while the first LBA still reads ZERO_HASH. The
    /// current check sees the whole Zero entry as live and re-emits it
    /// unchanged at the GC-output ULID — which is higher than both the
    /// original Zero segment and whichever segment now carries the real
    /// Data. On disk rebuild the re-emitted Zero shadows the real Data.
    ///
    /// Scenario:
    ///   - S1: Zero[0..4096). lbamap covers 0..4096 with ZERO_HASH.
    ///   - S2: Data h1 at LBA 2000. lbamap splits: 0..2000 → ZERO,
    ///     2000..2001 → h1, 2001..4096 → ZERO.
    ///
    /// Expected (post-fix): S1's live_entries contains Zero sub-runs only
    /// for the surviving ZERO_HASH ranges — no single entry spans LBA 2000
    /// where the current mapping is h1.
    ///
    /// Current (buggy) behaviour: S1's live_entries contains one Zero
    /// entry at start_lba=0, lba_length=4096 — covering LBA 2000 where
    /// lbamap says h1. Test assertion fails until the fix lands.
    #[test]
    fn collect_stats_trims_zero_entry_to_surviving_subranges() {
        let dir = TempDir::new().unwrap();
        let fork_dir = dir.path();

        elide_core::signing::generate_keypair(
            fork_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        let vk =
            elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
                .unwrap();

        let mut vol = elide_core::volume::Volume::open(fork_dir, fork_dir).unwrap();

        let mut h1_bytes = [0u8; 4096];
        for (i, b) in h1_bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(59).wrapping_add(11);
        }
        let h1 = blake3::hash(&h1_bytes);

        // S1: large Zero covering LBAs 0..4096.
        vol.write_zeroes(0, 4096).unwrap();
        vol.flush_wal().unwrap();

        // S2: Data h1 at LBA 2000 — punches a hole in the middle of S1's Zero.
        vol.write(2000, &h1_bytes).unwrap();
        vol.flush_wal().unwrap();

        // Drain pending → index/+cache/.
        let pending_dir = fork_dir.join("pending");
        let mut ulids: Vec<ulid::Ulid> = Vec::new();
        for entry in fs::read_dir(&pending_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name_str = name.to_str().unwrap();
            if name_str.contains('.') {
                continue;
            }
            if let Ok(ulid) = ulid::Ulid::from_string(name_str) {
                ulids.push(ulid);
            }
        }
        ulids.sort();
        for ulid in &ulids {
            vol.redact_segment(*ulid).unwrap();
            vol.promote_segment(*ulid).unwrap();
        }
        let s1_ulid = ulids[0].to_string();

        // Rebuild state + compute stats.
        let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
        let index = extentindex::rebuild(&rebuild_chain).unwrap();
        let lbamap = lbamap::rebuild_segments(&rebuild_chain).unwrap();
        let live_hashes = lbamap.lba_referenced_hashes();

        // Preconditions.
        assert_eq!(
            lbamap.hash_at(0),
            Some(elide_core::volume::ZERO_HASH),
            "LBA 0 must still be zero"
        );
        assert_eq!(lbamap.hash_at(2000), Some(h1), "LBA 2000 must be h1");
        assert_eq!(
            lbamap.hash_at(3000),
            Some(elide_core::volume::ZERO_HASH),
            "LBA 3000 must still be zero"
        );

        let stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, None).unwrap();

        let s1_stats = stats
            .iter()
            .find(|s| s.ulid_str == s1_ulid)
            .expect("S1 must be in collect_stats output");

        // For every Zero live_entry in S1, the *entire* covered range must
        // still map to ZERO_HASH in the lbamap. Before the fix, the single
        // Zero entry spans LBA 2000 where lbamap = h1 — assertion fails.
        for entry in &s1_stats.live_entries {
            if entry.kind != EntryKind::Zero {
                continue;
            }
            let end = entry.start_lba + entry.lba_length as u64;
            for lba in entry.start_lba..end {
                assert_eq!(
                    lbamap.hash_at(lba),
                    Some(elide_core::volume::ZERO_HASH),
                    "S1 Zero entry [{}+{}) covers LBA {} whose current mapping \
                     is {:?} — would shadow the correct binding on rebuild (bug I).",
                    entry.start_lba,
                    entry.lba_length,
                    lba,
                    lbamap.hash_at(lba).map(|h| h.to_hex().to_string()),
                );
            }
        }
    }

    /// Helper: drain pending/ into index/+cache/ in ULID order, return the sorted ULIDs.
    fn drain_pending(
        fork_dir: &std::path::Path,
        vol: &mut elide_core::volume::Volume,
    ) -> Vec<ulid::Ulid> {
        let pending_dir = fork_dir.join("pending");
        let mut ulids: Vec<ulid::Ulid> = Vec::new();
        for entry in fs::read_dir(&pending_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name_str = name.to_str().unwrap();
            if name_str.contains('.') {
                continue;
            }
            if let Ok(ulid) = ulid::Ulid::from_string(name_str) {
                ulids.push(ulid);
            }
        }
        ulids.sort();
        for ulid in &ulids {
            vol.redact_segment(*ulid).unwrap();
            vol.promote_segment(*ulid).unwrap();
        }
        ulids
    }

    /// Helper: assert the two rebuild invariants for S1 after collect_stats.
    ///
    /// If `has_partial_death` is set (DedupRef / Delta partial-death), the
    /// segment is excluded from this GC pass by `gc_fork`: it stays on disk
    /// with its original entries intact, and rebuild applies it in ULID order
    /// correctly. No further checks needed.
    ///
    /// Otherwise two invariants must hold across **post-expansion output** —
    /// for Data/Inline partial-death, the bloated live entry is replaced by
    /// `compact_segments` with a canonical-only + one sub-run entry per
    /// `partial_death_runs` slot, so the check consults those sub-runs
    /// instead of the original entry's `(start_lba, lba_length)`:
    ///
    /// 1. **Shadow.** Every entry that will be emitted at the output ULID
    ///    must agree with the current lbamap across its claimed range.
    ///    Otherwise rebuild at a higher ULID re-asserts a stale mapping and
    ///    overwrites the correct binding.
    ///
    /// 2. **Loss.** Every LBA within S1's original range that currently
    ///    resolves to S1's hash must be covered by some emitted non-
    ///    canonical-only entry with that hash.
    fn assert_no_shadow_or_loss_on_rebuild(
        s1_stats: &SegmentStats,
        lbamap: &elide_core::lbamap::LbaMap,
        original_range: std::ops::Range<u64>,
        original_hash: blake3::Hash,
        shape: &str,
    ) {
        if s1_stats.has_partial_death {
            return;
        }
        // Invariant 1: no shadow.
        for (entry, sub_runs) in s1_stats
            .live_entries
            .iter()
            .zip(s1_stats.partial_death_runs.iter())
        {
            if !matches!(
                entry.kind,
                EntryKind::Data | EntryKind::Inline | EntryKind::Delta
            ) {
                continue;
            }
            if entry.kind.is_canonical_only() || entry.lba_length == 0 {
                continue;
            }
            if let Some(runs) = sub_runs {
                // Partial-death Data/Inline: `compact_segments` will replace
                // this entry with the listed sub-runs (each hashed to its
                // own slice). Check shadow against each sub-run's claim.
                for run in runs.iter() {
                    for lba in run.range_start..run.range_end {
                        assert_eq!(
                            lbamap.hash_at(lba),
                            Some(run.hash),
                            "[{shape}] shadow in partial-death sub-run: S1 entry \
                             at [{}+{}) → sub-run [{}+{}) hash={} claims LBA {} \
                             whose current lbamap mapping is {:?}",
                            entry.start_lba,
                            entry.lba_length,
                            run.range_start,
                            run.range_end - run.range_start,
                            run.hash.to_hex(),
                            lba,
                            lbamap.hash_at(lba).map(|h| h.to_hex().to_string()),
                        );
                    }
                }
                continue;
            }
            let end = entry.start_lba + entry.lba_length as u64;
            for lba in entry.start_lba..end {
                assert_eq!(
                    lbamap.hash_at(lba),
                    Some(entry.hash),
                    "[{shape}] shadow: S1 live entry {:?} at [{}+{}) hash={} \
                     claims LBA {} whose current lbamap mapping is {:?} — \
                     would shadow the correct binding on rebuild.",
                    entry.kind,
                    entry.start_lba,
                    entry.lba_length,
                    entry.hash.to_hex(),
                    lba,
                    lbamap.hash_at(lba).map(|h| h.to_hex().to_string()),
                );
            }
        }

        // Invariant 2: no loss. Count emitted claims for `original_hash`
        // from both non-partial live entries and the sub-runs that will
        // replace partial-death ones.
        let mut claimed = std::collections::HashSet::new();
        for (entry, sub_runs) in s1_stats
            .live_entries
            .iter()
            .zip(s1_stats.partial_death_runs.iter())
        {
            if entry.kind.is_canonical_only() {
                continue;
            }
            if let Some(runs) = sub_runs {
                for run in runs.iter() {
                    if run.hash == original_hash {
                        for lba in run.range_start..run.range_end {
                            claimed.insert(lba);
                        }
                    }
                }
                continue;
            }
            if entry.hash != original_hash {
                continue;
            }
            if !matches!(
                entry.kind,
                EntryKind::Data | EntryKind::Inline | EntryKind::Delta
            ) {
                continue;
            }
            for lba in entry.start_lba..(entry.start_lba + entry.lba_length as u64) {
                claimed.insert(lba);
            }
        }
        for lba in original_range {
            if lbamap.hash_at(lba) == Some(original_hash) {
                assert!(
                    claimed.contains(&lba),
                    "[{shape}] loss: LBA {} still resolves to hash {} but no \
                     emitted S1 entry claims it — rebuild would lose this binding \
                     once S1's input segment is consumed.",
                    lba,
                    original_hash.to_hex(),
                );
            }
        }
    }

    /// Scenario for a multi-LBA Data entry overlapped by a later single-LBA
    /// write at position `overlap_off`.
    ///
    /// S1 writes 4 LBAs of high-entropy content at LBA 100 → one DATA entry
    /// with lba_length=4, hash=H.
    /// S2 writes 1 LBA of different content at LBA (100 + overlap_off).
    /// After both, lbamap splits S1's entry into sub-runs via
    /// `payload_block_offset` aliasing. No later segment on disk ever
    /// re-records the surviving H sub-run(s) as first-class claims.
    ///
    /// Bug: `collect_stats` sees S1's entry, point-queries hash_at(100)
    /// (still H except when overlap_off=0), and either keeps the whole
    /// (100, 4, H) claim intact or demotes to canonical_only. In both
    /// cases the GC output at a higher ULID shadows the correct binding
    /// on rebuild.
    fn run_multi_lba_overlap_scenario(overlap_off: u64, shape: &'static str) {
        let dir = TempDir::new().unwrap();
        let fork_dir = dir.path();

        elide_core::signing::generate_keypair(
            fork_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        let vk =
            elide_core::signing::load_verifying_key(fork_dir, elide_core::signing::VOLUME_PUB_FILE)
                .unwrap();

        let mut vol = elide_core::volume::Volume::open(fork_dir, fork_dir).unwrap();

        // 4 LBAs worth of high-entropy content, one hash.
        let mut h_bytes = vec![0u8; 4 * 4096];
        for (i, b) in h_bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(5);
        }
        let h = blake3::hash(&h_bytes);

        // Single-LBA overwriting content, distinct hash.
        let mut w_bytes = [0u8; 4096];
        for (i, b) in w_bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(59).wrapping_add(11);
        }
        let w = blake3::hash(&w_bytes);
        assert_ne!(h, w, "test precondition: H and W must differ");

        // S1: multi-LBA Data at LBA 100..104.
        vol.write(100, &h_bytes).unwrap();
        vol.flush_wal().unwrap();

        // S2: single-LBA Data at LBA (100 + overlap_off).
        vol.write(100 + overlap_off, &w_bytes).unwrap();
        vol.flush_wal().unwrap();

        let ulids = drain_pending(fork_dir, &mut vol);
        let s1_ulid = ulids[0].to_string();

        let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
        let index = extentindex::rebuild(&rebuild_chain).unwrap();
        let lbamap = lbamap::rebuild_segments(&rebuild_chain).unwrap();
        let live_hashes = lbamap.lba_referenced_hashes();

        // Preconditions: S2 landed at its LBA with hash w, and at least one
        // LBA in S1's range still resolves to h.
        assert_eq!(
            lbamap.hash_at(100 + overlap_off),
            Some(w),
            "[{shape}] LBA {} must be w",
            100 + overlap_off
        );
        let mut surviving_h = 0;
        for lba in 100u64..104 {
            if lbamap.hash_at(lba) == Some(h) {
                surviving_h += 1;
            }
        }
        assert!(
            surviving_h > 0,
            "[{shape}] test precondition: at least one LBA of S1's range must still resolve to h"
        );

        let stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, None).unwrap();

        let s1_stats = stats
            .iter()
            .find(|s| s.ulid_str == s1_ulid)
            .expect("S1 must be in collect_stats output");

        assert_no_shadow_or_loss_on_rebuild(s1_stats, &lbamap, 100..104, h, shape);
    }

    /// Bug (tail overwrite): S1 has DATA H at [100, 4). S2 overwrites LBA
    /// 103 with W. `hash_at(100) = H` → point query says LIVE → S1's
    /// whole entry kept intact → GC output at higher ULID re-asserts
    /// (100, 4, H), shadowing S2's W at LBA 103 on rebuild.
    ///
    /// Expected (post-fix): S1 is skipped (partially alive; segment
    /// ineligible for this compaction pass) OR the emitted claim
    /// covers only the surviving LBAs 100-102.
    #[test]
    fn collect_stats_skips_entry_with_tail_overwrite() {
        run_multi_lba_overlap_scenario(3, "tail");
    }

    /// Bug (interior overwrite): S1 has DATA H at [100, 4). S2 overwrites
    /// LBA 102 with W. `hash_at(100) = H` → point query says LIVE → whole
    /// entry kept intact → GC output shadows S2 on rebuild.
    ///
    /// Expected (post-fix): S1 is skipped or its emitted claim covers only
    /// LBAs whose current mapping is H.
    #[test]
    fn collect_stats_skips_entry_with_interior_overwrite() {
        run_multi_lba_overlap_scenario(2, "interior");
    }

    /// Bug (head overwrite): S1 has DATA H at [100, 4). S2 overwrites LBA
    /// 100 with W. `hash_at(100) = W` → point query says DEAD → falls into
    /// `canonical_only` demotion (because H is still live via the surviving
    /// tail 101..104). S1's disk segment is consumed. The surviving tail
    /// [101, 3, H) existed only as a runtime `payload_block_offset` alias
    /// inside S1's original entry — with S1 gone and no new segment
    /// re-asserting the tail claim, rebuild reads LBAs 101-103 as zero.
    /// Silent data loss.
    ///
    /// Expected (post-fix): S1 is skipped (partially alive; keep the
    /// bloated entry on disk so rebuild's ULID-order application still
    /// splits it correctly via the existing lbamap split-on-overwrite).
    #[test]
    fn collect_stats_skips_entry_with_head_overwrite() {
        run_multi_lba_overlap_scenario(0, "head");
    }

    /// End-to-end partial-death compaction of a multi-LBA Delta entry.
    ///
    /// Hand-crafts a 4-LBA Delta whose composite body deltas against a
    /// separately-written parent DATA entry, then overwrites one interior
    /// LBA (102). Runs gc_fork, applies the handoff on the volume side,
    /// and verifies reads return the reconstructed composite bytes for
    /// the surviving sub-runs and the overwrite bytes for LBA 102. No
    /// socket handoff: the test stops before `apply_done_handoffs` so
    /// the segment stays in bare `gc/<new>` on disk (still readable via
    /// `locate_segment_body`).
    ///
    /// Without step 3b the Delta segment would be deferred
    /// (`has_partial_death = true`) and no compaction output would be
    /// produced — the assertions on the GC output and reconstructed
    /// reads would both fail.
    #[tokio::test]
    async fn gc_delta_partial_death_compaction() {
        use elide_core::segment::{DeltaOption, write_segment_with_delta_body};

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        elide_core::signing::generate_keypair(
            dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        let signer =
            elide_core::signing::load_signer(dir, elide_core::signing::VOLUME_KEY_FILE).unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Parent body: 4 LBAs of high-entropy content. The whole body
        // (16 KiB) serves as the zstd dictionary for the child delta.
        let mut parent_bytes = vec![0u8; 4 * 4096];
        for (i, b) in parent_bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(37).wrapping_add(5);
        }
        let parent_hash = blake3::hash(&parent_bytes);

        // Child body: structurally similar to parent but with block-0
        // bytes swapped out, so the zstd-dict delta stays small.
        let mut child_bytes = parent_bytes.clone();
        for b in &mut child_bytes[0..256] {
            *b = 0xCC;
        }
        let child_hash = blake3::hash(&child_bytes);
        assert_ne!(parent_hash, child_hash);

        // Overwrite bytes for LBA 102.
        let overwrite_bytes = vec![0xFFu8; 4096];

        // ── S1: parent DATA at LBA 0..4, via the normal write path.
        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();
        vol.write(0, &parent_bytes).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        // ── S2: multi-LBA Delta at LBA 100..104, hand-crafted. The child
        //        body is zstd-dict compressed against the parent as
        //        dictionary and stored as the segment's delta body.
        let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &parent_bytes).unwrap();
        let delta_blob = compressor.compress(&child_bytes).unwrap();
        let delta_ulid = Ulid::new();
        let delta_pending = dir.join("pending").join(delta_ulid.to_string());
        let opt = DeltaOption {
            source_hash: parent_hash,
            delta_offset: 0,
            delta_length: delta_blob.len() as u32,
            delta_hash: blake3::hash(&delta_blob),
        };
        let mut delta_entries = vec![SegmentEntry::new_delta(child_hash, 100, 4, vec![opt])];
        write_segment_with_delta_body(
            &delta_pending,
            &mut delta_entries,
            &delta_blob,
            signer.as_ref(),
        )
        .unwrap();
        let bytes = fs::read(&delta_pending).unwrap();
        let key = segment_key("test-vol", &delta_ulid.to_string()).unwrap();
        store
            .put(&key, bytes::Bytes::from(bytes).into())
            .await
            .unwrap();
        vol.promote_segment(delta_ulid).unwrap();

        // ── S3: single-LBA overwrite at LBA 102, via the normal path.
        vol.write(102, &overwrite_bytes).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        drop(vol);

        // ── Run coordinator GC. Must NOT defer the Delta segment.
        let config = crate::config::GcConfig {
            density_threshold: 0.0,
            interval_secs: 0,
        };
        let u_gc = Ulid::new();
        let stats = gc_fork(dir, dir.parent().unwrap(), &config, u_gc).unwrap();
        assert_eq!(
            stats.deferred, 0,
            "Delta partial-death must be expanded in-band, not deferred"
        );
        assert!(
            stats.candidates >= 1,
            "GC must compact at least the Delta segment, got candidates={}",
            stats.candidates
        );

        // ── Apply the handoff on the volume side: .staged → bare
        //    gc/<new>, re-signed with the volume key. This is socket-free.
        //    Reopening the volume after apply rebuilds the index off the
        //    bare gc/<new> file plus any remaining index/<input>.idx.
        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();
        let applied = vol.apply_gc_handoffs().unwrap();
        assert!(applied > 0, "GC handoff must be applied");
        drop(vol);

        // ── Reopen and read. Sub-run LBAs must return the corresponding
        //    slice of the reconstructed composite; LBA 102 must return
        //    the overwrite bytes. Before step 3b the Delta segment was
        //    deferred, apply_gc_handoffs was a no-op, and reads for
        //    LBAs 100/101/103 went through the live Delta entry (which
        //    still worked — but the GC defer blocked reclamation). This
        //    test's value is in the `stats.deferred == 0` assertion plus
        //    confirming reads still round-trip after the Delta segment
        //    has been replaced by per-sub-run DATA entries.
        let vol = elide_core::volume::Volume::open(dir, dir).unwrap();
        assert_eq!(
            vol.read(100, 1).unwrap().as_slice(),
            &child_bytes[0..4096],
            "LBA 100 must return child_bytes[0..4096]"
        );
        assert_eq!(
            vol.read(101, 1).unwrap().as_slice(),
            &child_bytes[4096..8192],
            "LBA 101 must return child_bytes[4096..8192]"
        );
        assert_eq!(
            vol.read(102, 1).unwrap().as_slice(),
            overwrite_bytes.as_slice(),
            "LBA 102 must return the overwrite bytes"
        );
        assert_eq!(
            vol.read(103, 1).unwrap().as_slice(),
            &child_bytes[12288..16384],
            "LBA 103 must return child_bytes[12288..16384]"
        );
    }

    /// Regression: GC compactor must preserve the COMPRESSED flag on inline
    /// entries.  Without it, the GC output contains a tiny compressed blob
    /// marked as uncompressed; the read path skips decompression and tries to
    /// index 4096 bytes from a ~20-byte buffer → "inline payload too short".
    ///
    /// Sequence:
    ///   1. Write all-same-byte block (compresses below INLINE_THRESHOLD → inline)
    ///   2. Drain (redact + promote): segment with Inline entry in S3
    ///   3. GC compacts: output segment must carry compressed flag on inline data
    ///   4. Volume applies handoff + crash + reopen
    ///   5. Read must succeed — data served from inline section in .idx
    #[tokio::test]
    async fn gc_inline_entry_preserves_compressed_flag() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();

        elide_core::signing::generate_keypair(
            dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();

        // All-same-byte block compresses to ~20 bytes → inline.
        let block = [0xBBu8; 4096];
        vol.write(0, &block).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        // Write a second segment so GC has ≥2 candidates to sweep.
        let block2 = [0xCCu8; 4096];
        vol.write(1, &block2).unwrap();
        vol.flush_wal().unwrap();
        drain_with_redact(&mut vol, dir, "test-vol", &store).await;

        drop(vol);

        // GC: compact both segments.
        let config = crate::config::GcConfig {
            density_threshold: 0.0,
            interval_secs: 0,
        };
        let u_gc = Ulid::new();
        let stats = gc_fork(dir, dir.parent().unwrap(), &config, u_gc).unwrap();
        assert!(
            stats.candidates >= 2,
            "GC should compact ≥2 segments, got {}",
            stats.candidates
        );

        // Volume applies handoff.
        let mut vol = elide_core::volume::Volume::open(dir, dir).unwrap();
        let applied = vol.apply_gc_handoffs().unwrap();
        assert!(applied > 0, "GC handoff should have been applied");
        drop(vol);

        // Coordinator completes: upload + promote.
        let _mock = spawn_mock_socket(dir.to_owned()).await;
        let done = apply_done_handoffs(
            dir,
            "test-vol",
            &store,
            crate::upload::DEFAULT_PART_SIZE_BYTES,
        )
        .await
        .unwrap();
        assert!(done > 0, "handoff should complete");

        // Crash + reopen — rebuild from index/*.idx.
        let vol = elide_core::volume::Volume::open(dir, dir).unwrap();

        // Before the fix: "corrupt segment: inline payload too short"
        assert_eq!(
            vol.read(0, 1).unwrap().as_slice(),
            &block,
            "lba 0 must survive GC + crash + reopen (inline entry)"
        );
        assert_eq!(
            vol.read(1, 1).unwrap().as_slice(),
            &block2,
            "lba 1 must survive GC + crash + reopen (inline entry)"
        );
    }
}
