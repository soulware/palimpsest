// Coordinator-side S3 segment GC.
//
// Two strategies, matching lsvd's StartGC / SweepSmallSegments model and the
// volume's repack() / sweep_pending() split:
//
//   Repack pass:
//     Find the single least-dense segment (lowest live_bytes/file_bytes ratio).
//     If it is below the density threshold, repack it into one output segment.
//     Processes one segment per tick — the next tick handles the next one.
//     Mirrors lsvd StartGC and volume repack().
//
//   Sweep:
//     Bin-pack high-density segments toward SWEEP_LIVE_CAP (32 MiB live).
//     Tier 1 sorts segments with live_lba_bytes <= SWEEP_SMALL_THRESHOLD
//     (16 MiB) ascending and greedy-includes them. Tier 2 picks at most
//     one larger filler that fits the remaining headroom. Sweep candidates
//     must have density >= density_threshold; lower-density segments are
//     owned by repack and excluded here. Selection is purely about packing,
//     not dead-data removal — repack handles that, gated on density.
//     Mirrors lsvd SweepSmallSegments and volume sweep_pending().
//
// Both strategies run in the same tick if both find candidates, each producing
// an independent output segment with its own ULID. They operate on disjoint
// input sets, so they could be parallelised with tokio::join! in a future
// optimisation. Per-tick work is bounded in both cases.
//
// Handoff protocol (self-describing, crash-safe, filesystem-only coordination —
// see docs/design-gc-self-describing-handoff.md for the full design):
//
//   1. Coordinator writes the compacted segment to gc/<new-ulid>.staged via
//      tmp+rename. The segment carries the sorted list of input ULIDs in its
//      own header (`inputs_length` field). Signed with an ephemeral key —
//      coordinator does not hold the volume's private key.
//
//   2. Volume (idle tick) reads gc/<new-ulid>.staged, walks each input's
//      index/<input>.idx to derive the extent-index updates, writes a
//      re-signed copy to gc/<new-ulid>.tmp, renames .tmp → bare gc/<new-ulid>
//      (the atomic commit point), removes .staged.
//
//   3. Coordinator (next GC tick) sees the bare file: uploads it to S3, sends
//      promote_segment IPC (volume writes index/<new>.idx + cache/<new>.body
//      and deletes index/<input>.idx for each input), deletes old S3 objects,
//      sends finalize_gc_handoff IPC (volume deletes the bare body).
//
//   Crash recovery is content-resolved (no extra filename states): stale .tmp
//   and .staged.tmp are swept on every apply pass; .staged alone re-runs
//   apply (deterministic, byte-identical output); .staged + bare → bare wins.
//
//   All-dead and removal-only handoffs collapse into a zero-entry GC output
//   with a non-empty inputs list. promote_segment recognises this shape and
//   skips writing index/<new>.idx / cache/<new>.body — the bare file is then
//   deleted via finalize_gc_handoff the same as a live output.
//
// A pass is deferred if any .staged or bare gc/<ulid> files already exist
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
use bytes::Bytes;
use object_store::ObjectStore;
use ulid::Ulid;

use elide_core::extentindex::{self, ExtentIndex};
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

/// Maximum bytes per coalesced range-GET batch when fetching live bodies.
/// Matches the cap used by the demand-fetch engine in elide-fetch.
const RANGE_GET_MAX_BATCH: u64 = 4 * 1024 * 1024;

/// Minimum wasted bytes per range-GET batch to justify targeted fetches over
/// a single full body-section GET.  Each range-GET carries fixed overhead
/// (round-trip latency, S3 request cost), so it must avoid downloading at
/// least this many dead bytes to be worthwhile.  When `wasted_bytes /
/// batch_count` falls below this threshold, a single full-GET is cheaper.
const MIN_WASTE_PER_RANGE_GET: u64 = 128 * 1024;

/// Which GC strategy was executed.
#[derive(Debug, PartialEq)]
pub enum GcStrategy {
    /// No candidates found; nothing done.
    None,
    /// Repacked the single least-dense segment.
    Repack,
    /// Swept multiple small segments into one.
    Sweep,
    /// Both repack and sweep ran in the same tick.
    Both,
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
    fn none(total_segments: usize) -> Self {
        Self {
            strategy: GcStrategy::None,
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
/// Both repack and sweep run in the same tick if both find candidates.
/// Returns `GcStrategy::None` if neither finds candidates.
///
/// `repack_ulid` and `sweep_ulid` are the output segment names for each
/// strategy.  Both must be pre-resolved via `gc_checkpoint` IPC before
/// calling this function — they originate from the volume process so that
/// ULID ordering is consistent with the volume's write clock.
pub async fn gc_fork(
    fork_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
    config: &GcConfig,
    repack_ulid: Ulid,
    sweep_ulid: Ulid,
) -> Result<GcStats> {
    let index_dir = fork_dir.join("index");
    if !index_dir.exists() {
        return Ok(GcStats::none(0));
    }

    let gc_dir = fork_dir.join("gc");
    if has_pending_results(&gc_dir)? {
        return Ok(GcStats::none(0));
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

    let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
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

    // Segments with at least one partially-LBA-dead entry are ineligible
    // for compaction this pass: re-emitting their bloated LBA claim at the
    // GC output's higher ULID would shadow or erase later writes on rebuild.
    // Leave them in place at their original (low) ULID; the next rebuild
    // applies them in order and lbamap-insert's split logic handles the
    // overlap correctly. See `docs/design-gc-overlap-correctness.md`.
    let (deferred, all_stats): (Vec<SegmentStats>, Vec<SegmentStats>) =
        all_stats.into_iter().partition(|s| s.has_partial_death);
    let deferred_count = deferred.len();
    if !deferred.is_empty() {
        let deferred_ulids: Vec<String> = deferred
            .iter()
            .map(|s| s.ulid_str[..8].to_string())
            .collect();
        tracing::info!(
            "[gc] deferring {} segment(s) with partial-LBA-death entries: [{}]",
            deferred_count,
            deferred_ulids.join(", ")
        );
    }

    // Pre-pass: extract fully-dead segments (no live entries, no extent index
    // refs).  These are the cheapest possible GC: the handoff is tombstone-only
    // (just `dead` lines), no S3 fetch, no segment write, no upload — the
    // coordinator just DELETEs the old S3 objects.  Batch all of them into a
    // single tombstone handoff under repack_ulid.
    let mut dead_segments: Vec<SegmentStats> = Vec::new();
    let mut remaining: Vec<SegmentStats> = Vec::new();
    for s in all_stats {
        if s.live_entries.is_empty() && s.removed_hashes.is_empty() {
            dead_segments.push(s);
        } else {
            remaining.push(s);
        }
    }
    let mut all_stats = remaining;
    let dead_count = dead_segments.len();
    let repack_consumed_by_dead = if dead_count > 0 {
        let dead_ulids: Vec<String> = dead_segments.iter().map(|s| s.ulid_str.clone()).collect();
        tracing::info!(
            "[gc] dead pre-pass: {} fully-dead segment(s) → tombstone {repack_ulid}: [{}]",
            dead_count,
            dead_ulids.join(", "),
        );
        compact_segments(
            dead_segments,
            &gc_dir,
            volume_id,
            store,
            repack_ulid,
            &index,
        )
        .await
        .context("dead segment pre-pass")?;
        true
    } else {
        false
    };

    // Repack: density pass — extract the single least-dense segment.
    // Removes it from all_stats so sweep only sees the remainder.
    // Skipped when the pre-pass consumed repack_ulid.
    let mut repack_bytes: u64 = 0;
    let ran_repack = if repack_consumed_by_dead {
        false
    } else if let Some(pos) = find_least_dense(&all_stats, config.density_threshold) {
        let candidate = all_stats.remove(pos);
        repack_bytes = candidate.dead_lba_bytes();
        tracing::info!(
            "[gc] repack: {} → {} density={:.3} live_lba={} dead_lba={} \
             live_entries={} removed_hashes={}",
            candidate.ulid_str,
            repack_ulid,
            candidate.density(),
            candidate.live_lba_bytes,
            candidate.dead_lba_bytes(),
            candidate.live_entries.len(),
            candidate.removed_hashes.len(),
        );
        compact_segments(
            vec![candidate],
            &gc_dir,
            volume_id,
            store,
            repack_ulid,
            &index,
        )
        .await
        .context("density compaction")?;
        true
    } else {
        false
    };

    // Sweep: bin-pack high-density segments toward SWEEP_LIVE_CAP.
    //
    // Tier 1 — smalls (live_lba_bytes ≤ SWEEP_SMALL_THRESHOLD), sorted
    // ascending. Greedy include into the bucket until the next won't fit.
    // Tier 2 — at most one filler (live > SWEEP_SMALL_THRESHOLD) chosen
    // best-fit against the remaining headroom, only if the bucket already
    // has at least one small. The filler trades the cost of rewriting a
    // larger segment for one-fewer total segment, worth it only when
    // there's already a small bucket to attach to.
    //
    // Selection is purely about packing, not dead-data removal. Repack
    // owns dead-data (gated on `density_threshold`); low-density segments
    // are filtered out here so they go to repack instead. Sweep clears
    // dead entries within its selected inputs as a side-effect.
    let (smalls, fillers): (Vec<_>, Vec<_>) = all_stats
        .into_iter()
        .filter(|s| s.density() >= config.density_threshold)
        .partition(|s| s.live_lba_bytes <= SWEEP_SMALL_THRESHOLD);

    let mut smalls = smalls;
    smalls.sort_by_key(|s| s.live_lba_bytes);

    let mut bucket: Vec<SegmentStats> = Vec::new();
    let mut budget = SWEEP_LIVE_CAP;
    for s in smalls {
        if s.live_lba_bytes <= budget {
            budget -= s.live_lba_bytes;
            bucket.push(s);
        }
    }
    if !bucket.is_empty()
        && budget > 0
        && let Some(pos) = fillers
            .iter()
            .enumerate()
            .filter(|(_, s)| s.live_lba_bytes <= budget)
            .max_by_key(|(_, s)| s.live_lba_bytes)
            .map(|(i, _)| i)
    {
        let mut fillers = fillers;
        bucket.push(fillers.remove(pos));
    }

    // Sort the bucket back into ULID order before handing it to
    // `compact_segments` so its rebuild-order semantics hold (when two
    // input segments cover the same LBA the latest source wins).
    bucket.sort_by(|a, b| a.ulid_str.cmp(&b.ulid_str));

    // Single-input sweep is a no-op rewrite — skip. Repack handles
    // single-segment dead-data cleanup.
    let ran_sweep = if bucket.len() >= 2 {
        let sweep_candidates = bucket.len();
        let sweep_bytes: u64 = bucket.iter().map(|s| s.dead_lba_bytes()).sum();
        compact_segments(bucket, &gc_dir, volume_id, store, sweep_ulid, &index)
            .await
            .context("small-segment sweep")?;
        Some((sweep_candidates, sweep_bytes))
    } else {
        None
    };

    match (ran_repack, ran_sweep) {
        (false, None) if dead_count == 0 && deferred_count == 0 => {
            Ok(GcStats::none(total_segments))
        }
        (false, None) => Ok(GcStats {
            strategy: GcStrategy::None,
            candidates: 0,
            bytes_freed: 0,
            dead_cleaned: dead_count,
            deferred: deferred_count,
            total_segments,
        }),
        (true, None) => Ok(GcStats {
            strategy: GcStrategy::Repack,
            candidates: 1,
            bytes_freed: repack_bytes,
            dead_cleaned: dead_count,
            deferred: deferred_count,
            total_segments,
        }),
        (false, Some((n, sweep_bytes))) => Ok(GcStats {
            strategy: GcStrategy::Sweep,
            candidates: n,
            bytes_freed: sweep_bytes,
            dead_cleaned: dead_count,
            deferred: deferred_count,
            total_segments,
        }),
        (true, Some((n, sweep_bytes))) => Ok(GcStats {
            strategy: GcStrategy::Both,
            candidates: 1 + n,
            bytes_freed: repack_bytes + sweep_bytes,
            dead_cleaned: dead_count,
            deferred: deferred_count,
            total_segments,
        }),
    }
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
            // No extension; ULID-parseable stem; no sibling `.staged`
            // (would indicate mid-apply crash state that volume will
            // resolve on its next apply tick).
            if name.contains('.') {
                return false;
            }
            if Ulid::from_string(name).is_err() {
                return false;
            }
            !gc_dir.join(format!("{name}.staged")).exists()
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
        let key = segment_key(volume_id, &new_ulid_str)
            .with_context(|| format!("building key for {new_ulid_str}"))?;
        let data = tokio::fs::read(&gc_body)
            .await
            .with_context(|| format!("reading compacted segment {new_ulid_str}"))?;
        store
            .put(&key, Bytes::from(data).into())
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

/// Delete any `gc/<ulid>.staged.tmp` scratch files left by a crashed
/// `compact_segments` between the `write_gc_segment` call and the rename
/// to `<ulid>.staged`. The next pass mints a fresh ULID so the stale
/// scratch is inert, but it must be removed to avoid unbounded disk
/// growth and to let `create_new` succeed on any later re-use.
///
/// Coordinator-owned — the volume's apply-path sweeper deliberately
/// skips this suffix to avoid racing an in-flight write.
fn cleanup_staging_files(gc_dir: &Path) {
    let Ok(entries) = fs::read_dir(gc_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.ends_with(".staged.tmp")
            && let Err(e) = fs::remove_file(entry.path())
        {
            error!("[gc] failed to delete stale staging file {name_str}: {e}");
        }
    }
}

// --- internals ---

/// Logical block size: all LBA lengths are in units of this many bytes.
const BLOCK_BYTES: u64 = 4096;

/// Per-segment stats computed during the scan phase.
struct SegmentStats {
    ulid_str: String,
    /// `body_section_start` for this segment's S3 object (== .idx file size).
    /// Used to compute absolute byte offsets for range-GETs into S3.
    body_section_start: u64,
    /// Physical on-disk size (idx + DATA body bytes); kept for diagnostics
    /// and density logging.
    file_size: u64,
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
    /// Live entries (data field not yet populated for DATA entries).
    live_entries: Vec<SegmentEntry>,
    /// Per-entry position in the source segment's index section, parallel to
    /// `live_entries`. Used to index into `cache/<ulid>.present` when checking
    /// whether an entry's body bytes are available locally.
    live_entry_indices: Vec<u32>,
    /// Hashes that are in the extent index but not reachable from the LBA map.
    /// The volume must remove these from its extent index when applying the
    /// handoff, since the old segment files will be deleted.
    removed_hashes: Vec<blake3::Hash>,
    /// True if at least one body-bearing entry in this segment has a partial
    /// LBA-death — part of its claimed range is still live at its hash but
    /// another part has been overwritten. Such segments must not be
    /// compacted this pass (see `docs/design-gc-overlap-correctness.md`):
    /// re-emitting the bloated (start_lba, lba_length) claim at the GC
    /// output's higher ULID would shadow or erase the overwriter on
    /// rebuild. Leaving the segment in place preserves correctness
    /// because the original low-ULID claim is split correctly at
    /// lbamap-insert time by subsequent segments.
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
        //
        // idx_size == body_section_start: the .idx file is exactly the
        // [0, body_section_start) prefix of the full S3 segment.
        let idx_size = segment::idx_body_section_start(&idx_path)?;
        let (_, entries, _) = segment::read_and_verify_segment_index(&idx_path, vk)?;

        // Read inline section from .idx for any inline entries.
        let has_inline = entries.iter().any(|e| e.kind == EntryKind::Inline);
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
        let mut removed_hashes: Vec<blake3::Hash> = Vec::new();
        let mut has_partial_death = false;

        for (entry_idx, mut entry) in entries.into_iter().enumerate() {
            let entry_idx = entry_idx as u32;
            // Pre-populate inline entry data from the .idx inline section.
            // compact_segments needs this data to write the output segment.
            if entry.kind == EntryKind::Inline {
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
                }
                continue;
            }
            // Inline entries have stored bytes in the inline section (part of
            // .idx, not S3 body). They do not contribute to physical_body_bytes
            // but do participate in liveness like DATA entries.
            if entry.kind != EntryKind::Inline {
                physical_body_bytes += entry.stored_length as u64;
            }
            // DedupRef entries carry no body bytes (thin format: stored_length=0)
            // but still carry an LBA mapping. Liveness is LBA-based: the entry
            // is live if the LBA still maps to its hash. The canonical DATA
            // for the hash lives elsewhere (canonical-presence invariant).
            if entry.kind == EntryKind::DedupRef {
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
                let extent_live = index
                    .lookup(&entry.hash)
                    .is_some_and(|loc| loc.segment_id == seg_ulid);
                if lba_live {
                    live_lba_bytes += lba_bytes;
                    live_entries.push(entry);
                    live_entry_indices.push(entry_idx);
                } else if extent_live {
                    removed_hashes.push(entry.hash);
                }
                continue;
            }
            // DATA / Inline / Delta entries carry both a hash (→ body) and
            // an LBA claim. Liveness classification uses a full-range scan
            // against the lbamap, not a point query at `start_lba`, because
            // a multi-LBA entry's head, tail, or interior may have been
            // overwritten by later writes while `start_lba` is unaffected
            // (and vice versa). See `docs/design-gc-overlap-correctness.md`.
            //
            // Outcomes:
            //   - **fully alive** (every LBA in the range maps to
            //     `entry.hash`): keep the entry intact. Its LBA binding is
            //     authoritative and compacts cleanly.
            //   - **fully dead** (no LBA in the range maps to `entry.hash`):
            //     * hash still referenced elsewhere (DedupRef / alias-merge
            //       orphan): demote to `canonical_only`. Body survives for
            //       extent-index resolution; no LBA claim on rebuild.
            //     * hash orphaned entirely: record for extent-index removal
            //       and drop.
            //   - **partially alive** (some LBAs still live at `entry.hash`,
            //     others overwritten): mark the segment as having partial
            //     LBA-death. `gc_fork` excludes such segments from this
            //     compaction pass entirely — leaving the bloated entry in
            //     place at its original ULID so rebuild applies segments
            //     in ULID order and `lbamap::insert`'s split logic produces
            //     the correct final state. Re-emitting the bloated claim
            //     at the GC output's higher ULID would shadow or erase
            //     the overwriter.
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
            } else if matching_blocks == 0 {
                if extent_live && live_hashes.contains(&entry.hash) {
                    // Fully LBA-dead but hash still externally live.
                    // Demote the source entry into its canonical variant —
                    // body preserved for dedup resolution, LBA claim dropped.
                    live_entries.push(entry.into_canonical());
                    live_entry_indices.push(entry_idx);
                } else if extent_live {
                    // Fully dead.
                    removed_hashes.push(entry.hash);
                }
            } else {
                // Partially alive: mark the segment ineligible for this
                // compaction pass. Keep the original entry in live_entries
                // so density accounting reflects reality (partial live),
                // but `has_partial_death = true` will cause gc_fork to
                // exclude this segment from repack/sweep.
                live_lba_bytes += matching_blocks * BLOCK_BYTES;
                live_entries.push(entry);
                live_entry_indices.push(entry_idx);
                has_partial_death = true;
            }
        }

        // file_size: physical on-disk size of the segment object. Uses
        // stored (compressed) DATA and REF body bytes + idx overhead — not
        // LBA bytes, since this measures disk space, not logical coverage.
        let file_size = idx_size + physical_body_bytes;
        result.push(SegmentStats {
            ulid_str,
            body_section_start: idx_size,
            has_body_entries: physical_body_bytes > 0 || has_inline,
            file_size,
            live_lba_bytes,
            total_lba_bytes,
            live_entries,
            live_entry_indices,
            removed_hashes,
            has_partial_death,
        });
    }
    Ok(result)
}

/// Return the index of the least-dense segment whose density is below
/// `threshold`, or `None` if no segment qualifies.
fn find_least_dense(stats: &[SegmentStats], threshold: f64) -> Option<usize> {
    stats
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            // Exclude segments with no dead LBA bytes: nothing to reclaim.
            // Exclude segments with no body content: compacting a segment of
            // only zero extents writes an equivalent output with no physical
            // storage savings and no extent index changes — skip.
            s.density() < threshold && s.dead_lba_bytes() > 0 && s.has_data_content()
        })
        .min_by(|(_, a), (_, b)| {
            a.density()
                .partial_cmp(&b.density())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(i, _)| i)
}

/// Read live extent bodies from `cache/<ulid>.body` if every entry in
/// `body_indices` has its `.present` bit set. Returns:
///
/// - `Ok(true)` if every live DATA body was populated from the local cache.
/// - `Ok(false)` if any byte is missing locally (body file absent, any
///   required `.present` bit unset, or a read error that looks like a cache
///   miss) — caller should fall back to S3.
///
/// Never mutates `candidate` on the `Ok(false)` return, so S3 fallback can
/// populate the same entries safely.
fn try_fetch_live_bodies_local(
    candidate: &mut SegmentStats,
    fork_dir: &Path,
    body_indices: &[usize],
) -> Result<bool> {
    let cache_dir = fork_dir.join("cache");
    let body_path = cache_dir.join(format!("{}.body", candidate.ulid_str));
    let present_path = cache_dir.join(format!("{}.present", candidate.ulid_str));

    // Body file present? If not, definitely a miss.
    match body_path.try_exists() {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(_) => return Ok(false),
    }

    // Present bitmap: every required entry_idx must have its bit set.
    // Reading the file once is cheaper than N calls to check_present_bit
    // which re-read it each time.
    let present_bytes = match fs::read(&present_path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Ok(false),
    };
    for &i in body_indices {
        let entry_idx = candidate.live_entry_indices[i];
        let byte_idx = (entry_idx / 8) as usize;
        let bit = entry_idx % 8;
        let bit_set = present_bytes
            .get(byte_idx)
            .is_some_and(|b| b & (1 << bit) != 0);
        if !bit_set {
            return Ok(false);
        }
    }

    // All required bits set. Read body file once and slice each entry out.
    // cache/<ulid>.body uses body-section-relative offsets (BodyOnly layout)
    // — `stored_offset` is already the right offset into this file.
    let body = match fs::read(&body_path) {
        Ok(b) => b,
        Err(_) => return Ok(false),
    };
    for &i in body_indices {
        let e = &candidate.live_entries[i];
        let start = e.stored_offset as usize;
        let end = start + e.stored_length as usize;
        if end > body.len() {
            // Body file truncated mid-eviction or torn write. Bail.
            return Ok(false);
        }
        candidate.live_entries[i].data = Some(body[start..end].to_vec());
    }

    let total_local: u64 = body_indices
        .iter()
        .map(|&i| candidate.live_entries[i].stored_length as u64)
        .sum();
    tracing::info!(
        "[gc] fetch {}: local cache hit, read {} body bytes from cache/{}.body",
        candidate.ulid_str,
        total_local,
        candidate.ulid_str,
    );
    Ok(true)
}

/// Fetch live extent body bytes from S3 into `candidate.live_entries[].data`.
///
/// Computes coalesced range-GET batches for the live body entries, then decides
/// whether targeted range-GETs or a single full body-section GET is cheaper.
/// The heuristic: range-GETs are worthwhile when the wasted bytes they avoid
/// exceed `MIN_WASTE_PER_RANGE_GET` per batch; otherwise a single GET is
/// cheaper despite downloading dead bytes.
///
/// Local short-circuit: if every live DATA body is already present in
/// `cache/<ulid>.body` (checked via the `.present` bitmap), we read from
/// the local cache and skip S3 entirely. This is safe because cache/ is
/// append-only from the volume's perspective (the coordinator is the sole
/// deleter) and `.present` bits are durable before they are published —
/// observed bytes are guaranteed to match the S3 object's body.
async fn fetch_live_bodies(
    candidate: &mut SegmentStats,
    volume_id: &str,
    fork_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    // Collect indices of live entries that carry body bytes.
    // DedupRef entries contribute no body in the thin format
    // (stored_length=0) — we never fetch bytes for them.
    let body_indices: Vec<usize> = candidate
        .live_entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.kind == EntryKind::Data && e.stored_length > 0)
        .map(|(i, _)| i)
        .collect();

    if body_indices.is_empty() {
        return Ok(());
    }

    // Try the local cache first. On any miss or error, fall through to S3.
    if try_fetch_live_bodies_local(candidate, fork_dir, &body_indices)? {
        return Ok(());
    }

    let key = segment_key(volume_id, &candidate.ulid_str)
        .with_context(|| format!("building S3 key for {}", candidate.ulid_str))?;

    // Sort live body entries by stored_offset and coalesce adjacent entries
    // into batches.  Each batch is a contiguous byte range in the segment's
    // body section, capped at RANGE_GET_MAX_BATCH bytes.
    let mut sorted_indices = body_indices;
    sorted_indices.sort_by_key(|&i| candidate.live_entries[i].stored_offset);

    // batches: Vec<(batch_body_start, batch_body_end, &[sorted_index_positions])>
    // We store start/end positions within sorted_indices rather than cloning.
    let mut batches: Vec<(u64, u64, usize, usize)> = Vec::new(); // (body_start, body_end, si_start, si_end_inclusive)
    {
        let mut si = 0;
        while si < sorted_indices.len() {
            let first_idx = sorted_indices[si];
            let batch_body_start = candidate.live_entries[first_idx].stored_offset;
            let mut batch_body_end =
                batch_body_start + candidate.live_entries[first_idx].stored_length as u64;
            let mut si_end = si;

            for (j, &idx) in sorted_indices.iter().enumerate().skip(si + 1) {
                let e = &candidate.live_entries[idx];
                if e.stored_offset != batch_body_end {
                    break;
                }
                let new_end = batch_body_end + e.stored_length as u64;
                if new_end - batch_body_start > RANGE_GET_MAX_BATCH {
                    break;
                }
                batch_body_end = new_end;
                si_end = j;
            }

            batches.push((batch_body_start, batch_body_end, si, si_end));
            si = si_end + 1;
        }
    }

    let total_body_bytes = candidate
        .file_size
        .saturating_sub(candidate.body_section_start);
    let live_body_bytes: u64 = sorted_indices
        .iter()
        .map(|&i| candidate.live_entries[i].stored_length as u64)
        .sum();
    let wasted_bytes = total_body_bytes.saturating_sub(live_body_bytes);
    let batch_count = batches.len() as u64;

    let use_ranges = batch_count > 0 && wasted_bytes / batch_count >= MIN_WASTE_PER_RANGE_GET;

    if use_ranges {
        for &(batch_body_start, batch_body_end, si_start, si_end) in &batches {
            let abs_start = (candidate.body_section_start + batch_body_start) as usize;
            let abs_end = (candidate.body_section_start + batch_body_end) as usize;

            let data = store
                .get_range(&key, abs_start..abs_end)
                .await
                .with_context(|| {
                    format!(
                        "range-GET for {} (offset {}..{})",
                        candidate.ulid_str, abs_start, abs_end,
                    )
                })?;

            for &idx in &sorted_indices[si_start..=si_end] {
                let e = &candidate.live_entries[idx];
                let local_off = (e.stored_offset - batch_body_start) as usize;
                let local_end = local_off + e.stored_length as usize;
                candidate.live_entries[idx].data = Some(data[local_off..local_end].to_vec());
            }
        }

        tracing::info!(
            "[gc] fetch {}: range-GET {} batch(es), fetched {} of {} body bytes (saved {}, {:.0}%)",
            candidate.ulid_str,
            batch_count,
            live_body_bytes,
            total_body_bytes,
            wasted_bytes,
            if total_body_bytes > 0 {
                wasted_bytes as f64 / total_body_bytes as f64 * 100.0
            } else {
                0.0
            },
        );
    } else {
        // Single GET for the entire body section, slice out each live entry.
        let body_start = candidate.body_section_start as usize;
        let body_end = candidate.file_size as usize;
        let body = store
            .get_range(&key, body_start..body_end)
            .await
            .with_context(|| format!("fetching body section for {}", candidate.ulid_str))?;

        for &idx in &sorted_indices {
            let e = &candidate.live_entries[idx];
            let off = e.stored_offset as usize;
            let end = off + e.stored_length as usize;
            candidate.live_entries[idx].data = Some(body[off..end].to_vec());
        }

        let waste_per_batch = if batch_count > 0 {
            wasted_bytes / batch_count
        } else {
            0
        };
        tracing::info!(
            "[gc] fetch {}: full-body GET {} of {} body bytes \
             (waste {}, {} batch(es) \u{2192} {}/batch < {} threshold)",
            candidate.ulid_str,
            total_body_bytes,
            total_body_bytes,
            wasted_bytes,
            batch_count,
            waste_per_batch,
            MIN_WASTE_PER_RANGE_GET,
        );
    }

    Ok(())
}

/// Read live extent bodies from each candidate, write a compacted self-
/// describing segment to `gc/<new-ulid>.staged` (with the consumed input
/// ULID list embedded in the segment header — the volume's apply path
/// derives the extent-index updates from this field, no manifest sidecar).
///
/// For each candidate, the full segment is downloaded from S3 to a temporary
/// `gc/<ulid>.fetch` file. This guarantees the body is complete regardless of
/// demand-fetch state, and keeps the fetch consistent with other full-segment
/// files in gc/. The `.fetch` file is deleted after the body is read.
async fn compact_segments(
    mut candidates: Vec<SegmentStats>,
    gc_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
    new_ulid: Ulid,
    _extent_index: &ExtentIndex,
) -> Result<()> {
    let new_ulid_str = new_ulid.to_string();
    fs::create_dir_all(gc_dir).context("creating gc dir")?;

    // fork_dir is the parent of gc_dir; used to locate `cache/<ulid>.body`
    // when resolving bodies locally instead of from S3.
    let fork_dir = gc_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("gc_dir has no parent"))?;

    // For each candidate: try the local cache first (cache/<ulid>.body with
    // all required `.present` bits set), falling through to S3 on any miss.
    // Zero extents carry no body data, so candidates with only zero extents
    // skip the fetch entirely. removed_hashes are already fully populated
    // from collect_stats and need no fetch.
    let mut all_live: Vec<(String, SegmentEntry)> = Vec::new();
    let mut all_removed: Vec<(blake3::Hash, String)> = Vec::new();
    for candidate in &mut candidates {
        fetch_live_bodies(candidate, volume_id, fork_dir, store)
            .await
            .with_context(|| format!("fetching bodies for {}", candidate.ulid_str))?;

        // Verify every fetched body hashes to its declared hash before we
        // carry it forward. A mismatch indicates an already-poisoned input
        // segment (e.g. from the pre-308f778 stale-liveness bug) — propagating
        // it would corrupt the compacted output and, once inputs are dropped,
        // any surviving LBA that resolves to this hash.
        for entry in &candidate.live_entries {
            if matches!(entry.kind, EntryKind::Data | EntryKind::Inline)
                && let Some(bytes) = entry.data.as_deref()
            {
                segment::verify_body_hash(entry, bytes).with_context(|| {
                    format!(
                        "body integrity check failed for input segment {}",
                        candidate.ulid_str
                    )
                })?;
            }
        }

        for entry in candidate.live_entries.drain(..) {
            all_live.push((candidate.ulid_str.clone(), entry));
        }
        for hash in candidate.removed_hashes.drain(..) {
            all_removed.push((hash, candidate.ulid_str.clone()));
        }
    }

    // Self-describing GC handoff (step 4b): one unified code path for live,
    // removal-only and tombstone compactions. The output segment carries the
    // sorted list of input ULIDs in its header so the volume's apply path can
    // derive the extent-index updates directly from the segment. Tombstones
    // and removal-only handoffs produce a zero-entries segment with only the
    // inputs field populated — tiny but valid.
    let _ = &all_removed; // consumed implicitly by the volume's derive path

    fs::create_dir_all(gc_dir).context("creating gc dir")?;
    let tmp_path = gc_dir.join(format!("{new_ulid_str}.staged.tmp"));

    let mut new_entries: Vec<SegmentEntry> = Vec::with_capacity(all_live.len());
    for (_, e) in &mut all_live {
        match e.kind {
            EntryKind::Zero => {
                new_entries.push(SegmentEntry::new_zero(e.start_lba, e.lba_length));
            }
            EntryKind::DedupRef => {
                // DedupRef entries carry no body bytes in the thin format.
                // Pass them through the compactor unchanged: the canonical
                // DATA lives elsewhere (maintained by the canonical-presence
                // invariant) and reads resolve via the extent index.
                new_entries.push(SegmentEntry::new_dedup_ref(
                    e.hash,
                    e.start_lba,
                    e.lba_length,
                ));
            }
            EntryKind::Data | EntryKind::Inline => {
                let flags = if e.compressed {
                    segment::SegmentFlags::COMPRESSED
                } else {
                    segment::SegmentFlags::empty()
                };
                // `new_data` chooses Data vs Inline based on stored size.
                new_entries.push(SegmentEntry::new_data(
                    e.hash,
                    e.start_lba,
                    e.lba_length,
                    flags,
                    e.data.take().unwrap_or_default(),
                ));
            }
            EntryKind::CanonicalData | EntryKind::CanonicalInline => {
                // Canonical-body-only: preserve the body for dedup resolution,
                // no LBA claim on rebuild. Built from a Data/Inline entry via
                // `new_data` (which picks Data vs Inline based on stored size)
                // and then demoted via `into_canonical`.
                let flags = if e.compressed {
                    segment::SegmentFlags::COMPRESSED
                } else {
                    segment::SegmentFlags::empty()
                };
                let body = e.data.take().unwrap_or_default();
                let built = SegmentEntry::new_data(e.hash, 0, 0, flags, body);
                new_entries.push(built.into_canonical());
            }
            EntryKind::Delta => {
                // Delta entries carry no body bytes in the thin format.
                // Pass them through the compactor unchanged: the delta blob
                // stays in this segment's delta body section (carried by
                // rewrite_with_deltas elsewhere) and the source_hash is kept
                // alive via the lba_referenced_hashes fold. Reads resolve
                // via extent_index.lookup(source_hash) then zstd-dict
                // decompress.
                debug_assert!(
                    e.stored_offset == 0 && e.stored_length == 0,
                    "Delta entry must have zero stored_offset and stored_length"
                );
                new_entries.push(SegmentEntry::new_delta(
                    e.hash,
                    e.start_lba,
                    e.lba_length,
                    e.delta_options.clone(),
                ));
            }
        }
    }

    // The coordinator does not hold the volume's private key, so it signs the
    // compacted segment with an ephemeral key. The volume re-signs it with its
    // own key inside apply_gc_handoffs, producing a bare `gc/<ulid>` body.
    // That rename (performed by the volume) is the atomic commit point;
    // until then the file stays at `gc/<ulid>.staged`.
    //
    // `inputs` is the list of source segment ULIDs consumed by this compaction,
    // sorted for determinism so identical candidate sets produce byte-identical
    // output headers. The volume's apply path reads this field to derive
    // extent-index updates directly from the segment itself.
    let mut inputs: Vec<Ulid> = candidates
        .iter()
        .map(|c| Ulid::from_string(&c.ulid_str).context("parsing candidate ulid"))
        .collect::<Result<Vec<_>>>()?;
    inputs.sort();
    let (ephemeral_signer, _) = elide_core::signing::generate_ephemeral_signer();
    let _new_body_section_start = segment::write_gc_segment(
        &tmp_path,
        &mut new_entries,
        &inputs,
        ephemeral_signer.as_ref(),
    )
    .context("writing compacted segment")?;

    // Stage at `gc/<ulid>.staged` via rename. Volume apply will detect this,
    // derive the action set from `inputs` + each input's `.idx`, re-sign
    // into `<ulid>.tmp`, and rename to bare `<ulid>` as the commit point.
    let staged_path = gc_dir.join(format!("{new_ulid_str}.staged"));
    tokio::fs::rename(&tmp_path, &staged_path)
        .await
        .context("staging compacted segment in gc/")?;

    // Per-kind breakdown for visibility. Mirrors the volume's flush log line
    // (`data / inline / dedup-ref / zero / delta`) plus a `canonical-only`
    // column: Data/Inline entries whose LBA was dead but whose hash was still
    // live elsewhere — demoted in `collect_stats` to carry body for dedup
    // resolution while dropping the stale LBA claim.
    let mut data = 0usize;
    let mut inline = 0usize;
    let mut refs = 0usize;
    let mut zero = 0usize;
    let mut delta = 0usize;
    let mut canonical = 0usize;
    for e in &new_entries {
        match e.kind {
            segment::EntryKind::Data => data += 1,
            segment::EntryKind::Inline => inline += 1,
            segment::EntryKind::DedupRef => refs += 1,
            segment::EntryKind::Zero => zero += 1,
            segment::EntryKind::Delta => delta += 1,
            segment::EntryKind::CanonicalData | segment::EntryKind::CanonicalInline => {
                canonical += 1
            }
        }
    }
    tracing::info!(
        "[gc] staged output → {new_ulid_str}: {data} data, {inline} inline, \
         {refs} dedup-ref, {zero} zero, {delta} delta, {canonical} canonical-only \
         ({} entries total, {} inputs)",
        new_entries.len(),
        inputs.len()
    );

    Ok(())
}

/// Returns true if any in-flight GC handoffs exist in `gc_dir`.
///
/// An in-flight handoff is either:
/// - a `.staged` file (coordinator wrote it, volume has not yet applied), or
/// - a bare `gc/<ulid>` file (volume applied, coordinator upload pending).
///
/// A new GC pass is deferred while any of these exist — the coordinator
/// should finish the current batch before staging another.
fn has_pending_results(gc_dir: &Path) -> Result<bool> {
    if !gc_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(gc_dir).context("reading gc dir")? {
        let entry = entry.context("reading gc dir entry")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(".staged") {
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
    fn detects_staged_result_file() {
        // `has_pending_results` returns true while any handoff is in flight:
        // either a coordinator-staged `.staged` file or a volume-applied bare
        // `gc/<ulid>` waiting for upload.
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.staged"), "").unwrap();
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

    #[test]
    fn find_least_dense_picks_sparsest_below_threshold() {
        // A fake DATA entry — stored_length > 0 so has_data_content() returns true.
        fn data_entry() -> SegmentEntry {
            SegmentEntry::new_data(
                blake3::hash(b"x"),
                0,
                1,
                segment::SegmentFlags::empty(),
                vec![0u8; 4096],
            )
        }
        fn make(total_lba_bytes: u64, live_lba_bytes: u64) -> SegmentStats {
            SegmentStats {
                ulid_str: String::new(),
                body_section_start: 0,
                file_size: total_lba_bytes, // physical size irrelevant for density
                live_lba_bytes,
                total_lba_bytes,
                has_body_entries: true,
                live_entries: vec![data_entry()],
                live_entry_indices: vec![0],
                removed_hashes: Vec::new(),
                has_partial_death: false,
            }
        }

        // density: 0.8, 0.5, 0.6 — only 0.5 and 0.6 are below 0.7
        let stats = vec![make(100, 80), make(100, 50), make(100, 60)];
        assert_eq!(find_least_dense(&stats, 0.7), Some(1)); // 0.5 is least dense
    }

    #[test]
    fn sweep_skips_single_small_segment() {
        // Repack owns single-segment compaction (by density). By the time
        // sweep runs, a lone small segment — whether all-live or sparsely
        // live — has density >= threshold and is not worth a standalone GC pass.
        // Verify dead_lba_bytes() is available for the ≥2 case.
        let s = SegmentStats {
            ulid_str: String::new(),
            body_section_start: 0,
            file_size: 1024 * 1024,
            live_lba_bytes: 800 * 1024,
            total_lba_bytes: 1024 * 1024,
            has_body_entries: true,
            live_entries: Vec::new(),
            live_entry_indices: Vec::new(),
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };
        assert_eq!(s.dead_lba_bytes(), 224 * 1024);
        // density = 0.78 >= 0.70 threshold: repack would have caught it if below.
        assert!(s.density() >= 0.70);
    }

    #[test]
    fn find_least_dense_returns_none_when_all_above_threshold() {
        fn make(live_lba_bytes: u64) -> SegmentStats {
            SegmentStats {
                ulid_str: String::new(),
                body_section_start: 0,
                file_size: 100,
                live_lba_bytes,
                total_lba_bytes: 100,
                has_body_entries: true,
                live_entries: Vec::new(),
                live_entry_indices: Vec::new(),
                removed_hashes: Vec::new(),
                has_partial_death: false,
            }
        }
        let stats = vec![make(80), make(90), make(100)];
        assert_eq!(find_least_dense(&stats, 0.7), None);
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
        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn done_empty_gc_dir() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join("gc")).unwrap();
        let store = make_store();
        let n = apply_done_handoffs(tmp.path(), "vol", &store)
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
        let n = apply_done_handoffs(tmp.path(), "vol", &store)
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
        let sweep_ulid = Ulid::new();
        let repack_ulid = Ulid::new();
        let stats = gc_fork(dir, "test-vol", &store, &config, repack_ulid, sweep_ulid)
            .await
            .unwrap();
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
        let done = apply_done_handoffs(dir, "test-vol", &store).await.unwrap();
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
        let sweep_ulid = Ulid::new();
        let repack_ulid = Ulid::new();
        gc_fork(dir, "test-vol", &store, &config, repack_ulid, sweep_ulid)
            .await
            .unwrap();

        // Find the staged GC output body in gc/. Under the self-describing
        // protocol the coordinator writes `gc/<ulid>.staged`.
        let gc_dir = dir.join("gc");
        let gc_body = fs::read_dir(&gc_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|s| s.to_str()) == Some("staged"))
            .expect("gc/ must contain a .staged file");
        let (_bss, _entries, inputs) = elide_core::segment::read_segment_index(&gc_body).unwrap();

        assert_eq!(
            inputs, expected_inputs,
            "gc output must list all swept source ulids in sorted order"
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
    /// If `has_partial_death` is set, the segment is excluded from this
    /// GC pass by `gc_fork`: it stays on disk with its original entries
    /// intact, and rebuild applies it in ULID order correctly. No further
    /// checks needed.
    ///
    /// Otherwise two invariants must hold over `live_entries`:
    ///
    /// 1. **Shadow.** Every emitted live entry's LBA claim must match the
    ///    current lbamap across its full range. Otherwise rebuild at a
    ///    higher ULID re-asserts a stale mapping and overwrites the
    ///    correct binding.
    ///
    /// 2. **Loss.** Every LBA within S1's original range that currently
    ///    resolves to S1's hash must be covered by some emitted
    ///    non-canonical-only entry with that hash. Otherwise rebuild
    ///    produces a hole where a binding used to exist (particularly
    ///    important when the old segment is consumed and a surviving
    ///    `payload_block_offset`-aliased tail has no first-class claim
    ///    left on disk).
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
        for entry in &s1_stats.live_entries {
            if !matches!(
                entry.kind,
                EntryKind::Data | EntryKind::Inline | EntryKind::Delta
            ) {
                continue;
            }
            if entry.kind.is_canonical_only() || entry.lba_length == 0 {
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

        // Invariant 2: no loss.
        let mut claimed = std::collections::HashSet::new();
        for entry in &s1_stats.live_entries {
            if entry.kind.is_canonical_only() || entry.hash != original_hash {
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

    // --- fetch_live_bodies tests ---

    /// Helper: build a SegmentEntry with given kind, stored_offset, stored_length,
    /// but empty data (fetch_live_bodies will populate it).
    fn stub_entry(kind: EntryKind, stored_offset: u64, stored_length: u32) -> SegmentEntry {
        SegmentEntry {
            hash: blake3::hash(&stored_offset.to_le_bytes()),
            start_lba: 0,
            lba_length: 1,
            compressed: false,
            kind,
            stored_offset,
            stored_length,
            data: None,
            delta_options: Vec::new(),
        }
    }

    /// Put a fake segment object in the store: `body_section_start` bytes of
    /// zeros (index prefix) followed by `body` bytes.
    async fn put_fake_segment(
        store: &Arc<dyn ObjectStore>,
        volume_id: &str,
        ulid_str: &str,
        body_section_start: u64,
        body: &[u8],
    ) {
        let key = segment_key(volume_id, ulid_str).unwrap();
        let mut data = vec![0u8; body_section_start as usize];
        data.extend_from_slice(body);
        store
            .put(&key, bytes::Bytes::from(data).into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fetch_live_bodies_no_body_entries_skips_fetch() {
        let store = make_store();
        // No object in store — would fail if fetch_live_bodies tried to GET.
        let mut candidate = SegmentStats {
            ulid_str: Ulid::from_parts(1000, 1).to_string(),
            body_section_start: 100,
            file_size: 200,
            live_lba_bytes: 0,
            total_lba_bytes: 0,
            has_body_entries: false,
            live_entries: vec![
                SegmentEntry::new_zero(0, 1),
                SegmentEntry::new_dedup_ref(blake3::hash(b"x"), 1, 1),
            ],
            live_entry_indices: vec![0, 1],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };
        let tmp = TempDir::new().unwrap();
        fetch_live_bodies(&mut candidate, "vol", tmp.path(), &store)
            .await
            .unwrap();
        // No data populated (dedup_ref and zero have no body).
        assert!(candidate.live_entries.iter().all(|e| e.data.is_none()));
    }

    #[tokio::test]
    async fn fetch_live_bodies_full_get_when_waste_below_threshold() {
        // Scenario: body section has 2 live entries that are nearly contiguous,
        // so waste per batch is small → full-body GET is chosen.
        let store = make_store();
        let ulid_str = Ulid::from_parts(1000, 2).to_string();
        let body_section_start: u64 = 256;

        // Body: 8192 bytes total, two live entries of 4096 each at offsets 0 and 4096.
        // No waste at all → full-GET path.
        let body = vec![0xABu8; 8192];
        put_fake_segment(&store, "vol", &ulid_str, body_section_start, &body).await;

        let mut candidate = SegmentStats {
            ulid_str: ulid_str.clone(),
            body_section_start,
            file_size: body_section_start + 8192,
            live_lba_bytes: 8192,
            total_lba_bytes: 8192,
            has_body_entries: true,
            live_entries: vec![
                stub_entry(EntryKind::Data, 0, 4096),
                stub_entry(EntryKind::Data, 4096, 4096),
            ],
            live_entry_indices: vec![0, 1],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };

        let tmp = TempDir::new().unwrap();
        fetch_live_bodies(&mut candidate, "vol", tmp.path(), &store)
            .await
            .unwrap();

        assert_eq!(candidate.live_entries[0].data.as_ref().unwrap().len(), 4096);
        assert_eq!(candidate.live_entries[1].data.as_ref().unwrap().len(), 4096);
        assert!(
            candidate.live_entries[0]
                .data
                .as_ref()
                .unwrap()
                .iter()
                .all(|&b| b == 0xAB)
        );
        assert!(
            candidate.live_entries[1]
                .data
                .as_ref()
                .unwrap()
                .iter()
                .all(|&b| b == 0xAB)
        );
    }

    #[tokio::test]
    async fn fetch_live_bodies_range_get_when_waste_above_threshold() {
        // Scenario: body section is large (1MB), but live entries are small and
        // separated by large dead gaps → range-GETs are chosen.
        let store = make_store();
        let ulid_str = Ulid::from_parts(1000, 3).to_string();
        let body_section_start: u64 = 256;
        let body_size: u64 = 1024 * 1024; // 1MB body section

        // Fill body with position-dependent bytes so we can verify slicing.
        let mut body = vec![0u8; body_size as usize];
        // Live entry A: 4096 bytes at offset 0
        for b in &mut body[0..4096] {
            *b = 0xAA;
        }
        // Dead gap: offsets 4096..900_000
        // Live entry B: 4096 bytes at offset 900_000
        for b in &mut body[900_000..900_000 + 4096] {
            *b = 0xBB;
        }
        put_fake_segment(&store, "vol", &ulid_str, body_section_start, &body).await;

        let mut candidate = SegmentStats {
            ulid_str: ulid_str.clone(),
            body_section_start,
            file_size: body_section_start + body_size,
            live_lba_bytes: 8192,
            total_lba_bytes: body_size,
            has_body_entries: true,
            live_entries: vec![
                stub_entry(EntryKind::Data, 0, 4096),
                stub_entry(EntryKind::Data, 900_000, 4096),
            ],
            live_entry_indices: vec![0, 1],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };

        // Waste = 1MB - 8192 ≈ 1MB. Two batches (entries are far apart).
        // Waste per batch = ~512KB >> 128KB threshold → range-GETs.
        let tmp = TempDir::new().unwrap();
        fetch_live_bodies(&mut candidate, "vol", tmp.path(), &store)
            .await
            .unwrap();

        assert_eq!(candidate.live_entries[0].data.as_ref().unwrap().len(), 4096);
        assert_eq!(candidate.live_entries[1].data.as_ref().unwrap().len(), 4096);
        assert!(
            candidate.live_entries[0]
                .data
                .as_ref()
                .unwrap()
                .iter()
                .all(|&b| b == 0xAA)
        );
        assert!(
            candidate.live_entries[1]
                .data
                .as_ref()
                .unwrap()
                .iter()
                .all(|&b| b == 0xBB)
        );
    }

    #[tokio::test]
    async fn fetch_live_bodies_coalesces_adjacent_entries() {
        // Two adjacent entries should land in the same range-GET batch.
        // We verify by checking that both get correct data from a large body
        // with a big dead gap after them.
        let store = make_store();
        let ulid_str = Ulid::from_parts(1000, 4).to_string();
        let body_section_start: u64 = 256;
        let body_size: u64 = 1024 * 1024;

        let mut body = vec![0u8; body_size as usize];
        // Two adjacent entries at offset 0: 4096 + 4096 = 8192 bytes.
        for b in &mut body[0..4096] {
            *b = 0xCC;
        }
        for b in &mut body[4096..8192] {
            *b = 0xDD;
        }
        // Rest is dead gap.
        put_fake_segment(&store, "vol", &ulid_str, body_section_start, &body).await;

        let mut candidate = SegmentStats {
            ulid_str: ulid_str.clone(),
            body_section_start,
            file_size: body_section_start + body_size,
            live_lba_bytes: 8192,
            total_lba_bytes: body_size,
            has_body_entries: true,
            live_entries: vec![
                stub_entry(EntryKind::Data, 0, 4096),
                stub_entry(EntryKind::Data, 4096, 4096),
            ],
            live_entry_indices: vec![0, 1],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };

        // Waste ≈ 1MB, 1 batch (adjacent → coalesced) → waste/batch ≈ 1MB >> 128KB.
        let tmp = TempDir::new().unwrap();
        fetch_live_bodies(&mut candidate, "vol", tmp.path(), &store)
            .await
            .unwrap();

        assert_eq!(
            candidate.live_entries[0].data.as_deref(),
            Some(vec![0xCCu8; 4096].as_slice())
        );
        assert_eq!(
            candidate.live_entries[1].data.as_deref(),
            Some(vec![0xDDu8; 4096].as_slice())
        );
    }

    #[tokio::test]
    async fn fetch_live_bodies_skips_dedup_ref() {
        // DedupRef entries carry no body bytes in the thin format — they
        // must be skipped by fetch_live_bodies entirely, regardless of
        // their `stored_length` field. The canonical Data lives elsewhere
        // and is fetched separately (if at all) when its own segment is
        // in the candidate set.
        let store = make_store();
        let ulid_str = Ulid::from_parts(1000, 5).to_string();

        let mut candidate = SegmentStats {
            ulid_str: ulid_str.clone(),
            body_section_start: 128,
            file_size: 128,
            live_lba_bytes: 4096,
            total_lba_bytes: 4096,
            has_body_entries: false,
            // stored_length=0 matches the real thin-DedupRef format; the
            // filter in fetch_live_bodies now gates on `kind == Data` so
            // the length check is redundant but left for robustness.
            live_entries: vec![stub_entry(EntryKind::DedupRef, 0, 0)],
            live_entry_indices: vec![0],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };

        let tmp = TempDir::new().unwrap();
        fetch_live_bodies(&mut candidate, "vol", tmp.path(), &store)
            .await
            .unwrap();

        assert!(
            candidate.live_entries[0].data.is_none(),
            "DedupRef entries must not have body bytes fetched"
        );
    }

    #[tokio::test]
    async fn fetch_live_bodies_reads_from_local_cache_when_fully_present() {
        // Build a cache/<ulid>.body file with all required DATA extents and
        // a `.present` bitmap with the matching bits set. The in-memory store
        // is empty — if fetch_live_bodies tried to fall back to S3 it would
        // error. The local path must serve the bodies without any S3 GETs.
        let store = make_store();
        let ulid_str = Ulid::from_parts(2000, 1).to_string();
        let tmp = TempDir::new().unwrap();
        let fork_dir = tmp.path();
        let cache_dir = fork_dir.join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        // Body layout: entry_idx 0 at offset 0 (4096 bytes of 0xEE),
        //              entry_idx 1 at offset 4096 (4096 bytes of 0xFF).
        let body_section_start: u64 = 512;
        let mut body = vec![0u8; 8192];
        body[0..4096].fill(0xEE);
        body[4096..8192].fill(0xFF);
        std::fs::write(cache_dir.join(format!("{ulid_str}.body")), &body).unwrap();

        // Entry indices 0 and 1 in a 2-entry segment: a single byte with
        // both low bits set.
        std::fs::write(cache_dir.join(format!("{ulid_str}.present")), [0b0000_0011]).unwrap();

        let mut candidate = SegmentStats {
            ulid_str: ulid_str.clone(),
            body_section_start,
            file_size: body_section_start + 8192,
            live_lba_bytes: 8192,
            total_lba_bytes: 8192,
            has_body_entries: true,
            live_entries: vec![
                stub_entry(EntryKind::Data, 0, 4096),
                stub_entry(EntryKind::Data, 4096, 4096),
            ],
            live_entry_indices: vec![0, 1],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };

        fetch_live_bodies(&mut candidate, "vol", fork_dir, &store)
            .await
            .unwrap();

        assert!(
            candidate.live_entries[0]
                .data
                .as_deref()
                .is_some_and(|d| d.iter().all(|&b| b == 0xEE))
        );
        assert!(
            candidate.live_entries[1]
                .data
                .as_deref()
                .is_some_and(|d| d.iter().all(|&b| b == 0xFF))
        );
    }

    #[tokio::test]
    async fn fetch_live_bodies_falls_back_to_s3_when_present_bit_unset() {
        // cache/<ulid>.body exists but the `.present` bit for entry 1 is not
        // set — a partially demand-fetched segment. fetch_live_bodies must
        // fall through to S3 rather than serve torn bytes.
        let store = make_store();
        let ulid_str = Ulid::from_parts(2000, 2).to_string();
        let tmp = TempDir::new().unwrap();
        let fork_dir = tmp.path();
        let cache_dir = fork_dir.join("cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        // S3 body: byte-distinctive so we can verify it was used, not cache.
        let body_section_start: u64 = 512;
        let mut s3_body = vec![0u8; 8192];
        s3_body[0..4096].fill(0x11);
        s3_body[4096..8192].fill(0x22);
        put_fake_segment(&store, "vol", &ulid_str, body_section_start, &s3_body).await;

        // Local cache: body bytes are *wrong* (0xDE) — present bit is unset
        // so they must never be used. If we accidentally read from the cache,
        // the assertions below will fail.
        let local_body = vec![0xDEu8; 8192];
        std::fs::write(cache_dir.join(format!("{ulid_str}.body")), &local_body).unwrap();
        // Only entry 0 is marked present; entry 1 is missing.
        std::fs::write(cache_dir.join(format!("{ulid_str}.present")), [0b0000_0001]).unwrap();

        let mut candidate = SegmentStats {
            ulid_str: ulid_str.clone(),
            body_section_start,
            file_size: body_section_start + 8192,
            live_lba_bytes: 8192,
            total_lba_bytes: 8192,
            has_body_entries: true,
            live_entries: vec![
                stub_entry(EntryKind::Data, 0, 4096),
                stub_entry(EntryKind::Data, 4096, 4096),
            ],
            live_entry_indices: vec![0, 1],
            removed_hashes: Vec::new(),
            has_partial_death: false,
        };

        fetch_live_bodies(&mut candidate, "vol", fork_dir, &store)
            .await
            .unwrap();

        // Both bodies must have come from S3, not the local cache.
        assert!(
            candidate.live_entries[0]
                .data
                .as_deref()
                .is_some_and(|d| d.iter().all(|&b| b == 0x11))
        );
        assert!(
            candidate.live_entries[1]
                .data
                .as_deref()
                .is_some_and(|d| d.iter().all(|&b| b == 0x22))
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
        let sweep_ulid = Ulid::new();
        let repack_ulid = Ulid::new();
        let stats = gc_fork(dir, "test-vol", &store, &config, repack_ulid, sweep_ulid)
            .await
            .unwrap();
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
        let done = apply_done_handoffs(dir, "test-vol", &store).await.unwrap();
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
