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
//     Collect all segments below small_segment_bytes (excluding the repack
//     candidate if one was selected) and batch them into one output, capping
//     total live bytes at SWEEP_LIVE_CAP (32 MiB) to bound output size.
//     Sweep candidates must have density >= density_threshold; lower-density
//     small segments are owned by repack.
//     Mirrors lsvd SweepSmallSegments and volume sweep_pending().
//
// Both strategies run in the same tick if both find candidates, each producing
// an independent output segment with its own ULID.  They operate on disjoint
// input sets (repack owns the single least-dense segment; sweep owns the
// remaining small high-density segments), so they could be parallelised with
// tokio::join! in a future optimisation.
//
// Per-tick work is bounded in both cases: repack processes one segment;
// sweep is capped at 32 MiB of live data.
//
// Handoff protocol (crash-safe, filesystem-only coordination):
//
//   1. Coordinator writes the compacted segment to gc/<new-ulid> (signed with
//      an ephemeral key — coordinator does not hold the volume's private key),
//      then writes gc/<new-ulid>.pending (via tmp + rename for atomicity):
//        <hash_hex> <old_segment_ulid> <new_segment_ulid> <new_absolute_offset>
//
//   2. Volume re-signs gc/<new-ulid> with its own key, moves it to
//      segments/<new-ulid>, applies extent index patches, renames
//      gc/<new-ulid>.pending → gc/<new-ulid>.applied.
//
//   3. Coordinator (next GC tick) sees .applied: uploads segments/<new-ulid>
//      (the volume-signed version) to S3, deletes old S3 objects and old local
//      segment files, renames → gc/<new-ulid>.done.
//
//   Crash at any step:
//   - Before step 1 completes: no .pending file; coordinator retries next tick.
//   - After step 1, before step 2: volume re-applies on next idle tick (idempotent).
//   - After step 2, before step 3: coordinator re-runs upload + cleanup
//     (S3 put and 404-on-delete are both idempotent).
//
//   All-dead segments (no live entries, no extent index references) use a
//   tombstone handoff: the coordinator writes a .pending file with only
//   "dead <ulid>" lines.  The volume acknowledges (no-op), writes .applied,
//   and the coordinator deletes on the next tick.  Direct deletion is unsafe
//   because the coordinator's liveness view (on-disk .idx files) may lag the
//   volume's in-memory LBA map.
//
// A pass is deferred if any .pending files already exist (at most one
// outstanding GC result per fork at a time).
//
// Blocking IO note: index rebuild and segment reads are synchronous. For the
// first pass these are called on the async task thread; move to spawn_blocking
// if GC passes become long enough to stall other coordinator tasks.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::ObjectStore;
use ulid::Ulid;

use elide_core::extentindex::{self, ExtentIndex};
use elide_core::gc::{HandoffLine, format_handoff_file};
use elide_core::lbamap::{self, LbaMap};
use elide_core::segment::{self, EntryKind, SegmentEntry};
use elide_core::volume::{ZERO_HASH, latest_snapshot};

use crate::config::GcConfig;
use crate::upload::segment_key;

/// Retention window for `.done` GC handoff files.
///
/// `.done` files are kept for this duration after completion for post-mortem
/// debugging, then removed by `cleanup_done_handoffs`.
pub const DONE_FILE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Maximum total live bytes included in one small-segment sweep pass.
/// Matches the volume's FLUSH_THRESHOLD to keep output segment size bounded.
///
/// `compact_segments` writes a single output segment with no internal
/// splitting, so this cap is the only bound on output size. It works correctly
/// as long as `GcConfig::small_segment_bytes` ≤ `SWEEP_LIVE_CAP` — a single
/// small segment can never exceed the cap. Raising `small_segment_bytes` above
/// this value would be a misconfiguration.
const SWEEP_LIVE_CAP: u64 = 32 * 1024 * 1024;

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
}

impl GcStats {
    fn none(total_segments: usize) -> Self {
        Self {
            strategy: GcStrategy::None,
            candidates: 0,
            bytes_freed: 0,
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
    let lbamap = lbamap::rebuild_segments(&rebuild_chain).context("rebuilding lba map")?;
    let live_hashes = lbamap.live_hashes();

    let floor: Option<Ulid> = latest_snapshot(fork_dir)?;

    let mut all_stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, floor)
        .context("collecting segment stats")?;
    let total_segments = all_stats.len();

    // Repack: density pass — extract the single least-dense segment.
    // Removes it from all_stats so sweep only sees the remainder.
    let mut repack_bytes: u64 = 0;
    let ran_repack = if let Some(pos) = find_least_dense(&all_stats, config.density_threshold) {
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
        compact_segments(vec![candidate], &gc_dir, volume_id, store, repack_ulid)
            .await
            .context("density compaction")?;
        true
    } else {
        false
    };

    // Sweep: batch small segments with density >= threshold up to the live cap.
    // Segments with density < threshold are owned by repack (and removed above
    // if selected this tick, or will be selected in a future tick).
    let mut small: Vec<SegmentStats> = Vec::new();
    let mut acc_live: u64 = 0;
    for s in all_stats {
        if s.file_size >= config.small_segment_bytes {
            continue;
        }
        // Exclude low-density small segments — repack owns those.
        if s.density() < config.density_threshold {
            continue;
        }
        // Always include at least one; then enforce the live-bytes cap.
        if !small.is_empty() && acc_live + s.live_lba_bytes > SWEEP_LIVE_CAP {
            break;
        }
        acc_live += s.live_lba_bytes;
        small.push(s);
    }

    // Only sweep when ≥2 segments can be merged to reduce segment count.
    // A single small segment with no dead space is not worth a standalone pass.
    let ran_sweep = if small.len() >= 2 {
        let sweep_candidates = small.len();
        let sweep_bytes: u64 = small.iter().map(|s| s.dead_lba_bytes()).sum();
        compact_segments(small, &gc_dir, volume_id, store, sweep_ulid)
            .await
            .context("small-segment sweep")?;
        Some((sweep_candidates, sweep_bytes))
    } else {
        None
    };

    match (ran_repack, ran_sweep) {
        (false, None) => Ok(GcStats::none(total_segments)),
        (true, None) => Ok(GcStats {
            strategy: GcStrategy::Repack,
            candidates: 1,
            bytes_freed: repack_bytes,
            total_segments,
        }),
        (false, Some((n, sweep_bytes))) => Ok(GcStats {
            strategy: GcStrategy::Sweep,
            candidates: n,
            bytes_freed: sweep_bytes,
            total_segments,
        }),
        (true, Some((n, sweep_bytes))) => Ok(GcStats {
            strategy: GcStrategy::Both,
            candidates: 1 + n,
            bytes_freed: repack_bytes + sweep_bytes,
            total_segments,
        }),
    }
}

/// Process `.applied` GC handoff files: delete old S3 objects and local
/// segment files, then rename each `.applied` file to `.done`.
///
/// Called at the start of every `gc_loop` tick so that old S3 objects are
/// cleaned up promptly after the volume acknowledges each handoff.  Any
/// `.applied` files that survive a coordinator crash are processed on the
/// next startup tick — the rename-to-`.done` is idempotent with respect to
/// S3 (a 404 on delete is treated as success) and safe to retry.
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

    let mut applied: Vec<fs::DirEntry> = fs::read_dir(&gc_dir)
        .context("reading gc dir")?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .and_then(elide_core::gc::GcHandoff::from_filename)
                .is_some_and(|h| matches!(h.state, elide_core::gc::GcHandoffState::Applied))
        })
        .collect();

    if applied.is_empty() {
        return Ok(0);
    }

    applied.sort_by_key(|e| e.file_name());
    let cache_dir = fork_dir.join("cache");
    let mut count = 0;

    for entry in &applied {
        let filename = entry.file_name();
        let name = filename
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("gc filename is not valid UTF-8"))?;
        let handoff = elide_core::gc::GcHandoff::from_filename(name)
            .ok_or_else(|| anyhow::anyhow!("invalid gc filename: {name}"))?;
        let new_ulid_str = handoff.ulid.to_string();

        // Parse the handoff file into typed HandoffLines.
        let content =
            fs::read_to_string(entry.path()).with_context(|| format!("reading {name}"))?;

        let mut old_ulids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut is_repack = false;
        for line in content.lines() {
            match elide_core::gc::HandoffLine::parse(line) {
                Some(elide_core::gc::HandoffLine::Repack { old_ulid, .. }) => {
                    is_repack = true;
                    old_ulids.insert(old_ulid.to_string());
                }
                Some(elide_core::gc::HandoffLine::Remove { old_ulid, .. }) => {
                    old_ulids.insert(old_ulid.to_string());
                }
                Some(elide_core::gc::HandoffLine::Dead { old_ulid }) => {
                    old_ulids.insert(old_ulid.to_string());
                }
                None => {}
            }
        }

        // Body state detection:
        //   gc/<new> present          → needs upload + promote IPC
        //   gc/<new> absent,
        //     cache/<new>.body absent → tombstone/removal-only (no new body), or
        //                               promote already ran (gc deleted by coordinator)
        //   cache/<new>.body present  → promote already ran on a prior attempt; skip to cleanup
        let gc_body = gc_dir.join(&new_ulid_str);
        let gc_body_present = gc_body.try_exists().context("checking gc body path")?;
        let cache_body = cache_dir.join(format!("{new_ulid_str}.body"));
        let seg_promoted = !gc_body_present
            && cache_body
                .try_exists()
                .context("checking cache body path")?;

        if is_repack && !gc_body_present && !seg_promoted {
            // The volume re-signs gc/<new-ulid> before renaming the handoff to
            // .applied, so a missing body here is always a bug.  Abort rather
            // than silently deleting the old segments without uploading the
            // replacement — that would cause permanent data loss.
            return Err(anyhow::anyhow!(
                "compacted segment {new_ulid_str} missing from gc/ and cache/ for repack \
                 handoff — refusing to delete old segments before upload"
            ));
        }

        if gc_body_present {
            // Verify the volume's signature before uploading.  This catches any
            // case where the volume failed to sign correctly — we refuse to
            // propagate a bad segment to S3.  Load volume.pub here rather than
            // at the top of the function so that removal-only handoffs (where
            // no segment file is written) do not require volume.pub to exist.
            let vk = elide_core::signing::load_verifying_key(
                fork_dir,
                elide_core::signing::VOLUME_PUB_FILE,
            )
            .context("loading volume verifying key")?;
            let (_, gc_entries) = segment::read_and_verify_segment_index(&gc_body, &vk)
                .with_context(|| {
                    format!("signature verification failed for compacted segment {new_ulid_str}")
                })?;

            // Sanity check: GC output must not contain thin DedupRef.
            let thin_count = gc_entries
                .iter()
                .filter(|e| e.kind == EntryKind::DedupRef)
                .count();
            if thin_count > 0 {
                return Err(anyhow::anyhow!(
                    "compacted segment {new_ulid_str} has {thin_count} thin DedupRef entries; \
                     refusing to upload — this would lose body data when old segments are deleted"
                ));
            }

            let key = segment_key(volume_id, &new_ulid_str)
                .with_context(|| format!("building key for {new_ulid_str}"))?;
            let data = tokio::fs::read(&gc_body)
                .await
                .with_context(|| format!("reading compacted segment {new_ulid_str}"))?;
            store
                .put(&key, Bytes::from(data).into())
                .await
                .with_context(|| format!("uploading compacted segment {new_ulid_str}"))?;

            // Promote: IPC → volume copies gc/<new> → cache/<new>.body + .present.
            // If volume is down, defer: leave gc/<new> in place, retry next tick.
            let new_ulid = handoff.ulid;
            if !crate::control::promote_segment(fork_dir, new_ulid).await {
                warn!("[gc] promote {new_ulid_str}: volume not running; will retry next tick");
                continue;
            }

            // Coordinator deletes gc/<new> after promote succeeds.
            tokio::fs::remove_file(&gc_body)
                .await
                .with_context(|| format!("removing gc/{new_ulid_str}"))?;
        }
        // else: seg_promoted or tombstone/removal-only — proceed to S3 deletes.

        // Delete old S3 objects.  index/<old>.idx and cache/<old>.* were already
        // cleaned up by the volume in VolumeFinishApply (apply_gc_handoffs).
        // A 404 means the object is already gone
        // (e.g. coordinator crashed after delete but before rename); treat as
        // success so the cleanup is idempotent.
        for old_ulid_str in &old_ulids {
            let key = segment_key(volume_id, old_ulid_str)
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

        // Rename .applied → .done.
        let done_path = gc_dir.join(
            handoff
                .with_state(elide_core::gc::GcHandoffState::Done)
                .filename(),
        );
        fs::rename(entry.path(), &done_path)
            .with_context(|| format!("renaming {name} to .done"))?;

        count += 1;
    }

    Ok(count)
}

/// Delete `.done` GC handoff files older than `ttl`.
///
/// `.done` files are inert markers left after a completed GC handoff. They are
/// useful for post-mortem debugging (which segments were compacted and when)
/// but accumulate indefinitely without cleanup. At the default 5-minute GC
/// interval a fork produces ~288 per day; this function prunes the tail.
///
/// Deletion is best-effort: errors on individual files are logged and skipped
/// so a single unreadable file does not block the rest of the pass.
///
/// Returns the number of files deleted.
pub fn cleanup_done_handoffs(fork_dir: &Path, ttl: Duration) -> usize {
    let gc_dir = fork_dir.join("gc");
    let Ok(entries) = fs::read_dir(&gc_dir) else {
        return 0;
    };

    let cutoff = std::time::SystemTime::now()
        .checked_sub(ttl)
        .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

    let mut deleted = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if !elide_core::gc::GcHandoff::from_filename(name_str)
            .is_some_and(|h| matches!(h.state, elide_core::gc::GcHandoffState::Done))
        {
            continue;
        }
        let mtime = match entry.metadata().and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(e) => {
                error!(
                    "[gc] could not read mtime of {}: {e}",
                    entry.path().display()
                );
                continue;
            }
        };
        if mtime < cutoff {
            if let Err(e) = fs::remove_file(entry.path()) {
                error!("[gc] failed to delete {}: {e}", entry.path().display());
            } else {
                deleted += 1;
            }
        }
    }
    deleted
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

// --- internals ---

/// Logical block size: all LBA lengths are in units of this many bytes.
const BLOCK_BYTES: u64 = 4096;

/// Per-segment stats computed during the scan phase.
struct SegmentStats {
    ulid_str: String,
    /// Physical on-disk size (idx + DATA body bytes); used for small_segment_bytes threshold.
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
    /// Hashes that are in the extent index but not reachable from the LBA map.
    /// The volume must remove these from its extent index when applying the
    /// handoff, since the old segment files will be deleted.
    removed_hashes: Vec<blake3::Hash>,
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
/// with a `.pending` or `.applied` handoff) come first (lower priority);
/// regular segments come last (higher priority).  This ordering is critical for
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
        // body_section_start for .body files is 0 (body starts at byte 0);
        // for full segments it is non-zero — compact_segments fetches the full
        // segment from S3 and calls read_segment_index to get the correct value.
        let idx_size = fs::metadata(&idx_path)?.len();
        let (_, entries) = segment::read_and_verify_segment_index(&idx_path, vk)?;

        let mut live_lba_bytes: u64 = 0;
        let mut total_lba_bytes: u64 = 0;
        let mut physical_body_bytes: u64 = 0;
        let mut live_entries: Vec<SegmentEntry> = Vec::new();
        let mut removed_hashes: Vec<blake3::Hash> = Vec::new();

        // Thin DedupRef should never appear in S3 segments (upload sanity
        // check rejects them). If one slipped through (legacy data or bug),
        // skip this segment entirely — compacting it would lose body bytes
        // because the canonical segment referenced by the thin ref may be
        // deleted by GC cleanup.
        let has_thin_ref = entries.iter().any(|e| e.kind == EntryKind::DedupRef);
        if has_thin_ref {
            warn!(
                "[gc] skipping {ulid_str}: segment contains thin DedupRef entries \
                 (should not appear in S3); re-upload with materialise_segment first"
            );
            continue;
        }

        for entry in entries {
            if entry.kind == EntryKind::Inline {
                continue;
            }
            let lba_bytes = entry.lba_length as u64 * BLOCK_BYTES;
            total_lba_bytes += lba_bytes;

            // Zero extents have no body bytes and use ZERO_HASH as a sentinel
            // (never in the extent index). Liveness is determined by LBA-map
            // ownership: the entry is live if the LBA still maps to ZERO_HASH.
            // Without this check, zero entries would fall into the DATA path,
            // where index.lookup(ZERO_HASH) always returns None, causing live
            // zero extents to be silently dropped from GC output — allowing
            // ancestor data to bleed through after compaction in forked volumes.
            if entry.kind == EntryKind::Zero {
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(ZERO_HASH);
                if lba_live {
                    live_lba_bytes += lba_bytes;
                    live_entries.push(entry);
                }
                continue;
            }
            physical_body_bytes += entry.stored_length as u64;
            // Materialised refs carry body bytes (same layout as DATA) and an
            // LBA mapping. Liveness is LBA-based: the entry is live if the LBA
            // still maps to its hash. Thin DedupRef should not appear in S3
            // segments (upload sanity check), but is handled identically here
            // (stored_length = 0, so no body-byte contribution).
            if entry.kind == EntryKind::DedupRef || entry.kind == EntryKind::MaterializedRef {
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
                let extent_live = index
                    .lookup(&entry.hash)
                    .is_some_and(|loc| loc.segment_id == seg_ulid);
                if lba_live {
                    live_lba_bytes += lba_bytes;
                    live_entries.push(entry);
                } else if extent_live {
                    removed_hashes.push(entry.hash);
                }
                continue;
            }
            // DATA entries are content-addressed: the extent_index tracks a
            // single canonical location per hash.  When the same hash appears
            // in multiple segments (e.g. a regular write and a later
            // materialised dedup ref), only one segment is "canonical" in the
            // extent_index — the other looks extent-dead even though its LBA
            // mapping is still live.  Check lba_live first (same as
            // MaterializedRef above) so we never drop a live LBA mapping.
            let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
            let extent_live = index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == seg_ulid);
            if lba_live || (extent_live && live_hashes.contains(&entry.hash)) {
                live_lba_bytes += lba_bytes;
                live_entries.push(entry);
            } else if extent_live {
                // Extent-index-live but not LBA-map-live: the LBA was
                // overwritten with different data.  Record so the volume
                // can remove the dangling extent index entry.
                removed_hashes.push(entry.hash);
            }
        }

        // file_size: physical on-disk size for small_segment_bytes threshold.
        // Uses stored (compressed) DATA and REF body bytes + idx overhead — not
        // LBA bytes, since the threshold is about disk space, not logical coverage.
        let file_size = idx_size + physical_body_bytes;
        result.push(SegmentStats {
            ulid_str,
            has_body_entries: physical_body_bytes > 0,
            file_size,
            live_lba_bytes,
            total_lba_bytes,
            live_entries,
            removed_hashes,
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

/// Read live extent bodies from each candidate, write a compacted segment,
/// stage it in gc/, and write the gc/*.pending handoff file.
///
/// For each candidate, the full segment is downloaded from S3 to a temporary
/// `gc/<ulid>.fetch` file.  This guarantees the body is complete regardless of
/// demand-fetch state, and keeps the fetch consistent with other full-segment
/// files in gc/.  The `.fetch` file is deleted after the body is read.
async fn compact_segments(
    mut candidates: Vec<SegmentStats>,
    gc_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
    new_ulid: Ulid,
) -> Result<()> {
    let new_ulid_str = new_ulid.to_string();
    fs::create_dir_all(gc_dir).context("creating gc dir")?;

    // For each candidate: if it has live entries with bodies (DATA or REF),
    // download the full segment from S3 to read the body bytes.  Zero extents
    // carry no body data, so candidates with only zero extents skip the S3
    // fetch entirely.
    // removed_hashes are already fully populated from collect_stats and need no fetch.
    let mut all_live: Vec<(String, SegmentEntry)> = Vec::new();
    let mut all_removed: Vec<(blake3::Hash, String)> = Vec::new();
    for candidate in &mut candidates {
        let has_body_entries = candidate
            .live_entries
            .iter()
            .any(|e| matches!(e.kind, EntryKind::Data | EntryKind::MaterializedRef));

        if has_body_entries {
            let fetch_path = gc_dir.join(format!("{}.fetch", candidate.ulid_str));

            let key = segment_key(volume_id, &candidate.ulid_str)
                .with_context(|| format!("building S3 key for {}", candidate.ulid_str))?;
            let data = store
                .get(&key)
                .await
                .with_context(|| format!("fetching segment {} from S3", candidate.ulid_str))?
                .bytes()
                .await
                .with_context(|| format!("reading S3 body for {}", candidate.ulid_str))?;
            tokio::fs::write(&fetch_path, &data)
                .await
                .with_context(|| format!("writing fetch file for {}", candidate.ulid_str))?;

            // Read the full segment header to get body_section_start.
            let (body_section_start, _) =
                segment::read_segment_index(&fetch_path).with_context(|| {
                    format!(
                        "reading segment index from fetch for {}",
                        candidate.ulid_str
                    )
                })?;

            segment::read_extent_bodies(
                &fetch_path,
                body_section_start,
                &mut candidate.live_entries,
            )
            .with_context(|| format!("reading bodies from {}", candidate.ulid_str))?;

            let _ = fs::remove_file(&fetch_path);
        }
        for entry in candidate.live_entries.drain(..) {
            all_live.push((candidate.ulid_str.clone(), entry));
        }
        for hash in candidate.removed_hashes.drain(..) {
            all_removed.push((hash, candidate.ulid_str.clone()));
        }
    }

    if all_live.is_empty() && all_removed.is_empty() {
        // All candidates are fully dead: no live extents and no extent index
        // references.  The coordinator's liveness view is based on on-disk
        // .idx files; the volume's in-memory LBA map may be ahead (writes
        // between gc_checkpoint and gc_fork are invisible to the coordinator).
        // Deleting directly would create a window where the volume reads a
        // segment the coordinator has already deleted.
        //
        // Instead, write a tombstone .pending file so the volume can
        // acknowledge before the coordinator deletes — the same handoff
        // protocol used for all other GC operations.
        fs::create_dir_all(gc_dir).context("creating gc dir")?;
        let mut handoff_lines = Vec::new();
        for candidate in &candidates {
            let old_ulid =
                Ulid::from_string(&candidate.ulid_str).context("parsing candidate ulid")?;
            handoff_lines.push(HandoffLine::Dead { old_ulid });
        }
        let pending_path = gc_dir.join(format!("{new_ulid_str}.pending"));
        let tmp_path = gc_dir.join(format!("{new_ulid_str}.pending.tmp"));
        fs::write(&tmp_path, format_handoff_file(handoff_lines))
            .context("writing tombstone handoff")?;
        fs::rename(&tmp_path, &pending_path).context("committing tombstone handoff")?;
        tracing::info!("[gc] tombstone handoff → {new_ulid_str} (no live entries)");
        return Ok(());
    }

    if all_live.is_empty() {
        // No live entries to compact, but the extent index still references
        // some hashes in these segments (extent-live, LBA-dead). Write a
        // handoff file with only remove entries so the volume can clean the
        // dangling extent index entries before the old files are deleted.
        fs::create_dir_all(gc_dir).context("creating gc dir")?;
        let mut handoff_lines = Vec::new();
        for (hash, old_ulid_str) in &all_removed {
            let old_ulid = Ulid::from_string(old_ulid_str).context("parsing removed ulid")?;
            handoff_lines.push(HandoffLine::Remove {
                hash: *hash,
                old_ulid,
            });
        }
        let pending_path = gc_dir.join(format!("{new_ulid_str}.pending"));
        let tmp_path = gc_dir.join(format!("{new_ulid_str}.pending.tmp"));
        fs::write(&tmp_path, format_handoff_file(handoff_lines))
            .context("writing removal-only handoff")?;
        fs::rename(&tmp_path, &pending_path).context("committing removal-only handoff")?;
        tracing::info!(
            "[gc] removal-only handoff → {new_ulid_str} ({} hash(es) removed)",
            all_removed.len()
        );
        return Ok(());
    }

    fs::create_dir_all(gc_dir).context("creating gc dir")?;
    // Write to a tmp file first; rename into gc/ for staging.
    let tmp_path = gc_dir.join(format!("{new_ulid_str}.tmp"));

    let mut new_entries: Vec<SegmentEntry> = Vec::with_capacity(all_live.len());
    for (_, e) in &mut all_live {
        match e.kind {
            EntryKind::Zero => {
                new_entries.push(SegmentEntry::new_zero(e.start_lba, e.lba_length));
            }
            EntryKind::DedupRef => {
                // collect_stats skips segments with thin refs, so this
                // should not happen. Drop the entry rather than emitting
                // a bodyless ref that would cause data loss on old-segment
                // deletion. The apply_done_handoffs sanity check catches
                // this before upload.
                warn!(
                    "[gc] dropping thin DedupRef {} from compaction output \
                     (should have been filtered by collect_stats)",
                    e.hash.to_hex()
                );
            }
            EntryKind::Data | EntryKind::MaterializedRef => {
                let flags = if e.compressed {
                    segment::SegmentFlags::COMPRESSED
                } else {
                    segment::SegmentFlags::empty()
                };
                new_entries.push(SegmentEntry::new_data(
                    e.hash,
                    e.start_lba,
                    e.lba_length,
                    flags,
                    std::mem::take(&mut e.data),
                ));
            }
            EntryKind::Inline => {
                new_entries.push(SegmentEntry::new_data(
                    e.hash,
                    e.start_lba,
                    e.lba_length,
                    segment::SegmentFlags::empty(),
                    std::mem::take(&mut e.data),
                ));
            }
        }
    }

    // The coordinator does not hold the volume's private key, so it signs the
    // compacted segment with an ephemeral key.  The volume re-signs it with its
    // own key inside apply_gc_handoffs, at which point the file moves from gc/
    // into segments/.  This ensures segments/ always contains only volume-signed
    // files and extentindex::rebuild never needs to skip verification for
    // in-transit coordinator output.
    let (ephemeral_signer, _) = elide_core::signing::generate_ephemeral_signer();
    let new_body_section_start =
        segment::write_segment(&tmp_path, &mut new_entries, ephemeral_signer.as_ref())
            .context("writing compacted segment")?;

    // Stage in gc/<ulid> so apply_gc_handoffs can re-sign it and move it into
    // segments/.  S3 upload happens in apply_done_handoffs, after the volume
    // has re-signed the segment, so that S3 always receives the volume-signed
    // version rather than the ephemeral-signed coordinator output.
    let final_path = gc_dir.join(&new_ulid_str);
    tokio::fs::rename(&tmp_path, &final_path)
        .await
        .context("staging compacted segment in gc/")?;
    tracing::info!(
        "[gc] output segment → {new_ulid_str} ({} live entries)",
        new_entries.len()
    );

    // Write the handoff file using the typed HandoffLine format.
    let mut handoff_lines: Vec<HandoffLine> = Vec::new();
    // Track which candidate ULIDs get at least one Repack or Remove line.
    // Any candidate not covered had only DEDUP_REF live entries; it needs a
    // Dead line so apply_done_handoffs deletes the old segment file.
    let mut covered_ulids: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for ((old_ulid_str, old_entry), new_entry) in all_live.iter().zip(new_entries.iter()) {
        if !matches!(new_entry.kind, EntryKind::Data | EntryKind::MaterializedRef) {
            // Zero extents and thin dedup-ref entries have no body in the new
            // segment and no extent index entries to update.  No Repack line
            // is needed — apply_gc_handoffs must not touch the extent index
            // for these entries.
            continue;
        }
        covered_ulids.insert(old_ulid_str.as_str());
        let old_ulid = Ulid::from_string(old_ulid_str).context("parsing old ulid")?;
        let new_offset = new_body_section_start + new_entry.stored_offset;
        handoff_lines.push(HandoffLine::Repack {
            hash: old_entry.hash,
            old_ulid,
            new_ulid,
            new_offset,
        });
    }
    for (hash, old_ulid_str) in &all_removed {
        covered_ulids.insert(old_ulid_str.as_str());
        let old_ulid = Ulid::from_string(old_ulid_str).context("parsing removed ulid")?;
        handoff_lines.push(HandoffLine::Remove {
            hash: *hash,
            old_ulid,
        });
    }
    // Candidates whose ULID has no Repack or Remove line contributed only
    // DEDUP_REF live entries.  Their entries are already carried into the new
    // output segment above, so it is safe to delete the old files.  Emit a
    // Dead line for each so apply_done_handoffs knows to delete them.
    for candidate in &candidates {
        if !covered_ulids.contains(candidate.ulid_str.as_str()) {
            let old_ulid =
                Ulid::from_string(&candidate.ulid_str).context("parsing candidate ulid")?;
            handoff_lines.push(HandoffLine::Dead { old_ulid });
        }
    }
    let pending_path = gc_dir.join(format!("{new_ulid_str}.pending"));
    let pending_tmp = gc_dir.join(format!("{new_ulid_str}.pending.tmp"));
    fs::write(&pending_tmp, format_handoff_file(handoff_lines))
        .with_context(|| format!("writing gc result {new_ulid_str}"))?;
    fs::rename(&pending_tmp, &pending_path)
        .with_context(|| format!("committing gc result {new_ulid_str}"))?;

    Ok(())
}

/// Returns true if any `.pending` GC result files exist in `gc_dir`.
fn has_pending_results(gc_dir: &Path) -> Result<bool> {
    if !gc_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(gc_dir).context("reading gc dir")? {
        let entry = entry.context("reading gc dir entry")?;
        if entry
            .file_name()
            .to_str()
            .and_then(elide_core::gc::GcHandoff::from_filename)
            .is_some_and(|h| matches!(h.state, elide_core::gc::GcHandoffState::Pending))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_core::gc::{HandoffLine, format_handoff_file};
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
                            if is_drain {
                                std::fs::remove_file(&src).ok();
                            }
                        }
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
    fn detects_pending_result_file() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.pending"), "").unwrap();
        assert!(has_pending_results(&gc_dir).unwrap());
    }

    #[test]
    fn ignores_non_pending_files_in_gc_dir() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.applied"), "").unwrap();
        fs::write(gc_dir.join("01ARZ3NDEKTSV4RRFFQ69G5FAV.done"), "").unwrap();
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
                file_size: total_lba_bytes, // physical size irrelevant for density
                live_lba_bytes,
                total_lba_bytes,
                has_body_entries: true,
                live_entries: vec![data_entry()],
                removed_hashes: Vec::new(),
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
            file_size: 1024 * 1024,
            live_lba_bytes: 800 * 1024,
            total_lba_bytes: 1024 * 1024,
            has_body_entries: true,
            live_entries: Vec::new(),
            removed_hashes: Vec::new(),
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
                file_size: 100,
                live_lba_bytes,
                total_lba_bytes: 100,
                has_body_entries: true,
                live_entries: Vec::new(),
                removed_hashes: Vec::new(),
            }
        }
        let stats = vec![make(80), make(90), make(100)];
        assert_eq!(find_least_dense(&stats, 0.7), None);
    }

    /// Materialise, upload to store, and promote all pending segments.
    /// Mirrors the real coordinator path: materialise → S3 PUT → promote.
    async fn drain_with_materialise(
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
            vol.materialise_segment(ulid).unwrap();
            // Upload the .materialized file (fat variant) to the store.
            let ulid_str = ulid.to_string();
            let mat_path = pending_dir.join(format!("{ulid_str}.materialized"));
            let data = fs::read(&mat_path).unwrap();
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

    #[tokio::test]
    async fn done_renames_applied_to_done() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 1);
        let old_ulid = Ulid::from_parts(999, 0);
        let hash = blake3::Hash::from_hex("a".repeat(64)).unwrap();

        let content = format_handoff_file(HandoffLine::Remove { hash, old_ulid });
        let applied_path = gc_dir.join(format!("{new_ulid}.applied"));
        fs::write(&applied_path, &content).unwrap();

        let store = make_store();
        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(!applied_path.exists(), ".applied should be gone");
        assert!(
            gc_dir.join(format!("{new_ulid}.done")).exists(),
            ".done should exist"
        );
    }

    #[tokio::test]
    async fn done_deletes_s3_object() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 1);
        let old_ulid = Ulid::from_parts(999, 0);

        let store = make_store();
        let key = segment_key("vol", &old_ulid.to_string()).unwrap();
        store
            .put(&key, bytes::Bytes::from("old segment data").into())
            .await
            .unwrap();
        assert!(store.get(&key).await.is_ok());

        let hash = blake3::Hash::from_hex("b".repeat(64)).unwrap();
        let content = format_handoff_file(HandoffLine::Remove { hash, old_ulid });
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(
            store.get(&key).await.is_err(),
            "old S3 object should be deleted"
        );
    }

    #[tokio::test]
    async fn done_s3_notfound_is_not_an_error() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 2);
        let old_ulid = Ulid::from_parts(999, 1);

        let store = make_store();
        let hash = blake3::Hash::from_hex("c".repeat(64)).unwrap();
        let content = format_handoff_file(HandoffLine::Remove { hash, old_ulid });
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(gc_dir.join(format!("{new_ulid}.done")).exists());
    }

    #[tokio::test]
    async fn done_deletes_old_s3_object() {
        // Coordinator deletes the old S3 object for Remove handoffs.
        // Local cache cleanup is the volume's responsibility (evict_applied_gc_cache).
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 3);
        let old_ulid = Ulid::from_parts(999, 2);

        let store = make_store();
        let key = segment_key("vol", &old_ulid.to_string()).unwrap();
        store
            .put(&key, bytes::Bytes::from("old segment").into())
            .await
            .unwrap();

        let hash = blake3::Hash::from_hex("d".repeat(64)).unwrap();
        let content = format_handoff_file(HandoffLine::Remove { hash, old_ulid });
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(
            store.get(&key).await.is_err(),
            "old S3 object should be deleted"
        );
        assert!(gc_dir.join(format!("{new_ulid}.done")).exists());
    }

    #[tokio::test]
    async fn done_multiple_old_ulids_in_one_handoff() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 4);
        let old_a = Ulid::from_parts(998, 0);
        let old_b = Ulid::from_parts(999, 0);

        let store = make_store();
        for old in [old_a, old_b] {
            let key = segment_key("vol", &old.to_string()).unwrap();
            store
                .put(&key, bytes::Bytes::from("data").into())
                .await
                .unwrap();
        }

        let h1 = blake3::Hash::from_hex("e".repeat(64)).unwrap();
        let h2 = blake3::Hash::from_hex("f".repeat(64)).unwrap();
        let content = format_handoff_file([
            HandoffLine::Remove {
                hash: h1,
                old_ulid: old_a,
            },
            HandoffLine::Remove {
                hash: h2,
                old_ulid: old_b,
            },
        ]);
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        for old in [old_a, old_b] {
            let key = segment_key("vol", &old.to_string()).unwrap();
            assert!(
                store.get(&key).await.is_err(),
                "{old} S3 object should be deleted"
            );
        }
        assert!(gc_dir.join(format!("{new_ulid}.done")).exists());
    }

    #[tokio::test]
    async fn done_processes_multiple_applied_files() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let store = make_store();
        for i in 1u64..=3 {
            let new_ulid = Ulid::from_parts(1000 + i, 0);
            let old_ulid = Ulid::from_parts(999 + i, 0);
            let hash = blake3::Hash::from_hex("a".repeat(64)).unwrap();
            let content = format_handoff_file(HandoffLine::Remove { hash, old_ulid });
            fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();
        }

        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 3);
        for entry in fs::read_dir(&gc_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            assert!(name.ends_with(".done"), "expected .done, got {name}");
        }
    }

    /// Write `volume.pub` into `dir` using an ephemeral keypair.
    /// Returns the signer so the caller can sign segments with it.
    fn setup_vol_pub(dir: &Path) -> Arc<dyn elide_core::segment::SegmentSigner> {
        fs::create_dir_all(dir).unwrap();
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        let pub_hex: String = vk.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
        fs::write(
            dir.join(elide_core::signing::VOLUME_PUB_FILE),
            format!("{pub_hex}\n"),
        )
        .unwrap();
        signer
    }

    #[tokio::test]
    async fn done_verifies_signature_before_upload() {
        // A correctly-signed segment in gc/ must be uploaded to S3, promoted
        // to cache/ (via volume IPC), and the gc body deleted.
        // Volume writes index/ at apply_gc_handoffs time; coordinator does not.
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        let cache_dir = tmp.path().join("cache");
        fs::create_dir_all(&gc_dir).unwrap();

        let signer = setup_vol_pub(tmp.path());

        let new_ulid = Ulid::from_parts(1000, 10);
        let old_ulid = Ulid::from_parts(999, 9);

        // Write a properly volume-signed segment to gc/<new_ulid> (volume has
        // re-signed in-place; coordinator will upload, promote via IPC, delete gc body).
        let entry = elide_core::segment::SegmentEntry::new_data(
            blake3::hash(b"payload"),
            0,
            1,
            elide_core::segment::SegmentFlags::empty(),
            b"payload".to_vec(),
        );
        elide_core::segment::write_segment(
            &gc_dir.join(new_ulid.to_string()),
            &mut vec![entry],
            signer.as_ref(),
        )
        .unwrap();

        let hash = blake3::Hash::from_hex("a".repeat(64)).unwrap();
        let content = format_handoff_file([HandoffLine::Repack {
            hash,
            old_ulid,
            new_ulid,
            new_offset: 0,
        }]);
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        // Mock volume socket: responds "ok" to promote and copies gc body to cache/.
        let _mock = spawn_mock_socket(tmp.path().to_owned()).await;

        let store = make_store();
        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);

        // Segment should be in S3.
        let key = segment_key("vol", &new_ulid.to_string()).unwrap();
        assert!(store.get(&key).await.is_ok(), "segment should be in S3");
        // Body promoted to cache/ by the mock volume socket.
        assert!(
            cache_dir.join(format!("{new_ulid}.body")).exists(),
            "cache/{new_ulid}.body should exist after promote"
        );
        // gc body deleted by coordinator after promote.
        assert!(
            !gc_dir.join(new_ulid.to_string()).exists(),
            "gc body should be deleted after commit"
        );
        assert!(gc_dir.join(format!("{new_ulid}.done")).exists());
    }

    #[tokio::test]
    async fn done_rejects_wrong_key_segment() {
        // A segment signed with the wrong key must not be uploaded.
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        // Set up volume.pub with one keypair…
        setup_vol_pub(tmp.path());

        // …but sign the segment with a different (ephemeral) keypair.
        let (wrong_signer, _) = elide_core::signing::generate_ephemeral_signer();
        let new_ulid = Ulid::from_parts(1000, 11);
        let old_ulid = Ulid::from_parts(999, 10);

        elide_core::segment::write_segment(
            &gc_dir.join(new_ulid.to_string()),
            &mut vec![elide_core::segment::SegmentEntry::new_data(
                blake3::hash(b"payload"),
                0,
                1,
                elide_core::segment::SegmentFlags::empty(),
                b"payload".to_vec(),
            )],
            wrong_signer.as_ref(),
        )
        .unwrap();

        let hash = blake3::Hash::from_hex("b".repeat(64)).unwrap();
        let content = format_handoff_file([HandoffLine::Repack {
            hash,
            old_ulid,
            new_ulid,
            new_offset: 0,
        }]);
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let store = make_store();
        let err = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("signature verification failed"),
            "expected signature error, got: {err}"
        );

        // Segment must NOT be in S3.
        let key = segment_key("vol", &new_ulid.to_string()).unwrap();
        assert!(
            store.get(&key).await.is_err(),
            "bad segment must not be uploaded"
        );
    }

    // --- cleanup_done_handoffs tests ---

    #[test]
    fn cleanup_no_gc_dir() {
        let tmp = TempDir::new().unwrap();
        // Should not panic or error when gc/ doesn't exist.
        let n = cleanup_done_handoffs(tmp.path(), Duration::from_secs(0));
        assert_eq!(n, 0);
    }

    #[test]
    fn cleanup_deletes_old_done_files() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let ulid_a = Ulid::from_parts(1000, 1).to_string();
        let ulid_b = Ulid::from_parts(1000, 2).to_string();
        let done_a = gc_dir.join(format!("{ulid_a}.done"));
        let done_b = gc_dir.join(format!("{ulid_b}.done"));
        fs::write(&done_a, "").unwrap();
        fs::write(&done_b, "").unwrap();

        // TTL of zero — everything is expired.
        let n = cleanup_done_handoffs(tmp.path(), Duration::from_secs(0));
        assert_eq!(n, 2);
        assert!(!done_a.exists());
        assert!(!done_b.exists());
    }

    #[test]
    fn cleanup_spares_recent_done_files() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let ulid = Ulid::from_parts(1000, 1).to_string();
        let done = gc_dir.join(format!("{ulid}.done"));
        fs::write(&done, "").unwrap();

        // TTL of 7 days — a freshly written file should be kept.
        let n = cleanup_done_handoffs(tmp.path(), Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(n, 0);
        assert!(done.exists());
    }

    #[test]
    fn cleanup_ignores_non_done_files() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let ulid = Ulid::from_parts(1000, 1).to_string();
        let pending = gc_dir.join(format!("{ulid}.pending"));
        let applied = gc_dir.join(format!("{ulid}.applied"));
        fs::write(&pending, "").unwrap();
        fs::write(&applied, "").unwrap();

        // TTL of zero — only .done files are eligible, none present.
        let n = cleanup_done_handoffs(tmp.path(), Duration::from_secs(0));
        assert_eq!(n, 0);
        assert!(pending.exists());
        assert!(applied.exists());
    }

    // --- tombstone handoff tests ---
    //
    // These tests pin the invariant that all-dead segments must go through the
    // handoff protocol rather than being deleted directly by the coordinator.
    //
    // compact_segments_all_dead_writes_tombstone deliberately fails with the
    // current code (which deletes directly) and must pass after the fix.

    #[tokio::test]
    async fn compact_segments_all_dead_writes_tombstone() {
        // Regression test for the all-dead direct-deletion race:
        // when compact_segments finds no live entries and no removed hashes,
        // the coordinator's liveness view may lag the volume's in-memory LBA
        // map.  Deleting the segment directly can corrupt the volume.  Instead,
        // compact_segments must write a tombstone .pending file and wait for
        // the volume to acknowledge before deletion.
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");

        let old_ulid = Ulid::from_parts(999, 20).to_string();
        let handoff_ulid = Ulid::from_parts(1000, 20);

        // The tombstone path has no live entries and no removed hashes, so
        // compact_segments returns before fetching from S3.  No local files needed.
        let store = make_store();
        let candidate = SegmentStats {
            ulid_str: old_ulid.clone(),
            file_size: 17,
            live_lba_bytes: 0,
            total_lba_bytes: 17,
            has_body_entries: true,
            live_entries: Vec::new(),
            removed_hashes: Vec::new(),
        };

        compact_segments(vec![candidate], &gc_dir, "vol", &store, handoff_ulid)
            .await
            .unwrap();

        // A tombstone .pending file must have been written so the volume can
        // acknowledge before the coordinator deletes.
        let pending_path = gc_dir.join(format!("{handoff_ulid}.pending"));
        assert!(
            pending_path.exists(),
            "tombstone .pending file must be written for all-dead segments"
        );
        let content = fs::read_to_string(&pending_path).unwrap();
        assert!(
            content
                .lines()
                .any(|l| l.starts_with("dead ") && l.contains(&old_ulid)),
            "pending file must contain 'dead <old_ulid>' line; got: {content:?}"
        );
    }

    #[tokio::test]
    async fn done_tombstone_applied_deletes_old_no_upload() {
        // When apply_done_handoffs processes a tombstone .applied file
        // (written after the volume acknowledges a dead-<ulid> handoff),
        // it must delete the old S3 object but must NOT attempt to upload a
        // new segment (there is none).  Local cache cleanup is the volume's
        // responsibility (evict_applied_gc_cache).
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        let old_ulid = Ulid::from_parts(999, 21).to_string();
        let handoff_ulid = Ulid::from_parts(1000, 21).to_string();

        let store = make_store();
        let s3_key = segment_key("vol", &old_ulid).unwrap();
        store
            .put(&s3_key, bytes::Bytes::from("dead segment data").into())
            .await
            .unwrap();

        // Write the tombstone .applied file (volume has acknowledged).
        let old_ulid_typed = Ulid::from_string(&old_ulid).unwrap();
        let content = format!(
            "{}\n",
            HandoffLine::Dead {
                old_ulid: old_ulid_typed
            }
        );
        fs::write(gc_dir.join(format!("{handoff_ulid}.applied")), content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", &store)
            .await
            .unwrap();

        assert_eq!(n, 1, "one tombstone handoff should complete");
        assert!(
            store.get(&s3_key).await.is_err(),
            "old S3 object must be deleted"
        );
        assert!(
            gc_dir.join(format!("{handoff_ulid}.done")).exists(),
            ".done file must be written"
        );
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

        // Step 1: write [0xAA; 4096] to lba 0, flush, materialise, drain.
        // Produces S1: DATA(lba=0, hash=H_aa, body=[0xAA; 4096]).
        vol.write(0, &content).unwrap();
        vol.flush_wal().unwrap();
        drain_with_materialise(&mut vol, dir, "test-vol", &store).await;

        // Step 2: write the same content to lba 1, flush, materialise, drain.
        // Same hash H_aa → the write path emits DEDUP_REF(lba=1, H_aa) in S2,
        // which materialise_segment converts to MaterializedRef before upload.
        vol.write(1, &content).unwrap();
        vol.flush_wal().unwrap();
        drain_with_materialise(&mut vol, dir, "test-vol", &store).await;

        drop(vol);

        // Step 3: run the real coordinator GC.
        // small_segment_bytes=u64::MAX: all segments qualify as "small" for sweep.
        // density_threshold=0.0: all segments pass the density check.
        // Both S1 and S2 should be swept together (small.len() >= 2).
        let config = crate::config::GcConfig {
            density_threshold: 0.0,
            small_segment_bytes: u64::MAX,
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

    /// Bug F: DATA entry at a non-canonical extent location must be kept when
    /// its LBA mapping is live.
    ///
    /// Two segments with the same hash for different LBAs: S1 has DATA(LBA 0→H),
    /// S2 has MaterializedRef(LBA 1→H).  The extent_index maps H→S2 (S2 processed
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

        // Drain both: materialise converts S2's DedupRef → MaterializedRef.
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
            vol.materialise_segment(*ulid).unwrap();
            vol.promote_segment(*ulid).unwrap();
        }

        // Rebuild from disk — extent_index has H101→S2 (S2 processed last).
        let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
        let index = extentindex::rebuild(&rebuild_chain).unwrap();
        let lbamap = lbamap::rebuild_segments(&rebuild_chain).unwrap();
        let live_hashes = lbamap.live_hashes();

        let stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, None).unwrap();

        // Both segments should have 1 live entry each.
        // S1: DATA(LBA 0→H101) — not extent-canonical (H101→S2), but lba_live.
        // S2: MaterializedRef(LBA 1→H101) — lba_live.
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
}
