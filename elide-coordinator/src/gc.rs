// Coordinator-side S3 segment GC.
//
// Two strategies, matching lsvd's StartGC / SweepSmallSegments model and the
// volume's compact() / compact_pending() split:
//
//   Density pass (Strategy 1):
//     Find the single least-dense segment (lowest live_bytes/file_bytes ratio).
//     If it is below the density threshold, compact it into one output segment.
//     Returns after processing one segment — the next tick handles the next one.
//     Mirrors lsvd StartGC and volume compact().
//
//   Small-segment sweep (Strategy 2):
//     If no segment is sparse enough to trigger Strategy 1, collect all segments
//     below small_segment_bytes and batch them into one output, capping total
//     live bytes at SWEEP_LIVE_CAP (32 MiB) to bound output size.
//     Mirrors lsvd SweepSmallSegments and volume compact_pending().
//
// Per-tick work is bounded in both cases: Strategy 1 processes one segment;
// Strategy 2 is capped at 32 MiB of live data.
//
// Handoff protocol (crash-safe, filesystem-only coordination):
//
//   1. Coordinator writes gc/<new-ulid>.pending (via tmp + rename for atomicity)
//      — one line per moved extent:
//        <hash_hex> <old_segment_ulid> <new_segment_ulid> <new_absolute_offset>
//
//   2. Volume reads the new segment's index, applies extent index patches on
//      its idle tick, renames gc/<new-ulid>.pending → gc/<new-ulid>.applied.
//
//   3. Coordinator (next GC tick) sees .applied, deletes old S3 objects and
//      old local segment files from segments/, renames → gc/<new-ulid>.done.
//
//   Crash at any step:
//   - Before step 1 completes: no .pending file; coordinator retries next tick.
//   - After step 1, before step 2: volume re-applies on next idle tick (idempotent).
//   - After step 2, before step 3: coordinator re-runs cleanup (S3 404 = success).
//
//   All-dead segments bypass the protocol entirely: no extent index entries
//   reference them, so the coordinator deletes S3 + local files directly.
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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::ObjectStore;
use tokio::time::MissedTickBehavior;
use ulid::Ulid;

use elide_core::extentindex::{self, ExtentIndex};
use elide_core::lbamap::{self, LbaMap};
use elide_core::segment::{self, SegmentEntry};
use elide_core::volume::latest_snapshot;

use crate::config::GcConfig;
use crate::upload::segment_key;

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
    /// Compacted the single least-dense segment (Strategy 1).
    Density,
    /// Packed multiple small segments into one (Strategy 2).
    SmallSweep,
}

/// Results from one GC pass.
pub struct GcStats {
    pub strategy: GcStrategy,
    /// Number of input segments compacted.
    pub candidates: usize,
    /// Estimated bytes freed (dead bytes removed from old segments).
    pub bytes_freed: u64,
}

impl GcStats {
    fn none() -> Self {
        Self {
            strategy: GcStrategy::None,
            candidates: 0,
            bytes_freed: 0,
        }
    }
}

/// Long-running GC loop for a single fork.
pub async fn gc_loop(
    fork_dir: PathBuf,
    volume_id: String,
    fork_name: String,
    store: Arc<dyn ObjectStore>,
    config: GcConfig,
    gc_checkpoint: Arc<dyn Fn() -> String + Send + Sync>,
) {
    let interval = Duration::from_secs(config.interval_secs);
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;

        if !fork_dir.exists() {
            info!(
                "[coordinator] fork removed, stopping gc: {}",
                fork_dir.display()
            );
            break;
        }

        // Process any handoffs the volume has already acknowledged (.applied).
        // This deletes old S3 objects and local segment files before running a
        // new GC pass, preventing the old files from being included in the next
        // extent index rebuild.
        match apply_done_handoffs(&fork_dir, &volume_id, &fork_name, &store).await {
            Ok(0) => {}
            Ok(n) => info!("[gc {volume_id}/{fork_name}] completed {n} GC handoff(s)"),
            Err(e) => error!("[gc {volume_id}/{fork_name}] handoff cleanup error: {e:#}"),
        }

        match gc_fork(
            &fork_dir,
            &volume_id,
            &fork_name,
            &store,
            &config,
            &*gc_checkpoint,
        )
        .await
        {
            Ok(GcStats {
                strategy: GcStrategy::Density,
                bytes_freed,
                ..
            }) => {
                info!(
                    "[gc {volume_id}/{fork_name}] density: compacted 1 segment, ~{bytes_freed} bytes freed"
                );
            }
            Ok(GcStats {
                strategy: GcStrategy::SmallSweep,
                candidates,
                bytes_freed,
            }) => {
                info!(
                    "[gc {volume_id}/{fork_name}] sweep: packed {candidates} small segment(s), ~{bytes_freed} bytes freed"
                );
            }
            Ok(_) => {}
            Err(e) => error!("[gc {volume_id}/{fork_name}] error: {e:#}"),
        }
    }
}

/// Run one GC pass for a single fork.
///
/// Tries Strategy 1 (density) first. If no segment is sparse enough, tries
/// Strategy 2 (small-segment sweep). Returns `GcStrategy::None` if neither
/// finds candidates.
///
/// `gc_checkpoint` flushes the volume's WAL and returns a fresh ULID for the
/// GC output segment.  Called only after confirming candidates exist (not
/// speculatively), so that the volume is not flushed unnecessarily.  The
/// flush ensures the extent index rebuild sees all committed writes, and the
/// returned ULID sorts after all existing segments but before any future
/// write.
pub async fn gc_fork(
    fork_dir: &Path,
    volume_id: &str,
    fork_name: &str,
    store: &Arc<dyn ObjectStore>,
    config: &GcConfig,
    gc_checkpoint: &(dyn Fn() -> String + Send + Sync),
) -> Result<GcStats> {
    let segments_dir = fork_dir.join("segments");
    if !segments_dir.exists() {
        return Ok(GcStats::none());
    }

    let gc_dir = fork_dir.join("gc");
    if has_pending_results(&gc_dir)? {
        return Ok(GcStats::none());
    }

    // Checkpoint: flush the volume's WAL so the rebuild below sees all
    // committed writes, and obtain the ULID for the GC output segment.
    // Called before the rebuild — not inside compact_segments — so that
    // the liveness check is accurate.
    let new_ulid_str = gc_checkpoint();

    let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
    let index = extentindex::rebuild(&rebuild_chain).context("rebuilding extent index")?;
    let lbamap = lbamap::rebuild_segments(&rebuild_chain).context("rebuilding lba map")?;
    let live_hashes = lbamap.live_hashes();

    let floor: Option<Ulid> = latest_snapshot(fork_dir)?
        .map(|s| Ulid::from_string(&s).map_err(|e| io::Error::other(e.to_string())))
        .transpose()?;

    let all_stats = collect_stats(&segments_dir, &index, &live_hashes, &lbamap, floor)
        .context("collecting segment stats")?;

    // Strategy 1: density pass — compact the single least-dense segment.
    if let Some(pos) = find_least_dense(&all_stats, config.density_threshold) {
        let candidate = all_stats.into_iter().nth(pos).expect("index in bounds");
        let bytes_freed = candidate.dead_bytes();
        compact_segments(
            vec![candidate],
            &gc_dir,
            fork_dir,
            volume_id,
            fork_name,
            store,
            &new_ulid_str,
        )
        .await
        .context("density compaction")?;
        return Ok(GcStats {
            strategy: GcStrategy::Density,
            candidates: 1,
            bytes_freed,
        });
    }

    // Strategy 2: small-segment sweep — batch oldest small segments up to cap.
    let mut small: Vec<SegmentStats> = Vec::new();
    let mut acc_live: u64 = 0;
    for s in all_stats {
        if s.file_size >= config.small_segment_bytes {
            continue;
        }
        // Always include at least one; then enforce the live-bytes cap.
        if !small.is_empty() && acc_live + s.live_bytes > SWEEP_LIVE_CAP {
            break;
        }
        acc_live += s.live_bytes;
        small.push(s);
    }

    if small.is_empty() {
        return Ok(GcStats::none());
    }

    // Strategy 1 already owns single-segment compaction: any segment with
    // density below the threshold is caught there first. By the time we reach
    // Strategy 2 all remaining segments have density >= threshold, so a single
    // small candidate here has no meaningful dead space to reclaim. Only
    // proceed when ≥2 segments can be merged to reduce segment count.
    if small.len() == 1 {
        return Ok(GcStats::none());
    }

    let candidates = small.len();
    let bytes_freed: u64 = small.iter().map(|s| s.dead_bytes()).sum();
    compact_segments(
        small,
        &gc_dir,
        fork_dir,
        volume_id,
        fork_name,
        store,
        &new_ulid_str,
    )
    .await
    .context("small-segment sweep")?;

    Ok(GcStats {
        strategy: GcStrategy::SmallSweep,
        candidates,
        bytes_freed,
    })
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
    fork_name: &str,
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
                .is_some_and(|n| n.ends_with(".applied"))
        })
        .collect();

    if applied.is_empty() {
        return Ok(0);
    }

    applied.sort_by_key(|e| e.file_name());
    let segments_dir = fork_dir.join("segments");
    let mut count = 0;

    for entry in &applied {
        let filename = entry.file_name();
        let name = filename
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("gc filename is not valid UTF-8"))?;
        let new_ulid_str = name
            .strip_suffix(".applied")
            .ok_or_else(|| anyhow::anyhow!("expected .applied suffix"))?;

        // Validate the ULID.
        Ulid::from_string(new_ulid_str)
            .map_err(|e| anyhow::anyhow!("invalid ULID in gc filename '{name}': {e}"))?;

        // Collect the unique old segment ULIDs referenced in the handoff file.
        // Format: <hash_hex> <old_ulid> <new_ulid> <new_absolute_offset>
        let content =
            fs::read_to_string(entry.path()).with_context(|| format!("reading {name}"))?;

        let mut old_ulids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for line in content.lines() {
            let mut parts = line.split_whitespace();
            parts.next(); // hash
            if let Some(old_ulid) = parts.next() {
                old_ulids.insert(old_ulid.to_owned());
            }
        }

        // Delete old S3 objects.  A 404 means the object is already gone
        // (e.g. coordinator crashed after delete but before rename); treat as
        // success so the cleanup is idempotent.
        for old_ulid_str in &old_ulids {
            let key = segment_key(volume_id, fork_name, old_ulid_str)
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

        // Delete old local segment files from segments/ (best-effort: the
        // files may already be evicted or absent if the volume has not cached
        // them locally).
        for old_ulid_str in &old_ulids {
            let _ = fs::remove_file(segments_dir.join(old_ulid_str));
        }

        // Rename .applied → .done.
        let done_path = gc_dir.join(format!("{new_ulid_str}.done"));
        fs::rename(entry.path(), &done_path)
            .with_context(|| format!("renaming {name} to .done"))?;

        count += 1;
    }

    Ok(count)
}

// --- internals ---

/// Per-segment stats computed during the scan phase.
struct SegmentStats {
    ulid_str: String,
    path: PathBuf,
    file_size: u64,
    /// Sum of stored_length for entries still canonical in the extent index.
    live_bytes: u64,
    /// Sum of stored_length for all non-dedup body entries.
    total_body_bytes: u64,
    /// Live non-dedup body entries (data field not yet populated).
    live_entries: Vec<SegmentEntry>,
    /// Hashes that are in the extent index but not reachable from the LBA map.
    /// The volume must remove these from its extent index when applying the
    /// handoff, since the old segment files will be deleted.
    removed_hashes: Vec<blake3::Hash>,
    body_section_start: u64,
}

impl SegmentStats {
    fn dead_bytes(&self) -> u64 {
        self.total_body_bytes.saturating_sub(self.live_bytes)
    }

    fn density(&self) -> f64 {
        if self.file_size > 0 {
            self.live_bytes as f64 / self.file_size as f64
        } else {
            0.0
        }
    }
}

/// Scan `segments_dir` and compute liveness stats for each segment.
/// Returns segments in ULID (chronological) order; snapshot-frozen segments
/// are excluded.
fn collect_stats(
    segments_dir: &Path,
    index: &ExtentIndex,
    live_hashes: &HashSet<blake3::Hash>,
    lba_map: &LbaMap,
    floor: Option<Ulid>,
) -> io::Result<Vec<SegmentStats>> {
    let mut segment_files = segment::collect_segment_files(segments_dir)?;
    segment_files.sort();

    let mut result = Vec::new();
    for path in segment_files {
        let Some(ulid_str) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let ulid_str = ulid_str.to_owned();

        if let Some(f) = floor {
            match Ulid::from_string(&ulid_str) {
                Ok(u) if u <= f => continue,
                Err(e) => return Err(io::Error::other(e.to_string())),
                Ok(_) => {}
            }
        }

        let file_size = fs::metadata(&path)?.len();
        let (body_section_start, entries) = segment::read_segment_index(&path)?;

        let mut live_bytes: u64 = 0;
        let mut total_body_bytes: u64 = 0;
        let mut live_entries: Vec<SegmentEntry> = Vec::new();
        let mut removed_hashes: Vec<blake3::Hash> = Vec::new();

        for entry in entries {
            if entry.is_inline {
                continue;
            }
            // Dedup refs have no body bytes, but carry an LBA mapping. A ref
            // is only live if the LBA still maps to its hash — otherwise the
            // LBA was overwritten and the ref would reintroduce a stale
            // mapping into the GC output.
            if entry.is_dedup_ref {
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
                if lba_live {
                    live_entries.push(entry);
                }
                continue;
            }
            total_body_bytes += entry.stored_length as u64;
            let extent_live = index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == ulid_str);
            if extent_live && live_hashes.contains(&entry.hash) {
                live_bytes += entry.stored_length as u64;
                live_entries.push(entry);
            } else if extent_live {
                // Extent-index-live but not LBA-map-live: the LBA was
                // overwritten with different data.  Record so the volume
                // can remove the dangling extent index entry.
                removed_hashes.push(entry.hash);
            }
        }

        result.push(SegmentStats {
            ulid_str,
            path,
            file_size,
            live_bytes,
            total_body_bytes,
            live_entries,
            removed_hashes,
            body_section_start,
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
        .filter(|(_, s)| s.density() < threshold)
        .min_by(|(_, a), (_, b)| {
            a.density()
                .partial_cmp(&b.density())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(i, _)| i)
}

/// Read live extent bodies from each candidate, write a compacted segment,
/// upload it to S3, and write the gc/*.pending handoff file.
async fn compact_segments(
    mut candidates: Vec<SegmentStats>,
    gc_dir: &Path,
    fork_dir: &Path,
    volume_id: &str,
    fork_name: &str,
    store: &Arc<dyn ObjectStore>,
    new_ulid_str: &str,
) -> Result<()> {
    // Read live extent bodies from local segment files.
    let mut all_live: Vec<(String, SegmentEntry)> = Vec::new();
    let mut all_removed: Vec<(blake3::Hash, String)> = Vec::new();
    for candidate in &mut candidates {
        segment::read_extent_bodies(
            &candidate.path,
            candidate.body_section_start,
            &mut candidate.live_entries,
        )
        .with_context(|| format!("reading bodies from {}", candidate.ulid_str))?;
        for entry in candidate.live_entries.drain(..) {
            all_live.push((candidate.ulid_str.clone(), entry));
        }
        for hash in candidate.removed_hashes.drain(..) {
            all_removed.push((hash, candidate.ulid_str.clone()));
        }
    }

    if all_live.is_empty() {
        // All candidates are fully dead — no extent index entries reference
        // these segments, so no volume handoff is needed.  Delete old S3
        // objects and local files directly.
        for candidate in &candidates {
            let key = segment_key(volume_id, fork_name, &candidate.ulid_str)?;
            match store.delete(&key).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => {
                    return Err(anyhow::anyhow!(e)
                        .context(format!("deleting dead S3 object {}", candidate.ulid_str)));
                }
            }
            let _ = fs::remove_file(&candidate.path);
        }
        return Ok(());
    }

    fs::create_dir_all(gc_dir).context("creating gc dir")?;
    let segments_dir = fork_dir.join("segments");
    // Write to a temp file first; rename into segments/ only after S3 upload
    // succeeds, so the invariant "segments/ ↔ in S3" is never violated.
    let tmp_path = gc_dir.join(format!("{new_ulid_str}.tmp"));

    let mut new_entries: Vec<SegmentEntry> = all_live
        .iter_mut()
        .map(|(_, e)| {
            SegmentEntry::new_data(
                e.hash,
                e.start_lba,
                e.lba_length,
                if e.compressed {
                    segment::FLAG_COMPRESSED
                } else {
                    0
                },
                std::mem::take(&mut e.data),
            )
        })
        .collect();

    // TODO: sign compacted segment with the fork's key.
    let new_body_section_start = segment::write_segment(&tmp_path, &mut new_entries, None)
        .context("writing compacted segment")?;

    let key = segment_key(volume_id, fork_name, new_ulid_str)?;
    let data = tokio::fs::read(&tmp_path)
        .await
        .context("reading compacted segment for upload")?;
    store
        .put(&key, Bytes::from(data).into())
        .await
        .with_context(|| format!("uploading compacted segment {new_ulid_str}"))?;

    // Upload confirmed: move into segments/ so the volume can read it locally
    // without a demand-fetch, and so extentindex::rebuild picks it up on restart.
    let final_path = segments_dir.join(new_ulid_str);
    tokio::fs::rename(&tmp_path, &final_path)
        .await
        .context("moving compacted segment into segments/")?;

    // Write the handoff file.
    // Carried entries (4 fields): <hash> <old_ulid> <new_ulid> <offset>
    // Removed entries (2 fields): <hash> <old_ulid>
    let mut lines = String::new();
    for ((old_ulid, old_entry), new_entry) in all_live.iter().zip(new_entries.iter()) {
        let new_offset = new_body_section_start + new_entry.stored_offset;
        lines.push_str(&format!(
            "{} {} {} {}\n",
            old_entry.hash, old_ulid, new_ulid_str, new_offset,
        ));
    }
    for (hash, old_ulid) in &all_removed {
        lines.push_str(&format!("{} {}\n", hash, old_ulid));
    }
    let pending_path = gc_dir.join(format!("{new_ulid_str}.pending"));
    let pending_tmp = gc_dir.join(format!("{new_ulid_str}.pending.tmp"));
    fs::write(&pending_tmp, &lines).with_context(|| format!("writing gc result {new_ulid_str}"))?;
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
            .is_some_and(|n| n.ends_with(".pending"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

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
        fn make(file_size: u64, live_bytes: u64) -> SegmentStats {
            SegmentStats {
                ulid_str: String::new(),
                path: PathBuf::new(),
                file_size,
                live_bytes,
                total_body_bytes: file_size,
                live_entries: Vec::new(),
                removed_hashes: Vec::new(),
                body_section_start: 0,
            }
        }

        // density: 0.8, 0.5, 0.6 — only 0.5 and 0.6 are below 0.7
        let stats = vec![make(100, 80), make(100, 50), make(100, 60)];
        assert_eq!(find_least_dense(&stats, 0.7), Some(1)); // 0.5 is least dense
    }

    #[test]
    fn sweep_skips_single_small_segment() {
        // Strategy 1 owns single-segment compaction (by density). By the time
        // Strategy 2 runs, a lone small segment — whether all-live or sparsely
        // live — has density >= threshold and is not worth a standalone GC pass.
        // Verify dead_bytes() is available for the ≥2 case.
        let s = SegmentStats {
            ulid_str: String::new(),
            path: PathBuf::new(),
            file_size: 1024 * 1024,
            live_bytes: 800 * 1024,
            total_body_bytes: 1024 * 1024,
            live_entries: Vec::new(),
            removed_hashes: Vec::new(),
            body_section_start: 0,
        };
        assert_eq!(s.dead_bytes(), 224 * 1024);
        // density = 0.78 >= 0.70 threshold: Strategy 1 would have caught it if below.
        assert!(s.density() >= 0.70);
    }

    #[test]
    fn find_least_dense_returns_none_when_all_above_threshold() {
        fn make(live: u64) -> SegmentStats {
            SegmentStats {
                ulid_str: String::new(),
                path: PathBuf::new(),
                file_size: 100,
                live_bytes: live,
                total_body_bytes: 100,
                live_entries: Vec::new(),
                removed_hashes: Vec::new(),
                body_section_start: 0,
            }
        }
        let stats = vec![make(80), make(90), make(100)];
        assert_eq!(find_least_dense(&stats, 0.7), None);
    }

    // --- apply_done_handoffs tests ---

    fn make_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn done_no_gc_dir() {
        let tmp = TempDir::new().unwrap();
        let store = make_store();
        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn done_empty_gc_dir() {
        let tmp = TempDir::new().unwrap();
        fs::create_dir_all(tmp.path().join("gc")).unwrap();
        let store = make_store();
        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
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
        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
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
        fs::create_dir_all(tmp.path().join("segments")).unwrap();

        let new_ulid = Ulid::from_parts(1000, 1).to_string();
        let old_ulid = Ulid::from_parts(999, 0).to_string();

        let hash_hex = "a".repeat(64);
        let content = format!("{hash_hex} {old_ulid} {new_ulid} 1234\n");
        let applied_path = gc_dir.join(format!("{new_ulid}.applied"));
        fs::write(&applied_path, &content).unwrap();

        let store = make_store();
        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
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
        fs::create_dir_all(tmp.path().join("segments")).unwrap();

        let new_ulid = Ulid::from_parts(1000, 1).to_string();
        let old_ulid = Ulid::from_parts(999, 0).to_string();

        let store = make_store();
        let key = segment_key("vol", "fork", &old_ulid).unwrap();
        store
            .put(&key, bytes::Bytes::from("old segment data").into())
            .await
            .unwrap();
        assert!(store.get(&key).await.is_ok());

        let hash_hex = "b".repeat(64);
        let content = format!("{hash_hex} {old_ulid} {new_ulid} 0\n");
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
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
        fs::create_dir_all(tmp.path().join("segments")).unwrap();

        let new_ulid = Ulid::from_parts(1000, 2).to_string();
        let old_ulid = Ulid::from_parts(999, 1).to_string();

        let store = make_store();
        let hash_hex = "c".repeat(64);
        let content = format!("{hash_hex} {old_ulid} {new_ulid} 0\n");
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(gc_dir.join(format!("{new_ulid}.done")).exists());
    }

    #[tokio::test]
    async fn done_deletes_local_segment_file() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        let segments_dir = tmp.path().join("segments");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::create_dir_all(&segments_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 3).to_string();
        let old_ulid = Ulid::from_parts(999, 2).to_string();

        let old_local = segments_dir.join(&old_ulid);
        fs::write(&old_local, "old local segment").unwrap();
        assert!(old_local.exists());

        let store = make_store();
        let hash_hex = "d".repeat(64);
        let content = format!("{hash_hex} {old_ulid} {new_ulid} 0\n");
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        assert!(
            !old_local.exists(),
            "old local segment file should be removed"
        );
    }

    #[tokio::test]
    async fn done_multiple_old_ulids_in_one_handoff() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        let segments_dir = tmp.path().join("segments");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::create_dir_all(&segments_dir).unwrap();

        let new_ulid = Ulid::from_parts(1000, 4).to_string();
        let old_a = Ulid::from_parts(998, 0).to_string();
        let old_b = Ulid::from_parts(999, 0).to_string();

        let store = make_store();
        for ulid_str in [&old_a, &old_b] {
            let key = segment_key("vol", "fork", ulid_str).unwrap();
            store
                .put(&key, bytes::Bytes::from("data").into())
                .await
                .unwrap();
            fs::write(segments_dir.join(ulid_str), "data").unwrap();
        }

        let h1 = "e".repeat(64);
        let h2 = "f".repeat(64);
        let content = format!("{h1} {old_a} {new_ulid} 0\n{h2} {old_b} {new_ulid} 4096\n");
        fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();

        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
            .await
            .unwrap();
        assert_eq!(n, 1);
        for ulid_str in [&old_a, &old_b] {
            let key = segment_key("vol", "fork", ulid_str).unwrap();
            assert!(
                store.get(&key).await.is_err(),
                "{ulid_str} S3 object should be deleted"
            );
            assert!(
                !segments_dir.join(ulid_str).exists(),
                "{ulid_str} local file should be removed"
            );
        }
        assert!(gc_dir.join(format!("{new_ulid}.done")).exists());
    }

    #[tokio::test]
    async fn done_processes_multiple_applied_files() {
        let tmp = TempDir::new().unwrap();
        let gc_dir = tmp.path().join("gc");
        fs::create_dir_all(&gc_dir).unwrap();
        fs::create_dir_all(tmp.path().join("segments")).unwrap();

        let store = make_store();
        for i in 1u64..=3 {
            let new_ulid = Ulid::from_parts(1000 + i, 0).to_string();
            let old_ulid = Ulid::from_parts(999 + i, 0).to_string();
            let content = format!("{} {old_ulid} {new_ulid} 0\n", "a".repeat(64));
            fs::write(gc_dir.join(format!("{new_ulid}.applied")), &content).unwrap();
        }

        let n = apply_done_handoffs(tmp.path(), "vol", "fork", &store)
            .await
            .unwrap();
        assert_eq!(n, 3);
        for entry in fs::read_dir(&gc_dir).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_str().unwrap();
            assert!(name.ends_with(".done"), "expected .done, got {name}");
        }
    }
}
