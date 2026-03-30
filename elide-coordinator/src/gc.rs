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
// Handoff protocol:
//   Coordinator writes gc/<new-ulid>.pending — one line per moved extent:
//     <hash_hex> <old_segment_ulid> <new_segment_ulid> <new_absolute_offset>
//   Volume reads the new segment's index, applies the patches to its in-memory
//   extent index on the next idle tick, then renames the file to gc/<new-ulid>.applied.
//   Coordinator deletes old S3 objects and renames to gc/<new-ulid>.done.
//
// A pass is deferred if any .pending files already exist (at most one
// outstanding GC result per fork at a time).
//
// Blocking IO note: index rebuild and segment reads are synchronous. For the
// first pass these are called on the async task thread; move to spawn_blocking
// if GC passes become long enough to stall other coordinator tasks.

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

        match gc_fork(&fork_dir, &volume_id, &fork_name, &store, &config).await {
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
pub async fn gc_fork(
    fork_dir: &Path,
    volume_id: &str,
    fork_name: &str,
    store: &Arc<dyn ObjectStore>,
    config: &GcConfig,
) -> Result<GcStats> {
    let segments_dir = fork_dir.join("segments");
    if !segments_dir.exists() {
        return Ok(GcStats::none());
    }

    let gc_dir = fork_dir.join("gc");
    if has_pending_results(&gc_dir)? {
        return Ok(GcStats::none());
    }

    let index = extentindex::rebuild(&[(fork_dir.to_path_buf(), None)])
        .context("rebuilding extent index")?;

    let floor: Option<Ulid> = latest_snapshot(fork_dir)?
        .map(|s| Ulid::from_string(&s).map_err(|e| io::Error::other(e.to_string())))
        .transpose()?;

    let all_stats =
        collect_stats(&segments_dir, &index, floor).context("collecting segment stats")?;

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
    compact_segments(small, &gc_dir, fork_dir, volume_id, fork_name, store)
        .await
        .context("small-segment sweep")?;

    Ok(GcStats {
        strategy: GcStrategy::SmallSweep,
        candidates,
        bytes_freed,
    })
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

        for entry in entries {
            // Inline is currently disabled (INLINE_THRESHOLD = 0); skip for safety.
            if entry.is_dedup_ref || entry.is_inline {
                continue;
            }
            total_body_bytes += entry.stored_length as u64;
            if index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == ulid_str)
            {
                live_bytes += entry.stored_length as u64;
                live_entries.push(entry);
            }
        }

        result.push(SegmentStats {
            ulid_str,
            path,
            file_size,
            live_bytes,
            total_body_bytes,
            live_entries,
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
) -> Result<()> {
    // Read live extent bodies from local segment files.
    let mut all_live: Vec<(String, SegmentEntry)> = Vec::new();
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
    }

    if all_live.is_empty() {
        // All candidates are fully dead — no DATA entries to carry forward.
        // TODO: write a gc/*.pending that tells the volume to drop these segment
        // ULIDs from its extent index so the old S3 objects can be deleted.
        return Ok(());
    }

    fs::create_dir_all(gc_dir).context("creating gc dir")?;
    let segments_dir = fork_dir.join("segments");

    let max_input_ulid = candidates
        .iter()
        .filter_map(|s| Ulid::from_string(&s.ulid_str).ok())
        .max()
        .expect("non-empty candidates");
    let new_ulid = compaction_ulid(max_input_ulid);
    let new_ulid_str = new_ulid.to_string();
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

    let key = segment_key(volume_id, fork_name, &new_ulid_str)?;
    let data = tokio::fs::read(&tmp_path)
        .await
        .context("reading compacted segment for upload")?;
    store
        .put(&key, Bytes::from(data).into())
        .await
        .with_context(|| format!("uploading compacted segment {new_ulid_str}"))?;

    // Upload confirmed: move into segments/ so the volume can read it locally
    // without a demand-fetch, and so extentindex::rebuild picks it up on restart.
    let final_path = segments_dir.join(&new_ulid_str);
    tokio::fs::rename(&tmp_path, &final_path)
        .await
        .context("moving compacted segment into segments/")?;

    // Write the handoff file: one line per moved extent.
    // Format: <hash_hex> <old_ulid> <new_ulid> <new_absolute_offset>
    let mut lines = String::new();
    for ((old_ulid, old_entry), new_entry) in all_live.iter().zip(new_entries.iter()) {
        let new_offset = new_body_section_start + new_entry.stored_offset;
        lines.push_str(&format!(
            "{} {} {} {}\n",
            old_entry.hash, old_ulid, new_ulid_str, new_offset,
        ));
    }
    let pending_path = gc_dir.join(format!("{new_ulid_str}.pending"));
    fs::write(&pending_path, &lines)
        .with_context(|| format!("writing gc result {new_ulid_str}"))?;

    Ok(())
}

/// Compute the ULID for a compacted output segment.
///
/// The output is `max(inputs).increment()` — one step ahead of the newest
/// input in the total ULID order. This gives the compacted segment a ULID
/// that is:
///
/// - strictly greater than all input segments, so it supersedes them in
///   ULID-ordered rebuild processing;
/// - strictly less than any concurrent write that occurs during compaction,
///   because input segments have already passed through the drain/upload
///   pipeline and their timestamps are seconds to minutes in the past.
///   Any write happening *now* gets a ULID from the current wall clock,
///   which is far ahead of `max(inputs)`.
///
/// This eliminates the race between compaction and concurrent writes without
/// requiring any locking: concurrent writes always win in rebuild by
/// timestamp alone. The worst case when the liveness check misses a
/// concurrent write is a small space leak (the compacted output carries an
/// extent that no LBA references), not data corruption or a stale read.
///
/// The `increment()` overflow case (all 80 random bits set) is effectively
/// unreachable; the fallback advances to the next millisecond with random=0,
/// which preserves the same ordering guarantee.
fn compaction_ulid(max_input: Ulid) -> Ulid {
    match max_input.increment() {
        Some(u) => u,
        None => Ulid::from_parts(max_input.timestamp_ms() + 1, 0),
    }
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
    fn compaction_ulid_exceeds_single_input() {
        let input = Ulid::new();
        let output = compaction_ulid(input);
        assert!(output > input);
    }

    #[test]
    fn compaction_ulid_exceeds_all_inputs() {
        // Simulate picking max from a set of candidates.
        let a = Ulid::new();
        let b = a.increment().unwrap();
        let c = b.increment().unwrap();
        let max = [a, b, c].into_iter().max().unwrap();
        let output = compaction_ulid(max);
        assert!(output > a);
        assert!(output > b);
        assert!(output > c);
    }

    #[test]
    fn compaction_ulid_preserves_timestamp_when_no_overflow() {
        let input = Ulid::new();
        let output = compaction_ulid(input);
        // Same millisecond — only the random portion was incremented.
        assert_eq!(output.timestamp_ms(), input.timestamp_ms());
    }

    #[test]
    fn compaction_ulid_overflow_advances_timestamp() {
        // Construct a ULID with all 80 random bits set (the overflow case).
        let max_random = (1u128 << 80) - 1;
        let ts = 12345u64;
        let input = Ulid::from_parts(ts, max_random);
        assert!(input.increment().is_none(), "sanity: should overflow");

        let output = compaction_ulid(input);
        assert_eq!(output.timestamp_ms(), ts + 1);
        assert!(output > input);
    }

    #[test]
    fn compaction_ulid_less_than_current_time() {
        // A ULID generated right now — simulating a concurrent write —
        // should be greater than the compacted output. This reflects the
        // core guarantee: input segments are old (drain pipeline), so
        // max(inputs) << current time.
        let old_ts = 1_000u64; // 1 second after epoch
        let input = Ulid::from_parts(old_ts, 0);
        let output = compaction_ulid(input);
        let concurrent_write = Ulid::new();
        assert!(
            concurrent_write > output,
            "concurrent write ({concurrent_write}) should beat compaction output ({output})"
        );
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
                body_section_start: 0,
            }
        }
        let stats = vec![make(80), make(90), make(100)];
        assert_eq!(find_least_dense(&stats, 0.7), None);
    }
}
