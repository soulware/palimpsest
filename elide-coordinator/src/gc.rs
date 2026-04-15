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
use std::time::Duration;
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

/// Legacy retention window kept for call-site compatibility with the
/// `cleanup_done_handoffs` no-op stub. The self-describing GC handoff
/// protocol leaves no `.done` files to prune.
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
}

impl GcStats {
    fn none(total_segments: usize) -> Self {
        Self {
            strategy: GcStrategy::None,
            candidates: 0,
            bytes_freed: 0,
            dead_cleaned: 0,
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
    let live_hashes = lbamap.lba_referenced_hashes();

    let floor: Option<Ulid> = latest_snapshot(fork_dir)?;

    let all_stats = collect_stats(fork_dir, &vk, &index, &live_hashes, &lbamap, floor)
        .context("collecting segment stats")?;
    let total_segments = all_stats.len();

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
        compact_segments(small, &gc_dir, volume_id, store, sweep_ulid, &index)
            .await
            .context("small-segment sweep")?;
        Some((sweep_candidates, sweep_bytes))
    } else {
        None
    };

    match (ran_repack, ran_sweep) {
        (false, None) if dead_count == 0 => Ok(GcStats::none(total_segments)),
        (false, None) => Ok(GcStats {
            strategy: GcStrategy::None,
            candidates: 0,
            bytes_freed: 0,
            dead_cleaned: dead_count,
            total_segments,
        }),
        (true, None) => Ok(GcStats {
            strategy: GcStrategy::Repack,
            candidates: 1,
            bytes_freed: repack_bytes,
            dead_cleaned: dead_count,
            total_segments,
        }),
        (false, Some((n, sweep_bytes))) => Ok(GcStats {
            strategy: GcStrategy::Sweep,
            candidates: n,
            bytes_freed: sweep_bytes,
            dead_cleaned: dead_count,
            total_segments,
        }),
        (true, Some((n, sweep_bytes))) => Ok(GcStats {
            strategy: GcStrategy::Both,
            candidates: 1 + n,
            bytes_freed: repack_bytes + sweep_bytes,
            dead_cleaned: dead_count,
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

/// Legacy hook kept for call-site compatibility: under the self-describing
/// GC handoff protocol, handoffs complete by deleting `gc/<ulid>`, so there
/// are no `.done` markers to prune. Returns 0 until the caller stops
/// invoking it.
pub fn cleanup_done_handoffs(_fork_dir: &Path, _ttl: Duration) -> usize {
    0
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
    /// `body_section_start` for this segment's S3 object (== .idx file size).
    /// Used to compute absolute byte offsets for range-GETs into S3.
    body_section_start: u64,
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
        let mut removed_hashes: Vec<blake3::Hash> = Vec::new();

        for mut entry in entries {
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
                } else if extent_live {
                    removed_hashes.push(entry.hash);
                }
                continue;
            }
            // DATA entries are content-addressed: the extent_index tracks a
            // single canonical location per hash.  When the same hash appears
            // in multiple segments (e.g. a regular write and a later
            // dedup ref), only one segment is "canonical" in the
            // extent_index — the other looks extent-dead even though its LBA
            // mapping is still live.  Check lba_live first (same as
            // DedupRef above) so we never drop a live LBA mapping.
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
            body_section_start: idx_size,
            has_body_entries: physical_body_bytes > 0 || has_inline,
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

/// Fetch live extent body bytes from S3 into `candidate.live_entries[].data`.
///
/// Computes coalesced range-GET batches for the live body entries, then decides
/// whether targeted range-GETs or a single full body-section GET is cheaper.
/// The heuristic: range-GETs are worthwhile when the wasted bytes they avoid
/// exceed `MIN_WASTE_PER_RANGE_GET` per batch; otherwise a single GET is
/// cheaper despite downloading dead bytes.
async fn fetch_live_bodies(
    candidate: &mut SegmentStats,
    volume_id: &str,
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

    // For each candidate: fetch live extent bodies from S3 directly into
    // memory.  Zero extents carry no body data, so candidates with only zero
    // extents skip the S3 fetch entirely.
    // removed_hashes are already fully populated from collect_stats and need no fetch.
    let mut all_live: Vec<(String, SegmentEntry)> = Vec::new();
    let mut all_removed: Vec<(blake3::Hash, String)> = Vec::new();
    for candidate in &mut candidates {
        fetch_live_bodies(candidate, volume_id, store)
            .await
            .with_context(|| format!("fetching bodies for {}", candidate.ulid_str))?;

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
            EntryKind::Data => {
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
                    e.data.take().unwrap_or_default(),
                ));
            }
            EntryKind::Inline => {
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
                    e.data.take().unwrap_or_default(),
                ));
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
    tracing::info!(
        "[gc] staged output → {new_ulid_str} ({} live entries, {} inputs)",
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
            body_section_start: 0,
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
                body_section_start: 0,
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
            small_segment_bytes: u64::MAX,
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
            removed_hashes: Vec::new(),
        };
        fetch_live_bodies(&mut candidate, "vol", &store)
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
            removed_hashes: Vec::new(),
        };

        fetch_live_bodies(&mut candidate, "vol", &store)
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
            removed_hashes: Vec::new(),
        };

        // Waste = 1MB - 8192 ≈ 1MB. Two batches (entries are far apart).
        // Waste per batch = ~512KB >> 128KB threshold → range-GETs.
        fetch_live_bodies(&mut candidate, "vol", &store)
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
            removed_hashes: Vec::new(),
        };

        // Waste ≈ 1MB, 1 batch (adjacent → coalesced) → waste/batch ≈ 1MB >> 128KB.
        fetch_live_bodies(&mut candidate, "vol", &store)
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
            removed_hashes: Vec::new(),
        };

        fetch_live_bodies(&mut candidate, "vol", &store)
            .await
            .unwrap();

        assert!(
            candidate.live_entries[0].data.is_none(),
            "DedupRef entries must not have body bytes fetched"
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
