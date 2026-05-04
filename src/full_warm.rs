//! Background full-volume warming on writable volume start.
//!
//! Sequenced strictly after `body_prefetch` and wired with an S3-only
//! fetcher: the peer-hint warmer covers what the peer has, and anything
//! outside the hint set is presumed peer-cold, so probing the peer for
//! it just buys a 404 round-trip.
//!
//! Enumeration walks `LbaMap.lba_referenced_hashes() ∩ ExtentIndex` —
//! the post-overlap live view — and skips `BodySource::Local` (already
//! on disk) and entries already in `.present` (filtered inside
//! `warm_segment_impl`). Skipped on read-only mounts and when
//! `volume.toml` opts in via `lazy = true`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use elide_core::config::VolumeConfig;
use elide_core::extentindex::{BodySource, ExtentIndex};
use elide_core::lbamap::LbaMap;
use elide_core::segment::{self, BoxFetcher};
use tracing::{info, warn};
use ulid::Ulid;

const FULL_WARM_WORKERS: usize = 4;

/// Spawn a detached pool that, after `body_prefetch_done` joins,
/// warms every live cold extent directly from S3.
///
/// `fetcher` must be S3-direct (no `PeerRangeFetcher` wrapper).
pub fn spawn(
    head_dir: PathBuf,
    fork_dirs: Vec<PathBuf>,
    lba_map: Arc<LbaMap>,
    extent_index: Arc<ExtentIndex>,
    fetcher: BoxFetcher,
    body_prefetch_done: Option<thread::JoinHandle<()>>,
) {
    match VolumeConfig::read(&head_dir) {
        Ok(cfg) if cfg.lazy == Some(true) => {
            info!("[full-warm] volume opted into lazy mode; skipping");
            return;
        }
        Ok(_) => {}
        Err(e) => {
            warn!("[full-warm] failed to read volume.toml ({e}); defaulting to warm");
        }
    }

    let _ = thread::Builder::new()
        .name("full-warm".into())
        .spawn(move || {
            if let Some(handle) = body_prefetch_done {
                if let Err(e) = handle.join() {
                    warn!("[full-warm] body-prefetch pool panicked: {e:?}");
                }
                info!("[full-warm] body-prefetch drained; starting S3 warm");
            }
            run(fork_dirs, lba_map, extent_index, fetcher)
        });
}

fn run(
    fork_dirs: Vec<PathBuf>,
    lba_map: Arc<LbaMap>,
    extent_index: Arc<ExtentIndex>,
    fetcher: BoxFetcher,
) {
    let plan = match plan_segments(&fork_dirs, &lba_map, &extent_index) {
        Ok(p) => p,
        Err(e) => {
            warn!("[full-warm] planning failed: {e:#}");
            return;
        }
    };
    let total = plan.len();
    if total == 0 {
        info!("[full-warm] plan empty — nothing to warm");
        return;
    }
    info!("[full-warm] {total} segment(s) to warm");

    let work: Arc<Vec<SegmentWork>> = Arc::new(plan.into_values().collect());
    let cursor = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for w in 0..FULL_WARM_WORKERS.min(work.len()) {
        let cursor = Arc::clone(&cursor);
        let work = Arc::clone(&work);
        let fetcher = Arc::clone(&fetcher);
        if let Ok(h) = thread::Builder::new()
            .name(format!("full-warm-{w}"))
            .spawn(move || worker(cursor, work, fetcher))
        {
            handles.push(h);
        }
    }
    for h in handles {
        let _ = h.join();
    }
    info!("[full-warm] pool done: {total} segment(s) attempted");
}

struct SegmentWork {
    seg_ulid: Ulid,
    owner_dir: PathBuf,
    owner_vol_id: Ulid,
    /// Sorted ascending so `plan_warm_batches` can coalesce body-adjacent runs.
    populated_entries: Vec<u32>,
}

fn plan_segments(
    fork_dirs: &[PathBuf],
    lba_map: &LbaMap,
    extent_index: &ExtentIndex,
) -> std::io::Result<BTreeMap<Ulid, SegmentWork>> {
    let mut owner_by_seg: BTreeMap<Ulid, (PathBuf, Ulid)> = BTreeMap::new();
    for fork in fork_dirs.iter().rev() {
        let owner_vol_id = match fork
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| Ulid::from_string(s).ok())
        {
            Some(u) => u,
            None => continue,
        };
        let index_dir = fork.join("index");
        let dir = match std::fs::read_dir(&index_dir) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in dir.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".idx") else {
                continue;
            };
            if let Ok(seg) = Ulid::from_string(stem) {
                owner_by_seg
                    .entry(seg)
                    .or_insert_with(|| (fork.clone(), owner_vol_id));
            }
        }
    }

    let live_hashes = lba_map.lba_referenced_hashes();
    let mut by_segment: BTreeMap<Ulid, SegmentWork> = BTreeMap::new();
    for (hash, loc) in extent_index.iter() {
        if !live_hashes.contains(hash) {
            continue;
        }
        let entry_idx = match loc.body_source {
            BodySource::Cached(i) => i,
            BodySource::Local => continue,
        };
        let Some((owner_dir, owner_vol_id)) = owner_by_seg.get(&loc.segment_id) else {
            continue;
        };
        let work = by_segment
            .entry(loc.segment_id)
            .or_insert_with(|| SegmentWork {
                seg_ulid: loc.segment_id,
                owner_dir: owner_dir.clone(),
                owner_vol_id: *owner_vol_id,
                populated_entries: Vec::new(),
            });
        work.populated_entries.push(entry_idx);
    }
    for w in by_segment.values_mut() {
        w.populated_entries.sort_unstable();
        w.populated_entries.dedup();
    }
    Ok(by_segment)
}

fn worker(cursor: Arc<AtomicUsize>, work: Arc<Vec<SegmentWork>>, fetcher: BoxFetcher) {
    loop {
        let i = cursor.fetch_add(1, Ordering::Relaxed);
        if i >= work.len() {
            return;
        }
        let w = &work[i];
        if let Err(e) = warm_one_segment(w, &*fetcher) {
            info!("[full-warm {}] non-fatal: {e:#}", w.seg_ulid);
        }
    }
}

fn warm_one_segment(w: &SegmentWork, fetcher: &dyn segment::SegmentFetcher) -> std::io::Result<()> {
    let index_dir = w.owner_dir.join("index");
    let cache_dir = w.owner_dir.join("cache");
    let idx_path = index_dir.join(format!("{}.idx", w.seg_ulid));

    let layout = match segment::read_segment_layout(&idx_path) {
        Ok(l) => l,
        Err(e) => {
            info!("[full-warm {}] missing idx: {e}", w.seg_ulid);
            return Ok(());
        }
    };

    fetcher.warm_segment(
        w.seg_ulid,
        w.owner_vol_id,
        &index_dir,
        &cache_dir,
        layout.body_section_start,
        &w.populated_entries,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use elide_core::extentindex::ExtentLocation;

    fn touch_idx(owner: &std::path::Path, seg: Ulid) {
        let idx_dir = owner.join("index");
        std::fs::create_dir_all(&idx_dir).unwrap();
        std::fs::write(idx_dir.join(format!("{seg}.idx")), b"").unwrap();
    }

    #[test]
    fn plan_segments_filters_to_live_cached_extents() {
        let tmp = tempfile::tempdir().unwrap();
        let owner_id = Ulid::new();
        let owner = tmp.path().join(owner_id.to_string());
        std::fs::create_dir_all(&owner).unwrap();

        let seg_a = Ulid::new();
        let seg_b = Ulid::new();
        touch_idx(&owner, seg_a);
        touch_idx(&owner, seg_b);

        // hash_a: live + Cached → should be warmed
        // hash_b: live + Local  → skipped (already on disk)
        // hash_c: dead          → skipped (not in LBA map)
        let hash_a = blake3::hash(b"a");
        let hash_b = blake3::hash(b"b");
        let hash_c = blake3::hash(b"c");

        let mut lba_map = LbaMap::new();
        lba_map.insert(0, 8, hash_a);
        lba_map.insert(8, 8, hash_b);

        let mut extent_index = ExtentIndex::new();
        extent_index.insert(
            hash_a,
            ExtentLocation {
                segment_id: seg_a,
                body_offset: 0,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Cached(7),
                body_section_start: 0,
                inline_data: None,
            },
        );
        extent_index.insert(
            hash_b,
            ExtentLocation {
                segment_id: seg_a,
                body_offset: 4096,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Local,
                body_section_start: 0,
                inline_data: None,
            },
        );
        extent_index.insert(
            hash_c,
            ExtentLocation {
                segment_id: seg_b,
                body_offset: 0,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Cached(0),
                body_section_start: 0,
                inline_data: None,
            },
        );

        let plan = plan_segments(std::slice::from_ref(&owner), &lba_map, &extent_index).unwrap();
        assert_eq!(plan.len(), 1, "only seg_a should be planned");
        let work = plan.get(&seg_a).expect("seg_a present");
        assert_eq!(work.populated_entries, vec![7]);
        assert_eq!(work.owner_vol_id, owner_id);
    }

    #[test]
    fn plan_segments_dedups_entries_across_lba_aliases() {
        let tmp = tempfile::tempdir().unwrap();
        let owner_id = Ulid::new();
        let owner = tmp.path().join(owner_id.to_string());
        std::fs::create_dir_all(&owner).unwrap();
        let seg = Ulid::new();
        touch_idx(&owner, seg);

        // Same hash referenced by two different LBA ranges (dedup).
        let hash = blake3::hash(b"shared");
        let mut lba_map = LbaMap::new();
        lba_map.insert(0, 8, hash);
        lba_map.insert(8, 8, hash);

        let mut extent_index = ExtentIndex::new();
        extent_index.insert(
            hash,
            ExtentLocation {
                segment_id: seg,
                body_offset: 0,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Cached(3),
                body_section_start: 0,
                inline_data: None,
            },
        );

        let plan = plan_segments(&[owner], &lba_map, &extent_index).unwrap();
        let work = plan.get(&seg).expect("seg present");
        // The extent_index keys on hash, so dedup is implicit; assert
        // the entry shows up exactly once.
        assert_eq!(work.populated_entries, vec![3]);
    }

    #[test]
    fn plan_segments_skips_segments_not_in_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let owner = tmp.path().join(Ulid::new().to_string());
        std::fs::create_dir_all(owner.join("index")).unwrap();
        // No `.idx` for the segment we point at.

        let phantom_seg = Ulid::new();
        let hash = blake3::hash(b"x");
        let mut lba_map = LbaMap::new();
        lba_map.insert(0, 8, hash);
        let mut extent_index = ExtentIndex::new();
        extent_index.insert(
            hash,
            ExtentLocation {
                segment_id: phantom_seg,
                body_offset: 0,
                body_length: 4096,
                compressed: false,
                body_source: BodySource::Cached(0),
                body_section_start: 0,
                inline_data: None,
            },
        );

        let plan = plan_segments(&[owner], &lba_map, &extent_index).unwrap();
        assert!(plan.is_empty());
    }

    #[test]
    fn plan_segments_orders_entries_ascending_for_warm_planner() {
        let tmp = tempfile::tempdir().unwrap();
        let owner = tmp.path().join(Ulid::new().to_string());
        std::fs::create_dir_all(&owner).unwrap();
        let seg = Ulid::new();
        touch_idx(&owner, seg);

        let h0 = blake3::hash(b"0");
        let h1 = blake3::hash(b"1");
        let h2 = blake3::hash(b"2");
        // Insert LBAs in mixed order; the populated_entries list must
        // come out sorted so plan_warm_batches can coalesce body-adjacent
        // runs.
        let mut lba_map = LbaMap::new();
        lba_map.insert(16, 8, h2);
        lba_map.insert(0, 8, h0);
        lba_map.insert(8, 8, h1);

        let mut extent_index = ExtentIndex::new();
        for (h, idx) in [(h0, 5u32), (h1, 1), (h2, 9)] {
            extent_index.insert(
                h,
                ExtentLocation {
                    segment_id: seg,
                    body_offset: 0,
                    body_length: 4096,
                    compressed: false,
                    body_source: BodySource::Cached(idx),
                    body_section_start: 0,
                    inline_data: None,
                },
            );
        }

        let plan = plan_segments(&[owner], &lba_map, &extent_index).unwrap();
        let work = plan.get(&seg).expect("seg present");
        assert_eq!(work.populated_entries, vec![1, 5, 9]);
    }
}
