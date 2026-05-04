//! Background body warming on volume start.
//!
//! Consumes `cache/<ulid>.prefetch-hint` files persisted by the
//! coordinator's prefetch loop on a fresh claim — each one carries the
//! peer's per-segment `.present` bitmap, identifying body byte ranges
//! the previous claimer had warm. We translate each populated entry
//! into a body offset via the local `.idx` and dispatch through the
//! volume's `SegmentFetcher`. Demand-fetch and prefetch share the same
//! fetcher, so the per-segment coalescer absorbs runs and a guest read
//! that races with prefetch is naturally deduplicated rather than
//! double-fetched.
//!
//! Each call to `fetch_extent` is a starting point; the coalescer
//! extends the batch forward across body-adjacent entries that aren't
//! yet present, so a dense hint produces a few large Range GETs
//! rather than many small ones. Subsequent per-entry calls inside the
//! batched run early-return via the present-bit fast path.
//!
//! Failure modes match demand-fetch: peer 404 / partial coverage /
//! unreachable falls through to S3 inside `PeerRangeFetcher`, so the
//! prefetch loop just keeps iterating. The hint file is deleted once
//! every populated entry has been *attempted*; a daemon restart
//! re-derives any residual work by intersecting the on-disk hint with
//! the on-disk `.present` (entries set in the hint but absent from
//! `.present` are still outstanding).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use elide_core::segment::{self, BoxFetcher};
use elide_peer_fetch::PrefetchHint;
use tracing::{trace, warn};
use ulid::Ulid;

/// Worker count for the body-prefetch pool. The warming window runs
/// on fresh claim before any guest IO can race, so we run wide enough
/// to keep the peer's connection pool busy and the link saturated;
/// the per-segment coalescer + present-bit fast path absorb any
/// duplicated work if a guest read does arrive mid-warm.
const PREFETCH_WORKERS: usize = 16;

/// Spawn a detached worker pool that warms body cache from any
/// `cache/*.prefetch-hint` files in the chain. Returns the outer
/// thread's handle so callers can sequence downstream warming (e.g.
/// `full_warm`) strictly after the peer pool drains.
///
/// `fork_dirs` is the volume's ancestry chain (oldest-first), the same
/// list demand-fetch searches on a cache miss.
///
/// `on_drain` fires from the outer thread once every hint has been
/// attempted (or immediately when there are no hints, including the
/// panic-during-collect path via the drop guard). Used by the volume
/// daemon to swap the demand-fetcher's inner store from peer-then-S3
/// to S3-only — peer-fetch is a one-shot LAN warmup, and any later
/// cache miss (e.g. an evicted segment re-fetched days afterwards)
/// must skip the peer entirely.
pub fn spawn<F>(
    fork_dirs: Vec<PathBuf>,
    fetcher: BoxFetcher,
    on_drain: F,
) -> Option<thread::JoinHandle<()>>
where
    F: FnOnce() + Send + 'static,
{
    thread::Builder::new()
        .name("body-prefetch".into())
        .spawn(move || {
            // Drop guard: fires `on_drain` exactly once, on every exit
            // path of the outer thread (clean return *or* panic).
            // Without it, a panic in `collect_hints` would strand the
            // demand-fetcher with the peer wrapper still in place.
            struct DrainGuard<F: FnOnce()>(Option<F>);
            impl<F: FnOnce()> Drop for DrainGuard<F> {
                fn drop(&mut self) {
                    if let Some(f) = self.0.take() {
                        f();
                    }
                }
            }
            let _guard = DrainGuard(Some(on_drain));
            run(fork_dirs, fetcher);
        })
        .ok()
}

fn run(fork_dirs: Vec<PathBuf>, fetcher: BoxFetcher) {
    let hints = collect_hints(&fork_dirs);
    if hints.is_empty() {
        return;
    }
    let total = hints.len();
    trace!("[body-prefetch] {total} hint(s) discovered across chain");

    let hints = Arc::new(hints);
    let cursor = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for w in 0..PREFETCH_WORKERS.min(total) {
        let cursor = Arc::clone(&cursor);
        let hints = Arc::clone(&hints);
        let fetcher = Arc::clone(&fetcher);
        if let Ok(h) = thread::Builder::new()
            .name(format!("body-prefetch-{w}"))
            .spawn(move || worker(cursor, hints, fetcher))
        {
            handles.push(h);
        }
    }
    for h in handles {
        let _ = h.join();
    }
    trace!("[body-prefetch] pool done: {total} hint(s) attempted");
}

/// Walk the chain head-first and collect every `cache/*.prefetch-hint`
/// alongside its owning fork directory and parsed segment ULID.
///
/// Head-first ordering puts the writable fork's segments at the front
/// of the work queue: most-likely-touched bytes warm earliest, so the
/// demand-fetch path is least likely to find a cold extent before
/// prefetch reaches it.
fn collect_hints(fork_dirs: &[PathBuf]) -> Vec<HintEntry> {
    let mut out = Vec::new();
    // `fork_dirs` is oldest-first; iterate in reverse so the writable
    // head fork's segments queue ahead of ancestors.
    for dir in fork_dirs.iter().rev() {
        let cache = dir.join("cache");
        let entries = match std::fs::read_dir(&cache) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(stem) = name.strip_suffix(".prefetch-hint") else {
                continue;
            };
            let Ok(ulid) = Ulid::from_string(stem) else {
                continue;
            };
            out.push(HintEntry {
                owner_dir: dir.clone(),
                seg_ulid: ulid,
                hint_path: path,
            });
        }
    }
    out
}

struct HintEntry {
    owner_dir: PathBuf,
    seg_ulid: Ulid,
    hint_path: PathBuf,
}

fn worker(cursor: Arc<AtomicUsize>, hints: Arc<Vec<HintEntry>>, fetcher: BoxFetcher) {
    loop {
        let i = cursor.fetch_add(1, Ordering::Relaxed);
        if i >= hints.len() {
            return;
        }
        let h = &hints[i];
        if let Err(e) = process_hint(&h.owner_dir, h.seg_ulid, &h.hint_path, &*fetcher) {
            trace!("[body-prefetch {}] non-fatal error: {e:#}", h.seg_ulid);
        }
    }
}

/// Process one segment's hint: translate populated entries to byte
/// ranges via the local `.idx`, dispatch each range through the
/// fetcher, and remove the hint when every populated entry has been
/// attempted.
fn process_hint(
    owner_dir: &Path,
    seg_ulid: Ulid,
    hint_path: &Path,
    fetcher: &dyn segment::SegmentFetcher,
) -> std::io::Result<()> {
    let hint_bytes = std::fs::read(hint_path)?;
    let hint = PrefetchHint::from_vec(hint_bytes);

    let index_dir = owner_dir.join("index");
    let cache_dir = owner_dir.join("cache");
    let idx_path = index_dir.join(format!("{seg_ulid}.idx"));

    // The hint file's owner directory IS the segment's owning volume —
    // the coordinator's prefetch loop persists `.idx` and
    // `.prefetch-hint` together under the fork that authored the
    // segment. Pass the vol_ulid down so the warming path GETs
    // directly from the owner instead of fanning out a chain walk
    // through the inner store.
    let owner_vol_id = match owner_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|s| Ulid::from_string(s).ok())
    {
        Some(u) => u,
        None => {
            warn!(
                "[body-prefetch {seg_ulid}] owner_dir name is not a valid ULID: {}",
                owner_dir.display()
            );
            return Ok(());
        }
    };

    let layout = match segment::read_segment_layout(&idx_path) {
        Ok(l) => l,
        Err(e) => {
            // The coordinator persists `.idx` and `.prefetch-hint` in
            // the same per-segment task, so a hint without an idx is
            // unusual. Leave the hint on disk for a subsequent run
            // rather than deleting work we can't yet do.
            warn!("[body-prefetch {seg_ulid}] missing idx, leaving hint on disk: {e}");
            return Ok(());
        }
    };
    let (_, entries, _) = segment::read_segment_index(&idx_path)?;
    let entry_count = entries.len() as u32;

    let populated: Vec<u32> = hint.iter_populated_entries(entry_count).collect();
    if populated.is_empty() {
        // Nothing to warm; remove the hint so we don't reread it next
        // start.
        if let Err(e) = std::fs::remove_file(hint_path) {
            trace!(
                "[body-prefetch {seg_ulid}] failed to remove empty hint {}: {e}",
                hint_path.display()
            );
        }
        return Ok(());
    }

    if let Err(err) = fetcher.warm_segment(
        seg_ulid,
        owner_vol_id,
        &index_dir,
        &cache_dir,
        layout.body_section_start,
        &populated,
    ) {
        // Best-effort: peer 404 / S3 transient / etc. Log and move on;
        // demand-fetch will retry any unwarmed entry on first guest
        // read. Drop the hint anyway — the present-bit fast path makes
        // re-attempts cheap, and leaving it on disk would re-warm
        // already-cached bytes on every restart.
        trace!("[body-prefetch {seg_ulid}] warm_segment: {err:#}");
    }

    // Remove the hint now that every populated entry has been
    // attempted. Restart-safe via the present-bit fast path: any
    // entries that didn't make it into `.present` will be picked up by
    // demand-fetch, and entries that did won't be re-fetched.
    if let Err(e) = std::fs::remove_file(hint_path) {
        trace!(
            "[body-prefetch {seg_ulid}] failed to remove consumed hint {}: {e}",
            hint_path.display()
        );
    }
    trace!(
        "[body-prefetch {seg_ulid}] warmed {} entr(ies); hint consumed",
        populated.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use elide_core::segment::{ExtentFetch, SegmentFetcher};

    /// Counts `fetch_extent` calls per segment. Records every entry
    /// idx requested so tests can assert against the exact set the
    /// hint should have driven.
    struct RecordingFetcher {
        calls: Mutex<Vec<(Ulid, u32)>>,
    }

    impl RecordingFetcher {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl SegmentFetcher for RecordingFetcher {
        fn fetch_extent(
            &self,
            segment_id: Ulid,
            _owner_vol_id: Ulid,
            _index_dir: &Path,
            _body_dir: &Path,
            extent: &ExtentFetch,
        ) -> std::io::Result<()> {
            self.calls
                .lock()
                .expect("recording fetcher mutex")
                .push((segment_id, extent.entry_idx));
            Ok(())
        }

        fn fetch_delta_body(
            &self,
            _segment_id: Ulid,
            _owner_vol_id: Ulid,
            _index_dir: &Path,
            _body_dir: &Path,
        ) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Build a minimal segment with `entry_count` Data entries (each
    /// 1 KiB so they don't fall under the inline threshold), write a
    /// full segment file, then strip the body section to land an
    /// `.idx` in `<owner_dir>/index/`. Returns the segment ULID.
    /// `<owner_dir>/cache/` is also created so the test can drop a
    /// `.prefetch-hint` next to where `.body` would land.
    fn write_test_segment(owner_dir: &Path, entry_count: u32) -> std::io::Result<Ulid> {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};

        let index_dir = owner_dir.join("index");
        std::fs::create_dir_all(&index_dir)?;
        std::fs::create_dir_all(owner_dir.join("cache"))?;

        let seg_ulid = Ulid::new();
        let body_size: usize = 1024;
        let mut entries: Vec<SegmentEntry> = (0..entry_count)
            .map(|i| {
                let data = vec![i as u8; body_size];
                SegmentEntry::new_data(
                    blake3::hash(&data),
                    (i as u64) * 8,
                    8,
                    SegmentFlags::empty(),
                    data,
                )
            })
            .collect();
        let (signer, _vk) = elide_core::signing::generate_ephemeral_signer();
        let seg_path = owner_dir.join(format!("{seg_ulid}.tmp-full"));
        write_segment(&seg_path, &mut entries, signer.as_ref())?;

        // Strip the body section to leave just header + index + inline.
        let layout = elide_core::segment::read_segment_layout(&seg_path)?;
        let raw = std::fs::read(&seg_path)?;
        let idx_bytes = &raw[..layout.body_section_start as usize];
        std::fs::write(index_dir.join(format!("{seg_ulid}.idx")), idx_bytes)?;
        std::fs::remove_file(&seg_path)?;
        Ok(seg_ulid)
    }

    #[test]
    fn process_hint_dispatches_populated_entries_and_removes_hint() {
        let tmp = tempfile::tempdir().unwrap();
        // owner_dir's basename must be a valid ULID — process_hint
        // parses it as the segment's owning volume and skips warming
        // if it isn't one.
        let owner = tmp.path().join(Ulid::new().to_string());
        std::fs::create_dir_all(&owner).unwrap();
        let seg = write_test_segment(&owner, 16).unwrap();

        // Hint: entries 0, 3, 7, 12 populated.
        let mut bits = vec![0u8; 2];
        for idx in [0u32, 3, 7, 12] {
            bits[(idx / 8) as usize] |= 1 << (idx % 8);
        }
        let cache = owner.join("cache");
        std::fs::write(cache.join(format!("{seg}.prefetch-hint")), &bits).unwrap();

        let fetcher = Arc::new(RecordingFetcher::new());
        process_hint(
            &owner,
            seg,
            &cache.join(format!("{seg}.prefetch-hint")),
            &*fetcher,
        )
        .unwrap();

        let calls = fetcher.calls.lock().unwrap().clone();
        let entry_idxs: Vec<u32> = calls.iter().map(|(_, idx)| *idx).collect();
        assert_eq!(entry_idxs, vec![0, 3, 7, 12]);

        // Hint file is deleted post-attempt.
        assert!(!cache.join(format!("{seg}.prefetch-hint")).exists());
    }

    #[test]
    fn process_hint_with_missing_idx_leaves_hint_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let owner = tmp.path().join(Ulid::new().to_string());
        std::fs::create_dir_all(&owner).unwrap();
        std::fs::create_dir_all(owner.join("cache")).unwrap();
        std::fs::create_dir_all(owner.join("index")).unwrap();

        // Hint references a segment with no .idx on disk.
        let seg = Ulid::new();
        let cache = owner.join("cache");
        let hint_path = cache.join(format!("{seg}.prefetch-hint"));
        std::fs::write(&hint_path, [0xffu8; 4]).unwrap();

        let fetcher = Arc::new(RecordingFetcher::new());
        process_hint(&owner, seg, &hint_path, &*fetcher).unwrap();

        // Untouched: a future run (after idx prefetch lands) can retry.
        assert!(hint_path.exists());
        assert!(fetcher.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn collect_hints_walks_chain_head_first() {
        let tmp = tempfile::tempdir().unwrap();
        let parent = tmp.path().join("parent");
        let head = tmp.path().join("head");
        std::fs::create_dir_all(parent.join("cache")).unwrap();
        std::fs::create_dir_all(head.join("cache")).unwrap();

        let p_seg = Ulid::new();
        let h_seg = Ulid::new();
        std::fs::write(
            parent.join("cache").join(format!("{p_seg}.prefetch-hint")),
            b"x",
        )
        .unwrap();
        std::fs::write(
            head.join("cache").join(format!("{h_seg}.prefetch-hint")),
            b"x",
        )
        .unwrap();

        // Oldest-first chain: parent then head.
        let chain = vec![parent.clone(), head.clone()];
        let collected = collect_hints(&chain);
        let order: Vec<Ulid> = collected.iter().map(|h| h.seg_ulid).collect();
        // Head first in work queue.
        assert_eq!(order, vec![h_seg, p_seg]);
    }
}
