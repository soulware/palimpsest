// Shared simulation helpers for proptest files.
//
// `drain_local` and `simulate_coord_gc_local` mirror the real coordinator's
// drain-pending and GC logic without requiring an object store.  Both proptest
// suites (volume_proptest and actor_proptest) use these to drive the same
// coordinator-side simulation.
#![allow(dead_code)]

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use elide_core::gc::{HandoffLine, format_handoff_file};
use elide_core::{extentindex, lbamap, segment, signing};
use ulid::Ulid;

/// Create `dir` and write a fresh Ed25519 keypair into it.
///
/// Required before `Volume::open` in tests that construct their own directories
/// (rather than using a `tempfile::TempDir` that already has keys).
pub fn write_test_keypair(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    elide_core::signing::generate_keypair(
        dir,
        elide_core::signing::VOLUME_KEY_FILE,
        elide_core::signing::VOLUME_PUB_FILE,
    )
    .unwrap();
}

/// Move all committed segments from pending/ to segments/.
/// Simulates `drain-pending` (upload + rename) without touching an object store.
pub fn drain_local(fork_dir: &Path) {
    let pending = fork_dir.join("pending");
    let segments = fork_dir.join("segments");
    if let Ok(entries) = fs::read_dir(&pending) {
        for entry in entries.flatten() {
            let name_str = entry.file_name().to_string_lossy().into_owned();
            if !name_str.ends_with(".tmp") {
                let _ = fs::rename(entry.path(), segments.join(&name_str));
            }
        }
    }
}

/// Sort-for-rebuild ordering: GC outputs (.pending/.applied handoff present)
/// come first (lower priority); regular segments come last (higher priority).
/// Within each group, sort by ULID ascending.
fn sort_candidates(candidates: &mut Vec<(Ulid, PathBuf)>, gc_dir: &Path) {
    let is_gc = |u: &Ulid| {
        let name = u.to_string();
        gc_dir.join(format!("{name}.pending")).exists()
            || gc_dir.join(format!("{name}.applied")).exists()
    };
    candidates.sort_by(|(ua, _), (ub, _)| match (is_gc(ua), is_gc(ub)) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => ua.cmp(ub),
    });
}

/// Compact a pre-selected set of candidates using a pre-built liveness snapshot.
///
/// Reads live entries from each candidate, filters them against the shared
/// lba_map + extent_index snapshot, writes a merged output segment with
/// `new_ulid`, and writes a `gc/<new_ulid>.pending` handoff file.
///
/// Returns `(consumed_ulids, new_ulid, paths_to_delete)` if the pass
/// produced output, or if there were extent-index entries to remove.
/// Returns `None` only if the candidates list is empty.
fn compact_candidates_inner(
    fork_dir: &Path,
    candidates: Vec<(Ulid, PathBuf)>,
    lba_map: &lbamap::LbaMap,
    live_hashes: &HashSet<blake3::Hash>,
    extent_index: &extentindex::ExtentIndex,
    new_ulid: Ulid,
) -> Option<(Vec<Ulid>, Ulid, Vec<PathBuf>)> {
    if candidates.is_empty() {
        return None;
    }

    let vk = signing::load_verifying_key(fork_dir, signing::VOLUME_PUB_FILE).ok()?;

    // The coordinator uses an ephemeral signer — it doesn't have the volume's
    // private key.  The volume re-signs the output segment with its own key
    // inside apply_gc_handoffs, so the signature here is discarded.
    let (ephemeral_signer, _) = signing::generate_ephemeral_signer();

    let gc_dir = fork_dir.join("gc");

    let mut all_entries: Vec<segment::SegmentEntry> = Vec::new();
    let mut source_ulids: Vec<Ulid> = Vec::new();
    let mut removed: Vec<(blake3::Hash, Ulid)> = Vec::new();

    for (ulid, path) in &candidates {
        let seg_id = ulid.to_string();
        let Ok((bss, mut entries)) = segment::read_and_verify_segment_index(path, &vk) else {
            continue;
        };
        if segment::read_extent_bodies(path, bss, &mut entries).is_err() {
            continue;
        }
        for entry in entries.drain(..) {
            if entry.is_dedup_ref {
                // Carry a dedup ref only if the LBA still maps to this hash.
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
                if lba_live {
                    source_ulids.push(*ulid);
                    all_entries.push(entry);
                }
                continue;
            }
            let extent_live = extent_index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == seg_id);
            if extent_live && live_hashes.contains(&entry.hash) {
                source_ulids.push(*ulid);
                all_entries.push(entry);
            } else if extent_live {
                removed.push((entry.hash, *ulid));
            }
        }
    }

    if all_entries.is_empty() && removed.is_empty() {
        let consumed = candidates.iter().map(|(u, _)| *u).collect();
        let to_delete = candidates.into_iter().map(|(_, p)| p).collect();
        return Some((consumed, new_ulid, to_delete));
    }

    let _ = fs::create_dir_all(&gc_dir);

    if all_entries.is_empty() {
        // Only extent-index removals — no output segment needed.
        let handoff_lines: Vec<HandoffLine> = removed
            .iter()
            .map(|(hash, old_ulid)| HandoffLine::Remove {
                hash: *hash,
                old_ulid: *old_ulid,
            })
            .collect();
        let _ = fs::write(
            gc_dir.join(format!("{new_ulid}.pending")),
            format_handoff_file(handoff_lines),
        );
        let consumed = candidates.iter().map(|(u, _)| *u).collect();
        let to_delete = candidates.into_iter().map(|(_, p)| p).collect();
        return Some((consumed, new_ulid, to_delete));
    }

    // Write the compacted segment to gc/<ulid> — the volume re-signs it when
    // applying the handoff, at which point it moves to segments/<ulid>.
    let tmp_path = gc_dir.join(format!("{new_ulid}.tmp"));
    let final_path = gc_dir.join(new_ulid.to_string());
    let new_bss =
        match segment::write_segment(&tmp_path, &mut all_entries, ephemeral_signer.as_ref()) {
            Ok(bss) => bss,
            Err(_) => return None,
        };
    fs::rename(&tmp_path, &final_path).ok()?;

    let mut handoff_lines: Vec<HandoffLine> = all_entries
        .iter()
        .zip(source_ulids.iter())
        .filter(|(e, _)| !e.is_dedup_ref)
        .map(|(e, src_ulid)| HandoffLine::Repack {
            hash: e.hash,
            old_ulid: *src_ulid,
            new_ulid,
            new_offset: new_bss + e.stored_offset,
        })
        .collect();
    for (hash, old_ulid) in &removed {
        handoff_lines.push(HandoffLine::Remove {
            hash: *hash,
            old_ulid: *old_ulid,
        });
    }
    let _ = fs::write(
        gc_dir.join(format!("{new_ulid}.pending")),
        format_handoff_file(handoff_lines),
    );

    let consumed = candidates.iter().map(|(u, _)| *u).collect();
    let to_delete = candidates.into_iter().map(|(_, p)| p).collect();
    Some((consumed, new_ulid, to_delete))
}

/// Simulate one coordinator GC pass on `segments/` without an object store.
///
/// Picks the `n_candidates` lowest-priority segments (sort_for_rebuild order),
/// compacts their entries, writes a new segment with the given `new_ulid`
/// (obtained from `gc_checkpoint` which flushes the volume's WAL first), and
/// writes `gc/<new_ulid>.pending`.
///
/// The input segment files are **not** deleted inline.  The caller receives
/// the consumed paths and is responsible for deleting them — after calling
/// `vol.apply_gc_handoffs()`.  This models the real coordinator's ordering
/// constraint: local segment files must not disappear until the volume has
/// acknowledged the handoff.
///
/// Returns `Some((consumed_ulids, produced_ulid, paths_to_delete))` when
/// candidates were found, `None` when fewer than two segments exist.
pub fn simulate_coord_gc_local(
    fork_dir: &Path,
    new_ulid: Ulid,
    n_candidates: usize,
) -> Option<(Vec<Ulid>, Ulid, Vec<PathBuf>)> {
    let segments_dir = fork_dir.join("segments");
    let gc_dir = fork_dir.join("gc");

    let seg_files = segment::collect_segment_files(&segments_dir).ok()?;
    let mut candidates: Vec<(Ulid, PathBuf)> = seg_files
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            let ulid = Ulid::from_string(name).ok()?;
            Some((ulid, p.clone()))
        })
        .collect();
    if candidates.len() < 2 {
        return None;
    }

    sort_candidates(&mut candidates, &gc_dir);
    let n = n_candidates.min(candidates.len());
    let candidates = candidates[..n].to_vec();

    let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
    let lba_map = lbamap::rebuild_segments(&rebuild_chain).ok()?;
    let live_hashes = lba_map.live_hashes();
    let extent_index = extentindex::rebuild(&rebuild_chain).ok()?;

    compact_candidates_inner(
        fork_dir,
        candidates,
        &lba_map,
        &live_hashes,
        &extent_index,
        new_ulid,
    )
}

/// Simulate coordinator GC running both repack and sweep in one tick.
///
/// Requires ≥ 3 segments in `segments/`: the first (lowest-priority) segment
/// is the repack candidate; the remaining segments are the sweep candidates.
/// Both passes share a single liveness snapshot (lba_map + extent_index
/// rebuilt once before either pass runs), which matches the real coordinator's
/// behaviour: `gc_fork` calls `collect_stats` once, removes the repack
/// candidate with `all_stats.remove(pos)`, and sweeps the remainder — neither
/// pass rebuilds liveness data mid-tick.
///
/// Note: the real coordinator splits candidates by density threshold; this
/// simulation splits by position (first → repack, rest → sweep).  That
/// difference does not affect oracle correctness — the test only verifies that
/// two independent compactions from disjoint input sets preserve all data.
///
/// Returns `Some((repack_result, sweep_result))` when both strategies found
/// candidates, where each result is `(consumed_ulids, produced_ulid,
/// paths_to_delete)`.  Returns `None` when fewer than 3 segments exist.
pub fn simulate_coord_gc_both_local(
    fork_dir: &Path,
    repack_ulid: Ulid,
    sweep_ulid: Ulid,
) -> Option<(
    (Vec<Ulid>, Ulid, Vec<PathBuf>),
    (Vec<Ulid>, Ulid, Vec<PathBuf>),
)> {
    let segments_dir = fork_dir.join("segments");
    let gc_dir = fork_dir.join("gc");

    let seg_files = segment::collect_segment_files(&segments_dir).ok()?;
    let mut all_candidates: Vec<(Ulid, PathBuf)> = seg_files
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            let ulid = Ulid::from_string(name).ok()?;
            Some((ulid, p.clone()))
        })
        .collect();

    // Need at least 1 for repack + 2 for sweep.
    if all_candidates.len() < 3 {
        return None;
    }

    sort_candidates(&mut all_candidates, &gc_dir);

    // Rebuild liveness snapshot once — shared by both passes.
    let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
    let lba_map = lbamap::rebuild_segments(&rebuild_chain).ok()?;
    let live_hashes = lba_map.live_hashes();
    let extent_index = extentindex::rebuild(&rebuild_chain).ok()?;

    // Repack: first candidate (lowest priority / most likely stale).
    let mut iter = all_candidates.into_iter();
    let repack_candidate = vec![iter.next().unwrap()];
    // Sweep: all remaining candidates.
    let sweep_candidates: Vec<(Ulid, PathBuf)> = iter.collect();

    let repack = compact_candidates_inner(
        fork_dir,
        repack_candidate,
        &lba_map,
        &live_hashes,
        &extent_index,
        repack_ulid,
    )?;
    let sweep = compact_candidates_inner(
        fork_dir,
        sweep_candidates,
        &lba_map,
        &live_hashes,
        &extent_index,
        sweep_ulid,
    )?;

    Some((repack, sweep))
}
