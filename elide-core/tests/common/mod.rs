// Shared simulation helpers for proptest files.
//
// `drain_local` and `simulate_coord_gc_local` mirror the real coordinator's
// drain-pending and GC logic without requiring an object store.  Both proptest
// suites (volume_proptest and actor_proptest) use these to drive the same
// coordinator-side simulation.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use elide_core::{lbamap, segment};
use ulid::Ulid;

/// Move all committed segments from pending/ to segments/.
/// Simulates `drain-pending` (upload + rename) without touching an object store.
pub fn drain_local(fork_dir: &Path) {
    let pending = fork_dir.join("pending");
    let segments = fork_dir.join("segments");
    if let Ok(entries) = fs::read_dir(&pending) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.ends_with(".tmp") {
                let _ = fs::rename(entry.path(), segments.join(&*name_str));
            }
        }
    }
}

/// Simulate one coordinator GC pass on `segments/` without an object store.
///
/// Picks the two lowest-ULID segments as candidates, compacts their entries
/// (including REF entries so the oracle test can still resolve dedup hashes),
/// writes a new segment with the given `new_ulid` (obtained from
/// `gc_checkpoint` which flushes the volume's WAL first), and writes
/// `gc/<new_ulid>.pending` (the handoff file the coordinator produces).
///
/// The input segment files are **not** deleted inline.  The caller receives
/// the consumed paths and is responsible for deleting them — after calling
/// `vol.apply_gc_handoffs()` (or `handle.apply_gc_handoffs()`).  This models
/// the real coordinator's ordering constraint: local segment files must not
/// disappear until the volume has acknowledged the handoff by renaming
/// `.pending` → `.applied`.
///
/// Returns `Some((consumed_ulids, produced_ulid, paths_to_delete))` when
/// candidates were found, `None` when fewer than two segments exist.
pub fn simulate_coord_gc_local(
    fork_dir: &Path,
    new_ulid: Ulid,
) -> Option<(Vec<Ulid>, Ulid, Vec<PathBuf>)> {
    let segments_dir = fork_dir.join("segments");

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
    candidates.sort_by_key(|(u, _)| *u);
    let candidates = candidates[..2].to_vec();

    // Rebuild the LBA map to determine which hashes are still reachable.
    // gc_checkpoint flushed the WAL, so the rebuild sees all committed writes.
    // An entry is only carried into the GC output if its hash is both
    // extent-index-live and LBA-map-live, preventing stale LBA entries from
    // being compacted into the output.  Dedup-ref entries are also checked:
    // a ref is only carried if the LBA still maps to the ref's hash.
    let rebuild_chain = vec![(fork_dir.to_path_buf(), None)];
    let lba_map = lbamap::rebuild_segments(&rebuild_chain).ok()?;
    let live_hashes = lba_map.live_hashes();
    let extent_index = elide_core::extentindex::rebuild(&rebuild_chain).ok()?;

    // Track per-entry source ULIDs separately since SegmentEntry doesn't
    // derive Clone.  source_ulids[i] is the segment ULID that entry i came
    // from — needed for the .pending file format.
    let mut all_entries: Vec<segment::SegmentEntry> = Vec::new();
    let mut source_ulids: Vec<Ulid> = Vec::new();
    let mut removed: Vec<(blake3::Hash, Ulid)> = Vec::new();
    for (ulid, path) in &candidates {
        let seg_id = ulid.to_string();
        let Ok((bss, mut entries)) = segment::read_segment_index(path) else {
            continue;
        };
        if segment::read_extent_bodies(path, bss, &mut entries).is_err() {
            continue;
        }
        for entry in entries.drain(..) {
            if entry.is_dedup_ref {
                // Carry a dedup ref only if the LBA still maps to this hash.
                // A stale ref (LBA was overwritten with different data) must
                // be dropped — otherwise it reintroduces the old LBA mapping
                // into the GC output and corrupts reads after rebuild.
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
                if lba_live {
                    source_ulids.push(*ulid);
                    all_entries.push(entry);
                }
                continue;
            }
            // Check extent-level liveness: extent index points at this segment.
            let extent_live = extent_index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == seg_id);
            if extent_live && live_hashes.contains(&entry.hash) {
                source_ulids.push(*ulid);
                all_entries.push(entry);
            } else if extent_live {
                // Extent-live but LBA-dead: record for removal.
                removed.push((entry.hash, *ulid));
            }
            // If not extent-live, another segment owns this hash — skip.
        }
    }

    if all_entries.is_empty() {
        let consumed = candidates.iter().map(|(u, _)| *u).collect();
        let to_delete = candidates.into_iter().map(|(_, p)| p).collect();
        return Some((consumed, new_ulid, to_delete));
    }

    let tmp_path = segments_dir.join(format!("{new_ulid}.tmp"));
    let final_path = segments_dir.join(new_ulid.to_string());
    let new_bss = match segment::write_segment(&tmp_path, &mut all_entries, None) {
        Ok(bss) => bss,
        Err(_) => return None,
    };
    fs::rename(&tmp_path, &final_path).ok()?;

    // Write the handoff file with per-entry source segment ULIDs, matching
    // the real coordinator's format.
    // Carried entries (4 fields): <hash> <old_ulid> <new_ulid> <offset>
    // Removed entries (2 fields): <hash> <old_ulid>
    let gc_dir = fork_dir.join("gc");
    let _ = fs::create_dir_all(&gc_dir);
    let mut lines = String::new();
    for (e, src_ulid) in all_entries.iter().zip(source_ulids.iter()) {
        if !e.is_dedup_ref {
            let abs_offset = new_bss + e.stored_offset;
            lines.push_str(&format!(
                "{} {} {} {}\n",
                e.hash, src_ulid, new_ulid, abs_offset
            ));
        }
    }
    for (hash, old_ulid) in &removed {
        lines.push_str(&format!("{} {}\n", hash, old_ulid));
    }
    let _ = fs::write(gc_dir.join(format!("{new_ulid}.pending")), lines);

    let consumed = candidates.iter().map(|(u, _)| *u).collect();
    let to_delete = candidates.into_iter().map(|(_, p)| p).collect();
    Some((consumed, new_ulid, to_delete))
}
