// Shared simulation helpers for proptest files.
//
// `drain_with_redact` and `simulate_coord_gc_local` mirror the real
// coordinator's drain-pending and GC logic without requiring an object store.
// Both proptest suites (volume_proptest and actor_proptest) use these to drive
// the same coordinator-side simulation.

#![allow(dead_code)]

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use elide_core::actor::VolumeHandle;
use elide_core::gc::{HandoffLine, format_handoff_file};
use elide_core::{
    extentindex, lbamap,
    segment::{self, EntryKind},
    signing,
};
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

/// Drain via the full redact → promote path, matching the production
/// coordinator upload protocol.
///
/// For each pending segment: calls `vol.redact_segment(ulid)` (in-place
/// hole-punching of hash-dead DATA regions) then `vol.promote_segment(ulid)`
/// which writes `index/<ulid>.idx` + `cache/<ulid>.{body,present}` from the
/// pending file and updates the extent index.
pub fn drain_with_redact(vol: &mut elide_core::volume::Volume) {
    for ulid in pending_ulids(vol.base_dir()) {
        vol.redact_segment(ulid).unwrap();
        vol.promote_segment(ulid).unwrap();
    }
}

/// Drain via the actor handle: redact + promote each pending segment.
///
/// Equivalent to `drain_with_redact` but works when the `Volume` is behind
/// an actor — sends `RedactSegment` and `Promote` messages through the
/// handle's channel, so the actor's in-memory snapshot is updated correctly.
pub fn drain_via_handle(handle: &VolumeHandle, base_dir: &Path) {
    for ulid in pending_ulids(base_dir) {
        handle.redact_segment(ulid).unwrap();
        handle.promote_segment(ulid).unwrap();
    }
}

/// Collect sorted ULIDs of full segment files in pending/.
pub fn pending_ulids(base_dir: &Path) -> Vec<Ulid> {
    let pending_dir = base_dir.join("pending");
    let Ok(entries) = fs::read_dir(&pending_dir) else {
        return Vec::new();
    };
    let mut ulids: Vec<Ulid> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.contains('.') {
            continue;
        }
        if let Ok(ulid) = Ulid::from_string(name_str) {
            ulids.push(ulid);
        }
    }
    ulids.sort();
    ulids
}

/// Write a single-entry segment directly to `index/<ulid>.idx` +
/// `cache/<ulid>.{body,present}`, bypassing the WAL.
///
/// Simulates a `SegmentFetcher::fetch` result — a segment downloaded from S3 into
/// the local demand-fetch cache.  `lba` is the block address; `seed` fills the
/// 4096-byte block.
///
/// `.idx` goes to `index/` (coordinator-written, permanent LBA index).
/// `.body` and `.present` go to `cache/` (volume-managed body cache).
///
/// Signed with the volume's own key because `rebuild_segments` verifies `.idx`
/// signatures against `volume.pub`.
pub fn populate_cache(fork_dir: &Path, ulid: Ulid, lba: u64, seed: u8) {
    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    let _ = fs::create_dir_all(&index_dir);
    let _ = fs::create_dir_all(&cache_dir);

    let signer = signing::load_signer(fork_dir, signing::VOLUME_KEY_FILE).unwrap();
    let data = vec![seed; 4096];
    let hash = blake3::hash(&data);
    let mut entries = vec![segment::SegmentEntry::new_data(
        hash,
        lba,
        1,
        segment::SegmentFlags::empty(),
        data,
    )];

    // Write a complete segment to a temp file, then split into the two-directory format.
    let tmp = cache_dir.join(format!("{ulid}.tmp"));
    let bss = segment::write_segment(&tmp, &mut entries, signer.as_ref()).unwrap();
    let bytes = fs::read(&tmp).unwrap();
    fs::remove_file(&tmp).unwrap();

    let s = ulid.to_string();
    // .idx → index/ (coordinator-written LBA index; verified by rebuild_segments)
    fs::write(index_dir.join(format!("{s}.idx")), &bytes[..bss as usize]).unwrap();
    // .body → cache/ (body section; body-relative offsets; byte 0 = first body byte)
    fs::write(cache_dir.join(format!("{s}.body")), &bytes[bss as usize..]).unwrap();
    // .present → cache/ (all entries present; 1 entry, bit 0 set)
    segment::set_present_bit(&cache_dir.join(format!("{s}.present")), 0, 1).unwrap();
}

/// Sort-for-rebuild ordering: GC outputs (.pending/.applied handoff present)
/// come first (lower priority); regular segments come last (higher priority).
/// Within each group, sort by ULID ascending.
fn sort_candidates(candidates: &mut [(Ulid, PathBuf)], gc_dir: &Path) {
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

/// Result of a single GC compaction pass: `(consumed_ulids, produced_ulid, paths_to_delete)`.
pub type CompactResult = (Vec<Ulid>, Ulid, Vec<PathBuf>);

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
) -> Option<CompactResult> {
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
        let Ok((bss_header, mut entries)) = segment::read_and_verify_segment_index(path, &vk)
        else {
            continue;
        };
        // For .idx files (committed segments), bodies are in cache/<ulid>.body at offset 0.
        // For full segment files (pending/, gc/), bodies are in the same file at bss_header.
        let is_idx = path.extension().is_some_and(|e| e == "idx");
        let (body_path, bss) = if is_idx {
            let ulid_str = ulid.to_string();
            (
                fork_dir.join("cache").join(format!("{ulid_str}.body")),
                0u64,
            )
        } else {
            (path.clone(), bss_header)
        };
        // Read inline section for inline entries from the source (.idx or segment).
        let inline_bytes = segment::read_inline_section(path).unwrap_or_default();
        if segment::read_extent_bodies(
            &body_path,
            bss,
            &mut entries,
            [segment::EntryKind::Data, segment::EntryKind::Inline],
            &inline_bytes,
        )
        .is_err()
        {
            continue;
        }
        for entry in entries.drain(..) {
            if entry.kind == EntryKind::DedupRef {
                // DedupRef in S3 has body bytes (filled by materialization).
                // Liveness is LBA-based: keep if LBA still maps to this hash.
                let lba_live = lba_map.hash_at(entry.start_lba) == Some(entry.hash);
                let extent_live = extent_index
                    .lookup(&entry.hash)
                    .is_some_and(|loc| loc.segment_id == *ulid);
                if lba_live {
                    source_ulids.push(*ulid);
                    all_entries.push(entry);
                } else if extent_live {
                    removed.push((entry.hash, *ulid));
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
            let extent_live = extent_index
                .lookup(&entry.hash)
                .is_some_and(|loc| loc.segment_id == *ulid);
            if lba_live || (extent_live && live_hashes.contains(&entry.hash)) {
                source_ulids.push(*ulid);
                all_entries.push(entry);
            } else if extent_live {
                removed.push((entry.hash, *ulid));
            }
        }
    }

    let _ = fs::create_dir_all(&gc_dir);

    if all_entries.is_empty() && removed.is_empty() {
        // All candidates were entirely dead.  Write a tombstone .pending so
        // apply_gc_handoffs exercises the Dead acknowledgment path, matching
        // the real coordinator's tombstone protocol.
        let handoff_lines: Vec<HandoffLine> = candidates
            .iter()
            .map(|(ulid, _)| HandoffLine::Dead { old_ulid: *ulid })
            .collect();
        let _ = fs::write(
            gc_dir.join(format!("{new_ulid}.pending")),
            format_handoff_file(handoff_lines),
        );
        let consumed = candidates.iter().map(|(u, _)| *u).collect();
        let to_delete = candidates.into_iter().map(|(_, p)| p).collect();
        return Some((consumed, new_ulid, to_delete));
    }

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
    // applying the handoff, at which point it moves to cache/<ulid>.body.
    let tmp_path = gc_dir.join(format!("{new_ulid}.tmp"));
    let final_path = gc_dir.join(new_ulid.to_string());
    let new_bss =
        match segment::write_segment(&tmp_path, &mut all_entries, ephemeral_signer.as_ref()) {
            Ok(bss) => bss,
            Err(_) => return None,
        };
    fs::rename(&tmp_path, &final_path).ok()?;

    // Build Repack lines, deduplicating by hash: emit one Repack per unique
    // hash, preferring the extent-canonical source segment (lowest ULID).
    // With dedup, the same hash can appear in multiple input segments (DATA
    // in one, DedupRef in another).  The extent index tracks one
    // canonical location per hash (lowest ULID wins), and apply_gc_handoffs'
    // still_at_old check compares against the single old_ulid in the handoff
    // — so we must use the canonical segment's ULID.  Non-canonical entries
    // are still in the output segment (preserving their LBA mappings) but
    // don't generate Repack lines.
    let mut seen_repack_hashes: HashSet<blake3::Hash> = HashSet::new();
    let mut handoff_lines: Vec<HandoffLine> = Vec::new();
    // First pass: emit Repacks for extent-canonical entries.
    for (e, src_ulid) in all_entries.iter().zip(source_ulids.iter()) {
        if e.kind == EntryKind::DedupRef {
            continue;
        }
        if seen_repack_hashes.contains(&e.hash) {
            continue;
        }
        let is_canonical = extent_index
            .lookup(&e.hash)
            .is_some_and(|loc| loc.segment_id == *src_ulid);
        if is_canonical {
            seen_repack_hashes.insert(e.hash);
            handoff_lines.push(HandoffLine::Repack {
                hash: e.hash,
                old_ulid: *src_ulid,
                new_ulid,
                new_offset: new_bss + e.stored_offset,
            });
        }
    }
    // Second pass: emit Repacks for any remaining hashes (no canonical entry
    // was in the candidate set — use whichever source we have).
    for (e, src_ulid) in all_entries.iter().zip(source_ulids.iter()) {
        if e.kind == EntryKind::DedupRef {
            continue;
        }
        if seen_repack_hashes.insert(e.hash) {
            handoff_lines.push(HandoffLine::Repack {
                hash: e.hash,
                old_ulid: *src_ulid,
                new_ulid,
                new_offset: new_bss + e.stored_offset,
            });
        }
    }
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

/// Simulate one coordinator GC pass on committed segments without an object store.
///
/// Picks the `n_candidates` lowest-priority segments (sort_for_rebuild order)
/// from `index/*.idx`, compacts their entries, writes a new segment with the
/// given `new_ulid` (obtained from `gc_checkpoint` which flushes the volume's
/// WAL first), and writes `gc/<new_ulid>.pending`.
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
) -> Option<CompactResult> {
    let index_dir = fork_dir.join("index");
    let gc_dir = fork_dir.join("gc");

    let idx_files = segment::collect_idx_files(&index_dir).ok()?;
    let mut candidates: Vec<(Ulid, PathBuf)> = idx_files
        .iter()
        .filter_map(|p| {
            let stem = p.file_stem()?.to_str()?;
            let ulid = Ulid::from_string(stem).ok()?;
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
/// Requires ≥ 3 committed segments in `index/`: the first (lowest-priority)
/// segment is the repack candidate; the remaining segments are the sweep
/// candidates.  Both passes share a single liveness snapshot (lba_map +
/// extent_index rebuilt once before either pass runs), which matches the real
/// coordinator's behaviour: `gc_fork` calls `collect_stats` once, removes the
/// repack candidate with `all_stats.remove(pos)`, and sweeps the remainder —
/// neither pass rebuilds liveness data mid-tick.
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
) -> Option<(CompactResult, CompactResult)> {
    let index_dir = fork_dir.join("index");
    let gc_dir = fork_dir.join("gc");

    let idx_files = segment::collect_idx_files(&index_dir).ok()?;
    let mut all_candidates: Vec<(Ulid, PathBuf)> = idx_files
        .iter()
        .filter_map(|p| {
            let stem = p.file_stem()?.to_str()?;
            let ulid = Ulid::from_string(stem).ok()?;
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
