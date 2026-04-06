// Regression tests for GC handoff cleanup ordering:
// `index/<old>.idx` must be deleted BEFORE the old S3 object is removed.
//
// Invariant: `index/<ulid>.idx` present ↔ segment guaranteed to be in S3.
//
// If the coordinator deletes the S3 object but leaves `index/<old>.idx`, then
// after eviction + restart, `rebuild_segments` maps those LBAs to a segment
// that no longer exists in S3.  Every subsequent demand-fetch for those LBAs
// fails with "not found in any ancestor", making the volume unreadable.
//
// The fix (elide-coordinator/src/gc.rs): delete `index/<old>.idx` before
// deleting old S3 objects, so dangling index entries can never be created.
//
// These tests simulate the coordinator cleanup step locally (no object store)
// to verify that `index/<old>.idx` files are absent after GC + coordinator
// cleanup, and that the volume remains readable after eviction + restart.

use std::fs;
use std::path::PathBuf;

use elide_core::segment::extract_idx;
use elide_core::volume::Volume;

mod common;

/// After a correct GC + coordinator cleanup (old idx deleted before S3 deletion),
/// eviction + crash+reopen leaves no dangling `index/<old>.idx` entries and
/// the LBA map is fully intact.
///
/// Simulates the full coordinator cleanup sequence:
///   1. drain_local already wrote index/<old>.idx + cache/<old>.{body,present}
///   2. GC compacts → gc/<new>
///   3. apply_gc_handoffs: re-signs gc/<new>, updates extent index
///   4. Write index/<new>.idx + cache/<new>.{body,present} (simulating S3 upload + cache promotion)
///   5. Delete index/<old>.idx for each consumed segment  ← THE FIX
///   6. Delete cache/<old>.{body,present}
///   7. Delete gc/<new> (body moved to cache)
///   8. Evict: delete cache/<new>.body
///   9. Crash + reopen — verify no dangling idx, correct lbamap
#[test]
fn gc_cleanup_deletes_old_idx_before_evict() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Write two blocks across two separate flush cycles to produce two segments.
    // drain_local writes index/<ulid>.idx + cache/<ulid>.{body,present} for each.
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    vol.write(1, &[0xBB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    let gc_dir = fork_dir.join("gc");

    // Collect old idx names (without extension) for verification later.
    let old_idx_stems: Vec<String> = fs::read_dir(&index_dir)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".idx").map(String::from)
        })
        .collect();
    assert_eq!(old_idx_stems.len(), 2, "expected 2 committed segments");

    // Obtain GC checkpoint ULIDs from the volume (as the coordinator would).
    let (new_ulid, _) = vol.gc_checkpoint().unwrap();

    // Run GC compaction — produces `gc/<new>.pending`.
    let (consumed_ulids, produced_ulid, paths_to_delete) =
        common::simulate_coord_gc_local(&fork_dir, new_ulid, 2).unwrap();
    assert_eq!(produced_ulid, new_ulid);

    // Volume applies the handoff: re-signs gc/<new>, updates extent index,
    // renames gc/<new>.pending → gc/<new>.applied.
    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0);

    // --- Simulate correct coordinator cleanup ---
    //
    // After apply_gc_handoffs, the re-signed segment body is in gc/<new>.
    // The coordinator:
    //   1. Uploads gc/<new> to S3
    //   2. Writes index/<new>.idx (header+index section) + cache/<new>.{body,present}
    //   3. Deletes index/<old>.idx  ← THE FIX (before S3 deletion)
    //   4. Deletes cache/<old>.{body,present}
    //   5. Deletes gc/<new> (body now in cache)
    //   6. Renames .applied → .done
    let new_ulid_str = produced_ulid.to_string();
    let gc_seg_path = gc_dir.join(&new_ulid_str);

    // Step 1+2: upload gc/<new> to S3 (simulated), write index/<new>.idx.
    let new_idx_path = index_dir.join(format!("{new_ulid_str}.idx"));
    extract_idx(&gc_seg_path, &new_idx_path).unwrap();

    // Write cache/<new>.body (body section of gc/<new>) + cache/<new>.present.
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = fs::File::open(&gc_seg_path).unwrap();
        let mut header = [0u8; 96];
        f.read_exact(&mut header).unwrap();
        let index_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let inline_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let entry_count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let bss = 96 + index_length as u64 + inline_length as u64;
        f.seek(SeekFrom::Start(bss)).unwrap();
        let mut body = Vec::new();
        f.read_to_end(&mut body).unwrap();
        fs::write(cache_dir.join(format!("{new_ulid_str}.body")), &body).unwrap();
        let bitset_len = (entry_count as usize).div_ceil(8);
        fs::write(
            cache_dir.join(format!("{new_ulid_str}.present")),
            vec![0xFFu8; bitset_len],
        )
        .unwrap();
    }

    // Step 3: delete index/<old>.idx for every consumed segment — THE FIX.
    for old_ulid in &consumed_ulids {
        let old_ulid_str = old_ulid.to_string();
        let _ = fs::remove_file(index_dir.join(format!("{old_ulid_str}.idx")));
    }

    // Step 4: delete old cache files (simulates S3 object deletion).
    for path in &paths_to_delete {
        let _ = fs::remove_file(path);
    }
    for old_ulid in &consumed_ulids {
        let s = old_ulid.to_string();
        let _ = fs::remove_file(cache_dir.join(format!("{s}.body")));
        let _ = fs::remove_file(cache_dir.join(format!("{s}.present")));
    }

    // Step 5: delete gc/<new> body (moved to cache).
    fs::remove_file(&gc_seg_path).unwrap();

    // Step 6: rename gc/<new>.applied → gc/<new>.done.
    let applied_path = gc_dir.join(format!("{new_ulid_str}.applied"));
    let done_path = gc_dir.join(format!("{new_ulid_str}.done"));
    if applied_path.exists() {
        fs::rename(&applied_path, &done_path).unwrap();
    }

    // Evict: delete cache/<new>.body (simulates post-upload eviction).
    fs::remove_file(cache_dir.join(format!("{new_ulid_str}.body"))).unwrap();

    // Assert: no dangling old idx files remain in index/.
    for old_stem in &old_idx_stems {
        let dangling = index_dir.join(format!("{old_stem}.idx"));
        assert!(
            !dangling.exists(),
            "dangling index/{old_stem}.idx must not exist after correct GC cleanup"
        );
    }

    // Step 9: crash + reopen.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map must be rebuilt from index/<new>.idx only — both LBAs intact.
    assert_eq!(
        vol.lbamap_len(),
        2,
        "lba map must have both LBAs after GC + correct cleanup + evict + reopen"
    );
}

/// Regression: if the coordinator skips `index/<old>.idx` deletion (the
/// pre-fix bug), dangling index entries survive after eviction + restart.
/// On reopen, `rebuild_segments` maps those LBAs to a segment absent from
/// both disk and S3 — the volume would fail every read for those LBAs.
///
/// This test documents the failure mode: after the buggy cleanup path,
/// `index/` contains stale entries for the old (deleted) segments.
#[test]
fn gc_cleanup_without_idx_deletion_leaves_dangling_idx() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Two separate flush cycles to produce two committed segments in index/.
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    vol.write(1, &[0xBB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    let gc_dir = fork_dir.join("gc");

    let (new_ulid, _) = vol.gc_checkpoint().unwrap();
    let (consumed_ulids, produced_ulid, paths_to_delete) =
        common::simulate_coord_gc_local(&fork_dir, new_ulid, 2).unwrap();
    assert_eq!(produced_ulid, new_ulid);

    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0);

    // --- Simulate BUGGY coordinator cleanup (pre-fix) ---
    //
    // After apply_gc_handoffs, the re-signed segment is in gc/<new>.
    // The buggy cleanup:
    //   1. Uploads gc/<new> to S3, writes index/<new>.idx + cache/<new>.{body,present}
    //   2. Deletes gc/<new>
    //   BUG: skips deleting index/<old>.idx        ← missing step
    //   3. Deletes old cache files (old S3 objects deleted)
    //   4. Renames .applied → .done
    let new_ulid_str = produced_ulid.to_string();
    let gc_seg_path = gc_dir.join(&new_ulid_str);

    // Step 1: write index/<new>.idx (header+index section).
    let new_idx_path = index_dir.join(format!("{new_ulid_str}.idx"));
    extract_idx(&gc_seg_path, &new_idx_path).unwrap();

    // Write cache/<new>.body + cache/<new>.present.
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = fs::File::open(&gc_seg_path).unwrap();
        let mut header = [0u8; 96];
        f.read_exact(&mut header).unwrap();
        let index_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let inline_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let entry_count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let bss = 96 + index_length as u64 + inline_length as u64;
        f.seek(SeekFrom::Start(bss)).unwrap();
        let mut body = Vec::new();
        f.read_to_end(&mut body).unwrap();
        fs::write(cache_dir.join(format!("{new_ulid_str}.body")), &body).unwrap();
        let bitset_len = (entry_count as usize).div_ceil(8);
        fs::write(
            cache_dir.join(format!("{new_ulid_str}.present")),
            vec![0xFFu8; bitset_len],
        )
        .unwrap();
    }

    // Step 2: delete gc/<new>.
    fs::remove_file(&gc_seg_path).unwrap();

    // BUG: skip deleting index/<old>.idx files.
    // (The real fix does `fs::remove_file(index_dir.join(old_idx))` here.)
    // paths_to_delete = the consumed index/*.idx paths — we intentionally do NOT
    // delete them here to simulate the pre-fix coordinator that left them behind.
    let _ = &paths_to_delete; // acknowledge but don't use

    // Step 3: delete old cache files (simulating S3 object deletion).
    for old_ulid in &consumed_ulids {
        let s = old_ulid.to_string();
        let _ = fs::remove_file(cache_dir.join(format!("{s}.body")));
        let _ = fs::remove_file(cache_dir.join(format!("{s}.present")));
    }

    let applied_path = gc_dir.join(format!("{new_ulid_str}.applied"));
    let done_path = gc_dir.join(format!("{new_ulid_str}.done"));
    if applied_path.exists() {
        fs::rename(&applied_path, &done_path).unwrap();
    }

    // Assert the bug: dangling idx files are still present for old (gone) segments.
    let dangling_count = consumed_ulids
        .iter()
        .filter(|u| {
            let s = u.to_string();
            index_dir.join(format!("{s}.idx")).exists()
        })
        .count();
    assert!(
        dangling_count > 0,
        "pre-fix bug: expected dangling index/*.idx files for consumed segments"
    );

    // On reopen, rebuild_segments picks up the dangling idx files, mapping
    // those hashes to segment IDs that no longer exist anywhere.  The lbamap
    // may appear non-empty, but reads for those LBAs would fail at demand-fetch
    // time because the segments are absent from both disk and S3.
    drop(vol);
    // (We do not assert lbamap_len here because the mapping appears "present"
    // until a read is attempted — the corruption only surfaces on actual I/O.)
}
