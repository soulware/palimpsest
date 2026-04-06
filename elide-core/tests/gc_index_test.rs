// Regression tests for GC handoff index/ ownership.
//
// Invariant: `index/<ulid>.idx` present ↔ segment exists in persistent storage
// (pending/ or S3).
//
// In the new design, the VOLUME owns `index/` entirely:
//   - Written at WAL flush time (before coordinator upload).
//   - `index/<new>.idx` written by `apply_gc_handoffs` from the gc/ body.
//   - `index/<old>.idx` deleted by `apply_gc_handoffs` before renaming .applied.
//
// The coordinator never reads or writes `index/` directly.
//
// These tests verify:
//   1. `apply_gc_handoffs` correctly writes index/<new>.idx and deletes index/<old>.idx.
//   2. After eviction + restart, the volume is fully readable with a correct lbamap.
//   3. index/<old>.idx is always absent when the old S3 object is gone (ordering
//      invariant automatically satisfied because apply_gc_handoffs runs before
//      any S3 deletion).

use std::fs;
use std::path::PathBuf;

use elide_core::volume::Volume;

mod common;

/// After `apply_gc_handoffs`, `index/<new>.idx` exists and `index/<old>.idx` is
/// gone.  After simulating coordinator cleanup (promote + delete gc body) and
/// eviction + restart, the volume is fully readable and the lbamap is correct.
#[test]
fn gc_cleanup_deletes_old_idx_before_evict() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Write two blocks across two separate flush cycles to produce two segments.
    // drain_local simulates the coordinator drain: promote_segment IPC, which
    // copies body to cache/ and deletes pending/.
    // index/<ulid>.idx is written by the volume at flush time.
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    vol.write(1, &[0xBB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    let gc_dir = fork_dir.join("gc");

    // Collect old idx stems for verification.
    let old_idx_stems: Vec<String> = fs::read_dir(&index_dir)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".idx").map(String::from)
        })
        .collect();
    assert_eq!(
        old_idx_stems.len(),
        2,
        "expected 2 committed segments in index/"
    );

    // GC pass.
    let (new_ulid, _) = vol.gc_checkpoint().unwrap();
    let (consumed_ulids, produced_ulid, _paths_to_delete) =
        common::simulate_coord_gc_local(&fork_dir, new_ulid, 2).unwrap();
    assert_eq!(produced_ulid, new_ulid);

    // apply_gc_handoffs: re-signs gc/<new>, updates extent index, writes
    // index/<new>.idx, deletes index/<old>.idx, renames .pending → .applied.
    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0);

    let new_ulid_str = produced_ulid.to_string();
    let gc_seg_path = gc_dir.join(&new_ulid_str);

    // index/<new>.idx must be present (written by apply_gc_handoffs).
    assert!(
        index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "apply_gc_handoffs must write index/<new>.idx"
    );

    // index/<old>.idx must be absent (deleted by apply_gc_handoffs).
    for old_stem in &old_idx_stems {
        assert!(
            !index_dir.join(format!("{old_stem}.idx")).exists(),
            "apply_gc_handoffs must delete index/{old_stem}.idx"
        );
    }

    // Simulate coordinator: promote IPC → cache/<new>.{body,present}, delete gc/<new>.
    vol.promote_segment(produced_ulid).unwrap();
    assert!(cache_dir.join(format!("{new_ulid_str}.body")).exists());
    fs::remove_file(&gc_seg_path).unwrap();

    // Evict cache/<old>.* (actor calls evict_applied_gc_cache after publish).
    vol.evict_applied_gc_cache();
    for old_ulid in &consumed_ulids {
        let s = old_ulid.to_string();
        assert!(
            !cache_dir.join(format!("{s}.body")).exists(),
            "evict_applied_gc_cache must delete cache/{s}.body"
        );
    }

    // Rename .applied → .done (coordinator post-S3-delete step).
    let applied_path = gc_dir.join(format!("{new_ulid_str}.applied"));
    let done_path = gc_dir.join(format!("{new_ulid_str}.done"));
    if applied_path.exists() {
        fs::rename(&applied_path, &done_path).unwrap();
    }

    // Evict cache/<new>.body (simulates post-upload eviction).
    fs::remove_file(cache_dir.join(format!("{new_ulid_str}.body"))).unwrap();

    // Crash + reopen.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map must be rebuilt from index/<new>.idx only — both LBAs intact.
    assert_eq!(
        vol.lbamap_len(),
        2,
        "lba map must have both LBAs after GC + correct cleanup + evict + reopen"
    );
}

/// Verify that `apply_gc_handoffs` atomically maintains the ordering invariant:
/// `index/<old>.idx` is always absent by the time `.applied` is written, so
/// the coordinator can never observe `.applied` with a dangling old idx.
///
/// The invariant "index/<ulid>.idx present ↔ segment in persistent storage" is
/// enforced structurally: the volume deletes `index/<old>.idx` in the same atomic
/// step as the `.pending → .applied` rename.  The coordinator sees `.applied`
/// only after both are done.
#[test]
fn apply_gc_handoffs_deletes_old_idx_atomically_with_applied_rename() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0x11; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    vol.write(1, &[0x22; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let index_dir = fork_dir.join("index");
    let gc_dir = fork_dir.join("gc");

    let (new_ulid, _) = vol.gc_checkpoint().unwrap();
    let (consumed_ulids, produced_ulid, _) =
        common::simulate_coord_gc_local(&fork_dir, new_ulid, 2).unwrap();

    vol.apply_gc_handoffs().unwrap();

    let new_ulid_str = produced_ulid.to_string();

    // .applied marker must exist.
    assert!(
        gc_dir.join(format!("{new_ulid_str}.applied")).exists(),
        "gc/<new>.applied must be present after apply_gc_handoffs"
    );
    // index/<old>.idx must be absent — volume deleted them before writing .applied.
    for old_ulid in &consumed_ulids {
        let s = old_ulid.to_string();
        assert!(
            !index_dir.join(format!("{s}.idx")).exists(),
            "index/{s}.idx must be absent when .applied is written"
        );
    }
    // index/<new>.idx must be present.
    assert!(
        index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "index/<new>.idx must be written by apply_gc_handoffs"
    );
}
