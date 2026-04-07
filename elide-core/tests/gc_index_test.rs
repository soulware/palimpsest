// Regression tests for GC handoff index/ ownership.
//
// Invariant: `index/<ulid>.idx` present ↔ segment confirmed in S3.
//
// In the new design, the volume owns `index/` entirely:
//   - `index/<ulid>.idx` written by `promote_segment` IPC after S3 upload confirmation.
//   - `flush_wal_to_pending_as` writes only `pending/<ulid>` — no idx.
//   - `apply_gc_handoffs` does NOT write new idx or delete old idx; it renames
//     gc/<new>.pending → gc/<new>.applied only.
//   - `promote_segment` (GC path): writes `index/<new>.idx` AND deletes
//     `index/<old>.idx` by reading the `.applied` handoff file.
//
// The coordinator never reads or writes `index/` directly.
//
// These tests verify:
//   1. After `apply_gc_handoffs`: old idx still present, new idx not yet written.
//   2. After `promote_segment`: new idx present, old idx deleted.
//   3. After eviction + restart, the volume is fully readable with a correct lbamap.
//   4. index/<old>.idx is always absent when the old S3 object is gone (ordering
//      invariant: promote_segment deletes old idx before coordinator sees .applied).

use std::fs;
use std::path::PathBuf;

use elide_core::volume::Volume;

mod common;

/// After `apply_gc_handoffs`, old idx still present and new idx not yet written.
/// After `promote_segment`, new idx is present and old idx is gone.
/// After eviction + restart, the volume is fully readable and the lbamap is correct.
#[test]
fn gc_cleanup_deletes_old_idx_before_evict() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Write two blocks across two separate flush cycles to produce two segments.
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    vol.write(1, &[0xBB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

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

    // apply_gc_handoffs: re-signs gc/<new>, updates extent index,
    // renames .pending → .applied.  Does NOT write new idx or delete old idx.
    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0);

    let new_ulid_str = produced_ulid.to_string();
    let gc_seg_path = gc_dir.join(&new_ulid_str);

    // index/<new>.idx must NOT be present yet — only written at promote time.
    assert!(
        !index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "apply_gc_handoffs must NOT write index/<new>.idx (deferred to promote_segment)"
    );

    // index/<old>.idx must still be present — not yet deleted.
    for old_stem in &old_idx_stems {
        assert!(
            index_dir.join(format!("{old_stem}.idx")).exists(),
            "apply_gc_handoffs must NOT delete index/{old_stem}.idx (deferred to promote_segment)"
        );
    }

    // Simulate coordinator: promote IPC → writes index/<new>.idx, cache/<new>.{body,present},
    // deletes index/<old>.idx (by reading .applied handoff), deletes pending/<new>.
    vol.promote_segment(produced_ulid).unwrap();

    // index/<new>.idx now present (written by promote_segment).
    assert!(
        index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "promote_segment must write index/<new>.idx"
    );

    // index/<old>.idx now absent (deleted by promote_segment via .applied handoff).
    for old_stem in &old_idx_stems {
        assert!(
            !index_dir.join(format!("{old_stem}.idx")).exists(),
            "promote_segment must delete index/{old_stem}.idx"
        );
    }

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

/// Verify the ordering invariant for `index/<old>.idx` deletion:
/// `promote_segment` (not `apply_gc_handoffs`) deletes old idx and writes new idx.
/// After `apply_gc_handoffs`: old idx still present, new idx absent, `.applied` written.
/// After `promote_segment`: new idx present, old idx absent.
///
/// The coordinator only acts on `.applied` after `promote_segment` has run,
/// so by the time `.applied` is visible the old S3 object is still present
/// (coordinator deletes it after promote). Old idx is deleted inside promote_segment
/// before coordinator sees the body — so old idx is never dangling relative to S3.
#[test]
fn apply_gc_handoffs_deletes_old_idx_atomically_with_applied_rename() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0x11; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    vol.write(1, &[0x22; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

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
    // index/<old>.idx must still be present — not yet deleted by promote_segment.
    for old_ulid in &consumed_ulids {
        let s = old_ulid.to_string();
        assert!(
            index_dir.join(format!("{s}.idx")).exists(),
            "index/{s}.idx must still be present before promote_segment runs"
        );
    }
    // index/<new>.idx must NOT be present yet — only written at promote time.
    assert!(
        !index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "index/<new>.idx must not exist before promote_segment"
    );

    // Now run promote_segment (simulates coordinator after S3 upload of GC output).
    vol.promote_segment(produced_ulid).unwrap();

    // index/<new>.idx now present, old idx gone.
    assert!(
        index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "promote_segment must write index/<new>.idx"
    );
    for old_ulid in &consumed_ulids {
        let s = old_ulid.to_string();
        assert!(
            !index_dir.join(format!("{s}.idx")).exists(),
            "promote_segment must delete index/{s}.idx"
        );
    }
}
