// Regression tests for GC handoff index/ ownership.
//
// Invariant: `index/<ulid>.idx` present ↔ segment confirmed in S3.
//
// Under the self-describing GC handoff protocol the volume owns `index/`:
//   - `index/<ulid>.idx` is written by `promote_segment` IPC after S3 upload
//     confirmation.
//   - `flush_wal_to_pending_as` writes only `pending/<ulid>` — no idx.
//   - `apply_gc_handoffs` re-signs `gc/<new>.staged` to bare `gc/<new>` —
//     it does NOT write new idx or delete old idx.
//   - `promote_segment` (GC path) writes `index/<new>.idx` AND deletes
//     `index/<old>.idx` for each input ulid (read from the new segment's
//     own `inputs` header field).
//
// The coordinator never reads or writes `index/` directly.

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
    common::drain_with_redact(&mut vol);

    vol.write(1, &[0xBB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

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
    let (new_ulid, _) = vol.gc_checkpoint_for_test().unwrap();
    let (consumed_ulids, produced_ulid, _paths_to_delete) =
        common::simulate_coord_gc_local(&fork_dir, new_ulid, 2).unwrap();
    assert_eq!(produced_ulid, new_ulid);

    // apply_gc_handoffs: re-signs gc/<new>.staged, updates extent index,
    // renames .staged → bare. Does NOT write new idx or delete old idx.
    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0);

    let new_ulid_str = produced_ulid.to_string();
    let gc_seg_path = gc_dir.join(&new_ulid_str);
    assert!(gc_seg_path.exists(), "bare gc/<new> must exist after apply");
    assert!(
        !gc_dir.join(format!("{new_ulid_str}.plan")).exists(),
        "apply_gc_handoffs must remove the .plan sibling"
    );

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

    // Simulate coordinator: promote IPC → writes index/<new>.idx,
    // cache/<new>.{body,present}, deletes index/<old>.idx (using inputs
    // from the new segment's header field).
    vol.promote_segment(produced_ulid).unwrap();

    // index/<new>.idx now present (written by promote_segment).
    assert!(
        index_dir.join(format!("{new_ulid_str}.idx")).exists(),
        "promote_segment must write index/<new>.idx"
    );

    // index/<old>.idx now absent (deleted by promote_segment via inputs).
    for old_stem in &old_idx_stems {
        assert!(
            !index_dir.join(format!("{old_stem}.idx")).exists(),
            "promote_segment must delete index/{old_stem}.idx"
        );
    }

    assert!(cache_dir.join(format!("{new_ulid_str}.body")).exists());
    // Bare `gc/<new>` is deleted at finalize time (after S3 upload). For
    // this test we exercise that final step explicitly.
    vol.finalize_gc_handoff(produced_ulid).unwrap();
    assert!(
        !gc_seg_path.exists(),
        "finalize_gc_handoff must delete bare gc/<new>"
    );
    let _ = consumed_ulids;

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

/// Verify the ordering invariant for `index/<old>.idx` deletion under the
/// self-describing GC handoff protocol.
///
/// After `apply_gc_handoffs`: bare `gc/<new>` exists, old idx still present,
/// new idx absent.
/// After `promote_segment`: new idx present, old idx absent.
///
/// `promote_segment` reads the `inputs` field from the new segment's header
/// to know which old idx files to delete, so old idx is never dangling
/// relative to S3.
#[test]
fn apply_gc_handoffs_deletes_old_idx_atomically_with_applied_rename() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0x11; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    vol.write(1, &[0x22; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    let index_dir = fork_dir.join("index");
    let gc_dir = fork_dir.join("gc");

    let (new_ulid, _) = vol.gc_checkpoint_for_test().unwrap();
    let (consumed_ulids, produced_ulid, _) =
        common::simulate_coord_gc_local(&fork_dir, new_ulid, 2).unwrap();

    vol.apply_gc_handoffs().unwrap();

    let new_ulid_str = produced_ulid.to_string();

    // Bare gc/<new> must exist after apply.
    assert!(
        gc_dir.join(&new_ulid_str).exists(),
        "bare gc/<new> must be present after apply_gc_handoffs"
    );
    assert!(
        !gc_dir.join(format!("{new_ulid_str}.plan")).exists(),
        "apply_gc_handoffs must remove the .plan sibling"
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

    // promote_segment reads inputs from the new segment header and deletes
    // the corresponding old idx files.
    vol.promote_segment(produced_ulid).unwrap();

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
