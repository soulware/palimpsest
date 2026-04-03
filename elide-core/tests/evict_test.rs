// Regression tests for eviction correctness.
//
// `evict` (the `elide volume evict` command) deletes segment files from
// `segments/` to reclaim local disk space.  For a volume that has S3 backing,
// evicted segments must still be accessible after a crash+reopen: the LBA map
// is rebuilt at `Volume::open` from `pending/`, `segments/`, and
// `fetched/*.idx`.  If `evict` does not write `fetched/<ulid>.idx` before
// deleting `segments/<ulid>`, those LBAs are absent from the rebuilt map and
// reads silently return zeros.

use std::fs;
use std::path::PathBuf;

use elide_core::segment::extract_idx;
use elide_core::volume::Volume;

mod common;

/// After correctly evicting local segment bodies, data must still be
/// recoverable after a crash+reopen via the `.idx` files written to `fetched/`.
///
/// The correct evict sequence is: for each segment, write `fetched/<id>.idx`
/// (header+index bytes) first, then delete `segments/<id>`.  After a restart,
/// `Volume::open` rebuilds the LBA map from `fetched/*.idx` and subsequent
/// reads fall through to the `SegmentFetcher` for body bytes.
#[test]
fn evict_then_crash_data_survives() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0xAB; 4096]).unwrap();
    vol.write(1, &[0xCD; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir); // pending/ → segments/

    // Correct evict: write .idx to fetched/ before deleting the segment body.
    let segments_dir = fork_dir.join("segments");
    let fetched_dir = fork_dir.join("fetched");
    fs::create_dir_all(&fetched_dir).unwrap();
    for entry in fs::read_dir(&segments_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let idx_path = fetched_dir.join(format!("{}.idx", name.to_string_lossy()));
        extract_idx(&entry.path(), &idx_path).unwrap();
        fs::remove_file(entry.path()).unwrap();
    }

    // Crash + reopen (triggers full LBA map rebuild from disk).
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map is rebuilt from fetched/*.idx; the data is known to exist but
    // the body is not locally present.  Without a SegmentFetcher configured,
    // the read fails with "segment not found" rather than returning zeros —
    // which is the correct behaviour (fail loudly vs silently corrupt).
    //
    // For LBA map survival we check: the volume knows these LBAs exist (the
    // lbamap has entries for them).  The body-fetch path is tested separately.
    assert_eq!(
        vol.lbamap_len(),
        2,
        "lba map must have 2 entries after evict+crash"
    );
}

/// Without `.idx` files, evicting segment bodies causes silent data loss after
/// a crash: the LBA map has no entries and reads return zeros instead of
/// failing with a meaningful error.  This is the behaviour the fix eliminates.
#[test]
fn evict_without_idx_loses_lba_map() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0xAB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    // Broken evict: delete without writing .idx.
    let segments_dir = fork_dir.join("segments");
    for entry in fs::read_dir(&segments_dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }

    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map is empty — the segment's LBAs are gone without a trace.
    assert_eq!(
        vol.lbamap_len(),
        0,
        "broken evict must produce empty lba map"
    );
}
