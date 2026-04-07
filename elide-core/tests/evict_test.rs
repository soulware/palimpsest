// Regression tests for eviction correctness.
//
// `evict` (the `elide volume evict` command) deletes segment body files from
// `cache/` to reclaim local disk space.  The coordinator writes
// `index/<ulid>.idx` after confirmed S3 upload, so evict can safely delete the
// body without any additional work.  After a crash+reopen, `Volume::open`
// rebuilds the LBA map from `pending/` and `index/*.idx`.

use std::fs;
use std::path::PathBuf;

use elide_core::volume::Volume;

mod common;

/// After correctly evicting local segment bodies (with coordinator-written
/// `index/*.idx` already present), the LBA map survives a crash+reopen.
///
/// `drain_with_materialise` simulates the coordinator drain: materialise thin
/// refs then promote to `index/<ulid>.idx` + `cache/<ulid>.body`.  Evict then
/// deletes the body.  After a restart, `Volume::open` rebuilds the LBA map
/// from `index/*.idx` and subsequent reads fall through to the `SegmentFetcher`.
#[test]
fn evict_then_crash_data_survives() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0xAB; 4096]).unwrap();
    vol.write(1, &[0xCD; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    // Simulate evict: delete cache/ body files (index/.idx already present from drain).
    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "body") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    // Crash + reopen (triggers full LBA map rebuild from disk).
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map is rebuilt from index/*.idx; the data is known to exist but
    // the body is not locally present.  Without a SegmentFetcher configured,
    // the read fails with "segment not found" rather than returning zeros —
    // which is the correct behaviour (fail loudly vs silently corrupt).
    assert_eq!(
        vol.lbamap_len(),
        2,
        "lba map must have 2 entries after evict+crash"
    );
}

/// Without `index/*.idx` files, evicting segment bodies causes silent data
/// loss after a crash: the LBA map has no entries and reads return zeros.
#[test]
fn evict_without_idx_loses_lba_map() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0xAB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_materialise(&mut vol);

    // Broken evict: delete index/*.idx (coordinator never committed to S3),
    // then delete the body — no trace remains.
    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    for entry in fs::read_dir(&index_dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    for entry in fs::read_dir(&cache_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().is_some_and(|e| e == "body") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA map is empty — the segment's LBAs are gone without a trace.
    assert_eq!(
        vol.lbamap_len(),
        0,
        "evict without index/*.idx must produce empty lba map"
    );
}
