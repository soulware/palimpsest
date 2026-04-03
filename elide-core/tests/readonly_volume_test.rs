// Integration tests for ReadonlyVolume.
//
// ReadonlyVolume differs from Volume in three ways:
//   - Does not create wal/, does not acquire an exclusive lock.
//   - Does not replay the WAL — writes that have not been flushed to pending/
//     are invisible.
//   - Intended for the --readonly NBD serve path where a separate writer may
//     be active on the same fork concurrently.
//
// Each test opens a writable Volume, performs some operations, then opens a
// ReadonlyVolume on the same directory and verifies reads.

use std::path::PathBuf;

use elide_core::volume::{ReadonlyVolume, Volume, fork_volume};
use ulid::Ulid;

mod common;

/// Unwritten LBAs must return all-zero bytes.
#[test]
fn readonly_unwritten_lba_returns_zeros() {
    let dir = tempfile::TempDir::new().unwrap();
    let rv = ReadonlyVolume::open(dir.path(), dir.path()).unwrap();
    let actual = rv.read(0, 1).unwrap();
    assert_eq!(actual, vec![0u8; 4096]);
}

/// Data flushed to pending/ is visible; data only in the WAL is not.
#[test]
fn readonly_sees_flushed_pending_not_wal() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // LBA 0: flushed to pending/ — should be visible.
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();

    // LBA 1: in WAL only, not flushed — should be invisible (returns zeros).
    vol.write(1, &[0xBB; 4096]).unwrap();

    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();

    let lba0 = rv.read(0, 1).unwrap();
    assert_eq!(lba0, vec![0xAA; 4096], "flushed LBA 0 should be visible");

    let lba1 = rv.read(1, 1).unwrap();
    assert_eq!(lba1, vec![0u8; 4096], "WAL-only LBA 1 should be invisible");
}

/// Data drained to segments/ is visible.
#[test]
fn readonly_sees_drained_segments() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    for lba in 0u64..4 {
        vol.write(lba, &[0xCC; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
    for lba in 0u64..4 {
        let actual = rv.read(lba, 1).unwrap();
        assert_eq!(actual, vec![0xCC; 4096], "lba {lba} wrong after drain");
    }
}

/// Data is still visible after a coordinator GC pass.
#[test]
fn readonly_sees_data_after_gc() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    for lba in 0u64..4 {
        vol.write(lba, &[0xDD; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    for lba in 4u64..8 {
        vol.write(lba, &[0xEE; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let (gc_ulid_str, _) = vol.gc_checkpoint().unwrap();
    let gc_ulid = Ulid::from_string(&gc_ulid_str).unwrap();
    let (_, _, to_delete) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC should compact the two segments");
    vol.apply_gc_handoffs().unwrap();
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
    for lba in 0u64..4 {
        let actual = rv.read(lba, 1).unwrap();
        assert_eq!(actual, vec![0xDD; 4096], "lba {lba} wrong after GC");
    }
    for lba in 4u64..8 {
        let actual = rv.read(lba, 1).unwrap();
        assert_eq!(actual, vec![0xEE; 4096], "lba {lba} wrong after GC");
    }
}

/// The most recent flush wins: if the same LBA is flushed twice, ReadonlyVolume
/// returns the later value.
#[test]
fn readonly_returns_latest_flushed_value() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    vol.write(0, &[0x11; 4096]).unwrap();
    vol.flush_wal().unwrap();

    vol.write(0, &[0x22; 4096]).unwrap();
    vol.flush_wal().unwrap();

    let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
    let actual = rv.read(0, 1).unwrap();
    assert_eq!(
        actual,
        vec![0x22; 4096],
        "should return latest flushed value"
    );
}

/// ReadonlyVolume on a fork sees ancestor data that has been flushed to
/// pending/.  This exercises the ancestor-layer read path.
#[test]
fn readonly_fork_sees_ancestor_pending() {
    let dir = tempfile::TempDir::new().unwrap();
    let by_id = dir.path();

    let parent_dir: PathBuf = by_id.join(Ulid::new().to_string());
    common::write_test_keypair(&parent_dir);
    let mut parent = Volume::open(&parent_dir, by_id).unwrap();
    parent.write(0, &[0xAAu8; 4096]).unwrap();
    parent.flush_wal().unwrap(); // → pending/
    parent.snapshot().unwrap();
    drop(parent);

    let fork_dir: PathBuf = by_id.join(Ulid::new().to_string());
    fork_volume(&fork_dir, &parent_dir).unwrap();

    let rv = ReadonlyVolume::open(&fork_dir, by_id).unwrap();
    assert_eq!(rv.read(0, 1).unwrap(), vec![0xAAu8; 4096], "ancestor LBA 0");
    assert_eq!(rv.read(1, 1).unwrap(), vec![0u8; 4096], "unwritten LBA 1");
}

/// A fork's own flushed write shadows the ancestor's data for the same LBA.
#[test]
fn readonly_fork_shadows_ancestor() {
    let dir = tempfile::TempDir::new().unwrap();
    let by_id = dir.path();

    // Parent: LBA 0 = 0xAA, LBA 1 = 0xBB.
    let parent_dir: PathBuf = by_id.join(Ulid::new().to_string());
    common::write_test_keypair(&parent_dir);
    let mut parent = Volume::open(&parent_dir, by_id).unwrap();
    parent.write(0, &[0xAAu8; 4096]).unwrap();
    parent.write(1, &[0xBBu8; 4096]).unwrap();
    parent.flush_wal().unwrap();
    parent.snapshot().unwrap();
    drop(parent);

    // Fork: overwrite LBA 0 with 0xCC; leave LBA 1 untouched.
    let fork_dir: PathBuf = by_id.join(Ulid::new().to_string());
    fork_volume(&fork_dir, &parent_dir).unwrap();
    let mut fork_vol = Volume::open(&fork_dir, by_id).unwrap();
    fork_vol.write(0, &[0xCCu8; 4096]).unwrap();
    fork_vol.flush_wal().unwrap();
    drop(fork_vol);

    let rv = ReadonlyVolume::open(&fork_dir, by_id).unwrap();
    assert_eq!(
        rv.read(0, 1).unwrap(),
        vec![0xCCu8; 4096],
        "fork shadows ancestor LBA 0"
    );
    assert_eq!(
        rv.read(1, 1).unwrap(),
        vec![0xBBu8; 4096],
        "unshadowed ancestor LBA 1"
    );
}

/// ReadonlyVolume on a two-level fork chain: grandchild sees grandparent data
/// that neither parent nor child have overwritten.
#[test]
fn readonly_two_level_fork_chain() {
    let dir = tempfile::TempDir::new().unwrap();
    let by_id = dir.path();

    // Grandparent: LBA 0 = 0xAA.
    let gp_dir: PathBuf = by_id.join(Ulid::new().to_string());
    common::write_test_keypair(&gp_dir);
    let mut gp = Volume::open(&gp_dir, by_id).unwrap();
    gp.write(0, &[0xAAu8; 4096]).unwrap();
    gp.flush_wal().unwrap();
    gp.snapshot().unwrap();
    drop(gp);

    // Parent forks from grandparent; writes LBA 1 = 0xBB.
    let parent_dir: PathBuf = by_id.join(Ulid::new().to_string());
    fork_volume(&parent_dir, &gp_dir).unwrap();
    let mut parent = Volume::open(&parent_dir, by_id).unwrap();
    parent.write(1, &[0xBBu8; 4096]).unwrap();
    parent.flush_wal().unwrap();
    parent.snapshot().unwrap();
    drop(parent);

    // Child forks from parent; writes LBA 2 = 0xCC.
    let child_dir: PathBuf = by_id.join(Ulid::new().to_string());
    fork_volume(&child_dir, &parent_dir).unwrap();
    let mut child = Volume::open(&child_dir, by_id).unwrap();
    child.write(2, &[0xCCu8; 4096]).unwrap();
    child.flush_wal().unwrap();
    drop(child);

    let rv = ReadonlyVolume::open(&child_dir, by_id).unwrap();
    assert_eq!(
        rv.read(0, 1).unwrap(),
        vec![0xAAu8; 4096],
        "grandparent LBA 0"
    );
    assert_eq!(rv.read(1, 1).unwrap(), vec![0xBBu8; 4096], "parent LBA 1");
    assert_eq!(rv.read(2, 1).unwrap(), vec![0xCCu8; 4096], "child LBA 2");
    assert_eq!(rv.read(3, 1).unwrap(), vec![0u8; 4096], "unwritten LBA 3");
}
