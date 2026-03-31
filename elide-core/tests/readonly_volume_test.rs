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

use elide_core::volume::{ReadonlyVolume, Volume};
use ulid::Ulid;

mod common;

/// Unwritten LBAs must return all-zero bytes.
#[test]
fn readonly_unwritten_lba_returns_zeros() {
    let dir = tempfile::TempDir::new().unwrap();
    let rv = ReadonlyVolume::open(dir.path()).unwrap();
    let actual = rv.read(0, 1).unwrap();
    assert_eq!(actual, vec![0u8; 4096]);
}

/// Data flushed to pending/ is visible; data only in the WAL is not.
#[test]
fn readonly_sees_flushed_pending_not_wal() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    let mut vol = Volume::open(&fork_dir).unwrap();

    // LBA 0: flushed to pending/ — should be visible.
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();

    // LBA 1: in WAL only, not flushed — should be invisible (returns zeros).
    vol.write(1, &[0xBB; 4096]).unwrap();

    let rv = ReadonlyVolume::open(&fork_dir).unwrap();

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
    let mut vol = Volume::open(&fork_dir).unwrap();

    for lba in 0u64..4 {
        vol.write(lba, &[0xCC; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    let rv = ReadonlyVolume::open(&fork_dir).unwrap();
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
    let mut vol = Volume::open(&fork_dir).unwrap();

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

    let gc_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
    let (_, _, to_delete) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC should compact the two segments");
    vol.apply_gc_handoffs().unwrap();
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    let rv = ReadonlyVolume::open(&fork_dir).unwrap();
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
    let mut vol = Volume::open(&fork_dir).unwrap();

    vol.write(0, &[0x11; 4096]).unwrap();
    vol.flush_wal().unwrap();

    vol.write(0, &[0x22; 4096]).unwrap();
    vol.flush_wal().unwrap();

    let rv = ReadonlyVolume::open(&fork_dir).unwrap();
    let actual = rv.read(0, 1).unwrap();
    assert_eq!(
        actual,
        vec![0x22; 4096],
        "should return latest flushed value"
    );
}
