// Integration tests: GC correctness with interleaved live writes.
//
// Two scenarios that the random proptest rarely hits because CoordGcLocal
// silently no-ops when segments/ is empty:
//
// 1. Live write overwrites a GC candidate LBA *before* the GC pass runs.
//    `simulate_coord_gc_local` rebuilds the LBA map from all current segments,
//    so it filters out stale entries — the GC output will not contain the old
//    value for any LBA that was overwritten after gc_checkpoint().
//
// 2. Live write overwrites a GC candidate LBA *after* the GC pass runs (but
//    before the handoff is applied).  The GC output now contains the old value.
//    `sort_for_rebuild` gives GC-output segments lower priority than regular
//    segments, so the live write's pending segment wins on conflict during
//    rebuild.

use std::path::PathBuf;

use elide_core::volume::Volume;
use ulid::Ulid;

mod common;

/// Overwrite a GC candidate LBA before the GC pass: the GC output must not
/// contain the stale pre-overwrite value.
#[test]
fn gc_filters_stale_entries_when_lba_overwritten_before_gc() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Seed batch 1 — LBAs 0-3 = 0xAA — drain to segments/.
    for lba in 0u64..4 {
        vol.write(lba, &[0xAA; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    // Seed batch 2 — LBAs 4-7 = 0xBB — drain to segments/.
    for lba in 4u64..8 {
        vol.write(lba, &[0xBB; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    // Take GC checkpoint (flushes WAL, advances mint).
    let (gc_ulid_str, _) = vol.gc_checkpoint().unwrap();
    let gc_ulid = Ulid::from_string(&gc_ulid_str).unwrap();

    // Live overwrite of LBAs 0-3 *before* GC runs.
    for lba in 0u64..4 {
        vol.write(lba, &[0xCC; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    // Keep in pending/ — do not drain.

    // GC pass: LBA map includes the 0xCC overwrite, so 0xAA entries are stale
    // and must be filtered out of the GC output.
    let (_, _, to_delete) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC should compact the two seeded segments");
    vol.apply_gc_handoffs().unwrap();
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    // Live overwrite must win.
    let expected: &[(u64, u8)] = &[
        (0, 0xCC),
        (1, 0xCC),
        (2, 0xCC),
        (3, 0xCC),
        (4, 0xBB),
        (5, 0xBB),
        (6, 0xBB),
        (7, 0xBB),
    ];
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(actual, vec![byte; 4096], "lba {lba} wrong before crash");
    }

    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual,
            vec![byte; 4096],
            "lba {lba} wrong after crash+rebuild"
        );
    }
    drop(vol);
}

/// Overwrite a GC candidate LBA *after* the GC pass runs: sort_for_rebuild
/// must give the GC output lower priority than the live pending segment.
#[test]
fn gc_output_loses_to_live_write_applied_after_gc() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Seed batch 1 — LBAs 0-3 = 0xAA.
    for lba in 0u64..4 {
        vol.write(lba, &[0xAA; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    // Seed batch 2 — LBAs 4-7 = 0xBB.
    for lba in 4u64..8 {
        vol.write(lba, &[0xBB; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    common::drain_local(&fork_dir);

    // GC checkpoint then GC pass — no overwrites yet so GC output contains
    // 0xAA for LBAs 0-3 and 0xBB for LBAs 4-7.
    let (gc_ulid_str, _) = vol.gc_checkpoint().unwrap();
    let gc_ulid = Ulid::from_string(&gc_ulid_str).unwrap();
    let (_, _, to_delete) = common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC should compact the two seeded segments");

    // Live overwrite of LBAs 0-3 *after* GC ran but before handoff is applied.
    for lba in 0u64..4 {
        vol.write(lba, &[0xCC; 4096]).unwrap();
    }
    vol.flush_wal().unwrap();
    // Keep in pending/ — do not drain.

    // Apply handoff and delete old segments.  The GC output (containing stale
    // 0xAA for LBAs 0-3) and the live pending segment (containing 0xCC) both
    // exist; sort_for_rebuild must prefer the pending segment.
    vol.apply_gc_handoffs().unwrap();
    for path in &to_delete {
        let _ = std::fs::remove_file(path);
    }

    let expected: &[(u64, u8)] = &[
        (0, 0xCC),
        (1, 0xCC),
        (2, 0xCC),
        (3, 0xCC),
        (4, 0xBB),
        (5, 0xBB),
        (6, 0xBB),
        (7, 0xBB),
    ];
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(actual, vec![byte; 4096], "lba {lba} wrong before crash");
    }

    // Crash + rebuild: rebuild must still prefer the pending segment over the
    // GC output for LBAs 0-3.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    for &(lba, byte) in expected {
        let actual = vol.read(lba, 1).unwrap();
        assert_eq!(
            actual,
            vec![byte; 4096],
            "lba {lba} wrong after crash+rebuild"
        );
    }
    drop(vol);
}
