// Property-based tests for volume ULID ordering and crash-recovery correctness.
//
// Invariant tested by `ulid_monotonicity`:
//   Every segment created by a volume operation (flush, sweep_pending) has a
//   ULID strictly greater than all segment ULIDs that existed before the
//   operation. For simulated coordinator GC the invariant is narrower: the
//   output ULID exceeds the maximum of the consumed input ULIDs.
//
// Invariant tested by `crash_recovery_oracle`:
//   After any sequence of volume operations + coordinator GC + crash, every
//   LBA reads back exactly the value last written to it. This directly tests
//   that no combination of operations can produce a stale read after rebuild.
//
// Together these two properties guarantee that rebuild always applies segments
// in write order and that crash recovery is always correct.

use std::fs;
use std::path::Path;

use elide_core::volume::Volume;
use proptest::prelude::*;
use ulid::Ulid;

mod common;

// --- simulation helpers ---

/// Collect every ULID-named file across wal/, pending/, and segments/.
fn all_segment_ulids(fork_dir: &Path) -> std::collections::BTreeSet<Ulid> {
    let mut result = std::collections::BTreeSet::new();
    for subdir in ["wal", "pending", "segments"] {
        let dir = fork_dir.join(subdir);
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Ok(u) = Ulid::from_string(name) {
                        result.insert(u);
                    }
                }
            }
        }
    }
    result
}

// --- strategy ---

#[derive(Debug, Clone)]
enum SimOp {
    Write {
        lba: u8,
        seed: u8,
    },
    Flush,
    SweepPending,
    /// Volume-level density pass: rewrites sparse segments from both pending/
    /// and segments/. Analogous to the coordinator's repack pass but runs
    /// in-process on the volume.
    Repack,
    /// Move all committed pending/ segments to segments/, simulating
    /// drain-pending without S3 upload. Required before CoordGcLocal has
    /// material to work with.
    DrainLocal,
    CoordGcLocal {
        n: usize,
    },
    Crash,
    /// Read an LBA that has never been written; must return all-zero bytes.
    ReadUnwritten,
    /// Take a snapshot; segments at or below the returned ULID are frozen.
    Snapshot,
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        Just(SimOp::Flush),
        Just(SimOp::SweepPending),
        Just(SimOp::Repack),
        Just(SimOp::DrainLocal),
        (2usize..=5).prop_map(|n| SimOp::CoordGcLocal { n }),
        Just(SimOp::Crash),
        Just(SimOp::ReadUnwritten),
        Just(SimOp::Snapshot),
    ]
}

fn arb_sim_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop::collection::vec(arb_sim_op(), 1..40)
}

// --- property tests ---

proptest! {
    #[test]
    fn ulid_monotonicity(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        let mut vol = Volume::open(fork_dir).unwrap();
        // Tracks the latest snapshot ULID; segments at or below this are frozen.
        let mut snapshot_floor: Option<Ulid> = None;

        for op in &ops {
            let ulids_before = all_segment_ulids(fork_dir);
            let max_before = ulids_before
                .iter()
                .copied()
                .max()
                .unwrap_or(Ulid::from_parts(0, 0));

            match op {
                SimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "flush produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                }
                SimOp::SweepPending => {
                    let frozen_before: std::collections::BTreeSet<Ulid> =
                        if let Some(floor) = snapshot_floor {
                            ulids_before.iter().copied().filter(|u| *u <= floor).collect()
                        } else {
                            Default::default()
                        };
                    let _ = vol.sweep_pending();
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "sweep_pending produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                    for u in &frozen_before {
                        prop_assert!(
                            after.contains(u),
                            "sweep_pending deleted frozen segment {u} (floor {:?})",
                            snapshot_floor
                        );
                    }
                }
                SimOp::Repack => {
                    let frozen_before: std::collections::BTreeSet<Ulid> =
                        if let Some(floor) = snapshot_floor {
                            ulids_before.iter().copied().filter(|u| *u <= floor).collect()
                        } else {
                            Default::default()
                        };
                    // Use a high threshold so any segment with dead bytes is a
                    // candidate, maximising the chance of actually repacking.
                    let _ = vol.repack(0.9);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "compact produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                    for u in &frozen_before {
                        prop_assert!(
                            after.contains(u),
                            "repack deleted frozen segment {u} (floor {:?})",
                            snapshot_floor
                        );
                    }
                }
                SimOp::DrainLocal => {
                    common::drain_local(fork_dir);
                    // DrainLocal only renames files; no new ULIDs are created.
                }
                SimOp::CoordGcLocal { n } => {
                    let gc_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let to_delete = if let Some((consumed, produced, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
                        let max_consumed = consumed.iter().copied().max().unwrap();
                        prop_assert!(
                            produced > max_consumed,
                            "coord_gc produced ULID {produced} ≤ consumed max {max_consumed}"
                        );
                        paths
                    } else {
                        vec![]
                    };
                    // Apply any pending gc handoffs (from this pass or a
                    // pre-crash pass) through the volume's handoff path.
                    let _ = vol.apply_gc_handoffs();
                    // Delete old segment files only after the handoff is applied.
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir).unwrap();
                    // No assertion here: the next Flush or SweepPending
                    // will verify that the mint was correctly reseeded.
                }
                SimOp::ReadUnwritten => {
                    // LBA 64 is outside the Write range (0..8) so it is
                    // always unwritten; the volume must return all zeros.
                    let actual = vol.read(64, 1).unwrap();
                    prop_assert_eq!(
                        actual.as_slice(),
                        [0u8; 4096].as_slice(),
                        "unwritten lba 64 returned non-zero data"
                    );
                }
                SimOp::Snapshot => {
                    if let Ok(u) = vol.snapshot() {
                        snapshot_floor = Some(u);
                    }
                }
            }
        }
    }

    /// Oracle-based crash recovery test.
    ///
    /// Maintains a ground-truth map of the most recent data written to each
    /// LBA. After every Crash + reopen, asserts that every LBA reads back
    /// exactly the value last written to it. This directly tests that no
    /// sequence of volume operations + GC + crash can produce a stale read.
    #[test]
    fn crash_recovery_oracle(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        let mut vol = Volume::open(fork_dir).unwrap();
        let mut oracle: std::collections::HashMap<u64, [u8; 4096]> =
            std::collections::HashMap::new();

        for op in &ops {
            match op {
                SimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                    oracle.insert(*lba as u64, data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                }
                SimOp::SweepPending => {
                    let _ = vol.sweep_pending();
                }
                SimOp::Repack => {
                    let _ = vol.repack(0.9);
                }
                SimOp::DrainLocal => {
                    common::drain_local(fork_dir);
                }
                SimOp::CoordGcLocal { n } => {
                    let gc_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let to_delete = if let Some((_, _, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
                        paths
                    } else {
                        vec![]
                    };
                    // Apply handoffs before deleting consumed segments — mirrors
                    // the real coordinator protocol (volume acknowledges first,
                    // then coordinator deletes).
                    let _ = vol.apply_gc_handoffs();
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir).unwrap();
                    for (&lba, expected) in &oracle {
                        let actual = vol.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "lba {} wrong after crash+rebuild",
                            lba
                        );
                    }
                }
                SimOp::ReadUnwritten => {
                    // LBA 64 is outside the Write range (0..8) so it is
                    // always unwritten; the volume must return all zeros.
                    let actual = vol.read(64, 1).unwrap();
                    prop_assert_eq!(
                        actual.as_slice(),
                        [0u8; 4096].as_slice(),
                        "unwritten lba 64 returned non-zero data"
                    );
                }
                SimOp::Snapshot => {
                    // Snapshot does not change readable data; no oracle update.
                    let _ = vol.snapshot();
                }
            }
        }
    }
}
