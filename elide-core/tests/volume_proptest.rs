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

/// Collect every ULID-named file across wal/, pending/, segments/, and index/*.idx.
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
    // Include index/*.idx stems so ULID monotonicity assertions cover demand-fetched segments.
    if let Ok(entries) = fs::read_dir(fork_dir.join("index")) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if let Some(stem) = name.strip_suffix(".idx") {
                    if let Ok(u) = Ulid::from_string(stem) {
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
    /// Write data to an LBA directly on the volume (no actor channel).
    Write { lba: u8, seed: u8 },
    /// Flush the WAL to a pending/ segment, directly on the volume.
    Flush,
    /// Volume-level sweep: packs small segments from pending/ into a single
    /// new segment. Calls vol.sweep_pending() directly, bypassing the actor
    /// channel. Exercises ULID monotonicity and crash-recovery invariants.
    SweepPending,
    /// Volume-level density pass: rewrites sparse segments from both pending/
    /// and segments/. Analogous to the coordinator's repack pass but runs
    /// in-process on the volume, bypassing the actor channel.
    Repack,
    /// Move all committed pending/ segments to segments/, simulating
    /// drain-pending without S3 upload. Required before CoordGcLocal has
    /// material to work with.
    DrainLocal,
    /// Simulate one coordinator GC sweep pass directly on the filesystem,
    /// using `n` segments as input. Exercises ULID monotonicity and
    /// crash-recovery invariants for the coordinator GC path.
    CoordGcLocal { n: usize },
    /// Simulate coordinator GC running both repack and sweep in the same tick.
    /// Requires ≥ 3 segments in segments/; no-ops otherwise.
    CoordGcLocalBoth,
    /// Simulate a crash: drop the Volume, reopen it (triggering WAL recovery),
    /// and assert all oracle LBAs read back their last-written values.
    Crash,
    /// Read an LBA that has never been written; must return all-zero bytes.
    ReadUnwritten,
    /// Take a snapshot; segments at or below the returned ULID are frozen.
    Snapshot,
    /// Write a single block directly to cache/ in the three-file format
    /// (bypassing the WAL), simulating a demand-fetch result from S3.
    ///
    /// LBAs are offset by 16 (range 16–23) to stay disjoint from Write (0–7)
    /// and ReadUnwritten (64).  The oracle is updated immediately because after
    /// any subsequent Crash the rebuilt volume will serve this data from cache/.
    PopulateFetched { lba: u8, seed: u8 },
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        Just(SimOp::Flush),
        Just(SimOp::SweepPending),
        Just(SimOp::Repack),
        Just(SimOp::DrainLocal),
        (2usize..=5).prop_map(|n| SimOp::CoordGcLocal { n }),
        Just(SimOp::CoordGcLocalBoth),
        Just(SimOp::Crash),
        Just(SimOp::ReadUnwritten),
        Just(SimOp::Snapshot),
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::PopulateFetched { lba, seed }),
    ]
}

fn arb_sim_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop::collection::vec(arb_sim_op(), 1..40)
}

/// Two segments drained to `segments/` — CoordGcLocal has material to compact.
fn two_segment_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0xAA },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 1, seed: 0xBB },
        SimOp::Flush,
        SimOp::DrainLocal,
    ]
}

/// Two segments drained then a Snapshot taken — snapshot floor is non-null
/// from the start, so every subsequent SweepPending/Repack exercises the floor
/// guard rather than hitting the `floor.is_none()` fast path.
fn snapshot_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 2, seed: 0x11 },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 3, seed: 0x22 },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Snapshot,
    ]
}

/// Two flushes left in `pending/` without draining — exercises sweep_pending
/// and repack when data has not yet crossed the drain boundary.
fn pending_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 4, seed: 0x33 },
        SimOp::Flush,
        SimOp::Write { lba: 5, seed: 0x44 },
        SimOp::Flush,
    ]
}

/// Three segments drained to `segments/` — allows CoordGcLocal { n: 3 } to
/// fire so the n=3..=5 range is exercised rather than always no-opping.
fn multi_segment_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x55 },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 1, seed: 0x66 },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 2, seed: 0x77 },
        SimOp::Flush,
        SimOp::DrainLocal,
    ]
}

/// One low-density segment + two small high-density segments — sets up state
/// where both repack and sweep have candidates simultaneously.
///
/// S1 (LBA 0 = 0xAA) is overwritten by S2 (LBA 0 = 0xBB), making S1
/// low-density.  S3 (LBA 1) and S4 (LBA 2) are small and fully live.
/// CoordGcLocalBoth can fire: S1 → repack, S2+S3+S4 → sweep.
fn repack_and_sweep_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0xAA },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 0, seed: 0xBB }, // overwrites LBA 0 — S1 becomes stale
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 1, seed: 0xCC },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 2, seed: 0xDD },
        SimOp::Flush,
        SimOp::DrainLocal,
    ]
}

/// One full GC pass already applied — tests the "second round of GC" path and
/// rebuild from a volume that already has GC history.
fn post_gc_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x88 },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::Write { lba: 1, seed: 0x99 },
        SimOp::Flush,
        SimOp::DrainLocal,
        SimOp::CoordGcLocal { n: 2 },
    ]
}

fn with_prefix(prefix: Vec<SimOp>, ops: Vec<SimOp>) -> Vec<SimOp> {
    let mut v = prefix;
    v.extend(ops);
    v
}

fn arb_gc_interleaved_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop_oneof![
        // No prefix: cold-start and pathological early sequences.
        arb_sim_ops(),
        // Two drained segments: CoordGcLocal has material.
        arb_sim_ops().prop_map(|ops| with_prefix(two_segment_prefix(), ops)),
        // Snapshot in place: floor guard fires from the first SweepPending/Repack.
        arb_sim_ops().prop_map(|ops| with_prefix(snapshot_prefix(), ops)),
        // Pending-only: data in pending/ not yet drained to segments/.
        arb_sim_ops().prop_map(|ops| with_prefix(pending_prefix(), ops)),
        // Three drained segments: CoordGcLocal { n: 3..=5 } can fire.
        arb_sim_ops().prop_map(|ops| with_prefix(multi_segment_prefix(), ops)),
        // Post-GC: one GC pass already applied, tests second-round GC path.
        arb_sim_ops().prop_map(|ops| with_prefix(post_gc_prefix(), ops)),
        // One low-density + two small dense segments: CoordGcLocalBoth can fire.
        arb_sim_ops().prop_map(|ops| with_prefix(repack_and_sweep_prefix(), ops)),
    ]
}

// --- property tests ---

proptest! {
    #[test]
    fn ulid_monotonicity(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
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
                SimOp::CoordGcLocalBoth => {
                    let repack_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let sweep_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let mut to_delete: Vec<std::path::PathBuf> = vec![];
                    if let Some(((r_consumed, r_produced, r_paths), (s_consumed, s_produced, s_paths))) =
                        common::simulate_coord_gc_both_local(fork_dir, repack_ulid, sweep_ulid)
                    {
                        let r_max = r_consumed.iter().copied().max().unwrap();
                        prop_assert!(
                            r_produced > r_max,
                            "coord_gc_both repack produced {r_produced} ≤ consumed max {r_max}"
                        );
                        let s_max = s_consumed.iter().copied().max().unwrap();
                        prop_assert!(
                            s_produced > s_max,
                            "coord_gc_both sweep produced {s_produced} ≤ consumed max {s_max}"
                        );
                        to_delete.extend(r_paths);
                        to_delete.extend(s_paths);
                    }
                    let _ = vol.apply_gc_handoffs();
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
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
                SimOp::PopulateFetched { lba, seed } => {
                    // gc_checkpoint flushes the WAL (may create a pending segment) then
                    // mints a fresh ULID for the cache file.  Both must be > max_before.
                    let ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    common::populate_cache(fork_dir, ulid, 16 + *lba as u64, *seed);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "PopulateFetched produced out-of-order ULID: {u} <= {max_before}"
                        );
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
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
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
                SimOp::CoordGcLocalBoth => {
                    let repack_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let sweep_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let mut to_delete: Vec<std::path::PathBuf> = vec![];
                    if let Some(((_, _, r_paths), (_, _, s_paths))) =
                        common::simulate_coord_gc_both_local(fork_dir, repack_ulid, sweep_ulid)
                    {
                        to_delete.extend(r_paths);
                        to_delete.extend(s_paths);
                    }
                    let _ = vol.apply_gc_handoffs();
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
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
                SimOp::PopulateFetched { lba, seed } => {
                    // Write directly to cache/ (simulates a demand-fetch from S3).
                    // LBA is offset by 16 so it never overlaps with Write (0–7).
                    // The latest PopulateFetched for an LBA wins on rebuild (highest
                    // ULID applied last), so always overwrite the oracle entry.
                    let ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let actual_lba = 16 + *lba as u64;
                    common::populate_cache(fork_dir, ulid, actual_lba, *seed);
                    oracle.insert(actual_lba, [*seed; 4096]);
                }
            }
        }
    }

    /// GC-interleaved crash-recovery test.
    ///
    /// Identical oracle logic to `crash_recovery_oracle` but always starts with
    /// two segments already drained to `segments/`, ensuring `CoordGcLocal` has
    /// material to compact on most sequences rather than silently no-opping.
    #[test]
    fn gc_interleaved_oracle(ops in arb_gc_interleaved_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();
        common::write_test_keypair(fork_dir);
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
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
                    let _ = vol.apply_gc_handoffs();
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                }
                SimOp::CoordGcLocalBoth => {
                    let repack_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let sweep_ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let mut to_delete: Vec<std::path::PathBuf> = vec![];
                    if let Some(((_, _, r_paths), (_, _, s_paths))) =
                        common::simulate_coord_gc_both_local(fork_dir, repack_ulid, sweep_ulid)
                    {
                        to_delete.extend(r_paths);
                        to_delete.extend(s_paths);
                    }
                    let _ = vol.apply_gc_handoffs();
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
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
                    let actual = vol.read(64, 1).unwrap();
                    prop_assert_eq!(
                        actual.as_slice(),
                        [0u8; 4096].as_slice(),
                        "unwritten lba 64 returned non-zero data"
                    );
                }
                SimOp::Snapshot => {
                    let _ = vol.snapshot();
                }
                SimOp::PopulateFetched { lba, seed } => {
                    let ulid = Ulid::from_string(&vol.gc_checkpoint().unwrap()).unwrap();
                    let actual_lba = 16 + *lba as u64;
                    common::populate_cache(fork_dir, ulid, actual_lba, *seed);
                    oracle.insert(actual_lba, [*seed; 4096]);
                }
            }
        }
    }
}
