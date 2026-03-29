// Property-based tests for volume ULID ordering and crash-recovery correctness.
//
// Invariant tested by `ulid_monotonicity`:
//   Every segment created by a volume operation (flush, compact_pending) has a
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
use std::path::{Path, PathBuf};

use elide_core::segment;
use elide_core::volume::Volume;
use proptest::prelude::*;
use ulid::Ulid;

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

/// Simulate one coordinator GC pass on `segments/` without an object store.
///
/// Picks the two lowest-ULID segments as candidates, compacts their entries
/// (including REF entries so the oracle test can still resolve dedup hashes),
/// writes a new segment with ULID = `max(inputs).increment()`, and deletes
/// the inputs.
///
/// Returns `Some((consumed_ulids, produced_ulid))` when candidates were found,
/// `None` when fewer than two segments exist.
fn simulate_coord_gc_local(fork_dir: &Path) -> Option<(Vec<Ulid>, Ulid)> {
    let segments_dir = fork_dir.join("segments");

    let seg_files = segment::collect_segment_files(&segments_dir).ok()?;
    let mut candidates: Vec<(Ulid, PathBuf)> = seg_files
        .iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_str()?;
            let ulid = Ulid::from_string(name).ok()?;
            Some((ulid, p.clone()))
        })
        .collect();
    if candidates.len() < 2 {
        return None;
    }
    candidates.sort_by_key(|(u, _)| *u);
    let candidates = candidates[..2].to_vec();

    let mut all_entries: Vec<segment::SegmentEntry> = Vec::new();
    for (_, path) in &candidates {
        let Ok((bss, mut entries)) = segment::read_segment_index(path) else {
            continue;
        };
        if segment::read_extent_bodies(path, bss, &mut entries).is_err() {
            continue;
        }
        all_entries.extend(entries);
    }

    let max_input = candidates.iter().map(|(u, _)| *u).max()?;
    let new_ulid = max_input
        .increment()
        .unwrap_or_else(|| Ulid::from_parts(max_input.timestamp_ms() + 1, 0));

    if all_entries.is_empty() {
        for (_, path) in &candidates {
            let _ = fs::remove_file(path);
        }
        let consumed = candidates.iter().map(|(u, _)| *u).collect();
        return Some((consumed, new_ulid));
    }

    let tmp_path = segments_dir.join(format!("{new_ulid}.tmp"));
    let final_path = segments_dir.join(new_ulid.to_string());
    if segment::write_segment(&tmp_path, &mut all_entries, None).is_err() {
        return None;
    }
    fs::rename(&tmp_path, &final_path).ok()?;

    for (_, path) in &candidates {
        let _ = fs::remove_file(path);
    }

    let consumed = candidates.iter().map(|(u, _)| *u).collect();
    Some((consumed, new_ulid))
}

/// Move all committed segments from pending/ to segments/.
/// Simulates `drain-pending` (upload + rename) without touching an object store.
fn drain_local(fork_dir: &Path) {
    let pending = fork_dir.join("pending");
    let segments = fork_dir.join("segments");
    if let Ok(entries) = fs::read_dir(&pending) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.ends_with(".tmp") {
                let _ = fs::rename(entry.path(), segments.join(&*name));
            }
        }
    }
}

// --- strategy ---

#[derive(Debug, Clone)]
enum SimOp {
    Write {
        lba: u8,
        seed: u8,
    },
    Flush,
    CompactPending,
    /// Move all committed pending/ segments to segments/, simulating
    /// drain-pending without S3 upload. Required before CoordGcLocal has
    /// material to work with.
    DrainLocal,
    CoordGcLocal,
    Crash,
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        Just(SimOp::Flush),
        Just(SimOp::CompactPending),
        Just(SimOp::DrainLocal),
        Just(SimOp::CoordGcLocal),
        Just(SimOp::Crash),
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
                SimOp::CompactPending => {
                    let _ = vol.compact_pending();
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "compact_pending produced ULID {u} ≤ existing max {max_before}"
                        );
                    }
                }
                SimOp::DrainLocal => {
                    drain_local(fork_dir);
                    // DrainLocal only renames files; no new ULIDs are created.
                }
                SimOp::CoordGcLocal => {
                    if let Some((consumed, produced)) = simulate_coord_gc_local(fork_dir) {
                        let max_consumed = consumed.iter().copied().max().unwrap();
                        prop_assert!(
                            produced > max_consumed,
                            "coord_gc produced ULID {produced} ≤ consumed max {max_consumed}"
                        );
                    }
                }
                SimOp::Crash => {
                    drop(vol);
                    vol = Volume::open(fork_dir).unwrap();
                    // No assertion here: the next Flush or CompactPending
                    // will verify that the mint was correctly reseeded.
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
                SimOp::CompactPending => {
                    let _ = vol.compact_pending();
                }
                SimOp::DrainLocal => {
                    drain_local(fork_dir);
                }
                SimOp::CoordGcLocal => {
                    let _ = simulate_coord_gc_local(fork_dir);
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
            }
        }
    }
}
