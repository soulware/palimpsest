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

/// Collect every ULID-named file across wal/, pending/, and index/*.idx.
fn all_segment_ulids(fork_dir: &Path) -> std::collections::BTreeSet<Ulid> {
    let mut result = std::collections::BTreeSet::new();
    for subdir in ["wal", "pending"] {
        let dir = fork_dir.join(subdir);
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && let Ok(u) = Ulid::from_string(name)
                {
                    result.insert(u);
                }
            }
        }
    }
    // Include index/*.idx stems so ULID monotonicity assertions cover demand-fetched segments.
    if let Ok(entries) = fs::read_dir(fork_dir.join("index")) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str()
                && let Some(stem) = name.strip_suffix(".idx")
                && let Ok(u) = Ulid::from_string(stem)
            {
                result.insert(u);
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
    /// Volume-level density pass: rewrites sparse segments from pending/.
    /// Analogous to the coordinator's repack pass but runs in-process on the
    /// volume, bypassing the actor channel.
    Repack,
    /// Exercises the full materialise → promote path:
    /// materialise_segment (produces .materialized sidecar) then promote_segment
    /// (reads from .materialized, updates extent index, publishes snapshot).
    DrainWithMaterialise,
    /// Simulate one coordinator GC sweep pass directly on the filesystem,
    /// using `n` segments as input. Exercises ULID monotonicity and
    /// crash-recovery invariants for the coordinator GC path.
    CoordGcLocal { n: usize },
    /// Simulate coordinator GC running both repack and sweep in the same tick.
    /// Requires ≥ 3 segments in index/; no-ops otherwise.
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
    /// Write [seed; 4096] to lba_a, then [seed; 4096] to lba_b.
    /// Because both writes carry identical data, lba_b's WAL entry is a
    /// DEDUP_REF pointing at lba_a's segment.  Guarantees the dedup and
    /// dead-REF paths are exercised on every run that includes this op.
    DedupWrite { lba_a: u8, lba_b: u8, seed: u8 },
    /// Zero a single LBA.
    ///
    /// LBAs are in range 8..16 to stay disjoint from Write (0..8),
    /// PopulateFetched (16..23), and ReadUnwritten (64). The oracle is
    /// updated immediately with zeros; a subsequent Crash verifies the
    /// zero survives WAL recovery and rebuild.
    WriteZeroes { lba: u8 },
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        // Write and DedupWrite use seeds 0..=127 (bit 7 clear).
        // PopulateFetched effective seeds are always 128..=255 (bit 7 set).
        // This partitions the hash space so that populate_cache data can never
        // collide with data written through the volume's write path, mirroring
        // the production invariant that the fetcher only caches data that was
        // previously written through the volume (and thus already indexed).
        (0u8..8, 0u8..128u8).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        Just(SimOp::Flush),
        Just(SimOp::SweepPending),
        Just(SimOp::Repack),
        Just(SimOp::DrainWithMaterialise),
        (2usize..=5).prop_map(|n| SimOp::CoordGcLocal { n }),
        Just(SimOp::CoordGcLocalBoth),
        Just(SimOp::Crash),
        Just(SimOp::ReadUnwritten),
        Just(SimOp::Snapshot),
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::PopulateFetched { lba, seed }),
        (0u8..4, 4u8..8, 0u8..128u8).prop_map(|(lba_a, lba_b, seed)| SimOp::DedupWrite {
            lba_a,
            lba_b,
            seed
        }),
        (8u8..16).prop_map(|lba| SimOp::WriteZeroes { lba }),
    ]
}

fn arb_sim_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop::collection::vec(arb_sim_op(), 1..40)
}

/// Two segments drained to `index/` — CoordGcLocal has material to compact.
fn two_segment_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x0A },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 1, seed: 0x0B },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
    ]
}

/// Two segments drained then a Snapshot taken — snapshot floor is non-null
/// from the start, so every subsequent SweepPending/Repack exercises the floor
/// guard rather than hitting the `floor.is_none()` fast path.
fn snapshot_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 2, seed: 0x11 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 3, seed: 0x22 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
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

/// Dedup write drained via materialise → promote, then overwritten so GC
/// has dead entries. Exercises the full materialise + GC path with thin refs.
fn dedup_materialise_gc_prefix() -> Vec<SimOp> {
    vec![
        // Write canonical data and flush.
        SimOp::Write { lba: 0, seed: 0xD0 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        // Write same data to different LBA — creates thin DedupRef.
        SimOp::DedupWrite {
            lba_a: 1,
            lba_b: 2,
            seed: 0xD0,
        },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        // Overwrite LBA 0 to make the first segment's data dead for GC.
        SimOp::Write { lba: 0, seed: 0xD1 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
    ]
}

/// Three segments drained to `index/` — allows CoordGcLocal { n: 3 } to
/// fire so the n=3..=5 range is exercised rather than always no-opping.
fn multi_segment_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x55 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 1, seed: 0x66 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 2, seed: 0x77 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
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
        SimOp::Write { lba: 0, seed: 0x14 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 0, seed: 0x15 }, // overwrites LBA 0 — S1 becomes stale
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 1, seed: 0x16 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 2, seed: 0x17 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
    ]
}

/// One full GC pass already applied — tests the "second round of GC" path and
/// rebuild from a volume that already has GC history.
fn post_gc_prefix() -> Vec<SimOp> {
    vec![
        SimOp::Write { lba: 0, seed: 0x28 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
        SimOp::Write { lba: 1, seed: 0x29 },
        SimOp::Flush,
        SimOp::DrainWithMaterialise,
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
        // Pending-only: data in pending/ not yet drained to index/ + cache/.
        arb_sim_ops().prop_map(|ops| with_prefix(pending_prefix(), ops)),
        // Three drained segments: CoordGcLocal { n: 3..=5 } can fire.
        arb_sim_ops().prop_map(|ops| with_prefix(multi_segment_prefix(), ops)),
        // Post-GC: one GC pass already applied, tests second-round GC path.
        arb_sim_ops().prop_map(|ops| with_prefix(post_gc_prefix(), ops)),
        // One low-density + two small dense segments: CoordGcLocalBoth can fire.
        arb_sim_ops().prop_map(|ops| with_prefix(repack_and_sweep_prefix(), ops)),
        // Dedup + materialise + GC: exercises thin→fat materialisation before GC.
        arb_sim_ops().prop_map(|ops| with_prefix(dedup_materialise_gc_prefix(), ops)),
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
                SimOp::DrainWithMaterialise => {
                    common::drain_with_materialise(&mut vol);
                }
                SimOp::CoordGcLocal { n } => {
                    let (gc_ulid, gc_ulid2) = vol.gc_checkpoint().unwrap();
                    // Core invariant: the volume mint must have advanced past both
                    // GC ULIDs, so the next WAL flush produces a segment that sorts
                    // above them.  This is the property the pre-fix bug violated —
                    // the actor used Ulid::new() (system clock) instead of the
                    // volume's mint, so WAL segments could sort below GC outputs.
                    //
                    // Diff against post-checkpoint state (not ulids_before) so that
                    // W_new — the WAL opened inside gc_checkpoint with ULID between
                    // W_old and u1 — is excluded.  Only the WAL opened by *this*
                    // flush_wal (W_new2, which must be > u2) appears in the diff.
                    let ulids_after_checkpoint = all_segment_ulids(fork_dir);
                    vol.flush_wal().unwrap();
                    let after_flush = all_segment_ulids(fork_dir);
                    for u in after_flush.difference(&ulids_after_checkpoint) {
                        prop_assert!(
                            *u > gc_ulid2,
                            "WAL segment after gc_checkpoint sorts below GC output: {u} ≤ {gc_ulid2}"
                        );
                    }
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
                    // Only delete consumed segment files when the handoff was
                    // actually applied (returned > 0).  If Bug B cancelled the
                    // GC, the consumed files must survive for the next tick.
                    let applied = vol.apply_gc_handoffs().unwrap_or(0);
                    if applied > 0 {
                        for path in &to_delete {
                            let _ = std::fs::remove_file(path);
                        }
                    }
                }
                SimOp::CoordGcLocalBoth => {
                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();
                    // Same mint-advancement invariant as CoordGcLocal — diff against
                    // post-checkpoint state so only W_new2 (> sweep_ulid) appears.
                    let ulids_after_checkpoint = all_segment_ulids(fork_dir);
                    vol.flush_wal().unwrap();
                    let after_flush = all_segment_ulids(fork_dir);
                    for u in after_flush.difference(&ulids_after_checkpoint) {
                        prop_assert!(
                            *u > sweep_ulid,
                            "WAL segment after gc_checkpoint sorts below GC output: {u} ≤ {sweep_ulid}"
                        );
                    }
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
                        // Delete each pass's input paths only if that specific
                        // pass was applied.  Bug B may cancel one pass while the
                        // other succeeds; checking applied > 0 on the combined
                        // count would incorrectly delete the cancelled pass's
                        // inputs, breaking subsequent reads and crash+rebuild.
                        let gc_dir = fork_dir.join("gc");
                        let _ = vol.apply_gc_handoffs().unwrap_or(0);
                        if gc_dir.join(format!("{}.applied", r_produced)).exists() {
                            for path in &r_paths { let _ = std::fs::remove_file(path); }
                        }
                        if gc_dir.join(format!("{}.applied", s_produced)).exists() {
                            for path in &s_paths { let _ = std::fs::remove_file(path); }
                        }
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
                    // mints two fresh ULIDs; we use the first for the cache file.  All
                    // new ULIDs must be > max_before.
                    //
                    // effective_seed always has bit 7 set (128..=255) so it never
                    // collides with Write/DedupWrite seeds (0..=127, bit 7 clear).
                    // Bits 6-4 encode lba (0..7), bits 3-0 vary within the lba slot.
                    let effective_seed = 0x80u8 | ((*lba & 0x07) << 4) | (*seed & 0x0F);
                    let (ulid, _) = vol.gc_checkpoint().unwrap();
                    common::populate_cache(fork_dir, ulid, 16 + *lba as u64, effective_seed);
                    let after = all_segment_ulids(fork_dir);
                    for u in after.difference(&ulids_before) {
                        prop_assert!(
                            *u > max_before,
                            "PopulateFetched produced out-of-order ULID: {u} <= {max_before}"
                        );
                    }
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                }
                SimOp::WriteZeroes { lba } => {
                    // write_zeroes appends to the WAL in-place; no new ULID file
                    // is created, so no ULID ordering assertion is needed here.
                    let _ = vol.write_zeroes(*lba as u64, 1);
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
                SimOp::DrainWithMaterialise => {
                    common::drain_with_materialise(&mut vol);
                }
                SimOp::CoordGcLocal { n } => {
                    let (gc_ulid, _) = vol.gc_checkpoint().unwrap();
                    let to_delete = if let Some((_, _, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
                        paths
                    } else {
                        vec![]
                    };
                    // Apply handoffs before deleting consumed segments — mirrors
                    // the real coordinator protocol (volume acknowledges first,
                    // then coordinator deletes).  Only delete when the handoff was
                    // applied (returned > 0); Bug B cancellation leaves consumed
                    // files in place for the next GC tick.
                    let applied = vol.apply_gc_handoffs().unwrap_or(0);
                    if applied > 0 {
                        for path in &to_delete {
                            let _ = std::fs::remove_file(path);
                        }
                    }
                }
                SimOp::CoordGcLocalBoth => {
                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();
                    let gc_result =
                        common::simulate_coord_gc_both_local(fork_dir, repack_ulid, sweep_ulid);
                    let _ = vol.apply_gc_handoffs().unwrap_or(0);
                    let gc_dir = fork_dir.join("gc");
                    if let Some(((_, r_produced, r_paths), (_, s_produced, s_paths))) = gc_result {
                        if gc_dir.join(format!("{r_produced}.applied")).exists() {
                            for path in &r_paths { let _ = std::fs::remove_file(path); }
                        }
                        if gc_dir.join(format!("{s_produced}.applied")).exists() {
                            for path in &s_paths { let _ = std::fs::remove_file(path); }
                        }
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
                    // Simulate a demand-fetch from S3: write one segment directly to
                    // index/ + cache/, bypassing the WAL.
                    //
                    // In production, index/*.idx is written by the coordinator once
                    // after S3 upload and is never duplicated.  A re-fetch of an
                    // evicted segment only rewrites the cache/ body, not index/.
                    // Creating two index entries for the same LBA is therefore an
                    // invalid production state — skip if this LBA is already populated.
                    let actual_lba = 16 + *lba as u64;
                    if oracle.contains_key(&actual_lba) {
                        continue;
                    }
                    // effective_seed always has bit 7 set (128..=255) so it never
                    // collides with Write/DedupWrite seeds (0..=127, bit 7 clear).
                    // Bits 6-4 encode lba (0..7), bits 3-0 vary within the lba slot.
                    let effective_seed = 0x80u8 | ((*lba & 0x07) << 4) | (*seed & 0x0F);
                    let (ulid, _) = vol.gc_checkpoint().unwrap();
                    common::populate_cache(fork_dir, ulid, actual_lba, effective_seed);
                    oracle.insert(actual_lba, [effective_seed; 4096]);
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                    oracle.insert(*lba_a as u64, data);
                    oracle.insert(*lba_b as u64, data);
                }
                SimOp::WriteZeroes { lba } => {
                    let _ = vol.write_zeroes(*lba as u64, 1);
                    // Oracle records zeros; a subsequent Crash asserts they survive rebuild.
                    oracle.insert(*lba as u64, [0u8; 4096]);
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
                SimOp::DrainWithMaterialise => {
                    common::drain_with_materialise(&mut vol);
                }
                SimOp::CoordGcLocal { n } => {
                    let (gc_ulid, _) = vol.gc_checkpoint().unwrap();
                    let to_delete = if let Some((_, _, paths)) =
                        common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                    {
                        paths
                    } else {
                        vec![]
                    };
                    let applied = vol.apply_gc_handoffs().unwrap_or(0);
                    if applied > 0 {
                        for path in &to_delete {
                            let _ = std::fs::remove_file(path);
                        }
                    }
                }
                SimOp::CoordGcLocalBoth => {
                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();
                    let gc_result =
                        common::simulate_coord_gc_both_local(fork_dir, repack_ulid, sweep_ulid);
                    let _ = vol.apply_gc_handoffs().unwrap_or(0);
                    let gc_dir = fork_dir.join("gc");
                    if let Some(((_, r_produced, r_paths), (_, s_produced, s_paths))) = gc_result {
                        if gc_dir.join(format!("{r_produced}.applied")).exists() {
                            for path in &r_paths { let _ = std::fs::remove_file(path); }
                        }
                        if gc_dir.join(format!("{s_produced}.applied")).exists() {
                            for path in &s_paths { let _ = std::fs::remove_file(path); }
                        }
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
                    // Skip if already populated — see crash_recovery_oracle for rationale.
                    let actual_lba = 16 + *lba as u64;
                    if oracle.contains_key(&actual_lba) {
                        continue;
                    }
                    // effective_seed always has bit 7 set (128..=255) so it never
                    // collides with Write/DedupWrite seeds (0..=127, bit 7 clear).
                    let effective_seed = 0x80u8 | ((*lba & 0x07) << 4) | (*seed & 0x0F);
                    let (ulid, _) = vol.gc_checkpoint().unwrap();
                    common::populate_cache(fork_dir, ulid, actual_lba, effective_seed);
                    oracle.insert(actual_lba, [effective_seed; 4096]);
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                    oracle.insert(*lba_a as u64, data);
                    oracle.insert(*lba_b as u64, data);
                }
                SimOp::WriteZeroes { lba } => {
                    let _ = vol.write_zeroes(*lba as u64, 1);
                    oracle.insert(*lba as u64, [0u8; 4096]);
                }
            }
        }
    }
}
