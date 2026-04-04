// Property-based tests for the coordinator GC path.
//
// Invariant: after any sequence of writes, flushes, and GC sweeps, every
// LBA reads back the value last written to it.
//
// Unlike the proptest suite in elide-core (which uses a hand-rolled GC
// simulation in tests/common/mod.rs), this suite calls the real coordinator
// code: gc_fork() → vol.apply_gc_handoffs() → apply_done_handoffs().
// That closes the structural gap where the simulation could be correct while
// the production implementation had a bug.
//
// GcSweep runs the full coordinator round-trip:
//   1. gc_fork()            — real compact_segments / collect_stats
//   2. apply_gc_handoffs()  — volume re-signs and updates extent index
//   3. apply_done_handoffs() — uploads to InMemory store, deletes old files
// Then asserts all oracle LBAs are readable with correct data.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use elide_coordinator::config::GcConfig;
use elide_coordinator::gc::{GcStrategy, apply_done_handoffs, gc_fork};
use elide_core::volume::Volume;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum SimOp {
    /// Write [seed; 4096] to lba.
    Write { lba: u8, seed: u8 },
    /// Write [seed; 4096] to lba_a then lba_b (disjoint ranges 0..4 / 4..8).
    /// Because the data is identical, lba_b's WAL entry is a DEDUP_REF.
    DedupWrite { lba_a: u8, lba_b: u8, seed: u8 },
    /// Flush the WAL to a pending/ segment.
    Flush,
    /// Move all pending/ segments to segments/ (simulates S3 drain).
    DrainLocal,
    /// Run the full real coordinator GC round-trip, then assert the oracle.
    GcSweep,
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        (0u8..4, 4u8..8, any::<u8>()).prop_map(|(lba_a, lba_b, seed)| SimOp::DedupWrite {
            lba_a,
            lba_b,
            seed
        }),
        Just(SimOp::Flush),
        Just(SimOp::DrainLocal),
        Just(SimOp::GcSweep),
    ]
}

fn arb_sim_ops() -> impl Strategy<Value = Vec<SimOp>> {
    prop::collection::vec(arb_sim_op(), 1..30)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// Segment cleanup: after GC runs, every consumed input segment must be
    /// deleted from segments/.
    ///
    /// With test config (density_threshold=0.0, small_segment_bytes=MAX), the
    /// sweep pass always compacts ALL segments into one when ≥2 exist.  After
    /// apply_done_handoffs, segments/ must contain ≤1 file.
    ///
    /// Catches Bug A: DEDUP_REF-only segments were never deleted because
    /// compact_segments emitted no handoff line for them.
    #[test]
    fn gc_segment_cleanup(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();

        elide_core::signing::generate_keypair(
            fork_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

        fs::create_dir_all(fork_dir.join("segments")).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let gc_config = GcConfig {
            density_threshold: 0.0,
            small_segment_bytes: u64::MAX,
            interval_secs: 0,
        };

        let segments_dir = fork_dir.join("segments");

        for op in &ops {
            match op {
                SimOp::Write { lba, seed } => {
                    let _ = vol.write(*lba as u64, &[*seed; 4096]);
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                }
                SimOp::DrainLocal => {
                    let pending_dir = fork_dir.join("pending");
                    if let Ok(entries) = fs::read_dir(&pending_dir) {
                        for entry in entries.flatten() {
                            let _ = fs::rename(
                                entry.path(),
                                segments_dir.join(entry.file_name()),
                            );
                        }
                    }
                }
                SimOp::GcSweep => {
                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

                    let segs_before: usize = fs::read_dir(&segments_dir)
                        .map(|d| d.flatten().count())
                        .unwrap_or(0);

                    let gc_stats = rt.block_on(gc_fork(
                        fork_dir,
                        "test-vol",
                        &store,
                        &gc_config,
                        &repack_ulid,
                        &sweep_ulid,
                    ));
                    let _ = vol.apply_gc_handoffs();
                    let _ = rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store));

                    if let Ok(stats) = gc_stats {
                        if stats.strategy != GcStrategy::None {
                            let segs_after: usize = fs::read_dir(&segments_dir)
                                .map(|d| d.flatten().count())
                                .unwrap_or(0);
                            prop_assert!(
                                segs_after <= 1,
                                "after GcSweep on {} segments, {} remain in segments/ (expected ≤1)",
                                segs_before,
                                segs_after
                            );
                        }
                    }
                }
            }
        }
    }

    /// GC oracle: after any sequence of writes + GC sweeps, every LBA that
    /// has ever been written reads back its last-written value.
    #[test]
    fn gc_oracle(ops in arb_sim_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();

        elide_core::signing::generate_keypair(
            fork_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
        let mut oracle: HashMap<u64, [u8; 4096]> = HashMap::new();

        // segments/ must exist before gc_fork tries to read it.
        fs::create_dir_all(fork_dir.join("segments")).unwrap();

        // Single runtime reused across all GcSweep ops in this sequence.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let gc_config = GcConfig {
            density_threshold: 0.0,
            small_segment_bytes: u64::MAX,
            interval_secs: 0,
        };

        for op in &ops {
            match op {
                SimOp::Write { lba, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba as u64, &data);
                    oracle.insert(*lba as u64, data);
                }
                SimOp::DedupWrite { lba_a, lba_b, seed } => {
                    let data = [*seed; 4096];
                    let _ = vol.write(*lba_a as u64, &data);
                    let _ = vol.write(*lba_b as u64, &data);
                    oracle.insert(*lba_a as u64, data);
                    oracle.insert(*lba_b as u64, data);
                }
                SimOp::Flush => {
                    let _ = vol.flush_wal();
                }
                SimOp::DrainLocal => {
                    let pending_dir = fork_dir.join("pending");
                    let segments_dir = fork_dir.join("segments");
                    if let Ok(entries) = fs::read_dir(&pending_dir) {
                        for entry in entries.flatten() {
                            let _ = fs::rename(
                                entry.path(),
                                segments_dir.join(entry.file_name()),
                            );
                        }
                    }
                }
                SimOp::GcSweep => {
                    // gc_checkpoint flushes the WAL and returns two ULIDs from
                    // the volume's own mint.  Using these (not Ulid::new()) is
                    // critical: the mint advances past both ULIDs so all future
                    // WAL segments sort above the GC outputs on rebuild.
                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

                    // Step 1: real GC compaction (no-ops if nothing to compact).
                    let _ = rt.block_on(gc_fork(
                        fork_dir,
                        "test-vol",
                        &store,
                        &gc_config,
                        &repack_ulid,
                        &sweep_ulid,
                    ));

                    // Step 2: volume re-signs GC output, updates extent index.
                    let _ = vol.apply_gc_handoffs();

                    // Step 3: upload to InMemory store, delete old local files.
                    let _ = rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store));

                    // Assert: every oracle LBA still reads its expected value.
                    for (&lba, expected) in &oracle {
                        match vol.read(lba, 1) {
                            Ok(data) => prop_assert_eq!(
                                data.as_slice(),
                                expected.as_slice(),
                                "lba {} reads wrong data after GcSweep",
                                lba
                            ),
                            Err(e) => prop_assert!(
                                false,
                                "lba {} read failed after GcSweep: {}",
                                lba,
                                e
                            ),
                        }
                    }
                }
            }
        }
    }
}
