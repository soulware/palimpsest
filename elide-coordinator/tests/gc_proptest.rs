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
    /// Run the full real coordinator GC round-trip, then assert the oracle.
    ///
    /// Mirrors the coordinator tick order: drain pending/ → segments/ first
    /// (Upload, step 4), then gc_checkpoint + gc_fork + apply (GC, step 5).
    /// This ordering is an invariant of the production coordinator — GC always
    /// runs after pending/ is empty — and is enforced here to keep the model
    /// faithful.  Generating GcSweep before a drain would exercise a sequence
    /// the coordinator never produces and masks a structurally distinct bug
    /// (pre-existing pending segments with ULIDs below the GC output ULID).
    GcSweep,
    /// Simulate a coordinator/volume restart.
    ///
    /// Drops the current Volume and reopens it from disk (rebuilding the extent
    /// index from .idx files).  Mirrors the production invariant: the coordinator
    /// calls apply_gc_handoffs (IPC) immediately after restart to re-apply any
    /// .applied handoffs before apply_done_handoffs can delete old segments.
    ///
    /// After reopen, asserts that all oracle LBAs are still readable.  This
    /// catches the Bug E class: extent index rebuilt to stale state, then old
    /// segment deleted → "segment not found".
    Restart,
}

fn arb_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        4 => (0u8..8, any::<u8>()).prop_map(|(lba, seed)| SimOp::Write { lba, seed }),
        2 => (0u8..4, 4u8..8, any::<u8>()).prop_map(|(lba_a, lba_b, seed)| SimOp::DedupWrite {
            lba_a,
            lba_b,
            seed
        }),
        2 => Just(SimOp::Flush),
        1 => Just(SimOp::GcSweep),
        1 => Just(SimOp::Restart),
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
        let index_dir = fork_dir.join("index");
        fs::create_dir_all(&index_dir).unwrap();

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
                SimOp::GcSweep => {
                    // Drain pending/ → segments/ before gc_checkpoint, mirroring
                    // the coordinator upload tick. Write .idx before rename to
                    // keep the simulation faithful to the production invariant:
                    // segments/<ulid> exists → index/<ulid>.idx exists.
                    let pending_dir = fork_dir.join("pending");
                    if let Ok(entries) = fs::read_dir(&pending_dir) {
                        for entry in entries.flatten() {
                            let name = entry.file_name();
                            let src = entry.path();
                            let idx_path =
                                index_dir.join(format!("{}.idx", name.to_string_lossy()));
                            let _ = elide_core::segment::extract_idx(&src, &idx_path);
                            let _ = fs::rename(src, segments_dir.join(name));
                        }
                    }

                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

                    let segs_before: usize = fs::read_dir(&segments_dir)
                        .map(|d| d.flatten().count())
                        .unwrap_or(0);

                    let gc_stats = rt.block_on(gc_fork(
                        fork_dir,
                        "test-vol",
                        &store,
                        &gc_config,
                        repack_ulid,
                        sweep_ulid,
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
                SimOp::Restart => {
                    // Drop and reopen the volume, mirroring a coordinator/volume
                    // restart.  Volume::open rebuilds the extent index from .idx
                    // files; apply_gc_handoffs then re-applies any .applied
                    // handoffs (Bug E fix).  No oracle mutation: restart is
                    // invisible to the logical data model.
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
                    let _ = vol.apply_gc_handoffs();
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
        let index_dir = fork_dir.join("index");
        fs::create_dir_all(&index_dir).unwrap();

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
                SimOp::GcSweep => {
                    // Drain pending/ → segments/ before gc_checkpoint, mirroring
                    // the coordinator upload tick. Write .idx before rename to
                    // keep the simulation faithful to the production invariant:
                    // segments/<ulid> exists → index/<ulid>.idx exists.
                    let pending_dir = fork_dir.join("pending");
                    let segments_dir = fork_dir.join("segments");
                    if let Ok(entries) = fs::read_dir(&pending_dir) {
                        for entry in entries.flatten() {
                            let name = entry.file_name();
                            let src = entry.path();
                            let idx_path =
                                index_dir.join(format!("{}.idx", name.to_string_lossy()));
                            let _ = elide_core::segment::extract_idx(&src, &idx_path);
                            let _ = fs::rename(src, segments_dir.join(name));
                        }
                    }

                    // gc_checkpoint flushes the WAL under a pre-minted ULID
                    // (u_wal > u_sweep) and returns (u_repack, u_sweep) from
                    // the volume's own mint, so future WAL segments always sort
                    // above GC outputs on rebuild.
                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

                    // Step 1: real GC compaction (no-ops if nothing to compact).
                    let _ = rt.block_on(gc_fork(
                        fork_dir,
                        "test-vol",
                        &store,
                        &gc_config,
                        repack_ulid,
                        sweep_ulid,
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
                SimOp::Restart => {
                    // Drop and reopen the volume, mirroring a coordinator/volume
                    // restart.  Volume::open rebuilds the extent index from .idx
                    // files; apply_gc_handoffs then re-applies any .applied
                    // handoffs (Bug E fix) before any subsequent GcSweep can
                    // call apply_done_handoffs and delete old segments.
                    drop(vol);
                    vol = Volume::open(fork_dir, fork_dir).unwrap();
                    let _ = vol.apply_gc_handoffs();

                    // Assert: every oracle LBA is still readable after restart.
                    // The segment bodies have not been deleted yet (no
                    // apply_done_handoffs since restart), so both old and new
                    // segments are present — reads must succeed regardless of
                    // which one the extent index points to.
                    for (&lba, expected) in &oracle {
                        match vol.read(lba, 1) {
                            Ok(data) => prop_assert_eq!(
                                data.as_slice(),
                                expected.as_slice(),
                                "lba {} reads wrong data after Restart",
                                lba
                            ),
                            Err(e) => prop_assert!(
                                false,
                                "lba {} read failed after Restart: {}",
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
