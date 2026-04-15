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
//   1. gc_fork()              — real compact_segments / collect_stats
//   2. apply_gc_handoffs()    — volume re-signs and updates extent index
//   3. promote_gc_outputs()   — simulates coordinator promote IPC + gc body cleanup
//   4. evict_applied_gc_cache — volume evicts old cache files (actor step)
//   5. apply_done_handoffs()  — uploads to InMemory store, deletes old S3 objects
// Then asserts all oracle LBAs are readable with correct data.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use elide_coordinator::config::GcConfig;
use elide_coordinator::gc::{GcStrategy, apply_done_handoffs, gc_fork};
use elide_core::volume::Volume;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use proptest::prelude::*;

/// Simulate coordinator drain: for each file in pending/, promote the segment
/// body to cache/ and remove the pending file.  The volume already wrote
/// `index/<ulid>.idx` at WAL flush time, so drain does not write it again.
fn simulate_upload(vol: &mut Volume, dir: &Path) {
    let pending_dir = dir.join("pending");
    let cache_dir = dir.join("cache");
    fs::create_dir_all(&cache_dir).unwrap();

    let Ok(entries) = fs::read_dir(&pending_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(ulid_str) = name.to_str() else {
            continue;
        };
        if ulid_str.ends_with(".tmp") {
            continue;
        }
        let Ok(ulid) = ulid::Ulid::from_string(ulid_str) else {
            continue;
        };
        // Materialise thin DedupRef → DedupRef (filled), then promote.
        let _ = vol.redact_segment(ulid);
        let _ = vol.promote_segment(ulid);
    }
}

/// Simulate the coordinator promote+finalize sequence for each volume-applied
/// gc output: under the self-describing handoff protocol, those are the bare
/// `gc/<ulid>` files (no extension). promote_segment writes index/<new>.idx
/// and cache/<new>.body; finalize_gc_handoff deletes the bare body.
fn promote_gc_outputs(vol: &mut Volume, dir: &Path) {
    let gc_dir = dir.join("gc");
    let Ok(entries) = fs::read_dir(&gc_dir) else {
        return;
    };
    let mut bare: Vec<ulid::Ulid> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?;
            if name.contains('.') {
                return None;
            }
            ulid::Ulid::from_string(name).ok()
        })
        .collect();
    bare.sort();
    for ulid in bare {
        let _ = vol.promote_segment(ulid);
        let _ = vol.finalize_gc_handoff(ulid);
    }
}

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

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let gc_config = GcConfig {
            density_threshold: 0.0,
            small_segment_bytes: u64::MAX,
            interval_secs: 0,
        };

        let cache_dir = fork_dir.join("cache");
        let index_dir = fork_dir.join("index");

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
                    // Drain: volume already wrote index/ at flush; simulate
                    // coordinator drain (upload + promote IPC) by calling
                    // promote_segment directly on the volume.
                    simulate_upload(&mut vol, fork_dir);

                    // Count idx files before gc_checkpoint so we can detect
                    // any new segments it writes (WAL flush).  Those segments
                    // land in pending/ without a cache body, so collect_stats
                    // skips them — they will not be compacted this sweep and
                    // legitimately remain in index/ after GC.
                    let idx_pre_checkpoint: usize = fs::read_dir(&index_dir)
                        .map(|d| d.flatten().count())
                        .unwrap_or(0);

                    let (repack_ulid, sweep_ulid) = vol.gc_checkpoint().unwrap();

                    let idx_before: usize = fs::read_dir(&index_dir)
                        .map(|d| d.flatten().count())
                        .unwrap_or(0);
                    // Segments added by gc_checkpoint (WAL flush) are excluded
                    // from this GC pass and survive into the next tick.
                    let checkpoint_extra = idx_before.saturating_sub(idx_pre_checkpoint);

                    let gc_stats = rt.block_on(gc_fork(
                        fork_dir,
                        "test-vol",
                        &store,
                        &gc_config,
                        repack_ulid,
                        sweep_ulid,
                    ));

                    // Volume applies the handoff: re-signs gc body, updates extent
                    // index, writes index/<new>.idx, deletes index/<old>.idx.
                    let _ = vol.apply_gc_handoffs();

                    // Simulate coordinator promote IPC: copies gc/<new> → cache/<new>,
                    // deletes gc/<new>.  In production this is an IPC round-trip;
                    // here we call vol.promote_segment directly.
                    promote_gc_outputs(&mut vol, fork_dir);

                    // Actor step: evict old cache files after publishing the new snapshot.
                    vol.evict_applied_gc_cache();

                    // Coordinator: upload new segment to S3, delete old S3 objects.
                    // cache/<new>.body already exists (seg_promoted=true), so
                    // apply_done_handoffs skips the upload+promote branch.
                    let _ = rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store));

                    if let Ok(stats) = gc_stats
                        && stats.strategy != GcStrategy::None
                    {
                        // After GC, index/ should have ≤1 .idx file from
                        // the compacted set, plus any segments that
                        // gc_checkpoint wrote this tick (they were excluded
                        // from compaction because their cache body is not
                        // yet present and will be drained next tick).
                        let idx_after: usize = fs::read_dir(&index_dir)
                            .map(|d| d.flatten().count())
                            .unwrap_or(0);
                        let idx_max = 1 + checkpoint_extra;
                        prop_assert!(
                            idx_after <= idx_max,
                            "after GcSweep on {} segments, {} .idx files remain \
                             (expected ≤{}: 1 GC output + {} checkpoint segment(s))",
                            idx_before,
                            idx_after,
                            idx_max,
                            checkpoint_extra
                        );
                        // cache/ .body files: same count as .idx files.
                        let bodies_after: usize = fs::read_dir(&cache_dir)
                            .map(|d| {
                                d.flatten()
                                    .filter(|e| {
                                        e.path().extension().is_some_and(|x| x == "body")
                                    })
                                    .count()
                            })
                            .unwrap_or(0);
                        prop_assert!(
                            bodies_after <= 1,
                            "after GcSweep, {} .body files remain in cache/ (expected ≤1)",
                            bodies_after
                        );
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
                    // Drain: volume already wrote index/ at flush; simulate
                    // coordinator drain (upload + promote IPC) by calling
                    // promote_segment directly on the volume.
                    simulate_upload(&mut vol, fork_dir);

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

                    // Step 3: simulate coordinator promote IPC + actor evict.
                    promote_gc_outputs(&mut vol, fork_dir);
                    vol.evict_applied_gc_cache();

                    // Step 4: upload new segment to S3, delete old S3 objects.
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

/// Bug H: promote_segment with .materialized sidecar did not evict the file
/// handle cache.  After materialise + promote, the extent index has offsets
/// from the .materialized segment (larger index section → different
/// body_section_start), but the file cache may still hold an fd to the
/// deleted pending/ file with is_body=false.  The next read computes
/// bss_materialized + body_offset against the old file, seeking past the
/// body section → "failed to fill whole buffer".
///
/// Nondeterministic in the proptest because HashMap iteration order
/// determines whether the file cache happens to hold the affected segment.
///
/// Sequence: DedupWrite creates a segment with a thin DedupRef.  GcSweep
/// materialises + promotes it (replacing pending/ with cache/).  A
/// subsequent read of the dedup-ref LBA uses the stale cached fd.
#[test]
fn gc_oracle_repro_bug_h() {
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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let gc_config = GcConfig {
        density_threshold: 0.0,
        small_segment_bytes: u64::MAX,
        interval_secs: 0,
    };

    // DedupWrite: lba 3 gets DATA, lba 6 gets thin DEDUP_REF (same hash).
    let data_235 = [235u8; 4096];
    vol.write(3, &data_235).unwrap();
    vol.write(6, &data_235).unwrap();

    // GcSweep drains pending (none), then gc_checkpoint flushes the WAL
    // to pending/S1 (DATA + DEDUP_REF).  No index files yet → gc_fork is
    // a no-op.  The oracle read here populates the file cache with
    // (S1, is_body=false, fd→pending/S1).
    simulate_upload(&mut vol, fork_dir);
    let (r, s) = vol.gc_checkpoint().unwrap();
    let _ = rt.block_on(gc_fork(fork_dir, "test-vol", &store, &gc_config, r, s));
    let _ = vol.apply_gc_handoffs();
    promote_gc_outputs(&mut vol, fork_dir);
    vol.evict_applied_gc_cache();
    let _ = rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store));
    // Read both LBAs to populate file cache with the pending/ segment.
    assert_eq!(&vol.read(3, 1).unwrap(), &data_235);
    assert_eq!(&vol.read(6, 1).unwrap(), &data_235);

    // Write another LBA so gc_checkpoint has something to flush.
    vol.write(1, &[195u8; 4096]).unwrap();

    // Second GcSweep: simulate_upload materialises S1 (DedupRef → fat
    // DedupRef (filled)) and promotes it — pending/S1 is deleted, replaced by
    // cache/S1.body.  The extent index is updated with offsets from the
    // .materialized segment.  Without the Bug H fix, the file cache still
    // holds the stale fd to pending/S1.
    simulate_upload(&mut vol, fork_dir);
    let (r, s) = vol.gc_checkpoint().unwrap();
    let _ = rt.block_on(gc_fork(fork_dir, "test-vol", &store, &gc_config, r, s));
    let _ = vol.apply_gc_handoffs();
    promote_gc_outputs(&mut vol, fork_dir);
    vol.evict_applied_gc_cache();
    let _ = rt.block_on(apply_done_handoffs(fork_dir, "test-vol", &store));

    // These reads triggered "failed to fill whole buffer" before the fix.
    assert_eq!(&vol.read(3, 1).unwrap(), &data_235);
    assert_eq!(&vol.read(6, 1).unwrap(), &data_235);
    assert_eq!(&vol.read(1, 1).unwrap(), &[195u8; 4096]);
}
