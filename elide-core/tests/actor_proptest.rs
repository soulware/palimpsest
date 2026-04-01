// Property-based tests for the actor + snapshot layer.
//
// Tests four invariants:
//
// 1. Read-your-writes: after handle.write() returns Ok, handle.read() of the
//    same LBA immediately returns the written data. This exercises the ArcSwap
//    snapshot publication path — reads bypass the channel and load the current
//    snapshot directly, with no flush required.
//
// 2. Crash recovery: after shutting down the actor and reopening the Volume,
//    every LBA reads back the value last written to it. Volume::open() calls
//    recover_wal(), so writes that were never flushed to a pending segment are
//    still recoverable from the WAL.
//
// 3. GC handoff correctness: after DrainLocal + CoordGcLocal + ApplyGcHandoffs,
//    every oracle LBA is still readable via the handle with correct data.  This
//    exercises the ApplyGcHandoffs message path through the actor channel and
//    verifies that the snapshot is republished so reads reflect the updated
//    extent index immediately.
//
// 4. Compaction snapshot update: after SweepPending or Repack via the actor
//    channel, every oracle LBA is still readable with correct data.  These ops
//    delete old pending/ segment files and write new ones; if publish_snapshot()
//    did not bump flush_gen, handles would reuse stale cached fds and get ENOENT
//    or read from wrong offsets.
//
// Together these verify that the actor correctly publishes snapshots, that no
// combination of writes, flushes, compactions, and crashes loses data visible
// through the handle, and that coordinator GC handoffs propagate correctly
// through the actor/snapshot layer.

use std::collections::HashMap;
use std::thread;

use elide_core::actor::spawn;
use elide_core::volume::Volume;
use proptest::prelude::*;

mod common;

// --- strategy ---

#[derive(Debug, Clone)]
enum ActorOp {
    /// Write data to an LBA through the actor channel; immediately assert
    /// read-your-writes before any flush.
    Write { lba: u8, seed: u8 },
    /// Flush the WAL to a pending/ segment through the actor channel.
    Flush,
    /// Move all committed pending/ segments to segments/, simulating
    /// drain-pending. Required before CoordGcLocal has material to work with.
    DrainLocal,
    /// Simulate one coordinator GC pass and apply the resulting handoff
    /// through the actor channel.  Verifies that the snapshot is republished
    /// and all oracle LBAs remain readable via the handle.
    CoordGcLocal { n: usize },
    /// Sweep small pending segments via the actor channel.  After the call,
    /// old pending/ files are deleted; publish_snapshot() must have bumped
    /// flush_gen so handles evict stale cached fds before the next read.
    SweepPending,
    /// Repack sparse pending segments via the actor channel.  Same invariant
    /// as SweepPending: old files deleted, snapshot must be republished.
    Repack,
    /// Simulate a crash: shut down the actor, reopen the Volume (triggering
    /// WAL recovery), and assert all oracle LBAs are still readable.
    Crash,
}

fn arb_actor_op() -> impl Strategy<Value = ActorOp> {
    prop_oneof![
        4 => (0u8..8, any::<u8>()).prop_map(|(lba, seed)| ActorOp::Write { lba, seed }),
        2 => Just(ActorOp::Flush),
        2 => Just(ActorOp::DrainLocal),
        1 => (2usize..=5).prop_map(|n| ActorOp::CoordGcLocal { n }),
        1 => Just(ActorOp::SweepPending),
        1 => Just(ActorOp::Repack),
        1 => Just(ActorOp::Crash),
    ]
}

fn arb_actor_ops() -> impl Strategy<Value = Vec<ActorOp>> {
    prop::collection::vec(arb_actor_op(), 1..40)
}

// --- property ---

proptest! {
    /// Actor correctness: read-your-writes, crash recovery, and GC handoff.
    ///
    /// After every Write: immediately read the same LBA and assert the data
    /// matches (read-your-writes via ArcSwap snapshot, no flush needed).
    ///
    /// After every CoordGcLocal: simulate a coordinator GC pass, apply the
    /// handoff through the actor channel, then assert every oracle LBA is
    /// still readable with correct data.
    ///
    /// After every Crash: shut down the actor, reopen the Volume (triggering
    /// WAL recovery), spawn a new actor, then assert that every oracle entry
    /// is still readable.
    #[test]
    fn actor_correctness(ops in arb_actor_ops()) {
        let dir = tempfile::TempDir::new().unwrap();
        let fork_dir = dir.path();

        let vol = Volume::open(fork_dir, fork_dir).unwrap();
        let (actor, mut handle) = spawn(vol);
        let mut actor_thread = Some(
            thread::Builder::new()
                .name("volume-actor".into())
                .spawn(move || actor.run())
                .unwrap(),
        );

        let mut oracle: HashMap<u64, [u8; 4096]> = HashMap::new();

        for op in &ops {
            match op {
                ActorOp::Write { lba, seed } => {
                    let data = vec![*seed; 4096];
                    if handle.write(*lba as u64, data).is_ok() {
                        let expected = [*seed; 4096];
                        // Read-your-writes: snapshot must reflect this write
                        // immediately, before any flush to pending/.
                        let actual = handle.read(*lba as u64, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            &expected[..],
                            "read-your-writes failed for lba {}",
                            lba
                        );
                        oracle.insert(*lba as u64, expected);
                    }
                }
                ActorOp::Flush => {
                    let _ = handle.flush();
                }
                ActorOp::DrainLocal => {
                    common::drain_local(fork_dir);
                }
                ActorOp::CoordGcLocal { n } => {
                    // Checkpoint: flush WAL and obtain a ULID for the GC
                    // output, matching the real coordinator's gc_checkpoint.
                    let (repack_ulid, _) = handle
                        .gc_checkpoint()
                        .unwrap_or_else(|_| (ulid::Ulid::new().to_string(), String::new()));
                    let gc_ulid = ulid::Ulid::from_string(&repack_ulid)
                        .unwrap_or_else(|_| ulid::Ulid::new());
                    // Simulate one coordinator GC pass (writes gc/*.pending).
                    // Returns paths to delete — we hold them until after the
                    // handoff is applied, matching the real coordinator's ordering.
                    let to_delete = common::simulate_coord_gc_local(fork_dir, gc_ulid, *n)
                        .map(|(_, _, paths)| paths)
                        .unwrap_or_default();
                    // Apply the handoff through the actor channel.  This
                    // exercises the ApplyGcHandoffs message path and verifies
                    // the snapshot is republished with the updated extent index.
                    let _ = handle.apply_gc_handoffs();
                    // Old segment files are safe to delete only after the
                    // handoff is applied.
                    for path in &to_delete {
                        let _ = std::fs::remove_file(path);
                    }
                    // All oracle LBAs must still be readable with correct data.
                    for (&lba, expected) in &oracle {
                        let actual = handle.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "lba {} wrong after gc handoff",
                            lba
                        );
                    }
                }
                ActorOp::SweepPending => {
                    let _ = handle.sweep_pending();
                    // Old pending/ files are deleted; if publish_snapshot() did
                    // not bump flush_gen, handles reuse stale fds and get ENOENT.
                    for (&lba, expected) in &oracle {
                        let actual = handle.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "lba {} wrong after sweep_pending via actor",
                            lba
                        );
                    }
                }
                ActorOp::Repack => {
                    // Use 0.5 ratio: fires on any segment with >50% dead extents,
                    // which occurs naturally after write-overwrite-flush sequences.
                    let _ = handle.repack(0.5);
                    // Same invariant: repack deletes old files; snapshot must be
                    // republished so handles evict their cached fds.
                    for (&lba, expected) in &oracle {
                        let actual = handle.read(lba, 1).unwrap();
                        prop_assert_eq!(
                            actual.as_slice(),
                            expected.as_slice(),
                            "lba {} wrong after repack via actor",
                            lba
                        );
                    }
                }
                ActorOp::Crash => {
                    // Shut down the actor and wait for it to exit before
                    // reopening the volume directory.
                    handle.shutdown();
                    if let Some(t) = actor_thread.take() {
                        let _ = t.join();
                    }

                    let vol = Volume::open(fork_dir, fork_dir).unwrap();
                    let (new_actor, new_handle) = spawn(vol);
                    actor_thread = Some(
                        thread::Builder::new()
                            .name("volume-actor".into())
                            .spawn(move || new_actor.run())
                            .unwrap(),
                    );
                    handle = new_handle;

                    for (&lba, expected) in &oracle {
                        let actual = handle.read(lba, 1).unwrap();
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

        // Graceful shutdown after the property run completes.
        handle.shutdown();
        if let Some(t) = actor_thread {
            let _ = t.join();
        }
    }
}
