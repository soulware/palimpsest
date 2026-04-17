// Regression tests for the FLUSH / promote interaction after moving
// the old-WAL fsync off the actor thread (see `execute_promote` and
// `VolumeActor::park_or_resolve_flush`).
//
// The actor no longer fsyncs the old WAL at the 32 MiB threshold.
// Instead the fsync happens on the worker as the first step of the
// promote, and `VolumeRequest::Flush` parks on a promote generation
// counter until every promote dispatched before the flush has
// completed.  These tests verify that property: after `handle.flush()`
// returns, any promote triggered by a prior write is on disk and the
// old WAL file has been cleaned up.

use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use elide_core::actor::spawn;
use elide_core::volume::Volume;

mod common;

fn open_actor(dir: &Path) -> (elide_core::actor::VolumeHandle, thread::JoinHandle<()>) {
    common::write_test_keypair(dir);
    let vol = Volume::open(dir, dir).unwrap();
    let (actor, handle) = spawn(vol);
    let t = thread::spawn(move || actor.run());
    (handle, t)
}

fn incompressible_block(i: u64) -> Vec<u8> {
    let mut b = vec![0u8; 1024 * 1024];
    let fill = (i & 0xFF) as u8;
    for (j, x) in b.iter_mut().enumerate() {
        *x = fill ^ (j as u8).wrapping_mul(0x6D).wrapping_add(0x4F);
    }
    b
}

/// After writing enough to cross the 32 MiB threshold and then
/// issuing FLUSH, the pending/ segment must be on disk and the old
/// WAL file deleted before `handle.flush()` returns.
///
/// This is the load-bearing property of the off-actor fsync change:
/// FLUSH still guarantees durability of every prior write even though
/// the fsync + segment write now happen on the worker thread.
#[test]
fn flush_waits_for_in_flight_promote_to_complete() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    let (handle, actor_thread) = open_actor(&fork_dir);

    // 33 × 1 MiB of incompressible writes — exceeds the 32 MiB
    // FLUSH_THRESHOLD, so the actor dispatches a promote to the worker
    // after one of these writes returns.
    for i in 0..33u64 {
        handle.write(i * 256, incompressible_block(i)).unwrap();
    }

    // FLUSH: must wait for the dispatched promote's old-WAL fsync
    // (and, in this implementation, the entire promote) to complete.
    handle.flush().unwrap();

    // After flush returns we expect:
    //   - pending/<ulid> exists (segment committed by the worker)
    //   - exactly one wal/ file (the fresh one opened during prep)
    let pending_count = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter(|e| {
            let e = e.as_ref().unwrap();
            let name = e.file_name();
            let s = name.to_string_lossy();
            !s.ends_with(".tmp") && !s.starts_with('.')
        })
        .count();
    assert!(
        pending_count >= 1,
        "expected at least one committed pending/ segment after flush, got {pending_count}"
    );

    let wal_count = fs::read_dir(fork_dir.join("wal"))
        .unwrap()
        .filter(|e| e.is_ok())
        .count();
    assert_eq!(
        wal_count, 1,
        "expected exactly one WAL file after flush (old one should be deleted)"
    );

    drop(handle);
    actor_thread.join().unwrap();
}

/// FLUSH with no promote in flight takes the fast path: WAL fsync on
/// the active WAL and immediate reply.  No pending/ segments should be
/// produced.
#[test]
fn flush_without_pending_promote_is_fast_path() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    let (handle, actor_thread) = open_actor(&fork_dir);

    // One small write — far below the 32 MiB threshold.
    handle.write(0, vec![0xABu8; 4096]).unwrap();
    handle.flush().unwrap();

    // No promote was triggered, so pending/ should still be empty.
    let pending_count = fs::read_dir(fork_dir.join("pending"))
        .unwrap()
        .filter(|e| e.is_ok())
        .count();
    assert_eq!(
        pending_count, 0,
        "expected no pending/ segments when write is below threshold"
    );

    drop(handle);
    actor_thread.join().unwrap();
}

/// Writes that happened before FLUSH must be readable after a
/// simulated crash + reopen, even though the WAL fsync now happens
/// asynchronously on the worker.
///
/// This is the durability contract FLUSH protects: every write before
/// the FLUSH reply must survive a crash, regardless of whether the
/// worker or actor performed the fsync.
#[test]
fn data_survives_crash_after_flush_with_deferred_fsync() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();

    let probe_a = vec![0xABu8; 4096];
    let probe_b = vec![0xCDu8; 4096];

    {
        let (handle, actor_thread) = open_actor(&fork_dir);

        // Cross the threshold so a promote is in flight.
        for i in 0..33u64 {
            handle.write(i * 256, incompressible_block(i)).unwrap();
        }
        // Writes whose durability must survive reopen.
        handle.write(10_000, probe_a.clone()).unwrap();
        handle.write(10_001, probe_b.clone()).unwrap();
        handle.flush().unwrap();

        // Drop the handle to close the channel, then join the actor
        // — this is the cleanest "simulated crash+reopen" boundary
        // available at the library level.
        drop(handle);
        actor_thread.join().unwrap();
    }

    // Reopen and verify the post-flush writes are still there.
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    let got_a = vol.read(10_000, 1).unwrap();
    let got_b = vol.read(10_001, 1).unwrap();
    assert_eq!(
        got_a.as_slice(),
        probe_a.as_slice(),
        "LBA 10_000 must survive reopen after flush"
    );
    assert_eq!(
        got_b.as_slice(),
        probe_b.as_slice(),
        "LBA 10_001 must survive reopen after flush"
    );
}
