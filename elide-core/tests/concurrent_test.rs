// Concurrent integration test: coordinator GC + live volume reads.
//
// Verifies that reads never fail during a coordinator GC pass.  The failure
// mode under test: if the coordinator deletes old segment files before the
// volume has applied the GC handoff, any read of a cold LBA (one whose
// extent index entry still points at the now-deleted file) returns a
// file-not-found error.
//
// The fix: the coordinator must not delete old local segment files until after
// the volume has acknowledged the handoff (renamed gc/<ulid>.pending to
// gc/<ulid>.applied).  That rename signals that the volume's extent index now
// points at the new compacted segment, making the old files safe to delete.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use elide_core::actor::spawn;
use elide_core::volume::Volume;

mod common;

/// Concurrent coordinator GC must not create a window where reads fail.
///
/// Seeds two segments of data, then runs a coordinator thread (GC pass) and a
/// reader thread (continuous reads of seeded LBAs) concurrently.  The
/// coordinator GC produces a compacted segment and a `gc/*.pending` handoff
/// file; the reader must never observe a file-not-found error regardless of
/// when the handoff is applied relative to the deletion of the old files.
#[test]
fn coordinator_gc_does_not_create_read_failures() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);

    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    let (actor, handle) = spawn(vol);
    let actor_thread = thread::spawn(move || actor.run());

    let mut oracle: HashMap<u64, Vec<u8>> = HashMap::new();

    // Seed phase: write two separate batches, flush each to a distinct
    // segment, then drain both to index/ + cache/.  This gives the coordinator
    // two candidates to compact.
    for lba in 0u64..4 {
        let data = vec![(lba as u8).wrapping_mul(11); 4096];
        handle.write(lba, data.clone()).unwrap();
        oracle.insert(lba, data);
    }
    handle.flush().unwrap();
    common::drain_via_handle(&handle, &fork_dir);

    for lba in 4u64..8 {
        let data = vec![(lba as u8).wrapping_mul(13); 4096];
        handle.write(lba, data.clone()).unwrap();
        oracle.insert(lba, data);
    }
    handle.flush().unwrap();
    common::drain_via_handle(&handle, &fork_dir);

    // Reader thread: continuously reads all seeded LBAs.  These are cold —
    // they are in cache/, not in the WAL.  A read failure here means the
    // extent index still points at a file the coordinator has already deleted.
    let read_reader = handle.reader();
    let oracle_snap = oracle.clone();
    let read_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let read_errors_clone = Arc::clone(&read_errors);

    let reader = thread::spawn(move || {
        for _ in 0..500 {
            for lba in 0u64..8 {
                match read_reader.read(lba, 1) {
                    Ok(actual) => {
                        if let Some(expected) = oracle_snap.get(&lba)
                            && actual != *expected
                        {
                            read_errors_clone
                                .lock()
                                .unwrap()
                                .push(format!("lba {lba}: wrong data"));
                        }
                    }
                    Err(e) => {
                        read_errors_clone
                            .lock()
                            .unwrap()
                            .push(format!("lba {lba}: read error: {e}"));
                    }
                }
            }
            thread::sleep(Duration::from_micros(200));
        }
    });

    // Coordinator thread: compact the two seeded segments.
    // simulate_coord_gc_local returns the paths of the consumed input segments
    // rather than deleting them inline — deletion must happen only after the
    // handoff is applied.
    let gc_handle = handle.clone();
    let fork_dir_gc = fork_dir.clone();

    let coordinator = thread::spawn(move || {
        // Brief pause so the reader has time to start.
        thread::sleep(Duration::from_millis(5));

        let gc_ulid = gc_handle.gc_checkpoint().unwrap();
        if let Some((_, _, to_delete)) = common::simulate_coord_gc_local(&fork_dir_gc, gc_ulid, 2) {
            // Apply the handoff before deleting old files.  This updates the
            // volume's extent index to point at the new compacted segment,
            // ensuring reads find valid data before the old files disappear.
            gc_handle.apply_gc_handoffs().unwrap();

            // Old files are safe to delete only after the handoff is applied.
            for path in &to_delete {
                let _ = std::fs::remove_file(path);
            }
        }
    });

    coordinator.join().unwrap();
    reader.join().unwrap();

    let errors = read_errors.lock().unwrap();
    assert!(
        errors.is_empty(),
        "reads failed during concurrent GC: {:?}",
        &*errors
    );

    // Full oracle check after everything settles.
    let final_reader = handle.reader();
    for (lba, expected) in &oracle {
        let actual = final_reader.read(*lba, 1).unwrap();
        assert_eq!(actual, *expected, "lba {lba} wrong in final check");
    }

    handle.shutdown();
    actor_thread.join().unwrap();
}

/// Regression: two `apply_gc_handoffs` calls arriving close together must
/// not drop either reply channel.
///
/// The actor's internal `idle_tick` also calls `start_gc_handoffs` (with
/// `reply=None`), so an IPC caller's reply can collide with it in exactly
/// the same way two IPC callers collide. Prior to the fix, the second
/// invocation unconditionally overwrote `self.parked_handoffs`, dropping
/// the first caller's reply sender — the receiver saw
/// `"volume actor reply channel closed"`, matching the coordinator warning
/// users observed on a running volume with a pending handoff.
///
/// The fix rejects a concurrent call when a batch is already in flight;
/// the first caller always gets `Ok(n)`. The second caller sees either
/// `Ok(n)` (if its message arrived after the first batch drained) or an
/// `"apply_gc_handoffs already in progress"` error (the coordinator
/// retries next tick). What must never happen is "reply channel closed".
#[test]
fn concurrent_apply_gc_handoffs_does_not_drop_replies() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);

    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    let (actor, handle) = spawn(vol);
    let actor_thread = thread::spawn(move || actor.run());

    // Seed two committed segments in index/ so the GC simulator has
    // candidates. Each segment comes from: write → promote_wal (WAL →
    // pending/) → drain_via_handle (pending/ → index/ + cache/).
    for lba in 0u64..4 {
        let data = vec![(lba as u8).wrapping_mul(11); 4096];
        handle.write(lba, data).unwrap();
    }
    handle.promote_wal().unwrap();
    common::drain_via_handle(&handle, &fork_dir);

    for lba in 4u64..8 {
        let data = vec![(lba as u8).wrapping_mul(13); 4096];
        handle.write(lba, data).unwrap();
    }
    handle.promote_wal().unwrap();
    common::drain_via_handle(&handle, &fork_dir);

    // Stage a GC handoff (gc/<new>.plan) without applying it yet — the
    // coordinator-side plan emitter runs, but no one has called
    // apply_gc_handoffs on the volume.
    let gc_ulid = handle.gc_checkpoint().unwrap();
    common::simulate_coord_gc_local(&fork_dir, gc_ulid, 2)
        .expect("GC simulation must produce a plan");

    // Two threads race to apply the handoff. Before the fix, the second
    // call would drop the first caller's reply sender.
    let h1 = handle.clone();
    let h2 = handle.clone();
    let t1 = thread::spawn(move || h1.apply_gc_handoffs());
    let t2 = thread::spawn(move || h2.apply_gc_handoffs());

    let r1 = t1.join().expect("thread 1 panicked");
    let r2 = t2.join().expect("thread 2 panicked");

    for (label, result) in [("r1", &r1), ("r2", &r2)] {
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("reply channel closed"),
                "{label} saw dropped reply: {msg}"
            );
            assert!(
                msg.contains("already in progress"),
                "{label} got unexpected error: {msg}"
            );
        }
    }
    assert!(
        r1.is_ok() || r2.is_ok(),
        "at least one apply_gc_handoffs must succeed: r1={r1:?} r2={r2:?}"
    );

    handle.shutdown();
    actor_thread.join().unwrap();
}
