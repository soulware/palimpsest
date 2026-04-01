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

    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    let (actor, handle) = spawn(vol);
    let actor_thread = thread::spawn(move || actor.run());

    let mut oracle: HashMap<u64, Vec<u8>> = HashMap::new();

    // Seed phase: write two separate batches, flush each to a distinct
    // segment, then drain both to segments/.  This gives the coordinator
    // two candidates to compact.
    for lba in 0u64..4 {
        let data = vec![(lba as u8).wrapping_mul(11); 4096];
        handle.write(lba, data.clone()).unwrap();
        oracle.insert(lba, data);
    }
    handle.flush().unwrap();
    common::drain_local(&fork_dir);

    for lba in 4u64..8 {
        let data = vec![(lba as u8).wrapping_mul(13); 4096];
        handle.write(lba, data.clone()).unwrap();
        oracle.insert(lba, data);
    }
    handle.flush().unwrap();
    common::drain_local(&fork_dir);

    // Reader thread: continuously reads all seeded LBAs.  These are cold —
    // they are in segments/, not in the WAL.  A read failure here means the
    // extent index still points at a file the coordinator has already deleted.
    let read_handle = handle.clone();
    let oracle_snap = oracle.clone();
    let read_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let read_errors_clone = Arc::clone(&read_errors);

    let reader = thread::spawn(move || {
        for _ in 0..500 {
            for lba in 0u64..8 {
                match read_handle.read(lba, 1) {
                    Ok(actual) => {
                        if let Some(expected) = oracle_snap.get(&lba) {
                            if actual != *expected {
                                read_errors_clone
                                    .lock()
                                    .unwrap()
                                    .push(format!("lba {lba}: wrong data"));
                            }
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

        let (repack_ulid, _) = gc_handle.gc_checkpoint().unwrap();
        let gc_ulid = ulid::Ulid::from_string(&repack_ulid).unwrap();
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
    for (lba, expected) in &oracle {
        let actual = handle.read(*lba, 1).unwrap();
        assert_eq!(actual, *expected, "lba {lba} wrong in final check");
    }

    handle.shutdown();
    actor_thread.join().unwrap();
}
