// End-to-end coverage of the volume-side GC plan materialisation path.
//
// These tests build a real Volume, drive it to some on-disk state, hand-craft
// a `gc/<ulid>.plan` file, call `apply_gc_handoffs`, and verify the bare
// `gc/<ulid>` body appears and reads still round-trip.
//
// Unlike the coordinator-side tests, these do not involve any object store or
// IPC — the volume resolves bodies purely from its own local cache + WAL +
// pending + index directories.

use std::fs;
use std::path::PathBuf;

use elide_core::gc_plan::{GcPlan, PlanOutput};
use elide_core::volume::Volume;
use ulid::Ulid;

mod common;

/// Drive a volume to contain two single-LBA segments (LBA 0 and LBA 1), then
/// hand-write a plan that keeps both. The volume must materialise a bare
/// `gc/<ulid>` containing equivalent entries and reads must still return the
/// original bytes.
#[test]
fn plan_keep_two_data_entries_round_trips() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Two write-drain cycles produce two committed segments (each with one
    // fully-alive Data entry).
    vol.write(0, &[0xAA; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    vol.write(1, &[0xBB; 4096]).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    let index_dir = fork_dir.join("index");
    let gc_dir = fork_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let mut input_ulids: Vec<Ulid> = fs::read_dir(&index_dir)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name.strip_suffix(".idx")?;
            Ulid::from_string(stem).ok()
        })
        .collect();
    input_ulids.sort();
    assert_eq!(input_ulids.len(), 2, "expected two committed segments");

    // Mint a fresh ULID for the new GC output (must be > all inputs so
    // rebuild ordering keeps the new entries authoritative). Use
    // `gc_checkpoint` so the volume's ULID mint advances consistently.
    let (new_ulid, _sweep) = vol.gc_checkpoint().unwrap();

    let plan = GcPlan {
        new_ulid,
        outputs: vec![
            PlanOutput::Keep {
                input: input_ulids[0],
                entry_idx: 0,
            },
            PlanOutput::Keep {
                input: input_ulids[1],
                entry_idx: 0,
            },
        ],
    };
    let plan_path = gc_dir.join(format!("{new_ulid}.plan"));
    plan.write_atomic(&plan_path).unwrap();

    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0, "plan must have been applied");

    // Bare gc/<new> appears; .plan is gone.
    assert!(
        gc_dir.join(new_ulid.to_string()).exists(),
        "bare gc/<new> must be present after apply"
    );
    assert!(
        !plan_path.exists(),
        "plan file must be removed after successful apply"
    );

    // Reads still work through the live in-memory Volume.
    assert_eq!(vol.read(0, 1).unwrap().as_slice(), &[0xAA; 4096]);
    assert_eq!(vol.read(1, 1).unwrap().as_slice(), &[0xBB; 4096]);

    // Reopen (which rebuilds extent index from disk) and re-verify.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(vol.read(0, 1).unwrap().as_slice(), &[0xAA; 4096]);
    assert_eq!(vol.read(1, 1).unwrap().as_slice(), &[0xBB; 4096]);
}

/// A plan referencing an input whose `.idx` doesn't exist must be cancelled
/// (removed) rather than fail the apply pass. The coordinator retries next
/// tick if the input reappears.
#[test]
fn plan_missing_input_is_cancelled() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    let gc_dir = fork_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let new_ulid = Ulid::new();
    let phantom_input = Ulid::new();
    let plan = GcPlan {
        new_ulid,
        outputs: vec![PlanOutput::Drop {
            input: phantom_input,
        }],
    };
    let plan_path = gc_dir.join(format!("{new_ulid}.plan"));
    plan.write_atomic(&plan_path).unwrap();

    // apply_gc_handoffs returns the number of handoffs processed — a
    // cancelled plan counts as 0.
    let _applied = vol.apply_gc_handoffs().unwrap();

    assert!(
        !plan_path.exists(),
        "cancelled plan must be removed so the coordinator can retry"
    );
    assert!(
        !gc_dir.join(new_ulid.to_string()).exists(),
        "no bare output should be produced when the plan is cancelled"
    );
}

/// A plan with zero inputs is malformed (a GC output always consumes at
/// least one input) and must be cancelled.
#[test]
fn plan_with_empty_inputs_is_cancelled() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    let gc_dir = fork_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let new_ulid = Ulid::new();
    let plan = GcPlan {
        new_ulid,
        outputs: vec![],
    };
    let plan_path = gc_dir.join(format!("{new_ulid}.plan"));
    plan.write_atomic(&plan_path).unwrap();

    let _applied = vol.apply_gc_handoffs().unwrap();
    assert!(!plan_path.exists());
    assert!(!gc_dir.join(new_ulid.to_string()).exists());
}

/// Partial-LBA-death Data: parent LBA 0..4 with an overwrite at LBA 2. Plan
/// expands the parent into three sub-runs ([0..2), LBA 2 overwritten,
/// [3..4)). After materialisation, reads must still return the parent bytes
/// for surviving LBAs and the overwrite bytes for LBA 2.
#[test]
fn plan_partial_death_data_reconstructs_sub_runs() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir: PathBuf = dir.path().to_owned();
    common::write_test_keypair(&fork_dir);
    let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();

    // Parent: 4 × 4 KiB with distinguishable content per-LBA.
    let mut parent_bytes = vec![0u8; 4 * 4096];
    for (lba, chunk) in parent_bytes.chunks_mut(4096).enumerate() {
        chunk.fill(0x10 + lba as u8);
    }
    vol.write(0, &parent_bytes).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    // Overwrite LBA 2.
    let overwrite = vec![0xEE; 4096];
    vol.write(2, &overwrite).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);

    let index_dir = fork_dir.join("index");
    let gc_dir = fork_dir.join("gc");
    fs::create_dir_all(&gc_dir).unwrap();

    let mut input_ulids: Vec<Ulid> = fs::read_dir(&index_dir)
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let stem = name.strip_suffix(".idx")?;
            Ulid::from_string(stem).ok()
        })
        .collect();
    input_ulids.sort();
    assert_eq!(input_ulids.len(), 2);

    let parent_ulid = input_ulids[0];
    let overwrite_ulid = input_ulids[1];

    // Parent segment has one Data entry at idx 0 covering LBAs 0..4.
    // Build a partial plan: one `canonical` preserving the composite body
    // (parent_hash is still LBA-live via surviving sub-runs, so the
    // stale-liveness check requires the hash to be carried) plus two
    // `run` lines — [0..2) and [3..4).
    let (new_ulid, _sweep) = vol.gc_checkpoint().unwrap();
    let plan = GcPlan {
        new_ulid,
        outputs: vec![
            PlanOutput::Canonical {
                input: parent_ulid,
                entry_idx: 0,
            },
            PlanOutput::Run {
                input: parent_ulid,
                entry_idx: 0,
                payload_block_offset: 0,
                start_lba: 0,
                lba_length: 2,
            },
            PlanOutput::Run {
                input: parent_ulid,
                entry_idx: 0,
                payload_block_offset: 3,
                start_lba: 3,
                lba_length: 1,
            },
            PlanOutput::Keep {
                input: overwrite_ulid,
                entry_idx: 0,
            },
        ],
    };
    let plan_path = gc_dir.join(format!("{new_ulid}.plan"));
    plan.write_atomic(&plan_path).unwrap();

    let applied = vol.apply_gc_handoffs().unwrap();
    assert!(applied > 0, "plan must have been applied");
    assert!(gc_dir.join(new_ulid.to_string()).exists());
    assert!(!plan_path.exists());

    // Reads must round-trip: parent bytes for surviving LBAs, overwrite for LBA 2.
    assert_eq!(
        vol.read(0, 1).unwrap().as_slice(),
        &parent_bytes[0..4096],
        "LBA 0"
    );
    assert_eq!(
        vol.read(1, 1).unwrap().as_slice(),
        &parent_bytes[4096..8192],
        "LBA 1"
    );
    assert_eq!(
        vol.read(2, 1).unwrap().as_slice(),
        overwrite.as_slice(),
        "LBA 2"
    );
    assert_eq!(
        vol.read(3, 1).unwrap().as_slice(),
        &parent_bytes[12288..16384],
        "LBA 3"
    );

    // Reopen and re-verify so the rebuild path is exercised too.
    drop(vol);
    let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
    assert_eq!(vol.read(0, 1).unwrap().as_slice(), &parent_bytes[0..4096]);
    assert_eq!(
        vol.read(1, 1).unwrap().as_slice(),
        &parent_bytes[4096..8192]
    );
    assert_eq!(vol.read(2, 1).unwrap().as_slice(), overwrite.as_slice());
    assert_eq!(
        vol.read(3, 1).unwrap().as_slice(),
        &parent_bytes[12288..16384]
    );
}
