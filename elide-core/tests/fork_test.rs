// Deterministic integration tests for fork ancestry isolation.
//
// For property-based fork tests see fork_proptest.rs.

use std::path::PathBuf;

use elide_core::volume::{Volume, fork_volume};

mod common;

/// Regression test: `fork_volume` must write the real ULID into the origin
/// file even when the source path is a symlink (e.g. `by_name/<name>`).
///
/// Before the fix, `fork_volume` called `file_name()` on the raw source path,
/// so a symlink named `"myvol"` produced `"myvol/snapshots/..."` in the origin
/// file.  `Volume::open` then rejected it: "parent 'myvol' is not a valid ULID".
#[test]
fn fork_via_symlink_writes_ulid_in_origin() {
    let dir = tempfile::TempDir::new().unwrap();
    let by_id = dir.path().join("by_id");
    let by_name = dir.path().join("by_name");
    std::fs::create_dir_all(&by_id).unwrap();
    std::fs::create_dir_all(&by_name).unwrap();

    let source_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let fork_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let source_dir = by_id.join(source_ulid);
    let fork_dir = by_id.join(fork_ulid);

    // Create source volume with a snapshot.
    common::write_test_keypair(&source_dir);
    let mut vol = Volume::open(&source_dir, &by_id).unwrap();
    vol.write(0, &[0xABu8; 4096]).unwrap();
    vol.flush_wal().unwrap();
    vol.snapshot().unwrap();
    drop(vol);

    // Simulate the by_name layout: a symlink named "myvol" → "../by_id/<ulid>".
    let symlink = by_name.join("myvol");
    std::os::unix::fs::symlink(format!("../by_id/{source_ulid}"), &symlink).unwrap();

    // Fork via the symlink path — this is what the CLI does.
    fork_volume(&fork_dir, &symlink).unwrap();

    // The origin file must contain the real ULID, not the symlink name.
    let origin = std::fs::read_to_string(fork_dir.join("volume.parent")).unwrap();
    assert!(
        origin.starts_with(source_ulid),
        "origin should start with the source ULID, got: {origin:?}"
    );

    // Volume::open on the fork must succeed (would fail with "not a valid ULID"
    // if the origin contained the symlink name instead).
    Volume::open(&fork_dir, &by_id).unwrap();
}

/// Verifies isolation across a three-level ancestry chain: root → child → grandchild.
///
/// Each level writes to distinct LBAs and takes a snapshot before forking.
/// After creating the grandchild, post-branch writes are made at the root and
/// child levels, then the grandchild is crashed and reopened.
///
/// Expected reads from grandchild after crash+rebuild:
///   LBAs 0-1  root values (pre-branch from root)
///   LBAs 2-3  child values (pre-branch from child)
///   LBAs 4-5  grandchild's own values
///   LBAs 6-7  zero (written to root/child post-branch; invisible to grandchild)
#[test]
fn three_level_fork_isolation() {
    let dir = tempfile::TempDir::new().unwrap();
    let by_id = dir.path().to_path_buf();
    let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
    let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
    let grandchild_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
    let root_dir: PathBuf = by_id.join(root_ulid);
    let child_dir: PathBuf = by_id.join(child_ulid);
    let grandchild_dir: PathBuf = by_id.join(grandchild_ulid);

    // --- root level ---
    common::write_test_keypair(&root_dir);
    let mut base = Volume::open(&root_dir, &by_id).unwrap();
    base.write(0, &[0xAA; 4096]).unwrap();
    base.write(1, &[0xBB; 4096]).unwrap();
    base.flush_wal().unwrap();
    base.snapshot().unwrap();

    fork_volume(&child_dir, &root_dir).unwrap();

    // Post-branch root write — must be invisible to child and grandchild.
    base.write(6, &[0xDE; 4096]).unwrap();
    base.flush_wal().unwrap();

    // --- child level ---
    let mut child = Volume::open(&child_dir, &by_id).unwrap();
    child.write(2, &[0xCC; 4096]).unwrap();
    child.write(3, &[0xDD; 4096]).unwrap();
    child.flush_wal().unwrap();
    child.snapshot().unwrap();

    fork_volume(&grandchild_dir, &child_dir).unwrap();

    // Post-branch child write — must be invisible to grandchild.
    child.write(7, &[0xEF; 4096]).unwrap();
    child.flush_wal().unwrap();

    // --- grandchild level ---
    let mut gc = Volume::open(&grandchild_dir, &by_id).unwrap();
    gc.write(4, &[0xEE; 4096]).unwrap();
    gc.write(5, &[0xFF; 4096]).unwrap();
    gc.flush_wal().unwrap();

    // Crash + reopen grandchild — walk_ancestors must traverse two levels.
    drop(gc);
    let gc = Volume::open(&grandchild_dir, &by_id).unwrap();

    // Ancestral data visible.
    assert_eq!(gc.read(0, 1).unwrap(), vec![0xAA; 4096], "lba 0 (root)");
    assert_eq!(gc.read(1, 1).unwrap(), vec![0xBB; 4096], "lba 1 (root)");
    assert_eq!(gc.read(2, 1).unwrap(), vec![0xCC; 4096], "lba 2 (child)");
    assert_eq!(gc.read(3, 1).unwrap(), vec![0xDD; 4096], "lba 3 (child)");

    // Grandchild's own writes visible.
    assert_eq!(
        gc.read(4, 1).unwrap(),
        vec![0xEE; 4096],
        "lba 4 (grandchild)"
    );
    assert_eq!(
        gc.read(5, 1).unwrap(),
        vec![0xFF; 4096],
        "lba 5 (grandchild)"
    );

    // Post-branch writes at root and child levels must be invisible.
    assert_eq!(
        gc.read(6, 1).unwrap(),
        vec![0u8; 4096],
        "lba 6 (post-branch root write)"
    );
    assert_eq!(
        gc.read(7, 1).unwrap(),
        vec![0u8; 4096],
        "lba 7 (post-branch child write)"
    );
}
