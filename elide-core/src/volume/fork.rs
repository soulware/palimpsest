//! Fork creation: branch a new writable volume from an existing volume's
//! latest snapshot (or an explicitly pinned snapshot ULID). Split out of
//! `volume/mod.rs` for legibility — no behaviour change.

use std::fs;
use std::io;
use std::path::Path;

use ulid::Ulid;

use super::readonly::latest_snapshot;

/// Create a new volume directory, branched from the latest snapshot of the source volume.
///
/// The source volume must have at least one snapshot (written by `snapshot()`).
/// `new_fork_dir` is created with `wal/` and `pending/`, a fresh keypair is
/// generated, and a signed `volume.provenance` is written recording the
/// fork's `parent` field in the form `<source-ulid>/snapshots/<branch-ulid>`.
/// The source ULID is derived from `source_fork_dir`'s directory name.
///
/// Returns `Ok(())` on success; `new_fork_dir` must not already exist.
pub fn fork_volume(new_fork_dir: &Path, source_fork_dir: &Path) -> io::Result<()> {
    let branch_ulid = latest_snapshot(source_fork_dir)?.ok_or_else(|| {
        io::Error::other(format!(
            "source volume '{}' has no snapshots; run snapshot-volume first",
            source_fork_dir.display()
        ))
    })?;
    fork_volume_at(new_fork_dir, source_fork_dir, branch_ulid)
}

/// Like `fork_volume` but pins the fork to an explicit snapshot ULID.
///
/// Used by `volume create --from <vol_ulid>/<snap_ulid>` when the caller
/// wants the branch point to be something other than the source volume's
/// latest snapshot — typically because the source is a pulled readonly
/// ancestor and the caller has a specific snapshot ULID in mind.
///
/// The snapshot is **not** required to exist as a local file: a pulled
/// readonly ancestor may not have its snapshot markers prefetched yet at
/// the time of forking. The snapshot ULID is still recorded in the child's
/// signed provenance and will be resolved at open time once prefetch has
/// populated the ancestor's `snapshots/` directory.
pub fn fork_volume_at(
    new_fork_dir: &Path,
    source_fork_dir: &Path,
    branch_ulid: Ulid,
) -> io::Result<()> {
    fork_volume_at_inner(new_fork_dir, source_fork_dir, branch_ulid, None)
}

/// Like `fork_volume_at` but also records a `manifest_pubkey` override in
/// the child's provenance. The parent's identity key (for verifying the
/// ancestor's own `volume.provenance` and `.idx` signatures) is still
/// loaded from the source's on-disk `volume.pub`; `manifest_pubkey` is
/// used **only** for the pinned snapshot's `.manifest`.
///
/// Used by `volume create --from --force-snapshot` when the forker doesn't hold the
/// source owner's private key and instead signs the synthetic manifest
/// with an ephemeral key. That ephemeral pubkey goes here; the ancestor's
/// own artefacts continue to verify under the owner's key.
pub fn fork_volume_at_with_manifest_key(
    new_fork_dir: &Path,
    source_fork_dir: &Path,
    branch_ulid: Ulid,
    manifest_pubkey: crate::signing::VerifyingKey,
) -> io::Result<()> {
    fork_volume_at_inner(
        new_fork_dir,
        source_fork_dir,
        branch_ulid,
        Some(manifest_pubkey),
    )
}

fn fork_volume_at_inner(
    new_fork_dir: &Path,
    source_fork_dir: &Path,
    branch_ulid: Ulid,
    manifest_pubkey: Option<crate::signing::VerifyingKey>,
) -> io::Result<()> {
    if new_fork_dir.exists() {
        return Err(io::Error::other(format!(
            "fork directory '{}' already exists",
            new_fork_dir.display()
        )));
    }

    // Canonicalize so that symlink paths (e.g. by_name/<name>) resolve to
    // their real by_id/<ulid> directory before we extract the ULID component.
    let source_real = fs::canonicalize(source_fork_dir)?;
    let source_ulid = source_real
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("source fork dir has no name"))?;
    // Validate the source directory name really is a ULID before we embed
    // it in the child's provenance as an ancestor reference.
    Ulid::from_string(source_ulid).map_err(|e| {
        io::Error::other(format!(
            "source fork dir name is not a ULID ({}): {e}",
            source_real.display()
        ))
    })?;

    fs::create_dir_all(new_fork_dir.join("wal"))?;
    fs::create_dir_all(new_fork_dir.join("pending"))?;

    // Generate a fresh keypair for the new fork. Every writable volume must have
    // a signing key; the fork gets its own identity independent of its parent.
    // The signing key's in-memory form is reused immediately to write provenance
    // so we never have to re-read it from disk.
    let key = crate::signing::generate_keypair(
        new_fork_dir,
        crate::signing::VOLUME_KEY_FILE,
        crate::signing::VOLUME_PUB_FILE,
    )?;

    // Write signed provenance carrying the fork's parent reference. Extent
    // index is empty for forks — fork ancestry is a read-path relationship
    // tracked in `parent`, not a hash-pool relationship.
    //
    // Embed the parent's identity pubkey (loaded from the source's on-disk
    // `volume.pub`) under the child's signature so the fork's open-time
    // ancestor walk has a trust anchor for the parent's own signed
    // artefacts — see `ParentRef` in signing.rs. If a manifest_pubkey was
    // supplied (force-snapshot path), also embed it as a narrow override
    // for the pinned `.manifest` only.
    let parent_pubkey =
        crate::signing::load_verifying_key(&source_real, crate::signing::VOLUME_PUB_FILE)?;
    let lineage = crate::signing::ProvenanceLineage {
        parent: Some(crate::signing::ParentRef {
            volume_ulid: source_ulid.to_owned(),
            snapshot_ulid: branch_ulid.to_string(),
            pubkey: parent_pubkey.to_bytes(),
            manifest_pubkey: manifest_pubkey.map(|k| k.to_bytes()),
        }),
        extent_index: Vec::new(),
    };
    crate::signing::write_provenance(
        new_fork_dir,
        &key,
        crate::signing::VOLUME_PROVENANCE_FILE,
        &lineage,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::Volume;
    use super::super::test_util::*;
    use super::*;
    use std::fs;

    #[test]
    fn fork_volume_creates_fork_with_signed_provenance() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(root_ulid);
        let fork_dir = by_id.join(child_ulid);
        write_test_keypair(&default_dir);

        // snapshot default to give it a branch point.
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        let snap_ulid = vol.snapshot().unwrap().to_string();
        drop(vol);

        // Create the fork.
        fork_volume(&fork_dir, &default_dir).unwrap();
        assert!(fork_dir.join("wal").is_dir());
        assert!(fork_dir.join("pending").is_dir());
        assert!(
            !fork_dir.join("volume.parent").exists(),
            "standalone volume.parent file must not exist; parent lives in provenance"
        );

        // Parent lineage must be present in the signed provenance file.
        let lineage = crate::signing::read_lineage_verifying_signature(
            &fork_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let parent = lineage.parent.expect("fork must record parent");
        assert_eq!(parent.volume_ulid, root_ulid);
        assert_eq!(parent.snapshot_ulid, snap_ulid);
        assert!(lineage.extent_index.is_empty());

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_errors_if_source_has_no_snapshots() {
        let by_id = temp_dir();
        let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        // Create root_dir so canonicalize() succeeds, but leave it without
        // a snapshots/ directory so latest_snapshot returns "no snapshots".
        fs::create_dir_all(&root_dir).unwrap();
        let err = fork_volume(&child_dir, &root_dir).unwrap_err();
        assert!(err.to_string().contains("no snapshots"), "{err}");
    }

    #[test]
    fn fork_volume_at_pins_explicit_snapshot_without_requiring_local_marker() {
        // Simulate forking from a readonly ancestor: the source dir exists
        // but has no snapshots/ directory (prefetch hasn't landed yet). The
        // explicit pin must still succeed and be recorded in provenance.
        //
        // The source still needs its `volume.pub` locally so the fork can
        // embed the parent pubkey in its signed provenance — volume.pub is
        // pulled by the prefetch pathway before any fork operation.
        let by_id = temp_dir();
        let source_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let source_dir = by_id.join(source_ulid);
        let child_dir = by_id.join(child_ulid);
        fs::create_dir_all(&source_dir).unwrap();
        write_test_keypair(&source_dir);

        let branch_ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        fork_volume_at(&child_dir, &source_dir, branch_ulid).unwrap();

        let lineage = crate::signing::read_lineage_verifying_signature(
            &child_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let parent = lineage.parent.expect("fork must record parent");
        assert_eq!(parent.volume_ulid, source_ulid);
        assert_eq!(parent.snapshot_ulid, branch_ulid.to_string());

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_at_rejects_non_ulid_source_dir() {
        let tmp = temp_dir();
        let source_dir = tmp.join("not-a-ulid");
        let child_dir = tmp.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        fs::create_dir_all(&source_dir).unwrap();
        let branch_ulid = Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
        let err = fork_volume_at(&child_dir, &source_dir, branch_ulid).unwrap_err();
        assert!(err.to_string().contains("ULID"), "{err}");
        fs::remove_dir_all(tmp).ok();
    }

    #[test]
    fn fork_volume_uses_latest_snapshot_when_multiple_exist() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(root_ulid);
        let fork_dir = by_id.join(child_ulid);
        write_test_keypair(&default_dir);

        let data_snap1 = vec![0x11u8; 4096];
        let data_snap2 = vec![0x22u8; 4096];

        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        // First snapshot — should NOT be the branch point.
        vol.write(0, &data_snap1).unwrap();
        let snap1 = vol.snapshot().unwrap().to_string();
        // Second snapshot — should be the branch point.
        vol.write(1, &data_snap2).unwrap();
        let snap2 = vol.snapshot().unwrap().to_string();
        drop(vol);

        // snap2 must sort after snap1 (ULIDs are monotonically increasing).
        assert!(snap2 > snap1);

        fork_volume(&fork_dir, &default_dir).unwrap();
        let lineage = crate::signing::read_lineage_verifying_signature(
            &fork_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let parent = lineage.parent.expect("fork must record parent");
        assert_eq!(parent.volume_ulid, root_ulid);
        assert_eq!(
            parent.snapshot_ulid, snap2,
            "provenance parent should point to the latest snapshot"
        );

        // Fork branched from snap2 sees both pre-snap1 and pre-snap2 writes.
        let vol = Volume::open(&fork_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_snap1);
        assert_eq!(vol.read(1, 1).unwrap(), data_snap2);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_from_child_fork_creates_three_level_chain() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let default_dir = by_id.join(root_ulid);
        let mid_dir = by_id.join(mid_ulid);
        let leaf_dir = by_id.join(leaf_ulid);
        write_test_keypair(&default_dir);

        let data_root = vec![0xAAu8; 4096];
        let data_mid = vec![0xBBu8; 4096];
        let data_leaf = vec![0xCCu8; 4096];

        // Root volume: write + snapshot.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data_root).unwrap();
            vol.snapshot().unwrap();
        }

        // Mid volume: branch from default, write + snapshot.
        fork_volume(&mid_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&mid_dir, &by_id).unwrap();
            vol.write(1, &data_mid).unwrap();
            vol.snapshot().unwrap();
        }

        // Leaf volume: branch from mid.
        fork_volume(&leaf_dir, &mid_dir).unwrap();

        // origin chain: leaf → mid → default (read from signed provenance).
        let leaf_lineage = crate::signing::read_lineage_verifying_signature(
            &leaf_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let leaf_parent = leaf_lineage.parent.expect("leaf must record parent");
        assert_eq!(leaf_parent.volume_ulid, mid_ulid);
        let mid_lineage = crate::signing::read_lineage_verifying_signature(
            &mid_dir,
            crate::signing::VOLUME_PUB_FILE,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )
        .unwrap();
        let mid_parent = mid_lineage.parent.expect("mid must record parent");
        assert_eq!(mid_parent.volume_ulid, root_ulid);

        // Leaf sees data from all three levels.
        let vol = Volume::open(&leaf_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_root);
        assert_eq!(vol.read(1, 1).unwrap(), data_mid);
        assert_eq!(vol.read(2, 1).unwrap(), vec![0u8; 4096]); // unwritten

        // Write to leaf does not affect mid or default.
        drop(vol);
        {
            let mut vol = Volume::open(&leaf_dir, &by_id).unwrap();
            vol.write(2, &data_leaf).unwrap();
        }
        let vol = Volume::open(&leaf_dir, &by_id).unwrap();
        assert_eq!(vol.read(2, 1).unwrap(), data_leaf);
        assert_eq!(vol.ancestor_count(), 2);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_errors_if_fork_exists() {
        let by_id = temp_dir();
        let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&root_dir);
        let mut vol = Volume::open(&root_dir, &by_id).unwrap();
        vol.snapshot().unwrap();
        drop(vol);

        fork_volume(&child_dir, &root_dir).unwrap();
        let err = fork_volume(&child_dir, &root_dir).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");

        fs::remove_dir_all(by_id).unwrap();
    }
}
