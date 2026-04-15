// Integration tests for file-aware import.
//
// These tests build a tiny ext4 image from a temp directory using
// `mke2fs -d` (e2fsprogs) and run `import_image` against it, then
// verify the produced filemap and segment entries.
//
// If `mke2fs` is not available (no e2fsprogs installed) the tests are
// skipped with a warning. This keeps CI portable while still giving
// meaningful coverage on developer machines. A follow-up should
// commit a small pre-built ext4 fixture so the tests don't depend on
// a system tool at all — tracked as a TODO.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use elide_core::filemap;
use elide_core::import::import_image;
use elide_core::segment::{self, EntryKind};
use elide_core::signing;
use elide_core::volume::Volume;
use tempfile::TempDir;

mod common;

/// Returns the resolved `mke2fs` binary path, or `None` if not found.
fn find_mke2fs() -> Option<String> {
    if Command::new("mke2fs").arg("--version").output().is_ok() {
        return Some("mke2fs".to_owned());
    }
    if cfg!(target_os = "macos") {
        let keg = "/opt/homebrew/opt/e2fsprogs/sbin/mke2fs";
        if Path::new(keg).exists() {
            return Some(keg.to_owned());
        }
    }
    None
}

/// Build a small ext4 image from a rootfs directory via `mke2fs -d`.
fn build_ext4_image(rootfs: &Path, out: &Path, size_bytes: u64) {
    let mke2fs = match find_mke2fs() {
        Some(p) => p,
        None => {
            eprintln!("SKIP: mke2fs not available; cannot build ext4 fixture");
            std::process::exit(0);
        }
    };
    let rootfs_str = rootfs.to_str().unwrap();
    let out_str = out.to_str().unwrap();
    let size_k = format!("{}K", size_bytes / 1024);
    // Force 4 KiB blocks; mke2fs defaults to 1 KiB blocks on tiny
    // filesystems (< 512 MiB), which the ext4_scan loader rejects.
    let status = Command::new(&mke2fs)
        .args([
            "-t", "ext4", "-b", "4096", "-d", rootfs_str, out_str, &size_k,
        ])
        .status()
        .expect("run mke2fs");
    assert!(status.success(), "mke2fs failed: {status}");
}

/// Create a new volume directory with an ephemeral signer and return
/// the signer along with the dir path.
fn setup_volume_dir(tmp: &TempDir) -> (PathBuf, std::sync::Arc<dyn segment::SegmentSigner>) {
    let vol_dir = tmp.path().join("vol");
    fs::create_dir_all(&vol_dir).unwrap();
    // generate_keypair writes volume.key + volume.pub; import_image
    // only needs volume.pub for signing-identity bootstrap, but the
    // easiest way to get a matching pub-on-disk + signer pair in a
    // test is to use the keypair helper and then load the signer.
    let key =
        signing::generate_keypair(&vol_dir, signing::VOLUME_KEY_FILE, signing::VOLUME_PUB_FILE)
            .unwrap();
    // Phase 4 filemap generation walks the provenance chain via
    // BlockReader::open_snapshot, which requires volume.provenance — write
    // an empty (root) lineage so the chain terminates immediately.
    signing::write_provenance(
        &vol_dir,
        &key,
        signing::VOLUME_PROVENANCE_FILE,
        &signing::ProvenanceLineage::default(),
    )
    .unwrap();
    let signer = signing::load_signer(&vol_dir, signing::VOLUME_KEY_FILE).unwrap();
    // Volume config must exist for import_image's size-field write.
    elide_core::config::VolumeConfig {
        name: Some("test".to_owned()),
        size: None,
        nbd: None,
    }
    .write(&vol_dir)
    .unwrap();
    (vol_dir, signer)
}

/// Parse a filemap file, returning (path, file_offset, hash_hex, byte_count) tuples.
fn parse_filemap(path: &Path) -> Vec<(String, u64, String, u64)> {
    let content = fs::read_to_string(path).expect("read filemap");
    let mut out = Vec::new();
    for line in content.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 4, "bad filemap row: {line}");
        out.push((
            parts[0].to_owned(),
            parts[1].parse().unwrap(),
            parts[2].to_owned(),
            parts[3].parse().unwrap(),
        ));
    }
    out
}

/// Find the snapshot ULID inside `vol_dir/snapshots`, skipping `.filemap` files.
fn find_snapshot_ulid(snap_dir: &Path) -> String {
    for entry in fs::read_dir(snap_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if !name.contains('.') && ulid::Ulid::from_string(&name).is_ok() {
            return name;
        }
    }
    panic!("no snapshot ULID found in {}", snap_dir.display());
}

/// Read all segment entries from `vol_dir/pending/*`, returning a flat
/// Vec of (ulid, entry) pairs in pending-dir order.
fn read_all_pending_entries(vol_dir: &Path) -> Vec<(String, segment::SegmentEntry)> {
    let pending = vol_dir.join("pending");
    let mut segs: Vec<PathBuf> = fs::read_dir(&pending)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_none())
        .collect();
    segs.sort();
    let mut out = Vec::new();
    for seg_path in segs {
        let ulid = seg_path.file_name().unwrap().to_str().unwrap().to_owned();
        let (_, entries, _) = segment::read_segment_index(&seg_path).unwrap();
        for e in entries {
            out.push((ulid.clone(), e));
        }
    }
    out
}

#[test]
fn import_emits_file_sized_extents_and_filemap_v2() {
    let tmp = TempDir::new().unwrap();
    let rootfs = tmp.path().join("rootfs");
    fs::create_dir_all(&rootfs).unwrap();

    // Two regular files of different sizes; a small config file likely
    // fits in one block, the larger binary fits in several.
    let small = b"hello world\n".to_vec(); // 12 bytes, one-block fragment
    let medium = vec![0xABu8; 40_000]; // ~10 blocks
    fs::write(rootfs.join("greeting"), &small).unwrap();
    fs::write(rootfs.join("blob"), &medium).unwrap();

    let image_path = tmp.path().join("image.ext4");
    build_ext4_image(&rootfs, &image_path, 8 * 1024 * 1024);

    let (vol_dir, signer) = setup_volume_dir(&tmp);
    import_image(&image_path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap();

    // Filemap exists, has v2 header, contains our two files.
    let snap_dir = vol_dir.join("snapshots");
    let snap_ulid = find_snapshot_ulid(&snap_dir);
    let filemap_path = snap_dir.join(format!("{snap_ulid}.filemap"));
    let filemap_contents = fs::read_to_string(&filemap_path).unwrap();
    assert!(
        filemap_contents.starts_with("# elide-filemap v2\n"),
        "filemap header wrong: {}",
        filemap_contents.lines().next().unwrap_or("")
    );

    let rows = parse_filemap(&filemap_path);
    let mut saw_greeting = false;
    let mut saw_blob = false;
    for (path, file_offset, _hash, byte_count) in &rows {
        if path == "/greeting" {
            saw_greeting = true;
            assert_eq!(*file_offset, 0);
            assert_eq!(*byte_count, small.len() as u64);
        }
        if path == "/blob" {
            saw_blob = true;
            assert_eq!(*file_offset, 0);
            assert_eq!(*byte_count, medium.len() as u64);
        }
    }
    assert!(
        saw_greeting,
        "greeting missing from filemap:\n{filemap_contents}"
    );
    assert!(saw_blob, "blob missing from filemap:\n{filemap_contents}");

    // Every filemap fragment hash must be present in some segment
    // entry that carries real bytes (Data or Inline — the segment
    // writer may promote small compressed bodies to the inline
    // section). No parent extent index is set, so DedupRef is
    // impossible.
    let entries = read_all_pending_entries(&vol_dir);
    for (path, _file_offset, hash_hex, _byte_count) in &rows {
        let matched = entries.iter().any(|(_, e)| {
            e.hash.to_hex().to_string() == *hash_hex
                && (e.kind == EntryKind::Data || e.kind == EntryKind::Inline)
        });
        assert!(
            matched,
            "fragment hash {hash_hex} for {path} not found as Data/Inline entry in pending segments"
        );
    }

    // The blob is larger than one block, so its entry must have
    // lba_length > 1 — this is the defining property of file-aware
    // import versus the old block-granular path. 40_000 bytes needs
    // 10 4 KiB blocks.
    let blob_row = rows.iter().find(|(p, _, _, _)| p == "/blob").unwrap();
    let blob_entry = entries
        .iter()
        .find(|(_, e)| e.hash.to_hex().to_string() == blob_row.2)
        .unwrap();
    assert_eq!(
        blob_entry.1.lba_length,
        10,
        "blob should span 10 blocks ({} bytes); got lba_length={}",
        medium.len(),
        blob_entry.1.lba_length,
    );
}

/// Phase 4: regenerate the filemap for a sealed snapshot from segments alone.
///
/// Imports a small ext4 image (which writes the filemap as a side effect of
/// the import walk), drains all pending segments through the redact+promote
/// path so they live in `index/<ulid>.idx` + `cache/<ulid>.body`, deletes the
/// import-written filemap, then calls `filemap::generate_from_snapshot`. The
/// regenerated filemap must match the import-written one row-for-row,
/// including hashes — `generate_from_snapshot` looks up hashes via the LBA
/// map, so a mismatch would mean snapshot-time hashes don't agree with what
/// the import path stored in segment DATA entries.
#[test]
fn generate_from_snapshot_matches_import_filemap() {
    let tmp = TempDir::new().unwrap();
    let rootfs = tmp.path().join("rootfs");
    fs::create_dir_all(&rootfs).unwrap();

    fs::write(rootfs.join("greeting"), b"hello world\n").unwrap();
    fs::write(rootfs.join("blob"), vec![0xABu8; 40_000]).unwrap();
    fs::create_dir_all(rootfs.join("etc")).unwrap();
    fs::write(rootfs.join("etc").join("hosts"), b"127.0.0.1 localhost\n").unwrap();

    let image_path = tmp.path().join("image.ext4");
    build_ext4_image(&rootfs, &image_path, 8 * 1024 * 1024);

    let (vol_dir, signer) = setup_volume_dir(&tmp);
    import_image(&image_path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap();

    let snap_dir = vol_dir.join("snapshots");
    let snap_ulid_str = find_snapshot_ulid(&snap_dir);
    let snap_ulid = ulid::Ulid::from_string(&snap_ulid_str).unwrap();
    let filemap_path = snap_dir.join(format!("{snap_ulid_str}.filemap"));

    // Capture the import-written filemap, then drain pending → index/cache so
    // BlockReader::open_snapshot can rebuild from index/*.idx, then erase the
    // filemap so we know the regenerated one came from generate_from_snapshot.
    let import_rows = parse_filemap(&filemap_path);
    let import_text = fs::read_to_string(&filemap_path).unwrap();

    let mut vol = Volume::open(&vol_dir, vol_dir.parent().unwrap()).unwrap();
    common::drain_with_redact(&mut vol);
    drop(vol);

    fs::remove_file(&filemap_path).unwrap();

    let wrote = filemap::generate_from_snapshot(&vol_dir, &snap_ulid, Box::new(|_| None)).unwrap();
    assert!(
        wrote,
        "generate_from_snapshot returned false on an ext4 image"
    );
    assert!(
        filemap_path.exists(),
        "generate_from_snapshot did not write the filemap"
    );

    let phase4_rows = parse_filemap(&filemap_path);
    let phase4_text = fs::read_to_string(&filemap_path).unwrap();

    // Row sets must be equal; rows are sorted on write so byte-equality is
    // also a meaningful signal for "two identical filemaps from independent
    // producers".
    assert_eq!(
        phase4_rows.len(),
        import_rows.len(),
        "row count differs:\nimport:\n{import_text}\nphase4:\n{phase4_text}"
    );
    for row in &import_rows {
        assert!(
            phase4_rows.contains(row),
            "import row {row:?} missing from phase4 filemap:\n{phase4_text}"
        );
    }
    assert_eq!(
        phase4_text, import_text,
        "phase4 filemap should be byte-identical to import filemap (rows are pre-sorted)"
    );
}

#[test]
fn import_writes_snapshot_marker_and_volume_size() {
    let tmp = TempDir::new().unwrap();
    let rootfs = tmp.path().join("rootfs");
    fs::create_dir_all(&rootfs).unwrap();
    fs::write(rootfs.join("file"), b"x").unwrap();

    let image_path = tmp.path().join("image.ext4");
    build_ext4_image(&rootfs, &image_path, 2 * 1024 * 1024);

    let (vol_dir, signer) = setup_volume_dir(&tmp);
    import_image(&image_path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap();

    // Snapshot marker exists and is a valid ULID filename.
    let snap_dir = vol_dir.join("snapshots");
    let _ulid = find_snapshot_ulid(&snap_dir);

    // Volume size is recorded in volume.toml.
    let cfg = elide_core::config::VolumeConfig::read(&vol_dir).unwrap();
    assert_eq!(cfg.size, Some(2 * 1024 * 1024));
}
