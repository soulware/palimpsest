// Import an ext4 disk image into an Elide readonly volume.
//
// Reads the image in 4 KiB LBA-aligned blocks. Zero blocks are skipped — the
// volume read path returns zeros for unwritten LBAs, so they need no storage.
// Non-zero blocks are hashed, optionally compressed, and batched into large
// segment files written directly to `<vol_dir>/pending/` via the
// standard tmp-rename commit pattern (no WAL involved — all data is known
// upfront so crash recovery reduces to "retry the import").
//
// After all segments are written a snapshot marker is created in
// `<vol_dir>/base/snapshots/`. This ULID is the branch point for writable
// forks created with `fork-volume`. The volume root then gets `readonly` and
// `size` marker files, matching the layout described in docs/architecture.md.

use std::fs;
use std::io::{self, Read};
use std::path::Path;

use ulid::Ulid;

use crate::extentindex::ExtentIndex;
use crate::segment::{self, SegmentEntry, SegmentFlags, SegmentSigner};

const LBA_SIZE: usize = 4096;
const ZERO_BLOCK: [u8; LBA_SIZE] = [0u8; LBA_SIZE];

/// Soft cap on raw (uncompressed) data accumulated before flushing a segment.
///
/// Large segments amortise per-file overhead and make efficient S3 objects.
/// The cap is soft: the last block in a batch may push slightly past it.
const IMPORT_SEGMENT_BYTES: usize = 256 * 1024 * 1024; // 256 MiB raw

// Compression helpers — same logic as volume.rs, kept local to avoid coupling.

fn shannon_entropy(data: &[u8]) -> f64 {
    let mut counts = [0u32; 256];
    for &b in data {
        counts[b as usize] += 1;
    }
    let len = data.len() as f64;
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

/// Attempt lz4 compression on a single block.
///
/// Returns `Some(compressed)` only if entropy is below 7.0 bits/byte and the
/// result achieves at least a 1.5× ratio; otherwise returns `None` (store raw).
fn maybe_compress(block: &[u8]) -> Option<Vec<u8>> {
    if shannon_entropy(block) > 7.0 {
        return None;
    }
    let compressed = lz4_flex::compress_prepend_size(block);
    // Require at least 1.5× ratio (compressed × 3/2 < raw).
    if compressed.len() * 3 / 2 >= block.len() {
        return None;
    }
    Some(compressed)
}

/// Write `entries` to `pending/<ulid>` using the standard tmp-rename commit.
/// Clears `entries` on success. Returns the ULID used, or `None` if there was
/// nothing to write.
fn flush_segment(
    segments_dir: &Path,
    entries: &mut Vec<SegmentEntry>,
    signer: &dyn SegmentSigner,
) -> io::Result<Option<String>> {
    if entries.is_empty() {
        return Ok(None);
    }
    let ulid = Ulid::new().to_string();
    let tmp = segments_dir.join(format!("{ulid}.tmp"));
    let final_path = segments_dir.join(&ulid);
    segment::write_segment(&tmp, entries, signer)?;
    fs::rename(&tmp, &final_path)?;
    segment::fsync_dir(&final_path)?;
    entries.clear();
    Ok(Some(ulid))
}

/// Import an ext4 disk image into a new readonly Elide volume at `vol_dir`.
///
/// Creates `<vol_dir>/pending/` and `<vol_dir>/snapshots/`, reads
/// `image_path` in 4 KiB blocks, and writes segment files directly into
/// `pending/`. After all data is written, writes a snapshot marker (branch
/// point for future forks) and the `volume.size` marker.
///
/// `progress` receives `(blocks_done, total_blocks)` after each block is
/// processed. Pass a no-op closure if progress reporting is not needed.
///
/// # Errors
///
/// Returns an error if the image size is not a multiple of 4096, if any I/O
/// operation fails, or if `vol_dir` cannot be created.
pub fn import_image(
    image_path: &Path,
    vol_dir: &Path,
    signer: &dyn SegmentSigner,
    parent_extent_index: Option<&ExtentIndex>,
    mut progress: impl FnMut(u64, u64),
) -> io::Result<()> {
    // `vol_dir` may already exist (caller may have created it to write key files
    // before calling import). Fail only if pending/ already exists, which means
    // a previous import completed or is in progress.
    if vol_dir.join("pending").exists() {
        return Err(io::Error::other(format!(
            "volume already has pending segments: {}",
            vol_dir.display()
        )));
    }

    let image_size = fs::metadata(image_path)?.len();
    if image_size % LBA_SIZE as u64 != 0 {
        return Err(io::Error::other("image size is not a multiple of 4096"));
    }
    let total_blocks = image_size / LBA_SIZE as u64;

    // Write to pending/ so the coordinator's normal drain loop picks them up,
    // uploads to the store, and writes index/<ulid>.idx + cache/<ulid>.{body,present} — same path as WAL flushes.
    let segments_dir = vol_dir.join("pending");
    let snapshots_dir = vol_dir.join("snapshots");
    fs::create_dir_all(&segments_dir)?;
    fs::create_dir_all(&snapshots_dir)?;

    let mut image = fs::File::open(image_path)?;
    let mut block = [0u8; LBA_SIZE];
    let mut entries: Vec<SegmentEntry> = Vec::new();
    let mut batch_raw_bytes: usize = 0;
    let mut last_segment_ulid: Option<String> = None;

    for lba in 0..total_blocks {
        image.read_exact(&mut block)?;
        progress(lba + 1, total_blocks);

        if block == ZERO_BLOCK {
            continue;
        }

        let hash = blake3::hash(&block);

        // Parent dedup: if this block's hash already exists in a parent
        // volume's extent index (populated from `volume.extent_index` by the
        // caller), emit a thin DedupRef instead of a fresh Data entry. The
        // parent segment supplies the body bytes at read time.
        let parent_hit = parent_extent_index.and_then(|p| p.lookup(&hash));
        if let Some(loc) = parent_hit {
            entries.push(SegmentEntry::new_dedup_ref(
                hash,
                lba,
                1,
                loc.body_length,
                loc.compressed,
            ));
        } else {
            let (flags, data) = match maybe_compress(&block) {
                Some(compressed) => (SegmentFlags::COMPRESSED, compressed),
                None => (SegmentFlags::empty(), block.to_vec()),
            };
            entries.push(SegmentEntry::new_data(hash, lba, 1, flags, data));
        }
        batch_raw_bytes += LBA_SIZE;

        if batch_raw_bytes >= IMPORT_SEGMENT_BYTES {
            if let Some(ulid) = flush_segment(&segments_dir, &mut entries, signer)? {
                last_segment_ulid = Some(ulid);
            }
            batch_raw_bytes = 0;
        }
    }
    if let Some(ulid) = flush_segment(&segments_dir, &mut entries, signer)? {
        last_segment_ulid = Some(ulid);
    }

    // Snapshot marker reuses the last segment's ULID so the branch point is
    // self-describing. Falls back to a fresh ULID only for all-zero images
    // where no segments were written.
    let snap_ulid = last_segment_ulid.unwrap_or_else(|| Ulid::new().to_string());
    fs::write(snapshots_dir.join(&snap_ulid), "")?;

    // Write size into volume.toml (read-modify-write to preserve name if already set).
    let mut cfg = crate::config::VolumeConfig::read(vol_dir)?;
    cfg.size = Some(image_size);
    cfg.write(vol_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signing;
    use std::io::Write;
    use tempfile::TempDir;

    fn make_image(blocks: &[&[u8; LBA_SIZE]]) -> (TempDir, std::path::PathBuf) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.ext4");
        let mut f = fs::File::create(&path).unwrap();
        for b in blocks {
            f.write_all(*b).unwrap();
        }
        (tmp, path)
    }

    /// Write `volume.pub` into `dir` using an ephemeral keypair.
    /// Returns the signer so the caller can sign segments with it.
    fn setup_vol_pub(dir: &std::path::Path) -> std::sync::Arc<dyn SegmentSigner> {
        fs::create_dir_all(dir).unwrap();
        let (signer, vk) = signing::generate_ephemeral_signer();
        let pub_hex = signing::encode_hex(&vk.to_bytes()) + "\n";
        segment::write_file_atomic(&dir.join(signing::VOLUME_PUB_FILE), pub_hex.as_bytes())
            .unwrap();
        signer
    }

    #[test]
    fn import_creates_volume_layout() {
        let data_block: [u8; LBA_SIZE] = [0x42u8; LBA_SIZE];
        let zero_block: [u8; LBA_SIZE] = [0u8; LBA_SIZE];
        let (_src_tmp, image_path) = make_image(&[&data_block, &zero_block, &data_block]);

        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("testimport");
        let signer = setup_vol_pub(&vol_dir);
        import_image(&image_path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap();

        // readonly is now in meta.toml (written by caller, not import_image)
        assert!(!vol_dir.join("readonly").exists());
        assert_eq!(
            crate::config::VolumeConfig::read(&vol_dir).unwrap().size,
            Some((LBA_SIZE * 3) as u64)
        );
        assert!(vol_dir.join("pending").exists());
        // import_image writes to pending/ only; cache/ is written by the coordinator
        // after S3 upload (promote IPC). Verify import does not skip that step.
        assert!(!vol_dir.join("cache").exists());
        // Readonly volumes must have volume.pub (for segment verification) but
        // must NOT have volume.key (private key must never be written to disk).
        assert!(
            vol_dir.join(signing::VOLUME_PUB_FILE).exists(),
            "volume.pub must exist"
        );
        assert!(
            !vol_dir.join(signing::VOLUME_KEY_FILE).exists(),
            "volume.key must not exist on readonly volume"
        );

        // Exactly one snapshot marker, and its ULID matches the segment ULID.
        let segs: Vec<_> = fs::read_dir(vol_dir.join("pending"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(segs.len(), 1);
        let seg_name = segs[0].file_name().into_string().unwrap();

        let snaps: Vec<_> = fs::read_dir(vol_dir.join("snapshots"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(snaps.len(), 1);
        let snap_name = snaps[0].file_name().into_string().unwrap();

        assert_eq!(
            snap_name, seg_name,
            "snapshot ULID must match last segment ULID"
        );
    }

    #[test]
    fn import_skips_zero_blocks() {
        let zero_image: Vec<[u8; LBA_SIZE]> = vec![[0u8; LBA_SIZE]; 4];
        let blocks: Vec<&[u8; LBA_SIZE]> = zero_image.iter().collect();
        let (_src_tmp, image_path) = make_image(&blocks);

        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("zeroimport");
        let signer = setup_vol_pub(&vol_dir);
        import_image(&image_path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap();

        // All-zero image: no segment files should be written.
        let segs: Vec<_> = fs::read_dir(vol_dir.join("pending")).unwrap().collect();
        assert_eq!(segs.len(), 0);
    }

    #[test]
    fn import_rejects_unaligned_image() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.ext4");
        fs::write(&path, vec![0u8; 100]).unwrap();

        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("bad");
        let signer = setup_vol_pub(&vol_dir);
        let err = import_image(&path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap_err();
        assert!(err.to_string().contains("multiple of 4096"));
    }

    #[test]
    fn import_with_parent_emits_dedup_refs() {
        // Parent import writes two distinct data blocks.
        let b_shared: [u8; LBA_SIZE] = [0xAAu8; LBA_SIZE];
        let b_parent_only: [u8; LBA_SIZE] = [0xBBu8; LBA_SIZE];
        let b_child_only: [u8; LBA_SIZE] = [0xCCu8; LBA_SIZE];

        let (_src_parent, parent_image) = make_image(&[&b_shared, &b_parent_only]);
        let (_src_child, child_image) = make_image(&[&b_shared, &b_child_only]);

        // Parent volume: its own signing key + import.
        let vol_tmp = TempDir::new().unwrap();
        let by_id_dir = vol_tmp.path();
        let parent_dir = by_id_dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let parent_signer = setup_vol_pub(&parent_dir);
        import_image(
            &parent_image,
            &parent_dir,
            parent_signer.as_ref(),
            None,
            |_, _| {},
        )
        .unwrap();

        // Build the parent's extent index directly from its pending/ segments.
        // rebuild walks pending/ + index/ + cache/ and verifies signatures
        // using volume.pub, which setup_vol_pub wrote.
        let parent_extent_index = crate::extentindex::rebuild(&[(parent_dir.clone(), None)])
            .expect("parent extent index rebuild");
        assert!(
            parent_extent_index
                .lookup(&blake3::hash(&b_shared))
                .is_some(),
            "parent must contain shared-block hash"
        );

        // Child import reuses the parent's hash pool.
        let child_dir = by_id_dir.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        let child_signer = setup_vol_pub(&child_dir);
        import_image(
            &child_image,
            &child_dir,
            child_signer.as_ref(),
            Some(&parent_extent_index),
            |_, _| {},
        )
        .unwrap();

        // Read the child's segment back and assert: shared block is a
        // DedupRef, unique block is a Data entry.
        use crate::segment::EntryKind;
        let mut pending: Vec<_> = fs::read_dir(child_dir.join("pending"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(pending.len(), 1, "one child segment expected");
        let seg_path = pending.pop().unwrap().path();
        let vk = signing::load_verifying_key(&child_dir, signing::VOLUME_PUB_FILE).unwrap();
        let (_, entries) = segment::read_and_verify_segment_index(&seg_path, &vk).unwrap();

        let shared_hash = blake3::hash(&b_shared);
        let child_only_hash = blake3::hash(&b_child_only);
        let shared_entry = entries
            .iter()
            .find(|e| e.hash == shared_hash)
            .expect("shared hash present");
        assert_eq!(
            shared_entry.kind,
            EntryKind::DedupRef,
            "shared block must be a DedupRef pointing at the parent"
        );
        let unique_entry = entries
            .iter()
            .find(|e| e.hash == child_only_hash)
            .expect("child-only hash present");
        // A fresh block is stored locally as either Data (body) or Inline
        // (small compressed payload lifted into the index). Either way it is
        // NOT a DedupRef — the child wrote its own bytes.
        assert!(
            matches!(unique_entry.kind, EntryKind::Data | EntryKind::Inline),
            "child-only block must be stored locally, got {:?}",
            unique_entry.kind
        );
    }

    #[test]
    fn import_volume_readable() {
        // Write a known pattern into blocks 0 and 2; block 1 is zero.
        let mut b0 = [0u8; LBA_SIZE];
        b0.fill(0xAA);
        let mut b2 = [0u8; LBA_SIZE];
        b2.fill(0xBB);
        let (_src_tmp, image_path) = make_image(&[&b0, &[0u8; LBA_SIZE], &b2]);

        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("readable");
        let signer = setup_vol_pub(&vol_dir);
        import_image(&image_path, &vol_dir, signer.as_ref(), None, |_, _| {}).unwrap();

        // Readonly volumes must have volume.pub but not volume.key.
        assert!(
            vol_dir.join(signing::VOLUME_PUB_FILE).exists(),
            "volume.pub must exist"
        );
        assert!(
            !vol_dir.join(signing::VOLUME_KEY_FILE).exists(),
            "volume.key must not exist on readonly volume"
        );
        // Re-open with ReadonlyVolume (imported volumes have no volume.key).
        // No ancestors in this test; by_id_dir is unused.
        let by_id = vol_dir.parent().unwrap();
        let rv = crate::volume::ReadonlyVolume::open(&vol_dir, by_id).unwrap();
        let got0 = rv.read(0, 1).unwrap();
        assert_eq!(got0, b0);
        let got1 = rv.read(1, 1).unwrap();
        assert_eq!(got1, vec![0u8; LBA_SIZE]); // unwritten → zeros
        let got2 = rv.read(2, 1).unwrap();
        assert_eq!(got2, b2);
    }
}
