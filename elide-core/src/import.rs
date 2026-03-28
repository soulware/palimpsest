// Import an ext4 disk image into an Elide readonly volume.
//
// Reads the image in 4 KiB LBA-aligned blocks. Zero blocks are skipped — the
// volume read path returns zeros for unwritten LBAs, so they need no storage.
// Non-zero blocks are hashed, optionally compressed, and batched into large
// segment files written directly to `<vol_dir>/base/segments/` via the
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

use crate::segment::{self, FLAG_COMPRESSED, SegmentEntry};

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

/// Attempt zstd level-1 compression on a single block.
///
/// Returns `Some(compressed)` only if entropy is below 7.0 bits/byte and the
/// result achieves at least a 1.5× ratio; otherwise returns `None` (store raw).
fn maybe_compress(block: &[u8]) -> Option<Vec<u8>> {
    if shannon_entropy(block) > 7.0 {
        return None;
    }
    let compressed = zstd::bulk::compress(block, 1).ok()?;
    // Require at least 1.5× ratio (compressed × 3/2 < raw).
    if compressed.len() * 3 / 2 >= block.len() {
        return None;
    }
    Some(compressed)
}

/// Write `entries` to `segments/<ulid>` using the standard tmp-rename commit.
/// Clears `entries` on success. Returns the ULID used, or `None` if there was
/// nothing to write.
fn flush_segment(
    segments_dir: &Path,
    entries: &mut Vec<SegmentEntry>,
) -> io::Result<Option<String>> {
    if entries.is_empty() {
        return Ok(None);
    }
    let ulid = Ulid::new().to_string();
    let tmp = segments_dir.join(format!("{ulid}.tmp"));
    let final_path = segments_dir.join(&ulid);
    segment::write_segment(&tmp, entries)?;
    fs::rename(&tmp, &final_path)?;
    entries.clear();
    Ok(Some(ulid))
}

/// Import an ext4 disk image into a new readonly Elide volume at `vol_dir`.
///
/// Creates `<vol_dir>/base/{segments,snapshots}/`, reads
/// `image_path` in 4 KiB blocks, and writes segment files. After all data is
/// written, writes a snapshot marker (branch point for future forks) and the
/// `readonly` and `size` markers at the volume root.
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
    mut progress: impl FnMut(u64, u64),
) -> io::Result<()> {
    if vol_dir.exists() {
        return Err(io::Error::other(format!(
            "volume directory already exists: {}",
            vol_dir.display()
        )));
    }

    let image_size = fs::metadata(image_path)?.len();
    if image_size % LBA_SIZE as u64 != 0 {
        return Err(io::Error::other("image size is not a multiple of 4096"));
    }
    let total_blocks = image_size / LBA_SIZE as u64;

    let fork_dir = vol_dir.join("base");
    let segments_dir = fork_dir.join("segments");
    let snapshots_dir = fork_dir.join("snapshots");
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
        let (flags, data) = match maybe_compress(&block) {
            Some(compressed) => (FLAG_COMPRESSED, compressed),
            None => (0u8, block.to_vec()),
        };
        entries.push(SegmentEntry::new_data(hash, lba, 1, flags, data));
        batch_raw_bytes += LBA_SIZE;

        if batch_raw_bytes >= IMPORT_SEGMENT_BYTES {
            if let Some(ulid) = flush_segment(&segments_dir, &mut entries)? {
                last_segment_ulid = Some(ulid);
            }
            batch_raw_bytes = 0;
        }
    }
    if let Some(ulid) = flush_segment(&segments_dir, &mut entries)? {
        last_segment_ulid = Some(ulid);
    }

    // Snapshot marker reuses the last segment's ULID so the branch point is
    // self-describing. Falls back to a fresh ULID only for all-zero images
    // where no segments were written.
    let snap_ulid = last_segment_ulid.unwrap_or_else(|| Ulid::new().to_string());
    fs::write(snapshots_dir.join(&snap_ulid), "")?;

    // Volume-root size marker (readonly is written by the caller in meta.toml).
    fs::write(vol_dir.join("size"), image_size.to_string())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn import_creates_volume_layout() {
        let data_block: [u8; LBA_SIZE] = [0x42u8; LBA_SIZE];
        let zero_block: [u8; LBA_SIZE] = [0u8; LBA_SIZE];
        let (_src_tmp, image_path) = make_image(&[&data_block, &zero_block, &data_block]);

        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("testimport");
        import_image(&image_path, &vol_dir, |_, _| {}).unwrap();

        // readonly is now in meta.toml (written by caller, not import_image)
        assert!(!vol_dir.join("readonly").exists());
        assert_eq!(
            fs::read_to_string(vol_dir.join("size")).unwrap(),
            (LBA_SIZE * 3).to_string()
        );
        assert!(vol_dir.join("base").join("segments").exists());
        assert!(!vol_dir.join("base").join("pending").exists()); // frozen base: no pending/

        // Exactly one snapshot marker, and its ULID matches the segment ULID.
        let segs: Vec<_> = fs::read_dir(vol_dir.join("base").join("segments"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(segs.len(), 1);
        let seg_name = segs[0].file_name().into_string().unwrap();

        let snaps: Vec<_> = fs::read_dir(vol_dir.join("base").join("snapshots"))
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
        import_image(&image_path, &vol_dir, |_, _| {}).unwrap();

        // All-zero image: no segment files should be written.
        let segs: Vec<_> = fs::read_dir(vol_dir.join("base").join("segments"))
            .unwrap()
            .collect();
        assert_eq!(segs.len(), 0);
    }

    #[test]
    fn import_rejects_unaligned_image() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.ext4");
        fs::write(&path, vec![0u8; 100]).unwrap();

        let vol_tmp = TempDir::new().unwrap();
        let vol_dir = vol_tmp.path().join("bad");
        let err = import_image(&path, &vol_dir, |_, _| {}).unwrap_err();
        assert!(err.to_string().contains("multiple of 4096"));
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
        import_image(&image_path, &vol_dir, |_, _| {}).unwrap();

        // Re-open with ReadonlyVolume and verify the blocks.
        let rv = crate::volume::ReadonlyVolume::open(&vol_dir.join("base")).unwrap();
        let got0 = rv.read(0, 1).unwrap();
        assert_eq!(got0, b0);
        let got1 = rv.read(1, 1).unwrap();
        assert_eq!(got1, vec![0u8; LBA_SIZE]); // unwritten → zeros
        let got2 = rv.read(2, 1).unwrap();
        assert_eq!(got2, b2);
    }
}
