// File-aware import of an ext4 disk image into an Elide readonly volume.
//
// Parses the image's ext4 metadata to learn the physical layout of every
// regular file, then emits one DATA entry per *fragment* (contiguous LBA
// range owned by one file). Non-file blocks — ext4 metadata, directory
// blocks, allocation bitmaps, journal, and any orphan data — stay
// block-granular: they are read and emitted as one 4 KiB DATA entry
// each. Zero blocks are skipped (same as before).
//
// The filemap is written as a side effect of the import walk, using the
// v2 format (`# elide-filemap v2` with per-fragment lines). This is the
// only ext4 pass during import — there is no separate filemap
// generation step. See docs/design-delta-compression.md.
//
// For each emitted extent (fragment or single block), the parent extent
// index is consulted first: if the hash is already present in a parent
// volume's extent index (populated via `--extents-from` at the CLI),
// a thin DedupRef is written instead of a fresh DATA entry.
//
// Crash recovery reduces to "retry the import" — all data is known
// upfront, there is no WAL involved, and segments are written via the
// standard tmp-rename commit pattern.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use ulid::Ulid;

use crate::ext4_scan::{self, Ext4Scan, FileFragment};
use crate::extentindex::ExtentIndex;
use crate::filemap::{self, FilemapRow};
use crate::segment::{self, SegmentEntry, SegmentFlags, SegmentSigner};

const LBA_SIZE: usize = 4096;
const ZERO_BLOCK: [u8; LBA_SIZE] = [0u8; LBA_SIZE];

/// Soft cap on raw (uncompressed) data accumulated before flushing a segment.
///
/// Matches the runtime WAL flush threshold and GC sweep live-bytes target so
/// imported volumes produce the same segment granularity as everything else —
/// uniform eviction, demand-fetch, and bin-packing behaviour. The cap is soft:
/// the last fragment in a batch may push slightly past it.
const IMPORT_SEGMENT_BYTES: usize = 32 * 1024 * 1024; // 32 MiB raw

/// Write `entries` to `pending/<ulid>` using the standard tmp-rename commit.
/// Clears `entries` on success. Returns the ULID used, or `None` if there
/// was nothing to write.
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

/// One emitted extent plus its filemap contribution (if any).
struct EmittedEntry {
    entry: SegmentEntry,
    raw_bytes: usize,
}

fn make_entry(
    hash: blake3::Hash,
    start_lba: u64,
    lba_length: u32,
    body: &[u8],
    parent_extent_index: Option<&ExtentIndex>,
) -> EmittedEntry {
    let parent_hit = parent_extent_index.and_then(|p| p.lookup(&hash));
    if parent_hit.is_some() {
        return EmittedEntry {
            entry: SegmentEntry::new_dedup_ref(hash, start_lba, lba_length),
            raw_bytes: 0,
        };
    }
    let (flags, data) = match crate::volume::maybe_compress(body) {
        Some(compressed) => (SegmentFlags::COMPRESSED, compressed),
        None => (SegmentFlags::empty(), body.to_vec()),
    };
    EmittedEntry {
        entry: SegmentEntry::new_data(hash, start_lba, lba_length, flags, data),
        raw_bytes: body.len(),
    }
}

/// Import an ext4 disk image into a new readonly Elide volume at `vol_dir`.
///
/// Creates `<vol_dir>/pending/` and `<vol_dir>/snapshots/`, scans
/// `image_path` for file fragments via the ext4 extent tree, and writes
/// segment files (one DATA entry per file fragment + one DATA entry per
/// non-file block) directly into `pending/`. After all data is written,
/// writes the snapshot marker, the filemap v2 for the snapshot, and the
/// `volume.size` marker.
///
/// `progress` receives `(lbas_done, total_lbas)` approximately after
/// each emission. Pass a no-op closure if progress reporting is not
/// needed.
///
/// # Errors
///
/// Returns an error if the image is not ext4, the image size is not a
/// multiple of 4096, if any I/O operation fails, or if `vol_dir` cannot
/// be created.
pub fn import_image(
    image_path: &Path,
    vol_dir: &Path,
    signer: &dyn SegmentSigner,
    parent_extent_index: Option<&ExtentIndex>,
    mut progress: impl FnMut(u64, u64),
) -> io::Result<()> {
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

    // Write to pending/ so the coordinator's normal drain loop picks them up,
    // uploads to the store, and writes index/<ulid>.idx + cache/<ulid>.{body,present}.
    let segments_dir = vol_dir.join("pending");
    let snapshots_dir = vol_dir.join("snapshots");
    fs::create_dir_all(&segments_dir)?;
    fs::create_dir_all(&snapshots_dir)?;

    // Parse ext4 and collect file fragments + coverage bitset. This is
    // the single source of truth for both the segment-write loop and
    // the filemap written below.
    let scan: Ext4Scan = ext4_scan::scan(image_path)?;
    let total_lbas = scan.total_lbas;
    let Ext4Scan {
        fragments: scan_fragments,
        file_lba_coverage,
        ..
    } = scan;

    let mut image = fs::File::open(image_path)?;
    let mut block = [0u8; LBA_SIZE];
    let mut entries: Vec<SegmentEntry> = Vec::new();
    let mut batch_raw_bytes: usize = 0;
    let mut all_segment_ulids: Vec<Ulid> = Vec::new();
    let mut last_segment_ulid: Option<String> = None;

    // File fragments, drained in LBA order (ext4_scan sorts them).
    let mut frag_iter = scan_fragments.into_iter().peekable();

    // Closure-free helper: test whether an LBA is owned by a file
    // fragment (used only in the debug_assert; the main loop advances
    // via fragment iteration, not coverage lookup).
    let lba_is_file = |lba: u64| -> bool {
        if lba >= total_lbas {
            return false;
        }
        let idx = (lba / 64) as usize;
        let bit = lba % 64;
        file_lba_coverage
            .get(idx)
            .is_some_and(|w| w & (1 << bit) != 0)
    };

    // Per-fragment filemap rows, accumulated as we emit fragments so the
    // filemap write below is a single streaming pass.
    let mut filemap_rows: Vec<FilemapRow> = Vec::new();

    let mut lba = 0u64;
    while lba < total_lbas {
        // If a fragment starts at this LBA, emit it whole.
        let frag_start_here = frag_iter
            .peek()
            .map(|f| f.lba_start == lba)
            .unwrap_or(false);
        if frag_start_here {
            let f: FileFragment = frag_iter.next().expect("peeked");
            let lba_len = f.lba_length;
            let emitted = make_entry(f.hash, f.lba_start, lba_len, &f.body, parent_extent_index);
            entries.push(emitted.entry);
            batch_raw_bytes += emitted.raw_bytes;
            filemap_rows.push(FilemapRow {
                path: f.path,
                file_offset: f.file_offset,
                hash: f.hash,
                byte_count: f.byte_count,
            });
            lba += lba_len as u64;
            progress(lba, total_lbas);
        } else {
            // Non-file block: read it and emit block-granular DATA if
            // non-zero. The coverage bitset check is defensive — if a
            // block is flagged as file-covered but the fragment iter
            // has moved past, we would otherwise emit stale data. In
            // practice this cannot happen because fragments are sorted
            // and we only advance past a fragment by emitting it.
            debug_assert!(
                !lba_is_file(lba),
                "lba {lba} marked file-owned but no fragment starts here"
            );
            image.seek(SeekFrom::Start(lba * LBA_SIZE as u64))?;
            image.read_exact(&mut block)?;

            if block != ZERO_BLOCK {
                let hash = blake3::hash(&block);
                let emitted = make_entry(hash, lba, 1, &block, parent_extent_index);
                entries.push(emitted.entry);
                batch_raw_bytes += emitted.raw_bytes;
            }
            lba += 1;
            progress(lba, total_lbas);
        }

        if batch_raw_bytes >= IMPORT_SEGMENT_BYTES
            && let Some(ulid) = flush_segment(&segments_dir, &mut entries, signer)?
        {
            all_segment_ulids
                .push(Ulid::from_string(&ulid).expect("flush_segment returned invalid ULID"));
            last_segment_ulid = Some(ulid);
            batch_raw_bytes = 0;
        }
    }

    if let Some(ulid) = flush_segment(&segments_dir, &mut entries, signer)? {
        all_segment_ulids
            .push(Ulid::from_string(&ulid).expect("flush_segment returned invalid ULID"));
        last_segment_ulid = Some(ulid);
    }

    // Snapshot marker reuses the last segment's ULID so the branch point is
    // self-describing. Falls back to a fresh ULID only for all-zero images
    // where no segments were written.
    let snap_ulid_str = last_segment_ulid.unwrap_or_else(|| Ulid::new().to_string());
    let snap_ulid = Ulid::from_string(&snap_ulid_str)
        .map_err(|e| io::Error::other(format!("invalid snapshot ULID: {e}")))?;

    // Write the signed snapshot manifest before the marker — partial
    // sequences leave no snapshot visible (the marker is written last).
    crate::signing::write_snapshot_manifest(vol_dir, signer, &snap_ulid, &all_segment_ulids)?;

    fs::write(snapshots_dir.join(&snap_ulid_str), "")?;

    // Write the filemap alongside the snapshot marker.
    filemap::write(&snapshots_dir, &snap_ulid_str, &filemap_rows)?;

    // Write size into volume.toml (read-modify-write to preserve name if already set).
    let mut cfg = crate::config::VolumeConfig::read(vol_dir)?;
    cfg.size = Some(image_size);
    cfg.write(vol_dir)?;

    Ok(())
}
