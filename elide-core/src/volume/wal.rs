//! WAL lifecycle helpers used by the writable [`Volume`](super::Volume) open
//! and recovery paths: scan + replay an existing WAL, reopen it for continued
//! appending, or create a fresh one. Split out of `volume/mod.rs` for legibility
//! — no behaviour change.

use std::io;
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::{
    extentindex::{self, BodySource},
    lbamap, segment, writelog,
};

use super::ZERO_HASH;

/// Scan a WAL file and replay its records into `lbamap` + `extent_index`,
/// returning the WAL ULID, the valid (non-partial) tail size, and the
/// reconstructed pending_entries list.
///
/// Shared between:
/// - [`recover_wal`], which also reopens the file for continued appending
///   (latest WAL case).
/// - `Volume::open_impl`'s recovery-time promote loop, which promotes
///   each non-latest WAL to a fresh segment and deletes the WAL file
///   rather than reopening it.
///
/// `writelog::scan` truncates any partial-tail record before returning.
pub(super) fn replay_wal_records(
    path: &Path,
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
) -> io::Result<(Ulid, u64, Vec<segment::SegmentEntry>)> {
    let ulid_str = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("bad WAL filename"))?;
    let ulid = Ulid::from_string(ulid_str).map_err(|e| io::Error::other(e.to_string()))?;

    let (records, valid_size) = writelog::scan(path)?;

    let mut pending_entries = Vec::new();
    for record in records {
        match record {
            writelog::LogRecord::Data {
                hash,
                start_lba,
                lba_length,
                flags,
                body_offset,
                data,
            } => {
                let body_length = data.len() as u32;
                let compressed = flags.contains(writelog::WalFlags::COMPRESSED);
                // Translate WalFlags → SegmentFlags: the two namespaces use different
                // bit values (WalFlags::COMPRESSED = 0x01, SegmentFlags::COMPRESSED = 0x04).
                let seg_flags = if compressed {
                    segment::SegmentFlags::COMPRESSED
                } else {
                    segment::SegmentFlags::empty()
                };
                lbamap.insert(start_lba, lba_length, hash);
                // Temporary WAL offset — updated to segment offset on promotion.
                extent_index.insert(
                    hash,
                    extentindex::ExtentLocation {
                        segment_id: ulid,
                        body_offset,
                        body_length,
                        compressed,
                        body_source: BodySource::Local,
                        body_section_start: 0,
                        inline_data: None,
                    },
                );
                pending_entries.push(segment::SegmentEntry::new_data(
                    hash, start_lba, lba_length, seg_flags, data,
                ));
            }
            writelog::LogRecord::Ref {
                hash,
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, hash);
                // REF: no body bytes, no body reservation, no extent_index
                // update. The canonical entry is populated from whichever
                // segment holds the DATA for this hash.
                pending_entries.push(segment::SegmentEntry::new_dedup_ref(
                    hash, start_lba, lba_length,
                ));
            }
            writelog::LogRecord::Zero {
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, ZERO_HASH);
                pending_entries.push(segment::SegmentEntry::new_zero(start_lba, lba_length));
            }
        }
    }

    Ok((ulid, valid_size, pending_entries))
}

/// Scan an existing WAL, replay its records into `lbamap`, rebuild
/// `pending_entries`, and reopen the WAL for continued appending.
///
/// This is the single WAL scan on startup — it both updates the LBA map
/// (WAL is more recent than any segment) and recovers the pending_entries
/// list needed for the next promotion.
pub(super) fn recover_wal(
    path: PathBuf,
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
) -> io::Result<(
    writelog::WriteLog,
    Ulid,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let (ulid, valid_size, pending_entries) = replay_wal_records(&path, lbamap, extent_index)?;
    let wal = writelog::WriteLog::reopen(&path, valid_size)?;
    Ok((wal, ulid, path, pending_entries))
}

/// Create a new WAL file using the provided `ulid`.
///
/// The caller is responsible for generating a ULID that sorts after all
/// existing segments and WAL files (typically via `Volume::mint`).
pub(super) fn create_fresh_wal(
    wal_dir: &Path,
    ulid: Ulid,
) -> io::Result<(
    writelog::WriteLog,
    Ulid,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let path = wal_dir.join(ulid.to_string());
    let wal = writelog::WriteLog::create(&path)?;
    log::info!("new WAL {ulid}");
    Ok((wal, ulid, path, Vec::new()))
}
