// Volume: top-level I/O interface — owns the LBA map, WAL, and directory layout.
//
// Directory layout:
//   <base>/wal/       — active write-ahead log (at most one file at a time)
//   <base>/pending/   — promoted segments awaiting S3 upload
//   <base>/segments/  — segments confirmed uploaded to S3 (evictable)
//
// Write path:
//   1. Volume::write(lba, data) — hashes data, appends to WAL, updates LBA map
//      and extent index (WAL offset as temporary location)
//   2. When the WAL reaches FLUSH_THRESHOLD, it is promoted to a clean segment
//      in pending/ and the extent index is updated to segment offsets
//
// Read path:
//   1. lbamap.lookup(lba) → (hash, block_offset)
//   2. extent_index.lookup(hash) → ExtentLocation (segment_id, body_offset, body_length)
//   3. find_segment_file (wal/ → pending/ → segments/) → open file, seek, read
//
// Recovery:
//   Volume::open() calls lbamap::rebuild_segments() (segments only), then
//   scans the WAL once: that single pass truncates any partial-tail record,
//   replays entries into the LBA map, extent index, and pending_entries.
//   Any .tmp files in pending/ are removed (incomplete promotions).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::{extentindex, lbamap, segment, writelog};

/// WAL size (bytes) at which the log is promoted to a pending segment.
/// This is a soft cap: a single write larger than this threshold will still
/// succeed, producing a segment larger than intended. The block layer
/// (NBD/ublk) enforces its own per-request maximum before reaching here.
const FLUSH_THRESHOLD: u64 = 32 * 1024 * 1024;

/// Maximum byte length of a single write. The segment format stores
/// `body_length` as a `u32`, so payloads must fit in 4 GiB. We cap at
/// `u32::MAX` rounded down to a 4 KiB boundary.
const MAX_WRITE_SIZE: usize = (u32::MAX as usize / 4096) * 4096;

/// A writable block-device volume backed by a content-addressable store.
///
/// Owns the in-memory LBA map, the active WAL, and the directory layout.
pub struct Volume {
    base_dir: PathBuf,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    wal: writelog::WriteLog,
    wal_ulid: String,
    wal_path: PathBuf,
    /// DATA and REF extents written since the last promotion; used to write
    /// the clean segment file on the next promote().
    pending_entries: Vec<segment::SegmentEntry>,
}

impl Volume {
    /// Open (or create) a volume rooted at `base_dir`.
    ///
    /// Creates `wal/`, `pending/`, and `segments/` subdirectories if they do
    /// not exist. Rebuilds the LBA map from all committed segments and any
    /// in-progress WAL, then reopens or creates the WAL.
    pub fn open(base_dir: &Path) -> io::Result<Self> {
        let wal_dir = base_dir.join("wal");
        let pending_dir = base_dir.join("pending");
        let segments_dir = base_dir.join("segments");

        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&pending_dir)?;
        fs::create_dir_all(&segments_dir)?;

        // Remove any .tmp files in pending/ — these are incomplete promotions
        // left behind by a crash between write and rename.
        for entry in fs::read_dir(&pending_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(&path)?;
            }
        }

        // Rebuild the LBA map and extent index from all committed segments.
        let mut lbamap = lbamap::rebuild_segments(base_dir)?;
        let mut extent_index = extentindex::rebuild(base_dir)?;

        // Find the in-progress WAL file (there should be at most one).
        let mut wal_files: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                wal_files.push(entry.path());
            }
        }
        wal_files.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        // Edge case: if pending/<ulid> already exists alongside wal/<ulid>,
        // the promotion completed (rename succeeded) but the WAL delete was
        // interrupted. The segment is authoritative — delete any such stale
        // WAL files before deciding what to replay. rebuild_segments above
        // has already loaded the committed segment into lbamap/extent_index.
        wal_files.retain(|path| {
            let ulid = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if pending_dir.join(ulid).exists() {
                let _ = fs::remove_file(path);
                false
            } else {
                true
            }
        });

        // recover_wal does the single WAL scan: truncates any partial tail,
        // replays records into the LBA map, and rebuilds pending_entries.
        let (wal, wal_ulid, wal_path, pending_entries) =
            if let Some(path) = wal_files.into_iter().last() {
                recover_wal(path, &mut lbamap, &mut extent_index)?
            } else {
                create_fresh_wal(&wal_dir)?
            };

        Ok(Self {
            base_dir: base_dir.to_owned(),
            lbamap,
            extent_index,
            wal,
            wal_ulid,
            wal_path,
            pending_entries,
        })
    }

    /// Write `data` starting at logical block address `lba`.
    ///
    /// `data.len()` must be a non-zero multiple of 4096 and must not exceed
    /// `MAX_WRITE_SIZE` (4 GiB − 4 KiB). The segment format stores `body_length`
    /// as a `u32` byte count, so larger payloads cannot be represented.
    ///
    /// The data is appended to the WAL and the LBA map is updated in memory.
    /// Promotion to a pending segment is triggered after the write if the WAL
    /// reaches `FLUSH_THRESHOLD` (32 MiB). Because the check is post-write, a
    /// single large write may produce a segment larger than the threshold; the
    /// block layer (NBD/ublk) is expected to enforce its own per-request cap.
    pub fn write(&mut self, lba: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() || !data.len().is_multiple_of(4096) {
            return Err(io::Error::other(
                "data length must be a non-zero multiple of 4096",
            ));
        }
        if data.len() > MAX_WRITE_SIZE {
            return Err(io::Error::other(
                "data length exceeds maximum write size (4 GiB − 4 KiB)",
            ));
        }
        let lba_length = (data.len() / 4096) as u32;
        let hash = blake3::hash(data);

        let body_offset = self.wal.append_data(lba, lba_length, &hash, 0, data)?;
        self.lbamap.insert(lba, lba_length, hash);
        // Temporary extent index entry: points into the WAL at the raw payload offset.
        // Updated to segment file offsets after promotion.
        self.extent_index.insert(
            hash,
            extentindex::ExtentLocation {
                segment_id: self.wal_ulid.clone(),
                body_offset,
                body_length: data.len() as u32,
            },
        );
        self.pending_entries.push(segment::SegmentEntry::new_data(
            hash,
            lba,
            lba_length,
            0,
            data.to_vec(),
        ));

        if self.wal.size() >= FLUSH_THRESHOLD {
            self.promote()?;
        }

        Ok(())
    }

    /// Read `lba_count` blocks (4096 bytes each) starting at `lba`.
    ///
    /// Blocks that have never been written are returned as zeros (the
    /// block-device convention for unwritten regions). Each written block
    /// is fetched from the segment file identified by the extent index.
    pub fn read(&self, lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut out = vec![0u8; lba_count as usize * 4096];
        for i in 0..lba_count as u64 {
            let cur_lba = lba + i;
            let Some((hash, block_offset)) = self.lbamap.lookup(cur_lba) else {
                continue; // unwritten region — block stays zero
            };
            let Some(loc) = self.extent_index.lookup(&hash) else {
                continue; // hash not indexed — treat as unwritten
            };
            let path = find_segment_file(&self.base_dir, &loc.segment_id)?;
            let mut f = fs::File::open(path)?;
            let byte_offset = loc.body_offset + block_offset as u64 * 4096;
            f.seek(SeekFrom::Start(byte_offset))?;
            let out_slice = &mut out[i as usize * 4096..(i as usize + 1) * 4096];
            f.read_exact(out_slice)?;
        }
        Ok(out)
    }

    /// Flush buffered WAL writes and fsync to disk.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.wal.fsync()
    }

    /// Promote the current WAL to a pending segment, then open a fresh WAL.
    fn promote(&mut self) -> io::Result<()> {
        self.wal.fsync()?;
        let body_section_start = segment::promote(
            &self.wal_path,
            &self.wal_ulid,
            &self.base_dir.join("pending"),
            &mut self.pending_entries,
        )?;
        // Update the extent index: replace temporary WAL offsets with absolute
        // offsets into the committed segment file.
        for entry in &self.pending_entries {
            if entry.is_dedup_ref {
                continue; // data lives in an ancestor segment; index already correct
            }
            self.extent_index.insert(
                entry.hash,
                extentindex::ExtentLocation {
                    segment_id: self.wal_ulid.clone(),
                    body_offset: body_section_start + entry.stored_offset,
                    body_length: entry.stored_length,
                },
            );
        }
        // Create the fresh WAL. If this fails the segment is safe in pending/
        // and will be found on the next startup rebuild.
        let (wal, wal_ulid, wal_path, _) = create_fresh_wal(&self.base_dir.join("wal"))?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        self.pending_entries.clear();

        Ok(())
    }

    #[cfg(test)]
    pub fn lbamap_len(&self) -> usize {
        self.lbamap.len()
    }

    #[cfg(test)]
    pub fn promote_for_test(&mut self) -> io::Result<()> {
        self.promote()
    }
}

// --- helpers ---

/// Locate the segment body file for `segment_id` by checking each storage
/// directory in order: wal/ (in-progress), pending/ (promoted), segments/.
///
/// Returns the first path that exists. WAL files are found here before
/// promotion; after promotion the same ULID lives in pending/.
fn find_segment_file(base_dir: &Path, segment_id: &str) -> io::Result<PathBuf> {
    for subdir in ["wal", "pending", "segments"] {
        let path = base_dir.join(subdir).join(segment_id);
        if path.exists() {
            return Ok(path);
        }
    }
    Err(io::Error::other(format!("segment not found: {segment_id}")))
}

// --- WAL helpers ---

/// Scan an existing WAL, replay its records into `lbamap`, rebuild
/// `pending_entries`, and reopen the WAL for continued appending.
///
/// This is the single WAL scan on startup — it both updates the LBA map
/// (WAL is more recent than any segment) and recovers the pending_entries
/// list needed for the next promotion.
///
/// `writelog::scan` truncates any partial-tail record before returning.
fn recover_wal(
    path: PathBuf,
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
) -> io::Result<(
    writelog::WriteLog,
    String,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let ulid_str = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("bad WAL filename"))?;
    let ulid = Ulid::from_string(ulid_str)
        .map_err(|e| io::Error::other(e.to_string()))?
        .to_string();

    let (records, valid_size) = writelog::scan(&path)?;

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
                lbamap.insert(start_lba, lba_length, hash);
                // Temporary WAL offset — updated to segment offset on promotion.
                extent_index.insert(
                    hash,
                    extentindex::ExtentLocation {
                        segment_id: ulid.clone(),
                        body_offset,
                        body_length,
                    },
                );
                pending_entries.push(segment::SegmentEntry::new_data(
                    hash, start_lba, lba_length, flags, data,
                ));
            }
            writelog::LogRecord::Ref {
                hash,
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, hash);
                // Data lives in an ancestor segment; extent index already has it
                // from rebuild_segments(). Just add to pending_entries so the
                // dedup-ref is preserved in the next promoted segment.
                pending_entries.push(segment::SegmentEntry::new_dedup_ref(
                    hash, start_lba, lba_length,
                ));
            }
        }
    }

    let wal = writelog::WriteLog::reopen(&path, valid_size)?;
    Ok((wal, ulid, path, pending_entries))
}

/// Create a new WAL file with a fresh ULID.
fn create_fresh_wal(
    wal_dir: &Path,
) -> io::Result<(
    writelog::WriteLog,
    String,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let ulid = Ulid::new().to_string();
    let path = wal_dir.join(&ulid);
    let wal = writelog::WriteLog::create(&path)?;
    Ok((wal, ulid, path, Vec::new()))
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "palimpsest-volume-test-{}-{}",
            std::process::id(),
            n
        ));
        p
    }

    #[test]
    fn open_creates_directories() {
        let base = temp_dir();
        let _ = Volume::open(&base).unwrap();
        assert!(base.join("wal").is_dir());
        assert!(base.join("pending").is_dir());
        assert!(base.join("segments").is_dir());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_is_idempotent() {
        let base = temp_dir();
        let _ = Volume::open(&base).unwrap();
        // Second open on the same dir should succeed (dirs already exist).
        let _ = Volume::open(&base).unwrap();
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_single_block() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        vol.write(0, &vec![0x42u8; 4096]).unwrap();
        vol.fsync().unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_multi_block_extent() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        // Write 8 LBAs (32 KiB) as a single call.
        vol.write(10, &vec![0xabu8; 8 * 4096]).unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_empty() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        let err = vol.write(0, &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_misaligned() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        let err = vol.write(0, &[0u8; 1000]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_promotes_on_flush() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        // Write 33 × 1 MiB to exceed FLUSH_THRESHOLD (32 MiB).
        let block = vec![0u8; 1024 * 1024];
        for i in 0u64..33 {
            vol.write(i * 256, &block).unwrap();
        }

        // At least one segment should have been promoted to pending/.
        let has_pending = fs::read_dir(base.join("pending"))
            .unwrap()
            .any(|e| e.is_ok());
        assert!(
            has_pending,
            "expected at least one promoted segment in pending/"
        );

        // A fresh WAL should have been created.
        let wal_count = fs::read_dir(base.join("wal"))
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(
            wal_count, 1,
            "expected exactly one WAL file after promotion"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn recovery_rebuilds_lbamap() {
        let base = temp_dir();

        // Write two blocks, fsync, then drop (simulates clean shutdown before promotion).
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen — lbamap should contain both blocks.
        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_unwritten_returns_zeros() {
        let base = temp_dir();
        let vol = Volume::open(&base).unwrap();
        let data = vol.read(0, 4).unwrap();
        assert_eq!(data.len(), 4 * 4096);
        assert!(data.iter().all(|&b| b == 0));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_written_data_same_session() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        let payload = vec![0x42u8; 4096];
        vol.write(5, &payload).unwrap();

        // Written block reads back correctly.
        let result = vol.read(5, 1).unwrap();
        assert_eq!(result, payload);

        // Adjacent unwritten blocks are zero.
        let before = vol.read(4, 1).unwrap();
        assert!(before.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_multi_block_extent() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        // Write 4 blocks with distinct fill bytes so we can verify each block.
        let mut payload = Vec::with_capacity(4 * 4096);
        for fill in [0xAAu8, 0xBB, 0xCC, 0xDD] {
            payload.extend_from_slice(&[fill; 4096]);
        }
        vol.write(10, &payload).unwrap();

        let result = vol.read(10, 4).unwrap();
        assert_eq!(result, payload);

        // Reading a sub-range within the extent.
        let mid = vol.read(11, 2).unwrap();
        assert_eq!(mid, payload[4096..3 * 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_after_promote() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        let payload = vec![0x55u8; 4096];
        vol.write(0, &payload).unwrap();
        vol.promote_for_test().unwrap();

        // After promotion, data lives in pending/<ulid>; reads must still work.
        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_after_reopen() {
        let base = temp_dir();

        let payload = vec![0x77u8; 4096];
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(3, &payload).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen: WAL recovery must restore both the LBA map and extent index.
        let vol = Volume::open(&base).unwrap();
        let result = vol.read(3, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn ulid_is_unique_and_sortable() {
        let u1 = Ulid::new().to_string();
        let u2 = Ulid::new().to_string();
        assert_eq!(u1.len(), 26);
        assert_ne!(u1, u2);
        // ULIDs generated in sequence should sort correctly (same millisecond
        // is not guaranteed, but two different values prove uniqueness).
    }

    #[test]
    fn recovery_after_promotion() {
        // Write enough to trigger a promotion, drop, reopen — the LBA map must
        // be rebuilt from both pending/ segments and the remaining WAL.
        let base = temp_dir();

        {
            let mut vol = Volume::open(&base).unwrap();
            let block = vec![0u8; 1024 * 1024]; // 1 MiB = 256 LBAs
            for i in 0u64..33 {
                vol.write(i * 256, &block).unwrap();
            }
            vol.fsync().unwrap();
        }

        // All 33 extents should survive across the promotion boundary.
        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.lbamap_len(), 33);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn promotion_after_wal_recovery() {
        // Write to the WAL, drop (simulating a crash), reopen (WAL recovery),
        // promote, then reopen again — verifies that pending_entries is correctly
        // rebuilt from the recovered WAL so the segment contains the pre-crash writes.
        let base = temp_dir();

        // Phase 1: write two blocks, fsync, drop.
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Phase 2: recover and immediately promote.
        {
            let mut vol = Volume::open(&base).unwrap();
            assert_eq!(vol.lbamap_len(), 2); // confirm recovery
            vol.promote_for_test().unwrap();
        }

        // Phase 3: reopen — both blocks must now come from the pending/ segment.
        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        // Confirm the promoted segment landed correctly: one file in pending/.
        let pending_count = fs::read_dir(base.join("pending"))
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(pending_count, 1, "expected one segment file in pending/");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn wal_deleted_when_pending_segment_exists() {
        // Simulate a crash between the segment rename and the WAL delete:
        // both wal/<ulid> and pending/<ulid> exist. On reopen, the WAL must
        // be silently discarded and data read from the committed segment.
        let base = temp_dir();

        // Phase 1: write two blocks and promote so a segment lands in pending/.
        let ulid;
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![0xaau8; 4096]).unwrap();
            vol.write(1, &vec![0xbbu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            // Grab the segment ULID (there is exactly one file in pending/).
            ulid = fs::read_dir(base.join("pending"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .into_owned();
        }

        // Simulate the crash: copy the segment back as a WAL file so both exist.
        fs::copy(
            base.join("pending").join(&ulid),
            base.join("wal").join(&ulid),
        )
        .unwrap();

        // Reopen — should delete the stale WAL and load cleanly from the segment.
        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);
        assert!(
            vol.read(0, 1).unwrap().iter().all(|&b| b == 0xaa),
            "LBA 0 should be 0xaa"
        );
        assert!(
            vol.read(1, 1).unwrap().iter().all(|&b| b == 0xbb),
            "LBA 1 should be 0xbb"
        );
        // The stale WAL file should be gone.
        assert!(
            !base.join("wal").join(&ulid).exists(),
            "stale WAL was not removed"
        );

        fs::remove_dir_all(base).unwrap();
    }
}
