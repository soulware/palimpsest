//! Delta materialisation cache (`cache/<ULID>.dmat`).
//!
//! Local-only sidecar that holds materialised bytes for `FLAG_DELTA` segment
//! entries after their first read, so subsequent reads no longer pay the
//! zstd-dict-decompress cost. See `docs/design-delta-materialisation.md`.
//!
//! File layout:
//!
//! ```text
//! Magic (8 bytes):  "ELIDMAT\x01"
//! Records (appended sequentially):
//!     entry_idx      (u32 le)
//!     flags          (u8)             bit 0: FLAG_COMPRESSED (lz4_flex)
//!     stored_length  (u32 le)
//!     data           (stored_length bytes)
//! ```
//!
//! Discipline: append on first read, `sync_data()` per record, no rewrites.
//! On open, records are scanned sequentially with a caller-supplied verifier;
//! the first record that fails verification (truncated body, bad hash, bad
//! size) terminates the scan and the file is truncated to that point.
//! Subsequent reads of those entries re-materialise from `.delta`.
//!
//! Authentication is provided by the verifier — typically `BLAKE3(materialised)
//! == segment.entry[entry_idx].hash` against the segment's signed index.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use bitflags::bitflags;

bitflags! {
    /// Flag byte in a `.dmat` record.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct DmatFlags: u8 {
        /// `data` is `lz4_flex::compress_prepend_size` output.
        const COMPRESSED = 0x01;
    }
}

/// 8-byte magic at the start of every `.dmat` file.
pub const MAGIC: &[u8; 8] = b"ELIDMAT\x01";

const RECORD_HEADER_LEN: u64 = 4 + 1 + 4;

/// Location of a materialised record within the `.dmat` file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DmatLocation {
    /// File offset of the record's `data` field (i.e. past the 9-byte record header).
    pub data_offset: u64,
    pub stored_length: u32,
    pub flags: DmatFlags,
}

/// Counters reported by `open_or_create` for telemetry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanStats {
    /// Records accepted into the in-memory map.
    pub accepted: u32,
    /// Records that failed verification (truncated, bad size, hash mismatch).
    /// The first such record terminates the scan; this counter is `0` or `1`.
    pub invalid: u32,
    /// `true` if the file was truncated as a result of the scan.
    pub truncated: bool,
}

/// Open `.dmat` for a segment, ready for read and append.
pub struct Dmat {
    file: File,
    entries: HashMap<u32, DmatLocation>,
}

impl Dmat {
    /// Open an existing `.dmat`, or create a new one if absent.
    ///
    /// On open, every record is read, decompressed if `FLAG_COMPRESSED`, and
    /// passed to `verify`. The first record that fails the magic check, has a
    /// short body, or fails `verify` terminates the scan; the file is truncated
    /// to the start of that record.
    ///
    /// `verify(entry_idx, materialised_bytes) -> bool` returns `false` to
    /// reject the record (typically: hash mismatch against the segment index).
    pub fn open_or_create<V: FnMut(u32, &[u8]) -> bool>(
        path: &Path,
        verify: V,
    ) -> io::Result<(Self, ScanStats)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let metadata = file.metadata()?;
        let file_len = metadata.len();

        if file_len == 0 {
            file.write_all(MAGIC)?;
            file.sync_data()?;
            return Ok((
                Self {
                    file,
                    entries: HashMap::new(),
                },
                ScanStats::default(),
            ));
        }

        if file_len < MAGIC.len() as u64 {
            return Err(io::Error::other("dmat file shorter than magic"));
        }

        let mut magic_buf = [0u8; 8];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut magic_buf)?;
        if &magic_buf != MAGIC {
            return Err(io::Error::other("bad dmat magic"));
        }

        let (entries, stats) = scan_records(&mut file, file_len, verify)?;

        if stats.truncated {
            // Truncate to the offset where scan stopped. The end of the
            // last accepted record sits at the file's current cursor
            // position after `scan_records`, since it leaves the cursor
            // at the start of the rejected record (or EOF if all were
            // accepted but the tail was short).
            let truncate_to = file.stream_position()?;
            file.set_len(truncate_to)?;
            file.sync_data()?;
        }

        Ok((Self { file, entries }, stats))
    }

    /// Look up a previously materialised entry.
    pub fn lookup(&self, entry_idx: u32) -> Option<DmatLocation> {
        self.entries.get(&entry_idx).copied()
    }

    /// Number of materialised records currently indexed.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True if no records are materialised.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Read materialised bytes for a previously located record, decompressing
    /// if needed.
    pub fn read_materialised(&mut self, loc: DmatLocation) -> io::Result<Vec<u8>> {
        let mut stored = vec![0u8; loc.stored_length as usize];
        self.file.seek(SeekFrom::Start(loc.data_offset))?;
        self.file.read_exact(&mut stored)?;
        if loc.flags.contains(DmatFlags::COMPRESSED) {
            lz4_flex::decompress_size_prepended(&stored).map_err(io::Error::other)
        } else {
            Ok(stored)
        }
    }

    /// Append a materialised entry to the file.
    ///
    /// `materialised` is the canonical (uncompressed) extent bytes — the same
    /// bytes whose BLAKE3 hash equals `segment.entry[entry_idx].hash`. The
    /// shared entropy gate (`volume::maybe_compress`) decides whether to store
    /// lz4-compressed or raw.
    ///
    /// Returns the location of the newly-written record. The caller is free to
    /// read back via `read_materialised`, but typically already holds the
    /// bytes in memory.
    ///
    /// Calls `sync_data` after the write — durability boundary per record.
    pub fn append(
        &mut self,
        entry_idx: u32,
        materialised: &[u8],
        compressed: Option<&[u8]>,
    ) -> io::Result<DmatLocation> {
        let (stored, flags): (&[u8], DmatFlags) = match compressed {
            Some(c) => (c, DmatFlags::COMPRESSED),
            None => (materialised, DmatFlags::empty()),
        };
        let stored_length: u32 = stored
            .len()
            .try_into()
            .map_err(|_| io::Error::other("dmat record exceeds u32::MAX bytes"))?;

        let record_start = self.file.seek(SeekFrom::End(0))?;

        let mut header = [0u8; RECORD_HEADER_LEN as usize];
        header[0..4].copy_from_slice(&entry_idx.to_le_bytes());
        header[4] = flags.bits();
        header[5..9].copy_from_slice(&stored_length.to_le_bytes());
        self.file.write_all(&header)?;
        self.file.write_all(stored)?;
        self.file.sync_data()?;

        let loc = DmatLocation {
            data_offset: record_start + RECORD_HEADER_LEN,
            stored_length,
            flags,
        };
        self.entries.insert(entry_idx, loc);
        Ok(loc)
    }
}

fn scan_records<V: FnMut(u32, &[u8]) -> bool>(
    file: &mut File,
    file_len: u64,
    mut verify: V,
) -> io::Result<(HashMap<u32, DmatLocation>, ScanStats)> {
    let mut entries: HashMap<u32, DmatLocation> = HashMap::new();
    let mut stats = ScanStats::default();

    let mut cursor = MAGIC.len() as u64;
    file.seek(SeekFrom::Start(cursor))?;

    loop {
        if cursor == file_len {
            break;
        }
        if file_len - cursor < RECORD_HEADER_LEN {
            stats.truncated = true;
            file.seek(SeekFrom::Start(cursor))?;
            break;
        }

        let mut header = [0u8; RECORD_HEADER_LEN as usize];
        file.read_exact(&mut header)?;
        let entry_idx = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let raw_flags = header[4];
        let stored_length = u32::from_le_bytes([header[5], header[6], header[7], header[8]]);

        let Some(flags) = DmatFlags::from_bits(raw_flags) else {
            stats.invalid = 1;
            stats.truncated = true;
            file.seek(SeekFrom::Start(cursor))?;
            break;
        };

        let data_offset = cursor + RECORD_HEADER_LEN;
        let record_end = data_offset + stored_length as u64;
        if record_end > file_len {
            stats.truncated = true;
            file.seek(SeekFrom::Start(cursor))?;
            break;
        }

        let mut stored = vec![0u8; stored_length as usize];
        file.read_exact(&mut stored)?;

        let materialised: Vec<u8> = if flags.contains(DmatFlags::COMPRESSED) {
            match lz4_flex::decompress_size_prepended(&stored) {
                Ok(v) => v,
                Err(_) => {
                    stats.invalid = 1;
                    stats.truncated = true;
                    file.seek(SeekFrom::Start(cursor))?;
                    break;
                }
            }
        } else {
            stored
        };

        if !verify(entry_idx, &materialised) {
            stats.invalid = 1;
            stats.truncated = true;
            file.seek(SeekFrom::Start(cursor))?;
            break;
        }

        entries.insert(
            entry_idx,
            DmatLocation {
                data_offset,
                stored_length,
                flags,
            },
        );
        stats.accepted += 1;
        cursor = record_end;
    }

    Ok((entries, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "elide-dmat-test-{}-{}",
            std::process::id(),
            ulid::Ulid::new()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn open_creates_empty_file_with_magic() {
        let path = tmp_path("dmat");
        let (dmat, stats) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        assert!(dmat.is_empty());
        assert_eq!(stats, ScanStats::default());
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes, MAGIC);
    }

    #[test]
    fn append_and_lookup_roundtrip_uncompressed() {
        let path = tmp_path("dmat");
        let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        let payload = b"materialised extent bytes".to_vec();
        let loc = dmat.append(7, &payload, None).unwrap();
        assert_eq!(loc.flags, DmatFlags::empty());
        assert_eq!(loc.stored_length as usize, payload.len());
        let read = dmat.read_materialised(dmat.lookup(7).unwrap()).unwrap();
        assert_eq!(read, payload);
    }

    #[test]
    fn append_and_lookup_roundtrip_compressed() {
        let path = tmp_path("dmat");
        let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        let payload = vec![0u8; 8192];
        let compressed = lz4_flex::compress_prepend_size(&payload);
        let loc = dmat.append(11, &payload, Some(&compressed)).unwrap();
        assert!(loc.flags.contains(DmatFlags::COMPRESSED));
        assert_eq!(loc.stored_length as usize, compressed.len());
        let read = dmat.read_materialised(dmat.lookup(11).unwrap()).unwrap();
        assert_eq!(read, payload);
    }

    #[test]
    fn open_recovers_existing_records() {
        let path = tmp_path("dmat");
        {
            let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            dmat.append(1, b"alpha", None).unwrap();
            dmat.append(2, b"beta", None).unwrap();
            dmat.append(3, b"gamma", None).unwrap();
        }
        let (dmat, stats) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        assert_eq!(stats.accepted, 3);
        assert!(!stats.truncated);
        assert_eq!(dmat.len(), 3);
        for (idx, expected) in [(1u32, b"alpha".as_ref()), (2, b"beta"), (3, b"gamma")] {
            let loc = dmat.lookup(idx).unwrap();
            let mut dmat_mut = Dmat::open_or_create(&path, |_, _| true).unwrap().0;
            assert_eq!(dmat_mut.read_materialised(loc).unwrap(), expected);
        }
    }

    #[test]
    fn truncated_tail_is_recovered() {
        let path = tmp_path("dmat");
        {
            let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            dmat.append(1, b"alpha", None).unwrap();
            dmat.append(2, b"beta", None).unwrap();
        }
        // Simulate a torn write: append a partial record header.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xAA, 0xBB]).unwrap();
        }
        let len_before = std::fs::metadata(&path).unwrap().len();
        let (dmat, stats) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        assert!(stats.truncated);
        assert_eq!(stats.accepted, 2);
        assert_eq!(dmat.len(), 2);
        let len_after = std::fs::metadata(&path).unwrap().len();
        assert!(len_after < len_before);
        // Subsequent appends append cleanly past the truncation.
        let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        dmat.append(3, b"gamma", None).unwrap();
        assert_eq!(dmat.lookup(3).unwrap().stored_length, 5);
    }

    #[test]
    fn truncated_data_body_is_recovered() {
        let path = tmp_path("dmat");
        {
            let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            dmat.append(1, b"alpha", None).unwrap();
            dmat.append(2, b"beta-padding-padding", None).unwrap();
        }
        // Drop the last few bytes of the second record's data.
        let len = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 5).unwrap();
        let (dmat, stats) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        assert!(stats.truncated);
        assert_eq!(stats.accepted, 1);
        assert_eq!(dmat.len(), 1);
        assert!(dmat.lookup(1).is_some());
        assert!(dmat.lookup(2).is_none());
    }

    #[test]
    fn verify_failure_truncates_and_drops_record() {
        let path = tmp_path("dmat");
        {
            let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            dmat.append(1, b"good", None).unwrap();
            dmat.append(2, b"bad", None).unwrap();
            dmat.append(3, b"never", None).unwrap();
        }
        // Reject entry_idx == 2 on reopen.
        let (dmat, stats) = Dmat::open_or_create(&path, |idx, _| idx != 2).unwrap();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.invalid, 1);
        assert!(stats.truncated);
        assert_eq!(dmat.len(), 1);
        assert!(dmat.lookup(1).is_some());
        assert!(dmat.lookup(2).is_none());
        assert!(dmat.lookup(3).is_none());
    }

    #[test]
    fn rejects_bad_magic() {
        let path = tmp_path("dmat");
        std::fs::write(&path, b"NOTAMAT\x01extra").unwrap();
        let result = Dmat::open_or_create(&path, |_, _| true);
        assert!(result.is_err());
    }

    #[test]
    fn rejects_short_file_with_garbage() {
        let path = tmp_path("dmat");
        std::fs::write(&path, b"AB").unwrap();
        let result = Dmat::open_or_create(&path, |_, _| true);
        assert!(result.is_err());
    }

    #[test]
    fn append_after_reopen_persists() {
        let path = tmp_path("dmat");
        {
            let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            dmat.append(1, b"first", None).unwrap();
        }
        {
            let (mut dmat, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            dmat.append(2, b"second", None).unwrap();
        }
        let (dmat, stats) = Dmat::open_or_create(&path, |_, _| true).unwrap();
        assert_eq!(stats.accepted, 2);
        assert_eq!(dmat.len(), 2);
    }
}
