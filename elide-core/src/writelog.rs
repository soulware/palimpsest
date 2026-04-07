// Write log: the in-progress segment before it is promoted to a local segment.
//
// Each record is either:
//   DATA — a new extent with its (optionally compressed) payload
//   REF  — a dedup reference: an LBA mapping to an already-stored extent
//
// The log is an append-only file living in wal/<ULID>. On promotion, the
// promotion step reads the WAL and writes a clean segment file at
// pending/<ULID>, then deletes the WAL. The same ULID becomes the segment ID.
// On crash recovery, scan() reads the file and truncates any partial tail record.
//
// Record layout (DATA):
//   hash        (32 bytes)    BLAKE3 extent hash
//   start_lba   (u64 varint)  first logical block address
//   lba_length  (u32 varint)  extent length in 4KB blocks
//   flags       (u8)          see FLAG_* constants
//   data_length (u32 varint)  byte length of payload (compressed size if FLAG_COMPRESSED)
//   data        (data_length bytes)
//
// Record layout (REF, FLAG_DEDUP_REF set — thin, no body):
//   hash        (32 bytes)    BLAKE3 hash of the extent
//   start_lba   (u64 varint)
//   lba_length  (u32 varint)
//   flags       (u8)          FLAG_DEDUP_REF set; no further fields
//
// The hash is a key into the extent index for reads. Body bytes stay in
// the canonical segment; no duplicate data in the WAL.
//
// Record layout (ZERO, FLAG_ZERO set):
//   hash        (32 bytes)    ZERO_HASH ([0x00; 32])
//   start_lba   (u64 varint)
//   lba_length  (u32 varint)
//   flags       (u8)          FLAG_ZERO set; no further fields

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use bitflags::bitflags;

// --- flag bits ---

bitflags! {
    /// Flag byte in a WAL record.
    ///
    /// **These bit values differ from `segment::SegmentFlags`.**  Any code that
    /// translates WAL records into segment entries must explicitly convert between
    /// the two namespaces (see `volume::recover_wal`).
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct WalFlags: u8 {
        /// Payload is lz4-compressed; data_length is the compressed size.
        const COMPRESSED = 0x01;
        /// REF record; carries a materialised body payload (same layout as DATA).
        const DEDUP_REF  = 0x02;
        /// No data payload; this LBA range reads as zeros. Hash field is ZERO_HASH.
        const ZERO       = 0x04;
    }
}

const MAGIC: &[u8; 8] = b"ELIDWAL\x01";

// ---

/// A recovered record from the write log.
#[derive(Debug)]
pub enum LogRecord {
    Data {
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        flags: WalFlags,
        /// Byte offset of the data payload within the WAL file. Used as a
        /// temporary extent index location to enable reads before promotion.
        /// The promotion step writes a clean segment file; this offset is
        /// internal to the WAL and is not reused in the segment.
        body_offset: u64,
        /// Raw payload bytes (compressed if flags contains WalFlags::COMPRESSED).
        data: Vec<u8>,
    },
    Ref {
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
    },
    Zero {
        start_lba: u64,
        lba_length: u32,
    },
}

/// An open write log, ready for appending.
pub struct WriteLog {
    writer: File,
    /// Current byte size of the log, for flush-threshold checks.
    size: u64,
}

impl WriteLog {
    /// Current byte size of the log file.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl WriteLog {
    /// Create a new write log. Fails if the file already exists.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut wl = Self {
            writer: file,
            size: 0,
        };
        wl.write_all_bytes(MAGIC)?;
        Ok(wl)
    }

    /// Reopen an existing write log for continued appending after recovery.
    /// `size` should be the valid byte count returned by scan().
    pub fn reopen(path: &Path, size: u64) -> io::Result<Self> {
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self { writer: file, size })
    }

    /// Append a new data extent. `data` must already be compressed if `WalFlags::COMPRESSED` is set.
    /// `WalFlags::DEDUP_REF` must not be set in flags.
    ///
    /// Returns the byte offset of the data payload within the WAL file, for use
    /// as the temporary extent index location to enable reads before promotion.
    pub fn append_data(
        &mut self,
        start_lba: u64,
        lba_length: u32,
        hash: &blake3::Hash,
        flags: WalFlags,
        data: &[u8],
    ) -> io::Result<u64> {
        debug_assert!(
            !flags.contains(WalFlags::DEDUP_REF),
            "use append_ref for dedup references"
        );

        let mut header = Vec::with_capacity(32 + 10 + 5 + 1 + 5);
        header.extend_from_slice(hash.as_bytes());
        push_varint(&mut header, start_lba);
        push_varint32(&mut header, lba_length);
        header.push(flags.bits());
        push_varint32(&mut header, data.len() as u32);

        self.write_all_bytes(&header)?;
        let body_offset = self.size;
        self.write_all_bytes(data)?;
        Ok(body_offset)
    }

    /// Append a thin dedup reference (no body bytes).
    ///
    /// Only the hash + LBA mapping is recorded. The extent body stays in
    /// the canonical segment; reads resolve via the extent index.
    pub fn append_ref(
        &mut self,
        start_lba: u64,
        lba_length: u32,
        hash: &blake3::Hash,
    ) -> io::Result<()> {
        let mut rec = Vec::with_capacity(32 + 10 + 5 + 1);
        rec.extend_from_slice(hash.as_bytes());
        push_varint(&mut rec, start_lba);
        push_varint32(&mut rec, lba_length);
        rec.push(WalFlags::DEDUP_REF.bits());

        self.write_all_bytes(&rec)
    }

    /// Append a zero-extent record. No data payload is written.
    ///
    /// The entire LBA range will read back as zeros. A single record can cover
    /// the full volume — there is no chunking needed since there is no data body.
    pub fn append_zero(&mut self, start_lba: u64, lba_length: u32) -> io::Result<()> {
        let mut rec = Vec::with_capacity(32 + 10 + 5 + 1);
        rec.extend_from_slice(&[0u8; 32]); // ZERO_HASH sentinel
        push_varint(&mut rec, start_lba);
        push_varint32(&mut rec, lba_length);
        rec.push(WalFlags::ZERO.bits());

        self.write_all_bytes(&rec)
    }

    /// Fsync the log to disk.
    /// Call this when the guest issues an fsync — everything before this point is durable.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.writer.sync_data()
    }

    fn write_all_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)?;
        self.size += data.len() as u64;
        Ok(())
    }
}

/// Scan an existing write log without modifying it.
///
/// Returns `(records, has_partial_tail)`. A partial tail (e.g. from power loss
/// mid-write or an active WAL being written to) is noted but not truncated.
/// Use `scan` instead when truncation is desired (e.g. crash recovery).
pub fn scan_readonly(path: &Path) -> io::Result<(Vec<LogRecord>, bool)> {
    let data = std::fs::read(path)?;

    if data.len() < MAGIC.len() || &data[..MAGIC.len()] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad write log magic",
        ));
    }

    let mut pos = MAGIC.len();
    let mut records = Vec::new();

    while pos < data.len() {
        match parse_record(&data, &mut pos) {
            Ok(record) => records.push(record),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok((records, true));
            }
            Err(e) => return Err(e),
        }
    }

    Ok((records, false))
}

/// Scan an existing write log. Returns all complete records and the valid byte count.
///
/// If a partial record is found at the tail (e.g. due to power loss mid-write),
/// the file is truncated to the last complete record.
pub fn scan(path: &Path) -> io::Result<(Vec<LogRecord>, u64)> {
    let data = std::fs::read(path)?;

    if data.len() < MAGIC.len() || &data[..MAGIC.len()] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad write log magic",
        ));
    }

    let mut pos = MAGIC.len();
    let mut records = Vec::new();
    let mut last_good = pos;

    while pos < data.len() {
        match parse_record(&data, &mut pos) {
            Ok(record) => {
                records.push(record);
                last_good = pos;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Partial write at tail (power loss mid-record) — truncate and stop.
                OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(last_good as u64)?;
                break;
            }
            Err(e) => return Err(e), // InvalidData (corruption) — don't silently truncate
        }
    }

    Ok((records, last_good as u64))
}

// --- record parsing ---

fn parse_record(data: &[u8], pos: &mut usize) -> io::Result<LogRecord> {
    let hash = read_hash(data, pos)?;
    let start_lba = read_varint(data, pos)?;
    let lba_length = read_varint32(data, pos)?;
    let flags = WalFlags::from_bits_retain(read_u8(data, pos)?);

    if flags.contains(WalFlags::DEDUP_REF) {
        // Thin REF: no body bytes, just hash + LBA mapping.
        return Ok(LogRecord::Ref {
            hash,
            start_lba,
            lba_length,
        });
    }

    if flags.contains(WalFlags::ZERO) {
        return Ok(LogRecord::Zero {
            start_lba,
            lba_length,
        });
    }

    let data_len = read_varint32(data, pos)? as usize;
    let body_offset = *pos as u64;
    let payload = read_bytes(data, pos, data_len)?.to_vec();

    // When compressed, verify against the content hash (hash of uncompressed data).
    // When uncompressed, verify directly.
    let verify_against = if flags.contains(WalFlags::COMPRESSED) {
        lz4_flex::decompress_size_prepended(payload.as_slice())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "decompression failed"))?
    } else {
        payload.clone()
    };
    if blake3::hash(&verify_against) != hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "extent hash mismatch",
        ));
    }

    Ok(LogRecord::Data {
        hash,
        start_lba,
        lba_length,
        flags,
        body_offset,
        data: payload,
    })
}

// --- slice-based read helpers ---

fn read_hash(data: &[u8], pos: &mut usize) -> io::Result<blake3::Hash> {
    Ok(blake3::Hash::from_bytes(read_fixed(data, pos)?))
}

fn read_fixed<const N: usize>(data: &[u8], pos: &mut usize) -> io::Result<[u8; N]> {
    read_bytes(data, pos, N)?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated record"))
}

fn read_bytes<'a>(data: &'a [u8], pos: &mut usize, n: usize) -> io::Result<&'a [u8]> {
    if *pos + n > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated record",
        ));
    }
    let slice = &data[*pos..*pos + n];
    *pos += n;
    Ok(slice)
}

fn read_u8(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated record",
        ));
    }
    let b = data[*pos];
    *pos += 1;
    Ok(b)
}

fn read_varint(data: &[u8], pos: &mut usize) -> io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let b = read_u8(data, pos)? as u64;
        result |= (b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "varint overflow",
            ));
        }
    }
}

fn read_varint32(data: &[u8], pos: &mut usize) -> io::Result<u32> {
    let v = read_varint(data, pos)?;
    u32::try_from(v).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "varint32 overflow"))
}

// --- write varint helpers ---

fn push_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(b);
            break;
        }
        buf.push(b | 0x80);
    }
}

fn push_varint32(buf: &mut Vec<u8>, v: u32) {
    push_varint(buf, v as u64);
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-writelog-test-{}-{}", std::process::id(), n));
        p
    }

    #[test]
    fn roundtrip_data_record() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let payload = b"hello extent data";
        let hash = blake3::hash(payload);

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_data(42, 4, &hash, WalFlags::empty(), payload)
            .unwrap();
        wl.fsync().unwrap();
        drop(wl);

        let (records, size) = scan(&path).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            LogRecord::Data {
                hash: h,
                start_lba,
                lba_length,
                flags,
                body_offset,
                data,
            } => {
                assert_eq!(h, &hash);
                assert_eq!(*start_lba, 42);
                assert_eq!(*lba_length, 4);
                assert_eq!(*flags, WalFlags::empty());
                assert_eq!(data, payload);
                // body_offset points past the record header to the data bytes
                assert_eq!(*body_offset, size - payload.len() as u64);
            }
            _ => panic!("expected Data record"),
        }
        assert_eq!(size, std::fs::metadata(&path).unwrap().len());

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_ref_record() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let hash = blake3::hash(b"some existing extent data");

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_ref(100, 8, &hash).unwrap();
        wl.fsync().unwrap();
        drop(wl);

        let (records, _) = scan(&path).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            LogRecord::Ref {
                hash: h,
                start_lba,
                lba_length,
            } => {
                assert_eq!(h, &hash);
                assert_eq!(*start_lba, 100);
                assert_eq!(*lba_length, 8);
            }
            _ => panic!("expected Ref record"),
        }

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn truncates_partial_tail() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let payload = b"good extent";
        let hash = blake3::hash(payload);

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_data(0, 1, &hash, WalFlags::empty(), payload)
            .unwrap();
        wl.fsync().unwrap();
        let good_size = wl.size();
        drop(wl);

        // Append garbage to simulate a partial write.
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::End(0)).unwrap();
            f.write_all(b"\xde\xad\xbe\xef partial").unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > good_size);

        let (records, size) = scan(&path).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(size, good_size);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good_size);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn reopen_and_continue() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let h1 = blake3::hash(b"extent one");
        let h2 = blake3::hash(b"extent two");

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_data(0, 2, &h1, WalFlags::empty(), b"extent one")
            .unwrap();
        wl.fsync().unwrap();
        let size_after_first = wl.size();
        drop(wl);

        // Simulate crash-recovery then reopen.
        let (_, valid_size) = scan(&path).unwrap();
        assert_eq!(valid_size, size_after_first);

        let mut wl = WriteLog::reopen(&path, valid_size).unwrap();
        wl.append_ref(10, 2, &h2).unwrap();
        wl.fsync().unwrap();
        drop(wl);

        let (records, _) = scan(&path).unwrap();
        assert_eq!(records.len(), 2);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn corrupted_payload_returns_error() {
        // Corruption mid-file should propagate as InvalidData, not silently truncate.
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let payload = b"uncorrupted extent data";
        let hash = blake3::hash(payload);

        let mut wl = WriteLog::create(&path).unwrap();
        let body_offset = wl
            .append_data(0, 1, &hash, WalFlags::empty(), payload)
            .unwrap();
        wl.fsync().unwrap();
        drop(wl);

        // Flip a byte in the payload.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(body_offset)).unwrap();
            f.write_all(&[0xffu8]).unwrap();
        }

        let err = scan(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("hash mismatch"));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn mid_file_corruption_does_not_truncate_good_records() {
        // Two records: first corrupted, second valid. scan() should error,
        // not truncate to before the first record and return empty.
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let p1 = b"first extent";
        let p2 = b"second extent";
        let h1 = blake3::hash(p1);
        let h2 = blake3::hash(p2);

        let mut wl = WriteLog::create(&path).unwrap();
        let body1 = wl.append_data(0, 1, &h1, WalFlags::empty(), p1).unwrap();
        wl.append_data(8, 1, &h2, WalFlags::empty(), p2).unwrap();
        wl.fsync().unwrap();
        drop(wl);

        // Corrupt the first record's payload.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(body1)).unwrap();
            f.write_all(&[0xffu8]).unwrap();
        }

        let err = scan(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_zero_record() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_zero(256, 1024).unwrap();
        wl.fsync().unwrap();
        drop(wl);

        let (records, _) = scan(&path).unwrap();
        assert_eq!(records.len(), 1);
        match &records[0] {
            LogRecord::Zero {
                start_lba,
                lba_length,
            } => {
                assert_eq!(*start_lba, 256);
                assert_eq!(*lba_length, 1024);
            }
            _ => panic!("expected Zero record"),
        }

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn flag_compressed_roundtrips() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let original = b"compressed payload bytes - original uncompressed content";
        let compressed = lz4_flex::compress_prepend_size(original);
        // Hash is always of the uncompressed content.
        let hash = blake3::hash(original);

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_data(512, 6, &hash, WalFlags::COMPRESSED, &compressed)
            .unwrap();
        wl.fsync().unwrap();
        drop(wl);

        let (records, _) = scan(&path).unwrap();
        match &records[0] {
            LogRecord::Data {
                flags: f, data: d, ..
            } => {
                assert_eq!(*f, WalFlags::COMPRESSED);
                // The stored bytes are the compressed form.
                assert_eq!(d.as_slice(), compressed.as_slice());
            }
            _ => panic!("expected Data record"),
        }

        std::fs::remove_file(&path).unwrap();
    }
}
