// Write log: the in-progress segment before it is promoted to a local segment.
//
// Each record is either:
//   DATA — a new extent with its (optionally compressed) payload
//   REF  — a dedup reference: an LBA mapping to an already-stored extent
//
// The log is an append-only file living in wal/<ULID>. On promotion it is
// renamed to pending/<ULID> (zero-copy); the same ULID becomes the segment ID.
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
// Record layout (REF, FLAG_DEDUP_REF set):
//   hash        (32 bytes)
//   start_lba   (u64 varint)
//   lba_length  (u32 varint)
//   flags       (u8)          FLAG_DEDUP_REF set; no further fields

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

// --- flag bits ---

/// Payload is zstd-compressed; data_length is the compressed size.
pub const FLAG_COMPRESSED: u8 = 0x01;
/// No data payload; this LBA range maps to an existing extent identified by hash.
pub const FLAG_DEDUP_REF: u8 = 0x02;
/// High-entropy extent: bypass global dedup service, store in per-volume segment.
pub const FLAG_HIGH_ENTROPY: u8 = 0x04;

const MAGIC: &[u8; 8] = b"PLMPWL\x00\x01";

// ---

/// A recovered record from the write log.
pub enum LogRecord {
    Data {
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        flags: u8,
        /// Byte offset of the payload within the WAL file. Because the WAL is
        /// renamed directly to the segment body on promotion, this is also the
        /// `body_offset` to record in the segment `.idx` for this extent.
        body_offset: u64,
        /// Raw payload bytes (compressed if flags & FLAG_COMPRESSED).
        data: Vec<u8>,
    },
    Ref {
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
    },
}

/// An open write log, ready for appending.
pub struct WriteLog {
    writer: BufWriter<File>,
    /// Current byte size of the log, for flush-threshold checks.
    pub size: u64,
}

impl WriteLog {
    /// Create a new write log. Fails if the file already exists.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut wl = Self {
            writer: BufWriter::new(file),
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
        Ok(Self {
            writer: BufWriter::new(file),
            size,
        })
    }

    /// Append a new data extent. `data` must already be compressed if FLAG_COMPRESSED is set.
    /// FLAG_DEDUP_REF must not be set in flags.
    ///
    /// Returns the byte offset of the data payload within the WAL file. Because the WAL is
    /// renamed directly to the segment body on promotion (zero copy), this offset is also the
    /// `body_offset` recorded in the segment `.idx` for this extent.
    pub fn append_data(
        &mut self,
        start_lba: u64,
        lba_length: u32,
        hash: &blake3::Hash,
        flags: u8,
        data: &[u8],
    ) -> io::Result<u64> {
        debug_assert!(
            flags & FLAG_DEDUP_REF == 0,
            "use append_ref for dedup references"
        );

        let mut header = Vec::with_capacity(32 + 10 + 5 + 1 + 5);
        header.extend_from_slice(hash.as_bytes());
        push_varint(&mut header, start_lba);
        push_varint32(&mut header, lba_length);
        header.push(flags);
        push_varint32(&mut header, data.len() as u32);

        self.write_all_bytes(&header)?;
        let body_offset = self.size;
        self.write_all_bytes(data)?;
        Ok(body_offset)
    }

    /// Append a dedup reference. No data payload is written.
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
        rec.push(FLAG_DEDUP_REF);

        self.write_all_bytes(&rec)
    }

    /// Flush buffered writes and fsync the log to disk.
    /// Call this when the guest issues an fsync — everything before this point is durable.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()
    }

    fn write_all_bytes(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)?;
        self.size += data.len() as u64;
        Ok(())
    }
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
            Err(_) => {
                // Partial write at tail — truncate the file and stop.
                OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(last_good as u64)?;
                break;
            }
        }
    }

    Ok((records, last_good as u64))
}

// --- record parsing ---

fn parse_record(data: &[u8], pos: &mut usize) -> io::Result<LogRecord> {
    let hash = read_hash(data, pos)?;
    let start_lba = read_varint(data, pos)?;
    let lba_length = read_varint32(data, pos)?;
    let flags = read_u8(data, pos)?;

    if flags & FLAG_DEDUP_REF != 0 {
        return Ok(LogRecord::Ref {
            hash,
            start_lba,
            lba_length,
        });
    }

    let data_len = read_varint32(data, pos)? as usize;
    let body_offset = *pos as u64;
    let payload = read_bytes(data, pos, data_len)?.to_vec();

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
    let bytes: [u8; 32] = read_bytes(data, pos, 32)?
        .try_into()
        .expect("read_bytes(32) always returns 32 bytes");
    Ok(blake3::Hash::from_bytes(bytes))
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
        p.push(format!(
            "palimpsest-writelog-test-{}-{}",
            std::process::id(),
            n
        ));
        p
    }

    #[test]
    fn roundtrip_data_record() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let payload = b"hello extent data";
        let hash = blake3::hash(payload);

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_data(42, 4, &hash, 0, payload).unwrap();
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
                assert_eq!(*flags, 0);
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

        let hash = blake3::hash(b"some existing extent");

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
        wl.append_data(0, 1, &hash, 0, payload).unwrap();
        wl.fsync().unwrap();
        let good_size = wl.size;
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
        wl.append_data(0, 2, &h1, 0, b"extent one").unwrap();
        wl.fsync().unwrap();
        let size_after_first = wl.size;
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
    fn flags_compressed_and_high_entropy() {
        let path = temp_path();
        let _ = std::fs::remove_file(&path);

        let payload = b"compressed payload bytes";
        let hash = blake3::hash(payload);
        let flags = FLAG_COMPRESSED | FLAG_HIGH_ENTROPY;

        let mut wl = WriteLog::create(&path).unwrap();
        wl.append_data(512, 6, &hash, flags, payload).unwrap();
        wl.fsync().unwrap();
        drop(wl);

        let (records, _) = scan(&path).unwrap();
        match &records[0] {
            LogRecord::Data { flags: f, .. } => {
                assert_eq!(*f, FLAG_COMPRESSED | FLAG_HIGH_ENTROPY);
            }
            _ => panic!("expected Data record"),
        }

        std::fs::remove_file(&path).unwrap();
    }
}
