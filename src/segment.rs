// Segment index (.idx) writer, reader, and WAL promotion pipeline.
//
// The .idx file records per-extent metadata for a segment. It is always written
// before the segment body is committed (before the wal/ → pending/ rename), so
// every segment found in pending/ or segments/ has a valid .idx alongside it.
//
// .idx entry format (see DESIGN.md "S3 Layout and Index"):
//   hash        (32 bytes) — BLAKE3 extent hash
//   start_lba   (8 bytes)  — u64 le; first logical block address
//   lba_length  (4 bytes)  — u32 le; extent length in 4KB blocks
//   flags       (1 byte)   — bit 0: inline, bit 1: delta, bit 2: compressed
//
//   if !inline:
//     body_offset (8 bytes) — u64 le; byte offset within segment body
//     body_length (4 bytes) — u32 le; byte length in segment body
//
//   if inline:
//     body_length (4 bytes) — u32 le; byte length of inline data
//     data bytes  (body_length bytes)
//
// Promotion commit ordering:
//   1. Build .idx entries from session extent list
//   2. Write  pending/<ULID>.idx.tmp  (fsynced)
//   3. Rename pending/<ULID>.idx.tmp → pending/<ULID>.idx
//   4. Rename wal/<ULID>             → pending/<ULID>   ← COMMIT POINT
//
// Step 4 is atomic. The .idx always exists before the body appears (step 3
// before step 4), so LBA map rebuild on startup needs only .idx files.

use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::writelog;

// --- flag bits for .idx entries (distinct from writelog FLAG_* constants) ---

/// Extent bytes are stored inline in the .idx (no segment body reference).
pub const IDX_FLAG_INLINE: u8 = 0x01;
/// Extent is a delta against a source extent (not yet implemented).
#[allow(dead_code)]
pub const IDX_FLAG_DELTA: u8 = 0x02;
/// Payload is zstd-compressed.
pub const IDX_FLAG_COMPRESSED: u8 = 0x04;

/// Extents at or below this byte size are stored inline in the .idx.
/// 0 = inlining disabled. The main benefit is eliminating S3 byte-range GETs
/// for tiny extents; it is not meaningful until S3 integration.
const INLINE_THRESHOLD: usize = 0;

// ---

/// One entry in the in-memory representation of a segment .idx.
///
/// Built from `WriteLog` DATA records during a write session (via
/// [`IdxEntry::from_wal_data`]) or from an on-disk `.idx` file (via
/// [`read_idx`]).
pub struct IdxEntry {
    pub hash: blake3::Hash,
    pub start_lba: u64,
    pub lba_length: u32,
    /// True if the payload is zstd-compressed.
    pub compressed: bool,
    /// Byte offset of the payload within the segment body.
    /// Zero for inline entries (data lives in `inline_data`).
    pub body_offset: u64,
    /// Byte length of the payload (compressed size if `compressed`).
    pub body_length: u32,
    /// Extent bytes for inline entries. Empty for reference entries — the data
    /// lives in the segment body at `body_offset`.
    pub inline_data: Vec<u8>,
}

impl IdxEntry {
    /// Create from a DATA record appended to the WAL.
    ///
    /// `body_offset` is the value returned by [`writelog::WriteLog::append_data`].
    /// `data` is the raw payload (compressed if `FLAG_COMPRESSED` is set in `flags`).
    pub fn from_wal_data(
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        flags: u8,
        body_offset: u64,
        data: Vec<u8>,
    ) -> Self {
        let body_length = data.len() as u32;
        // Inline small extents when inlining is enabled.
        // INLINE_THRESHOLD is 0 (disabled) until S3 integration; the comparison
        // is intentionally a no-op for now and will become meaningful when enabled.
        #[allow(clippy::absurd_extreme_comparisons)]
        let inline = INLINE_THRESHOLD != 0 && data.len() <= INLINE_THRESHOLD;
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: flags & writelog::FLAG_COMPRESSED != 0,
            body_offset: if inline { 0 } else { body_offset },
            body_length,
            inline_data: if inline { data } else { Vec::new() },
        }
    }

    fn is_inline(&self) -> bool {
        !self.inline_data.is_empty()
    }
}

// --- .idx writer ---

/// Write a segment `.idx` to `path`.
///
/// `path` should be the `.idx.tmp` path; rename to `.idx` after this returns.
/// The file is fsynced before returning so the rename is crash-safe.
pub fn write_idx(path: &Path, entries: &[IdxEntry]) -> io::Result<()> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut w = BufWriter::new(file);
    for entry in entries {
        write_entry(&mut w, entry)?;
    }
    w.flush()?;
    w.get_ref().sync_data()?;
    Ok(())
}

fn write_entry<W: Write>(w: &mut W, e: &IdxEntry) -> io::Result<()> {
    let mut flags: u8 = 0;
    if e.is_inline() {
        flags |= IDX_FLAG_INLINE;
    }
    if e.compressed {
        flags |= IDX_FLAG_COMPRESSED;
    }

    w.write_all(e.hash.as_bytes())?;
    w.write_all(&e.start_lba.to_le_bytes())?;
    w.write_all(&e.lba_length.to_le_bytes())?;
    w.write_all(&[flags])?;

    if e.is_inline() {
        w.write_all(&e.body_length.to_le_bytes())?;
        w.write_all(&e.inline_data)?;
    } else {
        w.write_all(&e.body_offset.to_le_bytes())?;
        w.write_all(&e.body_length.to_le_bytes())?;
    }
    Ok(())
}

// --- .idx reader ---

/// Read a segment `.idx` file and return its entries.
/// Used during LBA map rebuild on startup.
pub fn read_idx(path: &Path) -> io::Result<Vec<IdxEntry>> {
    let data = fs::read(path)?;
    let mut pos = 0;
    let mut entries = Vec::new();
    while pos < data.len() {
        entries.push(parse_entry(&data, &mut pos)?);
    }
    Ok(entries)
}

fn parse_entry(data: &[u8], pos: &mut usize) -> io::Result<IdxEntry> {
    let hash_bytes: [u8; 32] = read_bytes(data, pos, 32)?
        .try_into()
        .expect("read_bytes(32) always returns 32 bytes");
    let hash = blake3::Hash::from_bytes(hash_bytes);

    let start_lba = u64::from_le_bytes(read_fixed(data, pos)?);
    let lba_length = u32::from_le_bytes(read_fixed(data, pos)?);
    let flags = read_u8(data, pos)?;

    let inline = flags & IDX_FLAG_INLINE != 0;
    let compressed = flags & IDX_FLAG_COMPRESSED != 0;

    if inline {
        let body_length = u32::from_le_bytes(read_fixed(data, pos)?);
        let payload = read_bytes(data, pos, body_length as usize)?.to_vec();
        Ok(IdxEntry {
            hash,
            start_lba,
            lba_length,
            compressed,
            body_offset: 0,
            body_length,
            inline_data: payload,
        })
    } else {
        let body_offset = u64::from_le_bytes(read_fixed(data, pos)?);
        let body_length = u32::from_le_bytes(read_fixed(data, pos)?);
        Ok(IdxEntry {
            hash,
            start_lba,
            lba_length,
            compressed,
            body_offset,
            body_length,
            inline_data: Vec::new(),
        })
    }
}

// --- slice read helpers ---

fn read_bytes<'a>(data: &'a [u8], pos: &mut usize, n: usize) -> io::Result<&'a [u8]> {
    if *pos + n > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated .idx entry",
        ));
    }
    let slice = &data[*pos..*pos + n];
    *pos += n;
    Ok(slice)
}

fn read_fixed<const N: usize>(data: &[u8], pos: &mut usize) -> io::Result<[u8; N]> {
    read_bytes(data, pos, N)?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated .idx entry"))
}

fn read_u8(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated .idx entry",
        ));
    }
    let b = data[*pos];
    *pos += 1;
    Ok(b)
}

// --- promotion pipeline ---

/// Promote a WAL to a committed local segment.
///
/// `entries` is the list of DATA extents accumulated during the write session.
/// Build each entry with [`IdxEntry::from_wal_data`] using the `body_offset`
/// returned by [`writelog::WriteLog::append_data`].
///
/// REF (dedup) records are not included in `entries`; the caller maintains
/// the LBA map for those separately.
///
/// After returning, the caller should update the in-memory LBA map from
/// `entries` (start_lba + lba_length → hash).
pub fn promote(
    wal_path: &Path,
    ulid: &str,
    pending_dir: &Path,
    entries: &[IdxEntry],
) -> io::Result<()> {
    let idx_tmp = pending_dir.join(format!("{ulid}.idx.tmp"));
    let idx_path = pending_dir.join(format!("{ulid}.idx"));
    let body_path = pending_dir.join(ulid);

    // Steps 1–2: build and write .idx.tmp (fsynced inside write_idx).
    write_idx(&idx_tmp, entries)?;

    // Step 3: rename .idx.tmp → .idx.
    fs::rename(&idx_tmp, &idx_path)?;

    // Step 4: rename wal → pending body.  ← COMMIT POINT
    fs::rename(wal_path, &body_path)?;

    Ok(())
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "palimpsest-segment-test-{}-{}{}",
            std::process::id(),
            n,
            suffix
        ));
        p
    }

    fn make_entry(start_lba: u64, lba_length: u32, data: &[u8]) -> IdxEntry {
        IdxEntry::from_wal_data(
            blake3::hash(data),
            start_lba,
            lba_length,
            0,    // no flags
            1024, // arbitrary body_offset
            data.to_vec(),
        )
    }

    #[test]
    fn roundtrip_reference_entry() {
        let path = temp_path(".idx");
        let _ = std::fs::remove_file(&path);

        let data = vec![0u8; 4096];
        let entries = vec![make_entry(10, 1, &data)];

        write_idx(&path, &entries).unwrap();

        let read_back = read_idx(&path).unwrap();
        assert_eq!(read_back.len(), 1);

        let e = &read_back[0];
        assert_eq!(e.hash, blake3::hash(&data));
        assert_eq!(e.start_lba, 10);
        assert_eq!(e.lba_length, 1);
        assert!(!e.compressed);
        assert!(!e.is_inline());
        assert_eq!(e.body_offset, 1024);
        assert_eq!(e.body_length, 4096);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_compressed_entry() {
        let path = temp_path(".idx");
        let _ = std::fs::remove_file(&path);

        let data = vec![0xabu8; 2048];
        let hash = blake3::hash(&data);
        let entry =
            IdxEntry::from_wal_data(hash, 42, 2, writelog::FLAG_COMPRESSED, 8192, data.clone());

        write_idx(&path, &[entry]).unwrap();

        let read_back = read_idx(&path).unwrap();
        assert_eq!(read_back.len(), 1);

        let e = &read_back[0];
        assert!(e.compressed);
        assert_eq!(e.body_offset, 8192);
        assert_eq!(e.body_length, 2048);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_multiple_entries() {
        let path = temp_path(".idx");
        let _ = std::fs::remove_file(&path);

        let entries: Vec<IdxEntry> = (0..5u64)
            .map(|i| make_entry(i * 8, 8, &[i as u8; 4096]))
            .collect();

        write_idx(&path, &entries).unwrap();

        let read_back = read_idx(&path).unwrap();
        assert_eq!(read_back.len(), 5);

        for (i, e) in read_back.iter().enumerate() {
            assert_eq!(e.start_lba, i as u64 * 8);
            assert_eq!(e.lba_length, 8);
        }

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn promote_renames_files() {
        // Set up a temp dir structure: wal/, pending/
        let base = temp_path("");
        std::fs::create_dir_all(base.join("wal")).unwrap();
        std::fs::create_dir_all(base.join("pending")).unwrap();

        let ulid = "01JQEXAMPLEULID0000000000A";

        // Write a minimal WAL file.
        let wal_path = base.join("wal").join(ulid);
        {
            let mut wl = writelog::WriteLog::create(&wal_path).unwrap();
            let data = b"test extent payload";
            let hash = blake3::hash(data);
            let body_offset = wl.append_data(0, 1, &hash, 0, data).unwrap();
            wl.fsync().unwrap();

            let entries = vec![IdxEntry::from_wal_data(
                hash,
                0,
                1,
                0,
                body_offset,
                data.to_vec(),
            )];

            promote(&wal_path, ulid, &base.join("pending"), &entries).unwrap();
        }

        // WAL should be gone; pending body and .idx should exist.
        assert!(!wal_path.exists(), "WAL should have been renamed away");
        assert!(
            base.join("pending").join(ulid).exists(),
            "segment body missing"
        );
        assert!(
            base.join("pending").join(format!("{ulid}.idx")).exists(),
            ".idx missing"
        );
        assert!(
            !base
                .join("pending")
                .join(format!("{ulid}.idx.tmp"))
                .exists(),
            ".idx.tmp should be gone"
        );

        std::fs::remove_dir_all(base).unwrap();
    }
}
