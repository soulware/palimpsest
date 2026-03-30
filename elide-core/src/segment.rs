// Segment file: unified on-disk and S3 format.
//
// Layout:
//   [Header: 96 bytes]
//   [Index section: index_length bytes]   — starts at byte 96
//   [Inline section: inline_length bytes] — starts at byte 96 + index_length
//   [Full body: body_length bytes]        — starts at byte 96 + index_length + inline_length
//   [Delta body: delta_length bytes]      — absent locally (delta_length = 0)
//
// Header (96 bytes):
//   0..8   magic         "ELIDSEG\x02"
//   8..12  entry_count   u32 le
//   12..16 index_length  u32 le
//   16..20 inline_length u32 le; 0 if no inline data
//   20..28 body_length   u64 le
//   28..32 delta_length  u32 le; 0 for locally-stored files
//   32..96 signature     Ed25519 sig over BLAKE3(header[0..32] || index_bytes); zeros if unsigned
//
// Index entry format:
//   hash       (32 bytes) BLAKE3 extent hash
//   start_lba  (8 bytes)  u64 le
//   lba_length (4 bytes)  u32 le
//   flags      (1 byte)   see FLAG_* constants
//
//   if FLAG_DEDUP_REF: (no further fields — data lives in an ancestor segment)
//   else:
//     stored_offset (8 bytes) u64 le — offset within body or inline section
//     stored_length (4 bytes) u32 le — byte length of stored data
//
// Body section: raw concatenated DATA-record extent bytes, no framing.
// REF-record extents contribute nothing to the body.
//
// Locally-stored files have delta_length = 0. The local file IS the S3 object
// minus the delta body, which the coordinator appends at upload time.
//
// Promotion commit ordering:
//   1. Build entries in memory from WAL records
//   2. Write pending/<ULID>.tmp (fsynced)
//   3. Rename pending/<ULID>.tmp → pending/<ULID>   ← COMMIT POINT
//   4. Delete wal/<ULID>
//   5. Caller updates extent index (WAL offsets → segment offsets) and LBA map

use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// --- constants ---

const MAGIC: &[u8; 8] = b"ELIDSEG\x02";
const HEADER_LEN: u64 = 96;

/// Trait for signing segment files at promotion time.
///
/// The signing input is `BLAKE3(header[0..32] || index_section_bytes)`.
/// Returning a 64-byte Ed25519 signature. Implementations must be infallible
/// — a missing or corrupt key should be caught at startup, not per-segment.
pub trait SegmentSigner: Send + Sync {
    fn sign(&self, msg: &[u8]) -> [u8; 64];
}

/// Trait for fetching segments from remote storage on a local cache miss.
///
/// Implementations write the three-file fetched format to `fetched_dir`:
///   `<segment_id>.idx`     — header + index section (bytes `[0, body_section_start)`)
///   `<segment_id>.body`    — body bytes (body-relative offsets, byte 0 = first body byte)
///   `<segment_id>.present` — packed bitset, one bit per index entry; set = body bytes present
///
/// All three files are written atomically (tmp + rename). On success,
/// `<fetched_dir>/<segment_id>.body` is readable for extent data.
///
/// `elide-core` is synchronous; async fetchers must wrap their runtime (e.g.
/// `Runtime::block_on`) inside this interface.
pub trait SegmentFetcher: Send + Sync {
    /// Fetch `segment_id` from remote storage and write the three-file fetched
    /// format into `fetched_dir`. Returns `Ok(())` on success or an error if
    /// the segment cannot be fetched.
    fn fetch(&self, segment_id: &str, fetched_dir: &Path) -> io::Result<()>;
}

/// Convenience alias for an optional heap-allocated `SegmentFetcher`.
pub type BoxFetcher = Arc<dyn SegmentFetcher>;

/// Size of a DEDUP_REF index entry: hash(32) + start_lba(8) + lba_length(4) + flags(1).
const IDX_ENTRY_REF_LEN: u32 = 45;
/// Size of a DATA index entry: above + stored_offset(8) + stored_length(4).
const IDX_ENTRY_DATA_LEN: u32 = 57;

/// Extents at or below this byte size are stored inline in the inline section.
/// 0 = disabled until S3 integration.
const INLINE_THRESHOLD: usize = 0;

// --- flag bits ---

/// Extent data is in the inline section; stored_offset is inline-section-relative.
pub const FLAG_INLINE: u8 = 0x01;
/// One or more delta options follow this entry (S3 only; not used locally).
#[allow(dead_code)]
pub const FLAG_HAS_DELTAS: u8 = 0x02;
/// Stored data is zstd-compressed; stored_length is the compressed size.
pub const FLAG_COMPRESSED: u8 = 0x04;
/// Extent data lives in an ancestor segment; no bytes in this segment's body.
pub const FLAG_DEDUP_REF: u8 = 0x08;

// --- SegmentEntry ---

/// One entry in the in-memory representation of a segment's index section.
///
/// Used in two contexts:
/// - Entries accumulated during a write session for promotion: `data` holds raw
///   extent bytes and `stored_offset` is filled in by `write_segment`.
/// - Entries read from an existing segment during startup rebuild: `data` is
///   empty (not needed; body lives on disk). `stored_offset` is section-relative.
#[derive(Debug)]
pub struct SegmentEntry {
    pub hash: blake3::Hash,
    pub start_lba: u64,
    pub lba_length: u32,
    pub compressed: bool,
    /// True for dedup references (FLAG_DEDUP_REF). No bytes in this segment.
    pub is_dedup_ref: bool,
    /// True if extent bytes are in the inline section rather than the body.
    pub is_inline: bool,
    /// Section-relative byte offset. For body entries: offset within the body
    /// section. For inline entries: offset within the inline section. Zero for
    /// dedup refs. Set by `write_segment` when writing; already set when reading.
    pub stored_offset: u64,
    /// Byte length of the stored data (compressed size if `compressed`).
    /// Zero for dedup refs.
    pub stored_length: u32,
    /// Raw extent bytes for entries being built for promotion. Empty for dedup
    /// refs and for entries read from a committed segment (data stays on disk).
    pub data: Vec<u8>,
}

impl SegmentEntry {
    /// Create a DATA entry from a written extent.
    ///
    /// `flags` may include `FLAG_COMPRESSED` if `data` is already compressed.
    /// `data` is moved in — no copy.
    pub fn new_data(
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        flags: u8,
        data: Vec<u8>,
    ) -> Self {
        let stored_length = data.len() as u32;
        #[allow(clippy::absurd_extreme_comparisons)]
        let is_inline = INLINE_THRESHOLD != 0 && data.len() <= INLINE_THRESHOLD;
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: flags & FLAG_COMPRESSED != 0,
            is_dedup_ref: false,
            is_inline,
            stored_offset: 0, // filled by write_segment
            stored_length,
            data,
        }
    }

    /// Create a DEDUP_REF entry (no data payload — extent lives in an ancestor segment).
    pub fn new_dedup_ref(hash: blake3::Hash, start_lba: u64, lba_length: u32) -> Self {
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: false,
            is_dedup_ref: true,
            is_inline: false,
            stored_offset: 0,
            stored_length: 0,
            data: Vec::new(),
        }
    }
}

// --- write ---

/// Write a segment file in the unified format.
///
/// Assigns `stored_offset` for each entry, writes header + index + inline +
/// body, and fsyncs before returning. `path` should be the `.tmp` path; the
/// caller renames it to the final name after this returns.
///
/// If `signer` is provided the segment is signed: the 64-byte Ed25519
/// signature over `BLAKE3(header[0..32] || index_bytes)` is written at
/// `header[32..96]`. Without a signer the signature field is all zeros.
///
/// Returns `body_section_start` (the absolute byte offset of the body section
/// in the file). Callers use this to convert body entries' `stored_offset` to
/// absolute file offsets: `body_section_start + entry.stored_offset`.
pub fn write_segment(
    path: &Path,
    entries: &mut [SegmentEntry],
    signer: Option<&dyn SegmentSigner>,
) -> io::Result<u64> {
    let (index_length, inline_length, body_length) = assign_offsets(entries);
    let body_section_start = HEADER_LEN + index_length as u64 + inline_length as u64;

    // Build index section into a buffer first — needed for signing.
    let mut index_buf = Vec::with_capacity(index_length as usize);
    for entry in entries.iter() {
        write_index_entry(&mut index_buf, entry)?;
    }

    // Build the first 32 bytes of the header (fields only, no signature).
    let header_fields = build_header_fields(
        entries.len() as u32,
        index_length,
        inline_length,
        body_length,
    );

    // Compute signature: BLAKE3(header[0..32] || index_bytes), then sign.
    let signature: [u8; 64] = if let Some(s) = signer {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header_fields);
        hasher.update(&index_buf);
        s.sign(hasher.finalize().as_bytes())
    } else {
        [0u8; 64]
    };

    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut w = BufWriter::new(file);

    // Header: 32 bytes of fields + 64 bytes of signature = 96 bytes.
    w.write_all(&header_fields)?;
    w.write_all(&signature)?;
    w.write_all(&index_buf)?;

    // Inline section (currently empty: INLINE_THRESHOLD = 0).
    for entry in entries.iter() {
        if entry.is_inline {
            w.write_all(&entry.data)?;
        }
    }
    // Body section: DATA extents only, raw bytes, no framing.
    for entry in entries.iter() {
        if !entry.is_dedup_ref && !entry.is_inline {
            w.write_all(&entry.data)?;
        }
    }

    w.flush()?;
    w.get_ref().sync_data()?;
    Ok(body_section_start)
}

/// Assign `stored_offset` for each entry and return section sizes.
///
/// Modifies entries in-place; `stored_offset` is meaningful only after this call.
fn assign_offsets(entries: &mut [SegmentEntry]) -> (u32, u32, u64) {
    let mut index_length: u32 = 0;
    let mut inline_cursor: u64 = 0;
    let mut body_cursor: u64 = 0;

    for entry in entries.iter_mut() {
        index_length += if entry.is_dedup_ref {
            IDX_ENTRY_REF_LEN
        } else {
            IDX_ENTRY_DATA_LEN
        };

        if entry.is_inline {
            entry.stored_offset = inline_cursor;
            inline_cursor += entry.stored_length as u64;
        } else if !entry.is_dedup_ref {
            entry.stored_offset = body_cursor;
            body_cursor += entry.stored_length as u64;
        }
    }

    (index_length, inline_cursor as u32, body_cursor)
}

/// Build the first 32 bytes of the segment header (the signed fields).
///
/// These are the bytes hashed when computing the segment signature.
fn build_header_fields(
    entry_count: u32,
    index_length: u32,
    inline_length: u32,
    body_length: u64,
) -> [u8; 32] {
    let mut h = [0u8; 32];
    h[0..8].copy_from_slice(MAGIC);
    h[8..12].copy_from_slice(&entry_count.to_le_bytes());
    h[12..16].copy_from_slice(&index_length.to_le_bytes());
    h[16..20].copy_from_slice(&inline_length.to_le_bytes());
    h[20..28].copy_from_slice(&body_length.to_le_bytes());
    h[28..32].copy_from_slice(&0u32.to_le_bytes()); // delta_length = 0 locally
    h
}

fn write_index_entry<W: Write>(w: &mut W, e: &SegmentEntry) -> io::Result<()> {
    let mut flags: u8 = 0;
    if e.is_inline {
        flags |= FLAG_INLINE;
    }
    if e.compressed {
        flags |= FLAG_COMPRESSED;
    }
    if e.is_dedup_ref {
        flags |= FLAG_DEDUP_REF;
    }

    w.write_all(e.hash.as_bytes())?;
    w.write_all(&e.start_lba.to_le_bytes())?;
    w.write_all(&e.lba_length.to_le_bytes())?;
    w.write_all(&[flags])?;

    if !e.is_dedup_ref {
        // Both inline and body entries store offset + length (in their respective sections).
        w.write_all(&e.stored_offset.to_le_bytes())?;
        w.write_all(&e.stored_length.to_le_bytes())?;
    }
    Ok(())
}

// --- read ---

/// Read the index section of a segment file.
///
/// Reads only `[0, body_section_start)` — header + index section. Does not
/// read inline data, body, or delta.
///
/// Returns `(body_section_start, entries)`.
/// `body_section_start` is the absolute byte offset of the body section in the
/// file. Callers use it to convert body entries' `stored_offset` to absolute
/// file offsets: `body_section_start + entry.stored_offset`.
pub fn read_segment_index(path: &Path) -> io::Result<(u64, Vec<SegmentEntry>)> {
    let mut f = fs::File::open(path)?;

    // Header is 96 bytes: 32 bytes of fields + 64 bytes of signature.
    let mut h = [0u8; HEADER_LEN as usize];
    f.read_exact(&mut h)?;

    let mut pos = 0usize;
    let magic: [u8; 8] = read_fixed(&h, &mut pos)?;
    if magic != *MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad segment magic",
        ));
    }

    let entry_count = u32::from_le_bytes(read_fixed(&h, &mut pos)?);
    let index_length = u32::from_le_bytes(read_fixed(&h, &mut pos)?);
    let inline_length = u32::from_le_bytes(read_fixed(&h, &mut pos)?);
    let _body_length = u64::from_le_bytes(read_fixed::<8>(&h, &mut pos)?);
    let _delta_length = u32::from_le_bytes(read_fixed(&h, &mut pos)?);
    // Signature field bytes[32..96] — not parsed here; available for verification
    // via read_and_verify_segment_index when a SegmentSigner/verifier is present.

    let body_section_start = HEADER_LEN + index_length as u64 + inline_length as u64;

    let mut index_buf = vec![0u8; index_length as usize];
    f.read_exact(&mut index_buf)?;

    let entries = parse_index_section(&index_buf, entry_count)?;
    Ok((body_section_start, entries))
}

fn parse_index_section(data: &[u8], entry_count: u32) -> io::Result<Vec<SegmentEntry>> {
    let mut pos = 0usize;
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        let hash = blake3::Hash::from_bytes(read_fixed(data, &mut pos)?);
        let start_lba = u64::from_le_bytes(read_fixed(data, &mut pos)?);
        let lba_length = u32::from_le_bytes(read_fixed(data, &mut pos)?);
        let flags = read_u8(data, &mut pos)?;

        let is_dedup_ref = flags & FLAG_DEDUP_REF != 0;
        let is_inline = flags & FLAG_INLINE != 0;
        let compressed = flags & FLAG_COMPRESSED != 0;

        let (stored_offset, stored_length) = if is_dedup_ref {
            (0u64, 0u32)
        } else {
            let off = u64::from_le_bytes(read_fixed(data, &mut pos)?);
            let len = u32::from_le_bytes(read_fixed(data, &mut pos)?);
            (off, len)
        };

        entries.push(SegmentEntry {
            hash,
            start_lba,
            lba_length,
            compressed,
            is_dedup_ref,
            is_inline,
            stored_offset,
            stored_length,
            data: Vec::new(),
        });
    }

    Ok(entries)
}

/// Populate `entry.data` for all body entries by reading from the segment file.
///
/// `body_section_start` is the absolute file offset of the body section, as
/// returned by `read_segment_index`. Dedup-ref and inline entries are skipped.
/// After this call, each body entry's `data` holds exactly `stored_length` bytes
/// read from `body_section_start + stored_offset` in the file.
///
/// Used by the compaction path to materialise live extent bytes before writing
/// a new, denser segment.
pub fn read_extent_bodies(
    path: &Path,
    body_section_start: u64,
    entries: &mut [SegmentEntry],
) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = fs::File::open(path)?;
    for entry in entries.iter_mut() {
        if entry.is_dedup_ref || entry.is_inline {
            continue;
        }
        f.seek(SeekFrom::Start(body_section_start + entry.stored_offset))?;
        entry.data = vec![0u8; entry.stored_length as usize];
        f.read_exact(&mut entry.data)?;
    }
    Ok(())
}

// --- promotion ---

/// Promote a WAL to a committed local segment.
///
/// `entries` must be the DATA and REF records accumulated during the write
/// session. On return, each entry's `stored_offset` is set to its
/// section-relative position.
///
/// After returning, the caller must:
/// 1. Update the extent index for each DATA entry: the absolute file offset is
///    `body_section_start + entry.stored_offset` (body entries) or the inline
///    section (inline entries, not yet implemented).
/// 2. Open a fresh WAL.
///
/// Returns `body_section_start` so the caller can compute absolute offsets.
pub fn promote(
    wal_path: &Path,
    ulid: &str,
    pending_dir: &Path,
    entries: &mut [SegmentEntry],
    signer: Option<&dyn SegmentSigner>,
) -> io::Result<u64> {
    let tmp_path = pending_dir.join(format!("{ulid}.tmp"));
    let final_path = pending_dir.join(ulid);

    let body_section_start = write_segment(&tmp_path, entries, signer)?;

    // Atomic rename — COMMIT POINT.
    fs::rename(&tmp_path, &final_path)?;

    // WAL is now redundant; segment is the sole copy.
    fs::remove_file(wal_path)?;

    Ok(body_section_start)
}

// --- filesystem helpers ---

/// Collect all committed segment files in `dir` (files with valid ULID names,
/// no extension). Excludes `.tmp` files (incomplete promotions).
///
/// Used by `lbamap` and `extentindex` during startup rebuild.
pub fn collect_segment_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    match fs::read_dir(dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
        Ok(entries) => {
            let mut paths = Vec::new();
            for entry in entries {
                let path = entry?.path();
                if path.extension().is_some() {
                    continue; // skip .tmp and any other suffixed files
                }
                if let Some(name) = path.file_name().and_then(|s| s.to_str())
                    && ulid::Ulid::from_string(name).is_ok()
                {
                    paths.push(path);
                }
            }
            Ok(paths)
        }
    }
}

/// Collect all `.idx` files in `fetched_dir` whose stem is a valid ULID.
///
/// Used by `lbamap` and `extentindex` during startup rebuild to include
/// segments that were demand-fetched in a previous session. `.idx` files are
/// the persistent header+index portion of the fetched three-file format.
pub fn collect_fetched_idx_files(fetched_dir: &Path) -> io::Result<Vec<PathBuf>> {
    match fs::read_dir(fetched_dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
        Ok(entries) => {
            let mut paths = Vec::new();
            for entry in entries {
                let path = entry?.path();
                if path.extension().and_then(|e| e.to_str()) != Some("idx") {
                    continue;
                }
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                    && ulid::Ulid::from_string(stem).is_ok()
                {
                    paths.push(path);
                }
            }
            Ok(paths)
        }
    }
}

/// Sort segment paths for rebuild, with GC output segments first.
///
/// GC output segments — identified by a corresponding
/// `gc/<ulid>.{pending,applied,done}` handoff file — are processed before
/// non-GC segments.  Within each group, segments are sorted by ULID
/// (oldest first).  This ensures that any non-GC segment (a direct volume
/// write) overwrites a GC output's entry for the same LBA range, because
/// the non-GC segment is processed later and its insert replaces the
/// GC-derived entry in the LBA map.
///
/// Without this ordering, a GC output whose ULID exceeds a concurrent
/// write's segment ULID would be processed last, and its stale LBA
/// entries would shadow the concurrent write's correct entries.
pub fn sort_for_rebuild(fork_dir: &Path, paths: &mut Vec<PathBuf>) {
    let gc_dir = fork_dir.join("gc");
    // Partition into GC outputs and non-GC segments.
    let mut gc_paths: Vec<PathBuf> = Vec::new();
    let mut non_gc_paths: Vec<PathBuf> = Vec::new();
    for p in paths.drain(..) {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if is_gc_output(&gc_dir, name) {
            gc_paths.push(p);
        } else {
            non_gc_paths.push(p);
        }
    }
    gc_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));
    non_gc_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));
    // GC outputs first (lower priority), then non-GC segments (higher priority).
    paths.extend(gc_paths);
    paths.extend(non_gc_paths);
}

/// Returns true if `segment_name` is an in-flight GC output.
///
/// A segment is a GC output while its handoff file is in `.pending` or
/// `.applied` state — i.e. while the old input segments may still exist
/// in `segments/`. During this window `sort_for_rebuild` places these
/// segments first (lower priority) so the originals win on rebuild.
///
/// `.done` is intentionally excluded: by the time a handoff reaches
/// `.done` the coordinator has already deleted the old input segments,
/// so there is nothing to conflict with. ULID ordering (guaranteed by
/// `gc_checkpoint`) ensures the GC output sorts before any subsequent
/// write without needing special classification.
fn is_gc_output(gc_dir: &Path, segment_name: &str) -> bool {
    [".pending", ".applied"]
        .iter()
        .any(|suffix| gc_dir.join(format!("{segment_name}{suffix}")).exists())
}

// --- slice read helpers ---

fn read_fixed<const N: usize>(data: &[u8], pos: &mut usize) -> io::Result<[u8; N]> {
    if *pos + N > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated segment data",
        ));
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&data[*pos..*pos + N]);
    *pos += N;
    Ok(arr)
}

fn read_u8(data: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated segment data",
        ));
    }
    let b = data[*pos];
    *pos += 1;
    Ok(b)
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(suffix: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "elide-segment-test-{}-{}{}",
            std::process::id(),
            n,
            suffix
        ));
        p
    }

    fn temp_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "elide-segment-test-dir-{}-{}",
            std::process::id(),
            n
        ));
        p
    }

    #[test]
    fn roundtrip_single_data_entry() {
        let path = temp_path(".seg");
        let data = vec![0x42u8; 4096];
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_data(hash, 10, 1, 0, data.clone())];
        let bss = write_segment(&path, &mut entries, None).unwrap();

        let (bss2, read_entries) = read_segment_index(&path).unwrap();
        assert_eq!(bss, bss2);
        assert_eq!(read_entries.len(), 1);

        let e = &read_entries[0];
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 10);
        assert_eq!(e.lba_length, 1);
        assert!(!e.compressed);
        assert!(!e.is_dedup_ref);
        assert!(!e.is_inline);
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 4096);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_multiple_entries() {
        let path = temp_path(".seg");

        let mut entries: Vec<SegmentEntry> = (0..4u64)
            .map(|i| {
                let data = vec![i as u8; 4096];
                let hash = blake3::hash(&data);
                SegmentEntry::new_data(hash, i * 8, 2, 0, data)
            })
            .collect();

        write_segment(&path, &mut entries, None).unwrap();

        let (_, read_back) = read_segment_index(&path).unwrap();
        assert_eq!(read_back.len(), 4);

        // stored_offsets should be consecutive multiples of 4096.
        for (i, e) in read_back.iter().enumerate() {
            assert_eq!(e.start_lba, i as u64 * 8);
            assert_eq!(e.stored_offset, i as u64 * 4096);
            assert_eq!(e.stored_length, 4096);
        }

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_dedup_ref_entry() {
        let path = temp_path(".seg");
        let hash = blake3::hash(b"existing extent");

        let mut entries = vec![SegmentEntry::new_dedup_ref(hash, 5, 3)];
        write_segment(&path, &mut entries, None).unwrap();

        let (_, read_back) = read_segment_index(&path).unwrap();
        assert_eq!(read_back.len(), 1);

        let e = &read_back[0];
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 5);
        assert_eq!(e.lba_length, 3);
        assert!(e.is_dedup_ref);
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 0);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_mixed_data_and_ref() {
        let path = temp_path(".seg");

        let data = vec![0xABu8; 8192];
        let data_hash = blake3::hash(&data);
        let ref_hash = blake3::hash(b"some ancestor extent");

        let mut entries = vec![
            SegmentEntry::new_data(data_hash, 0, 2, 0, data),
            SegmentEntry::new_dedup_ref(ref_hash, 2, 1),
            SegmentEntry::new_data(blake3::hash(b"x"), 10, 1, 0, b"x".repeat(4096).to_vec()),
        ];

        write_segment(&path, &mut entries, None).unwrap();

        let (_, read_back) = read_segment_index(&path).unwrap();
        assert_eq!(read_back.len(), 3);

        assert!(!read_back[0].is_dedup_ref);
        assert_eq!(read_back[0].stored_offset, 0); // first body entry
        assert_eq!(read_back[0].stored_length, 8192);

        assert!(read_back[1].is_dedup_ref); // ref contributes no body bytes

        assert!(!read_back[2].is_dedup_ref);
        assert_eq!(read_back[2].stored_offset, 8192); // body cursor advanced past first entry
        assert_eq!(read_back[2].stored_length, 4096);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_compressed_entry() {
        let path = temp_path(".seg");
        let data = vec![0xCDu8; 2048]; // "compressed" payload
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_data(hash, 20, 1, FLAG_COMPRESSED, data)];
        write_segment(&path, &mut entries, None).unwrap();

        let (_, read_back) = read_segment_index(&path).unwrap();
        assert_eq!(read_back.len(), 1);
        assert!(read_back[0].compressed);
        assert_eq!(read_back[0].stored_length, 2048);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn body_bytes_readable_via_stored_offset() {
        // Verify that body bytes are actually written at the right offsets.
        use std::io::{Read, Seek, SeekFrom};

        let path = temp_path(".seg");
        let data1 = vec![0x11u8; 4096];
        let data2 = vec![0x22u8; 4096];
        let h1 = blake3::hash(&data1);
        let h2 = blake3::hash(&data2);

        let mut entries = vec![
            SegmentEntry::new_data(h1, 0, 1, 0, data1.clone()),
            SegmentEntry::new_data(h2, 1, 1, 0, data2.clone()),
        ];

        let bss = write_segment(&path, &mut entries, None).unwrap();

        let (bss2, index) = read_segment_index(&path).unwrap();
        assert_eq!(bss, bss2);

        let mut f = fs::File::open(&path).unwrap();

        let e0 = &index[0];
        f.seek(SeekFrom::Start(bss + e0.stored_offset)).unwrap();
        let mut buf = vec![0u8; e0.stored_length as usize];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data1);

        let e1 = &index[1];
        f.seek(SeekFrom::Start(bss + e1.stored_offset)).unwrap();
        let mut buf = vec![0u8; e1.stored_length as usize];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data2);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn bad_magic_returns_error() {
        let path = temp_path(".seg");
        // File must be at least HEADER_LEN (96) bytes for the magic check to be reached.
        let mut buf = [0u8; HEADER_LEN as usize];
        buf[..8].copy_from_slice(b"BADMAGIC");
        fs::write(&path, buf).unwrap();
        let err = read_segment_index(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn promote_writes_segment_and_deletes_wal() {
        use crate::writelog;

        let base = temp_dir();
        fs::create_dir_all(base.join("wal")).unwrap();
        fs::create_dir_all(base.join("pending")).unwrap();

        let ulid = "01JQEXAMPLEULID0000000000A";
        let wal_path = base.join("wal").join(ulid);

        // Write a minimal WAL.
        let payload = b"segment extent payload";
        let hash = blake3::hash(payload);
        {
            let mut wl = writelog::WriteLog::create(&wal_path).unwrap();
            wl.append_data(0, 1, &hash, 0, payload).unwrap();
            wl.fsync().unwrap();
        }

        let mut entries = vec![SegmentEntry::new_data(hash, 0, 1, 0, payload.to_vec())];
        let bss = promote(&wal_path, ulid, &base.join("pending"), &mut entries, None).unwrap();

        // WAL must be gone.
        assert!(!wal_path.exists(), "WAL should be deleted after promotion");

        // Segment must exist (no .tmp).
        let seg_path = base.join("pending").join(ulid);
        assert!(seg_path.exists(), "segment missing from pending/");
        assert!(
            !base.join("pending").join(format!("{ulid}.tmp")).exists(),
            ".tmp must be gone"
        );

        // Segment must be readable and entries match.
        let (bss2, read_back) = read_segment_index(&seg_path).unwrap();
        assert_eq!(bss, bss2);
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].hash, hash);

        // stored_offset in the written entry should match read-back.
        assert_eq!(entries[0].stored_offset, read_back[0].stored_offset);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn collect_segment_files_excludes_tmp() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();

        // Create a valid segment file and a .tmp file.
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        fs::write(dir.join(ulid), b"").unwrap();
        fs::write(dir.join(format!("{ulid}.tmp")), b"").unwrap();
        fs::write(dir.join("notaulid"), b"").unwrap();

        let files = collect_segment_files(&dir).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_name().unwrap(), ulid);

        fs::remove_dir_all(dir).unwrap();
    }

    // --- sort_for_rebuild tests ---

    fn make_fork_dir() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::TempDir::new().unwrap();
        let fork_dir = tmp.path().to_path_buf();
        fs::create_dir_all(fork_dir.join("segments")).unwrap();
        fs::create_dir_all(fork_dir.join("gc")).unwrap();
        (tmp, fork_dir)
    }

    fn seg_path(fork_dir: &Path, ulid: &str) -> PathBuf {
        fork_dir.join("segments").join(ulid)
    }

    #[test]
    fn sort_for_rebuild_pending_is_gc_output() {
        let (_tmp, fork_dir) = make_fork_dir();
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let path = seg_path(&fork_dir, ulid);
        fs::write(&path, b"").unwrap();
        fs::write(fork_dir.join("gc").join(format!("{ulid}.pending")), b"").unwrap();

        let mut paths = vec![path.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        // A .pending sidecar marks the segment as a GC output (first / lower priority).
        assert_eq!(paths, vec![path]);
    }

    #[test]
    fn sort_for_rebuild_applied_is_gc_output() {
        let (_tmp, fork_dir) = make_fork_dir();
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let path = seg_path(&fork_dir, ulid);
        fs::write(&path, b"").unwrap();
        fs::write(fork_dir.join("gc").join(format!("{ulid}.applied")), b"").unwrap();

        let mut paths = vec![path.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        // A .applied sidecar marks the segment as a GC output (first / lower priority).
        assert_eq!(paths, vec![path]);
    }

    #[test]
    fn sort_for_rebuild_done_is_not_gc_output() {
        let (_tmp, fork_dir) = make_fork_dir();
        let gc_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let reg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAC0";
        let gc_path = seg_path(&fork_dir, gc_ulid);
        let reg_path = seg_path(&fork_dir, reg_ulid);
        fs::write(&gc_path, b"").unwrap();
        fs::write(&reg_path, b"").unwrap();
        // Only a .done sidecar — handoff is complete, old inputs already deleted.
        fs::write(fork_dir.join("gc").join(format!("{gc_ulid}.done")), b"").unwrap();

        let mut paths = vec![gc_path.clone(), reg_path.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        // Both segments treated as regular; sorted by ULID (gc_ulid < reg_ulid).
        assert_eq!(paths, vec![gc_path, reg_path]);
    }

    #[test]
    fn sort_for_rebuild_gc_outputs_precede_regular() {
        let (_tmp, fork_dir) = make_fork_dir();
        let gc_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let reg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAA9"; // lower ULID than gc_ulid
        let gc_path = seg_path(&fork_dir, gc_ulid);
        let reg_path = seg_path(&fork_dir, reg_ulid);
        fs::write(&gc_path, b"").unwrap();
        fs::write(&reg_path, b"").unwrap();
        fs::write(fork_dir.join("gc").join(format!("{gc_ulid}.pending")), b"").unwrap();

        let mut paths = vec![gc_path.clone(), reg_path.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        // GC output comes first regardless of ULID order so the regular
        // segment (higher priority) is processed last and wins on rebuild.
        assert_eq!(paths, vec![gc_path, reg_path]);
    }
}
