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
//   32..96 signature     Ed25519 sig over BLAKE3(header[0..32] || index_bytes)
//
// Index entry format:
//   hash       (32 bytes) BLAKE3 extent hash
//   start_lba  (8 bytes)  u64 le
//   lba_length (4 bytes)  u32 le
//   flags      (1 byte)   see FLAG_* constants
//
//   if FLAG_DEDUP_REF (thin): no further fields — data in canonical segment
//   if FLAG_DEDUP_REF | FLAG_MATERIALIZED (fat): same layout as DATA
//   if FLAG_ZERO: no further fields
//   else (DATA / INLINE):
//     stored_offset (8 bytes) u64 le — offset within body or inline section
//     stored_length (4 bytes) u32 le — byte length of stored data
//
// Body section: raw concatenated DATA + MaterializedRef extent bytes, no framing.
// Thin DedupRef and Zero entries contribute nothing to the body.
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

use bitflags::bitflags;
use ed25519_dalek::{Signature, VerifyingKey};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, little_endian as LE};

// --- constants ---

const MAGIC: &[u8; 8] = b"ELIDSEG\x02";
const HEADER_LEN: u64 = 96;

// --- on-disk header ---

/// The 96-byte segment file header.
///
/// Layout: magic(8) + entry_count(4) + index_length(4) + inline_length(4) +
///         body_length(8) + delta_length(4) + signature(64) = 96 bytes.
///
/// The Ed25519 signature covers `BLAKE3(header[0..32] || index_section_bytes)`.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct SegmentHeader {
    magic: [u8; 8],
    entry_count: LE::U32,
    index_length: LE::U32,
    inline_length: LE::U32,
    body_length: LE::U64,
    delta_length: LE::U32,
    signature: [u8; 64],
}

const _: () = assert!(
    std::mem::size_of::<SegmentHeader>() == HEADER_LEN as usize,
    "SegmentHeader must be exactly 96 bytes"
);

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
/// On success, files are written into two separate directories:
///   `<index_dir>/<segment_id>.idx`     — header + index section (bytes `[0, body_section_start)`)
///   `<body_dir>/<segment_id>.body`     — body bytes (body-relative offsets)
///   `<body_dir>/<segment_id>.present`  — packed bitset, one bit per index entry
///
/// `index_dir` is the volume's `index/` directory (coordinator-maintained, permanent).
/// `body_dir` is the volume's `cache/` directory (volume-managed read cache).
///
/// `elide-core` is synchronous; async fetchers must wrap their runtime (e.g.
/// `Runtime::block_on`) inside this interface.
pub trait SegmentFetcher: Send + Sync {
    /// Fetch the full segment body and write `.idx` to `index_dir` and
    /// `.body`/`.present` to `body_dir`.
    fn fetch(&self, segment_id: ulid::Ulid, index_dir: &Path, body_dir: &Path) -> io::Result<()>;

    /// Fetch a single extent and write body bytes into `body_dir/<segment_id>.body`
    /// at `extent.body_offset`, then set bit `extent.entry_idx` in `.present`.
    ///
    /// The default implementation fetches the entire segment body; override
    /// for efficient per-extent range-GETs.
    fn fetch_extent(
        &self,
        segment_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
        _extent: &ExtentFetch,
    ) -> io::Result<()> {
        self.fetch(segment_id, index_dir, body_dir)
    }
}

/// Parameters for fetching a single extent from an object store.
///
/// Passed to [`SegmentFetcher::fetch_extent`] to describe which extent to
/// fetch and where to write the result.
pub struct ExtentFetch {
    /// Absolute offset in the object where the body section starts
    /// (parsed from the `.idx` header; `body_section_start` in the segment format).
    pub body_section_start: u64,
    /// Body-relative offset of this extent's compressed bytes.
    pub body_offset: u64,
    /// Compressed (stored) length of this extent in bytes.
    pub body_length: u32,
    /// Index of this entry within the segment's index section.
    /// Used to address the corresponding `.present` bit.
    pub entry_idx: u32,
}

/// Convenience alias for an optional heap-allocated `SegmentFetcher`.
pub type BoxFetcher = Arc<dyn SegmentFetcher>;

/// Size of a ZERO or thin DEDUP_REF index entry: hash(32) + start_lba(8) + lba_length(4) + flags(1).
/// Neither has body bytes; no body_offset/body_length fields.
const IDX_ENTRY_ZERO_LEN: u32 = 45;
/// Size of a DATA or MATERIALIZED_REF index entry: above + stored_offset(8) + stored_length(4).
/// Both carry body bytes in the segment's body section.
const IDX_ENTRY_DATA_LEN: u32 = 57;

/// Extents at or below this byte size are stored inline in the inline section.
/// 0 = disabled until S3 integration.
const INLINE_THRESHOLD: usize = 0;

// --- flag bits ---

bitflags! {
    /// Flag byte in a segment index entry.
    ///
    /// **These bit values differ from `writelog::WalFlags`.**  Code that translates
    /// WAL records into segment entries must explicitly convert between the two
    /// namespaces (see `volume::recover_wal`).
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct SegmentFlags: u8 {
        /// Extent data is in the inline section; stored_offset is inline-section-relative.
        const INLINE     = 0x01;
        /// One or more delta options follow this entry (S3 only; not used locally).
        const HAS_DELTAS = 0x02;
        /// Stored data is lz4-compressed; stored_length is the compressed size.
        const COMPRESSED = 0x04;
        /// Thin dedup reference; extent data lives in the canonical segment (via extent index).
        /// No body bytes in this segment unless MATERIALIZED is also set.
        const DEDUP_REF    = 0x08;
        /// Zero extent; hash field is ZERO_HASH; no bytes in this segment's body; reads as zeros.
        const ZERO         = 0x10;
        /// Modifier for DEDUP_REF: body bytes have been materialised into this segment's body
        /// section (fat variant). Set by `materialise_segment` before S3 upload.
        /// Always combined with DEDUP_REF; never set alone.
        const MATERIALIZED = 0x20;
    }
}

// --- EntryKind / SegmentEntry ---

/// Discriminant for a segment index entry.
///
/// Variants are mutually exclusive and exhaustive — use `match entry.kind`
/// rather than chains of `is_*` booleans so the compiler enforces handling
/// of new variants at every call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// Standard data entry; body bytes live in the body section.
    Data,
    /// Thin dedup reference; no body bytes in this segment. Reads resolve
    /// the hash through the extent index to find the canonical segment's body.
    DedupRef,
    /// Fat dedup reference (FLAG_DEDUP_REF | FLAG_MATERIALIZED); body bytes
    /// are materialised in this segment's body section. Same layout as Data.
    /// Produced by `materialise_segment` before S3 upload.
    MaterializedRef,
    /// Zero extent; LBA range reads as zeros, no body bytes stored.
    Zero,
    /// Inline entry; body bytes live in the inline section.
    /// Currently unreachable (INLINE_THRESHOLD = 0) but present for format completeness.
    Inline,
}

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
    /// Entry kind: discriminates between Data, DedupRef, Zero, and Inline.
    pub kind: EntryKind,
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
    /// `flags` may include `SegmentFlags::COMPRESSED` if `data` is already compressed.
    /// `data` is moved in — no copy.
    pub fn new_data(
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        flags: SegmentFlags,
        data: Vec<u8>,
    ) -> Self {
        let stored_length = data.len() as u32;
        #[allow(clippy::absurd_extreme_comparisons)]
        let kind = if INLINE_THRESHOLD != 0 && data.len() <= INLINE_THRESHOLD {
            EntryKind::Inline
        } else {
            EntryKind::Data
        };
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: flags.contains(SegmentFlags::COMPRESSED),
            kind,
            stored_offset: 0, // filled by write_segment
            stored_length,
            data,
        }
    }

    /// Create a thin DEDUP_REF entry (no body bytes — reads via extent index).
    pub fn new_dedup_ref(hash: blake3::Hash, start_lba: u64, lba_length: u32) -> Self {
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: false,
            kind: EntryKind::DedupRef,
            stored_offset: 0,
            stored_length: 0,
            data: Vec::new(),
        }
    }

    /// Create a fat MATERIALIZED_REF entry (body bytes in segment, same layout as Data).
    ///
    /// `flags` may include `SegmentFlags::COMPRESSED` if `data` is already compressed.
    pub fn new_materialized_ref(
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        flags: SegmentFlags,
        data: Vec<u8>,
    ) -> Self {
        let stored_length = data.len() as u32;
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: flags.contains(SegmentFlags::COMPRESSED),
            kind: EntryKind::MaterializedRef,
            stored_offset: 0, // filled by write_segment
            stored_length,
            data,
        }
    }

    /// Create a ZERO entry (no data payload — LBA range reads as zeros).
    pub fn new_zero(start_lba: u64, lba_length: u32) -> Self {
        Self {
            hash: blake3::Hash::from_bytes([0u8; 32]),
            start_lba,
            lba_length,
            compressed: false,
            kind: EntryKind::Zero,
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
/// The segment is signed: the 64-byte Ed25519 signature over
/// `BLAKE3(header[0..32] || index_bytes)` is written at `header[32..96]`.
///
/// Returns `body_section_start` (the absolute byte offset of the body section
/// in the file). Callers use this to convert body entries' `stored_offset` to
/// absolute file offsets: `body_section_start + entry.stored_offset`.
pub fn write_segment(
    path: &Path,
    entries: &mut [SegmentEntry],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    let (index_length, inline_length, body_length) = assign_offsets(entries);
    let body_section_start = HEADER_LEN + index_length as u64 + inline_length as u64;

    // Build index section into a buffer first — needed for signing.
    let mut index_buf = Vec::with_capacity(index_length as usize);
    for entry in entries.iter() {
        write_index_entry(&mut index_buf, entry)?;
    }

    // Build the header (fields only; signature filled in below).
    let mut header = SegmentHeader::new_zeroed();
    header.magic.copy_from_slice(MAGIC);
    header.entry_count.set(entries.len() as u32);
    header.index_length.set(index_length);
    header.inline_length.set(inline_length);
    header.body_length.set(body_length);
    // delta_length = 0 (already zeroed); local files never have a delta body.

    // Compute signature: BLAKE3(header[0..32] || index_bytes), then sign.
    let sig_bytes: [u8; 64] = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header.as_bytes()[..32]);
        hasher.update(&index_buf);
        signer.sign(hasher.finalize().as_bytes())
    };
    header.signature.copy_from_slice(&sig_bytes);

    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut w = BufWriter::new(file);

    w.write_all(header.as_bytes())?;
    w.write_all(&index_buf)?;

    // Inline section (currently empty: INLINE_THRESHOLD = 0).
    for entry in entries.iter() {
        if entry.kind == EntryKind::Inline {
            w.write_all(&entry.data)?;
        }
    }
    // Body section: DATA and MaterializedRef extents, raw bytes, no framing.
    // ZERO and thin DedupRef contribute no bytes; inline entries are in the inline section.
    for entry in entries.iter() {
        if entry.kind == EntryKind::Data || entry.kind == EntryKind::MaterializedRef {
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
        index_length += match entry.kind {
            EntryKind::Zero | EntryKind::DedupRef => IDX_ENTRY_ZERO_LEN,
            EntryKind::Data | EntryKind::MaterializedRef | EntryKind::Inline => IDX_ENTRY_DATA_LEN,
        };

        match entry.kind {
            EntryKind::Inline => {
                entry.stored_offset = inline_cursor;
                inline_cursor += entry.stored_length as u64;
            }
            EntryKind::Data | EntryKind::MaterializedRef => {
                entry.stored_offset = body_cursor;
                body_cursor += entry.stored_length as u64;
            }
            EntryKind::Zero | EntryKind::DedupRef => {}
        }
    }

    (index_length, inline_cursor as u32, body_cursor)
}

fn write_index_entry<W: Write>(w: &mut W, e: &SegmentEntry) -> io::Result<()> {
    let mut flags = SegmentFlags::empty();
    match e.kind {
        EntryKind::Inline => flags |= SegmentFlags::INLINE,
        EntryKind::DedupRef => flags |= SegmentFlags::DEDUP_REF,
        EntryKind::MaterializedRef => flags |= SegmentFlags::DEDUP_REF | SegmentFlags::MATERIALIZED,
        EntryKind::Zero => flags |= SegmentFlags::ZERO,
        EntryKind::Data => {}
    }
    if e.compressed {
        flags |= SegmentFlags::COMPRESSED;
    }

    w.write_all(e.hash.as_bytes())?;
    w.write_all(&e.start_lba.to_le_bytes())?;
    w.write_all(&e.lba_length.to_le_bytes())?;
    w.write_all(&[flags.bits()])?;

    match e.kind {
        EntryKind::Data | EntryKind::MaterializedRef | EntryKind::Inline => {
            w.write_all(&e.stored_offset.to_le_bytes())?;
            w.write_all(&e.stored_length.to_le_bytes())?;
        }
        EntryKind::Zero | EntryKind::DedupRef => {
            // No body fields — 45-byte entry.
        }
    }
    Ok(())
}

// --- read ---

/// Verify the Ed25519 signature of a segment from raw bytes.
///
/// Checks `header[32..96]` against `BLAKE3(header[0..32] || index_bytes)`.
/// Returns `InvalidData` if the signature is missing (all zeros) or invalid.
///
/// Used by the fetch path to verify segments against the volume's public key
/// before writing them to the local cache.
pub fn verify_segment_bytes(
    bytes: &[u8],
    segment_id: &str,
    verifying_key: &VerifyingKey,
) -> io::Result<()> {
    if bytes.len() < HEADER_LEN as usize {
        return Err(io::Error::other(format!(
            "segment {segment_id}: too short to verify ({} bytes)",
            bytes.len()
        )));
    }
    let raw = &bytes[..HEADER_LEN as usize];
    let h = SegmentHeader::read_from_bytes(raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;

    if h.magic != *MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("segment {segment_id}: bad magic"),
        ));
    }

    let index_length = h.index_length.get() as usize;
    let index_end = HEADER_LEN as usize + index_length;
    if bytes.len() < index_end {
        return Err(io::Error::other(format!(
            "segment {segment_id}: truncated before end of index section"
        )));
    }
    let index_buf = &bytes[HEADER_LEN as usize..index_end];

    if h.signature == [0u8; 64] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("segment {segment_id} is unsigned"),
        ));
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(&raw[..32]);
    hasher.update(index_buf);
    let hash = hasher.finalize();

    let signature = Signature::from_bytes(&h.signature);
    verifying_key
        .verify_strict(hash.as_bytes(), &signature)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment {segment_id} has invalid signature"),
            )
        })
}

/// Parse a segment index without verifying the signature.
///
/// Returns `(body_section_start, entries)`.
///
/// Used when reading a cached `.idx` file in the extent-fetch path, where the
/// index was already verified by `verify_segment_bytes` when it was first cached.
/// For full-segment reads, use `read_and_verify_segment_index` instead.
pub fn read_segment_index(path: &Path) -> io::Result<(u64, Vec<SegmentEntry>)> {
    let (body_section_start, index_buf, entry_count, _h) = read_segment_header(path)?;
    let entries = parse_index_section(&index_buf, entry_count)?;
    Ok((body_section_start, entries))
}

/// Read and verify a segment index.
///
/// Returns `(body_section_start, entries)`.
/// `body_section_start` is the absolute byte offset of the body section in the
/// file. Callers use it to convert body entries' `stored_offset` to absolute
/// file offsets: `body_section_start + entry.stored_offset`.
///
/// Verifies the Ed25519 signature at `header[32..96]` against
/// `BLAKE3(header[0..32] || index_bytes)` using `verifying_key`.
/// Returns `InvalidData` if the signature is missing (all zeros) or invalid.
pub fn read_and_verify_segment_index(
    path: &Path,
    verifying_key: &VerifyingKey,
) -> io::Result<(u64, Vec<SegmentEntry>)> {
    let (body_section_start, index_buf, entry_count, h) = read_segment_header(path)?;

    let sig_bytes: [u8; 64] = h[32..96].try_into().expect("slice is exactly 64 bytes");

    // Reject unsigned segments (all-zero signature field).
    if sig_bytes == [0u8; 64] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("segment {} is unsigned", path.display()),
        ));
    }

    // Verify: Ed25519(sig, BLAKE3(header[0..32] || index_bytes)).
    let mut hasher = blake3::Hasher::new();
    hasher.update(&h[..32]);
    hasher.update(&index_buf);
    let hash = hasher.finalize();

    let signature = Signature::from_bytes(&sig_bytes);
    verifying_key
        .verify_strict(hash.as_bytes(), &signature)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment {} has invalid signature", path.display()),
            )
        })?;

    let entries = parse_index_section(&index_buf, entry_count)?;
    Ok((body_section_start, entries))
}

/// Read the segment header and index bytes from disk.
///
/// Returns `(body_section_start, index_buf, entry_count, header)`.
/// The header is the raw 96-byte header array (32 fields + 64 signature bytes).
fn read_segment_header(path: &Path) -> io::Result<(u64, Vec<u8>, u32, [u8; HEADER_LEN as usize])> {
    let mut f = fs::File::open(path)?;

    let mut raw = [0u8; HEADER_LEN as usize];
    f.read_exact(&mut raw)?;

    let h = SegmentHeader::read_from_bytes(&raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;

    if h.magic != *MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad segment magic",
        ));
    }

    let entry_count = h.entry_count.get();
    let index_length = h.index_length.get();
    let inline_length = h.inline_length.get();
    let body_section_start = HEADER_LEN + index_length as u64 + inline_length as u64;

    let mut index_buf = vec![0u8; index_length as usize];
    f.read_exact(&mut index_buf)?;

    Ok((body_section_start, index_buf, entry_count, raw))
}

fn parse_index_section(data: &[u8], entry_count: u32) -> io::Result<Vec<SegmentEntry>> {
    let mut pos = 0usize;
    let mut entries = Vec::with_capacity(entry_count as usize);

    for _ in 0..entry_count {
        let hash = blake3::Hash::from_bytes(read_fixed(data, &mut pos)?);
        let start_lba = u64::from_le_bytes(read_fixed(data, &mut pos)?);
        let lba_length = u32::from_le_bytes(read_fixed(data, &mut pos)?);
        let flags = SegmentFlags::from_bits_retain(read_u8(data, &mut pos)?);

        let compressed = flags.contains(SegmentFlags::COMPRESSED);
        let kind = if flags.contains(SegmentFlags::ZERO) {
            EntryKind::Zero
        } else if flags.contains(SegmentFlags::DEDUP_REF) {
            if flags.contains(SegmentFlags::MATERIALIZED) {
                EntryKind::MaterializedRef
            } else {
                EntryKind::DedupRef
            }
        } else if flags.contains(SegmentFlags::INLINE) {
            EntryKind::Inline
        } else {
            EntryKind::Data
        };

        // ZERO and thin DedupRef have no body fields (45-byte entry).
        // DATA, MaterializedRef, and Inline carry stored_offset + stored_length.
        let (stored_offset, stored_length) = match kind {
            EntryKind::Zero | EntryKind::DedupRef => (0u64, 0u32),
            EntryKind::Data | EntryKind::MaterializedRef | EntryKind::Inline => {
                let off = u64::from_le_bytes(read_fixed(data, &mut pos)?);
                let len = u32::from_le_bytes(read_fixed(data, &mut pos)?);
                (off, len)
            }
        };

        entries.push(SegmentEntry {
            hash,
            start_lba,
            lba_length,
            compressed,
            kind,
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
/// returned by `read_segment_index`. Only DATA and MaterializedRef entries are
/// read — they have body bytes in the body section. ZERO, thin DedupRef, and
/// Inline entries are skipped.
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
        match entry.kind {
            EntryKind::Data | EntryKind::MaterializedRef => {}
            EntryKind::Zero | EntryKind::DedupRef | EntryKind::Inline => continue,
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
    ulid: ulid::Ulid,
    pending_dir: &Path,
    entries: &mut [SegmentEntry],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    let ulid_str = ulid.to_string();
    let tmp_path = pending_dir.join(format!("{ulid_str}.tmp"));
    let final_path = pending_dir.join(&ulid_str);

    let body_section_start = write_segment(&tmp_path, entries, signer)?;

    // Atomic rename — COMMIT POINT.
    fs::rename(&tmp_path, &final_path)?;
    // Fsync the directory so the rename is durable before we delete the WAL.
    fsync_dir(&final_path)?;

    // WAL is now redundant; segment is the sole copy.
    fs::remove_file(wal_path)?;

    Ok(body_section_start)
}

// --- filesystem helpers ---

/// Fsync the directory containing `path`, making any preceding rename durable.
///
/// A `rename()` atomically updates the directory entry but does not guarantee
/// the entry is flushed to disk until the parent directory is fsynced.  Call
/// this immediately after every `.tmp` → final rename to close that gap.
pub(crate) fn fsync_dir(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("path has no parent directory"))?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

/// Write `content` to `path` atomically via a `.tmp` sibling file.
///
/// Sequence: write to `<path>.tmp` → `sync_data()` → rename to `path` →
/// `fsync` parent directory.  This ensures that either the complete file is
/// visible after a crash or no file is visible — never a partial write.
/// Used for small metadata files (`origin`, `size`, key material) that are
/// not large enough to warrant the full segment write path but still need
/// atomic, durable creation.
pub(crate) fn write_file_atomic(path: &Path, content: &[u8]) -> io::Result<()> {
    let tmp = {
        let mut name = path
            .file_name()
            .ok_or_else(|| io::Error::other("path has no filename"))?
            .to_owned();
        name.push(".tmp");
        path.with_file_name(name)
    };
    {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(content)?;
        f.sync_data()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(path)?;
    Ok(())
}

/// Collect all committed segment files in `dir` (files with valid ULID names,
/// no extension). Excludes `.tmp` files (incomplete promotions).
///
/// Used by `lbamap` and `extentindex` during startup rebuild.
/// Copy the header+index section (bytes `[0, body_section_start)`) from a full
/// segment file into an `.idx` file in the cache three-file format.
///
/// Does nothing if `idx_path` already exists — the operation is idempotent and
/// safe to call on a segment that was previously evicted or partially processed.
///
/// Written atomically via `.tmp` + rename.  The resulting `.idx` is identical
/// to what a `SegmentFetcher` would write, so `rebuild_segments` and
/// `extentindex::rebuild` treat it exactly like a demand-cached index.
pub fn extract_idx(segment_path: &Path, idx_path: &Path) -> io::Result<()> {
    if idx_path.exists() {
        return Ok(());
    }
    let (body_section_start, _, _, header) = read_segment_header(segment_path)?;
    let index_inline_len = body_section_start as usize - HEADER_LEN as usize;
    let mut f = fs::File::open(segment_path)?;
    use std::io::{Read, Seek, SeekFrom};
    f.seek(SeekFrom::Start(HEADER_LEN))?;
    let mut index_inline = vec![0u8; index_inline_len];
    f.read_exact(&mut index_inline)?;
    let mut buf = Vec::with_capacity(body_section_start as usize);
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&index_inline);
    write_file_atomic(idx_path, &buf)
}

/// Copy a segment body into the local cache.
///
/// Reads `src_path` (a full segment in `pending/` or `gc/`), extracts the body
/// section, writes it to `body_path` (`cache/<ulid>.body`), and writes an
/// all-present bitset to `present_path` (`cache/<ulid>.present`).
///
/// Both files are written via tmp+rename for crash safety. Idempotent: if
/// `body_path` already exists, the function returns `Ok(())` immediately without
/// re-reading the source file.
pub fn promote_to_cache(src_path: &Path, body_path: &Path, present_path: &Path) -> io::Result<()> {
    if body_path.try_exists()? {
        return Ok(());
    }
    let data = fs::read(src_path)?;
    if data.len() < HEADER_LEN as usize {
        return Err(io::Error::other("segment too short to parse header"));
    }
    let entry_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    let index_length = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
    let inline_length = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
    let body_section_start = HEADER_LEN as usize + index_length as usize + inline_length as usize;
    if data.len() < body_section_start {
        return Err(io::Error::other("segment truncated before body section"));
    }
    write_file_atomic(body_path, &data[body_section_start..])?;
    let bitset_len = (entry_count as usize).div_ceil(8);
    write_file_atomic(present_path, &vec![0xffu8; bitset_len])
}

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

/// Collect volume-signed GC output bodies from `gc/` that are in `.applied`
/// state (awaiting coordinator upload to S3).
///
/// These are segments that the volume has re-signed in-place within `gc/` and
/// acknowledged (`.applied` marker written), but that the coordinator has not
/// yet uploaded to S3 and written `index/<new>.idx` for.  They must be included
/// in LBA map and extent index rebuild at lower priority than `index/*.idx`
/// entries so the original input segments (still in `index/`) win for any LBA
/// they cover.
///
/// Bodies with `.pending` markers (coordinator-signed, not yet volume-applied)
/// are excluded — the old input segments referenced by `index/` are still authoritative.
pub fn collect_gc_applied_segment_files(fork_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let gc_dir = fork_dir.join("gc");
    match fs::read_dir(&gc_dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
        Ok(entries) => {
            let mut paths = Vec::new();
            for entry in entries {
                let path = entry?.path();
                // Skip sidecar files (.pending, .applied, .done, .tmp).
                if path.extension().is_some() {
                    continue;
                }
                let name = match path.file_name().and_then(|s| s.to_str()) {
                    Some(n) => n,
                    None => continue,
                };
                if ulid::Ulid::from_string(name).is_err() {
                    continue;
                }
                // Only include bodies that the volume has re-signed (.applied present).
                if gc_dir.join(format!("{name}.applied")).exists() {
                    paths.push(path);
                }
            }
            Ok(paths)
        }
    }
}

/// Collect all `.idx` files in `index_dir` whose stem is a valid ULID.
///
/// Used by `lbamap` and `extentindex` during startup rebuild. The `index/`
/// directory holds the coordinator-written header+index portion for all
/// segments confirmed to be in S3.
pub fn collect_idx_files(index_dir: &Path) -> io::Result<Vec<PathBuf>> {
    match fs::read_dir(index_dir) {
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

// --- .present bitset helpers ---

/// Read `entry_count` from the 96-byte header of a segment or `.idx` file.
///
/// `entry_count` is stored at bytes 8–12 (little-endian u32). Used when
/// creating or resizing the `.present` bitset for per-extent fetching.
pub fn read_entry_count(path: &Path) -> io::Result<u32> {
    let mut f = fs::File::open(path)?;
    let mut h = [0u8; 12];
    f.read_exact(&mut h)?;
    if &h[0..8] != MAGIC {
        return Err(io::Error::other("bad segment magic"));
    }
    Ok(u32::from_le_bytes([h[8], h[9], h[10], h[11]]))
}

/// Return `true` if bit `entry_idx` is set in the `.present` file at `path`.
///
/// Returns `false` if the file does not exist or is too short (treat as not
/// present so the caller demand-fetches the extent).
pub fn check_present_bit(path: &Path, entry_idx: u32) -> io::Result<bool> {
    match fs::read(path) {
        Ok(bytes) => {
            let byte_idx = (entry_idx / 8) as usize;
            let bit = entry_idx % 8;
            Ok(bytes.get(byte_idx).is_some_and(|b| b & (1 << bit) != 0))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Set bit `entry_idx` in the `.present` file at `path`.
///
/// Creates the file (all-zero, sized for `entry_count` entries) if absent.
/// The file is written in place — no tmp+rename — since the `.present` file
/// is a cache: losing a bit on crash just causes a re-fetch on next access.
pub fn set_present_bit(path: &Path, entry_idx: u32, entry_count: u32) -> io::Result<()> {
    let bitset_len = (entry_count as usize).div_ceil(8);
    let mut bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => vec![0u8; bitset_len],
        Err(e) => return Err(e),
    };
    if bytes.len() < bitset_len {
        bytes.resize(bitset_len, 0);
    }
    bytes[entry_idx as usize / 8] |= 1 << (entry_idx % 8);
    fs::write(path, &bytes)
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
    // Partition by source directory: paths from gc/ are in-flight GC outputs
    // (lower priority); paths from pending/ are regular (higher priority).
    // The source directory is the authoritative signal.
    let mut gc_paths: Vec<PathBuf> = Vec::new();
    let mut non_gc_paths: Vec<PathBuf> = Vec::new();
    for p in paths.drain(..) {
        if p.parent() == Some(gc_dir.as_path()) {
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

    /// Generate a fresh Ed25519 keypair for use in tests.
    fn test_signer() -> (Arc<dyn SegmentSigner>, VerifyingKey) {
        crate::signing::generate_ephemeral_signer()
    }

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
        let (signer, vk) = test_signer();
        let data = vec![0x42u8; 4096];
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_data(
            hash,
            10,
            1,
            SegmentFlags::empty(),
            data.clone(),
        )];
        let bss = write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (bss2, read_entries) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(bss, bss2);
        assert_eq!(read_entries.len(), 1);

        let e = &read_entries[0];
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 10);
        assert_eq!(e.lba_length, 1);
        assert!(!e.compressed);
        assert_eq!(e.kind, EntryKind::Data);
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 4096);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_multiple_entries() {
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();

        let mut entries: Vec<SegmentEntry> = (0..4u64)
            .map(|i| {
                let data = vec![i as u8; 4096];
                let hash = blake3::hash(&data);
                SegmentEntry::new_data(hash, i * 8, 2, SegmentFlags::empty(), data)
            })
            .collect();

        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back) = read_and_verify_segment_index(&path, &vk).unwrap();
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
        let (signer, vk) = test_signer();
        let hash = blake3::hash(b"existing extent");

        let mut entries = vec![SegmentEntry::new_dedup_ref(hash, 5, 3)];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 1);

        let e = &read_back[0];
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 5);
        assert_eq!(e.lba_length, 3);
        assert_eq!(e.kind, EntryKind::DedupRef);
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 0);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_mixed_data_and_ref() {
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();

        let data = vec![0xABu8; 8192];
        let data_hash = blake3::hash(&data);
        let ref_hash = blake3::hash(b"some ancestor extent");

        let mut entries = vec![
            SegmentEntry::new_data(data_hash, 0, 2, SegmentFlags::empty(), data),
            SegmentEntry::new_dedup_ref(ref_hash, 2, 1),
            SegmentEntry::new_data(
                blake3::hash(b"x"),
                10,
                1,
                SegmentFlags::empty(),
                b"x".repeat(4096).to_vec(),
            ),
        ];

        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 3);

        assert_eq!(read_back[0].kind, EntryKind::Data);
        assert_eq!(read_back[0].stored_offset, 0); // first body entry
        assert_eq!(read_back[0].stored_length, 8192);

        assert_eq!(read_back[1].kind, EntryKind::DedupRef); // ref contributes no body bytes

        assert_eq!(read_back[2].kind, EntryKind::Data);
        assert_eq!(read_back[2].stored_offset, 8192); // body cursor advanced past first entry
        assert_eq!(read_back[2].stored_length, 4096);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_compressed_entry() {
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data = vec![0xCDu8; 2048]; // "compressed" payload
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_data(
            hash,
            20,
            1,
            SegmentFlags::COMPRESSED,
            data,
        )];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back) = read_and_verify_segment_index(&path, &vk).unwrap();
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
        let (signer, vk) = test_signer();
        let data1 = vec![0x11u8; 4096];
        let data2 = vec![0x22u8; 4096];
        let h1 = blake3::hash(&data1);
        let h2 = blake3::hash(&data2);

        let mut entries = vec![
            SegmentEntry::new_data(h1, 0, 1, SegmentFlags::empty(), data1.clone()),
            SegmentEntry::new_data(h2, 1, 1, SegmentFlags::empty(), data2.clone()),
        ];

        let bss = write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (bss2, index) = read_and_verify_segment_index(&path, &vk).unwrap();
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
        let (_, vk) = test_signer();
        // File must be at least HEADER_LEN (96) bytes for the magic check to be reached.
        let mut buf = [0u8; HEADER_LEN as usize];
        buf[..8].copy_from_slice(b"BADMAGIC");
        fs::write(&path, buf).unwrap();
        let err = read_and_verify_segment_index(&path, &vk).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn promote_writes_segment_and_deletes_wal() {
        use crate::writelog;

        let base = temp_dir();
        fs::create_dir_all(base.join("wal")).unwrap();
        fs::create_dir_all(base.join("pending")).unwrap();

        let ulid = ulid::Ulid::from_string("01JQTEST000000000000000001").unwrap();
        let wal_path = base.join("wal").join(ulid.to_string());

        // Write a minimal WAL.
        let payload = b"segment extent payload";
        let hash = blake3::hash(payload);
        {
            let mut wl = writelog::WriteLog::create(&wal_path).unwrap();
            wl.append_data(0, 1, &hash, writelog::WalFlags::empty(), payload)
                .unwrap();
            wl.fsync().unwrap();
        }

        let (signer, vk) = test_signer();
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            payload.to_vec(),
        )];
        let bss = promote(
            &wal_path,
            ulid,
            &base.join("pending"),
            &mut entries,
            signer.as_ref(),
        )
        .unwrap();

        // WAL must be gone.
        assert!(!wal_path.exists(), "WAL should be deleted after promotion");

        // Segment must exist (no .tmp).
        let ulid_str = ulid.to_string();
        let seg_path = base.join("pending").join(&ulid_str);
        assert!(seg_path.exists(), "segment missing from pending/");
        assert!(
            !base
                .join("pending")
                .join(format!("{ulid_str}.tmp"))
                .exists(),
            ".tmp must be gone"
        );

        // Segment must be readable and entries match.
        let (bss2, read_back) = read_and_verify_segment_index(&seg_path, &vk).unwrap();
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
        fs::create_dir_all(fork_dir.join("pending")).unwrap();
        fs::create_dir_all(fork_dir.join("gc")).unwrap();
        (tmp, fork_dir)
    }

    fn seg_path(fork_dir: &Path, ulid: &str) -> PathBuf {
        fork_dir.join("pending").join(ulid)
    }

    fn gc_path(fork_dir: &Path, ulid: &str) -> PathBuf {
        fork_dir.join("gc").join(ulid)
    }

    #[test]
    fn sort_for_rebuild_gc_dir_path_is_gc_output() {
        // A path in gc/ is treated as a GC output (lower priority) regardless
        // of which sidecar files exist.  The source directory is the signal.
        let (_tmp, fork_dir) = make_fork_dir();
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let path = gc_path(&fork_dir, ulid);
        fs::write(&path, b"").unwrap();
        fs::write(fork_dir.join("gc").join(format!("{ulid}.applied")), b"").unwrap();

        let mut paths = vec![path.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        assert_eq!(paths, vec![path]);
    }

    #[test]
    fn sort_for_rebuild_pending_dir_path_is_not_gc_output() {
        // A path in pending/ is always regular (higher priority), even if a
        // .done sidecar exists in gc/ — .done means the handoff is complete and
        // the old inputs are already gone.
        let (_tmp, fork_dir) = make_fork_dir();
        let ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let path = seg_path(&fork_dir, ulid);
        fs::write(&path, b"").unwrap();
        fs::write(fork_dir.join("gc").join(format!("{ulid}.done")), b"").unwrap();

        let mut paths = vec![path.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        assert_eq!(paths, vec![path]);
    }

    #[test]
    fn sort_for_rebuild_gc_outputs_precede_regular() {
        let (_tmp, fork_dir) = make_fork_dir();
        let gc_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAB";
        let reg_ulid = "01AAAAAAAAAAAAAAAAAAAAAAA9"; // lower ULID than gc_ulid
        let path_gc = gc_path(&fork_dir, gc_ulid);
        let path_reg = seg_path(&fork_dir, reg_ulid);
        fs::write(&path_gc, b"").unwrap();
        fs::write(&path_reg, b"").unwrap();
        fs::write(fork_dir.join("gc").join(format!("{gc_ulid}.applied")), b"").unwrap();

        let mut paths = vec![path_gc.clone(), path_reg.clone()];
        sort_for_rebuild(&fork_dir, &mut paths);

        // GC output (from gc/) comes first regardless of ULID order so the
        // regular segment (higher priority) is processed last and wins on rebuild.
        assert_eq!(paths, vec![path_gc, path_reg]);
    }

    // --- .present bitset helpers ---

    #[test]
    fn present_bit_missing_file_returns_false() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.present");
        assert!(!check_present_bit(&path, 0).unwrap());
        assert!(!check_present_bit(&path, 7).unwrap());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn present_bit_set_and_query() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.present");

        set_present_bit(&path, 0, 8).unwrap();
        assert!(check_present_bit(&path, 0).unwrap());
        assert!(!check_present_bit(&path, 1).unwrap());
        assert!(!check_present_bit(&path, 7).unwrap());

        set_present_bit(&path, 7, 8).unwrap();
        assert!(check_present_bit(&path, 0).unwrap());
        assert!(check_present_bit(&path, 7).unwrap());
        assert!(!check_present_bit(&path, 1).unwrap());

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn present_bit_crosses_byte_boundary() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.present");

        // 16 entries → 2 bytes. Set bit 8 (first bit of second byte).
        set_present_bit(&path, 8, 16).unwrap();
        assert!(!check_present_bit(&path, 7).unwrap());
        assert!(check_present_bit(&path, 8).unwrap());
        assert!(!check_present_bit(&path, 9).unwrap());
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[0], 0x00);
        assert_eq!(bytes[1], 0x01);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn set_present_bit_resizes_short_file() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.present");

        // Write a 1-byte file, then set a bit in the (non-existent) second byte.
        fs::write(&path, &[0u8]).unwrap();
        set_present_bit(&path, 8, 16).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[1] & 0x01, 0x01);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn read_entry_count_from_segment_file() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();

        let data = vec![0xAAu8; 4096];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let (signer, vk) = test_signer();
        let path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        assert_eq!(read_entry_count(&path).unwrap(), 1);

        // Also works on a .idx file (same header format).
        let full_bytes = fs::read(&path).unwrap();
        let (bss, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        let idx_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA.idx");
        fs::write(&idx_path, &full_bytes[..bss as usize]).unwrap();
        assert_eq!(read_entry_count(&idx_path).unwrap(), 1);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn roundtrip_materialized_ref_entry() {
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data = vec![0xFFu8; 4096];
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_materialized_ref(
            hash,
            7,
            2,
            SegmentFlags::empty(),
            data,
        )];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 1);

        let e = &read_back[0];
        assert_eq!(e.kind, EntryKind::MaterializedRef);
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 7);
        assert_eq!(e.lba_length, 2);
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 4096);
        assert!(!e.compressed);

        // Verify the flag byte round-trips as DEDUP_REF | MATERIALIZED.
        // We already confirmed kind == MaterializedRef above, which is only
        // produced when both flags are present during parsing.
        // Additionally, read the raw flag byte from the file to be explicit.
        use std::io::{Read, Seek, SeekFrom};
        let mut f = fs::File::open(&path).unwrap();
        // The first index entry starts at HEADER_LEN. The flag byte is at
        // offset: hash(32) + start_lba(8) + lba_length(4) = 44 bytes in.
        f.seek(SeekFrom::Start(HEADER_LEN as u64 + 44)).unwrap();
        let mut flag_byte = [0u8; 1];
        f.read_exact(&mut flag_byte).unwrap();
        let flags = SegmentFlags::from_bits_truncate(flag_byte[0]);
        assert!(flags.contains(SegmentFlags::DEDUP_REF));
        assert!(flags.contains(SegmentFlags::MATERIALIZED));

        fs::remove_file(&path).unwrap();
    }
}
