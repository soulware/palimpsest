// Segment file: unified on-disk and S3 format.
//
// Layout:
//   [Header: 100 bytes]
//   [Index section: index_length bytes]   — starts at byte 100
//   [Inline section: inline_length bytes] — starts at byte 100 + index_length
//   [Full body: body_length bytes]        — starts at byte 100 + index_length + inline_length
//   [Delta body: delta_length bytes]      — starts at byte 100 + index_length + inline_length + body_length
//
// Header (100 bytes):
//   0..8    magic         "ELIDSEG\x05"
//   8..12   entry_count   u32 le
//   12..16  index_length  u32 le; total bytes of the index section (base + delta table + inputs)
//   16..20  inline_length u32 le; 0 if no inline data
//   20..28  body_length   u64 le
//   28..32  delta_length  u32 le; 0 if no delta body
//   32..36  inputs_length u32 le; bytes of input ULID table at tail of index section (multiple of 16)
//   36..100 signature     Ed25519 sig over BLAKE3(header[0..36] || index_bytes)
//
// Index section layout:
//   [entry_count × 64 bytes: base entries]
//   [delta table: variable-length, only present when entries have HAS_DELTAS]
//   [inputs table: inputs_length bytes, 16 bytes per input ULID] — populated only for GC outputs
//
// Base entry format (64 bytes, fixed-size):
//   hash          (32 bytes) BLAKE3 extent hash
//   start_lba     (8 bytes)  u64 le
//   lba_length    (4 bytes)  u32 le
//   flags         (1 byte)   see FLAG_* constants
//   stored_offset (8 bytes)  u64 le — offset within body or inline section
//   stored_length (4 bytes)  u32 le — byte length of stored data
//   reserved      (7 bytes)  must be zero
//
// Delta table (appended after base entries within index section):
//   per entry with deltas:
//     entry_index  (4 bytes) u32 le — index of the base entry
//     delta_count  (1 byte)  number of delta options
//     per delta option (45 bytes):
//       source_hash    (32 bytes) BLAKE3 hash of the dictionary extent
//       option_flags   (1 byte)   bit 0: FLAG_DELTA_INLINE (reserved)
//       delta_offset   (8 bytes)  u64 le — offset within delta body section
//       delta_length   (4 bytes)  u32 le — byte length in delta body
//
// Body section: raw concatenated extent bytes, no framing.
// Data entries have real body bytes. DedupRef and Zero entries contribute nothing
// to the body: they carry no body bytes and reserve no space. DedupRef reads
// resolve through the extent index to the canonical segment's body.
//
// Local segment files and S3 objects use the same format. File-aware imports
// produce segments with a populated delta body section (elide-import's Phase
// B2 stage), which is then preserved locally by `promote_to_cache` into
// `cache/<id>.body` as a contiguous region appended after the sparse body
// section — the `.body` file mirrors the S3 object's `[body_offset, EOF)`
// range byte-for-byte.
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

pub const MAGIC: &[u8; 8] = b"ELIDSEG\x05";
pub const HEADER_LEN: u64 = 100;
/// Number of header bytes covered by the signature, excluding the signature field itself.
const HEADER_SIGNED_PREFIX: usize = 36;
/// Size of a single input ULID in the inputs table.
const INPUT_ULID_LEN: usize = 16;

// --- on-disk header ---

/// The 100-byte segment file header.
///
/// Layout: magic(8) + entry_count(4) + index_length(4) + inline_length(4) +
///         body_length(8) + delta_length(4) + inputs_length(4) + signature(64) = 100 bytes.
///
/// The Ed25519 signature covers `BLAKE3(header[0..36] || index_section_bytes)`,
/// so the inputs table — which lives at the tail of the index section — is
/// authenticated along with the base entries and delta table.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C, packed)]
struct SegmentHeader {
    magic: [u8; 8],
    entry_count: LE::U32,
    index_length: LE::U32,
    inline_length: LE::U32,
    body_length: LE::U64,
    delta_length: LE::U32,
    inputs_length: LE::U32,
    signature: [u8; 64],
}

const _: () = assert!(
    std::mem::size_of::<SegmentHeader>() == HEADER_LEN as usize,
    "SegmentHeader must be exactly 100 bytes"
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
    /// Fetch a single extent and write body bytes into `body_dir/<segment_id>.body`
    /// at `extent.body_offset`, then set bit `extent.entry_idx` in `.present`.
    fn fetch_extent(
        &self,
        segment_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
        extent: &ExtentFetch,
    ) -> io::Result<()>;

    /// Fetch a segment's delta body section and write it atomically to
    /// `body_dir/<segment_id>.delta`.
    ///
    /// The delta body lives at `[body_section_start + body_length,
    /// body_section_start + body_length + delta_length)` in the remote
    /// segment object; this method issues a single range-GET for exactly
    /// that region, so every Delta entry in the segment is serviced by
    /// one request. The resulting file starts at delta-region byte 0
    /// (no leading body section), matching `promote_to_cache`'s output
    /// shape so `try_read_delta_extent` can open it uniformly.
    ///
    /// Written via tmp+rename for crash safety.
    fn fetch_delta_body(
        &self,
        segment_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
    ) -> io::Result<()>;
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

/// Size of every index entry: hash(32) + start_lba(8) + lba_length(4) +
/// flags(1) + stored_offset(8) + stored_length(4) + reserved(7) = 64 bytes.
///
/// Fixed-size, cache-line-aligned. Delta options are stored in a separate
/// delta table appended after the base entries within the index section.
const IDX_ENTRY_LEN: u32 = 64;

/// Size of one serialized delta option in the delta table:
/// source_hash(32) + option_flags(1) + delta_offset(8) + delta_length(4) = 45 bytes.
const DELTA_OPTION_LEN: u32 = 45;

/// Size of one delta table entry header: entry_index(4) + delta_count(1) = 5 bytes.
const DELTA_TABLE_ENTRY_HEADER: u32 = 5;

/// Extents at or below this byte size are stored inline in the inline section.
/// 0 = disabled until S3 integration.
/// Extents with stored data (after compression) strictly below this size go
/// into the segment's inline section rather than the body section.  Inline
/// data survives cache eviction (it lives in the `.idx` file and in-memory
/// `ExtentLocation::inline_data`), eliminating S3 demand-fetch for small writes.
///
/// Extents with stored (compressed) size below this go in the inline section
/// of the `.idx` file.  Only genuinely tiny data should inline — the `.idx` is
/// fetched by every host, so bloating it with compressed block data defeats
/// demand-fetch and delta compression.
///
/// 256 bytes captures mostly-zero blocks, tiny config files, and sparse
/// metadata while keeping real data in the body section.
const INLINE_THRESHOLD: usize = 256;

/// Compute the byte length of the base entries + delta table region.
///
/// Does not include the inputs table. Total index section length is this
/// value plus `inputs.len() * INPUT_ULID_LEN`.
fn entries_region_length(entries: &[SegmentEntry]) -> u32 {
    let base = entries.len() as u32 * IDX_ENTRY_LEN;
    let delta_table: u32 = entries
        .iter()
        .filter(|e| !e.delta_options.is_empty())
        .map(|e| DELTA_TABLE_ENTRY_HEADER + e.delta_options.len() as u32 * DELTA_OPTION_LEN)
        .sum();
    base + delta_table
}

/// Length in bytes of the input ULID table for a given input count.
fn inputs_region_length(inputs: &[ulid::Ulid]) -> u32 {
    (inputs.len() * INPUT_ULID_LEN) as u32
}

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
        /// Dedup reference; no body bytes, no body reservation. Reads resolve
        /// through the extent index to the canonical segment's body.
        const DEDUP_REF    = 0x08;
        /// Zero extent; hash field is ZERO_HASH; no bytes in this segment's body; reads as zeros.
        const ZERO         = 0x10;
        /// Thin delta entry; no body bytes. Reads decompress a delta blob from
        /// the delta-body section against a source extent (looked up via the
        /// extent index). Entries with this flag must carry `HAS_DELTAS` and
        /// have at least one delta option.
        const DELTA        = 0x20;
        /// Canonical-body-only: this entry carries a body for its hash (read
        /// via `extent_index.lookup`), but makes **no LBA claim** on rebuild.
        /// `start_lba` / `lba_length` are serialised as zero and the entry is
        /// skipped by `lbamap::rebuild_segments`. Emitted by GC when a
        /// DATA/INLINE entry's original LBA has been overwritten but the
        /// hash is still referenced elsewhere via a DedupRef — preserves
        /// the canonical body without shadowing the live LBA mapping.
        /// Co-exists with `INLINE` (body in inline section) or without it
        /// (body in body section). See `docs/formats.md`.
        const CANONICAL_ONLY = 0x40;
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
    /// Dedup reference; no body bytes in any segment. Reads resolve through
    /// the extent index to the canonical segment's body.
    DedupRef,
    /// Zero extent; LBA range reads as zeros, no body bytes stored.
    Zero,
    /// Inline entry; body bytes live in the inline section. Produced when a
    /// write's stored size falls below `INLINE_THRESHOLD` (small, highly
    /// compressible payloads such as all-zero blocks).
    Inline,
    /// Thin delta entry; no body bytes. Content is materialised by fetching a
    /// delta blob from the segment's delta body section and decompressing it
    /// against a source extent body located via `extent_index.lookup(source_hash)`.
    /// Must carry at least one `DeltaOption`. The source must resolve to a
    /// `Data` entry — there are no delta-of-delta chains.
    Delta,
    /// Canonical-body-only (body in body section): carries body bytes whose
    /// hash is canonical in this segment, but makes **no LBA claim** on rebuild.
    /// Emitted by GC when carrying forward a hash that's still referenced
    /// elsewhere (via DedupRef or Delta base) after its original LBA has
    /// been overwritten. `start_lba` / `lba_length` are always zero.
    CanonicalData,
    /// Canonical-body-only (body in inline section). Same semantics as
    /// `CanonicalData` but the body lives in the inline section — emitted
    /// when demoting an `Inline` entry.
    CanonicalInline,
}

impl EntryKind {
    /// Entry kinds with locally-available body bytes (pending/ segments).
    pub const LOCAL_BODY: [EntryKind; 2] = [EntryKind::Data, EntryKind::CanonicalData];
    /// Entry kinds with body bytes in materialised/S3 segments.
    pub const ALL_BODY: [EntryKind; 3] = [
        EntryKind::Data,
        EntryKind::DedupRef,
        EntryKind::CanonicalData,
    ];

    /// True for entries that carry no LBA claim on rebuild.
    pub fn is_canonical_only(self) -> bool {
        matches!(self, EntryKind::CanonicalData | EntryKind::CanonicalInline)
    }
}

/// A delta option attached to a segment index entry.
///
/// Each option records a zstd-dictionary-compressed representation of the
/// extent body using `source_hash` as the dictionary.  Readers that already
/// have the source extent cached locally can fetch the (much smaller) delta
/// blob instead of the full body.
#[derive(Debug, Clone)]
pub struct DeltaOption {
    /// BLAKE3 hash of the source extent used as the zstd dictionary.
    pub source_hash: blake3::Hash,
    /// Byte offset of the delta blob within the segment's delta body section.
    pub delta_offset: u64,
    /// Byte length of the delta blob in the delta body section.
    pub delta_length: u32,
}

/// One entry in the in-memory representation of a segment's index section.
///
/// Used in two contexts:
/// - Entries accumulated during a write session for promotion: `data` holds raw
///   extent bytes and `stored_offset` is filled in by `write_segment`.
/// - Entries read from an existing segment during startup rebuild: `data` is
///   empty (not needed; body lives on disk). `stored_offset` is section-relative.
///
/// `Clone` is supported for the segment-index cache, which hands out a fresh
/// `Vec<SegmentEntry>` on each hit. On read-from-disk entries, `data` is
/// `None` and the clone is a small memcpy plus one `Vec<DeltaOption>` copy.
#[derive(Debug, Clone)]
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
    /// Raw extent bytes for entries being built for promotion. `None` for dedup
    /// refs, zero entries, and entries read from a committed segment (data stays
    /// on disk). Populated by `read_extent_bodies` or during write-session
    /// accumulation.
    pub data: Option<Vec<u8>>,
    /// Delta options for this entry (S3 segments only; empty locally).
    /// Each option provides a zstd-dictionary-compressed alternative to the
    /// full body, using a different source extent as dictionary.
    pub delta_options: Vec<DeltaOption>,
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
        let kind = if data.len() < INLINE_THRESHOLD {
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
            data: Some(data),
            delta_options: Vec::new(),
        }
    }

    /// Create a DEDUP_REF entry.
    ///
    /// DedupRef entries carry no body bytes and reserve no body space.
    /// `stored_offset` and `stored_length` are both zero; the entry contributes
    /// nothing to the segment's `body_length`. Reads resolve through the extent
    /// index to the canonical DATA entry's body, which may live in any segment.
    pub fn new_dedup_ref(hash: blake3::Hash, start_lba: u64, lba_length: u32) -> Self {
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: false,
            kind: EntryKind::DedupRef,
            stored_offset: 0,
            stored_length: 0,
            data: None,
            delta_options: Vec::new(),
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
            data: None,
            delta_options: Vec::new(),
        }
    }

    /// Create a CANONICAL_DATA / CANONICAL_INLINE entry from an existing
    /// body-bearing entry whose LBA claim has been superseded but whose
    /// hash must survive for dedup resolution (DedupRef or Delta base).
    ///
    /// Chooses `CanonicalInline` if the source was `Inline`, `CanonicalData`
    /// otherwise. `start_lba` / `lba_length` are zeroed.
    pub fn into_canonical(mut self) -> Self {
        self.kind = match self.kind {
            EntryKind::Inline | EntryKind::CanonicalInline => EntryKind::CanonicalInline,
            _ => EntryKind::CanonicalData,
        };
        self.start_lba = 0;
        self.lba_length = 0;
        self
    }

    /// Create a thin DELTA entry.
    ///
    /// Carries the extent's content hash plus one or more `DeltaOption`s
    /// (zstd-dict-compressed alternatives). At read time the client picks
    /// the first option whose `source_hash` resolves via the local extent
    /// index, fetches the source body, fetches the delta blob from this
    /// segment's delta body section, and decompresses with the source as
    /// the zstd dictionary.
    ///
    /// Delta entries reserve no body space: `stored_offset` and
    /// `stored_length` are both zero and they contribute nothing to
    /// `body_length`. The delta options themselves (and the delta blob
    /// bytes in the delta body section) are the entry's entire payload.
    ///
    /// Panics in debug builds if `delta_options` is empty.
    pub fn new_delta(
        hash: blake3::Hash,
        start_lba: u64,
        lba_length: u32,
        delta_options: Vec<DeltaOption>,
    ) -> Self {
        debug_assert!(
            !delta_options.is_empty(),
            "new_delta requires at least one delta option"
        );
        Self {
            hash,
            start_lba,
            lba_length,
            compressed: false,
            kind: EntryKind::Delta,
            stored_offset: 0,
            stored_length: 0,
            data: None,
            delta_options,
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
    write_segment_full(path, entries, &[], &[], signer)
}

/// Write a segment file with an attached delta body section.
///
/// Same layout contract as [`write_segment`], plus `delta_body` is
/// appended after the body section and `header.delta_length` is set
/// to `delta_body.len()`. Used by thin Delta entry producers and by
/// tests that need to construct a complete Delta-bearing segment in
/// a single call.
///
/// The delta options table (written from each entry's `delta_options`
/// vec) holds `delta_offset` values that are byte offsets *within*
/// this `delta_body` slice. The caller is responsible for filling
/// those in coherently before writing.
pub fn write_segment_with_delta_body(
    path: &Path,
    entries: &mut [SegmentEntry],
    delta_body: &[u8],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    write_segment_full(path, entries, delta_body, &[], signer)
}

/// Write a segment file with an explicit GC inputs list.
///
/// Used by the coordinator when compacting several segments into one: `inputs`
/// carries the ULIDs of the source segments that fed this output. The volume's
/// apply-handoff path reads this field to derive the extent-index updates
/// directly from the segment, without a sidecar manifest.
///
/// For non-GC writes (WAL promote, fetch, import, etc.) use [`write_segment`]
/// or [`write_segment_with_delta_body`], which pass an empty `inputs` slice.
pub fn write_gc_segment(
    path: &Path,
    entries: &mut [SegmentEntry],
    inputs: &[ulid::Ulid],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    write_segment_full(path, entries, &[], inputs, signer)
}

/// Core write path. All public writers funnel through here.
pub fn write_segment_full(
    path: &Path,
    entries: &mut [SegmentEntry],
    delta_body: &[u8],
    inputs: &[ulid::Ulid],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    let (entries_region, inline_length, body_length) = assign_offsets(entries);
    let inputs_region = inputs_region_length(inputs);
    let index_length = entries_region + inputs_region;
    let body_section_start = HEADER_LEN + index_length as u64 + inline_length as u64;

    // Build index section into a buffer first — needed for signing.
    // Layout: [N × 64 base entries] [delta table (if any)] [inputs table].
    let mut index_buf = Vec::with_capacity(index_length as usize);
    for entry in entries.iter() {
        write_index_entry(&mut index_buf, entry)?;
    }
    write_delta_table(&mut index_buf, entries)?;
    for input in inputs {
        index_buf.extend_from_slice(&input.to_bytes());
    }
    debug_assert_eq!(index_buf.len(), index_length as usize);

    // Build the header (fields only; signature filled in below).
    let mut header = SegmentHeader::new_zeroed();
    header.magic.copy_from_slice(MAGIC);
    header.entry_count.set(entries.len() as u32);
    header.index_length.set(index_length);
    header.inline_length.set(inline_length);
    header.body_length.set(body_length);
    header.delta_length.set(delta_body.len() as u32);
    header.inputs_length.set(inputs_region);

    // Compute signature: BLAKE3(header[0..36] || index_bytes), then sign.
    let sig_bytes: [u8; 64] = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header.as_bytes()[..HEADER_SIGNED_PREFIX]);
        hasher.update(&index_buf);
        signer.sign(hasher.finalize().as_bytes())
    };
    header.signature.copy_from_slice(&sig_bytes);

    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut w = BufWriter::new(file);

    w.write_all(header.as_bytes())?;
    w.write_all(&index_buf)?;

    // Inline section.
    for entry in entries.iter() {
        if matches!(entry.kind, EntryKind::Inline | EntryKind::CanonicalInline)
            && let Some(data) = &entry.data
        {
            w.write_all(data)?;
        }
    }
    // Body section: raw bytes, no framing.
    // Data and CanonicalData entries write their body bytes.
    // DedupRef, Zero, Delta, Inline, and CanonicalInline contribute nothing.
    for entry in entries.iter() {
        match entry.kind {
            EntryKind::Data | EntryKind::CanonicalData => {
                if let Some(data) = &entry.data {
                    w.write_all(data)?;
                }
            }
            EntryKind::DedupRef
            | EntryKind::Zero
            | EntryKind::Inline
            | EntryKind::CanonicalInline
            | EntryKind::Delta => {}
        }
    }

    // Delta body section: raw concatenated delta blobs. Addressed by
    // delta_offset within this slice from each entry's delta options.
    if !delta_body.is_empty() {
        w.write_all(delta_body)?;
    }

    // `body_length = Σ Data stored_length`, matching what was just written.
    // set_len is defensive: nothing above should leave the file short, but
    // keeping it makes the file size invariant explicit regardless of how
    // entries are ordered.
    let expected_len = body_section_start + body_length + delta_body.len() as u64;
    w.get_ref().set_len(expected_len)?;

    w.flush()?;
    w.get_ref().sync_data()?;
    Ok(body_section_start)
}

/// Assign `stored_offset` for each entry and return section sizes.
///
/// Modifies entries in-place; `stored_offset` is meaningful only after this call.
fn assign_offsets(entries: &mut [SegmentEntry]) -> (u32, u32, u64) {
    let mut inline_cursor: u64 = 0;
    let mut body_cursor: u64 = 0;

    for entry in entries.iter_mut() {
        match entry.kind {
            EntryKind::Inline | EntryKind::CanonicalInline => {
                entry.stored_offset = inline_cursor;
                inline_cursor += entry.stored_length as u64;
            }
            EntryKind::Data | EntryKind::CanonicalData => {
                entry.stored_offset = body_cursor;
                body_cursor += entry.stored_length as u64;
            }
            EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => {
                // DedupRef, Zero, and Delta contribute nothing to the body
                // section. stored_offset/stored_length stay 0. Delta
                // entries' bytes live in the delta body section and are
                // addressed via `delta_options`, not `stored_offset`.
            }
        }
    }

    let entries_region = entries_region_length(entries);
    (entries_region, inline_cursor as u32, body_cursor)
}

fn write_index_entry<W: Write>(w: &mut W, e: &SegmentEntry) -> io::Result<()> {
    let mut flags = SegmentFlags::empty();
    if e.kind.is_canonical_only() {
        debug_assert!(
            e.start_lba == 0 && e.lba_length == 0,
            "canonical-only entries must have zero start_lba and lba_length"
        );
        flags |= SegmentFlags::CANONICAL_ONLY;
    }
    match e.kind {
        EntryKind::Inline | EntryKind::CanonicalInline => flags |= SegmentFlags::INLINE,
        EntryKind::DedupRef => flags |= SegmentFlags::DEDUP_REF,
        EntryKind::Zero => flags |= SegmentFlags::ZERO,
        EntryKind::Delta => flags |= SegmentFlags::DELTA,
        EntryKind::Data | EntryKind::CanonicalData => {}
    }
    if e.compressed {
        flags |= SegmentFlags::COMPRESSED;
    }
    if !e.delta_options.is_empty() {
        flags |= SegmentFlags::HAS_DELTAS;
    }
    debug_assert!(
        e.kind != EntryKind::Delta || flags.contains(SegmentFlags::HAS_DELTAS),
        "Delta entry must carry at least one delta option"
    );

    w.write_all(e.hash.as_bytes())?; // 32
    w.write_all(&e.start_lba.to_le_bytes())?; // 8
    w.write_all(&e.lba_length.to_le_bytes())?; // 4
    w.write_all(&[flags.bits()])?; // 1
    w.write_all(&e.stored_offset.to_le_bytes())?; // 8
    w.write_all(&e.stored_length.to_le_bytes())?; // 4
    w.write_all(&[0u8; 7])?; // 7 reserved
    Ok(())
}

/// Write the delta table: entries with delta options, appended after the
/// fixed-size base entries within the index section.
fn write_delta_table<W: Write>(w: &mut W, entries: &[SegmentEntry]) -> io::Result<()> {
    for (i, entry) in entries.iter().enumerate() {
        if entry.delta_options.is_empty() {
            continue;
        }
        w.write_all(&(i as u32).to_le_bytes())?; // entry_index: 4
        w.write_all(&[entry.delta_options.len() as u8])?; // delta_count: 1
        for opt in &entry.delta_options {
            w.write_all(opt.source_hash.as_bytes())?; // 32
            w.write_all(&[0u8])?; // option_flags: 1
            w.write_all(&opt.delta_offset.to_le_bytes())?; // 8
            w.write_all(&opt.delta_length.to_le_bytes())?; // 4
        }
    }
    Ok(())
}

/// Rewrite a segment with delta options and a delta body appended.
///
/// Reads the source segment (which must be a valid, signed segment — typically
/// the `.materialized` file), attaches `delta_options` to the specified entries,
/// and writes a new segment file at `dst_path` with:
///   - Updated index section (base entries + delta table)
///   - Identical inline and body sections (copied verbatim)
///   - Delta body appended after the body section
///   - Re-signed header (new index changes the signature)
///
/// `deltas` maps entry index → list of delta options for that entry.
/// `delta_body` is the concatenated delta blobs; offsets in each `DeltaOption`
/// are relative to the start of this buffer.
///
/// Returns `Ok(())` on success. The caller is responsible for cleanup of
/// `dst_path` on error.
pub fn rewrite_with_deltas(
    src_path: &Path,
    dst_path: &Path,
    deltas: &[(usize, Vec<DeltaOption>)],
    delta_body: &[u8],
    signer: &dyn SegmentSigner,
) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    // Read source header to get section sizes.
    let mut src = fs::File::open(src_path)?;
    let mut raw = [0u8; HEADER_LEN as usize];
    src.read_exact(&mut raw)?;
    let src_h = SegmentHeader::read_from_bytes(&raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;
    if src_h.magic != *MAGIC {
        return Err(io::Error::other("bad segment magic"));
    }

    let src_index_length = src_h.index_length.get();
    let src_inline_length = src_h.inline_length.get();
    let src_body_length = src_h.body_length.get();
    let src_inputs_length = src_h.inputs_length.get();
    let entry_count = src_h.entry_count.get();

    // Parse entries + inputs from source index. Inputs carry through unchanged
    // — a GC output that gets delta-rewritten keeps its original inputs list.
    let mut index_buf = vec![0u8; src_index_length as usize];
    src.read_exact(&mut index_buf)?;
    let (mut entries, inputs) = parse_index_section(&index_buf, entry_count, src_inputs_length)?;

    // Attach delta options to the specified entries.
    for (idx, opts) in deltas {
        if *idx >= entries.len() {
            return Err(io::Error::other(format!(
                "delta entry index {idx} out of range ({})",
                entries.len()
            )));
        }
        entries[*idx].delta_options = opts.clone();
    }

    // Build new index section (base entries + delta table + inputs).
    let new_entries_region = entries_region_length(&entries);
    let new_inputs_region = inputs_region_length(&inputs);
    let new_index_length = new_entries_region + new_inputs_region;
    let mut new_index_buf = Vec::with_capacity(new_index_length as usize);
    for entry in &entries {
        write_index_entry(&mut new_index_buf, entry)?;
    }
    write_delta_table(&mut new_index_buf, &entries)?;
    for input in &inputs {
        new_index_buf.extend_from_slice(&input.to_bytes());
    }

    // Build new header.
    let mut header = SegmentHeader::new_zeroed();
    header.magic.copy_from_slice(MAGIC);
    header.entry_count.set(entry_count);
    header.index_length.set(new_index_length);
    header.inline_length.set(src_inline_length);
    header.body_length.set(src_body_length);
    header.delta_length.set(delta_body.len() as u32);
    header.inputs_length.set(new_inputs_region);

    // Sign: BLAKE3(header[0..36] || new_index_bytes).
    let sig_bytes: [u8; 64] = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header.as_bytes()[..HEADER_SIGNED_PREFIX]);
        hasher.update(&new_index_buf);
        signer.sign(hasher.finalize().as_bytes())
    };
    header.signature.copy_from_slice(&sig_bytes);

    // Read inline + body from source (verbatim copy).
    let inline_body_len = src_inline_length as u64 + src_body_length;
    src.seek(SeekFrom::Start(HEADER_LEN + src_index_length as u64))?;
    let mut inline_body = vec![0u8; inline_body_len as usize];
    src.read_exact(&mut inline_body)?;

    // Write destination file.
    let dst = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst_path)?;
    let mut w = BufWriter::new(dst);
    w.write_all(header.as_bytes())?;
    w.write_all(&new_index_buf)?;
    w.write_all(&inline_body)?;
    w.write_all(delta_body)?;
    w.flush()?;
    w.get_ref().sync_data()?;

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
    hasher.update(&raw[..HEADER_SIGNED_PREFIX]);
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

/// Return `body_section_start` for a cached `.idx` file.
///
/// A `.idx` file contains exactly `[0, body_section_start)` of the full
/// segment (header + index section + inline section).  Its file size **is**
/// `body_section_start` — no header parse required.
///
/// This is the authoritative way to obtain `body_section_start` when you
/// already have (or only need) the `.idx` path.  Callers that also need the
/// parsed entries should still use [`read_segment_index`] or
/// [`read_and_verify_segment_index`], which return the same value via the
/// header.
pub fn idx_body_section_start(idx_path: &Path) -> io::Result<u64> {
    Ok(fs::metadata(idx_path)?.len())
}

/// Parse a segment index without verifying the signature.
///
/// Returns `(body_section_start, entries, inputs)`. `inputs` is empty for
/// non-GC segments and lists the source ULIDs for GC outputs.
///
/// Used when reading a cached `.idx` file in the extent-fetch path, where the
/// index was already verified by `verify_segment_bytes` when it was first cached.
/// For full-segment reads, use `read_and_verify_segment_index` instead.
pub fn read_segment_index(path: &Path) -> io::Result<(u64, Vec<SegmentEntry>, Vec<ulid::Ulid>)> {
    let (body_section_start, index_buf, entry_count, raw) = read_segment_header(path)?;
    let h = SegmentHeader::read_from_bytes(&raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;
    let inputs_length = h.inputs_length.get();
    let (entries, inputs) = parse_index_section(&index_buf, entry_count, inputs_length)?;
    Ok((body_section_start, entries, inputs))
}

/// Read and verify a segment index.
///
/// Returns `(body_section_start, entries, inputs)`. See [`read_segment_index`]
/// for the meaning of each field.
///
/// Verifies the Ed25519 signature at `header[36..100]` against
/// `BLAKE3(header[0..36] || index_bytes)` using `verifying_key`.
/// Returns `InvalidData` if the signature is missing (all zeros) or invalid.
pub fn read_and_verify_segment_index(
    path: &Path,
    verifying_key: &VerifyingKey,
) -> io::Result<(u64, Vec<SegmentEntry>, Vec<ulid::Ulid>)> {
    let (body_section_start, index_buf, entry_count, raw) = read_segment_header(path)?;
    let h = SegmentHeader::read_from_bytes(&raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;
    let inputs_length = h.inputs_length.get();

    let sig_bytes: [u8; 64] = raw[HEADER_SIGNED_PREFIX..HEADER_LEN as usize]
        .try_into()
        .expect("slice is exactly 64 bytes");

    // Reject unsigned segments (all-zero signature field).
    if sig_bytes == [0u8; 64] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("segment {} is unsigned", path.display()),
        ));
    }

    // Verify: Ed25519(sig, BLAKE3(header[0..36] || index_bytes)).
    let mut hasher = blake3::Hasher::new();
    hasher.update(&raw[..HEADER_SIGNED_PREFIX]);
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

    let (entries, inputs) = parse_index_section(&index_buf, entry_count, inputs_length)?;
    Ok((body_section_start, entries, inputs))
}

/// Read the segment header and index bytes from disk.
///
/// Returns `(body_section_start, index_buf, entry_count, header)`.
/// The header is the raw 96-byte header array (32 fields + 64 signature bytes).
/// Layout information pulled from a segment file's header, enough to
/// locate any section (body, delta body) by absolute file offset.
#[derive(Debug, Clone, Copy)]
pub struct SegmentLayoutInfo {
    /// Absolute file offset of the body section.
    pub body_section_start: u64,
    /// Byte length of the body section (sum of DATA `stored_length`).
    pub body_length: u64,
    /// Byte length of the delta body section (may be zero).
    pub delta_length: u32,
}

impl SegmentLayoutInfo {
    /// Absolute file offset of the delta body section.
    pub fn delta_body_offset(&self) -> u64 {
        self.body_section_start + self.body_length
    }
}

/// Read just enough of a segment file header to determine section
/// offsets. Does not verify the signature; callers that need
/// verification should use `read_and_verify_segment_index` instead.
pub fn read_segment_layout(path: &Path) -> io::Result<SegmentLayoutInfo> {
    let (body_section_start, _, _, raw) = read_segment_header(path)?;
    let h = SegmentHeader::read_from_bytes(&raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;
    Ok(SegmentLayoutInfo {
        body_section_start,
        body_length: h.body_length.get(),
        delta_length: h.delta_length.get(),
    })
}

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

/// Read the inline section bytes from a segment or `.idx` file.
///
/// Returns an empty `Vec` when `inline_length == 0` (no inline extents).
/// Works for both full segment files and `.idx` files (which include the
/// inline section: header + index + inline).
pub fn read_inline_section(path: &Path) -> io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = fs::File::open(path)?;
    let mut raw = [0u8; HEADER_LEN as usize];
    f.read_exact(&mut raw)?;
    let h = SegmentHeader::read_from_bytes(&raw)
        .map_err(|_| io::Error::other("segment header size mismatch"))?;
    let index_length = h.index_length.get();
    let inline_length = h.inline_length.get();
    if inline_length == 0 {
        return Ok(Vec::new());
    }
    let inline_start = HEADER_LEN + index_length as u64;
    f.seek(SeekFrom::Start(inline_start))?;
    let mut buf = vec![0u8; inline_length as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn parse_index_section(
    data: &[u8],
    entry_count: u32,
    inputs_length: u32,
) -> io::Result<(Vec<SegmentEntry>, Vec<ulid::Ulid>)> {
    if !(inputs_length as usize).is_multiple_of(INPUT_ULID_LEN) {
        return Err(io::Error::other(format!(
            "inputs_length {inputs_length} is not a multiple of {INPUT_ULID_LEN}"
        )));
    }
    if (inputs_length as usize) > data.len() {
        return Err(io::Error::other(format!(
            "inputs_length {inputs_length} exceeds index section length {}",
            data.len()
        )));
    }
    let entries_region_end = data.len() - inputs_length as usize;
    let entries_data = &data[..entries_region_end];

    let base_len = entry_count as usize * IDX_ENTRY_LEN as usize;
    if base_len > entries_data.len() {
        return Err(io::Error::other(format!(
            "entry_count {entry_count} exceeds index region size"
        )));
    }
    let mut entries = Vec::with_capacity(entry_count as usize);

    // Pass 1: parse fixed-size base entries (64 bytes each).
    let mut pos = 0usize;
    let mut has_deltas = false;
    for _ in 0..entry_count {
        let hash = blake3::Hash::from_bytes(read_fixed(entries_data, &mut pos)?);
        let start_lba = u64::from_le_bytes(read_fixed(entries_data, &mut pos)?);
        let lba_length = u32::from_le_bytes(read_fixed(entries_data, &mut pos)?);
        let flags = SegmentFlags::from_bits_retain(read_u8(entries_data, &mut pos)?);

        let compressed = flags.contains(SegmentFlags::COMPRESSED);
        if flags.contains(SegmentFlags::HAS_DELTAS) {
            has_deltas = true;
        }
        let canonical_only = flags.contains(SegmentFlags::CANONICAL_ONLY);
        let inline = flags.contains(SegmentFlags::INLINE);
        let kind = if flags.contains(SegmentFlags::ZERO) {
            EntryKind::Zero
        } else if flags.contains(SegmentFlags::DEDUP_REF) {
            EntryKind::DedupRef
        } else if flags.contains(SegmentFlags::DELTA) {
            EntryKind::Delta
        } else if canonical_only && inline {
            EntryKind::CanonicalInline
        } else if canonical_only {
            EntryKind::CanonicalData
        } else if inline {
            EntryKind::Inline
        } else {
            EntryKind::Data
        };

        let stored_offset = u64::from_le_bytes(read_fixed(entries_data, &mut pos)?);
        let stored_length = u32::from_le_bytes(read_fixed(entries_data, &mut pos)?);
        let _reserved: [u8; 7] = read_fixed(entries_data, &mut pos)?;

        entries.push(SegmentEntry {
            hash,
            start_lba,
            lba_length,
            compressed,
            kind,
            stored_offset,
            stored_length,
            data: None,
            delta_options: Vec::new(),
        });
    }

    // Pass 2: parse delta table (appended after base entries, before inputs).
    if has_deltas && pos < entries_data.len() {
        parse_delta_table(entries_data, base_len, &mut entries)?;
    }

    // Pass 3: parse inputs table (tail of index section).
    let mut inputs = Vec::with_capacity(inputs_length as usize / INPUT_ULID_LEN);
    let inputs_data = &data[entries_region_end..];
    let mut ipos = 0usize;
    while ipos < inputs_data.len() {
        let bytes: [u8; INPUT_ULID_LEN] = read_fixed(inputs_data, &mut ipos)?;
        inputs.push(ulid::Ulid::from_bytes(bytes));
    }

    Ok((entries, inputs))
}

/// Parse the delta table from the index section, starting at `table_start`.
///
/// Each delta table entry: entry_index(4) + delta_count(1) + N × 45 bytes.
fn parse_delta_table(
    data: &[u8],
    table_start: usize,
    entries: &mut [SegmentEntry],
) -> io::Result<()> {
    let mut pos = table_start;
    while pos < data.len() {
        let entry_index = u32::from_le_bytes(read_fixed(data, &mut pos)?) as usize;
        let delta_count = read_u8(data, &mut pos)? as usize;
        if entry_index >= entries.len() {
            return Err(io::Error::other(format!(
                "delta table entry_index {entry_index} out of range ({})",
                entries.len()
            )));
        }
        let mut opts = Vec::with_capacity(delta_count);
        for _ in 0..delta_count {
            let source_hash = blake3::Hash::from_bytes(read_fixed(data, &mut pos)?);
            let _option_flags = read_u8(data, &mut pos)?;
            let delta_offset = u64::from_le_bytes(read_fixed(data, &mut pos)?);
            let delta_length = u32::from_le_bytes(read_fixed(data, &mut pos)?);
            opts.push(DeltaOption {
                source_hash,
                delta_offset,
                delta_length,
            });
        }
        entries[entry_index].delta_options = opts;
    }
    Ok(())
}

/// Populate `entry.data` by reading body bytes from the segment file.
///
/// `body_section_start` is the absolute file offset of the body section, as
/// returned by `read_segment_index`.  Only entries whose `kind` is in
/// `kinds` and whose `stored_length > 0` are read.
///
/// For `EntryKind::Inline` entries, pass the inline section bytes via
/// `inline_bytes`.  Inline data is sliced from this buffer rather than read
/// from the body section.  When `inline_bytes` is empty and `kinds` includes
/// `Inline`, inline entries are silently skipped.
///
/// Common usage:
/// - `&[EntryKind::Data]` — local pending/ segments where DedupRef body
///   regions are sparse holes (not real data).
/// - `&[EntryKind::Data, EntryKind::DedupRef]` — S3-fetched or materialised
///   segments where all body regions are populated.
/// - `&[EntryKind::Data, EntryKind::Inline]` — repack with inline data.
pub fn read_extent_bodies(
    path: &Path,
    body_section_start: u64,
    entries: &mut [SegmentEntry],
    kinds: impl AsRef<[EntryKind]>,
    inline_bytes: &[u8],
) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let kinds = kinds.as_ref();
    let mut f = fs::File::open(path)?;
    for entry in entries.iter_mut() {
        if entry.stored_length == 0 {
            continue;
        }
        if !kinds.contains(&entry.kind) {
            continue;
        }
        if matches!(entry.kind, EntryKind::Inline | EntryKind::CanonicalInline) {
            let start = entry.stored_offset as usize;
            let end = start + entry.stored_length as usize;
            if end <= inline_bytes.len() {
                entry.data = Some(inline_bytes[start..end].to_vec());
            }
            continue;
        }
        f.seek(SeekFrom::Start(body_section_start + entry.stored_offset))?;
        let mut buf = vec![0u8; entry.stored_length as usize];
        f.read_exact(&mut buf)?;
        entry.data = Some(buf);
    }
    Ok(())
}

/// Verify that `body` hashes to `entry.hash`.
///
/// `body` is the stored bytes as they appear in the segment — if the entry
/// is marked compressed, `body` is lz4-compressed and is decompressed here
/// before hashing. The declared hash is always computed over the 4 KiB-
/// aligned uncompressed block content (see `Volume::write`).
///
/// Non-body kinds (`DedupRef`, `Zero`, `Delta`) return `Ok(())` — they carry
/// no body and their integrity is guaranteed by the segment signature.
///
/// Returns `io::ErrorKind::InvalidData` on mismatch so callers can
/// distinguish corruption from unrelated I/O failures.
pub fn verify_body_hash(entry: &SegmentEntry, body: &[u8]) -> io::Result<()> {
    if !matches!(
        entry.kind,
        EntryKind::Data | EntryKind::Inline | EntryKind::CanonicalData | EntryKind::CanonicalInline
    ) {
        return Ok(());
    }
    let computed = if entry.compressed {
        let decompressed = lz4_flex::decompress_size_prepended(body).map_err(io::Error::other)?;
        blake3::hash(&decompressed)
    } else {
        blake3::hash(body)
    };
    if computed == entry.hash {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "body hash mismatch at lba={} len={}B: declared={} computed={}",
                entry.start_lba,
                body.len(),
                &entry.hash.to_hex()[..16],
                &computed.to_hex()[..16],
            ),
        ))
    }
}

// --- promotion ---

/// Write a pending segment from the accumulated WAL entries and durably
/// commit it at `pending/<ulid>`.
///
/// This is the "heavy middle" of a WAL promote — write `.tmp`, rename to the
/// final name, fsync the directory — with no WAL-file delete. The caller is
/// responsible for deleting the WAL (or, once the promote is offloaded, for
/// deferring the delete until after the apply phase has published the new
/// snapshot).
///
/// `entries` must be the DATA and REF records accumulated during the write
/// session. On return, each entry's `stored_offset` is set to its
/// section-relative position.
///
/// After returning, the caller must:
/// 1. Update the extent index for each DATA entry: the absolute file offset is
///    `body_section_start + entry.stored_offset` (body entries) or the inline
///    section.
/// 2. Delete the original WAL file (or defer to the apply phase).
/// 3. Open a fresh WAL.
///
/// Returns `body_section_start` so the caller can compute absolute offsets.
pub fn write_and_commit(
    pending_dir: &Path,
    ulid: ulid::Ulid,
    entries: &mut [SegmentEntry],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    let ulid_str = ulid.to_string();
    let tmp_path = pending_dir.join(format!("{ulid_str}.tmp"));
    let final_path = pending_dir.join(&ulid_str);

    let body_section_start = write_segment(&tmp_path, entries, signer)?;

    // Atomic rename — COMMIT POINT.
    fs::rename(&tmp_path, &final_path)?;
    // Fsync the directory so the rename is durable before the caller acts
    // on the committed segment.
    fsync_dir(&final_path)?;

    Ok(body_section_start)
}

/// Promote a WAL to a committed local segment and delete the WAL file.
///
/// Thin wrapper around [`write_and_commit`]: commits the segment, then
/// deletes the WAL atomically with respect to the rest of the promote.
/// Callers that need to defer the WAL delete until after the apply phase
/// (e.g. the off-actor worker path) should call `write_and_commit` directly.
pub fn promote(
    wal_path: &Path,
    ulid: ulid::Ulid,
    pending_dir: &Path,
    entries: &mut [SegmentEntry],
    signer: &dyn SegmentSigner,
) -> io::Result<u64> {
    let body_section_start = write_and_commit(pending_dir, ulid, entries, signer)?;
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
pub fn write_file_atomic(path: &Path, content: &[u8]) -> io::Result<()> {
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

/// Copy a segment body into the local cache, preserving sparseness.
///
/// Reads `src_path` (a full segment in `pending/` or `gc/`) and writes
/// `body_path` (`cache/<ulid>.body`) containing only the Data-entry body bytes
/// at their stored offsets.  DedupRef and Zero regions are left as OS holes:
/// `set_len(body_length)` establishes the file size, and only Data entries are
/// written, so unwritten regions never allocate disk blocks.  The `.present`
/// bitset marks only Data entries as present; reads of DedupRef ranges fall
/// through to the canonical segment via the extent index.
///
/// If the source segment has a populated delta body section
/// (`header.delta_length > 0`), the delta blobs are written to a
/// separate `cache/<id>.delta` file (derived from `body_path` by
/// swapping the extension), starting at byte 0 and sized exactly
/// `delta_length`. Keeping the delta region out of `.body` means
/// `.body` has a single, unambiguous shape (sparse body section,
/// size = body_length) and a pull host can demand-fetch just the
/// delta region by creating `.delta` without touching `.body`.
///
/// All files are written via tmp+rename for crash safety. `.body` is
/// written **last** so its existence acts as a commit marker for the
/// whole promote — any crash before the final rename is recovered by
/// re-running the promote, since the idempotence guard at the top
/// only triggers once `.body` is in place. Callers can therefore
/// trust that if `body_path` exists, `.delta` (when `delta_length >
/// 0`) and `.present` also exist.
pub fn promote_to_cache(src_path: &Path, body_path: &Path, present_path: &Path) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};

    if body_path.try_exists()? {
        return Ok(());
    }

    let mut src = fs::File::open(src_path)?;
    let mut header = [0u8; HEADER_LEN as usize];
    src.read_exact(&mut header)?;
    if &header[0..8] != MAGIC {
        return Err(io::Error::other("bad segment magic"));
    }
    let entry_count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let index_length = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    let inline_length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    let body_length = u64::from_le_bytes([
        header[20], header[21], header[22], header[23], header[24], header[25], header[26],
        header[27],
    ]);
    let delta_length = u32::from_le_bytes([header[28], header[29], header[30], header[31]]) as u64;
    let inputs_length = u32::from_le_bytes([header[32], header[33], header[34], header[35]]);
    let body_section_start = HEADER_LEN + index_length as u64 + inline_length as u64;

    let mut index_data = vec![0u8; index_length as usize];
    src.read_exact(&mut index_data)?;
    let (entries, _inputs) = parse_index_section(&index_data, entry_count, inputs_length)?;

    // Build the sparse body in a temp file but do not rename yet —
    // the rename is the last step so `.body`'s existence implies
    // `.delta` and `.present` are both already committed.
    let body_tmp = {
        let mut name = body_path
            .file_name()
            .ok_or_else(|| io::Error::other("path has no filename"))?
            .to_owned();
        name.push(".tmp");
        body_path.with_file_name(name)
    };
    {
        let mut dst = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&body_tmp)?;
        dst.set_len(body_length)?;
        for entry in &entries {
            if !matches!(entry.kind, EntryKind::Data | EntryKind::CanonicalData)
                || entry.stored_length == 0
            {
                continue;
            }
            src.seek(SeekFrom::Start(body_section_start + entry.stored_offset))?;
            dst.seek(SeekFrom::Start(entry.stored_offset))?;
            // io::copy uses copy_file_range/sendfile/fcopyfile where available,
            // keeping the extent bytes in the kernel on Linux.
            let n = io::copy(&mut (&mut src).take(entry.stored_length as u64), &mut dst)?;
            if n != entry.stored_length as u64 {
                return Err(io::Error::other("short read promoting segment body"));
            }
        }
        dst.sync_data()?;
    }

    // Commit .delta first so that once .body appears, .delta is
    // guaranteed to be in place.
    if delta_length > 0 {
        let delta_path = body_path.with_extension("delta");
        let delta_tmp = {
            let mut name = delta_path
                .file_name()
                .ok_or_else(|| io::Error::other("path has no filename"))?
                .to_owned();
            name.push(".tmp");
            delta_path.with_file_name(name)
        };
        {
            let mut dst = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&delta_tmp)?;
            src.seek(SeekFrom::Start(body_section_start + body_length))?;
            let n = io::copy(&mut (&mut src).take(delta_length), &mut dst)?;
            if n != delta_length {
                return Err(io::Error::other("short read promoting delta body"));
            }
            dst.sync_data()?;
        }
        fs::rename(&delta_tmp, &delta_path)?;
        fsync_dir(&delta_path)?;
    }

    // Commit .present next, still before .body.
    let bitset_len = (entry_count as usize).div_ceil(8);
    let mut bitset = vec![0u8; bitset_len];
    for (i, entry) in entries.iter().enumerate() {
        if entry.kind == EntryKind::Data {
            bitset[i / 8] |= 1 << (i % 8);
        }
    }
    write_file_atomic(present_path, &bitset)?;

    // Final rename makes the whole promote visible atomically — any
    // crash before this point leaves the old state intact and the
    // next promote re-runs from scratch.
    fs::rename(&body_tmp, body_path)?;
    fsync_dir(body_path)?;

    Ok(())
}

/// Layout of the file that holds a segment's body bytes, telling the
/// caller how to compute an absolute seek from an entry-relative
/// `body_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentBodyLayout {
    /// Full segment file: byte 0 is the segment header. Body and delta
    /// sections sit after the header+index+inline prefix of length
    /// `body_section_start`. Used by `wal/<id>`, `pending/<id>`, and
    /// `gc/<id>` (post-`.applied`).
    FullSegment,
    /// Body-only file: byte 0 is body byte 0. The full segment's
    /// header+index+inline prefix is not present. Used by
    /// `cache/<id>.body`.
    BodyOnly,
}

impl SegmentBodyLayout {
    /// File offset at which the body section begins in this file.
    /// Callers compose this with intra-section offsets:
    ///   - extent body: `body_section_file_offset(bss) + body_offset`
    ///   - delta body : `body_section_file_offset(bss) + body_length + delta_offset`
    #[inline]
    pub fn body_section_file_offset(self, body_section_start: u64) -> u64 {
        match self {
            Self::FullSegment => body_section_start,
            Self::BodyOnly => 0,
        }
    }
}

/// Resolve a segment's body location within a single fork directory,
/// following the canonical lifecycle precedence:
///
///   1. `wal/<id>`                       — live, being written
///   2. `pending/<id>`                   — sealed, awaiting upload
///   3. `gc/<id>` (bare, no suffix)      — GC output, awaiting upload
///   4. `cache/<id>.body`                — drained or demand-fetched
///
/// Returns `None` if the segment body is not present in any of those
/// locations. Does not consult `.present` bits, ancestor forks, or
/// fetchers — callers layer those concerns on top.
///
/// Under the self-describing GC handoff protocol a bare `gc/<id>` means
/// "volume-applied, awaiting coordinator upload"; a `gc/<id>.staged`
/// (coordinator-staged, not yet volume-applied) has the `.staged`
/// extension and is naturally excluded by the bare-name match.
pub fn locate_segment_body(
    base_dir: &Path,
    segment_id: ulid::Ulid,
) -> Option<(PathBuf, SegmentBodyLayout)> {
    let sid = segment_id.to_string();
    let wal = base_dir.join("wal").join(&sid);
    if wal.exists() {
        return Some((wal, SegmentBodyLayout::FullSegment));
    }
    let pending = base_dir.join("pending").join(&sid);
    if pending.exists() {
        return Some((pending, SegmentBodyLayout::FullSegment));
    }
    let gc_body = base_dir.join("gc").join(&sid);
    if gc_body.exists() {
        return Some((gc_body, SegmentBodyLayout::FullSegment));
    }
    let cache_body = base_dir.join("cache").join(format!("{sid}.body"));
    if cache_body.exists() {
        return Some((cache_body, SegmentBodyLayout::BodyOnly));
    }
    None
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
/// These are segments that the volume has applied and re-signed within `gc/`
/// but that the coordinator has not yet uploaded to S3 and written
/// `index/<new>.idx` for. They must be included in LBA map and extent index
/// rebuild at lower priority than `index/*.idx` entries so the original input
/// segments (still in `index/`) win for any LBA they cover.
///
/// Under the self-describing GC handoff protocol, a bare `gc/<id>` means
/// "volume-applied, awaiting coordinator upload." Bodies with a `.pending`
/// sibling (legacy coordinator-signed staged state) or a `.staged` sibling
/// (step 4 coordinator-staged state) are excluded — the old input segments
/// referenced by `index/` are still authoritative until the volume applies.
pub fn collect_gc_applied_segment_files(fork_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let gc_dir = fork_dir.join("gc");
    match fs::read_dir(&gc_dir) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
        Ok(entries) => {
            let mut paths = Vec::new();
            for entry in entries {
                let path = entry?.path();
                // Skip sidecar files (.pending, .applied, .done, .staged, .tmp).
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
                // Exclude bodies still in a coordinator-staged state:
                // `.pending` (legacy manifest path) or `.staged` (step 4
                // derive path). Either sibling means the volume has not yet
                // re-signed the body.
                if gc_dir.join(format!("{name}.pending")).exists()
                    || gc_dir.join(format!("{name}.staged")).exists()
                {
                    continue;
                }
                paths.push(path);
            }
            Ok(paths)
        }
    }
}

/// Punch a hole in `file`, releasing the disk blocks backing `[offset, offset + length)`.
///
/// On Linux uses `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`:
/// file size is preserved, reads of the punched range return zeros, and the
/// underlying disk blocks are freed. On platforms without a direct equivalent
/// (macOS, BSDs) the fallback zero-writes the range — reads still return
/// zeros but the blocks remain allocated.
pub fn punch_hole(file: &mut fs::File, offset: u64, length: u64) -> io::Result<()> {
    if length == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use nix::fcntl::{FallocateFlags, fallocate};
        fallocate(
            file,
            FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE,
            offset as i64,
            length as i64,
        )
        .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(offset))?;
        const CHUNK: usize = 64 * 1024;
        let chunk_size = CHUNK.min(length as usize);
        let zeros = vec![0u8; chunk_size];
        let mut remaining = length;
        while remaining > 0 {
            let n = (remaining as usize).min(chunk_size);
            file.write_all(&zeros[..n])?;
            remaining -= n as u64;
        }
        Ok(())
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

// --- fork rebuild discovery ---

/// Which source directory a discovered segment lives in. Dictates
/// rebuild processing priority within a single fork:
/// `GcApplied < Index < Pending` (last-write-wins on overlapping LBAs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentTier {
    /// `gc/<ulid>` — volume-applied GC output awaiting coordinator upload.
    GcApplied,
    /// `index/<ulid>.idx` — committed, promoted segment.
    Index,
    /// `pending/<ulid>` — in-flight WAL flush, not yet promoted.
    Pending,
}

/// A segment file discovered during a fork rebuild pass.
#[derive(Debug)]
pub struct SegmentRef {
    pub ulid: ulid::Ulid,
    pub tier: SegmentTier,
    pub path: PathBuf,
}

/// Discover every segment file for a single fork, in rebuild-processing
/// order (lowest priority first).
///
/// **Listing order: pending → gc → index.** A concurrent promote writes
/// `index/<ulid>.idx` *then* removes the source (`pending/<ulid>` or
/// `gc/<ulid>`). Listing sources first guarantees a mid-promote segment
/// is captured by at least one list — either its source (before the
/// removal) or the new index entry (written before the removal).
/// Reversing the order (index first) leaves a narrow window in which a
/// segment is absent from both lists; see `extentindex::rebuild` for
/// the same discipline.
///
/// **Processing order: gc → index → pending**, each tier sorted by ULID.
/// This is the priority ordering — later processing wins when the same
/// LBA appears in multiple tiers.
///
/// `branch_ulid` applies an ULID-string cutoff used for ancestor layers
/// in forked volumes: entries with ULID string greater than the cutoff
/// are excluded. `None` includes everything.
pub fn discover_fork_segments(
    fork_dir: &Path,
    branch_ulid: Option<&str>,
) -> io::Result<Vec<SegmentRef>> {
    // pending/ first (source of drain-path promote).
    let mut pending_paths = collect_segment_files(&fork_dir.join("pending"))?;
    pending_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

    // gc/ next (source of gc-carried promote).
    let mut gc_paths = collect_gc_applied_segment_files(fork_dir)?;
    gc_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

    // index/ last (destination of both promote paths).
    let mut idx_paths = collect_idx_files(&fork_dir.join("index"))?;
    idx_paths.sort_unstable_by(|a, b| a.file_stem().cmp(&b.file_stem()));

    // Build output in processing order: gc → index → pending.
    let mut out: Vec<SegmentRef> =
        Vec::with_capacity(gc_paths.len() + idx_paths.len() + pending_paths.len());

    for p in gc_paths {
        if let Some(sref) = segment_ref_from_path(p, SegmentTier::GcApplied, branch_ulid) {
            out.push(sref);
        }
    }
    for p in idx_paths {
        if let Some(sref) = segment_ref_from_path(p, SegmentTier::Index, branch_ulid) {
            out.push(sref);
        }
    }
    for p in pending_paths {
        if let Some(sref) = segment_ref_from_path(p, SegmentTier::Pending, branch_ulid) {
            out.push(sref);
        }
    }

    Ok(out)
}

/// Build a `SegmentRef` from a path, applying the optional branch-ulid
/// cutoff. Returns `None` if the filename is not a valid ULID or if the
/// cutoff excludes it.
fn segment_ref_from_path(
    path: PathBuf,
    tier: SegmentTier,
    branch_ulid: Option<&str>,
) -> Option<SegmentRef> {
    // `file_stem` handles both bare ULIDs (pending/, gc/) and `<ulid>.idx`.
    let stem = path.file_stem()?.to_str()?;
    if let Some(cutoff) = branch_ulid
        && stem > cutoff
    {
        return None;
    }
    let ulid = ulid::Ulid::from_string(stem).ok()?;
    Some(SegmentRef { ulid, tier, path })
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
        // Use data larger than INLINE_THRESHOLD so this is a body entry.
        let data = vec![0x42u8; 8192];
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_data(
            hash,
            10,
            2,
            SegmentFlags::empty(),
            data.clone(),
        )];
        let bss = write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (bss2, read_entries, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(bss, bss2);
        assert_eq!(read_entries.len(), 1);

        let e = &read_entries[0];
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 10);
        assert_eq!(e.lba_length, 2);
        assert!(!e.compressed);
        assert_eq!(e.kind, EntryKind::Data);
        assert_eq!(e.stored_offset, 0);
        assert_eq!(e.stored_length, 8192);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_inputs_empty_by_default() {
        // Non-GC segments write an empty inputs list. Reader returns Vec::new().
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data = vec![0x11u8; 4096];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();
        let (_, _, inputs) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert!(inputs.is_empty());
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn roundtrip_gc_inputs_preserved() {
        // write_gc_segment stores the inputs list inside the index section and
        // the signature authenticates it. read_* returns the same ULIDs in order.
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data = vec![0x33u8; 4096];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let inputs_in = vec![ulid::Ulid::new(), ulid::Ulid::new(), ulid::Ulid::new()];
        write_gc_segment(&path, &mut entries, &inputs_in, signer.as_ref()).unwrap();

        let (_, read_back, inputs_out) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 1);
        assert_eq!(inputs_out, inputs_in);

        // Unverified reader returns the same inputs.
        let (_, _, inputs_unverified) = read_segment_index(&path).unwrap();
        assert_eq!(inputs_unverified, inputs_in);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn inputs_tamper_fails_signature() {
        // Mutating a byte in the inputs table invalidates the signature,
        // confirming the inputs region is authenticated.
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data = vec![0x44u8; 4096];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let inputs_in = vec![ulid::Ulid::new()];
        write_gc_segment(&path, &mut entries, &inputs_in, signer.as_ref()).unwrap();

        // Flip one byte inside the inputs region (tail of index section).
        let mut bytes = fs::read(&path).unwrap();
        let idx_len = {
            let h = SegmentHeader::read_from_bytes(&bytes[..HEADER_LEN as usize]).unwrap();
            h.index_length.get() as usize
        };
        let inputs_byte = HEADER_LEN as usize + idx_len - 1;
        bytes[inputs_byte] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        let err = read_and_verify_segment_index(&path, &vk).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
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

        let (_, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();
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

        let (_, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();
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

        let data2 = b"x".repeat(8192);
        let mut entries = vec![
            SegmentEntry::new_data(data_hash, 0, 2, SegmentFlags::empty(), data),
            SegmentEntry::new_dedup_ref(ref_hash, 2, 1),
            SegmentEntry::new_data(blake3::hash(&data2), 10, 2, SegmentFlags::empty(), data2),
        ];

        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 3);

        assert_eq!(read_back[0].kind, EntryKind::Data);
        assert_eq!(read_back[0].stored_offset, 0); // first body entry
        assert_eq!(read_back[0].stored_length, 8192);

        // DedupRef contributes nothing to the body: zeroed offset/length.
        assert_eq!(read_back[1].kind, EntryKind::DedupRef);
        assert_eq!(read_back[1].stored_offset, 0);
        assert_eq!(read_back[1].stored_length, 0);

        // Next Data entry lands directly after the first Data — no DedupRef gap.
        assert_eq!(read_back[2].kind, EntryKind::Data);
        assert_eq!(read_back[2].stored_offset, 8192);
        assert_eq!(read_back[2].stored_length, 8192);

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

        let (_, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 1);
        assert!(read_back[0].compressed);
        assert_eq!(read_back[0].stored_length, 2048);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn body_bytes_readable_via_stored_offset() {
        // Verify that body bytes are actually written at the right offsets.
        // Uses data larger than INLINE_THRESHOLD so entries go into the body section.
        use std::io::{Read, Seek, SeekFrom};

        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data1 = vec![0x11u8; 8192];
        let data2 = vec![0x22u8; 8192];
        let h1 = blake3::hash(&data1);
        let h2 = blake3::hash(&data2);

        let mut entries = vec![
            SegmentEntry::new_data(h1, 0, 2, SegmentFlags::empty(), data1.clone()),
            SegmentEntry::new_data(h2, 2, 2, SegmentFlags::empty(), data2.clone()),
        ];

        let bss = write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let (bss2, index, _) = read_and_verify_segment_index(&path, &vk).unwrap();
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
    fn old_format_version_rejected() {
        // Segments written with the pre-thin-DedupRef format (magic "\x03")
        // must be rejected. Per no-compat-by-default, there is no migration
        // path — users regenerate volumes from their source data.
        let path = temp_path(".seg");
        let (_, vk) = test_signer();
        let mut buf = [0u8; HEADER_LEN as usize];
        buf[..8].copy_from_slice(b"ELIDSEG\x03");
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
        let (bss2, read_back, _) = read_and_verify_segment_index(&seg_path, &vk).unwrap();
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
        fs::write(&path, [0u8]).unwrap();
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
        let (bss, _, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        let idx_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA.idx");
        fs::write(&idx_path, &full_bytes[..bss as usize]).unwrap();
        assert_eq!(read_entry_count(&idx_path).unwrap(), 1);

        fs::remove_dir_all(dir).unwrap();
    }

    // --- inline extent tests ---

    #[test]
    fn inline_entry_roundtrip() {
        // Data below INLINE_THRESHOLD (256 bytes) becomes an Inline entry.
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();
        let data = vec![0x42u8; 100]; // well below 256-byte threshold
        let hash = blake3::hash(&data);

        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data.clone(),
        )];
        assert_eq!(entries[0].kind, EntryKind::Inline);

        let bss = write_segment(&path, &mut entries, signer.as_ref()).unwrap();
        let (bss2, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(bss, bss2);
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].kind, EntryKind::Inline);
        assert_eq!(read_back[0].hash, hash);
        assert_eq!(read_back[0].stored_length, 100);

        // Inline data lives in the inline section, not body.
        // Body section should be empty (body_length = 0 in header).
        let file_len = fs::metadata(&path).unwrap().len();
        // file = header(96) + index(64) + inline(100) + body(0) = 260
        assert_eq!(file_len, bss); // bss = header + index + inline; no body beyond it

        // Verify inline section contains the data.
        let inline_bytes = read_inline_section(&path).unwrap();
        assert_eq!(inline_bytes.len(), 100);
        assert_eq!(inline_bytes, data);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn mixed_inline_and_body_entries() {
        // A segment with both inline and body entries.
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();

        let small = vec![0xAAu8; 64]; // inline (below 256-byte threshold)
        let large = vec![0xBBu8; 8192]; // body
        let small_hash = blake3::hash(&small);
        let large_hash = blake3::hash(&large);

        let mut entries = vec![
            SegmentEntry::new_data(small_hash, 0, 1, SegmentFlags::empty(), small.clone()),
            SegmentEntry::new_data(large_hash, 1, 2, SegmentFlags::empty(), large.clone()),
        ];
        assert_eq!(entries[0].kind, EntryKind::Inline);
        assert_eq!(entries[1].kind, EntryKind::Data);

        write_segment(&path, &mut entries, signer.as_ref()).unwrap();
        let (_, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();

        assert_eq!(read_back[0].kind, EntryKind::Inline);
        assert_eq!(read_back[0].stored_length, 64);
        assert_eq!(read_back[1].kind, EntryKind::Data);
        assert_eq!(read_back[1].stored_length, 8192);

        // Inline data is readable from inline section.
        let inline_bytes = read_inline_section(&path).unwrap();
        assert_eq!(inline_bytes, small);

        // Body data is readable via read_extent_bodies.
        let (bss, mut entries2, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        read_extent_bodies(&path, bss, &mut entries2, [EntryKind::Data], &[]).unwrap();
        assert_eq!(entries2[1].data.as_deref(), Some(large.as_slice()));
        // Inline entry data is NOT populated by body read (empty inline_bytes).
        assert!(entries2[0].data.is_none());

        // Now read with inline bytes too.
        let (_, mut entries3, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        read_extent_bodies(
            &path,
            bss,
            &mut entries3,
            [EntryKind::Data, EntryKind::Inline],
            &inline_bytes,
        )
        .unwrap();
        assert_eq!(entries3[0].data.as_deref(), Some(small.as_slice()));
        assert_eq!(entries3[1].data.as_deref(), Some(large.as_slice()));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn all_inline_segment_has_zero_body_length() {
        // When every entry is inline, body_length should be 0.
        let path = temp_path(".seg");
        let (signer, vk) = test_signer();

        let d1 = vec![0x11u8; 32];
        let d2 = vec![0x22u8; 64];
        let mut entries = vec![
            SegmentEntry::new_data(blake3::hash(&d1), 0, 1, SegmentFlags::empty(), d1.clone()),
            SegmentEntry::new_data(blake3::hash(&d2), 1, 1, SegmentFlags::empty(), d2.clone()),
        ];
        assert!(entries.iter().all(|e| e.kind == EntryKind::Inline));

        let bss = write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        // File size == bss (no body section).
        let file_len = fs::metadata(&path).unwrap().len();
        assert_eq!(file_len, bss);

        // read_inline_section returns both entries' data concatenated.
        let inline_bytes = read_inline_section(&path).unwrap();
        assert_eq!(inline_bytes.len(), 32 + 64);

        // Entries are parseable and both inline.
        let (_, read_back, _) = read_and_verify_segment_index(&path, &vk).unwrap();
        assert_eq!(read_back.len(), 2);
        assert!(read_back.iter().all(|e| e.kind == EntryKind::Inline));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn extract_idx_preserves_inline_section() {
        // .idx files must include the inline section so inline data survives
        // cache eviction.
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let seg_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let idx_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA.idx");
        let (signer, vk) = test_signer();

        let data = vec![0xFFu8; 200];
        let hash = blake3::hash(&data);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            data.clone(),
        )];
        write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        // Extract .idx from the full segment.
        extract_idx(&seg_path, &idx_path).unwrap();

        // .idx should include inline section.
        let idx_inline = read_inline_section(&idx_path).unwrap();
        assert_eq!(idx_inline, data, ".idx must contain inline data");

        // Entries parsed from .idx should match.
        let (_, idx_entries, _) = read_and_verify_segment_index(&idx_path, &vk).unwrap();
        assert_eq!(idx_entries.len(), 1);
        assert_eq!(idx_entries[0].kind, EntryKind::Inline);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rewrite_with_deltas_roundtrip() {
        // Write a segment with two DATA entries, rewrite with delta options
        // on the first entry, read back and verify.
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let seg_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let delta_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA.delta");
        let (signer, vk) = test_signer();

        let data1 = vec![0xAAu8; 8192];
        let data2 = vec![0xBBu8; 4096];
        let hash1 = blake3::hash(&data1);
        let hash2 = blake3::hash(&data2);
        let mut entries = vec![
            SegmentEntry::new_data(hash1, 0, 2, SegmentFlags::empty(), data1),
            SegmentEntry::new_data(hash2, 2, 1, SegmentFlags::empty(), data2),
        ];
        write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        // Fake delta blob and source hash.
        let source_hash = blake3::hash(b"source-extent");
        let delta_blob = b"compressed-delta-data";
        let delta_body = delta_blob.to_vec();

        let deltas = vec![(
            0,
            vec![DeltaOption {
                source_hash,
                delta_offset: 0,
                delta_length: delta_blob.len() as u32,
            }],
        )];

        rewrite_with_deltas(
            &seg_path,
            &delta_path,
            &deltas,
            &delta_body,
            signer.as_ref(),
        )
        .unwrap();

        // Read back and verify.
        let (bss, read_entries, _) = read_and_verify_segment_index(&delta_path, &vk).unwrap();
        assert_eq!(read_entries.len(), 2);

        // First entry should have HAS_DELTAS and one delta option.
        let e0 = &read_entries[0];
        assert_eq!(e0.hash, hash1);
        assert_eq!(e0.delta_options.len(), 1);
        assert_eq!(e0.delta_options[0].source_hash, source_hash);
        assert_eq!(e0.delta_options[0].delta_offset, 0);
        assert_eq!(e0.delta_options[0].delta_length, delta_blob.len() as u32);

        // Second entry should have no deltas.
        let e1 = &read_entries[1];
        assert_eq!(e1.hash, hash2);
        assert!(e1.delta_options.is_empty());

        // Body data should still be readable.
        let mut read_back = read_entries;
        read_extent_bodies(&delta_path, bss, &mut read_back, [EntryKind::Data], &[]).unwrap();
        assert_eq!(read_back[0].data.as_deref(), Some(&[0xAAu8; 8192][..]));
        assert_eq!(read_back[1].data.as_deref(), Some(&[0xBBu8; 4096][..]));

        // Verify delta_length in header by reading raw bytes.
        let raw_header = fs::read(&delta_path).unwrap();
        let h = SegmentHeader::read_from_bytes(&raw_header[..HEADER_LEN as usize]).unwrap();
        assert_eq!(h.delta_length.get(), delta_blob.len() as u32);

        // Verify delta body bytes at the end of the file.
        let file_len = fs::metadata(&delta_path).unwrap().len();
        let delta_start = file_len - delta_blob.len() as u64;
        let delta_bytes = &raw_header[delta_start as usize..];
        assert_eq!(delta_bytes, delta_blob);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn delta_entry_roundtrip() {
        // A thin Delta entry must roundtrip through write_segment /
        // read_segment_index with stored_offset=0, stored_length=0, the
        // kind preserved, and the delta options intact. It must not
        // contribute to body_length — a segment containing a Data entry
        // plus a Delta entry has the same body_length as a segment with
        // just the Data entry.
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let seg_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let (signer, vk) = test_signer();

        // A normal Data entry so we have something in the body section.
        let data_body = vec![0xAAu8; 8192];
        let data_hash = blake3::hash(&data_body);

        // A Delta entry: its own content hash (post-decompression) plus
        // one delta option pointing at a source extent.
        let delta_content_hash = blake3::hash(b"delta-content");
        let source_hash = blake3::hash(b"source-extent");
        let delta_option = DeltaOption {
            source_hash,
            delta_offset: 0,
            delta_length: 128,
        };

        let mut entries = vec![
            SegmentEntry::new_data(data_hash, 0, 2, SegmentFlags::empty(), data_body.clone()),
            SegmentEntry::new_delta(delta_content_hash, 10, 3, vec![delta_option.clone()]),
        ];
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        // body_length must include only the Data entry's bytes; the Delta
        // entry must reserve none.
        let raw = fs::read(&seg_path).unwrap();
        let header = SegmentHeader::read_from_bytes(&raw[..HEADER_LEN as usize]).unwrap();
        assert_eq!(header.body_length.get(), data_body.len() as u64);
        assert_eq!(
            header.delta_length.get(),
            0,
            "write_segment does not write a delta body"
        );

        // Roundtrip parse. Kind must be Delta, stored_offset/length both
        // zero, and the single delta option preserved verbatim.
        let (bss_read, read_back, _) = read_and_verify_segment_index(&seg_path, &vk).unwrap();
        assert_eq!(bss_read, bss);
        assert_eq!(read_back.len(), 2);

        let data_entry = &read_back[0];
        assert_eq!(data_entry.kind, EntryKind::Data);
        assert_eq!(data_entry.hash, data_hash);

        let delta_entry = &read_back[1];
        assert_eq!(delta_entry.kind, EntryKind::Delta);
        assert_eq!(delta_entry.hash, delta_content_hash);
        assert_eq!(delta_entry.start_lba, 10);
        assert_eq!(delta_entry.lba_length, 3);
        assert_eq!(delta_entry.stored_offset, 0);
        assert_eq!(delta_entry.stored_length, 0);
        assert_eq!(delta_entry.delta_options.len(), 1);
        assert_eq!(delta_entry.delta_options[0].source_hash, source_hash);
        assert_eq!(delta_entry.delta_options[0].delta_offset, 0);
        assert_eq!(delta_entry.delta_options[0].delta_length, 128);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn delta_entry_supports_multiple_source_options() {
        // Multiple delta options per entry are valid — the reader picks
        // the first source that resolves locally (earliest-source preference
        // applies at read time). Verify the writer/parser preserve all of
        // them in order.
        let dir = temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let seg_path = dir.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        let (signer, vk) = test_signer();

        let content_hash = blake3::hash(b"multi-source-delta");
        let options = vec![
            DeltaOption {
                source_hash: blake3::hash(b"source-a"),
                delta_offset: 0,
                delta_length: 100,
            },
            DeltaOption {
                source_hash: blake3::hash(b"source-b"),
                delta_offset: 100,
                delta_length: 200,
            },
            DeltaOption {
                source_hash: blake3::hash(b"source-c"),
                delta_offset: 300,
                delta_length: 50,
            },
        ];

        let mut entries = vec![SegmentEntry::new_delta(content_hash, 0, 1, options.clone())];
        write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        let (_, read_back, _) = read_and_verify_segment_index(&seg_path, &vk).unwrap();
        assert_eq!(read_back.len(), 1);
        let entry = &read_back[0];
        assert_eq!(entry.kind, EntryKind::Delta);
        assert_eq!(entry.delta_options.len(), 3);
        for (orig, read) in options.iter().zip(entry.delta_options.iter()) {
            assert_eq!(orig.source_hash, read.source_hash);
            assert_eq!(orig.delta_offset, read.delta_offset);
            assert_eq!(orig.delta_length, read.delta_length);
        }

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn locate_segment_body_precedence_and_gc_bare() {
        // Confirms the canonical lifecycle precedence
        //   wal → pending → gc/ bare → cache/.body
        // and that `gc/<id>.staged` is naturally excluded (extension
        // filter) while bare `gc/<id>` is accepted without any sidecar.
        let dir = temp_dir();
        for sub in ["wal", "pending", "gc", "cache"] {
            fs::create_dir_all(dir.join(sub)).unwrap();
        }
        let sid = ulid::Ulid::new();
        let sid_s = sid.to_string();

        // No file anywhere → None.
        assert!(locate_segment_body(&dir, sid).is_none());

        // cache/.body only → BodyOnly hit.
        let cache_body = dir.join("cache").join(format!("{sid_s}.body"));
        fs::write(&cache_body, b"cache").unwrap();
        assert_eq!(
            locate_segment_body(&dir, sid),
            Some((cache_body.clone(), SegmentBodyLayout::BodyOnly))
        );

        // `.staged` alone is not readable — the `.staged` extension means
        // "coordinator-staged, awaiting volume apply" and `locate_segment_body`
        // matches only the bare name.
        let staged = dir.join("gc").join(format!("{sid_s}.staged"));
        fs::write(&staged, b"coordinator-staged").unwrap();
        assert_eq!(
            locate_segment_body(&dir, sid),
            Some((cache_body.clone(), SegmentBodyLayout::BodyOnly))
        );

        // Bare gc/<id> → FullSegment hit, ranking above cache/.body.
        let gc_body = dir.join("gc").join(&sid_s);
        fs::write(&gc_body, b"gc-bare").unwrap();
        assert_eq!(
            locate_segment_body(&dir, sid),
            Some((gc_body.clone(), SegmentBodyLayout::FullSegment))
        );

        // pending/ wins over gc/ bare.
        let pending = dir.join("pending").join(&sid_s);
        fs::write(&pending, b"pending").unwrap();
        assert_eq!(
            locate_segment_body(&dir, sid),
            Some((pending.clone(), SegmentBodyLayout::FullSegment))
        );

        // wal/ wins over everything.
        let wal = dir.join("wal").join(&sid_s);
        fs::write(&wal, b"wal").unwrap();
        assert_eq!(
            locate_segment_body(&dir, sid),
            Some((wal.clone(), SegmentBodyLayout::FullSegment))
        );

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn body_section_file_offset_composes_seek_arithmetic() {
        let full = SegmentBodyLayout::FullSegment;
        let body = SegmentBodyLayout::BodyOnly;
        let bss = 128u64;
        let body_offset = 40u64;
        let body_length = 1024u64;
        // Body read: bss + body_offset (full) vs body_offset (body-only).
        assert_eq!(full.body_section_file_offset(bss) + body_offset, 168);
        assert_eq!(body.body_section_file_offset(bss) + body_offset, 40);
        // Delta read: delta section starts at body_section_start + body_length.
        assert_eq!(full.body_section_file_offset(bss) + body_length, 1152);
        assert_eq!(body.body_section_file_offset(bss) + body_length, 1024);
    }

    // --- verify_body_hash ---

    #[test]
    fn verify_body_hash_accepts_matching_uncompressed_body() {
        let data = vec![0xABu8; 4096];
        let hash = blake3::hash(&data);
        let entry = SegmentEntry::new_data(hash, 0, 1, SegmentFlags::empty(), data.clone());
        assert!(verify_body_hash(&entry, &data).is_ok());
    }

    #[test]
    fn verify_body_hash_rejects_zero_filled_body() {
        let data = vec![0xABu8; 4096];
        let hash = blake3::hash(&data);
        let entry = SegmentEntry::new_data(hash, 0, 1, SegmentFlags::empty(), data);
        let zeros = vec![0u8; 4096];
        let err = verify_body_hash(&entry, &zeros).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("hash mismatch"));
    }

    #[test]
    fn verify_body_hash_accepts_matching_compressed_body() {
        let data = vec![0x55u8; 4096];
        let hash = blake3::hash(&data);
        let compressed = lz4_flex::compress_prepend_size(&data);
        let entry =
            SegmentEntry::new_data(hash, 0, 1, SegmentFlags::COMPRESSED, compressed.clone());
        assert!(verify_body_hash(&entry, &compressed).is_ok());
    }

    #[test]
    fn verify_body_hash_rejects_compressed_with_wrong_plaintext() {
        let declared_data = vec![0x55u8; 4096];
        let declared_hash = blake3::hash(&declared_data);
        let other_data = vec![0xAAu8; 4096];
        let other_compressed = lz4_flex::compress_prepend_size(&other_data);
        let entry = SegmentEntry::new_data(
            declared_hash,
            0,
            1,
            SegmentFlags::COMPRESSED,
            other_compressed.clone(),
        );
        let err = verify_body_hash(&entry, &other_compressed).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn verify_body_hash_skips_non_body_kinds() {
        let fake_hash = blake3::hash(b"anything");
        let deduping = SegmentEntry::new_dedup_ref(fake_hash, 0, 1);
        let zero = SegmentEntry::new_zero(0, 1);
        // Arbitrary bytes that do not hash to `fake_hash` — should still pass.
        assert!(verify_body_hash(&deduping, &[]).is_ok());
        assert!(verify_body_hash(&zero, &[]).is_ok());
    }
}
