//! Read path: extent assembly, segment-file lookup across the fork ancestry,
//! and the LRU-of-open-fds the read path uses to amortise `open` syscalls.
//!
//! Pulled out of `volume/mod.rs` for legibility — no behaviour change. The
//! free functions here are the seam between the writable `Volume` and the
//! read-only `ReadonlyVolume` / actor read snapshots: they take a
//! `(lbamap, extent_index, file_cache, dirs, fetcher)` and serve reads
//! without depending on the broader volume actor state.

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::{
    delta_compute,
    extentindex::{self, BodySource},
    lbamap,
    segment::{self},
};

use super::{AncestorLayer, BoxFetcher, ZERO_HASH};

/// Default capacity for the segment file handle LRU cache.
const FILE_CACHE_CAPACITY: usize = 16;

/// The on-disk layout of a cached segment file, which determines how body
/// offsets are interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::volume) enum SegmentLayout {
    /// A full segment file (wal/, pending/, gc/). Body data starts at
    /// `body_section_start` — callers must add it to body-relative offsets.
    Full,
    /// A `.body` cache file (cache/<id>.body). Contains only body bytes
    /// starting at offset 0 — body-relative offsets are file offsets directly.
    BodyOnly,
}

impl SegmentLayout {
    /// Determine the layout from a file path: `.body` extension → `BodyOnly`,
    /// everything else → `Full`.
    fn from_path(path: &Path) -> Self {
        if path.extension().is_some_and(|e| e == "body") {
            Self::BodyOnly
        } else {
            Self::Full
        }
    }
}

/// Approximate-LRU cache of open segment file handles using the CLOCK algorithm.
///
/// Fixed-size ring buffer keyed by segment ULID. Each slot has a `referenced`
/// bit that is set on access. On eviction the clock hand sweeps the ring,
/// clearing referenced bits until it finds an unreferenced slot to evict.
///
/// The hot-path operation (`get`) is a linear scan + flag set — no data
/// movement, no allocation, no pointer chasing.  At 16 slots the scan fits
/// comfortably in L1 cache.
pub(crate) struct FileCache {
    slots: Vec<Option<FileCacheSlot>>,
    hand: usize,
}

struct FileCacheSlot {
    segment_id: Ulid,
    layout: SegmentLayout,
    file: fs::File,
    referenced: bool,
}

impl Default for FileCache {
    fn default() -> Self {
        Self::new(FILE_CACHE_CAPACITY)
    }
}

impl FileCache {
    pub(crate) fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        Self { slots, hand: 0 }
    }

    /// Look up a cached file handle by segment id.
    /// On hit, sets the referenced bit and returns the layout and file handle.
    pub(in crate::volume) fn get(
        &mut self,
        segment_id: Ulid,
    ) -> Option<(SegmentLayout, &mut fs::File)> {
        let slot = self
            .slots
            .iter_mut()
            .flatten()
            .find(|s| s.segment_id == segment_id)?;
        slot.referenced = true;
        Some((slot.layout, &mut slot.file))
    }

    /// Insert a file handle. If the segment is already cached, replaces it
    /// in-place. Otherwise, uses the CLOCK algorithm to find a slot to evict.
    pub(in crate::volume) fn insert(
        &mut self,
        segment_id: Ulid,
        layout: SegmentLayout,
        file: fs::File,
    ) {
        // Replace in-place if already present.
        for slot in self.slots.iter_mut() {
            if slot.as_ref().is_some_and(|s| s.segment_id == segment_id) {
                *slot = Some(FileCacheSlot {
                    segment_id,
                    layout,
                    file,
                    referenced: true,
                });
                return;
            }
        }

        // Fill an empty slot if one exists.
        for slot in self.slots.iter_mut() {
            if slot.is_none() {
                *slot = Some(FileCacheSlot {
                    segment_id,
                    layout,
                    file,
                    referenced: true,
                });
                return;
            }
        }

        // CLOCK sweep: advance the hand, clearing referenced bits, until we
        // find an unreferenced slot to evict.
        let len = self.slots.len();
        loop {
            let slot = self.slots[self.hand].as_mut().expect("all slots occupied");
            if slot.referenced {
                slot.referenced = false;
                self.hand = (self.hand + 1) % len;
            } else {
                self.slots[self.hand] = Some(FileCacheSlot {
                    segment_id,
                    layout,
                    file,
                    referenced: true,
                });
                self.hand = (self.hand + 1) % len;
                return;
            }
        }
    }

    /// Evict all entries for a given segment.
    pub(crate) fn evict(&mut self, segment_id: Ulid) {
        for slot in self.slots.iter_mut() {
            if slot.as_ref().is_some_and(|s| s.segment_id == segment_id) {
                *slot = None;
            }
        }
    }

    /// Clear all entries.
    pub(crate) fn clear(&mut self) {
        for slot in self.slots.iter_mut() {
            *slot = None;
        }
    }
}

/// Read `lba_count` 4KB blocks starting at `lba` from the given LBA map and extent index.
///
/// Unwritten blocks are returned as zeros. Written blocks are fetched extent-by-extent
/// using `find_segment` to locate each segment file, with recently-opened file handles
/// cached in `file_cache` (LRU) to amortize `open` syscalls across reads.
pub(crate) fn read_extents(
    lba: u64,
    lba_count: u32,
    lbamap: &lbamap::LbaMap,
    extent_index: &extentindex::ExtentIndex,
    file_cache: &RefCell<FileCache>,
    find_segment: impl Fn(Ulid, u64, BodySource) -> io::Result<PathBuf>,
    open_delta_body: impl Fn(Ulid) -> io::Result<fs::File>,
) -> io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut out = vec![0u8; lba_count as usize * 4096];
    for er in lbamap.extents_in_range(lba, lba + lba_count as u64) {
        // Zero extents: output buffer is already zeroed; nothing to fetch.
        if er.hash == ZERO_HASH {
            continue;
        }

        // Extract owned copies so the borrow of extent_index ends before
        // we mutate file_cache.
        let direct = extent_index.lookup(&er.hash).map(|loc| {
            (
                loc.segment_id,
                loc.body_offset,
                loc.body_length,
                loc.compressed,
                loc.body_section_start,
                loc.body_source,
                loc.inline_data.clone(),
            )
        });
        let (
            segment_id,
            body_offset,
            body_length,
            compressed,
            body_section_start,
            body_source,
            inline_data,
        ) = match direct {
            Some(loc) => loc,
            None => {
                // No direct DATA/Inline entry. Try a Delta entry.
                if try_read_delta_extent(
                    &er,
                    lba,
                    extent_index,
                    file_cache,
                    &find_segment,
                    &open_delta_body,
                    &mut out,
                )? {
                    continue;
                }
                continue; // truly unknown — treat as unwritten
            }
        };

        // Inline extents: data is held in memory, no file I/O needed.
        if let Some(ref idata) = inline_data {
            let block_count = (er.range_end - er.range_start) as usize;
            let out_start = (er.range_start - lba) as usize * 4096;
            let out_slice = &mut out[out_start..out_start + block_count * 4096];

            let raw = if compressed {
                lz4_flex::decompress_size_prepended(idata).map_err(io::Error::other)?
            } else {
                idata.to_vec()
            };
            let src_start = er.payload_block_offset as usize * 4096;
            let src_end = src_start + block_count * 4096;
            let src_slice = raw
                .get(src_start..src_end)
                .ok_or_else(|| io::Error::other("corrupt segment: inline payload too short"))?;
            out_slice.copy_from_slice(src_slice);
            continue;
        }

        // For cached entries, always call find_segment to check the .present
        // bitset — the .body file may exist but the specific entry may not
        // yet be fetched.
        let mut cache = file_cache.borrow_mut();
        if matches!(body_source, BodySource::Cached(_)) || cache.get(segment_id).is_none() {
            let path = find_segment(segment_id, body_section_start, body_source)?;
            let layout = SegmentLayout::from_path(&path);
            cache.insert(segment_id, layout, fs::File::open(&path)?);
        }
        let (layout, f) = cache
            .get(segment_id)
            .expect("entry was just inserted or found");

        // body_offset is always body-relative (= stored_offset from the segment index).
        // For full segment files we must add body_section_start to get the file offset.
        let file_body_offset = match layout {
            SegmentLayout::BodyOnly => body_offset,
            SegmentLayout::Full => body_section_start + body_offset,
        };

        let block_count = (er.range_end - er.range_start) as usize;
        let out_start = (er.range_start - lba) as usize * 4096;
        let out_slice = &mut out[out_start..out_start + block_count * 4096];

        if compressed {
            f.seek(SeekFrom::Start(file_body_offset))?;
            let mut compressed_buf = vec![0u8; body_length as usize];
            f.read_exact(&mut compressed_buf)?;
            let decompressed =
                lz4_flex::decompress_size_prepended(&compressed_buf).map_err(|e| {
                    log::error!(
                        "lz4 decompression failed: lba={} segment={} layout={:?} \
                     bss={} body_offset={} body_length={} body_source={:?} \
                     file_body_offset={} first_bytes={:?} err={}",
                        lba,
                        segment_id,
                        layout,
                        body_section_start,
                        body_offset,
                        body_length,
                        body_source,
                        file_body_offset,
                        &compressed_buf[..compressed_buf.len().min(16)],
                        e,
                    );
                    io::Error::other(e)
                })?;
            let src_start = er.payload_block_offset as usize * 4096;
            let src_end = src_start + block_count * 4096;
            let src_slice = decompressed.get(src_start..src_end).ok_or_else(|| {
                io::Error::other("corrupt segment: decompressed payload too short")
            })?;
            out_slice.copy_from_slice(src_slice);
        } else {
            f.seek(SeekFrom::Start(
                file_body_offset + er.payload_block_offset as u64 * 4096,
            ))?;
            if let Err(e) = f.read_exact(out_slice) {
                let file_size = f.metadata().map(|m| m.len()).unwrap_or(0);
                log::error!(
                    "read_extents failed: lba={} segment={} layout={:?} \
                     bss={} body_offset={} body_length={} payload_block_offset={} \
                     file_body_offset={} read_len={} file_size={} err={}",
                    lba,
                    segment_id,
                    layout,
                    body_section_start,
                    body_offset,
                    body_length,
                    er.payload_block_offset,
                    file_body_offset,
                    out_slice.len(),
                    file_size,
                    e,
                );
                return Err(e);
            }
        }
    }
    Ok(out)
}

/// Try to materialise a Delta extent for the range covered by `er`,
/// writing decoded bytes into `out` at the appropriate offset.
///
/// Returns `Ok(true)` if a Delta entry was found and decompressed
/// successfully, `Ok(false)` if no Delta entry is registered for
/// `er.hash` (caller falls through to "unwritten" handling), or
/// `Err` for any I/O or decompression failure.
///
/// Source selection uses the earliest-source preference: scan the
/// delta options in order, pick the first one whose `source_hash`
/// resolves via `extent_index.lookup` to a DATA/Inline location. No
/// caching of decompressed output — each read decompresses fresh.
/// Phase C accepts the decompression cost in exchange for
/// implementation simplicity; a follow-up can add a content-hash-
/// addressed materialisation cache if telemetry shows it matters.
#[allow(clippy::too_many_arguments)]
fn try_read_delta_extent(
    er: &lbamap::ExtentRead,
    lba: u64,
    extent_index: &extentindex::ExtentIndex,
    file_cache: &RefCell<FileCache>,
    find_segment: &dyn Fn(Ulid, u64, BodySource) -> io::Result<PathBuf>,
    open_delta_body: &dyn Fn(Ulid) -> io::Result<fs::File>,
    out: &mut [u8],
) -> io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};

    let Some(delta_loc) = extent_index.lookup_delta(&er.hash) else {
        return Ok(false);
    };
    let delta_segment_id = delta_loc.segment_id;
    let delta_body_source = delta_loc.body_source;
    let options = delta_loc.options.clone();

    // Pick the first option whose source hash resolves to a DATA/Inline
    // location. This is the earliest-source preference in its simplest
    // form; a more sophisticated version (prefer already-cached sources,
    // then earliest ULID among uncached) is a follow-up once the
    // demand-fetch path integrates.
    let mut picked: Option<(segment::DeltaOption, extentindex::ExtentLocation)> = None;
    for opt in &options {
        if let Some(source_loc) = extent_index.lookup(&opt.source_hash) {
            picked = Some((opt.clone(), source_loc.clone()));
            break;
        }
    }
    let Some((opt, source_loc)) = picked else {
        return Err(io::Error::other(format!(
            "delta extent {}: no source option resolved in extent index",
            er.hash.to_hex()
        )));
    };

    // --- Read the source body (full extent, lz4-decompressed if needed). ---
    let source_bytes: Vec<u8> = if let Some(ref idata) = source_loc.inline_data {
        if source_loc.compressed {
            lz4_flex::decompress_size_prepended(idata).map_err(io::Error::other)?
        } else {
            idata.to_vec()
        }
    } else {
        let mut cache = file_cache.borrow_mut();
        if matches!(source_loc.body_source, BodySource::Cached(_))
            || cache.get(source_loc.segment_id).is_none()
        {
            let path = find_segment(
                source_loc.segment_id,
                source_loc.body_section_start,
                source_loc.body_source,
            )?;
            let layout = SegmentLayout::from_path(&path);
            cache.insert(source_loc.segment_id, layout, fs::File::open(&path)?);
        }
        let (layout, f) = cache
            .get(source_loc.segment_id)
            .expect("source just inserted or found");
        let file_body_offset = match layout {
            SegmentLayout::BodyOnly => source_loc.body_offset,
            SegmentLayout::Full => source_loc.body_section_start + source_loc.body_offset,
        };
        f.seek(SeekFrom::Start(file_body_offset))?;
        let mut buf = vec![0u8; source_loc.body_length as usize];
        f.read_exact(&mut buf)?;
        if source_loc.compressed {
            lz4_flex::decompress_size_prepended(&buf).map_err(io::Error::other)?
        } else {
            buf
        }
    };

    // --- Read the delta blob from the Delta segment's delta body section. ---
    //
    // Two shapes: a full segment in `pending/` (delta body inline at
    // `body_section_start + body_length`) or a separate
    // `cache/<id>.delta` file (delta body starts at byte 0). The
    // extent_index records which via `DeltaBodySource`. For the
    // cached case we call `open_delta_body`, which returns an open
    // file handle — demand-fetching from the volume's attached
    // `SegmentFetcher` on miss.
    let delta_blob: Vec<u8> = match delta_body_source {
        extentindex::DeltaBodySource::Full {
            body_section_start: delta_bss,
            body_length: delta_body_length,
        } => {
            let mut cache = file_cache.borrow_mut();
            if cache.get(delta_segment_id).is_none() {
                let path = find_segment(delta_segment_id, delta_bss, BodySource::Local)?;
                let layout = SegmentLayout::from_path(&path);
                cache.insert(delta_segment_id, layout, fs::File::open(&path)?);
            }
            let (_layout, f) = cache
                .get(delta_segment_id)
                .expect("delta segment just inserted or found");
            f.seek(SeekFrom::Start(
                delta_bss + delta_body_length + opt.delta_offset,
            ))?;
            let mut buf = vec![0u8; opt.delta_length as usize];
            f.read_exact(&mut buf)?;
            buf
        }
        extentindex::DeltaBodySource::Cached => {
            // Opens cache/<id>.delta (demand-fetching via the attached
            // `SegmentFetcher` if the file is absent on a pull host).
            // Not routed through `file_cache` because .delta is a
            // distinct file from the segment body, and delta reads
            // are rare enough that caching the FD would complicate
            // eviction for little benefit.
            let mut f = open_delta_body(delta_segment_id)?;
            f.seek(SeekFrom::Start(opt.delta_offset))?;
            let mut buf = vec![0u8; opt.delta_length as usize];
            f.read_exact(&mut buf)?;
            buf
        }
    };

    // Reconstruct the full fragment bytes. We slice out the requested
    // portion below; the decompressor returns every byte the delta was
    // computed over, regardless of which LBA sub-range we want.
    let decompressed = delta_compute::apply_delta(&source_bytes, &delta_blob)?;

    // Copy the requested portion into the output buffer.
    let block_count = (er.range_end - er.range_start) as usize;
    let out_start = (er.range_start - lba) as usize * 4096;
    let out_slice = &mut out[out_start..out_start + block_count * 4096];
    let src_start = er.payload_block_offset as usize * 4096;
    let src_end = src_start + block_count * 4096;
    let src_slice = decompressed
        .get(src_start..src_end)
        .ok_or_else(|| io::Error::other("delta decompressed payload too short"))?;
    out_slice.copy_from_slice(src_slice);
    Ok(true)
}

/// Open `cache/<id>.delta` for reading, demand-fetching it on miss.
///
/// Only called from `try_read_delta_extent` when the extent_index
/// recorded the Delta entry as `DeltaBodySource::Cached` — i.e. the
/// segment has already been promoted to the three-file cache shape,
/// so the delta body, if local, lives in its own `.delta` file
/// rather than inline in a full segment.
///
/// On a pull host where `.delta` is absent the attached fetcher
/// downloads it atomically (tmp+rename) before we open. Returns
/// `NotFound` when the file is missing locally and no fetcher is
/// attached to fetch it.
pub(crate) fn open_delta_body_in_dirs(
    segment_id: Ulid,
    base_dir: &Path,
    ancestor_layers: &[AncestorLayer],
    fetcher: Option<&BoxFetcher>,
) -> io::Result<fs::File> {
    let sid = segment_id.to_string();

    let cache_delta = base_dir.join("cache").join(format!("{sid}.delta"));
    if cache_delta.exists() {
        return fs::File::open(&cache_delta);
    }
    for layer in ancestor_layers.iter().rev() {
        let ancestor_delta = layer.dir.join("cache").join(format!("{sid}.delta"));
        if ancestor_delta.exists() {
            return fs::File::open(&ancestor_delta);
        }
    }
    if let Some(fetcher) = fetcher {
        let index_dir = base_dir.join("index");
        let body_dir = base_dir.join("cache");
        fetcher.fetch_delta_body(segment_id, &index_dir, &body_dir)?;
        return fs::File::open(&cache_delta);
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("delta body not found: {sid}"),
    ))
}

/// Gate a `cache/<id>.body` hit on the `.present` bit for `Cached` entries.
/// Returns true for any non-cache layout (wal/pending/gc) and for cache hits
/// on `Local` entries. For `Cached` cache hits, checks the corresponding
/// `cache/<id>.present` bit alongside the `.body` file in `dir`.
fn cache_hit_allowed(
    layout: segment::SegmentBodyLayout,
    dir: &Path,
    sid: &str,
    body_source: BodySource,
) -> bool {
    if layout != segment::SegmentBodyLayout::BodyOnly {
        return true;
    }
    match body_source {
        BodySource::Local => true,
        BodySource::Cached(idx) => {
            let present_path = dir.join("cache").join(format!("{sid}.present"));
            segment::check_present_bit(&present_path, idx).unwrap_or(false)
        }
    }
}

/// Search for a segment file across the fork directory tree.
///
/// Search order:
///   1. Current fork: `wal/`, `pending/`, bare `gc/<id>`, `cache/<id>.body`
///   2. Ancestor forks (newest-first): `pending/`, bare `gc/<id>`, `cache/<id>.body`
///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
///
/// For `Cached` entries, a `cache/<id>.body` hit is only accepted if the
/// corresponding bit in `cache/<id>.present` is set — otherwise the entry
/// is not yet locally available and we fall through to the fetcher.
///
/// `.idx` files live in `index/` (coordinator-written, permanent).
/// `.body` and `.present` files live in `cache/` (volume-managed read cache).
///
/// Extracted from `Volume::find_segment_file` so that `VolumeReader` can serve
/// reads directly from a `ReadSnapshot` without going through the actor channel.
pub(crate) fn find_segment_in_dirs(
    segment_id: Ulid,
    base_dir: &Path,
    ancestor_layers: &[AncestorLayer],
    fetcher: Option<&BoxFetcher>,
    body_section_start: u64,
    body_source: BodySource,
) -> io::Result<PathBuf> {
    let sid = segment_id.to_string();
    // Self dir: full canonical precedence (wal → pending → bare gc/<id> → cache).
    // The bare-`gc/<id>` branch matters here because the extent index flips to
    // the new segment_id the moment the volume renames `<id>.tmp → <id>` (the
    // commit point of apply), before the coordinator has promoted the body to
    // `cache/`.
    if let Some((path, layout)) = segment::locate_segment_body(base_dir, segment_id)
        && cache_hit_allowed(layout, base_dir, &sid, body_source)
    {
        return Ok(path);
    }
    // Ancestor layers: segments here are always fork-parent state. They cannot
    // be mid-GC-handoff from this child's perspective, and they have no live
    // wal/, but pending/ and cache/<id>.body can both appear — the same helper
    // yields the right path; we just re-gate cache hits on the layer's own
    // `.present` file.
    for layer in ancestor_layers.iter().rev() {
        if let Some((path, layout)) = segment::locate_segment_body(&layer.dir, segment_id)
            && cache_hit_allowed(layout, &layer.dir, &sid, body_source)
        {
            return Ok(path);
        }
    }
    if let (Some(fetcher), BodySource::Cached(idx)) = (fetcher, body_source) {
        // The segment's `.idx` file lives in the index directory of whichever
        // volume wrote it — self for locally-written segments, an ancestor
        // for fork-parent segments. Search self first, then the ancestor
        // chain (in the same order rebuild_segments merges), and use that
        // volume's dirs so the fetched body lands in the owner's `cache/`
        // (where subsequent reads will find it via the ancestor scan above).
        let idx_filename = format!("{sid}.idx");
        let owner_dir = std::iter::once(base_dir)
            .chain(ancestor_layers.iter().map(|l| l.dir.as_path()))
            .find(|dir| dir.join("index").join(&idx_filename).exists())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "segment index not found in self or ancestors: {sid}.idx \
                         (ancestor chain may not be prefetched yet)"
                    ),
                )
            })?;
        let index_dir = owner_dir.join("index");
        let body_dir = owner_dir.join("cache");
        fetcher.fetch_extent(
            segment_id,
            &index_dir,
            &body_dir,
            &segment::ExtentFetch {
                body_section_start,
                body_offset: 0,
                body_length: 0,
                entry_idx: idx,
            },
        )?;
        return Ok(body_dir.join(format!("{sid}.body")));
    }
    Err(io::Error::other(format!("segment not found: {sid}")))
}
