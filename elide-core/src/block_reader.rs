// Read-only block-level view of a volume, backed by its segments + WAL.
//
// `BlockReader` rebuilds an `LbaMap` and `ExtentIndex` from a volume's on-disk
// state, then serves 4 KiB reads through them — demand-fetching segment bodies
// from the object store on cache miss. It implements `ext4_view::Ext4Read` so
// the ext4 filesystem inside the volume can be parsed without mounting.
//
// Two modes:
// - `open_live`: follows the current fork + WAL tail. Used by `elide volume ls`
//   to inspect a possibly-running volume.
// - `open_snapshot`: pinned to a signed `snapshots/<ulid>.manifest`. Used by
//   Phase 4 filemap generation to parse the ext4 filesystem exactly as it
//   existed at snapshot time, with no WAL replay and no `pending/` state.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ext4_view::Ext4Read;

use crate::extentindex::{BodySource, DeltaBodySource, DeltaLocation, ExtentLocation};
use crate::segment::{EntryKind, SegmentFetcher};
use crate::signing::VerifyingKey;
use crate::{extentindex, lbamap, segment, signing, volume, writelog};

/// Factory for constructing a demand-fetcher once the ancestor chain is known.
///
/// The factory receives the reader's search directories in **newest-first**
/// order (fork first, ancestors after). Callers that need oldest-first (e.g.
/// `RemoteFetcher`) must reverse locally.
pub type FetcherFactory<'a> = dyn FnOnce(&[PathBuf]) -> Option<Box<dyn SegmentFetcher>> + 'a;

/// Block-level reader over a volume's merged fork + ancestor state.
pub struct BlockReader {
    /// Search path for segment files: fork dir first, then ancestors.
    search_dirs: Vec<PathBuf>,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    /// Demand-fetcher for evicted segment bodies. `None` for local-only volumes
    /// or when no object store is configured.
    fetcher: Option<Box<dyn SegmentFetcher>>,
}

impl BlockReader {
    /// Open a live view of `dir`: ancestors walked via `volume::walk_ancestors`,
    /// WAL tail replayed on top so in-flight writes are visible.
    ///
    /// `mk_fetcher` is called once the search-dir list is known; it should
    /// return `None` when no object store is configured. Returning `None` is
    /// fine for fully-local volumes — reads that miss local state will error.
    pub fn open_live(dir: &Path, mk_fetcher: Box<FetcherFactory<'_>>) -> io::Result<Self> {
        // Canonicalize so by_name/<name> symlinks resolve to by_id/<ulid>,
        // making dir.parent() the correct by_id/ directory for ancestor lookup.
        let dir = fs::canonicalize(dir).unwrap_or_else(|_| dir.to_owned());
        let by_id_dir = dir.parent().unwrap_or(&dir);
        let ancestor_layers = volume::walk_ancestors(&dir, by_id_dir)?;

        let rebuild_chain: Vec<(PathBuf, Option<String>)> = ancestor_layers
            .iter()
            .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
            .chain(std::iter::once((dir.clone(), None)))
            .collect();

        let mut search_dirs: Vec<PathBuf> = std::iter::once(dir.clone())
            .chain(ancestor_layers.into_iter().map(|l| l.dir))
            .collect();
        search_dirs.dedup();

        let mut lbamap = lbamap::rebuild_segments(&rebuild_chain)?;
        let mut extent_index = extentindex::rebuild(&rebuild_chain)?;

        // Replay WAL records on top. Use scan_readonly so we don't truncate
        // partial tails that may exist on a currently-running volume.
        for path in segment::collect_segment_files(&dir.join("wal"))? {
            let ulid = path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| ulid::Ulid::from_string(s).ok())
                .ok_or_else(|| io::Error::other("bad WAL filename"))?;

            let (records, _partial_tail) = writelog::scan_readonly(&path)?;
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
                        lbamap.insert(start_lba, lba_length, hash);
                        extent_index.insert(
                            hash,
                            extentindex::ExtentLocation {
                                segment_id: ulid,
                                body_offset,
                                body_length: data.len() as u32,
                                compressed: flags.contains(writelog::WalFlags::COMPRESSED),
                                body_source: extentindex::BodySource::Local,
                                body_section_start: 0,
                                inline_data: None,
                            },
                        );
                    }
                    writelog::LogRecord::Ref {
                        hash,
                        start_lba,
                        lba_length,
                    } => {
                        lbamap.insert(start_lba, lba_length, hash);
                    }
                    writelog::LogRecord::Zero {
                        start_lba,
                        lba_length,
                    } => {
                        lbamap.insert(start_lba, lba_length, crate::volume::ZERO_HASH);
                    }
                }
            }
        }

        let fetcher = mk_fetcher(&search_dirs);

        Ok(Self {
            search_dirs,
            lbamap,
            extent_index,
            fetcher,
        })
    }

    /// Open a snapshot-pinned view of `dir` at `snap_ulid`. The view includes
    /// exactly the segments listed in `dir`'s signed `snapshots/<snap_ulid>.manifest`
    /// plus every ancestor's snapshot manifest reached via the `parent_pubkey`
    /// chain in the provenance. No WAL replay, no `pending/`, no bare `gc/`.
    ///
    /// This is the correct view for tools that need to parse the volume at a
    /// specific sealed snapshot — notably Phase 4 filemap generation, which
    /// must see the ext4 filesystem exactly as it existed at snapshot time.
    pub fn open_snapshot(
        dir: &Path,
        snap_ulid: &ulid::Ulid,
        mk_fetcher: Box<FetcherFactory<'_>>,
    ) -> io::Result<Self> {
        let dir = fs::canonicalize(dir).unwrap_or_else(|_| dir.to_owned());
        let by_id_dir = dir.parent().unwrap_or(&dir);

        // Trust root: this volume's own volume.pub. Callers are expected to
        // have validated that they asked for this identity.
        let own_pubkey = signing::load_verifying_key(&dir, signing::VOLUME_PUB_FILE)?;
        let own_segs = signing::read_snapshot_manifest(&dir, &own_pubkey, snap_ulid)?.segment_ulids;
        let own_lineage =
            signing::read_lineage_with_key(&dir, &own_pubkey, signing::VOLUME_PROVENANCE_FILE)?;

        // Walk the parent chain, accumulating (dir, segs, vk) tuples.
        // Parent's provenance is verified under the parent_pubkey the child
        // signed over — not the parent's own volume.pub on disk.
        let mut parents: Vec<SnapshotLayer> = Vec::new();
        let mut cursor = own_lineage.parent;
        while let Some(parent) = cursor {
            let parent_dir = volume::resolve_ancestor_dir(by_id_dir, &parent.volume_ulid);
            if !parent_dir.exists() {
                return Err(io::Error::other(format!(
                    "ancestor {} not found locally",
                    parent.volume_ulid
                )));
            }
            let parent_vk = VerifyingKey::from_bytes(&parent.pubkey).map_err(|e| {
                io::Error::other(format!(
                    "invalid parent pubkey in provenance for {}: {e}",
                    parent.volume_ulid
                ))
            })?;
            // Forker-attested pins carry a separate manifest key; fall back
            // to the identity key when absent.
            let manifest_vk = match parent.manifest_pubkey {
                Some(bytes) => VerifyingKey::from_bytes(&bytes).map_err(|e| {
                    io::Error::other(format!(
                        "invalid parent manifest pubkey in provenance for {}: {e}",
                        parent.volume_ulid
                    ))
                })?,
                None => parent_vk,
            };
            let parent_snap = ulid::Ulid::from_string(&parent.snapshot_ulid).map_err(|e| {
                io::Error::other(format!(
                    "invalid snapshot ulid in provenance parent {}: {e}",
                    parent.volume_ulid
                ))
            })?;
            let segs = signing::read_snapshot_manifest(&parent_dir, &manifest_vk, &parent_snap)?
                .segment_ulids;
            let parent_lineage = signing::read_lineage_with_key(
                &parent_dir,
                &parent_vk,
                signing::VOLUME_PROVENANCE_FILE,
            )?;
            parents.push(SnapshotLayer {
                dir: parent_dir,
                segs,
                vk: parent_vk,
            });
            cursor = parent_lineage.parent;
        }

        // Apply oldest-first: root ancestor, ..., immediate parent, own fork.
        parents.reverse();
        let mut layers = parents;
        layers.push(SnapshotLayer {
            dir: dir.clone(),
            segs: own_segs,
            vk: own_pubkey,
        });

        let mut lbamap = lbamap::LbaMap::new();
        let mut extent_index = extentindex::ExtentIndex::new();
        for layer in &layers {
            apply_snapshot_layer(&mut lbamap, &mut extent_index, layer)?;
        }

        // search_dirs in newest-first order (fork first, ancestors after).
        let mut search_dirs: Vec<PathBuf> = layers.into_iter().rev().map(|l| l.dir).collect();
        search_dirs.dedup();

        let fetcher = mk_fetcher(&search_dirs);

        Ok(Self {
            search_dirs,
            lbamap,
            extent_index,
            fetcher,
        })
    }

    /// Look up the content hash at `lba` via the LBA map, without reading any
    /// body bytes. Returns `None` for unwritten LBAs.
    ///
    /// Phase 4 filemap generation uses this to record each file fragment's
    /// hash directly from the extent the import path already wrote, so the
    /// filemap row matches the segment entry without rehashing.
    pub fn hash_for_lba(&self, lba: u64) -> Option<blake3::Hash> {
        self.lbamap.lookup(lba).map(|(h, _)| h)
    }

    /// Read the 4 KiB block at `lba`.
    ///
    /// Dispatches on the lbamap winner for `lba`:
    /// - unmapped → 4 KiB of zeros (LBA never written)
    /// - `ZERO_HASH` → 4 KiB of zeros (LBA explicitly zeroed)
    /// - hash in the data index → read the data/inline extent
    /// - hash in the delta index → reconstruct via zstd-dict against a source
    ///   extent
    /// - hash in neither → hard error. This used to silently return zeros,
    ///   which masked the missing Delta path and any future index/lbamap
    ///   inconsistency. Loud errors are required so corruption surfaces
    ///   instead of being read as a hole.
    pub fn read_block(&self, lba: u64) -> io::Result<[u8; 4096]> {
        let Some((hash, block_offset)) = self.lbamap.lookup(lba) else {
            return Ok([0u8; 4096]);
        };
        if hash == volume::ZERO_HASH {
            return Ok([0u8; 4096]);
        }
        if let Some(loc) = self.extent_index.lookup(&hash) {
            return self.read_data_block(&loc.clone(), block_offset);
        }
        if let Some(delta_loc) = self.extent_index.lookup_delta(&hash) {
            return self.read_delta_block(&delta_loc.clone(), block_offset);
        }
        Err(io::Error::other(format!(
            "lba {lba}: hash {} present in lbamap but not in extent index (data, inline, or delta) — possible corruption",
            hash.to_hex()
        )))
    }

    /// Read a 4 KiB block from a Data or Inline extent location.
    fn read_data_block(&self, loc: &ExtentLocation, block_offset: u32) -> io::Result<[u8; 4096]> {
        if let Some(ref idata) = loc.inline_data {
            let raw = if loc.compressed {
                lz4_flex::decompress_size_prepended(idata).map_err(io::Error::other)?
            } else {
                idata.to_vec()
            };
            let src = block_offset as usize * 4096;
            let mut block = [0u8; 4096];
            block.copy_from_slice(&raw[src..src + 4096]);
            return Ok(block);
        }

        self.ensure_extent_present(loc)?;

        let (path, layout) = find_segment_file(&self.search_dirs, loc.segment_id)?;
        let seek = layout.body_seek(loc);
        let mut f = fs::File::open(path)?;
        let mut block = [0u8; 4096];
        if loc.compressed {
            f.seek(SeekFrom::Start(seek))?;
            let mut buf = vec![0u8; loc.body_length as usize];
            f.read_exact(&mut buf)?;
            let decompressed =
                lz4_flex::decompress_size_prepended(&buf).map_err(io::Error::other)?;
            let src = block_offset as usize * 4096;
            block.copy_from_slice(&decompressed[src..src + 4096]);
        } else {
            f.seek(SeekFrom::Start(seek + block_offset as u64 * 4096))?;
            f.read_exact(&mut block)?;
        }
        Ok(block)
    }

    /// Read a 4 KiB block from a Delta extent.
    ///
    /// Picks the first `delta_loc.options` whose `source_hash` resolves via
    /// the data index, reads the source extent body, fetches the delta blob
    /// (from the segment file or `cache/<id>.delta`, demand-fetching the
    /// latter on miss), zstd-decompresses against the source as dictionary,
    /// and slices out the requested 4 KiB.
    fn read_delta_block(
        &self,
        delta_loc: &DeltaLocation,
        block_offset: u32,
    ) -> io::Result<[u8; 4096]> {
        let mut picked = None;
        for opt in &delta_loc.options {
            if let Some(source_loc) = self.extent_index.lookup(&opt.source_hash) {
                picked = Some((opt.clone(), source_loc.clone()));
                break;
            }
        }
        let (opt, source_loc) = picked.ok_or_else(|| {
            io::Error::other(format!(
                "delta extent in segment {}: no source option resolved in extent index",
                delta_loc.segment_id
            ))
        })?;

        let source_bytes = self.read_extent_body_at(&source_loc)?;
        let delta_blob = self.read_delta_blob(
            delta_loc.segment_id,
            delta_loc.body_source,
            opt.delta_offset,
            opt.delta_length,
        )?;

        let decompressed = crate::delta_compute::apply_delta(&source_bytes, &delta_blob)?;

        let src = block_offset as usize * 4096;
        let src_end = src + 4096;
        if decompressed.len() < src_end {
            return Err(io::Error::other(format!(
                "delta decompressed payload too short: got {} bytes, need {}",
                decompressed.len(),
                src_end
            )));
        }
        let mut block = [0u8; 4096];
        block.copy_from_slice(&decompressed[src..src_end]);
        Ok(block)
    }

    /// Read the delta blob bytes for a single delta option.
    ///
    /// `Full` blobs live in a complete segment file (in `pending/` or `wal/`)
    /// at `body_section_start + body_length + delta_offset`. `Cached` blobs
    /// live in `cache/<segment_id>.delta` starting at byte 0; on miss they
    /// are demand-fetched via the attached fetcher.
    fn read_delta_blob(
        &self,
        segment_id: ulid::Ulid,
        body_source: DeltaBodySource,
        delta_offset: u64,
        delta_length: u32,
    ) -> io::Result<Vec<u8>> {
        let (path, base) = match body_source {
            DeltaBodySource::Full {
                body_section_start,
                body_length,
            } => {
                let (path, layout) = find_segment_file(&self.search_dirs, segment_id)?;
                // Delta body sits right after the body section.
                let base = layout.body_section_file_offset(body_section_start) + body_length;
                (path, base)
            }
            DeltaBodySource::Cached => (self.find_delta_body(segment_id)?, 0u64),
        };
        let mut f = fs::File::open(path)?;
        f.seek(SeekFrom::Start(base + delta_offset))?;
        let mut buf = vec![0u8; delta_length as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Locate `cache/<segment_id>.delta` across the fork's search dirs,
    /// demand-fetching into the owner's cache dir if not present
    /// locally. The owner is the search dir whose `index/` holds this
    /// segment's `.idx`; the fetched body lands alongside it.
    fn find_delta_body(&self, segment_id: ulid::Ulid) -> io::Result<PathBuf> {
        let sid = segment_id.to_string();
        for dir in &self.search_dirs {
            let p = dir.join("cache").join(format!("{sid}.delta"));
            if p.exists() {
                return Ok(p);
            }
        }
        if let Some(fetcher) = &self.fetcher {
            let (owner_vol_id, index_dir, body_dir) = self
                .find_owner_dirs_for_segment(segment_id)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("segment index not found in any search dir: {sid}.idx"),
                    )
                })?;
            fetcher.fetch_delta_body(segment_id, owner_vol_id, &index_dir, &body_dir)?;
            let p = body_dir.join(format!("{sid}.delta"));
            if p.exists() {
                return Ok(p);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("delta body not found locally and no fetcher could supply it: {sid}"),
        ))
    }

    /// Demand-fetch the body for `loc` if it's a `Cached` extent missing its
    /// `.present` bit. No-op for `Local` extents.
    fn ensure_extent_present(&self, loc: &ExtentLocation) -> io::Result<()> {
        let extentindex::BodySource::Cached(entry_idx) = loc.body_source else {
            return Ok(());
        };
        let (owner_vol_id, index_dir, body_dir) = self
            .find_owner_dirs_for_segment(loc.segment_id)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "segment index not found in any search dir: {}.idx",
                        loc.segment_id
                    ),
                )
            })?;
        let present_path = body_dir.join(format!("{}.present", loc.segment_id));
        if segment::check_present_bit(&present_path, entry_idx)? {
            return Ok(());
        }
        match &self.fetcher {
            Some(fetcher) => fetcher.fetch_extent(
                loc.segment_id,
                owner_vol_id,
                &index_dir,
                &body_dir,
                &segment::ExtentFetch {
                    body_section_start: loc.body_section_start,
                    body_offset: loc.body_offset,
                    body_length: loc.body_length,
                    entry_idx,
                },
            ),
            None => Err(io::Error::other(format!(
                "extent {}[{}] not cached and no fetcher configured",
                loc.segment_id, entry_idx
            ))),
        }
    }

    /// Read the full plaintext body of an extent at `loc`. Used by the delta
    /// path to materialise a source extent for zstd-dict decompression.
    fn read_extent_body_at(&self, loc: &ExtentLocation) -> io::Result<Vec<u8>> {
        if let Some(ref idata) = loc.inline_data {
            return if loc.compressed {
                lz4_flex::decompress_size_prepended(idata).map_err(io::Error::other)
            } else {
                Ok(idata.to_vec())
            };
        }
        self.ensure_extent_present(loc)?;
        let (path, layout) = find_segment_file(&self.search_dirs, loc.segment_id)?;
        let mut f = fs::File::open(path)?;
        f.seek(SeekFrom::Start(layout.body_seek(loc)))?;
        let mut buf = vec![0u8; loc.body_length as usize];
        f.read_exact(&mut buf)?;
        if loc.compressed {
            lz4_flex::decompress_size_prepended(&buf).map_err(io::Error::other)
        } else {
            Ok(buf)
        }
    }

    /// Read the full plaintext bytes of the extent with content hash `hash`.
    ///
    /// Returns the decompressed body of the whole extent — not a 4 KiB slice.
    /// Fails if `hash` is not known to this reader's extent index, or if the
    /// underlying body is evicted and no fetcher is configured.
    ///
    /// Delta repack uses this to obtain the dictionary source for a prior
    /// snapshot's extent when building Tier 1 deltas.
    pub fn read_extent_body(&self, hash: &blake3::Hash) -> io::Result<Vec<u8>> {
        let loc = self
            .extent_index
            .lookup(hash)
            .ok_or_else(|| io::Error::other(format!("extent {hash} not in index")))?
            .clone();
        self.read_extent_body_at(&loc)
    }

    /// Find the (owner_vol_id, index_dir, cache_dir) for a segment by
    /// scanning `search_dirs` for the one whose `index/<seg>.idx`
    /// exists. Returns `None` if no fork in this layer chain has the
    /// `.idx` (caller decides whether that's fatal).
    fn find_owner_dirs_for_segment(
        &self,
        segment_id: ulid::Ulid,
    ) -> Option<(ulid::Ulid, PathBuf, PathBuf)> {
        let sid = segment_id.to_string();
        for dir in &self.search_dirs {
            let idx = dir.join("index").join(format!("{sid}.idx"));
            if idx.exists() {
                let owner_vol_id = dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| ulid::Ulid::from_string(s).ok())?;
                return Some((owner_vol_id, dir.join("index"), dir.join("cache")));
            }
        }
        None
    }
}

impl Ext4Read for BlockReader {
    fn read(
        &mut self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut written = 0;
        while written < dst.len() {
            let byte_pos = start_byte + written as u64;
            let lba = byte_pos / 4096;
            let offset_in_block = (byte_pos % 4096) as usize;
            let bytes_from_block = (4096 - offset_in_block).min(dst.len() - written);
            let block = self.read_block(lba).map_err(Box::new)?;
            dst[written..written + bytes_from_block]
                .copy_from_slice(&block[offset_in_block..offset_in_block + bytes_from_block]);
            written += bytes_from_block;
        }
        Ok(())
    }
}

/// One layer in a snapshot-pinned rebuild: the layer's on-disk directory, the
/// sorted segment ULIDs listed in that layer's snapshot manifest, and the
/// verifying key under which each segment's `.idx` file is authenticated.
struct SnapshotLayer {
    dir: PathBuf,
    segs: Vec<ulid::Ulid>,
    vk: VerifyingKey,
}

/// Apply one snapshot layer to an in-progress `(LbaMap, ExtentIndex)` pair.
///
/// Mirrors the `index/*.idx` branch of `extentindex::rebuild` (the post-
/// eviction two-file format), restricted to the exact segment set listed in
/// `layer.segs`. Snapshot mode never sees `pending/` or bare `gc/` — those
/// are in-flight states that only exist on live forks.
fn apply_snapshot_layer(
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
    layer: &SnapshotLayer,
) -> io::Result<()> {
    let index_dir = layer.dir.join("index");
    for seg in &layer.segs {
        let idx_path = index_dir.join(format!("{seg}.idx"));
        let body_section_start = segment::idx_body_section_start(&idx_path)?;
        let layout = segment::read_segment_layout(&idx_path)?;
        let _body_length = layout.body_length;

        let (_bss, entries, _inputs) =
            segment::read_and_verify_segment_index(&idx_path, &layer.vk)?;

        let has_inline = entries
            .iter()
            .any(|e| matches!(e.kind, EntryKind::Inline | EntryKind::CanonicalInline));
        let inline_bytes = if has_inline {
            segment::read_inline_section(&idx_path)?
        } else {
            Vec::new()
        };

        for (raw_idx, entry) in entries.iter().enumerate() {
            // LBA map: every non-canonical entry contributes its range →
            // content hash. Canonical entries carry body for dedup resolution
            // only and make no LBA claim.
            if !entry.kind.is_canonical_only() {
                if entry.kind == EntryKind::Delta {
                    let sources: std::sync::Arc<[blake3::Hash]> =
                        entry.delta_options.iter().map(|o| o.source_hash).collect();
                    lbamap.insert_delta(entry.start_lba, entry.lba_length, entry.hash, sources);
                } else {
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                }
            }

            match entry.kind {
                EntryKind::Data
                | EntryKind::Inline
                | EntryKind::CanonicalData
                | EntryKind::CanonicalInline => {}
                EntryKind::DedupRef | EntryKind::Zero => continue,
                EntryKind::Delta => {
                    extent_index.insert_delta_if_absent(
                        entry.hash,
                        DeltaLocation {
                            segment_id: *seg,
                            body_source: DeltaBodySource::Cached,
                            options: entry.delta_options.clone(),
                        },
                    );
                    continue;
                }
            }

            let idata = if entry.kind.is_inline() {
                let start = entry.stored_offset as usize;
                let end = start + entry.stored_length as usize;
                if end <= inline_bytes.len() {
                    Some(inline_bytes[start..end].into())
                } else {
                    continue;
                }
            } else {
                None
            };

            extent_index.insert_if_absent(
                entry.hash,
                ExtentLocation {
                    segment_id: *seg,
                    body_offset: entry.stored_offset,
                    body_length: entry.stored_length,
                    compressed: entry.compressed,
                    body_source: BodySource::Cached(raw_idx as u32),
                    body_section_start,
                    inline_data: idata,
                },
            );
        }
    }
    Ok(())
}

fn find_segment_file(
    search_dirs: &[PathBuf],
    segment_id: ulid::Ulid,
) -> io::Result<(PathBuf, crate::segment::SegmentBodyLayout)> {
    for dir in search_dirs {
        if let Some(hit) = crate::segment::locate_segment_body(dir, segment_id) {
            return Ok(hit);
        }
    }
    Err(io::Error::other(format!(
        "segment not found: {}",
        segment_id
    )))
}
