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
/// `ObjectStoreFetcher`) must reverse locally.
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
    /// Directory where coordinator-written `.idx` files live (`<fork>/index/`).
    primary_index_dir: PathBuf,
    /// Directory where demand-fetched `.body` files are written (`<fork>/cache/`).
    primary_cache_dir: PathBuf,
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

        let primary_index_dir = dir.join("index");
        let primary_cache_dir = dir.join("cache");
        let fetcher = mk_fetcher(&search_dirs);

        Ok(Self {
            search_dirs,
            lbamap,
            extent_index,
            fetcher,
            primary_index_dir,
            primary_cache_dir,
        })
    }

    /// Open a snapshot-pinned view of `dir` at `snap_ulid`. The view includes
    /// exactly the segments listed in `dir`'s signed `snapshots/<snap_ulid>.manifest`
    /// plus every ancestor's snapshot manifest reached via the `parent_pubkey`
    /// chain in the provenance. No WAL replay, no `pending/`, no `gc/.applied`.
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
        let own_segs = signing::read_snapshot_manifest(&dir, &own_pubkey, snap_ulid)?;
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
            let parent_snap = ulid::Ulid::from_string(&parent.snapshot_ulid).map_err(|e| {
                io::Error::other(format!(
                    "invalid snapshot ulid in provenance parent {}: {e}",
                    parent.volume_ulid
                ))
            })?;
            let segs = signing::read_snapshot_manifest(&parent_dir, &parent_vk, &parent_snap)?;
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

        let primary_index_dir = dir.join("index");
        let primary_cache_dir = dir.join("cache");
        let fetcher = mk_fetcher(&search_dirs);

        Ok(Self {
            search_dirs,
            lbamap,
            extent_index,
            fetcher,
            primary_index_dir,
            primary_cache_dir,
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

    /// Read the 4 KiB block at `lba`. Returns zeros for unwritten LBAs.
    pub fn read_block(&self, lba: u64) -> io::Result<[u8; 4096]> {
        let Some((hash, block_offset)) = self.lbamap.lookup(lba) else {
            return Ok([0u8; 4096]);
        };
        let Some(loc) = self.extent_index.lookup(&hash) else {
            return Ok([0u8; 4096]);
        };
        let loc = loc.clone();

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

        if let extentindex::BodySource::Cached(entry_idx) = loc.body_source {
            let (index_dir, body_dir) =
                self.find_dirs_for_segment(loc.segment_id)
                    .unwrap_or_else(|| {
                        (
                            self.primary_index_dir.clone(),
                            self.primary_cache_dir.clone(),
                        )
                    });
            let present_path = body_dir.join(format!("{}.present", loc.segment_id));
            if !segment::check_present_bit(&present_path, entry_idx)? {
                match &self.fetcher {
                    Some(fetcher) => fetcher.fetch_extent(
                        loc.segment_id,
                        &index_dir,
                        &body_dir,
                        &segment::ExtentFetch {
                            body_section_start: loc.body_section_start,
                            body_offset: loc.body_offset,
                            body_length: loc.body_length,
                            entry_idx,
                        },
                    )?,
                    None => {
                        return Err(io::Error::other(format!(
                            "extent {}[{}] not cached and no fetcher configured",
                            loc.segment_id, entry_idx
                        )));
                    }
                }
            }
        }

        let path = find_segment_file(&self.search_dirs, loc.segment_id)?;
        let is_body = path.extension().is_some_and(|e| e == "body");
        let file_base = if is_body { 0 } else { loc.body_section_start };
        let mut f = fs::File::open(path)?;
        let mut block = [0u8; 4096];
        if loc.compressed {
            f.seek(SeekFrom::Start(file_base + loc.body_offset))?;
            let mut buf = vec![0u8; loc.body_length as usize];
            f.read_exact(&mut buf)?;
            let decompressed =
                lz4_flex::decompress_size_prepended(&buf).map_err(io::Error::other)?;
            let src = block_offset as usize * 4096;
            block.copy_from_slice(&decompressed[src..src + 4096]);
        } else {
            f.seek(SeekFrom::Start(
                file_base + loc.body_offset + block_offset as u64 * 4096,
            ))?;
            f.read_exact(&mut block)?;
        }
        Ok(block)
    }

    fn find_dirs_for_segment(&self, segment_id: ulid::Ulid) -> Option<(PathBuf, PathBuf)> {
        let sid = segment_id.to_string();
        for dir in &self.search_dirs {
            let idx = dir.join("index").join(format!("{sid}.idx"));
            if idx.exists() {
                return Some((dir.join("index"), dir.join("cache")));
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
/// `layer.segs`. Snapshot mode never sees `pending/` or `gc/.applied` — those
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

        let (_bss, entries) = segment::read_and_verify_segment_index(&idx_path, &layer.vk)?;

        let has_inline = entries.iter().any(|e| e.kind == EntryKind::Inline);
        let inline_bytes = if has_inline {
            segment::read_inline_section(&idx_path)?
        } else {
            Vec::new()
        };

        for (raw_idx, entry) in entries.iter().enumerate() {
            // LBA map: every entry contributes its range → content hash.
            lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
            if entry.kind == EntryKind::Delta {
                for opt in &entry.delta_options {
                    lbamap.register_delta_source(opt.source_hash);
                }
            }

            match entry.kind {
                EntryKind::Data | EntryKind::Inline => {}
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

            let idata = if entry.kind == EntryKind::Inline {
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

fn find_segment_file(search_dirs: &[PathBuf], segment_id: ulid::Ulid) -> io::Result<PathBuf> {
    let sid = segment_id.to_string();
    for dir in search_dirs {
        for subdir in ["wal", "pending"] {
            let path = dir.join(subdir).join(&sid);
            if path.exists() {
                return Ok(path);
            }
        }
        let body = dir.join("cache").join(format!("{sid}.body"));
        if body.exists() {
            return Ok(body);
        }
    }
    Err(io::Error::other(format!("segment not found: {sid}")))
}
