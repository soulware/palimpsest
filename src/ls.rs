// Read-only ext4 view of a volume directory.
//
// Builds the LBA map and extent index from the volume's segments and WAL,
// then presents the virtual disk as an Ext4Read implementor so that
// ext4_view can parse the filesystem without mounting it.
//
// Reads the given node and all ancestor nodes in the snapshot chain.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ext4_view::{Ext4, Ext4Read, FileType, PathBuf as Ext4PathBuf};

use elide_core::segment::SegmentFetcher;
use elide_core::{extentindex, lbamap, segment, volume, writelog};

use elide_fetch::{FetchConfig, ObjectStoreFetcher};

pub fn run(dir: &Path, fs_path: &str) -> io::Result<()> {
    let reader = VolumeReader::open(dir)?;
    let fs =
        Ext4::load(Box::new(reader)).map_err(|e| io::Error::other(format!("ext4 load: {e}")))?;

    let path = Ext4PathBuf::new(fs_path);
    let meta = fs
        .metadata(&path)
        .map_err(|e| io::Error::other(format!("{fs_path}: {e}")))?;

    if meta.is_dir() {
        list_dir(&fs, fs_path, &path)?;
    } else {
        let tc = fmt_type_char(meta.file_type());
        let mode = fmt_mode(meta.mode());
        let suffix = entry_suffix(meta.file_type());
        println!("{tc}{mode}  {:>10}  {fs_path}{suffix}", meta.len());
    }

    Ok(())
}

fn list_dir(fs: &Ext4, header: &str, path: &Ext4PathBuf) -> io::Result<()> {
    let mut entries: Vec<(String, ext4_view::Metadata)> = fs
        .read_dir(path)
        .map_err(|e| io::Error::other(format!("read_dir: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name() != "." && e.file_name() != "..")
        .filter_map(|e| {
            let name = e.file_name().display().to_string();
            e.metadata().ok().map(|m| (name, m))
        })
        .collect();

    entries.sort_by(|a, b| a.0.cmp(&b.0));

    println!("{header}");
    for (name, meta) in &entries {
        let tc = fmt_type_char(meta.file_type());
        let mode = fmt_mode(meta.mode());
        let suffix = entry_suffix(meta.file_type());
        println!("{tc}{mode}  {:>10}  {name}{suffix}", meta.len());
    }

    Ok(())
}

fn fmt_type_char(ft: FileType) -> char {
    match ft {
        FileType::Regular => '-',
        FileType::Directory => 'd',
        FileType::Symlink => 'l',
        FileType::CharacterDevice => 'c',
        FileType::BlockDevice => 'b',
        FileType::Fifo => 'p',
        FileType::Socket => 's',
    }
}

fn fmt_mode(mode: u16) -> String {
    [
        (0o400, 'r'),
        (0o200, 'w'),
        (0o100, 'x'),
        (0o040, 'r'),
        (0o020, 'w'),
        (0o010, 'x'),
        (0o004, 'r'),
        (0o002, 'w'),
        (0o001, 'x'),
    ]
    .iter()
    .map(|&(bit, c)| if mode & bit != 0 { c } else { '-' })
    .collect()
}

fn entry_suffix(ft: FileType) -> &'static str {
    match ft {
        FileType::Directory => "/",
        FileType::Symlink => "@",
        _ => "",
    }
}

// --- VolumeReader ---

struct VolumeReader {
    /// Search path for segment files: fork dir first, then ancestors (oldest last).
    search_dirs: Vec<PathBuf>,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    /// Demand-fetcher: downloads a segment body from the object store on a miss.
    /// `None` if no store is configured (local-only volumes always have their bodies).
    fetcher: Option<Box<dyn SegmentFetcher>>,
    /// Directory where coordinator-written `.idx` files live (`<fork>/index/`).
    primary_index_dir: PathBuf,
    /// Directory where demand-fetched `.body` files are written (`<fork>/cache/`).
    primary_cache_dir: PathBuf,
}

impl VolumeReader {
    fn open(dir: &Path) -> io::Result<Self> {
        // Canonicalize so that by_name/<name> symlinks resolve to by_id/<ulid>,
        // making dir.parent() the correct by_id/ directory for ancestor lookup.
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_owned());
        let by_id_dir = dir.parent().unwrap_or(&dir);
        let ancestor_layers = volume::walk_ancestors(&dir, by_id_dir)?;
        let rebuild_chain: Vec<(std::path::PathBuf, Option<String>)> = ancestor_layers
            .iter()
            .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
            .chain(std::iter::once((dir.to_owned(), None)))
            .collect();
        // Collect all directories to search for segment files (fork first, then ancestors).
        let mut search_dirs: Vec<PathBuf> = std::iter::once(dir.to_owned())
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
                        lbamap.insert(start_lba, lba_length, elide_core::volume::ZERO_HASH);
                    }
                }
            }
        }

        // Try to configure demand-fetch from the object store.
        // search_dirs is newest-first; ancestry_chain expects oldest-first.
        let primary_index_dir = dir.join("index");
        let primary_cache_dir = dir.join("cache");
        let data_dir = by_id_dir.parent().unwrap_or(by_id_dir);
        let fetcher: Option<Box<dyn SegmentFetcher>> =
            FetchConfig::load(data_dir).ok().flatten().and_then(|cfg| {
                let fork_dirs: Vec<PathBuf> = search_dirs.iter().rev().cloned().collect();
                ObjectStoreFetcher::new(&cfg, &fork_dirs)
                    .ok()
                    .map(|f| Box::new(f) as Box<dyn SegmentFetcher>)
            });

        Ok(Self {
            search_dirs,
            lbamap,
            extent_index,
            fetcher,
            primary_index_dir,
            primary_cache_dir,
        })
    }

    /// Find the `(index_dir, body_dir)` pair for `segment_id` by locating its
    /// `.idx` file in one of the search dirs' `index/` subdirectories.
    /// Returns `None` if not found in any search dir (fall back to primary dirs).
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

    fn read_block(&self, lba: u64) -> io::Result<[u8; 4096]> {
        let Some((hash, block_offset)) = self.lbamap.lookup(lba) else {
            return Ok([0u8; 4096]); // unwritten block — return zeros
        };
        let Some(loc) = self.extent_index.lookup(&hash) else {
            return Ok([0u8; 4096]); // hash not indexed — treat as unwritten
        };
        let loc = loc.clone();

        // Inline extents: data is held in memory, no file I/O needed.
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

        // Per-extent demand-fetch for cached entries.
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
}

impl Ext4Read for VolumeReader {
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

fn find_segment_file(search_dirs: &[PathBuf], segment_id: ulid::Ulid) -> io::Result<PathBuf> {
    let sid = segment_id.to_string();
    for dir in search_dirs {
        for subdir in ["wal", "pending"] {
            let path = dir.join(subdir).join(&sid);
            if path.exists() {
                return Ok(path);
            }
        }
        // Check for a demand-fetched body file. The `.body` file contains only
        // the body section, and ExtentLocation.body_offset is body-relative for
        // cached entries, so seeking to body_offset in this file is correct.
        let body = dir.join("cache").join(format!("{sid}.body"));
        if body.exists() {
            return Ok(body);
        }
    }
    Err(io::Error::other(format!("segment not found: {sid}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_core::volume::{Volume, fork_volume};
    use tempfile::TempDir;

    /// Allocate a fresh volume directory path under `by_id` (does not create it).
    /// Use this when the dir will be created by `fork_volume`, which generates
    /// its own keypair.
    fn new_vol_path(by_id: &std::path::Path) -> PathBuf {
        by_id.join(ulid::Ulid::new().to_string())
    }

    /// Allocate a fresh volume directory under `by_id`, create it, and write a
    /// keypair + default provenance so `Volume::open` can load `volume.key`
    /// and ancestor walkers find a valid provenance file.
    fn new_vol_dir(by_id: &std::path::Path) -> PathBuf {
        let dir = new_vol_path(by_id);
        std::fs::create_dir_all(&dir).unwrap();
        let key = elide_core::signing::generate_keypair(
            &dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        elide_core::signing::write_provenance(
            &dir,
            &key,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
            &elide_core::signing::ProvenanceLineage::default(),
        )
        .unwrap();
        dir
    }

    /// Write one block, snapshot (WAL→pending), drop.
    fn write_and_snapshot(vol_dir: &std::path::Path, by_id: &std::path::Path, lba: u64, byte: u8) {
        let mut vol = Volume::open(vol_dir, by_id).unwrap();
        vol.write(lba, &[byte; 4096]).unwrap();
        vol.snapshot().unwrap();
    }

    // ── flat volume ───────────────────────────────────────────────────────────

    /// VolumeReader can read blocks from a flat (non-fork) volume's WAL.
    #[test]
    fn volume_reader_flat_reads_wal() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = new_vol_dir(tmp.path());
        let mut vol = Volume::open(&vol_dir, tmp.path()).unwrap();
        vol.write(0, &[0x42u8; 4096]).unwrap();
        drop(vol); // WAL remains unflushed

        let reader = VolumeReader::open(&vol_dir).unwrap();
        assert_eq!(reader.read_block(0).unwrap(), [0x42u8; 4096]);
        assert_eq!(reader.read_block(1).unwrap(), [0u8; 4096]); // unwritten
    }

    /// VolumeReader can read blocks that have been promoted to pending/.
    #[test]
    fn volume_reader_flat_reads_pending() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = new_vol_dir(tmp.path());
        write_and_snapshot(&vol_dir, tmp.path(), 0, 0xAA);

        let reader = VolumeReader::open(&vol_dir).unwrap();
        assert_eq!(reader.read_block(0).unwrap(), [0xAAu8; 4096]);
    }

    // ── fork via symlink ──────────────────────────────────────────────────────

    /// VolumeReader on a by_name/<name> symlink correctly resolves ancestor
    /// segments.  This was broken when dir.parent() returned by_name/ instead
    /// of by_id/.
    #[test]
    fn volume_reader_fork_via_symlink_reads_ancestor() {
        let data_dir = TempDir::new().unwrap();
        let by_id = data_dir.path().join("by_id");
        let by_name = data_dir.path().join("by_name");
        std::fs::create_dir_all(&by_id).unwrap();
        std::fs::create_dir_all(&by_name).unwrap();

        // Parent: write a known block and snapshot.
        let parent_dir = new_vol_dir(&by_id);
        write_and_snapshot(&parent_dir, &by_id, 0, 0xAA);

        // Fork branching off the parent.
        let fork_dir = new_vol_path(&by_id);
        fork_volume(&fork_dir, &parent_dir).unwrap();

        // by_name symlink pointing at the fork.
        let symlink = by_name.join("my-fork");
        let rel = format!(
            "../by_id/{}",
            fork_dir.file_name().unwrap().to_str().unwrap()
        );
        std::os::unix::fs::symlink(&rel, &symlink).unwrap();

        // Open via symlink — must see the ancestor block.
        let reader = VolumeReader::open(&symlink).unwrap();
        assert_eq!(
            reader.read_block(0).unwrap(),
            [0xAAu8; 4096],
            "ancestor block must be visible through symlink path"
        );
        assert_eq!(reader.read_block(1).unwrap(), [0u8; 4096]);
    }

    /// VolumeReader triggers per-extent demand-fetch when the `.present` bit is
    /// unset: the `.body` file is written, the bit is set, and the block reads back
    /// correctly.
    #[test]
    fn volume_reader_per_extent_demand_fetch() {
        use object_store::ObjectStore;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as StorePath;
        use std::sync::Arc;

        let data_dir = TempDir::new().unwrap();
        let by_id = data_dir.path().join("by_id");
        let store_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(&by_id).unwrap();

        // Write fetch.toml in data_dir pointing to the local store.
        std::fs::write(
            data_dir.path().join("fetch.toml"),
            format!(
                "local_path = {:?}\n",
                store_dir.path().to_string_lossy().as_ref()
            ),
        )
        .unwrap();

        let vol_dir = new_vol_dir(&by_id);
        let vol_id = vol_dir.file_name().unwrap().to_str().unwrap().to_owned();

        // Write high-entropy data larger than INLINE_THRESHOLD so the extent
        // goes into the body section and triggers demand-fetch.
        {
            let mut vol = Volume::open(&vol_dir, &by_id).unwrap();
            let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
            vol.write(0, &data).unwrap();
            vol.snapshot().unwrap();
        }

        // snapshot() auto-promotes pending → index/cache. Find the segment
        // in index/, reconstruct the full segment bytes from .idx + .body for
        // upload, then evict the local cache to force demand-fetch.
        let index_dir = vol_dir.join("index");
        let cache_dir = vol_dir.join("cache");
        let idx_entry = std::fs::read_dir(&index_dir)
            .unwrap()
            .flatten()
            .find(|e| e.file_name().to_str().is_some_and(|s| s.ends_with(".idx")))
            .unwrap();
        let seg_id = idx_entry
            .file_name()
            .to_str()
            .unwrap()
            .strip_suffix(".idx")
            .unwrap()
            .to_owned();
        let idx_bytes = std::fs::read(idx_entry.path()).unwrap();
        let body_bytes = std::fs::read(cache_dir.join(format!("{seg_id}.body"))).unwrap();
        let seg_bytes = [idx_bytes.as_slice(), body_bytes.as_slice()].concat();

        // Upload the full segment to the local store.
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_dir.path()).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let seg_ulid: ulid::Ulid = seg_id.parse().unwrap();
        let dt: chrono::DateTime<chrono::Utc> = seg_ulid.datetime().into();
        let date = dt.format("%Y%m%d").to_string();
        let key = StorePath::from(format!("by_id/{vol_id}/segments/{date}/{seg_id}"));
        rt.block_on(store.put(&key, seg_bytes.into())).unwrap();

        // Evict the local cache to simulate a cold read that must demand-fetch.
        std::fs::remove_file(cache_dir.join(format!("{seg_id}.body"))).unwrap();
        std::fs::remove_file(cache_dir.join(format!("{seg_id}.present"))).unwrap();

        // VolumeReader should configure a fetcher from fetch.toml and trigger
        // per-extent fetch on the first read_block(0).
        let reader = VolumeReader::open(&vol_dir).unwrap();
        let block = reader.read_block(0).unwrap();
        let expected: Vec<u8> = (0..4096).map(|i| ((i * 7 + 13) & 0xFF) as u8).collect();
        assert_eq!(
            block.as_slice(),
            expected.as_slice(),
            "demand-fetched block must match written data"
        );

        // .body and .present must have been created by the demand-fetch in cache/.
        assert!(
            cache_dir.join(format!("{seg_id}.body")).exists(),
            ".body should be created"
        );
        assert!(
            cache_dir.join(format!("{seg_id}.present")).exists(),
            ".present should be created"
        );
    }

    /// A fork's own writes shadow the ancestor's data for the same LBA.
    #[test]
    fn volume_reader_fork_shadows_ancestor() {
        let data_dir = TempDir::new().unwrap();
        let by_id = data_dir.path().join("by_id");
        let by_name = data_dir.path().join("by_name");
        std::fs::create_dir_all(&by_id).unwrap();
        std::fs::create_dir_all(&by_name).unwrap();

        // Parent: LBA 0 = 0xAA, LBA 1 = 0xBB.
        let parent_dir = new_vol_dir(&by_id);
        write_and_snapshot(&parent_dir, &by_id, 0, 0xAA);
        write_and_snapshot(&parent_dir, &by_id, 1, 0xBB);

        // Fork: overwrite LBA 0 with 0xCC; LBA 1 unchanged.
        let fork_dir = new_vol_path(&by_id);
        fork_volume(&fork_dir, &parent_dir).unwrap();
        let mut fork_vol = Volume::open(&fork_dir, &by_id).unwrap();
        fork_vol.write(0, &[0xCCu8; 4096]).unwrap();
        drop(fork_vol); // WAL remains

        let symlink = by_name.join("shadowed");
        let rel = format!(
            "../by_id/{}",
            fork_dir.file_name().unwrap().to_str().unwrap()
        );
        std::os::unix::fs::symlink(&rel, &symlink).unwrap();

        let reader = VolumeReader::open(&symlink).unwrap();
        assert_eq!(
            reader.read_block(0).unwrap(),
            [0xCCu8; 4096],
            "fork write must shadow ancestor"
        );
        assert_eq!(
            reader.read_block(1).unwrap(),
            [0xBBu8; 4096],
            "unshadowed ancestor block must still be visible"
        );
    }
}
