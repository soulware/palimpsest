// Read-only ext4 view of a volume directory.
//
// A thin CLI wrapper around `elide_core::block_reader::BlockReader`: builds a
// live view of the fork (ancestors + WAL tail) and hands it to `ext4-view` so
// the filesystem can be listed without mounting.

use std::io;
use std::path::{Path, PathBuf};

use ext4_view::{Ext4, FileType, PathBuf as Ext4PathBuf};

use elide_core::block_reader::BlockReader;
use elide_core::segment::SegmentFetcher;

use elide_fetch::{FetchConfig, RemoteFetcher};

pub fn run(dir: &Path, fs_path: &str) -> io::Result<()> {
    let reader = open_live_reader(dir)?;
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

/// Open a live `BlockReader` for `dir`, wiring a `RemoteFetcher` from
/// `fetch.toml` if one is present alongside the `by_id/` directory.
fn open_live_reader(dir: &Path) -> io::Result<BlockReader> {
    BlockReader::open_live(dir, Box::new(remote_fetcher_factory))
}

fn remote_fetcher_factory(search_dirs: &[PathBuf]) -> Option<Box<dyn SegmentFetcher>> {
    let dir0 = search_dirs.first()?;
    let by_id_dir = dir0.parent()?;
    let data_dir = by_id_dir.parent().unwrap_or(by_id_dir);
    let cfg = FetchConfig::load(data_dir).ok().flatten()?;
    RemoteFetcher::new(&cfg)
        .ok()
        .map(|f| Box::new(f) as Box<dyn SegmentFetcher>)
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

#[cfg(test)]
mod tests {
    use super::*;
    use elide_core::volume::{Volume, fork_volume};
    use tempfile::TempDir;

    fn new_vol_path(by_id: &std::path::Path) -> PathBuf {
        by_id.join(ulid::Ulid::new().to_string())
    }

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

    fn write_and_snapshot(vol_dir: &std::path::Path, by_id: &std::path::Path, lba: u64, byte: u8) {
        let mut vol = Volume::open(vol_dir, by_id).unwrap();
        vol.write(lba, &[byte; 4096]).unwrap();
        vol.snapshot().unwrap();
    }

    /// BlockReader::open_live can read blocks from a flat volume's WAL.
    #[test]
    fn block_reader_flat_reads_wal() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = new_vol_dir(tmp.path());
        let mut vol = Volume::open(&vol_dir, tmp.path()).unwrap();
        vol.write(0, &[0x42u8; 4096]).unwrap();
        drop(vol);

        let reader = open_live_reader(&vol_dir).unwrap();
        assert_eq!(reader.read_block(0).unwrap(), [0x42u8; 4096]);
        assert_eq!(reader.read_block(1).unwrap(), [0u8; 4096]);
    }

    /// BlockReader::open_live can read blocks that have been promoted to pending/.
    #[test]
    fn block_reader_flat_reads_pending() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = new_vol_dir(tmp.path());
        write_and_snapshot(&vol_dir, tmp.path(), 0, 0xAA);

        let reader = open_live_reader(&vol_dir).unwrap();
        assert_eq!(reader.read_block(0).unwrap(), [0xAAu8; 4096]);
    }

    /// BlockReader::open_live on a by_name/<name> symlink correctly resolves
    /// ancestor segments.
    #[test]
    fn block_reader_fork_via_symlink_reads_ancestor() {
        let data_dir = TempDir::new().unwrap();
        let by_id = data_dir.path().join("by_id");
        let by_name = data_dir.path().join("by_name");
        std::fs::create_dir_all(&by_id).unwrap();
        std::fs::create_dir_all(&by_name).unwrap();

        let parent_dir = new_vol_dir(&by_id);
        write_and_snapshot(&parent_dir, &by_id, 0, 0xAA);

        let fork_dir = new_vol_path(&by_id);
        fork_volume(&fork_dir, &parent_dir).unwrap();

        let symlink = by_name.join("my-fork");
        let rel = format!(
            "../by_id/{}",
            fork_dir.file_name().unwrap().to_str().unwrap()
        );
        std::os::unix::fs::symlink(&rel, &symlink).unwrap();

        let reader = open_live_reader(&symlink).unwrap();
        assert_eq!(
            reader.read_block(0).unwrap(),
            [0xAAu8; 4096],
            "ancestor block must be visible through symlink path"
        );
        assert_eq!(reader.read_block(1).unwrap(), [0u8; 4096]);
    }

    /// BlockReader triggers per-extent demand-fetch when the `.present` bit is
    /// unset: the `.body` file is written, the bit is set, and the block reads
    /// back correctly.
    #[test]
    fn block_reader_per_extent_demand_fetch() {
        use object_store::ObjectStore;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as StorePath;
        use std::sync::Arc;

        let data_dir = TempDir::new().unwrap();
        let by_id = data_dir.path().join("by_id");
        let store_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(&by_id).unwrap();

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

        {
            let mut vol = Volume::open(&vol_dir, &by_id).unwrap();
            let data: Vec<u8> = (0..8192).map(|i| (i * 7 + 13) as u8).collect();
            vol.write(0, &data).unwrap();
            vol.snapshot().unwrap();
        }

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

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_dir.path()).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let seg_ulid: ulid::Ulid = seg_id.parse().unwrap();
        let dt: chrono::DateTime<chrono::Utc> = seg_ulid.datetime().into();
        let date = dt.format("%Y%m%d").to_string();
        let key = StorePath::from(format!("by_id/{vol_id}/segments/{date}/{seg_id}"));
        rt.block_on(store.put(&key, seg_bytes.into())).unwrap();

        std::fs::remove_file(cache_dir.join(format!("{seg_id}.body"))).unwrap();
        std::fs::remove_file(cache_dir.join(format!("{seg_id}.present"))).unwrap();

        let reader = open_live_reader(&vol_dir).unwrap();
        let block = reader.read_block(0).unwrap();
        let expected: Vec<u8> = (0..4096).map(|i| ((i * 7 + 13) & 0xFF) as u8).collect();
        assert_eq!(
            block.as_slice(),
            expected.as_slice(),
            "demand-fetched block must match written data"
        );

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
    fn block_reader_fork_shadows_ancestor() {
        let data_dir = TempDir::new().unwrap();
        let by_id = data_dir.path().join("by_id");
        let by_name = data_dir.path().join("by_name");
        std::fs::create_dir_all(&by_id).unwrap();
        std::fs::create_dir_all(&by_name).unwrap();

        let parent_dir = new_vol_dir(&by_id);
        write_and_snapshot(&parent_dir, &by_id, 0, 0xAA);
        write_and_snapshot(&parent_dir, &by_id, 1, 0xBB);

        let fork_dir = new_vol_path(&by_id);
        fork_volume(&fork_dir, &parent_dir).unwrap();
        let mut fork_vol = Volume::open(&fork_dir, &by_id).unwrap();
        fork_vol.write(0, &[0xCCu8; 4096]).unwrap();
        drop(fork_vol);

        let symlink = by_name.join("shadowed");
        let rel = format!(
            "../by_id/{}",
            fork_dir.file_name().unwrap().to_str().unwrap()
        );
        std::os::unix::fs::symlink(&rel, &symlink).unwrap();

        let reader = open_live_reader(&symlink).unwrap();
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
