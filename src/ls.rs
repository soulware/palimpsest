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

use elide_core::{extentindex, lbamap, segment, volume, writelog};

/// Brief summary of a fork's ext4 filesystem, for use in inspect output.
pub struct FsSummary {
    /// Value of PRETTY_NAME from /etc/os-release, if present.
    pub os_name: Option<String>,
    /// Names of entries at the filesystem root, sorted.
    pub root_entries: Vec<String>,
}

/// Try to load `fork_dir` as an ext4 volume and return a brief summary.
///
/// Returns `None` silently if the volume is not ext4 or cannot be read.
pub fn try_fs_summary(fork_dir: &Path) -> Option<FsSummary> {
    let reader = VolumeReader::open(fork_dir).ok()?;
    let fs = Ext4::load(Box::new(reader)).ok()?;

    let os_name = read_os_name(&fs);

    let root = Ext4PathBuf::new("/");
    let mut root_entries = fs
        .read_dir(&root)
        .ok()?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name().display().to_string();
            n != "." && n != ".."
        })
        .map(|e| {
            let name = e.file_name().display().to_string();
            let suffix = e
                .metadata()
                .ok()
                .map(|m| entry_suffix(m.file_type()))
                .unwrap_or("");
            format!("{name}{suffix}")
        })
        .collect::<Vec<_>>();
    root_entries.sort();

    Some(FsSummary {
        os_name,
        root_entries,
    })
}

fn read_os_name(fs: &Ext4) -> Option<String> {
    let path = Ext4PathBuf::new("/etc/os-release");
    let data = fs.read(&path).ok()?;
    let text = std::str::from_utf8(&data).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("PRETTY_NAME=") {
            return Some(rest.trim_matches('"').to_owned());
        }
    }
    None
}

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
                .ok_or_else(|| io::Error::other("bad WAL filename"))?
                .to_owned();

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
                                segment_id: ulid.clone(),
                                body_offset,
                                body_length: data.len() as u32,
                                compressed: flags & writelog::FLAG_COMPRESSED != 0,
                            },
                        );
                    }
                    writelog::LogRecord::Ref {
                        hash,
                        start_lba,
                        lba_length,
                    } => {
                        // Data lives in a segment already covered by rebuild above.
                        lbamap.insert(start_lba, lba_length, hash);
                    }
                }
            }
        }

        Ok(Self {
            search_dirs,
            lbamap,
            extent_index,
        })
    }

    fn read_block(&self, lba: u64) -> io::Result<[u8; 4096]> {
        let Some((hash, block_offset)) = self.lbamap.lookup(lba) else {
            return Ok([0u8; 4096]); // unwritten block — return zeros
        };
        let Some(loc) = self.extent_index.lookup(&hash) else {
            return Ok([0u8; 4096]); // hash not indexed — treat as unwritten
        };
        let loc = loc.clone();
        let path = find_segment_file(&self.search_dirs, &loc.segment_id)?;
        let mut f = fs::File::open(path)?;
        let mut block = [0u8; 4096];
        if loc.compressed {
            f.seek(SeekFrom::Start(loc.body_offset))?;
            let mut buf = vec![0u8; loc.body_length as usize];
            f.read_exact(&mut buf)?;
            let decompressed =
                lz4_flex::decompress_size_prepended(&buf).map_err(io::Error::other)?;
            let src = block_offset as usize * 4096;
            block.copy_from_slice(&decompressed[src..src + 4096]);
        } else {
            f.seek(SeekFrom::Start(
                loc.body_offset + block_offset as u64 * 4096,
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

#[cfg(test)]
mod tests {
    use super::*;
    use elide_core::volume::{Volume, fork_volume};
    use tempfile::TempDir;

    /// Allocate a fresh volume directory under `by_id`.
    fn new_vol_dir(by_id: &std::path::Path) -> PathBuf {
        by_id.join(ulid::Ulid::new().to_string())
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
        let fork_dir = new_vol_dir(&by_id);
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
        let fork_dir = new_vol_dir(&by_id);
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

fn find_segment_file(search_dirs: &[PathBuf], segment_id: &str) -> io::Result<PathBuf> {
    for dir in search_dirs {
        for subdir in ["wal", "pending", "segments"] {
            let path = dir.join(subdir).join(segment_id);
            if path.exists() {
                return Ok(path);
            }
        }
    }
    Err(io::Error::other(format!("segment not found: {segment_id}")))
}
