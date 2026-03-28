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
    base_dir: PathBuf,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
}

impl VolumeReader {
    fn open(dir: &Path) -> io::Result<Self> {
        // Rebuild LBA map and extent index from all committed segments (including ancestors).
        let ancestor_layers = volume::walk_ancestors(dir)?;
        let rebuild_chain: Vec<(std::path::PathBuf, Option<String>)> = ancestor_layers
            .into_iter()
            .map(|l| (l.dir, l.branch_ulid))
            .chain(std::iter::once((dir.to_owned(), None)))
            .collect();
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
            base_dir: dir.to_owned(),
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
        let path = find_segment_file(&self.base_dir, &loc.segment_id)?;
        let mut f = fs::File::open(path)?;
        let mut block = [0u8; 4096];
        if loc.compressed {
            f.seek(SeekFrom::Start(loc.body_offset))?;
            let mut buf = vec![0u8; loc.body_length as usize];
            f.read_exact(&mut buf)?;
            let decompressed = zstd::decode_all(buf.as_slice()).map_err(io::Error::other)?;
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

fn find_segment_file(base_dir: &Path, segment_id: &str) -> io::Result<PathBuf> {
    for subdir in ["wal", "pending", "segments"] {
        let path = base_dir.join(subdir).join(segment_id);
        if path.exists() {
            return Ok(path);
        }
    }
    Err(io::Error::other(format!("segment not found: {segment_id}")))
}
