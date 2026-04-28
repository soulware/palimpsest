// Walks an ext4 image's inode table to find all regular file extents,
// hashes each extent's content, and reports dedup statistics.
//
// Ported from ext4-view (MIT OR Apache-2.0), Google LLC 2024.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use ext4_view::PathBuf as Ext4PathBuf;
use ext4_view::{DirEntry, Ext4, Metadata};

// --- ext4 constants ---

const SUPERBLOCK_OFFSET: u64 = 1024;
const EXT4_MAGIC: u16 = 0xef53;
const EXTENT_MAGIC: u16 = 0xf30a;
const INODE_FLAG_EXTENTS: u32 = 0x0008_0000;
const S_IFREG: u16 = 0x8000;
const S_IFMT: u16 = 0xf000;
const INCOMPAT_64BIT: u32 = 0x80;
const EXTENT_ENTRY_SIZE: usize = 12;

// --- byte helpers ---

fn u16le(data: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(data[off..off + 2].try_into().unwrap())
}

fn u32le(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

fn hilo64(hi: u32, lo: u32) -> u64 {
    (u64::from(hi) << 32) | u64::from(lo)
}

// --- superblock ---

struct Superblock {
    block_size: u64,
    inode_size: usize,
    inodes_per_group: u32,
    num_block_groups: u32,
    is_64bit: bool,
}

impl Superblock {
    fn read(f: &mut File) -> io::Result<Self> {
        let mut buf = vec![0u8; 1024];
        f.seek(SeekFrom::Start(SUPERBLOCK_OFFSET))?;
        f.read_exact(&mut buf)?;

        if u16le(&buf, 0x38) != EXT4_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not ext4"));
        }

        let block_size = 1024u64 << u32le(&buf, 0x18);
        let inode_size = u16le(&buf, 0x58) as usize;
        let inodes_per_group = u32le(&buf, 0x28);
        let blocks_per_group = u32le(&buf, 0x20);
        let first_data_block = u32le(&buf, 0x14) as u64;
        let blocks_count = hilo64(u32le(&buf, 0x150), u32le(&buf, 0x04));
        let is_64bit = (u32le(&buf, 0x60) & INCOMPAT_64BIT) != 0;

        let num_data_blocks = blocks_count.saturating_sub(first_data_block);
        let num_block_groups = num_data_blocks.div_ceil(blocks_per_group as u64) as u32;

        Ok(Self {
            block_size,
            inode_size,
            inodes_per_group,
            num_block_groups,
            is_64bit,
        })
    }

    fn bgdt_start(&self) -> u64 {
        // Block group descriptor table starts at block 1 (or 2 if block_size==1024).
        if self.block_size == 1024 {
            2 * self.block_size
        } else {
            self.block_size
        }
    }

    fn bgd_size(&self) -> u64 {
        if self.is_64bit { 64 } else { 32 }
    }
}

// --- block group descriptor ---

fn inode_table_block(f: &mut File, sb: &Superblock, group: u32) -> io::Result<u64> {
    let offset = sb.bgdt_start() + group as u64 * sb.bgd_size();
    let mut buf = vec![0u8; sb.bgd_size() as usize];
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(&mut buf)?;

    let lo = u32le(&buf, 0x08) as u64;
    let hi = if sb.is_64bit {
        u32le(&buf, 0x28) as u64
    } else {
        0
    };
    Ok((hi << 32) | lo)
}

// --- extent tree ---

// Collect all leaf extents from an extent tree node. Recurses into
// index nodes by seeking into the image file.
fn collect_extents(
    data: &[u8],
    f: &mut File,
    sb: &Superblock,
    out: &mut Vec<(u64, u16)>, // (start_block, num_blocks)
) -> io::Result<()> {
    if data.len() < EXTENT_ENTRY_SIZE {
        return Ok(());
    }
    if u16le(data, 0) != EXTENT_MAGIC {
        return Ok(());
    }

    let num_entries = u16le(data, 2) as usize;
    let depth = u16le(data, 6);

    for i in 0..num_entries {
        let off = (i + 1) * EXTENT_ENTRY_SIZE;
        if off + EXTENT_ENTRY_SIZE > data.len() {
            break;
        }
        let entry = &data[off..off + EXTENT_ENTRY_SIZE];

        if depth == 0 {
            // Leaf extent.
            let len = u16le(entry, 4) & 0x7fff; // strip uninitialised bit
            let start = hilo64(u16le(entry, 6) as u32, u32le(entry, 8));
            out.push((start, len));
        } else {
            // Index node — recurse into child block.
            let child = hilo64(u16le(entry, 8) as u32, u32le(entry, 4));
            let mut child_data = vec![0u8; sb.block_size as usize];
            f.seek(SeekFrom::Start(child * sb.block_size))?;
            f.read_exact(&mut child_data)?;
            collect_extents(&child_data, f, sb, out)?;
        }
    }

    Ok(())
}

// --- inode iteration ---

/// One extent from a regular file: content hash and byte count.
pub struct ExtentEntry {
    pub hash: blake3::Hash,
    pub byte_count: u64,
}

// --- extent tree (with logical block numbers, for correct file ordering) ---

// Like collect_extents but also captures the logical block number of each leaf,
// so callers can sort extents into logical file order before hashing.
fn collect_extents_with_logical(
    data: &[u8],
    f: &mut File,
    sb: &Superblock,
    out: &mut Vec<(u32, u64, u16)>, // (logical_block, phys_start_block, num_blocks)
) -> io::Result<()> {
    if data.len() < EXTENT_ENTRY_SIZE {
        return Ok(());
    }
    if u16le(data, 0) != EXTENT_MAGIC {
        return Ok(());
    }

    let num_entries = u16le(data, 2) as usize;
    let depth = u16le(data, 6);

    for i in 0..num_entries {
        let off = (i + 1) * EXTENT_ENTRY_SIZE;
        if off + EXTENT_ENTRY_SIZE > data.len() {
            break;
        }
        let entry = &data[off..off + EXTENT_ENTRY_SIZE];

        if depth == 0 {
            let logical = u32le(entry, 0);
            let len = u16le(entry, 4) & 0x7fff;
            let phys = hilo64(u16le(entry, 6) as u32, u32le(entry, 8));
            out.push((logical, phys, len));
        } else {
            let child = hilo64(u16le(entry, 8) as u32, u32le(entry, 4));
            let mut child_data = vec![0u8; sb.block_size as usize];
            f.seek(SeekFrom::Start(child * sb.block_size))?;
            f.read_exact(&mut child_data)?;
            collect_extents_with_logical(&child_data, f, sb, out)?;
        }
    }

    Ok(())
}

// Per-inode data: full-file hash (for joining with ext4_view paths) plus
// per-extent disk locations and hashes.
struct InodeExtents {
    full_hash: blake3::Hash,
    // (disk_start_byte, file_logical_offset, byte_count, extent_hash)
    extents: Vec<(u64, u64, u64, blake3::Hash)>,
}

// Scan all regular-file inodes, grouping extents by inode and computing both
// a per-extent hash and the full-file hash (in logical block order).
fn scan_file_extents_with_full_hash(
    f: &mut File,
    sb: &Superblock,
) -> io::Result<Vec<InodeExtents>> {
    let mut results = Vec::new();
    let mut inode_buf = vec![0u8; sb.inode_size];

    for group in 0..sb.num_block_groups {
        let table_block = inode_table_block(f, sb, group)?;
        let table_offset = table_block * sb.block_size;

        for idx in 0..sb.inodes_per_group {
            let inode_offset = table_offset + idx as u64 * sb.inode_size as u64;
            f.seek(SeekFrom::Start(inode_offset))?;
            if f.read_exact(&mut inode_buf).is_err() {
                break;
            }

            let i_mode = u16le(&inode_buf, 0x00);
            if (i_mode & S_IFMT) != S_IFREG {
                continue;
            }
            if u16le(&inode_buf, 0x1a) == 0 {
                continue;
            }
            let i_flags = u32le(&inode_buf, 0x20);
            if (i_flags & INODE_FLAG_EXTENTS) == 0 {
                continue;
            }
            let i_size = hilo64(u32le(&inode_buf, 0x6c), u32le(&inode_buf, 0x04));
            if i_size == 0 {
                continue;
            }

            let i_block = inode_buf[0x28..0x28 + 60].to_vec();
            let mut raw: Vec<(u32, u64, u16)> = Vec::new();
            collect_extents_with_logical(&i_block, f, sb, &mut raw)?;
            if raw.is_empty() {
                continue;
            }

            raw.sort_by_key(|&(logical, _, _)| logical);

            let mut full_hasher = blake3::Hasher::new();
            let mut extent_entries = Vec::new();
            let mut bytes_remaining = i_size;

            for (logical, phys_block, num_blocks) in raw {
                if bytes_remaining == 0 {
                    break;
                }
                let file_logical_offset = logical as u64 * sb.block_size;
                if file_logical_offset >= i_size {
                    // Preallocated extent beyond i_size — not real file data, skip.
                    continue;
                }
                let allocated = num_blocks as u64 * sb.block_size;
                let byte_count = allocated.min(bytes_remaining);
                bytes_remaining = bytes_remaining.saturating_sub(byte_count);

                let disk_start_byte = phys_block * sb.block_size;
                let mut buf = vec![0u8; byte_count as usize];
                f.seek(SeekFrom::Start(disk_start_byte))?;
                f.read_exact(&mut buf)?;

                full_hasher.update(&buf);
                extent_entries.push((
                    disk_start_byte,
                    file_logical_offset,
                    byte_count,
                    blake3::hash(&buf),
                ));
            }

            results.push(InodeExtents {
                full_hash: full_hasher.finalize(),
                extents: extent_entries,
            });
        }
    }

    Ok(results)
}

// Build a map from extent_hash → (file_path, file_logical_offset, byte_count) for the given image.
// Uses the file-logical byte offset (not disk offset) to locate the extent within ext4_view-read data.
fn build_extent_logical_map(image: &Path) -> io::Result<HashMap<blake3::Hash, (String, u64, u64)>> {
    let mut f = File::open(image)?;
    let sb = Superblock::read(&mut f)?;
    let inode_data = scan_file_extents_with_full_hash(&mut f, &sb)?;

    let file_hashes = enumerate_file_hashes(image)?;
    let hash_to_path: HashMap<blake3::Hash, String> = file_hashes
        .into_iter()
        .map(|(path, (hash, _))| (hash, path))
        .collect();

    let mut map = HashMap::new();
    for inode in inode_data {
        let path = hash_to_path
            .get(&inode.full_hash)
            .cloned()
            .unwrap_or_default();
        for (_disk_start_byte, file_logical_offset, byte_count, ext_hash) in inode.extents {
            map.insert(ext_hash, (path.clone(), file_logical_offset, byte_count));
        }
    }
    Ok(map)
}

// --- boot trace I/O ---

pub struct TraceEntry {
    pub hash: blake3::Hash,
    pub start_byte: u64,
    pub byte_count: u64,
}

pub fn save_trace(path: &Path, entries: &[TraceEntry]) -> io::Result<()> {
    let mut out = String::from("# elide-boot-trace v1\n");
    for e in entries {
        out.push_str(&format!(
            "{} {} {}\n",
            e.hash.to_hex(),
            e.start_byte,
            e.byte_count
        ));
    }
    std::fs::write(path, out)
}

fn load_trace(path: &Path) -> io::Result<Vec<TraceEntry>> {
    let content = std::fs::read_to_string(path)?;
    let mut entries = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let hash_str = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing hash"))?;
        let start_byte: u64 = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad start_byte"))?;
        let byte_count: u64 = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad byte_count"))?;
        let hash = hex_to_hash(hash_str)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad hash"))?;
        entries.push(TraceEntry {
            hash,
            start_byte,
            byte_count,
        });
    }
    Ok(entries)
}

fn hex_to_hash(s: &str) -> Option<blake3::Hash> {
    if s.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Some(blake3::Hash::from(bytes))
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// Reads every regular-file inode in the image and yields its extents.
// Returns (entries, block_size).
fn scan_file_extents(f: &mut File, sb: &Superblock) -> io::Result<Vec<ExtentEntry>> {
    let mut results = Vec::new();
    let mut inode_buf = vec![0u8; sb.inode_size];
    let mut block_buf: Vec<u8>;

    for group in 0..sb.num_block_groups {
        let table_block = inode_table_block(f, sb, group)?;
        let table_offset = table_block * sb.block_size;

        for idx in 0..sb.inodes_per_group {
            let inode_offset = table_offset + idx as u64 * sb.inode_size as u64;
            f.seek(SeekFrom::Start(inode_offset))?;
            match f.read_exact(&mut inode_buf) {
                Ok(_) => {}
                Err(_) => break,
            }

            let i_mode = u16le(&inode_buf, 0x00);
            if (i_mode & S_IFMT) != S_IFREG {
                continue;
            }
            let i_links = u16le(&inode_buf, 0x1a);
            if i_links == 0 {
                continue;
            }
            let i_flags = u32le(&inode_buf, 0x20);
            if (i_flags & INODE_FLAG_EXTENTS) == 0 {
                continue;
            }

            let i_size = hilo64(u32le(&inode_buf, 0x6c), u32le(&inode_buf, 0x04));
            if i_size == 0 {
                continue;
            }

            let i_block = inode_buf[0x28..0x28 + 60].to_vec();
            let mut extents: Vec<(u64, u16)> = Vec::new();
            collect_extents(&i_block, f, sb, &mut extents)?;

            if extents.is_empty() {
                continue;
            }

            let mut bytes_remaining = i_size;
            for (start_block, num_blocks) in &extents {
                if bytes_remaining == 0 {
                    break;
                }
                let allocated = *num_blocks as u64 * sb.block_size;
                let to_read = allocated.min(bytes_remaining);
                bytes_remaining = bytes_remaining.saturating_sub(to_read);

                block_buf = vec![0u8; to_read as usize];
                f.seek(SeekFrom::Start(start_block * sb.block_size))?;
                f.read_exact(&mut block_buf)?;

                results.push(ExtentEntry {
                    hash: blake3::hash(&block_buf),
                    byte_count: to_read,
                });
            }
        }
    }

    Ok(results)
}

// --- stats ---

#[derive(Default)]
struct ExtentStats {
    extent_count: usize,
    total_bytes: u64,
    hash_to_bytes: HashMap<blake3::Hash, u64>,
}

impl ExtentStats {
    fn record(&mut self, hash: blake3::Hash, bytes: u64) {
        self.extent_count += 1;
        self.total_bytes += bytes;
        self.hash_to_bytes.entry(hash).or_insert(bytes);
    }

    fn unique(&self) -> usize {
        self.hash_to_bytes.len()
    }
}

fn build_stats(entries: &[ExtentEntry]) -> ExtentStats {
    let mut stats = ExtentStats::default();
    for e in entries {
        stats.record(e.hash, e.byte_count);
    }
    stats
}

// --- output ---

fn print_stats(label: &str, stats: &ExtentStats) {
    let unique = stats.unique();
    let total_mb = stats.total_bytes as f64 / (1024.0 * 1024.0);
    let dedup_ratio = if unique > 0 {
        stats.extent_count as f64 / unique as f64
    } else {
        1.0
    };

    println!("=== {} ===", label);
    println!("  File extents:     {}", stats.extent_count);
    println!(
        "  Unique extents:   {} ({:.1}% unique)",
        unique,
        100.0 * unique as f64 / stats.extent_count.max(1) as f64
    );
    println!("  Dedup ratio:      {:.2}x", dedup_ratio);
    println!("  Data (non-zero):  {:.1} MB", total_mb);
    println!();
}

fn print_overlap(stats1: &ExtentStats, stats2: &ExtentStats) {
    let common = stats1
        .hash_to_bytes
        .keys()
        .filter(|h| stats2.hash_to_bytes.contains_key(h))
        .count();

    let common_bytes: u64 = stats1
        .hash_to_bytes
        .iter()
        .filter(|(h, _)| stats2.hash_to_bytes.contains_key(h))
        .map(|(_, &b)| b)
        .sum();

    let pct_extents1 = 100.0 * common as f64 / stats1.unique().max(1) as f64;
    let pct_extents2 = 100.0 * common as f64 / stats2.unique().max(1) as f64;
    let pct_bytes1 = 100.0 * common_bytes as f64 / stats1.total_bytes.max(1) as f64;
    let pct_bytes2 = 100.0 * common_bytes as f64 / stats2.total_bytes.max(1) as f64;

    println!("=== Overlap ===");
    println!("  Common unique extents: {}", common);
    println!(
        "  {:.1}% of image1's unique extents  /  {:.1}% of image2's",
        pct_extents1, pct_extents2
    );
    println!(
        "  Bytes covered:  {:.1} MB  ({:.1}% of image1  /  {:.1}% of image2)",
        common_bytes as f64 / (1024.0 * 1024.0),
        pct_bytes1,
        pct_bytes2,
    );
}

// --- delta analysis (path-based) ---

const MB: f64 = 1024.0 * 1024.0;
const BLOCK_BYTES: usize = 4096;

/// Walk an ext4 image and return a map of path → (blake3 hash of full file, byte count).
fn enumerate_file_hashes(image: &Path) -> io::Result<HashMap<String, (blake3::Hash, u64)>> {
    let fs = Ext4::load_from_path(image).map_err(|e| io::Error::other(e.to_string()))?;
    let mut out = HashMap::new();
    let mut queue: Vec<Ext4PathBuf> = vec![Ext4PathBuf::new("/")];

    while let Some(dir) = queue.pop() {
        let entries = match fs.read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries {
            let entry: DirEntry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let path = entry.path();
            let metadata: Metadata = match fs.symlink_metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if metadata.file_type().is_dir() {
                queue.push(path);
            } else if metadata.file_type().is_regular_file() {
                let data = match fs.read(&path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let hash = blake3::hash(&data);
                let bytes = data.len() as u64;
                out.insert(path.to_str().unwrap_or("").to_string(), (hash, bytes));
            }
        }
    }

    Ok(out)
}

fn analyse_delta(image1: &Path, image2: &Path, level: i32) -> io::Result<()> {
    // Pass 1: enumerate all file hashes for both images.
    let hashes1 = enumerate_file_hashes(image1)?;
    let hashes2 = enumerate_file_hashes(image2)?;

    let total_bytes2: u64 = hashes2.values().map(|&(_, b)| b).sum();

    let mut exact_count = 0usize;
    let mut exact_bytes = 0u64;
    let mut changed_paths: Vec<String> = Vec::new();
    let mut new_bytes = 0u64;
    let mut removed_bytes = 0u64;

    for (path, &(h2, b2)) in &hashes2 {
        match hashes1.get(path) {
            Some(&(h1, _)) if h1 == h2 => {
                exact_count += 1;
                exact_bytes += b2;
            }
            Some(_) => {
                changed_paths.push(path.clone());
            }
            None => {
                new_bytes += b2;
            }
        }
    }
    for (path, &(_, b1)) in &hashes1 {
        if !hashes2.contains_key(path) {
            removed_bytes += b1;
        }
    }

    // Pass 2: measure delta compression on changed files.
    let fs1 = Ext4::load_from_path(image1).map_err(|e| io::Error::other(e.to_string()))?;
    let fs2 = Ext4::load_from_path(image2).map_err(|e| io::Error::other(e.to_string()))?;

    let mut raw_changed = 0u64;
    let mut standalone_total = 0u64;
    let mut dict_total = 0u64;
    let mut buckets = [0usize; 5];

    for path in &changed_paths {
        let ext4_path = Ext4PathBuf::new(path);
        let data1 = match fs1.read(&ext4_path) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let data2 = match fs2.read(&ext4_path) {
            Ok(d) => d,
            Err(_) => continue,
        };

        raw_changed += data2.len() as u64;

        let standalone = zstd::bulk::compress(&data2, level).unwrap_or_default();
        let dict_compressed = zstd::bulk::Compressor::with_dictionary(level, &data1)
            .ok()
            .and_then(|mut c| c.compress(&data2).ok())
            .unwrap_or_else(|| standalone.clone());

        let s = standalone.len();
        let d = dict_compressed.len();
        standalone_total += s as u64;
        dict_total += d as u64;

        let saving_pct = if s > 0 {
            100.0 * (1.0 - d as f64 / s as f64)
        } else {
            0.0
        };
        buckets[((saving_pct / 20.0) as usize).min(4)] += 1;
    }

    let marginal_cost = dict_total + new_bytes;

    println!("=== Delta Analysis ===");
    println!(
        "  Exact match:   {:>6} files  {:>7.1} MB ({:.1}%)  — zero marginal cost",
        exact_count,
        exact_bytes as f64 / MB,
        100.0 * exact_bytes as f64 / total_bytes2.max(1) as f64,
    );
    println!(
        "  Changed:       {:>6} files  {:>7.1} MB raw",
        changed_paths.len(),
        raw_changed as f64 / MB,
    );
    if !changed_paths.is_empty() {
        println!(
            "    Standalone compressed:  {:.1} MB",
            standalone_total as f64 / MB,
        );
        println!(
            "    Dict compressed:       {:.1} MB  ({:.1}% reduction vs standalone)",
            dict_total as f64 / MB,
            100.0 * (standalone_total as f64 - dict_total as f64) / standalone_total.max(1) as f64,
        );
    }
    println!(
        "  New (img2 only):        {:>7.1} MB  — full store",
        new_bytes as f64 / MB,
    );
    println!(
        "  Removed (img1 only):    {:>7.1} MB",
        removed_bytes as f64 / MB,
    );
    println!(
        "\n  Marginal cost: {:.1} MB of {:.1} MB total ({:.1}% saving vs full fetch)",
        marginal_cost as f64 / MB,
        total_bytes2 as f64 / MB,
        100.0 * (1.0 - marginal_cost as f64 / total_bytes2.max(1) as f64),
    );

    if !changed_paths.is_empty() {
        println!("\n  Delta benefit distribution (dict vs standalone):");
        let labels = [
            "worse/same  (0–20%)",
            "modest     (20–40%)",
            "good       (40–60%)",
            "great      (60–80%)",
            "excellent  (80–100%)",
        ];
        let n = changed_paths.len() as f64;
        for (i, &count) in buckets.iter().enumerate() {
            let pct = 100.0 * count as f64 / n;
            let bar: String = "#".repeat((pct / 2.0) as usize);
            println!("    {}  {:>6} ({:>5.1}%)  {}", labels[i], count, pct, bar);
        }
    }

    Ok(())
}

// --- located extents (for NBD read tracking) ---

/// An extent with its disk byte offset, for mapping NBD reads back to extents.
pub struct LocatedExtent {
    pub start_byte: u64,
    pub byte_count: u64,
    pub hash: blake3::Hash,
}

/// Scan all regular-file extents and return them sorted by disk byte offset.
pub fn locate_extents(image: &Path) -> io::Result<Vec<LocatedExtent>> {
    let mut f = File::open(image)?;
    let sb = Superblock::read(&mut f)?;
    let mut results = Vec::new();
    let mut inode_buf = vec![0u8; sb.inode_size];

    for group in 0..sb.num_block_groups {
        let table_block = inode_table_block(&mut f, &sb, group)?;
        let table_offset = table_block * sb.block_size;

        for idx in 0..sb.inodes_per_group {
            let inode_offset = table_offset + idx as u64 * sb.inode_size as u64;
            f.seek(SeekFrom::Start(inode_offset))?;
            if f.read_exact(&mut inode_buf).is_err() {
                break;
            }

            let i_mode = u16le(&inode_buf, 0x00);
            if (i_mode & S_IFMT) != S_IFREG {
                continue;
            }
            if u16le(&inode_buf, 0x1a) == 0 {
                continue;
            }
            let i_flags = u32le(&inode_buf, 0x20);
            if (i_flags & INODE_FLAG_EXTENTS) == 0 {
                continue;
            }
            let i_size = hilo64(u32le(&inode_buf, 0x6c), u32le(&inode_buf, 0x04));
            if i_size == 0 {
                continue;
            }

            let i_block = inode_buf[0x28..0x28 + 60].to_vec();
            let mut raw_extents: Vec<(u64, u16)> = Vec::new();
            collect_extents(&i_block, &mut f, &sb, &mut raw_extents)?;

            let mut bytes_remaining = i_size;
            for (start_block, num_blocks) in raw_extents {
                if bytes_remaining == 0 {
                    break;
                }
                let allocated = num_blocks as u64 * sb.block_size;
                let byte_count = allocated.min(bytes_remaining);
                bytes_remaining = bytes_remaining.saturating_sub(byte_count);

                let start_byte = start_block * sb.block_size;
                let mut buf = vec![0u8; byte_count as usize];
                f.seek(SeekFrom::Start(start_byte))?;
                f.read_exact(&mut buf)?;
                results.push(LocatedExtent {
                    start_byte,
                    byte_count,
                    hash: blake3::hash(&buf),
                });
            }
        }
    }

    results.sort_by_key(|e| e.start_byte);
    Ok(results)
}

// --- entry point ---

pub fn run(image1: &Path, image2: Option<&Path>, level: i32) -> io::Result<()> {
    println!("Scanning extents in {} ...", image1.display());
    let (entries1, _) = scan_inode(image1)?;
    let stats1 = build_stats(&entries1);
    print_stats(&image1.display().to_string(), &stats1);

    if let Some(img2) = image2 {
        println!("Scanning extents in {} ...", img2.display());
        let (entries2, _) = scan_inode(img2)?;
        let stats2 = build_stats(&entries2);
        print_stats(&img2.display().to_string(), &stats2);
        print_overlap(&stats1, &stats2);
        println!();
        analyse_delta(image1, img2, level)?;
    }

    Ok(())
}

fn scan_inode(path: &Path) -> io::Result<(Vec<ExtentEntry>, u64)> {
    let mut f = File::open(path)?;
    let sb = Superblock::read(&mut f)?;
    let block_size = sb.block_size;
    let entries = scan_file_extents(&mut f, &sb)?;
    Ok((entries, block_size))
}

// --- rename analysis ---

pub fn run_rename_analysis(image1: &Path, image2: &Path) -> io::Result<()> {
    println!("Scanning image1 ...");
    let hashes1 = enumerate_file_hashes(image1)?;
    println!("Scanning image2 ...");
    let hashes2 = enumerate_file_hashes(image2)?;

    // Invert image1: hash → path (for exact-rename detection)
    let mut hash_to_path1: HashMap<blake3::Hash, String> = HashMap::new();
    for (path, (hash, _)) in &hashes1 {
        hash_to_path1.entry(*hash).or_insert_with(|| path.clone());
    }

    // Invert image1: size → paths (for size-matched candidates)
    let mut size_to_paths1: HashMap<u64, Vec<String>> = HashMap::new();
    for (path, (_, size)) in &hashes1 {
        size_to_paths1.entry(*size).or_default().push(path.clone());
    }

    let mut exact_renames: Vec<(String, String, u64)> = Vec::new(); // (old, new, bytes)
    let mut size_candidates: Vec<(String, u64)> = Vec::new(); // (new_path, size)
    let mut genuinely_new: Vec<(String, u64)> = Vec::new();

    for (path2, (hash2, size2)) in &hashes2 {
        if hashes1.contains_key(path2) {
            // Path exists in both — not a rename
            continue;
        }
        // Path is new in image2. Check for exact rename.
        if let Some(old_path) = hash_to_path1.get(hash2) {
            exact_renames.push((old_path.clone(), path2.clone(), *size2));
            continue;
        }
        // Check for size match — rename + modification candidate
        if let Some(candidates) = size_to_paths1.get(size2)
            && !candidates.is_empty()
            && *size2 > 0
        {
            size_candidates.push((path2.clone(), *size2));
            continue;
        }
        genuinely_new.push((path2.clone(), *size2));
    }

    // Files removed in image2 (present in image1 but not image2, not accounted for as rename source)
    let renamed_away: std::collections::HashSet<String> = exact_renames
        .iter()
        .map(|(old, _, _)| old.clone())
        .collect();
    let removed: Vec<(&String, u64)> = hashes1
        .iter()
        .filter(|(path, _)| !hashes2.contains_key(*path) && !renamed_away.contains(*path))
        .map(|(path, (_, size))| (path, *size))
        .collect();

    let exact_bytes: u64 = exact_renames.iter().map(|(_, _, b)| b).sum();
    let candidate_bytes: u64 = size_candidates.iter().map(|(_, b)| b).sum();
    let new_bytes: u64 = genuinely_new.iter().map(|(_, b)| b).sum();

    println!("\n=== Rename Analysis ===");
    println!("  image1: {}", image1.display());
    println!("  image2: {}", image2.display());
    println!();
    println!("  New paths in image2 (not present in image1 by path):");
    println!(
        "    Exact renames (same content, different path): {:>5} files  {:>7.1} MB",
        exact_renames.len(),
        exact_bytes as f64 / MB
    );
    println!(
        "    Size-matched (rename+modify candidates):      {:>5} files  {:>7.1} MB",
        size_candidates.len(),
        candidate_bytes as f64 / MB
    );
    println!(
        "    Genuinely new (no size match in image1):      {:>5} files  {:>7.1} MB",
        genuinely_new.len(),
        new_bytes as f64 / MB
    );
    println!();
    println!(
        "  Removed paths in image2 (not accounted for as rename source): {:>5} files",
        removed.len()
    );

    if !exact_renames.is_empty() {
        println!("\n  Exact renames:");
        let mut sorted = exact_renames.clone();
        sorted.sort_by_key(|t| std::cmp::Reverse(t.2));
        for (old, new, bytes) in sorted.iter().take(20) {
            println!("    {:>7.1} KB  {} → {}", *bytes as f64 / 1024.0, old, new);
        }
        if sorted.len() > 20 {
            println!("    ... and {} more", sorted.len() - 20);
        }
    }

    if !size_candidates.is_empty() {
        println!("\n  Size-matched candidates (rename+modify — need similarity check to confirm):");
        let mut sorted = size_candidates.clone();
        sorted.sort_by_key(|t| std::cmp::Reverse(t.1));
        for (path, bytes) in sorted.iter().take(20) {
            println!("    {:>7.1} KB  {}", *bytes as f64 / 1024.0, path);
        }
        if sorted.len() > 20 {
            println!("    ... and {} more", sorted.len() - 20);
        }
    }

    Ok(())
}

// --- sparse analysis ---

/// Compare two images at 4KB block granularity to measure sparse-strategy savings.
///
/// For each file that changed between image1 and image2, count how many 4KB blocks
/// actually differ. This shows how effective the sparse fetch strategy would be
/// compared to re-uploading entire changed extents.
pub fn run_sparse_analysis(image1: &Path, image2: &Path) -> io::Result<()> {
    let hashes1 = enumerate_file_hashes(image1)?;
    let hashes2 = enumerate_file_hashes(image2)?;

    let total_bytes2: u64 = hashes2.values().map(|&(_, b)| b).sum();

    let fs1 = Ext4::load_from_path(image1).map_err(|e| io::Error::other(e.to_string()))?;
    let fs2 = Ext4::load_from_path(image2).map_err(|e| io::Error::other(e.to_string()))?;

    let mut exact_count = 0usize;
    let mut exact_bytes = 0u64;
    let mut changed_count = 0usize;
    let mut changed_raw_bytes = 0u64;
    let mut total_blocks = 0u64;
    let mut changed_blocks_total = 0u64;
    let mut new_count = 0usize;
    let mut new_bytes = 0u64;
    // 5 buckets: 0–20%, 20–40%, 40–60%, 60–80%, 80–100% blocks changed
    let mut buckets = [0usize; 5];

    for (path, &(h2, b2)) in &hashes2 {
        match hashes1.get(path) {
            Some(&(h1, _)) if h1 == h2 => {
                exact_count += 1;
                exact_bytes += b2;
            }
            Some(_) => {
                let ext4_path = Ext4PathBuf::new(path);
                let data2 = match fs2.read(&ext4_path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let data1 = match fs1.read(&ext4_path) {
                    Ok(d) => d,
                    Err(_) => {
                        // File present in both by path but unreadable from image1 — treat as new.
                        new_count += 1;
                        new_bytes += b2;
                        continue;
                    }
                };

                let file_blocks = b2.div_ceil(BLOCK_BYTES as u64) as usize;
                let mut file_changed = 0usize;

                for i in 0..file_blocks {
                    let start = i * BLOCK_BYTES;
                    let end2 = (start + BLOCK_BYTES).min(data2.len());
                    let block2 = &data2[start..end2];

                    // A block is unchanged only if the same byte range in image1 is identical.
                    // Blocks past the end of data1 (file grew) are always changed.
                    let identical = if start < data1.len() {
                        let end1 = (start + BLOCK_BYTES).min(data1.len());
                        data1[start..end1] == *block2
                    } else {
                        false
                    };

                    if !identical {
                        file_changed += 1;
                    }
                }

                total_blocks += file_blocks as u64;
                changed_blocks_total += file_changed as u64;

                let frac = file_changed as f64 / file_blocks.max(1) as f64;
                buckets[((frac * 5.0) as usize).min(4)] += 1;

                changed_count += 1;
                changed_raw_bytes += b2;
            }
            None => {
                new_count += 1;
                new_bytes += b2;
            }
        }
    }

    let mut removed_count = 0usize;
    let mut removed_bytes = 0u64;
    for (path, &(_, b)) in &hashes1 {
        if !hashes2.contains_key(path) {
            removed_count += 1;
            removed_bytes += b;
        }
    }

    let unchanged_blocks = total_blocks - changed_blocks_total;
    // Each changed block is at most BLOCK_BYTES; cap at actual raw changed file bytes
    // to avoid overcounting the last partial block per file.
    let changed_block_bytes = (changed_blocks_total * BLOCK_BYTES as u64).min(changed_raw_bytes);
    let marginal_sparse = changed_block_bytes + new_bytes;
    let marginal_full = changed_raw_bytes + new_bytes;

    println!("=== Sparse Analysis ===");
    println!("  image1: {}", image1.display());
    println!("  image2: {}", image2.display());
    println!();
    println!(
        "  Total image2 file data: {:.1} MB",
        total_bytes2 as f64 / MB
    );
    println!();
    println!(
        "  Exact match (zero cost):   {:>6} files  {:>7.1} MB  ({:.1}%)",
        exact_count,
        exact_bytes as f64 / MB,
        100.0 * exact_bytes as f64 / total_bytes2.max(1) as f64,
    );
    println!(
        "  Changed (sparse applies):  {:>6} files  {:>7.1} MB raw  ({:.1}%)",
        changed_count,
        changed_raw_bytes as f64 / MB,
        100.0 * changed_raw_bytes as f64 / total_bytes2.max(1) as f64,
    );
    println!(
        "  New (image2 only):         {:>6} files  {:>7.1} MB  (full upload)",
        new_count,
        new_bytes as f64 / MB,
    );
    println!(
        "  Removed (image1 only):     {:>6} files  {:>7.1} MB",
        removed_count,
        removed_bytes as f64 / MB,
    );

    if changed_count > 0 {
        println!();
        println!("  Block-level breakdown within changed files:");
        println!(
            "    Total 4KB blocks: {:>8}  ({:.1} MB)",
            total_blocks,
            total_blocks as f64 * BLOCK_BYTES as f64 / MB,
        );
        println!(
            "    Unchanged (skip): {:>8}  ({:.1} MB,  {:.1}%)",
            unchanged_blocks,
            unchanged_blocks as f64 * BLOCK_BYTES as f64 / MB,
            100.0 * unchanged_blocks as f64 / total_blocks.max(1) as f64,
        );
        println!(
            "    Changed (upload): {:>8}  ({:.1} MB,  {:.1}%)",
            changed_blocks_total,
            changed_block_bytes as f64 / MB,
            100.0 * changed_blocks_total as f64 / total_blocks.max(1) as f64,
        );
        println!(
            "    Sparse saving vs full re-upload of changed files: {:.1}%",
            100.0 * (1.0 - changed_block_bytes as f64 / changed_raw_bytes.max(1) as f64),
        );

        println!();
        println!("  Change concentration (fraction of 4KB blocks changed per file):");
        let labels = [
            "0–20%   (highly sparse)",
            "20–40%                ",
            "40–60%                ",
            "60–80%                ",
            "80–100% (mostly changed)",
        ];
        let n = changed_count as f64;
        for (i, &count) in buckets.iter().enumerate() {
            let pct = 100.0 * count as f64 / n;
            let bar: String = "#".repeat((pct / 2.0) as usize);
            println!("    {}:  {:>6} ({:>5.1}%)  {}", labels[i], count, pct, bar);
        }
    }

    println!();
    println!("  Marginal S3 upload cost (new files + sparse changed blocks):");
    println!(
        "    Sparse:            {:.1} MB  ({:.1}% saving vs naive full image fetch)",
        marginal_sparse as f64 / MB,
        100.0 * (1.0 - marginal_sparse as f64 / total_bytes2.max(1) as f64),
    );
    println!(
        "    Full (no sparse):  {:.1} MB  ({:.1}% saving vs naive full image fetch)",
        marginal_full as f64 / MB,
        100.0 * (1.0 - marginal_full as f64 / total_bytes2.max(1) as f64),
    );

    Ok(())
}

// --- sparse cold-boot analysis ---

/// Per-boot-extent analysis: for each extent read during boot that changed between
/// image1 and image2, measure fetch cost under four strategies:
/// zstd-only, zstd+sparse (skip unchanged 4KB blocks), zstd+delta (image1 file as dict),
/// and zstd+delta+sparse (changed blocks only, image1 file as dict).
pub fn run_cold_boot(
    image1: &Path,
    image2: &Path,
    trace_path: &Path,
    level: i32,
) -> io::Result<()> {
    println!("Loading boot trace from {} ...", trace_path.display());
    let trace = load_trace(trace_path)?;
    println!("  {} extents in trace", trace.len());

    println!("Building image2 extent → (path, file-logical offset) map ...");
    let extent_to_file2 = build_extent_logical_map(image2)?;

    println!("Scanning image2 total file size ...");
    let total_image2_bytes: u64 = {
        let mut f = File::open(image2)?;
        let sb = Superblock::read(&mut f)?;
        scan_file_extents(&mut f, &sb)?
            .iter()
            .map(|e| e.byte_count)
            .sum()
    };

    println!("Scanning image1 extent hashes ...");
    let image1_hashes: std::collections::HashSet<blake3::Hash> = {
        let mut f = File::open(image1)?;
        let sb = Superblock::read(&mut f)?;
        scan_file_extents(&mut f, &sb)?
            .iter()
            .map(|e| e.hash)
            .collect()
    };

    println!("Loading image1 and image2 filesystems ...");
    let fs1 = Ext4::load_from_path(image1).map_err(|e| io::Error::other(e.to_string()))?;
    let fs2 = Ext4::load_from_path(image2).map_err(|e| io::Error::other(e.to_string()))?;
    // Raw image2 file for reading new/unmapped extents by disk offset.
    let mut image2_raw = File::open(image2)?;

    let mut exact_count = 0usize;
    let mut exact_bytes = 0u64;
    let mut changed_count = 0usize;
    let mut changed_raw_bytes = 0u64;
    let mut changed_block_bytes = 0u64; // raw bytes in changed 4KB blocks
    let mut changed_standalone_bytes = 0u64; // full extent, standalone zstd
    let mut changed_zstd_bytes = 0u64; // changed blocks only, standalone zstd
    let mut changed_delta_bytes = 0u64; // full extent, image1 file as dict
    let mut changed_delta_sparse_bytes = 0u64; // changed blocks only, image1 file as dict
    let mut new_count = 0usize;
    let mut new_bytes = 0u64;
    let mut new_zstd_bytes = 0u64;
    let mut unmapped = 0usize;
    let mut total_boot_bytes = 0u64;
    // 5 buckets: 0–20%, 20–40%, 40–60%, 60–80%, 80–100% blocks changed within extent
    let mut buckets = [0usize; 5];

    println!("Classifying {} boot extents ...", trace.len());
    for entry in &trace {
        if image1_hashes.contains(&entry.hash) {
            exact_count += 1;
            exact_bytes += entry.byte_count;
            total_boot_bytes += entry.byte_count;
            continue;
        }

        let Some((path, file_logical_offset, byte_count)) = extent_to_file2.get(&entry.hash) else {
            unmapped += 1;
            continue;
        };

        total_boot_bytes += entry.byte_count;

        // New extents: read raw bytes from disk, compress for the zstd baseline.
        let compress_new = |raw: &mut File, start: u64, len: u64| -> u64 {
            let mut buf = vec![0u8; len as usize];
            if raw.seek(SeekFrom::Start(start)).is_err() {
                return len;
            }
            if raw.read_exact(&mut buf).is_err() {
                return len;
            }
            let compressed = zstd::bulk::compress(&buf, level).unwrap_or_default();
            compressed.len().min(buf.len()) as u64
        };

        if path.is_empty() {
            new_count += 1;
            new_bytes += byte_count;
            new_zstd_bytes += compress_new(&mut image2_raw, entry.start_byte, *byte_count);
            continue;
        }

        let ext4_path = Ext4PathBuf::new(path);
        let data2 = match fs2.read(&ext4_path) {
            Ok(d) => d,
            Err(_) => {
                new_count += 1;
                new_bytes += byte_count;
                new_zstd_bytes += compress_new(&mut image2_raw, entry.start_byte, *byte_count);
                continue;
            }
        };
        let data1 = match fs1.read(&ext4_path) {
            Ok(d) => d,
            Err(_) => {
                // Genuinely new file in image2.
                new_count += 1;
                new_bytes += byte_count;
                new_zstd_bytes += compress_new(&mut image2_raw, entry.start_byte, *byte_count);
                continue;
            }
        };

        // Slice out the specific extent range within the file.
        let fstart = *file_logical_offset as usize;
        if fstart >= data2.len() {
            // Preallocated extent past i_size that slipped through — treat as new.
            new_count += 1;
            new_bytes += byte_count;
            new_zstd_bytes += compress_new(&mut image2_raw, entry.start_byte, *byte_count);
            continue;
        }
        let fend2 = (fstart + *byte_count as usize).min(data2.len());
        let slice2 = &data2[fstart..fend2];
        let slice1 = if fstart < data1.len() {
            let fend1 = (fstart + *byte_count as usize).min(data1.len());
            &data1[fstart..fend1]
        } else {
            &[][..]
        };

        let num_blocks = slice2.len().div_ceil(BLOCK_BYTES);
        let mut file_changed = 0usize;
        // Collect changed block bytes to compress together (best case for zstd+sparse).
        let mut changed_block_data: Vec<u8> = Vec::new();

        for i in 0..num_blocks {
            let bs = i * BLOCK_BYTES;
            let be2 = (bs + BLOCK_BYTES).min(slice2.len());
            let block2 = &slice2[bs..be2];
            // image1 may be shorter (file grew); blocks past the old EOF are always changed.
            let block1 = if bs < slice1.len() {
                let be1 = (bs + BLOCK_BYTES).min(slice1.len());
                &slice1[bs..be1]
            } else {
                &[][..]
            };
            if block1 != block2 {
                file_changed += 1;
                changed_block_data.extend_from_slice(block2);
            }
        }

        let this_block_bytes = (file_changed as u64 * BLOCK_BYTES as u64).min(*byte_count);

        // Helper: try dict compression, fall back to standalone if it fails or is larger.
        let dict_compress = |data: &[u8]| -> u64 {
            let c = zstd::bulk::Compressor::with_dictionary(level, &data1)
                .ok()
                .and_then(|mut c| c.compress(data).ok())
                .unwrap_or_else(|| zstd::bulk::compress(data, level).unwrap_or_default());
            c.len().min(data.len()) as u64
        };

        // (1) zstd-only: full extent, standalone.
        let standalone_full = zstd::bulk::compress(slice2, level).unwrap_or_default();
        let this_standalone = standalone_full.len().min(slice2.len()) as u64;

        // (2) zstd+sparse: changed blocks only, standalone.
        let this_sparse_zstd = if changed_block_data.is_empty() {
            0
        } else {
            let c = zstd::bulk::compress(&changed_block_data, level).unwrap_or_default();
            c.len().min(changed_block_data.len()) as u64
        };

        // (3) zstd+delta: full extent with image1 file as dict.
        let this_delta = dict_compress(slice2);

        // (4) zstd+delta+sparse: changed blocks with image1 file as dict.
        let this_delta_sparse = if changed_block_data.is_empty() {
            0
        } else {
            dict_compress(&changed_block_data)
        };

        let frac = file_changed as f64 / num_blocks.max(1) as f64;
        buckets[((frac * 5.0) as usize).min(4)] += 1;

        changed_count += 1;
        changed_raw_bytes += byte_count;
        changed_block_bytes += this_block_bytes;
        changed_standalone_bytes += this_standalone;
        changed_zstd_bytes += this_sparse_zstd;
        changed_delta_bytes += this_delta;
        changed_delta_sparse_bytes += this_delta_sparse;
    }

    let warm_zstd_only = changed_standalone_bytes + new_zstd_bytes;
    let warm_zstd_sparse = changed_zstd_bytes + new_zstd_bytes;
    let warm_zstd_delta = changed_delta_bytes + new_zstd_bytes;
    let warm_zstd_delta_sparse = changed_delta_sparse_bytes + new_zstd_bytes;
    let cold_zstd_only = exact_bytes + changed_standalone_bytes + new_zstd_bytes;
    let cold_zstd_sparse = exact_bytes + changed_zstd_bytes + new_zstd_bytes;
    let cold_zstd_delta = exact_bytes + changed_delta_bytes + new_zstd_bytes;
    let cold_zstd_delta_sparse = exact_bytes + changed_delta_sparse_bytes + new_zstd_bytes;
    let boot_extents = trace.len() - unmapped;

    println!("\n=== Sparse Cold Boot Analysis ===");
    println!("  image1: {}", image1.display());
    println!("  image2: {}", image2.display());
    println!("  trace:  {}", trace_path.display());
    println!();
    println!(
        "  Boot footprint:      {:>5} extents   {:>7.1} MB  ({:.1}% of {:.1} MB total image)",
        boot_extents,
        total_boot_bytes as f64 / MB,
        100.0 * total_boot_bytes as f64 / total_image2_bytes.max(1) as f64,
        total_image2_bytes as f64 / MB,
    );
    if unmapped > 0 {
        println!(
            "  Unmapped reads:      {:>5} extents             (metadata/journal — excluded)",
            unmapped,
        );
    }
    println!();
    println!(
        "  Exact match (dedup): {:>5} extents   {:>7.1} MB  ({:.1}% of boot)",
        exact_count,
        exact_bytes as f64 / MB,
        100.0 * exact_bytes as f64 / total_boot_bytes.max(1) as f64,
    );
    if changed_count > 0 {
        let unchanged_block_bytes = changed_raw_bytes.saturating_sub(changed_block_bytes);
        println!(
            "  Changed extents:     {:>5} extents   {:>7.1} MB raw  ({:.1}% of blocks unchanged)",
            changed_count,
            changed_raw_bytes as f64 / MB,
            100.0 * unchanged_block_bytes as f64 / changed_raw_bytes.max(1) as f64,
        );

        println!();
        println!("  Change concentration (fraction of 4KB blocks changed per boot extent):");
        let labels = [
            "0–20%   (highly sparse)",
            "20–40%                ",
            "40–60%                ",
            "60–80%                ",
            "80–100% (mostly changed)",
        ];
        let n = changed_count as f64;
        for (i, &count) in buckets.iter().enumerate() {
            let pct = 100.0 * count as f64 / n;
            let bar: String = "#".repeat((pct / 2.0) as usize);
            println!("    {}:  {:>5} ({:>5.1}%)  {}", labels[i], count, pct, bar);
        }
    }
    println!(
        "  New (no prior):      {:>5} extents   {:>7.1} MB raw  →  {:.1} MB zstd",
        new_count,
        new_bytes as f64 / MB,
        new_zstd_bytes as f64 / MB,
    );

    println!();
    println!("  Fetch cost — warm host (deduped extents already in local SSD cache):");
    println!("    Strategy            MB      vs boot fetch");
    println!(
        "    zstd-only         {:>6.1}    ({:.1}% reduction)",
        warm_zstd_only as f64 / MB,
        100.0 * (1.0 - warm_zstd_only as f64 / total_boot_bytes.max(1) as f64),
    );
    println!(
        "    zstd+sparse       {:>6.1}    ({:.1}% reduction)",
        warm_zstd_sparse as f64 / MB,
        100.0 * (1.0 - warm_zstd_sparse as f64 / total_boot_bytes.max(1) as f64),
    );
    println!(
        "    zstd+delta        {:>6.1}    ({:.1}% reduction)",
        warm_zstd_delta as f64 / MB,
        100.0 * (1.0 - warm_zstd_delta as f64 / total_boot_bytes.max(1) as f64),
    );
    println!(
        "    zstd+delta+sparse {:>6.1}    ({:.1}% reduction)",
        warm_zstd_delta_sparse as f64 / MB,
        100.0 * (1.0 - warm_zstd_delta_sparse as f64 / total_boot_bytes.max(1) as f64),
    );
    println!();
    println!("  Fetch cost — cold host (no local cache, fetch all from S3):");
    println!("    Strategy            MB      vs naive full image");
    println!(
        "    zstd-only         {:>6.1}    ({:.1}% reduction)",
        cold_zstd_only as f64 / MB,
        100.0 * (1.0 - cold_zstd_only as f64 / total_image2_bytes.max(1) as f64),
    );
    println!(
        "    zstd+sparse       {:>6.1}    ({:.1}% reduction)",
        cold_zstd_sparse as f64 / MB,
        100.0 * (1.0 - cold_zstd_sparse as f64 / total_image2_bytes.max(1) as f64),
    );
    println!(
        "    zstd+delta        {:>6.1}    ({:.1}% reduction)",
        cold_zstd_delta as f64 / MB,
        100.0 * (1.0 - cold_zstd_delta as f64 / total_image2_bytes.max(1) as f64),
    );
    println!(
        "    zstd+delta+sparse {:>6.1}    ({:.1}% reduction)",
        cold_zstd_delta_sparse as f64 / MB,
        100.0 * (1.0 - cold_zstd_delta_sparse as f64 / total_image2_bytes.max(1) as f64),
    );

    Ok(())
}
