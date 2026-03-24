// Walks an ext4 image's inode table to find all regular file extents,
// hashes each extent's content, and reports dedup statistics.
//
// Ported from ext4-view (MIT OR Apache-2.0), Google LLC 2024.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use ext4_view::{DirEntry, Ext4, Metadata};
use ext4_view::PathBuf as Ext4PathBuf;

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
        let num_block_groups =
            ((num_data_blocks + blocks_per_group as u64 - 1) / blocks_per_group as u64) as u32;

        Ok(Self { block_size, inode_size, inodes_per_group, num_block_groups, is_64bit })
    }

    fn bgdt_start(&self) -> u64 {
        // Block group descriptor table starts at block 1 (or 2 if block_size==1024).
        if self.block_size == 1024 { 2 * self.block_size } else { self.block_size }
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
    let hi = if sb.is_64bit { u32le(&buf, 0x28) as u64 } else { 0 };
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
    let dedup_ratio = if unique > 0 { stats.extent_count as f64 / unique as f64 } else { 1.0 };

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
    let common = stats1.hash_to_bytes.keys().filter(|h| stats2.hash_to_bytes.contains_key(h)).count();

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

/// Walk an ext4 image and return a map of path → (blake3 hash of full file, byte count).
fn enumerate_file_hashes(image: &Path) -> io::Result<HashMap<String, (blake3::Hash, u64)>> {
    let fs = Ext4::load_from_path(image)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
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
    let fs1 = Ext4::load_from_path(image1)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let fs2 = Ext4::load_from_path(image2)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

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

        let saving_pct = if s > 0 { 100.0 * (1.0 - d as f64 / s as f64) } else { 0.0 };
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
