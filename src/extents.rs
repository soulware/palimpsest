// Walks an ext4 image's inode table to find all regular file extents,
// hashes each extent's content, and reports dedup statistics.
//
// Ported from ext4-view (MIT OR Apache-2.0), Google LLC 2024.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

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

// Reads every regular-file inode in the image and yields its extents
// (each as a content hash).  Returns a vec of (hash, byte_count) pairs.
fn scan_file_extents(f: &mut File, sb: &Superblock) -> io::Result<Vec<(blake3::Hash, u64)>> {
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
                Err(_) => break, // past end of image
            }

            // Skip non-regular files and deleted inodes.
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

            // Parse extent tree rooted at i_block (bytes 0x28..0x64).
            let i_block = &inode_buf[0x28..0x28 + 60].to_vec();
            let mut extents: Vec<(u64, u16)> = Vec::new();
            collect_extents(i_block, f, sb, &mut extents)?;

            if extents.is_empty() {
                continue;
            }

            // Hash each physical extent's content independently.
            // Trim the last extent to the actual file size.
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

                let hash = blake3::hash(&block_buf);
                results.push((hash, to_read));
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

// --- entry point ---

pub fn run(image1: &Path, image2: Option<&Path>) -> io::Result<()> {
    println!("Scanning extents in {} ...", image1.display());
    let mut stats1 = ExtentStats::default();
    for (hash, bytes) in scan_inode(image1)? {
        stats1.record(hash, bytes);
    }
    print_stats(&image1.display().to_string(), &stats1);

    if let Some(img2) = image2 {
        println!("Scanning extents in {} ...", img2.display());
        let mut stats2 = ExtentStats::default();
        for (hash, bytes) in scan_inode(img2)? {
            stats2.record(hash, bytes);
        }
        print_stats(&img2.display().to_string(), &stats2);
        print_overlap(&stats1, &stats2);
    }

    Ok(())
}

fn scan_inode(path: &Path) -> io::Result<Vec<(blake3::Hash, u64)>> {
    let mut f = File::open(path)?;
    let sb = Superblock::read(&mut f)?;
    scan_file_extents(&mut f, &sb)
}
