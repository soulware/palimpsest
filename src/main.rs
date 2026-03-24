use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use clap::{Parser, Subcommand};
use ext4_view::{DirEntry, Ext4, Ext4Error, Metadata, PathBuf as Ext4PathBuf};
#[allow(unused_imports)]
use ext4_view::FileType;

mod delta;
mod extents;
mod manifest;
mod nbd;
mod similar;
mod snapshot;

const DEFAULT_CHUNK_KB: usize = 32;
const DEFAULT_ENTROPY_THRESHOLD: f64 = 7.0;

/// Analyse raw disk images at the chunk level to understand dedup potential.
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Chunk a raw disk image at fixed offsets (block-level)
    Blocks {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
        #[arg(long, default_value_t = DEFAULT_ENTROPY_THRESHOLD)]
        entropy_threshold: f64,
    },
    /// Walk an ext4 filesystem image, chunk by file contents
    Files {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
        #[arg(long, default_value_t = DEFAULT_ENTROPY_THRESHOLD)]
        entropy_threshold: f64,
    },
    /// Serve a raw ext4 image over NBD, tracking which chunks are read
    Serve {
        image: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
    },
    /// Extract kernel and initrd from an ext4 image's /boot directory
    ExtractBoot {
        image: String,
        #[arg(long, default_value = ".")]
        out_dir: String,
    },
    /// Generate a binary snapshot from an ext4 image
    Snapshot {
        image: String,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
        #[arg(long)]
        parent: Option<String>,
        #[arg(long, default_value = "snapshots")]
        out_dir: String,
    },
    /// Print metadata from a binary snapshot file
    SnapshotInfo {
        snapshot: String,
    },
    /// Diff two binary snapshots
    SnapshotDiff {
        snapshot1: String,
        snapshot2: String,
    },
    /// Generate a chunk manifest from an ext4 image
    Manifest {
        image: String,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
        #[arg(long, default_value = "manifest.tsv")]
        out: String,
    },
    /// Diff two chunk manifests, reporting what changed between snapshots
    Diff {
        manifest1: String,
        manifest2: String,
        #[arg(long)]
        verbose: bool,
    },
    /// Find similar (non-identical) chunks within one or two images using MinHash/LSH
    Similar {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
    },
    /// Scan an image for file extents and analyse dedup + delta compression potential
    Extents {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
    /// Compare chunks between two images, measuring delta compression benefit
    Delta {
        image1: String,
        image2: String,
        #[arg(long, default_value_t = DEFAULT_CHUNK_KB)]
        chunk_kb: usize,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
}

// --- entropy ---

fn shannon_entropy(data: &[u8]) -> f64 {
    let mut counts = [0u32; 256];
    for &byte in data {
        counts[byte as usize] += 1;
    }
    let len = data.len() as f64;
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

// --- stats ---

#[derive(Default)]
struct Stats {
    total_chunks: usize,
    unique_hashes: HashSet<blake3::Hash>,
    zero_chunks: usize,
    high_entropy_chunks: usize,
    entropy_buckets: [usize; 8],
    total_files: usize,
    skipped_files: usize,
}

impl Stats {
    fn record_chunk(&mut self, chunk: &[u8], entropy_threshold: f64) {
        self.total_chunks += 1;
        let hash = blake3::hash(chunk);
        self.unique_hashes.insert(hash);
        if chunk.iter().all(|&b| b == 0) {
            self.zero_chunks += 1;
        }
        let entropy = shannon_entropy(chunk);
        self.entropy_buckets[(entropy as usize).min(7)] += 1;
        if entropy >= entropy_threshold {
            self.high_entropy_chunks += 1;
        }
    }
}

// --- block-level analysis ---

fn analyse_blocks(path: &Path, chunk_size: usize, entropy_threshold: f64) -> io::Result<Stats> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(chunk_size * 8, file);
    let mut buf = vec![0u8; chunk_size];
    let mut stats = Stats::default();

    loop {
        let mut bytes_read = 0;
        while bytes_read < chunk_size {
            match reader.read(&mut buf[bytes_read..])? {
                0 => break,
                n => bytes_read += n,
            }
        }
        if bytes_read == 0 {
            break;
        }
        stats.record_chunk(&buf[..bytes_read], entropy_threshold);
        if bytes_read < chunk_size {
            break;
        }
    }

    Ok(stats)
}

// --- file-level analysis ---

fn analyse_files(image_path: &Path, chunk_size: usize, entropy_threshold: f64) -> Result<Stats, Ext4Error> {
    let fs = Ext4::load_from_path(image_path)?;
    let mut stats = Stats::default();
    let mut queue: Vec<Ext4PathBuf> = vec![Ext4PathBuf::new("/")];

    while let Some(dir) = queue.pop() {
        for entry in fs.read_dir(&dir)? {
            let entry: DirEntry = entry?;
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let path = entry.path();
            let metadata: Metadata = match fs.symlink_metadata(&path) {
                Ok(m) => m,
                Err(_) => { stats.skipped_files += 1; continue; }
            };

            if metadata.file_type().is_dir() {
                queue.push(path);
            } else if metadata.file_type().is_regular_file() {
                stats.total_files += 1;
                let data: Vec<u8> = match fs.read(&path) {
                    Ok(d) => d,
                    Err(_) => { stats.skipped_files += 1; continue; }
                };
                for chunk in data.chunks(chunk_size) {
                    stats.record_chunk(chunk, entropy_threshold);
                }
            }
        }
    }

    Ok(stats)
}

// --- output ---

fn print_stats(label: &str, stats: &Stats, chunk_size: usize, entropy_threshold: f64, show_files: bool) {
    let total = stats.total_chunks;
    let unique = stats.unique_hashes.len();
    let dedup_ratio = if unique > 0 { total as f64 / unique as f64 } else { 1.0 };
    let total_mb = (total * chunk_size) as f64 / (1024.0 * 1024.0);

    println!("=== {} ===", label);
    if show_files {
        println!("  Files visited:    {}", stats.total_files);
        if stats.skipped_files > 0 {
            println!("  Files skipped:    {}", stats.skipped_files);
        }
    }
    println!("  Data (approx):    {:.1} MB", total_mb);
    println!("  Total chunks:     {}", total);
    println!("  Unique chunks:    {} ({:.1}% unique)", unique, 100.0 * unique as f64 / total.max(1) as f64);
    println!("  Dedup ratio:      {:.2}x", dedup_ratio);
    if !show_files {
        println!("  Zero chunks:      {} ({:.1}%)", stats.zero_chunks, 100.0 * stats.zero_chunks as f64 / total.max(1) as f64);
    }
    println!(
        "  High entropy (>={:.1}): {} ({:.1}%)",
        entropy_threshold,
        stats.high_entropy_chunks,
        100.0 * stats.high_entropy_chunks as f64 / total.max(1) as f64
    );
    println!("  Entropy distribution:");
    for (i, &count) in stats.entropy_buckets.iter().enumerate() {
        let pct = 100.0 * count as f64 / total.max(1) as f64;
        let bar: String = "#".repeat((pct / 2.0) as usize);
        println!("    [{}-{}): {:>6} ({:>5.1}%)  {}", i, i + 1, count, pct, bar);
    }
    println!();
}

fn print_overlap(stats1: &Stats, stats2: &Stats) {
    let common = stats1.unique_hashes.intersection(&stats2.unique_hashes).count();
    let pct1 = 100.0 * common as f64 / stats1.unique_hashes.len().max(1) as f64;
    let pct2 = 100.0 * common as f64 / stats2.unique_hashes.len().max(1) as f64;
    println!("=== Overlap ===");
    println!("  Common unique chunks: {}", common);
    println!("  {:.1}% of image1's unique chunks", pct1);
    println!("  {:.1}% of image2's unique chunks", pct2);
}

// --- main ---

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Blocks { image1, image2, chunk_kb, entropy_threshold } => {
            let chunk_size = chunk_kb * 1024;
            let path1 = Path::new(&image1);
            println!("Analysing {} ...", path1.display());
            let stats1 = analyse_blocks(path1, chunk_size, entropy_threshold).expect("failed to read image1");
            print_stats(&image1, &stats1, chunk_size, entropy_threshold, false);

            if let Some(ref img2) = image2 {
                let path2 = Path::new(img2);
                println!("Analysing {} ...", path2.display());
                let stats2 = analyse_blocks(path2, chunk_size, entropy_threshold).expect("failed to read image2");
                print_stats(img2, &stats2, chunk_size, entropy_threshold, false);
                print_overlap(&stats1, &stats2);
            }
        }

        Command::Files { image1, image2, chunk_kb, entropy_threshold } => {
            let chunk_size = chunk_kb * 1024;
            let path1 = Path::new(&image1);
            println!("Analysing {} ...", path1.display());
            let stats1 = analyse_files(path1, chunk_size, entropy_threshold).expect("failed to read image1");
            print_stats(&image1, &stats1, chunk_size, entropy_threshold, true);

            if let Some(ref img2) = image2 {
                let path2 = Path::new(img2);
                println!("Analysing {} ...", path2.display());
                let stats2 = analyse_files(path2, chunk_size, entropy_threshold).expect("failed to read image2");
                print_stats(img2, &stats2, chunk_size, entropy_threshold, true);
                print_overlap(&stats1, &stats2);
            }
        }

        Command::Serve { image, port, chunk_kb } => {
            nbd::run(&image, port, chunk_kb * 1024).expect("NBD server error");
        }

        Command::Snapshot { image, chunk_kb, parent, out_dir } => {
            snapshot::generate(
                Path::new(&image),
                chunk_kb,
                parent.as_deref().map(Path::new),
                Path::new(&out_dir),
            ).expect("snapshot failed");
        }

        Command::SnapshotInfo { snapshot } => {
            snapshot::info(Path::new(&snapshot));
        }

        Command::SnapshotDiff { snapshot1, snapshot2 } => {
            snapshot::diff(Path::new(&snapshot1), Path::new(&snapshot2));
        }

        Command::Manifest { image, chunk_kb, out } => {
            manifest::generate(Path::new(&image), chunk_kb, Path::new(&out)).expect("manifest failed");
        }

        Command::Diff { manifest1, manifest2, verbose } => {
            manifest::diff(Path::new(&manifest1), Path::new(&manifest2), verbose);
        }

        Command::Similar { image1, image2, chunk_kb } => {
            similar::run(Path::new(&image1), image2.as_deref().map(Path::new), chunk_kb * 1024).expect("similar failed");
        }

        Command::Extents { image1, image2, level } => {
            extents::run(Path::new(&image1), image2.as_deref().map(Path::new), level)
                .expect("extents failed");
        }

        Command::Delta { image1, image2, chunk_kb, level } => {
            let chunk_size = chunk_kb * 1024;
            delta::run(Path::new(&image1), Path::new(&image2), chunk_size, level)
                .expect("delta failed");
        }

        Command::ExtractBoot { image, out_dir } => {
            extract_boot(Path::new(&image), Path::new(&out_dir)).expect("extract-boot failed");
        }
    }
}

fn extract_boot(image: &Path, out_dir: &Path) -> Result<(), Ext4Error> {
    let fs = Ext4::load_from_path(image)?;
    std::fs::create_dir_all(out_dir).ok();

    for name in &["vmlinuz", "initrd.img"] {
        let path_str = format!("/boot/{}", name);
        let src = Ext4PathBuf::new(&path_str);
        match fs.read(&src) {
            Ok(data) => {
                let dst = out_dir.join(name);
                std::fs::write(&dst, &data).expect("write failed");
                println!("Extracted /boot/{} → {} ({:.1} MB)", name, dst.display(), data.len() as f64 / (1024.0 * 1024.0));
            }
            Err(e) => eprintln!("Could not read /boot/{}: {}", name, e),
        }
    }

    Ok(())
}
