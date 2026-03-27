use std::path::Path;

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

mod extentindex;
mod extents;
mod inspect;
mod lbamap;
mod ls;
mod nbd;
mod segment;
mod volume;
mod writelog;

/// Analyse ext4 disk images for dedup and delta compression potential.
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scan an image for file extents and analyse dedup + delta compression potential
    Extents {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
    /// Serve a raw ext4 image over NBD, tracking which blocks are read
    Serve {
        image: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
        /// Write a boot trace file on disconnect (for use with cold-boot)
        #[arg(long)]
        save_trace: Option<String>,
    },
    /// Combine a boot trace with cross-image analysis to estimate cold-boot fetch cost (4 strategies: zstd-only, zstd+sparse, zstd+delta, zstd+delta+sparse)
    ColdBoot {
        image1: String,
        image2: String,
        #[arg(long)]
        trace: String,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },
    /// Measure file renames between two images (exact renames and size-matched rename+modify candidates)
    RenameAnalysis { image1: String, image2: String },
    /// Measure sparse-strategy savings: within changed files, how many 4KB blocks actually differ?
    SparseAnalysis { image1: String, image2: String },
    /// Serve an elide volume directory over NBD
    ServeVolume {
        /// Path to the volume directory (created if it doesn't exist)
        dir: String,
        /// Volume size (e.g. "4G", "512M", "1073741824"). Required on first use;
        /// ignored on subsequent opens (size is stored in <dir>/size).
        #[arg(long)]
        size: Option<String>,
        /// Address to bind (use 0.0.0.0 to allow connections from VMs)
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
    },
    /// Extract kernel and initrd from an ext4 image's /boot directory
    ExtractBoot {
        image: String,
        #[arg(long, default_value = ".")]
        out_dir: String,
    },
    /// Inspect an elide volume directory and print a human-readable summary
    InspectVolume {
        /// Path to the volume root directory
        dir: String,
    },
    /// List ext4 filesystem contents of a volume directory (read-only)
    LsVolume {
        /// Path to the volume root directory
        dir: String,
        /// Path within the ext4 filesystem to list (default: /)
        #[arg(default_value = "/")]
        path: String,
    },
    /// Compact sparse segments in a volume, reclaiming space from overwritten extents
    CompactVolume {
        /// Path to the volume root directory
        dir: String,
        /// Compact segments where fewer than this fraction of stored bytes are live (default: 0.7)
        #[arg(long, default_value_t = 0.7)]
        min_live_ratio: f64,
    },
    /// Take a snapshot of a live volume node, freezing it and continuing in a new child node
    SnapshotVolume {
        /// Path to the live volume node directory
        dir: String,
    },
}

fn main() {
    let args = Args::parse();

    match args.command {
        Command::Extents {
            image1,
            image2,
            level,
        } => {
            extents::run(Path::new(&image1), image2.as_deref().map(Path::new), level)
                .expect("extents failed");
        }

        Command::Serve {
            image,
            port,
            save_trace,
        } => {
            nbd::run(&image, port, save_trace.as_deref()).expect("NBD server error");
        }

        Command::ColdBoot {
            image1,
            image2,
            trace,
            level,
        } => {
            extents::run_cold_boot(
                Path::new(&image1),
                Path::new(&image2),
                Path::new(&trace),
                level,
            )
            .expect("cold-boot analysis failed");
        }

        Command::RenameAnalysis { image1, image2 } => {
            extents::run_rename_analysis(Path::new(&image1), Path::new(&image2))
                .expect("rename-analysis failed");
        }

        Command::SparseAnalysis { image1, image2 } => {
            extents::run_sparse_analysis(Path::new(&image1), Path::new(&image2))
                .expect("sparse-analysis failed");
        }

        Command::ServeVolume {
            dir,
            size,
            bind,
            port,
        } => {
            let dir = Path::new(&dir);
            let size_bytes =
                resolve_volume_size(dir, size.as_deref()).expect("failed to determine volume size");
            nbd::run_volume(dir, size_bytes, &bind, port).expect("volume NBD server error");
        }

        Command::ExtractBoot { image, out_dir } => {
            extract_boot(Path::new(&image), Path::new(&out_dir)).expect("extract-boot failed");
        }

        Command::InspectVolume { dir } => {
            inspect::run(Path::new(&dir)).expect("inspect-volume failed");
        }

        Command::LsVolume { dir, path } => {
            ls::run(Path::new(&dir), &path).expect("ls-volume failed");
        }

        Command::CompactVolume {
            dir,
            min_live_ratio,
        } => {
            let mut vol = volume::Volume::open(Path::new(&dir)).expect("failed to open volume");
            let stats = vol.compact(min_live_ratio).expect("compaction failed");
            println!(
                "segments compacted: {}  bytes freed: {}  extents removed: {}",
                stats.segments_compacted, stats.bytes_freed, stats.extents_removed,
            );
        }

        Command::SnapshotVolume { dir } => {
            let mut vol = volume::Volume::open(Path::new(&dir)).expect("failed to open volume");
            let child_path = vol.snapshot().expect("snapshot failed");
            println!("{}", child_path.display());
        }
    }
}

/// Parse a human-readable size string: plain bytes, or with suffix K/M/G/T (base-2).
fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, shift) = if let Some(rest) = s.strip_suffix('T').or_else(|| s.strip_suffix("TB")) {
        (rest, 40)
    } else if let Some(rest) = s.strip_suffix('G').or_else(|| s.strip_suffix("GB")) {
        (rest, 30)
    } else if let Some(rest) = s.strip_suffix('M').or_else(|| s.strip_suffix("MB")) {
        (rest, 20)
    } else if let Some(rest) = s.strip_suffix('K').or_else(|| s.strip_suffix("KB")) {
        (rest, 10)
    } else {
        (s, 0)
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid size: {}", s))?;
    Ok(n << shift)
}

/// Read the volume size from `<dir>/size`, or create it from `--size` if not present.
fn resolve_volume_size(dir: &Path, size_arg: Option<&str>) -> std::io::Result<u64> {
    let size_file = dir.join("size");
    if size_file.exists() {
        let s = std::fs::read_to_string(&size_file)?;
        s.trim()
            .parse::<u64>()
            .map_err(|e| std::io::Error::other(format!("bad size file: {}", e)))
    } else {
        let s = size_arg.ok_or_else(|| {
            std::io::Error::other("volume size required on first use: pass --size (e.g. --size 4G)")
        })?;
        let bytes =
            parse_size(s).map_err(|e| std::io::Error::other(format!("bad --size: {}", e)))?;
        if bytes == 0 {
            return Err(std::io::Error::other("volume size must be non-zero"));
        }
        std::fs::create_dir_all(dir)?;
        std::fs::write(&size_file, bytes.to_string())?;
        Ok(bytes)
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
                println!(
                    "Extracted /boot/{} → {} ({:.1} MB)",
                    name,
                    dst.display(),
                    data.len() as f64 / (1024.0 * 1024.0)
                );
            }
            Err(e) => eprintln!("Could not read /boot/{}: {}", name, e),
        }
    }

    Ok(())
}
