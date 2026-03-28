use std::path::Path;

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

use elide_core::volume;
use elide_signing::{FORK_KEY_FILE, FORK_ORIGIN_FILE, FORK_PUB_FILE};

mod extents;
mod inspect;
mod ls;
mod nbd;

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
    /// Serve an elide fork over NBD
    ServeVolume {
        /// Path to the volume directory (e.g. volumes/ubuntu-22.04)
        vol_dir: String,
        /// Name of the fork to serve (e.g. vm1)
        fork: String,
        /// Volume size (e.g. "4G", "512M", "1073741824"). Required on first use;
        /// ignored on subsequent opens (size is stored in <vol-dir>/size).
        #[arg(long)]
        size: Option<String>,
        /// Address to bind (use 0.0.0.0 to allow connections from VMs)
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
        /// Serve as a read-only block device (required for readonly-base forks)
        #[arg(long)]
        readonly: bool,
        /// Skip the fork.origin hostname/path check (use after an intentional move)
        #[arg(long)]
        force_origin: bool,
    },
    /// Extract kernel and initrd from an ext4 image's /boot directory
    ExtractBoot {
        image: String,
        #[arg(long, default_value = ".")]
        out_dir: String,
    },
    /// Inspect an elide volume directory and print a human-readable summary
    InspectVolume {
        /// Path to the volume root directory, or a fork directory (resolves to its parent)
        dir: String,
    },
    /// List ext4 filesystem contents of a fork (read-only)
    LsVolume {
        /// Path to the volume directory (e.g. volumes/ubuntu-22.04)
        vol_dir: String,
        /// Name of the fork to inspect (e.g. vm1)
        fork: String,
        /// Path within the ext4 filesystem to list (default: /)
        #[arg(default_value = "/")]
        path: String,
    },
    /// Compact sparse segments in a fork, reclaiming space from overwritten extents
    CompactVolume {
        /// Path to the volume directory (e.g. volumes/ubuntu-22.04)
        vol_dir: String,
        /// Name of the fork to compact (e.g. vm1)
        fork: String,
        /// Compact segments where fewer than this fraction of stored bytes are live (default: 0.7)
        #[arg(long, default_value_t = 0.7)]
        min_live_ratio: f64,
    },
    /// Checkpoint a fork by writing a snapshot marker; the fork stays live
    SnapshotVolume {
        /// Path to the volume directory (e.g. volumes/ubuntu-22.04)
        vol_dir: String,
        /// Name of the fork to snapshot (e.g. vm1)
        fork: String,
    },
    /// Create a new named fork branched from the latest snapshot of the source fork
    ForkVolume {
        /// Path to the volume directory (contains the named forks)
        vol_dir: String,
        /// Name for the new fork
        fork_name: String,
        /// Source fork to branch from (default: "base")
        #[arg(long, default_value = "base")]
        from: String,
    },
    /// List all forks in a volume directory
    ListForks {
        /// Path to the volume directory
        vol_dir: String,
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
            vol_dir,
            fork,
            size,
            bind,
            port,
            readonly,
            force_origin,
        } => {
            let vol_path = Path::new(&vol_dir);
            let fork_dir = vol_path.join("forks").join(&fork);
            let size_bytes = resolve_volume_size(vol_path, size.as_deref())
                .expect("failed to determine volume size");
            if readonly {
                nbd::run_volume_readonly(&fork_dir, size_bytes, &bind, port)
                    .expect("readonly NBD server error");
            } else {
                // Ensure the fork directory exists before touching key files.
                std::fs::create_dir_all(&fork_dir).expect("failed to create fork directory");
                let signer = if fork_dir.join(FORK_KEY_FILE).exists() {
                    if !force_origin {
                        elide_signing::verify_origin(&fork_dir, FORK_PUB_FILE, FORK_ORIGIN_FILE)
                            .map_err(|e| {
                                std::io::Error::other(format!(
                                    "{e} — use --force-origin if this fork has been intentionally moved"
                                ))
                            })
                            .expect("fork.origin check failed");
                    }
                    elide_signing::load_signer(&fork_dir, FORK_KEY_FILE)
                        .expect("failed to load fork signing key")
                } else {
                    // First use: generate keypair and record origin.
                    let key =
                        elide_signing::generate_keypair(&fork_dir, FORK_KEY_FILE, FORK_PUB_FILE)
                            .expect("failed to generate fork keypair");
                    elide_signing::write_origin(&fork_dir, &key, FORK_ORIGIN_FILE)
                        .expect("failed to write fork.origin");
                    elide_signing::load_signer(&fork_dir, FORK_KEY_FILE)
                        .expect("failed to load fork signing key")
                };
                nbd::run_volume_signed(&fork_dir, size_bytes, &bind, port, signer)
                    .expect("volume NBD server error");
            }
        }

        Command::ExtractBoot { image, out_dir } => {
            extract_boot(Path::new(&image), Path::new(&out_dir)).expect("extract-boot failed");
        }

        Command::InspectVolume { dir } => {
            inspect::run(Path::new(&dir)).expect("inspect-volume failed");
        }

        Command::LsVolume {
            vol_dir,
            fork,
            path,
        } => {
            let fork_dir = Path::new(&vol_dir).join("forks").join(&fork);
            ls::run(&fork_dir, &path).expect("ls-volume failed");
        }

        Command::CompactVolume {
            vol_dir,
            fork,
            min_live_ratio,
        } => {
            let fork_dir = Path::new(&vol_dir).join("forks").join(&fork);
            let mut vol = volume::Volume::open(&fork_dir).expect("failed to open volume");
            let stats = vol.compact(min_live_ratio).expect("compaction failed");
            println!(
                "segments compacted: {}  bytes freed: {}  extents removed: {}",
                stats.segments_compacted, stats.bytes_freed, stats.extents_removed,
            );
        }

        Command::SnapshotVolume { vol_dir, fork } => {
            let fork_dir = Path::new(&vol_dir).join("forks").join(&fork);
            let mut vol = volume::Volume::open(&fork_dir).expect("failed to open volume");
            let snap_ulid = vol.snapshot().expect("snapshot failed");
            println!("{snap_ulid}");
        }

        Command::ForkVolume {
            vol_dir,
            fork_name,
            from,
        } => {
            let fork_dir = volume::fork_volume(Path::new(&vol_dir), &fork_name, &from)
                .expect("fork-volume failed");
            let key = elide_signing::generate_keypair(&fork_dir, FORK_KEY_FILE, FORK_PUB_FILE)
                .expect("failed to generate fork keypair");
            elide_signing::write_origin(&fork_dir, &key, FORK_ORIGIN_FILE)
                .expect("failed to write fork.origin");
            println!("{}", fork_dir.display());
        }

        Command::ListForks { vol_dir } => {
            list_forks(Path::new(&vol_dir)).expect("list-forks failed");
        }
    }
}

struct VolMeta {
    readonly: bool,
}

fn read_vol_meta(vol_dir: &Path) -> Option<VolMeta> {
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(default)]
        readonly: bool,
    }
    let content = std::fs::read_to_string(vol_dir.join("meta.toml")).ok()?;
    let raw: Raw = toml::from_str(&content).ok()?;
    Some(VolMeta {
        readonly: raw.readonly,
    })
}

fn list_forks(vol_dir: &Path) -> std::io::Result<()> {
    use std::fs;

    let is_readonly = read_vol_meta(vol_dir).is_some_and(|m| m.readonly);

    if is_readonly {
        println!("readonly volume (template)");
    }

    let forks_dir = vol_dir.join("forks");
    let mut forks: Vec<(String, bool)> = Vec::new(); // (name, is_live)
    match fs::read_dir(&forks_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let name = match path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_owned(),
                    None => continue,
                };
                let is_live = path.join("wal").is_dir();
                forks.push((name, is_live));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    forks.sort_by(|a, b| a.0.cmp(&b.0));

    if forks.is_empty() {
        println!("  (no forks yet — use fork-volume to create one)");
    } else {
        for (name, is_live) in &forks {
            let state = if *is_live { "live" } else { "base" };
            let snap_count = count_snapshots(&forks_dir.join(name));
            println!("  {name}  [{state}]  {snap_count} snapshot(s)");
        }
    }
    Ok(())
}

fn count_snapshots(fork_dir: &Path) -> usize {
    let snapshots_dir = fork_dir.join("snapshots");
    std::fs::read_dir(&snapshots_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .and_then(|n| ulid::Ulid::from_string(n).ok())
                        .is_some()
                })
                .count()
        })
        .unwrap_or(0)
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
