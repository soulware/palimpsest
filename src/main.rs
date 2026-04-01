use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

use elide_core::signing::{FORK_KEY_FILE, FORK_ORIGIN_FILE, FORK_PUB_FILE};
use elide_core::volume;

mod control;
mod coordinator_client;
mod extents;
mod fetcher;
mod inspect;
mod inspect_files;
mod ls;
mod nbd;

/// Elide volume management and analysis tools.
#[derive(Parser)]
struct Args {
    /// Directory containing volumes and the coordinator socket.
    #[arg(
        long,
        env = "ELIDE_DATA_DIR",
        default_value = "elide_data",
        global = true
    )]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manage volumes
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
    },

    /// Serve an elide fork over NBD (spawned by coordinator; not for direct use)
    #[command(hide = true)]
    ServeVolume {
        /// Path to the fork directory
        fork_dir: PathBuf,
        /// Volume size (e.g. "4G", "512M"). Required on first use;
        /// ignored on subsequent opens (size is stored in <vol-dir>/size).
        #[arg(long)]
        size: Option<String>,
        /// Address to bind (use 0.0.0.0 to allow connections from VMs)
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
        /// Serve as a read-only block device
        #[arg(long)]
        readonly: bool,
        /// Skip the fork.origin hostname/path check (use after an intentional move)
        #[arg(long)]
        force_origin: bool,
    },

    /// Scan an image for file extents and analyse dedup + delta compression potential
    #[command(hide = true)]
    Extents {
        image1: String,
        image2: Option<String>,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },

    /// Serve a raw ext4 image over NBD, tracking which blocks are read
    #[command(hide = true)]
    Serve {
        image: String,
        #[arg(long, default_value_t = 10809)]
        port: u16,
        #[arg(long)]
        save_trace: Option<String>,
    },

    /// Combine a boot trace with cross-image analysis to estimate cold-boot fetch cost
    #[command(hide = true)]
    ColdBoot {
        image1: String,
        image2: String,
        #[arg(long)]
        trace: String,
        #[arg(long, default_value_t = 3)]
        level: i32,
    },

    /// Measure file renames between two images
    #[command(hide = true)]
    RenameAnalysis { image1: String, image2: String },

    /// Measure sparse-strategy savings within changed files
    #[command(hide = true)]
    SparseAnalysis { image1: String, image2: String },

    /// Extract kernel and initrd from an ext4 image's /boot directory
    #[command(hide = true)]
    ExtractBoot {
        image: String,
        #[arg(long, default_value = ".")]
        out_dir: String,
    },

    /// Repack sparse segments in a fork (diagnostic)
    #[command(hide = true)]
    Repack {
        fork_dir: PathBuf,
        #[arg(long, default_value_t = 0.7)]
        min_live_ratio: f64,
    },

    /// Print header and index entries of a segment or .idx file (diagnostic)
    #[command(hide = true)]
    InspectSegment { path: PathBuf },

    /// Print all records in a WAL file (diagnostic)
    #[command(hide = true)]
    InspectWal { path: PathBuf },
}

#[derive(Subcommand)]
enum VolumeCommand {
    /// List all volumes in the data directory
    List,

    /// Show a human-readable summary of a volume
    Info {
        /// Path to the volume root directory
        vol_dir: PathBuf,
    },

    /// Browse ext4 filesystem contents of a fork
    Ls {
        /// Path to the volume directory
        vol_dir: PathBuf,
        /// Name of the fork to inspect
        fork: String,
        /// Path within the ext4 filesystem (default: /)
        #[arg(default_value = "/")]
        path: String,
    },

    /// Write a snapshot marker; the fork stays live
    Snapshot {
        /// Path to the volume directory
        vol_dir: PathBuf,
        /// Name of the fork to snapshot
        fork: String,
    },

    /// Create a new named fork branched from the latest snapshot of the source fork
    Fork {
        /// Path to the volume directory
        vol_dir: PathBuf,
        /// Name for the new fork
        fork_name: String,
        /// Source fork to branch from
        #[arg(long, default_value = "base")]
        from: String,
    },

    /// Create a new volume
    Create {
        /// Volume name
        name: String,
        /// Volume size (e.g. "4G", "512M")
        #[arg(long)]
        size: Option<String>,
    },

    /// Show the running status of a volume
    Status {
        /// Volume name
        name: String,
    },

    /// Manage OCI imports
    Import {
        #[command(subcommand)]
        command: ImportCommand,
    },

    /// Stop all processes for a volume and remove its directory
    Delete {
        /// Volume name
        name: String,
    },
}

#[derive(Subcommand)]
enum ImportCommand {
    /// Pull an OCI image into a new volume (coordinator-spawned; returns immediately with a ULID)
    Start {
        /// Volume name to create
        name: String,
        /// OCI image reference (e.g. ubuntu:22.04, ghcr.io/org/image:tag)
        oci_ref: String,
    },

    /// Poll the state of a running or completed import
    Status {
        /// Import ULID (returned by `import start`)
        ulid: String,
    },

    /// Stream output from a running or completed import
    Attach {
        /// Import ULID (returned by `import start`)
        ulid: String,
    },
}

fn main() {
    tracing_log::LogTracer::init().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init()
        .ok();

    let args = Args::parse();

    let socket_path = args.data_dir.join("control.sock");

    match args.command {
        Command::Volume { command } => match command {
            VolumeCommand::List => {
                list_volumes(&args.data_dir).expect("volume list failed");
            }

            VolumeCommand::Info { vol_dir } => {
                inspect::run(&vol_dir).expect("volume info failed");
            }

            VolumeCommand::Ls {
                vol_dir,
                fork,
                path,
            } => {
                let fork_dir = vol_dir.join("forks").join(&fork);
                ls::run(&fork_dir, &path).expect("volume ls failed");
            }

            VolumeCommand::Snapshot { vol_dir, fork } => {
                let fork_dir = vol_dir.join("forks").join(&fork);
                let mut vol = volume::Volume::open(&fork_dir).expect("failed to open volume");
                let snap_ulid = vol.snapshot().expect("snapshot failed");
                println!("{snap_ulid}");
            }

            VolumeCommand::Fork {
                vol_dir,
                fork_name,
                from,
            } => {
                let fork_dir =
                    volume::fork_volume(&vol_dir, &fork_name, &from).expect("volume fork failed");
                let key =
                    elide_core::signing::generate_keypair(&fork_dir, FORK_KEY_FILE, FORK_PUB_FILE)
                        .expect("failed to generate fork keypair");
                elide_core::signing::write_origin(&fork_dir, &key, FORK_ORIGIN_FILE)
                    .expect("failed to write fork.origin");
                println!("{}", fork_dir.display());
            }

            VolumeCommand::Create { name, size } => {
                let vol_dir = args.data_dir.join(&name);
                create_volume(&vol_dir, size.as_deref()).expect("volume create failed");
                if let Err(e) = coordinator_client::rescan(&socket_path) {
                    eprintln!(
                        "warning: coordinator not running, volume will be picked up on next scan ({e})"
                    );
                }
            }

            VolumeCommand::Status { name } => {
                let resp = coordinator_client::status(&socket_path, &name)
                    .unwrap_or_else(|e| format!("err {e}"));
                match resp.split_once(' ') {
                    Some(("ok", rest)) => println!("{name}: {rest}"),
                    Some(("err", msg)) => {
                        eprintln!("{name}: {msg}");
                        std::process::exit(1);
                    }
                    _ => println!("{name}: {resp}"),
                }
            }

            VolumeCommand::Import { command } => match command {
                ImportCommand::Start { name, oci_ref } => {
                    let ulid = coordinator_client::import_start(&socket_path, &name, &oci_ref)
                        .expect("import start failed");
                    println!("{ulid}");
                }
                ImportCommand::Status { ulid } => {
                    let resp = coordinator_client::import_status(&socket_path, &ulid)
                        .unwrap_or_else(|e| format!("err {e}"));
                    match resp.split_once(' ') {
                        Some(("ok", state)) => println!("{ulid}: {state}"),
                        Some(("err", msg)) => {
                            eprintln!("{ulid}: {msg}");
                            std::process::exit(1);
                        }
                        _ => println!("{ulid}: {resp}"),
                    }
                }
                ImportCommand::Attach { ulid } => {
                    let mut stdout = std::io::stdout();
                    if let Err(e) =
                        coordinator_client::import_attach(&socket_path, &ulid, &mut stdout)
                    {
                        eprintln!("import failed: {e}");
                        std::process::exit(1);
                    }
                }
            },

            VolumeCommand::Delete { name } => {
                coordinator_client::delete_volume(&socket_path, &name)
                    .expect("volume delete failed");
            }
        },

        Command::ServeVolume {
            fork_dir,
            size,
            bind,
            port,
            readonly,
            force_origin,
        } => {
            let vol_dir = fork_dir
                .parent()
                .and_then(|p| p.parent())
                .expect("fork_dir must be <vol>/forks/<name>");
            let size_bytes = resolve_volume_size(vol_dir, size.as_deref())
                .expect("failed to determine volume size");
            let fetch_config =
                fetcher::FetchConfig::load(vol_dir).expect("failed to load fetch config");
            if readonly {
                nbd::run_volume_readonly(&fork_dir, size_bytes, &bind, port, fetch_config)
                    .expect("readonly NBD server error");
            } else {
                std::fs::create_dir_all(&fork_dir).expect("failed to create fork directory");
                let signer = if fork_dir.join(FORK_KEY_FILE).exists() {
                    if !force_origin {
                        elide_core::signing::verify_origin(
                            &fork_dir,
                            FORK_PUB_FILE,
                            FORK_ORIGIN_FILE,
                        )
                        .map_err(|e| {
                            std::io::Error::other(format!(
                                "{e} — use --force-origin if this fork has been intentionally moved"
                            ))
                        })
                        .expect("fork.origin check failed");
                    }
                    elide_core::signing::load_signer(&fork_dir, FORK_KEY_FILE)
                        .expect("failed to load fork signing key")
                } else {
                    let key = elide_core::signing::generate_keypair(
                        &fork_dir,
                        FORK_KEY_FILE,
                        FORK_PUB_FILE,
                    )
                    .expect("failed to generate fork keypair");
                    elide_core::signing::write_origin(&fork_dir, &key, FORK_ORIGIN_FILE)
                        .expect("failed to write fork.origin");
                    elide_core::signing::load_signer(&fork_dir, FORK_KEY_FILE)
                        .expect("failed to load fork signing key")
                };
                nbd::run_volume_signed(&fork_dir, size_bytes, &bind, port, signer, fetch_config)
                    .expect("volume NBD server error");
            }
        }

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

        Command::ExtractBoot { image, out_dir } => {
            extract_boot(Path::new(&image), Path::new(&out_dir)).expect("extract-boot failed");
        }

        Command::Repack {
            fork_dir,
            min_live_ratio,
        } => {
            let mut vol = volume::Volume::open(&fork_dir).expect("failed to open volume");
            let stats = vol.repack(min_live_ratio).expect("repack failed");
            println!(
                "segments repacked: {}  bytes freed: {}  extents removed: {}",
                stats.segments_compacted, stats.bytes_freed, stats.extents_removed,
            );
        }

        Command::InspectSegment { path } => {
            inspect_files::inspect_segment(&path).expect("inspect-segment failed");
        }

        Command::InspectWal { path } => {
            inspect_files::inspect_wal(&path).expect("inspect-wal failed");
        }
    }
}

fn list_volumes(data_dir: &Path) -> std::io::Result<()> {
    let mut volumes: Vec<(String, usize)> = Vec::new();
    match std::fs::read_dir(data_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let fork_count = count_forks(&path);
                volumes.push((name.to_owned(), fork_count));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    volumes.sort_by(|a, b| a.0.cmp(&b.0));
    if volumes.is_empty() {
        println!("no volumes found in {}", data_dir.display());
    } else {
        for (name, forks) in &volumes {
            println!("{name}  ({forks} fork(s))");
        }
    }
    Ok(())
}

fn count_forks(vol_dir: &Path) -> usize {
    std::fs::read_dir(vol_dir.join("forks"))
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .count()
        })
        .unwrap_or(0)
}

fn create_volume(vol_dir: &Path, size: Option<&str>) -> std::io::Result<()> {
    std::fs::create_dir_all(vol_dir.join("forks"))?;
    if let Some(s) = size {
        let bytes = parse_size(s).map_err(|e| std::io::Error::other(format!("bad --size: {e}")))?;
        if bytes == 0 {
            return Err(std::io::Error::other("volume size must be non-zero"));
        }
        std::fs::write(vol_dir.join("size"), bytes.to_string())?;
    }
    println!("{}", vol_dir.display());
    Ok(())
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
