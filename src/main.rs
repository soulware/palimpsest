use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

use elide_core::signing::{VOLUME_KEY_FILE, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE};
use elide_core::volume;

use elide::{
    coordinator_client, extents, inspect, inspect_files, ls, nbd, parse_size, resolve_volume_dir,
    resolve_volume_size, validate_volume_name,
};

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

    /// Serve an elide volume over NBD (spawned by coordinator; not for direct use)
    #[command(hide = true)]
    ServeVolume {
        /// Path to the volume directory (by_id/<ulid>/)
        fork_dir: PathBuf,
        /// Volume size (e.g. "4G", "512M"). Required on first use;
        /// ignored on subsequent opens (size is stored in <vol-dir>/size).
        #[arg(long)]
        size: Option<String>,
        /// Address to bind the NBD server (use 0.0.0.0 for VM access).
        /// Ignored if --port is not set.
        #[arg(long, default_value = "127.0.0.1")]
        bind: String,
        /// Port for the NBD server. If omitted, no NBD server is started
        /// (volume runs for coordinator IPC only).
        #[arg(long)]
        port: Option<u16>,
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
        /// Volume name
        name: String,
    },

    /// Browse ext4 filesystem contents of a volume
    Ls {
        /// Volume name
        name: String,
        /// Path within the ext4 filesystem (default: /)
        #[arg(default_value = "/")]
        path: String,
    },

    /// Write a snapshot marker; the volume stays live
    Snapshot {
        /// Volume name
        name: String,
    },

    /// Create a new volume branched from the latest snapshot of the source volume
    Fork {
        /// Name for the new volume
        fork_name: String,
        /// Source volume to branch from
        #[arg(long)]
        from: String,
    },

    /// Create a new volume
    Create {
        /// Volume name
        name: String,
        /// Volume size (e.g. "4G", "512M")
        #[arg(long)]
        size: Option<String>,
        /// Port for the NBD server (exposes the volume over NBD on first start)
        #[arg(long)]
        nbd_port: Option<u16>,
        /// Address to bind the NBD server (default: 127.0.0.1)
        #[arg(long)]
        nbd_bind: Option<String>,
    },

    /// Update configuration for a running volume
    Update {
        /// Volume name
        name: String,
        /// Change the NBD server port (restarts the volume process)
        #[arg(long)]
        nbd_port: Option<u16>,
        /// Change the NBD bind address (restarts the volume process)
        #[arg(long)]
        nbd_bind: Option<String>,
        /// Disable NBD serving (removes nbd.port and nbd.bind, restarts the volume process)
        #[arg(long)]
        no_nbd: bool,
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

    /// Interact with the remote object store
    Remote {
        #[command(subcommand)]
        command: RemoteCommand,
    },
}

#[derive(Subcommand)]
enum RemoteCommand {
    /// List all named volumes available in the store
    List,

    /// Download a volume from the store and register it locally
    Pull {
        /// Volume name to pull
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
    let by_id_dir = args.data_dir.join("by_id");

    match args.command {
        Command::Volume { command } => match command {
            VolumeCommand::List => {
                if let Err(e) = list_volumes(&args.data_dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Info { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = inspect::run(&vol_dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Ls { name, path } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = ls::run(&vol_dir, &path) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Snapshot { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                match snapshot_volume(&vol_dir, &by_id_dir) {
                    Ok(ulid) => println!("{ulid}"),
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }

            VolumeCommand::Fork { fork_name, from } => {
                if let Err(e) = validate_volume_name(&fork_name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                let by_name_dir = args.data_dir.join("by_name");
                let symlink_path = by_name_dir.join(&fork_name);
                if symlink_path.exists() {
                    eprintln!("error: volume already exists: {fork_name}");
                    std::process::exit(1);
                }
                let source_fork_dir = resolve_volume_dir(&args.data_dir, &from);
                if source_fork_dir.join("import.lock").exists() {
                    eprintln!(
                        "error: volume '{from}' is still importing; wait for import to complete before forking"
                    );
                    std::process::exit(1);
                }
                // Take an implicit snapshot of the source so fork branches from "now".
                // Idempotent if the tip is already snapshotted.
                if let Err(e) = snapshot_volume(&source_fork_dir, &by_id_dir) {
                    eprintln!("error: failed to snapshot source volume: {e}");
                    std::process::exit(1);
                }
                let new_vol_ulid = ulid::Ulid::new().to_string();
                let new_fork_dir = args.data_dir.join("by_id").join(&new_vol_ulid);
                if let Err(e) = volume::fork_volume(&new_fork_dir, &source_fork_dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                // Copy volume.size from source so the coordinator can serve the fork.
                match std::fs::read(source_fork_dir.join("volume.size")) {
                    Err(e) => {
                        let _ = std::fs::remove_dir_all(&new_fork_dir);
                        eprintln!(
                            "error: source volume has no volume.size (import may not have completed): {e}"
                        );
                        std::process::exit(1);
                    }
                    Ok(size) => {
                        if let Err(e) = std::fs::write(new_fork_dir.join("volume.size"), size) {
                            let _ = std::fs::remove_dir_all(&new_fork_dir);
                            eprintln!("error: failed to copy volume.size: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                if let Err(e) = std::fs::write(new_fork_dir.join("volume.name"), &fork_name) {
                    let _ = std::fs::remove_dir_all(&new_fork_dir);
                    eprintln!("error: failed to write volume.name: {e}");
                    std::process::exit(1);
                }
                if let Err(e) = std::fs::create_dir_all(&by_name_dir) {
                    let _ = std::fs::remove_dir_all(&new_fork_dir);
                    eprintln!("error: failed to create by_name dir: {e}");
                    std::process::exit(1);
                }
                if let Err(e) =
                    std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid}"), &symlink_path)
                {
                    let _ = std::fs::remove_dir_all(&new_fork_dir);
                    eprintln!("error: failed to create by_name symlink: {e}");
                    std::process::exit(1);
                }
                let key = match elide_core::signing::generate_keypair(
                    &new_fork_dir,
                    VOLUME_KEY_FILE,
                    VOLUME_PUB_FILE,
                ) {
                    Ok(k) => k,
                    Err(e) => {
                        let _ = std::fs::remove_file(&symlink_path);
                        let _ = std::fs::remove_dir_all(&new_fork_dir);
                        eprintln!("error: failed to generate fork keypair: {e}");
                        std::process::exit(1);
                    }
                };
                if let Err(e) =
                    elide_core::signing::write_origin(&new_fork_dir, &key, VOLUME_PROVENANCE_FILE)
                {
                    let _ = std::fs::remove_file(&symlink_path);
                    let _ = std::fs::remove_dir_all(&new_fork_dir);
                    eprintln!("error: failed to write volume.provenance: {e}");
                    std::process::exit(1);
                }
                println!("{}", new_fork_dir.display());
                if coordinator_client::rescan(&socket_path).is_err() {
                    eprintln!(
                        "warning: coordinator unreachable; volume will be picked up on next scan"
                    );
                }
            }

            VolumeCommand::Create {
                name,
                size,
                nbd_port,
                nbd_bind,
            } => {
                if let Err(e) =
                    create_volume(&args.data_dir, &name, size.as_deref(), nbd_port, nbd_bind)
                {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                if coordinator_client::rescan(&socket_path).is_err() {
                    eprintln!(
                        "warning: coordinator unreachable; volume will be picked up on next scan"
                    );
                }
            }

            VolumeCommand::Update {
                name,
                nbd_port,
                nbd_bind,
                no_nbd,
            } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = update_volume(&vol_dir, nbd_port, nbd_bind, no_nbd) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
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
                    match coordinator_client::import_start(&socket_path, &name, &oci_ref) {
                        Ok(ulid) => println!("{ulid}"),
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
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
                if let Err(e) = coordinator_client::delete_volume(&socket_path, &name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Remote { command } => {
                let config = match elide_fetch::FetchConfig::load(&args.data_dir) {
                    Ok(Some(c)) => c,
                    Ok(None) => {
                        eprintln!(
                            "error: no store configured — create fetch.toml or set ELIDE_S3_BUCKET"
                        );
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("error: loading store config: {e}");
                        std::process::exit(1);
                    }
                };
                match command {
                    RemoteCommand::List => {
                        if let Err(e) = remote_list(&config) {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                    RemoteCommand::Pull { name } => {
                        if let Err(e) = remote_pull(&config, &name, &args.data_dir, &socket_path) {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                }
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
            // In the flat layout, fork_dir IS the volume directory.
            let size_bytes = resolve_volume_size(&fork_dir, size.as_deref())
                .expect("failed to determine volume size");
            let fetch_config =
                elide_fetch::FetchConfig::load(&fork_dir).expect("failed to load fetch config");
            if readonly {
                nbd::run_volume_readonly(&fork_dir, size_bytes, &bind, port, fetch_config)
                    .expect("readonly NBD server error");
            } else {
                std::fs::create_dir_all(&fork_dir).expect("failed to create fork directory");
                let signer = if fork_dir.join(VOLUME_KEY_FILE).exists() {
                    if !force_origin {
                        elide_core::signing::verify_origin(
                            &fork_dir,
                            VOLUME_PUB_FILE,
                            VOLUME_PROVENANCE_FILE,
                        )
                        .map_err(|e| {
                            std::io::Error::other(format!(
                                "{e} — use --force-origin if this fork has been intentionally moved"
                            ))
                        })
                        .expect("volume.provenance check failed");
                    }
                    elide_core::signing::load_signer(&fork_dir, VOLUME_KEY_FILE)
                        .expect("failed to load volume signing key")
                } else {
                    let key = elide_core::signing::generate_keypair(
                        &fork_dir,
                        VOLUME_KEY_FILE,
                        VOLUME_PUB_FILE,
                    )
                    .expect("failed to generate volume keypair");
                    elide_core::signing::write_origin(&fork_dir, &key, VOLUME_PROVENANCE_FILE)
                        .expect("failed to write volume.provenance");
                    elide_core::signing::load_signer(&fork_dir, VOLUME_KEY_FILE)
                        .expect("failed to load volume signing key")
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
            let by_id_dir = fork_dir.parent().unwrap_or(&fork_dir).to_owned();
            let mut vol =
                volume::Volume::open(&fork_dir, &by_id_dir).expect("failed to open volume");
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

/// Snapshot a volume: uses the control socket if the volume is live,
/// falls back to direct open if not running.  Returns the snapshot ULID.
fn snapshot_volume(vol_dir: &Path, by_id_dir: &Path) -> std::io::Result<String> {
    use std::io::{self, BufRead, Write};
    use std::os::unix::net::UnixStream;
    match UnixStream::connect(vol_dir.join("control.sock")) {
        Ok(mut stream) => {
            writeln!(stream, "snapshot")?;
            stream.flush()?;
            let mut reader = io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let line = line.trim();
            line.strip_prefix("ok ")
                .map(|u| u.trim().to_owned())
                .ok_or_else(|| io::Error::other(format!("snapshot failed: {line}")))
        }
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            let mut vol = volume::Volume::open(vol_dir, by_id_dir)?;
            vol.snapshot().map(|u| u.to_string())
        }
        Err(e) => Err(e),
    }
}

fn list_volumes(data_dir: &Path) -> std::io::Result<()> {
    let by_name_dir = data_dir.join("by_name");
    let mut names: Vec<String> = Vec::new();
    match std::fs::read_dir(&by_name_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                names.push(name);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    names.sort();
    if names.is_empty() {
        println!("no volumes found in {}", data_dir.display());
    } else {
        for name in &names {
            println!("{name}");
        }
    }
    Ok(())
}

fn create_volume(
    data_dir: &Path,
    name: &str,
    size: Option<&str>,
    nbd_port: Option<u16>,
    nbd_bind: Option<String>,
) -> std::io::Result<()> {
    validate_volume_name(name)?;
    let size_str =
        size.ok_or_else(|| std::io::Error::other("--size is required (e.g. --size 4G)"))?;
    let bytes =
        parse_size(size_str).map_err(|e| std::io::Error::other(format!("bad --size: {e}")))?;
    if bytes == 0 {
        return Err(std::io::Error::other("volume size must be non-zero"));
    }

    let by_name_dir = data_dir.join("by_name");

    // Enforce local name uniqueness.
    if by_name_dir.join(name).exists() {
        return Err(std::io::Error::other(format!(
            "volume already exists: {name}"
        )));
    }

    let vol_ulid = ulid::Ulid::new().to_string();
    let vol_dir = data_dir.join("by_id").join(&vol_ulid);

    std::fs::create_dir_all(&vol_dir)?;
    std::fs::write(vol_dir.join("volume.name"), name)?;
    std::fs::create_dir_all(vol_dir.join("pending"))?;
    std::fs::create_dir_all(vol_dir.join("segments"))?;
    std::fs::write(vol_dir.join("volume.size"), bytes.to_string())?;

    let key = elide_core::signing::generate_keypair(&vol_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE)?;
    elide_core::signing::write_origin(&vol_dir, &key, VOLUME_PROVENANCE_FILE)?;

    if let Some(port) = nbd_port {
        std::fs::write(vol_dir.join("nbd.port"), port.to_string())?;
        if let Some(bind) = nbd_bind {
            std::fs::write(vol_dir.join("nbd.bind"), bind)?;
        }
    }

    std::fs::create_dir_all(&by_name_dir)?;
    std::os::unix::fs::symlink(format!("../by_id/{vol_ulid}"), by_name_dir.join(name))?;

    println!("{}", vol_dir.display());
    Ok(())
}

/// Update configuration for a volume and restart it if running.
///
/// Writes or removes nbd.port / nbd.bind, then sends `shutdown` to the volume's
/// control socket. The supervisor restarts the process, picking up the new config.
fn update_volume(
    vol_dir: &Path,
    nbd_port: Option<u16>,
    nbd_bind: Option<String>,
    no_nbd: bool,
) -> std::io::Result<()> {
    use std::io::{BufRead, Write};
    use std::os::unix::net::UnixStream;

    if no_nbd {
        let _ = std::fs::remove_file(vol_dir.join("nbd.port"));
        let _ = std::fs::remove_file(vol_dir.join("nbd.bind"));
    } else {
        if let Some(port) = nbd_port {
            std::fs::write(vol_dir.join("nbd.port"), port.to_string())?;
        }
        if let Some(bind) = nbd_bind {
            std::fs::write(vol_dir.join("nbd.bind"), bind)?;
        }
    }

    // Restart the volume process so it picks up the new config.
    let sock = vol_dir.join("control.sock");
    match UnixStream::connect(&sock) {
        Ok(mut stream) => {
            writeln!(stream, "shutdown")?;
            stream.flush()?;
            stream.shutdown(std::net::Shutdown::Write)?;
            let mut reader = std::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let line = line.trim();
            if line == "ok" {
                println!("volume restarting with new config");
            } else {
                return Err(std::io::Error::other(format!("shutdown failed: {line}")));
            }
        }
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            println!("volume not running; config will take effect on next start");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

/// List all named volumes in the remote store.
///
/// Performs a single `LIST names/` against the store and prints each name
/// with its ULID. Does not require a running coordinator.
fn remote_list(config: &elide_fetch::FetchConfig) -> std::io::Result<()> {
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    use object_store::path::Path as StorePath;

    let store = config
        .build_store()
        .map_err(|e| std::io::Error::other(format!("store: {e}")))?;
    let rt = tokio::runtime::Runtime::new()?;

    let names_prefix = StorePath::from("names/");
    let objects: Vec<_> = rt.block_on(async {
        store
            .list(Some(&names_prefix))
            .try_collect()
            .await
            .map_err(|e| std::io::Error::other(format!("listing names/: {e}")))
    })?;

    if objects.is_empty() {
        println!("(no volumes in store)");
        return Ok(());
    }

    for obj in &objects {
        let name = obj.location.filename().unwrap_or("?");
        let ulid = rt.block_on(async {
            let data = store
                .get(&obj.location)
                .await
                .map_err(|e| std::io::Error::other(format!("reading names/{name}: {e}")))?;
            let bytes = data
                .bytes()
                .await
                .map_err(|e| std::io::Error::other(format!("reading names/{name}: {e}")))?;
            std::str::from_utf8(&bytes)
                .map(|s| s.trim().to_owned())
                .map_err(|e| std::io::Error::other(format!("names/{name} is not valid utf-8: {e}")))
        })?;
        println!("{name}  {ulid}");
    }

    Ok(())
}

/// Pull a named volume from the remote store and register it locally.
///
/// 1. Resolves `names/<name>` → ULID
/// 2. Downloads `by_id/<ulid>/manifest.toml` and `by_id/<ulid>/volume.pub`
/// 3. Reconstructs the local directory skeleton under `data_dir/by_id/<ulid>/`
/// 4. Creates `data_dir/by_name/<name>` symlink
/// 5. Signals the coordinator to rescan (best-effort)
///
/// After this returns, the coordinator's next tick will run `prefetch_indexes`
/// to download segment index sections, making the volume openable.
fn remote_pull(
    config: &elide_fetch::FetchConfig,
    name: &str,
    data_dir: &Path,
    socket_path: &Path,
) -> std::io::Result<()> {
    use object_store::ObjectStore;
    use object_store::path::Path as StorePath;

    validate_volume_name(name)?;

    let store = config
        .build_store()
        .map_err(|e| std::io::Error::other(format!("store: {e}")))?;
    let rt = tokio::runtime::Runtime::new()?;

    // Step 1: resolve name → ULID.
    let name_key = StorePath::from(format!("names/{name}"));
    let volume_id = rt.block_on(async {
        let data = store.get(&name_key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                std::io::Error::other(format!("volume '{name}' not found in store"))
            }
            e => std::io::Error::other(format!("reading names/{name}: {e}")),
        })?;
        let bytes = data
            .bytes()
            .await
            .map_err(|e| std::io::Error::other(format!("reading names/{name}: {e}")))?;
        std::str::from_utf8(&bytes)
            .map(|s| s.trim().to_owned())
            .map_err(|e| std::io::Error::other(format!("names/{name}: {e}")))
    })?;

    // Validate the ULID before using it as a directory name.
    ulid::Ulid::from_string(&volume_id)
        .map_err(|e| std::io::Error::other(format!("names/{name} contains invalid ULID: {e}")))?;

    let vol_dir = data_dir.join("by_id").join(&volume_id);
    if vol_dir.exists() {
        return Err(std::io::Error::other(format!(
            "volume already present locally: {volume_id}"
        )));
    }

    // Step 2: download manifest.toml and volume.pub.
    let manifest_key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
    let manifest_bytes = rt.block_on(async {
        let data = store.get(&manifest_key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => std::io::Error::other(format!(
                "manifest not found in store for volume '{name}' ({volume_id})"
            )),
            e => std::io::Error::other(format!("downloading manifest: {e}")),
        })?;
        data.bytes()
            .await
            .map_err(|e| std::io::Error::other(format!("reading manifest: {e}")))
    })?;

    let manifest: toml::Table = toml::from_str(
        std::str::from_utf8(&manifest_bytes)
            .map_err(|e| std::io::Error::other(format!("manifest is not valid utf-8: {e}")))?,
    )
    .map_err(|e| std::io::Error::other(format!("parsing manifest.toml: {e}")))?;

    let pub_key_bytes = rt.block_on(async {
        let pub_key = StorePath::from(format!("by_id/{volume_id}/volume.pub"));
        let data = store
            .get(&pub_key)
            .await
            .map_err(|e| std::io::Error::other(format!("downloading volume.pub: {e}")))?;
        data.bytes()
            .await
            .map_err(|e| std::io::Error::other(format!("reading volume.pub: {e}")))
    })?;

    // Step 3: reconstruct local directory skeleton.
    std::fs::create_dir_all(&vol_dir)?;

    std::fs::write(vol_dir.join("volume.name"), name)?;

    let size = manifest
        .get("size")
        .and_then(|v| v.as_integer())
        .ok_or_else(|| std::io::Error::other("manifest.toml missing 'size'"))?;
    std::fs::write(vol_dir.join("volume.size"), size.to_string())?;

    if manifest
        .get("readonly")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        std::fs::write(vol_dir.join("volume.readonly"), "")?;
    }

    if let Some(origin) = manifest.get("origin").and_then(|v| v.as_str()) {
        std::fs::write(vol_dir.join("volume.parent"), origin)?;
    }

    std::fs::write(vol_dir.join("volume.pub"), &pub_key_bytes)?;

    // Create segments/ so discover_volumes picks up the volume and the
    // coordinator runs prefetch_indexes to download the .idx files.
    std::fs::create_dir_all(vol_dir.join("segments"))?;

    // Step 4: create by_name symlink.
    let by_name_dir = data_dir.join("by_name");
    std::fs::create_dir_all(&by_name_dir)?;
    let symlink_path = by_name_dir.join(name);
    if symlink_path.is_symlink() || symlink_path.exists() {
        // Clean up and fail cleanly.
        let _ = std::fs::remove_dir_all(&vol_dir);
        return Err(std::io::Error::other(format!(
            "volume name already in use locally: {name}"
        )));
    }
    std::os::unix::fs::symlink(format!("../by_id/{volume_id}"), &symlink_path)?;

    println!("pulled {name} ({volume_id})");

    // Step 5: signal coordinator to rescan (best-effort).
    if coordinator_client::rescan(socket_path).is_err() {
        eprintln!("warning: coordinator unreachable; volume will be picked up on next scan");
    }

    Ok(())
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
