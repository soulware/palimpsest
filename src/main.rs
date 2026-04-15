mod volume_up;

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

use elide_core::signing::{VOLUME_KEY_FILE, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE};
use elide_core::volume;

use elide::{
    coordinator_client, extents, inspect, inspect_files, ls, nbd, parse_size, resolve_volume_dir,
    resolve_volume_size, tui, validate_volume_name,
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
    /// Interactively inspect a volume's ext4 filesystem and its backing extents
    Tui {
        /// Volume name
        name: String,
    },

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
        /// Unix socket path for the NBD server. Mutually exclusive with --port.
        #[arg(long, conflicts_with = "port")]
        socket: Option<PathBuf>,
        /// Serve as a read-only block device (auto-detected for imported bases;
        /// use this flag to explicitly serve a writable volume read-only)
        #[arg(long)]
        readonly: bool,
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

    /// Background daemon for `volume up` (not for direct use)
    #[command(hide = true, name = "volume-daemon")]
    VolumeDaemon {
        /// Path to the volume directory (by_id/<ulid>/)
        vol_dir: PathBuf,
        /// Path to mount the filesystem
        mountpoint: PathBuf,
        /// Format with ext4 if no filesystem is detected (skips prompt)
        #[arg(long)]
        format: bool,
    },

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
    /// List volumes in the data directory (writable by default)
    List {
        /// List only readonly volumes (imported bases)
        #[arg(long, conflicts_with = "all")]
        readonly: bool,
        /// List all volumes (writable and readonly)
        #[arg(long)]
        all: bool,
    },

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

    /// Create a new volume branched from a source volume.
    ///
    /// `--from` accepts three forms:
    ///   - `<name>` — branches from the volume's latest snapshot
    ///   - `<vol_ulid>` — bare ULID, branches from latest snapshot
    ///   - `<vol_ulid>/<snap_ulid>` — pins to a specific snapshot
    ///
    /// If the source is not found locally, the volume and its ancestor
    /// chain are auto-pulled from the remote store before forking.
    ///
    /// ULID-wins: if the `--from` value parses as a valid ULID it is always
    /// treated as a volume ID, never as a volume name.
    Fork {
        /// Name for the new volume
        fork_name: String,
        /// Source: `<name>`, `<vol_ulid>`, or `<vol_ulid>/<snap_ulid>`
        #[arg(long)]
        from: String,
    },

    /// Create a new volume
    ///
    /// With `--from`, creates a fork of an existing volume (equivalent to
    /// `volume fork --from`).  Without `--from`, creates a fresh empty volume
    /// and `--size` is required.
    Create {
        /// Volume name
        name: String,
        /// Volume size (e.g. "4G", "512M"). Required for fresh volumes;
        /// conflicts with `--from` (size is inherited from the source).
        #[arg(long, conflicts_with = "from")]
        size: Option<String>,
        /// Fork from an existing volume: `<name>`, `<vol_ulid>`, or
        /// `<vol_ulid>/<snap_ulid>`
        #[arg(long)]
        from: Option<String>,
        /// Port for the NBD server (exposes the volume over NBD on first start)
        #[arg(long, conflicts_with = "nbd_socket")]
        nbd_port: Option<u16>,
        /// Address to bind the NBD server (default: 127.0.0.1)
        #[arg(long)]
        nbd_bind: Option<String>,
        /// Unix socket path for the NBD server. Omit the path to use the
        /// default (nbd.sock inside the volume directory).
        #[arg(long, conflicts_with = "nbd_port", num_args = 0..=1, default_missing_value = "nbd.sock")]
        nbd_socket: Option<PathBuf>,
    },

    /// Update configuration for a running volume
    Update {
        /// Volume name
        name: String,
        /// Change the NBD server port (restarts the volume process)
        #[arg(long, conflicts_with = "nbd_socket")]
        nbd_port: Option<u16>,
        /// Change the NBD bind address (restarts the volume process)
        #[arg(long)]
        nbd_bind: Option<String>,
        /// Set or change the Unix socket path for the NBD server. Omit the
        /// path to use the default (nbd.sock inside the volume directory).
        /// Restarts the volume process.
        #[arg(long, conflicts_with = "nbd_port", num_args = 0..=1, default_missing_value = "nbd.sock")]
        nbd_socket: Option<PathBuf>,
        /// Disable NBD serving (removes nbd.port, nbd.bind, nbd.socket; restarts the volume process)
        #[arg(long)]
        no_nbd: bool,
    },

    /// Show the running status of a volume
    Status {
        /// Volume name
        name: String,
    },

    /// Import an OCI image into a new readonly volume (sync by default)
    Import(ImportArgs),

    /// Evict locally cached data so it is demand-fetched on next read
    ///
    /// Deletes cache/<ulid>.body and cache/<ulid>.present for segments where
    /// index/<ulid>.idx confirms S3 upload. The LBA map (index/) is untouched;
    /// evicted bodies are re-fetched from S3 on next access. Only safe on
    /// volumes with S3 backing.
    ///
    /// --segment <ulid>: evict one specific body by ULID.
    Evict {
        /// Evict a single segment body by ULID
        #[arg(long)]
        segment: Option<String>,

        /// Volume name
        name: String,
    },

    /// Reclaim fragmented storage via alias-merge extent rewriting.
    ///
    /// Scans the volume for hashes whose stored payload has detectable
    /// dead weight (from in-place overwrites that split the original
    /// entry) and rewrites them as fresh compact entries via the
    /// internal-origin write path. The old bloated bodies are left
    /// orphaned for coordinator GC to reclaim on its next pass.
    ///
    /// Runs entirely on the volume; heavy work (read + hash + compress)
    /// happens off the actor thread so NBD writes are not blocked for
    /// the full duration of the pass.
    ///
    /// Safe on any running volume. Idempotent — re-running against
    /// already-compact state is a fast no-op.
    Reclaim {
        /// Volume name
        name: String,
    },

    /// Stop all processes for a volume and remove its directory
    Delete {
        /// Volume name
        name: String,
    },

    /// Mount a volume as a block device and make its filesystem available.
    ///
    /// Starts an embedded NBD server, attaches /dev/nbdN, and mounts at the
    /// given path. On first use (blank device) you will be prompted to format;
    /// pass --format to skip the prompt. Runs a background daemon that owns
    /// the full lifecycle; use `volume down` to unmount cleanly.
    Up {
        /// Volume name
        name: String,
        /// Path to mount the filesystem
        #[arg(default_value = "/mnt")]
        mountpoint: PathBuf,
        /// Format the device with ext4 if no filesystem is detected (no prompt)
        #[arg(long)]
        format: bool,
    },

    /// Unmount a volume previously started with `volume up`.
    Down {
        /// Volume name
        name: String,
    },

    /// Stop a running volume (flushes and halts the NBD server; drain/GC continue)
    Stop {
        /// Volume name
        name: String,
    },

    /// Start a previously stopped volume
    Start {
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

    /// Download a volume from the store as a readonly ancestor.
    ///
    /// Accepts either a volume name (resolved via `names/<name>` in the store)
    /// or an explicit `<vol_ulid>[/<snap_ulid>]`. Pulled volumes land under
    /// `readonly/<vol_ulid>/` and are never supervised locally — they exist
    /// only as fork ancestors. The full ancestor chain is walked and every
    /// ancestor not already present locally is pulled too.
    Pull {
        /// Volume spec: `<name>` or `<vol_ulid>` or `<vol_ulid>/<snap_ulid>`
        spec: String,
    },
}

#[derive(clap::Args)]
#[command(args_conflicts_with_subcommands = true)]
struct ImportArgs {
    #[command(subcommand)]
    command: Option<ImportSubcommand>,

    /// Volume name to create
    name: Option<String>,

    /// OCI image reference (e.g. ubuntu:22.04, ghcr.io/org/image:tag)
    oci_ref: Option<String>,

    /// Create a fork with this name immediately after the import completes
    #[arg(long, conflicts_with = "detach")]
    fork: Option<String>,

    /// Name of an existing volume whose extent index contributes to the new
    /// volume's hash pool. Repeat the flag to union multiple sources.
    ///
    /// The new volume is written as a fresh import but any 4 KiB block whose
    /// content hash matches a block in any listed source is recorded as a
    /// `DedupRef` pointing back at that source's segment. Sources must exist
    /// locally (with their index files materialised). Source data is never
    /// merged into the new volume's read path — only its extent index is
    /// consulted for dedup.
    #[arg(long = "extents-from")]
    extents_from: Vec<String>,

    /// Start the import in the background and return immediately
    #[arg(long)]
    detach: bool,
}

#[derive(Subcommand)]
enum ImportSubcommand {
    /// Show the state of a running or completed import
    Status {
        /// Volume name
        name: String,
    },
    /// Stream output from a running import
    Attach {
        /// Volume name
        name: String,
    },
}

fn main() {
    tracing_log::LogTracer::init().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let args = Args::parse();

    let socket_path = args.data_dir.join("control.sock");
    let by_id_dir = args.data_dir.join("by_id");

    match args.command {
        Command::Tui { name } => {
            if let Err(e) = validate_volume_name(&name) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
            let vol_dir = resolve_volume_dir(&args.data_dir, &name);
            if let Err(e) = tui::run(&vol_dir, &name) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }

        Command::Volume { command } => match command {
            VolumeCommand::List { readonly, all } => {
                let filter = if all {
                    ListFilter::All
                } else if readonly {
                    ListFilter::Readonly
                } else {
                    ListFilter::Writable
                };
                if let Err(e) = list_volumes(&args.data_dir, filter) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Info { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = inspect::run(&vol_dir, &by_id_dir) {
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

            VolumeCommand::Snapshot { name } => match snapshot_volume(&args.data_dir, &name) {
                Ok(ulid) => println!("{ulid}"),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            },

            VolumeCommand::Fork { fork_name, from } => {
                if let Err(e) = validate_volume_name(&fork_name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                if let Err(e) = create_fork(
                    &args.data_dir,
                    &fork_name,
                    &from,
                    &socket_path,
                    &by_id_dir,
                    None,
                ) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Create {
                name,
                size,
                from,
                nbd_port,
                nbd_bind,
                nbd_socket,
            } => {
                let nbd = if let Some(path) = nbd_socket {
                    Some(elide_core::config::NbdConfig {
                        socket: Some(path),
                        ..Default::default()
                    })
                } else {
                    nbd_port.map(|port| elide_core::config::NbdConfig {
                        port: Some(port),
                        bind: nbd_bind,
                        ..Default::default()
                    })
                };

                if let Some(from) = &from {
                    if let Err(e) = validate_volume_name(&name) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                    if let Err(e) =
                        create_fork(&args.data_dir, &name, from, &socket_path, &by_id_dir, nbd)
                    {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                } else {
                    if let Err(e) = create_volume(&args.data_dir, &name, size.as_deref(), nbd) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                    if coordinator_client::rescan(&socket_path).is_err() {
                        eprintln!(
                            "warning: coordinator unreachable; volume will be picked up on next scan"
                        );
                    }
                }
            }

            VolumeCommand::Update {
                name,
                nbd_port,
                nbd_bind,
                nbd_socket,
                no_nbd,
            } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = update_volume(&vol_dir, nbd_port, nbd_bind, nbd_socket, no_nbd) {
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

            VolumeCommand::Import(import_args) => match import_args.command {
                Some(ImportSubcommand::Status { name }) => {
                    let resp = coordinator_client::import_status_by_name(&socket_path, &name)
                        .unwrap_or_else(|e| format!("err {e}"));
                    match resp.split_once(' ') {
                        Some(("ok", state)) => println!("{name}: {state}"),
                        Some(("err", msg)) => {
                            eprintln!("{name}: {msg}");
                            std::process::exit(1);
                        }
                        _ => println!("{name}: {resp}"),
                    }
                }
                Some(ImportSubcommand::Attach { name }) => {
                    let mut stdout = std::io::stdout();
                    if let Err(e) =
                        coordinator_client::import_attach_by_name(&socket_path, &name, &mut stdout)
                    {
                        eprintln!("import failed: {e}");
                        std::process::exit(1);
                    }
                }
                None => {
                    let (name, oci_ref) = match (import_args.name, import_args.oci_ref) {
                        (Some(n), Some(r)) => (n, r),
                        _ => {
                            eprintln!(
                                "error: usage: elide volume import <name> <oci_ref> [--fork <name>] [--detach]"
                            );
                            std::process::exit(1);
                        }
                    };
                    if let Err(e) = validate_volume_name(&name) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                    if let Err(e) = coordinator_client::import_start(
                        &socket_path,
                        &name,
                        &oci_ref,
                        &import_args.extents_from,
                    ) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                    if import_args.detach {
                        eprintln!("Import started for '{name}'.");
                        eprintln!("  elide volume import attach {name}   # stream output");
                        eprintln!("  elide volume import status {name}   # check state");
                    } else {
                        // Sync: stream output and wait for completion.
                        // Install a Ctrl-C handler so the user gets a clear
                        // message if they interrupt: the import keeps running
                        // in the background and can be re-attached later.
                        let name_for_ctrlc = name.clone();
                        ctrlc::set_handler(move || {
                            eprintln!("\nImport still running in background.");
                            eprintln!("  elide volume import attach {name_for_ctrlc}");
                            eprintln!("  elide volume import status {name_for_ctrlc}");
                            std::process::exit(130);
                        })
                        .ok();

                        let mut stdout = std::io::stdout();
                        if let Err(e) = coordinator_client::import_attach_by_name(
                            &socket_path,
                            &name,
                            &mut stdout,
                        ) {
                            eprintln!("import failed: {e}");
                            std::process::exit(1);
                        }
                        // Optionally create a fork immediately after import.
                        if let Some(fork_name) = import_args.fork
                            && let Err(e) = create_fork(
                                &args.data_dir,
                                &fork_name,
                                &name,
                                &socket_path,
                                &by_id_dir,
                                None,
                            )
                        {
                            eprintln!("error creating fork '{fork_name}': {e}");
                            std::process::exit(1);
                        }
                    }
                }
            },

            VolumeCommand::Evict { segment, name } => {
                match coordinator_client::evict_volume(&socket_path, &name, segment.as_deref()) {
                    Ok(n) => {
                        let label = if n == 1 { "segment" } else { "segments" };
                        println!("evicted {n} {label}");
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }

            VolumeCommand::Reclaim { name } => {
                match coordinator_client::reclaim_volume(&socket_path, &name) {
                    Ok(stats) => {
                        if stats.candidates_scanned == 0 {
                            println!("nothing to reclaim");
                        } else {
                            println!(
                                "reclaimed {} run{} ({} bytes) from {} candidate{}{}",
                                stats.runs_rewritten,
                                if stats.runs_rewritten == 1 { "" } else { "s" },
                                stats.bytes_rewritten,
                                stats.candidates_scanned,
                                if stats.candidates_scanned == 1 {
                                    ""
                                } else {
                                    "s"
                                },
                                if stats.discarded > 0 {
                                    format!(
                                        " ({} discarded due to concurrent writes)",
                                        stats.discarded
                                    )
                                } else {
                                    String::new()
                                },
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }

            VolumeCommand::Delete { name } => {
                if let Err(e) = coordinator_client::delete_volume(&socket_path, &name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Up {
                name,
                mountpoint,
                format,
            } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = volume_up::cmd_volume_up(&vol_dir, &mountpoint, format) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Down { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = volume_up::cmd_volume_down(&vol_dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Stop { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = stop_volume(&vol_dir, &name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Start { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                if let Err(e) = start_volume(&vol_dir, &name, &args.data_dir, &socket_path) {
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
                    RemoteCommand::Pull { spec } => {
                        if let Err(e) = remote_pull(&config, &spec, &args.data_dir, &socket_path) {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                }
            }
        },

        Command::VolumeDaemon {
            vol_dir,
            mountpoint,
            format,
        } => {
            if let Err(e) = volume_up::run_volume_daemon(&vol_dir, &mountpoint, format) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }

        Command::ServeVolume {
            fork_dir,
            size,
            bind,
            port,
            socket,
            readonly,
        } => {
            // In the flat layout, fork_dir IS the volume directory.
            let size_bytes = resolve_volume_size(&fork_dir, size.as_deref())
                .expect("failed to determine volume size");
            let fetch_config =
                elide_fetch::FetchConfig::load(&fork_dir).expect("failed to load fetch config");
            let nbd_bind = if let Some(path) = socket {
                Some(nbd::NbdBind::Unix(path))
            } else {
                port.map(|p| nbd::NbdBind::Tcp { bind, port: p })
            };
            // Serve as readonly if explicitly requested or if the volume.readonly
            // marker is present (imported bases have no private key on disk).
            if readonly || fork_dir.join("volume.readonly").exists() {
                nbd::run_volume_readonly(&fork_dir, size_bytes, nbd_bind, fetch_config)
                    .expect("readonly NBD server error");
            } else {
                std::fs::create_dir_all(&fork_dir).expect("failed to create fork directory");
                let signer = if fork_dir.join(VOLUME_KEY_FILE).exists() {
                    elide_core::signing::read_lineage_verifying_signature(
                        &fork_dir,
                        VOLUME_PUB_FILE,
                        VOLUME_PROVENANCE_FILE,
                    )
                    .expect("volume.provenance signature check failed");
                    elide_core::signing::load_signer(&fork_dir, VOLUME_KEY_FILE)
                        .expect("failed to load volume signing key")
                } else {
                    let key = elide_core::signing::generate_keypair(
                        &fork_dir,
                        VOLUME_KEY_FILE,
                        VOLUME_PUB_FILE,
                    )
                    .expect("failed to generate volume keypair");
                    elide_core::signing::write_provenance(
                        &fork_dir,
                        &key,
                        VOLUME_PROVENANCE_FILE,
                        &elide_core::signing::ProvenanceLineage::default(),
                    )
                    .expect("failed to write volume.provenance");
                    elide_core::signing::load_signer(&fork_dir, VOLUME_KEY_FILE)
                        .expect("failed to load volume signing key")
                };
                nbd::run_volume_signed(&fork_dir, size_bytes, nbd_bind, signer, fetch_config)
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

/// Snapshot a volume by asking the coordinator to orchestrate the full
/// sequence (flush → drain → sign manifest → upload). The coordinator is
/// the only path — there is no in-process fallback, because the snapshot
/// cannot write a signed `.manifest` file without the drain step, and
/// the drain step requires the coordinator's per-volume lock and S3
/// client.
///
/// Returns the snapshot ULID on success.
fn snapshot_volume(data_dir: &Path, name: &str) -> std::io::Result<String> {
    let socket = data_dir.join("control.sock");
    coordinator_client::snapshot_volume(&socket, name)
}

enum ListFilter {
    Writable,
    Readonly,
    All,
}

fn list_volumes(data_dir: &Path, filter: ListFilter) -> std::io::Result<()> {
    let by_name_dir = data_dir.join("by_name");
    let mut entries: Vec<(String, String)> = Vec::new();
    match std::fs::read_dir(&by_name_dir) {
        Ok(dir_entries) => {
            for entry in dir_entries {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                // Resolve symlink to get the actual volume dir.
                let vol_dir = std::fs::read_link(entry.path())
                    .ok()
                    .map(|target| {
                        if target.is_absolute() {
                            target
                        } else {
                            by_name_dir.join(target)
                        }
                    })
                    .unwrap_or_else(|| entry.path());
                let is_readonly = vol_dir.join("volume.readonly").exists();
                let include = match filter {
                    ListFilter::All => true,
                    ListFilter::Readonly => is_readonly,
                    ListFilter::Writable => !is_readonly,
                };
                if include {
                    let suffix = if vol_dir.join(STOPPED_FILE).exists() {
                        "  (stopped)".to_owned()
                    } else if vol_dir.join("import.lock").exists() {
                        "  (importing)".to_owned()
                    } else {
                        // Show the NBD endpoint when the volume is running.
                        let ep = elide_core::config::VolumeConfig::read(&vol_dir)
                            .ok()
                            .and_then(|cfg| cfg.nbd)
                            .and_then(|nbd| nbd.endpoint(&vol_dir));
                        match ep {
                            Some(ep) => format!("  ({ep})"),
                            None => String::new(),
                        }
                    };
                    entries.push((name, suffix));
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    if entries.is_empty() {
        println!("no volumes found in {}", data_dir.display());
    } else {
        for (name, suffix) in &entries {
            println!("{name}{suffix}");
        }
    }
    Ok(())
}

fn create_volume(
    data_dir: &Path,
    name: &str,
    size: Option<&str>,
    nbd: Option<elide_core::config::NbdConfig>,
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
    std::fs::create_dir_all(vol_dir.join("pending"))?;
    std::fs::create_dir_all(vol_dir.join("index"))?;
    std::fs::create_dir_all(vol_dir.join("cache"))?;

    let key = elide_core::signing::generate_keypair(&vol_dir, VOLUME_KEY_FILE, VOLUME_PUB_FILE)?;
    elide_core::signing::write_provenance(
        &vol_dir,
        &key,
        VOLUME_PROVENANCE_FILE,
        &elide_core::signing::ProvenanceLineage::default(),
    )?;

    elide_core::config::VolumeConfig {
        name: Some(name.to_owned()),
        size: Some(bytes),
        nbd,
    }
    .write(&vol_dir)?;

    std::fs::create_dir_all(&by_name_dir)?;
    let by_name_path = by_name_dir.join(name);
    std::os::unix::fs::symlink(format!("../by_id/{vol_ulid}"), &by_name_path)?;

    println!("{}", by_name_path.display());
    println!("{}", vol_dir.display());
    Ok(())
}

/// Create a new volume forked from a source.
///
/// `from` is one of:
///   - `<vol_ulid>/<snap_ulid>` — explicit pin to a specific snapshot
///   - `<vol_ulid>` — bare ULID, resolved by ID (snapshot chosen automatically)
///   - `<name>` — volume name, resolved locally then by remote store
///
/// All three forms try local first, then fall back to the remote store
/// (auto-pulling the volume and its ancestor chain).
///
/// ULID-wins rule: if `from` (or the part before `/`) parses as a valid ULID
/// it is always treated as one, never looked up as a volume name. This
/// prevents ambiguity when a volume is named with a ULID string.
///
/// For writable volumes, an implicit snapshot is taken first. For readonly
/// volumes (pulled or already local), the latest snapshot is discovered from
/// the remote store. For explicit pins the caller already chose a snapshot.
fn create_fork(
    data_dir: &Path,
    fork_name: &str,
    from: &str,
    socket_path: &Path,
    by_id_dir: &Path,
    nbd: Option<elide_core::config::NbdConfig>,
) -> std::io::Result<()> {
    validate_volume_name(fork_name)?;

    let by_name_dir = data_dir.join("by_name");
    let symlink_path = by_name_dir.join(fork_name);
    if symlink_path.exists() {
        return Err(std::io::Error::other(format!(
            "volume already exists: {fork_name}"
        )));
    }

    // Parse `from` into one of three forms.
    enum FromSpec {
        ExplicitPin(ulid::Ulid, ulid::Ulid),
        BareUlid(ulid::Ulid),
        Name,
    }

    let spec = if let Some((vol, snap)) = from.split_once('/') {
        let vol = ulid::Ulid::from_string(vol)
            .map_err(|e| std::io::Error::other(format!("invalid volume ULID in --from: {e}")))?;
        let snap = ulid::Ulid::from_string(snap)
            .map_err(|e| std::io::Error::other(format!("invalid snapshot ULID in --from: {e}")))?;
        FromSpec::ExplicitPin(vol, snap)
    } else if let Ok(vol) = ulid::Ulid::from_string(from) {
        FromSpec::BareUlid(vol)
    } else {
        FromSpec::Name
    };

    // Resolve the source directory. Try local first; for ULID-based forms
    // fall back to auto-pulling from the remote store. For names, try local
    // `by_name/` first, then resolve the name in the remote store.
    //
    // `pulled_vol_ulid` is set when we auto-pulled from the store so we can
    // discover the latest remote snapshot later.
    let mut pulled_vol_ulid: Option<String> = None;

    let source_fork_dir = match &spec {
        FromSpec::ExplicitPin(vol_ulid, _) | FromSpec::BareUlid(vol_ulid) => {
            let ulid_str = vol_ulid.to_string();
            let dir = volume::resolve_ancestor_dir(by_id_dir, &ulid_str);
            if dir.exists() {
                dir
            } else {
                let config = load_fetch_config(data_dir, &ulid_str)?;
                eprintln!("pulling {ulid_str} from remote store...");
                remote_pull(&config, from, data_dir, socket_path)?;
                let dir = volume::resolve_ancestor_dir(by_id_dir, &ulid_str);
                if !dir.exists() {
                    return Err(std::io::Error::other(format!(
                        "source volume {ulid_str} not found in remote store"
                    )));
                }
                pulled_vol_ulid = Some(ulid_str);
                dir
            }
        }
        FromSpec::Name => {
            let local = resolve_volume_dir(data_dir, from);
            if local.exists() {
                local
            } else {
                // Name not found locally — try the remote store.
                let config = load_fetch_config_for_name(data_dir, from)?;
                eprintln!("pulling '{from}' from remote store...");
                remote_pull(&config, from, data_dir, socket_path)?;
                // remote_pull resolved the name to a ULID and pulled into
                // readonly/<ulid>/. Find it by re-resolving the pull spec.
                let store = config
                    .build_store()
                    .map_err(|e| std::io::Error::other(format!("store: {e}")))?;
                let rt = tokio::runtime::Runtime::new()?;
                let ulid_str = resolve_pull_spec(&rt, &*store, from)?;
                let dir = volume::resolve_ancestor_dir(by_id_dir, &ulid_str);
                if !dir.exists() {
                    return Err(std::io::Error::other(format!(
                        "volume '{from}' not found in remote store"
                    )));
                }
                pulled_vol_ulid = Some(ulid_str);
                dir
            }
        }
    };

    if source_fork_dir.join("import.lock").exists() {
        return Err(std::io::Error::other(format!(
            "volume '{from}' is still importing; wait for import to complete before forking"
        )));
    }

    // Determine the snapshot ULID to branch from.
    let snap_ulid: Option<ulid::Ulid> = match &spec {
        FromSpec::ExplicitPin(_, snap) => Some(*snap),
        _ if source_fork_dir.join("volume.readonly").exists() => {
            // Readonly volume (pulled or already local). Prefer the local
            // snapshots/ directory — it is always authoritative when present
            // (e.g. after a local import). Fall back to the remote store
            // only when snapshots/ is missing or empty (volume pulled from
            // the store but coordinator hasn't prefetched yet).
            if let Some(snap) = volume::latest_snapshot(&source_fork_dir)? {
                Some(snap)
            } else {
                // Canonicalize through any by_name/ symlink so file_name()
                // returns the ULID, not the human volume name.
                let real_dir = std::fs::canonicalize(&source_fork_dir)?;
                let vol_id = pulled_vol_ulid.as_deref().unwrap_or_else(|| {
                    real_dir
                        .file_name()
                        .and_then(|n| n.to_str())
                        .expect("readonly dir must have a ULID name")
                });
                let config = load_fetch_config(data_dir, vol_id)?;
                Some(resolve_latest_remote_snapshot(&config, vol_id)?)
            }
        }
        FromSpec::BareUlid(_) => {
            // Writable volume addressed by ULID: take an implicit snapshot.
            // The coordinator identifies volumes by name, so read it from
            // the volume config.
            let name = elide_core::config::VolumeConfig::read(&source_fork_dir)?
                .name
                .ok_or_else(|| std::io::Error::other("source volume has no name in volume.toml"))?;
            snapshot_volume(data_dir, &name)?;
            None // fork_volume will use latest_snapshot()
        }
        FromSpec::Name => {
            // Writable local volume addressed by name.
            snapshot_volume(data_dir, from)?;
            None
        }
    };

    let new_vol_ulid = ulid::Ulid::new().to_string();
    let new_fork_dir = data_dir.join("by_id").join(&new_vol_ulid);

    let cleanup = |new_fork_dir: &Path, symlink_path: &Path| {
        let _ = std::fs::remove_file(symlink_path);
        let _ = std::fs::remove_dir_all(new_fork_dir);
    };

    match snap_ulid {
        Some(snap) => {
            volume::fork_volume_at(&new_fork_dir, &source_fork_dir, snap)?;
        }
        None => {
            volume::fork_volume(&new_fork_dir, &source_fork_dir)?;
        }
    }

    let src_cfg = elide_core::config::VolumeConfig::read(&source_fork_dir).map_err(|e| {
        cleanup(&new_fork_dir, &symlink_path);
        std::io::Error::other(format!("failed to read source volume config: {e}"))
    })?;
    let size = src_cfg.size.ok_or_else(|| {
        cleanup(&new_fork_dir, &symlink_path);
        std::io::Error::other("source volume has no size (import may not have completed)")
    })?;
    if let Err(e) = (elide_core::config::VolumeConfig {
        name: Some(fork_name.to_owned()),
        size: Some(size),
        nbd,
    }
    .write(&new_fork_dir))
    {
        cleanup(&new_fork_dir, &symlink_path);
        return Err(std::io::Error::other(format!(
            "failed to write volume config: {e}"
        )));
    }

    if let Err(e) = std::fs::create_dir_all(&by_name_dir) {
        cleanup(&new_fork_dir, &symlink_path);
        return Err(std::io::Error::other(format!(
            "failed to create by_name dir: {e}"
        )));
    }

    if let Err(e) = std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid}"), &symlink_path) {
        cleanup(&new_fork_dir, &symlink_path);
        return Err(std::io::Error::other(format!(
            "failed to create by_name symlink: {e}"
        )));
    }

    // fork_volume wrote keypair + signed provenance already; nothing to add.

    println!("{}", new_fork_dir.display());
    if coordinator_client::rescan(socket_path).is_err() {
        eprintln!("warning: coordinator unreachable; volume will be picked up on next scan");
    }
    Ok(())
}

/// Load the fetch config, or return a clear error mentioning the volume ULID.
fn load_fetch_config(data_dir: &Path, ulid_str: &str) -> std::io::Result<elide_fetch::FetchConfig> {
    match elide_fetch::FetchConfig::load(data_dir) {
        Ok(Some(c)) => Ok(c),
        Ok(None) => Err(std::io::Error::other(format!(
            "source volume {ulid_str} not found locally and no store configured for remote pull"
        ))),
        Err(e) => Err(std::io::Error::other(format!(
            "source volume {ulid_str} not found locally; failed to load store config: {e}"
        ))),
    }
}

/// Load the fetch config, or return a clear error mentioning a volume name.
fn load_fetch_config_for_name(
    data_dir: &Path,
    name: &str,
) -> std::io::Result<elide_fetch::FetchConfig> {
    match elide_fetch::FetchConfig::load(data_dir) {
        Ok(Some(c)) => Ok(c),
        Ok(None) => Err(std::io::Error::other(format!(
            "volume '{name}' not found locally and no store configured for remote pull"
        ))),
        Err(e) => Err(std::io::Error::other(format!(
            "volume '{name}' not found locally; failed to load store config: {e}"
        ))),
    }
}

/// Query the remote store for the latest snapshot of a volume.
///
/// Lists `by_id/<volume_id>/snapshots/` and returns the maximum snapshot ULID.
/// Filemaps (filenames containing `.`) are filtered out.
fn resolve_latest_remote_snapshot(
    config: &elide_fetch::FetchConfig,
    volume_id: &str,
) -> std::io::Result<ulid::Ulid> {
    use object_store::path::Path as StorePath;

    let store = config
        .build_store()
        .map_err(|e| std::io::Error::other(format!("store: {e}")))?;
    let rt = tokio::runtime::Runtime::new()?;

    let prefix = StorePath::from(format!("by_id/{volume_id}/snapshots/"));
    let objects: Vec<object_store::ObjectMeta> =
        rt.block_on(async {
            use futures::TryStreamExt;
            store.list(Some(&prefix)).try_collect().await.map_err(|e| {
                std::io::Error::other(format!("listing snapshots for {volume_id}: {e}"))
            })
        })?;

    let mut latest: Option<ulid::Ulid> = None;
    for obj in &objects {
        let Some(name) = obj.location.filename() else {
            continue;
        };
        if name.contains('.') {
            continue;
        }
        if let Ok(ulid) = ulid::Ulid::from_string(name) {
            latest = Some(match latest {
                Some(prev) if ulid > prev => ulid,
                Some(prev) => prev,
                None => ulid,
            });
        }
    }

    latest.ok_or_else(|| {
        std::io::Error::other(format!(
            "volume {volume_id} has no snapshots in the remote store"
        ))
    })
}

/// Update configuration for a volume and restart it if running.
///
/// Writes or removes nbd.port / nbd.bind / nbd.socket, then sends `shutdown`
/// to the volume's control socket. The supervisor restarts the process, picking
/// up the new config.
fn update_volume(
    vol_dir: &Path,
    nbd_port: Option<u16>,
    nbd_bind: Option<String>,
    nbd_socket: Option<PathBuf>,
    no_nbd: bool,
) -> std::io::Result<()> {
    use std::io::{BufRead, Write};
    use std::os::unix::net::UnixStream;

    let mut cfg = elide_core::config::VolumeConfig::read(vol_dir)?;
    if no_nbd {
        cfg.nbd = None;
    } else if let Some(path) = nbd_socket {
        cfg.nbd = Some(elide_core::config::NbdConfig {
            socket: Some(path),
            ..Default::default()
        });
    } else if nbd_port.is_some() || nbd_bind.is_some() {
        let existing = cfg.nbd.get_or_insert_with(Default::default);
        if let Some(port) = nbd_port {
            existing.port = Some(port);
            existing.socket = None; // switching to TCP clears socket
        }
        if let Some(bind) = nbd_bind {
            existing.bind = Some(bind);
        }
    }
    cfg.write(vol_dir)?;

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

const STOPPED_FILE: &str = "volume.stopped";

/// Send a one-shot command to the volume's control socket and return the
/// response line. Returns `None` if the volume is not running.
fn control_call(vol_dir: &Path, cmd: &str) -> std::io::Result<Option<String>> {
    use std::io::{BufRead, Write};
    use std::os::unix::net::UnixStream;

    let sock = vol_dir.join("control.sock");
    match UnixStream::connect(&sock) {
        Ok(mut stream) => {
            writeln!(stream, "{cmd}")?;
            stream.flush()?;
            stream.shutdown(std::net::Shutdown::Write)?;
            let mut reader = std::io::BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line)?;
            Ok(Some(line.trim().to_owned()))
        }
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

fn stop_volume(vol_dir: &Path, name: &str) -> std::io::Result<()> {
    if vol_dir.join("volume.readonly").exists() {
        return Err(std::io::Error::other("volume is readonly; nothing to stop"));
    }
    if vol_dir.join(STOPPED_FILE).exists() {
        println!("{name}: stopped");
        return Ok(());
    }

    // Refuse to stop while an NBD client is connected.
    if let Some(resp) = control_call(vol_dir, "connected")?
        && resp == "ok true"
    {
        return Err(std::io::Error::other(
            "nbd client is connected; disconnect it first",
        ));
    }

    // Write the marker before sending shutdown so the supervisor won't restart.
    std::fs::write(vol_dir.join(STOPPED_FILE), "")?;

    match control_call(vol_dir, "shutdown")? {
        Some(resp) if resp == "ok" => {}
        Some(resp) => {
            let _ = std::fs::remove_file(vol_dir.join(STOPPED_FILE));
            return Err(std::io::Error::other(format!("shutdown failed: {resp}")));
        }
        None => {
            // Already not running — marker is still written, which is correct.
        }
    }

    println!("{name}: stopped");
    Ok(())
}

fn start_volume(
    vol_dir: &Path,
    name: &str,
    data_dir: &Path,
    coordinator_socket: &Path,
) -> std::io::Result<()> {
    if !vol_dir.join(STOPPED_FILE).exists() {
        return Err(std::io::Error::other("volume is not stopped"));
    }

    // Refuse to start if another active volume uses the same NBD endpoint.
    if let Some(conflict) = elide_core::config::find_nbd_conflict(vol_dir, data_dir)? {
        return Err(std::io::Error::other(format!(
            "nbd endpoint {} conflicts with volume '{}'",
            conflict.endpoint, conflict.name,
        )));
    }

    std::fs::remove_file(vol_dir.join(STOPPED_FILE))?;

    // Trigger an immediate rescan so the supervisor picks up the change.
    if let Err(e) = coordinator_client::rescan(coordinator_socket) {
        // Non-fatal: the supervisor will notice on its next poll anyway.
        eprintln!("warning: could not notify coordinator: {e}");
    }

    println!("{name}: started");
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

        // Check for snapshot availability (determines if this volume can be pulled).
        let has_snapshot = rt.block_on(async {
            let snap_prefix = StorePath::from(format!("by_id/{ulid}/snapshots/"));
            let mut listing = store.list(Some(&snap_prefix));
            // We only need to know if at least one snapshot exists.
            use futures::StreamExt;
            listing.next().await.is_some()
        });

        if has_snapshot {
            println!("{name}  {ulid}  [snapshot]");
        } else {
            println!("{name}  {ulid}");
        }
    }

    Ok(())
}

/// Pull a volume (and its full ancestor chain) from the remote store as
/// readonly fork sources.
///
/// `spec` is one of:
///   - a volume name (resolved via `names/<name>` → ULID in the store)
///   - `<vol_ulid>` (address directly by ULID)
///   - `<vol_ulid>/<snap_ulid>` (address a specific snapshot; the ULID part
///     is what gets pulled — the snapshot ULID is retained for the caller
///     that wants to pin provenance, e.g. `volume fork --from`)
///
/// Each pulled volume lands under `data_dir/readonly/<vol_ulid>/` with
/// `volume.readonly`, `volume.pub`, `volume.provenance`, and an empty
/// `index/` dir. The coordinator's next rescan then runs `prefetch_indexes`
/// which downloads the signed `.idx` files into each volume's own `index/`
/// directory (signature verification uses that volume's own `volume.pub`).
///
/// The ancestor chain is walked via the downloaded `volume.provenance`:
/// for every parent ULID not already present in `by_id/` or `readonly/`,
/// its skeleton is pulled too. Encountering a mid-chain ancestor that is
/// already local terminates the walk — the local copy is authoritative.
fn remote_pull(
    config: &elide_fetch::FetchConfig,
    spec: &str,
    data_dir: &Path,
    socket_path: &Path,
) -> std::io::Result<()> {
    let store = config
        .build_store()
        .map_err(|e| std::io::Error::other(format!("store: {e}")))?;
    let rt = tokio::runtime::Runtime::new()?;

    // Step 1: parse `spec` and resolve to a root ULID to pull.
    let root_ulid = resolve_pull_spec(&rt, &*store, spec)?;

    // Step 2: walk the ancestor chain, pulling each skeleton that isn't
    // already local. Start from the requested volume; after each pull, parse
    // its downloaded provenance to find the next parent.
    let mut pulled: Vec<String> = Vec::new();
    let mut next: Option<String> = Some(root_ulid);
    while let Some(ulid_str) = next.take() {
        if ancestor_exists_locally(data_dir, &ulid_str) {
            // Local copy is authoritative; stop walking up from here.
            break;
        }
        let parent_ulid = pull_one_readonly(&rt, &*store, data_dir, &ulid_str)?;
        pulled.push(ulid_str);
        next = parent_ulid;
    }

    if pulled.is_empty() {
        println!("pull: volume already present locally");
    } else {
        for id in &pulled {
            println!("pulled {id}");
        }
    }

    // Step 3: signal coordinator to rescan (best-effort) so it picks up the
    // new readonly volumes and kicks off prefetch_indexes on each.
    if coordinator_client::rescan(socket_path).is_err() {
        eprintln!("warning: coordinator unreachable; volume will be picked up on next scan");
    }

    Ok(())
}

/// Parse a pull spec and resolve it to a volume ULID to fetch.
///
/// Accepts `<name>`, `<vol_ulid>`, or `<vol_ulid>/<snap_ulid>`. For ULID
/// forms the snapshot portion is validated but discarded — this function
/// only decides *which volume* to pull; the snapshot ULID is a pinning
/// concern for the caller (`volume fork --from`), not for pull itself.
fn resolve_pull_spec(
    rt: &tokio::runtime::Runtime,
    store: &dyn object_store::ObjectStore,
    spec: &str,
) -> std::io::Result<String> {
    use object_store::path::Path as StorePath;

    if let Some((vol, snap)) = spec.split_once('/') {
        let vol = ulid::Ulid::from_string(vol)
            .map_err(|e| std::io::Error::other(format!("invalid volume ULID in spec: {e}")))?;
        ulid::Ulid::from_string(snap)
            .map_err(|e| std::io::Error::other(format!("invalid snapshot ULID in spec: {e}")))?;
        return Ok(vol.to_string());
    }
    if let Ok(vol) = ulid::Ulid::from_string(spec) {
        return Ok(vol.to_string());
    }

    // Fallback: treat `spec` as a name and resolve via `names/<name>`.
    validate_volume_name(spec)?;
    let name_key = StorePath::from(format!("names/{spec}"));
    let raw = rt.block_on(async {
        let data = store.get(&name_key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                std::io::Error::other(format!("volume '{spec}' not found in store"))
            }
            e => std::io::Error::other(format!("reading names/{spec}: {e}")),
        })?;
        data.bytes()
            .await
            .map_err(|e| std::io::Error::other(format!("reading names/{spec}: {e}")))
    })?;
    let ulid_str = std::str::from_utf8(&raw)
        .map_err(|e| std::io::Error::other(format!("names/{spec}: {e}")))?
        .trim();
    let parsed = ulid::Ulid::from_string(ulid_str)
        .map_err(|e| std::io::Error::other(format!("names/{spec} contains invalid ULID: {e}")))?;
    Ok(parsed.to_string())
}

/// Return `true` if a local copy of `<ulid>` exists in either tree.
fn ancestor_exists_locally(data_dir: &Path, ulid_str: &str) -> bool {
    data_dir.join("by_id").join(ulid_str).exists()
        || data_dir.join("readonly").join(ulid_str).exists()
}

/// Download the readonly skeleton for one volume into `readonly/<ulid>/`.
///
/// Fetches `manifest.toml`, `volume.pub`, and `volume.provenance` from the
/// store and writes them, along with a `volume.readonly` marker and an empty
/// `index/` directory so `discover_volumes` queues the volume for prefetch.
///
/// Returns the parent volume ULID to pull next, or `None` if this volume is
/// the root of its fork chain. The parent is extracted from the downloaded
/// provenance's `parent` field (`<ulid>/snapshots/<ulid>`), signature-verified
/// against the volume's own `volume.pub`.
fn pull_one_readonly(
    rt: &tokio::runtime::Runtime,
    store: &dyn object_store::ObjectStore,
    data_dir: &Path,
    volume_id: &str,
) -> std::io::Result<Option<String>> {
    use object_store::path::Path as StorePath;

    let vol_dir = data_dir.join("readonly").join(volume_id);
    if vol_dir.exists() {
        return Err(std::io::Error::other(format!(
            "readonly volume already present locally: {volume_id}"
        )));
    }

    // Fetch manifest.toml.
    let manifest_key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
    let manifest_bytes = rt.block_on(async {
        let data = store.get(&manifest_key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => std::io::Error::other(format!(
                "manifest not found in store for volume {volume_id}"
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
    let size = manifest
        .get("size")
        .and_then(|v| v.as_integer())
        .ok_or_else(|| std::io::Error::other("manifest.toml missing 'size'"))?;

    // Fetch volume.pub.
    let pub_key_bytes = rt.block_on(async {
        let key = StorePath::from(format!("by_id/{volume_id}/volume.pub"));
        let data = store
            .get(&key)
            .await
            .map_err(|e| std::io::Error::other(format!("downloading volume.pub: {e}")))?;
        data.bytes()
            .await
            .map_err(|e| std::io::Error::other(format!("reading volume.pub: {e}")))
    })?;

    // Fetch volume.provenance. Every volume — including roots — has a
    // signed provenance recording its lineage (possibly empty). NotFound is
    // a hard error: it means the upstream store is missing load-bearing
    // metadata and we cannot safely materialise a readonly skeleton.
    let provenance_bytes = rt.block_on(async {
        let key = StorePath::from(format!("by_id/{volume_id}/volume.provenance"));
        let data = store.get(&key).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => std::io::Error::other(format!(
                "volume.provenance not found in store for volume {volume_id}"
            )),
            e => std::io::Error::other(format!("downloading volume.provenance: {e}")),
        })?;
        data.bytes()
            .await
            .map_err(|e| std::io::Error::other(format!("reading volume.provenance: {e}")))
    })?;

    // Write the skeleton.
    std::fs::create_dir_all(&vol_dir)?;
    elide_core::config::VolumeConfig {
        name: manifest
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned()),
        size: Some(size as u64),
        nbd: None,
    }
    .write(&vol_dir)?;
    std::fs::write(vol_dir.join("volume.readonly"), "")?;
    std::fs::write(vol_dir.join("volume.pub"), &pub_key_bytes)?;
    std::fs::write(
        vol_dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE),
        &provenance_bytes,
    )?;
    // Empty index/ so discover_volumes queues the volume for prefetch.
    std::fs::create_dir_all(vol_dir.join("index"))?;

    // Parse the downloaded provenance to find the parent ULID (if any).
    // Signature is verified against the just-written `volume.pub`.
    let parent_ulid = match elide_core::signing::read_lineage_verifying_signature(
        &vol_dir,
        elide_core::signing::VOLUME_PUB_FILE,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
    ) {
        Ok(lineage) => lineage.parent.map(|p| p.volume_ulid),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&vol_dir);
            return Err(std::io::Error::other(format!(
                "verifying provenance for {volume_id}: {e}"
            )));
        }
    };

    Ok(parent_ulid)
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
