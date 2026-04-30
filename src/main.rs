use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

use elide_core::signing::{VOLUME_KEY_FILE, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE};
use elide_core::volume;

use elide::{
    coordinator_client, extents, inspect, inspect_files, ls, nbd, parse_size, resolve_volume_dir,
    resolve_volume_size, ublk, validate_volume_name, verify,
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
        #[arg(long, conflicts_with_all = ["ublk", "ublk_id"])]
        port: Option<u16>,
        /// Unix socket path for the NBD server. Mutually exclusive with --port.
        #[arg(long, conflicts_with_all = ["port", "ublk", "ublk_id"])]
        socket: Option<PathBuf>,
        /// Serve as a read-only block device (auto-detected for imported bases;
        /// use this flag to explicitly serve a writable volume read-only)
        #[arg(long)]
        readonly: bool,
        /// Serve over ublk (Linux userspace block device) instead of NBD.
        /// Requires Linux with CONFIG_BLK_DEV_UBLK and the 'ublk' cargo
        /// feature enabled.
        #[arg(long, conflicts_with_all = ["port", "socket"])]
        ublk: bool,
        /// Explicit ublk device id (maps to /dev/ublkb<id>). If omitted and
        /// --ublk is passed, the kernel auto-allocates.
        #[arg(long, conflicts_with_all = ["port", "socket"], requires = "ublk")]
        ublk_id: Option<i32>,
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

    /// Manage ublk devices (diagnostic, Linux-only)
    #[command(hide = true)]
    Ublk {
        #[command(subcommand)]
        command: UblkCommand,
    },
}

#[derive(Subcommand)]
enum UblkCommand {
    /// List ublk devices currently known to the kernel
    List,
    /// Delete a ublk device by id, or all devices with --all
    Delete {
        /// Specific device id (maps to /dev/ublkb<id>)
        id: Option<i32>,
        /// Delete every device found in /sys/class/ublk-char
        #[arg(long, conflicts_with = "id")]
        all: bool,
    },
}

#[derive(Subcommand)]
enum VolumeCommand {
    /// List volumes in the data directory (all by default)
    List {
        /// List only readonly volumes (imported bases)
        #[arg(long, conflicts_with = "rw")]
        ro: bool,
        /// List only writable volumes
        #[arg(long)]
        rw: bool,
        /// Also include pulled ancestors (no name, no by_name/ symlink)
        #[arg(long)]
        all: bool,
    },

    /// Show a human-readable summary of a volume
    Info {
        /// Volume name
        name: String,
    },

    /// Verify every extent's stored body hashes to its declared hash.
    ///
    /// Walks `index/*.idx`, reads each Data/Inline body from its local
    /// location (wal, pending, gc, or cache), and reports any extent whose
    /// body does not match. Evicted bodies are skipped (reported as
    /// skipped). Exit code: 0 clean, 1 if any mismatches, 2 on scan errors.
    Verify {
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

    /// Generate the snapshot filemap for a volume.
    ///
    /// Walks the ext4 layout of the named snapshot (defaulting to the
    /// latest local one) and writes `snapshots/<ulid>.filemap`. Demand-
    /// fetches segment bytes from S3 if needed, so this works on
    /// freshly-pulled volumes whose segments haven't been hydrated.
    ///
    /// The filemap is consumed by `volume import --extents-from <name>`
    /// for file-aware delta compression. Run this against the source
    /// volume before importing if you want delta savings — without it,
    /// the import path skips delta opportunities for that source and
    /// falls back to plain DATA entries.
    GenerateFilemap {
        /// Volume name
        name: String,
        /// Specific snapshot ULID (defaults to the latest local snapshot)
        #[arg(long, value_name = "ULID")]
        snapshot: Option<String>,
    },

    /// Create a new volume.
    ///
    /// With `--from`, creates a writable replica branched from an existing
    /// volume.  `--from` accepts three forms:
    ///   - `<name>` — branches from the volume's latest snapshot
    ///   - `<vol_ulid>` — bare ULID, branches from latest snapshot
    ///   - `<vol_ulid>/<snap_ulid>` — pins to a specific snapshot
    ///
    /// If the source is not found locally, the volume and its ancestor
    /// chain are auto-pulled from the remote store.
    ///
    /// ULID-wins: if the `--from` value parses as a valid ULID it is always
    /// treated as a volume ID, never as a volume name.
    ///
    /// Without `--from`, creates a fresh empty volume and `--size` is
    /// required.
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
        #[arg(long, conflicts_with_all = ["nbd_socket", "ublk", "ublk_id"])]
        nbd_port: Option<u16>,
        /// Address to bind the NBD server (default: 127.0.0.1)
        #[arg(long, conflicts_with_all = ["ublk", "ublk_id"])]
        nbd_bind: Option<String>,
        /// Unix socket path for the NBD server. Omit the path to use the
        /// default (nbd.sock inside the volume directory).
        #[arg(long, conflicts_with_all = ["nbd_port", "ublk", "ublk_id"], num_args = 0..=1, default_missing_value = "nbd.sock")]
        nbd_socket: Option<PathBuf>,
        /// Serve this volume over ublk (Linux userspace block device) instead
        /// of NBD. Mutually exclusive with the --nbd-* flags.
        #[arg(long, conflicts_with_all = ["nbd_port", "nbd_bind", "nbd_socket"])]
        ublk: bool,
        /// Pin an explicit ublk device id (maps to /dev/ublkb<id>). If
        /// omitted, the kernel auto-allocates on first start. Implies --ublk.
        #[arg(long, conflicts_with_all = ["nbd_port", "nbd_bind", "nbd_socket"])]
        ublk_id: Option<i32>,
        /// When forking: upload a new "now" snapshot marker to the remote
        /// store and branch from it, instead of relying on an existing
        /// snapshot. Required when the source has no snapshot (e.g.
        /// recovering from a dead host). Conflicts with an explicit snapshot
        /// pin in `--from <vol>/<snap>`.
        #[arg(long, requires = "from")]
        force_snapshot: bool,
    },

    /// Update configuration for a running volume
    Update {
        /// Volume name
        name: String,
        /// Change the NBD server port (restarts the volume process)
        #[arg(long, conflicts_with_all = ["nbd_socket", "ublk", "ublk_id", "no_ublk"])]
        nbd_port: Option<u16>,
        /// Change the NBD bind address (restarts the volume process)
        #[arg(long, conflicts_with_all = ["ublk", "ublk_id", "no_ublk"])]
        nbd_bind: Option<String>,
        /// Set or change the Unix socket path for the NBD server. Omit the
        /// path to use the default (nbd.sock inside the volume directory).
        /// Restarts the volume process.
        #[arg(long, conflicts_with_all = ["nbd_port", "ublk", "ublk_id", "no_ublk"], num_args = 0..=1, default_missing_value = "nbd.sock")]
        nbd_socket: Option<PathBuf>,
        /// Disable NBD serving (removes the [nbd] section; restarts the volume process)
        #[arg(long, conflicts_with_all = ["ublk", "ublk_id", "no_ublk"])]
        no_nbd: bool,
        /// Switch this volume to ublk transport (writes [ublk] section;
        /// restarts the volume process). Mutually exclusive with the --nbd-* flags.
        #[arg(long, conflicts_with_all = ["nbd_port", "nbd_bind", "nbd_socket", "no_nbd"])]
        ublk: bool,
        /// Pin an explicit ublk device id. Implies --ublk. Restarts the
        /// volume process.
        #[arg(long, conflicts_with_all = ["nbd_port", "nbd_bind", "nbd_socket", "no_nbd"])]
        ublk_id: Option<i32>,
        /// Disable ublk serving (removes the [ublk] section; restarts the volume process)
        #[arg(long, conflicts_with_all = ["nbd_port", "nbd_bind", "nbd_socket", "no_nbd", "ublk", "ublk_id"])]
        no_ublk: bool,
    },

    /// Show the running status of a volume
    Status {
        /// Volume name
        name: String,
        /// Fetch `names/<name>` from the bucket and print the
        /// authoritative cross-coordinator record (vol_ulid, state,
        /// coordinator_id, hostname, claimed_at, parent,
        /// handoff_snapshot) plus this coordinator's eligibility
        /// (owned, foreign, released-claimable, …). Without this,
        /// `volume status` is local-only and never reaches S3.
        #[arg(long)]
        remote: bool,
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

    /// Remove the local instance of a volume.
    ///
    /// Removes the on-disk fork (`by_id/<ulid>/`) and its `by_name/<name>`
    /// symlink. Does not delete bucket-side records or segments — this is
    /// a local-instance verb, not a `purge`.
    ///
    /// The volume must be stopped first. Without `--force`, the local
    /// state must be fully flushed and uploaded to S3 (no segments in
    /// `pending/`, no records in `wal/`); otherwise `--force` is required
    /// to discard the unflushed local state.
    Remove {
        /// Volume name
        name: String,
        /// Skip the "all writes flushed and uploaded" check and discard
        /// any local segments still pending upload or WAL records still
        /// unflushed. Use only if you don't need the local-only state.
        #[arg(long)]
        force: bool,
    },

    /// Stop a running volume (flushes and halts the NBD server; drain/GC continue)
    Stop {
        /// Volume name
        name: String,
        /// Release ownership in the same operation. Drains the WAL,
        /// publishes a handoff snapshot, and flips the bucket-side
        /// state to `released` so any coordinator can claim the name
        /// via `volume claim`. Equivalent to running `volume release`
        /// after `volume stop`.
        #[arg(long)]
        release: bool,
    },

    /// Start a previously stopped volume locally.
    ///
    /// Pure local resume: refuses if the volume has no local state on
    /// this host, or if the bucket-side record is `Released`. Use
    /// `volume claim` to take a `Released` name first, or pass
    /// `--claim` here to compose claim + start.
    Start {
        /// Volume name
        name: String,
        /// Compose with `volume claim`: claim the bucket-side record
        /// first (in-place reclaim if the local fork matches the
        /// released ULID, otherwise pull + mint a fresh fork), then
        /// start the daemon.
        #[arg(long)]
        claim: bool,
    },

    /// Claim a `Released` volume name into local ownership without
    /// starting the daemon. Result is a `Stopped` volume bound to
    /// this coordinator; use `volume start` to bring up the daemon.
    ///
    /// In-place reclaim if the local fork's ULID matches the released
    /// ULID. Otherwise pulls only the delta since the last local
    /// ancestor, mints a fresh local fork descending from the
    /// released snapshot, and atomically rebinds `names/<name>`.
    ///
    /// Refuses if the bucket record is `Live` or `Stopped` and owned
    /// by another coordinator. To override an unreachable owner,
    /// run `volume release --force <name>` first (which declares the
    /// previous owner dead and flips the record to `Released`), then
    /// `volume claim <name>`. The two-step sequence is intentional:
    /// `release --force` is the unconditional override, and `claim`
    /// is always CAS-protected against concurrent claimants.
    Claim {
        /// Volume name
        name: String,
    },

    /// Release a volume's name back to the pool. Drains the WAL, publishes
    /// a handoff snapshot, halts the daemon, and flips the bucket-side
    /// `names/<name>` record to `released` so any coordinator (this host
    /// or another) may claim it via `volume claim`. Use this to relocate
    /// a named volume between hosts; use `volume stop` for a temporary
    /// halt that retains ownership.
    Release {
        /// Volume name
        name: String,
        /// Override foreign ownership when the previous owner is
        /// unreachable. The recovering coordinator synthesises a
        /// handoff snapshot from S3-visible segments under the dead
        /// fork's prefix, signs it with its own coordinator key, and
        /// unconditionally rewrites `names/<name>`.
        ///
        /// Skips local drain (the dead owner's WAL is unreachable).
        /// Data-loss boundary: writes the dead owner accepted but
        /// never promoted to S3.
        #[arg(long)]
        force: bool,
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

    // rustls 0.23 requires a process-level default `CryptoProvider` to be
    // installed before any TLS connection is made. Feature-flag auto-detect
    // fails in our dependency graph (rustls pulled in transitively, not as
    // a direct dep of any crate that enables the `aws_lc_rs` feature on it),
    // so install it explicitly. `install_default` returns `Err` if another
    // provider was already installed — ignore that, it's fine.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args = Args::parse();

    let socket_path = args.data_dir.join("control.sock");
    let by_id_dir = args.data_dir.join("by_id");

    match args.command {
        Command::Volume { command } => match command {
            VolumeCommand::List { ro, rw, all } => {
                let filter = if ro {
                    ListFilter::Readonly
                } else if rw {
                    ListFilter::Writable
                } else {
                    ListFilter::All
                };
                if let Err(e) = list_volumes(&args.data_dir, &socket_path, filter, all) {
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

            VolumeCommand::Verify { name } => {
                let vol_dir = resolve_volume_dir(&args.data_dir, &name);
                match verify::run(&vol_dir) {
                    Ok(counts) if counts.mismatches == 0 && counts.scan_errors == 0 => {}
                    Ok(counts) if counts.mismatches > 0 => std::process::exit(1),
                    Ok(_) => std::process::exit(2),
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(2);
                    }
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

            VolumeCommand::GenerateFilemap { name, snapshot } => {
                match coordinator_client::generate_filemap(&socket_path, &name, snapshot.as_deref())
                {
                    Ok(ulid) => println!("{name}: filemap written for snapshot {ulid}"),
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }

            VolumeCommand::Create {
                name,
                size,
                from,
                nbd_port,
                nbd_bind,
                nbd_socket,
                ublk,
                ublk_id,
                force_snapshot,
            } => {
                if let Some(from) = &from {
                    if let Err(e) = validate_volume_name(&name) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                    let flags = encode_transport_flags(
                        nbd_port, nbd_bind, nbd_socket, false, ublk, ublk_id, false,
                    );
                    if let Err(e) = create_fork(
                        &args.data_dir,
                        &name,
                        from,
                        &socket_path,
                        &by_id_dir,
                        &flags,
                        force_snapshot,
                    ) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                } else {
                    let size_str = match size.as_deref() {
                        Some(s) => s,
                        None => {
                            eprintln!("error: --size is required (e.g. --size 4G)");
                            std::process::exit(1);
                        }
                    };
                    let bytes = match parse_size(size_str) {
                        Ok(b) if b > 0 => b,
                        Ok(_) => {
                            eprintln!("error: volume size must be non-zero");
                            std::process::exit(1);
                        }
                        Err(e) => {
                            eprintln!("error: bad --size: {e}");
                            std::process::exit(1);
                        }
                    };
                    let flags = encode_transport_flags(
                        nbd_port, nbd_bind, nbd_socket, false, ublk, ublk_id, false,
                    );
                    let ulid = match coordinator_client::create_volume_remote(
                        &socket_path,
                        &name,
                        bytes,
                        &flags,
                    ) {
                        Ok(u) => u,
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    };
                    let by_name = args.data_dir.join("by_name").join(&name);
                    let by_id = args.data_dir.join("by_id").join(&ulid);
                    println!("{}", by_name.display());
                    println!("{}", by_id.display());
                }
            }

            VolumeCommand::Update {
                name,
                nbd_port,
                nbd_bind,
                nbd_socket,
                no_nbd,
                ublk,
                ublk_id,
                no_ublk,
            } => {
                let flags = encode_transport_flags(
                    nbd_port, nbd_bind, nbd_socket, no_nbd, ublk, ublk_id, no_ublk,
                );
                match coordinator_client::update_volume(&socket_path, &name, &flags) {
                    Ok(note) if note == "restarting" => {
                        println!("volume restarting with new config")
                    }
                    Ok(note) if note == "not-running" => {
                        println!("volume not running; config will take effect on next start")
                    }
                    Ok(other) => println!("ok {other}"),
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }

            VolumeCommand::Status { name, remote } => {
                if remote {
                    match coordinator_client::status_remote(&socket_path, &name) {
                        Ok(rs) => print_remote_status(&name, &rs),
                        Err(e) => {
                            eprintln!("{name}: {e}");
                            std::process::exit(1);
                        }
                    }
                } else {
                    match coordinator_client::status(&socket_path, &name) {
                        Ok(reply) => println!("{name}: {}", reply.lifecycle.wire_body()),
                        Err(e) => {
                            eprintln!("{name}: {e}");
                            std::process::exit(1);
                        }
                    }
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
                                &[],
                                false,
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

            VolumeCommand::Remove { name, force } => {
                if let Err(e) = coordinator_client::remove_volume(&socket_path, &name, force) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Stop { name, release } => {
                if let Err(e) = coordinator_client::stop_volume(&socket_path, &name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                if release {
                    match coordinator_client::release_volume(&socket_path, &name, false) {
                        Ok(reply) => {
                            println!(
                                "{name}: released at handoff snapshot {}",
                                reply.handoff_snapshot
                            );
                        }
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                } else {
                    println!("{name}: stopped");
                }
            }

            VolumeCommand::Start { name, claim } => {
                if claim {
                    // run_claim's foreign-claim path streams the prefetch
                    // already; no second await needed here.
                    if let Err(e) = run_claim(&args.data_dir, &name, &socket_path, &by_id_dir) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                } else if let Some(vol_ulid) = resolve_local_volume_ulid(&args.data_dir, &name) {
                    // Plain start. Common case: volume was claimed and
                    // started before, prefetch is long done — quick probe
                    // returns Ok instantly and we stay silent. Edge case:
                    // a previous claim's streaming was Ctrl-C'd before
                    // completion, or the coordinator restarted mid-
                    // prefetch — quick probe times out, we surface the
                    // wait so start isn't a silent multi-second hang.
                    install_prefetch_ctrlc_handler(&name, "[start]");
                    let quick = std::time::Duration::from_millis(250);
                    if coordinator_client::await_prefetch(&socket_path, &vol_ulid, quick).is_err() {
                        eprintln!("[start] waiting for ancestor prefetch...");
                        match coordinator_client::await_prefetch(
                            &socket_path,
                            &vol_ulid,
                            coordinator_client::PREFETCH_AWAIT_BUDGET,
                        ) {
                            Ok(()) => eprintln!("[start] ready"),
                            Err(e) => eprintln!(
                                "[start] prefetch did not finish in time ({e}); \
                                 coordinator continues in background"
                            ),
                        }
                    }
                }
                if let Err(e) = coordinator_client::start_volume(&socket_path, &name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                if claim {
                    println!("{name}: claimed and started");
                } else {
                    println!("{name}: started");
                }
            }

            VolumeCommand::Claim { name } => {
                if let Err(e) = run_claim(&args.data_dir, &name, &socket_path, &by_id_dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                println!("{name}: claimed");
            }

            VolumeCommand::Release { name, force } => {
                match coordinator_client::release_volume(&socket_path, &name, force) {
                    Ok(reply) => {
                        let kind = if force {
                            format!(
                                "force-released at synthesised handoff snapshot {}",
                                reply.handoff_snapshot
                            )
                        } else {
                            format!("released at handoff snapshot {}", reply.handoff_snapshot)
                        };
                        println!("{name}: {kind}");
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }
        },

        Command::ServeVolume {
            fork_dir,
            size,
            bind,
            port,
            socket,
            readonly,
            ublk,
            ublk_id,
        } => {
            // In the flat layout, fork_dir IS the volume directory.
            let size_bytes = resolve_volume_size(&fork_dir, size.as_deref())
                .expect("failed to determine volume size");
            let fetch_config =
                resolve_volume_fetch_config(&fork_dir).expect("failed to load fetch config");
            // `volume.draining` forces IPC-only mode: skip every
            // transport (ublk + NBD) regardless of CLI flags. The
            // coordinator sets this marker when transparently restarting
            // a stopped volume to drain it for `volume release`, so the
            // brief restart window can never expose the volume to a
            // client. The supervisor also drops the transport flags in
            // this case; this is the second line of defence.
            let draining = fork_dir.join("volume.draining").exists();
            if ublk && !draining {
                if readonly {
                    panic!("ublk transport does not yet support --readonly");
                }
                match ublk::run_volume_ublk(&fork_dir, size_bytes, fetch_config, ublk_id) {
                    Ok(()) => return,
                    Err(ublk::UblkRunError::Config(msg)) => {
                        eprintln!("ublk: {msg}");
                        std::process::exit(ublk::EXIT_CONFIG);
                    }
                    Err(ublk::UblkRunError::Other(e)) => {
                        eprintln!("ublk server error: {e}");
                        std::process::exit(1);
                    }
                }
            }
            let nbd_bind = if draining {
                None
            } else if let Some(path) = socket {
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

        Command::Ublk { command } => match command {
            UblkCommand::List => {
                if let Err(e) = ublk::list_devices() {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
            UblkCommand::Delete { id, all } => {
                let result = match (id, all) {
                    (Some(id), false) => ublk::delete_device(id),
                    (None, true) => ublk::delete_all_devices(),
                    _ => {
                        eprintln!("error: specify a device id, or use --all (but not both)");
                        std::process::exit(1);
                    }
                };
                if let Err(e) = result {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        },
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

use elide_coordinator::volume_state::{IMPORT_LOCK_FILE, VolumeLifecycle, VolumeMode};

/// Cell value for the STATE column in `elide volume list`. Wraps the
/// coordinator-derived `VolumeLifecycle` with two CLI-only sentinels:
///
/// - `Ancestor` — pulled ancestor volume that has no `by_name/` entry
///   and is never supervised. Lifecycle classification doesn't apply.
/// - `CoordinatorDown` — coordinator IPC is unreachable, so on-disk
///   markers may not reflect what the supervisor would do. Render
///   `-` rather than guessing.
enum CliVolumeState {
    Lifecycle(VolumeLifecycle),
    Ancestor,
    CoordinatorDown,
}

impl CliVolumeState {
    fn label(&self) -> &'static str {
        match self {
            Self::Lifecycle(l) => l.label(),
            Self::Ancestor => "ancestor",
            Self::CoordinatorDown => "-",
        }
    }
}

struct VolumeRow {
    name: String,
    ulid: String,
    mode: VolumeMode,
    state: CliVolumeState,
    transport: String,
    pid: String,
}

fn list_volumes(
    data_dir: &Path,
    socket_path: &Path,
    filter: ListFilter,
    include_ancestors: bool,
) -> std::io::Result<()> {
    let coordinator_up = coordinator_client::is_reachable(socket_path);
    let by_name_dir = data_dir.join("by_name");
    let by_id_dir = data_dir.join("by_id");
    let mut rows: Vec<VolumeRow> = Vec::new();
    let mut seen: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
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
                if let Ok(canonical) = std::fs::canonicalize(&vol_dir) {
                    seen.insert(canonical);
                }
                let is_readonly = vol_dir.join("volume.readonly").exists();
                let include = match filter {
                    ListFilter::All => true,
                    ListFilter::Readonly => is_readonly,
                    ListFilter::Writable => !is_readonly,
                };
                if !include {
                    continue;
                }
                rows.push(volume_row(name, &vol_dir, is_readonly, coordinator_up));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    if include_ancestors && !matches!(filter, ListFilter::Writable) {
        match std::fs::read_dir(&by_id_dir) {
            Ok(dir_entries) => {
                for entry in dir_entries {
                    let entry = entry?;
                    let vol_dir = entry.path();
                    if !vol_dir.is_dir() {
                        continue;
                    }
                    let Some(ulid_str) = vol_dir.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    if ulid::Ulid::from_string(ulid_str).is_err() {
                        continue;
                    }
                    let canonical =
                        std::fs::canonicalize(&vol_dir).unwrap_or_else(|_| vol_dir.clone());
                    if seen.contains(&canonical) {
                        continue;
                    }
                    if !vol_dir.join("volume.readonly").exists() {
                        continue;
                    }
                    rows.push(volume_row("-".to_owned(), &vol_dir, true, coordinator_up));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    // Named volumes first (alphabetical), pulled ancestors at the bottom (by ULID).
    rows.sort_by(|a, b| match (a.name.as_str(), b.name.as_str()) {
        ("-", "-") => a.ulid.cmp(&b.ulid),
        ("-", _) => std::cmp::Ordering::Greater,
        (_, "-") => std::cmp::Ordering::Less,
        _ => a.name.cmp(&b.name),
    });
    if rows.is_empty() {
        println!("no volumes found in {}", data_dir.display());
        if !coordinator_up {
            println!("coordinator is not running ({})", socket_path.display());
        }
        return Ok(());
    }
    let name_w = rows.iter().map(|r| r.name.len()).max().unwrap_or(4).max(4);
    let ulid_w = rows.iter().map(|r| r.ulid.len()).max().unwrap_or(4).max(4);
    let mode_w = 4;
    let state_w = rows
        .iter()
        .map(|r| r.state.label().len())
        .max()
        .unwrap_or(5)
        .max(5);
    let transport_w = rows
        .iter()
        .map(|r| r.transport.len())
        .max()
        .unwrap_or(9)
        .max(9);
    println!(
        "{:<name_w$}  {:<ulid_w$}  {:<mode_w$}  {:<state_w$}  {:<transport_w$}  PID",
        "NAME", "ULID", "MODE", "STATE", "TRANSPORT"
    );
    for r in &rows {
        println!(
            "{:<name_w$}  {:<ulid_w$}  {:<mode_w$}  {:<state_w$}  {:<transport_w$}  {}",
            r.name,
            r.ulid,
            r.mode.label(),
            r.state.label(),
            r.transport,
            r.pid
        );
    }
    if !coordinator_up {
        println!();
        println!("coordinator is not running ({})", socket_path.display());
    }
    Ok(())
}

/// Gather per-volume display state: lifecycle, transport summary, and pid.
///
/// State labels mirror the coordinator's IPC `volume_status`
/// (`elide-coordinator/src/inbound.rs`) when the coordinator is reachable:
///
/// - `running`          — `volume.pid` present and the process is alive.
/// - `importing`        — an import lock is held.
/// - `stopped (manual)` — `volume.stopped` is set; the coordinator will not
///   auto-start this volume.
/// - `stopped`          — neither a live pid nor a manual-stop marker.
///
/// When `coordinator_up` is false, lifecycle inference from on-disk markers is
/// suppressed and STATE/PID render as `-`. The volume's behaviour without a
/// running coordinator is "paused pending coordinator restart" regardless of
/// which marker happens to be on disk, so collapsing to `-` plus a footer line
/// (printed by the caller) keeps the table honest. Transport stays populated
/// — it reflects static config, not lifecycle.
fn volume_row(name: String, vol_dir: &Path, is_readonly: bool, coordinator_up: bool) -> VolumeRow {
    let transport = transport_summary(vol_dir);
    let ulid = vol_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|s| ulid::Ulid::from_string(s).ok())
        .map(|u| u.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let mode = if is_readonly {
        VolumeMode::Ro
    } else {
        VolumeMode::Rw
    };
    // Pulled ancestors (no by_name/ symlink, no volume.name) are never
    // supervised — render a static "ancestor" state instead of inferring
    // lifecycle from markers that don't apply.
    let state = if name == "-" && is_readonly {
        CliVolumeState::Ancestor
    } else if !coordinator_up {
        CliVolumeState::CoordinatorDown
    } else {
        CliVolumeState::Lifecycle(VolumeLifecycle::from_dir(vol_dir))
    };
    let pid = match &state {
        CliVolumeState::Lifecycle(l) => l
            .pid()
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_owned()),
        _ => "-".to_owned(),
    };
    VolumeRow {
        name,
        ulid,
        mode,
        state,
        transport,
        pid,
    }
}

/// Summarise the configured block-device transport for display: `nbd <endpoint>`,
/// `ublk <device>`, or `-` if neither is configured. For ublk, prefers the
/// kernel-assigned id recorded in `ublk.id` (set on a successful ADD); falls
/// back to the configured `dev_id` or `auto` for not-yet-bound volumes.
fn transport_summary(vol_dir: &Path) -> String {
    let cfg = match elide_core::config::VolumeConfig::read(vol_dir) {
        Ok(c) => c,
        Err(_) => return "-".to_owned(),
    };
    if let Some(nbd) = cfg.nbd.as_ref() {
        return match nbd.endpoint(vol_dir) {
            Some(ep) => format!("nbd {ep}"),
            None => "nbd".to_owned(),
        };
    }
    if cfg.ublk.is_some() {
        let runtime_id = std::fs::read_to_string(vol_dir.join("ublk.id"))
            .ok()
            .and_then(|s| s.trim().parse::<i32>().ok());
        let id = runtime_id.or(cfg.ublk.as_ref().and_then(|u| u.dev_id));
        return match id {
            Some(id) => format!("ublk /dev/ublkb{id}"),
            None => "ublk auto".to_owned(),
        };
    }
    "-".to_owned()
}

/// Encode the CLI's typed transport flags as the space-separated tokens
/// understood by the coordinator's `create` / `update` IPC verbs. Order
/// follows the IPC parser; absent options emit nothing.
fn encode_transport_flags(
    nbd_port: Option<u16>,
    nbd_bind: Option<String>,
    nbd_socket: Option<PathBuf>,
    no_nbd: bool,
    ublk: bool,
    ublk_id: Option<i32>,
    no_ublk: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(p) = nbd_port {
        out.push(format!("nbd-port={p}"));
    }
    if let Some(b) = nbd_bind {
        out.push(format!("nbd-bind={b}"));
    }
    if let Some(s) = nbd_socket {
        out.push(format!("nbd-socket={}", s.display()));
    }
    if no_nbd {
        out.push("no-nbd".to_owned());
    }
    if ublk {
        out.push("ublk".to_owned());
    }
    if let Some(id) = ublk_id {
        out.push(format!("ublk-id={id}"));
    }
    if no_ublk {
        out.push("no-ublk".to_owned());
    }
    out
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
///
/// `force_snapshot` (readonly sources only): upload a new "now" snapshot
/// marker to the remote store and branch from it. Needed when the source
/// has no existing snapshot. Conflicts with an explicit pin.
#[allow(clippy::too_many_arguments)]
fn create_fork(
    data_dir: &Path,
    fork_name: &str,
    from: &str,
    socket_path: &Path,
    by_id_dir: &Path,
    flags: &[String],
    force_snapshot: bool,
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

    if force_snapshot && matches!(spec, FromSpec::ExplicitPin(..)) {
        return Err(std::io::Error::other(
            "--force-snapshot conflicts with an explicit snapshot pin in --from <vol>/<snap>",
        ));
    }

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
                let config = load_fetch_config(socket_path, data_dir, &ulid_str)?;
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
                let config = load_fetch_config_for_name(socket_path, data_dir, from)?;
                eprintln!("pulling '{from}' from remote store...");
                remote_pull(&config, from, data_dir, socket_path)?;
                // remote_pull resolved the name to a ULID and pulled into
                // by_id/<ulid>/. Find it by re-resolving the pull spec.
                let store = elide::build_object_store(&config)
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

    if source_fork_dir.join(IMPORT_LOCK_FILE).exists() {
        return Err(std::io::Error::other(format!(
            "volume '{from}' is still importing; wait for import to complete before forking"
        )));
    }

    // Resolve source ULID for the fork-create IPC. For ULID forms it's
    // already in `spec`; for Name it comes from the canonicalized symlink
    // target (or the auto-pull's ULID if we just pulled it).
    let source_ulid_str: String = match &spec {
        FromSpec::ExplicitPin(vol, _) | FromSpec::BareUlid(vol) => vol.to_string(),
        FromSpec::Name => {
            if let Some(p) = &pulled_vol_ulid {
                p.clone()
            } else {
                std::fs::canonicalize(&source_fork_dir)?
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| std::io::Error::other("source dir has no ULID file name"))?
                    .to_owned()
            }
        }
    };

    // Determine the snapshot ULID + (for force-snapshot) the ephemeral
    // pubkey we'll record in the fork's provenance.
    let mut parent_key_hex: Option<String> = None;
    let snap_ulid: Option<ulid::Ulid> = match &spec {
        FromSpec::ExplicitPin(_, snap) => Some(*snap),
        _ if source_fork_dir.join("volume.readonly").exists() => {
            if force_snapshot {
                // Coordinator synthesizes the "now" snapshot (uploads marker,
                // verifies pinned segments, writes signed manifest under an
                // ephemeral key). Returns the snap ULID + ephemeral pubkey hex.
                let (snap_str, pubkey_hex) =
                    coordinator_client::force_snapshot_now(socket_path, &source_ulid_str)?;
                eprintln!("attested now-snapshot {snap_str} for {source_ulid_str}");
                parent_key_hex = Some(pubkey_hex);
                Some(ulid::Ulid::from_string(&snap_str).map_err(|e| {
                    std::io::Error::other(format!("coordinator returned invalid snap ULID: {e}"))
                })?)
            } else if let Some(snap) = volume::latest_snapshot(&source_fork_dir)? {
                Some(snap)
            } else {
                let config = load_fetch_config(socket_path, data_dir, &source_ulid_str)?;
                match resolve_latest_remote_snapshot(&config, &source_ulid_str) {
                    Ok(snap) => Some(snap),
                    Err(_) => {
                        return Err(std::io::Error::other(format!(
                            "source volume {source_ulid_str} has no snapshots; pass \
                             --force-snapshot to upload a new 'now' marker"
                        )));
                    }
                }
            }
        }
        FromSpec::BareUlid(_) => {
            // Writable volume addressed by ULID: take an implicit snapshot.
            let name = elide_core::config::VolumeConfig::read(&source_fork_dir)?
                .name
                .ok_or_else(|| std::io::Error::other("source volume has no name in volume.toml"))?;
            snapshot_volume(data_dir, &name)?;
            None
        }
        FromSpec::Name => {
            snapshot_volume(data_dir, from)?;
            None
        }
    };

    let snap_str = snap_ulid.map(|s| s.to_string());
    let new_vol_ulid = coordinator_client::fork_create(
        socket_path,
        fork_name,
        &source_ulid_str,
        snap_str.as_deref(),
        parent_key_hex.as_deref(),
        flags,
        false, // brand-new name → mark_initial claims it
    )?;
    let new_fork_dir = by_id_dir.join(&new_vol_ulid);
    println!("{}", new_fork_dir.display());
    Ok(())
}

/// Classification of a `by_name/<name>` symlink for the
/// claim-from-released path. Reaching this code requires the operator
/// to have passed `--remote` (the IPC refuses bare `start` against a
/// `Released` record), so any pre-existing symlink is treated as a
/// stale local pin to be replaced — only the previous target is
/// reported for the operator log.
#[derive(Debug, PartialEq, Eq)]
enum SymlinkState {
    /// No `by_name/<name>` entry exists.
    Absent,
    /// Symlink exists and will be removed before fork-create writes a
    /// fresh one. `previous_target` is the target ULID parsed from the
    /// symlink (the orphaned local fork) when readable, used for the
    /// operator log message; `None` if the symlink target couldn't be
    /// parsed (broken read_link, malformed path).
    Stale { previous_target: Option<String> },
}

/// Inspect `by_name/<name>` and decide how to handle it before
/// claiming a released name. The symlink target is read without
/// following it; the trailing path component is the previous fork's
/// ULID (target shape: `../by_id/<ulid>` relative or
/// `<data_dir>/by_id/<ulid>` absolute).
fn classify_symlink_for_claim(symlink: &Path) -> SymlinkState {
    match std::fs::read_link(symlink) {
        Ok(target) => SymlinkState::Stale {
            previous_target: target
                .file_name()
                .and_then(|n| n.to_str())
                .map(str::to_owned),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => SymlinkState::Absent,
        Err(_) => SymlinkState::Stale {
            previous_target: None,
        },
    }
}

/// Decide whether the local fork at `by_id/<prev_ulid>/` is exactly
/// what a fresh `fork_create` would mint for this claim — i.e., it has
/// a verifiable signed provenance recording `expected_parent_ulid` as
/// the parent volume and `expected_snap` as the branch snapshot.
///
/// Returns `true` only when:
///   - `by_id/<prev_ulid>/` exists,
///   - `volume.provenance` is present and verifies under
///     `volume.pub` in the same directory,
///   - the lineage records a parent matching `expected_parent_ulid`
///     and `expected_snap`.
///
/// On any failure (missing dir, missing files, signature mismatch,
/// parent mismatch, no parent at all) returns `false` — the caller
/// then falls back to the regular "remove stale symlink, fork-create
/// fresh" path. Provenance verification means we never reuse a fork
/// whose signed lineage we can't trust.
fn orphan_matches_intended_fork(
    by_id_dir: &Path,
    prev_ulid: &str,
    expected_parent_ulid: &str,
    expected_snap: &str,
) -> bool {
    let prev_dir = by_id_dir.join(prev_ulid);
    if !prev_dir.is_dir() {
        return false;
    }
    let lineage = match elide_core::signing::read_lineage_verifying_signature(
        &prev_dir,
        elide_core::signing::VOLUME_PUB_FILE,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
    ) {
        Ok(l) => l,
        Err(_) => return false,
    };
    match lineage.parent {
        Some(p) => p.volume_ulid == expected_parent_ulid && p.snapshot_ulid == expected_snap,
        None => false,
    }
}

/// CLI orchestration of `volume claim`.
///
/// Calls the coordinator's `claim` IPC and dispatches:
///   - `Reclaimed`: nothing else to do; the bucket flipped in place.
///   - `NeedsClaim`: pull source if needed, mint a fresh local fork
///     via `fork-create`, then `rebind-name` to atomically rebind the
///     bucket record to the new fork (in `Stopped` state). The
///     conditional PUT inside `rebind-name` resolves races; the local
///     fork is left in place as a usable orphan if another
///     coordinator wins.
fn run_claim(
    data_dir: &Path,
    name: &str,
    socket_path: &Path,
    by_id_dir: &Path,
) -> std::io::Result<()> {
    use coordinator_client::ClaimOutcome;
    match coordinator_client::claim_volume_bucket(socket_path, name)? {
        ClaimOutcome::Reclaimed => Ok(()),
        ClaimOutcome::NeedsClaim {
            released_vol_ulid,
            handoff_snapshot,
        } => orchestrate_foreign_claim(
            data_dir,
            name,
            &released_vol_ulid,
            handoff_snapshot.as_deref(),
            socket_path,
            by_id_dir,
        ),
    }
}

fn orchestrate_foreign_claim(
    data_dir: &Path,
    name: &str,
    released_vol_ulid: &str,
    handoff_snapshot: Option<&str>,
    socket_path: &Path,
    by_id_dir: &Path,
) -> std::io::Result<()> {
    let snap = handoff_snapshot.ok_or_else(|| {
        std::io::Error::other(format!(
            "name '{name}' is Released but has no handoff snapshot recorded — \
             the releasing coordinator did not publish one (older release?). \
             Manual recovery required: see docs/operations.md"
        ))
    })?;
    let from = format!("{released_vol_ulid}/{snap}");

    // Stale-symlink recovery. Two cases for a pre-existing
    // `by_name/<name>` entry:
    //
    //   1. **Resumable orphan** — Ctrl-C'd a prior `volume claim` run
    //      after `fork_create` succeeded but before `rebind_name`. The
    //      orphan fork at `by_id/<prev>/` is exactly what a fresh
    //      `fork_create` would mint: its signed provenance records the
    //      same parent ULID and snapshot ULID we're about to fork from.
    //      Reuse it: skip `fork_create`, call `rebind_name` directly on
    //      the existing ULID. This makes claim retry idempotent — no
    //      orphan accumulation, no wasted ULID minting, no wasted I/O.
    //
    //   2. **True stale entry** — symlink left over from an unrelated
    //      operation (the orphan's provenance doesn't match this claim,
    //      or the target dir is missing/corrupted). Remove the symlink
    //      and proceed with a fresh `fork_create`. The previous
    //      `by_id/<prev>/` is left on disk — if it's an ancestor of
    //      the claimed snapshot it serves as a transparent ancestor
    //      cache, otherwise it's a true orphan eligible for cleanup.
    let symlink = data_dir.join("by_name").join(name);
    let resume_existing_ulid: Option<String> = match classify_symlink_for_claim(&symlink) {
        SymlinkState::Absent => None,
        SymlinkState::Stale { previous_target } => match previous_target {
            Some(prev_ulid)
                if orphan_matches_intended_fork(by_id_dir, &prev_ulid, released_vol_ulid, snap) =>
            {
                eprintln!("[claim] resuming aborted prior attempt: reusing local fork {prev_ulid}");
                Some(prev_ulid)
            }
            other => {
                std::fs::remove_file(&symlink).map_err(|e| {
                    std::io::Error::other(format!("removing stale by_name/{name}: {e}"))
                })?;
                match other {
                    Some(prev) => eprintln!(
                        "[claim] removed stale by_name/{name} (was {prev}; \
                         by_id/{prev}/ retained on disk)"
                    ),
                    None => eprintln!("[claim] removed stale by_name/{name}"),
                }
                None
            }
        },
    };

    let source_dir = volume::resolve_ancestor_dir(by_id_dir, released_vol_ulid);
    if !source_dir.exists() {
        let config = load_fetch_config(socket_path, data_dir, released_vol_ulid)?;
        eprintln!("pulling {released_vol_ulid} from remote store...");
        remote_pull(&config, &from, data_dir, socket_path)?;
        if !volume::resolve_ancestor_dir(by_id_dir, released_vol_ulid).exists() {
            return Err(std::io::Error::other(format!(
                "source volume {released_vol_ulid} not found in remote store"
            )));
        }
    }

    let new_vol_ulid = match resume_existing_ulid {
        Some(ulid) => ulid,
        None => {
            // Fresh fork path: resolve the handoff key (only used as
            // fork_create input, so skipped on resume) and mint a new
            // local fork. The signed provenance written by
            // `fork_volume_at` will record this ULID so a subsequent
            // resume can recognise this attempt as a match.
            let parent_key_hex = match coordinator_client::resolve_handoff_key(
                socket_path,
                released_vol_ulid,
                snap,
            )? {
                coordinator_client::HandoffKey::Normal => None,
                coordinator_client::HandoffKey::Recovery {
                    manifest_pubkey_hex,
                } => {
                    eprintln!(
                        "[claim] handoff snapshot {snap} is synthesised — verifying under recovering coordinator's key"
                    );
                    Some(manifest_pubkey_hex)
                }
            };
            coordinator_client::fork_create(
                socket_path,
                name,
                released_vol_ulid,
                Some(snap),
                parent_key_hex.as_deref(),
                &[],
                true,
            )?
        }
    };

    coordinator_client::rebind_name(socket_path, name, &new_vol_ulid)?;

    // Surface the coordinator's background prefetch to the user. The
    // bucket-side claim and local fork are already durable above; the
    // prefetch task on the coordinator pulls the ancestor `.idx` files
    // (and runs boot-prewarm) so a subsequent `volume start` is offline-
    // ready. The CLI is just a subscriber to the existing
    // `await-prefetch` IPC; Ctrl-C kills only the subscriber, never the
    // coordinator's prefetch work.
    install_prefetch_ctrlc_handler(name, "[claim]");
    eprintln!("[claim] prefetching ancestor index...");
    match coordinator_client::await_prefetch(
        socket_path,
        &new_vol_ulid,
        coordinator_client::PREFETCH_AWAIT_BUDGET,
    ) {
        Ok(()) => eprintln!("[claim] ready"),
        Err(e) => {
            // Don't fail the claim — the bucket-side claim succeeded and
            // the local fork is committed. The error here is most
            // commonly the 60s subscriber-side budget expiring while the
            // coordinator's prefetch task is still running (deep ancestor
            // chain, slow network); it can also be a real prefetch
            // failure. In both cases the coordinator continues on its
            // own and `volume start` will await prefetch again before
            // opening the volume.
            eprintln!(
                "[claim] prefetch did not finish in time ({e}); \
                 coordinator continues in background"
            );
        }
    }
    Ok(())
}

/// Install a process-wide Ctrl-C handler for prefetch waits.
///
/// The handler prints a "continuing in background" message tagged with
/// `label` (e.g. `"[claim]"`, `"[start]"`) and exits 130 (sigint
/// convention). The coordinator's prefetch task is server-side and
/// runs to completion regardless; this handler only signals that the
/// CLI subscriber has gone away.
fn install_prefetch_ctrlc_handler(name: &str, label: &'static str) {
    let name_for_ctrlc = name.to_owned();
    ctrlc::set_handler(move || {
        eprintln!(
            "\n{label} prefetch continuing in background for {name_for_ctrlc}; \
             coordinator will report completion in its log"
        );
        std::process::exit(130);
    })
    .ok();
}

/// Resolve a local volume name to its ULID by reading the
/// `by_name/<name>` symlink. Returns `None` if the symlink is absent,
/// is broken, or points to a non-ULID directory.
fn resolve_local_volume_ulid(data_dir: &Path, name: &str) -> Option<String> {
    let symlink = data_dir.join("by_name").join(name);
    let target = std::fs::read_link(&symlink).ok()?;
    let last = target.file_name().and_then(|n| n.to_str())?;
    // Parse-don't-validate: round-trip through the ULID type so the
    // returned string is canonical.
    Some(ulid::Ulid::from_string(last).ok()?.to_string())
}

/// Ask a running coordinator for its store config and (for S3) credentials.
///
/// Returns `Ok(Some(_))` when the coordinator is reachable and provided a
/// usable config. Returns `Ok(None)` when no coordinator is running at
/// `socket_path` — callers should fall back to `FetchConfig::load` in that
/// case. Returns an error when the coordinator IS running but reported an
/// error (e.g. `get-store-creds` failed because the coordinator's env has
/// no AWS credentials) — that's an operator-visible misconfiguration and
/// falling back would silently paper over it.
///
/// On success for an S3 store, exports `AWS_ACCESS_KEY_ID`,
/// `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` into this
/// process's env so `rust-s3`'s default credential chain picks them up when
/// building the fetcher.
fn fetch_config_via_coordinator(
    socket_path: &Path,
) -> std::io::Result<Option<elide_fetch::FetchConfig>> {
    if !socket_path.exists() {
        return Ok(None);
    }
    let config = match coordinator_client::get_store_config(socket_path) {
        Ok(c) => c,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    if let Some(path) = config.local_path {
        return Ok(Some(elide_fetch::FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(path),
            fetch_batch_bytes: None,
        }));
    }
    let Some(bucket) = config.bucket else {
        return Err(std::io::Error::other(
            "coordinator returned empty store config",
        ));
    };
    let creds = coordinator_client::get_store_creds(socket_path)?;
    // SAFETY: CLI startup is single-threaded at this point — no background
    // tasks have been spawned, so there are no concurrent env readers.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", &creds.access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &creds.secret_access_key);
        if let Some(tok) = &creds.session_token {
            std::env::set_var("AWS_SESSION_TOKEN", tok);
        } else {
            std::env::remove_var("AWS_SESSION_TOKEN");
        }
    }
    Ok(Some(elide_fetch::FetchConfig {
        bucket: Some(bucket),
        endpoint: config.endpoint,
        region: config.region,
        local_path: None,
        fetch_batch_bytes: None,
    }))
}

/// Resolve the fetch config: try the running coordinator first, then fall
/// back to `FetchConfig::load` (which reads `fetch.toml`, `ELIDE_S3_BUCKET`,
/// or `./elide_store`). Returns `Ok(None)` if no source is available.
fn resolve_fetch_config(
    socket_path: &Path,
    data_dir: &Path,
) -> std::io::Result<Option<elide_fetch::FetchConfig>> {
    if let Some(cfg) = fetch_config_via_coordinator(socket_path)? {
        return Ok(Some(cfg));
    }
    elide_fetch::FetchConfig::load(data_dir)
}

/// Resolve the fetch config for a volume subprocess (`serve-volume`).
///
/// The coordinator exports `ELIDE_COORDINATOR_SOCKET` into each spawned
/// volume's environment. When set, we pull store config from the
/// coordinator over IPC and authenticate to its credential vending via
/// the macaroon handshake (`register` then `credentials`). The volume
/// ULID is the fork directory's basename. When the env var is unset
/// (standalone `elide serve-volume` invocation with no coordinator),
/// fall back to `FetchConfig::load` from the volume directory.
fn resolve_volume_fetch_config(
    fork_dir: &Path,
) -> std::io::Result<Option<elide_fetch::FetchConfig>> {
    if let Ok(sock) = std::env::var("ELIDE_COORDINATOR_SOCKET") {
        let socket_path = Path::new(&sock);
        let volume_ulid = elide_fetch::derive_volume_id(fork_dir)?;
        if let Some(cfg) = fetch_config_via_coordinator_macaroon(socket_path, &volume_ulid)? {
            return Ok(Some(cfg));
        }
    }
    elide_fetch::FetchConfig::load(fork_dir)
}

/// Volume-side counterpart to `fetch_config_via_coordinator` that uses the
/// PID-bound macaroon handshake instead of the unauthenticated
/// `get-store-creds`. The CLI keeps using `get-store-creds` because it
/// is not a spawned volume process and has no entry in `volume.pid`.
fn fetch_config_via_coordinator_macaroon(
    socket_path: &Path,
    volume_ulid: &str,
) -> std::io::Result<Option<elide_fetch::FetchConfig>> {
    if !socket_path.exists() {
        return Ok(None);
    }
    let config = match coordinator_client::get_store_config(socket_path) {
        Ok(c) => c,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    if let Some(path) = config.local_path {
        return Ok(Some(elide_fetch::FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(path),
            fetch_batch_bytes: None,
        }));
    }
    let Some(bucket) = config.bucket else {
        return Err(std::io::Error::other(
            "coordinator returned empty store config",
        ));
    };
    let creds = coordinator_client::register_and_get_creds(socket_path, volume_ulid)?;
    // SAFETY: serve-volume startup is single-threaded at this point — no
    // background tasks have been spawned, so there are no concurrent env
    // readers.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", &creds.access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &creds.secret_access_key);
        if let Some(tok) = &creds.session_token {
            std::env::set_var("AWS_SESSION_TOKEN", tok);
        } else {
            std::env::remove_var("AWS_SESSION_TOKEN");
        }
    }
    Ok(Some(elide_fetch::FetchConfig {
        bucket: Some(bucket),
        endpoint: config.endpoint,
        region: config.region,
        local_path: None,
        fetch_batch_bytes: None,
    }))
}

/// Load the fetch config, or return a clear error mentioning the volume ULID.
fn load_fetch_config(
    socket_path: &Path,
    data_dir: &Path,
    ulid_str: &str,
) -> std::io::Result<elide_fetch::FetchConfig> {
    match resolve_fetch_config(socket_path, data_dir) {
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
    socket_path: &Path,
    data_dir: &Path,
    name: &str,
) -> std::io::Result<elide_fetch::FetchConfig> {
    match resolve_fetch_config(socket_path, data_dir) {
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

    let store = elide::build_object_store(config)
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

/// Pretty-print a `StatusRemoteReply` for `elide volume status --remote`.
fn print_remote_status(name: &str, rs: &coordinator_client::StatusRemoteReply) {
    println!("{name}");
    println!("  state           {}", rs.state);
    println!("  vol_ulid        {}", rs.vol_ulid);
    if let Some(id) = &rs.coordinator_id {
        println!("  coordinator_id  {id}");
    }
    if let Some(host) = &rs.hostname {
        println!("  hostname        {host}");
    }
    if let Some(when) = &rs.claimed_at {
        println!("  claimed_at      {when}");
    }
    if let Some(parent) = &rs.parent {
        println!("  parent          {parent}");
    }
    if let Some(snap) = &rs.handoff_snapshot {
        println!("  handoff_snap    {snap}");
    }
    println!("  eligibility     {}", rs.eligibility.wire_str());
}

/// Pull a volume (and its full ancestor chain) from the remote store as
/// readonly fork sources.
///
/// `spec` is one of:
///   - a volume name (resolved via `names/<name>` → ULID in the store)
///   - `<vol_ulid>` (address directly by ULID)
///   - `<vol_ulid>/<snap_ulid>` (address a specific snapshot; the ULID part
///     is what gets pulled — the snapshot ULID is retained for the caller
///     that wants to pin provenance, e.g. `volume create --from`)
///
/// Each pulled volume lands under `data_dir/by_id/<vol_ulid>/` with
/// `volume.readonly`, `volume.pub`, `volume.provenance`, an empty `index/`
/// dir, and a `volume.toml` with no name (the absent name and absent
/// `by_name/` symlink mark this as a pulled ancestor). The coordinator's
/// next rescan then runs `prefetch_indexes` which downloads the signed
/// `.idx` files into each volume's own `index/` directory (signature
/// verification uses that volume's own `volume.pub`).
///
/// The ancestor chain is walked via the downloaded `volume.provenance`:
/// for every parent ULID not already present in `by_id/`, its ancestor
/// entry is pulled too. Encountering a mid-chain ancestor that is already
/// local terminates the walk — the local copy is authoritative.
fn remote_pull(
    config: &elide_fetch::FetchConfig,
    spec: &str,
    data_dir: &Path,
    socket_path: &Path,
) -> std::io::Result<()> {
    let store = elide::build_object_store(config)
        .map_err(|e| std::io::Error::other(format!("store: {e}")))?;
    let rt = tokio::runtime::Runtime::new()?;

    // Step 1: parse `spec` and resolve to a root ULID to pull.
    let root_ulid = resolve_pull_spec(&rt, &*store, spec)?;

    // Step 2: walk the ancestor chain, pulling each ancestor that isn't
    // already local. Start from the requested volume; after each pull, parse
    // its downloaded provenance to find the next parent. The actual write
    // is delegated to the coordinator's `pull-readonly` IPC so the
    // root-owned by_id/ tree is written by the coordinator process, not
    // the (possibly non-root) CLI.
    let mut pulled: Vec<String> = Vec::new();
    let mut next: Option<String> = Some(root_ulid);
    while let Some(ulid_str) = next.take() {
        if ancestor_exists_locally(data_dir, &ulid_str) {
            // Local copy is authoritative; stop walking up from here.
            break;
        }
        let parent_ulid = coordinator_client::pull_readonly(socket_path, &ulid_str)?;
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
    // new ancestor entries and kicks off prefetch_indexes on each.
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
/// concern for the caller (`volume create --from`), not for pull itself.
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
    let body = std::str::from_utf8(&raw)
        .map_err(|e| std::io::Error::other(format!("names/{spec}: {e}")))?;
    let record = elide_core::name_record::NameRecord::from_toml(body)
        .map_err(|e| std::io::Error::other(format!("parsing names/{spec}: {e}")))?;
    Ok(record.vol_ulid.to_string())
}

/// Return `true` if a local copy of `<ulid>` exists.
fn ancestor_exists_locally(data_dir: &Path, ulid_str: &str) -> bool {
    data_dir.join("by_id").join(ulid_str).exists()
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

#[cfg(test)]
mod claim_symlink_tests {
    use super::*;
    use std::os::unix::fs::symlink;
    use tempfile::TempDir;

    const RELEASED: &str = "01J0000000000000000000000V";
    const OTHER: &str = "01J9999999999999999999999V";

    fn setup() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let by_name = tmp.path().join("by_name");
        std::fs::create_dir_all(&by_id).unwrap();
        std::fs::create_dir_all(&by_name).unwrap();
        (tmp, by_id, by_name)
    }

    #[test]
    fn absent_when_symlink_missing() {
        let (_t, _id, by_name) = setup();
        assert_eq!(
            classify_symlink_for_claim(&by_name.join("vol")),
            SymlinkState::Absent,
        );
    }

    #[test]
    fn stale_when_target_matches_released() {
        // In-place reclaim: previous symlink targets the same ULID
        // we're about to claim. Reaching this code via the cross-
        // coord claim path is unreachable in practice (the IPC's
        // mark_reclaimed_local short-circuits and returns
        // `ok reclaimed`), but the classifier still reports it as
        // Stale with the previous_target carrying the ULID.
        let (_t, by_id, by_name) = setup();
        std::fs::create_dir_all(by_id.join(RELEASED)).unwrap();
        symlink(format!("../by_id/{RELEASED}"), by_name.join("vol")).unwrap();
        assert_eq!(
            classify_symlink_for_claim(&by_name.join("vol")),
            SymlinkState::Stale {
                previous_target: Some(RELEASED.to_owned()),
            },
        );
    }

    #[test]
    fn stale_when_target_missing() {
        let (_t, _id, by_name) = setup();
        // Symlink dangling: target by_id directory does not exist.
        symlink(format!("../by_id/{RELEASED}"), by_name.join("vol")).unwrap();
        assert_eq!(
            classify_symlink_for_claim(&by_name.join("vol")),
            SymlinkState::Stale {
                previous_target: Some(RELEASED.to_owned()),
            },
        );
    }

    #[test]
    fn stale_when_target_is_different_existing_ulid() {
        // Cross-coordinator claim: another coordinator has claimed,
        // written, and released the name; A's local symlink still
        // points at A's old fork. Operator passed --remote so we
        // remove the symlink and report the previous target — the
        // orphaned by_id/<prev>/ stays on disk and serves as
        // ancestor cache if it's part of the claimed chain.
        let (_t, by_id, by_name) = setup();
        std::fs::create_dir_all(by_id.join(OTHER)).unwrap();
        symlink(format!("../by_id/{OTHER}"), by_name.join("vol")).unwrap();
        assert_eq!(
            classify_symlink_for_claim(&by_name.join("vol")),
            SymlinkState::Stale {
                previous_target: Some(OTHER.to_owned()),
            },
        );
    }

    /// Helper: write a fork directory with a signed provenance
    /// recording `parent_ulid` and `snap_ulid` as the parent ref. This
    /// mirrors the on-disk shape `fork_volume_at` produces, without
    /// requiring a full source volume with snapshots.
    fn write_orphan_fork(dir: &std::path::Path, parent_ulid: &str, snap_ulid: &str) {
        std::fs::create_dir_all(dir).unwrap();
        let key = elide_core::signing::generate_keypair(
            dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        // Stable dummy parent pubkey — the resume check verifies the
        // *child's* signature with the child's volume.pub and matches
        // only on the recorded parent ULID + snapshot ULID; the
        // parent_pubkey field is not examined by `orphan_matches_intended_fork`.
        let parent_pubkey = [0u8; 32];
        let lineage = elide_core::signing::ProvenanceLineage {
            parent: Some(elide_core::signing::ParentRef {
                volume_ulid: parent_ulid.to_owned(),
                snapshot_ulid: snap_ulid.to_owned(),
                pubkey: parent_pubkey,
                manifest_pubkey: None,
            }),
            extent_index: Vec::new(),
        };
        elide_core::signing::write_provenance(
            dir,
            &key,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
            &lineage,
        )
        .unwrap();
    }

    const ORPHAN: &str = "01J5555555555555555555555V";
    const SNAP: &str = "01J6666666666666666666666V";
    const OTHER_SNAP: &str = "01J7777777777777777777777V";

    #[test]
    fn orphan_matches_when_provenance_records_expected_parent_and_snap() {
        let (_t, by_id, _by_name) = setup();
        write_orphan_fork(&by_id.join(ORPHAN), RELEASED, SNAP);
        assert!(orphan_matches_intended_fork(&by_id, ORPHAN, RELEASED, SNAP));
    }

    #[test]
    fn orphan_doesnt_match_when_parent_ulid_differs() {
        // Orphan from a previous attempt that forked from a different
        // released ULID — must not be reused.
        let (_t, by_id, _by_name) = setup();
        write_orphan_fork(&by_id.join(ORPHAN), OTHER, SNAP);
        assert!(!orphan_matches_intended_fork(
            &by_id, ORPHAN, RELEASED, SNAP
        ));
    }

    #[test]
    fn orphan_doesnt_match_when_snap_differs() {
        // Same parent but different branch snapshot — must not reuse.
        let (_t, by_id, _by_name) = setup();
        write_orphan_fork(&by_id.join(ORPHAN), RELEASED, OTHER_SNAP);
        assert!(!orphan_matches_intended_fork(
            &by_id, ORPHAN, RELEASED, SNAP
        ));
    }

    #[test]
    fn orphan_doesnt_match_when_dir_missing() {
        let (_t, by_id, _by_name) = setup();
        // No write_orphan_fork call — directory absent.
        assert!(!orphan_matches_intended_fork(
            &by_id, ORPHAN, RELEASED, SNAP
        ));
    }

    #[test]
    fn orphan_doesnt_match_when_provenance_corrupted() {
        // Provenance file present but empty — signature verification
        // fails, must not be treated as resumable.
        let (_t, by_id, _by_name) = setup();
        let dir = by_id.join(ORPHAN);
        write_orphan_fork(&dir, RELEASED, SNAP);
        std::fs::write(dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE), "").unwrap();
        assert!(!orphan_matches_intended_fork(
            &by_id, ORPHAN, RELEASED, SNAP
        ));
    }
}
