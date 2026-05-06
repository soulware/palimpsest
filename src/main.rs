use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use ext4_view::{Ext4, Ext4Error, PathBuf as Ext4PathBuf};

use elide_core::signing::{VOLUME_KEY_FILE, VOLUME_PROVENANCE_FILE, VOLUME_PUB_FILE};
use elide_core::volume;

use elide::{
    VolumeFetchInputs, coordinator_client, extents, inspect, inspect_files, ls, nbd, parse_size,
    resolve_volume_dir, resolve_volume_size, ublk, validate_volume_name, verify,
};

/// Elide volume management and analysis tools.
#[derive(Parser)]
struct Args {
    /// Directory containing volumes and the coordinator socket.
    /// When unset, defaults to `elide_data` for non-coord commands;
    /// `coord run`/`start`/`stop` instead consult `--config` (if given)
    /// and fall back to `elide_data` only when neither is provided, so
    /// the value in `coordinator.toml` is honoured.
    #[arg(long, env = "ELIDE_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,

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

    /// Manage the coordinator daemon (start, stop)
    Coord {
        #[command(subcommand)]
        command: CoordCommand,
    },
}

#[derive(Subcommand)]
enum CoordCommand {
    /// Start the coordinator as a detached background process.
    ///
    /// Spawns `elide-coordinator serve` in a new session (setsid),
    /// redirects its stdout/stderr to `<data_dir>/elide.log`, and
    /// waits for the control socket to come up before returning.
    Start {
        /// Path to the coordinator config file (forwarded to the
        /// daemon). Defaults to `coordinator.toml` in the daemon's
        /// working directory.
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Stop the coordinator. By default, also terminates managed volume
    /// processes. With `--keep-volumes`, the coordinator exits but its
    /// volume children continue running detached — used for rolling
    /// upgrades where the coordinator is being replaced without
    /// disturbing live volumes.
    Stop {
        /// Leave volume children running (rolling-upgrade path).
        #[arg(long)]
        keep_volumes: bool,
        /// Path to the coordinator config file. Used to resolve the
        /// data_dir (and thus the control socket) when `--data-dir`
        /// is not given. Falls back to `elide_data` if neither is set.
        #[arg(long)]
        config: Option<PathBuf>,
    },

    /// Run the coordinator in the foreground.
    ///
    /// Thin wrapper that execs the sibling `elide-coordinator serve`
    /// binary with stdio inherited. Use this as the `ExecStart` for a
    /// `Type=simple` systemd unit, or for interactive debugging.
    /// Signals reach the coordinator directly; both SIGINT and SIGTERM
    /// trigger the defensive shutdown (volumes left running).
    Run {
        /// Path to the coordinator config file. Defaults to
        /// `coordinator.toml` in the working directory.
        #[arg(long)]
        config: Option<PathBuf>,
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

    /// Show the per-name event log for a volume.
    ///
    /// Lists every entry in `events/<name>/`, one per line, in
    /// chronological order. Each event's signature is verified
    /// against the emitting coordinator's published pubkey; events
    /// whose signature is invalid or whose pubkey can't be fetched
    /// are flagged with a sigil rather than hidden.
    Events {
        /// Volume name
        name: String,
        /// Emit one JSON object per event instead of the human form.
        #[arg(long)]
        json: bool,
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
    // rustls 0.23 requires a process-level default `CryptoProvider` to be
    // installed before any TLS connection is made. Feature-flag auto-detect
    // fails in our dependency graph (rustls pulled in transitively, not as
    // a direct dep of any crate that enables the `aws_lc_rs` feature on it),
    // so install it explicitly. `install_default` returns `Err` if another
    // provider was already installed — ignore that, it's fine.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args = Args::parse();

    // Concrete data_dir for non-coord commands: CLI flag (or env) wins,
    // otherwise fall back to `elide_data`. The coord subcommands use
    // their own resolution that also considers `--config`.
    let cli_data_dir = args.data_dir.clone();
    let data_dir = cli_data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("elide_data"));
    let coord = coordinator_client::Client::new(data_dir.join("control.sock"));
    let by_id_dir = data_dir.join("by_id");

    // Initialise tracing. `serve-volume` is a long-lived host process
    // that shares the unified `<data_dir>/elide.log` with the
    // coordinator (each writer opens its own fd; concurrent appends
    // compose). It also tees through the coord-side log relay socket
    // so live output reaches whichever coord is currently attached
    // to the operator's terminal. Every other subcommand is
    // short-lived CLI work and logs to stderr only.
    match &args.command {
        Command::ServeVolume { fork_dir, .. } => {
            // fork_dir is `<data_dir>/by_id/<ulid>/`, so data_dir is two
            // levels up. Fall back to `data_dir` from the CLI flag if the
            // path shape is unexpected — init failure should not stop the
            // volume coming up.
            let inferred = fork_dir
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| data_dir.clone());
            if elide_coordinator::log_init::init_for_volume(&inferred).is_err() {
                elide_coordinator::log_init::init_stderr();
            }
        }
        _ => elide_coordinator::log_init::init_stderr(),
    }

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
                if let Err(e) = list_volumes(&data_dir, &coord, filter, all) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Info { name } => {
                let vol_dir = resolve_volume_dir(&data_dir, &name);
                if let Err(e) = inspect::run(&vol_dir, &by_id_dir) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Verify { name } => {
                let vol_dir = resolve_volume_dir(&data_dir, &name);
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
                let vol_dir = resolve_volume_dir(&data_dir, &name);
                if let Err(e) = ls::run(&vol_dir, &path) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Snapshot { name } => match coord.snapshot_volume(&name) {
                Ok(ulid) => println!("{ulid}"),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            },

            VolumeCommand::GenerateFilemap { name, snapshot } => {
                match coord.generate_filemap(&name, snapshot.as_deref()) {
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
                        &data_dir,
                        &name,
                        from,
                        &coord,
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
                    let ulid = match coord.create_volume_remote(&name, bytes, &flags) {
                        Ok(u) => u,
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    };
                    let by_name = data_dir.join("by_name").join(&name);
                    let by_id = data_dir.join("by_id").join(&ulid);
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
                match coord.update_volume(&name, &flags) {
                    Ok(reply) if reply.restarted => {
                        println!("volume restarting with new config")
                    }
                    Ok(_) => {
                        println!("volume not running; config will take effect on next start")
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }

            VolumeCommand::Status { name, remote } => {
                if remote {
                    match coord.status_remote(&name) {
                        Ok(rs) => print_remote_status(&name, &rs),
                        Err(e) => {
                            eprintln!("{name}: {e}");
                            std::process::exit(1);
                        }
                    }
                } else {
                    match coord.status(&name) {
                        Ok(reply) => println!("{name}: {}", reply.lifecycle.wire_body()),
                        Err(e) => {
                            eprintln!("{name}: {e}");
                            std::process::exit(1);
                        }
                    }
                }
            }

            VolumeCommand::Events { name, json } => match coord.volume_events(&name) {
                Ok(reply) => {
                    if json {
                        for entry in &reply.events {
                            match serde_json::to_string(entry) {
                                Ok(s) => println!("{s}"),
                                Err(e) => {
                                    eprintln!("{name}: serialise event: {e}");
                                    std::process::exit(1);
                                }
                            }
                        }
                    } else {
                        print_volume_events(&reply);
                    }
                }
                Err(e) => {
                    eprintln!("{name}: {e}");
                    std::process::exit(1);
                }
            },

            VolumeCommand::Import(import_args) => match import_args.command {
                Some(ImportSubcommand::Status { name }) => {
                    use coordinator_client::ImportStatusReply;
                    match coord.import_status_by_name(&name) {
                        Ok(ImportStatusReply::Running) => println!("{name}: running"),
                        Ok(ImportStatusReply::Done) => println!("{name}: done"),
                        Err(e) => {
                            eprintln!("{name}: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                Some(ImportSubcommand::Attach { name }) => {
                    let mut stdout = std::io::stdout();
                    if let Err(e) = coord.import_attach_by_name(&name, &mut stdout) {
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
                    if let Err(e) = coord.import_start(&name, &oci_ref, &import_args.extents_from) {
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
                        if let Err(e) = coord.import_attach_by_name(&name, &mut stdout) {
                            eprintln!("import failed: {e}");
                            std::process::exit(1);
                        }
                        // Optionally create a fork immediately after import.
                        if let Some(fork_name) = import_args.fork
                            && let Err(e) = create_fork(
                                &data_dir,
                                &fork_name,
                                &name,
                                &coord,
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
                match coord.evict_volume(&name, segment.as_deref()) {
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
                if let Err(e) = coord.remove_volume(&name, force) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }

            VolumeCommand::Stop { name, release } => {
                if let Err(e) = coord.stop_volume(&name) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                if release {
                    match coord.release_volume(&name, false) {
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
                    if let Err(e) = run_claim(&name, &coord) {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                } else if let Some(vol_ulid) = resolve_local_volume_ulid(&data_dir, &name) {
                    // Plain start. Common case: volume was claimed and
                    // started before, prefetch is long done — quick probe
                    // returns Ok instantly and we stay silent. Edge case:
                    // a previous claim's streaming was Ctrl-C'd before
                    // completion, or the coordinator restarted mid-
                    // prefetch — quick probe times out, we surface the
                    // wait so start isn't a silent multi-second hang.
                    install_prefetch_ctrlc_handler(&name, "[start]");
                    let quick = std::time::Duration::from_millis(250);
                    if coord.await_prefetch(&vol_ulid, quick).is_err() {
                        eprintln!("[start] waiting for ancestor prefetch...");
                        match coord
                            .await_prefetch(&vol_ulid, coordinator_client::PREFETCH_AWAIT_BUDGET)
                        {
                            Ok(()) => eprintln!("[start] ready"),
                            Err(e) => eprintln!(
                                "[start] prefetch did not finish in time ({e}); \
                                 coordinator continues in background"
                            ),
                        }
                    }
                }
                if let Err(e) = coord.start_volume(&name) {
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
                if let Err(e) = run_claim(&name, &coord) {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
                println!("{name}: claimed");
            }

            VolumeCommand::Release { name, force } => match coord.release_volume(&name, force) {
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
            },
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

        Command::Coord { command } => match command {
            CoordCommand::Start { config } => {
                let result = resolve_coord_data_dir(cli_data_dir.as_deref(), config.as_deref())
                    .and_then(|dd| coord_start(&dd, cli_data_dir.as_deref(), config.as_deref()));
                if let Err(e) = result {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
            CoordCommand::Stop {
                keep_volumes,
                config,
            } => {
                let result = resolve_coord_data_dir(cli_data_dir.as_deref(), config.as_deref())
                    .and_then(|dd| {
                        let coord = coordinator_client::Client::new(dd.join("control.sock"));
                        coord_stop(&coord, &dd, keep_volumes)
                    });
                if let Err(e) = result {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
            CoordCommand::Run { config } => {
                // coord_run execs the coordinator and lets it own
                // data_dir resolution from --config. We only forward
                // --data-dir when the user explicitly set it.
                let e = coord_run(cli_data_dir.as_deref(), config.as_deref());
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
    }
}

/// Resolve the data_dir for `coord` subcommands: explicit `--data-dir`
/// (or `ELIDE_DATA_DIR`) wins, otherwise parse `--config` to read its
/// `data_dir` field, otherwise fall back to `elide_data`. This mirrors
/// the precedence used inside the coordinator itself, so the CLI and
/// the daemon always agree on which directory holds the pidfile,
/// socket, and per-volume state.
fn resolve_coord_data_dir(cli: Option<&Path>, config: Option<&Path>) -> std::io::Result<PathBuf> {
    if let Some(dd) = cli {
        return Ok(dd.to_owned());
    }
    if let Some(cfg) = config {
        let parsed = elide_coordinator::config::load(cfg)
            .map_err(|e| std::io::Error::other(format!("loading {}: {e:#}", cfg.display())))?;
        return Ok(parsed.data_dir);
    }
    Ok(PathBuf::from("elide_data"))
}

/// Exec the sibling `elide-coordinator serve` with stdio inherited.
/// Returns only on failure — on success, exec replaces this process so
/// signals and exit status flow directly through the coordinator.
fn coord_run(cli_data_dir: Option<&Path>, config: Option<&Path>) -> std::io::Error {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let bin = sibling_bin("elide-coordinator");
    let mut cmd = Command::new(&bin);
    cmd.arg("serve");
    // Only forward --data-dir if the user explicitly set it; otherwise
    // let the coordinator's config loader own the default so the
    // `data_dir` field in coordinator.toml is honoured.
    if let Some(dd) = cli_data_dir {
        cmd.arg("--data-dir").arg(dd);
    }
    if let Some(cfg) = config {
        cmd.arg("--config").arg(cfg);
    }
    // exec() returns only on failure; the success path replaces this
    // process image with the coordinator's, so signals (SIGINT,
    // SIGTERM, SIGHUP) and the exit code flow through directly.
    let err = cmd.exec();
    std::io::Error::other(format!("execing {}: {err}", bin.display()))
}

/// Time to wait for the coordinator's control socket to appear after
/// `coord start` spawns the daemon.
const COORD_START_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Time to wait for the coordinator to exit after `coord stop` sends
/// the Shutdown IPC. Generous because full teardown waits for volume
/// children to exit (10s SIGTERM grace + drain).
const COORD_STOP_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// In the direct-teardown fallback, how long to give a volume child to
/// react to SIGTERM (graceful flush + exit) before escalating to
/// SIGKILL. The ublk transport's signal watcher caps its own flush at
/// 3s, so 10s is well past the worst legitimate case.
const SIGTERM_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// After SIGKILL, how long to wait for the kernel to actually reap the
/// process before giving up. SIGKILL can't be blocked, so anything
/// past this is a kernel-side stuck process (e.g. D-state on broken
/// I/O) that no supervisor can clean up.
const SIGKILL_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Spawn `elide-coordinator serve` as a detached background process.
///
/// Stdout/stderr are appended to `<data_dir>/elide.log`; the
/// child is placed in a new session (setsid) so it survives the parent
/// shell. We then poll for the control socket to appear, returning
/// once it accepts connections.
fn coord_start(
    data_dir: &Path,
    cli_data_dir: Option<&Path>,
    config: Option<&Path>,
) -> std::io::Result<()> {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    std::fs::create_dir_all(data_dir).map_err(|e| {
        std::io::Error::other(format!("creating data dir {}: {e}", data_dir.display()))
    })?;
    let pid_path = data_dir.join("coordinator.pid");
    if let Ok(text) = std::fs::read_to_string(&pid_path)
        && let Ok(pid) = text.trim().parse::<u32>()
        && elide_core::process::pid_is_alive(pid)
    {
        return Err(std::io::Error::other(format!(
            "coordinator already running (pid {pid})"
        )));
    }

    let bin = sibling_bin("elide-coordinator");
    let log_path = data_dir.join("elide.log");
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|e| std::io::Error::other(format!("opening {}: {e}", log_path.display())))?;
    let log_clone = log_file.try_clone()?;

    let mut cmd = Command::new(&bin);
    cmd.arg("serve");
    // Only forward --data-dir if the user explicitly set it; otherwise
    // let the coordinator resolve from --config (or its own default)
    // so the `data_dir` field in coordinator.toml is honoured.
    if let Some(dd) = cli_data_dir {
        cmd.arg("--data-dir").arg(dd);
    }
    if let Some(cfg) = config {
        cmd.arg("--config").arg(cfg);
    }
    cmd.stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_clone));
    // Detach from the parent's session so the daemon survives the shell.
    // pre_exec runs between fork() and exec(); setsid() is async-signal-safe.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| std::io::Error::other(format!("spawning {}: {e}", bin.display())))?;
    let child_pid = child.id();

    let socket_path = data_dir.join("control.sock");
    let deadline = std::time::Instant::now() + COORD_START_WAIT;
    loop {
        if std::os::unix::net::UnixStream::connect(&socket_path).is_ok() {
            println!(
                "coordinator started (pid {child_pid}, socket {})",
                socket_path.display()
            );
            return Ok(());
        }
        // Detect early exit: try_wait reaps the zombie if the daemon
        // already exited. pid_is_alive returns true for zombies (the
        // process entry exists until the parent waits), which would
        // mask early failures until our deadline expires.
        if let Ok(Some(status)) = child.try_wait() {
            return Err(std::io::Error::other(format!(
                "coordinator exited before becoming ready ({status}); see {}",
                log_path.display()
            )));
        }
        if std::time::Instant::now() >= deadline {
            return Err(std::io::Error::other(format!(
                "timed out waiting for control socket {}; see {}",
                socket_path.display(),
                log_path.display()
            )));
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Send the `Shutdown` IPC and poll until the coordinator process actually
/// exits (socket disappears / pidfile pid stops responding).
///
/// When the coordinator is *not* running, falls back to a direct
/// per-volume teardown: walks `<data_dir>/by_id/*/`, SIGTERMs every
/// `volume.pid` and `import.pid` it finds, and waits for them to exit.
/// This makes `coord stop` the universal "clean up after me" verb,
/// covering both the dev workflow (Ctrl+C'd a foreground `coord run`,
/// orphans left behind) and post-crash cleanup. With `--keep-volumes`
/// the fallback is a no-op since "coordinator gone, volumes running"
/// is already the desired state.
fn coord_stop(
    coord: &coordinator_client::Client,
    data_dir: &Path,
    keep_volumes: bool,
) -> std::io::Result<()> {
    if !coord.is_reachable() {
        if keep_volumes {
            println!(
                "coordinator not running ({}); volumes left running",
                coord.socket_path().display()
            );
            return Ok(());
        }
        return fallback_stop_volumes(data_dir);
    }
    coord.shutdown(keep_volumes)?;

    let pid_path = data_dir.join("coordinator.pid");
    let pid = std::fs::read_to_string(&pid_path)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok());
    let deadline = std::time::Instant::now() + COORD_STOP_WAIT;
    loop {
        let still_alive = match pid {
            Some(p) => elide_core::process::pid_is_alive(p),
            None => coord.is_reachable(),
        };
        if !still_alive {
            println!(
                "coordinator stopped{}",
                if keep_volumes {
                    " (volumes left running)"
                } else {
                    ""
                }
            );
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(std::io::Error::other(format!(
                "coordinator did not exit within {COORD_STOP_WAIT:?}"
            )));
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Direct teardown of every running volume + import process under
/// `<data_dir>/by_id/*/` when the coordinator socket is unreachable.
/// Reads each `volume.pid` / `import.pid`, SIGTERMs the live ones, and
/// polls until they exit or the wait budget expires.
fn fallback_stop_volumes(data_dir: &Path) -> std::io::Result<()> {
    let by_id = data_dir.join("by_id");
    let entries = match std::fs::read_dir(&by_id) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("no volume processes to stop ({} absent)", by_id.display());
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    let mut pids: Vec<(u32, String)> = Vec::new();
    for entry in entries.flatten() {
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        for filename in &["volume.pid", "import.pid"] {
            let pid_path = dir.join(filename);
            let Ok(text) = std::fs::read_to_string(&pid_path) else {
                continue;
            };
            let Ok(pid) = text.trim().parse::<u32>() else {
                continue;
            };
            if !elide_core::process::pid_is_alive(pid) {
                continue;
            }
            let Ok(raw) = i32::try_from(pid) else {
                continue;
            };
            let label = format!("{filename}={pid} in {}", dir.display());
            // SAFETY: libc::kill takes a pid + signal; the kernel checks
            // permission and existence. SIGTERM has no observable
            // side-effect on this process.
            if unsafe { libc::kill(raw, libc::SIGTERM) } != 0 {
                let err = std::io::Error::last_os_error();
                eprintln!("SIGTERM {label}: {err}");
                continue;
            }
            println!("SIGTERM {label}");
            pids.push((pid, label));
        }
    }

    if pids.is_empty() {
        println!("no volume processes to stop");
        return Ok(());
    }

    // Phase 1: wait up to SIGTERM_GRACE for graceful exit.
    let term_deadline = std::time::Instant::now() + SIGTERM_GRACE;
    let mut survivors = wait_for_exit(&pids, term_deadline);
    if survivors.is_empty() {
        println!("all volume processes stopped");
        return Ok(());
    }

    // Phase 2: anything still alive after SIGTERM_GRACE gets SIGKILL.
    // The volume's ublk/NBD signal watcher caps its own flush at 3s,
    // so anything past SIGTERM_GRACE is a wedged process the operator
    // wants gone. Mirrors `systemctl stop`'s TimeoutStopSec → SIGKILL.
    for (pid, label) in &survivors {
        let Ok(raw) = i32::try_from(*pid) else {
            continue;
        };
        // SAFETY: libc::kill takes a pid + signal; the kernel checks
        // permission and existence. SIGKILL cannot be caught or
        // blocked, so this either succeeds or returns ESRCH.
        if unsafe { libc::kill(raw, libc::SIGKILL) } != 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("SIGKILL {label}: {err}");
            continue;
        }
        println!("SIGKILL {label} (did not exit within {SIGTERM_GRACE:?})");
    }

    let kill_deadline = std::time::Instant::now() + SIGKILL_WAIT;
    survivors = wait_for_exit(&survivors, kill_deadline);
    if survivors.is_empty() {
        println!("all volume processes stopped");
        return Ok(());
    }
    let labels: Vec<&str> = survivors.iter().map(|(_, l)| l.as_str()).collect();
    Err(std::io::Error::other(format!(
        "{n} process(es) still alive after SIGKILL+{SIGKILL_WAIT:?}: {labels:?}",
        n = survivors.len()
    )))
}

/// Wait until `deadline` for every pid in `pids` to exit; return the
/// ones still alive when the wait expires. Polls at 100ms intervals.
fn wait_for_exit(pids: &[(u32, String)], deadline: std::time::Instant) -> Vec<(u32, String)> {
    loop {
        let still: Vec<(u32, String)> = pids
            .iter()
            .filter(|(p, _)| elide_core::process::pid_is_alive(*p))
            .cloned()
            .collect();
        if still.is_empty() || std::time::Instant::now() >= deadline {
            return still;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Locate a sibling binary alongside the running `elide` executable.
/// Falls back to PATH lookup if the current exe path can't be resolved.
fn sibling_bin(name: &str) -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|dir| dir.join(name)))
        .unwrap_or_else(|| PathBuf::from(name))
}

enum ListFilter {
    Writable,
    Readonly,
    All,
}

use elide_coordinator::volume_state::{VolumeLifecycle, VolumeMode};

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
    coord: &coordinator_client::Client,
    filter: ListFilter,
    include_ancestors: bool,
) -> std::io::Result<()> {
    let coordinator_up = coord.is_reachable();
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
            println!(
                "coordinator is not running ({})",
                coord.socket_path().display()
            );
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
        println!(
            "coordinator is not running ({})",
            coord.socket_path().display()
        );
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
    coord: &coordinator_client::Client,
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

    // Parse `from` into one of three forms. Maps directly to the
    // `ForkSource` enum the coordinator's `fork-start` IPC takes.
    let source = if let Some((vol, snap)) = from.split_once('/') {
        let vol_ulid = ulid::Ulid::from_string(vol)
            .map_err(|e| std::io::Error::other(format!("invalid volume ULID in --from: {e}")))?;
        let snap_ulid = ulid::Ulid::from_string(snap)
            .map_err(|e| std::io::Error::other(format!("invalid snapshot ULID in --from: {e}")))?;
        if force_snapshot {
            return Err(std::io::Error::other(
                "--force-snapshot conflicts with an explicit snapshot pin in --from <vol>/<snap>",
            ));
        }
        coordinator_client::ForkSource::Pinned {
            vol_ulid,
            snap_ulid,
        }
    } else if let Ok(vol_ulid) = ulid::Ulid::from_string(from) {
        coordinator_client::ForkSource::BareUlid { vol_ulid }
    } else {
        coordinator_client::ForkSource::Name {
            name: from.to_owned(),
        }
    };

    coord.fork_start(fork_name, source, force_snapshot, flags)?;

    // Stream coordinator-side progress to stderr (chain pull, snapshot
    // decision, fork mint, prefetch warm-up). The terminal `Done`
    // event hands back the new fork's ULID.
    let mut stderr = std::io::stderr();
    let new_vol_ulid = coord.fork_attach_by_name(fork_name, &mut stderr)?;
    let new_fork_dir = by_id_dir.join(new_vol_ulid.to_string());
    println!("{}", new_fork_dir.display());
    Ok(())
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
fn run_claim(name: &str, coord: &coordinator_client::Client) -> std::io::Result<()> {
    use coordinator_client::ClaimStartReply;
    match coord.claim_start(name)? {
        ClaimStartReply::Reclaimed => Ok(()),
        ClaimStartReply::Claiming { released_vol_ulid } => {
            eprintln!("[claim] claiming '{name}' from {released_vol_ulid}");
            install_prefetch_ctrlc_handler(name, "[claim]");
            let mut stderr = std::io::stderr();
            // Don't fail the claim if the attach stream errors out at
            // the prefetch step — the bucket-side claim and local fork
            // are durable by the time we get here, mirroring the prior
            // CLI behaviour. Pre-prefetch errors do still surface.
            match coord.claim_attach_by_name(name, &mut stderr) {
                Ok(_) => Ok(()),
                Err(e) => {
                    eprintln!(
                        "[claim] attach stream ended with error ({e}); \
                         coordinator continues in background"
                    );
                    Ok(())
                }
            }
        }
    }
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

/// Resolve the fetch config for a volume subprocess (`serve-volume`).
///
/// The coordinator exports `ELIDE_COORDINATOR_SOCKET` into each spawned
/// volume's environment. When set, we pull store config from the
/// coordinator over IPC and authenticate to its credential vending via
/// the macaroon handshake (`register` then `credentials`). The volume
/// ULID is the fork directory's basename. When the env var is unset
/// (standalone `elide serve-volume` invocation with no coordinator),
/// fall back to `FetchConfig::load` from the volume directory.
fn resolve_volume_fetch_config(fork_dir: &Path) -> std::io::Result<VolumeFetchInputs> {
    if let Ok(sock) = std::env::var("ELIDE_COORDINATOR_SOCKET") {
        let coord = coordinator_client::Client::new(&sock);
        let volume_ulid = elide_fetch::derive_volume_id(fork_dir)?;
        if let Some(inputs) = fetch_config_via_coordinator_macaroon(&coord, &volume_ulid)? {
            return Ok(inputs);
        }
    }
    Ok(VolumeFetchInputs {
        fetch_config: elide_fetch::FetchConfig::load(fork_dir)?,
        peer_endpoint: None,
    })
}

/// Pull store config + macaroon-scoped S3 credentials from the
/// coordinator over IPC. The CLI itself never holds raw S3 credentials
/// — only spawned volume subprocesses can authenticate (PID-bound via
/// SO_PEERCRED) and obtain creds for demand-fetch.
///
/// The coordinator's [`coordinator_client::RegisterReply`] also carries
/// the discovered peer-fetch endpoint for the volume's previous
/// claimer (when peer-fetch is configured and a clean handoff is in
/// the event log); it is returned alongside the fetch config so the
/// daemon can stack a peer body byte-range fetcher in front of S3.
fn fetch_config_via_coordinator_macaroon(
    coord: &coordinator_client::Client,
    volume_ulid: &str,
) -> std::io::Result<Option<VolumeFetchInputs>> {
    if !coord.socket_path().exists() {
        return Ok(None);
    }
    let config = match coord.get_store_config() {
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
        return Ok(Some(VolumeFetchInputs {
            fetch_config: Some(elide_fetch::FetchConfig {
                bucket: None,
                endpoint: None,
                region: None,
                local_path: Some(path),
                fetch_batch_bytes: None,
            }),
            peer_endpoint: None,
        }));
    }
    let Some(bucket) = config.bucket else {
        return Err(std::io::Error::other(
            "coordinator returned empty store config",
        ));
    };
    let registered = coord.register_and_get_creds(volume_ulid)?;
    // SAFETY: serve-volume startup is single-threaded at this point — no
    // background tasks have been spawned, so there are no concurrent env
    // readers.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", &registered.creds.access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &registered.creds.secret_access_key);
        if let Some(tok) = &registered.creds.session_token {
            std::env::set_var("AWS_SESSION_TOKEN", tok);
        } else {
            std::env::remove_var("AWS_SESSION_TOKEN");
        }
    }
    Ok(Some(VolumeFetchInputs {
        fetch_config: Some(elide_fetch::FetchConfig {
            bucket: Some(bucket),
            endpoint: config.endpoint,
            region: config.region,
            local_path: None,
            fetch_batch_bytes: None,
        }),
        peer_endpoint: registered.peer_endpoint,
    }))
}

/// Pretty-print a `StatusRemoteReply` for `elide volume status --remote`.
fn print_volume_events(reply: &coordinator_client::VolumeEventsReply) {
    use coordinator_client::SignatureStatus;
    use elide_core::volume_event::EventKind;

    if reply.events.is_empty() {
        println!("(no events)");
        return;
    }

    for entry in &reply.events {
        let ev = &entry.event;
        // Sigil column: a single character, padded to width 1, so the
        // log lines stay aligned. Empty for valid signatures keeps
        // the common case visually quiet.
        let sigil = match &entry.signature_status {
            SignatureStatus::Valid => ' ',
            SignatureStatus::Invalid { .. } => '!',
            SignatureStatus::KeyUnavailable { .. } => '?',
            SignatureStatus::Missing => '-',
        };
        let when = ev.at.format("%Y-%m-%d %H:%M:%SZ");
        let kind_label = ev.kind.as_str();
        let coord = &ev.coordinator_id;
        let vol = ev.vol_ulid;
        // `on=<hostname>` only when the event recorded one. Old
        // events from before this field landed render without it
        // rather than with `on=<unknown>` clutter.
        let host = match ev.hostname.as_deref() {
            Some(h) => format!(" on={h}"),
            None => String::new(),
        };
        let extra = match &ev.kind {
            EventKind::Created | EventKind::Claimed => String::new(),
            EventKind::Released { handoff_snapshot } => {
                format!(" handoff={handoff_snapshot}")
            }
            EventKind::ForceReleased {
                handoff_snapshot,
                displaced_coordinator_id,
            } => {
                format!(" handoff={handoff_snapshot} displaced={displaced_coordinator_id}")
            }
            EventKind::ForkedFrom {
                source_name,
                source_vol_ulid,
                source_snap_ulid,
            } => {
                format!(" source={source_name}@{source_vol_ulid}/{source_snap_ulid}")
            }
            EventKind::RenamedTo { new_name } => format!(" to={new_name}"),
            EventKind::RenamedFrom {
                old_name,
                inherits_log_from,
            } => {
                format!(" from={old_name} inherits={inherits_log_from}")
            }
        };
        println!("{sigil} {when}  {kind_label:<14} vol={vol}  by={coord}{host}{extra}");
    }

    // Footer: any non-Valid statuses get explained, so the operator
    // sees the failure reason without re-running with --json.
    let mut had_explanation = false;
    for entry in &reply.events {
        let reason = match &entry.signature_status {
            SignatureStatus::Valid => continue,
            SignatureStatus::Invalid { reason } => format!("invalid: {reason}"),
            SignatureStatus::KeyUnavailable { reason } => {
                format!("key unavailable: {reason}")
            }
            SignatureStatus::Missing => "no signature on event".to_string(),
        };
        if !had_explanation {
            println!();
            had_explanation = true;
        }
        println!("  {} {}", entry.event.event_ulid, reason);
    }
}

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
