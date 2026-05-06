// elide-coordinator: manages segment upload, GC, and volume process supervision.
//
// Subcommands:
//   serve [--config <path>]
//     Start the coordinator daemon. Watches configured volume roots, discovers
//     forks, supervises volume processes, drains pending segments to S3, and
//     runs segment GC. Configuration comes from coordinator.toml.
//   init [--config <path>] [--force]
//     Write a default coordinator.toml (commented template) to the given path.

// Binary-only modules (process supervision, IPC, import jobs).
mod claim;
mod credential;
mod daemon;
mod fork;
mod import;
mod inbound;
mod macaroon;
mod rescan;
mod shutdown;
mod supervisor;
mod ublk_sweep;

// Re-use the library's shared modules so types are identical across the
// lib and bin compilation units.
use elide_coordinator::{config, portable};

use std::path::PathBuf;
use std::process;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use elide_core::process::pid_is_alive;

/// Coordinator-process pidfile. Lives at `<data_dir>/coordinator.pid` so
/// `elide coord start` can refuse to start a second instance for the
/// same data directory and `elide coord stop` can fall back to
/// PID-based liveness if the IPC reply never arrives.
const COORDINATOR_PID_FILE: &str = "coordinator.pid";

#[derive(Parser)]
#[command(about = "Elide coordinator: manages volumes, segment upload, and GC")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the coordinator daemon.
    ///
    /// Watches the configured data directory, discovers forks automatically,
    /// supervises volume processes, and continuously drains pending segments to
    /// the object store. Configuration is read from coordinator.toml.
    Serve {
        #[arg(long, default_value = "coordinator.toml")]
        config: PathBuf,
        /// Override the data_dir from the config file.
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Write a default coordinator.toml (commented template) to the given path.
    ///
    /// Every field in the template is commented out — the values shown are
    /// the defaults the daemon would use if the file were absent. Edit the
    /// fields you want to override.
    Init {
        #[arg(long, default_value = "coordinator.toml")]
        config: PathBuf,
        /// Overwrite the file if it already exists.
        #[arg(long)]
        force: bool,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        tracing::error!("{e:#}");
        process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Serve { config, data_dir } => {
            let mut config = config::load(&config)?;
            if let Some(dir) = data_dir {
                config.data_dir = dir;
            }
            // Initialise tracing now that we know the resolved data_dir.
            // Coordinator + every volume share `<data_dir>/elide.log`,
            // each opening it independently. When stderr already points
            // at the same file (e.g. `elide coord start` redirected the
            // daemon's stdio there), the tee is suppressed.
            elide_coordinator::log_init::init_with_data_dir(&config.data_dir).with_context(
                || format!("initialising tracing in {}", config.data_dir.display()),
            )?;
            // Refuse to start if another coordinator is already serving
            // this data dir, and write our own pidfile. Removed when
            // `daemon::run` returns (clean shutdown). On panic / kill -9
            // a stale-but-dead pidfile is silently replaced by the next
            // start.
            std::fs::create_dir_all(&config.data_dir)
                .with_context(|| format!("creating data dir: {}", config.data_dir.display()))?;
            let pid_path = config.data_dir.join(COORDINATOR_PID_FILE);
            if let Ok(text) = std::fs::read_to_string(&pid_path)
                && let Ok(pid) = text.trim().parse::<u32>()
                && pid_is_alive(pid)
            {
                bail!(
                    "coordinator already running (pid {pid}, pidfile {})",
                    pid_path.display()
                );
            }
            std::fs::write(&pid_path, std::process::id().to_string())
                .with_context(|| format!("writing pidfile: {}", pid_path.display()))?;

            let store = config.store.build()?;
            tracing::info!("[coordinator] store: {}", config.store.describe());
            tracing::info!(
                "[coordinator] store scoping: passthrough (single key for every \
                 op; per-volume scoping not yet wired)"
            );
            config.store.probe(store.as_ref()).await?;
            tracing::info!("[coordinator] store: reachable");

            // Verify conditional-PUT support up front: the lifecycle
            // verbs (`mark_stopped`, `mark_released`,
            // `claim_started_from_released`) all rely on `If-Match`
            // ETag updates against `names/<name>`. Failing here is far
            // clearer than warning on every `volume stop` later.
            let probe_key = object_store::path::Path::from("__elide_caps_probe__");
            let caps = portable::probe_capabilities(store.as_ref(), &probe_key)
                .await
                .context("probing bucket capabilities")?;
            if !caps.conditional_put {
                bail!(
                    "object store ({}) does not support conditional PUT \
                     (If-Match / If-None-Match); portable-live-volume \
                     lifecycle verbs cannot run safely against this \
                     backend. For S3/Tigris this typically means \
                     `with_conditional_put(S3ConditionalPut::ETagMatch)` \
                     was not set on the client",
                    config.store.describe()
                );
            }
            tracing::info!("[coordinator] store: conditional PUT supported");

            let stores: std::sync::Arc<dyn elide_coordinator::stores::ScopedStores> =
                std::sync::Arc::new(elide_coordinator::stores::PassthroughStores::new(store));
            let result = daemon::run(config, stores).await;
            let _ = std::fs::remove_file(&pid_path);
            result
        }
        Command::Init { config, force } => {
            elide_coordinator::log_init::init_stderr();
            init_config(&config, force)
        }
    }
}

fn init_config(path: &std::path::Path, force: bool) -> Result<()> {
    if path.exists() && !force {
        bail!(
            "{} already exists; pass --force to overwrite",
            path.display()
        );
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating parent directory: {}", parent.display()))?;
    }
    std::fs::write(path, config::DEFAULT_CONFIG_TEMPLATE)
        .with_context(|| format!("writing config: {}", path.display()))?;
    println!("wrote default config to {}", path.display());
    Ok(())
}
