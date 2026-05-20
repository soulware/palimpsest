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
mod enroll;
mod fetch;
mod fork;
mod import;
mod inbound;
mod mint_client;
mod mint_stores;
mod pidfile;
mod rescan;
mod shutdown;
mod start_remote;
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

    /// Enrol this coordinator with the configured mint and provision
    /// its per-role credentials.
    ///
    /// One blocking step: POST /v1/enroll, wait while the operator runs
    /// `mint enroll approve <coordinator-id>` on the mint host, then
    /// exchange the ticket for every role, writing
    /// `<data_dir>/credentials/<role>`. Requires `[mint]` in the
    /// config. Idempotent: re-running only fills missing roles (use
    /// `--force` to re-exchange all). `coord serve` refuses to start
    /// until this has completed.
    Enroll {
        #[arg(long, default_value = "coordinator.toml")]
        config: PathBuf,
        /// Override the data_dir from the config file.
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Bootstrap macaroon: the macaroon text inline, a file path,
        /// or `-` for stdin. Distributed out of band by the operator.
        bootstrap: String,
        /// Overall bound on waiting for operator approval (humantime).
        #[arg(long, default_value = "30m", value_parser = parse_humantime)]
        timeout: std::time::Duration,
        /// Re-exchange and overwrite every role credential, not just
        /// the missing ones.
        #[arg(long)]
        force: bool,
    },
}

fn parse_humantime(s: &str) -> Result<std::time::Duration, String> {
    humantime::parse_duration(s).map_err(|e| format!("invalid duration {s:?}: {e}"))
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
            elide_coordinator::log_init::init_for_coord(&config.data_dir).with_context(|| {
                format!("initialising tracing in {}", config.data_dir.display())
            })?;
            // Start the volume → coord log relay server so volumes
            // tee their output through whichever coord is currently
            // attached to the operator's terminal. No-op when stderr
            // already routes to elide.log (daemon mode). Best-effort:
            // a failure here just means volumes lose the live-tee
            // path and fall back to file-only — coord itself is
            // unaffected, so we log and continue rather than refuse
            // to start.
            if let Err(e) = elide_coordinator::log_relay::start(&config.data_dir) {
                tracing::warn!(
                    "[log-relay] failed to start ({e}); volumes will log to elide.log only"
                );
            }
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

            // Loaded here so the coord-id is in scope for the
            // mint-stores wiring and the per-coordinator caps-probe key
            // below; daemon::run also calls load_or_generate, but it's
            // idempotent.
            let identity = std::sync::Arc::new(
                elide_coordinator::identity::CoordinatorIdentity::load_or_generate(
                    &config.data_dir,
                )
                .map_err(|e| anyhow::anyhow!("loading coordinator identity: {e}"))?,
            );

            if let Some(mint_cfg) = &config.mint {
                mint_cfg.validate()?;
                // Refuse to start half-credentialed: every enrolled
                // role's credential must be present and decode. The
                // mint path is not exercisable until enrollment has
                // provisioned them.
                enroll::assert_enrolled(&config.data_dir)?;
                tracing::info!(
                    "[coordinator] store: mint-backed scoped \
                     (coord-base / coord-writer / coord-data); reachability \
                     and conditional-PUT are validated lazily on first \
                     assume-role per role"
                );
                let stores: std::sync::Arc<dyn elide_coordinator::stores::ScopedStores> =
                    std::sync::Arc::new(mint_stores::MintScopedStores::new(
                        mint_cfg,
                        config.store.clone(),
                        config.data_dir.clone(),
                        identity.clone(),
                    ));
                let result = daemon::run(config, stores).await;
                let _ = std::fs::remove_file(&pid_path);
                return result;
            }

            let store = config.store.build()?;
            tracing::info!("[coordinator] store: {}", config.store.describe());
            tracing::info!(
                "[coordinator] store scoping: shared-key passthrough \
                 (single AWS_* key for every op; no per-volume scoping)"
            );
            config.store.precheck_env()?;

            // Verify conditional-PUT support up front: the lifecycle
            // verbs (`mark_stopped`, `mark_released`,
            // `claim_started_from_released`) all rely on `If-Match`
            // ETag updates against `names/<name>`. Failing here is far
            // clearer than warning on every `volume stop` later.
            // Probe key is per-coordinator so concurrent startups against
            // the same bucket don't race on a shared key. Placed under
            // `by_id/` — the prefix the probe's Put+Delete is always
            // permitted on.
            let probe_key = object_store::path::Path::from(format!(
                "by_id/__elide_caps_probe_{}__",
                identity.coordinator_id_str()
            ));
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
        Command::Enroll {
            config,
            data_dir,
            bootstrap,
            timeout,
            force,
        } => {
            elide_coordinator::log_init::init_stderr();
            let mut config = config::load(&config)?;
            if let Some(dir) = data_dir {
                config.data_dir = dir;
            }
            let mint_cfg = config.mint.as_ref().with_context(|| {
                "`elide coord enroll` requires a [mint] section in coordinator.toml \
                 (without it the coordinator uses the shared-key downgrade and has \
                 nothing to enrol)"
            })?;
            mint_cfg.validate()?;
            std::fs::create_dir_all(&config.data_dir)
                .with_context(|| format!("creating data dir: {}", config.data_dir.display()))?;
            let identity = elide_coordinator::identity::CoordinatorIdentity::load_or_generate(
                &config.data_dir,
            )
            .with_context(|| "loading coordinator identity")?;
            enroll::run(
                mint_cfg,
                &identity,
                &config.data_dir,
                &bootstrap,
                timeout,
                force,
            )
            .await
            .map_err(anyhow::Error::from)
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
