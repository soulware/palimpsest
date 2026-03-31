// elide-coordinator: manages segment upload and object store lifecycle.
//
// Subcommands:
//   run [--config <path>]
//     Start the coordinator daemon. Watches configured volume roots, discovers
//     forks, and continuously drains pending segments to S3. Store and timing
//     configuration comes from the config file (default: coordinator.toml).
//
//   drain-pending <fork-dir>
//     Upload all committed segments from pending/ to the object store, then exit.
//     Each segment is handled independently; partial success is possible.
//     Exits non-zero if any segment failed to upload.
//
//   prefetch-indexes <fork-dir>
//     Download index sections (.idx) for all ancestor segments not present locally.
//
// Store selection for one-shot commands (mutually exclusive):
//   --local <path>          Use a local directory as the object store (no server needed).
//   (default)               Use S3 via env vars: ELIDE_S3_BUCKET, AWS_ENDPOINT_URL,
//                           AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.

mod config;
mod control;
mod daemon;
mod gc;
mod prefetch;
mod serve_config;
mod store;
mod supervisor;
mod upload;

use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use object_store::ObjectStore;

#[derive(Parser)]
#[command(about = "Elide coordinator: manages segment upload and object store lifecycle")]
struct Args {
    /// Use a local directory as the object store instead of S3.
    /// Applies to one-shot commands (drain-pending, prefetch-indexes).
    /// The `run` command reads its store config from coordinator.toml.
    #[arg(long, global = true, value_name = "PATH")]
    local: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the coordinator daemon.
    ///
    /// Watches configured volume root directories, discovers forks automatically,
    /// and continuously drains pending segments to the object store. Store and
    /// timing configuration are read from the config file.
    Run {
        /// Path to the coordinator configuration file.
        #[arg(long, default_value = "coordinator.toml")]
        config: PathBuf,
    },

    /// Upload all pending segments for a fork to the object store, then exit.
    DrainPending {
        /// Path to the fork directory (e.g. volumes/myvm/base or volumes/myvm/forks/vm1)
        fork_dir: PathBuf,
    },

    /// Download index sections (.idx) for all ancestor segments not present locally.
    ///
    /// Run this before serving a forked volume on a host that has no local copies
    /// of its ancestor segments. Populates fetched/<ulid>.idx for each ancestor
    /// segment so Volume::open can rebuild the LBA map without the full segment
    /// bodies. Individual reads then demand-fetch body bytes on first access.
    PrefetchIndexes {
        /// Path to the fork directory to warm up (e.g. volumes/myvm/forks/vm2)
        fork_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    tracing_log::LogTracer::init().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Err(e) = run().await {
        tracing::error!("{e:#}");
        process::exit(1);
    }
}

fn build_store(local: Option<PathBuf>) -> Result<Arc<dyn ObjectStore>> {
    if let Some(path) = local {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("creating local store dir: {}", path.display()))?;
        Ok(Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&path)
                .context("building local store")?,
        ))
    } else {
        store::StoreConfig::from_env()?.build()
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Run { config } => {
            let config = config::load(&config)?;
            let store = config.store.build()?;
            daemon::run(config, store).await
        }

        Command::DrainPending { fork_dir } => {
            let store = build_store(args.local)?;
            let (volume_id, fork_name) = upload::derive_names(&fork_dir)
                .context("resolving volume and fork names from fork dir")?;

            let result = upload::drain_pending(&fork_dir, &volume_id, &fork_name, &store).await?;

            println!("{} uploaded, {} failed", result.uploaded, result.failed);

            if result.failed > 0 {
                process::exit(1);
            }

            Ok(())
        }

        Command::PrefetchIndexes { fork_dir } => {
            let store = build_store(args.local)?;
            let result = prefetch::prefetch_indexes(&fork_dir, &store).await?;

            println!(
                "{} fetched, {} already present, {} failed",
                result.fetched, result.skipped, result.failed
            );

            if result.failed > 0 {
                process::exit(1);
            }

            Ok(())
        }
    }
}
