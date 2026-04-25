// elide-coordinator: manages segment upload, GC, and volume process supervision.
//
// Subcommands:
//   serve [--config <path>]
//     Start the coordinator daemon. Watches configured volume roots, discovers
//     forks, supervises volume processes, drains pending segments to S3, and
//     runs segment GC. Configuration comes from coordinator.toml.

// Binary-only modules (process supervision, IPC, import jobs).
mod credential;
mod daemon;
mod import;
mod inbound;
mod macaroon;
mod supervisor;

// Re-use the library's shared modules so types are identical across the
// lib and bin compilation units.
use elide_coordinator::config;

use std::path::PathBuf;
use std::process;

use anyhow::Result;
use clap::{Parser, Subcommand};

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
}

#[tokio::main]
async fn main() {
    tracing_log::LogTracer::init().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

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
            let store = config.store.build()?;
            tracing::info!("[coordinator] store: {}", config.store.describe());
            config.store.probe(store.as_ref()).await?;
            tracing::info!("[coordinator] store: reachable");
            daemon::run(config, store).await
        }
    }
}
