//! mint server entry point.
//!
//! Usage: `mint <config.toml> [bind-addr]` (default bind `127.0.0.1:8085`).
//!
//! The prototype always wires the faked keypair minter — no real Tigris
//! call is made. Swapping in a real `TigrisMinter` is the single change
//! to go live (see `iam.rs`).

use std::net::SocketAddr;
use std::sync::Arc;

use mint::audit::AuditLog;
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let config_path = args.next().ok_or("usage: mint <config.toml> [bind-addr]")?;
    let bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:8085".into())
        .parse()?;

    let config = Arc::new(Config::load(std::path::Path::new(&config_path))?);
    tracing::info!(
        audience = %config.audience,
        roles = config.roles.len(),
        admin_credential = config.admin.is_some(),
        "loaded config (prototype: keypair minting is FAKED)"
    );
    if config.admin.is_none() {
        tracing::warn!(
            "no Tigris admin credential in env (AWS_ACCESS_KEY_ID / \
             AWS_SECRET_ACCESS_KEY); fine for the faked minter, but a \
             real Tigris minter would refuse to start"
        );
    }

    let state = AppState {
        config,
        minter: Arc::new(FakeMinter::new()),
        audit: Arc::new(AuditLog::new(Box::new(std::io::stdout()))),
    };

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "mint listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
