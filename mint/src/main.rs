//! mint entry point.
//!
//! ```text
//! mint serve <config.toml> [bind-addr]            # default bind 127.0.0.1:8085
//! mint enroll-token <config.toml> --coord <ulid> --coord-pub ed25519:<b64> [--ttl <secs>]
//! ```
//!
//! `serve` runs the verification/vending HTTP surface. The prototype
//! always wires the faked keypair minter — swapping in a real
//! `TigrisMinter` is the single change to go live (see `iam.rs`).
//!
//! `enroll-token` is the operator side of issuance (`docs/design-mint.md`
//! § *Coordinator bootstrap*): it holds the mint root (from the config
//! TOML) and emits a base64 enrollment token for one named coordinator —
//! the admin-local "vouch for coordinator X" act that stands in for an
//! identity-authority discharge while third-party caveats are deferred
//! (design *Open questions* #15). The token is handed out-of-band to
//! whoever brings the coordinator up; the coordinator exchanges it for a
//! non-expiring primary at `POST /v1/enroll`.

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use mint::audit::AuditLog;
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::issuance::mint_enrollment_token;

/// Default enrollment-token lifetime: a provisioning window, not a
/// session. One-time and key-bound, so an unused leak is inert; this
/// only bounds how long a minted-but-unredeemed token stays usable.
const DEFAULT_ENROLL_TTL_SECONDS: u64 = 3600;

const USAGE: &str = "usage:\n  \
    mint serve <config.toml> [bind-addr]\n  \
    mint enroll-token <config.toml> --coord <ulid> --coord-pub ed25519:<b64> [--ttl <secs>]";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().ok_or(USAGE)?;
    match cmd.as_str() {
        "serve" => serve(args.collect()).await,
        "enroll-token" => enroll_token(args.collect()),
        _ => Err(USAGE.into()),
    }
}

async fn serve(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut a = args.into_iter();
    let config_path = a.next().ok_or(USAGE)?;
    let bind: SocketAddr = a
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

fn enroll_token(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let mut a = args.into_iter();
    let config_path = a.next().ok_or(USAGE)?;
    let mut coord: Option<String> = None;
    let mut coord_pub: Option<String> = None;
    let mut ttl = DEFAULT_ENROLL_TTL_SECONDS;
    while let Some(flag) = a.next() {
        match flag.as_str() {
            "--coord" => coord = Some(a.next().ok_or("--coord needs a value")?),
            "--coord-pub" => coord_pub = Some(a.next().ok_or("--coord-pub needs a value")?),
            "--ttl" => {
                ttl = a
                    .next()
                    .ok_or("--ttl needs a value")?
                    .parse()
                    .map_err(|_| "--ttl must be a positive integer (seconds)")?
            }
            other => return Err(format!("unknown flag {other}\n{USAGE}").into()),
        }
    }
    let coord = coord.ok_or("--coord <ulid> is required")?;
    let coord_pub = coord_pub.ok_or("--coord-pub ed25519:<b64> is required")?;

    let config = Config::load(std::path::Path::new(&config_path))?;
    let not_after = (Utc::now().timestamp().max(0) as u64)
        .checked_add(ttl)
        .ok_or("--ttl overflows")?;

    let token = mint_enrollment_token(
        &config.trust_root,
        &config.audience,
        &coord,
        &coord_pub,
        not_after,
    )?;
    // The token is the only thing this command emits — straight to
    // stdout so it can be piped/captured; diagnostics go to stderr.
    eprintln!(
        "enrollment token for coord={coord} audience={} expires in {ttl}s",
        config.audience
    );
    println!("{}", token.encode());
    Ok(())
}
