//! mint entry point.
//!
//! ```text
//! mint serve <config.toml> [bind-addr] [--tigris]  # default bind 127.0.0.1:8085
//! mint enroll-token <config.toml> --coord <ulid> --coord-pub ed25519:<b64> [--ttl <secs>]
//! ```
//!
//! `serve` runs the verification/vending HTTP surface. Without
//! `--tigris` it wires the faked keypair minter (no live account
//! needed). With `--tigris` it wires the real Tigris IAM minter and
//! requires a Tigris admin credential in the environment.
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
use mint::iam::{FakeMinter, KeypairMinter};
use mint::issuance::mint_enrollment_token;
use mint::tigris::TigrisMinter;

/// Default enrollment-token lifetime: a provisioning window, not a
/// session. One-time and key-bound, so an unused leak is inert; this
/// only bounds how long a minted-but-unredeemed token stays usable.
const DEFAULT_ENROLL_TTL_SECONDS: u64 = 3600;

const USAGE: &str = "usage:\n  \
    mint serve <config.toml> [bind-addr] [--tigris]\n  \
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

    let mut tigris = false;
    let mut positional = Vec::new();
    for arg in args {
        match arg.as_str() {
            "--tigris" => tigris = true,
            other if other.starts_with("--") => {
                return Err(format!("unknown flag {other}\n{USAGE}").into());
            }
            _ => positional.push(arg),
        }
    }
    let mut p = positional.into_iter();
    let config_path = p.next().ok_or(USAGE)?;
    let bind: SocketAddr = p
        .next()
        .unwrap_or_else(|| "127.0.0.1:8085".into())
        .parse()?;

    let config = Arc::new(Config::load(std::path::Path::new(&config_path))?);

    // Pick the minter before binding so a misconfigured --tigris fails
    // fast rather than at the first request.
    let minter: Arc<dyn KeypairMinter> = if tigris {
        let admin = config.admin.as_ref().ok_or(
            "--tigris requires a Tigris admin credential in the environment \
             (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)",
        )?;
        Arc::new(TigrisMinter::new(admin)?)
    } else {
        Arc::new(FakeMinter::new())
    };
    tracing::info!(
        audience = %config.audience,
        roles = config.roles.len(),
        admin_credential = config.admin.is_some(),
        minter = if tigris { "tigris" } else { "FAKED" },
        "loaded config"
    );

    let state = AppState {
        config,
        minter,
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
