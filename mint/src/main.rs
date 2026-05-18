//! mint entry point (`docs/design-mint.md` § *Reference client &
//! demo*). clap-derived CLI, matching the elide coordinator's shape.
//!
//! `serve` runs the verification/vending HTTP surface. Until the live
//! Tigris SigV4 minter lands this binary wires [`FakeMinter`] and warns
//! loudly on every start: the enroll/exchange flow is real, but
//! `assume-role` returns a **deterministic fake keypair**. This is an
//! explicit, temporary interim — not a silent optional path — removed
//! when the real minter is wired (`docs/design-mint.md` § *Reference
//! client & demo*: "no stub backend").
//!
//! `bootstrap` / `enroll` are the operator side. The networked
//! `mint client` (the coordinator's half) is the staged tail.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use mint::audit::AuditLog;
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::{FakeMinter, KeypairMinter};
use mint::issuance::mint_bootstrap;
use mint::state::Store;
use mint::tigris::TigrisMinter;

#[derive(Parser)]
#[command(about = "mint: macaroon-authenticated scoped-credential vending for Tigris")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the verification/vending HTTP service.
    Serve {
        #[arg(long, default_value = "mint.toml")]
        config: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8085")]
        bind: SocketAddr,
        /// Use the real Tigris IAM minter (requires a Tigris admin
        /// credential in the environment). Without it, assume-role
        /// returns a deterministic fake keypair.
        #[arg(long)]
        tigris: bool,
    },
    /// Print the bootstrap macaroon (reusable, non-expiring).
    ///
    /// The macaroon goes to stdout for piping; diagnostics to stderr.
    Bootstrap {
        #[arg(long, default_value = "mint.toml")]
        config: PathBuf,
        /// Draw a new bootstrap nonce first, cancelling in-flight
        /// enrollments (outstanding credentials are unaffected).
        #[arg(long)]
        rotate: bool,
    },
    /// Operator: inspect and approve pending enrollments.
    Enroll {
        #[command(subcommand)]
        cmd: EnrollCmd,
    },
    /// Operator: inspect the configured role inventory (read-only).
    Role {
        #[command(subcommand)]
        cmd: RoleCmd,
    },
    /// Reference client — the coordinator's half of the flow.
    Client {
        /// Identity + received-macaroon directory (default
        /// `./mint_client`, analogous to the server's `./mint_data`).
        #[arg(long)]
        client_dir: Option<PathBuf>,
        #[command(subcommand)]
        cmd: ClientCmd,
    },
}

#[derive(Subcommand)]
enum ClientCmd {
    /// Generate a fresh `client.key` / `client.pub` identity pair.
    Keygen {
        /// Overwrite an existing identity (a key is an identity —
        /// off by default).
        #[arg(long)]
        force: bool,
    },
    /// Print this identity's `cnf` value + fingerprint (what the
    /// operator compares out of band before `enroll approve`).
    Fingerprint,
    /// Attenuate the bootstrap macaroon with `sub`/`cnf`, enrol, and
    /// save the returned credential ticket.
    Enroll {
        #[arg(long, default_value = "http://127.0.0.1:8085")]
        url: String,
        /// Opaque principal id — the `sub` (Elide: coordinator ULID).
        #[arg(long)]
        id: String,
        /// Filename (under the client dir) to write the credential
        /// ticket to.
        #[arg(long, default_value_t = mint::client::CREDENTIAL_TICKET_FILE.to_string())]
        out: String,
        /// Bootstrap macaroon: the macaroon text inline, a file path,
        /// or `-` for stdin.
        #[arg(value_name = "BOOTSTRAP")]
        bootstrap: String,
    },
    /// Exchange the credential ticket for the credential (after
    /// approval). Exits 2 while still awaiting operator approval.
    Exchange {
        #[arg(long, default_value = "http://127.0.0.1:8085")]
        url: String,
        /// Role to exchange the ticket for. One credential per role —
        /// run `exchange` once per role you are authorized for.
        #[arg(long)]
        role: String,
        /// Credential-ticket filename (under the client dir) to present.
        #[arg(long = "in", default_value_t = mint::client::CREDENTIAL_TICKET_FILE.to_string())]
        in_file: String,
        /// Filename (under the client dir) to write the credential to.
        /// Defaults to `credentials/<role>`.
        #[arg(long)]
        out: Option<String>,
    },
    /// Inspect the per-role credentials held on disk (local-only).
    Credential {
        #[command(subcommand)]
        cmd: CredentialCmd,
    },
    /// Assume a role with the held credential; prints the keypair JSON.
    AssumeRole {
        #[arg(long, default_value = "http://127.0.0.1:8085")]
        url: String,
        /// Credential filename (under the client dir) to exercise.
        /// Defaults to `credentials/<role>`.
        #[arg(long = "in")]
        in_file: Option<String>,
        /// PoP-signed request body as a JSON object: inline, `@file`,
        /// or `-` for stdin. Opaque pass-through into `request.*` —
        /// `ts`/`role`/`ttl_seconds` are client-owned and ignored here.
        #[arg(long, value_name = "JSON|@FILE|-")]
        request: Option<String>,
        /// Narrowing caveat to attenuate the credential with (repeatable).
        /// Vocabulary-agnostic — e.g. `--caveat elide:Volume=01VOL`.
        #[arg(long = "caveat", value_name = "NAME=VALUE")]
        caveat: Vec<String>,
        #[arg(long, default_value_t = 900)]
        ttl: u64,
        /// Role name from the mint config.
        #[arg(value_name = "ROLE")]
        role: String,
    },
}

#[derive(Subcommand)]
enum CredentialCmd {
    /// List held per-role credentials: role, role caveat, caveat count, sub.
    List,
    /// Narrate one role credential's caveat chain.
    Inspect {
        /// Role whose credential to inspect (`credentials/<role>`).
        #[arg(value_name = "ROLE")]
        role: String,
    },
}

#[derive(Subcommand)]
enum RoleCmd {
    /// List configured roles: name, required caveats, TTL bounds.
    List {
        #[arg(long, default_value = "mint.toml")]
        config: PathBuf,
    },
    /// Show one role: TTL bounds, required caveats, policy source, and
    /// the raw policy template + the substitution surface it references.
    Inspect {
        #[arg(long, default_value = "mint.toml")]
        config: PathBuf,
        /// Role name from the mint config.
        #[arg(value_name = "ROLE")]
        name: String,
    },
}

#[derive(Subcommand)]
enum EnrollCmd {
    /// List pending enrollment records.
    List {
        #[arg(long, default_value = "mint.toml")]
        config: PathBuf,
    },
    /// Approve a pending record by its `sub`.
    ///
    /// Prints the record's `cnf` fingerprint and asks for an
    /// interactive y/N confirmation: confirming **is** the trust anchor
    /// (it must match what the client reports via
    /// `mint client fingerprint`). `--yes` skips the prompt for
    /// automation.
    Approve {
        #[arg(long, default_value = "mint.toml")]
        config: PathBuf,
        /// The opaque principal id (Elide: the coordinator ULID).
        sub: String,
        /// Skip the interactive confirmation (automation only — you are
        /// asserting the fingerprint was verified out of band).
        #[arg(long)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Args::parse().command {
        Command::Serve {
            config,
            bind,
            tigris,
        } => serve(&config, bind, tigris).await,
        Command::Bootstrap { config, rotate } => bootstrap(&config, rotate),
        Command::Enroll { cmd } => match cmd {
            EnrollCmd::List { config } => enroll_list(&config),
            EnrollCmd::Approve { config, sub, yes } => enroll_approve(&config, &sub, yes),
        },
        Command::Role { cmd } => match cmd {
            RoleCmd::List { config } => role_list(&config),
            RoleCmd::Inspect { config, name } => role_inspect(&config, &name),
        },
        Command::Client { client_dir, cmd } => client_cmd(client_dir, cmd).await,
    }
}

async fn client_cmd(
    client_dir: Option<PathBuf>,
    cmd: ClientCmd,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = mint::client::client_dir(client_dir);
    match cmd {
        ClientCmd::Keygen { force } => {
            let (cnf, fp) = mint::client::keygen(&dir, force)?;
            eprintln!("wrote {}/client.key (0600) + client.pub", dir.display());
            println!("cnf={cnf}");
            println!("fingerprint={fp}");
            Ok(())
        }
        ClientCmd::Fingerprint => {
            let (cnf, fp) = mint::client::identity(&dir)?;
            println!("cnf={cnf}");
            println!("fingerprint={fp}");
            Ok(())
        }
        ClientCmd::Enroll {
            url,
            bootstrap,
            id,
            out,
        } => {
            mint::client::enroll(&dir, &url, &bootstrap, &id, &out).await?;
            eprintln!("  (compare the fingerprint out of band before approving)");
            Ok(())
        }
        ClientCmd::Exchange {
            url,
            role,
            in_file,
            out,
        } => {
            let out = out.unwrap_or_else(|| mint::client::credential_path(&role));
            if mint::client::exchange(&dir, &url, &in_file, &role, &out).await? {
                Ok(())
            } else {
                eprintln!(
                    "  re-run `mint client exchange --role {role}` once the operator approves"
                );
                std::process::exit(2);
            }
        }
        ClientCmd::Credential { cmd } => match cmd {
            CredentialCmd::List => Ok(mint::client::credential_list(&dir)?),
            CredentialCmd::Inspect { role } => Ok(mint::client::credential_inspect(&dir, &role)?),
        },
        ClientCmd::AssumeRole {
            url,
            in_file,
            request,
            caveat,
            ttl,
            role,
        } => {
            let in_file = in_file.unwrap_or_else(|| mint::client::credential_path(&role));
            let kp = mint::client::assume_role(
                &dir,
                &url,
                &role,
                request.as_deref(),
                &caveat,
                ttl,
                &in_file,
            )
            .await?;
            println!("{kp}");
            Ok(())
        }
    }
}

fn load(path: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    Ok(Config::load(path)?)
}

/// Open the persisted state store from the config's `data_dir`
/// (defaults to `mint_data` when the config omits it).
fn open_store(cfg: &Config) -> Result<Store, Box<dyn std::error::Error>> {
    Ok(Store::open(&cfg.data_dir)?)
}

async fn serve(
    config: &Path,
    bind: SocketAddr,
    tigris: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Arc::new(load(config)?);
    let store = Arc::new(open_store(&config)?);

    // Pick the minter before binding so a misconfigured --tigris fails
    // fast rather than at the first request.
    let minter: Arc<dyn KeypairMinter> = if tigris {
        let admin = config.admin.as_ref().ok_or(
            "--tigris requires a Tigris admin credential in the environment \
             (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)",
        )?;
        Arc::new(TigrisMinter::new(admin)?)
    } else {
        tracing::warn!(
            "INTERIM: assume-role uses the FAKE keypair minter — it returns a \
             deterministic non-production keypair. Pass --tigris for real \
             Tigris keys. The enroll/exchange flow is real either way."
        );
        Arc::new(FakeMinter::new())
    };
    tracing::info!(
        audience = %config.audience,
        roles = config.roles.len(),
        admin_credential = config.admin.is_some(),
        data_dir = %config.data_dir.display(),
        roles_dir = %config.roles_dir.display(),
        minter = if tigris { "tigris" } else { "fake" },
        "loaded config"
    );

    let state = AppState {
        config,
        minter,
        audit: Arc::new(AuditLog::new(Box::new(std::io::stdout()))),
        store,
    };

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, "mint listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

fn bootstrap(config: &Path, rotate: bool) -> Result<(), Box<dyn std::error::Error>> {
    let config = load(config)?;
    let store = open_store(&config)?;
    let nonce = if rotate {
        let n = store.rotate_bootstrap()?;
        eprintln!("rotated bootstrap nonce; in-flight enrollments cancelled");
        n
    } else {
        store.current_bootstrap()?
    };
    let mac = mint_bootstrap(&store.root_key(), &config.audience, &nonce);
    eprintln!(
        "bootstrap macaroon for audience={} (non-expiring, reusable)",
        config.audience
    );
    println!("{}", mac.encode());
    Ok(())
}

fn enroll_list(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let config = load(config)?;
    let store = open_store(&config)?;
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let rows = store.list(now)?;
    if rows.is_empty() {
        eprintln!("no pending enrollments");
        return Ok(());
    }
    println!(
        "{:<28} {:<18} {:<16} {:>7} {:<9} FLAGS",
        "SUB", "FINGERPRINT", "PEER", "AGE(s)", "APPROVED"
    );
    for r in rows {
        println!(
            "{:<28} {:<18} {:<16} {:>7} {:<9} {}",
            r.sub,
            r.fingerprint,
            r.peer_ip,
            r.age_seconds,
            if r.approved { "yes" } else { "no" },
            if r.anomalous_pub { "ANOMALOUS-PUB" } else { "" }
        );
    }
    Ok(())
}

fn enroll_approve(config: &Path, sub: &str, yes: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;

    let config = load(config)?;
    let store = open_store(&config)?;
    let pending = store
        .get_pending(sub)?
        .ok_or_else(|| format!("no pending enrollment for sub {sub}"))?;
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let fp = mint::state::fingerprint(&pending.pubkey);

    eprintln!("pending enrollment:");
    eprintln!("  sub:         {sub}");
    eprintln!("  fingerprint: {fp}");
    eprintln!("  peer:        {}", pending.peer_ip);
    eprintln!("  age:         {}s", now.saturating_sub(pending.first_seen));

    if !yes {
        eprint!(
            "Approve? This authorises the binding — the fingerprint must \
             match what the client reports (`mint client fingerprint`). [y/N] "
        );
        std::io::stderr().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        if !matches!(line.trim(), "y" | "Y" | "yes" | "YES") {
            eprintln!("not approved");
            std::process::exit(1);
        }
    }

    if store.approve(sub)? {
        eprintln!("approved {sub}");
        Ok(())
    } else {
        // Raced away between get_pending and approve (e.g. GC / rotate).
        Err(format!("no pending enrollment for sub {sub}").into())
    }
}

fn role_list(config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let config = load(config)?;
    if config.roles.is_empty() {
        eprintln!("no roles configured");
        return Ok(());
    }
    println!(
        "{:<16} {:>7} {:>7} {:>7}  REQUIRED-CAVEATS",
        "NAME", "MIN", "DEF", "MAX"
    );
    // config.roles is a BTreeMap, so iteration is name-sorted.
    for r in config.roles.values() {
        println!(
            "{:<16} {:>7} {:>7} {:>7}  {}",
            r.name,
            r.min_ttl_seconds,
            r.default_ttl_seconds,
            r.max_ttl_seconds,
            if r.required_caveats.is_empty() {
                "(none)".to_string()
            } else {
                r.required_caveats.join(", ")
            }
        );
    }
    Ok(())
}

fn role_inspect(config: &Path, name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config = load(config)?;
    let role = config
        .roles
        .get(name)
        .ok_or_else(|| format!("no role {name} in config (see `mint role list`)"))?;
    eprintln!("role: {}", role.name);
    eprintln!(
        "  ttl_seconds:      min={} default={} max={}",
        role.min_ttl_seconds, role.default_ttl_seconds, role.max_ttl_seconds
    );
    eprintln!(
        "  required_caveats: {}",
        if role.required_caveats.is_empty() {
            "(none)".to_string()
        } else {
            role.required_caveats.join(", ")
        }
    );
    eprintln!("  audience:         {}", config.audience);
    eprintln!("  tenant.bucket:    {}", config.tenant.bucket);
    eprintln!("  policy source:    {}", role.policy_path.display());

    // The policy is a request-parameterised template: there is no
    // single concrete grant to print, so show the substitution surface
    // (by trust provenance) + the raw template, not a rendering.
    let surface = mint::template::template_surface(&role.policy);
    eprintln!("  policy references:");
    for (label, vals) in [
        ("caveat (MAC-bound)", &surface.caveats),
        ("request (PoP-bound)", &surface.request),
        ("tenant (config)", &surface.tenant),
        ("system (mint-computed)", &surface.system),
    ] {
        if !vals.is_empty() {
            eprintln!("    {label}: {}", vals.join(", "));
        }
    }
    eprintln!("  policy template:");
    println!("{}", role.policy);
    Ok(())
}
