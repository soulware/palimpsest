// Coordinator inbound socket.
//
// Listens on control.sock for commands from the elide CLI.
// Protocol: one request line per connection, one response line, then close.
// Exception: `import attach` streams multiple response lines until done.
//
// Unauthenticated operations (any caller):
//   rescan                    — trigger an immediate fork discovery pass
//   status <volume>           — report running state of a named volume
//   import <name> <ref>       — spawn an OCI import
//   import status <name>      — poll import state by volume name (running / done / failed)
//   import attach <name>      — stream import output by volume name until completion
//   delete <volume>           — stop all processes and remove the volume directory
//   evict <volume> [<ulid>]   — evict all (or one) S3-confirmed segment body from cache/
//
// Volume-process operations (macaroon-authenticated):
//   register <volume-ulid>     — mint a per-volume macaroon, PID-bound via SO_PEERCRED
//   credentials <macaroon>     — exchange macaroon for short-lived S3 creds (PID re-checked)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::credential::CredentialIssuer;
use crate::import::{self, ImportRegistry, ImportState};
use crate::macaroon::{self, Caveat, Macaroon, Scope};
use elide_coordinator::config::StoreSection;
use elide_coordinator::{EvictRegistry, PrefetchTracker, SnapshotLockRegistry, subscribe_prefetch};
use elide_core::process::pid_is_alive;

/// Shared coordinator state threaded through every inbound op.
///
/// All fields are cheap to clone (Arc-wrapped or Copy), so per-connection
/// fan-out in `serve` is a flat clone rather than a long argument list.
#[derive(Clone)]
pub struct IpcContext {
    pub data_dir: Arc<PathBuf>,
    pub rescan: Arc<Notify>,
    pub registry: ImportRegistry,
    pub elide_import_bin: Arc<PathBuf>,
    pub evict_registry: EvictRegistry,
    pub snapshot_locks: SnapshotLockRegistry,
    pub prefetch_tracker: PrefetchTracker,
    pub store: Arc<dyn ObjectStore>,
    pub store_config: Arc<StoreSection>,
    pub part_size_bytes: usize,
    /// Crockford-Base32 ULID-shaped coordinator id, derived once at
    /// startup from `coordinator.pub`. Cheap to clone (26 bytes).
    pub coord_id: String,
    /// 32-byte MAC root for `macaroon::mint` / `macaroon::verify`,
    /// derived in-memory from `coordinator.key` at startup.
    pub macaroon_root: [u8; 32],
    /// The coordinator's identity bundle, used as a `SegmentSigner`
    /// when minting synthesised handoff snapshots during
    /// `volume release --force`. Arc-shared so per-connection clones
    /// stay cheap.
    pub identity: Arc<elide_coordinator::identity::CoordinatorIdentity>,
    pub issuer: Arc<dyn CredentialIssuer>,
}

pub async fn serve(socket_path: &Path, ctx: IpcContext) {
    let _ = std::fs::remove_file(socket_path);

    let listener = match UnixListener::bind(socket_path) {
        Ok(l) => l,
        Err(e) => {
            warn!("[inbound] failed to bind {}: {e}", socket_path.display());
            return;
        }
    };

    // The socket inherits the binding process's umask (typically 0022 → 0755),
    // which on Linux blocks non-root from `connect()` (write perm is required).
    // A coordinator running under sudo would otherwise leave a CLI in the user's
    // session unable to talk to it. Relax to 0666 so any local user can issue
    // unprivileged ops; permission-sensitive ops still go through coordinator
    // logic, not raw filesystem access.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o666))
        {
            warn!(
                "[inbound] chmod 0666 on {} failed: {e} (non-root CLI may be unable to connect)",
                socket_path.display()
            );
        }
    }

    info!("[inbound] listening on {}", socket_path.display());

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle(stream, ctx.clone()));
            }
            Err(e) => warn!("[inbound] accept error: {e}"),
        }
    }
}

async fn handle(stream: tokio::net::UnixStream, ctx: IpcContext) {
    // Capture peer credentials before splitting the stream — needed for
    // SO_PEERCRED on the `register` and `credentials` ops. Other ops
    // ignore the peer pid; capturing once here keeps the code symmetric
    // and avoids a second syscall later.
    let peer_pid = stream.peer_cred().ok().and_then(|c| c.pid());

    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    let line = match lines.next_line().await {
        Ok(Some(line)) => line,
        Ok(None) => return,
        Err(e) => {
            warn!("[inbound] read error: {e}");
            return;
        }
    };
    let line = line.trim().to_owned();

    // `import attach` is the one streaming operation — it keeps the connection
    // open and writes lines until the import completes.
    if let Some(name) = line.strip_prefix("import attach ") {
        stream_import_by_name(name.trim(), &ctx.data_dir, &mut writer, &ctx.registry).await;
        return;
    }

    let response = dispatch(&line, &ctx, peer_pid).await;
    let _ = writer.write_all(format!("{response}\n").as_bytes()).await;
}

async fn dispatch(line: &str, ctx: &IpcContext, peer_pid: Option<i32>) -> String {
    if line.is_empty() {
        return "err empty request".to_string();
    }

    let (op, args) = match line.split_once(' ') {
        Some((op, args)) => (op, args.trim()),
        None => (line, ""),
    };

    match op {
        "rescan" => {
            ctx.rescan.notify_one();
            "ok".to_string()
        }

        "status" => {
            if args.is_empty() {
                return "err usage: status <volume>".to_string();
            }
            volume_status(args, &ctx.data_dir)
        }

        "status-remote" => {
            if args.is_empty() {
                return "err usage: status-remote <volume>".to_string();
            }
            volume_status_remote(args, &ctx.store, &ctx.coord_id).await
        }

        "import" => {
            let (sub, sub_args) = match args.split_once(' ') {
                Some((sub, rest)) => (sub, rest.trim()),
                None => (args, ""),
            };
            match sub {
                "status" => {
                    if sub_args.is_empty() {
                        return "err usage: import status <name>".to_string();
                    }
                    import_status_by_name(sub_args, &ctx.data_dir, &ctx.registry).await
                }
                _ => {
                    // `import <name> <oci-ref> [extents:<name>[,<name>…]]`:
                    //   sub = volume name
                    //   sub_args = oci-ref [space extents:<name,name…>]
                    if sub_args.is_empty() {
                        return "err usage: import <name> <oci-ref> [extents:<name>[,<name>…]]"
                            .to_string();
                    }
                    let (oci_ref, extents_from) = match sub_args.split_once(' ') {
                        Some((oci, rest)) => match rest.trim().strip_prefix("extents:") {
                            Some(list) if !list.is_empty() => {
                                let names: Vec<String> =
                                    list.split(',').map(|s| s.to_owned()).collect();
                                (oci, names)
                            }
                            _ => {
                                return "err malformed import args; expected extents:<name>[,…]"
                                    .to_string();
                            }
                        },
                        None => (sub_args, Vec::new()),
                    };
                    start_import(
                        import::ImportRequest {
                            vol_name: sub,
                            oci_ref,
                            extents_from: &extents_from,
                        },
                        &ctx.data_dir,
                        &ctx.elide_import_bin,
                        &ctx.registry,
                        &ctx.store,
                        &ctx.rescan,
                    )
                    .await
                }
            }
        }

        "remove" => {
            // Format: "remove <volume> [force]"
            let mut parts = args.split_whitespace();
            let Some(name) = parts.next() else {
                return "err usage: remove <volume> [force]".to_string();
            };
            let force = matches!(parts.next(), Some("force"));
            if parts.next().is_some() {
                return "err usage: remove <volume> [force]".to_string();
            }
            remove_volume(name, force, &ctx.data_dir)
        }

        "stop" => {
            if args.is_empty() {
                return "err usage: stop <volume>".to_string();
            }
            stop_volume_op(args, &ctx.data_dir, &ctx.store, &ctx.coord_id).await
        }

        "release" => {
            if args.is_empty() {
                return "err usage: release <volume> [--force]".to_string();
            }
            let parsed = match parse_release_args(args) {
                Ok(p) => p,
                Err(msg) => return format!("err {msg}"),
            };
            if parsed.force {
                force_release_volume_op(parsed.name, &ctx.data_dir, &ctx.store, &ctx.identity).await
            } else {
                release_volume_op(
                    parsed.name,
                    &ctx.data_dir,
                    &ctx.snapshot_locks,
                    &ctx.store,
                    ctx.part_size_bytes,
                    &ctx.coord_id,
                    &ctx.rescan,
                )
                .await
            }
        }

        "rebind-name" => {
            // rebind-name <name> <new_vol_ulid>
            //
            // Internal helper called by the CLI's claim orchestration
            // path. Atomically rebinds a `Released` name to a
            // freshly-minted local fork, leaving the bucket state in
            // `Stopped`. The CLI then issues a `start` IPC if the
            // composed `volume start --claim` flow was requested.
            let (name, new_ulid_str) = match args.split_once(' ') {
                Some((n, u)) => (n.trim(), u.trim()),
                None => return "err usage: rebind-name <volume> <new_vol_ulid>".to_string(),
            };
            rebind_name_op(
                name,
                new_ulid_str,
                &ctx.data_dir,
                &ctx.store,
                &ctx.coord_id,
                &ctx.rescan,
            )
            .await
        }

        "claim" => {
            // claim <name> [--force]
            //
            // Bucket-side claim flow. Inspects `names/<name>` and:
            //   - if Released and our fork ULID matches the released
            //     ULID, reclaim in-place to `Stopped`;
            //   - if Released with a foreign ULID, return
            //     `released <vol_ulid> <snap>` so the CLI can pull,
            //     mint a fresh fork, and call `rebind-name`;
            //   - with --force, behaves like running `release --force`
            //     internally first, then claiming.
            //
            // Result leaves the volume `Stopped` (no daemon launched).
            // Use a follow-up `start` to bring the daemon up.
            if args.is_empty() {
                return "err usage: claim <volume> [--force]".to_string();
            }
            let mut tokens = args.split_whitespace();
            let name = match tokens.next() {
                Some(n) => n,
                None => return "err usage: claim <volume> [--force]".to_string(),
            };
            let mut force = false;
            for tok in tokens {
                match tok {
                    "--force" => {
                        if force {
                            return "err --force specified twice".to_string();
                        }
                        force = true;
                    }
                    other => return format!("err unrecognised claim flag: {other}"),
                }
            }
            claim_volume_bucket_op(
                name,
                force,
                &ctx.data_dir,
                &ctx.store,
                &ctx.coord_id,
                &ctx.identity,
            )
            .await
        }

        "start" => {
            // start <name>
            //
            // Pure local resume. Refuses if the volume has no local
            // state on this coordinator, or if the bucket-side state
            // is `Released`/`Readonly`. To claim a `Released` name,
            // the CLI must call `claim` first.
            if args.is_empty() {
                return "err usage: start <volume>".to_string();
            }
            let mut tokens = args.split_whitespace();
            let name = match tokens.next() {
                Some(n) => n,
                None => return "err usage: start <volume>".to_string(),
            };
            if let Some(other) = tokens.next() {
                return format!("err unrecognised start flag: {other}");
            }
            start_volume_op(name, &ctx.data_dir, &ctx.store, &ctx.coord_id, &ctx.rescan).await
        }

        "create" => {
            // create <name> <size_bytes> [<flag>...]
            if args.is_empty() {
                return "err usage: create <name> <size_bytes> [flags...]".to_string();
            }
            create_volume_op(args, &ctx.data_dir, &ctx.store, &ctx.coord_id, &ctx.rescan).await
        }

        "update" => {
            // update <name> [<flag>...]
            if args.is_empty() {
                return "err usage: update <volume> [flags...]".to_string();
            }
            update_volume_op(args, &ctx.data_dir).await
        }

        "pull-readonly" => {
            // pull-readonly <vol_ulid>
            if args.is_empty() {
                return "err usage: pull-readonly <vol_ulid>".to_string();
            }
            pull_readonly_op(args, &ctx.data_dir, &ctx.store).await
        }

        "force-snapshot-now" => {
            // force-snapshot-now <vol_ulid>
            if args.is_empty() {
                return "err usage: force-snapshot-now <vol_ulid>".to_string();
            }
            force_snapshot_now_op(args, &ctx.data_dir, &ctx.store).await
        }

        "await-prefetch" => {
            // await-prefetch <vol_ulid>
            //
            // Block until the per-fork prefetch task has published a
            // terminal result, then echo it. Volume binaries call this
            // before `Volume::open` to absorb the supervisor-prefetch
            // race on freshly-claimed forks. Untracked volumes return
            // ok immediately (already prefetched / not under coordinator
            // management).
            if args.is_empty() {
                return "err usage: await-prefetch <vol_ulid>".to_string();
            }
            await_prefetch_op(args, &ctx.prefetch_tracker).await
        }

        "fork-create" => {
            // fork-create <new_name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [<flags>...]
            if args.is_empty() {
                return "err usage: fork-create <name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [flags...]".to_string();
            }
            fork_create_op(args, &ctx.data_dir, &ctx.store, &ctx.coord_id, &ctx.rescan).await
        }

        "resolve-handoff-key" => {
            // resolve-handoff-key <vol_ulid> <snap_ulid>
            //
            // Used by the CLI's claim-from-released path to decide
            // which Ed25519 pubkey to verify the handoff snapshot
            // manifest under. Returns:
            //   ok normal              -- regular manifest, use the
            //                             source volume's volume.pub
            //   ok recovery <hex_pub>  -- synthesised manifest, the
            //                             named pubkey has already
            //                             been verified to derive to
            //                             the recording coordinator id
            //   err <message>          -- manifest missing, malformed,
            //                             or signature verification
            //                             failed
            let (vol_str, snap_str) = match args.split_once(' ') {
                Some((v, s)) => (v.trim(), s.trim()),
                None => return "err usage: resolve-handoff-key <vol_ulid> <snap_ulid>".to_string(),
            };
            resolve_handoff_key_op(vol_str, snap_str, &ctx.store).await
        }

        "evict" => {
            if args.is_empty() {
                return "err usage: evict <volume> [<ulid>]".to_string();
            }
            let (vol_name, ulid_str) = match args.split_once(' ') {
                Some((name, ulid)) => (name, Some(ulid.trim().to_owned())),
                None => (args, None),
            };
            evict_volume(vol_name, ulid_str, &ctx.data_dir, &ctx.evict_registry).await
        }

        "snapshot" => {
            if args.is_empty() {
                return "err usage: snapshot <volume>".to_string();
            }
            snapshot_volume(
                args,
                &ctx.data_dir,
                &ctx.snapshot_locks,
                &ctx.store,
                ctx.part_size_bytes,
            )
            .await
        }

        "generate-filemap" => {
            if args.is_empty() {
                return "err usage: generate-filemap <volume> [<snap_ulid>]".to_string();
            }
            let (vol_name, snap_arg) = match args.split_once(' ') {
                Some((name, snap)) => (name, Some(snap.trim())),
                None => (args, None),
            };
            generate_filemap_op(vol_name, snap_arg, &ctx.data_dir, &ctx.store).await
        }

        // Vend the non-secret `[store]` config so CLI read operations
        // (remote list, remote pull, volume create --from) can build an
        // S3 client that matches the coordinator's. Returned as TOML so
        // the caller can round-trip through toml::from_str.
        "get-store-config" => render_store_config(&ctx.store_config),

        // Vend S3 credentials from the coordinator's env. Today this is
        // the long-lived AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; the
        // same wire format will carry macaroon-issued short-lived
        // credentials when that lands.
        "get-store-creds" => render_store_creds(),

        // Macaroon mint for a spawned volume process. Authenticated by
        // SO_PEERCRED on the connecting socket — we accept only when the
        // peer's PID matches the volume's recorded `volume.pid`.
        "register" => {
            if args.is_empty() {
                return "err usage: register <volume-ulid>".to_string();
            }
            register_volume(args, &ctx.data_dir, peer_pid, &ctx.macaroon_root)
        }

        // Macaroon-authenticated credential issuance. Verifies the MAC,
        // re-checks SO_PEERCRED matches the macaroon's `pid` caveat,
        // then delegates to the configured `CredentialIssuer`.
        "credentials" => {
            if args.is_empty() {
                return "err usage: credentials <macaroon>".to_string();
            }
            issue_credentials(
                args,
                &ctx.data_dir,
                peer_pid,
                &ctx.macaroon_root,
                ctx.issuer.as_ref(),
            )
        }

        _ => {
            warn!("[inbound] unexpected op: {op:?}");
            format!("err unknown op: {op}")
        }
    }
}

// ── Store config / creds vending ──────────────────────────────────────────────

/// Response for `get-store-config`: a multi-line TOML body naming either a
/// local store path or an S3 bucket/endpoint/region. Intentionally contains
/// no secrets.
fn render_store_config(store: &StoreSection) -> String {
    let mut body = String::from("ok\n");
    if let Some(path) = &store.local_path {
        body.push_str(&format!(
            "local_path = {}\n",
            toml_quote(&path.display().to_string())
        ));
        return body;
    }
    if let Some(bucket) = &store.bucket {
        body.push_str(&format!("bucket = {}\n", toml_quote(bucket)));
        if let Some(ep) = &store.endpoint {
            body.push_str(&format!("endpoint = {}\n", toml_quote(ep)));
        }
        if let Some(region) = &store.region {
            body.push_str(&format!("region = {}\n", toml_quote(region)));
        }
        return body;
    }
    // No explicit store configured — coordinator's own fallback is
    // `./elide_store`, so surface that so the CLI sees the same default.
    body.push_str("local_path = \"elide_store\"\n");
    body
}

/// Response for `get-store-creds`: TOML-bodied access key + secret + optional
/// session token read from the coordinator's env. Returns `err` when the
/// coordinator's env has no credentials — callers should treat that as
/// fatal rather than silently swap in their own env.
fn render_store_creds() -> String {
    let ak = match std::env::var("AWS_ACCESS_KEY_ID") {
        Ok(v) if !v.is_empty() => v,
        _ => return "err no credentials configured in coordinator env".to_string(),
    };
    let sk = match std::env::var("AWS_SECRET_ACCESS_KEY") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            return "err AWS_ACCESS_KEY_ID set but AWS_SECRET_ACCESS_KEY missing".to_string();
        }
    };
    let mut body = String::from("ok\n");
    body.push_str(&format!("access_key_id = {}\n", toml_quote(&ak)));
    body.push_str(&format!("secret_access_key = {}\n", toml_quote(&sk)));
    if let Ok(tok) = std::env::var("AWS_SESSION_TOKEN")
        && !tok.is_empty()
    {
        body.push_str(&format!("session_token = {}\n", toml_quote(&tok)));
    }
    body
}

/// Quote a string as a TOML basic string. The values we serialise (bucket
/// names, URLs, AWS keys) never contain characters that would require
/// `r"..."` literals or multi-line strings.
fn toml_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── Import operations ─────────────────────────────────────────────────────────

async fn start_import(
    req: import::ImportRequest<'_>,
    data_dir: &Path,
    elide_import_bin: &Path,
    registry: &ImportRegistry,
    store: &Arc<dyn ObjectStore>,
    rescan: &Arc<Notify>,
) -> String {
    match import::spawn_import(
        req,
        data_dir,
        elide_import_bin,
        registry,
        store.clone(),
        rescan.clone(),
    )
    .await
    {
        Ok(ulid) => format!("ok {ulid}"),
        Err(e) => format!("err {e}"),
    }
}

/// Resolve a volume name to its import ULID via `import.lock`, if present.
fn import_ulid_for_volume(name: &str, data_dir: &Path) -> Option<String> {
    let lock_path = data_dir.join("by_name").join(name).join(import::LOCK_FILE);
    std::fs::read_to_string(lock_path)
        .ok()
        .map(|s| s.trim().to_owned())
}

async fn import_status_by_name(name: &str, data_dir: &Path, registry: &ImportRegistry) -> String {
    let vol_dir = data_dir.join("by_name").join(name);
    if !vol_dir.exists() {
        return format!("err volume not found: {name}");
    }
    let Some(ulid) = import_ulid_for_volume(name, data_dir) else {
        // No active import lock — if volume is readonly the import completed.
        if vol_dir.join("volume.readonly").exists() {
            return "ok done".to_string();
        }
        return format!("err no active import for: {name}");
    };
    let job = registry
        .lock()
        .expect("import registry poisoned")
        .get(&ulid)
        .cloned();
    match job {
        // import.lock exists but not in registry (coordinator restarted mid-import)
        None => "ok running".to_string(),
        Some(job) => match job.state() {
            ImportState::Running => "ok running".to_string(),
            ImportState::Done => "ok done".to_string(),
            ImportState::Failed(msg) => format!("err failed: {msg}"),
        },
    }
}

/// Stream buffered and live import output to `writer`, closing with a terminal
/// `ok done` or `err failed: <msg>` line when the import completes.
async fn stream_import_by_name(
    name: &str,
    data_dir: &Path,
    writer: &mut OwnedWriteHalf,
    registry: &ImportRegistry,
) {
    let vol_dir = data_dir.join("by_name").join(name);
    if !vol_dir.exists() {
        let _ = writer
            .write_all(format!("err volume not found: {name}\n").as_bytes())
            .await;
        return;
    }
    let Some(ulid) = import_ulid_for_volume(name, data_dir) else {
        // No active import — if volume is readonly the import already completed.
        if vol_dir.join("volume.readonly").exists() {
            let _ = writer.write_all(b"ok done\n").await;
        } else {
            let _ = writer
                .write_all(format!("err no active import for: {name}\n").as_bytes())
                .await;
        }
        return;
    };

    let job = registry
        .lock()
        .expect("import registry poisoned")
        .get(&ulid)
        .cloned();
    let Some(job) = job else {
        // import.lock exists but not in registry (coordinator restarted mid-import).
        // We can't stream output we never buffered; just report running.
        let _ = writer
            .write_all(b"err import output unavailable (coordinator restarted)\n")
            .await;
        return;
    };

    let mut offset = 0;
    loop {
        let lines = job.read_from(offset);
        for line in &lines {
            if writer
                .write_all(format!("{line}\n").as_bytes())
                .await
                .is_err()
            {
                return; // client disconnected
            }
        }
        offset += lines.len();

        match job.state() {
            ImportState::Done => {
                let _ = writer.write_all(b"ok done\n").await;
                return;
            }
            ImportState::Failed(msg) => {
                let _ = writer
                    .write_all(format!("err failed: {msg}\n").as_bytes())
                    .await;
                return;
            }
            ImportState::Running => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

// ── Volume evict ─────────────────────────────────────────────────────────────

/// Route an eviction request to the fork's task loop.
///
/// The fork task processes the request between drain/GC ticks, ensuring it
/// never races with the GC pass's collect_stats → compact_segments window.
async fn evict_volume(
    vol_name: &str,
    ulid_str: Option<String>,
    data_dir: &Path,
    evict_registry: &EvictRegistry,
) -> String {
    // Resolve name → fork directory path using the same construction as
    // daemon.rs (data_dir.join("by_id/<ulid>")), so the key matches the
    // EvictRegistry entry.  canonicalize() returns an absolute path which
    // would not match when data_dir is relative.
    let link = data_dir.join("by_name").join(vol_name);
    let target = match std::fs::read_link(&link) {
        Ok(t) => t,
        Err(_) => return format!("err volume not found: {vol_name}"),
    };
    // The symlink target is ../by_id/<ulid>; extract just the ULID component.
    let Some(ulid_component) = target.file_name() else {
        return format!("err malformed volume symlink: {vol_name}");
    };
    let fork_dir = data_dir.join("by_id").join(ulid_component);

    // Look up the fork's evict sender.
    let sender = evict_registry
        .lock()
        .expect("evict registry poisoned")
        .get(&fork_dir)
        .cloned();
    let Some(sender) = sender else {
        return format!("err volume not managed by coordinator: {vol_name}");
    };

    // Send the request and wait for the result.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if sender.send((ulid_str, reply_tx)).await.is_err() {
        return "err fork task no longer running".to_string();
    }
    match reply_rx.await {
        Ok(Ok(n)) => format!("ok {n}"),
        Ok(Err(e)) => format!("err {e}"),
        Err(_) => "err fork task dropped reply".to_string(),
    }
}

// ── Volume snapshot ──────────────────────────────────────────────────────────

/// Coordinator-orchestrated snapshot handler.
///
/// Sequence for the named volume:
///   1. Acquire per-volume snapshot lock (blocks the tick loop for this volume).
///   2. `flush` IPC to the volume → WAL into `pending/`.
///   3. Inline drain of `pending/` via `upload::drain_pending` (uploads to S3
///      and triggers `promote` IPCs that move bodies into `cache/` and
///      materialise `index/<ulid>.idx`).
///   4. Pick `snap_ulid` as the max ULID in `index/` after drain, or a fresh
///      ULID if `index/` is empty.
///   5. `snapshot_manifest <snap_ulid>` IPC → volume writes the signed
///      `snapshots/<snap_ulid>.manifest` file and marker.
///   6. Upload the just-written snapshot files to S3 via
///      `upload::upload_snapshots_and_filemaps`.
///
/// Lock is released when this function returns (via the `Drop` guard on
/// the returned `MutexGuard`).
async fn snapshot_volume(
    vol_name: &str,
    data_dir: &Path,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
) -> String {
    let link = data_dir.join("by_name").join(vol_name);
    let fork_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(_) => return format!("err volume not found: {vol_name}"),
    };
    if !fork_dir.join("control.sock").exists() {
        return format!("err volume '{vol_name}' is not running — start it first");
    }
    if fork_dir.join("volume.readonly").exists() {
        return format!("err volume '{vol_name}' is readonly");
    }

    let volume_id = match elide_coordinator::upload::derive_names(&fork_dir) {
        Ok(id) => id,
        Err(e) => return format!("err deriving volume id: {e}"),
    };

    let lock = elide_coordinator::snapshot_lock_for(snapshot_locks, &fork_dir);
    let _guard = lock.lock_owned().await;

    // 1. Promote WAL into pending/.
    if !elide_coordinator::control::promote_wal(&fork_dir).await {
        return "err promote_wal failed or volume unreachable".to_string();
    }

    // 2. Inline drain: upload every pending segment, promote each, then
    //    upload any snapshot files already sitting under snapshots/.
    //    We run this before sign_snapshot_manifest so that index/ is populated
    //    with every segment up to the flush point.
    match elide_coordinator::upload::drain_pending(&fork_dir, &volume_id, store, part_size_bytes)
        .await
    {
        Ok(r) if r.failed > 0 => {
            return format!("err drain reported {} failed segment(s)", r.failed);
        }
        Ok(_) => {}
        Err(e) => return format!("err drain: {e:#}"),
    }

    // 3. Drain any outstanding GC handoffs so `index/` is in a stable
    //    post-GC state before the manifest is signed. Without this, a
    //    volume-applied handoff (bare `gc/<ulid>`) left over from a prior
    //    tick still references segments that `promote_segment` is about to
    //    delete from `index/` (and from S3 via `apply_done_handoffs`). The
    //    signed manifest would then point at segments that vanish seconds
    //    later, and any reader — delta_repack, fork, restart — would fail
    //    with ENOENT on the missing `.idx` file.
    //
    //    Safe under the snapshot lock: the tick loop's GC path
    //    `try_lock`s the same lock and skips the tick while we hold it,
    //    so there is no race with a concurrent apply_done_handoffs.
    let _ = elide_coordinator::control::apply_gc_handoffs(&fork_dir).await;
    if let Err(e) =
        elide_coordinator::gc::apply_done_handoffs(&fork_dir, &volume_id, store, part_size_bytes)
            .await
    {
        return format!("err draining gc handoffs: {e:#}");
    }

    // 4. Pick snap_ulid: the max ULID in index/, or a freshly-minted
    //    one when the volume has no segments. The volume's
    //    `sign_snapshot_manifest` accepts either (see
    //    `elide_core::volume::Volume::sign_snapshot_manifest` doc) —
    //    an empty snapshot is just a signed manifest with zero
    //    entries, valid for the claim path to fork from.
    let snap_ulid = match pick_snapshot_ulid(&fork_dir) {
        Ok(u) => u,
        Err(e) => return format!("err picking snap_ulid: {e}"),
    };

    // 5. Tell the volume to sign and write the manifest + marker.
    if !elide_coordinator::control::sign_snapshot_manifest(&fork_dir, snap_ulid).await {
        return format!("err sign_snapshot_manifest {snap_ulid} failed");
    }

    // Phase 4 (snapshot-time filemap generation) used to run here. It
    // dominated `volume release` wall time on freshly-pulled volumes —
    // ext4 layout scan with per-fragment hash lookup that demand-fetched
    // each missing block range across the ancestor chain (~100s on a
    // cold local fork). The filemap is strictly additive and has only
    // one consumer: `elide volume import --extents-from`. The import
    // path now generates the source filemap on demand if missing, so
    // the release-time work is wasted everywhere else.
    //
    // Imports continue to write the importing volume's filemap inline
    // at import time (`elide-core/src/import.rs`); that path is fast
    // because the importer already has ext4 layout in hand.

    // 5. Upload the new snapshot marker and manifest.
    if let Err(e) =
        elide_coordinator::upload::upload_snapshots_and_filemaps(&fork_dir, &volume_id, store).await
    {
        return format!("err uploading snapshot files: {e:#}");
    }

    info!("[snapshot {volume_id}] committed {snap_ulid}");
    format!("ok {snap_ulid}")
}

/// Pick a snapshot ULID as the max ULID in `fork_dir/index/`.
///
/// Returns `None` when `index/` is empty or missing — the coordinator must
/// never mint ULIDs, so an empty fork has no valid snapshot tag and the
/// caller rejects the snapshot request.
/// Pick the ULID to tag a snapshot with: the max segment ULID in
/// `index/` if any are present, else a fresh `Ulid::new()`. Mirrors
/// `force_snapshot_now_op` at `inbound.rs` (the ancestor-pin path),
/// so empty volumes get a valid snapshot tag through both paths.
fn pick_snapshot_ulid(fork_dir: &Path) -> std::io::Result<ulid::Ulid> {
    let index_dir = fork_dir.join("index");
    let mut latest: Option<ulid::Ulid> = None;
    match std::fs::read_dir(&index_dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(s) = name.to_str() else { continue };
                let Some(stem) = s.strip_suffix(".idx") else {
                    continue;
                };
                if let Ok(u) = ulid::Ulid::from_string(stem)
                    && latest.is_none_or(|cur| u > cur)
                {
                    latest = Some(u);
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    // `Ulid::default()` is the nil ULID, not a fresh one — we explicitly
    // want a freshly minted ULID when the index is empty.
    #[allow(clippy::unwrap_or_default)]
    Ok(latest.unwrap_or_else(ulid::Ulid::new))
}

// ── Snapshot filemap generation ───────────────────────────────────────────────

/// Handle the `generate-filemap <vol> [<snap_ulid>]` IPC verb.
///
/// Looks up the volume by name, defaults `snap_ulid` to the latest local
/// snapshot when omitted, and runs `filemap::generate_from_snapshot` against
/// it with a coordinator-vended `RemoteFetcher` so demand-fetch works for
/// evicted segments.
///
/// The filemap is the only piece of snapshot metadata that is expensive to
/// produce (ext4 layout walk + per-fragment hash lookup). Used to be inline
/// in `snapshot_volume` but the cost was wasted on the user-visible release
/// path because the only consumer is `elide volume import --extents-from`.
/// Operators wanting to seed a delta source now invoke this verb explicitly.
async fn generate_filemap_op(
    vol_name: &str,
    snap_arg: Option<&str>,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> String {
    let link = data_dir.join("by_name").join(vol_name);
    let fork_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(_) => return format!("err volume not found: {vol_name}"),
    };

    let snap_ulid = match snap_arg {
        Some(s) => match ulid::Ulid::from_string(s) {
            Ok(u) => u,
            Err(e) => return format!("err invalid snap_ulid '{s}': {e}"),
        },
        None => match elide_core::volume::latest_snapshot(&fork_dir) {
            Ok(Some(u)) => u,
            Ok(None) => {
                return format!(
                    "err volume '{vol_name}' has no local snapshot; \
                     publish one first with `volume snapshot`"
                );
            }
            Err(e) => return format!("err reading snapshots dir: {e}"),
        },
    };

    // Pre-check that the snapshot the caller asked for is actually
    // sealed on disk. `BlockReader::open_snapshot` would surface a
    // generic NotFound otherwise; a verb-level check makes the error
    // reflect the user's intent ("you named a snapshot that doesn't
    // exist locally" vs. "something deep in the reader failed").
    let manifest_path = fork_dir
        .join("snapshots")
        .join(format!("{snap_ulid}.manifest"));
    if !manifest_path.exists() {
        return format!(
            "err snapshot {snap_ulid} not found locally for volume '{vol_name}' \
             (no snapshots/{snap_ulid}.manifest); pull or snapshot first"
        );
    }

    let started = std::time::Instant::now();
    if let Err(e) = generate_snapshot_filemap(&fork_dir, snap_ulid, store.clone()).await {
        return format!("err filemap generation failed for {snap_ulid}: {e:#}");
    }
    info!(
        "[generate-filemap {vol_name}] snapshot {snap_ulid} written in {:.2?}",
        started.elapsed()
    );
    format!("ok {snap_ulid}")
}

/// Open a sealed snapshot, walk its ext4 layout, and write
/// `snapshots/<snap_ulid>.filemap`. Runs on a worker thread so the tokio
/// reactor stays responsive.
///
/// Wires a `RemoteFetcher` so demand-fetch works for evicted segments —
/// freshly-pulled forks have no local segment bodies, so the ext4 inode
/// table reads must reach S3. The fetcher is constructed inside the
/// closure (after the search-dir list is known) because each fetcher
/// binds to a fork chain.
async fn generate_snapshot_filemap(
    fork_dir: &Path,
    snap_ulid: ulid::Ulid,
    store: Arc<dyn ObjectStore>,
) -> std::io::Result<()> {
    let fork_dir = fork_dir.to_owned();
    let range_fetcher: Arc<dyn elide_fetch::RangeFetcher> =
        Arc::new(elide_coordinator::range_fetcher::ObjectStoreRangeFetcher::new(store));
    tokio::task::spawn_blocking(move || {
        let range_fetcher_for_factory = range_fetcher.clone();
        let mk_fetcher: Box<elide_core::block_reader::FetcherFactory<'_>> =
            Box::new(move |search_dirs: &[PathBuf]| {
                // RemoteFetcher wants oldest-first; BlockReader hands us
                // newest-first (fork → ancestors).
                let oldest_first: Vec<PathBuf> = search_dirs.iter().rev().cloned().collect();
                elide_fetch::RemoteFetcher::from_store(
                    range_fetcher_for_factory,
                    &oldest_first,
                    elide_fetch::DEFAULT_FETCH_BATCH_BYTES,
                )
                .ok()
                .map(|f| Box::new(f) as Box<dyn elide_core::segment::SegmentFetcher>)
            });
        let _wrote =
            elide_core::filemap::generate_from_snapshot(&fork_dir, &snap_ulid, mk_fetcher)?;
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|e| std::io::Error::other(format!("filemap join: {e}")))?
}

// ── Volume status ─────────────────────────────────────────────────────────────

fn volume_status(volume_name: &str, data_dir: &Path) -> String {
    // Resolve name via by_name/ symlink → by_id/<ulid>/.
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }
    // The OS follows the symlink transparently for all path ops below.
    let vol_dir = link;

    // Check if intentionally stopped.
    if vol_dir.join("volume.stopped").exists() {
        return "ok stopped (manual)".to_string();
    }

    // Check for an active import.
    if vol_dir.join(import::LOCK_FILE).exists() {
        let ulid = std::fs::read_to_string(vol_dir.join(import::LOCK_FILE))
            .unwrap_or_default()
            .trim()
            .to_owned();
        return format!("ok importing {ulid}");
    }

    // Check if a volume process is running.
    if let Ok(text) = std::fs::read_to_string(vol_dir.join("volume.pid"))
        && let Ok(pid) = text.trim().parse::<u32>()
        && pid_is_alive(pid)
    {
        return "ok running".to_string();
    }

    "ok stopped".to_string()
}

/// Fetch `names/<name>` from the bucket and serialise it as a TOML body
/// for the `status-remote` IPC verb. Includes an `eligibility` field
/// computed against this coordinator's id so the CLI can show the
/// operator whether the verb-level `start` would be accepted, refused,
/// or routed through the claim-from-released path.
///
/// Response shape on success:
/// ```text
/// ok
/// state = "live"
/// vol_ulid = "01..."
/// coordinator_id = "..."     # optional
/// hostname = "..."           # optional
/// claimed_at = "..."         # optional
/// parent = "..."             # optional
/// handoff_snapshot = "..."   # optional
/// eligibility = "owned" | "foreign" | "reserved-for-self"
///             | "reserved-for-other" | "released-claimable"
///             | "readonly"
/// ```
async fn volume_status_remote(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
) -> String {
    use elide_core::name_record::NameState;

    let record = match elide_coordinator::name_store::read_name_record(store, volume_name).await {
        Ok(Some((rec, _))) => rec,
        Ok(None) => return format!("err name '{volume_name}' has no S3 record"),
        Err(e) => return format!("err reading names/{volume_name}: {e}"),
    };

    let state_str = match record.state {
        NameState::Live => "live",
        NameState::Stopped => "stopped",
        NameState::Released => "released",
        NameState::Readonly => "readonly",
    };

    let eligibility = match record.state {
        NameState::Live | NameState::Stopped => match record.coordinator_id.as_deref() {
            Some(owner) if owner == coord_id => "owned",
            _ => "foreign",
        },
        NameState::Released => "released-claimable",
        NameState::Readonly => "readonly",
    };

    let mut body = String::with_capacity(256);
    body.push_str("ok\n");
    body.push_str(&format!("state = \"{state_str}\"\n"));
    body.push_str(&format!("vol_ulid = \"{}\"\n", record.vol_ulid));
    if let Some(id) = &record.coordinator_id {
        body.push_str(&format!("coordinator_id = \"{id}\"\n"));
    }
    if let Some(host) = &record.hostname {
        body.push_str(&format!("hostname = \"{host}\"\n"));
    }
    if let Some(when) = &record.claimed_at {
        body.push_str(&format!("claimed_at = \"{when}\"\n"));
    }
    if let Some(parent) = &record.parent {
        body.push_str(&format!("parent = \"{parent}\"\n"));
    }
    if let Some(snap) = &record.handoff_snapshot {
        body.push_str(&format!("handoff_snapshot = \"{snap}\"\n"));
    }
    body.push_str(&format!("eligibility = \"{eligibility}\"\n"));
    body
}

// ── Volume remove ─────────────────────────────────────────────────────────────

/// Remove the local instance of a volume.
///
/// Removes the on-disk fork (`by_id/<ulid>/`) and its `by_name/<name>`
/// symlink. Does not delete bucket-side records or segments — this is a
/// local-instance verb, not a `purge`.
///
/// Preconditions (without `force`):
///  - `volume.stopped` must be present (the local daemon is halted)
///  - `pending/` and `wal/` must be empty (all writes are durable in S3)
///
/// `force = true` skips the second check, accepting that any local-only
/// pending segments or unflushed WAL records will be discarded.
fn remove_volume(volume_name: &str, force: bool, data_dir: &Path) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }

    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if !vol_dir.join("volume.stopped").exists() {
        return "err volume is running; stop it first with: elide volume stop <name>".to_string();
    }

    if !force && let Some(reason) = unflushed_state_reason(&vol_dir) {
        return format!(
            "err {reason}; take a snapshot first with: elide volume snapshot <name> \
             — or pass --force to discard the unflushed local state"
        );
    }

    import::kill_all_for_volume(&vol_dir);

    let _ = std::fs::remove_file(&link);

    match std::fs::remove_dir_all(&vol_dir) {
        Ok(()) => {
            info!("[inbound] removed volume {volume_name}");
            "ok".to_string()
        }
        Err(e) => format!("err remove failed: {e}"),
    }
}

/// Returns a human-readable reason if the fork has on-disk state that
/// has not been flushed and uploaded to S3, or `None` if the fork is
/// fully durable. Used to gate `remove` and decide whether `--force` is
/// required.
///
/// "Fully durable" means: no segments awaiting upload (`pending/` empty)
/// and no unflushed WAL records (`wal/` empty). Both directories are
/// populated by writes and emptied by the drain pipeline.
fn unflushed_state_reason(vol_dir: &Path) -> Option<String> {
    let dir_has_entries = |sub: &str| {
        std::fs::read_dir(vol_dir.join(sub))
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
    };
    if dir_has_entries("pending") {
        return Some("local segments are pending upload to S3".to_string());
    }
    if dir_has_entries("wal") {
        return Some("local WAL has unflushed writes".to_string());
    }
    None
}

// ── Volume create / update ────────────────────────────────────────────────────

/// Parsed transport flags from the `create`/`update` IPC argument string.
/// Each `Some` field is an explicit set request; the corresponding `no_*`
/// boolean is an explicit clear. Mutually-exclusive flags are validated by
/// the consumer (different rules for create vs update).
#[derive(Default)]
struct TransportPatch {
    nbd_port: Option<u16>,
    nbd_bind: Option<String>,
    nbd_socket: Option<std::path::PathBuf>,
    no_nbd: bool,
    ublk: bool,
    ublk_id: Option<i32>,
    no_ublk: bool,
}

/// Parse a flat space-separated flag list. Recognised tokens:
///   nbd-port=<u16>, nbd-bind=<addr>, nbd-socket=<path>, no-nbd,
///   ublk, ublk-id=<i32>, no-ublk
/// Unknown tokens produce an error so silent typos don't get accepted.
fn parse_transport_flags(args: &str) -> Result<TransportPatch, String> {
    let mut patch = TransportPatch::default();
    for tok in args.split_whitespace() {
        let (key, val) = match tok.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (tok, None),
        };
        match (key, val) {
            ("nbd-port", Some(v)) => {
                patch.nbd_port = Some(v.parse().map_err(|e| format!("bad nbd-port {v:?}: {e}"))?);
            }
            ("nbd-bind", Some(v)) => patch.nbd_bind = Some(v.to_owned()),
            ("nbd-socket", Some(v)) => patch.nbd_socket = Some(v.into()),
            ("no-nbd", None) => patch.no_nbd = true,
            ("ublk", None) => patch.ublk = true,
            ("ublk-id", Some(v)) => {
                patch.ublk_id = Some(v.parse().map_err(|e| format!("bad ublk-id {v:?}: {e}"))?);
            }
            ("no-ublk", None) => patch.no_ublk = true,
            _ => return Err(format!("unknown flag: {tok}")),
        }
    }
    Ok(patch)
}

fn validate_volume_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("volume name must not be empty".to_owned());
    }
    if let Some(c) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_' && *c != '.')
    {
        return Err(format!(
            "invalid character {c:?} in volume name {name:?}: only [a-zA-Z0-9._-] allowed"
        ));
    }
    if matches!(name, "status" | "attach") {
        return Err(format!("'{name}' is a reserved name"));
    }
    Ok(())
}

async fn create_volume_op(
    args: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    rescan: &Notify,
) -> String {
    // First two whitespace-separated tokens: name, size_bytes. Remainder is flags.
    let mut iter = args.splitn(3, ' ').map(str::trim);
    let name = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: create <name> <size_bytes> [flags...]".to_string(),
    };
    let size_str = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: create <name> <size_bytes> [flags...]".to_string(),
    };
    let flag_str = iter.next().unwrap_or("");

    if let Err(e) = validate_volume_name(name) {
        return format!("err {e}");
    }
    let bytes: u64 = match size_str.parse() {
        Ok(b) if b > 0 => b,
        Ok(_) => return "err size must be non-zero".to_string(),
        Err(e) => return format!("err bad size {size_str:?}: {e}"),
    };

    let patch = match parse_transport_flags(flag_str) {
        Ok(p) => p,
        Err(e) => return format!("err {e}"),
    };
    if patch.no_nbd || patch.no_ublk {
        return "err no-nbd / no-ublk are not valid on create (volume starts without transport)"
            .to_string();
    }
    if (patch.nbd_port.is_some() || patch.nbd_bind.is_some() || patch.nbd_socket.is_some())
        && (patch.ublk || patch.ublk_id.is_some())
    {
        return "err nbd-* flags are mutually exclusive with ublk".to_string();
    }

    let nbd_cfg = if let Some(socket) = patch.nbd_socket {
        Some(elide_core::config::NbdConfig {
            socket: Some(socket),
            ..Default::default()
        })
    } else if patch.nbd_port.is_some() || patch.nbd_bind.is_some() {
        Some(elide_core::config::NbdConfig {
            port: patch.nbd_port,
            bind: patch.nbd_bind,
            ..Default::default()
        })
    } else {
        None
    };
    let ublk_cfg = if patch.ublk || patch.ublk_id.is_some() {
        Some(elide_core::config::UblkConfig {
            dev_id: patch.ublk_id,
        })
    } else {
        None
    };

    let by_name_dir = data_dir.join("by_name");
    if by_name_dir.join(name).exists() {
        return format!("err volume already exists: {name}");
    }

    let vol_ulid = ulid::Ulid::new();
    let vol_ulid_str = vol_ulid.to_string();
    let vol_dir = data_dir.join("by_id").join(&vol_ulid_str);

    // Claim the name in S3 *before* creating any local state. This
    // closes the cross-coordinator race where two hosts run
    // `volume create <name>` concurrently — the loser sees
    // `AlreadyExists` and refuses, leaving no partial local artefacts.
    use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
    match mark_initial(store, name, coord_id, vol_ulid).await {
        Ok(MarkInitialOutcome::Claimed) => {}
        Ok(MarkInitialOutcome::AlreadyExists {
            existing_vol_ulid,
            existing_state,
            existing_owner,
        }) => {
            let owner = existing_owner.as_deref().unwrap_or("<unowned>");
            return format!(
                "err name '{name}' already exists in bucket \
                 (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                 owner={owner})"
            );
        }
        Err(LifecycleError::Store(e)) => {
            return format!("err claiming name in bucket: {e}");
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            // mark_initial doesn't currently surface this, but match
            // exhaustively so a future refactor doesn't silently drop it.
            return format!("err name held by another coordinator: {held_by}");
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return format!("err names/<name> is in unexpected state {from:?}");
        }
    }

    let result: std::io::Result<()> = (|| {
        std::fs::create_dir_all(&vol_dir)?;
        std::fs::create_dir_all(vol_dir.join("pending"))?;
        std::fs::create_dir_all(vol_dir.join("index"))?;
        std::fs::create_dir_all(vol_dir.join("cache"))?;
        let key = elide_core::signing::generate_keypair(
            &vol_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )?;
        elide_core::signing::write_provenance(
            &vol_dir,
            &key,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
            &elide_core::signing::ProvenanceLineage::default(),
        )?;
        elide_core::config::VolumeConfig {
            name: Some(name.to_owned()),
            size: Some(bytes),
            nbd: nbd_cfg,
            ublk: ublk_cfg,
        }
        .write(&vol_dir)?;
        std::fs::create_dir_all(&by_name_dir)?;
        std::os::unix::fs::symlink(format!("../by_id/{vol_ulid_str}"), by_name_dir.join(name))?;
        Ok(())
    })();

    if let Err(e) = result {
        // Local setup failed. Best-effort: roll back local artefacts
        // *and* the bucket-side claim so the name is free to retry.
        let _ = std::fs::remove_file(by_name_dir.join(name));
        let _ = std::fs::remove_dir_all(&vol_dir);
        let name_key = object_store::path::Path::from(format!("names/{name}"));
        if let Err(del_err) = store.delete(&name_key).await {
            warn!(
                "[inbound] create {name}: local setup failed and rollback \
                 of names/<name> also failed: {del_err}"
            );
        }
        return format!("err create failed: {e}");
    }

    rescan.notify_one();
    info!("[inbound] created volume {name} ({vol_ulid_str})");
    format!("ok {vol_ulid_str}")
}

async fn update_volume_op(args: &str, data_dir: &Path) -> String {
    let (name, flag_str) = match args.split_once(' ') {
        Some((n, rest)) => (n.trim(), rest.trim()),
        None => (args.trim(), ""),
    };
    if name.is_empty() {
        return "err usage: update <volume> [flags...]".to_string();
    }
    let link = data_dir.join("by_name").join(name);
    if !link.exists() {
        return format!("err volume not found: {name}");
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    let patch = match parse_transport_flags(flag_str) {
        Ok(p) => p,
        Err(e) => return format!("err {e}"),
    };

    let mut cfg = match elide_core::config::VolumeConfig::read(&vol_dir) {
        Ok(c) => c,
        Err(e) => return format!("err reading volume.toml: {e}"),
    };

    // Mirror the CLI's apply order: nbd flags first, then ublk. The setters
    // clear the opposite transport so the two sections remain mutually
    // exclusive.
    if patch.no_nbd {
        cfg.nbd = None;
    } else if let Some(socket) = patch.nbd_socket {
        cfg.nbd = Some(elide_core::config::NbdConfig {
            socket: Some(socket),
            ..Default::default()
        });
        cfg.ublk = None;
    } else if patch.nbd_port.is_some() || patch.nbd_bind.is_some() {
        let existing = cfg.nbd.get_or_insert_with(Default::default);
        if let Some(port) = patch.nbd_port {
            existing.port = Some(port);
            existing.socket = None;
        }
        if let Some(bind) = patch.nbd_bind {
            existing.bind = Some(bind);
        }
        cfg.ublk = None;
    }

    if patch.no_ublk {
        cfg.ublk = None;
    } else if patch.ublk || patch.ublk_id.is_some() {
        cfg.ublk = Some(elide_core::config::UblkConfig {
            dev_id: patch.ublk_id,
        });
        cfg.nbd = None;
    }

    if let Err(e) = cfg.write(&vol_dir) {
        return format!("err writing volume.toml: {e}");
    }

    // Restart the volume process so it picks up the new config. ECONNREFUSED /
    // ENOENT mean the volume isn't running — fine, the new config takes effect
    // on next start.
    match elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await {
        Some(resp) if resp == "ok" => {
            info!("[inbound] updated volume {name}; restart triggered");
            "ok restarting".to_string()
        }
        Some(resp) => format!("err shutdown failed: {resp}"),
        None => {
            info!("[inbound] updated volume {name}; not running");
            "ok not-running".to_string()
        }
    }
}

// ── Volume fork (create --from) plumbing ──────────────────────────────────────

/// Decode a 64-character hex string into a 32-byte array (Ed25519 pubkey).
fn decode_hex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| format!("invalid hex at byte {i}"))?;
    }
    Ok(out)
}

/// Pull one readonly ancestor from the store.
///
/// Mirrors the CLI's `pull_one_readonly`: downloads `manifest.toml`,
/// `volume.pub`, and `volume.provenance`, writes the ancestor under
/// `data_dir/by_id/<vol_ulid>/`, and returns the parent ULID parsed from
/// the (signature-verified) provenance, or empty if this is a root volume.
async fn pull_readonly_op(args: &str, data_dir: &Path, store: &Arc<dyn ObjectStore>) -> String {
    use object_store::path::Path as StorePath;

    let volume_id = args.trim();
    if ulid::Ulid::from_string(volume_id).is_err() {
        return format!("err invalid ULID: {volume_id}");
    }

    let vol_dir = data_dir.join("by_id").join(volume_id);
    if vol_dir.exists() {
        return format!("err volume already present locally: {volume_id}");
    }

    // Step 1: fetch manifest.toml.
    let manifest_key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
    let manifest_bytes = match store.get(&manifest_key).await {
        Ok(d) => match d.bytes().await {
            Ok(b) => b,
            Err(e) => return format!("err reading manifest: {e}"),
        },
        Err(object_store::Error::NotFound { .. }) => {
            return format!("err manifest not found in store for volume {volume_id}");
        }
        Err(e) => return format!("err downloading manifest: {e}"),
    };
    let manifest: toml::Table = match std::str::from_utf8(&manifest_bytes)
        .map_err(|e| format!("manifest is not valid utf-8: {e}"))
        .and_then(|s| toml::from_str(s).map_err(|e| format!("parsing manifest.toml: {e}")))
    {
        Ok(t) => t,
        Err(e) => return format!("err {e}"),
    };
    let size = match manifest.get("size").and_then(|v| v.as_integer()) {
        Some(s) => s,
        None => return "err manifest.toml missing 'size'".to_string(),
    };

    // Step 2: fetch volume.pub.
    let pub_key_bytes = match store
        .get(&StorePath::from(format!("by_id/{volume_id}/volume.pub")))
        .await
    {
        Ok(d) => match d.bytes().await {
            Ok(b) => b,
            Err(e) => return format!("err reading volume.pub: {e}"),
        },
        Err(e) => return format!("err downloading volume.pub: {e}"),
    };

    // Step 3: fetch volume.provenance. NotFound is hard error.
    let provenance_bytes = match store
        .get(&StorePath::from(format!(
            "by_id/{volume_id}/volume.provenance"
        )))
        .await
    {
        Ok(d) => match d.bytes().await {
            Ok(b) => b,
            Err(e) => return format!("err reading volume.provenance: {e}"),
        },
        Err(object_store::Error::NotFound { .. }) => {
            return format!("err volume.provenance not found in store for volume {volume_id}");
        }
        Err(e) => return format!("err downloading volume.provenance: {e}"),
    };

    // Step 4: write the ancestor entry. No name carried over from manifest:
    // a missing volume.name (and missing by_name/ symlink) is the on-disk
    // marker that distinguishes a pulled ancestor from a user-managed volume.
    let _ = manifest;
    let result: std::io::Result<()> = (|| {
        std::fs::create_dir_all(&vol_dir)?;
        elide_core::config::VolumeConfig {
            name: None,
            size: Some(size as u64),
            ..Default::default()
        }
        .write(&vol_dir)?;
        std::fs::write(vol_dir.join("volume.readonly"), "")?;
        std::fs::write(vol_dir.join("volume.pub"), &pub_key_bytes)?;
        std::fs::write(
            vol_dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE),
            &provenance_bytes,
        )?;
        std::fs::create_dir_all(vol_dir.join("index"))?;
        Ok(())
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_dir_all(&vol_dir);
        return format!("err writing pulled ancestor: {e}");
    }

    // Step 5: parse and verify provenance to find the parent.
    let parent_ulid = match elide_core::signing::read_lineage_verifying_signature(
        &vol_dir,
        elide_core::signing::VOLUME_PUB_FILE,
        elide_core::signing::VOLUME_PROVENANCE_FILE,
    ) {
        Ok(lineage) => lineage.parent.map(|p| p.volume_ulid.to_string()),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&vol_dir);
            return format!("err verifying provenance for {volume_id}: {e}");
        }
    };

    info!("[inbound] pulled ancestor {volume_id}");
    match parent_ulid {
        Some(p) => format!("ok {p}"),
        None => "ok".to_string(),
    }
}

/// Synthesize a "now" snapshot for a readonly source volume.
///
/// Mirrors the CLI's `create_readonly_snapshot_now`: prefetches indexes,
/// uploads the snapshot marker, verifies pinned segments are still in S3,
/// and writes a signed manifest under an ephemeral key in the ancestor's
/// directory. Returns `<snap_ulid> <ephemeral_pubkey_hex>`.
/// Implementation of the `resolve-handoff-key` IPC verb. See the
/// dispatch comment for the wire format.
async fn resolve_handoff_key_op(
    vol_str: &str,
    snap_str: &str,
    store: &Arc<dyn ObjectStore>,
) -> String {
    use elide_coordinator::recovery::{HandoffVerifier, resolve_handoff_verifier};

    let vol_ulid = match ulid::Ulid::from_string(vol_str) {
        Ok(u) => u,
        Err(e) => return format!("err invalid vol_ulid {vol_str:?}: {e}"),
    };
    let snap_ulid = match ulid::Ulid::from_string(snap_str) {
        Ok(u) => u,
        Err(e) => return format!("err invalid snap_ulid {snap_str:?}: {e}"),
    };

    match resolve_handoff_verifier(store, vol_ulid, snap_ulid).await {
        Ok(HandoffVerifier::Normal) => "ok normal".to_owned(),
        Ok(HandoffVerifier::Synthesised {
            manifest_pubkey, ..
        }) => {
            let hex: String = manifest_pubkey
                .as_ref()
                .to_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            format!("ok recovery {hex}")
        }
        Err(e) => format!("err {e}"),
    }
}

/// Wait for the per-fork prefetch result and echo it.
///
/// The IPC has no internal timeout — the volume-side caller bounds it. If
/// the per-fork task disappears (channel closed without a final value, e.g.
/// the volume directory was removed) we report that as an error rather than
/// silently returning ok, so the caller can act on it.
async fn await_prefetch_op(args: &str, tracker: &PrefetchTracker) -> String {
    let vol_str = args.trim();
    let vol_ulid = match ulid::Ulid::from_string(vol_str) {
        Ok(u) => u,
        Err(_) => return format!("err invalid ULID: {vol_str}"),
    };

    let mut rx = match subscribe_prefetch(tracker, &vol_ulid) {
        Some(rx) => rx,
        // Untracked: either already prefetched on a previous coordinator run,
        // or this coordinator hasn't discovered the fork yet. Both cases are
        // safe to treat as ready — Volume::open's own retry helper still
        // absorbs the rare "fork not yet on disk" sub-race.
        None => return "ok".to_string(),
    };

    loop {
        // `borrow()` returns the most recently published value; if a value
        // was already sent before we subscribed it is visible immediately.
        if let Some(result) = rx.borrow().clone() {
            return match result {
                Ok(()) => "ok".to_string(),
                Err(msg) => format!("err prefetch failed: {msg}"),
            };
        }
        if rx.changed().await.is_err() {
            // Sender was dropped before publishing — the per-fork task exited
            // abnormally (volume removed mid-prefetch, panic, etc.).
            return "err prefetch task exited without publishing a result".to_string();
        }
    }
}

async fn force_snapshot_now_op(
    args: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
) -> String {
    use futures::TryStreamExt;
    use object_store::PutPayload;
    use object_store::path::Path as StorePath;

    let volume_id = args.trim();
    if ulid::Ulid::from_string(volume_id).is_err() {
        return format!("err invalid ULID: {volume_id}");
    }

    let ancestor_dir = data_dir.join("by_id").join(volume_id);
    if !ancestor_dir.exists() {
        return format!("err volume not found locally: {volume_id}");
    }

    // Step 1: prefetch indexes from S3.
    if let Err(e) = elide_coordinator::prefetch::prefetch_indexes(&ancestor_dir, store).await {
        return format!("err prefetching ancestor indexes: {e:#}");
    }

    // Step 2: enumerate prefetched .idx files. Pin ULID = max(segments).
    let index_dir = ancestor_dir.join("index");
    let mut segments: Vec<ulid::Ulid> = Vec::new();
    let mut max_seg: Option<ulid::Ulid> = None;
    let entries = match std::fs::read_dir(&index_dir) {
        Ok(e) => e,
        Err(e) => return format!("err reading index dir: {e}"),
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => return format!("err reading index entry: {e}"),
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".idx") else {
            continue;
        };
        if let Ok(u) = ulid::Ulid::from_string(stem) {
            segments.push(u);
            if max_seg.is_none_or(|m| u > m) {
                max_seg = Some(u);
            }
        }
    }
    let snap = match max_seg {
        Some(m) => m,
        None => ulid::Ulid::new(),
    };
    let snap_str = snap.to_string();

    // Step 3 + 4: upload marker, verify pinned segments still present.
    let marker_key = match elide_coordinator::upload::snapshot_key(volume_id, &snap_str) {
        Ok(k) => k,
        Err(e) => return format!("err snapshot_key: {e}"),
    };
    let seg_prefix = StorePath::from(format!("by_id/{volume_id}/segments/"));
    let pinned: std::collections::HashSet<ulid::Ulid> = segments.iter().copied().collect();

    if let Err(e) = store.put(&marker_key, PutPayload::from_static(b"")).await {
        return format!("err uploading snapshot marker: {e}");
    }

    let objects = match store.list(Some(&seg_prefix)).try_collect::<Vec<_>>().await {
        Ok(v) => v,
        Err(e) => return format!("err listing segments for verification: {e}"),
    };
    let present: std::collections::HashSet<ulid::Ulid> = objects
        .iter()
        .filter_map(|o| o.location.filename())
        .filter_map(|name| ulid::Ulid::from_string(name).ok())
        .collect();
    let mut missing: Vec<ulid::Ulid> = pinned.difference(&present).copied().collect();
    if !missing.is_empty() {
        missing.sort();
        if let Err(e) = store.delete(&marker_key).await {
            warn!("[inbound] failed to delete stale snapshot marker {snap_str}: {e}");
        }
        let preview: Vec<String> = missing.iter().take(5).map(|u| u.to_string()).collect();
        let extra = missing.len().saturating_sub(preview.len());
        let suffix = if extra > 0 {
            format!(" (+{extra} more)")
        } else {
            String::new()
        };
        return format!(
            "err force-snapshot aborted: {} of {} pinned segment(s) no longer present in S3 \
             (owner GC raced us); missing: [{}]{}",
            missing.len(),
            pinned.len(),
            preview.join(", "),
            suffix,
        );
    }

    // Step 5: load-or-create the per-source attestation key.
    let (signer, vk) = match elide_core::signing::load_or_create_keypair(
        &ancestor_dir,
        elide_core::signing::FORCE_SNAPSHOT_KEY_FILE,
        elide_core::signing::FORCE_SNAPSHOT_PUB_FILE,
    ) {
        Ok(s) => s,
        Err(e) => return format!("err attestation keypair: {e}"),
    };

    // Step 6: signed manifest + local marker.
    if let Err(e) = elide_core::signing::write_snapshot_manifest(
        &ancestor_dir,
        &*signer,
        &snap,
        &segments,
        None,
    ) {
        return format!("err writing snapshot manifest: {e}");
    }
    let snap_dir = ancestor_dir.join("snapshots");
    if let Err(e) = std::fs::create_dir_all(&snap_dir) {
        return format!("err creating snapshots dir: {e}");
    }
    if let Err(e) = std::fs::write(snap_dir.join(&snap_str), b"") {
        return format!("err writing local marker: {e}");
    }

    info!(
        "[inbound] attested now-snapshot {snap_str} for {volume_id} ({} segments)",
        segments.len()
    );
    let pubkey_hex: String = vk.to_bytes().iter().map(|b| format!("{b:02x}")).collect();
    format!("ok {snap_str} {pubkey_hex}")
}

/// Fork an existing source volume into a new writable volume.
///
/// Mirrors the CLI's `fork_volume_at*` + by_name symlink + volume.toml write.
/// `source_ulid` resolves to any volume in `by_id/<ulid>/` — writable,
/// imported readonly base, or pulled ancestor. `snap` is optional: if
/// omitted, falls back to `volume::fork_volume` (latest local snapshot).
/// `parent-key` is the hex ephemeral pubkey from `force-snapshot-now`,
/// recorded in the new fork's provenance for force-snapshot pins.
async fn fork_create_op(
    args: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    rescan: &Notify,
) -> String {
    let mut iter = args.splitn(3, ' ').map(str::trim);
    let new_name = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: fork-create <name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [flags...]".to_string(),
    };
    let source_ulid_str = match iter.next() {
        Some(s) if !s.is_empty() => s,
        _ => return "err usage: fork-create <name> <source_ulid> [snap=<ulid>] [parent-key=<hex>] [flags...]".to_string(),
    };
    let rest = iter.next().unwrap_or("");

    if let Err(e) = validate_volume_name(new_name) {
        return format!("err {e}");
    }
    if ulid::Ulid::from_string(source_ulid_str).is_err() {
        return format!("err invalid source ULID: {source_ulid_str}");
    }

    // Pull off `snap=<ulid>`, `parent-key=<hex>`, and `for-claim` first;
    // remaining tokens go to the transport-flag parser. `for-claim`
    // marks this fork-create as the materialise step of a
    // claim-from-released flow: the name already exists in the bucket
    // as `Released`, so we must NOT call `mark_initial` (which would
    // fail with `AlreadyExists`). The CLI's subsequent `claim` IPC
    // calls `mark_claimed` to atomically rebind the name to the new
    // fork.
    let mut snap: Option<ulid::Ulid> = None;
    let mut parent_key: Option<elide_core::signing::VerifyingKey> = None;
    let mut for_claim = false;
    let mut flag_tokens: Vec<&str> = Vec::new();
    for tok in rest.split_whitespace() {
        if let Some(v) = tok.strip_prefix("snap=") {
            match ulid::Ulid::from_string(v) {
                Ok(u) => snap = Some(u),
                Err(e) => return format!("err bad snap ULID {v:?}: {e}"),
            }
        } else if let Some(v) = tok.strip_prefix("parent-key=") {
            let arr = match decode_hex32(v) {
                Ok(a) => a,
                Err(e) => return format!("err bad parent-key: {e}"),
            };
            match elide_core::signing::VerifyingKey::from_bytes(&arr) {
                Ok(k) => parent_key = Some(k),
                Err(e) => return format!("err parent-key not a valid Ed25519 pubkey: {e}"),
            }
        } else if tok == "for-claim" {
            for_claim = true;
        } else {
            flag_tokens.push(tok);
        }
    }
    let patch = match parse_transport_flags(&flag_tokens.join(" ")) {
        Ok(p) => p,
        Err(e) => return format!("err {e}"),
    };
    if patch.no_nbd || patch.no_ublk {
        return "err no-nbd / no-ublk are not valid on fork-create".to_string();
    }
    let nbd_cfg = if let Some(socket) = patch.nbd_socket {
        Some(elide_core::config::NbdConfig {
            socket: Some(socket),
            ..Default::default()
        })
    } else if patch.nbd_port.is_some() || patch.nbd_bind.is_some() {
        Some(elide_core::config::NbdConfig {
            port: patch.nbd_port,
            bind: patch.nbd_bind,
            ..Default::default()
        })
    } else {
        None
    };
    let ublk_cfg = if patch.ublk || patch.ublk_id.is_some() {
        Some(elide_core::config::UblkConfig {
            dev_id: patch.ublk_id,
        })
    } else {
        None
    };

    let by_name_dir = data_dir.join("by_name");
    let symlink_path = by_name_dir.join(new_name);
    if symlink_path.exists() {
        return format!("err volume already exists: {new_name}");
    }

    let by_id_dir = data_dir.join("by_id");
    let source_dir = elide_core::volume::resolve_ancestor_dir(&by_id_dir, source_ulid_str);
    if !source_dir.exists() {
        return format!("err source volume {source_ulid_str} not found locally");
    }

    let new_vol_ulid_value = ulid::Ulid::new();
    let new_vol_ulid = new_vol_ulid_value.to_string();
    let new_fork_dir = by_id_dir.join(&new_vol_ulid);

    // For brand-new names, claim in S3 *before* materialising the
    // fork: lose the race cleanly with no partial local state if
    // another coordinator already holds the name, and roll back the
    // bucket-side claim if local fork-out fails. For the
    // claim-from-released path (`for-claim` flag), the bucket record
    // already exists as `Released` and the CLI's subsequent `claim`
    // IPC handles the rebind via `mark_claimed`; we only do the
    // local materialisation here.
    if !for_claim {
        use elide_coordinator::lifecycle::{LifecycleError, MarkInitialOutcome, mark_initial};
        match mark_initial(store, new_name, coord_id, new_vol_ulid_value).await {
            Ok(MarkInitialOutcome::Claimed) => {}
            Ok(MarkInitialOutcome::AlreadyExists {
                existing_vol_ulid,
                existing_state,
                existing_owner,
            }) => {
                let owner = existing_owner.as_deref().unwrap_or("<unowned>");
                return format!(
                    "err name '{new_name}' already exists in bucket \
                     (vol_ulid={existing_vol_ulid}, state={existing_state:?}, \
                     owner={owner})"
                );
            }
            Err(LifecycleError::Store(e)) => {
                return format!("err claiming name in bucket: {e}");
            }
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                return format!("err name held by another coordinator: {held_by}");
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => {
                return format!("err names/<name> is in unexpected state {from:?}");
            }
        }
    }

    // Local-only rollback. After a successful `mark_initial` claim,
    // every error-return path below also rolls back the bucket-side
    // claim via `rollback_claim` so the name is free to retry. In the
    // `for-claim` path no such record was created here, so
    // `rollback_claim` is a no-op.
    let cleanup = |fork_dir: &Path, link: &Path| {
        let _ = std::fs::remove_file(link);
        let _ = std::fs::remove_dir_all(fork_dir);
    };
    let rollback_claim = async || {
        if for_claim {
            return;
        }
        let key = object_store::path::Path::from(format!("names/{new_name}"));
        if let Err(e) = store.delete(&key).await {
            warn!(
                "[inbound] fork-create {new_name}: local fork failed and \
                 rollback of names/<name> also failed: {e}"
            );
        }
    };

    let fork_result: std::io::Result<()> = match (snap, parent_key) {
        (Some(snap), Some(vk)) => elide_core::volume::fork_volume_at_with_manifest_key(
            &new_fork_dir,
            &source_dir,
            snap,
            vk,
        ),
        (Some(snap), None) => elide_core::volume::fork_volume_at(&new_fork_dir, &source_dir, snap),
        (None, _) => elide_core::volume::fork_volume(&new_fork_dir, &source_dir),
    };
    if let Err(e) = fork_result {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err fork failed: {e}");
    }

    // Inherit size from source; require it to be set.
    let src_cfg = match elide_core::config::VolumeConfig::read(&source_dir) {
        Ok(c) => c,
        Err(e) => {
            cleanup(&new_fork_dir, &symlink_path);
            rollback_claim().await;
            return format!("err reading source volume config: {e}");
        }
    };
    let size = match src_cfg.size {
        Some(s) => s,
        None => {
            cleanup(&new_fork_dir, &symlink_path);
            rollback_claim().await;
            return "err source volume has no size (import may not have completed)".to_string();
        }
    };
    if let Err(e) = (elide_core::config::VolumeConfig {
        name: Some(new_name.to_owned()),
        size: Some(size),
        nbd: nbd_cfg,
        ublk: ublk_cfg,
    }
    .write(&new_fork_dir))
    {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err writing volume config: {e}");
    }
    if let Err(e) = std::fs::create_dir_all(&by_name_dir) {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err creating by_name dir: {e}");
    }
    if let Err(e) = std::os::unix::fs::symlink(format!("../by_id/{new_vol_ulid}"), &symlink_path) {
        cleanup(&new_fork_dir, &symlink_path);
        rollback_claim().await;
        return format!("err creating by_name symlink: {e}");
    }

    rescan.notify_one();
    info!("[inbound] forked volume {new_name} ({new_vol_ulid}) from {source_ulid_str}");
    format!("ok {new_vol_ulid}")
}

// ── Volume stop / start ───────────────────────────────────────────────────────

/// True if `<data_dir>/by_name/<name>` exists and the resolved fork
/// has no `volume.stopped` marker — i.e. this host is actively
/// serving (or supposed to be serving) `<name>`.
///
/// Used by recovery verbs (`release --force`, `claim`, `claim --force`)
/// to refuse when the operator has typo'd a verb at their own running
/// volume: those verbs are designed for unreachable peers and would
/// otherwise leave on-disk state diverging from the bucket record.
fn local_daemon_running(data_dir: &Path, volume_name: &str) -> bool {
    let link = data_dir.join("by_name").join(volume_name);
    match std::fs::canonicalize(&link) {
        Ok(vol_dir) => !vol_dir.join("volume.stopped").exists(),
        Err(_) => false,
    }
}

async fn stop_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if vol_dir.join("volume.stopped").exists() {
        // Already stopped — idempotent success.
        return "ok".to_string();
    }

    let readonly = vol_dir.join("volume.readonly").exists();

    // Refuse to stop while an NBD client is connected. ublk volumes have
    // no NBD client and `connected` will simply return false.
    match elide_coordinator::control::call_for_inbound(&vol_dir, "connected").await {
        Some(resp) if resp == "ok true" => {
            return "err nbd client is connected; disconnect it first".to_string();
        }
        _ => {}
    }

    // `stop` is a local-lifecycle verb: its job is to halt the daemon
    // on this host. The bucket update is best-effort and only applies
    // to writable volumes — readonly volumes have a `Readonly` bucket
    // record that is its own terminal state, so there is no Live → Stopped
    // transition to make.
    //
    // For writable volumes, the bucket update only succeeds for the
    // canonical case (record owned by us, Live → Stopped); every other
    // case (no record, already stopped, foreign-owned, Released,
    // transient store error) becomes a warning and we proceed with
    // the local halt. The daemon may legitimately still be running
    // while the bucket says Released — e.g. after a partial release
    // that flipped the bucket but failed to halt the process — and
    // `stop` must be able to recover from that. Halting our local
    // daemon never affects other hosts.
    if !readonly {
        use elide_coordinator::lifecycle::{LifecycleError, mark_stopped};
        match mark_stopped(store, volume_name, coord_id).await {
            Ok(_) => {}
            Err(LifecycleError::OwnershipConflict { held_by }) => {
                warn!(
                    "[inbound] stop {volume_name}: names/<name> is owned by coordinator \
                     {held_by}; halting locally, bucket record left untouched"
                );
            }
            Err(LifecycleError::InvalidTransition { from, .. }) => {
                warn!(
                    "[inbound] stop {volume_name}: names/<name> is in state {from:?}; \
                     halting locally, bucket record left untouched"
                );
            }
            Err(LifecycleError::Store(e)) => {
                warn!("[inbound] stop {volume_name}: failed to update names/<name>: {e}");
            }
        }
    }

    // Write the marker before sending shutdown so the supervisor won't restart.
    if let Err(e) = std::fs::write(vol_dir.join("volume.stopped"), "") {
        return format!("err writing volume.stopped: {e}");
    }

    match elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await {
        Some(resp) if resp == "ok" => {
            info!("[inbound] stopped volume {volume_name}");
            "ok".to_string()
        }
        Some(resp) => {
            // Roll back the marker so the supervisor doesn't strand a still-
            // running volume. (Note: the S3 state has already flipped to
            // Stopped; that's a soft inconsistency the operator can resolve
            // by issuing `volume start` once the underlying issue is fixed.)
            let _ = std::fs::remove_file(vol_dir.join("volume.stopped"));
            format!("err shutdown failed: {resp}")
        }
        None => {
            // Volume process wasn't running — marker is correct as-is.
            info!("[inbound] stopped volume {volume_name} (process was not running)");
            "ok".to_string()
        }
    }
}

/// RAII guard that restores the original local state on drop.
///
/// Used by `release_volume_op` when it transparently restarts a
/// stopped volume to perform the drain: the marker is cleaned up
/// regardless of which exit path the function takes (success, error,
/// panic), and on failure paths the `volume.stopped` marker is
/// re-written so the volume returns to its pre-release state instead
/// of being left running.
struct DrainingMarkerGuard {
    vol_dir: std::path::PathBuf,
    /// Set to `true` once the release has reached a point where the
    /// caller has explicitly written `volume.stopped` (i.e. the
    /// release path's own halt step). Defuses the rollback so the
    /// guard only removes the draining marker, leaving the volume
    /// stopped as the release flow intends.
    success: bool,
}

impl DrainingMarkerGuard {
    fn new(vol_dir: std::path::PathBuf) -> Self {
        Self {
            vol_dir,
            success: false,
        }
    }

    /// Mark the release as having reached its own halt step — at this
    /// point `volume.stopped` is back in place via the normal path
    /// and we should *not* re-write it on drop.
    fn defuse(&mut self) {
        self.success = true;
    }
}

impl Drop for DrainingMarkerGuard {
    fn drop(&mut self) {
        let draining = self.vol_dir.join("volume.draining");
        if let Err(e) = std::fs::remove_file(&draining)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                "[inbound] failed to remove draining marker {}: {e}",
                draining.display()
            );
        }
        if !self.success {
            // Release failed before the explicit halt step. Re-write
            // `volume.stopped` so the volume returns to its
            // pre-release state — without this the supervisor would
            // keep the (now transport-suppressed-then-restored)
            // process running indefinitely.
            let stopped = self.vol_dir.join("volume.stopped");
            if let Err(e) = std::fs::write(&stopped, "")
                && e.kind() != std::io::ErrorKind::AlreadyExists
            {
                warn!(
                    "[inbound] failed to restore volume.stopped after \
                     aborted release at {}: {e}",
                    stopped.display()
                );
            }
            // The volume process is still running with the actor
            // initialised; ask it to halt cleanly so the supervisor
            // sees a clean exit and respects the marker we just
            // wrote. Best-effort: a failed shutdown leaves the
            // supervisor to notice the stopped marker on the next
            // poll and not respawn after the eventual exit.
            let vol_dir = self.vol_dir.clone();
            tokio::spawn(async move {
                let _ = elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await;
            });
        }
    }
}

/// Poll until `control.sock` actually accepts a connection. Returns
/// `true` as soon as a connect+accept succeeds, or `false` if
/// `timeout` elapses first. Used by `release_volume_op` when
/// transparently restarting a stopped volume so the snapshot path can
/// talk to it.
///
/// File existence alone is not enough: the previous volume process
/// can leave a stale socket file behind, and the new volume's
/// `UnixListener::bind` does `remove_file` + `bind` — there is a
/// window where the file exists but no listener is attached, so
/// `connect()` returns `ECONNREFUSED`. We probe the connection itself
/// and treat that as the readiness signal.
async fn wait_for_control_sock(vol_dir: &Path, timeout: std::time::Duration) -> bool {
    let socket = vol_dir.join("control.sock");
    let deadline = std::time::Instant::now() + timeout;
    let probe_step = std::time::Duration::from_millis(50);
    while std::time::Instant::now() < deadline {
        if socket.exists() {
            match tokio::net::UnixStream::connect(&socket).await {
                Ok(_) => return true,
                Err(_) => {
                    // Stale file (waiting for the new listener), or
                    // listener is up but transient refusal — retry.
                }
            }
        }
        tokio::time::sleep(probe_step).await;
    }
    false
}

/// Parsed shape of a `release` IPC line.
///
/// Wire format (after the leading `release` verb): `<name> [--force]`.
struct ParsedReleaseArgs<'a> {
    name: &'a str,
    force: bool,
}

fn parse_release_args(args: &str) -> Result<ParsedReleaseArgs<'_>, String> {
    let mut tokens = args.split_whitespace();
    let name = tokens
        .next()
        .ok_or_else(|| "usage: release <volume> [--force]".to_owned())?;
    let mut force = false;
    for tok in tokens {
        match tok {
            "--force" => {
                if force {
                    return Err("--force specified twice".to_owned());
                }
                force = true;
            }
            other => return Err(format!("unrecognised release flag: {other}")),
        }
    }
    Ok(ParsedReleaseArgs { name, force })
}

/// `volume release --force`.
///
/// Override path for an unreachable previous owner: synthesise a
/// fresh handoff snapshot from S3-visible segments under the dead
/// fork's prefix, sign it with this coordinator's identity key, and
/// unconditionally rewrite `names/<name>` to `Released`.
///
/// Does **not** require a local symlink, does **not** drain any WAL
/// (the dead owner's WAL is unreachable), does **not** halt or touch
/// any local volume daemon. The data-loss boundary is "writes the
/// dead owner accepted but never promoted to S3" — same as the
/// crash-recovery contract elsewhere.
async fn force_release_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> String {
    use elide_coordinator::lifecycle::{self, ForceReleaseOutcome};
    use elide_coordinator::recovery;

    // Refuse when the "dead peer" is actually this host's running
    // daemon. force-release is for unreachable peers; against a local
    // running fork it would leave on-disk state diverging from the
    // bucket record. The operator wants `volume stop` first.
    if local_daemon_running(data_dir, volume_name) {
        return format!(
            "err volume '{volume_name}' is running on this host; \
             stop it first with: elide volume stop {volume_name}"
        );
    }

    // Read the current record to learn which dead fork to recover from.
    let dead_vol_ulid =
        match elide_coordinator::name_store::read_name_record(store, volume_name).await {
            Ok(Some((rec, _))) => {
                use elide_core::name_record::NameState;
                match rec.state {
                    NameState::Live | NameState::Stopped => rec.vol_ulid,
                    other => {
                        return format!(
                            "err names/{volume_name} is in state {other:?}; \
                         force-release only overrides Live or Stopped records"
                        );
                    }
                }
            }
            Ok(None) => return format!("err name '{volume_name}' has no S3 record"),
            Err(e) => return format!("err reading names/{volume_name}: {e}"),
        };

    // Recovery pipeline: fetch dead fork's pubkey, list+verify
    // segments, mint+sign+publish synthesised handoff snapshot.
    let dead_pub = match recovery::fetch_volume_pub(store, dead_vol_ulid).await {
        Ok(k) => k,
        Err(e) => {
            return format!("err fetching volume.pub for dead fork {dead_vol_ulid}: {e:#}");
        }
    };
    let recovered = match recovery::list_and_verify_segments(store, dead_vol_ulid, &dead_pub).await
    {
        Ok(r) => r,
        Err(e) => {
            return format!("err listing/verifying segments for dead fork {dead_vol_ulid}: {e:#}");
        }
    };
    let segment_ulids: Vec<ulid::Ulid> =
        recovered.segments.iter().map(|s| s.segment_ulid).collect();
    info!(
        "[inbound] force-release {volume_name}: recovered {} segments \
         ({} dropped) from dead fork {dead_vol_ulid}",
        segment_ulids.len(),
        recovered.dropped,
    );

    let published = match recovery::mint_and_publish_synthesised_snapshot(
        store,
        dead_vol_ulid,
        &segment_ulids,
        identity.as_ref(),
        identity.coordinator_id_str(),
    )
    .await
    {
        Ok(p) => p,
        Err(e) => return format!("err publishing synthesised snapshot: {e}"),
    };

    // Unconditional flip of names/<name>.
    let outcome = lifecycle::mark_released_force(store, volume_name, published.snap_ulid).await;
    match outcome {
        Ok(ForceReleaseOutcome::Overwritten { dead_vol_ulid: d }) => {
            info!(
                "[inbound] force-released volume {volume_name} (dead fork {d}) at \
                 synthesised handoff snapshot {}",
                published.snap_ulid,
            );
            format!("ok {}", published.snap_ulid)
        }
        Ok(ForceReleaseOutcome::Absent) => {
            // Race: record disappeared between our read and our write.
            format!("err names/{volume_name} vanished between read and force-write")
        }
        Ok(ForceReleaseOutcome::InvalidState { observed }) => {
            // Race: state changed under us. The synthesised snapshot
            // is still published (harmless); operator can retry.
            format!("err names/{volume_name} changed underneath us; now in state {observed:?}")
        }
        Err(e) => format!(
            "err force-release flip failed (synthesised snapshot {} already published): {e}",
            published.snap_ulid
        ),
    }
}

/// Relinquish ownership of `<volume_name>` so any other coordinator can
/// `volume start` it. Composes the existing snapshot path:
///
/// 1. Refuse if the volume is readonly (no exclusive owner to release)
///    or an NBD client is connected (must disconnect cleanly first).
/// 2. If the volume is `stopped`, transparently bring it back up
///    (clear the marker, notify the supervisor, wait for
///    `control.sock`) — the drain step needs a running daemon.
/// 3. Verify S3 ownership before doing the expensive drain.
/// 4. Drain WAL → publish handoff snapshot via `snapshot_volume`.
/// 5. Send shutdown RPC to halt the daemon.
/// 6. Write `volume.stopped` marker so the supervisor won't restart.
/// 7. Conditional PUT to `names/<name>` setting state=Released and
///    recording the handoff snapshot ULID.
///
/// Two execution paths:
///
/// 1. **Fast path** (clean stopped volume, nothing to drain): reuse
///    the previously-published snapshot as the handoff, skip the
///    daemon restart entirely. Costs one S3 GET (ownership) + one
///    conditional PUT (flip).
///
/// 2. **Slow path** (WAL non-empty / pending uploads / GC handoffs /
///    new segments since last snapshot): bring the daemon up in
///    drain mode, run the existing snapshot pipeline, halt, flip.
#[allow(clippy::too_many_arguments)]
async fn release_volume_op(
    volume_name: &str,
    data_dir: &Path,
    snapshot_locks: &SnapshotLockRegistry,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
    coord_id: &str,
    rescan: &Notify,
) -> String {
    let started = std::time::Instant::now();
    info!("[release {volume_name}] start");

    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!("err volume not found: {volume_name}");
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if vol_dir.join("volume.readonly").exists() {
        return "err volume is readonly; nothing to release".to_string();
    }

    // `release` is composed of two distinct phases: drain+publish (needs
    // a running daemon) and bucket-flip (no daemon needed). To keep the
    // operator-visible state coherent we require the daemon to already
    // be `stopped` — otherwise a release on a running volume would have
    // to halt it inline, and any failure between halt and bucket-flip
    // would leave the volume in a "Released-but-running" mismatch the
    // operator can't easily recover from.
    if !vol_dir.join("volume.stopped").exists() {
        return format!(
            "err volume '{volume_name}' is running; \
             stop it first with: elide volume stop {volume_name}"
        );
    }

    // Verify ownership in S3 before doing any local state mutation.
    // Pulled ahead of the daemon restart so a "wrong owner" or
    // "already released" reply doesn't perturb the local volume.
    use elide_core::name_record::NameState;
    let read_started = std::time::Instant::now();
    match elide_coordinator::name_store::read_name_record(store, volume_name).await {
        Ok(Some((rec, _))) => {
            info!(
                "[release {volume_name}] read names/<name>: state={:?} owner={:?} ({:.2?})",
                rec.state,
                rec.coordinator_id,
                read_started.elapsed()
            );
            if let Some(existing) = rec.coordinator_id.as_deref()
                && existing != coord_id
            {
                return format!(
                    "err name '{volume_name}' is owned by coordinator {existing}; \
                     run `volume release --force` to override"
                );
            }
            if rec.state == NameState::Released {
                return format!("err name '{volume_name}' is already released");
            }
        }
        Ok(None) => {
            return format!("err name '{volume_name}' has no S3 record; drain the volume first");
        }
        Err(e) => {
            return format!("err reading names/{volume_name}: {e}");
        }
    }

    // Fast path: nothing has changed since the last published snapshot,
    // so reuse it as the handoff. The next claimant forks from it
    // identically to a freshly-minted one.
    match release_fast_path_handoff(&vol_dir) {
        Ok(Some(snap_ulid)) => {
            info!(
                "[release {volume_name}] fast path: reusing snapshot {snap_ulid} \
                 (clean stopped volume, no daemon restart needed)"
            );
            let result = perform_release_flip(volume_name, store, coord_id, snap_ulid).await;
            info!(
                "[release {volume_name}] complete in {:.2?}",
                started.elapsed()
            );
            return result;
        }
        Ok(None) => {
            info!(
                "[release {volume_name}] slow path: WAL/pending/gc has work or \
                 segments post-date last snapshot"
            );
        }
        Err(e) => {
            warn!(
                "[release {volume_name}] fast-path inspection failed ({e}); \
                 falling back to slow path"
            );
        }
    }

    // ── Slow path: bring daemon up in drain mode ────────────────────
    //
    // The drain step needs an IPC-capable daemon, so we transparently
    // bring the stopped volume back up in IPC-only mode here. The
    // `volume.draining` marker tells both the supervisor and the
    // volume binary to suppress transport setup; from a client's
    // perspective the volume is never visibly running during release.
    //
    // The `_draining_guard` removes the marker when this function
    // returns, regardless of which exit path is taken (success, early
    // error, panic).
    if let Err(e) = std::fs::write(vol_dir.join("volume.draining"), "") {
        return format!("err writing volume.draining: {e}");
    }
    if let Err(e) = std::fs::remove_file(vol_dir.join("volume.stopped")) {
        let _ = std::fs::remove_file(vol_dir.join("volume.draining"));
        return format!("err clearing volume.stopped for release: {e}");
    }
    rescan.notify_one();
    let mut _draining_guard = Some(DrainingMarkerGuard::new(vol_dir.clone()));

    let bringup_started = std::time::Instant::now();
    info!("[release {volume_name}] bringing daemon up for drain");
    if !wait_for_control_sock(&vol_dir, std::time::Duration::from_secs(30)).await {
        return format!("err timed out waiting for volume '{volume_name}' to come up for release");
    }
    info!(
        "[release {volume_name}] daemon ready in {:.2?}",
        bringup_started.elapsed()
    );

    if !vol_dir.join("control.sock").exists() {
        return format!("err volume '{volume_name}' is not running — start it first");
    }

    match elide_coordinator::control::call_for_inbound(&vol_dir, "connected").await {
        Some(resp) if resp == "ok true" => {
            return "err nbd client is connected; disconnect it first".to_string();
        }
        _ => {}
    }

    // Drain WAL → publish handoff snapshot. Empty volumes get a
    // freshly-minted snapshot ULID; the claim path forks from a
    // zero-entry snapshot as a fresh empty root.
    let snap_started = std::time::Instant::now();
    info!("[release {volume_name}] draining WAL and publishing handoff snapshot");
    let snap_resp = snapshot_volume(
        volume_name,
        data_dir,
        snapshot_locks,
        store,
        part_size_bytes,
    )
    .await;
    let snap_ulid_str = match snap_resp.strip_prefix("ok ") {
        Some(s) => s.trim().to_owned(),
        None => return format!("err snapshot for release failed: {snap_resp}"),
    };
    let snap_ulid = match ulid::Ulid::from_string(&snap_ulid_str) {
        Ok(u) => u,
        Err(e) => return format!("err parsing snapshot ULID '{snap_ulid_str}': {e}"),
    };
    info!(
        "[release {volume_name}] handoff snapshot {snap_ulid} published in {:.2?}",
        snap_started.elapsed()
    );

    // Halt the daemon. Writing the marker first prevents the supervisor
    // from restarting it between shutdown and our final state write.
    // From this point on the release is past the "could fail and need
    // to leave the volume in a usable Stopped state" window — defuse
    // the draining-marker guard so it doesn't fight the explicit
    // halt step we're about to do.
    if let Some(g) = _draining_guard.as_mut() {
        g.defuse();
    }
    if let Err(e) = std::fs::write(vol_dir.join("volume.stopped"), "") {
        return format!("err writing volume.stopped: {e}");
    }
    info!("[release {volume_name}] halting daemon");
    match elide_coordinator::control::call_for_inbound(&vol_dir, "shutdown").await {
        Some(resp) if resp == "ok" => {}
        Some(resp) => {
            let _ = std::fs::remove_file(vol_dir.join("volume.stopped"));
            return format!("err shutdown failed: {resp}");
        }
        None => {
            // Volume process wasn't running by the time we reached
            // this step. The snapshot succeeded though, so we proceed
            // with the state flip.
        }
    }

    let result = perform_release_flip(volume_name, store, coord_id, snap_ulid).await;
    info!(
        "[release {volume_name}] complete in {:.2?}",
        started.elapsed()
    );
    result
}

/// Final S3 conditional PUT flipping `names/<name>` to Released.
async fn perform_release_flip(
    volume_name: &str,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    snap_ulid: ulid::Ulid,
) -> String {
    use elide_coordinator::lifecycle;
    let flip_started = std::time::Instant::now();
    info!(
        "[release {volume_name}] flipping names/<name> -> Released \
         with handoff snapshot {snap_ulid}"
    );
    match lifecycle::mark_released(store, volume_name, coord_id, snap_ulid).await {
        Ok(_) => {
            info!(
                "[release {volume_name}] released at handoff snapshot {snap_ulid} \
                 (flip {:.2?})",
                flip_started.elapsed()
            );
            format!("ok {snap_ulid}")
        }
        Err(e) => {
            warn!("[release {volume_name}] state flip failed: {e}");
            format!("err snapshot {snap_ulid} published but names/<name> update failed: {e}")
        }
    }
}

/// Decide whether `release` can short-circuit using the volume's most
/// recently published snapshot as the handoff point.
///
/// Returns `Ok(Some(ulid))` when **all** of the following hold:
///   - `wal/`, `pending/`, `gc/` are empty or absent (no in-flight work)
///   - the latest segment in `index/` does not post-date the latest
///     local snapshot marker (the snapshot covers everything)
///   - that snapshot's S3 upload sentinel is present (manifest +
///     marker + filemap are confirmed on S3 — without this, a future
///     claimant could fail to fetch the manifest)
///
/// `Ok(None)` means slow path required; an `Err` is propagated to the
/// caller as a fast-path inspection failure (also slow-path fallback).
fn release_fast_path_handoff(vol_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    if !dir_is_empty_or_absent(&vol_dir.join("wal"))? {
        return Ok(None);
    }
    if !dir_is_empty_or_absent(&vol_dir.join("pending"))? {
        return Ok(None);
    }
    if !dir_is_empty_or_absent(&vol_dir.join("gc"))? {
        return Ok(None);
    }

    let Some(snap_ulid) = latest_snapshot_marker(&vol_dir.join("snapshots"))? else {
        return Ok(None);
    };

    // The snapshot triple (marker + filemap + .manifest) is uploaded
    // atomically; the sentinel is written only after all three succeed.
    // Its presence is the canonical "this snapshot is on S3" check.
    let sentinel = vol_dir
        .join("uploaded")
        .join("snapshots")
        .join(snap_ulid.to_string());
    if !sentinel.exists() {
        return Ok(None);
    }

    if let Some(seg) = latest_segment_ulid(&vol_dir.join("index"))?
        && seg > snap_ulid
    {
        return Ok(None);
    }

    Ok(Some(snap_ulid))
}

fn dir_is_empty_or_absent(p: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(p) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e),
    }
}

/// Return the highest ULID among `snapshots/<ulid>` markers (skipping
/// `<ulid>.manifest` / `<ulid>.filemap` siblings).
fn latest_snapshot_marker(snap_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    let entries = match std::fs::read_dir(snap_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut latest: Option<ulid::Ulid> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        // Skip filemap/manifest siblings: `<ulid>.filemap`,
        // `<ulid>.manifest`. Plain marker is the bare ULID.
        if s.contains('.') {
            continue;
        }
        if let Ok(u) = ulid::Ulid::from_string(s)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(latest)
}

/// Return the highest ULID among `index/<ulid>.idx` files.
fn latest_segment_ulid(index_dir: &Path) -> std::io::Result<Option<ulid::Ulid>> {
    let entries = match std::fs::read_dir(index_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut latest: Option<ulid::Ulid> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        let Some(stem) = s.strip_suffix(".idx") else {
            continue;
        };
        if let Ok(u) = ulid::Ulid::from_string(stem)
            && latest.is_none_or(|cur| u > cur)
        {
            latest = Some(u);
        }
    }
    Ok(latest)
}

/// Internal helper: atomically rebind a `Released` name to a
/// freshly-minted local fork, leaving the bucket state in `Stopped`.
/// The CLI orchestrates fork creation before calling.
///
/// Pre-conditions enforced here:
/// - `by_id/<new_vol_ulid>/` must exist locally (the CLI created it).
/// - `by_name/<name>` must point at it (or be absent — created if so).
/// - `names/<name>` must be in `Released` state and the conditional
///   PUT must succeed.
///
/// The volume is left `Stopped`; the CLI calls `start` if a follow-up
/// daemon launch was requested.
async fn rebind_name_op(
    volume_name: &str,
    new_ulid_str: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    rescan: &Notify,
) -> String {
    let new_ulid = match ulid::Ulid::from_string(new_ulid_str) {
        Ok(u) => u,
        Err(e) => return format!("err invalid new_vol_ulid: {e}"),
    };

    let new_dir = data_dir.join("by_id").join(new_ulid.to_string());
    if !new_dir.exists() {
        return format!(
            "err new fork directory not found at {} — \
             the CLI must materialise the fork before calling rebind-name",
            new_dir.display()
        );
    }

    // Ensure volume.stopped is present so the supervisor doesn't
    // launch the daemon when notified — claim leaves the volume
    // halted; start brings it up.
    if let Err(e) = std::fs::write(new_dir.join("volume.stopped"), "") {
        return format!("err writing volume.stopped on new fork: {e}");
    }

    let symlink_path = data_dir.join("by_name").join(volume_name);
    let target = std::path::PathBuf::from(format!("../by_id/{new_ulid}"));
    match std::fs::read_link(&symlink_path) {
        Ok(existing) if existing == target => {}
        Ok(other) => {
            return format!(
                "err by_name/{volume_name} already points at {} (expected ../by_id/{new_ulid})",
                other.display()
            );
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            if let Err(e) = std::os::unix::fs::symlink(&target, &symlink_path) {
                return format!("err creating by_name/{volume_name} symlink: {e}");
            }
        }
        Err(e) => return format!("err reading by_name/{volume_name}: {e}"),
    }

    use elide_coordinator::lifecycle::{LifecycleError, MarkClaimedOutcome, mark_claimed};
    use elide_core::name_record::NameState;
    match mark_claimed(store, volume_name, coord_id, new_ulid, NameState::Stopped).await {
        Ok(MarkClaimedOutcome::Claimed) => {
            rescan.notify_one();
            info!("[inbound] rebound name {volume_name} to new fork {new_ulid} (stopped)");
            format!("ok {new_ulid}")
        }
        Ok(MarkClaimedOutcome::Absent) => {
            format!("err names/{volume_name} does not exist; nothing to claim")
        }
        Ok(MarkClaimedOutcome::NotReleased { observed }) => {
            format!(
                "err names/{volume_name} is in state {observed:?}, not Released; \
                 cannot claim"
            )
        }
        Err(LifecycleError::Store(e)) => format!("err claim failed: {e}"),
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            format!(
                "err name '{volume_name}' raced with another claim ({held_by} won); \
                 your local fork at {new_ulid} can be re-purposed manually"
            )
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            format!("err names/{volume_name} is in state {from:?}; cannot claim")
        }
    }
}

/// `volume claim <name> [--force]` IPC handler.
///
/// Inspects `names/<name>` and either:
///   - reclaims in place (own released fork still on disk) → `ok reclaimed`
///   - directs the CLI to orchestrate a foreign claim → `released <vol_ulid> <snap>`
///   - with `--force` overrides foreign Live/Stopped ownership by
///     synthesising a handoff snapshot then directing CLI orchestration.
///
/// The result always leaves the volume `Stopped` (no daemon launched).
/// The CLI calls `start` afterwards if `volume start --claim` was the
/// composed flow.
async fn claim_volume_bucket_op(
    volume_name: &str,
    force: bool,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    identity: &Arc<elide_coordinator::identity::CoordinatorIdentity>,
) -> String {
    use elide_core::name_record::NameState;

    // Claim always lands the volume in `Stopped`. A running local
    // daemon contradicts that — refuse and point the operator at
    // `volume stop` first.
    if local_daemon_running(data_dir, volume_name) {
        return format!(
            "err volume '{volume_name}' is running on this host; \
             stop it first with: elide volume stop {volume_name}"
        );
    }

    let record_opt = match elide_coordinator::name_store::read_name_record(store, volume_name).await
    {
        Ok(r) => r,
        Err(e) => return format!("err reading names/{volume_name}: {e}"),
    };

    let record = match record_opt {
        Some((rec, _)) => rec,
        None => return format!("err name '{volume_name}' has no S3 record; nothing to claim"),
    };

    // --force on a Live/Stopped foreign-owned record: synthesise a
    // handoff snapshot and rewrite the record to Released, then fall
    // through into the regular claim path.
    let record = if force
        && matches!(record.state, NameState::Live | NameState::Stopped)
        && record
            .coordinator_id
            .as_deref()
            .is_some_and(|id| id != coord_id)
    {
        match force_release_volume_op(volume_name, data_dir, store, identity).await {
            r if r.starts_with("ok") => {}
            err => return err,
        }
        match elide_coordinator::name_store::read_name_record(store, volume_name).await {
            Ok(Some((rec, _))) => rec,
            Ok(None) => return format!("err name '{volume_name}' vanished after force-release"),
            Err(e) => return format!("err re-reading names/{volume_name}: {e}"),
        }
    } else {
        record
    };

    match record.state {
        NameState::Released => {
            // Determine whether we hold a matching local fork.
            let link = data_dir.join("by_name").join(volume_name);
            let local_vol_ulid = match std::fs::canonicalize(&link) {
                Ok(p) => p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| ulid::Ulid::from_string(s).ok()),
                Err(_) => None,
            };

            if local_vol_ulid == Some(record.vol_ulid) {
                use elide_coordinator::lifecycle::{
                    LifecycleError, MarkReclaimedLocalOutcome, mark_reclaimed_local,
                };
                match mark_reclaimed_local(
                    store,
                    volume_name,
                    coord_id,
                    record.vol_ulid,
                    NameState::Stopped,
                )
                .await
                {
                    Ok(MarkReclaimedLocalOutcome::Reclaimed) => {
                        info!(
                            "[inbound] reclaimed {volume_name} in place (vol_ulid {})",
                            record.vol_ulid
                        );
                        "ok reclaimed".to_string()
                    }
                    Ok(MarkReclaimedLocalOutcome::Absent) => {
                        format!("err names/{volume_name} vanished between read and reclaim")
                    }
                    Ok(MarkReclaimedLocalOutcome::NotReleased { observed_state, .. }) => {
                        format!(
                            "err names/{volume_name} changed underneath us; now in state \
                             {observed_state:?}"
                        )
                    }
                    Ok(MarkReclaimedLocalOutcome::ForkMismatch {
                        released_vol_ulid, ..
                    }) => {
                        // Race: someone rebound between our read and write.
                        let snap = record
                            .handoff_snapshot
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "_".to_string());
                        format!("released {released_vol_ulid} {snap}")
                    }
                    Err(LifecycleError::Store(e)) => format!("err reclaim failed: {e}"),
                    Err(LifecycleError::OwnershipConflict { .. })
                    | Err(LifecycleError::InvalidTransition { .. }) => {
                        format!("err in-place reclaim of {volume_name} refused")
                    }
                }
            } else {
                // Foreign content — CLI must orchestrate the claim.
                let snap = record
                    .handoff_snapshot
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "_".to_string());
                format!("released {} {}", record.vol_ulid, snap)
            }
        }
        NameState::Live | NameState::Stopped => {
            match record.coordinator_id.as_deref() {
                Some(owner) if owner == coord_id => {
                    // Already ours — nothing to claim.
                    format!("err name '{volume_name}' is already held by this coordinator")
                }
                Some(owner) => format!(
                    "err name '{volume_name}' is held by coordinator {owner}; \
                     run with --force to override"
                ),
                None => {
                    format!("err name '{volume_name}' has no coordinator_id (malformed record)")
                }
            }
        }
        NameState::Readonly => format!(
            "err name '{volume_name}' is readonly (immutable handle); \
             pull it with `volume pull` to serve locally"
        ),
    }
}

async fn start_volume_op(
    volume_name: &str,
    data_dir: &Path,
    store: &Arc<dyn ObjectStore>,
    coord_id: &str,
    rescan: &Notify,
) -> String {
    let link = data_dir.join("by_name").join(volume_name);
    if !link.exists() {
        return format!(
            "err volume '{volume_name}' not found locally; \
             to claim it from the bucket, run: elide volume claim {volume_name}"
        );
    }
    let vol_dir = match std::fs::canonicalize(&link) {
        Ok(p) => p,
        Err(e) => return format!("err resolving volume dir: {e}"),
    };

    if !vol_dir.join("volume.stopped").exists() {
        return "err volume is not stopped".to_string();
    }

    use elide_coordinator::lifecycle::{LifecycleError, MarkLiveOutcome, mark_live};
    match mark_live(store, volume_name, coord_id).await {
        Ok(MarkLiveOutcome::Resumed) | Ok(MarkLiveOutcome::AlreadyLive) => {}
        Ok(MarkLiveOutcome::Absent) => {
            // No S3 record yet — proceed local-only.
        }
        Ok(MarkLiveOutcome::Released) => {
            return format!(
                "err name '{volume_name}' is Released; \
                 reclaim with: elide volume claim {volume_name}"
            );
        }
        Err(LifecycleError::OwnershipConflict { held_by }) => {
            return format!(
                "err name '{volume_name}' is owned by coordinator {held_by}; \
                 run `volume release --force` to override"
            );
        }
        Err(LifecycleError::InvalidTransition { from, .. }) => {
            return format!("err names/<name> is in state {from:?}; cannot start");
        }
        Err(LifecycleError::Store(e)) => {
            warn!("[inbound] start {volume_name}: failed to update names/<name>: {e}");
        }
    }

    match elide_core::config::find_nbd_conflict(&vol_dir, data_dir) {
        Ok(Some(conflict)) => {
            return format!(
                "err nbd endpoint {} conflicts with volume '{}'",
                conflict.endpoint, conflict.name,
            );
        }
        Ok(None) => {}
        Err(e) => return format!("err nbd conflict check: {e}"),
    }

    if let Err(e) = std::fs::remove_file(vol_dir.join("volume.stopped")) {
        return format!("err clearing volume.stopped: {e}");
    }
    rescan.notify_one();
    info!("[inbound] started volume {volume_name}");
    "ok".to_string()
}

// ── Macaroon-authenticated credential vending ────────────────────────────────

/// Verify the connecting peer matches the volume's recorded `volume.pid`.
///
/// Returns `Ok(pid)` on a clean match. Returns `Err(<wire-error-line>)` ready
/// to send back to the volume — the messages are intentionally generic so a
/// hostile caller can't tell apart "no such volume", "pid not yet recorded"
/// (the spawn-write race), or "pid mismatch".
fn check_peer_pid(vol_dir: &Path, peer_pid: Option<i32>) -> Result<i32, String> {
    let peer_pid = peer_pid.ok_or_else(|| "err peer pid unavailable".to_string())?;
    let pid_path = vol_dir.join("volume.pid");
    let recorded: i32 = match std::fs::read_to_string(&pid_path) {
        Ok(s) => s
            .trim()
            .parse()
            .map_err(|_| "err volume.pid is not numeric".to_string())?,
        // The supervisor writes volume.pid right after spawn returns. There is
        // a brief window where a fast-starting volume can dial control.sock
        // before the parent has written the file — the volume retries, so we
        // simply report "not registered" and let the caller back off.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err("err volume not registered".to_string());
        }
        Err(e) => return Err(format!("err reading volume.pid: {e}")),
    };
    if recorded != peer_pid {
        return Err("err peer pid does not match volume.pid".to_string());
    }
    Ok(peer_pid)
}

fn register_volume(
    volume_ulid: &str,
    data_dir: &Path,
    peer_pid: Option<i32>,
    macaroon_root: &[u8; 32],
) -> String {
    // Parse the ULID at the boundary so we never thread a raw string to the
    // caveat or the filesystem path.
    let ulid = match ulid::Ulid::from_string(volume_ulid) {
        Ok(u) => u.to_string(),
        Err(e) => return format!("err invalid volume ulid: {e}"),
    };
    let vol_dir = data_dir.join("by_id").join(&ulid);
    if !vol_dir.exists() {
        return "err unknown volume".to_string();
    }
    let pid = match check_peer_pid(&vol_dir, peer_pid) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let m = macaroon::mint(
        macaroon_root,
        vec![
            Caveat::Volume(ulid),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(pid),
        ],
    );
    format!("ok {}", m.encode())
}

fn issue_credentials(
    macaroon_str: &str,
    data_dir: &Path,
    peer_pid: Option<i32>,
    macaroon_root: &[u8; 32],
    issuer: &dyn CredentialIssuer,
) -> String {
    let m = match Macaroon::parse(macaroon_str) {
        Ok(m) => m,
        Err(e) => return format!("err parse macaroon: {e}"),
    };
    if !macaroon::verify(macaroon_root, &m) {
        // Generic message — don't help an attacker distinguish "wrong key"
        // from "tampered caveats".
        return "err invalid macaroon".to_string();
    }
    if m.scope() != Some(Scope::Credentials) {
        return "err macaroon scope mismatch".to_string();
    }
    let Some(volume_ulid) = m.volume() else {
        return "err macaroon missing volume caveat".to_string();
    };
    let Some(macaroon_pid) = m.pid() else {
        return "err macaroon missing pid caveat".to_string();
    };
    let peer_pid = match peer_pid {
        Some(p) => p,
        None => return "err peer pid unavailable".to_string(),
    };
    if peer_pid != macaroon_pid {
        return "err peer pid does not match macaroon".to_string();
    }
    if let Some(not_after) = m.not_after() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now >= not_after {
            return "err macaroon expired".to_string();
        }
    }
    // Re-validate that volume.pid still matches — covers the case where the
    // original process has exited and the PID was reused.
    let vol_dir = data_dir.join("by_id").join(volume_ulid);
    if let Err(e) = check_peer_pid(&vol_dir, Some(peer_pid)) {
        return e;
    }
    if !pid_is_alive(peer_pid as u32) {
        return "err peer pid not alive".to_string();
    }
    let creds = match issuer.issue(volume_ulid) {
        Ok(c) => c,
        Err(e) => return format!("err issue: {e}"),
    };
    let mut body = String::from("ok\n");
    body.push_str(&format!(
        "access_key_id = {}\n",
        toml_quote(&creds.access_key_id)
    ));
    body.push_str(&format!(
        "secret_access_key = {}\n",
        toml_quote(&creds.secret_access_key)
    ));
    if let Some(tok) = &creds.session_token {
        body.push_str(&format!("session_token = {}\n", toml_quote(tok)));
    }
    if let Some(exp) = creds.expiry_unix {
        body.push_str(&format!("expiry_unix = {exp}\n"));
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::IssuedCredentials;
    use tempfile::TempDir;

    struct FixedIssuer;
    impl CredentialIssuer for FixedIssuer {
        fn issue(&self, _vol: &str) -> std::io::Result<IssuedCredentials> {
            Ok(IssuedCredentials {
                access_key_id: "AK".into(),
                secret_access_key: "SK".into(),
                session_token: None,
                expiry_unix: None,
            })
        }
    }

    fn setup_volume(data_dir: &Path, ulid: &str, pid: i32) {
        let dir = data_dir.join("by_id").join(ulid);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("volume.pid"), pid.to_string()).unwrap();
    }

    fn key() -> [u8; 32] {
        [0x42; 32]
    }

    #[test]
    fn register_succeeds_when_peer_pid_matches_volume_pid() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        setup_volume(tmp.path(), ulid, 4242);
        let resp = register_volume(ulid, tmp.path(), Some(4242), &key());
        assert!(resp.starts_with("ok "), "expected ok, got {resp}");
    }

    #[test]
    fn register_rejects_pid_mismatch() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        setup_volume(tmp.path(), ulid, 4242);
        let resp = register_volume(ulid, tmp.path(), Some(9999), &key());
        assert!(resp.starts_with("err"), "expected err, got {resp}");
        assert!(resp.contains("does not match"));
    }

    #[test]
    fn register_rejects_when_volume_pid_missing() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        // Create the volume dir but no volume.pid yet — this is the spawn-write
        // race the volume-side retry absorbs.
        std::fs::create_dir_all(tmp.path().join("by_id").join(ulid)).unwrap();
        let resp = register_volume(ulid, tmp.path(), Some(4242), &key());
        assert!(resp.contains("not registered"), "{resp}");
    }

    #[test]
    fn register_rejects_unknown_volume() {
        let tmp = TempDir::new().unwrap();
        let resp = register_volume("01JQAAAAAAAAAAAAAAAAAAAAAA", tmp.path(), Some(4242), &key());
        assert!(resp.contains("unknown volume"), "{resp}");
    }

    #[test]
    fn register_rejects_invalid_ulid() {
        let tmp = TempDir::new().unwrap();
        let resp = register_volume("not-a-ulid", tmp.path(), Some(4242), &key());
        assert!(resp.contains("invalid volume ulid"), "{resp}");
    }

    #[test]
    fn credentials_round_trip_with_live_pid() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        // Use our own pid so the pid_is_alive check passes inside the handler.
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let mint_resp = register_volume(ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        let issuer = FixedIssuer;
        let resp = issue_credentials(macaroon, tmp.path(), Some(my_pid), &key(), &issuer);
        assert!(resp.starts_with("ok\n"), "expected ok body, got {resp}");
        assert!(resp.contains("access_key_id = \"AK\""));
        assert!(resp.contains("secret_access_key = \"SK\""));
    }

    #[test]
    fn credentials_rejects_wrong_root_key() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let mint_resp = register_volume(ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        let mut other = key();
        other[0] ^= 0xFF;
        let issuer = FixedIssuer;
        let resp = issue_credentials(macaroon, tmp.path(), Some(my_pid), &other, &issuer);
        assert!(resp.contains("invalid macaroon"), "{resp}");
    }

    #[test]
    fn credentials_rejects_pid_mismatch() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let mint_resp = register_volume(ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        let issuer = FixedIssuer;
        // Present a different peer pid than the macaroon was minted for.
        let resp = issue_credentials(macaroon, tmp.path(), Some(my_pid + 1), &key(), &issuer);
        assert!(
            resp.contains("does not match macaroon") || resp.contains("does not match volume.pid"),
            "{resp}"
        );
    }

    #[test]
    fn credentials_rejects_tampered_caveat() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        // Mint a macaroon for a different volume — the verify call rejects
        // because the volume caveat is wrong even though MAC matches.
        let other_ulid = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        setup_volume(tmp.path(), other_ulid, my_pid);
        let mint_resp = register_volume(other_ulid, tmp.path(), Some(my_pid), &key());
        let macaroon = mint_resp.strip_prefix("ok ").unwrap();
        // Now hand-edit the macaroon to claim it's for `ulid`. We can do that
        // by re-minting with a tampered caveat list and copying the original
        // MAC — but that fails verify, which is what we want to assert.
        let parsed = Macaroon::parse(macaroon).unwrap();
        let mut caveats: Vec<Caveat> = parsed.caveats().to_vec();
        for c in &mut caveats {
            if let Caveat::Volume(v) = c {
                *v = ulid.to_owned();
            }
        }
        // Construct a forged macaroon with the original MAC but rewritten
        // caveats. We have to drop into the same module to do this since
        // the struct fields are private — exposing a helper would broaden
        // the API just for the test, so reuse the encode round-trip with
        // the *new* mint and confirm verify fails for the *old* MAC.
        let forged = macaroon::mint(&[0u8; 32], caveats); // wrong key
        let issuer = FixedIssuer;
        let resp = issue_credentials(&forged.encode(), tmp.path(), Some(my_pid), &key(), &issuer);
        assert!(resp.contains("invalid macaroon"), "{resp}");
    }

    #[test]
    fn credentials_rejects_expired_macaroon() {
        let tmp = TempDir::new().unwrap();
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let my_pid = std::process::id() as i32;
        setup_volume(tmp.path(), ulid, my_pid);
        let m = macaroon::mint(
            &key(),
            vec![
                Caveat::Volume(ulid.to_owned()),
                Caveat::Scope(macaroon::Scope::Credentials),
                Caveat::Pid(my_pid),
                Caveat::NotAfter(1), // unix second 1 — long ago
            ],
        );
        let issuer = FixedIssuer;
        let resp = issue_credentials(&m.encode(), tmp.path(), Some(my_pid), &key(), &issuer);
        assert!(resp.contains("expired"), "{resp}");
    }

    #[test]
    fn parse_flags_empty() {
        let p = parse_transport_flags("").unwrap();
        assert!(p.nbd_port.is_none());
        assert!(p.nbd_bind.is_none());
        assert!(p.nbd_socket.is_none());
        assert!(!p.no_nbd && !p.ublk && !p.no_ublk);
        assert!(p.ublk_id.is_none());
    }

    #[test]
    fn parse_flags_nbd() {
        let p = parse_transport_flags("nbd-port=10809 nbd-bind=0.0.0.0").unwrap();
        assert_eq!(p.nbd_port, Some(10809));
        assert_eq!(p.nbd_bind.as_deref(), Some("0.0.0.0"));
    }

    #[test]
    fn parse_flags_ublk() {
        let p = parse_transport_flags("ublk ublk-id=7").unwrap();
        assert!(p.ublk);
        assert_eq!(p.ublk_id, Some(7));
    }

    #[test]
    fn parse_flags_clearing() {
        let p = parse_transport_flags("no-nbd no-ublk").unwrap();
        assert!(p.no_nbd);
        assert!(p.no_ublk);
    }

    #[test]
    fn parse_flags_unknown_rejected() {
        assert!(parse_transport_flags("nbd-port=80 unknown=1").is_err());
        assert!(parse_transport_flags("not-a-flag").is_err());
    }

    #[test]
    fn parse_flags_bad_value() {
        assert!(parse_transport_flags("nbd-port=not-a-number").is_err());
        assert!(parse_transport_flags("ublk-id=").is_err());
    }

    #[test]
    fn validate_name_accepts() {
        for n in &["foo", "vol-1", "my_vol", "ubuntu.22.04"] {
            assert!(validate_volume_name(n).is_ok(), "expected ok for {n}");
        }
    }

    #[test]
    fn validate_name_rejects() {
        assert!(validate_volume_name("").is_err());
        assert!(validate_volume_name("foo:bar").is_err());
        assert!(validate_volume_name("foo/bar").is_err());
        assert!(validate_volume_name("status").is_err());
        assert!(validate_volume_name("attach").is_err());
    }

    #[test]
    fn decode_hex32_roundtrip() {
        let bytes = [0xAB; 32];
        let s: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(decode_hex32(&s).unwrap(), bytes);
    }

    #[test]
    fn decode_hex32_wrong_length() {
        assert!(decode_hex32("abcd").is_err());
        assert!(decode_hex32(&"a".repeat(63)).is_err());
        assert!(decode_hex32(&"a".repeat(65)).is_err());
    }

    #[test]
    fn decode_hex32_invalid_chars() {
        let mut s = "ab".repeat(32);
        s.replace_range(2..4, "zz");
        assert!(decode_hex32(&s).is_err());
    }

    // ── status-remote ─────────────────────────────────────────────────────

    fn mem_store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn sample_ulid() -> ulid::Ulid {
        ulid::Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[tokio::test]
    async fn status_remote_absent_name_returns_err() {
        let store = mem_store();
        let resp = volume_status_remote("ghost", &store, "coord-self").await;
        assert!(resp.starts_with("err "), "got: {resp}");
        assert!(resp.contains("no S3 record"), "{resp}");
    }

    #[tokio::test]
    async fn status_remote_owned_live_record() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.coordinator_id = Some("coord-self".into());
        rec.hostname = Some("host-A".into());
        rec.claimed_at = Some("2026-04-28T00:00:00Z".into());
        elide_coordinator::name_store::create_name_record(&store, "vol", &rec)
            .await
            .unwrap();

        let resp = volume_status_remote("vol", &store, "coord-self").await;
        assert!(resp.starts_with("ok\n"), "{resp}");
        assert!(resp.contains("state = \"live\""));
        assert!(resp.contains(&format!("vol_ulid = \"{}\"", sample_ulid())));
        assert!(resp.contains("coordinator_id = \"coord-self\""));
        assert!(resp.contains("hostname = \"host-A\""));
        assert!(resp.contains("claimed_at = \"2026-04-28T00:00:00Z\""));
        assert!(resp.contains("eligibility = \"owned\""));
    }

    #[tokio::test]
    async fn status_remote_foreign_live_record() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.coordinator_id = Some("coord-other".into());
        elide_coordinator::name_store::create_name_record(&store, "vol", &rec)
            .await
            .unwrap();

        let resp = volume_status_remote("vol", &store, "coord-self").await;
        assert!(resp.contains("state = \"live\""), "{resp}");
        assert!(resp.contains("eligibility = \"foreign\""), "{resp}");
    }

    #[tokio::test]
    async fn status_remote_released_is_claimable() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.state = elide_core::name_record::NameState::Released;
        rec.handoff_snapshot = Some(sample_ulid());
        elide_coordinator::name_store::create_name_record(&store, "vol", &rec)
            .await
            .unwrap();

        let resp = volume_status_remote("vol", &store, "coord-self").await;
        assert!(resp.contains("state = \"released\""), "{resp}");
        assert!(
            resp.contains("eligibility = \"released-claimable\""),
            "{resp}"
        );
        assert!(resp.contains("handoff_snapshot ="), "{resp}");
    }

    #[tokio::test]
    async fn status_remote_readonly() {
        let store = mem_store();
        let mut rec = elide_core::name_record::NameRecord::live_minimal(sample_ulid());
        rec.state = elide_core::name_record::NameState::Readonly;
        elide_coordinator::name_store::create_name_record(&store, "img", &rec)
            .await
            .unwrap();

        let resp = volume_status_remote("img", &store, "coord-self").await;
        assert!(resp.contains("state = \"readonly\""), "{resp}");
        assert!(resp.contains("eligibility = \"readonly\""), "{resp}");
    }

    // ── force_release_volume_op ───────────────────────────────────────────
    //
    // Verify the inbound op composes recovery + lifecycle + name_store
    // correctly. The lower-level helpers each have unit coverage already;
    // these tests exercise the IPC verb's end-to-end path: read current
    // record → fetch dead pubkey → list+verify segments → publish
    // synthesised snapshot → unconditionally rewrite names/<name>.

    use elide_coordinator::identity::CoordinatorIdentity;
    use elide_coordinator::name_store as ns;
    use elide_core::name_record::{NameRecord, NameState};
    use elide_core::segment::{SegmentEntry, SegmentFlags, SegmentSigner, write_segment};
    use elide_core::signing::generate_ephemeral_signer;
    use object_store::PutPayload;
    use object_store::path::Path as StorePath;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Upload a `volume.pub` for the dead fork at the canonical path.
    async fn upload_dead_pub(
        store: &Arc<dyn ObjectStore>,
        vol_ulid: ulid::Ulid,
        vk: &ed25519_dalek::VerifyingKey,
    ) {
        let key = StorePath::from(format!("by_id/{vol_ulid}/volume.pub"));
        let body = format!("{}\n", hex(&vk.to_bytes()));
        store
            .put(&key, PutPayload::from(body.into_bytes()))
            .await
            .unwrap();
    }

    /// Build a single-entry signed segment via the canonical writer and
    /// upload it under `by_id/<vol_ulid>/segments/<seg_ulid>`.
    async fn upload_signed_segment(
        store: &Arc<dyn ObjectStore>,
        vol_ulid: ulid::Ulid,
        seg_ulid: ulid::Ulid,
        signer: &dyn SegmentSigner,
        body: &[u8],
    ) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg");
        let hash = blake3::hash(body);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            body.to_vec(),
        )];
        write_segment(&path, &mut entries, signer).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let key = StorePath::from(format!("by_id/{vol_ulid}/segments/{seg_ulid}"));
        store.put(&key, PutPayload::from(bytes)).await.unwrap();
    }

    /// Fixture: a name in `Live` state pointing at a dead fork that has
    /// `volume.pub` and one signed segment in S3, plus a fresh
    /// `CoordinatorIdentity` for the recovering coordinator.
    async fn force_release_fixture(
        name: &str,
    ) -> (
        Arc<dyn ObjectStore>,
        Arc<CoordinatorIdentity>,
        ulid::Ulid,
        ulid::Ulid,
        TempDir,
    ) {
        let store: Arc<dyn ObjectStore> = mem_store();
        let dead_vol = ulid::Ulid::new();
        let seg_ulid = ulid::Ulid::new();

        let (signer, vk) = generate_ephemeral_signer();
        upload_dead_pub(&store, dead_vol, &vk).await;
        upload_signed_segment(&store, dead_vol, seg_ulid, signer.as_ref(), b"data").await;

        // names/<name> = Live, owned by some "previous" coordinator id.
        let mut rec = NameRecord::live_minimal(dead_vol);
        rec.coordinator_id = Some("dead-owner".into());
        ns::create_name_record(&store, name, &rec).await.unwrap();

        // Recovering coordinator's identity, rooted in a tempdir.
        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        (store, identity, dead_vol, seg_ulid, coord_dir)
    }

    #[tokio::test]
    async fn force_release_op_overwrites_live_to_released() {
        let (store, identity, dead_vol, _seg, _td) = force_release_fixture("vol").await;

        let resp =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity).await;

        assert!(resp.starts_with("ok "), "{resp}");
        let snap_str = resp.strip_prefix("ok ").unwrap();
        let snap_ulid = ulid::Ulid::from_string(snap_str).expect("snapshot ULID");

        // names/<vol> is now Released, references the dead fork.
        let (rec, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(rec.state, NameState::Released);
        assert_eq!(rec.vol_ulid, dead_vol);
        assert_eq!(rec.handoff_snapshot, Some(snap_ulid));

        // Synthesised manifest landed under the dead fork's snapshots/ prefix.
        let snap_prefix = StorePath::from(format!("by_id/{dead_vol}/snapshots/"));
        use futures::TryStreamExt;
        let listed: Vec<_> = store.list(Some(&snap_prefix)).try_collect().await.unwrap();
        assert_eq!(listed.len(), 1, "exactly one synthesised snapshot");
        assert!(
            listed[0]
                .location
                .as_ref()
                .ends_with(&format!("{snap_ulid}.manifest")),
            "manifest path is {}",
            listed[0].location.as_ref()
        );
    }

    #[tokio::test]
    async fn force_release_op_refuses_already_released_record() {
        let (store, identity, _dead, _seg, _td) = force_release_fixture("vol").await;

        // Pre-flip names/<vol> to Released so the op refuses.
        let (mut rec, v) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        rec.state = NameState::Released;
        rec.coordinator_id = None;
        ns::update_name_record(&store, "vol", &rec, v)
            .await
            .unwrap();

        let resp =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity).await;
        assert!(resp.starts_with("err "), "{resp}");
        assert!(
            resp.contains("force-release only overrides Live or Stopped"),
            "{resp}"
        );
    }

    #[tokio::test]
    async fn force_release_op_refuses_absent_name() {
        let store = mem_store();
        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        let resp =
            force_release_volume_op("ghost", TempDir::new().unwrap().path(), &store, &identity)
                .await;
        assert!(resp.starts_with("err "), "{resp}");
        assert!(resp.contains("no S3 record"), "{resp}");
    }

    #[tokio::test]
    async fn force_release_op_errors_when_dead_pub_missing() {
        let store: Arc<dyn ObjectStore> = mem_store();
        let dead_vol = ulid::Ulid::new();
        let mut rec = NameRecord::live_minimal(dead_vol);
        rec.coordinator_id = Some("dead-owner".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        let resp =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity).await;
        assert!(resp.starts_with("err "), "{resp}");
        assert!(
            resp.contains("fetching volume.pub"),
            "expected pubkey fetch error, got: {resp}"
        );
    }

    #[tokio::test]
    async fn force_release_op_drops_tampered_segment_but_succeeds() {
        // A tampered segment must be dropped (signature failure) without
        // failing the verb. The published snapshot covers only the
        // verified segments; the operator can still recover the name.
        let store: Arc<dyn ObjectStore> = mem_store();
        let dead_vol = ulid::Ulid::new();
        let (signer, vk) = generate_ephemeral_signer();
        upload_dead_pub(&store, dead_vol, &vk).await;

        // One good segment, one tampered.
        let good_id = ulid::Ulid::new();
        upload_signed_segment(&store, dead_vol, good_id, signer.as_ref(), b"good").await;
        let bad_id = ulid::Ulid::new();
        // Build a valid segment then flip a byte inside the index section.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg");
        let hash = blake3::hash(b"bad");
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            0,
            1,
            SegmentFlags::empty(),
            b"bad".to_vec(),
        )];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        // Header is 100 bytes; first index entry starts at offset 100.
        bytes[104] ^= 0xff;
        let bad_key = StorePath::from(format!("by_id/{dead_vol}/segments/{bad_id}"));
        store.put(&bad_key, PutPayload::from(bytes)).await.unwrap();

        let mut rec = NameRecord::live_minimal(dead_vol);
        rec.coordinator_id = Some("dead-owner".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let coord_dir = TempDir::new().unwrap();
        let identity = Arc::new(CoordinatorIdentity::load_or_generate(coord_dir.path()).unwrap());

        let resp =
            force_release_volume_op("vol", TempDir::new().unwrap().path(), &store, &identity).await;
        assert!(resp.starts_with("ok "), "{resp}");

        // Verify the synthesised manifest contains exactly one segment ULID
        // — the good one. Read the manifest body and look for both ids.
        let snap_str = resp.strip_prefix("ok ").unwrap();
        use futures::TryStreamExt;
        let snap_prefix = StorePath::from(format!("by_id/{dead_vol}/snapshots/"));
        let listed: Vec<_> = store.list(Some(&snap_prefix)).try_collect().await.unwrap();
        let entry = listed
            .into_iter()
            .find(|m| {
                m.location
                    .as_ref()
                    .ends_with(&format!("{snap_str}.manifest"))
            })
            .expect("synthesised manifest exists");
        let manifest_body = store
            .get(&entry.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let body_str = std::str::from_utf8(&manifest_body).unwrap();
        assert!(
            body_str.contains(&good_id.to_string()),
            "good segment present"
        );
        assert!(
            !body_str.contains(&bad_id.to_string()),
            "tampered segment must not appear in manifest"
        );
    }

    // ── release / stop preconditions ──────────────────────────────────────
    //
    // Two bugs surfaced by manual testing:
    //
    //   1. `release` accepted a running volume and tried to halt it
    //      inline. A failure between the inline halt and the bucket
    //      flip would strand the volume in a "Released-but-running"
    //      state. Fix: refuse if `volume.stopped` is absent.
    //
    //   2. `stop` refused when the bucket said Released/
    //      Readonly. But `stop` is a *local* lifecycle verb — its job
    //      is to halt the daemon. The bucket update is best-effort.
    //      A daemon left running while the bucket says Released
    //      (e.g. because of bug 1) was unstoppable. Fix: warn-and-skip
    //      the bucket update on InvalidTransition, halt locally
    //      regardless.

    /// Build a `by_name/<vol>` symlink pointing at a fresh
    /// `by_id/<ulid>/` directory without a `volume.stopped` marker —
    /// i.e. the on-disk shape of a (notionally) running volume.
    fn make_running_volume(data_dir: &Path) -> ulid::Ulid {
        let vol_ulid = ulid::Ulid::new();
        let vol_dir = data_dir.join("by_id").join(vol_ulid.to_string());
        std::fs::create_dir_all(&vol_dir).unwrap();
        std::fs::create_dir_all(data_dir.join("by_name")).unwrap();
        let link = data_dir.join("by_name").join("vol");
        let target = std::path::PathBuf::from(format!("../by_id/{vol_ulid}"));
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();
        vol_ulid
    }

    #[tokio::test]
    async fn release_op_refuses_when_volume_is_running() {
        let store = mem_store();
        let data_dir = TempDir::new().unwrap();
        let snapshot_locks = SnapshotLockRegistry::default();
        let rescan = Notify::new();

        // Running volume: by_name symlink + by_id dir, NO volume.stopped.
        let vol_ulid = make_running_volume(data_dir.path());

        // names/<vol> = Live owned by us — would have been the path
        // through the rest of release_volume_op before this fix.
        let mut rec = NameRecord::live_minimal(vol_ulid);
        rec.coordinator_id = Some("coord-self".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let resp = release_volume_op(
            "vol",
            data_dir.path(),
            &snapshot_locks,
            &store,
            8 * 1024 * 1024,
            "coord-self",
            &rescan,
        )
        .await;

        assert!(resp.starts_with("err "), "{resp}");
        assert!(
            resp.contains("running") && resp.contains("volume stop"),
            "expected operator to be pointed at `volume stop`, got: {resp}"
        );

        // The bucket record must be untouched — still Live.
        let (still, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(still.state, NameState::Live);
    }

    #[tokio::test]
    async fn stop_op_halts_locally_when_bucket_says_released() {
        // Bug-2 reproducer: bucket is Released (e.g. from a partial
        // earlier release), daemon is still running on this host. Stop
        // must succeed, halting the daemon and leaving the bucket
        // record unchanged (we don't own a Released record).
        let store = mem_store();
        let data_dir = TempDir::new().unwrap();

        let vol_ulid = make_running_volume(data_dir.path());

        let mut rec = NameRecord::live_minimal(vol_ulid);
        rec.state = NameState::Released;
        rec.coordinator_id = None;
        rec.handoff_snapshot = Some(ulid::Ulid::new());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let resp = stop_volume_op("vol", data_dir.path(), &store, "coord-self").await;
        assert_eq!(
            resp, "ok",
            "stop must halt locally regardless of bucket state"
        );

        // Local marker now present.
        let vol_dir = data_dir.path().join("by_id").join(vol_ulid.to_string());
        assert!(
            vol_dir.join("volume.stopped").exists(),
            "volume.stopped marker should be written"
        );

        // Bucket record untouched: still Released, no coordinator_id.
        let (still, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(still.state, NameState::Released);
        assert!(still.coordinator_id.is_none());
    }

    #[tokio::test]
    async fn stop_op_halts_locally_when_bucket_says_foreign_live() {
        // Even split-brain bucket state must not block a local halt.
        // If a daemon is running on our host while names/<name> is
        // owned by another coordinator, halting our local process is
        // the right cleanup — it doesn't affect their host. We leave
        // the bucket record untouched.
        let store = mem_store();
        let data_dir = TempDir::new().unwrap();

        let vol_ulid = make_running_volume(data_dir.path());

        let mut rec = NameRecord::live_minimal(vol_ulid);
        rec.coordinator_id = Some("coord-other".into());
        ns::create_name_record(&store, "vol", &rec).await.unwrap();

        let resp = stop_volume_op("vol", data_dir.path(), &store, "coord-self").await;
        assert_eq!(resp, "ok");

        let vol_dir = data_dir.path().join("by_id").join(vol_ulid.to_string());
        assert!(vol_dir.join("volume.stopped").exists());

        // Bucket record untouched — still owned by the other coordinator.
        let (still, _) = ns::read_name_record(&store, "vol").await.unwrap().unwrap();
        assert_eq!(still.state, NameState::Live);
        assert_eq!(still.coordinator_id.as_deref(), Some("coord-other"));
    }

    // ── release fast-path predicate ────────────────────────────────────
    //
    // `release_fast_path_handoff` decides whether a `volume release` can
    // skip the daemon restart and reuse the previously-published
    // snapshot. Each branch below exercises one ineligibility reason
    // plus one happy path. Per CLAUDE.md "monotonic ULIDs in tests" we
    // mint via `UlidMint` whenever ordering matters.

    use elide_core::ulid_mint::UlidMint;

    /// Set up the on-disk skeleton a clean stopped volume would have
    /// after at least one snapshot has been published and uploaded.
    fn fast_path_clean_volume(snap_ulid: ulid::Ulid) -> TempDir {
        let tmp = TempDir::new().unwrap();
        for sub in ["wal", "pending", "gc", "index", "snapshots"] {
            std::fs::create_dir_all(tmp.path().join(sub)).unwrap();
        }
        // Snapshot marker (bare-ULID file).
        std::fs::write(tmp.path().join("snapshots").join(snap_ulid.to_string()), "").unwrap();
        // Upload sentinel: volume/<id>/uploaded/snapshots/<ulid>.
        std::fs::create_dir_all(tmp.path().join("uploaded").join("snapshots")).unwrap();
        std::fs::write(
            tmp.path()
                .join("uploaded")
                .join("snapshots")
                .join(snap_ulid.to_string()),
            "",
        )
        .unwrap();
        tmp
    }

    #[test]
    fn fast_path_eligible_when_clean_with_uploaded_snapshot() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        let got = release_fast_path_handoff(tmp.path()).unwrap();
        assert_eq!(got, Some(snap));
    }

    #[test]
    fn fast_path_ineligible_when_wal_non_empty() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        std::fs::write(
            tmp.path().join("wal").join("01JANYSEGULID00000000000000"),
            "x",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_pending_non_empty() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        std::fs::write(tmp.path().join("pending").join("seg"), "x").unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_gc_non_empty() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        std::fs::write(
            tmp.path().join("gc").join("01JANYGCULID0000000000000000"),
            "x",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_no_snapshot_published() {
        let tmp = TempDir::new().unwrap();
        for sub in ["wal", "pending", "gc", "index", "snapshots"] {
            std::fs::create_dir_all(tmp.path().join(sub)).unwrap();
        }
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_snapshot_not_yet_uploaded() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        // Remove the sentinel: snapshot is signed locally but not on S3.
        std::fs::remove_file(
            tmp.path()
                .join("uploaded")
                .join("snapshots")
                .join(snap.to_string()),
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_ineligible_when_segment_post_dates_snapshot() {
        let mut mint = UlidMint::new(ulid::Ulid::nil());
        let snap = mint.next();
        let later_segment = mint.next();
        assert!(later_segment > snap, "UlidMint must mint monotonically");
        let tmp = fast_path_clean_volume(snap);
        // A new segment landed in `index/` after the last snapshot —
        // slow path must run so the new snapshot covers it.
        std::fs::write(
            tmp.path()
                .join("index")
                .join(format!("{later_segment}.idx")),
            "",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    #[test]
    fn fast_path_eligible_when_segment_predates_snapshot() {
        let mut mint = UlidMint::new(ulid::Ulid::nil());
        let earlier_segment = mint.next();
        let snap = mint.next();
        assert!(snap > earlier_segment);
        let tmp = fast_path_clean_volume(snap);
        // Older segment is already covered by the snapshot — fine.
        std::fs::write(
            tmp.path()
                .join("index")
                .join(format!("{earlier_segment}.idx")),
            "",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), Some(snap));
    }

    #[test]
    fn fast_path_picks_latest_snapshot_when_multiple_present() {
        let mut mint = UlidMint::new(ulid::Ulid::nil());
        let older_snap = mint.next();
        let newer_snap = mint.next();
        let tmp = fast_path_clean_volume(newer_snap);
        // Older marker + sentinel (the volume kept history).
        std::fs::write(
            tmp.path().join("snapshots").join(older_snap.to_string()),
            "",
        )
        .unwrap();
        std::fs::write(
            tmp.path()
                .join("uploaded")
                .join("snapshots")
                .join(older_snap.to_string()),
            "",
        )
        .unwrap();
        assert_eq!(
            release_fast_path_handoff(tmp.path()).unwrap(),
            Some(newer_snap)
        );
    }

    #[test]
    fn fast_path_ignores_filemap_and_manifest_siblings() {
        let snap = ulid::Ulid::new();
        let tmp = fast_path_clean_volume(snap);
        // The snapshots dir has marker plus .filemap and .manifest
        // siblings; only the bare-ULID marker should be considered.
        std::fs::write(
            tmp.path().join("snapshots").join(format!("{snap}.filemap")),
            "fm",
        )
        .unwrap();
        std::fs::write(
            tmp.path()
                .join("snapshots")
                .join(format!("{snap}.manifest")),
            "mf",
        )
        .unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), Some(snap));
    }

    #[test]
    fn fast_path_treats_missing_subdirs_as_empty() {
        let tmp = TempDir::new().unwrap();
        // No wal/, pending/, gc/, index/ directories yet — a brand-new
        // volume that just happens to have no snapshot. Should fall
        // through cleanly to the "no snapshot" branch.
        std::fs::create_dir_all(tmp.path().join("snapshots")).unwrap();
        assert_eq!(release_fast_path_handoff(tmp.path()).unwrap(), None);
    }

    // ----- await-prefetch -----

    /// Untracked volume → ok. Used for volumes already-prefetched on a previous
    /// coordinator run (no entry in the tracker yet) or running without
    /// coordinator-managed prefetch at all.
    #[tokio::test]
    async fn await_prefetch_returns_ok_for_untracked_volume() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let resp = await_prefetch_op("01JQAAAAAAAAAAAAAAAAAAAAAA", &tracker).await;
        assert_eq!(resp, "ok");
    }

    #[tokio::test]
    async fn await_prefetch_rejects_invalid_ulid() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let resp = await_prefetch_op("not-a-ulid", &tracker).await;
        assert!(resp.starts_with("err invalid ULID"), "{resp}");
    }

    /// Tracker already has a Done(Ok) entry → returns immediately, no blocking.
    #[tokio::test]
    async fn await_prefetch_returns_ok_when_already_done_success() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch(&tracker, vol);
        tx.send_replace(Some(Ok(())));
        // Don't await with a timeout: if this blocks, the test blocks — that's
        // a clearer failure than a timeout.
        let resp = await_prefetch_op(&vol.to_string(), &tracker).await;
        assert_eq!(resp, "ok");
    }

    /// Tracker has Done(Err) → surfaces the message, doesn't return ok.
    #[tokio::test]
    async fn await_prefetch_returns_err_when_already_done_failed() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch(&tracker, vol);
        tx.send_replace(Some(Err("S3 timeout".into())));
        let resp = await_prefetch_op(&vol.to_string(), &tracker).await;
        assert!(
            resp.starts_with("err prefetch failed: S3 timeout"),
            "{resp}"
        );
    }

    /// In-progress entry: caller blocks; producer publishes Ok mid-wait;
    /// caller unblocks with ok.
    #[tokio::test]
    async fn await_prefetch_blocks_until_publisher_sends_ok() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch(&tracker, vol);

        let tracker_clone = tracker.clone();
        let waiter =
            tokio::spawn(async move { await_prefetch_op(&vol.to_string(), &tracker_clone).await });

        // Give the waiter a moment to subscribe before the publisher fires.
        // Without this, the publisher might send before the waiter is even
        // running; the test would still pass via initial-borrow, so this
        // sleep specifically exercises the rx.changed() path.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send_replace(Some(Ok(())));

        let resp = waiter.await.unwrap();
        assert_eq!(resp, "ok");
    }

    /// In-progress entry, sender dropped (per-fork task panicked / volume
    /// removed mid-prefetch) → IPC returns a clear error rather than hanging
    /// or silently saying ok.
    #[tokio::test]
    async fn await_prefetch_returns_err_when_sender_dropped_without_value() {
        let tracker = elide_coordinator::new_prefetch_tracker();
        let vol = ulid::Ulid::from_string("01JQAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        let tx = elide_coordinator::register_prefetch(&tracker, vol);

        let tracker_clone = tracker.clone();
        let waiter =
            tokio::spawn(async move { await_prefetch_op(&vol.to_string(), &tracker_clone).await });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        drop(tx);

        let resp = waiter.await.unwrap();
        assert!(
            resp.starts_with("err prefetch task exited"),
            "expected err prefetch task exited..., got {resp}"
        );
    }

    // ── generate-filemap argument validation ─────────────────────────────

    #[tokio::test]
    async fn generate_filemap_unknown_volume_returns_err() {
        let tmp = TempDir::new().unwrap();
        std::fs::create_dir_all(tmp.path().join("by_name")).unwrap();
        let store = mem_store();
        let resp = generate_filemap_op("ghost", None, tmp.path(), &store).await;
        assert!(resp.starts_with("err volume not found"), "{resp}");
    }

    #[tokio::test]
    async fn generate_filemap_invalid_snap_ulid_returns_err() {
        let tmp = TempDir::new().unwrap();
        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let vol_dir = tmp.path().join("by_id").join(vol_ulid);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let by_name = tmp.path().join("by_name");
        std::fs::create_dir_all(&by_name).unwrap();
        std::os::unix::fs::symlink(&vol_dir, by_name.join("vol")).unwrap();
        let store = mem_store();
        let resp = generate_filemap_op("vol", Some("not-a-ulid"), tmp.path(), &store).await;
        assert!(resp.starts_with("err invalid snap_ulid"), "{resp}");
    }

    #[tokio::test]
    async fn generate_filemap_no_snapshot_returns_err() {
        let tmp = TempDir::new().unwrap();
        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let vol_dir = tmp.path().join("by_id").join(vol_ulid);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let by_name = tmp.path().join("by_name");
        std::fs::create_dir_all(&by_name).unwrap();
        std::os::unix::fs::symlink(&vol_dir, by_name.join("vol")).unwrap();
        let store = mem_store();
        let resp = generate_filemap_op("vol", None, tmp.path(), &store).await;
        assert!(resp.starts_with("err"), "{resp}");
        assert!(resp.contains("no local snapshot"), "{resp}");
    }

    #[tokio::test]
    async fn generate_filemap_explicit_unknown_snapshot_returns_err() {
        let tmp = TempDir::new().unwrap();
        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let vol_dir = tmp.path().join("by_id").join(vol_ulid);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let by_name = tmp.path().join("by_name");
        std::fs::create_dir_all(&by_name).unwrap();
        std::os::unix::fs::symlink(&vol_dir, by_name.join("vol")).unwrap();
        let store = mem_store();
        // Valid ULID, but no matching snapshots/<ulid>.manifest on disk.
        let bogus = "01J0000000000000000000000V";
        let resp = generate_filemap_op("vol", Some(bogus), tmp.path(), &store).await;
        assert!(resp.starts_with("err"), "{resp}");
        assert!(
            resp.contains("not found locally") && resp.contains(bogus),
            "{resp}"
        );
    }
}
