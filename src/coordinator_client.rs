// Client for the coordinator inbound socket.
//
// Connects to control.sock, sends one command, reads one response line.
// A new connection is made per call — the protocol is request-response-close.
//
// Exception: `import_attach_by_name` reads multiple lines until a terminal line.

use std::io::{self, BufRead, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use serde::Deserialize;

fn call(socket_path: &Path, cmd: &str) -> io::Result<String> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    writeln!(stream, "{cmd}")?;
    stream.flush()?;
    // Ignore ENOTCONN: the coordinator may have already closed its end by the
    // time we shut down our write half, which is harmless.
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut reader = io::BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line.trim().to_owned())
}

/// Like `call`, but reads the entire response body until EOF — for commands
/// that return a multi-line TOML body (`get-store-config`, `get-store-creds`).
fn call_all(socket_path: &Path, cmd: &str) -> io::Result<String> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    writeln!(stream, "{cmd}")?;
    stream.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut buf = String::new();
    stream.read_to_string(&mut buf)?;
    Ok(buf)
}

/// Non-secret store config as served by the coordinator's `get-store-config`
/// command. Either `local_path` is set, or `bucket` is set (with optional
/// `endpoint` / `region`).
#[derive(Debug, Deserialize)]
pub struct StoreConfig {
    pub local_path: Option<String>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

/// Short-lived store credentials as served by `get-store-creds`. Today these
/// are the coordinator's long-lived AWS env creds; the same shape will carry
/// macaroon-scoped tokens in the future.
#[derive(Debug, Deserialize)]
pub struct StoreCreds {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Parse a `get-store-*` response: `ok\n<TOML>\n…` or `err <message>\n`.
fn parse_toml_response<T: for<'de> Deserialize<'de>>(raw: &str) -> io::Result<T> {
    let trimmed = raw.trim_start();
    if let Some(body) = trimmed
        .strip_prefix("ok\n")
        .or_else(|| trimmed.strip_prefix("ok\r\n"))
    {
        toml::from_str(body).map_err(|e| io::Error::other(format!("parse response: {e}")))
    } else if let Some(msg) = trimmed.strip_prefix("err ") {
        Err(io::Error::other(msg.trim().to_owned()))
    } else {
        Err(io::Error::other(format!(
            "unexpected response shape: {}",
            trimmed.lines().next().unwrap_or("")
        )))
    }
}

/// Returns true if the coordinator is currently listening on `socket_path`.
///
/// A successful `connect()` is sufficient evidence — we don't send a command
/// (no waste of an inbound slot) and we don't need to interpret a response.
/// Failure modes that mean "down": socket file missing, ECONNREFUSED (stale
/// socket file but no listener), or any other connect error.
pub fn is_reachable(socket_path: &Path) -> bool {
    UnixStream::connect(socket_path).is_ok()
}

/// Ask the coordinator for its non-secret store config (bucket, endpoint,
/// region, or local_path). This is the static counterpart to
/// `get_store_creds`.
pub fn get_store_config(socket_path: &Path) -> io::Result<StoreConfig> {
    let raw = call_all(socket_path, "get-store-config")?;
    parse_toml_response(&raw)
}

/// Ask the coordinator for S3 credentials (long-lived today, macaroon-scoped
/// later). Errors if the coordinator's env has no credentials configured.
pub fn get_store_creds(socket_path: &Path) -> io::Result<StoreCreds> {
    let raw = call_all(socket_path, "get-store-creds")?;
    parse_toml_response(&raw)
}

/// Mint a per-volume macaroon for the spawned volume process.
///
/// The coordinator authenticates the request via SO_PEERCRED on the
/// connecting socket and refuses if the peer pid does not match the
/// volume's recorded `volume.pid`. There is a brief window after spawn
/// where the parent has not yet written `volume.pid`; the caller in
/// `register_and_get_creds` retries on that condition.
pub fn register_volume(socket_path: &Path, volume_ulid: &str) -> io::Result<String> {
    let resp = call(socket_path, &format!("register {volume_ulid}"))?;
    if let Some(macaroon) = resp.strip_prefix("ok ") {
        Ok(macaroon.to_owned())
    } else if let Some(msg) = resp.strip_prefix("err ") {
        Err(io::Error::other(msg.to_owned()))
    } else {
        Err(io::Error::other(format!("unexpected response: {resp}")))
    }
}

/// Exchange a registered macaroon for short-lived S3 credentials.
///
/// Coordinator re-checks SO_PEERCRED against the macaroon's `pid` caveat
/// and the volume's recorded `volume.pid` before delegating to the
/// configured `CredentialIssuer`.
pub fn macaroon_credentials(socket_path: &Path, macaroon: &str) -> io::Result<StoreCreds> {
    let raw = call_all(socket_path, &format!("credentials {macaroon}"))?;
    parse_toml_response(&raw)
}

/// Combined registration + credential fetch with retry.
///
/// The supervisor writes `volume.pid` after `Command::spawn()` returns,
/// which can race against a fast-starting volume reaching this code path
/// before the parent has flushed the pid file. We retry a handful of
/// times with short backoff to absorb that window.
pub fn register_and_get_creds(socket_path: &Path, volume_ulid: &str) -> io::Result<StoreCreds> {
    const MAX_ATTEMPTS: u32 = 10;
    const BACKOFF_MS: u64 = 50;
    let mut last_err: Option<io::Error> = None;
    for _ in 0..MAX_ATTEMPTS {
        match register_volume(socket_path, volume_ulid) {
            Ok(m) => return macaroon_credentials(socket_path, &m),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not registered") || msg.contains("does not match") {
                    last_err = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(BACKOFF_MS));
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("register: exhausted retries")))
}

/// Trigger an immediate fork discovery pass.
pub fn rescan(socket_path: &Path) -> io::Result<()> {
    let resp = call(socket_path, "rescan")?;
    if resp == "ok" {
        Ok(())
    } else {
        Err(io::Error::other(format!("unexpected response: {resp}")))
    }
}

/// Query the running state of a named volume.
pub fn status(socket_path: &Path, volume: &str) -> io::Result<String> {
    call(socket_path, &format!("status {volume}"))
}

/// Authoritative bucket-side status of a named volume, plus this
/// coordinator's eligibility to act on it. Reaches into S3 — only
/// invoked when the operator passes `volume status --remote`.
#[derive(Debug, Deserialize)]
pub struct RemoteStatus {
    pub state: String,
    pub vol_ulid: String,
    #[serde(default)]
    pub coordinator_id: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub claimed_at: Option<String>,
    #[serde(default)]
    pub parent: Option<String>,
    #[serde(default)]
    pub handoff_snapshot: Option<String>,
    pub eligibility: String,
}

/// Fetch `names/<volume>` from the bucket via the coordinator and parse
/// the authoritative record. Returns an error when the name is absent
/// from the bucket or the coordinator cannot reach S3.
pub fn status_remote(socket_path: &Path, volume: &str) -> io::Result<RemoteStatus> {
    let raw = call_all(socket_path, &format!("status-remote {volume}"))?;
    parse_toml_response(&raw)
}

/// Ask the coordinator to start an OCI import.
/// Returns the import ULID on success (internal; callers use the volume name).
///
/// `extents_from` is a list of volume names whose extent indices should
/// populate the new volume's hash pool (dedup/delta source). The coordinator
/// validates each name and writes the flat `volume.extent_index` file.
pub fn import_start(
    socket_path: &Path,
    name: &str,
    oci_ref: &str,
    extents_from: &[String],
) -> io::Result<()> {
    let line = if extents_from.is_empty() {
        format!("import {name} {oci_ref}")
    } else {
        // Volume names are [a-zA-Z0-9._-] so comma is a safe separator.
        let joined = extents_from.join(",");
        format!("import {name} {oci_ref} extents:{joined}")
    };
    let resp = call(socket_path, &line)?;
    match resp.split_once(' ') {
        Some(("ok", _ulid)) => Ok(()),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Poll the state of an import job by volume name.
/// Returns the raw state string (`running`, `done`, or an error).
pub fn import_status_by_name(socket_path: &Path, name: &str) -> io::Result<String> {
    call(socket_path, &format!("import status {name}"))
}

/// Stream import output to `out` until the import completes, identified by volume name.
///
/// Reads lines from the coordinator until a terminal `ok done` or `err ...`
/// line is received. Each output line is written to `out`. Returns Ok(()) on
/// success, Err on import failure or I/O error.
pub fn import_attach_by_name(
    socket_path: &Path,
    name: &str,
    out: &mut dyn Write,
) -> io::Result<()> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    writeln!(stream, "import attach {name}")?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut reader = io::BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if trimmed == "ok done" {
            return Ok(());
        }
        if let Some(msg) = trimmed.strip_prefix("err ") {
            return Err(io::Error::other(msg.to_owned()));
        }
        writeln!(out, "{trimmed}")?;
    }
    Ok(())
}

/// Evict S3-confirmed segment bodies from a volume's cache/ directory.
///
/// Routed through the coordinator so eviction is sequenced between drain/GC
/// ticks and never races with the GC pass's collect_stats → compact_segments
/// window.
///
/// If `ulid` is `Some`, evicts only that segment. If `None`, evicts all
/// S3-confirmed bodies.
///
/// Returns the number of segments evicted.
pub fn evict_volume(socket_path: &Path, name: &str, ulid: Option<&str>) -> io::Result<usize> {
    let cmd = match ulid {
        Some(u) => format!("evict {name} {u}"),
        None => format!("evict {name}"),
    };
    let resp = call(socket_path, &cmd)?;
    match resp.split_once(' ') {
        Some(("ok", n)) => n
            .parse::<usize>()
            .map_err(|e| io::Error::other(format!("unexpected response: {resp}: {e}"))),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Trigger a coordinator-orchestrated snapshot of a running volume.
///
/// The coordinator acquires the per-volume snapshot lock, flushes the WAL
/// via the volume control socket, runs the inline drain, triggers the
/// signed `.manifest` file, and uploads the snapshot files. The CLI
/// only blocks on the reply.
///
/// Returns the snapshot ULID on success.
pub fn snapshot_volume(socket_path: &Path, name: &str) -> io::Result<String> {
    let resp = call(socket_path, &format!("snapshot {name}"))?;
    match resp.split_once(' ') {
        Some(("ok", ulid)) => Ok(ulid.trim().to_owned()),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Stats returned by `reclaim_volume`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReclaimStats {
    /// Number of scanner candidates the volume considered.
    pub candidates_scanned: usize,
    /// Total contiguous runs rewritten via internal-origin writes.
    pub runs_rewritten: u64,
    /// Total bytes committed to fresh compact entries.
    pub bytes_rewritten: u64,
    /// Number of candidates whose phase-3 commit discarded due to
    /// concurrent mutation. Not an error — they can be retried.
    pub discarded: usize,
}

/// Trigger an alias-merge extent reclamation pass on a running volume.
///
/// The coordinator resolves the volume name to its fork directory and
/// relays a `reclaim` IPC to the volume's control.sock. The volume
/// scans its current snapshot for bloated-hash candidates and reclaims
/// them most-wasteful-first using default thresholds. Heavy work runs
/// off the actor thread, so NBD writes are not blocked for the full
/// duration of the pass.
pub fn reclaim_volume(socket_path: &Path, name: &str) -> io::Result<ReclaimStats> {
    let resp = call(socket_path, &format!("reclaim {name}"))?;
    let rest = match resp.strip_prefix("ok ") {
        Some(r) => r,
        None => match resp.strip_prefix("err ") {
            Some(msg) => return Err(io::Error::other(msg.to_owned())),
            None => return Err(io::Error::other(format!("unexpected response: {resp}"))),
        },
    };
    let mut parts = rest.splitn(4, ' ');
    let parse_err = |field: &str| io::Error::other(format!("malformed reclaim response: {field}"));
    let candidates_scanned: usize = parts
        .next()
        .ok_or_else(|| parse_err("candidates"))?
        .parse()
        .map_err(|_| parse_err("candidates"))?;
    let runs_rewritten: u64 = parts
        .next()
        .ok_or_else(|| parse_err("runs"))?
        .parse()
        .map_err(|_| parse_err("runs"))?;
    let bytes_rewritten: u64 = parts
        .next()
        .ok_or_else(|| parse_err("bytes"))?
        .parse()
        .map_err(|_| parse_err("bytes"))?;
    let discarded: usize = parts
        .next()
        .ok_or_else(|| parse_err("discarded"))?
        .trim()
        .parse()
        .map_err(|_| parse_err("discarded"))?;
    Ok(ReclaimStats {
        candidates_scanned,
        runs_rewritten,
        bytes_rewritten,
        discarded,
    })
}

/// Stop all processes for a volume and remove its directory.
pub fn delete_volume(socket_path: &Path, name: &str) -> io::Result<()> {
    let resp = call(socket_path, &format!("delete {name}"))?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ if resp == "ok" => Ok(()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Stop a volume's supervised process. Coordinator writes the stopped marker
/// and forwards `shutdown` to the volume's control socket. Both the marker
/// file and that socket are root-owned when the coordinator runs under sudo,
/// so this path is what makes `volume stop` work for non-root CLI callers.
pub fn stop_volume(socket_path: &Path, name: &str) -> io::Result<()> {
    let resp = call(socket_path, &format!("stop {name}"))?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ if resp == "ok" => Ok(()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Outcome of a `resolve_handoff_key` call.
///
/// Drives the claim-from-released path's choice of `parent-key` for
/// the new fork's `volume.provenance`. For `Normal` manifests the
/// CLI uses the source volume's own `volume.pub`; for `Recovery` it
/// embeds the recovering coordinator's pubkey so the new fork's
/// open-time ancestor walk verifies the synthesised handoff snapshot
/// against the right key.
#[derive(Debug, Clone)]
pub enum HandoffKey {
    /// Manifest is signed by the source volume's own key.
    Normal,
    /// Manifest is a synthesised handoff snapshot signed by a
    /// recovering coordinator. The carried hex string is the
    /// already-verified Ed25519 pubkey, ready to pass to
    /// `fork-create` as `parent-key=<hex>`.
    Recovery { manifest_pubkey_hex: String },
}

/// Ask the coordinator to resolve which Ed25519 key the snapshot
/// manifest at `by_id/<vol_ulid>/snapshots/.../<snap_ulid>.manifest`
/// is signed by. Returns `HandoffKey::Recovery { hex }` for
/// synthesised handoff snapshots minted by `volume release --force`,
/// or `HandoffKey::Normal` for ordinary manifests.
///
/// The coordinator verifies the manifest signature and the
/// recovering coordinator's pub binding before returning a
/// `Recovery` result, so the CLI can use the returned hex as a
/// trusted `parent-key` for `fork-create`.
pub fn resolve_handoff_key(
    socket_path: &Path,
    vol_ulid: &str,
    snap_ulid: &str,
) -> io::Result<HandoffKey> {
    let resp = call(
        socket_path,
        &format!("resolve-handoff-key {vol_ulid} {snap_ulid}"),
    )?;
    if resp == "ok normal" {
        return Ok(HandoffKey::Normal);
    }
    if let Some(rest) = resp.strip_prefix("ok recovery ") {
        return Ok(HandoffKey::Recovery {
            manifest_pubkey_hex: rest.trim().to_owned(),
        });
    }
    if let Some(msg) = resp.strip_prefix("err ") {
        return Err(io::Error::other(msg.to_owned()));
    }
    Err(io::Error::other(format!("unexpected response: {resp}")))
}

/// Options for `release_volume`.
#[derive(Default, Clone, Debug)]
pub struct ReleaseOpts<'a> {
    /// `--force`: override foreign ownership of `names/<name>`.
    /// Skips the local drain (the dead owner's WAL is unreachable),
    /// synthesises a handoff snapshot from S3-visible segments, signs
    /// it with the local coordinator's identity key, and
    /// unconditionally flips `names/<name>` to `released` (or
    /// `reserved` if `to` is set).
    pub force: bool,
    /// `--to <coord_id>`: targeted handoff. Final state is `reserved`
    /// for the named coordinator instead of `released`. Composes with
    /// `force`.
    pub to: Option<&'a str>,
}

/// Release a volume's name back to the pool so any other coordinator can
/// claim it via `volume start`. Drains WAL, publishes a handoff snapshot,
/// halts the daemon, and flips `names/<name>` to `state=released` with
/// the snapshot ULID recorded so the next claimant can fork from it.
/// Returns the handoff snapshot ULID on success.
///
/// With `opts.force`, skips ownership and drain (used when the
/// previous owner is unreachable). With `opts.to`, the final state is
/// `reserved` for the named coordinator. Both compose.
pub fn release_volume(socket_path: &Path, name: &str, opts: ReleaseOpts<'_>) -> io::Result<String> {
    let mut cmd = format!("release {name}");
    if opts.force {
        cmd.push_str(" --force");
    }
    if let Some(target) = opts.to {
        cmd.push_str(&format!(" --to {target}"));
    }
    let resp = call(socket_path, &cmd)?;
    match resp.split_once(' ') {
        Some(("ok", snap)) => Ok(snap.trim().to_owned()),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Outcome of a `start_volume` call. The coordinator routes by the
/// bucket-side `names/<name>` state; the response distinguishes the
/// local-only path from the claim-from-released path that needs CLI
/// orchestration.
pub enum StartOutcome {
    /// Daemon was started locally (state was Stopped, ours; or Live, ours;
    /// or no S3 record yet).
    Started,
    /// Name is in `Released` state — the CLI needs to orchestrate fork
    /// creation, then call `claim_volume` with the new ULID.
    NeedsClaim {
        released_vol_ulid: String,
        handoff_snapshot: Option<String>,
    },
}

/// Start a volume.
///
/// Defaults to **local-only**: if `<name>` has no local state on the
/// coordinator's data dir, returns an error pointing the operator at
/// `--remote`. With `remote = true`, the IPC reads `names/<name>` from
/// the bucket; if the record is `Released` (or `Reserved` for this
/// coordinator), returns `NeedsClaim` so the CLI can pull, fork, and
/// claim. Bucket reads only happen when `remote` is set.
pub fn start_volume(socket_path: &Path, name: &str, remote: bool) -> io::Result<StartOutcome> {
    let cmd = if remote {
        format!("start {name} --remote")
    } else {
        format!("start {name}")
    };
    let resp = call(socket_path, &cmd)?;
    if resp == "ok" {
        return Ok(StartOutcome::Started);
    }
    if let Some(rest) = resp.strip_prefix("released ") {
        let mut parts = rest.split_whitespace();
        let vol = parts
            .next()
            .ok_or_else(|| io::Error::other(format!("malformed released response: {resp}")))?;
        let snap = parts.next();
        return Ok(StartOutcome::NeedsClaim {
            released_vol_ulid: vol.to_owned(),
            handoff_snapshot: snap.filter(|s| *s != "_").map(str::to_owned),
        });
    }
    if let Some(msg) = resp.strip_prefix("err ") {
        return Err(io::Error::other(msg.to_owned()));
    }
    Err(io::Error::other(format!("unexpected response: {resp}")))
}

/// Atomically rebind a `Released` name to a freshly-minted local fork.
/// The CLI must have already materialised `by_id/<new_vol_ulid>/` before
/// calling this. Returns the new ULID on success.
pub fn claim_volume(socket_path: &Path, name: &str, new_vol_ulid: &str) -> io::Result<String> {
    let resp = call(socket_path, &format!("claim {name} {new_vol_ulid}"))?;
    match resp.split_once(' ') {
        Some(("ok", ulid)) => Ok(ulid.trim().to_owned()),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Create a fresh writable volume. Returns the new volume's ULID. Routes
/// through the coordinator so a sudo'd coordinator can create root-owned
/// state on behalf of a non-root CLI invocation.
pub fn create_volume_remote(
    socket_path: &Path,
    name: &str,
    size_bytes: u64,
    flags: &[String],
) -> io::Result<String> {
    let mut line = format!("create {name} {size_bytes}");
    for f in flags {
        line.push(' ');
        line.push_str(f);
    }
    let resp = call(socket_path, &line)?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        Some(("ok", ulid)) => Ok(ulid.to_owned()),
        _ if resp == "ok" => Ok(String::new()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Update transport configuration on an existing volume. Returns "restarting"
/// if the running volume process was signalled to pick up the new config, or
/// "not-running" if it wasn't running.
pub fn update_volume(socket_path: &Path, name: &str, flags: &[String]) -> io::Result<String> {
    let mut line = format!("update {name}");
    for f in flags {
        line.push(' ');
        line.push_str(f);
    }
    let resp = call(socket_path, &line)?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        Some(("ok", note)) => Ok(note.to_owned()),
        _ if resp == "ok" => Ok(String::new()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Pull one readonly ancestor from the store via the coordinator.
/// Returns the parent ULID (for the ancestor walk), or `None` for a root.
pub fn pull_readonly(socket_path: &Path, vol_ulid: &str) -> io::Result<Option<String>> {
    let resp = call(socket_path, &format!("pull-readonly {vol_ulid}"))?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        Some(("ok", parent)) if !parent.is_empty() => Ok(Some(parent.to_owned())),
        _ if resp == "ok" => Ok(None),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Synthesize a "now" snapshot for a readonly source. Returns
/// `(snap_ulid, ephemeral_pubkey_hex)`.
pub fn force_snapshot_now(socket_path: &Path, vol_ulid: &str) -> io::Result<(String, String)> {
    let resp = call(socket_path, &format!("force-snapshot-now {vol_ulid}"))?;
    if let Some(("err", msg)) = resp.split_once(' ') {
        return Err(io::Error::other(msg.to_owned()));
    }
    let mut parts = resp.split_whitespace();
    match (parts.next(), parts.next(), parts.next()) {
        (Some("ok"), Some(snap), Some(pubkey)) => Ok((snap.to_owned(), pubkey.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Fork a new writable volume from a local source. Returns the new volume ULID.
pub fn fork_create(
    socket_path: &Path,
    new_name: &str,
    source_ulid: &str,
    snap: Option<&str>,
    parent_key_hex: Option<&str>,
    flags: &[String],
    for_claim: bool,
) -> io::Result<String> {
    let mut line = format!("fork-create {new_name} {source_ulid}");
    if let Some(s) = snap {
        line.push_str(&format!(" snap={s}"));
    }
    if let Some(k) = parent_key_hex {
        line.push_str(&format!(" parent-key={k}"));
    }
    if for_claim {
        // Tell the coordinator to skip the `mark_initial` step: this
        // fork-create is the materialise step of a claim-from-
        // released flow. The CLI's subsequent `claim` IPC rebinds the
        // existing `Released` record to the new fork.
        line.push_str(" for-claim");
    }
    for f in flags {
        line.push(' ');
        line.push_str(f);
    }
    let resp = call(socket_path, &line)?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        Some(("ok", ulid)) => Ok(ulid.to_owned()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}
