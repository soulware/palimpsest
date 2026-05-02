// Client for the coordinator inbound socket.
//
// Connects to control.sock, sends one typed JSON request, reads one or
// more typed JSON envelopes. A new connection is made per call — the
// protocol is request-response-close.
//
// Exception: `import_attach_by_name` reads a sequence of typed
// `ImportAttachEvent` envelopes until the import terminates.

use std::io::{self, BufRead, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use elide_coordinator::ipc::{Envelope, IpcError, Request};
use serde::Deserialize;

pub use elide_coordinator::ipc::{
    ClaimReply, CreateReply, EvictReply, ForceSnapshotNowReply, ForkAttachEvent, ForkCreateReply,
    ForkSource, ForkStartReply, GenerateFilemapReply, ImportAttachEvent, ImportStartReply,
    ImportStatusReply, IpcErrorKind, LatestSnapshotReply, PullReadonlyReply, RegisterReply,
    ReleaseReply, ResolveHandoffKeyReply, ResolveNameReply, SignatureStatus, SnapshotReply,
    StatusRemoteReply, StatusReply, StoreConfigReply, StoreCredsReply, UpdateReply,
    VolumeEventEntry, VolumeEventsReply,
};

/// Public-API aliases for the client side. The underlying types live
/// in `elide_coordinator::ipc` so server and client share the wire
/// shape; the aliases preserve the `coordinator_client::StoreConfig` /
/// `StoreCreds` names used by the volume-subprocess macaroon flow.
pub type StoreConfig = StoreConfigReply;
pub type StoreCreds = StoreCredsReply;
pub use elide_coordinator::volume_state::VolumeLifecycle;

/// Send a typed JSON request and parse a typed JSON reply.
///
/// Returns:
/// - `Ok(Ok(reply))` — server returned `Envelope::Ok`.
/// - `Ok(Err(IpcError))` — server returned a typed error.
/// - `Err(io::Error)` — transport failure (socket missing, bad JSON, etc.).
fn call_typed<R>(socket_path: &Path, request: &Request) -> io::Result<Result<R, IpcError>>
where
    R: for<'de> Deserialize<'de>,
{
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    let line = serde_json::to_string(request)
        .map_err(|e| io::Error::other(format!("encode request: {e}")))?;
    writeln!(stream, "{line}")?;
    stream.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut reader = io::BufReader::new(stream);
    let mut response_line = String::new();
    reader.read_line(&mut response_line)?;
    let env: Envelope<R> = serde_json::from_str(response_line.trim())
        .map_err(|e| io::Error::other(format!("parse reply: {e}")))?;
    Ok(env.into_result())
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
/// region, or local_path). Used by spawned volume subprocesses (over the
/// macaroon handshake) to build an object_store that matches the
/// coordinator's view; the CLI itself never builds an object_store.
pub fn get_store_config(socket_path: &Path) -> io::Result<StoreConfig> {
    call_typed(socket_path, &Request::GetStoreConfig)?.map_err(io::Error::other)
}

/// Resolve a volume name to its current `vol_ulid` via the coordinator.
/// The coordinator performs the bucket-side `names/<name>` lookup; the CLI
/// holds no S3 credentials.
pub fn resolve_name(socket_path: &Path, name: &str) -> io::Result<ulid::Ulid> {
    let reply: ResolveNameReply = call_typed(
        socket_path,
        &Request::ResolveName {
            name: name.to_owned(),
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.vol_ulid)
}

/// Return the highest snapshot ULID under `by_id/<vol_ulid>/snapshots/`,
/// or `None` if the volume has no snapshots in the store.
pub fn latest_snapshot(socket_path: &Path, vol_ulid: ulid::Ulid) -> io::Result<Option<ulid::Ulid>> {
    let reply: LatestSnapshotReply =
        call_typed(socket_path, &Request::LatestSnapshot { vol_ulid })?
            .map_err(io::Error::other)?;
    Ok(reply.snapshot_ulid)
}

/// Default budget for [`await_prefetch`]. Longer than `OPEN_RETRY_BUDGET`
/// because prefetch can include real S3 GETs across deep ancestor chains;
/// short enough that a stalled coordinator surfaces a loud error before
/// the supervisor's restart loop fires.
pub const PREFETCH_AWAIT_BUDGET: Duration = Duration::from_secs(60);

/// Block until the coordinator's per-fork prefetch task publishes a
/// terminal result, or until `budget` expires.
///
/// Returns `Ok(())` when prefetch is complete (or untracked, which is
/// treated as ready). Returns `Err` when prefetch failed, when the
/// coordinator is unreachable, or on timeout. Volume binaries call this
/// before `Volume::open` to absorb the race between the coordinator's
/// prefetch task and the supervisor spawning this process on a freshly
/// claimed fork.
pub fn await_prefetch(socket_path: &Path, vol_ulid: &str, budget: Duration) -> io::Result<()> {
    let parsed = ulid::Ulid::from_string(vol_ulid)
        .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
    let stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    // Bound the full request including the coordinator's wait on the
    // per-fork watch channel. Read timeout fires as `WouldBlock` /
    // `TimedOut`; surface that as a clear error so the supervisor can
    // log + retry rather than the volume hanging on a stuck prefetch.
    stream
        .set_read_timeout(Some(budget))
        .map_err(|e| io::Error::other(format!("set read timeout: {e}")))?;
    stream
        .set_write_timeout(Some(budget))
        .map_err(|e| io::Error::other(format!("set write timeout: {e}")))?;
    let request = Request::AwaitPrefetch { vol_ulid: parsed };
    let line = serde_json::to_string(&request)
        .map_err(|e| io::Error::other(format!("encode request: {e}")))?;
    let mut writer = &stream;
    writeln!(writer, "{line}")?;
    writer.flush()?;
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut reader = io::BufReader::new(&stream);
    let mut response_line = String::new();
    match reader.read_line(&mut response_line) {
        Ok(_) => {}
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ) =>
        {
            return Err(io::Error::other(format!(
                "await-prefetch: coordinator did not respond within {budget:?}"
            )));
        }
        Err(e) => return Err(e),
    }
    let env: Envelope<()> = serde_json::from_str(response_line.trim())
        .map_err(|e| io::Error::other(format!("parse reply: {e}")))?;
    env.into_result()
        .map_err(|e| io::Error::other(format!("await-prefetch: {e}")))
}

/// Mint a per-volume macaroon for the spawned volume process.
///
/// The coordinator authenticates the request via SO_PEERCRED on the
/// connecting socket and refuses if the peer pid does not match the
/// volume's recorded `volume.pid`. There is a brief window after spawn
/// where the parent has not yet written `volume.pid`; the caller in
/// `register_and_get_creds` retries on that condition.
pub fn register_volume(socket_path: &Path, volume_ulid: &str) -> io::Result<String> {
    let parsed = ulid::Ulid::from_string(volume_ulid)
        .map_err(|e| io::Error::other(format!("invalid volume_ulid: {e}")))?;
    let reply: RegisterReply = call_typed(
        socket_path,
        &Request::Register {
            volume_ulid: parsed,
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.macaroon)
}

/// Exchange a registered macaroon for short-lived S3 credentials.
///
/// Coordinator re-checks SO_PEERCRED against the macaroon's `pid` caveat
/// and the volume's recorded `volume.pid` before delegating to the
/// configured `CredentialIssuer`.
pub fn macaroon_credentials(socket_path: &Path, macaroon: &str) -> io::Result<StoreCreds> {
    call_typed(
        socket_path,
        &Request::Credentials {
            macaroon: macaroon.to_owned(),
        },
    )?
    .map_err(io::Error::other)
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
    call_typed::<()>(socket_path, &Request::Rescan)?.map_err(io::Error::other)
}

/// Query the running state of a named volume.
pub fn status(socket_path: &Path, volume: &str) -> io::Result<StatusReply> {
    call_typed(
        socket_path,
        &Request::Status {
            volume: volume.to_owned(),
        },
    )?
    .map_err(io::Error::other)
}

/// Fetch `names/<volume>` from the bucket via the coordinator and parse
/// the authoritative record. Returns an error when the name is absent
/// from the bucket or the coordinator cannot reach S3.
pub fn status_remote(socket_path: &Path, volume: &str) -> io::Result<StatusRemoteReply> {
    call_typed(
        socket_path,
        &Request::StatusRemote {
            volume: volume.to_owned(),
        },
    )?
    .map_err(io::Error::other)
}

/// List the per-name event log under `events/<volume>/`.
/// Each entry includes a `signature_status` reporting whether the
/// emitting coordinator's pubkey was reachable and the signature
/// verified.
pub fn volume_events(socket_path: &Path, volume: &str) -> io::Result<VolumeEventsReply> {
    call_typed(
        socket_path,
        &Request::VolumeEvents {
            volume: volume.to_owned(),
        },
    )?
    .map_err(io::Error::other)
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
) -> io::Result<ImportStartReply> {
    call_typed(
        socket_path,
        &Request::ImportStart {
            volume: name.to_owned(),
            oci_ref: oci_ref.to_owned(),
            extents_from: extents_from.to_vec(),
        },
    )?
    .map_err(io::Error::other)
}

/// Poll the state of an import job by volume name. `Ok(reply)` carries
/// the import state; failures (no active import, import failed) come
/// back as `Err`.
pub fn import_status_by_name(socket_path: &Path, name: &str) -> io::Result<ImportStatusReply> {
    call_typed(
        socket_path,
        &Request::ImportStatus {
            volume: name.to_owned(),
        },
    )?
    .map_err(io::Error::other)
}

/// Stream import output to `out` until the import completes, identified by volume name.
///
/// Reads typed [`ImportAttachEvent`] envelopes from the coordinator
/// until a terminal `Done` event or an `Envelope::Err` is received.
/// Each `Line` event's content is written to `out`. Returns `Ok(())`
/// on success, `Err` on import failure or I/O error.
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
    let request = Request::ImportAttach {
        volume: name.to_owned(),
    };
    let line = serde_json::to_string(&request)
        .map_err(|e| io::Error::other(format!("encode request: {e}")))?;
    writeln!(stream, "{line}")?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut reader = io::BufReader::new(stream);
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            // EOF before terminal — treat as transport failure.
            return Err(io::Error::other(
                "import stream closed before terminal event",
            ));
        }
        let env: Envelope<ImportAttachEvent> = serde_json::from_str(buf.trim())
            .map_err(|e| io::Error::other(format!("parse import event: {e}")))?;
        match env.into_result() {
            Ok(ImportAttachEvent::Line { content }) => {
                writeln!(out, "{content}")?;
            }
            Ok(ImportAttachEvent::Done) => return Ok(()),
            Err(e) => return Err(io::Error::other(e)),
        }
    }
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
    let segment_ulid = match ulid {
        Some(s) => Some(
            ulid::Ulid::from_string(s)
                .map_err(|e| io::Error::other(format!("invalid segment ulid: {e}")))?,
        ),
        None => None,
    };
    let reply: EvictReply = call_typed(
        socket_path,
        &Request::Evict {
            volume: name.to_owned(),
            segment_ulid,
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.evicted)
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
    let reply: SnapshotReply = call_typed(
        socket_path,
        &Request::Snapshot {
            volume: name.to_owned(),
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.snap_ulid.to_string())
}

/// Generate `snapshots/<snap_ulid>.filemap` for a local volume.
///
/// `snap_ulid` defaults to the volume's latest local snapshot when
/// omitted. The coordinator opens the sealed snapshot (with demand-fetch
/// against S3 for evicted segments), walks the ext4 layout, and writes
/// the filemap. Used to seed delta-compression sources for subsequent
/// `volume import --extents-from <name>` invocations.
///
/// Returns the snapshot ULID on success.
pub fn generate_filemap(
    socket_path: &Path,
    name: &str,
    snap_ulid: Option<&str>,
) -> io::Result<String> {
    let snap_ulid = match snap_ulid {
        Some(s) => Some(
            ulid::Ulid::from_string(s)
                .map_err(|e| io::Error::other(format!("invalid snap ulid: {e}")))?,
        ),
        None => None,
    };
    let reply: GenerateFilemapReply = call_typed(
        socket_path,
        &Request::GenerateFilemap {
            volume: name.to_owned(),
            snap_ulid,
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.snap_ulid.to_string())
}

/// Remove the local instance of a volume (by_name symlink + by_id/<ulid>/
/// directory). The volume must be stopped first. With `force = false`,
/// the coordinator additionally requires that all local writes have been
/// flushed and uploaded to S3 (`pending/` and `wal/` empty). Bucket-side
/// records and segments are untouched.
pub fn remove_volume(socket_path: &Path, name: &str, force: bool) -> io::Result<()> {
    call_typed::<()>(
        socket_path,
        &Request::Remove {
            volume: name.to_owned(),
            force,
        },
    )?
    .map_err(io::Error::other)
}

/// Stop a volume's supervised process. Coordinator writes the stopped marker
/// and forwards `shutdown` to the volume's control socket. Both the marker
/// file and that socket are root-owned when the coordinator runs under sudo,
/// so this path is what makes `volume stop` work for non-root CLI callers.
pub fn stop_volume(socket_path: &Path, name: &str) -> io::Result<()> {
    call_typed::<()>(
        socket_path,
        &Request::Stop {
            volume: name.to_owned(),
        },
    )?
    .map_err(io::Error::other)
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
    let vol_ulid = ulid::Ulid::from_string(vol_ulid)
        .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
    let snap_ulid = ulid::Ulid::from_string(snap_ulid)
        .map_err(|e| io::Error::other(format!("invalid snap_ulid: {e}")))?;
    let reply: ResolveHandoffKeyReply = call_typed(
        socket_path,
        &Request::ResolveHandoffKey {
            vol_ulid,
            snap_ulid,
        },
    )?
    .map_err(io::Error::other)?;
    Ok(match reply {
        ResolveHandoffKeyReply::Normal => HandoffKey::Normal,
        ResolveHandoffKeyReply::Recovery {
            manifest_pubkey_hex,
        } => HandoffKey::Recovery {
            manifest_pubkey_hex,
        },
    })
}

/// Release a volume's name back to the pool so any other coordinator can
/// `volume claim` it. Drains WAL, publishes a handoff snapshot, halts
/// the daemon, and flips `names/<name>` to `state=released` with the
/// snapshot ULID recorded so the next claimant can fork from it.
/// Returns the handoff snapshot ULID on success.
///
/// With `force`, skips ownership and drain — used when the previous
/// owner is unreachable. The recovering coordinator synthesises a
/// handoff snapshot from S3-visible segments and unconditionally
/// rewrites the record.
pub fn release_volume(socket_path: &Path, name: &str, force: bool) -> io::Result<ReleaseReply> {
    call_typed(
        socket_path,
        &Request::Release {
            volume: name.to_owned(),
            force,
        },
    )?
    .map_err(io::Error::other)
}

/// Start a volume locally. Pure local resume — flips an owned
/// `Stopped` record to `Live`. Refuses if the volume has no local
/// state, or if the bucket says `Released`. To take a `Released`
/// name, call `claim_volume_bucket` first.
pub fn start_volume(socket_path: &Path, name: &str) -> io::Result<()> {
    call_typed::<()>(
        socket_path,
        &Request::Start {
            volume: name.to_owned(),
        },
    )?
    .map_err(io::Error::other)
}

/// Outcome of a `claim_volume_bucket` call.
pub enum ClaimOutcome {
    /// In-place reclaim: the local fork's ULID matched the released
    /// record's. The bucket flipped from `Released` to `Stopped` with
    /// the same `vol_ulid`. No CLI orchestration was needed.
    Reclaimed,
    /// The released record's `vol_ulid` is foreign content. The CLI
    /// must pull the source, mint a fresh local fork, and call
    /// `rebind_name` with the new ULID.
    NeedsClaim {
        released_vol_ulid: String,
        handoff_snapshot: Option<String>,
    },
}

/// `volume claim <name>` IPC: bucket-side claim flow.
///
/// On success, the volume is left in `Stopped` state — no daemon is
/// launched. The CLI calls `start_volume` afterwards if a composed
/// `start --claim` flow was requested.
///
/// Always CAS-protected. To override an unreachable owner, the
/// operator must first run `volume release --force <name>` (the
/// unconditional override), then call this verb against the now-
/// `Released` record. Splitting the two steps means concurrent
/// claimants are arbitrated by the conditional PUT inside
/// `mark_claimed`, not by an unconditional rewrite.
pub fn claim_volume_bucket(socket_path: &Path, name: &str) -> io::Result<ClaimOutcome> {
    let reply: ClaimReply = call_typed(
        socket_path,
        &Request::Claim {
            volume: name.to_owned(),
        },
    )?
    .map_err(io::Error::other)?;
    Ok(match reply {
        ClaimReply::Reclaimed => ClaimOutcome::Reclaimed,
        ClaimReply::MustClaimFresh {
            released_vol_ulid,
            handoff_snapshot,
        } => ClaimOutcome::NeedsClaim {
            released_vol_ulid: released_vol_ulid.to_string(),
            handoff_snapshot: handoff_snapshot.map(|s| s.to_string()),
        },
    })
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
    let reply: CreateReply = call_typed(
        socket_path,
        &Request::Create {
            volume: name.to_owned(),
            size_bytes,
            flags: flags.to_vec(),
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.vol_ulid.to_string())
}

/// Update transport configuration on an existing volume. `restarted = true`
/// means the running volume process was signalled to pick up the new
/// config; `false` means it wasn't running and the new config takes
/// effect on next start.
pub fn update_volume(socket_path: &Path, name: &str, flags: &[String]) -> io::Result<UpdateReply> {
    call_typed(
        socket_path,
        &Request::Update {
            volume: name.to_owned(),
            flags: flags.to_vec(),
        },
    )?
    .map_err(io::Error::other)
}

/// Pull one readonly ancestor from the store via the coordinator.
/// Returns the parent ULID (for the ancestor walk), or `None` for a root.
pub fn pull_readonly(socket_path: &Path, vol_ulid: &str) -> io::Result<Option<String>> {
    let parsed = ulid::Ulid::from_string(vol_ulid)
        .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
    let reply: PullReadonlyReply =
        call_typed(socket_path, &Request::PullReadonly { vol_ulid: parsed })?
            .map_err(io::Error::other)?;
    Ok(reply.parent.map(|u| u.to_string()))
}

/// Synthesize a "now" snapshot for a readonly source. Returns
/// `(snap_ulid, ephemeral_pubkey_hex)`.
pub fn force_snapshot_now(socket_path: &Path, vol_ulid: &str) -> io::Result<(String, String)> {
    let parsed = ulid::Ulid::from_string(vol_ulid)
        .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
    let reply: ForceSnapshotNowReply =
        call_typed(socket_path, &Request::ForceSnapshotNow { vol_ulid: parsed })?
            .map_err(io::Error::other)?;
    Ok((reply.snap_ulid.to_string(), reply.attestation_pubkey_hex))
}

/// Register a fork-from-source job with the coordinator. Returns
/// immediately on success; progress (and the eventual fork ULID) is
/// observed via [`fork_attach_by_name`]. Mirrors the
/// `import_start` + `import_attach_by_name` pattern.
pub fn fork_start(
    socket_path: &Path,
    new_name: &str,
    from: ForkSource,
    force_snapshot: bool,
    flags: &[String],
) -> io::Result<ForkStartReply> {
    call_typed(
        socket_path,
        &Request::ForkStart {
            new_name: new_name.to_owned(),
            from,
            force_snapshot,
            flags: flags.to_vec(),
        },
    )?
    .map_err(io::Error::other)
}

/// Stream fork progress for `new_name` to `out` until the fork
/// completes. Returns the freshly-minted fork's ULID, parsed from the
/// terminal `ForkCreated` event in the stream.
pub fn fork_attach_by_name(
    socket_path: &Path,
    new_name: &str,
    out: &mut dyn Write,
) -> io::Result<ulid::Ulid> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    let request = Request::ForkAttach {
        new_name: new_name.to_owned(),
    };
    let line = serde_json::to_string(&request)
        .map_err(|e| io::Error::other(format!("encode request: {e}")))?;
    writeln!(stream, "{line}")?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut reader = io::BufReader::new(stream);
    let mut buf = String::new();
    let mut new_vol_ulid: Option<ulid::Ulid> = None;
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            return Err(io::Error::other("fork stream closed before terminal event"));
        }
        let env: Envelope<ForkAttachEvent> = serde_json::from_str(buf.trim())
            .map_err(|e| io::Error::other(format!("parse fork event: {e}")))?;
        match env.into_result() {
            Ok(event) => {
                if let Some(line) = render_fork_event(&event) {
                    writeln!(out, "{line}")?;
                }
                if let ForkAttachEvent::ForkCreated { new_vol_ulid: u } = event {
                    new_vol_ulid = Some(u);
                }
                if matches!(event, ForkAttachEvent::Done) {
                    return new_vol_ulid.ok_or_else(|| {
                        io::Error::other("fork completed without ForkCreated event")
                    });
                }
            }
            Err(e) => return Err(io::Error::other(e)),
        }
    }
}

/// Render a coordinator-side fork event as one user-visible line.
/// Returns `None` for events that don't warrant a print (the terminal
/// `Done`, which the caller handles structurally).
fn render_fork_event(event: &ForkAttachEvent) -> Option<String> {
    match event {
        ForkAttachEvent::ResolvingName { name } => {
            Some(format!("resolving '{name}' in remote store"))
        }
        ForkAttachEvent::PullingAncestor { vol_ulid } => Some(format!("pulled {vol_ulid}")),
        ForkAttachEvent::SnapshotTaken { snap_ulid } => {
            Some(format!("took implicit snapshot {snap_ulid}"))
        }
        ForkAttachEvent::AttestedSnapshot { snap_ulid, .. } => {
            Some(format!("attested now-snapshot {snap_ulid}"))
        }
        ForkAttachEvent::ForkingFrom {
            source_vol_ulid,
            snap_ulid,
        } => Some(match snap_ulid {
            Some(s) => format!("forking from {source_vol_ulid}/{s}"),
            None => format!("forking from {source_vol_ulid}"),
        }),
        ForkAttachEvent::ForkCreated { new_vol_ulid } => {
            Some(format!("fork created at {new_vol_ulid}"))
        }
        ForkAttachEvent::PrefetchStarted => Some("prefetching ancestor index...".to_owned()),
        ForkAttachEvent::PrefetchDone => Some("ready".to_owned()),
        ForkAttachEvent::Done => None,
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
    let source_vol_ulid = ulid::Ulid::from_string(source_ulid)
        .map_err(|e| io::Error::other(format!("invalid source ULID: {e}")))?;
    let snap = match snap {
        Some(s) => Some(
            ulid::Ulid::from_string(s)
                .map_err(|e| io::Error::other(format!("invalid snap ULID: {e}")))?,
        ),
        None => None,
    };
    let reply: ForkCreateReply = call_typed(
        socket_path,
        &Request::ForkCreate {
            new_name: new_name.to_owned(),
            source_vol_ulid,
            snap,
            parent_key_hex: parent_key_hex.map(|s| s.to_owned()),
            for_claim,
            flags: flags.to_vec(),
        },
    )?
    .map_err(io::Error::other)?;
    Ok(reply.new_vol_ulid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    static SEQ: AtomicU32 = AtomicU32::new(0);

    fn temp_socket() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = dir.path().join(format!("c{n}.sock"));
        (dir, path)
    }

    /// Spawn a thread that accepts one connection on `path`, reads one line,
    /// optionally sleeps, then writes `response` and closes. Returns the
    /// JoinHandle so the caller can await teardown.
    fn spawn_one_shot_server(
        path: std::path::PathBuf,
        delay: Duration,
        response: &'static str,
    ) -> thread::JoinHandle<()> {
        let listener = UnixListener::bind(&path).unwrap();
        thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let mut reader = io::BufReader::new(&stream);
            let mut line = String::new();
            // The client may close mid-read in the timeout test; ignore.
            let _ = reader.read_line(&mut line);
            thread::sleep(delay);
            // After the delay the client may already be gone (timeout).
            // BrokenPipe is expected — don't unwrap.
            let mut writer = &stream;
            let _ = writeln!(writer, "{response}");
            let _ = writer.flush();
        })
    }

    #[test]
    fn await_prefetch_returns_ok_on_ok_response() {
        let (_guard, sock) = temp_socket();
        let server = spawn_one_shot_server(
            sock.clone(),
            Duration::ZERO,
            r#"{"outcome":"ok","data":null}"#,
        );
        let r = await_prefetch(&sock, "01JQAAAAAAAAAAAAAAAAAAAAAA", Duration::from_secs(5));
        server.join().unwrap();
        assert!(r.is_ok(), "{r:?}");
    }

    #[test]
    fn await_prefetch_propagates_err_response() {
        let (_guard, sock) = temp_socket();
        let server = spawn_one_shot_server(
            sock.clone(),
            Duration::ZERO,
            r#"{"outcome":"err","error":{"kind":"internal","message":"prefetch failed: boom"}}"#,
        );
        let r = await_prefetch(&sock, "01JQAAAAAAAAAAAAAAAAAAAAAA", Duration::from_secs(5));
        server.join().unwrap();
        let e = r.unwrap_err().to_string();
        assert!(e.contains("prefetch failed: boom"), "{e}");
    }

    /// Server sleeps past the budget; client must surface a timeout error
    /// rather than hang. Read timeout fires as WouldBlock/TimedOut.
    #[test]
    fn await_prefetch_times_out_when_server_hangs() {
        let (_guard, sock) = temp_socket();
        let server = spawn_one_shot_server(
            sock.clone(),
            Duration::from_secs(2),
            r#"{"outcome":"ok","data":null}"#,
        );
        let r = await_prefetch(
            &sock,
            "01JQAAAAAAAAAAAAAAAAAAAAAA",
            Duration::from_millis(200),
        );
        let _ = server.join();
        let e = r.unwrap_err().to_string();
        assert!(
            e.contains("did not respond within"),
            "expected timeout error, got: {e}"
        );
    }

    /// No socket file at all → caller gets a clear "coordinator not running"
    /// error rather than a generic ENOENT. The `volume_open` wrapper checks
    /// `is_reachable` first, but the IPC must also be safe to call directly.
    #[test]
    fn await_prefetch_errors_when_socket_absent() {
        let dir = TempDir::new().unwrap();
        let r = await_prefetch(
            &dir.path().join("missing.sock"),
            "01JQAAAAAAAAAAAAAAAAAAAAAA",
            Duration::from_secs(1),
        );
        let e = r.unwrap_err().to_string();
        assert!(e.contains("coordinator not running"), "{e}");
    }

    // ── JSON / typed client tests ─────────────────────────────────────

    /// rescan: client sends `{"verb":"rescan"}`, server replies
    /// `{"outcome":"ok","data":null}`.
    #[test]
    fn rescan_round_trips_typed_envelope() {
        let (_guard, sock) = temp_socket();
        let server = spawn_one_shot_server(
            sock.clone(),
            Duration::ZERO,
            r#"{"outcome":"ok","data":null}"#,
        );
        rescan(&sock).expect("rescan should succeed");
        server.join().unwrap();
    }

    /// status-remote: client sends a typed request, server replies with a
    /// fully-populated `StatusRemoteReply`. Confirms field-by-field that
    /// the typed reply round-trips.
    #[test]
    fn status_remote_parses_typed_reply() {
        use elide_coordinator::eligibility::Eligibility;
        use elide_core::name_record::NameState;
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"ok","data":{"state":"live","vol_ulid":"01J0000000000000000000000V","coordinator_id":"coord-self","eligibility":"owned"}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let reply = status_remote(&sock, "vol").expect("status-remote should succeed");
        server.join().unwrap();
        assert_eq!(reply.state, NameState::Live);
        assert_eq!(reply.coordinator_id.as_deref(), Some("coord-self"));
        assert_eq!(reply.eligibility, Eligibility::Owned);
    }

    /// resolve-name: client sends `{"verb":"resolve-name","name":"vol"}`,
    /// server returns the bound `vol_ulid`. Confirms the typed reply
    /// round-trips and the helper unwraps the ULID directly.
    #[test]
    fn resolve_name_round_trips_typed_envelope() {
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"ok","data":{"vol_ulid":"01J0000000000000000000000V"}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let vol_ulid = resolve_name(&sock, "vol").expect("resolve-name should succeed");
        server.join().unwrap();
        assert_eq!(vol_ulid.to_string(), "01J0000000000000000000000V");
    }

    /// resolve-name: NotFound from the coordinator surfaces as a
    /// transport-level `Err` whose message preserves the typed reason.
    #[test]
    fn resolve_name_propagates_not_found() {
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"err","error":{"kind":"not-found","message":"volume 'ghost' not found in store"}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let err = resolve_name(&sock, "ghost").expect_err("ghost name should error");
        server.join().unwrap();
        assert!(err.to_string().contains("not found in store"), "{err}");
    }

    /// latest-snapshot: with a populated reply, the helper returns
    /// `Some(ulid)`. The empty-store case (`snapshot_ulid: null`)
    /// returns `None` so the caller can produce a clean error message.
    #[test]
    fn latest_snapshot_round_trips_some_and_none() {
        let (_guard, sock) = temp_socket();
        let some_body = r#"{"outcome":"ok","data":{"snapshot_ulid":"01J0000000000000000000000V"}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, some_body);
        let vol = ulid::Ulid::from_string("01J9999999999999999999999V").unwrap();
        let reply = latest_snapshot(&sock, vol).expect("call should succeed");
        server.join().unwrap();
        assert_eq!(
            reply.map(|u| u.to_string()).as_deref(),
            Some("01J0000000000000000000000V"),
        );

        let (_guard2, sock2) = temp_socket();
        let none_body = r#"{"outcome":"ok","data":{}}"#;
        let server2 = spawn_one_shot_server(sock2.clone(), Duration::ZERO, none_body);
        let reply = latest_snapshot(&sock2, vol).expect("call should succeed");
        server2.join().unwrap();
        assert!(reply.is_none());
    }

    /// Typed errors come back as `Err` with the kind preserved.
    #[test]
    fn status_remote_propagates_typed_error_kind() {
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"err","error":{"kind":"not-found","message":"name 'ghost' has no S3 record"}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let err = status_remote(&sock, "ghost").expect_err("ghost name should error");
        server.join().unwrap();
        assert!(err.to_string().contains("no S3 record"), "{err}");
    }

    /// Streaming server: spawn a thread that writes a fixed sequence of
    /// envelopes (one per line) and closes. Used to drive the
    /// `import_attach_by_name` reader.
    fn spawn_streaming_server(
        path: std::path::PathBuf,
        replies: Vec<String>,
    ) -> thread::JoinHandle<()> {
        let listener = UnixListener::bind(&path).unwrap();
        thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            // Read and discard the request line.
            let mut reader = io::BufReader::new(&stream);
            let mut req = String::new();
            let _ = reader.read_line(&mut req);
            let mut writer = &stream;
            for r in &replies {
                let _ = writeln!(writer, "{r}");
            }
            let _ = writer.flush();
        })
    }

    /// import attach: server emits two `Line` events then `Done`. The
    /// client should write each line's content to `out` and return Ok.
    #[test]
    fn import_attach_streams_lines_and_terminates_on_done() {
        let (_guard, sock) = temp_socket();
        let replies = vec![
            r#"{"outcome":"ok","data":{"kind":"line","content":"first"}}"#.to_string(),
            r#"{"outcome":"ok","data":{"kind":"line","content":"second"}}"#.to_string(),
            r#"{"outcome":"ok","data":{"kind":"done"}}"#.to_string(),
        ];
        let server = spawn_streaming_server(sock.clone(), replies);
        let mut out: Vec<u8> = Vec::new();
        import_attach_by_name(&sock, "vol", &mut out).expect("attach should succeed");
        server.join().unwrap();
        assert_eq!(std::str::from_utf8(&out).unwrap(), "first\nsecond\n");
    }

    /// import attach: a terminal `Envelope::Err` translates into an
    /// `io::Error` carrying the typed message.
    #[test]
    fn import_attach_propagates_terminal_error() {
        let (_guard, sock) = temp_socket();
        let replies = vec![
            r#"{"outcome":"ok","data":{"kind":"line","content":"warming up"}}"#.to_string(),
            r#"{"outcome":"err","error":{"kind":"internal","message":"import failed: boom"}}"#
                .to_string(),
        ];
        let server = spawn_streaming_server(sock.clone(), replies);
        let mut out: Vec<u8> = Vec::new();
        let err = import_attach_by_name(&sock, "vol", &mut out)
            .expect_err("terminal error should propagate");
        server.join().unwrap();
        assert!(err.to_string().contains("import failed: boom"), "{err}");
        // The pre-error line still made it through.
        assert_eq!(std::str::from_utf8(&out).unwrap(), "warming up\n");
    }

    /// EOF before any terminal event is treated as a transport failure.
    #[test]
    fn import_attach_errors_on_premature_eof() {
        let (_guard, sock) = temp_socket();
        let replies =
            vec![r#"{"outcome":"ok","data":{"kind":"line","content":"only line"}}"#.to_string()];
        let server = spawn_streaming_server(sock.clone(), replies);
        let mut out: Vec<u8> = Vec::new();
        let err =
            import_attach_by_name(&sock, "vol", &mut out).expect_err("premature EOF should error");
        server.join().unwrap();
        assert!(err.to_string().contains("closed before terminal"), "{err}");
    }

    /// volume-events: empty list deserialises cleanly and the typed
    /// reply preserves the empty `events` Vec.
    #[test]
    fn volume_events_parses_empty_reply() {
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"ok","data":{"events":[]}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let reply = volume_events(&sock, "vol").expect("volume-events should succeed");
        server.join().unwrap();
        assert!(reply.events.is_empty());
    }

    /// volume-events: a populated reply round-trips through the typed
    /// envelope. Confirms `signature_status` decodes from its
    /// `{"status":"valid"}` wire shape.
    #[test]
    fn volume_events_parses_populated_reply() {
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"ok","data":{"events":[{"event":{"version":1,"event_ulid":"01J0000000000000000000000V","at":"2024-01-01T00:00:00.000Z","name":"vol","coordinator_id":"coord-a","vol_ulid":"01J0000000000000000000000W","kind":"created","signature":"00"},"signature_status":{"status":"valid"}}]}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let reply = volume_events(&sock, "vol").expect("volume-events should succeed");
        server.join().unwrap();
        assert_eq!(reply.events.len(), 1);
        assert_eq!(reply.events[0].signature_status, SignatureStatus::Valid);
    }
}
