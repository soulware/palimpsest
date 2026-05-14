// Client for the coordinator inbound socket.
//
// Connects to control.sock, sends one typed JSON request, reads one or
// more typed JSON envelopes. A new connection is made per call — the
// protocol is request-response-close.
//
// Exception: `Client::import_attach_by_name` reads a sequence of typed
// `ImportAttachEvent` envelopes until the import terminates.

use std::io::{self, BufRead, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use elide_coordinator::ipc::{Envelope, IpcError, Request};
use elide_coordinator::macaroon::{Caveat, Macaroon};
use serde::Deserialize;

pub use elide_coordinator::ipc::{
    ClaimAttachEvent, ClaimStartReply, CreateReply, EvictReply, FetchAttachEvent, FetchStartReply,
    FetchStatusReply, ForkAttachEvent, ForkSource, ForkStartReply, GenerateFilemapReply,
    ImportAttachEvent, ImportStartReply, ImportStatusReply, IpcErrorKind, MintOperatorTokenReply,
    RegisterReply, ReleaseReply, ResolveHandoffKeyReply, SignatureStatus, SnapshotReply,
    StatusRemoteReply, StatusReply, StoreConfigReply, StoreCredsReply, UpdateReply,
    VolumeEventEntry, VolumeEventsReply,
};
pub use elide_peer_fetch::PeerEndpoint;

/// Public-API aliases for the client side. The underlying types live
/// in `elide_coordinator::ipc` so server and client share the wire
/// shape; the aliases preserve the `coordinator_client::StoreConfig` /
/// `StoreCreds` names used by the volume-subprocess macaroon flow.
pub type StoreConfig = StoreConfigReply;
pub type StoreCreds = StoreCredsReply;
pub use elide_coordinator::volume_state::VolumeLifecycle;

/// Default budget for [`Client::await_prefetch`]. Longer than `OPEN_RETRY_BUDGET`
/// because prefetch can include real S3 GETs across deep ancestor chains;
/// short enough that a stalled coordinator surfaces a loud error before
/// the supervisor's restart loop fires.
pub const PREFETCH_AWAIT_BUDGET: Duration = Duration::from_secs(60);

/// Lifetime of the per-request macaroon presented to `Request::Credentials`.
/// The holder of the long-lived volume-start macaroon attenuates it with
/// a `NotAfter(now + CREDS_REQ_TTL_SECS)` before each call so that a
/// captured request token is unusable beyond this window.
///
/// Sized to cover IPC + S3-issuer round-trip latency under load (small
/// double-digit seconds) without giving a stolen token meaningful reuse
/// time.
pub const CREDS_REQ_TTL_SECS: u64 = 60;

/// Result of [`Client::register_volume_with_retry`]: the macaroon
/// (used by the lazy-creds wrapper to acquire S3 credentials on the
/// first demand fetch and re-acquire after each idle drop) plus the
/// optional peer-fetch endpoint vended in the registration reply.
///
/// No S3 credentials are issued at this point — the volume calls the
/// coordinator's `Credentials` IPC lazily, only when its first cache
/// miss requires an S3 GET.
pub struct RegisteredVolume {
    pub macaroon: String,
    pub peer_endpoint: Option<PeerEndpoint>,
}

/// IPC client to the coordinator's `control.sock`.
///
/// Constructed once per CLI invocation (or per test) and used to make
/// any number of typed request/response calls. Each call opens a fresh
/// `UnixStream` — the protocol is request-response-close.
pub struct Client {
    socket_path: PathBuf,
}

impl Client {
    /// Construct a client that connects to `socket_path` for every call.
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
        }
    }

    /// The socket path this client was constructed with.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Send a typed JSON request and parse a typed JSON reply.
    ///
    /// Returns:
    /// - `Ok(Ok(reply))` — server returned `Envelope::Ok`.
    /// - `Ok(Err(IpcError))` — server returned a typed error.
    /// - `Err(io::Error)` — transport failure (socket missing, bad JSON, etc.).
    fn call_typed<R>(&self, request: &Request) -> io::Result<Result<R, IpcError>>
    where
        R: for<'de> Deserialize<'de>,
    {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
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

    /// Returns true if the coordinator is currently listening.
    ///
    /// A successful `connect()` is sufficient evidence — we don't send a
    /// command (no waste of an inbound slot) and we don't need to
    /// interpret a response. Failure modes that mean "down": socket
    /// file missing, ECONNREFUSED (stale socket file but no listener),
    /// or any other connect error.
    pub fn is_reachable(&self) -> bool {
        UnixStream::connect(&self.socket_path).is_ok()
    }

    /// Ask the coordinator for its non-secret store config (bucket,
    /// endpoint, region, or local_path). Used by spawned volume
    /// subprocesses (over the macaroon handshake) to build an
    /// object_store that matches the coordinator's view; the CLI
    /// itself never builds an object_store.
    pub fn get_store_config(&self) -> io::Result<StoreConfig> {
        self.call_typed(&Request::GetStoreConfig)?
            .map_err(io::Error::other)
    }

    /// Block until the coordinator's per-fork prefetch task publishes a
    /// terminal result, or until `budget` expires.
    ///
    /// Returns `Ok(())` when prefetch is complete (or untracked, which
    /// is treated as ready). Returns `Err` when prefetch failed, when
    /// the coordinator is unreachable, or on timeout. Volume binaries
    /// call this before `Volume::open` to absorb the race between the
    /// coordinator's prefetch task and the supervisor spawning this
    /// process on a freshly claimed fork.
    pub fn await_prefetch(&self, vol_ulid: &str, budget: Duration) -> io::Result<()> {
        let parsed = ulid::Ulid::from_string(vol_ulid)
            .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
        let stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
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

    /// Notify the coordinator that `Volume::open` succeeded for this
    /// fork. The coordinator uses this signal to clean up the
    /// stop-snapshot from S3 — the local fork is provably sufficient
    /// to serve, so the bucket-side basis is no longer needed.
    ///
    /// Best-effort: callers should not block on this and should ignore
    /// errors. A missed notification leaves the stop-snapshot in S3
    /// until the next `volume stop` overwrites it — operationally
    /// harmless, just a small amount of S3 storage and a GC floor
    /// anchor at the old snapshot point.
    pub fn notify_volume_ready(&self, vol_ulid: &str) -> io::Result<()> {
        let parsed = ulid::Ulid::from_string(vol_ulid)
            .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
        let stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
            ))
        })?;
        let budget = Duration::from_secs(5);
        stream
            .set_read_timeout(Some(budget))
            .map_err(|e| io::Error::other(format!("set read timeout: {e}")))?;
        stream
            .set_write_timeout(Some(budget))
            .map_err(|e| io::Error::other(format!("set write timeout: {e}")))?;
        let request = Request::NotifyVolumeReady { vol_ulid: parsed };
        let line = serde_json::to_string(&request)
            .map_err(|e| io::Error::other(format!("encode request: {e}")))?;
        let mut writer = &stream;
        writeln!(writer, "{line}")?;
        writer.flush()?;
        let _ = stream.shutdown(std::net::Shutdown::Write);
        let mut reader = io::BufReader::new(&stream);
        let mut response_line = String::new();
        let _ = reader.read_line(&mut response_line);
        let env: Envelope<()> = serde_json::from_str(response_line.trim())
            .map_err(|e| io::Error::other(format!("parse reply: {e}")))?;
        env.into_result()
            .map_err(|e| io::Error::other(format!("notify-volume-ready: {e}")))
    }

    /// Best-effort fire-and-forget variant of [`Self::notify_volume_ready`]
    /// for use from the volume binary's serve startup path: derive the
    /// coordinator socket from the fork directory, send the
    /// notification, and log any failure. Never panics, never returns
    /// — the volume serving path continues regardless.
    pub fn notify_volume_ready_from_fork_dir(fork_dir: &Path) {
        let Some(vol_ulid) = fork_dir.file_name().and_then(|n| n.to_str()) else {
            return;
        };
        let Some(data_dir) = fork_dir.parent().and_then(|p| p.parent()) else {
            return;
        };
        let socket = data_dir.join("control.sock");
        if !socket.exists() {
            // No coordinator on this host — operating standalone.
            return;
        }
        let client = Client::new(&socket);
        if let Err(e) = client.notify_volume_ready(vol_ulid) {
            tracing::warn!(
                "[volume {vol_ulid}] notify-volume-ready to coordinator failed: {e}; \
                 stop-snapshot cleanup deferred to next stop"
            );
        }
    }

    /// Mint a per-volume macaroon for the spawned volume process.
    ///
    /// The coordinator authenticates the request via SO_PEERCRED on the
    /// connecting socket and refuses if the peer pid does not match the
    /// volume's recorded `volume.pid`. There is a brief window after
    /// spawn where the parent has not yet written `volume.pid`; the
    /// caller in [`Client::register_and_get_creds`] retries on that
    /// condition.
    ///
    /// The returned reply also carries the peer-fetch endpoint resolved
    /// for this claim (when peer-fetch is locally configured and a
    /// clean handoff predecessor is reachable). The volume process uses
    /// it to build a body byte-range peer-fetcher in front of S3.
    pub fn register_volume(&self, volume_ulid: &str) -> io::Result<RegisterReply> {
        let parsed = ulid::Ulid::from_string(volume_ulid)
            .map_err(|e| io::Error::other(format!("invalid volume_ulid: {e}")))?;
        self.call_typed(&Request::Register {
            volume_ulid: parsed,
        })?
        .map_err(io::Error::other)
    }

    /// Exchange a registered macaroon for short-lived S3 credentials.
    ///
    /// The long-lived volume-start macaroon stays in this process; the
    /// wire token is a per-request attenuation that re-pins the existing
    /// `Scope` and appends `NotAfter(now + CREDS_REQ_TTL_SECS)`. The
    /// re-pinned scope is largely ceremony today (the start macaroon
    /// already carries it), but it makes the attenuation pattern
    /// explicit and decoupled from any future broadening of the start
    /// scope.
    ///
    /// Coordinator re-checks SO_PEERCRED against the macaroon's `pid`
    /// caveat and the volume's recorded `volume.pid` before delegating
    /// to the configured `CredentialIssuer`.
    pub fn macaroon_credentials(&self, macaroon: &str) -> io::Result<StoreCreds> {
        let attenuated = attenuate_for_creds_request(macaroon, now_unix())?;
        self.call_typed(&Request::Credentials {
            macaroon: attenuated,
        })?
        .map_err(io::Error::other)
    }

    /// Register the volume and obtain its macaroon, with retry on the
    /// pid-not-yet-written race.
    ///
    /// The supervisor writes `volume.pid` after `Command::spawn()`
    /// returns, which can race against a fast-starting volume reaching
    /// this code path before the parent has flushed the pid file. We
    /// retry a handful of times with short backoff to absorb that
    /// window.
    ///
    /// No `Credentials` IPC is issued here — the lazy-creds wrapper
    /// calls it on the first demand fetch only, so volumes whose warm
    /// plan is empty and whose reads are all cached never trigger an
    /// S3-credentials issuance.
    pub fn register_volume_with_retry(&self, volume_ulid: &str) -> io::Result<RegisteredVolume> {
        const MAX_ATTEMPTS: u32 = 10;
        const BACKOFF_MS: u64 = 50;
        let mut last_err: Option<io::Error> = None;
        for _ in 0..MAX_ATTEMPTS {
            match self.register_volume(volume_ulid) {
                Ok(reply) => {
                    return Ok(RegisteredVolume {
                        macaroon: reply.macaroon,
                        peer_endpoint: reply.peer_endpoint,
                    });
                }
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

    /// Register a coordinator-spawned `elide fetch-volume` worker
    /// and obtain its macaroon. PID-bound to `<by_id>/<vol_ulid>/
    /// fetch.pid` (written by the orchestrator at spawn time), with
    /// the same not-yet-written-race retry as
    /// [`Self::register_volume_with_retry`].
    pub fn register_fetch_worker_with_retry(
        &self,
        volume_ulid: &str,
    ) -> io::Result<RegisteredVolume> {
        const MAX_ATTEMPTS: u32 = 10;
        const BACKOFF_MS: u64 = 50;
        let parsed = ulid::Ulid::from_string(volume_ulid)
            .map_err(|e| io::Error::other(format!("invalid volume_ulid: {e}")))?;
        let mut last_err: Option<io::Error> = None;
        for _ in 0..MAX_ATTEMPTS {
            let result: io::Result<RegisterReply> = self
                .call_typed(&Request::RegisterFetchWorker {
                    volume_ulid: parsed,
                })?
                .map_err(io::Error::other);
            match result {
                Ok(reply) => {
                    return Ok(RegisteredVolume {
                        macaroon: reply.macaroon,
                        peer_endpoint: reply.peer_endpoint,
                    });
                }
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
        Err(last_err
            .unwrap_or_else(|| io::Error::other("register-fetch-worker: exhausted retries")))
    }

    /// Trigger an immediate fork discovery pass.
    pub fn rescan(&self) -> io::Result<()> {
        self.call_typed::<()>(&Request::Rescan)?
            .map_err(io::Error::other)
    }

    /// Request the coordinator to shut down. Returns once the
    /// coordinator has acknowledged the request; the caller polls
    /// [`Self::is_reachable`] to detect actual exit. With
    /// `keep_volumes = true` the coordinator leaves its volume children
    /// running for a rolling-upgrade restart.
    pub fn shutdown(&self, keep_volumes: bool) -> io::Result<()> {
        self.call_typed::<()>(&Request::Shutdown { keep_volumes })?
            .map_err(io::Error::other)
    }

    /// Query the running state of a named volume.
    pub fn status(&self, volume: &str) -> io::Result<StatusReply> {
        self.call_typed(&Request::Status {
            volume: volume.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Fetch `names/<volume>` from the bucket via the coordinator and
    /// parse the authoritative record. Returns an error when the name
    /// is absent from the bucket or the coordinator cannot reach S3.
    pub fn status_remote(&self, volume: &str) -> io::Result<StatusRemoteReply> {
        self.call_typed(&Request::StatusRemote {
            volume: volume.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// List the per-name event log under `events/<volume>/`.
    /// Each entry includes a `signature_status` reporting whether the
    /// emitting coordinator's pubkey was reachable and the signature
    /// verified.
    pub fn volume_events(&self, volume: &str) -> io::Result<VolumeEventsReply> {
        self.call_typed(&Request::VolumeEvents {
            volume: volume.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Ask the coordinator to start an OCI import.
    /// Returns the import ULID on success (internal; callers use the
    /// volume name).
    ///
    /// `extents_from` is a list of volume names whose extent indices
    /// should populate the new volume's hash pool (dedup/delta source).
    /// The coordinator validates each name and writes the flat
    /// `volume.extent_index` file.
    pub fn import_start(
        &self,
        name: &str,
        oci_ref: &str,
        extents_from: &[String],
    ) -> io::Result<ImportStartReply> {
        self.call_typed(&Request::ImportStart {
            volume: name.to_owned(),
            oci_ref: oci_ref.to_owned(),
            extents_from: extents_from.to_vec(),
        })?
        .map_err(io::Error::other)
    }

    /// Poll the state of an import job by volume name. `Ok(reply)`
    /// carries the import state; failures (no active import, import
    /// failed) come back as `Err`.
    pub fn import_status_by_name(&self, name: &str) -> io::Result<ImportStatusReply> {
        self.call_typed(&Request::ImportStatus {
            volume: name.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Stream import output to `out` until the import completes,
    /// identified by volume name.
    ///
    /// Reads typed [`ImportAttachEvent`] envelopes from the coordinator
    /// until a terminal `Done` event or an `Envelope::Err` is received.
    /// Each `Line` event's content is written to `out`. Returns
    /// `Ok(())` on success, `Err` on import failure or I/O error.
    pub fn import_attach_by_name(&self, name: &str, out: &mut dyn Write) -> io::Result<()> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
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

    /// Ask the coordinator to start a `volume fetch` job that warms a
    /// foreign volume's local cache against its most recent owner-
    /// signed snapshot. Returns immediately with the resolved
    /// `vol_ulid`, `basis_snapshot`, and a `fetch_ulid` for the job.
    /// Subsequent progress is observed via [`Self::fetch_attach_by_name`]
    /// or [`Self::fetch_status_by_name`].
    pub fn fetch_start(&self, name: &str) -> io::Result<FetchStartReply> {
        self.call_typed(&Request::FetchStart {
            volume: name.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Poll the state of an in-flight fetch job by volume name.
    pub fn fetch_status_by_name(&self, name: &str) -> io::Result<FetchStatusReply> {
        self.call_typed(&Request::FetchStatus {
            volume: name.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Stream fetch progress lines to `out` until the job terminates.
    ///
    /// Mirrors [`Self::import_attach_by_name`]: reads typed
    /// [`FetchAttachEvent`] envelopes until a terminal `Done` (success)
    /// or an `Envelope::Err` (failure).
    pub fn fetch_attach_by_name(&self, name: &str, out: &mut dyn Write) -> io::Result<()> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
            ))
        })?;
        let request = Request::FetchAttach {
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
                return Err(io::Error::other(
                    "fetch stream closed before terminal event",
                ));
            }
            let env: Envelope<FetchAttachEvent> = serde_json::from_str(buf.trim())
                .map_err(|e| io::Error::other(format!("parse fetch event: {e}")))?;
            match env.into_result() {
                Ok(FetchAttachEvent::Line { content }) => {
                    writeln!(out, "{content}")?;
                }
                Ok(FetchAttachEvent::Done) => return Ok(()),
                Err(e) => return Err(io::Error::other(e)),
            }
        }
    }

    /// Evict S3-confirmed segment bodies from a volume's cache/
    /// directory.
    ///
    /// Routed through the coordinator so eviction is sequenced between
    /// drain/GC ticks and never races with the GC pass's collect_stats
    /// → compact_segments window.
    ///
    /// If `ulid` is `Some`, evicts only that segment. If `None`, evicts
    /// all S3-confirmed bodies.
    ///
    /// Returns the number of segments evicted.
    pub fn evict_volume(&self, name: &str, ulid: Option<&str>) -> io::Result<usize> {
        let segment_ulid = match ulid {
            Some(s) => Some(
                ulid::Ulid::from_string(s)
                    .map_err(|e| io::Error::other(format!("invalid segment ulid: {e}")))?,
            ),
            None => None,
        };
        let reply: EvictReply = self
            .call_typed(&Request::Evict {
                volume: name.to_owned(),
                segment_ulid,
            })?
            .map_err(io::Error::other)?;
        Ok(reply.evicted)
    }

    /// Trigger a coordinator-orchestrated snapshot of a running volume.
    ///
    /// The coordinator acquires the per-volume snapshot lock, flushes
    /// the WAL via the volume control socket, runs the inline drain,
    /// triggers the signed `.manifest` file, and uploads the snapshot
    /// files. The CLI only blocks on the reply.
    ///
    /// Returns the snapshot ULID on success.
    pub fn snapshot_volume(&self, name: &str) -> io::Result<String> {
        let reply: SnapshotReply = self
            .call_typed(&Request::Snapshot {
                volume: name.to_owned(),
            })?
            .map_err(io::Error::other)?;
        Ok(reply.snap_ulid.to_string())
    }

    /// Generate `snapshots/<snap_ulid>.filemap` for a local volume.
    ///
    /// `snap_ulid` defaults to the volume's latest local snapshot when
    /// omitted. The coordinator opens the sealed snapshot (with
    /// demand-fetch against S3 for evicted segments), walks the ext4
    /// layout, and writes the filemap. Used to seed delta-compression
    /// sources for subsequent `volume import --extents-from <name>`
    /// invocations.
    ///
    /// Returns the snapshot ULID on success.
    pub fn generate_filemap(&self, name: &str, snap_ulid: Option<&str>) -> io::Result<String> {
        let snap_ulid = match snap_ulid {
            Some(s) => Some(
                ulid::Ulid::from_string(s)
                    .map_err(|e| io::Error::other(format!("invalid snap ulid: {e}")))?,
            ),
            None => None,
        };
        let reply: GenerateFilemapReply = self
            .call_typed(&Request::GenerateFilemap {
                volume: name.to_owned(),
                snap_ulid,
            })?
            .map_err(io::Error::other)?;
        Ok(reply.snap_ulid.to_string())
    }

    /// Remove the local instance of a volume (by_name symlink +
    /// by_id/<ulid>/ directory). The volume must be stopped first. With
    /// `force = false`, the coordinator additionally requires that all
    /// local writes have been flushed and uploaded to S3 (`pending/`
    /// and `wal/` empty). Bucket-side records and segments are
    /// untouched.
    ///
    /// `operator_token` is the encoded attenuated operator macaroon
    /// (typically produced by [`crate::operator_token::attenuate_for`]).
    /// The coordinator rejects the call with `kind = "forbidden"` if
    /// the token is absent, malformed, or fails any caveat check —
    /// see `docs/design-auth-model.md`.
    pub fn remove_volume(&self, name: &str, force: bool, operator_token: String) -> io::Result<()> {
        self.call_typed::<()>(&Request::Remove {
            volume: name.to_owned(),
            force,
            operator_token: Some(operator_token),
        })?
        .map_err(io::Error::other)
    }

    /// Mint a coordinator-wide operator token. Backs
    /// `elide token create`. The trust floor is socket reachability;
    /// no operator token is required to call this verb (and could not
    /// be — there is no token to present until the first mint).
    pub fn mint_operator_token(&self, expires_unix: u64) -> io::Result<MintOperatorTokenReply> {
        self.call_typed::<MintOperatorTokenReply>(&Request::MintOperatorToken { expires_unix })?
            .map_err(io::Error::other)
    }

    /// Stop a volume's supervised process. Coordinator writes the
    /// stopped marker and forwards `shutdown` to the volume's control
    /// socket. Both the marker file and that socket are root-owned when
    /// the coordinator runs under sudo, so this path is what makes
    /// `volume stop` work for non-root CLI callers.
    pub fn stop_volume(&self, name: &str, force: bool) -> io::Result<()> {
        self.call_typed::<()>(&Request::Stop {
            volume: name.to_owned(),
            force,
        })?
        .map_err(io::Error::other)
    }

    /// Release a volume's name back to the pool so any other
    /// coordinator can `volume claim` it. Drains WAL, publishes a
    /// handoff snapshot, halts the daemon, and flips `names/<name>` to
    /// `state=released` with the snapshot ULID recorded so the next
    /// claimant can fork from it. Returns the handoff snapshot ULID on
    /// success.
    ///
    /// With `force`, skips ownership and drain — used when the previous
    /// owner is unreachable. The recovering coordinator synthesises a
    /// handoff snapshot from S3-visible segments and unconditionally
    /// rewrites the record.
    pub fn release_volume(&self, name: &str, force: bool) -> io::Result<ReleaseReply> {
        self.call_typed(&Request::Release {
            volume: name.to_owned(),
            force,
        })?
        .map_err(io::Error::other)
    }

    /// Start a volume locally. Pure local resume — flips an owned
    /// `Stopped` record to `Live`. Refuses if the volume has no local
    /// state, or if the bucket says `Released`. To take a `Released`
    /// name, call [`Client::claim_start`] first.
    pub fn start_volume(&self, name: &str) -> io::Result<()> {
        self.call_typed::<()>(&Request::Start {
            volume: name.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Register a name-claim job with the coordinator. `Reclaimed` is
    /// the no-op happy path; `Claiming { released_vol_ulid }` indicates
    /// a foreign-content claim is now running in the background — the
    /// caller subscribes via [`Client::claim_attach_by_name`] to stream
    /// progress and learn the freshly-minted fork ULID.
    pub fn claim_start(&self, volume: &str) -> io::Result<ClaimStartReply> {
        self.call_typed(&Request::ClaimStart {
            volume: volume.to_owned(),
        })?
        .map_err(io::Error::other)
    }

    /// Stream claim progress for `volume` to `out`. Returns the new
    /// fork's ULID parsed from the terminal `ForkCreated` event.
    pub fn claim_attach_by_name(
        &self,
        volume: &str,
        out: &mut dyn Write,
    ) -> io::Result<ulid::Ulid> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
            ))
        })?;
        let request = Request::ClaimAttach {
            volume: volume.to_owned(),
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
                return Err(io::Error::other(
                    "claim stream closed before terminal event",
                ));
            }
            let env: Envelope<ClaimAttachEvent> = serde_json::from_str(buf.trim())
                .map_err(|e| io::Error::other(format!("parse claim event: {e}")))?;
            match env.into_result() {
                Ok(event) => {
                    if let Some(line) = render_claim_event(&event) {
                        writeln!(out, "{line}")?;
                    }
                    if let ClaimAttachEvent::ForkCreated { new_vol_ulid: u } = event {
                        new_vol_ulid = Some(u);
                    }
                    if matches!(event, ClaimAttachEvent::Done) {
                        return new_vol_ulid.ok_or_else(|| {
                            io::Error::other("claim completed without ForkCreated event")
                        });
                    }
                }
                Err(e) => return Err(io::Error::other(e)),
            }
        }
    }

    /// Create a fresh writable volume. Returns the new volume's ULID.
    /// Routes through the coordinator so a sudo'd coordinator can
    /// create root-owned state on behalf of a non-root CLI invocation.
    pub fn create_volume_remote(
        &self,
        name: &str,
        size_bytes: u64,
        flags: &[String],
    ) -> io::Result<String> {
        let reply: CreateReply = self
            .call_typed(&Request::Create {
                volume: name.to_owned(),
                size_bytes,
                flags: flags.to_vec(),
            })?
            .map_err(io::Error::other)?;
        Ok(reply.vol_ulid.to_string())
    }

    /// Update transport configuration on an existing volume.
    /// `restarted = true` means the running volume process was
    /// signalled to pick up the new config; `false` means it wasn't
    /// running and the new config takes effect on next start.
    pub fn update_volume(&self, name: &str, flags: &[String]) -> io::Result<UpdateReply> {
        self.call_typed(&Request::Update {
            volume: name.to_owned(),
            flags: flags.to_vec(),
        })?
        .map_err(io::Error::other)
    }

    /// Register a fork-from-source job with the coordinator. Returns
    /// immediately on success; progress (and the eventual fork ULID) is
    /// observed via [`Client::fork_attach_by_name`]. Mirrors the
    /// `import_start` + `import_attach_by_name` pattern.
    pub fn fork_start(
        &self,
        new_name: &str,
        from: ForkSource,
        force_snapshot: bool,
        flags: &[String],
    ) -> io::Result<ForkStartReply> {
        self.call_typed(&Request::ForkStart {
            new_name: new_name.to_owned(),
            from,
            force_snapshot,
            flags: flags.to_vec(),
        })?
        .map_err(io::Error::other)
    }

    /// Stream fork progress for `new_name` to `out` until the fork
    /// completes. Returns the freshly-minted fork's ULID, parsed from
    /// the terminal `ForkCreated` event in the stream.
    pub fn fork_attach_by_name(
        &self,
        new_name: &str,
        out: &mut dyn Write,
    ) -> io::Result<ulid::Ulid> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            io::Error::other(format!(
                "coordinator not running ({}): {e}",
                self.socket_path.display()
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
}

fn render_claim_event(event: &ClaimAttachEvent) -> Option<String> {
    match event {
        ClaimAttachEvent::PullingAncestor { vol_ulid } => {
            Some(format!("[claim] pulled {vol_ulid}"))
        }
        ClaimAttachEvent::HandoffKeyResolved { key } => match key {
            ResolveHandoffKeyReply::Normal => None,
            ResolveHandoffKeyReply::Recovery { .. } => Some(
                "[claim] handoff snapshot is synthesised — verifying under recovering coordinator's key"
                    .to_owned(),
            ),
        },
        ClaimAttachEvent::ForkCreated { new_vol_ulid } => {
            Some(format!("[claim] fork created at {new_vol_ulid}"))
        }
        ClaimAttachEvent::PrefetchStarted => Some("[claim] prefetching ancestor index...".to_owned()),
        ClaimAttachEvent::PrefetchDone => Some("[claim] ready".to_owned()),
        ClaimAttachEvent::Done => None,
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

/// Build the per-request macaroon presented to `Request::Credentials`.
///
/// Parses the held start macaroon, re-pins its existing `Scope` and
/// appends a tight `NotAfter(now + CREDS_REQ_TTL_SECS)`. Returns the
/// encoded wire form. The original macaroon is not consumed and the
/// chain still verifies against the coordinator's root key — appended
/// caveats can only restrict authority.
fn attenuate_for_creds_request(macaroon: &str, now_unix: u64) -> io::Result<String> {
    let parsed = Macaroon::parse(macaroon)?;
    let scope = parsed
        .scope()
        .ok_or_else(|| io::Error::other("creds macaroon missing scope caveat"))?;
    let attenuated = parsed
        .attenuate(Caveat::Scope(scope))
        .attenuate(Caveat::NotAfter(now_unix + CREDS_REQ_TTL_SECS));
    Ok(attenuated.encode())
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_coordinator::macaroon::{self, Scope};
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
        let r =
            Client::new(&sock).await_prefetch("01JQAAAAAAAAAAAAAAAAAAAAAA", Duration::from_secs(5));
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
        let r =
            Client::new(&sock).await_prefetch("01JQAAAAAAAAAAAAAAAAAAAAAA", Duration::from_secs(5));
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
        let r = Client::new(&sock)
            .await_prefetch("01JQAAAAAAAAAAAAAAAAAAAAAA", Duration::from_millis(200));
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
        let r = Client::new(dir.path().join("missing.sock"))
            .await_prefetch("01JQAAAAAAAAAAAAAAAAAAAAAA", Duration::from_secs(1));
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
        Client::new(&sock).rescan().expect("rescan should succeed");
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
        let reply = Client::new(&sock)
            .status_remote("vol")
            .expect("status-remote should succeed");
        server.join().unwrap();
        assert_eq!(reply.state, NameState::Live);
        assert_eq!(reply.coordinator_id.as_deref(), Some("coord-self"));
        assert_eq!(reply.eligibility, Eligibility::Owned);
    }

    /// Typed errors come back as `Err` with the kind preserved.
    #[test]
    fn status_remote_propagates_typed_error_kind() {
        let (_guard, sock) = temp_socket();
        let body = r#"{"outcome":"err","error":{"kind":"not-found","message":"name 'ghost' has no S3 record"}}"#;
        let server = spawn_one_shot_server(sock.clone(), Duration::ZERO, body);
        let err = Client::new(&sock)
            .status_remote("ghost")
            .expect_err("ghost name should error");
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
        Client::new(&sock)
            .import_attach_by_name("vol", &mut out)
            .expect("attach should succeed");
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
        let err = Client::new(&sock)
            .import_attach_by_name("vol", &mut out)
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
        let err = Client::new(&sock)
            .import_attach_by_name("vol", &mut out)
            .expect_err("premature EOF should error");
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
        let reply = Client::new(&sock)
            .volume_events("vol")
            .expect("volume-events should succeed");
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
        let reply = Client::new(&sock)
            .volume_events("vol")
            .expect("volume-events should succeed");
        server.join().unwrap();
        assert_eq!(reply.events.len(), 1);
        assert_eq!(reply.events[0].signature_status, SignatureStatus::Valid);
    }

    fn root_key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn mint_start_macaroon(scope: Scope) -> String {
        macaroon::mint(
            &root_key(),
            vec![
                Caveat::Volume("01JQAAAAAAAAAAAAAAAAAAAAAA".to_owned()),
                Caveat::Scope(scope),
                Caveat::Pid(12345),
            ],
        )
        .encode()
    }

    /// Attenuation re-pins the start scope, appends `NotAfter(now+TTL)`,
    /// and the resulting chain still verifies against the root key.
    #[test]
    fn attenuate_for_creds_request_appends_scope_and_not_after() {
        let wire = mint_start_macaroon(Scope::Credentials);
        let now = 1_750_000_000_u64;
        let out = attenuate_for_creds_request(&wire, now).expect("attenuate");

        let parsed = Macaroon::parse(&out).expect("parse attenuated");
        assert!(
            macaroon::verify(&root_key(), &parsed),
            "attenuated chain must still verify against root key"
        );
        assert_eq!(parsed.scope(), Some(Scope::Credentials));
        assert_eq!(parsed.narrowest_not_after(), Some(now + CREDS_REQ_TTL_SECS),);

        // Caveat order: original three, then re-pinned Scope, then NotAfter.
        let caveats = parsed.caveats();
        assert_eq!(caveats.len(), 5);
        assert!(matches!(caveats[3], Caveat::Scope(Scope::Credentials),));
        assert!(matches!(caveats[4], Caveat::NotAfter(t) if t == now + CREDS_REQ_TTL_SECS));
    }

    /// Fetch-worker start macaroons get the same shape — the helper
    /// reads the scope from the held macaroon and re-pins it.
    #[test]
    fn attenuate_for_creds_request_preserves_fetch_worker_scope() {
        let wire = mint_start_macaroon(Scope::FetchWorker);
        let out = attenuate_for_creds_request(&wire, 0).expect("attenuate");
        let parsed = Macaroon::parse(&out).expect("parse attenuated");
        assert_eq!(parsed.scope(), Some(Scope::FetchWorker));
    }

    /// A malformed start macaroon (no scope) is a programmer error;
    /// surface it as `io::Error::other` rather than silently producing a
    /// token the coordinator will reject for a less obvious reason.
    #[test]
    fn attenuate_for_creds_request_rejects_missing_scope() {
        let wire = macaroon::mint(
            &root_key(),
            vec![
                Caveat::Volume("01JQAAAAAAAAAAAAAAAAAAAAAA".to_owned()),
                Caveat::Pid(12345),
            ],
        )
        .encode();
        let err = attenuate_for_creds_request(&wire, 0).unwrap_err();
        assert!(err.to_string().contains("scope"), "{err}");
    }

    /// Past-`NotAfter` tokens are rejected by `check_caveats`. Confirms
    /// the attenuated tail is enforced end-to-end.
    #[test]
    fn attenuated_token_with_expired_not_after_is_rejected() {
        let wire = mint_start_macaroon(Scope::Credentials);
        let now_at_mint = 1_000_u64;
        let out = attenuate_for_creds_request(&wire, now_at_mint).expect("attenuate");
        let parsed = Macaroon::parse(&out).expect("parse");

        let ctx = macaroon::VerifyCtx {
            volume: "01JQAAAAAAAAAAAAAAAAAAAAAA",
            peer_pid: 12345,
            now_unix: now_at_mint + CREDS_REQ_TTL_SECS + 1,
            accepted_scopes: &[Scope::Credentials, Scope::FetchWorker],
        };
        let err = macaroon::check_caveats(&parsed, &ctx).unwrap_err();
        assert_eq!(err, "macaroon expired");
    }
}
