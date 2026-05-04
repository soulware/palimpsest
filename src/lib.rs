// Elide library: module declarations and utilities shared across the binary
// and its tests.

pub mod body_prefetch;
pub mod control;
pub mod coordinator_client;
pub mod extents;
pub mod inspect;
pub mod inspect_files;
pub mod ls;
pub mod nbd;
pub mod ublk;
pub mod verify;
pub mod volume_open;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use elide_core::signing::VOLUME_KEY_FILE;
use elide_fetch::{FetchConfig, RangeFetcher, RemoteFetcher};
use elide_peer_fetch::{
    BodyFetchClient, PeerEndpoint, PeerFetchCountersHandle, PeerRangeFetcher, VolumeBodySigner,
};
use object_store::ObjectStore;

/// Bundle of inputs the volume daemon needs to construct its remote
/// fetcher. `fetch_config` drives S3 / local-store access; when
/// `peer_endpoint` is present the daemon stacks a `PeerRangeFetcher`
/// in front of S3 to opportunistically serve body byte ranges from
/// the previous claimer over LAN.
pub struct VolumeFetchInputs {
    pub fetch_config: Option<FetchConfig>,
    pub peer_endpoint: Option<PeerEndpoint>,
}

/// Process-wide multi-thread tokio runtime used by the volume daemon
/// for peer-fetch I/O. Initialised lazily on the first call to
/// [`peer_fetch_runtime_handle`]. The runtime is intentionally leaked
/// (it lives for the lifetime of the volume process) so the
/// `tokio::runtime::Handle` captured by [`PeerRangeFetcher`] stays
/// valid for every later sync `get_range` call.
static PEER_FETCH_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn peer_fetch_runtime_handle() -> io::Result<tokio::runtime::Handle> {
    let rt = match PEER_FETCH_RUNTIME.get() {
        Some(rt) => rt,
        None => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("elide-peer-fetch")
                .worker_threads(4)
                .build()
                .map_err(|e| io::Error::other(format!("peer-fetch runtime: {e}")))?;
            PEER_FETCH_RUNTIME.get_or_init(|| rt)
        }
    };
    Ok(rt.handle().clone())
}

/// Output of [`build_volume_fetcher`]. Always returns the
/// `RemoteFetcher`; the optional `peer_counters` handle is present iff
/// the peer-fetch decorator was actually stacked (peer endpoint
/// available and `volume.key` on disk).
pub struct VolumeFetcherBuild {
    pub fetcher: RemoteFetcher,
    pub peer_counters: Option<PeerFetchCountersHandle>,
}

/// Emit a single tracing event with the final peer-fetch counter
/// snapshot. Called from the volume daemon's signal-watcher thread
/// just before `process::exit`. No-op when peer-fetch was not enabled
/// for this run.
pub fn log_peer_fetch_counters_at_shutdown(counters: Option<&PeerFetchCountersHandle>) {
    let Some(counters) = counters else {
        return;
    };
    let snap = counters.snapshot();
    tracing::info!(
        target = "peer-fetch::counters",
        body_bytes_from_peer = snap.body_bytes_from_peer,
        body_bytes_from_store = snap.body_bytes_from_store,
        "peer-fetch shutdown counters"
    );
}

/// Build the `RemoteFetcher` the volume daemon uses for demand-fetch.
///
/// When `inputs.peer_endpoint` is `Some` and the fork has a local
/// `volume.key`, the inner store is wrapped with a [`PeerRangeFetcher`]
/// so body byte ranges consult the previous claimer's peer-fetch
/// server before falling through to S3. Missing `volume.key` (e.g.
/// imported readonly bases) silently runs S3-only — the body-fetch
/// token requires the running fork's signing key.
///
/// Returns `Ok(None)` when no `FetchConfig` is available (the volume
/// is fully local with no remote tier).
pub fn build_volume_fetcher(
    fork_dir: &Path,
    fork_dirs: &[PathBuf],
    inputs: VolumeFetchInputs,
) -> io::Result<Option<VolumeFetcherBuild>> {
    let Some(config) = inputs.fetch_config else {
        return Ok(None);
    };
    let store: Arc<dyn RangeFetcher> = config.build_fetcher()?;
    let (store, peer_counters) = match inputs.peer_endpoint {
        Some(endpoint) if fork_dir.join(VOLUME_KEY_FILE).exists() => {
            let vol_ulid_str = elide_fetch::derive_volume_id(fork_dir)?;
            let vol_ulid = ulid::Ulid::from_string(&vol_ulid_str)
                .map_err(|e| io::Error::other(format!("invalid volume ulid: {e}")))?;
            let signer = VolumeBodySigner::load(fork_dir, VOLUME_KEY_FILE, vol_ulid)?;
            let body_client = BodyFetchClient::new(Arc::new(signer))
                .map_err(|e| io::Error::other(format!("body fetch client: {e}")))?;
            let runtime = peer_fetch_runtime_handle()?;
            let data_dir = fork_dir
                .parent()
                .and_then(Path::parent)
                .unwrap_or(fork_dir)
                .to_path_buf();
            let peer = PeerRangeFetcher::new(store, body_client, endpoint, data_dir, runtime);
            let counters = peer.counters();
            (Arc::new(peer) as Arc<dyn RangeFetcher>, Some(counters))
        }
        _ => (store, None),
    };
    let fetcher = RemoteFetcher::from_store(
        store,
        fork_dirs,
        config
            .fetch_batch_bytes
            .unwrap_or(elide_fetch::DEFAULT_FETCH_BATCH_BYTES),
    )?;
    Ok(Some(VolumeFetcherBuild {
        fetcher,
        peer_counters,
    }))
}

/// Build an `object_store` client from a [`FetchConfig`].
///
/// The volume binary uses this for CLI subcommands that hit S3 directly
/// (`pull`, `ls`, fork-from-S3) and for the embedded coordinator tasks loop
/// (`volume up`). The async `object_store` API is needed there because that
/// code already runs inside tokio. The demand-fetch hot path uses
/// `elide_fetch::FetchConfig::build_fetcher()` instead — sync, no tokio.
pub fn build_object_store(config: &FetchConfig) -> io::Result<Arc<dyn ObjectStore>> {
    if let Some(path) = &config.local_path {
        let store = object_store::local::LocalFileSystem::new_with_prefix(path)
            .map_err(|e| io::Error::other(format!("local store at {path}: {e}")))?;
        return Ok(Arc::new(store));
    }
    let bucket = config.bucket.as_deref().ok_or_else(|| {
        io::Error::other("fetch.toml: one of 'bucket' or 'local_path' is required")
    })?;
    let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    let store = builder
        .build()
        .map_err(|e| io::Error::other(format!("S3 store ({bucket}): {e}")))?;
    Ok(Arc::new(store))
}

/// Resolve a volume name to its directory via `<data_dir>/by_name/<name>`.
///
/// The path is returned as-is; the OS follows the symlink transparently.
pub fn resolve_volume_dir(data_dir: &Path, name: &str) -> PathBuf {
    data_dir.join("by_name").join(name)
}

/// Volume names must be non-empty, contain only `[a-zA-Z0-9._-]`, and not
/// be reserved by the `import` subcommand (`status`, `attach`).
pub fn validate_volume_name(name: &str) -> io::Result<()> {
    if name.is_empty() {
        return Err(io::Error::other("volume name must not be empty"));
    }
    if let Some(c) = name
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_' && *c != '.')
    {
        return Err(io::Error::other(format!(
            "invalid character {c:?} in volume name {name:?}: only [a-zA-Z0-9._-] allowed"
        )));
    }
    if matches!(name, "status" | "attach") {
        return Err(io::Error::other(format!("'{name}' is a reserved name")));
    }
    Ok(())
}

/// Parse a human-readable size string: plain bytes, or with suffix K/M/G/T (base-2).
///
/// Accepts both bare suffixes (`4G`) and SI-style (`4GB`).
pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (num, shift) = if let Some(rest) = s.strip_suffix('T').or_else(|| s.strip_suffix("TB")) {
        (rest, 40)
    } else if let Some(rest) = s.strip_suffix('G').or_else(|| s.strip_suffix("GB")) {
        (rest, 30)
    } else if let Some(rest) = s.strip_suffix('M').or_else(|| s.strip_suffix("MB")) {
        (rest, 20)
    } else if let Some(rest) = s.strip_suffix('K').or_else(|| s.strip_suffix("KB")) {
        (rest, 10)
    } else {
        (s, 0)
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid size: {s}"))?;
    Ok(n << shift)
}

/// Read the volume size from `volume.toml`, or create it from `size_arg` if absent.
pub fn resolve_volume_size(dir: &Path, size_arg: Option<&str>) -> io::Result<u64> {
    let cfg = elide_core::config::VolumeConfig::read(dir)?;
    if let Some(size) = cfg.size {
        return Ok(size);
    }
    let s = size_arg.ok_or_else(|| {
        io::Error::other("volume size required on first use: pass --size (e.g. --size 4G)")
    })?;
    let bytes = parse_size(s).map_err(|e| io::Error::other(format!("bad --size: {e}")))?;
    if bytes == 0 {
        return Err(io::Error::other("volume size must be non-zero"));
    }
    std::fs::create_dir_all(dir)?;
    let mut updated = cfg;
    updated.size = Some(bytes);
    updated.write(dir)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_volume_name ──────────────────────────────────────────────────

    #[test]
    fn valid_names_accepted() {
        for name in &["foo", "my-vol", "vol_1", "ubuntu-22.04", "a.b.c"] {
            assert!(
                validate_volume_name(name).is_ok(),
                "expected ok for {name:?}"
            );
        }
    }

    #[test]
    fn empty_name_rejected() {
        assert!(validate_volume_name("").is_err());
    }

    #[test]
    fn colon_rejected() {
        let err = validate_volume_name("ubuntu:22.04").unwrap_err();
        assert!(err.to_string().contains(':'));
    }

    #[test]
    fn slash_rejected() {
        assert!(validate_volume_name("foo/bar").is_err());
    }

    #[test]
    fn reserved_names_rejected() {
        for name in &["status", "attach"] {
            let err = validate_volume_name(name).unwrap_err();
            assert!(
                err.to_string().contains("reserved"),
                "expected 'reserved' in error for {name:?}, got: {err}"
            );
        }
    }

    // ── parse_size ────────────────────────────────────────────────────────────

    #[test]
    fn parse_size_bytes() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_size_suffixes() {
        assert_eq!(parse_size("4K").unwrap(), 4 * 1024);
        assert_eq!(parse_size("4KB").unwrap(), 4 * 1024);
        assert_eq!(parse_size("2M").unwrap(), 2 << 20);
        assert_eq!(parse_size("2MB").unwrap(), 2 << 20);
        assert_eq!(parse_size("1G").unwrap(), 1u64 << 30);
        assert_eq!(parse_size("1GB").unwrap(), 1u64 << 30);
        assert_eq!(parse_size("1T").unwrap(), 1u64 << 40);
        assert_eq!(parse_size("1TB").unwrap(), 1u64 << 40);
    }

    #[test]
    fn parse_size_trims_whitespace() {
        assert_eq!(parse_size("  8G  ").unwrap(), 8u64 << 30);
    }

    #[test]
    fn parse_size_invalid() {
        assert!(parse_size("abc").is_err());
        assert!(parse_size("").is_err());
        assert!(parse_size("4X").is_err());
    }
}
