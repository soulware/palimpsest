// Coordinator configuration, loaded from coordinator.toml.
//
// Example coordinator.toml:
//
//   data_dir = "elide_data"   # directory containing volumes; default: ./elide_data
//
//   # [store] section is optional; defaults to a local directory at ./elide_store
//   # To use a specific local path:
//   # [store]
//   # local_path = "/var/lib/elide/store"
//   #
//   # To use S3:
//   # [store]
//   # bucket   = "my-elide-bucket"
//   # endpoint = "https://s3.amazonaws.com"  # optional; omit for AWS default
//   # region   = "us-east-1"                 # optional; falls back to AWS_DEFAULT_REGION
//   #
//   # To use Tigris (single global endpoint, region "auto"):
//   # [store]
//   # bucket   = "my-elide-bucket"
//   # endpoint = "https://t3.storage.dev"
//   # region   = "auto"
//   #
//   # Multipart upload tuning for segment bodies (all optional):
//   # multipart_part_size_mb = 5      # part size in MiB (min 5, S3 rule)
//   # request_timeout       = "5m"    # per-HTTP-request timeout (humantime)
//   # connect_timeout       = "5s"    # TCP+TLS connect timeout (humantime)
//   #
//   # Access keys are NOT configured here — they are read from the usual
//   # AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) by both
//   # the coordinator and the spawned volume subprocesses. The coordinator
//   # exports `ELIDE_S3_BUCKET`, `AWS_ENDPOINT_URL`, and `AWS_DEFAULT_REGION`
//   # into each volume subprocess so only coordinator.toml needs to be set
//   # — no per-volume `fetch.toml` required for a uniform store.
//
//   [supervisor]
//   drain_interval  = "5s"   # how often each fork is checked for pending segments
//   scan_interval   = "30s"  # how often root directories are re-scanned for new forks
//
//   [gc]
//   density_threshold = 0.70   # compact when live_bytes/file_bytes < threshold
//   interval          = "10s"  # how often GC runs per fork
//   retention_window  = "10m"  # how long GC inputs are retained in S3
//
// All duration fields use humantime ("5s", "30m", "24h").

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use humantime_serde::re::humantime;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{ClientOptions, ObjectStore};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct CoordinatorConfig {
    /// Directory containing volumes. Default: `./elide_data`.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Path to the coordinator inbound socket.
    /// Defaults to `<data_dir>/control.sock`.
    pub socket_path: Option<PathBuf>,

    /// Object store configuration. Defaults to a local directory at `./elide_store`.
    #[serde(default)]
    pub store: StoreSection,

    /// Supervisor loop timings (drain cadence, root scan cadence).
    #[serde(default)]
    pub supervisor: SupervisorConfig,

    /// Path to the `elide` volume binary.
    /// Defaults to `"elide"` (resolved via PATH).
    #[serde(default = "default_elide_bin")]
    pub elide_bin: PathBuf,

    /// Path to the `elide-import` binary.
    /// Defaults to `"elide-import"` (resolved via PATH).
    #[serde(default = "default_elide_import_bin")]
    pub elide_import_bin: PathBuf,

    /// GC configuration.
    #[serde(default)]
    pub gc: GcConfig,
}

impl CoordinatorConfig {
    /// Resolve the socket path: explicit config value, or `<data_dir>/control.sock`.
    pub fn resolved_socket_path(&self) -> PathBuf {
        self.socket_path
            .clone()
            .unwrap_or_else(|| self.data_dir.join("control.sock"))
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("elide_data")
}

fn default_elide_bin() -> PathBuf {
    sibling_bin("elide")
}

fn default_elide_import_bin() -> PathBuf {
    sibling_bin("elide-import")
}

/// Return a path to `name` in the same directory as the running coordinator
/// binary. Falls back to just `name` (PATH lookup) if the current exe path
/// cannot be determined.
fn sibling_bin(name: &str) -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|dir| dir.join(name)))
        .unwrap_or_else(|| PathBuf::from(name))
}

#[derive(Clone, Deserialize)]
pub struct StoreSection {
    /// Use a local directory as the object store (for testing).
    /// Mutually exclusive with `bucket`.
    #[serde(default)]
    pub local_path: Option<PathBuf>,

    /// S3 bucket name.
    #[serde(default)]
    pub bucket: Option<String>,

    /// S3-compatible endpoint URL (optional; omit for AWS default).
    #[serde(default)]
    pub endpoint: Option<String>,

    /// AWS region (optional; falls back to AWS_DEFAULT_REGION env var).
    #[serde(default)]
    pub region: Option<String>,

    /// Multipart upload part size in MiB for segment bodies. Must be at
    /// least 5 (S3 minimum part size, except the final part). Larger parts
    /// amortise request overhead; smaller parts retry faster on failure.
    /// Default: 5.
    #[serde(default = "default_multipart_part_size_mb")]
    pub multipart_part_size_mb: u64,

    /// Per-request timeout. Covers the full HTTP request lifetime (DNS +
    /// connect + TLS + body transfer + response). Must be long enough for
    /// a single multipart part to upload on the slowest link the
    /// coordinator is expected to run on. Default: 5m.
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,

    /// TCP+TLS connection-establishment timeout. Default: 5s.
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
}

fn default_multipart_part_size_mb() -> u64 {
    5
}
fn default_request_timeout() -> Duration {
    Duration::from_secs(300)
}
fn default_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

impl Default for StoreSection {
    fn default() -> Self {
        Self {
            local_path: None,
            bucket: None,
            endpoint: None,
            region: None,
            multipart_part_size_mb: default_multipart_part_size_mb(),
            request_timeout: default_request_timeout(),
            connect_timeout: default_connect_timeout(),
        }
    }
}

impl StoreSection {
    /// Env vars to export into spawned volume subprocesses so the volume's
    /// fetcher picks up the same store config as the coordinator without
    /// requiring the operator to also set env vars on the parent shell or
    /// drop a `fetch.toml` into every volume directory.
    ///
    /// Only the non-secret settings are exported — access keys still come
    /// from the coordinator's own env (`AWS_ACCESS_KEY_ID` /
    /// `AWS_SECRET_ACCESS_KEY`) and are inherited by the subprocess. Local
    /// store mode returns an empty list; the volume's fallback to
    /// `./elide_store` handles that case.
    pub fn child_env(&self) -> Vec<(&'static str, String)> {
        let mut env = Vec::new();
        if let Some(bucket) = &self.bucket {
            env.push(("ELIDE_S3_BUCKET", bucket.clone()));
            if let Some(ep) = &self.endpoint {
                env.push(("AWS_ENDPOINT_URL", ep.clone()));
            }
            if let Some(region) = &self.region {
                env.push(("AWS_DEFAULT_REGION", region.clone()));
            }
        }
        env
    }

    /// Multipart part size in bytes, clamped to the S3 minimum of 5 MiB.
    pub fn multipart_part_size_bytes(&self) -> usize {
        (self.multipart_part_size_mb.max(5) * 1024 * 1024) as usize
    }

    /// `reqwest` client options (timeouts) derived from config.
    fn client_options(&self) -> ClientOptions {
        ClientOptions::default()
            .with_timeout(self.request_timeout)
            .with_connect_timeout(self.connect_timeout)
    }

    /// One-line human-readable summary of the configured object store, for
    /// startup logs. Does not include secrets.
    pub fn describe(&self) -> String {
        if let Some(path) = &self.local_path {
            format!("local {}", path.display())
        } else if let Some(bucket) = &self.bucket {
            let mut s = format!("s3 bucket={bucket}");
            if let Some(ep) = &self.endpoint {
                s.push_str(&format!(" endpoint={ep}"));
            }
            if let Some(region) = &self.region {
                s.push_str(&format!(" region={region}"));
            }
            s.push_str(&format!(
                " part={}MiB req_timeout={} connect_timeout={}",
                self.multipart_part_size_mb.max(5),
                humantime::format_duration(self.request_timeout),
                humantime::format_duration(self.connect_timeout),
            ));
            s
        } else {
            "local elide_store (default)".to_owned()
        }
    }

    /// Verify that the store is reachable with the configured credentials.
    /// Fails fast at startup so operators see "bad credentials" once rather
    /// than as a spinning retry in every per-volume task.
    ///
    /// For S3 stores, bails immediately if `AWS_ACCESS_KEY_ID` is unset —
    /// without that, the object_store client falls back to the EC2 IMDS
    /// credential provider and spends ~11s per call on retries. Then issues
    /// a single `list` request against the configured bucket to surface
    /// auth/permission errors (e.g. 403) with clean context.
    ///
    /// For local stores, issues the same `list` request as a minimal
    /// readability check on the store root.
    pub async fn probe(&self, store: &dyn ObjectStore) -> Result<()> {
        if self.bucket.is_some() && std::env::var_os("AWS_ACCESS_KEY_ID").is_none() {
            bail!(
                "object store configured for S3 (bucket={}) but AWS_ACCESS_KEY_ID \
                 is not set in the environment; set AWS_ACCESS_KEY_ID and \
                 AWS_SECRET_ACCESS_KEY before starting the coordinator",
                self.bucket.as_deref().unwrap_or("?"),
            );
        }
        let prefix = StorePath::from("by_id/");
        let mut stream = store.list(Some(&prefix));
        match stream.next().await {
            None | Some(Ok(_)) => Ok(()),
            Some(Err(e)) => {
                Err(anyhow::Error::new(e).context(format!("probing store ({})", self.describe())))
            }
        }
    }

    pub fn build(&self) -> Result<Arc<dyn ObjectStore>> {
        if let Some(path) = &self.local_path {
            std::fs::create_dir_all(path)
                .with_context(|| format!("creating local store dir: {}", path.display()))?;
            // Wrap LocalFileSystem so PutMode::Update is honoured — the
            // upstream impl returns NotImplemented for it, which would
            // break the lifecycle verbs' If-Match read-modify-write.
            let local = LocalFileSystem::new_with_prefix(path).context("building local store")?;
            Ok(Arc::new(
                crate::local_cond_store::ConditionalLocalStore::new(local),
            ))
        } else if let Some(bucket) = &self.bucket {
            // Enable conditional PUT (`If-Match` ETag) so the lifecycle
            // verbs in `crate::lifecycle` can do read-modify-write on
            // `names/<name>` records atomically. Tigris and current AWS
            // S3 both support this; without it the S3 backend returns
            // `Error::NotImplemented` for `PutMode::Update`.
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_client_options(self.client_options())
                .with_conditional_put(S3ConditionalPut::ETagMatch);
            if let Some(ep) = &self.endpoint {
                builder = builder
                    .with_endpoint(ep)
                    .with_virtual_hosted_style_request(false);
            }
            if let Some(region) = &self.region {
                builder = builder.with_region(region);
            }
            Ok(Arc::new(builder.build().context("building S3 client")?))
        } else {
            // Default to a local directory store.
            let path = PathBuf::from("elide_store");
            std::fs::create_dir_all(&path)
                .with_context(|| format!("creating local store dir: {}", path.display()))?;
            Ok(Arc::new(
                LocalFileSystem::new_with_prefix(&path).context("building local store")?,
            ))
        }
    }
}

#[derive(Deserialize)]
pub struct SupervisorConfig {
    /// How often each fork is checked for pending segments to upload.
    #[serde(default = "default_drain_interval", with = "humantime_serde")]
    pub drain_interval: Duration,

    /// How often root directories are re-scanned for newly-created forks.
    #[serde(default = "default_scan_interval", with = "humantime_serde")]
    pub scan_interval: Duration,
}

fn default_drain_interval() -> Duration {
    Duration::from_secs(5)
}
fn default_scan_interval() -> Duration {
    Duration::from_secs(30)
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            drain_interval: default_drain_interval(),
            scan_interval: default_scan_interval(),
        }
    }
}

/// Configuration for coordinator-driven segment GC.
#[derive(Deserialize, Clone)]
pub struct GcConfig {
    /// Compact a segment when live_bytes / file_bytes falls below this ratio.
    /// Default: 0.70.
    #[serde(default = "default_gc_density")]
    pub density_threshold: f64,

    /// How often to run a GC pass per fork. Default: 10s.
    #[serde(default = "default_gc_interval", with = "humantime_serde")]
    pub interval: Duration,

    /// Retention window for GC input segments. After a successful GC
    /// handoff, inputs are not deleted from S3 immediately; the
    /// coordinator writes a retention marker at
    /// `by_id/<vol>/retention/<gc_output_ulid>` and the reaper deletes
    /// the inputs once this window has elapsed. Accepts humantime-style
    /// strings like `"24h"`, `"30s"`, `"5m"`. Default: `10m`.
    #[serde(default = "default_retention_window", with = "humantime_serde")]
    pub retention_window: Duration,
}

fn default_gc_density() -> f64 {
    0.70
}
fn default_gc_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_retention_window() -> Duration {
    Duration::from_secs(10 * 60)
}

impl GcConfig {
    /// Cadence at which the reaper ticks: `max(retention / 10, 1s)`. The 1s
    /// floor exists for tests with very short retention; production T is
    /// hours, so the floor never binds in real deployments.
    pub fn reaper_cadence(&self) -> Duration {
        let derived = self.retention_window / 10;
        derived.max(Duration::from_secs(1))
    }
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            density_threshold: default_gc_density(),
            interval: default_gc_interval(),
            retention_window: default_retention_window(),
        }
    }
}

/// Default `coordinator.toml` template emitted by `elide-coordinator init`.
/// All fields are commented out so the file documents itself: every value
/// shown is the default the daemon would use if the file were absent.
pub const DEFAULT_CONFIG_TEMPLATE: &str = r#"# Elide coordinator configuration.
# Every field below is optional; the values shown are the defaults.

# data_dir = "elide_data"
# socket_path = "elide_data/control.sock"  # defaults to <data_dir>/control.sock
# elide_bin = "elide"                      # resolved via PATH
# elide_import_bin = "elide-import"        # resolved via PATH

[store]
# Local directory store (default if neither local_path nor bucket is set):
# local_path = "elide_store"
#
# S3-compatible store:
# bucket   = "my-elide-bucket"
# endpoint = "https://s3.amazonaws.com"   # optional; omit for AWS default
# region   = "us-east-1"                  # falls back to AWS_DEFAULT_REGION
#
# Tigris (https://www.tigrisdata.com) — single global endpoint, region "auto":
# bucket   = "my-elide-bucket"
# endpoint = "https://t3.storage.dev"
# region   = "auto"
#
# Access keys come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the
# coordinator's environment and are inherited by spawned volume subprocesses.
#
# multipart_part_size_mb = 5      # min 5 (S3 rule)
# request_timeout        = "5m"   # per-HTTP-request timeout (humantime)
# connect_timeout        = "5s"   # TCP+TLS connect timeout (humantime)

[supervisor]
# drain_interval = "5s"   # how often each fork is checked for pending segments
# scan_interval  = "30s"  # how often roots are re-scanned for new forks

[gc]
# density_threshold = 0.70    # compact when live_bytes / file_bytes < threshold
# interval          = "10s"   # how often GC runs per fork
# retention_window  = "10m"   # how long GC inputs stay in S3 before reaping
"#;

/// Load and parse a `coordinator.toml` file.
///
/// If the file does not exist, returns a default config (all fields use their
/// defaults: `data_dir = ./elide_data`, `store = ./elide_store`, etc.).
pub fn load(path: &Path) -> Result<CoordinatorConfig> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text)
            .with_context(|| format!("parsing config file: {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(CoordinatorConfig::default()),
        Err(e) => Err(e).with_context(|| format!("reading config file: {}", path.display())),
    }
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            socket_path: None,
            store: StoreSection::default(),
            supervisor: SupervisorConfig::default(),
            elide_bin: default_elide_bin(),
            elide_import_bin: default_elide_import_bin(),
            gc: GcConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_env_empty_for_local_store() {
        let store = StoreSection {
            local_path: Some(PathBuf::from("/tmp/whatever")),
            ..StoreSection::default()
        };
        assert!(store.child_env().is_empty());
    }

    #[test]
    fn child_env_empty_when_unset() {
        let store = StoreSection::default();
        assert!(store.child_env().is_empty());
    }

    #[test]
    fn child_env_exports_bucket_endpoint_region() {
        let store = StoreSection {
            bucket: Some("elide-test".into()),
            endpoint: Some("https://t3.storage.dev".into()),
            region: Some("auto".into()),
            local_path: None,
            ..StoreSection::default()
        };
        let env = store.child_env();
        assert_eq!(
            env,
            vec![
                ("ELIDE_S3_BUCKET", "elide-test".to_owned()),
                ("AWS_ENDPOINT_URL", "https://t3.storage.dev".to_owned()),
                ("AWS_DEFAULT_REGION", "auto".to_owned()),
            ]
        );
    }

    #[test]
    fn child_env_bucket_only_omits_optional_fields() {
        let store = StoreSection {
            bucket: Some("elide-test".into()),
            endpoint: None,
            region: None,
            local_path: None,
            ..StoreSection::default()
        };
        let env = store.child_env();
        assert_eq!(env, vec![("ELIDE_S3_BUCKET", "elide-test".to_owned())]);
    }

    #[test]
    fn parses_toml_with_store_section() {
        let toml_str = r#"
            data_dir = "elide_data"

            [store]
            bucket = "elide-test"
            endpoint = "https://t3.storage.dev"
            region = "auto"
        "#;
        let cfg: CoordinatorConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.store.bucket.as_deref(), Some("elide-test"));
        assert_eq!(
            cfg.store.endpoint.as_deref(),
            Some("https://t3.storage.dev")
        );
        assert_eq!(cfg.store.region.as_deref(), Some("auto"));
    }

    #[test]
    fn default_config_template_parses_to_defaults() {
        let cfg: CoordinatorConfig = toml::from_str(DEFAULT_CONFIG_TEMPLATE)
            .expect("DEFAULT_CONFIG_TEMPLATE must parse as a CoordinatorConfig");
        let defaults = CoordinatorConfig::default();
        assert_eq!(cfg.data_dir, defaults.data_dir);
        assert_eq!(
            cfg.supervisor.drain_interval,
            defaults.supervisor.drain_interval
        );
        assert_eq!(
            cfg.supervisor.scan_interval,
            defaults.supervisor.scan_interval
        );
        assert_eq!(cfg.gc.interval, defaults.gc.interval);
        assert_eq!(cfg.gc.retention_window, defaults.gc.retention_window);
        assert_eq!(cfg.store.request_timeout, defaults.store.request_timeout);
        assert_eq!(cfg.store.connect_timeout, defaults.store.connect_timeout);
    }

    #[test]
    fn parses_humantime_durations() {
        let toml_str = r#"
            [supervisor]
            drain_interval = "2s"
            scan_interval  = "1m"

            [store]
            request_timeout = "10m"
            connect_timeout = "500ms"

            [gc]
            interval         = "15s"
            retention_window = "1h"
        "#;
        let cfg: CoordinatorConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.supervisor.drain_interval, Duration::from_secs(2));
        assert_eq!(cfg.supervisor.scan_interval, Duration::from_secs(60));
        assert_eq!(cfg.store.request_timeout, Duration::from_secs(600));
        assert_eq!(cfg.store.connect_timeout, Duration::from_millis(500));
        assert_eq!(cfg.gc.interval, Duration::from_secs(15));
        assert_eq!(cfg.gc.retention_window, Duration::from_secs(3600));
    }
}
