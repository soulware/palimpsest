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
//   # Multipart upload tuning for segment bodies (all optional):
//   # multipart_part_size_mb = 5    # part size in MiB (min 5, S3 rule)
//   # request_timeout_secs   = 300  # per-HTTP-request timeout
//   # connect_timeout_secs   = 5    # TCP+TLS connect timeout
//   #
//   # Access keys are NOT configured here — they are read from the usual
//   # AWS env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) by both
//   # the coordinator and the spawned volume subprocesses. The coordinator
//   # exports `ELIDE_S3_BUCKET`, `AWS_ENDPOINT_URL`, and `AWS_DEFAULT_REGION`
//   # into each volume subprocess so only coordinator.toml needs to be set
//   # — no per-volume `fetch.toml` required for a uniform store.
//
//   [drain]
//   interval_secs      = 5    # how often each fork is checked for pending segments
//   scan_interval_secs = 30   # how often root directories are re-scanned for new forks
//
//   [gc]
//   density_threshold  = 0.70          # compact when live_bytes/file_bytes < threshold
//   interval_secs      = 30            # how often GC runs per fork (seconds)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
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

    /// Drain and scan timing.
    #[serde(default)]
    pub drain: DrainConfig,

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

    /// Per-request timeout, in seconds. Covers the full HTTP request
    /// lifetime (DNS + connect + TLS + body transfer + response). Must be
    /// long enough for a single multipart part to upload on the slowest
    /// link the coordinator is expected to run on. Default: 300.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// TCP+TLS connection-establishment timeout, in seconds. Default: 5.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

fn default_multipart_part_size_mb() -> u64 {
    5
}
fn default_request_timeout_secs() -> u64 {
    300
}
fn default_connect_timeout_secs() -> u64 {
    5
}

impl Default for StoreSection {
    fn default() -> Self {
        Self {
            local_path: None,
            bucket: None,
            endpoint: None,
            region: None,
            multipart_part_size_mb: default_multipart_part_size_mb(),
            request_timeout_secs: default_request_timeout_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
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
            .with_timeout(Duration::from_secs(self.request_timeout_secs))
            .with_connect_timeout(Duration::from_secs(self.connect_timeout_secs))
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
                " part={}MiB req_timeout={}s connect_timeout={}s",
                self.multipart_part_size_mb.max(5),
                self.request_timeout_secs,
                self.connect_timeout_secs,
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
            Ok(Arc::new(
                LocalFileSystem::new_with_prefix(path).context("building local store")?,
            ))
        } else if let Some(bucket) = &self.bucket {
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_client_options(self.client_options());
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
pub struct DrainConfig {
    /// How often (seconds) each fork is checked for pending segments to upload.
    #[serde(default = "default_interval")]
    pub interval_secs: u64,

    /// How often (seconds) root directories are re-scanned for newly-created forks.
    #[serde(default = "default_scan_interval")]
    pub scan_interval_secs: u64,
}

fn default_interval() -> u64 {
    5
}
fn default_scan_interval() -> u64 {
    30
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_interval(),
            scan_interval_secs: default_scan_interval(),
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

    /// How often (seconds) to run a GC pass per fork. Default: 10.
    #[serde(default = "default_gc_interval")]
    pub interval_secs: u64,
}

fn default_gc_density() -> f64 {
    0.70
}
fn default_gc_interval() -> u64 {
    10
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            density_threshold: default_gc_density(),
            interval_secs: default_gc_interval(),
        }
    }
}

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
            drain: DrainConfig::default(),
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
}
