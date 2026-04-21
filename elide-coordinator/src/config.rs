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

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
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

#[derive(Default, Deserialize)]
pub struct StoreSection {
    /// Use a local directory as the object store (for testing).
    /// Mutually exclusive with `bucket`.
    pub local_path: Option<PathBuf>,

    /// S3 bucket name.
    pub bucket: Option<String>,

    /// S3-compatible endpoint URL (optional; omit for AWS default).
    pub endpoint: Option<String>,

    /// AWS region (optional; falls back to AWS_DEFAULT_REGION env var).
    pub region: Option<String>,
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

    pub fn build(&self) -> Result<Arc<dyn ObjectStore>> {
        if let Some(path) = &self.local_path {
            std::fs::create_dir_all(path)
                .with_context(|| format!("creating local store dir: {}", path.display()))?;
            Ok(Arc::new(
                LocalFileSystem::new_with_prefix(path).context("building local store")?,
            ))
        } else if let Some(bucket) = &self.bucket {
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
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
