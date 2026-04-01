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
//
//   [drain]
//   interval_secs      = 5    # how often each fork is checked for pending segments
//   scan_interval_secs = 30   # how often root directories are re-scanned for new forks
//
//   [gc]
//   density_threshold  = 0.70          # compact when live_bytes/file_bytes < threshold
//   small_segment_bytes = 8388608      # also compact segments smaller than this
//   interval_secs      = 300           # how often GC runs per fork (seconds)

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
    PathBuf::from("elide")
}

fn default_elide_import_bin() -> PathBuf {
    PathBuf::from("elide-import")
}

#[derive(Deserialize)]
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

impl Default for StoreSection {
    fn default() -> Self {
        Self {
            local_path: None,
            bucket: None,
            endpoint: None,
            region: None,
        }
    }
}

impl StoreSection {
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

    /// Also compact segments whose file size is below this threshold (bytes).
    /// Default: 8 MiB.
    #[serde(default = "default_gc_small_segment")]
    pub small_segment_bytes: u64,

    /// How often (seconds) to run a GC pass per fork. Default: 300.
    #[serde(default = "default_gc_interval")]
    pub interval_secs: u64,
}

fn default_gc_density() -> f64 {
    0.70
}
fn default_gc_small_segment() -> u64 {
    8 * 1024 * 1024
}
fn default_gc_interval() -> u64 {
    300
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            density_threshold: default_gc_density(),
            small_segment_bytes: default_gc_small_segment(),
            interval_secs: default_gc_interval(),
        }
    }
}

/// Load and parse a `coordinator.toml` file.
pub fn load(path: &Path) -> Result<CoordinatorConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file: {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing config file: {}", path.display()))
}
