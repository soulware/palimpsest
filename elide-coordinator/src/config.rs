// Coordinator configuration, loaded from coordinator.toml.
//
// Example coordinator.toml:
//
//   roots = ["/var/lib/elide/volumes"]
//
//   [store]
//   local_path = "/tmp/elide-store"   # for testing; mutually exclusive with bucket
//
//   # [store]
//   # bucket   = "my-elide-bucket"
//   # endpoint = "https://s3.amazonaws.com"  # optional; omit for AWS default
//   # region   = "us-east-1"                 # optional; falls back to AWS_DEFAULT_REGION
//
//   [drain]
//   interval_secs      = 5    # how often each fork is checked for pending segments
//   scan_interval_secs = 30   # how often root directories are re-scanned for new forks

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use serde::Deserialize;

use crate::store::StoreConfig;

#[derive(Deserialize)]
pub struct CoordinatorConfig {
    /// Root directories to watch for volumes and forks.
    pub roots: Vec<PathBuf>,

    /// Object store configuration.
    pub store: StoreSection,

    /// Drain and scan timing.
    #[serde(default)]
    pub drain: DrainConfig,

    /// Path to the `elide` volume binary.
    /// Defaults to `"elide"` (resolved via PATH).
    #[serde(default = "default_elide_bin")]
    pub elide_bin: PathBuf,
}

fn default_elide_bin() -> PathBuf {
    PathBuf::from("elide")
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
            // Fall back to environment variables (same as one-shot commands).
            StoreConfig::from_env()?.build()
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

/// Load and parse a `coordinator.toml` file.
pub fn load(path: &Path) -> Result<CoordinatorConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file: {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing config file: {}", path.display()))
}
