// Demand-fetch: pull segments from remote storage on a local cache miss.
//
// Config (fetch.toml in the volume directory):
//
//   [S3 / S3-compatible]
//   bucket   = "my-bucket"
//   endpoint = "https://s3.amazonaws.com"  # optional; omit for AWS default
//   region   = "us-east-1"                 # optional; from env if omitted
//
//   [Local filesystem — for testing without a real object store]
//   local_path = "/tmp/elide-store"
//
// If fetch.toml is absent, env vars are tried: ELIDE_S3_BUCKET (required),
// AWS_ENDPOINT_URL and AWS_DEFAULT_REGION (optional).
//
// Key layout mirrors the coordinator's upload layout:
//   <volume_id>/<fork_name>/YYYYMMDD/<ulid>
//
// Fetch sequence per segment miss:
//   1. Try each fork in the ancestry chain (newest first)
//   2. On first hit: write to <segments_dir>/<ulid>.tmp, then rename
//   3. On all-miss: return a NotFound error

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde::Deserialize;
use tokio::runtime::Runtime;
use ulid::Ulid;

use elide_core::segment::SegmentFetcher;

// --- config ---

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchConfig {
    /// S3 bucket name. Required for S3 mode; absent in local mode.
    #[serde(default)]
    pub bucket: Option<String>,
    /// S3 endpoint URL override (e.g. MinIO). Defaults to AWS standard.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// AWS region. Falls back to `AWS_DEFAULT_REGION` env var.
    #[serde(default)]
    pub region: Option<String>,
    /// Local filesystem path for testing without a real object store.
    #[serde(default)]
    pub local_path: Option<String>,
}

impl FetchConfig {
    /// Load from `<vol_dir>/fetch.toml` if present; fall back to env vars.
    /// Returns `Ok(None)` if neither config file nor env vars are available.
    pub fn load(vol_dir: &Path) -> io::Result<Option<Self>> {
        let config_path = vol_dir.join("fetch.toml");
        if config_path.exists() {
            let s = std::fs::read_to_string(&config_path)?;
            let cfg: Self =
                toml::from_str(&s).map_err(|e| io::Error::other(format!("fetch.toml: {e}")))?;
            return Ok(Some(cfg));
        }
        // Env var fallback
        if let Ok(bucket) = std::env::var("ELIDE_S3_BUCKET") {
            return Ok(Some(FetchConfig {
                bucket: Some(bucket),
                endpoint: std::env::var("AWS_ENDPOINT_URL").ok(),
                region: std::env::var("AWS_DEFAULT_REGION").ok(),
                local_path: None,
            }));
        }
        Ok(None)
    }

    fn build_store(&self) -> io::Result<Arc<dyn ObjectStore>> {
        if let Some(path) = &self.local_path {
            let store = object_store::local::LocalFileSystem::new_with_prefix(path)
                .map_err(|e| io::Error::other(format!("local store at {path}: {e}")))?;
            return Ok(Arc::new(store));
        }
        let bucket = self.bucket.as_deref().ok_or_else(|| {
            io::Error::other("fetch.toml: one of 'bucket' or 'local_path' is required")
        })?;
        let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        let store = builder
            .build()
            .map_err(|e| io::Error::other(format!("S3 store ({bucket}): {e}")))?;
        Ok(Arc::new(store))
    }
}

// --- fetcher ---

pub struct ObjectStoreFetcher {
    store: Arc<dyn ObjectStore>,
    /// (volume_id, fork_name) pairs, oldest-first. Newest-first search on miss.
    forks: Vec<(String, String)>,
    rt: Runtime,
}

impl ObjectStoreFetcher {
    pub fn new(config: &FetchConfig, forks: Vec<(String, String)>) -> io::Result<Self> {
        let store = config.build_store()?;
        let rt = Runtime::new().map_err(|e| io::Error::other(format!("tokio runtime: {e}")))?;
        Ok(Self { store, forks, rt })
    }
}

impl SegmentFetcher for ObjectStoreFetcher {
    fn fetch(&self, segment_id: &str, dest: &Path) -> io::Result<()> {
        self.rt
            .block_on(fetch_segment(&self.store, &self.forks, segment_id, dest))
    }
}

async fn fetch_segment(
    store: &Arc<dyn ObjectStore>,
    forks: &[(String, String)],
    segment_id: &str,
    dest: &Path,
) -> io::Result<()> {
    // Try forks newest-first (reverse of oldest-first slice).
    for (volume_id, fork_name) in forks.iter().rev() {
        let key = segment_key(volume_id, fork_name, segment_id)?;
        match store.get(&key).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| io::Error::other(format!("reading {segment_id}: {e}")))?;
                write_atomic(dest, &bytes)?;
                return Ok(());
            }
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => {
                return Err(io::Error::other(format!(
                    "fetching {segment_id} from {volume_id}/{fork_name}: {e}"
                )));
            }
        }
    }
    Err(io::Error::other(format!(
        "segment {segment_id} not found in any fork"
    )))
}

/// Build the S3 object key for a segment.
/// Format: `<volume_id>/<fork_name>/YYYYMMDD/<ulid>`
fn segment_key(volume_id: &str, fork_name: &str, ulid_str: &str) -> io::Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| io::Error::other(format!("invalid segment id '{ulid_str}': {e}")))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "{volume_id}/{fork_name}/{date}/{ulid_str}"
    )))
}

/// Write `data` to `dest` atomically using a `.tmp` sibling + rename.
/// Creates parent directories if they do not exist.
fn write_atomic(dest: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = dest.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, dest)
}

/// Derive `(volume_id, fork_name)` from a fork directory path.
///
/// The fork directory must be either `<vol_root>/base` (base fork) or
/// `<vol_root>/forks/<name>` (named fork). The volume_id is the last component
/// of the volume root directory.
pub fn derive_fork_name(dir: &Path) -> io::Result<(String, String)> {
    let fork_name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("fork dir has no name"))?
        .to_owned();
    let parent = dir
        .parent()
        .ok_or_else(|| io::Error::other("fork dir has no parent"))?;
    let volume_root = if parent.file_name().and_then(|n| n.to_str()) == Some("forks") {
        parent
            .parent()
            .ok_or_else(|| io::Error::other("forks/ dir has no parent"))?
    } else {
        parent
    };
    let volume_id = volume_root
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("volume root has no name"))?
        .to_owned();
    Ok((volume_id, fork_name))
}

/// Build the ancestry chain as `(volume_id, fork_name)` pairs (oldest-first)
/// from a list of fork directories returned by `Volume::fork_dirs()`.
pub fn ancestry_chain(fork_dirs: &[PathBuf]) -> io::Result<Vec<(String, String)>> {
    fork_dirs.iter().map(|d| derive_fork_name(d)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_fork_name_base_fork() {
        let (vol, fork) = derive_fork_name(Path::new("/volumes/myvm/base")).unwrap();
        assert_eq!(vol, "myvm");
        assert_eq!(fork, "base");
    }

    #[test]
    fn derive_fork_name_named_fork() {
        let (vol, fork) = derive_fork_name(Path::new("/volumes/myvm/forks/vm1")).unwrap();
        assert_eq!(vol, "myvm");
        assert_eq!(fork, "vm1");
    }

    #[test]
    fn segment_key_format() {
        use chrono::{DateTime, Utc};
        use ulid::Ulid;

        let ulid = Ulid::from_parts(1743120000000, 42);
        let ulid_str = ulid.to_string();
        let dt: DateTime<Utc> = ulid.datetime().into();
        let expected_date = dt.format("%Y%m%d").to_string();

        let key = segment_key("myvm", "base", &ulid_str).unwrap();
        assert_eq!(
            key.as_ref(),
            format!("myvm/base/{expected_date}/{ulid_str}")
        );
    }
}
