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
//   by_id/<volume_id>/YYYYMMDD/<ulid>
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
    /// Volume ULIDs in the ancestry chain, oldest-first. Searched newest-first on miss.
    volume_ids: Vec<String>,
    rt: Runtime,
}

impl ObjectStoreFetcher {
    pub fn new(config: &FetchConfig, volume_ids: Vec<String>) -> io::Result<Self> {
        let store = config.build_store()?;
        let rt = Runtime::new().map_err(|e| io::Error::other(format!("tokio runtime: {e}")))?;
        Ok(Self {
            store,
            volume_ids,
            rt,
        })
    }
}

impl SegmentFetcher for ObjectStoreFetcher {
    fn fetch(&self, segment_id: &str, fetched_dir: &Path) -> io::Result<()> {
        self.rt.block_on(fetch_segment(
            &self.store,
            &self.volume_ids,
            segment_id,
            fetched_dir,
        ))
    }
}

async fn fetch_segment(
    store: &Arc<dyn ObjectStore>,
    volume_ids: &[String],
    segment_id: &str,
    fetched_dir: &Path,
) -> io::Result<()> {
    // Try volumes newest-first (reverse of oldest-first slice).
    for volume_id in volume_ids.iter().rev() {
        let key = segment_key(volume_id, segment_id)?;
        match store.get(&key).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| io::Error::other(format!("reading {segment_id}: {e}")))?;
                write_fetched(fetched_dir, segment_id, &bytes)?;
                return Ok(());
            }
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => {
                return Err(io::Error::other(format!(
                    "fetching {segment_id} from by_id/{volume_id}: {e}"
                )));
            }
        }
    }
    Err(io::Error::other(format!(
        "segment {segment_id} not found in any ancestor"
    )))
}

/// Segment header layout (first 96 bytes):
///   0..8   magic         "ELIDSEG\x02"
///   8..12  entry_count   u32 le
///   12..16 index_length  u32 le
///   16..20 inline_length u32 le
///   20..28 body_length   u64 le
///   28..32 delta_length  u32 le
///   32..96 signature     Ed25519 (64 bytes)
const SEGMENT_HEADER_LEN: usize = 96;
const SEGMENT_MAGIC: &[u8; 8] = b"ELIDSEG\x02";

/// Write the three-file fetched format into `fetched_dir`:
///   `<segment_id>.idx`     — header + index + inline bytes `[0, body_section_start)`
///   `<segment_id>.body`    — body bytes (body-relative; byte 0 = first body byte)
///   `<segment_id>.present` — packed bitset, one bit per index entry; all bits set
///
/// All three files are written via tmp + rename. Commit order: `.idx` first
/// (enables rebuild on the next restart), then `.body` (enables reads), then
/// `.present`. A crash after `.idx` but before `.body` leaves an orphan `.idx`
/// which is harmless — it will be re-fetched on the next access.
fn write_fetched(fetched_dir: &Path, segment_id: &str, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() < SEGMENT_HEADER_LEN {
        return Err(io::Error::other(format!(
            "segment {segment_id}: too short to parse header ({} bytes)",
            bytes.len()
        )));
    }
    if &bytes[0..8] != SEGMENT_MAGIC {
        return Err(io::Error::other(format!("segment {segment_id}: bad magic")));
    }

    // Parse header fields (all within the first 96 bytes, already bounds-checked).
    let entry_count = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let index_length = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let inline_length = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    let body_section_start = SEGMENT_HEADER_LEN + index_length as usize + inline_length as usize;

    if bytes.len() < body_section_start {
        return Err(io::Error::other(format!(
            "segment {segment_id}: truncated before body section (need {body_section_start}, got {})",
            bytes.len()
        )));
    }

    let idx_bytes = &bytes[..body_section_start];
    let body_bytes = &bytes[body_section_start..];

    // Presence bitset: all bits set (full body fetched in this initial implementation).
    let bitset_len = (entry_count as usize).div_ceil(8);
    let present_bytes = vec![0xFFu8; bitset_len];

    std::fs::create_dir_all(fetched_dir)?;

    let idx_tmp = fetched_dir.join(format!("{segment_id}.idx.tmp"));
    let body_tmp = fetched_dir.join(format!("{segment_id}.body.tmp"));
    let present_tmp = fetched_dir.join(format!("{segment_id}.present.tmp"));

    std::fs::write(&idx_tmp, idx_bytes)?;
    std::fs::write(&body_tmp, body_bytes)?;
    std::fs::write(&present_tmp, &present_bytes)?;

    // Commit: idx first (enables index rebuild), then body (enables reads), then present.
    std::fs::rename(&idx_tmp, fetched_dir.join(format!("{segment_id}.idx")))?;
    std::fs::rename(&body_tmp, fetched_dir.join(format!("{segment_id}.body")))?;
    std::fs::rename(
        &present_tmp,
        fetched_dir.join(format!("{segment_id}.present")),
    )?;

    Ok(())
}

/// Build the S3 object key for a segment.
///
/// Format: `by_id/<volume_id>/YYYYMMDD/<segment_ulid>`
fn segment_key(volume_id: &str, ulid_str: &str) -> io::Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| io::Error::other(format!("invalid segment id '{ulid_str}': {e}")))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "by_id/{volume_id}/{date}/{ulid_str}"
    )))
}

/// Extract the volume ULID from a fork directory path.
///
/// In the flat layout every volume lives at `<data_dir>/by_id/<ulid>/`.
/// The directory name is validated as a ULID.
pub fn derive_volume_id(dir: &Path) -> io::Result<String> {
    let name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("fork dir has no name"))?;
    ulid::Ulid::from_string(name)
        .map(|_| name.to_owned())
        .map_err(|e| io::Error::other(format!("fork dir name is not a valid ULID '{name}': {e}")))
}

/// Build the ancestry chain as a list of volume ULIDs (oldest-first)
/// from a list of fork directories returned by `Volume::fork_dirs()`.
pub fn ancestry_chain(fork_dirs: &[PathBuf]) -> io::Result<Vec<String>> {
    fork_dirs.iter().map(|d| derive_volume_id(d)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal segment file in memory and verify that `write_fetched`
    /// produces well-formed `.idx`, `.body`, and `.present` files.
    #[test]
    fn write_fetched_splits_correctly() {
        use elide_core::segment::{
            SegmentEntry, collect_fetched_idx_files, read_segment_index, write_segment,
        };
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("elide-fetcher-test-{}-{}", std::process::id(), n));
        let seg_path = dir.join("01AAAAAAAAAAAAAAAAAAAAAAAA.full");
        std::fs::create_dir_all(&dir).unwrap();

        // Write a real segment with two data entries.
        let data1 = vec![0x11u8; 4096];
        let data2 = vec![0x22u8; 8192];
        let h1 = blake3::hash(&data1);
        let h2 = blake3::hash(&data2);
        let mut entries = vec![
            SegmentEntry::new_data(h1, 0, 1, 0, data1.clone()),
            SegmentEntry::new_data(h2, 1, 2, 0, data2.clone()),
        ];
        let bss = write_segment(&seg_path, &mut entries, None).unwrap();

        // Read the full segment bytes and split them via write_fetched.
        let full_bytes = std::fs::read(&seg_path).unwrap();
        let fetched_dir = dir.join("fetched");
        let segment_id = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        write_fetched(&fetched_dir, segment_id, &full_bytes).unwrap();

        // Check .idx file: should be parseable and match the original index.
        let idx_path = fetched_dir.join(format!("{segment_id}.idx"));
        let (bss2, idx_entries) = read_segment_index(&idx_path).unwrap();
        assert_eq!(bss, bss2, "body_section_start must match");
        assert_eq!(idx_entries.len(), 2);
        assert_eq!(idx_entries[0].hash, h1);
        assert_eq!(idx_entries[1].hash, h2);
        assert_eq!(idx_entries[0].stored_offset, 0);
        assert_eq!(idx_entries[0].stored_length, 4096);
        assert_eq!(idx_entries[1].stored_offset, 4096);
        assert_eq!(idx_entries[1].stored_length, 8192);

        // Check .body file: should contain the body bytes (body-relative).
        let body_path = fetched_dir.join(format!("{segment_id}.body"));
        let body_bytes = std::fs::read(&body_path).unwrap();
        assert_eq!(
            body_bytes.len(),
            4096 + 8192,
            "body must contain both extents"
        );
        // First extent at body-relative offset 0.
        assert_eq!(&body_bytes[0..4096], data1.as_slice());
        // Second extent at body-relative offset 4096.
        assert_eq!(&body_bytes[4096..], data2.as_slice());

        // Check .present file: 2 entries → ceil(2/8) = 1 byte, all bits set.
        let present_path = fetched_dir.join(format!("{segment_id}.present"));
        let present_bytes = std::fs::read(&present_path).unwrap();
        assert_eq!(present_bytes.len(), 1);
        assert_eq!(present_bytes[0], 0xFF);

        // Check that collect_fetched_idx_files finds the .idx file.
        let found = collect_fetched_idx_files(&fetched_dir).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].file_stem().unwrap(), segment_id);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn derive_volume_id_returns_ulid() {
        let ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let id = derive_volume_id(Path::new(&format!("/data/by_id/{ulid}"))).unwrap();
        assert_eq!(id, ulid);
    }

    #[test]
    fn derive_volume_id_rejects_non_ulid() {
        assert!(derive_volume_id(Path::new("/data/by_id/not-a-ulid")).is_err());
    }

    #[test]
    fn segment_key_format() {
        use chrono::{DateTime, Utc};
        use ulid::Ulid;

        let vol_ulid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let seg_ulid = Ulid::from_parts(1743120000000, 42);
        let seg_str = seg_ulid.to_string();
        let dt: DateTime<Utc> = seg_ulid.datetime().into();
        let expected_date = dt.format("%Y%m%d").to_string();

        let key = segment_key(vol_ulid, &seg_str).unwrap();
        assert_eq!(
            key.as_ref(),
            format!("by_id/{vol_ulid}/{expected_date}/{seg_str}")
        );
    }
}
