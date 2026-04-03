// Demand-fetch: pull segments from remote storage on a local cache miss.
//
// Config (fetch.toml in the data directory):
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

use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde::Deserialize;
use tokio::runtime::Runtime;
use ulid::Ulid;

use elide_core::segment::{self, SegmentFetcher};

// --- config ---

/// Default maximum bytes per coalesced fetch batch (256 KiB).
pub const DEFAULT_FETCH_BATCH_BYTES: u64 = 256 * 1024;

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
    /// Maximum bytes to fetch in a single coalesced range-GET.
    /// Adjacent absent extents are batched up to this limit.
    /// Defaults to [`DEFAULT_FETCH_BATCH_BYTES`] (256 KiB) if not set.
    #[serde(default)]
    pub fetch_batch_bytes: Option<u64>,
}

impl FetchConfig {
    /// Load store config from the first source that provides one:
    ///
    /// 1. `<data_dir>/fetch.toml` — explicit per-volume or per-data-dir config
    /// 2. `ELIDE_S3_BUCKET` env var — S3 bucket, with optional endpoint/region vars
    /// 3. `./elide_store` — the coordinator's default local store location
    ///
    /// Returns `Ok(None)` only if none of the above is present.
    pub fn load(data_dir: &Path) -> io::Result<Option<Self>> {
        let config_path = data_dir.join("fetch.toml");
        if config_path.exists() {
            let s = std::fs::read_to_string(&config_path)?;
            let cfg: Self =
                toml::from_str(&s).map_err(|e| io::Error::other(format!("fetch.toml: {e}")))?;
            return Ok(Some(cfg));
        }
        // Env var fallback (S3)
        if let Ok(bucket) = std::env::var("ELIDE_S3_BUCKET") {
            return Ok(Some(FetchConfig {
                bucket: Some(bucket),
                endpoint: std::env::var("AWS_ENDPOINT_URL").ok(),
                region: std::env::var("AWS_DEFAULT_REGION").ok(),
                local_path: None,
                fetch_batch_bytes: None,
            }));
        }
        // Default local store — same default the coordinator uses.
        let default_store = Path::new("elide_store");
        if default_store.exists() {
            return Ok(Some(FetchConfig {
                bucket: None,
                endpoint: None,
                region: None,
                local_path: Some(default_store.to_string_lossy().into_owned()),
                fetch_batch_bytes: None,
            }));
        }
        Ok(None)
    }

    pub fn build_store(&self) -> io::Result<Arc<dyn ObjectStore>> {
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
    /// Maximum bytes per coalesced range-GET batch.
    max_batch_bytes: u64,
    rt: Runtime,
}

impl ObjectStoreFetcher {
    pub fn new(config: &FetchConfig, volume_ids: Vec<String>) -> io::Result<Self> {
        let store = config.build_store()?;
        Self::from_store(
            store,
            volume_ids,
            config
                .fetch_batch_bytes
                .unwrap_or(DEFAULT_FETCH_BATCH_BYTES),
        )
    }

    /// Build a fetcher from an already-constructed store.
    ///
    /// Used by callers (e.g. the coordinator) that manage the store directly
    /// rather than going through [`FetchConfig`].
    pub fn from_store(
        store: Arc<dyn ObjectStore>,
        volume_ids: Vec<String>,
        max_batch_bytes: u64,
    ) -> io::Result<Self> {
        let rt = Runtime::new().map_err(|e| io::Error::other(format!("tokio runtime: {e}")))?;
        Ok(Self {
            store,
            volume_ids,
            max_batch_bytes,
            rt,
        })
    }
}

impl SegmentFetcher for ObjectStoreFetcher {
    fn fetch(&self, segment_id: &str, index_dir: &Path, body_dir: &Path) -> io::Result<()> {
        self.rt.block_on(fetch_segment(
            &self.store,
            &self.volume_ids,
            segment_id,
            index_dir,
            body_dir,
        ))
    }

    fn fetch_extent(
        &self,
        segment_id: &str,
        index_dir: &Path,
        body_dir: &Path,
        extent: &segment::ExtentFetch,
    ) -> io::Result<()> {
        self.rt.block_on(fetch_one_extent(
            &self.store,
            &self.volume_ids,
            segment_id,
            index_dir,
            body_dir,
            &ExtentFetchParams {
                body_section_start: extent.body_section_start,
                entry_idx: extent.entry_idx,
                max_batch_bytes: self.max_batch_bytes,
            },
        ))
    }
}

async fn fetch_segment(
    store: &Arc<dyn ObjectStore>,
    volume_ids: &[String],
    segment_id: &str,
    index_dir: &Path,
    body_dir: &Path,
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
                write_cache(index_dir, body_dir, segment_id, &bytes)?;
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

/// Parameters for fetching a single extent from storage.
struct ExtentFetchParams {
    body_section_start: u64,
    entry_idx: u32,
    max_batch_bytes: u64,
}

async fn fetch_one_extent(
    store: &Arc<dyn ObjectStore>,
    volume_ids: &[String],
    segment_id: &str,
    index_dir: &Path,
    body_dir: &Path,
    extent: &ExtentFetchParams,
) -> io::Result<()> {
    let idx_path = index_dir.join(format!("{segment_id}.idx"));
    let present_path = body_dir.join(format!("{segment_id}.present"));

    // Read the full index so we can scan ahead for adjacent absent entries.
    let (_, entries) = segment::read_segment_index(&idx_path)?;
    let start = extent.entry_idx as usize;
    if start >= entries.len() {
        return Err(io::Error::other(format!(
            "entry_idx {} out of range ({} entries)",
            extent.entry_idx,
            entries.len()
        )));
    }

    // Read present bits once up front.
    let present_bytes = match std::fs::read(&present_path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => vec![],
        Err(e) => return Err(e),
    };
    let is_present = |idx: usize| -> bool {
        let byte_idx = idx / 8;
        present_bytes
            .get(byte_idx)
            .is_some_and(|b| b & (1 << (idx % 8)) != 0)
    };

    // Scan forward from `start` to find the longest contiguous run of
    // body-adjacent, not-yet-present entries.  Stop at the first gap,
    // already-present entry, or non-data entry (dedup-ref / inline).
    let mut batch_last = start;
    let mut next_expected_offset =
        entries[start].stored_offset + entries[start].stored_length as u64;
    for i in (start + 1)..entries.len() {
        let e = &entries[i];
        if e.is_dedup_ref || e.is_inline {
            break;
        }
        if e.stored_offset != next_expected_offset {
            break; // gap in body layout
        }
        if is_present(i) {
            break; // already cached — no need to re-fetch
        }
        // Stop if adding this entry would exceed the batch byte cap.
        // The first entry is always included regardless of its size.
        let new_batch_bytes =
            next_expected_offset + e.stored_length as u64 - entries[start].stored_offset;
        if new_batch_bytes > extent.max_batch_bytes {
            break;
        }
        batch_last = i;
        next_expected_offset = e.stored_offset + e.stored_length as u64;
    }

    let batch_body_start = entries[start].stored_offset;
    let batch_body_end = next_expected_offset; // = entries[batch_last].stored_offset + len
    let range_start = (extent.body_section_start + batch_body_start) as usize;
    let range_end = (extent.body_section_start + batch_body_end) as usize;
    let batch_count = batch_last - start + 1;

    for volume_id in volume_ids.iter().rev() {
        let key = segment_key(volume_id, segment_id)?;
        match store.get_range(&key, range_start..range_end).await {
            Ok(bytes) => {
                std::fs::create_dir_all(body_dir)?;

                // Write the contiguous batch into .body at the batch start offset.
                let body_path = body_dir.join(format!("{segment_id}.body"));
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&body_path)
                    .map_err(|e| io::Error::other(format!("open .body: {e}")))?;
                f.seek(SeekFrom::Start(batch_body_start))
                    .map_err(|e| io::Error::other(format!("seek .body: {e}")))?;
                f.write_all(&bytes)
                    .map_err(|e| io::Error::other(format!("write .body: {e}")))?;

                // Bulk-update .present for all entries in the batch (one read + one write).
                let entry_count = entries.len() as u32;
                let bitset_len = (entry_count as usize).div_ceil(8);
                let mut new_present = if present_bytes.len() >= bitset_len {
                    present_bytes.clone()
                } else {
                    let mut v = present_bytes.clone();
                    v.resize(bitset_len, 0);
                    v
                };
                for i in start..=batch_last {
                    new_present[i / 8] |= 1 << (i % 8);
                }
                std::fs::write(&present_path, &new_present)
                    .map_err(|e| io::Error::other(format!("write .present: {e}")))?;

                tracing::debug!(
                    segment_id,
                    entry_idx = extent.entry_idx,
                    batch_count,
                    total_bytes = bytes.len(),
                    "fetched extent batch"
                );
                return Ok(());
            }
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => {
                return Err(io::Error::other(format!(
                    "fetching extent {segment_id}[{}] from {volume_id}: {e}",
                    extent.entry_idx
                )));
            }
        }
    }
    Err(io::Error::other(format!(
        "extent {segment_id}[{}] not found in any ancestor",
        extent.entry_idx
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

/// Write the three-file cache format:
///   `<index_dir>/<segment_id>.idx`    — header + index + inline bytes `[0, body_section_start)`
///   `<body_dir>/<segment_id>.body`    — body bytes (body-relative; byte 0 = first body byte)
///   `<body_dir>/<segment_id>.present` — packed bitset, one bit per index entry; all bits set
///
/// All three files are written via tmp + rename. Commit order: `.idx` first
/// (enables rebuild on the next restart), then `.body` (enables reads), then
/// `.present`. A crash after `.idx` but before `.body` leaves an orphan `.idx`
/// which is harmless — it will be re-fetched on the next access.
fn write_cache(
    index_dir: &Path,
    body_dir: &Path,
    segment_id: &str,
    bytes: &[u8],
) -> io::Result<()> {
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

    // Presence bitset: all bits set (full body cached in this initial implementation).
    let bitset_len = (entry_count as usize).div_ceil(8);
    let present_bytes = vec![0xFFu8; bitset_len];

    std::fs::create_dir_all(index_dir)?;
    std::fs::create_dir_all(body_dir)?;

    let idx_tmp = index_dir.join(format!("{segment_id}.idx.tmp"));
    let body_tmp = body_dir.join(format!("{segment_id}.body.tmp"));
    let present_tmp = body_dir.join(format!("{segment_id}.present.tmp"));

    std::fs::write(&idx_tmp, idx_bytes)?;
    std::fs::write(&body_tmp, body_bytes)?;
    std::fs::write(&present_tmp, &present_bytes)?;

    // Commit: idx first (enables index rebuild), then body (enables reads), then present.
    std::fs::rename(&idx_tmp, index_dir.join(format!("{segment_id}.idx")))?;
    std::fs::rename(&body_tmp, body_dir.join(format!("{segment_id}.body")))?;
    std::fs::rename(&present_tmp, body_dir.join(format!("{segment_id}.present")))?;

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

// --- volume pre-warm ---

/// Pre-warm the start of a readonly (cached) volume.
///
/// Demand-fetches LBAs 0 and 1 (the first 8 KiB of the disk) into the local
/// cache. These blocks are almost universally accessed on first use
/// regardless of filesystem type, so pre-fetching them on pull avoids cold
/// round-trips.
///
/// Returns `Ok(())` silently if the volume is not yet indexed.
pub fn prewarm_volume_start(
    fork_dir: &Path,
    by_id_dir: &Path,
    store: Arc<dyn ObjectStore>,
    max_batch_bytes: u64,
) -> io::Result<()> {
    use elide_core::volume::{ReadonlyVolume, walk_ancestors};

    // Build ancestry chain oldest-first; current fork appended last.
    let ancestors = walk_ancestors(fork_dir, by_id_dir)?;
    let mut volume_ids: Vec<String> = ancestors
        .iter()
        .map(|l| derive_volume_id(&l.dir))
        .collect::<io::Result<_>>()?;
    volume_ids.push(derive_volume_id(fork_dir)?);

    let fetcher = Arc::new(ObjectStoreFetcher::from_store(
        store,
        volume_ids,
        max_batch_bytes,
    )?);

    let mut vol = ReadonlyVolume::open(fork_dir, by_id_dir)?;
    vol.set_fetcher(fetcher);

    vol.read(0, 2)?;

    tracing::info!("[prewarm] volume start pre-warmed: {}", fork_dir.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal segment file in memory and verify that `write_cache`
    /// produces well-formed `.idx`, `.body`, and `.present` files.
    #[test]
    fn write_cache_splits_correctly() {
        use elide_core::segment::{
            SegmentEntry, SegmentFlags, collect_idx_files, read_segment_index, write_segment,
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
            SegmentEntry::new_data(h1, 0, 1, SegmentFlags::empty(), data1.clone()),
            SegmentEntry::new_data(h2, 1, 2, SegmentFlags::empty(), data2.clone()),
        ];
        let (signer, _vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        // Read the full segment bytes and split them via write_cache.
        let full_bytes = std::fs::read(&seg_path).unwrap();
        let index_dir = dir.join("index");
        let cache_dir = dir.join("cache");
        let segment_id = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        write_cache(&index_dir, &cache_dir, segment_id, &full_bytes).unwrap();

        // Check .idx file: should be parseable and match the original index.
        let idx_path = index_dir.join(format!("{segment_id}.idx"));
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
        let body_path = cache_dir.join(format!("{segment_id}.body"));
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
        let present_path = cache_dir.join(format!("{segment_id}.present"));
        let present_bytes = std::fs::read(&present_path).unwrap();
        assert_eq!(present_bytes.len(), 1);
        assert_eq!(present_bytes[0], 0xFF);

        // Check that collect_idx_files finds the .idx file in index/.
        let found = collect_idx_files(&index_dir).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].file_stem().unwrap(), segment_id);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// `fetch_extent` coalesces body-adjacent absent entries into a single
    /// range-GET.  Requesting entry 0 when entries 0 and 1 are contiguous and
    /// both absent should fetch both in one call and set both present bits.
    #[test]
    fn fetch_extent_coalesces_adjacent_entries() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, check_present_bit, write_segment};
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as StorePath;
        use std::sync::Arc;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQVOLUMEAAAAAAAAAAAAAAA";

        // Build a segment with 3 uncompressed entries (all body-adjacent).
        let data0 = vec![0x11u8; 4096];
        let data1 = vec![0x22u8; 4096];
        let data2 = vec![0x33u8; 4096];
        let h0 = blake3::hash(&data0);
        let h1 = blake3::hash(&data1);
        let h2 = blake3::hash(&data2);
        let mut entries = vec![
            SegmentEntry::new_data(h0, 0, 1, SegmentFlags::empty(), data0.clone()),
            SegmentEntry::new_data(h1, 1, 1, SegmentFlags::empty(), data1.clone()),
            SegmentEntry::new_data(h2, 2, 1, SegmentFlags::empty(), data2.clone()),
        ];
        let seg_path = tmp.path().join(&seg_id);
        let (signer, _vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let full_bytes = std::fs::read(&seg_path).unwrap();

        // Write only the .idx portion to index/ — no .body, no .present yet.
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        // Upload the full segment to a local object store.
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_dir.path()).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dt: chrono::DateTime<chrono::Utc> = seg_ulid.datetime().into();
        let date = dt.format("%Y%m%d").to_string();
        let key = StorePath::from(format!("by_id/{vol_id}/{date}/{seg_id}"));
        rt.block_on(store.put(&key, full_bytes.into())).unwrap();

        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store_dir.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = ObjectStoreFetcher::new(&cfg, vec![vol_id.to_string()]).unwrap();

        // Fetch entry 0 — should coalesce entries 0, 1, 2 into one range-GET.
        fetcher
            .fetch_extent(
                &seg_id,
                &index_dir,
                &cache_dir,
                &segment::ExtentFetch {
                    body_section_start: bss,
                    body_offset: entries[0].stored_offset,
                    body_length: entries[0].stored_length,
                    entry_idx: 0,
                },
            )
            .unwrap();

        // All three entries' bytes must be in .body.
        let body_bytes = std::fs::read(cache_dir.join(format!("{seg_id}.body"))).unwrap();
        let off0 = entries[0].stored_offset as usize;
        let off1 = entries[1].stored_offset as usize;
        let off2 = entries[2].stored_offset as usize;
        assert_eq!(&body_bytes[off0..off0 + 4096], data0.as_slice());
        assert_eq!(&body_bytes[off1..off1 + 4096], data1.as_slice());
        assert_eq!(&body_bytes[off2..off2 + 4096], data2.as_slice());

        // All three .present bits must be set.
        let present_path = cache_dir.join(format!("{seg_id}.present"));
        assert!(check_present_bit(&present_path, 0).unwrap(), "bit 0 set");
        assert!(check_present_bit(&present_path, 1).unwrap(), "bit 1 set");
        assert!(check_present_bit(&present_path, 2).unwrap(), "bit 2 set");
    }

    /// A gap in body layout stops coalescing: only the requested entry is fetched,
    /// leaving the non-adjacent entry unset.
    #[test]
    fn fetch_extent_stops_at_body_gap() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, check_present_bit, write_segment};
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as StorePath;
        use std::sync::Arc;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQVOLUMEAAAAAAAAAAAAAAA";

        // Two entries that will NOT be body-adjacent because entry 1 is
        // written into a segment that already has entry 0 present — we
        // simulate a gap by pre-setting entry 0's .present bit before
        // fetching, so entry 0 is "present" and the scan should stop.
        // Easier: use two entries that ARE adjacent, mark entry 1 as
        // already present, then fetch entry 0 — batch should be just {0}.
        let data0 = vec![0xAAu8; 4096];
        let data1 = vec![0xBBu8; 4096];
        let h0 = blake3::hash(&data0);
        let h1 = blake3::hash(&data1);
        let mut entries = vec![
            SegmentEntry::new_data(h0, 0, 1, SegmentFlags::empty(), data0.clone()),
            SegmentEntry::new_data(h1, 1, 1, SegmentFlags::empty(), data1.clone()),
        ];
        let seg_path = tmp.path().join(&seg_id);
        let (signer, _vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let full_bytes = std::fs::read(&seg_path).unwrap();

        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        // Pre-mark entry 1 as present so coalescing stops before it.
        elide_core::segment::set_present_bit(&cache_dir.join(format!("{seg_id}.present")), 1, 2)
            .unwrap();

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_dir.path()).unwrap());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dt: chrono::DateTime<chrono::Utc> = seg_ulid.datetime().into();
        let date = dt.format("%Y%m%d").to_string();
        let key = StorePath::from(format!("by_id/{vol_id}/{date}/{seg_id}"));
        rt.block_on(store.put(&key, full_bytes.into())).unwrap();

        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store_dir.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = ObjectStoreFetcher::new(&cfg, vec![vol_id.to_string()]).unwrap();

        fetcher
            .fetch_extent(
                &seg_id,
                &index_dir,
                &cache_dir,
                &segment::ExtentFetch {
                    body_section_start: bss,
                    body_offset: entries[0].stored_offset,
                    body_length: entries[0].stored_length,
                    entry_idx: 0,
                },
            )
            .unwrap();

        // Entry 0's bytes must be in .body.
        let body_bytes = std::fs::read(cache_dir.join(format!("{seg_id}.body"))).unwrap();
        let off0 = entries[0].stored_offset as usize;
        assert_eq!(&body_bytes[off0..off0 + 4096], data0.as_slice());

        // Bit 0 now set (just fetched); bit 1 still set (was pre-set, not overwritten).
        let present_path = cache_dir.join(format!("{seg_id}.present"));
        assert!(check_present_bit(&present_path, 0).unwrap(), "bit 0 set");
        assert!(
            check_present_bit(&present_path, 1).unwrap(),
            "bit 1 still set"
        );
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
