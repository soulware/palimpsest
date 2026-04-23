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
//   by_id/<volume_id>/segments/YYYYMMDD/<ulid>
//
// Fetch sequence per extent miss:
//   1. Read the local .idx to find the extent's S3 byte range
//   2. Try each fork in the ancestry chain (newest first)
//   3. On first hit: write body bytes into cache/<ulid>.body, set present bit
//   4. On all-miss: return a NotFound error
//
// This crate is intentionally synchronous: the volume process owns the
// demand-fetch path and is sync end-to-end. The S3 client is `rust-s3` with
// the `sync` feature (attohttpc transport, no tokio). The coordinator, which
// runs tokio for its own reasons, adapts its `object_store` to the
// `RangeFetcher` trait when it needs to construct a fetcher.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use elide_core::signing::VerifyingKey;
use s3::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::Deserialize;
use ulid::Ulid;

use elide_core::segment::{self, SegmentFetcher};
use elide_core::signing;

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
    ///
    /// Note: CLI commands (`remote list`, `remote pull`, `create --from`)
    /// should prefer asking a running coordinator for config+creds via its
    /// IPC socket before falling through to this function. `FetchConfig`
    /// intentionally does not read `coordinator.toml` itself — the config
    /// there names a bucket but the matching `AWS_ACCESS_KEY_ID` /
    /// `AWS_SECRET_ACCESS_KEY` live in the coordinator's process env, not
    /// on disk, so consuming just the `[store]` section would give the
    /// illusion of being configured without having usable credentials.
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

    /// Build a `RangeFetcher` from this config.
    ///
    /// Returns a [`LocalRangeFetcher`] when `local_path` is set, otherwise an
    /// [`S3RangeFetcher`] using the configured bucket / endpoint / region.
    pub fn build_fetcher(&self) -> io::Result<Arc<dyn RangeFetcher>> {
        if let Some(path) = &self.local_path {
            return Ok(Arc::new(LocalRangeFetcher::new(PathBuf::from(path))));
        }
        let bucket = self.bucket.as_deref().ok_or_else(|| {
            io::Error::other("fetch.toml: one of 'bucket' or 'local_path' is required")
        })?;
        let fetcher =
            S3RangeFetcher::new(bucket, self.endpoint.as_deref(), self.region.as_deref())?;
        Ok(Arc::new(fetcher))
    }
}

// --- range-fetcher abstraction ---

/// Synchronous range-fetch interface: read a byte range from a key in some
/// backing store. Implementations exist for S3 (`S3RangeFetcher`) and local
/// filesystem (`LocalRangeFetcher`); the coordinator wraps its
/// `object_store::ObjectStore` to satisfy this trait when it constructs a
/// `RemoteFetcher`.
///
/// The `end` bound is exclusive (matches Rust range semantics, not HTTP
/// `Range:` semantics — implementations translate as needed).
///
/// Implementations must return `io::ErrorKind::NotFound` when the key does
/// not exist. The fetcher walks the ancestry chain on `NotFound` and treats
/// any other error as fatal.
pub trait RangeFetcher: Send + Sync {
    fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>>;
}

/// `RangeFetcher` over a local directory tree. The key is appended to `root`
/// to form a filesystem path. Used for tests and for the coordinator's
/// default local-store mode (`elide_store/`).
pub struct LocalRangeFetcher {
    root: PathBuf,
}

impl LocalRangeFetcher {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl RangeFetcher for LocalRangeFetcher {
    fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
        let path = self.root.join(key);
        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(start))?;
        let len = end_exclusive.saturating_sub(start) as usize;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
}

/// `RangeFetcher` backed by a `rust-s3` blocking (`sync` feature) client.
pub struct S3RangeFetcher {
    bucket: Box<Bucket>,
}

impl S3RangeFetcher {
    /// Build an S3 fetcher.
    ///
    /// `endpoint` is optional — set it for S3-compatible stores (MinIO,
    /// Garage, R2, …). When set, path-style addressing is used; without an
    /// endpoint, virtual-host style is used (the AWS default).
    ///
    /// `region` is required by AWS but accepted as a free-form string for
    /// custom endpoints (R2 uses `"auto"`, Garage uses `"garage"`, etc.).
    /// Falls back to `AWS_DEFAULT_REGION`, then to `us-east-1`.
    ///
    /// Credentials come from `aws-creds`'s default chain (env vars, profile,
    /// IMDS).
    pub fn new(
        bucket_name: &str,
        endpoint: Option<&str>,
        region: Option<&str>,
    ) -> io::Result<Self> {
        let region_name = region
            .map(|s| s.to_owned())
            .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
            .unwrap_or_else(|| "us-east-1".to_owned());
        let region = match endpoint {
            Some(ep) => Region::Custom {
                region: region_name,
                endpoint: ep.to_owned(),
            },
            None => region_name
                .parse::<Region>()
                .map_err(|e| io::Error::other(format!("aws region '{region_name}': {e}")))?,
        };
        let creds = Credentials::default()
            .map_err(|e| io::Error::other(format!("aws credentials: {e}")))?;
        let mut bucket = Bucket::new(bucket_name, region, creds)
            .map_err(|e| io::Error::other(format!("s3 bucket {bucket_name}: {e}")))?;
        if endpoint.is_some() {
            bucket = bucket.with_path_style();
        }
        Ok(Self { bucket })
    }
}

impl RangeFetcher for S3RangeFetcher {
    fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
        // rust-s3 panics inside `get_object_range` when `start > end_inclusive`;
        // reject the empty-range case up front so the caller gets an error
        // rather than a dead thread.
        if end_exclusive <= start {
            return Err(io::Error::other(format!(
                "s3 get_range {key}: empty range [{start}, {end_exclusive})"
            )));
        }
        // HTTP Range is inclusive on both ends; our trait uses exclusive end.
        let end_inclusive = end_exclusive - 1;
        let resp = self
            .bucket
            .get_object_range(key, start, Some(end_inclusive))
            .map_err(|e| io::Error::other(format!("s3 get_range {key}: {e}")))?;
        match resp.status_code() {
            200 | 206 => Ok(resp.as_slice().to_vec()),
            404 => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{key} not found"),
            )),
            n => Err(io::Error::other(format!("s3 get_range {key}: status {n}"))),
        }
    }
}

// --- fetcher ---

/// Demand-fetcher implementing `SegmentFetcher` on top of a sync
/// [`RangeFetcher`]. Holds the ancestry chain (oldest-first) and the per-batch
/// coalescing budget.
pub struct RemoteFetcher {
    store: Arc<dyn RangeFetcher>,
    /// Volume ancestry chain oldest-first: (volume_ulid, verifying_key).
    /// Searched newest-first on a cache miss; verifying key used to verify
    /// fetched segment bytes before writing to the local cache.
    chain: Vec<(String, VerifyingKey)>,
    /// Maximum bytes per coalesced range-GET batch.
    max_batch_bytes: u64,
}

impl RemoteFetcher {
    /// Build a fetcher from a [`FetchConfig`] and the fork directories in the
    /// ancestry chain (oldest-first, current fork last).
    ///
    /// Each fork directory must contain a `volume.pub` file. The volume ULID
    /// is derived from the directory name.
    pub fn new(config: &FetchConfig, fork_dirs: &[PathBuf]) -> io::Result<Self> {
        let store = config.build_fetcher()?;
        Self::from_store(
            store,
            fork_dirs,
            config
                .fetch_batch_bytes
                .unwrap_or(DEFAULT_FETCH_BATCH_BYTES),
        )
    }

    /// Build a fetcher from an already-constructed `RangeFetcher`.
    ///
    /// Used by callers (e.g. the coordinator) that manage the store directly
    /// rather than going through [`FetchConfig`].
    pub fn from_store(
        store: Arc<dyn RangeFetcher>,
        fork_dirs: &[PathBuf],
        max_batch_bytes: u64,
    ) -> io::Result<Self> {
        let chain = load_chain(fork_dirs)?;
        Ok(Self {
            store,
            chain,
            max_batch_bytes,
        })
    }
}

impl SegmentFetcher for RemoteFetcher {
    fn fetch_extent(
        &self,
        segment_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
        extent: &segment::ExtentFetch,
    ) -> io::Result<()> {
        fetch_one_extent(
            &*self.store,
            &self.chain,
            &segment_id.to_string(),
            index_dir,
            body_dir,
            &ExtentFetchParams {
                body_section_start: extent.body_section_start,
                entry_idx: extent.entry_idx,
                max_batch_bytes: self.max_batch_bytes,
            },
        )
    }

    fn fetch_delta_body(
        &self,
        segment_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
    ) -> io::Result<()> {
        fetch_one_delta_body(
            &*self.store,
            &self.chain,
            &segment_id.to_string(),
            index_dir,
            body_dir,
        )
    }
}

/// Load the ancestry chain from fork directories.
///
/// Each directory must be named with a valid ULID and contain a `volume.pub`
/// file. Returns `(volume_ulid, verifying_key)` pairs in the same order as
/// `fork_dirs` (oldest-first).
fn load_chain(fork_dirs: &[PathBuf]) -> io::Result<Vec<(String, VerifyingKey)>> {
    fork_dirs
        .iter()
        .map(|dir| {
            let id = derive_volume_id(dir)?;
            let vk = signing::load_verifying_key(dir, signing::VOLUME_PUB_FILE)?;
            Ok((id, vk))
        })
        .collect()
}

/// Parameters for fetching a single extent from storage.
struct ExtentFetchParams {
    body_section_start: u64,
    entry_idx: u32,
    max_batch_bytes: u64,
}

fn fetch_one_extent(
    store: &dyn RangeFetcher,
    chain: &[(String, VerifyingKey)],
    segment_id: &str,
    index_dir: &Path,
    body_dir: &Path,
    extent: &ExtentFetchParams,
) -> io::Result<()> {
    let idx_path = index_dir.join(format!("{segment_id}.idx"));
    let present_path = body_dir.join(format!("{segment_id}.present"));

    // Read the full index so we can scan ahead for adjacent absent entries.
    let (_, entries, _) = segment::read_segment_index(&idx_path)?;
    let start = extent.entry_idx as usize;
    if start >= entries.len() {
        return Err(io::Error::other(format!(
            "entry_idx {} out of range ({} entries)",
            extent.entry_idx,
            entries.len()
        )));
    }
    // Body-section demand-fetch only applies to Data / CanonicalData entries
    // with a non-empty stored body. Anything else means an `ExtentLocation`
    // upstream was built with `BodySource::Cached(idx)` pointing at the
    // wrong kind of entry — that's a bug and an empty range-GET would
    // otherwise panic the S3 client.
    let start_entry = &entries[start];
    if !start_entry.kind.is_data() || start_entry.stored_length == 0 {
        return Err(io::Error::other(format!(
            "demand-fetch {segment_id}[{}]: expected data-kind entry with non-empty body, \
             got kind={:?} stored_length={}",
            start, start_entry.kind, start_entry.stored_length
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
        // Only Data / CanonicalData entries carry body bytes addressable
        // via stored_offset into the body section. DedupRef is thin;
        // Inline / CanonicalInline live in the inline section.
        if !e.kind.is_data() {
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
    let range_start = extent.body_section_start + batch_body_start;
    let range_end = extent.body_section_start + batch_body_end;
    let batch_count = batch_last - start + 1;

    for (volume_id, _) in chain.iter().rev() {
        let key = segment_key(volume_id, segment_id)?;
        match store.get_range(&key, range_start, range_end) {
            Ok(bytes) => {
                // Hash-verify every entry in the batch before touching disk.
                // The .idx is signed and authentic, so each entry's declared
                // hash is trusted; the bytes we just pulled from the store
                // are not. A mismatch here means either the remote object is
                // tampered/corrupt or the range slice is wrong — in either
                // case the batch is rejected and we try the next ancestor.
                if bytes.len() as u64 != range_end - range_start {
                    return Err(io::Error::other(format!(
                        "short range-GET for {segment_id}: expected {} bytes, got {}",
                        range_end - range_start,
                        bytes.len(),
                    )));
                }
                for (i, e) in entries.iter().enumerate().take(batch_last + 1).skip(start) {
                    let rel = (e.stored_offset - batch_body_start) as usize;
                    let len = e.stored_length as usize;
                    let slice = bytes.get(rel..rel + len).ok_or_else(|| {
                        io::Error::other(format!(
                            "entry {i} out of batch bounds while verifying {segment_id}"
                        ))
                    })?;
                    segment::verify_body_hash(e, slice).map_err(|err| {
                        io::Error::new(err.kind(), format!("demand-fetch {segment_id}[{i}]: {err}"))
                    })?;
                }

                std::fs::create_dir_all(body_dir)?;

                // Durability ordering: write .body bytes and fsync them before
                // publishing the present bit.  The invariant is "a set present
                // bit implies the body bytes are durable" — the opposite
                // reordering (bit durable, bytes still in page cache) would
                // surface as a silent read of zeros after a crash.  .present is
                // then written via write_file_atomic (tmp + rename + dir fsync)
                // so a torn write can't leave a partially updated bitset.
                let body_path = body_dir.join(format!("{segment_id}.body"));
                let body_is_new = !body_path.exists();
                {
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
                    f.sync_data()
                        .map_err(|e| io::Error::other(format!("fsync .body: {e}")))?;
                }
                // If this call created the .body file, fsync the parent dir so
                // the new directory entry is durable before the present bit
                // that references it.
                if body_is_new {
                    std::fs::File::open(body_dir)
                        .and_then(|d| d.sync_all())
                        .map_err(|e| io::Error::other(format!("fsync body dir: {e}")))?;
                }

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
                segment::write_file_atomic(&present_path, &new_present)
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
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
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

/// Fetch a segment's delta body section and write it to `body_dir/<id>.delta`.
///
/// Reads layout from the local `.idx` (header carries `body_length` and
/// `delta_length`), issues one range-GET for exactly the delta region,
/// and writes the result atomically (tmp+rename). Searches the ancestry
/// chain newest-first on NotFound, mirroring `fetch_one_extent`.
fn fetch_one_delta_body(
    store: &dyn RangeFetcher,
    chain: &[(String, VerifyingKey)],
    segment_id: &str,
    index_dir: &Path,
    body_dir: &Path,
) -> io::Result<()> {
    let idx_path = index_dir.join(format!("{segment_id}.idx"));
    let layout = segment::read_segment_layout(&idx_path)?;
    if layout.delta_length == 0 {
        return Err(io::Error::other(format!(
            "segment {segment_id} has no delta body to fetch"
        )));
    }

    let delta_path = body_dir.join(format!("{segment_id}.delta"));
    if delta_path.try_exists()? {
        return Ok(());
    }

    let range_start = layout.body_section_start + layout.body_length;
    let range_end = range_start + layout.delta_length as u64;

    // Read the signed .idx so each delta option's authentic `delta_hash`
    // is available for verifying the bytes we pull from the remote store.
    let (_, entries, _) = segment::read_segment_index(&idx_path)?;

    for (volume_id, _) in chain.iter().rev() {
        let key = segment_key(volume_id, segment_id)?;
        match store.get_range(&key, range_start, range_end) {
            Ok(bytes) => {
                if bytes.len() as u64 != range_end - range_start {
                    return Err(io::Error::other(format!(
                        "short range-GET for delta body {segment_id}: expected {} bytes, got {}",
                        range_end - range_start,
                        bytes.len(),
                    )));
                }
                // Verify every delta option's blob against its signed
                // `delta_hash`. The delta body section is outside the
                // segment signature, so the hash in each option is the
                // only authentication. A mismatch means tamper or
                // corruption in the remote object — refuse the batch.
                for (i, e) in entries.iter().enumerate() {
                    for (j, opt) in e.delta_options.iter().enumerate() {
                        let start = opt.delta_offset as usize;
                        let end = start + opt.delta_length as usize;
                        let slice = bytes.get(start..end).ok_or_else(|| {
                            io::Error::other(format!(
                                "delta option {segment_id}[{i}][{j}] out of bounds \
                                 (offset {start}, length {}) for delta body of {} bytes",
                                opt.delta_length,
                                bytes.len(),
                            ))
                        })?;
                        segment::verify_delta_blob_hash(opt, slice).map_err(|err| {
                            io::Error::new(
                                err.kind(),
                                format!("demand-fetch delta {segment_id}[{i}][{j}]: {err}"),
                            )
                        })?;
                    }
                }

                std::fs::create_dir_all(body_dir)?;
                let tmp_path = body_dir.join(format!("{segment_id}.delta.tmp"));
                {
                    let mut f = std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&tmp_path)
                        .map_err(|e| io::Error::other(format!("open .delta.tmp: {e}")))?;
                    f.write_all(&bytes)
                        .map_err(|e| io::Error::other(format!("write .delta.tmp: {e}")))?;
                    f.sync_data()
                        .map_err(|e| io::Error::other(format!("fsync .delta.tmp: {e}")))?;
                }
                std::fs::rename(&tmp_path, &delta_path)
                    .map_err(|e| io::Error::other(format!("rename .delta: {e}")))?;
                tracing::debug!(segment_id, bytes = bytes.len(), "fetched delta body");
                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(io::Error::other(format!(
                    "fetching delta body {segment_id} from {volume_id}: {e}"
                )));
            }
        }
    }
    Err(io::Error::other(format!(
        "delta body {segment_id} not found in any ancestor"
    )))
}

/// Build the S3 object key for a segment.
///
/// Format: `by_id/<volume_id>/segments/YYYYMMDD/<segment_ulid>`
fn segment_key(volume_id: &str, ulid_str: &str) -> io::Result<String> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| io::Error::other(format!("invalid segment id '{ulid_str}': {e}")))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(format!("by_id/{volume_id}/segments/{date}/{ulid_str}"))
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
    store: Arc<dyn RangeFetcher>,
    max_batch_bytes: u64,
) -> io::Result<()> {
    use elide_core::volume::{ReadonlyVolume, walk_ancestors};

    // Build fork dir list oldest-first; current fork appended last.
    let ancestors = walk_ancestors(fork_dir, by_id_dir)?;
    let mut fork_dirs: Vec<PathBuf> = ancestors.iter().map(|l| l.dir.clone()).collect();
    fork_dirs.push(fork_dir.to_path_buf());

    let fetcher = Arc::new(RemoteFetcher::from_store(
        store,
        &fork_dirs,
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

    /// Helper: write `bytes` into the local store at the given key path,
    /// creating parent directories as needed.
    fn put_local(root: &Path, key: &str, bytes: &[u8]) {
        let path = root.join(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, bytes).unwrap();
    }

    /// `fetch_extent` coalesces body-adjacent absent entries into a single
    /// range-GET.  Requesting entry 0 when entries 0 and 1 are contiguous and
    /// both absent should fetch both in one call and set both present bits.
    #[test]
    fn fetch_extent_coalesces_adjacent_entries() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, check_present_bit, write_segment};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQAAAAAAAAAAAAAAAAAAAAAA";

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
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let full_bytes = std::fs::read(&seg_path).unwrap();

        // Write only the .idx portion to index/ — no .body, no .present yet.
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        // Upload the full segment to the local store.
        let key = segment_key(vol_id, &seg_id).unwrap();
        put_local(store_dir.path(), &key, &full_bytes);

        // Create a fork dir named with vol_id and write volume.pub so the
        // fetcher can load the verifying key for signature verification.
        let vol_dir = tmp.path().join(vol_id);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let pub_hex = vk
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        std::fs::write(vol_dir.join("volume.pub"), pub_hex.as_bytes()).unwrap();

        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store_dir.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = RemoteFetcher::new(&cfg, &[vol_dir]).unwrap();

        // Fetch entry 0 — should coalesce entries 0, 1, 2 into one range-GET.
        fetcher
            .fetch_extent(
                seg_ulid,
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
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQAAAAAAAAAAAAAAAAAAAAAA";

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
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
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

        let key = segment_key(vol_id, &seg_id).unwrap();
        put_local(store_dir.path(), &key, &full_bytes);

        // Create a fork dir named with vol_id and write volume.pub.
        let vol_dir = tmp.path().join(vol_id);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let pub_hex = vk
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        std::fs::write(vol_dir.join("volume.pub"), pub_hex.as_bytes()).unwrap();

        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store_dir.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = RemoteFetcher::new(&cfg, &[vol_dir]).unwrap();

        fetcher
            .fetch_extent(
                seg_ulid,
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

    /// Tampered body bytes in the remote store fail per-entry hash verification
    /// at fetch time: no `.body` is written and no present bits are set.
    #[test]
    fn fetch_extent_rejects_tampered_body() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, check_present_bit, write_segment};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQAAAAAAAAAAAAAAAAAAAAAA";

        let data0 = vec![0x55u8; 4096];
        let h0 = blake3::hash(&data0);
        let mut entries = vec![SegmentEntry::new_data(
            h0,
            0,
            1,
            SegmentFlags::empty(),
            data0.clone(),
        )];
        let seg_path = tmp.path().join(&seg_id);
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let mut full_bytes = std::fs::read(&seg_path).unwrap();

        // Write the signed .idx — the signed section is authentic.
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        // Flip a byte inside the body section. Bodies are not covered by the
        // segment signature, so the .idx is still valid — only per-entry hash
        // verification catches this.
        let body_byte = bss as usize + entries[0].stored_offset as usize + 17;
        full_bytes[body_byte] ^= 0xFF;
        let key = segment_key(vol_id, &seg_id).unwrap();
        put_local(store_dir.path(), &key, &full_bytes);

        let vol_dir = tmp.path().join(vol_id);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let pub_hex = vk
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        std::fs::write(vol_dir.join("volume.pub"), pub_hex.as_bytes()).unwrap();

        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store_dir.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = RemoteFetcher::new(&cfg, &[vol_dir]).unwrap();

        let err = fetcher
            .fetch_extent(
                seg_ulid,
                &index_dir,
                &cache_dir,
                &segment::ExtentFetch {
                    body_section_start: bss,
                    body_offset: entries[0].stored_offset,
                    body_length: entries[0].stored_length,
                    entry_idx: 0,
                },
            )
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("demand-fetch"),
            "error should identify the source: {err}"
        );

        // The .body file must NOT exist and no present bit may be set.
        assert!(
            !cache_dir.join(format!("{seg_id}.body")).exists(),
            ".body must not be written on hash mismatch"
        );
        let present_path = cache_dir.join(format!("{seg_id}.present"));
        if present_path.exists() {
            assert!(
                !check_present_bit(&present_path, 0).unwrap(),
                "bit 0 must not be set when verification fails"
            );
        }
    }

    /// A tampered delta blob in the remote store fails per-option hash
    /// verification at delta-body fetch time: no `.delta` is written.
    #[test]
    fn fetch_delta_body_rejects_tampered_blob() {
        use elide_core::segment::{DeltaOption, SegmentEntry, write_segment_with_delta_body};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQAAAAAAAAAAAAAAAAAAAAAA";

        // One Delta entry with one option. The delta blob is 256 bytes; we
        // record its hash in the option, build the segment, then tamper the
        // blob in the stored object.
        let delta_blob = vec![0x77u8; 256];
        let delta_hash = blake3::hash(&delta_blob);
        let option = DeltaOption {
            source_hash: blake3::hash(b"source"),
            delta_offset: 0,
            delta_length: delta_blob.len() as u32,
            delta_hash,
        };
        let reconstructed_hash = blake3::hash(b"reconstructed-content");
        let mut entries = vec![SegmentEntry::new_delta(
            reconstructed_hash,
            0,
            1,
            vec![option],
        )];
        let seg_path = tmp.path().join(&seg_id);
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        write_segment_with_delta_body(&seg_path, &mut entries, &delta_blob, signer.as_ref())
            .unwrap();
        let mut full_bytes = std::fs::read(&seg_path).unwrap();

        // Publish the signed .idx locally.
        let layout = elide_core::segment::read_segment_layout(&seg_path).unwrap();
        let bss = layout.body_section_start;
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        // Tamper a byte inside the delta body section of the stored object.
        let delta_start = (bss + layout.body_length) as usize;
        full_bytes[delta_start + 3] ^= 0xFF;
        let key = segment_key(vol_id, &seg_id).unwrap();
        put_local(store_dir.path(), &key, &full_bytes);

        let vol_dir = tmp.path().join(vol_id);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let pub_hex = vk
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        std::fs::write(vol_dir.join("volume.pub"), pub_hex.as_bytes()).unwrap();

        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store_dir.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = RemoteFetcher::new(&cfg, &[vol_dir]).unwrap();

        let err = fetcher
            .fetch_delta_body(seg_ulid, &index_dir, &cache_dir)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("demand-fetch delta"),
            "error should identify delta fetch: {err}"
        );

        // .delta must not exist.
        assert!(
            !cache_dir.join(format!("{seg_id}.delta")).exists(),
            ".delta must not be written on hash mismatch"
        );
    }

    /// A segment with a flipped body byte fails signature verification.
    #[test]
    fn verify_rejects_tampered_bytes() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let seg_path = tmp.path().join("seg");
        let data = vec![0xABu8; 4096];
        let mut entries = vec![SegmentEntry::new_data(
            blake3::hash(&data),
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        let mut bytes = std::fs::read(&seg_path).unwrap();
        // Flip the first byte of the index section (covered by the signature).
        // The body is NOT covered by the signature, so tampering there would pass.
        bytes[96] ^= 0xFF;

        let err = elide_core::segment::verify_segment_bytes(&bytes, "test", &vk).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid signature"));
    }

    /// A segment signed with key A is rejected when verified with key B.
    #[test]
    fn verify_rejects_wrong_key() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let seg_path = tmp.path().join("seg");
        let data = vec![0xCDu8; 4096];
        let mut entries = vec![SegmentEntry::new_data(
            blake3::hash(&data),
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let (signer_a, _vk_a) = elide_core::signing::generate_ephemeral_signer();
        let (_signer_b, vk_b) = elide_core::signing::generate_ephemeral_signer();
        write_segment(&seg_path, &mut entries, signer_a.as_ref()).unwrap();

        let bytes = std::fs::read(&seg_path).unwrap();
        let err = elide_core::segment::verify_segment_bytes(&bytes, "test", &vk_b).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid signature"));
    }

    /// A segment with a zeroed signature field is rejected as unsigned.
    #[test]
    fn verify_rejects_unsigned_segment() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let seg_path = tmp.path().join("seg");
        let data = vec![0xEFu8; 4096];
        let mut entries = vec![SegmentEntry::new_data(
            blake3::hash(&data),
            0,
            1,
            SegmentFlags::empty(),
            data,
        )];
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();

        let mut bytes = std::fs::read(&seg_path).unwrap();
        // Zero out the signature field at header[36..100] (segment v5).
        bytes[36..100].fill(0);

        let err = elide_core::segment::verify_segment_bytes(&bytes, "test", &vk).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("unsigned"));
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
            key,
            format!("by_id/{vol_ulid}/segments/{expected_date}/{seg_str}")
        );
    }

    #[test]
    fn local_range_fetcher_returns_not_found() {
        use tempfile::TempDir;
        let tmp = TempDir::new().unwrap();
        let f = LocalRangeFetcher::new(tmp.path().to_path_buf());
        let err = f.get_range("missing/key", 0, 16).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn local_range_fetcher_reads_range() {
        use tempfile::TempDir;
        let tmp = TempDir::new().unwrap();
        put_local(tmp.path(), "a/b/c", b"0123456789");
        let f = LocalRangeFetcher::new(tmp.path().to_path_buf());
        let bytes = f.get_range("a/b/c", 2, 6).unwrap();
        assert_eq!(&bytes, b"2345");
    }
}
