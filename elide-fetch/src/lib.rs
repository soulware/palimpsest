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

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use s3::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use serde::Deserialize;
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

// --- in-flight coalescing ---

/// Cross-thread coalescing for in-flight `fetch_one_extent` calls.
///
/// Two callers whose batches overlap on the same segment must not
/// race: each batch ends with a write to `cache/<sid>.body` and a
/// rewrite of `cache/<sid>.present`, and the wasted S3 egress on
/// duplicate range-GETs is exactly what we want to avoid. This
/// coordinator gives the first arrival a [`BatchLease`]; subsequent
/// callers whose `[start, end]` entry-range overlaps a live lease
/// block on its `done` condvar, then re-loop in
/// [`fetch_one_extent`] to recompute their batch against the fresh
/// `.present` bytes (their target may now be present, or covered
/// only partially).
///
/// Disjoint batches on the same segment do **not** block each other:
/// the overlap test is per-batch, not per-segment, so two readers
/// fetching from far ends of one segment proceed in parallel.
///
/// On lease drop (RAII), the entry is removed and waiters are
/// notified — covers success, error, and panic uniformly.
///
/// `present_locks` is a separate per-segment serializer for the
/// `.present` read-modify-write phase of [`fetch_batch`]. Disjoint
/// leases write disjoint bits, but the on-disk `.present` is one
/// file per segment — without this lock the RMW from a stale
/// in-memory snapshot would silently overwrite a concurrent leader's
/// bits and force the next reader to re-fetch already-cached body
/// bytes.
#[derive(Clone, Default)]
pub(crate) struct FetchCoalescer {
    state: Arc<Mutex<Vec<InFlight>>>,
    present_locks: Arc<Mutex<HashMap<Ulid, Arc<Mutex<()>>>>>,
}

struct InFlight {
    segment: Ulid,
    /// Inclusive entry-index range this lease claims.
    start: u32,
    end: u32,
    /// `(completed, condvar)` — set + notified on lease drop.
    done: Arc<(Mutex<bool>, std::sync::Condvar)>,
}

impl FetchCoalescer {
    fn new() -> Self {
        Self::default()
    }

    /// Return the per-segment write lock for `.present`. Lazily created
    /// on first use. Held by [`fetch_batch`] across the read-modify-write
    /// of `.present` so concurrent disjoint leases on the same segment
    /// merge their bits against the freshest on-disk state instead of
    /// each other's stale snapshots.
    fn present_lock_for(&self, segment: Ulid) -> Arc<Mutex<()>> {
        let mut map = self
            .present_locks
            .lock()
            .expect("present_locks map mutex poisoned");
        map.entry(segment)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Try to acquire a lease for `[start, end]` on `segment`. Returns
    /// `Some(BatchLease)` if no in-flight lease overlaps; otherwise
    /// blocks until the conflicting lease releases and returns `None`
    /// (caller must recompute their batch and retry).
    fn claim_or_wait(&self, segment: Ulid, start: u32, end: u32) -> Option<BatchLease> {
        // Find an overlapping in-flight, or insert our own — under the
        // state lock so the decision is atomic.
        let conflict = {
            let mut s = self.state.lock().expect("coalescer state lock poisoned");
            if let Some(found) = s
                .iter()
                .find(|f| f.segment == segment && f.start <= end && start <= f.end)
            {
                Some(found.done.clone())
            } else {
                let done = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
                s.push(InFlight {
                    segment,
                    start,
                    end,
                    done: done.clone(),
                });
                return Some(BatchLease {
                    state: self.state.clone(),
                    segment,
                    start,
                    end,
                });
            }
        };
        // Wait outside the state lock — the leader needs the state lock
        // to remove its entry on completion.
        if let Some(handle) = conflict {
            let (m, cv) = &*handle;
            let mut g = m.lock().expect("lease done lock poisoned");
            while !*g {
                g = cv.wait(g).expect("lease done wait poisoned");
            }
        }
        None
    }
}

/// RAII handle held by the lease holder. On drop: remove the matching
/// entry from the coalescer state and notify any waiters.
struct BatchLease {
    state: Arc<Mutex<Vec<InFlight>>>,
    segment: Ulid,
    start: u32,
    end: u32,
}

impl Drop for BatchLease {
    fn drop(&mut self) {
        let mut s = match self.state.lock() {
            Ok(s) => s,
            // Don't panic in Drop — a poisoned mutex during drop would
            // double-panic. Best-effort: leak the entry; a stale entry
            // means future waiters see a phantom overlap and serialise
            // through an in-process barrier, which is suboptimal but
            // correct.
            Err(_) => return,
        };
        if let Some(pos) = s
            .iter()
            .position(|f| f.segment == self.segment && f.start == self.start && f.end == self.end)
        {
            let removed = s.remove(pos);
            // Drop state lock before signalling so woken waiters can
            // immediately reacquire it on the next claim attempt.
            drop(s);
            let (m, cv) = &*removed.done;
            if let Ok(mut g) = m.lock() {
                *g = true;
                cv.notify_all();
            }
        }
    }
}

// --- fetcher ---

/// Demand-fetcher implementing `SegmentFetcher` on top of a sync
/// [`RangeFetcher`]. Stateless w.r.t. the fork chain — every per-extent
/// call carries its own `owner_vol_id`, since the read path that
/// triggered the demand-fetch already iterated the local layout to
/// resolve the segment's owner.
pub struct RemoteFetcher {
    store: Arc<dyn RangeFetcher>,
    /// Maximum bytes per coalesced range-GET batch.
    max_batch_bytes: u64,
    /// Cross-thread in-flight coalescing for `fetch_extent`. Two callers
    /// whose batches overlap on the same segment share one fetch: the
    /// second caller waits, then re-reads `.present` and finds its bytes
    /// already cached. Disjoint batches on the same segment proceed in
    /// parallel. See [`FetchCoalescer`] for the algorithm.
    coalescer: FetchCoalescer,
}

impl RemoteFetcher {
    /// Build a fetcher from a [`FetchConfig`].
    pub fn new(config: &FetchConfig) -> io::Result<Self> {
        let store = config.build_fetcher()?;
        Ok(Self::from_store(
            store,
            config
                .fetch_batch_bytes
                .unwrap_or(DEFAULT_FETCH_BATCH_BYTES),
        ))
    }

    /// Build a fetcher from an already-constructed `RangeFetcher`.
    ///
    /// Used by callers (e.g. the coordinator) that manage the store directly
    /// rather than going through [`FetchConfig`].
    pub fn from_store(store: Arc<dyn RangeFetcher>, max_batch_bytes: u64) -> Self {
        Self {
            store,
            max_batch_bytes,
            coalescer: FetchCoalescer::new(),
        }
    }
}

impl SegmentFetcher for RemoteFetcher {
    fn fetch_extent(
        &self,
        segment_id: ulid::Ulid,
        owner_vol_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
        extent: &segment::ExtentFetch,
    ) -> io::Result<()> {
        fetch_one_extent(
            &*self.store,
            owner_vol_id,
            segment_id,
            index_dir,
            body_dir,
            &ExtentFetchParams {
                body_section_start: extent.body_section_start,
                entry_idx: extent.entry_idx,
                max_batch_bytes: self.max_batch_bytes,
            },
            &self.coalescer,
        )
    }

    fn fetch_delta_body(
        &self,
        segment_id: ulid::Ulid,
        owner_vol_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
    ) -> io::Result<()> {
        fetch_one_delta_body(
            &*self.store,
            owner_vol_id,
            &segment_id.to_string(),
            index_dir,
            body_dir,
        )
    }

    fn warm_segment(
        &self,
        segment_id: ulid::Ulid,
        owner_vol_id: ulid::Ulid,
        index_dir: &Path,
        body_dir: &Path,
        body_section_start: u64,
        populated_entries: &[u32],
    ) -> io::Result<()> {
        warm_segment_impl(
            &*self.store,
            owner_vol_id,
            &self.coalescer,
            self.max_batch_bytes,
            segment_id,
            index_dir,
            body_dir,
            body_section_start,
            populated_entries,
        )
    }
}

/// Parameters for fetching a single extent from storage.
struct ExtentFetchParams {
    body_section_start: u64,
    entry_idx: u32,
    max_batch_bytes: u64,
}

#[allow(clippy::too_many_arguments)]
fn fetch_one_extent(
    store: &dyn RangeFetcher,
    owner_vol_id: ulid::Ulid,
    segment_ulid: Ulid,
    index_dir: &Path,
    body_dir: &Path,
    extent: &ExtentFetchParams,
    coalescer: &FetchCoalescer,
) -> io::Result<()> {
    let segment_id = segment_ulid.to_string();
    let idx_path = index_dir.join(format!("{segment_id}.idx"));
    let present_path = body_dir.join(format!("{segment_id}.present"));

    // Read the full index so we can scan ahead for adjacent absent entries.
    // The .idx is invariant for the segment's lifetime, so we read it once
    // and reuse across the retry loop below.
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

    // Coalescing retry loop: re-read present bits, recompute the batch
    // against the freshest state, and try to claim a non-overlapping
    // lease. If an overlapping lease is in flight,
    // [`FetchCoalescer::claim_or_wait`] blocks until it releases and
    // returns `None`; we then loop and re-read so we see whatever the
    // leader populated. Termination: each iteration either claims a
    // lease, returns Ok (target is now present), or returns the
    // not-found-in-ancestors error. The loop can run at most one extra
    // iteration per concurrent overlapping caller, which is bounded by
    // the small number of in-flight readers.
    loop {
        // Read present bits at the top of every iteration so a
        // re-tried call observes whatever the previous leader wrote.
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

        // Fast exit: target entry was filled in by a prior leader's
        // batch while we were waiting (or even before we entered).
        if is_present(start) {
            return Ok(());
        }

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

        // Try to claim the lease for `[start, batch_last]`. On overlap
        // with an in-flight lease the call blocks then returns `None`;
        // we loop and re-derive the batch against fresh `.present`.
        let lease = match coalescer.claim_or_wait(segment_ulid, start as u32, batch_last as u32) {
            Some(lease) => lease,
            None => continue,
        };

        return fetch_batch(
            BatchContext {
                store,
                owner_vol_id,
                segment_ulid,
                segment_id: &segment_id,
                body_dir,
                present_path: &present_path,
                entries: &entries,
                extent,
                start,
                batch_last,
                next_expected_offset,
                coalescer,
            },
            lease,
        );
    }
}

/// Context for a single batch fetch held under a [`BatchLease`].
/// Bundles the read-only state that fell out of [`fetch_one_extent`]'s
/// retry loop (index entries, the present bitmap snapshot the loop
/// observed, and the batch range it computed) so the actual fetch
/// helper can stay a flat function without a long argument list.
struct BatchContext<'a> {
    store: &'a dyn RangeFetcher,
    /// Volume ULID that authored this segment. Resolved by the read
    /// path before demand-fetch is invoked (see
    /// `volume/read.rs::find_segment_in_dirs`); we GET against this
    /// one volume only, no chain walk.
    owner_vol_id: ulid::Ulid,
    /// Segment ULID — used to look up the per-segment `.present`
    /// write lock from `coalescer`.
    segment_ulid: Ulid,
    segment_id: &'a str,
    body_dir: &'a Path,
    present_path: &'a Path,
    entries: &'a [segment::SegmentEntry],
    extent: &'a ExtentFetchParams,
    /// Inclusive entry-index range of this batch.
    start: usize,
    batch_last: usize,
    /// Body-section offset of the byte just past the last entry —
    /// the exclusive end of the body slice we'll range-GET.
    next_expected_offset: u64,
    /// Source of the per-segment `.present` write lock. The merged
    /// write at the end of [`fetch_batch`] re-reads `.present` under
    /// this lock so disjoint leases on the same segment don't
    /// clobber each other's bits via a stale-snapshot RMW.
    coalescer: &'a FetchCoalescer,
}

fn fetch_batch(ctx: BatchContext<'_>, _lease: BatchLease) -> io::Result<()> {
    let BatchContext {
        store,
        owner_vol_id,
        segment_ulid,
        segment_id,
        body_dir,
        present_path,
        entries,
        extent,
        start,
        batch_last,
        next_expected_offset,
        coalescer,
    } = ctx;
    let batch_body_start = entries[start].stored_offset;
    let batch_body_end = next_expected_offset; // = entries[batch_last].stored_offset + len
    let range_start = extent.body_section_start + batch_body_start;
    let range_end = extent.body_section_start + batch_body_end;
    let batch_count = batch_last - start + 1;

    let owner_str = owner_vol_id.to_string();
    let key = segment_key(&owner_str, segment_id)?;
    let bytes = store.get_range(&key, range_start, range_end).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "fetching extent {segment_id}[{}] from {owner_str}: {e}",
                extent.entry_idx
            ),
        )
    })?;

    // Hash-verify every entry in the batch before touching disk. The
    // .idx is signed and authentic, so each entry's declared hash is
    // trusted; the bytes we just pulled from the store are not. A
    // mismatch here means either the remote object is tampered/corrupt
    // or the range slice is wrong — reject the batch.
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
    // If this call created the .body file, fsync the parent dir so the
    // new directory entry is durable before the present bit that
    // references it.
    if body_is_new {
        std::fs::File::open(body_dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io::Error::other(format!("fsync body dir: {e}")))?;
    }

    // Bulk-update .present for all entries in the batch.
    //
    // Disjoint coalescer leases on the same segment write disjoint
    // bits, but `.present` is one file per segment — a stale-snapshot
    // RMW would clobber the sibling leader's bits and leave the next
    // reader re-fetching already-cached body bytes. The per-segment
    // lock + re-read inside the lock is what makes the merge converge
    // across concurrent leaders.
    let present_lock = coalescer.present_lock_for(segment_ulid);
    let _present_guard = present_lock
        .lock()
        .expect("per-segment .present lock poisoned");
    let entry_count = entries.len() as u32;
    let bitset_len = (entry_count as usize).div_ceil(8);
    let mut new_present = match std::fs::read(present_path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    if new_present.len() < bitset_len {
        new_present.resize(bitset_len, 0);
    }
    for i in start..=batch_last {
        new_present[i / 8] |= 1 << (i % 8);
    }
    segment::write_file_atomic(present_path, &new_present)
        .map_err(|e| io::Error::other(format!("write .present: {e}")))?;

    tracing::debug!(
        segment_id,
        entry_idx = extent.entry_idx,
        batch_count,
        total_bytes = bytes.len(),
        "fetched extent batch"
    );
    Ok(())
}

/// Fetch a segment's delta body section and write it to `body_dir/<id>.delta`.
///
/// Reads layout from the local `.idx` (header carries `body_length` and
/// `delta_length`), issues one range-GET for exactly the delta region,
/// and writes the result atomically (tmp+rename). Targets the
/// segment's owning volume directly; the read path resolves
/// `owner_vol_id` from `<owner>/index/<seg>.idx` before invoking us.
fn fetch_one_delta_body(
    store: &dyn RangeFetcher,
    owner_vol_id: ulid::Ulid,
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

    let owner_str = owner_vol_id.to_string();
    let key = segment_key(&owner_str, segment_id)?;
    let bytes = store.get_range(&key, range_start, range_end).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("fetching delta body {segment_id} from {owner_str}: {e}"),
        )
    })?;
    if bytes.len() as u64 != range_end - range_start {
        return Err(io::Error::other(format!(
            "short range-GET for delta body {segment_id}: expected {} bytes, got {}",
            range_end - range_start,
            bytes.len(),
        )));
    }
    // Verify every delta option's blob against its signed `delta_hash`.
    // The delta body section is outside the segment signature, so the
    // hash in each option is the only authentication. A mismatch means
    // tamper or corruption in the remote object — refuse the batch.
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
    Ok(())
}

/// Maximum concurrent in-flight Range GETs per warming segment.
///
/// Each in-flight request holds up to `max_batch_bytes` of buffer. With
/// the default 256 KiB batch and 4 inner threads per segment, peak
/// per-segment buffer is ~1 MiB; with 16 outer prewarm workers running
/// independent segments, total prewarm memory is bounded at a few tens
/// of MiB even for a fully-fan-out claim.
const WARM_INTRA_SEGMENT_PARALLELISM: usize = 4;

/// One coalesced batch in a segment-wide warming pass: a contiguous run
/// of body-adjacent entry indices we'll cover with one Range GET.
struct WarmBatch {
    /// Inclusive entry-index range in `entries`. Used for hash-verify
    /// after the GET and for the `.present`-bit OR at end of segment.
    start_entry: usize,
    end_entry_inclusive: usize,
    /// Body-section-relative range covered by this batch.
    body_start: u64,
    body_end: u64,
}

/// Plan a segment-wide warming pass: walk `populated_entries` in order
/// and group body-adjacent, not-yet-present entries into batches up to
/// `max_batch_bytes`. The first entry in a batch is always included
/// regardless of size (matches `fetch_one_extent`'s same-entry-always-
/// first rule). Already-present entries are skipped.
fn plan_warm_batches(
    entries: &[segment::SegmentEntry],
    populated_entries: &[u32],
    is_present: &dyn Fn(usize) -> bool,
    max_batch_bytes: u64,
) -> Vec<WarmBatch> {
    let mut batches = Vec::new();
    let mut current: Option<WarmBatch> = None;

    for &idx in populated_entries {
        let i = idx as usize;
        let Some(entry) = entries.get(i) else {
            continue;
        };
        if !entry.kind.is_data() || entry.stored_length == 0 {
            continue;
        }
        if is_present(i) {
            // Already cached: close the current batch (its run is
            // body-adjacent on stored offsets, and a present-and-skipped
            // entry breaks the run), and continue past.
            if let Some(b) = current.take() {
                batches.push(b);
            }
            continue;
        }
        let entry_start = entry.stored_offset;
        let entry_end = entry_start + entry.stored_length as u64;

        match current.as_mut() {
            Some(b) if b.body_end == entry_start => {
                // Body-adjacent extension. Honour `max_batch_bytes`,
                // but always include the first entry of a batch.
                if entry_end - b.body_start > max_batch_bytes {
                    batches.push(current.take().expect("just matched Some"));
                    current = Some(WarmBatch {
                        start_entry: i,
                        end_entry_inclusive: i,
                        body_start: entry_start,
                        body_end: entry_end,
                    });
                } else {
                    b.end_entry_inclusive = i;
                    b.body_end = entry_end;
                }
            }
            _ => {
                // No current batch, or a body-layout gap broke the run.
                if let Some(b) = current.take() {
                    batches.push(b);
                }
                current = Some(WarmBatch {
                    start_entry: i,
                    end_entry_inclusive: i,
                    body_start: entry_start,
                    body_end: entry_end,
                });
            }
        }
    }
    if let Some(b) = current.take() {
        batches.push(b);
    }
    batches
}

/// Optimised whole-segment warming for `RemoteFetcher::warm_segment`.
///
/// Differs from the per-extent demand-fetch path in three ways:
///
/// 1. Multiple batches are issued concurrently (`std::thread::scope`,
///    capped by `WARM_INTRA_SEGMENT_PARALLELISM`); body bytes are
///    `pwrite`-ed at disjoint offsets so the writers don't contend.
/// 2. fsync of `.body` and `body_dir` runs **once** after every batch
///    has landed, instead of per batch — the bulk of the per-batch
///    latency in the demand path is the fsync chain, and warming
///    semantics don't need per-batch durability (a crash before the
///    final fsync just leaves the relevant `.present` bits clear, so
///    demand-fetch will re-fetch).
/// 3. The `.present` RMW happens once at end-of-segment under the
///    coalescer's per-segment lock, OR-ing every successfully-warmed
///    bit in one atomic write. Concurrent demand-fetch on the same
///    segment is unaffected: it still acquires the same lock, and the
///    re-read inside the lock means neither side clobbers the other's
///    bits.
///
/// Unlike demand-fetch's per-extent path, warming targets the segment's
/// **owning volume directly** — body-prefetch knows the owner from the
/// hint file's location, so there's no need to fan out a chain walk
/// that would issue N−1 NotFound store round-trips ahead of every
/// successful warm hit.
#[allow(clippy::too_many_arguments)]
fn warm_segment_impl(
    store: &dyn RangeFetcher,
    owner_vol_id: ulid::Ulid,
    coalescer: &FetchCoalescer,
    max_batch_bytes: u64,
    segment_ulid: Ulid,
    index_dir: &Path,
    body_dir: &Path,
    body_section_start: u64,
    populated_entries: &[u32],
) -> io::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let segment_id = segment_ulid.to_string();
    let idx_path = index_dir.join(format!("{segment_id}.idx"));
    let present_path = body_dir.join(format!("{segment_id}.present"));
    let body_path = body_dir.join(format!("{segment_id}.body"));

    let (_, entries, _) = segment::read_segment_index(&idx_path)?;

    let present_at_start = match std::fs::read(&present_path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    let is_present = |idx: usize| -> bool {
        present_at_start
            .get(idx / 8)
            .is_some_and(|b| b & (1 << (idx % 8)) != 0)
    };

    let batches = plan_warm_batches(&entries, populated_entries, &is_present, max_batch_bytes);
    if batches.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(body_dir)?;
    let body_was_new = !body_path.try_exists()?;
    let body_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&body_path)
        .map_err(|e| io::Error::other(format!("open .body for warm: {e}")))?;

    // Issue batches in parallel. Each thread picks the next batch from
    // a shared cursor, runs the GET + verify + pwrite, and records the
    // entry indices it warmed for the final `.present` OR.
    let cursor = AtomicUsize::new(0);
    let warmed: Mutex<Vec<u32>> = Mutex::new(Vec::with_capacity(populated_entries.len()));
    let first_err: Mutex<Option<io::Error>> = Mutex::new(None);

    std::thread::scope(|scope| {
        let workers = WARM_INTRA_SEGMENT_PARALLELISM.min(batches.len());
        for _ in 0..workers {
            let cursor = &cursor;
            let batches = &batches;
            let warmed = &warmed;
            let first_err = &first_err;
            let entries = &entries;
            let body_file = &body_file;
            let segment_id = segment_id.as_str();
            scope.spawn(move || {
                loop {
                    let i = cursor.fetch_add(1, Ordering::Relaxed);
                    if i >= batches.len() {
                        return;
                    }
                    if first_err
                        .lock()
                        .expect("first_err mutex poisoned")
                        .is_some()
                    {
                        return;
                    }
                    let batch = &batches[i];
                    match warm_one_batch(
                        store,
                        owner_vol_id,
                        segment_id,
                        entries,
                        body_file,
                        body_section_start,
                        batch,
                    ) {
                        Ok(()) => {
                            let mut g = warmed.lock().expect("warmed mutex poisoned");
                            for idx in batch.start_entry..=batch.end_entry_inclusive {
                                g.push(idx as u32);
                            }
                        }
                        Err(e) => {
                            let mut g = first_err.lock().expect("first_err mutex poisoned");
                            if g.is_none() {
                                *g = Some(e);
                            }
                        }
                    }
                }
            });
        }
    });

    if let Some(e) = first_err.into_inner().expect("first_err mutex poisoned") {
        return Err(e);
    }
    let mut warmed = warmed.into_inner().expect("warmed mutex poisoned");
    if warmed.is_empty() {
        return Ok(());
    }

    // Single fsync of .body covers every parallel pwrite above.
    body_file
        .sync_data()
        .map_err(|e| io::Error::other(format!("fsync .body for warm: {e}")))?;
    drop(body_file);
    if body_was_new {
        std::fs::File::open(body_dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io::Error::other(format!("fsync body dir for warm: {e}")))?;
    }

    // Single .present update under the per-segment write lock. Re-read
    // inside the lock so a concurrent demand-fetch's bits are merged
    // rather than clobbered (same convergence rule as `fetch_batch`).
    let lock = coalescer.present_lock_for(segment_ulid);
    let _present_guard = lock.lock().expect("per-segment .present lock poisoned");
    let entry_count = entries.len();
    let bitset_len = entry_count.div_ceil(8);
    let mut new_present = match std::fs::read(&present_path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    if new_present.len() < bitset_len {
        new_present.resize(bitset_len, 0);
    }
    warmed.sort_unstable();
    warmed.dedup();
    for idx in &warmed {
        let i = *idx as usize;
        new_present[i / 8] |= 1 << (i % 8);
    }
    segment::write_file_atomic(&present_path, &new_present)
        .map_err(|e| io::Error::other(format!("write .present for warm: {e}")))?;

    tracing::debug!(
        segment_id,
        batches = batches.len(),
        entries_warmed = warmed.len(),
        "warm_segment complete"
    );
    Ok(())
}

/// Fetch one warming batch directly from the segment's owning volume
/// (no chain walk), verify each entry against its hash from the local
/// `.idx`, and `pwrite` the bytes into `body_file` at body-relative
/// offset `batch.body_start`. No fsync — the caller does one
/// segment-wide fsync after every batch lands.
#[allow(clippy::too_many_arguments)]
fn warm_one_batch(
    store: &dyn RangeFetcher,
    owner_vol_id: ulid::Ulid,
    segment_id: &str,
    entries: &[segment::SegmentEntry],
    body_file: &std::fs::File,
    body_section_start: u64,
    batch: &WarmBatch,
) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    let range_start = body_section_start + batch.body_start;
    let range_end = body_section_start + batch.body_end;
    let owner_str = owner_vol_id.to_string();
    let key = segment_key(&owner_str, segment_id)?;
    let bytes = store.get_range(&key, range_start, range_end).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "warm fetch {segment_id} batch [{}..={}] from {owner_str}: {e}",
                batch.start_entry, batch.end_entry_inclusive
            ),
        )
    })?;
    if bytes.len() as u64 != range_end - range_start {
        return Err(io::Error::other(format!(
            "short range-GET for warm {segment_id}: expected {} bytes, got {}",
            range_end - range_start,
            bytes.len(),
        )));
    }
    for (i, e) in entries
        .iter()
        .enumerate()
        .take(batch.end_entry_inclusive + 1)
        .skip(batch.start_entry)
    {
        let rel = (e.stored_offset - batch.body_start) as usize;
        let len = e.stored_length as usize;
        let slice = bytes.get(rel..rel + len).ok_or_else(|| {
            io::Error::other(format!(
                "warm entry {i} out of batch bounds for {segment_id}"
            ))
        })?;
        segment::verify_body_hash(e, slice)
            .map_err(|err| io::Error::new(err.kind(), format!("warm {segment_id}[{i}]: {err}")))?;
    }
    // .body is body-section-relative: offset 0 in the file corresponds
    // to body_section_start in the remote object.
    body_file
        .write_all_at(&bytes, batch.body_start)
        .map_err(|e| io::Error::other(format!("pwrite .body for warm: {e}")))?;
    Ok(())
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
        let owner_vol_id = ulid::Ulid::from_string(vol_id).unwrap();

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
        let fetcher = RemoteFetcher::new(&cfg).unwrap();

        // Fetch entry 0 — should coalesce entries 0, 1, 2 into one range-GET.
        fetcher
            .fetch_extent(
                seg_ulid,
                owner_vol_id,
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
        let owner_vol_id = ulid::Ulid::from_string(vol_id).unwrap();

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
        let fetcher = RemoteFetcher::new(&cfg).unwrap();

        fetcher
            .fetch_extent(
                seg_ulid,
                owner_vol_id,
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
        let owner_vol_id = ulid::Ulid::from_string(vol_id).unwrap();

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
        let fetcher = RemoteFetcher::new(&cfg).unwrap();

        let err = fetcher
            .fetch_extent(
                seg_ulid,
                owner_vol_id,
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
        let owner_vol_id = ulid::Ulid::from_string(vol_id).unwrap();

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
        let fetcher = RemoteFetcher::new(&cfg).unwrap();

        let err = fetcher
            .fetch_delta_body(seg_ulid, owner_vol_id, &index_dir, &cache_dir)
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

    // --- coalescer unit tests ---

    /// Basic happy path: a fresh coalescer hands out a lease and the
    /// lease drops cleanly with no waiters.
    #[test]
    fn coalescer_grants_lease_when_no_conflict() {
        let c = FetchCoalescer::new();
        let seg = ulid::Ulid::new();
        let lease = c.claim_or_wait(seg, 0, 5);
        assert!(lease.is_some(), "first claim should succeed");
        drop(lease);
        // After release, a follow-up claim on the same range succeeds again.
        assert!(c.claim_or_wait(seg, 0, 5).is_some());
    }

    /// Disjoint ranges on the same segment must NOT block each other.
    /// The whole point of using `[start, end]` overlap rather than
    /// per-segment locking is that two readers fetching far apart
    /// within the same segment proceed in parallel.
    #[test]
    fn coalescer_disjoint_ranges_do_not_conflict() {
        let c = FetchCoalescer::new();
        let seg = ulid::Ulid::new();
        let lease_a = c.claim_or_wait(seg, 0, 4);
        assert!(lease_a.is_some(), "first claim succeeds");
        // Range [10, 15] doesn't overlap [0, 4] → second claim succeeds
        // without blocking. If overlap detection were segment-level,
        // this would block waiting for the first lease to release.
        let lease_b = c.claim_or_wait(seg, 10, 15);
        assert!(
            lease_b.is_some(),
            "disjoint range on same segment must not conflict",
        );
    }

    /// Ranges on different segments are always disjoint by definition.
    #[test]
    fn coalescer_different_segments_do_not_conflict() {
        let c = FetchCoalescer::new();
        let seg_a = ulid::Ulid::new();
        let seg_b = ulid::Ulid::new();
        let _la = c.claim_or_wait(seg_a, 0, 100).unwrap();
        let lb = c.claim_or_wait(seg_b, 0, 100);
        assert!(
            lb.is_some(),
            "different segment, same range, must not block"
        );
    }

    /// Overlap-and-block: a second claimer whose range overlaps a live
    /// lease must block until the lease drops, then return `None` so
    /// the caller knows to recompute its batch and retry. Drives the
    /// blocking semantics through real threads + a brief sleep so the
    /// wakeup path is exercised.
    #[test]
    fn coalescer_overlapping_range_blocks_until_release() {
        use std::sync::Arc as StdArc;
        use std::thread;
        use std::time::{Duration, Instant};

        let c = StdArc::new(FetchCoalescer::new());
        let seg = ulid::Ulid::new();

        // Hold a lease covering [0, 10] on the main thread.
        let lease = c.claim_or_wait(seg, 0, 10).expect("primary claim");

        // Spawn a thread that tries to claim [5, 7] (overlapping). It
        // should block until we drop the primary lease below.
        let waiter_started = Arc::new(std::sync::Mutex::new(None::<Instant>));
        let waiter_returned = Arc::new(std::sync::Mutex::new(None::<Instant>));
        let c_w = c.clone();
        let started_w = waiter_started.clone();
        let returned_w = waiter_returned.clone();
        let waiter = thread::spawn(move || {
            *started_w.lock().unwrap() = Some(Instant::now());
            let result = c_w.claim_or_wait(seg, 5, 7);
            *returned_w.lock().unwrap() = Some(Instant::now());
            // Returns None — we're a waiter, not a leaseholder.
            assert!(result.is_none());
        });

        // Give the waiter time to enter the wait.
        thread::sleep(Duration::from_millis(50));
        assert!(
            waiter_returned.lock().unwrap().is_none(),
            "waiter must still be blocked"
        );

        // Drop the primary lease — this notifies the waiter.
        let drop_at = Instant::now();
        drop(lease);
        waiter.join().unwrap();

        let returned = waiter_returned.lock().unwrap().expect("waiter returned");
        assert!(
            returned >= drop_at,
            "waiter must have unblocked after the primary lease was dropped",
        );
    }

    /// Cross-thread concurrent demand-fetches on the same segment
    /// must all succeed AND fire only one underlying range-GET per
    /// distinct batch. Models the ublk-fanout race directly: spawn
    /// N threads all targeting entry 0, count how many times the
    /// inner range-fetcher is invoked.
    #[test]
    fn fetch_extent_coalesces_concurrent_demand_fetches_for_same_segment() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
        use std::sync::Mutex as StdMutex;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;
        use std::time::Duration;
        use tempfile::TempDir;

        // A wrapping range-fetcher that counts calls and sleeps long
        // enough that overlapping callers definitely arrive before
        // the leader returns.
        struct CountingFetcher {
            inner: LocalRangeFetcher,
            calls: AtomicUsize,
            delay: Duration,
        }
        impl RangeFetcher for CountingFetcher {
            fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(self.delay);
                self.inner.get_range(key, start, end_exclusive)
            }
        }

        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let owner_vol_id = ulid::Ulid::from_string(vol_id).unwrap();

        // Three contiguous entries — same shape as the existing
        // coalescing test. The whole batch is what the leader will
        // fetch; followers' targets all fall inside it.
        let data: Vec<Vec<u8>> = (0..3).map(|i| vec![0x10 + i as u8; 4096]).collect();
        let hashes: Vec<_> = data.iter().map(|d| blake3::hash(d)).collect();
        let mut entries: Vec<SegmentEntry> = data
            .iter()
            .zip(hashes.iter())
            .enumerate()
            .map(|(i, (d, h))| {
                SegmentEntry::new_data(*h, i as u64, 1, SegmentFlags::empty(), d.clone())
            })
            .collect();
        let seg_path = tmp.path().join(&seg_id);
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let full_bytes = std::fs::read(&seg_path).unwrap();

        // .idx in place; .body / .present absent so every thread
        // demand-fetches.
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        let key = segment_key(vol_id, &seg_id).unwrap();
        put_local(store_dir.path(), &key, &full_bytes);

        // Fork dir holding volume.pub.
        let vol_dir = tmp.path().join(vol_id);
        std::fs::create_dir_all(&vol_dir).unwrap();
        let pub_hex = vk
            .to_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            + "\n";
        std::fs::write(vol_dir.join("volume.pub"), pub_hex.as_bytes()).unwrap();

        let counting = Arc::new(CountingFetcher {
            inner: LocalRangeFetcher::new(store_dir.path().to_path_buf()),
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(50),
        });
        let fetcher = Arc::new(RemoteFetcher::from_store(
            counting.clone(),
            DEFAULT_FETCH_BATCH_BYTES,
        ));

        // Spawn N threads, all racing on entry 0 of the same segment.
        // Without coalescing, every thread fires its own range-GET; with
        // coalescing, only the first thread fetches and the others wait
        // then find their bytes already present.
        // Capture entry-0's offset/length as Copy primitives so the
        // closures don't need to share `entries` itself.
        let entry0_offset = entries[0].stored_offset;
        let entry0_length = entries[0].stored_length;
        let n_threads = 8;
        let errors: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let fetcher = fetcher.clone();
                let index_dir = index_dir.clone();
                let cache_dir = cache_dir.clone();
                let errs = errors.clone();
                thread::spawn(move || {
                    let result = fetcher.fetch_extent(
                        seg_ulid,
                        owner_vol_id,
                        &index_dir,
                        &cache_dir,
                        &segment::ExtentFetch {
                            body_section_start: bss,
                            body_offset: entry0_offset,
                            body_length: entry0_length,
                            entry_idx: 0,
                        },
                    );
                    if let Err(e) = result {
                        errs.lock().unwrap().push(format!("{e:#}"));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let errs = errors.lock().unwrap();
        assert!(errs.is_empty(), "all fetches must succeed: {errs:?}");

        // Exactly one range-GET fired even though N threads all called
        // fetch_extent. The leader's batch covered every follower's
        // target entry; followers re-read present_bytes after waiting,
        // saw their entry was set, and fast-exited.
        let calls = counting.calls.load(Ordering::SeqCst);
        assert_eq!(
            calls, 1,
            "concurrent demand-fetches on the same segment must coalesce to one range-GET"
        );

        // All present bits set, body file populated.
        let body_bytes = std::fs::read(cache_dir.join(format!("{seg_id}.body"))).unwrap();
        for (i, d) in data.iter().enumerate() {
            let off = entries[i].stored_offset as usize;
            assert_eq!(&body_bytes[off..off + d.len()], d.as_slice());
        }
    }

    /// Two `fetch_extent` calls with **disjoint** entry ranges run
    /// concurrently. Each leader takes its own coalescer lease (the
    /// ranges don't overlap), reads the on-disk `.present` bitmap,
    /// fetches its body slice, then writes the merged `.present` back.
    ///
    /// The bug: each leader uses the snapshot it read *before* the
    /// fetch as the merge base — so a sibling leader's writes that
    /// landed in between are silently overwritten by the next
    /// `write_file_atomic`. Disjoint lease ranges write disjoint bits,
    /// but the on-disk `.present` is one file per segment; the RMW
    /// from a stale snapshot loses the other writer's bits.
    ///
    /// This test forces the race with a barrier inside the inner
    /// range-fetcher, so all leaders are past the `.present` read and
    /// inside their range-GET at the same instant. With the bug, at
    /// least one leader's bits are missing after both threads return.
    /// With the fix (re-read `.present` immediately before
    /// `write_file_atomic` under a per-segment write lock), every bit
    /// the run set is durable.
    #[test]
    fn fetch_extent_disjoint_concurrent_batches_preserve_present_bits() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
        use std::sync::Barrier;
        use std::sync::Mutex as StdMutex;
        use std::thread;
        use std::time::Duration;
        use tempfile::TempDir;

        // Range-fetcher that pauses every caller at a barrier *inside*
        // get_range, then sleeps a thread-specific extra delay before
        // returning. The barrier synchronises N readers past the
        // top-of-loop `.present` read; the staggered post-barrier sleep
        // makes their writes interleave at the on-disk-rename layer.
        struct BarrierFetcher {
            inner: LocalRangeFetcher,
            barrier: Arc<Barrier>,
            counter: std::sync::atomic::AtomicU32,
        }
        impl RangeFetcher for BarrierFetcher {
            fn get_range(&self, key: &str, start: u64, end_exclusive: u64) -> io::Result<Vec<u8>> {
                self.barrier.wait();
                let n = self
                    .counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis((n as u64) * 5));
                self.inner.get_range(key, start, end_exclusive)
            }
        }

        const N: usize = 4;

        // Build a segment with N Data entries, each 4 KiB. With
        // `max_batch_bytes = 4096` every fetch_extent call is forced
        // to a single-entry batch, so calls on different starts get
        // **disjoint** coalescer leases and proceed in parallel.
        let tmp = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();
        let index_dir = tmp.path().join("index");
        let cache_dir = tmp.path().join("cache");

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let owner_vol_id = ulid::Ulid::from_string(vol_id).unwrap();

        let data: Vec<Vec<u8>> = (0..N).map(|i| vec![0xA0u8 + i as u8; 4096]).collect();
        let hashes: Vec<_> = data.iter().map(|d| blake3::hash(d)).collect();
        let mut entries: Vec<SegmentEntry> = data
            .iter()
            .zip(hashes.iter())
            .enumerate()
            .map(|(i, (d, h))| {
                SegmentEntry::new_data(*h, i as u64, 1, SegmentFlags::empty(), d.clone())
            })
            .collect();
        let seg_path = tmp.path().join(&seg_id);
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let full_bytes = std::fs::read(&seg_path).unwrap();

        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();
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

        // Cap the batch at one entry so disjoint starts → disjoint
        // leases. With the default 256 KiB cap and 4 KiB entries the
        // first call would batch all four, masking the bug.
        let small_batch = entries[0].stored_length as u64;
        let inner = LocalRangeFetcher::new(store_dir.path().to_path_buf());
        let barrier_fetcher = Arc::new(BarrierFetcher {
            inner,
            barrier: Arc::new(Barrier::new(N)),
            counter: std::sync::atomic::AtomicU32::new(0),
        });
        let fetcher = Arc::new(RemoteFetcher::from_store(
            barrier_fetcher.clone(),
            small_batch,
        ));

        let stored_offsets: Vec<u64> = entries.iter().map(|e| e.stored_offset).collect();
        let stored_lengths: Vec<u32> = entries.iter().map(|e| e.stored_length).collect();
        let errors: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));

        let handles: Vec<_> = (0..N)
            .map(|i| {
                let fetcher = fetcher.clone();
                let index_dir = index_dir.clone();
                let cache_dir = cache_dir.clone();
                let body_offset = stored_offsets[i];
                let body_length = stored_lengths[i];
                let errs = errors.clone();
                thread::spawn(move || {
                    let result = fetcher.fetch_extent(
                        seg_ulid,
                        owner_vol_id,
                        &index_dir,
                        &cache_dir,
                        &segment::ExtentFetch {
                            body_section_start: bss,
                            body_offset,
                            body_length,
                            entry_idx: i as u32,
                        },
                    );
                    if let Err(e) = result {
                        errs.lock().unwrap().push(format!("entry {i}: {e:#}"));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let errs = errors.lock().unwrap();
        assert!(errs.is_empty(), "all fetches must succeed: {errs:?}");

        // Every entry's bit must be set in `.present` after the race.
        let present_path = cache_dir.join(format!("{seg_id}.present"));
        let present = std::fs::read(&present_path).expect("present file written");
        for i in 0..N {
            let byte = present.get(i / 8).copied().unwrap_or(0);
            assert!(
                byte & (1 << (i % 8)) != 0,
                "bit {i} unset after disjoint concurrent fetch — RMW race lost it. \
                 present bytes: {present:?}",
            );
        }
    }

    // --- warm_segment tests ---

    /// Helper: build a segment with `n` body-adjacent uncompressed
    /// entries, write its `.idx` to `index_dir`, upload the full file to
    /// `store_root` keyed under `vol_id`, and return everything the
    /// `warm_segment` tests need: the segment ULID, body-section start,
    /// the entries' raw payloads, and the constructed `RemoteFetcher`.
    #[allow(clippy::type_complexity)]
    fn build_warm_fixture(
        tmp: &Path,
        store_root: &Path,
        n: usize,
        entry_size: usize,
    ) -> (
        RemoteFetcher,
        ulid::Ulid,
        ulid::Ulid,
        u64,
        Vec<Vec<u8>>,
        PathBuf,
        PathBuf,
    ) {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};

        let index_dir = tmp.join("index");
        let cache_dir = tmp.join("cache");
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::create_dir_all(&cache_dir).unwrap();

        let seg_ulid = ulid::Ulid::new();
        let seg_id = seg_ulid.to_string();
        let vol_id_str = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let owner_vol_id = ulid::Ulid::from_string(vol_id_str).unwrap();

        let payloads: Vec<Vec<u8>> = (0..n)
            .map(|i| vec![(i as u8).wrapping_add(0x10); entry_size])
            .collect();
        let mut entries: Vec<SegmentEntry> = payloads
            .iter()
            .enumerate()
            .map(|(i, data)| {
                SegmentEntry::new_data(
                    blake3::hash(data),
                    i as u64,
                    1,
                    SegmentFlags::empty(),
                    data.clone(),
                )
            })
            .collect();
        let seg_path = tmp.join(&seg_id);
        let (signer, vk) = elide_core::signing::generate_ephemeral_signer();
        let bss = write_segment(&seg_path, &mut entries, signer.as_ref()).unwrap();
        let full_bytes = std::fs::read(&seg_path).unwrap();

        std::fs::write(
            index_dir.join(format!("{seg_id}.idx")),
            &full_bytes[..bss as usize],
        )
        .unwrap();

        let key = segment_key(vol_id_str, &seg_id).unwrap();
        put_local(store_root, &key, &full_bytes);

        let vol_dir = tmp.join(vol_id_str);
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
            local_path: Some(store_root.to_string_lossy().into_owned()),
            fetch_batch_bytes: None,
        };
        let fetcher = RemoteFetcher::new(&cfg).unwrap();
        (
            fetcher,
            seg_ulid,
            owner_vol_id,
            bss,
            payloads,
            index_dir,
            cache_dir,
        )
    }

    /// `warm_segment` warms every populated entry: bytes land in
    /// `.body` and the matching `.present` bit is set.
    #[test]
    fn warm_segment_warms_populated_entries() {
        use elide_core::segment::check_present_bit;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store = TempDir::new().unwrap();
        let (fetcher, seg_ulid, owner_vol_id, bss, payloads, index_dir, cache_dir) =
            build_warm_fixture(tmp.path(), store.path(), 4, 4096);

        fetcher
            .warm_segment(
                seg_ulid,
                owner_vol_id,
                &index_dir,
                &cache_dir,
                bss,
                &[0, 1, 2, 3],
            )
            .unwrap();

        let body = std::fs::read(cache_dir.join(format!("{seg_ulid}.body"))).unwrap();
        for (i, want) in payloads.iter().enumerate() {
            let off = i * 4096;
            assert_eq!(
                &body[off..off + 4096],
                want.as_slice(),
                "entry {i} bytes mismatch"
            );
        }
        let present_path = cache_dir.join(format!("{seg_ulid}.present"));
        for i in 0..4 {
            assert!(
                check_present_bit(&present_path, i as u32).unwrap(),
                "bit {i} not set"
            );
        }
    }

    /// `warm_segment` does not re-fetch entries already marked present;
    /// it only fills in the missing ones and merges with the existing
    /// `.present` bitmap.
    #[test]
    fn warm_segment_skips_already_present_entries() {
        use elide_core::segment::check_present_bit;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store = TempDir::new().unwrap();
        let (fetcher, seg_ulid, owner_vol_id, bss, _payloads, index_dir, cache_dir) =
            build_warm_fixture(tmp.path(), store.path(), 4, 4096);

        // Pre-seed: entry 1 is already "present" (bit set, body file
        // empty). warm_segment must leave bit 1 alone, fill bits 0, 2, 3.
        let present_path = cache_dir.join(format!("{seg_ulid}.present"));
        std::fs::write(&present_path, [0b0000_0010u8]).unwrap();

        fetcher
            .warm_segment(
                seg_ulid,
                owner_vol_id,
                &index_dir,
                &cache_dir,
                bss,
                &[0, 1, 2, 3],
            )
            .unwrap();

        for i in 0..4 {
            assert!(
                check_present_bit(&present_path, i as u32).unwrap(),
                "bit {i} not set after warm"
            );
        }
        // The pre-seeded bit (1) was preserved through the merge.
        let final_present = std::fs::read(&present_path).unwrap();
        assert_eq!(final_present[0] & 0b0000_1111, 0b0000_1111);
    }

    /// `warm_segment` is a no-op when `populated_entries` is empty —
    /// no `.body` file gets created and no `.present` write happens.
    #[test]
    fn warm_segment_empty_populated_is_noop() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store = TempDir::new().unwrap();
        let (fetcher, seg_ulid, owner_vol_id, bss, _payloads, index_dir, cache_dir) =
            build_warm_fixture(tmp.path(), store.path(), 4, 4096);

        fetcher
            .warm_segment(seg_ulid, owner_vol_id, &index_dir, &cache_dir, bss, &[])
            .unwrap();

        assert!(!cache_dir.join(format!("{seg_ulid}.body")).exists());
        assert!(!cache_dir.join(format!("{seg_ulid}.present")).exists());
    }

    /// Many adjacent entries → multiple batches under a small
    /// `max_batch_bytes`. All entries must still warm correctly with
    /// the parallel batch path.
    #[test]
    fn warm_segment_splits_into_multiple_batches() {
        use elide_core::segment::check_present_bit;
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let store = TempDir::new().unwrap();
        // 12 entries × 4 KiB = 48 KiB total; with a 16 KiB batch cap we
        // expect ~3 batches, exercising the parallel scope path.
        let n = 12;
        let entry_size = 4096;
        let (_default_fetcher, seg_ulid, owner_vol_id, bss, payloads, index_dir, cache_dir) =
            build_warm_fixture(tmp.path(), store.path(), n, entry_size);

        // Rebuild the fetcher with a small batch cap; the helper above
        // uses the default. Same store, just different config —
        // RemoteFetcher::new no longer takes fork_dirs after the
        // owner-vol-id threading.
        let cfg = FetchConfig {
            bucket: None,
            endpoint: None,
            region: None,
            local_path: Some(store.path().to_string_lossy().into_owned()),
            fetch_batch_bytes: Some(16 * 1024),
        };
        let fetcher = RemoteFetcher::new(&cfg).unwrap();

        let populated: Vec<u32> = (0..n as u32).collect();
        fetcher
            .warm_segment(
                seg_ulid,
                owner_vol_id,
                &index_dir,
                &cache_dir,
                bss,
                &populated,
            )
            .unwrap();

        let body = std::fs::read(cache_dir.join(format!("{seg_ulid}.body"))).unwrap();
        for (i, want) in payloads.iter().enumerate() {
            let off = i * entry_size;
            assert_eq!(
                &body[off..off + entry_size],
                want.as_slice(),
                "entry {i} bytes mismatch after multi-batch warm"
            );
        }
        let present_path = cache_dir.join(format!("{seg_ulid}.present"));
        for i in 0..n {
            assert!(
                check_present_bit(&present_path, i as u32).unwrap(),
                "bit {i} not set after multi-batch warm"
            );
        }
    }
}
