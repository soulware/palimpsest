// Segment upload: drain all committed segments from pending/ to the object store.
//
// Object key format: by_id/<volume_ulid>/segments/YYYYMMDD/<ulid>
//
// The date is extracted from the ULID timestamp (creation time, not upload time),
// so keys are stable and deterministic regardless of when drain-pending runs.
//
// Each segment is handled independently. A failure on one segment does not
// prevent the remaining segments from uploading.
//
// Upload commit sequence per segment:
//   1. Read pending/<ulid> into memory
//   2. PUT to object store at the derived key
//   3. IPC → volume: "promote <ulid>"
//      Volume copies pending/<ulid> → cache/<ulid>.body, writes cache/<ulid>.present,
//      and deletes pending/<ulid>.
//   4. On failure at any step: leave pending/<ulid> in place, record error, continue.
//      If volume is not running: leave pending/ in place, retry next tick.
//
// Crash safety:
//   - Crash before step 3: pending/<ulid> still exists; drain re-uploads (idempotent
//     S3 PUT) and re-sends promote on next tick.
//   - Crash after step 3: pending/<ulid> is gone (volume deleted it); done.
//
// Ordering invariant: index/<ulid>.idx present ↔ segment confirmed in S3.
// The volume writes index/<ulid>.idx inside the promote_segment IPC handler,
// which the coordinator calls only after a confirmed S3 PUT. This means every
// .idx file the coordinator sees is safe to fetch from the object store.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::path::Path as StorePath;
use object_store::{
    Attribute, AttributeValue, Attributes, ObjectStore, PutOptions, WriteMultipart,
};
use serde::Serialize;
use ulid::Ulid;

/// `Content-Type` for plain UTF-8 text files (volume.pub, provenance, filemap, etc.).
const MIME_TEXT: &str = "text/plain; charset=utf-8";
/// `Content-Type` for TOML manifests.
const MIME_TOML: &str = "application/toml; charset=utf-8";

/// Default multipart part size for tests and non-configurable callers.
/// Operational code paths read the value from `StoreSection::multipart_part_size_bytes()`.
pub const DEFAULT_PART_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Build `PutOptions` that set `Content-Type` on the uploaded object.
fn put_opts_with_type(content_type: &'static str) -> PutOptions {
    let mut attrs = Attributes::new();
    attrs.insert(Attribute::ContentType, AttributeValue::from(content_type));
    attrs.into()
}

/// PUT a payload with `Content-Type` set. If the backing store returns
/// `NotImplemented` for attribute options (as `LocalFileSystem` does —
/// tests hit this path), retry without attributes. Production S3 always
/// uses the typed path.
async fn put_with_content_type(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    payload: Bytes,
    content_type: &'static str,
) -> std::result::Result<(), object_store::Error> {
    match store
        .put_opts(
            key,
            payload.clone().into(),
            put_opts_with_type(content_type),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotImplemented) => {
            store.put(key, payload.into()).await.map(|_| ())
        }
        Err(e) => Err(e),
    }
}

/// Directory under each volume that holds upload-completion records — one
/// file per S3 object we've confirmed uploaded. For small metadata the file
/// holds a verbatim copy of the uploaded bytes, so `diff uploaded/<f>
/// <source>` works with standard tools and re-upload decisions are taken by
/// exact content comparison rather than mtime. The snapshot triple uses a
/// plain empty sentinel since the S3 marker is empty and filemap/.manifest
/// are already inspectable under `snapshots/`.
const UPLOADED_DIR: &str = "uploaded";

fn upload_sentinel(vol_dir: &Path, relative: &str) -> PathBuf {
    vol_dir.join(UPLOADED_DIR).join(relative)
}

/// Return true iff `sentinel` exists and its bytes equal `expected`. A
/// partial-write after a crash fails the equality check and triggers a
/// self-healing re-upload on the next tick.
fn is_already_uploaded(sentinel: &Path, expected: &[u8]) -> bool {
    std::fs::read(sentinel)
        .map(|b| b == expected)
        .unwrap_or(false)
}

fn mark_uploaded(sentinel: &Path, content: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = sentinel.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(sentinel, content)?;
    Ok(())
}

pub struct DrainResult {
    pub uploaded: usize,
    pub failed: usize,
}

/// Return the volume ULID from a volume directory path.
///
/// In the flat layout every volume lives at `<data_dir>/by_id/<ulid>/`.
/// The directory name is validated as a ULID.
pub fn derive_names(vol_dir: &Path) -> Result<String> {
    let name = vol_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("vol dir has no name: {}", vol_dir.display()))?;
    ulid::Ulid::from_string(name)
        .map(|_| name.to_owned())
        .map_err(|e| anyhow::anyhow!("vol dir name is not a valid ULID '{name}': {e}"))
}

/// Build the object store key for a segment.
///
/// Format: `by_id/<volume_ulid>/segments/YYYYMMDD/<segment_ulid>`
pub fn segment_key(volume_id: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "by_id/{volume_id}/segments/{date}/{ulid_str}"
    )))
}

/// Build the object store key for a snapshot marker.
///
/// Format: `by_id/<volume_ulid>/snapshots/YYYYMMDD/<snapshot_ulid>`
pub fn snapshot_key(volume_id: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "by_id/{volume_id}/snapshots/{date}/{ulid_str}"
    )))
}

/// Build the object store key for a snapshot filemap.
///
/// Format: `by_id/<volume_ulid>/snapshots/YYYYMMDD/<snapshot_ulid>.filemap`
pub fn filemap_key(volume_id: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "by_id/{volume_id}/snapshots/{date}/{ulid_str}.filemap"
    )))
}

/// Build the object store key for a signed snapshot manifest.
///
/// Format: `by_id/<volume_ulid>/snapshots/YYYYMMDD/<snapshot_ulid>.manifest`
pub fn snapshot_manifest_key(volume_id: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "by_id/{volume_id}/snapshots/{date}/{ulid_str}.manifest"
    )))
}

/// Volume manifest written to `by_id/<ulid>/manifest.toml` in the store.
///
/// Contains everything a new host needs to reconstruct the local directory
/// skeleton before prefetching segment indexes.
#[derive(Serialize)]
struct Manifest<'a> {
    name: &'a str,
    size: u64,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    readonly: bool,
    /// Present on forks only. Format: `<parent-ulid>/snapshots/<snapshot-ulid>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    origin: Option<&'a str>,
    /// Present on OCI-imported volumes only.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<ManifestSource<'a>>,
}

#[derive(Serialize)]
struct ManifestSource<'a> {
    image: &'a str,
    digest: &'a str,
    arch: &'a str,
}

/// Upload all committed segments from `pending/` to the object store, then
/// promote each segment to the local cache.
///
/// For writable volumes the promote is done via IPC to the running volume
/// process (`promote <ulid>`), which copies the body to `cache/` and deletes
/// `pending/<ulid>`.  If the volume is not running the segment stays in
/// `pending/` and the coordinator retries on the next tick.
///
/// For readonly volumes no volume process ever runs, so the coordinator
/// performs the promote directly: it writes `index/<ulid>.idx` and
/// `cache/<ulid>.{body,present}` then deletes `pending/<ulid>`.  This is safe
/// because readonly volumes have no concurrent readers or writers — the only
/// `drain_pending` is a one-shot batch command. Metadata (pub key, manifest,
/// name entry) is re-uploaded on every invocation — all are idempotent and tiny.
pub async fn drain_pending(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
) -> Result<DrainResult> {
    let pending_dir = vol_dir.join("pending");

    // Upload volume metadata before segments so that any host that
    // demand-fetches a segment can immediately verify it and bootstrap the vol.
    upload_volume_metadata(vol_dir, volume_id, store).await;

    let entries = std::fs::read_dir(&pending_dir)
        .with_context(|| format!("opening pending dir: {}", pending_dir.display()))?;

    let mut uploaded = 0usize;
    let mut failed = 0usize;

    for entry in entries {
        let entry = entry.context("reading pending dir entry")?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name.ends_with(".tmp") {
            continue;
        }
        let ulid = match ulid::Ulid::from_string(name) {
            Ok(u) => u,
            Err(_) => continue,
        };
        let segment_path = entry.path();

        // Redact dead DATA regions in place before upload. Best-effort: if
        // the volume is not running, proceed anyway — segments with no
        // hash-dead entries are a no-op, and the thin DedupRef format means
        // DedupRef bodies are never in the file to begin with.
        crate::control::redact_segment(vol_dir, ulid).await;

        match upload_segment_file(&segment_path, name, volume_id, store, part_size_bytes).await {
            Ok(()) => {
                // Segment confirmed in S3; promote IPC tells the controlling
                // process (volume or import in serve phase) to write index/ +
                // cache/ and delete pending/<ulid>.
                if crate::control::promote_segment(vol_dir, ulid).await {
                    uploaded += 1;
                } else {
                    // No process listening; pending/<ulid> stays in place.
                    // The coordinator retries on the next drain tick.
                    warn!("promote {name}: no process listening; will retry next tick");
                    failed += 1;
                }
            }
            Err(e) => {
                warn!("upload failed for segment {name}: {e:#}");
                failed += 1;
            }
        }
    }

    Ok(DrainResult { uploaded, failed })
}

/// Upload volume metadata: public key, signed provenance, manifest.toml,
/// names/<name> entry, snapshot markers, and filemaps.
///
/// All uploads are best-effort — failures are logged but do not abort drain.
/// Each artifact is gated on an `uploaded/<name>` file whose bytes must
/// equal the value we are about to upload; a mismatch (or missing file)
/// triggers upload. For small metadata (volume.pub, provenance, manifest.toml,
/// names_<name>) the `uploaded/` entry holds a verbatim copy of the uploaded
/// bytes, so the directory is inspectable with standard tools. The snapshot
/// triple (marker + filemap + .manifest) is covered by a single empty
/// sentinel at `uploaded/snapshots/<ulid>`.
async fn upload_volume_metadata(vol_dir: &Path, volume_id: &str, store: &Arc<dyn ObjectStore>) {
    let pub_key_path = vol_dir.join("volume.pub");
    match std::fs::read(&pub_key_path) {
        Ok(bytes) => {
            let sentinel = upload_sentinel(vol_dir, "volume.pub");
            if !is_already_uploaded(&sentinel, &bytes) {
                match upload_small_bytes(&bytes, volume_id, "volume.pub", MIME_TEXT, store).await {
                    Ok(()) => {
                        if let Err(e) = mark_uploaded(&sentinel, &bytes) {
                            warn!("failed to mark volume.pub sentinel: {e}");
                        }
                    }
                    Err(e) => warn!("pub key upload failed: {e:#}"),
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("failed to read volume.pub: {e:#}"),
    }

    let provenance_path = vol_dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE);
    match std::fs::read(&provenance_path) {
        Ok(bytes) => {
            let sentinel = upload_sentinel(vol_dir, elide_core::signing::VOLUME_PROVENANCE_FILE);
            if !is_already_uploaded(&sentinel, &bytes) {
                match upload_small_bytes(
                    &bytes,
                    volume_id,
                    elide_core::signing::VOLUME_PROVENANCE_FILE,
                    MIME_TEXT,
                    store,
                )
                .await
                {
                    Ok(()) => {
                        if let Err(e) = mark_uploaded(&sentinel, &bytes) {
                            warn!("failed to mark provenance sentinel: {e}");
                        }
                    }
                    Err(e) => warn!("provenance upload failed: {e:#}"),
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("failed to read provenance: {e:#}"),
    }

    if let Err(e) = upload_manifest(vol_dir, volume_id, store).await {
        warn!("manifest upload failed: {e:#}");
    }

    if let Err(e) = upload_snapshots_and_filemaps(vol_dir, volume_id, store).await {
        warn!("snapshot/filemap upload failed: {e:#}");
    }
}

/// Upload `<vol_dir>/volume.pub` to `by_id/<volume_id>/volume.pub` and write
/// the local upload sentinel.
///
/// Used at create / fork time to establish the invariant
/// "`names/<name>` only ever points at a `vol_ulid` whose `volume.pub` is
/// already in the bucket". If the coordinator dies after this call but
/// before the caller publishes `names/<name>`, the only artefact left in
/// S3 is an orphan `volume.pub` keyed by an unreferenced ULID — harmless,
/// and reclaimable by future GC.
///
/// The sentinel write means the daemon's later `upload_volume_metadata`
/// pass observes a content-equal sentinel and skips the redundant PUT.
pub async fn upload_volume_pub_initial(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let pub_key_path = vol_dir.join("volume.pub");
    let bytes = std::fs::read(&pub_key_path)
        .with_context(|| format!("reading {}", pub_key_path.display()))?;
    upload_small_bytes(&bytes, volume_id, "volume.pub", MIME_TEXT, store).await?;
    let sentinel = upload_sentinel(vol_dir, "volume.pub");
    mark_uploaded(&sentinel, &bytes)
        .with_context(|| format!("writing upload sentinel {}", sentinel.display()))?;
    Ok(())
}

async fn upload_small_bytes(
    data: &[u8],
    volume_id: &str,
    remote_name: &str,
    content_type: &'static str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let len = data.len();
    let key = StorePath::from(format!("by_id/{volume_id}/{remote_name}"));
    let started = Instant::now();
    put_with_content_type(store, &key, Bytes::copy_from_slice(data), content_type)
        .await
        .with_context(|| format!("uploading {remote_name} to {key}"))?;
    info!("[upload] {key} ({len} bytes in {:.2?})", started.elapsed());
    Ok(())
}

/// Build and upload `by_id/<volume_id>/manifest.toml` from local metadata files.
async fn upload_manifest(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let cfg = elide_core::config::VolumeConfig::read(vol_dir).context("reading volume.toml")?;
    let name = cfg
        .name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("volume.toml missing name"))?
        .trim()
        .to_owned();
    let name = name.as_str();
    let size = cfg
        .size
        .ok_or_else(|| anyhow::anyhow!("volume.toml missing size"))?;

    let readonly = vol_dir.join("volume.readonly").exists();

    let origin_raw = std::fs::read_to_string(vol_dir.join("volume.parent")).ok();
    let origin = origin_raw.as_deref().map(str::trim);

    // Read OCI source metadata from local meta.toml if present.
    let meta_raw = std::fs::read_to_string(vol_dir.join("meta.toml")).ok();
    let meta_table: Option<toml::Table> = meta_raw.as_deref().and_then(|s| toml::from_str(s).ok());
    let source = meta_table.as_ref().and_then(|t| {
        let image = t.get("source")?.as_str()?;
        let digest = t.get("digest")?.as_str()?;
        let arch = t.get("arch")?.as_str()?;
        Some(ManifestSource {
            image,
            digest,
            arch,
        })
    });

    let manifest = Manifest {
        name,
        size,
        readonly,
        origin,
        source,
    };

    // Render manifest.toml up front so we can compare against the cached
    // copy at `uploaded/manifest.toml`. Content-equal gating means re-saving
    // volume.toml without changing a manifest-visible field does not trigger
    // a re-upload.
    let content_bytes = toml::to_string(&manifest)
        .context("serializing manifest.toml")?
        .into_bytes();
    let manifest_sentinel = upload_sentinel(vol_dir, "manifest.toml");
    if !is_already_uploaded(&manifest_sentinel, &content_bytes) {
        let manifest_len = content_bytes.len();
        let key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
        let started = Instant::now();
        put_with_content_type(
            store,
            &key,
            Bytes::copy_from_slice(&content_bytes),
            MIME_TOML,
        )
        .await
        .with_context(|| format!("uploading manifest.toml to {key}"))?;
        info!(
            "[upload] {key} ({manifest_len} bytes in {:.2?})",
            started.elapsed()
        );
        if let Err(e) = mark_uploaded(&manifest_sentinel, &content_bytes) {
            warn!("failed to mark manifest.toml sentinel: {e}");
        }
    }

    // The `names/<name>` record is owned by the lifecycle verbs in
    // `crate::lifecycle`: `mark_initial` claims it at create / fork /
    // import time, and `mark_stopped` / `mark_released` / `mark_live`
    // / `mark_claimed` mutate it on state transitions. Drain has no
    // business writing it — an unconditional PUT here would clobber
    // the populated record (overwriting `coordinator_id`,
    // `claimed_at`, `hostname`).

    Ok(())
}

/// Upload a snapshot marker as an empty object at
/// `by_id/<volume_id>/snapshots/YYYYMMDD/<snapshot_ulid>`.
pub async fn upload_snapshot(
    volume_id: &str,
    snapshot_ulid: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let key = snapshot_key(volume_id, snapshot_ulid)?;
    let started = Instant::now();
    store
        .put(&key, Bytes::new().into())
        .await
        .with_context(|| format!("uploading snapshot marker to {key}"))?;
    info!(
        "[upload] {key} (0 bytes, snapshot marker in {:.2?})",
        started.elapsed()
    );
    Ok(())
}

/// Upload all snapshot markers, filemaps, and signed segments manifests from
/// `vol_dir/snapshots/` to S3.
///
/// For each snapshot ULID found locally, uploads:
/// - The empty snapshot marker at `snapshots/YYYYMMDD/<ulid>`
/// - The filemap at `snapshots/YYYYMMDD/<ulid>.filemap` (if present)
/// - The signed snapshot manifest at `snapshots/YYYYMMDD/<ulid>.manifest` (if present)
pub async fn upload_snapshots_and_filemaps(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let snap_dir = vol_dir.join("snapshots");
    let entries = match std::fs::read_dir(&snap_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };

    for entry in entries {
        let entry = entry.context("reading snapshots dir entry")?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        // Skip filemap files — they are uploaded alongside their snapshot marker.
        if name.contains('.') {
            continue;
        }
        // Validate as ULID.
        if ulid::Ulid::from_string(name).is_err() {
            continue;
        }

        // Per-snapshot triple (marker + filemap + .manifest) is immutable
        // for a given ULID, so one empty sentinel covers all three. Bytes
        // aren't useful here — the S3 marker is empty, and the filemap /
        // .manifest pair is already inspectable under `snapshots/<ulid>.*`.
        let sentinel = upload_sentinel(vol_dir, &format!("snapshots/{name}"));
        if is_already_uploaded(&sentinel, &[]) {
            continue;
        }

        let mut all_ok = true;

        // Upload snapshot marker.
        if let Err(e) = upload_snapshot(volume_id, name, store).await {
            warn!("snapshot marker upload failed for {name}: {e:#}");
            all_ok = false;
        }

        // Upload filemap if present.
        let filemap_path = snap_dir.join(format!("{name}.filemap"));
        if filemap_path.exists() {
            let key = filemap_key(volume_id, name)?;
            let data = std::fs::read(&filemap_path)
                .with_context(|| format!("reading filemap: {}", filemap_path.display()))?;
            let len = data.len();
            let started = Instant::now();
            match put_with_content_type(store, &key, Bytes::from(data), MIME_TEXT).await {
                Ok(()) => {
                    info!("[upload] {key} ({len} bytes in {:.2?})", started.elapsed());
                }
                Err(e) => {
                    warn!("filemap upload failed for {key}: {e:#}");
                    all_ok = false;
                }
            }
        }

        // Upload signed segments manifest if present.
        let manifest_path = snap_dir.join(format!("{name}.manifest"));
        if manifest_path.exists() {
            let key = snapshot_manifest_key(volume_id, name)?;
            let data = std::fs::read(&manifest_path).with_context(|| {
                format!("reading snapshot manifest: {}", manifest_path.display())
            })?;
            let len = data.len();
            let started = Instant::now();
            match put_with_content_type(store, &key, Bytes::from(data), MIME_TEXT).await {
                Ok(()) => {
                    info!("[upload] {key} ({len} bytes in {:.2?})", started.elapsed());
                }
                Err(e) => {
                    warn!("snapshot manifest upload failed for {key}: {e:#}");
                    all_ok = false;
                }
            }
        }

        if all_ok && let Err(e) = mark_uploaded(&sentinel, &[]) {
            warn!("failed to mark snapshot {name} sentinel: {e}");
        }
    }

    Ok(())
}

/// Upload a segment body file to S3 at its canonical segment key.
///
/// Used by both the drain path (pending/<ulid>) and GC compaction (gc body).
/// The upload goes via multipart: each part is a separate request with its
/// own timeout and retry, so a stalled part no longer forces a restart of
/// the whole segment upload. Small segments complete in a single part at
/// roughly the same cost as a simple PUT.
///
/// Concurrency is capped at `MAX_CONCURRENT_PARTS` via `wait_for_capacity`
/// so parallel parts don't saturate the upload link and trip reqwest's
/// 30s per-request timeout. Two parts in flight is enough to hide one
/// request's handshake latency without fanning out.
pub(crate) async fn upload_segment_file(
    path: &Path,
    ulid_str: &str,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
    part_size_bytes: usize,
) -> Result<()> {
    const MAX_CONCURRENT_PARTS: usize = 2;

    let key = segment_key(volume_id, ulid_str)?;
    let data = std::fs::read(path).with_context(|| format!("reading segment {ulid_str}"))?;
    let len = data.len();
    let mut bytes = Bytes::from(data);

    let started = Instant::now();
    let upload = store
        .put_multipart(&key)
        .await
        .with_context(|| format!("initiating multipart upload for {key}"))?;
    let mut writer = WriteMultipart::new_with_chunk_size(upload, part_size_bytes);
    while !bytes.is_empty() {
        let take = bytes.len().min(part_size_bytes);
        let part = bytes.split_to(take);
        writer
            .wait_for_capacity(MAX_CONCURRENT_PARTS)
            .await
            .with_context(|| format!("multipart part upload for {key}"))?;
        writer.put(part);
    }
    writer
        .finish()
        .await
        .with_context(|| format!("uploading segment {ulid_str} to {key}"))?;
    info!("[upload] {key} ({len} bytes in {:.2?})", started.elapsed());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    struct MockSocket(tokio::task::JoinHandle<()>);
    impl Drop for MockSocket {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    /// Spawn a mock volume control socket at `<fork_dir>/control.sock`.
    ///
    /// Replies `Envelope::Ok` to any request. For [`VolumeRequest::Promote`]
    /// also performs the volume's promote behaviour: copies the segment body
    /// from pending/ into cache/ and deletes the pending/ file (drain path).
    async fn spawn_mock_socket(fork_dir: std::path::PathBuf) -> MockSocket {
        use elide_core::ipc::Envelope;
        use elide_core::volume_ipc::VolumeRequest;

        let socket_path = fork_dir.join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let dir = fork_dir.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
                    let (r, mut w) = tokio::io::split(stream);
                    let mut buf = BufReader::new(r);
                    let mut line = String::new();
                    let _ = buf.read_line(&mut line).await;
                    let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
                    if let Ok(VolumeRequest::Promote { segment_ulid }) =
                        serde_json::from_str::<VolumeRequest>(trimmed)
                    {
                        let ulid_str = segment_ulid.to_string();
                        let src = dir.join("pending").join(&ulid_str);
                        if src.exists() {
                            let cache = dir.join("cache");
                            std::fs::create_dir_all(&cache).ok();
                            let body = cache.join(format!("{ulid_str}.body"));
                            let present = cache.join(format!("{ulid_str}.present"));
                            elide_core::segment::promote_to_cache(&src, &body, &present).ok();
                            std::fs::remove_file(&src).ok();
                        }
                    }
                    let reply = Envelope::<()>::ok(());
                    let mut bytes = serde_json::to_vec(&reply).unwrap();
                    bytes.push(b'\n');
                    w.write_all(&bytes).await.ok();
                });
            }
        });
        MockSocket(handle)
    }

    fn make_ulid(ts_ms: u64, random: u128) -> String {
        Ulid::from_parts(ts_ms, random).to_string()
    }

    const VOL_ULID: &str = "01JQAAAAAAAAAAAAAAAAAAAAAA";

    #[test]
    fn derive_names_returns_ulid() {
        let id = derive_names(Path::new(&format!("/data/by_id/{VOL_ULID}"))).unwrap();
        assert_eq!(id, VOL_ULID);
    }

    #[test]
    fn derive_names_rejects_non_ulid() {
        assert!(derive_names(Path::new("/data/by_id/not-a-ulid")).is_err());
    }

    #[test]
    fn segment_key_format() {
        let ulid = Ulid::from_parts(1743120000000, 42);
        let ulid_str = ulid.to_string();

        let dt: DateTime<Utc> = ulid.datetime().into();
        let expected_date = dt.format("%Y%m%d").to_string();

        let key = segment_key(VOL_ULID, &ulid_str).unwrap();
        assert_eq!(
            key.as_ref(),
            format!("by_id/{VOL_ULID}/segments/{expected_date}/{ulid_str}")
        );
    }

    #[test]
    fn snapshot_key_format() {
        let ulid = Ulid::from_parts(1743120000000, 42);
        let ulid_str = ulid.to_string();

        let dt: DateTime<Utc> = ulid.datetime().into();
        let expected_date = dt.format("%Y%m%d").to_string();

        let key = snapshot_key(VOL_ULID, &ulid_str).unwrap();
        assert_eq!(
            key.as_ref(),
            format!("by_id/{VOL_ULID}/snapshots/{expected_date}/{ulid_str}")
        );
    }

    #[tokio::test]
    async fn drain_pending_uploads_and_commits() {
        use elide_core::segment::{SegmentEntry, SegmentFlags, write_segment};
        use elide_core::signing::generate_ephemeral_signer;

        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        let pending_dir = vol_dir.join("pending");
        let cache_dir = vol_dir.join("cache");
        std::fs::create_dir_all(&pending_dir).unwrap();
        elide_core::config::VolumeConfig {
            name: Some("test-vol".into()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol_dir)
        .unwrap();

        let (signer, _) = generate_ephemeral_signer();

        let ulid1 = make_ulid(1743120000000, 1);
        let ulid2 = make_ulid(1743120000000, 2);

        let data1 = vec![0xABu8; 4096];
        let h1 = blake3::hash(&data1);
        let mut entries1 = vec![SegmentEntry::new_data(
            h1,
            0,
            1,
            SegmentFlags::empty(),
            data1,
        )];
        write_segment(&pending_dir.join(&ulid1), &mut entries1, signer.as_ref()).unwrap();

        let data2 = vec![0xCDu8; 4096];
        let h2 = blake3::hash(&data2);
        let mut entries2 = vec![SegmentEntry::new_data(
            h2,
            1,
            1,
            SegmentFlags::empty(),
            data2,
        )];
        write_segment(&pending_dir.join(&ulid2), &mut entries2, signer.as_ref()).unwrap();

        // .tmp files must be left in place.
        std::fs::write(pending_dir.join(format!("{ulid1}.tmp")), b"incomplete").unwrap();

        // Mock volume socket: responds "ok" to promote and copies pending → cache.
        let _mock = spawn_mock_socket(vol_dir.clone()).await;

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();

        assert_eq!(result.uploaded, 2);
        assert_eq!(result.failed, 0);

        // pending/ entries are removed by the volume after promote IPC (mocked here).
        assert!(!pending_dir.join(&ulid1).exists());
        assert!(!pending_dir.join(&ulid2).exists());
        assert!(pending_dir.join(format!("{ulid1}.tmp")).exists());

        // cache/ body + present files are written by the mock volume promote handler.
        assert!(cache_dir.join(format!("{ulid1}.body")).exists());
        assert!(cache_dir.join(format!("{ulid1}.present")).exists());
        assert!(cache_dir.join(format!("{ulid2}.body")).exists());
        assert!(cache_dir.join(format!("{ulid2}.present")).exists());

        let key1 = segment_key(VOL_ULID, &ulid1).unwrap();
        let key2 = segment_key(VOL_ULID, &ulid2).unwrap();
        store
            .head(&key1)
            .await
            .expect("object 1 should be in store");
        store
            .head(&key2)
            .await
            .expect("object 2 should be in store");
    }

    #[tokio::test]
    async fn drain_pending_uploads_pub_key() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        let pending_dir = vol_dir.join("pending");
        std::fs::create_dir_all(&pending_dir).unwrap();
        elide_core::config::VolumeConfig {
            name: Some("test-vol".into()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol_dir)
        .unwrap();

        let fake_pub = b"fakepublickey12345678901234567890";
        std::fs::write(vol_dir.join("volume.pub"), fake_pub).unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();
        assert_eq!(result.uploaded, 0);
        assert_eq!(result.failed, 0);

        let pub_key = StorePath::from(format!("by_id/{VOL_ULID}/volume.pub"));
        let got = store.get(&pub_key).await.expect("volume.pub not in store");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), fake_pub);

        // uploaded/volume.pub holds a verbatim copy of the uploaded bytes.
        let sentinel = vol_dir.join("uploaded").join("volume.pub");
        assert_eq!(std::fs::read(&sentinel).unwrap(), fake_pub);
    }

    #[tokio::test]
    async fn drain_pending_uploads_manifest_and_does_not_touch_name_record() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        let pending_dir = vol_dir.join("pending");
        std::fs::create_dir_all(&pending_dir).unwrap();
        elide_core::config::VolumeConfig {
            name: Some("my-vol".into()),
            size: Some(8192),
            ..Default::default()
        }
        .write(&vol_dir)
        .unwrap();
        std::fs::write(vol_dir.join("volume.readonly"), "").unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();

        // manifest.toml should be present and parseable.
        let manifest_key = StorePath::from(format!("by_id/{VOL_ULID}/manifest.toml"));
        let got = store
            .get(&manifest_key)
            .await
            .expect("manifest.toml not in store");
        let content = String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap();
        let table: toml::Table = toml::from_str(&content).unwrap();
        assert_eq!(table["name"].as_str(), Some("my-vol"));
        assert_eq!(table["size"].as_integer(), Some(8192));
        assert_eq!(table["readonly"].as_bool(), Some(true));

        // names/<name> is owned by the lifecycle verbs, not drain. The
        // drain path must not touch it — assert no record was created.
        let name_key = StorePath::from("names/my-vol");
        assert!(
            store.head(&name_key).await.is_err(),
            "drain_pending must not write names/<name>; that is owned by mark_initial / lifecycle verbs"
        );

        // uploaded/ holds verbatim copies of the uploaded bytes — diff-able
        // with standard tools against the store objects.
        let uploaded = vol_dir.join("uploaded");
        assert_eq!(
            std::fs::read_to_string(uploaded.join("manifest.toml")).unwrap(),
            content
        );
        // No `uploaded/names_<name>` sentinel either — drain doesn't write
        // the record, so it has no sentinel to compare against.
        assert!(!uploaded.join("names_my-vol").exists());
    }

    /// Volume-metadata upload is skipped on re-drain when the rendered bytes
    /// match the existing `uploaded/<f>` entry. Regression guard for the
    /// mtime→content-equal gating switch: touching `volume.toml` (or any
    /// source) without changing the rendered manifest must not re-upload.
    #[tokio::test]
    async fn drain_skips_reupload_when_metadata_unchanged() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        std::fs::create_dir_all(vol_dir.join("pending")).unwrap();
        elide_core::config::VolumeConfig {
            name: Some("stable".into()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol_dir)
        .unwrap();
        std::fs::write(vol_dir.join("volume.pub"), b"k").unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();

        // Delete the store objects behind the coordinator's back. If gating
        // works, re-drain sees matching `uploaded/` and skips — the objects
        // remain absent. If gating is broken (e.g. reverted to mtime), the
        // objects reappear.
        let manifest_key = StorePath::from(format!("by_id/{VOL_ULID}/manifest.toml"));
        let pub_key = StorePath::from(format!("by_id/{VOL_ULID}/volume.pub"));
        store.delete(&manifest_key).await.unwrap();
        store.delete(&pub_key).await.unwrap();

        drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();

        assert!(store.head(&manifest_key).await.is_err());
        assert!(store.head(&pub_key).await.is_err());

        // Now change volume.pub content — re-drain must upload.
        std::fs::write(vol_dir.join("volume.pub"), b"rotated").unwrap();
        drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();
        let got = store.get(&pub_key).await.expect("volume.pub re-uploaded");
        assert_eq!(got.bytes().await.unwrap().as_ref(), b"rotated");
    }

    #[tokio::test]
    async fn upload_snapshot_writes_empty_object() {
        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let snap_ulid = Ulid::from_parts(1743120000000, 99).to_string();
        upload_snapshot(VOL_ULID, &snap_ulid, &store).await.unwrap();

        let key = snapshot_key(VOL_ULID, &snap_ulid).unwrap();
        let meta = store.head(&key).await.expect("snapshot not in store");
        assert_eq!(meta.size, 0);
    }

    #[test]
    fn filemap_key_format() {
        let ulid = Ulid::from_parts(1743120000000, 42);
        let ulid_str = ulid.to_string();

        let dt: DateTime<Utc> = ulid.datetime().into();
        let expected_date = dt.format("%Y%m%d").to_string();

        let key = filemap_key(VOL_ULID, &ulid_str).unwrap();
        assert_eq!(
            key.as_ref(),
            format!("by_id/{VOL_ULID}/snapshots/{expected_date}/{ulid_str}.filemap")
        );
    }

    #[tokio::test]
    async fn drain_uploads_snapshot_and_filemap() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        let pending_dir = vol_dir.join("pending");
        let snap_dir = vol_dir.join("snapshots");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&snap_dir).unwrap();
        elide_core::config::VolumeConfig {
            name: Some("test-vol".into()),
            size: Some(4096),
            ..Default::default()
        }
        .write(&vol_dir)
        .unwrap();

        // Create a snapshot marker and filemap.
        let snap_ulid = Ulid::from_parts(1743120000000, 77).to_string();
        std::fs::write(snap_dir.join(&snap_ulid), "").unwrap();
        let filemap_content = "# elide-filemap v1\n/etc/hosts\tabcd1234\t128\n";
        std::fs::write(
            snap_dir.join(format!("{snap_ulid}.filemap")),
            filemap_content,
        )
        .unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        drain_pending(&vol_dir, VOL_ULID, &store, DEFAULT_PART_SIZE_BYTES)
            .await
            .unwrap();

        // Snapshot marker should be in store.
        let snap_key = snapshot_key(VOL_ULID, &snap_ulid).unwrap();
        let meta = store
            .head(&snap_key)
            .await
            .expect("snapshot marker not in store");
        assert_eq!(meta.size, 0);

        // Filemap should be in store with correct content.
        let fm_key = filemap_key(VOL_ULID, &snap_ulid).unwrap();
        let got = store.get(&fm_key).await.expect("filemap not in store");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), filemap_content);
    }
}
