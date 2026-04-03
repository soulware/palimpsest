// Segment upload: drain all committed segments from pending/ to the object store.
//
// Object key format: by_id/<volume_ulid>/YYYYMMDD/<ulid>
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
//   3. On success: rename pending/<ulid> → segments/<ulid>
//   4. Write index/<ulid>.idx (header+index section) — confirms S3 availability
//   5. On failure: leave in pending/, record error, continue

use std::path::Path;

use std::sync::Arc;
use tracing::warn;

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde::Serialize;
use ulid::Ulid;

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
/// Format: `by_id/<volume_ulid>/YYYYMMDD/<segment_ulid>`
pub fn segment_key(volume_id: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "by_id/{volume_id}/{date}/{ulid_str}"
    )))
}

/// Build the object store key for a snapshot marker.
///
/// Format: `by_id/<volume_ulid>/snapshots/YYYYMMDD/<snapshot_ulid>`
#[allow(dead_code)] // used when S3 snapshot upload is wired up
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

/// Upload all committed segments from `pending/` to the object store, moving
/// each successfully uploaded segment to `segments/`. Also uploads the volume's
/// public key and manifest so that new hosts can bootstrap the volume.
///
/// `drain_pending` is a one-shot batch command. Metadata (pub key, manifest,
/// name entry) is re-uploaded on every invocation — all are idempotent and tiny.
pub async fn drain_pending(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<DrainResult> {
    let pending_dir = vol_dir.join("pending");
    let segments_dir = vol_dir.join("segments");
    let index_dir = vol_dir.join("index");

    // Ensure segments/ exists — import_image only creates pending/.
    tokio::fs::create_dir_all(&segments_dir)
        .await
        .with_context(|| format!("creating segments dir: {}", segments_dir.display()))?;

    // Upload volume metadata before segments so that any host that
    // demand-fetches a segment can immediately verify it and bootstrap the vol.
    upload_volume_metadata(vol_dir, volume_id, store).await;

    let mut entries = tokio::fs::read_dir(&pending_dir)
        .await
        .with_context(|| format!("opening pending dir: {}", pending_dir.display()))?;

    let mut uploaded = 0usize;
    let mut failed = 0usize;

    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name.ends_with(".tmp") {
            continue;
        }
        let name = name.to_owned();
        let segment_path = entry.path();

        match upload_segment(
            &segment_path,
            &name,
            &segments_dir,
            &index_dir,
            volume_id,
            store,
        )
        .await
        {
            Ok(()) => uploaded += 1,
            Err(e) => {
                warn!("upload failed for segment {name}: {e:#}");
                failed += 1;
            }
        }
    }

    Ok(DrainResult { uploaded, failed })
}

/// Upload volume metadata: public key, manifest.toml, and names/<name> entry.
///
/// All uploads are best-effort — failures are logged but do not abort drain.
async fn upload_volume_metadata(vol_dir: &Path, volume_id: &str, store: &Arc<dyn ObjectStore>) {
    let pub_key_path = vol_dir.join("volume.pub");
    if pub_key_path.exists()
        && let Err(e) = upload_pub_key(&pub_key_path, volume_id, store).await
    {
        warn!("pub key upload failed: {e:#}");
    }

    if let Err(e) = upload_manifest(vol_dir, volume_id, store).await {
        warn!("manifest upload failed: {e:#}");
    }
}

async fn upload_pub_key(
    pub_key_path: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let data = tokio::fs::read(pub_key_path)
        .await
        .with_context(|| format!("reading pub key: {}", pub_key_path.display()))?;
    let key = StorePath::from(format!("by_id/{volume_id}/volume.pub"));
    store
        .put(&key, Bytes::from(data).into())
        .await
        .with_context(|| format!("uploading pub key to {key}"))?;
    Ok(())
}

/// Build and upload `by_id/<volume_id>/manifest.toml` from local metadata files.
async fn upload_manifest(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let name = tokio::fs::read_to_string(vol_dir.join("volume.name"))
        .await
        .context("reading volume.name")?;
    let name = name.trim();

    let size_str = tokio::fs::read_to_string(vol_dir.join("volume.size"))
        .await
        .context("reading volume.size")?;
    let size: u64 = size_str.trim().parse().context("parsing volume.size")?;

    let readonly = vol_dir.join("volume.readonly").exists();

    let origin_raw = tokio::fs::read_to_string(vol_dir.join("volume.parent"))
        .await
        .ok();
    let origin = origin_raw.as_deref().map(str::trim);

    // Read OCI source metadata from local meta.toml if present.
    let meta_raw = tokio::fs::read_to_string(vol_dir.join("meta.toml"))
        .await
        .ok();
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

    let content = toml::to_string(&manifest).context("serializing manifest.toml")?;
    let key = StorePath::from(format!("by_id/{volume_id}/manifest.toml"));
    store
        .put(&key, Bytes::from(content.into_bytes()).into())
        .await
        .with_context(|| format!("uploading manifest.toml to {key}"))?;

    // Upload the name index entry: names/<name> → ULID.
    let name_key = StorePath::from(format!("names/{name}"));
    store
        .put(
            &name_key,
            Bytes::from(volume_id.to_owned().into_bytes()).into(),
        )
        .await
        .with_context(|| format!("uploading name entry to {name_key}"))?;

    Ok(())
}

/// Upload a snapshot marker as an empty object at
/// `by_id/<volume_id>/snapshots/YYYYMMDD/<snapshot_ulid>`.
#[allow(dead_code)] // used when S3 snapshot upload is wired up
pub async fn upload_snapshot(
    volume_id: &str,
    snapshot_ulid: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let key = snapshot_key(volume_id, snapshot_ulid)?;
    store
        .put(&key, Bytes::new().into())
        .await
        .with_context(|| format!("uploading snapshot marker to {key}"))?;
    Ok(())
}

async fn upload_segment(
    path: &Path,
    ulid_str: &str,
    segments_dir: &Path,
    index_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let key = segment_key(volume_id, ulid_str)?;

    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("reading segment {ulid_str}"))?;

    store
        .put(&key, Bytes::from(data).into())
        .await
        .with_context(|| format!("uploading segment {ulid_str} to {key}"))?;

    let dest = segments_dir.join(ulid_str);
    tokio::fs::rename(path, &dest)
        .await
        .with_context(|| format!("committing segment {ulid_str} to segments/"))?;

    // Write index/<ulid>.idx after confirmed S3 upload. This is the signal
    // that the segment is S3-backed and local body bytes are no longer needed
    // for LBA map correctness. Idempotent: skipped if .idx already exists.
    std::fs::create_dir_all(index_dir)
        .with_context(|| format!("creating index dir: {}", index_dir.display()))?;
    let idx_path = index_dir.join(format!("{ulid_str}.idx"));
    elide_core::segment::extract_idx(&dest, &idx_path)
        .with_context(|| format!("writing index/{ulid_str}.idx"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

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
            format!("by_id/{VOL_ULID}/{expected_date}/{ulid_str}")
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
        let segments_dir = vol_dir.join("segments");
        let index_dir = vol_dir.join("index");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&segments_dir).unwrap();
        std::fs::write(vol_dir.join("volume.name"), "test-vol").unwrap();
        std::fs::write(vol_dir.join("volume.size"), "4096").unwrap();

        let (signer, _) = generate_ephemeral_signer();

        let ulid1 = make_ulid(1743120000000, 1);
        let ulid2 = make_ulid(1743120000000, 2);

        // Write real segments — extract_idx requires valid segment format.
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

        // .tmp files must be left in place
        std::fs::write(pending_dir.join(format!("{ulid1}.tmp")), b"incomplete").unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&vol_dir, VOL_ULID, &store).await.unwrap();

        assert_eq!(result.uploaded, 2);
        assert_eq!(result.failed, 0);

        assert!(!pending_dir.join(&ulid1).exists());
        assert!(!pending_dir.join(&ulid2).exists());
        assert!(segments_dir.join(&ulid1).exists());
        assert!(segments_dir.join(&ulid2).exists());
        assert!(pending_dir.join(format!("{ulid1}.tmp")).exists());

        // index/*.idx must be written after confirmed upload.
        assert!(index_dir.join(format!("{ulid1}.idx")).exists());
        assert!(index_dir.join(format!("{ulid2}.idx")).exists());

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
        let segments_dir = vol_dir.join("segments");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&segments_dir).unwrap();
        std::fs::write(vol_dir.join("volume.name"), "test-vol").unwrap();
        std::fs::write(vol_dir.join("volume.size"), "4096").unwrap();

        let fake_pub = b"fakepublickey12345678901234567890";
        std::fs::write(vol_dir.join("volume.pub"), fake_pub).unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&vol_dir, VOL_ULID, &store).await.unwrap();
        assert_eq!(result.uploaded, 0);
        assert_eq!(result.failed, 0);

        let pub_key = StorePath::from(format!("by_id/{VOL_ULID}/volume.pub"));
        let got = store.get(&pub_key).await.expect("volume.pub not in store");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), fake_pub);
    }

    #[tokio::test]
    async fn drain_pending_uploads_manifest_and_name_entry() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        let pending_dir = vol_dir.join("pending");
        let segments_dir = vol_dir.join("segments");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&segments_dir).unwrap();
        std::fs::write(vol_dir.join("volume.name"), "my-vol").unwrap();
        std::fs::write(vol_dir.join("volume.size"), "8192").unwrap();
        std::fs::write(vol_dir.join("volume.readonly"), "").unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        drain_pending(&vol_dir, VOL_ULID, &store).await.unwrap();

        // manifest.toml should be present and parseable
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

        // names/<name> should contain the ULID
        let name_key = StorePath::from("names/my-vol");
        let got = store.get(&name_key).await.expect("name entry not in store");
        let ulid_bytes = got.bytes().await.unwrap();
        assert_eq!(std::str::from_utf8(&ulid_bytes).unwrap(), VOL_ULID);
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
}
