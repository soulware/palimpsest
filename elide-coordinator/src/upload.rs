// Segment upload: drain all committed segments from pending/ to the object store.
//
// Object key format: <volume_ulid>/YYYYMMDD/<ulid>
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
//   4. On failure: leave in pending/, record error, continue

use std::path::Path;

use std::sync::Arc;
use tracing::warn;

use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
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
/// Format: `<volume_ulid>/YYYYMMDD/<ulid>`
pub fn segment_key(volume_id: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!("{volume_id}/{date}/{ulid_str}")))
}

/// Upload all committed segments from `pending/` to the object store, moving
/// each successfully uploaded segment to `segments/`. Also uploads the volume's
/// public key to `<volume_id>/volume.pub` so that new hosts can verify fetched
/// segments (trust-on-first-use).
///
/// `drain_pending` is a one-shot batch command. The key is re-uploaded on every
/// invocation (idempotent, 32 bytes). A future persistent watcher will replace
/// this: it uploads the key once at startup and then streams segments as they
/// appear in `pending/`, making the per-invocation key upload unnecessary.
pub async fn drain_pending(
    vol_dir: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<DrainResult> {
    let pending_dir = vol_dir.join("pending");
    let segments_dir = vol_dir.join("segments");

    // Upload the volume public key before segments so that any host that
    // demand-fetches a segment immediately after upload can verify it.
    let pub_key_path = vol_dir.join("volume.pub");
    if pub_key_path.exists()
        && let Err(e) = upload_pub_key(&pub_key_path, volume_id, store).await
    {
        warn!("pub key upload failed: {e:#}");
    }

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

        match upload_segment(&segment_path, &name, &segments_dir, volume_id, store).await {
            Ok(()) => uploaded += 1,
            Err(e) => {
                warn!("upload failed for segment {name}: {e:#}");
                failed += 1;
            }
        }
    }

    Ok(DrainResult { uploaded, failed })
}

async fn upload_pub_key(
    pub_key_path: &Path,
    volume_id: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let data = tokio::fs::read(pub_key_path)
        .await
        .with_context(|| format!("reading pub key: {}", pub_key_path.display()))?;
    let key = StorePath::from(format!("{volume_id}/volume.pub"));
    store
        .put(&key, Bytes::from(data).into())
        .await
        .with_context(|| format!("uploading pub key to {key}"))?;
    Ok(())
}

async fn upload_segment(
    path: &Path,
    ulid_str: &str,
    segments_dir: &Path,
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
            format!("{VOL_ULID}/{expected_date}/{ulid_str}")
        );
    }

    #[tokio::test]
    async fn drain_pending_uploads_and_commits() {
        let tmp = TempDir::new().unwrap();
        let vol_dir = tmp.path().join(VOL_ULID);
        let pending_dir = vol_dir.join("pending");
        let segments_dir = vol_dir.join("segments");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&segments_dir).unwrap();

        let ulid1 = make_ulid(1743120000000, 1);
        let ulid2 = make_ulid(1743120000000, 2);
        std::fs::write(pending_dir.join(&ulid1), b"segment data 1").unwrap();
        std::fs::write(pending_dir.join(&ulid2), b"segment data 2").unwrap();
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

        let fake_pub = b"fakepublickey12345678901234567890";
        std::fs::write(vol_dir.join("volume.pub"), fake_pub).unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&vol_dir, VOL_ULID, &store).await.unwrap();
        assert_eq!(result.uploaded, 0);
        assert_eq!(result.failed, 0);

        let pub_key = StorePath::from(format!("{VOL_ULID}/volume.pub"));
        let got = store.get(&pub_key).await.expect("volume.pub not in store");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), fake_pub);
    }
}
