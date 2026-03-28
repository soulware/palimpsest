// Segment upload: drain all committed segments from pending/ to the object store.
//
// Object key format: <volume_id>/<fork_name>/YYYYMMDD/<ulid>
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

/// Derive `(volume_id, fork_name)` from a fork directory path.
///
/// Fork directories are either `<vol-root>/base` (the base fork) or
/// `<vol-root>/forks/<name>` (named forks). The volume_id is the directory
/// name of the volume root.
pub fn derive_names(fork_dir: &Path) -> Result<(String, String)> {
    let fork_name = fork_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("fork dir has no name: {}", fork_dir.display()))?
        .to_owned();

    let parent = fork_dir
        .parent()
        .ok_or_else(|| anyhow::anyhow!("fork dir has no parent: {}", fork_dir.display()))?;

    // Named forks live under forks/; the base fork lives directly under the vol root.
    let volume_root = if parent.file_name().and_then(|n| n.to_str()) == Some("forks") {
        parent
            .parent()
            .ok_or_else(|| anyhow::anyhow!("forks/ dir has no parent"))?
    } else {
        parent
    };

    let volume_id = volume_root
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("volume root has no name: {}", volume_root.display()))?
        .to_owned();

    Ok((volume_id, fork_name))
}

/// Build the object store key for a segment.
///
/// Format: `<volume_id>/<fork_name>/YYYYMMDD/<ulid>`
pub fn segment_key(volume_id: &str, fork_name: &str, ulid_str: &str) -> Result<StorePath> {
    let ulid: Ulid = ulid_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid ULID '{ulid_str}': {e}"))?;
    let dt: DateTime<Utc> = ulid.datetime().into();
    let date = dt.format("%Y%m%d").to_string();
    Ok(StorePath::from(format!(
        "{volume_id}/{fork_name}/{date}/{ulid_str}"
    )))
}

/// Upload all committed segments from `pending/` to the object store, moving
/// each successfully uploaded segment to `segments/`. Also uploads the fork's
/// public key to `<volume_id>/<fork_name>/fork.pub` so that new hosts can
/// verify fetched segments (trust-on-first-use).
pub async fn drain_pending(
    fork_dir: &Path,
    volume_id: &str,
    fork_name: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<DrainResult> {
    let pending_dir = fork_dir.join("pending");
    let segments_dir = fork_dir.join("segments");

    // Upload the fork public key before segments so that any host that
    // demand-fetches a segment immediately after upload can verify it.
    // For the base fork the key lives at <vol_root>/base.pub; for named forks
    // it is fork.pub inside the fork directory.
    let pub_key_path = pub_key_path(fork_dir, fork_name);
    if pub_key_path.exists() {
        if let Err(e) = upload_pub_key(&pub_key_path, volume_id, fork_name, store).await {
            eprintln!("pub key upload failed: {e:#}");
        }
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

        match upload_segment(
            &segment_path,
            &name,
            &segments_dir,
            volume_id,
            fork_name,
            store,
        )
        .await
        {
            Ok(()) => uploaded += 1,
            Err(e) => {
                eprintln!("upload failed for segment {name}: {e:#}");
                failed += 1;
            }
        }
    }

    Ok(DrainResult { uploaded, failed })
}

/// Return the local path of the fork's public key.
///
/// The base fork stores its key at `<vol_root>/base.pub` (sibling of `base/`).
/// Named forks store it at `<fork_dir>/fork.pub`.
fn pub_key_path(fork_dir: &Path, fork_name: &str) -> std::path::PathBuf {
    if fork_name == "base" {
        // base fork: key is at <vol_root>/base.pub, next to the base/ directory
        fork_dir
            .parent()
            .map(|p| p.join("base.pub"))
            .unwrap_or_else(|| fork_dir.join("base.pub"))
    } else {
        fork_dir.join("fork.pub")
    }
}

async fn upload_pub_key(
    pub_key_path: &Path,
    volume_id: &str,
    fork_name: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let data = tokio::fs::read(pub_key_path)
        .await
        .with_context(|| format!("reading pub key: {}", pub_key_path.display()))?;
    let key = StorePath::from(format!("{volume_id}/{fork_name}/fork.pub"));
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
    fork_name: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let key = segment_key(volume_id, fork_name, ulid_str)?;

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

    #[test]
    fn derive_names_base_fork() {
        let (vol, fork) = derive_names(Path::new("/volumes/myvm/base")).unwrap();
        assert_eq!(vol, "myvm");
        assert_eq!(fork, "base");
    }

    #[test]
    fn derive_names_named_fork() {
        let (vol, fork) = derive_names(Path::new("/volumes/myvm/forks/vm1")).unwrap();
        assert_eq!(vol, "myvm");
        assert_eq!(fork, "vm1");
    }

    #[test]
    fn segment_key_format() {
        let ulid = Ulid::from_parts(1743120000000, 42);
        let ulid_str = ulid.to_string();

        // Derive the expected date the same way the production code does.
        let dt: DateTime<Utc> = ulid.datetime().into();
        let expected_date = dt.format("%Y%m%d").to_string();

        let key = segment_key("myvm", "base", &ulid_str).unwrap();
        assert_eq!(
            key.as_ref(),
            format!("myvm/base/{expected_date}/{ulid_str}")
        );
    }

    #[tokio::test]
    async fn drain_pending_uploads_and_commits() {
        let volume_tmp = TempDir::new().unwrap();
        let fork_dir = volume_tmp.path().join("myvm").join("base");
        let pending_dir = fork_dir.join("pending");
        let segments_dir = fork_dir.join("segments");
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

        let result = drain_pending(&fork_dir, "myvm", "base", &store)
            .await
            .unwrap();

        assert_eq!(result.uploaded, 2);
        assert_eq!(result.failed, 0);

        // Segments must have moved out of pending/
        assert!(!pending_dir.join(&ulid1).exists());
        assert!(!pending_dir.join(&ulid2).exists());

        // Segments must be present in segments/
        assert!(segments_dir.join(&ulid1).exists());
        assert!(segments_dir.join(&ulid2).exists());

        // .tmp must be untouched in pending/
        assert!(pending_dir.join(format!("{ulid1}.tmp")).exists());

        // Objects must exist in the store at the expected keys
        let key1 = segment_key("myvm", "base", &ulid1).unwrap();
        let key2 = segment_key("myvm", "base", &ulid2).unwrap();
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
    async fn drain_pending_uploads_pub_key_for_base_fork() {
        let volume_tmp = TempDir::new().unwrap();
        let fork_dir = volume_tmp.path().join("myvm").join("base");
        let pending_dir = fork_dir.join("pending");
        let segments_dir = fork_dir.join("segments");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&segments_dir).unwrap();

        // Write a fake base.pub at the volume root (sibling of base/).
        let vol_root = volume_tmp.path().join("myvm");
        let fake_pub = b"fakepublickey12345678901234567890";
        std::fs::write(vol_root.join("base.pub"), fake_pub).unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&fork_dir, "myvm", "base", &store)
            .await
            .unwrap();
        assert_eq!(result.uploaded, 0);
        assert_eq!(result.failed, 0);

        // fork.pub must have been uploaded to the expected key
        let pub_key = StorePath::from("myvm/base/fork.pub");
        let got = store.get(&pub_key).await.expect("fork.pub not in store");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), fake_pub);
    }

    #[tokio::test]
    async fn drain_pending_uploads_pub_key_for_named_fork() {
        let volume_tmp = TempDir::new().unwrap();
        let fork_dir = volume_tmp.path().join("myvm").join("forks").join("vm1");
        let pending_dir = fork_dir.join("pending");
        let segments_dir = fork_dir.join("segments");
        std::fs::create_dir_all(&pending_dir).unwrap();
        std::fs::create_dir_all(&segments_dir).unwrap();

        // Write a fake fork.pub inside the fork directory.
        let fake_pub = b"fakeforkpublickey0123456789012345";
        std::fs::write(fork_dir.join("fork.pub"), fake_pub).unwrap();

        let store_tmp = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(store_tmp.path()).unwrap());

        let result = drain_pending(&fork_dir, "myvm", "vm1", &store)
            .await
            .unwrap();
        assert_eq!(result.uploaded, 0);
        assert_eq!(result.failed, 0);

        let pub_key = StorePath::from("myvm/vm1/fork.pub");
        let got = store.get(&pub_key).await.expect("fork.pub not in store");
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), fake_pub);
    }
}
