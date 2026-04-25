// Pull a readonly ancestor for a volume from S3 into the local data dir.
//
// A pulled ancestor is the minimal on-disk presence needed for the
// coordinator's prefetch task (and downstream code like `Volume::open`) to
// resolve a child's lineage:
//
//   by_id/<ulid>/
//     volume.toml              — size only; no name (visual indicator that
//                                this entry is a pulled ancestor, not a
//                                user-managed volume)
//     volume.readonly          — marker
//     volume.pub               — Ed25519 verifying key
//     volume.provenance        — signed lineage (parent + extent_index)
//     index/                   — empty, populated by `prefetch_indexes`
//
// This is the async counterpart to the CLI's `pull_one_readonly` in
// `src/main.rs`. The coordinator uses it to auto-heal ancestor chains when
// a newly-discovered volume references a parent that isn't locally present
// (the "self-healing prefetch" path).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;

/// Pull a readonly ancestor for `volume_id` from the object store into
/// `<data_dir>/by_id/<volume_id>/`. If the directory already exists,
/// returns its path without re-pulling (idempotent — safe to call on every
/// prefetch tick).
///
/// Fetches:
///   - `by_id/<volume_id>/manifest.toml`
///   - `by_id/<volume_id>/volume.pub`
///   - `by_id/<volume_id>/volume.provenance`
///
/// Writes them into `<data_dir>/by_id/<volume_id>/` along with a
/// `volume.readonly` marker, a `volume.toml` carrying only the size (no
/// name), and an empty `index/` directory. The absent name and absent
/// `by_name/` symlink mark this entry as a pulled ancestor rather than a
/// user-managed volume.
///
/// Signature verification of the downloaded `volume.provenance` is *not*
/// performed here — the caller is responsible for verifying under the
/// pubkey it trusts (typically embedded in the child's `ParentRef`).
/// Doing verification here would require choosing a key, which the pull
/// API has no context to do safely.
pub async fn pull_volume_skeleton(
    store: &Arc<dyn ObjectStore>,
    data_dir: &Path,
    volume_id: &str,
) -> Result<PathBuf> {
    let vol_dir = data_dir.join("by_id").join(volume_id);
    if vol_dir.exists() {
        return Ok(vol_dir);
    }

    let manifest_bytes = fetch_bytes(
        store,
        &StorePath::from(format!("by_id/{volume_id}/manifest.toml")),
        "manifest.toml",
        volume_id,
    )
    .await?;
    let manifest: toml::Table = toml::from_str(
        std::str::from_utf8(&manifest_bytes)
            .with_context(|| format!("manifest.toml for {volume_id} is not valid UTF-8"))?,
    )
    .with_context(|| format!("parsing manifest.toml for {volume_id}"))?;
    let size = manifest
        .get("size")
        .and_then(|v| v.as_integer())
        .ok_or_else(|| anyhow::anyhow!("manifest.toml for {volume_id} missing 'size'"))?;

    let pub_bytes = fetch_bytes(
        store,
        &StorePath::from(format!("by_id/{volume_id}/volume.pub")),
        "volume.pub",
        volume_id,
    )
    .await?;
    let provenance_bytes = fetch_bytes(
        store,
        &StorePath::from(format!(
            "by_id/{volume_id}/{}",
            elide_core::signing::VOLUME_PROVENANCE_FILE
        )),
        elide_core::signing::VOLUME_PROVENANCE_FILE,
        volume_id,
    )
    .await?;

    std::fs::create_dir_all(&vol_dir).with_context(|| format!("creating {}", vol_dir.display()))?;
    elide_core::config::VolumeConfig {
        name: None,
        size: Some(size as u64),
        ..Default::default()
    }
    .write(&vol_dir)
    .with_context(|| format!("writing volume.toml for {volume_id}"))?;
    std::fs::write(vol_dir.join("volume.readonly"), "")
        .with_context(|| format!("writing volume.readonly for {volume_id}"))?;
    std::fs::write(vol_dir.join("volume.pub"), &pub_bytes)
        .with_context(|| format!("writing volume.pub for {volume_id}"))?;
    std::fs::write(
        vol_dir.join(elide_core::signing::VOLUME_PROVENANCE_FILE),
        &provenance_bytes,
    )
    .with_context(|| format!("writing volume.provenance for {volume_id}"))?;
    std::fs::create_dir_all(vol_dir.join("index"))
        .with_context(|| format!("creating index/ for {volume_id}"))?;

    Ok(vol_dir)
}

async fn fetch_bytes(
    store: &Arc<dyn ObjectStore>,
    key: &StorePath,
    what: &str,
    volume_id: &str,
) -> Result<bytes::Bytes> {
    let resp = store
        .get(key)
        .await
        .with_context(|| format!("downloading {what} for {volume_id}"))?;
    resp.bytes()
        .await
        .with_context(|| format!("reading {what} for {volume_id}"))
}
