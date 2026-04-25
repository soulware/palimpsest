// Pending-delete reaper.
//
// Coordinator-wide ticker that walks each owned volume's
// `retention/` prefix, validates each marker, and deletes the
// referenced input segments + the marker itself when the retention
// window has elapsed.
//
// Marker shape and validation rules: see `crate::pending_delete` and
// `docs/design-replica-model.md` (Reaper / Target validation /
// Cadence and dispatch).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::pending_delete::{parse_marker_body, parse_marker_key, segment_key_for};

/// Spawn the coordinator-wide reaper ticker.
///
/// Cadence is `gc_config.reaper_cadence()` (= `max(retention/10, 1s)`).
/// `retention` is read on each tick so that operator changes to the
/// `pending_delete_retention` config apply immediately to all in-flight
/// markers — see `docs/design-replica-model.md` (*Marker record*) for
/// why we derive rather than stamp.
pub fn start(
    store: Arc<dyn ObjectStore>,
    data_dir: PathBuf,
    cadence: Duration,
    retention: Duration,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(cadence);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            tick_once(&store, &data_dir, retention).await;
        }
    });
}

/// One tick. Discovers owned volumes under `data_dir`, spawns one
/// non-blocking reap operation per volume, returns immediately.
async fn tick_once(store: &Arc<dyn ObjectStore>, data_dir: &Path, retention: Duration) {
    for vol_dir in owned_volumes(data_dir) {
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(e) = reap_volume(&store, &vol_dir, retention).await {
                warn!("[reaper {}] reap failed: {e:#}", vol_dir.display());
            }
        });
    }
}

/// Run one reap pass against a single owned volume.
pub async fn reap_volume(
    store: &Arc<dyn ObjectStore>,
    vol_dir: &Path,
    retention: Duration,
) -> anyhow::Result<()> {
    let vol_ulid = volume_ulid(vol_dir)?;
    reap_volume_inner(store, vol_ulid, retention, SystemTime::now()).await
}

async fn reap_volume_inner(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    retention: Duration,
    now: SystemTime,
) -> anyhow::Result<()> {
    let prefix = StorePath::from(format!("by_id/{vol_ulid}/retention"));
    let listing: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("listing {prefix}: {e}"))?;

    for object in listing {
        let key = object.location.as_ref().to_owned();
        if let Err(e) = process_marker(store, vol_ulid, &key, retention, now).await {
            warn!("[reaper {vol_ulid}] skipping marker {key}: {e}");
        }
    }
    Ok(())
}

/// Process one marker. `Err` means the marker should be left in place
/// (validation failure, parse error, or transient S3 error). Errors are
/// purely advisory — the caller logs and the next tick retries.
async fn process_marker(
    store: &Arc<dyn ObjectStore>,
    vol_ulid: Ulid,
    key: &str,
    retention: Duration,
    now: SystemTime,
) -> anyhow::Result<()> {
    // Checkpoint 2: marker-path-parsed volume must equal the invocation ULID.
    let (path_vol, gc_output_ulid) =
        parse_marker_key(key).map_err(|e| anyhow::anyhow!("parsing marker key: {e}"))?;
    if path_vol != vol_ulid {
        anyhow::bail!("marker path volume {path_vol} does not match invocation {vol_ulid}");
    }

    // Deadline derived from the filename ULID + current retention.
    let deadline = ulid_creation_time(gc_output_ulid)? + retention;
    if now < deadline {
        debug!("[reaper {vol_ulid}] {gc_output_ulid} not yet at deadline; skipping");
        return Ok(());
    }

    // Fetch + parse marker body.
    let store_key = StorePath::from(key.to_owned());
    let body = store
        .get(&store_key)
        .await
        .map_err(|e| anyhow::anyhow!("fetching marker: {e}"))?
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("reading marker body: {e}"))?;
    let text =
        std::str::from_utf8(&body).map_err(|e| anyhow::anyhow!("marker body not utf-8: {e}"))?;
    let inputs =
        parse_marker_body(text).map_err(|e| anyhow::anyhow!("parsing marker body: {e}"))?;

    // Reap. Volume scope is structural — every reconstructed key sits
    // under `vol_ulid`'s prefix because the prefix is built from the
    // trusted invocation argument. A malformed input ULID can produce
    // at worst a 404 DELETE under this volume, never a delete in
    // another volume's prefix.
    for input_ulid in &inputs {
        let target_key = segment_key_for(vol_ulid, *input_ulid);
        match store.delete(&target_key).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => anyhow::bail!("deleting target {target_key}: {e}"),
        }
    }
    match store.delete(&store_key).await {
        Ok(_) => {}
        Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => anyhow::bail!("deleting marker {key}: {e}"),
    }
    info!(
        "[reaper {vol_ulid}] reaped marker {gc_output_ulid} ({} input(s))",
        inputs.len(),
    );
    Ok(())
}

fn ulid_creation_time(u: Ulid) -> anyhow::Result<SystemTime> {
    Ok(UNIX_EPOCH + Duration::from_millis(u.timestamp_ms()))
}

/// Walk `<data_dir>/by_id/` and return paths to volumes the local
/// coordinator owns as a writer — i.e. directories whose name parses as
/// a ULID and that lack a `volume.readonly` marker.
///
/// Read-only references (cheap-reference replicas of upstream volumes)
/// have no S3 write authority; the upstream's own coordinator reaps
/// their prefixes. See *Scope* in `docs/design-replica-model.md`.
fn owned_volumes(data_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let by_id = data_dir.join("by_id");
    let entries = match std::fs::read_dir(&by_id) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return out,
        Err(e) => {
            warn!("[reaper] cannot read {}: {e}", by_id.display());
            return out;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if Ulid::from_string(name).is_err() {
            continue;
        }
        if path.join("volume.readonly").exists() {
            continue;
        }
        out.push(path);
    }
    out
}

fn volume_ulid(vol_dir: &Path) -> anyhow::Result<Ulid> {
    let name = vol_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow::anyhow!("vol dir has no name: {}", vol_dir.display()))?;
    Ulid::from_string(name)
        .map_err(|e| anyhow::anyhow!("vol dir name is not a valid ULID '{name}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pending_delete::{marker_key, render_marker};
    use bytes::Bytes;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    fn vol_id() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    fn ulid_at(ms: u64) -> Ulid {
        Ulid::from_parts(ms, 42)
    }

    async fn put_segment(store: &Arc<dyn ObjectStore>, vol: Ulid, seg: Ulid) -> StorePath {
        let key = segment_key_for(vol, seg);
        store
            .put(&key, PutPayload::from(Bytes::from_static(b"seg-body")))
            .await
            .unwrap();
        key
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn reaps_expired_marker_and_inputs() {
        let store = store();
        let vol = vol_id();
        let input_a = ulid_at(1_700_000_000_000);
        let input_b = ulid_at(1_700_000_000_001);
        let key_a = put_segment(&store, vol, input_a).await;
        let key_b = put_segment(&store, vol, input_b).await;

        let gc_output = ulid_at(1_700_000_000_500);
        let body = render_marker(&[input_a, input_b]);
        let mk = marker_key(vol, gc_output);
        store
            .put(&mk, PutPayload::from(Bytes::from(body)))
            .await
            .unwrap();

        // Now well past gc_output's timestamp + 1s retention
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_010_000);
        reap_volume_inner(&store, vol, Duration::from_secs(1), now)
            .await
            .unwrap();

        for k in [&key_a, &key_b, &mk] {
            assert!(
                matches!(
                    store.head(k).await,
                    Err(object_store::Error::NotFound { .. })
                ),
                "expected {k} reaped",
            );
        }
    }

    #[tokio::test]
    async fn leaves_unexpired_marker_alone() {
        let store = store();
        let vol = vol_id();
        let input_a = ulid_at(1_700_000_000_000);
        let key_a = put_segment(&store, vol, input_a).await;

        let gc_output = ulid_at(1_700_000_000_500);
        let body = render_marker(&[input_a]);
        let mk = marker_key(vol, gc_output);
        store
            .put(&mk, PutPayload::from(Bytes::from(body)))
            .await
            .unwrap();

        // Now is one minute past creation; retention is 1h
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_060_000);
        reap_volume_inner(&store, vol, Duration::from_secs(3600), now)
            .await
            .unwrap();

        assert!(store.head(&key_a).await.is_ok());
        assert!(store.head(&mk).await.is_ok());
    }

    #[tokio::test]
    async fn malformed_marker_does_not_fire_deletes() {
        let store = store();
        let vol = vol_id();
        let input_a = ulid_at(1_700_000_000_000);
        let key_a = put_segment(&store, vol, input_a).await;

        let gc_output = ulid_at(1_700_000_000_500);
        // Garbage line
        let body = format!("{input_a}\nnot-a-ulid\n");
        let mk = marker_key(vol, gc_output);
        store
            .put(&mk, PutPayload::from(Bytes::from(body)))
            .await
            .unwrap();

        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_010_000);
        reap_volume_inner(&store, vol, Duration::from_secs(1), now)
            .await
            .unwrap();

        // Marker rejected — input segment must still exist; marker still in place
        assert!(store.head(&key_a).await.is_ok());
        assert!(store.head(&mk).await.is_ok());
    }

    #[tokio::test]
    async fn changing_retention_takes_effect_immediately() {
        let store = store();
        let vol = vol_id();
        let input_a = ulid_at(1_700_000_000_000);
        let key_a = put_segment(&store, vol, input_a).await;

        let gc_output = ulid_at(1_700_000_000_500);
        let body = render_marker(&[input_a]);
        let mk = marker_key(vol, gc_output);
        store
            .put(&mk, PutPayload::from(Bytes::from(body)))
            .await
            .unwrap();

        // Marker is ~10s old. With T=1h, not expired.
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_010_000);
        reap_volume_inner(&store, vol, Duration::from_secs(3600), now)
            .await
            .unwrap();
        assert!(store.head(&mk).await.is_ok());

        // Same marker, same `now`, but T dropped to 1s — operator change
        // applies immediately under the derive-not-stamp rule.
        reap_volume_inner(&store, vol, Duration::from_secs(1), now)
            .await
            .unwrap();
        assert!(matches!(
            store.head(&key_a).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            store.head(&mk).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}
