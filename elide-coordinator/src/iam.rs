//! Coordinator-side IAM glue for the per-volume RO key model.
//!
//! Bridges the abstract [`crate::credential::CredentialIssuer`] used by
//! the IPC dispatch path to the [`elide_tigris_iam`] client. Owns the
//! lifecycle of per-volume RO keys and policies and the on-disk
//! `iam.json` inventory file.
//!
//! Initial-pass scope (matches `docs/design-iam-key-model.md` §
//! "Per-volume read-only key"): mint on first issuance, cache the
//! secret in memory, persist non-secret metadata to
//! `<data_dir>/by_id/<vol-ulid>/iam.json`. Writer key, peer-fetch key,
//! and ephemeral fetch keys are not yet wired.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use elide_tigris_iam::{
    CoordinatorWriterPolicy, PerVolumeReadOnlyPolicy, PolicyDocument, TigrisIamClient,
    TigrisIamConfig,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{info, warn};
use ulid::Ulid;

use crate::credential::{CredentialIssuer, Credentialer, IssuedCredentials};

/// Non-secret per-volume IAM inventory persisted to
/// `<data_dir>/by_id/<vol_ulid>/iam.json`. Mirrors the JSON sketch in
/// `docs/design-iam-key-model.md` § "Inventory".
#[derive(Serialize, Deserialize, Clone)]
pub struct PersistedVolumeIam {
    pub ro_access_key_id: String,
    pub policy_name: String,
    pub policy_arn: String,
    /// Unix-seconds the policy's `DateLessThan` condition fires.
    pub policy_expiry_unix: u64,
    pub ancestor_chain: Vec<String>,
}

const IAM_JSON_FILE: &str = "iam.json";

/// In-memory entry: persisted metadata + the secret which is held only
/// for the volume's lifetime.
#[derive(Clone)]
struct CachedKey {
    persisted: PersistedVolumeIam,
    secret_access_key: String,
}

/// Manages per-volume RO keys and policies for one coordinator's set
/// of local volumes. Construct once at coordinator start; share via
/// `Arc`.
pub struct VolumeIamManager {
    client: TigrisIamClient,
    bucket: String,
    coord_id: String,
    data_dir: PathBuf,
    ro_key_lifetime: Duration,
    source_ips: Vec<String>,
    cache: Mutex<HashMap<Ulid, CachedKey>>,
}

impl VolumeIamManager {
    pub fn new(
        config: TigrisIamConfig,
        bucket: String,
        coord_id: String,
        data_dir: PathBuf,
        ro_key_lifetime: Duration,
        source_ips: Vec<String>,
    ) -> io::Result<Self> {
        let client =
            TigrisIamClient::new(config).map_err(|e| io::Error::other(format!("iam: {e}")))?;
        Ok(Self {
            client,
            bucket,
            coord_id,
            data_dir,
            ro_key_lifetime,
            source_ips,
            cache: Mutex::new(HashMap::new()),
        })
    }

    async fn provision_volume_ro_inner(
        &self,
        vol_ulid: Ulid,
        ancestors: &[Ulid],
    ) -> io::Result<IssuedCredentials> {
        let mut cache = self.cache.lock().await;
        if let Some(cached) = cache.get(&vol_ulid) {
            return Ok(IssuedCredentials {
                access_key_id: cached.persisted.ro_access_key_id.clone(),
                secret_access_key: cached.secret_access_key.clone(),
                session_token: None,
                expiry_unix: Some(cached.persisted.policy_expiry_unix),
            });
        }

        let policy_name = format!("elide-{}-{}-ro", self.coord_id, vol_ulid);
        let expiry_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            .saturating_add(self.ro_key_lifetime.as_secs());
        let expiry_iso = format_unix_iso8601(expiry_unix);

        let policy_doc = PerVolumeReadOnlyPolicy {
            bucket: &self.bucket,
            vol_ulid: &vol_ulid,
            ancestor_ulids: ancestors,
            expiry_iso8601: &expiry_iso,
            source_ips: if self.source_ips.is_empty() {
                None
            } else {
                Some(&self.source_ips)
            },
        }
        .build();
        let policy_json = PolicyDocument::to_json(&policy_doc)
            .map_err(|e| io::Error::other(format!("iam policy json: {e}")))?;

        let access_key = self
            .client
            .create_access_key()
            .await
            .map_err(|e| io::Error::other(format!("create_access_key: {e}")))?;

        let policy = match self.client.create_policy(&policy_name, &policy_json).await {
            Ok(p) => p,
            Err(e) => {
                // Roll back the orphan access key so the operator
                // doesn't accumulate phantom keys after a transient
                // policy-creation error.
                if let Err(del_err) = self
                    .client
                    .delete_access_key(&access_key.access_key_id)
                    .await
                {
                    warn!("[iam] rollback delete_access_key after create_policy fail: {del_err}");
                }
                return Err(io::Error::other(format!("create_policy: {e}")));
            }
        };

        if let Err(e) = self
            .client
            .attach_user_policy(&access_key.access_key_id, &policy.policy_arn)
            .await
        {
            // Roll back both the policy and the access key.
            if let Err(del_err) = self.client.delete_policy(&policy.policy_arn).await {
                warn!("[iam] rollback delete_policy after attach fail: {del_err}");
            }
            if let Err(del_err) = self
                .client
                .delete_access_key(&access_key.access_key_id)
                .await
            {
                warn!("[iam] rollback delete_access_key after attach fail: {del_err}");
            }
            return Err(io::Error::other(format!("attach_user_policy: {e}")));
        }

        let persisted = PersistedVolumeIam {
            ro_access_key_id: access_key.access_key_id.clone(),
            policy_name: policy.policy_name.clone(),
            policy_arn: policy.policy_arn.clone(),
            policy_expiry_unix: expiry_unix,
            ancestor_chain: ancestors.iter().map(|u| u.to_string()).collect(),
        };
        write_iam_json(&self.data_dir, &vol_ulid, &persisted)?;
        info!(
            target: "iam::provision",
            vol_ulid = %vol_ulid,
            ro_access_key_id = %access_key.access_key_id,
            policy_arn = %policy.policy_arn,
            expiry_unix,
            "minted per-volume RO key",
        );

        let creds = IssuedCredentials {
            access_key_id: access_key.access_key_id,
            secret_access_key: access_key.secret_access_key.clone(),
            session_token: None,
            expiry_unix: Some(expiry_unix),
        };
        cache.insert(
            vol_ulid,
            CachedKey {
                persisted,
                secret_access_key: access_key.secret_access_key,
            },
        );
        Ok(creds)
    }

    async fn release_volume_ro_inner(&self, vol_ulid: Ulid) {
        let persisted = match read_iam_json(&self.data_dir, &vol_ulid) {
            Ok(Some(p)) => p,
            Ok(None) => return,
            Err(e) => {
                warn!(
                    "[iam] release {vol_ulid}: read iam.json failed: {e}; \
                     skipping IAM cleanup"
                );
                return;
            }
        };
        if let Err(e) = self
            .client
            .detach_user_policy(&persisted.ro_access_key_id, &persisted.policy_arn)
            .await
        {
            warn!("[iam] release {vol_ulid}: detach_user_policy: {e}");
        }
        if let Err(e) = self.client.delete_policy(&persisted.policy_arn).await {
            warn!("[iam] release {vol_ulid}: delete_policy: {e}");
        }
        if let Err(e) = self
            .client
            .delete_access_key(&persisted.ro_access_key_id)
            .await
        {
            warn!("[iam] release {vol_ulid}: delete_access_key: {e}");
        }
        let _ = remove_iam_json(&self.data_dir, &vol_ulid);
        let _ = self.cache.lock().await.remove(&vol_ulid);
    }
}

#[async_trait]
impl Credentialer for VolumeIamManager {
    async fn provision_volume_ro(
        &self,
        vol_ulid: Ulid,
        ancestors: &[Ulid],
    ) -> io::Result<IssuedCredentials> {
        self.provision_volume_ro_inner(vol_ulid, ancestors).await
    }

    async fn release_volume_ro(&self, vol_ulid: Ulid) {
        self.release_volume_ro_inner(vol_ulid).await
    }
}

/// `CredentialIssuer` impl that vends per-volume RO credentials by
/// delegating to a [`Credentialer`]. The issuer loads the volume's
/// ancestor chain from local provenance and passes it to the
/// credentialer; the credentialer itself only sees ULIDs and never
/// touches local state, so the same impl works whether the
/// credentialer is in-process, a sidecar over UDS, or a remote
/// service.
pub struct IamCredentialIssuer {
    credentialer: Arc<dyn Credentialer>,
    data_dir: PathBuf,
}

impl IamCredentialIssuer {
    pub fn new(credentialer: Arc<dyn Credentialer>, data_dir: PathBuf) -> Self {
        Self {
            credentialer,
            data_dir,
        }
    }
}

#[async_trait]
impl CredentialIssuer for IamCredentialIssuer {
    async fn issue(&self, volume_id: &str) -> io::Result<IssuedCredentials> {
        let vol_ulid = Ulid::from_string(volume_id)
            .map_err(|e| io::Error::other(format!("invalid vol_ulid: {e}")))?;
        let by_id_dir = self.data_dir.join("by_id");
        let fork_dir = by_id_dir.join(vol_ulid.to_string());
        let ancestors = elide_core::volume::lineage_ulids(&fork_dir, &by_id_dir)
            .map_err(|e| io::Error::other(format!("loading ancestor chain: {e}")))?;
        self.credentialer
            .provision_volume_ro(vol_ulid, &ancestors)
            .await
    }
}

/// Non-secret writer-key inventory persisted to
/// `<data_dir>/coordinator/iam.json`. Mirrors the inventory shape in
/// `docs/design-iam-key-model.md` § "Writer key metadata" — the writer
/// secret is held in memory only and re-minted on each coordinator
/// start.
#[derive(Serialize, Deserialize, Clone)]
pub struct PersistedWriterIam {
    pub writer_access_key_id: String,
    pub policy_name: String,
    pub policy_arn: String,
}

const WRITER_IAM_DIR: &str = "coordinator";
const WRITER_IAM_FILE: &str = "iam.json";

/// One-per-coordinator writer-key lifecycle.
///
/// `ensure_writer_key` returns a usable writer triple, doing one of:
///
/// * **First start** (`coordinator/iam.json` absent): mint policy +
///   key + attach via the admin client; persist metadata; return creds.
/// * **Restart** (`coordinator/iam.json` present): the writer secret
///   was never persisted (matches the design's RO-key convention) — so
///   we rotate: mint a fresh access key, attach it to the existing
///   policy, delete the old key, rewrite metadata. The policy ARN
///   stays stable across restarts.
///
/// Rollback on every step mirrors `VolumeIamManager::provision_volume_ro_inner`.
pub struct WriterKeyManager {
    client: TigrisIamClient,
    bucket: String,
    coord_id: String,
    data_dir: PathBuf,
}

impl WriterKeyManager {
    pub fn new(
        config: TigrisIamConfig,
        bucket: String,
        coord_id: String,
        data_dir: PathBuf,
    ) -> io::Result<Self> {
        let client =
            TigrisIamClient::new(config).map_err(|e| io::Error::other(format!("iam: {e}")))?;
        Ok(Self {
            client,
            bucket,
            coord_id,
            data_dir,
        })
    }

    pub async fn ensure_writer_key(&self) -> io::Result<IssuedCredentials> {
        match read_writer_iam(&self.data_dir)? {
            Some(existing) => self.rotate_writer_key(existing).await,
            None => self.bootstrap_writer_key().await,
        }
    }

    async fn bootstrap_writer_key(&self) -> io::Result<IssuedCredentials> {
        let policy_name = format!("elide-{}-writer", self.coord_id);
        let policy_doc = CoordinatorWriterPolicy {
            bucket: &self.bucket,
        }
        .build();
        let policy_json = PolicyDocument::to_json(&policy_doc)
            .map_err(|e| io::Error::other(format!("iam policy json: {e}")))?;

        let access_key = self
            .client
            .create_access_key()
            .await
            .map_err(|e| io::Error::other(format!("create_access_key (writer): {e}")))?;

        let policy = match self.client.create_policy(&policy_name, &policy_json).await {
            Ok(p) => p,
            Err(e) => {
                if let Err(del_err) = self
                    .client
                    .delete_access_key(&access_key.access_key_id)
                    .await
                {
                    warn!(
                        "[iam] rollback delete_access_key after create_policy (writer) fail: {del_err}"
                    );
                }
                return Err(io::Error::other(format!("create_policy (writer): {e}")));
            }
        };

        if let Err(e) = self
            .client
            .attach_user_policy(&access_key.access_key_id, &policy.policy_arn)
            .await
        {
            if let Err(del_err) = self.client.delete_policy(&policy.policy_arn).await {
                warn!("[iam] rollback delete_policy after attach (writer) fail: {del_err}");
            }
            if let Err(del_err) = self
                .client
                .delete_access_key(&access_key.access_key_id)
                .await
            {
                warn!("[iam] rollback delete_access_key after attach (writer) fail: {del_err}");
            }
            return Err(io::Error::other(format!(
                "attach_user_policy (writer): {e}"
            )));
        }

        let persisted = PersistedWriterIam {
            writer_access_key_id: access_key.access_key_id.clone(),
            policy_name: policy.policy_name.clone(),
            policy_arn: policy.policy_arn.clone(),
        };
        write_writer_iam(&self.data_dir, &persisted)?;
        info!(
            target: "iam::writer",
            writer_access_key_id = %access_key.access_key_id,
            policy_arn = %policy.policy_arn,
            "minted coordinator writer key (first start)",
        );

        Ok(IssuedCredentials {
            access_key_id: access_key.access_key_id,
            secret_access_key: access_key.secret_access_key,
            session_token: None,
            expiry_unix: None,
        })
    }

    async fn rotate_writer_key(
        &self,
        existing: PersistedWriterIam,
    ) -> io::Result<IssuedCredentials> {
        let new_key = self
            .client
            .create_access_key()
            .await
            .map_err(|e| io::Error::other(format!("create_access_key (writer rotate): {e}")))?;

        if let Err(e) = self
            .client
            .attach_user_policy(&new_key.access_key_id, &existing.policy_arn)
            .await
        {
            if let Err(del_err) = self.client.delete_access_key(&new_key.access_key_id).await {
                warn!("[iam] rollback delete_access_key after rotate-attach fail: {del_err}");
            }
            return Err(io::Error::other(format!(
                "attach_user_policy (writer rotate): {e}"
            )));
        }

        if let Err(e) = self
            .client
            .delete_access_key(&existing.writer_access_key_id)
            .await
        {
            warn!(
                "[iam] rotate: delete old writer access key {} failed: {e}; \
                 new key is live, old key may linger in IAM until reconciled",
                existing.writer_access_key_id
            );
        }

        let persisted = PersistedWriterIam {
            writer_access_key_id: new_key.access_key_id.clone(),
            policy_name: existing.policy_name.clone(),
            policy_arn: existing.policy_arn.clone(),
        };
        write_writer_iam(&self.data_dir, &persisted)?;
        info!(
            target: "iam::writer",
            writer_access_key_id = %new_key.access_key_id,
            policy_arn = %existing.policy_arn,
            "rotated coordinator writer key on restart",
        );

        Ok(IssuedCredentials {
            access_key_id: new_key.access_key_id,
            secret_access_key: new_key.secret_access_key,
            session_token: None,
            expiry_unix: None,
        })
    }
}

fn writer_iam_path(data_dir: &Path) -> PathBuf {
    data_dir.join(WRITER_IAM_DIR).join(WRITER_IAM_FILE)
}

fn write_writer_iam(data_dir: &Path, value: &PersistedWriterIam) -> io::Result<()> {
    let path = writer_iam_path(data_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(value)
        .map_err(|e| io::Error::other(format!("writer iam.json: {e}")))?;
    std::fs::write(&path, body)
}

fn read_writer_iam(data_dir: &Path) -> io::Result<Option<PersistedWriterIam>> {
    let path = writer_iam_path(data_dir);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let value: PersistedWriterIam = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::other(format!("writer iam.json: {e}")))?;
            Ok(Some(value))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn iam_json_path(data_dir: &Path, vol_ulid: &Ulid) -> PathBuf {
    data_dir
        .join("by_id")
        .join(vol_ulid.to_string())
        .join(IAM_JSON_FILE)
}

fn write_iam_json(data_dir: &Path, vol_ulid: &Ulid, value: &PersistedVolumeIam) -> io::Result<()> {
    let path = iam_json_path(data_dir, vol_ulid);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body =
        serde_json::to_vec_pretty(value).map_err(|e| io::Error::other(format!("iam.json: {e}")))?;
    std::fs::write(&path, body)
}

fn read_iam_json(data_dir: &Path, vol_ulid: &Ulid) -> io::Result<Option<PersistedVolumeIam>> {
    let path = iam_json_path(data_dir, vol_ulid);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let value: PersistedVolumeIam = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::other(format!("iam.json: {e}")))?;
            Ok(Some(value))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn remove_iam_json(data_dir: &Path, vol_ulid: &Ulid) -> io::Result<()> {
    let path = iam_json_path(data_dir, vol_ulid);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Minimal RFC-3339 / ISO-8601 formatter for unix seconds, sufficient
/// for the `aws:CurrentTime` policy condition. We avoid pulling in
/// `chrono` features just for this — IAM accepts the basic
/// `YYYY-MM-DDTHH:MM:SSZ` shape.
fn format_unix_iso8601(unix_seconds: u64) -> String {
    // Days-from-civil algorithm (Hinnant). 86_400 seconds per day.
    let secs_in_day = 86_400u64;
    let days = (unix_seconds / secs_in_day) as i64;
    let remainder = unix_seconds % secs_in_day;
    let hour = remainder / 3600;
    let minute = (remainder % 3600) / 60;
    let second = remainder % 60;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn iam_json_round_trips() {
        let tmp = TempDir::new().unwrap();
        let vol = Ulid::new();
        let p = PersistedVolumeIam {
            ro_access_key_id: "tid_abc".into(),
            policy_name: "elide-COORD-VOL-ro".into(),
            policy_arn: "arn:aws:iam::tid:policy/elide-COORD-VOL-ro".into(),
            policy_expiry_unix: 1_900_000_000,
            ancestor_chain: vec!["ANC1".into(), "ANC2".into()],
        };
        write_iam_json(tmp.path(), &vol, &p).unwrap();
        let read = read_iam_json(tmp.path(), &vol).unwrap().unwrap();
        assert_eq!(read.ro_access_key_id, p.ro_access_key_id);
        assert_eq!(read.policy_arn, p.policy_arn);
        assert_eq!(read.policy_expiry_unix, p.policy_expiry_unix);
        assert_eq!(read.ancestor_chain, p.ancestor_chain);
    }

    #[test]
    fn writer_iam_round_trips() {
        let tmp = TempDir::new().unwrap();
        let p = PersistedWriterIam {
            writer_access_key_id: "tid_writer".into(),
            policy_name: "elide-COORD-writer".into(),
            policy_arn: "arn:aws:iam::tid:policy/elide-COORD-writer".into(),
        };
        write_writer_iam(tmp.path(), &p).unwrap();
        let read = read_writer_iam(tmp.path()).unwrap().unwrap();
        assert_eq!(read.writer_access_key_id, p.writer_access_key_id);
        assert_eq!(read.policy_arn, p.policy_arn);
    }

    #[test]
    fn writer_iam_read_returns_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        assert!(read_writer_iam(tmp.path()).unwrap().is_none());
    }

    #[test]
    fn iam_json_read_returns_none_when_missing() {
        let tmp = TempDir::new().unwrap();
        let vol = Ulid::new();
        let r = read_iam_json(tmp.path(), &vol).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn format_unix_iso8601_known_values() {
        // 1970-01-01T00:00:00Z
        assert_eq!(format_unix_iso8601(0), "1970-01-01T00:00:00Z");
        // 2026-05-10T00:00:00Z = 20583 days since epoch
        assert_eq!(format_unix_iso8601(1_778_371_200), "2026-05-10T00:00:00Z");
        // 2030-01-01T00:00:00Z = 1893456000
        assert_eq!(format_unix_iso8601(1_893_456_000), "2030-01-01T00:00:00Z");
        // Non-midnight value: 13:45:09 on the same day
        assert_eq!(
            format_unix_iso8601(1_778_371_200 + 13 * 3600 + 45 * 60 + 9),
            "2026-05-10T13:45:09Z"
        );
    }

    /// `Credentialer` stub that records every `provision_volume_ro` call
    /// so `IamCredentialIssuer` tests can assert what (vol_ulid,
    /// ancestors) tuple the issuer derived from local provenance.
    #[derive(Default)]
    struct RecordingCredentialer {
        calls: std::sync::Mutex<Vec<(Ulid, Vec<Ulid>)>>,
    }

    #[async_trait]
    impl Credentialer for RecordingCredentialer {
        async fn provision_volume_ro(
            &self,
            vol_ulid: Ulid,
            ancestors: &[Ulid],
        ) -> io::Result<IssuedCredentials> {
            self.calls
                .lock()
                .unwrap()
                .push((vol_ulid, ancestors.to_vec()));
            Ok(IssuedCredentials {
                access_key_id: "stub".into(),
                secret_access_key: "stub".into(),
                session_token: None,
                expiry_unix: Some(0),
            })
        }

        async fn release_volume_ro(&self, _vol_ulid: Ulid) {}
    }

    #[tokio::test]
    async fn issuer_root_volume_passes_empty_ancestors() {
        // No volume.provenance on disk → load_lineage_or_empty returns
        // a default lineage → walk_ancestors and walk_extent_ancestors
        // both return empty → IamCredentialIssuer must call the
        // credentialer with an empty ancestor slice.
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let vol_ulid = Ulid::new();
        let vol_dir = by_id.join(vol_ulid.to_string());
        std::fs::create_dir_all(&vol_dir).unwrap();

        let recorder = Arc::new(RecordingCredentialer::default());
        let issuer = IamCredentialIssuer::new(recorder.clone(), tmp.path().to_path_buf());

        issuer.issue(&vol_ulid.to_string()).await.unwrap();

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, vol_ulid);
        assert!(
            calls[0].1.is_empty(),
            "root volume must yield no ancestors; got {:?}",
            calls[0].1
        );
    }

    #[tokio::test]
    async fn issuer_forked_volume_passes_parent_ulid() {
        // Child's signed provenance lists `parent_ulid` as fork parent.
        // IamCredentialIssuer must load the chain and pass [parent_ulid]
        // to the credentialer — this is the regression guard for the
        // empty-ancestor bug fixed earlier in the IAM branch.
        let tmp = TempDir::new().unwrap();
        let by_id = tmp.path().join("by_id");
        let parent_ulid = Ulid::new();
        let child_ulid = Ulid::new();
        let snapshot_ulid = Ulid::new();
        let child_dir = by_id.join(child_ulid.to_string());
        std::fs::create_dir_all(&child_dir).unwrap();

        let lineage = elide_core::signing::ProvenanceLineage {
            parent: Some(elide_core::signing::ParentRef {
                volume_ulid: parent_ulid.to_string(),
                snapshot_ulid: snapshot_ulid.to_string(),
                pubkey: [0u8; 32],
                manifest_pubkey: None,
            }),
            extent_index: vec![],
            oci_source: None,
        };
        elide_core::signing::setup_readonly_identity(
            &child_dir,
            elide_core::signing::VOLUME_PUB_FILE,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
            &lineage,
        )
        .unwrap();

        let recorder = Arc::new(RecordingCredentialer::default());
        let issuer = IamCredentialIssuer::new(recorder.clone(), tmp.path().to_path_buf());

        issuer.issue(&child_ulid.to_string()).await.unwrap();

        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, child_ulid);
        assert_eq!(calls[0].1, vec![parent_ulid]);
    }
}
