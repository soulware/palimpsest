// Coordinator identity: Ed25519 keypair, derived `coordinator_id`,
// derived in-memory macaroon MAC root.
//
// Mirrors the per-volume convention: `<data_dir>/coordinator.key`
// (private, mode 0600) and `<data_dir>/coordinator.pub` (public).
// `coordinator_id` derives from the **public** key, so the binding
// between the id and the published pubkey is intrinsic — readers can
// recompute and verify. The macaroon MAC root derives from the
// **private** key; it is held only in memory and never written to
// disk.
//
// See `docs/design-portable-live-volume.md` § "Coordinator identity"
// and Phase 1.5 of `docs/portable-live-volume-plan.md`.

use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use rand_core::OsRng;
use tracing::info;

use crate::portable;

const KEY_FILE: &str = "coordinator.key";
const PUB_FILE: &str = "coordinator.pub";
/// Sibling file containing the public coordinator id derived from
/// `coordinator.pub` — Crockford-Base32 ULID-shaped, single line.
/// Inspectable with `cat` so operators can correlate `names/<name>`
/// `coordinator_id` entries against this host.
const ID_FILE: &str = "coordinator.id";

const PRIV_LEN: usize = 32;
// Used by `fetch_coordinator_pub` (Phase 3 — claimant-side verification
// of synthesised handoff snapshots). Kept here so the wire-length check
// stays consistent with the publish path.
#[allow(dead_code)]
const PUB_LEN: usize = 32;

/// Domain-separation context for the macaroon MAC root.
const MACAROON_ROOT_CONTEXT: &str = "elide macaroon-root v1";

/// In-memory coordinator identity bundle.
///
/// All material derives from the on-disk `coordinator.key`. The
/// signing key is private to this struct; callers reach the
/// derived id and macaroon root through the accessor methods.
pub struct CoordinatorIdentity {
    signing_key: SigningKey,
    coordinator_id_str: String,
    macaroon_root: [u8; 32],
}

/// Treat the coordinator's identity keypair as a `SegmentSigner` so
/// it can sign synthesised handoff snapshots produced by
/// `volume release --force`. The signing input is identical to any
/// other Ed25519 signature; the key is the coordinator's identity
/// rather than a per-volume key.
impl elide_core::segment::SegmentSigner for CoordinatorIdentity {
    fn sign(&self, msg: &[u8]) -> [u8; 64] {
        use ed25519_dalek::Signer;
        self.signing_key.sign(msg).to_bytes()
    }
}

impl std::fmt::Debug for CoordinatorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorIdentity")
            .field("coordinator_id", &self.coordinator_id_str)
            .field("signing_key", &"<redacted>")
            .field("macaroon_root", &"<redacted>")
            .finish()
    }
}

impl CoordinatorIdentity {
    /// Load `coordinator.key` / `coordinator.pub` from `data_dir`,
    /// generating a fresh Ed25519 keypair on first start.
    ///
    /// On first start writes the keypair files (mode 0600 / 0644)
    /// atomically via create+rename and a sibling `coordinator.id`
    /// breadcrumb. On subsequent starts the on-disk pub is verified
    /// against the loaded private key — a mismatch is a hard error.
    pub fn load_or_generate(data_dir: &Path) -> io::Result<Self> {
        let signing_key = load_or_generate_signing_key(data_dir)?;
        let verifying_key = signing_key.verifying_key();

        load_or_write_pub(data_dir, &verifying_key)?;

        let coord_id = portable::coordinator_id(verifying_key.as_bytes());
        let coordinator_id_str = portable::format_coordinator_id(&coord_id);

        write_id_breadcrumb(data_dir, &coordinator_id_str)?;

        let macaroon_root =
            blake3::derive_key(MACAROON_ROOT_CONTEXT, signing_key.as_bytes().as_slice());

        Ok(Self {
            signing_key,
            coordinator_id_str,
            macaroon_root,
        })
    }

    /// Crockford-Base32 ULID-shaped coordinator id.
    pub fn coordinator_id_str(&self) -> &str {
        &self.coordinator_id_str
    }

    /// 32-byte MAC root for `macaroon::mint` / `macaroon::verify`.
    /// Held only in memory; never persisted.
    pub fn macaroon_root(&self) -> &[u8; 32] {
        &self.macaroon_root
    }

    /// Sign `msg` with the coordinator's Ed25519 signing key.
    /// Used for any artefact attributed to this coordinator's
    /// identity — synthesised handoff snapshots (via the
    /// `SegmentSigner` impl) and per-name event log entries (via
    /// `name_event_store::emit_event`). Domain separation between
    /// these uses is enforced by the *callers' canonical-form
    /// pre-images*, not by separate keys.
    pub fn sign(&self, msg: &[u8]) -> [u8; 64] {
        use ed25519_dalek::Signer;
        self.signing_key.sign(msg).to_bytes()
    }

    /// Public (verifying) half of the keypair.
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Publish `coordinator.pub` to S3 at
    /// `coordinators/<coordinator_id>/coordinator.pub`.
    ///
    /// Idempotent: if a record already exists, it must match the
    /// local pub byte-for-byte (which means the same private key
    /// also derived this id, so this is the same coordinator
    /// restarting). A mismatch is a hard startup error — it would
    /// mean a different keyholder has already claimed this
    /// `coordinator_id`, which is essentially impossible for an
    /// honest 32-byte derivation but worth refusing loudly.
    pub async fn publish_pub(&self, store: &dyn ObjectStore) -> io::Result<()> {
        let pub_bytes = self.verifying_key().to_bytes();
        let key = pub_object_path(&self.coordinator_id_str);

        match store.get(&key).await {
            Ok(get_result) => {
                let body = get_result
                    .bytes()
                    .await
                    .map_err(|e| io::Error::other(format!("read existing coordinator.pub: {e}")))?;
                if body.as_ref() != pub_bytes.as_slice() {
                    return Err(io::Error::other(format!(
                        "coordinators/{}/coordinator.pub already exists with different bytes",
                        self.coordinator_id_str
                    )));
                }
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => {
                portable::put_if_absent(store, &key, Bytes::copy_from_slice(&pub_bytes))
                    .await
                    .map_err(|e| io::Error::other(format!("publish coordinator.pub: {e}")))?;
                info!(
                    "[coordinator] published coordinator.pub at coordinators/{}/coordinator.pub",
                    self.coordinator_id_str
                );
                Ok(())
            }
            Err(e) => Err(io::Error::other(format!("head coordinator.pub: {e}"))),
        }
    }
}

/// Fetch a coordinator's public key from
/// `coordinators/<coord_id>/coordinator.pub` and verify the binding:
/// `coordinator_id` must derive from the fetched bytes.
///
/// Used by claimants on `start --remote` to verify a synthesised
/// handoff snapshot's signature against the recovering coordinator,
/// and by [`crate::name_event_store::list_and_verify_events`] to
/// verify per-name event log signatures.
pub async fn fetch_coordinator_pub(
    store: &dyn ObjectStore,
    coord_id_str: &str,
) -> io::Result<VerifyingKey> {
    let key = pub_object_path(coord_id_str);
    let get_result = store
        .get(&key)
        .await
        .map_err(|e| io::Error::other(format!("fetch coordinator.pub for {coord_id_str}: {e}")))?;
    let body = get_result
        .bytes()
        .await
        .map_err(|e| io::Error::other(format!("read coordinator.pub for {coord_id_str}: {e}")))?;

    if body.len() != PUB_LEN {
        return Err(io::Error::other(format!(
            "coordinator.pub for {coord_id_str} has wrong length (expected {PUB_LEN}, got {})",
            body.len()
        )));
    }
    let mut pub_bytes = [0u8; PUB_LEN];
    pub_bytes.copy_from_slice(&body);

    let derived = portable::format_coordinator_id(&portable::coordinator_id(&pub_bytes));
    if derived != coord_id_str {
        return Err(io::Error::other(format!(
            "coordinator.pub at coordinators/{coord_id_str}/coordinator.pub does not derive to that id (got {derived})"
        )));
    }

    VerifyingKey::from_bytes(&pub_bytes)
        .map_err(|e| io::Error::other(format!("invalid Ed25519 pubkey for {coord_id_str}: {e}")))
}

fn pub_object_path(coord_id_str: &str) -> StorePath {
    StorePath::from(format!("coordinators/{coord_id_str}/coordinator.pub"))
}

fn load_or_generate_signing_key(data_dir: &Path) -> io::Result<SigningKey> {
    let path = data_dir.join(KEY_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            if bytes.len() != PRIV_LEN {
                return Err(io::Error::other(format!(
                    "{} has wrong length (expected {PRIV_LEN}, got {})",
                    path.display(),
                    bytes.len()
                )));
            }
            let mut seed = [0u8; PRIV_LEN];
            seed.copy_from_slice(&bytes);
            Ok(SigningKey::from_bytes(&seed))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let signing_key = SigningKey::generate(&mut OsRng);
            atomic_write(&path, signing_key.as_bytes(), 0o600)?;
            info!("[coordinator] generated signing key at {}", path.display());
            Ok(signing_key)
        }
        Err(e) => Err(e),
    }
}

fn load_or_write_pub(data_dir: &Path, verifying_key: &VerifyingKey) -> io::Result<()> {
    let path = data_dir.join(PUB_FILE);
    let pub_bytes = verifying_key.to_bytes();

    match std::fs::read(&path) {
        Ok(existing) => {
            if existing.as_slice() != pub_bytes.as_slice() {
                return Err(io::Error::other(format!(
                    "{} does not match the loaded private key — keypair files are inconsistent",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            atomic_write(&path, &pub_bytes, 0o644)?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn write_id_breadcrumb(data_dir: &Path, coord_id_str: &str) -> io::Result<()> {
    let path = data_dir.join(ID_FILE);
    if let Ok(existing) = std::fs::read_to_string(&path)
        && existing.trim_end() == coord_id_str
    {
        return Ok(());
    }
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o644)
            .open(&tmp)?;
        f.write_all(coord_id_str.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)
}

fn atomic_write(path: &Path, body: &[u8], mode: u32) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(mode)
            .open(&tmp)?;
        f.write_all(body)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn load_or_generate_creates_keypair_with_expected_perms() {
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();

        let key_path = tmp.path().join(KEY_FILE);
        let pub_path = tmp.path().join(PUB_FILE);
        let id_path = tmp.path().join(ID_FILE);

        assert!(key_path.exists() && pub_path.exists() && id_path.exists());

        let key_mode = std::fs::metadata(&key_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            key_mode, 0o600,
            "private key must be 0600, got {key_mode:o}"
        );

        let pub_mode = std::fs::metadata(&pub_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(pub_mode, 0o644, "public key must be 0644, got {pub_mode:o}");

        // ID breadcrumb is a 26-char Crockford-Base32 ULID.
        let breadcrumb = std::fs::read_to_string(&id_path).unwrap();
        let trimmed = breadcrumb.trim_end();
        assert_eq!(trimmed.len(), 26);
        assert_eq!(trimmed, id.coordinator_id_str());
    }

    #[test]
    fn load_or_generate_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let first = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let second = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        assert_eq!(first.coordinator_id_str(), second.coordinator_id_str());
        assert_eq!(first.verifying_key(), second.verifying_key());
        assert_eq!(first.macaroon_root(), second.macaroon_root());
    }

    #[test]
    fn coordinator_id_self_authenticates_against_published_pub() {
        // The id derives from the pub bytes via the shared helper.
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let pub_bytes = id.verifying_key().to_bytes();
        let derived = portable::format_coordinator_id(&portable::coordinator_id(&pub_bytes));
        assert_eq!(derived, id.coordinator_id_str());
    }

    #[test]
    fn rejects_mismatched_keypair_files_on_disk() {
        let tmp = TempDir::new().unwrap();
        // Seed a private key.
        let seed = [0xABu8; 32];
        std::fs::write(tmp.path().join(KEY_FILE), seed).unwrap();
        // Write a public key that doesn't match.
        let bogus_pub = [0xCDu8; 32];
        std::fs::write(tmp.path().join(PUB_FILE), bogus_pub).unwrap();

        let err = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("does not match"));
    }

    #[test]
    fn rejects_wrong_length_private_key() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join(KEY_FILE), b"too short").unwrap();
        let err = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("wrong length"));
    }

    #[tokio::test]
    async fn publish_pub_first_call_uploads_and_second_call_is_noop() {
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let store = InMemory::new();

        id.publish_pub(&store).await.expect("first publish");
        id.publish_pub(&store)
            .await
            .expect("second publish (no-op)");

        let fetched = fetch_coordinator_pub(&store, id.coordinator_id_str())
            .await
            .expect("fetch");
        assert_eq!(fetched.to_bytes(), id.verifying_key().to_bytes());
    }

    #[tokio::test]
    async fn publish_pub_rejects_mismatched_existing_record() {
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let store = InMemory::new();

        // Pre-seed the bucket path with a *different* pubkey.
        let bogus = [0u8; 32];
        let key = pub_object_path(id.coordinator_id_str());
        store
            .put(&key, Bytes::copy_from_slice(&bogus).into())
            .await
            .expect("seed bogus pub");

        let err = id.publish_pub(&store).await.unwrap_err();
        assert!(err.to_string().contains("different bytes"));
    }

    #[tokio::test]
    async fn fetch_coordinator_pub_rejects_pub_that_doesnt_derive_to_id() {
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let store = InMemory::new();

        // Place a valid Ed25519 pubkey at the path, but the path's id
        // doesn't derive from it. Simulates a tampered or misfiled pub.
        let other = SigningKey::generate(&mut OsRng).verifying_key().to_bytes();
        let key = pub_object_path(id.coordinator_id_str());
        store
            .put(&key, Bytes::copy_from_slice(&other).into())
            .await
            .expect("seed mismatched pub");

        let err = fetch_coordinator_pub(&store, id.coordinator_id_str())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not derive"));
    }

    #[tokio::test]
    async fn fetch_coordinator_pub_rejects_wrong_length_record() {
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let store = InMemory::new();

        let key = pub_object_path(id.coordinator_id_str());
        store
            .put(&key, Bytes::from_static(b"too short").into())
            .await
            .expect("seed short pub");

        let err = fetch_coordinator_pub(&store, id.coordinator_id_str())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("wrong length"));
    }

    #[test]
    fn macaroon_root_is_domain_separated_from_coordinator_id() {
        // Same private key bytes feeding the macaroon-root derivation
        // and the coordinator-id derivation must produce different
        // 32-byte outputs (different inputs *and* different contexts).
        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let coord_id_bytes = portable::coordinator_id(&id.verifying_key().to_bytes());
        assert_ne!(id.macaroon_root(), &coord_id_bytes);
    }

    #[test]
    fn sign_produces_signature_verifiable_against_published_pub() {
        use ed25519_dalek::{Signature, Verifier};

        let tmp = TempDir::new().unwrap();
        let id = CoordinatorIdentity::load_or_generate(tmp.path()).unwrap();
        let msg = b"synthesised handoff snapshot test";
        let sig = Signature::from_bytes(&id.sign(msg));
        id.verifying_key()
            .verify(msg, &sig)
            .expect("sig verifies against own pubkey");
    }
}
