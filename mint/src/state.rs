//! Persisted mint state: the current bootstrap nonce and the transient
//! pending-enrollment table (`docs/design-mint.md` § *Enrollment*).
//!
//! On-disk layout — a directory of files so the lifecycle is
//! inspectable with `ls`, never a hidden binary blob:
//!
//! ```text
//! <data_dir>/
//!   root_key               32-byte macaroon root key (hex), mode 0600
//!   bootstrap              current random nonce (hex), mode 0600
//!   pending/<sub>.json     {pub, bootstrap, first_seen, peer_ip}
//!   approved/<sub>         empty marker; mtime = approval time
//! ```
//!
//! The root key is the symmetric secret mint both mints and verifies
//! macaroons with (the "root key" of the Macaroons paper) — generated
//! on first start, never leaving the process, mirroring the elide
//! coordinator's `coordinator.key`. The bootstrap nonce shares its
//! custody. The pending
//! table is **transient, not a registry**: a record is *not* consumed
//! at exchange (the credential ticket is multi-use until its `exp`, so
//! one approval backs one credential per role), so its lifetime is the
//! ticket's — `gc` reclaims it, approved or not, on a bound ≥ that
//! `exp`. Keyed by `sub`; a second request for the same `sub` with a
//! different `pub` is a conflict that never auto-resolves and never
//! overwrites.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};

/// One pending-enrollment record (`pending/<sub>.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    /// The self-asserted `cnf` value (`ed25519:<b64 pub>`).
    pub pubkey: String,
    /// The bootstrap nonce this enrollment was opened under; rotation
    /// drops records whose nonce is no longer current.
    pub bootstrap: String,
    /// First-seen unix seconds (kept stable across idempotent retries).
    pub first_seen: u64,
    /// Peer IP at first sight, for the operator's out-of-band check.
    pub peer_ip: String,
}

/// What `record_pending` did.
#[derive(Debug, PartialEq, Eq)]
pub enum Recorded {
    /// New record written.
    Created,
    /// Identical `(sub, pub)` already pending — idempotent retry.
    Idempotent,
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("malformed sub")]
    BadSub,
    /// A different `pub` is already pending for this `sub` — never
    /// overwritten, never auto-resolved (operator must intervene).
    #[error("sub already pending with a different key")]
    Conflict,
    #[error("corrupt pending record")]
    Corrupt,
}

/// One row of `mint enroll list`.
#[derive(Debug, Clone)]
pub struct PendingView {
    pub sub: String,
    pub pubkey: String,
    /// Short, stable fingerprint of the bound key for the operator's
    /// side-channel comparison (the client prints the same).
    pub fingerprint: String,
    pub peer_ip: String,
    pub age_seconds: u64,
    pub approved: bool,
    /// This `pub` is also pending under a *different* `sub` — anomalous
    /// (a new key is a new principal); surfaced, not auto-rejected.
    pub anomalous_pub: bool,
}

/// `sub` becomes a path segment, so it must be a safe, inspectable
/// token. Opaque but constrained: ULIDs and the like pass; anything
/// with a separator or control char is rejected at the boundary.
fn safe_sub(sub: &str) -> bool {
    !sub.is_empty()
        && sub.len() <= 256
        && sub
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        && sub != "."
        && sub != ".."
}

/// Stable short fingerprint of a `cnf` pubkey value, for the operator's
/// out-of-band comparison. BLAKE3 of the raw value, first 8 bytes hex —
/// the client computes the identical string from its own key.
pub fn fingerprint(pubkey_value: &str) -> String {
    let h = blake3::hash(pubkey_value.as_bytes());
    h.as_bytes()[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn write_0600(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    fs::rename(&tmp, path)
}

/// Load the macaroon root key from `path` (64 hex chars → 32 bytes),
/// generating a fresh CSPRNG one (hex, mode 0600) on first start. Hex
/// so the secret is a single ASCII line — backup/transport friendly
/// (an operator who loses `data_dir` loses every outstanding macaroon).
/// The root is symmetric, so there is no public half.
fn load_or_generate_root_key(path: &Path) -> io::Result<[u8; 32]> {
    match fs::read_to_string(path) {
        Ok(text) => decode_root_key(text.trim())
            .ok_or_else(|| io::Error::other(format!("{}: not 64 hex chars", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut key = [0u8; 32];
            OsRng.fill_bytes(&mut key);
            write_0600(path, encode_root_key(&key).as_bytes())?;
            Ok(key)
        }
        Err(e) => Err(e),
    }
}

fn encode_root_key(key: &[u8; 32]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_root_key(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Filesystem-backed state. Mutating operations are serialised by a
/// process-local mutex — enrollment is rare and correctness beats
/// throughput; the lock never spans an `.await`.
pub struct Store {
    dir: PathBuf,
    /// The macaroon root, loaded (or generated on first start) from
    /// `<dir>/root_key`. Symmetric: mint both mints and verifies with
    /// it. Copied out via [`Store::root_key`]; never logged.
    root_key: [u8; 32],
    guard: Mutex<()>,
}

impl Store {
    /// Open (creating the layout if absent). Loads the persisted
    /// bootstrap nonce, minting and persisting one on first start.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Store> {
        let dir = dir.into();
        fs::create_dir_all(dir.join("pending"))?;
        fs::create_dir_all(dir.join("approved"))?;
        let root_key = load_or_generate_root_key(&dir.join("root_key"))?;
        let store = Store {
            dir,
            root_key,
            guard: Mutex::new(()),
        };
        if !store.bootstrap_path().exists() {
            let _g = store
                .guard
                .lock()
                .map_err(|_| io::Error::other("poisoned"))?;
            write_0600(&store.bootstrap_path(), fresh_nonce().as_bytes())?;
        }
        Ok(store)
    }

    /// The macaroon root key. Symmetric — used to both mint and verify.
    pub fn root_key(&self) -> [u8; 32] {
        self.root_key
    }

    fn bootstrap_path(&self) -> PathBuf {
        self.dir.join("bootstrap")
    }
    fn pending_path(&self, sub: &str) -> PathBuf {
        self.dir.join("pending").join(format!("{sub}.json"))
    }
    fn approved_path(&self, sub: &str) -> PathBuf {
        self.dir.join("approved").join(sub)
    }

    /// The current bootstrap nonce — the value a presented bootstrap
    /// macaroon's `bootstrap` caveat must equal.
    pub fn current_bootstrap(&self) -> io::Result<String> {
        Ok(fs::read_to_string(self.bootstrap_path())?
            .trim()
            .to_string())
    }

    /// Draw and persist a new bootstrap nonce, then drop every pending
    /// record not opened under it (rotation cancels in-flight
    /// enrollments; outstanding primaries are unaffected).
    pub fn rotate_bootstrap(&self) -> io::Result<String> {
        let _g = self
            .guard
            .lock()
            .map_err(|_| io::Error::other("poisoned"))?;
        let nonce = fresh_nonce();
        write_0600(&self.bootstrap_path(), nonce.as_bytes())?;
        for sub in self.pending_subs()? {
            if let Ok(p) = self.read_pending(&sub)
                && p.bootstrap != nonce
            {
                let _ = fs::remove_file(self.pending_path(&sub));
                let _ = fs::remove_file(self.approved_path(&sub));
            }
        }
        Ok(nonce)
    }

    fn read_pending(&self, sub: &str) -> Result<Pending, StateError> {
        let bytes = fs::read(self.pending_path(sub))?;
        serde_json::from_slice(&bytes).map_err(|_| StateError::Corrupt)
    }

    fn pending_subs(&self) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.dir.join("pending"))? {
            let name = entry?.file_name();
            if let Some(s) = name.to_str().and_then(|n| n.strip_suffix(".json")) {
                out.push(s.to_string());
            }
        }
        Ok(out)
    }

    /// Record (or idempotently re-confirm) a pending enrollment.
    /// Same `(sub, pub)` is idempotent; a different `pub` for an
    /// existing `sub` is a [`StateError::Conflict`] — never overwritten.
    pub fn record_pending(
        &self,
        sub: &str,
        pubkey: &str,
        bootstrap: &str,
        peer_ip: &str,
        now_unix: u64,
    ) -> Result<Recorded, StateError> {
        if !safe_sub(sub) {
            return Err(StateError::BadSub);
        }
        let _g = self
            .guard
            .lock()
            .map_err(|_| StateError::Io(io::Error::other("poisoned")))?;
        if self.pending_path(sub).exists() {
            let existing = self.read_pending(sub)?;
            if existing.pubkey == pubkey {
                return Ok(Recorded::Idempotent);
            }
            return Err(StateError::Conflict);
        }
        let rec = Pending {
            pubkey: pubkey.to_string(),
            bootstrap: bootstrap.to_string(),
            first_seen: now_unix,
            peer_ip: peer_ip.to_string(),
        };
        let bytes = serde_json::to_vec(&rec).map_err(|_| StateError::Corrupt)?;
        write_0600(&self.pending_path(sub), &bytes)?;
        Ok(Recorded::Created)
    }

    /// Operator approval — marks an existing pending `sub` approved.
    /// `false` if there is no pending record for `sub`.
    pub fn approve(&self, sub: &str) -> Result<bool, StateError> {
        if !safe_sub(sub) {
            return Err(StateError::BadSub);
        }
        let _g = self
            .guard
            .lock()
            .map_err(|_| StateError::Io(io::Error::other("poisoned")))?;
        if !self.pending_path(sub).exists() {
            return Ok(false);
        }
        write_0600(&self.approved_path(sub), b"")?;
        Ok(true)
    }

    /// Whether `sub` has an approved pending record awaiting exchange.
    pub fn is_approved(&self, sub: &str) -> bool {
        safe_sub(sub) && self.approved_path(sub).exists() && self.pending_path(sub).exists()
    }

    /// The pending record for `sub`, if any (used at exchange to read
    /// back the bound `pub`).
    pub fn get_pending(&self, sub: &str) -> Result<Option<Pending>, StateError> {
        if !safe_sub(sub) {
            return Err(StateError::BadSub);
        }
        if !self.pending_path(sub).exists() {
            return Ok(None);
        }
        self.read_pending(sub).map(Some)
    }

    /// Drop records older than `max_age_seconds` by `first_seen`,
    /// **approved or not**. The credential ticket is multi-use until
    /// its `exp` (one approval → one credential per role), so a record
    /// is not consumed at exchange; its lifetime *is* the ticket's, and
    /// this is the only thing that reclaims it. The bound is ≥ the
    /// ticket `exp`, keeping the table transient (bounded by `exp`)
    /// rather than a registry. Both files are unlinked.
    pub fn gc(&self, now_unix: u64, max_age_seconds: u64) -> io::Result<usize> {
        let _g = self
            .guard
            .lock()
            .map_err(|_| io::Error::other("poisoned"))?;
        let mut dropped = 0;
        for sub in self.pending_subs()? {
            if let Ok(p) = self.read_pending(&sub)
                && now_unix.saturating_sub(p.first_seen) > max_age_seconds
            {
                let _ = fs::remove_file(self.approved_path(&sub));
                fs::remove_file(self.pending_path(&sub))?;
                dropped += 1;
            }
        }
        Ok(dropped)
    }

    /// All pending records, for `mint enroll list`.
    pub fn list(&self, now_unix: u64) -> io::Result<Vec<PendingView>> {
        let subs = self.pending_subs()?;
        let mut rows: Vec<(String, Pending)> = Vec::new();
        for sub in subs {
            if let Ok(p) = self.read_pending(&sub) {
                rows.push((sub, p));
            }
        }
        let mut out = Vec::with_capacity(rows.len());
        for (sub, p) in &rows {
            let anomalous_pub = rows.iter().any(|(s, q)| s != sub && q.pubkey == p.pubkey);
            out.push(PendingView {
                sub: sub.clone(),
                pubkey: p.pubkey.clone(),
                fingerprint: fingerprint(&p.pubkey),
                peer_ip: p.peer_ip.clone(),
                age_seconds: now_unix.saturating_sub(p.first_seen),
                approved: self.approved_path(sub).exists(),
                anomalous_pub,
            });
        }
        out.sort_by(|a, b| a.sub.cmp(&b.sub));
        Ok(out)
    }
}

fn fresh_nonce() -> String {
    let mut raw = [0u8; 32];
    OsRng.fill_bytes(&mut raw);
    BASE64.encode(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let d = tempfile::tempdir().expect("tempdir");
        let s = Store::open(d.path()).expect("open");
        (d, s)
    }

    const PUBA: &str = "ed25519:AAAA";
    const PUBB: &str = "ed25519:BBBB";

    #[test]
    fn bootstrap_persists_and_is_stable_across_open() {
        let d = tempfile::tempdir().unwrap();
        let n1 = Store::open(d.path()).unwrap().current_bootstrap().unwrap();
        let n2 = Store::open(d.path()).unwrap().current_bootstrap().unwrap();
        assert_eq!(n1, n2, "restart preserves the nonce");
        assert!(!n1.is_empty());
    }

    #[test]
    fn root_key_generated_once_and_stable_across_open() {
        let d = tempfile::tempdir().unwrap();
        let r1 = Store::open(d.path()).unwrap().root_key();
        let r2 = Store::open(d.path()).unwrap().root_key();
        assert_eq!(r1, r2, "restart preserves the key");
        assert_ne!(r1, [0u8; 32], "key is random, not zero");
        let f = d.path().join("root_key");
        assert_eq!(
            std::fs::metadata(&f).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let text = std::fs::read_to_string(&f).unwrap();
        assert_eq!(text.trim().len(), 64, "stored as 64 hex chars");
    }

    #[test]
    fn root_key_seeded_file_is_loaded() {
        let d = tempfile::tempdir().unwrap();
        let hex: String = [7u8; 32].iter().map(|b| format!("{b:02x}")).collect();
        std::fs::write(d.path().join("root_key"), hex).unwrap();
        assert_eq!(Store::open(d.path()).unwrap().root_key(), [7u8; 32]);
    }

    #[test]
    fn root_key_bad_format_is_an_error() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join("root_key"), b"not hex").unwrap();
        assert!(Store::open(d.path()).is_err());
    }

    #[test]
    fn rotate_changes_nonce_and_drops_noncurrent_pending() {
        let (_d, s) = store();
        let old = s.current_bootstrap().unwrap();
        s.record_pending("01ARZ", PUBA, &old, "1.2.3.4", 100)
            .unwrap();
        let new = s.rotate_bootstrap().unwrap();
        assert_ne!(old, new);
        assert!(
            s.get_pending("01ARZ").unwrap().is_none(),
            "stale pending dropped"
        );
    }

    #[test]
    fn record_is_idempotent_for_same_pub_and_conflicts_on_different() {
        let (_d, s) = store();
        let b = s.current_bootstrap().unwrap();
        assert_eq!(
            s.record_pending("01ARZ", PUBA, &b, "ip", 1).unwrap(),
            Recorded::Created
        );
        assert_eq!(
            s.record_pending("01ARZ", PUBA, &b, "ip2", 9).unwrap(),
            Recorded::Idempotent
        );
        // first_seen is not reset by the idempotent retry.
        assert_eq!(s.get_pending("01ARZ").unwrap().unwrap().first_seen, 1);
        assert!(matches!(
            s.record_pending("01ARZ", PUBB, &b, "ip", 1),
            Err(StateError::Conflict)
        ));
    }

    #[test]
    fn approve_requires_existing_pending_and_persists() {
        let (_d, s) = store();
        let b = s.current_bootstrap().unwrap();
        assert!(!s.approve("01ARZ").unwrap(), "no pending → not approved");
        s.record_pending("01ARZ", PUBA, &b, "ip", 1).unwrap();
        assert!(!s.is_approved("01ARZ"));
        assert!(s.approve("01ARZ").unwrap());
        assert!(s.is_approved("01ARZ"));
        // The record is not consumed at exchange any more — one
        // approval backs every per-role exchange until GC reclaims it.
        assert!(s.is_approved("01ARZ"), "approval persists across exchanges");
        assert!(s.get_pending("01ARZ").unwrap().is_some());
    }

    #[test]
    fn gc_drops_old_records_approved_or_not() {
        let (_d, s) = store();
        let b = s.current_bootstrap().unwrap();
        s.record_pending("old", PUBA, &b, "ip", 0).unwrap();
        s.record_pending("old-approved", PUBB, &b, "ip", 0).unwrap();
        s.approve("old-approved").unwrap();
        s.record_pending("fresh", PUBA, &b, "ip", 950).unwrap();
        let dropped = s.gc(1_000, 100).unwrap();
        assert_eq!(dropped, 2, "both stale records go, approved or not");
        assert!(s.get_pending("old").unwrap().is_none());
        assert!(s.get_pending("old-approved").unwrap().is_none());
        assert!(
            !s.is_approved("old-approved"),
            "approval marker cleared too"
        );
        assert!(s.get_pending("fresh").unwrap().is_some());
    }

    #[test]
    fn malformed_sub_rejected() {
        let (_d, s) = store();
        let b = s.current_bootstrap().unwrap();
        for bad in ["../etc", "a/b", "", "."] {
            assert!(matches!(
                s.record_pending(bad, PUBA, &b, "ip", 1),
                Err(StateError::BadSub)
            ));
        }
    }

    #[test]
    fn list_flags_anomalous_shared_pub() {
        let (_d, s) = store();
        let b = s.current_bootstrap().unwrap();
        s.record_pending("subX", PUBA, &b, "ip", 1).unwrap();
        s.record_pending("subY", PUBA, &b, "ip", 1).unwrap();
        let rows = s.list(10).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.anomalous_pub));
    }
}
