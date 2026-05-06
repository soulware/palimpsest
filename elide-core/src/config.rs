use std::fmt;
use std::io;
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const CONFIG_FILE: &str = "volume.toml";

/// Consolidated per-volume configuration stored in `volume.toml`.
///
/// Contains everything that was previously scattered across `volume.name`,
/// `volume.size`, `nbd.port`, `nbd.bind`, and `nbd.socket`.
///
/// Files that remain separate:
/// - `volume.key` / `volume.pub` / `volume.provenance` — signing key material
/// - `volume.lock` — advisory lock (flock on a standalone fd)
/// - `volume.readonly` — safety marker written early during import
/// - `control.sock` — runtime Unix socket
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct VolumeConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbd: Option<NbdConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ublk: Option<UblkConfig>,
    /// Opt out of background full-volume warming on writable start.
    /// `Some(true)` keeps the volume on demand-fetch only; default (None /
    /// false) eagerly warms every live extent from S3 in the background.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lazy: Option<bool>,
}

/// NBD server configuration within `volume.toml`.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct NbdConfig {
    /// IP address to bind on. Defaults to `127.0.0.1` if omitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind: Option<String>,
    /// TCP port. Mutually exclusive with `socket`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Unix socket path. Mutually exclusive with `port`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket: Option<PathBuf>,
}

/// ublk server configuration within `volume.toml`.
///
/// The presence of the `[ublk]` section means "serve this volume over ublk
/// instead of NBD". The two transports are mutually exclusive per volume; a
/// `volume.toml` containing both `[nbd]` and `[ublk]` is rejected at parse
/// time.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct UblkConfig {
    /// Bound ublk device id (maps to `/dev/ublkb<id>`).
    ///
    /// Carries two related meanings:
    ///   * Before the first ADD: user-supplied pin. `None` means "kernel
    ///     auto-allocates on first start".
    ///   * After a successful ADD: the kernel-assigned id, written back so
    ///     the next serve recognises and recovers the QUIESCED device.
    ///
    /// Authoritative ownership lives in the kernel device's `target_data`
    /// stamp; this field is the local hint for which id to look at.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev_id: Option<i32>,
}

/// Resolved NBD endpoint for conflict detection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NbdEndpoint {
    Tcp { bind: String, port: u16 },
    Socket(PathBuf),
}

impl fmt::Display for NbdEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp { bind, port } => write!(f, "{bind}:{port}"),
            Self::Socket(path) => write!(f, "{}", path.display()),
        }
    }
}

impl NbdEndpoint {
    /// Probe whether something is already listening on this endpoint.
    ///
    /// For sockets: attempts a non-blocking `connect(2)`.
    /// For TCP: attempts a blocking connect to `bind:port`.
    ///
    /// Returns `true` if a connection succeeds (endpoint is in use).
    pub fn is_in_use(&self) -> bool {
        match self {
            Self::Socket(path) => UnixStream::connect(path).is_ok(),
            Self::Tcp { bind, port } => TcpStream::connect((bind.as_str(), *port)).is_ok(),
        }
    }
}

impl NbdConfig {
    /// Resolve this config to an endpoint, using the volume directory to
    /// absolutify relative socket paths.
    pub fn endpoint(&self, vol_dir: &Path) -> Option<NbdEndpoint> {
        if let Some(ref socket) = self.socket {
            let resolved = if socket.is_absolute() {
                socket.clone()
            } else {
                vol_dir.join(socket)
            };
            Some(NbdEndpoint::Socket(resolved))
        } else {
            let port = self.port?;
            let bind = self.bind.clone().unwrap_or_else(|| "127.0.0.1".to_owned());
            Some(NbdEndpoint::Tcp { bind, port })
        }
    }
}

impl VolumeConfig {
    /// Read `volume.toml` from `dir`. Returns an empty config if the file does
    /// not exist (e.g. volume predates the consolidated config).
    pub fn read(dir: &Path) -> io::Result<Self> {
        let path = dir.join(CONFIG_FILE);
        let cfg: Self = match std::fs::read_to_string(&path) {
            Ok(s) => toml::from_str(&s)
                .map_err(|e| io::Error::other(format!("invalid {CONFIG_FILE}: {e}")))?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(e),
        };
        if cfg.nbd.is_some() && cfg.ublk.is_some() {
            return Err(io::Error::other(format!(
                "invalid {CONFIG_FILE}: [nbd] and [ublk] are mutually exclusive — pick one transport"
            )));
        }
        Ok(cfg)
    }

    /// Write `volume.toml` to `dir` atomically (via temp-file rename).
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let s = toml::to_string(self)
            .map_err(|e| io::Error::other(format!("serializing {CONFIG_FILE}: {e}")))?;
        crate::segment::write_file_atomic(&dir.join(CONFIG_FILE), s.as_bytes())
    }

    /// Read the bound ublk device id, if any.
    ///
    /// `None` means either no ublk transport (no `[ublk]` section) or
    /// `[ublk]` exists but no id has been bound yet (auto-allocate on
    /// next ADD). Callers that need to distinguish the two cases must
    /// inspect `cfg.ublk` directly.
    pub fn bound_ublk_id(dir: &Path) -> io::Result<Option<i32>> {
        Ok(Self::read(dir)?.ublk.and_then(|u| u.dev_id))
    }

    /// Persist the kernel-assigned ublk device id into `volume.toml`.
    ///
    /// Called from the ublk daemon's `wait_hook` once the kernel commits
    /// to an id. Read-modify-write so unrelated config (size, name, nbd
    /// settings) is preserved. Idempotent: rewriting the same id is a
    /// no-op on disk if the file content is unchanged.
    pub fn set_bound_ublk_id(dir: &Path, id: i32) -> io::Result<()> {
        let mut cfg = Self::read(dir)?;
        let ublk = cfg.ublk.get_or_insert_with(Default::default);
        ublk.dev_id = Some(id);
        cfg.write(dir)
    }

    /// Clear the bound ublk device id while preserving the `[ublk]`
    /// section. Used by `elide ublk delete` and the coordinator's
    /// reconciliation sweep: the operator removed the binding but did
    /// not change transport policy, so the next serve auto-allocates.
    ///
    /// No-op (no write) if there is no `[ublk]` section or the id is
    /// already absent.
    pub fn clear_bound_ublk_id(dir: &Path) -> io::Result<()> {
        let mut cfg = Self::read(dir)?;
        let Some(ublk) = cfg.ublk.as_mut() else {
            return Ok(());
        };
        if ublk.dev_id.is_none() {
            return Ok(());
        }
        ublk.dev_id = None;
        cfg.write(dir)
    }
}

/// Details of an NBD endpoint conflict.
pub struct NbdConflict {
    pub endpoint: NbdEndpoint,
    /// Human-readable name of the conflicting volume (falls back to ULID).
    pub name: String,
    /// Directory of the conflicting volume, if the conflict was found via
    /// config scan. `None` when detected by endpoint probe only.
    pub dir: Option<PathBuf>,
}

/// Check whether `vol_dir`'s NBD endpoint conflicts with another volume or
/// is already in use by something else.
///
/// Two checks are performed:
///   1. Config scan: walks `data_dir/by_id/` looking for another active
///      (non-stopped, non-readonly) volume with the same endpoint.
///   2. Probe: tries to connect to the endpoint to catch conflicts from
///      outside elide or stale state.
///
/// Returns `Ok(Some(conflict))` on conflict, `Ok(None)` if the endpoint is
/// free (or the volume has no NBD config).
pub fn find_nbd_conflict(vol_dir: &Path, data_dir: &Path) -> io::Result<Option<NbdConflict>> {
    let cfg = VolumeConfig::read(vol_dir)?;
    let endpoint = match cfg.nbd.as_ref().and_then(|nbd| nbd.endpoint(vol_dir)) {
        Some(ep) => ep,
        None => return Ok(None),
    };

    // Canonicalize vol_dir so we can skip it in the scan (vol_dir may be a
    // by_name/ symlink pointing into by_id/).
    let canonical = std::fs::canonicalize(vol_dir)?;

    // 1. Config scan: another elide volume claims the same endpoint.
    let by_id = data_dir.join("by_id");
    let entries = match std::fs::read_dir(&by_id) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    for entry in entries.flatten() {
        let other = entry.path();
        if !other.is_dir() {
            continue;
        }
        // Skip self.
        if let Ok(other_canon) = std::fs::canonicalize(&other)
            && other_canon == canonical
        {
            continue;
        }
        // Skip stopped / readonly volumes — they won't bind the endpoint.
        if other.join("volume.stopped").exists() || other.join("volume.readonly").exists() {
            continue;
        }
        let other_cfg = match VolumeConfig::read(&other) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if let Some(other_nbd) = other_cfg.nbd.as_ref()
            && let Some(other_ep) = other_nbd.endpoint(&other)
            && other_ep == endpoint
        {
            let name = other_cfg.name.unwrap_or_else(|| {
                other
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
            return Ok(Some(NbdConflict {
                endpoint,
                name,
                dir: Some(other.clone()),
            }));
        }
    }

    // 2. Probe: something outside elide (or a volume whose config we missed)
    //    is already listening on the endpoint.
    if endpoint.is_in_use() {
        return Ok(Some(NbdConflict {
            endpoint,
            name: "unknown (endpoint already in use)".to_owned(),
            dir: None,
        }));
    }

    Ok(None)
}

/// Details of a ublk dev-id conflict.
pub struct UblkConflict {
    pub dev_id: i32,
    /// Human-readable name of the conflicting volume.
    pub name: String,
    pub dir: PathBuf,
}

/// Check whether `vol_dir`'s ublk dev-id collides with another active volume.
///
/// Only volumes that pin an explicit `dev_id` participate — auto-allocated
/// devices (no `dev_id` in `[ublk]`) are resolved by the kernel at start time
/// and cannot conflict ahead of spawn.
///
/// Returns `Ok(Some(conflict))` on conflict, `Ok(None)` otherwise.
pub fn find_ublk_conflict(vol_dir: &Path, data_dir: &Path) -> io::Result<Option<UblkConflict>> {
    let cfg = VolumeConfig::read(vol_dir)?;
    let dev_id = match cfg.ublk.as_ref().and_then(|u| u.dev_id) {
        Some(id) => id,
        None => return Ok(None),
    };

    let canonical = std::fs::canonicalize(vol_dir)?;

    let by_id = data_dir.join("by_id");
    let entries = match std::fs::read_dir(&by_id) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    for entry in entries.flatten() {
        let other = entry.path();
        if !other.is_dir() {
            continue;
        }
        if let Ok(other_canon) = std::fs::canonicalize(&other)
            && other_canon == canonical
        {
            continue;
        }
        if other.join("volume.stopped").exists() || other.join("volume.readonly").exists() {
            continue;
        }
        let other_cfg = match VolumeConfig::read(&other) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if let Some(other_ublk) = other_cfg.ublk.as_ref()
            && other_ublk.dev_id == Some(dev_id)
        {
            let name = other_cfg.name.unwrap_or_else(|| {
                other
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
            return Ok(Some(UblkConflict {
                dev_id,
                name,
                dir: other,
            }));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_config(dir: &Path, contents: &str) {
        std::fs::write(dir.join(CONFIG_FILE), contents).unwrap();
    }

    #[test]
    fn ublk_section_with_no_keys_parses_as_enabled() {
        let tmp = TempDir::new().unwrap();
        write_config(tmp.path(), "[ublk]\n");
        let cfg = VolumeConfig::read(tmp.path()).unwrap();
        let ublk = cfg.ublk.expect("ublk section should be present");
        assert_eq!(ublk.dev_id, None);
    }

    #[test]
    fn ublk_dev_id_roundtrips() {
        let tmp = TempDir::new().unwrap();
        write_config(tmp.path(), "[ublk]\ndev_id = 7\n");
        let cfg = VolumeConfig::read(tmp.path()).unwrap();
        assert_eq!(cfg.ublk.unwrap().dev_id, Some(7));
    }

    #[test]
    fn bound_ublk_id_returns_none_when_no_section() {
        let tmp = TempDir::new().unwrap();
        // No volume.toml at all.
        assert_eq!(VolumeConfig::bound_ublk_id(tmp.path()).unwrap(), None);
        // Empty volume.toml.
        write_config(tmp.path(), "");
        assert_eq!(VolumeConfig::bound_ublk_id(tmp.path()).unwrap(), None);
        // [ublk] enabled but no id bound yet.
        write_config(tmp.path(), "[ublk]\n");
        assert_eq!(VolumeConfig::bound_ublk_id(tmp.path()).unwrap(), None);
    }

    #[test]
    fn set_bound_ublk_id_creates_section_and_preserves_other_fields() {
        let tmp = TempDir::new().unwrap();
        write_config(tmp.path(), "name = \"alpha\"\nsize = 1024\n");
        VolumeConfig::set_bound_ublk_id(tmp.path(), 5).unwrap();

        let cfg = VolumeConfig::read(tmp.path()).unwrap();
        assert_eq!(cfg.name.as_deref(), Some("alpha"));
        assert_eq!(cfg.size, Some(1024));
        assert_eq!(cfg.ublk.unwrap().dev_id, Some(5));
    }

    #[test]
    fn set_bound_ublk_id_overwrites_existing_id() {
        let tmp = TempDir::new().unwrap();
        write_config(tmp.path(), "[ublk]\ndev_id = 3\n");
        VolumeConfig::set_bound_ublk_id(tmp.path(), 9).unwrap();
        assert_eq!(VolumeConfig::bound_ublk_id(tmp.path()).unwrap(), Some(9));
    }

    #[test]
    fn clear_bound_ublk_id_keeps_section_drops_id() {
        let tmp = TempDir::new().unwrap();
        write_config(tmp.path(), "[ublk]\ndev_id = 5\n");
        VolumeConfig::clear_bound_ublk_id(tmp.path()).unwrap();

        let cfg = VolumeConfig::read(tmp.path()).unwrap();
        let ublk = cfg
            .ublk
            .expect("clearing the dev_id must not remove the [ublk] section");
        assert_eq!(ublk.dev_id, None);
    }

    #[test]
    fn clear_bound_ublk_id_is_noop_without_section() {
        let tmp = TempDir::new().unwrap();
        // Empty file: no [ublk] section means there's nothing to clear.
        write_config(tmp.path(), "name = \"alpha\"\n");
        VolumeConfig::clear_bound_ublk_id(tmp.path()).unwrap();
        let cfg = VolumeConfig::read(tmp.path()).unwrap();
        assert!(cfg.ublk.is_none());
        assert_eq!(cfg.name.as_deref(), Some("alpha"));
    }

    #[test]
    fn nbd_and_ublk_together_are_rejected() {
        let tmp = TempDir::new().unwrap();
        write_config(tmp.path(), "[nbd]\nport = 10809\n\n[ublk]\n");
        let err = VolumeConfig::read(tmp.path()).unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected mutex error, got: {err}"
        );
    }

    #[test]
    fn find_ublk_conflict_detects_dev_id_collision() {
        let tmp = TempDir::new().unwrap();
        let data = tmp.path();
        let by_id = data.join("by_id");
        std::fs::create_dir_all(&by_id).unwrap();

        let a = by_id.join("01J000000000000000000000A0");
        let b = by_id.join("01J000000000000000000000B0");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        write_config(&a, "name = \"alpha\"\n[ublk]\ndev_id = 3\n");
        write_config(&b, "name = \"beta\"\n[ublk]\ndev_id = 3\n");

        let conflict = find_ublk_conflict(&a, data).unwrap().expect("conflict");
        assert_eq!(conflict.dev_id, 3);
        assert_eq!(conflict.name, "beta");
    }

    #[test]
    fn find_ublk_conflict_ignores_auto_alloc() {
        let tmp = TempDir::new().unwrap();
        let data = tmp.path();
        let by_id = data.join("by_id");
        std::fs::create_dir_all(&by_id).unwrap();

        let a = by_id.join("01J000000000000000000000A0");
        let b = by_id.join("01J000000000000000000000B0");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        write_config(&a, "[ublk]\n");
        write_config(&b, "[ublk]\n");

        assert!(find_ublk_conflict(&a, data).unwrap().is_none());
    }

    #[test]
    fn find_ublk_conflict_skips_stopped_volume() {
        let tmp = TempDir::new().unwrap();
        let data = tmp.path();
        let by_id = data.join("by_id");
        std::fs::create_dir_all(&by_id).unwrap();

        let a = by_id.join("01J000000000000000000000A0");
        let b = by_id.join("01J000000000000000000000B0");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        write_config(&a, "[ublk]\ndev_id = 4\n");
        write_config(&b, "[ublk]\ndev_id = 4\n");
        std::fs::write(b.join("volume.stopped"), "").unwrap();

        assert!(find_ublk_conflict(&a, data).unwrap().is_none());
    }
}
