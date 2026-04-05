use std::io;
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

impl VolumeConfig {
    /// Read `volume.toml` from `dir`. Returns an empty config if the file does
    /// not exist (e.g. volume predates the consolidated config).
    pub fn read(dir: &Path) -> io::Result<Self> {
        let path = dir.join(CONFIG_FILE);
        match std::fs::read_to_string(&path) {
            Ok(s) => toml::from_str(&s)
                .map_err(|e| io::Error::other(format!("invalid {CONFIG_FILE}: {e}"))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Write `volume.toml` to `dir` atomically (via temp-file rename).
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let s = toml::to_string(self)
            .map_err(|e| io::Error::other(format!("serializing {CONFIG_FILE}: {e}")))?;
        crate::segment::write_file_atomic(&dir.join(CONFIG_FILE), s.as_bytes())
    }
}
