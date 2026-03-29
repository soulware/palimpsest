// Per-fork serve configuration, loaded from serve.toml in the fork directory.
//
// A fork with a serve.toml is opted in to NBD supervision by the coordinator.
// Without a serve.toml, the fork is drained but not served.
//
// Example serve.toml:
//
//   bind = "0.0.0.0:10809"
//   auto_flush_secs = 10    # optional; defaults to serve-volume's own default
//   readonly = false        # optional; defaults to false

use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct ServeConfig {
    /// NBD bind address and port (e.g. "0.0.0.0:10809" or "127.0.0.1:10810").
    pub bind: String,

    /// Auto-flush idle interval in seconds.
    /// Omit to use serve-volume's default (10s).
    pub auto_flush_secs: Option<u64>,

    /// Serve the fork as read-only.
    pub readonly: Option<bool>,
}

/// Load serve.toml from a fork directory.
/// Returns `Ok(None)` if the file does not exist (fork is not supervised).
pub fn load(fork_dir: &Path) -> Result<Option<ServeConfig>> {
    let path = fork_dir.join("serve.toml");
    if !path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading serve.toml: {}", path.display()))?;
    let config =
        toml::from_str(&text).with_context(|| format!("parsing serve.toml: {}", path.display()))?;
    Ok(Some(config))
}
