//! Process-global paths to the sibling `elide` and `elide-import`
//! binaries. Set once at daemon boot from [`crate::config::CoordinatorConfig`]
//! so the supervisor (which spawns `elide serve-volume` per fork) and the
//! import path (which spawns `elide-import`) can read the values without us
//! threading them through every caller.
//!
//! Defaults are bare names (`elide`, `elide-import`) resolved via `PATH`,
//! mirroring the config defaults — so paths in code that runs before
//! `daemon::run` (or in unit tests that never call the setters) still see
//! a usable value.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static ELIDE_BIN: OnceLock<PathBuf> = OnceLock::new();
static ELIDE_IMPORT_BIN: OnceLock<PathBuf> = OnceLock::new();

const DEFAULT_ELIDE_BIN: &str = "elide";
const DEFAULT_ELIDE_IMPORT_BIN: &str = "elide-import";

/// Install the `elide` binary path. Called once by `daemon::run`; later
/// calls are silently ignored.
pub fn set_elide_bin(path: PathBuf) {
    let _ = ELIDE_BIN.set(path);
}

/// Install the `elide-import` binary path. Called once by `daemon::run`;
/// later calls are silently ignored.
pub fn set_elide_import_bin(path: PathBuf) {
    let _ = ELIDE_IMPORT_BIN.set(path);
}

/// Path used to spawn `elide serve-volume`.
pub fn elide_bin() -> &'static Path {
    ELIDE_BIN
        .get()
        .map(PathBuf::as_path)
        .unwrap_or_else(|| Path::new(DEFAULT_ELIDE_BIN))
}

/// Path used to spawn `elide-import`.
pub fn elide_import_bin() -> &'static Path {
    ELIDE_IMPORT_BIN
        .get()
        .map(PathBuf::as_path)
        .unwrap_or_else(|| Path::new(DEFAULT_ELIDE_IMPORT_BIN))
}
