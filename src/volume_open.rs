// Volume open helpers used by every transport (NBD, ublk, IPC-only).
//
// On a freshly-claimed fork the coordinator's prefetch task races
// the supervisor: the volume binary may try to open the fork before
// every ancestor manifest / index has been pulled. Rather than have
// each transport re-implement this, the open helpers:
//
//   1. Block on the coordinator's `await-prefetch` IPC if a coordinator
//      is reachable. That gives a strong "ancestors are populated"
//      signal — we wait for the actual prefetch to complete, instead
//      of just retrying on the symptom (NotFound during open).
//   2. Then retry briefly on missing-file errors during `Volume::open`
//      itself. This is the second line of defence: covers untracked
//      forks (no coordinator), `force-snapshot-now` style races, and
//      filesystem-level latency between the prefetch finishing and the
//      `.idx` becoming visible to a peer process.

use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use elide_core::volume::{ReadonlyVolume, Volume};
use tracing::warn;

use crate::coordinator_client::{PREFETCH_AWAIT_BUDGET, await_prefetch, is_reachable};

/// How long volume open retries wait in total before giving up when an
/// ancestor artifact is missing. The common case is the coordinator's
/// prefetch task racing with the supervisor's spawn — a second or two
/// is plenty. After the budget expires we propagate the error.
pub const OPEN_RETRY_BUDGET: Duration = Duration::from_secs(10);
const OPEN_RETRY_STEP: Duration = Duration::from_millis(500);

/// Return `true` if `e` looks like a transient "ancestor artifact not yet
/// on disk" error (NotFound at the OS layer, wrapped in `io::Error::other`
/// with a message that includes the underlying ENOENT text). The retry
/// loop treats these as retryable; everything else propagates immediately.
fn is_missing_file_error(e: &io::Error) -> bool {
    if e.kind() == io::ErrorKind::NotFound {
        return true;
    }
    // Upstream signing / volume-open helpers wrap ENOENT in
    // `io::Error::other(format!("… not readable: {e}"))`, discarding the
    // original `ErrorKind`. Detect the embedded ENOENT via its text.
    let msg = e.to_string();
    msg.contains("No such file or directory")
}

/// Open a writable volume, retrying briefly on missing-file errors so
/// the coordinator's prefetch task has time to land ancestor artifacts.
pub fn open_volume_with_retry(dir: &Path, by_id_dir: &Path) -> io::Result<Volume> {
    wait_for_prefetch_if_coordinator_present(dir)?;
    let started = Instant::now();
    loop {
        match Volume::open(dir, by_id_dir) {
            Ok(v) => return Ok(v),
            Err(e) if is_missing_file_error(&e) && started.elapsed() < OPEN_RETRY_BUDGET => {
                warn!("volume open: ancestor artifact missing, retrying ({e})");
                std::thread::sleep(OPEN_RETRY_STEP);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Readonly counterpart to [`open_volume_with_retry`].
pub fn open_readonly_volume_with_retry(dir: &Path, by_id_dir: &Path) -> io::Result<ReadonlyVolume> {
    wait_for_prefetch_if_coordinator_present(dir)?;
    let started = Instant::now();
    loop {
        match ReadonlyVolume::open(dir, by_id_dir) {
            Ok(v) => return Ok(v),
            Err(e) if is_missing_file_error(&e) && started.elapsed() < OPEN_RETRY_BUDGET => {
                warn!("volume open (readonly): ancestor artifact missing, retrying ({e})");
                std::thread::sleep(OPEN_RETRY_STEP);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Block on the coordinator's `await-prefetch` IPC when a coordinator is
/// reachable.
///
/// Layout: `dir = <data_dir>/by_id/<vol_ulid>`, so the coordinator socket
/// lives at `<data_dir>/control.sock`. Standalone volumes (no coordinator,
/// no socket) skip the wait — there is no prefetch task to wait for, and
/// the per-call `Volume::open` retry loop still covers any residual
/// missing-file window.
fn wait_for_prefetch_if_coordinator_present(dir: &Path) -> io::Result<()> {
    let Some((socket_path, vol_ulid)) = derive_coordinator_socket(dir) else {
        return Ok(());
    };
    if !is_reachable(&socket_path) {
        return Ok(());
    }
    await_prefetch(&socket_path, &vol_ulid, PREFETCH_AWAIT_BUDGET)
}

fn derive_coordinator_socket(dir: &Path) -> Option<(std::path::PathBuf, String)> {
    let by_id = dir.parent()?;
    let data_dir = by_id.parent()?;
    let vol_ulid = dir.file_name()?.to_str()?.to_owned();
    // Sanity: only treat the directory as ULID-named when the parent is
    // literally `by_id`, so single-segment scratch volumes used in tests
    // (e.g. `tempdir/vol-xyz`) never accidentally trigger an IPC.
    if by_id.file_name()?.to_str()? != "by_id" {
        return None;
    }
    Some((data_dir.join("control.sock"), vol_ulid))
}
