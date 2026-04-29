// Volume open helpers used by every transport (NBD, ublk, IPC-only).
//
// On a freshly-claimed fork the coordinator's prefetch task races
// the supervisor: the volume binary may try to open the fork before
// every ancestor manifest / index has been pulled. Rather than have
// each transport re-implement this, the open helpers retry briefly
// on missing-file errors and propagate everything else.

use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use elide_core::volume::{ReadonlyVolume, Volume};
use tracing::warn;

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
