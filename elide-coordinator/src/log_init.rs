//! Shared tracing initialisation for `elide` and `elide-coordinator`
//! processes.
//!
//! Every long-lived process (coordinator + every volume) opens the
//! same `<data_dir>/elide.log` file independently with `O_APPEND`.
//! Concurrent appenders compose under POSIX (atomic up to `PIPE_BUF`,
//! 4 KiB on Linux), so per-line writes from many writers don't
//! interleave. Each process owns its own fd, so the log keeps flowing
//! across coordinator restarts and a volume's lifetime is decoupled
//! from whoever spawned it — no inherited stdio, no dangling-pty
//! hangs on shutdown.
//!
//! In addition to the file, writes are tee'd to inherited `stderr` if
//! and only if the inherited stderr points at a *different* underlying
//! file (i.e. it's a terminal, a pipe, journald, etc.). When stderr
//! has already been redirected to the same `elide.log` file (e.g. by
//! `elide coord start`'s file-redirect of the daemon's stdio) the tee
//! is suppressed so log lines aren't doubled.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

/// Initialise tracing with stderr-only output. Use for short-lived CLI
/// commands that don't have (or care about) a `data_dir`.
pub fn init_stderr() {
    tracing_log::LogTracer::init().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
}

/// Initialise tracing for a long-lived process: open
/// `<data_dir>/elide.log` (creating it and any parents if necessary),
/// configure tracing to write to it, and tee to inherited `stderr` iff
/// stderr is a different underlying file (terminal/pipe/journald). On
/// success the returned `File` handle is dropped — the kernel keeps
/// the fd alive inside the tracing subscriber's writer.
pub fn init_with_data_dir(data_dir: &Path) -> io::Result<()> {
    if let Some(parent) = data_dir.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::create_dir_all(data_dir).ok();
    let log_path = data_dir.join("elide.log");
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;

    let tee_stderr = stderr_is_distinct_from(&file).unwrap_or(true);

    tracing_log::LogTracer::init().ok();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let file_writer = SharedFile::new(file);
    if tee_stderr {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(file_writer.and(io::stderr))
            .try_init()
            .ok();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(file_writer)
            .try_init()
            .ok();
    }
    Ok(())
}

/// Return `Ok(true)` iff fd 2 (stderr) refers to a different
/// underlying file (different `(st_dev, st_ino)`) than `file`. Returns
/// `Ok(false)` if they're the same file (so the caller should suppress
/// the tee). Returns `Err` if either fstat fails — caller can choose
/// the safer fallback (we default to teeing).
fn stderr_is_distinct_from(file: &File) -> io::Result<bool> {
    let file_stat = unsafe {
        let mut s: nix::libc::stat = std::mem::zeroed();
        if nix::libc::fstat(file.as_raw_fd(), &mut s) != 0 {
            return Err(io::Error::last_os_error());
        }
        s
    };
    let stderr_stat = unsafe {
        let mut s: nix::libc::stat = std::mem::zeroed();
        if nix::libc::fstat(nix::libc::STDERR_FILENO, &mut s) != 0 {
            return Err(io::Error::last_os_error());
        }
        s
    };
    Ok(file_stat.st_dev != stderr_stat.st_dev || file_stat.st_ino != stderr_stat.st_ino)
}

/// `MakeWriter` that hands out short-lived locked guards over a single
/// shared `File`. Cloning is cheap (Arc bump). Mirrors the pattern
/// used by `tracing-appender` for non-rolling files.
#[derive(Clone)]
struct SharedFile(Arc<Mutex<File>>);

impl SharedFile {
    fn new(file: File) -> Self {
        Self(Arc::new(Mutex::new(file)))
    }
}

impl<'a> MakeWriter<'a> for SharedFile {
    type Writer = SharedFileGuard<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        SharedFileGuard(self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

struct SharedFileGuard<'a>(std::sync::MutexGuard<'a, File>);

impl io::Write for SharedFileGuard<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
