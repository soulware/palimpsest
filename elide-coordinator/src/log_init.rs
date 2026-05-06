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
//! Two roles, two helpers:
//!
//! * [`init_for_coord`] — coordinator process. tee writes to
//!   `elide.log` and `stderr`. When stderr is already pointing at
//!   `elide.log` (e.g. `coord start` daemon mode where the CLI
//!   redirected stdio to the file), the stderr leg is suppressed via
//!   `(st_dev, st_ino)` comparison so log lines aren't doubled.
//!
//! * [`init_for_volume`] — volume process. tee writes to `elide.log`
//!   and a Unix-socket relay at `<data_dir>/log.sock` served by the
//!   *currently-running* coordinator. The relay decouples a volume's
//!   live-on-terminal output from the inherited stdio of whichever
//!   coord spawned it: when coord rolls (Ctrl-C, replace), volumes'
//!   relay connections EPIPE silently and reconnect on the next
//!   tracing event, so the new coord transparently picks up the
//!   stream. When no coord is running, the connect attempt fails
//!   `ECONNREFUSED` and the relay sink is silently inert — the file
//!   continues to receive every line.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

/// Filename of the coord-side log relay Unix socket under `data_dir`.
/// Mirrored in [`crate::log_relay`].
const LOG_RELAY_SOCKET: &str = "log.sock";

/// Don't retry the relay connect more often than this when it's down.
/// Connect is cheap (`ECONNREFUSED` returns immediately) but we still
/// avoid spinning syscalls on every log event.
const RELAY_RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// Initialise tracing with stderr-only output. Use for short-lived
/// CLI commands that don't have (or care about) a `data_dir`.
pub fn init_stderr() {
    tracing_log::LogTracer::init().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .ok();
}

/// Initialise tracing for the coordinator process. Tee writes to
/// `<data_dir>/elide.log` and `stderr`, suppressing the stderr leg
/// when stderr already points at the same file.
pub fn init_for_coord(data_dir: &Path) -> io::Result<()> {
    let file = open_elide_log(data_dir)?;
    let tee_stderr = stderr_is_distinct_from(&file).unwrap_or(true);
    let file_writer = SharedFile::new(file);

    tracing_log::LogTracer::init().ok();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if tee_stderr {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_writer(file_writer.and(SilentStderr))
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

/// Initialise tracing for a volume process. Tee writes to
/// `<data_dir>/elide.log` and a Unix-socket relay at
/// `<data_dir>/log.sock` (served by the currently-running
/// coordinator, if any).
pub fn init_for_volume(data_dir: &Path) -> io::Result<()> {
    let file = open_elide_log(data_dir)?;
    let file_writer = SharedFile::new(file);
    let relay_writer = RelayClient::new(data_dir.join(LOG_RELAY_SOCKET));

    tracing_log::LogTracer::init().ok();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(file_writer.and(relay_writer))
        .try_init()
        .ok();
    Ok(())
}

fn open_elide_log(data_dir: &Path) -> io::Result<File> {
    if let Some(parent) = data_dir.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::create_dir_all(data_dir).ok();
    let log_path = data_dir.join("elide.log");
    OpenOptions::new().create(true).append(true).open(&log_path)
}

/// Return `Ok(true)` iff fd 2 (stderr) refers to a different
/// underlying file (different `(st_dev, st_ino)`) than `file`.
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

// ── coord-side: stderr writer that swallows write errors ──────────

/// `MakeWriter` over `stderr` that pretends every write succeeded —
/// the underlying `write_all` is a `let _ = ...`. Required so a
/// transiently-dead inherited stderr fd cannot cause
/// tracing-subscriber's fmt layer to panic via its eprintln-on-error
/// fallback (which writes to the same broken fd, EIOs, and panics
/// `eprintln!`).
#[derive(Clone, Copy)]
struct SilentStderr;

impl<'a> MakeWriter<'a> for SilentStderr {
    type Writer = SilentStderrWriter;
    fn make_writer(&'a self) -> Self::Writer {
        SilentStderrWriter
    }
}

struct SilentStderrWriter;

impl io::Write for SilentStderrWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = io::Write::write_all(&mut io::stderr(), buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ── shared file writer ────────────────────────────────────────────

/// `MakeWriter` that hands out short-lived locked guards over a
/// single shared `File`. Cloning is cheap (Arc bump). Mirrors the
/// pattern used by `tracing-appender` for non-rolling files.
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

// ── volume-side: log relay client ─────────────────────────────────

/// `MakeWriter` that lazily maintains a Unix-stream connection to
/// `<data_dir>/log.sock` (the coord's relay server). On write error
/// (EPIPE from a coord that just exited) the stream is dropped and
/// the next event triggers a reconnect, subject to a small backoff
/// so volumes don't hammer connect() during a coord-less window.
///
/// All write errors — connect failure, write failure, no server —
/// are silently swallowed (writer reports success). The relay is a
/// best-effort secondary sink; the file path is the durable one.
/// This is essential to avoid the same panic-on-write-error chain
/// that `SilentStderr` exists to defend against.
#[derive(Clone)]
struct RelayClient {
    socket_path: PathBuf,
    state: Arc<Mutex<RelayState>>,
}

struct RelayState {
    stream: Option<UnixStream>,
    /// Last time we attempted a `connect()`; used to debounce
    /// reconnects when the coordinator is down.
    last_attempt: Instant,
}

impl RelayClient {
    fn new(socket_path: PathBuf) -> Self {
        Self {
            socket_path,
            state: Arc::new(Mutex::new(RelayState {
                stream: None,
                // A timestamp far enough in the past that the first
                // event triggers an immediate connect attempt rather
                // than waiting out the backoff.
                last_attempt: Instant::now()
                    .checked_sub(RELAY_RECONNECT_BACKOFF)
                    .unwrap_or_else(Instant::now),
            })),
        }
    }
}

impl<'a> MakeWriter<'a> for RelayClient {
    type Writer = RelayWriter<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        RelayWriter(self)
    }
}

struct RelayWriter<'a>(&'a RelayClient);

impl io::Write for RelayWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());

        // Fast path: connection live → write through. On error,
        // drop the stream and fall through to reconnect logic.
        if let Some(stream) = state.stream.as_mut()
            && stream.write_all(buf).is_ok()
        {
            return Ok(buf.len());
        }
        state.stream = None;

        // Backoff after a recent failed attempt; the coord is
        // probably down and we don't want to syscall on every event.
        if state.last_attempt.elapsed() < RELAY_RECONNECT_BACKOFF {
            return Ok(buf.len());
        }
        state.last_attempt = Instant::now();

        if let Ok(mut stream) = UnixStream::connect(&self.0.socket_path)
            && stream.write_all(buf).is_ok()
        {
            state.stream = Some(stream);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
