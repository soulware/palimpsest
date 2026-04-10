// Client for the coordinator inbound socket.
//
// Connects to coordinator.sock, sends one command, reads one response line.
// A new connection is made per call — the protocol is request-response-close.
//
// Exception: `import_attach_by_name` reads multiple lines until a terminal line.

use std::io::{self, BufRead, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

fn call(socket_path: &Path, cmd: &str) -> io::Result<String> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    writeln!(stream, "{cmd}")?;
    stream.flush()?;
    // Ignore ENOTCONN: the coordinator may have already closed its end by the
    // time we shut down our write half, which is harmless.
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut reader = io::BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(line.trim().to_owned())
}

/// Trigger an immediate fork discovery pass.
pub fn rescan(socket_path: &Path) -> io::Result<()> {
    let resp = call(socket_path, "rescan")?;
    if resp == "ok" {
        Ok(())
    } else {
        Err(io::Error::other(format!("unexpected response: {resp}")))
    }
}

/// Query the running state of a named volume.
pub fn status(socket_path: &Path, volume: &str) -> io::Result<String> {
    call(socket_path, &format!("status {volume}"))
}

/// Ask the coordinator to start an OCI import.
/// Returns the import ULID on success (internal; callers use the volume name).
///
/// `extents_from` is a list of volume names whose extent indices should
/// populate the new volume's hash pool (dedup/delta source). The coordinator
/// validates each name and writes the flat `volume.extent_index` file.
pub fn import_start(
    socket_path: &Path,
    name: &str,
    oci_ref: &str,
    extents_from: &[String],
) -> io::Result<()> {
    let line = if extents_from.is_empty() {
        format!("import {name} {oci_ref}")
    } else {
        // Volume names are [a-zA-Z0-9._-] so comma is a safe separator.
        let joined = extents_from.join(",");
        format!("import {name} {oci_ref} extents:{joined}")
    };
    let resp = call(socket_path, &line)?;
    match resp.split_once(' ') {
        Some(("ok", _ulid)) => Ok(()),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Poll the state of an import job by volume name.
/// Returns the raw state string (`running`, `done`, or an error).
pub fn import_status_by_name(socket_path: &Path, name: &str) -> io::Result<String> {
    call(socket_path, &format!("import status {name}"))
}

/// Stream import output to `out` until the import completes, identified by volume name.
///
/// Reads lines from the coordinator until a terminal `ok done` or `err ...`
/// line is received. Each output line is written to `out`. Returns Ok(()) on
/// success, Err on import failure or I/O error.
pub fn import_attach_by_name(
    socket_path: &Path,
    name: &str,
    out: &mut dyn Write,
) -> io::Result<()> {
    let mut stream = UnixStream::connect(socket_path).map_err(|e| {
        io::Error::other(format!(
            "coordinator not running ({}): {e}",
            socket_path.display()
        ))
    })?;
    writeln!(stream, "import attach {name}")?;
    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Write)?;

    let mut reader = io::BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if trimmed == "ok done" {
            return Ok(());
        }
        if let Some(msg) = trimmed.strip_prefix("err ") {
            return Err(io::Error::other(msg.to_owned()));
        }
        writeln!(out, "{trimmed}")?;
    }
    Ok(())
}

/// Evict S3-confirmed segment bodies from a volume's cache/ directory.
///
/// Routed through the coordinator so eviction is sequenced between drain/GC
/// ticks and never races with the GC pass's collect_stats → compact_segments
/// window.
///
/// If `ulid` is `Some`, evicts only that segment. If `None`, evicts all
/// S3-confirmed bodies.
///
/// Returns the number of segments evicted.
pub fn evict_volume(socket_path: &Path, name: &str, ulid: Option<&str>) -> io::Result<usize> {
    let cmd = match ulid {
        Some(u) => format!("evict {name} {u}"),
        None => format!("evict {name}"),
    };
    let resp = call(socket_path, &cmd)?;
    match resp.split_once(' ') {
        Some(("ok", n)) => n
            .parse::<usize>()
            .map_err(|e| io::Error::other(format!("unexpected response: {resp}: {e}"))),
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}

/// Stop all processes for a volume and remove its directory.
pub fn delete_volume(socket_path: &Path, name: &str) -> io::Result<()> {
    let resp = call(socket_path, &format!("delete {name}"))?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ if resp == "ok" => Ok(()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}
