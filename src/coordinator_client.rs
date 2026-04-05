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
    stream.shutdown(std::net::Shutdown::Write)?;
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
pub fn import_start(socket_path: &Path, name: &str, oci_ref: &str) -> io::Result<()> {
    let resp = call(socket_path, &format!("import {name} {oci_ref}"))?;
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

/// Stop all processes for a volume and remove its directory.
pub fn delete_volume(socket_path: &Path, name: &str) -> io::Result<()> {
    let resp = call(socket_path, &format!("delete {name}"))?;
    match resp.split_once(' ') {
        Some(("err", msg)) => Err(io::Error::other(msg.to_owned())),
        _ if resp == "ok" => Ok(()),
        _ => Err(io::Error::other(format!("unexpected response: {resp}"))),
    }
}
