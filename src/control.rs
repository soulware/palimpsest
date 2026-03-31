// Volume control socket server.
//
// Listens on <fork_dir>/control.sock for coordinator requests.
// Each connection carries one request and one response, then closes.
//
// Protocol:
//   Request:  "<op> [args...]\n"
//   Success:  "ok [values...]\n"
//   Error:    "err <message>\n"
//
// Supported operations:
//   flush
//     Flush the WAL to a pending segment.  Returns "ok".
//
//   sweep_pending
//     Compact small pending segments.
//     Returns "ok <segments_compacted> <bytes_freed> <extents_removed>".
//
//   repack <min_live_ratio>
//     Compact sparse pending segments below the given ratio.
//     Returns "ok <segments_compacted> <bytes_freed> <extents_removed>".
//
//   gc_checkpoint
//     Flush WAL and return two ULIDs for GC output segments.
//     Returns "ok <repack_ulid> <sweep_ulid>".

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::thread;

use elide_core::actor::VolumeHandle;

/// Start the control socket server for `fork_dir`.
///
/// Binds `<fork_dir>/control.sock`, removes any stale socket from a previous
/// run first.  Spawns a dedicated thread that serves one request per
/// connection until the listener is closed.  The socket file is removed when
/// the thread exits.
///
/// The handle is cloned onto the server thread; the caller retains its own
/// clone.
pub fn start(fork_dir: &Path, handle: VolumeHandle) -> std::io::Result<()> {
    let socket_path = fork_dir.join("control.sock");
    // Remove stale socket from a previous (crashed) run.
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    thread::Builder::new()
        .name("control-server".into())
        .spawn(move || run(listener, socket_path, handle))
        .map_err(std::io::Error::other)?;
    Ok(())
}

fn run(listener: UnixListener, socket_path: PathBuf, handle: VolumeHandle) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream, &handle),
            Err(_) => break,
        }
    }
    let _ = std::fs::remove_file(&socket_path);
}

fn handle_connection(stream: std::os::unix::net::UnixStream, handle: &VolumeHandle) {
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let line = line.trim();
    if line == "flush" {
        match handle.flush() {
            Ok(()) => {
                let _ = writeln!(writer, "ok");
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
            }
        }
    } else if line == "sweep_pending" {
        match handle.sweep_pending() {
            Ok(s) => {
                let _ = writeln!(
                    writer,
                    "ok {} {} {} {}",
                    s.segments_compacted, s.new_segments, s.bytes_freed, s.extents_removed
                );
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
            }
        }
    } else if let Some(ratio_str) = line.strip_prefix("repack ") {
        match ratio_str.parse::<f64>() {
            Ok(ratio) => match handle.repack(ratio) {
                Ok(s) => {
                    let _ = writeln!(
                        writer,
                        "ok {} {} {} {}",
                        s.segments_compacted, s.new_segments, s.bytes_freed, s.extents_removed
                    );
                }
                Err(e) => {
                    let _ = writeln!(writer, "err {e}");
                }
            },
            Err(_) => {
                let _ = writeln!(writer, "err invalid ratio: {ratio_str}");
            }
        }
    } else if line == "gc_checkpoint" {
        match handle.gc_checkpoint() {
            Ok((u1, u2)) => {
                let _ = writeln!(writer, "ok {u1} {u2}");
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
            }
        }
    } else {
        let _ = writeln!(writer, "err unknown op: {line}");
    }
}
