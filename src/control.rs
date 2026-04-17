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
//
//   apply_gc_handoffs
//     Apply any pending or applied GC handoffs (updates in-memory extent index).
//     Returns "ok <n>" where n is the number of handoffs processed.
//
//   redact <ulid>
//     Hole-punch hash-dead DATA entries in pending/<ulid> in place so
//     deleted data never leaves the host via S3 upload. Idempotent;
//     no-op when the segment has no hash-dead entries.
//     Returns "ok".
//
//   snapshot_manifest <snap_ulid>
//     Sign and write snapshots/<snap_ulid>.manifest (listing every
//     segment ULID in index/ at the moment of the call) followed by
//     the snapshots/<snap_ulid> marker. Called by the coordinator at
//     the end of the inline drain step of a coordinator-driven
//     snapshot. Returns "ok".
//
//   reclaim
//     Run a full alias-merge extent reclamation pass: scan for bloated-hash
//     candidates, then process each via the three-phase primitive. Uses
//     default thresholds. Returns
//     "ok <candidates_scanned> <runs_rewritten> <bytes_rewritten> <discarded>".
//
//   connected
//     Returns "ok true" if an NBD client is currently connected, "ok false"
//     otherwise. Always "ok false" for IPC-only volumes.
//
//   shutdown
//     Flush WAL and exit cleanly.  Returns "ok" then terminates the process.
//     The supervisor restarts the volume, picking up any updated config files.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use tracing::{debug, info};

use elide_core::actor::VolumeHandle;

/// Start the control socket server for `fork_dir`.
///
/// Binds `<fork_dir>/control.sock`, removes any stale socket from a previous
/// run first.  Spawns a dedicated thread that serves one request per
/// connection until the listener is closed.  The socket file is removed when
/// the thread exits.
///
/// `nbd_connected` is shared with the NBD accept loop: `true` while a client
/// is connected, `false` otherwise. IPC-only volumes pass a permanently-false
/// flag.
///
/// The handle is cloned onto the server thread; the caller retains its own
/// clone.
pub fn start(
    fork_dir: &Path,
    handle: VolumeHandle,
    nbd_connected: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let socket_path = fork_dir.join("control.sock");
    // Remove stale socket from a previous (crashed) run.
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    thread::Builder::new()
        .name("control-server".into())
        .spawn(move || run(listener, socket_path, handle, nbd_connected))
        .map_err(std::io::Error::other)?;
    Ok(())
}

fn run(
    listener: UnixListener,
    socket_path: PathBuf,
    handle: VolumeHandle,
    nbd_connected: Arc<AtomicBool>,
) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream, &handle, &nbd_connected),
            Err(_) => break,
        }
    }
    let _ = std::fs::remove_file(&socket_path);
}

fn handle_connection(
    stream: std::os::unix::net::UnixStream,
    handle: &VolumeHandle,
    nbd_connected: &AtomicBool,
) {
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        // Caller disconnected before sending a request — not an error.
        return;
    }
    let line = line.trim();
    if line.is_empty() {
        let _ = writeln!(writer, "err empty request");
        return;
    }
    if line == "flush" {
        match handle.flush() {
            Ok(()) => {
                let _ = writeln!(writer, "ok");
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
            }
        }
    } else if line == "promote_wal" {
        match handle.promote_wal() {
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
    } else if line == "delta_repack" {
        match handle.delta_repack_post_snapshot() {
            Ok(s) => {
                let _ = writeln!(
                    writer,
                    "ok {} {} {} {} {}",
                    s.segments_scanned,
                    s.segments_rewritten,
                    s.entries_converted,
                    s.original_body_bytes,
                    s.delta_body_bytes,
                );
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
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
    } else if line == "apply_gc_handoffs" {
        match handle.apply_gc_handoffs() {
            Ok(n) => {
                let _ = writeln!(writer, "ok {n}");
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
            }
        }
    } else if let Some(ulid_str) = line.strip_prefix("redact ") {
        match ulid::Ulid::from_string(ulid_str.trim()) {
            Ok(ulid) => match handle.redact_segment(ulid) {
                Ok(()) => {
                    let _ = writeln!(writer, "ok");
                }
                Err(e) => {
                    let _ = writeln!(writer, "err {e}");
                }
            },
            Err(_) => {
                let _ = writeln!(writer, "err invalid ulid: {ulid_str}");
            }
        }
    } else if let Some(ulid_str) = line.strip_prefix("snapshot_manifest ") {
        match ulid::Ulid::from_string(ulid_str.trim()) {
            Ok(ulid) => match handle.sign_snapshot_manifest(ulid) {
                Ok(()) => {
                    let _ = writeln!(writer, "ok");
                }
                Err(e) => {
                    let _ = writeln!(writer, "err {e}");
                }
            },
            Err(_) => {
                let _ = writeln!(writer, "err invalid ulid: {ulid_str}");
            }
        }
    } else if let Some(ulid_str) = line.strip_prefix("promote ") {
        match ulid::Ulid::from_string(ulid_str.trim()) {
            Ok(ulid) => match handle.promote_segment(ulid) {
                Ok(()) => {
                    let _ = writeln!(writer, "ok");
                }
                Err(e) => {
                    let _ = writeln!(writer, "err {e}");
                }
            },
            Err(_) => {
                let _ = writeln!(writer, "err invalid ulid: {ulid_str}");
            }
        }
    } else if let Some(ulid_str) = line.strip_prefix("finalize_gc_handoff ") {
        match ulid::Ulid::from_string(ulid_str.trim()) {
            Ok(ulid) => match handle.finalize_gc_handoff(ulid) {
                Ok(()) => {
                    let _ = writeln!(writer, "ok");
                }
                Err(e) => {
                    let _ = writeln!(writer, "err {e}");
                }
            },
            Err(_) => {
                let _ = writeln!(writer, "err invalid ulid: {ulid_str}");
            }
        }
    } else if line == "reclaim" {
        // End-to-end alias-merge pass over the whole volume:
        //   1. Scan the current snapshot for bloated-hash candidates.
        //   2. Reclaim each candidate (most-wasteful-first) via the
        //      three-phase primitive.
        // No args: the volume uses default thresholds. No locks taken
        // by this handler thread — the primitive does its own
        // short-critical-section work on the actor thread.
        //
        // Returns "ok <candidates_scanned> <runs_rewritten> <bytes_rewritten> <discarded>".
        let candidates =
            handle.reclaim_candidates(elide_core::volume::ReclaimThresholds::default());
        let scanned = candidates.len();
        info!("[reclaim] scan found {scanned} candidate(s)");
        let mut total_runs: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut discarded: u64 = 0;
        let mut io_err: Option<std::io::Error> = None;
        for c in candidates {
            debug!(
                "[reclaim] candidate lba={} len={} dead_blocks={} live_blocks={} stored_bytes={}",
                c.start_lba, c.lba_length, c.dead_blocks, c.live_blocks, c.stored_bytes,
            );
            match handle.reclaim_alias_merge(c.start_lba, c.lba_length) {
                Ok(outcome) => {
                    if outcome.discarded {
                        discarded += 1;
                        debug!(
                            "[reclaim] candidate lba={} discarded (concurrent mutation)",
                            c.start_lba
                        );
                    } else {
                        total_runs += outcome.runs_rewritten as u64;
                        total_bytes += outcome.bytes_rewritten;
                        debug!(
                            "[reclaim] candidate lba={} committed runs={} bytes={}",
                            c.start_lba, outcome.runs_rewritten, outcome.bytes_rewritten
                        );
                    }
                }
                Err(e) => {
                    io_err = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = io_err {
            let _ = writeln!(writer, "err {e}");
        } else {
            info!(
                "[reclaim] done: scanned={scanned} runs_rewritten={total_runs} \
                 bytes_rewritten={total_bytes} discarded={discarded}"
            );
            let _ = writeln!(
                writer,
                "ok {scanned} {total_runs} {total_bytes} {discarded}"
            );
        }
    } else if line == "connected" {
        let connected = nbd_connected.load(Ordering::Relaxed);
        let _ = writeln!(writer, "ok {connected}");
    } else if line == "shutdown" {
        match handle.promote_wal() {
            Ok(()) => {
                let _ = writeln!(writer, "ok");
            }
            Err(e) => {
                let _ = writeln!(writer, "err {e}");
                return;
            }
        }
        std::process::exit(0);
    } else {
        let _ = writeln!(writer, "err unknown op: {line}");
    }
}
