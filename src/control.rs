// Volume control socket server.
//
// Listens on <fork_dir>/control.sock for coordinator requests.
// Each connection carries one typed JSON request and one typed JSON
// reply (both as single NDJSON lines), then closes.
//
// Wire format: see `elide_core::ipc::Envelope` and
// `elide_core::volume_ipc::VolumeRequest` for the type definitions.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use serde::Serialize;
use tracing::{debug, info};

use elide_core::actor::VolumeClient;
use elide_core::ipc::{Envelope, IpcError};
use elide_core::volume::ReclaimThresholds;
use elide_core::volume_ipc::{
    ApplyGcHandoffsReply, CompactionReply, ConnectedReply, DeltaRepackReply, GcCheckpointReply,
    ReclaimReply, VolumeRequest,
};

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
    handle: VolumeClient,
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
    handle: VolumeClient,
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
    handle: &VolumeClient,
    nbd_connected: &AtomicBool,
) {
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        // Caller disconnected before sending a request — not an error.
        return;
    }
    let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
    if trimmed.is_empty() {
        let _ = write_envelope::<()>(
            &mut writer,
            &Envelope::err(IpcError::bad_request("empty request")),
        );
        return;
    }
    let request: VolumeRequest = match serde_json::from_str(trimmed) {
        Ok(r) => r,
        Err(e) => {
            let _ = write_envelope::<()>(
                &mut writer,
                &Envelope::err(IpcError::bad_request(format!("invalid request: {e}"))),
            );
            return;
        }
    };

    // `Shutdown` is the only verb that exits the process; the rest
    // dispatch through `dispatch` below, which writes the reply and
    // returns.
    if matches!(request, VolumeRequest::Shutdown) {
        match handle.promote_wal() {
            Ok(()) => {
                let _ = write_envelope(&mut writer, &Envelope::<()>::ok(()));
            }
            Err(e) => {
                let _ = write_envelope::<()>(
                    &mut writer,
                    &Envelope::err(IpcError::internal(e.to_string())),
                );
                return;
            }
        }
        std::process::exit(0);
    }

    dispatch(request, handle, nbd_connected, &mut writer);
}

fn dispatch(
    request: VolumeRequest,
    handle: &VolumeClient,
    nbd_connected: &AtomicBool,
    writer: &mut impl Write,
) {
    match request {
        VolumeRequest::Flush => write_unit(writer, handle.flush()),
        VolumeRequest::PromoteWal => write_unit(writer, handle.promote_wal()),
        VolumeRequest::SweepPending => match handle.sweep_pending() {
            Ok(stats) => {
                let _ = write_envelope(writer, &Envelope::ok(CompactionReply { stats }));
            }
            Err(e) => {
                let _ = write_envelope::<CompactionReply>(
                    writer,
                    &Envelope::err(IpcError::internal(e.to_string())),
                );
            }
        },
        VolumeRequest::Repack { min_live_ratio } => match handle.repack(min_live_ratio) {
            Ok(stats) => {
                let _ = write_envelope(writer, &Envelope::ok(CompactionReply { stats }));
            }
            Err(e) => {
                let _ = write_envelope::<CompactionReply>(
                    writer,
                    &Envelope::err(IpcError::internal(e.to_string())),
                );
            }
        },
        VolumeRequest::DeltaRepack => match handle.delta_repack_post_snapshot() {
            Ok(stats) => {
                let _ = write_envelope(writer, &Envelope::ok(DeltaRepackReply { stats }));
            }
            Err(e) => {
                let _ = write_envelope::<DeltaRepackReply>(
                    writer,
                    &Envelope::err(IpcError::internal(e.to_string())),
                );
            }
        },
        VolumeRequest::GcCheckpoint => match handle.gc_checkpoint() {
            Ok(gc_ulid) => {
                let _ = write_envelope(writer, &Envelope::ok(GcCheckpointReply { gc_ulid }));
            }
            Err(e) => {
                let _ = write_envelope::<GcCheckpointReply>(
                    writer,
                    &Envelope::err(IpcError::internal(e.to_string())),
                );
            }
        },
        VolumeRequest::ApplyGcHandoffs => match handle.apply_gc_handoffs() {
            Ok(processed) => {
                let _ = write_envelope(
                    writer,
                    &Envelope::ok(ApplyGcHandoffsReply {
                        processed: u32::try_from(processed).unwrap_or(u32::MAX),
                    }),
                );
            }
            Err(e) => {
                let _ = write_envelope::<ApplyGcHandoffsReply>(
                    writer,
                    &Envelope::err(IpcError::internal(e.to_string())),
                );
            }
        },
        VolumeRequest::Redact { segment_ulid } => {
            write_unit(writer, handle.redact_segment(segment_ulid))
        }
        VolumeRequest::SnapshotManifest { snap_ulid } => {
            write_unit(writer, handle.sign_snapshot_manifest(snap_ulid))
        }
        VolumeRequest::Promote { segment_ulid } => {
            write_unit(writer, handle.promote_segment(segment_ulid))
        }
        VolumeRequest::FinalizeGcHandoff { gc_ulid } => {
            write_unit(writer, handle.finalize_gc_handoff(gc_ulid))
        }
        VolumeRequest::Reclaim { cap } => dispatch_reclaim(cap, handle, writer),
        VolumeRequest::Connected => {
            let connected = nbd_connected.load(Ordering::Relaxed);
            let _ = write_envelope(writer, &Envelope::ok(ConnectedReply { connected }));
        }
        VolumeRequest::Shutdown => unreachable!("shutdown is handled inline above"),
    }
}

fn dispatch_reclaim(cap: Option<u32>, handle: &VolumeClient, writer: &mut impl Write) {
    // End-to-end alias-merge pass over the whole volume:
    //   1. Scan the current snapshot for bloated-hash candidates.
    //   2. Reclaim each candidate (most-wasteful-first) via the
    //      three-phase primitive.
    //
    // `cap`: None → unlimited; Some(n) → process at most n.
    let candidates = handle.reclaim_candidates(ReclaimThresholds::default());
    let scanned = candidates.len();
    if scanned > 0 {
        info!("[reclaim] scan found {scanned} candidate(s), cap={cap:?}");
    } else {
        debug!("[reclaim] scan found 0 candidate(s), cap={cap:?}");
    }
    let to_process: Vec<_> = match cap {
        Some(n) => candidates.into_iter().take(n as usize).collect(),
        None => candidates,
    };
    let mut total_runs: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut discarded: u32 = 0;
    let mut io_err: Option<std::io::Error> = None;
    for c in to_process {
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
        let _ = write_envelope::<ReclaimReply>(
            writer,
            &Envelope::err(IpcError::internal(e.to_string())),
        );
        return;
    }
    if total_runs > 0 || discarded > 0 {
        info!(
            "[reclaim] done: scanned={scanned} runs_rewritten={total_runs} \
             bytes_rewritten={total_bytes} discarded={discarded}"
        );
    } else {
        debug!(
            "[reclaim] done: scanned={scanned} runs_rewritten={total_runs} \
             bytes_rewritten={total_bytes} discarded={discarded}"
        );
    }
    let _ = write_envelope(
        writer,
        &Envelope::ok(ReclaimReply {
            candidates_scanned: u32::try_from(scanned).unwrap_or(u32::MAX),
            runs_rewritten: total_runs,
            bytes_rewritten: total_bytes,
            discarded,
        }),
    );
}

fn write_unit(writer: &mut impl Write, result: std::io::Result<()>) {
    match result {
        Ok(()) => {
            let _ = write_envelope(writer, &Envelope::<()>::ok(()));
        }
        Err(e) => {
            let _ = write_envelope::<()>(writer, &Envelope::err(IpcError::internal(e.to_string())));
        }
    }
}

fn write_envelope<T: Serialize>(
    writer: &mut impl Write,
    envelope: &Envelope<T>,
) -> std::io::Result<()> {
    let mut buf = serde_json::to_vec(envelope)?;
    buf.push(b'\n');
    writer.write_all(&buf)?;
    writer.flush()?;
    Ok(())
}
