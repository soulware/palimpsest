// Coordinator daemon: watches the configured data directory, discovers volumes,
// drains pending segments to S3, runs GC, and supervises volume processes.
//
// Architecture:
//   - A root scanner runs every `supervisor.scan_interval`, walking configured root
//     directories to find volume directories. Each newly-discovered volume gets:
//       - a run_volume_tasks task: drain + GC, sequential within each tick
//       - a supervisor task (if `serve.toml` exists): spawns and restarts
//         `elide serve-volume`
//   - The first scan fires immediately at startup to clear any backlog and
//     adopt any volume processes already running.
//   - Shutdown: SIGINT and SIGTERM both run the defensive teardown — abort
//     coordinator tasks and drain the JoinSet, but leave volume children
//     running. Volumes are spawned with setsid so they survive the
//     coordinator's exit and are re-adopted by the next coordinator
//     instance. The drain has a bounded timeout so cancellation propagates
//     while the tokio runtime is still healthy; skipping it can panic the
//     time driver if a task (e.g. an `object_store` retry mid-backoff)
//     registers a `tokio::time` timer after runtime shutdown begins.
//     Full teardown (SIGTERM the volume children) is reachable only via
//     the explicit `Shutdown { keep_volumes: false }` IPC sent by
//     `elide coord stop`.
//
// Directory layout expected:
//   <data_dir>/by_id/<ulid>/    — one directory per volume (ULID name = S3 prefix)
//   <data_dir>/by_name/<name>   — symlinks into by_id/ for human navigation

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use elide_coordinator::stores::ScopedStores;
use humantime_serde::re::humantime;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::config::CoordinatorConfig;
use crate::credential::{SharedKeyPassthrough, set_credential_issuer};
use crate::import;
use crate::inbound;
use crate::supervisor;
use elide_coordinator::identity::CoordinatorIdentity;
use elide_coordinator::volume_state::{IMPORTING_FILE, PID_FILE};
use elide_coordinator::{
    EvictRegistry, PrefetchTracker, SnapshotLockRegistry, new_prefetch_tracker,
    new_snapshot_lock_registry, register_prefetch_or_get, replace_prefetch,
};

pub async fn run(config: CoordinatorConfig, stores: Arc<dyn ScopedStores>) -> Result<()> {
    let drain_interval = config.supervisor.drain_interval;
    let scan_interval = config.supervisor.scan_interval;
    let gc_config = config.gc.clone();
    // Install the configured multipart chunk size as a process-global so
    // SegmentUploader (deep in the upload path) can read it without us
    // threading the value through every layer between here and there.
    elide_coordinator::upload::set_part_size_bytes(config.store.multipart_part_size_bytes());
    // Same pattern for the sibling-binary paths used by the supervisor
    // (`elide serve-volume`) and the import path (`elide-import`).
    elide_coordinator::bins::set_elide_bin(config.elide_bin.clone());
    elide_coordinator::bins::set_elide_import_bin(config.elide_import_bin.clone());
    // And for the `[store]` config vended over `GetStoreConfig` to
    // spawned volume subprocesses. Box::leak keeps the value as a
    // `&'static StoreSection` so the IPC handler reads it without any
    // threading from `daemon::run`.
    elide_coordinator::config::set_store_config(config.store.clone());
    let socket_path = config.resolved_socket_path();
    elide_coordinator::config::set_coordinator_socket_path(socket_path.clone());
    let data_dir = Arc::new(config.data_dir.clone());
    let child_env: supervisor::ChildEnv = {
        let mut env = config.store.child_env();
        // Let spawned volume subprocesses call back into us over the IPC
        // socket to request store config + credentials, rather than
        // inheriting AWS_* via the parent's env. This is the path that
        // macaroon-scoped credentials will flow over later.
        env.push((
            "ELIDE_COORDINATOR_SOCKET",
            config.resolved_socket_path().to_string_lossy().into_owned(),
        ));
        Arc::new(env)
    };
    std::fs::create_dir_all(data_dir.as_ref())
        .with_context(|| format!("creating data_dir: {}", data_dir.display()))?;
    std::fs::create_dir_all(data_dir.join("by_id"))
        .with_context(|| format!("creating by_id dir under {}", data_dir.display()))?;
    std::fs::create_dir_all(data_dir.join("by_name"))
        .with_context(|| format!("creating by_name dir under {}", data_dir.display()))?;

    // Load (or generate on first start) the coordinator's Ed25519
    // keypair, derive `coordinator_id` from the public half, derive the
    // macaroon MAC root in-memory from the private half, and publish
    // the pubkey to S3 so other coordinators can verify signatures by
    // `coordinator_id` lookup.
    let identity = Arc::new(CoordinatorIdentity::load_or_generate(&config.data_dir)?);
    info!(
        "[coordinator] coordinator_id: {}",
        identity.coordinator_id_str()
    );
    // Publishing coordinator.pub is a coordinator-wide write
    // (`coordinator/<id>.pub`), not a per-volume op.
    let coord_wide = stores.writer();
    if let Err(e) = identity.publish_pub(coord_wide.as_ref()).await {
        return Err(anyhow::anyhow!("publish coordinator.pub: {e}"));
    }
    // If peer-fetch is configured, advertise our endpoint at
    // `coordinators/<id>/peer-endpoint.toml` so other coordinators
    // can find us during handoff discovery. Sibling write to
    // `coordinator.pub`. Absence of `peer_fetch.port` keeps the whole
    // mechanism off — no server, no advertisement, no peer tier.
    let mut peer_fetch_handle: Option<elide_coordinator::tasks::PeerFetchHandle> = None;
    let mut peer_fetch_server: Option<(
        std::net::SocketAddr,
        elide_peer_fetch::server::ServerContext,
    )> = None;
    if let Some(port) = config.peer_fetch.port {
        let host = config.peer_fetch.advertised_host(identity.hostname());
        let endpoint = elide_peer_fetch::PeerEndpoint::new(host, port);
        if let Err(e) = endpoint
            .publish(coord_wide.as_ref(), identity.coordinator_id_str())
            .await
        {
            return Err(anyhow::anyhow!("publish peer-endpoint.toml: {e}"));
        }
        info!(
            "[coordinator] peer-fetch endpoint advertised: {} (bind {})",
            endpoint.url(),
            config.peer_fetch.bind_addr(),
        );
        // Build a single peer-fetch client to share across all
        // per-volume tasks. The client pools HTTP/2 connections
        // internally and signs tokens on demand via the
        // `TokenSigner` impl on `CoordinatorIdentity`.
        let signer: std::sync::Arc<dyn elide_peer_fetch::TokenSigner> = identity.clone();
        match elide_peer_fetch::PeerFetchClient::new(signer) {
            Ok(client) => {
                peer_fetch_handle = Some(elide_coordinator::tasks::PeerFetchHandle { client });
            }
            Err(e) => return Err(anyhow::anyhow!("build peer-fetch client: {e}")),
        }
        // Server context for the inbound HTTP listener. The auth state
        // reads `coordinator.pub` and the ETag-conditional
        // `names/<name>` (steps 2–3, the gap-free force-release fence)
        // via the read-only `coord-base` credential — the exposed
        // verifier holds no write-capable key. Lineage (step 4) is
        // verified against a
        // separate local store rooted at `data_dir`: the peer walks the
        // signed `by_id/<vol>/volume.{provenance,pub}` it already holds
        // for every fork it serves — no S3 read, no credential. A fork
        // it doesn't serve has no local chain, so the walk fails closed
        // and the requester falls back to S3. The route handler
        // resolves payload files under `data_dir/by_id/...` (step 5).
        let bind_addr_str = format!("{}:{}", config.peer_fetch.bind_addr(), port);
        let addr: std::net::SocketAddr = bind_addr_str
            .parse()
            .map_err(|e| anyhow::anyhow!("parsing peer-fetch bind {bind_addr_str:?}: {e}"))?;
        let lineage_store: std::sync::Arc<dyn object_store::ObjectStore> = std::sync::Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(data_dir.as_ref()).map_err(
                |e| anyhow::anyhow!("peer-fetch lineage store at {:?}: {e}", data_dir.as_ref()),
            )?,
        );
        let auth =
            elide_peer_fetch::auth::AuthState::new(stores.peer_verifier_store(), lineage_store);
        let ctx = elide_peer_fetch::server::ServerContext::new(auth, data_dir.as_ref().clone());
        peer_fetch_server = Some((addr, ctx));
    }
    // Install the peer-fetch handle as a process-global so per-volume
    // tasks, the claim orchestrator, and the IPC `Register` handler
    // can read it without us threading the value through. `None` when
    // peer-fetch is unconfigured.
    elide_coordinator::tasks::set_peer_fetch_handle(peer_fetch_handle);

    // Credential issuer. `[mint]` routes per-volume RO issuance
    // through the external mint service (docs/design-mint.md
    // § "Coordinator configuration"); its absence keeps the shared-key
    // downgrade where every volume gets the coordinator's own AWS_*
    // key.
    let credentialer: Option<std::sync::Arc<dyn crate::credential::Credentialer>> =
        match &config.mint {
            Some(mint_cfg) => {
                mint_cfg.validate()?;
                let credentialer: std::sync::Arc<dyn crate::credential::Credentialer> =
                    std::sync::Arc::new(crate::mint_client::MintCredentialer::new(
                        mint_cfg,
                        config.data_dir.clone(),
                        identity.clone(),
                    ));
                set_credential_issuer(crate::mint_client::MintCredentialIssuer::new(
                    credentialer.clone(),
                    config.data_dir.clone(),
                ));
                info!(
                    "[coordinator] credential issuer: external mint service ({})",
                    mint_cfg.url
                );
                Some(credentialer)
            }
            None => {
                set_credential_issuer(SharedKeyPassthrough::new_with_warning());
                None
            }
        };

    info!(
        "[coordinator] data_dir: {}; drain every {}, scan every {}; elide bin: {}",
        data_dir.display(),
        humantime::format_duration(drain_interval),
        humantime::format_duration(scan_interval),
        elide_coordinator::bins::elide_bin().display(),
    );

    // Reconcile by_name/ against by_id/: remove symlinks whose target no longer
    // exists, add missing symlinks for volumes that have a volume.name file.
    reconcile_by_name(&data_dir);

    // Clean up any stale import locks left by a previous coordinator run.
    import::cleanup_stale_locks(&config.data_dir);

    // Reconcile ublk kernel devices against on-disk bindings. Under
    // shutdown-park, daemons leave devices QUIESCED for recovery; this
    // sweep handles the residual cases where the volume directory has
    // been removed out-of-band, the host has rebooted (sysfs cleared
    // but the bound dev_id in volume.toml remains), or an operator
    // manually deleted a device.
    crate::ublk_sweep::reconcile(&data_dir).await;

    // Import job registry: tracks running and recently-completed import jobs.
    let import_registry = import::new_registry();

    // Fork job registry: in-memory tracking of in-flight `fork-start`
    // orchestrations. Empty after a coordinator restart — partial
    // forks that didn't reach `fork-create` rely on `fork_create_op`'s
    // stale-symlink cleanup on retry.
    let fork_registry = crate::fork::new_registry();

    // Claim job registry: same shape as `fork_registry` but for the
    // `claim-start` flow's foreign-content branch.
    let claim_registry = crate::claim::new_registry();

    // Fetch job registry: tracks in-flight `volume fetch` jobs that
    // warm a foreign volume's local cache without claiming the name.
    let fetch_registry = crate::fetch::new_registry();

    // Per-fork eviction channel registry.
    let evict_registry: EvictRegistry = Arc::new(std::sync::Mutex::new(HashMap::new()));

    // Per-fork snapshot lock registry (shared by the snapshot inbound handler
    // and every per-volume tick loop via try_lock).
    let snapshot_locks: SnapshotLockRegistry = new_snapshot_lock_registry();

    // Per-fork prefetch tracker. Read by the `await-prefetch` IPC; written by
    // the daemon on volume discovery (entry inserted before `run_volume_tasks`
    // is spawned, so the volume binary's first IPC always finds an entry).
    let prefetch_tracker: PrefetchTracker = new_prefetch_tracker();

    // Tracks each known volume's directory inode and the abort handle
    // for its supervisor task (when supervised). Prune drops the entry
    // when *either* signal goes stale:
    //
    //   - Path no longer exists — `volume remove` tore the dir down.
    //   - Inode changed — recreated by hydrate/import at the same path.
    //   - Supervisor task finished — saw `fork_dir.exists() == false`
    //     and exited, regardless of whether inode coincidentally
    //     matches (ext4 routinely reuses freed inodes for the next
    //     allocation, so `rm -rf` followed by `mkdir` at the same
    //     path can produce identical inode numbers).
    //
    // The supervisor-handle check is what makes `volume remove`
    // followed by `volume start` reliably re-spawn the daemon even
    // when the kernel reuses the inode. Without it, the stale entry
    // survives both the path check and the inode check, and the
    // first-discovery branch is skipped — leaving the volume
    // perpetually "stopped" despite a successful `start`.
    //
    // `supervised` is also retained as a bookkeeping flag for the
    // readonly→writable transition path: a volume first seen with
    // `volume.readonly` present (e.g. a partially-hydrated remote-
    // owned fork) gets per-volume tasks but no supervisor; a
    // subsequent `volume start` that strips the marker must trigger
    // supervisor spawn on the next scan tick.
    struct KnownVolume {
        inode: u64,
        supervised: bool,
        supervisor: Option<tokio::task::AbortHandle>,
    }
    let mut known: HashMap<PathBuf, KnownVolume> = HashMap::new();
    // Single JoinSet for all spawned tasks (per-volume drain/GC,
    // supervisors, inbound socket, reaper). On Ctrl-C we abort_all and
    // drain so cancellation propagates while the runtime is still alive
    // — see the module-level shutdown note.
    let mut tasks: JoinSet<()> = JoinSet::new();

    // Inbound socket server: spawn into `tasks` so it is aborted and
    // drained on Ctrl-C alongside the per-volume tasks.
    {
        let ctx = inbound::IpcContext {
            data_dir: data_dir.clone(),
            registry: import_registry.clone(),
            fork_registry: fork_registry.clone(),
            fetch_registry: fetch_registry.clone(),
            claim_registry: claim_registry.clone(),
            evict_registry: evict_registry.clone(),
            snapshot_locks: snapshot_locks.clone(),
            prefetch_tracker: prefetch_tracker.clone(),
            stores: stores.clone(),
            identity: identity.clone(),
            credentialer: credentialer.clone(),
        };
        tasks.spawn(async move {
            inbound::serve(&socket_path, ctx).await;
        });
    }

    // Peer-fetch HTTP server: bound only when `[peer_fetch].port` is
    // set in coordinator.toml. Shares the same `JoinSet` so Ctrl-C
    // aborts and drains it alongside the rest.
    if let Some((addr, ctx)) = peer_fetch_server.take() {
        tasks.spawn(async move {
            if let Err(e) = elide_peer_fetch::server::serve(addr, ctx).await {
                warn!("[coordinator] peer-fetch server exited: {e}");
            }
        });
    }

    // Retention reaping is folded into the per-volume tick loop
    // (`gc_cycle::GcCycleOrchestrator::reap_expired`); see
    // `docs/design-segment-index.md` *Reaper fold*. No coordinator-
    // wide reaper task — the per-volume loop is the sole writer of
    // its volume's HEAD and the sole DELETEr of its superseded
    // inputs.

    let mut scan_tick = tokio::time::interval(scan_interval);
    scan_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the first tick so the loop body runs immediately at startup.
    scan_tick.tick().await;

    // Defensive signal policy: SIGINT and SIGTERM both leave the volume
    // children running (the rolling-upgrade path). Volumes are expensive
    // state — mounted devices, in-flight I/O, populated caches — and a
    // coordinator going down for any reason (panic, OOM, supervisor
    // bounce, `systemctl restart`) must not drag the data plane with
    // it. The "tear down volumes too" path is the explicit
    // `Shutdown { keep_volumes: false }` IPC sent by
    // `elide coord stop --stop-volumes`.
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    loop {
        // Prune volumes that have been deleted since we last saw them so that
        // if the same ULID directory is recreated (e.g. by `volume remote pull`
        // after a `volume delete`), it will be discovered and processed again.
        // Prune volumes whose path no longer exists OR whose directory inode
        // changed (deleted and recreated at the same path with the same ULID).
        known.retain(|p, k| {
            // Drop on any of:
            //   - path gone (metadata fails)
            //   - inode changed (delete-and-recreate, distinct inode)
            //   - supervisor exited (saw dir gone and broke out of
            //     its loop, even if inode was coincidentally reused)
            let path_ok = p.metadata().map(|m| m.ino() == k.inode).unwrap_or(false);
            let supervisor_alive = k
                .supervisor
                .as_ref()
                .map(|h| !h.is_finished())
                .unwrap_or(true);
            path_ok && supervisor_alive
        });

        let volumes = discover_volumes(&data_dir);
        for vol_dir in volumes {
            // Skip volumes in the write phase of an import (volume.importing present,
            // no control.sock). The serve phase (both present) is handled normally
            // — run_volume_tasks drains pending/ via promote IPC.
            if vol_dir.join(IMPORTING_FILE).exists() && !vol_dir.join("control.sock").exists() {
                continue;
            }
            let vol_ino = vol_dir.metadata().map(|m| m.ino()).unwrap_or(0);
            let readonly_now = vol_dir.join("volume.readonly").exists();
            // Look up *without* inserting first — we want to preserve
            // the existing supervisor handle on no-change ticks (the
            // prune step already dropped stale entries whose
            // supervisor died). Only the spawn-fresh / transition
            // paths below mutate the entry.
            let prev_ino = known.get(&vol_dir).map(|k| k.inode);
            let prev_supervised = known.get(&vol_dir).map(|k| k.supervised).unwrap_or(false);
            // Two trigger conditions for spawning per-volume tasks:
            //   - first discovery (`prev_ino` is None) — may pick up a
            //     prefetch tracker entry that `fork_create_op` already
            //     registered, via the idempotent `register_prefetch_or_get`.
            //   - inode-change rediscovery (delete-and-recreate at the
            //     same ULID, e.g. `volume delete` followed by `remote
            //     pull`) — must force-replace the tracker entry so any
            //     pre-existing subscribers from the previous incarnation
            //     see `changed() -> Err` and retry.
            let force_replace = matches!(prev_ino, Some(old) if old != vol_ino);
            if prev_ino.is_none() || force_replace {
                let label = volume_label(&vol_dir);
                info!("[coordinator] discovered volume: {label}");

                // Reconcile the local volume.stopped marker against the
                // bucket-side names/<name> state so the supervisor sees
                // a consistent picture before launching. S3 wins; this is
                // a best-effort recovery from prior drift (e.g. a `volume
                // stop` whose S3 update succeeded but local marker write
                // failed, or vice versa). Skipped for readonly volumes
                // (imported bases have no name in the portable sense).
                if !vol_dir.join("volume.readonly").exists()
                    && let Some(name) = elide_coordinator::tasks::read_volume_name(&vol_dir)
                {
                    // Reads/writes names/<name>: coordinator-wide.
                    stores
                        .name_claims()
                        .reconcile_marker(&vol_dir, &name, identity.coordinator_id_str())
                        .await;
                }

                let (evict_tx, evict_rx) = tokio::sync::mpsc::channel(4);
                evict_registry
                    .lock()
                    .expect("evict registry poisoned")
                    .insert(vol_dir.clone(), evict_tx);

                // Obtain the prefetch tracker sender BEFORE spawning the
                // per-volume task and BEFORE the supervisor (which spawns the
                // volume binary). Ordering: `await-prefetch` from the volume
                // process or CLI always finds an entry; never races a missing
                // key. For first discovery use the idempotent or-get (so we
                // pick up any pre-registration done by `fork_create_op`); for
                // inode-change rediscovery force-replace so prior subscribers
                // see `Err`.
                let prefetch_tx = match vol_dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| ulid::Ulid::from_string(s).ok())
                {
                    Some(u) => {
                        if force_replace {
                            Some(replace_prefetch(&prefetch_tracker, u))
                        } else {
                            Some(register_prefetch_or_get(&prefetch_tracker, u))
                        }
                    }
                    None => {
                        warn!(
                            "[coordinator] volume dir {} is not ULID-named; skipping prefetch tracking",
                            vol_dir.display()
                        );
                        None
                    }
                };
                let prefetch_tx = prefetch_tx.unwrap_or_else(|| {
                    // Fall back to a discarded sender so the task signature
                    // stays uniform; nobody can subscribe to it via the
                    // tracker because we never registered the entry.
                    Arc::new(tokio::sync::watch::channel(None).0)
                });

                // Per-volume drain / GC / prefetch loop runs against
                // that volume's `coord-data` handle. A directory whose
                // name is not a ULID is not a real volume; it falls
                // back to the coordinator-wide writer handle.
                let vol_store = match vol_dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| ulid::Ulid::from_string(s).ok())
                {
                    Some(u) => stores.data_for_volume(&u),
                    None => stores.writer(),
                };
                tasks.spawn(elide_coordinator::tasks::run_volume_tasks(
                    vol_dir.clone(),
                    vol_store,
                    stores.event_journal_ro(),
                    drain_interval,
                    gc_config.clone(),
                    evict_rx,
                    snapshot_locks.clone(),
                    prefetch_tx,
                    prefetch_tracker.clone(),
                ));

                // Readonly volumes (imported bases) have no live process —
                // skip supervision so we don't crash-loop on serve-volume.
                let supervisor_handle = if !readonly_now {
                    Some(tasks.spawn(supervisor::supervise(
                        vol_dir.clone(),
                        data_dir.as_ref().clone(),
                        child_env.clone(),
                    )))
                } else {
                    None
                };
                known.insert(
                    vol_dir,
                    KnownVolume {
                        inode: vol_ino,
                        supervised: !readonly_now,
                        supervisor: supervisor_handle,
                    },
                );
            } else if !prev_supervised && !readonly_now {
                // Known volume that transitioned readonly→writable since
                // last tick. This happens when a remote-owned fork was
                // first discovered with `volume.readonly` still stamped
                // (partial hydrate left over from an earlier failed
                // start) and a later successful `volume start` stripped
                // the marker via `hydrate_remote_owned`. Per-volume
                // tasks were spawned at first discovery; spawn the
                // supervisor now so the daemon actually comes up.
                let label = volume_label(&vol_dir);
                info!("[coordinator] supervising newly-writable volume: {label}");
                let handle = tasks.spawn(supervisor::supervise(
                    vol_dir.clone(),
                    data_dir.as_ref().clone(),
                    child_env.clone(),
                ));
                if let Some(k) = known.get_mut(&vol_dir) {
                    k.supervised = true;
                    k.supervisor = Some(handle);
                }
            }
            // else: same inode and already supervised — leave the
            // entry as-is to preserve the existing supervisor handle.
        }

        // Reap any tasks that have exited (e.g. fork directory removed).
        while tasks.try_join_next().is_some() {}

        // Whether shutdown was requested, and whether to leave volume
        // children running. `Some(true)` is the rolling-upgrade path
        // (skip the SIGTERM step); `Some(false)` is the full teardown.
        let mut keep_volumes: Option<bool> = None;
        tokio::select! {
            _ = scan_tick.tick() => {}
            _ = crate::rescan::wait() => {
                info!("[coordinator] rescan triggered via socket");
            }
            _ = sigint.recv() => {
                info!("[coordinator] shutting down (SIGINT, keep_volumes)");
                keep_volumes = Some(true);
            }
            _ = sigterm.recv() => {
                info!("[coordinator] shutting down (SIGTERM, keep_volumes)");
                keep_volumes = Some(true);
            }
            kv = crate::shutdown::wait() => {
                info!(
                    "[coordinator] shutting down via IPC (keep_volumes = {kv})"
                );
                keep_volumes = Some(kv);
            }
        }

        if let Some(keep) = keep_volumes {
            // Abort supervisor and drain tasks first so they cannot
            // interfere with or restart processes we are about to stop
            // (or, in the keep-volumes path, would otherwise attempt to
            // restart between us deciding to exit and the process
            // actually terminating).
            tasks.abort_all();

            if keep {
                // Rolling-upgrade path: leave volume daemons running.
                // Volume daemons are session-leaders (setsid) and
                // survive coordinator exit; their pid files remain so
                // the next coordinator instance adopts them on its
                // first scan. Any in-flight import or fetch worker is
                // expected to share the coordinator's session and is
                // not retained across restart.
                info!("[coordinator] leaving volume processes running");
            } else {
                // SIGTERM every volume, import, and fetch process across all known volumes.
                let all_pids: Vec<u32> = known
                    .keys()
                    .flat_map(|vol_dir| import::terminate_fork_processes(vol_dir))
                    .collect();

                if all_pids.is_empty() {
                    info!("[coordinator] no processes to stop");
                } else {
                    info!(
                        "[coordinator] waiting for {} process(es) to exit...",
                        all_pids.len()
                    );
                    wait_for_pids(&all_pids, Duration::from_secs(10)).await;
                    info!("[coordinator] done");
                }

                // Clean up pid files now that processes have exited. The
                // supervisor tasks were aborted above and cannot do this
                // themselves.
                for vol_dir in known.keys() {
                    let _ = std::fs::remove_file(vol_dir.join(PID_FILE));
                }
            }

            // Drain the JoinSet so abort propagates while the
            // runtime is still in normal state. Without this, an
            // in-flight `object_store` retry calling
            // `tokio::time::sleep` for backoff can race the
            // runtime's time-driver shutdown and panic with
            // "A Tokio 1.x context was found, but it is being
            // shutdown."
            let drain = async { while tasks.join_next().await.is_some() {} };
            if tokio::time::timeout(Duration::from_secs(2), drain)
                .await
                .is_err()
            {
                warn!("[coordinator] shutdown drain timed out; some tasks did not unwind");
            }

            break;
        }
    }

    Ok(())
}

/// Poll until all pids have exited or the timeout elapses. After the
/// SIGTERM grace window, escalate to SIGKILL on any stragglers and wait
/// briefly for them to die. Under shutdown-park, volume daemons exit
/// directly via `process::exit` after a bounded WAL flush — neither
/// del_dev nor STOP_DEV is involved — so the SIGKILL escalation is
/// purely defensive against a daemon that ignores SIGTERM or is wedged
/// somewhere outside the signal watcher.
async fn wait_for_pids(pids: &[u32], grace: Duration) {
    let still_alive = poll_until_exit_or_deadline(pids, grace).await;
    if still_alive.is_empty() {
        return;
    }
    warn!(
        "[coordinator] shutdown grace expired; SIGKILL {} process(es): {:?}",
        still_alive.len(),
        still_alive
    );
    for &pid in &still_alive {
        if let Ok(raw) = i32::try_from(pid) {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(raw),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
    let leftover = poll_until_exit_or_deadline(&still_alive, Duration::from_secs(2)).await;
    if !leftover.is_empty() {
        warn!(
            "[coordinator] {} process(es) still alive after SIGKILL: {:?}",
            leftover.len(),
            leftover
        );
    }
}

/// Returns the pids that are still alive when the deadline elapses
/// (empty if everything exited).
async fn poll_until_exit_or_deadline(pids: &[u32], timeout: Duration) -> Vec<u32> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let still_alive: Vec<u32> = pids
            .iter()
            .copied()
            .filter(|&pid| {
                if let Ok(raw) = i32::try_from(pid) {
                    nix::sys::signal::kill(nix::unistd::Pid::from_raw(raw), None).is_ok()
                } else {
                    false
                }
            })
            .collect();
        if still_alive.is_empty() {
            return Vec::new();
        }
        if tokio::time::Instant::now() >= deadline {
            return still_alive;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Scan `<data_dir>/by_id/` for volume directories that need coordinator
/// attention (drain, GC, supervision, or prefetch).
///
/// All volumes — writable, imported readonly bases, and pulled ancestors —
/// live in `by_id/<ulid>/`. The supervisor gate in the caller uses
/// `volume.readonly` to decide whether to spawn a serve process.
///
/// Skips:
///   - entries whose name is not a valid ULID
///   - volumes with no `pending/` or `index/` subdirectory (not yet initialised)
///   - readonly volumes that are fully indexed: `index/` is non-empty
///     (prefetch completed)
///
/// Readonly volumes with no `index/` (or an empty one) are included for
/// prefetch. Writable volumes with `pending/` are included for drain.
fn discover_volumes(data_dir: &Path) -> Vec<PathBuf> {
    let mut volumes = Vec::new();
    let root = data_dir.join("by_id");
    let entries = match std::fs::read_dir(&root) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return volumes,
        Err(e) => {
            warn!(
                "[coordinator] cannot read by_id dir {}: {e}",
                root.display()
            );
            return volumes;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if ulid::Ulid::from_string(name).is_err() {
            continue;
        }
        let has_pending = path.join("pending").exists();
        let has_index = path.join("index").exists();
        if !has_pending && !has_index {
            continue;
        }
        // Skip readonly volumes that are already fully indexed locally —
        // index/ is non-empty (prefetch completed).
        if path.join("volume.readonly").exists() && !has_pending {
            let dir_non_empty = |sub: &str| {
                path.join(sub)
                    .read_dir()
                    .map(|mut d| d.next().is_some())
                    .unwrap_or(false)
            };
            if dir_non_empty("index") {
                continue;
            }
        }
        volumes.push(path);
    }
    volumes
}

/// Reconcile `<data_dir>/by_name/` against `<data_dir>/by_id/`.
///
/// - Removes stale symlinks whose target ULID no longer exists in `by_id/`.
/// - Adds missing symlinks for volumes that have a `volume.name` file but no
///   corresponding `by_name/` entry.
fn reconcile_by_name(data_dir: &Path) {
    let by_id_dir = data_dir.join("by_id");
    let by_name_dir = data_dir.join("by_name");

    // Remove stale symlinks: entries that are symlinks but whose target no
    // longer exists.  Non-symlink entries (real directories, files) are left
    // alone and warned about — they indicate unexpected manual changes.
    if let Ok(entries) = std::fs::read_dir(&by_name_dir) {
        for entry in entries.flatten() {
            let link = entry.path();
            if !link.is_symlink() {
                warn!(
                    "[coordinator] by_name/{} is not a symlink; skipping",
                    entry.file_name().to_string_lossy()
                );
                continue;
            }
            // Broken symlink: target no longer exists.
            if !link.exists() {
                let _ = std::fs::remove_file(&link);
                info!(
                    "[coordinator] removed stale by_name symlink: {}",
                    link.display()
                );
            }
        }
    }

    // Add missing symlinks.
    let Ok(entries) = std::fs::read_dir(&by_id_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let vol_dir = entry.path();
        if !vol_dir.is_dir() {
            continue;
        }
        let Some(ulid_str) = vol_dir.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if ulid::Ulid::from_string(ulid_str).is_err() {
            continue;
        }
        let Some(name) = elide_coordinator::tasks::read_volume_name(&vol_dir) else {
            continue;
        };
        let name = name.as_str();
        let link = by_name_dir.join(name);
        if link.is_symlink() || link.exists() {
            // Already present (symlink or unexpected non-symlink); leave it.
            continue;
        }
        let target = format!("../by_id/{ulid_str}");
        if let Err(e) = std::os::unix::fs::symlink(&target, &link) {
            warn!("[coordinator] failed to create by_name/{name} -> {target}: {e}");
        } else {
            info!("[coordinator] created by_name/{name} -> {target}");
        }
    }
}

/// Human-readable label for log messages: "name (ulid)" if the volume has a
/// name, otherwise just the path.
fn volume_label(fork_dir: &Path) -> String {
    let path_str = fork_dir.display().to_string();
    match elide_coordinator::tasks::read_volume_name(fork_dir) {
        Some(name) => {
            if let Some(ulid) = fork_dir.file_name().and_then(|n| n.to_str()) {
                format!("{name} ({ulid})")
            } else {
                format!("{name} ({path_str})")
            }
        }
        None => path_str,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk(root: &Path, rel: &str) {
        std::fs::create_dir_all(root.join(rel)).unwrap();
    }

    #[test]
    fn discover_volumes_scans_by_id() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Valid writable volumes in by_id/.
        let ulid1 = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let ulid2 = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        mk(root, &format!("by_id/{ulid1}/index")); // has index/ → discovered
        mk(root, &format!("by_id/{ulid2}/pending")); // has pending/ → discovered

        // Readonly volume with non-empty index/ — fully drained, must be skipped.
        let ulid3 = "01JQCCCCCCCCCCCCCCCCCCCCCC";
        mk(root, &format!("by_id/{ulid3}/index"));
        std::fs::write(
            root.join(format!(
                "by_id/{ulid3}/index/01JQAAAAAAAAAAAAAAAAAAAAA1.idx"
            )),
            "",
        )
        .unwrap();
        std::fs::write(root.join(format!("by_id/{ulid3}/volume.readonly")), "").unwrap();

        // Readonly volume with non-empty index/ (from prefetch) — prefetch done, skip.
        let ulid6 = "01JQFFFFFFFFFFFFFFFFFFFFFF";
        mk(root, &format!("by_id/{ulid6}/index"));
        std::fs::write(
            root.join(format!(
                "by_id/{ulid6}/index/01JQAAAAAAAAAAAAAAAAAAAAA1.idx"
            )),
            "",
        )
        .unwrap();
        std::fs::write(root.join(format!("by_id/{ulid6}/volume.readonly")), "").unwrap();

        // Readonly volume with empty index/ — pulled but not yet prefetched, include.
        let ulid5 = "01JQEEEEEEEEEEEEEEEEEEEEEE";
        mk(root, &format!("by_id/{ulid5}/index")); // empty index dir
        std::fs::write(root.join(format!("by_id/{ulid5}/volume.readonly")), "").unwrap();

        // Readonly volume with pending/ — newly imported, must be included for drain.
        let ulid7 = "01JQGGGGGGGGGGGGGGGGGGGGGG";
        mk(root, &format!("by_id/{ulid7}/pending"));
        std::fs::write(root.join(format!("by_id/{ulid7}/volume.readonly")), "").unwrap();

        // Non-ULID entry — must be skipped.
        mk(root, "by_id/not-a-ulid/index");

        // ULID dir with no pending/ or index/ — not yet initialised, skip.
        let ulid4 = "01JQDDDDDDDDDDDDDDDDDDDDDDD";
        mk(root, &format!("by_id/{ulid4}"));

        let volumes = discover_volumes(root);
        let mut names: Vec<String> = volumes
            .iter()
            .map(|p| p.strip_prefix(root).unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();

        assert_eq!(
            names,
            vec![
                format!("by_id/{ulid1}"),
                format!("by_id/{ulid2}"),
                format!("by_id/{ulid5}"),
                format!("by_id/{ulid7}"),
            ]
        );
    }

    #[test]
    fn discover_volumes_includes_pulled_ancestor_pending_prefetch() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();

        // Writable volume.
        let wid = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        mk(root, &format!("by_id/{wid}/index"));

        // Pulled ancestor with empty index/ — needs prefetch.
        let rid_new = "01JQBBBBBBBBBBBBBBBBBBBBBB";
        mk(root, &format!("by_id/{rid_new}/index"));
        std::fs::write(root.join(format!("by_id/{rid_new}/volume.readonly")), "").unwrap();

        // Pulled ancestor with populated index/ — prefetch done, skip.
        let rid_done = "01JQCCCCCCCCCCCCCCCCCCCCCC";
        mk(root, &format!("by_id/{rid_done}/index"));
        std::fs::write(
            root.join(format!(
                "by_id/{rid_done}/index/01JQAAAAAAAAAAAAAAAAAAAAA1.idx"
            )),
            "",
        )
        .unwrap();
        std::fs::write(root.join(format!("by_id/{rid_done}/volume.readonly")), "").unwrap();

        let volumes = discover_volumes(root);
        let mut names: Vec<String> = volumes
            .iter()
            .map(|p| p.strip_prefix(root).unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();

        assert_eq!(
            names,
            vec![format!("by_id/{wid}"), format!("by_id/{rid_new}")]
        );
    }
}
