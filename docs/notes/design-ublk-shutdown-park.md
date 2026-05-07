---
status: landed
related: [design-ublk-transport.md]
---

# ublk shutdown leaves QUIESCED; deletion is explicit

Daemon shutdown and ublk device deletion are separate operations. SIGTERM/SIGINT/SIGHUP exits the daemon process without `STOP_DEV`; the kernel transitions LIVE → QUIESCED via its daemon-exit detection (when control fds close) and `UBLK_F_USER_RECOVERY` keeps the device usable. Mounts stay in place; `stop → start` while mounted now Just Works.

Deletion is explicit: `elide ublk delete <id>` for ad-hoc cleanup, and a startup reconciliation sweep on the coordinator for orphans. Volume removal orchestrates daemon stop → `del_dev` → directory removal in that order.

## Kernel behaviour (the load-bearing fact)

`STOP_DEV` is the explicit "tear down" verb — it transitions LIVE → **DEAD** regardless of `UBLK_F_USER_RECOVERY`. The QUIESCED state is reached only via the kernel's daemon-exit path. Therefore "leave the device QUIESCED" requires *not* calling `STOP_DEV`. Graceful shutdown must look the same as SIGKILL from the kernel's perspective — just exit the process.

## Implementation summary

- `src/ublk.rs` `spawn_ublk_signal_watcher` — on signal, `client.flush()` (3 s watchdog) then `process::exit(0)`. No `kill_dev`.
- libublk's `Drop` calls `del_dev()` synchronously when the ctrl was constructed for-add. Suppress with `mem::forget(ctrl)` after stop, with a comment noting the kernel reaps the fd at exit. (Long-term: upstream a `keep_alive` flag.)
- `ublk.id` persists across daemon exits.
- `elide-coordinator/src/ublk_sweep.rs` — startup sweep enumerating `/sys/class/ublk-char/`, deleting orphan kernel devices not bound to any volume directory, and clearing stale `ublk.id` files whose sysfs entry is gone (covers host reboot). Wraps `del_dev` in PR #140's bounded-thread + timeout pattern.

## Durability on graceful shutdown

`Volume::write` does not currently fsync before reply, so writes acked to the guest may not yet be on disk. The shutdown flush (`client.flush()` → `volume.wal_fsync()`) closes that window for graceful exits. SIGKILL bypasses this, matching the existing crash-recovery contract — anything not durable also wasn't acked to the kernel and gets reissued on `START_USER_RECOVERY`.

## Operations ordering

The third sequence — start while mounted — is the one users actually want to "just work." Pre-fix it didn't.

| Sequence | Pre-fix (PR #140) | Post-fix |
|---|---|---|
| `umount → stop → start → mount` | clean | clean |
| `stop → umount → start → mount` | recoverable: timeout, deferred del, fresh ADD | clean: device parked, recovery on start |
| `stop → start → umount → mount` | broken: start fails until umount finishes del | clean: recovery succeeds while mounted |
| `stop` then volume deleted | clean (del_dev ran during stop) | requires explicit `elide ublk delete` (or coordinator sweep) |
| `stop` then daemon never restarted | clean (device gone) | stale device until coordinator sweep |

## Tradeoffs

- Devices accumulate if a volume directory is removed out-of-band while the daemon is stopped — the coordinator's startup sweep cleans these up. `elide volume delete` is the supported path and orchestrates the sequence correctly.
- "Volume daemon stopped" is no longer the same as "ublk device gone." `elide volume status` shows both. More honest than the previous conflation.
