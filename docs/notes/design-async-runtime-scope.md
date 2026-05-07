---
status: landed
related: [design-ublk-transport.md]
---

# Async runtime scope

Goal: keep async confined to the coordinator and `elide-import`. The volume's I/O hot path (NBD, ublk, the actor and its worker) is synchronous and stays that way.

Three independent steps compose the trajectory:

1. **Sync demand-fetch — landed.** `elide-fetch` uses `rust-s3` with the `sync` feature (attohttpc transport, no tokio) behind a small sync `RangeFetcher` trait. The coordinator continues to use `object_store` for its own list/put/delete and adapts its store to `RangeFetcher` when constructing a fetcher.
2. **Thread-bounded async in ublk queue threads — landed.** Each ublk queue thread runs a `smol::LocalExecutor` for its async per-tag tasks; backend work is offloaded to a per-queue `std::thread` worker pool. Each worker owns its own `VolumeReader` and blocks on `crossbeam-channel` into the actor exactly as the NBD path does. Completions cross from worker threads back into the queue's io_uring via an eventfd watched by the queue's own ring — see [design-ublk-transport.md](design-ublk-transport.md) for why that bridge is necessary. The async surface is local to `src/ublk.rs`; `VolumeClient`/`VolumeReader` and everything downstream stay synchronous.
3. **Drop `tokio` from the volume binary — outstanding.** `elide/Cargo.toml` still pulls in `tokio` because the binary hosts the embedded coordinator-tasks loop and CLI subcommands that hit S3 (`pull`, `ls`, fork-from-S3). Removing it requires relocating those into `elide-coordinator` (the daemon) or behind a thin sync RPC.

**Rationale.** The volume is the correctness-critical hot path. Keeping the actor and backend synchronous matches the I/O model at the actor interface — NBD and ublk both dispatch through synchronous `VolumeClient`/`VolumeReader` calls — and removes the only external dependency that would force a *global* async runtime into the volume. The `smol::LocalExecutor` inside each ublk queue thread is thread-local, not a runtime; it coexists with tokio if tokio is ever re-introduced for unrelated reasons. The coordinator and import tool are naturally async (HTTP, supervision, signals) and keep tokio unchanged.
