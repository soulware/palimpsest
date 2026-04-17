# Plan: offload `sign_snapshot_manifest` off the volume actor

**Status:** Not started. Step 7 of [actor-offload-plan.md](actor-offload-plan.md). Reuses the worker-thread infrastructure established by [promote-offload-plan.md](promote-offload-plan.md) and extended through steps 2–6.

## Goal

Move the heavy middle of `Volume::sign_snapshot_manifest` — `index/` enumeration, Ed25519 signing, manifest file write (with fsync), marker file write — off the actor thread. Production snapshots fire this as the last step of the coordinator's `snapshot_volume` sequence (`elide-coordinator/src/inbound.rs:478`); during the call every queued write on the volume stalls behind the manifest fsync.

Folded into the same landing: delete `VolumeRequest::Snapshot`, `VolumeHandle::snapshot()`, and the `snapshot` arm in `src/control.rs`. Grep finds no wire-level senders of the `snapshot\n` control-socket command — the CLI (`main.rs:902`) and coordinator both go through the decomposed sequence (`promote_wal` / `promote` / `apply_gc_handoffs` / `snapshot_manifest`). `Volume::snapshot()` the method is kept as a direct entry point for the 30+ in-process test callers.

**Non-goals.** Changing the coordinator-driven snapshot sequence. Changing the on-disk layout of `snapshots/` or the manifest format. Parallelising manifest signing across multiple workers. Touching `Volume::snapshot()`'s inline promote-segment loop (that method never runs on the actor — the dead `VolumeRequest::Snapshot` is the only path that was ever going to).

## Current flow

Production snapshot, per `snapshot_volume` (`elide-coordinator/src/inbound.rs:478`):

1. `control::promote_wal` → `VolumeRequest::PromoteWal` → **worker (offloaded, step 2)**
2. `upload::drain_pending` → per-segment `VolumeRequest::Promote` → **worker (offloaded, step 4)**
3. `control::apply_gc_handoffs` → `VolumeRequest::ApplyGcHandoffs` → **worker (offloaded, step 3)**
4. `control::sign_snapshot_manifest` → `VolumeRequest::SignSnapshotManifest` → **actor (this step)**

The actor-thread body of step 4 is `Volume::sign_snapshot_manifest` (`elide-core/src/volume.rs:3166`):

1. Enumerate `index/` — `fs::read_dir` scans N `.idx` files; each entry is parsed as a ULID.
2. `fs::create_dir_all("snapshots/")`.
3. `signing::write_snapshot_manifest` — sort + dedup ULIDs, build signing input, Ed25519 sign, serialize, `write_file_atomic` (tmp + fsync + rename).
4. `fs::write` the marker file `snapshots/<snap_ulid>`.
5. Set `self.has_new_segments = false`.

The fsync inside `write_file_atomic` dominates; a 10k-segment manifest is ~260 KB signed, but the latency is fsync-bound not size-bound. Typical 10–100 ms; worse on slow storage.

## Proposed flow

### Phase shape (same three-phase pattern as other offloads)

**Prep (actor):** snapshot the signer `Arc` and paths into a `SignSnapshotManifestJob`. No directory enumeration — the worker reads `index/` itself. This keeps the actor's critical section at O(1).

**Heavy middle (worker):** enumerate `index/`, sign the manifest, atomic-write it, write the marker. Pure function of the job plus on-disk state.

**Apply (actor):** flip `has_new_segments = false`, publish snapshot (cheap; no maps changed), reply to the parked caller.

The apply phase is tiny — almost degenerate — but we keep it on the actor to stay consistent with the other offloads and to keep the `has_new_segments` flag mutation on the single writer.

### Data structures

```rust
pub struct SignSnapshotManifestJob {
    pub snap_ulid: Ulid,
    pub base_dir: PathBuf,
    pub signer: Arc<dyn segment::SegmentSigner>,
}

pub struct SignSnapshotManifestResult {
    pub snap_ulid: Ulid,
}
```

`WorkerJob` and `WorkerResult` grow a `SignSnapshotManifest(..)` variant each. No new thread. No new channel.

### Worker

```rust
pub fn execute_sign_snapshot_manifest(
    job: SignSnapshotManifestJob,
) -> io::Result<SignSnapshotManifestResult>;
```

1. `read_dir(base_dir.join("index"))` → collect `Vec<Ulid>` from `.idx` stems (parse via `Ulid::from_string`; skip non-matching entries). `NotFound` → empty list.
2. `create_dir_all(snapshots_dir)`.
3. `signing::write_snapshot_manifest(&base_dir, signer.as_ref(), &snap_ulid, &ulids)` — this path already does the tmp-write + fsync + rename dance internally.
4. `fs::write(snapshots_dir.join(snap_ulid.to_string()), "")` — marker written **after** the manifest is durable, as today.
5. Return `SignSnapshotManifestResult { snap_ulid }`.

### Apply phase (actor)

On `WorkerResult::SignSnapshotManifest(Ok(result))`:

1. `self.volume.mark_snapshot_clean()` — sets `has_new_segments = false`.
2. No `publish_snapshot()` needed — neither `lbamap` nor `extent_index` changed. (But we may call it anyway for consistency; trivially cheap.)
3. Pop the `parked_sign_snapshot_manifest` reply, send `Ok(())`.

On error: pop the reply, forward the error.

### Parking

```rust
parked_sign_snapshot_manifest: Option<Sender<io::Result<()>>>,
```

Single slot — concurrent `SignSnapshotManifest` requests are rejected with an error, matching the `parked_sweep` / `parked_repack` / `parked_delta_repack` pattern. In practice the coordinator holds a per-volume snapshot lock across the whole `snapshot_volume` sequence, so concurrent requests never arise.

### Concurrency with other worker jobs

The worker is FIFO single-threaded. The coordinator's snapshot lock ensures the steps of one snapshot serialize in the intended order (promote_wal → drain → apply_gc_handoffs → sign_manifest). What's new is that *writes and sweep/repack/delta_repack may still interleave* with the manifest sign on the worker queue — writes land on the fresh WAL (never touches `index/`), and sweep/repack only rewrite `pending/` contents. The only potentially-surprising interleave:

| Interleaving | Safe? |
|---|---|
| `WorkerJob::SignSnapshotManifest` runs on worker while actor processes a subsequent write | Safe. Writes land on the fresh WAL opened by the prior `promote_wal` (step 1 of `snapshot_volume`); they don't appear in `index/` until a future drain. Manifest reflects state as of the snapshot lock acquisition. |
| `WorkerJob::Promote` (WAL→pending) interleaves with `SignSnapshotManifest` on the worker | Cannot happen within one snapshot: `snapshot_volume` calls `promote_wal` synchronously (parked reply) before `sign_snapshot_manifest`. Between different snapshots, the snapshot lock serialises. |
| `WorkerJob::PromoteSegment` (pending→index) interleaves on the worker | Cannot happen within one snapshot: `drain_pending` awaits every `promote` IPC before returning; `snapshot_volume` calls `drain_pending` before `sign_snapshot_manifest`. |
| `WorkerJob::Sweep` / `Repack` / `DeltaRepack` running concurrently with `SignSnapshotManifest` | Safe on the single-worker FIFO: they serialise. Their outputs rename over files in `pending/`, not `index/`. Manifest's `index/` enumeration is unaffected. |

So the manifest's enumeration snapshot is well-defined: it's the state of `index/` at the moment the worker picks up the job, which is after the FIFO queue drains to it. The coordinator's per-volume lock plus the FIFO guarantee this is exactly the state after the preceding `promote`s.

### Crash semantics

| Crash point | State | Recovery |
|---|---|---|
| Before manifest tmp-write completes | No manifest, no marker | Coordinator retries on next snapshot attempt; no visible partial snapshot. |
| After manifest rename, before marker write | `snapshots/<snap_ulid>.manifest` exists, no marker | On the next snapshot attempt the coordinator picks the same (or a later) `snap_ulid` — `write_snapshot_manifest` overwrites via tmp+rename, then writes the marker. Without a marker there is no visible snapshot, so no reader ever saw the orphan. |
| After marker write, before apply | Manifest + marker on disk, `has_new_segments` still true in memory | Process is dying; in-memory flag is moot. Next process open rereads directory state; `has_new_segments` is derived from WAL / `pending/` content, not persisted. |

No on-disk invariant changes. The manifest's "marker written last" rule already handles partial-write crashes.

### Failure handling

If the worker returns an error:
- Log, pop the parked reply, forward the error to the coordinator.
- On-disk state may have a stale `.manifest` tmp (swept by the atomic-write's error path) or nothing at all; next snapshot retries.

## Free wins folded in

1. **Dead-code deletion.** `VolumeRequest::Snapshot`, `VolumeHandle::snapshot()`, the `snapshot` control-socket arm — all unreachable from any live sender. Removing them closes one actor arm and clarifies the production path.
2. **`VolumeRequest::SignSnapshotManifest` becomes the single actor entry point for snapshot writing.** The only remaining actor-owned piece of snapshot is the parking slot and the one-flag apply.

## Testing

1. **Unit test: manifest content unchanged under offload.** Drive a `VolumeHandle::sign_snapshot_manifest(u)` through the actor; read back `snapshots/<u>.manifest`; assert bytes match a reference produced by the direct `Volume::sign_snapshot_manifest(u)` call on a sibling volume with identical segment set.
2. **Unit test: concurrent write during manifest sign.** Dispatch `sign_snapshot_manifest`, issue a write before the parked reply arrives, assert the write lands on the fresh WAL and does not appear in the signed manifest.
3. **Unit test: crash-between-manifest-and-marker recovery.** Force a worker failure after manifest write but before marker; reopen; call `sign_snapshot_manifest` again; assert success and exactly one marker.
4. **Regression: `parked_sign_snapshot_manifest` rejects concurrent requests.** Fire two `sign_snapshot_manifest` calls without awaiting; assert the second errors.
5. **Existing tests preserved.** `sign_snapshot_manifest` tests in `volume.rs` continue to exercise `Volume::sign_snapshot_manifest` directly; actor-path tests exercise the offload end-to-end.

## Landing sequence

1. **Split `Volume::sign_snapshot_manifest`.** Factor into `prepare_sign_snapshot_manifest` (returns `SignSnapshotManifestJob`), `actor::execute_sign_snapshot_manifest` (worker-side; pure), and `apply_sign_snapshot_manifest_result` (flips `has_new_segments`). Inline `Volume::sign_snapshot_manifest` stays as a thin wrapper calling all three — tests and the direct-`Volume::snapshot` path are unaffected.
2. **Add the worker variants and job/result types.** `WorkerJob::SignSnapshotManifest`, `WorkerResult::SignSnapshotManifest`, `SignSnapshotManifestJob`, `SignSnapshotManifestResult`.
3. **Route `VolumeRequest::SignSnapshotManifest` through the worker.** Prep on the actor, dispatch to worker, park reply in `parked_sign_snapshot_manifest`, apply on result receipt.
4. **Delete the dead `Snapshot` wire command.** Remove `VolumeRequest::Snapshot`, `VolumeHandle::snapshot()`, the `snapshot` arm in `src/control.rs`. No sender to notify.
5. **Update `actor-offload-plan.md` step 7** to reflect the landed scope.

## Open questions

- **Is the apply phase worth keeping?** The apply is just one bool flip plus a reply. We could send the reply from the worker directly, except that the `has_new_segments` flag mutation must stay on the actor (only owner of `&mut Volume`). The overhead is one channel round-trip, microseconds — not worth optimising. Keep the three-phase shape for consistency.
- **Do we want per-worker parallelism for snapshot signing?** No. Snapshots fire on user-scale cadence (minutes to hours); one manifest per call; single-worker serialisation is free.
- **Should `Volume::snapshot()` the method be deleted too?** Out of scope for this landing. It has 30+ direct callers in tests and runs synchronously off the actor. A follow-up could migrate tests to compose `flush_wal_to_pending` + per-segment `promote_segment` + `sign_snapshot_manifest` explicitly, but that's ergonomics, not latency.
