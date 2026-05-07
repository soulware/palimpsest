---
status: landed
related: [plan-actor-offload.md]
---

# Offload WAL promotion off the volume actor

Step 2 of [plan-actor-offload.md](plan-actor-offload.md). Landed across PRs #51 (extent-index CAS), #55 (initial worker thread + promote offload), #56, #57. PR #58 generalised the dedicated flusher into the shared worker thread that all subsequent offloads reuse.

## The decision that mattered

`flush_wal()` previously conflated two operations:

- **Flush** = `wal.fsync()`. Durability barrier. What `NBD_CMD_FLUSH` requires.
- **Promote** = serialise WAL entries into `pending/<ulid>`, update the extent index to segment-relative offsets, delete the old WAL, open a fresh WAL.

The offload split them. `VolumeRequest::Flush` is a WAL fsync plus (if a promote is in flight) a park on the promote generation counter — no segment I/O, no actor blocking. Promotion is triggered independently by `needs_promote()` and the idle tick, dispatched as `WorkerJob::Promote`. The old WAL's `fsync()` runs as the first step of the worker job, not on the actor. New writes queued on the channel keep flowing onto the fresh WAL while the FLUSH caller waits.

## Latent race fixed in Landing 2

The pre-offload code reused the WAL ULID for the promoted segment (`segment_ulid == wal_ulid`). `find_segment_in_dirs` searches `wal/` before `pending/`, so during the promote window stale readers without a cached fd could fall through to `pending/<same_ulid>` while their extent-index entries still held WAL-relative offsets — wrong bytes returned.

Promotion now mints a **fresh** segment ULID, distinct from the WAL ULID. `gc_checkpoint` had been doing this all along; the offload extended the pattern to the common case. No `u_wal` is reserved post-checkpoint — the WAL is opened lazily on the next write.

## Apply-phase CAS

Concurrent writes during the worker's heavy middle may supersede the entries the worker is operating on. `apply_promote` uses `ExtentIndex::replace_if_matches(hash, expected_segment_id, expected_body_offset, new_location)` so concurrent overwrites lose the CAS and survive untouched.

## Code shape

```rust
pub struct PromoteJob {
    pub segment_ulid: Ulid,               // fresh, distinct from the WAL ulid
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub entries: Vec<SegmentEntry>,
    pub pre_promote_offsets: Vec<Option<u64>>, // CAS tokens snapshotted at prep time
    pub signer: Arc<dyn SegmentSigner>,
    pub pending_dir: PathBuf,
}

pub struct PromoteResult {
    pub segment_ulid: Ulid,
    pub old_wal_ulid: Ulid,
    pub old_wal_path: PathBuf,
    pub body_section_start: u64,
    pub entries: Vec<SegmentEntry>,       // stored_offset now segment-relative
    pub pre_promote_offsets: Vec<Option<u64>>,
}
```

Two bounded crossbeam channels connect the actor and worker: a job channel (`bounded::<WorkerJob>(4)`, ~128 MiB at 32 MiB per WAL) and a result channel. The actor's `select!` loop has three arms — `VolumeRequest`, worker results, idle tick.
