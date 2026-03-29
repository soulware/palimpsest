# Operations

Ongoing system behaviour: S3 upload, garbage collection, repacking, and filesystem metadata awareness.

## WAL Promotion

The WAL is promoted to a `pending/` segment in two ways:

**Size threshold (32 MB):** every write checks whether the WAL has crossed 32 MB. If so, `promote()` is called immediately before the write returns. This is a soft cap — a single write larger than 32 MB will still succeed, producing an oversized segment.

**Idle flush:** `serve-volume` watches for write inactivity and promotes the WAL automatically after the configured idle period. This ensures that a short burst of writes — e.g. writing a few files from a VM — lands in `pending/` without requiring an explicit `snapshot-volume` call.

```
elide serve-volume <vol-dir> <fork> --auto-flush <SECS>
```

`--auto-flush` defaults to **10 seconds**. Pass `0` to disable. The server prints `[auto-flush: Xs idle]` at startup to confirm the setting. During an idle window, the 200 ms read-timeout loop checks whether `last_write.elapsed() >= threshold`; if so, `flush_wal()` is called and `last_write` is cleared. A subsequent write resets the timer.

Both triggers call the same `promote()` path, producing an identical segment format. Neither is on the fsync critical path — a guest `fsync` returns as soon as the WAL record is durable; promotion happens inline on the next write (size trigger) or on the idle timer.

## S3 Upload

Segments accumulate in `pending/` after WAL promotion (see [formats.md](formats.md) for the promotion commit sequence). The coordinator is responsible for uploading them to the object store and moving them to `segments/` on success. Each segment is handled independently — a failure on one does not block the others.

**`drain-pending`** is the current upload mechanism. It is a one-shot command that scans `pending/`, uploads each segment, and exits. It is invoked explicitly rather than running as a daemon, which gives direct control over when uploads occur during testing and development:

```
elide-coordinator drain-pending <fork-dir>
```

Store selection:
- `--local <path>` — use a local directory as the object store (no server needed; useful for testing)
- default — use S3 via environment variables: `ELIDE_S3_BUCKET`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

The `object_store` crate is used for all store access, which provides a uniform interface across local filesystem, S3, GCS, and Azure backends. Switching from a local store to S3 or Tigris is a configuration change, not a code change.

**Upload commit sequence per segment:**
1. Read `pending/<ulid>` into memory
2. PUT to object store at key `<volume_id>/<fork_name>/YYYYMMDD/<ulid>`
3. On success: rename `pending/<ulid>` → `segments/<ulid>` (atomic commit)
4. On failure: leave in `pending/`, record error, continue with remaining segments

The rename in step 3 is the local commit point. If the coordinator crashes between steps 2 and 3, the object is in S3 but the segment is still in `pending/` — a retry will re-PUT (idempotent) and then rename. No ledger file is needed.

**Exit code:** non-zero if any segment failed to upload. Segments that succeeded are committed regardless — the exit code signals that a re-run is needed for the remainder.

**GC and upload ordering:** see the GC section below for the required sequencing between GC and upload when both operate on `pending/` segments.

## Demand-fetch

Segments in `segments/` are S3-backed and evictable. When `find_segment_file` is called during a read and the segment file is absent locally, the volume delegates to an optional `SegmentFetcher`. If a fetcher is configured, it downloads the segment from the object store, writes the three-file fetched format to `fetched/`, and the read proceeds normally. If no fetcher is configured, the read fails with "segment not found".

**Rebuild vs. runtime:** demand-fetch operates at read time, after the volume is open. The LBA map and extent index are rebuilt at `Volume::open` by scanning `pending/`, `segments/`, and `fetched/*.idx` on local disk. If none of these are present for an ancestor segment, that data will appear as zeros at open time rather than triggering a fetch. This is the **cold-start problem**.

**`prefetch-indexes` solves cold-start.** Before opening a forked volume on a host that has no local segments for its ancestors, run:

```
elide-coordinator prefetch-indexes [--local <path>] <fork-dir>
```

This walks the fork's ancestry chain, lists S3 objects for each ancestor fork, and downloads the header+index portion (`[0, body_section_start)`) of each segment not already present locally, writing it as `fetched/<ulid>.idx` in the ancestor's fork directory. Body bytes are not downloaded. After this, `Volume::open` rebuilds the full LBA map from `fetched/*.idx`, and individual reads demand-fetch body bytes on first access.

**Configuration — `fetch.toml`** in the volume root directory:

```toml
# S3 / S3-compatible (MinIO, Tigris, etc.)
bucket   = "my-elide-bucket"
endpoint = "https://s3.amazonaws.com"   # omit for AWS default
region   = "us-east-1"                  # optional; falls back to AWS_DEFAULT_REGION

# Local filesystem — for testing without a real object store
# local_path = "/tmp/elide-store"
```

If `fetch.toml` is absent, the following env vars are tried: `ELIDE_S3_BUCKET` (required), `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION`. If neither is present, demand-fetch is disabled and `serve-volume` runs normally.

**End-to-end test procedure (local store, cold-start fork):**

```bash
# 1. Create a data volume and write data from a VM
mkdir -p volumes/data-vol
elide serve-volume volumes/data-vol default --size 1G --bind 0.0.0.0
# (in VM) nbd-client <host> 10809 /dev/nbd0
#         mkfs.ext4 /dev/nbd0 && mount /dev/nbd0 /mnt
#         echo "hello from vm1" > /mnt/testfile && umount /mnt
#         nbd-client -d /dev/nbd0

# 2. Upload the default fork's segments to a local store
mkdir -p /tmp/elide-store
elide-coordinator drain-pending --local /tmp/elide-store volumes/data-vol/forks/default

# 3. Configure demand-fetch for the volume
cat > volumes/data-vol/fetch.toml << 'EOF'
local_path = "/tmp/elide-store"
EOF

# 4. Fork from the default fork for a new VM
elide snapshot-volume volumes/data-vol default
elide fork-volume volumes/data-vol vm2

# 5. Simulate a fresh host: delete local segments from the ancestor fork
rm -f volumes/data-vol/forks/default/segments/*

# 6. Prefetch indexes — downloads .idx for each ancestor segment (no bodies)
elide-coordinator prefetch-indexes --local /tmp/elide-store volumes/data-vol/forks/vm2
# Output: "1 fetched, 0 already present, 0 failed" (one .idx per uploaded segment)

# 7. Serve the fork — opens correctly because fetched/*.idx rebuilt the LBA map
elide serve-volume volumes/data-vol vm2 --size 1G --bind 0.0.0.0
# Output includes "[demand-fetch enabled]"

# 8. From a VM, mount and read the data written in step 1
# (in VM) nbd-client <host> 10809 /dev/nbd0
#         mount /dev/nbd0 /mnt
#         cat /mnt/testfile   ← triggers demand-fetch; should print "hello from vm1"
#
# serve-volume output will show the segment being fetched from /tmp/elide-store.
# The file appears in volumes/data-vol/forks/default/fetched/ as .idx + .body + .present
```

**Warm-start test (eviction during serving):**

```bash
# Simpler scenario: serve with segments present, then delete one while running.
elide serve-volume volumes/data-vol default --size 1G --bind 0.0.0.0 &
# (in VM: mount, read to confirm data present)

# Delete a segment mid-run to simulate eviction:
SEGMENT=$(ls volumes/data-vol/forks/default/segments/ | head -1)
rm volumes/data-vol/forks/default/segments/$SEGMENT

# Next VM read touching that segment triggers demand-fetch transparently.
```

**Next step: eviction.** Track segment access time (mtime touch on fetch or read), enforce a configurable `max_cache_bytes` in `fetch.toml`, and evict least-recently-used segments from `segments/` and `fetched/` (never from `pending/`). Eviction + demand-fetch together make `segments/` a transparent cache tier.

## Diagnostic tools

Two commands inspect the raw binary file formats written by `elide`. Both are read-only.

**`inspect-segment <path>`** — prints the header and index entries of a segment file or a fetched `.idx` file:

```
elide inspect-segment volumes/myvm/forks/default/pending/01JQEXAMPLE...
elide inspect-segment volumes/myvm/forks/vm2/fetched/01JQEXAMPLE....idx
```

Output includes: file kind (full segment vs index-only), entry counts (data / dedup_ref), a table of data entries sorted by body offset (LBA range, body offset, stored length, compression flag), and total body utilisation. Entries that would read past the end of the body file are flagged `OVERFLOW` — this indicates a segment corruption or a flag-translation bug.

**`inspect-wal <path>`** — prints every record in a WAL file:

```
elide inspect-wal volumes/myvm/forks/default/wal/01JQEXAMPLE...
```

Output includes: record counts (data / dedup_ref), a table of records (type, LBA range, body offset within the WAL file, payload size, compression flag). Truncated tail records (from a crash mid-write) are reported but not an error — `Volume::open` handles them during recovery.

Both commands are useful when debugging read failures: `inspect-segment` surfaces the compressed/uncompressed flag and stored lengths that the read path relies on; `inspect-wal` shows what a WAL contains before it is promoted.

## GC and Repacking

### Background pending compaction

`serve-volume` runs a compaction pass on `pending/` automatically, triggered by the idle flush. After `flush_wal()` succeeds in the idle arm (no write is in flight), the pass runs immediately. Because compaction only runs during the idle window, it never delays a write.

**What the pass does:**

1. Scan all segments in `pending/`
2. Cross-reference against the live LBA map to identify dead extents (LBAs since overwritten)
3. Identify candidates: any segment with at least one dead extent, or any segment below 8 MB
4. If no candidates: return (no-op — all pending segments are fully live and dense)
5. Collect all live extents from every candidate segment
6. Write one new `pending/<ulid>` containing the merged live extents (split at 32 MB if the merged output would exceed the WAL promotion threshold)
7. Update the in-memory LBA map and extent index to point to the new segment
8. Delete the original candidate segments
9. Log: `[compact-pending: N → M segments, X MB reclaimed]`

**Write-path isolation:** neither promotion trigger (32 MB WAL threshold or idle flush) blocks on compaction. After any promotion, a new empty WAL is immediately available for writes. Compaction catches up in the next idle window.

**Snapshot floor:** segments at or below the latest snapshot ULID are frozen and are never touched, even if they are in `pending/`.

**Key property:** data written then deleted before `drain-pending` runs is never uploaded to S3. The compaction pass removes it from `pending/` entirely.

---

**GC has two distinct scopes with different constraints:**

*Local GC* (space reclamation on disk) operates only on live leaf nodes — those containing `wal/`. Frozen ancestor nodes are structurally immutable and shared by all their descendants; their local segments cannot be touched while any live descendant exists. This matches the lsvd reference implementation's approach: `removeSegmentIfPossible()` refuses to delete a segment referenced by any volume. In the directory model this is structural: absence of `wal/` means no local GC.

To reclaim local space from a frozen ancestor, all its live descendants must first be deleted or re-based. This constraint is intentional: it makes the invariant ("ancestor segments are immutable") enforceable without any reference counting. A practical consequence: a host running many long-lived VMs all forked from the same ancestor will accumulate unreclaimable local disk usage in that ancestor's `segments/` until the VMs are deleted or re-based onto a newer snapshot. S3 repacking is not subject to this constraint and can consolidate or remove ancestor data regardless of live descendants.

Within a live leaf node, **all** of its own segments are GC-eligible — there is no distinction between segments promoted in the current session and those promoted in earlier sessions. GC operates on `pending/` and `segments/` within the live node's directory only; ancestor directories are never scanned or modified.

**GC and S3 upload ordering:** GC intentionally covers `pending/` segments (not yet uploaded) as well as `segments/` (already uploaded). Compacting a `pending/` segment before it is uploaded avoids sending sparse data to S3 — only the denser replacement is uploaded. This requires that GC and the S3 uploader are serialised by the coordinator and never run concurrently on the same `pending/` segment. The coordinator-level ownership of both tasks makes this natural: the intended sequencing is promote → GC → upload.

*S3 repacking* (locality optimisation in object storage) is a coordinator-level operation and is **not subject to the leaf-only constraint**. The coordinator can read extents from any node's local segments or from S3, create new S3 objects with better layout, and update the extent index to point to them. Local files are caches — the coordinator does not modify them to repack at the S3 level.

The unit of an S3 read is a byte-range GET covering a contiguous chunk of a segment body. The goal of repacking is to minimise the number of such GETs needed to serve a given access pattern. By co-locating extents that are accessed together (e.g. during VM boot) into a contiguous region of the same S3 object, all of them can be retrieved in a single byte-range GET — or at most a small number of adjacent chunk fetches. Extents scattered across many segments, or far apart within a segment, require a separate GET per chunk.

**Standard local GC within a live node:** walk the live node's LBA map, identify extents no longer referenced by any LBA range (overwritten or deleted), remove them from local segments after a grace period. Compact sparse segments by merging live extents into fresh, denser segments and updating the extent index.

**Delta dependency handling:** when a source extent is about to be removed and a live delta in S3 depends on it, materialise the delta first (fetch source + delta → full extent, write full extent to S3, update extent index). Then remove the source. The dependency map is derived fresh each GC sweep from the extent index — no persistent reverse index needed.

**Access-pattern-driven repacking:** the coordinator observes which extents are accessed during VM boot and co-locates them contiguously in a dedicated S3 segment. A cold VM boot then requires a small number of byte-range GETs — ideally one — to satisfy the entire boot sequence, rather than one GET per chunk scattered across many segments. Because the extent index covers the full snapshot tree, the repacker sees all boot extents in one place regardless of which ancestor node's segment originally held them.

**Boot hint accumulation:** every VM boot records which extent hashes were accessed during the boot phase (identified by time window after volume attach, or explicit VM lifecycle signals from the hypervisor). Observations accumulate per snapshot node. With the snapshot tree, multiple VMs forked from the same ancestor all boot the same base extents — their observations aggregate for the same image, accelerating convergence. After sufficient boots (converges quickly at scale — 500 VMs/day = 500 observations/day), the hint set is stable enough to drive repacking.

**Continuous improvement:** first boot is cold; boot access patterns are recorded; next repack co-locates those extents in S3; subsequent boots are faster. The feedback loop strengthens with scale.

**Snapshot-aligned repacking:** the snapshot tree makes the two-tier S3 layout structurally explicit rather than heuristically inferred. Extents that live in ancestor nodes are by definition shared across all descendants — they are natural candidates for base segments. Extents in recent leaf nodes are unique to that branch — they belong in snapshot-specific segments. Directory depth is a direct proxy for sharing depth; no similarity analysis needed.

```
s3://bucket/segments/base-<hash>     — extents from ancestor nodes; shared across all descendants
s3://bucket/segments/snap-<id>-N     — extents unique to a specific snapshot node
```

Shared extents (e.g. the ~84% identical between Ubuntu 22.04 point releases) are consolidated into base segments. A new host serving any snapshot from the same family fetches these once. Snapshot-specific segments contain only the changed extents, stored as deltas against their counterparts in the base segments. When a repacked base segment supersedes extents previously stored in per-node S3 objects, the extent index is updated to point to the repacked location; the per-node objects can then be removed once no extent index entry references them.

**Ext4 re-alignment during GC:** GC is a natural point to perform or improve extent re-alignment, not just snapshot time.

- **Snapshot nodes:** safe and clean — a snapshot is frozen, the filesystem state is fixed. GC can parse ext4 metadata and re-align any snapshot that was not aligned at creation time.
- **Live nodes:** the filesystem is in flux. GC uses the most recent frozen ancestor's ext4 metadata as a proxy. Re-alignment is approximate but safe — dedup quality improves progressively without risk of data corruption.

## Filesystem Metadata Awareness

Since the system controls the underlying block device, it sees every write — including writes to ext4 metadata structures (superblock, group descriptors, inode tables, extent trees, journal). This visibility is an opportunity to handle metadata blocks smarter than opaque data blocks.

**Metadata extent tagging:** once metadata LBAs are identified from the superblock (all at well-defined offsets), those extents can be tagged in the LBA map. Tagged metadata extents receive special treatment:
- Skip dedup — inode tables and group descriptors are volume-specific (unique inode numbers, volume-specific block addresses) and will never match across snapshots
- Cache aggressively — metadata blocks are hot; every filesystem operation reads them

**Incremental shadow filesystem view:** because every write to a known metadata LBA is visible, the system could maintain a continuously-updated internal view of the filesystem layout — which LBA ranges belong to which files, as files are created, deleted, and modified. At snapshot time, the shadow view is already current: no parse-from-scratch, re-alignment is essentially free.

**The journal problem:** ext4 metadata does not go directly to its final LBA. It is written to the jbd2 journal first (write → journal commit → checkpoint to final location). There are three levels of journal handling with very different complexity profiles:

- **Level 1 — detect that metadata changed (trivial):** writes to journal LBAs are visible at the block device level. We know a transaction is in flight but not what changed. Useful only for invalidating the shadow view ("metadata changed, re-parse at next opportunity"). No journal parsing required.

- **Level 2 — parse committed transactions at snapshot/GC time (moderate):** at a known-clean point (snapshot or GC checkpoint), read the journal, walk committed transactions, and replay them to recover current metadata state. This is what `e2fsck` does. The jbd2 format is well-documented with reference implementations in `e2fsprogs` and the kernel. A few hundred lines of careful Rust. Risk is low — we are parsing a frozen, consistent state. This is sufficient for snapshot and GC re-alignment.

- **Level 3 — live transaction tracking (high):** intercept journal writes in real time, parse each transaction as it commits, and update the shadow view incrementally. Requires recognising journal LBAs in the write stream, parsing jbd2 descriptor blocks to correlate data blocks with their final destinations, and correctly handling the circular log structure, transaction abort/rollback, and journal checkpointing. Getting this wrong silently produces an incorrect shadow view. The kernel's jbd2 module is the authoritative reference and is non-trivial.

One simplifying factor across all levels: ext4's default journaling mode is `data=ordered` — only metadata goes through the journal; data blocks are written directly to their final locations. Journal handling is therefore scoped to metadata only, not the full write stream.

**Recommended approach:** implement Level 2 first — sufficient for snapshot/GC re-alignment and well-understood. Level 3 (live shadow view) is only needed for real-time file-identity-aware dedup decisions and should be deferred until Level 2 is working well.

**Future potential:** a live shadow filesystem view would enable real-time dedup decisions informed by file identity — knowing that a write is to a known shared library vs. a per-VM log file, for example, without waiting for snapshot time. This is a significant capability that falls out naturally from controlling the block device, and is worth designing toward even if not implemented immediately.
