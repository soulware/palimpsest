# Operations

Ongoing system behaviour: S3 upload, garbage collection, repacking, and filesystem metadata awareness.

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

Segments in `segments/` are S3-backed and evictable. When `find_segment_file` is called during a read and the segment file is absent locally, the volume delegates to an optional `SegmentFetcher`. If a fetcher is configured, it downloads the segment from the object store, writes it atomically to `segments/`, and the read proceeds normally. If no fetcher is configured, the read fails with "segment not found".

**Important constraint — rebuild vs. runtime:** demand-fetch operates at read time, after the volume is open. The LBA map and extent index are rebuilt at `Volume::open` by reading segment index sections directly from disk. If a segment is missing at open time, the rebuild simply won't see it — that data will appear as zeros, not as a fetch opportunity. Demand-fetch therefore only applies to segments that go missing while the volume is already running (i.e. after eviction removes them from `segments/`). Cold-start scenarios (fresh host with no local segments) require either full pre-fetch before open, or manifest persistence so the LBA map can be rebuilt without segment files present.

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

**Manual test procedure (local store):**

```bash
# 1. Import a base volume (if not already done)
elide-import volumes/ubuntu-22.04 --image ubuntu:22.04

# 2. Fork and serve it; connect from a VM, write data, disconnect
elide fork-volume volumes/ubuntu-22.04 vm1
elide serve-volume volumes/ubuntu-22.04 vm1 --size 4G --bind 0.0.0.0
# ... (in VM) nbd-client <host> 10809, mkfs, write, unmount, nbd-client -d ...

# 3. Upload vm1 segments to a local store
mkdir -p /tmp/elide-store
elide-coordinator drain-pending --local /tmp/elide-store volumes/ubuntu-22.04/forks/vm1

# After drain: vm1 segments are in both forks/vm1/segments/ and /tmp/elide-store/

# 4. Configure demand-fetch
cat > volumes/ubuntu-22.04/fetch.toml << 'EOF'
local_path = "/tmp/elide-store"
EOF

# 5. Serve vm1 again. The volume opens normally (all segments present locally).
elide serve-volume volumes/ubuntu-22.04 vm1 --size 4G --bind 0.0.0.0 &
NBD_PID=$!

# 6. In a second terminal: delete a segment from the running volume to simulate eviction.
#    (Find a segment ULID from forks/vm1/segments/ first.)
#    SEGMENT=<ulid>
rm volumes/ubuntu-22.04/forks/vm1/segments/$SEGMENT

# 7. From the VM, read data that was stored in that segment. The read should succeed
#    because the fetcher pulls the segment from /tmp/elide-store.
#    "[demand-fetch enabled]" appears in the serve-volume output at startup.
```

**Current limitation — no eviction:** step 6 (manual deletion) is only necessary because eviction is not yet implemented. Once a size-cap LRU eviction policy is added, the fetcher fires automatically when `segments/` exceeds the cap and old segments are removed. Until then, demand-fetch is wired and testable but does not activate in normal operation.

**Next step: eviction.** Track segment access time (mtime touch on fetch or read), enforce a configurable `max_cache_bytes` in `fetch.toml`, and evict least-recently-used segments from `segments/` (never from `pending/`). Eviction + demand-fetch together make `segments/` a transparent cache tier.

## GC and Repacking

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
