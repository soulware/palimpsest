# Design

## Problem Statement

Running many VMs at scale on shared infrastructure presents a storage challenge: base images are largely identical across instances, evolve incrementally over time, and yet are conventionally treated as independent copies. Each VM gets its own full copy of the image, even though 90%+ of the data is shared and most of it is never read at runtime.

The goal is a block storage system that minimises storage cost, minimises cold-start latency, and handles image updates efficiently at scale. The approach combines four techniques that individually exist but have not previously been integrated in this way:

- **Log-structured virtual disk (LSVD)** — write ordering and local durability, with S3 as the large capacity tier
- **Demand-fetch** — only retrieve data from S3 when it is actually needed; data never accessed is never transferred
- **Content-addressed dedup** — identical chunks are stored once regardless of how many volumes reference them
- **Delta compression** — near-identical chunks (e.g. a file updated by a security patch) are stored as small deltas against their predecessors, reducing S3 fetch size for updated images

The combination is particularly effective for the VM-at-scale use case because: base images are highly repetitive across VMs (dedup captures shared content), images evolve incrementally (delta compression captures changed content), most of each image is never read at runtime (demand-fetch avoids fetching unused data), and the same image is booted many times (locality optimisation pays back repeatedly).

## Key Concepts

**Block** — the fundamental unit of a block device, 4KB. This is what the VM sees.

**Extent** — a contiguous run of blocks at adjacent LBA addresses, and the fundamental unit of dedup and storage. Extents are variable-size and identified by the BLAKE3 hash of their content. BLAKE3 is chosen because: it is as fast as non-cryptographic hashes on modern hardware (SIMD-parallel tree construction), its 256-bit output makes accidental collisions negligible (birthday bound ~2^128 operations), and it has first-class Rust support. Collisions cannot be prevented by any hash function, but with 256-bit output the probability is effectively zero for any realistic chunk count.

Note: "extent" is also an ext4 term — an ext4 extent is a mapping from a range of logical file blocks to physical disk blocks, recorded in the inode extent tree. These are distinct concepts. Where the distinction matters, "ext4 extent" or "filesystem extent" refers to the filesystem structure; "extent" alone refers to the LSVD storage unit.

**Live write extents** are bounded by fsync and contiguous LBA writes. A write to LBA 100–115 and an adjacent write to LBA 116–131 arriving before the next fsync are coalesced into one extent covering LBA 100–131. Writes to non-contiguous LBAs stay as separate extents regardless of fsync timing — coalescing only applies to adjacent LBA ranges. Live write extents are **opportunistic dedup candidates**: full-file writes (e.g. `apt install`) may happen to align with file boundaries and dedup well; partial file edits will not.

**Manifest** — a sorted list of `(start_LBA, length, extent_hash)` triples describing the complete state of a volume at a point in time. Held in memory on the host for running volumes. **The manifest is always derivable from the segments** — each segment carries the LBA metadata for the extents it contains, so the manifest can be reconstructed by scanning segments on startup. S3 persistence of the manifest is an optimisation (to avoid expensive segment scans at startup), not a correctness requirement.

**Snapshot** — a frozen, immutable manifest. Snapshots and images are the same thing — there is no separate image concept. A snapshot is taken by freezing the current in-memory manifest. Since extents are immutable and content-addressed, no data is copied. Snapshot identity is `blake3(all extent hashes in LBA order)` — derived from the manifest, not stored separately.

**Segment** — a packed collection of extents, typically ~32MB, stored as a single S3 object. The 32MB size is a soft flush threshold inherited from the lab47/lsvd reference implementation. Each segment carries the LBA metadata for its extents, making the manifest reconstructible from segments alone. Segments are the unit of S3 I/O.

**Write log** — the local durability boundary. Writes land here first (fsync = durable). Extents are promoted to segments in the background.

**Extent index** — maps `extent_hash → S3 location`. Tells the read path where in S3 a given extent lives. Maintained by the global service, updated by GC when extents are repacked.

## Operation Modes

The system operates in two tiers depending on whether the volume's filesystem is understood:

**Basic LSVD** (any filesystem or raw block usage):
- Write coalescing, local durability, demand-fetch, S3 backend, snapshots — all work correctly
- Dedup is opportunistic: fsync-bounded extents may or may not align with file boundaries
- Cross-version dedup quality is low without alignment — raw fixed-offset blocks yield ~1% overlap between image versions
- Suitable for data volumes, Windows VMs, XFS/btrfs volumes, or any use case where dedup is not the primary concern

**Enhanced LSVD + dedup** (ext4 volumes):
- Everything above, plus snapshot-time ext4 re-alignment of extents to file boundaries
- Reliable file-aligned extents → ~84% exact match between Ubuntu point releases
- Delta compression maximally effective because extents correspond to files
- The approximation "one extent ≈ one file" holds well — the palimpsest `extents` subcommand, which parses ext4 inode extent trees directly, is the prototype for this

The block device itself is filesystem-agnostic in both modes. ext4 awareness is an optional layer that sits alongside the snapshot process — it re-slices and re-hashes extents at file boundaries using the ext4 extent tree as ground truth. A coalesced extent spanning multiple files is split; multiple extents covering one file are merged. The live write path is unaffected in either mode.

Other filesystem parsers (XFS, btrfs) could bring additional filesystems into the enhanced tier over time. The interface is simply: "given this volume at snapshot time, return file extent boundaries."

## Architecture

Two components run on each host:

**Per-volume process** — one per running VM. Owns the ublk/NBD frontend, the live manifest (in memory), the write log (local NVMe), and the per-volume extent cache. Classifies extents by entropy, routes low-entropy extents to the global service for dedup, stores high-entropy extents directly in per-volume segments.

**Global service** — one per host. Owns the extent index (on-disk), the in-memory filter (xor/ribbon), and the host-level read cache. Handles dedup lookups, segment packing, S3 upload/download, and GC.

S3 is shared across all hosts. Segments from any volume on any host land in a single shared namespace. The in-memory manifest (reconstructible from segments) and extent index together replace the per-volume segment list of the reference LSVD implementation.

```
VM
 │  block I/O (ublk / NBD)
 ▼
Per-volume process
 │  write path: buffer → extent boundary → hash → dedup check
 │  read path:  LBA → manifest → hash → local cache → S3
 │
 ├─ Write log (local NVMe, durability boundary)
 ├─ Live manifest (in memory, LBA → hash, reconstructible from segments)
 └─ Global service client
      │
      ▼
Global service (per host)
 ├─ Extent index (on-disk, hash → S3 location)
 ├─ Xor/ribbon filter (in memory, ~100MB)
 ├─ Read cache (small, absorbs S3 fetch bursts)
 └─ S3 (shared, all hosts)
      ├─ segments/<id>  — packed extents (carry LBA metadata)
      └─ index/extent-index  — global extent hash → location
```

## Write Path

```
1. VM issues write for LBA range
2. Buffer contiguous writes; each non-contiguous LBA gap finalises an extent
3. For each extent:
   a. Entropy check
      - High entropy → local tier (per-volume segment, no dedup)
      - Low entropy  → continue
   b. Hash extent content → extent_hash
   c. Check per-volume extent cache (in memory)
      - Hit  → point LBA range at existing extent, done
      - Miss → continue
   d. Check xor/ribbon filter (in memory)
      - Miss → new extent, store it
      - Hit  → check extent index on disk to confirm
   e. If new: write to write log (fsync = durable), promote to segment in background
   f. If duplicate: reference existing extent, no write
4. Update live manifest with new (start_LBA, length, extent_hash) entries
```

Durability is at the write log. S3 upload is asynchronous and not on the critical path.

Live write dedup is **opportunistic** — extents are fsync-bounded and contiguous-LBA-bounded, not file-aligned. Full file writes (e.g. `apt install`, library replacement) may happen to align with file boundaries and dedup well; partial file edits will not. Reliable file-aligned dedup happens at snapshot time via ext4 re-alignment.

## Read Path

```
1. VM reads LBA range
2. Look up LBA in live manifest → chunk hash H
3. Check local cache for H
   - Hit  → return data
   - Miss → look up H in chunk index → S3 location
4. Fetch chunk from S3, populate local cache
5. Return data to VM
```

The kernel page cache sits above the block device and handles most hot reads — the system never sees page cache hits. The local chunk cache is a small S3 fetch buffer, not a general-purpose cache.

## Manifest Format

The manifest is primarily an **in-memory data structure** — a sorted list of `(start_LBA, length, extent_hash)` triples. It is always reconstructible by scanning segment metadata, so S3 persistence is an optimisation rather than a requirement.

When persisted (at snapshot time, or as a startup cache), the format is a binary flat file:

**Header (84 bytes):**

| Offset | Size | Field        | Description                          |
|--------|------|--------------|--------------------------------------|
| 0      | 8    | magic        | `PLMPST\x00\x02`                     |
| 8      | 32   | volume_id    | blake3 of all extent hashes in LBA order |
| 40     | 32   | parent_id    | snapshot_id of parent; zeros = root  |
| 72     | 4    | entry_count  | number of entries (u32 le)           |
| 76     | 8    | timestamp    | unix seconds (u64 le)                |

**Entries (44 bytes each, sorted by start_LBA):**

| Offset | Size | Field      | Description                          |
|--------|------|------------|--------------------------------------|
| 0      | 8    | start_lba  | first logical block address (u64 le) |
| 4      | 4    | length     | extent length in 4KB blocks (u32 le) |
| 12     | 32   | hash       | BLAKE3 extent hash                   |

One entry per extent. Unwritten LBA ranges have no entry (implicitly zero).

**Snapshot identity:** `snapshot_id = blake3(all extent hashes in LBA order)` — derived from the in-memory manifest, not from the file bytes. Identical volume state always produces the same snapshot_id regardless of when or where the manifest was serialised.

**Volume identity:** `volume_id = blake3(all extent hashes in LBA order)` — same derivation. Two independently-generated snapshots of the same image produce the same `volume_id`.

**Parent chain:** `parent_id` references the `snapshot_id` of the previous snapshot, enabling chain traversal.

**Reconstruction:** on startup, if no cached manifest is available (or the cache is stale), the manifest is rebuilt by scanning all segment metadata headers. Each segment records the `(start_LBA, length, extent_hash)` of every extent it contains.

## Extent Index

Maps `extent_hash → S3 location`. Separate from the manifest — the manifest is purely logical (what data is at each LBA range), the extent index is physical (where that data lives in S3).

This separation means GC can repack extents (changing their S3 location) by updating only the extent index. Manifests are never rewritten after being frozen.

The extent index also stores delta compression metadata: if extent B is stored as a delta against extent A, the index records `hash_B → {segment, offset, delta_source: hash_A}`. The manifest is unaware of this — it just records `(start_lba, length, hash_B)`. The read path fetches the delta and the source extent, reconstructs B, and caches the full extent locally.

**In-memory filter:** an xor or ribbon filter (~100MB for 80M entries) guards the on-disk index. Extents not in the filter are definitively new — no disk lookup needed. False positives fall through to disk. Filter is rebuilt periodically during GC sweep.

## Dedup

**Exact dedup:** two extents with the same BLAKE3 hash are identical. The second write costs nothing — the manifest is updated to point the new LBA range at the existing extent. No data stored, no S3 upload.

**Delta compression:** extents that are similar but not identical (e.g. a file updated by a security patch) can be stored as a delta against a known extent. Applied at S3 upload time — the local cache always holds full reconstructed extents. The benefit is reduced S3 fetch size, not storage cost. The primary value is latency: fetching a small delta instead of a full extent from S3 is dramatically faster on the cold-read path.

**Delta source selection** is trivial at the extent level: the natural reference for a changed file is the same-path file in the previous snapshot. No similarity search required — the manifest parent chain gives direct access to the prior version of each extent.

Delta compression is compelling for point-release image updates; not worth the complexity for cross-version (major version) updates where content is genuinely different throughout.

**Empirically measured (Ubuntu 22.04 point releases, 14 months apart):**
- 84% of file extents are exact matches by count (zero marginal cost)
- 35% of bytes are covered by exact-match extents; the remaining 65% are in files touched by security patches
- The 65% in changed extents is the delta compression target: whole-file deltas against the previous snapshot's copy, which are typically tiny (a patch changes a small region of a large binary)
- Overall marginal S3 fetch to advance from one point release to the next: ~94% saving vs fetching fresh

## Volume Types and Namespace Scoping

Volumes have a type that determines which chunk namespace they participate in.

**Image volumes** (rootfs, shared base images) — opt into the global dedup namespace. Low-entropy chunks are routed to the global service for dedup check and shared index storage. Boot hint sets are accumulated and repacking for locality applies.

**Data volumes** (databases, application data) — never touch the global chunk namespace. Chunks go directly to per-volume S3 segments with no dedup check. Still benefit from the local NVMe cache tier, free snapshots, and cheap migration. Kept out of the global namespace to avoid index pollution with high-churn, low-hit-rate entries.

Snapshot manifests are uniform across volume types — snapshot management is identical regardless of type. Only the chunk storage routing differs.

**Routing at write time:**
- `volume.type == Image && entropy(extent) < threshold` → global service (dedup check)
- Everything else → per-volume segments (no dedup)

**Open question:** binary global/non-global routing may not be granular enough. Hierarchical namespaces (global → org → image-family → volume) are a plausible future requirement. The design should treat namespace as an attribute of the volume, not a boolean flag.

## Snapshots

A snapshot is a frozen manifest. Taking a snapshot is cheap: copy the current in-memory manifest, assign a snapshot_id, write to S3. Cost is proportional to manifest size, not volume size.

**Snapshots are images.** There is no separate image concept. Deploying a new image version means taking a snapshot on a configured VM and distributing the manifest. The storage layer handles dedup, delta compression, and locality transparently — the snapshot mechanism is unaware of them.

**Taking a snapshot:**

```
1. Pause writes briefly (or use a copy-on-write fence)
2. Freeze the current in-memory manifest → snapshot manifest
3. [Enhanced mode only] Parse ext4 extent tree from the frozen filesystem state
   Re-align extents to file boundaries: re-slice data at ext4 boundaries, re-hash
   - Coalesced extents spanning multiple files → split into per-file extents
   - Multiple extents covering one file → merged into one extent
4. Write snapshot manifest to S3
5. Write consolidated extent index to S3 (see below)
6. Resume writes
```

In enhanced mode, the ext4 re-alignment is what makes snapshot extents reliable dedup candidates. The ext4 metadata is the ground truth; the current manifest's extent boundaries are discarded and replaced with file-aligned ones. In basic mode, the manifest is frozen as-is — dedup quality is lower but correctness is unaffected.

**A snapshot is self-contained.** At snapshot time, all referenced extents are guaranteed to be in S3. This is the natural moment to write a consolidated extent index covering exactly those extents:

```
s3://bucket/snapshots/<id>/manifest   — LBA → extent hash
s3://bucket/snapshots/<id>/index      — extent hash → segment+offset
```

The snapshot index covers no more and no less than what is needed to read that snapshot. Serving a specific image on a new host requires only these two files — no global index scan, no segment enumeration.

**Cross-image dedup on a new host** is bootstrapped by unioning the snapshot indexes for the images being served. Only the relevant snapshots need to be loaded, not a global index of everything that ever existed.

**The gap between snapshots** — extents written since the last snapshot — is covered by per-segment `.idx` files (see S3 Layout). Recovery is therefore: latest snapshot index + `.idx` files for segments written since.

**GC interaction:** the GC sweep walks all manifests, including frozen snapshots. Extents referenced by any snapshot are kept alive. Deleting a snapshot releases its extent references; the next GC sweep reclaims extents no longer referenced by any remaining manifest.

**Rollback:** replace the live manifest with a snapshot manifest and discard the write log since the snapshot point. Instant at the block device level.

**Migration and disaster recovery** share the snapshot code path: start a volume from a snapshot manifest on a new host. One operation, multiple use cases.

## S3 Layout and Index

Segments are the unit of S3 storage. Each segment is a packed collection of extents (~32MB). Alongside each segment, a small companion index file is written:

```
s3://bucket/segments/<id>       — packed extents (~32MB)
s3://bucket/segments/<id>.idx   — extent hash → byte offset within segment
```

The `.idx` file is small: ~40 bytes per extent × ~1000 extents per 32MB segment ≈ ~40KB, roughly 1/700th the size of the segment. It is written atomically with the segment upload.

**Inline extents:** small extents are stored directly in the `.idx` file rather than referenced by offset into the segment. A byte-range GET has fixed overhead (latency + S3 request cost) that dominates for tiny extents — inlining eliminates that round-trip entirely. The `.idx` entry format distinguishes the two cases:

```
For each entry:
  hash   (32 bytes)
  flags  (1 byte: reference | inline)
  if reference: offset (8 bytes) + length (4 bytes)
  if inline:    length (4 bytes) + data bytes
```

This is particularly effective for the boot path: small config files, scripts, and locale data appear frequently during boot and are naturally small. Inlining them means loading the snapshot index delivers those extents with zero additional S3 requests. The empirical finding that 84% of extents by count are small files (representing only 35% of bytes) means inlining captures the majority of extents with modest `.idx` size growth.

The inline threshold is an open question — too low misses most small extents, too high bloats `.idx` files and makes index reconstruction expensive. A threshold in the range of a few KB seems reasonable; the actual extent size distribution from image analysis should drive the final value.

**The extent index on the global service** (on-disk, hash → S3 location) is built from these `.idx` files. It is a cache — always reconstructible from S3 without reading segment data. On a cold start or after index loss, reconstruction is: download all `.idx` files (fast, small) rather than scanning all segment data (slow, large).

**Snapshot indexes** are consolidated views written at snapshot time. They cover all extents reachable from that snapshot and remain immutable. A snapshot index is smaller than the full global index — it contains only live extents at that point in time, not historical or GC'd ones.

**Index recovery flow:**

```
1. Download latest snapshot index for each relevant image
2. Download .idx files for segments written since that snapshot
3. Union → full extent index for the extents you care about
4. Rebuild xor/ribbon filter from index
```

**Adaptive segment fetch:** when extents are needed from a segment, the `.idx` file is consulted first to subtract any already cached locally. The fetch strategy for the remainder is then chosen based on how much of the segment is needed:

```
missing = extents needed from segment not in local cache
if missing.bytes / segment.total_bytes > threshold:
    fetch full segment → populate cache with all extents
else:
    byte-range GET per missing extent individually
```

A full segment fetch amortises the S3 request overhead across all extents; individual byte-range GETs avoid transferring data that won't be used. The threshold is byte-ratio based rather than count-based, since extents are variable size. Boot-hint repacking makes this decision easy in the common case — boot extents are co-located in boot segments, so the ratio is high and a full segment fetch triggers naturally.

**Range coalescing** (fetching one range covering multiple nearby extents, accepting some wasted bytes for gaps) is a possible intermediate strategy but probably not worth the added complexity. The two-choice strategy covers the common cases well, repacking eliminates most of the messy intermediate cases, and gap tolerance would introduce another tunable parameter. Worth revisiting if profiling shows the intermediate case is significant in practice.

**Segment indexes are the ground truth.** The global service's on-disk extent index, the in-memory filter, and the snapshot indexes are all derived from segment `.idx` files. Segments (data + `.idx`) are the canonical record — everything else is a cache.

## GC and Repacking

**Standard GC:** walk all manifests, build the set of live extent hashes, delete unreferenced extents from S3 after a grace period. No per-write refcounting — the manifest scan is the reference count.

**Delta dependency handling:** when a source extent is about to be deleted and a live delta depends on it, materialise the delta first (fetch source + delta → full extent, write full extent to S3, update extent index). Then delete the source. The dependency map is derived fresh each GC sweep from the extent index — no persistent reverse index needed.

**Access-pattern-driven repacking:** GC extends beyond space reclamation to also improve data locality. Boot-path extents — identified from observed access patterns during VM startup — are co-located in dedicated segments. A cold VM boot then fetches one or two S3 segments to get everything needed for boot, rather than many scattered segments.

**Boot hint accumulation:** every VM boot records which extents were accessed during the boot phase (identified by time window after volume attach, or explicit VM lifecycle signals from the hypervisor). These observations accumulate per snapshot. After sufficient boots (converges quickly at scale — 500 VMs/day = 500 observations/day), the hint set is stable enough to guide repacking decisions.

**Continuous improvement:** first boot is cold; boot access patterns are recorded; next GC repack co-locates those extents; subsequent boots are faster. The feedback loop strengthens with scale. This is novel in production block storage — most S3-backed systems are write-once and never reorganise for locality.

**Snapshot-aligned repacking:** GC can reorganise segments around snapshot boundaries, converging toward a two-tier layout:

```
s3://bucket/segments/base-<hash>     — extents shared across many snapshots
s3://bucket/segments/snap-<id>-N     — extents unique to a specific snapshot
```

Shared extents (e.g. the ~84% identical between Ubuntu 22.04 point releases) are consolidated into base segments. A new host serving any image from the same family fetches these once — they are amortised across all versions. Snapshot-specific segments contain only the extents that changed, stored as deltas against their counterparts in the base segments.

The result is that bringing up a new host to serve a specific snapshot requires fetching only:
1. **Base segments** — large but shared; already cached if any image from the same family has been served on this host
2. **Snapshot-specific segments** — small; contain only the changed extents as deltas

This unifies the repacking objectives: boot hint ordering applies *within* segments (extents ordered by boot access sequence), while snapshot alignment determines *which* extents go into which segment. The snapshot index records exactly which base segments and snapshot-specific segments are needed, making new host setup fully declarative.

GC repacking loop: observe which extents are referenced across multiple snapshots → consolidate into base segments → write per-snapshot remainder into small delta segments → update snapshot indexes. Layout converges toward optimal with each GC cycle.

**Ext4 re-alignment during GC:** GC is a natural point to perform or improve extent re-alignment, not just snapshot time. The scope differs by what is being processed:

- **Snapshot manifests:** safe and clean — a snapshot is frozen, the filesystem state is fixed. GC can parse ext4 metadata from the snapshot and re-align any snapshot that was not aligned at creation time (e.g. created in basic mode). This retroactively upgrades basic-mode snapshots to enhanced dedup quality.

- **Live volumes:** the filesystem is in flux; parsing ext4 directly risks inconsistency. GC instead uses the most recent snapshot's ext4 metadata as a proxy for the current filesystem layout. Re-alignment is approximate (files created or moved since the snapshot won't be captured) but safe — dedup quality improves progressively without risk of data corruption.

The effect is that volumes gain better extent alignment over successive GC cycles regardless of whether explicit enhanced-mode snapshots were taken. Dedup quality converges upward automatically.

## Filesystem Metadata Awareness

Since the system controls the underlying block device, it sees every write — including writes to ext4 metadata structures (superblock, group descriptors, inode tables, extent trees, journal). This visibility is an opportunity to handle metadata blocks smarter than opaque data blocks.

**Metadata extent tagging:** once metadata LBAs are identified from the superblock (all at well-defined offsets), those extents can be tagged in the manifest. Tagged metadata extents receive special treatment:
- Skip dedup — inode tables and group descriptors are volume-specific (unique inode numbers, volume-specific block addresses) and will never match across volumes
- Stay local-tier — no point routing through the global dedup service
- Cache aggressively — metadata blocks are hot; every filesystem operation reads them

**Incremental shadow filesystem view:** because every write to a known metadata LBA is visible, the system could maintain a continuously-updated internal view of the filesystem layout — which LBA ranges belong to which files, as files are created, deleted, and modified. At snapshot time, the shadow view is already current: no parse-from-scratch, re-alignment is essentially free.

**The journal problem:** ext4 metadata does not go directly to its final LBA. It is written to the jbd2 journal first (write → journal commit → checkpoint to final location). There are three levels of journal handling with very different complexity profiles:

- **Level 1 — detect that metadata changed (trivial):** writes to journal LBAs are visible at the block device level. We know a transaction is in flight but not what changed. Useful only for invalidating the shadow view ("metadata changed, re-parse at next opportunity"). No journal parsing required.

- **Level 2 — parse committed transactions at snapshot/GC time (moderate):** at a known-clean point (snapshot or GC checkpoint), read the journal, walk committed transactions, and replay them to recover current metadata state. This is what `e2fsck` does. The jbd2 format is well-documented with reference implementations in `e2fsprogs` and the kernel. A few hundred lines of careful Rust. Risk is low — we are parsing a frozen, consistent state. This is sufficient for snapshot and GC re-alignment.

- **Level 3 — live transaction tracking (high):** intercept journal writes in real time, parse each transaction as it commits, and update the shadow view incrementally. Requires recognising journal LBAs in the write stream, parsing jbd2 descriptor blocks to correlate data blocks with their final destinations, and correctly handling the circular log structure, transaction abort/rollback, and journal checkpointing. Getting this wrong silently produces an incorrect shadow view. The kernel's jbd2 module is the authoritative reference and is non-trivial.

One simplifying factor across all levels: ext4's default journaling mode is `data=ordered` — only metadata goes through the journal; data blocks are written directly to their final locations. Journal handling is therefore scoped to metadata only, not the full write stream.

**Recommended approach:** implement Level 2 first — sufficient for snapshot/GC re-alignment and well-understood. Level 3 (live shadow view) is only needed for real-time file-identity-aware dedup decisions and should be deferred until Level 2 is working well.

**Future potential:** a live shadow filesystem view would enable real-time dedup decisions informed by file identity — knowing that a write is to a known shared library vs. a per-VM log file, for example, without waiting for snapshot time. This is a significant capability that falls out naturally from controlling the block device, and is worth designing toward even if not implemented immediately.

## Empirical Findings

Measured using `palimpsest` — a Rust tool purpose-built to explore these concepts against real Ubuntu images.

### Demand-fetch: how much of an image is actually read?

Ubuntu 22.04 minimal cloud image (2.1GB root partition, 68,512 × 32KB chunks):

| Stage | Chunks read | Data | % of image |
|---|---|---|---|
| Full systemd boot to login prompt | 4,159 | 130 MB | 6.1% |
| + all shared libraries | 923 | 29 MB | 7.6% cumulative |
| + all of /usr/share | 4,244 | 133 MB | 13.8% cumulative |
| + all executables | 1,289 | 40 MB | 15.7% cumulative |

**93.9% of the image is never read during a full boot.** Even exhaustive use of the system touches only ~16% of the raw image (including unallocated space; ~35% of actual filesystem data).

### Dedup: extent overlap between image versions

Extent-level dedup using inode-based physical extent boundaries:

| Comparison | Exact extent overlap (count) | Exact extent overlap (bytes) |
|---|---|---|
| 22.04 point releases (14 months apart) | 84% | 35% |

The count/bytes divergence reveals the size distribution: the 84% of extents that match are predominantly small files (configs, scripts, locale data). The 16% that don't match are the large files (libraries, executables) touched by security patches — these account for 65% of bytes. That 65% is the delta compression target.

For comparison, earlier analysis using fixed-size file-content-aware chunking:

| Approach | Exact overlap |
|---|---|
| 32KB chunks, file-aligned | ~70% of chunks |
| Raw block-level (fixed offsets) | ~1% of chunks |

The chunk-level 70% includes partial credit — a library with a 200-byte patch still contributes 31/32 unchanged chunks. Extent-level loses that partial credit but recovers it via delta compression at a coarser, more natural granularity (whole-file deltas with trivial source selection).

### Delta compression: marginal S3 cost

| Scenario | Exact dedup | Delta benefit | Marginal fetch |
|---|---|---|---|
| 22.04 point release | 67% exact | 56% of remainder | ~43MB of ~700MB (~94% saving) |
| 22.04 vs 24.04 | 19% exact | 13% of remainder | ~95MB of ~700MB (~86% saving) |

The 22.04 vs 24.04 saving (86%) is almost entirely from compression — delta contributes little. For point releases, delta compression does the heavy lifting.

In production, the relevant comparison is always point-release: continuous deployment means each update is a small delta from the previous. The system always operates in the point-release regime, never the major-version regime.

### Manifest size

Ubuntu 22.04 (~762MB of file data): ~33,700 extents. At 44 bytes per entry, the binary manifest is ~1.5MB. Well within "a few MB" as expected.

## Open Questions

- **Inline extent threshold:** extents below this size are stored inline in `.idx` files rather than referenced by segment offset. Needs empirical validation against the actual extent size distribution in target images.
- **Entropy threshold:** 7.0 bits used in experiments, taken from the lab47/lsvd reference implementation. Optimal value depends on workload mix.
- **Segment size:** ~32MB soft threshold, taken from the lab47/lsvd reference implementation (`FlushThreshHold = 32MB`). Not a hard maximum — a segment closes when it exceeds the threshold. Optimal value depends on S3 request cost vs read amplification tradeoff.
- **Write log format:** not yet designed. Affects recovery, promotion to segments, snapshot consistency.
- **Extent index implementation:** sled, rocksdb, or custom. Needs random reads and range scans.
- **Shared extent index:** per-host or shared service? DynamoDB, S3-backed, or dedicated process?
- **Extent boundary detection on the live write path:** buffering contiguous writes into extents is straightforward; the open question is the coalescing window — how long to wait for a burst to complete before finalising an extent. Too short fragments large files; too long adds write latency.
- **Manifest cache invalidation:** the reference implementation (lab47/lsvd) validates the cached manifest against a hash of current segment IDs. Same approach applies here.
- **Namespace granularity:** binary global/non-global may not be sufficient for multi-tenancy.
- **Boot hint persistence:** where are hint sets stored, how are they distributed across hosts?
- **Empirical validation of repacking benefit:** measure segment fetch count before and after access-pattern-driven repacking.
- **ublk integration:** Linux-only, io_uring-based. NBD kept for development and macOS.
