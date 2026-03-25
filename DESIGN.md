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
      ├─ segments/<ULID>  — packed extents (carry LBA metadata)
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

## LBA Map

The **LBA map** is the live in-memory data structure mapping logical block addresses to content. It is a sorted structure (B-tree or equivalent) keyed by `start_LBA`, where each entry holds `(start_lba, lba_length, extent_hash)`. It is updated on every write (new entries added, existing entries trimmed or replaced for overwrites) and is the authoritative source for read path lookups.

**Contrast with lab47/lsvd:** the reference implementation calls this `lba2pba` and maps `LBA → segment+offset` (physical location). GC repacking must update it for every moved extent. Palimpsest maps `LBA → hash` — the logical layer. Physical location (`hash → segment+offset`) is a separate extent index. This two-level indirection means GC repacking updates only the extent index; the LBA map is never rewritten for GC. Manifests and snapshots become immutable once written.

### LBA map persistence

The LBA map is persisted to a local `lba.map` file on clean shutdown and used as a fast-start cache on restart — analogous to the reference implementation's `head.map`.

**Freshness guard:** the file includes a BLAKE3 hash of the sorted list of all current local segment IDs. On startup, if the guard matches the current segment list, the cached LBA map is loaded directly without scanning `.idx` files. If the guard doesn't match (new segments were written or old ones removed since last checkpoint), the LBA map is rebuilt from scratch.

**Rebuild procedure:**
1. Scan all local segment `.idx` files (fast — `.idx` files are small, ~60KB each)
2. For each `.idx` entry, apply to the LBA map (later segments take precedence for any overlapping LBA range)
3. Replay any WAL file(s) on top (WAL entries are the most recent writes)

Since `.idx` files are the ground truth for segment contents, rebuilding the LBA map requires only `.idx` files and the WAL — never the segment data bodies. A full startup rebuild for a volume with 100 segments is a scan of ~6MB of `.idx` data, not 3GB of segment bodies.

### Manifest format

"Manifest" refers specifically to the **serialised form** written at snapshot time (or as a startup cache). The live LBA map is the source; the manifest is a point-in-time freeze of it.

When persisted, the format is a binary flat file:

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

**Snapshot identity:** `snapshot_id = blake3(all extent hashes in LBA order)` — derived from the live LBA map, not from the file bytes. Identical volume state always produces the same snapshot_id regardless of when or where the manifest was serialised.

**Volume identity:** `volume_id = blake3(all extent hashes in LBA order)` — same derivation. Two independently-generated snapshots of the same image produce the same `volume_id`.

**Parent chain:** `parent_id` references the `snapshot_id` of the previous snapshot, enabling chain traversal.

## Extent Index

Maps `extent_hash → S3 location`. Separate from the manifest — the manifest is purely logical (what data is at each LBA range), the extent index is physical (where that data lives in S3).

**Contrast with lab47/lsvd:** the reference implementation uses a single `lba2pba` map — a direct `LBA → segment+offset` (physical location) index. GC repacking requires updating this map for every moved extent. The manifest + extent index split replaces that single map with two levels of indirection: the manifest is purely logical (`LBA → hash`) and never changes after a snapshot is frozen; only the extent index (`hash → physical location`) is updated during GC. Manifests and snapshots are therefore immutable once written.

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
s3://bucket/segments/<ULID>       — packed extents (~32MB), raw bytes
s3://bucket/segments/<ULID>.idx   — per-extent metadata (LBA, hash, body location)
```

The segment body is raw bytes — concatenated extent data with no per-extent framing. All structure lives in the `.idx` file. The `.idx` is always fetched before the segment body: it tells the read path whether a segment fetch is needed at all, and if so which byte ranges to request.

The `.idx` file is small: ~60 bytes per extent × ~1000 extents per 32MB segment ≈ ~60KB, roughly 1/500th the size of the segment. It is written atomically with the segment upload.

**`.idx` entry format:**

```
For each entry:
  hash        (32 bytes) — BLAKE3 extent hash
  start_lba   (8 bytes)  — first logical block address (u64 le)
  lba_length  (4 bytes)  — extent length in 4KB blocks (u32 le); uncompressed size = lba_length × 4096
  flags       (1 byte)   — bits: 0=inline, 1=delta, 2=compressed

  if delta:
    source_hash  (32 bytes) — BLAKE3 hash of the source extent to apply delta against

  if !inline (reference):
    body_offset  (8 bytes) — byte offset within segment body (u64 le)
    body_length  (4 bytes) — byte length in segment body (u32 le); equals lba_length×4096 if not compressed

  if inline:
    body_length  (4 bytes) — byte length of inline data (u32 le)
    data bytes   (body_length bytes) — extent or delta bytes, compressed if flag set
```

`inline` and `delta` are orthogonal bits: a delta can be inlined (delta bytes stored directly in `.idx`) or referenced (delta bytes in segment body). Inlined deltas are common — delta bytes for a patched file are often small, and storing them in `.idx` eliminates a byte-range GET against the segment body. The source extent is always fetched separately via its own hash lookup regardless.

The `.idx` entry serves two purposes with a single scan: manifest reconstruction (`start_lba + lba_length + hash`) and extent index population (`hash → segment_id + body_offset + body_length`). No separate pass needed. These could be split into separate files (the global service only needs the hash→location part; the per-volume process only needs the LBA part), but the hash would then be stored twice and two S3 fetches would be required. Since `.idx` files are small and manifest reconstruction is a rare cold-start path, a single combined file is the better tradeoff.

**Compression:** low-entropy extents (those routed through the global service) are compressed with zstd before being written to the segment body. The `compressed` flag is set in `.idx`; `body_length` records the compressed size, which is what determines byte-range GET size. `lba_length × 4096` always gives the uncompressed size — no second size field needed. High-entropy extents (routed to per-volume segments) are stored uncompressed; the entropy check already tells us compression will not help.

**Inline extents:** small extents are stored directly in the `.idx` file rather than referenced by offset into the segment. A byte-range GET has fixed overhead (latency + S3 request cost) that dominates for tiny extents — inlining eliminates that round-trip entirely.

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

See [FINDINGS.md](FINDINGS.md) for full measurements. Key results:

- **93.9% of a 2.1GB Ubuntu image is never read during a full boot** — validates the demand-fetch model
- **84% of extents match exactly between 22.04 point releases** (by count); 35% by bytes — the large changed files are the delta compression target
- **~94% marginal S3 fetch saving** for a point-release update (exact dedup + delta compression combined)
- **~1.5MB manifest** for a 762MB filesystem (~33,700 extents at 44 bytes each)

## Write Log

The write log is the local durability boundary. Writes land here on fsync; the log is promoted to a segment and uploaded to S3 in the background.

### File format

A single append-only file per in-progress segment, living at `wal/<ULID>`. This is the same shape as the lsvd reference implementation — one file, records appended sequentially, no separate index. On promotion the file is renamed to `pending/<ULID>` (zero copy); no data is written.

**Magic header:** `PLMPWL\x00\x01` (8 bytes)

**Record types:**

*DATA record* — a new extent with its payload:
```
hash        (32 bytes)    BLAKE3 extent hash
start_lba   (u64 varint)  first logical block address
lba_length  (u32 varint)  extent length in 4KB blocks
flags       (u8)          see flag bits below
data_length (u32 varint)  byte length of payload (compressed size if FLAG_COMPRESSED)
data        (data_length bytes)
```

*REF record* — a dedup reference; no data payload, maps an LBA range to an existing extent:
```
hash        (32 bytes)    BLAKE3 hash of the existing extent
start_lba   (u64 varint)
lba_length  (u32 varint)
flags       (u8)          FLAG_DEDUP_REF set; no further fields
```

**Flag bits:**
- `0x01` `FLAG_COMPRESSED` — payload is zstd-compressed; `data_length` is compressed size
- `0x02` `FLAG_DEDUP_REF` — REF record; no data payload
- `0x04` `FLAG_HIGH_ENTROPY` — bypass global dedup service; store in per-volume segment only

The hash is computed before the dedup check and stored in the log record. Recovery can reconstruct the manifest without re-reading or re-hashing the data.

### Pre-log coalescing

Contiguous LBA writes are merged in memory before they reach the write log — in the NBD/ublk handler, not in the log itself. This mirrors lsvd's `pendingWrite` buffer. The coalescing window is bounded by both a block count limit (to prevent unbounded memory accumulation between fsyncs) and the fsync boundary (a guest fsync flushes any pending buffer). The write log only ever sees finalised, already-coalesced extents.

### Durability model

```
write arrives → in-memory coalescing buffer
                        │
               count limit or fsync
                        │
                        ▼
               hash → dedup check → append_data / append_ref → bufio (OS buffer)
                        │
                    guest fsync
                        │
                        ▼
               logF.sync_data() ← write log durable on local disk; reply sent to guest
                        │
                [background, async]
                        │
                        ▼
               segment close → local segment file + .idx written → S3 upload
```

After a guest fsync returns, all prior writes are durable in the write log on local NVMe. S3 upload is asynchronous and not on the fsync critical path. This matches lsvd's two-tier durability model exactly.

### Crash recovery

On startup, if a write log file exists, `scan()` reads it sequentially. If a partial record is found at the tail (power loss mid-write), the file is truncated to the last complete record. All complete records are replayed to reconstruct the in-memory LBA map. The write log is then reopened for continued appending.

**Failure scenarios:**

| Scenario | State on restart | Recovery |
|---|---|---|
| Crash mid-write (before fsync) | WAL tail partial | Truncate WAL to last complete record; replay |
| Crash after fsync, before promotion starts | `wal/<ULID>` intact; nothing in `pending/` | Replay WAL; promote normally |
| Crash during `.idx` write (steps 1–3) | `.idx.tmp` may exist in `pending/`; WAL intact in `wal/` | Delete `.idx.tmp`; replay WAL |
| Crash between `.idx` rename and body rename (steps 3–4) | `pending/<ULID>.idx` exists; WAL intact in `wal/` | `.idx` exists but no body — WAL intact, complete the rename to finish promotion |
| Crash after body rename, before LBA map update (step 4+) | Both `pending/<ULID>` and `.idx` exist | Rebuild LBA map from `pending/` `.idx` files; queue S3 upload |
| Crash mid-upload or after upload before rename (steps 6–7) | Body and `.idx` still in `pending/`; may be in S3 already | Retry upload (idempotent); complete rename on success |
| Crash between body rename and `.idx` rename (steps 7–8) | Body in `segments/`; `.idx` still in `pending/` | Move `pending/<ULID>.idx` → `segments/<ULID>.idx` |
| Total local disk loss | All local state gone | Data loss bounded to writes not yet in S3 — same guarantee as a local SSD |

The commit point (step 4, body rename into `pending/`) requires no fsync of a list file. The WAL is available as fallback for any crash before that rename. After it, the segment is committed and the WAL is gone — recovery reads `.idx` files directly from the filesystem.

The final row is an intentional design choice: local NVMe is the durability boundary, matching the stated goal of "durability semantics similar to a local SSD". S3 is async offload, not the primary durability mechanism.

### Promotion to segment

When the write log reaches the 32MB threshold (or on an explicit flush), the background promotion task converts the WAL into a committed local segment. The WAL is assigned a ULID at creation time; that same ULID becomes the segment ID, tying the two together throughout the promotion.

**Zero-copy promotion:** the WAL file is renamed directly to `pending/<ULID>` — no data is copied. WAL records are `[header | data]`, so the segment body contains embedded WAL headers as inert padding between the extent data regions. The `.idx` `body_offset` for each extent points past the WAL header to the start of that extent's data bytes. Reads use `.idx` byte-range offsets and never parse the embedded headers. GC repacking rewrites segments into clean bodies (no embedded headers) as a side effect of extent coalescing.

The `body_offset` for each extent is recorded at write time: `append_data` returns the byte offset of the data bytes within the WAL file (captured from `self.size` after writing the header, before writing the data). This is what goes into the `.idx` as `body_offset` — correct because the WAL file becomes the segment body unchanged.

**Directory layout:**

```
wal/<ULID>              — WAL file (active or awaiting promotion)
pending/<ULID>          — promoted segment, not yet uploaded to S3
pending/<ULID>.idx      — .idx for a pending segment
segments/<ULID>         — segment confirmed uploaded to S3
segments/<ULID>.idx     — .idx for an uploaded segment
```

Each directory corresponds to one stage in the lifecycle, and each transition is an atomic rename:

```
wal/<ULID>  →  pending/<ULID>  →  segments/<ULID>
```

`wal/` normally contains one entry — the active WAL — but can contain two during the brief promotion window: the old WAL being promoted by the background task and the new WAL already receiving writes. Since promotion is fast (write ~60KB `.idx`, two renames), this is transient. On crash recovery all files in `wal/` are treated identically: scan, truncate partial tail, promote. The active/inactive distinction does not matter for recovery.

`pending/` segments are the only local copy of their data; they must not be evicted. `segments/` are S3-backed caches; freely evictable under space pressure. No list files are needed — the filesystem is the index.

**Commit ordering:**

```
1. Build .idx in memory from the session's extent list (body_offsets from append_data)
2. Write pending/<ULID>.idx.tmp
3. Rename pending/<ULID>.idx.tmp → pending/<ULID>.idx
4. Rename wal/<ULID> → pending/<ULID>            ← COMMIT POINT
5. Update LBA map in memory
```

Step 4 is the commit point — the presence of the body file in `pending/` signals that promotion is complete. No fsync of a list file is needed; the rename is atomic. The `.idx` must exist before the body appears (step 3 before step 4) because LBA map rebuild reads `.idx` for every segment found on startup.

**S3 upload completion:**

```
6. Upload pending/<ULID> and pending/<ULID>.idx to S3
7. Rename pending/<ULID> → segments/<ULID>
8. Rename pending/<ULID>.idx → segments/<ULID>.idx
```

Steps 7 and 8 are two renames, not one atomic operation. If we crash between them, the body is in `segments/` but `.idx` is still in `pending/`. Recovery: if a body exists in `segments/` without a `.idx` alongside it, look for the `.idx` in `pending/` and move it. If an upload succeeds but we crash before the rename, the upload is retried on restart — idempotent for content-addressed data.

**On startup:** scan all three directories. Each maps to one recovery action:
- `wal/` — replay (truncate partial tail if needed) and promote
- `pending/` — load `.idx` for LBA map rebuild; queue S3 upload
- `segments/` — load `.idx` for LBA map rebuild

The read path checks `pending/` before `segments/` (or maintains an in-memory set of pending IDs populated at startup to avoid redundant stat calls).

---

## lsvd Reference Implementation Notes

The [lab47/lsvd](https://github.com/lab47/lsvd) Go implementation is the primary reference. Key design decisions we studied and how they influenced palimpsest:

### lsvd local directory layout

```
<volume-dir>/
├── head.map                          — persisted LBA→PBA map (CBOR-encoded,
│                                       SHA-256 of segment list as freshness guard)
├── writecache.<ULID>                 — active WAL (receiving writes)
├── writecache.<ULID>                 — old WAL(s) queued for promotion (up to 20)
├── segments/
│   ├── segment.<ULID>               — single-file segment:
│   │                                   [SegmentHeader 8B][varint index][data body]
│   └── segment.<ULID>.complete      — transient during Flush(); renamed to final
└── volumes/
    └── <volName>/
        └── segments                 — binary: appended 16-byte ULIDs, one per segment
```

WAL files live at the root alongside `head.map`. There is no upload-state distinction in the directory layout because S3 upload is synchronous within the background promotion goroutine — by the time a WAL is deleted, its segment is already in S3 and `volumes/<vol>/segments` has been updated. Everything in `segments/` is guaranteed to be in S3.

### Palimpsest local directory layout (comparison)

```
<volume-dir>/
├── lba.map                          — persisted LBA map (rebuilt from .idx if stale)
├── wal/
│   └── <ULID>                      — WAL file(s): active or awaiting promotion
├── pending/
│   ├── <ULID>                      — segment body committed locally, S3 upload pending
│   └── <ULID>.idx                  — segment index
└── segments/
    ├── <ULID>                      — segment body confirmed uploaded to S3 (evictable)
    └── <ULID>.idx                  — segment index
```

The `pending/` directory exists because palimpsest decouples local promotion from S3 upload. lsvd has no equivalent — it never has locally-committed segments that aren't yet in S3. The three-directory structure makes the full lifecycle visible via `ls`: `wal/` = in flight, `pending/` = local only, `segments/` = safely in S3.

| | lsvd | Palimpsest |
|---|---|---|
| WAL location | Root-level `writecache.<ULID>` | `wal/` subdir |
| Segment format | Single file (index embedded) | Separate body + `.idx` |
| Upload tracking | Not needed (S3 sync in promotion) | `pending/` vs `segments/` dirs |
| Temp files | `segment.<ULID>.complete` | `pending/<ULID>.idx.tmp` |
| LBA map | `head.map` (CBOR, SHA-256 guard) | `lba.map` (rebuilt from `.idx` files) |
| Eviction policy | Not applicable | `segments/` evictable; `pending/` never |

**Segment format:** A single file per segment: `[SegmentHeader (8 bytes)][ExtentHeaders (varint-encoded)][body data]`. No companion `.idx` file — all metadata is embedded in the body. Palimpsest uses separate body and `.idx` files for three reasons: (1) the `.idx` can be fetched alone to decide whether a segment fetch is needed at all and which byte ranges to request, avoiding transferring data that isn't needed; (2) the extent index and in-memory filter can be rebuilt from `.idx` files without reading segment bodies; (3) inline small extents and delta metadata are stored in `.idx` without inflating the body. The cost is that `.idx` loss without a corresponding S3 copy would leave a segment's LBA mappings unrecoverable — mitigated by uploading `.idx` to S3 alongside the body.

**Write log format:** Single append-only file, one record per finalised extent, each record is `[header][data]` appended sequentially. No coalescing in the log. Palimpsest's write log follows this shape exactly, adding the BLAKE3 hash and a `FLAG_DEDUP_REF` record type (absent in lsvd which is LBA-addressed, not content-addressed). Additionally, `append_data` records each extent's `body_offset` (byte position of data within the WAL file) so the WAL can be renamed directly to the segment body on promotion — zero copy.

**Pre-log coalescing:** lsvd's `nbdWrapper` buffers up to 20 contiguous blocks in a `pendingWrite` buffer before calling `WriteExtent()`. This is where adjacent LBA writes are merged. The log sees finalised extents, not raw 4KB writes. Palimpsest follows the same pattern; the count limit (lsvd: 20 blocks) is a tuning parameter.

**Fsync handling:** `writeLog()` calls `bufio.Flush()` after each extent (OS buffer, not disk). The actual `fsync` happens only in `SegmentBuilder.Sync()`, called when the guest issues a flush. Reply is sent after `logF.Sync()` completes. Palimpsest's `WriteLog::fsync()` follows this exactly.

**Async promotion — important distinction:** lsvd's `closeSegmentAsync()` sends a `CloseSegment` event to a background goroutine and the write path returns immediately. However, within that goroutine, `Flush()` calls `UploadSegment()` synchronously — the LBA map is not updated until after the S3 upload completes. If S3 is slow or unavailable, the background queue backs up and old WAL files accumulate on local NVMe (up to 20 queued, one per channel slot). Palimpsest decouples local promotion from S3 upload entirely: the WAL is committed as a local segment (segments.list fsync + WAL rename) and the LBA map is updated without waiting for S3. S3 upload is a separate background step tracked via `uploaded.list`.

**Durability equivalence:** both lsvd and palimpsest have the same fundamental durability guarantee — local NVMe is the boundary, not S3. At any moment lsvd has: one active WAL + up to 20 queued old WALs not yet in S3. Palimpsest has: one active WAL + some number of promoted-but-not-yet-uploaded local segments. In both cases, total local disk failure loses data that the guest's fsync acknowledged. Neither design guarantees S3 durability for in-flight writes. The difference is only naming: lsvd calls staged data "old WAL files"; palimpsest calls them "local segments".

**Local segment cache:** Segment files are kept in `segments/` on local disk indefinitely after upload. Reads check in-memory write cache → previous segment cache → local disk → S3 (via HTTP range requests). Palimpsest retains the same local cache tier.

**Post-hoc extent merging:** lsvd does not merge adjacent extents at write time. Adjacent-extent merging happens in `pack.go` during GC, where runs of adjacent extents are coalesced into larger records (up to 100 blocks). Palimpsest's repacking / GC pass will do the same.

**LBA map vs content-addressed:** lsvd calls its in-memory working structure `lba2pba` (`LBA → segment+offset`, physical location). GC repacking must update it for every moved extent. Palimpsest's equivalent is the **LBA map** (`LBA → hash`, logical). Physical location (`hash → segment+offset`) is tracked separately in the extent index. GC repacking updates only the extent index; the LBA map is unaffected. Frozen manifests (snapshots) are therefore immutable. This is the primary structural divergence from lsvd. The persistence and recovery story is otherwise analogous: lsvd persists `head.map` with a segment-list hash guard; palimpsest persists `lba.map` with the same guard, rebuilding from `.idx` files if stale.

**Compression:** lsvd uses LZ4 with an entropy threshold of 7.0 bits/byte and a minimum compression ratio of 1.5×. Palimpsest uses zstd (already a dependency) with the same 7.0-bit entropy threshold as a starting point.

---

## Implementation Notes

**S3 is intentionally deferred.** The system can be developed and validated end-to-end using local storage only — `pending/` and `segments/` act as local-only segment stores without any upload step. This covers the full write path, promotion pipeline, LBA map, crash recovery, and read path. S3 hookup comes later.

A clean progression for introducing S3:
1. **Local only** — `pending/` and `segments/` as local stores, no upload step
2. **Local S3-compatible service** (MinIO, LocalStack) — write the S3 client code against a local service; real upload/download paths exercised without cloud dependency
3. **Real S3** — swap in credentials, no code changes needed

Constraints to keep in mind so S3 integration stays straightforward:
- Segment IDs (ULIDs) are already globally unique and suitable as S3 object keys
- The `pending/` → `segments/` transition maps cleanly to "upload to S3, then rename locally"
- The `.idx` format is already valid as a standalone S3 object
- Persistent structures (manifests, `.idx` files) reference segment IDs only — local paths are derived at runtime, never stored

---

## Open Questions

- **Hash output size:** BLAKE3 at full 256-bit is the current choice — collision probability is negligible (~2^-128 birthday bound) at any realistic extent count, and speed is equivalent to non-cryptographic hashes on AVX2/NEON hardware. A truncated 128-bit output would halve the per-entry cost in the extent index (~1.6GB saved at 80M entries) while keeping collision probability effectively zero at practical scales (birthday bound ~2^64). Worth revisiting once the index size and memory pressure are measured empirically.
- **Inline extent threshold:** extents below this size are stored inline in `.idx` files rather than referenced by segment offset. Needs empirical validation against the actual extent size distribution in target images.
- **Entropy threshold:** 7.0 bits used in experiments, taken from the lab47/lsvd reference implementation. Optimal value depends on workload mix.
- **Segment size:** ~32MB soft threshold, taken from the lab47/lsvd reference implementation (`FlushThreshHold = 32MB`). Not a hard maximum — a segment closes when it exceeds the threshold. Optimal value depends on S3 request cost vs read amplification tradeoff.
- **Extent index implementation:** sled, rocksdb, or custom. Needs random reads and range scans.
- **Shared extent index:** per-host or shared service? DynamoDB, S3-backed, or dedicated process?
- **Pre-log coalescing block limit:** lsvd uses 20 blocks. The right value for palimpsest depends on typical write burst sizes and acceptable memory footprint between fsyncs.
- **Manifest cache invalidation:** the reference implementation (lab47/lsvd) validates the cached manifest against a hash of current segment IDs. Same approach applies here.
- **Namespace granularity:** binary global/non-global may not be sufficient for multi-tenancy.
- **Boot hint persistence:** where are hint sets stored, how are they distributed across hosts?
- **Empirical validation of repacking benefit:** measure segment fetch count before and after access-pattern-driven repacking.
- **ublk integration:** Linux-only, io_uring-based. NBD kept for development and macOS.
