# Overview

## Problem Statement

Running many VMs at scale on shared infrastructure presents a storage challenge: base images are largely identical across instances, evolve incrementally over time, and yet are conventionally treated as independent copies. Each VM gets its own full copy of the image, even though 90%+ of the data is shared and most of it is never read at runtime.

The goal is a block storage system that minimises storage cost, minimises cold-start latency, and handles image updates efficiently at scale. The approach combines four techniques that individually exist but have not previously been integrated in this way:

- **Log-structured virtual disk (LSVD)** — write ordering and local durability, with S3 as the large capacity tier
- **Demand-fetch** — only retrieve data from S3 when it is actually needed; data never accessed is never transferred
- **Content-addressed dedup** — identical chunks written to the same volume tree are stored once; dedup is local and opportunistic
- **Delta compression** — near-identical chunks (e.g. a file updated by a security patch) are stored as small deltas in S3, reducing fetch size for updated images

The combination is particularly effective for the VM-at-scale use case because: base images are highly repetitive across snapshots of the same volume (dedup captures shared content), images evolve incrementally (delta compression captures changed content), most of each image is never read at runtime (demand-fetch avoids fetching unused data), and the same image is booted many times (locality optimisation pays back repeatedly).

## Key Concepts

**Block** — the fundamental unit of a block device, 4KB. This is what the VM sees.

**Extent** — a contiguous run of blocks at adjacent LBA addresses, and the fundamental unit of dedup and storage. Extents are variable-size and identified by the BLAKE3 hash of their content. BLAKE3 is chosen because: it is as fast as non-cryptographic hashes on modern hardware (SIMD-parallel tree construction), its 256-bit output makes accidental collisions negligible (birthday bound ~2^128 operations), and it has first-class Rust support. Collisions cannot be prevented by any hash function, but with 256-bit output the probability is effectively zero for any realistic chunk count.

Note: "extent" is also an ext4 term — an ext4 extent is a mapping from a range of logical file blocks to physical disk blocks, recorded in the inode extent tree. These are distinct concepts. Where the distinction matters, "ext4 extent" or "filesystem extent" refers to the filesystem structure; "extent" alone refers to the LSVD storage unit.

**Live write extents** are bounded by fsync and contiguous LBA writes. A write to LBA 100–115 and an adjacent write to LBA 116–131 arriving before the next fsync are coalesced into one extent covering LBA 100–131. Writes to non-contiguous LBAs stay as separate extents regardless of fsync timing — coalescing only applies to adjacent LBA ranges. Live write extents are **opportunistic dedup candidates**: full-file writes (e.g. `apt install`) may happen to align with file boundaries and dedup well; partial file edits will not.

**Manifest** — a serialised point-in-time freeze of the LBA map. The live LBA map is the authoritative source; the manifest is an optional cache of it, useful for fast startup. **The manifest is always derivable from the segments** — each segment's index section carries the LBA metadata for its extents, so the LBA map can be reconstructed by scanning the volume tree's segment files. S3 persistence of the manifest is an optimisation (to avoid segment scans at startup), not a correctness requirement.

**Snapshot** — a frozen volume node. Taking a snapshot creates a new live child node under the current node; the current node becomes frozen (read-only). Snapshots form a tree: the directory structure is the source of truth for the parent chain. No separate manifest is required to traverse the tree. A snapshot can be used as a rollback point or as the base for multiple independent forks.

**Segment** — a packed collection of extents, typically ~32MB, stored as a single S3 object with four sections: header, index, inline data, and extent body. The index section is small (~60–200KB) and can be fetched independently to make retrieval decisions without transferring the full body. Segments are the unit of S3 I/O. See [formats.md](formats.md) for the full segment file format.

**Write log** — the local durability boundary. Writes land here first (fsync = durable). Extents are promoted to segments in the background. See [formats.md](formats.md) for the WAL format.

**Extent index** — maps `extent_hash → (segment_ULID, body_offset)`. Tells the read path where a given extent lives. The ULID is globally unique; its path on disk is derived at runtime by scanning the common root. Built at startup by scanning the volume's own tree plus, opportunistically, all other volumes' segments under the common root — enabling best-effort cross-volume dedup with no coordinator involvement.

**LBA map** — the live in-memory structure mapping logical block addresses to extent hashes. Updated on every write; the authoritative source for read path lookups. See [architecture.md](architecture.md) for persistence and layer-merging details.

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

## Empirical Findings

See [FINDINGS.md](../FINDINGS.md) for full measurements. Key results:

- **93.9% of a 2.1GB Ubuntu image is never read during a full boot** — validates the demand-fetch model
- **84% of extents match exactly between 22.04 point releases** (by count); 35% by bytes — the large changed files are the delta compression target
- **~94% marginal S3 fetch saving** for a point-release update (exact dedup + delta compression combined)
- **~1.5MB manifest** for a 762MB filesystem (~33,700 extents at 44 bytes each)
