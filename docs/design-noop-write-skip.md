# Design: skip no-op writes

**Status:** Proposed.

## Problem

`Volume::write()` (`elide-core/src/volume.rs:505`) treats every inbound block uniformly: hash, dedup-check, append WAL record, update LBA map. For a block whose content already lives at that LBA, the path still writes a ~50 byte DedupRef record and pays the durability barrier. ext4 journal replay after an unclean mount, metadata rewrites landing on identical values, page-cache double-flushes, and zero-over-zero writes during `mkfs`/`fstrim` all generate these no-op writes.

## Proposal

Before hashing, try a locality-gated byte-compare of the existing content. Skip the write if the bytes match.

```rust
if let Some(existing) = self.try_read_local(lba, lba_length)?
    && existing == data
{
    self.noop_skipped_writes += 1;
    self.noop_skipped_bytes += data.len() as u64;
    return Ok(());
}

let hash = blake3::hash(data);
// ... existing path
```

`try_read_local` walks `lbamap.extents_in_range(lba, lba+lba_length)` and returns `Some(bytes)` only if every overlapping extent resolves through `extent_index` to a local body (`body_source == Local`, or lives in the live WAL / `pending_entries`). If any extent is remote or unwritten, it returns `None` and the caller falls through to the normal path — we never trigger a demand fetch.

This subsumes three cases in one check: whole-extent no-op, per-block no-op inside a larger local extent, and fragmented-but-still-matching ranges. No format change, no LBA map bloat.

## Ordering: check before hash

The skip check runs *before* `blake3::hash(data)`. On a hit, we avoid the incoming-block hash entirely.

Rough costs per 4 KiB, modern core:

| step                 | cost     |
| -------------------- | -------- |
| blake3 incoming      | ~1-4 µs  |
| read warm local      | ~0.2 µs  |
| memcmp 4 KiB         | ~0.2 µs  |

- **Skip hit (warm):** ~0.4 µs. No hash, no WAL, no fsync contribution.
- **Skip miss (warm):** ~0.4 µs wasted, then the normal hashed path.
- **Skip miss (cold local):** a real disk read wasted. Mitigation below.

Byte-compare is roughly an order of magnitude cheaper per byte than blake3, so the comparison itself is in the noise. The costs that matter are the existing-body read and the avoided hash.

Hash-compare (hash existing, compare digests) is strictly worse: hashing existing costs the same as memcmping it, and the 32-vs-4096-byte compare is lost in the noise.

## Correctness

**No body fetch.** `try_read_local` returns `None` for any range that would require fetching from S3 or an ancestor we don't have locally. A miss falls through silently; the skip is purely an optimisation over existing state.

**Fork layering.** `open_read_state()` (`volume.rs:2783`) builds a flat `LbaMap` via `lbamap::rebuild_segments()` (`lbamap.rs:319`) that walks the ancestor chain oldest-first. Parent entries are flattened into `self.lbamap` at open time; lookup does no chain walking. A child fork's LBA map already surfaces inherited parent mappings, so a no-op write against an inherited LBA hits the skip path iff the parent's body is local.

**Snapshots.** A skipped write produces no local segment entry. The snapshot's view of that LBA is inherited from the parent — which is correct, since the content is unchanged.

**Durability.** The NBD layer still honours FUA/FLUSH by calling `volume.fsync()`. A skip only means *this* call adds no new WAL bytes; previously-appended WAL data still needs durable commit on flush. The skip does not change the flush contract.

**Collision concerns.** None — this is a direct byte-compare, no hash-based equality claim.

## Cold-data mitigation

For cold local data the wasted read on a skip miss is non-trivial. Two mitigations if measurement shows it matters:

1. Only attempt the skip when every overlapping extent's body lives in the live WAL, `pending_entries`, or the most recent local segment — i.e., data the page cache is likely to hold warm.
2. Gate the skip on a config flag defaulted on for `volume import` (where everything is warm by construction) and off elsewhere until we have numbers.

Neither is needed for an initial landing; they're levers if the counters reveal a problem.

## Counters

Three counters on `Volume`, exposed via the existing stats surface and printed at the end of `elide volume import`:

- `noop_skipped_writes` — number of calls that short-circuited
- `noop_skipped_bytes` — bytes the skip avoided writing to the WAL
- `noop_check_reads_bytes` — bytes read to perform the compare (hit and miss combined)

The ratio `noop_skipped_bytes / noop_check_reads_bytes` is the effective win: the bytes we avoided writing per byte of read-side overhead. `volume import` is the primary measurement rig because it runs offline with everything local and warm.

## Comparison: lsvd

lab47/lsvd has no write-time content dedup at all. `disk.go:681` (`WriteExtent`) buffers every incoming write unconditionally; `segment.go:538` computes entropy and compression stats but no content hash. Duplicate LBA writes overwrite previous LBA map entries and stale extents are reclaimed later by GC.

Elide already diverges by doing write-time content dedup (the REF-record path). This proposal adds a cheaper, earlier check: before paying for the hash, look at what's already there.

## Non-goals

- Any skip that would trigger a demand fetch.
- Any change to `write_zeroes` / `trim`, which already bypass hashing.
- Any change to the REF-record path for cross-LBA dedup.
- Per-block hashes stored in the LBA map or segment entries (considered and rejected as too expensive for the expected hit rate).
