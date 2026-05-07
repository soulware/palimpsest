---
status: landed
related: [design-extent-reclamation.md]
---

# Skip no-op writes

`Volume::write()` already hashes every incoming block (for cross-LBA dedup, the WAL header, and the LBA-map insert). Before falling through to the normal path, check the LBA map for an exact match (LBA, length, hash, `payload_block_offset == 0`); if it matches, skip — no WAL append, no segment entry. **Zero file I/O.**

Correctness rests on BLAKE3 collision resistance — hash equality implies byte equality. The check fires for every body source (local, cached present/absent, S3-only) without touching the body file or the network.

## Why no byte-compare tier

An earlier two-tier design walked the LBA map for fragmented matches and byte-compared bodies on hash miss. We removed it: a measurement on `dd if=/dev/urandom` over fresh ext4-on-NBD showed tier 2 read **~690 MiB to save ~50 MiB**, almost all of which the hash check already caught (ext4 metadata blocks). The marginal contribution on the random-data payload was ~zero.

Tier 2 also ran synchronously on the actor thread — extending write latency — and forced an API split: a parallel `write_internal` to bypass tier 2 for extent-reclamation rewrites where incoming bytes equal existing bytes by construction. Removing tier 2 collapses that split.

If a future workload makes fragmented matches worth catching, reintroduce with a shape gate (only fire on multi-extent overlap) and streaming early-exit compare. Want measured demand before paying the complexity again.

## Coverage

| scenario | skipped? |
| --- | --- |
| Whole-extent match, any body source | yes |
| Different content at the same LBA range | no |
| Unwritten range | no |
| Fragmented match (incoming spans many extents) | no |
| All-zeros write over a `ZERO_HASH` extent | no — `blake3::hash([0; N])` is not the sentinel; userspace zero-fills usually arrive as TRIM/WRITE_ZEROES anyway |

## Notes

- **Fork layering:** parent entries are flattened into `self.lbamap` at open time, so a no-op write against an inherited LBA matches unconditionally.
- **Reclamation rewrites** (`design-extent-reclamation.md`) call `write_with_hash` with a precomputed hash; the same skip applies, which is correct — re-running reclamation over an already-merged range is a no-op by definition.
- **Durability contract is unchanged.** `NBD_CMD_FLUSH` routes through `VolumeClient::flush → Volume::flush_wal`, an entirely separate path from `Volume::write`. A skip means *this* write call adds no new WAL bytes; previously-appended WAL data still becomes durable on the next flush. The skip does not change the flush contract.
- **Snapshots:** a skipped write produces no local segment entry. The snapshot's view of the LBA is inherited from whatever entry already covered it — exactly right, since the content is unchanged.
- Counters (`skipped_writes`, `skipped_bytes`) on `Volume`, exposed via `VolumeClient::noop_stats()`.
