# Finding: cargo-build workload reproduces stale-read corruption (write loss)

## Symptom

Real-world ext4 corruption when a heavy write workload (cargo build) runs
on a mounted volume while the coordinator is cycled stop/start. After
unmount + fsck:

```text
Inode 525745 seems to contain garbage. Clear? no
Inode 525745 passes checks, but checksum does not match inode. Fix? no
...
Multiply-claimed block(s) in inode 525745: 2130612
Multiply-claimed block(s) in inode 525752: 2143558--2143694
...
Error while scanning inodes (525761): Inode checksum does not match inode
/dev/ublkb0: ********** WARNING: Filesystem still has errors **********
```

100+ inodes corrupt across consecutive blocks of a single inode-table
region. Multiply-claimed-block pattern between inodes shows that some
inodes still hold *old* extent metadata referencing blocks that have
since been reallocated to other inodes — i.e. the inode-table block
returned by the volume is from before a kernel write that the kernel
believes succeeded.

Reproduces in two independent runs (yesterday's spontaneous failure and
today's deliberate repro), with identical fsck shape on different inode
ranges. The workload was the same in both cases:

1. mkfs.ext4 the volume, mount it
2. clone the elide repo into the mount
3. `cargo build` with the coordinator cycling stop / start one or more
   times during the build
4. unmount, fsck

## Reproduction (today)

| Time     | Event |
|----------|-------|
| 13:55:06 | new WAL `01KR6GAEDV40166QDCERTY9RR0` |
| 13:55:08 | flush `01KR6GAG236SEED0V6WSNNBK3Y` — includes a `zero [2097152+524288)` covering 2 GiB (full block group, kernel TRIM) |
| 13:55:13 | GC compact 17 inputs → `01KR6GAN6M5SQ5DHNM62YVS2GM` (`drop=8`) |
| 13:55:48 | GC compact 5 inputs → `01KR6GBPVV4FA05GHMBDSEHK29` (`drop=1`) |
| 13:56:19 | coord SIGINT |
| 13:57:34 | coord SIGINT (second cycle while pending was draining) |
| 13:58:18 | drain repack: **13 segments → 9 outputs** (`01KR6GG80R*` series) |
| 14:00:51 | GC compact 5 inputs → `01KR6GMZ9M01AAYCF9VFJNDVWQ` |
| 14:01:29 | GC compact 2 inputs → `01KR6GP49KMGJTQ4J1GXJ7QVFZ` |

After build completion the volume was unmounted and fsck reported the
errors above. Volume daemon stopped + restarted; the corruption is
preserved on disk (deterministic stale reads).

## Evidence chain

1. **Read path is deterministic.** `dd if=/dev/ublkb0 bs=4096 skip=2097275 count=1`
   returns the same hash on repeat reads (within and across daemon
   restarts).
2. **Read path is correct against on-disk metadata.** Reading 3 blocks
   `[2097275..2097278)` returns bytes that hash to
   `d2f762890ef2cb209eb2143ef310b13bd4bc55127aaa35e78db2dfbf591cbbcd`
   — exactly the hash recorded in segment
   `01KR6GG80RN227C0V58TS3W9J3` for entry `data [2097275+3)`.
3. **Latest claim per all current segments.** Scanning every `index/*.idx`
   for entries covering LBA 2097275, the highest-ULID covering segment is
   `01KR6GG80RN227C0V58TS3W9J3` (one of the 9 bin-packed repack outputs
   from 13:58:18). No `pending/`, `wal/`, `gc/` segment carries a later
   write to that LBA. Stop / restart preserves this state.
4. **Kernel disagrees with the bytes.** Inode 525745 (which lives at
   block 2097275, offset 0x000) is reported as bad-checksum AND
   multiply-claimed with another inode. The "multiply-claimed" pattern
   only happens when one of the inodes holds *stale* extent metadata —
   the kernel deallocated the block from inode A, allocated it to inode
   B, and updated inode A's extents to release it; the volume returned
   the pre-update bytes, so inode A still claims the block.

Conclusion: a write that the kernel issued to LBA 2097275 (and to many
neighbouring inode-table LBAs in the same region) was lost between the
kernel-issued write and the on-disk segment. The volume's read path,
extent index, and lbamap are internally consistent — they just point at
a body whose contents pre-date the kernel's last write at that LBA.

## Where the loss lives — primary suspect

The 9 outputs of the 13:58:18 bin-packed repack are
`01KR6GG80R3MT8…`, `01KR6GG80RA2NV…`, `01KR6GG80RKD1…ET`,
`01KR6GG80RKD1…EV`, `01KR6GG80RN227…J2`, `01KR6GG80RN227…J3`,
`01KR6GG80RN227…J4`, `01KR6GG80RX245…37`, `01KR6GG80RX245…38` —
9 ULIDs minted from one `prepare_repack(segs.len()+1=14)` call,
producing 9 buckets from 13 inputs.

This is the unified-repack apply path introduced by PR #297, running
under direct-write traffic from PR #302. The interaction is:

1. `prepare_repack` (actor thread, under volume mutex)
   - mints N+1 output ULIDs and `u_flush`
   - `flush_wal_to_pending_as(u_flush)` drains the open WAL into a
     pending segment
   - returns `RepackJob` with snapshots of `lbamap` / `extent_index` /
     `ancestor_layers` / `fetcher`
2. Actor releases the lock, dispatches the job to the worker thread
3. **Direct writes from ublk threads continue to land** under the volume
   mutex — they update the live `lbamap` and `extent_index` and append
   to a fresh WAL (since `prepare_repack` took the previous one)
4. Worker reads the snapshot, classifies every non-floor pending
   segment, bin-packs candidates into N output buckets, materialises
   each bucket's plan into one rewrite output
5. `apply_repack_result` (actor thread, under lock again)
   - per-bucket: CAS-removes owned hashes against per-input ULIDs; CAS-
     inserts carried entries gated on `current.segment_id ∈
     bucket.input_ulids`; `lbamap.insert_consuming_inputs(...,
     output_ulid, &bucket_input_ulids)`; deletes input files

The lost write must originate during step 3 — a kernel write at an LBA
that step 4's classifier looked at *before* the write landed. The
classifier saw the snapshot's "live" entry and emitted Keep into the
output bucket; by the time apply runs, the live `lbamap` reflects the
new write but `insert_consuming_inputs` correctly refuses to clobber
it. That part is fine.

The candidate failure is in the *body* the worker materialised. The
bucket output's body bytes are the snapshot's live bytes — which the
read path then serves, instead of the new write's bytes. If the LBA's
new claim was later **lost** to a subsequent structural commit's apply
(another bin-packed repack, GC apply, or promote), the lbamap would
roll back to the bucket's stale entry, and reads would return the
pre-write bytes — exactly what we observe.

## Where to look in the code

- `elide-core/src/actor.rs:2125` `execute_repack` — classification and
  bin-packing under the prep-time snapshot
- `elide-core/src/actor.rs:2298` phase-3 materialisation per bucket
- `elide-core/src/volume/repack.rs:243` `apply_repack_result` — per-input
  CAS-remove, per-bucket CAS-insert, `insert_consuming_inputs`
- `elide-core/src/volume/mod.rs:2017` `flush_wal_to_pending_as` —
  bumps `lbamap` claimants from WAL ULID to flush ULID under the lock
- `elide-core/src/lbamap.rs:225` `insert_consuming_inputs` —
  blocks-vs-not-blocks logic against `consumed_inputs`
- `elide-core/src/volume/mod.rs:2587` `apply_promote` and `mod.rs:2528`
  `prepare_promote` — same prep / lock-drop / apply pattern; same
  classes of races possible

The duplicate-Zero-entries pattern observed in the on-disk segments
(e.g. `01KR608V2QM0CXNGYGJ2F2MDSQ` has `zero [2097184+512)` listed
twice) is a separate, cosmetic bug — the bin-packed apply emits two
Keep records when two inputs in the same bucket both have a Zero entry
covering the same LBA range. The lbamap dedups on insert, so this
inflates segment file size but does not directly corrupt data.

## Why proptest didn't catch it

Direct-write through the volume mutex (PR #302) is exercised only by
`VolumeClient::write` from real transports. The proptest suites
(`actor_proptest`, `volume_proptest`) drive writes through the actor
request channel, which serialises against structural commits and never
exposes the prep / lock-drop / worker / apply window to a concurrent
write at the same LBA.

The `volume-invariants` `assert_lbamap_consistent` check (738f0bd
relaxed to flag only `in_memory < disk`) does not trip here because the
final on-disk state is internally consistent — the lbamap entry, the
extent-index entry, the segment file, and the body bytes all agree.
The bug is not a divergence between memory and disk; it is a *write
that never made it into either*.

## Reproducer assets

- Volume directory snapshot: `elide_data/by_name/myvolume/` on the
  Lima `elide-dev` VM (preserved on disk; fsck reproduces the symptom)
- Suspect bucket output: segment
  `01KR6GG80RN227C0V58TS3W9J3` (in `index/`, body in `cache/`)
- Drain log: `elide_data/elide.log` from 13:55 — 14:03 captures the
  full sequence of structural commits

## Proposed next steps

1. **Construct a deterministic proptest case** that exercises the
   direct-write window: hold a `RepackJob` in-flight on the worker, do
   a direct write at one of the snapshot's LBAs, verify the apply
   commits the new write rather than the snapshot's body. The
   workload-side wrapper should be exposed as a test seam.
2. **Audit the body-byte path inside `execute_repack`**: confirm that
   when the classifier picks an entry into a bucket from a snapshot
   that is racing with a direct write, the materialised body is
   eventually superseded by the direct write and not surfaced through
   any read path even after subsequent structural commits.
3. **Audit subsequent structural-commit apply paths** (GC apply, the
   next repack pass, promote) for any case where an LBA whose latest
   claim is a fresh WAL ULID could be rolled back to a bin-packed
   repack output's body.
4. **Restore the disabled regression test**
   `gc_preserves_data_entry_when_lba_live_but_not_extent_canonical`
   (#[ignore]'d in PR #297 with a note that it covers a property the
   proptest suite covers — but the explicit regression is what
   would have failed loudest here).

## Cross-references

- `docs/design-pending-compaction-unification.md` — PR #297 design
- `docs/finding-sweep-flush-claimant-bug.md` — the predecessor
  consumed-inputs override that fixed an earlier symptom in this same
  area
- PR #297 (de9e6eb): unify sweep into repack as a single bin-packing pass
- PR #302 (50511cd): write directly through volume mutex from ublk threads
- PR #304 (89093bc): hoist blake3 + lz4 off the volume mutex
