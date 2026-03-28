# Formats

On-disk and on-wire formats for the write log (WAL) and segment files.

## Write Log

The write log is the local durability boundary. Writes land here on fsync; the log is promoted to a segment in the background.

### File format

A single append-only file per in-progress segment, living at `wal/<ULID>`. One file, records appended sequentially, no separate index.

**Magic header:** `ELIDWAL\x01` (8 bytes)

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

The hash is computed before the dedup check and stored in the log record. Recovery can reconstruct the LBA map without re-reading or re-hashing the data.

### Pre-log coalescing

Contiguous LBA writes are merged in memory before they reach the write log — in the NBD/ublk handler, not in the log itself. This mirrors lsvd's `pendingWrite` buffer. The coalescing window is bounded by both a block count limit (to prevent unbounded memory accumulation between fsyncs) and the fsync boundary (a guest fsync flushes any pending buffer). The write log only ever sees finalised, already-coalesced extents.

### Durability model

```
write arrives → in-memory coalescing buffer
                        │
               count limit or fsync
                        │
                        ▼
               hash → local dedup check → append_data / append_ref → bufio (OS buffer)
                        │
                    guest fsync
                        │
                        ▼
               logF.sync_data() ← write log durable on local disk; reply sent to guest
                        │
                [background, async]
                        │
                        ▼
               segment close → clean segment file written → S3 upload
```

After a guest fsync returns, all prior writes are durable in the write log on local NVMe. S3 upload is asynchronous and not on the fsync critical path.

### Crash recovery

On startup, if a write log file exists, `scan()` reads it sequentially. If a partial record is found at the tail (power loss mid-write), the file is truncated to the last complete record. All complete records are replayed to reconstruct the in-memory LBA map. The write log is then reopened for continued appending.

**Failure scenarios:**

| Scenario | State on restart | Recovery |
|---|---|---|
| Crash mid-write (before fsync) | WAL tail partial | Truncate WAL to last complete record; replay |
| Crash after fsync, before promotion starts | `wal/<ULID>` intact; nothing in `pending/` | Replay WAL; promote normally |
| Crash during segment file write (steps 1–2) | `pending/<ULID>.tmp` may exist; WAL intact | Delete `.tmp`; replay WAL |
| Crash after rename, before WAL delete (steps 3–4) | Both `pending/<ULID>` and `wal/<ULID>` exist | Delete WAL; use pending segment |
| Crash after WAL delete, before LBA map update (steps 4–5) | `pending/<ULID>` present; no WAL | Rebuild LBA map from pending segment header + index |
| Crash mid-upload or after upload before rename (steps 6–8) | Segment still in `pending/`; may be in S3 already | Retry upload (idempotent); rename on success |
| Total local disk loss | All local state gone | Data loss bounded to writes not yet in S3 — same guarantee as a local SSD |

The final row is an intentional design choice: local NVMe is the durability boundary, matching the stated goal of "durability semantics similar to a local SSD". S3 is async offload, not the primary durability mechanism.

### Promotion to segment

When the write log reaches the 32MB threshold (or on an explicit flush), the background promotion task converts the WAL into a committed local segment. The WAL is assigned a ULID at creation time; that same ULID becomes the segment ID.

**Promotion writes a clean segment file.** The WAL format includes per-record headers that are useful for recovery but should not be part of the permanent segment format. Promotion reads the WAL sequentially and writes only the raw extent data bytes (no headers) to a clean body section. REF records contribute no bytes to the body — their index entries carry only the LBA mapping and `FLAG_DEDUP_REF`. All segments — freshly promoted or GC-repacked — have the same uniform format.

**Directory layout within a live node:**

```
wal/<ULID>          — WAL file (active or awaiting promotion)
pending/<ULID>      — segment file committed locally, S3 upload pending
segments/<ULID>     — segment file confirmed uploaded to S3 (evictable)
```

Each directory corresponds to one stage in the lifecycle:

```
wal/<ULID>  →  pending/<ULID>  →  segments/<ULID>
```

Both `pending/` and `segments/` hold segment files in the same format (header + index + inline + body). The distinction is upload state, not file format. Locally-stored segment files have `delta_length = 0` in the header; the coordinator appends the delta body when computing deltas at S3 upload time, producing the final S3 object.

`wal/` normally contains one entry — the active WAL — but can contain two during the brief promotion window. On crash recovery all files in `wal/` are treated identically: scan, truncate partial tail, promote.

`pending/` segments are the only local copy of their data; they must not be evicted. `segments/` are S3-backed caches; freely evictable under space pressure. No list files are needed — the filesystem is the index.

**Commit ordering:**

```
1. Build index section in memory from WAL extent list
2. Write pending/<ULID>.tmp: header + index + inline + body (DATA extents only, no headers)
3. Rename pending/<ULID>.tmp → pending/<ULID>            ← COMMIT POINT
4. Delete wal/<ULID>
5. Update LBA map in memory
```

Step 3 is the commit point — a complete segment file at `pending/<ULID>` means promotion is done. The entire file is written atomically via rename; there is no window where a partial file is visible as the committed name.

**S3 upload completion:**

```
6. Read pending/<ULID>; choose S3 reduction strategy (if applicable):
   a. Delta compression: compute delta body against ancestor segments;
      S3 object = local file + appended delta body (header and index updated with delta offsets)
   b. Sparse: compare extents block-by-block against ancestor; build a fresh S3 object
      containing only changed-block extents; S3 manifest reflects the sparse LBA map
   c. Neither: upload local file as-is
7. Upload S3 object
8. Rename pending/<ULID> → segments/<ULID>
```

Under **delta compression** the S3 object is derived from the local file (body section identical, delta body appended, header/index updated). The local file can be streamed directly.

Under the **sparse** strategy the S3 object diverges structurally from the local file: its index section contains entries for changed blocks only, not the original full-extent entries. The coordinator builds the S3 object fresh. H_new (the full extent hash) is not registered in the S3 extent index — only the changed block hashes are. The local segment retains H_new as a full DATA record; local reads are unaffected.

The two strategies are not mutually exclusive: sparse can be applied first (skip unchanged blocks), and delta compression applied to the changed blocks that remain. See [architecture.md](architecture.md) for the trade-off comparison.

**On startup:** scan all three directories within the live node. Each maps to one recovery action:
- `wal/` — replay (truncate partial tail if needed) and promote
- `pending/` — read header + index section for LBA map rebuild; queue S3 upload
- `segments/` — read header + index section for LBA map rebuild

Then scan ancestor nodes' `segments/` directories (no `wal/` or `pending/` — they are frozen), oldest ancestor first, to build the full merged LBA map.

---

## Segment File Format

Each segment is a **single file** both locally and in S3. The same format is used throughout — the local `pending/<ULID>` file is the S3 object minus the delta body, which the coordinator appends at upload time.

### File layout

```
[Header: 96 bytes]
  magic          (8 bytes)  — "ELIDSEG\x02"
  entry_count    (4 bytes)  — number of index entries (u32 le)
  index_length   (4 bytes)  — byte length of index section (u32 le)
  inline_length  (4 bytes)  — byte length of inline section (u32 le); 0 if none
  body_length    (8 bytes)  — byte length of full extent body (u64 le)
  delta_length   (4 bytes)  — byte length of delta body (u32 le); 0 if no deltas
  signature      (64 bytes) — Ed25519 signature (see Fork ownership and signing below)

[Index section]             — starts at byte 96; length = index_length
[Inline section]            — starts at byte 96 + index_length; length = inline_length
[Full body]                 — starts at byte 96 + index_length + inline_length; length = body_length
[Delta body]                — starts at byte 96 + index_length + inline_length + body_length; length = delta_length
```

Derived section offsets (computable from the header alone):
```
index_offset  = 96
inline_offset = 96 + index_length
body_offset   = 96 + index_length + inline_length
delta_offset  = 96 + index_length + inline_length + body_length
```

**The full body** is raw concatenated extent data — DATA-record extents only, clean bytes, no framing. REF-record extents contribute nothing to the body. All navigation is via the index section.

**The delta body** is raw concatenated delta blobs, referenced by byte offset from the index section. Absent on locally-stored segment files (`delta_length = 0`); present on S3 objects when the coordinator has computed deltas against ancestor segments.

**The inline section** holds raw bytes for inlined extents and inlined delta blobs. It is placed before the full body so a single `GET [0, body_offset)` retrieves the header, index, and all inline data together — sufficient for a warm-start client to serve all small extents without fetching the body at all.

### Index section entry format

**Flag bits** (1 byte per entry):
- `0x01` `FLAG_INLINE` — extent data is in the inline section; no body fetch needed
- `0x02` `FLAG_HAS_DELTAS` — one or more delta options follow
- `0x04` `FLAG_COMPRESSED` — stored data is zstd-compressed; lengths are compressed sizes
- `0x08` `FLAG_DEDUP_REF` — extent data lives in an ancestor segment; no body in this segment

```
For each extent:
  hash          (32 bytes)  — BLAKE3 extent hash
  start_lba     (8 bytes)   — first logical block address (u64 le)
  lba_length    (4 bytes)   — extent length in 4KB blocks (u32 le)
  flags         (1 byte)    — flag bits above

  if FLAG_DEDUP_REF:
    (no body fields — data located via extent index lookup on hash)

  if !FLAG_DEDUP_REF and !FLAG_INLINE:
    body_offset (8 bytes)   — byte offset within full body section (u64 le)
    body_length (4 bytes)   — byte length (compressed size if FLAG_COMPRESSED)

  if FLAG_INLINE:
    inline_offset (8 bytes) — byte offset within inline section (u64 le)
    inline_length (4 bytes) — byte length of inline data

  if FLAG_HAS_DELTAS:
    delta_count  (1 byte)   — number of delta options (≥1)
    per delta option:
      source_hash        (32 bytes) — BLAKE3 hash of the source extent
      option_flags       (1 byte)   — bit 0: FLAG_DELTA_INLINE
      if !FLAG_DELTA_INLINE:
        delta_offset     (8 bytes)  — byte offset within delta body section (u64 le)
        delta_length     (4 bytes)  — byte length in delta body (u32 le)
      if FLAG_DELTA_INLINE:
        delta_inline_offset (8 bytes) — byte offset within inline section (u64 le)
        delta_inline_length (4 bytes) — byte length of inline delta
```

`lba_length × 4096` always gives the uncompressed extent size. `body_length` / `inline_length` gives the stored (possibly compressed) size.

**FLAG_DEDUP_REF entries** carry only the LBA mapping, sufficient for LBA map reconstruction at startup. The extent data is located via the extent index (`hash → ULID + body_offset`), populated from ancestor segment files at startup.

**FLAG_INLINE extents** store their full data in the inline section. Particularly effective for the boot path: small config files, scripts, and locale data appear frequently during boot and are naturally small. A warm-start client that fetches `[0, body_offset)` gets all inline extents with no further requests.

**Multiple delta options** allow an extent to have deltas against several source extents (e.g. against the immediately prior snapshot and an earlier one). The client picks the first option whose `source_hash` is in its local extent index. If no source is available, the full extent is fetched from the body instead. This provides graceful degradation across skipped releases.

**FLAG_DELTA_INLINE** applies the same logic to delta blobs: a small delta is stored in the inline section, avoiding a byte-range fetch into the delta body.

**Index entries serve two purposes with a single scan:** LBA map reconstruction (`start_lba + lba_length + hash`) and extent index population (`hash → ULID + body_offset + body_length`). No separate pass needed.

### Typical segment file sizes (~1000 extents, ~32MB body)

| Configuration | Index section | Notes |
|---|---|---|
| No deltas | ~57KB | Base case |
| 3 delta options, 16% of extents | ~70KB | Realistic point-release update |
| 3 delta options, all extents | ~193KB | Worst case |

Inline section size depends on the inline threshold and extent size distribution — typically small if the threshold is kept tight (e.g. ≤ a few KB per extent).

### S3 object key

```
<volume_id>/<fork_name>/YYYYMMDD/<ulid>
```

Keys are namespaced by volume and fork, with a date prefix derived from the ULID's embedded millisecond timestamp. The date is decoded and formatted as `YYYYMMDD` rather than taken from the ULID character prefix directly — the character prefix gives either ~3-day or ~2-hour buckets depending on how many characters are used, neither of which aligns with a calendar day. The date reflects segment creation time, not upload time, so keys are stable regardless of when `drain-pending` runs.

Example key for a segment promoted in the `vm1` fork of volume `ubuntu-22.04`:
```
ubuntu-22.04/vm1/20260328/01KMTENX98EN5A20523Y2JC8N5
```

Benefits of this scheme:
- `list <volume_id>/<fork_name>/` returns all segments for a fork regardless of age
- `list <volume_id>/<fork_name>/YYYYMMDD/` returns all segments written on a given day — useful for GC audit and time-bounded operations
- Volume and fork are visible directly in the key — no manifest lookup needed to identify ownership

### Retrieval strategies

The header is 96 bytes; all section offsets are computable from it. This drives three distinct retrieval patterns:

**Cold start** (no local data — cannot use deltas):
```
Single GET of the entire file.
Delta body is at the end; the extra bytes are the cost of one request instead of two.
Parse index section → materialise all extents from body.
```

**Warm start** (some local data):
```
1. GET [0, body_offset)         — header + index + inline; make all fetch decisions
2. GET byte-ranges within body  — full extents needed (ranges coalesced)
3. GET byte-ranges within delta — delta blobs where source is available locally (ranges coalesced)
```

Steps 2 and 3 are independent and can be issued in parallel. Byte ranges within each section are sorted and nearby ranges merged into single GETs before issuing.

**Index-only** (startup LBA map and extent index rebuild):
```
GET [0, inline_offset)          — header + index section only; skip inline, body, delta
```

**Adaptive full-body fetch:** when the ratio of needed body bytes to `body_length` exceeds a threshold, a single GET of the body section is cheaper than many byte-range GETs. Threshold is byte-ratio based (not count-based) since extents are variable size.

### Snapshot indexes

Snapshot indexes are consolidated index-section views written at snapshot time, covering all extents reachable from that snapshot. They are smaller than the full set of per-segment index sections and remain immutable. A snapshot index enables fast cold startup on a new host: download the snapshot index, then download index sections for segments written since the snapshot, union to get the full extent index.

**Index recovery flow:**
```
1. GET snapshot index for the relevant snapshot (if available)
2. GET [0, inline_offset) for each segment written since that snapshot
3. Union → full extent index
```

**Segment files are the ground truth.** All derived structures (in-memory extent index, optional manifest) are caches reconstructible from segment files. On cold start or after index loss, reconstruction is: download index sections of all segment files (fast, small) rather than full segment bodies.

---

## Fork Ownership and Signing

Each fork has exactly one owner: the host that holds its private key. This is a convention enforced at consumption time — signing does not prevent an unauthorised client from uploading bytes to S3 (that is an access-control concern handled at the object-store level), but it does mean any such upload will be **detected and rejected** when a demand-fetch client verifies the segment.

**This is not a key management system.** Elide generates an Ed25519 keypair when a fork is created and stores both files in the fork directory. There is no key escrow, rotation, revocation, or HSM integration. The guarantee is simple: a client without the private key cannot produce a valid segment signature for that fork. If `fork.key` is copied to another host, that host becomes an equally valid signer — the single-owner property then depends entirely on the operator keeping the key on one machine.

The practical value is narrow but real: it catches the common misconfiguration case where a coordinator is pointed at the wrong fork, and it provides per-segment integrity at demand-fetch time. It does not replace proper access control on the S3 bucket.

"Moving" a fork to a different host is done by creating a new fork from a snapshot on the destination host; the new fork gets a fresh keypair. The original fork's private key is not transferred — that would make two valid signers for the same fork.

### Key files

Two files live in the fork directory:

```
forks/<name>/fork.key   — Ed25519 private key (32 bytes); never leaves the host
forks/<name>/fork.pub   — Ed25519 public key (32 bytes); also uploaded to S3 at fork creation
```

S3 location of the public key:
```
<volume_id>/<fork_name>/fork.pub
```

The S3 copy of `fork.pub` is a convenience for hosts that need to verify ancestor segments they do not own. It is not authoritative — if S3 credentials are compromised an attacker could overwrite it, which would allow a crafted segment to pass verification. Locally-pinned public keys (see Verification below) are more trustworthy.

### Signing

Every segment is signed by its fork's private key at **promotion time** — when the WAL is promoted to `pending/<ulid>`. The segment file is complete and signed before it leaves the host.

**Signing input:** `BLAKE3(header[0..32] || index_section_bytes)`

The first 32 bytes of the header (all fields except the 64-byte signature field) are hashed together with the raw bytes of the index section. The signature field at `header[32..96]` is not included — it holds the output.

Signing at promotion rather than upload means every copy of the segment (`pending/`, `segments/`, S3) carries the same signature. The coordinator uploads the file unchanged; no re-signing step.

**Segments written before key generation** (e.g. by `elide-import`) have an all-zero signature field. These are accepted locally but will be rejected on demand-fetch by a strict verifier. This is expected: imported base segments predate the fork keypair and are not covered by the single-writer guarantee.

### Verification

Verification at demand-fetch time requires only the header and index section — the same bytes retrieved in an index-only fetch (`GET [0, inline_offset)`):

1. Verify `sig` against `BLAKE3(header[0..32] || index_bytes)` using the fork's public key — confirms the index was written by the keyholder
2. On each subsequent byte-range fetch from the body: verify `BLAKE3(fetched_bytes) == entry.hash` against the signed index — confirms the body bytes are what the keyholder wrote

A segment with a missing, malformed, or invalid signature is rejected: not cached, not served.

**Public key trust:** verification uses a locally-pinned copy of the public key, not the S3 copy. For the live fork this is `forks/<name>/fork.pub` on disk. For ancestor forks (segments written by a parent fork that this host does not own), the public key is fetched from S3 once and pinned locally before demand-fetch is enabled for that ancestor. Trust-on-first-use from S3 is a known limitation — it is weaker than out-of-band key distribution but sufficient for the misconfiguration-detection use case.
