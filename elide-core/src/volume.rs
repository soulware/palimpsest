// Volume: top-level I/O interface — owns the LBA map, WAL, and directory layout.
//
// Directory layout:
//   <base>/wal/       — active write-ahead log (at most one file at a time)
//   <base>/pending/   — promoted segments awaiting S3 upload
//   <base>/index/     — coordinator-written LBA index files (*.idx); permanent; never evicted
//   <base>/cache/     — coordinator-written body cache (*.body, *.present); evictable
//   <base>/gc/        — coordinator GC handoff files (*.pending → *.applied → *.done)
//
// Write path:
//   1. Volume::write(lba, data) — hashes data, appends to WAL, updates LBA map
//      and extent index (WAL offset as temporary location)
//   2. When the WAL reaches FLUSH_THRESHOLD, it is promoted to a clean segment
//      in pending/ and the extent index is updated to segment offsets
//
// Read path:
//   1. lbamap.lookup(lba) → (hash, block_offset)
//   2. extent_index.lookup(hash) → ExtentLocation (segment_id, body_offset, body_length)
//   3. find_segment_file (wal/ → pending/ → gc/*.applied → cache/<id>.body) → open file, seek, read
//
// Recovery:
//   Volume::open() calls lbamap::rebuild_segments() (segments only), then
//   scans the WAL once: that single pass truncates any partial-tail record,
//   replays entries into the LBA map, extent index, and pending_entries.
//   Any .tmp files in pending/ are removed (incomplete promotions).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use segment::BoxFetcher;

use ulid::Ulid;

use crate::{
    extentindex,
    gc::{GcHandoff, GcHandoffState, HandoffLine},
    lbamap,
    segment::{self, EntryKind},
    ulid_mint::UlidMint,
    writelog,
};

/// Compute the Shannon entropy of `data` in bits per byte.
///
/// Used to gate compression: data with entropy above 7.0 bits/byte is
/// already close to random and unlikely to compress meaningfully.
fn shannon_entropy(data: &[u8]) -> f64 {
    let mut counts = [0u32; 256];
    for &b in data {
        counts[b as usize] += 1;
    }
    let len = data.len() as f64;
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / len;
            -p * p.log2()
        })
        .sum()
}

/// Entropy threshold above which compression is skipped (bits/byte).
///
/// Taken from the lab47/lsvd reference implementation. Data at or above this
/// level is already near-random and compression would at best be a no-op.
const ENTROPY_THRESHOLD: f64 = 7.0;

/// Minimum compression ratio required to store compressed data (1.5×).
///
/// If the compressed payload is not at least 1/3 smaller than the original,
/// the compression overhead is not worth it and the raw data is stored instead.
const MIN_COMPRESSION_RATIO_NUM: usize = 3;
const MIN_COMPRESSION_RATIO_DEN: usize = 2;

/// Attempt lz4 compression on `data`.
///
/// Returns `Some(compressed_bytes)` if the entropy is low enough and the
/// compression ratio meets the minimum threshold; `None` to store raw.
fn maybe_compress(data: &[u8]) -> Option<Vec<u8>> {
    if shannon_entropy(data) > ENTROPY_THRESHOLD {
        return None;
    }
    let compressed = lz4_flex::compress_prepend_size(data);
    // Only keep if we achieve at least MIN_COMPRESSION_RATIO (1.5×).
    if compressed.len() * MIN_COMPRESSION_RATIO_NUM / MIN_COMPRESSION_RATIO_DEN >= data.len() {
        return None;
    }
    Some(compressed)
}

/// WAL size (bytes) at which the log is promoted to a pending segment.
/// This is a soft cap: a single write larger than this threshold will still
/// succeed, producing a segment larger than intended. The block layer
/// (NBD/ublk) enforces its own per-request maximum before reaching here.
const FLUSH_THRESHOLD: u64 = 32 * 1024 * 1024;

/// Maximum byte length of a single write. The segment format stores
/// `body_length` as a `u32`, so payloads must fit in 4 GiB. We cap at
/// `u32::MAX` rounded down to a 4 KiB boundary.
const MAX_WRITE_SIZE: usize = (u32::MAX as usize / 4096) * 4096;

/// Sentinel hash used in the LBA map and segment entries to represent an
/// explicitly-zeroed LBA range. All-zero bytes cannot be a valid BLAKE3 output
/// for any non-trivial input; finding a preimage would require breaking 256-bit
/// hash preimage resistance.
pub const ZERO_HASH: blake3::Hash = blake3::Hash::from_bytes([0u8; 32]);

/// Results from a single compaction run.
#[derive(Debug, Default)]
pub struct CompactionStats {
    /// Number of input segments consumed (deleted after compaction).
    pub segments_compacted: usize,
    /// Number of output segments written.
    pub new_segments: usize,
    /// Stored bytes reclaimed from deleted segment bodies.
    pub bytes_freed: u64,
    /// Number of dead extent entries removed from the extent index.
    pub extents_removed: usize,
}

/// A fork ancestry layer used when rebuilding the LBA map and extent index.
///
/// `branch_ulid` is the latest segment ULID from this fork that belongs to the
/// derived fork's view — segments with a strictly greater ULID were written after
/// the branch point and must not be included. `None` for the live (current) fork,
/// where all segments are always included.
#[derive(Clone)]
pub struct AncestorLayer {
    pub dir: PathBuf,
    pub branch_ulid: Option<String>,
}

/// A writable block-device volume backed by a content-addressable store.
///
/// Owns the in-memory LBA map, the active WAL, and the directory layout.
/// In the Named Forks model, `base_dir` is the fork directory (e.g.
/// `volumes/myvm/default/`), not the volume root.
pub struct Volume {
    base_dir: PathBuf,
    /// Ancestor fork layers, oldest-first. Does not include the current fork.
    ancestor_layers: Vec<AncestorLayer>,
    /// Exclusive lock on `base_dir/volume.lock`. Held for the lifetime of the Volume.
    /// The `Flock` releases the lock automatically when dropped.
    #[allow(dead_code)]
    lock_file: nix::fcntl::Flock<fs::File>,
    lbamap: Arc<lbamap::LbaMap>,
    extent_index: Arc<extentindex::ExtentIndex>,
    wal: writelog::WriteLog,
    wal_ulid: Ulid,
    wal_path: PathBuf,
    /// DATA and REF extents written since the last promotion; used to write
    /// the clean segment file on the next promote().
    pending_entries: Vec<segment::SegmentEntry>,
    /// True if at least one segment has been committed since the last snapshot
    /// (or since open, if no snapshot has been taken this session). Used by
    /// `snapshot()` to decide whether a new marker is needed or the latest
    /// existing snapshot can be reused.
    has_new_segments: bool,
    /// ULID of the most recently committed segment across pending/ and index/,
    /// or `None` if no segments exist. Used by `snapshot()` to name the snapshot
    /// marker with the same ULID as the segment it covers.
    last_segment_ulid: Option<Ulid>,
    /// Single-entry file handle cache for the read path.
    ///
    /// Retains the last opened segment file across `read` calls so that
    /// sequential reads hitting the same segment avoid repeated `open` syscalls.
    /// `RefCell` keeps `read` logically non-mutating (`&self`) while allowing
    /// the cache to be updated internally.
    file_cache: RefCell<Option<(Ulid, bool, fs::File)>>,
    /// Signer for segment promotion. Every segment written by this volume
    /// (at WAL promotion and compaction) is signed with the fork's private key.
    /// See `segment::SegmentSigner`.
    signer: Arc<dyn segment::SegmentSigner>,
    /// Verifying key derived from `volume.key` at open time. Used to verify
    /// segment signatures when reading during compaction and GC.
    verifying_key: ed25519_dalek::VerifyingKey,
    /// Optional fetcher for demand-fetch on segment cache miss. When set,
    /// `find_segment_file` fetches missing segments from remote storage and
    /// caches them in `cache/`. See `segment::SegmentFetcher`.
    fetcher: Option<BoxFetcher>,
    /// Monotonic ULID generator. Seeded from the highest known ULID at open
    /// (WAL filename or max segment). Used for all WAL and compaction outputs
    /// to guarantee strict ordering regardless of host clock behaviour.
    mint: UlidMint,
}

impl Volume {
    /// Open (or create) a fork at `base_dir`.
    ///
    /// `base_dir` must be the fork directory (e.g. `volumes/myvm/default/`), not the
    /// volume root. Creates `wal/` and `pending/` if they do not exist.
    /// Rebuilds the LBA map from all committed segments across the ancestry chain
    /// (following `volume.parent` files), then recovers or creates the WAL.
    ///
    /// Loads the signing key from `volume.key` in `base_dir`. Fails hard if the key
    /// is absent — every writable volume must have a signing key. Fork from a snapshot
    /// to create a new writable volume with a fresh keypair.
    pub fn open(base_dir: &Path, by_id_dir: &Path) -> io::Result<Self> {
        let (signer, verifying_key) =
            crate::signing::load_keypair(base_dir, crate::signing::VOLUME_KEY_FILE).map_err(
                |e| {
                    io::Error::other(format!(
                        "{e}; fork from a snapshot to create a writable volume"
                    ))
                },
            )?;
        Self::open_impl(base_dir, signer, verifying_key, by_id_dir)
    }

    fn open_impl(
        base_dir: &Path,
        signer: Arc<dyn segment::SegmentSigner>,
        verifying_key: ed25519_dalek::VerifyingKey,
        by_id_dir: &Path,
    ) -> io::Result<Self> {
        let wal_dir = base_dir.join("wal");
        let pending_dir = base_dir.join("pending");

        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&pending_dir)?;

        // Acquire exclusive lock. Fails immediately if another process has this
        // fork open. The lock is released when Volume is dropped.
        let lock_file = acquire_lock(base_dir)?;

        // Remove any .tmp files in pending/ — incomplete promotions from a crash.
        for entry in fs::read_dir(&pending_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(&path)?;
            }
        }

        // Walk the origin chain and rebuild maps from all committed segments.
        let (ancestor_layers, mut lbamap, mut extent_index) = open_read_state(base_dir, by_id_dir)?;

        // Find the in-progress WAL file (there should be at most one).
        let mut wal_files: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                wal_files.push(entry.path());
            }
        }
        wal_files.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        // Edge case: if pending/<ulid> already exists alongside wal/<ulid>,
        // the promotion completed (rename succeeded) but the WAL delete was
        // interrupted. The segment is authoritative — delete the stale WAL file.
        wal_files.retain(|path| {
            let Some(ulid) = path.file_name().and_then(|s| s.to_str()) else {
                return true; // non-UTF-8 name: leave it alone
            };
            if pending_dir.join(ulid).exists() {
                let _ = fs::remove_file(path);
                false
            } else {
                true
            }
        });

        // Scan pending/ and index/ to find the latest committed segment ULID
        // and determine whether any segments postdate the latest snapshot.
        // Cross-session ULID comparison is reliable: those files came from
        // earlier runs at distinct timestamps.
        //
        // Done before WAL recovery so we can compute the mint floor below.
        let latest_snap = latest_snapshot(base_dir)?;
        let mut last_segment_ulid: Option<Ulid> = None;
        // Collect pending/ segment ULIDs (full files, not yet uploaded).
        for p in segment::collect_segment_files(&base_dir.join("pending"))? {
            if let Some(ulid) = p
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| Ulid::from_string(s).ok())
                && last_segment_ulid < Some(ulid)
            {
                last_segment_ulid = Some(ulid);
            }
        }
        // Collect index/*.idx ULIDs (uploaded segments; file stem is the ULID).
        for p in segment::collect_idx_files(&base_dir.join("index"))? {
            if let Some(ulid) = p
                .file_stem()
                .and_then(|n| n.to_str())
                .and_then(|s| Ulid::from_string(s).ok())
                && last_segment_ulid < Some(ulid)
            {
                last_segment_ulid = Some(ulid);
            }
        }
        // A GC output in .applied state has a ULID = max(inputs).increment(),
        // which may be the highest known ULID — include it so the mint floor is correct.
        for p in segment::collect_gc_applied_segment_files(base_dir)? {
            if let Some(ulid) = p
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| Ulid::from_string(s).ok())
                && last_segment_ulid < Some(ulid)
            {
                last_segment_ulid = Some(ulid);
            }
        }

        // Compute the mint floor: max of the highest segment ULID and the
        // WAL filename ULID (if one exists). This guarantees the first fresh
        // WAL ULID is above all existing local data even when the system clock
        // has drifted backwards.
        let segment_floor = last_segment_ulid.unwrap_or(Ulid::from_parts(0, 0));
        let wal_floor = wal_files
            .last()
            .and_then(|p| p.file_name().and_then(|n| n.to_str()))
            .and_then(|s| Ulid::from_string(s).ok())
            .unwrap_or(Ulid::from_parts(0, 0));
        let mut mint = UlidMint::new(segment_floor.max(wal_floor));

        // recover_wal does the single WAL scan: truncates any partial tail,
        // replays records into the LBA map, and rebuilds pending_entries.
        let (wal, wal_ulid, wal_path, pending_entries) =
            if let Some(path) = wal_files.into_iter().last() {
                recover_wal(path, &mut lbamap, &mut extent_index)?
            } else {
                create_fresh_wal(&wal_dir, mint.next())?
            };

        let has_new_segments = !pending_entries.is_empty()
            || matches!((&latest_snap, &last_segment_ulid), (Some(snap), Some(last)) if last > snap);

        Ok(Self {
            base_dir: base_dir.to_owned(),
            ancestor_layers,
            lock_file,
            lbamap: Arc::new(lbamap),
            extent_index: Arc::new(extent_index),
            wal,
            wal_ulid,
            wal_path,
            pending_entries,
            has_new_segments,
            last_segment_ulid,
            file_cache: RefCell::new(None),
            signer,
            verifying_key,
            fetcher: None,
            mint,
        })
    }

    /// Write `data` starting at logical block address `lba`.
    ///
    /// `data.len()` must be a non-zero multiple of 4096 and must not exceed
    /// `MAX_WRITE_SIZE` (4 GiB − 4 KiB). The segment format stores `body_length`
    /// as a `u32` byte count, so larger payloads cannot be represented.
    ///
    /// The data is appended to the WAL and the LBA map is updated in memory.
    /// Promotion to a pending segment is triggered after the write if the WAL
    /// reaches `FLUSH_THRESHOLD` (32 MiB). Because the check is post-write, a
    /// single large write may produce a segment larger than the threshold; the
    /// block layer (NBD/ublk) is expected to enforce its own per-request cap.
    pub fn write(&mut self, lba: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() || !data.len().is_multiple_of(4096) {
            return Err(io::Error::other(
                "data length must be a non-zero multiple of 4096",
            ));
        }
        if data.len() > MAX_WRITE_SIZE {
            return Err(io::Error::other(
                "data length exceeds maximum write size (4 GiB − 4 KiB)",
            ));
        }
        let lba_length = (data.len() / 4096) as u32;
        let hash = blake3::hash(data);

        let compressed_data = maybe_compress(data);
        let compressed = compressed_data.is_some();
        let owned_data: Vec<u8> = compressed_data.unwrap_or_else(|| data.to_vec());
        let wal_flags = if compressed {
            writelog::WalFlags::COMPRESSED
        } else {
            writelog::WalFlags::empty()
        };

        // Write-path dedup: if this extent already exists in this volume's
        // segment tree (own segments + ancestors), write a thin REF record
        // instead of a DATA record. No body bytes in the WAL — reads resolve
        // through the extent index to the canonical segment's body.
        if self.extent_index.lookup(&hash).is_some() {
            self.wal.append_ref(lba, lba_length, &hash)?;
            Arc::make_mut(&mut self.lbamap).insert(lba, lba_length, hash);
            // Do NOT update extent_index — the canonical entry already points
            // to the segment with the body bytes.
            self.pending_entries
                .push(segment::SegmentEntry::new_dedup_ref(hash, lba, lba_length));
            return Ok(());
        }

        let seg_flags = if compressed {
            segment::SegmentFlags::COMPRESSED
        } else {
            segment::SegmentFlags::empty()
        };

        let body_offset = self
            .wal
            .append_data(lba, lba_length, &hash, wal_flags, &owned_data)?;
        Arc::make_mut(&mut self.lbamap).insert(lba, lba_length, hash);
        // Temporary extent index entry: points into the WAL at the raw payload offset.
        // Updated to segment file offsets after promotion.
        Arc::make_mut(&mut self.extent_index).insert(
            hash,
            extentindex::ExtentLocation {
                segment_id: self.wal_ulid,
                body_offset,
                body_length: owned_data.len() as u32,
                compressed,
                entry_idx: None,
                body_section_start: 0,
            },
        );
        self.pending_entries.push(segment::SegmentEntry::new_data(
            hash, lba, lba_length, seg_flags, owned_data,
        ));

        Ok(())
    }

    /// Zero `lba_count` blocks starting at `lba`.
    ///
    /// Appends a single ZERO WAL record covering the entire range — no hashing,
    /// no data payload, no chunking. The LBA map entry uses `ZERO_HASH` as a
    /// sentinel, which the read path recognises and short-circuits to return
    /// zeros without any extent index lookup.
    ///
    /// Zero extents explicitly override ancestor data: a ZERO_HASH entry in the
    /// LBA map masks any data at those LBAs in ancestor segments, unlike an
    /// unwritten LBA range which falls through to the ancestor.
    pub fn write_zeroes(&mut self, start_lba: u64, lba_count: u32) -> io::Result<()> {
        self.wal.append_zero(start_lba, lba_count)?;
        Arc::make_mut(&mut self.lbamap).insert(start_lba, lba_count, ZERO_HASH);
        self.pending_entries
            .push(segment::SegmentEntry::new_zero(start_lba, lba_count));
        Ok(())
    }

    /// Trim (discard) `lba_count` blocks starting at `lba`.
    ///
    /// Implemented via `write_zeroes` — a single zero-extent WAL record with no
    /// data payload. The whole-volume TRIM issued by `mkfs.ext4` becomes one
    /// ~40-byte record regardless of volume size.
    pub fn trim(&mut self, start_lba: u64, lba_count: u32) -> io::Result<()> {
        self.write_zeroes(start_lba, lba_count)
    }

    /// Read `lba_count` blocks (4096 bytes each) starting at `lba`.
    ///
    /// Blocks that have never been written are returned as zeros (the
    /// block-device convention for unwritten regions). Written blocks are
    /// fetched extent-by-extent: one file open and one read (or decompress)
    /// per extent, regardless of how many blocks within the extent are needed.
    pub fn read(&self, lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        read_extents(
            lba,
            lba_count,
            &self.lbamap,
            &self.extent_index,
            &self.file_cache,
            |id, bss, idx| self.find_segment_file(id, bss, idx),
        )
    }

    /// Flush buffered WAL writes and fsync to disk.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.wal.fsync()
    }

    /// Compact sparse segments in `pending/`.
    ///
    /// For each segment where the ratio of live stored bytes to total stored
    /// bytes is below `min_live_ratio`, the live extents are copied into a new
    /// denser segment in `pending/` and the old segment is deleted. Segments
    /// where all extents are dead are deleted directly without writing a new one.
    ///
    /// The WAL is not touched. The extent index is updated in place.
    ///
    /// `min_live_ratio` is in [0.0, 1.0]: 0.7 compacts any segment where more
    /// than 30% of stored bytes are dead.
    pub fn repack(&mut self, min_live_ratio: f64) -> io::Result<CompactionStats> {
        use std::collections::HashSet;

        let live: HashSet<blake3::Hash> = self.lbamap.live_hashes();
        let mut stats = CompactionStats::default();

        // Segments at or below the latest snapshot ULID are frozen: they may be
        // referenced by child forks that branched from a snapshot in this fork.
        // Only post-snapshot segments are eligible for compaction.
        let floor: Option<Ulid> = latest_snapshot(&self.base_dir)?;

        let all_segs = segment::collect_segment_files(&self.base_dir.join("pending"))?;

        for seg_path in all_segs {
            let seg_id = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            let seg_id = Ulid::from_string(seg_id).map_err(|e| io::Error::other(e.to_string()))?;

            // Skip segments frozen by the latest snapshot.
            if floor.is_some_and(|f| seg_id <= f) {
                continue;
            }

            let (body_section_start, mut entries) =
                match segment::read_and_verify_segment_index(&seg_path, &self.verifying_key) {
                    Ok(v) => v,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };

            // Only DATA and MaterializedRef entries have body bytes.
            // Thin DedupRef and Zero have stored_length=0.
            let total_bytes: u64 = entries
                .iter()
                .filter(|e| matches!(e.kind, EntryKind::Data | EntryKind::MaterializedRef))
                .map(|e| e.stored_length as u64)
                .sum();

            if total_bytes == 0 {
                continue;
            }

            let live_bytes: u64 = entries
                .iter()
                .filter(|e| {
                    matches!(e.kind, EntryKind::Data | EntryKind::MaterializedRef)
                        && live.contains(&e.hash)
                })
                .map(|e| e.stored_length as u64)
                .sum();

            if live_bytes as f64 / total_bytes as f64 >= min_live_ratio {
                continue;
            }

            let (mut live_entries, dead_entries): (Vec<_>, Vec<_>) =
                entries.drain(..).partition(|e| match e.kind {
                    EntryKind::Zero => self.lbamap.hash_at(e.start_lba) == Some(ZERO_HASH),
                    EntryKind::DedupRef | EntryKind::MaterializedRef => {
                        self.lbamap.hash_at(e.start_lba) == Some(e.hash)
                    }
                    EntryKind::Data | EntryKind::Inline => live.contains(&e.hash),
                });

            // Remove dead entries from the extent index (only those pointing at
            // this segment — entries pointing elsewhere belong to another copy).
            // Thin DedupRef and Zero entries are not in the extent index.
            let mut removed = 0usize;
            for entry in &dead_entries {
                if entry.kind == EntryKind::Zero || entry.kind == EntryKind::DedupRef {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .map(|loc| loc.segment_id == seg_id)
                    .unwrap_or(false)
                {
                    Arc::make_mut(&mut self.extent_index).remove(&entry.hash);
                    removed += 1;
                }
            }

            if !live_entries.is_empty() {
                // Read body bytes for live entries, then write a new denser segment.
                segment::read_extent_bodies(&seg_path, body_section_start, &mut live_entries)?;

                // Reuse the source segment's own ULID for the output.  This
                // guarantees the output ULID < the current WAL ULID (all segments
                // predate the current WAL), so a subsequent WAL flush always
                // produces a higher ULID and wins on rebuild.  Using mint.next()
                // here would generate a ULID past the WAL ULID and break that
                // ordering — the same bug fixed in sweep_pending.
                let new_ulid = seg_id;
                let new_ulid_str = new_ulid.to_string();
                let pending_dir = self.base_dir.join("pending");
                let tmp_path = pending_dir.join(format!("{new_ulid_str}.tmp"));
                let final_path = pending_dir.join(&new_ulid_str);
                // write_segment reassigns stored_offset in live_entries to new positions.
                let new_bss =
                    segment::write_segment(&tmp_path, &mut live_entries, self.signer.as_ref())?;
                fs::rename(&tmp_path, &final_path)?;
                segment::fsync_dir(&final_path)?;
                stats.new_segments += 1;

                for entry in &live_entries {
                    match entry.kind {
                        EntryKind::Data | EntryKind::MaterializedRef => {
                            Arc::make_mut(&mut self.extent_index).insert(
                                entry.hash,
                                extentindex::ExtentLocation {
                                    segment_id: new_ulid,
                                    body_offset: entry.stored_offset,
                                    body_length: entry.stored_length,
                                    compressed: entry.compressed,
                                    entry_idx: None,
                                    body_section_start: new_bss,
                                },
                            );
                        }
                        EntryKind::DedupRef | EntryKind::Zero | EntryKind::Inline => {}
                    }
                }
            }

            // Evict the old segment from the file handle cache before deleting it.
            let mut cache = self.file_cache.borrow_mut();
            if cache.as_ref().map(|(id, _, _)| *id) == Some(seg_id) {
                *cache = None;
            }
            drop(cache);

            // The rename above replaced the source atomically (final_path == seg_path),
            // so there is nothing left to delete.
            stats.segments_compacted += 1;
            stats.bytes_freed += total_bytes - live_bytes;
            stats.extents_removed += removed;
        }

        Ok(stats)
    }

    /// Minimum segment file size below which a `pending/` segment is always a
    /// merge candidate regardless of its live ratio.
    const COMPACT_SMALL_THRESHOLD: u64 = 8 * 1024 * 1024;

    /// Compact `pending/` segments opportunistically, before upload.
    ///
    /// Scans every segment in `pending/`. A segment is a candidate if:
    /// - it has at least one dead extent (an LBA since overwritten), or
    /// - its file size is below [`COMPACT_SMALL_THRESHOLD`] (8 MiB).
    ///
    /// All candidates are merged: their live extents are collected, written into
    /// one or more new `pending/<ulid>` segments (split at [`FLUSH_THRESHOLD`]),
    /// the extent index is updated, and the originals are deleted.
    ///
    /// Segments at or below the latest snapshot ULID are frozen and skipped.
    /// Returns immediately (no-op) if there are no candidates.
    pub fn sweep_pending(&mut self) -> io::Result<CompactionStats> {
        use std::collections::HashSet;

        let live: HashSet<blake3::Hash> = self.lbamap.live_hashes();
        let mut stats = CompactionStats::default();

        let floor: Option<Ulid> = latest_snapshot(&self.base_dir)?;

        let pending_dir = self.base_dir.join("pending");
        let mut seg_paths = segment::collect_segment_files(&pending_dir)?;
        // Sort by filename (ULID) ascending so entries appear oldest-first in
        // the merged output.  rebuild_segments applies entries in sequence and
        // the last entry wins for each LBA, so this guarantees the most-recent
        // write takes precedence even when two candidates both cover the same LBA
        // with the same data hash (hash-based liveness keeps both alive but
        // ordering ensures the correct one survives crash+rebuild).
        seg_paths.sort_unstable_by(|a, b| a.file_name().cmp(&b.file_name()));

        let mut candidate_paths: Vec<std::path::PathBuf> = Vec::new();
        let mut merged_live: Vec<segment::SegmentEntry> = Vec::new();
        let mut any_dead = false;

        for seg_path in &seg_paths {
            let seg_filename = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            let seg_ulid =
                Ulid::from_string(seg_filename).map_err(|e| io::Error::other(e.to_string()))?;

            if floor.is_some_and(|f| seg_ulid <= f) {
                continue;
            }

            let file_size = fs::metadata(seg_path)?.len();
            let (body_section_start, mut entries) =
                segment::read_and_verify_segment_index(seg_path, &self.verifying_key)?;

            let has_dead = entries.iter().any(|e| !live.contains(&e.hash));
            let is_small = file_size < Self::COMPACT_SMALL_THRESHOLD;

            if !has_dead && !is_small {
                continue;
            }

            if has_dead {
                any_dead = true;
            }

            let (live_entries, dead_entries): (Vec<_>, Vec<_>) =
                entries.drain(..).partition(|e| match e.kind {
                    EntryKind::DedupRef => {
                        // A dedup ref is only live if the LBA still maps to
                        // this hash. If the LBA was overwritten with different
                        // data, carrying the stale ref would reintroduce the
                        // old mapping after crash + rebuild.
                        self.lbamap.hash_at(e.start_lba) == Some(e.hash)
                    }
                    _ => live.contains(&e.hash),
                });

            let dead_bytes: u64 = dead_entries.iter().map(|e| e.stored_length as u64).sum();

            for entry in &dead_entries {
                if entry.kind == EntryKind::Zero || entry.kind == EntryKind::DedupRef {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .map(|loc| loc.segment_id == seg_ulid)
                    .unwrap_or(false)
                {
                    Arc::make_mut(&mut self.extent_index).remove(&entry.hash);
                    stats.extents_removed += 1;
                }
            }

            let mut live_entries = live_entries;
            segment::read_extent_bodies(seg_path, body_section_start, &mut live_entries)?;
            merged_live.extend(live_entries);

            candidate_paths.push(seg_path.clone());
            stats.segments_compacted += 1;
            stats.bytes_freed += dead_bytes;
        }

        if candidate_paths.is_empty() {
            return Ok(stats);
        }

        // A single small segment with no dead extents gains nothing from
        // rewriting: the output would be the same size and content. Only
        // merge when dead space is reclaimed or two or more small segments
        // can be combined into one.
        if candidate_paths.len() == 1 && !any_dead {
            return Ok(CompactionStats::default());
        }

        // Use max(candidate ULIDs) as the output ULID. This guarantees the
        // output sorts below the current WAL ULID (all pending segments were
        // created before the WAL was opened, so their ULIDs are strictly less).
        // Preserving this invariant ensures that a WAL flush always produces a
        // segment with a higher ULID than any compact output, so rebuild always
        // applies data in write order. Using mint.next() here would generate a
        // ULID past the WAL ULID and break that ordering.
        //
        // The merged output is written as a single segment (no FLUSH_THRESHOLD
        // split). The split served only to bound segment size, but FLUSH_THRESHOLD
        // is a soft cap. Avoiding a split means we need only one output ULID,
        // which is safe to derive from the inputs.
        let new_ulid = candidate_paths
            .iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|s| Ulid::from_string(s).ok())
            })
            .max()
            .ok_or_else(|| io::Error::other("sweep_pending: no valid candidate ULIDs"))?;
        let new_ulid_str = new_ulid.to_string();

        // Write the merged output, atomically replacing the max-ULID candidate.
        if !merged_live.is_empty() {
            let tmp_path = pending_dir.join(format!("{new_ulid_str}.tmp"));
            let final_path = pending_dir.join(&new_ulid_str);
            let new_bss =
                segment::write_segment(&tmp_path, &mut merged_live, self.signer.as_ref())?;
            fs::rename(&tmp_path, &final_path)?;
            segment::fsync_dir(&final_path)?;
            stats.new_segments += 1;

            for entry in &merged_live {
                match entry.kind {
                    EntryKind::Data | EntryKind::MaterializedRef => {
                        Arc::make_mut(&mut self.extent_index).insert(
                            entry.hash,
                            extentindex::ExtentLocation {
                                segment_id: new_ulid,
                                body_offset: entry.stored_offset,
                                body_length: entry.stored_length,
                                compressed: entry.compressed,
                                entry_idx: None,
                                body_section_start: new_bss,
                            },
                        );
                    }
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Inline => {}
                }
            }
        }

        // Evict and delete input candidates. The max-ULID candidate was already
        // replaced atomically by the output rename above; skip re-deleting it.
        for seg_path in &candidate_paths {
            let seg_ulid_opt = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| Ulid::from_string(s).ok());
            if seg_ulid_opt == Some(new_ulid) && !merged_live.is_empty() {
                continue; // already replaced atomically above
            }
            let mut cache = self.file_cache.borrow_mut();
            if seg_ulid_opt.is_some() && cache.as_ref().map(|(id, _, _)| *id) == seg_ulid_opt {
                *cache = None;
            }
            drop(cache);
            fs::remove_file(seg_path)?;
        }

        Ok(stats)
    }

    /// Establish a consistent checkpoint for coordinator GC.
    ///
    /// Mints three ULIDs from the volume's monotonic clock — `u_repack`,
    /// `u_sweep`, and `u_wal` — in that order, then flushes the current WAL to
    /// `pending/` under the name `u_wal` (not the WAL's existing ULID), and
    /// opens a fresh WAL with ULID > `u_wal`.  Returns `(u_repack, u_sweep)` to
    /// the coordinator.
    ///
    /// **Why four ULIDs, minted first.**
    ///
    /// The monotonic mint is a logical clock.  Pulling all four identifiers
    /// from it *before* any I/O encodes the relative ordering of operations in
    /// advance: `u_repack < u_sweep < u_flush < u_wal`.  The I/O steps then
    /// execute in the pre-determined logical order without requiring any
    /// coordination after the fact.
    ///
    /// Without pre-minting `u_flush`, the WAL segment flushed by this call would
    /// carry the WAL's *existing* ULID (assigned when the WAL was opened,
    /// before the GC ULIDs were minted).  That ULID is lower than `u_sweep`, so
    /// after the segment is drained to `index/`, crash-recovery rebuild would
    /// apply the GC output *after* the WAL segment and return stale data.
    ///
    /// When the WAL is empty, the WAL file is deleted and `u_flush` is not used
    /// (no segment is produced), so the empty-WAL case is also safe: the fresh
    /// WAL opened after minting carries a ULID > `u_sweep`.
    ///
    /// All ULIDs come from the volume's own monotonic mint, never from an
    /// external clock — coordinator clock skew cannot corrupt ULID ordering.
    pub fn gc_checkpoint(&mut self) -> io::Result<(Ulid, Ulid)> {
        // Mint all four ULIDs before any I/O.  The ordering constraint —
        // u_repack < u_sweep < u_flush < u_wal — is established here, before
        // any flush or WAL rotation.  UlidMint guarantees strict monotonicity
        // even within the same millisecond (increments random bits).
        let u_repack = self.mint.next();
        let u_sweep = self.mint.next();
        let u_flush = self.mint.next();
        let u_wal = self.mint.next();
        // Flush the current WAL to pending/ under u_flush.  If the WAL is
        // empty, the file is deleted and u_flush is unused (no segment produced).
        self.flush_wal_to_pending_as(u_flush)?;
        // Open a new WAL with u_wal > u_flush.
        let (wal, wal_ulid, wal_path, pending_entries) =
            create_fresh_wal(&self.base_dir.join("wal"), u_wal)?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        self.pending_entries = pending_entries;
        Ok((u_repack, u_sweep))
    }

    /// Apply pending GC handoff files written by the coordinator.
    ///
    /// The coordinator writes the compacted segment to `gc/<new-ulid>` (staged,
    /// signed with an ephemeral key) and then writes `gc/<new-ulid>.pending`.
    /// This method re-signs `gc/<new-ulid>` in-place with the volume's own key,
    /// updates the in-memory extent index, and renames the handoff file to
    /// `gc/<new-ulid>.applied`.  The coordinator then uploads the segment to S3
    /// and sends a `promote <new-ulid>` IPC.  The `promote_segment` handler writes
    /// `index/<new-ulid>.idx` and `cache/<new-ulid>.{body,present}`, and deletes
    /// `index/<old>.idx` for each consumed segment.
    ///
    /// This two-phase approach preserves the invariant: **`index/<ulid>.idx`
    /// present ↔ segment confirmed in S3**.  The idx is never written before the
    /// coordinator confirms the upload, so a segment in `gc/` or `pending/` with
    /// no idx is never mistaken for an S3-confirmed segment.
    ///
    /// **`.pending` handoffs** (normal path):
    /// Re-signs the coordinator-staged segment body with the volume key, applies
    /// extent index updates, then renames the file to `.applied` to signal the
    /// coordinator that it is safe to upload.
    ///
    /// **`.applied` handoffs** (restart-safety path):
    /// Re-applies extent index updates without re-signing or renaming.  On restart,
    /// `Volume::open` rebuilds the extent index from `gc/*.applied` (via
    /// `collect_gc_applied_segment_files`) which correctly shadows any stale
    /// `index/<old>.idx` entries still on disk.  The `still_at_old` check makes
    /// this re-application idempotent.
    ///
    /// Returns the number of handoff files processed.  Returns `Ok(0)` if the
    /// `gc/` directory does not exist yet (coordinator has not run).
    pub fn apply_gc_handoffs(&mut self) -> io::Result<usize> {
        let gc_dir = self.base_dir.join("gc");
        if !gc_dir.try_exists()? {
            return Ok(0);
        }

        let mut pending: Vec<(String, GcHandoff)> = fs::read_dir(&gc_dir)?
            .filter_map(|e| {
                let e = e.ok()?;
                let name = e.file_name().into_string().ok()?;
                let handoff = GcHandoff::from_filename(&name)?;
                handoff.state.needs_apply().then_some((name, handoff))
            })
            .collect();

        if pending.is_empty() {
            return Ok(0);
        }

        // Process oldest-first so the extent index is correct after a partial run.
        pending.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut count = 0;

        for (name, handoff) in &pending {
            let new_ulid = handoff.ulid;
            let new_ulid_str = new_ulid.to_string();
            let is_already_applied = handoff.state == GcHandoffState::Applied;

            // Parse the .pending / .applied file into typed HandoffLines.
            //
            // old_ulid_by_hash maps each hash to the old segment it came from
            // so the extent index can be updated only when it still points at
            // the old segment.  Dead lines carry no hash — they are a no-op
            // from the volume's perspective (just an acknowledgment).
            let pending_content = fs::read_to_string(gc_dir.join(name))?;
            let mut old_ulid_by_hash: HashMap<blake3::Hash, Ulid> = HashMap::new();
            let mut dead_ulids: Vec<Ulid> = Vec::new();
            let mut is_tombstone = false;
            for line in pending_content.lines() {
                match HandoffLine::parse(line) {
                    Some(HandoffLine::Repack { hash, old_ulid, .. })
                    | Some(HandoffLine::Remove { hash, old_ulid }) => {
                        old_ulid_by_hash.insert(hash, old_ulid);
                    }
                    Some(HandoffLine::Dead { old_ulid }) => {
                        dead_ulids.push(old_ulid);
                        is_tombstone = true;
                    }
                    None => {}
                }
            }

            // The coordinator stages its output in gc/<ulid> with an ephemeral
            // key.  Re-sign it in-place with the volume's key (write to
            // gc/<ulid>.tmp, rename over gc/<ulid>).  The body stays in gc/
            // until the coordinator uploads it to S3 and writes index/<ulid>.idx
            // + cache/<ulid>.{body,present}, then deletes gc/<ulid>.
            //
            // Idempotency: re-signing is a pure function of the segment content;
            // if we crash mid-rename and retry, the output is identical.
            let gc_seg_path = gc_dir.join(&new_ulid_str);
            // .pending handoffs: re-sign the coordinator-staged body with the volume key.
            // .applied handoffs: already volume-signed; skip re-signing.
            if !is_already_applied && gc_seg_path.try_exists()? {
                let (bss, mut entries) = segment::read_segment_index(&gc_seg_path)?;
                segment::read_extent_bodies(&gc_seg_path, bss, &mut entries)?;
                let tmp_path = gc_dir.join(format!("{new_ulid_str}.tmp"));
                segment::write_segment(&tmp_path, &mut entries, self.signer.as_ref())?;
                fs::rename(&tmp_path, &gc_seg_path)?;
            }

            // Locate the segment for index reads: normally gc/ (the volume-signed
            // body); after a restart where the coordinator already uploaded and
            // deleted gc/<ulid>, fall back to index/<ulid>.idx — it contains the
            // same header+index section and is sufficient for read_and_verify_segment_index.
            let body_path: Option<PathBuf> = if gc_seg_path.try_exists()? {
                Some(gc_seg_path.clone())
            } else if is_already_applied {
                let idx_path = self
                    .base_dir
                    .join("index")
                    .join(format!("{new_ulid_str}.idx"));
                idx_path.try_exists()?.then_some(idx_path)
            } else {
                None
            };
            let segment_exists = body_path.is_some();

            // If the new segment doesn't exist locally, only some handoff
            // types can proceed without it:
            //   tombstone — no segment ever exists; just acknowledge.
            //   removal-only — no segment needed; just clean extent index.
            //   repack — needs the segment for extent index updates;
            //            defer until available (e.g. fetched from S3).
            if !segment_exists {
                let has_carried = pending_content
                    .lines()
                    .any(|l| matches!(HandoffLine::parse(l), Some(HandoffLine::Repack { .. })));
                if !is_tombstone && (has_carried || old_ulid_by_hash.is_empty()) {
                    continue;
                }
            }

            // Read the (now volume-signed) compacted segment's index.  This
            // is done once and reused for both the carried_hashes scan and the
            // extent index update, avoiding a second signature verification.
            let segment_index = body_path
                .as_ref()
                .map(|bp| segment::read_and_verify_segment_index(bp, &self.verifying_key))
                .transpose()?;

            // First pass: build carried_hashes WITHOUT touching the extent
            // index.  We must know the full set before the Bug B check below,
            // because the check must run before any extent index mutations —
            // if we cancel mid-apply the index would be left in a partially
            // updated state.
            let mut carried_hashes: HashSet<blake3::Hash> = HashSet::new();
            if let Some((_, ref entries)) = segment_index {
                for e in entries {
                    if e.kind != EntryKind::DedupRef {
                        carried_hashes.insert(e.hash);
                    }
                }
            }

            // Bug B: a DEDUP_REF written after gc_checkpoint makes a hash H
            // live again in the LBA map, but the coordinator's liveness view
            // (built at gc_fork time) did not see it.  H is therefore absent
            // from carried_hashes, yet deleting the old segment would
            // permanently lose H's data — the extent index entry for H still
            // points to the old segment, which apply_done_handoffs would
            // delete.
            //
            // Detection: any hash in old_ulid_by_hash that is (a) not
            // carried into the GC output and (b) still referenced by an LBA.
            //
            // Resolution: cancel this GC pass by deleting the .pending file
            // (and the stale body if present) so gc_fork can re-run with
            // current liveness data on the next tick.  The old segment
            // remains until the corrected GC pass handles it safely.
            //
            // This check MUST precede any extent index mutations (second pass
            // below) so there is nothing to undo if we cancel.
            //
            // For .applied handoffs: skip this check.  The stale-liveness
            // detection already ran (and passed) before the .applied marker
            // was created.  Re-running it after a restart would incorrectly
            // cancel a committed handoff — the .applied state means the volume
            // already acknowledged that deleting the old segment is safe.
            let live = self.lbamap.live_hashes();
            if !is_already_applied {
                let stale: Vec<blake3::Hash> = old_ulid_by_hash
                    .keys()
                    .filter(|h| !carried_hashes.contains(h) && live.contains(h))
                    .copied()
                    .collect();
                if !stale.is_empty() {
                    let details: Vec<String> = stale
                        .iter()
                        .map(|h| {
                            let lbas = self.lbamap.lbas_for_hash(h);
                            let in_pending = self.pending_entries.iter().any(|e| &e.hash == h);
                            let seg = self
                                .extent_index
                                .lookup(h)
                                .map(|loc| {
                                    format!("seg={} off={}", loc.segment_id, loc.body_offset)
                                })
                                .unwrap_or_else(|| "not-in-extent-index".to_string());
                            let old_ulid = old_ulid_by_hash
                                .get(h)
                                .map(|u| u.to_string())
                                .unwrap_or_else(|| "?".to_string());
                            format!(
                                "{}: lbas={:?} in_pending_entries={} {} old_ulid={}",
                                &h.to_hex()[..12],
                                lbas,
                                in_pending,
                                seg,
                                old_ulid,
                            )
                        })
                        .collect();
                    log::warn!(
                        "GC handoff {name}: stale-liveness cancellation — {} hash(es) live \
                         in volume but absent from coordinator output; re-running next tick.\n  {}",
                        stale.len(),
                        details.join("\n  "),
                    );
                    let _ = fs::remove_file(gc_dir.join(name)); // .pending
                    if gc_seg_path.try_exists()? {
                        let _ = fs::remove_file(&gc_seg_path); // body
                    }
                    continue;
                }
            }

            // Second pass: apply extent index updates for carried hashes.
            // Safe to mutate the index now — stale_liveness was clear above.
            //
            // `index_mutated` tracks whether this pass actually changed the
            // extent index.  For `.applied` handoffs (restart-recovery path)
            // the idle tick has usually already applied the same mutations, so
            // `still_at_old` will be false for every entry and nothing changes.
            // We only count an `.applied` handoff toward the return value when
            // mutations did happen, so the coordinator can distinguish genuine
            // restart recovery from a redundant steady-state re-check.
            let mut index_mutated = false;
            if let Some((body_section_start, ref entries)) = segment_index {
                for (i, e) in entries.iter().enumerate() {
                    if e.kind == EntryKind::DedupRef {
                        continue;
                    }
                    // Only update if the extent index still points at the old
                    // segment that GC consumed.  If a newer write has
                    // superseded it, the current entry is more recent and must
                    // not be overwritten.
                    let still_at_old = match (
                        self.extent_index.lookup(&e.hash),
                        old_ulid_by_hash.get(&e.hash),
                    ) {
                        (Some(loc), Some(old_ulid)) => loc.segment_id == *old_ulid,
                        _ => false,
                    };
                    if !still_at_old {
                        continue;
                    }
                    Arc::make_mut(&mut self.extent_index).insert(
                        e.hash,
                        extentindex::ExtentLocation {
                            segment_id: new_ulid,
                            body_offset: e.stored_offset,
                            body_length: e.stored_length,
                            compressed: e.compressed,
                            entry_idx: Some(i as u32),
                            body_section_start,
                        },
                    );
                    index_mutated = true;
                }
            }

            // Remove extent index entries for hashes that were in the
            // consumed segments but filtered out of the GC output (LBA-dead
            // extents).  Without this, the extent index would retain a
            // dangling reference to the old segment file which is about to
            // be deleted.
            for (hash, old_ulid) in &old_ulid_by_hash {
                if carried_hashes.contains(hash) {
                    continue;
                }
                // Defense-in-depth: stale_liveness above ensures no live hash
                // reaches this point, but guard here as well.
                if live.contains(hash) {
                    continue;
                }
                if self
                    .extent_index
                    .lookup(hash)
                    .is_some_and(|loc| loc.segment_id == *old_ulid)
                {
                    Arc::make_mut(&mut self.extent_index).remove(hash);
                    index_mutated = true;
                }
            }

            // For .pending handoffs: commit the GC handoff:
            //
            //   1. Delete pending/<old> for each consumed input (best-effort;
            //      these files are normally already gone since consumed segments
            //      were S3-confirmed and thus already promoted).
            //   1b. For tombstone/remove-only handoffs (no GC output body):
            //      delete index/<old>.idx here, since promote_segment will
            //      never run (there is no new body to promote).  For regular
            //      repacks, idx deletion is deferred to the promote_segment IPC
            //      handler (after the coordinator confirms S3 upload).
            //   2. Rename .pending → .applied to signal the coordinator.
            //
            //   For regular repacks: index/<new>.idx write and index/<old>.idx
            //   deletion are deferred to the promote_segment IPC handler, which
            //   runs after the coordinator confirms S3 upload of the new segment.
            //   This preserves the invariant: idx present ↔ segment in S3.
            //
            //   cache/<old>.{body,present} are NOT deleted here.  Concurrent
            //   readers may hold a snapshot that still references old_ulid; the
            //   actor loop calls evict_applied_gc_cache() AFTER publishing the
            //   new snapshot, eliminating the read-error window.
            //
            // For .applied handoffs: the above steps already ran; skip them.
            if !is_already_applied {
                // Step 1: clean up any stale pending/ files for consumed segments.
                let pending_dir = self.base_dir.join("pending");
                let mut old_ulids: Vec<Ulid> = old_ulid_by_hash.values().copied().collect();
                old_ulids.extend_from_slice(&dead_ulids);
                old_ulids.sort_unstable();
                old_ulids.dedup();
                for old_ulid in &old_ulids {
                    let _ = fs::remove_file(pending_dir.join(old_ulid.to_string()));
                }

                // Step 1b: tombstone/remove-only — no GC body exists so
                // promote_segment will never run; delete old idx here instead.
                if !segment_exists {
                    let index_dir = self.base_dir.join("index");
                    for old_ulid in &old_ulids {
                        let _ = fs::remove_file(index_dir.join(format!("{}.idx", old_ulid)));
                    }
                }

                // Step 2: rename .pending → .applied.
                let applied_path =
                    gc_dir.join(handoff.with_state(GcHandoffState::Applied).filename());
                fs::rename(gc_dir.join(name), &applied_path)?;
            }

            // Count every handoff that was successfully processed: both normal
            // .pending applications and .applied re-applications on restart.
            // Volume::open already incorporates gc/*.applied during rebuild so
            // the extent index is already correct on the restart path — count
            // the handoff regardless of whether mutations occurred.
            let _ = index_mutated; // tracked above but no longer drives the count
            count += 1;
        }

        Ok(count)
    }

    /// Evict old cache files for completed GC handoffs.
    ///
    /// Called by the actor AFTER publishing the new snapshot (which redirects
    /// all new reads to the GC output segment).  Scans gc/*.applied files and
    /// deletes cache/<old>.{body,present} for each consumed input ULID.
    ///
    /// Safe to call multiple times — file deletions are best-effort and
    /// silently skip already-absent files.
    pub fn evict_applied_gc_cache(&self) {
        let gc_dir = self.base_dir.join("gc");
        let cache_dir = self.base_dir.join("cache");
        let Ok(entries) = fs::read_dir(&gc_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            let Some(handoff) = crate::gc::GcHandoff::from_filename(name_str) else {
                continue;
            };
            if handoff.state != crate::gc::GcHandoffState::Applied {
                continue;
            }
            let Ok(content) = fs::read_to_string(entry.path()) else {
                continue;
            };
            let mut old_ulids: Vec<Ulid> = Vec::new();
            for line in content.lines() {
                match crate::gc::HandoffLine::parse(line) {
                    Some(crate::gc::HandoffLine::Repack { old_ulid, .. })
                    | Some(crate::gc::HandoffLine::Remove { old_ulid, .. })
                    | Some(crate::gc::HandoffLine::Dead { old_ulid }) => {
                        old_ulids.push(old_ulid);
                    }
                    None => {}
                }
            }
            old_ulids.sort_unstable();
            old_ulids.dedup();
            for old_ulid in old_ulids {
                let s = old_ulid.to_string();
                let _ = fs::remove_file(cache_dir.join(format!("{s}.body")));
                let _ = fs::remove_file(cache_dir.join(format!("{s}.present")));
            }
        }
    }

    /// Promote a segment to the local cache after confirmed S3 upload.
    ///
    /// Called in response to the coordinator's `promote <ulid>` IPC, which is
    /// sent only after a confirmed S3 upload.
    ///
    /// Writes `index/<ulid>.idx` first (restoring the invariant that idx presence
    /// ↔ segment confirmed in S3), then `cache/<ulid>.body` and
    /// `cache/<ulid>.present`.
    ///
    /// **Drain path** (`pending/<ulid>` exists): also deletes `pending/<ulid>`.
    /// The coordinator never deletes `pending/` directly.
    ///
    /// **GC path** (`gc/<ulid>` exists): also deletes `index/<old>.idx` for each
    /// segment consumed by the GC handoff (read from `gc/<ulid>.applied`).  This
    /// happens after writing the new idx so there is never a window where no idx
    /// covers the affected LBAs.  The coordinator deletes `gc/<ulid>` itself after
    /// receiving `ok`.
    ///
    /// Idempotent: if `cache/<ulid>.body` already exists the function returns
    /// `Ok(())` without re-writing.
    pub fn promote_segment(&mut self, ulid: Ulid) -> io::Result<()> {
        let ulid_str = ulid.to_string();
        let cache_dir = self.base_dir.join("cache");
        let body_path = cache_dir.join(format!("{ulid_str}.body"));
        let present_path = cache_dir.join(format!("{ulid_str}.present"));

        // Determine the source: pending/ (drain) or gc/ (GC).
        let pending_path = self.base_dir.join("pending").join(&ulid_str);
        let gc_path = self.base_dir.join("gc").join(&ulid_str);
        let (src_path, is_drain) = if pending_path.try_exists()? {
            (pending_path.clone(), true)
        } else if gc_path.try_exists()? {
            (gc_path, false)
        } else if body_path.try_exists()? {
            // Already promoted on a prior attempt; idempotent success.
            return Ok(());
        } else {
            return Err(io::Error::other(format!(
                "promote {ulid_str}: segment not found in pending/ or gc/"
            )));
        };

        // For drain path: prefer the .materialized sidecar (fat variant) over
        // the original pending segment. The .materialized file matches what was
        // uploaded to S3 — its index has MaterializedRef entries (not thin
        // DedupRef), so the .idx file and cache body are consistent with S3.
        let mat_path = self
            .base_dir
            .join("pending")
            .join(format!("{ulid_str}.materialized"));
        let promote_src = if is_drain && mat_path.try_exists()? {
            &mat_path
        } else {
            &src_path
        };

        // Write index/<ulid>.idx now — after confirmed S3 upload — so that
        // idx presence ↔ segment confirmed in S3 (restored invariant).
        // This must happen before deleting old idx files (GC path below) so
        // there is no window where no idx covers the affected LBAs.
        let index_dir = self.base_dir.join("index");
        fs::create_dir_all(&index_dir)?;
        let idx_path = index_dir.join(format!("{ulid_str}.idx"));
        segment::extract_idx(promote_src, &idx_path)?;

        fs::create_dir_all(&cache_dir)?;
        segment::promote_to_cache(promote_src, &body_path, &present_path)?;

        // When promoting from .materialized, the cache body has different
        // offsets than the original pending segment (index section grew:
        // DedupRef 45 bytes → MaterializedRef 57 bytes). Update the extent
        // index so reads find body bytes at the correct positions. The actor
        // serializes all requests — no reads can run during this update.
        //
        // Future: if we switch to promoting the thin version to cache (for
        // local storage savings), this update would use the thin .idx offsets
        // instead — and a separate fat .idx would be written for GC.
        if is_drain && promote_src == &mat_path {
            let (new_bss, entries) =
                segment::read_and_verify_segment_index(promote_src, &self.verifying_key)?;
            for entry in &entries {
                match entry.kind {
                    EntryKind::Data | EntryKind::MaterializedRef => {}
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Inline => continue,
                }
                let points_to_this_segment = self
                    .extent_index
                    .lookup(&entry.hash)
                    .is_some_and(|loc| loc.segment_id == ulid);
                if points_to_this_segment {
                    Arc::make_mut(&mut self.extent_index).insert(
                        entry.hash,
                        extentindex::ExtentLocation {
                            segment_id: ulid,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            entry_idx: None,
                            body_section_start: new_bss,
                        },
                    );
                }
            }
        }

        if is_drain {
            // Clean up .materialized sidecar if materialise_segment produced one.
            let _ = fs::remove_file(&mat_path);
            fs::remove_file(&pending_path)?;
        } else {
            // GC path: delete index/<old>.idx for each segment consumed by this
            // handoff.  Parse the .applied file to find the old ULIDs.
            let applied_path = self.base_dir.join("gc").join(format!("{ulid_str}.applied"));
            if let Ok(content) = fs::read_to_string(&applied_path) {
                let mut old_ulids: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for line in content.lines() {
                    match crate::gc::HandoffLine::parse(line) {
                        Some(crate::gc::HandoffLine::Repack { old_ulid, .. })
                        | Some(crate::gc::HandoffLine::Remove { old_ulid, .. })
                        | Some(crate::gc::HandoffLine::Dead { old_ulid }) => {
                            old_ulids.insert(old_ulid.to_string());
                        }
                        None => {}
                    }
                }
                for old_str in &old_ulids {
                    let _ = fs::remove_file(index_dir.join(format!("{old_str}.idx")));
                }
            }
        }
        Ok(())
    }

    /// Produce `pending/<ulid>.materialized` — a self-contained segment with
    /// every thin DedupRef replaced by a fat MaterializedRef. Called by the
    /// coordinator before reading the segment for S3 upload.
    ///
    /// If the segment already has no thin DedupRef entries, a hard link to
    /// the original is created (zero-cost copy). Otherwise a new fat segment
    /// is written alongside the original.
    ///
    /// The original `pending/<ulid>` is never modified — the extent index
    /// stays valid and reads continue to use the original file.
    ///
    /// Idempotent: if `.materialized` already exists, returns Ok immediately.
    /// `promote_segment` cleans up `.materialized` alongside the original.
    pub fn materialise_segment(&self, ulid: Ulid) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};

        let ulid_str = ulid.to_string();
        let pending_dir = self.base_dir.join("pending");
        let seg_path = pending_dir.join(&ulid_str);
        let mat_path = pending_dir.join(format!("{ulid_str}.materialized"));

        if mat_path.try_exists()? {
            return Ok(());
        }

        let (body_section_start, entries) =
            segment::read_and_verify_segment_index(&seg_path, &self.verifying_key)?;

        // Fast path: no thin refs — hard link the original.
        let has_thin = entries.iter().any(|e| e.kind == EntryKind::DedupRef);
        if !has_thin {
            fs::hard_link(&seg_path, &mat_path)?;
            return Ok(());
        }

        // Build new entries. For each entry:
        // - DATA: read body bytes from the original segment file
        // - DedupRef (thin): read body from canonical segment, emit MaterializedRef
        // - Zero: pass through as-is
        // All body reads happen here; no separate read_extent_bodies call
        // (MaterializedRef entries have stored_offset=0 which is invalid for
        // the original file — they must not be passed to read_extent_bodies).
        let mut seg_file = fs::File::open(&seg_path)?;
        let mut new_entries: Vec<segment::SegmentEntry> = Vec::with_capacity(entries.len());
        for entry in entries {
            match entry.kind {
                EntryKind::Data | EntryKind::MaterializedRef => {
                    // Read body from the original segment file.
                    let mut body = vec![0u8; entry.stored_length as usize];
                    seg_file.seek(SeekFrom::Start(body_section_start + entry.stored_offset))?;
                    seg_file.read_exact(&mut body)?;
                    let flags = if entry.compressed {
                        segment::SegmentFlags::COMPRESSED
                    } else {
                        segment::SegmentFlags::empty()
                    };
                    new_entries.push(segment::SegmentEntry::new_data(
                        entry.hash,
                        entry.start_lba,
                        entry.lba_length,
                        flags,
                        body,
                    ));
                }
                EntryKind::DedupRef => {
                    // Thin → look up canonical location in extent index.
                    let loc = self.extent_index.lookup(&entry.hash).ok_or_else(|| {
                        io::Error::other(format!(
                            "materialise {ulid_str}: hash {} not in extent index",
                            entry.hash.to_hex()
                        ))
                    })?;
                    let canonical_path = self.find_segment_file(
                        loc.segment_id,
                        loc.body_section_start,
                        loc.entry_idx,
                    )?;
                    let is_body = canonical_path.extension().is_some_and(|e| e == "body");
                    let file_offset = if is_body {
                        loc.body_offset
                    } else {
                        loc.body_section_start + loc.body_offset
                    };
                    let mut f = fs::File::open(&canonical_path)?;
                    f.seek(SeekFrom::Start(file_offset))?;
                    let mut data = vec![0u8; loc.body_length as usize];
                    f.read_exact(&mut data)?;

                    let flags = if loc.compressed {
                        segment::SegmentFlags::COMPRESSED
                    } else {
                        segment::SegmentFlags::empty()
                    };
                    new_entries.push(segment::SegmentEntry::new_materialized_ref(
                        entry.hash,
                        entry.start_lba,
                        entry.lba_length,
                        flags,
                        data,
                    ));
                }
                EntryKind::Zero => {
                    new_entries.push(segment::SegmentEntry::new_zero(
                        entry.start_lba,
                        entry.lba_length,
                    ));
                }
                EntryKind::Inline => {
                    new_entries.push(entry);
                }
            }
        }

        {
            let (mut fat, mut data, mut zero) = (0usize, 0usize, 0usize);
            for e in &new_entries {
                match e.kind {
                    EntryKind::MaterializedRef => fat += 1,
                    EntryKind::Data => data += 1,
                    EntryKind::Zero => zero += 1,
                    EntryKind::DedupRef | EntryKind::Inline => {}
                }
            }
            log::info!(
                "materialise {ulid_str}: {fat} thin→fat, \
                 {data} data, {zero} zero ({} entries total)",
                new_entries.len()
            );
        }

        // Write to .tmp then rename to .materialized (atomic).
        let tmp_path = pending_dir.join(format!("{ulid_str}.tmp"));
        segment::write_segment(&tmp_path, &mut new_entries, self.signer.as_ref())?;
        fs::rename(&tmp_path, &mat_path)?;
        segment::fsync_dir(&mat_path)?;
        Ok(())
    }

    /// Flush the current WAL to a segment in this node's `pending/`, update
    /// the extent index, and clear `pending_entries`. The WAL file is deleted.
    ///
    /// If `pending_entries` is empty (nothing written since last flush), the
    /// WAL file is deleted directly without writing a segment.
    ///
    /// Does NOT open a new WAL — the caller is responsible for that.
    fn flush_wal_to_pending(&mut self) -> io::Result<()> {
        self.flush_wal_to_pending_as(self.wal_ulid)
    }

    /// Like `flush_wal_to_pending`, but names the output segment `segment_ulid`
    /// rather than the WAL's own ULID.
    ///
    /// Used by `gc_checkpoint` to give the flushed WAL segment a ULID that has
    /// been pre-minted above the GC output ULIDs, so that the pending segment
    /// sorts correctly above GC outputs on crash-recovery rebuild.
    ///
    /// The WAL file itself retains its original name (the WAL ULID) — only the
    /// output segment in `pending/` receives `segment_ulid`.
    fn flush_wal_to_pending_as(&mut self, segment_ulid: Ulid) -> io::Result<()> {
        self.wal.fsync()?;
        if self.pending_entries.is_empty() {
            fs::remove_file(&self.wal_path)?;
            return Ok(());
        }
        self.has_new_segments = true;
        self.last_segment_ulid = Some(segment_ulid);
        let body_section_start = segment::promote(
            &self.wal_path,
            segment_ulid,
            &self.base_dir.join("pending"),
            &mut self.pending_entries,
            self.signer.as_ref(),
        )?;
        // Update the extent index: replace temporary WAL offsets with
        // body-relative offsets into the committed segment file.
        // Thin DedupRef entries have no body in this segment — the extent
        // index already points to the canonical segment. Zero extents are
        // not indexed.
        for entry in &self.pending_entries {
            match entry.kind {
                EntryKind::Data | EntryKind::MaterializedRef => {}
                EntryKind::DedupRef | EntryKind::Zero | EntryKind::Inline => continue,
            }
            Arc::make_mut(&mut self.extent_index).insert(
                entry.hash,
                extentindex::ExtentLocation {
                    segment_id: segment_ulid,
                    body_offset: entry.stored_offset,
                    body_length: entry.stored_length,
                    compressed: entry.compressed,
                    entry_idx: None,
                    body_section_start,
                },
            );
        }
        {
            let (mut data, mut refs, mut zero) = (0usize, 0usize, 0usize);
            for e in &self.pending_entries {
                match e.kind {
                    EntryKind::Data => data += 1,
                    EntryKind::DedupRef => refs += 1,
                    EntryKind::Zero => zero += 1,
                    EntryKind::MaterializedRef | EntryKind::Inline => {}
                }
            }
            log::info!(
                "flush {segment_ulid}: {data} data, {refs} dedup-ref, \
                 {zero} zero ({} entries total)",
                self.pending_entries.len()
            );
        }
        self.pending_entries.clear();
        // index/<ulid>.idx is written later by the promote_segment IPC handler,
        // after the coordinator confirms S3 upload.  Until then pending/<ulid>
        // is the authoritative body source for both reads and crash recovery.
        // Evict the promoted WAL from the file handle cache.  After promotion
        // the body offsets in the extent index point into the new segment file;
        // any cached fd for this ULID would use the old WAL byte layout.
        // The cache key is the WAL's original ULID (the file that was deleted),
        // not segment_ulid — the cache is keyed by the path that was open.
        let mut cache = self.file_cache.borrow_mut();
        if cache.as_ref().map(|(id, _, _)| *id) == Some(self.wal_ulid) {
            *cache = None;
        }
        Ok(())
    }

    /// Promote the current WAL to a pending segment, then open a fresh WAL.
    fn promote(&mut self) -> io::Result<()> {
        self.flush_wal_to_pending()?;
        // Create the fresh WAL. If this fails the segment is safe in pending/
        // and will be found on the next startup rebuild.
        let (wal, wal_ulid, wal_path, _) =
            create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        Ok(())
    }

    /// Checkpoint the fork at the current point in the segment sequence.
    ///
    /// Flushes the WAL to a segment in `pending/`, then writes a
    /// `snapshots/<ulid>` marker file. The fork stays live and continues
    /// writing in the same directory — no directory structure changes occur.
    ///
    /// If no new data has been committed since the latest existing snapshot
    /// (nothing in `pending/` or `index/` sorts after it), the existing
    /// snapshot ULID is returned without writing a new marker.
    ///
    /// Returns the snapshot ULID.
    pub fn snapshot(&mut self) -> io::Result<Ulid> {
        // Flush WAL to pending/ first so the snapshot marker sorts after it.
        self.flush_wal_to_pending()?;

        // If no new segments have been committed since the last snapshot, reuse
        // the existing snapshot ULID rather than writing a new marker.
        if !self.has_new_segments
            && let Some(latest_str) = latest_snapshot(&self.base_dir)?
        {
            let (wal, wal_ulid, wal_path, pending_entries) =
                create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
            self.wal = wal;
            self.wal_ulid = wal_ulid;
            self.wal_path = wal_path;
            self.pending_entries = pending_entries;
            return Ok(latest_str);
        }

        // Write a new snapshot marker, reusing the last segment's ULID so the
        // branch point is self-describing. Falls back to a fresh ULID only when
        // no segments exist (e.g. first snapshot on an empty fork).
        let snap_ulid = self.last_segment_ulid.unwrap_or_else(|| self.mint.next());
        let snap_ulid_str = snap_ulid.to_string();
        let snapshots_dir = self.base_dir.join("snapshots");
        fs::create_dir_all(&snapshots_dir)?;
        fs::write(snapshots_dir.join(&snap_ulid_str), "")?;
        self.has_new_segments = false;

        // Open a fresh WAL to continue writing.
        let (wal, wal_ulid, wal_path, pending_entries) =
            create_fresh_wal(&self.base_dir.join("wal"), self.mint.next())?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        self.pending_entries = pending_entries;

        Ok(snap_ulid)
    }

    /// Locate the segment body file for `segment_id` within this fork's
    /// ancestry chain.
    ///
    /// Search order:
    ///   1. Current fork: `wal/`, `pending/`, `gc/*.applied`, `cache/<id>.body`
    ///   2. Ancestor forks (newest first): `pending/`, `gc/*.applied`, `cache/<id>.body`
    ///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
    ///
    /// For full segment files (`wal/`, `pending/`, `gc/*.applied`), body reads use
    /// absolute file offsets (`ExtentLocation.body_offset`). For cached body
    /// files (`cache/<id>.body`), the file IS the body section, so reads use
    /// body-relative offsets — consistent with how `extentindex::rebuild` stores
    /// offsets for cached entries.
    fn find_segment_file(
        &self,
        segment_id: Ulid,
        body_section_start: u64,
        entry_idx: Option<u32>,
    ) -> io::Result<PathBuf> {
        find_segment_in_dirs(
            segment_id,
            &self.base_dir,
            &self.ancestor_layers,
            self.fetcher.as_ref(),
            body_section_start,
            entry_idx,
        )
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn ancestor_count(&self) -> usize {
        self.ancestor_layers.len()
    }

    pub fn lbamap_len(&self) -> usize {
        self.lbamap.len()
    }

    /// Attach a `SegmentFetcher` for demand-fetch on segment cache miss.
    ///
    /// Once set, `find_segment_file` will call the fetcher after all local
    /// directories are checked, caching the result in `cache/`.
    pub fn set_fetcher(&mut self, fetcher: BoxFetcher) {
        self.fetcher = Some(fetcher);
    }

    /// Return all fork directories in the ancestry chain, oldest-first,
    /// with the current fork last.
    ///
    /// Used by callers building a `SegmentFetcher` that needs to know which
    /// forks to search on a cache miss.
    pub fn fork_dirs(&self) -> Vec<PathBuf> {
        self.ancestor_layers
            .iter()
            .map(|l| l.dir.clone())
            .chain(std::iter::once(self.base_dir.clone()))
            .collect()
    }

    /// Return the current LBA map and extent index as shared references.
    ///
    /// Called by `VolumeActor` after every mutation to publish a new `ReadSnapshot`.
    /// The cost is two `Arc::clone` calls — O(1) unless a snapshot reader is still
    /// holding the previous version, in which case `Arc::make_mut` in the next
    /// mutation triggers a copy-on-write clone.
    pub fn snapshot_maps(&self) -> (Arc<lbamap::LbaMap>, Arc<extentindex::ExtentIndex>) {
        (Arc::clone(&self.lbamap), Arc::clone(&self.extent_index))
    }

    /// Ancestor layers for this fork, oldest-first.
    pub fn ancestor_layers(&self) -> &[AncestorLayer] {
        &self.ancestor_layers
    }

    /// The attached demand-fetch fetcher, if any.
    pub fn fetcher(&self) -> Option<&BoxFetcher> {
        self.fetcher.as_ref()
    }

    /// Flush the current WAL to a pending segment if it contains any entries.
    /// No-op if the WAL is empty. Called by the idle-flush path in the NBD server.
    pub fn flush_wal(&mut self) -> io::Result<()> {
        if self.pending_entries.is_empty() {
            return Ok(());
        }
        self.promote()
    }

    /// True if the WAL has reached the 32 MiB soft cap and should be promoted.
    ///
    /// The actor calls this after every write reply and promotes if true.
    /// The check is separated from `write()` so that writes are always fast
    /// (WAL append only) and the promotion cost is never borne by the write caller.
    pub fn needs_promote(&self) -> bool {
        self.wal.size() >= FLUSH_THRESHOLD
    }

    pub fn promote_for_test(&mut self) -> io::Result<()> {
        self.promote()
    }
}

// --- helpers ---

/// Read `lba_count` 4KB blocks starting at `lba` from the given LBA map and extent index.
///
/// Unwritten blocks are returned as zeros. Written blocks are fetched extent-by-extent
/// using `find_segment` to locate each segment file, with the last-opened file handle
/// cached in `file_cache` to amortize `open` syscalls across sequential reads.
pub(crate) fn read_extents(
    lba: u64,
    lba_count: u32,
    lbamap: &lbamap::LbaMap,
    extent_index: &extentindex::ExtentIndex,
    file_cache: &RefCell<Option<(Ulid, bool, fs::File)>>,
    find_segment: impl Fn(Ulid, u64, Option<u32>) -> io::Result<PathBuf>,
) -> io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut out = vec![0u8; lba_count as usize * 4096];
    for er in lbamap.extents_in_range(lba, lba + lba_count as u64) {
        // Zero extents: output buffer is already zeroed; nothing to fetch.
        if er.hash == ZERO_HASH {
            continue;
        }

        // Extract owned copies so the borrow of extent_index ends before
        // we mutate file_cache.
        let (segment_id, body_offset, body_length, compressed, body_section_start, entry_idx) = {
            let Some(loc) = extent_index.lookup(&er.hash) else {
                continue; // hash not indexed — treat as unwritten
            };
            (
                loc.segment_id,
                loc.body_offset,
                loc.body_length,
                loc.compressed,
                loc.body_section_start,
                loc.entry_idx,
            )
        };

        // Reuse the cached file handle if it is for the same segment.
        // For cached entries (entry_idx.is_some()), always call find_segment to
        // check the .present bitset — the .body file may exist but the specific
        // entry may not yet be fetched.
        let mut cache = file_cache.borrow_mut();
        let need_find =
            cache.as_ref().map(|(id, _, _)| *id) != Some(segment_id) || entry_idx.is_some();
        if need_find {
            let path = find_segment(segment_id, body_section_start, entry_idx)?;
            // .body files store body bytes starting at offset 0 (body-relative);
            // full segment files store them starting at body_section_start.
            let is_body = path.extension().is_some_and(|e| e == "body");
            *cache = Some((segment_id, is_body, fs::File::open(path)?));
        }
        let (_, is_body, f) = cache.as_mut().expect("cache was just assigned Some above");

        // body_offset is always body-relative (= stored_offset from the segment index).
        // For full segment files we must add body_section_start to get the file offset.
        let file_body_offset = if *is_body {
            body_offset
        } else {
            body_section_start + body_offset
        };

        let block_count = (er.range_end - er.range_start) as usize;
        let out_start = (er.range_start - lba) as usize * 4096;
        let out_slice = &mut out[out_start..out_start + block_count * 4096];

        if compressed {
            f.seek(SeekFrom::Start(file_body_offset))?;
            let mut compressed_buf = vec![0u8; body_length as usize];
            f.read_exact(&mut compressed_buf)?;
            let decompressed =
                lz4_flex::decompress_size_prepended(&compressed_buf).map_err(|e| {
                    let (cached_id, is_body_ref, _) = cache.as_ref().expect("cache assigned above");
                    log::error!(
                        "lz4 decompression failed: lba={} segment={} cached_id={} is_body={} \
                     bss={} body_offset={} body_length={} entry_idx={:?} \
                     file_body_offset={} first_bytes={:?} err={}",
                        lba,
                        segment_id,
                        cached_id,
                        is_body_ref,
                        body_section_start,
                        body_offset,
                        body_length,
                        entry_idx,
                        file_body_offset,
                        &compressed_buf[..compressed_buf.len().min(16)],
                        e,
                    );
                    io::Error::other(e)
                })?;
            let src_start = er.payload_block_offset as usize * 4096;
            let src_end = src_start + block_count * 4096;
            let src_slice = decompressed.get(src_start..src_end).ok_or_else(|| {
                io::Error::other("corrupt segment: decompressed payload too short")
            })?;
            out_slice.copy_from_slice(src_slice);
        } else {
            f.seek(SeekFrom::Start(
                file_body_offset + er.payload_block_offset as u64 * 4096,
            ))?;
            if let Err(e) = f.read_exact(out_slice) {
                let file_size = f.metadata().map(|m| m.len()).unwrap_or(0);
                let (seg_id, is_body_ref, _) = cache.as_ref().expect("cache assigned above");
                log::error!(
                    "read_extents failed: lba={} segment={} is_body={} \
                     bss={} body_offset={} body_length={} payload_block_offset={} \
                     file_body_offset={} read_len={} file_size={} err={}",
                    lba,
                    seg_id,
                    is_body_ref,
                    body_section_start,
                    body_offset,
                    body_length,
                    er.payload_block_offset,
                    file_body_offset,
                    out_slice.len(),
                    file_size,
                    e,
                );
                return Err(e);
            }
        }
    }
    Ok(out)
}

/// Search for a segment file across the fork directory tree.
///
/// Search order:
///   1. Current fork: `wal/`, `pending/`, `gc/*.applied`, `cache/<id>.body`
///   2. Ancestor forks (newest-first): `pending/`, `gc/*.applied`, `cache/<id>.body`
///   3. Demand-fetch via fetcher (writes three-file format to `cache/`)
///
/// When `entry_idx` is `Some`, a `cache/<id>.body` hit is only accepted if
/// the corresponding bit in `cache/<id>.present` is set — otherwise the entry
/// is not yet locally available and we fall through to the fetcher.
///
/// `.idx` files live in `index/` (coordinator-written, permanent).
/// `.body` and `.present` files live in `cache/` (volume-managed read cache).
///
/// Extracted from `Volume::find_segment_file` so that `VolumeHandle` can serve
/// reads directly from a `ReadSnapshot` without going through the actor channel.
pub(crate) fn find_segment_in_dirs(
    segment_id: Ulid,
    base_dir: &Path,
    ancestor_layers: &[AncestorLayer],
    fetcher: Option<&BoxFetcher>,
    body_section_start: u64,
    entry_idx: Option<u32>,
) -> io::Result<PathBuf> {
    let sid = segment_id.to_string();
    for subdir in ["wal", "pending"] {
        let path = base_dir.join(subdir).join(&sid);
        if path.exists() {
            return Ok(path);
        }
    }
    // During the .applied GC handoff window the new segment body lives in gc/
    // (volume-signed, awaiting coordinator upload to S3).  The extent index
    // already points at this segment_id, so reads must be able to find it here.
    // The .applied marker distinguishes a volume-signed body from a coordinator-
    // staged body (.pending) which is not yet safe to read.
    let gc_body = base_dir.join("gc").join(&sid);
    if gc_body.exists() && base_dir.join("gc").join(format!("{sid}.applied")).exists() {
        return Ok(gc_body);
    }
    let cache_body = base_dir.join("cache").join(format!("{sid}.body"));
    if cache_body.exists() {
        let entry_present = entry_idx.is_none_or(|idx| {
            let present_path = base_dir.join("cache").join(format!("{sid}.present"));
            segment::check_present_bit(&present_path, idx).unwrap_or(false)
        });
        if entry_present {
            return Ok(cache_body);
        }
        // Entry not yet fetched — fall through to fetcher below.
    }
    for layer in ancestor_layers.iter().rev() {
        let path = layer.dir.join("pending").join(&sid);
        if path.exists() {
            return Ok(path);
        }
        let cache_body = layer.dir.join("cache").join(format!("{sid}.body"));
        if cache_body.exists() {
            let entry_present = entry_idx.is_none_or(|idx| {
                let present_path = layer.dir.join("cache").join(format!("{sid}.present"));
                segment::check_present_bit(&present_path, idx).unwrap_or(false)
            });
            if entry_present {
                return Ok(cache_body);
            }
        }
    }
    if let Some(fetcher) = fetcher {
        let index_dir = base_dir.join("index");
        let body_dir = base_dir.join("cache");
        if let Some(idx) = entry_idx {
            fetcher.fetch_extent(
                segment_id,
                &index_dir,
                &body_dir,
                &segment::ExtentFetch {
                    body_section_start,
                    body_offset: 0,
                    body_length: 0,
                    entry_idx: idx,
                },
            )?;
        } else {
            fetcher.fetch(segment_id, &index_dir, &body_dir)?;
        }
        return Ok(base_dir.join("cache").join(format!("{sid}.body")));
    }
    Err(io::Error::other(format!("segment not found: {sid}")))
}

/// Acquire an exclusive non-blocking flock on `<dir>/volume.lock`.
///
/// Creates the lock file if it does not exist. Returns the open `File` — the
/// lock is held for as long as this handle is open and released when dropped.
/// Returns an error immediately if the lock is already held by another process.
fn acquire_lock(dir: &Path) -> io::Result<nix::fcntl::Flock<fs::File>> {
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join("volume.lock"))?;
    nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock)
        .map_err(|(_, e)| io::Error::from(e))
}

/// A read-only view of a fork. Used for readonly NBD serving (no WAL, no write lock).
/// Reads work identically to `Volume`; writes and fsyncs are not supported.
pub struct ReadonlyVolume {
    base_dir: PathBuf,
    ancestor_layers: Vec<AncestorLayer>,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    file_cache: RefCell<Option<(Ulid, bool, fs::File)>>,
    fetcher: Option<BoxFetcher>,
}

impl ReadonlyVolume {
    /// Open a volume directory for read-only access.
    ///
    /// Does not create `wal/`, does not acquire an exclusive lock, and does not
    /// replay the WAL. WAL records from an active writer on the same volume will
    /// not be visible. Intended for the `--readonly` NBD serve path.
    pub fn open(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Self> {
        let (ancestor_layers, lbamap, extent_index) = open_read_state(fork_dir, by_id_dir)?;
        Ok(Self {
            base_dir: fork_dir.to_owned(),
            ancestor_layers,
            lbamap,
            extent_index,
            file_cache: RefCell::new(None),
            fetcher: None,
        })
    }

    /// Read `lba_count` 4KB blocks starting at `start_lba`.
    /// Unwritten blocks are returned as zeros.
    pub fn read(&self, start_lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        read_extents(
            start_lba,
            lba_count,
            &self.lbamap,
            &self.extent_index,
            &self.file_cache,
            |id, bss, idx| self.find_segment_file(id, bss, idx),
        )
    }

    fn find_segment_file(
        &self,
        segment_id: Ulid,
        body_section_start: u64,
        entry_idx: Option<u32>,
    ) -> io::Result<PathBuf> {
        find_segment_in_dirs(
            segment_id,
            &self.base_dir,
            &self.ancestor_layers,
            self.fetcher.as_ref(),
            body_section_start,
            entry_idx,
        )
    }

    /// Attach a `SegmentFetcher` for demand-fetch on segment cache miss.
    pub fn set_fetcher(&mut self, fetcher: BoxFetcher) {
        self.fetcher = Some(fetcher);
    }

    /// Return all fork directories in the ancestry chain, oldest-first,
    /// with the current fork last.
    pub fn fork_dirs(&self) -> Vec<PathBuf> {
        self.ancestor_layers
            .iter()
            .map(|l| l.dir.clone())
            .chain(std::iter::once(self.base_dir.clone()))
            .collect()
    }
}

/// Walk the fork ancestry chain and return ancestor layers, oldest-first.
/// Public so that `ls.rs` and other read-only tools can build the rebuild chain.
///
/// Walk the ancestry chain and rebuild the LBA map and extent index.
///
/// This is the common open-time setup shared by `Volume::open` and
/// `ReadonlyVolume::open`.  Returns the ancestor layers (oldest-first), the
/// rebuilt LBA map, and the rebuilt extent index.
fn open_read_state(
    fork_dir: &Path,
    by_id_dir: &Path,
) -> io::Result<(Vec<AncestorLayer>, lbamap::LbaMap, extentindex::ExtentIndex)> {
    let ancestor_layers = walk_ancestors(fork_dir, by_id_dir)?;
    let rebuild_chain: Vec<(PathBuf, Option<String>)> = ancestor_layers
        .iter()
        .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
        .chain(std::iter::once((fork_dir.to_owned(), None)))
        .collect();
    let lbamap = lbamap::rebuild_segments(&rebuild_chain)?;
    let extent_index = extentindex::rebuild(&rebuild_chain)?;
    Ok((ancestor_layers, lbamap, extent_index))
}

/// Each layer holds the ancestor fork directory and the branch-point ULID.
/// Segments with ULID > `branch_ulid` in that ancestor fork were written after
/// the branch and are excluded when rebuilding the LBA map.
///
/// A fork with no `volume.parent` file is the root fork; returns an empty vec.
/// The `volume.parent` file format is `<parent-ulid>/snapshots/<branch-ulid>`, where
/// `parent-ulid` is the ULID-named directory under `by_id_dir`. Both components
/// are validated as ULIDs to prevent path traversal.
pub fn walk_ancestors(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Vec<AncestorLayer>> {
    let origin_path = fork_dir.join("volume.parent");
    let content = match fs::read_to_string(&origin_path) {
        Ok(s) => s,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let origin = content.trim();
    let (parent_ulid_str, branch_ulid_str) =
        origin.rsplit_once("/snapshots/").ok_or_else(|| {
            io::Error::other(format!(
                "malformed volume.parent in {}: {origin}",
                fork_dir.display()
            ))
        })?;
    let branch_ulid = Ulid::from_string(branch_ulid_str)
        .map_err(|e| io::Error::other(format!("bad branch ULID in origin: {e}")))?
        .to_string();

    // Validate parent component: must be a valid ULID (no path traversal).
    Ulid::from_string(parent_ulid_str).map_err(|_| {
        io::Error::other(format!(
            "malformed volume.parent in {}: parent '{}' is not a valid ULID",
            fork_dir.display(),
            parent_ulid_str
        ))
    })?;

    let parent_fork_dir = by_id_dir.join(parent_ulid_str);

    // Recurse into the parent's ancestry first (builds oldest-first order).
    let mut ancestors = walk_ancestors(&parent_fork_dir, by_id_dir)?;
    ancestors.push(AncestorLayer {
        dir: parent_fork_dir,
        branch_ulid: Some(branch_ulid),
    });
    Ok(ancestors)
}

/// Return the latest snapshot ULID string for a fork, or `None` if no
/// snapshots exist. Snapshots live as plain files under `fork_dir/snapshots/`.
pub fn latest_snapshot(fork_dir: &Path) -> io::Result<Option<Ulid>> {
    let snapshots_dir = fork_dir.join("snapshots");
    let iter = match fs::read_dir(&snapshots_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let latest = iter
        .filter_map(|e| e.ok())
        .filter_map(|e| Ulid::from_string(e.file_name().to_str()?).ok())
        .max();
    Ok(latest)
}

/// Create a new volume directory, branched from the latest snapshot of the source volume.
///
/// The source volume must have at least one snapshot (written by `snapshot()`).
/// `new_fork_dir` is created with `wal/`, `pending/`, and a `volume.parent`
/// file using the flat format: `<source-ulid>/snapshots/<branch-ulid>`.
/// The source ULID is derived from `source_fork_dir`'s directory name.
///
/// Returns `Ok(())` on success; `new_fork_dir` must not already exist.
pub fn fork_volume(new_fork_dir: &Path, source_fork_dir: &Path) -> io::Result<()> {
    if new_fork_dir.exists() {
        return Err(io::Error::other(format!(
            "fork directory '{}' already exists",
            new_fork_dir.display()
        )));
    }

    // Canonicalize so that symlink paths (e.g. by_name/<name>) resolve to
    // their real by_id/<ulid> directory before we extract the ULID component.
    let source_real = fs::canonicalize(source_fork_dir)?;
    let source_ulid = source_real
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("source fork dir has no name"))?;

    let branch_ulid = latest_snapshot(source_fork_dir)?.ok_or_else(|| {
        io::Error::other(format!(
            "source volume '{}' has no snapshots; run snapshot-volume first",
            source_fork_dir.display()
        ))
    })?;

    fs::create_dir_all(new_fork_dir.join("wal"))?;
    fs::create_dir_all(new_fork_dir.join("pending"))?;
    // volume.parent format: "<source-ulid>/snapshots/<branch-ulid>"
    let origin = format!("{source_ulid}/snapshots/{branch_ulid}");
    segment::write_file_atomic(&new_fork_dir.join("volume.parent"), origin.as_bytes())?;

    // Generate a fresh keypair for the new fork. Every writable volume must have
    // a signing key; the fork gets its own identity independent of its parent.
    crate::signing::generate_keypair(
        new_fork_dir,
        crate::signing::VOLUME_KEY_FILE,
        crate::signing::VOLUME_PUB_FILE,
    )?;

    Ok(())
}

// --- WAL helpers ---

/// Scan an existing WAL, replay its records into `lbamap`, rebuild
/// `pending_entries`, and reopen the WAL for continued appending.
///
/// This is the single WAL scan on startup — it both updates the LBA map
/// (WAL is more recent than any segment) and recovers the pending_entries
/// list needed for the next promotion.
///
/// `writelog::scan` truncates any partial-tail record before returning.
fn recover_wal(
    path: PathBuf,
    lbamap: &mut lbamap::LbaMap,
    extent_index: &mut extentindex::ExtentIndex,
) -> io::Result<(
    writelog::WriteLog,
    Ulid,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let ulid_str = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("bad WAL filename"))?;
    let ulid = Ulid::from_string(ulid_str).map_err(|e| io::Error::other(e.to_string()))?;

    let (records, valid_size) = writelog::scan(&path)?;

    let mut pending_entries = Vec::new();
    for record in records {
        match record {
            writelog::LogRecord::Data {
                hash,
                start_lba,
                lba_length,
                flags,
                body_offset,
                data,
            } => {
                let body_length = data.len() as u32;
                let compressed = flags.contains(writelog::WalFlags::COMPRESSED);
                // Translate WalFlags → SegmentFlags: the two namespaces use different
                // bit values (WalFlags::COMPRESSED = 0x01, SegmentFlags::COMPRESSED = 0x04).
                let seg_flags = if compressed {
                    segment::SegmentFlags::COMPRESSED
                } else {
                    segment::SegmentFlags::empty()
                };
                lbamap.insert(start_lba, lba_length, hash);
                // Temporary WAL offset — updated to segment offset on promotion.
                extent_index.insert(
                    hash,
                    extentindex::ExtentLocation {
                        segment_id: ulid,
                        body_offset,
                        body_length,
                        compressed,
                        entry_idx: None,
                        body_section_start: 0,
                    },
                );
                pending_entries.push(segment::SegmentEntry::new_data(
                    hash, start_lba, lba_length, seg_flags, data,
                ));
            }
            writelog::LogRecord::Ref {
                hash,
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, hash);
                // Thin REF: no body bytes, no extent_index update — the
                // canonical entry is populated from the segment that holds
                // the body (rebuilt from pending/ or segments/).
                pending_entries.push(segment::SegmentEntry::new_dedup_ref(
                    hash, start_lba, lba_length,
                ));
            }
            writelog::LogRecord::Zero {
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, ZERO_HASH);
                pending_entries.push(segment::SegmentEntry::new_zero(start_lba, lba_length));
            }
        }
    }

    let wal = writelog::WriteLog::reopen(&path, valid_size)?;
    Ok((wal, ulid, path, pending_entries))
}

/// Create a new WAL file using the provided `ulid`.
///
/// The caller is responsible for generating a ULID that sorts after all
/// existing segments and WAL files (typically via `Volume::mint`).
fn create_fresh_wal(
    wal_dir: &Path,
    ulid: Ulid,
) -> io::Result<(
    writelog::WriteLog,
    Ulid,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let path = wal_dir.join(ulid.to_string());
    let wal = writelog::WriteLog::create(&path)?;
    Ok((wal, ulid, path, Vec::new()))
}

// --- tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-volume-test-{}-{}", std::process::id(), n));
        p
    }

    /// Simulate coordinator drain: upload all pending segments to S3 (no-op in
    /// tests) then call `promote_segment` on each.  `promote_segment` writes
    /// `index/<ulid>.idx`, copies the body to `cache/`, and deletes `pending/<ulid>`.
    fn simulate_upload(vol: &Volume) {
        let pending_dir = vol.base_dir.join("pending");
        for entry in std::fs::read_dir(&pending_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.ends_with(".tmp") {
                continue;
            }
            let ulid = ulid::Ulid::from_string(&name).unwrap();
            vol.promote_segment(ulid).unwrap();
        }
    }

    /// Generate a keypair and write `volume.key` + `volume.pub` into `dir`.
    ///
    /// Must be called before `Volume::open` in any test that creates a volume.
    fn write_test_keypair(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        crate::signing::generate_keypair(
            dir,
            crate::signing::VOLUME_KEY_FILE,
            crate::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
    }

    /// Create a temp dir and pre-populate it with a test keypair.
    ///
    /// Use in place of `temp_dir()` whenever the dir will be passed directly
    /// to `Volume::open`.
    fn keyed_temp_dir() -> PathBuf {
        let dir = temp_dir();
        write_test_keypair(&dir);
        dir
    }

    #[test]
    fn open_creates_directories() {
        let base = keyed_temp_dir();
        let _ = Volume::open(&base, &base).unwrap();
        assert!(base.join("wal").is_dir());
        assert!(base.join("pending").is_dir());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_is_idempotent() {
        let base = keyed_temp_dir();
        let _ = Volume::open(&base, &base).unwrap();
        // Second open on the same dir should succeed (dirs already exist).
        let _ = Volume::open(&base, &base).unwrap();
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_single_block() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0x42u8; 4096]).unwrap();
        vol.fsync().unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_multi_block_extent() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        // Write 8 LBAs (32 KiB) as a single call.
        vol.write(10, &vec![0xabu8; 8 * 4096]).unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_empty() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let err = vol.write(0, &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_misaligned() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        let err = vol.write(0, &[0u8; 1000]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_sets_needs_promote_after_threshold() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write 33 × 1 MiB of incompressible data to exceed FLUSH_THRESHOLD (32 MiB).
        // Each block uses a unique byte value so entropy is high and compression is skipped.
        let mut block = vec![0u8; 1024 * 1024];
        for i in 0u64..33 {
            // Fill with a pattern that defeats compression: vary every byte.
            let fill = (i & 0xFF) as u8;
            for (j, b) in block.iter_mut().enumerate() {
                *b = fill ^ (j as u8).wrapping_mul(0x6D).wrapping_add(0x4F);
            }
            vol.write(i * 256, &block).unwrap();
        }

        // writes no longer auto-promote; needs_promote() should be true.
        assert!(
            vol.needs_promote(),
            "expected needs_promote() after 33 MiB of writes"
        );

        // Explicit flush_wal() should promote to pending/.
        vol.flush_wal().unwrap();

        // At least one segment should have been promoted to pending/.
        let has_pending = fs::read_dir(base.join("pending"))
            .unwrap()
            .any(|e| e.is_ok());
        assert!(
            has_pending,
            "expected at least one promoted segment in pending/"
        );

        // A fresh WAL should have been created.
        let wal_count = fs::read_dir(base.join("wal"))
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(
            wal_count, 1,
            "expected exactly one WAL file after promotion"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn recovery_rebuilds_lbamap() {
        let base = keyed_temp_dir();

        // Write two blocks, fsync, then drop (simulates clean shutdown before promotion).
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen — lbamap should contain both blocks.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_unwritten_returns_zeros() {
        let base = keyed_temp_dir();
        let vol = Volume::open(&base, &base).unwrap();
        let data = vol.read(0, 4).unwrap();
        assert_eq!(data.len(), 4 * 4096);
        assert!(data.iter().all(|&b| b == 0));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_reads_back_as_zeros() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write real data, then zero it out.
        vol.write(0, &vec![0xabu8; 4096]).unwrap();
        vol.write_zeroes(0, 4).unwrap();

        let result = vol.read(0, 4).unwrap();
        assert_eq!(result.len(), 4 * 4096);
        assert!(result.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_no_data_in_segment() {
        // After write_zeroes + promote, the segment has a zero entry with no body bytes.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write_zeroes(0, 16).unwrap();
        vol.flush_wal().unwrap();

        let seg_path = segment::collect_segment_files(&base.join("pending"))
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one pending segment");

        let (_, entries) = segment::read_segment_index(&seg_path).unwrap();
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind, segment::EntryKind::Zero);
        assert_eq!(e.stored_length, 0);
        assert_eq!(e.start_lba, 0);
        assert_eq!(e.lba_length, 16);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_after_zeroes_overrides() {
        // Data written after write_zeroes should be readable.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write_zeroes(0, 4).unwrap();
        let payload = vec![0x77u8; 4096];
        vol.write(0, &payload).unwrap();

        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_survives_wal_recovery() {
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write_zeroes(5, 8).unwrap();
            vol.fsync().unwrap();
            // Drop without promoting — WAL remains.
        }

        // Reopen: WAL is replayed; zeroed range should read as zeros.
        let vol = Volume::open(&base, &base).unwrap();
        let result = vol.read(5, 8).unwrap();
        assert!(result.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_zeroes_masks_ancestor_data() {
        // An explicit zero in the child masks ancestor data at those LBAs.
        let by_id = temp_dir();
        let ancestor_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&ancestor_dir);

        // Write data in ancestor, promote, snapshot.
        {
            let mut vol = Volume::open(&ancestor_dir, &by_id).unwrap();
            vol.write(0, &vec![0xbbu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        // Fork and zero the LBA in the child.
        fork_volume(&child_dir, &ancestor_dir).unwrap();
        let mut child_vol = Volume::open(&child_dir, &by_id).unwrap();
        child_vol.write_zeroes(0, 1).unwrap();

        let result = child_vol.read(0, 1).unwrap();
        assert!(
            result.iter().all(|&b| b == 0),
            "zero extent should mask ancestor data"
        );

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn read_written_data_same_session() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload = vec![0x42u8; 4096];
        vol.write(5, &payload).unwrap();

        // Written block reads back correctly.
        let result = vol.read(5, 1).unwrap();
        assert_eq!(result, payload);

        // Adjacent unwritten blocks are zero.
        let before = vol.read(4, 1).unwrap();
        assert!(before.iter().all(|&b| b == 0));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_multi_block_extent() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write 4 blocks with distinct fill bytes so we can verify each block.
        let mut payload = Vec::with_capacity(4 * 4096);
        for fill in [0xAAu8, 0xBB, 0xCC, 0xDD] {
            payload.extend_from_slice(&[fill; 4096]);
        }
        vol.write(10, &payload).unwrap();

        let result = vol.read(10, 4).unwrap();
        assert_eq!(result, payload);

        // Reading a sub-range within the extent.
        let mid = vol.read(11, 2).unwrap();
        assert_eq!(mid, payload[4096..3 * 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_after_promote() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload = vec![0x55u8; 4096];
        vol.write(0, &payload).unwrap();
        vol.promote_for_test().unwrap();

        // After promotion, data lives in pending/<ulid>; reads must still work.
        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_after_reopen() {
        let base = keyed_temp_dir();

        let payload = vec![0x77u8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(3, &payload).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen: WAL recovery must restore both the LBA map and extent index.
        let vol = Volume::open(&base, &base).unwrap();
        let result = vol.read(3, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    /// Regression: compressed WAL entries must be promoted with the correct
    /// SegmentFlags::COMPRESSED so reads after recovery+promote work.
    ///
    /// WalFlags::COMPRESSED=0x01; SegmentFlags::COMPRESSED=0x04.
    /// recover_wal must translate between them before calling new_data().
    #[test]
    fn compressed_entry_survives_recover_and_promote() {
        let base = keyed_temp_dir();

        // Write compressible data (zeros compress very well).
        let payload = vec![0u8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &payload).unwrap();
            vol.fsync().unwrap();
            // Drop without promoting — WAL contains the compressed entry.
        }

        // Reopen (recover_wal runs) then promote (writes segment).
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Reopen again and read — must not fail with "failed to fill whole buffer".
        let vol = Volume::open(&base, &base).unwrap();
        let result = vol.read(0, 1).unwrap();
        assert_eq!(result, payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn ulid_is_unique_and_sortable() {
        let u1 = Ulid::new().to_string();
        let u2 = Ulid::new().to_string();
        assert_eq!(u1.len(), 26);
        assert_ne!(u1, u2);
        // ULIDs generated in sequence should sort correctly (same millisecond
        // is not guaranteed, but two different values prove uniqueness).
    }

    #[test]
    fn recovery_after_promotion() {
        // Write enough to trigger a promotion, drop, reopen — the LBA map must
        // be rebuilt from both pending/ segments and the remaining WAL.
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let block = vec![0u8; 1024 * 1024]; // 1 MiB = 256 LBAs
            for i in 0u64..33 {
                vol.write(i * 256, &block).unwrap();
            }
            vol.fsync().unwrap();
        }

        // All 33 extents should survive across the promotion boundary.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 33);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn promotion_after_wal_recovery() {
        // Write to the WAL, drop (simulating a crash), reopen (WAL recovery),
        // promote, then reopen again — verifies that pending_entries is correctly
        // rebuilt from the recovered WAL so the segment contains the pre-crash writes.
        let base = keyed_temp_dir();

        // Phase 1: write two blocks, fsync, drop.
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Phase 2: recover and immediately promote.
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            assert_eq!(vol.lbamap_len(), 2); // confirm recovery
            vol.promote_for_test().unwrap();
        }

        // Phase 3: reopen — both blocks must now come from the pending/ segment.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        // Confirm the promoted segment landed correctly: one file in pending/.
        let pending_count = fs::read_dir(base.join("pending"))
            .unwrap()
            .filter(|e| e.is_ok())
            .count();
        assert_eq!(pending_count, 1, "expected one segment file in pending/");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn wal_deleted_when_pending_segment_exists() {
        // Simulate a crash between the segment rename and the WAL delete:
        // both wal/<ulid> and pending/<ulid> exist. On reopen, the WAL must
        // be silently discarded and data read from the committed segment.
        let base = keyed_temp_dir();

        // Phase 1: write two blocks and promote so a segment lands in pending/.
        let ulid;
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![0xaau8; 4096]).unwrap();
            vol.write(1, &vec![0xbbu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            // Grab the segment ULID (there is exactly one file in pending/).
            let entry = fs::read_dir(base.join("pending"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let filename = entry.file_name();
            ulid = filename.to_string_lossy().into_owned();
        }

        // Simulate the crash: copy the segment back as a WAL file so both exist.
        fs::copy(
            base.join("pending").join(&ulid),
            base.join("wal").join(&ulid),
        )
        .unwrap();

        // Reopen — should delete the stale WAL and load cleanly from the segment.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);
        assert!(
            vol.read(0, 1).unwrap().iter().all(|&b| b == 0xaa),
            "LBA 0 should be 0xaa"
        );
        assert!(
            vol.read(1, 1).unwrap().iter().all(|&b| b == 0xbb),
            "LBA 1 should be 0xbb"
        );
        // The stale WAL file should be gone.
        assert!(
            !base.join("wal").join(&ulid).exists(),
            "stale WAL was not removed"
        );

        fs::remove_dir_all(base).unwrap();
    }

    // --- durability guarantee tests ---
    //
    // These tests make the crash-recovery guarantees from docs/formats.md explicit
    // and executable. They simulate the intermediate filesystem states that can
    // arise from a machine crash at each step of the promotion commit sequence,
    // and verify that Volume::open() recovers correctly in each case.
    //
    // What these tests cannot cover: whether sync_data() / fsync_dir() actually
    // flush to physical media. That requires hardware fault injection (dm-flakey,
    // CrashMonkey, etc.) and is out of scope for a unit test suite.

    #[test]
    fn recovery_reads_data_after_promotion_and_reopen() {
        // Guarantee: after flush_wal() completes (WAL promoted to pending/),
        // a subsequent Volume::open() reads the correct data from the segment.
        // This covers the common path: crash after a guest fsync, before the
        // coordinator uploads the segment to S3.
        let base = keyed_temp_dir();

        let payload_a = vec![0xAAu8; 4096];
        let payload_b = vec![0xBBu8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &payload_a).unwrap();
            vol.write(1, &payload_b).unwrap();
            // promote_for_test flushes the WAL to pending/ and opens a fresh WAL.
            vol.promote_for_test().unwrap();
            // Drop without explicit shutdown — simulates a process crash after promotion.
        }

        // On reopen, data must come from the pending/ segment.
        // The fresh empty WAL (opened after promotion) contributes nothing.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), payload_a);
        assert_eq!(vol.read(1, 1).unwrap(), payload_b);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn recovery_removes_tmp_orphans() {
        // Guarantee: a .tmp file left in pending/ by a crashed segment write
        // (crash between write_segment and rename — the rename never committed)
        // is removed by Volume::open() and does not affect recovery.
        // The WAL is intact as a fallback and is replayed normally.
        let base = keyed_temp_dir();

        let payload = vec![0xCCu8; 4096];
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &payload).unwrap();
            vol.fsync().unwrap();
            // Drop with WAL intact — simulates crash before/during promotion.
        }

        // Simulate a crash mid-promotion: a .tmp file exists in pending/ but
        // no completed segment (the rename never happened).
        let orphan = base.join("pending").join("01AAAAAAAAAAAAAAAAAAAAAAAAA.tmp");
        fs::write(&orphan, b"incomplete segment bytes").unwrap();

        // Recovery must succeed, data must be correct, and the orphan removed.
        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        assert_eq!(vol.read(0, 1).unwrap(), payload);
        assert!(!orphan.exists(), ".tmp orphan should be cleaned up on open");

        fs::remove_dir_all(base).unwrap();
    }

    // --- compaction tests ---

    #[test]
    fn repack_noop_when_all_live() {
        // Write two blocks, promote, compact — nothing should be compacted
        // since all data is still referenced.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.write(1, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.repack(0.7).unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.bytes_freed, 0);
        assert_eq!(stats.extents_removed, 0);

        // Data still readable.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_reclaims_overwritten_extent() {
        // Write block A, promote, overwrite block A with B, promote.
        // First segment now has a dead extent; compaction should reclaim it.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let original = vec![0x11u8; 4096];
        let replacement = vec![0x22u8; 4096];

        vol.write(0, &original).unwrap();
        vol.promote_for_test().unwrap();

        vol.write(0, &replacement).unwrap();
        vol.promote_for_test().unwrap();

        // Two segments: first is 100% dead, second is live.
        let stats = vol.repack(0.7).unwrap();
        assert_eq!(
            stats.segments_compacted, 1,
            "first segment should be compacted"
        );
        assert!(stats.bytes_freed > 0);
        assert_eq!(stats.extents_removed, 1);

        // Data still reads back correctly after compaction.
        assert_eq!(vol.read(0, 1).unwrap(), replacement);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_reads_back_correctly_after_reopen() {
        // Verify that the compacted segment is a valid segment that survives
        // a volume reopen (LBA map rebuild + extent index rebuild).
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &vec![0xAAu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            vol.write(0, &vec![0xBBu8; 4096]).unwrap(); // overwrite
            vol.promote_for_test().unwrap();
            vol.repack(0.7).unwrap();
        }

        let vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), vec![0xBBu8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_partial_segment() {
        // Segment has two extents; one is overwritten (dead), one is live.
        // Compaction should rewrite the segment keeping only the live extent.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap(); // will be overwritten
        vol.write(1, &vec![0x22u8; 4096]).unwrap(); // stays live
        vol.promote_for_test().unwrap();

        vol.write(0, &vec![0x33u8; 4096]).unwrap(); // overwrites LBA 0
        vol.promote_for_test().unwrap();

        // First segment is 50% dead — above default threshold of 30% dead (0.7 live).
        let stats = vol.repack(0.7).unwrap();
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_freed > 0);

        // Both LBAs read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x33u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_respects_min_live_ratio() {
        // With a strict ratio (1.0), any dead byte triggers compaction.
        // With a lenient ratio (0.0), nothing is ever compacted.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap(); // LBA 0 now dead in seg 1
        vol.promote_for_test().unwrap();

        // Lenient threshold: first segment is 100% dead but ratio=0.0 → nothing compacted.
        let stats = vol.repack(0.0).unwrap();
        assert_eq!(stats.segments_compacted, 0);

        // Strict threshold: compact anything with any dead bytes.
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(stats.segments_compacted, 1);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_does_not_touch_pre_snapshot_segments() {
        // Write and overwrite a block, then snapshot. The dead segment is
        // pre-snapshot and must not be compacted — it is frozen by the floor.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Snapshot freezes both segments (floor = latest segment ULID).
        vol.snapshot().unwrap();

        // Even with a strict threshold the pre-snapshot segments must be skipped.
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "pre-snapshot segments must not be compacted"
        );

        // Data still readable.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_only_touches_post_snapshot_segments() {
        // Pre-snapshot dead segment: frozen. Post-snapshot dead segment: compactable.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Pre-snapshot: write and overwrite LBA 0.
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        vol.snapshot().unwrap();

        // Post-snapshot: write and overwrite LBA 1.
        vol.write(1, &vec![0x33u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &vec![0x44u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // One pre-snapshot dead segment (frozen) + one post-snapshot dead segment (eligible).
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(
            stats.segments_compacted, 1,
            "exactly the post-snapshot dead segment should be compacted"
        );

        // Both LBAs read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x44u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_does_not_touch_uploaded_segments() {
        // Simulate an uploaded segment (promoted to cache/ by the coordinator).
        // repack() must not touch it even if its extents are dead.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
        simulate_upload(&vol);

        // Overwrite LBA 0 — the uploaded segment's extent is now dead.
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Strict threshold: repack anything with dead bytes.
        let stats = vol.repack(1.0).unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "repack must not touch uploaded (cache/) segments"
        );

        // Data still reads correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    // --- sweep_pending tests ---

    #[test]
    fn sweep_pending_noop_when_all_live() {
        // Single pending segment with no dead extents: sweep_pending must not
        // rewrite it. Rewriting a single all-live small segment is a no-op that
        // only wastes IO — merging only makes sense when >=2 segments combine or
        // dead space is reclaimed.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.write(1, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.new_segments, 0);
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_removes_dead_extents() {
        // Write LBA 0, promote, overwrite LBA 0, promote.
        // sweep_pending should remove the dead extent from the first segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert!(stats.segments_compacted >= 1);
        assert!(stats.bytes_freed > 0);
        assert_eq!(stats.extents_removed, 1);

        // Current value of LBA 0 must be the replacement.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_only_scans_pending_not_uploaded() {
        // Upload a segment (simulate coordinator promoting pending → cache/).
        // sweep_pending must not touch uploaded segments.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
        simulate_upload(&vol);

        // Now overwrite LBA 0 and promote — creates a new pending segment.
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        // The old dead extent is in cache/ — sweep_pending doesn't touch it.
        assert_eq!(stats.extents_removed, 0);
        // The new pending segment is small and all-live: single segment, no
        // dead extents, so sweep_pending correctly leaves it alone.
        assert_eq!(stats.segments_compacted, 0);

        // Data still reads correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_respects_snapshot_floor() {
        // Segments at or below the snapshot ULID must not be touched.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Write and promote before snapshot.
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        vol.snapshot().unwrap();

        // The two pre-snapshot segments are now frozen.
        let stats = vol.sweep_pending().unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "pre-snapshot segments must not be touched"
        );

        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_merges_multiple_small_segments() {
        // Three separate promotes → three small pending segments.
        // sweep_pending should merge them into one.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0xaau8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &vec![0xbbu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(2, &vec![0xccu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(stats.segments_compacted, 3);
        assert_eq!(stats.new_segments, 1);

        // All three LBAs must still read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0xaau8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0xbbu8; 4096]);
        assert_eq!(vol.read(2, 1).unwrap(), vec![0xccu8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    // --- compression helper unit tests ---

    /// Build a 4096-byte block where every byte is distinct (entropy = 8 bits/byte).
    /// The LCG multiplier 109 (0x6D) is odd so it is coprime to 256, giving a
    /// bijection on [0, 255] — each value appears exactly 16 times in 4096 bytes.
    fn high_entropy_block(seed: u8) -> Vec<u8> {
        (0..4096u16)
            .map(|i| (i as u8).wrapping_mul(0x6D).wrapping_add(seed))
            .collect()
    }

    #[test]
    fn shannon_entropy_all_same_byte() {
        assert_eq!(shannon_entropy(&vec![0x42u8; 4096]), 0.0);
    }

    #[test]
    fn shannon_entropy_uniform_is_8_bits() {
        // 256 distinct values each appearing 16 times → exactly 8 bits/byte.
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let e = shannon_entropy(&data);
        assert!((e - 8.0).abs() < 0.01, "expected ~8.0, got {e}");
    }

    #[test]
    fn maybe_compress_compresses_low_entropy() {
        // All-zeros: entropy = 0, compresses to almost nothing.
        let data = vec![0u8; 4096];
        let compressed = maybe_compress(&data).expect("expected compression to succeed");
        // Must achieve at least 1.5× ratio.
        assert!(
            compressed.len() * MIN_COMPRESSION_RATIO_NUM / MIN_COMPRESSION_RATIO_DEN < data.len()
        );
    }

    #[test]
    fn maybe_compress_skips_high_entropy() {
        let data = high_entropy_block(0);
        assert!(shannon_entropy(&data) > ENTROPY_THRESHOLD);
        assert!(maybe_compress(&data).is_none());
    }

    // --- volume read/write tests for compressed and uncompressed paths ---

    #[test]
    fn read_incompressible_data() {
        // High-entropy data must not be compressed, and must read back correctly.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload = high_entropy_block(0x5A);
        assert!(
            shannon_entropy(&payload) > ENTROPY_THRESHOLD,
            "test data must be incompressible"
        );

        vol.write(0, &payload).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), payload);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn compressed_and_uncompressed_extents_coexist() {
        // Write one compressible and one incompressible extent; both must read back correctly.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let compressible = vec![0xCCu8; 4096];
        let incompressible = high_entropy_block(0xA3);

        vol.write(0, &compressible).unwrap();
        vol.write(1, &incompressible).unwrap();

        assert_eq!(vol.read(0, 1).unwrap(), compressible);
        assert_eq!(vol.read(1, 1).unwrap(), incompressible);

        fs::remove_dir_all(base).unwrap();
    }

    // --- write-path dedup tests ---

    #[test]
    fn dedup_write_same_data_same_lba() {
        // Writing identical data to the same LBA twice: second write is a dedup hit.
        // The LBA map must have exactly one entry, reads must return the correct data.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0x42u8; 4096];
        vol.write(0, &data).unwrap();
        vol.write(0, &data).unwrap();

        assert_eq!(vol.lbamap_len(), 1);
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_write_same_data_different_lba() {
        // Identical data written to two different LBAs: second write is a dedup hit.
        // Both LBA entries exist in the map; reads return the correct data from both.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0x77u8; 4096];
        vol.write(0, &data).unwrap();
        vol.write(5, &data).unwrap();

        assert_eq!(vol.lbamap_len(), 2);
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(5, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_ref_survives_promote_and_reopen() {
        // Write data, promote so it lands in pending/, then write the same data
        // to a new LBA (dedup REF in WAL). Reopen and verify both LBAs read back.
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let data = vec![0xABu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            // Second write: same data, different LBA → dedup hit, REF record in WAL.
            vol.write(1, &data).unwrap();
            vol.fsync().unwrap();
        }

        let vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xABu8; 4096];
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_ref_in_segment_survives_reopen() {
        // Write data, promote, write same data (REF in WAL), promote again so
        // the REF lands in a segment. Reopen and verify reads still work.
        let base = keyed_temp_dir();

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            let data = vec![0xCDu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            vol.write(1, &data).unwrap(); // REF
            vol.promote_for_test().unwrap(); // REF lands in segment
        }

        let vol = Volume::open(&base, &base).unwrap();
        let data = vec![0xCDu8; 4096];
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    // --- walk_ancestors tests ---

    #[test]
    fn walk_ancestors_root_returns_empty() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        // No origin file → root volume; ancestors are empty.
        assert!(walk_ancestors(&vol_dir, &by_id).unwrap().is_empty());
    }

    #[test]
    fn walk_ancestors_rejects_invalid_origin_paths() {
        let by_id = temp_dir();
        let fork_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        fs::create_dir_all(&fork_dir).unwrap();

        let bad_origins = [
            // not a ULID parent (old "base/" prefix)
            "base/snapshots/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // path traversal attempt
            "../01AAAAAAAAAAAAAAAAAAAAAAAA/snapshots/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // parent component is not a valid ULID
            "not-a-ulid/snapshots/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // missing /snapshots/ separator entirely
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // branch ULID missing after snapshots/
            "01AAAAAAAAAAAAAAAAAAAAAAAA/snapshots/",
        ];
        for bad in bad_origins {
            fs::write(fork_dir.join("volume.parent"), bad).unwrap();
            assert!(
                walk_ancestors(&fork_dir, &by_id).is_err(),
                "expected error for origin: {bad}"
            );
        }
    }

    #[test]
    fn walk_ancestors_one_level() {
        let by_id = temp_dir();
        let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(parent_ulid);
        let dev_dir = by_id.join(child_ulid);

        // dev's origin points to default at a fixed branch ULID.
        fs::create_dir_all(&dev_dir).unwrap();
        fs::write(
            dev_dir.join("volume.parent"),
            format!("{parent_ulid}/snapshots/01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        )
        .unwrap();

        let ancestors = walk_ancestors(&dev_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 1);
        assert_eq!(ancestors[0].dir, default_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_ancestors_two_levels() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let default_dir = by_id.join(root_ulid);
        let mid_dir = by_id.join(mid_ulid);
        let leaf_dir = by_id.join(leaf_ulid);

        fs::create_dir_all(&mid_dir).unwrap();
        fs::write(
            mid_dir.join("volume.parent"),
            format!("{root_ulid}/snapshots/01ARZ3NDEKTSV4RRFFQ69G5FAV"),
        )
        .unwrap();

        fs::create_dir_all(&leaf_dir).unwrap();
        fs::write(
            leaf_dir.join("volume.parent"),
            format!("{mid_ulid}/snapshots/01BX5ZZKJKTSV4RRFFQ69G5FAV"),
        )
        .unwrap();

        let ancestors = walk_ancestors(&leaf_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 2);
        assert_eq!(ancestors[0].dir, default_dir);
        assert_eq!(ancestors[1].dir, mid_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            ancestors[1].branch_ulid.as_deref(),
            Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
        );
    }

    // --- ancestor-aware open / read integration test ---

    /// Write data into a root volume, snapshot it, create a child volume via
    /// fork_volume, and verify the child can read the ancestor's data.
    #[test]
    fn open_reads_ancestor_segments() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        // Write data into the root volume and promote to a segment.
        let data = vec![0xABu8; 4096];
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        // Create a child volume branched from default.
        fork_volume(&child_dir, &default_dir).unwrap();

        // Child should see the ancestor's data through layer merge.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(by_id).unwrap();
    }

    /// Ancestor data is shadowed by a write in the live child volume.
    #[test]
    fn child_write_shadows_ancestor() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);
        let ancestor_data = vec![0xAAu8; 4096];
        let child_data = vec![0xBBu8; 4096];

        // Write into the root volume, promote, snapshot.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &ancestor_data).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        // Create child volume, write different data at the same LBA, promote.
        fork_volume(&child_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&child_dir, &by_id).unwrap();
            vol.write(0, &child_data).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open child and verify child data wins.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), child_data);

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- lock tests ---

    #[test]
    fn double_open_same_fork_fails() {
        let fork_dir = keyed_temp_dir();
        let _vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        // Second open on the same live fork must fail (lock already held).
        assert!(Volume::open(&fork_dir, &fork_dir).is_err());
        fs::remove_dir_all(fork_dir).unwrap();
    }

    // --- snapshot() tests ---

    #[test]
    fn snapshot_writes_marker_and_stays_live() {
        let fork_dir = keyed_temp_dir();
        let data = vec![0xAAu8; 4096];

        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &data).unwrap();
        let snap_ulid = vol.snapshot().unwrap();

        // Fork still has wal/ (still live).
        assert!(fork_dir.join("wal").is_dir());
        // Snapshot marker file exists.
        assert!(
            fork_dir
                .join("snapshots")
                .join(snap_ulid.to_string())
                .exists()
        );

        // Writes after snapshot still go to the same fork.
        let new_data = vec![0xBBu8; 4096];
        vol.write(1, &new_data).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), new_data);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_ulid_matches_last_segment_ulid() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        let snap_ulid = vol.snapshot().unwrap().to_string();

        // The snapshot file name must equal the segment file name in pending/.
        let pending_files: Vec<_> = fs::read_dir(fork_dir.join("pending"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(pending_files.len(), 1);
        let seg_name = pending_files[0].file_name().into_string().unwrap();
        assert_eq!(snap_ulid, seg_name);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_empty_wal_no_segment_written() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        // No writes — WAL is empty.
        vol.snapshot().unwrap();

        // pending/ should be empty (no segment written for empty WAL).
        let pending: Vec<_> = fs::read_dir(fork_dir.join("pending")).unwrap().collect();
        assert!(pending.is_empty());

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_idempotent_when_no_new_data() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();

        let ulid1 = vol.snapshot().unwrap();
        // No new writes — second snapshot must return the same ULID.
        let ulid2 = vol.snapshot().unwrap();
        assert_eq!(ulid1, ulid2);

        // Still only one snapshot marker on disk.
        let snaps: Vec<_> = fs::read_dir(fork_dir.join("snapshots")).unwrap().collect();
        assert_eq!(snaps.len(), 1);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_not_idempotent_after_new_write() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();

        let ulid1 = vol.snapshot().unwrap();
        vol.write(1, &vec![0xBBu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let ulid2 = vol.snapshot().unwrap();
        assert_ne!(ulid1, ulid2);

        let snaps: Vec<_> = fs::read_dir(fork_dir.join("snapshots")).unwrap().collect();
        assert_eq!(snaps.len(), 2);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_idempotent_after_auto_promoted_data_already_snapshotted() {
        // Data promoted via FLUSH_THRESHOLD (pending_entries empty at snapshot
        // time) but that segment was already covered by a prior snapshot.
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        vol.promote_for_test().unwrap(); // lands in pending/ with wal_ulid_1
        let ulid1 = vol.snapshot().unwrap(); // snapshot covers pending/wal_ulid_1
        // pending_entries is now empty; pending/ has one file but it's <= ulid1.
        let ulid2 = vol.snapshot().unwrap();
        assert_eq!(ulid1, ulid2);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn snapshot_lock_held_after_snapshot() {
        let fork_dir = keyed_temp_dir();
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.snapshot().unwrap();

        // Fork is still locked (still live); second open must fail.
        assert!(Volume::open(&fork_dir, &fork_dir).is_err());
        drop(vol); // now released

        // After drop, a fresh open succeeds.
        assert!(Volume::open(&fork_dir, &fork_dir).is_ok());

        fs::remove_dir_all(fork_dir).unwrap();
    }

    // --- fork_volume tests ---

    #[test]
    fn fork_volume_creates_fork_with_origin() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(root_ulid);
        let fork_dir = by_id.join(child_ulid);
        write_test_keypair(&default_dir);

        // snapshot default to give it a branch point.
        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        vol.write(0, &vec![0xAAu8; 4096]).unwrap();
        let snap_ulid = vol.snapshot().unwrap().to_string();
        drop(vol);

        // Create the fork.
        fork_volume(&fork_dir, &default_dir).unwrap();
        assert!(fork_dir.join("wal").is_dir());
        assert!(fork_dir.join("pending").is_dir());

        let origin = fs::read_to_string(fork_dir.join("volume.parent")).unwrap();
        assert_eq!(origin.trim(), format!("{root_ulid}/snapshots/{snap_ulid}"));

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_errors_if_source_has_no_snapshots() {
        let by_id = temp_dir();
        let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        // Create root_dir so canonicalize() succeeds, but leave it without
        // a snapshots/ directory so latest_snapshot returns "no snapshots".
        fs::create_dir_all(&root_dir).unwrap();
        let err = fork_volume(&child_dir, &root_dir).unwrap_err();
        assert!(err.to_string().contains("no snapshots"), "{err}");
    }

    #[test]
    fn fork_volume_uses_latest_snapshot_when_multiple_exist() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(root_ulid);
        let fork_dir = by_id.join(child_ulid);
        write_test_keypair(&default_dir);

        let data_snap1 = vec![0x11u8; 4096];
        let data_snap2 = vec![0x22u8; 4096];

        let mut vol = Volume::open(&default_dir, &by_id).unwrap();
        // First snapshot — should NOT be the branch point.
        vol.write(0, &data_snap1).unwrap();
        let snap1 = vol.snapshot().unwrap().to_string();
        // Second snapshot — should be the branch point.
        vol.write(1, &data_snap2).unwrap();
        let snap2 = vol.snapshot().unwrap().to_string();
        drop(vol);

        // snap2 must sort after snap1 (ULIDs are monotonically increasing).
        assert!(snap2 > snap1);

        fork_volume(&fork_dir, &default_dir).unwrap();
        let origin = fs::read_to_string(fork_dir.join("volume.parent")).unwrap();
        assert_eq!(
            origin.trim(),
            format!("{root_ulid}/snapshots/{snap2}"),
            "origin should point to the latest snapshot"
        );

        // Fork branched from snap2 sees both pre-snap1 and pre-snap2 writes.
        let vol = Volume::open(&fork_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_snap1);
        assert_eq!(vol.read(1, 1).unwrap(), data_snap2);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_from_child_fork_creates_three_level_chain() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let default_dir = by_id.join(root_ulid);
        let mid_dir = by_id.join(mid_ulid);
        let leaf_dir = by_id.join(leaf_ulid);
        write_test_keypair(&default_dir);

        let data_root = vec![0xAAu8; 4096];
        let data_mid = vec![0xBBu8; 4096];
        let data_leaf = vec![0xCCu8; 4096];

        // Root volume: write + snapshot.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data_root).unwrap();
            vol.snapshot().unwrap();
        }

        // Mid volume: branch from default, write + snapshot.
        fork_volume(&mid_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&mid_dir, &by_id).unwrap();
            vol.write(1, &data_mid).unwrap();
            vol.snapshot().unwrap();
        }

        // Leaf volume: branch from mid.
        fork_volume(&leaf_dir, &mid_dir).unwrap();

        // origin chain: leaf → mid → default (ULID-based flat layout).
        let leaf_origin = fs::read_to_string(leaf_dir.join("volume.parent")).unwrap();
        assert!(
            leaf_origin.starts_with(&format!("{mid_ulid}/snapshots/")),
            "leaf origin: {leaf_origin}"
        );
        let mid_origin = fs::read_to_string(mid_dir.join("volume.parent")).unwrap();
        assert!(
            mid_origin.starts_with(&format!("{root_ulid}/snapshots/")),
            "mid origin: {mid_origin}"
        );

        // Leaf sees data from all three levels.
        let vol = Volume::open(&leaf_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_root);
        assert_eq!(vol.read(1, 1).unwrap(), data_mid);
        assert_eq!(vol.read(2, 1).unwrap(), vec![0u8; 4096]); // unwritten

        // Write to leaf does not affect mid or default.
        drop(vol);
        {
            let mut vol = Volume::open(&leaf_dir, &by_id).unwrap();
            vol.write(2, &data_leaf).unwrap();
        }
        let vol = Volume::open(&leaf_dir, &by_id).unwrap();
        assert_eq!(vol.read(2, 1).unwrap(), data_leaf);
        assert_eq!(vol.ancestor_count(), 2);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn fork_volume_errors_if_fork_exists() {
        let by_id = temp_dir();
        let root_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&root_dir);
        let mut vol = Volume::open(&root_dir, &by_id).unwrap();
        vol.snapshot().unwrap();
        drop(vol);

        fork_volume(&child_dir, &root_dir).unwrap();
        let err = fork_volume(&child_dir, &root_dir).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- multi-snapshot read tests ---

    #[test]
    fn two_snapshots_data_readable_after_reopen() {
        let fork_dir = keyed_temp_dir();
        let data_a = vec![0xAAu8; 4096];
        let data_b = vec![0xBBu8; 4096];

        {
            let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
            vol.write(0, &data_a).unwrap();
            vol.snapshot().unwrap();
            vol.write(1, &data_b).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open the same fork: both writes visible.
        let vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_a);
        assert_eq!(vol.read(1, 1).unwrap(), data_b);

        fs::remove_dir_all(fork_dir).unwrap();
    }

    #[test]
    fn fork_data_visible_across_ancestry() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);
        let data_a = vec![0xAAu8; 4096];
        let data_b = vec![0xBBu8; 4096];

        // Write to default, snapshot, create fork, write to fork.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &data_a).unwrap();
            vol.promote_for_test().unwrap();
            vol.snapshot().unwrap();
        }

        fork_volume(&child_dir, &default_dir).unwrap();
        {
            let mut vol = Volume::open(&child_dir, &by_id).unwrap();
            vol.write(1, &data_b).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open child: sees both ancestor and own data.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data_a);
        assert_eq!(vol.read(1, 1).unwrap(), data_b);
        assert_eq!(vol.ancestor_count(), 1);

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- ULID cutoff tests ---

    /// Segments written to an ancestor volume *after* the branch point must not
    /// be visible to a child volume. This is the core correctness property of
    /// the per-ancestor ULID cutoff stored in `origin`.
    #[test]
    fn ulid_cutoff_hides_post_branch_ancestor_writes() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let pre_branch = vec![0xAAu8; 4096];
        let post_branch = vec![0xBBu8; 4096];

        // Write pre-branch data to ancestor, snapshot, then branch.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &pre_branch).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();

        // Write post-branch data to the ancestor volume at LBA 1 (a new LBA).
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(1, &post_branch).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Child must see pre-branch data at LBA 0 and zeros at LBA 1.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(
            vol.read(0, 1).unwrap(),
            pre_branch,
            "pre-branch data must be visible"
        );
        assert_eq!(
            vol.read(1, 1).unwrap(),
            vec![0u8; 4096],
            "post-branch ancestor write must be invisible"
        );

        fs::remove_dir_all(by_id).unwrap();
    }

    /// A post-branch write to an ancestor that *overwrites* a pre-branch LBA
    /// must also be invisible — the child must still see the original value.
    #[test]
    fn ulid_cutoff_overwrite_stays_invisible() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let original = vec![0xAAu8; 4096];
        let overwrite = vec![0xBBu8; 4096];

        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &original).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();

        // Ancestor overwrites LBA 0 after the branch.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &overwrite).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Child must still see the original pre-branch value.
        let vol = Volume::open(&child_dir, &by_id).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), original);

        fs::remove_dir_all(by_id).unwrap();
    }

    // --- ReadonlyVolume tests ---

    #[test]
    fn readonly_volume_unwritten_returns_zeros() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        // Create the directory structure without a WAL (simulating a readonly base).
        fs::create_dir_all(fork_dir.join("pending")).unwrap();

        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), vec![0u8; 4096]);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_volume_reads_committed_segment() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        write_test_keypair(&fork_dir);

        let data = vec![0xCCu8; 4096];

        // Write data into the fork via Volume, then drop the lock.
        {
            let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
        }
        // Remove wal/ so ReadonlyVolume::open doesn't see a live WAL.
        // (ReadonlyVolume intentionally skips WAL replay; this also tests the
        //  no-WAL path.)
        fs::remove_dir_all(fork_dir.join("wal")).unwrap();

        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), data);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_volume_reads_ancestor_data() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let ancestor_data = vec![0xDDu8; 4096];

        // Write data into default, snapshot, fork.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &ancestor_data).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();
        // ReadonlyVolume doesn't take a write lock, so this always works.

        let rv = ReadonlyVolume::open(&child_dir, &by_id).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), ancestor_data);

        fs::remove_dir_all(by_id).unwrap();
    }

    #[test]
    fn readonly_volume_does_not_see_wal_records() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        write_test_keypair(&fork_dir);

        let committed = vec![0xEEu8; 4096];
        let in_wal = vec![0xFFu8; 4096];

        // Write and promote LBA 0, then write LBA 1 to the WAL only.
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &committed).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &in_wal).unwrap();
        // Do NOT promote — LBA 1 is only in the WAL.
        // Drop the writable volume so the lock is released.
        drop(vol);

        // ReadonlyVolume skips WAL replay: LBA 1 must appear as zeros.
        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), committed);
        assert_eq!(rv.read(1, 1).unwrap(), vec![0u8; 4096]);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    // --- apply_gc_handoffs tests ---
    //
    // These tests simulate the coordinator GC workflow:
    //   write → flush → drain (pending→cache + index) → coordinator compacts
    //   into new segment + writes gc/*.pending → volume applies handoff.

    /// Simulate one coordinator GC pass: read the given uploaded segment (from
    /// index/ + cache/), write a compacted copy to gc/<new_ulid> (signed with
    /// an ephemeral key, as the real coordinator would), and write gc/<new>.pending.
    ///
    /// The volume re-signs the staged segment when apply_gc_handoffs is called.
    /// Does NOT delete the old cache/ body or index/.idx.
    fn simulate_coord_gc(vol: &mut Volume, fork_dir: &Path, old_ulid: &str) -> String {
        use crate::{segment, signing};

        // Read index from .idx (header+index section); read body from cache/.body
        // (body bytes starting at offset 0).
        let idx_path = fork_dir.join("index").join(format!("{old_ulid}.idx"));
        let body_path = fork_dir.join("cache").join(format!("{old_ulid}.body"));
        let (_old_bss, mut entries) =
            segment::read_and_verify_segment_index(&idx_path, &vol.verifying_key).unwrap();
        // Cache .body files start at byte 0 of the body section.
        segment::read_extent_bodies(&body_path, 0, &mut entries).unwrap();

        let (new_ulid, _) = vol.gc_checkpoint().unwrap();
        let new_ulid_str = new_ulid.to_string();

        let gc_dir = fork_dir.join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        // Coordinator uses an ephemeral signer; the volume re-signs on handoff.
        let (ephemeral_signer, _) = signing::generate_ephemeral_signer();
        let tmp_path = gc_dir.join(format!("{new_ulid_str}.tmp"));
        let new_bss =
            segment::write_segment(&tmp_path, &mut entries, ephemeral_signer.as_ref()).unwrap();
        fs::rename(&tmp_path, gc_dir.join(&new_ulid_str)).unwrap();

        let old_ulid_parsed = Ulid::from_string(old_ulid).unwrap();
        let handoff_lines: Vec<HandoffLine> = entries
            .iter()
            .filter(|e| e.kind != segment::EntryKind::DedupRef)
            .map(|e| HandoffLine::Repack {
                hash: e.hash,
                old_ulid: old_ulid_parsed,
                new_ulid,
                new_offset: new_bss + e.stored_offset,
            })
            .collect();
        fs::write(
            gc_dir.join(format!("{new_ulid_str}.pending")),
            crate::gc::format_handoff_file(handoff_lines),
        )
        .unwrap();

        new_ulid_str
    }

    #[test]
    fn gc_handoff_applies_and_renames() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let data = vec![0x42u8; 4096];
        vol.write(0, &data).unwrap();
        vol.promote_for_test().unwrap();

        // Drain: simulate coordinator upload + promote IPC.
        // promote_segment writes index/<ulid>.idx, copies body to cache/, deletes pending/.
        let pending_dir = base.join("pending");
        let old_ulid = fs::read_dir(&pending_dir)
            .unwrap()
            .flatten()
            .next()
            .unwrap()
            .file_name()
            .into_string()
            .unwrap();
        simulate_upload(&vol);

        // Coordinator GC: compact into new segment, write .pending.
        let new_ulid = simulate_coord_gc(&mut vol, &base, &old_ulid);

        // Apply the handoff.
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);

        // .pending was renamed to .applied.
        let gc_dir = base.join("gc");
        assert!(!gc_dir.join(format!("{new_ulid}.pending")).exists());
        assert!(gc_dir.join(format!("{new_ulid}.applied")).exists());

        // After apply_gc_handoffs: old idx still present (not deleted until promote),
        // new idx not yet written (written by promote_segment after S3 upload).
        let cache_dir = base.join("cache");
        let index_dir = base.join("index");
        assert!(
            index_dir.join(format!("{old_ulid}.idx")).exists(),
            "old idx must persist until promote_segment runs"
        );
        assert!(
            !index_dir.join(format!("{new_ulid}.idx")).exists(),
            "new idx must not exist before promote_segment (not yet S3-confirmed)"
        );

        // Simulate coordinator: upload gc/<new> to S3, then send promote IPC.
        // promote_segment writes index/<new>.idx, cache/<new>.body+.present,
        // reads the .applied handoff to find old ULIDs, and deletes index/<old>.idx.
        let new_ulid_parsed = Ulid::from_string(&new_ulid).unwrap();
        vol.promote_segment(new_ulid_parsed).unwrap();

        assert!(
            index_dir.join(format!("{new_ulid}.idx")).exists(),
            "promote_segment must write index/<new>.idx"
        );
        assert!(
            !index_dir.join(format!("{old_ulid}.idx")).exists(),
            "promote_segment must delete index/<old>.idx"
        );

        // cache/<old>.* is evicted by evict_applied_gc_cache() AFTER snapshot publish
        // (called from actor loop); calling it explicitly here simulates that step.
        vol.evict_applied_gc_cache();
        assert!(
            !cache_dir.join(format!("{old_ulid}.body")).exists(),
            "evict_applied_gc_cache must delete cache/<old>.body"
        );
        assert!(
            !cache_dir.join(format!("{old_ulid}.present")).exists(),
            "evict_applied_gc_cache must delete cache/<old>.present"
        );
        // Reads still work via cache/<new>.body.
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_handoff_skips_missing_segment() {
        // gc/*.pending exists but the new segment body has not yet been fetched
        // locally.  apply_gc_handoffs must skip the file and return Ok(0) so
        // the next idle tick retries when the segment is available.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let gc_dir = base.join("gc");
        fs::create_dir_all(&gc_dir).unwrap();

        // Use a plausible ULID that has no matching segment file.
        let phantom_ulid = Ulid::new().to_string();
        let pending_path = gc_dir.join(format!("{phantom_ulid}.pending"));
        fs::write(&pending_path, "").unwrap();

        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 0);

        // File must still be present — not renamed or deleted.
        assert!(pending_path.exists());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_handoff_idempotent_after_crash() {
        // Simulate a crash between coordinator writing .pending and the volume
        // processing it.  The coordinator-staged segment is in gc/<new_ulid>;
        // the old segment body remains in cache/ until apply_gc_handoffs renames
        // .pending → .applied (signalling the coordinator it is safe to delete).
        //
        // After reopen, the extent index is rebuilt from index/*.idx (old segment
        // still has its .idx), so reads are correct before the handoff is applied.
        // apply_gc_handoffs re-signs gc/<new_ulid> in-place, updates the extent
        // index, and renames .pending → .applied.
        let base = keyed_temp_dir();

        let old_ulid;
        let new_ulid;
        let data = vec![0xABu8; 4096];

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();

            // Drain: simulate coordinator upload + promote IPC.
            let pending_dir = base.join("pending");
            old_ulid = fs::read_dir(&pending_dir)
                .unwrap()
                .flatten()
                .next()
                .unwrap()
                .file_name()
                .into_string()
                .unwrap();
            simulate_upload(&vol);

            // Coordinator GC: staged segment in gc/<new_ulid> + .pending written.
            // Old segment body intentionally NOT deleted — coordinator waits for .applied.
            new_ulid = simulate_coord_gc(&mut vol, &base, &old_ulid);

            // "Crash" — drop the volume before apply_gc_handoffs runs.
        }

        // Reopen: rebuild scans index/ and finds the old segment's .idx.  Reads
        // work via cache/ even though the coordinator has already produced a replacement.
        let mut vol = Volume::open(&base, &base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), data);

        // Apply the pending handoff: re-signs gc/<new_ulid> in-place,
        // updates extent index, renames .pending → .applied.
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);

        let gc_dir = base.join("gc");
        assert!(!gc_dir.join(format!("{new_ulid}.pending")).exists());
        assert!(gc_dir.join(format!("{new_ulid}.applied")).exists());

        // After apply_gc_handoffs: old idx still present, new idx not yet written.
        let cache_dir = base.join("cache");
        let index_dir = base.join("index");
        assert!(
            index_dir.join(format!("{old_ulid}.idx")).exists(),
            "old idx must persist until promote_segment runs"
        );
        assert!(
            !index_dir.join(format!("{new_ulid}.idx")).exists(),
            "new idx must not exist before promote_segment"
        );

        // Simulate coordinator: upload gc/<new> to S3, then send promote IPC.
        let new_ulid_parsed = Ulid::from_string(&new_ulid).unwrap();
        vol.promote_segment(new_ulid_parsed).unwrap();

        assert!(
            index_dir.join(format!("{new_ulid}.idx")).exists(),
            "promote_segment must write index/<new>.idx"
        );
        assert!(
            !index_dir.join(format!("{old_ulid}.idx")).exists(),
            "promote_segment must delete index/<old>.idx"
        );

        // cache/<old>.* evicted by actor after snapshot publish; simulate here.
        vol.evict_applied_gc_cache();
        assert!(
            !cache_dir.join(format!("{old_ulid}.body")).exists(),
            "evict_applied_gc_cache must delete cache/<old>.body"
        );
        assert!(
            !cache_dir.join(format!("{old_ulid}.present")).exists(),
            "evict_applied_gc_cache must delete cache/<old>.present"
        );

        // Reads still correct: extent index points to new_ulid, body in cache/.
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn gc_handoff_idempotent_after_partial_resign() {
        // Simulate a crash between the in-place re-sign rename completing
        // (gc/<new_ulid>.tmp → gc/<new_ulid>) and the .pending → .applied rename.
        // State: gc/<new_ulid> holds the volume-signed segment; .pending still
        // exists; no .applied yet.
        //
        // apply_gc_handoffs must succeed on retry: it re-signs gc/<new_ulid>
        // idempotently, updates the extent index, and renames .pending → .applied.
        // The body stays in gc/ — the coordinator uploads it to S3 and writes
        // index/<new>.idx + cache/<new>.{body,present} after confirmed upload.
        let base = keyed_temp_dir();

        let old_ulid;
        let new_ulid;
        let data = vec![0xCDu8; 4096];

        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();

            let pending_dir = base.join("pending");
            old_ulid = fs::read_dir(&pending_dir)
                .unwrap()
                .flatten()
                .next()
                .unwrap()
                .file_name()
                .into_string()
                .unwrap();
            simulate_upload(&vol);

            new_ulid = simulate_coord_gc(&mut vol, &base, &old_ulid);
        }

        // Run apply once to produce the re-signed gc/<new_ulid> and .applied.
        {
            let mut vol = Volume::open(&base, &base).unwrap();
            vol.apply_gc_handoffs().unwrap();
        }

        // Restore crash state: rename .applied back to .pending.
        // gc/<new_ulid> is already volume-signed and still present — this
        // represents a crash after the in-place rename but before .applied was
        // written.
        let gc_dir = base.join("gc");
        fs::rename(
            gc_dir.join(format!("{new_ulid}.applied")),
            gc_dir.join(format!("{new_ulid}.pending")),
        )
        .unwrap();
        assert!(
            gc_dir.join(&new_ulid).exists(),
            "gc/<ulid> must still exist (body stays in gc/ until coordinator moves it)"
        );

        // Retry: apply_gc_handoffs re-signs gc/<new_ulid> idempotently and
        // renames .pending → .applied.
        let mut vol = Volume::open(&base, &base).unwrap();
        let count = vol.apply_gc_handoffs().unwrap();
        assert_eq!(count, 1);

        assert!(!gc_dir.join(format!("{new_ulid}.pending")).exists());
        assert!(gc_dir.join(format!("{new_ulid}.applied")).exists());
        assert!(
            gc_dir.join(&new_ulid).exists(),
            "gc/<ulid> stays until coordinator uploads it and promotes to cache/"
        );

        // Data still correct.
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }
}
