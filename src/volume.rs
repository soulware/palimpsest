// Volume: top-level I/O interface — owns the LBA map, WAL, and directory layout.
//
// Directory layout:
//   <base>/wal/       — active write-ahead log (at most one file at a time)
//   <base>/pending/   — promoted segments awaiting S3 upload
//   <base>/segments/  — segments confirmed uploaded to S3 (evictable)
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
//   3. find_segment_file (wal/ → pending/ → segments/) → open file, seek, read
//
// Recovery:
//   Volume::open() calls lbamap::rebuild_segments() (segments only), then
//   scans the WAL once: that single pass truncates any partial-tail record,
//   replays entries into the LBA map, extent index, and pending_entries.
//   Any .tmp files in pending/ are removed (incomplete promotions).

use std::cell::RefCell;
use std::fs;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::{extentindex, lbamap, segment, writelog};

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

/// Attempt zstd compression on `data`.
///
/// Returns `Some(compressed_bytes)` if the entropy is low enough and the
/// compression ratio meets the minimum threshold; `None` to store raw.
fn maybe_compress(data: &[u8]) -> Option<Vec<u8>> {
    if shannon_entropy(data) > ENTROPY_THRESHOLD {
        return None;
    }
    let compressed = zstd::bulk::compress(data, 1).ok()?;
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

/// Results from a single compaction run.
#[derive(Debug, Default)]
pub struct CompactionStats {
    /// Number of segments rewritten (old segment deleted, new denser segment written).
    pub segments_compacted: usize,
    /// Stored bytes reclaimed from deleted segment bodies.
    pub bytes_freed: u64,
    /// Number of dead extent entries removed from the extent index.
    pub extents_removed: usize,
}

/// A writable block-device volume backed by a content-addressable store.
///
/// Owns the in-memory LBA map, the active WAL, and the directory layout.
pub struct Volume {
    base_dir: PathBuf,
    /// Ancestor node directories, oldest-first. Does not include `base_dir`.
    ancestor_dirs: Vec<PathBuf>,
    /// Exclusive lock on `base_dir/volume.lock`. Held for the lifetime of the
    /// Volume; dropped (releasing the lock) when Volume is dropped or when
    /// `snapshot()` is called and transitions writes to a child node.
    lock_file: fs::File,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    wal: writelog::WriteLog,
    wal_ulid: String,
    wal_path: PathBuf,
    /// DATA and REF extents written since the last promotion; used to write
    /// the clean segment file on the next promote().
    pending_entries: Vec<segment::SegmentEntry>,
    /// Single-entry file handle cache for the read path.
    ///
    /// Retains the last opened segment file across `read` calls so that
    /// sequential reads hitting the same segment avoid repeated `open` syscalls.
    /// `RefCell` keeps `read` logically non-mutating (`&self`) while allowing
    /// the cache to be updated internally.
    file_cache: RefCell<Option<(String, fs::File)>>,
}

impl Volume {
    /// Open (or create) a volume rooted at `base_dir`.
    ///
    /// Creates `wal/`, `pending/`, and `segments/` subdirectories if they do
    /// not exist. Rebuilds the LBA map from all committed segments and any
    /// in-progress WAL, then reopens or creates the WAL.
    ///
    /// If `base_dir` is a frozen node (`children/` exists, meaning `snapshot()`
    /// has been called on it at least once), a new child node is created
    /// automatically and opened instead. The caller always gets a live, writable
    /// volume; the actual node path may differ from `base_dir` in this case.
    pub fn open(base_dir: &Path) -> io::Result<Self> {
        // A frozen node has had snapshot() called on it at least once, leaving
        // children/ present and wal/ absent. Rather than erroring, create a new
        // sibling child and open that — the caller gets a fresh live node that
        // inherits the frozen node's data via the ancestor chain.
        if base_dir.join("children").exists() {
            let child_dir = base_dir.join("children").join(Ulid::new().to_string());
            return Self::open(&child_dir);
        }

        let wal_dir = base_dir.join("wal");
        let pending_dir = base_dir.join("pending");
        let segments_dir = base_dir.join("segments");

        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&pending_dir)?;
        fs::create_dir_all(&segments_dir)?;

        // Acquire exclusive lock. Fails immediately if another process has this
        // node open. The lock is released when Volume is dropped.
        let lock_file = acquire_lock(base_dir)?;

        // Remove any .tmp files in pending/ — these are incomplete promotions
        // left behind by a crash between write and rename.
        for entry in fs::read_dir(&pending_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(&path)?;
            }
        }

        // Build the full node chain: ancestors oldest-first, live node last.
        let ancestor_dirs = walk_ancestors(base_dir);
        let node_chain: Vec<PathBuf> = ancestor_dirs
            .iter()
            .cloned()
            .chain(std::iter::once(base_dir.to_owned()))
            .collect();

        // Rebuild the LBA map and extent index from all committed segments.
        let mut lbamap = lbamap::rebuild_segments(&node_chain)?;
        let mut extent_index = extentindex::rebuild(&node_chain)?;

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
        // interrupted. The segment is authoritative — delete any such stale
        // WAL files before deciding what to replay. rebuild_segments above
        // has already loaded the committed segment into lbamap/extent_index.
        wal_files.retain(|path| {
            let ulid = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if pending_dir.join(ulid).exists() {
                let _ = fs::remove_file(path);
                false
            } else {
                true
            }
        });

        // recover_wal does the single WAL scan: truncates any partial tail,
        // replays records into the LBA map, and rebuilds pending_entries.
        let (wal, wal_ulid, wal_path, pending_entries) =
            if let Some(path) = wal_files.into_iter().last() {
                recover_wal(path, &mut lbamap, &mut extent_index)?
            } else {
                create_fresh_wal(&wal_dir)?
            };

        Ok(Self {
            base_dir: base_dir.to_owned(),
            ancestor_dirs,
            lock_file,
            lbamap,
            extent_index,
            wal,
            wal_ulid,
            wal_path,
            pending_entries,
            file_cache: RefCell::new(None),
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

        // Write-path dedup: if this extent already exists in this volume's
        // segment tree (own segments + ancestors), write a REF record instead
        // of a DATA record. Scope is limited to within-volume so that every
        // REF resolves within the same S3 volume tree (self-contained uploads).
        if self.extent_index.lookup(&hash).is_some() {
            self.wal.append_ref(lba, lba_length, &hash)?;
            self.lbamap.insert(lba, lba_length, hash);
            self.pending_entries
                .push(segment::SegmentEntry::new_dedup_ref(hash, lba, lba_length));
            if self.wal.size() >= FLUSH_THRESHOLD {
                self.promote()?;
            }
            return Ok(());
        }

        let compressed_data = maybe_compress(data);
        let compressed = compressed_data.is_some();
        let owned_data: Vec<u8> = compressed_data.unwrap_or_else(|| data.to_vec());
        let wal_flags = if compressed {
            writelog::FLAG_COMPRESSED
        } else {
            0
        };
        let seg_flags = if compressed {
            segment::FLAG_COMPRESSED
        } else {
            0
        };

        let body_offset = self
            .wal
            .append_data(lba, lba_length, &hash, wal_flags, &owned_data)?;
        self.lbamap.insert(lba, lba_length, hash);
        // Temporary extent index entry: points into the WAL at the raw payload offset.
        // Updated to segment file offsets after promotion.
        self.extent_index.insert(
            hash,
            extentindex::ExtentLocation {
                segment_id: self.wal_ulid.clone(),
                body_offset,
                body_length: owned_data.len() as u32,
                compressed,
            },
        );
        self.pending_entries.push(segment::SegmentEntry::new_data(
            hash, lba, lba_length, seg_flags, owned_data,
        ));

        if self.wal.size() >= FLUSH_THRESHOLD {
            self.promote()?;
        }

        Ok(())
    }

    /// Read `lba_count` blocks (4096 bytes each) starting at `lba`.
    ///
    /// Blocks that have never been written are returned as zeros (the
    /// block-device convention for unwritten regions). Written blocks are
    /// fetched extent-by-extent: one file open and one read (or decompress)
    /// per extent, regardless of how many blocks within the extent are needed.
    pub fn read(&self, lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut out = vec![0u8; lba_count as usize * 4096];
        for er in self.lbamap.extents_in_range(lba, lba + lba_count as u64) {
            // Extract owned copies so the borrow of self.extent_index ends before
            // we mutate self.file_cache.
            let (segment_id, body_offset, body_length, compressed) = {
                let Some(loc) = self.extent_index.lookup(&er.hash) else {
                    continue; // hash not indexed — treat as unwritten
                };
                (
                    loc.segment_id.clone(),
                    loc.body_offset,
                    loc.body_length,
                    loc.compressed,
                )
            };

            // Reuse the cached file handle if it is for the same segment;
            // otherwise open the new segment and replace the cache entry.
            let mut cache = self.file_cache.borrow_mut();
            if cache.as_ref().map(|(id, _)| id.as_str()) != Some(segment_id.as_str()) {
                let path = self.find_segment_file(&segment_id)?;
                *cache = Some((segment_id, fs::File::open(path)?));
            }
            let f = &mut cache.as_mut().unwrap().1;

            let block_count = (er.range_end - er.range_start) as usize;
            let out_start = (er.range_start - lba) as usize * 4096;
            let out_slice = &mut out[out_start..out_start + block_count * 4096];

            if compressed {
                f.seek(SeekFrom::Start(body_offset))?;
                let mut compressed_buf = vec![0u8; body_length as usize];
                f.read_exact(&mut compressed_buf)?;
                let decompressed =
                    zstd::decode_all(compressed_buf.as_slice()).map_err(io::Error::other)?;
                let src_start = er.payload_block_offset as usize * 4096;
                out_slice.copy_from_slice(&decompressed[src_start..src_start + block_count * 4096]);
            } else {
                f.seek(SeekFrom::Start(
                    body_offset + er.payload_block_offset as u64 * 4096,
                ))?;
                f.read_exact(out_slice)?;
            }
        }
        Ok(out)
    }

    /// Flush buffered WAL writes and fsync to disk.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.wal.fsync()
    }

    /// Compact sparse segments in `pending/` and `segments/`.
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
    pub fn compact(&mut self, min_live_ratio: f64) -> io::Result<CompactionStats> {
        use std::collections::HashSet;

        let live: HashSet<blake3::Hash> = self.lbamap.live_hashes();
        let mut stats = CompactionStats::default();

        let mut all_segs = segment::collect_segment_files(&self.base_dir.join("pending"))?;
        all_segs.extend(segment::collect_segment_files(
            &self.base_dir.join("segments"),
        )?);

        for seg_path in all_segs {
            let seg_id = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| io::Error::other("bad segment filename"))?;
            let seg_id = Ulid::from_string(seg_id)
                .map_err(|e| io::Error::other(e.to_string()))?
                .to_string();

            let (body_section_start, mut entries) = segment::read_segment_index(&seg_path)?;

            // Dedup-refs have no body bytes; only count DATA entries.
            let total_bytes: u64 = entries
                .iter()
                .filter(|e| !e.is_dedup_ref)
                .map(|e| e.stored_length as u64)
                .sum();

            if total_bytes == 0 {
                continue;
            }

            let live_bytes: u64 = entries
                .iter()
                .filter(|e| !e.is_dedup_ref && live.contains(&e.hash))
                .map(|e| e.stored_length as u64)
                .sum();

            if live_bytes as f64 / total_bytes as f64 >= min_live_ratio {
                continue;
            }

            let (mut live_entries, dead_entries): (Vec<_>, Vec<_>) =
                entries.drain(..).partition(|e| live.contains(&e.hash));

            // Remove dead entries from the extent index (only those pointing at
            // this segment — entries pointing elsewhere belong to another copy).
            let mut removed = 0usize;
            for entry in &dead_entries {
                if self
                    .extent_index
                    .lookup(&entry.hash)
                    .map(|loc| loc.segment_id == seg_id)
                    .unwrap_or(false)
                {
                    self.extent_index.remove(&entry.hash);
                    removed += 1;
                }
            }

            if !live_entries.is_empty() {
                // Read body bytes for live entries, then write a new denser segment.
                segment::read_extent_bodies(&seg_path, body_section_start, &mut live_entries)?;

                let new_ulid = Ulid::new().to_string();
                let pending_dir = self.base_dir.join("pending");
                let tmp_path = pending_dir.join(format!("{new_ulid}.tmp"));
                let final_path = pending_dir.join(&new_ulid);
                // write_segment reassigns stored_offset in live_entries to new positions.
                let new_bss = segment::write_segment(&tmp_path, &mut live_entries)?;
                fs::rename(&tmp_path, &final_path)?;

                for entry in &live_entries {
                    if !entry.is_dedup_ref {
                        self.extent_index.insert(
                            entry.hash,
                            extentindex::ExtentLocation {
                                segment_id: new_ulid.clone(),
                                body_offset: new_bss + entry.stored_offset,
                                body_length: entry.stored_length,
                                compressed: entry.compressed,
                            },
                        );
                    }
                }
            }

            // Evict the old segment from the file handle cache before deleting it.
            let mut cache = self.file_cache.borrow_mut();
            if cache.as_ref().map(|(id, _)| id.as_str()) == Some(seg_id.as_str()) {
                *cache = None;
            }
            drop(cache);

            fs::remove_file(&seg_path)?;

            stats.segments_compacted += 1;
            stats.bytes_freed += total_bytes - live_bytes;
            stats.extents_removed += removed;
        }

        Ok(stats)
    }

    /// Flush the current WAL to a segment in this node's `pending/`, update
    /// the extent index, and clear `pending_entries`. The WAL file is deleted.
    ///
    /// If `pending_entries` is empty (nothing written since last flush), the
    /// WAL file is deleted directly without writing a segment.
    ///
    /// Does NOT open a new WAL — the caller is responsible for that.
    fn flush_wal_to_pending(&mut self) -> io::Result<()> {
        self.wal.fsync()?;
        if self.pending_entries.is_empty() {
            fs::remove_file(&self.wal_path)?;
            return Ok(());
        }
        let body_section_start = segment::promote(
            &self.wal_path,
            &self.wal_ulid,
            &self.base_dir.join("pending"),
            &mut self.pending_entries,
        )?;
        // Update the extent index: replace temporary WAL offsets with absolute
        // offsets into the committed segment file.
        for entry in &self.pending_entries {
            if entry.is_dedup_ref {
                continue; // data lives in an ancestor segment; index already correct
            }
            self.extent_index.insert(
                entry.hash,
                extentindex::ExtentLocation {
                    segment_id: self.wal_ulid.clone(),
                    body_offset: body_section_start + entry.stored_offset,
                    body_length: entry.stored_length,
                    compressed: entry.compressed,
                },
            );
        }
        self.pending_entries.clear();
        Ok(())
    }

    /// Promote the current WAL to a pending segment, then open a fresh WAL.
    fn promote(&mut self) -> io::Result<()> {
        self.flush_wal_to_pending()?;
        // Create the fresh WAL. If this fails the segment is safe in pending/
        // and will be found on the next startup rebuild.
        let (wal, wal_ulid, wal_path, _) = create_fresh_wal(&self.base_dir.join("wal"))?;
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        Ok(())
    }

    /// Freeze the current live node and continue writing in a new child node.
    ///
    /// This is the snapshot operation (verb). After it returns:
    /// - This node is a frozen node: `wal/` is absent, `children/<ulid>/` exists,
    ///   data is preserved in `pending/` and `segments/`.
    /// - `self` now points at the new child node, which is the live continuation.
    ///   All subsequent writes go to the child.
    ///
    /// The exclusive lock transfers atomically to the child (child lock acquired
    /// before the old lock is released).
    ///
    /// Returns the path of the new child (live) node.
    pub fn snapshot(&mut self) -> io::Result<PathBuf> {
        let child_ulid = Ulid::new().to_string();
        let child_dir = self.base_dir.join("children").join(&child_ulid);

        // Create child directory structure.
        fs::create_dir_all(child_dir.join("wal"))?;
        fs::create_dir_all(child_dir.join("pending"))?;
        fs::create_dir_all(child_dir.join("segments"))?;

        // Acquire exclusive lock on the child before redirecting writes.
        let child_lock = acquire_lock(&child_dir)?;

        // Flush current WAL to a segment in this node's pending/ before freezing.
        // After this: WAL file is deleted, any data is in pending/<wal_ulid>.
        self.flush_wal_to_pending()?;

        // Freeze this node: remove the now-empty wal/ directory.
        fs::remove_dir(self.base_dir.join("wal"))?;

        // Open a fresh WAL in the child node.
        let (wal, wal_ulid, wal_path, pending_entries) = create_fresh_wal(&child_dir.join("wal"))?;

        // Transition self to the child node. Replacing lock_file releases the
        // old lock; the child lock is already held.
        let old_base = std::mem::replace(&mut self.base_dir, child_dir.clone());
        self.ancestor_dirs.push(old_base);
        self.wal = wal;
        self.wal_ulid = wal_ulid;
        self.wal_path = wal_path;
        self.pending_entries = pending_entries;
        self.lock_file = child_lock;
        *self.file_cache.borrow_mut() = None;

        Ok(child_dir)
    }

    /// Locate the segment body file for `segment_id` within this volume's
    /// node chain. Checks the live node's `wal/`, `pending/`, `segments/`
    /// first, then searches ancestor nodes' `pending/` and `segments/`.
    ///
    /// Ancestor nodes have no `wal/` (they are frozen), but may still have
    /// `pending/` segments awaiting S3 upload.
    fn find_segment_file(&self, segment_id: &str) -> io::Result<PathBuf> {
        for subdir in ["wal", "pending", "segments"] {
            let path = self.base_dir.join(subdir).join(segment_id);
            if path.exists() {
                return Ok(path);
            }
        }
        for ancestor in self.ancestor_dirs.iter().rev() {
            for subdir in ["pending", "segments"] {
                let path = ancestor.join(subdir).join(segment_id);
                if path.exists() {
                    return Ok(path);
                }
            }
        }
        Err(io::Error::other(format!("segment not found: {segment_id}")))
    }

    #[cfg(test)]
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    #[cfg(test)]
    pub fn lbamap_len(&self) -> usize {
        self.lbamap.len()
    }

    #[cfg(test)]
    pub fn promote_for_test(&mut self) -> io::Result<()> {
        self.promote()
    }
}

// --- helpers ---

/// Acquire an exclusive non-blocking flock on `<dir>/volume.lock`.
///
/// Creates the lock file if it does not exist. Returns the open `File` — the
/// lock is held for as long as this handle is open and released when dropped.
/// Returns an error immediately if the lock is already held by another process.
fn acquire_lock(dir: &Path) -> io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dir.join("volume.lock"))?;
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(file)
    }
}

/// Walk up the directory tree from `base_dir` and return all ancestor node
/// directories, oldest-first. `base_dir` itself is NOT included.
///
/// A non-root live node lives at `<parent>/children/<ulid>/`. This function
/// detects that pattern and climbs until it reaches the root (a directory
/// whose parent is not named `children`).
pub fn walk_ancestors(base_dir: &Path) -> Vec<PathBuf> {
    let mut ancestors = Vec::new();
    let mut cur = base_dir.to_owned();
    while let Some(children_dir) = cur.parent().map(|p| p.to_owned()) {
        if children_dir
            .file_name()
            .map(|n| n != "children")
            .unwrap_or(true)
        {
            break;
        }
        let Some(parent_node) = children_dir.parent().map(|p| p.to_owned()) else {
            break;
        };
        ancestors.push(parent_node.clone());
        cur = parent_node;
    }
    ancestors.reverse();
    ancestors
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
    String,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let ulid_str = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::other("bad WAL filename"))?;
    let ulid = Ulid::from_string(ulid_str)
        .map_err(|e| io::Error::other(e.to_string()))?
        .to_string();

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
                let compressed = flags & writelog::FLAG_COMPRESSED != 0;
                lbamap.insert(start_lba, lba_length, hash);
                // Temporary WAL offset — updated to segment offset on promotion.
                extent_index.insert(
                    hash,
                    extentindex::ExtentLocation {
                        segment_id: ulid.clone(),
                        body_offset,
                        body_length,
                        compressed,
                    },
                );
                pending_entries.push(segment::SegmentEntry::new_data(
                    hash, start_lba, lba_length, flags, data,
                ));
            }
            writelog::LogRecord::Ref {
                hash,
                start_lba,
                lba_length,
            } => {
                lbamap.insert(start_lba, lba_length, hash);
                // Data lives in an ancestor segment; extent index already has it
                // from rebuild_segments(). Just add to pending_entries so the
                // dedup-ref is preserved in the next promoted segment.
                pending_entries.push(segment::SegmentEntry::new_dedup_ref(
                    hash, start_lba, lba_length,
                ));
            }
        }
    }

    let wal = writelog::WriteLog::reopen(&path, valid_size)?;
    Ok((wal, ulid, path, pending_entries))
}

/// Create a new WAL file with a fresh ULID.
fn create_fresh_wal(
    wal_dir: &Path,
) -> io::Result<(
    writelog::WriteLog,
    String,
    PathBuf,
    Vec<segment::SegmentEntry>,
)> {
    let ulid = Ulid::new().to_string();
    let path = wal_dir.join(&ulid);
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

    #[test]
    fn open_creates_directories() {
        let base = temp_dir();
        let _ = Volume::open(&base).unwrap();
        assert!(base.join("wal").is_dir());
        assert!(base.join("pending").is_dir());
        assert!(base.join("segments").is_dir());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_is_idempotent() {
        let base = temp_dir();
        let _ = Volume::open(&base).unwrap();
        // Second open on the same dir should succeed (dirs already exist).
        let _ = Volume::open(&base).unwrap();
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_single_block() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        vol.write(0, &vec![0x42u8; 4096]).unwrap();
        vol.fsync().unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_multi_block_extent() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        // Write 8 LBAs (32 KiB) as a single call.
        vol.write(10, &vec![0xabu8; 8 * 4096]).unwrap();
        assert_eq!(vol.lbamap_len(), 1);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_empty() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        let err = vol.write(0, &[]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_rejects_misaligned() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        let err = vol.write(0, &[0u8; 1000]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn write_promotes_on_flush() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();

        // Write two blocks, fsync, then drop (simulates clean shutdown before promotion).
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen — lbamap should contain both blocks.
        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.lbamap_len(), 2);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_unwritten_returns_zeros() {
        let base = temp_dir();
        let vol = Volume::open(&base).unwrap();
        let data = vol.read(0, 4).unwrap();
        assert_eq!(data.len(), 4 * 4096);
        assert!(data.iter().all(|&b| b == 0));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn read_written_data_same_session() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();

        let payload = vec![0x77u8; 4096];
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(3, &payload).unwrap();
            vol.fsync().unwrap();
        }

        // Reopen: WAL recovery must restore both the LBA map and extent index.
        let vol = Volume::open(&base).unwrap();
        let result = vol.read(3, 1).unwrap();
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
        let base = temp_dir();

        {
            let mut vol = Volume::open(&base).unwrap();
            let block = vec![0u8; 1024 * 1024]; // 1 MiB = 256 LBAs
            for i in 0u64..33 {
                vol.write(i * 256, &block).unwrap();
            }
            vol.fsync().unwrap();
        }

        // All 33 extents should survive across the promotion boundary.
        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.lbamap_len(), 33);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn promotion_after_wal_recovery() {
        // Write to the WAL, drop (simulating a crash), reopen (WAL recovery),
        // promote, then reopen again — verifies that pending_entries is correctly
        // rebuilt from the recovered WAL so the segment contains the pre-crash writes.
        let base = temp_dir();

        // Phase 1: write two blocks, fsync, drop.
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![1u8; 4096]).unwrap();
            vol.write(1, &vec![2u8; 4096]).unwrap();
            vol.fsync().unwrap();
        }

        // Phase 2: recover and immediately promote.
        {
            let mut vol = Volume::open(&base).unwrap();
            assert_eq!(vol.lbamap_len(), 2); // confirm recovery
            vol.promote_for_test().unwrap();
        }

        // Phase 3: reopen — both blocks must now come from the pending/ segment.
        let vol = Volume::open(&base).unwrap();
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
        let base = temp_dir();

        // Phase 1: write two blocks and promote so a segment lands in pending/.
        let ulid;
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![0xaau8; 4096]).unwrap();
            vol.write(1, &vec![0xbbu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            // Grab the segment ULID (there is exactly one file in pending/).
            ulid = fs::read_dir(base.join("pending"))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .into_owned();
        }

        // Simulate the crash: copy the segment back as a WAL file so both exist.
        fs::copy(
            base.join("pending").join(&ulid),
            base.join("wal").join(&ulid),
        )
        .unwrap();

        // Reopen — should delete the stale WAL and load cleanly from the segment.
        let vol = Volume::open(&base).unwrap();
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

    // --- compaction tests ---

    #[test]
    fn compact_noop_when_all_live() {
        // Write two blocks, promote, compact — nothing should be compacted
        // since all data is still referenced.
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.write(1, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.compact(0.7).unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.bytes_freed, 0);
        assert_eq!(stats.extents_removed, 0);

        // Data still readable.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x11u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn compact_reclaims_overwritten_extent() {
        // Write block A, promote, overwrite block A with B, promote.
        // First segment now has a dead extent; compaction should reclaim it.
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        let original = vec![0x11u8; 4096];
        let replacement = vec![0x22u8; 4096];

        vol.write(0, &original).unwrap();
        vol.promote_for_test().unwrap();

        vol.write(0, &replacement).unwrap();
        vol.promote_for_test().unwrap();

        // Two segments: first is 100% dead, second is live.
        let stats = vol.compact(0.7).unwrap();
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
    fn compact_reads_back_correctly_after_reopen() {
        // Verify that the compacted segment is a valid segment that survives
        // a volume reopen (LBA map rebuild + extent index rebuild).
        let base = temp_dir();

        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &vec![0xAAu8; 4096]).unwrap();
            vol.promote_for_test().unwrap();
            vol.write(0, &vec![0xBBu8; 4096]).unwrap(); // overwrite
            vol.promote_for_test().unwrap();
            vol.compact(0.7).unwrap();
        }

        let vol = Volume::open(&base).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), vec![0xBBu8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn compact_partial_segment() {
        // Segment has two extents; one is overwritten (dead), one is live.
        // Compaction should rewrite the segment keeping only the live extent.
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap(); // will be overwritten
        vol.write(1, &vec![0x22u8; 4096]).unwrap(); // stays live
        vol.promote_for_test().unwrap();

        vol.write(0, &vec![0x33u8; 4096]).unwrap(); // overwrites LBA 0
        vol.promote_for_test().unwrap();

        // First segment is 50% dead — above default threshold of 30% dead (0.7 live).
        let stats = vol.compact(0.7).unwrap();
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_freed > 0);

        // Both LBAs read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x33u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn compact_respects_min_live_ratio() {
        // With a strict ratio (1.0), any dead byte triggers compaction.
        // With a lenient ratio (0.0), nothing is ever compacted.
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap(); // LBA 0 now dead in seg 1
        vol.promote_for_test().unwrap();

        // Lenient threshold: first segment is 100% dead but ratio=0.0 → nothing compacted.
        let stats = vol.compact(0.0).unwrap();
        assert_eq!(stats.segments_compacted, 0);

        // Strict threshold: compact anything with any dead bytes.
        let stats = vol.compact(1.0).unwrap();
        assert_eq!(stats.segments_compacted, 1);

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
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();

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
        let base = temp_dir();

        {
            let mut vol = Volume::open(&base).unwrap();
            let data = vec![0xABu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            // Second write: same data, different LBA → dedup hit, REF record in WAL.
            vol.write(1, &data).unwrap();
            vol.fsync().unwrap();
        }

        let vol = Volume::open(&base).unwrap();
        let data = vec![0xABu8; 4096];
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn dedup_ref_in_segment_survives_reopen() {
        // Write data, promote, write same data (REF in WAL), promote again so
        // the REF lands in a segment. Reopen and verify reads still work.
        let base = temp_dir();

        {
            let mut vol = Volume::open(&base).unwrap();
            let data = vec![0xCDu8; 4096];
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
            vol.write(1, &data).unwrap(); // REF
            vol.promote_for_test().unwrap(); // REF lands in segment
        }

        let vol = Volume::open(&base).unwrap();
        let data = vec![0xCDu8; 4096];
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    // --- walk_ancestors tests ---

    #[test]
    fn walk_ancestors_root_returns_empty() {
        let root = temp_dir();
        assert!(walk_ancestors(&root).is_empty());
    }

    #[test]
    fn walk_ancestors_one_level() {
        let root = temp_dir();
        let child = root.join("children").join("01CHILDULID000000000000000");
        let ancestors = walk_ancestors(&child);
        assert_eq!(ancestors, vec![root]);
    }

    #[test]
    fn walk_ancestors_two_levels() {
        let root = temp_dir();
        let child = root.join("children").join("01CHILDULID000000000000000");
        let grandchild = child.join("children").join("01GRANDULID000000000000000");
        let ancestors = walk_ancestors(&grandchild);
        assert_eq!(ancestors, vec![root, child]);
    }

    // --- ancestor-aware open / read integration test ---

    /// Set up a frozen ancestor node with a written extent, then open a child
    /// live node on top. The child should be able to read the ancestor's data.
    #[test]
    fn open_reads_ancestor_segments() {
        let root = temp_dir();
        let child_dir = root.join("children").join("01CHILDULID000000000000000");

        // Write data into the ancestor (root) and promote to a segment.
        let data = vec![0xABu8; 4096];
        {
            let mut vol = Volume::open(&root).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
        }
        // Freeze the ancestor: remove wal/ to simulate a node after snapshot() was called.
        fs::remove_dir_all(root.join("wal")).unwrap();

        // Open the child node (empty — no local writes yet).
        let vol = Volume::open(&child_dir).unwrap();

        // Child should see the ancestor's data through layer merge.
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(root).unwrap();
    }

    /// Ancestor data is shadowed by a write in the live child node.
    #[test]
    fn child_write_shadows_ancestor() {
        let root = temp_dir();
        let child_dir = root.join("children").join("01CHILDULID000000000000000");

        let ancestor_data = vec![0xAAu8; 4096];
        let child_data = vec![0xBBu8; 4096];

        // Write into ancestor, promote, freeze.
        {
            let mut vol = Volume::open(&root).unwrap();
            vol.write(0, &ancestor_data).unwrap();
            vol.promote_for_test().unwrap();
        }
        fs::remove_dir_all(root.join("wal")).unwrap();

        // Open child, write different data at the same LBA, promote.
        {
            let mut vol = Volume::open(&child_dir).unwrap();
            vol.write(0, &child_data).unwrap();
            vol.promote_for_test().unwrap();
        }

        // Re-open child and verify child data wins.
        let vol = Volume::open(&child_dir).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), child_data);

        fs::remove_dir_all(root).unwrap();
    }

    // --- lock tests ---

    #[test]
    fn double_open_same_node_fails() {
        let base = temp_dir();
        let _vol = Volume::open(&base).unwrap();
        // Second open on the same live node must fail (lock already held).
        assert!(Volume::open(&base).is_err());
        fs::remove_dir_all(base).unwrap();
    }

    // --- snapshot() operation tests ---

    #[test]
    fn snapshot_freezes_parent_and_continues_in_child() {
        let base = temp_dir();
        let data = vec![0xAAu8; 4096];

        let mut vol = Volume::open(&base).unwrap();
        vol.write(0, &data).unwrap();

        let child_path = vol.snapshot().unwrap();

        // Parent wal/ is gone (frozen).
        assert!(!base.join("wal").exists());
        // Child wal/ exists.
        assert!(child_path.join("wal").is_dir());
        // child_path is under base/children/.
        assert_eq!(child_path.parent().unwrap(), base.join("children"));

        // Writes after snapshot go to the child.
        let new_data = vec![0xBBu8; 4096];
        vol.write(1, &new_data).unwrap();

        // Both LBAs readable.
        assert_eq!(vol.read(0, 1).unwrap(), data);
        assert_eq!(vol.read(1, 1).unwrap(), new_data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn snapshot_data_readable_after_reopen_of_child() {
        let base = temp_dir();
        let pre_snap = vec![0xAAu8; 4096];
        let post_snap = vec![0xBBu8; 4096];

        let child_path = {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &pre_snap).unwrap();
            let child = vol.snapshot().unwrap();
            vol.write(1, &post_snap).unwrap();
            vol.promote_for_test().unwrap();
            child
        };

        // Re-open the child: should see both pre- and post-snapshot data.
        let vol = Volume::open(&child_path).unwrap();
        assert_eq!(vol.read(0, 1).unwrap(), pre_snap);
        assert_eq!(vol.read(1, 1).unwrap(), post_snap);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn snapshot_lock_transfers_to_child() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        let child_path = vol.snapshot().unwrap();

        // Base is now a frozen node (wal/ gone, lock released).
        // Verify the child node is locked by attempting a second open on it.
        let _vol2_child_fail = Volume::open(&child_path);
        assert!(_vol2_child_fail.is_err());

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_frozen_node_creates_new_child() {
        let base = temp_dir();
        let data = vec![0xAAu8; 4096];

        // Write, call snapshot(), drop — base is now a frozen node.
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.write(0, &data).unwrap();
            vol.snapshot().unwrap();
        }

        // Opening the frozen base should silently create a new child.
        let vol = Volume::open(&base).unwrap();
        // The live node is under base/children/, not base itself.
        assert!(vol.base_dir().starts_with(base.join("children")));
        // Ancestor data is visible.
        assert_eq!(vol.read(0, 1).unwrap(), data);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn open_frozen_node_two_callers_get_siblings() {
        let base = temp_dir();

        // Call snapshot() to freeze base.
        {
            let mut vol = Volume::open(&base).unwrap();
            vol.snapshot().unwrap();
        }

        // Two independent opens of the frozen base should produce two
        // distinct sibling live nodes (different ULIDs, both under children/).
        let vol_a = Volume::open(&base).unwrap();
        let vol_b = Volume::open(&base).unwrap();
        assert_ne!(vol_a.base_dir(), vol_b.base_dir());
        assert!(vol_a.base_dir().starts_with(base.join("children")));
        assert!(vol_b.base_dir().starts_with(base.join("children")));

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn snapshot_empty_wal_no_segment_written() {
        let base = temp_dir();
        let mut vol = Volume::open(&base).unwrap();
        // No writes — WAL is empty.
        vol.snapshot().unwrap();

        // Parent pending/ should be empty (no segment written for empty WAL).
        let pending: Vec<_> = fs::read_dir(base.join("pending")).unwrap().collect();
        assert!(pending.is_empty());

        fs::remove_dir_all(base).unwrap();
    }
}
