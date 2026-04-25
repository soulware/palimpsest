//! Sweep / repack / delta-repack data types and the `impl Volume` blocks
//! that drive them.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use ulid::Ulid;

use crate::{
    extentindex::{self, BodySource},
    lbamap,
    segment::{self, EntryKind},
    segment_cache,
};

use super::{Volume, latest_snapshot};

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

/// Stats from a single `delta_repack_post_snapshot` pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct DeltaRepackStats {
    /// Number of post-snapshot segments inspected.
    pub segments_scanned: usize,
    /// Number of segments actually rewritten (had at least one conversion).
    pub segments_rewritten: usize,
    /// Total Data→Delta conversions across all rewritten segments.
    pub entries_converted: usize,
    /// Sum of original `stored_length` for converted entries.
    pub original_body_bytes: u64,
    /// Sum of delta blob sizes written.
    pub delta_body_bytes: u64,
}

/// Data needed by the worker to compact small / dead-bearing segments in
/// `pending/`. Produced by [`super::Volume::prepare_sweep`] on the actor thread.
///
/// `lbamap` is an `Arc` snapshot used by the worker to make liveness
/// decisions for `DedupRef` entries (`hash_at(lba)` queries). Concurrent
/// writes after prep are not visible to the worker — the apply phase
/// uses CAS on the source `(segment_id, body_offset)` pair, which makes
/// the conservative liveness snapshot safe (we may keep a now-dead hash
/// alive for one more cycle, never the reverse).
pub struct SweepJob {
    pub lbamap: Arc<lbamap::LbaMap>,
    pub floor: Option<Ulid>,
    pub pending_dir: PathBuf,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// A live entry carried into the swept output, paired with the CAS
/// preconditions from its source segment. Apply uses
/// `replace_if_matches(hash, source_segment_id, source_body_offset, ..)`
/// so a concurrent overwrite leaves the index untouched.
pub struct SweptLiveEntry {
    pub entry: segment::SegmentEntry,
    pub source_segment_id: Ulid,
    pub source_body_offset: u64,
}

/// A dead entry dropped by sweep, paired with the CAS preconditions from
/// its source segment. Apply uses `remove_if_matches`.
///
/// Only Data and Inline entries appear here — Zero, DedupRef, and Delta
/// entries are not in the `inner` extent index.
pub struct SweptDeadEntry {
    pub hash: blake3::Hash,
    pub source_segment_id: Ulid,
    pub source_body_offset: u64,
}

/// Result of a [`SweepJob`]. Consumed by [`super::Volume::apply_sweep_result`]
/// on the actor thread.
///
/// `new_ulid` is `None` when the merged-live set was empty (every input
/// was fully dead) — in that case the apply phase still runs the dead
/// removals and deletes the candidate files. `candidate_paths` lists the
/// inputs to evict from the file cache and unlink. The candidate whose
/// ULID equals `new_ulid` was already replaced atomically by the rename
/// inside the worker; apply must skip its `remove_file` while still
/// evicting its cached fd.
pub struct SweepResult {
    pub stats: CompactionStats,
    pub new_ulid: Option<Ulid>,
    pub new_body_section_start: u64,
    pub merged_live: Vec<SweptLiveEntry>,
    pub dead_entries: Vec<SweptDeadEntry>,
    pub candidate_paths: Vec<PathBuf>,
}

/// Data needed by the worker to repack sparse segments in `pending/`.
/// Produced by [`super::Volume::prepare_repack`] on the actor thread.
///
/// Same shape as [`SweepJob`] plus `min_live_ratio` — the worker iterates
/// every non-floor segment, recomputes liveness against the `lbamap`
/// snapshot, and rewrites (in place, reusing the input ULID) any segment
/// whose live ratio falls below the threshold.
pub struct RepackJob {
    pub lbamap: Arc<lbamap::LbaMap>,
    pub floor: Option<Ulid>,
    pub pending_dir: PathBuf,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
    pub min_live_ratio: f64,
}

/// A live entry carried into the repacked output, paired with the CAS
/// precondition from its source segment. Apply uses
/// `replace_if_matches(hash, seg_id, source_body_offset, ..)` — the
/// output reuses the same ULID, so only `body_offset` changes on success.
pub struct RepackedLiveEntry {
    pub entry: segment::SegmentEntry,
    pub source_body_offset: u64,
}

/// A dead entry dropped by repack, paired with the CAS precondition from
/// its source segment. Apply uses `remove_if_matches`. Only Data and
/// Inline entries appear here — Zero, DedupRef, and Delta are thin
/// entries with no extent-index slot.
pub struct RepackedDeadEntry {
    pub hash: blake3::Hash,
    pub source_body_offset: u64,
}

/// Per-segment payload from a [`RepackJob`]. One of these is produced for
/// every segment the worker rewrote or deleted.
///
/// When `all_dead_deleted` is `true`, the worker has already `remove_file`d
/// the segment; `live` is empty and `new_body_section_start` is 0. When
/// `false`, the worker renamed a fresh `.tmp` over the original file, so
/// `new_body_section_start` and the `live` entries (with post-write
/// offsets) are valid.
pub struct RepackedSegment {
    pub seg_id: Ulid,
    pub new_body_section_start: u64,
    pub live: Vec<RepackedLiveEntry>,
    pub dead: Vec<RepackedDeadEntry>,
    pub all_dead_deleted: bool,
    pub bytes_freed: u64,
}

/// Result of a [`RepackJob`]. Consumed by [`super::Volume::apply_repack_result`]
/// on the actor thread.
pub struct RepackResult {
    pub stats: CompactionStats,
    pub segments: Vec<RepackedSegment>,
}

/// Data needed by the worker to rewrite post-snapshot pending segments
/// with zstd-dictionary deltas against the prior sealed snapshot.
///
/// Produced by [`super::Volume::prepare_delta_repack`] on the actor thread.
///
/// `snap_ulid` is the latest sealed snapshot: only segments with a
/// strictly greater ULID are rewritten; the snapshot itself is frozen.
/// The worker constructs a snapshot-pinned `BlockReader` from
/// `base_dir` + `snap_ulid` — kept off the actor so the manifest /
/// provenance / extent-index rebuild runs on the worker thread.
pub struct DeltaRepackJob {
    pub base_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub snap_ulid: Ulid,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Per-segment payload from a [`DeltaRepackJob`]. One of these is
/// produced for every segment the worker actually rewrote
/// (segments that had no convertible entries are skipped).
pub struct DeltaRepackedSegment {
    pub seg_id: Ulid,
    pub rewrite: crate::delta_compute::RewrittenSegment,
}

/// Result of a [`DeltaRepackJob`]. Consumed by
/// [`super::Volume::apply_delta_repack_result`] on the actor thread.
pub struct DeltaRepackResult {
    pub stats: DeltaRepackStats,
    pub segments: Vec<DeltaRepackedSegment>,
}

impl Volume {
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
    ///
    /// Synchronous wrapper around [`Self::prepare_repack`] +
    /// [`crate::actor::execute_repack`] + [`Self::apply_repack_result`].
    /// The actor offloads the middle phase to the worker thread; callers
    /// that hold a `&mut Volume` directly (tests, tools) can keep using
    /// this wrapper.
    pub fn repack(&mut self, min_live_ratio: f64) -> io::Result<CompactionStats> {
        let Some(job) = self.prepare_repack(min_live_ratio)? else {
            return Ok(CompactionStats::default());
        };
        let result = crate::actor::execute_repack(job)?;
        self.apply_repack_result(result)
    }

    /// Prep phase of `repack` — runs on the actor thread.
    ///
    /// Snapshots `lbamap` (used by the worker to recompute liveness),
    /// resolves the snapshot floor, and packages the directory + signer
    /// state into a [`RepackJob`]. Returns `None` when `pending/` is
    /// missing or has no segments.
    ///
    /// All segment reads, eligibility decisions, and rewrites run in
    /// [`crate::actor::execute_repack`]. The apply phase
    /// [`Self::apply_repack_result`] does the conditional extent-index
    /// updates and file-cache eviction.
    pub fn prepare_repack(&self, min_live_ratio: f64) -> io::Result<Option<RepackJob>> {
        let pending_dir = self.base_dir.join("pending");
        let segs = match segment::collect_segment_files(&pending_dir) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if segs.is_empty() {
            return Ok(None);
        }
        let floor = latest_snapshot(&self.base_dir)?;
        Ok(Some(RepackJob {
            lbamap: Arc::clone(&self.lbamap),
            floor,
            pending_dir,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
            min_live_ratio,
        }))
    }

    /// Apply phase of `repack` — runs on the actor thread after the
    /// worker returns.
    ///
    /// For each rewritten segment: re-points live Data/Inline entries via
    /// `replace_if_matches((hash, seg_id, source_body_offset) → new_loc)`,
    /// where the new location reuses the same `seg_id` but with the
    /// post-write `body_offset`. A concurrent writer that superseded the
    /// hash between prep and apply fails the CAS and is preserved
    /// untouched — the repacked body simply becomes unreferenced until
    /// the next pass picks it up.
    ///
    /// Dead entries are dropped with `remove_if_matches` for the same
    /// reason. File-cache eviction runs for every touched segment so a
    /// cached fd on the old inode cannot serve stale offsets after the
    /// rename.
    pub fn apply_repack_result(&mut self, result: RepackResult) -> io::Result<CompactionStats> {
        let RepackResult {
            mut stats,
            segments,
        } = result;

        for seg in &segments {
            for dead in &seg.dead {
                if Arc::make_mut(&mut self.extent_index).remove_if_matches(
                    &dead.hash,
                    seg.seg_id,
                    dead.source_body_offset,
                ) {
                    stats.extents_removed += 1;
                }
            }

            self.evict_cached_segment(seg.seg_id);

            if !seg.all_dead_deleted {
                for live in &seg.live {
                    let entry = &live.entry;
                    let inline_data = match entry.kind {
                        EntryKind::Data | EntryKind::CanonicalData => None,
                        EntryKind::Inline | EntryKind::CanonicalInline => {
                            entry.data.clone().map(Vec::into_boxed_slice)
                        }
                        EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
                    };
                    Arc::make_mut(&mut self.extent_index).replace_if_matches(
                        entry.hash,
                        seg.seg_id,
                        live.source_body_offset,
                        extentindex::ExtentLocation {
                            segment_id: seg.seg_id,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Local,
                            body_section_start: seg.new_body_section_start,
                            inline_data,
                        },
                    );
                }
            }
        }

        Ok(stats)
    }

    /// Phase 5 Tier 1: rewrite post-snapshot pending segments with
    /// zstd-dictionary deltas against same-LBA extents from the prior
    /// sealed snapshot.
    ///
    /// For every segment in `pending/` whose ULID is greater than the
    /// latest sealed snapshot, walks single-block `Data` entries and
    /// looks up the LBA in a snapshot-pinned `BlockReader` on that
    /// snapshot. If the prior snapshot holds a different extent at
    /// that LBA and the source body is locally available, the entry
    /// is converted to a thin `Delta` with the prior extent as its
    /// dictionary source.
    ///
    /// Runs on post-snapshot segments only — never touches segments
    /// that are part of a sealed snapshot. No-op when there is no
    /// sealed snapshot (nothing to source deltas from) or when no
    /// entries match.
    ///
    /// Synchronous wrapper around [`Self::prepare_delta_repack`] +
    /// [`crate::actor::execute_delta_repack`] +
    /// [`Self::apply_delta_repack_result`]. The actor uses the three
    /// phases directly so that the heavy middle phase runs off the
    /// request channel; this wrapper exists for tests and any inline
    /// callers.
    pub fn delta_repack_post_snapshot(&mut self) -> io::Result<DeltaRepackStats> {
        let Some(job) = self.prepare_delta_repack()? else {
            return Ok(DeltaRepackStats::default());
        };
        let result = crate::actor::execute_delta_repack(job)?;
        self.apply_delta_repack_result(result)
    }

    /// Prep phase of `delta_repack_post_snapshot` — runs on the actor
    /// thread.
    ///
    /// Resolves the latest sealed snapshot and packages the signer
    /// state + segment-cache handle + base/pending directories into a
    /// [`DeltaRepackJob`]. Returns `None` when there is no sealed
    /// snapshot (nothing to source deltas from); the worker dispatch is
    /// then skipped.
    ///
    /// The snapshot-pinned `BlockReader` is constructed by the worker,
    /// not here — its provenance walk and extent-index rebuild can run
    /// off the actor.
    pub fn prepare_delta_repack(&self) -> io::Result<Option<DeltaRepackJob>> {
        let Some(snap_ulid) = latest_snapshot(&self.base_dir)? else {
            return Ok(None);
        };
        Ok(Some(DeltaRepackJob {
            base_dir: self.base_dir.clone(),
            pending_dir: self.base_dir.join("pending"),
            snap_ulid,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `delta_repack_post_snapshot` — runs on the actor
    /// thread after the worker returns.
    ///
    /// For each rewritten segment: evicts the fd cache, then walks
    /// entries paired with their pre-rewrite `(kind, stored_offset)`.
    /// CAS against the source location so a concurrent writer that
    /// re-pointed a hash at a newer segment wins — we leave their
    /// entry untouched, and the repacked body simply becomes
    /// unreferenced until the next pass picks it up.
    ///
    /// - Data kept as Data, Inline kept as Inline:
    ///   `replace_if_matches(hash, seg_id, pre_offset, new_loc)`.
    /// - Data converted to Delta:
    ///   `remove_if_matches(hash, seg_id, pre_offset)` followed by
    ///   `insert_delta_if_absent(...)`.
    /// - Pre-existing Delta: re-register via `insert_delta_if_absent`
    ///   — no-op if the delta slot is still valid, which matches the
    ///   pre-offload behaviour.
    pub fn apply_delta_repack_result(
        &mut self,
        result: DeltaRepackResult,
    ) -> io::Result<DeltaRepackStats> {
        let DeltaRepackResult {
            mut stats,
            segments,
        } = result;

        for seg in segments {
            let DeltaRepackedSegment { seg_id, rewrite } = seg;
            let crate::delta_compute::RewrittenSegment {
                entries,
                new_body_section_start: new_bss,
                delta_region_body_length,
                stats: seg_stats,
            } = rewrite;

            // Evict the file-cache fd — the segment file was swapped
            // atomically by the worker's rename. A surviving cached fd
            // on the old inode would serve reads with new offsets after
            // the index update below lands.
            self.evict_cached_segment(seg_id);

            let entries_len = entries.len();
            let ei = Arc::make_mut(&mut self.extent_index);
            for item in &entries {
                let post = &item.post;
                match (item.pre_kind, post.kind) {
                    (EntryKind::Data, EntryKind::Data) => {
                        ei.replace_if_matches(
                            post.hash,
                            seg_id,
                            item.pre_stored_offset,
                            extentindex::ExtentLocation {
                                segment_id: seg_id,
                                body_offset: post.stored_offset,
                                body_length: post.stored_length,
                                compressed: post.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: None,
                            },
                        );
                    }
                    (EntryKind::Inline, EntryKind::Inline) => {
                        ei.replace_if_matches(
                            post.hash,
                            seg_id,
                            item.pre_stored_offset,
                            extentindex::ExtentLocation {
                                segment_id: seg_id,
                                body_offset: post.stored_offset,
                                body_length: post.stored_length,
                                compressed: post.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: post.data.clone().map(Vec::into_boxed_slice),
                            },
                        );
                    }
                    (EntryKind::Data, EntryKind::Delta) => {
                        ei.remove_if_matches(&post.hash, seg_id, item.pre_stored_offset);
                        ei.insert_delta_if_absent(
                            post.hash,
                            extentindex::DeltaLocation {
                                segment_id: seg_id,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: post.delta_options.clone(),
                            },
                        );
                        let sources: Arc<[blake3::Hash]> =
                            post.delta_options.iter().map(|o| o.source_hash).collect();
                        Arc::make_mut(&mut self.lbamap).set_delta_sources_if_matches(
                            post.start_lba,
                            post.hash,
                            sources,
                        );
                    }
                    (EntryKind::Delta, EntryKind::Delta) => {
                        // Pre-existing delta: re-register so a reader
                        // hitting this hash sees the updated
                        // body_section_start. `_if_absent` is a no-op
                        // when the slot still exists — matches the
                        // pre-offload behaviour.
                        ei.insert_delta_if_absent(
                            post.hash,
                            extentindex::DeltaLocation {
                                segment_id: seg_id,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: post.delta_options.clone(),
                            },
                        );
                        let sources: Arc<[blake3::Hash]> =
                            post.delta_options.iter().map(|o| o.source_hash).collect();
                        Arc::make_mut(&mut self.lbamap).set_delta_sources_if_matches(
                            post.start_lba,
                            post.hash,
                            sources,
                        );
                    }
                    (EntryKind::DedupRef, _) | (EntryKind::Zero, _) => {}
                    // Kind transitions other than Data→Data, Data→Delta,
                    // Inline→Inline, Delta→Delta aren't produced by
                    // `rewrite_post_snapshot_with_prior`; ignore rather
                    // than assert so a future rewriter extension can't
                    // crash the actor.
                    _ => {}
                }
            }

            log::info!(
                "delta_repack: seg {seg_id} converted {}/{} entries, {}→{} bytes",
                seg_stats.entries_converted,
                entries_len,
                seg_stats.original_body_bytes,
                seg_stats.delta_body_bytes,
            );

            stats.segments_rewritten += 1;
            stats.entries_converted += seg_stats.entries_converted;
            stats.original_body_bytes += seg_stats.original_body_bytes;
            stats.delta_body_bytes += seg_stats.delta_body_bytes;
        }

        Ok(stats)
    }

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
    ///
    /// Synchronous wrapper around the offloadable prep / execute / apply
    /// trio. The actor uses [`Self::prepare_sweep`] +
    /// [`crate::actor::execute_sweep`] + [`Self::apply_sweep_result`]
    /// directly so that the worker thread runs the heavy middle phase off
    /// the request channel; this wrapper exists for tests and any
    /// remaining inline callers.
    pub fn sweep_pending(&mut self) -> io::Result<CompactionStats> {
        let Some(job) = self.prepare_sweep()? else {
            return Ok(CompactionStats::default());
        };
        let result = crate::actor::execute_sweep(job)?;
        self.apply_sweep_result(result)
    }

    /// Prep phase of `sweep_pending` — runs on the actor thread.
    ///
    /// Snapshots `lbamap` (used by the worker for `DedupRef` liveness),
    /// resolves the snapshot floor, and packages the directory + signer
    /// state into a [`SweepJob`]. Returns `None` when `pending/` is
    /// missing or has no segments — there is nothing for the worker to
    /// do, so the dispatch is skipped.
    ///
    /// All actual segment reads, partitioning, and the rewrite happen in
    /// [`crate::actor::execute_sweep`]. The apply phase
    /// [`Self::apply_sweep_result`] does the conditional extent-index
    /// updates and candidate deletion.
    pub fn prepare_sweep(&self) -> io::Result<Option<SweepJob>> {
        let pending_dir = self.base_dir.join("pending");
        let segs = match segment::collect_segment_files(&pending_dir) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if segs.is_empty() {
            return Ok(None);
        }
        let floor = latest_snapshot(&self.base_dir)?;
        Ok(Some(SweepJob {
            lbamap: Arc::clone(&self.lbamap),
            floor,
            pending_dir,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `sweep_pending` — runs on the actor thread after
    /// the worker returns.
    ///
    /// Drops dead Data/Inline entries from the extent index using
    /// [`extentindex::ExtentIndex::remove_if_matches`] so a concurrent
    /// writer that re-pointed the hash at a newer segment is left
    /// untouched. Re-points carried-live entries with
    /// [`extentindex::ExtentIndex::replace_if_matches`] for the same
    /// reason — a CAS miss simply means the new write wins (apply does
    /// not insert, leaving the body unreferenced until the next sweep,
    /// which is the conservative direction).
    ///
    /// File-cache eviction and candidate deletion run last. The
    /// candidate whose ULID equals `new_ulid` was already replaced
    /// atomically by the worker's `tmp` → final rename; apply skips its
    /// `remove_file` while still evicting the cached fd (the old inode
    /// was unlinked and a surviving handle would seek into stale
    /// offsets after the publish).
    pub fn apply_sweep_result(&mut self, result: SweepResult) -> io::Result<CompactionStats> {
        let SweepResult {
            mut stats,
            new_ulid,
            new_body_section_start,
            merged_live,
            dead_entries,
            candidate_paths,
        } = result;

        for dead in &dead_entries {
            if Arc::make_mut(&mut self.extent_index).remove_if_matches(
                &dead.hash,
                dead.source_segment_id,
                dead.source_body_offset,
            ) {
                stats.extents_removed += 1;
            }
        }

        if let Some(new_ulid) = new_ulid {
            for live in &merged_live {
                let entry = &live.entry;
                let inline_data = match entry.kind {
                    EntryKind::Data | EntryKind::CanonicalData => None,
                    EntryKind::Inline | EntryKind::CanonicalInline => {
                        entry.data.clone().map(Vec::into_boxed_slice)
                    }
                    EntryKind::DedupRef | EntryKind::Zero | EntryKind::Delta => continue,
                };
                Arc::make_mut(&mut self.extent_index).replace_if_matches(
                    entry.hash,
                    live.source_segment_id,
                    live.source_body_offset,
                    extentindex::ExtentLocation {
                        segment_id: new_ulid,
                        body_offset: entry.stored_offset,
                        body_length: entry.stored_length,
                        compressed: entry.compressed,
                        body_source: BodySource::Local,
                        body_section_start: new_body_section_start,
                        inline_data,
                    },
                );
            }
        }

        for seg_path in &candidate_paths {
            let seg_ulid_opt = seg_path
                .file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| Ulid::from_string(s).ok());
            if let Some(ulid) = seg_ulid_opt {
                self.file_cache.borrow_mut().evict(ulid);
            }
            if seg_ulid_opt == new_ulid {
                // Already replaced atomically by the worker's rename;
                // re-deleting would unlink the new content.
                continue;
            }
            match fs::remove_file(seg_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        Ok(stats)
    }
}
