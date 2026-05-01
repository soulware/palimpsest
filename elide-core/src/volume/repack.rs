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
#[derive(Debug, Default, Clone, Copy, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Default, Clone, Copy, serde::Serialize, serde::Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use std::fs;

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
        simulate_upload(&mut vol);

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
        simulate_upload(&mut vol);

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
    fn sweep_pending_multi_block_inplace_overwrite_same_wal() {
        // Regression: two multi-block DATA writes at the same LBA range in the
        // same WAL flush. Both land as DATA entries (different hashes) in one
        // pending segment. sweep_pending then partitions entries into live/dead,
        // rewrites the segment, and updates the extent index — the surviving
        // live entry must read back correctly from the rewritten segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // High-entropy so neither payload is inlined and both stay in the
        // body section. Eight 4 KiB blocks each.
        let payload_a: Vec<u8> = (0..8 * 4096usize).map(|i| (i * 7 + 13) as u8).collect();
        let payload_b: Vec<u8> = (0..8 * 4096usize).map(|i| (i * 11 + 3) as u8).collect();
        assert_ne!(payload_a, payload_b);

        vol.write(24, &payload_a).unwrap();
        vol.write(24, &payload_b).unwrap();
        vol.flush_wal().unwrap();
        assert_eq!(
            vol.read(24, 8).unwrap(),
            payload_b,
            "pre-sweep read must return the second write"
        );

        vol.sweep_pending().unwrap();
        assert_eq!(
            vol.read(24, 8).unwrap(),
            payload_b,
            "post-sweep read must still return the second write"
        );

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

    /// Build a 4 KiB block whose first byte is `seed` and the rest are
    /// pseudo-random — high entropy so compression stays a no-op.
    fn unique_block(seed: u32) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        let s = seed as u64;
        for (i, b) in buf.iter_mut().enumerate() {
            // Distinct per-(seed,i) using a cheap hash. Coprime multipliers
            // keep the byte distribution uniform.
            *b = ((s.wrapping_mul(0x9E37_79B9).wrapping_add(i as u64)) ^ (i as u64 * 31)) as u8;
        }
        buf
    }

    /// Promote `block_count` distinct 4 KiB blocks into one pending segment.
    fn promote_segment_with_blocks(vol: &mut Volume, base_lba: u64, block_count: u64, tag: u32) {
        for i in 0..block_count {
            // Mix `tag` into the seed so different segments don't dedup.
            let block = unique_block(tag.wrapping_mul(0x10001).wrapping_add(i as u32));
            vol.write(base_lba + i, &block).unwrap();
        }
        vol.promote_for_test().unwrap();
    }

    #[test]
    fn sweep_pending_packs_small_with_filler() {
        // One small (~4 KiB live) + one large filler (~17 MiB live).
        // Tier 1 picks up the small; tier 2 sees ~32 MiB - 4 KiB headroom
        // and pulls in the 17 MiB filler. Output is one ~17 MiB segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Small segment: 1 block.
        promote_segment_with_blocks(&mut vol, 0, 1, 1);
        // Filler: 17 MiB live (4352 blocks of 4 KiB).
        // Above the 16 MiB SWEEP_SMALL_THRESHOLD so it's filler material,
        // not a small. Must fit in the 32 MiB budget after the small.
        promote_segment_with_blocks(&mut vol, 1, 4352, 2);

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(
            stats.segments_compacted, 2,
            "tier 2 must pull the filler in alongside the small"
        );
        assert_eq!(stats.new_segments, 1);

        // Both ranges must still read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), unique_block(0x10001));
        assert_eq!(vol.read(1, 1).unwrap(), unique_block(0x10001 * 2));
        assert_eq!(
            vol.read(4352, 1).unwrap(),
            unique_block(0x10001u32.wrapping_mul(2).wrapping_add(4351))
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_respects_entry_cap() {
        // Three pending segments, each carrying 4096 DedupRef entries
        // (live_bytes = 0 — DedupRef has no body cost) plus one tiny
        // DATA segment, total 12_289 entries. Without an entry cap,
        // tier-1 packing would admit all three (byte budget never bites
        // on 0-live_bytes inputs) and produce a 12_289-entry output —
        // far past the WAL's flush cap. With SWEEP_ENTRY_CAP = 8192,
        // tier 1 admits exactly two of the dedup segments (8192 entries)
        // and stops; the third dedup segment and the lone DATA are left
        // for a later pass.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Anchor segment: a single DATA entry establishing the dedup hash.
        let payload = unique_block(0xCAFE);
        vol.write(0, &payload).unwrap();
        vol.promote_for_test().unwrap();

        // Two dedup-only pending segments, 4096 DedupRef entries each.
        for i in 1..=4096u64 {
            vol.write(i, &payload).unwrap();
        }
        vol.promote_for_test().unwrap();
        for i in 100_000..(100_000u64 + 4096) {
            vol.write(i, &payload).unwrap();
        }
        vol.promote_for_test().unwrap();

        // Plus another dedup-only segment so the cap actually has to
        // refuse one of them.
        for i in 200_000..(200_000u64 + 4096) {
            vol.write(i, &payload).unwrap();
        }
        vol.promote_for_test().unwrap();

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(
            stats.segments_compacted, 2,
            "sweep must stop at SWEEP_ENTRY_CAP — exactly two of the \
             three dedup segments fit (8192 entries), the third is left \
             for a later pass"
        );
        assert_eq!(stats.new_segments, 1);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn sweep_pending_skips_lone_filler() {
        // A single filler (~17 MiB live, no small to pair with) must
        // not be rewritten — sweep is for packing, not for moving large
        // segments around. Repack is what handles single-segment cleanup.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        promote_segment_with_blocks(&mut vol, 0, 4352, 1);

        let stats = vol.sweep_pending().unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.new_segments, 0);

        fs::remove_dir_all(base).unwrap();
    }
}
