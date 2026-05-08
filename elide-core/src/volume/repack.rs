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

/// Data needed by the worker to compact small pending segments in
/// `pending/`. Produced by [`super::Volume::prepare_sweep`] on the actor
/// thread after pre-minting `output_ulid` (= `u_sweep`, with
/// `u_sweep < u_flush`) and flushing the WAL into `pending/<u_flush>`
/// so that the rewrite output sorts below every future WAL flush.
///
/// Snapshots of `lbamap` and `extent_index` capture the volume state
/// the worker classifies and materialises against. Concurrent writes
/// after prep don't perturb the snapshot; apply re-derives index
/// updates against the live state with a per-input CAS gate so the
/// worker's stale view degrades gracefully.
pub struct SweepJob {
    pub output_ulid: Ulid,
    pub base_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub floor: Option<Ulid>,
    pub lbamap_snapshot: Arc<lbamap::LbaMap>,
    pub extent_index_snapshot: Arc<extentindex::ExtentIndex>,
    pub ancestor_layers: Vec<super::AncestorLayer>,
    pub fetcher: Option<segment::BoxFetcher>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Result of a [`SweepJob`]. Consumed by [`super::Volume::apply_sweep_result`]
/// on the actor thread.
///
/// `new_ulid` is `Some(output_ulid)` when at least one entry was
/// carried into the rewrite output; `None` indicates the worker
/// declined to merge (insufficient candidates, or every selected input
/// classified as fully dead with nothing to write). In either case
/// `inputs` lists the segments the worker selected, paired with each
/// segment's owned-body hashes for the apply-phase to-remove
/// derivation, and `input_paths` lists the files to unlink.
pub struct SweepResult {
    pub stats: CompactionStats,
    pub new_ulid: Option<Ulid>,
    pub new_body_section_start: u64,
    pub out_entries: Vec<segment::SegmentEntry>,
    pub inputs: Vec<SweptInput>,
}

/// One selected input from a sweep run. `owned_hashes` is the set of
/// body-bearing entry hashes from this input segment — used by apply
/// to derive the to-remove set (`owned_hashes` minus the carried set
/// from `out_entries`, gated by per-input CAS).
pub struct SweptInput {
    pub seg_ulid: Ulid,
    pub seg_path: PathBuf,
    pub owned_hashes: Vec<blake3::Hash>,
}

/// Data needed by the worker to repack sparse segments in `pending/`.
/// Produced by [`super::Volume::prepare_repack`] on the actor thread.
///
/// Per-segment output ULIDs are pre-minted in `output_ulids` (one per
/// pending segment at prep time, monotonically increasing, all below
/// `u_flush` and the next WAL ULID). The worker assigns them in
/// input-ULID order and only consumes as many as it actually rewrites.
pub struct RepackJob {
    pub base_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub floor: Option<Ulid>,
    pub output_ulids: Vec<Ulid>,
    pub lbamap_snapshot: Arc<lbamap::LbaMap>,
    pub extent_index_snapshot: Arc<extentindex::ExtentIndex>,
    pub ancestor_layers: Vec<super::AncestorLayer>,
    pub fetcher: Option<segment::BoxFetcher>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Per-segment payload from a [`RepackJob`]. One of these is produced
/// for every input segment the worker repacked or deleted.
///
/// `output` is `None` when every entry classified as fully dead and the
/// worker deleted the file outright — apply still removes any
/// owned-body hashes from the extent index. When `Some`, the worker
/// has materialised a fresh segment under `new_ulid` and written it to
/// `pending/<new_ulid>`; apply CAS-updates the extent index against
/// the per-input gate (`current loc.segment_id == input_ulid`) and
/// unlinks the old `pending/<input_ulid>`.
pub struct RepackedSegment {
    pub input_ulid: Ulid,
    pub input_path: PathBuf,
    pub owned_hashes: Vec<blake3::Hash>,
    pub output: Option<RepackedOutput>,
    pub bytes_freed: u64,
}

/// Materialised rewrite output for a single repacked input.
pub struct RepackedOutput {
    pub new_ulid: Ulid,
    pub new_body_section_start: u64,
    pub out_entries: Vec<segment::SegmentEntry>,
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
    /// Pre-minted output ULIDs (one per post-snapshot pending segment
    /// at prep time, monotonically increasing, all below the next WAL
    /// ULID). Worker assigns them in input-ULID order and only
    /// consumes as many as it actually rewrites.
    pub output_ulids: Vec<Ulid>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// Per-segment payload from a [`DeltaRepackJob`]. One of these is
/// produced for every segment the worker actually rewrote
/// (segments that had no convertible entries are skipped).
pub struct DeltaRepackedSegment {
    pub input_ulid: Ulid,
    pub input_path: PathBuf,
    pub new_ulid: Ulid,
    pub rewrite: crate::delta_compute::RewrittenSegment,
}

/// Result of a [`DeltaRepackJob`]. Consumed by
/// [`super::Volume::apply_delta_repack_result`] on the actor thread.
pub struct DeltaRepackResult {
    pub stats: DeltaRepackStats,
    pub segments: Vec<DeltaRepackedSegment>,
}

impl Volume {
    /// Rewrite every pending segment with at least one hash-dead body
    /// entry under a freshly-minted ULID; delete all-dead segments
    /// outright. Skips fully-live segments — sweep bin-packs those.
    /// Guarantees deleted data does not leave the host.
    ///
    /// Synchronous wrapper around [`Self::prepare_repack`] +
    /// [`crate::actor::execute_repack`] + [`Self::apply_repack_result`]
    /// for tests and inline callers; the actor uses the trio directly
    /// to offload the middle phase.
    pub fn repack(&mut self) -> io::Result<CompactionStats> {
        let Some(job) = self.prepare_repack()? else {
            return Ok(CompactionStats::default());
        };
        let result = crate::actor::execute_repack(job)?;
        self.apply_repack_result(result)
    }

    /// Prep phase of `repack` — runs on the actor thread.
    ///
    /// Pre-mints `u_flush` and one output ULID per pending segment at
    /// prep time, then flushes the WAL into `pending/<u_flush>`. The
    /// pre-minted ULIDs are monotonically increasing and all sort
    /// below the next WAL ULID so subsequent flushes win on rebuild —
    /// preserves `max(pending) < running_WAL`. Snapshots `lbamap`,
    /// `extent_index`, `ancestor_layers`, and `fetcher` for the
    /// worker's classifier and body resolver.
    ///
    /// Returns `None` when `pending/` is missing or has no segments.
    pub fn prepare_repack(&mut self) -> io::Result<Option<RepackJob>> {
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

        // Pre-mint output ULIDs (one per current pending segment plus
        // one for the WAL-flush peer that prepare creates next) and
        // u_flush. The WAL-flush peer can itself be hash-dead-bearing
        // — multiple writes to the same LBA inside one open WAL leave
        // the earlier hashes dead in the flushed segment — so the
        // worker may need to rewrite it too.
        let mut output_ulids: Vec<Ulid> = Vec::with_capacity(segs.len() + 1);
        for _ in 0..segs.len() + 1 {
            output_ulids.push(self.mint.next());
        }
        let u_flush = self.mint.next();
        self.flush_wal_to_pending_as(u_flush)?;

        Ok(Some(RepackJob {
            base_dir: self.base_dir.clone(),
            pending_dir,
            floor,
            output_ulids,
            lbamap_snapshot: Arc::clone(&self.lbamap),
            extent_index_snapshot: Arc::clone(&self.extent_index),
            ancestor_layers: self.ancestor_layers.clone(),
            fetcher: self.fetcher.clone(),
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `repack` — runs on the actor thread after the
    /// worker returns.
    ///
    /// For each repacked segment:
    ///   - CAS-remove dropped owned hashes (`current loc.segment_id ==
    ///     input_ulid`); concurrent writers that re-pointed the hash
    ///     win.
    ///   - If a fresh output was written: insert carried entries into
    ///     the extent index under the same per-input CAS gate, and
    ///     unlink `pending/<input_ulid>`. The worker has already
    ///     written `pending/<new_ulid>` separately.
    ///   - If every entry was dead: the worker already deleted the
    ///     input file; nothing more to unlink.
    ///   - Evict the file-cache fd for the input ULID either way.
    ///
    /// Then merges each segment's output entries into `self.lbamap`
    /// via `insert_if_newer` keyed on `out.new_ulid`. Sub-run hashes
    /// from `Run` records get installed as they appear in the output;
    /// concurrent live writes have higher claimant ULIDs and are
    /// preserved on overlapping LBAs.
    pub fn apply_repack_result(&mut self, result: RepackResult) -> io::Result<CompactionStats> {
        let RepackResult {
            mut stats,
            segments,
        } = result;

        let pending_dir = self.base_dir.join("pending");

        for seg in &segments {
            let carried_hashes: std::collections::HashSet<blake3::Hash> = seg
                .output
                .as_ref()
                .map(|o| {
                    o.out_entries
                        .iter()
                        .filter(|e| e.kind != EntryKind::DedupRef)
                        .map(|e| e.hash)
                        .collect()
                })
                .unwrap_or_default();

            {
                let index = Arc::make_mut(&mut self.extent_index);
                for hash in &seg.owned_hashes {
                    if carried_hashes.contains(hash) {
                        continue;
                    }
                    // `remove_owner_at` covers both `inner` and `deltas`
                    // — `lookup` alone misses Delta-canonical hashes.
                    if index.remove_owner_at(hash, seg.input_ulid) {
                        stats.extents_removed += 1;
                    }
                }

                if let Some(out) = &seg.output {
                    for e in &out.out_entries {
                        // DedupRef and Zero entries don't own a body — same
                        // filter pattern as the redact / GC apply paths.
                        if matches!(e.kind, EntryKind::DedupRef | EntryKind::Zero) {
                            continue;
                        }
                        let current = index.lookup(&e.hash);
                        let should_update = match current {
                            None => true,
                            Some(loc) => loc.segment_id == seg.input_ulid,
                        };
                        if !should_update {
                            continue;
                        }
                        let inline_data = if e.kind.is_inline() {
                            e.data.clone().map(Vec::into_boxed_slice)
                        } else {
                            None
                        };
                        index.insert(
                            e.hash,
                            extentindex::ExtentLocation {
                                segment_id: out.new_ulid,
                                body_offset: e.stored_offset,
                                body_length: e.stored_length,
                                compressed: e.compressed,
                                body_source: BodySource::Local,
                                body_section_start: out.new_body_section_start,
                                inline_data,
                            },
                        );
                    }
                }
            }

            if let Some(out) = &seg.output {
                if self.last_segment_ulid < Some(out.new_ulid) {
                    self.last_segment_ulid = Some(out.new_ulid);
                }
                self.has_new_segments = true;

                let lbamap = Arc::make_mut(&mut self.lbamap);
                for e in &out.out_entries {
                    if e.kind.is_canonical_only() {
                        continue;
                    }
                    if e.kind == EntryKind::Delta {
                        let sources: Arc<[blake3::Hash]> =
                            e.delta_options.iter().map(|o| o.source_hash).collect();
                        lbamap.insert_delta_if_newer(
                            e.start_lba,
                            e.lba_length,
                            e.hash,
                            out.new_ulid,
                            sources,
                        );
                    } else {
                        lbamap.insert_if_newer(e.start_lba, e.lba_length, e.hash, out.new_ulid);
                    }
                }
            }

            self.evict_cached_segment(seg.input_ulid);

            if seg.output.is_some() {
                match fs::remove_file(&seg.input_path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }

            stats.bytes_freed += seg.bytes_freed;
        }

        segment::fsync_dir(&pending_dir)?;

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
    pub fn prepare_delta_repack(&mut self) -> io::Result<Option<DeltaRepackJob>> {
        let Some(snap_ulid) = latest_snapshot(&self.base_dir)? else {
            return Ok(None);
        };
        let pending_dir = self.base_dir.join("pending");

        // Pre-mint one output ULID per post-snapshot pending segment +
        // u_flush. Mirrors `prepare_repack`. Bound: every pending
        // segment could in principle be a candidate; unused ULIDs are
        // wasted advances of the mint counter (cheap).
        let candidate_count = match segment::collect_segment_files(&pending_dir) {
            Ok(v) => v.len(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };
        let mut output_ulids: Vec<Ulid> = Vec::with_capacity(candidate_count);
        for _ in 0..candidate_count {
            output_ulids.push(self.mint.next());
        }
        let u_flush = self.mint.next();
        self.flush_wal_to_pending_as(u_flush)?;

        Ok(Some(DeltaRepackJob {
            base_dir: self.base_dir.clone(),
            pending_dir,
            snap_ulid,
            output_ulids,
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `delta_repack_post_snapshot` — runs on the actor
    /// thread after the worker returns.
    ///
    /// For each rewritten segment: CAS-updates entries against the
    /// **input** ULID (`current loc.segment_id == input_ulid`) but
    /// re-points them at the freshly-minted output ULID. Concurrent
    /// writers that re-pointed a hash at a newer segment win — the
    /// rewritten body simply becomes unreferenced until the next pass.
    /// Updates each lbamap entry's claimant ULID in place to the new
    /// output ULID (the rewrite preserves start_lba/lba_length/hash;
    /// only storage representation changes), evicts the input's fd
    /// cache, and unlinks the input pending file.
    pub fn apply_delta_repack_result(
        &mut self,
        result: DeltaRepackResult,
    ) -> io::Result<DeltaRepackStats> {
        let DeltaRepackResult {
            mut stats,
            segments,
        } = result;
        let pending_dir = self.base_dir.join("pending");

        for seg in segments {
            let DeltaRepackedSegment {
                input_ulid,
                input_path,
                new_ulid,
                rewrite,
            } = seg;
            let crate::delta_compute::RewrittenSegment {
                entries,
                new_body_section_start: new_bss,
                delta_region_body_length,
                stats: seg_stats,
            } = rewrite;

            let entries_len = entries.len();
            let ei = Arc::make_mut(&mut self.extent_index);
            let lm = Arc::make_mut(&mut self.lbamap);
            for (raw_idx, item) in entries.iter().enumerate() {
                let post = &item.post;
                match (item.pre_kind, post.kind) {
                    (EntryKind::Data, EntryKind::Data) => {
                        ei.replace_if_matches(
                            post.hash,
                            input_ulid,
                            item.pre_stored_offset,
                            extentindex::ExtentLocation {
                                segment_id: new_ulid,
                                body_offset: post.stored_offset,
                                body_length: post.stored_length,
                                compressed: post.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: None,
                            },
                        );
                        lm.set_claimant_if_matches(
                            post.start_lba,
                            post.lba_length,
                            post.hash,
                            new_ulid,
                        );
                    }
                    (EntryKind::Inline, EntryKind::Inline) => {
                        ei.replace_if_matches(
                            post.hash,
                            input_ulid,
                            item.pre_stored_offset,
                            extentindex::ExtentLocation {
                                segment_id: new_ulid,
                                body_offset: post.stored_offset,
                                body_length: post.stored_length,
                                compressed: post.compressed,
                                body_source: BodySource::Local,
                                body_section_start: new_bss,
                                inline_data: post.data.clone().map(Vec::into_boxed_slice),
                            },
                        );
                        lm.set_claimant_if_matches(
                            post.start_lba,
                            post.lba_length,
                            post.hash,
                            new_ulid,
                        );
                    }
                    (EntryKind::Data, EntryKind::Delta) => {
                        ei.remove_if_matches(&post.hash, input_ulid, item.pre_stored_offset);
                        ei.insert_delta_if_absent(
                            post.hash,
                            extentindex::DeltaLocation {
                                segment_id: new_ulid,
                                entry_idx: raw_idx as u32,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: post.delta_options.clone(),
                            },
                        );
                        let sources: Arc<[blake3::Hash]> =
                            post.delta_options.iter().map(|o| o.source_hash).collect();
                        lm.set_delta_sources_if_matches(post.start_lba, post.hash, sources);
                        lm.set_claimant_if_matches(
                            post.start_lba,
                            post.lba_length,
                            post.hash,
                            new_ulid,
                        );
                    }
                    (EntryKind::Delta, EntryKind::Delta) => {
                        ei.insert_delta_if_absent(
                            post.hash,
                            extentindex::DeltaLocation {
                                segment_id: new_ulid,
                                entry_idx: raw_idx as u32,
                                body_source: extentindex::DeltaBodySource::Full {
                                    body_section_start: new_bss,
                                    body_length: delta_region_body_length,
                                },
                                options: post.delta_options.clone(),
                            },
                        );
                        let sources: Arc<[blake3::Hash]> =
                            post.delta_options.iter().map(|o| o.source_hash).collect();
                        lm.set_delta_sources_if_matches(post.start_lba, post.hash, sources);
                        lm.set_claimant_if_matches(
                            post.start_lba,
                            post.lba_length,
                            post.hash,
                            new_ulid,
                        );
                    }
                    (EntryKind::DedupRef, EntryKind::DedupRef)
                    | (EntryKind::Zero, EntryKind::Zero) => {
                        // No body / extent_index update; LBA range and
                        // hash unchanged. Bring the lbamap claimant
                        // into agreement with the new on-disk segment.
                        lm.set_claimant_if_matches(
                            post.start_lba,
                            post.lba_length,
                            post.hash,
                            new_ulid,
                        );
                    }
                    _ => {}
                }
            }

            self.evict_cached_segment(input_ulid);
            match fs::remove_file(&input_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            if self.last_segment_ulid < Some(new_ulid) {
                self.last_segment_ulid = Some(new_ulid);
            }
            self.has_new_segments = true;

            log::info!(
                "delta_repack: seg {input_ulid} -> {new_ulid} converted {}/{} entries, {}→{} bytes",
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

        segment::fsync_dir(&pending_dir)?;

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
    /// Pre-mints `u_sweep < u_flush` and flushes any open WAL to
    /// `pending/<u_flush>`. The output ULID `u_sweep` is below every
    /// future WAL ULID so subsequent flushes win over the merged
    /// segment on rebuild — preserves the `max(pending) < running_WAL`
    /// invariant. Snapshots `lbamap`/`extent_index`/`ancestor_layers`/
    /// `fetcher` for the worker's classifier and body resolver.
    ///
    /// Returns `None` when `pending/` is missing or has no segments —
    /// the dispatch is skipped without burning a flush.
    ///
    /// All actual segment reads, classification, plan materialisation,
    /// and the rewrite happen in [`crate::actor::execute_sweep`]. The
    /// apply phase [`Self::apply_sweep_result`] does the per-input CAS
    /// extent-index updates and input deletion.
    pub fn prepare_sweep(&mut self) -> io::Result<Option<SweepJob>> {
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

        let u_sweep = self.mint.next();
        let u_flush = self.mint.next();
        self.flush_wal_to_pending_as(u_flush)?;

        Ok(Some(SweepJob {
            output_ulid: u_sweep,
            base_dir: self.base_dir.clone(),
            pending_dir,
            floor,
            lbamap_snapshot: Arc::clone(&self.lbamap),
            extent_index_snapshot: Arc::clone(&self.extent_index),
            ancestor_layers: self.ancestor_layers.clone(),
            fetcher: self.fetcher.clone(),
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
            segment_cache: Arc::clone(&self.segment_cache),
        }))
    }

    /// Apply phase of `sweep_pending` — runs on the actor thread after
    /// the worker returns.
    ///
    /// Walks the worker's output entries and inserts them into the
    /// extent index gated by a per-input CAS check
    /// (`current loc.segment_id ∈ inputs ∪ {None}`) — concurrent writes
    /// during the worker phase that re-pointed a hash at a newer
    /// segment win, leaving the swept body unreferenced until the next
    /// pass picks it up.
    ///
    /// Derives `to_remove` from each input's owned hashes minus the
    /// carried set, removes them from the extent index under the same
    /// per-input CAS gate, evicts each input's cached fd, unlinks the
    /// input pending files, and merges the output entries into
    /// `lbamap` via `insert_if_newer` keyed on `new_ulid`. Concurrent
    /// post-prep live writes have higher claimant ULIDs (`u_sweep` is
    /// minted before `u_flush`) and are preserved on overlapping LBAs.
    pub fn apply_sweep_result(&mut self, result: SweepResult) -> io::Result<CompactionStats> {
        let SweepResult {
            mut stats,
            new_ulid,
            new_body_section_start,
            out_entries,
            inputs,
        } = result;

        let input_ulids: std::collections::HashSet<Ulid> =
            inputs.iter().map(|i| i.seg_ulid).collect();
        let carried_hashes: std::collections::HashSet<blake3::Hash> = out_entries
            .iter()
            .filter(|e| e.kind != EntryKind::DedupRef)
            .map(|e| e.hash)
            .collect();

        let index = Arc::make_mut(&mut self.extent_index);

        for input in &inputs {
            for hash in &input.owned_hashes {
                if carried_hashes.contains(hash) {
                    continue;
                }
                // `remove_owner_at` covers both `inner` and `deltas` —
                // `lookup` alone misses Delta-canonical hashes.
                if index.remove_owner_at(hash, input.seg_ulid) {
                    stats.extents_removed += 1;
                }
            }
        }

        if let Some(new_ulid) = new_ulid {
            for e in &out_entries {
                // DedupRef and Zero entries don't own a body — same filter
                // pattern as the redact / GC / sweep apply paths.
                if matches!(e.kind, EntryKind::DedupRef | EntryKind::Zero) {
                    continue;
                }
                let current = index.lookup(&e.hash);
                let should_update = match current {
                    None => true,
                    Some(loc) => input_ulids.contains(&loc.segment_id),
                };
                if !should_update {
                    continue;
                }
                let inline_data = if e.kind.is_inline() {
                    e.data.clone().map(Vec::into_boxed_slice)
                } else {
                    None
                };
                index.insert(
                    e.hash,
                    extentindex::ExtentLocation {
                        segment_id: new_ulid,
                        body_offset: e.stored_offset,
                        body_length: e.stored_length,
                        compressed: e.compressed,
                        body_source: BodySource::Local,
                        body_section_start: new_body_section_start,
                        inline_data,
                    },
                );
            }
            if self.last_segment_ulid < Some(new_ulid) {
                self.last_segment_ulid = Some(new_ulid);
            }
            self.has_new_segments = true;

            let lbamap = Arc::make_mut(&mut self.lbamap);
            for e in &out_entries {
                if e.kind.is_canonical_only() {
                    continue;
                }
                if e.kind == EntryKind::Delta {
                    let sources: Arc<[blake3::Hash]> =
                        e.delta_options.iter().map(|o| o.source_hash).collect();
                    lbamap.insert_delta_if_newer(
                        e.start_lba,
                        e.lba_length,
                        e.hash,
                        new_ulid,
                        sources,
                    );
                } else {
                    lbamap.insert_if_newer(e.start_lba, e.lba_length, e.hash, new_ulid);
                }
            }
        }

        for input in &inputs {
            self.evict_cached_segment(input.seg_ulid);
            match fs::remove_file(&input.seg_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        segment::fsync_dir(&self.base_dir.join("pending"))?;

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

        let stats = vol.repack().unwrap();
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
        let stats = vol.repack().unwrap();
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
            vol.repack().unwrap();
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

        // First segment has a hash-dead entry — repack rewrites it.
        let stats = vol.repack().unwrap();
        assert_eq!(stats.segments_compacted, 1);
        assert!(stats.bytes_freed > 0);

        // Both LBAs read back correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x33u8; 4096]);
        assert_eq!(vol.read(1, 1).unwrap(), vec![0x22u8; 4096]);

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
        let stats = vol.repack().unwrap();
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
        let stats = vol.repack().unwrap();
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
        let stats = vol.repack().unwrap();
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
