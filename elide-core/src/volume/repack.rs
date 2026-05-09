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

/// Data needed by the worker to repack sparse segments in `pending/`.
/// Produced by [`super::Volume::prepare_repack`] on the actor thread.
///
/// Per-segment output ULIDs are pre-minted in `output_ulids` (one per
/// pending segment at prep time, monotonically increasing, all below
/// `u_flush` and the next WAL ULID). The worker assigns them in
/// input-ULID order and only consumes as many as it actually rewrites.
///
/// `ceiling` is the WAL-flush ULID minted at prep time: every output
/// ULID is below it, so any pending segment with a strictly greater ULID
/// was minted after prep (e.g. by a `prepare_promote` racing under the
/// dropped lock) and the prep-time `lbamap_snapshot` knows nothing
/// about its entries. The worker skips such segments — including them
/// as bucket inputs would let `apply_repack_result` delete them and
/// clobber the lbamap claims they made.
pub struct RepackJob {
    pub base_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub floor: Option<Ulid>,
    pub ceiling: Ulid,
    pub output_ulids: Vec<Ulid>,
    pub lbamap_snapshot: Arc<lbamap::LbaMap>,
    pub extent_index_snapshot: Arc<extentindex::ExtentIndex>,
    pub ancestor_layers: Vec<super::AncestorLayer>,
    pub fetcher: Option<segment::BoxFetcher>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub segment_cache: Arc<segment_cache::SegmentIndexCache>,
}

/// One bucket from a repack run. A bucket pairs N input segments (1
/// for solo rewrites, ≥2 for bin-packed merges) with a single rewrite
/// output, or with `output = None` when every input classified
/// fully dead and the worker deleted them outright.
///
/// Apply (a) derives the per-input "to remove from extent_index" set
/// as `owned_hashes - carried_hashes(output)` and CAS-removes against
/// the per-input gate (`current loc.segment_id == input_ulid`), then
/// (b) inserts the carried entries under the same gate against the
/// new output ULID, and (c) unlinks each input file.
pub struct RepackedBucket {
    pub inputs: Vec<RepackedInput>,
    pub output: Option<RepackedOutput>,
    pub bytes_freed: u64,
}

/// One selected input contributing to a [`RepackedBucket`].
pub struct RepackedInput {
    pub input_ulid: Ulid,
    pub input_path: PathBuf,
    pub owned_hashes: Vec<blake3::Hash>,
}

/// Materialised rewrite output for a repack bucket.
pub struct RepackedOutput {
    pub new_ulid: Ulid,
    pub new_body_section_start: u64,
    pub out_entries: Vec<segment::SegmentEntry>,
}

/// Result of a [`RepackJob`]. Consumed by [`super::Volume::apply_repack_result`]
/// on the actor thread.
pub struct RepackResult {
    pub stats: CompactionStats,
    pub buckets: Vec<RepackedBucket>,
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
    /// outright. Skips fully-live segments larger than the small threshold
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
            ceiling: u_flush,
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
        let RepackResult { mut stats, buckets } = result;

        let pending_dir = self.base_dir.join("pending");

        for bucket in &buckets {
            let carried_hashes: std::collections::HashSet<blake3::Hash> = bucket
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

            let bucket_input_ulids: std::collections::HashSet<Ulid> =
                bucket.inputs.iter().map(|i| i.input_ulid).collect();

            {
                let index = Arc::make_mut(&mut self.extent_index);

                // Per-input CAS-remove for hashes the bucket's output
                // didn't carry — gated on the specific input's ULID so
                // a concurrent writer that re-pointed the hash wins.
                for input in &bucket.inputs {
                    for hash in &input.owned_hashes {
                        if carried_hashes.contains(hash) {
                            continue;
                        }
                        // `remove_owner_at` covers both `inner` and `deltas`.
                        if index.remove_owner_at(hash, input.input_ulid) {
                            stats.extents_removed += 1;
                        }
                    }
                }

                // Insert carried entries against the new bucket output,
                // gated on the current owner being any of the bucket's
                // inputs.
                if let Some(out) = &bucket.output {
                    for e in &out.out_entries {
                        if matches!(e.kind, EntryKind::DedupRef | EntryKind::Zero) {
                            continue;
                        }
                        let current = index.lookup(&e.hash);
                        let should_update = match current {
                            None => true,
                            Some(loc) => bucket_input_ulids.contains(&loc.segment_id),
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

            if let Some(out) = &bucket.output {
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
                        lbamap.insert_delta_consuming_inputs(
                            e.start_lba,
                            e.lba_length,
                            e.hash,
                            out.new_ulid,
                            sources,
                            &bucket_input_ulids,
                        );
                    } else {
                        lbamap.insert_consuming_inputs(
                            e.start_lba,
                            e.lba_length,
                            e.hash,
                            out.new_ulid,
                            &bucket_input_ulids,
                        );
                    }
                }
            }

            // Per-input bookkeeping: evict cache fd; unlink the input
            // file if the worker wrote a fresh output (when output is
            // None the worker already deleted the input).
            for input in &bucket.inputs {
                self.evict_cached_segment(input.input_ulid);

                if bucket.output.is_some() {
                    match fs::remove_file(&input.input_path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
            }

            stats.bytes_freed += bucket.bytes_freed;

            let inputs_fmt = bucket
                .inputs
                .iter()
                .map(|i| i.input_ulid.to_string())
                .collect::<Vec<_>>()
                .join(",");
            match &bucket.output {
                Some(out) => log::info!(
                    "repack: [{inputs_fmt}] -> {} ({} entries, {} bytes freed)",
                    out.new_ulid,
                    out.out_entries.len(),
                    bucket.bytes_freed,
                ),
                None => log::info!(
                    "repack: [{inputs_fmt}] -> deleted ({} bytes freed)",
                    bucket.bytes_freed,
                ),
            }
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

        // Two segments: first is 100% dead, second is live small.
        // The unified pass packs both into one bucket.
        let stats = vol.repack().unwrap();
        assert_eq!(
            stats.segments_compacted, 2,
            "both inputs go into the packed bucket"
        );
        assert_eq!(stats.new_segments, 1, "single packed output");
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

        // First segment has a hash-dead entry; second is small and live.
        // The unified pass packs both into one bucket.
        let stats = vol.repack().unwrap();
        assert_eq!(stats.segments_compacted, 2);
        assert_eq!(stats.new_segments, 1);
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

        // Pre-snapshot dead segment is frozen; the two post-snapshot segments
        // (one dead, one live small) pack into one bucket.
        let stats = vol.repack().unwrap();
        assert_eq!(
            stats.segments_compacted, 2,
            "both post-snapshot segments are packed; pre-snapshot is frozen"
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

    // --- packing-specific tests ---

    #[test]
    fn repack_removes_dead_extents() {
        // Write LBA 0, promote, overwrite LBA 0, promote.
        // repack should remove the dead extent from the first segment.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.repack().unwrap();
        assert!(stats.segments_compacted >= 1);
        assert!(stats.bytes_freed > 0);
        assert_eq!(stats.extents_removed, 1);

        // Current value of LBA 0 must be the replacement.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_only_scans_pending_not_uploaded() {
        // Upload a segment (simulate coordinator promoting pending → cache/).
        // repack must not touch uploaded segments.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0x11u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        // Simulate coordinator upload + promote IPC: pending → index/ + cache/.
        simulate_upload(&mut vol);

        // Now overwrite LBA 0 and promote — creates a new pending segment.
        vol.write(0, &vec![0x22u8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.repack().unwrap();
        // The old dead extent is in cache/ — repack doesn't touch it.
        assert_eq!(stats.extents_removed, 0);
        // The new pending segment is small and all-live: single segment, no
        // dead extents, so repack correctly leaves it alone.
        assert_eq!(stats.segments_compacted, 0);

        // Data still reads correctly.
        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_respects_snapshot_floor() {
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
        let stats = vol.repack().unwrap();
        assert_eq!(
            stats.segments_compacted, 0,
            "pre-snapshot segments must not be touched"
        );

        assert_eq!(vol.read(0, 1).unwrap(), vec![0x22u8; 4096]);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_multi_block_inplace_overwrite_same_wal() {
        // Regression: two multi-block DATA writes at the same LBA range in the
        // same WAL flush. Both land as DATA entries (different hashes) in one
        // pending segment. repack then partitions entries into live/dead,
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
            "pre-repack read must return the second write"
        );

        vol.repack().unwrap();
        assert_eq!(
            vol.read(24, 8).unwrap(),
            payload_b,
            "post-repack read must still return the second write"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_merges_multiple_small_segments() {
        // Three separate promotes → three small pending segments.
        // repack should merge them into one.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(0, &vec![0xaau8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &vec![0xbbu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(2, &vec![0xccu8; 4096]).unwrap();
        vol.promote_for_test().unwrap();

        let stats = vol.repack().unwrap();
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
    fn repack_packs_small_with_filler() {
        // One small (~4 KiB live) + one ~17 MiB live segment.
        // Bin-pack admits both into one bucket — 17 MiB + 4 KiB
        // ≤ 32 MiB target.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Small segment: 1 block.
        promote_segment_with_blocks(&mut vol, 0, 1, 1);
        // 17 MiB live (4352 blocks of 4 KiB) — must fit in the 32 MiB
        // budget alongside the small.
        promote_segment_with_blocks(&mut vol, 1, 4352, 2);

        let stats = vol.repack().unwrap();
        assert_eq!(
            stats.segments_compacted, 2,
            "bin-pack must combine the small and the 17 MiB segment"
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
    fn repack_respects_entry_cap() {
        // Three pending segments, each carrying 4096 DedupRef entries
        // (live_bytes = 0 — DedupRef has no body cost) plus one tiny
        // DATA segment, total 12_289 entries. Without an entry cap,
        // tier-1 packing would admit all three (byte budget never bites
        // on 0-live_bytes inputs) and produce a 12_289-entry output —
        // far past the WAL's flush cap. With REPACK_ENTRY_CAP = 8192,
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

        let stats = vol.repack().unwrap();
        // Anchor (1 entry) + first dedup (4096 entries) fill bucket[0]
        // (4097 entries; the next 4096 wouldn't fit). Buckets[1] takes
        // the remaining two dedup segments (4096 + 4096 = 8192). All
        // four inputs are processed in this pass; the entry cap forces
        // two output buckets rather than leaving a segment behind.
        assert_eq!(
            stats.segments_compacted, 4,
            "all four candidates are bucketed (entry cap forces two outputs)"
        );
        assert_eq!(stats.new_segments, 2);

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn repack_skips_lone_filler() {
        // A single filler (~17 MiB live, no small to pair with) must
        // not be rewritten — repack does not pack across the small threshold
        // segments around. Repack is what handles single-segment cleanup.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        promote_segment_with_blocks(&mut vol, 0, 4352, 1);

        let stats = vol.repack().unwrap();
        assert_eq!(stats.segments_compacted, 0);
        assert_eq!(stats.new_segments, 0);

        fs::remove_dir_all(base).unwrap();
    }

    // ----------------------------------------------------------------------
    // Lock-drop window regression tests.
    //
    // PR #302 (50511cd) lets `VolumeClient::write` acquire the volume mutex
    // from the calling thread instead of routing through the actor request
    // channel.  That moves writes outside the actor's serialisation window:
    // a write can land between `prepare_repack` returning and
    // `apply_repack_result` reacquiring the lock, while the worker is
    // classifying / materialising against a frozen snapshot.
    //
    // These tests exercise that window deterministically by driving the
    // prep / execute / apply trio directly (the synchronous wrapper
    // `Volume::repack` would close the window before the test could
    // interpose a write).  They mirror the production sequence:
    //
    //   1. Set up at least one pending segment carrying a Keep entry.
    //   2. Call `prepare_repack` — captures lbamap + extent_index snapshot,
    //      mints u_flush + output_ulids.
    //   3. Issue a `Volume::write` that targets one of the snapshot's
    //      Keep entries' LBA ranges.
    //   4. Call `execute_repack` on the captured job — classifies against
    //      the frozen snapshot, materialises the bucket output.
    //   5. Call `apply_repack_result` — must commit the post-prep direct
    //      write, not roll lbamap back to the snapshot's body.
    //
    // See `docs/finding-cargo-build-stale-read.md`.
    // ----------------------------------------------------------------------

    #[test]
    fn lock_drop_full_overwrite_single_block() {
        // Sanity: a single-block Keep entry, fully overwritten by a direct
        // write during the lock-drop window.  apply_repack_result's
        // insert_consuming_inputs blocks check should refuse to clobber
        // the post-prep claim.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload_a = vec![0x11u8; 4096];
        let payload_b = vec![0x22u8; 4096];
        let payload_peer = vec![0x33u8; 4096];

        // Pending S1: data [100+1, H_A].  Pending S2 at LBA 200 is the peer
        // that lets the bin-pack put S1 into a non-solo bucket so the rewrite
        // actually runs (a solo all-live bucket would be skipped).
        vol.write(100, &payload_a).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(200, &payload_peer).unwrap();
        vol.promote_for_test().unwrap();

        let job = vol.prepare_repack().unwrap().expect("repack job");

        // Lock-drop window: same LBA, different bytes → different hash.
        vol.write(100, &payload_b).unwrap();

        let result = crate::actor::execute_repack(job).unwrap();
        vol.apply_repack_result(result).unwrap();

        assert_eq!(
            vol.read(100, 1).unwrap(),
            payload_b,
            "post-apply read must reflect the post-prep direct write"
        );

        drop(vol);
        let vol2 = Volume::open(&base, &base).unwrap();
        assert_eq!(
            vol2.read(100, 1).unwrap(),
            payload_b,
            "reopen rebuild must agree"
        );

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn lock_drop_full_overwrite_multi_block() {
        // Multi-block Keep entry, fully overwritten by three single-block
        // direct writes during the lock-drop window.  Each direct write
        // splits the predecessor in lbamap; insert_consuming_inputs at
        // apply time must mark every sub-range as blocked.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload_a: Vec<u8> = (0..3 * 4096usize).map(|i| (i * 7 + 13) as u8).collect();
        let kernel_blocks: Vec<Vec<u8>> = (0..3)
            .map(|n| (0..4096).map(|i| ((i + n * 1009) * 11 + 3) as u8).collect())
            .collect();
        let peer = vec![0xCDu8; 4096];

        // Pending S1: data [100+3, H_A].
        vol.write(100, &payload_a).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(200, &peer).unwrap();
        vol.promote_for_test().unwrap();

        let job = vol.prepare_repack().unwrap().expect("repack job");

        // Lock-drop window: three single-block direct writes covering the
        // whole [100..103) range.
        vol.write(100, &kernel_blocks[0]).unwrap();
        vol.write(101, &kernel_blocks[1]).unwrap();
        vol.write(102, &kernel_blocks[2]).unwrap();

        let result = crate::actor::execute_repack(job).unwrap();
        vol.apply_repack_result(result).unwrap();

        for (i, block) in kernel_blocks.iter().enumerate() {
            assert_eq!(
                &vol.read(100 + i as u64, 1).unwrap(),
                block,
                "lba {} must reflect the post-prep direct write",
                100 + i,
            );
        }

        drop(vol);
        let vol2 = Volume::open(&base, &base).unwrap();
        for (i, block) in kernel_blocks.iter().enumerate() {
            assert_eq!(
                &vol2.read(100 + i as u64, 1).unwrap(),
                block,
                "reopen rebuild must agree at lba {}",
                100 + i,
            );
        }

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn lock_drop_partial_overwrite_middle_block() {
        // Multi-block Keep entry with a SINGLE middle block overwritten
        // during the lock-drop window.
        //
        // Mirrors the cargo-build inode-table workload shape: snapshot
        // carries a 3-block DATA at [L+3], kernel writes a single block
        // somewhere inside.  At apply time `insert_consuming_inputs` must:
        //   - install the bucket output's claim on the head and tail
        //     sub-ranges with the correct payload_block_offset (0 and 2),
        //   - leave the middle sub-range pointing at the direct write.
        //
        // The current `insert_inner` hard-codes payload_block_offset: 0 for
        // every fresh insert, so a multi-block Keep that has to be split
        // around a blocked middle sub-range loses the trailing block's
        // offset — reads at the trailing LBA return the snapshot's first
        // body block instead of the third.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let block_a0: Vec<u8> = (0..4096).map(|i| (i * 7 + 13) as u8).collect();
        let block_a1: Vec<u8> = (0..4096).map(|i| (i * 11 + 3) as u8).collect();
        let block_a2: Vec<u8> = (0..4096).map(|i| (i * 13 + 5) as u8).collect();
        assert_ne!(block_a0, block_a1);
        assert_ne!(block_a1, block_a2);
        assert_ne!(block_a0, block_a2);
        let payload_a: Vec<u8> = block_a0
            .iter()
            .chain(block_a1.iter())
            .chain(block_a2.iter())
            .copied()
            .collect();
        let kernel_middle: Vec<u8> = (0..4096).map(|i| (i * 17 + 23) as u8).collect();
        let peer = vec![0xCDu8; 4096];

        // Pending S1: data [100+3, H_A].
        vol.write(100, &payload_a).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(200, &peer).unwrap();
        vol.promote_for_test().unwrap();

        let job = vol.prepare_repack().unwrap().expect("repack job");

        // Lock-drop window: single-block direct write at the middle of the
        // 3-block range.  Splits lbamap into three:
        //   [100..101) = (H_A, S1, payload_block_offset=0)
        //   [101..102) = (H_kernel, U_w, payload_block_offset=0)
        //   [102..103) = (H_A, S1, payload_block_offset=2)
        vol.write(101, &kernel_middle).unwrap();

        // Pre-apply sanity: reads still walk through the live state correctly.
        assert_eq!(vol.read(100, 1).unwrap(), block_a0, "pre-apply lba 100");
        assert_eq!(
            vol.read(101, 1).unwrap(),
            kernel_middle,
            "pre-apply lba 101"
        );
        assert_eq!(vol.read(102, 1).unwrap(), block_a2, "pre-apply lba 102");

        let result = crate::actor::execute_repack(job).unwrap();
        vol.apply_repack_result(result).unwrap();

        // After apply, the bucket output O carries the same 3-block H_A
        // body.  Reads must still resolve to the correct block of H_A on
        // the head and tail, and the kernel's middle write on lba 101.
        assert_eq!(
            vol.read(100, 1).unwrap(),
            block_a0,
            "post-apply lba 100 — should be block 0 of H_A"
        );
        assert_eq!(
            vol.read(101, 1).unwrap(),
            kernel_middle,
            "post-apply lba 101 — should be the post-prep direct write"
        );
        assert_eq!(
            vol.read(102, 1).unwrap(),
            block_a2,
            "post-apply lba 102 — should be block 2 of H_A (NOT block 0)"
        );

        drop(vol);
        let vol2 = Volume::open(&base, &base).unwrap();
        assert_eq!(vol2.read(100, 1).unwrap(), block_a0, "reopen lba 100");
        assert_eq!(vol2.read(101, 1).unwrap(), kernel_middle, "reopen lba 101");
        assert_eq!(vol2.read(102, 1).unwrap(), block_a2, "reopen lba 102");

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn lock_drop_full_overwrite_then_wal_flush() {
        // Same shape as `lock_drop_full_overwrite_multi_block` but with a
        // WAL flush after the direct writes and before `execute_repack`.
        // This bumps lbamap claimants for the kernel writes from the WAL
        // ULID to a fresh pending segment ULID — exercising the
        // claimant-bump path in `flush_wal_to_pending_as` while the
        // worker still holds the original snapshot.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload_a: Vec<u8> = (0..3 * 4096usize).map(|i| (i * 7 + 13) as u8).collect();
        let kernel_blocks: Vec<Vec<u8>> = (0..3)
            .map(|n| (0..4096).map(|i| ((i + n * 1009) * 11 + 3) as u8).collect())
            .collect();
        let peer = vec![0xCDu8; 4096];

        vol.write(100, &payload_a).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(200, &peer).unwrap();
        vol.promote_for_test().unwrap();

        let job = vol.prepare_repack().unwrap().expect("repack job");

        for (i, block) in kernel_blocks.iter().enumerate() {
            vol.write(100 + i as u64, block).unwrap();
        }
        // Flush the post-prep WAL into a fresh pending segment with ULID
        // strictly greater than every output ULID.  Bumps lbamap claimants
        // from U_w to the new segment ULID.
        vol.flush_wal().unwrap();

        let result = crate::actor::execute_repack(job).unwrap();
        vol.apply_repack_result(result).unwrap();

        for (i, block) in kernel_blocks.iter().enumerate() {
            assert_eq!(
                &vol.read(100 + i as u64, 1).unwrap(),
                block,
                "post-apply lba {}",
                100 + i,
            );
        }

        drop(vol);
        let vol2 = Volume::open(&base, &base).unwrap();
        for (i, block) in kernel_blocks.iter().enumerate() {
            assert_eq!(
                &vol2.read(100 + i as u64, 1).unwrap(),
                block,
                "reopen lba {}",
                100 + i,
            );
        }

        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn lock_drop_full_overwrite_then_second_repack_pass() {
        // After the first repack apply, the post-prep direct writes live in
        // the running WAL (or, after a flush, in a fresh pending segment).
        // A second repack pass should pick those up and carry them into a
        // new bucket output.  Exercises the cross-pass interaction.
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let payload_a: Vec<u8> = (0..3 * 4096usize).map(|i| (i * 7 + 13) as u8).collect();
        let kernel_blocks: Vec<Vec<u8>> = (0..3)
            .map(|n| (0..4096).map(|i| ((i + n * 1009) * 11 + 3) as u8).collect())
            .collect();
        let peer = vec![0xCDu8; 4096];

        vol.write(100, &payload_a).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(200, &peer).unwrap();
        vol.promote_for_test().unwrap();

        let job = vol.prepare_repack().unwrap().expect("repack job");
        for (i, block) in kernel_blocks.iter().enumerate() {
            vol.write(100 + i as u64, block).unwrap();
        }
        let result = crate::actor::execute_repack(job).unwrap();
        vol.apply_repack_result(result).unwrap();

        // Second repack pass — now operating on the bucket output + the
        // segment(s) carrying the kernel writes.
        vol.flush_wal().unwrap();
        vol.repack().unwrap();

        for (i, block) in kernel_blocks.iter().enumerate() {
            assert_eq!(
                &vol.read(100 + i as u64, 1).unwrap(),
                block,
                "after second repack, lba {}",
                100 + i,
            );
        }

        drop(vol);
        let vol2 = Volume::open(&base, &base).unwrap();
        for (i, block) in kernel_blocks.iter().enumerate() {
            assert_eq!(
                &vol2.read(100 + i as u64, 1).unwrap(),
                block,
                "reopen after second repack, lba {}",
                100 + i,
            );
        }

        fs::remove_dir_all(base).unwrap();
    }
}
