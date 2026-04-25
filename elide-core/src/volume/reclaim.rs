//! Extent-reclamation data types, the candidate scanner, and the
//! `impl Volume` block that drives the prepare → execute → apply trio.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use ulid::Ulid;

use crate::{
    extentindex::{self, BodySource},
    lbamap,
    segment::{self, EntryKind},
};

use super::{Volume, ZERO_HASH, latest_snapshot};

/// Data needed by the worker to execute extent reclamation off-actor.
///
/// Produced by [`super::Volume::prepare_reclaim`] on the actor thread. The heavy
/// middle phase — reading live bytes for each bloated run, re-hashing,
/// compressing, and assembling one segment file — runs on the worker
/// thread via [`crate::actor::execute_reclaim`]. The actor reclaims no
/// lock during that window; writes continue to flow through the channel.
///
/// `lbamap_snapshot` is kept private on the carried `ReclaimResult`: the
/// pointer identity is the entire precondition check, and exposing it
/// would invite accidental aliasing that weakens the guarantee.
pub struct ReclaimJob {
    pub target_start_lba: u64,
    pub target_lba_length: u32,
    pub entries: Vec<lbamap::ExtentRead>,
    pub lbamap_snapshot: Arc<lbamap::LbaMap>,
    pub extent_index_snapshot: Arc<extentindex::ExtentIndex>,
    pub search_dirs: Vec<PathBuf>,
    pub pending_dir: PathBuf,
    /// Pre-minted on the actor so the worker can write
    /// `pending/<segment_ulid>` without needing access to the mint.
    pub segment_ulid: Ulid,
    pub signer: Arc<dyn segment::SegmentSigner>,
    /// Latest sealed snapshot ULID for this fork at prepare time, or
    /// `None` if no snapshots exist. A hash whose segment is `<=` this
    /// floor lives in a snapshot-pinned segment and cannot be dropped
    /// for the lifetime of the snapshot — reclaim treats that as
    /// indefinite retention and prefers a thin Delta output over a
    /// fresh body (the body is already permanent either way).
    pub snapshot_floor_ulid: Option<Ulid>,
}

/// A rewritten entry placed in the reclaim output segment, paired with
/// the uncompressed byte count it represents (so outcome accounting
/// reflects logical size rather than stored length after compression).
pub struct ReclaimedEntry {
    pub entry: segment::SegmentEntry,
    pub uncompressed_bytes: u64,
}

/// Result of a [`ReclaimJob`]. Consumed by [`super::Volume::apply_reclaim_result`]
/// on the actor thread.
///
/// `segment_written` distinguishes the "nothing to do" case (empty
/// proposal set, no file on disk) from the "worker committed a segment"
/// case. Apply must either splice the entries into the live lbamap +
/// extent index (pointer-equality precondition holds) or delete
/// `pending/<segment_ulid>` as an orphan (precondition failed).
pub struct ReclaimResult {
    pub lbamap_snapshot: Arc<lbamap::LbaMap>,
    pub segment_ulid: Ulid,
    pub body_section_start: u64,
    /// Sum of `stored_length` for body-section entries in the written
    /// segment. Needed by apply to build
    /// [`extentindex::DeltaBodySource::Full`] for any Delta outputs, whose
    /// delta blobs live at `body_section_start + body_length` in the
    /// pending file.
    pub body_length: u64,
    pub entries: Vec<ReclaimedEntry>,
    pub segment_written: bool,
    pub pending_dir: PathBuf,
}

/// Outcome of a complete alias-merge reclaim pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimOutcome {
    /// True if the apply precondition failed (the LBA map was mutated
    /// between prepare and apply) and nothing was committed.
    pub discarded: bool,
    /// Number of rewrite entries committed (excluding ones the noop-skip
    /// hash check absorbed because the LBA map already records the rewrite).
    pub runs_rewritten: u32,
    /// Total uncompressed bytes committed to fresh compact entries.
    pub bytes_rewritten: u64,
}

/// Per-hash thresholds controlling which hashes the reclamation scanner
/// proposes as worth rewriting. All defaults are placeholders pending
/// empirical tuning on real aged volumes — see the open questions in
/// `docs/design-extent-reclamation.md § Measurement before mechanism`.
#[derive(Debug, Clone, Copy)]
pub struct ReclaimThresholds {
    /// Minimum number of 4K blocks detectably dead inside a hash's stored
    /// payload before the hash is a candidate. Small waste isn't worth the
    /// rewrite cost.
    pub min_dead_blocks: u32,
    /// Minimum `dead / total` ratio. `payload_block_offset` aliasing
    /// already serves reads without decompress-to-discard below this
    /// ratio, so rewriting is pure write amplification.
    pub min_dead_ratio: f64,
    /// Minimum stored body size. Rewriting a tiny entry amortises badly
    /// over the WAL-append + extent_index-update overhead.
    pub min_stored_bytes: u64,
}

impl Default for ReclaimThresholds {
    fn default() -> Self {
        Self {
            min_dead_blocks: 8,
            min_dead_ratio: 0.3,
            min_stored_bytes: 64 * 1024,
        }
    }
}

/// A single reclamation candidate identified by the scanner. The caller
/// passes `(start_lba, lba_length)` to
/// [`crate::actor::VolumeClient::reclaim_alias_merge`].
///
/// The range is chosen to tightly cover every LBA map run for this
/// hash. The primitive's containment check therefore always succeeds
/// for this hash — but the range may also sweep in other, unrelated
/// hashes that happen to sit between this hash's runs; those are left
/// alone by the primitive's own per-hash containment check.
#[derive(Debug, Clone, Copy)]
pub struct ReclaimCandidate {
    pub start_lba: u64,
    pub lba_length: u32,
    /// Detectable dead block count for this hash's stored payload.
    pub dead_blocks: u32,
    /// Sum of live block lengths across all runs that reference this hash.
    pub live_blocks: u32,
    /// Stored body length in bytes (compressed if the payload was compressed).
    pub stored_bytes: u64,
    /// `true` if the stored payload is compressed and the dead count is
    /// a lower bound rather than exact (we can't know trailing-dead bytes
    /// inside a compressed payload without decompressing).
    pub dead_count_is_lower_bound: bool,
}

/// Walk the LBA map, fold per-hash run lists, and emit reclamation
/// candidates that clear all three thresholds in `ReclaimThresholds`.
///
/// The scanner is read-only and takes `&LbaMap` / `&ExtentIndex` so it
/// can run on a [`crate::actor::VolumeClient`] snapshot without any
/// actor round-trip. Returned candidates are sorted by `dead_blocks`
/// descending (the most wasteful rewrites first).
///
/// **Dead-block detection:** for each hash H we compute `live_blocks =
/// sum(run.length)` and `max_payload_end = max(run.offset + run.length)`
/// across all runs. For uncompressed payloads the exact logical length
/// is `body_length / 4096` and `dead_blocks = logical_length -
/// live_blocks`. For compressed payloads and thin Delta entries the
/// exact logical length is unknown without decompressing, so we use
/// `max_payload_end - live_blocks` — a lower bound that never produces
/// false positives but may miss dead bytes past the last observed run.
///
/// Zero-extents, Inline entries, and hashes absent from both the Data
/// and Delta tables are skipped.
pub fn scan_reclaim_candidates(
    lbamap: &lbamap::LbaMap,
    extent_index: &extentindex::ExtentIndex,
    thresholds: ReclaimThresholds,
) -> Vec<ReclaimCandidate> {
    // Per-hash aggregate: (min_lba, max_lba_end, sum_live_blocks, max_offset_end)
    #[derive(Clone, Copy)]
    struct HashAgg {
        min_lba: u64,
        max_lba_end: u64,
        live_blocks: u64,
        max_offset_end: u64,
    }

    let mut per_hash: HashMap<blake3::Hash, HashAgg> = HashMap::new();
    for (lba, length, hash, offset) in lbamap.iter_entries() {
        if hash == ZERO_HASH {
            continue;
        }
        let lba_end = lba + length as u64;
        let offset_end = offset as u64 + length as u64;
        per_hash
            .entry(hash)
            .and_modify(|agg| {
                if lba < agg.min_lba {
                    agg.min_lba = lba;
                }
                if lba_end > agg.max_lba_end {
                    agg.max_lba_end = lba_end;
                }
                agg.live_blocks += length as u64;
                if offset_end > agg.max_offset_end {
                    agg.max_offset_end = offset_end;
                }
            })
            .or_insert(HashAgg {
                min_lba: lba,
                max_lba_end: lba_end,
                live_blocks: length as u64,
                max_offset_end: offset_end,
            });
    }

    let mut candidates = Vec::new();
    for (hash, agg) in &per_hash {
        // Resolve the hash as either a Data/Inline or Delta entry.
        // Determines how we bound logical body size and what counts as
        // stored bytes for the `min_stored_bytes` threshold.
        //
        // Returns:
        // - `logical_blocks`: upper/exact bound on the payload's
        //   logical size in 4 KiB blocks.
        // - `is_lower_bound`: true when `logical_blocks` is a lower
        //   bound (compressed Data, Delta), false when exact.
        // - `stored_bytes`: bytes on disk that rewriting would
        //   orphan — body_length for Data, decompressed-size estimate
        //   for Delta.
        let (logical_blocks, is_lower_bound, stored_bytes) =
            if let Some(loc) = extent_index.lookup(hash) {
                // Inline entries are small by construction and do not
                // benefit from compaction — their bytes live in the
                // .idx, not the body section.
                if loc.inline_data.is_some() {
                    continue;
                }
                if loc.compressed {
                    (agg.max_offset_end, true, loc.body_length as u64)
                } else {
                    (loc.body_length as u64 / 4096, false, loc.body_length as u64)
                }
            } else if extent_index.lookup_delta(hash).is_some() {
                // Delta-backed: the logical fragment size is not
                // recorded on disk (the Delta entry's stored_length
                // is zero — the delta blob is accessed via the
                // separate delta body section). Use `max_offset_end`
                // as a lower bound and approximate stored_bytes as
                // the implied logical body size. Catches middle
                // splits; misses pure tail overwrites of a Delta
                // fragment (rare — Delta fragments are emitted at
                // import or post-snapshot delta-repack, and partial
                // overwrites of their tail specifically are atypical).
                (agg.max_offset_end, true, agg.max_offset_end * 4096)
            } else {
                continue;
            };

        if logical_blocks < agg.live_blocks {
            // Can happen for compressed/delta payloads when the lower
            // bound underestimates — treat as "no detectable bloat".
            continue;
        }
        let dead_blocks = logical_blocks - agg.live_blocks;
        if dead_blocks < u64::from(thresholds.min_dead_blocks) {
            continue;
        }
        if stored_bytes < thresholds.min_stored_bytes {
            continue;
        }
        let dead_ratio = dead_blocks as f64 / logical_blocks as f64;
        if dead_ratio < thresholds.min_dead_ratio {
            continue;
        }
        let lba_length = agg.max_lba_end - agg.min_lba;
        if lba_length > u32::MAX as u64 {
            // Pathological: wouldn't fit in a single reclaim call. Skip.
            continue;
        }
        candidates.push(ReclaimCandidate {
            start_lba: agg.min_lba,
            lba_length: lba_length as u32,
            dead_blocks: dead_blocks.min(u32::MAX as u64) as u32,
            live_blocks: agg.live_blocks.min(u32::MAX as u64) as u32,
            stored_bytes,
            dead_count_is_lower_bound: is_lower_bound,
        });
    }

    candidates.sort_unstable_by(|a, b| b.dead_blocks.cmp(&a.dead_blocks));
    candidates
}

impl Volume {
    /// Synchronous alias-merge wrapper: prepare → execute → apply.
    ///
    /// Used by tests and inline callers that hold a `&mut Volume`
    /// directly. Production callers go through the actor channel via
    /// [`crate::actor::VolumeClient::reclaim_alias_merge`], where the
    /// heavy middle phase runs on the worker thread.
    pub fn reclaim_alias_merge(
        &mut self,
        start_lba: u64,
        lba_length: u32,
    ) -> io::Result<ReclaimOutcome> {
        let job = self.prepare_reclaim(start_lba, lba_length)?;
        let result = crate::actor::execute_reclaim(job)?;
        self.apply_reclaim_result(result)
    }

    /// Prep phase of reclaim — runs on the actor thread.
    ///
    /// Snapshots `lbamap` (precondition token + read source for the
    /// worker's bloat-gate walk), snapshots `extent_index` (so the
    /// worker can resolve hashes to segment bodies without an actor
    /// round-trip), captures the clipped range entries, and mints the
    /// output segment ULID so the worker can write
    /// `pending/<segment_ulid>` directly.
    ///
    /// Search dirs are the fork directory followed by ancestor layers
    /// in the same order `BlockReader` uses — the worker's read helper
    /// walks them to find segment body files.
    ///
    /// ## Ordering invariant: `u_flush < u_reclaim`
    ///
    /// Flushes any open WAL to `pending/<u_flush>` before minting the
    /// reclaim output ULID `u_reclaim`. Mirrors the `u_gc < u_flush`
    /// invariant documented on `GcCheckpointUlids`, but with reversed
    /// polarity: reclaim outputs must sort *above* the flushed-WAL
    /// segment, because reclaim's new LBA mappings supersede the
    /// pre-reclaim WAL entries they consumed.
    ///
    /// Without this flush, a WAL entry for an LBA that reclaim rewrites
    /// survives to `pending/<u_flush>` where `u_flush > u_reclaim`. On
    /// crash rebuild the flushed segment applies after the reclaim
    /// output and shadows it — re-pointing the LBA at a hash whose body
    /// `redact_segment` may have already hole-punched (hash-dead under
    /// the post-reclaim lbamap), producing zero reads.
    pub fn prepare_reclaim(&mut self, start_lba: u64, lba_length: u32) -> io::Result<ReclaimJob> {
        self.flush_wal()?;

        let end_lba = start_lba + lba_length as u64;
        let entries = self.lbamap.extents_in_range(start_lba, end_lba);

        let mut search_dirs: Vec<PathBuf> = vec![self.base_dir.clone()];
        for layer in &self.ancestor_layers {
            if !search_dirs.contains(&layer.dir) {
                search_dirs.push(layer.dir.clone());
            }
        }

        let segment_ulid = self.mint.next();
        let snapshot_floor_ulid = latest_snapshot(&self.base_dir)?;

        Ok(ReclaimJob {
            target_start_lba: start_lba,
            target_lba_length: lba_length,
            entries,
            lbamap_snapshot: Arc::clone(&self.lbamap),
            extent_index_snapshot: Arc::clone(&self.extent_index),
            search_dirs,
            pending_dir: self.base_dir.join("pending"),
            segment_ulid,
            signer: Arc::clone(&self.signer),
            snapshot_floor_ulid,
        })
    }

    /// Apply phase of reclaim — runs on the actor thread after the
    /// worker returns.
    ///
    /// Precondition: `Arc::ptr_eq(result.lbamap_snapshot, self.lbamap)`.
    /// Any mutation between prepare and apply would have called
    /// `Arc::make_mut` on the volume's `lbamap` Arc (externally visible
    /// via at least the published snapshot), which reallocates — so the
    /// pointers differ and this returns cleanly with `discarded: true`,
    /// deleting the worker's pending segment as an orphan. GC would
    /// eventually classify it as all-dead, but cleaning up eagerly
    /// avoids the extra GC round-trip.
    ///
    /// On success, splices the worker's entries into the live lbamap +
    /// extent index. Runs under the actor lock; no CAS needed because
    /// the pointer-equality guard above already proved no concurrent
    /// mutation happened.
    pub fn apply_reclaim_result(&mut self, result: ReclaimResult) -> io::Result<ReclaimOutcome> {
        if !Arc::ptr_eq(&result.lbamap_snapshot, &self.lbamap) {
            // Orphan cleanup: delete the worker's pending/<segment_ulid>.
            if result.segment_written {
                let path = result.pending_dir.join(result.segment_ulid.to_string());
                let _ = std::fs::remove_file(&path);
            }
            return Ok(ReclaimOutcome {
                discarded: true,
                ..Default::default()
            });
        }

        if !result.segment_written {
            return Ok(ReclaimOutcome::default());
        }

        self.has_new_segments = true;
        self.last_segment_ulid = Some(result.segment_ulid);

        let lbamap = Arc::make_mut(&mut self.lbamap);
        let extent_index = Arc::make_mut(&mut self.extent_index);
        for re in &result.entries {
            let entry = &re.entry;
            match entry.kind {
                EntryKind::Data => {
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                    extent_index.insert(
                        entry.hash,
                        extentindex::ExtentLocation {
                            segment_id: result.segment_ulid,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Local,
                            body_section_start: result.body_section_start,
                            inline_data: None,
                        },
                    );
                }
                EntryKind::Inline => {
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                    extent_index.insert(
                        entry.hash,
                        extentindex::ExtentLocation {
                            segment_id: result.segment_ulid,
                            body_offset: entry.stored_offset,
                            body_length: entry.stored_length,
                            compressed: entry.compressed,
                            body_source: BodySource::Local,
                            body_section_start: result.body_section_start,
                            inline_data: entry.data.clone().map(Vec::into_boxed_slice),
                        },
                    );
                }
                EntryKind::DedupRef => {
                    // Canonical body already indexed — nothing to insert
                    // in extent_index; lbamap just claims the LBA run.
                    lbamap.insert(entry.start_lba, entry.lba_length, entry.hash);
                }
                EntryKind::Delta => {
                    // Thin Delta: claim the LBA run with its source
                    // list attached (so GC's `lba_referenced_hashes`
                    // fold keeps the source alive), and register the
                    // delta in the extent_index. `body_length` is the
                    // tail of the body section; the delta region
                    // begins at `body_section_start + body_length`.
                    let source_hashes: Arc<[blake3::Hash]> =
                        entry.delta_options.iter().map(|o| o.source_hash).collect();
                    lbamap.insert_delta(
                        entry.start_lba,
                        entry.lba_length,
                        entry.hash,
                        source_hashes,
                    );
                    extent_index.insert_delta_if_absent(
                        entry.hash,
                        extentindex::DeltaLocation {
                            segment_id: result.segment_ulid,
                            body_source: extentindex::DeltaBodySource::Full {
                                body_section_start: result.body_section_start,
                                body_length: result.body_length,
                            },
                            options: entry.delta_options.clone(),
                        },
                    );
                }
                EntryKind::CanonicalData | EntryKind::CanonicalInline | EntryKind::Zero => {
                    unreachable!(
                        "reclaim output produces only Data/Inline/DedupRef/Delta, got {:?}",
                        entry.kind
                    );
                }
            }
        }

        let mut outcome = ReclaimOutcome::default();
        for re in &result.entries {
            outcome.runs_rewritten += 1;
            outcome.bytes_rewritten += re.uncompressed_bytes;
        }
        Ok(outcome)
    }
}
