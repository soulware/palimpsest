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

    candidates.sort_unstable_by_key(|c| std::cmp::Reverse(c.dead_blocks));
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

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use std::fs;

    // ---------- extent reclamation (alias-merge) ----------

    /// Produce a 4096-byte block whose bytes depend on `seed` and `block_idx`,
    /// giving incompressible, distinct content per block so that splitting an
    /// originally-contiguous payload exposes the fragmentation clearly.
    fn reclaim_block(seed: u8, block_idx: usize) -> [u8; 4096] {
        let mut buf = [0u8; 4096];
        let key = [seed; 32];
        let mut hasher = blake3::Hasher::new_keyed(&key);
        hasher.update(&(block_idx as u64).to_le_bytes());
        let mut xof = hasher.finalize_xof();
        xof.fill(&mut buf);
        buf
    }

    fn reclaim_payload(seed: u8, n_blocks: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n_blocks * 4096);
        for i in 0..n_blocks {
            out.extend_from_slice(&reclaim_block(seed, i));
        }
        out
    }

    /// Write a single 8-block entry, overwrite the middle 2 blocks with a
    /// smaller (1-block) entry so the original is split prefix/tail, then
    /// run alias-merge over the whole range. The split-tail entry has
    /// `payload_block_offset != 0`, so the primitive should detect bloat
    /// and rewrite both the prefix and the tail as fresh compact entries.
    #[test]
    fn reclaim_alias_merge_rewrites_split_entry() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Big 8-block write at LBA 100.
        let big = reclaim_payload(0xA1, 8);
        vol.write(100, &big).unwrap();
        // Overwrite LBA 103 (1 block, middle) with unrelated content. The map
        // now has [100,103)@0 (hash A) + [103,104)@0 (hash B) + [104,108)@4 (hash A).
        let hole = [0x77u8; 4096];
        vol.write(103, &hole).unwrap();

        // Oracle expected bytes at [100, 108).
        let mut expected = vec![0u8; 8 * 4096];
        expected[..3 * 4096].copy_from_slice(&big[..3 * 4096]);
        expected[3 * 4096..4 * 4096].copy_from_slice(&hole);
        expected[4 * 4096..].copy_from_slice(&big[4 * 4096..]);
        assert_eq!(vol.read(100, 8).unwrap(), expected);

        // Before: 3 entries.
        assert_eq!(vol.lbamap_len(), 3);

        // Sync prep+execute+apply via the in-process wrapper.
        let outcome = vol.reclaim_alias_merge(100, 8).unwrap();
        // Two rewrites: prefix run [100,103) and tail run [104,108). The
        // middle [103,104) is a clean single-block entry with offset=0 —
        // its hash has no `offset != 0` run anywhere, so it's left alone.
        assert!(!outcome.discarded);
        assert_eq!(outcome.runs_rewritten, 2);
        assert_eq!(outcome.bytes_rewritten, (3 + 4) * 4096);

        // Readback still matches.
        assert_eq!(vol.read(100, 8).unwrap(), expected);

        // Second pass is an idempotent no-op: hashes are now stable, every
        // rewrite the worker would propose hits the lbamap noop-skip.
        let outcome2 = vol.reclaim_alias_merge(100, 8).unwrap();
        assert!(!outcome2.discarded);
        assert_eq!(outcome2.runs_rewritten, 0);

        fs::remove_dir_all(base).unwrap();
    }

    /// Query range that slices mid-way through a hash's span: the prefix of
    /// the big write is outside the query. Containment check must refuse to
    /// rewrite — doing so would strand the outside references on the bloated
    /// body and *introduce* fragmentation.
    #[test]
    fn reclaim_alias_merge_skips_non_contained_hash() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Map state: [100, 150) → H/offset=0 (single big entry).
        let big = reclaim_payload(0x3C, 50);
        vol.write(100, &big).unwrap();

        // Query only the tail half — H's first 25 blocks live outside.
        // Containment fails: H has a run [100, 150) which starts at 100,
        // outside the query [125, 150). Nothing to rewrite.
        let outcome = vol.reclaim_alias_merge(125, 25).unwrap();
        assert!(!outcome.discarded);
        assert_eq!(outcome.runs_rewritten, 0);

        fs::remove_dir_all(base).unwrap();
    }

    /// When the LBA map is mutated between prepare and apply, the apply
    /// phase must discard cleanly — orphan-cleaning the worker's output
    /// segment — with no state change to the live lbamap.
    #[test]
    fn reclaim_alias_merge_discards_on_concurrent_mutation() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        let big = reclaim_payload(0x5E, 8);
        vol.write(200, &big).unwrap();
        let hole = [0x11u8; 4096];
        vol.write(203, &hole).unwrap();

        let job = vol.prepare_reclaim(200, 8).unwrap();
        let result = crate::actor::execute_reclaim(job).unwrap();
        // The worker must have produced at least one rewrite.
        assert!(result.segment_written);
        let segment_path = result.pending_dir.join(result.segment_ulid.to_string());
        assert!(segment_path.exists(), "worker segment should be on disk");

        // Simulate concurrent mutation: any write bumps the lbamap Arc and
        // breaks the pointer-equality precondition.
        vol.write(500, &reclaim_payload(0x77, 1)).unwrap();

        // Apply must detect the mutation, discard, and delete the orphan.
        let outcome = vol.apply_reclaim_result(result).unwrap();
        assert!(outcome.discarded);
        assert_eq!(outcome.runs_rewritten, 0);
        assert_eq!(outcome.bytes_rewritten, 0);
        assert!(
            !segment_path.exists(),
            "apply must remove the orphan segment on discard"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Zero extents carry no body and must never be rewritten by alias-merge.
    #[test]
    fn reclaim_alias_merge_skips_zero_extents() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write_zeroes(300, 10).unwrap();
        // Split the zero extent with an unrelated data write so the tail
        // ends up with payload_block_offset != 0. Our rule would normally
        // treat that as "bloat" for any non-zero hash, but ZERO_HASH is
        // always skipped.
        vol.write(304, &[0xABu8; 4096]).unwrap();

        let outcome = vol.reclaim_alias_merge(300, 10).unwrap();
        assert!(!outcome.discarded);
        assert_eq!(outcome.runs_rewritten, 0);

        fs::remove_dir_all(base).unwrap();
    }

    // ---------- reclaim candidate scanner ----------

    fn scanner_thresholds_permissive() -> crate::volume::ReclaimThresholds {
        // Loose thresholds so tests can use small payloads while still
        // exercising the scanner's detection logic.
        crate::volume::ReclaimThresholds {
            min_dead_blocks: 1,
            min_dead_ratio: 0.0,
            min_stored_bytes: 0,
        }
    }

    /// A clean volume with a single compact write produces no candidates.
    #[test]
    fn scan_reclaim_candidates_no_bloat() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(100, &reclaim_payload(0x11, 8)).unwrap();

        let (lbamap, extent_index) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            scanner_thresholds_permissive(),
        );
        assert!(
            candidates.is_empty(),
            "fresh compact entry should not be a candidate, got {candidates:?}"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Write an 8-block entry, overwrite the middle 2 blocks with a
    /// 2-block entry. The original's payload now has dead bytes (blocks
    /// 3..4 are LBA-overwritten). The scanner should flag exactly one
    /// candidate covering the full extent of the original hash's
    /// surviving runs.
    #[test]
    fn scan_reclaim_candidates_flags_split_entry() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Big incompressible 8-block write so payload lives in the body.
        vol.write(200, &reclaim_payload(0x22, 8)).unwrap();
        // Overwrite the middle 2 blocks with unrelated content.
        let hole: Vec<u8> = (0..2).flat_map(|_| [0x77u8; 4096]).collect();
        vol.write(203, &hole).unwrap();

        let (lbamap, extent_index) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            scanner_thresholds_permissive(),
        );
        assert_eq!(
            candidates.len(),
            1,
            "expected exactly one candidate for the bloated hash, got {candidates:?}"
        );
        let c = candidates[0];
        // Tight LBA bound: the hash's first live run starts at 200, its
        // last live run ends at 208.
        assert_eq!(c.start_lba, 200);
        assert_eq!(c.lba_length, 8);
        // Live: 3 prefix + 3 tail = 6. Dead: 2 (the hole in the middle).
        assert_eq!(c.live_blocks, 6);
        assert_eq!(c.dead_blocks, 2);

        fs::remove_dir_all(base).unwrap();
    }

    /// Thresholds are respected: bumping `min_dead_blocks` above the
    /// actual dead count drops the candidate.
    #[test]
    fn scan_reclaim_candidates_respects_min_dead_blocks() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(300, &reclaim_payload(0x33, 8)).unwrap();
        vol.write(303, &[0x99u8; 4096]).unwrap(); // 1 block hole

        let (lbamap, extent_index) = vol.snapshot_maps();

        // With min_dead_blocks=1 the 1-block hole qualifies.
        let loose = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            crate::volume::ReclaimThresholds {
                min_dead_blocks: 1,
                min_dead_ratio: 0.0,
                min_stored_bytes: 0,
            },
        );
        assert_eq!(loose.len(), 1);

        // With min_dead_blocks=2 it does not.
        let strict = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            crate::volume::ReclaimThresholds {
                min_dead_blocks: 2,
                min_dead_ratio: 0.0,
                min_stored_bytes: 0,
            },
        );
        assert!(strict.is_empty());

        fs::remove_dir_all(base).unwrap();
    }

    /// Candidates produced by the scanner must always be valid inputs
    /// to `reclaim_alias_merge` — round-trip through the primitive
    /// rewrites the hash and a rescan produces no further candidates
    /// for it.
    #[test]
    fn scan_reclaim_candidates_round_trip_through_primitive() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        vol.write(400, &reclaim_payload(0x44, 8)).unwrap();
        vol.write(404, &[0x11u8; 4096]).unwrap();

        // Oracle: bytes at [400, 408) after the second write.
        let expected = {
            let mut buf = vec![0u8; 8 * 4096];
            let orig = reclaim_payload(0x44, 8);
            buf[..4 * 4096].copy_from_slice(&orig[..4 * 4096]);
            buf[4 * 4096..5 * 4096].fill(0x11);
            buf[5 * 4096..].copy_from_slice(&orig[5 * 4096..]);
            buf
        };
        assert_eq!(vol.read(400, 8).unwrap(), expected);

        let (lbamap, extent_index) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap,
            &extent_index,
            scanner_thresholds_permissive(),
        );
        assert_eq!(candidates.len(), 1);
        let c = candidates[0];

        // Drop the snapshot clones before mutating Volume state, so
        // Arc::make_mut in the primitive doesn't reallocate.
        drop(lbamap);
        drop(extent_index);

        let outcome = vol.reclaim_alias_merge(c.start_lba, c.lba_length).unwrap();
        assert!(!outcome.discarded);
        assert!(outcome.runs_rewritten > 0);

        // Content preserved.
        assert_eq!(vol.read(400, 8).unwrap(), expected);

        // Rescan: the old hash is gone from the LBA map, the new
        // compact ones have no bloat — zero candidates.
        let (lbamap2, extent_index2) = vol.snapshot_maps();
        let candidates2 = crate::volume::scan_reclaim_candidates(
            &lbamap2,
            &extent_index2,
            scanner_thresholds_permissive(),
        );
        assert!(
            candidates2.is_empty(),
            "rescan after reclaim should find nothing, got {candidates2:?}"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Idempotent-convergence property: a reclaim pass over an
    /// already-optimal range produces no rewrites at all.
    #[test]
    fn reclaim_alias_merge_optimal_range_is_noop() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();
        vol.write(400, &reclaim_payload(0x7A, 4)).unwrap();

        let outcome = vol.reclaim_alias_merge(400, 4).unwrap();
        assert_eq!(outcome.runs_rewritten, 0);
        assert!(!outcome.discarded);

        fs::remove_dir_all(base).unwrap();
    }

    /// A pure tail overwrite leaves the surviving run with
    /// `payload_block_offset == 0` even though the stored body has a
    /// dead tail. Historically the primitive silently rejected this
    /// shape (its gate required at least one run with `offset != 0`)
    /// while the scanner flagged it — causing "0 runs from N
    /// candidates" on `elide volume reclaim`. Regression for the
    /// wider gate in `execute_reclaim` that matches the scanner's
    /// `live_blocks < logical_blocks` criterion.
    #[test]
    fn reclaim_alias_merge_rewrites_tail_overwrite() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // 8-block incompressible write at LBA 600.
        let big = reclaim_payload(0xB1, 8);
        vol.write(600, &big).unwrap();

        // Overwrite LBAs [606, 608) — the last 2 blocks of the extent.
        // Surviving run is [600, 606) with `payload_block_offset = 0`.
        let tail = [0x44u8; 2 * 4096];
        vol.write(606, &tail).unwrap();

        // Precondition: exactly one surviving run of the original hash
        // and its offset is zero — the shape the old gate missed.
        let (lbamap_pre, _) = vol.snapshot_maps();
        let original_hash = blake3::hash(&big);
        let runs_pre = lbamap_pre.runs_for_hash(&original_hash);
        assert_eq!(runs_pre.len(), 1);
        assert_eq!(
            runs_pre[0].2, 0,
            "surviving tail-overwrite run has offset 0"
        );
        drop(lbamap_pre);

        // Oracle.
        let mut expected = Vec::with_capacity(8 * 4096);
        expected.extend_from_slice(&big[..6 * 4096]);
        expected.extend_from_slice(&tail);
        assert_eq!(vol.read(600, 8).unwrap(), expected);

        let outcome = vol.reclaim_alias_merge(600, 8).unwrap();
        assert!(!outcome.discarded);
        assert_eq!(
            outcome.runs_rewritten, 1,
            "tail-overwrite must rewrite the surviving head run"
        );
        assert_eq!(vol.read(600, 8).unwrap(), expected);

        // Rescan: no residual candidates.
        let (lbamap_post, extent_index_post) = vol.snapshot_maps();
        let candidates = crate::volume::scan_reclaim_candidates(
            &lbamap_post,
            &extent_index_post,
            scanner_thresholds_permissive(),
        );
        assert!(
            candidates.is_empty(),
            "post-reclaim rescan must be empty, got {candidates:?}"
        );

        fs::remove_dir_all(base).unwrap();
    }

    /// Coordinator tick semantics: when the IPC handler is called with
    /// `cap = 1` every tick (as `tasks.rs::run_volume_tasks` does), a
    /// backlog of bloated hashes drains to zero over successive calls.
    /// Simulates that loop against the Volume API directly.
    #[test]
    fn reclaim_cap_one_per_call_converges_to_zero_candidates() {
        let base = keyed_temp_dir();
        let mut vol = Volume::open(&base, &base).unwrap();

        // Three independent bloated hashes at disjoint LBA ranges.
        // Each gets a middle overwrite to split it and trip the
        // scanner's dead-block criterion.
        for (seed, base_lba) in [(0xA1u8, 100u64), (0xB2, 200), (0xC3, 300)] {
            vol.write(base_lba, &reclaim_payload(seed, 8)).unwrap();
            vol.write(base_lba + 3, &[0xFFu8; 4096]).unwrap();
        }

        let thresholds = scanner_thresholds_permissive();

        // Scanner starts with three candidates.
        let (lbamap, ei) = vol.snapshot_maps();
        let initial = crate::volume::scan_reclaim_candidates(&lbamap, &ei, thresholds);
        assert_eq!(
            initial.len(),
            3,
            "expected 3 initial candidates, got {initial:?}"
        );
        drop(lbamap);
        drop(ei);

        // Simulate the tick loop: each "tick" scans and processes at
        // most one candidate. Bound iterations so a regression can't
        // infinite-loop the test.
        let mut tick_count = 0usize;
        for _ in 0..10 {
            let (lbamap, ei) = vol.snapshot_maps();
            let mut candidates = crate::volume::scan_reclaim_candidates(&lbamap, &ei, thresholds);
            drop(lbamap);
            drop(ei);
            if candidates.is_empty() {
                break;
            }
            let c = candidates.remove(0);
            let outcome = vol.reclaim_alias_merge(c.start_lba, c.lba_length).unwrap();
            assert!(!outcome.discarded);
            assert!(outcome.runs_rewritten > 0);
            tick_count += 1;
        }

        // Exactly three productive ticks for three initial candidates.
        assert_eq!(tick_count, 3);

        // Rescan: converged.
        let (lbamap, ei) = vol.snapshot_maps();
        let remaining = crate::volume::scan_reclaim_candidates(&lbamap, &ei, thresholds);
        assert!(
            remaining.is_empty(),
            "converged scan must be empty, got {remaining:?}"
        );

        fs::remove_dir_all(base).unwrap();
    }
}
