//! Per-segment redaction: drop dead extents from a `pending/<ulid>`
//! segment before upload so deleted data never leaves the host.
//!
//! Split into the standard prepare → worker execute → apply trio so the
//! heavy I/O (read input, classify, materialise plan, write output)
//! runs off the actor thread; new writes continue to flow during the
//! window. The actor thread runs prep (mint output ULID, flush WAL, take
//! snapshots) and apply (CAS-update extent index, evict caches,
//! rebuild lbamap, unlink input).

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use ulid::Ulid;

use crate::{
    extentindex::{self, BodySource},
    lbamap,
    segment::{self, BoxFetcher, EntryKind, SegmentEntry},
};

use super::{AncestorLayer, Volume};

/// Data needed by the worker to classify and materialise a redact
/// rewrite off-actor.
///
/// Produced by [`super::Volume::prepare_redact`] on the actor thread:
/// the WAL has already been flushed under `u_flush`, the output ULID
/// `new_ulid` (= `u_redact`, with `u_redact < u_flush`) is pre-minted,
/// and Arc snapshots of `lbamap`/`extent_index` capture the volume
/// state the worker classifies against. Concurrent writes after prep
/// don't perturb the snapshot — apply re-derives index updates against
/// the live state with CAS so the worker's stale view degrades
/// gracefully rather than clobbering newer writes.
pub struct RedactJob {
    pub input_ulid: Ulid,
    pub new_ulid: Ulid,
    pub base_dir: PathBuf,
    pub pending_dir: PathBuf,
    pub lbamap_snapshot: Arc<lbamap::LbaMap>,
    pub extent_index_snapshot: Arc<extentindex::ExtentIndex>,
    pub ancestor_layers: Vec<AncestorLayer>,
    pub fetcher: Option<BoxFetcher>,
    pub signer: Arc<dyn segment::SegmentSigner>,
    pub verifying_key: ed25519_dalek::VerifyingKey,
}

/// Outcome of a [`RedactJob`]. Consumed by
/// [`super::Volume::apply_redact_result`] on the actor thread.
///
/// `NoOp` means every entry classified as fully-live; the worker did
/// not write a new file and apply just replies with `input_ulid`.
/// `Rewritten` carries the materialised entries and the input's
/// owned-body hashes so apply can derive extent-index updates and
/// removals against the **current** index, not the prep-time snapshot.
pub enum RedactResult {
    NoOp {
        input_ulid: Ulid,
    },
    Rewritten {
        input_ulid: Ulid,
        new_ulid: Ulid,
        new_body_section_start: u64,
        out_entries: Vec<SegmentEntry>,
        /// Owned-body entries from the input segment (hash + kind), in
        /// input order. Apply diffs these against `out_entries` to
        /// derive the to-remove set for extent-index cleanup.
        input_owned_hashes: Vec<blake3::Hash>,
    },
}

impl Volume {
    /// Drop hash-dead and partially-dead bodies from a `pending/<ulid>`
    /// segment, rewriting it under a freshly-minted ULID
    /// (`u_redact < u_flush`) so concurrent VolumeReaders holding the
    /// pre-rewrite snapshot can't observe a same-path / different-body
    /// alias mid-flight.
    ///
    /// Returns the new ULID. Callers (notably `coordinator/upload.rs`)
    /// use the returned ULID for upload + promote.
    ///
    /// Synchronous wrapper around the offloadable trio. The actor uses
    /// [`Self::prepare_redact`] + [`crate::actor::execute_redact`] +
    /// [`Self::apply_redact_result`] directly so the heavy middle
    /// runs off the request channel; this wrapper exists for tests and
    /// inline callers.
    pub fn redact_segment(&mut self, ulid: Ulid) -> io::Result<Ulid> {
        let job = self.prepare_redact(ulid)?;
        let result = crate::actor::execute_redact(job)?;
        self.apply_redact_result(result)
    }

    /// Prep phase of `redact_segment` — runs on the actor thread.
    ///
    /// Stats `pending/<ulid>` (NotFound bubbles up); pre-mints
    /// `u_redact < u_flush` and flushes any open WAL to
    /// `pending/<u_flush>`; snapshots `lbamap` + `extent_index` for the
    /// worker's classifier and body resolver. The flush mutates
    /// extent-index entries (WAL-relative offsets → segment-relative)
    /// and may delete the open WAL file — the actor calls
    /// `publish_snapshot` after prep so readers don't resolve hashes
    /// through the pre-flush snapshot into a deleted WAL.
    pub fn prepare_redact(&mut self, ulid: Ulid) -> io::Result<RedactJob> {
        let pending_dir = self.base_dir.join("pending");
        let old_seg_path = pending_dir.join(ulid.to_string());
        let _ = fs::metadata(&old_seg_path)?;

        // Mint u_redact < u_flush, sealing any open WAL into a separate
        // pending segment. Mirrors `mint_gc_checkpoint_ulids` for the
        // same reason: the rewrite must sort below every future WAL
        // ULID so `max(pending) < running_WAL` holds. `flush_wal_to_pending_as`
        // is a no-op when the WAL is empty or absent.
        let u_redact = self.mint.next();
        let u_flush = self.mint.next();
        self.flush_wal_to_pending_as(u_flush)?;

        Ok(RedactJob {
            input_ulid: ulid,
            new_ulid: u_redact,
            base_dir: self.base_dir.clone(),
            pending_dir,
            lbamap_snapshot: Arc::clone(&self.lbamap),
            extent_index_snapshot: Arc::clone(&self.extent_index),
            ancestor_layers: self.ancestor_layers.clone(),
            fetcher: self.fetcher.clone(),
            signer: Arc::clone(&self.signer),
            verifying_key: self.verifying_key,
        })
    }

    /// Apply phase of `redact_segment` — runs on the actor thread
    /// after the worker returns.
    ///
    /// On `NoOp`, returns the input ULID unchanged. On `Rewritten`,
    /// derives `to_remove` from the input's owned-body hashes minus
    /// the carried set, runs CAS-style updates on the **current**
    /// extent index (`loc.segment_id == input_ulid` gates), evicts
    /// the input's cached fd, unlinks `pending/<input_ulid>`, cleans
    /// stale promote siblings, and rebuilds lbamap from disk so
    /// sub-run hashes from `Run` records are reflected. The CAS gate
    /// means concurrent re-points of the same hash by writes during
    /// the worker phase win — the redacted body simply becomes
    /// unreferenced until the next pass picks it up.
    pub fn apply_redact_result(&mut self, result: RedactResult) -> io::Result<Ulid> {
        match result {
            RedactResult::NoOp { input_ulid } => {
                self.assert_lbamap_consistent("apply_redact_result_noop");
                Ok(input_ulid)
            }
            RedactResult::Rewritten {
                input_ulid,
                new_ulid,
                new_body_section_start,
                out_entries,
                input_owned_hashes,
            } => {
                let carried_hashes: std::collections::HashSet<blake3::Hash> = out_entries
                    .iter()
                    .filter(|e| e.kind != EntryKind::DedupRef)
                    .map(|e| e.hash)
                    .collect();
                let to_remove: Vec<blake3::Hash> = input_owned_hashes
                    .into_iter()
                    .filter(|h| !carried_hashes.contains(h))
                    .collect();

                let index = Arc::make_mut(&mut self.extent_index);
                for hash in &to_remove {
                    if index
                        .lookup(hash)
                        .is_some_and(|loc| loc.segment_id == input_ulid)
                    {
                        index.remove(hash);
                    }
                }
                for e in &out_entries {
                    if e.kind == EntryKind::DedupRef {
                        continue;
                    }
                    let current = index.lookup(&e.hash);
                    let should_update = match current {
                        None => true,
                        Some(loc) => loc.segment_id == input_ulid,
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

                self.evict_cached_segment(input_ulid);
                if self.last_segment_ulid < Some(new_ulid) {
                    self.last_segment_ulid = Some(new_ulid);
                }
                self.has_new_segments = true;

                let pending_dir = self.base_dir.join("pending");
                let old_seg_path = pending_dir.join(input_ulid.to_string());
                fs::remove_file(&old_seg_path)?;
                segment::fsync_dir(&pending_dir)?;

                crate::actor::invalidate_promote_siblings(
                    &self.base_dir.join("index"),
                    &self.base_dir.join("cache"),
                    input_ulid,
                )?;

                self.rebuild_lbamap_from_disk()?;

                log::info!(
                    "redact {input_ulid} -> {new_ulid}: \
                     {} entries kept, {} entry-position(s) dropped or split",
                    out_entries.len(),
                    to_remove.len(),
                );
                self.assert_lbamap_consistent("apply_redact_result_rewritten");
                Ok(new_ulid)
            }
        }
    }
}
