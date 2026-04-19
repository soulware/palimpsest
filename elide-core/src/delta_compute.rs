// File-aware delta computation for imported readonly volumes.
//
// Runs inside `elide-import` after all pending segments are written but
// before `serve_promote` publishes the control socket. Matches the newly
// imported volume's filemap against each extent_index source's filemap
// by path, and for each changed file fragment with a locally-available
// source body, computes a zstd-dict-compressed delta blob and rewrites
// the pending segment so the matching DATA entry becomes a thin Delta
// entry. The signer stays in the import process — no key material ever
// leaves the volume process.
//
// See docs/design-delta-compression.md §"Filemap-based delta".

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::block_reader::BlockReader;
use crate::extentindex::{self, ExtentIndex, ExtentLocation};
use crate::filemap::{self, Filemap};
use crate::segment::{
    self, DeltaOption, EntryKind, SegmentEntry, SegmentFlags, SegmentSigner,
    populate_inline_bodies, read_and_verify_segment_index, read_body_section_bodies,
    write_segment_with_delta_body,
};
use crate::segment_cache::SegmentIndexCache;
use crate::signing::{self, VerifyingKey};
use crate::volume;

/// zstd compression level for delta blobs. Deltas are computed once at
/// import time and fetched infrequently; a middling level is a good
/// tradeoff between ratio and import latency.
const ZSTD_LEVEL: i32 = 3;

/// Summary of delta work performed for a single pending volume.
#[derive(Debug, Default)]
pub struct DeltaStats {
    /// Number of pending segments that had one or more entries converted.
    pub segments_rewritten: usize,
    /// Total Data-to-Delta conversions across all rewritten segments.
    pub entries_converted: usize,
    /// Sum of original `stored_length` for converted entries (before
    /// conversion). Zero after conversion because Delta entries reserve
    /// no body space.
    pub original_body_bytes: u64,
    /// Sum of delta blob sizes produced.
    pub delta_body_bytes: u64,
}

/// Compute and apply filemap-based deltas for a freshly imported volume.
///
/// `vol_dir` is the newly imported volume (`pending/` populated, filemap
/// written, `volume.provenance` signed). `by_id_dir` is the parent
/// directory so extent-index ancestors can be resolved.
///
/// Resolution of the lineage and filemap matching runs per source so
/// each source's filemap is paired with its own extent index — this is
/// how we know which source volume's on-disk bytes to read when we find
/// a delta candidate.
///
/// Returns `Ok(DeltaStats::default())` when there is nothing to do
/// (no lineage, no snapshot, no filemap, no convertible entries).
pub fn rewrite_pending_with_deltas(
    vol_dir: &Path,
    by_id_dir: &Path,
    signer: &dyn SegmentSigner,
) -> io::Result<DeltaStats> {
    // Resolve the child's latest snapshot and filemap. No snapshot =
    // nothing was written = nothing to delta.
    let Some(child_snap_ulid) = volume::latest_snapshot(vol_dir)? else {
        return Ok(DeltaStats::default());
    };
    let child_filemap_path = vol_dir
        .join("snapshots")
        .join(format!("{child_snap_ulid}.filemap"));
    if !child_filemap_path.exists() {
        // A non-ext4 or zero-sized import produces no filemap; silently skip.
        return Ok(DeltaStats::default());
    }
    let child_filemap = filemap::read(&child_filemap_path)?;

    // Walk the signed extent-index ancestors. Empty list is the common
    // case for a standalone (non-parented) import — nothing to delta.
    let ancestors = volume::walk_extent_ancestors(vol_dir, by_id_dir)?;
    if ancestors.is_empty() {
        return Ok(DeltaStats::default());
    }

    // Match the child filemap against each source independently. Per
    // source: load its filemap, rebuild its (standalone) extent index
    // so hash lookups resolve to segments inside that specific source
    // volume's directory. A hash that appears in multiple sources is
    // resolved by whichever source is walked first — extent-index
    // ancestors are already deduped at import time.
    //
    // Accumulates child-hash → (source_hash, delta_blob) across all
    // sources. The actual segment rewrite happens after matching so
    // a child segment containing conversions from multiple sources is
    // written once.
    let mut conversions: HashMap<blake3::Hash, Conversion> = HashMap::new();

    for layer in &ancestors {
        let source_dir = &layer.dir;
        let source_snap = match layer.branch_ulid.clone() {
            Some(u) => u,
            None => match volume::latest_snapshot(source_dir)? {
                Some(u) => u.to_string(),
                None => continue,
            },
        };
        let source_filemap_path = source_dir
            .join("snapshots")
            .join(format!("{source_snap}.filemap"));
        if !source_filemap_path.exists() {
            continue;
        }
        let source_filemap = filemap::read(&source_filemap_path)?;

        let source_chain: Vec<(PathBuf, Option<String>)> =
            vec![(source_dir.clone(), Some(source_snap.clone()))];
        let source_index = extentindex::rebuild(&source_chain)?;

        match_filemaps_into(
            &child_filemap,
            &source_filemap,
            &source_index,
            source_dir,
            &mut conversions,
        )?;
    }

    if conversions.is_empty() {
        return Ok(DeltaStats::default());
    }

    // Verifying key for reading the pending segments we just signed.
    let vk = signing::load_verifying_key(vol_dir, signing::VOLUME_PUB_FILE)?;

    // Rewrite every pending segment that contains at least one entry
    // matching the conversion map.
    let pending_dir = vol_dir.join("pending");
    let mut stats = DeltaStats::default();
    let mut entries_iter = fs::read_dir(&pending_dir)?;
    while let Some(entry) = entries_iter.next().transpose()? {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name.ends_with(".tmp") {
            continue;
        }
        if Ulid::from_string(name).is_err() {
            continue;
        }
        let seg_path = entry.path();

        let seg_stats = maybe_rewrite_segment(&seg_path, &conversions, signer, &vk)?;
        if seg_stats.entries_converted > 0 {
            stats.segments_rewritten += 1;
            stats.entries_converted += seg_stats.entries_converted;
            stats.original_body_bytes += seg_stats.original_body_bytes;
            stats.delta_body_bytes += seg_stats.delta_body_bytes;
        }
    }

    Ok(stats)
}

struct Conversion {
    source_hash: blake3::Hash,
    delta_blob: Vec<u8>,
}

/// Walk both filemaps grouped by path, pair fragments by
/// `(file_offset, byte_count)`, and for each differing-hash pair where
/// the source body is locally available, compute a zstd-dict delta and
/// insert it into `out`. A child hash already present in `out` (from
/// an earlier source) is kept — first source wins.
fn match_filemaps_into(
    child: &Filemap,
    source: &Filemap,
    source_index: &ExtentIndex,
    source_dir: &Path,
    out: &mut HashMap<blake3::Hash, Conversion>,
) -> io::Result<()> {
    for path in child.paths() {
        let Some(child_frags) = child.fragments(path) else {
            continue;
        };
        let Some(source_frags) = source.fragments(path) else {
            continue; // new file in child, no delta candidate
        };
        if child_frags.len() != source_frags.len() {
            // Fragmented layout mismatch — skip this file entirely.
            // Graceful degradation per design doc §"Multi-extent files".
            continue;
        }
        // Fragment layouts match iff every (file_offset, byte_count)
        // tuple lines up. Both sides are sorted by file_offset.
        let layouts_match = child_frags
            .iter()
            .zip(source_frags.iter())
            .all(|(c, s)| c.file_offset == s.file_offset && c.byte_count == s.byte_count);
        if !layouts_match {
            continue;
        }

        for (child_frag, source_frag) in child_frags.iter().zip(source_frags.iter()) {
            if child_frag.hash == source_frag.hash {
                continue; // unchanged — dedup handles it
            }
            if out.contains_key(&child_frag.hash) {
                continue; // already produced a delta against an earlier source
            }
            // Source body must resolve locally — we never fetch from S3
            // just to compute a delta.
            let Some(loc) = source_index.lookup(&source_frag.hash) else {
                continue;
            };
            let Ok(source_body) = read_source_extent(source_dir, loc) else {
                continue;
            };
            let source_plain = if loc.compressed {
                decompress_lz4(&source_body)?
            } else {
                source_body
            };

            out.insert(
                child_frag.hash,
                Conversion {
                    source_hash: source_frag.hash,
                    // Placeholder; filled in during the segment pass
                    // where we also have the child body bytes.
                    delta_blob: source_plain,
                },
            );
        }
    }
    Ok(())
}

/// Rewrite one pending segment if it contains entries whose hashes are
/// in the conversion map. For matching entries, computes the zstd
/// delta against the source body (currently stashed in
/// `Conversion::delta_blob` as plaintext), replaces the stored blob
/// with the compressed delta, accumulates it into the segment's delta
/// body section, and re-signs the segment via `signer`.
fn maybe_rewrite_segment(
    seg_path: &Path,
    conversions: &HashMap<blake3::Hash, Conversion>,
    signer: &dyn SegmentSigner,
    vk: &VerifyingKey,
) -> io::Result<SegmentDeltaStats> {
    let (body_section_start, mut entries, _inputs) = read_and_verify_segment_index(seg_path, vk)?;

    let any_match = entries
        .iter()
        .any(|e| e.kind == EntryKind::Data && conversions.contains_key(&e.hash));
    if !any_match {
        return Ok(SegmentDeltaStats::default());
    }

    // Load body bytes for all body-section entries so the rewrite can copy
    // unconverted bodies through verbatim. `DATA_KINDS` covers Data and
    // CanonicalData; delta_compute inputs are fresh imports today so
    // canonical variants don't appear in practice, but the filter aligns
    // with `is_data()` so any future canonical inputs are handled.
    read_body_section_bodies(seg_path, body_section_start, &mut entries)?;

    let mut delta_body: Vec<u8> = Vec::new();
    let mut stats = SegmentDeltaStats::default();

    for entry in entries.iter_mut() {
        if entry.kind != EntryKind::Data {
            continue;
        }
        let Some(conv) = conversions.get(&entry.hash) else {
            continue;
        };
        let Some(stored) = entry.data.as_deref() else {
            continue;
        };
        let child_plain_owned: Vec<u8>;
        let child_plain: &[u8] = if entry.compressed {
            child_plain_owned = decompress_lz4(stored)?;
            &child_plain_owned
        } else {
            stored
        };

        let delta_blob = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &conv.delta_blob)
            .map_err(|e| io::Error::other(format!("zstd compressor init failed: {e}")))?
            .compress(child_plain)
            .map_err(|e| io::Error::other(format!("zstd delta compression failed: {e}")))?;

        // Skip conversion if the delta isn't actually smaller than the
        // stored (possibly lz4-compressed) body — storing a larger
        // delta just to drop the DATA body would be a net loss on hosts
        // that already have the source cached but a net loss too on
        // cold hosts that would otherwise fetch the raw body.
        if delta_blob.len() >= entry.stored_length as usize {
            continue;
        }

        let delta_offset = delta_body.len() as u64;
        let delta_length = delta_blob.len() as u32;
        delta_body.extend_from_slice(&delta_blob);

        stats.original_body_bytes += entry.stored_length as u64;
        stats.delta_body_bytes += delta_length as u64;
        stats.entries_converted += 1;

        // Convert entry in place. Clear body bookkeeping, drop data,
        // add delta option, flip the kind.
        entry.kind = EntryKind::Delta;
        entry.stored_offset = 0;
        entry.stored_length = 0;
        entry.data = None;
        entry.compressed = false;
        entry.delta_options.push(DeltaOption {
            source_hash: conv.source_hash,
            delta_offset,
            delta_length,
        });
    }

    if stats.entries_converted == 0 {
        return Ok(stats);
    }

    // Any remaining Data entries need non-None `data` so
    // write_segment_with_delta_body can emit their bytes. We loaded
    // them above, so this should always hold; treat a missing body as
    // a programmer error rather than silently corrupting output.
    for entry in entries.iter() {
        if entry.kind == EntryKind::Data && entry.data.is_none() {
            return Err(io::Error::other(format!(
                "pending segment {} has Data entry with no body bytes loaded",
                seg_path.display()
            )));
        }
        // Inline entries: write_segment_with_delta_body writes inline
        // bodies from `entry.data`, but entries we loaded above were
        // filtered to Data only. Load inline bodies too by re-reading.
        let _ = SegmentFlags::empty(); // keep the import alive
    }
    // Re-read inline bodies if any are present (separate pass because
    // inline bodies live in the inline section, not the body section).
    let has_inline = entries.iter().any(|e| e.kind.is_inline());
    if has_inline {
        let inline_bytes = read_inline_section(seg_path, &entries)?;
        populate_inline_bodies(&mut entries, &inline_bytes);
    }

    // Write to a tmp sibling then rename atomically.
    let tmp_path = {
        let mut name = seg_path
            .file_name()
            .ok_or_else(|| io::Error::other("segment path has no filename"))?
            .to_owned();
        name.push(".delta.tmp");
        seg_path.with_file_name(name)
    };
    let _ = fs::remove_file(&tmp_path);
    write_segment_with_delta_body(&tmp_path, &mut entries, &delta_body, signer)?;
    fs::rename(&tmp_path, seg_path)?;
    segment::fsync_dir(seg_path)?;

    Ok(stats)
}

/// Read the inline section bytes from a full segment file. The inline
/// section sits between the index section and the body section; its
/// length comes from the header. Returned bytes are passed to
/// `read_extent_bodies` as `inline_bytes`.
fn read_inline_section(seg_path: &Path, entries: &[SegmentEntry]) -> io::Result<Vec<u8>> {
    // Inline section length = sum of stored_length of Inline / CanonicalInline
    // entries. Position = body_section_start - inline_length.
    let layout = segment::read_segment_layout(seg_path)?;
    let inline_length: u64 = entries
        .iter()
        .filter(|e| e.kind.is_inline())
        .map(|e| e.stored_length as u64)
        .sum();
    if inline_length == 0 {
        return Ok(Vec::new());
    }
    let inline_start = layout.body_section_start - inline_length;
    let mut f = fs::File::open(seg_path)?;
    f.seek(SeekFrom::Start(inline_start))?;
    let mut buf = vec![0u8; inline_length as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read the stored (possibly lz4-compressed) bytes for a source extent.
///
/// Inline entries are served directly from `loc.inline_data`, which the
/// extent-index rebuild already populates from the source segment's
/// `.idx` inline section — the `body_offset`/`body_length` fields on
/// an Inline location are inline-section-relative and must not be used
/// as a body seek. For non-inline entries, resolve the segment body
/// via `segment::locate_segment_body` (canonical precedence wal →
/// pending → gc/.applied → cache/.body) and pick the seek arithmetic
/// from the returned layout: body-only files seek at `body_offset`
/// alone, full segment files seek at `body_section_start + body_offset`.
fn read_source_extent(source_dir: &Path, loc: &ExtentLocation) -> io::Result<Vec<u8>> {
    if let Some(inline) = loc.inline_data.as_deref() {
        return Ok(inline.to_vec());
    }

    let (path, layout) =
        segment::locate_segment_body(source_dir, loc.segment_id).ok_or_else(|| {
            io::Error::other(format!(
                "source extent segment {} not found under {}",
                loc.segment_id,
                source_dir.display()
            ))
        })?;
    let mut f = fs::File::open(&path)?;
    f.seek(SeekFrom::Start(layout.body_seek(loc)))?;
    let mut buf = vec![0u8; loc.body_length as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn decompress_lz4(data: &[u8]) -> io::Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| io::Error::other(format!("lz4 decompression failed: {e}")))
}

#[derive(Default, Debug)]
pub struct SegmentDeltaStats {
    pub entries_converted: usize,
    pub original_body_bytes: u64,
    pub delta_body_bytes: u64,
}

/// A single entry rewritten by [`rewrite_post_snapshot_with_prior`]. Carries
/// both the post-rewrite entry and the pre-rewrite (kind, stored_offset)
/// pair so the apply phase can CAS against the source location.
pub struct RewrittenEntry {
    /// Entry kind before the rewrite.
    pub pre_kind: EntryKind,
    /// `stored_offset` before the rewrite (CAS key for the apply phase).
    pub pre_stored_offset: u64,
    /// Entry after the rewrite.
    pub post: SegmentEntry,
}

/// Result of rewriting one segment. The apply phase uses `entries` +
/// `new_body_section_start` + `delta_region_body_length` to update the
/// extent index; `stats` feeds the aggregated pass stats.
pub struct RewrittenSegment {
    pub entries: Vec<RewrittenEntry>,
    pub new_body_section_start: u64,
    /// Length of the tail body region that holds the Data entries
    /// re-emitted verbatim; the delta region starts at
    /// `new_body_section_start + delta_region_body_length`.
    pub delta_region_body_length: u64,
    pub stats: SegmentDeltaStats,
}

/// Rewrite one post-snapshot pending segment, converting single-block
/// `Data` entries to thin `Delta` entries whenever the prior sealed
/// snapshot holds a same-LBA extent that compresses well as a zstd
/// dictionary.
///
/// `prior` must be a snapshot-pinned `BlockReader` opened against the
/// latest sealed snapshot. For each single-block `Data` entry in
/// `seg_path`, we resolve `prior.hash_for_lba(lba)` to find the
/// candidate source hash, read the full source plaintext via
/// `prior.read_extent_body`, and compress the child body with that
/// source as the zstd dictionary. Conversions with `delta_len >=
/// stored_length` are skipped; multi-block entries are left alone (a
/// Tier 2 refinement relaxes that).
///
/// Returns the rewritten entries (with their fresh `stored_offset`
/// values from the in-place rewrite), the new `body_section_start`, and
/// a stats struct. The caller is responsible for updating the in-memory
/// extent index — this function only touches the segment file.
///
/// Tmp + rename in place (same ULID), same crash-recovery story as
/// `Volume::repack`.
pub fn rewrite_post_snapshot_with_prior(
    seg_path: &Path,
    prior: &BlockReader,
    signer: &dyn SegmentSigner,
    vk: &VerifyingKey,
    segment_cache: &SegmentIndexCache,
) -> io::Result<Option<RewrittenSegment>> {
    let parsed = segment_cache.read_and_verify(seg_path, vk)?;
    let body_section_start = parsed.body_section_start;
    let mut entries = parsed.entries.clone();
    // Capture pre-rewrite (kind, stored_offset) per entry so the apply
    // phase can CAS against the source location. Paired by index with
    // the post-rewrite entries returned below.
    let pre_meta: Vec<(EntryKind, u64)> =
        entries.iter().map(|e| (e.kind, e.stored_offset)).collect();

    // Early out: does any single-block Data entry have a same-LBA prior hash?
    let any_candidate = entries.iter().any(|e| {
        e.kind == EntryKind::Data && e.lba_length == 1 && prior.hash_for_lba(e.start_lba).is_some()
    });
    if !any_candidate {
        return Ok(None);
    }

    // Load all body-section bodies — both the ones we might convert and
    // the ones we need to re-emit verbatim. Same pattern as
    // `maybe_rewrite_segment`.
    read_body_section_bodies(seg_path, body_section_start, &mut entries)?;

    let mut delta_body: Vec<u8> = Vec::new();
    let mut stats = SegmentDeltaStats::default();
    let mut source_plain_cache: HashMap<blake3::Hash, Vec<u8>> = HashMap::new();

    for entry in entries.iter_mut() {
        if entry.kind != EntryKind::Data || entry.lba_length != 1 {
            continue;
        }
        let Some(source_hash) = prior.hash_for_lba(entry.start_lba) else {
            continue;
        };
        if source_hash == entry.hash {
            // Same content as prior snapshot — dedup handles this via
            // hash equality; nothing to delta.
            continue;
        }
        let Some(stored) = entry.data.as_deref() else {
            continue;
        };

        // Fetch source plaintext (cached per source hash — a hot file
        // being rewritten at multiple LBAs shares its dictionary).
        let source_plain = match source_plain_cache.get(&source_hash) {
            Some(v) => v,
            None => {
                let plain = match prior.read_extent_body(&source_hash) {
                    Ok(p) => p,
                    // Source body missing locally (e.g. evicted and no
                    // fetcher configured). Skip this entry — delta is
                    // best-effort.
                    Err(_) => continue,
                };
                source_plain_cache.entry(source_hash).or_insert(plain)
            }
        };

        let child_plain_owned: Vec<u8>;
        let child_plain: &[u8] = if entry.compressed {
            child_plain_owned = decompress_lz4(stored)?;
            &child_plain_owned
        } else {
            stored
        };

        let delta_blob = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, source_plain)
            .map_err(|e| io::Error::other(format!("zstd compressor init failed: {e}")))?
            .compress(child_plain)
            .map_err(|e| io::Error::other(format!("zstd delta compression failed: {e}")))?;

        if delta_blob.len() >= entry.stored_length as usize {
            continue;
        }

        let delta_offset = delta_body.len() as u64;
        let delta_length = delta_blob.len() as u32;
        delta_body.extend_from_slice(&delta_blob);

        stats.original_body_bytes += entry.stored_length as u64;
        stats.delta_body_bytes += delta_length as u64;
        stats.entries_converted += 1;

        entry.kind = EntryKind::Delta;
        entry.stored_offset = 0;
        entry.stored_length = 0;
        entry.data = None;
        entry.compressed = false;
        entry.delta_options.push(DeltaOption {
            source_hash,
            delta_offset,
            delta_length,
        });
    }

    if stats.entries_converted == 0 {
        return Ok(None);
    }

    // Remaining Data entries must have their body bytes loaded so
    // write_segment_with_delta_body can emit them. Inline entries live
    // in the inline section and need a separate pass.
    for entry in entries.iter() {
        if entry.kind == EntryKind::Data && entry.data.is_none() {
            return Err(io::Error::other(format!(
                "post-snapshot segment {} has Data entry with no body bytes loaded",
                seg_path.display()
            )));
        }
    }
    let has_inline = entries.iter().any(|e| e.kind.is_inline());
    if has_inline {
        let inline_bytes = read_inline_section(seg_path, &entries)?;
        populate_inline_bodies(&mut entries, &inline_bytes);
    }

    let tmp_path = {
        let mut name = seg_path
            .file_name()
            .ok_or_else(|| io::Error::other("segment path has no filename"))?
            .to_owned();
        name.push(".delta.tmp");
        seg_path.with_file_name(name)
    };
    let _ = fs::remove_file(&tmp_path);
    let new_body_section_start =
        write_segment_with_delta_body(&tmp_path, &mut entries, &delta_body, signer)?;
    fs::rename(&tmp_path, seg_path)?;
    segment::fsync_dir(seg_path)?;

    // Delta region starts at `new_body_section_start + delta_region_body_length`.
    // Compute it from post-rewrite stored_length on Data entries; that's the
    // same sum `write_segment_with_delta_body` used when laying out the
    // body section.
    let delta_region_body_length: u64 = entries
        .iter()
        .filter(|e| e.kind == EntryKind::Data)
        .map(|e| e.stored_length as u64)
        .sum();

    let rewritten = entries
        .into_iter()
        .zip(pre_meta)
        .map(|(post, (pre_kind, pre_stored_offset))| RewrittenEntry {
            pre_kind,
            pre_stored_offset,
            post,
        })
        .collect();

    Ok(Some(RewrittenSegment {
        entries: rewritten,
        new_body_section_start,
        delta_region_body_length,
        stats,
    }))
}
