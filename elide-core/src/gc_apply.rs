//! Volume-side materialisation of a coordinator-emitted [`GcPlan`].
//!
//! See `docs/design-gc-plan-handoff.md`.
//!
//! The coordinator classifies inputs and writes a [`GcPlan`]; the volume
//! reads it, resolves bodies through its own ancestor-aware / fetcher-aware
//! primitives, builds the output segment's entries and delta body bytes, and
//! hands them off to the caller to sign and commit.
//!
//! This module contains only the pure materialisation step. The commit path
//! — writing the signed segment, renaming tmp → bare, deriving extent-index
//! updates — lives in `volume.rs` next to the existing handoff commit logic.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use ulid::Ulid;

use crate::extentindex::{BodySource, ExtentIndex};
use crate::gc_plan::{GcPlan, PlanOutput};
use crate::segment::{self, BoxFetcher, EntryKind, SegmentBodyLayout, SegmentEntry, SegmentFlags};

/// Block size in bytes (4 KiB).
pub const BLOCK_BYTES: u64 = 4096;

/// Parsed state of one input segment, used during materialisation.
struct InputState {
    entries: Vec<SegmentEntry>,
    inline: Vec<u8>,
    body_section_start: u64,
    body_length: u64,
}

/// Result of a successful materialisation: the entries to write into the
/// output segment and the concatenated delta body bytes (empty when no Delta
/// entries are carried forward).
pub struct Materialised {
    pub entries: Vec<SegmentEntry>,
    pub delta_body: Vec<u8>,
}

/// Error outcomes distinct from hard I/O failures. Returned when the plan
/// cannot be applied this pass (the caller treats these the same as the
/// self-describing stale-liveness cancellation: remove the plan file and let
/// the coordinator retry).
#[derive(Debug)]
pub enum MaterialiseError {
    /// The plan references an input whose `.idx` is missing or whose
    /// referenced entry index is out of range.
    MissingInput(String),
    /// A partial-death expansion requested a composite body whose hash is
    /// not resolvable via the merged extent index.
    UnresolvableHash(String),
    /// A body was read but failed its declared hash (poisoned input).
    BodyIntegrity(String),
    /// Other internal inconsistency (size mismatch, truncated body, etc.).
    Internal(String),
}

impl std::fmt::Display for MaterialiseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingInput(s) => write!(f, "missing input: {s}"),
            Self::UnresolvableHash(s) => write!(f, "unresolvable hash: {s}"),
            Self::BodyIntegrity(s) => write!(f, "body integrity: {s}"),
            Self::Internal(s) => write!(f, "internal: {s}"),
        }
    }
}

impl std::error::Error for MaterialiseError {}

pub type MaterialiseResult<T> = Result<T, MaterialiseOutcome>;

/// Either a hard I/O error (propagates as `io::Error`) or a soft cancellation
/// (remove the plan, let the coordinator retry next pass).
#[derive(Debug)]
pub enum MaterialiseOutcome {
    Io(io::Error),
    Cancel(MaterialiseError),
}

impl From<io::Error> for MaterialiseOutcome {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<MaterialiseError> for MaterialiseOutcome {
    fn from(e: MaterialiseError) -> Self {
        Self::Cancel(e)
    }
}

/// External state a materialiser needs to resolve bodies.
///
/// Designed to match what `Volume` carries so the production caller is a
/// thin wrapper; tests can mock by constructing each field directly.
pub struct MaterialiseCtx<'a> {
    /// The volume's fork directory (e.g. `volumes/myvm/default/`).
    pub base_dir: &'a Path,
    /// Per-input-segment data read from `index/<ulid>.idx`, keyed by input ULID.
    /// Built by [`load_input_states`].
    input_states: HashMap<Ulid, InputState>,
    /// Merged (ancestor-aware) extent index — used to resolve composite
    /// bodies for DedupRef / Delta partial-death expansion.
    pub extent_index: &'a ExtentIndex,
    /// Resolver for locating segment bodies across self + ancestors + S3.
    pub resolver: &'a dyn BodyResolver,
}

/// Segment body resolution against the volume's self + ancestor dirs and an
/// optional demand-fetcher. In production this delegates to
/// `volume::find_segment_in_dirs` + `volume::open_delta_body_in_dirs`; tests
/// can implement it directly against an in-memory tree.
pub trait BodyResolver {
    /// Locate a segment body file, demand-fetching into local cache if a
    /// fetcher is attached. Returns the path (which may be a full segment
    /// file or a `cache/<id>.body` body-only file).
    ///
    /// For `BodySource::Cached(entry_idx)` the caller must guarantee the
    /// entry being read is a body-carrying kind (Data/Inline) whose
    /// `.present` bit is set — the production implementation gates cache
    /// hits on that bit.
    fn find_segment(
        &self,
        segment_id: Ulid,
        body_section_start: u64,
        body_source: BodySource,
    ) -> io::Result<(std::path::PathBuf, SegmentBodyLayout)>;

    /// Locate a segment file without any `.present` gating. Used by the
    /// delta-blob read path: Delta entries have no body bytes of their own,
    /// so the `.present` bitmap is not set for them, but the segment's
    /// `.body`/`.delta` files may still be on disk. Returns `None` if no
    /// file for this segment exists in self + ancestors.
    fn locate_segment_unchecked(
        &self,
        segment_id: Ulid,
    ) -> Option<(std::path::PathBuf, SegmentBodyLayout)>;

    /// Open the delta body file for a cached segment (`cache/<id>.delta`),
    /// demand-fetching on miss. Only called when the input's segment file
    /// layout is `BodyOnly` — for `FullSegment` the delta bytes are in the
    /// same file at `body_section_start + body_length + delta_offset`.
    fn open_delta_body(&self, segment_id: Ulid) -> io::Result<fs::File>;
}

/// Load per-input idx state. Returns `Err(MissingInput(...))` if any input's
/// `.idx` is absent — in that case the caller cancels the plan (the input
/// was deleted between plan emission and apply).
fn load_input_states(
    base_dir: &Path,
    inputs: &[Ulid],
) -> Result<HashMap<Ulid, InputState>, MaterialiseOutcome> {
    let index_dir = base_dir.join("index");
    let mut out = HashMap::with_capacity(inputs.len());
    for input_ulid in inputs {
        let idx_path = index_dir.join(format!("{input_ulid}.idx"));
        let parsed = match segment::read_segment_index(&idx_path) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(MaterialiseError::MissingInput(format!(
                    "input {input_ulid} .idx missing"
                ))
                .into());
            }
            Err(e) => return Err(e.into()),
        };
        let (bss, entries, _inputs) = parsed;
        let layout = segment::read_segment_layout(&idx_path)?;
        let inline = segment::read_inline_section(&idx_path)?;
        out.insert(
            *input_ulid,
            InputState {
                entries,
                inline,
                body_section_start: bss,
                body_length: layout.body_length,
            },
        );
    }
    Ok(out)
}

/// Materialise a plan into output entries + delta body. See the module
/// docstring.
pub fn materialise_plan(
    plan: &GcPlan,
    ctx: &MaterialiseCtx<'_>,
) -> Result<Materialised, MaterialiseOutcome> {
    let mut out_entries: Vec<SegmentEntry> = Vec::new();
    let mut delta_body: Vec<u8> = Vec::new();
    // Cache of uncompressed composite bodies, keyed by (input, entry_idx).
    // Consecutive `Run` records for the same entry share the resolved body.
    let mut composite_cache: HashMap<(Ulid, u32), Vec<u8>> = HashMap::new();

    for output in &plan.outputs {
        apply_one_output(
            output,
            ctx,
            &mut out_entries,
            &mut delta_body,
            &mut composite_cache,
        )?;
    }

    Ok(Materialised {
        entries: out_entries,
        delta_body,
    })
}

fn apply_one_output(
    output: &PlanOutput,
    ctx: &MaterialiseCtx<'_>,
    out_entries: &mut Vec<SegmentEntry>,
    delta_body: &mut Vec<u8>,
    composite_cache: &mut HashMap<(Ulid, u32), Vec<u8>>,
) -> Result<(), MaterialiseOutcome> {
    match output {
        PlanOutput::Keep { input, entry_idx } => {
            emit_keep(*input, *entry_idx, ctx, out_entries, delta_body)
        }
        PlanOutput::ZeroSplit {
            input,
            entry_idx,
            start_lba,
            lba_length,
        } => emit_zero_split(
            *input,
            *entry_idx,
            *start_lba,
            *lba_length,
            ctx,
            out_entries,
        ),
        PlanOutput::Canonical { input, entry_idx } => {
            emit_canonical(*input, *entry_idx, ctx, out_entries)
        }
        PlanOutput::Run {
            input,
            entry_idx,
            payload_block_offset,
            start_lba,
            lba_length,
        } => emit_run(
            *input,
            *entry_idx,
            *payload_block_offset,
            *start_lba,
            *lba_length,
            ctx,
            out_entries,
            composite_cache,
        ),
        PlanOutput::Drop { .. } => {
            // No output entry — the apply path evicts this input's hashes
            // from the extent index via the derive-at-apply logic.
            Ok(())
        }
    }
}

fn input<'a>(
    ctx: &'a MaterialiseCtx<'_>,
    input_ulid: Ulid,
) -> Result<&'a InputState, MaterialiseOutcome> {
    ctx.input_states.get(&input_ulid).ok_or_else(|| {
        MaterialiseError::MissingInput(format!("no state for input {input_ulid}")).into()
    })
}

fn input_entry<'a>(
    ctx: &'a MaterialiseCtx<'_>,
    input_ulid: Ulid,
    entry_idx: u32,
) -> Result<(&'a InputState, &'a SegmentEntry), MaterialiseOutcome> {
    let state = input(ctx, input_ulid)?;
    let entry = state.entries.get(entry_idx as usize).ok_or_else(|| {
        MaterialiseOutcome::from(MaterialiseError::MissingInput(format!(
            "input {input_ulid} entry {entry_idx} out of range (len={})",
            state.entries.len()
        )))
    })?;
    Ok((state, entry))
}

fn emit_keep(
    input_ulid: Ulid,
    entry_idx: u32,
    ctx: &MaterialiseCtx<'_>,
    out_entries: &mut Vec<SegmentEntry>,
    delta_body: &mut Vec<u8>,
) -> Result<(), MaterialiseOutcome> {
    let (state, entry) = input_entry(ctx, input_ulid, entry_idx)?;
    match entry.kind {
        EntryKind::Zero => {
            out_entries.push(SegmentEntry::new_zero(entry.start_lba, entry.lba_length));
        }
        EntryKind::DedupRef => {
            out_entries.push(SegmentEntry::new_dedup_ref(
                entry.hash,
                entry.start_lba,
                entry.lba_length,
            ));
        }
        EntryKind::Data => {
            let bytes =
                read_input_extent_stored_bytes(input_ulid, entry_idx, state, entry, ctx.resolver)?;
            verify_body_hash(entry, &bytes)?;
            out_entries.push(SegmentEntry::new_data(
                entry.hash,
                entry.start_lba,
                entry.lba_length,
                compression_flags(entry),
                bytes,
            ));
        }
        EntryKind::Inline => {
            let bytes = read_input_inline_stored_bytes(state, entry)?;
            verify_body_hash(entry, bytes)?;
            out_entries.push(SegmentEntry::new_data(
                entry.hash,
                entry.start_lba,
                entry.lba_length,
                compression_flags(entry),
                bytes.to_vec(),
            ));
        }
        EntryKind::CanonicalData => {
            let bytes =
                read_input_extent_stored_bytes(input_ulid, entry_idx, state, entry, ctx.resolver)?;
            verify_body_hash(entry, &bytes)?;
            let built = SegmentEntry::new_data(entry.hash, 0, 0, compression_flags(entry), bytes);
            out_entries.push(built.into_canonical());
        }
        EntryKind::CanonicalInline => {
            let bytes = read_input_inline_stored_bytes(state, entry)?;
            verify_body_hash(entry, bytes)?;
            let built =
                SegmentEntry::new_data(entry.hash, 0, 0, compression_flags(entry), bytes.to_vec());
            out_entries.push(built.into_canonical());
        }
        EntryKind::Delta => {
            // Delta carries no body bytes of its own — the delta blob lives
            // in the source segment's delta body section. Copy each of the
            // entry's delta blobs into the output delta body and rewrite the
            // options' `delta_offset` to point at the new location.
            let _ = entry_idx;
            let mut new_options = Vec::with_capacity(entry.delta_options.len());
            for opt in &entry.delta_options {
                let blob = read_input_delta_blob(
                    input_ulid,
                    state,
                    opt.delta_offset,
                    opt.delta_length,
                    ctx.resolver,
                )?;
                let new_offset = delta_body.len() as u64;
                delta_body.extend_from_slice(&blob);
                let mut new_opt = opt.clone();
                new_opt.delta_offset = new_offset;
                new_options.push(new_opt);
            }
            out_entries.push(SegmentEntry::new_delta(
                entry.hash,
                entry.start_lba,
                entry.lba_length,
                new_options,
            ));
        }
    }
    Ok(())
}

fn emit_zero_split(
    input_ulid: Ulid,
    entry_idx: u32,
    start_lba: u64,
    lba_length: u32,
    ctx: &MaterialiseCtx<'_>,
    out_entries: &mut Vec<SegmentEntry>,
) -> Result<(), MaterialiseOutcome> {
    let (_, entry) = input_entry(ctx, input_ulid, entry_idx)?;
    if entry.kind != EntryKind::Zero {
        return Err(MaterialiseError::Internal(format!(
            "zero_split referenced non-Zero entry (input={input_ulid} idx={entry_idx} kind={:?})",
            entry.kind
        ))
        .into());
    }
    out_entries.push(SegmentEntry::new_zero(start_lba, lba_length));
    Ok(())
}

fn emit_canonical(
    input_ulid: Ulid,
    entry_idx: u32,
    ctx: &MaterialiseCtx<'_>,
    out_entries: &mut Vec<SegmentEntry>,
) -> Result<(), MaterialiseOutcome> {
    let (state, entry) = input_entry(ctx, input_ulid, entry_idx)?;
    match entry.kind {
        EntryKind::Data | EntryKind::CanonicalData => {
            let bytes =
                read_input_extent_stored_bytes(input_ulid, entry_idx, state, entry, ctx.resolver)?;
            verify_body_hash(entry, &bytes)?;
            let built = SegmentEntry::new_data(entry.hash, 0, 0, compression_flags(entry), bytes);
            out_entries.push(built.into_canonical());
        }
        EntryKind::Inline | EntryKind::CanonicalInline => {
            let bytes = read_input_inline_stored_bytes(state, entry)?;
            verify_body_hash(entry, bytes)?;
            let built =
                SegmentEntry::new_data(entry.hash, 0, 0, compression_flags(entry), bytes.to_vec());
            out_entries.push(built.into_canonical());
        }
        EntryKind::Delta => {
            // Partial-death Delta with external ref: reconstruct the
            // composite body and emit as a canonical full-body entry (not
            // re-encoded as a Delta), so dedup reads resolve in O(1).
            let mut cache: HashMap<(Ulid, u32), Vec<u8>> = HashMap::new();
            let composite = resolve_composite_body(input_ulid, entry_idx, ctx, &mut cache)?;
            let built = SegmentEntry::new_data(entry.hash, 0, 0, SegmentFlags::empty(), composite);
            out_entries.push(built.into_canonical());
        }
        other => {
            return Err(MaterialiseError::Internal(format!(
                "canonical plan output references unsupported entry kind {other:?} \
                 (input={input_ulid} idx={entry_idx})"
            ))
            .into());
        }
    }
    Ok(())
}

/// Emit one fresh Data entry for a surviving sub-run of a partial-death
/// entry. Resolves the composite body (using a cache keyed by `(input,
/// entry_idx)`) and slices at the requested offset.
#[allow(clippy::too_many_arguments)]
fn emit_run(
    input_ulid: Ulid,
    entry_idx: u32,
    payload_block_offset: u32,
    start_lba: u64,
    lba_length: u32,
    ctx: &MaterialiseCtx<'_>,
    out_entries: &mut Vec<SegmentEntry>,
    composite_cache: &mut HashMap<(Ulid, u32), Vec<u8>>,
) -> Result<(), MaterialiseOutcome> {
    if lba_length == 0 {
        return Ok(());
    }
    let composite = resolve_composite_body(input_ulid, entry_idx, ctx, composite_cache)?;
    let start = payload_block_offset as usize * BLOCK_BYTES as usize;
    let end = start + lba_length as usize * BLOCK_BYTES as usize;
    if end > composite.len() {
        return Err(MaterialiseError::Internal(format!(
            "run out of range: input={input_ulid} idx={entry_idx} \
             run=[{start_lba}..{}) payload_offset={payload_block_offset} \
             run_len={} body_len={}",
            start_lba + lba_length as u64,
            lba_length as usize * BLOCK_BYTES as usize,
            composite.len()
        ))
        .into());
    }
    let slice = &composite[start..end];
    let run_hash = blake3::hash(slice);
    out_entries.push(SegmentEntry::new_data(
        run_hash,
        start_lba,
        lba_length,
        SegmentFlags::empty(),
        slice.to_vec(),
    ));
    Ok(())
}

/// Resolve the uncompressed composite body for an input entry, caching by
/// `(input, entry_idx)`. Dispatches on the input entry's kind:
/// - Data / Inline: read stored bytes (+ decompress)
/// - DedupRef: resolve the composite hash via the merged extent index
/// - Delta: resolve base + delta blob + `apply_delta`
fn resolve_composite_body(
    input_ulid: Ulid,
    entry_idx: u32,
    ctx: &MaterialiseCtx<'_>,
    cache: &mut HashMap<(Ulid, u32), Vec<u8>>,
) -> Result<Vec<u8>, MaterialiseOutcome> {
    if let Some(body) = cache.get(&(input_ulid, entry_idx)) {
        return Ok(body.clone());
    }
    let (state, entry) = input_entry(ctx, input_ulid, entry_idx)?;
    let body: Vec<u8> = match entry.kind {
        EntryKind::Data => {
            let stored =
                read_input_extent_stored_bytes(input_ulid, entry_idx, state, entry, ctx.resolver)?;
            verify_body_hash(entry, &stored)?;
            decompress_if_needed(&stored, entry.compressed)?
        }
        EntryKind::Inline => {
            let stored = read_input_inline_stored_bytes(state, entry)?;
            verify_body_hash(entry, stored)?;
            decompress_if_needed(stored, entry.compressed)?
        }
        EntryKind::DedupRef => resolve_body_by_hash_decompressed(&entry.hash, ctx)?,
        EntryKind::Delta => {
            let opt = entry
                .delta_options
                .iter()
                .find(|o| ctx.extent_index.lookup(&o.source_hash).is_some())
                .ok_or_else(|| {
                    MaterialiseOutcome::from(MaterialiseError::UnresolvableHash(format!(
                        "Delta: no delta_option source_hash resolves \
                         (input={input_ulid} idx={entry_idx} composite={})",
                        entry.hash
                    )))
                })?;
            let base = resolve_body_by_hash_decompressed(&opt.source_hash, ctx)?;
            let blob = read_input_delta_blob(
                input_ulid,
                state,
                opt.delta_offset,
                opt.delta_length,
                ctx.resolver,
            )?;
            crate::delta_compute::apply_delta(&base, &blob).map_err(|e| {
                MaterialiseOutcome::from(MaterialiseError::Internal(format!(
                    "apply_delta failed (input={input_ulid} idx={entry_idx}): {e}"
                )))
            })?
        }
        other => {
            return Err(MaterialiseError::Internal(format!(
                "composite body resolution on unsupported kind {other:?} \
                 (input={input_ulid} idx={entry_idx})"
            ))
            .into());
        }
    };
    let expected_len = entry.lba_length as usize * BLOCK_BYTES as usize;
    if body.len() != expected_len {
        return Err(MaterialiseError::Internal(format!(
            "composite body size mismatch: input={input_ulid} idx={entry_idx} \
             hash={} lba_length={} got={} expected={}",
            entry.hash,
            entry.lba_length,
            body.len(),
            expected_len
        ))
        .into());
    }
    cache.insert((input_ulid, entry_idx), body.clone());
    Ok(body)
}

fn compression_flags(entry: &SegmentEntry) -> SegmentFlags {
    if entry.compressed {
        SegmentFlags::COMPRESSED
    } else {
        SegmentFlags::empty()
    }
}

fn decompress_if_needed(stored: &[u8], compressed: bool) -> Result<Vec<u8>, MaterialiseOutcome> {
    if compressed {
        lz4_flex::decompress_size_prepended(stored).map_err(|e| {
            MaterialiseOutcome::from(MaterialiseError::BodyIntegrity(format!(
                "lz4 decompress failed: {e}"
            )))
        })
    } else {
        Ok(stored.to_vec())
    }
}

/// Resolve the uncompressed body for `hash` via the merged extent index and
/// the body resolver. Returns `UnresolvableHash` if the index has no entry
/// for this hash.
fn resolve_body_by_hash_decompressed(
    hash: &blake3::Hash,
    ctx: &MaterialiseCtx<'_>,
) -> Result<Vec<u8>, MaterialiseOutcome> {
    let loc = ctx
        .extent_index
        .lookup(hash)
        .ok_or_else(|| {
            MaterialiseOutcome::from(MaterialiseError::UnresolvableHash(format!(
                "hash {hash} not in merged extent index"
            )))
        })?
        .clone();

    if let Some(idata) = &loc.inline_data {
        return decompress_if_needed(idata, loc.compressed);
    }

    let (path, layout) =
        ctx.resolver
            .find_segment(loc.segment_id, loc.body_section_start, loc.body_source)?;
    let seek = layout.body_section_file_offset(loc.body_section_start) + loc.body_offset;
    let mut f = fs::File::open(&path)?;
    f.seek(SeekFrom::Start(seek))?;
    let mut buf = vec![0u8; loc.body_length as usize];
    f.read_exact(&mut buf)?;
    decompress_if_needed(&buf, loc.compressed)
}

/// Read the stored (possibly compressed) bytes for a Data-kind entry in an
/// input segment. For input segments, the body lives in `cache/<id>.body`
/// (written during input's original drain/promote cycle) or in a still-local
/// `pending/<id>` / `wal/<id>` / `gc/<id>` file.
///
/// `entry_idx` is the index of `entry` within the input segment's entry
/// table — the resolver passes it through as `BodySource::Cached(entry_idx)`
/// so the `.present`-bit gate correctly admits a cache hit on the right
/// entry (and triggers a demand-fetch if the bit is unset).
fn read_input_extent_stored_bytes(
    input_ulid: Ulid,
    entry_idx: u32,
    state: &InputState,
    entry: &SegmentEntry,
    resolver: &dyn BodyResolver,
) -> Result<Vec<u8>, MaterialiseOutcome> {
    if !entry.kind.has_body_bytes()
        || matches!(entry.kind, EntryKind::Inline | EntryKind::CanonicalInline)
    {
        return Err(MaterialiseError::Internal(format!(
            "stored-byte read requested for non-extent kind {:?} (input={input_ulid})",
            entry.kind
        ))
        .into());
    }
    // Input segments are always `BodyOnly` in production (their body has
    // been promoted to cache) or `FullSegment` while still in pending/wal
    // (pre-promotion). Either layout is acceptable; the resolver tells us.
    let (path, layout) = resolver.find_segment(
        input_ulid,
        state.body_section_start,
        BodySource::Cached(entry_idx),
    )?;
    let seek = layout.body_section_file_offset(state.body_section_start) + entry.stored_offset;
    let mut f = fs::File::open(&path)?;
    f.seek(SeekFrom::Start(seek))?;
    let mut buf = vec![0u8; entry.stored_length as usize];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read stored bytes for an Inline-kind entry from the input's inline
/// section (already loaded into memory during [`load_input_states`]).
fn read_input_inline_stored_bytes<'a>(
    state: &'a InputState,
    entry: &SegmentEntry,
) -> Result<&'a [u8], MaterialiseOutcome> {
    if !matches!(entry.kind, EntryKind::Inline | EntryKind::CanonicalInline) {
        return Err(MaterialiseError::Internal(format!(
            "inline read requested for non-inline kind {:?}",
            entry.kind
        ))
        .into());
    }
    let start = entry.stored_offset as usize;
    let end = start + entry.stored_length as usize;
    state.inline.get(start..end).ok_or_else(|| {
        MaterialiseOutcome::from(MaterialiseError::Internal(format!(
            "inline slice out of range: {start}..{end} vs len={}",
            state.inline.len()
        )))
    })
}

/// Read `delta_length` bytes of a delta blob from an input segment at
/// `delta_offset` within its delta body section.
///
/// Delta entries carry no body bytes of their own, so the `.present` bitmap
/// has no bit for them — we use [`BodyResolver::locate_segment_unchecked`]
/// to find the segment file without presence gating. When the input is in
/// full-segment form (wal / pending / bare gc), the delta blob lives at
/// `body_section_start + body_length + delta_offset` within the same file;
/// when the input has been promoted to cache, the delta blob is in a
/// sibling `cache/<id>.delta` file at offset `delta_offset`.
fn read_input_delta_blob(
    input_ulid: Ulid,
    state: &InputState,
    delta_offset: u64,
    delta_length: u32,
    resolver: &dyn BodyResolver,
) -> Result<Vec<u8>, MaterialiseOutcome> {
    match resolver.locate_segment_unchecked(input_ulid) {
        Some((path, SegmentBodyLayout::FullSegment)) => {
            let seek = SegmentBodyLayout::FullSegment
                .body_section_file_offset(state.body_section_start)
                + state.body_length
                + delta_offset;
            let mut f = fs::File::open(&path)?;
            f.seek(SeekFrom::Start(seek))?;
            let mut buf = vec![0u8; delta_length as usize];
            f.read_exact(&mut buf)?;
            Ok(buf)
        }
        Some((_, SegmentBodyLayout::BodyOnly)) | None => {
            // Delta lives in a sibling `cache/<id>.delta` file (or is
            // demand-fetched by the resolver on miss).
            let mut f = resolver.open_delta_body(input_ulid)?;
            f.seek(SeekFrom::Start(delta_offset))?;
            let mut buf = vec![0u8; delta_length as usize];
            f.read_exact(&mut buf)?;
            Ok(buf)
        }
    }
}

fn verify_body_hash(entry: &SegmentEntry, body: &[u8]) -> Result<(), MaterialiseOutcome> {
    segment::verify_body_hash(entry, body)
        .map_err(|e| MaterialiseOutcome::from(MaterialiseError::BodyIntegrity(format!("{e}"))))
}

impl<'a> MaterialiseCtx<'a> {
    /// Construct a ctx by eagerly loading per-input idx state from disk.
    pub fn new(
        base_dir: &'a Path,
        inputs: &[Ulid],
        extent_index: &'a ExtentIndex,
        resolver: &'a dyn BodyResolver,
    ) -> Result<Self, MaterialiseOutcome> {
        let input_states = load_input_states(base_dir, inputs)?;
        Ok(Self {
            base_dir,
            input_states,
            extent_index,
            resolver,
        })
    }
}

/// Helper keeping `BoxFetcher` out of this module's public API — used only
/// by the production `Volume` wiring in `volume.rs` via a thin adapter.
#[doc(hidden)]
pub type VolumeFetcher = Option<BoxFetcher>;

#[cfg(test)]
mod tests {
    //! Unit tests drive materialisation against a mock `BodyResolver` backed
    //! by an in-memory map. The production `Volume` wiring is exercised by
    //! integration tests living alongside the existing
    //! `gc_delta_partial_death_compaction` deterministic test.

    use super::*;
    use crate::gc_plan::{GcPlan, PlanOutput};
    use crate::segment::{self, DeltaOption, SegmentEntry, SegmentFlags};

    struct MockResolver {
        files: HashMap<Ulid, (std::path::PathBuf, SegmentBodyLayout)>,
        delta_files: HashMap<Ulid, std::path::PathBuf>,
    }

    impl BodyResolver for MockResolver {
        fn find_segment(
            &self,
            segment_id: Ulid,
            _body_section_start: u64,
            _body_source: BodySource,
        ) -> io::Result<(std::path::PathBuf, SegmentBodyLayout)> {
            self.files.get(&segment_id).cloned().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("mock: no file for {segment_id}"),
                )
            })
        }

        fn locate_segment_unchecked(
            &self,
            segment_id: Ulid,
        ) -> Option<(std::path::PathBuf, SegmentBodyLayout)> {
            self.files.get(&segment_id).cloned()
        }

        fn open_delta_body(&self, segment_id: Ulid) -> io::Result<fs::File> {
            let path = self.delta_files.get(&segment_id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("mock: no delta file for {segment_id}"),
                )
            })?;
            fs::File::open(path)
        }
    }

    fn ephemeral_signer() -> std::sync::Arc<dyn segment::SegmentSigner> {
        let (s, _) = crate::signing::generate_ephemeral_signer();
        s
    }

    /// Build a fake input segment containing a fully-alive Data entry, drop
    /// it on disk as a FullSegment, return its ULID and path.
    fn write_simple_input(
        dir: &Path,
        body: &[u8],
    ) -> (Ulid, std::path::PathBuf, u64, blake3::Hash) {
        let ulid = Ulid::new();
        let path = dir.join(ulid.to_string());
        let hash = blake3::hash(body);
        let mut entries = vec![SegmentEntry::new_data(
            hash,
            100,
            (body.len() / BLOCK_BYTES as usize) as u32,
            SegmentFlags::empty(),
            body.to_vec(),
        )];
        let bss = segment::write_segment(&path, &mut entries, ephemeral_signer().as_ref()).unwrap();
        (ulid, path, bss, hash)
    }

    #[test]
    fn materialise_keep_passes_data_through() {
        let dir = tempfile::TempDir::new().unwrap();
        let body = vec![7u8; 4096];
        let (input_ulid, input_path, _bss, hash) = write_simple_input(dir.path(), &body);

        // Build idx-dir linkage: base_dir/index/<ulid>.idx
        let base = dir.path().join("vol");
        fs::create_dir_all(base.join("index")).unwrap();
        let idx = base.join("index").join(format!("{input_ulid}.idx"));
        // For this test we hand the full segment file as both idx and body
        // (idx header == full segment header + index; sufficient for parsing).
        fs::copy(&input_path, &idx).unwrap();

        let mut resolver = MockResolver {
            files: HashMap::new(),
            delta_files: HashMap::new(),
        };
        resolver.files.insert(
            input_ulid,
            (input_path.clone(), SegmentBodyLayout::FullSegment),
        );

        let index = ExtentIndex::default();
        let new_ulid = Ulid::new();
        let plan = GcPlan {
            new_ulid,
            outputs: vec![PlanOutput::Keep {
                input: input_ulid,
                entry_idx: 0,
            }],
        };

        let inputs = plan.inputs();
        let ctx = MaterialiseCtx::new(&base, &inputs, &index, &resolver).unwrap();
        let out = materialise_plan(&plan, &ctx).unwrap();
        assert_eq!(out.entries.len(), 1);
        let e = &out.entries[0];
        assert_eq!(e.kind, EntryKind::Data);
        assert_eq!(e.hash, hash);
        assert_eq!(e.start_lba, 100);
        assert_eq!(e.lba_length, 1);
        assert_eq!(e.data.as_deref(), Some(body.as_slice()));
        assert!(out.delta_body.is_empty());
    }

    #[test]
    fn materialise_zero_split_emits_fresh_zero() {
        let dir = tempfile::TempDir::new().unwrap();
        let base = dir.path().join("vol");
        fs::create_dir_all(base.join("index")).unwrap();

        let input_ulid = Ulid::new();
        let idx_path = base.join("index").join(format!("{input_ulid}.idx"));
        let mut entries = vec![SegmentEntry::new_zero(0, 1000)];
        segment::write_segment(&idx_path, &mut entries, ephemeral_signer().as_ref()).unwrap();

        let resolver = MockResolver {
            files: HashMap::new(),
            delta_files: HashMap::new(),
        };
        let index = ExtentIndex::default();
        let plan = GcPlan {
            new_ulid: Ulid::new(),
            outputs: vec![PlanOutput::ZeroSplit {
                input: input_ulid,
                entry_idx: 0,
                start_lba: 200,
                lba_length: 50,
            }],
        };
        let inputs = plan.inputs();
        let ctx = MaterialiseCtx::new(&base, &inputs, &index, &resolver).unwrap();
        let out = materialise_plan(&plan, &ctx).unwrap();
        assert_eq!(out.entries.len(), 1);
        let e = &out.entries[0];
        assert_eq!(e.kind, EntryKind::Zero);
        assert_eq!(e.start_lba, 200);
        assert_eq!(e.lba_length, 50);
    }

    #[test]
    fn materialise_missing_input_returns_cancel() {
        let dir = tempfile::TempDir::new().unwrap();
        let base = dir.path().join("vol");
        fs::create_dir_all(base.join("index")).unwrap();

        let resolver = MockResolver {
            files: HashMap::new(),
            delta_files: HashMap::new(),
        };
        let index = ExtentIndex::default();
        let input_ulid = Ulid::new();
        let err = MaterialiseCtx::new(&base, &[input_ulid], &index, &resolver).err();
        assert!(matches!(
            err,
            Some(MaterialiseOutcome::Cancel(MaterialiseError::MissingInput(
                _
            )))
        ));
    }

    // DeltaOption left available in scope for later tests.
    #[allow(dead_code)]
    fn _touch_delta_option() -> DeltaOption {
        DeltaOption {
            source_hash: blake3::hash(b"x"),
            delta_offset: 0,
            delta_length: 0,
            delta_hash: blake3::hash(b""),
        }
    }
}
