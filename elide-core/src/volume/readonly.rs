//! Read-only volume view and the ancestor-walk helpers it shares with the
//! writable open path. Split out of `volume/mod.rs` for legibility — no
//! behaviour change.
//!
//! See `volume/fork.rs` for fork creation and `volume/wal.rs` for the WAL
//! lifecycle helpers; both used to live here.

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use ulid::Ulid;

use crate::{
    extentindex::{self, BodySource},
    lbamap,
};

use super::{
    AncestorLayer, BoxFetcher, FileCache, find_segment_in_dirs, open_delta_body_in_dirs,
    read_extents,
};

/// Read-only view of a volume: rebuilds LBA map and extent index from
/// segments + ancestor chain, no WAL replay, no exclusive lock.
pub struct ReadonlyVolume {
    base_dir: PathBuf,
    ancestor_layers: Vec<AncestorLayer>,
    lbamap: lbamap::LbaMap,
    extent_index: extentindex::ExtentIndex,
    file_cache: RefCell<FileCache>,
    fetcher: Option<BoxFetcher>,
}

impl ReadonlyVolume {
    /// Open a volume directory for read-only access.
    ///
    /// Does not create `wal/`, does not acquire an exclusive lock, and does not
    /// replay the WAL. WAL records from an active writer on the same volume will
    /// not be visible. Intended for the `--readonly` NBD serve path.
    pub fn open(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Self> {
        let (ancestor_layers, lbamap, extent_index) = open_read_state(fork_dir, by_id_dir)?;
        Ok(Self {
            base_dir: fork_dir.to_owned(),
            ancestor_layers,
            lbamap,
            extent_index,
            file_cache: RefCell::new(FileCache::default()),
            fetcher: None,
        })
    }

    /// Read `lba_count` 4KB blocks starting at `start_lba`.
    /// Unwritten blocks are returned as zeros.
    pub fn read(&self, start_lba: u64, lba_count: u32) -> io::Result<Vec<u8>> {
        read_extents(
            start_lba,
            lba_count,
            &self.lbamap,
            &self.extent_index,
            &self.file_cache,
            |id, bss, idx| self.find_segment_file(id, bss, idx),
            |id| {
                open_delta_body_in_dirs(
                    id,
                    &self.base_dir,
                    &self.ancestor_layers,
                    self.fetcher.as_ref(),
                )
            },
        )
    }

    fn find_segment_file(
        &self,
        segment_id: Ulid,
        body_section_start: u64,
        body_source: BodySource,
    ) -> io::Result<PathBuf> {
        find_segment_in_dirs(
            segment_id,
            &self.base_dir,
            &self.ancestor_layers,
            self.fetcher.as_ref(),
            body_section_start,
            body_source,
        )
    }

    /// Attach a `SegmentFetcher` for demand-fetch on segment cache miss.
    pub fn set_fetcher(&mut self, fetcher: BoxFetcher) {
        self.fetcher = Some(fetcher);
    }

    /// Return all fork directories in the ancestry chain, oldest-first,
    /// with the current fork last.
    pub fn fork_dirs(&self) -> Vec<PathBuf> {
        self.ancestor_layers
            .iter()
            .map(|l| l.dir.clone())
            .chain(std::iter::once(self.base_dir.clone()))
            .collect()
    }
}

/// Walk the ancestry chain and rebuild the LBA map and extent index.
///
/// This is the common open-time setup shared by `Volume::open` and
/// `ReadonlyVolume::open`. Returns the ancestor layers (oldest-first, fork
/// parents first then extent-index sources deduped by dir), the rebuilt
/// LBA map, and the rebuilt extent index.
///
/// **Ancestor layer semantics have two jobs** and used to conflate them:
///
/// 1. *LBA-map contribution* — which volumes' segments claim LBAs that
///    should be visible in this volume's read view. This is strictly the
///    fork parent chain (`volume.parent`); extent-index sources never
///    contribute LBA claims.
/// 2. *Body lookup search path* — when an extent resolves via the extent
///    index to a canonical segment, where to find that segment's body on
///    disk (and where to route demand-fetches). **This must include
///    extent-index sources**, because a fork's parent may hold DedupRef
///    entries whose canonical bodies live in an extent-index source.
///    Earlier versions of this function only returned fork parents, which
///    caused silent zero-fill on fork reads through DedupRef — see
///    `docs/architecture.md`.
///
/// The rebuilt `LbaMap` is computed from `lba_chain` (fork-only, correct).
/// The returned `ancestor_layers` is the broader set (fork + extent), used
/// downstream by `find_segment_in_dirs`, `open_delta_body_in_dirs`,
/// `prepare_reclaim`, and `RemoteFetcher`'s search list.
pub(super) fn open_read_state(
    fork_dir: &Path,
    by_id_dir: &Path,
) -> io::Result<(Vec<AncestorLayer>, lbamap::LbaMap, extentindex::ExtentIndex)> {
    let total_started = std::time::Instant::now();

    // Fail-fast verification: every ancestor in the fork chain must have a
    // signed `.manifest` file whose listed `.idx` files are all present
    // locally. The trust chain is rooted in this volume's own pubkey and
    // walked via the `parent_pubkey` embedded in each child's provenance.
    let verify_started = std::time::Instant::now();
    verify_ancestor_manifests(fork_dir, by_id_dir)?;
    let verify_elapsed = verify_started.elapsed();

    let fork_walk_started = std::time::Instant::now();
    let fork_layers = walk_ancestors(fork_dir, by_id_dir)?;
    let fork_walk_elapsed = fork_walk_started.elapsed();
    let fork_layers_n = fork_layers.len();

    let lba_chain: Vec<(PathBuf, Option<String>)> = fork_layers
        .iter()
        .map(|l| (l.dir.clone(), l.branch_ulid.clone()))
        .chain(std::iter::once((fork_dir.to_owned(), None)))
        .collect();
    let lbamap_started = std::time::Instant::now();
    let lbamap = lbamap::rebuild_segments(&lba_chain)?;
    let lbamap_elapsed = lbamap_started.elapsed();

    // Extent-index sources: recursed across the fork chain by
    // `walk_extent_ancestors`. They contribute canonical hashes to the
    // extent index and must also be searchable for body lookups.
    let extent_walk_started = std::time::Instant::now();
    let extent_sources = walk_extent_ancestors(fork_dir, by_id_dir)?;
    let extent_walk_elapsed = extent_walk_started.elapsed();

    // Build the hash chain for extent-index rebuild: fork chain + extent
    // sources (deduped by dir). `extent_index.lookup` returns canonical
    // locations populated from both.
    let mut hash_chain = lba_chain;
    for layer in &extent_sources {
        if !hash_chain.iter().any(|(dir, _)| dir == &layer.dir) {
            hash_chain.push((layer.dir.clone(), layer.branch_ulid.clone()));
        }
    }
    let hash_chain_len = hash_chain.len();
    let extent_rebuild_started = std::time::Instant::now();
    let extent_index = extentindex::rebuild(&hash_chain)?;
    let extent_rebuild_elapsed = extent_rebuild_started.elapsed();

    // The returned `ancestor_layers` unifies fork parents and extent
    // sources. Callers use this as the body-lookup search path; the
    // LBA-map-only subset was already consumed above.
    let mut ancestor_layers = fork_layers;
    for layer in extent_sources {
        if !ancestor_layers.iter().any(|l| l.dir == layer.dir) {
            ancestor_layers.push(layer);
        }
    }

    log::info!(
        "[open_read_state] {} in {:.2?} (verify_manifests {:.2?}, walk_ancestors {:.2?}, lbamap::rebuild_segments {:.2?}, walk_extent_ancestors {:.2?}, extentindex::rebuild {:.2?}, fork_layers={}, hash_chain={})",
        fork_dir.display(),
        total_started.elapsed(),
        verify_elapsed,
        fork_walk_elapsed,
        lbamap_elapsed,
        extent_walk_elapsed,
        extent_rebuild_elapsed,
        fork_layers_n,
        hash_chain_len,
    );
    Ok((ancestor_layers, lbamap, extent_index))
}

/// Parse a `<source-ulid>/<snapshot-ulid>` lineage entry, validating
/// both components as ULIDs to prevent path traversal. Returns the source ULID
/// slice (borrowed from `entry`) and the owned snapshot ULID string.
fn parse_lineage_entry<'a>(
    entry: &'a str,
    field: &str,
    fork_dir: &Path,
) -> io::Result<(&'a str, String)> {
    let (source_ulid_str, snapshot_ulid_str) = entry.split_once('/').ok_or_else(|| {
        io::Error::other(format!(
            "malformed {field} entry in {}: {entry}",
            fork_dir.display()
        ))
    })?;
    if snapshot_ulid_str.contains('/') {
        return Err(io::Error::other(format!(
            "malformed {field} entry in {}: {entry} has more than one '/' separator",
            fork_dir.display()
        )));
    }
    let snapshot_ulid = Ulid::from_string(snapshot_ulid_str)
        .map_err(|e| io::Error::other(format!("bad snapshot ULID in {field}: {e}")))?
        .to_string();
    Ulid::from_string(source_ulid_str).map_err(|_| {
        io::Error::other(format!(
            "malformed {field} entry in {}: source '{source_ulid_str}' is not a valid ULID",
            fork_dir.display(),
        ))
    })?;
    Ok((source_ulid_str, snapshot_ulid))
}

/// A volume with no `volume.provenance` is treated as root (empty chain).
/// All other provenance read errors propagate — in particular, a missing
/// or malformed file on a volume that had lineage is a loud failure.
fn load_lineage_or_empty(fork_dir: &Path) -> io::Result<crate::signing::ProvenanceLineage> {
    let provenance_path = fork_dir.join(crate::signing::VOLUME_PROVENANCE_FILE);
    if !provenance_path.exists() {
        return Ok(crate::signing::ProvenanceLineage::default());
    }
    crate::signing::read_lineage_verifying_signature(
        fork_dir,
        crate::signing::VOLUME_PUB_FILE,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )
}

/// Resolve an ancestor volume directory by ULID.
///
/// All ancestors — writable, imported readonly bases, and ancestors pulled
/// from S3 to satisfy a child's lineage — live in `by_id/<ulid>/`. The
/// returned path is deterministic so callers (and tests) can report it in
/// errors even if it does not yet exist.
pub fn resolve_ancestor_dir(by_id_dir: &Path, ulid: &str) -> PathBuf {
    by_id_dir.join(ulid)
}

/// Verify every ancestor of `fork_dir` by walking the fork chain from the
/// current volume, using the `parent_pubkey` embedded in each child's
/// signed provenance as the trust anchor for the next link.
///
/// For each ancestor in the chain:
/// 1. Verify the ancestor's `volume.provenance` under the pubkey the child
///    signed over (NOT the `volume.pub` on disk at the ancestor path).
/// 2. Read the ancestor's `snapshots/<snap_ulid>.manifest` file, also
///    verified under the same pubkey.
/// 3. Assert every segment ULID listed in the manifest is present as
///    `index/<ulid>.idx` in the ancestor directory.
///
/// Fails fast on any missing file, failed signature, or missing `.idx`.
/// Does not perform any demand-fetch — the caller is expected to prefetch
/// ancestor data before opening a fork.
///
/// The trust root is the current volume's own `volume.pub`, which the
/// caller has already validated as the identity of the volume they asked
/// to open.
pub fn verify_ancestor_manifests(fork_dir: &Path, by_id_dir: &Path) -> io::Result<()> {
    // Fast-path: if this volume has no parent, nothing to verify.
    let provenance_path = fork_dir.join(crate::signing::VOLUME_PROVENANCE_FILE);
    if !provenance_path.exists() {
        return Ok(());
    }
    let own_pubkey = crate::signing::load_verifying_key(fork_dir, crate::signing::VOLUME_PUB_FILE)?;
    let own_lineage = crate::signing::read_lineage_with_key(
        fork_dir,
        &own_pubkey,
        crate::signing::VOLUME_PROVENANCE_FILE,
    )?;
    let Some(mut current_parent) = own_lineage.parent else {
        return Ok(());
    };

    loop {
        let parent_dir = resolve_ancestor_dir(by_id_dir, &current_parent.volume_ulid);
        if !parent_dir.exists() {
            return Err(io::Error::other(format!(
                "ancestor {} not found locally (run `elide volume remote pull` first)",
                current_parent.volume_ulid
            )));
        }
        let parent_verifying = crate::signing::VerifyingKey::from_bytes(&current_parent.pubkey)
            .map_err(|e| {
                io::Error::other(format!(
                    "invalid parent pubkey in provenance for {}: {e}",
                    current_parent.volume_ulid
                ))
            })?;
        // For forker-attested "now" pins the `.manifest` is signed by a
        // different (ephemeral) key than the parent's identity. When set,
        // use it for the manifest; fall back to the identity key otherwise.
        let manifest_verifying = match current_parent.manifest_pubkey {
            Some(bytes) => crate::signing::VerifyingKey::from_bytes(&bytes).map_err(|e| {
                io::Error::other(format!(
                    "invalid parent manifest pubkey in provenance for {}: {e}",
                    current_parent.volume_ulid
                ))
            })?,
            None => parent_verifying,
        };

        let snap_ulid = Ulid::from_string(&current_parent.snapshot_ulid).map_err(|e| {
            io::Error::other(format!("invalid snapshot ULID in provenance parent: {e}"))
        })?;
        let segments =
            crate::signing::read_snapshot_manifest(&parent_dir, &manifest_verifying, &snap_ulid)?
                .segment_ulids;

        let index_dir = parent_dir.join("index");
        for seg in &segments {
            let idx_path = index_dir.join(format!("{seg}.idx"));
            if !idx_path.exists() {
                return Err(io::Error::other(format!(
                    "ancestor {} snapshot {}: missing index/{}.idx",
                    current_parent.volume_ulid, snap_ulid, seg
                )));
            }
        }

        // Advance to this ancestor's own parent (if any), verifying its
        // provenance under the identity key we already trust (from the
        // previous child's embedded parent_pubkey).
        let parent_lineage = crate::signing::read_lineage_with_key(
            &parent_dir,
            &parent_verifying,
            crate::signing::VOLUME_PROVENANCE_FILE,
        )?;
        let Some(next) = parent_lineage.parent else {
            return Ok(());
        };
        current_parent = next;
    }
}

/// Walk the fork ancestry chain and return ancestor layers, oldest-first.
/// Public so that `ls.rs` and other read-only tools can build the rebuild chain.
pub fn walk_ancestors(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Vec<AncestorLayer>> {
    let lineage = load_lineage_or_empty(fork_dir)?;
    let Some(parent) = lineage.parent else {
        return Ok(Vec::new());
    };
    let parent_fork_dir = resolve_ancestor_dir(by_id_dir, &parent.volume_ulid);

    // Recurse into the parent's fork chain first (builds oldest-first order).
    let mut ancestors = walk_ancestors(&parent_fork_dir, by_id_dir)?;
    ancestors.push(AncestorLayer {
        dir: parent_fork_dir,
        branch_ulid: Some(parent.snapshot_ulid),
    });
    Ok(ancestors)
}

/// Collect all extent-index source volumes reachable from `fork_dir`,
/// recursing through the fork-parent chain.
///
/// The `extent_index` field of a `volume.provenance` is a flat list of
/// `<source-ulid>/<snapshot-ulid>` entries, each naming a snapshot whose
/// extents populate the volume's `ExtentIndex` for dedup / delta source
/// lookups. At write time these hashes are consulted to decide whether
/// to emit a thin `DedupRef` / `Delta` entry instead of a fresh body.
///
/// **At read time**, every volume in the fork chain may contain thin
/// entries whose canonical bodies live in an extent-index source listed
/// by *that* ancestor. A fork child must therefore see the union of every
/// ancestor's extent-index sources, not just its own (`fork_volume` writes
/// an empty `extent_index` for forks — see `volume/fork.rs`).
/// Without this recursion, a fork reading through DedupRef entries in its
/// parent silently zero-fills, because the extent_index rebuild would
/// never scan the source that owns the canonical body.
///
/// The `extent_index` field itself is flat at attach time (the coordinator
/// concatenates + dedupes the sources' own lists during import), so each
/// layer we visit contributes a fully-expanded set. This function's job
/// is the orthogonal recursion across *fork parents*: we walk `lineage.parent`
/// from `fork_dir` upward, unioning each volume's `extent_index`.
///
/// Dedup is by `source_dir`; when multiple ancestors reference the same
/// source at different snapshots, we keep the lexicographically greatest
/// `snapshot_ulid` — that's the cutoff that includes the most data.
pub fn walk_extent_ancestors(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Vec<AncestorLayer>> {
    let mut layers: Vec<AncestorLayer> = Vec::new();
    let mut cursor: Option<PathBuf> = Some(fork_dir.to_owned());
    while let Some(dir) = cursor {
        let lineage = load_lineage_or_empty(&dir)?;
        for entry in &lineage.extent_index {
            let (source_ulid_str, snapshot_ulid) =
                parse_lineage_entry(entry, "extent_index", &dir)?;
            let source_dir = resolve_ancestor_dir(by_id_dir, source_ulid_str);
            match layers.iter_mut().find(|l| l.dir == source_dir) {
                Some(existing) => {
                    if existing
                        .branch_ulid
                        .as_deref()
                        .is_none_or(|prev| snapshot_ulid.as_str() > prev)
                    {
                        existing.branch_ulid = Some(snapshot_ulid);
                    }
                }
                None => {
                    layers.push(AncestorLayer {
                        dir: source_dir,
                        branch_ulid: Some(snapshot_ulid),
                    });
                }
            }
        }
        cursor = lineage
            .parent
            .map(|p| resolve_ancestor_dir(by_id_dir, &p.volume_ulid));
    }
    Ok(layers)
}

/// Return the latest snapshot ULID string for a fork, or `None` if no
/// snapshots exist. Snapshots live as plain files under `fork_dir/snapshots/`.
pub fn latest_snapshot(fork_dir: &Path) -> io::Result<Option<Ulid>> {
    let snapshots_dir = fork_dir.join("snapshots");
    let iter = match fs::read_dir(&snapshots_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let latest = iter
        .filter_map(|e| e.ok())
        .filter_map(|e| Ulid::from_string(e.file_name().to_str()?).ok())
        .max();
    Ok(latest)
}

#[cfg(test)]
mod tests {
    use super::super::Volume;
    use super::super::fork::fork_volume;
    use super::super::test_util::*;
    use super::*;
    use std::fs;
    use std::sync::Arc;

    // --- walk_ancestors tests ---

    #[test]
    fn walk_ancestors_root_returns_empty() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        // No origin file → root volume; ancestors are empty.
        assert!(walk_ancestors(&vol_dir, &by_id).unwrap().is_empty());
    }

    #[test]
    fn walk_ancestors_rejects_invalid_parent_entries() {
        let by_id = temp_dir();
        let fork_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        let bad_parents = [
            // not a ULID parent (old "base/" prefix)
            "base/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // path traversal attempt
            "../01AAAAAAAAAAAAAAAAAAAAAAAA/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // parent component is not a valid ULID
            "not-a-ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // missing '/' separator entirely
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            // branch ULID missing
            "01AAAAAAAAAAAAAAAAAAAAAAAA/",
        ];
        for bad in bad_parents {
            write_test_provenance(&fork_dir, Some(bad), &[]);
            assert!(
                walk_ancestors(&fork_dir, &by_id).is_err(),
                "expected error for parent entry: {bad}"
            );
        }
    }

    #[test]
    fn walk_ancestors_one_level() {
        let by_id = temp_dir();
        let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let default_dir = by_id.join(parent_ulid);
        let dev_dir = by_id.join(child_ulid);

        // dev's provenance names default at a fixed branch ULID.
        write_test_provenance(
            &dev_dir,
            Some(&format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
            &[],
        );

        let ancestors = walk_ancestors(&dev_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 1);
        assert_eq!(ancestors[0].dir, default_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_ancestors_two_levels() {
        let by_id = temp_dir();
        let root_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let mid_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let leaf_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let default_dir = by_id.join(root_ulid);
        let mid_dir = by_id.join(mid_ulid);
        let leaf_dir = by_id.join(leaf_ulid);

        write_test_provenance(
            &mid_dir,
            Some(&format!("{root_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV")),
            &[],
        );
        write_test_provenance(
            &leaf_dir,
            Some(&format!("{mid_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV")),
            &[],
        );

        let ancestors = walk_ancestors(&leaf_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 2);
        assert_eq!(ancestors[0].dir, default_dir);
        assert_eq!(ancestors[1].dir, mid_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            ancestors[1].branch_ulid.as_deref(),
            Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
        );
    }

    // --- walk_extent_ancestors tests ---

    #[test]
    fn walk_extent_ancestors_missing_file_is_empty() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        fs::create_dir_all(&vol_dir).unwrap();
        assert!(walk_extent_ancestors(&vol_dir, &by_id).unwrap().is_empty());
    }

    #[test]
    fn walk_extent_ancestors_rejects_invalid_entries() {
        let by_id = temp_dir();
        let vol_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        let bad_entries = [
            "not-a-ulid/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "../01AAAAAAAAAAAAAAAAAAAAAAAA/01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01AAAAAAAAAAAAAAAAAAAAAAAA/",
            "01AAAAAAAAAAAAAAAAAAAAAAAA",
        ];
        for bad in bad_entries {
            write_test_provenance(&vol_dir, None, &[bad]);
            assert!(
                walk_extent_ancestors(&vol_dir, &by_id).is_err(),
                "expected error for extent_index entry: {bad}"
            );
        }
    }

    #[test]
    fn walk_extent_ancestors_one_level() {
        let by_id = temp_dir();
        let parent_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let child_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let parent_dir = by_id.join(parent_ulid);
        let child_dir = by_id.join(child_ulid);
        let entry = format!("{parent_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        write_test_provenance(&child_dir, None, &[&entry]);

        let ancestors = walk_extent_ancestors(&child_dir, &by_id).unwrap();
        assert_eq!(ancestors.len(), 1);
        assert_eq!(ancestors[0].dir, parent_dir);
        assert_eq!(
            ancestors[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_extent_ancestors_multi_entry() {
        // Flat union of several sources in a single signed provenance.
        let by_id = temp_dir();
        let a_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let b_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let a_dir = by_id.join(a_ulid);
        let b_dir = by_id.join(b_ulid);
        let c_dir = by_id.join(c_ulid);
        let a_entry = format!("{a_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let b_entry = format!("{b_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
        write_test_provenance(&c_dir, None, &[&a_entry, &b_entry]);

        let layers = walk_extent_ancestors(&c_dir, &by_id).unwrap();
        assert_eq!(layers.len(), 2, "two sources expected");
        assert_eq!(layers[0].dir, a_dir);
        assert_eq!(layers[1].dir, b_dir);
        assert_eq!(
            layers[0].branch_ulid.as_deref(),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
        assert_eq!(
            layers[1].branch_ulid.as_deref(),
            Some("01BX5ZZKJKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn walk_extent_ancestors_dedupes_duplicate_entries() {
        let by_id = temp_dir();
        let a_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let a_dir = by_id.join(a_ulid);
        let c_dir = by_id.join(c_ulid);
        // Same source listed twice — later entry is silently dropped.
        let a1 = format!("{a_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let a2 = format!("{a_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
        write_test_provenance(&c_dir, None, &[&a1, &a2]);

        let layers = walk_extent_ancestors(&c_dir, &by_id).unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].dir, a_dir);
    }

    #[test]
    fn walk_extent_ancestors_combined_with_walk_ancestors() {
        // A single signed provenance carrying both fork parent (P) and
        // extent-index source (X). walk_ancestors returns [P],
        // walk_extent_ancestors returns [X]: the two chains are distinct.
        let by_id = temp_dir();
        let p_ulid = "01AAAAAAAAAAAAAAAAAAAAAAAA";
        let x_ulid = "01BBBBBBBBBBBBBBBBBBBBBBBB";
        let c_ulid = "01CCCCCCCCCCCCCCCCCCCCCCCC";
        let p_dir = by_id.join(p_ulid);
        let x_dir = by_id.join(x_ulid);
        let c_dir = by_id.join(c_ulid);
        let parent_entry = format!("{p_ulid}/01ARZ3NDEKTSV4RRFFQ69G5FAV");
        let extent_entry = format!("{x_ulid}/01BX5ZZKJKTSV4RRFFQ69G5FAV");
        write_test_provenance(&c_dir, Some(&parent_entry), &[&extent_entry]);

        let fork_chain = walk_ancestors(&c_dir, &by_id).unwrap();
        let extent_chain = walk_extent_ancestors(&c_dir, &by_id).unwrap();
        assert_eq!(fork_chain.len(), 1);
        assert_eq!(fork_chain[0].dir, p_dir);
        assert_eq!(extent_chain.len(), 1);
        assert_eq!(extent_chain[0].dir, x_dir);
    }

    // --- ReadonlyVolume tests ---

    #[test]
    fn readonly_volume_unwritten_returns_zeros() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        // Create the directory structure without a WAL (simulating a readonly base).
        fs::create_dir_all(fork_dir.join("pending")).unwrap();

        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), vec![0u8; 4096]);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_volume_reads_committed_segment() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        write_test_keypair(&fork_dir);

        let data = vec![0xCCu8; 4096];

        // Write data into the fork via Volume, then drop the lock.
        {
            let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
            vol.write(0, &data).unwrap();
            vol.promote_for_test().unwrap();
        }
        // Remove wal/ so ReadonlyVolume::open doesn't see a live WAL.
        // (ReadonlyVolume intentionally skips WAL replay; this also tests the
        //  no-WAL path.)
        fs::remove_dir_all(fork_dir.join("wal")).unwrap();

        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), data);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_volume_reads_ancestor_data() {
        let by_id = temp_dir();
        let default_dir = by_id.join("01AAAAAAAAAAAAAAAAAAAAAAAA");
        let child_dir = by_id.join("01BBBBBBBBBBBBBBBBBBBBBBBB");
        write_test_keypair(&default_dir);

        let ancestor_data = vec![0xDDu8; 4096];

        // Write data into default, snapshot, fork.
        {
            let mut vol = Volume::open(&default_dir, &by_id).unwrap();
            vol.write(0, &ancestor_data).unwrap();
            vol.snapshot().unwrap();
        }
        fork_volume(&child_dir, &default_dir).unwrap();
        // ReadonlyVolume doesn't take a write lock, so this always works.

        let rv = ReadonlyVolume::open(&child_dir, &by_id).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), ancestor_data);

        fs::remove_dir_all(by_id).unwrap();
    }

    /// Regression test for the fork-from-remote demand-fetch bug: when a
    /// forked child needs to demand-fetch a segment that belongs to an
    /// ancestor, `find_segment_in_dirs` must route the fetcher at the
    /// ancestor's `index/` and `cache/` directories — not the child's.
    /// The child's `index/` does not hold ancestor `.idx` files; using the
    /// child's dirs fails with ENOENT on the very first read.
    #[test]
    fn find_segment_in_dirs_routes_fetcher_at_ancestor_index_dir() {
        use crate::extentindex::BodySource;
        use std::sync::Mutex;

        struct OwnerAssertingFetcher {
            captured: Mutex<Option<(PathBuf, PathBuf)>>,
        }

        impl crate::segment::SegmentFetcher for OwnerAssertingFetcher {
            fn fetch_extent(
                &self,
                segment_id: Ulid,
                index_dir: &Path,
                body_dir: &Path,
                _extent: &crate::segment::ExtentFetch,
            ) -> io::Result<()> {
                *self.captured.lock().unwrap() = Some((index_dir.to_owned(), body_dir.to_owned()));
                // Simulate a successful fetch: write the body file where
                // the caller expects to find it on return.
                std::fs::create_dir_all(body_dir)?;
                std::fs::write(body_dir.join(format!("{segment_id}.body")), b"fake body")?;
                Ok(())
            }

            fn fetch_delta_body(
                &self,
                _segment_id: Ulid,
                _index_dir: &Path,
                _body_dir: &Path,
            ) -> io::Result<()> {
                Err(io::Error::other("unused"))
            }
        }

        let tmp = temp_dir();
        let child_dir = tmp.join("child");
        let ancestor_dir = tmp.join("ancestor");
        std::fs::create_dir_all(child_dir.join("index")).unwrap();
        std::fs::create_dir_all(ancestor_dir.join("index")).unwrap();

        // Only the ancestor holds the segment's `.idx`, matching the
        // fork-from-remote layout where each volume's signed index lives
        // in its own `index/` directory. Content is irrelevant — the
        // routing code only checks existence.
        let seg_ulid = Ulid::new();
        let idx_name = format!("{seg_ulid}.idx");
        std::fs::write(ancestor_dir.join("index").join(&idx_name), b"stub").unwrap();

        let layers = vec![AncestorLayer {
            dir: ancestor_dir.clone(),
            branch_ulid: None,
        }];
        let concrete = Arc::new(OwnerAssertingFetcher {
            captured: Mutex::new(None),
        });
        let fetcher: BoxFetcher = concrete.clone();

        let returned = find_segment_in_dirs(
            seg_ulid,
            &child_dir,
            &layers,
            Some(&fetcher),
            0,
            BodySource::Cached(0),
        )
        .expect("fetcher should have been routed at the ancestor's dirs");

        // The body must land under the ancestor, not the child.
        assert_eq!(
            returned,
            ancestor_dir.join("cache").join(format!("{seg_ulid}.body")),
        );
        assert!(
            !child_dir
                .join("cache")
                .join(format!("{seg_ulid}.body"))
                .exists(),
            "body must not be written under the child's cache dir"
        );

        // And the fetcher itself must have been called with the ancestor's
        // dirs — this is what the pre-fix code got wrong.
        let (idx_dir, body_dir) = concrete
            .captured
            .lock()
            .unwrap()
            .clone()
            .expect("fetcher must be called");
        assert_eq!(idx_dir, ancestor_dir.join("index"));
        assert_eq!(body_dir, ancestor_dir.join("cache"));

        fs::remove_dir_all(tmp).unwrap();
    }

    /// Complement to the previous test: when the child itself owns the
    /// segment (its own `index/` holds the `.idx`), the fetcher must be
    /// routed at the child's own dirs even if an ancestor is present.
    #[test]
    fn find_segment_in_dirs_prefers_self_over_ancestor_when_self_owns_idx() {
        use crate::extentindex::BodySource;
        use std::sync::Mutex;

        struct CaptureFetcher {
            captured: Mutex<Option<PathBuf>>,
        }
        impl crate::segment::SegmentFetcher for CaptureFetcher {
            fn fetch_extent(
                &self,
                segment_id: Ulid,
                index_dir: &Path,
                body_dir: &Path,
                _extent: &crate::segment::ExtentFetch,
            ) -> io::Result<()> {
                *self.captured.lock().unwrap() = Some(index_dir.to_owned());
                std::fs::create_dir_all(body_dir)?;
                std::fs::write(body_dir.join(format!("{segment_id}.body")), b"")?;
                Ok(())
            }
            fn fetch_delta_body(&self, _: Ulid, _: &Path, _: &Path) -> io::Result<()> {
                Err(io::Error::other("unused"))
            }
        }

        let tmp = temp_dir();
        let child_dir = tmp.join("child");
        let ancestor_dir = tmp.join("ancestor");
        std::fs::create_dir_all(child_dir.join("index")).unwrap();
        std::fs::create_dir_all(ancestor_dir.join("index")).unwrap();

        let seg_ulid = Ulid::new();
        let idx_name = format!("{seg_ulid}.idx");
        // Both self and ancestor have the `.idx`; self must win.
        std::fs::write(child_dir.join("index").join(&idx_name), b"stub").unwrap();
        std::fs::write(ancestor_dir.join("index").join(&idx_name), b"stub").unwrap();

        let layers = vec![AncestorLayer {
            dir: ancestor_dir.clone(),
            branch_ulid: None,
        }];
        let concrete = Arc::new(CaptureFetcher {
            captured: Mutex::new(None),
        });
        let fetcher: BoxFetcher = concrete.clone();

        let returned = find_segment_in_dirs(
            seg_ulid,
            &child_dir,
            &layers,
            Some(&fetcher),
            0,
            BodySource::Cached(0),
        )
        .unwrap();

        assert_eq!(
            returned,
            child_dir.join("cache").join(format!("{seg_ulid}.body")),
        );
        assert_eq!(
            concrete.captured.lock().unwrap().clone().unwrap(),
            child_dir.join("index")
        );

        fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn readonly_volume_does_not_see_wal_records() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("base");
        write_test_keypair(&fork_dir);

        let committed = vec![0xEEu8; 4096];
        let in_wal = vec![0xFFu8; 4096];

        // Write and promote LBA 0, then write LBA 1 to the WAL only.
        let mut vol = Volume::open(&fork_dir, &fork_dir).unwrap();
        vol.write(0, &committed).unwrap();
        vol.promote_for_test().unwrap();
        vol.write(1, &in_wal).unwrap();
        // Do NOT promote — LBA 1 is only in the WAL.
        // Drop the writable volume so the lock is released.
        drop(vol);

        // ReadonlyVolume skips WAL replay: LBA 1 must appear as zeros.
        let rv = ReadonlyVolume::open(&fork_dir, &fork_dir).unwrap();
        assert_eq!(rv.read(0, 1).unwrap(), committed);
        assert_eq!(rv.read(1, 1).unwrap(), vec![0u8; 4096]);

        fs::remove_dir_all(vol_dir).unwrap();
    }
}
