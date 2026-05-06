// Snapshot manifest must omit segments whose entries are all dead.
//
// The filter runs at sign time inside `execute_sign_snapshot_manifest` (and
// the in-process `Volume::snapshot`). A segment whose body hashes have all
// been displaced from the extent index *and* whose thin entries' LBAs no
// longer map to the entry's hash contributes nothing to a reader and is
// dropped from the manifest's segment list. The segment file itself is left
// alone — GC reclaims storage on its own cadence.

use std::path::Path;

use elide_core::signing::{self, VOLUME_PUB_FILE, read_snapshot_manifest};
use elide_core::volume::Volume;
use ulid::Ulid;

mod common;

fn block(seed: u8) -> Vec<u8> {
    vec![seed; 4096]
}

fn manifest_segments(fork_dir: &Path, snap_ulid: Ulid) -> Vec<Ulid> {
    let vk = signing::load_verifying_key(fork_dir, VOLUME_PUB_FILE).unwrap();
    let manifest = read_snapshot_manifest(fork_dir, &vk, &snap_ulid).unwrap();
    manifest.segment_ulids
}

/// Write zeros to LBA 0 → drain → seg A (a single `Zero` entry). Overwrite
/// LBA 0 with non-zero data → drain → seg B. Sign a snapshot manifest.
///
/// Seg A's only entry is `Zero(start_lba=0, lba_length=1)` and the lbamap
/// no longer maps LBA 0 to `ZERO_HASH` (B's write replaced it), so the
/// liveness predicate marks A's entry dead. Seg B carries the live Data
/// entry. The manifest must list B and omit A.
///
/// This is the easy "fully dead via thin entry" case the conservative
/// filter is designed to catch. The body-bearing-orphan case where a Data
/// entry's hash is still in the extent index but no longer LBA-referenced
/// is reclaimed by GC's full classification, not by this filter.
#[test]
fn snapshot_manifest_omits_fully_dead_segment() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();
    common::write_test_keypair(fork_dir);

    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();

    // Seg A: write a Zero entry at LBA 0, flush WAL → pending, drain → index/.
    vol.write_zeroes(0, 1).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);
    assert!(common::pending_ulids(fork_dir).is_empty());
    let after_a: Vec<Ulid> = index_ulids(fork_dir);
    assert_eq!(after_a.len(), 1);
    let seg_a = after_a[0];

    // Seg B: overwrite LBA 0 with non-zero. lbamap[0] flips to hash_BB,
    // so seg A's Zero entry is now LBA-dead.
    vol.write(0, &block(0xBB)).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);
    let after_b: Vec<Ulid> = index_ulids(fork_dir);
    assert_eq!(after_b.len(), 2);
    let seg_b = *after_b.iter().find(|u| **u != seg_a).expect("seg B");

    let snap_ulid = vol.sign_snapshot_manifest_at_max().unwrap();
    let listed = manifest_segments(fork_dir, snap_ulid);
    assert!(
        listed.contains(&seg_b),
        "manifest must list live segment B {seg_b}: got {listed:?}",
    );
    assert!(
        !listed.contains(&seg_a),
        "manifest must omit dead segment A {seg_a}: got {listed:?}",
    );

    // Filter is manifest-only: the file itself is still on disk.
    assert!(
        fork_dir.join("index").join(format!("{seg_a}.idx")).exists(),
        "filter must not delete the segment file — that's GC's job",
    );
}

/// A segment whose LBAs are still mapped (no overwrite, no GC) must remain
/// listed in the manifest. Guards against a too-aggressive predicate that
/// would incorrectly classify a fully-alive segment as dead.
#[test]
fn snapshot_manifest_keeps_fully_alive_segment() {
    let dir = tempfile::TempDir::new().unwrap();
    let fork_dir = dir.path();
    common::write_test_keypair(fork_dir);

    let mut vol = Volume::open(fork_dir, fork_dir).unwrap();
    vol.write(0, &block(0x11)).unwrap();
    vol.write(8, &block(0x22)).unwrap();
    vol.flush_wal().unwrap();
    common::drain_with_redact(&mut vol);
    let segs = index_ulids(fork_dir);
    assert_eq!(segs.len(), 1);

    let snap_ulid = vol.sign_snapshot_manifest_at_max().unwrap();
    let listed = manifest_segments(fork_dir, snap_ulid);
    assert_eq!(listed, segs, "alive segment must survive the filter");
}

fn index_ulids(fork_dir: &Path) -> Vec<Ulid> {
    let mut out: Vec<Ulid> = std::fs::read_dir(fork_dir.join("index"))
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            let stem = s.strip_suffix(".idx")?;
            Ulid::from_string(stem).ok()
        })
        .collect();
    out.sort();
    out
}

trait SignAtMax {
    fn sign_snapshot_manifest_at_max(&mut self) -> std::io::Result<Ulid>;
}

impl SignAtMax for Volume {
    /// Pick the largest ULID under `index/`, sign over it, and return it.
    /// Mirrors the production flow where the coordinator picks the snapshot
    /// ULID from the segment set the volume just confirmed.
    fn sign_snapshot_manifest_at_max(&mut self) -> std::io::Result<Ulid> {
        let mut all = index_ulids(self.base_dir());
        all.sort();
        let snap_ulid = *all.last().expect("test must have at least one segment");
        self.sign_snapshot_manifest(snap_ulid)?;
        Ok(snap_ulid)
    }
}
