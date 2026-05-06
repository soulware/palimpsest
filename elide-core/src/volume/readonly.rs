//! Read-only volume view: rebuilds LBA map and extent index without
//! claiming the WAL lock or replaying any in-flight writes. Used by the
//! `--readonly` NBD serve path.
//!
//! Ancestor-chain helpers used by both readonly and writable opens live
//! in `volume/ancestry.rs`; the shared open-time rebuild lives in
//! `volume/open_state.rs`.

use std::cell::RefCell;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    extent_index: Arc<extentindex::ExtentIndex>,
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
        let (ancestor_layers, lbamap, extent_index) =
            super::open_state::open_read_state(fork_dir, by_id_dir)?;
        Ok(Self {
            base_dir: fork_dir.to_owned(),
            ancestor_layers,
            lbamap,
            extent_index: Arc::new(extent_index),
            file_cache: RefCell::new(FileCache::default()),
            fetcher: None,
        })
    }

    /// Shared handle on the in-memory extent index. The body-prefetch
    /// pool uses this so its `.present` writes mirror back into the
    /// same `Arc<SegmentPresence>` reachable from the read path.
    pub fn extent_index_arc(&self) -> Arc<extentindex::ExtentIndex> {
        Arc::clone(&self.extent_index)
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
            &self.extent_index,
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

#[cfg(test)]
mod tests {
    use super::super::Volume;
    use super::super::fork::fork_volume;
    use super::super::test_util::*;
    use super::*;
    use std::fs;
    use std::sync::Arc;

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
                _owner_vol_id: Ulid,
                index_dir: &Path,
                body_dir: &Path,
                _extent: &crate::segment::ExtentFetch,
                _presence: Option<Arc<crate::extentindex::SegmentPresence>>,
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
                _owner_vol_id: Ulid,
                _index_dir: &Path,
                _body_dir: &Path,
            ) -> io::Result<()> {
                Err(io::Error::other("unused"))
            }
        }

        let tmp = temp_dir();
        // Owner-volume threading requires fork directory names to be
        // valid ULIDs (the volume process always lays them out as
        // `by_id/<ulid>/`); use distinct ULIDs for child and ancestor.
        let child_dir = tmp.join(Ulid::new().to_string());
        let ancestor_dir = tmp.join(Ulid::new().to_string());
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

        let mut empty_index = crate::extentindex::ExtentIndex::new();
        // The test segment has at least one entry; install an
        // all-zero presence so the read path falls through to the
        // fetcher (the file doesn't exist yet anyway).
        empty_index.set_segment_presence(
            seg_ulid,
            Arc::new(crate::extentindex::SegmentPresence::zeroed(1)),
        );
        let returned = find_segment_in_dirs(
            seg_ulid,
            &child_dir,
            &layers,
            Some(&fetcher),
            &empty_index,
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
                _owner_vol_id: Ulid,
                index_dir: &Path,
                body_dir: &Path,
                _extent: &crate::segment::ExtentFetch,
                _presence: Option<Arc<crate::extentindex::SegmentPresence>>,
            ) -> io::Result<()> {
                *self.captured.lock().unwrap() = Some(index_dir.to_owned());
                std::fs::create_dir_all(body_dir)?;
                std::fs::write(body_dir.join(format!("{segment_id}.body")), b"")?;
                Ok(())
            }
            fn fetch_delta_body(&self, _: Ulid, _: Ulid, _: &Path, _: &Path) -> io::Result<()> {
                Err(io::Error::other("unused"))
            }
        }

        let tmp = temp_dir();
        // ULID-named dirs because owner-volume threading parses the
        // fork dir name as the owning volume's ULID.
        let child_dir = tmp.join(Ulid::new().to_string());
        let ancestor_dir = tmp.join(Ulid::new().to_string());
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

        let mut empty_index = crate::extentindex::ExtentIndex::new();
        empty_index.set_segment_presence(
            seg_ulid,
            Arc::new(crate::extentindex::SegmentPresence::zeroed(1)),
        );
        let returned = find_segment_in_dirs(
            seg_ulid,
            &child_dir,
            &layers,
            Some(&fetcher),
            &empty_index,
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
