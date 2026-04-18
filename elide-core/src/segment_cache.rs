//! Small LRU cache over `segment::read_and_verify_segment_index`.
//!
//! Parsed segment indices and their Ed25519 signature verification are
//! expensive enough to matter across the worker offload paths
//! (`apply_gc_handoffs`, `promote_segment`, `delta_repack_post_snapshot`,
//! sweep/repack). The same `(path, file_len)` is often read multiple
//! times within a single maintenance pass — and an `.idx` file is
//! content-immutable once written, so a cached entry remains valid until
//! the file is renamed away or rewritten at a different length.
//!
//! The cache is keyed by `(PathBuf, file_len)`. A tmp+rename rewrite of
//! a pending segment always changes the entry count (sweep removes dead
//! entries; repack merges), which changes the index section length and
//! therefore the file length — so a stale entry will naturally miss on
//! the next read. Redact preserves file length (it only punches body
//! holes) and also preserves the index section, so a cache hit there
//! returns the same parsed index either way.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ed25519_dalek::VerifyingKey;
use lru::LruCache;
use ulid::Ulid;

use crate::segment::{self, SegmentEntry};

/// Parsed index payload held in the cache.
#[derive(Debug)]
pub struct ParsedIndex {
    pub body_section_start: u64,
    pub entries: Vec<SegmentEntry>,
    pub inputs: Vec<Ulid>,
    /// Hash of the verifying key used to parse+verify this entry. Keeps
    /// the cache correct across forks that share a base_dir but have
    /// different keys (e.g. a stale entry from a prior fork would fail
    /// verification against the new key on re-parse anyway, but this
    /// short-circuits the match and keeps the cache honest).
    vk_hash: u64,
}

fn vk_hash(vk: &VerifyingKey) -> u64 {
    let mut h = DefaultHasher::new();
    vk.as_bytes().hash(&mut h);
    h.finish()
}

/// Shared, thread-safe LRU. A single instance is shared between the
/// actor thread and the worker thread via `Arc`.
pub struct SegmentIndexCache {
    inner: Mutex<LruCache<(PathBuf, u64), Arc<ParsedIndex>>>,
}

impl SegmentIndexCache {
    /// Build a cache bounded to `capacity` entries.
    ///
    /// `capacity` must be non-zero. Zero is a programming error that
    /// would make the cache permanently empty — callers that want "no
    /// caching" should not construct one.
    pub fn new(capacity: usize) -> Self {
        let cap =
            NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).expect("1 is non-zero"));
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Drop-in replacement for `segment::read_and_verify_segment_index`.
    ///
    /// On a hit, returns the cached `Arc` directly. On a miss, parses,
    /// verifies, inserts, and returns. The caller gets an `Arc` to keep
    /// the cache slot alive for the lifetime of the borrow — if the
    /// caller needs to mutate the entries, it should clone.
    pub fn read_and_verify(&self, path: &Path, vk: &VerifyingKey) -> io::Result<Arc<ParsedIndex>> {
        let file_len = std::fs::metadata(path)?.len();
        let key = (path.to_path_buf(), file_len);
        let want_vk = vk_hash(vk);

        if let Some(hit) = self
            .inner
            .lock()
            .expect("segment index cache poisoned")
            .get(&key)
            .cloned()
            && hit.vk_hash == want_vk
        {
            return Ok(hit);
        }
        // Verifying key mismatch or miss: fall through to re-parse.
        // On vk mismatch the new entry replaces the old one under the
        // same (path, len) key below.

        let (body_section_start, entries, inputs) =
            segment::read_and_verify_segment_index(path, vk)?;
        let parsed = Arc::new(ParsedIndex {
            body_section_start,
            entries,
            inputs,
            vk_hash: want_vk,
        });
        self.inner
            .lock()
            .expect("segment index cache poisoned")
            .put(key, Arc::clone(&parsed));
        Ok(parsed)
    }
}

impl std::fmt::Debug for SegmentIndexCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.inner.lock().expect("segment index cache poisoned");
        f.debug_struct("SegmentIndexCache")
            .field("len", &guard.len())
            .field("cap", &guard.cap())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{EntryKind, SegmentEntry, write_segment};

    fn signer_pair() -> (Arc<dyn crate::segment::SegmentSigner>, VerifyingKey) {
        crate::signing::generate_ephemeral_signer()
    }

    fn dummy_entry(lba: u64) -> SegmentEntry {
        SegmentEntry {
            hash: blake3::hash(&lba.to_le_bytes()),
            start_lba: lba,
            lba_length: 1,
            compressed: false,
            stored_offset: 0,
            stored_length: 8,
            kind: EntryKind::Data,
            data: Some(vec![0xAB; 8]),
            delta_options: Vec::new(),
            canonical_only: false,
        }
    }

    #[test]
    fn hit_after_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("seg");
        let (signer, vk) = signer_pair();
        let mut entries = vec![dummy_entry(0)];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let cache = SegmentIndexCache::new(16);
        let a = cache.read_and_verify(&path, &vk).unwrap();
        let b = cache.read_and_verify(&path, &vk).unwrap();
        // Hit path returns the same Arc.
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn miss_on_file_length_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("seg");
        let (signer, vk) = signer_pair();
        let mut entries = vec![dummy_entry(0)];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let cache = SegmentIndexCache::new(16);
        let a = cache.read_and_verify(&path, &vk).unwrap();

        // Rewrite with more entries — index section + body grow, so
        // file length changes and the cache sees a new key.
        std::fs::remove_file(&path).unwrap();
        let mut entries2 = vec![dummy_entry(0), dummy_entry(1)];
        write_segment(&path, &mut entries2, signer.as_ref()).unwrap();

        let b = cache.read_and_verify(&path, &vk).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(b.entries.len(), 2);
    }

    #[test]
    fn miss_on_verifying_key_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("seg");
        let (signer, vk) = signer_pair();
        let mut entries = vec![dummy_entry(0)];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let cache = SegmentIndexCache::new(16);
        let a = cache.read_and_verify(&path, &vk).unwrap();

        // Rewrite with a different key — cached entry's vk_hash no
        // longer matches, so we re-parse.  Keep the same file length
        // so the (path, len) key still hits, forcing the vk_hash
        // discriminator to do its job.
        std::fs::remove_file(&path).unwrap();
        let (signer2, vk2) = signer_pair();
        let mut entries2 = vec![dummy_entry(0)];
        write_segment(&path, &mut entries2, signer2.as_ref()).unwrap();

        let b = cache.read_and_verify(&path, &vk2).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
