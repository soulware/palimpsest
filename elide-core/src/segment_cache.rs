//! Small LRU cache over `segment::read_and_verify_segment_index`.
//!
//! Parsed segment indices and their Ed25519 signature verification are
//! expensive enough to matter across the worker offload paths
//! (`apply_gc_handoffs`, `promote_segment`, `delta_repack_post_snapshot`,
//! sweep/repack). The same path is often read multiple times across
//! adjacent maintenance passes; the cache amortises signature
//! verification across those reads.
//!
//! The cache is keyed by `(PathBuf, signature_bytes, vk_hash)`. The
//! 64-byte Ed25519 signature in the segment header is the
//! correctness-load-bearing component: by Ed25519's collision
//! resistance, two files signed by the same verifying key with the
//! same signature have the same signed content. Path is included only
//! as a cache-locality hint — strictly redundant for correctness, but
//! cheap and useful for the LRU's hashing.
//!
//! On every lookup we read the 64-byte signature at fixed header
//! offset 36 (`pread`, no parse). On a hit, we return the cached
//! `Arc<ParsedIndex>` without re-verifying — the signature match
//! proves the content is what we already verified. On a miss, we fall
//! through to `read_and_verify_segment_index` and insert under the new
//! signature.
//!
//! ### Why not file length / inode
//!
//! Earlier versions of this cache keyed on `(path, file_len)` and
//! relied on "any rewrite changes the index entry count and therefore
//! the file length." That assumption broke for `sweep_pending`: when
//! sweep merges N inputs into an output named with the max input
//! ULID, the rewritten file at that path can have the same entry
//! count and same compressed body shape as the pre-sweep file,
//! yielding identical length and a stale cache hit. The stale
//! `entries` then propagated through `execute_promote_segment` into
//! `apply_promote_segment_result`, corrupting the extent index.
//!
//! `(path, ino, file_len)` would catch that specific case, but inode
//! reuse on Linux/Unix filesystems is permitted immediately after
//! unlink — a tmp+rename cycle can land the same inode number back at
//! the same path. The cache result is on the correctness path (it
//! drives extent-index updates), so an extraordinarily-rare
//! coincidence is still a correctness bug.
//!
//! Signature-keying makes the bug class structurally impossible: the
//! cache only returns a result the caller has already vouched for via
//! a signature it produced.

use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ed25519_dalek::VerifyingKey;
use lru::LruCache;
use ulid::Ulid;

use crate::segment::{self, SegmentEntry};

/// Byte offset of the 64-byte Ed25519 signature within the segment
/// header. The header layout is `[signed_prefix(36) | signature(64)]`,
/// total `HEADER_LEN = 100`.
const SIGNATURE_OFFSET: u64 = 36;
const SIGNATURE_LEN: usize = 64;

/// Parsed index payload held in the cache.
#[derive(Debug)]
pub struct ParsedIndex {
    pub body_section_start: u64,
    pub entries: Vec<SegmentEntry>,
    pub inputs: Vec<Ulid>,
}

fn vk_hash(vk: &VerifyingKey) -> u64 {
    let mut h = DefaultHasher::new();
    vk.as_bytes().hash(&mut h);
    h.finish()
}

/// Shared, thread-safe LRU. A single instance is shared between the
/// actor thread and the worker thread via `Arc`.
pub struct SegmentIndexCache {
    inner: Mutex<LruCache<CacheKey, Arc<ParsedIndex>>>,
}

/// `(path, signature_bytes, vk_hash)`. The signature is the
/// content discriminator — see the module-level doc.
type CacheKey = (PathBuf, [u8; SIGNATURE_LEN], u64);

/// Read the 64-byte Ed25519 signature from the segment header at fixed
/// offset 36. Used as the content discriminator for the cache key.
fn read_signature(path: &Path) -> io::Result<[u8; SIGNATURE_LEN]> {
    let f = fs::File::open(path)?;
    let mut buf = [0u8; SIGNATURE_LEN];
    f.read_exact_at(&mut buf, SIGNATURE_OFFSET)?;
    Ok(buf)
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
    ///
    /// The cache key includes the file's signature bytes, so a hit
    /// proves the on-disk content is what we already verified — even
    /// across path-reuse + inode-reuse coincidences that a path/ino/len
    /// key would mishandle.
    pub fn read_and_verify(&self, path: &Path, vk: &VerifyingKey) -> io::Result<Arc<ParsedIndex>> {
        let signature = read_signature(path)?;
        let want_vk = vk_hash(vk);
        let key: CacheKey = (path.to_path_buf(), signature, want_vk);

        if let Some(hit) = self
            .inner
            .lock()
            .expect("segment index cache poisoned")
            .get(&key)
            .cloned()
        {
            return Ok(hit);
        }

        let (body_section_start, entries, inputs) =
            segment::read_and_verify_segment_index(path, vk)?;
        let parsed = Arc::new(ParsedIndex {
            body_section_start,
            entries,
            inputs,
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
    fn miss_on_content_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("seg");
        let (signer, vk) = signer_pair();
        let mut entries = vec![dummy_entry(0)];
        write_segment(&path, &mut entries, signer.as_ref()).unwrap();

        let cache = SegmentIndexCache::new(16);
        let a = cache.read_and_verify(&path, &vk).unwrap();

        // Rewrite with different entries → different signature → miss.
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

        // Different vk → different key → miss. Independent re-sign also
        // produces a different signature, but the vk_hash component
        // alone is sufficient.
        std::fs::remove_file(&path).unwrap();
        let (signer2, vk2) = signer_pair();
        let mut entries2 = vec![dummy_entry(0)];
        write_segment(&path, &mut entries2, signer2.as_ref()).unwrap();

        let b = cache.read_and_verify(&path, &vk2).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
    }

    /// Regression for the sweep_pending corruption: when a tmp+rename
    /// rewrite produces a new file at the same path with the same
    /// length, a `(path, file_len)`-keyed cache returned stale entries
    /// that propagated wrong offsets through `apply_promote_segment_result`.
    /// Signature-keying makes this structurally impossible — different
    /// content always produces a different Ed25519 signature.
    #[test]
    fn miss_on_same_length_different_content() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("seg");
        let (signer, vk) = signer_pair();

        let mut entries_a = vec![dummy_entry(0), dummy_entry(1)];
        write_segment(&path, &mut entries_a, signer.as_ref()).unwrap();
        let len_a = std::fs::metadata(&path).unwrap().len();

        let cache = SegmentIndexCache::new(16);
        let a = cache.read_and_verify(&path, &vk).unwrap();

        std::fs::remove_file(&path).unwrap();
        // Different LBAs → different hashes → different signature, but
        // identical entry count and body shape → identical file length.
        let mut entries_b = vec![dummy_entry(2), dummy_entry(3)];
        write_segment(&path, &mut entries_b, signer.as_ref()).unwrap();
        let len_b = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            len_a, len_b,
            "test setup: rewrite must land at the same file length to exercise the bug"
        );

        let b = cache.read_and_verify(&path, &vk).unwrap();
        assert!(
            !Arc::ptr_eq(&a, &b),
            "cache must miss on same-length, different-content rewrite"
        );
        // Confirm we got the new entries, not stale ones from `a`.
        assert_eq!(b.entries[0].start_lba, 2);
        assert_eq!(b.entries[1].start_lba, 3);
    }
}
