// Filesystem-only drain for actor-based tests.
//
// Actor tests (`actor_proptest`, `concurrent_test`) move the `Volume` into a
// background actor thread and only hold a `VolumeHandle`.  They cannot call
// `drain_with_materialise(&mut vol)` because the `Volume` is not directly
// accessible.
//
// `drain_local` performs a raw filesystem promotion: it splits each pending
// segment file into `index/<ulid>.idx` + `cache/<ulid>.{body,present}` and
// deletes the pending file.  This **does not** go through
// `materialise_segment`/`promote_segment`, so:
//
//   - The volume's in-memory extent index and LBA map are NOT updated.
//   - DedupRef entries are NOT materialised to MaterializedRef.
//
// This is acceptable for actor-layer tests because:
//
//   1. The actor's snapshot is stale after `drain_local`, but the next actor
//      operation (write, flush, gc_checkpoint, crash+reopen) will either
//      update the snapshot or rebuild from disk.
//
//   2. The GC coordinator simulation (`simulate_coord_gc_local`) rebuilds its
//      own LBA map and extent index from disk, independent of the volume's
//      in-memory state, so it sees the drained segments correctly.
//
//   3. Using `drain_with_materialise` via the actor handle would update the
//      volume's extent index, but that creates a mismatch: the GC coordinator
//      sees the materialised `.idx` on disk (where S2 is canonical for a hash)
//      while the volume's extent index still points to S1 (the original DATA
//      entry).  `apply_gc_handoffs`' `still_at_old` check then fails to update
//      the extent index, leaving a stale pointer after the old segments are
//      deleted.
//
// **Rule**: use `drain_local` only in tests where `Volume` is behind an actor
// handle.  All other tests should use `drain_with_materialise(&mut vol)`.

use std::fs;
use std::path::Path;

/// Promote all pending segments to index/ + cache/ via raw file splitting.
///
/// See module-level docs for when this is appropriate vs `drain_with_materialise`.
pub fn drain_local(fork_dir: &Path) {
    const HEADER_LEN: usize = 96;
    let pending = fork_dir.join("pending");
    let index_dir = fork_dir.join("index");
    let cache_dir = fork_dir.join("cache");
    let _ = fs::create_dir_all(&index_dir);
    let _ = fs::create_dir_all(&cache_dir);
    let Ok(entries) = fs::read_dir(&pending) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let Some(ulid_str) = name.to_str() else {
            continue;
        };
        if ulid_str.ends_with(".tmp") || ulid_str.contains('.') {
            continue;
        }
        let Ok(data) = fs::read(&path) else {
            continue;
        };
        if data.len() < HEADER_LEN {
            continue;
        }
        let entry_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let index_length = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let inline_length = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
        let bss = HEADER_LEN + index_length as usize + inline_length as usize;
        if data.len() < bss {
            continue;
        }
        let _ = fs::write(index_dir.join(format!("{ulid_str}.idx")), &data[..bss]);
        let _ = fs::write(cache_dir.join(format!("{ulid_str}.body")), &data[bss..]);
        let bitset_len = (entry_count as usize).div_ceil(8);
        let _ = fs::write(
            cache_dir.join(format!("{ulid_str}.present")),
            vec![0xFFu8; bitset_len],
        );
        let _ = fs::remove_file(&path);
    }
}
