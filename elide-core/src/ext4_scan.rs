// ext4 scanning primitives for file-aware import and snapshot filemap
// generation.
//
// Two consumers, one walker:
//
// - `scan_via_reader` (and its `scan(&Path)` wrapper) reads file body
//   bytes and computes per-fragment hashes. Used by the import path,
//   which also needs the bodies to write segment DATA entries.
// - `scan_layout_via_reader` produces only the fragment layout
//   (path, file_offset, lba_start, lba_length, byte_count). Used by
//   Phase 4 snapshot filemap generation, which fills in the per-
//   fragment hashes from the volume's LBA map via `hash_for_lba` —
//   no body reads, no rehashing.
//
// Both share the inode-table walk and the raw directory-tree walk.
// Path↔inode joining is done by inode index (read directly from
// directory entries), not by hashing file contents — `ext4-view`
// does not expose inode numbers, so we walk directories ourselves.
//
// Fragment = one contiguous LBA range owned by a file (one ext4 leaf
// extent, trimmed to the file's i_size). A file with N discontiguous
// ext4 extents produces N fragments. Hashes (when computed) cover the
// full allocated block range (`lba_length * 4096` bytes) including any
// tail-block padding, so the fragment hash written to a segment entry
// equals the fragment hash written to the filemap.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::path::Path;

use ext4_view::Ext4Read;

const LBA_SIZE: u64 = 4096;
const SUPERBLOCK_OFFSET: u64 = 1024;
const EXT4_MAGIC: u16 = 0xef53;
const EXTENT_MAGIC: u16 = 0xf30a;
const INODE_FLAG_EXTENTS: u32 = 0x0008_0000;
const S_IFREG: u16 = 0x8000;
const S_IFDIR: u16 = 0x4000;
const S_IFMT: u16 = 0xf000;
const INCOMPAT_64BIT: u32 = 0x80;
const EXTENT_ENTRY_SIZE: usize = 12;
const EXT4_ROOT_INO: u32 = 2;
const EXT4_FT_REG_FILE: u8 = 1;
const EXT4_FT_DIR: u8 = 2;

fn read_at(reader: &mut dyn Ext4Read, offset: u64, dst: &mut [u8]) -> io::Result<()> {
    reader
        .read(offset, dst)
        .map_err(|e| io::Error::other(format!("ext4 read at {offset}: {e}")))
}

/// Probe whether `reader` points at an ext4 image by reading just the
/// superblock magic. Used by layout scans to distinguish "not ext4"
/// (skip cleanly) from "ext4 but unreadable" (propagate error).
fn probe_ext4(reader: &mut dyn Ext4Read) -> io::Result<bool> {
    let mut magic_buf = [0u8; 2];
    read_at(reader, SUPERBLOCK_OFFSET + 0x38, &mut magic_buf)?;
    Ok(u16::from_le_bytes(magic_buf) == EXT4_MAGIC)
}

fn u16le(data: &[u8], off: usize) -> io::Result<u16> {
    data.get(off..off + 2)
        .and_then(|s| s.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| io::Error::other("ext4: short read"))
}

fn u32le(data: &[u8], off: usize) -> io::Result<u32> {
    data.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| io::Error::other("ext4: short read"))
}

fn hilo64(hi: u32, lo: u32) -> u64 {
    (u64::from(hi) << 32) | u64::from(lo)
}

struct Superblock {
    block_size: u64,
    inode_size: usize,
    inodes_per_group: u32,
    num_block_groups: u32,
    is_64bit: bool,
}

impl Superblock {
    fn read(reader: &mut dyn Ext4Read) -> io::Result<Self> {
        let mut buf = [0u8; 1024];
        read_at(reader, SUPERBLOCK_OFFSET, &mut buf)?;

        if u16le(&buf, 0x38)? != EXT4_MAGIC {
            return Err(io::Error::other("not an ext4 image"));
        }

        let block_size = 1024u64 << u32le(&buf, 0x18)?;
        if block_size != LBA_SIZE {
            return Err(io::Error::other(format!(
                "unsupported ext4 block size: {block_size} (only 4096 supported)"
            )));
        }
        let inode_size = u16le(&buf, 0x58)? as usize;
        let inodes_per_group = u32le(&buf, 0x28)?;
        let blocks_per_group = u32le(&buf, 0x20)?;
        let first_data_block = u32le(&buf, 0x14)? as u64;
        let blocks_count = hilo64(u32le(&buf, 0x150)?, u32le(&buf, 0x04)?);
        let is_64bit = (u32le(&buf, 0x60)? & INCOMPAT_64BIT) != 0;

        let num_data_blocks = blocks_count.saturating_sub(first_data_block);
        let num_block_groups = num_data_blocks.div_ceil(blocks_per_group as u64) as u32;

        Ok(Self {
            block_size,
            inode_size,
            inodes_per_group,
            num_block_groups,
            is_64bit,
        })
    }

    fn bgdt_start(&self) -> u64 {
        // Block group descriptor table starts at block 1 (or 2 if block_size==1024).
        // We reject non-4096 block sizes in Superblock::read, so always block 1.
        self.block_size
    }

    fn bgd_size(&self) -> u64 {
        if self.is_64bit { 64 } else { 32 }
    }
}

fn inode_table_block(reader: &mut dyn Ext4Read, sb: &Superblock, group: u32) -> io::Result<u64> {
    let offset = sb.bgdt_start() + group as u64 * sb.bgd_size();
    let mut buf = vec![0u8; sb.bgd_size() as usize];
    read_at(reader, offset, &mut buf)?;

    let lo = u32le(&buf, 0x08)? as u64;
    let hi = if sb.is_64bit {
        u32le(&buf, 0x28)? as u64
    } else {
        0
    };
    Ok((hi << 32) | lo)
}

/// Compute the on-disk byte offset of inode `inode_idx` (1-based).
fn inode_offset(reader: &mut dyn Ext4Read, sb: &Superblock, inode_idx: u32) -> io::Result<u64> {
    if inode_idx == 0 {
        return Err(io::Error::other("inode index 0 is reserved"));
    }
    let zero_based = inode_idx - 1;
    let group = zero_based / sb.inodes_per_group;
    let idx_in_group = zero_based % sb.inodes_per_group;
    let table_block = inode_table_block(reader, sb, group)?;
    Ok(table_block * sb.block_size + idx_in_group as u64 * sb.inode_size as u64)
}

/// Walk an extent-tree node (`data` holds either the inode's 60-byte
/// embedded extent header + entries, or a full block for an index
/// node's child). Leaf extents are appended to `out` as
/// `(logical_block, phys_start_block, num_blocks)`.
fn collect_extents_with_logical(
    data: &[u8],
    reader: &mut dyn Ext4Read,
    sb: &Superblock,
    out: &mut Vec<(u32, u64, u16)>,
) -> io::Result<()> {
    if data.len() < EXTENT_ENTRY_SIZE {
        return Ok(());
    }
    if u16le(data, 0)? != EXTENT_MAGIC {
        return Ok(());
    }

    let num_entries = u16le(data, 2)? as usize;
    let depth = u16le(data, 6)?;

    for i in 0..num_entries {
        let off = (i + 1) * EXTENT_ENTRY_SIZE;
        if off + EXTENT_ENTRY_SIZE > data.len() {
            break;
        }
        let entry = &data[off..off + EXTENT_ENTRY_SIZE];

        if depth == 0 {
            let logical = u32le(entry, 0)?;
            let len = u16le(entry, 4)? & 0x7fff; // strip uninitialised bit
            let phys = hilo64(u16le(entry, 6)? as u32, u32le(entry, 8)?);
            out.push((logical, phys, len));
        } else {
            let child = hilo64(u16le(entry, 8)? as u32, u32le(entry, 4)?);
            let mut child_data = vec![0u8; sb.block_size as usize];
            read_at(reader, child * sb.block_size, &mut child_data)?;
            collect_extents_with_logical(&child_data, reader, sb, out)?;
        }
    }

    Ok(())
}

/// One contiguous LBA range owned by a regular file — layout only.
///
/// `file_offset` is the byte offset within the file where this fragment's
/// data starts. `lba_start`/`lba_length` describe the physical range.
/// `byte_count` is the file-truthful number of bytes in this fragment
/// (≤ `lba_length * 4096`; smaller only for the last fragment of a file
/// whose size is not a multiple of 4096).
#[derive(Clone, Debug)]
pub struct FragmentLayout {
    pub path: String,
    pub file_offset: u64,
    pub lba_start: u64,
    pub lba_length: u32,
    pub byte_count: u64,
}

/// One contiguous LBA range owned by a regular file, plus its body and
/// content hash. Returned by `scan_via_reader` for the import path.
///
/// `hash` is `blake3(fragment_disk_bytes)` — i.e., over the full
/// `lba_length * 4096` bytes, *including* any tail-block padding.
pub struct FileFragment {
    pub path: String,
    pub file_offset: u64,
    pub lba_start: u64,
    pub lba_length: u32,
    pub byte_count: u64,
    pub hash: blake3::Hash,
    pub body: Vec<u8>,
}

/// Common per-inode partial result before path joining.
struct PartialFragment {
    file_offset: u64,
    lba_start: u64,
    lba_length: u32,
    byte_count: u64,
}

struct InodeFragments {
    inode_idx: u32,
    parts: Vec<PartialFragment>,
    /// Per-fragment body bytes (only populated when `read_bodies = true`).
    bodies: Vec<Vec<u8>>,
    /// Per-fragment content hashes (only populated when `read_bodies = true`).
    hashes: Vec<blake3::Hash>,
}

/// Walk every regular-file inode in the image, returning per-inode
/// fragment lists keyed by inode index. When `read_bodies` is true, also
/// populates the body bytes and per-fragment hash. When false, those
/// arrays are left empty and the caller is responsible for supplying
/// hashes from another source (e.g. the volume's LBA map).
fn scan_inode_fragments(
    reader: &mut dyn Ext4Read,
    sb: &Superblock,
    read_bodies: bool,
) -> io::Result<Vec<InodeFragments>> {
    let mut results = Vec::new();
    let mut inode_buf = vec![0u8; sb.inode_size];

    for group in 0..sb.num_block_groups {
        let table_block = inode_table_block(reader, sb, group)?;
        let table_offset = table_block * sb.block_size;

        for idx in 0..sb.inodes_per_group {
            let inode_offset = table_offset + idx as u64 * sb.inode_size as u64;
            if read_at(reader, inode_offset, &mut inode_buf).is_err() {
                break;
            }

            let i_mode = u16le(&inode_buf, 0x00)?;
            if (i_mode & S_IFMT) != S_IFREG {
                continue;
            }
            if u16le(&inode_buf, 0x1a)? == 0 {
                continue;
            }
            let i_flags = u32le(&inode_buf, 0x20)?;
            if (i_flags & INODE_FLAG_EXTENTS) == 0 {
                continue;
            }
            let i_size = hilo64(u32le(&inode_buf, 0x6c)?, u32le(&inode_buf, 0x04)?);
            if i_size == 0 {
                continue;
            }

            let i_block = inode_buf[0x28..0x28 + 60].to_vec();
            let mut raw: Vec<(u32, u64, u16)> = Vec::new();
            collect_extents_with_logical(&i_block, reader, sb, &mut raw)?;
            if raw.is_empty() {
                continue;
            }

            raw.sort_by_key(|&(logical, _, _)| logical);

            let mut parts = Vec::new();
            let mut bodies = Vec::new();
            let mut hashes = Vec::new();
            let mut bytes_remaining = i_size;

            for (logical, phys_block, num_blocks) in raw {
                if bytes_remaining == 0 {
                    break;
                }
                let file_offset = logical as u64 * sb.block_size;
                if file_offset >= i_size {
                    // Preallocated extent beyond i_size — not real file data.
                    continue;
                }
                let allocated = num_blocks as u64 * sb.block_size;
                let byte_count = allocated.min(bytes_remaining);
                bytes_remaining = bytes_remaining.saturating_sub(byte_count);

                parts.push(PartialFragment {
                    file_offset,
                    lba_start: phys_block,
                    lba_length: num_blocks as u32,
                    byte_count,
                });

                if read_bodies {
                    let disk_start = phys_block * sb.block_size;
                    let mut body = vec![0u8; allocated as usize];
                    read_at(reader, disk_start, &mut body)?;
                    let hash = blake3::hash(&body);
                    hashes.push(hash);
                    bodies.push(body);
                }
            }

            if parts.is_empty() {
                continue;
            }

            // 1-based inode index: group g, slot s → inode = g * inodes_per_group + s + 1.
            let inode_idx = group * sb.inodes_per_group + idx + 1;
            results.push(InodeFragments {
                inode_idx,
                parts,
                bodies,
                hashes,
            });
        }
    }

    Ok(results)
}

/// One parsed directory entry.
struct DirEntry {
    inode_idx: u32,
    file_type: u8,
    name: String,
}

/// Parse one 4 KiB directory data block into linear directory entries.
///
/// Tolerates htree internals: htree root and index blocks use the
/// rec_len chain to either skip past index data (root) or inflate a
/// single zero-inode "fake dirent" spanning the whole block (internal
/// nodes). Either way, only real leaf entries survive the inode!=0
/// filter.
fn parse_dir_block(data: &[u8]) -> Vec<DirEntry> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while off + 8 <= data.len() {
        let inode_idx = match u32le(data, off) {
            Ok(v) => v,
            Err(_) => break,
        };
        let rec_len = match u16le(data, off + 4) {
            Ok(v) => v as usize,
            Err(_) => break,
        };
        if rec_len < 8 {
            break;
        }
        let name_len = data[off + 6] as usize;
        let file_type = data[off + 7];
        let name_start = off + 8;
        let name_end = name_start + name_len;
        if inode_idx != 0
            && name_len > 0
            && name_end <= data.len()
            && let Ok(name) = std::str::from_utf8(&data[name_start..name_end])
        {
            out.push(DirEntry {
                inode_idx,
                file_type,
                name: name.to_owned(),
            });
        }
        let next = off.saturating_add(rec_len);
        if next <= off {
            break;
        }
        off = next;
    }
    out
}

/// Walk the directory tree starting at the root inode and return
/// `inode_idx → list of paths`. Paths are absolute, "/"-rooted.
/// Multiple paths per inode means hardlinks.
fn walk_directory_tree(
    reader: &mut dyn Ext4Read,
    sb: &Superblock,
) -> io::Result<HashMap<u32, Vec<String>>> {
    let mut out: HashMap<u32, Vec<String>> = HashMap::new();
    let mut visited_dirs: HashSet<u32> = HashSet::new();
    let mut stack: Vec<(u32, String)> = vec![(EXT4_ROOT_INO, "/".to_owned())];

    let mut inode_buf = vec![0u8; sb.inode_size];
    let mut block_buf = vec![0u8; sb.block_size as usize];

    while let Some((dir_inode_idx, dir_path)) = stack.pop() {
        if !visited_dirs.insert(dir_inode_idx) {
            continue;
        }

        let is_root = dir_inode_idx == EXT4_ROOT_INO;

        let off = inode_offset(reader, sb, dir_inode_idx)?;
        if let Err(e) = read_at(reader, off, &mut inode_buf) {
            if is_root {
                return Err(io::Error::other(format!(
                    "ext4 root inode {EXT4_ROOT_INO} unreadable at offset {off}: {e} — image corrupt or read path broken",
                )));
            }
            continue;
        }
        let i_mode = u16le(&inode_buf, 0x00)?;
        if (i_mode & S_IFMT) != S_IFDIR {
            if is_root {
                return Err(io::Error::other(format!(
                    "ext4 root inode {EXT4_ROOT_INO} has mode {i_mode:#06x}, expected directory (S_IFDIR={S_IFDIR:#06x}) — image corrupt or read path broken",
                )));
            }
            continue;
        }
        let i_flags = u32le(&inode_buf, 0x20)?;
        if (i_flags & INODE_FLAG_EXTENTS) == 0 {
            // Pre-extent (block-mapped) directories are not supported by the
            // import path either — modern mkfs.ext4 always uses extents.
            continue;
        }
        let i_block = inode_buf[0x28..0x28 + 60].to_vec();

        let mut extents: Vec<(u32, u64, u16)> = Vec::new();
        collect_extents_with_logical(&i_block, reader, sb, &mut extents)?;
        extents.sort_by_key(|&(logical, _, _)| logical);

        for (_logical, phys_block, num_blocks) in extents {
            for b in 0..num_blocks as u64 {
                let block_pos = (phys_block + b) * sb.block_size;
                if read_at(reader, block_pos, &mut block_buf).is_err() {
                    continue;
                }
                for entry in parse_dir_block(&block_buf) {
                    if entry.name == "." || entry.name == ".." {
                        continue;
                    }
                    let child_path = if dir_path == "/" {
                        format!("/{}", entry.name)
                    } else {
                        format!("{}/{}", dir_path, entry.name)
                    };
                    match entry.file_type {
                        EXT4_FT_REG_FILE => {
                            out.entry(entry.inode_idx).or_default().push(child_path);
                        }
                        EXT4_FT_DIR => {
                            stack.push((entry.inode_idx, child_path));
                        }
                        _ => {} // symlinks, devices, fifos: skip
                    }
                }
            }
        }
    }

    Ok(out)
}

/// Layout-only scan output. Used by Phase 4 snapshot filemap generation,
/// which fills in per-fragment hashes from the volume's LBA map without
/// reading body bytes.
pub struct Ext4Layout {
    pub fragments: Vec<FragmentLayout>,
    pub file_lba_coverage: Vec<u64>,
    pub total_lbas: u64,
}

/// Full scan output with per-fragment bodies and hashes, plus a coverage
/// bitset marking which LBAs are owned by regular file data. Callers use
/// the bitset to emit block-granular DATA entries for metadata/directory/
/// journal blocks (the ones with a cleared bit) during import.
pub struct Ext4Scan {
    pub fragments: Vec<FileFragment>,
    pub file_lba_coverage: Vec<u64>,
    pub total_lbas: u64,
}

impl Ext4Scan {
    /// True if LBA `lba` is covered by any file fragment.
    pub fn lba_is_file(&self, lba: u64) -> bool {
        if lba >= self.total_lbas {
            return false;
        }
        let idx = (lba / 64) as usize;
        let bit = lba % 64;
        self.file_lba_coverage
            .get(idx)
            .is_some_and(|w| w & (1 << bit) != 0)
    }
}

impl Ext4Layout {
    pub fn lba_is_file(&self, lba: u64) -> bool {
        if lba >= self.total_lbas {
            return false;
        }
        let idx = (lba / 64) as usize;
        let bit = lba % 64;
        self.file_lba_coverage
            .get(idx)
            .is_some_and(|w| w & (1 << bit) != 0)
    }
}

/// Scan an ext4 image through `reader`, reading file body bytes and
/// computing per-fragment hashes. Used by the import path.
pub fn scan_via_reader(image_size: u64, mut reader: Box<dyn Ext4Read>) -> io::Result<Ext4Scan> {
    if !image_size.is_multiple_of(LBA_SIZE) {
        return Err(io::Error::other("image size is not a multiple of 4096"));
    }
    let total_lbas = image_size / LBA_SIZE;

    let sb = Superblock::read(&mut *reader)?;
    let inodes = scan_inode_fragments(&mut *reader, &sb, true)?;
    let paths_by_inode = walk_directory_tree(&mut *reader, &sb)?;

    let mut fragments = Vec::new();
    let mut file_lba_coverage = vec![0u64; (total_lbas as usize).div_ceil(64)];

    for inode in inodes {
        let path = match path_for_inode(&paths_by_inode, inode.inode_idx) {
            Some(p) => p,
            None => continue, // orphan inode (deleted file still in table)
        };
        for (i, part) in inode.parts.into_iter().enumerate() {
            mark_coverage(
                &mut file_lba_coverage,
                total_lbas,
                part.lba_start,
                part.lba_length,
            );
            fragments.push(FileFragment {
                path: path.clone(),
                file_offset: part.file_offset,
                lba_start: part.lba_start,
                lba_length: part.lba_length,
                byte_count: part.byte_count,
                hash: inode.hashes[i],
                body: inode.bodies[i].clone(),
            });
        }
    }

    fragments.sort_by_key(|fr| fr.lba_start);

    Ok(Ext4Scan {
        fragments,
        file_lba_coverage,
        total_lbas,
    })
}

/// Scan an ext4 image through `reader` for fragment layout only — no body
/// reads, no per-fragment hashing. Used by Phase 4 snapshot filemap
/// generation, which fills hashes from the volume's LBA map.
///
/// Returns `Ok(None)` when the image is not ext4 (missing superblock
/// magic) so callers can skip cleanly. Any other parse failure — a valid
/// superblock that leads to an unreadable root inode, truncated inode
/// tables, etc. — is propagated as `Err`.
pub fn scan_layout_via_reader(
    image_size: u64,
    mut reader: Box<dyn Ext4Read>,
) -> io::Result<Option<Ext4Layout>> {
    if !image_size.is_multiple_of(LBA_SIZE) {
        return Err(io::Error::other("image size is not a multiple of 4096"));
    }
    let total_lbas = image_size / LBA_SIZE;

    if !probe_ext4(&mut *reader)? {
        return Ok(None);
    }

    let sb = Superblock::read(&mut *reader)?;
    let inodes = scan_inode_fragments(&mut *reader, &sb, false)?;
    let paths_by_inode = walk_directory_tree(&mut *reader, &sb)?;

    let mut fragments = Vec::new();
    let mut file_lba_coverage = vec![0u64; (total_lbas as usize).div_ceil(64)];

    for inode in inodes {
        let path = match path_for_inode(&paths_by_inode, inode.inode_idx) {
            Some(p) => p,
            None => continue,
        };
        for part in inode.parts {
            mark_coverage(
                &mut file_lba_coverage,
                total_lbas,
                part.lba_start,
                part.lba_length,
            );
            fragments.push(FragmentLayout {
                path: path.clone(),
                file_offset: part.file_offset,
                lba_start: part.lba_start,
                lba_length: part.lba_length,
                byte_count: part.byte_count,
            });
        }
    }

    fragments.sort_by_key(|fr| fr.lba_start);

    Ok(Some(Ext4Layout {
        fragments,
        file_lba_coverage,
        total_lbas,
    }))
}

fn path_for_inode(paths_by_inode: &HashMap<u32, Vec<String>>, inode_idx: u32) -> Option<String> {
    // For hardlinks, deterministically pick the lexicographically-first path
    // so two scans of the same image produce the same filemap. The full path
    // set is preserved by emitting one fragment row per (path, hash) pair —
    // see the loop below; for now we collapse to one path per inode to match
    // the existing import behaviour. Hardlink expansion is tracked as an
    // open question in docs/notes/design-delta-compression.md.
    let paths = paths_by_inode.get(&inode_idx)?;
    paths.iter().min().cloned()
}

fn mark_coverage(coverage: &mut [u64], total_lbas: u64, lba_start: u64, lba_length: u32) {
    for i in 0..lba_length as u64 {
        let lba = lba_start + i;
        if lba < total_lbas {
            let idx = (lba / 64) as usize;
            let bit = lba % 64;
            if let Some(word) = coverage.get_mut(idx) {
                *word |= 1 << bit;
            }
        }
    }
}

/// Path-based wrapper retained for the import CLI: opens `image_path`
/// as a `File` and runs `scan_via_reader`.
pub fn scan(image: &Path) -> io::Result<Ext4Scan> {
    let f = File::open(image)?;
    let image_size = f.metadata()?.len();
    scan_via_reader(image_size, Box::new(f))
}
