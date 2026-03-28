// Inspect a volume directory and print a human-readable summary.
//
// In the Named Forks layout, `dir` is the volume directory containing named
// fork subdirectories (default/, dev/, etc.). Each fork has its own wal/,
// pending/, segments/, and snapshots/.
//
// Does not modify any files.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use elide_core::{segment, writelog};

use crate::ls;

pub fn run(dir: &Path) -> io::Result<()> {
    let vol_name = dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("<unknown>");
    let size_bytes = read_size(dir)?;
    let is_readonly = dir.join("readonly").exists();

    println!("Volume: {vol_name}");
    match size_bytes {
        Some(b) => println!("Size:   {} ({} bytes)", fmt_size(b), fmt_commas(b)),
        None => println!("Size:   (no size file found)"),
    }
    if is_readonly {
        println!("Type:   readonly");
    }
    println!();

    // Collect all forks (subdirectories that look like fork directories).
    let mut fork_dirs: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| !matches!(n, "readonly" | "size"))
                    .unwrap_or(false)
        })
        .collect();
    fork_dirs.sort();

    if fork_dirs.is_empty() {
        // Legacy: treat dir itself as a single fork node.
        let node = collect_node(dir, true, true)?;
        print_node(&node, "", "  ");
        let t = totals(&node);
        print_totals(&t);
    } else {
        let mut grand_total = Totals::default();
        let n = fork_dirs.len();
        for (i, fork_dir) in fork_dirs.iter().enumerate() {
            let fork_name = fork_dir.file_name().and_then(|s| s.to_str()).unwrap_or("?");
            let last = i + 1 == n;
            let (connector, child_prefix) = if last {
                ("└── ", "    ")
            } else {
                ("├── ", "│   ")
            };
            let node = collect_node(fork_dir, false, false)?;
            let snap_count = count_snapshots(fork_dir);
            let state = if node.is_live { "live" } else { "base" };
            let origin = read_origin(fork_dir);
            print!("{connector}{fork_name}  [{state}]");
            if let Some(ref o) = origin {
                print!("  origin: {o}");
            }
            if snap_count > 0 {
                print!("  {snap_count} snapshot(s)");
            }
            println!();
            print_wal_section(&node.wal_files, child_prefix, node.is_live);
            print_seg_section("pending", &node.pending, child_prefix, node.is_live);
            print_seg_section("segments", &node.segments, child_prefix, true);
            if let Some(summary) = ls::try_fs_summary(fork_dir) {
                print_fs_summary(&summary, child_prefix);
            }
            accumulate(&node, &mut grand_total);
        }
        println!();
        print_totals(&grand_total);
    }

    Ok(())
}

// --- node collection ---

struct NodeInfo {
    is_root: bool,
    ulid: Option<String>,
    is_live: bool,
    wal_files: Vec<WalInfo>,
    pending: Vec<SegInfo>,
    segments: Vec<SegInfo>,
    children: Vec<NodeInfo>,
}

struct WalInfo {
    ulid: String,
    file_size: u64,
    record_count: usize,
    data_count: usize,
    ref_count: usize,
    lba_blocks: u64,
    has_partial_tail: bool,
    error: Option<String>,
}

struct SegInfo {
    ulid: String,
    file_size: u64,
    entry_count: usize,
    body_bytes: u64,
    lba_blocks: u64,
    dedup_ref_count: usize,
    error: Option<String>,
}

fn collect_node(dir: &Path, is_root: bool, with_children: bool) -> io::Result<NodeInfo> {
    let ulid = if is_root {
        None
    } else {
        dir.file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_owned())
    };

    let is_live = dir.join("wal").is_dir();

    let wal_files = collect_wal_dir(&dir.join("wal"))?;
    let pending = collect_seg_dir(&dir.join("pending"))?;
    let segments = collect_seg_dir(&dir.join("segments"))?;

    // Children only used in legacy single-node mode; Named Forks lists forks separately.
    let mut children = Vec::new();
    if with_children {
        let children_dir = dir.join("children");
        match fs::read_dir(&children_dir) {
            Ok(entries) => {
                let mut child_dirs: Vec<PathBuf> = entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| {
                        p.is_dir()
                            && p.file_name()
                                .and_then(|s| s.to_str())
                                .map(|s| ulid::Ulid::from_string(s).is_ok())
                                .unwrap_or(false)
                    })
                    .collect();
                child_dirs.sort();
                for child_dir in child_dirs {
                    children.push(collect_node(&child_dir, false, true)?);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                eprintln!("warning: cannot read {}: {e}", children_dir.display());
            }
        }
    }

    Ok(NodeInfo {
        is_root,
        ulid,
        is_live,
        wal_files,
        pending,
        segments,
        children,
    })
}

fn count_snapshots(fork_dir: &Path) -> usize {
    let snapshots_dir = fork_dir.join("snapshots");
    fs::read_dir(&snapshots_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .and_then(|n| ulid::Ulid::from_string(n).ok())
                        .is_some()
                })
                .count()
        })
        .unwrap_or(0)
}

fn read_origin(fork_dir: &Path) -> Option<String> {
    fs::read_to_string(fork_dir.join("origin"))
        .ok()
        .map(|s| s.trim().to_owned())
}

fn print_totals(t: &Totals) {
    if t.wal_files > 0 {
        println!(
            "Total: {} segment entries, {} stored  (+{} WAL records across {} file{})",
            fmt_commas(t.seg_entries as u64),
            fmt_size(t.body_bytes),
            t.wal_records,
            t.wal_files,
            if t.wal_files == 1 { "" } else { "s" },
        );
    } else {
        println!(
            "Total: {} entries, {} stored",
            fmt_commas(t.seg_entries as u64),
            fmt_size(t.body_bytes),
        );
    }
}

fn collect_wal_dir(dir: &Path) -> io::Result<Vec<WalInfo>> {
    let mut infos: Vec<WalInfo> = segment::collect_segment_files(dir)?
        .into_iter()
        .map(|path| {
            let ulid = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("?")
                .to_owned();
            let file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            match writelog::scan_readonly(&path) {
                Ok((records, has_partial_tail)) => {
                    let data_count = records
                        .iter()
                        .filter(|r| matches!(r, writelog::LogRecord::Data { .. }))
                        .count();
                    let lba_blocks: u64 = records
                        .iter()
                        .map(|r| match r {
                            writelog::LogRecord::Data { lba_length, .. } => *lba_length as u64,
                            writelog::LogRecord::Ref { lba_length, .. } => *lba_length as u64,
                        })
                        .sum();
                    WalInfo {
                        ulid,
                        file_size,
                        record_count: records.len(),
                        data_count,
                        ref_count: records.len() - data_count,
                        lba_blocks,
                        has_partial_tail,
                        error: None,
                    }
                }
                Err(e) => WalInfo {
                    ulid,
                    file_size,
                    record_count: 0,
                    data_count: 0,
                    ref_count: 0,
                    lba_blocks: 0,
                    has_partial_tail: false,
                    error: Some(e.to_string()),
                },
            }
        })
        .collect();
    infos.sort_by(|a, b| a.ulid.cmp(&b.ulid));
    Ok(infos)
}

fn collect_seg_dir(dir: &Path) -> io::Result<Vec<SegInfo>> {
    let mut infos: Vec<SegInfo> = segment::collect_segment_files(dir)?
        .into_iter()
        .map(|path| {
            let ulid = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("?")
                .to_owned();
            let file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            match segment::read_segment_index(&path) {
                Ok((_body_start, entries)) => {
                    let dedup_ref_count = entries.iter().filter(|e| e.is_dedup_ref).count();
                    let body_bytes: u64 = entries
                        .iter()
                        .filter(|e| !e.is_dedup_ref && !e.is_inline)
                        .map(|e| e.stored_length as u64)
                        .sum();
                    let lba_blocks: u64 = entries.iter().map(|e| e.lba_length as u64).sum();
                    SegInfo {
                        ulid,
                        file_size,
                        entry_count: entries.len(),
                        body_bytes,
                        lba_blocks,
                        dedup_ref_count,
                        error: None,
                    }
                }
                Err(e) => SegInfo {
                    ulid,
                    file_size,
                    entry_count: 0,
                    body_bytes: 0,
                    lba_blocks: 0,
                    dedup_ref_count: 0,
                    error: Some(e.to_string()),
                },
            }
        })
        .collect();
    infos.sort_by(|a, b| a.ulid.cmp(&b.ulid));
    Ok(infos)
}

// --- totals ---

#[derive(Default)]
struct Totals {
    seg_entries: usize,
    body_bytes: u64,
    wal_files: usize,
    wal_records: usize,
}

fn totals(node: &NodeInfo) -> Totals {
    let mut t = Totals::default();
    accumulate(node, &mut t);
    t
}

fn accumulate(node: &NodeInfo, t: &mut Totals) {
    for w in &node.wal_files {
        t.wal_files += 1;
        t.wal_records += w.record_count;
    }
    for s in node.pending.iter().chain(node.segments.iter()) {
        t.seg_entries += s.entry_count;
        t.body_bytes += s.body_bytes;
    }
    for child in &node.children {
        accumulate(child, t);
    }
}

// --- display ---
//
// `line_prefix`  — prepended to the node header line (includes connector for non-root)
// `child_prefix` — prepended to section headers and children of this node

fn print_node(node: &NodeInfo, line_prefix: &str, child_prefix: &str) {
    if node.is_root {
        let state = if node.is_live { "live" } else { "frozen" };
        println!("[{state} root]");
    } else {
        let ulid = node.ulid.as_deref().unwrap_or("?");
        let state = if node.is_live { "live" } else { "frozen" };
        println!("{line_prefix}{ulid}  [{state}]");
    }

    print_wal_section(&node.wal_files, child_prefix, node.is_live);
    print_seg_section("pending", &node.pending, child_prefix, node.is_live);
    print_seg_section("segments", &node.segments, child_prefix, true);

    let n = node.children.len();
    for (i, child) in node.children.iter().enumerate() {
        let last = i + 1 == n;
        let (connector, continuation) = if last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };
        let child_line_prefix = format!("{child_prefix}{connector}");
        let grandchild_prefix = format!("{child_prefix}{continuation}");
        print_node(child, &child_line_prefix, &grandchild_prefix);
    }
}

fn print_wal_section(files: &[WalInfo], prefix: &str, always_show: bool) {
    if files.is_empty() {
        if always_show {
            println!("{prefix}wal/: empty");
        }
        return;
    }
    let plural = if files.len() == 1 { "file" } else { "files" };
    println!("{prefix}wal/ ({} {plural}):", files.len());
    for f in files {
        let p = format!("{prefix}  ");
        if let Some(ref e) = f.error {
            println!("{p}{}  {}  [error: {e}]", f.ulid, fmt_size(f.file_size));
            continue;
        }
        let tail = if f.has_partial_tail {
            "  [partial tail — active?]"
        } else {
            ""
        };
        println!(
            "{p}{}  {}  {} records ({} data, {} ref), {} LBA blocks{}",
            f.ulid,
            fmt_size(f.file_size),
            f.record_count,
            f.data_count,
            f.ref_count,
            f.lba_blocks,
            tail,
        );
    }
}

fn print_seg_section(label: &str, segs: &[SegInfo], prefix: &str, always_show: bool) {
    if segs.is_empty() {
        if always_show {
            println!("{prefix}{label}/: empty");
        }
        return;
    }
    let plural = if segs.len() == 1 { "file" } else { "files" };
    println!("{prefix}{label}/ ({} {plural}):", segs.len());
    for s in segs {
        let p = format!("{prefix}  ");
        if let Some(ref e) = s.error {
            println!("{p}{}  {}  [error: {e}]", s.ulid, fmt_size(s.file_size));
            continue;
        }
        let ref_note = if s.dedup_ref_count > 0 {
            format!(
                ", {} dedup ref{}",
                s.dedup_ref_count,
                if s.dedup_ref_count == 1 { "" } else { "s" }
            )
        } else {
            String::new()
        };
        println!(
            "{p}{}  {}  {} entries, {} body, {} LBA blocks{}",
            s.ulid,
            fmt_size(s.file_size),
            s.entry_count,
            fmt_size(s.body_bytes),
            s.lba_blocks,
            ref_note,
        );
    }
}

fn print_fs_summary(summary: &ls::FsSummary, prefix: &str) {
    println!("{prefix}filesystem (ext4):");
    if let Some(ref name) = summary.os_name {
        println!("{prefix}  os: {name}");
    }
    if !summary.root_entries.is_empty() {
        let listing = summary.root_entries.join("  ");
        println!("{prefix}  /:  {listing}");
    }
}

// --- helpers ---

fn read_size(dir: &Path) -> io::Result<Option<u64>> {
    match fs::read_to_string(dir.join("size")) {
        Ok(s) => {
            let b = s
                .trim()
                .parse::<u64>()
                .map_err(|e| io::Error::other(format!("bad size file: {e}")))?;
            Ok(Some(b))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn fmt_size(bytes: u64) -> String {
    const GIB: u64 = 1 << 30;
    const MIB: u64 = 1 << 20;
    const KIB: u64 = 1 << 10;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn fmt_commas(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume::Volume;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-inspect-test-{}-{}", std::process::id(), n));
        p
    }

    #[test]
    fn fresh_live_fork() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("default");
        let _vol = Volume::open(&fork_dir).unwrap();

        let node = collect_node(&fork_dir, true, false).unwrap();
        assert!(node.is_live);
        assert!(node.is_root);
        assert!(node.children.is_empty());
        assert_eq!(node.wal_files.len(), 1);
        assert_eq!(node.wal_files[0].record_count, 0);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn after_snapshot_fork_stays_live() {
        let vol_dir = temp_dir();
        let fork_dir = vol_dir.join("default");

        {
            let mut vol = Volume::open(&fork_dir).unwrap();
            vol.write(0, &vec![0xAAu8; 4096]).unwrap();
            vol.snapshot().unwrap();
        }

        // Fork is still live after snapshot (wal/ still present).
        let node = collect_node(&fork_dir, true, false).unwrap();
        assert!(node.is_live);
        // Snapshot flushed the WAL → one segment in pending/.
        assert_eq!(node.pending.len(), 1);
        assert_eq!(node.pending[0].entry_count, 1);

        fs::remove_dir_all(vol_dir).unwrap();
    }

    #[test]
    fn readonly_base_fork_shows_not_live() {
        let vol_dir = temp_dir();
        let default_dir = vol_dir.join("default");

        // Simulate a readonly base: create segments/ with no wal/.
        fs::create_dir_all(default_dir.join("segments")).unwrap();
        fs::create_dir_all(default_dir.join("pending")).unwrap();

        let node = collect_node(&default_dir, true, false).unwrap();
        assert!(!node.is_live);

        fs::remove_dir_all(vol_dir).unwrap();
    }
}
