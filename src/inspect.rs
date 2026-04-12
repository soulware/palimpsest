// Inspect a volume directory and print a human-readable summary.
//
// `dir` is a single volume directory (by_id/<ulid>/ or a by_name/<name>
// symlink resolving to one). The volume owns its own wal/, pending/,
// index/, cache/, and snapshots/ directly — there is no forks/ subdirectory.
//
// Does not modify any files.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use serde::Deserialize;

use elide_core::{
    segment::{self, EntryKind},
    volume, writelog,
};

pub fn run(dir: &Path, by_id_dir: &Path) -> io::Result<()> {
    let cfg = elide_core::config::VolumeConfig::read(dir)?;

    let vol_name = cfg
        .name
        .as_deref()
        .or_else(|| dir.file_name().and_then(|s| s.to_str()))
        .unwrap_or("<unknown>")
        .to_owned();

    let size_bytes = cfg.size;
    let is_readonly = dir.join("volume.readonly").exists();
    let meta = read_meta(dir);

    println!("Volume:     {vol_name}");
    match size_bytes {
        Some(b) => println!("Size:       {} ({} bytes)", fmt_size(b), fmt_commas(b)),
        None => println!("Size:       (no size file found)"),
    }
    println!("Filesystem: ext4");
    if is_readonly {
        println!("Type:       readonly");
    }
    if let Some(VolumeMeta {
        source: Some(source),
        digest: Some(digest),
        arch: Some(arch),
        ..
    }) = &meta
    {
        let short_digest = digest.get(7..19).unwrap_or(digest);
        println!(
            "Source:     {}  (sha256:{})  {}",
            source, short_digest, arch
        );
    }
    if let Some(origin) = read_origin(dir) {
        println!("Origin:     {origin}");
    }
    println!();

    let snap_count = count_snapshots(dir);
    let latest_snap = volume::latest_snapshot(dir).ok().flatten();
    match (snap_count, &latest_snap) {
        (0, _) => {}
        (n, Some(s)) => println!("{n} snapshot(s), latest {s}"),
        (n, None) => println!("{n} snapshot(s)"),
    }

    let latest_snap_str = latest_snap.as_ref().map(|s| s.to_string());
    let node = collect_node(dir)?;
    let ancestors = collect_ancestor_nodes(dir, by_id_dir)?;
    print_node(&node, latest_snap_str.as_deref());
    print_ancestor_nodes(&ancestors);

    let t = totals(&node, &ancestors);
    print_totals(&t);

    Ok(())
}

// --- node collection ---

struct NodeInfo {
    is_live: bool,
    wal_files: Vec<WalInfo>,
    pending: Vec<SegInfo>,
    cache: Vec<CacheInfo>,
}

struct AncestorNode {
    volume_ulid: String,
    branch_ulid: Option<String>,
    cache: Vec<CacheInfo>,
}

struct CacheInfo {
    ulid: String,
    /// Total entries in the index (all kinds).
    entry_count: usize,
    /// Entries of each kind.
    data_count: usize,
    dedup_ref_count: usize,
    zero_count: usize,
    inline_count: usize,
    delta_count: usize,
    /// Entries with body data (Data + DedupRef) — the ones that can be fetched.
    fetchable_count: usize,
    /// How many fetchable entries have their present bit set.
    present_count: usize,
    /// Sum of stored_length for Data entries only (the unique bytes this segment stores).
    data_body_bytes: u64,
    /// Sum of stored_length for DedupRef entries (body bytes referenced from canonical segments).
    dedup_ref_body_bytes: u64,
    /// Byte length of the segment's delta body section (from the segment header).
    delta_body_bytes: u64,
    /// Size of the `.idx` file on disk.
    idx_file_bytes: u64,
    /// Actual disk blocks occupied by the .body sparse file.
    body_bytes_cached: u64,
    error: Option<String>,
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
    delta_count: usize,
    delta_body_bytes: u64,
    error: Option<String>,
}

fn collect_node(dir: &Path) -> io::Result<NodeInfo> {
    let is_live = dir.join("wal").is_dir();

    let wal_files = collect_wal_dir(&dir.join("wal"))?;
    let pending = collect_seg_dir(&dir.join("pending"))?;
    let cache = collect_cache_dir(dir)?;

    Ok(NodeInfo {
        is_live,
        wal_files,
        pending,
        cache,
    })
}

/// Walk the ancestry chain, newest-first, collecting each ancestor's
/// committed segments up to the branch point.
fn collect_ancestor_nodes(fork_dir: &Path, by_id_dir: &Path) -> io::Result<Vec<AncestorNode>> {
    let layers = match volume::walk_ancestors(fork_dir, by_id_dir) {
        Ok(l) => l,
        Err(_) => return Ok(Vec::new()),
    };
    // walk_ancestors returns oldest-first; user wants newest-first.
    let mut nodes: Vec<AncestorNode> = Vec::new();
    for layer in layers.into_iter().rev() {
        let volume_ulid = layer
            .dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_owned();
        let mut cache = collect_cache_dir(&layer.dir)?;
        if let Some(ref branch) = layer.branch_ulid {
            cache.retain(|c| c.ulid.as_str() <= branch.as_str());
        }
        nodes.push(AncestorNode {
            volume_ulid,
            branch_ulid: layer.branch_ulid,
            cache,
        });
    }
    Ok(nodes)
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
    fs::read_to_string(fork_dir.join("volume.parent"))
        .ok()
        .map(|s| s.trim().to_owned())
}

fn print_totals(t: &Totals) {
    println!();
    if t.cache_files > 0 {
        let pct = if t.cache_fetchable > 0 {
            format!(
                "{:.1}%",
                100.0 * t.cache_present as f64 / t.cache_fetchable as f64
            )
        } else {
            "0%".to_owned()
        };
        let total_index: usize =
            t.cache_data + t.cache_dedup_ref + t.cache_delta + t.cache_zero + t.cache_inline;
        println!(
            "Total: {} segment{}",
            t.cache_files,
            if t.cache_files == 1 { "" } else { "s" },
        );
        println!(
            "  index:   {} ({} idx, {} body on disk)  (dedup {}, inline {}, zero {})",
            fmt_commas(total_index as u64),
            fmt_size(t.cache_idx_file_bytes),
            fmt_size(t.cache_body_actual),
            fmt_commas(t.cache_dedup_ref as u64),
            fmt_commas(t.cache_inline as u64),
            fmt_commas(t.cache_zero as u64),
        );
        println!(
            "  data:    {} ({})",
            fmt_commas(t.cache_data as u64),
            fmt_size(t.cache_data_body),
        );
        println!(
            "  delta:   {} ({})",
            fmt_commas(t.cache_delta as u64),
            fmt_size(t.cache_delta_body),
        );
        println!(
            "  present: {} / {} fetchable ({})",
            fmt_commas(t.cache_present as u64),
            fmt_commas(t.cache_fetchable as u64),
            pct,
        );
    }
    if t.wal_files > 0 || t.seg_entries > 0 {
        println!(
            "  wal:     {} record{} across {} file{}, {} pending entries ({} stored)",
            t.wal_records,
            if t.wal_records == 1 { "" } else { "s" },
            t.wal_files,
            if t.wal_files == 1 { "" } else { "s" },
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
                            writelog::LogRecord::Zero { lba_length, .. } => *lba_length as u64,
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
                    let dedup_ref_count = entries
                        .iter()
                        .filter(|e| e.kind == EntryKind::DedupRef)
                        .count();
                    let delta_count = entries
                        .iter()
                        .filter(|e| e.kind == EntryKind::Delta)
                        .count();
                    let body_bytes: u64 = entries
                        .iter()
                        .filter(|e| matches!(e.kind, EntryKind::Data | EntryKind::DedupRef))
                        .map(|e| e.stored_length as u64)
                        .sum();
                    let lba_blocks: u64 = entries.iter().map(|e| e.lba_length as u64).sum();
                    let delta_body_bytes = segment::read_segment_layout(&path)
                        .map(|l| l.delta_length as u64)
                        .unwrap_or(0);
                    SegInfo {
                        ulid,
                        file_size,
                        entry_count: entries.len(),
                        body_bytes,
                        lba_blocks,
                        dedup_ref_count,
                        delta_count,
                        delta_body_bytes,
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
                    delta_count: 0,
                    delta_body_bytes: 0,
                    error: Some(e.to_string()),
                },
            }
        })
        .collect();
    infos.sort_by(|a, b| a.ulid.cmp(&b.ulid));
    Ok(infos)
}

fn collect_cache_dir(dir: &Path) -> io::Result<Vec<CacheInfo>> {
    // .idx files live in index/ (coordinator-written); .body/.present in cache/ (volume cache).
    let index_dir = dir.join("index");
    let cache_dir = dir.join("cache");
    let rd = match fs::read_dir(&index_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut idx_paths: Vec<PathBuf> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "idx")
                .unwrap_or(false)
        })
        .collect();
    idx_paths.sort();

    let mut infos = Vec::new();
    for idx_path in idx_paths {
        let ulid = idx_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_owned();
        match collect_cache_file(&cache_dir, &ulid, &idx_path) {
            Ok(info) => infos.push(info),
            Err(e) => infos.push(CacheInfo {
                ulid,
                entry_count: 0,
                data_count: 0,
                dedup_ref_count: 0,
                zero_count: 0,
                inline_count: 0,
                delta_count: 0,
                fetchable_count: 0,
                present_count: 0,
                data_body_bytes: 0,
                dedup_ref_body_bytes: 0,
                delta_body_bytes: 0,
                idx_file_bytes: 0,
                body_bytes_cached: 0,
                error: Some(e.to_string()),
            }),
        }
    }
    Ok(infos)
}

fn collect_cache_file(cache_dir: &Path, ulid: &str, idx_path: &Path) -> io::Result<CacheInfo> {
    let (_body_start, entries) = segment::read_segment_index(idx_path)
        .map_err(|e| io::Error::other(format!("reading index: {e}")))?;

    let mut data_count = 0usize;
    let mut dedup_ref_count = 0usize;
    let mut zero_count = 0usize;
    let mut inline_count = 0usize;
    let mut delta_count = 0usize;
    let mut data_body_bytes = 0u64;
    let mut dedup_ref_body_bytes = 0u64;
    for e in &entries {
        match e.kind {
            EntryKind::Data => {
                data_count += 1;
                data_body_bytes += e.stored_length as u64;
            }
            EntryKind::DedupRef => {
                dedup_ref_count += 1;
                dedup_ref_body_bytes += e.stored_length as u64;
            }
            EntryKind::Zero => zero_count += 1,
            EntryKind::Inline => inline_count += 1,
            EntryKind::Delta => delta_count += 1,
        }
    }
    let fetchable_count = data_count + dedup_ref_count;

    // Count set bits in .present, capped at fetchable_count to ignore padding
    // bits in the last byte (write_cache sets all bits in the final byte).
    let present_path = cache_dir.join(format!("{ulid}.present"));
    let present_bytes = fs::read(&present_path).unwrap_or_default();
    let present_count: usize = present_bytes
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum::<usize>()
        .min(fetchable_count);

    // Actual disk blocks used by the sparse .body file.
    let body_path = cache_dir.join(format!("{ulid}.body"));
    #[cfg(unix)]
    let body_bytes_cached = fs::metadata(&body_path)
        .map(|m| m.blocks() * 512)
        .unwrap_or(0);
    #[cfg(not(unix))]
    let body_bytes_cached = fs::metadata(&body_path).map(|m| m.len()).unwrap_or(0);

    // Delta body section size is recorded in the segment header; .idx files
    // carry the same header so read_segment_layout works on them.
    let delta_body_bytes = segment::read_segment_layout(idx_path)
        .map(|l| l.delta_length as u64)
        .unwrap_or(0);

    let idx_file_bytes = fs::metadata(idx_path).map(|m| m.len()).unwrap_or(0);

    Ok(CacheInfo {
        ulid: ulid.to_owned(),
        entry_count: entries.len(),
        data_count,
        dedup_ref_count,
        zero_count,
        inline_count,
        delta_count,
        fetchable_count,
        present_count,
        data_body_bytes,
        dedup_ref_body_bytes,
        delta_body_bytes,
        idx_file_bytes,
        body_bytes_cached,
        error: None,
    })
}

// --- totals ---

#[derive(Default)]
struct Totals {
    seg_entries: usize,
    body_bytes: u64,
    wal_files: usize,
    wal_records: usize,
    cache_files: usize,
    cache_data: usize,
    cache_dedup_ref: usize,
    cache_zero: usize,
    cache_inline: usize,
    cache_delta: usize,
    cache_fetchable: usize,
    cache_present: usize,
    cache_data_body: u64,
    cache_dedup_ref_body: u64,
    cache_delta_body: u64,
    cache_idx_file_bytes: u64,
    cache_body_actual: u64,
}

fn totals(node: &NodeInfo, ancestors: &[AncestorNode]) -> Totals {
    let mut t = Totals::default();
    accumulate(node, &mut t);
    for a in ancestors {
        accumulate_cache(&a.cache, &mut t);
    }
    t
}

fn accumulate(node: &NodeInfo, t: &mut Totals) {
    for w in &node.wal_files {
        t.wal_files += 1;
        t.wal_records += w.record_count;
    }
    for s in node.pending.iter() {
        t.seg_entries += s.entry_count;
        t.body_bytes += s.body_bytes;
    }
    accumulate_cache(&node.cache, t);
}

fn accumulate_cache(cache: &[CacheInfo], t: &mut Totals) {
    for f in cache {
        t.cache_files += 1;
        t.cache_data += f.data_count;
        t.cache_dedup_ref += f.dedup_ref_count;
        t.cache_zero += f.zero_count;
        t.cache_inline += f.inline_count;
        t.cache_delta += f.delta_count;
        t.cache_fetchable += f.fetchable_count;
        t.cache_present += f.present_count;
        t.cache_data_body += f.data_body_bytes;
        t.cache_dedup_ref_body += f.dedup_ref_body_bytes;
        t.cache_delta_body += f.delta_body_bytes;
        t.cache_idx_file_bytes += f.idx_file_bytes;
        t.cache_body_actual += f.body_bytes_cached;
    }
}

// --- display ---

fn print_node(node: &NodeInfo, latest_snap: Option<&str>) {
    let state = if node.is_live { "live" } else { "frozen" };
    println!("[{state} root]");

    let prefix = "  ";
    print_wal_section(&node.wal_files, prefix, node.is_live, latest_snap);
    print_seg_section("pending", &node.pending, prefix, node.is_live, latest_snap);
    print_cache_section(&node.cache, prefix, latest_snap, true);
}

fn print_ancestor_nodes(ancestors: &[AncestorNode]) {
    for a in ancestors {
        if a.cache.is_empty() {
            continue;
        }
        let plural = if a.cache.len() == 1 { "file" } else { "files" };
        match &a.branch_ulid {
            Some(b) => println!(
                "  index/ (from ancestor {} @ snap {}, {} {}):",
                a.volume_ulid,
                b,
                a.cache.len(),
                plural,
            ),
            None => println!(
                "  index/ (from ancestor {}, {} {}):",
                a.volume_ulid,
                a.cache.len(),
                plural,
            ),
        }
        // Ancestor segments are by definition in a snapshot, so no
        // post-snapshot marker applies.
        print_cache_section(&a.cache, "  ", None, false);
    }
}

/// True if `ulid` sorts strictly greater than the latest snapshot ULID
/// — meaning this file was created after the most recent snapshot and is
/// therefore still eligible for future repacking/GC.
fn is_post_snapshot(ulid: &str, latest_snap: Option<&str>) -> bool {
    match latest_snap {
        Some(s) => ulid > s,
        None => false,
    }
}

fn post_snap_tag(ulid: &str, latest_snap: Option<&str>) -> &'static str {
    if is_post_snapshot(ulid, latest_snap) {
        "  [post-snapshot]"
    } else {
        ""
    }
}

fn print_wal_section(
    files: &[WalInfo],
    prefix: &str,
    always_show: bool,
    latest_snap: Option<&str>,
) {
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
            "{p}{}  {}  {} records ({} data, {} ref), {} LBA blocks{}{}",
            f.ulid,
            fmt_size(f.file_size),
            f.record_count,
            f.data_count,
            f.ref_count,
            f.lba_blocks,
            tail,
            post_snap_tag(&f.ulid, latest_snap),
        );
    }
}

fn print_seg_section(
    label: &str,
    segs: &[SegInfo],
    prefix: &str,
    always_show: bool,
    latest_snap: Option<&str>,
) {
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
        let delta_note = if s.delta_count > 0 {
            format!(
                ", {} delta{} ({} delta body)",
                s.delta_count,
                if s.delta_count == 1 { "" } else { "s" },
                fmt_size(s.delta_body_bytes),
            )
        } else {
            String::new()
        };
        println!(
            "{p}{}  {}  {} entries, {} body, {} LBA blocks{}{}{}",
            s.ulid,
            fmt_size(s.file_size),
            s.entry_count,
            fmt_size(s.body_bytes),
            s.lba_blocks,
            ref_note,
            delta_note,
            post_snap_tag(&s.ulid, latest_snap),
        );
    }
}

fn print_cache_section(
    cache: &[CacheInfo],
    prefix: &str,
    latest_snap: Option<&str>,
    print_header: bool,
) {
    if cache.is_empty() {
        return;
    }
    if print_header {
        let plural = if cache.len() == 1 { "file" } else { "files" };
        println!("{prefix}index/ ({} {plural}):", cache.len());
    }
    for f in cache {
        let p = format!("{prefix}  ");
        if let Some(ref e) = f.error {
            println!("{p}{}  [error: {e}]", f.ulid);
            continue;
        }
        let pct = if f.fetchable_count > 0 {
            format!(
                "{:.1}%",
                100.0 * f.present_count as f64 / f.fetchable_count as f64
            )
        } else {
            "0%".to_owned()
        };
        let indent = format!("{p}  ");
        println!("{p}{}{}", f.ulid, post_snap_tag(&f.ulid, latest_snap),);
        println!(
            "{indent}index:   {} ({})  (dedup {}, inline {}, zero {})",
            fmt_commas(f.entry_count as u64),
            fmt_size(f.idx_file_bytes),
            fmt_commas(f.dedup_ref_count as u64),
            fmt_commas(f.inline_count as u64),
            fmt_commas(f.zero_count as u64),
        );
        println!(
            "{indent}data:    {} ({})",
            fmt_commas(f.data_count as u64),
            fmt_size(f.data_body_bytes),
        );
        println!(
            "{indent}delta:   {} ({})",
            fmt_commas(f.delta_count as u64),
            fmt_size(f.delta_body_bytes),
        );
        println!(
            "{indent}present: {} / {} fetchable ({})",
            fmt_commas(f.present_count as u64),
            fmt_commas(f.fetchable_count as u64),
            pct,
        );
    }
}

// --- helpers ---

#[derive(Deserialize)]
struct VolumeMeta {
    source: Option<String>,
    digest: Option<String>,
    arch: Option<String>,
}

fn read_meta(dir: &Path) -> Option<VolumeMeta> {
    let content = fs::read_to_string(dir.join("meta.toml")).ok()?;
    toml::from_str(&content).ok()
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
    use elide_core::volume::Volume;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_vol_dir() -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("elide-inspect-test-{}-{}", std::process::id(), n));
        p
    }

    #[test]
    fn fresh_live_volume() {
        let tmp = temp_vol_dir();
        let by_id_dir = tmp.join("by_id");
        let vol_dir = by_id_dir.join("01JQAAAAAAAAAAAAAAAAAAAAAA");
        fs::create_dir_all(&vol_dir).unwrap();
        elide_core::signing::generate_keypair(
            &vol_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        let _vol = Volume::open(&vol_dir, &by_id_dir).unwrap();

        let node = collect_node(&vol_dir).unwrap();
        assert!(node.is_live);
        assert_eq!(node.wal_files.len(), 1);
        assert_eq!(node.wal_files[0].record_count, 0);

        fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn after_snapshot_volume_stays_live() {
        let tmp = temp_vol_dir();
        let by_id_dir = tmp.join("by_id");
        let vol_dir = by_id_dir.join("01JQAAAAAAAAAAAAAAAAAAAAAA");
        fs::create_dir_all(&vol_dir).unwrap();
        let key = elide_core::signing::generate_keypair(
            &vol_dir,
            elide_core::signing::VOLUME_KEY_FILE,
            elide_core::signing::VOLUME_PUB_FILE,
        )
        .unwrap();
        elide_core::signing::write_provenance(
            &vol_dir,
            &key,
            elide_core::signing::VOLUME_PROVENANCE_FILE,
            &elide_core::signing::ProvenanceLineage::default(),
        )
        .unwrap();

        {
            let mut vol = Volume::open(&vol_dir, &by_id_dir).unwrap();
            vol.write(0, &vec![0xAAu8; 4096]).unwrap();
            vol.snapshot().unwrap();
        }

        let node = collect_node(&vol_dir).unwrap();
        assert!(node.is_live);
        // snapshot() auto-promotes pending segments to index/cache.
        assert_eq!(node.cache.len(), 1);
        assert_eq!(node.cache[0].entry_count, 1);

        fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn readonly_volume_shows_not_live() {
        let tmp = temp_vol_dir();
        let vol_dir = tmp.join("by_id").join("01JQAAAAAAAAAAAAAAAAAAAAAA");

        fs::create_dir_all(vol_dir.join("index")).unwrap();
        fs::create_dir_all(vol_dir.join("pending")).unwrap();

        let node = collect_node(&vol_dir).unwrap();
        assert!(!node.is_live);

        fs::remove_dir_all(tmp).unwrap();
    }
}
