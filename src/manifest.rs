use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use ext4_view::{DirEntry, Ext4, Ext4Error, Metadata, PathBuf as Ext4PathBuf};

pub struct ManifestEntry {
    pub file: String,
    pub chunk_idx: usize,
    pub hash: blake3::Hash,
    pub chunk_size: usize,
}

pub fn generate(image: &Path, chunk_kb: usize, out: &Path) -> Result<(), Ext4Error> {
    let chunk_size = chunk_kb * 1024;
    let fs = Ext4::load_from_path(image)?;
    let mut entries: Vec<ManifestEntry> = Vec::new();
    let mut queue: Vec<Ext4PathBuf> = vec![Ext4PathBuf::new("/")];

    while let Some(dir) = queue.pop() {
        for entry in fs.read_dir(&dir)? {
            let entry: DirEntry = entry?;
            let name = entry.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let path = entry.path();
            let metadata: Metadata = match fs.symlink_metadata(&path) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if metadata.file_type().is_dir() {
                queue.push(path);
            } else if metadata.file_type().is_regular_file() {
                let data: Vec<u8> = match fs.read(&path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let file_name = String::from_utf8_lossy(path.as_ref()).into_owned();
                for (idx, chunk) in data.chunks(chunk_size).enumerate() {
                    if chunk.iter().all(|&b| b == 0) {
                        continue;
                    }
                    let hash = blake3::hash(chunk);
                    entries.push(ManifestEntry {
                        file: file_name.clone(),
                        chunk_idx: idx,
                        hash,
                        chunk_size: chunk.len(),
                    });
                }
            }
        }
    }

    entries.sort_by(|a, b| a.file.cmp(&b.file).then(a.chunk_idx.cmp(&b.chunk_idx)));

    let file = File::create(out).expect("failed to create output file");
    let mut writer = BufWriter::new(file);
    for e in &entries {
        writeln!(writer, "{}\t{}\t{}", e.hash, e.chunk_idx, e.file).expect("write failed");
    }

    println!("Manifest: {} entries → {}", entries.len(), out.display());
    let total_bytes: usize = entries.iter().map(|e| e.chunk_size).sum();
    println!("  Image data covered: {:.1} MB", total_bytes as f64 / (1024.0 * 1024.0));
    println!("  Unique chunks:      {}", {
        let mut hashes: Vec<_> = entries.iter().map(|e| e.hash).collect();
        hashes.sort_unstable_by_key(|h| *h.as_bytes());
        hashes.dedup();
        hashes.len()
    });

    Ok(())
}

struct Manifest {
    by_position: HashMap<(String, usize), blake3::Hash>,
    hashes: std::collections::HashSet<blake3::Hash>,
}

fn load_manifest(path: &Path) -> Manifest {
    let file = File::open(path).expect("failed to open manifest");
    let reader = BufReader::new(file);
    let mut by_position = HashMap::new();
    let mut hashes = std::collections::HashSet::new();
    for line in reader.lines() {
        let line = line.expect("read error");
        let mut parts = line.splitn(3, '\t');
        let hash_hex = parts.next().unwrap_or("");
        let idx_str = parts.next().unwrap_or("0");
        let file_path = parts.next().unwrap_or("").to_owned();
        if let Ok(hash) = blake3::Hash::from_hex(hash_hex) {
            if let Ok(idx) = idx_str.parse::<usize>() {
                by_position.insert((file_path, idx), hash);
                hashes.insert(hash);
            }
        }
    }
    Manifest { by_position, hashes }
}

pub fn diff(manifest1: &Path, manifest2: &Path, verbose: bool) {
    let m1 = load_manifest(manifest1);
    let m2 = load_manifest(manifest2);

    let mut unchanged = 0usize;
    let mut modified = 0usize;
    let mut deleted = 0usize;
    let mut added = 0usize;

    // chunks in m1
    for (key, h1) in &m1.by_position {
        match m2.by_position.get(key) {
            Some(h2) if h1 == h2 => unchanged += 1,
            Some(_) => {
                modified += 1;
                if verbose {
                    println!("  modified  {}[{}]", key.0, key.1);
                }
            }
            None => {
                deleted += 1;
                if verbose {
                    println!("  deleted   {}[{}]", key.0, key.1);
                }
            }
        }
    }

    // chunks only in m2
    for (key, _) in &m2.by_position {
        if !m1.by_position.contains_key(key) {
            added += 1;
            if verbose {
                println!("  added     {}[{}]", key.0, key.1);
            }
        }
    }

    // content-addressed overlap: hashes in m2 not seen anywhere in m1
    let content_new: usize = m2.hashes.iter().filter(|h| !m1.hashes.contains(h)).count();
    let content_shared = m2.hashes.len().saturating_sub(content_new);

    let total1 = m1.by_position.len();
    let total2 = m2.by_position.len();
    let changed = modified + deleted + added;
    let marginal_path = modified + added;

    println!("=== Manifest Diff ===");
    println!("  {}: {} chunks ({} unique hashes)", manifest1.display(), total1, m1.hashes.len());
    println!("  {}: {} chunks ({} unique hashes)", manifest2.display(), total2, m2.hashes.len());

    println!("\n  Path-based diff (same file, same position):");
    println!("  Unchanged: {:>7}  ({:.1}%)", unchanged, 100.0 * unchanged as f64 / total1.max(1) as f64);
    println!("  Modified:  {:>7}  ({:.1}%)", modified,  100.0 * modified  as f64 / total1.max(1) as f64);
    println!("  Deleted:   {:>7}  ({:.1}%)", deleted,   100.0 * deleted   as f64 / total1.max(1) as f64);
    println!("  Added:     {:>7}  ({:.1}%)", added,     100.0 * added     as f64 / total2.max(1) as f64);
    println!("  Marginal (path-based):    {} chunks", marginal_path);

    println!("\n  Content-addressed overlap (hash seen anywhere in snapshot 1):");
    println!("  Shared hashes: {:>7}  ({:.1}% of snapshot 2)", content_shared, 100.0 * content_shared as f64 / m2.hashes.len().max(1) as f64);
    println!("  New hashes:    {:>7}  ({:.1}% of snapshot 2)", content_new,    100.0 * content_new    as f64 / m2.hashes.len().max(1) as f64);
    println!("  Marginal (content-addressed): {} unique chunks must be fetched", content_new);
    println!("\n  Note: path-based marginal ({} chunks) overcounts renames/moves;", marginal_path);
    println!("        content-addressed marginal ({} chunks) is the true S3 fetch cost.", content_new);
    let _ = changed;
}
