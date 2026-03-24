use std::collections::HashMap;
use std::path::Path;

use ext4_view::{DirEntry, Ext4, Ext4Error, Metadata, PathBuf as Ext4PathBuf};

const NUM_HASHES: usize = 128;
const BANDS: usize = 32;
const ROWS: usize = NUM_HASHES / BANDS; // 4
const MAX_BUCKET_SIZE: usize = 50; // skip buckets with too many members to avoid O(n²) explosion

fn make_hash_params() -> (Vec<u64>, Vec<u64>) {
    let mut state: u64 = 0xdeadbeefcafe1234;
    let mut a = Vec::with_capacity(NUM_HASHES);
    let mut b = Vec::with_capacity(NUM_HASHES);
    for _ in 0..NUM_HASHES {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        a.push(state | 1);
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        b.push(state);
    }
    (a, b)
}

fn minhash(chunk: &[u8], a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut sig = vec![u64::MAX; NUM_HASHES];
    for shingle in chunk.chunks(4) {
        let x = shingle.iter().fold(0u64, |acc, &byte| acc.wrapping_shl(8) | byte as u64);
        for i in 0..NUM_HASHES {
            let h = a[i].wrapping_mul(x).wrapping_add(b[i]);
            if h < sig[i] {
                sig[i] = h;
            }
        }
    }
    sig
}

fn jaccard_estimate(sig1: &[u64], sig2: &[u64]) -> f64 {
    let matches = sig1.iter().zip(sig2.iter()).filter(|(a, b)| a == b).count();
    matches as f64 / NUM_HASHES as f64
}

struct ChunkInfo {
    file: String,
    position: usize,
    hash: blake3::Hash,
    sig: Vec<u64>,
}

pub fn run(image: &Path, chunk_size: usize) -> Result<(), Ext4Error> {
    println!("Loading {}...", image.display());
    let fs = Ext4::load_from_path(image)?;
    let (a, b) = make_hash_params();
    let mut chunks: Vec<ChunkInfo> = Vec::new();

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
                for (pos, chunk) in data.chunks(chunk_size).enumerate() {
                    if chunk.len() < 64 || chunk.iter().all(|&b| b == 0) {
                        continue;
                    }
                    let hash = blake3::hash(chunk);
                    let sig = minhash(chunk, &a, &b);
                    chunks.push(ChunkInfo { file: file_name.clone(), position: pos, hash, sig });
                }
            }
        }
    }

    println!("Chunks loaded: {}", chunks.len());
    println!("Computing LSH buckets...");

    let mut candidate_pairs: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();

    for band in 0..BANDS {
        let mut buckets: HashMap<u64, Vec<usize>> = HashMap::new();
        let row_start = band * ROWS;
        let row_end = row_start + ROWS;

        for (idx, chunk) in chunks.iter().enumerate() {
            let band_key = chunk.sig[row_start..row_end]
                .iter()
                .fold(0u64, |acc, &v| acc.wrapping_mul(6364136223846793005).wrapping_add(v));
            buckets.entry(band_key).or_default().push(idx);
        }

        for members in buckets.values() {
            if members.len() < 2 || members.len() > MAX_BUCKET_SIZE {
                continue;
            }
            for i in 0..members.len() {
                for j in i + 1..members.len() {
                    let (lo, hi) = if members[i] < members[j] {
                        (members[i], members[j])
                    } else {
                        (members[j], members[i])
                    };
                    candidate_pairs.insert((lo, hi));
                }
            }
        }
    }

    println!("Candidate pairs: {}", candidate_pairs.len());

    // Filter out exact matches and compute Jaccard for the rest
    let mut similarities: Vec<(f64, usize, usize)> = candidate_pairs
        .iter()
        .filter(|&&(i, j)| chunks[i].hash != chunks[j].hash)
        .map(|&(i, j)| (jaccard_estimate(&chunks[i].sig, &chunks[j].sig), i, j))
        .collect();

    similarities.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

    let exact_pairs = candidate_pairs.len() - similarities.len();

    println!("\n=== Similarity Report ===");
    println!("  Chunks analysed:         {}", chunks.len());
    println!("  Candidate pairs (LSH):   {}", candidate_pairs.len());
    println!("  Exact match pairs:       {}", exact_pairs);
    println!("  Near-duplicate pairs:    {}", similarities.len());

    if similarities.is_empty() {
        println!("  No near-duplicate pairs found.");
        return Ok(());
    }

    let mut dist_buckets = [0usize; 5];
    for &(sim, _, _) in &similarities {
        let bucket = ((sim * 5.0) as usize).min(4);
        dist_buckets[bucket] += 1;
    }

    println!("\n  Jaccard similarity distribution (near-duplicates only):");
    let labels = ["0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"];
    for (i, &count) in dist_buckets.iter().enumerate() {
        let pct = 100.0 * count as f64 / similarities.len() as f64;
        let bar: String = "#".repeat((pct / 2.0) as usize);
        println!("    [{}]: {:>6} ({:>5.1}%)  {}", labels[i], count, pct, bar);
    }

    println!("\n  Top 30 most similar pairs:");
    for &(sim, i, j) in similarities.iter().take(30) {
        println!(
            "    {:.3}  {}[{}]  <>  {}[{}]",
            sim, chunks[i].file, chunks[i].position, chunks[j].file, chunks[j].position
        );
    }

    // Investigate perfect-Jaccard (1.0) pairs: do a second pass to load the actual
    // chunk bytes and measure how many bytes actually differ.
    let perfect_pairs: Vec<(usize, usize)> = similarities
        .iter()
        .filter(|&&(sim, _, _)| sim == 1.0)
        .map(|&(_, i, j)| (i, j))
        .take(100) // cap to keep the second pass tractable
        .collect();

    if perfect_pairs.is_empty() {
        return Ok(());
    }

    println!("\n  Loading chunk data for {} perfect-Jaccard pairs...", perfect_pairs.len());

    // Build the set of (file, position) we need to fetch
    let needed: std::collections::HashSet<(String, usize)> = perfect_pairs
        .iter()
        .flat_map(|&(i, j)| {
            [
                (chunks[i].file.clone(), chunks[i].position),
                (chunks[j].file.clone(), chunks[j].position),
            ]
        })
        .collect();

    // Second pass: walk the filesystem and collect needed chunks
    let mut chunk_data: HashMap<(String, usize), Vec<u8>> = HashMap::new();
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
                let file_name = String::from_utf8_lossy(path.as_ref()).into_owned();
                // Only read files we actually need
                let positions_needed: Vec<usize> = needed
                    .iter()
                    .filter(|(f, _)| f == &file_name)
                    .map(|(_, p)| *p)
                    .collect();
                if positions_needed.is_empty() {
                    continue;
                }
                let data: Vec<u8> = match fs.read(&path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                for pos in positions_needed {
                    let start = pos * chunk_size;
                    if start < data.len() {
                        let end = (start + chunk_size).min(data.len());
                        chunk_data.insert((file_name.clone(), pos), data[start..end].to_vec());
                    }
                }
            }
        }
    }

    const ZSTD_LEVEL: i32 = 3;

    println!("\n  Perfect-Jaccard pairs — byte differences and delta compression:");
    println!("  {:>8}  {:>7}  {:>9}  {:>9}  {:>9}  {}",
        "diffbytes", "diff%", "raw", "standalone", "dict", "pair");

    let mut diff_counts: Vec<usize> = Vec::new();
    let mut total_raw = 0usize;
    let mut total_standalone = 0usize;
    let mut total_dict = 0usize;

    for (i, j) in &perfect_pairs {
        let key_i = (chunks[*i].file.clone(), chunks[*i].position);
        let key_j = (chunks[*j].file.clone(), chunks[*j].position);
        if let (Some(ci), Some(cj)) = (chunk_data.get(&key_i), chunk_data.get(&key_j)) {
            let len = ci.len().min(cj.len());
            let diff = ci[..len].iter().zip(cj[..len].iter()).filter(|(a, b)| a != b).count()
                + ci.len().abs_diff(cj.len());
            let diff_pct = 100.0 * diff as f64 / len.max(1) as f64;
            diff_counts.push(diff);

            let standalone = zstd::bulk::compress(cj, ZSTD_LEVEL).unwrap_or_default();
            let dict_compressed = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, ci)
                .and_then(|mut c| c.compress(cj))
                .unwrap_or_else(|_| standalone.clone());

            total_raw += cj.len();
            total_standalone += standalone.len();
            total_dict += dict_compressed.len();

            println!(
                "  {:>8}  {:>6.2}%  {:>9}  {:>9}  {:>9}  {}[{}]  <>  {}[{}]",
                diff, diff_pct,
                cj.len(), standalone.len(), dict_compressed.len(),
                chunks[*i].file, chunks[*i].position,
                chunks[*j].file, chunks[*j].position
            );
        }
    }

    if !diff_counts.is_empty() {
        diff_counts.sort();
        println!("\n  Summary for {} pairs:", diff_counts.len());
        println!("    Differing bytes — median: {}, max: {}", diff_counts[diff_counts.len() / 2], diff_counts[diff_counts.len() - 1]);
        println!("    Raw total:        {:>9} bytes", total_raw);
        println!("    Standalone zstd:  {:>9} bytes ({:.1}x)", total_standalone, total_raw as f64 / total_standalone.max(1) as f64);
        println!("    Dict zstd:        {:>9} bytes ({:.1}x)", total_dict, total_raw as f64 / total_dict.max(1) as f64);
        println!("    Delta benefit:    {:>9} bytes saved ({:.1}%)",
            total_standalone.saturating_sub(total_dict),
            100.0 * total_standalone.saturating_sub(total_dict) as f64 / total_standalone.max(1) as f64);
    }

    Ok(())
}
