use std::collections::HashMap;
use std::path::Path;

use ext4_view::{DirEntry, Ext4, Ext4Error, Metadata, PathBuf as Ext4PathBuf};

/// Chunk all regular files in an ext4 image, returning a map from file path to list of chunks.
fn chunk_image(image: &Path, chunk_size: usize) -> Result<HashMap<String, Vec<Vec<u8>>>, Ext4Error> {
    let fs = Ext4::load_from_path(image)?;
    let mut files: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
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
                let chunks: Vec<Vec<u8>> = data.chunks(chunk_size).map(|c| c.to_vec()).collect();
                let key = String::from_utf8_lossy(path.as_ref()).into_owned();
                files.insert(key, chunks);
            }
        }
    }

    Ok(files)
}

pub fn run(image1: &Path, image2: &Path, chunk_size: usize, level: i32) -> Result<(), Ext4Error> {
    println!("Loading {}...", image1.display());
    let files1 = chunk_image(image1, chunk_size)?;
    println!("Loading {}...", image2.display());
    let files2 = chunk_image(image2, chunk_size)?;

    // Find files present in both images
    let common_paths: Vec<&String> = files1.keys().filter(|p| files2.contains_key(*p)).collect();
    println!("Common files: {}", common_paths.len());

    let mut total_pairs = 0usize;
    let mut exact_matches = 0usize;
    let mut diff_pairs = 0usize;

    // For non-matching pairs: standalone and dictionary-compressed sizes
    let mut standalone_sizes: Vec<usize> = Vec::new();
    let mut dict_sizes: Vec<usize> = Vec::new();

    for path in &common_paths {
        let chunks1 = &files1[*path];
        let chunks2 = &files2[*path];
        let pairs = chunks1.len().min(chunks2.len());

        for i in 0..pairs {
            total_pairs += 1;
            let c1 = &chunks1[i];
            let c2 = &chunks2[i];

            if c1 == c2 {
                exact_matches += 1;
                continue;
            }

            diff_pairs += 1;

            // Standalone compression of c2
            let standalone = zstd::bulk::compress(c2, level).unwrap();

            // Dictionary compression of c2 using c1 as the dictionary
            let mut compressor = zstd::bulk::Compressor::with_dictionary(level, c1).unwrap();
            let dict_compressed = compressor.compress(c2).unwrap_or(standalone.clone());

            standalone_sizes.push(standalone.len());
            dict_sizes.push(dict_compressed.len());
        }
    }

    println!("\n=== Delta Compression Report ===");
    println!("  Chunk size:      {} KB", chunk_size / 1024);
    println!("  Total pairs:     {}", total_pairs);
    println!("  Exact matches:   {} ({:.1}%)", exact_matches, 100.0 * exact_matches as f64 / total_pairs.max(1) as f64);
    println!("  Differing pairs: {} ({:.1}%)", diff_pairs, 100.0 * diff_pairs as f64 / total_pairs.max(1) as f64);

    if !standalone_sizes.is_empty() {
        let n = standalone_sizes.len() as f64;
        let raw: usize = diff_pairs * chunk_size;
        let standalone_total: usize = standalone_sizes.iter().sum();
        let dict_total: usize = dict_sizes.iter().sum();

        println!("\n  For the {} differing pairs:", diff_pairs);
        println!("    Raw size:              {:.1} MB", raw as f64 / (1024.0 * 1024.0));
        println!("    Standalone compressed: {:.1} MB ({:.1}x)", standalone_total as f64 / (1024.0 * 1024.0), raw as f64 / standalone_total as f64);
        println!("    Dict compressed:       {:.1} MB ({:.1}x)", dict_total as f64 / (1024.0 * 1024.0), raw as f64 / dict_total as f64);
        println!("    Delta benefit:         {:.1} MB saved ({:.1}%)",
            (standalone_total as f64 - dict_total as f64) / (1024.0 * 1024.0),
            100.0 * (standalone_total as f64 - dict_total as f64) / standalone_total as f64);

        // Distribution of savings per chunk
        println!("\n  Per-chunk delta benefit distribution:");
        let mut buckets = [0usize; 5]; // 0-20%, 20-40%, 40-60%, 60-80%, 80-100% size reduction
        for (s, d) in standalone_sizes.iter().zip(dict_sizes.iter()) {
            let saving_pct = if *s > 0 {
                100.0 * (1.0 - *d as f64 / *s as f64)
            } else {
                0.0
            };
            let bucket = ((saving_pct / 20.0) as usize).min(4);
            buckets[bucket] += 1;
        }
        let labels = ["dict worse/same (0-20%)", "modest (20-40%)", "good (40-60%)", "great (60-80%)", "excellent (80-100%)"];
        for (i, &count) in buckets.iter().enumerate() {
            let pct = 100.0 * count as f64 / n;
            let bar: String = "#".repeat((pct / 2.0) as usize);
            println!("    {:30} {:>6} ({:>5.1}%)  {}", labels[i], count, pct, bar);
        }
    }

    Ok(())
}
