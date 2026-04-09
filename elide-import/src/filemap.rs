// Generate a snapshot filemap from an ext4 image.
//
// Walks the ext4 directory tree, hashes every regular file with blake3,
// and writes a `snapshots/<ulid>.filemap` mapping path → (hash, byte_count).
// The coordinator uses this at upload time to match files by path across
// parent/child snapshots for delta compression source selection.

use std::io::Write;
use std::path::Path;

use anyhow::Context;
use ext4_view::{DirEntry, Ext4, Metadata, PathBuf as Ext4PathBuf};

/// A single filemap entry: filesystem path, content hash, and byte count.
struct FilemapEntry {
    path: String,
    hash: blake3::Hash,
    byte_count: u64,
}

/// Walk an ext4 image and collect (path, hash, byte_count) for every regular file.
fn enumerate_files(image: &Path) -> anyhow::Result<Vec<FilemapEntry>> {
    let fs =
        Ext4::load_from_path(image).map_err(|e| anyhow::anyhow!("failed to load ext4: {e}"))?;
    let mut entries = Vec::new();
    let mut queue: Vec<Ext4PathBuf> = vec![Ext4PathBuf::new("/")];

    while let Some(dir) = queue.pop() {
        let dir_entries = match fs.read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in dir_entries {
            let entry: DirEntry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
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
                let data = match fs.read(&path) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let hash = blake3::hash(&data);
                let byte_count = data.len() as u64;
                let path_str = path.to_str().unwrap_or("").to_string();
                if !path_str.is_empty() {
                    entries.push(FilemapEntry {
                        path: path_str,
                        hash,
                        byte_count,
                    });
                }
            }
        }
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

/// Generate a filemap for the snapshot in `vol_dir` from the given ext4 image.
///
/// Finds the snapshot ULID in `snapshots/`, walks the ext4 filesystem,
/// and writes `snapshots/<ulid>.filemap`.
pub fn generate(ext4_path: &Path, vol_dir: &Path) -> anyhow::Result<()> {
    let snap_dir = vol_dir.join("snapshots");
    let snap_ulid = find_snapshot_ulid(&snap_dir)?;

    let entries = enumerate_files(ext4_path)?;

    let filemap_path = snap_dir.join(format!("{snap_ulid}.filemap"));
    let mut out = std::fs::File::create(&filemap_path)
        .with_context(|| format!("create {}", filemap_path.display()))?;

    writeln!(out, "# elide-filemap v1")?;
    for entry in &entries {
        writeln!(
            out,
            "{}\t{}\t{}",
            entry.path,
            entry.hash.to_hex(),
            entry.byte_count
        )?;
    }
    out.flush()?;

    eprintln!(
        "Filemap: {} files written to {}",
        entries.len(),
        filemap_path.display()
    );
    Ok(())
}

/// Find the single snapshot ULID in `snap_dir`.
fn find_snapshot_ulid(snap_dir: &Path) -> anyhow::Result<String> {
    let mut ulid = None;
    for entry in std::fs::read_dir(snap_dir).context("read snapshots dir")? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().unwrap_or("");
        // Skip filemap files and anything with an extension.
        if name.contains('.') {
            continue;
        }
        if ulid::Ulid::from_string(name).is_ok() {
            ulid = Some(name.to_string());
        }
    }
    ulid.ok_or_else(|| anyhow::anyhow!("no snapshot ULID found in snapshots/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_snapshot_ulid_skips_filemaps() {
        let tmp = tempfile::TempDir::new().unwrap();
        let snap_dir = tmp.path();
        let ulid = ulid::Ulid::new().to_string();
        std::fs::write(snap_dir.join(&ulid), "").unwrap();
        std::fs::write(snap_dir.join(format!("{ulid}.filemap")), "").unwrap();

        let found = find_snapshot_ulid(snap_dir).unwrap();
        assert_eq!(found, ulid);
    }

    #[test]
    fn find_snapshot_ulid_errors_when_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(find_snapshot_ulid(tmp.path()).is_err());
    }
}
