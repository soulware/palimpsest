// Snapshot filemap (`snapshots/<ulid>.filemap`) read/write for format v2.
//
// v2 layout:
//
//   # elide-filemap v2
//   <path>\t<file_offset>\t<blake3-hex>\t<byte_count>
//   ...
//
// One row per file fragment. A contiguous file is a single row with
// file_offset = 0. A fragmented file has multiple rows with ascending
// file_offset. See docs/design-delta-compression.md §"Snapshot filemap".

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

pub const HEADER_V2: &str = "# elide-filemap v2";

#[derive(Clone, Debug)]
pub struct FilemapRow {
    pub path: String,
    pub file_offset: u64,
    pub hash: blake3::Hash,
    pub byte_count: u64,
}

/// A parsed filemap grouped by path. Per-path fragment lists are sorted by
/// `file_offset` ascending.
#[derive(Debug, Default)]
pub struct Filemap {
    by_path: BTreeMap<String, Vec<FilemapRow>>,
}

impl Filemap {
    pub fn fragments(&self, path: &str) -> Option<&[FilemapRow]> {
        self.by_path.get(path).map(|v| v.as_slice())
    }

    pub fn paths(&self) -> impl Iterator<Item = &str> {
        self.by_path.keys().map(|s| s.as_str())
    }

    pub fn is_empty(&self) -> bool {
        self.by_path.is_empty()
    }
}

/// Parse `snapshots/<ulid>.filemap` from disk.
pub fn read(path: &Path) -> io::Result<Filemap> {
    let text = fs::read_to_string(path)?;
    parse(&text)
}

/// Parse filemap v2 text. Rejects any other version or malformed input.
pub fn parse(text: &str) -> io::Result<Filemap> {
    let mut lines = text.lines();
    let header = lines
        .next()
        .ok_or_else(|| io::Error::other("filemap is empty"))?;
    if header.trim() != HEADER_V2 {
        return Err(io::Error::other(format!(
            "unexpected filemap header {header:?}, want {HEADER_V2:?}"
        )));
    }

    let mut by_path: BTreeMap<String, Vec<FilemapRow>> = BTreeMap::new();
    for (lineno, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut cols = line.split('\t');
        let path = cols
            .next()
            .ok_or_else(|| io::Error::other(format!("filemap line {lineno}: missing path")))?;
        let file_offset: u64 = cols
            .next()
            .ok_or_else(|| io::Error::other(format!("filemap line {lineno}: missing file_offset")))?
            .parse()
            .map_err(|e| {
                io::Error::other(format!("filemap line {lineno}: bad file_offset: {e}"))
            })?;
        let hash_hex = cols
            .next()
            .ok_or_else(|| io::Error::other(format!("filemap line {lineno}: missing hash")))?;
        let hash = blake3::Hash::from_hex(hash_hex)
            .map_err(|e| io::Error::other(format!("filemap line {lineno}: bad hash: {e}")))?;
        let byte_count: u64 = cols
            .next()
            .ok_or_else(|| io::Error::other(format!("filemap line {lineno}: missing byte_count")))?
            .parse()
            .map_err(|e| io::Error::other(format!("filemap line {lineno}: bad byte_count: {e}")))?;

        by_path
            .entry(path.to_owned())
            .or_default()
            .push(FilemapRow {
                path: path.to_owned(),
                file_offset,
                hash,
                byte_count,
            });
    }

    for rows in by_path.values_mut() {
        rows.sort_by_key(|r| r.file_offset);
    }

    Ok(Filemap { by_path })
}

/// Write `rows` to `snapshots/<ulid>.filemap` in v2 format. Rows are sorted
/// by `(path, file_offset)` so two imports of similar images produce
/// byte-identical filemaps modulo content.
pub fn write(snapshots_dir: &Path, snap_ulid: &str, rows: &[FilemapRow]) -> io::Result<()> {
    let mut sorted: Vec<&FilemapRow> = rows.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path).then(a.file_offset.cmp(&b.file_offset)));

    let path = snapshots_dir.join(format!("{snap_ulid}.filemap"));
    let mut out = fs::File::create(&path)?;
    writeln!(out, "{HEADER_V2}")?;
    for row in sorted {
        writeln!(
            out,
            "{}\t{}\t{}\t{}",
            row.path,
            row.file_offset,
            row.hash.to_hex(),
            row.byte_count
        )?;
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_single_fragment() {
        let tmp = tempfile::tempdir().unwrap();
        let snap = "01JQAAAAAAAAAAAAAAAAAAAAAA";
        let h = blake3::hash(b"file-content");
        let rows = vec![FilemapRow {
            path: "/etc/hosts".into(),
            file_offset: 0,
            hash: h,
            byte_count: 128,
        }];
        write(tmp.path(), snap, &rows).unwrap();

        let map = read(&tmp.path().join(format!("{snap}.filemap"))).unwrap();
        let got = map.fragments("/etc/hosts").unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].file_offset, 0);
        assert_eq!(got[0].hash, h);
        assert_eq!(got[0].byte_count, 128);
    }

    #[test]
    fn parse_groups_fragments_by_path() {
        let h1 = blake3::hash(b"a");
        let h2 = blake3::hash(b"b");
        let text = format!(
            "{HEADER_V2}\n/foo\t0\t{}\t4096\n/foo\t4096\t{}\t4096\n/bar\t0\t{}\t10\n",
            h1.to_hex(),
            h2.to_hex(),
            h1.to_hex(),
        );
        let map = parse(&text).unwrap();
        let foo = map.fragments("/foo").unwrap();
        assert_eq!(foo.len(), 2);
        assert_eq!(foo[0].file_offset, 0);
        assert_eq!(foo[1].file_offset, 4096);
        assert_eq!(map.fragments("/bar").unwrap().len(), 1);
    }

    #[test]
    fn rejects_wrong_header() {
        let err = parse("# elide-filemap v1\n").unwrap_err();
        assert!(err.to_string().contains("unexpected filemap header"));
    }
}
