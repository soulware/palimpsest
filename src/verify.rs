// Scan every segment in a volume directory and verify each extent's stored
// body hashes to the declared hash in the segment index. Reports mismatches
// (the signature of the corruption pattern described in
// docs/notes/reference.md / project_gc_body_integrity_check.md).
//
// Read-only: does not modify any files. Prints a one-line summary per
// segment and a per-extent "MISMATCH" line for each poisoned body, then
// an overall summary. Exit code: 0 clean, 1 mismatches, 2 scan errors.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use elide_core::segment::{self, EntryKind, SegmentBodyLayout};

pub struct Counts {
    pub segments_scanned: usize,
    pub entries_checked: usize,
    pub entries_skipped_no_body: usize,
    pub entries_skipped_not_present: usize,
    pub mismatches: usize,
    pub scan_errors: usize,
}

pub fn run(dir: &Path) -> io::Result<Counts> {
    let index_dir = dir.join("index");
    let mut idx_files: Vec<PathBuf> = match fs::read_dir(&index_dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("idx"))
            .collect(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e),
    };
    idx_files.sort();

    let mut counts = Counts {
        segments_scanned: 0,
        entries_checked: 0,
        entries_skipped_no_body: 0,
        entries_skipped_not_present: 0,
        mismatches: 0,
        scan_errors: 0,
    };

    for idx_path in &idx_files {
        let stem = idx_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_owned();
        let ulid = match ulid::Ulid::from_string(&stem) {
            Ok(u) => u,
            Err(_) => {
                eprintln!("{stem}: unparseable ULID, skipping");
                counts.scan_errors += 1;
                continue;
            }
        };

        let (body_section_start, entries) = match segment::read_segment_index(idx_path) {
            Ok((bss, entries, _inputs)) => (bss, entries),
            Err(e) => {
                eprintln!("{stem}: reading index: {e}");
                counts.scan_errors += 1;
                continue;
            }
        };
        counts.segments_scanned += 1;

        let (body_path, layout) = match segment::locate_segment_body(dir, ulid) {
            Some(v) => v,
            None => {
                // No body locally — every Data/Inline extent is "evicted";
                // count them as skipped.
                let skipped = entries
                    .iter()
                    .filter(|e| matches!(e.kind, EntryKind::Data | EntryKind::Inline))
                    .count();
                counts.entries_skipped_no_body += skipped;
                println!("{stem}: body not local ({skipped} extent(s) skipped)");
                continue;
            }
        };

        let present_path = dir.join("cache").join(format!("{stem}.present"));
        let has_present_file = matches!(layout, SegmentBodyLayout::BodyOnly);

        // Inline section always lives in the .idx file (which the full
        // segment also embeds at the same offset). Read from idx_path so we
        // handle BodyOnly (cache/<id>.body + index/<id>.idx) and FullSegment
        // (pending/wal/gc) uniformly.
        let inline_bytes = match segment::read_inline_section(idx_path) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("{stem}: reading inline section: {e}");
                counts.scan_errors += 1;
                continue;
            }
        };

        let mut seg_mismatches = 0usize;
        let mut seg_checked = 0usize;
        let mut seg_skipped = 0usize;

        let body_file = match fs::File::open(&body_path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{stem}: opening body {}: {e}", body_path.display());
                counts.scan_errors += 1;
                continue;
            }
        };

        for (i, entry) in entries.iter().enumerate() {
            if !matches!(entry.kind, EntryKind::Data | EntryKind::Inline) {
                continue;
            }
            if entry.stored_length == 0 {
                continue;
            }

            // Inline: always present when the index is (lives in the inline
            // section). Data: needs the body section — and when we're
            // reading from a cache/<id>.body, must consult the .present
            // bitmap to avoid hashing sparse zero regions.
            let body_bytes = match entry.kind {
                EntryKind::Inline => {
                    let start = entry.stored_offset as usize;
                    let end = start + entry.stored_length as usize;
                    if end > inline_bytes.len() {
                        eprintln!(
                            "{stem} entry={i}: inline slice {start}..{end} exceeds section ({})",
                            inline_bytes.len()
                        );
                        counts.scan_errors += 1;
                        continue;
                    }
                    inline_bytes[start..end].to_vec()
                }
                EntryKind::Data => {
                    if has_present_file {
                        match segment::check_present_bit(&present_path, i as u32) {
                            Ok(true) => {}
                            Ok(false) => {
                                seg_skipped += 1;
                                continue;
                            }
                            Err(e) => {
                                eprintln!("{stem} entry={i}: checking .present: {e}");
                                counts.scan_errors += 1;
                                continue;
                            }
                        }
                    }

                    use std::io::{Read, Seek, SeekFrom};
                    let abs =
                        layout.body_section_file_offset(body_section_start) + entry.stored_offset;
                    let mut f = &body_file;
                    if let Err(e) = f.seek(SeekFrom::Start(abs)) {
                        eprintln!("{stem} entry={i}: seek {abs}: {e}");
                        counts.scan_errors += 1;
                        continue;
                    }
                    let mut buf = vec![0u8; entry.stored_length as usize];
                    if let Err(e) = f.read_exact(&mut buf) {
                        eprintln!("{stem} entry={i}: read {} bytes: {e}", buf.len());
                        counts.scan_errors += 1;
                        continue;
                    }
                    buf
                }
                _ => unreachable!(),
            };

            match segment::verify_body_hash(entry, &body_bytes) {
                Ok(()) => seg_checked += 1,
                Err(e) => {
                    seg_mismatches += 1;
                    println!(
                        "MISMATCH {stem} entry={i} lba={} len={}B kind={:?}: {e}",
                        entry.start_lba, entry.stored_length, entry.kind,
                    );
                }
            }
        }

        counts.entries_checked += seg_checked;
        counts.mismatches += seg_mismatches;
        counts.entries_skipped_not_present += seg_skipped;

        if seg_mismatches == 0 {
            println!(
                "{stem}: ok ({} checked, {} skipped)",
                seg_checked, seg_skipped
            );
        } else {
            println!(
                "{stem}: {} MISMATCH ({} checked, {} skipped)",
                seg_mismatches, seg_checked, seg_skipped
            );
        }
    }

    println!();
    println!(
        "scanned {} segment(s): {} extents ok, {} MISMATCH, {} skipped (not-present), {} skipped (no local body), {} errors",
        counts.segments_scanned,
        counts.entries_checked,
        counts.mismatches,
        counts.entries_skipped_not_present,
        counts.entries_skipped_no_body,
        counts.scan_errors,
    );

    Ok(counts)
}
