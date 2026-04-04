// Human-readable inspection of elide binary file formats.
//
// inspect-segment <path>
//   Prints the header and index entries of a segment file or a cached .idx
//   file. Works on both: full segments (pending/, segments/) and index-only
//   files (cache/*.idx). Flags each data entry as OK or OVERFLOW relative
//   to the body file on disk (if present).
//
// inspect-wal <path>
//   Prints every record in a WAL file (wal/<ulid>). Uses scan_readonly so
//   the file is never modified.

use std::path::Path;

use elide_core::segment;
use elide_core::writelog;

// --- inspect-segment ---

pub fn inspect_segment(path: &Path) -> std::io::Result<()> {
    let (body_section_start, entries) = segment::read_segment_index(path)?;

    // Detect whether this is a full segment or an index-only .idx file.
    let file_size = std::fs::metadata(path)?.len();
    let is_idx_only = file_size == body_section_start;

    // If it's a full segment, body bytes follow immediately; body_size is
    // derivable from file_size. For .idx files, look for a sibling .body
    // file to get the actual body size for the overflow check.
    let body_size: Option<u64> = if is_idx_only {
        // Look for <stem>.body next to the .idx file.
        let body_path = path.with_extension("body");
        body_path
            .exists()
            .then(|| std::fs::metadata(&body_path).ok().map(|m| m.len()))
            .flatten()
    } else {
        Some(file_size - body_section_start)
    };

    let data_count = entries
        .iter()
        .filter(|e| !e.is_dedup_ref && !e.is_inline)
        .count();
    let dedup_count = entries.iter().filter(|e| e.is_dedup_ref).count();
    let inline_count = entries.iter().filter(|e| e.is_inline).count();

    println!("file:               {}", path.display());
    println!(
        "kind:               {}",
        if is_idx_only {
            "index-only (.idx)"
        } else {
            "full segment"
        }
    );
    println!("entry_count:        {}", entries.len());
    println!("body_section_start: {body_section_start}");
    println!(
        "body_size:          {}",
        match body_size {
            Some(n) => n.to_string(),
            None => "(no .body file)".to_string(),
        }
    );
    println!(
        "entries:            {data_count} data, {dedup_count} dedup_ref{}",
        if inline_count > 0 {
            format!(", {inline_count} inline")
        } else {
            String::new()
        }
    );

    let data_entries: Vec<_> = entries
        .iter()
        .filter(|e| !e.is_dedup_ref && !e.is_inline)
        .collect();
    if data_entries.is_empty() {
        return Ok(());
    }

    // Sort by stored_offset for a clear body layout view.
    let mut sorted = data_entries.clone();
    sorted.sort_by_key(|e| e.stored_offset);

    let max_end = sorted
        .last()
        .map(|e| e.stored_offset + e.stored_length as u64)
        .unwrap_or(0);
    let overflow_count = body_size
        .map(|bs| {
            sorted
                .iter()
                .filter(|e| e.stored_offset + e.stored_length as u64 > bs)
                .count()
        })
        .unwrap_or(0);

    println!();
    println!(
        "{:<6}  {:<14}  {:>10}  {:>8}  {:<4}  status",
        "type", "lba_range", "body_off", "len", "comp"
    );
    println!("{}", "-".repeat(65));

    for e in &sorted {
        let end = e.stored_offset + e.stored_length as u64;
        let status = match body_size {
            Some(bs) if end > bs => "OVERFLOW",
            _ => "ok",
        };
        println!(
            "{:<6}  {:<14}  {:>10}  {:>8}  {:<4}  {}",
            "data",
            format!("[{}+{})", e.start_lba, e.lba_length),
            e.stored_offset,
            e.stored_length,
            if e.compressed { "yes" } else { "no" },
            status,
        );
    }

    println!();
    println!(
        "max body used: {max_end}{}",
        body_size.map(|bs| format!(" / {bs}")).unwrap_or_default()
    );
    if overflow_count > 0 {
        println!("WARNING: {overflow_count} entries overflow the body file");
    }

    Ok(())
}

// --- inspect-wal ---

pub fn inspect_wal(path: &Path) -> std::io::Result<()> {
    let (records, truncated) = writelog::scan_readonly(path)?;

    let data_count = records
        .iter()
        .filter(|r| matches!(r, writelog::LogRecord::Data { .. }))
        .count();
    let ref_count = records
        .iter()
        .filter(|r| matches!(r, writelog::LogRecord::Ref { .. }))
        .count();

    println!("file:     {}", path.display());
    println!("records:  {} data, {} dedup_ref", data_count, ref_count);
    if truncated {
        println!("WARNING:  truncated tail record detected (crash recovery may apply)");
    }

    if records.is_empty() {
        println!("(empty)");
        return Ok(());
    }

    println!();
    println!(
        "{:<6}  {:<14}  {:>10}  {:>8}  comp",
        "type", "lba_range", "body_off", "len"
    );
    println!("{}", "-".repeat(55));

    for record in &records {
        match record {
            writelog::LogRecord::Data {
                start_lba,
                lba_length,
                flags,
                body_offset,
                data,
                ..
            } => {
                let compressed = flags.contains(writelog::WalFlags::COMPRESSED);
                println!(
                    "{:<6}  {:<14}  {:>10}  {:>8}  {}",
                    "data",
                    format!("[{}+{})", start_lba, lba_length),
                    body_offset,
                    data.len(),
                    if compressed { "yes" } else { "no" },
                );
            }
            writelog::LogRecord::Ref {
                start_lba,
                lba_length,
                ..
            } => {
                println!(
                    "{:<6}  {:<14}  {:>10}  {:>8}  -",
                    "ref",
                    format!("[{}+{})", start_lba, lba_length),
                    "-",
                    "-",
                );
            }
            writelog::LogRecord::Zero {
                start_lba,
                lba_length,
            } => {
                println!(
                    "{:<6}  {:<14}  {:>10}  {:>8}  -",
                    "zero",
                    format!("[{}+{})", start_lba, lba_length),
                    "-",
                    "-",
                );
            }
        }
    }

    Ok(())
}
