// Property-based tests for `cache/<ULID>.dmat` framing recovery.
//
// Generates a sequence of (entry_idx, payload) records, appends them all,
// then truncates the file at an arbitrary offset to simulate a torn write.
// Asserts:
//
// 1. Reopening recovers exactly the records that were fully durable before
//    the truncation point.
// 2. The reopened file is truncated to the end of the last fully-durable
//    record (or to the magic boundary if none survived).
// 3. Subsequent appends extend the file cleanly past the truncation.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use elide_core::dmat::{self, Dmat, MAGIC};
use proptest::prelude::*;
use ulid::Ulid;

const RECORD_HEADER_LEN: u64 = 4 + 1 + 4;

/// Output bytes for `Dmat::append`'s record header + payload, given the
/// arguments passed in.
fn record_byte_size(payload_len: usize) -> u64 {
    RECORD_HEADER_LEN + payload_len as u64
}

fn tmp_dir() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "elide-dmat-proptest-{}-{}",
        std::process::id(),
        Ulid::new()
    ));
    fs::create_dir_all(&p).unwrap();
    p
}

fn arb_records() -> impl Strategy<Value = Vec<(u32, Vec<u8>)>> {
    // Small entry_idx domain so duplicates can occur (last-write-wins).
    // Payload sizes from 0..256 keep the test fast but still exercise the
    // entropy gate near the small-payload boundary.
    prop::collection::vec(
        (0u32..32, prop::collection::vec(any::<u8>(), 0..256)),
        0..32,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// Append a generated sequence, truncate the file at an arbitrary
    /// offset, reopen, and verify the recovered map matches the records
    /// that were fully durable before the truncation.
    #[test]
    fn truncation_recovery_matches_durable_prefix(
        records in arb_records(),
        // truncation_offset will be modulo'd into `[8, file_len]` below.
        truncation_seed in 0u64..1_000_000,
    ) {
        let dir = tmp_dir();
        let path = dir.join("seg.dmat");

        // Append every record, recording the absolute end-offset of each so
        // we can compute which were fully durable for any truncation point.
        let mut record_ends: Vec<(u32, u64)> = Vec::new();
        let mut last_value_for: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut cursor = MAGIC.len() as u64;
        {
            let (mut d, _stats) = Dmat::open_or_create(&path, |_, _| true).unwrap();
            for (idx, payload) in &records {
                d.append(*idx, payload, None).unwrap();
                cursor += record_byte_size(payload.len());
                record_ends.push((*idx, cursor));
                last_value_for.insert(*idx, payload.clone());
            }
        }
        let durable_len = fs::metadata(&path).unwrap().len();
        prop_assert_eq!(durable_len, cursor);

        // Pick a truncation offset uniformly in [magic_len, durable_len].
        let span = durable_len - MAGIC.len() as u64 + 1;
        let truncation = MAGIC.len() as u64 + (truncation_seed % span);

        // Compute expected post-recovery state: the records whose end <=
        // truncation are durable; for repeated entry_idx the last-such-record
        // wins (matching `Dmat::append`'s overwrite-on-duplicate behaviour).
        let mut expected_present: HashMap<u32, ()> = HashMap::new();
        let mut expected_file_end = MAGIC.len() as u64;
        let mut last_entry_value: HashMap<u32, Vec<u8>> = HashMap::new();
        for (i, (idx, end)) in record_ends.iter().enumerate() {
            if *end <= truncation {
                expected_present.insert(*idx, ());
                expected_file_end = *end;
                last_entry_value.insert(*idx, records[i].1.clone());
            }
        }

        // Truncate.
        let f = fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(truncation).unwrap();

        // Reopen.
        let (mut d, scan) = Dmat::open_or_create(&path, |_, _| true).unwrap();

        // Map contains exactly the durable entry_idxs.
        prop_assert_eq!(d.len(), expected_present.len());
        for idx in expected_present.keys() {
            prop_assert!(d.lookup(*idx).is_some(), "expected entry_idx {} present", idx);
        }

        // File is truncated back to the end of the last durable record.
        let after = fs::metadata(&path).unwrap().len();
        prop_assert_eq!(after, expected_file_end);

        // ScanStats.truncated reflects "the scan had to physically truncate
        // the file" — set iff our truncation landed mid-record. A truncation
        // that happens to land at a record boundary leaves the file at a
        // clean end and the scan reports `truncated = false` (the file size
        // already matches the last durable record).
        let truncation_was_clean = truncation == expected_file_end;
        if !truncation_was_clean {
            prop_assert!(
                scan.truncated,
                "ScanStats.truncated must be set when truncation landed mid-record \
                 (truncation={truncation}, expected_file_end={expected_file_end})"
            );
        }

        // Each surviving entry's bytes match the *latest* durable append for
        // that entry_idx.
        for (idx, expected_bytes) in last_entry_value {
            let loc = d.lookup(idx).unwrap();
            let got = d.read_materialised(loc).unwrap();
            prop_assert_eq!(got, expected_bytes);
        }

        // Subsequent appends extend cleanly past the truncation point.
        let pre_extend = fs::metadata(&path).unwrap().len();
        d.append(99, b"posttrunc", None).unwrap();
        let post_extend = fs::metadata(&path).unwrap().len();
        prop_assert_eq!(post_extend, pre_extend + record_byte_size(b"posttrunc".len()));

        let _ = fs::remove_dir_all(&dir);
    }
}

/// Deterministic regression for the proptest scenario above: a torn-mid-record
/// truncation must drop the partially-written record entirely, leaving the
/// preceding record intact.
///
/// This is the named deterministic counterpart per the proptest convention —
/// it documents the bug class and is fast.
#[test]
fn deterministic_truncation_in_middle_of_second_record_drops_only_second() {
    let dir = tmp_dir();
    let path = dir.join("seg.dmat");

    let (mut d, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
    d.append(7, b"alpha", None).unwrap();
    d.append(11, b"betalonger", None).unwrap();
    drop(d);

    let len = fs::metadata(&path).unwrap().len();
    // Truncate 3 bytes into the second record's body.
    let first_record_end = MAGIC.len() as u64 + RECORD_HEADER_LEN + b"alpha".len() as u64;
    let mid = first_record_end + RECORD_HEADER_LEN + 3;
    assert!(mid < len);
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(mid)
        .unwrap();

    let (d, scan) = Dmat::open_or_create(&path, |_, _| true).unwrap();
    assert!(scan.truncated);
    assert_eq!(scan.accepted, 1);
    assert_eq!(d.len(), 1);
    assert!(d.lookup(7).is_some());
    assert!(d.lookup(11).is_none());

    let after = fs::metadata(&path).unwrap().len();
    assert_eq!(after, first_record_end);
    let _ = fs::remove_dir_all(&dir);
}

/// Deterministic regression: dropping just enough bytes to corrupt only the
/// final record's last byte still drops the whole record.
#[test]
fn deterministic_truncation_one_byte_short_drops_last_record() {
    let dir = tmp_dir();
    let path = dir.join("seg.dmat");

    let (mut d, _) = Dmat::open_or_create(&path, |_, _| true).unwrap();
    d.append(1, b"hello", None).unwrap();
    d.append(2, b"worlds", None).unwrap();
    drop(d);

    let len = fs::metadata(&path).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 1)
        .unwrap();

    let (d, scan) = Dmat::open_or_create(&path, |_, _| true).unwrap();
    assert!(scan.truncated);
    assert_eq!(d.len(), 1);
    assert!(d.lookup(1).is_some());
    assert!(d.lookup(2).is_none());
    let _ = fs::remove_dir_all(&dir);
}

/// `dmat::ScanStats` is exposed through the public surface used by these
/// tests; this is a smoke test that the public re-exports survive.
#[test]
fn scan_stats_default_fields() {
    let s = dmat::ScanStats::default();
    assert_eq!(s.accepted, 0);
    assert_eq!(s.invalid, 0);
    assert!(!s.truncated);
}
