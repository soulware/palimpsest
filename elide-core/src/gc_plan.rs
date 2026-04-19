//! Coordinator→volume GC handoff plan.
//!
//! The coordinator classifies each input-segment entry during `collect_stats`
//! but no longer fetches bodies or writes the compacted segment. Instead it
//! emits a plaintext tab-separated plan file at `gc/<new_ulid>.plan`. The
//! volume (idle tick) reads the plan, resolves bodies through its own
//! `BlockReader` (ancestor-aware, fetcher-aware, S3-aware), assembles the
//! output segment, signs it with the volume key, and renames it into place
//! as a bare `gc/<new_ulid>`.
//!
//! # Wire format
//!
//! One record per line, tab-separated. The first non-blank / non-comment
//! line is a version tag (`v1`). Each record translates **directly** to a
//! single output entry in the new segment:
//!
//! ```text
//! v1
//! keep        <input-ulid>  <entry-idx>
//! zero_split  <input-ulid>  <entry-idx>  <start-lba>  <lba-length>
//! canonical   <input-ulid>  <entry-idx>
//! run         <input-ulid>  <entry-idx>  <payload-block-offset>  <start-lba>  <lba-length>
//! ```
//!
//! - `keep` — pass through the input entry unchanged (fully-alive
//!   Data / Inline / DedupRef / Zero / Delta).
//! - `zero_split` — emit a fresh Zero at `(start_lba, lba_length)`,
//!   discarding the input entry's original span (used when a multi-LBA
//!   Zero is split into surviving sub-runs).
//! - `canonical` — demote the input entry to `CanonicalData` or
//!   `CanonicalInline`. Used for fully-LBA-dead-hash-live entries **and**
//!   to preserve the composite body of a partial-death entry when the hash
//!   has external references.
//! - `run` — emit a fresh Data entry for one surviving sub-run.
//!   `payload_block_offset` indexes into the composite body (resolved from
//!   the input entry) in 4 KiB units.
//!
//! A partial-death entry with N sub-runs and an external-reference canonical
//! is encoded as one `canonical` + N `run` lines:
//!
//! ```text
//! canonical   01HAAAA...  3
//! run         01HAAAA...  3  0  100  2
//! run         01HAAAA...  3  3  103  1
//! ```
//!
//! Inputs are the set of `<input-ulid>` ULIDs referenced by any record —
//! [`GcPlan::inputs`] recovers them as a sorted, deduplicated list.
//!
//! Removals (hash-level extent-index evictions) are not listed in the plan:
//! the volume derives them at apply time by diffing each input's `.idx`
//! against the plan's output entries, the same way the self-describing
//! handoff already derives extent-index updates.

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use ulid::Ulid;

/// One record in the plan. Most variants encode one output instruction —
/// the materialise side walks the plan in order and translates each record
/// into a single [`crate::segment::SegmentEntry`]. [`PlanOutput::Drop`] is
/// the exception: it contributes no output entry but marks its `input` as a
/// consumed input so the apply path evicts the input's hashes from the
/// extent index (used for fully-dead inputs in tombstone / dead pre-pass).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanOutput {
    /// Pass an input entry through unchanged.
    ///
    /// Covers fully-alive Data / Inline / DedupRef / Zero / Delta. For
    /// Delta the materialise side also carries the entry's delta blob
    /// across into the output segment's delta body section.
    Keep { input: Ulid, entry_idx: u32 },

    /// Emit a fresh `Zero` entry at `(start_lba, lba_length)`, discarding
    /// the input entry's original span.
    ZeroSplit {
        input: Ulid,
        entry_idx: u32,
        start_lba: u64,
        lba_length: u32,
    },

    /// Demote the input entry to `CanonicalData` or `CanonicalInline`.
    /// Used both for fully-LBA-dead-hash-live entries and to preserve the
    /// composite body of a partial-death entry whose hash is externally
    /// referenced.
    Canonical { input: Ulid, entry_idx: u32 },

    /// Emit a fresh `Data` entry for one surviving sub-run of a
    /// partial-death entry. The volume resolves the composite body from
    /// `(input, entry_idx)`, slices `lba_length` blocks starting at
    /// `payload_block_offset * 4096`, and hashes the slice for the new
    /// entry.
    Run {
        input: Ulid,
        entry_idx: u32,
        payload_block_offset: u32,
        start_lba: u64,
        lba_length: u32,
    },

    /// Mark `input` as a consumed input without emitting any output. The
    /// apply path walks this input's `.idx` and evicts every body-owning
    /// hash from the extent index (subject to stale-liveness). Used by
    /// the coordinator for fully-dead inputs in the dead pre-pass and for
    /// mixed batches where one input has no live entries.
    Drop { input: Ulid },
}

/// Full plan for one compacted segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlan {
    pub new_ulid: Ulid,
    pub outputs: Vec<PlanOutput>,
}

const VERSION_TAG: &str = "v1";

impl PlanOutput {
    /// The input ULID this record references.
    pub fn input(&self) -> Ulid {
        match self {
            Self::Keep { input, .. }
            | Self::ZeroSplit { input, .. }
            | Self::Canonical { input, .. }
            | Self::Run { input, .. }
            | Self::Drop { input } => *input,
        }
    }
}

impl GcPlan {
    /// Sorted, deduplicated list of input ULIDs referenced by this plan's
    /// records.
    pub fn inputs(&self) -> Vec<Ulid> {
        let mut set: BTreeSet<Ulid> = BTreeSet::new();
        for out in &self.outputs {
            set.insert(out.input());
        }
        set.into_iter().collect()
    }

    /// Serialise this plan to a writer. Writes a trailing newline after the
    /// last record.
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        writeln!(w, "{VERSION_TAG}")?;
        for out in &self.outputs {
            match out {
                PlanOutput::Keep { input, entry_idx } => {
                    writeln!(w, "keep\t{input}\t{entry_idx}")?;
                }
                PlanOutput::ZeroSplit {
                    input,
                    entry_idx,
                    start_lba,
                    lba_length,
                } => {
                    writeln!(
                        w,
                        "zero_split\t{input}\t{entry_idx}\t{start_lba}\t{lba_length}"
                    )?;
                }
                PlanOutput::Canonical { input, entry_idx } => {
                    writeln!(w, "canonical\t{input}\t{entry_idx}")?;
                }
                PlanOutput::Run {
                    input,
                    entry_idx,
                    payload_block_offset,
                    start_lba,
                    lba_length,
                } => {
                    writeln!(
                        w,
                        "run\t{input}\t{entry_idx}\t{payload_block_offset}\t{start_lba}\t{lba_length}"
                    )?;
                }
                PlanOutput::Drop { input } => {
                    writeln!(w, "drop\t{input}")?;
                }
            }
        }
        Ok(())
    }

    /// Write this plan to `path` atomically via tmp + rename. The tmp file is
    /// `<path>.tmp` (sibling in the same directory for an atomic rename).
    pub fn write_atomic(&self, path: &Path) -> io::Result<()> {
        let tmp = {
            let mut s = path.as_os_str().to_owned();
            s.push(".tmp");
            std::path::PathBuf::from(s)
        };
        {
            let mut f = fs::File::create(&tmp)?;
            self.write_to(&mut f)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Parse a plan from a reader. `new_ulid` comes from the filename — the
    /// plan body itself does not carry it.
    pub fn read_from<R: BufRead>(new_ulid: Ulid, r: R) -> io::Result<Self> {
        let mut lines = r.lines();
        let first =
            next_meaningful_line(&mut lines)?.ok_or_else(|| io::Error::other("gc plan: empty"))?;
        if first != VERSION_TAG {
            return Err(io::Error::other(format!(
                "gc plan: unknown version tag {first:?}"
            )));
        }

        let mut outputs: Vec<PlanOutput> = Vec::new();
        while let Some(line) = next_meaningful_line(&mut lines)? {
            let mut fields = line.split('\t');
            let tag = fields
                .next()
                .ok_or_else(|| io::Error::other("gc plan: empty record"))?;
            match tag {
                "keep" => {
                    let input = parse_ulid(fields.next(), "keep.input")?;
                    let entry_idx = parse_u32(fields.next(), "keep.entry_idx")?;
                    require_end_of_fields(&mut fields, "keep")?;
                    outputs.push(PlanOutput::Keep { input, entry_idx });
                }
                "zero_split" => {
                    let input = parse_ulid(fields.next(), "zero_split.input")?;
                    let entry_idx = parse_u32(fields.next(), "zero_split.entry_idx")?;
                    let start_lba = parse_u64(fields.next(), "zero_split.start_lba")?;
                    let lba_length = parse_u32(fields.next(), "zero_split.lba_length")?;
                    require_end_of_fields(&mut fields, "zero_split")?;
                    outputs.push(PlanOutput::ZeroSplit {
                        input,
                        entry_idx,
                        start_lba,
                        lba_length,
                    });
                }
                "canonical" => {
                    let input = parse_ulid(fields.next(), "canonical.input")?;
                    let entry_idx = parse_u32(fields.next(), "canonical.entry_idx")?;
                    require_end_of_fields(&mut fields, "canonical")?;
                    outputs.push(PlanOutput::Canonical { input, entry_idx });
                }
                "run" => {
                    let input = parse_ulid(fields.next(), "run.input")?;
                    let entry_idx = parse_u32(fields.next(), "run.entry_idx")?;
                    let payload_block_offset =
                        parse_u32(fields.next(), "run.payload_block_offset")?;
                    let start_lba = parse_u64(fields.next(), "run.start_lba")?;
                    let lba_length = parse_u32(fields.next(), "run.lba_length")?;
                    require_end_of_fields(&mut fields, "run")?;
                    outputs.push(PlanOutput::Run {
                        input,
                        entry_idx,
                        payload_block_offset,
                        start_lba,
                        lba_length,
                    });
                }
                "drop" => {
                    let input = parse_ulid(fields.next(), "drop.input")?;
                    require_end_of_fields(&mut fields, "drop")?;
                    outputs.push(PlanOutput::Drop { input });
                }
                other => {
                    return Err(io::Error::other(format!(
                        "gc plan: unknown record tag {other:?}"
                    )));
                }
            }
        }

        Ok(GcPlan { new_ulid, outputs })
    }

    /// Read a plan from `path`. The filename stem must be a valid ULID —
    /// that's the plan's `new_ulid`.
    pub fn read(path: &Path) -> io::Result<Self> {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::other("gc plan: path has no file stem"))?;
        let new_ulid = Ulid::from_string(stem)
            .map_err(|e| io::Error::other(format!("gc plan: filename is not a ULID: {e}")))?;
        let f = fs::File::open(path)?;
        Self::read_from(new_ulid, BufReader::new(f))
    }
}

fn next_meaningful_line<I>(lines: &mut I) -> io::Result<Option<String>>
where
    I: Iterator<Item = io::Result<String>>,
{
    for line in lines.by_ref() {
        let line = line?;
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Ok(Some(trimmed.to_owned()));
    }
    Ok(None)
}

fn parse_ulid(field: Option<&str>, what: &str) -> io::Result<Ulid> {
    let raw = field.ok_or_else(|| io::Error::other(format!("gc plan: missing {what}")))?;
    Ulid::from_string(raw)
        .map_err(|e| io::Error::other(format!("gc plan: invalid {what} {raw:?}: {e}")))
}

fn parse_u64(field: Option<&str>, what: &str) -> io::Result<u64> {
    let raw = field.ok_or_else(|| io::Error::other(format!("gc plan: missing {what}")))?;
    raw.parse::<u64>()
        .map_err(|e| io::Error::other(format!("gc plan: invalid {what} {raw:?}: {e}")))
}

fn parse_u32(field: Option<&str>, what: &str) -> io::Result<u32> {
    let raw = field.ok_or_else(|| io::Error::other(format!("gc plan: missing {what}")))?;
    raw.parse::<u32>()
        .map_err(|e| io::Error::other(format!("gc plan: invalid {what} {raw:?}: {e}")))
}

fn require_end_of_fields<'a, I>(fields: &mut I, what: &str) -> io::Result<()>
where
    I: Iterator<Item = &'a str>,
{
    if let Some(extra) = fields.next() {
        return Err(io::Error::other(format!(
            "gc plan: extra field in {what} record: {extra:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ulid(s: &str) -> Ulid {
        Ulid::from_string(s).unwrap()
    }

    #[test]
    fn round_trip_minimal_plan() {
        let new = ulid("01HZZZZZZZZZZZZZZZZZZZZZZZ");
        let plan = GcPlan {
            new_ulid: new,
            outputs: vec![PlanOutput::Keep {
                input: ulid("01HYYYYYYYYYYYYYYYYYYYYYYY"),
                entry_idx: 3,
            }],
        };
        let mut buf = Vec::new();
        plan.write_to(&mut buf).unwrap();
        let parsed = GcPlan::read_from(new, &buf[..]).unwrap();
        assert_eq!(plan, parsed);
    }

    #[test]
    fn round_trip_every_variant() {
        let a = ulid("01HAAAAAAAAAAAAAAAAAAAAAAA");
        let b = ulid("01HBBBBBBBBBBBBBBBBBBBBBBB");
        let new = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let plan = GcPlan {
            new_ulid: new,
            outputs: vec![
                PlanOutput::Keep {
                    input: a,
                    entry_idx: 0,
                },
                PlanOutput::ZeroSplit {
                    input: a,
                    entry_idx: 1,
                    start_lba: 100,
                    lba_length: 10,
                },
                PlanOutput::Canonical {
                    input: b,
                    entry_idx: 2,
                },
                PlanOutput::Run {
                    input: b,
                    entry_idx: 3,
                    payload_block_offset: 0,
                    start_lba: 200,
                    lba_length: 4,
                },
                PlanOutput::Run {
                    input: b,
                    entry_idx: 3,
                    payload_block_offset: 20,
                    start_lba: 220,
                    lba_length: 4,
                },
            ],
        };
        let mut buf = Vec::new();
        plan.write_to(&mut buf).unwrap();
        let parsed = GcPlan::read_from(new, &buf[..]).unwrap();
        assert_eq!(plan, parsed);
    }

    #[test]
    fn inputs_derives_sorted_dedup_set() {
        let a = ulid("01HAAAAAAAAAAAAAAAAAAAAAAA");
        let b = ulid("01HBBBBBBBBBBBBBBBBBBBBBBB");
        let c = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let new = ulid("01HZZZZZZZZZZZZZZZZZZZZZZZ");
        let plan = GcPlan {
            new_ulid: new,
            outputs: vec![
                PlanOutput::Keep {
                    input: c,
                    entry_idx: 0,
                },
                PlanOutput::Keep {
                    input: a,
                    entry_idx: 0,
                },
                PlanOutput::Run {
                    input: b,
                    entry_idx: 1,
                    payload_block_offset: 0,
                    start_lba: 0,
                    lba_length: 1,
                },
                PlanOutput::Canonical {
                    input: a,
                    entry_idx: 2,
                },
            ],
        };
        assert_eq!(plan.inputs(), vec![a, b, c]);
    }

    #[test]
    fn ignores_blank_and_comment_lines() {
        let new = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let bytes =
            b"v1\n\n# this is a comment\nkeep\t01HAAAAAAAAAAAAAAAAAAAAAAA\t0\n\n# another\n";
        let plan = GcPlan::read_from(new, &bytes[..]).unwrap();
        assert_eq!(plan.outputs.len(), 1);
        assert_eq!(plan.inputs(), vec![ulid("01HAAAAAAAAAAAAAAAAAAAAAAA")]);
    }

    #[test]
    fn rejects_unknown_version() {
        let new = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let bytes = b"v2\n";
        let err = GcPlan::read_from(new, &bytes[..]).unwrap_err();
        assert!(err.to_string().contains("unknown version"));
    }

    #[test]
    fn rejects_unknown_record() {
        let new = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let bytes = b"v1\nsomething_else\t01HAAAAAAAAAAAAAAAAAAAAAAA\t0\n";
        let err = GcPlan::read_from(new, &bytes[..]).unwrap_err();
        assert!(err.to_string().contains("unknown record"));
    }

    #[test]
    fn rejects_extra_field() {
        let new = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let bytes = b"v1\nkeep\t01HAAAAAAAAAAAAAAAAAAAAAAA\t0\textra\n";
        let err = GcPlan::read_from(new, &bytes[..]).unwrap_err();
        assert!(err.to_string().contains("extra field"));
    }

    #[test]
    fn round_trip_through_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let new = ulid("01HCCCCCCCCCCCCCCCCCCCCCCC");
        let path = dir.path().join(format!("{new}.plan"));
        let plan = GcPlan {
            new_ulid: new,
            outputs: vec![PlanOutput::Keep {
                input: ulid("01HAAAAAAAAAAAAAAAAAAAAAAA"),
                entry_idx: 7,
            }],
        };
        plan.write_atomic(&path).unwrap();
        let parsed = GcPlan::read(&path).unwrap();
        assert_eq!(plan, parsed);
        assert!(!dir.path().join(format!("{new}.plan.tmp")).exists());
    }
}
