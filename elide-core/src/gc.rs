// GC handoff file format — shared between coordinator (writer) and volume (reader).
//
// Each line in a .pending file has one of three forms:
//
//   repack <hash_hex> <old_ulid> <new_ulid> <new_absolute_offset>
//   remove <hash_hex> <old_ulid>
//   dead   <old_ulid>
//
// `repack`: the extent identified by `hash` was live and has been copied to
// `new_ulid` at `new_absolute_offset`.  The volume must update its extent
// index to point at the new location before the old segment is deleted.
//
// `remove`: the extent identified by `hash` was present in the extent index
// but its LBA has been overwritten, so it is not carried forward.  The
// volume must remove the dangling extent index entry before the old segment
// is deleted.
//
// `dead`: the old segment had no live extents and no extent index references
// at the time the coordinator analysed it.  From the volume's perspective
// this is a no-op acknowledgment — it simply renames .pending → .applied so
// the coordinator knows it is safe to delete.

use std::fmt;

use ulid::Ulid;

/// The lifecycle state of a GC handoff file in `gc/`.
///
/// Files progress through this sequence:
///
///   `gc/<ulid>.pending`  — coordinator staged the compacted segment; volume
///                          has not yet applied the handoff.
///   `gc/<ulid>.applied`  — volume applied the handoff (re-signed segment,
///                          updated extent index); coordinator has not yet
///                          uploaded the new segment to S3 or deleted old ones.
///   `gc/<ulid>.done`     — coordinator completed upload and cleanup; file is
///                          kept for `DONE_FILE_TTL` for post-mortem debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcHandoffState {
    Pending,
    Applied,
    Done,
}

impl GcHandoffState {
    /// The filename suffix for this state (without leading dot).
    pub fn suffix(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Applied => "applied",
            Self::Done => "done",
        }
    }

    fn from_suffix(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "applied" => Some(Self::Applied),
            "done" => Some(Self::Done),
            _ => None,
        }
    }
}

/// A parsed GC handoff filename: a ULID plus its current lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcHandoff {
    pub ulid: Ulid,
    pub state: GcHandoffState,
}

impl GcHandoff {
    /// Parse a gc/ directory entry filename into a `GcHandoff`.
    ///
    /// Returns `None` for bare segment files (no recognised suffix) and any
    /// name whose ULID stem is invalid.
    pub fn from_filename(name: &str) -> Option<Self> {
        let (stem, suffix) = name.rsplit_once('.')?;
        let state = GcHandoffState::from_suffix(suffix)?;
        let ulid = Ulid::from_string(stem).ok()?;
        Some(Self { ulid, state })
    }

    /// Reconstruct the filename for this handoff in its current state.
    pub fn filename(&self) -> String {
        format!("{}.{}", self.ulid, self.state.suffix())
    }

    /// Return a new `GcHandoff` with the same ULID in `state`.
    pub fn with_state(self, state: GcHandoffState) -> Self {
        Self {
            ulid: self.ulid,
            state,
        }
    }
}

/// One line from a GC handoff (.pending) file.
#[derive(Debug, PartialEq)]
pub enum HandoffLine {
    /// A live extent has been copied from `old_ulid` to `new_ulid`.
    Repack {
        hash: blake3::Hash,
        old_ulid: Ulid,
        new_ulid: Ulid,
        new_offset: u64,
    },
    /// An extent index entry for `hash` must be removed; the extent is
    /// LBA-dead and was not carried forward.
    Remove { hash: blake3::Hash, old_ulid: Ulid },
    /// The segment was entirely dead; the volume just needs to acknowledge.
    Dead { old_ulid: Ulid },
}

impl HandoffLine {
    /// Parse one line from a handoff file.  Returns `None` for blank lines or
    /// unrecognised prefixes (forward-compatibility: ignore unknown lines).
    pub fn parse(line: &str) -> Option<Self> {
        let mut parts = line.split_whitespace();
        match parts.next()? {
            "repack" => {
                let hash = blake3::Hash::from_hex(parts.next()?).ok()?;
                let old_ulid = Ulid::from_string(parts.next()?).ok()?;
                let new_ulid = Ulid::from_string(parts.next()?).ok()?;
                let new_offset: u64 = parts.next()?.parse().ok()?;
                Some(Self::Repack {
                    hash,
                    old_ulid,
                    new_ulid,
                    new_offset,
                })
            }
            "remove" => {
                let hash = blake3::Hash::from_hex(parts.next()?).ok()?;
                let old_ulid = Ulid::from_string(parts.next()?).ok()?;
                Some(Self::Remove { hash, old_ulid })
            }
            "dead" => {
                let old_ulid = Ulid::from_string(parts.next()?).ok()?;
                Some(Self::Dead { old_ulid })
            }
            _ => None,
        }
    }
}

impl IntoIterator for HandoffLine {
    type Item = HandoffLine;
    type IntoIter = std::iter::Once<HandoffLine>;
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self)
    }
}

/// Serialize `HandoffLine`s into file content (one line each).
///
/// This is the canonical way to write a `.pending` file.  Accepts any
/// `IntoIterator<Item = HandoffLine>` — pass a single line, a `Vec`, or an
/// array.
///
/// Parsing is done line-by-line with `HandoffLine::parse`.
pub fn format_handoff_file(lines: impl IntoIterator<Item = HandoffLine>) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    for line in lines {
        writeln!(out, "{line}").expect("write to String is infallible");
    }
    out
}

impl fmt::Display for HandoffLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repack {
                hash,
                old_ulid,
                new_ulid,
                new_offset,
            } => write!(f, "repack {hash} {old_ulid} {new_ulid} {new_offset}"),
            Self::Remove { hash, old_ulid } => write!(f, "remove {hash} {old_ulid}"),
            Self::Dead { old_ulid } => write!(f, "dead {old_ulid}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ulid(ms: u64, r: u128) -> Ulid {
        Ulid::from_parts(ms, r)
    }

    #[test]
    fn roundtrip_repack() {
        let hash = blake3::hash(b"payload");
        let old = ulid(1, 0);
        let new = ulid(2, 0);
        let line = HandoffLine::Repack {
            hash,
            old_ulid: old,
            new_ulid: new,
            new_offset: 4096,
        };
        let s = line.to_string();
        assert_eq!(HandoffLine::parse(&s), Some(line));
    }

    #[test]
    fn roundtrip_remove() {
        let hash = blake3::hash(b"removed");
        let old = ulid(3, 0);
        let line = HandoffLine::Remove {
            hash,
            old_ulid: old,
        };
        let s = line.to_string();
        assert_eq!(HandoffLine::parse(&s), Some(line));
    }

    #[test]
    fn roundtrip_dead() {
        let old = ulid(4, 0);
        let line = HandoffLine::Dead { old_ulid: old };
        let s = line.to_string();
        assert_eq!(HandoffLine::parse(&s), Some(line));
    }

    #[test]
    fn parse_unknown_prefix_returns_none() {
        assert_eq!(HandoffLine::parse("unknown abc def"), None);
    }

    #[test]
    fn parse_blank_line_returns_none() {
        assert_eq!(HandoffLine::parse(""), None);
        assert_eq!(HandoffLine::parse("   "), None);
    }

    #[test]
    fn parse_repack_bad_hash_returns_none() {
        let old = ulid(1, 0);
        let new = ulid(2, 0);
        assert_eq!(
            HandoffLine::parse(&format!("repack NOTHEX {old} {new} 0")),
            None
        );
    }

    #[test]
    fn parse_dead_bad_ulid_returns_none() {
        assert_eq!(HandoffLine::parse("dead not-a-ulid"), None);
    }
}
