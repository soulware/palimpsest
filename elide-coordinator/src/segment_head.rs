// Per-volume HEAD: the post-snapshot delta over the latest signed manifest.
//
// See `docs/design-segment-index.md` for the surrounding design. This module
// defines the on-disk record and parsing primitives plus the live-set
// computation; the single-writer tick-loop integration lives in
// `crate::gc_cycle`, and the read-side consumers (prefetch, fork, recovery)
// call `read_head` + `live_set`.
//
// Object shape:
//   path     = `by_id/<vol_ulid>/HEAD`
//   content  =
//     anchor: <snap_ulid|nil>
//     added:
//       <ulid>
//       ...
//     superseded:
//       <input-ulid> <output-ulid> <since-rfc3339>
//       ...
//     tombstoned:
//       <ulid>
//       ...
//
// All three sections are always present (empty when no entries) — a
// canonical form so the rebuild's bytes match what an incremental writer
// would produce. ULIDs are sorted lex within each section; `superseded`
// is keyed by `input` (the segment being killed). `since` is RFC3339,
// matching the manifest's `recovered_at`. No `sig:` — HEAD is derived,
// unsigned state (every segment carries its own Ed25519 signature).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tracing::warn;
use ulid::Ulid;

use crate::portable::MIME_TEXT;

/// The post-snapshot delta carried by `by_id/<vol>/HEAD`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentHead {
    /// The manifest this HEAD is a delta over. `None` (rendered as `nil`)
    /// on a fresh volume with no snapshot yet. Self-describing for
    /// operators; not load-bearing for correctness (the manifest set is
    /// the arbiter regardless).
    pub anchor: Option<Ulid>,
    /// Segments uploaded (drain) or produced (GC output) since the
    /// anchor manifest.
    pub added: BTreeSet<Ulid>,
    /// GC supersession edges: `input → (output, since)`. `since` is the
    /// wall-clock instant the supersession was recorded — required
    /// because the GC output ULID is history-derived
    /// (`max(inputs).increment()`), not wall-clock, so the retention
    /// deadline cannot be derived from the output ULID alone.
    pub superseded: BTreeMap<Ulid, Supersession>,
    /// Segments the reaper has DELETEd from S3. Cleared at each seal
    /// (the new manifest simply doesn't enumerate them).
    pub tombstoned: BTreeSet<Ulid>,
}

/// A single GC supersession edge stored in [`SegmentHead::superseded`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Supersession {
    pub output: Ulid,
    pub since: DateTime<Utc>,
}

impl SegmentHead {
    /// Empty HEAD anchored at `anchor`. Used at seal time to truncate.
    pub fn empty(anchor: Option<Ulid>) -> Self {
        Self {
            anchor,
            added: BTreeSet::new(),
            superseded: BTreeMap::new(),
            tombstoned: BTreeSet::new(),
        }
    }

    /// `true` when no entries are present in any section.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.superseded.is_empty() && self.tombstoned.is_empty()
    }

    /// Reflect a reap step that deleted `reaped` from S3: drop them
    /// from `added` (if present), drop their `superseded` edges (input
    /// is gone, edge is meaningless), and record `tombstoned`. The
    /// `live_set` formula is unconditional and works regardless, but
    /// keeping the body minimal avoids redundant entries.
    pub fn apply_reap(&mut self, reaped: &[Ulid]) {
        for u in reaped {
            self.added.remove(u);
            self.superseded.remove(u);
            self.tombstoned.insert(*u);
        }
    }
}

/// Build the canonical S3 key for a volume's HEAD object. Fixed key per
/// volume — like `snapshots/LATEST`, whole-object overwrite.
pub fn head_key(vol: Ulid) -> StorePath {
    StorePath::from(format!("by_id/{vol}/HEAD"))
}

/// Render a [`SegmentHead`] to its canonical on-disk form. Total: any
/// `SegmentHead` produces a valid body. Sorting is structural (the
/// fields are `BTreeSet`/`BTreeMap`), so the output is deterministic.
pub fn render(head: &SegmentHead) -> String {
    let mut out = String::new();
    out.push_str("anchor: ");
    match head.anchor {
        Some(u) => out.push_str(&u.to_string()),
        None => out.push_str("nil"),
    }
    out.push('\n');

    out.push_str("added:\n");
    for u in &head.added {
        out.push_str("  ");
        out.push_str(&u.to_string());
        out.push('\n');
    }

    out.push_str("superseded:\n");
    for (input, edge) in &head.superseded {
        out.push_str("  ");
        out.push_str(&input.to_string());
        out.push(' ');
        out.push_str(&edge.output.to_string());
        out.push(' ');
        out.push_str(&edge.since.to_rfc3339());
        out.push('\n');
    }

    out.push_str("tombstoned:\n");
    for u in &head.tombstoned {
        out.push_str("  ");
        out.push_str(&u.to_string());
        out.push('\n');
    }

    out
}

/// Parse a HEAD body. Strict: unknown section headers, missing required
/// sections, malformed entries, and trailing data all reject the whole
/// body. The rebuild defines correctness — a divergence between this
/// parser and the writer is a bug, not a tolerated case.
pub fn parse(body: &str) -> Result<SegmentHead, ParseHeadError> {
    let mut anchor: Option<Option<Ulid>> = None;
    let mut added: Option<BTreeSet<Ulid>> = None;
    let mut superseded: Option<BTreeMap<Ulid, Supersession>> = None;
    let mut tombstoned: Option<BTreeSet<Ulid>> = None;

    let mut lines = body.lines().enumerate().peekable();
    while let Some((lineno, line)) = lines.next() {
        if let Some(rest) = line.strip_prefix("anchor: ") {
            if anchor.is_some() {
                return Err(ParseHeadError::DuplicateSection { line: lineno });
            }
            anchor = Some(parse_anchor(rest, lineno)?);
        } else if line == "added:" {
            if added.is_some() {
                return Err(ParseHeadError::DuplicateSection { line: lineno });
            }
            added = Some(consume_ulid_section(&mut lines)?);
        } else if line == "superseded:" {
            if superseded.is_some() {
                return Err(ParseHeadError::DuplicateSection { line: lineno });
            }
            superseded = Some(consume_superseded_section(&mut lines)?);
        } else if line == "tombstoned:" {
            if tombstoned.is_some() {
                return Err(ParseHeadError::DuplicateSection { line: lineno });
            }
            tombstoned = Some(consume_ulid_section(&mut lines)?);
        } else {
            return Err(ParseHeadError::UnknownLine { line: lineno });
        }
    }

    Ok(SegmentHead {
        anchor: anchor.ok_or(ParseHeadError::MissingSection { name: "anchor" })?,
        added: added.ok_or(ParseHeadError::MissingSection { name: "added" })?,
        superseded: superseded.ok_or(ParseHeadError::MissingSection { name: "superseded" })?,
        tombstoned: tombstoned.ok_or(ParseHeadError::MissingSection { name: "tombstoned" })?,
    })
}

fn parse_anchor(rest: &str, lineno: usize) -> Result<Option<Ulid>, ParseHeadError> {
    if rest == "nil" {
        Ok(None)
    } else {
        Ulid::from_string(rest)
            .map(Some)
            .map_err(|_| ParseHeadError::InvalidUlid { line: lineno })
    }
}

fn consume_ulid_section<'a, I>(
    lines: &mut std::iter::Peekable<I>,
) -> Result<BTreeSet<Ulid>, ParseHeadError>
where
    I: Iterator<Item = (usize, &'a str)>,
{
    let mut out = BTreeSet::new();
    while let Some((lineno, peek)) = lines.peek().copied() {
        let Some(entry) = peek.strip_prefix("  ") else {
            break;
        };
        lines.next();
        let u =
            Ulid::from_string(entry).map_err(|_| ParseHeadError::InvalidUlid { line: lineno })?;
        if !out.insert(u) {
            return Err(ParseHeadError::DuplicateEntry { line: lineno });
        }
    }
    Ok(out)
}

fn consume_superseded_section<'a, I>(
    lines: &mut std::iter::Peekable<I>,
) -> Result<BTreeMap<Ulid, Supersession>, ParseHeadError>
where
    I: Iterator<Item = (usize, &'a str)>,
{
    let mut out: BTreeMap<Ulid, Supersession> = BTreeMap::new();
    while let Some((lineno, peek)) = lines.peek().copied() {
        let Some(entry) = peek.strip_prefix("  ") else {
            break;
        };
        lines.next();
        let mut parts = entry.split(' ');
        let input_s = parts
            .next()
            .ok_or(ParseHeadError::MalformedSuperseded { line: lineno })?;
        let output_s = parts
            .next()
            .ok_or(ParseHeadError::MalformedSuperseded { line: lineno })?;
        let since_s = parts
            .next()
            .ok_or(ParseHeadError::MalformedSuperseded { line: lineno })?;
        if parts.next().is_some() {
            return Err(ParseHeadError::MalformedSuperseded { line: lineno });
        }
        let input =
            Ulid::from_string(input_s).map_err(|_| ParseHeadError::InvalidUlid { line: lineno })?;
        let output = Ulid::from_string(output_s)
            .map_err(|_| ParseHeadError::InvalidUlid { line: lineno })?;
        let since = DateTime::parse_from_rfc3339(since_s)
            .map_err(|_| ParseHeadError::InvalidTimestamp { line: lineno })?
            .with_timezone(&Utc);
        if out.insert(input, Supersession { output, since }).is_some() {
            return Err(ParseHeadError::DuplicateEntry { line: lineno });
        }
    }
    Ok(out)
}

/// Compute the live segment set from the anchor manifest and HEAD.
///
/// `live = manifest ∪ added − superseded.inputs − tombstoned`
///
/// Matches `docs/design-segment-index.md` *Read path*. The manifest
/// `segment_ulids` is authoritative for the snapshot/HEAD boundary;
/// HEAD is a pure delta. `Superseded` is applied over the manifest set
/// too, not just over `added` — a pre-snapshot input GC superseded
/// *after* the snapshot is in the manifest and must still be skipped.
pub fn live_set(manifest_segments: &BTreeSet<Ulid>, head: &SegmentHead) -> BTreeSet<Ulid> {
    let mut live = manifest_segments.clone();
    live.extend(head.added.iter().copied());
    for input in head.superseded.keys() {
        live.remove(input);
    }
    for u in &head.tombstoned {
        live.remove(u);
    }
    live
}

/// GET `by_id/<vol>/HEAD`. `Ok(SegmentHead::empty(None))` when absent —
/// HEAD self-heals on the next active tick, so a 404 starts the writer
/// from an empty state rather than failing.
///
/// An unparseable body is also treated as empty (logged), matching the
/// design's "derived, unsigned state" stance: HEAD is an
/// availability/enumeration hint, not an authenticity root, and
/// corruption self-heals on the next writer pass.
pub async fn read_head(
    store: &Arc<dyn ObjectStore>,
    vol: Ulid,
) -> Result<SegmentHead, ReadHeadError> {
    let key = head_key(vol);
    let bytes = match store.get(&key).await {
        Ok(g) => g
            .bytes()
            .await
            .map_err(|e| ReadHeadError::Get(format!("{key}: {e}")))?,
        Err(object_store::Error::NotFound { .. }) => return Ok(SegmentHead::empty(None)),
        Err(e) => return Err(ReadHeadError::Get(format!("{key}: {e}"))),
    };
    let text = std::str::from_utf8(&bytes).map_err(|e| ReadHeadError::NotUtf8(e.to_string()))?;
    match parse(text) {
        Ok(h) => Ok(h),
        Err(e) => {
            warn!(
                "[segment_head] {key} unparseable ({e}); treating as empty (self-heals on next tick)"
            );
            Ok(SegmentHead::empty(None))
        }
    }
}

/// PUT `by_id/<vol>/HEAD` with the rendered body. Whole-object overwrite,
/// no CAS — the per-volume tick loop is the sole writer (drain → GC →
/// reap sequential within one tick), so single-writer-per-vol-epoch is a
/// structural fact, not a runtime check.
pub async fn put_head(
    store: &Arc<dyn ObjectStore>,
    vol: Ulid,
    head: &SegmentHead,
) -> Result<(), PutHeadError> {
    let key = head_key(vol);
    let body = render(head);
    crate::upload::put_with_content_type(store, &key, Bytes::from(body.into_bytes()), MIME_TEXT)
        .await
        .map_err(|e| PutHeadError::Put(format!("{key}: {e}")))?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseHeadError {
    MissingSection { name: &'static str },
    DuplicateSection { line: usize },
    UnknownLine { line: usize },
    InvalidUlid { line: usize },
    InvalidTimestamp { line: usize },
    MalformedSuperseded { line: usize },
    DuplicateEntry { line: usize },
}

impl fmt::Display for ParseHeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseHeadError::MissingSection { name } => write!(f, "missing {name} section"),
            ParseHeadError::DuplicateSection { line } => {
                write!(f, "line {line}: duplicate section")
            }
            ParseHeadError::UnknownLine { line } => write!(f, "line {line}: unrecognised"),
            ParseHeadError::InvalidUlid { line } => write!(f, "line {line}: invalid ULID"),
            ParseHeadError::InvalidTimestamp { line } => {
                write!(f, "line {line}: invalid RFC3339 timestamp")
            }
            ParseHeadError::MalformedSuperseded { line } => {
                write!(f, "line {line}: malformed superseded entry")
            }
            ParseHeadError::DuplicateEntry { line } => write!(f, "line {line}: duplicate entry"),
        }
    }
}

impl std::error::Error for ParseHeadError {}

#[derive(Debug)]
pub enum ReadHeadError {
    Get(String),
    NotUtf8(String),
}

impl fmt::Display for ReadHeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadHeadError::Get(s) => write!(f, "getting HEAD: {s}"),
            ReadHeadError::NotUtf8(s) => write!(f, "HEAD body not utf-8: {s}"),
        }
    }
}

impl std::error::Error for ReadHeadError {}

#[derive(Debug)]
pub enum PutHeadError {
    Put(String),
}

impl fmt::Display for PutHeadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PutHeadError::Put(s) => write!(f, "putting HEAD: {s}"),
        }
    }
}

impl std::error::Error for PutHeadError {}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_core::ulid_mint::UlidMint;

    fn mint() -> UlidMint {
        UlidMint::new(Ulid::nil())
    }

    fn vol() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[test]
    fn empty_head_renders_canonical_form() {
        let h = SegmentHead::empty(None);
        let body = render(&h);
        assert_eq!(
            body, "anchor: nil\nadded:\nsuperseded:\ntombstoned:\n",
            "empty HEAD must always emit all three section headers"
        );
    }

    #[test]
    fn empty_head_round_trips() {
        let h = SegmentHead::empty(None);
        let parsed = parse(&render(&h)).unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn populated_head_round_trips() {
        let mut m = mint();
        let anchor = m.next();
        let a1 = m.next();
        let a2 = m.next();
        let in1 = m.next();
        let out1 = m.next();
        let t1 = m.next();

        let mut h = SegmentHead::empty(Some(anchor));
        h.added.insert(a1);
        h.added.insert(a2);
        h.superseded.insert(
            in1,
            Supersession {
                output: out1,
                since: DateTime::parse_from_rfc3339("2026-05-20T12:34:56Z")
                    .unwrap()
                    .with_timezone(&Utc),
            },
        );
        h.tombstoned.insert(t1);

        let parsed = parse(&render(&h)).unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn render_sorts_sections_lex() {
        let mut m = mint();
        let early = m.next();
        let late = m.next();
        // BTreeSet sorts; the render must emit `early` before `late`
        // even if inserted in reverse order.
        let mut h = SegmentHead::empty(None);
        h.added.insert(late);
        h.added.insert(early);
        let body = render(&h);
        let early_pos = body.find(&early.to_string()).unwrap();
        let late_pos = body.find(&late.to_string()).unwrap();
        assert!(
            early_pos < late_pos,
            "added section must be sorted lex (chronological for ULIDs)"
        );
    }

    #[test]
    fn parse_rejects_unknown_section() {
        let body = "anchor: nil\nadded:\nsuperseded:\ntombstoned:\nextra:\n";
        let err = parse(body).unwrap_err();
        assert!(matches!(err, ParseHeadError::UnknownLine { .. }));
    }

    #[test]
    fn parse_rejects_missing_section() {
        let body = "anchor: nil\nadded:\nsuperseded:\n"; // no tombstoned
        let err = parse(body).unwrap_err();
        assert!(matches!(
            err,
            ParseHeadError::MissingSection { name: "tombstoned" }
        ));
    }

    #[test]
    fn parse_rejects_invalid_ulid_in_added() {
        let body = "anchor: nil\nadded:\n  not-a-ulid\nsuperseded:\ntombstoned:\n";
        let err = parse(body).unwrap_err();
        assert!(matches!(err, ParseHeadError::InvalidUlid { .. }));
    }

    #[test]
    fn parse_rejects_malformed_superseded() {
        let body = "anchor: nil\nadded:\nsuperseded:\n  01J0000000000000000000000V only-one\ntombstoned:\n";
        let err = parse(body).unwrap_err();
        assert!(matches!(err, ParseHeadError::MalformedSuperseded { .. }));
    }

    #[test]
    fn parse_rejects_invalid_timestamp() {
        let v = Ulid::from_string("01J0000000000000000000000V").unwrap();
        let body = format!("anchor: nil\nadded:\nsuperseded:\n  {v} {v} not-a-time\ntombstoned:\n");
        let err = parse(&body).unwrap_err();
        assert!(matches!(err, ParseHeadError::InvalidTimestamp { .. }));
    }

    #[test]
    fn parse_rejects_duplicate_added_entry() {
        let v = "01J0000000000000000000000V";
        let body = format!("anchor: nil\nadded:\n  {v}\n  {v}\nsuperseded:\ntombstoned:\n");
        let err = parse(&body).unwrap_err();
        assert!(matches!(err, ParseHeadError::DuplicateEntry { .. }));
    }

    #[test]
    fn parse_rejects_duplicate_section() {
        let body = "anchor: nil\nadded:\nadded:\nsuperseded:\ntombstoned:\n";
        let err = parse(body).unwrap_err();
        assert!(matches!(err, ParseHeadError::DuplicateSection { .. }));
    }

    #[test]
    fn live_set_matches_design_formula() {
        let mut m = mint();
        let pre1 = m.next(); // in manifest
        let pre2 = m.next(); // in manifest, will be superseded
        let pre3 = m.next(); // in manifest, will be tombstoned
        let post1 = m.next(); // added post-snapshot
        let post2 = m.next(); // added then superseded
        let out = m.next(); // gc output for pre2 and post2

        let manifest: BTreeSet<Ulid> = [pre1, pre2, pre3].into_iter().collect();

        let mut head = SegmentHead::empty(Some(Ulid::nil()));
        head.added.insert(post1);
        head.added.insert(post2);
        head.added.insert(out);
        head.superseded.insert(
            pre2,
            Supersession {
                output: out,
                since: Utc::now(),
            },
        );
        head.superseded.insert(
            post2,
            Supersession {
                output: out,
                since: Utc::now(),
            },
        );
        head.tombstoned.insert(pre3);

        let live = live_set(&manifest, &head);
        let expected: BTreeSet<Ulid> = [pre1, post1, out].into_iter().collect();
        assert_eq!(live, expected);
    }

    #[test]
    fn apply_reap_drops_added_and_superseded_and_records_tombstone() {
        let mut m = mint();
        let input = m.next();
        let output = m.next();
        let unrelated_post = m.next();

        let mut head = SegmentHead::empty(None);
        head.added.insert(input);
        head.added.insert(unrelated_post);
        head.superseded.insert(
            input,
            Supersession {
                output,
                since: Utc::now(),
            },
        );

        head.apply_reap(&[input]);

        assert!(
            !head.added.contains(&input),
            "reaped input dropped from added"
        );
        assert!(
            head.added.contains(&unrelated_post),
            "unrelated added entry retained"
        );
        assert!(
            !head.superseded.contains_key(&input),
            "supersession edge for reaped input dropped"
        );
        assert!(head.tombstoned.contains(&input), "reaped input tombstoned");
    }

    #[test]
    fn head_key_matches_design() {
        let v = vol();
        assert_eq!(head_key(v).as_ref(), format!("by_id/{v}/HEAD"));
    }

    #[test]
    fn anchor_nil_round_trips() {
        let h = SegmentHead::empty(None);
        let parsed = parse(&render(&h)).unwrap();
        assert_eq!(parsed.anchor, None);
    }

    #[test]
    fn anchor_some_round_trips() {
        let mut m = mint();
        let a = m.next();
        let h = SegmentHead::empty(Some(a));
        let parsed = parse(&render(&h)).unwrap();
        assert_eq!(parsed.anchor, Some(a));
    }

    #[tokio::test]
    async fn read_returns_empty_when_absent() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let h = read_head(&store, vol()).await.unwrap();
        assert_eq!(h, SegmentHead::empty(None));
    }

    #[tokio::test]
    async fn put_then_read_round_trips() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut m = mint();
        let anchor = m.next();
        let a = m.next();
        let input = m.next();
        let output = m.next();
        let tomb = m.next();

        let mut h = SegmentHead::empty(Some(anchor));
        h.added.insert(a);
        h.added.insert(output);
        h.superseded.insert(
            input,
            Supersession {
                output,
                since: DateTime::parse_from_rfc3339("2026-05-20T12:34:56Z")
                    .unwrap()
                    .with_timezone(&Utc),
            },
        );
        h.tombstoned.insert(tomb);

        put_head(&store, vol(), &h).await.unwrap();
        let got = read_head(&store, vol()).await.unwrap();
        assert_eq!(got, h);
    }

    #[tokio::test]
    async fn put_overwrites_previous_body() {
        // Whole-object overwrite: a second put with fewer entries must
        // replace, not merge — single-writer-per-vol-epoch is the
        // invariant that makes this safe.
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut m = mint();
        let a1 = m.next();
        let a2 = m.next();

        let mut h1 = SegmentHead::empty(None);
        h1.added.insert(a1);
        h1.added.insert(a2);
        put_head(&store, vol(), &h1).await.unwrap();

        let h2 = SegmentHead::empty(None);
        put_head(&store, vol(), &h2).await.unwrap();

        let got = read_head(&store, vol()).await.unwrap();
        assert!(got.added.is_empty(), "second put must overwrite, not merge");
    }

    #[tokio::test]
    async fn unparseable_body_treated_as_empty() {
        // Corruption self-heals on the next writer pass — HEAD is
        // derived state, not authority.
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let key = head_key(vol());
        store
            .put(&key, Bytes::from_static(b"not a valid head body").into())
            .await
            .unwrap();
        let h = read_head(&store, vol()).await.unwrap();
        assert_eq!(h, SegmentHead::empty(None));
    }
}
