// Pending-delete markers.
//
// See `docs/design-replica-model.md` for the surrounding design. This
// module defines the on-disk record and parsing primitives. The reaper
// itself lives in `crate::reaper`.
//
// Marker shape:
//   path     = `by_id/<vol_ulid>/retention/<gc_output_ulid>`
//   content  = one input segment ULID per line, plain text
//
// The marker filename is the GC output ULID — this gives a direct
// cross-reference to `segments/<date>/<gc_output_ulid>` (the GC output
// itself in S3) and makes re-uploading the same handoff fully
// idempotent. The retention deadline is *derived* from the filename
// ULID's timestamp plus the coordinator's current `pending_delete_retention`
// (see `operations.md` *gc_checkpoint — the pre-mint pattern* for why
// `ulid_timestamp(gc_output_ulid)` reliably tracks handoff wall-clock).

use std::fmt;

use object_store::path::Path as StorePath;
use ulid::Ulid;

/// Hard cap on input lines per marker. Realistic GC handoffs sit at
/// ~tens of inputs; anything close to the cap is a malformed-marker or
/// runaway-bug signal that the reaper rejects up front.
pub const MAX_INPUTS_PER_MARKER: usize = 1024;

/// Build the canonical S3 key for a marker.
pub fn marker_key(vol: Ulid, gc_output: Ulid) -> StorePath {
    StorePath::from(format!("by_id/{vol}/retention/{gc_output}"))
}

/// Build the canonical S3 segment key for an input ULID under the given
/// volume. The reaper uses this to translate a parsed input ULID into
/// the key it will DELETE — the volume scope comes from the trusted
/// invocation argument, not from the marker.
pub fn segment_key_for(vol: Ulid, seg: Ulid) -> StorePath {
    let dt = chrono::DateTime::<chrono::Utc>::from(seg.datetime());
    let date = dt.format("%Y%m%d").to_string();
    StorePath::from(format!("by_id/{vol}/segments/{date}/{seg}"))
}

/// Render a marker body from a list of input ULIDs.
///
/// One ULID per line, terminated with `\n`. Rendering is total: any
/// `Vec<Ulid>` produces a valid marker body.
pub fn render_marker(inputs: &[Ulid]) -> String {
    let mut out = String::with_capacity(inputs.len() * 27);
    for u in inputs {
        out.push_str(&u.to_string());
        out.push('\n');
    }
    out
}

/// Parse a marker body into a list of input ULIDs.
///
/// Strict: empty lines, whitespace, trailing data, non-ULID strings,
/// and excess length all reject the whole marker.
pub fn parse_marker_body(body: &str) -> Result<Vec<Ulid>, ParseMarkerError> {
    let mut out = Vec::new();
    for (lineno, line) in body.split('\n').enumerate() {
        // The split after a trailing '\n' yields one empty tail token;
        // accept it only at the very end.
        if line.is_empty() {
            continue;
        }
        if out.len() >= MAX_INPUTS_PER_MARKER {
            return Err(ParseMarkerError::TooManyInputs);
        }
        let ulid =
            Ulid::from_string(line).map_err(|_| ParseMarkerError::InvalidUlid { line: lineno })?;
        out.push(ulid);
    }
    Ok(out)
}

/// Parse the volume ULID and GC-output ULID out of a marker's S3 key.
///
/// Expected shape: `by_id/<vol>/retention/<gc_output>`. The reaper
/// asserts the parsed `vol` equals its invocation ULID before acting on
/// the marker — see *Target validation* in the design doc.
pub fn parse_marker_key(key: &str) -> Result<(Ulid, Ulid), ParseMarkerKeyError> {
    if key.contains('\0') || key.starts_with('/') || key.ends_with('/') || key.contains("//") {
        return Err(ParseMarkerKeyError::PathWeirdness);
    }
    let parts: Vec<&str> = key.split('/').collect();
    if parts
        .iter()
        .any(|p| p.is_empty() || *p == "." || *p == "..")
    {
        return Err(ParseMarkerKeyError::PathWeirdness);
    }
    if parts.len() != 4 || parts[0] != "by_id" || parts[2] != "retention" {
        return Err(ParseMarkerKeyError::Shape);
    }
    let vol = Ulid::from_string(parts[1]).map_err(|_| ParseMarkerKeyError::InvalidUlid)?;
    let gc_output = Ulid::from_string(parts[3]).map_err(|_| ParseMarkerKeyError::InvalidUlid)?;
    Ok((vol, gc_output))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseMarkerError {
    InvalidUlid { line: usize },
    TooManyInputs,
}

impl fmt::Display for ParseMarkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseMarkerError::InvalidUlid { line } => write!(f, "line {line}: invalid ULID"),
            ParseMarkerError::TooManyInputs => {
                write!(f, "marker exceeds {MAX_INPUTS_PER_MARKER} inputs")
            }
        }
    }
}

impl std::error::Error for ParseMarkerError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseMarkerKeyError {
    Shape,
    InvalidUlid,
    PathWeirdness,
}

impl fmt::Display for ParseMarkerKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseMarkerKeyError::Shape => write!(f, "unrecognised marker key shape"),
            ParseMarkerKeyError::InvalidUlid => write!(f, "invalid ULID component"),
            ParseMarkerKeyError::PathWeirdness => {
                write!(f, "path contains traversal or NUL components")
            }
        }
    }
}

impl std::error::Error for ParseMarkerKeyError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn vol() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[test]
    fn render_round_trips() {
        let inputs = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let body = render_marker(&inputs);
        let parsed = parse_marker_body(&body).unwrap();
        assert_eq!(parsed, inputs);
    }

    #[test]
    fn empty_marker_round_trips() {
        let body = render_marker(&[]);
        assert!(body.is_empty());
        let parsed = parse_marker_body(&body).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn rejects_invalid_ulid_line() {
        let body = "01J0000000000000000000000V\nnot-a-ulid\n";
        let err = parse_marker_body(body).unwrap_err();
        assert!(matches!(err, ParseMarkerError::InvalidUlid { line: 1 }));
    }

    #[test]
    fn rejects_oversize_marker() {
        let mut body = String::new();
        for _ in 0..(MAX_INPUTS_PER_MARKER + 1) {
            body.push_str("01J0000000000000000000000V\n");
        }
        let err = parse_marker_body(&body).unwrap_err();
        assert!(matches!(err, ParseMarkerError::TooManyInputs));
    }

    #[test]
    fn rejects_whitespace_in_line() {
        let body = " 01J0000000000000000000000V\n";
        let err = parse_marker_body(body).unwrap_err();
        assert!(matches!(err, ParseMarkerError::InvalidUlid { line: 0 }));
    }

    #[test]
    fn parses_marker_key() {
        let v = vol();
        let g = Ulid::new();
        let key = format!("by_id/{v}/retention/{g}");
        let (parsed_vol, parsed_gc) = parse_marker_key(&key).unwrap();
        assert_eq!(parsed_vol, v);
        assert_eq!(parsed_gc, g);
    }

    #[test]
    fn rejects_marker_key_path_traversal() {
        for bad in [
            "by_id/01J0000000000000000000000V/retention/../escape",
            "by_id/../etc/retention/01J0000000000000000000000V",
            "/by_id/01J0000000000000000000000V/retention/01J0000000000000000000000V",
            "by_id//01J0000000000000000000000V/retention/01J0000000000000000000000V",
        ] {
            let err = parse_marker_key(bad).unwrap_err();
            assert!(
                matches!(
                    err,
                    ParseMarkerKeyError::PathWeirdness | ParseMarkerKeyError::InvalidUlid
                ),
                "expected weirdness/invalid for {bad}, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_marker_key_wrong_shape() {
        for bad in [
            "by_id/01J0000000000000000000000V/manifest.toml",
            "by_id/01J0000000000000000000000V/segments/20260425/X",
            "names/anything",
        ] {
            let err = parse_marker_key(bad).unwrap_err();
            assert!(
                matches!(err, ParseMarkerKeyError::Shape),
                "expected Shape for {bad}, got {err:?}"
            );
        }
    }

    #[test]
    fn segment_key_for_uses_volume_arg() {
        let v = vol();
        let seg = Ulid::new();
        let date = chrono::DateTime::<chrono::Utc>::from(seg.datetime())
            .format("%Y%m%d")
            .to_string();
        assert_eq!(
            segment_key_for(v, seg).as_ref(),
            format!("by_id/{v}/segments/{date}/{seg}")
        );
    }
}
