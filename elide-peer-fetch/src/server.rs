//! Peer-fetch HTTP server.
//!
//! Routes, all full-file GETs (no Range support in v1):
//!
//! - `GET /v1/<vol_id>/<ulid>.idx`         → serves `<data_dir>/by_id/<vol_id>/index/<ulid>.idx`
//! - `GET /v1/<vol_id>/<ulid>.prefetch`    → serves `<data_dir>/by_id/<vol_id>/cache/<ulid>.present`
//! - `GET /v1/<vol_id>/<ulid>.manifest`    → serves `<data_dir>/by_id/<vol_id>/snapshots/<ulid>.manifest`
//! - `GET /v1/<vol_id>/volume.pub`         → serves `<data_dir>/by_id/<vol_id>/volume.pub`
//! - `GET /v1/<vol_id>/volume.provenance`  → serves `<data_dir>/by_id/<vol_id>/volume.provenance`
//!
//! The last two are the per-fork skeleton files an ancestor walk needs
//! before any of the segment/snapshot routes can be issued for that
//! ancestor; serving them peer-first lets `pull_volume_skeleton` close
//! out the last category of S3 traffic on the claim path.
//!
//! The wire `.prefetch` resource is deliberately a different name from
//! the on-disk `.present` file: the bytes are returned verbatim in v1
//! but clients consume the response as advisory state, never as
//! authoritative cache state. See `docs/design-peer-segment-fetch.md`
//! § "What's served" for the rationale.
//!
//! For the `.manifest` route the second URL component is a *snapshot*
//! ULID, not a segment ULID. The route runs under
//! [`crate::auth::RouteAuthMode::SkeletonsOnly`] alongside
//! `volume.pub` / `volume.provenance` — manifests are signed and the
//! caller verifies before trusting bytes, and `skip_empty_intermediates`
//! has to read each ancestor's manifest to discover its parent before
//! the requester's own `volume.provenance` is published.
//!
//! Each request runs the [`crate::auth`] five-step verify pipeline
//! before the local file is touched. A successful auth doesn't imply
//! the file exists — segment-membership is the route-level local
//! stat, and a missing file falls out as 404. That's
//! indistinguishable from "no such ulid" by design (the requester
//! computes ancestry themselves and could already enumerate which
//! ULIDs *should* exist).

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::net::TcpListener;
use tracing::info;
use ulid::Ulid;

use crate::auth::{AuthError, AuthState, parse_bearer};
use elide_core::segment::{EntryKind as SegEntryKind, read_segment_index};

/// Errors the route handler emits before reaching the auth pipeline
/// or the local file. Map to status codes for the response.
#[derive(Debug)]
enum RouteError {
    /// `<vol_id>` segment is not a parseable ULID.
    BadVolId,
    /// Filename doesn't match `<ulid>.idx` or `<ulid>.prefetch`.
    UnknownFilename,
    /// `<ulid>` portion of the filename is not a parseable ULID.
    BadUlid,
    /// The local file the route resolves to is absent (or unreadable
    /// for transient I/O reasons — both surface as 404 because the
    /// peer is opportunistic).
    NotFound,
    /// Auth pipeline rejected the request.
    Auth(AuthError),
    /// Unexpected I/O error reading the local file.
    Io(std::io::Error),
    /// `Range` header missing or malformed for the `.body` route.
    BadRange,
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadVolId | Self::UnknownFilename | Self::BadUlid | Self::NotFound => {
                StatusCode::NOT_FOUND
            }
            Self::Auth(e) => {
                StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::UNAUTHORIZED)
            }
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BadRange => StatusCode::RANGE_NOT_SATISFIABLE,
        };
        let body = match &self {
            Self::Auth(e) => e.to_string(),
            Self::Io(e) => format!("io: {e}"),
            other => format!("{other:?}"),
        };
        (status, body).into_response()
    }
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadVolId => f.write_str("vol_id segment is not a ulid"),
            Self::UnknownFilename => f.write_str("filename suffix not recognised"),
            Self::BadUlid => f.write_str("ulid portion is not a ulid"),
            Self::NotFound => f.write_str("not found"),
            Self::Auth(e) => write!(f, "auth: {e}"),
            Self::Io(e) => write!(f, "io: {e}"),
            Self::BadRange => f.write_str("Range header missing or malformed"),
        }
    }
}

/// Identifier carried in the second URL component.
///
/// Per-segment / per-snapshot routes embed a ULID; the per-fork
/// skeleton routes embed a literal filename (`volume.pub`,
/// `volume.provenance`) and have no ULID.
#[derive(Clone, Copy)]
enum ResourceTarget {
    Ulid(Ulid),
    Skeleton,
}

impl std::fmt::Display for ResourceTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ulid(u) => u.fmt(f),
            Self::Skeleton => f.write_str("-"),
        }
    }
}

#[derive(Clone, Copy)]
enum ResourceKind {
    Idx,
    Prefetch,
    SnapshotManifest,
    /// Per-fork `volume.pub` — fetched by ancestor-skeleton pull.
    VolumePub,
    /// Per-fork `volume.provenance` — fetched by ancestor-skeleton pull.
    VolumeProvenance,
}

impl ResourceKind {
    /// Subdirectory under `by_id/<vol_id>/` where the file lives, or
    /// `None` for files that sit directly in the fork directory.
    fn local_subdir(self) -> Option<&'static str> {
        match self {
            Self::Idx => Some("index"),
            Self::Prefetch => Some("cache"),
            Self::SnapshotManifest => Some("snapshots"),
            Self::VolumePub | Self::VolumeProvenance => None,
        }
    }

    fn local_filename(self, target: ResourceTarget) -> String {
        match (self, target) {
            // The on-disk `.idx` filename matches the wire suffix.
            (Self::Idx, ResourceTarget::Ulid(u)) => format!("{u}.idx"),
            // The on-disk file is `.present`; the wire calls it
            // `.prefetch` (see docs/design-peer-segment-fetch.md).
            (Self::Prefetch, ResourceTarget::Ulid(u)) => format!("{u}.present"),
            (Self::SnapshotManifest, ResourceTarget::Ulid(u)) => format!("{u}.manifest"),
            (Self::VolumePub, ResourceTarget::Skeleton) => "volume.pub".to_owned(),
            (Self::VolumeProvenance, ResourceTarget::Skeleton) => "volume.provenance".to_owned(),
            // Other combinations are rejected at parse time before
            // ever reaching the resolver.
            (kind, _) => kind.wire_suffix().to_owned(),
        }
    }

    fn wire_suffix(self) -> &'static str {
        match self {
            Self::Idx => ".idx",
            Self::Prefetch => ".prefetch",
            Self::SnapshotManifest => ".manifest",
            Self::VolumePub => "volume.pub",
            Self::VolumeProvenance => "volume.provenance",
        }
    }

    /// Auth mode this route requests from the pipeline.
    ///
    /// Skeleton-class routes (`volume.pub`, `volume.provenance`,
    /// `.manifest`) carry signed metadata already broadly readable
    /// from S3 within the bucket trust boundary, and need to
    /// authenticate during the claim-time chain walk *before* the
    /// requesting fork's own `volume.provenance` is published — so
    /// the lineage walk is skipped. Payload routes (`.idx`,
    /// `.prefetch`) keep the full pipeline. See
    /// `crate::auth::RouteAuthMode` for the security analysis.
    fn auth_mode(self) -> crate::auth::RouteAuthMode {
        match self {
            Self::VolumePub | Self::VolumeProvenance | Self::SnapshotManifest => {
                crate::auth::RouteAuthMode::SkeletonsOnly
            }
            Self::Idx | Self::Prefetch => crate::auth::RouteAuthMode::LineageGated,
        }
    }
}

/// Server context shared across handlers — cheap to clone.
#[derive(Clone)]
pub struct ServerContext {
    pub auth: AuthState,
    pub data_dir: Arc<PathBuf>,
}

impl ServerContext {
    pub fn new(auth: AuthState, data_dir: PathBuf) -> Self {
        Self {
            auth,
            data_dir: Arc::new(data_dir),
        }
    }
}

/// Build the axum [`Router`] for this server. Exposed so tests can
/// drive it with `axum::serve` against a local listener.
pub fn router(ctx: ServerContext) -> Router {
    Router::new()
        .route("/v1/:vol_id/:filename", get(handle_segment))
        .with_state(ctx)
}

/// Bind a TCP listener at `addr` and serve the peer-fetch routes.
/// Returns once the server stops (e.g. on signal handling at the
/// caller).
pub async fn serve(addr: std::net::SocketAddr, ctx: ServerContext) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router(ctx))
        .await
        .map_err(std::io::Error::other)
}

async fn handle_segment(
    State(ctx): State<ServerContext>,
    Path((vol_id_str, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, RouteError> {
    // The `.body` route has its own auth pipeline (volume-signed
    // tokens, no `names/<name>` ownership step) and its own response
    // shape (Range-bounded bytes, coverage-checked against `.present`).
    // Branch on suffix here so the existing index-class flow stays
    // untouched.
    if let Some(seg_str) = filename.strip_suffix(".body") {
        return handle_body(&ctx, &vol_id_str, seg_str, &headers).await;
    }

    let result = handle_segment_inner(&ctx, &vol_id_str, &filename, &headers).await;
    match &result {
        Ok((kind, target, bytes)) => {
            info!(
                "[peer-fetch] served {} {}/{} ({} bytes)",
                kind.wire_suffix(),
                vol_id_str,
                target,
                bytes.len(),
            );
        }
        Err(e) => {
            // Log every rejection. Auth failures are the interesting
            // case for the manual matrix (401/403); the rest are
            // route-shape errors a typo would produce.
            info!("[peer-fetch] rejected {}/{}: {}", vol_id_str, filename, e);
        }
    }
    result.map(|(_, _, bytes)| {
        Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .header(http::header::CONTENT_LENGTH, bytes.len())
            .body(Body::from(bytes))
            .expect("response builder accepts well-formed parts")
    })
}

async fn handle_segment_inner(
    ctx: &ServerContext,
    vol_id_str: &str,
    filename: &str,
    headers: &HeaderMap,
) -> Result<(ResourceKind, ResourceTarget, Vec<u8>), RouteError> {
    // Path parsing is cheap; do it before the auth round-trip so a
    // bad URL doesn't burn S3 lookups.
    let vol_id = Ulid::from_string(vol_id_str).map_err(|_| RouteError::BadVolId)?;

    // Per-fork skeleton routes have no ULID component — match by
    // literal filename first.
    let (kind, target) = match filename {
        "volume.pub" => (ResourceKind::VolumePub, ResourceTarget::Skeleton),
        "volume.provenance" => (ResourceKind::VolumeProvenance, ResourceTarget::Skeleton),
        _ => {
            let (ulid_str, kind) = if let Some(s) = filename.strip_suffix(".idx") {
                (s, ResourceKind::Idx)
            } else if let Some(s) = filename.strip_suffix(".prefetch") {
                (s, ResourceKind::Prefetch)
            } else if let Some(s) = filename.strip_suffix(".manifest") {
                (s, ResourceKind::SnapshotManifest)
            } else {
                return Err(RouteError::UnknownFilename);
            };
            let ulid = Ulid::from_string(ulid_str).map_err(|_| RouteError::BadUlid)?;
            (kind, ResourceTarget::Ulid(ulid))
        }
    };

    // Auth pipeline. Mode is route-specific: skeleton routes skip
    // the lineage walk so the claim-time chain pull authenticates
    // before the requester's own provenance is published; payload
    // routes keep the full check.
    let mode = kind.auth_mode();
    let bearer_header = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let bearer = parse_bearer(bearer_header).map_err(RouteError::Auth)?;
    let authorized = ctx
        .auth
        .verify(bearer, vol_id, mode)
        .await
        .map_err(RouteError::Auth)?;

    // Audit log: every authenticated request, with mode visible.
    // Operator surface for spotting anomalies (e.g. one coord
    // hitting many unrelated `vol_id`s on skeleton routes — the
    // route relaxation's predicted abuse shape).
    info!(
        "[peer-fetch] authorized: requester {coord_id} for volume {volume_name} → {url_vol_id}/{filename} ({mode:?})",
        coord_id = authorized.coordinator_id,
        volume_name = authorized.volume_name,
        url_vol_id = vol_id,
        filename = kind.wire_suffix(),
    );

    // Step 5: local stat of the route's file. Skeleton files sit
    // directly under `by_id/<vol_id>/`; segment/snapshot files sit
    // under a per-kind subdirectory.
    let mut path = ctx.data_dir.join("by_id").join(vol_id.to_string());
    if let Some(subdir) = kind.local_subdir() {
        path.push(subdir);
    }
    path.push(kind.local_filename(target));

    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(RouteError::NotFound),
        Err(e) => return Err(RouteError::Io(e)),
    };

    Ok((kind, target, bytes))
}

/// Handle a `GET /v1/<vol_id>/<seg_ulid>.body` request.
///
/// Auth: volume-signed bearer token verified against
/// `by_id/<token.vol_ulid>/volume.pub` plus a lineage check that
/// `<vol_id>` is in the signing volume's signed ancestry.
///
/// Coverage:
/// - Every overlapping Data entry present → `200 OK`, full body
///   bytes for the requested range.
/// - First overlapping Data entry missing → `404 Not Found`; the
///   client falls through to S3.
/// - Some overlapping Data entries present, then a missing one →
///   `206 Partial Content` with `Content-Range: bytes
///   <start>-<start+prefix-1>/*` and the maximal contiguous
///   prefix bytes. The client splices the prefix with an S3 range
///   GET for the remainder.
///
/// Range header: required, single-range only, `bytes=<start>-<end>`
/// inclusive. Anything else returns 416.
async fn handle_body(
    ctx: &ServerContext,
    vol_id_str: &str,
    seg_str: &str,
    headers: &HeaderMap,
) -> Result<Response, RouteError> {
    let vol_id = Ulid::from_string(vol_id_str).map_err(|_| RouteError::BadVolId)?;
    let seg_ulid = Ulid::from_string(seg_str).map_err(|_| RouteError::BadUlid)?;

    let bearer_header = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let bearer = parse_bearer(bearer_header).map_err(RouteError::Auth)?;
    ctx.auth
        .verify_body_token(bearer, vol_id)
        .await
        .map_err(RouteError::Auth)?;

    let range_header = headers
        .get(http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .ok_or(RouteError::BadRange)?;
    let (range_start, range_end_inclusive) =
        parse_single_byte_range(range_header).ok_or(RouteError::BadRange)?;
    if range_end_inclusive < range_start {
        return Err(RouteError::BadRange);
    }
    let range_len = range_end_inclusive - range_start + 1;

    let by_id_dir = ctx.data_dir.join("by_id").join(vol_id.to_string());
    let body_path = by_id_dir.join("cache").join(format!("{seg_ulid}.body"));
    let present_path = by_id_dir.join("cache").join(format!("{seg_ulid}.present"));
    let idx_path = by_id_dir.join("index").join(format!("{seg_ulid}.idx"));

    // Local-file presence is the equivalent of step 5 from the
    // index-class auth pipeline. Any of these missing → 404, treating
    // the peer as opportunistic. Don't distinguish between the
    // various missing-file cases: from the caller's POV this segment
    // is simply not peer-fetchable.
    let body_exists = tokio::fs::try_exists(&body_path).await.unwrap_or(false);
    let idx_exists = tokio::fs::try_exists(&idx_path).await.unwrap_or(false);
    if !body_exists || !idx_exists {
        return Err(RouteError::NotFound);
    }

    // Coverage check + bytes read happen on a blocking thread: both
    // are sync ops that read smallish files (`.idx` typically < 100
    // KB, `.present` a handful of bytes, body file is sparse and
    // pread-friendly).
    let body_path_owned = body_path.clone();
    let idx_path_owned = idx_path.clone();
    let present_path_owned = present_path.clone();
    let (bytes, covered) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, u64), RouteError> {
            let covered =
                body_covered_prefix(&idx_path_owned, &present_path_owned, range_start, range_len)
                    .map_err(RouteError::Io)?;
            if covered == 0 {
                return Err(RouteError::NotFound);
            }
            let bytes =
                read_body_range(&body_path_owned, range_start, covered).map_err(RouteError::Io)?;
            Ok((bytes, covered))
        })
        .await
        .map_err(|e| RouteError::Io(std::io::Error::other(format!("blocking task: {e}"))))??;

    let status = if covered == range_len {
        StatusCode::OK
    } else {
        StatusCode::PARTIAL_CONTENT
    };
    let served_end_inclusive = range_start + covered - 1;
    info!(
        "[peer-fetch] served .body {}/{} ({} bytes, status {}, requested bytes={}-{}, served bytes={}-{})",
        vol_id_str,
        seg_ulid,
        bytes.len(),
        status.as_u16(),
        range_start,
        range_end_inclusive,
        range_start,
        served_end_inclusive,
    );

    Ok(Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header(http::header::CONTENT_LENGTH, bytes.len())
        .header(
            http::header::CONTENT_RANGE,
            format!("bytes {}-{}/*", range_start, served_end_inclusive),
        )
        .body(Body::from(bytes))
        .expect("response builder accepts well-formed parts"))
}

/// Parse a single-range `bytes=<start>-<end>` Range header. Returns
/// the inclusive bounds, or `None` if the header is malformed,
/// multi-range, or uses suffix / open-ended forms (which v1.1 does
/// not support).
fn parse_single_byte_range(header: &str) -> Option<(u64, u64)> {
    let rest = header.strip_prefix("bytes=")?;
    if rest.contains(',') {
        return None;
    }
    let (start_s, end_s) = rest.split_once('-')?;
    let start = start_s.parse::<u64>().ok()?;
    let end = end_s.parse::<u64>().ok()?;
    Some((start, end))
}

/// Compute the maximal contiguous covered prefix of
/// `[range_start, range_start + range_len)` for which every overlapping
/// Data entry has its `.present` bit set.
///
/// Returns the prefix length in bytes:
/// - `range_len` when every overlapping Data entry is present (full
///   coverage; the route serves 200).
/// - A smaller positive value when an overlapping Data entry is
///   missing — the prefix runs up to the start of that missing entry,
///   clamped to `range_start` (the route serves 206 with
///   `Content-Range: bytes <start>-<start+prefix-1>/*`).
/// - `0` when the first overlapping Data entry is already missing —
///   the route serves 404 and the client falls through to S3.
///
/// DedupRef, Inline, and Zero entries are skipped — they don't
/// contribute body bytes. A range falling on a body region not
/// covered by any Data entry returns the full `range_len` (the
/// underlying pread reads sparse-hole zeros), but in practice the
/// body layout is contiguous Data entries so this case doesn't arise
/// on well-formed requests.
///
/// `.present` is read once into memory rather than reopened per
/// entry; the file is one byte per eight entries, so a kilobyte
/// covers a segment with 8000 extents.
fn body_covered_prefix(
    idx_path: &std::path::Path,
    present_path: &std::path::Path,
    range_start: u64,
    range_len: u64,
) -> std::io::Result<u64> {
    let (_body_section_start, entries, _inputs) = read_segment_index(idx_path)?;
    let present = read_present_bitmap(present_path)?;
    let request_end = range_start + range_len;

    for (idx, entry) in entries.iter().enumerate() {
        if entry.kind != SegEntryKind::Data {
            continue;
        }
        let entry_start = entry.stored_offset;
        let entry_end = entry_start + entry.stored_length as u64;
        if entry_end <= range_start || entry_start >= request_end {
            continue;
        }
        if !present_bit_in_buf(&present, idx as u32) {
            // Maximal prefix runs up to the start of the missing
            // entry, clamped to range_start so a missing entry that
            // begins before the request still yields a 0-length
            // prefix rather than underflowing.
            let prefix_end = entry_start.max(range_start);
            return Ok(prefix_end - range_start);
        }
    }
    Ok(range_len)
}

/// Read the `.present` bitmap into a `Vec<u8>`. Missing file is
/// treated as an all-zero bitmap (every entry not present).
fn read_present_bitmap(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

fn present_bit_in_buf(buf: &[u8], entry_idx: u32) -> bool {
    let byte_idx = (entry_idx / 8) as usize;
    let bit = entry_idx % 8;
    buf.get(byte_idx).is_some_and(|b| b & (1 << bit) != 0)
}

fn read_body_range(
    body_path: &std::path::Path,
    range_start: u64,
    range_len: u64,
) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(body_path)?;
    let mut buf = vec![0u8; range_len as usize];
    f.read_exact_at(&mut buf, range_start)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use bytes::Bytes;
    use ed25519_dalek::{Signer, SigningKey};
    use elide_core::name_record::{NameRecord, NameState};
    use elide_core::signing::{ProvenanceLineage, write_provenance};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path as StorePath;
    use rand_core::OsRng;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use crate::token::PeerFetchToken;

    fn pub_hex(key: &SigningKey) -> String {
        let bytes = key.verifying_key().to_bytes();
        let mut s = String::with_capacity(64);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s.push('\n');
        s
    }

    fn sign_token(
        volume_name: &str,
        coordinator_id: &str,
        issued_at: u64,
        key: &SigningKey,
    ) -> PeerFetchToken {
        let payload = PeerFetchToken::signing_payload(volume_name, coordinator_id, issued_at);
        let sig = key.sign(&payload).to_bytes();
        PeerFetchToken {
            volume_name: volume_name.to_owned(),
            coordinator_id: coordinator_id.to_owned(),
            issued_at,
            signature: sig,
        }
    }

    /// Set up: in-memory store + tmpdir data_dir, one volume + name
    /// claimed by one coordinator. Returns a wired Router and the
    /// pieces tests need to mint requests.
    struct Fixture {
        router: Router,
        data_dir: TempDir,
        coord_key: SigningKey,
        coord_id: String,
        vol_ulid: Ulid,
        vol_name: String,
    }

    async fn fixture() -> Fixture {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let auth = AuthState::new(store.clone());
        let data_dir = TempDir::new().unwrap();
        let coord_key = SigningKey::generate(&mut OsRng);
        let vol_key = SigningKey::generate(&mut OsRng);
        let coord_id = "coord-a".to_owned();
        let vol_name = "myvol".to_owned();
        let vol_ulid = Ulid::new();

        // Publish coordinator.pub
        store
            .put(
                &StorePath::from(format!("coordinators/{coord_id}/coordinator.pub")),
                Bytes::from(pub_hex(&coord_key).into_bytes()).into(),
            )
            .await
            .unwrap();

        // Publish volume.pub + provenance via the canonical writer.
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("volume.pub"), pub_hex(&vol_key)).unwrap();
        write_provenance(
            tmp.path(),
            &vol_key,
            "volume.provenance",
            &ProvenanceLineage::default(),
        )
        .unwrap();
        let pub_bytes = std::fs::read(tmp.path().join("volume.pub")).unwrap();
        let prov_bytes = std::fs::read(tmp.path().join("volume.provenance")).unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_ulid}/volume.pub")),
                Bytes::from(pub_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_ulid}/volume.provenance")),
                Bytes::from(prov_bytes).into(),
            )
            .await
            .unwrap();

        // Publish names/<vol_name>
        let mut record = NameRecord::live_minimal(vol_ulid, 4 * 1024 * 1024 * 1024);
        record.coordinator_id = Some(coord_id.clone());
        record.state = NameState::Live;
        store
            .put(
                &StorePath::from(format!("names/{vol_name}")),
                Bytes::from(record.to_toml().unwrap().into_bytes()).into(),
            )
            .await
            .unwrap();

        let ctx = ServerContext::new(auth, data_dir.path().to_owned());
        let router = router(ctx);

        Fixture {
            router,
            data_dir,
            coord_key,
            coord_id,
            vol_ulid,
            vol_name,
        }
    }

    fn write_local_file(
        data_dir: &std::path::Path,
        vol_ulid: Ulid,
        subdir: &str,
        filename: &str,
        body: &[u8],
    ) {
        let dir = data_dir
            .join("by_id")
            .join(vol_ulid.to_string())
            .join(subdir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(filename), body).unwrap();
    }

    fn bearer_header(token: &PeerFetchToken) -> String {
        format!("Bearer {}", token.encode())
    }

    async fn run(router: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
        let (status, _, body) = run_full(router, req).await;
        (status, body)
    }

    /// Like [`run`] but also returns the response headers. Used by the
    /// `.body` route tests that need to inspect `Content-Range` to
    /// distinguish 206 partial-coverage from 200 full-coverage.
    async fn run_full(
        router: &Router,
        req: Request<Body>,
    ) -> (StatusCode, http::HeaderMap, Vec<u8>) {
        let response = router.clone().oneshot(req).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body_bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (status, headers, body_bytes.to_vec())
    }

    #[tokio::test]
    async fn idx_happy_path_returns_local_bytes() {
        let f = fixture().await;
        let segment_ulid = Ulid::new();
        let body = b"the quick brown fox\n";
        write_local_file(
            f.data_dir.path(),
            f.vol_ulid,
            "index",
            &format!("{segment_ulid}.idx"),
            body,
        );

        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.idx", f.vol_ulid, segment_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, returned) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(returned, body);
    }

    #[tokio::test]
    async fn prefetch_happy_path_serves_present_file() {
        let f = fixture().await;
        let segment_ulid = Ulid::new();
        let body = b"\xff\xff\x00\x01"; // arbitrary bitmap-shaped bytes
        write_local_file(
            f.data_dir.path(),
            f.vol_ulid,
            "cache",
            &format!("{segment_ulid}.present"),
            body,
        );

        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.prefetch", f.vol_ulid, segment_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, returned) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(returned, body);
    }

    #[tokio::test]
    async fn missing_local_file_returns_404() {
        let f = fixture().await;
        let segment_ulid = Ulid::new();
        // Deliberately do not write the file.

        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.idx", f.vol_ulid, segment_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn missing_authorization_header_is_401() {
        let f = fixture().await;
        let segment_ulid = Ulid::new();
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.idx", f.vol_ulid, segment_ulid))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn unknown_filename_suffix_is_404() {
        let f = fixture().await;
        let segment_ulid = Ulid::new();
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            // `.bogus` isn't a recognised suffix; v1.1 added `.body`
            // so use something that'll never match.
            .uri(format!("/v1/{}/{}.bogus", f.vol_ulid, segment_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn bad_vol_id_is_404() {
        let f = fixture().await;
        let segment_ulid = Ulid::new();
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/not-a-ulid/{}.idx", segment_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn snapshot_manifest_happy_path() {
        let f = fixture().await;
        let snap_ulid = Ulid::new();
        let body = b"signed manifest payload";
        write_local_file(
            f.data_dir.path(),
            f.vol_ulid,
            "snapshots",
            &format!("{snap_ulid}.manifest"),
            body,
        );

        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.manifest", f.vol_ulid, snap_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, returned) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(returned, body);
    }

    /// Write a per-fork file directly under `by_id/<vol_id>/`
    /// (no subdirectory). For `volume.pub` / `volume.provenance`.
    fn write_local_fork_file(
        data_dir: &std::path::Path,
        vol_ulid: Ulid,
        filename: &str,
        body: &[u8],
    ) {
        let dir = data_dir.join("by_id").join(vol_ulid.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(filename), body).unwrap();
    }

    #[tokio::test]
    async fn volume_pub_happy_path_returns_local_bytes() {
        let f = fixture().await;
        let body = b"abcdef0123456789\n";
        write_local_fork_file(f.data_dir.path(), f.vol_ulid, "volume.pub", body);

        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/volume.pub", f.vol_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, returned) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(returned, body);
    }

    #[tokio::test]
    async fn volume_provenance_happy_path_returns_local_bytes() {
        let f = fixture().await;
        let body = b"toml-ish provenance bytes";
        write_local_fork_file(f.data_dir.path(), f.vol_ulid, "volume.provenance", body);

        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/volume.provenance", f.vol_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, returned) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(returned, body);
    }

    #[tokio::test]
    async fn volume_pub_missing_local_file_returns_404() {
        let f = fixture().await;
        // Deliberately do not write the local file. The S3 fixture has
        // it published, but the route serves from the local `data_dir`
        // — and the peer is opportunistic, so a local miss is a 404.
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/volume.pub", f.vol_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn volume_pub_outside_lineage_is_404_not_403() {
        // Skeleton routes (`volume.pub`, `volume.provenance`) run
        // under `RouteAuthMode::SkeletonsOnly` — the lineage walk
        // is intentionally skipped so a claim-time chain pull can
        // authenticate before the requester's own provenance is
        // published. A request for an unrelated `vol_id` therefore
        // passes auth and reaches the local-stat step, which 404s
        // when the file isn't on disk.
        //
        // The lineage gate still applies to payload routes — see
        // `vol_id_outside_lineage_is_403` for the `.idx` case.
        let f = fixture().await;
        let unrelated_ulid = Ulid::new();
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/volume.pub", unrelated_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn volume_provenance_outside_lineage_is_404_not_403() {
        // Sibling test: same relaxation applies to volume.provenance.
        let f = fixture().await;
        let unrelated_ulid = Ulid::new();
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/volume.provenance", unrelated_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn snapshot_manifest_outside_lineage_is_404_not_403() {
        // `.manifest` runs under `RouteAuthMode::SkeletonsOnly` so that
        // `skip_empty_intermediates` (and any other claim-time chain
        // walker) can read each ancestor's signed manifest before the
        // requester's own `volume.provenance` has been published — the
        // requester's parent isn't even known until those manifests
        // have been read. Manifests are signed and the caller verifies
        // before trusting bytes, mirroring the `volume.pub` /
        // `volume.provenance` relaxation.
        let f = fixture().await;
        let unrelated_ulid = Ulid::new();
        let snap_ulid = Ulid::new();
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.manifest", unrelated_ulid, snap_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn vol_id_outside_lineage_is_403() {
        let f = fixture().await;
        let unrelated_ulid = Ulid::new();
        let segment_ulid = Ulid::new();
        let token = sign_token(
            &f.vol_name,
            &f.coord_id,
            PeerFetchToken::now_unix_seconds(),
            &f.coord_key,
        );
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.idx", unrelated_ulid, segment_ulid))
            .header(http::header::AUTHORIZATION, bearer_header(&token))
            .body(Body::empty())
            .unwrap();

        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    // --- .body route tests ---

    use crate::body_token::BodyFetchToken;
    use elide_core::segment::{
        SegmentEntry, SegmentFlags, SegmentSigner, promote_to_cache, set_present_bit, write_segment,
    };
    use std::path::Path as StdPath;

    /// Test-only signer wrapping a `SigningKey`. The test code can't
    /// use `elide_core::signing::Ed25519Signer` directly (its
    /// constructor is private), so we re-implement the trait locally.
    struct TestSegSigner(SigningKey);
    impl SegmentSigner for TestSegSigner {
        fn sign(&self, msg: &[u8]) -> [u8; 64] {
            self.0.sign(msg).to_bytes()
        }
    }

    /// Build a real segment with `entries`, signed by `signing_key`,
    /// and split it into `index/<seg>.idx` + `cache/<seg>.body` +
    /// `cache/<seg>.present` under the data_dir's by_id/<vol_ulid>/
    /// tree. Mark every Data entry as present (bit set in `.present`).
    fn write_segment_files(
        data_dir: &StdPath,
        vol_ulid: Ulid,
        seg_ulid: Ulid,
        mut entries: Vec<SegmentEntry>,
        signing_key: &SigningKey,
    ) {
        let by_id = data_dir.join("by_id").join(vol_ulid.to_string());
        let cache = by_id.join("cache");
        let index = by_id.join("index");
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::create_dir_all(&index).unwrap();

        // Stage the full segment file via write_segment, then split
        // into its on-disk pieces (the same way promote does in
        // production).
        let staging = data_dir.join(format!("staging_{seg_ulid}"));
        let signer = TestSegSigner(signing_key.clone());
        write_segment(&staging, &mut entries, &signer).unwrap();

        // Generate cache/<seg>.body + cache/<seg>.present.
        let body_path = cache.join(format!("{seg_ulid}.body"));
        let present_path = cache.join(format!("{seg_ulid}.present"));
        promote_to_cache(&staging, &body_path, &present_path).unwrap();

        // Set every Data entry's present bit. (promote_to_cache
        // doesn't write the .present unless it allocates body bytes —
        // and even when it does, bits are zeroed; the volume sets
        // them on its own demand-fetch path. Tests bake the "fully
        // populated" state in directly.)
        let entry_count = entries.len() as u32;
        for (idx, entry) in entries.iter().enumerate() {
            if entry.kind == SegEntryKind::Data {
                set_present_bit(&present_path, idx as u32, entry_count).unwrap();
            }
        }

        // Write index/<seg>.idx — the [0..body_section_start) prefix
        // of the staged segment.
        let seg_bytes = std::fs::read(&staging).unwrap();
        let index_length =
            u32::from_le_bytes([seg_bytes[12], seg_bytes[13], seg_bytes[14], seg_bytes[15]]);
        let inline_length =
            u32::from_le_bytes([seg_bytes[16], seg_bytes[17], seg_bytes[18], seg_bytes[19]]);
        let body_section_start = 100 + index_length as usize + inline_length as usize;
        let idx_path = index.join(format!("{seg_ulid}.idx"));
        std::fs::write(&idx_path, &seg_bytes[..body_section_start]).unwrap();
    }

    fn sign_body_token(vol_ulid: Ulid, key: &SigningKey) -> BodyFetchToken {
        let issued = BodyFetchToken::now_unix_seconds();
        let payload = BodyFetchToken::signing_payload(vol_ulid, issued);
        let sig = key.sign(&payload).to_bytes();
        BodyFetchToken {
            vol_ulid,
            issued_at: issued,
            signature: sig,
        }
    }

    fn body_bearer(token: &BodyFetchToken) -> String {
        format!("Bearer {}", token.encode())
    }

    /// Volume-side fixture: in-memory store, tmp data_dir, one
    /// volume's `volume.pub` published, the volume's signing key
    /// retained for token minting. No coordinator/name records — body
    /// auth doesn't need them.
    struct BodyFixture {
        router: Router,
        data_dir: TempDir,
        vol_key: SigningKey,
        vol_ulid: Ulid,
    }

    async fn body_fixture() -> BodyFixture {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let auth = AuthState::new(store.clone());
        let data_dir = TempDir::new().unwrap();
        let vol_key = SigningKey::generate(&mut OsRng);
        let vol_ulid = Ulid::new();

        // Publish volume.pub + provenance under by_id/<vol_ulid>/.
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join("volume.pub"), pub_hex(&vol_key)).unwrap();
        write_provenance(
            tmp.path(),
            &vol_key,
            "volume.provenance",
            &ProvenanceLineage::default(),
        )
        .unwrap();
        let pub_bytes = std::fs::read(tmp.path().join("volume.pub")).unwrap();
        let prov_bytes = std::fs::read(tmp.path().join("volume.provenance")).unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_ulid}/volume.pub")),
                Bytes::from(pub_bytes).into(),
            )
            .await
            .unwrap();
        store
            .put(
                &StorePath::from(format!("by_id/{vol_ulid}/volume.provenance")),
                Bytes::from(prov_bytes).into(),
            )
            .await
            .unwrap();

        let ctx = ServerContext::new(auth, data_dir.path().to_owned());
        let router = router(ctx);

        BodyFixture {
            router,
            data_dir,
            vol_key,
            vol_ulid,
        }
    }

    #[tokio::test]
    async fn body_happy_path_returns_extent_bytes() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let payload = vec![0xABu8; 4096];
        let hash = blake3::hash(&payload);
        let entry = SegmentEntry::new_data(hash, 0, 1, SegmentFlags::empty(), payload.clone());
        write_segment_files(
            f.data_dir.path(),
            f.vol_ulid,
            seg_ulid,
            vec![entry],
            &f.vol_key,
        );

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .header(http::header::RANGE, "bytes=0-4095")
            .body(Body::empty())
            .unwrap();
        let (status, body) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.len(), 4096);
        assert_eq!(body, payload);
    }

    #[tokio::test]
    async fn body_missing_range_header_is_416() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let payload = vec![0u8; 4096];
        let hash = blake3::hash(&payload);
        let entry = SegmentEntry::new_data(hash, 0, 1, SegmentFlags::empty(), payload);
        write_segment_files(
            f.data_dir.path(),
            f.vol_ulid,
            seg_ulid,
            vec![entry],
            &f.vol_key,
        );

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test]
    async fn body_malformed_range_is_416() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let payload = vec![0u8; 4096];
        let hash = blake3::hash(&payload);
        let entry = SegmentEntry::new_data(hash, 0, 1, SegmentFlags::empty(), payload);
        write_segment_files(
            f.data_dir.path(),
            f.vol_ulid,
            seg_ulid,
            vec![entry],
            &f.vol_key,
        );

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        for bad in [
            "garbage",
            "bytes=0",
            "bytes=-100",
            "bytes=0-",
            "bytes=10-5",         // end < start
            "bytes=0-99,200-299", // multi-range
        ] {
            let req = Request::builder()
                .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
                .header(http::header::AUTHORIZATION, body_bearer(&token))
                .header(http::header::RANGE, bad)
                .body(Body::empty())
                .unwrap();
            let (status, _) = run(&f.router, req).await;
            assert_eq!(
                status,
                StatusCode::RANGE_NOT_SATISFIABLE,
                "Range header `{bad}` should be 416"
            );
        }
    }

    #[tokio::test]
    async fn body_missing_local_file_returns_404() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new(); // no files written

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .header(http::header::RANGE, "bytes=0-99")
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// `.body` exists but the `.present` bit for the requested range
    /// is unset → the peer treats it as a miss (404). The caller
    /// falls through to S3.
    #[tokio::test]
    async fn body_present_bit_unset_returns_404() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let payload = vec![0xCDu8; 4096];
        let hash = blake3::hash(&payload);
        let entry = SegmentEntry::new_data(hash, 0, 1, SegmentFlags::empty(), payload);
        // Build segment files with the present bit set, then clear it
        // for this test.
        write_segment_files(
            f.data_dir.path(),
            f.vol_ulid,
            seg_ulid,
            vec![entry],
            &f.vol_key,
        );
        // Truncate .present to all-zero bits.
        let present_path = f
            .data_dir
            .path()
            .join("by_id")
            .join(f.vol_ulid.to_string())
            .join("cache")
            .join(format!("{seg_ulid}.present"));
        std::fs::write(&present_path, [0u8; 1]).unwrap();

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .header(http::header::RANGE, "bytes=0-4095")
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn body_missing_authorization_is_401() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::RANGE, "bytes=0-99")
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn body_outside_lineage_is_403() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let unrelated = Ulid::new();
        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", unrelated, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .header(http::header::RANGE, "bytes=0-99")
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// Two Data entries, second entry's `.present` bit cleared.
    /// Requesting the full body range produces a 206 with the maximal
    /// covered prefix (the first entry's bytes); `Content-Range`
    /// reports `bytes 0-(prefix-1)/*`. The client splices the prefix
    /// with an S3 fetch for the remainder.
    #[tokio::test]
    async fn body_partial_coverage_returns_206_with_content_range() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let first = vec![0xAAu8; 4096];
        let second = vec![0xBBu8; 4096];
        let entries = vec![
            SegmentEntry::new_data(
                blake3::hash(&first),
                0,
                1,
                SegmentFlags::empty(),
                first.clone(),
            ),
            SegmentEntry::new_data(blake3::hash(&second), 1, 1, SegmentFlags::empty(), second),
        ];
        write_segment_files(f.data_dir.path(), f.vol_ulid, seg_ulid, entries, &f.vol_key);
        // Clear bit 1 (the second Data entry) while keeping bit 0
        // (the first) set. With two entries the bitmap is one byte.
        let present_path = f
            .data_dir
            .path()
            .join("by_id")
            .join(f.vol_ulid.to_string())
            .join("cache")
            .join(format!("{seg_ulid}.present"));
        std::fs::write(&present_path, [0b0000_0001u8]).unwrap();

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .header(http::header::RANGE, "bytes=0-8191")
            .body(Body::empty())
            .unwrap();
        let (status, headers, body) = run_full(&f.router, req).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(body.len(), 4096, "should serve only the first entry");
        assert_eq!(body, first);
        assert_eq!(
            headers.get(http::header::CONTENT_RANGE).unwrap(),
            "bytes 0-4095/*"
        );
    }

    /// First overlapping Data entry's `.present` bit is cleared → 404,
    /// because the maximal contiguous prefix is empty and there's
    /// nothing useful for the client to splice. The route must NOT
    /// emit a 206 with zero bytes.
    #[tokio::test]
    async fn body_first_entry_missing_returns_404_not_empty_206() {
        let f = body_fixture().await;
        let seg_ulid = Ulid::new();
        let first = vec![0xAAu8; 4096];
        let second = vec![0xBBu8; 4096];
        let entries = vec![
            SegmentEntry::new_data(blake3::hash(&first), 0, 1, SegmentFlags::empty(), first),
            SegmentEntry::new_data(blake3::hash(&second), 1, 1, SegmentFlags::empty(), second),
        ];
        write_segment_files(f.data_dir.path(), f.vol_ulid, seg_ulid, entries, &f.vol_key);
        // Clear bit 0 (first entry); keep bit 1 (second entry) set.
        // The client can't use a 206 starting after a hole — fall
        // through to S3 cleanly.
        let present_path = f
            .data_dir
            .path()
            .join("by_id")
            .join(f.vol_ulid.to_string())
            .join("cache")
            .join(format!("{seg_ulid}.present"));
        std::fs::write(&present_path, [0b0000_0010u8]).unwrap();

        let token = sign_body_token(f.vol_ulid, &f.vol_key);
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(http::header::AUTHORIZATION, body_bearer(&token))
            .header(http::header::RANGE, "bytes=0-8191")
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// A coordinator-flavour token sent at the `.body` route fails
    /// the body-token decode (different wire shape) → 401 BadToken.
    #[tokio::test]
    async fn body_route_rejects_coord_token() {
        let f = body_fixture().await;
        // Mint a coordinator-flavour token signed by the vol_key (so
        // it's a valid signature against *something*); the body
        // decoder still rejects on wire shape.
        let coord_token = sign_token(
            "any-vol",
            "any-coord",
            PeerFetchToken::now_unix_seconds(),
            &f.vol_key,
        );
        let seg_ulid = Ulid::new();
        let req = Request::builder()
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, seg_ulid))
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer {}", coord_token.encode()),
            )
            .header(http::header::RANGE, "bytes=0-99")
            .body(Body::empty())
            .unwrap();
        let (status, _) = run(&f.router, req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
