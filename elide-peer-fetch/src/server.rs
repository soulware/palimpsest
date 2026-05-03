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
        let response = router.clone().oneshot(req).await.unwrap();
        let status = response.status();
        let body_bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (status, body_bytes.to_vec())
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
            .uri(format!("/v1/{}/{}.body", f.vol_ulid, segment_ulid))
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
}
