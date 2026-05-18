//! End-to-end: a primary (op=assume-role) + holder-of-key PoP -> HTTP
//! -> op gate -> role gate -> policy render -> faked keypair. The whole
//! vertical slice without a live Tigris.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mint::audit::AuditLog;
use mint::caveat::{Caveat, name, op};
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::issuance::mint_primary;
use mint::macaroon::{Macaroon, mint};
use mint::pop;
use mint::state::Store;
use tower::ServiceExt;

mod common;

const ROOT: [u8; 32] = [42u8; 32];
/// Stands in for the coordinator's Ed25519 identity-key seed.
const COORD_SEED: [u8; 32] = [7u8; 32];
const SUB: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

fn config() -> Config {
    common::parse_config(TOML_TEMPLATE, &[("volume-ro.json", POLICY)])
}

const TOML_TEMPLATE: &str = r#"
audience = "mint"
[tenant]
bucket = "demo-bucket"
[[role]]
name = "volume-ro"
required_caveats = ["elide:Volume", "aud", "exp"]
min_ttl_seconds = 60
max_ttl_seconds = 2592000
default_ttl_seconds = 2592000
policy_file = "volume-ro.json"
"#;

const POLICY: &str = r#"
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat "elide:Volume"}}/*"
      {{#each request.ancestors}},
      "arn:aws:s3:::{{../tenant.bucket}}/by_id/{{this}}/*"
      {{/each}}
    ],
    "Condition": {"DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}}
  }]
}
"#;

#[derive(Clone)]
struct AuditSink(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for AuditSink {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("poisoned"))?
            .extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn state_with_audit() -> (
    AppState,
    Arc<Mutex<Vec<u8>>>,
    Arc<FakeMinter>,
    tempfile::TempDir,
) {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let minter = Arc::new(FakeMinter::new());
    let dir = tempfile::tempdir().expect("tempdir");
    // Seed the known root key (hex) so Store::open loads it (vs
    // generating one) and the macaroons minted with ROOT verify.
    let root_hex: String = ROOT.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(dir.path().join("root_key"), root_hex).expect("seed root_key");
    let state = AppState {
        config: Arc::new(config()),
        minter: minter.clone(),
        audit: Arc::new(AuditLog::new(Box::new(AuditSink(buf.clone())))),
        store: Arc::new(Store::open(dir.path()).expect("store")),
    };
    (state, buf, minter, dir)
}

fn far_future() -> u64 {
    (chrono::Utc::now().timestamp() as u64) + 365 * 24 * 3600
}

/// A held primary (op=assume-role, aud, sub, cnf) attenuated per
/// request with a tighter `exp` and an `elide:Volume`.
fn request_macaroon() -> Macaroon {
    mint_primary(&ROOT, "mint", SUB, &pop::cnf_value(&COORD_SEED))
        .attenuate(Caveat::scalar(name::EXP, far_future().to_string()))
        .attenuate(Caveat::scalar("elide:Volume", "VOL1"))
}

fn signed_request(m: &Macaroon, inner_fields: &str) -> Request<Body> {
    let ts = chrono::Utc::now().timestamp() as u64;
    let body = format!("{{\"ts\":{ts},{inner_fields}}}");
    let sig = pop::client_signature(&COORD_SEED, m.tail(), body.as_bytes());
    Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {}", m.encode()))
        .header("x-mint-coord-pop", sig)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn body_string(resp: axum::response::Response) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("collect body");
    (status, String::from_utf8(bytes.to_vec()).expect("utf8"))
}

#[tokio::test]
async fn happy_path_mints_scoped_keypair() {
    let (state, audit_buf, minter, _dir) = state_with_audit();
    let app = router(state);
    let m = request_macaroon();

    let req = signed_request(
        &m,
        r#""role":"volume-ro","ttl_seconds":3600,"ancestors":["ANC1","ANC2"]"#,
    );
    let (status, body) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(body.contains("tid_fake_00000000"), "body: {body}");

    let calls = minter.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].policy_json.contains("demo-bucket/by_id/VOL1/*"));
    assert!(calls[0].policy_json.contains("by_id/ANC1/*"));

    let audit = String::from_utf8(audit_buf.lock().unwrap().clone()).unwrap();
    assert!(audit.contains("\"outcome\":\"granted\""), "audit: {audit}");
}

#[tokio::test]
async fn wrong_op_is_opaque_401() {
    // A correctly key-bound, role-shaped macaroon but op=enroll instead
    // of assume-role: the positive op gate refuses it.
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let m = mint(
        &ROOT,
        vec![
            Caveat::scalar(name::OP, op::ENROLL),
            Caveat::scalar(name::AUD, "mint"),
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::scalar(name::EXP, far_future().to_string()),
            Caveat::scalar(name::CNF, pop::cnf_value(&COORD_SEED)),
        ],
    );
    let (status, _) = body_string(
        app.oneshot(signed_request(&m, r#""role":"volume-ro""#))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn key_bound_without_pop_is_opaque_401() {
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let m = request_macaroon();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {}", m.encode()))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"role":"volume-ro","ancestors":[]}"#))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn pop_over_a_different_body_is_401() {
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let m = request_macaroon();
    let ts = chrono::Utc::now().timestamp() as u64;
    let signed = format!(r#"{{"ts":{ts},"role":"volume-ro","ancestors":["ANC1"]}}"#);
    let sig = pop::client_signature(&COORD_SEED, m.tail(), signed.as_bytes());
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {}", m.encode()))
        .header("x-mint-coord-pop", sig)
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"ts":{ts},"role":"volume-ro","ancestors":["EVIL"]}}"#
        )))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn contradictory_cnf_fails_closed_not_bearer() {
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let m = request_macaroon().attenuate(Caveat::scalar(name::CNF, "ed25519:AAAA"));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {}", m.encode()))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"role":"volume-ro","ancestors":[]}"#))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bad_mac_is_opaque_401() {
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let forged = mint(&[1u8; 32], vec![Caveat::scalar(name::OP, op::ASSUME_ROLE)]).encode();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {forged}"))
        .body(Body::from(r#"{"role":"volume-ro"}"#))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn missing_required_caveat_is_400() {
    // op=assume-role + aud, a plain bearer (no cnf → PoP not required),
    // but no elide:Volume → the role gate denies with 400.
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let m = mint(
        &ROOT,
        vec![
            Caveat::scalar(name::OP, op::ASSUME_ROLE),
            Caveat::scalar(name::AUD, "mint"),
            Caveat::scalar(name::EXP, far_future().to_string()),
        ],
    )
    .encode();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {m}"))
        .body(Body::from(r#"{"role":"volume-ro"}"#))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn no_auth_header_is_401() {
    let (state, _, _, _dir) = state_with_audit();
    let app = router(state);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .body(Body::from(r#"{"role":"volume-ro"}"#))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
