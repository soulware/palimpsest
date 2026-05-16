//! End-to-end: capability macaroon + holder-of-key PoP -> HTTP -> role
//! gate -> policy render -> faked keypair. Exercises the whole vertical
//! slice (incl. the elide:CoordKey PoP over the signed body) without a
//! live Tigris.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mint::audit::AuditLog;
use mint::caveat::Caveat;
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::macaroon::{Macaroon, mint};
use mint::pop;
use tower::ServiceExt;

const ROOT: [u8; 32] = [42u8; 32];
/// Stands in for the coordinator's Ed25519 identity-key seed.
const COORD_SEED: [u8; 32] = [7u8; 32];

fn root_hex() -> String {
    ROOT.iter().map(|b| format!("{b:02x}")).collect()
}

fn config() -> Config {
    let toml = TOML_TEMPLATE.replace("__ROOT__", &root_hex());
    Config::from_toml_str(&toml).expect("config parses")
}

const TOML_TEMPLATE: &str = r#"
audience = "mint"
trust_root_hex = "__ROOT__"
[tenant]
bucket = "demo-bucket"
[[role]]
name = "volume-ro"
required_caveats = ["elide:Volume", "Audience", "NotAfter"]
min_ttl_seconds = 60
max_ttl_seconds = 2592000
default_ttl_seconds = 2592000
policy = """
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
"""
"#;

fn state_with_audit() -> (AppState, Arc<Mutex<Vec<u8>>>, Arc<FakeMinter>) {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let sink = AuditSink(buf.clone());
    let minter = Arc::new(FakeMinter::new());
    let state = AppState {
        config: Arc::new(config()),
        minter: minter.clone(),
        audit: Arc::new(AuditLog::new(Box::new(sink))),
    };
    (state, buf, minter)
}

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

fn far_future() -> u64 {
    (chrono::Utc::now().timestamp() as u64) + 365 * 24 * 3600
}

/// A primary-shaped macaroon: capability caveats + the issuer-sealed
/// `elide:CoordKey` holder-of-key binding.
fn key_bound_macaroon() -> Macaroon {
    mint(
        &ROOT,
        vec![
            Caveat::scalar("Audience", "mint"),
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::scalar("NotAfter", far_future().to_string()),
            Caveat::scalar(pop::COORD_KEY_CAVEAT, pop::coord_key_value(&COORD_SEED)),
        ],
    )
}

/// Build a request whose body is `{"ts":<now>,<inner_fields>}`, signed
/// by the coordinator key. `ts` rides in the body (covered by the
/// signature); only the detached signature is a header.
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
    let (state, audit_buf, minter) = state_with_audit();
    let app = router(state);
    let m = key_bound_macaroon();

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
    assert!(calls[0].policy_json.contains("by_id/ANC2/*"));

    let audit = String::from_utf8(audit_buf.lock().unwrap().clone()).unwrap();
    assert!(audit.contains("\"outcome\":\"granted\""), "audit: {audit}");
}

#[tokio::test]
async fn key_bound_without_pop_is_opaque_401() {
    let (state, _, _) = state_with_audit();
    let app = router(state);
    let m = key_bound_macaroon();

    // Valid MAC, elide:CoordKey present, but no PoP headers.
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
    let (state, _, _) = state_with_audit();
    let app = router(state);
    let m = key_bound_macaroon();

    // Sign one body, send another (the request.* the policy renders
    // from must be exactly what was signed).
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
async fn contradictory_coordkey_fails_closed_not_bearer() {
    let (state, _, _) = state_with_audit();
    let app = router(state);

    // Capture a key-bound primary and append a second, bogus
    // elide:CoordKey (append needs only the trailing MAC). It must NOT
    // downgrade to a usable bearer — opaque 401.
    let m = key_bound_macaroon().attenuate(Caveat::scalar(pop::COORD_KEY_CAVEAT, "ed25519:AAAA"));

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
    let (state, _, _) = state_with_audit();
    let app = router(state);

    // Mint under a different root -> MAC won't verify (before PoP).
    let forged = mint(&[1u8; 32], vec![Caveat::scalar("Audience", "mint")]).encode();

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
    let (state, _, _) = state_with_audit();
    let app = router(state);

    // Valid MAC, no elide:CoordKey (a plain bearer — PoP not required),
    // and no elide:Volume -> role gate denies with 400.
    let m = mint(
        &ROOT,
        vec![
            Caveat::scalar("Audience", "mint"),
            Caveat::scalar("NotAfter", far_future().to_string()),
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
    let (state, _, _) = state_with_audit();
    let app = router(state);
    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .body(Body::from(r#"{"role":"volume-ro"}"#))
        .unwrap();
    let (status, _) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
