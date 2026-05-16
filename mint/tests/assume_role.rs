//! End-to-end: macaroon -> HTTP -> role gate -> policy render -> faked
//! keypair. Exercises the whole vertical slice without a live Tigris.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mint::audit::AuditLog;
use mint::caveat::Caveat;
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::macaroon::mint;
use tower::ServiceExt;

const ROOT: [u8; 32] = [42u8; 32];

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
required_caveats = ["elide:Volume", "elide:Ancestors", "Audience", "NotAfter"]
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
      {{#each (caveat "elide:Ancestors")}},
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

fn good_macaroon() -> String {
    mint(
        &ROOT,
        vec![
            Caveat::scalar("Audience", "mint"),
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::list("elide:Ancestors", ["ANC1", "ANC2"]),
            Caveat::scalar("NotAfter", far_future().to_string()),
        ],
    )
    .encode()
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

    let req = Request::builder()
        .method("POST")
        .uri("/v1/assume-role")
        .header("authorization", format!("Macaroon {}", good_macaroon()))
        .header("content-type", "application/json")
        .body(Body::from(r#"{"role":"volume-ro","ttl_seconds":3600}"#))
        .unwrap();

    let (status, body) = body_string(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert!(body.contains("tid_fake_00000000"), "body: {body}");

    // The faked minter saw the rendered, volume-scoped policy.
    let calls = minter.calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].policy_json.contains("demo-bucket/by_id/VOL1/*"));
    assert!(calls[0].policy_json.contains("by_id/ANC1/*"));
    assert!(calls[0].policy_json.contains("by_id/ANC2/*"));

    let audit = String::from_utf8(audit_buf.lock().unwrap().clone()).unwrap();
    assert!(audit.contains("\"outcome\":\"granted\""), "audit: {audit}");
}

#[tokio::test]
async fn bad_mac_is_opaque_401() {
    let (state, _, _) = state_with_audit();
    let app = router(state);

    // Mint under a different root -> MAC won't verify.
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

    // Valid MAC, but no elide:Volume / elide:Ancestors.
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
