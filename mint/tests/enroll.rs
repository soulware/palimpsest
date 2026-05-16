//! End-to-end issuance: operator mints an enrollment token, the
//! coordinator exchanges it at `/v1/enroll` (proving possession of its
//! identity key), receives a non-expiring primary, then attenuates that
//! primary and successfully calls `/v1/assume-role` with it. Plus the
//! refusals that matter: expired token, wrong-key PoP, bearer token.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mint::audit::AuditLog;
use mint::caveat::{Caveat, EffectiveCaveats, Resolved};
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::issuance::{COORD_CAVEAT, NOT_AFTER_CAVEAT, mint_enrollment_token};
use mint::macaroon::Macaroon;
use mint::pop::{self, COORD_KEY_CAVEAT};
use tower::ServiceExt;

const ROOT: [u8; 32] = [42u8; 32];
/// Stands in for the coordinator's Ed25519 identity-key seed.
const COORD_SEED: [u8; 32] = [7u8; 32];
const COORD_ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

fn root_hex() -> String {
    ROOT.iter().map(|b| format!("{b:02x}")).collect()
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
    "Resource": ["arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat "elide:Volume"}}/*"],
    "Condition": {"DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}}
  }]
}
"""
"#;

fn config() -> Config {
    Config::from_toml_str(&TOML_TEMPLATE.replace("__ROOT__", &root_hex())).expect("config parses")
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

fn app() -> (axum::Router, Arc<Mutex<Vec<u8>>>) {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let state = AppState {
        config: Arc::new(config()),
        minter: Arc::new(FakeMinter::new()),
        audit: Arc::new(AuditLog::new(Box::new(AuditSink(buf.clone())))),
    };
    (router(state), buf)
}

fn now() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn far_future() -> u64 {
    now() + 365 * 24 * 3600
}

/// A PoP-signed request: body is `{"ts":<now>[,<extra>]}`, signature by
/// `seed` over the presented macaroon's tail and that exact body.
fn signed(uri: &str, m: &Macaroon, seed: &[u8; 32], extra: &str) -> Request<Body> {
    let body = format!("{{\"ts\":{}{extra}}}", now());
    let sig = pop::client_signature(seed, m.tail(), body.as_bytes());
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Macaroon {}", m.encode()))
        .header("x-mint-coord-pop", sig)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn parts(resp: axum::response::Response) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("collect body");
    (status, String::from_utf8(bytes.to_vec()).expect("utf8"))
}

fn primary_from(body: &str) -> Macaroon {
    let v: serde_json::Value = serde_json::from_str(body).expect("json");
    Macaroon::decode(v["primary"].as_str().expect("primary field")).expect("decode primary")
}

#[tokio::test]
async fn enroll_then_assume_role_full_flow() {
    let (app, audit) = app();

    // (1) Operator vouches for the coordinator: enrollment token bound
    // to its public identity, short-lived.
    let token = mint_enrollment_token(
        &ROOT,
        "mint",
        COORD_ULID,
        &pop::coord_key_value(&COORD_SEED),
        now() + 600,
    )
    .expect("mint enrollment token");

    // (2) Coordinator exchanges it, proving possession of its key.
    let (status, body) = parts(
        app.clone()
            .oneshot(signed("/v1/enroll", &token, &COORD_SEED, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");

    let primary = primary_from(&body);
    assert!(primary.verify(&ROOT), "primary must verify under the root");
    let eff = EffectiveCaveats::new(primary.caveats());
    assert_eq!(
        eff.resolve(COORD_CAVEAT),
        Resolved::Value(COORD_ULID.into())
    );
    assert_eq!(
        eff.resolve(COORD_KEY_CAVEAT),
        Resolved::Value(pop::coord_key_value(&COORD_SEED))
    );
    // The primary does not expire — the NotAfter scaffolding is gone.
    assert_eq!(eff.not_after(NOT_AFTER_CAVEAT), None);
    assert_eq!(primary.caveats().len(), 3);

    let a = String::from_utf8(audit.lock().unwrap().clone()).unwrap();
    assert!(a.contains("\"outcome\":\"enroll:granted\""), "audit: {a}");

    // (3) Coordinator attenuates the held primary per request (tighter
    // NotAfter + elide:Volume) and assumes a role with it.
    let req = primary
        .attenuate(Caveat::scalar("NotAfter", far_future().to_string()))
        .attenuate(Caveat::scalar("elide:Volume", "VOL1"));
    let (status, body) = parts(
        app.oneshot(signed(
            "/v1/assume-role",
            &req,
            &COORD_SEED,
            r#","role":"volume-ro","ttl_seconds":3600"#,
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "assume-role body: {body}");
    assert!(body.contains("tid_fake_00000000"), "body: {body}");
}

#[tokio::test]
async fn expired_enrollment_token_is_opaque_401() {
    let (app, _) = app();
    let token = mint_enrollment_token(
        &ROOT,
        "mint",
        COORD_ULID,
        &pop::coord_key_value(&COORD_SEED),
        now() - 1, // already past
    )
    .expect("mint");
    let (status, _) = parts(
        app.oneshot(signed("/v1/enroll", &token, &COORD_SEED, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn enroll_pop_by_wrong_key_is_opaque_401() {
    let (app, _) = app();
    // Token bound to COORD_SEED's pubkey, but signed by a different key:
    // a stolen enrollment token is inert without coordinator.key.
    let token = mint_enrollment_token(
        &ROOT,
        "mint",
        COORD_ULID,
        &pop::coord_key_value(&COORD_SEED),
        now() + 600,
    )
    .expect("mint");
    let (status, _) = parts(
        app.oneshot(signed("/v1/enroll", &token, &[9u8; 32], ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bearer_enrollment_token_is_opaque_401() {
    let (app, _) = app();
    // No elide:CoordKey: a captured copy would enrol. The exchange must
    // refuse a bearer enrollment token even with a valid MAC.
    let token = mint::macaroon::mint(
        &ROOT,
        vec![
            Caveat::scalar("Audience", "mint"),
            Caveat::scalar(COORD_CAVEAT, COORD_ULID),
            Caveat::scalar(NOT_AFTER_CAVEAT, (now() + 600).to_string()),
        ],
    );
    let body = format!(r#"{{"ts":{}}}"#, now());
    let req = Request::builder()
        .method("POST")
        .uri("/v1/enroll")
        .header("authorization", format!("Macaroon {}", token.encode()))
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let (status, _) = parts(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
