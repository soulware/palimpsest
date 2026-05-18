//! End-to-end enrollment (`docs/design-mint.md` § *Enrollment*):
//! reusable bootstrap macaroon → client self-asserts `sub`/`cnf` at
//! `POST /v1/enroll` (pending record + intermediate) → operator
//! approval → `POST /v1/enroll-exchange` (403 until approved, then the
//! non-expiring primary) → the primary attenuates and assumes a role.
//! Plus the refusals that matter: stale bootstrap, wrong-key PoP,
//! bearer (no cnf), no pending record, conflicting key for a `sub`.

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mint::audit::AuditLog;
use mint::caveat::{Caveat, EffectiveCaveats, Resolved, name, op};
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::issuance::{mint_bootstrap, mint_intermediate};
use mint::macaroon::Macaroon;
use mint::pop;
use mint::state::Store;
use tower::ServiceExt;

mod common;

const ROOT: [u8; 32] = [42u8; 32];
const COORD_SEED: [u8; 32] = [7u8; 32];
const OTHER_SEED: [u8; 32] = [9u8; 32];
const SUB: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

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
    "Resource": ["arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat "elide:Volume"}}/*"],
    "Condition": {"DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}}
  }]
}
"#;

fn config() -> Config {
    common::parse_config(TOML_TEMPLATE, &[("volume-ro.json", POLICY)])
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

/// (router, audit-buffer, store handle, tempdir guard). The store
/// handle lets a test play the operator (`approve`); the tempdir must
/// outlive the app.
fn app() -> (
    axum::Router,
    Arc<Mutex<Vec<u8>>>,
    Arc<Store>,
    tempfile::TempDir,
) {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let dir = tempfile::tempdir().expect("tempdir");
    // Seed the known root key (hex) so Store::open loads it (vs
    // generating one) and the macaroons minted with ROOT verify.
    let root_hex: String = ROOT.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(dir.path().join("root_key"), root_hex).expect("seed root_key");
    let store = Arc::new(Store::open(dir.path()).expect("store"));
    let state = AppState {
        config: Arc::new(config()),
        minter: Arc::new(FakeMinter::new()),
        audit: Arc::new(AuditLog::new(Box::new(AuditSink(buf.clone())))),
        store: store.clone(),
    };
    (router(state), buf, store, dir)
}

fn now() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn far_future() -> u64 {
    now() + 365 * 24 * 3600
}

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

fn field(body: &str, key: &str) -> Macaroon {
    let v: serde_json::Value = serde_json::from_str(body).expect("json");
    Macaroon::decode(v[key].as_str().expect("field present")).expect("decode")
}

/// The client's self-asserted bootstrap: the reusable bootstrap
/// macaroon with `sub`/`cnf` appended for `seed`.
fn client_bootstrap(nonce: &str, seed: &[u8; 32]) -> Macaroon {
    mint_bootstrap(&ROOT, "mint", nonce)
        .attenuate(Caveat::scalar(name::SUB, SUB))
        .attenuate(Caveat::scalar(name::CNF, pop::cnf_value(seed)))
}

#[tokio::test]
async fn full_flow_enroll_approve_exchange_then_assume_role() {
    let (app, audit, store, _dir) = app();
    let nonce = store.current_bootstrap().unwrap();
    let cb = client_bootstrap(&nonce, &COORD_SEED);

    // (1) enroll → pending + intermediate
    let (status, body) = parts(
        app.clone()
            .oneshot(signed("/v1/enroll", &cb, &COORD_SEED, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let intermediate = field(&body, "intermediate");
    assert!(intermediate.verify(&ROOT));

    // (2) exchange before approval → 403 (awaited, not a failure)
    let (status, _) = parts(
        app.clone()
            .oneshot(signed(
                "/v1/enroll-exchange",
                &intermediate,
                &COORD_SEED,
                "",
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // (3) operator approves the displayed sub
    assert!(store.approve(SUB).unwrap());

    // (4) exchange → non-expiring primary
    let (status, body) = parts(
        app.clone()
            .oneshot(signed(
                "/v1/enroll-exchange",
                &intermediate,
                &COORD_SEED,
                "",
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let primary = field(&body, "primary");
    assert!(primary.verify(&ROOT));
    let eff = EffectiveCaveats::new(primary.caveats());
    assert_eq!(
        eff.resolve(name::OP),
        Resolved::Value(op::ASSUME_ROLE.into())
    );
    assert_eq!(eff.resolve(name::SUB), Resolved::Value(SUB.into()));
    assert_eq!(
        eff.resolve(name::CNF),
        Resolved::Value(pop::cnf_value(&COORD_SEED))
    );
    assert_eq!(eff.not_after(name::EXP), None, "primary does not expire");

    // record consumed: a second exchange now fails closed (no pending)
    let (status, _) = parts(
        app.clone()
            .oneshot(signed(
                "/v1/enroll-exchange",
                &intermediate,
                &COORD_SEED,
                "",
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // (5) attenuate the primary and assume a role with it
    let req = primary
        .attenuate(Caveat::scalar(name::EXP, far_future().to_string()))
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

    let a = String::from_utf8(audit.lock().unwrap().clone()).unwrap();
    assert!(a.contains("\"outcome\":\"exchange:granted\""), "audit: {a}");
}

#[tokio::test]
async fn idempotent_reenroll_same_pair() {
    let (app, _a, store, _dir) = app();
    let nonce = store.current_bootstrap().unwrap();
    let cb = client_bootstrap(&nonce, &COORD_SEED);
    for _ in 0..2 {
        let (status, _) = parts(
            app.clone()
                .oneshot(signed("/v1/enroll", &cb, &COORD_SEED, ""))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}

#[tokio::test]
async fn conflicting_key_for_same_sub_is_opaque_401() {
    let (app, _a, store, _dir) = app();
    let nonce = store.current_bootstrap().unwrap();
    let (s, _) = parts(
        app.clone()
            .oneshot(signed(
                "/v1/enroll",
                &client_bootstrap(&nonce, &COORD_SEED),
                &COORD_SEED,
                "",
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // Same sub, a different key — must not overwrite or auto-resolve.
    let (s, _) = parts(
        app.oneshot(signed(
            "/v1/enroll",
            &client_bootstrap(&nonce, &OTHER_SEED),
            &OTHER_SEED,
            "",
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn stale_bootstrap_nonce_is_opaque_401() {
    let (app, _a, store, _dir) = app();
    let stale = store.current_bootstrap().unwrap();
    let cb = client_bootstrap(&stale, &COORD_SEED);
    store.rotate_bootstrap().unwrap(); // current nonce moves on
    let (status, _) = parts(
        app.oneshot(signed("/v1/enroll", &cb, &COORD_SEED, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn enroll_pop_by_wrong_key_is_opaque_401() {
    let (app, _a, store, _dir) = app();
    let nonce = store.current_bootstrap().unwrap();
    // cnf bound to COORD_SEED, but the request is signed by OTHER_SEED.
    let cb = client_bootstrap(&nonce, &COORD_SEED);
    let (status, _) = parts(
        app.oneshot(signed("/v1/enroll", &cb, &OTHER_SEED, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn bearer_bootstrap_without_cnf_is_opaque_401() {
    let (app, _a, store, _dir) = app();
    let nonce = store.current_bootstrap().unwrap();
    // sub but no cnf, and no PoP header: a captured bootstrap copy must
    // not enrol. NotKeyBound is a refusal here.
    let cb = mint_bootstrap(&ROOT, "mint", &nonce).attenuate(Caveat::scalar(name::SUB, SUB));
    let req = Request::builder()
        .method("POST")
        .uri("/v1/enroll")
        .header("authorization", format!("Macaroon {}", cb.encode()))
        .header("content-type", "application/json")
        .body(Body::from(format!(r#"{{"ts":{}}}"#, now())))
        .unwrap();
    let (status, _) = parts(app.oneshot(req).await.unwrap()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn exchange_without_a_pending_record_is_opaque_401() {
    let (app, _a, _store, _dir) = app();
    // A perfectly well-formed intermediate (minted from root) for a sub
    // that was never enrolled: no pending record → fail closed.
    let inter = mint_intermediate(
        &ROOT,
        "mint",
        SUB,
        &pop::cnf_value(&COORD_SEED),
        now() + 600,
    );
    let (status, _) = parts(
        app.oneshot(signed("/v1/enroll-exchange", &inter, &COORD_SEED, ""))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
