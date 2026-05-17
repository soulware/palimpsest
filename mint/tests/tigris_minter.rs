//! TigrisMinter against a mock IAM Query-API server. No live account:
//! a local axum server stands in for `https://iam.storage.dev`, records
//! every request, and returns canned AWS-style XML. This proves the
//! real wire sequence — `CreateAccessKey` → `CreatePolicy` →
//! `AttachUserPolicy`, SigV4-signed, form-encoded — and the error path.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use mint::config::AdminCredential;
use mint::iam::{KeypairMinter, MintError};
use mint::tigris::TigrisMinter;

#[derive(Clone, Default)]
struct Recorder {
    /// Every request: (Action value, had-SigV4-Authorization).
    seen: Arc<Mutex<Vec<(String, bool)>>>,
    /// If set, the server replies 400 with a Throttling ErrorResponse
    /// instead of the success XML, on the request whose Action matches.
    fail_on: Option<&'static str>,
}

fn action_of(body: &str) -> String {
    body.split('&')
        .find_map(|kv| kv.strip_prefix("Action="))
        .unwrap_or("")
        .to_string()
}

async fn handler(
    State(rec): State<Recorder>,
    headers: HeaderMap,
    body: String,
) -> (axum::http::StatusCode, String) {
    let action = action_of(&body);
    let signed = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.starts_with("AWS4-HMAC-SHA256"));
    rec.seen.lock().unwrap().push((action.clone(), signed));

    if rec.fail_on == Some(action.as_str()) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "<ErrorResponse><Error><Code>Throttling</Code>\
             <Message>rate exceeded</Message></Error></ErrorResponse>"
                .to_string(),
        );
    }

    let xml = match action.as_str() {
        "CreateAccessKey" => {
            "<CreateAccessKeyResponse><CreateAccessKeyResult><AccessKey>\
             <AccessKeyId>tid_live123</AccessKeyId>\
             <SecretAccessKey>tsec_live456</SecretAccessKey>\
             </AccessKey></CreateAccessKeyResult></CreateAccessKeyResponse>"
        }
        "CreatePolicy" => {
            "<CreatePolicyResponse><CreatePolicyResult><Policy>\
             <PolicyName>mint-x</PolicyName>\
             <Arn>arn:aws:iam::tid:policy/mint-x</Arn>\
             </Policy></CreatePolicyResult></CreatePolicyResponse>"
        }
        "AttachUserPolicy" => {
            "<AttachUserPolicyResponse><ResponseMetadata>\
             <RequestId>r1</RequestId></ResponseMetadata></AttachUserPolicyResponse>"
        }
        _ => "<unexpected/>",
    };
    (axum::http::StatusCode::OK, xml.to_string())
}

async fn spawn_mock(rec: Recorder) -> String {
    let app = Router::new().route("/", post(handler)).with_state(rec);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn admin() -> AdminCredential {
    AdminCredential {
        access_key_id: "tid_admin".into(),
        secret_access_key: "secret_admin".into(),
        session_token: None,
    }
}

#[tokio::test]
async fn mints_keypair_via_three_signed_calls_in_order() {
    let rec = Recorder::default();
    let endpoint = spawn_mock(rec.clone()).await;
    let minter = TigrisMinter::with_endpoint(&admin(), endpoint).unwrap();

    let kp = minter
        .mint_keypair(r#"{"Version":"2012-10-17"}"#, Duration::from_secs(3600))
        .await
        .expect("mint succeeds");

    assert_eq!(kp.access_key_id, "tid_live123");
    assert_eq!(kp.secret_access_key, "tsec_live456");
    assert!(kp.expiration > chrono::Utc::now());

    let seen = rec.seen.lock().unwrap().clone();
    let actions: Vec<&str> = seen.iter().map(|(a, _)| a.as_str()).collect();
    assert_eq!(
        actions,
        ["CreateAccessKey", "CreatePolicy", "AttachUserPolicy"],
        "exact wire sequence"
    );
    assert!(
        seen.iter().all(|(_, signed)| *signed),
        "every request must carry a SigV4 Authorization header"
    );
}

#[tokio::test]
async fn iam_error_maps_to_backend_unavailable() {
    let rec = Recorder {
        fail_on: Some("CreatePolicy"),
        ..Recorder::default()
    };
    let endpoint = spawn_mock(rec.clone()).await;
    let minter = TigrisMinter::with_endpoint(&admin(), endpoint).unwrap();

    let err = minter
        .mint_keypair("{}", Duration::from_secs(60))
        .await
        .expect_err("CreatePolicy 400 must fail the mint");
    let MintError::Backend(msg) = err;
    assert!(msg.contains("Throttling"), "got: {msg}");
    assert!(msg.contains("rate exceeded"), "got: {msg}");

    // The key was created before the failing CreatePolicy; mint does no
    // rollback (an unattached key grants nothing — IAM default-deny).
    let actions: Vec<String> = rec
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(a, _)| a.clone())
        .collect();
    assert_eq!(actions, ["CreateAccessKey", "CreatePolicy"]);
}
