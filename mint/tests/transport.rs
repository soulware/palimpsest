//! UDS transport end-to-end (`docs/design-mint.md` § *Transport*): the
//! bundled single-host dev shape. Every other test drives the router
//! in-process via `tower::oneshot`; this one binds a real
//! `UnixListener`, serves the router with `axum::serve`, and runs the
//! reference client over `--url unix:<path>` — exercising the server
//! bind+chmod path, the `unix:` scheme parse, and the `hyperlocal`
//! client leg that `reqwest` cannot do. The macaroon + Ed25519 PoP is
//! identical to the TCP path, so a green full flow here proves the
//! transport seam, not the auth.

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use mint::audit::AuditLog;
use mint::config::Config;
use mint::http::{AppState, router};
use mint::iam::FakeMinter;
use mint::issuance::mint_bootstrap;
use mint::state::Store;

mod common;

const ROOT: [u8; 32] = [42u8; 32];
const SUB: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

const TOML: &str = r#"
audience = "mint"
[tenant]
bucket = "demo-bucket"
[[role]]
name = "coord-data"
required_caveats = ["aud"]
min_ttl_seconds = 60
max_ttl_seconds = 3600
default_ttl_seconds = 900
policy_file = "coord-data.json"
"#;

fn config() -> Config {
    common::parse_config(
        TOML,
        &[(
            "coord-data.json",
            r#"{"Version":"2012-10-17","Statement":[]}"#,
        )],
    )
}

#[tokio::test]
async fn full_flow_over_unix_socket() {
    // Server state with a seeded root key so client-side bootstrap
    // macaroons minted from ROOT verify.
    let srv_dir = tempfile::tempdir().expect("srv tempdir");
    let root_hex: String = ROOT.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(srv_dir.path().join("root_key"), root_hex).expect("seed root_key");
    let store = Arc::new(Store::open(srv_dir.path()).expect("store"));
    let nonce = store.current_bootstrap().expect("nonce");
    let state = AppState {
        config: Arc::new(config()),
        minter: Arc::new(FakeMinter::new()),
        audit: Arc::new(AuditLog::new(Box::new(std::io::sink()))),
        store: store.clone(),
    };

    // Bind the socket (listening immediately, so client connects queue
    // in the backlog even before the accept task is scheduled), chmod
    // 0o666 as the server does, then serve in the background.
    let sock = srv_dir.path().join("mint.sock");
    let listener = tokio::net::UnixListener::bind(&sock).expect("bind uds");
    std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o666)).expect("chmod");
    assert_eq!(
        std::fs::metadata(&sock).expect("stat").permissions().mode() & 0o777,
        0o666,
    );
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.expect("serve");
    });

    let url = format!("unix:{}", sock.display());
    let cdir = tempfile::tempdir().expect("client tempdir");
    mint::client::keygen(cdir.path(), false).expect("keygen");
    let bootstrap = mint_bootstrap(&ROOT, "mint", &nonce).encode();

    // enroll → credential ticket persisted client-side.
    mint::client::enroll(
        cdir.path(),
        &url,
        &bootstrap,
        SUB,
        mint::client::CREDENTIAL_TICKET_FILE,
    )
    .await
    .expect("enroll over uds");

    // exchange before approval: not a failure, just not yet approved.
    let role = "coord-data";
    let cred = mint::client::credential_path(role);
    assert!(
        !mint::client::exchange(
            cdir.path(),
            &url,
            mint::client::CREDENTIAL_TICKET_FILE,
            role,
            &cred,
        )
        .await
        .expect("exchange call over uds"),
        "unapproved exchange must report not-yet-approved, not error",
    );

    // Operator approves, then exchange yields the credential.
    assert!(store.approve(SUB).expect("approve"));
    assert!(
        mint::client::exchange(
            cdir.path(),
            &url,
            mint::client::CREDENTIAL_TICKET_FILE,
            role,
            &cred,
        )
        .await
        .expect("post-approval exchange over uds"),
    );

    // assume-role returns the (fake) keypair JSON — the full chain
    // verified and a scoped credential minted, all over the socket.
    let kp = mint::client::assume_role(cdir.path(), &url, role, None, &[], 900, &cred)
        .await
        .expect("assume-role over uds");
    let v: serde_json::Value = serde_json::from_str(&kp).expect("keypair json");
    assert!(
        v.get("access_key_id").is_some(),
        "expected a keypair, got: {kp}"
    );
}
