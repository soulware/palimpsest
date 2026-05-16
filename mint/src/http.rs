//! HTTP surface (`docs/design-mint.md` § *Protocol*).
//!
//! ```text
//! POST /v1/assume-role   Authorization: Macaroon <b64>
//! GET  /healthz
//! ```
//!
//! Error model is deliberately coarse (§ *Authentication*, § *Response*):
//!
//! - any macaroon problem (missing header, bad base64, bad MAC) → `401`
//!   with no detail, so an attacker can't distinguish failure causes;
//! - role/caveat denial → `400`;
//! - backend (Tigris) failure → `503`;
//! - every response carries `X-Request-Id`.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

use crate::audit::{AuditEntry, AuditLog, sanitise_caveats};
use crate::config::Config;
use crate::iam::KeypairMinter;
use crate::macaroon::Macaroon;
use crate::pop;
use crate::role::{self, Denied};
use crate::template::render_policy;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub minter: Arc<dyn KeypairMinter>,
    pub audit: Arc<AuditLog>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/assume-role", post(assume_role))
        .with_state(state)
}

#[derive(Deserialize)]
struct AssumeRoleBody {
    role: String,
    ttl_seconds: Option<u64>,
}

fn respond(request_id: &str, status: StatusCode, body: serde_json::Value) -> Response {
    let mut resp = (status, axum::Json(body)).into_response();
    if let Ok(v) = request_id.parse() {
        resp.headers_mut().insert("x-request-id", v);
    }
    resp
}

/// Pull the bearer macaroon out of `Authorization: Macaroon <b64>`.
fn extract_macaroon(headers: &HeaderMap) -> Option<Macaroon> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let b64 = raw.strip_prefix("Macaroon ")?;
    Macaroon::decode(b64).ok()
}

async fn assume_role(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let caller = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let audit = |entry: AuditEntry| state.audit.record(&entry);
    let now = Utc::now();
    let base_entry = |outcome: &str| AuditEntry {
        timestamp: now.to_rfc3339(),
        request_id: request_id.clone(),
        caller_address: caller.clone(),
        macaroon_nonce: None,
        macaroon_caveats: Vec::new(),
        role: String::new(),
        granted_ttl_seconds: None,
        outcome: outcome.to_string(),
        tigris_access_key_id: None,
    };

    // --- Authentication: any failure is an opaque 401. ---
    let Some(mac) = extract_macaroon(&headers) else {
        audit(base_entry("denied:unauthenticated"));
        return respond(
            &request_id,
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized"}),
        );
    };
    if !mac.verify(&state.config.trust_root) {
        audit(base_entry("denied:bad_mac"));
        return respond(
            &request_id,
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized"}),
        );
    }

    let nonce_hex = mac.nonce_hex();
    let caveats = mac.caveats().to_vec();
    let entry = |outcome: &str, role: &str, ttl: Option<u64>, key: Option<String>| AuditEntry {
        timestamp: now.to_rfc3339(),
        request_id: request_id.clone(),
        caller_address: caller.clone(),
        macaroon_nonce: Some(nonce_hex.clone()),
        macaroon_caveats: sanitise_caveats(&caveats),
        role: role.to_string(),
        granted_ttl_seconds: ttl,
        outcome: outcome.to_string(),
        tigris_access_key_id: key,
    };

    // --- Holder-of-key PoP (elide:CoordKey) ---
    // Enforced before anything reads the body: the proof signs over the
    // exact raw body bytes, so a verified PoP is what makes the
    // request.* template inputs trustworthy. Any failure is the same
    // opaque 401 as a bad MAC (don't distinguish causes); a
    // contradictory elide:CoordKey fails closed here, never downgrades
    // to bearer.
    let pop_proof = match headers
        .get("x-mint-coord-pop")
        .and_then(|v| v.to_str().ok())
    {
        Some(sig) => match pop::Proof::from_b64(sig) {
            Ok(p) => Some(p),
            Err(_) => {
                audit(entry("denied:pop", "", None, None));
                return respond(
                    &request_id,
                    StatusCode::UNAUTHORIZED,
                    json!({"error": "unauthorized"}),
                );
            }
        },
        None => None,
    };
    if let Err(_e) = pop::check(
        &caveats,
        mac.tail(),
        &body,
        pop_proof,
        now.timestamp().max(0) as u64,
    ) {
        audit(entry("denied:pop", "", None, None));
        return respond(
            &request_id,
            StatusCode::UNAUTHORIZED,
            json!({"error": "unauthorized"}),
        );
    }

    // --- Request body ---
    // Parse twice from the same bytes: the typed view (role/ttl) and
    // the generic `request.*` template namespace. Both are derived from
    // the exact bytes the PoP signature covers (§ pop): the policy can
    // only ever reflect the body the coordinator signed.
    let Ok(req) = serde_json::from_slice::<AssumeRoleBody>(&body) else {
        audit(entry("denied:bad_request", "", None, None));
        return respond(
            &request_id,
            StatusCode::BAD_REQUEST,
            json!({"error": "bad request"}),
        );
    };
    let request_json: serde_json::Value =
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);

    // Default TTL is the role's default; resolve before authorize so the
    // clamp in `role::authorize` sees a concrete number.
    let requested_ttl = match req.ttl_seconds {
        Some(t) => t,
        None => match state.config.roles.get(&req.role) {
            Some(r) => r.default_ttl_seconds,
            None => {
                audit(entry("denied:unknown_role", &req.role, None, None));
                return respond(
                    &request_id,
                    StatusCode::BAD_REQUEST,
                    json!({"error": "bad request"}),
                );
            }
        },
    };

    let granted = match role::authorize(
        &state.config,
        &caveats,
        &req.role,
        requested_ttl,
        now.timestamp().max(0) as u64,
    ) {
        Ok(g) => g,
        Err(d) => {
            audit(entry(
                &format!("denied:{}", denied_tag(&d)),
                &req.role,
                None,
                None,
            ));
            return respond(
                &request_id,
                StatusCode::BAD_REQUEST,
                json!({"error": "bad request"}),
            );
        }
    };

    // --- Render policy + mint keypair ---
    let expiry = now + chrono::Duration::seconds(granted.ttl_seconds as i64);
    let expiry_iso = expiry.to_rfc3339();
    let policy = match render_policy(
        &granted.role.policy,
        &state.config.tenant,
        &caveats,
        &request_json,
        &expiry_iso,
    ) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, role = %req.role, "policy render failed");
            audit(entry("denied:policy_render", &req.role, None, None));
            return respond(
                &request_id,
                StatusCode::BAD_REQUEST,
                json!({"error": "bad request"}),
            );
        }
    };

    match state
        .minter
        .mint_keypair(&policy, Duration::from_secs(granted.ttl_seconds))
        .await
    {
        Ok(kp) => {
            audit(entry(
                "granted",
                &req.role,
                Some(granted.ttl_seconds),
                Some(kp.access_key_id.clone()),
            ));
            respond(
                &request_id,
                StatusCode::OK,
                json!({
                    "access_key_id": kp.access_key_id,
                    "secret_access_key": kp.secret_access_key,
                    "expiration": kp.expiration.to_rfc3339(),
                }),
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "keypair mint failed");
            audit(entry(
                "tigris_error",
                &req.role,
                Some(granted.ttl_seconds),
                None,
            ));
            let mut resp = respond(
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "service unavailable"}),
            );
            if let Ok(v) = "5".parse() {
                resp.headers_mut().insert("retry-after", v);
            }
            resp
        }
    }
}

fn denied_tag(d: &Denied) -> &'static str {
    match d {
        Denied::UnknownRole => "unknown_role",
        Denied::WrongAudience => "wrong_audience",
        Denied::RoleNotPermitted => "role_not_permitted",
        Denied::MissingRequiredCaveat(_) => "missing_required_caveat",
        Denied::UnsatisfiableCaveat(_) => "unsatisfiable_caveat",
        Denied::Expired => "expired",
        Denied::TtlTooShort => "ttl_too_short",
    }
}
