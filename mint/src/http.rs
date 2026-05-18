//! HTTP surface (`docs/design-mint.md` § *Protocol*).
//!
//! ```text
//! POST /v1/assume-role      op=assume-role   (per request)
//! POST /v1/enroll           op=enroll        (creates a pending record)
//! POST /v1/enroll-exchange  op=enroll-exchange (403 until approved)
//! GET  /healthz
//! ```
//!
//! Authentication is identical across all three operations: MAC against
//! the root, the positively-required `op` for the endpoint, `aud`, and
//! the holder-of-key PoP over `tail ‖ BLAKE3(body)` (the body is the
//! freshness `ts` for the enrollment endpoints, the full exercise body
//! for `assume-role`). Every failure is an opaque `401` with no detail
//! so an attacker can't distinguish causes; role/caveat denial is
//! `400`; backend failure `503`. The **sole** non-`401` authorization
//! outcome is `/v1/enroll-exchange` returning `403` for a
//! not-yet-approved pending record — an awaited state, not a failure.

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
use crate::caveat::{Caveat, EffectiveCaveats, Resolved, name, op};
use crate::config::Config;
use crate::iam::KeypairMinter;
use crate::issuance;
use crate::macaroon::Macaroon;
use crate::pop::{self, PopOutcome};
use crate::role::{self, Denied};
use crate::state::{Recorded, StateError, Store};
use crate::template::render_policy;

/// Intermediate lifetime: long enough for an operator to approve out of
/// band, short by design. If it lapses the client just re-enrols
/// (idempotent for the same `(sub, pub)` → fresh intermediate).
const INTERMEDIATE_TTL_SECONDS: u64 = 600;
/// Unapproved pending records age out past this (≥ the intermediate
/// `exp`, so a still-usable intermediate always has its record).
const PENDING_MAX_AGE_SECONDS: u64 = 3600;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub minter: Arc<dyn KeypairMinter>,
    pub audit: Arc<AuditLog>,
    pub store: Arc<Store>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/assume-role", post(assume_role))
        .route("/v1/enroll", post(enroll))
        .route("/v1/enroll-exchange", post(enroll_exchange))
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

fn unauthorized(request_id: &str) -> Response {
    respond(
        request_id,
        StatusCode::UNAUTHORIZED,
        json!({"error": "unauthorized"}),
    )
}

/// Pull the bearer macaroon out of `Authorization: Macaroon <b64>`.
fn extract_macaroon(headers: &HeaderMap) -> Option<Macaroon> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let b64 = raw.strip_prefix("Macaroon ")?;
    Macaroon::decode(b64).ok()
}

fn peer_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string()
}

/// A scalar caveat must be present and equal to `expected` — the
/// positive-value gate (`op`/`aud`). Absent, contradictory, or any
/// other value all fail closed; no path tests for absence.
fn scalar_is(caveats: &[Caveat], n: &str, expected: &str) -> bool {
    matches!(
        EffectiveCaveats::new(caveats).resolve(n),
        Resolved::Value(v) if v == expected
    )
}

/// The detached PoP from `X-Mint-Coord-Pop`, if syntactically present.
/// A malformed header is a hard `Err` (caller maps to 401); absence is
/// `Ok(None)` (caller decides whether key-binding is required).
fn pop_proof(headers: &HeaderMap) -> Result<Option<pop::Proof>, ()> {
    match headers
        .get("x-mint-coord-pop")
        .and_then(|v| v.to_str().ok())
    {
        Some(sig) => pop::Proof::from_b64(sig).map(Some).map_err(|_| ()),
        None => Ok(None),
    }
}

async fn assume_role(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let caller = peer_ip(&headers);
    let audit = |entry: AuditEntry| state.audit.record(&entry);
    let now = Utc::now();
    let now_unix = now.timestamp().max(0) as u64;
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
        return unauthorized(&request_id);
    };
    if !mac.verify(&state.store.root_key()) {
        audit(base_entry("denied:bad_mac"));
        return unauthorized(&request_id);
    }
    let caveats = mac.caveats().to_vec();
    // Positive op gate: this endpoint serves op=assume-role only.
    if !scalar_is(&caveats, name::OP, op::ASSUME_ROLE) {
        audit(base_entry("denied:wrong_op"));
        return unauthorized(&request_id);
    }

    let nonce_hex = mac.nonce_hex();
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

    // --- Holder-of-key PoP (cnf). Enforced before anything reads the
    // body: the proof signs the exact raw bytes, so a verified PoP is
    // what makes the request.* template inputs trustworthy. ---
    let proof = match pop_proof(&headers) {
        Ok(p) => p,
        Err(()) => {
            audit(entry("denied:pop", "", None, None));
            return unauthorized(&request_id);
        }
    };
    if pop::check(&caveats, mac.tail(), &body, proof, now_unix).is_err() {
        audit(entry("denied:pop", "", None, None));
        return unauthorized(&request_id);
    }

    // --- Request body (the exact bytes the PoP covers). ---
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

    let granted = match role::authorize(&state.config, &caveats, &req.role, requested_ttl, now_unix)
    {
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

    let expiry = now + chrono::Duration::seconds(granted.ttl_seconds as i64);
    let expiry_iso = expiry.to_rfc3339();
    let policy = match render_policy(
        &granted.role.policy,
        &state.config.tenant,
        &caveats,
        &request_json,
        &expiry_iso,
        &granted.role.name,
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

/// `POST /v1/enroll` (`docs/design-mint.md` § *Enrollment* (1)). The
/// client presents the coordinator-attenuated bootstrap macaroon
/// (`op=enroll`, current `bootstrap`, self-asserted `sub`/`cnf`) and a
/// PoP. Mint records a **pending** record keyed by `sub` and returns a
/// short-lived intermediate. Always `200` for an accepted
/// (new or idempotent) `(sub, pub)`; conflicts and auth failures are
/// the opaque `401`.
async fn enroll(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let caller = peer_ip(&headers);
    let now_unix = Utc::now().timestamp().max(0) as u64;
    let audit = |outcome: &str, caveats: &[Caveat]| {
        state.audit.record(&AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            request_id: request_id.clone(),
            caller_address: caller.clone(),
            macaroon_nonce: None,
            macaroon_caveats: sanitise_caveats(caveats),
            role: String::new(),
            granted_ttl_seconds: None,
            outcome: format!("enroll:{outcome}"),
            tigris_access_key_id: None,
        });
    };

    // Opportunistic GC keeps the pending table transient.
    if let Err(e) = state.store.gc(now_unix, PENDING_MAX_AGE_SECONDS) {
        tracing::warn!(error = %e, "pending gc failed");
    }

    let Some(mac) = extract_macaroon(&headers) else {
        audit("denied:unauthenticated", &[]);
        return unauthorized(&request_id);
    };
    if !mac.verify(&state.store.root_key()) {
        audit("denied:bad_mac", &[]);
        return unauthorized(&request_id);
    }
    let caveats = mac.caveats().to_vec();

    if !scalar_is(&caveats, name::OP, op::ENROLL)
        || !scalar_is(&caveats, name::AUD, &state.config.audience)
    {
        audit("denied:wrong_op", &caveats);
        return unauthorized(&request_id);
    }
    let current = match state.store.current_bootstrap() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "read bootstrap nonce");
            return respond(
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "service unavailable"}),
            );
        }
    };
    if !scalar_is(&caveats, name::BOOTSTRAP, &current) {
        audit("denied:stale_bootstrap", &caveats);
        return unauthorized(&request_id);
    }

    // PoP is mandatory here (the bootstrap is bearer until the client
    // attenuates cnf; NotKeyBound means it didn't, so a captured copy
    // could enrol). Body is the freshness ts only.
    let proof = match pop_proof(&headers) {
        Ok(p) => p,
        Err(()) => {
            audit("denied:pop", &caveats);
            return unauthorized(&request_id);
        }
    };
    match pop::check(&caveats, mac.tail(), &body, proof, now_unix) {
        Ok(PopOutcome::Verified) => {}
        Ok(PopOutcome::NotKeyBound) | Err(_) => {
            audit("denied:pop", &caveats);
            return unauthorized(&request_id);
        }
    }

    let (sub, cnf) = match issuance::bound_identity(&mac) {
        Ok(v) => v,
        Err(_) => {
            audit("denied:identity", &caveats);
            return unauthorized(&request_id);
        }
    };

    match state
        .store
        .record_pending(&sub, &cnf, &current, &caller, now_unix)
    {
        Ok(Recorded::Created) | Ok(Recorded::Idempotent) => {}
        Err(StateError::Io(e)) => {
            tracing::error!(error = %e, "record pending");
            return respond(
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "service unavailable"}),
            );
        }
        // Conflict / BadSub / Corrupt: opaque 401 (don't distinguish).
        Err(_) => {
            audit("denied:conflict", &caveats);
            return unauthorized(&request_id);
        }
    }

    let intermediate = issuance::mint_intermediate(
        &state.store.root_key(),
        &state.config.audience,
        &sub,
        &cnf,
        now_unix.saturating_add(INTERMEDIATE_TTL_SECONDS),
    );
    audit("pending", &caveats);
    respond(
        &request_id,
        StatusCode::OK,
        json!({ "intermediate": intermediate.encode() }),
    )
}

/// `POST /v1/enroll-exchange` (`docs/design-mint.md` § *Enrollment*
/// (3)). The client presents the intermediate (`op=enroll-exchange`,
/// unexpired `exp`) and a PoP. If the pending record is approved, mint
/// re-mints the non-expiring primary from root and **consumes** the
/// record. `403` (not `401`) while approval is still pending — the one
/// awaited, non-failure outcome.
async fn enroll_exchange(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let caller = peer_ip(&headers);
    let now_unix = Utc::now().timestamp().max(0) as u64;
    let audit = |outcome: &str, caveats: &[Caveat]| {
        state.audit.record(&AuditEntry {
            timestamp: Utc::now().to_rfc3339(),
            request_id: request_id.clone(),
            caller_address: caller.clone(),
            macaroon_nonce: None,
            macaroon_caveats: sanitise_caveats(caveats),
            role: String::new(),
            granted_ttl_seconds: None,
            outcome: format!("exchange:{outcome}"),
            tigris_access_key_id: None,
        });
    };

    let Some(mac) = extract_macaroon(&headers) else {
        audit("denied:unauthenticated", &[]);
        return unauthorized(&request_id);
    };
    if !mac.verify(&state.store.root_key()) {
        audit("denied:bad_mac", &[]);
        return unauthorized(&request_id);
    }
    let caveats = mac.caveats().to_vec();

    if !scalar_is(&caveats, name::OP, op::ENROLL_EXCHANGE)
        || !scalar_is(&caveats, name::AUD, &state.config.audience)
    {
        audit("denied:wrong_op", &caveats);
        return unauthorized(&request_id);
    }
    match EffectiveCaveats::new(&caveats).not_after(name::EXP) {
        Some(exp) if exp > now_unix => {}
        _ => {
            audit("denied:expired", &caveats);
            return unauthorized(&request_id);
        }
    }

    let proof = match pop_proof(&headers) {
        Ok(p) => p,
        Err(()) => {
            audit("denied:pop", &caveats);
            return unauthorized(&request_id);
        }
    };
    match pop::check(&caveats, mac.tail(), &body, proof, now_unix) {
        Ok(PopOutcome::Verified) => {}
        Ok(PopOutcome::NotKeyBound) | Err(_) => {
            audit("denied:pop", &caveats);
            return unauthorized(&request_id);
        }
    }

    let (sub, cnf) = match issuance::bound_identity(&mac) {
        Ok(v) => v,
        Err(_) => {
            audit("denied:identity", &caveats);
            return unauthorized(&request_id);
        }
    };

    // The pending record must exist and its bound key must match the
    // presented cnf — the approval was for *this* (sub, pub) pair.
    match state.store.get_pending(&sub) {
        Ok(Some(p)) if p.pubkey == cnf => {}
        Ok(_) => {
            audit("denied:no_pending", &caveats);
            return unauthorized(&request_id);
        }
        Err(StateError::Io(e)) => {
            tracing::error!(error = %e, "read pending");
            return respond(
                &request_id,
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "service unavailable"}),
            );
        }
        Err(_) => {
            audit("denied:no_pending", &caveats);
            return unauthorized(&request_id);
        }
    }

    if !state.store.is_approved(&sub) {
        // The one non-401 authorization outcome: awaited, not a failure.
        audit("awaiting_approval", &caveats);
        return respond(
            &request_id,
            StatusCode::FORBIDDEN,
            json!({"error": "awaiting operator approval"}),
        );
    }

    let primary =
        issuance::mint_primary(&state.store.root_key(), &state.config.audience, &sub, &cnf);
    if let Err(e) = state.store.consume(&sub) {
        // Don't hand out a primary while the record lingers — the
        // client retries; consume+re-mint is idempotent in identity.
        tracing::error!(error = %e, "consume pending");
        return respond(
            &request_id,
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "service unavailable"}),
        );
    }
    audit("granted", &caveats);
    respond(
        &request_id,
        StatusCode::OK,
        json!({ "primary": primary.encode() }),
    )
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
