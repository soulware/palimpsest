//! Real Tigris IAM keypair minter (`docs/design-mint.md` § *Open
//! questions* #9, § *Cleanup*).
//!
//! Tigris exposes only the **AWS IAM Query API** at
//! `https://iam.storage.dev` — there is no Tigris-native IAM REST
//! endpoint. This module speaks that Query API directly: SigV4-signed
//! `application/x-www-form-urlencoded` POSTs, XML responses.
//!
//! It is a deliberately minimal, self-contained port of the three
//! operations mint needs — `CreateAccessKey`, `CreatePolicy`,
//! `AttachUserPolicy`. mint **never** deletes or lists keys: a keypair
//! lives until the `DateLessThan` already rendered into its policy
//! document expires (§ *Cleanup*), so the full IAM surface
//! (`DeleteAccessKey` / `DeletePolicy` / `Detach` / `ListPolicies`)
//! that the elide coordinator's per-volume model needs is intentionally
//! absent here. Ported rather than shared because `mint/` stays free of
//! `elide-*` dependencies (it is destined to be a standalone project).
//!
//! Tigris quirk: `AttachUserPolicy` takes the **access key ID** in the
//! `UserName` slot — Tigris has no IAM users; policies attach to keys.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use chrono::Utc;

use crate::config::AdminCredential;
use crate::iam::{KeypairMinter, MintError, MintedKeypair};

const IAM_API_VERSION: &str = "2010-05-08";
const SIGNING_SERVICE: &str = "iam";
const DEFAULT_ENDPOINT: &str = "https://iam.storage.dev";
/// Tigris is not region-aware for IAM; it accepts AWS's `us-east-1`.
const SIGNING_REGION: &str = "us-east-1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Internal failure surface. Every variant maps to
/// [`MintError::Backend`] at the [`KeypairMinter`] boundary — the HTTP
/// layer renders that as `503` with `Retry-After` (§ *Failure modes*).
#[derive(Debug, thiserror::Error)]
enum TigrisError {
    #[error("http transport: {0}")]
    Transport(String),
    #[error("building http request: {0}")]
    HttpBuild(String),
    #[error("signing request: {0}")]
    Signing(String),
    #[error("parsing response: {0}")]
    Parse(String),
    #[error("iam error {code}: {message}")]
    Service { code: String, message: String },
    #[error("unexpected response (status {status}): {body}")]
    Unexpected { status: u16, body: String },
}

/// `reqwest::Error`'s `Display` drops the cause — dns vs connect-refused
/// vs tls all look identical unless we walk `.source()` ourselves.
fn transport(e: reqwest::Error) -> TigrisError {
    let mut msg = e.to_string();
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        msg.push_str(&format!(": {s}"));
        src = s.source();
    }
    TigrisError::Transport(msg)
}

/// The Tigris IAM Query-API client: an admin credential plus a reqwest
/// client. The endpoint is overridable via `MINT_IAM_ENDPOINT` (staging
/// or a mock server in tests); production uses [`DEFAULT_ENDPOINT`].
pub struct TigrisMinter {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    http: reqwest::Client,
}

impl TigrisMinter {
    pub fn new(admin: &AdminCredential) -> Result<Self, MintError> {
        let endpoint = std::env::var("MINT_IAM_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned());
        Self::with_endpoint(admin, endpoint)
    }

    /// Construct against an explicit endpoint. Used by staging and by
    /// tests pointing at a local mock IAM server (the programmatic seam
    /// alternative to the `MINT_IAM_ENDPOINT` env override, which is
    /// process-global and would race parallel tests).
    pub fn with_endpoint(
        admin: &AdminCredential,
        endpoint: impl Into<String>,
    ) -> Result<Self, MintError> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| MintError::Backend(format!("http client build: {e}")))?;
        Ok(Self {
            endpoint: endpoint.into(),
            access_key_id: admin.access_key_id.clone(),
            secret_access_key: admin.secret_access_key.clone(),
            session_token: admin.session_token.clone(),
            http,
        })
    }

    async fn create_access_key(&self) -> Result<(String, String), TigrisError> {
        let body = encode_form(&[("Action", "CreateAccessKey"), ("Version", IAM_API_VERSION)]);
        let xml = self.send(body).await?;
        let id = extract_first_text(&xml, "AccessKeyId")
            .ok_or_else(|| TigrisError::Parse("missing <AccessKeyId>".into()))?;
        let secret = extract_first_text(&xml, "SecretAccessKey")
            .ok_or_else(|| TigrisError::Parse("missing <SecretAccessKey>".into()))?;
        Ok((id, secret))
    }

    async fn create_policy(&self, name: &str, document_json: &str) -> Result<String, TigrisError> {
        let body = encode_form(&[
            ("Action", "CreatePolicy"),
            ("Version", IAM_API_VERSION),
            ("PolicyName", name),
            ("PolicyDocument", document_json),
        ]);
        let xml = self.send(body).await?;
        extract_first_text(&xml, "Arn").ok_or_else(|| TigrisError::Parse("missing <Arn>".into()))
    }

    /// `UserName` is the access key ID (Tigris has no IAM users).
    async fn attach_user_policy(&self, key_id: &str, policy_arn: &str) -> Result<(), TigrisError> {
        let body = encode_form(&[
            ("Action", "AttachUserPolicy"),
            ("Version", IAM_API_VERSION),
            ("UserName", key_id),
            ("PolicyArn", policy_arn),
        ]);
        self.send(body).await.map(|_| ())
    }

    async fn send(&self, body: String) -> Result<String, TigrisError> {
        let url = &self.endpoint;
        let now = SystemTime::now();

        let credentials = Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
            self.session_token.clone(),
            None,
            "tigris-iam-static",
        );
        let identity = credentials.into();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(SIGNING_REGION)
            .name(SIGNING_SERVICE)
            .time(now)
            .settings(SigningSettings::default())
            .build()
            .map_err(|e| TigrisError::Signing(e.to_string()))?
            .into();

        let signable = SignableRequest::new(
            "POST",
            url,
            std::iter::once(("content-type", "application/x-www-form-urlencoded")),
            SignableBody::Bytes(body.as_bytes()),
        )
        .map_err(|e| TigrisError::Signing(e.to_string()))?;
        let (instructions, _sig) = sign(signable, &signing_params)
            .map_err(|e| TigrisError::Signing(e.to_string()))?
            .into_parts();

        let mut req = http::Request::builder()
            .method("POST")
            .uri(url)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body.clone())
            .map_err(|e| TigrisError::HttpBuild(e.to_string()))?;
        instructions.apply_to_request_http1x(&mut req);

        let mut builder = self
            .http
            .post(url)
            .body(body)
            .header("content-type", "application/x-www-form-urlencoded");
        for (k, v) in req.headers().iter() {
            // content-type already set; everything else (host,
            // x-amz-date, authorization, …) is what SigV4 added.
            if k.as_str().eq_ignore_ascii_case("content-type") {
                continue;
            }
            builder = builder.header(k.as_str(), v);
        }

        let resp = builder.send().await.map_err(transport)?;
        let status = resp.status();
        let text = resp.text().await.map_err(transport)?;
        tracing::debug!(target: "mint::tigris", status = status.as_u16(), "iam response");
        if !status.is_success() {
            if let Some((code, message)) = parse_error_response(&text) {
                return Err(TigrisError::Service { code, message });
            }
            return Err(TigrisError::Unexpected {
                status: status.as_u16(),
                body: text,
            });
        }
        Ok(text)
    }
}

#[async_trait]
impl KeypairMinter for TigrisMinter {
    /// `CreateAccessKey` → `CreatePolicy` → `AttachUserPolicy`. No
    /// rollback on partial failure: an unattached policy or a
    /// policy-less key grants nothing (IAM defaults to deny), and mint
    /// never deletes — the `DateLessThan` in `policy_json` is the only
    /// lifetime (§ *Cleanup*). `ttl` is not sent to Tigris (the bound is
    /// already baked into `policy_json` as `DateLessThan`); it only
    /// dates the returned keypair for the caller and the audit log.
    async fn mint_keypair(
        &self,
        policy_json: &str,
        ttl: Duration,
    ) -> Result<MintedKeypair, MintError> {
        let backend = |e: TigrisError| MintError::Backend(e.to_string());

        let (access_key_id, secret_access_key) = self.create_access_key().await.map_err(backend)?;
        // Policy name must be unique per issuance and within IAM's
        // [\w+=,.@-]{1,128} charset; `mint-` + 32 hex satisfies both and
        // marks the policy as mint-issued for operator visibility.
        let policy_name = format!("mint-{}", uuid::Uuid::new_v4().simple());
        let policy_arn = self
            .create_policy(&policy_name, policy_json)
            .await
            .map_err(backend)?;
        self.attach_user_policy(&access_key_id, &policy_arn)
            .await
            .map_err(backend)?;

        let expiration = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|_| MintError::Backend("ttl out of range".into()))?;
        Ok(MintedKeypair {
            access_key_id,
            secret_access_key,
            expiration,
        })
    }
}

/// URL-encode form parameters. The IAM Query API accepts only a small
/// character set; this keeps the on-the-wire and SigV4-canonical forms
/// byte-identical (a mismatch is a signature failure).
fn encode_form(params: &[(&str, &str)]) -> String {
    let mut out = String::new();
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        out.push_str(&pct_encode(k));
        out.push('=');
        out.push_str(&pct_encode(v));
    }
    out
}

fn pct_encode(s: &str) -> String {
    // RFC 3986 unreserved set; SigV4 canonicalisation expects exactly
    // this, so signing and wire forms stay identical.
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn parse_error_response(xml: &str) -> Option<(String, String)> {
    let code = extract_first_text(xml, "Code")?;
    let message = extract_first_text(xml, "Message").unwrap_or_default();
    Some((code, message))
}

/// Text of the first element with the given local name. The IAM Query
/// shapes mint reads (`AccessKeyId`, `SecretAccessKey`, `Arn`, `Code`,
/// `Message`) each appear at most once, so a streaming first-match is
/// sufficient.
fn extract_first_text(xml: &str, local_name: &str) -> Option<String> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut in_tag = false;
    let mut text_accum = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) if e.name().as_ref() == local_name.as_bytes() => {
                in_tag = true;
                text_accum.clear();
            }
            Ok(Event::End(e)) if in_tag && e.name().as_ref() == local_name.as_bytes() => {
                return Some(text_accum);
            }
            Ok(Event::Text(t)) if in_tag => {
                let bytes: &[u8] = t.as_ref();
                if let Ok(s) = std::str::from_utf8(bytes) {
                    text_accum.push_str(s);
                }
            }
            Ok(Event::Eof) => return None,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CREATE_KEY_XML: &str = r#"<CreateAccessKeyResponse><CreateAccessKeyResult>
      <AccessKey><AccessKeyId>tid_abc</AccessKeyId>
      <SecretAccessKey>tsec_xyz</SecretAccessKey></AccessKey>
    </CreateAccessKeyResult></CreateAccessKeyResponse>"#;

    const CREATE_POLICY_XML: &str = r#"<CreatePolicyResponse><CreatePolicyResult>
      <Policy><PolicyName>mint-deadbeef</PolicyName>
      <Arn>arn:aws:iam::tid:policy/mint-deadbeef</Arn></Policy>
    </CreatePolicyResult></CreatePolicyResponse>"#;

    const ERROR_XML: &str = r#"<ErrorResponse><Error>
      <Code>Throttling</Code><Message>rate exceeded</Message>
    </Error></ErrorResponse>"#;

    #[test]
    fn parses_create_access_key_fields() {
        assert_eq!(
            extract_first_text(CREATE_KEY_XML, "AccessKeyId").as_deref(),
            Some("tid_abc")
        );
        assert_eq!(
            extract_first_text(CREATE_KEY_XML, "SecretAccessKey").as_deref(),
            Some("tsec_xyz")
        );
    }

    #[test]
    fn parses_create_policy_arn() {
        assert_eq!(
            extract_first_text(CREATE_POLICY_XML, "Arn").as_deref(),
            Some("arn:aws:iam::tid:policy/mint-deadbeef")
        );
    }

    #[test]
    fn parses_error_response_to_code_message() {
        let (code, msg) = parse_error_response(ERROR_XML).expect("error parsed");
        assert_eq!(code, "Throttling");
        assert_eq!(msg, "rate exceeded");
    }

    #[test]
    fn missing_field_is_none_not_panic() {
        assert!(extract_first_text("<X/>", "AccessKeyId").is_none());
        assert!(extract_first_text("not xml at all", "Arn").is_none());
    }

    #[test]
    fn pct_encode_keeps_signing_and_wire_identical() {
        assert_eq!(
            encode_form(&[("PolicyDocument", "{\"a\":\"b/c\"}")]),
            "PolicyDocument=%7B%22a%22%3A%22b%2Fc%22%7D"
        );
        // Unreserved set passes through untouched.
        assert_eq!(pct_encode("aZ09-._~"), "aZ09-._~");
    }
}
