//! HTTP client for the Tigris IAM Query API.
//!
//! Tigris emulates the AWS IAM Query API at `https://iam.storage.dev`:
//! SigV4-signed `application/x-www-form-urlencoded` POSTs with XML
//! responses. We sign with `aws-sigv4` and parse the small subset of
//! response shapes we care about with `quick-xml`.
//!
//! Tigris-specific quirks we work around:
//!
//! * `AttachUserPolicy` / `DetachUserPolicy` take an *access key ID* in
//!   the `UserName` slot (Tigris has no IAM users).
//! * `ListPolicies` has no name-prefix filter — callers filter by name
//!   client-side.

use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use tracing::debug;

use crate::error::IamError;

const IAM_API_VERSION: &str = "2010-05-08";
const SIGNING_SERVICE: &str = "iam";

/// Configuration for the Tigris IAM client.
///
/// `endpoint` defaults to `https://iam.storage.dev`. `region` is the
/// SigV4 signing region; AWS IAM is `aws-global`/`us-east-1`, and Tigris
/// accepts `us-east-1` here. Override only if Tigris adds region-aware
/// IAM in the future.
#[derive(Clone, Debug)]
pub struct TigrisIamConfig {
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub request_timeout: Duration,
}

impl TigrisIamConfig {
    pub fn tigris(access_key_id: String, secret_access_key: String) -> Self {
        Self {
            endpoint: "https://iam.storage.dev".to_owned(),
            region: "us-east-1".to_owned(),
            access_key_id,
            secret_access_key,
            request_timeout: Duration::from_secs(30),
        }
    }
}

pub struct TigrisIamClient {
    config: TigrisIamConfig,
    http: reqwest::Client,
}

/// Result of `CreateAccessKey`. Tigris returns access key ID prefixed
/// `tid_…` and the matching secret. Both are owned strings; callers
/// should keep the secret in memory only (per the design doc, secret
/// material is not persisted).
#[derive(Debug)]
pub struct CreatedAccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Result of `CreatePolicy`. The ARN is the handle used by
/// `AttachUserPolicy` / `DeletePolicy`.
#[derive(Debug, Clone)]
pub struct CreatedPolicy {
    pub policy_name: String,
    pub policy_arn: String,
}

impl TigrisIamClient {
    pub fn new(config: TigrisIamConfig) -> Result<Self, IamError> {
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()?;
        Ok(Self { config, http })
    }

    pub async fn create_access_key(&self) -> Result<CreatedAccessKey, IamError> {
        let body = encode_form(&[("Action", "CreateAccessKey"), ("Version", IAM_API_VERSION)]);
        let xml = self.send(body).await?;
        parse_create_access_key(&xml)
    }

    pub async fn delete_access_key(&self, access_key_id: &str) -> Result<(), IamError> {
        let body = encode_form(&[
            ("Action", "DeleteAccessKey"),
            ("Version", IAM_API_VERSION),
            ("AccessKeyId", access_key_id),
        ]);
        let _ = self.send(body).await?;
        Ok(())
    }

    pub async fn create_policy(
        &self,
        name: &str,
        document_json: &str,
    ) -> Result<CreatedPolicy, IamError> {
        let body = encode_form(&[
            ("Action", "CreatePolicy"),
            ("Version", IAM_API_VERSION),
            ("PolicyName", name),
            ("PolicyDocument", document_json),
        ]);
        let xml = self.send(body).await?;
        parse_create_policy(&xml)
    }

    pub async fn delete_policy(&self, policy_arn: &str) -> Result<(), IamError> {
        let body = encode_form(&[
            ("Action", "DeletePolicy"),
            ("Version", IAM_API_VERSION),
            ("PolicyArn", policy_arn),
        ]);
        let _ = self.send(body).await?;
        Ok(())
    }

    /// Attach a managed policy to an access key.
    ///
    /// Tigris uses the AWS spelling `AttachUserPolicy`, but the
    /// `UserName` parameter here is the access key ID — Tigris has no
    /// users. The caller passes the key ID; we only label the parameter
    /// `key_id` to make this unambiguous.
    pub async fn attach_user_policy(&self, key_id: &str, policy_arn: &str) -> Result<(), IamError> {
        let body = encode_form(&[
            ("Action", "AttachUserPolicy"),
            ("Version", IAM_API_VERSION),
            ("UserName", key_id),
            ("PolicyArn", policy_arn),
        ]);
        let _ = self.send(body).await?;
        Ok(())
    }

    pub async fn detach_user_policy(&self, key_id: &str, policy_arn: &str) -> Result<(), IamError> {
        let body = encode_form(&[
            ("Action", "DetachUserPolicy"),
            ("Version", IAM_API_VERSION),
            ("UserName", key_id),
            ("PolicyArn", policy_arn),
        ]);
        let _ = self.send(body).await?;
        Ok(())
    }

    /// List all locally-managed policies. Tigris's `ListPolicies` has
    /// no name-prefix filter; the caller filters client-side by the
    /// `elide-<coord-ulid>-` prefix to scope reconciliation to its own
    /// inventory.
    pub async fn list_policies(&self) -> Result<Vec<CreatedPolicy>, IamError> {
        let mut acc = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut params: Vec<(&str, &str)> = vec![
                ("Action", "ListPolicies"),
                ("Version", IAM_API_VERSION),
                // `Local` mirrors the AWS default: customer-managed
                // policies only, not AWS-managed (Tigris has none, but
                // the parameter is part of the request shape).
                ("Scope", "Local"),
            ];
            if let Some(m) = &marker {
                params.push(("Marker", m.as_str()));
            }
            let body = encode_form(&params);
            let xml = self.send(body).await?;
            let (page, next) = parse_list_policies(&xml)?;
            acc.extend(page);
            match next {
                Some(m) => marker = Some(m),
                None => break,
            }
        }
        Ok(acc)
    }

    async fn send(&self, body: String) -> Result<String, IamError> {
        let url = self.config.endpoint.clone();
        let now = SystemTime::now();

        let credentials = Credentials::new(
            self.config.access_key_id.clone(),
            self.config.secret_access_key.clone(),
            None,
            None,
            "tigris-iam-static",
        );
        let identity = credentials.into();
        let signing_settings = SigningSettings::default();
        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.config.region)
            .name(SIGNING_SERVICE)
            .time(now)
            .settings(signing_settings)
            .build()
            .map_err(|e| IamError::Signing(e.to_string()))?
            .into();

        let body_bytes = body.as_bytes();
        let signable = SignableRequest::new(
            "POST",
            &url,
            std::iter::once(("content-type", "application/x-www-form-urlencoded")),
            SignableBody::Bytes(body_bytes),
        )
        .map_err(|e| IamError::Signing(e.to_string()))?;

        let (instructions, _signature) = sign(signable, &signing_params)
            .map_err(|e| IamError::Signing(e.to_string()))?
            .into_parts();

        let mut req = http::Request::builder()
            .method("POST")
            .uri(&url)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body.clone())
            .map_err(|e| IamError::HttpBuild(e.to_string()))?;
        instructions.apply_to_request_http1x(&mut req);

        // Translate the signed `http::Request` into a reqwest call.
        let mut builder = self
            .http
            .post(&url)
            .body(body)
            .header("content-type", "application/x-www-form-urlencoded");
        for (k, v) in req.headers().iter() {
            // content-type is already set above; everything else (host,
            // x-amz-date, authorization, etc.) is what SigV4 added.
            if k.as_str().eq_ignore_ascii_case("content-type") {
                continue;
            }
            builder = builder.header(k.as_str(), v);
        }

        let resp = builder.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        debug!(target: "elide_tigris_iam", status = status.as_u16(), "iam response");
        if !status.is_success() {
            // Tigris returns AWS-style ErrorResponse XML for failures.
            if let Some((code, message)) = parse_error_response(&text) {
                return Err(IamError::Service { code, message });
            }
            return Err(IamError::Unexpected {
                status: status.as_u16(),
                body: text,
            });
        }
        Ok(text)
    }
}

/// URL-encode form parameters. We avoid pulling in `url` for this; the
/// IAM Query API only accepts a small character set in parameter values
/// (mostly ASCII, with `+`, `=`, `:`, `/` in policy ARNs and
/// JSON-document values).
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
    // RFC 3986 unreserved set: ALPHA / DIGIT / `-` / `.` / `_` / `~`.
    // SigV4 canonicalisation also expects this exact set, so we keep
    // signing and on-the-wire forms identical.
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn parse_create_access_key(xml: &str) -> Result<CreatedAccessKey, IamError> {
    let access_key_id = extract_first_text(xml, "AccessKeyId")
        .ok_or_else(|| IamError::Parse("missing <AccessKeyId>".into()))?;
    let secret_access_key = extract_first_text(xml, "SecretAccessKey")
        .ok_or_else(|| IamError::Parse("missing <SecretAccessKey>".into()))?;
    Ok(CreatedAccessKey {
        access_key_id,
        secret_access_key,
    })
}

fn parse_create_policy(xml: &str) -> Result<CreatedPolicy, IamError> {
    let policy_name = extract_first_text(xml, "PolicyName")
        .ok_or_else(|| IamError::Parse("missing <PolicyName>".into()))?;
    let policy_arn =
        extract_first_text(xml, "Arn").ok_or_else(|| IamError::Parse("missing <Arn>".into()))?;
    Ok(CreatedPolicy {
        policy_name,
        policy_arn,
    })
}

fn parse_list_policies(xml: &str) -> Result<(Vec<CreatedPolicy>, Option<String>), IamError> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut policies: Vec<CreatedPolicy> = Vec::new();
    let mut next_marker: Option<String> = None;

    let mut path: Vec<String> = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_arn: Option<String> = None;
    let mut text_accum = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .map_err(|_| IamError::Parse("non-utf8 tag".into()))?
                    .to_owned();
                path.push(tag);
                text_accum.clear();
            }
            Ok(Event::End(e)) => {
                let tag = std::str::from_utf8(e.name().as_ref())
                    .map_err(|_| IamError::Parse("non-utf8 tag".into()))?
                    .to_owned();
                let in_policy_member = path.iter().any(|p| p == "member");
                if in_policy_member {
                    match tag.as_str() {
                        "PolicyName" => current_name = Some(text_accum.clone()),
                        "Arn" => current_arn = Some(text_accum.clone()),
                        "member" => {
                            if let (Some(n), Some(a)) = (current_name.take(), current_arn.take()) {
                                policies.push(CreatedPolicy {
                                    policy_name: n,
                                    policy_arn: a,
                                });
                            }
                        }
                        _ => {}
                    }
                }
                if tag == "Marker" {
                    next_marker = Some(text_accum.clone()).filter(|s| !s.is_empty());
                }
                if tag == "IsTruncated" && text_accum.trim() != "true" {
                    next_marker = None;
                }
                path.pop();
                text_accum.clear();
            }
            Ok(Event::Text(t)) => {
                let bytes: &[u8] = t.as_ref();
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| IamError::Parse(format!("xml utf8: {e}")))?;
                text_accum.push_str(s);
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(IamError::Parse(format!("xml: {e}"))),
            _ => {}
        }
        buf.clear();
    }

    Ok((policies, next_marker))
}

fn parse_error_response(xml: &str) -> Option<(String, String)> {
    let code = extract_first_text(xml, "Code")?;
    let message = extract_first_text(xml, "Message").unwrap_or_default();
    Some((code, message))
}

/// Extract the text content of the first element with the given local
/// name, ignoring namespace prefixes. Sufficient for the small XML
/// shapes returned by IAM Query API: every field we need (AccessKeyId,
/// SecretAccessKey, Arn, PolicyName, Code, Message) appears at most
/// once at any given depth, so a streaming first-match is fine.
fn extract_first_text(xml: &str, local_name: &str) -> Option<String> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut in_tag = false;
    let mut text_accum = String::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                if e.name().as_ref() == local_name.as_bytes() {
                    in_tag = true;
                    text_accum.clear();
                }
            }
            Ok(Event::End(e)) => {
                if e.name().as_ref() == local_name.as_bytes() && in_tag {
                    return Some(text_accum.clone());
                }
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

    const CREATE_KEY_XML: &str = r#"<CreateAccessKeyResponse>
<CreateAccessKeyResult>
  <AccessKey>
    <AccessKeyId>tid_abc</AccessKeyId>
    <SecretAccessKey>tsec_xyz</SecretAccessKey>
  </AccessKey>
</CreateAccessKeyResult>
</CreateAccessKeyResponse>"#;

    const CREATE_POLICY_XML: &str = r#"<CreatePolicyResponse>
<CreatePolicyResult>
  <Policy>
    <PolicyName>elide-COORD-VOL-ro</PolicyName>
    <Arn>arn:aws:iam::tid:policy/elide-COORD-VOL-ro</Arn>
  </Policy>
</CreatePolicyResult>
</CreatePolicyResponse>"#;

    const LIST_POLICIES_XML: &str = r#"<ListPoliciesResponse>
<ListPoliciesResult>
  <IsTruncated>false</IsTruncated>
  <Policies>
    <member>
      <PolicyName>elide-COORD-V1-ro</PolicyName>
      <Arn>arn:aws:iam::tid:policy/elide-COORD-V1-ro</Arn>
    </member>
    <member>
      <PolicyName>elide-COORD-V2-ro</PolicyName>
      <Arn>arn:aws:iam::tid:policy/elide-COORD-V2-ro</Arn>
    </member>
  </Policies>
</ListPoliciesResult>
</ListPoliciesResponse>"#;

    const ERROR_XML: &str = r#"<ErrorResponse>
<Error>
  <Code>NoSuchEntity</Code>
  <Message>policy not found</Message>
</Error>
</ErrorResponse>"#;

    #[test]
    fn parses_create_access_key() {
        let r = parse_create_access_key(CREATE_KEY_XML).unwrap();
        assert_eq!(r.access_key_id, "tid_abc");
        assert_eq!(r.secret_access_key, "tsec_xyz");
    }

    #[test]
    fn parses_create_policy() {
        let r = parse_create_policy(CREATE_POLICY_XML).unwrap();
        assert_eq!(r.policy_name, "elide-COORD-VOL-ro");
        assert!(r.policy_arn.ends_with("/elide-COORD-VOL-ro"));
    }

    #[test]
    fn parses_list_policies() {
        let (policies, marker) = parse_list_policies(LIST_POLICIES_XML).unwrap();
        assert_eq!(policies.len(), 2);
        assert_eq!(policies[0].policy_name, "elide-COORD-V1-ro");
        assert_eq!(policies[1].policy_name, "elide-COORD-V2-ro");
        assert!(marker.is_none());
    }

    #[test]
    fn parses_error_response() {
        let (code, msg) = parse_error_response(ERROR_XML).unwrap();
        assert_eq!(code, "NoSuchEntity");
        assert_eq!(msg, "policy not found");
    }

    #[test]
    fn pct_encodes_json_body() {
        let encoded = encode_form(&[("PolicyDocument", "{\"a\":\"b/c\"}")]);
        assert_eq!(encoded, "PolicyDocument=%7B%22a%22%3A%22b%2Fc%22%7D");
    }
}
