//! Configuration (`docs/design-mint.md` § *Mint configuration*). v1 is
//! single-tenant, single-trust-root.
//!
//! Audience, trust root, tenant, and roles are file-backed (TOML). The
//! Tigris admin credential is the one input that comes from the
//! environment — `AWS_*`, resolved by [`AdminCredential::from_env`] at
//! load — never the TOML, so secrets and role definitions stay on
//! separate management planes. The prototype's faked minter ignores it,
//! so it resolves to `Option`.

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("trust_root_hex must be 64 hex chars (32 bytes)")]
    BadTrustRoot,
    #[error("duplicate role name: {0}")]
    DuplicateRole(String),
    #[error("role {role}: {field} must be > 0 and min <= default <= max")]
    BadTtlBounds { role: String, field: String },
}

#[derive(Debug, Deserialize)]
pub struct RawConfig {
    /// The audience name this mint answers to. A macaroon whose
    /// `Audience` caveat differs is rejected (cross-service replay
    /// defence).
    pub audience: String,
    /// Single v1 trust root: 32-byte symmetric macaroon-signing key,
    /// hex-encoded.
    pub trust_root_hex: String,
    pub tenant: Tenant,
    #[serde(rename = "role", default)]
    pub roles: Vec<RawRole>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tenant {
    /// Bucket name; surfaced to templates as `{{tenant.bucket}}`.
    pub bucket: String,
}

/// Tigris admin credential, read from the standard AWS environment
/// variables — the same convention the elide coordinator uses for its
/// IAM-mode admin credential. It is deliberately **not** in the TOML
/// config: the credential is a secret delivered by the environment
/// (systemd `LoadCredential=`, a secrets manager, …), never committed
/// alongside role definitions.
#[derive(Clone)]
pub struct AdminCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// `AWS_SESSION_TOKEN` if present (STS-style temporary creds).
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AdminCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the secret — only that a credential is present.
        f.debug_struct("AdminCredential")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl AdminCredential {
    /// Resolve from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
    /// (+ optional `AWS_SESSION_TOKEN`). `None` if either required var
    /// is unset or empty — the prototype's faked minter does not need
    /// it, so absence is a warning at startup, not a hard error.
    pub fn from_env() -> Option<Self> {
        let access_key_id = non_empty_env("AWS_ACCESS_KEY_ID")?;
        let secret_access_key = non_empty_env("AWS_SECRET_ACCESS_KEY")?;
        Some(Self {
            access_key_id,
            secret_access_key,
            session_token: non_empty_env("AWS_SESSION_TOKEN"),
        })
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

#[derive(Debug, Deserialize)]
pub struct RawRole {
    pub name: String,
    #[serde(default)]
    pub required_caveats: Vec<String>,
    pub min_ttl_seconds: u64,
    pub max_ttl_seconds: u64,
    pub default_ttl_seconds: u64,
    /// IAM policy document, a handlebars template (see
    /// [`crate::template`]).
    pub policy: String,
}

/// Validated configuration, ready to serve.
#[derive(Debug)]
pub struct Config {
    pub audience: String,
    pub trust_root: [u8; 32],
    pub tenant: Tenant,
    /// Resolved from the AWS environment at load time. `None` when the
    /// env is unset (fine for the prototype's faked minter; a real
    /// Tigris minter must check this is `Some`).
    pub admin: Option<AdminCredential>,
    pub roles: BTreeMap<String, Role>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    pub required_caveats: Vec<String>,
    pub min_ttl_seconds: u64,
    pub max_ttl_seconds: u64,
    pub default_ttl_seconds: u64,
    pub policy: String,
}

impl Config {
    pub fn from_toml_str(s: &str) -> Result<Config, ConfigError> {
        Self::from_raw(toml::from_str(s)?)
    }

    pub fn load(path: &std::path::Path) -> Result<Config, ConfigError> {
        Self::from_toml_str(&std::fs::read_to_string(path)?)
    }

    fn from_raw(raw: RawConfig) -> Result<Config, ConfigError> {
        let trust_root = decode_root(&raw.trust_root_hex)?;
        let mut roles = BTreeMap::new();
        for r in raw.roles {
            if r.min_ttl_seconds == 0
                || r.min_ttl_seconds > r.default_ttl_seconds
                || r.default_ttl_seconds > r.max_ttl_seconds
            {
                return Err(ConfigError::BadTtlBounds {
                    role: r.name.clone(),
                    field: "ttl_seconds".into(),
                });
            }
            let role = Role {
                name: r.name.clone(),
                required_caveats: r.required_caveats,
                min_ttl_seconds: r.min_ttl_seconds,
                max_ttl_seconds: r.max_ttl_seconds,
                default_ttl_seconds: r.default_ttl_seconds,
                policy: r.policy,
            };
            if roles.insert(r.name.clone(), role).is_some() {
                return Err(ConfigError::DuplicateRole(r.name));
            }
        }
        Ok(Config {
            audience: raw.audience,
            trust_root,
            tenant: raw.tenant,
            admin: AdminCredential::from_env(),
            roles,
        })
    }
}

fn decode_root(hex: &str) -> Result<[u8; 32], ConfigError> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err(ConfigError::BadTrustRoot);
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| ConfigError::BadTrustRoot)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
audience = "mint"
trust_root_hex = "0000000000000000000000000000000000000000000000000000000000000000"

[tenant]
bucket = "demo-bucket"

[[role]]
name = "volume-ro"
required_caveats = ["elide:Volume", "Audience", "NotAfter"]
min_ttl_seconds = 60
max_ttl_seconds = 2592000
default_ttl_seconds = 2592000
policy = "{}"
"#;

    #[test]
    fn parses_sample() {
        let c = Config::from_toml_str(SAMPLE).expect("parse");
        assert_eq!(c.audience, "mint");
        assert_eq!(c.tenant.bucket, "demo-bucket");
        assert!(c.roles.contains_key("volume-ro"));
    }

    #[test]
    fn rejects_bad_root() {
        let bad = SAMPLE.replace(
            "0000000000000000000000000000000000000000000000000000000000000000",
            "deadbeef",
        );
        assert!(matches!(
            Config::from_toml_str(&bad),
            Err(ConfigError::BadTrustRoot)
        ));
    }

    #[test]
    fn rejects_inverted_ttl_bounds() {
        let bad = SAMPLE.replace("max_ttl_seconds = 2592000", "max_ttl_seconds = 10");
        assert!(matches!(
            Config::from_toml_str(&bad),
            Err(ConfigError::BadTtlBounds { .. })
        ));
    }
}
