//! Keypair minting boundary.
//!
//! In production this is the `CreatePolicy` + `CreateAccessKey` +
//! `AttachUserPolicy` sequence against Tigris IAM (`docs/design-mint.md`
//! § *Open questions* #9). mint never deletes keys — they expire via the
//! policy's `DateLessThan` (§ *Cleanup*).
//!
//! The minter is behind a trait so the HTTP/macaroon/role shape can run
//! end-to-end without a live Tigris account. [`FakeMinter`] returns
//! deterministic keys and records every call for assertions;
//! [`crate::tigris::TigrisMinter`] is the real Tigris IAM Query-API
//! implementation (`serve --tigris`). The Tigris client is ported into
//! `mint/` rather than shared with `elide-tigris-iam` so the eventual
//! standalone project carries no `elide-*` dependency.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MintedKeypair {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub expiration: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum MintError {
    /// Tigris-side transient failure (rate limit, quota, admin
    /// credential rejection). Maps to HTTP 503.
    #[error("backend unavailable: {0}")]
    Backend(String),
}

#[async_trait]
pub trait KeypairMinter: Send + Sync {
    /// Mint a keypair scoped by `policy_json`, expiring after `ttl`.
    async fn mint_keypair(
        &self,
        policy_json: &str,
        ttl: Duration,
    ) -> Result<MintedKeypair, MintError>;
}

#[derive(Debug, Clone)]
pub struct RecordedMint {
    pub policy_json: String,
    pub ttl: Duration,
    pub issued_key_id: String,
}

/// Deterministic in-memory minter for the prototype and tests.
pub struct FakeMinter {
    calls: Mutex<Vec<RecordedMint>>,
}

impl FakeMinter {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Snapshot of every `mint_keypair` call so far.
    pub fn calls(&self) -> Vec<RecordedMint> {
        self.calls.lock().map(|c| c.clone()).unwrap_or_default()
    }
}

impl Default for FakeMinter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KeypairMinter for FakeMinter {
    async fn mint_keypair(
        &self,
        policy_json: &str,
        ttl: Duration,
    ) -> Result<MintedKeypair, MintError> {
        let mut calls = self
            .calls
            .lock()
            .map_err(|_| MintError::Backend("poisoned call log".into()))?;
        let n = calls.len();
        let key_id = format!("tid_fake_{n:08}");
        calls.push(RecordedMint {
            policy_json: policy_json.to_string(),
            ttl,
            issued_key_id: key_id.clone(),
        });
        let expiration = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|_| MintError::Backend("ttl out of range".into()))?;
        Ok(MintedKeypair {
            access_key_id: key_id,
            secret_access_key: format!("fakesecret{n:08}"),
            expiration,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fake_minter_records_and_is_deterministic() {
        let m = FakeMinter::new();
        let k0 = m.mint_keypair("{}", Duration::from_secs(60)).await.unwrap();
        let k1 = m.mint_keypair("{}", Duration::from_secs(60)).await.unwrap();
        assert_eq!(k0.access_key_id, "tid_fake_00000000");
        assert_eq!(k1.access_key_id, "tid_fake_00000001");
        assert_eq!(m.calls().len(), 2);
    }
}
