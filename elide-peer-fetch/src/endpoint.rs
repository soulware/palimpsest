//! `PeerEndpoint` — coordinator-published advertisement of where its
//! peer-fetch HTTP server can be reached.
//!
//! Lives at the well-known S3 path
//! `coordinators/<coordinator_id>/peer-endpoint.toml`, sibling to the
//! `coordinator.pub` already published by `CoordinatorIdentity`.
//! Sibling layout means both files share the same per-coordinator
//! prefix and lifecycle; an operator inspecting the bucket sees the
//! key + endpoint together.
//!
//! Schema is intentionally minimal:
//!
//! ```toml
//! host = "10.0.0.42"
//! port = 8443
//! ```
//!
//! v1 is plain HTTP only; the URL helper hard-codes `http://`. When
//! TLS is added later, the schema will grow a `scheme` field (or a
//! `tls = { ... }` section); current readers should treat absence of
//! such fields as "http".

use std::io;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use serde::{Deserialize, Serialize};

/// Where in the bucket each coordinator publishes its endpoint.
const ENDPOINT_FILENAME: &str = "peer-endpoint.toml";

fn endpoint_path(coordinator_id: &str) -> StorePath {
    StorePath::from(format!("coordinators/{coordinator_id}/{ENDPOINT_FILENAME}"))
}

/// Coordinator-side advertisement of its peer-fetch endpoint.
///
/// Written at coordinator startup when peer-fetch is configured;
/// other coordinators read it during handoff discovery to dial the
/// previous claimer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerEndpoint {
    /// Hostname or IP that other coordinators should dial. Operators
    /// configure this explicitly when the host's `gethostname()` value
    /// is not routable on the LAN.
    pub host: String,
    /// TCP port the peer-fetch HTTP server listens on.
    pub port: u16,
}

impl PeerEndpoint {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// Wire URL for v1: always `http://host:port`. The peer-fetch HTTP
    /// server appends per-segment paths to this base.
    pub fn url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    /// Serialise the endpoint as the TOML wire form.
    pub fn to_toml_bytes(&self) -> Result<Vec<u8>, toml::ser::Error> {
        toml::to_string(self).map(String::into_bytes)
    }

    /// Parse the TOML wire form.
    pub fn from_toml_bytes(bytes: &[u8]) -> Result<Self, EndpointParseError> {
        let s = std::str::from_utf8(bytes).map_err(EndpointParseError::Utf8)?;
        toml::from_str(s).map_err(EndpointParseError::Toml)
    }

    /// Publish this endpoint at
    /// `coordinators/<coordinator_id>/peer-endpoint.toml`. Overwrites any
    /// previously-published endpoint for this coordinator (e.g. across
    /// restarts with a different port). Idempotent — re-publishing the
    /// same bytes is fine.
    pub async fn publish(&self, store: &dyn ObjectStore, coordinator_id: &str) -> io::Result<()> {
        let bytes = self
            .to_toml_bytes()
            .map_err(|e| io::Error::other(format!("serialise peer-endpoint: {e}")))?;
        let key = endpoint_path(coordinator_id);
        store
            .put(&key, Bytes::from(bytes).into())
            .await
            .map_err(|e| io::Error::other(format!("publish peer-endpoint: {e}")))?;
        Ok(())
    }

    /// Fetch the endpoint advertised by `coordinator_id`. Returns
    /// `Ok(None)` cleanly when the endpoint object is absent — that's
    /// the normal "this coordinator hasn't enabled peer fetch" case
    /// and callers should fall back to S3 without treating it as an
    /// error.
    pub async fn fetch(store: &dyn ObjectStore, coordinator_id: &str) -> io::Result<Option<Self>> {
        let key = endpoint_path(coordinator_id);
        match store.get(&key).await {
            Ok(get_result) => {
                let bytes = get_result
                    .bytes()
                    .await
                    .map_err(|e| io::Error::other(format!("read peer-endpoint body: {e}")))?;
                let endpoint = Self::from_toml_bytes(&bytes).map_err(|e| {
                    io::Error::other(format!("parse peer-endpoint for {coordinator_id}: {e}"))
                })?;
                Ok(Some(endpoint))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(io::Error::other(format!("fetch peer-endpoint: {e}"))),
        }
    }
}

/// Errors parsing a `peer-endpoint.toml` file.
#[derive(Debug)]
pub enum EndpointParseError {
    Utf8(std::str::Utf8Error),
    Toml(toml::de::Error),
}

impl std::fmt::Display for EndpointParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8(e) => write!(f, "peer-endpoint not valid utf-8: {e}"),
            Self::Toml(e) => write!(f, "peer-endpoint toml parse error: {e}"),
        }
    }
}

impl std::error::Error for EndpointParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8(e) => Some(e),
            Self::Toml(e) => Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[test]
    fn url_is_plain_http_for_v1() {
        let ep = PeerEndpoint::new("10.0.0.5".to_owned(), 8443);
        assert_eq!(ep.url(), "http://10.0.0.5:8443");
    }

    #[test]
    fn toml_round_trip() {
        let ep = PeerEndpoint::new("host.example".to_owned(), 9000);
        let bytes = ep.to_toml_bytes().unwrap();
        let parsed = PeerEndpoint::from_toml_bytes(&bytes).unwrap();
        assert_eq!(parsed, ep);
    }

    #[test]
    fn parse_rejects_non_utf8() {
        let bad = [0xff, 0xfe, 0xfd];
        let err = PeerEndpoint::from_toml_bytes(&bad).expect_err("invalid utf-8");
        assert!(matches!(err, EndpointParseError::Utf8(_)));
    }

    #[test]
    fn parse_rejects_malformed_toml() {
        let bad = b"host = \nport ="; // missing values
        let err = PeerEndpoint::from_toml_bytes(bad).expect_err("malformed toml");
        assert!(matches!(err, EndpointParseError::Toml(_)));
    }

    #[test]
    fn parse_rejects_missing_required_fields() {
        let bad = b"host = \"x\""; // missing port
        let err = PeerEndpoint::from_toml_bytes(bad).expect_err("missing port");
        assert!(matches!(err, EndpointParseError::Toml(_)));
    }

    #[tokio::test]
    async fn publish_then_fetch_round_trip() {
        let store = store();
        let ep = PeerEndpoint::new("10.0.0.42".to_owned(), 8443);
        ep.publish(store.as_ref(), "coord-a").await.unwrap();
        let fetched = PeerEndpoint::fetch(store.as_ref(), "coord-a")
            .await
            .unwrap()
            .expect("endpoint present");
        assert_eq!(fetched, ep);
    }

    #[tokio::test]
    async fn fetch_returns_none_when_absent() {
        let store = store();
        let fetched = PeerEndpoint::fetch(store.as_ref(), "coord-missing")
            .await
            .unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn publish_overwrites_existing() {
        let store = store();
        let first = PeerEndpoint::new("h".to_owned(), 1);
        first.publish(store.as_ref(), "c").await.unwrap();
        let second = PeerEndpoint::new("h2".to_owned(), 2);
        second.publish(store.as_ref(), "c").await.unwrap();
        let fetched = PeerEndpoint::fetch(store.as_ref(), "c")
            .await
            .unwrap()
            .expect("endpoint present");
        assert_eq!(fetched, second);
    }

    #[tokio::test]
    async fn coordinators_with_distinct_ids_do_not_collide() {
        let store = store();
        let a = PeerEndpoint::new("ha".to_owned(), 1);
        let b = PeerEndpoint::new("hb".to_owned(), 2);
        a.publish(store.as_ref(), "coord-a").await.unwrap();
        b.publish(store.as_ref(), "coord-b").await.unwrap();
        let fa = PeerEndpoint::fetch(store.as_ref(), "coord-a")
            .await
            .unwrap()
            .unwrap();
        let fb = PeerEndpoint::fetch(store.as_ref(), "coord-b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fa, a);
        assert_eq!(fb, b);
    }
}
