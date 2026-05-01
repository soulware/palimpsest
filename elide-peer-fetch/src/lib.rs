//! Opportunistic LAN peer-fetch tier in front of S3 for Elide segment data.
//!
//! v1 scope is `.idx` fetch only; body fetch is deferred. See
//! `docs/design-peer-segment-fetch.md` and
//! `docs/peer-segment-fetch-v1-plan.md` for the design and plan.
//!
//! This crate contains:
//!
//! - [`PeerFetchToken`] — bearer-token type signed by the fetching
//!   coordinator's Ed25519 key. v1 verification is purely against S3:
//!   peer reads `coordinators/<id>/coordinator.pub` to verify the
//!   signature and `names/<volume>` to confirm the requester is the
//!   current claimer.
//!
//! - [`PeerEndpoint`] — coordinator-published advertisement at
//!   `coordinators/<id>/peer-endpoint.toml` of where its peer-fetch
//!   HTTP server can be reached. Coordinators read this during
//!   handoff discovery to dial the previous claimer.
//!
//! Subsequent items in the v1 plan (HTTP server, client, prefetch
//! integration) will land alongside these.

pub mod endpoint;
pub mod token;

pub use endpoint::{EndpointParseError, PeerEndpoint};
pub use token::{
    DEFAULT_FRESHNESS_WINDOW_SECS, DOMAIN_TAG, PeerFetchToken, TokenDecodeError, TokenVerifyError,
};
