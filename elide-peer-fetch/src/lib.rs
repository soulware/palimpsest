//! Opportunistic LAN peer-fetch tier in front of S3 for Elide segment data.
//!
//! v1 scope is `.idx` + `.prefetch` fetch only; peer body fetch is
//! deferred. See `docs/design-peer-segment-fetch.md` and
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
//! - [`ancestry`] — ObjectStore-backed walker that resolves a volume's
//!   signed fork-parent chain into the set of fork ULIDs the peer is
//!   allowed to serve segments from on its behalf.
//!
//! - [`auth`] — five-step verify pipeline that runs once per request:
//!   token decode + freshness, signature, ownership (`names/<name>`
//!   ETag-conditional), lineage (ancestry walk + URL prefix check),
//!   and segment-membership (the local file existence falls out at the
//!   route handler).
//!
//! - [`server`] — axum HTTP server with the two GET routes
//!   (`/v1/<vol_id>/<ulid>.idx` and `/v1/<vol_id>/<ulid>.prefetch`)
//!   wired through the auth middleware to the local `data_dir`.
//!
//! Items remaining in the v1 plan (client, prefetch integration) will
//! land alongside these.

pub mod ancestry;
pub mod auth;
pub mod body_token;
pub mod client;
pub mod endpoint;
pub mod hint;
pub mod range_fetcher;
pub mod server;
pub mod token;
pub mod volume_signer;

pub use body_token::{BODY_DOMAIN_TAG, BodyFetchToken};
pub use client::{
    BodyFetchClient, BodyTokenSigner, BuildError, PeerFetchClient, PeerFetchClientBuilder,
    TokenSigner,
};
pub use endpoint::{EndpointParseError, PeerEndpoint};
pub use hint::PrefetchHint;
pub use range_fetcher::{PeerFetchCounters, PeerFetchCountersHandle, PeerRangeFetcher};
pub use token::{
    DEFAULT_FRESHNESS_WINDOW_SECS, DOMAIN_TAG, PeerFetchToken, TokenDecodeError, TokenVerifyError,
};
pub use volume_signer::VolumeBodySigner;
