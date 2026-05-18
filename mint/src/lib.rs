//! `mint` — macaroon-authenticated scoped-credential vending for Tigris.
//!
//! Prototype tracking `docs/design-mint.md`. The mint verifies a
//! caller-presented macaroon against its macaroon root key, looks up
//! a role, renders the role's IAM-policy template from the macaroon's
//! caveats, mints a scoped short-lived keypair, and returns it. It is
//! never in the data path.
//!
//! This crate lives in the elide workspace during the design phase and
//! is deliberately free of `elide-*` dependencies — it is destined to
//! become a standalone OSS project. The Tigris IAM call is the one
//! production-coupled piece and sits behind [`iam::KeypairMinter`]; the
//! prototype ships [`iam::FakeMinter`] so the whole flow runs without a
//! live account.

pub mod audit;
pub mod caveat;
pub mod client;
pub mod config;
pub mod http;
pub mod iam;
pub mod issuance;
pub mod macaroon;
pub mod pop;
pub mod role;
pub mod state;
pub mod template;
pub mod tigris;

pub use caveat::{Caveat, Resolved};
pub use config::Config;
pub use macaroon::Macaroon;
