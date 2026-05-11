//! Tigris IAM client + per-volume policy templates.
//!
//! Tigris exposes only the AWS IAM Query API at `https://iam.storage.dev`.
//! There is no Tigris-native IAM REST endpoint. This crate speaks that
//! Query API directly: SigV4-signed form-encoded POSTs, XML responses.
//!
//! Scope: just the operations needed by the elide coordinator's
//! per-volume key model (`docs/design-iam-key-model.md`):
//!
//! * `CreateAccessKey` / `DeleteAccessKey`
//! * `CreatePolicy` / `DeletePolicy`
//! * `AttachUserPolicy` / `DetachUserPolicy`
//! * `ListPolicies`
//!
//! Tigris's `AttachUserPolicy` takes an **access key ID** in the
//! `UserName` slot — Tigris has no IAM users; policies attach to keys
//! directly. The wire-level operation name keeps AWS's spelling for
//! tooling compatibility.

pub mod client;
pub mod error;
pub mod policy;

pub use client::{TigrisIamClient, TigrisIamConfig};
pub use error::IamError;
pub use policy::{CoordinatorWriterPolicy, PerVolumeReadOnlyPolicy, PolicyDocument};
