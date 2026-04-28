//! Bucket-level read/write helpers for `names/<name>` records.
//!
//! Phase 2 of the portable-live-volume work uses this module to claim,
//! update, and release names. Every write is mediated by S3 conditional
//! PUT — see `crate::portable` — so the helpers carry an
//! `UpdateVersion` (the ETag observed at read time) that callers must
//! pass back on `update_name_record` to detect concurrent modification.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutResult, UpdateVersion};

use elide_core::name_record::{NameRecord, ParseNameRecordError};

use crate::portable::{ConditionalPutError, put_if_absent, put_with_match};

/// Errors from `name_store` operations.
#[derive(Debug)]
pub enum NameStoreError {
    /// Failed to serialise a `NameRecord` as TOML.
    Serialise(toml::ser::Error),
    /// Failed to parse a `NameRecord` from TOML.
    Parse(ParseNameRecordError),
    /// The underlying store reported an error.
    Store(object_store::Error),
    /// Conditional precondition failed (e.g. another coordinator
    /// modified the record between our read and write).
    PreconditionFailed,
    /// Bytes returned from the store are not valid UTF-8.
    NotUtf8,
}

impl std::fmt::Display for NameStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialise(e) => write!(f, "serialising NameRecord: {e}"),
            Self::Parse(e) => write!(f, "parsing NameRecord: {e}"),
            Self::Store(e) => write!(f, "{e}"),
            Self::PreconditionFailed => write!(f, "names/<name> changed underneath us"),
            Self::NotUtf8 => write!(f, "names/<name> body is not valid UTF-8"),
        }
    }
}

impl std::error::Error for NameStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialise(e) => Some(e),
            Self::Parse(e) => Some(e),
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<object_store::Error> for NameStoreError {
    fn from(e: object_store::Error) -> Self {
        Self::Store(e)
    }
}

impl From<ConditionalPutError> for NameStoreError {
    fn from(e: ConditionalPutError) -> Self {
        match e {
            ConditionalPutError::PreconditionFailed => Self::PreconditionFailed,
            ConditionalPutError::Other(e) => Self::Store(e),
        }
    }
}

fn name_key(name: &str) -> StorePath {
    StorePath::from(format!("names/{name}"))
}

fn serialise(record: &NameRecord) -> Result<Bytes, NameStoreError> {
    let s = record.to_toml().map_err(NameStoreError::Serialise)?;
    Ok(Bytes::from(s.into_bytes()))
}

fn parse(bytes: &[u8]) -> Result<NameRecord, NameStoreError> {
    let s = std::str::from_utf8(bytes).map_err(|_| NameStoreError::NotUtf8)?;
    NameRecord::from_toml(s).map_err(NameStoreError::Parse)
}

fn put_result_to_version(r: PutResult) -> UpdateVersion {
    UpdateVersion::from(r)
}

/// Read `names/<name>`. Returns `Ok(None)` if the key is absent.
///
/// On success returns the parsed record and an `UpdateVersion` that
/// must be supplied to `update_name_record` for any subsequent
/// conditional update.
pub async fn read_name_record(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> Result<Option<(NameRecord, UpdateVersion)>, NameStoreError> {
    let key = name_key(name);
    let got = match store.get(&key).await {
        Ok(g) => g,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(NameStoreError::Store(e)),
    };
    let meta = got.meta.clone();
    let bytes = got.bytes().await.map_err(NameStoreError::Store)?;
    let record = parse(&bytes)?;
    let version = UpdateVersion {
        e_tag: meta.e_tag,
        version: meta.version,
    };
    Ok(Some((record, version)))
}

/// Atomically create `names/<name>` only if it does not already exist
/// (`If-None-Match: *`). Returns the `UpdateVersion` of the newly
/// created object on success; returns `PreconditionFailed` if the key
/// is already present.
pub async fn create_name_record(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    record: &NameRecord,
) -> Result<UpdateVersion, NameStoreError> {
    let body = serialise(record)?;
    let key = name_key(name);
    let r = put_if_absent(store.as_ref(), &key, body).await?;
    Ok(put_result_to_version(r))
}

/// Atomically replace `names/<name>` only if its current version
/// matches `expected` (`If-Match: <etag>`). Returns the new
/// `UpdateVersion`; returns `PreconditionFailed` if the version
/// differs (another coordinator modified the record since we read it)
/// or the key has been deleted.
pub async fn update_name_record(
    store: &Arc<dyn ObjectStore>,
    name: &str,
    record: &NameRecord,
    expected: UpdateVersion,
) -> Result<UpdateVersion, NameStoreError> {
    let body = serialise(record)?;
    let key = name_key(name);
    let r = put_with_match(store.as_ref(), &key, body, expected).await?;
    Ok(put_result_to_version(r))
}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_core::name_record::NameState;
    use object_store::memory::InMemory;
    use ulid::Ulid;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn sample_ulid() -> Ulid {
        Ulid::from_string("01J0000000000000000000000V").unwrap()
    }

    #[tokio::test]
    async fn read_returns_none_for_absent() {
        let s = store();
        let r = read_name_record(&s, "nope").await.unwrap();
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn create_then_read_round_trips() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());

        create_name_record(&s, "vol", &rec).await.unwrap();

        let (got, _v) = read_name_record(&s, "vol").await.unwrap().unwrap();
        assert_eq!(got.vol_ulid, sample_ulid());
        assert_eq!(got.state, NameState::Live);
    }

    #[tokio::test]
    async fn create_rejects_duplicate() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());

        create_name_record(&s, "vol", &rec).await.unwrap();
        let err = create_name_record(&s, "vol", &rec)
            .await
            .expect_err("second create should fail");
        assert!(matches!(err, NameStoreError::PreconditionFailed));
    }

    #[tokio::test]
    async fn update_with_correct_version_succeeds() {
        let s = store();
        let mut rec = NameRecord::live_minimal(sample_ulid());

        create_name_record(&s, "vol", &rec).await.unwrap();
        let (_r, v) = read_name_record(&s, "vol").await.unwrap().unwrap();

        rec.state = NameState::Stopped;
        update_name_record(&s, "vol", &rec, v).await.unwrap();

        let (got, _) = read_name_record(&s, "vol").await.unwrap().unwrap();
        assert_eq!(got.state, NameState::Stopped);
    }

    #[tokio::test]
    async fn update_with_stale_version_fails() {
        let s = store();
        let mut rec = NameRecord::live_minimal(sample_ulid());

        create_name_record(&s, "vol", &rec).await.unwrap();
        let (_r, stale) = read_name_record(&s, "vol").await.unwrap().unwrap();

        // Concurrent update by some other writer.
        rec.state = NameState::Stopped;
        let (_, fresh) = read_name_record(&s, "vol").await.unwrap().unwrap();
        update_name_record(&s, "vol", &rec, fresh).await.unwrap();

        // Our stale-version write must fail.
        rec.state = NameState::Released;
        let err = update_name_record(&s, "vol", &rec, stale)
            .await
            .expect_err("stale version must fail");
        assert!(matches!(err, NameStoreError::PreconditionFailed));
    }

    #[tokio::test]
    async fn update_of_missing_key_fails() {
        let s = store();
        let rec = NameRecord::live_minimal(sample_ulid());
        let err = update_name_record(
            &s,
            "missing",
            &rec,
            UpdateVersion {
                e_tag: Some("nonexistent".into()),
                version: None,
            },
        )
        .await
        .expect_err("update of missing key must fail");
        assert!(matches!(err, NameStoreError::PreconditionFailed));
    }
}
