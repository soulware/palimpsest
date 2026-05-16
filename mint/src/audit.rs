//! Audit log (`docs/design-mint.md` § *Audit log*). One JSON object per
//! `AssumeRole` call, appended as a line. v1 is local/file-based; sink
//! shipping is an operational concern, not a mint concern.
//!
//! Caveats are recorded by name+value but never secrets — the only
//! secret in the flow is the minted Tigris key, and that is recorded by
//! its **id** only.

use std::io::Write;
use std::sync::Mutex;

use serde::Serialize;

use crate::caveat::Caveat;

#[derive(Debug, Serialize)]
pub struct AuditEntry {
    pub timestamp: String,
    pub request_id: String,
    pub caller_address: String,
    pub macaroon_nonce: Option<String>,
    pub macaroon_caveats: Vec<AuditCaveat>,
    pub role: String,
    pub granted_ttl_seconds: Option<u64>,
    pub outcome: String,
    pub tigris_access_key_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AuditCaveat {
    pub name: String,
    pub value: String,
}

pub fn sanitise_caveats(caveats: &[Caveat]) -> Vec<AuditCaveat> {
    caveats
        .iter()
        .map(|c| AuditCaveat {
            name: c.name.clone(),
            value: c.value.clone(),
        })
        .collect()
}

/// A line-oriented audit sink. Wraps any `Write` (a file in production,
/// an in-memory buffer in tests).
pub struct AuditLog {
    sink: Mutex<Box<dyn Write + Send>>,
}

impl AuditLog {
    pub fn new(sink: Box<dyn Write + Send>) -> Self {
        Self {
            sink: Mutex::new(sink),
        }
    }

    /// Append one entry. A logging failure must not fail the request —
    /// it is logged to stderr via tracing and swallowed.
    pub fn record(&self, entry: &AuditEntry) {
        let line = match serde_json::to_string(entry) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "audit serialise failed");
                return;
            }
        };
        let Ok(mut sink) = self.sink.lock() else {
            tracing::error!("audit sink poisoned");
            return;
        };
        if let Err(e) = writeln!(sink, "{line}").and_then(|()| sink.flush()) {
            tracing::error!(error = %e, "audit write failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// `Write` into a shared buffer so a test can read back what the
    /// log wrote.
    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);
    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("poisoned"))?
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn writes_one_json_line() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let log = AuditLog::new(Box::new(SharedBuf(buf.clone())));
        log.record(&AuditEntry {
            timestamp: "t".into(),
            request_id: "r".into(),
            caller_address: "1.2.3.4".into(),
            macaroon_nonce: Some("abcd".into()),
            macaroon_caveats: sanitise_caveats(&[Caveat::scalar("Audience", "mint")]),
            role: "volume-ro".into(),
            granted_ttl_seconds: Some(3600),
            outcome: "granted".into(),
            tigris_access_key_id: Some("tid_fake_00000000".into()),
        });
        let written = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert_eq!(written.lines().count(), 1);
        assert!(written.contains("\"outcome\":\"granted\""));
        assert!(written.contains("\"Audience\""));
    }
}
