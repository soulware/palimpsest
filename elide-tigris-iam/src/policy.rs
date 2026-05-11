//! Policy document templates for the per-volume key model.
//!
//! Only the per-volume read-only policy is implemented here — that's the
//! initial-pass scope of `docs/design-iam-key-model.md`. Writer,
//! peer-fetch, and ephemeral-fetch policy templates land alongside
//! their respective key-class wiring later.

use serde::Serialize;
use ulid::Ulid;

/// A complete IAM policy document, serialised as JSON for the
/// `CreatePolicy` request's `PolicyDocument` parameter.
#[derive(Serialize)]
pub struct PolicyDocument {
    #[serde(rename = "Version")]
    pub version: &'static str,
    #[serde(rename = "Statement")]
    pub statement: Vec<Statement>,
}

#[derive(Serialize)]
pub struct Statement {
    #[serde(rename = "Sid", skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: &'static str,
    #[serde(rename = "Action")]
    pub action: Vec<&'static str>,
    #[serde(rename = "Resource")]
    pub resource: Vec<String>,
    #[serde(rename = "Condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<Condition>,
}

#[derive(Serialize)]
pub struct Condition {
    #[serde(rename = "DateLessThan", skip_serializing_if = "Option::is_none")]
    pub date_less_than: Option<DateLessThan>,
    #[serde(rename = "IpAddress", skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<IpAddressCondition>,
}

#[derive(Serialize)]
pub struct DateLessThan {
    #[serde(rename = "aws:CurrentTime")]
    pub aws_current_time: String,
}

#[derive(Serialize)]
pub struct IpAddressCondition {
    #[serde(rename = "aws:SourceIp")]
    pub aws_source_ip: Vec<String>,
}

/// Builder for a per-volume read-only policy.
///
/// The resource set covers `by_id/<self>/*` and one entry per ancestor
/// in the lineage, mirroring the policy sketch in
/// `docs/design-iam-key-model.md` § "Per-volume read-only key".
pub struct PerVolumeReadOnlyPolicy<'a> {
    pub bucket: &'a str,
    pub vol_ulid: &'a Ulid,
    pub ancestor_ulids: &'a [Ulid],
    /// ISO-8601 timestamp the `DateLessThan` condition fires at.
    pub expiry_iso8601: &'a str,
    /// Optional egress IP pin. None disables the `IpAddress` condition.
    pub source_ips: Option<&'a [String]>,
}

impl PerVolumeReadOnlyPolicy<'_> {
    pub fn build(&self) -> PolicyDocument {
        let mut resource = Vec::with_capacity(1 + self.ancestor_ulids.len());
        resource.push(format!(
            "arn:aws:s3:::{bucket}/by_id/{vol}/*",
            bucket = self.bucket,
            vol = self.vol_ulid
        ));
        for a in self.ancestor_ulids {
            resource.push(format!(
                "arn:aws:s3:::{bucket}/by_id/{a}/*",
                bucket = self.bucket
            ));
        }

        let condition = Some(Condition {
            date_less_than: Some(DateLessThan {
                aws_current_time: self.expiry_iso8601.to_owned(),
            }),
            ip_address: self.source_ips.map(|ips| IpAddressCondition {
                aws_source_ip: ips.to_vec(),
            }),
        });

        PolicyDocument {
            version: "2012-10-17",
            statement: vec![Statement {
                sid: Some("ReadVolumeAndAncestors".to_owned()),
                effect: "Allow",
                action: vec!["s3:GetObject"],
                resource,
                condition,
            }],
        }
    }
}

impl PolicyDocument {
    /// Render as the JSON form Tigris's `CreatePolicy` expects in the
    /// `PolicyDocument` request parameter.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// Builder for the one-per-coordinator writer policy.
///
/// Mirrors the JSON sketch in `docs/design-iam-key-model.md` §
/// "Coordinator writer key": Get/Put/Delete on `by_id/*` and `names/*`;
/// Get/Put (no Delete) on `events/*` and `coordinators/*` so the
/// append-only and immutable invariants hold at the IAM layer; bucket-
/// wide ListBucket. No `DateLessThan` — writer-key rotation is
/// operator-driven, not time-bounded.
pub struct CoordinatorWriterPolicy<'a> {
    pub bucket: &'a str,
}

impl CoordinatorWriterPolicy<'_> {
    pub fn build(&self) -> PolicyDocument {
        let bucket = self.bucket;
        let by_id = format!("arn:aws:s3:::{bucket}/by_id/*");
        let names = format!("arn:aws:s3:::{bucket}/names/*");
        let events = format!("arn:aws:s3:::{bucket}/events/*");
        let coords = format!("arn:aws:s3:::{bucket}/coordinators/*");
        let bucket_arn = format!("arn:aws:s3:::{bucket}");

        PolicyDocument {
            version: "2012-10-17",
            statement: vec![
                Statement {
                    sid: Some("VolumeData".to_owned()),
                    effect: "Allow",
                    action: vec!["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    resource: vec![by_id],
                    condition: None,
                },
                Statement {
                    sid: Some("Names".to_owned()),
                    effect: "Allow",
                    action: vec!["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    resource: vec![names],
                    condition: None,
                },
                Statement {
                    sid: Some("EventLogAppendOnly".to_owned()),
                    effect: "Allow",
                    action: vec!["s3:GetObject", "s3:PutObject"],
                    resource: vec![events],
                    condition: None,
                },
                Statement {
                    sid: Some("CoordinatorIdentityImmutable".to_owned()),
                    effect: "Allow",
                    action: vec!["s3:GetObject", "s3:PutObject"],
                    resource: vec![coords],
                    condition: None,
                },
                Statement {
                    sid: Some("BucketList".to_owned()),
                    effect: "Allow",
                    action: vec!["s3:ListBucket"],
                    resource: vec![bucket_arn],
                    condition: None,
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn ro_policy_self_only() {
        let vol = Ulid::nil();
        let pol = PerVolumeReadOnlyPolicy {
            bucket: "elide-test",
            vol_ulid: &vol,
            ancestor_ulids: &[],
            expiry_iso8601: "2030-01-01T00:00:00Z",
            source_ips: None,
        }
        .build();
        let json = pol.to_json().unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["Version"], "2012-10-17");
        let stmt = &v["Statement"][0];
        assert_eq!(stmt["Effect"], "Allow");
        assert_eq!(stmt["Action"][0], "s3:GetObject");
        assert_eq!(stmt["Resource"].as_array().unwrap().len(), 1);
        assert!(
            stmt["Resource"][0]
                .as_str()
                .unwrap()
                .contains("/by_id/00000000000000000000000000/*")
        );
        assert_eq!(
            stmt["Condition"]["DateLessThan"]["aws:CurrentTime"],
            "2030-01-01T00:00:00Z"
        );
        assert!(stmt["Condition"]["IpAddress"].is_null());
    }

    #[test]
    fn writer_policy_shape() {
        let pol = CoordinatorWriterPolicy { bucket: "b" }.build();
        let json = pol.to_json().unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["Version"], "2012-10-17");
        let stmts = v["Statement"].as_array().unwrap();
        assert_eq!(stmts.len(), 5);

        let sid_to_stmt: std::collections::HashMap<&str, &Value> = stmts
            .iter()
            .map(|s| (s["Sid"].as_str().unwrap(), s))
            .collect();

        let by_id = sid_to_stmt["VolumeData"];
        let by_id_actions: Vec<&str> = by_id["Action"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(by_id_actions.contains(&"s3:DeleteObject"));
        assert_eq!(by_id["Resource"][0], "arn:aws:s3:::b/by_id/*");

        let events = sid_to_stmt["EventLogAppendOnly"];
        let event_actions: Vec<&str> = events["Action"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(!event_actions.contains(&"s3:DeleteObject"));
        assert_eq!(events["Resource"][0], "arn:aws:s3:::b/events/*");

        let coords = sid_to_stmt["CoordinatorIdentityImmutable"];
        let coord_actions: Vec<&str> = coords["Action"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert!(!coord_actions.contains(&"s3:DeleteObject"));

        let list = sid_to_stmt["BucketList"];
        assert_eq!(list["Resource"][0], "arn:aws:s3:::b");
    }

    #[test]
    fn ro_policy_includes_ancestors_and_ip_condition() {
        let vol = Ulid::from(1u128);
        let parent = Ulid::from(2u128);
        let grand = Ulid::from(3u128);
        let ips = vec!["203.0.113.5/32".to_owned()];
        let pol = PerVolumeReadOnlyPolicy {
            bucket: "b",
            vol_ulid: &vol,
            ancestor_ulids: &[parent, grand],
            expiry_iso8601: "2030-01-01T00:00:00Z",
            source_ips: Some(&ips),
        }
        .build();
        let json = pol.to_json().unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        let resources = v["Statement"][0]["Resource"].as_array().unwrap();
        assert_eq!(resources.len(), 3);
        assert_eq!(
            v["Statement"][0]["Condition"]["IpAddress"]["aws:SourceIp"][0],
            "203.0.113.5/32"
        );
    }
}
