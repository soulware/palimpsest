//! Policy-template rendering (`docs/design-mint.md` § *Templating*).
//!
//! Three substitution classes are exposed to a role's policy template:
//!
//! - `{{tenant.X}}`            — server-side config (`tenant.bucket`).
//! - `{{caveat "elide:X"}}`    — verified-macaroon caveat, looked up
//!   through a registered `caveat` helper. Scalars render directly;
//!   list caveats iterate as `{{#each (caveat "elide:X")}}`.
//! - `{{system.X}}`            — mint-computed (`system.expiry_iso8601`).
//!
//! Caveats are reached through the `caveat` *helper* — not a
//! `{{caveat.X}}` data path — for two reasons:
//!
//! 1. The design doc namespaces caveats with `:` (`elide:Volume`),
//!    which is not a legal handlebars path segment. The helper takes
//!    the name as a string argument, so the doc's `:` convention is
//!    preserved unchanged (no issuer-side rename).
//! 2. It tightens the "mint ships no policy DSL" property: the only
//!    template surface is `{{tenant.*}}` / `{{system.*}}` plain paths,
//!    one `caveat` lookup helper, and the built-in `{{#each}}`. There
//!    is no arbitrary data-graph traversal.
//!
//! The helper resolves names against the **effective** caveat set
//! ([`crate::caveat::EffectiveCaveats::effective`]) — list caveats are
//! intersected, scalars must agree — so the minted policy reflects
//! exactly the authority the gate evaluated, never a broader
//! last-occurrence view.

use std::collections::BTreeMap;

use handlebars::{
    Context, Handlebars, Helper, HelperDef, RenderContext, RenderError, RenderErrorReason,
    ScopedJson,
};
use serde_json::{Map, Value};

use crate::caveat::{Caveat, CaveatValue, EffectiveCaveats};
use crate::config::Tenant;

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("render policy: {0}")]
    Render(#[from] handlebars::RenderError),
    #[error("rendered policy is not valid JSON: {0}")]
    NotJson(serde_json::Error),
}

/// The `caveat` lookup helper. Holds the precomputed effective-caveat
/// map; `{{caveat "name"}}` / `{{#each (caveat "name")}}` resolve
/// against it. An unknown name is a hard render error (strict mode):
/// a role template that references a caveat the macaroon doesn't carry
/// must fail closed, not mint an unscoped credential.
struct CaveatHelper {
    effective: BTreeMap<String, Value>,
}

impl HelperDef for CaveatHelper {
    fn call_inner<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
    ) -> Result<ScopedJson<'rc>, RenderError> {
        let name = h
            .param(0)
            .and_then(|p| p.value().as_str())
            .ok_or(RenderErrorReason::ParamNotFoundForIndex("caveat", 0))?;
        let value = self.effective.get(name).ok_or_else(|| {
            RenderErrorReason::Other(format!("caveat not present or unsatisfiable: {name}"))
        })?;
        Ok(ScopedJson::Derived(value.clone()))
    }
}

/// Build the effective-caveat map: one entry per distinct caveat name,
/// value computed under intersection semantics. A self-contradictory
/// scalar caveat resolves to absent (omitted), so any template
/// referencing it fails closed.
fn effective_map(caveats: &[Caveat]) -> BTreeMap<String, Value> {
    let eff = EffectiveCaveats::new(caveats);
    let mut map = BTreeMap::new();
    for name in eff.names() {
        if let Some(v) = eff.effective(name) {
            let json = match v {
                CaveatValue::Scalar(s) => Value::String(s),
                CaveatValue::List(items) => {
                    Value::Array(items.into_iter().map(Value::String).collect())
                }
            };
            map.insert(name.to_string(), json);
        }
    }
    map
}

/// Render `policy_template` into a concrete IAM policy JSON string.
pub fn render_policy(
    policy_template: &str,
    tenant: &Tenant,
    caveats: &[Caveat],
    expiry_iso8601: &str,
) -> Result<String, TemplateError> {
    let mut reg = Handlebars::new();
    // Policies are JSON, not HTML — disable entity escaping.
    reg.register_escape_fn(handlebars::no_escape);
    // A missing variable is a misconfigured role, not an empty string.
    reg.set_strict_mode(true);
    reg.register_helper(
        "caveat",
        Box::new(CaveatHelper {
            effective: effective_map(caveats),
        }),
    );

    let mut tenant_map = Map::new();
    tenant_map.insert("bucket".into(), Value::String(tenant.bucket.clone()));

    let mut system_map = Map::new();
    system_map.insert(
        "expiry_iso8601".into(),
        Value::String(expiry_iso8601.to_string()),
    );

    let mut data = Map::new();
    data.insert("tenant".into(), Value::Object(tenant_map));
    data.insert("system".into(), Value::Object(system_map));

    let rendered = reg.render_template(policy_template, &Value::Object(data))?;
    serde_json::from_str::<Value>(&rendered).map_err(TemplateError::NotJson)?;
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> Tenant {
        Tenant {
            bucket: "demo".into(),
        }
    }

    const TPL: &str = r#"{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:GetObject"],
    "Resource": [
      "arn:aws:s3:::{{tenant.bucket}}/by_id/{{caveat "elide:Volume"}}/*"
      {{#each (caveat "elide:Ancestors")}},
      "arn:aws:s3:::{{../tenant.bucket}}/by_id/{{this}}/*"
      {{/each}}
    ],
    "Condition": {"DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}}
  }]
}"#;

    #[test]
    fn renders_scalar_and_list_and_system() {
        let caveats = vec![
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::list("elide:Ancestors", ["ANC1", "ANC2"]),
        ];
        let out = render_policy(TPL, &tenant(), &caveats, "2026-05-15T14:30:00Z").unwrap();
        assert!(out.contains("demo/by_id/VOL1/*"));
        assert!(out.contains("by_id/ANC1/*"));
        assert!(out.contains("by_id/ANC2/*"));
        assert!(out.contains("2026-05-15T14:30:00Z"));
        serde_json::from_str::<Value>(&out).expect("valid json");
    }

    #[test]
    fn list_caveat_uses_intersection_not_last_occurrence() {
        // Issued [A,B,C], attenuated to [A,B] then [B,C]:
        // effective authority is the intersection {B}. A last-
        // occurrence renderer would wrongly emit B AND C.
        let caveats = vec![
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::list("elide:Ancestors", ["A", "B", "C"]),
            Caveat::list("elide:Ancestors", ["A", "B"]),
            Caveat::list("elide:Ancestors", ["B", "C"]),
        ];
        let out = render_policy(TPL, &tenant(), &caveats, "t").unwrap();
        assert!(out.contains("by_id/B/*"), "out: {out}");
        assert!(!out.contains("by_id/A/*"), "A leaked: {out}");
        assert!(!out.contains("by_id/C/*"), "C leaked: {out}");
    }

    #[test]
    fn unknown_caveat_is_error() {
        let err = render_policy(r#"{{caveat "nope"}}"#, &tenant(), &[], "x");
        assert!(matches!(err, Err(TemplateError::Render(_))));
    }

    #[test]
    fn contradictory_scalar_caveat_fails_closed() {
        // Two disagreeing scalar occurrences ⇒ unsatisfiable ⇒ omitted
        // ⇒ template referencing it errors rather than minting.
        let caveats = vec![
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::scalar("elide:Volume", "VOL2"),
        ];
        let err = render_policy(r#"{{caveat "elide:Volume"}}"#, &tenant(), &caveats, "x");
        assert!(matches!(err, Err(TemplateError::Render(_))));
    }
}
