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

use crate::caveat::{Caveat, EffectiveCaveats, Resolved};
use crate::config::Tenant;

#[derive(Debug, thiserror::Error)]
pub enum TemplateError {
    #[error("render policy: {0}")]
    Render(#[from] handlebars::RenderError),
    #[error("compile policy template: {0}")]
    Compile(#[from] handlebars::TemplateError),
    #[error("rendered policy is not valid JSON: {0}")]
    NotJson(serde_json::Error),
}

/// The `caveat` scalar lookup helper. Holds the resolved-caveat map;
/// `{{caveat "name"}}` resolves against it. A name that is absent **or
/// unsatisfiable** is a hard render error (fail closed): a role
/// template referencing a caveat the macaroon doesn't carry — or whose
/// occurrences contradict — must never mint an unscoped or downgraded
/// credential. All caveats are scalar; there is no `{{#each}}` over a
/// caveat (ancestor-style lists are PoP-signed `request.*` data).
struct CaveatHelper {
    resolved: BTreeMap<String, String>,
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
        let value = self.resolved.get(name).ok_or_else(|| {
            RenderErrorReason::Other(format!("caveat not present or unsatisfiable: {name}"))
        })?;
        Ok(ScopedJson::Derived(Value::String(value.clone())))
    }
}

/// Build the resolved-caveat map: one entry per distinct caveat name
/// whose chain occurrences resolve to a single agreed value. `Absent`
/// and `Unsatisfiable` names are omitted, so a template referencing
/// either fails the render closed.
fn resolved_map(caveats: &[Caveat]) -> BTreeMap<String, String> {
    let eff = EffectiveCaveats::new(caveats);
    let mut map = BTreeMap::new();
    for name in eff.names() {
        if let Resolved::Value(v) = eff.resolve(name) {
            map.insert(name.to_string(), v);
        }
    }
    map
}

/// Render `policy_template` into a concrete IAM policy JSON string.
///
/// `request` is the **PoP-verified** request body (its provenance is
/// `coordinator.key`, bound to this macaroon and moment — see
/// [`crate::pop`]); it is exposed as the `request.*` namespace. The
/// caller must verify the PoP signature *before* passing the body here.
/// Each substitution class has a distinct, explicit trust provenance:
/// `caveat.*` MAC-bound, `request.*` PoP-bound, `tenant.*` config,
/// `system.*` mint-computed.
pub fn render_policy(
    policy_template: &str,
    tenant: &Tenant,
    caveats: &[Caveat],
    request: &Value,
    expiry_iso8601: &str,
    role: &str,
) -> Result<String, TemplateError> {
    let mut reg = Handlebars::new();
    // Policies are JSON, not HTML — disable entity escaping.
    reg.register_escape_fn(handlebars::no_escape);
    // A missing variable is a misconfigured role, not an empty string.
    reg.set_strict_mode(true);
    reg.register_helper(
        "caveat",
        Box::new(CaveatHelper {
            resolved: resolved_map(caveats),
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
    data.insert("request".into(), request.clone());

    // Register under the role name so handlebars error messages name
    // the role ("...rendering \"read\"...") instead of the opaque
    // "Unnamed template" that render_template's anonymous path emits.
    reg.register_template_string(role, policy_template)?;
    let rendered = reg.render(role, &Value::Object(data))?;
    serde_json::from_str::<Value>(&rendered).map_err(TemplateError::NotJson)?;
    Ok(rendered)
}

/// The substitution surface a policy template references, grouped by
/// trust provenance (`docs/design-mint.md` § *Templating*): `caveats`
/// MAC-bound, `request` PoP-bound, `tenant` config, `system`
/// mint-computed. Each list is sorted and de-duplicated.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TemplateSurface {
    pub caveats: Vec<String>,
    pub tenant: Vec<String>,
    pub system: Vec<String>,
    pub request: Vec<String>,
}

/// Extract the [`TemplateSurface`] of a policy template by scanning the
/// four documented token shapes — `{{caveat "name"}}`, `{{tenant.*}}`,
/// `{{system.*}}`, `{{request.*}}` (with optional `../`/`./` scope
/// prefixes and `(…)` subexpression wrapping). Lets `mint role inspect`
/// state what a role's policy depends on without rendering it:
/// rendering needs a live verified request and fails closed on any
/// absent caveat, so there is no static "what this grants" to show.
pub fn template_surface(template: &str) -> TemplateSurface {
    let mut s = TemplateSurface::default();
    let mut i = 0;
    while let Some(open) = template[i..].find("{{") {
        let start = i + open + 2;
        let Some(rel_close) = template[start..].find("}}") else {
            break;
        };
        let end = start + rel_close;
        // Inner span; trim mustache modifiers (block #, close /, raw {,
        // unescape ~) and whitespace at the edges.
        let inner = template[start..end]
            .trim_matches(|c: char| c.is_whitespace() || matches!(c, '{' | '}' | '#' | '~'));
        i = end + 2;
        if inner.starts_with('/') || inner.starts_with('!') || inner.starts_with('>') {
            continue; // block close, comment, partial — no data refs
        }
        let mut tokens = inner.split_whitespace().peekable();
        while let Some(tok) = tokens.next() {
            // `caveat "name"` / `(caveat "name")` — the name is the
            // next token, quote- and paren-stripped (caveat names carry
            // `:`, so only trim wrapping punctuation).
            if tok.trim_start_matches('(') == "caveat" {
                if let Some(arg) = tokens.peek() {
                    let name = arg.trim_matches(|c: char| matches!(c, '(' | ')' | '"' | '\''));
                    if !name.is_empty() {
                        s.caveats.push(name.to_string());
                    }
                }
                continue;
            }
            // A plain path: strip a leading `(`, any number of `../`
            // and a `./` scope prefix, and a trailing `)`.
            let mut p = tok.trim_start_matches('(').trim_end_matches(')');
            while let Some(rest) = p.strip_prefix("../") {
                p = rest;
            }
            p = p.strip_prefix("./").unwrap_or(p);
            let bucket = if p == "tenant" || p.starts_with("tenant.") {
                Some(&mut s.tenant)
            } else if p == "system" || p.starts_with("system.") {
                Some(&mut s.system)
            } else if p == "request" || p.starts_with("request.") {
                Some(&mut s.request)
            } else {
                None
            };
            if let Some(v) = bucket {
                v.push(p.to_string());
            }
        }
    }
    for v in [&mut s.caveats, &mut s.tenant, &mut s.system, &mut s.request] {
        v.sort();
        v.dedup();
    }
    s
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
      {{#each request.ancestors}},
      "arn:aws:s3:::{{../tenant.bucket}}/by_id/{{this}}/*"
      {{/each}}
    ],
    "Condition": {"DateLessThan": {"aws:CurrentTime": "{{system.expiry_iso8601}}"}}
  }]
}"#;

    fn req(ancestors: &[&str]) -> Value {
        serde_json::json!({ "ancestors": ancestors })
    }

    #[test]
    fn renders_scalar_caveat_signed_request_list_and_system() {
        let caveats = vec![Caveat::scalar("elide:Volume", "VOL1")];
        let out = render_policy(
            TPL,
            &tenant(),
            &caveats,
            &req(&["ANC1", "ANC2"]),
            "2026-05-15T14:30:00Z",
            "volume-ro",
        )
        .unwrap();
        assert!(out.contains("demo/by_id/VOL1/*"));
        assert!(out.contains("by_id/ANC1/*"));
        assert!(out.contains("by_id/ANC2/*"));
        assert!(out.contains("2026-05-15T14:30:00Z"));
        serde_json::from_str::<Value>(&out).expect("valid json");
    }

    #[test]
    fn empty_request_ancestors_renders_self_only() {
        // Maximal narrowing — zero ancestors is a coherent grant, not
        // an error: the {{#each}} simply emits nothing.
        let caveats = vec![Caveat::scalar("elide:Volume", "VOL1")];
        let out = render_policy(TPL, &tenant(), &caveats, &req(&[]), "t", "volume-ro").unwrap();
        assert!(out.contains("by_id/VOL1/*"));
        assert!(!out.contains("by_id//*"));
        serde_json::from_str::<Value>(&out).expect("valid json");
    }

    #[test]
    fn unknown_caveat_is_error() {
        let err = render_policy(r#"{{caveat "nope"}}"#, &tenant(), &[], &req(&[]), "x", "r");
        assert!(matches!(err, Err(TemplateError::Render(_))));
    }

    #[test]
    fn missing_request_field_fails_closed() {
        // Strict mode: a template referencing request.ancestors when
        // the signed body omitted it must fail the render, not mint.
        let caveats = vec![Caveat::scalar("elide:Volume", "VOL1")];
        let err = render_policy(TPL, &tenant(), &caveats, &serde_json::json!({}), "t", "r");
        assert!(matches!(err, Err(TemplateError::Render(_))));
    }

    #[test]
    fn render_error_names_the_role_not_unnamed_template() {
        // Operator-facing: the handlebars message must point at the
        // role, not the opaque "Unnamed template".
        let err = render_policy(
            "{{request.prefix}}",
            &tenant(),
            &[],
            &serde_json::json!({}),
            "t",
            "read",
        )
        .expect_err("missing request.prefix must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("\"read\""),
            "message should name the role: {msg}"
        );
        assert!(
            !msg.contains("Unnamed template"),
            "message still anonymous: {msg}"
        );
    }

    #[test]
    fn contradictory_scalar_caveat_fails_closed() {
        // Two disagreeing scalar occurrences ⇒ Unsatisfiable ⇒ omitted
        // from the resolved map ⇒ template referencing it errors
        // rather than minting a downgraded credential.
        let caveats = vec![
            Caveat::scalar("elide:Volume", "VOL1"),
            Caveat::scalar("elide:Volume", "VOL2"),
        ];
        let err = render_policy(
            r#"{{caveat "elide:Volume"}}"#,
            &tenant(),
            &caveats,
            &req(&[]),
            "x",
            "r",
        );
        assert!(matches!(err, Err(TemplateError::Render(_))));
    }

    #[test]
    fn surface_groups_refs_by_provenance_through_scopes_and_subexprs() {
        // TPL exercises every shape: a scalar caveat, a `../`-scoped
        // tenant ref inside an #each block, a request.* block path, and
        // a system.* ref. `{{this}}` and the `each` helper are not data
        // refs and must not leak in.
        let s = template_surface(TPL);
        assert_eq!(s.caveats, vec!["elide:Volume"]);
        assert_eq!(s.tenant, vec!["tenant.bucket"]); // ../ scope folded
        assert_eq!(s.system, vec!["system.expiry_iso8601"]);
        assert_eq!(s.request, vec!["request.ancestors"]);

        // Subexpression form `{{#each (caveat "elide:X")}}` and a bare
        // namespace token both resolve; duplicates collapse.
        let s = template_surface(
            r#"{{caveat "a"}} {{caveat "a"}} {{#each (caveat "b")}}{{tenant}}{{/each}}"#,
        );
        assert_eq!(s.caveats, vec!["a", "b"]);
        assert_eq!(s.tenant, vec!["tenant"]);
        assert!(s.system.is_empty() && s.request.is_empty());

        // Comments/partials contribute nothing.
        assert_eq!(
            template_surface("{{! caveat \"x\" }}{{> partial}}"),
            TemplateSurface::default()
        );
    }
}
