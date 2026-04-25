// Proptest coverage for the retention-marker parsers.
//
// The reaper acts on whatever these parsers accept. The invariant
// exercised here is totality: no byte string can panic the parser,
// every Ok parse round-trips through the canonical renderer.

use elide_coordinator::pending_delete::{parse_marker_body, parse_marker_key, render_marker};
use proptest::prelude::*;
use ulid::Ulid;

fn arb_ulid() -> impl Strategy<Value = Ulid> {
    any::<u128>().prop_map(Ulid::from)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 4096,
        ..ProptestConfig::default()
    })]

    /// Arbitrary byte strings must never panic `parse_marker_body`.
    /// On Ok, every parsed ULID round-trips back to the rendered body.
    #[test]
    fn parse_marker_body_total(s in ".{0,4096}") {
        match parse_marker_body(&s) {
            Ok(ulids) => {
                let rendered = render_marker(&ulids);
                let reparsed = parse_marker_body(&rendered).unwrap();
                prop_assert_eq!(reparsed, ulids);
            }
            Err(_) => {}
        }
    }

    /// `render_marker` always produces a body that `parse_marker_body`
    /// accepts, recovering the original list.
    #[test]
    fn render_then_parse_is_identity(ulids in prop::collection::vec(arb_ulid(), 0..32)) {
        let rendered = render_marker(&ulids);
        let parsed = parse_marker_body(&rendered).unwrap();
        prop_assert_eq!(parsed, ulids);
    }

    /// `parse_marker_key` is total. An Ok result reconstructs the
    /// original key bit-for-bit.
    #[test]
    fn parse_marker_key_total(s in ".{0,256}") {
        if let Ok((vol, gc_output)) = parse_marker_key(&s) {
            let reconstructed = format!("by_id/{vol}/retention/{gc_output}");
            prop_assert_eq!(reconstructed, s);
        }
    }
}
