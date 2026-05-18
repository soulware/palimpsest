//! Shared integration-test harness. Mirrors the crate-private
//! `config::parse_for_test`: write each role's policy into a tempdir,
//! splice an absolute `roles_dir` into the TOML, and parse via the real
//! file-read path. The tempdir only needs to outlive the parse —
//! `policy` is read eagerly — so it is dropped on return.

use mint::Config;

pub fn parse_config(toml: &str, roles: &[(&str, &str)]) -> Config {
    let dir = tempfile::tempdir().expect("tempdir");
    for (name, body) in roles {
        std::fs::write(dir.path().join(name), body).expect("write role file");
    }
    let injected = toml.replacen(
        "[tenant]",
        &format!(
            "roles_dir = {:?}\n[tenant]",
            dir.path().display().to_string()
        ),
        1,
    );
    Config::from_toml_str(&injected).expect("config parses")
}
