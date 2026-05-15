// Operator-token resolution and attenuation, CLI side.
//
// The persistent operator token is per-user and per-coordinator: it
// carries `Role::Operator` and a long `NotAfter`, but no `Op` or
// `Volume`. The per-token random identity is the struct-level nonce
// minted inside the coordinator's `mint_operator`, not a caveat. Each
// gated CLI verb narrows the token at the moment of use by appending a
// typed `Op` caveat, a `Volume` caveat scoped to the verb's target,
// and a short additional `NotAfter`. The narrowed token is what
// crosses the IPC boundary; the persistent token never leaves the
// operator's machine.
//
// Tokens live in a single per-user file, `~/.elide/tokens.toml`, with
// one entry per coordinator keyed by that coordinator's canonical
// data_dir. This keeps the trust boundary per-user (file mode 0600
// under `$HOME`) while letting several coordinators run on one host
// without sharing a token. `elide token create` upserts the entry for
// the addressed coordinator; gated verbs look it up by the same
// canonical data_dir they use to reach the socket.
//
// See docs/design-auth-model.md.

use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, io};

use elide_coordinator::macaroon::{Caveat, Macaroon, OperatorOp};
use serde::{Deserialize, Serialize};

/// Environment variable consulted after `--token` and before the
/// per-coordinator entry in `~/.elide/tokens.toml`.
pub const TOKEN_ENV_VAR: &str = "ELIDE_OPERATOR_TOKEN";

/// On-wire expiry the CLI attaches at use time. Short by design: the
/// attenuated token only needs to outlive the IPC round-trip plus
/// modest clock skew.
pub const USE_EXPIRY_SECS: u64 = 60;

/// On-disk shape of `~/.elide/tokens.toml`: an array of per-coordinator
/// entries. Unknown fields are preserved on read only insofar as we
/// rewrite the whole file from this struct — a future per-coordinator
/// field would need to be added here before it survives an upsert.
#[derive(Debug, Default, Serialize, Deserialize)]
struct TokenStore {
    #[serde(default)]
    coordinator: Vec<CoordinatorToken>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoordinatorToken {
    #[serde(rename = "data-dir")]
    data_dir: String,
    #[serde(rename = "operator-token")]
    operator_token: String,
}

/// Path to the per-user token file. Returns `None` when `$HOME` is
/// unset (e.g. minimal container environments).
pub fn tokens_file_path() -> Option<PathBuf> {
    let home = env::var_os("HOME")?;
    let mut p = PathBuf::from(home);
    p.push(".elide");
    p.push("tokens.toml");
    Some(p)
}

/// Canonical key for a coordinator's data_dir. Both `token create` and
/// gated verbs derive the lookup key this way, so they agree as long
/// as they were handed the same `--data-dir`. The directory exists in
/// both paths (the coordinator owns the socket beneath it), so
/// canonicalisation does not normally fail.
fn data_dir_key(data_dir: &Path) -> io::Result<String> {
    let canon = fs::canonicalize(data_dir)?;
    Ok(canon.to_string_lossy().into_owned())
}

fn load_store(path: &Path) -> io::Result<TokenStore> {
    match fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text)
            .map_err(|e| io::Error::other(format!("parsing {}: {e}", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(TokenStore::default()),
        Err(e) => Err(e),
    }
}

/// Resolve the persistent operator token: `--token` flag wins, then
/// `ELIDE_OPERATOR_TOKEN`, then the entry for `data_dir` in
/// `~/.elide/tokens.toml`. Trims trailing whitespace. Returns `None`
/// if no source has a non-empty value.
pub fn resolve(token_flag: Option<&str>, data_dir: &Path) -> io::Result<Option<String>> {
    if let Some(t) = token_flag {
        let t = t.trim();
        if !t.is_empty() {
            return Ok(Some(t.to_owned()));
        }
    }
    if let Ok(t) = env::var(TOKEN_ENV_VAR) {
        let t = t.trim();
        if !t.is_empty() {
            return Ok(Some(t.to_owned()));
        }
    }
    let Some(path) = tokens_file_path() else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    // A missing data_dir means no coordinator is reachable there; treat
    // it as "no token" rather than a hard error so the caller emits the
    // standard missing-token hint.
    let Ok(key) = data_dir_key(data_dir) else {
        return Ok(None);
    };
    let store = load_store(&path)?;
    let found = store
        .coordinator
        .into_iter()
        .find(|c| c.data_dir == key)
        .map(|c| c.operator_token);
    Ok(found.and_then(|t| {
        let t = t.trim();
        (!t.is_empty()).then(|| t.to_owned())
    }))
}

/// Upsert the operator token for `data_dir` into `~/.elide/tokens.toml`,
/// preserving every other coordinator's entry. Writes atomically with
/// mode 0600 and returns the file path. Errors if `$HOME` is unset.
pub fn store_token(data_dir: &Path, token: &str) -> io::Result<PathBuf> {
    let path = tokens_file_path()
        .ok_or_else(|| io::Error::other("$HOME is unset; cannot locate ~/.elide/tokens.toml"))?;
    let key = data_dir_key(data_dir)?;

    let mut store = load_store(&path)?;
    if let Some(entry) = store.coordinator.iter_mut().find(|c| c.data_dir == key) {
        entry.operator_token = token.to_owned();
    } else {
        store.coordinator.push(CoordinatorToken {
            data_dir: key,
            operator_token: token.to_owned(),
        });
    }

    let body = toml::to_string(&store).map_err(io::Error::other)?;

    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    let tmp = path.with_extension("toml.tmp");
    {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    Ok(path)
}

/// Diagnostic message for the "no token" case. Shared between CLI
/// verbs so the suggestion stays consistent.
pub fn missing_token_hint(op: OperatorOp) -> String {
    format!(
        "`elide volume {op_name}` requires an operator token.\n\
         Run `elide token create` (writes ~/.elide/tokens.toml for this\n  \
         coordinator), or pass --token <value>, or set {env}.",
        op_name = op.as_str(),
        env = TOKEN_ENV_VAR,
    )
}

/// Take a stored (coordinator-wide) operator token, append the
/// per-use caveats — `Op(op)`, `Volume(volume)`, short `NotAfter` —
/// and return the encoded wire token ready to hand to the
/// coordinator.
///
/// Attenuation is single-caveat-at-a-time per the macaroon API: each
/// call extends the chain from the previous tail MAC. The resulting
/// token verifies against the same root key as the stored one because
/// the chain replays correctly.
pub fn attenuate_for(stored: &str, op: OperatorOp, volume: &str) -> io::Result<String> {
    let m = Macaroon::parse(stored)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let wire = m
        .attenuate(Caveat::Op(op))
        .attenuate(Caveat::Volume(volume.to_owned()))
        .attenuate(Caveat::NotAfter(now + USE_EXPIRY_SECS));
    Ok(wire.encode())
}

#[cfg(test)]
mod tests {
    use super::*;
    use elide_coordinator::macaroon::{VerifyOperatorCtx, mint_operator, verify_operator};

    fn key() -> [u8; 32] {
        [42u8; 32]
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn attenuate_round_trips_and_verifies() {
        let root = mint_operator(&key(), now() + 86_400);
        let wire = attenuate_for(&root.encode(), OperatorOp::Remove, "myvm").unwrap();
        let ctx = VerifyOperatorCtx {
            now_unix: now(),
            op: OperatorOp::Remove,
            op_volume: "myvm",
        };
        let m = verify_operator(&key(), &wire, &ctx).unwrap();
        assert_eq!(m.op(), Some(OperatorOp::Remove));
        assert_eq!(m.volume(), Some("myvm"));
    }

    #[test]
    fn attenuate_scopes_to_named_volume_only() {
        let root = mint_operator(&key(), now() + 86_400);
        let wire = attenuate_for(&root.encode(), OperatorOp::Remove, "myvm").unwrap();
        let ctx = VerifyOperatorCtx {
            now_unix: now(),
            op: OperatorOp::Remove,
            op_volume: "othervm",
        };
        assert!(verify_operator(&key(), &wire, &ctx).is_err());
    }

    #[test]
    fn resolve_prefers_flag_over_env() {
        // SAFETY: tests in this crate are single-threaded by default
        // and we restore the env after.
        unsafe { env::set_var(TOKEN_ENV_VAR, "from-env") };
        let got = resolve(Some("from-flag"), Path::new(".")).unwrap();
        unsafe { env::remove_var(TOKEN_ENV_VAR) };
        assert_eq!(got.as_deref(), Some("from-flag"));
    }

    #[test]
    fn resolve_falls_back_to_env_when_flag_empty() {
        unsafe { env::set_var(TOKEN_ENV_VAR, "from-env") };
        let got = resolve(Some(""), Path::new(".")).unwrap();
        unsafe { env::remove_var(TOKEN_ENV_VAR) };
        assert_eq!(got.as_deref(), Some("from-env"));
    }

    #[test]
    fn store_then_resolve_round_trips_per_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let dd_a = tmp.path().join("coord-a");
        let dd_b = tmp.path().join("coord-b");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&dd_a).unwrap();
        fs::create_dir_all(&dd_b).unwrap();

        // SAFETY: single-threaded test; HOME restored at end.
        let prev_home = env::var_os("HOME");
        unsafe { env::set_var("HOME", &home) };

        store_token(&dd_a, "token-a").unwrap();
        store_token(&dd_b, "token-b").unwrap();
        // Re-mint for A: B must survive the upsert.
        store_token(&dd_a, "token-a2").unwrap();

        let got_a = resolve(None, &dd_a).unwrap();
        let got_b = resolve(None, &dd_b).unwrap();

        // Mode is 0600.
        let mode = fs::metadata(tokens_file_path().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;

        match prev_home {
            Some(h) => unsafe { env::set_var("HOME", h) },
            None => unsafe { env::remove_var("HOME") },
        }

        assert_eq!(got_a.as_deref(), Some("token-a2"));
        assert_eq!(got_b.as_deref(), Some("token-b"));
        assert_eq!(mode, 0o600);
    }
}
