// Typed macaroons for coordinator-issued credentials.
//
// A macaroon is a bearer token bound to a chain of typed caveats, MACed
// with the coordinator's root key. Verification is stateless: the
// coordinator re-derives the expected MAC from the root key + caveat
// chain — no token storage is needed.
//
// Wire format (a single hex line, fits the existing IPC line protocol):
//     v1.<32-byte mac, hex>.<caveats blob, hex>
//
// Caveats blob (binary, hex-encoded for transport):
//     u8: count
//     repeated:
//       u8 tag
//       Volume   (tag 0): u8 len, N UTF-8 bytes
//       Scope    (tag 1): u8 (0 = credentials)
//       Pid      (tag 2): i32 BE
//       NotAfter (tag 3): u64 BE  (unix seconds)
//
// The MAC is `blake3::keyed_hash(root_key, caveats_blob)`. blake3 in keyed
// mode is HMAC-equivalent for our purposes (per the blake3 spec).

use std::io;

const MAGIC: &str = "v1";
const TAG_VOLUME: u8 = 0;
const TAG_SCOPE: u8 = 1;
const TAG_PID: u8 = 2;
const TAG_NOT_AFTER: u8 = 3;

const SCOPE_CREDENTIALS: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Credentials,
}

impl Scope {
    fn to_byte(self) -> u8 {
        match self {
            Self::Credentials => SCOPE_CREDENTIALS,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            SCOPE_CREDENTIALS => Some(Self::Credentials),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caveat {
    Volume(String),
    Scope(Scope),
    Pid(i32),
    NotAfter(u64),
}

#[derive(Debug, Clone)]
pub struct Macaroon {
    caveats: Vec<Caveat>,
    mac: [u8; 32],
}

impl Macaroon {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn caveats(&self) -> &[Caveat] {
        &self.caveats
    }

    pub fn volume(&self) -> Option<&str> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Volume(v) => Some(v.as_str()),
            _ => None,
        })
    }

    pub fn scope(&self) -> Option<Scope> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Scope(s) => Some(*s),
            _ => None,
        })
    }

    pub fn pid(&self) -> Option<i32> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::Pid(p) => Some(*p),
            _ => None,
        })
    }

    pub fn not_after(&self) -> Option<u64> {
        self.caveats.iter().find_map(|c| match c {
            Caveat::NotAfter(t) => Some(*t),
            _ => None,
        })
    }

    pub fn encode(&self) -> String {
        let blob = serialize_caveats(&self.caveats);
        format!("{MAGIC}.{}.{}", encode_hex(&self.mac), encode_hex(&blob))
    }

    pub fn parse(s: &str) -> io::Result<Self> {
        let mut parts = s.splitn(3, '.');
        let magic = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        if magic != MAGIC {
            return Err(io::Error::other(format!(
                "unsupported macaroon version: {magic}"
            )));
        }
        let mac_hex = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        let cav_hex = parts
            .next()
            .ok_or_else(|| io::Error::other("malformed macaroon"))?;
        let mac = decode_hex_fixed::<32>(mac_hex)?;
        let blob = decode_hex(cav_hex)?;
        let caveats = deserialize_caveats(&blob)?;
        Ok(Self { caveats, mac })
    }
}

/// Mint a macaroon by MACing the serialized caveat chain with `root_key`.
pub fn mint(root_key: &[u8; 32], caveats: Vec<Caveat>) -> Macaroon {
    let blob = serialize_caveats(&caveats);
    let mac = blake3::keyed_hash(root_key, &blob);
    Macaroon {
        caveats,
        mac: *mac.as_bytes(),
    }
}

/// Constant-time MAC verification. The caller is still responsible for
/// checking individual caveat values against runtime context (volume,
/// scope, pid, expiry).
pub fn verify(root_key: &[u8; 32], m: &Macaroon) -> bool {
    let blob = serialize_caveats(&m.caveats);
    let expected = blake3::keyed_hash(root_key, &blob);
    constant_time_eq(expected.as_bytes(), &m.mac)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

fn serialize_caveats(caveats: &[Caveat]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    out.push(caveats.len() as u8);
    for c in caveats {
        match c {
            Caveat::Volume(v) => {
                out.push(TAG_VOLUME);
                let bytes = v.as_bytes();
                debug_assert!(bytes.len() <= u8::MAX as usize);
                out.push(bytes.len() as u8);
                out.extend_from_slice(bytes);
            }
            Caveat::Scope(s) => {
                out.push(TAG_SCOPE);
                out.push(s.to_byte());
            }
            Caveat::Pid(p) => {
                out.push(TAG_PID);
                out.extend_from_slice(&p.to_be_bytes());
            }
            Caveat::NotAfter(t) => {
                out.push(TAG_NOT_AFTER);
                out.extend_from_slice(&t.to_be_bytes());
            }
        }
    }
    out
}

fn deserialize_caveats(blob: &[u8]) -> io::Result<Vec<Caveat>> {
    let mut cur = blob;
    let count = read_u8(&mut cur)?;
    let mut caveats = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tag = read_u8(&mut cur)?;
        let c = match tag {
            TAG_VOLUME => {
                let len = read_u8(&mut cur)? as usize;
                let bytes = read_n(&mut cur, len)?;
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| io::Error::other("non-utf8 volume caveat"))?;
                Caveat::Volume(s.to_owned())
            }
            TAG_SCOPE => {
                let b = read_u8(&mut cur)?;
                Caveat::Scope(
                    Scope::from_byte(b)
                        .ok_or_else(|| io::Error::other(format!("unknown scope: {b}")))?,
                )
            }
            TAG_PID => {
                let bytes = read_n(&mut cur, 4)?;
                let mut a = [0u8; 4];
                a.copy_from_slice(bytes);
                Caveat::Pid(i32::from_be_bytes(a))
            }
            TAG_NOT_AFTER => {
                let bytes = read_n(&mut cur, 8)?;
                let mut a = [0u8; 8];
                a.copy_from_slice(bytes);
                Caveat::NotAfter(u64::from_be_bytes(a))
            }
            _ => return Err(io::Error::other(format!("unknown caveat tag: {tag}"))),
        };
        caveats.push(c);
    }
    if !cur.is_empty() {
        return Err(io::Error::other("trailing bytes in caveat blob"));
    }
    Ok(caveats)
}

fn read_u8(cur: &mut &[u8]) -> io::Result<u8> {
    if cur.is_empty() {
        return Err(io::Error::other("unexpected eof in caveat blob"));
    }
    let b = cur[0];
    *cur = &cur[1..];
    Ok(b)
}

fn read_n<'a>(cur: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if cur.len() < n {
        return Err(io::Error::other("unexpected eof in caveat blob"));
    }
    let r = &cur[..n];
    *cur = &cur[n..];
    Ok(r)
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn decode_hex(s: &str) -> io::Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(io::Error::other("hex string has odd length"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| io::Error::other(format!("invalid hex at position {i}")))
        })
        .collect()
}

fn decode_hex_fixed<const N: usize>(s: &str) -> io::Result<[u8; N]> {
    let v = decode_hex(s)?;
    v.try_into()
        .map_err(|v: Vec<u8>| io::Error::other(format!("expected {N} bytes, got {}", v.len())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn sample_caveats() -> Vec<Caveat> {
        vec![
            Caveat::Volume("01JQAAAAAAAAAAAAAAAAAAAAAA".to_owned()),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(12345),
        ]
    }

    #[test]
    fn mint_then_verify_roundtrip() {
        let m = mint(&key(), sample_caveats());
        assert!(verify(&key(), &m));
    }

    #[test]
    fn encode_then_parse_roundtrip() {
        let m = mint(&key(), sample_caveats());
        let s = m.encode();
        let parsed = Macaroon::parse(&s).unwrap();
        assert_eq!(parsed.caveats, m.caveats);
        assert_eq!(parsed.mac, m.mac);
        assert!(verify(&key(), &parsed));
    }

    #[test]
    fn accessors_extract_caveat_values() {
        let m = mint(&key(), sample_caveats());
        assert_eq!(m.volume(), Some("01JQAAAAAAAAAAAAAAAAAAAAAA"));
        assert_eq!(m.scope(), Some(Scope::Credentials));
        assert_eq!(m.pid(), Some(12345));
        assert_eq!(m.not_after(), None);
    }

    #[test]
    fn tampered_mac_fails_verify() {
        let m = mint(&key(), sample_caveats());
        let mut s = m.encode();
        // Flip a byte in the MAC region: format is `v1.<mac>.<blob>`,
        // so the first dot is at index 2.
        let dot = s.find('.').unwrap();
        let pos = dot + 2;
        let bytes = unsafe { s.as_bytes_mut() };
        bytes[pos] = if bytes[pos] == b'a' { b'b' } else { b'a' };
        let parsed = Macaroon::parse(&s).unwrap();
        assert!(!verify(&key(), &parsed));
    }

    #[test]
    fn tampered_caveat_fails_verify() {
        let m = mint(&key(), sample_caveats());
        let mut new_caveats = m.caveats().to_vec();
        // Mutate the pid — verify must reject because the MAC was over the
        // original caveat chain.
        for c in &mut new_caveats {
            if let Caveat::Pid(p) = c {
                *p = 99999;
            }
        }
        let forged = Macaroon {
            caveats: new_caveats,
            mac: m.mac,
        };
        assert!(!verify(&key(), &forged));
    }

    #[test]
    fn wrong_root_key_fails_verify() {
        let m = mint(&key(), sample_caveats());
        let mut other = key();
        other[0] ^= 0xFF;
        assert!(!verify(&other, &m));
    }

    #[test]
    fn parse_rejects_unknown_version() {
        let err = Macaroon::parse("v9.0011.00").unwrap_err();
        assert!(err.to_string().contains("version"));
    }

    #[test]
    fn parse_rejects_truncated_blob() {
        // Valid hex MAC, blob claims one Volume caveat but no length byte.
        let mac = "00".repeat(32);
        let blob_hex = encode_hex(&[1u8, TAG_VOLUME]);
        let s = format!("v1.{mac}.{blob_hex}");
        assert!(Macaroon::parse(&s).is_err());
    }

    #[test]
    fn not_after_caveat_roundtrips() {
        let caveats = vec![
            Caveat::Volume("vol".to_owned()),
            Caveat::Scope(Scope::Credentials),
            Caveat::Pid(1),
            Caveat::NotAfter(1_700_000_000),
        ];
        let m = mint(&key(), caveats);
        let s = m.encode();
        let parsed = Macaroon::parse(&s).unwrap();
        assert_eq!(parsed.not_after(), Some(1_700_000_000));
        assert!(verify(&key(), &parsed));
    }
}
