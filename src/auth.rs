//! Peer authentication with a pre-shared key.
//!
//! Self-signed certificates give encryption but say nothing about *who* the
//! peer is, so TLS alone leaves both an authorization hole (anyone who can
//! reach the port becomes a peer) and an active-MITM hole. A shared secret
//! closes both, provided the proof is bound to the TLS channel.
//!
//! # Protocol
//!
//! After `Hello` (which carries a fresh 32-byte nonce from each side) both
//! peers send an `Auth` frame containing
//!
//! ```text
//! HMAC-SHA256(secret, role || nonce_initiator || nonce_responder || binding)
//! ```
//!
//! where `role` distinguishes the two directions — otherwise an attacker could
//! reflect our own MAC back at us — and `binding` is the SHA-256 of the
//! server's TLS certificate.
//!
//! The binding is what defeats a MITM: to sit in the middle it must present its
//! own certificate to the dialing peer while speaking to the real listener as a
//! client. The two ends then hash different certificates, the MACs disagree, and
//! both connections are dropped. Over plaintext there is no certificate to bind
//! to, so `--secret` with `--insecure` gets authorization only, and we say so.

use anyhow::{bail, Result};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const NONCE_LEN: usize = 32;
pub type Nonce = [u8; NONCE_LEN];

pub fn random_nonce() -> Nonce {
    rand::random()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// We dialed out.
    Initiator,
    /// We accepted the connection.
    Responder,
}

impl Role {
    fn tag(self) -> &'static [u8] {
        match self {
            Role::Initiator => b"p2psync-auth-v1-initiator",
            Role::Responder => b"p2psync-auth-v1-responder",
        }
    }

    pub fn peer(self) -> Role {
        match self {
            Role::Initiator => Role::Responder,
            Role::Responder => Role::Initiator,
        }
    }
}

/// Compute the proof a peer in `role` should send.
pub fn mac(
    secret: &[u8],
    role: Role,
    initiator_nonce: &Nonce,
    responder_nonce: &Nonce,
    binding: Option<&[u8; 32]>,
) -> Vec<u8> {
    let mut h = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    h.update(role.tag());
    h.update(initiator_nonce);
    h.update(responder_nonce);
    // Domain-separate "no binding" from a binding that happens to be zeros.
    match binding {
        Some(b) => {
            h.update(b"bound");
            h.update(b);
        }
        None => h.update(b"unbound"),
    }
    h.finalize().into_bytes().to_vec()
}

/// Verify the peer's proof. Comparison is constant-time.
pub fn verify(
    secret: &[u8],
    peer_role: Role,
    initiator_nonce: &Nonce,
    responder_nonce: &Nonce,
    binding: Option<&[u8; 32]>,
    presented: &[u8],
) -> Result<()> {
    let expect = mac(secret, peer_role, initiator_nonce, responder_nonce, binding);
    if expect.len() != presented.len() {
        bail!("peer failed authentication (malformed proof)");
    }
    let mut diff = 0u8;
    for (a, b) in expect.iter().zip(presented.iter()) {
        diff |= a ^ b;
    }
    if diff != 0 {
        bail!("peer failed authentication (wrong secret, or the channel is being intercepted)");
    }
    Ok(())
}

/// Resolve the shared secret from the flag or the environment.
pub fn resolve_secret(flag: Option<String>) -> Option<Vec<u8>> {
    flag.or_else(|| std::env::var("P2PSYNC_SECRET").ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_secrets_authenticate() {
        let (a, b) = (random_nonce(), random_nonce());
        let proof = mac(b"hunter2", Role::Initiator, &a, &b, None);
        verify(b"hunter2", Role::Initiator, &a, &b, None, &proof).unwrap();
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let (a, b) = (random_nonce(), random_nonce());
        let proof = mac(b"hunter2", Role::Initiator, &a, &b, None);
        assert!(verify(b"other", Role::Initiator, &a, &b, None, &proof).is_err());
    }

    #[test]
    fn reflecting_our_own_proof_is_rejected() {
        let (a, b) = (random_nonce(), random_nonce());
        // An attacker echoes the initiator's proof back as if it were the
        // responder's; the role tag makes it invalid.
        let ours = mac(b"s3cret", Role::Initiator, &a, &b, None);
        assert!(verify(b"s3cret", Role::Responder, &a, &b, None, &ours).is_err());
    }

    #[test]
    fn different_channel_binding_is_rejected() {
        let (a, b) = (random_nonce(), random_nonce());
        let real = [7u8; 32];
        let mitm = [9u8; 32];
        // The far end proves against the real certificate; we saw the MITM's.
        let proof = mac(b"s3cret", Role::Responder, &a, &b, Some(&real));
        assert!(verify(b"s3cret", Role::Responder, &a, &b, Some(&mitm), &proof).is_err());
        verify(b"s3cret", Role::Responder, &a, &b, Some(&real), &proof).unwrap();
    }

    #[test]
    fn nonces_are_not_reusable() {
        let (a, b) = (random_nonce(), random_nonce());
        let proof = mac(b"s3cret", Role::Initiator, &a, &b, None);
        let (a2, _) = (random_nonce(), random_nonce());
        assert!(verify(b"s3cret", Role::Initiator, &a2, &b, None, &proof).is_err());
    }
}
