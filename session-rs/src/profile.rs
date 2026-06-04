//! SessionProfile op — capability assertion at stream open.
//!
//! Opcode 0x0100 (first standard extension). Appears after `IsaStreamHeader`
//! and before any data ops. Declares the trust level, authority claim, issuer,
//! generation, and optional expiry for the stream's capability assertion.
//!
//! ## Trust levels
//!
//! | Level    | Mechanism                     | When to use                            |
//! |----------|-------------------------------|----------------------------------------|
//! | Advisory | No signature                  | Single-tenant / internal               |
//! | Asserted | Signed by issuer_domain key   | Cross-tenant federation                |
//! | Attested | Signed + stream MAC'd         | Hostile transit / compliance audit     |
//!
//! ## Wire formats
//!
//! `profile_version = 1` — legacy format (single `capability_mask: u64`):
//! ```text
//! profile_id:      u32
//! profile_version: u16  (= 1)
//! trust_level:     u8
//! capability_mask: u64  → mapped to all four AuthorityPolicy axes
//! issuer_domain:   u64
//! session_id:      u64
//! generation:      u64
//! expiry_epoch:    u64
//! sig_len:         u16
//! signature:       u8[sig_len]
//! ```
//!
//! `profile_version = 2` — four-axis format (extends the payload by 24 bytes):
//! ```text
//! profile_id:         u32
//! profile_version:    u16  (= 2)
//! trust_level:        u8
//! assertion_mask:     u64
//! delegation_mask:    u64
//! observability_mask: u64
//! disclosure_mask:    u64
//! issuer_domain:      u64
//! session_id:         u64
//! generation:         u64
//! expiry_epoch:       u64
//! sig_len:            u16
//! signature:          u8[sig_len]
//! ```
//!
//! ## Asserted signature
//!
//! Ed25519 over the 64-byte canonical payload (little-endian):
//! ```text
//! bytes  0– 7   assertion_mask
//! bytes  8–15   delegation_mask
//! bytes 16–23   observability_mask
//! bytes 24–31   disclosure_mask
//! bytes 32–39   issuer_domain
//! bytes 40–47   session_id
//! bytes 48–55   generation
//! bytes 56–63   expiry_epoch
//! ```
//!
//! The `session_id` binding prevents splice attacks: a valid profile from
//! session A cannot be replayed into session B.
//!
//! ## Revocation via generation
//!
//! `generation` is a monotone counter. The issuer maintains a `min_valid_generation`
//! floor per domain. Any profile with `generation < floor` is revoked.
//! Expiry uses `causal_epoch` (not wall-clock) for distributed correctness.

use ed25519_dalek::{Signature, VerifyingKey};
use rustc_hash::FxHashMap;
use crate::{TenantId, SessionId, auth::AuthorityPolicy};

// ── TrustStore ────────────────────────────────────────────────────────────────

/// Issuer trust store for `Asserted` / `Attested` profile verification.
///
/// Maps `TenantId` → Ed25519 `VerifyingKey`. The runtime holds one `TrustStore`;
/// the host pre-registers keys for tenants that may issue `Asserted` profiles.
/// An issuer absent from the store causes `ProfileError::UnknownIssuer`.
pub type TrustStore = FxHashMap<TenantId, VerifyingKey>;

pub type ProfileId = u32;

// ── TrustLevel ────────────────────────────────────────────────────────────────

/// Trust level for a `SessionProfile` capability assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TrustLevel {
    /// No cryptographic binding. Effective capability clipped by parent ceiling.
    Advisory = 0x01,
    /// Signed by `issuer_domain` key. Required for cross-tenant federation.
    Asserted  = 0x02,
    /// Signed + per-record stream MAC under derived session key.
    Attested  = 0x03,
}

impl TrustLevel {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Advisory),
            0x02 => Some(Self::Asserted),
            0x03 => Some(Self::Attested),
            _    => None,
        }
    }

    pub fn as_u8(self) -> u8 { self as u8 }
}

// ── SessionProfile ────────────────────────────────────────────────────────────

/// Capability profile carried in the stream at opcode 0x0100.
///
/// Parsed from `profile_version = 1` (legacy) or `profile_version = 2`
/// (four-axis). Both are represented with four separate authority masks;
/// v1 maps the single `capability_mask` to all four axes.
#[derive(Clone, Debug)]
pub struct SessionProfile {
    pub profile_id:         ProfileId,
    pub profile_version:    u16,
    pub trust_level:        TrustLevel,
    /// Assertion bits claimed by this profile.
    pub assertion_mask:     u64,
    /// Delegation bits claimed by this profile.
    pub delegation_mask:    u64,
    /// Observability bits claimed by this profile.
    pub observability_mask: u64,
    /// Disclosure bits claimed by this profile.
    pub disclosure_mask:    u64,
    pub issuer_domain:      TenantId,
    /// Session this profile was issued for. Must match the stream's session_id.
    pub session_id:         SessionId,
    /// Monotone revocation counter. Revoked if `generation < min_valid_generation`.
    pub generation:         u64,
    /// `causal_epoch` at which this profile expires. `u64::MAX` = never expires.
    pub expiry_epoch:       u64,
    /// Empty for Advisory; Ed25519 bytes (64) for Asserted/Attested.
    pub signature:          Vec<u8>,
}

impl SessionProfile {
    /// The claimed `AuthorityPolicy` from this profile's four masks.
    pub fn claimed_policy(&self) -> AuthorityPolicy {
        AuthorityPolicy {
            assertion:     self.assertion_mask,
            delegation:    self.delegation_mask,
            observability: self.observability_mask,
            disclosure:    self.disclosure_mask,
        }
    }

    /// 64-byte canonical signing payload for Asserted/Attested verification.
    ///
    /// All fields little-endian; the session_id binding prevents splice attacks.
    pub fn canonical_signing_payload(&self) -> [u8; 64] {
        let mut buf = [0u8; 64];
        buf[ 0.. 8].copy_from_slice(&self.assertion_mask.to_le_bytes());
        buf[ 8..16].copy_from_slice(&self.delegation_mask.to_le_bytes());
        buf[16..24].copy_from_slice(&self.observability_mask.to_le_bytes());
        buf[24..32].copy_from_slice(&self.disclosure_mask.to_le_bytes());
        buf[32..40].copy_from_slice(&self.issuer_domain.0.to_le_bytes());
        buf[40..48].copy_from_slice(&self.session_id.0.to_le_bytes());
        buf[48..56].copy_from_slice(&self.generation.to_le_bytes());
        buf[56..64].copy_from_slice(&self.expiry_epoch.to_le_bytes());
        buf
    }

    // ── Shared validation ─────────────────────────────────────────────────────

    fn check_session_binding(&self, actual_session: SessionId) -> Result<(), ProfileError> {
        if self.session_id != actual_session {
            return Err(ProfileError::SessionMismatch {
                profile_session: self.session_id,
                actual_session,
            });
        }
        Ok(())
    }

    fn check_expiry(&self, current_epoch: u64) -> Result<(), ProfileError> {
        if self.expiry_epoch != u64::MAX && current_epoch > self.expiry_epoch {
            return Err(ProfileError::Expired {
                expiry:  self.expiry_epoch,
                current: current_epoch,
            });
        }
        Ok(())
    }

    fn check_generation(&self, min_generation: u64) -> Result<(), ProfileError> {
        if self.generation < min_generation {
            return Err(ProfileError::GenerationRevoked {
                profile:   self.generation,
                min_valid: min_generation,
            });
        }
        Ok(())
    }

    // ── Advisory ─────────────────────────────────────────────────────────────

    /// Verify an Advisory profile.
    ///
    /// Checks session binding, expiry, and generation. Returns the effective
    /// `AuthorityPolicy`: `claimed.intersect(parent_policy)`.
    pub fn verify_advisory(
        &self,
        parent_policy:  AuthorityPolicy,
        current_epoch:  u64,
        min_generation: u64,
        actual_session: SessionId,
    ) -> Result<AuthorityPolicy, ProfileError> {
        if self.trust_level != TrustLevel::Advisory {
            return Err(ProfileError::WrongTrustLevel);
        }
        self.check_session_binding(actual_session)?;
        self.check_expiry(current_epoch)?;
        self.check_generation(min_generation)?;
        Ok(self.claimed_policy().intersect(parent_policy))
    }

    // ── Asserted ──────────────────────────────────────────────────────────────

    /// Verify an Asserted (or Attested) profile.
    ///
    /// Performs all Advisory checks plus Ed25519 signature verification
    /// against the issuer's key in `trust_store`. The signature covers the
    /// 64-byte canonical payload binding all four authority masks plus
    /// `issuer_domain`, `session_id`, `generation`, and `expiry_epoch`.
    ///
    /// Returns the effective `AuthorityPolicy`: `claimed.intersect(parent_policy)`.
    pub fn verify_asserted(
        &self,
        parent_policy:  AuthorityPolicy,
        current_epoch:  u64,
        min_generation: u64,
        actual_session: SessionId,
        trust_store:    &TrustStore,
    ) -> Result<AuthorityPolicy, ProfileError> {
        if !matches!(self.trust_level, TrustLevel::Asserted | TrustLevel::Attested) {
            return Err(ProfileError::WrongTrustLevel);
        }
        self.check_session_binding(actual_session)?;
        self.check_expiry(current_epoch)?;
        self.check_generation(min_generation)?;

        // Look up the issuer's verifying key
        let verifying_key = trust_store
            .get(&self.issuer_domain)
            .ok_or(ProfileError::UnknownIssuer { issuer: self.issuer_domain })?;

        // Decode the 64-byte Ed25519 signature
        let sig_bytes: [u8; 64] = self.signature.as_slice().try_into()
            .map_err(|_| ProfileError::SignatureInvalid)?;
        let sig = Signature::from_bytes(&sig_bytes);

        // Verify against the canonical payload
        let payload = self.canonical_signing_payload();
        verifying_key.verify_strict(&payload, &sig)
            .map_err(|_| ProfileError::SignatureInvalid)?;

        Ok(self.claimed_policy().intersect(parent_policy))
    }
}

// ── ProfileError ──────────────────────────────────────────────────────────────

/// Error from profile verification.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("unknown trust level 0x{0:02x}")]
    UnknownTrustLevel(u8),
    #[error("wrong trust level for this verification path")]
    WrongTrustLevel,
    #[error("profile expired: expiry_epoch={expiry} < current_epoch={current}")]
    Expired { expiry: u64, current: u64 },
    #[error("generation revoked: profile_generation={profile} min_valid={min_valid}")]
    GenerationRevoked { profile: u64, min_valid: u64 },
    #[error("session mismatch: profile bound to {profile_session:?}, presented on {actual_session:?}")]
    SessionMismatch { profile_session: SessionId, actual_session: SessionId },
    #[error("unknown issuer domain {issuer:?} in trust store")]
    UnknownIssuer { issuer: TenantId },
    #[error("signature verification failed")]
    SignatureInvalid,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn advisory(
        assertion: u64, delegation: u64, observability: u64, disclosure: u64,
        generation: u64, expiry: u64, session_id: u64,
    ) -> SessionProfile {
        SessionProfile {
            profile_id:         1,
            profile_version:    2,
            trust_level:        TrustLevel::Advisory,
            assertion_mask:     assertion,
            delegation_mask:    delegation,
            observability_mask: observability,
            disclosure_mask:    disclosure,
            issuer_domain:      TenantId(1),
            session_id:         SessionId(session_id),
            generation,
            expiry_epoch:       expiry,
            signature:          vec![],
        }
    }

    fn all_advisory(session_id: u64) -> SessionProfile {
        advisory(u64::MAX, u64::MAX, u64::MAX, u64::MAX, 0, u64::MAX, session_id)
    }

    #[test]
    fn advisory_clips_to_parent_ceiling() {
        let profile = all_advisory(42);
        let parent  = AuthorityPolicy {
            assertion: 0xFF, delegation: 0x00, observability: 0xFF, disclosure: 0x00
        };
        let eff = profile.verify_advisory(parent, 0, 0, SessionId(42)).unwrap();
        assert_eq!(eff, parent);
    }

    #[test]
    fn advisory_session_id_must_match() {
        let profile = all_advisory(42);
        let err = profile.verify_advisory(AuthorityPolicy::ALL, 0, 0, SessionId(99)).unwrap_err();
        assert!(matches!(err, ProfileError::SessionMismatch { .. }));
    }

    #[test]
    fn advisory_expired_profile_rejected() {
        let profile = advisory(0xFF, 0, 0xFF, 0, 0, 100, 1); // expires at epoch 100
        let err = profile.verify_advisory(AuthorityPolicy::ALL, 101, 0, SessionId(1)).unwrap_err();
        assert!(matches!(err, ProfileError::Expired { .. }));
    }

    #[test]
    fn advisory_not_yet_expired() {
        let profile = advisory(0xFF, 0, 0xFF, 0, 0, 100, 1);
        assert!(profile.verify_advisory(AuthorityPolicy::ALL, 99, 0, SessionId(1)).is_ok());
    }

    #[test]
    fn advisory_never_expires_when_max() {
        let profile = all_advisory(5);
        assert!(profile.verify_advisory(AuthorityPolicy::ALL, u64::MAX - 1, 0, SessionId(5)).is_ok());
    }

    #[test]
    fn advisory_revoked_generation() {
        let profile = advisory(0xFF, 0, 0xFF, 0, 3, u64::MAX, 7); // generation=3
        let err = profile.verify_advisory(AuthorityPolicy::ALL, 0, 5, SessionId(7)).unwrap_err();
        assert!(matches!(err, ProfileError::GenerationRevoked { .. }));
    }

    #[test]
    fn advisory_valid_generation() {
        let profile = advisory(0xFF, 0, 0xFF, 0, 5, u64::MAX, 7); // generation=5
        assert!(profile.verify_advisory(AuthorityPolicy::ALL, 0, 5, SessionId(7)).is_ok());
    }

    #[test]
    fn advisory_wrong_trust_level_rejected() {
        let mut profile = all_advisory(1);
        profile.trust_level = TrustLevel::Asserted;
        let err = profile.verify_advisory(AuthorityPolicy::ALL, 0, 0, SessionId(1)).unwrap_err();
        assert!(matches!(err, ProfileError::WrongTrustLevel));
    }

    // ── Asserted ──────────────────────────────────────────────────────────────

    fn make_signing_key(seed: u64) -> SigningKey {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&seed.to_le_bytes());
        SigningKey::from_bytes(&bytes)
    }

    fn sign_profile(profile: &mut SessionProfile, signing_key: &SigningKey) {
        use ed25519_dalek::Signer;
        let payload = profile.canonical_signing_payload();
        let sig: Signature = signing_key.sign(&payload);
        profile.signature = sig.to_bytes().to_vec();
        profile.trust_level = TrustLevel::Asserted;
    }

    fn make_trust_store(tenant_id: TenantId, signing_key: &SigningKey) -> TrustStore {
        let mut store = TrustStore::default();
        store.insert(tenant_id, signing_key.verifying_key());
        store
    }

    #[test]
    fn asserted_valid_signature_accepted() {
        let signing_key = make_signing_key(0xDEAD_BEEF);
        let mut profile = all_advisory(10);
        profile.issuer_domain = TenantId(99);
        sign_profile(&mut profile, &signing_key);

        let trust_store = make_trust_store(TenantId(99), &signing_key);
        let eff = profile.verify_asserted(
            AuthorityPolicy::ALL, 0, 0, SessionId(10), &trust_store
        ).unwrap();
        assert_eq!(eff, AuthorityPolicy::ALL);
    }

    #[test]
    fn asserted_clips_to_parent_ceiling() {
        let signing_key = make_signing_key(0x1234);
        let mut profile = all_advisory(10);
        profile.issuer_domain = TenantId(5);
        sign_profile(&mut profile, &signing_key);

        let parent = AuthorityPolicy { assertion: 0x0F, delegation: 0, observability: 0x0F, disclosure: 0 };
        let trust_store = make_trust_store(TenantId(5), &signing_key);
        let eff = profile.verify_asserted(parent, 0, 0, SessionId(10), &trust_store).unwrap();
        assert_eq!(eff, parent);
    }

    #[test]
    fn asserted_unknown_issuer_rejected() {
        let signing_key = make_signing_key(0xABCD);
        let mut profile = all_advisory(10);
        profile.issuer_domain = TenantId(99);
        sign_profile(&mut profile, &signing_key);

        // Trust store does not contain TenantId(99)
        let trust_store = make_trust_store(TenantId(1), &signing_key);
        let err = profile.verify_asserted(
            AuthorityPolicy::ALL, 0, 0, SessionId(10), &trust_store
        ).unwrap_err();
        assert!(matches!(err, ProfileError::UnknownIssuer { issuer: TenantId(99) }));
    }

    #[test]
    fn asserted_tampered_signature_rejected() {
        let signing_key = make_signing_key(0x5678);
        let mut profile = all_advisory(10);
        profile.issuer_domain = TenantId(7);
        sign_profile(&mut profile, &signing_key);

        // Tamper with one assertion bit
        profile.assertion_mask = 0;

        let trust_store = make_trust_store(TenantId(7), &signing_key);
        let err = profile.verify_asserted(
            AuthorityPolicy::ALL, 0, 0, SessionId(10), &trust_store
        ).unwrap_err();
        assert!(matches!(err, ProfileError::SignatureInvalid));
    }

    #[test]
    fn asserted_session_mismatch_rejected() {
        let signing_key = make_signing_key(0x9ABC);
        let mut profile = all_advisory(10);
        profile.issuer_domain = TenantId(3);
        sign_profile(&mut profile, &signing_key);

        let trust_store = make_trust_store(TenantId(3), &signing_key);
        // Present on wrong session
        let err = profile.verify_asserted(
            AuthorityPolicy::ALL, 0, 0, SessionId(99), &trust_store
        ).unwrap_err();
        assert!(matches!(err, ProfileError::SessionMismatch { .. }));
    }

    #[test]
    fn asserted_wrong_trust_level_rejected() {
        let trust_store = TrustStore::default();
        let profile = all_advisory(1); // trust_level = Advisory
        let err = profile.verify_asserted(
            AuthorityPolicy::ALL, 0, 0, SessionId(1), &trust_store
        ).unwrap_err();
        assert!(matches!(err, ProfileError::WrongTrustLevel));
    }

    #[test]
    fn canonical_signing_payload_is_64_bytes() {
        let profile = all_advisory(1);
        let payload = profile.canonical_signing_payload();
        assert_eq!(payload.len(), 64);
    }

    #[test]
    fn canonical_payload_encodes_fields() {
        let profile = SessionProfile {
            profile_id:         1,
            profile_version:    2,
            trust_level:        TrustLevel::Asserted,
            assertion_mask:     0x0102030405060708,
            delegation_mask:    0x090a0b0c0d0e0f10,
            observability_mask: 0x1112131415161718,
            disclosure_mask:    0x191a1b1c1d1e1f20,
            issuer_domain:      TenantId(0x2122232425262728),
            session_id:         SessionId(0x292a2b2c2d2e2f30),
            generation:         0x3132333435363738,
            expiry_epoch:       0x393a3b3c3d3e3f40,
            signature:          vec![],
        };
        let p = profile.canonical_signing_payload();
        assert_eq!(&p[ 0.. 8], &0x0102030405060708u64.to_le_bytes());
        assert_eq!(&p[ 8..16], &0x090a0b0c0d0e0f10u64.to_le_bytes());
        assert_eq!(&p[16..24], &0x1112131415161718u64.to_le_bytes());
        assert_eq!(&p[24..32], &0x191a1b1c1d1e1f20u64.to_le_bytes());
        assert_eq!(&p[32..40], &0x2122232425262728u64.to_le_bytes());
        assert_eq!(&p[40..48], &0x292a2b2c2d2e2f30u64.to_le_bytes());
        assert_eq!(&p[48..56], &0x3132333435363738u64.to_le_bytes());
        assert_eq!(&p[56..64], &0x393a3b3c3d3e3f40u64.to_le_bytes());
    }
}
