//! SessionProfile op — capability assertion at stream open.
//!
//! Opcode 0x0100 (first standard extension). Appears after `IsaStreamHeader`
//! and before any data ops. Declares the trust level, capability claim, issuer,
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
//! ## Capability inheritance always applies
//!
//! Regardless of trust level, effective_caps = min(claimed, parent.capabilities).
//! A forged ADVISORY profile claiming TOP is clipped to whatever the tenant holds.
//! ASSERTED/ATTESTED profiles additionally require cryptographic verification.
//!
//! ## Forgery protection
//!
//! The ASSERTED signature must bind `(capability_mask | issuer_domain | generation
//! | expiry_epoch | session_id)`. The `session_id` binding prevents splice attacks
//! (a valid profile from session A cannot be replayed into session B).
//!
//! ## Revocation via generation
//!
//! `generation` is a monotone counter. The issuer maintains a `min_valid_generation`
//! floor per domain. Any profile with `generation < floor` is revoked.
//! Expiry uses `causal_epoch` (not wall-clock) for distributed correctness.
//!
//! ## v1 trust level support
//!
//! **ADVISORY** is the production-ready trust level for v1. Verification is
//! stateless: expiry, generation, and session-id binding are checked; no
//! signature is required; effective capability is `min(claimed, parent.capabilities)`.
//!
//! **ASSERTED** requires an issuer trust store wired into `SessionRuntime`
//! (`HashMap<TenantId, VerifyingKey>`) and Ed25519 verification over the
//! canonical payload `capability_mask ‖ issuer_domain ‖ session_id ‖
//! generation ‖ expiry_epoch`. `verify_asserted` returns
//! `ProfileError::Unimplemented` until this is wired. To complete: add
//! `ed25519-dalek` or `ring`, add `trust_store` to `SessionRuntime`, call
//! `VerifyingKey::verify()` in `verify_asserted`.
//!
//! **ATTESTED** additionally requires per-record stream MAC under a
//! session-derived key established during bootstrap. Post-v1.

use pgress_core::partition::CapabilityBits;
use crate::{SessionId, TenantId};

pub type ProfileId = u32;

/// Trust level for a `SessionProfile` capability assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TrustLevel {
    /// No cryptographic binding. Effective capability clipped by parent ceiling.
    Advisory = 0x01,
    /// Signed by `issuer_domain` key. Required for cross-tenant federation.
    /// Verification is a stub until key infrastructure is wired (returns `Unimplemented`).
    Asserted = 0x02,
    /// Signed + per-record stream MAC under derived session key.
    Attested = 0x03,
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

/// Capability profile carried in the stream at opcode 0x0100.
///
/// Wire payload layout:
/// ```text
/// profile_id:      u32
/// profile_version: u16
/// trust_level:     u8
/// capability_mask: u64
/// issuer_domain:   u64  (TenantId wire form)
/// session_id:      u64  (binds signature to this session — splice protection)
/// generation:      u64
/// expiry_epoch:    u64  (causal_epoch at expiry; u64::MAX = never)
/// sig_len:         u16
/// signature:       u8[sig_len]
/// ```
#[derive(Clone, Debug)]
pub struct SessionProfile {
    pub profile_id:      ProfileId,
    pub profile_version: u16,
    pub trust_level:     TrustLevel,
    pub capability_mask: CapabilityBits,
    pub issuer_domain:   TenantId,
    /// Session this profile was issued for. Must match the stream's session_id.
    pub session_id:      SessionId,
    /// Monotone revocation counter. Revoked if `generation < min_valid_generation`.
    pub generation:      u64,
    /// `causal_epoch` at which this profile expires. `u64::MAX` = never expires.
    pub expiry_epoch:    u64,
    /// Empty for Advisory; Ed25519/HMAC bytes for Asserted/Attested.
    pub signature:       Vec<u8>,
}

/// Error from profile verification.
#[derive(Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("unknown trust level 0x{0:02x}")]
    UnknownTrustLevel(u8),
    #[error("profile expired: expiry_epoch={expiry} < current_epoch={current}")]
    Expired { expiry: u64, current: u64 },
    #[error("generation revoked: profile_generation={profile} min_valid={min_valid}")]
    GenerationRevoked { profile: u64, min_valid: u64 },
    #[error("session mismatch: profile bound to {profile_session:?}, presented on {actual_session:?}")]
    SessionMismatch { profile_session: SessionId, actual_session: SessionId },
    #[error("ASSERTED/ATTESTED verification not yet implemented")]
    Unimplemented,
    #[error("signature verification failed")]
    SignatureInvalid,
}

impl SessionProfile {
    /// Verify an Advisory profile.
    ///
    /// Checks expiry, generation, and session binding.
    /// Returns the effective capability: `min(claimed, parent_caps)`.
    pub fn verify_advisory(
        &self,
        parent_caps:     CapabilityBits,
        current_epoch:   u64,
        min_generation:  u64,
        actual_session:  SessionId,
    ) -> Result<CapabilityBits, ProfileError> {
        // Trust level check
        if self.trust_level != TrustLevel::Advisory {
            return Err(ProfileError::Unimplemented);
        }

        // Session binding (splice protection)
        if self.session_id != actual_session {
            return Err(ProfileError::SessionMismatch {
                profile_session: self.session_id,
                actual_session,
            });
        }

        // Expiry check (causal_epoch based)
        if self.expiry_epoch != u64::MAX && current_epoch > self.expiry_epoch {
            return Err(ProfileError::Expired {
                expiry:  self.expiry_epoch,
                current: current_epoch,
            });
        }

        // Revocation check
        if self.generation < min_generation {
            return Err(ProfileError::GenerationRevoked {
                profile:   self.generation,
                min_valid: min_generation,
            });
        }

        // Capability inheritance: child cannot exceed parent ceiling
        Ok(CapabilityBits(self.capability_mask.0 & parent_caps.0))
    }

    /// Verify an Asserted or Attested profile.
    ///
    /// Currently returns `Err(Unimplemented)`. Production implementation
    /// will verify the Ed25519/HMAC signature over the canonical payload
    /// `(capability_mask | issuer_domain | session_id | generation | expiry_epoch)`.
    pub fn verify_asserted(
        &self,
        _parent_caps:    CapabilityBits,
        _current_epoch:  u64,
        _min_generation: u64,
        _actual_session: SessionId,
    ) -> Result<CapabilityBits, ProfileError> {
        Err(ProfileError::Unimplemented)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn advisory(caps: CapabilityBits, generation: u64, expiry: u64, session_id: u64) -> SessionProfile {
        SessionProfile {
            profile_id:      1,
            profile_version: 1,
            trust_level:     TrustLevel::Advisory,
            capability_mask: caps,
            issuer_domain:   TenantId(1),
            session_id:      SessionId(session_id),
            generation,
            expiry_epoch:    expiry,
            signature:       vec![],
        }
    }

    #[test]
    fn advisory_clips_to_parent_ceiling() {
        let profile = advisory(CapabilityBits::ALL, 0, u64::MAX, 42);
        let parent  = CapabilityBits::READ | CapabilityBits::PROPAGATE;
        let eff     = profile.verify_advisory(parent, 0, 0, SessionId(42)).unwrap();
        assert_eq!(eff, parent);
    }

    #[test]
    fn advisory_session_id_must_match() {
        let profile = advisory(CapabilityBits::READ, 0, u64::MAX, 42);
        let err = profile.verify_advisory(CapabilityBits::ALL, 0, 0, SessionId(99)).unwrap_err();
        assert!(matches!(err, ProfileError::SessionMismatch { .. }));
    }

    #[test]
    fn advisory_expired_profile_rejected() {
        let profile = advisory(CapabilityBits::READ, 0, 100, 1);  // expires at epoch 100
        let err = profile.verify_advisory(CapabilityBits::ALL, 101, 0, SessionId(1)).unwrap_err();
        assert!(matches!(err, ProfileError::Expired { .. }));
    }

    #[test]
    fn advisory_not_yet_expired() {
        let profile = advisory(CapabilityBits::READ, 0, 100, 1);
        assert!(profile.verify_advisory(CapabilityBits::ALL, 99, 0, SessionId(1)).is_ok());
    }

    #[test]
    fn advisory_never_expires_when_max() {
        let profile = advisory(CapabilityBits::READ, 0, u64::MAX, 5);
        assert!(profile.verify_advisory(CapabilityBits::ALL, u64::MAX - 1, 0, SessionId(5)).is_ok());
    }

    #[test]
    fn advisory_revoked_generation() {
        let profile = advisory(CapabilityBits::READ, 3, u64::MAX, 7);  // generation=3
        let err = profile.verify_advisory(CapabilityBits::ALL, 0, 5, SessionId(7)).unwrap_err();
        assert!(matches!(err, ProfileError::GenerationRevoked { .. }));
    }

    #[test]
    fn advisory_valid_generation() {
        let profile = advisory(CapabilityBits::READ, 5, u64::MAX, 7);  // generation=5
        assert!(profile.verify_advisory(CapabilityBits::ALL, 0, 5, SessionId(7)).is_ok());
    }

    #[test]
    fn asserted_returns_unimplemented() {
        let profile = SessionProfile {
            trust_level: TrustLevel::Asserted,
            ..advisory(CapabilityBits::READ, 0, u64::MAX, 1)
        };
        let err = profile.verify_asserted(CapabilityBits::ALL, 0, 0, SessionId(1)).unwrap_err();
        assert!(matches!(err, ProfileError::Unimplemented));
    }
}
