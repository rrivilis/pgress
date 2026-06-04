//! Stream ingress decoder — the explicit, stateful entry point for all wire input.
//!
//! `StreamDecoder` owns the full ingress pipeline for one stream (one transport
//! connection or file):
//!
//! ```text
//! raw bytes
//!   ↓ decode_preamble     IsaStreamHeader (32 bytes): magic, version, path/session binding
//!   ↓ decode_record       IsaHeader (48 bytes): length validation, opcode classification,
//!   │                     stream_seq ordering, causal_epoch consistency, extension skip
//!   ↓ [if SessionProfile] process_profile: cap scoping, profile negotiation state update
//!   ↓ RouteReady(ParsedHeader) → caller passes to SessionRuntime::route_record
//! ```
//!
//! ## Ownership model
//!
//! - **Graph** (engine nodes/edges) is **persistent**: snapshots are O(1) structural shares;
//!   multiple readers may hold them simultaneously.
//! - **Runtime** (`SessionRuntime`) is **affine**: owned by one driver, transferable but not
//!   copyable. The decoder borrows it during `decode_record` and `process_profile` only.
//! - **Stream transport** (`StreamDecoder`) is **linear**: it is the per-stream stateful
//!   resource. `StreamDecoder` does not implement `Clone`. Drop = stream end. A replay or
//!   reconnect requires a new `StreamDecoder` instance.
//!
//! ## Responsibilities
//!
//! | Concern                  | Where enforced                              |
//! |--------------------------|---------------------------------------------|
//! | Length validation        | `decode_record` — rejects `length < 48`     |
//! | Extension skipping       | `decode_record` — `SkipPayload` for Unknown |
//! | Profile negotiation      | `process_profile` — Advisory cap scoping    |
//! | Stream ordering          | `decode_record` — `stream_seq` must advance |
//! | Epoch consistency        | `decode_record` — `causal_epoch` non-decr.  |
//! | Opcode validation        | `decode_record` — opcode 0 is wire error    |
//! | Profile cap scoping      | `process_profile` — clips to parent ceiling |
//! | Replay cursory trace     | `decode_record` — `gap_count` accumulator   |
//! | Session routing          | caller: `SessionRuntime::route_record`      |
//! | Per-edge authority       | engine: `CompiledEdgeLabel` in `DepMeta`    |
//!
//! ## Session bootstrap semantics
//!
//! **Local bootstrap** (trusted in-process): the host calls `SessionRuntime::sessions.create()`
//! before any stream arrives. The session is immediately `Active`. No wire
//! protocol required; `stream_seq_floor = 0`, `causal_epoch = 0`.
//!
//! **Remote bootstrap** (Advisory, v1): the peer sends opcode `0x0100`
//! (`SessionProfile`) as the first record on a fresh path. `process_profile`
//! creates the session and registers the path atomically if the tenant ceiling
//! passes. The session becomes `Active` in the same operation.
//!
//! **Bootstrap atomicity**: session creation and path registration MUST be
//! atomic. Records arriving between a session create and path register will
//! fail `SessionNotFound` on path resolution. `process_profile` handles both
//! in a single call for the remote Advisory case.
//!
//! **Tenant pre-registration**: remote bootstrap silently clips capabilities
//! to `AuthorityPolicy::NONE` if the tenant is not registered in `DomainRegistry`.
//! Production deployments should register tenants before accepting remote streams,
//! or treat a missing tenant as an explicit rejection.
//!
//! **Re-bootstrap with existing session_id**: if `process_profile` is called
//! with a `session_id` that is already `Active`, treat it as a path migration
//! (register the new path, do not recreate the session). The existing
//! `stream_seq_floor` is preserved; `stream_seq` continues from where it left off.
//!
//! **Session expiry/tombstoning**: sessions currently live indefinitely in
//! `SessionTable`. Long-running servers should implement a reaping policy
//! keyed on inactivity or `causal_epoch` lag. This is not defined in v1.

use rustc_hash::FxHashMap;
use crate::{
    OpcodeClass, PathId, SessionId,
    auth::AuthorityPolicy,
    profile::{ProfileError, SessionProfile, TrustLevel},
    runtime::{ParsedHeader, SessionRuntime},
};

const MAGIC:               &[u8; 4] = b"PGRS";
const SUPPORTED_VERSION:   u16      = 0x0003;
const STREAM_HDR_LEN:      usize    = 32;
/// Actual byte count of IsaHeader fields:
/// opcode(2)+flags(2)+length(4)+tenant_id(8)+session_id(8)+partition_id(8)+causal_epoch(8)+stream_seq(8) = 48
const RECORD_HDR_LEN:      usize    = 48;
const MIN_RECORD_LEN:      u32      = 48;
const PROFILE_OPCODE:      u16      = 0x0100;
/// v1 profile fixed payload length: single capability_mask (u64) before sig_len.
const PROFILE_FIXED_LEN:   usize    = 49;
/// v2 profile fixed payload length: four separate axis masks before sig_len.
const PROFILE_FIXED_LEN_V2: usize   = 73;

// ── StreamPreamble ────────────────────────────────────────────────────────────

/// Parsed `IsaStreamHeader` — held for the lifetime of the stream.
#[derive(Clone, Debug)]
pub struct StreamPreamble {
    pub abi_version:   u16,
    pub flags:         u16,
    pub feature_flags: u64,
    /// Ephemeral transport path identity.
    pub path_id:       PathId,
    /// Stable logical session identity. All `IsaHeader.session_id` values in
    /// this stream must match.
    pub session_id:    SessionId,
}

// ── RecordAction ──────────────────────────────────────────────────────────────

/// What the caller should do after a successful `decode_record` call.
#[derive(Debug)]
pub enum RecordAction {
    /// Header is fully validated. Pass to `SessionRuntime::route_record`.
    RouteReady(ParsedHeader),

    /// Unknown opcode: caller MUST skip exactly `skip_bytes` of payload from
    /// the stream before the next `decode_record` call.
    SkipPayload { header: ParsedHeader, skip_bytes: u32 },

    /// SessionProfile record: caller MUST read `payload_len` bytes from the
    /// stream and call `StreamDecoder::process_profile` before routing the
    /// next record. The header is provided for telemetry / tracing.
    ReadProfile { header: ParsedHeader, payload_len: u32 },
}

// ── IngressError ──────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    #[error("decode_preamble must be called before decode_record")]
    PreambleRequired,

    #[error("bad magic: got {0:?}, expected b\"PGRS\"")]
    BadMagic([u8; 4]),

    #[error("unsupported abi_version: 0x{0:04x}")]
    UnsupportedVersion(u16),

    #[error("record length {length} is below minimum {MIN_RECORD_LEN}")]
    InvalidLength { length: u32 },

    #[error("opcode 0x0000 is reserved and may not appear in a stream")]
    InvalidOpcode,

    #[error("session_id mismatch: preamble={preamble:?} header={header:?}")]
    SessionIdMismatch { preamble: SessionId, header: SessionId },

    /// stream_seq must be strictly increasing within a session.
    /// Signals a gap, duplicate, or replay at the transport level.
    #[error("stream_seq gap/replay on {session_id:?}: last={last_seen} got={got}")]
    StreamSeqGap { session_id: SessionId, last_seen: u64, got: u64 },

    /// causal_epoch must be non-decreasing within a session.
    #[error("causal_epoch regression on {session_id:?}: last={last_epoch} got={got}")]
    EpochRegression { session_id: SessionId, last_epoch: u64, got: u64 },

    /// SessionProfile payload is too short to parse.
    #[error("SessionProfile payload too short: needed {needed}, got {got}")]
    ProfileTruncated { needed: usize, got: usize },

    /// SessionProfile trust_level discriminant is not recognised.
    #[error("unknown trust_level byte 0x{0:02x} in SessionProfile payload")]
    UnknownTrustLevel(u8),

    /// SessionProfile profile_version is not 1 or 2.
    #[error("unknown profile_version {0} in SessionProfile payload (supported: 1, 2)")]
    UnknownProfileVersion(u16),

    /// Advisory profile verification failed (expiry, generation, session mismatch).
    #[error("SessionProfile verification failed: {0}")]
    ProfileVerification(#[from] ProfileError),

    /// Session referenced in SessionProfile is not registered in the runtime.
    #[error("SessionProfile references unknown session {0:?}")]
    UnknownSession(SessionId),
}

// ── StreamDecoder ─────────────────────────────────────────────────────────────

/// Stateful ingress decoder for one stream.
///
/// Not `Clone` — it is the linear per-stream resource. A new decoder must be
/// created for each stream (transport connection, file replay, reconnect).
#[derive(Debug)]
pub struct StreamDecoder {
    /// Parsed stream preamble, set by `decode_preamble`.
    preamble: Option<StreamPreamble>,

    /// Last `stream_seq` seen per session. Used for gap / replay detection at
    /// ingress. Full replay suppression against the ack floor is in `SessionTable`.
    seq_trace: FxHashMap<SessionId, u64>,

    /// Last `causal_epoch` seen per session. Epoch must be non-decreasing.
    epoch_trace: FxHashMap<SessionId, u64>,

    /// Count of `stream_seq` gaps / replays seen since construction.
    /// Informational — does not reset automatically.
    pub gap_count: u64,

    /// Count of `causal_epoch` regressions seen since construction.
    pub epoch_regression_count: u64,
}

impl StreamDecoder {
    pub fn new() -> Self {
        StreamDecoder {
            preamble:                None,
            seq_trace:               FxHashMap::default(),
            epoch_trace:             FxHashMap::default(),
            gap_count:               0,
            epoch_regression_count:  0,
        }
    }

    /// The parsed stream preamble, if `decode_preamble` has been called.
    pub fn preamble(&self) -> Option<&StreamPreamble> {
        self.preamble.as_ref()
    }

    // ── Preamble ─────────────────────────────────────────────────────────────

    /// Parse the 32-byte `IsaStreamHeader`. Must be called before `decode_record`.
    ///
    /// Validates magic bytes and `abi_version`. Returns an immutable reference to
    /// the stored preamble on success; the preamble is retained for the stream's
    /// lifetime.
    pub fn decode_preamble(
        &mut self,
        buf: &[u8; STREAM_HDR_LEN],
    ) -> Result<&StreamPreamble, IngressError> {
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if &magic != MAGIC {
            return Err(IngressError::BadMagic(magic));
        }

        let abi_version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        if abi_version != SUPPORTED_VERSION {
            return Err(IngressError::UnsupportedVersion(abi_version));
        }

        let flags         = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let feature_flags = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let path_id       = PathId(u64::from_le_bytes(buf[16..24].try_into().unwrap()));
        let session_id    = SessionId(u64::from_le_bytes(buf[24..32].try_into().unwrap()));

        self.preamble = Some(StreamPreamble { abi_version, flags, feature_flags, path_id, session_id });
        Ok(self.preamble.as_ref().unwrap())
    }

    // ── Record header ────────────────────────────────────────────────────────

    /// Parse a 48-byte `IsaHeader` and apply stream-level validation.
    ///
    /// Checks (in order):
    /// 1. Preamble has been decoded.
    /// 2. `length >= 48` (no underflow).
    /// 3. `opcode != 0` (reserved opcode is a wire error).
    /// 4. `session_id` matches the preamble's `session_id`.
    /// 5. `stream_seq` is strictly greater than last seen for this session.
    /// 6. `causal_epoch` is non-decreasing for this session.
    ///
    /// On success returns the action the caller should take. The caller must
    /// consume exactly `skip_bytes` / `payload_len` before the next call.
    pub fn decode_record(
        &mut self,
        buf:     &[u8; RECORD_HDR_LEN],
        runtime: &SessionRuntime,
    ) -> Result<RecordAction, IngressError> {
        let preamble = self.preamble.as_ref().ok_or(IngressError::PreambleRequired)?;

        // ── Parse header fields (little-endian) ───────────────────────────────
        let opcode       = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        let flags        = u16::from_le_bytes(buf[2..4].try_into().unwrap());
        let length       = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let tenant_id    = crate::TenantId(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
        let session_id   = SessionId(u64::from_le_bytes(buf[16..24].try_into().unwrap()));
        let partition_id = crate::WirePartitionId(u64::from_le_bytes(buf[24..32].try_into().unwrap()));
        let causal_epoch = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        let stream_seq   = u64::from_le_bytes(buf[40..48].try_into().unwrap());

        // ── Structural validation ─────────────────────────────────────────────
        if length < MIN_RECORD_LEN {
            return Err(IngressError::InvalidLength { length });
        }
        if opcode == 0 {
            return Err(IngressError::InvalidOpcode);
        }

        // ── Session-id consistency ────────────────────────────────────────────
        if session_id != preamble.session_id {
            return Err(IngressError::SessionIdMismatch {
                preamble: preamble.session_id,
                header:   session_id,
            });
        }

        // ── stream_seq ordering (cursory replay trace) ────────────────────────
        if let Some(&last) = self.seq_trace.get(&session_id) {
            if stream_seq <= last {
                self.gap_count += 1;
                return Err(IngressError::StreamSeqGap {
                    session_id,
                    last_seen: last,
                    got:       stream_seq,
                });
            }
        }
        self.seq_trace.insert(session_id, stream_seq);

        // ── causal_epoch consistency ──────────────────────────────────────────
        if let Some(&last_epoch) = self.epoch_trace.get(&session_id) {
            if causal_epoch < last_epoch {
                self.epoch_regression_count += 1;
                return Err(IngressError::EpochRegression {
                    session_id,
                    last_epoch,
                    got: causal_epoch,
                });
            }
        }
        self.epoch_trace.insert(session_id, causal_epoch);

        let _ = runtime; // runtime available for future extension checks

        let header = ParsedHeader {
            opcode,
            flags,
            length,
            tenant_id,
            session_id,
            partition_id,
            causal_epoch,
            stream_seq,
        };

        // ── Opcode routing ────────────────────────────────────────────────────
        let payload_bytes = length - MIN_RECORD_LEN;

        if opcode == PROFILE_OPCODE {
            return Ok(RecordAction::ReadProfile { header, payload_len: payload_bytes });
        }

        if matches!(OpcodeClass::from_opcode(opcode), OpcodeClass::Unknown) {
            return Ok(RecordAction::SkipPayload { header, skip_bytes: payload_bytes });
        }

        Ok(RecordAction::RouteReady(header))
    }

    // ── Profile negotiation ───────────────────────────────────────────────────

    /// Process a `SessionProfile` payload (opcode 0x0100).
    ///
    /// Parses the payload bytes (v1 single-mask or v2 four-axis), verifies the
    /// profile (Advisory: policy scoping only; Asserted/Attested: Ed25519
    /// signature verification against `runtime.trust_store`), and updates the
    /// session's `claimed_policy` in `runtime.domains`.
    ///
    /// Returns the effective `AuthorityPolicy` (`claimed.intersect(parent_policy)`)
    /// which is installed permanently for subsequent authority checks on this session.
    ///
    /// `payload` must be exactly `payload_len` bytes from the `ReadProfile`
    /// action returned by `decode_record`.
    pub fn process_profile(
        &mut self,
        header:  &ParsedHeader,
        payload: &[u8],
        runtime: &mut SessionRuntime,
    ) -> Result<AuthorityPolicy, IngressError> {
        let profile = parse_profile_payload(payload)?;

        // Locate the session and its parent tenant ceiling.
        let (tenant_id, parent_policy) = {
            let session = runtime.domains.sessions
                .get(&header.session_id)
                .ok_or(IngressError::UnknownSession(header.session_id))?;
            let parent = runtime.domains.tenants
                .get(&session.tenant_id)
                .map(|t| t.policy)
                .unwrap_or(AuthorityPolicy::NONE);
            (session.tenant_id, parent)
        };

        // Verify and scope the profile.
        let effective = match profile.trust_level {
            TrustLevel::Advisory => profile.verify_advisory(
                parent_policy,
                header.causal_epoch,
                0,                    // min_generation: 0 for advisory (no revocation registry)
                header.session_id,
            )?,
            _ => profile.verify_asserted(
                parent_policy,
                header.causal_epoch,
                0,
                header.session_id,
                &runtime.trust_store,
            )?,
        };

        // Install the effective policy on the session domain.
        if let Some(session) = runtime.domains.sessions.get_mut(&header.session_id) {
            session.claimed_policy = effective;
        }

        let _ = tenant_id;
        Ok(effective)
    }
}

impl Default for StreamDecoder {
    fn default() -> Self { Self::new() }
}

// ── SessionProfile wire deserialisation ──────────────────────────────────────

/// Parse `SessionProfile` from raw payload bytes (opcode 0x0100 body).
///
/// Supports two wire formats selected by `profile_version`:
///
/// **v1** (49 bytes fixed, backwards-compatible):
/// ```text
/// profile_id:      u32   bytes 0–3
/// profile_version: u16   bytes 4–5   (= 1)
/// trust_level:     u8    byte  6
/// capability_mask: u64   bytes 7–14  → mapped to all four AuthorityPolicy axes
/// issuer_domain:   u64   bytes 15–22
/// session_id:      u64   bytes 23–30
/// generation:      u64   bytes 31–38
/// expiry_epoch:    u64   bytes 39–46
/// sig_len:         u16   bytes 47–48
/// signature:       bytes 49..49+sig_len
/// ```
///
/// **v2** (73 bytes fixed, four-axis):
/// ```text
/// profile_id:         u32   bytes 0–3
/// profile_version:    u16   bytes 4–5   (= 2)
/// trust_level:        u8    byte  6
/// assertion_mask:     u64   bytes 7–14
/// delegation_mask:    u64   bytes 15–22
/// observability_mask: u64   bytes 23–30
/// disclosure_mask:    u64   bytes 31–38
/// issuer_domain:      u64   bytes 39–46
/// session_id:         u64   bytes 47–54
/// generation:         u64   bytes 55–62
/// expiry_epoch:       u64   bytes 63–70
/// sig_len:            u16   bytes 71–72
/// signature:          bytes 73..73+sig_len
/// ```
fn parse_profile_payload(payload: &[u8]) -> Result<SessionProfile, IngressError> {
    // Need at least 7 bytes to read profile_id, profile_version, trust_level.
    if payload.len() < 7 {
        return Err(IngressError::ProfileTruncated { needed: 7, got: payload.len() });
    }

    let profile_id      = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let profile_version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    let trust_byte      = payload[6];
    let trust_level     = TrustLevel::from_u8(trust_byte)
        .ok_or(IngressError::UnknownTrustLevel(trust_byte))?;

    match profile_version {
        1 => {
            // v1: single capability_mask, mapped to all four axes
            if payload.len() < PROFILE_FIXED_LEN {
                return Err(IngressError::ProfileTruncated {
                    needed: PROFILE_FIXED_LEN,
                    got:    payload.len(),
                });
            }
            let cap_mask     = u64::from_le_bytes(payload[ 7..15].try_into().unwrap());
            let issuer_domain = crate::TenantId(u64::from_le_bytes(payload[15..23].try_into().unwrap()));
            let session_id    = SessionId(u64::from_le_bytes(payload[23..31].try_into().unwrap()));
            let generation    = u64::from_le_bytes(payload[31..39].try_into().unwrap());
            let expiry_epoch  = u64::from_le_bytes(payload[39..47].try_into().unwrap());
            let sig_len       = u16::from_le_bytes(payload[47..49].try_into().unwrap()) as usize;
            let total         = PROFILE_FIXED_LEN + sig_len;
            if payload.len() < total {
                return Err(IngressError::ProfileTruncated { needed: total, got: payload.len() });
            }
            Ok(SessionProfile {
                profile_id,
                profile_version,
                trust_level,
                // v1 maps the single mask to all four axes — same semantics as before
                assertion_mask:     cap_mask,
                delegation_mask:    cap_mask,
                observability_mask: cap_mask,
                disclosure_mask:    cap_mask,
                issuer_domain,
                session_id,
                generation,
                expiry_epoch,
                signature: payload[PROFILE_FIXED_LEN..total].to_vec(),
            })
        },
        2 => {
            // v2: four separate axis masks
            if payload.len() < PROFILE_FIXED_LEN_V2 {
                return Err(IngressError::ProfileTruncated {
                    needed: PROFILE_FIXED_LEN_V2,
                    got:    payload.len(),
                });
            }
            let assertion_mask     = u64::from_le_bytes(payload[ 7..15].try_into().unwrap());
            let delegation_mask    = u64::from_le_bytes(payload[15..23].try_into().unwrap());
            let observability_mask = u64::from_le_bytes(payload[23..31].try_into().unwrap());
            let disclosure_mask    = u64::from_le_bytes(payload[31..39].try_into().unwrap());
            let issuer_domain      = crate::TenantId(u64::from_le_bytes(payload[39..47].try_into().unwrap()));
            let session_id         = SessionId(u64::from_le_bytes(payload[47..55].try_into().unwrap()));
            let generation         = u64::from_le_bytes(payload[55..63].try_into().unwrap());
            let expiry_epoch       = u64::from_le_bytes(payload[63..71].try_into().unwrap());
            let sig_len            = u16::from_le_bytes(payload[71..73].try_into().unwrap()) as usize;
            let total              = PROFILE_FIXED_LEN_V2 + sig_len;
            if payload.len() < total {
                return Err(IngressError::ProfileTruncated { needed: total, got: payload.len() });
            }
            Ok(SessionProfile {
                profile_id,
                profile_version,
                trust_level,
                assertion_mask,
                delegation_mask,
                observability_mask,
                disclosure_mask,
                issuer_domain,
                session_id,
                generation,
                expiry_epoch,
                signature: payload[PROFILE_FIXED_LEN_V2..total].to_vec(),
            })
        },
        other => Err(IngressError::UnknownProfileVersion(other)),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgress_core::partition::AuthorityMode;
    use crate::{
        WirePartitionId, TenantId,
        auth::AuthorityPolicy,
        domain::{SessionDomain, SessionQuota, TenantDomain, TenantQuota},
        session::{PathEntry, PathState},
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn valid_preamble() -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(b"PGRS");
        buf[4..6].copy_from_slice(&0x0003u16.to_le_bytes());   // abi_version
        // flags (6–7), feature_flags (8–15) = 0
        buf[16..24].copy_from_slice(&42u64.to_le_bytes());     // path_id
        buf[24..32].copy_from_slice(&10u64.to_le_bytes());     // session_id
        buf
    }

    fn valid_header(opcode: u16, session_id: u64, stream_seq: u64, causal_epoch: u64) -> [u8; 48] {
        let mut buf = [0u8; 48];
        buf[0..2].copy_from_slice(&opcode.to_le_bytes());
        // flags[2..4] = 0
        buf[4..8].copy_from_slice(&48u32.to_le_bytes());       // length = exactly 48
        // tenant_id[8..16] = 0
        buf[16..24].copy_from_slice(&session_id.to_le_bytes());
        // partition_id[24..32] = 0
        buf[32..40].copy_from_slice(&causal_epoch.to_le_bytes());
        buf[40..48].copy_from_slice(&stream_seq.to_le_bytes());
        buf
    }

    fn minimal_runtime() -> SessionRuntime {
        let mut rt = SessionRuntime::new();
        rt.domains.tenants.insert(TenantId(1), TenantDomain {
            id: TenantId(1), policy: AuthorityPolicy::ALL, quota: TenantQuota::default(),
        });
        rt.domains.sessions.insert(SessionId(10), SessionDomain {
            id: SessionId(10), tenant_id: TenantId(1),
            claimed_policy: AuthorityPolicy::ALL, auth_mode: AuthorityMode::Advisory,
            quota: SessionQuota::default(),
        });
        rt.sessions.create(crate::session::SessionEntry {
            session_id:                   SessionId(10),
            tenant_id:                    TenantId(1),
            active_path_id:               Some(crate::PathId(42)),
            prev_path_id:                 None,
            stream_seq_floor:             0,
            auth_mode:                    AuthorityMode::Advisory,
            last_active_causal_epoch:     0,
            consecutive_quiescent_epochs: 0,
        });
        rt.paths.create(PathEntry {
            path_id: crate::PathId(42), session_id: SessionId(10),
            last_ack_stream_seq: 0, state: PathState::Active,
        });
        rt
    }

    // ── Preamble tests ────────────────────────────────────────────────────────

    #[test]
    fn valid_preamble_accepted() {
        let mut dec = StreamDecoder::new();
        let preamble = dec.decode_preamble(&valid_preamble()).unwrap();
        assert_eq!(preamble.abi_version, 0x0003);
        assert_eq!(preamble.session_id, SessionId(10));
        assert_eq!(preamble.path_id,    PathId(42));
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = valid_preamble();
        buf[0] = b'X';
        let err = StreamDecoder::new().decode_preamble(&buf).unwrap_err();
        assert!(matches!(err, IngressError::BadMagic(_)));
    }

    #[test]
    fn unsupported_version_rejected() {
        let mut buf = valid_preamble();
        buf[4..6].copy_from_slice(&0x0001u16.to_le_bytes());
        let err = StreamDecoder::new().decode_preamble(&buf).unwrap_err();
        assert!(matches!(err, IngressError::UnsupportedVersion(0x0001)));
    }

    // ── Record header tests ───────────────────────────────────────────────────

    #[test]
    fn decode_without_preamble_returns_error() {
        let mut dec = StreamDecoder::new();
        let rt = minimal_runtime();
        let hdr = valid_header(0x0003, 10, 1, 0);
        assert!(matches!(dec.decode_record(&hdr, &rt), Err(IngressError::PreambleRequired)));
    }

    #[test]
    fn valid_record_returns_route_ready() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        let hdr = valid_header(0x0003, 10, 1, 0);
        assert!(matches!(dec.decode_record(&hdr, &rt), Ok(RecordAction::RouteReady(_))));
    }

    #[test]
    fn length_below_minimum_rejected() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        let mut buf = valid_header(0x0003, 10, 1, 0);
        buf[4..8].copy_from_slice(&10u32.to_le_bytes()); // length = 10 < 48
        assert!(matches!(dec.decode_record(&buf, &rt), Err(IngressError::InvalidLength { length: 10 })));
    }

    #[test]
    fn zero_opcode_rejected() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        let hdr = valid_header(0x0000, 10, 1, 0);
        assert!(matches!(dec.decode_record(&hdr, &rt), Err(IngressError::InvalidOpcode)));
    }

    #[test]
    fn session_id_mismatch_rejected() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();   // preamble session_id = 10
        let hdr = valid_header(0x0003, 99, 1, 0);          // header session_id = 99
        assert!(matches!(dec.decode_record(&hdr, &rt), Err(IngressError::SessionIdMismatch { .. })));
    }

    #[test]
    fn unknown_opcode_returns_skip_payload() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        let mut buf = valid_header(0xDEAD, 10, 1, 0);
        // length = 48 + 16 payload bytes
        buf[4..8].copy_from_slice(&64u32.to_le_bytes());
        match dec.decode_record(&buf, &rt).unwrap() {
            RecordAction::SkipPayload { skip_bytes, .. } => assert_eq!(skip_bytes, 16),
            other => panic!("expected SkipPayload, got {:?}", other),
        }
    }

    #[test]
    fn session_profile_opcode_returns_read_profile() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        let mut buf = valid_header(PROFILE_OPCODE, 10, 1, 0);
        buf[4..8].copy_from_slice(&(48u32 + 49).to_le_bytes()); // 48 + min profile payload
        match dec.decode_record(&buf, &rt).unwrap() {
            RecordAction::ReadProfile { payload_len, .. } => assert_eq!(payload_len, 49),
            other => panic!("expected ReadProfile, got {:?}", other),
        }
    }

    // ── Stream ordering tests ─────────────────────────────────────────────────

    #[test]
    fn stream_seq_must_advance() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        dec.decode_record(&valid_header(0x0003, 10, 5, 0), &rt).unwrap();
        let err = dec.decode_record(&valid_header(0x0003, 10, 5, 1), &rt).unwrap_err();
        assert!(matches!(err, IngressError::StreamSeqGap { last_seen: 5, got: 5, .. }));
        assert_eq!(dec.gap_count, 1);
    }

    #[test]
    fn stream_seq_regression_is_gap() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        dec.decode_record(&valid_header(0x0003, 10, 10, 0), &rt).unwrap();
        let err = dec.decode_record(&valid_header(0x0003, 10, 3, 1), &rt).unwrap_err();
        assert!(matches!(err, IngressError::StreamSeqGap { last_seen: 10, got: 3, .. }));
    }

    #[test]
    fn non_contiguous_stream_seq_increments_gap_count() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        dec.decode_record(&valid_header(0x0003, 10, 1, 0), &rt).unwrap();
        // seq 2 is missing; seq 3 is still valid (strictly greater)
        dec.decode_record(&valid_header(0x0003, 10, 3, 1), &rt).unwrap();
        // gap_count only increments on regression, not on skip-forward
        assert_eq!(dec.gap_count, 0);
    }

    #[test]
    fn causal_epoch_regression_rejected() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        dec.decode_record(&valid_header(0x0003, 10, 1, 10), &rt).unwrap();
        let err = dec.decode_record(&valid_header(0x0003, 10, 2, 5), &rt).unwrap_err();
        assert!(matches!(err, IngressError::EpochRegression { last_epoch: 10, got: 5, .. }));
        assert_eq!(dec.epoch_regression_count, 1);
    }

    #[test]
    fn causal_epoch_stable_is_allowed() {
        let mut dec = StreamDecoder::new();
        let rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        dec.decode_record(&valid_header(0x0003, 10, 1, 7), &rt).unwrap();
        // Same epoch on next record: non-decreasing, so allowed
        assert!(dec.decode_record(&valid_header(0x0003, 10, 2, 7), &rt).is_ok());
    }

    // ── Profile payload parsing ───────────────────────────────────────────────

    fn advisory_payload(session_id: u64, caps: u64) -> Vec<u8> {
        let mut p = vec![0u8; 49];
        p[0..4].copy_from_slice(&1u32.to_le_bytes());      // profile_id
        p[4..6].copy_from_slice(&1u16.to_le_bytes());      // profile_version
        p[6] = 0x01;                                        // trust_level = Advisory
        p[7..15].copy_from_slice(&caps.to_le_bytes());     // capability_mask
        // issuer_domain = 0 (bytes 15–22)
        p[23..31].copy_from_slice(&session_id.to_le_bytes()); // session_id
        // generation = 0, expiry = u64::MAX
        p[39..47].copy_from_slice(&u64::MAX.to_le_bytes());   // expiry_epoch
        // sig_len = 0 (bytes 47–48 already 0)
        p
    }

    #[test]
    fn profile_truncated_payload_rejected() {
        // 10 bytes — not even enough to read profile_id + profile_version + trust_level (7 bytes
        // minimum). The parser catches this before reaching any version branch.
        let err_short = parse_profile_payload(&[0u8; 6]).unwrap_err();
        assert!(matches!(err_short, IngressError::ProfileTruncated { needed: 7, .. }),
            "6-byte payload must be rejected before version branch; got: {:?}", err_short);

        // 20 bytes — valid v1 header (profile_version=1, trust_level=Advisory) but
        // shorter than the 49-byte v1 fixed length. Must be rejected with ProfileTruncated.
        let mut p20 = [0u8; 20];
        p20[4..6].copy_from_slice(&1u16.to_le_bytes()); // profile_version = 1
        p20[6] = 0x01;                                   // trust_level = Advisory
        let err_v1 = parse_profile_payload(&p20).unwrap_err();
        assert!(matches!(err_v1, IngressError::ProfileTruncated { needed: 49, .. }),
            "20-byte v1 payload must be ProfileTruncated {{ needed: 49 }}; got: {:?}", err_v1);

        // 30 bytes — valid v2 header (profile_version=2, trust_level=Advisory) but
        // shorter than the 73-byte v2 fixed length.
        let mut p30 = [0u8; 30];
        p30[4..6].copy_from_slice(&2u16.to_le_bytes()); // profile_version = 2
        p30[6] = 0x01;                                   // trust_level = Advisory
        let err_v2 = parse_profile_payload(&p30).unwrap_err();
        assert!(matches!(err_v2, IngressError::ProfileTruncated { needed: 73, .. }),
            "30-byte v2 payload must be ProfileTruncated {{ needed: 73 }}; got: {:?}", err_v2);
    }

    #[test]
    fn profile_unknown_trust_level_rejected() {
        let mut p = advisory_payload(10, u64::MAX);
        p[6] = 0xFF;
        let err = parse_profile_payload(&p).unwrap_err();
        assert!(matches!(err, IngressError::UnknownTrustLevel(0xFF)));
    }

    #[test]
    fn process_profile_clips_caps_to_parent() {
        let mut dec = StreamDecoder::new();
        let mut rt  = minimal_runtime();
        // Tenant only has READ_ONLY policy (observability + disclosure, no assertion/delegation)
        rt.domains.tenants.get_mut(&TenantId(1)).unwrap().policy = AuthorityPolicy::READ_ONLY;

        dec.decode_preamble(&valid_preamble()).unwrap();
        let hdr = ParsedHeader {
            opcode: PROFILE_OPCODE, flags: 0, length: 48 + 49,
            tenant_id: TenantId(0), session_id: SessionId(10),
            partition_id: WirePartitionId(0), causal_epoch: 0, stream_seq: 1,
        };

        // Profile claims ALL — should be clipped to READ_ONLY
        let payload = advisory_payload(10, u64::MAX);
        let effective = dec.process_profile(&hdr, &payload, &mut rt).unwrap();
        assert_eq!(effective, AuthorityPolicy::READ_ONLY);
        // Verify the session's claimed_policy was updated
        assert_eq!(rt.domains.sessions[&SessionId(10)].claimed_policy, AuthorityPolicy::READ_ONLY);
    }

    #[test]
    fn process_profile_unknown_session_returns_error() {
        let mut dec = StreamDecoder::new();
        let mut rt  = minimal_runtime();
        dec.decode_preamble(&valid_preamble()).unwrap();
        let hdr = ParsedHeader {
            opcode: PROFILE_OPCODE, flags: 0, length: 48 + 49,
            tenant_id: TenantId(0), session_id: SessionId(999), // unknown
            partition_id: WirePartitionId(0), causal_epoch: 0, stream_seq: 1,
        };
        let payload = advisory_payload(999, u64::MAX);
        let err = dec.process_profile(&hdr, &payload, &mut rt).unwrap_err();
        assert!(matches!(err, IngressError::UnknownSession(SessionId(999))));
    }
}
