//! Dispatcher — the high (driver) layer that wires session-rs gate to pgress-core.
//!
//! Implements:
//!   - `parse_payload`: wire bytes → `IsaOp` for all 17 core opcodes.
//!   - `EnginePool`: `ShardId → Engine` map with get-or-create semantics.
//!   - `Dispatcher`: drives the full pipeline from raw wire bytes through
//!     `engine.apply()`, and feeds partition creation back into the session
//!     runtime's `DomainRegistry`.
//!
//! ## Relationship to session-rs
//!
//! `SessionRuntime` is the **low (gate) layer**: it authenticates, authorises,
//! and applies admission control on record headers only, without touching the
//! record body. The `Dispatcher` is the **high (driver) layer**: it owns the
//! `EnginePool`, drives body parsing, calls `engine.apply()`, and propagates
//! control-op outcomes back into the session runtime.
//!
//! ```text
//! Dispatcher {
//!     decoder: StreamDecoder,   // ingress: wire bytes → ParsedHeader
//!     runtime: SessionRuntime,  // gate:    ParsedHeader → RouteOutcome
//!     shards:  EnginePool,      // exec:    ShardId → engine.apply()
//! }
//! ```
//!
//! ## End-to-end flow
//!
//! ```text
//! raw bytes (TCP / QUIC / file)
//!   ↓ process_preamble  — validates stream header (magic, version, path/session)
//!   ↓ [loop]
//!   ↓ process_record    — per-record:
//!       ↓ StreamDecoder::decode_record   — header validation, opcode routing
//!       ↓ [if SessionProfile] handle_profile — Advisory bootstrap
//!       ↓ [if RouteReady] SessionRuntime::route_record — gate
//!       ↓ parse_payload  — wire bytes → IsaOp
//!       ↓ EnginePool::apply
//!       ↓ SessionRuntime::update_pressure
//!       ↓ [PartitionCreate] feedback → DomainRegistry + PartitionAuthTable
//! ```
//!
//! ## v1 scope
//!
//! - Only `ADVISORY` bootstrap is wired end-to-end. `ASSERTED`/`ATTESTED` return
//!   `ProfileVerification(Unimplemented)` until signing infrastructure is wired
//!   (see `profile.rs`).
//! - `DepKind::Remote` (cross-partition edges) returns `RemoteDepNotSupported`
//!   in v1. Full `RemoteDep` wire encoding will be defined in a future ABI revision.
//! - After `PartitionCreate` succeeds on the engine, the Dispatcher registers the
//!   new partition in `runtime.domains.partitions` with `ShardId(0)` (the bootstrap
//!   shard). Multi-shard placement is a future enhancement.
//! - Session expiry/tombstoning is not implemented: sessions created via remote
//!   bootstrap live indefinitely in `SessionTable`. See `docs/integration.md`.

use rustc_hash::FxHashMap;
use pgress_core::{
    attr::{Attrs, Val},
    engine::{Engine, EngineError},
    isa::IsaOp,
    node::{
        ComputeRule, ConvergencePolicy, ExecMode, ExecutionPolicy, NodeKind,
        Port, PortKind, QueuePriority, RewriteBudget, RewriteStrategy,
        RetryPolicy, StabilizationConfig, StabilizationDomain,
    },
    partition::{CapabilityBits, CausalScope, DepKind, EdgeLabel, LatticeClass, PartitionId},
    propagate::PropEvent,
    region::{CompilePolicy, RegionBoundary, StabilityContract},
    ternary::T,
    uid::Uid,
};

use crate::{
    admission::ShardPressure,
    auth::{AuthTableKey, PartitionAuthRow},
    decode::{IngressError, RecordAction, StreamDecoder, StreamPreamble},
    domain::{PartitionDomain, SessionDomain, SessionQuota},
    runtime::{ParsedHeader, RejectionReason, RouteOutcome, SessionRuntime},
    session::{PathEntry, PathState, SessionEntry},
    OpcodeClass, PathId, SessionId, ShardId, TenantId, WirePartitionId,
};

// ── UID / partition-ID translation ───────────────────────────────────────────

/// Translate a wire `u64` node UID to the engine's `Uid` (`uuid::Uuid`).
///
/// The mapping is `Uuid::from_u128(wire_id as u128)` — deterministic,
/// invertible, requires no registry. Wire UID `0` is the null handle; the
/// engine will reject operations on nodes that have never been created.
#[inline]
fn wire_to_uid(id: u64) -> Uid {
    Uid::from_u128(id as u128)
}

/// Translate a wire `u64` partition ID to the engine's `PartitionId` (`uuid::Uuid`).
#[inline]
fn wire_to_partition_id(id: u64) -> PartitionId {
    PartitionId::from_u128(id as u128)
}

// ── T from wire i8 ────────────────────────────────────────────────────────────

fn wire_to_t(v: i8) -> Result<T, DispatchError> {
    match v {
        -1 => Ok(T::Neg),
         0 => Ok(T::Zero),
         1 => Ok(T::Pos),
         _ => Err(DispatchError::InvalidTernaryValue(v)),
    }
}

// ── Payload reader ────────────────────────────────────────────────────────────

/// Cursor-based reader over a byte slice. All integer reads are little-endian.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self { Reader { buf, pos: 0 } }

    fn remaining(&self) -> usize { self.buf.len().saturating_sub(self.pos) }

    fn need(&self, n: usize) -> Result<(), DispatchError> {
        if self.remaining() < n {
            Err(DispatchError::PayloadTruncated { needed: n, available: self.remaining() })
        } else {
            Ok(())
        }
    }

    fn read_u8(&mut self) -> Result<u8, DispatchError> {
        self.need(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_i8(&mut self) -> Result<i8, DispatchError> {
        Ok(self.read_u8()? as i8)
    }

    fn read_u16(&mut self) -> Result<u16, DispatchError> {
        self.need(2)?;
        let v = u16::from_le_bytes(self.buf[self.pos..self.pos+2].try_into().unwrap());
        self.pos += 2;
        Ok(v)
    }

    fn read_u32(&mut self) -> Result<u32, DispatchError> {
        self.need(4)?;
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos+4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_u64(&mut self) -> Result<u64, DispatchError> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos+8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_i64(&mut self) -> Result<i64, DispatchError> {
        self.need(8)?;
        let v = i64::from_le_bytes(self.buf[self.pos..self.pos+8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_f64(&mut self) -> Result<f64, DispatchError> {
        self.need(8)?;
        let bits = u64::from_le_bytes(self.buf[self.pos..self.pos+8].try_into().unwrap());
        self.pos += 8;
        // Canonicalize NaN per ABI spec: any NaN → quiet NaN 0x7FF8000000000000
        let v = f64::from_bits(bits);
        if v.is_nan() { Ok(f64::from_bits(0x7FF8_0000_0000_0000)) } else { Ok(v) }
    }

    /// Read a u16-length-prefixed UTF-8 string.
    fn read_str_u16(&mut self) -> Result<String, DispatchError> {
        let len = self.read_u16()? as usize;
        self.need(len)?;
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos+len])
            .map_err(|_| DispatchError::InvalidUtf8)?
            .to_owned();
        self.pos += len;
        Ok(s)
    }

    /// Read a u32-length-prefixed byte blob (for Bytes attr tag).
    fn read_bytes_u32(&mut self) -> Result<Vec<u8>, DispatchError> {
        let len = self.read_u32()? as usize;
        self.need(len)?;
        let v = self.buf[self.pos..self.pos+len].to_vec();
        self.pos += len;
        Ok(v)
    }
}

// ── AttrMap parser ────────────────────────────────────────────────────────────

/// Parse an `AttrMap` from the reader.
///
/// Wire format (per `spec/abi.md`):
/// ```text
/// count:   u16
/// entry[]: { key_len:u16, key:utf8, tag:u8, payload:… }
/// ```
///
/// Tags 0x05 (Bytes) and 0x07 (Uid) have no direct `Val` variant: Bytes
/// returns `UnsupportedAttrTag`; Uid is stored as `Val::Int(uid as i64)`.
fn parse_attr_map(r: &mut Reader) -> Result<Attrs, DispatchError> {
    let count = r.read_u16()?;
    let mut attrs = Attrs::new();
    for _ in 0..count {
        let key = r.read_str_u16()?;
        let tag = r.read_u8()?;
        let val = match tag {
            0x01 => Val::Bool(r.read_u8()? != 0),
            0x02 => Val::Int(r.read_i64()?),
            0x03 => Val::Float(r.read_f64()?),
            0x04 => {
                // Text: u16-prefixed utf8
                let len = r.read_u16()? as usize;
                r.need(len)?;
                let s = std::str::from_utf8(&r.buf[r.pos..r.pos+len])
                    .map_err(|_| DispatchError::InvalidUtf8)?
                    .to_owned();
                r.pos += len;
                Val::Text(s)
            },
            0x05 => {
                // Bytes: no Val::Bytes variant — skip and error.
                let _skip = r.read_bytes_u32()?;
                return Err(DispatchError::UnsupportedAttrTag(0x05));
            },
            0x06 => Val::Ternary(wire_to_t(r.read_i8()?)?),
            0x07 => {
                // Uid: no Val::Uid variant — store as Int (wire u64 → i64; high
                // UIDs wrap silently, which is acceptable for v1 observability use).
                let uid = r.read_u64()?;
                Val::Int(uid as i64)
            },
            other => return Err(DispatchError::UnsupportedAttrTag(other)),
        };
        attrs.set(key, val);
    }
    Ok(attrs)
}

// ── parse_payload ─────────────────────────────────────────────────────────────

/// Deserialise `payload` bytes into an `IsaOp` for the given `opcode`.
///
/// Implements the inverse of the payload encoding rules in `spec/abi.md`.
/// All integers are little-endian. Strings are `u16`-length-prefixed UTF-8.
///
/// Called by `Dispatcher` after `SessionRuntime::route_record` returns
/// `Admitted`. The caller must pass exactly `header.length − 48` bytes.
///
/// # Errors
///
/// - `PayloadTruncated` — payload is too short for the declared opcode.
/// - `InvalidTernaryValue` — an `i8` value is not in `{−1, 0, 1}`.
/// - `UnknownNodeRule` / `UnknownPortKind` / `UnknownDepKind` / `UnknownExecMode`
///   — an enum discriminant byte is out of range.
/// - `RemoteDepNotSupported` — `dep_kind=1` (Remote) is not supported in v1.
/// - `InvalidUtf8` — a string field is not valid UTF-8.
/// - `UnknownOpcode` — the opcode is not in the 17-opcode core ISA. (Unknown
///   opcodes should have been routed to `SkipPayload` before reaching here.)
pub fn parse_payload(opcode: u16, payload: &[u8]) -> Result<IsaOp, DispatchError> {
    let mut r = Reader::new(payload);
    match opcode {

        // ── Extension (0x0001–0x0005) ─────────────────────────────────────────

        0x0001 => {
            // NodeCreate: uid:u64, typ:str_u16, rule:u8, count:u16, attrs…
            let uid  = wire_to_uid(r.read_u64()?);
            let typ  = r.read_str_u16()?;
            let rule = r.read_u8()?;
            let kind = match rule {
                0x00 => NodeKind::Input,
                0x01 => NodeKind::Computed(ComputeRule::Identity),
                0x02 => NodeKind::Computed(ComputeRule::MvNeg),
                0x03 => NodeKind::Computed(ComputeRule::MvAdd),
                0x04 => NodeKind::Computed(ComputeRule::MvMul),
                0x05 => NodeKind::Computed(ComputeRule::MvSub),
                0x06 => NodeKind::Computed(ComputeRule::Merge),
                0x07 => NodeKind::Computed(ComputeRule::MeetAll),
                0x08 => NodeKind::Computed(ComputeRule::JoinAny),
                0x09 => NodeKind::Computed(ComputeRule::BochvarFold),
                0x0A => NodeKind::Computed(ComputeRule::PowerProduct),
                b    => return Err(DispatchError::UnknownNodeRule(b)),
            };
            let attrs = parse_attr_map(&mut r)?;
            Ok(IsaOp::NodeCreate { id: uid, typ, rule: kind, attrs })
        },

        0x0002 => {
            // EdgeConnect: edge_id:u64, src:u64, tgt:u64, dep_kind:u8,
            //              port_kind:u8, port_name:str_u16
            let edge_id   = wire_to_uid(r.read_u64()?);
            let src       = wire_to_uid(r.read_u64()?);
            let tgt       = wire_to_uid(r.read_u64()?);
            let dep_kind  = r.read_u8()?;
            let port_kind = r.read_u8()?;
            let port_name = r.read_str_u16()?;

            let pk = match port_kind {
                0x00 => PortKind::Signal,
                0x01 => PortKind::Effort,
                0x02 => PortKind::Flow,
                0x03 => PortKind::Bond,
                b    => return Err(DispatchError::UnknownPortKind(b)),
            };

            let dep = match dep_kind {
                0x00 => {
                    // Local: port is Some when port_name is non-empty.
                    let port = if port_name.is_empty() {
                        None
                    } else {
                        Some(Port { name: port_name, kind: pk })
                    };
                    DepKind::Local(port)
                },
                0x01 => {
                    // Remote: full RemoteDep wire encoding is not defined in v1.
                    return Err(DispatchError::RemoteDepNotSupported);
                },
                b => return Err(DispatchError::UnknownDepKind(b)),
            };

            Ok(IsaOp::EdgeConnect { id: edge_id, typ: "dep".into(), src, tgt, dep })
        },

        0x0003 => {
            // SetValue: node:u64, value:i8
            let node = wire_to_uid(r.read_u64()?);
            let val  = wire_to_t(r.read_i8()?)?;
            Ok(IsaOp::SetValue { node, val })
        },

        0x0004 => {
            // Propagate: node:u64
            Ok(IsaOp::Propagate { node: wire_to_uid(r.read_u64()?) })
        },

        0x0005 => {
            // Subscribe: source:u64, subscriber:u64
            let source     = wire_to_uid(r.read_u64()?);
            let subscriber = wire_to_uid(r.read_u64()?);
            Ok(IsaOp::Subscribe { source, subscriber })
        },

        // ── Inhibition (0x0006–0x0009) ────────────────────────────────────────

        0x0006 => {
            // DelNode: id:u64
            Ok(IsaOp::DelNode { id: wire_to_uid(r.read_u64()?) })
        },

        0x0007 => {
            // DelEdge: id:u64
            Ok(IsaOp::DelEdge { id: wire_to_uid(r.read_u64()?) })
        },

        0x0008 => {
            // Demand: node:u64
            Ok(IsaOp::Demand { node: wire_to_uid(r.read_u64()?) })
        },

        0x0009 => {
            // SetMode: node:u64, mode:u8 (0=Eager, 1=Lazy, 2=Stabilizing)
            let node = wire_to_uid(r.read_u64()?);
            let mode = match r.read_u8()? {
                0x00 => ExecMode::Eager,
                0x01 => ExecMode::Lazy,
                0x02 => ExecMode::Stabilizing,
                b    => return Err(DispatchError::UnknownExecMode(b)),
            };
            Ok(IsaOp::SetMode { node, mode })
        },

        // ── Reflection (0x000A–0x000B) ────────────────────────────────────────

        0x000A => {
            // Reflect: node:u64
            Ok(IsaOp::Reflect { node: wire_to_uid(r.read_u64()?) })
        },

        0x000B => {
            // Stabilize: count:u32, node_ids:u64[count]
            // count=0 → global stabilization (all Zero nodes in the partition).
            let count = r.read_u32()? as usize;
            let region = if count == 0 {
                None
            } else {
                let mut ids = Vec::with_capacity(count);
                for _ in 0..count {
                    ids.push(wire_to_uid(r.read_u64()?));
                }
                Some(ids)
            };
            Ok(IsaOp::Stabilize { region })
        },

        // ── Authority / partition (0x000C–0x0011) ────────────────────────────

        0x000C => {
            // PartitionCreate: id:u64, authority_root:u64, lattice_class:u64,
            //                  causal_domain_bits:u64
            let wire_id           = r.read_u64()?;
            let authority_root    = r.read_u64()?;
            let lattice_class     = r.read_u64()?;
            let causal_domain_bits = r.read_u64()?;
            Ok(IsaOp::PartitionCreate {
                id:             wire_to_partition_id(wire_id),
                authority_root: wire_to_partition_id(authority_root),
                lattice_class:  LatticeClass(lattice_class),
                causal_domain:  CausalScope {
                    scope_bits: causal_domain_bits,
                    root:       wire_to_partition_id(wire_id),
                },
            })
        },

        0x000D => {
            // PartitionBind: node:u64, partition:u64
            let node      = wire_to_uid(r.read_u64()?);
            let partition = wire_to_partition_id(r.read_u64()?);
            Ok(IsaOp::PartitionBind { node, partition })
        },

        0x000E => {
            // SetPartitionAuthority: partition:u64, lattice_class:u64,
            //                        causal_domain_bits:u64
            let partition          = r.read_u64()?;
            let lattice_class      = r.read_u64()?;
            let causal_domain_bits = r.read_u64()?;
            Ok(IsaOp::SetPartitionAuthority {
                partition:     wire_to_partition_id(partition),
                lattice_class: LatticeClass(lattice_class),
                causal_domain: CausalScope {
                    scope_bits: causal_domain_bits,
                    root:       PartitionId::nil(),
                },
            })
        },

        0x000F => {
            // SetEdgeLabel: src:u64, tgt:u64, lattice_class:u64,
            //               capability_bits:u8, causal_scope_bits:u64
            let src               = wire_to_uid(r.read_u64()?);
            let tgt               = wire_to_uid(r.read_u64()?);
            let lattice_class     = r.read_u64()?;
            let capability_bits   = r.read_u8()? as u64;
            let causal_scope_bits = r.read_u64()?;
            Ok(IsaOp::SetEdgeLabel {
                source: src,
                target: tgt,
                label: EdgeLabel {
                    capability:      CapabilityBits(capability_bits),
                    lattice_class:   LatticeClass(lattice_class),
                    projection_mask: pgress_core::delta::ProjectionMask::wildcard(),
                    causal_scope:    CausalScope {
                        scope_bits: causal_scope_bits,
                        root:       PartitionId::nil(),
                    },
                },
            })
        },

        0x0010 => {
            // SetStabilizationConfig: node:u64, strategy:u8, domain:u8,
            //                         max_iterations:u32, convergence_policy:u8
            let node           = wire_to_uid(r.read_u64()?);
            let strategy_byte  = r.read_u8()?;
            let domain_byte    = r.read_u8()?;
            let max_iterations = r.read_u32()? as usize;
            let conv_byte      = r.read_u8()?;

            let strategy = match strategy_byte {
                0x00 => RewriteStrategy::FullL3,
                b    => RewriteStrategy::Restricted(b as u64),
            };
            let domain = match domain_byte {
                0x00 => StabilizationDomain::LocalOnly,
                0x01 => StabilizationDomain::Federated,
                _    => StabilizationDomain::Inherit,
            };
            let convergence_policy = match conv_byte {
                0x00 => ConvergencePolicy::Canonical,
                0x01 => ConvergencePolicy::Witness,
                _    => ConvergencePolicy::BudgetExhausted,
            };
            Ok(IsaOp::SetStabilizationConfig {
                node,
                config: StabilizationConfig {
                    strategy,
                    domain,
                    budget: RewriteBudget { max_iterations },
                    convergence_policy,
                },
            })
        },

        0x0011 => {
            // SetExecutionPolicy: node:u64, queue_priority:u8, retry_policy:u8
            let node          = wire_to_uid(r.read_u64()?);
            let priority_byte = r.read_u8()?;
            let retry_byte    = r.read_u8()?;

            let queue_priority = match priority_byte {
                0x01 => QueuePriority::High,
                0x02 => QueuePriority::Low,
                _    => QueuePriority::Normal,
            };
            let retry_policy = match retry_byte {
                0x01 => RetryPolicy::Retry { max_attempts: 3 },
                _    => RetryPolicy::Fail,
            };
            Ok(IsaOp::SetExecutionPolicy {
                node,
                policy: ExecutionPolicy { queue_priority, retry_policy },
            })
        },

        0x0012 => {
            // RegionDeclare: root:u64, boundary_tag:u8, <boundary>, stability:u8, compile:u8
            let root         = wire_to_uid(r.read_u64()?);
            let boundary_tag = r.read_u8()?;
            let boundary = match boundary_tag {
                0x00 => {
                    // DepClosure: max_depth:u32
                    let max_depth = r.read_u32()? as usize;
                    RegionBoundary::DepClosure { max_depth }
                },
                0x01 => {
                    // ExplicitSet: count:u32, node_ids:u64[count]
                    let count = r.read_u32()? as usize;
                    let mut ids = Vec::with_capacity(count);
                    for _ in 0..count {
                        ids.push(wire_to_uid(r.read_u64()?));
                    }
                    RegionBoundary::ExplicitSet(ids)
                },
                0x02 => {
                    // PartitionScoped: partition_id:u64
                    let pid = wire_to_partition_id(r.read_u64()?);
                    RegionBoundary::PartitionScoped(pid)
                },
                b => return Err(DispatchError::UnknownRegionBoundaryTag(b)),
            };
            let stability = match r.read_u8()? {
                0x00 => StabilityContract::Pinned,
                0x01 => StabilityContract::EpochTracked,
                b    => return Err(DispatchError::UnknownStabilityContract(b)),
            };
            let compile = match r.read_u8()? {
                0x00 => CompilePolicy::Eager,
                0x01 => CompilePolicy::Lazy,
                0x02 => CompilePolicy::Never,
                b    => return Err(DispatchError::UnknownCompilePolicy(b)),
            };
            Ok(IsaOp::RegionDeclare { root, boundary, stability, compile })
        },

        other => Err(DispatchError::UnknownOpcode(other)),
    }
}

// ── DispatchError ─────────────────────────────────────────────────────────────

/// Error from the dispatch pipeline.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("ingress decode error: {0}")]
    Ingress(#[from] IngressError),

    #[error("engine error: {0}")]
    Engine(#[from] EngineError),

    #[error("payload truncated: needed {needed} bytes but only {available} remain")]
    PayloadTruncated { needed: usize, available: usize },

    #[error("invalid UTF-8 in payload string field")]
    InvalidUtf8,

    #[error("invalid ternary wire value {0}: must be −1, 0, or 1")]
    InvalidTernaryValue(i8),

    #[error("unknown opcode 0x{0:04x} in parse_payload; should have been routed to SkipPayload")]
    UnknownOpcode(u16),

    #[error("unknown node rule byte 0x{0:02x}")]
    UnknownNodeRule(u8),

    #[error("unknown port kind byte 0x{0:02x}")]
    UnknownPortKind(u8),

    #[error("unknown dep kind byte 0x{0:02x}")]
    UnknownDepKind(u8),

    #[error("unknown exec mode byte 0x{0:02x}")]
    UnknownExecMode(u8),

    #[error("unsupported attr value tag byte 0x{0:02x} (no Val variant)")]
    UnsupportedAttrTag(u8),

    #[error("remote dep (dep_kind=1) is not supported in v1; full wire encoding is a future ABI revision")]
    RemoteDepNotSupported,

    #[error("unknown region boundary tag byte 0x{0:02x}")]
    UnknownRegionBoundaryTag(u8),

    #[error("unknown stability contract byte 0x{0:02x}")]
    UnknownStabilityContract(u8),

    #[error("unknown compile policy byte 0x{0:02x}")]
    UnknownCompilePolicy(u8),
}

// ── DispatchOutcome ───────────────────────────────────────────────────────────

/// What the `Dispatcher` did with a record.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// Record was admitted by the session manager and applied to an engine shard.
    Applied {
        shard_id: ShardId,
        events:   Vec<PropEvent>,
    },
    /// Record was a `SessionProfile` (`0x0100`) and the session was bootstrapped
    /// or its capabilities were updated.
    Bootstrapped { session_id: SessionId },
    /// Record had an `Unknown` opcode; payload was skipped per the `length` field.
    Skipped,
    /// Record was rejected by the session manager gate.
    Rejected(RejectionReason),
}

// ── EnginePool ────────────────────────────────────────────────────────────────

/// `ShardId → Engine` map.
///
/// Owns one `pgress_core::Engine` per shard. Engines are created on first
/// access with default settings (`Advisory` authority mode, default max steps).
/// Calling code may configure specific shard engines via `get_or_insert` before
/// the first record arrives on that shard.
#[derive(Default)]
pub struct EnginePool {
    engines: FxHashMap<ShardId, Engine>,
}

impl EnginePool {
    pub fn new() -> Self { Self::default() }

    /// Return (or create) the engine for `shard_id`.
    pub fn get_or_insert(&mut self, shard_id: ShardId) -> &mut Engine {
        self.engines.entry(shard_id).or_insert_with(Engine::new)
    }

    /// Apply an `IsaOp` to the engine for `shard_id` (creating it if absent).
    ///
    /// Returns the `PropEvent`s produced during this apply cycle.
    pub fn apply(
        &mut self,
        shard_id: ShardId,
        op:       IsaOp,
    ) -> Result<Vec<PropEvent>, DispatchError> {
        self.get_or_insert(shard_id)
            .apply(op)
            .map_err(DispatchError::Engine)
    }

    /// Snapshot current `ShardPressure` for `shard_id`.
    ///
    /// Derived from `PropStats`:
    /// - `queue_depth` ← `stats.max_queue_depth` (cast to u32)
    /// - `cursor_count` ← 0 (no `WorkCursor` in v1)
    /// - `budget_consumed` ← `stats.nodes_materialized`
    pub fn pressure_snapshot(&self, shard_id: ShardId) -> ShardPressure {
        self.engines
            .get(&shard_id)
            .map(|e| {
                let s = e.stats();
                ShardPressure {
                    queue_depth:     s.max_queue_depth as u32,
                    cursor_count:    0,
                    budget_consumed: s.nodes_materialized,
                }
            })
            .unwrap_or_default()
    }

    /// Number of shards currently in the pool.
    pub fn shard_count(&self) -> usize { self.engines.len() }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

/// Full end-to-end pipeline driver.
///
/// The caller is responsible for byte framing:
/// - Call `process_preamble` with exactly 32 bytes.
/// - For each record: read the 48-byte `IsaHeader`, extract `length` from
///   bytes `[4..8]` (little-endian `u32`), read `length − 48` more bytes,
///   then call `process_record` with the 48-byte slice and the payload slice.
pub struct Dispatcher {
    pub decoder: StreamDecoder,
    pub runtime: SessionRuntime,
    pub shards:  EnginePool,
}

impl Dispatcher {
    pub fn new() -> Self {
        Dispatcher {
            decoder: StreamDecoder::new(),
            runtime: SessionRuntime::new(),
            shards:  EnginePool::new(),
        }
    }

    /// Parse the 32-byte `IsaStreamHeader` preamble.
    ///
    /// Must be called once before any `process_record` calls. For remote
    /// Advisory bootstrap the first record after the preamble should be a
    /// `SessionProfile` (`0x0100`) to create the session.
    pub fn process_preamble(
        &mut self,
        buf: &[u8; 32],
    ) -> Result<&StreamPreamble, DispatchError> {
        self.decoder.decode_preamble(buf).map_err(DispatchError::Ingress)
    }

    /// Process one complete record.
    ///
    /// - `header_buf` — the 48-byte `IsaHeader` slice.
    /// - `payload`    — exactly `header.length − 48` bytes.
    ///
    /// Returns a `DispatchOutcome` describing what was done. On error the stream
    /// is in an indeterminate state and the caller should drop the connection.
    pub fn process_record(
        &mut self,
        header_buf: &[u8; 48],
        payload:    &[u8],
    ) -> Result<DispatchOutcome, DispatchError> {
        let action = self.decoder.decode_record(header_buf, &self.runtime)?;

        match action {
            RecordAction::SkipPayload { .. } => Ok(DispatchOutcome::Skipped),

            RecordAction::ReadProfile { header, .. } => {
                self.handle_profile(&header, payload)
            },

            RecordAction::RouteReady(header) => {
                match self.runtime.route_record(&header) {
                    RouteOutcome::Rejected(r) => Ok(DispatchOutcome::Rejected(r)),
                    RouteOutcome::Admitted { opcode_class } => {
                        self.handle_admitted(&header, opcode_class, payload)
                    },
                }
            },
        }
    }

    // ── Profile / bootstrap ───────────────────────────────────────────────────

    fn handle_profile(
        &mut self,
        header:  &ParsedHeader,
        payload: &[u8],
    ) -> Result<DispatchOutcome, DispatchError> {
        let new_path_id = self.decoder.preamble()
            .map(|p| p.path_id)
            .unwrap_or(PathId(0));

        if self.runtime.sessions.get(header.session_id).is_none() {
            // ── First bootstrap: session does not exist yet ───────────────────
            // Create session + path + DomainRegistry entry atomically before
            // calling process_profile (which requires the session to be present
            // in both SessionTable and DomainRegistry).
            //
            // Bootstrap under the single-tenant sentinel (TenantId(0)). If the
            // actual tenant should differ, the host should pre-register the
            // session via local bootstrap or register tenants before accepting
            // remote streams.
            let tenant_id = TenantId(0);
            let auth_mode = pgress_core::partition::AuthorityMode::Advisory;

            self.runtime.sessions.create(SessionEntry {
                session_id:       header.session_id,
                tenant_id,
                active_path_id:   Some(new_path_id),
                prev_path_id:     None,
                stream_seq_floor: header.stream_seq,
                auth_mode,
            });
            self.runtime.paths.create(PathEntry {
                path_id:             new_path_id,
                session_id:          header.session_id,
                last_ack_stream_seq: header.stream_seq,
                state:               PathState::Active,
            });

            // Mirror into DomainRegistry so process_profile can locate it.
            self.runtime.domains.register_session(SessionDomain {
                id:           header.session_id,
                tenant_id,
                claimed_caps: CapabilityBits::NONE,
                auth_mode,
                quota:        SessionQuota::default(),
            });
        } else {
            // ── Re-bootstrap: session exists, new path arriving ───────────────
            // Reconnect scenario (QUIC-style path migration). The session_id is
            // stable but the transport path has changed. Migrate to the new path:
            //   - marks old path as Draining,
            //   - registers new_path_id as Active,
            //   - updates session.active_path_id.
            // stream_seq is session-scoped and continues from where it left off;
            // stream_seq_floor is NOT reset here.
            //
            // If new_path_id is the same as the current active_path_id (duplicate
            // SessionProfile on the same path), migrate_path is a no-op with respect
            // to gap detection — the old path entry is simply re-marked Active.
            let last_ack = self.runtime.sessions
                .get(header.session_id)
                .and_then(|s| s.active_path_id)
                .and_then(|pid| self.runtime.paths.get(pid))
                .map(|p| p.last_ack_stream_seq)
                .unwrap_or(0);

            let _ = self.runtime.sessions.migrate_path(
                header.session_id,
                new_path_id,
                last_ack,
                &mut self.runtime.paths,
            );
        }

        self.decoder.process_profile(header, payload, &mut self.runtime)?;
        Ok(DispatchOutcome::Bootstrapped { session_id: header.session_id })
    }

    // ── Admitted record dispatch ──────────────────────────────────────────────

    fn handle_admitted(
        &mut self,
        header:       &ParsedHeader,
        opcode_class: OpcodeClass,
        payload:      &[u8],
    ) -> Result<DispatchOutcome, DispatchError> {
        // Resolve shard: registered partition → its shard; unregistered Control
        // ops use the bootstrap shard (ShardId(0)). Data ops on unregistered
        // partitions were already rejected by route_record with UnknownPartition.
        let shard_id = self.runtime.domains.partitions
            .get(&header.partition_id)
            .map(|p| p.shard_id)
            .unwrap_or(ShardId(0));

        // Deserialise the record body.
        let op = parse_payload(header.opcode, payload)?;

        // Apply to the engine shard.
        let events = self.shards.apply(shard_id, op.clone())?;

        // Update shard pressure so the session runtime's topology and admission
        // control reflect current load.
        let pressure = self.shards.pressure_snapshot(shard_id);
        self.runtime.update_pressure(shard_id, pressure);

        // Feedback: when control ops mutate partition structure, propagate the
        // change back into the session runtime's gate tables.
        if let OpcodeClass::Control = opcode_class {
            self.feedback_control_op(header, &op);
        }

        Ok(DispatchOutcome::Applied { shard_id, events })
    }

    /// Propagate control-op outcomes back into the session runtime.
    ///
    /// `PartitionCreate`: register new partition in `DomainRegistry.partitions`
    /// (using `ShardId(0)` in v1) and install permissive auth rows in
    /// `PartitionAuthTable` so subsequent data ops can pass the gate.
    ///
    /// `SetPartitionAuthority`: update the `lattice_class` / `causal_scope` in
    /// the existing partition domain entry.
    fn feedback_control_op(&mut self, header: &ParsedHeader, op: &IsaOp) {
        match op {
            IsaOp::PartitionCreate { id, lattice_class, causal_domain, .. } => {
                // Recover the wire partition ID from the UUID's low 64 bits.
                let wire_id = WirePartitionId(id.as_u128() as u64);

                // Register partition → shard mapping.
                self.runtime.domains.register_partition(PartitionDomain {
                    id:            wire_id,
                    session_id:    header.session_id,
                    lattice_class: *lattice_class,
                    causal_scope:  causal_domain.scope_bits,
                    shard_id:      ShardId(0),
                });

                // Install permissive auth rows for every opcode class so that
                // subsequent data ops on this partition pass the gate. A host
                // that wants tighter lattice enforcement should call
                // `runtime.auth.install(…)` with a restrictive row after creation.
                for class in [
                    OpcodeClass::Control,
                    OpcodeClass::SetValue,
                    OpcodeClass::Propagate,
                    OpcodeClass::Demand,
                    OpcodeClass::Stabilize,
                    OpcodeClass::Data,
                    OpcodeClass::Subscribe,
                ] {
                    self.runtime.auth.install(
                        AuthTableKey { partition_id: wire_id, opcode_class: class },
                        PartitionAuthRow::permissive(),
                    );
                }
            },

            IsaOp::SetPartitionAuthority { partition, lattice_class, causal_domain } => {
                let wire_id = WirePartitionId(partition.as_u128() as u64);
                if let Some(p) = self.runtime.domains.partitions.get_mut(&wire_id) {
                    p.lattice_class = *lattice_class;
                    p.causal_scope  = causal_domain.scope_bits;
                }
            },

            _ => {}
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self { Self::new() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_payload ─────────────────────────────────────────────────────────

    fn le_u64(v: u64) -> [u8; 8] { v.to_le_bytes() }
    fn le_u32(v: u32) -> [u8; 4] { v.to_le_bytes() }
    fn le_u16(v: u16) -> [u8; 2] { v.to_le_bytes() }

    #[test]
    fn set_value_pos() {
        // payload: node=42, value=+1 (Pos)
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(42));
        p.push(1i8 as u8);
        let op = parse_payload(0x0003, &p).unwrap();
        assert!(matches!(op, IsaOp::SetValue { val: T::Pos, .. }));
    }

    #[test]
    fn set_value_neg() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1));
        p.push((-1i8) as u8);
        let op = parse_payload(0x0003, &p).unwrap();
        assert!(matches!(op, IsaOp::SetValue { val: T::Neg, .. }));
    }

    #[test]
    fn set_value_zero() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(7));
        p.push(0u8);
        let op = parse_payload(0x0003, &p).unwrap();
        assert!(matches!(op, IsaOp::SetValue { val: T::Zero, .. }));
    }

    #[test]
    fn set_value_invalid_ternary() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1));
        p.push(2u8); // 2 is not valid
        assert!(matches!(
            parse_payload(0x0003, &p),
            Err(DispatchError::InvalidTernaryValue(2))
        ));
    }

    #[test]
    fn propagate_roundtrip() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(99));
        let op = parse_payload(0x0004, &p).unwrap();
        assert!(matches!(op, IsaOp::Propagate { .. }));
    }

    #[test]
    fn demand_roundtrip() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(55));
        let op = parse_payload(0x0008, &p).unwrap();
        assert!(matches!(op, IsaOp::Demand { .. }));
    }

    #[test]
    fn reflect_roundtrip() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(11));
        let op = parse_payload(0x000A, &p).unwrap();
        assert!(matches!(op, IsaOp::Reflect { .. }));
    }

    #[test]
    fn stabilize_global() {
        // count=0 → global (None region)
        let mut p = Vec::new();
        p.extend_from_slice(&le_u32(0));
        let op = parse_payload(0x000B, &p).unwrap();
        assert!(matches!(op, IsaOp::Stabilize { region: None }));
    }

    #[test]
    fn stabilize_with_region() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u32(2));
        p.extend_from_slice(&le_u64(10));
        p.extend_from_slice(&le_u64(20));
        let op = parse_payload(0x000B, &p).unwrap();
        if let IsaOp::Stabilize { region: Some(ids) } = op {
            assert_eq!(ids.len(), 2);
        } else {
            panic!("expected Stabilize with Some region");
        }
    }

    #[test]
    fn node_create_input() {
        let typ = b"my-node";
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(5));              // uid
        p.extend_from_slice(&le_u16(typ.len() as u16)); // typ_len
        p.extend_from_slice(typ);                     // typ
        p.push(0x00);                                 // rule = Input
        p.extend_from_slice(&le_u16(0));              // attr count = 0
        let op = parse_payload(0x0001, &p).unwrap();
        assert!(matches!(op, IsaOp::NodeCreate { rule: NodeKind::Input, .. }));
    }

    #[test]
    fn node_create_computed_identity() {
        let typ = b"c";
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(7));
        p.extend_from_slice(&le_u16(1));
        p.extend_from_slice(typ);
        p.push(0x01);  // Identity
        p.extend_from_slice(&le_u16(0));
        let op = parse_payload(0x0001, &p).unwrap();
        assert!(matches!(op, IsaOp::NodeCreate {
            rule: NodeKind::Computed(ComputeRule::Identity), ..
        }));
    }

    #[test]
    fn edge_connect_local_named_port() {
        let port = b"x";
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1)); // edge_id
        p.extend_from_slice(&le_u64(2)); // src
        p.extend_from_slice(&le_u64(3)); // tgt
        p.push(0x00);                    // dep_kind = Local
        p.push(0x00);                    // port_kind = Signal
        p.extend_from_slice(&le_u16(port.len() as u16));
        p.extend_from_slice(port);
        let op = parse_payload(0x0002, &p).unwrap();
        assert!(matches!(op, IsaOp::EdgeConnect { dep: DepKind::Local(Some(_)), .. }));
    }

    #[test]
    fn edge_connect_local_no_port() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1));
        p.extend_from_slice(&le_u64(2));
        p.extend_from_slice(&le_u64(3));
        p.push(0x00); // Local
        p.push(0x00); // Signal
        p.extend_from_slice(&le_u16(0)); // empty port name
        let op = parse_payload(0x0002, &p).unwrap();
        assert!(matches!(op, IsaOp::EdgeConnect { dep: DepKind::Local(None), .. }));
    }

    #[test]
    fn edge_connect_remote_dep_rejected() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1));
        p.extend_from_slice(&le_u64(2));
        p.extend_from_slice(&le_u64(3));
        p.push(0x01); // Remote
        p.push(0x00);
        p.extend_from_slice(&le_u16(0));
        assert!(matches!(
            parse_payload(0x0002, &p),
            Err(DispatchError::RemoteDepNotSupported)
        ));
    }

    #[test]
    fn partition_create_roundtrip() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(42));  // id
        p.extend_from_slice(&le_u64(42));  // authority_root = same as id
        p.extend_from_slice(&le_u64(0));   // lattice_class = BOTTOM
        p.extend_from_slice(&le_u64(u64::MAX)); // causal_domain = UNIVERSAL
        let op = parse_payload(0x000C, &p).unwrap();
        assert!(matches!(op, IsaOp::PartitionCreate { .. }));
    }

    #[test]
    fn set_edge_label_roundtrip() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(10)); // src
        p.extend_from_slice(&le_u64(20)); // tgt
        p.extend_from_slice(&le_u64(0));  // lattice_class = BOTTOM
        p.push(0x07);                     // capability_bits = READ|PROPAGATE|STABILIZE
        p.extend_from_slice(&le_u64(u64::MAX)); // causal_scope = UNIVERSAL
        let op = parse_payload(0x000F, &p).unwrap();
        if let IsaOp::SetEdgeLabel { label, .. } = op {
            assert_eq!(label.capability, CapabilityBits(0x07));
            assert_eq!(label.lattice_class, LatticeClass::BOTTOM);
        } else {
            panic!("expected SetEdgeLabel");
        }
    }

    #[test]
    fn set_mode_lazy() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(3));
        p.push(0x01); // Lazy
        let op = parse_payload(0x0009, &p).unwrap();
        assert!(matches!(op, IsaOp::SetMode { mode: ExecMode::Lazy, .. }));
    }

    #[test]
    fn set_stabilization_config() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(5));    // node
        p.push(0x00);                       // strategy = FullL3
        p.push(0x02);                       // domain = Inherit
        p.extend_from_slice(&le_u32(128));  // max_iterations
        p.push(0x00);                       // convergence_policy = Canonical
        let op = parse_payload(0x0010, &p).unwrap();
        if let IsaOp::SetStabilizationConfig { config, .. } = op {
            assert!(matches!(config.strategy, RewriteStrategy::FullL3));
            assert_eq!(config.budget.max_iterations, 128);
        } else {
            panic!("expected SetStabilizationConfig");
        }
    }

    #[test]
    fn set_execution_policy_high_priority() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(8));
        p.push(0x01); // High
        p.push(0x00); // Fail
        let op = parse_payload(0x0011, &p).unwrap();
        if let IsaOp::SetExecutionPolicy { policy, .. } = op {
            assert_eq!(policy.queue_priority, QueuePriority::High);
        } else {
            panic!("expected SetExecutionPolicy");
        }
    }

    #[test]
    fn attr_map_with_bool_and_ternary() {
        // NodeCreate with two attrs: "flag"=true (0x01), "t"=Pos (0x06)
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1)); // uid
        p.extend_from_slice(&le_u16(1)); // typ_len
        p.push(b'n');                    // typ = "n"
        p.push(0x00);                    // rule = Input

        // AttrMap: count=2
        p.extend_from_slice(&le_u16(2));
        // entry 1: key="flag", tag=Bool, value=true
        p.extend_from_slice(&le_u16(4));
        p.extend_from_slice(b"flag");
        p.push(0x01); // Bool
        p.push(0x01); // true
        // entry 2: key="t", tag=Ternary, value=+1
        p.extend_from_slice(&le_u16(1));
        p.extend_from_slice(b"t");
        p.push(0x06); // Ternary
        p.push(1u8);  // Pos

        let op = parse_payload(0x0001, &p).unwrap();
        if let IsaOp::NodeCreate { attrs, .. } = op {
            assert_eq!(attrs.get("flag"), Some(&Val::Bool(true)));
            assert_eq!(attrs.get("t"), Some(&Val::Ternary(T::Pos)));
        } else {
            panic!("expected NodeCreate");
        }
    }

    #[test]
    fn region_declare_dep_closure() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1));   // root uid
        p.push(0x00);                      // boundary_tag = DepClosure
        p.extend_from_slice(&le_u32(3));   // max_depth = 3
        p.push(0x01);                      // stability = EpochTracked
        p.push(0x00);                      // compile = Eager
        let op = parse_payload(0x0012, &p).unwrap();
        assert!(matches!(op, IsaOp::RegionDeclare {
            boundary: RegionBoundary::DepClosure { max_depth: 3 },
            stability: StabilityContract::EpochTracked,
            compile: CompilePolicy::Eager,
            ..
        }));
    }

    #[test]
    fn region_declare_explicit_set() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(10));  // root
        p.push(0x01);                      // boundary_tag = ExplicitSet
        p.extend_from_slice(&le_u32(2));   // count = 2
        p.extend_from_slice(&le_u64(10));  // node 1
        p.extend_from_slice(&le_u64(20));  // node 2
        p.push(0x00);                      // stability = Pinned
        p.push(0x02);                      // compile = Never
        let op = parse_payload(0x0012, &p).unwrap();
        if let IsaOp::RegionDeclare { boundary: RegionBoundary::ExplicitSet(ids), stability, compile, .. } = op {
            assert_eq!(ids.len(), 2);
            assert_eq!(stability, StabilityContract::Pinned);
            assert_eq!(compile, CompilePolicy::Never);
        } else {
            panic!("expected RegionDeclare with ExplicitSet");
        }
    }

    #[test]
    fn region_declare_unknown_boundary_tag_errors() {
        let mut p = Vec::new();
        p.extend_from_slice(&le_u64(1));
        p.push(0xFF); // unknown tag
        assert!(matches!(
            parse_payload(0x0012, &p),
            Err(DispatchError::UnknownRegionBoundaryTag(0xFF))
        ));
    }

    #[test]
    fn truncated_payload_returns_error() {
        // SetValue needs 9 bytes (8 for uid + 1 for i8), give only 3.
        let p = [0u8; 3];
        assert!(matches!(
            parse_payload(0x0003, &p),
            Err(DispatchError::PayloadTruncated { .. })
        ));
    }

    // ── EnginePool ────────────────────────────────────────────────────────────

    #[test]
    fn engine_pool_creates_on_demand() {
        let mut pool = EnginePool::new();
        assert_eq!(pool.shard_count(), 0);
        pool.get_or_insert(ShardId(0));
        assert_eq!(pool.shard_count(), 1);
        pool.get_or_insert(ShardId(0)); // same shard, no new creation
        assert_eq!(pool.shard_count(), 1);
        pool.get_or_insert(ShardId(1));
        assert_eq!(pool.shard_count(), 2);
    }

    #[test]
    fn engine_pool_apply_node_create() {
        let mut pool = EnginePool::new();

        // Build a NodeCreate payload manually: uid=1, typ="n", rule=Input, attrs=empty
        let uid: u64 = 1;
        let op = IsaOp::input_node(wire_to_uid(uid), "n");
        let events = pool.apply(ShardId(0), op).unwrap();
        // NodeCreate doesn't produce PropEvents (structural change only)
        assert!(events.is_empty());

        // SetValue produces a ValueChanged event
        let op2 = IsaOp::SetValue { node: wire_to_uid(uid), val: T::Pos };
        let events2 = pool.apply(ShardId(0), op2).unwrap();
        assert!(!events2.is_empty());
    }

    #[test]
    fn engine_pool_pressure_snapshot_reflects_stats() {
        let mut pool = EnginePool::new();
        let uid = wire_to_uid(1);
        pool.apply(ShardId(0), IsaOp::input_node(uid, "n")).unwrap();
        pool.apply(ShardId(0), IsaOp::SetValue { node: uid, val: T::Pos }).unwrap();

        let p = pool.pressure_snapshot(ShardId(0));
        // After a SetValue with no subscribers, nothing is materialised
        // but the shard should have been touched (queue_depth may be 0).
        assert_eq!(p.cursor_count, 0); // always 0 in v1
    }

    // ── Dispatcher end-to-end ─────────────────────────────────────────────────

    /// Build a valid 32-byte IsaStreamHeader preamble.
    fn make_preamble(path_id: u64, session_id: u64) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(b"PGRS");          // magic
        buf[4..6].copy_from_slice(&0x0003u16.to_le_bytes()); // abi_version
        // flags, feature_flags = 0
        buf[16..24].copy_from_slice(&path_id.to_le_bytes());
        buf[24..32].copy_from_slice(&session_id.to_le_bytes());
        buf
    }

    /// Build a 48-byte IsaHeader.
    fn make_header(
        opcode:       u16,
        length:       u32,   // total record length (header + payload)
        tenant_id:    u64,
        session_id:   u64,
        partition_id: u64,
        causal_epoch: u64,
        stream_seq:   u64,
    ) -> [u8; 48] {
        let mut buf = [0u8; 48];
        buf[0..2].copy_from_slice(&opcode.to_le_bytes());
        buf[4..8].copy_from_slice(&length.to_le_bytes());
        buf[8..16].copy_from_slice(&tenant_id.to_le_bytes());
        buf[16..24].copy_from_slice(&session_id.to_le_bytes());
        buf[24..32].copy_from_slice(&partition_id.to_le_bytes());
        buf[32..40].copy_from_slice(&causal_epoch.to_le_bytes());
        buf[40..48].copy_from_slice(&stream_seq.to_le_bytes());
        buf
    }

    fn setup_dispatcher_with_session() -> (Dispatcher, u64, u64, u64) {
        use pgress_core::partition::{AuthorityMode, CapabilityBits};
        use crate::{
            auth::{AuthTableKey, PartitionAuthRow},
            domain::{PartitionDomain, TenantDomain, TenantQuota},
            session::{PathEntry, PathState, SessionEntry},
        };

        let session_id: u64 = 1;
        let path_id:    u64 = 100;
        let partition_id: u64 = 99;

        let mut d = Dispatcher::new();

        // Local bootstrap: register tenant, session, path, partition.
        d.runtime.domains.register_tenant(TenantDomain {
            id:           TenantId(0),
            capabilities: CapabilityBits::ALL,
            quota:        TenantQuota::default(),
        });
        d.runtime.domains.register_session(SessionDomain {
            id:           SessionId(session_id),
            tenant_id:    TenantId(0),
            claimed_caps: CapabilityBits::ALL,
            auth_mode:    AuthorityMode::Advisory,
            quota:        SessionQuota::default(),
        });
        d.runtime.sessions.create(SessionEntry {
            session_id:       SessionId(session_id),
            tenant_id:        TenantId(0),
            active_path_id:   Some(PathId(path_id)),
            prev_path_id:     None,
            stream_seq_floor: 0,
            auth_mode:        AuthorityMode::Advisory,
        });
        d.runtime.paths.create(PathEntry {
            path_id:             PathId(path_id),
            session_id:          SessionId(session_id),
            last_ack_stream_seq: 0,
            state:               PathState::Active,
        });
        d.runtime.domains.register_partition(PartitionDomain {
            id:            WirePartitionId(partition_id),
            session_id:    SessionId(session_id),
            lattice_class: LatticeClass::BOTTOM,
            causal_scope:  u64::MAX,
            shard_id:      ShardId(0),
        });
        for class in [
            OpcodeClass::Control, OpcodeClass::SetValue, OpcodeClass::Propagate,
            OpcodeClass::Demand, OpcodeClass::Stabilize,
        ] {
            d.runtime.auth.install(
                AuthTableKey { partition_id: WirePartitionId(partition_id), opcode_class: class },
                PartitionAuthRow::permissive(),
            );
        }

        (d, session_id, path_id, partition_id)
    }

    #[test]
    fn dispatcher_end_to_end_set_value() {
        let (mut d, session_id, path_id, partition_id) = setup_dispatcher_with_session();

        // Preamble
        let preamble = make_preamble(path_id, session_id);
        d.process_preamble(&preamble).unwrap();

        // Create the node first (typ="" length=0), then set its value.
        let node_uid: u64 = 1;
        let mut nc_payload: Vec<u8> = Vec::new();
        nc_payload.extend_from_slice(&le_u64(node_uid));  // uid
        nc_payload.extend_from_slice(&le_u16(0));          // typ_len = 0
        nc_payload.push(0x00);                             // rule = Input
        nc_payload.extend_from_slice(&le_u16(0));          // attr count = 0
        let nc_hdr = make_header(
            0x0001,
            48 + nc_payload.len() as u32,
            0, session_id, partition_id, 1, 1,
        );
        let outcome = d.process_record(&nc_hdr, &nc_payload).unwrap();
        assert!(matches!(outcome, DispatchOutcome::Applied { .. }), "NodeCreate should be Applied");

        // SetValue: node=1, value=Pos
        let mut sv_payload: Vec<u8> = Vec::new();
        sv_payload.extend_from_slice(&le_u64(node_uid));
        sv_payload.push(1u8); // Pos
        let sv_hdr = make_header(
            0x0003,
            48 + sv_payload.len() as u32,
            0, session_id, partition_id, 2, 2,
        );
        let outcome2 = d.process_record(&sv_hdr, &sv_payload).unwrap();
        if let DispatchOutcome::Applied { shard_id, events } = outcome2 {
            assert_eq!(shard_id, ShardId(0));
            // SetValue on a node with no subscribers → ValueChanged event
            assert!(!events.is_empty(), "SetValue should produce at least one PropEvent");
        } else {
            panic!("expected Applied outcome, got: {:?}", outcome2);
        }
    }

    #[test]
    fn dispatcher_unknown_partition_data_op_rejected() {
        let (mut d, session_id, path_id, _) = setup_dispatcher_with_session();

        let preamble = make_preamble(path_id, session_id);
        d.process_preamble(&preamble).unwrap();

        // SetValue on an unregistered partition (partition_id=999)
        let mut sv_payload: Vec<u8> = Vec::new();
        sv_payload.extend_from_slice(&le_u64(1));
        sv_payload.push(1u8);
        let sv_hdr = make_header(
            0x0003, 48 + sv_payload.len() as u32,
            0, session_id,
            999, // unknown partition
            1, 1,
        );
        let outcome = d.process_record(&sv_hdr, &sv_payload).unwrap();
        assert!(matches!(
            outcome,
            DispatchOutcome::Rejected(RejectionReason::UnknownPartition)
        ));
    }

    #[test]
    fn dispatcher_partition_create_feedback() {
        // PartitionCreate on an unknown partition should succeed and register
        // the new partition in DomainRegistry.
        let (mut d, session_id, path_id, _existing_pid) = setup_dispatcher_with_session();

        let preamble = make_preamble(path_id, session_id);
        d.process_preamble(&preamble).unwrap();

        let new_pid: u64 = 200;

        // PartitionCreate payload: id, authority_root, lattice_class, causal_domain_bits
        let mut pc_payload = Vec::new();
        pc_payload.extend_from_slice(&le_u64(new_pid));    // id
        pc_payload.extend_from_slice(&le_u64(new_pid));    // authority_root = self
        pc_payload.extend_from_slice(&le_u64(0));          // lattice_class = BOTTOM
        pc_payload.extend_from_slice(&le_u64(u64::MAX));   // causal_domain = UNIVERSAL
        let pc_hdr = make_header(
            0x000C,
            48 + pc_payload.len() as u32,
            0, session_id,
            0, // header partition_id = 0 (bootstrap); the NEW id is in the payload
            1, 1,
        );

        let outcome = d.process_record(&pc_hdr, &pc_payload).unwrap();
        assert!(matches!(outcome, DispatchOutcome::Applied { .. }),
            "PartitionCreate should be Applied, got: {:?}", outcome);

        // After PartitionCreate, the new partition must be in DomainRegistry.
        assert!(
            d.runtime.domains.partitions.contains_key(&WirePartitionId(new_pid)),
            "new partition must be registered in DomainRegistry"
        );
    }
}
