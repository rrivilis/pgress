//! Wire encoder — symmetric counterpart to `dispatch::parse_payload`.
//!
//! Encodes `IsaOp` values to the binary ABI defined in `spec/abi.md`.
//!
//! Responsibilities:
//! - `encode_stream_preamble` — 32-byte `IsaStreamHeader`
//! - `write_payload`          — body bytes only (no header); symmetric to `parse_payload`
//! - `encode_record`          — 48-byte `IsaHeader` + payload, length field included
//!
//! Writer obligations from `spec/abi.md`:
//! - W1: all integers little-endian
//! - W2: `length` in `IsaHeader` counts the full record (header + payload)
//! - W3: strings are `u16`-length-prefixed UTF-8 byte counts
//! - W4: `AttrMap` keys written in lexicographic sort order
//! - W5: NaN f64 values canonicalised to quiet NaN `0x7FF8000000000000`
//! - W6: wire Uid `0` is invalid; callers must not pass null handles
//! - W7: no padding bytes between fields
//! - W8: undefined flag bits set to zero

use pgress_core::{
    attr::{Attrs, Val},
    isa::IsaOp,
    node::{
        ComputeRule, ConvergencePolicy, ExecMode, NodeKind, PortKind,
        QueuePriority, RetryPolicy, RewriteStrategy, StabilizationDomain,
    },
    partition::{DepKind, PartitionId},
    region::{CompilePolicy, RegionBoundary, StabilityContract},
    ternary::T,
    uid::Uid,
};
use crate::{PathId, SessionId, TenantId, WirePartitionId};

// ── ID wire conversions (inverse of dispatch wire_to_uid) ─────────────────────

/// Convert engine `Uid` back to the wire `u64` representation.
/// Inverse of `wire_to_uid` in dispatch.rs: `Uid::from_u128(id as u128)`.
#[inline]
pub fn uid_to_wire(id: Uid) -> u64 { id.as_u128() as u64 }

/// Convert engine `PartitionId` back to the wire `u64` representation.
#[inline]
pub fn partition_id_to_wire(id: PartitionId) -> u64 { id.as_u128() as u64 }

// ── T ↔ wire i8 ───────────────────────────────────────────────────────────────

#[inline]
pub fn t_to_wire(t: T) -> i8 {
    match t {
        T::Neg  => -1,
        T::Zero =>  0,
        T::Pos  =>  1,
    }
}

// ── Low-level push helpers (all little-endian) ────────────────────────────────

#[inline] fn push_u8(buf: &mut Vec<u8>, v: u8)  { buf.push(v); }
#[inline] fn push_i8(buf: &mut Vec<u8>, v: i8)  { buf.push(v as u8); }
#[inline] fn push_u16(buf: &mut Vec<u8>, v: u16) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline] fn push_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline] fn push_u64(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_le_bytes()); }
#[inline] fn push_i64(buf: &mut Vec<u8>, v: i64) { buf.extend_from_slice(&v.to_le_bytes()); }

/// W3: u16-length-prefixed UTF-8 byte-count.
#[inline]
fn push_str_u16(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    push_u16(buf, bytes.len() as u16);
    buf.extend_from_slice(bytes);
}

#[inline] fn push_uid(buf: &mut Vec<u8>, id: Uid)          { push_u64(buf, uid_to_wire(id)); }
#[inline] fn push_pid(buf: &mut Vec<u8>, id: PartitionId)  { push_u64(buf, partition_id_to_wire(id)); }

// ── AttrMap encoder ───────────────────────────────────────────────────────────

/// Encode an `AttrMap` (W4: keys written in lexicographic sort order).
fn write_attr_map(attrs: &Attrs, buf: &mut Vec<u8>) {
    let mut entries: Vec<(&str, &Val)> = attrs.iter().collect();
    // W4: lexicographic sort on key bytes
    entries.sort_unstable_by_key(|(k, _)| *k);

    push_u16(buf, entries.len() as u16);
    for (key, val) in entries {
        push_str_u16(buf, key);
        match val {
            Val::Bool(b)     => { push_u8(buf, 0x01); push_u8(buf, *b as u8); }
            Val::Int(i)      => { push_u8(buf, 0x02); push_i64(buf, *i); }
            Val::Float(f)    => {
                push_u8(buf, 0x03);
                // W5: canonicalise NaN → quiet NaN 0x7FF8000000000000
                let bits = if f.is_nan() { 0x7FF8_0000_0000_0000u64 } else { f.to_bits() };
                buf.extend_from_slice(&bits.to_le_bytes());
            }
            Val::Text(s)     => { push_u8(buf, 0x04); push_str_u16(buf, s); }
            Val::Ternary(t)  => { push_u8(buf, 0x06); push_i8(buf, t_to_wire(*t)); }
        }
    }
}

// ── Opcode discriminant ───────────────────────────────────────────────────────

/// Return the 16-bit opcode discriminant for an `IsaOp`.
pub fn opcode_for(op: &IsaOp) -> u16 {
    match op {
        IsaOp::NodeCreate { .. }              => 0x0001,
        IsaOp::EdgeConnect { .. }             => 0x0002,
        IsaOp::SetValue { .. }                => 0x0003,
        IsaOp::Propagate { .. }               => 0x0004,
        IsaOp::Subscribe { .. }               => 0x0005,
        IsaOp::DelNode { .. }                 => 0x0006,
        IsaOp::DelEdge { .. }                 => 0x0007,
        IsaOp::Demand { .. }                  => 0x0008,
        IsaOp::SetMode { .. }                 => 0x0009,
        IsaOp::Reflect { .. }                 => 0x000A,
        IsaOp::Stabilize { .. }               => 0x000B,
        IsaOp::PartitionCreate { .. }         => 0x000C,
        IsaOp::PartitionBind { .. }           => 0x000D,
        IsaOp::SetPartitionAuthority { .. }   => 0x000E,
        IsaOp::SetEdgeLabel { .. }            => 0x000F,
        IsaOp::SetStabilizationConfig { .. }  => 0x0010,
        IsaOp::SetExecutionPolicy { .. }      => 0x0011,
        IsaOp::RegionDeclare { .. }           => 0x0012,
    }
}

// ── write_payload ─────────────────────────────────────────────────────────────

/// Serialise the payload body of an `IsaOp` into `buf` (no `IsaHeader`).
///
/// This is the symmetric complement to `dispatch::parse_payload`.
/// Callers that need the full 48-byte header + payload should use
/// `encode_record` instead.
pub fn write_payload(op: &IsaOp, buf: &mut Vec<u8>) {
    match op {

        // ── Extension (0x0001–0x0005) ─────────────────────────────────────────

        IsaOp::NodeCreate { id, typ, rule, attrs } => {
            push_uid(buf, *id);
            push_str_u16(buf, typ);
            let rule_byte: u8 = match rule {
                NodeKind::Input                           => 0x00,
                NodeKind::Computed(ComputeRule::Identity)    => 0x01,
                NodeKind::Computed(ComputeRule::MvNeg)       => 0x02,
                NodeKind::Computed(ComputeRule::MvAdd)       => 0x03,
                NodeKind::Computed(ComputeRule::MvMul)       => 0x04,
                NodeKind::Computed(ComputeRule::MvSub)       => 0x05,
                NodeKind::Computed(ComputeRule::Merge)       => 0x06,
                NodeKind::Computed(ComputeRule::MeetAll)     => 0x07,
                NodeKind::Computed(ComputeRule::JoinAny)     => 0x08,
                NodeKind::Computed(ComputeRule::BochvarFold) => 0x09,
                NodeKind::Computed(ComputeRule::PowerProduct) => 0x0A,
            };
            push_u8(buf, rule_byte);
            write_attr_map(attrs, buf);
        }

        IsaOp::EdgeConnect { id, src, tgt, dep, .. } => {
            push_uid(buf, *id);
            push_uid(buf, *src);
            push_uid(buf, *tgt);
            match dep {
                DepKind::Local(None) => {
                    push_u8(buf, 0x00);  // dep_kind = Local
                    push_u8(buf, 0x00);  // port_kind = Signal (ignored when port_name empty)
                    push_str_u16(buf, ""); // empty port_name → no port
                }
                DepKind::Local(Some(port)) => {
                    push_u8(buf, 0x00);  // dep_kind = Local
                    push_u8(buf, match port.kind {
                        PortKind::Signal => 0x00,
                        PortKind::Effort => 0x01,
                        PortKind::Flow   => 0x02,
                        PortKind::Bond   => 0x03,
                    });
                    push_str_u16(buf, &port.name);
                }
                DepKind::Remote(_) => {
                    // Remote dep wire encoding is not defined in v1 — emit
                    // dep_kind=0x01 with an empty port payload so the record is
                    // syntactically framed, even though the decoder will reject it.
                    push_u8(buf, 0x01);
                    push_u8(buf, 0x00);
                    push_str_u16(buf, "");
                }
            }
        }

        IsaOp::SetValue { node, val } => {
            push_uid(buf, *node);
            push_i8(buf, t_to_wire(*val));
        }

        IsaOp::Propagate { node } => push_uid(buf, *node),
        IsaOp::Subscribe { source, subscriber } => {
            push_uid(buf, *source);
            push_uid(buf, *subscriber);
        }

        // ── Inhibition (0x0006–0x0009) ────────────────────────────────────────

        IsaOp::DelNode { id } => push_uid(buf, *id),
        IsaOp::DelEdge { id } => push_uid(buf, *id),
        IsaOp::Demand  { node } => push_uid(buf, *node),

        IsaOp::SetMode { node, mode } => {
            push_uid(buf, *node);
            push_u8(buf, match mode {
                ExecMode::Eager       => 0x00,
                ExecMode::Lazy        => 0x01,
                ExecMode::Stabilizing => 0x02,
            });
        }

        // ── Reflection (0x000A–0x000B) ────────────────────────────────────────

        IsaOp::Reflect   { node }   => push_uid(buf, *node),

        IsaOp::Stabilize { region } => {
            match region {
                None => push_u32(buf, 0),
                Some(ids) => {
                    push_u32(buf, ids.len() as u32);
                    for &id in ids { push_uid(buf, id); }
                }
            }
        }

        // ── Authority / partition (0x000C–0x0011) ─────────────────────────────

        IsaOp::PartitionCreate { id, authority_root, lattice_class, causal_domain } => {
            push_pid(buf, *id);
            push_pid(buf, *authority_root);
            push_u64(buf, lattice_class.0);
            push_u64(buf, causal_domain.scope_bits);
        }

        IsaOp::PartitionBind { node, partition } => {
            push_uid(buf, *node);
            push_pid(buf, *partition);
        }

        IsaOp::SetPartitionAuthority { partition, lattice_class, causal_domain } => {
            push_pid(buf, *partition);
            push_u64(buf, lattice_class.0);
            push_u64(buf, causal_domain.scope_bits);
        }

        IsaOp::SetEdgeLabel { source, target, label } => {
            push_uid(buf, *source);
            push_uid(buf, *target);
            push_u64(buf, label.lattice_class.0);
            push_u8(buf, label.capability.0 as u8);
            push_u64(buf, label.causal_scope.scope_bits);
        }

        IsaOp::SetStabilizationConfig { node, config } => {
            push_uid(buf, *node);
            push_u8(buf, match &config.strategy {
                RewriteStrategy::FullL3          => 0x00,
                RewriteStrategy::Restricted(v)   => *v as u8,
            });
            push_u8(buf, match config.domain {
                StabilizationDomain::LocalOnly  => 0x00,
                StabilizationDomain::Federated  => 0x01,
                StabilizationDomain::Inherit    => 0x02,
            });
            push_u32(buf, config.budget.max_iterations as u32);
            push_u8(buf, match config.convergence_policy {
                ConvergencePolicy::Canonical       => 0x00,
                ConvergencePolicy::Witness         => 0x01,
                ConvergencePolicy::BudgetExhausted => 0x02,
            });
        }

        IsaOp::SetExecutionPolicy { node, policy } => {
            push_uid(buf, *node);
            push_u8(buf, match policy.queue_priority {
                QueuePriority::Normal => 0x00,
                QueuePriority::High   => 0x01,
                QueuePriority::Low    => 0x02,
            });
            push_u8(buf, match policy.retry_policy {
                RetryPolicy::Fail        => 0x00,
                RetryPolicy::Retry { .. } => 0x01,
            });
        }

        // ── Compilation hints (0x0012) ────────────────────────────────────────

        IsaOp::RegionDeclare { root, boundary, stability, compile } => {
            push_uid(buf, *root);
            match boundary {
                RegionBoundary::DepClosure { max_depth } => {
                    push_u8(buf, 0x00);
                    push_u32(buf, *max_depth as u32);
                }
                RegionBoundary::ExplicitSet(ids) => {
                    push_u8(buf, 0x01);
                    push_u32(buf, ids.len() as u32);
                    for &id in ids { push_uid(buf, id); }
                }
                RegionBoundary::PartitionScoped(pid) => {
                    push_u8(buf, 0x02);
                    push_pid(buf, *pid);
                }
            }
            push_u8(buf, match stability {
                StabilityContract::Pinned       => 0x00,
                StabilityContract::EpochTracked => 0x01,
            });
            push_u8(buf, match compile {
                CompilePolicy::Eager => 0x00,
                CompilePolicy::Lazy  => 0x01,
                CompilePolicy::Never => 0x02,
            });
        }
    }
}

// ── Full-record encoder ───────────────────────────────────────────────────────

/// Context fields that populate the `IsaHeader` of an encoded record.
///
/// The `length` field is computed automatically — do not set it here.
#[derive(Clone, Debug)]
pub struct RecordContext {
    /// Tenant owning this session.
    pub tenant_id:    TenantId,
    /// Session this record belongs to.
    pub session_id:   SessionId,
    /// Target partition (0 = default shard).
    pub partition_id: WirePartitionId,
    /// Causal epoch at time of emission.
    pub causal_epoch: u64,
    /// Strictly-increasing stream sequence number for this session.
    pub stream_seq:   u64,
    /// Flags word — set undefined bits to zero (W8).
    pub flags:        u16,
}

impl RecordContext {
    /// Minimal context for single-session in-process use (tenant=0, partition=0).
    pub fn simple(session_id: SessionId, stream_seq: u64) -> Self {
        RecordContext {
            tenant_id:    TenantId(0),
            session_id,
            partition_id: WirePartitionId(0),
            causal_epoch: 0,
            stream_seq,
            flags:        0,
        }
    }
}

/// Encode a complete `IsaHeader` (48 bytes) + payload for `op`.
///
/// The `length` field is set to `48 + payload_len`, satisfying the decoder
/// contract that `payload_bytes = header.length − 48`.
///
/// ```text
/// IsaHeader layout (all little-endian):
///   [0..2]   opcode:       u16
///   [2..4]   flags:        u16
///   [4..8]   length:       u32   ← header + payload, always >= 48
///   [8..16]  tenant_id:    u64
///   [16..24] session_id:   u64
///   [24..32] partition_id: u64
///   [32..40] causal_epoch: u64
///   [40..48] stream_seq:   u64
///   [48..]   payload bytes
/// ```
pub fn encode_record(op: &IsaOp, ctx: &RecordContext) -> Vec<u8> {
    let opcode = opcode_for(op);
    let mut payload = Vec::new();
    write_payload(op, &mut payload);

    let length = (48usize + payload.len()) as u32;
    let mut out = Vec::with_capacity(48 + payload.len());

    out.extend_from_slice(&opcode.to_le_bytes());
    out.extend_from_slice(&ctx.flags.to_le_bytes());
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&ctx.tenant_id.0.to_le_bytes());
    out.extend_from_slice(&ctx.session_id.0.to_le_bytes());
    out.extend_from_slice(&ctx.partition_id.0.to_le_bytes());
    out.extend_from_slice(&ctx.causal_epoch.to_le_bytes());
    out.extend_from_slice(&ctx.stream_seq.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Encode the 32-byte `IsaStreamHeader` preamble for a new stream.
///
/// ```text
/// IsaStreamHeader layout (all little-endian):
///   [0..4]   magic:         b"PGRS"
///   [4..6]   abi_version:   u16   = 0x0003
///   [6..8]   flags:         u16
///   [8..16]  feature_flags: u64
///   [16..24] path_id:       u64
///   [24..32] session_id:    u64
/// ```
pub fn encode_stream_preamble(
    path_id:       PathId,
    session_id:    SessionId,
    feature_flags: u64,
    flags:         u16,
) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0..4].copy_from_slice(b"PGRS");
    buf[4..6].copy_from_slice(&0x0003u16.to_le_bytes());
    buf[6..8].copy_from_slice(&flags.to_le_bytes());
    buf[8..16].copy_from_slice(&feature_flags.to_le_bytes());
    buf[16..24].copy_from_slice(&path_id.0.to_le_bytes());
    buf[24..32].copy_from_slice(&session_id.0.to_le_bytes());
    buf
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::parse_payload;
    use pgress_core::{
        attr::Attrs,
        isa::IsaOp,
        node::{ComputeRule, ExecMode, NodeKind},
        partition::{CausalScope, DepKind, LatticeClass, PartitionId},
        region::{CompilePolicy, RegionBoundary, StabilityContract},
        ternary::T,
        uid::Uid,
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Round-trip: encode → decode via parse_payload; assert decoded == original.
    fn roundtrip(op: IsaOp) -> IsaOp {
        let mut payload = Vec::new();
        write_payload(&op, &mut payload);
        let opcode = opcode_for(&op);
        parse_payload(opcode, &payload).expect("parse_payload failed on encoded payload")
    }

    fn uid(n: u64) -> Uid { Uid::from_u128(n as u128) }
    fn pid(n: u64) -> PartitionId { PartitionId::from_u128(n as u128) }

    // ── Opcode dispatch ───────────────────────────────────────────────────────

    #[test]
    fn opcode_for_all_variants() {
        let ops: &[(u16, IsaOp)] = &[
            (0x0001, IsaOp::NodeCreate { id: uid(1), typ: "n".into(), rule: NodeKind::Input, attrs: Attrs::new() }),
            (0x0003, IsaOp::SetValue { node: uid(1), val: T::Pos }),
            (0x0006, IsaOp::DelNode  { id: uid(1) }),
            (0x0008, IsaOp::Demand   { node: uid(1) }),
            (0x000B, IsaOp::Stabilize { region: None }),
            (0x0012, IsaOp::RegionDeclare {
                root:      uid(1),
                boundary:  RegionBoundary::DepClosure { max_depth: 4 },
                stability: StabilityContract::Pinned,
                compile:   CompilePolicy::Eager,
            }),
        ];
        for (expected_opcode, op) in ops {
            assert_eq!(opcode_for(op), *expected_opcode, "wrong opcode for {:?}", op);
        }
    }

    // ── Round-trip tests for each opcode ──────────────────────────────────────

    #[test]
    fn roundtrip_node_create_input() {
        let op = IsaOp::NodeCreate {
            id:    uid(42),
            typ:   "sensor".into(),
            rule:  NodeKind::Input,
            attrs: Attrs::new(),
        };
        let decoded = roundtrip(op.clone());
        assert!(matches!(decoded, IsaOp::NodeCreate { rule: NodeKind::Input, .. }));
        if let (IsaOp::NodeCreate { id: d_id, typ: d_typ, .. },
                IsaOp::NodeCreate { id: o_id, typ: o_typ, .. }) = (&decoded, &op) {
            assert_eq!(d_id, o_id);
            assert_eq!(d_typ, o_typ);
        }
    }

    #[test]
    fn roundtrip_node_create_computed_meetall() {
        let op = IsaOp::NodeCreate {
            id:    uid(7),
            typ:   "gate".into(),
            rule:  NodeKind::Computed(ComputeRule::MeetAll),
            attrs: Attrs::new(),
        };
        let decoded = roundtrip(op);
        assert!(matches!(
            decoded,
            IsaOp::NodeCreate { rule: NodeKind::Computed(ComputeRule::MeetAll), .. }
        ));
    }

    #[test]
    fn roundtrip_node_create_with_attrs() {
        let mut attrs = Attrs::new();
        attrs.set("z_key", true);     // Bool
        attrs.set("a_key", 99i64);    // Int — sorted before z_key
        let op = IsaOp::NodeCreate { id: uid(3), typ: "x".into(), rule: NodeKind::Input, attrs };
        let decoded = roundtrip(op);
        if let IsaOp::NodeCreate { attrs, .. } = decoded {
            assert_eq!(attrs.iter().count(), 2);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn roundtrip_edge_connect_no_port() {
        let op = IsaOp::EdgeConnect {
            id:  uid(1),
            typ: "dep".into(),
            src: uid(10),
            tgt: uid(20),
            dep: DepKind::Local(None),
        };
        let decoded = roundtrip(op);
        assert!(matches!(decoded, IsaOp::EdgeConnect { dep: DepKind::Local(None), .. }));
    }

    #[test]
    fn roundtrip_set_value_all_ternary() {
        for t in [T::Neg, T::Zero, T::Pos] {
            let op = IsaOp::SetValue { node: uid(5), val: t };
            let decoded = roundtrip(op);
            if let IsaOp::SetValue { val, .. } = decoded { assert_eq!(val, t); }
        }
    }

    #[test]
    fn roundtrip_propagate() {
        let decoded = roundtrip(IsaOp::Propagate { node: uid(3) });
        assert!(matches!(decoded, IsaOp::Propagate { .. }));
    }

    #[test]
    fn roundtrip_subscribe() {
        let op = IsaOp::Subscribe { source: uid(1), subscriber: uid(2) };
        let decoded = roundtrip(op);
        if let IsaOp::Subscribe { source, subscriber } = decoded {
            assert_eq!(source,     uid(1));
            assert_eq!(subscriber, uid(2));
        }
    }

    #[test]
    fn roundtrip_del_node() {
        let decoded = roundtrip(IsaOp::DelNode { id: uid(99) });
        assert!(matches!(decoded, IsaOp::DelNode { .. }));
    }

    #[test]
    fn roundtrip_del_edge() {
        let decoded = roundtrip(IsaOp::DelEdge { id: uid(77) });
        assert!(matches!(decoded, IsaOp::DelEdge { .. }));
    }

    #[test]
    fn roundtrip_demand() {
        let decoded = roundtrip(IsaOp::Demand { node: uid(8) });
        assert!(matches!(decoded, IsaOp::Demand { .. }));
    }

    #[test]
    fn roundtrip_set_mode_all_variants() {
        for mode in [ExecMode::Eager, ExecMode::Lazy, ExecMode::Stabilizing] {
            let op = IsaOp::SetMode { node: uid(4), mode };
            let decoded = roundtrip(op);
            if let IsaOp::SetMode { mode: m, .. } = decoded { assert_eq!(m, mode); }
        }
    }

    #[test]
    fn roundtrip_reflect() {
        let decoded = roundtrip(IsaOp::Reflect { node: uid(11) });
        assert!(matches!(decoded, IsaOp::Reflect { .. }));
    }

    #[test]
    fn roundtrip_stabilize_global() {
        let decoded = roundtrip(IsaOp::Stabilize { region: None });
        assert!(matches!(decoded, IsaOp::Stabilize { region: None }));
    }

    #[test]
    fn roundtrip_stabilize_explicit_region() {
        let ids = vec![uid(1), uid(2), uid(3)];
        let op = IsaOp::Stabilize { region: Some(ids.clone()) };
        let decoded = roundtrip(op);
        if let IsaOp::Stabilize { region: Some(r) } = decoded {
            assert_eq!(r, ids);
        } else { panic!("expected Some region"); }
    }

    #[test]
    fn roundtrip_partition_create() {
        let op = IsaOp::PartitionCreate {
            id:             pid(100),
            authority_root: pid(1),
            lattice_class:  LatticeClass(0xFF),
            causal_domain:  CausalScope { scope_bits: 0xDEAD, root: pid(100) },
        };
        let decoded = roundtrip(op);
        if let IsaOp::PartitionCreate { id, lattice_class, causal_domain, .. } = decoded {
            assert_eq!(id, pid(100));
            assert_eq!(lattice_class, LatticeClass(0xFF));
            assert_eq!(causal_domain.scope_bits, 0xDEAD);
        }
    }

    #[test]
    fn roundtrip_partition_bind() {
        let op = IsaOp::PartitionBind { node: uid(5), partition: pid(200) };
        let decoded = roundtrip(op);
        if let IsaOp::PartitionBind { node, partition } = decoded {
            assert_eq!(node,      uid(5));
            assert_eq!(partition, pid(200));
        }
    }

    #[test]
    fn roundtrip_region_declare_dep_closure() {
        let op = IsaOp::RegionDeclare {
            root:      uid(9),
            boundary:  RegionBoundary::DepClosure { max_depth: 8 },
            stability: StabilityContract::EpochTracked,
            compile:   CompilePolicy::Lazy,
        };
        let decoded = roundtrip(op);
        if let IsaOp::RegionDeclare { root, boundary, stability, compile } = decoded {
            assert_eq!(root, uid(9));
            assert!(matches!(boundary, RegionBoundary::DepClosure { max_depth: 8 }));
            assert_eq!(stability, StabilityContract::EpochTracked);
            assert_eq!(compile,   CompilePolicy::Lazy);
        }
    }

    #[test]
    fn roundtrip_region_declare_explicit_set() {
        let members = vec![uid(10), uid(11), uid(12)];
        let op = IsaOp::RegionDeclare {
            root:      uid(10),
            boundary:  RegionBoundary::ExplicitSet(members.clone()),
            stability: StabilityContract::Pinned,
            compile:   CompilePolicy::Never,
        };
        let decoded = roundtrip(op);
        if let IsaOp::RegionDeclare { boundary: RegionBoundary::ExplicitSet(ids), .. } = decoded {
            assert_eq!(ids, members);
        } else { panic!("wrong boundary variant"); }
    }

    // ── encode_record header layout ───────────────────────────────────────────

    #[test]
    fn encode_record_header_layout() {
        let op  = IsaOp::SetValue { node: uid(1), val: T::Pos };
        let ctx = RecordContext::simple(SessionId(42), 7);
        let bytes = encode_record(&op, &ctx);

        // opcode at [0..2]
        assert_eq!(u16::from_le_bytes(bytes[0..2].try_into().unwrap()), 0x0003);
        // flags at [2..4]
        assert_eq!(u16::from_le_bytes(bytes[2..4].try_into().unwrap()), 0x0000);
        // length at [4..8]: header(48) + uid(8) + i8(1) = 57
        let length = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(length as usize, bytes.len());
        assert_eq!(length, 57);
        // session_id at [16..24]
        assert_eq!(u64::from_le_bytes(bytes[16..24].try_into().unwrap()), 42);
        // stream_seq at [40..48]
        assert_eq!(u64::from_le_bytes(bytes[40..48].try_into().unwrap()), 7);
    }

    #[test]
    fn encode_record_length_field_matches_actual_size() {
        let ops = vec![
            IsaOp::NodeCreate { id: uid(1), typ: "x".into(), rule: NodeKind::Input, attrs: Attrs::new() },
            IsaOp::SetValue   { node: uid(1), val: T::Neg },
            IsaOp::Stabilize  { region: Some(vec![uid(1), uid(2)]) },
        ];
        let ctx = RecordContext::simple(SessionId(1), 0);
        for op in ops {
            let bytes = encode_record(&op, &ctx);
            let declared = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
            assert_eq!(declared, bytes.len(),
                "length field doesn't match actual byte count for {:?}", op);
        }
    }

    // ── encode_stream_preamble ────────────────────────────────────────────────

    #[test]
    fn encode_stream_preamble_round_trips() {
        use crate::decode::StreamDecoder;
        let buf = encode_stream_preamble(PathId(99), SessionId(200), 0xCAFE, 0);
        let mut dec = StreamDecoder::new();
        let preamble = dec.decode_preamble(&buf).unwrap();
        assert_eq!(preamble.abi_version, 0x0003);
        assert_eq!(preamble.path_id,     PathId(99));
        assert_eq!(preamble.session_id,  SessionId(200));
        assert_eq!(preamble.feature_flags, 0xCAFE);
    }

    // ── NaN canonicalisation ─────────────────────────────────────────────────

    #[test]
    fn nan_f64_attr_canonicalized() {
        let mut attrs = Attrs::new();
        attrs.set("x", f64::NAN);
        let op = IsaOp::NodeCreate { id: uid(1), typ: "n".into(), rule: NodeKind::Input, attrs };
        let decoded = roundtrip(op);
        if let IsaOp::NodeCreate { attrs, .. } = decoded {
            if let Some(pgress_core::attr::Val::Float(f)) = attrs.get("x") {
                assert!(f.is_nan());
                assert_eq!(f.to_bits(), 0x7FF8_0000_0000_0000u64);
            } else { panic!("expected Float attr"); }
        }
    }
}
