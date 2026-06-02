//! Partition boundary types, authority lattice, and edge-label types.
//!
//! ## Separation of concerns
//!
//! | Layer      | Responsibility                                            |
//! |------------|-----------------------------------------------------------|
//! | LocalDep   | Preserves meaning — value exists in local context         |
//! | RemoteDep  | Transmits evidence — typed, versioned, causal provenance  |
//! | Stabilize  | Constructs meaning — interprets evidence locally          |
//! | EdgeLabel  | Declares authority — who may authorize propagation        |
//!
//! ## Authority / causality commutation
//!
//! Authority (who may authorize) and causality (where effects travel) are
//! kept strictly separate. `EdgeLabel` encodes both dimensions as compiled
//! flat types. Authority checks are O(1) bit operations; registry traversal
//! never occurs at propagation time.

use rustc_hash::FxHashMap;
use crate::{delta::ProjectionMask, node::PortKind, time::VectorClock, uid::Uid};

/// Globally unique identifier for a partition (process / shard / node).
pub type PartitionId = uuid::Uuid;

/// Globally unique identifier for an authority root.
/// Typically the PartitionId of the declaring partition.
pub type AuthorityRoot = PartitionId;

// ── Primitive authority types ─────────────────────────────────────────────────

/// Precomputed Denning lattice class.
///
/// `u64` encodes the transitive closure of the partition class hierarchy.
/// The partial order is a single bit-op: `flows_to` is O(1).
///
/// Bottom (0x0) = lowest privilege; information at bottom can flow anywhere.
/// Top (0xFFFF…) = highest privilege; information at top can only flow to top.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct LatticeClass(pub u64);

impl LatticeClass {
    /// Lowest privilege class — information may flow to any class.
    pub const BOTTOM: Self = Self(0);
    /// Highest privilege class — information may only flow to itself.
    pub const TOP: Self = Self(u64::MAX);

    /// True if information at `self` may flow to `other` (self ≤ other in lattice).
    #[inline]
    pub fn flows_to(self, other: LatticeClass) -> bool {
        (self.0 & other.0) == self.0
    }
}

/// CHERI-inspired permission bitfield.
///
/// Each bit encodes a permitted operation on a dependency:
///   `READ`      — may observe source value
///   `PROPAGATE` — may enqueue downstream work items
///   `STABILIZE` — may trigger Stabilize on the target region
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct CapabilityBits(pub u64);

impl CapabilityBits {
    pub const NONE:      Self = Self(0);
    pub const ALL:       Self = Self(u64::MAX);
    pub const READ:      Self = Self(1 << 0);
    pub const PROPAGATE: Self = Self(1 << 1);
    pub const STABILIZE: Self = Self(1 << 2);

    /// True if `self` grants all bits required by `required`.
    #[inline]
    pub fn allows(self, required: CapabilityBits) -> bool {
        (self.0 & required.0) == required.0
    }
}

impl std::ops::BitOr for CapabilityBits {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self { CapabilityBits(self.0 | rhs.0) }
}

impl std::ops::BitOrAssign for CapabilityBits {
    fn bitor_assign(&mut self, rhs: Self) { self.0 |= rhs.0; }
}

impl std::ops::BitAnd for CapabilityBits {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self { CapabilityBits(self.0 & rhs.0) }
}

impl std::ops::BitAndAssign for CapabilityBits {
    fn bitand_assign(&mut self, rhs: Self) { self.0 &= rhs.0; }
}

/// Causal scope: where may the causal effect of this edge travel?
///
/// `scope_bits` is the compiled containment mask (hot-path form).
/// `root` is the partition that declared this scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CausalScope {
    pub scope_bits: u64,
    pub root:       PartitionId,
}

impl CausalScope {
    /// Universal scope — causal effects may travel anywhere.
    pub const UNIVERSAL: Self = Self {
        scope_bits: u64::MAX,
        root: PartitionId::nil(),
    };

    /// True if `other_bits` is fully contained within this scope.
    #[inline]
    pub fn contains(self, other_bits: u64) -> bool {
        (self.scope_bits & other_bits) == other_bits
    }
}

impl Default for CausalScope {
    fn default() -> Self { Self::UNIVERSAL }
}

// ── EdgeLabel — structural authority label on a dep edge ──────────────────────

/// Declared authority label on a dependency edge.
///
/// Lives in the identity layer: set at `EdgeConnect` or `SetEdgeLabel` time,
/// never mutated by propagation. Compiled into `CompiledEdgeLabel` for the
/// hot path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeLabel {
    pub capability:      CapabilityBits,
    pub lattice_class:   LatticeClass,
    pub projection_mask: ProjectionMask,
    pub causal_scope:    CausalScope,
}

impl EdgeLabel {
    /// Default: fully permissive — wildcard projection, bottom class,
    /// all capabilities, universal scope. Preserves existing behavior.
    pub fn permissive() -> Self {
        EdgeLabel {
            capability:      CapabilityBits::ALL,
            lattice_class:   LatticeClass::BOTTOM,
            projection_mask: ProjectionMask::wildcard(),
            causal_scope:    CausalScope::UNIVERSAL,
        }
    }
}

impl Default for EdgeLabel {
    fn default() -> Self { Self::permissive() }
}

/// Compiled (flat) form of `EdgeLabel`. Lives in `DepMeta` for hot-path access.
///
/// Produced by `PartitionRegistry::compile_label`. Stored alongside `DepMeta`
/// in `DepRegistry`. Registry traversal never occurs when reading this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledEdgeLabel {
    pub capability:        CapabilityBits,
    pub lattice_class:     LatticeClass,
    pub projection_mask:   ProjectionMask,
    pub causal_scope_bits: u64,
}

impl CompiledEdgeLabel {
    /// Permissive default — wildcard projection, bottom class, all caps,
    /// universal scope. Used when no `SetEdgeLabel` has been called.
    pub fn permissive_default() -> Self {
        CompiledEdgeLabel {
            capability:        CapabilityBits::ALL,
            lattice_class:     LatticeClass::BOTTOM,
            projection_mask:   ProjectionMask::wildcard(),
            causal_scope_bits: u64::MAX,
        }
    }

    /// Compile from a declared `EdgeLabel`.
    pub fn compile(label: &EdgeLabel) -> Self {
        CompiledEdgeLabel {
            capability:        label.capability,
            lattice_class:     label.lattice_class,
            projection_mask:   label.projection_mask.clone(),
            causal_scope_bits: label.causal_scope.scope_bits,
        }
    }
}

impl Default for CompiledEdgeLabel {
    fn default() -> Self { Self::permissive_default() }
}

// ── EmittedAuth — authority context of a propagating value ───────────────────

/// The authority context carried by a propagating value.
/// Constructed at enqueue time from the source node's partition + edge label.
/// Never stored — ephemeral, constructed on the stack.
#[derive(Clone, Copy, Debug)]
pub struct EmittedAuth {
    pub source_root: AuthorityRoot,
    pub class:       LatticeClass,
    pub capability:  CapabilityBits,
    pub scope_bits:  u64,
}

impl EmittedAuth {
    /// Cross-partition: read directly from `RemoteDep` authority fields.
    pub fn from_remote(dep: &RemoteDep) -> Self {
        EmittedAuth {
            source_root: dep.source_authority,
            class:       dep.emitted_class,
            capability:  dep.capability,
            scope_bits:  dep.causal_scope.scope_bits,
        }
    }

    /// Intra-partition: derive from source node's partition + compiled edge label.
    pub fn from_local(label: &CompiledEdgeLabel, partition: &PartitionDecl) -> Self {
        EmittedAuth {
            source_root: partition.authority_root,
            class:       partition.lattice_class,
            capability:  label.capability,
            scope_bits:  label.causal_scope_bits,
        }
    }

    /// Unbound node: bottom of lattice, zero capability, no scope.
    /// In Advisory/Audit modes this never suppresses. In Enforced mode,
    /// `UnboundPolicy` governs whether to allow or deny.
    pub fn unbound() -> Self {
        EmittedAuth {
            source_root: AuthorityRoot::nil(),
            class:       LatticeClass::BOTTOM,
            capability:  CapabilityBits::ALL, // unbound = unconstrained
            scope_bits:  u64::MAX,
        }
    }
}

// ── GateReason — diagnostic payload on AuthViolation ─────────────────────────

/// Why `authority_gate` returned false for a particular (src, tgt) pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateReason {
    CapabilityDenied {
        required: CapabilityBits,
        provided: CapabilityBits,
    },
    ScopeViolation {
        required_bits: u64,
        provided_bits: u64,
    },
    LatticeViolation {
        required_class: LatticeClass,
        emitted_class:  LatticeClass,
    },
    UnboundSource,
}

// ── authority_gate — O(1), three bit operations ───────────────────────────────

/// Check whether `emitted` satisfies the authority requirements of `compiled`.
/// Three bit operations; no allocation, no registry access.
#[inline]
pub fn authority_gate(compiled: &CompiledEdgeLabel, emitted: &EmittedAuth) -> Result<(), GateReason> {
    if !compiled.capability.allows(emitted.capability) {
        return Err(GateReason::CapabilityDenied {
            required: compiled.capability,
            provided: emitted.capability,
        });
    }
    if !(CausalScope { scope_bits: compiled.causal_scope_bits, root: PartitionId::nil() })
           .contains(emitted.scope_bits) {
        return Err(GateReason::ScopeViolation {
            required_bits: compiled.causal_scope_bits,
            provided_bits: emitted.scope_bits,
        });
    }
    if !emitted.class.flows_to(compiled.lattice_class) {
        return Err(GateReason::LatticeViolation {
            required_class: compiled.lattice_class,
            emitted_class:  emitted.class,
        });
    }
    Ok(())
}

// ── ZeroKind — typed uncertainty ─────────────────────────────────────────────

/// The named reason why a partition emitted a Zero value.
///
/// Zero never crosses a partition as raw truth — it crosses only as a typed
/// message carrying this reason. The receiving partition interprets it locally.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ZeroKind {
    /// Two locally consistent values disagree — classic merge conflict.
    Conflict,
    /// Computation started but deps have not fully resolved.
    Incomplete,
    /// Multiple valid resolutions exist and none is preferred by local policy.
    Ambiguous,
    /// A previously definite (Pos/Neg) value was retracted.
    Retracted,
    /// The port type contract was violated at the partition boundary.
    BoundaryViolation,
    /// The received version is not causally consistent with local state.
    VersionSkew,
}

// ── PayloadKind ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadKind {
    Definite,
    TypedZero(ZeroKind),
}

// ── RemoteDep — evidence from another partition ───────────────────────────────

/// Metadata carried by a dependency edge that crosses a partition boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteDep {
    // ── existing causal provenance ────────────────────────────────────────────
    pub source_partition: PartitionId,
    pub source_uid:       Uid,
    pub source_version:   u64,
    pub port_kind:        PortKind,
    pub causal_frontier:  VectorClock,
    pub payload_kind:     PayloadKind,

    // ── authority provenance (new) ────────────────────────────────────────────
    pub source_authority: AuthorityRoot,
    pub emitted_class:    LatticeClass,
    pub capability:       CapabilityBits,
    pub causal_scope:     CausalScope,
}

impl RemoteDep {
    /// Construct with explicit causal provenance.
    /// Authority fields default to permissive (bottom class, all caps, universal scope).
    pub fn new(
        source_partition: PartitionId,
        source_uid:       Uid,
        source_version:   u64,
        port_kind:        PortKind,
        causal_frontier:  VectorClock,
        payload_kind:     PayloadKind,
    ) -> Self {
        RemoteDep {
            source_partition,
            source_uid,
            source_version,
            port_kind,
            causal_frontier,
            payload_kind,
            source_authority: AuthorityRoot::nil(),
            emitted_class:    LatticeClass::BOTTOM,
            capability:       CapabilityBits::ALL,
            causal_scope:     CausalScope::UNIVERSAL,
        }
    }

    /// Builder: set authority fields.
    pub fn with_authority(
        mut self,
        source_authority: AuthorityRoot,
        emitted_class:    LatticeClass,
        capability:       CapabilityBits,
        causal_scope:     CausalScope,
    ) -> Self {
        self.source_authority = source_authority;
        self.emitted_class    = emitted_class;
        self.capability       = capability;
        self.causal_scope     = causal_scope;
        self
    }

    pub fn is_zero(&self) -> bool {
        matches!(self.payload_kind, PayloadKind::TypedZero(_))
    }

    pub fn zero_kind(&self) -> Option<ZeroKind> {
        match &self.payload_kind {
            PayloadKind::TypedZero(k) => Some(*k),
            PayloadKind::Definite     => None,
        }
    }
}

// ── DepKind ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DepKind {
    Local(Option<crate::node::Port>),
    Remote(RemoteDep),
}

impl DepKind {
    pub fn port_kind(&self) -> PortKind {
        match self {
            DepKind::Local(Some(p)) => p.kind,
            DepKind::Local(None)    => PortKind::Signal,
            DepKind::Remote(r)      => r.port_kind,
        }
    }

    pub fn port_name(&self) -> Option<&str> {
        match self {
            DepKind::Local(Some(p)) => Some(p.name.as_str()),
            _                       => None,
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, DepKind::Remote(_))
    }

    pub fn remote(&self) -> Option<&RemoteDep> {
        match self {
            DepKind::Remote(r) => Some(r),
            _                  => None,
        }
    }
}

impl Default for DepKind {
    fn default() -> Self { DepKind::Local(None) }
}

// ── PartitionDecl ─────────────────────────────────────────────────────────────

/// Declared structural identity of a partition.
/// Id-layer: set at `PartitionCreate`, mutated only by `SetPartitionAuthority`.
#[derive(Clone, Debug)]
pub struct PartitionDecl {
    pub id:              PartitionId,
    pub authority_root:  AuthorityRoot,
    pub lattice_class:   LatticeClass,
    pub causal_domain:   CausalScope,
}

// ── PartitionRegistry ─────────────────────────────────────────────────────────

/// Registry of declared partitions. Owned by Engine. Never accessed during
/// `step_push` or `should_propagate` — all hot-path data is pre-compiled into
/// `CompiledEdgeLabel` stored in `DepMeta`.
#[derive(Clone, Debug, Default)]
pub struct PartitionRegistry {
    pub partitions: FxHashMap<PartitionId, PartitionDecl>,
}

impl PartitionRegistry {
    pub fn new() -> Self { Self::default() }

    pub fn register(&mut self, decl: PartitionDecl) {
        self.partitions.insert(decl.id, decl);
    }

    pub fn decl(&self, id: PartitionId) -> Option<&PartitionDecl> {
        self.partitions.get(&id)
    }

    /// Compile an `EdgeLabel` into a `CompiledEdgeLabel`.
    /// Incorporates partition context if the source partition is known.
    pub fn compile_label(
        &self,
        label: &EdgeLabel,
        source_partition: Option<PartitionId>,
    ) -> CompiledEdgeLabel {
        let mut compiled = CompiledEdgeLabel::compile(label);
        // If source partition is known and has a tighter scope, constrain.
        if let Some(pid) = source_partition {
            if let Some(decl) = self.partitions.get(&pid) {
                // Narrow scope to the intersection of edge scope and partition domain.
                compiled.causal_scope_bits &= decl.causal_domain.scope_bits;
            }
        }
        compiled
    }
}

// ── AuthorityMode ─────────────────────────────────────────────────────────────

/// Engine-level authority enforcement setting.
///
/// Advisory is the default and is zero-cost: no auth struct construction,
/// no `authority_gate` call — only a single enum branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AuthorityMode {
    /// Zero cost: mode branch only. No gate construction or evaluation.
    #[default]
    Advisory,
    /// Evaluate gate, log violations as `PropEvent::AuthViolation`.
    /// Does not suppress propagation.
    Audit,
    /// Evaluate gate; suppress unauthorized subscriber enqueue.
    Enforced,
}

/// Policy for nodes not bound to any partition via `PartitionBind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum UnboundPolicy {
    /// Unbound nodes propagate freely (default in Advisory).
    #[default]
    Allow,
    /// Log but do not suppress (default in Audit).
    AuditOnly,
    /// Suppress all propagation from unbound nodes (default in Enforced).
    Deny,
}

impl UnboundPolicy {
    pub fn default_for(mode: AuthorityMode) -> Self {
        match mode {
            AuthorityMode::Advisory  => UnboundPolicy::Allow,
            AuthorityMode::Audit     => UnboundPolicy::AuditOnly,
            AuthorityMode::Enforced  => UnboundPolicy::Deny,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Port, PortKind};

    #[test]
    fn test_dep_kind_local_port_kind() {
        let d = DepKind::Local(Some(Port::effort("e")));
        assert_eq!(d.port_kind(), PortKind::Effort);
        assert_eq!(d.port_name(), Some("e"));
        assert!(!d.is_remote());
    }

    #[test]
    fn test_dep_kind_local_no_port() {
        let d = DepKind::Local(None);
        assert_eq!(d.port_kind(), PortKind::Signal);
        assert_eq!(d.port_name(), None);
    }

    #[test]
    fn test_dep_kind_remote() {
        let r = RemoteDep::new(
            PartitionId::new_v4(), crate::uid::fresh(), 3,
            PortKind::Flow, VectorClock::new(), PayloadKind::Definite,
        );
        let d = DepKind::Remote(r);
        assert_eq!(d.port_kind(), PortKind::Flow);
        assert!(d.is_remote());
        assert!(!d.remote().unwrap().is_zero());
    }

    #[test]
    fn test_zero_kind_typed() {
        let r = RemoteDep::new(
            PartitionId::new_v4(), crate::uid::fresh(), 1,
            PortKind::Bond, VectorClock::new(),
            PayloadKind::TypedZero(ZeroKind::Conflict),
        );
        assert!(r.is_zero());
        assert_eq!(r.zero_kind(), Some(ZeroKind::Conflict));
    }

    #[test]
    fn test_lattice_class_flows_to() {
        let a = LatticeClass(0b0011);
        let b = LatticeClass(0b0111);
        assert!(a.flows_to(b));   // a ≤ b
        assert!(!b.flows_to(a));  // b ≰ a
        assert!(a.flows_to(a));   // reflexive
    }

    #[test]
    fn test_capability_bits_allows() {
        let edge = CapabilityBits::READ | CapabilityBits::PROPAGATE;
        assert!(edge.allows(CapabilityBits::READ));
        assert!(edge.allows(CapabilityBits::PROPAGATE));
        assert!(!edge.allows(CapabilityBits::STABILIZE));
    }

    #[test]
    fn test_authority_gate_permissive() {
        let compiled = CompiledEdgeLabel::permissive_default();
        let emitted  = EmittedAuth::unbound();
        assert!(authority_gate(&compiled, &emitted).is_ok());
    }

    #[test]
    fn test_authority_gate_lattice_violation() {
        let compiled = CompiledEdgeLabel {
            lattice_class: LatticeClass(0b0001),  // requires class 1
            ..CompiledEdgeLabel::permissive_default()
        };
        // emitted class 0b0010 does NOT flow to 0b0001 (0b0010 & 0b0001 != 0b0010)
        let emitted = EmittedAuth { class: LatticeClass(0b0010), ..EmittedAuth::unbound() };
        assert!(matches!(
            authority_gate(&compiled, &emitted),
            Err(GateReason::LatticeViolation { .. })
        ));
    }
}
