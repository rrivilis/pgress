//! Partition-scoped authority gate and the four-dimensional `AuthorityPolicy` type.
//!
//! ## Two-level model
//!
//! ```text
//! Session manager (header-only):  (partition_id, opcode_class) → PartitionAuthRow
//!   └─ lattice-flow check + session policy check
//!
//! Engine (body-parsed):           (src, tgt) edge_id → CompiledEdgeLabel
//!   └─ fine-grained per-edge capability/scope/projection check
//! ```
//!
//! ## Authority decomposition — integrity vs. confidentiality
//!
//! The single `CapabilityBits` model conflated two orthogonal authority axes:
//!
//! **Integrity axis** (who can create or transfer distinctions):
//! - `assertion`  — can mutate graph state (SetValue, NodeCreate, Stabilize, …)
//! - `delegation` — can transfer assertion rights to child sessions / profiles
//!
//! **Confidentiality axis** (who can witness or reveal distinctions):
//! - `observability` — can witness distinctions (Subscribe, Demand); defines which
//!   quotient of the e-graph is visible to this principal
//! - `disclosure`    — can reveal distinctions across partition / session boundaries
//!
//! ## PartitionAuthRow.policy semantics
//!
//! `policy` encodes the **minimum bits required** from the session. A zero value
//! (`AuthorityPolicy::NONE`) is the permissive default: require nothing. A fully
//! set value (`AuthorityPolicy::ALL`) requires the session to hold all bits in the
//! relevant dimension — the maximally restrictive row configuration.
//!
//! ## Disclosure enforcement (cross-partition edges)
//!
//! Cross-partition `EdgeConnect` (DepKind::Remote) and `SetEdgeLabel` with
//! non-universal causal scope are checked in the Dispatcher control pipeline
//! (after body parsing) before engine apply:
//!
//! ```text
//! session.effective_policy().disclosure & edge.causal_scope_bits
//!     == edge.causal_scope_bits      →  allow
//!     != edge.causal_scope_bits      →  DispatchError::DisclosureViolation
//! ```

use rustc_hash::FxHashMap;
use pgress_core::partition::LatticeClass;
use crate::{OpcodeClass, WirePartitionId};

// ── AuthorityPolicy ───────────────────────────────────────────────────────────

/// Four-dimensional authority policy decomposing capability space along
/// the integrity (assertion/delegation) and confidentiality (observability/disclosure)
/// axes.
///
/// Each dimension is a `u64` bitmask. The universal properties are:
/// - `child.is_bounded_by(parent)` must hold for a valid delegation.
/// - `claimed.intersect(parent_ceiling)` yields the effective policy.
///
/// ## Dimension meanings
///
/// When used as a **session / tenant policy** (claimed capability):
/// - bits present = the session holds that permission.
/// - `ALL` = maximum claim; `NONE` = nothing claimed.
///
/// When used as a **PartitionAuthRow minimum requirement**:
/// - bits present = the session must hold those bits to pass.
/// - `NONE` = no requirement (permissive); `ALL` = session must hold everything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AuthorityPolicy {
    /// Can create or mutate distinctions in the target partition.
    ///
    /// Gates: `Control` (NodeCreate, EdgeConnect, PartitionCreate, SetEdgeLabel, …),
    /// `SetValue`, `Propagate` (intra-partition; cross-partition uses `disclosure`),
    /// `Stabilize`, `Data` (Reflect).
    pub assertion:     u64,

    /// Can transfer assertion rights to child sessions or profiles.
    ///
    /// Gates: `Profile` op issuance. Delegation is strictly bounded: you may only
    /// delegate bits you hold in `assertion`.
    pub delegation:    u64,

    /// Can witness distinctions — subscribe to state, demand lazy evaluation.
    ///
    /// Defines the observable quotient coarseness for this principal:
    /// coarser quotient = lower observability. A principal with `observability = 0`
    /// on a partition may not Subscribe or Demand there.
    ///
    /// Gates: `Subscribe`, `Demand`.
    pub observability: u64,

    /// Can reveal observed distinctions across partition or session boundaries.
    ///
    /// Enforced in the Dispatcher for cross-partition edges:
    /// `session.disclosure & edge.causal_scope_bits == edge.causal_scope_bits`.
    ///
    /// Gates: `EdgeConnect` with `DepKind::Remote`, `SetEdgeLabel` with
    /// non-zero causal scope bits.
    pub disclosure:    u64,
}

impl AuthorityPolicy {
    /// All dimensions fully open — maximum claim or maximum restriction.
    pub const ALL: Self = Self {
        assertion:     u64::MAX,
        delegation:    u64::MAX,
        observability: u64::MAX,
        disclosure:    u64::MAX,
    };

    /// All dimensions zero — minimum claim; permissive requirement.
    pub const NONE: Self = Self {
        assertion:     0,
        delegation:    0,
        observability: 0,
        disclosure:    0,
    };

    /// Read-only: can observe and disclose but not assert or delegate.
    pub const READ_ONLY: Self = Self {
        assertion:     0,
        delegation:    0,
        observability: u64::MAX,
        disclosure:    u64::MAX,
    };

    /// Component-wise intersection (`AND` on each axis).
    ///
    /// The canonical way to compute effective capability:
    /// `claimed.intersect(parent_ceiling)` → child can never exceed parent on any axis.
    #[inline]
    pub fn intersect(self, other: Self) -> Self {
        Self {
            assertion:     self.assertion     & other.assertion,
            delegation:    self.delegation    & other.delegation,
            observability: self.observability & other.observability,
            disclosure:    self.disclosure    & other.disclosure,
        }
    }

    /// True iff every axis of `self` is a subset of the corresponding axis of `ceiling`.
    #[inline]
    pub fn is_bounded_by(self, ceiling: Self) -> bool {
        self == self.intersect(ceiling)
    }

    /// Return the relevant dimension bits for a given opcode class.
    ///
    /// Used both to look up the session's claimed bits for an opcode class
    /// and to look up the row's required bits.
    #[inline]
    pub fn for_opcode_class(&self, oc: OpcodeClass) -> u64 {
        match oc {
            OpcodeClass::Control
            | OpcodeClass::SetValue
            | OpcodeClass::Propagate
            | OpcodeClass::Stabilize
            | OpcodeClass::Data       => self.assertion,
            OpcodeClass::Subscribe
            | OpcodeClass::Demand     => self.observability,
            OpcodeClass::Profile      => self.delegation,
            OpcodeClass::Unknown      => 0,
        }
    }
}

// ── AuthAxis ──────────────────────────────────────────────────────────────────

/// Which dimension of `AuthorityPolicy` was violated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthAxis {
    Assertion,
    Delegation,
    Observability,
    Disclosure,
}

// ── PartitionAuthRow ──────────────────────────────────────────────────────────

/// Compiled authority row for a (partition, opcode_class) pair.
///
/// `policy` encodes the **minimum required bits** from the session's effective
/// policy for the relevant dimension. Use `AuthorityPolicy::NONE` for the
/// permissive default (require nothing). Use a non-zero value to require
/// specific capability bits from sessions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionAuthRow {
    /// Required lattice class: `emitted_class.flows_to(lattice_class)` must hold.
    pub lattice_class: LatticeClass,
    /// Minimum required authority from the session (NONE = permissive).
    pub policy:        AuthorityPolicy,
}

impl PartitionAuthRow {
    /// Permissive default: bottom lattice class (anything flows in),
    /// no required policy bits (any session passes).
    pub fn permissive() -> Self {
        PartitionAuthRow {
            lattice_class: LatticeClass::BOTTOM,
            policy:        AuthorityPolicy::NONE,
        }
    }
}

// ── AuthTableKey ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AuthTableKey {
    pub partition_id: WirePartitionId,
    pub opcode_class: OpcodeClass,
}

// ── Gate result ───────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataplaneGateReason {
    /// Source lattice class does not flow into the partition's required class.
    LatticeViolation,
    /// Session's effective policy is insufficient for this opcode class.
    PolicyViolation(AuthAxis),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateResult {
    Allow,
    Deny(DataplaneGateReason),
}

impl GateResult {
    pub fn is_allowed(self) -> bool { matches!(self, GateResult::Allow) }
}

// ── PartitionAuthTable ────────────────────────────────────────────────────────

/// Partition-scoped authority table. Keyed on `(WirePartitionId, OpcodeClass)`.
///
/// Gate evaluation (in order):
/// 1. `emitted_class.flows_to(row.lattice_class)` — partition-side lattice check.
/// 2. `(session_policy.for_opcode_class(oc) & row.policy.for_opcode_class(oc))
///        == row.policy.for_opcode_class(oc)` — session-side policy check.
///
/// No row installed → `Allow` (permissive by default; restriction is explicit).
#[derive(Clone, Debug, Default)]
pub struct PartitionAuthTable {
    rows: FxHashMap<AuthTableKey, PartitionAuthRow>,
}

impl PartitionAuthTable {
    pub fn new() -> Self { Self::default() }

    /// Install or replace a row. Called by the control pipeline.
    pub fn install(&mut self, key: AuthTableKey, row: PartitionAuthRow) {
        self.rows.insert(key, row);
    }

    /// Remove a row (e.g. when a partition is deleted).
    pub fn remove(&mut self, key: &AuthTableKey) {
        self.rows.remove(key);
    }

    /// Evaluate the gate for an incoming record.
    ///
    /// `emitted_class`  — source lattice class; use `LatticeClass::BOTTOM`
    ///                    when not carried in the header (permissive fallback).
    /// `session_policy` — effective policy for the session (after parent ceiling
    ///                    intersection). Use `AuthorityPolicy::ALL` if not
    ///                    available (permissive fallback).
    pub fn gate(
        &self,
        partition_id:   WirePartitionId,
        opcode_class:   OpcodeClass,
        emitted_class:  LatticeClass,
        session_policy: AuthorityPolicy,
    ) -> GateResult {
        let key = AuthTableKey { partition_id, opcode_class };
        let row = match self.rows.get(&key) {
            Some(r) => r,
            None    => return GateResult::Allow,
        };

        // 1. Lattice class check
        if !emitted_class.flows_to(row.lattice_class) {
            return GateResult::Deny(DataplaneGateReason::LatticeViolation);
        }

        // 2. Session policy check — session must hold at least the required bits
        //    in the relevant dimension for this opcode class.
        let session_dim  = session_policy.for_opcode_class(opcode_class);
        let required_dim = row.policy.for_opcode_class(opcode_class);
        if (session_dim & required_dim) != required_dim {
            let axis = match opcode_class {
                OpcodeClass::Subscribe | OpcodeClass::Demand => AuthAxis::Observability,
                OpcodeClass::Profile                          => AuthAxis::Delegation,
                _                                             => AuthAxis::Assertion,
            };
            return GateResult::Deny(DataplaneGateReason::PolicyViolation(axis));
        }

        GateResult::Allow
    }

    pub fn len(&self) -> usize { self.rows.len() }
    pub fn is_empty(&self) -> bool { self.rows.is_empty() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const P1: WirePartitionId = WirePartitionId(1);

    fn row(lattice: LatticeClass, policy: AuthorityPolicy) -> PartitionAuthRow {
        PartitionAuthRow { lattice_class: lattice, policy }
    }

    // ── AuthorityPolicy ───────────────────────────────────────────────────────

    #[test]
    fn intersect_clips_component_wise() {
        let a = AuthorityPolicy { assertion: 0b1111, delegation: 0b0011,
                                   observability: 0b1010, disclosure: 0b0110 };
        let b = AuthorityPolicy { assertion: 0b0101, delegation: 0b0001,
                                   observability: 0b1100, disclosure: 0b0010 };
        let r = a.intersect(b);
        assert_eq!(r.assertion,     0b0101);
        assert_eq!(r.delegation,    0b0001);
        assert_eq!(r.observability, 0b1000);
        assert_eq!(r.disclosure,    0b0010);
    }

    #[test]
    fn intersect_all_with_none_is_none() {
        assert_eq!(AuthorityPolicy::ALL.intersect(AuthorityPolicy::NONE), AuthorityPolicy::NONE);
        assert_eq!(AuthorityPolicy::NONE.intersect(AuthorityPolicy::ALL), AuthorityPolicy::NONE);
    }

    #[test]
    fn is_bounded_by_holds_for_subset() {
        let parent = AuthorityPolicy { assertion: 0b1111, ..AuthorityPolicy::NONE };
        let child  = AuthorityPolicy { assertion: 0b0101, ..AuthorityPolicy::NONE };
        assert!(child.is_bounded_by(parent));
    }

    #[test]
    fn is_bounded_by_fails_for_superset() {
        let parent = AuthorityPolicy { assertion: 0b0001, ..AuthorityPolicy::NONE };
        let exceeds = AuthorityPolicy { assertion: 0b0011, ..AuthorityPolicy::NONE };
        assert!(!exceeds.is_bounded_by(parent));
    }

    #[test]
    fn for_opcode_class_routes_correctly() {
        let p = AuthorityPolicy {
            assertion: 1, delegation: 2, observability: 4, disclosure: 8
        };
        assert_eq!(p.for_opcode_class(OpcodeClass::SetValue),   1);
        assert_eq!(p.for_opcode_class(OpcodeClass::Control),    1);
        assert_eq!(p.for_opcode_class(OpcodeClass::Stabilize),  1);
        assert_eq!(p.for_opcode_class(OpcodeClass::Data),       1);
        assert_eq!(p.for_opcode_class(OpcodeClass::Propagate),  1);
        assert_eq!(p.for_opcode_class(OpcodeClass::Subscribe),  4);
        assert_eq!(p.for_opcode_class(OpcodeClass::Demand),     4);
        assert_eq!(p.for_opcode_class(OpcodeClass::Profile),    2);
        assert_eq!(p.for_opcode_class(OpcodeClass::Unknown),    0);
    }

    // ── PartitionAuthTable — lattice checks ───────────────────────────────────

    #[test]
    fn no_row_is_permissive() {
        let table = PartitionAuthTable::new();
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::TOP, AuthorityPolicy::ALL),
            GateResult::Allow
        );
    }

    #[test]
    fn bottom_emitter_always_flows_to_any_lattice_class() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            row(LatticeClass(0b1111), AuthorityPolicy::NONE),
        );
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::BOTTOM, AuthorityPolicy::ALL),
            GateResult::Allow
        );
    }

    #[test]
    fn incompatible_lattice_class_denied() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            row(LatticeClass(0b0001), AuthorityPolicy::NONE),
        );
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass(0b0010), AuthorityPolicy::ALL),
            GateResult::Deny(DataplaneGateReason::LatticeViolation)
        );
    }

    #[test]
    fn compatible_lattice_class_allowed() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::Demand },
            row(LatticeClass(0b0111), AuthorityPolicy::NONE),
        );
        assert_eq!(
            table.gate(P1, OpcodeClass::Demand, LatticeClass(0b0011), AuthorityPolicy::ALL),
            GateResult::Allow
        );
    }

    #[test]
    fn row_only_applies_to_matching_opcode_class() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            row(LatticeClass::TOP, AuthorityPolicy::NONE),
        );
        assert_eq!(
            table.gate(P1, OpcodeClass::Demand, LatticeClass::BOTTOM, AuthorityPolicy::ALL),
            GateResult::Allow
        );
    }

    #[test]
    fn remove_row_restores_permissive() {
        let mut table = PartitionAuthTable::new();
        let key = AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue };
        table.install(key, row(LatticeClass(0b0001), AuthorityPolicy::NONE));
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass(0b0010), AuthorityPolicy::ALL),
            GateResult::Deny(DataplaneGateReason::LatticeViolation)
        );
        table.remove(&key);
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass(0b0010), AuthorityPolicy::ALL),
            GateResult::Allow
        );
    }

    // ── PartitionAuthTable — session policy checks ────────────────────────────

    #[test]
    fn permissive_row_allows_any_session() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow::permissive(),
        );
        // Even a session with zero assertion passes a permissive row (required = 0)
        let no_write = AuthorityPolicy { assertion: 0, ..AuthorityPolicy::NONE };
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::BOTTOM, no_write),
            GateResult::Allow
        );
    }

    #[test]
    fn required_assertion_bit_denied_when_session_lacks_it() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow {
                lattice_class: LatticeClass::BOTTOM,
                policy: AuthorityPolicy { assertion: 0x01, ..AuthorityPolicy::NONE },
            },
        );
        let no_write = AuthorityPolicy { assertion: 0, ..AuthorityPolicy::ALL };
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::BOTTOM, no_write),
            GateResult::Deny(DataplaneGateReason::PolicyViolation(AuthAxis::Assertion))
        );
    }

    #[test]
    fn session_superset_satisfies_partial_requirement() {
        let mut table = PartitionAuthTable::new();
        // Partition requires bit 0x01 in assertion
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow {
                lattice_class: LatticeClass::BOTTOM,
                policy: AuthorityPolicy { assertion: 0x01, ..AuthorityPolicy::NONE },
            },
        );
        // Session holds 0xFF — superset of 0x01
        let policy = AuthorityPolicy { assertion: 0xFF, ..AuthorityPolicy::NONE };
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::BOTTOM, policy),
            GateResult::Allow
        );
    }

    #[test]
    fn observability_violation_reported_correctly() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::Demand },
            PartitionAuthRow {
                lattice_class: LatticeClass::BOTTOM,
                policy: AuthorityPolicy { observability: 0x01, ..AuthorityPolicy::NONE },
            },
        );
        let no_obs = AuthorityPolicy { observability: 0, ..AuthorityPolicy::ALL };
        assert_eq!(
            table.gate(P1, OpcodeClass::Demand, LatticeClass::BOTTOM, no_obs),
            GateResult::Deny(DataplaneGateReason::PolicyViolation(AuthAxis::Observability))
        );
    }
}
