//! Partition-scoped authority gate — the dataplane's coarse admission check.
//!
//! ## Granularity
//!
//! The gate is keyed on `(WirePartitionId, OpcodeClass)`. This is the finest
//! granularity achievable from the `IsaHeader` alone, because `edge_id` is
//! NOT present in the header.
//!
//! Per-edge authority (`EdgeLabel.capability_bits`, `projection_mask`, etc.) is
//! compiled into `DepMeta.label` at `SetEdgeLabel` time and lives entirely in
//! the engine. It is never consulted by the session manager; it governs which
//! admitted records the engine allows to propagate across a specific dependency
//! edge.
//!
//! ## Two-level model
//!
//! ```text
//! Session manager (header-only):  (partition_id, opcode_class) → PartitionAuthRow
//!   └─ coarse lattice-flow check; rejects obviously unauthorised records
//!
//! Engine (body-parsed):           (src, tgt) edge_id → CompiledEdgeLabel
//!   └─ fine-grained per-edge capability/scope/projection check
//! ```

use rustc_hash::FxHashMap;
use pgress_core::partition::{CapabilityBits, LatticeClass};
use crate::{OpcodeClass, WirePartitionId};

// ── PartitionAuthRow ──────────────────────────────────────────────────────────

/// Compiled authority row for a (partition, opcode_class) pair.
/// Installed by the control pipeline after `PartitionCreate` / `SetPartitionAuthority`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionAuthRow {
    /// Required lattice class: the emitted record's source class must `flows_to`
    /// this class for the gate to pass.
    pub lattice_class: LatticeClass,
    /// Opaque capability mask reserved for future use.
    /// Currently not checked at the dataplane; included for forward compatibility.
    pub capability_mask: CapabilityBits,
}

impl PartitionAuthRow {
    /// Permissive default: bottom lattice class (anything flows in), all capabilities.
    pub fn permissive() -> Self {
        PartitionAuthRow {
            lattice_class:   LatticeClass::BOTTOM,
            capability_mask: CapabilityBits::ALL,
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
    /// The emitted record's lattice class cannot flow into the target partition's class.
    LatticeViolation,
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
/// Populated by the control pipeline when a `PartitionCreate` or
/// `SetPartitionAuthority` op completes on the engine. After installation,
/// all gate checks for records targeting that partition + opcode class are
/// pure hashmap lookups — no engine involvement.
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

    /// Evaluate the partition-scoped gate for an incoming record.
    ///
    /// - If no row is installed for `(partition_id, opcode_class)`: **Allow**
    ///   (permissive default; restriction must be explicitly installed).
    /// - If a row is installed: check `emitted_class.flows_to(row.lattice_class)`.
    ///
    /// `emitted_class` is the lattice class of the record's source.
    /// When the source is not known from the header alone, pass `LatticeClass::BOTTOM`
    /// (least restrictive); the engine will apply per-edge class checks.
    pub fn gate(
        &self,
        partition_id:  WirePartitionId,
        opcode_class:  OpcodeClass,
        emitted_class: LatticeClass,
    ) -> GateResult {
        let key = AuthTableKey { partition_id, opcode_class };
        let row = match self.rows.get(&key) {
            Some(r) => r,
            None    => return GateResult::Allow,
        };

        if !emitted_class.flows_to(row.lattice_class) {
            return GateResult::Deny(DataplaneGateReason::LatticeViolation);
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

    #[test]
    fn no_row_is_permissive() {
        let table = PartitionAuthTable::new();
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::TOP),
            GateResult::Allow
        );
    }

    #[test]
    fn bottom_emitter_always_flows_to_any_class() {
        let mut table = PartitionAuthTable::new();
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow { lattice_class: LatticeClass(0b1111), capability_mask: CapabilityBits::ALL },
        );
        // BOTTOM (0x0) flows_to anything
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass::BOTTOM),
            GateResult::Allow
        );
    }

    #[test]
    fn incompatible_lattice_class_denied() {
        let mut table = PartitionAuthTable::new();
        // Partition requires class 0b0001
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow { lattice_class: LatticeClass(0b0001), capability_mask: CapabilityBits::ALL },
        );
        // Class 0b0010 does NOT flow_to 0b0001 (0b0010 & 0b0001 != 0b0010)
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass(0b0010)),
            GateResult::Deny(DataplaneGateReason::LatticeViolation)
        );
    }

    #[test]
    fn compatible_lattice_class_allowed() {
        let mut table = PartitionAuthTable::new();
        // Partition requires class 0b0111
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::Demand },
            PartitionAuthRow { lattice_class: LatticeClass(0b0111), capability_mask: CapabilityBits::ALL },
        );
        // Class 0b0011 flows_to 0b0111 (0b0011 & 0b0111 == 0b0011)
        assert_eq!(
            table.gate(P1, OpcodeClass::Demand, LatticeClass(0b0011)),
            GateResult::Allow
        );
    }

    #[test]
    fn row_only_applies_to_matching_opcode_class() {
        let mut table = PartitionAuthTable::new();
        // Only restrict SetValue; Demand is unrestricted
        table.install(
            AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow { lattice_class: LatticeClass::TOP, capability_mask: CapabilityBits::ALL },
        );
        // Demand has no row → Allow even with non-TOP class
        assert_eq!(
            table.gate(P1, OpcodeClass::Demand, LatticeClass::BOTTOM),
            GateResult::Allow
        );
    }

    #[test]
    fn remove_row_restores_permissive() {
        let mut table = PartitionAuthTable::new();
        let key = AuthTableKey { partition_id: P1, opcode_class: OpcodeClass::SetValue };
        // Partition requires class 0b0001.
        // Emitter class 0b0010 cannot flow to 0b0001:
        //   flows_to(a, b) = (a & b) == a → (0b0010 & 0b0001) == 0b0010 → 0 == 2 → false
        table.install(key, PartitionAuthRow {
            lattice_class:   LatticeClass(0b0001),
            capability_mask: CapabilityBits::ALL,
        });
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass(0b0010)),
            GateResult::Deny(DataplaneGateReason::LatticeViolation)
        );
        table.remove(&key);
        // After removal: permissive regardless of emitter class
        assert_eq!(
            table.gate(P1, OpcodeClass::SetValue, LatticeClass(0b0010)),
            GateResult::Allow
        );
    }
}
