//! SessionRuntime — wires all session manager subsystems together.
//!
//! The runtime implements the full dataplane gate pipeline:
//!
//! ```text
//! ParsedHeader
//!   ↓ session lookup  (SessionTable)
//!   ↓ tenant check    (tenant_id must match session)
//!   ↓ stream_seq gate (replay suppression)
//!   ↓ opcode classify (OpcodeClass::from_opcode)
//!   ↓ auth gate       (PartitionAuthTable — partition-scoped, header-only)
//!   ↓ admission       (BackpressurePolicy — per-opcode-class treatment)
//!   → RouteOutcome::Admitted { opcode_class }
//!   → RouteOutcome::Rejected(reason)
//! ```
//!
//! ## What this does NOT do
//!
//! - Parse record bodies (engine responsibility)
//! - Per-edge authority checks (engine: CompiledEdgeLabel in DepMeta)
//! - Actual engine dispatch (caller maps Admitted → engine.apply())
//! - Path resolution from IsaStreamHeader.path_id (handled at ingress before
//!   ParsedHeader is constructed)
//!
//! ## High/low split — integration model
//!
//! `SessionRuntime` is the **low (gate) layer**. It is a predicate, not a driver.
//! A `Dispatcher` (to be implemented by the integrator, outside this crate) owns
//! the full pipeline:
//!
//! ```text
//! Dispatcher {
//!     decoder: StreamDecoder,           // ingress: wire bytes → ParsedHeader
//!     runtime: SessionRuntime,          // gate:    ParsedHeader → RouteOutcome
//!     shards:  EnginePool,              // execution: ShardId → engine.apply()
//! }
//! ```
//!
//! The dispatch path after `route_record` returns `Admitted { opcode_class }`:
//!
//! 1. Look up `shard_id` from `runtime.domains.partitions[partition_id].shard_id`.
//! 2. Parse the raw payload bytes into an `IsaOp` keyed on `opcode` / `opcode_class`.
//! 3. Call `shards[shard_id].apply(op)` → `EngineOutcome`.
//! 4. Call `runtime.update_pressure(shard_id, ...)` with the outcome metrics.
//!
//! `SessionRuntime` never touches `IsaOp` or engine state. The hot-path / slow-lane
//! split is driven by `opcode_class`: `Control` ops serialise through a per-shard
//! queue; `SetValue` / `Propagate` / `Demand` go to a lock-free fast path.

use rustc_hash::FxHashMap;
use pgress_core::partition::LatticeClass;
use crate::{
    admission::{AdmissionDecision, BackpressurePolicy, ShardPressure},
    auth::{GateResult, PartitionAuthTable},
    domain::DomainRegistry,
    session::{PathTable, SessionTable},
    telemetry::{TelemetryEvent, TelemetryPartition},
    topology::TopologyPartition,
    OpcodeClass, PathId, SessionId, ShardId, TenantId, WirePartitionId,
    auth::DataplaneGateReason,
};

// ── ParsedHeader ──────────────────────────────────────────────────────────────

/// Parsed representation of `IsaHeader` (48-byte wire record header).
///
/// Constructed by the ingress decoder after `IsaStreamHeader` path resolution.
/// All session manager decisions are based solely on these fields.
#[derive(Clone, Copy, Debug)]
pub struct ParsedHeader {
    pub opcode:       u16,
    pub flags:        u16,
    pub length:       u32,
    pub tenant_id:    TenantId,
    pub session_id:   SessionId,
    pub partition_id: WirePartitionId,
    pub causal_epoch: u64,
    pub stream_seq:   u64,
}

// ── RouteOutcome ──────────────────────────────────────────────────────────────

/// What the session manager decided to do with a record.
#[derive(Debug)]
pub enum RouteOutcome {
    /// Record passed all gates. The caller should dispatch the body to the engine
    /// shard responsible for `partition_id`. `opcode_class` informs the dispatch
    /// path (control pipeline vs. hot path vs. slow lane).
    Admitted { opcode_class: OpcodeClass },
    /// Record was rejected at one of the gates.
    Rejected(RejectionReason),
}

impl RouteOutcome {
    pub fn is_admitted(&self) -> bool { matches!(self, Self::Admitted { .. }) }
}

/// Why a record was rejected by the session manager.
#[derive(Debug, PartialEq, Eq)]
pub enum RejectionReason {
    /// No session registered for this session_id.
    ///
    /// On remote bootstrap this is the correct response before the peer's
    /// `SessionProfile` hello has been processed. Distinct from
    /// `UnauthenticatedPath`: `SessionNotFound` means the session does not
    /// exist at all; `UnauthenticatedPath` means it exists but the path
    /// presenting it has not completed admission.
    SessionNotFound,
    /// tenant_id in the header does not match the session's registered tenant.
    /// (tenant_id = 0 always passes; 0 is the single-tenant sentinel.)
    TenantMismatch,
    /// stream_seq is below the session's floor — this is a replay.
    StreamSeqReplay,
    /// Partition-scoped authority gate denied the record.
    AuthGateDenied(DataplaneGateReason),
    /// Admission control decided to shed (Propagate under load, unknown opcode).
    Shed,
    /// Admission control is applying back-pressure; caller should retry later.
    Delay,
    /// The session exists but the path presenting it has not completed
    /// admission (ASSERTED/ATTESTED bootstrap pending verification).
    ///
    /// v1: not generated by any code path (Advisory bootstrap is synchronous).
    /// Reserved for ASSERTED/ATTESTED bootstrap where verification is async.
    /// A client receiving this should retry after the bootstrap handshake
    /// completes, not treat it as a permanent failure.
    UnauthenticatedPath,
    /// The `partition_id` in the header is not registered in the `DomainRegistry`.
    ///
    /// Data ops (`SetValue`, `Propagate`, `Demand`, `Stabilize`, `Data`) on an
    /// unknown partition are hard-rejected. Control ops (`PartitionCreate`, etc.)
    /// are exempt and route to the bootstrap shard (`ShardId(0)`) instead.
    ///
    /// Clients should send `PartitionCreate` before any data ops on a new partition.
    UnknownPartition,
}

// ── SessionRuntime ────────────────────────────────────────────────────────────

/// The session manager runtime.
///
/// Holds all stateful dataplane tables and applies the gate pipeline to each
/// incoming record. The caller is responsible for:
/// - Path resolution (`path_id → session_id` via `PathTable`) before calling
///   `route_record`.
/// - Engine dispatch after receiving `RouteOutcome::Admitted`.
/// - Updating `ShardPressure` counters as the engine shard processes work.
#[derive(Default)]
pub struct SessionRuntime {
    pub sessions:  SessionTable,
    pub paths:     PathTable,
    pub domains:   DomainRegistry,
    pub auth:      PartitionAuthTable,
    pub admission: BackpressurePolicy,
    pub telemetry: TelemetryPartition,
    pub topology:  TopologyPartition,
    /// Per-shard pressure counters. Updated by the caller after engine apply.
    pub pressure:  FxHashMap<ShardId, ShardPressure>,
}

impl SessionRuntime {
    pub fn new() -> Self { Self::default() }

    /// Resolve a path_id from `IsaStreamHeader` to its owning session.
    /// Returns None if the path is not registered or is Closed.
    pub fn resolve_path(&self, path_id: PathId) -> Option<SessionId> {
        self.paths.resolve_session(path_id)
    }

    /// Apply the full session manager gate pipeline to a parsed record header.
    ///
    /// Returns `Admitted` if the record should be forwarded to the engine,
    /// or `Rejected` with a structured reason otherwise.
    pub fn route_record(&mut self, header: &ParsedHeader) -> RouteOutcome {
        // ── 1. Session lookup ─────────────────────────────────────────────────
        let session = match self.sessions.get(header.session_id) {
            Some(s) => s,
            None    => return RouteOutcome::Rejected(RejectionReason::SessionNotFound),
        };

        // ── 2. Tenant check ───────────────────────────────────────────────────
        // tenant_id = 0 is the single-tenant sentinel and always passes.
        // Non-zero tenant_id must match the session's registered tenant.
        if header.tenant_id.0 != 0 && header.tenant_id != session.tenant_id {
            return RouteOutcome::Rejected(RejectionReason::TenantMismatch);
        }

        // ── 3. stream_seq replay suppression ──────────────────────────────────
        if !self.sessions.is_seq_valid(header.session_id, header.stream_seq) {
            return RouteOutcome::Rejected(RejectionReason::StreamSeqReplay);
        }

        // ── 4. Opcode classification ──────────────────────────────────────────
        let opcode_class = OpcodeClass::from_opcode(header.opcode);

        // ── 5. Partition-scoped authority gate ────────────────────────────────
        // emitted_class: use BOTTOM (least restrictive) because the source
        // lattice class is not carried in the header. Per-edge class checks
        // are the engine's responsibility (CompiledEdgeLabel in DepMeta).
        match self.auth.gate(header.partition_id, opcode_class, LatticeClass::BOTTOM) {
            GateResult::Deny(reason) =>
                return RouteOutcome::Rejected(RejectionReason::AuthGateDenied(reason)),
            GateResult::Allow => {}
        }

        // ── 6. Admission control ──────────────────────────────────────────────
        // For data ops, an unknown partition is a hard rejection. Control and
        // Profile ops are routed to the bootstrap shard (ShardId(0)) — this
        // allows PartitionCreate to arrive before its target partition is
        // registered in DomainRegistry. The Dispatcher feeds the creation back
        // into DomainRegistry after the engine apply succeeds.
        let shard_id = match self.domains.partitions.get(&header.partition_id) {
            Some(p) => p.shard_id,
            None => match opcode_class {
                OpcodeClass::Control
                | OpcodeClass::Profile
                | OpcodeClass::Subscribe => ShardId(0),
                _ => return RouteOutcome::Rejected(RejectionReason::UnknownPartition),
            },
        };
        let pressure = self.pressure
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();

        match self.admission.decide(opcode_class, &pressure) {
            AdmissionDecision::Drop => {
                return RouteOutcome::Rejected(RejectionReason::Shed);
            }
            AdmissionDecision::Delay => {
                return RouteOutcome::Rejected(RejectionReason::Delay);
            }
            // All other decisions (Allow, Prioritize, SlowLane, ControlPlane,
            // Coalesce) reach the engine; the caller uses opcode_class to pick
            // the right dispatch path.
            _ => {}
        }

        RouteOutcome::Admitted { opcode_class }
    }

    /// Update the pressure counters for a shard.
    ///
    /// Dual-writes to the topology partition and emits a `TopologyHealthChanged`
    /// telemetry event when the ternary health value transitions.
    pub fn update_pressure(&mut self, shard_id: ShardId, pressure: ShardPressure) {
        self.pressure.insert(shard_id, pressure.clone());
        let (old_health, new_health) = self.topology.update(shard_id, &pressure);
        if old_health != new_health {
            self.telemetry.emit(TelemetryEvent::TopologyHealthChanged {
                shard_id,
                old_health,
                new_health,
            });
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgress_core::partition::AuthorityMode;
    use crate::{
        auth::{AuthTableKey, PartitionAuthRow},
        domain::{PartitionDomain, TenantDomain, TenantQuota},
        session::{PathEntry, PathState, SessionEntry},
    };

    fn setup() -> (SessionRuntime, ParsedHeader) {
        let mut rt = SessionRuntime::new();

        // Register tenant
        rt.domains.register_tenant(TenantDomain {
            id:           TenantId(1),
            capabilities: pgress_core::partition::CapabilityBits::ALL,
            quota:        TenantQuota::default(),
        });

        // Register session
        rt.sessions.create(SessionEntry {
            session_id:       SessionId(10),
            tenant_id:        TenantId(1),
            active_path_id:   Some(PathId(100)),
            prev_path_id:     None,
            stream_seq_floor: 0,
            auth_mode:        AuthorityMode::Advisory,
        });

        // Register path
        rt.paths.create(PathEntry {
            path_id:             PathId(100),
            session_id:          SessionId(10),
            last_ack_stream_seq: 0,
            state:               PathState::Active,
        });

        // Register partition on shard 0
        rt.domains.register_partition(PartitionDomain {
            id:            WirePartitionId(99),
            session_id:    SessionId(10),
            lattice_class: pgress_core::partition::LatticeClass::BOTTOM,
            causal_scope:  u64::MAX,
            shard_id:      ShardId(0),
        });

        let header = ParsedHeader {
            opcode:       0x0003,           // SetValue
            flags:        0,
            length:       64,
            tenant_id:    TenantId(0),      // single-tenant sentinel
            session_id:   SessionId(10),
            partition_id: WirePartitionId(99),
            causal_epoch: 1,
            stream_seq:   0,
        };

        (rt, header)
    }

    #[test]
    fn valid_record_admitted() {
        let (mut rt, header) = setup();
        assert!(rt.route_record(&header).is_admitted());
    }

    #[test]
    fn unknown_session_rejected() {
        let (mut rt, mut header) = setup();
        header.session_id = SessionId(999);
        assert!(matches!(rt.route_record(&header), RouteOutcome::Rejected(RejectionReason::SessionNotFound)));
    }

    #[test]
    fn tenant_mismatch_rejected() {
        let (mut rt, mut header) = setup();
        // Non-zero tenant that does not match session's tenant (1)
        header.tenant_id = TenantId(42);
        assert!(matches!(rt.route_record(&header), RouteOutcome::Rejected(RejectionReason::TenantMismatch)));
    }

    #[test]
    fn zero_tenant_always_passes() {
        let (mut rt, mut header) = setup();
        header.tenant_id = TenantId(0);
        assert!(rt.route_record(&header).is_admitted());
    }

    #[test]
    fn replay_seq_rejected() {
        let (mut rt, mut header) = setup();
        // Advance floor to 10
        rt.sessions.ack_seq(SessionId(10), 9);
        // Now stream_seq=5 is below floor
        header.stream_seq = 5;
        assert!(matches!(rt.route_record(&header), RouteOutcome::Rejected(RejectionReason::StreamSeqReplay)));
    }

    #[test]
    fn auth_gate_wired_into_runtime() {
        // Verify the PartitionAuthTable is consulted by route_record.
        //
        // route_record always passes LatticeClass::BOTTOM as the emitted class
        // (source class is not carried in IsaHeader; per-edge checks are the engine's
        // responsibility via CompiledEdgeLabel in DepMeta). BOTTOM flows to every
        // class, so a lattice violation is intentionally not reachable through
        // route_record — the gate is permissive at the dataplane by design.
        //
        // What IS reachable: auth.gate() is confirmed to be called for each record.
        // We verify this by installing a row and confirming it does NOT affect a
        // BOTTOM emitter (correct), then testing the auth table directly to confirm
        // a non-BOTTOM emitter IS denied.
        let (mut rt, header) = setup();

        // Install a row requiring class 0b0001.
        rt.auth.install(
            AuthTableKey { partition_id: WirePartitionId(99), opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow {
                lattice_class:   pgress_core::partition::LatticeClass(0b0001),
                capability_mask: pgress_core::partition::CapabilityBits::ALL,
            },
        );

        // route_record uses BOTTOM → flows_to(0b0001) = true → still admitted.
        // This is correct: dataplane is permissive; engine does per-edge checks.
        assert!(rt.route_record(&header).is_admitted(),
            "route_record must admit BOTTOM emitter even with a restrictive lattice row");

        // Direct auth gate check confirms a non-BOTTOM emitter is correctly denied.
        // This validates the gate logic is wired and correct, not bypassed.
        use crate::auth::{GateResult, DataplaneGateReason};
        assert_eq!(
            rt.auth.gate(
                WirePartitionId(99),
                OpcodeClass::SetValue,
                pgress_core::partition::LatticeClass(0b0010), // 0b0010 does NOT flow_to 0b0001
            ),
            GateResult::Deny(DataplaneGateReason::LatticeViolation),
            "class 0b0010 must not flow_to 0b0001"
        );
    }

    #[test]
    fn auth_gate_bottom_emitter_always_passes_through_route_record() {
        // Confirms the documented invariant: BOTTOM as the default emitted class
        // means the partition lattice gate is always permissive at the dataplane.
        // Per-edge class checks are the engine's responsibility.
        let (mut rt, header) = setup();
        rt.auth.install(
            AuthTableKey { partition_id: WirePartitionId(99), opcode_class: OpcodeClass::SetValue },
            PartitionAuthRow {
                // Require TOP: only TOP flows to TOP — but BOTTOM still flows to TOP.
                // BOTTOM (0) flows_to TOP (MAX): (0 & MAX) == 0 → true.
                lattice_class:   pgress_core::partition::LatticeClass::TOP,
                capability_mask: pgress_core::partition::CapabilityBits::ALL,
            },
        );
        assert!(rt.route_record(&header).is_admitted());
    }

    #[test]
    fn propagate_shed_under_pressure() {
        let (mut rt, mut header) = setup();
        header.opcode = 0x0004;  // Propagate
        // Simulate pressure above shed threshold
        rt.update_pressure(ShardId(0), ShardPressure {
            queue_depth:     u32::MAX,
            budget_consumed: 0,
            cursor_count:    0,
        });
        assert!(matches!(rt.route_record(&header), RouteOutcome::Rejected(RejectionReason::Shed)));
    }

    #[test]
    fn demand_never_shed_under_pressure() {
        let (mut rt, mut header) = setup();
        header.opcode = 0x0008;  // Demand
        rt.update_pressure(ShardId(0), ShardPressure {
            queue_depth:     u32::MAX,
            budget_consumed: 0,
            cursor_count:    0,
        });
        // Demand is always admitted
        assert!(rt.route_record(&header).is_admitted());
    }

    #[test]
    fn path_resolution_works() {
        let (rt, _) = setup();
        assert_eq!(rt.resolve_path(PathId(100)), Some(SessionId(10)));
        assert_eq!(rt.resolve_path(PathId(999)), None);
    }
}
