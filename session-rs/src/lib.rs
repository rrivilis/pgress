//! pgress-session — session manager layer above pgress-core.
//!
//! Implements the dataplane between the wire and the engine:
//!
//! ```text
//! wire record
//!   ↓ parse IsaStreamHeader (path_id → session_id)
//!   ↓ parse IsaHeader (session routing fields)
//!   ↓ session lookup
//!   ↓ tenant check
//!   ↓ stream_seq replay suppression
//!   ↓ authority / capability gate  (partition-scoped; header-only)
//!   ↓ admission control / backpressure
//!   ↓ route to engine shard
//! ```
//!
//! ## Domain hierarchy
//!
//! ```text
//! tenant  (capability ceiling, aggregate quota)
//!   └─ session  (stream continuity, auth mode, backpressure scope)
//!        └─ partition  (causal isolation, PartitionAuthTable scope, shard assignment)
//!             └─ shard  (engine instance, quiescence guarantee)
//! ```
//!
//! Authority is strictly bounded downward:
//! `tenant.capabilities ⊇ session.effective_caps ⊇ partition.capability_mask ⊇ edge.label.capability_bits`

pub mod opcode;
pub mod decode;
pub mod domain;
pub mod session;
pub mod auth;
pub mod profile;
pub mod admission;
pub mod recovery;
pub mod telemetry;
pub mod topology;
pub mod runtime;
pub mod dispatch;
pub mod encode;

// ── Wire-format ID newtypes (u64 from IsaHeader / IsaStreamHeader) ────────────
//
// These are deliberately distinct from pgress_core::partition::PartitionId (uuid::Uuid).
// The session manager works with the wire representation; translation to engine types
// happens at the engine shard boundary.

/// Tenant scope identifier from the wire.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct TenantId(pub u64);

/// Stable logical session identifier. Survives path migration and reconnect.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct SessionId(pub u64);

/// Ephemeral transport path identifier. Changes on reconnect; does NOT reset stream_seq.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct PathId(pub u64);

/// Partition identifier as it appears in IsaHeader (u64 wire form).
/// Distinct from `pgress_core::partition::PartitionId` which is `uuid::Uuid`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct WirePartitionId(pub u64);

/// Engine shard identifier.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct ShardId(pub u64);

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use opcode::OpcodeClass;
pub use domain::{
    TenantDomain, SessionDomain, PartitionDomain, ShardDomain,
    DomainRegistry, TenantQuota, SessionQuota,
};
pub use session::{SessionEntry, PathEntry, PathState, SessionTable, PathTable, SessionError};
pub use auth::{PartitionAuthRow, PartitionAuthTable, AuthTableKey, GateResult, DataplaneGateReason};
pub use profile::{SessionProfile, TrustLevel, ProfileId, ProfileError};
pub use admission::{AdmissionDecision, BackpressurePolicy, ShardPressure};
pub use recovery::{WorkCursor, EngineOutcome, FailureAction};
pub use decode::{StreamDecoder, StreamPreamble, RecordAction, IngressError};
pub use telemetry::{TelemetryPartition, TelemetryEvent, TelemetryNodeName, PARTITION_TELEMETRY};
pub use topology::{TopologyPartition, TopologyNode, TopologyNodeName, PARTITION_TOPOLOGY};
pub use domain::{ShardFabricAddr, FabricScope};
pub use runtime::{SessionRuntime, ParsedHeader, RouteOutcome, RejectionReason};
pub use dispatch::{Dispatcher, DispatchOutcome, DispatchError, EnginePool, parse_payload};
pub use encode::{encode_record, encode_stream_preamble, write_payload, opcode_for, RecordContext};
