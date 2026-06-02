//! pgrs-core — the PGRS computation substrate.
//!
//! Implements the ISA over a persistent attributed graph with three execution modes:
//!   Extension  → eager monotone forward propagation
//!   Inhibition → lazy demand-driven (non-monotone retraction)
//!   Reflection → e-graph equivalence saturation (Bochvar boundary resolution)
//!
//! ## Distribution model
//!
//! Cross-partition communication is typed and disciplined:
//!   time        → product timestamps + Frontier + VectorClock
//!   partition   → PartitionId, ZeroKind, DepKind (Local/Remote)
//!   delta       → ProjectionMask, ShapeMask, AttrPathSet, Delta
//!   sensitivity → Sensitivity enum + VersionedState<T> + triggers_recompute
//!   mask        → MaskRegistry (interned ProjectionMasks)
//!   stream      → RemoteStream, RemoteEvent, Scope, emit()

pub mod ternary;
pub mod uid;
pub mod attr;
pub mod node;
pub mod graph;
pub mod region;
pub mod deps;
pub mod value_store;
pub mod propagate;
pub mod egraph;
pub mod isa;
pub mod engine;

// ── Distribution model ────────────────────────────────────────────────────────
pub mod time;
pub mod partition;
pub mod delta;
pub mod sensitivity;
pub mod mask;
pub mod stream;

// Re-export the most commonly used types at crate root
pub use ternary::{T, PropState};
pub use uid::Uid;
pub use attr::{Attrs, Val};
pub use node::{Node, Edge, NodeKind, ComputeRule, ExecMode, Port, PortKind};
pub use graph::Graph;
pub use deps::{DepRegistry, DepMeta};
pub use propagate::{PropEngine, PropEvent, PropError, PropStats, WorkItem};
pub use isa::IsaOp;
pub use engine::{Engine, EngineError};

// Distribution model re-exports
pub use time::{TimeDim, TimeDimSet, ProductTime, Frontier, VectorClock};
pub use partition::{PartitionId, ZeroKind, DepKind, RemoteDep};
pub use delta::{Delta, ProjectionMask, ShapeMask, AttrPathSet};
pub use sensitivity::{Sensitivity, VersionedState, StateClass};
pub use mask::{MaskId, MaskRegistry};
pub use stream::{RemoteStream, RemoteEvent, RemotePayload, Scope, Subscription, emit};

// Region compilation re-exports
pub use region::{
    RegionBoundary, StabilityContract, CompilePolicy,
    CompiledRegion, RegionArtifactCache, AdjEpoch,
    run_compiled_region,
};
