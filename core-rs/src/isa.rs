//! ISA — the graph primitive instruction set.
//!
//! The ~10 ISA operations are the only way to modify graph state.
//! They map directly onto the three primitives (I1):
//!   Extension  → NODE_CREATE, EDGE_CONNECT, SUBSCRIBE, SET_VALUE, PROPAGATE
//!   Inhibition → DEL_NODE, DEL_EDGE, DEMAND (lazy mode)
//!   Reflection → REFLECT, STABILIZE
//!
//! ## Layer contract (I8)
//!
//! Each operation touches exactly one layer:
//!
//! | Operation               | id_layer | interp_layer |
//! |-------------------------|----------|--------------|
//! | NodeCreate, EdgeConnect | grows    | initialised at version=0 |
//! | Subscribe               | grows    | — |
//! | SetValue                | —        | mutated (version++) |
//! | Propagate               | —        | may mutate (version++) |
//! | DelNode, DelEdge        | shrinks  | — (survivors unchanged) |
//! | Demand                  | —        | may mutate (version++) |
//! | SetMode                 | grows    | — (mode is id-layer metadata) |
//! | Reflect                 | —        | mutated (version++) |
//! | Stabilize               | —        | mutated (version++) |

use crate::{
    attr::Attrs,
    node::{ComputeRule, ExecMode, ExecutionPolicy, NodeKind, Port, StabilizationConfig},
    partition::{AuthorityRoot, CausalScope, DepKind, EdgeLabel, LatticeClass, PartitionId},
    region::{CompilePolicy, RegionBoundary, StabilityContract},
    ternary::T,
    uid::Uid,
};

/// An ISA operation. This is the primitive unit of graph mutation.
#[derive(Debug, Clone)]
pub enum IsaOp {
    // ── Extension ────────────────────────────────────────────────────────────

    /// Add a computation node.
    /// Touches: id_layer (new node); interp_layer (initialised, version=0).
    NodeCreate {
        id:    Uid,
        typ:   String,
        rule:  NodeKind,
        attrs: Attrs,
    },

    /// Connect src → tgt as a dependency edge (tgt reads from src).
    /// `dep` encodes whether this is intra-partition (Local) or cross-partition (Remote)
    /// and carries the Dirac-structure type of the connection.
    /// Touches: id_layer only (new edge + subscription).
    EdgeConnect {
        id:  Uid,
        typ: String,
        src: Uid,
        tgt: Uid,
        /// Dependency kind: Local(port) for intra-partition, Remote(meta) for cross-partition.
        dep: DepKind,
    },

    /// Explicitly register a push subscription (src pushes to subscriber).
    /// Usually derived from EdgeConnect, but can be set independently.
    /// Touches: id_layer only.
    Subscribe {
        source:     Uid,
        subscriber: Uid,
    },

    /// Set the value of an input node. Enqueues a push for the node.
    /// Touches: interp_layer (value + version++).
    SetValue {
        node: Uid,
        val:  T,
    },

    /// Eagerly propagate a node's current value to its subscribers.
    /// Touches: interp_layer of downstream nodes (value + version++ if changed).
    Propagate {
        node: Uid,
    },

    // ── Inhibition ───────────────────────────────────────────────────────────

    /// Remove a node (and all its incident edges) from the graph.
    /// Touches: id_layer (removes node + dangling edges).
    /// Interp of surviving nodes is unchanged before propagation.
    DelNode {
        id: Uid,
    },

    /// Remove a dependency edge.
    /// Touches: id_layer only.
    DelEdge {
        id: Uid,
    },

    /// Request demand-driven evaluation of a node (pull mode).
    /// The node and its transitive pending deps are pulled bottom-up.
    /// Touches: interp_layer of pulled nodes (version++ if value changes).
    Demand {
        node: Uid,
    },

    /// Set a node's execution mode.
    /// Mode is id-layer metadata (structural causal assignment), not versioned.
    SetMode {
        node: Uid,
        mode: ExecMode,
    },

    // ── Reflection ───────────────────────────────────────────────────────────

    /// Flip ternary polarity of a node's value and all its ternary attrs.
    /// Touches: interp_layer (value = mv_neg(value), attrs reflected, version++).
    Reflect {
        node: Uid,
    },

    /// Run e-graph equivalence saturation over a region (set of nodes).
    /// If no region specified, stabilizes all conflicted (Zero) nodes.
    /// Touches: interp_layer of conflicted nodes in region (version++ if resolved).
    Stabilize {
        region: Option<Vec<Uid>>,
    },

    // ── Authority / partition (id-layer only) ─────────────────────────────────

    /// Declare a new partition with its authority root and lattice position.
    /// Triggers `AuthorityCache` recompilation for all partition pairs.
    /// Touches: id_layer (grows partition registry).
    PartitionCreate {
        id:             PartitionId,
        authority_root: AuthorityRoot,
        lattice_class:  LatticeClass,
        causal_domain:  CausalScope,
    },

    /// Bind a node to a partition.
    /// Triggers `CompiledEdgeLabel` recompilation for all edges incident to node.
    /// Touches: id_layer (grows node→partition binding map).
    PartitionBind {
        node:      Uid,
        partition: PartitionId,
    },

    /// Mutate a partition's lattice class and causal domain.
    /// Triggers `AuthorityCache` recompilation for affected partition pairs.
    /// Touches: id_layer (mutates partition declaration).
    SetPartitionAuthority {
        partition:     PartitionId,
        lattice_class: LatticeClass,
        causal_domain: CausalScope,
    },

    /// Set the authority edge-label for a (source, target) dep.
    /// Compiles `EdgeLabel` → `CompiledEdgeLabel` and stores in `DepMeta`.
    /// Touches: id_layer (mutates dep metadata, no propagation enqueued).
    SetEdgeLabel {
        source: Uid,
        target: Uid,
        label:  EdgeLabel,
    },

    /// Set the stabilization config for a node (e-graph semantics, id-layer).
    /// Touches: id_layer only.
    SetStabilizationConfig {
        node:   Uid,
        config: StabilizationConfig,
    },

    /// Set the execution policy for a node (scheduling, id-layer).
    /// Touches: id_layer only.
    SetExecutionPolicy {
        node:   Uid,
        policy: ExecutionPolicy,
    },

    // ── Region compilation (id-layer: freezes topology) ───────────────────────

    /// Declare a subgraph region for compilation into a sparse circuit.
    ///
    /// `root` is the anchor node used as the cache key.
    /// `boundary` specifies how the member set is expanded from root.
    /// `stability` controls epoch-tracking and invalidation on topology rewrites.
    /// `compile` controls when the compiled artifact is produced.
    ///
    /// Touches: id_layer only (adds to `RegionArtifactCache`; no value changes).
    RegionDeclare {
        root:       Uid,
        boundary:   RegionBoundary,
        stability:  StabilityContract,
        compile:    CompilePolicy,
    },
}

impl IsaOp {
    // ── Convenience constructors: nodes ──────────────────────────────────────

    /// Create a simple input node with no attrs.
    pub fn input_node(id: Uid, typ: impl Into<String>) -> IsaOp {
        IsaOp::NodeCreate {
            id, typ: typ.into(),
            rule: NodeKind::Input,
            attrs: Attrs::new(),
        }
    }

    /// Create a computed node with a built-in rule.
    pub fn computed_node(id: Uid, typ: impl Into<String>, rule: ComputeRule) -> IsaOp {
        IsaOp::NodeCreate {
            id, typ: typ.into(),
            rule: NodeKind::Computed(rule),
            attrs: Attrs::new(),
        }
    }

    // ── Convenience constructors: edges ──────────────────────────────────────

    /// Plain Signal dependency edge (no power semantics). Default untyped edge.
    pub fn dep_edge(id: Uid, src: Uid, tgt: Uid) -> IsaOp {
        IsaOp::EdgeConnect { id, typ: "dep".into(), src, tgt, dep: DepKind::Local(None) }
    }

    /// Effort port edge: src computes ∂H/∂x (costate), tgt reads it pull-style.
    /// Target node prefers `Lazy` (effort-causal) ExecMode.
    pub fn effort_edge(id: Uid, src: Uid, tgt: Uid, port_name: impl Into<String>) -> IsaOp {
        IsaOp::EdgeConnect {
            id, typ: "effort".into(), src, tgt,
            dep: DepKind::Local(Some(Port::effort(port_name))),
        }
    }

    /// Flow port edge: src computes dx/dt (rate), pushes eagerly to tgt.
    /// Target node prefers `Eager` (flow-causal) ExecMode.
    pub fn flow_edge(id: Uid, src: Uid, tgt: Uid, port_name: impl Into<String>) -> IsaOp {
        IsaOp::EdgeConnect {
            id, typ: "flow".into(), src, tgt,
            dep: DepKind::Local(Some(Port::flow(port_name))),
        }
    }

    /// Bond edge: full conjugate pair (effort + flow). Causal direction
    /// is resolved by Stabilize when the power sign is Zero.
    pub fn bond_edge(id: Uid, src: Uid, tgt: Uid, port_name: impl Into<String>) -> IsaOp {
        IsaOp::EdgeConnect {
            id, typ: "bond".into(), src, tgt,
            dep: DepKind::Local(Some(Port::bond(port_name))),
        }
    }

    /// Signal edge with an explicit port name (for ordered dep indexing).
    pub fn named_dep_edge(id: Uid, src: Uid, tgt: Uid, port_name: impl Into<String>) -> IsaOp {
        IsaOp::EdgeConnect {
            id, typ: "dep".into(), src, tgt,
            dep: DepKind::Local(Some(Port::signal(port_name))),
        }
    }

    /// Remote dependency edge: carries cross-partition evidence.
    pub fn remote_dep_edge(id: Uid, src: Uid, tgt: Uid, remote: crate::partition::RemoteDep) -> IsaOp {
        IsaOp::EdgeConnect {
            id, typ: "remote-dep".into(), src, tgt,
            dep: DepKind::Remote(remote),
        }
    }

    // ── Layer classification ──────────────────────────────────────────────────

    /// Which primitive class this operation belongs to (for event logging / I1).
    pub fn primitive_class(&self) -> &'static str {
        match self {
            IsaOp::NodeCreate { .. }
            | IsaOp::EdgeConnect { .. }
            | IsaOp::Subscribe { .. }
            | IsaOp::SetValue { .. }
            | IsaOp::Propagate { .. }  => "extension",
            IsaOp::DelNode { .. }
            | IsaOp::DelEdge { .. }
            | IsaOp::Demand { .. }
            | IsaOp::SetMode { .. }    => "inhibition",
            IsaOp::Reflect { .. }
            | IsaOp::Stabilize { .. }  => "reflection",
            // Authority / partition ops are id-layer mutations (extension of the id lattice).
            IsaOp::PartitionCreate { .. }
            | IsaOp::PartitionBind { .. }
            | IsaOp::SetPartitionAuthority { .. }
            | IsaOp::SetEdgeLabel { .. }
            | IsaOp::SetStabilizationConfig { .. }
            | IsaOp::SetExecutionPolicy { .. }
            | IsaOp::RegionDeclare { .. }       => "extension",
        }
    }

    /// Which layer(s) this operation primarily touches (for audit / I8).
    pub fn touches_layers(&self) -> LayerTouch {
        match self {
            IsaOp::NodeCreate { .. }
            | IsaOp::EdgeConnect { .. }
            | IsaOp::Subscribe { .. }  => LayerTouch::IdOnly,
            IsaOp::SetValue { .. }
            | IsaOp::Reflect { .. }
            | IsaOp::Stabilize { .. }  => LayerTouch::InterpOnly,
            IsaOp::Propagate { .. }
            | IsaOp::Demand { .. }     => LayerTouch::InterpOnly,  // id unchanged; interp may change
            IsaOp::DelNode { .. }
            | IsaOp::DelEdge { .. }    => LayerTouch::IdOnly,      // survivors' interp unchanged
            IsaOp::SetMode { .. }      => LayerTouch::IdOnly,      // mode is id-layer metadata
            // All authority / partition ops touch only the id layer.
            IsaOp::PartitionCreate { .. }
            | IsaOp::PartitionBind { .. }
            | IsaOp::SetPartitionAuthority { .. }
            | IsaOp::SetEdgeLabel { .. }
            | IsaOp::SetStabilizationConfig { .. }
            | IsaOp::SetExecutionPolicy { .. }
            | IsaOp::RegionDeclare { .. }       => LayerTouch::IdOnly,
        }
    }
}

/// Which layer(s) a given ISA operation primarily touches (I8 audit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerTouch {
    /// Only the identity layer is mutated (topology changes).
    IdOnly,
    /// Only the interpretation layer is mutated (values/versions change).
    InterpOnly,
    /// Both layers are mutated in one atomic step (should be rare).
    Both,
}
