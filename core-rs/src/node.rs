//! Node and edge types. Computation rules for derived nodes.
//!
//! ## Two-layer decomposition (I8)
//!
//! Every `Node` carries two strictly separated layers:
//!
//! **Identity layer** — persistent; set at `NodeCreate`, never mutated thereafter:
//!   `id`, `typ`, `kind`
//!
//! **Interpretation layer** — versioned; mutated only by `SetValue`, `Reflect`,
//!   `Stabilize`. Each write increments `version`. Snapshots capture a frozen
//!   `(id, version)` pair.
//!
//! Every `Edge` carries:
//!
//! **Identity layer**: `id`, `typ`, `src`, `tgt`
//! **Port layer** (part of identity): `port: Option<Port>` — the typed interface
//!   through which the bond flows. Port kind is structural (part of the Dirac
//!   structure), not semantic (not subject to versioning).

use crate::{attr::Attrs, partition::DepKind, ternary::T, uid::Uid};
use smallvec::SmallVec;

// ── Node ─────────────────────────────────────────────────────────────────────

/// A computation node in the persistent graph.
///
/// # Layer separation (I8)
/// - Identity fields (`id`, `typ`, `kind`): set once at creation, never changed.
/// - Interpretation fields (`value`, `attrs`, `version`): mutated only through
///   `Node::set_value` / `Node::set_attrs_reflect`, which always bump `version`.
#[derive(Clone, Debug)]
pub struct Node {
    // ── Identity layer (persistent) ──────────────────────────────────────────
    pub id:   Uid,
    pub typ:  String,
    pub kind: NodeKind,

    // ── Interpretation layer (versioned) ─────────────────────────────────────
    pub value:   T,
    pub attrs:   Attrs,
    /// Monotone interpretation clock. Increments on every write to `value` or
    /// `attrs`. Never resets. A `(id, version)` pair addresses a unique
    /// historical interpretation.
    pub version: u64,
}

impl Node {
    pub fn input(id: Uid, typ: impl Into<String>) -> Self {
        Node {
            id, typ: typ.into(), kind: NodeKind::Input,
            value: T::Neg, attrs: Attrs::new(), version: 0,
        }
    }

    pub fn computed(id: Uid, typ: impl Into<String>, rule: ComputeRule) -> Self {
        Node {
            id, typ: typ.into(), kind: NodeKind::Computed(rule),
            value: T::Neg, attrs: Attrs::new(), version: 0,
        }
    }

    // ── Interpretation-layer writes (always bump version) ─────────────────

    /// Set the node's value. Returns the old value.
    /// Bumps `version` only when the value actually changes (I8: version
    /// tracks interpretation changes, not write events).
    /// This is the **only** correct way to mutate `value` outside the engine.
    pub fn set_value(&mut self, new: T) -> T {
        let old = self.value;
        if new != old {
            self.value = new;
            self.version += 1;
        }
        old
    }

    /// Reflect all ternary attrs (mv_neg each ternary Val). Bumps `version`.
    pub fn reflect_attrs(&mut self) {
        self.attrs.reflect_ternary();
        self.version += 1;
    }

    /// Returns the node's compute rule, if it is a `Computed` node.
    /// Returns `None` for `Input` nodes.
    pub fn compute_rule(&self) -> Option<&ComputeRule> {
        match &self.kind {
            NodeKind::Computed(r) => Some(r),
            NodeKind::Input => None,
        }
    }

    /// Apply mv_neg to the node's value and reflect all ternary attrs.
    /// Returns the old value. Bumps `version` once.
    pub fn reflect(&mut self) -> T {
        let old = self.value;
        self.value = self.value.mv_neg();
        self.attrs.reflect_ternary();
        self.version += 1;
        old
    }
}

/// How a node produces its output value.
#[derive(Clone, Debug)]
pub enum NodeKind {
    /// Value is set externally via SET_VALUE / PROPAGATE.
    Input,
    /// Value is derived from dependency values using a rule.
    Computed(ComputeRule),
}

/// Built-in computation rules over ternary dep values.
/// The rule receives `deps: &[T]` in edge-connection order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComputeRule {
    /// Output = deps[0] (pass-through).
    Identity,
    /// Output = ¬deps[0].
    MvNeg,
    /// Output = deps[0] ⊕ deps[1] (MV-add).
    MvAdd,
    /// Output = deps[0] ⊗ deps[1] (MV-mul).
    MvMul,
    /// Output = deps[0] ⊖ deps[1] (MV bounded difference).
    MvSub,
    /// Output = merge(deps[0]=base, deps[1]=left, deps[2]=right).
    Merge,
    /// Output = meet of all deps (AND over the chain order).
    MeetAll,
    /// Output = join of any dep (OR over the chain order).
    JoinAny,
    /// Output = Bochvar-add of all deps (Zero-infectious fold).
    BochvarFold,
    /// Output = deps[0] ⊗ deps[1] — sign of port power (effort × flow).
    /// deps[0] = effort value, deps[1] = flow value.
    PowerProduct,
}

impl ComputeRule {
    /// Evaluate this rule over the given dep values.
    /// Returns None if dep count is wrong for the rule.
    pub fn eval(&self, deps: &[T]) -> Option<T> {
        use ComputeRule::*;
        match self {
            Identity      => deps.first().copied(),
            MvNeg         => Some(deps.first()?.mv_neg()),
            MvAdd         => Some(deps.get(0)?.mv_add(*deps.get(1)?)),
            MvMul         => Some(deps.get(0)?.mv_mul(*deps.get(1)?)),
            MvSub         => Some(deps.get(0)?.mv_sub(*deps.get(1)?)),
            Merge         => Some(T::merge(*deps.get(0)?, *deps.get(1)?, *deps.get(2)?)),
            MeetAll       => Some(T::meet_all(deps)),
            JoinAny       => Some(T::join_any(deps)),
            BochvarFold   => Some(T::bochvar_fold_add(deps)),
            PowerProduct  => Some(deps.get(0)?.mv_mul(*deps.get(1)?)),
        }
    }

    /// Incremental delta: returns the new output value if it differs from the
    /// old output, or `None` if re-evaluation produces the same result.
    ///
    /// In the L₃ discrete domain the "diff" is simply a full re-evaluation —
    /// the savings come from skipping downstream propagation when output is
    /// unchanged. For richer domains, this is the hook for incremental computation.
    pub fn diff(&self, old_deps: &[T], new_deps: &[T]) -> Option<T> {
        let old_val = self.eval(old_deps)?;
        let new_val = self.eval(new_deps)?;
        if new_val != old_val { Some(new_val) } else { None }
    }
}

// ── Port ─────────────────────────────────────────────────────────────────────

/// The typed interface on a dependency edge.
///
/// Ports are part of the **identity layer** — they are structural (part of
/// the Dirac structure in port-Hamiltonian terms) and do not participate in
/// interpretation versioning.
///
/// # Port-Hamiltonian semantics
///
/// A bond carries a conjugate pair `(effort e, flow f)` where `e·f = power`.
/// In the ternary sign algebra: `sign(e) ⊗ sign(f) ∈ T` is the power sign.
///
/// | PortKind | Variable carried | Causal preference | ExecMode hint |
/// |----------|-----------------|-------------------|---------------|
/// | `Effort` | ∂H/∂x (costate) | effort-causal     | `Lazy`        |
/// | `Flow`   | dx/dt (rate)    | flow-causal       | `Eager`       |
/// | `Bond`   | both e and f    | resolved by Stab  | `Stabilizing` |
/// | `Signal` | pure info       | none              | `Eager`       |
///
/// When the power sign of a `Bond` port is `Zero`, the causal assignment is
/// indeterminate — the engine routes the region to `Stabilize`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Port {
    /// Name of the port (used for ordered dep indexing).
    pub name: String,
    /// Physical kind of variable flowing across this port.
    pub kind: PortKind,
}

impl Port {
    pub fn new(name: impl Into<String>, kind: PortKind) -> Self {
        Port { name: name.into(), kind }
    }

    pub fn signal(name: impl Into<String>) -> Self {
        Port { name: name.into(), kind: PortKind::Signal }
    }

    pub fn effort(name: impl Into<String>) -> Self {
        Port { name: name.into(), kind: PortKind::Effort }
    }

    pub fn flow(name: impl Into<String>) -> Self {
        Port { name: name.into(), kind: PortKind::Flow }
    }

    pub fn bond(name: impl Into<String>) -> Self {
        Port { name: name.into(), kind: PortKind::Bond }
    }

    /// The ExecMode this port kind prefers for the target node.
    pub fn preferred_mode(&self) -> Option<ExecMode> {
        match self.kind {
            PortKind::Effort  => Some(ExecMode::Lazy),
            PortKind::Flow    => Some(ExecMode::Eager),
            PortKind::Bond    => Some(ExecMode::Stabilizing),
            PortKind::Signal  => None,  // no preference — inherit node default
        }
    }
}

/// The physical kind of a port. Determines power semantics and causal direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PortKind {
    /// Effort variable (∂H/∂x, costate). Pull-causal: target demands from source.
    Effort,
    /// Flow variable (dx/dt, rate). Push-causal: source pushes to target eagerly.
    Flow,
    /// Full conjugate pair. Causal direction resolved by equivalence saturation.
    Bond,
    /// Pure information signal. No power semantics. Default untyped edge.
    #[default]
    Signal,
}

impl PortKind {
    /// Whether this port carries a power variable (effort or flow component).
    pub fn is_power(&self) -> bool {
        matches!(self, PortKind::Effort | PortKind::Flow | PortKind::Bond)
    }

    /// The power sign of this port given the value flowing through it.
    /// For Signal ports, returns None (no power semantics).
    pub fn power_sign(&self, val: T) -> Option<T> {
        if self.is_power() { Some(val) } else { None }
    }
}

// ── Edge ─────────────────────────────────────────────────────────────────────

/// A dependency edge from `src` to `tgt`.
/// Semantics: `tgt` reads from `src` (src is a dep of tgt).
///
/// The `dep` field is part of the **identity layer**: it encodes whether this
/// is an intra-partition or cross-partition dependency and carries the Dirac
/// structure of the connection (port kind, causal provenance).
/// `DepKind` does not change after edge creation.
#[derive(Clone, Debug)]
pub struct Edge {
    // ── Identity layer ────────────────────────────────────────────────────────
    pub id:    Uid,
    pub typ:   String,
    pub src:   Uid,
    pub tgt:   Uid,
    /// Dependency kind: Local(port) for intra-partition, Remote(meta) for cross-partition.
    pub dep:   DepKind,
    pub attrs: Attrs,
}

impl Edge {
    pub fn new(id: Uid, typ: impl Into<String>, src: Uid, tgt: Uid) -> Self {
        Edge { id, typ: typ.into(), src, tgt, dep: DepKind::Local(None), attrs: Attrs::new() }
    }

    pub fn with_port(mut self, port: Port) -> Self {
        self.dep = DepKind::Local(Some(port));
        self
    }

    /// The effective port kind — works for both local and remote deps.
    pub fn port_kind(&self) -> PortKind {
        self.dep.port_kind()
    }

    /// The port name, if set (local deps only).
    pub fn port_name(&self) -> Option<&str> {
        self.dep.port_name()
    }

    /// Whether this edge crosses a partition boundary.
    pub fn is_remote(&self) -> bool {
        self.dep.is_remote()
    }
}

// ── Execution mode ────────────────────────────────────────────────────────────

/// Per-node execution mode. Controls how propagation handles this node.
///
/// Corresponds to the causal assignment in port-Hamiltonian terms:
/// - `Eager` = flow-causal (source pushes; corresponds to `Flow` ports)
/// - `Lazy`  = effort-causal (demand-driven; corresponds to `Effort` ports)
/// - `Stabilizing` = causal direction unresolved; route Zero to e-graph
///   (corresponds to `Bond` ports with indeterminate power sign)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ExecMode {
    /// Extension: push delta eagerly to all subscribers on value change.
    #[default]
    Eager,
    /// Inhibition: demand-driven; do not recompute until DEMAND fires.
    Lazy,
    /// Reflection: local stabilization pending; route Zero to e-graph.
    Stabilizing,
}

/// Ordered list of dep UIDs (typically short — fits in a SmallVec).
pub type DepList = SmallVec<[Uid; 4]>;

// ── StabilizationConfig ───────────────────────────────────────────────────────

/// Node-local e-graph semantics for stabilization.
/// Controls which rewrite rules apply and what convergence means.
/// Stored in Engine's `node_stabilization` map; id-layer metadata.
#[derive(Clone, Debug)]
pub struct StabilizationConfig {
    /// Which rewrite ruleset applies when this node triggers Stabilize.
    pub strategy:           RewriteStrategy,
    /// Where may Stabilize effects travel from this node?
    pub domain:             StabilizationDomain,
    /// Semantic e-graph iteration cap.
    pub budget:             RewriteBudget,
    /// What constitutes convergence.
    pub convergence_policy: ConvergencePolicy,
}

impl Default for StabilizationConfig {
    fn default() -> Self {
        StabilizationConfig {
            strategy:           RewriteStrategy::FullL3,
            domain:             StabilizationDomain::Inherit,
            budget:             RewriteBudget::default(),
            convergence_policy: ConvergencePolicy::Canonical,
        }
    }
}

impl StabilizationConfig {
    /// Static default for use in const/reference contexts (avoids heap alloc).
    pub const DEFAULT: StabilizationConfig = StabilizationConfig {
        strategy:           RewriteStrategy::FullL3,
        domain:             StabilizationDomain::Inherit,
        budget:             RewriteBudget { max_iterations: 256 },
        convergence_policy: ConvergencePolicy::Canonical,
    };
}

/// Which rewrite ruleset applies when a node triggers Stabilize.
#[derive(Clone, Debug)]
pub enum RewriteStrategy {
    /// All 16 L₃ rules (default).
    FullL3,
    /// Compiled bitmask over the rule table.
    Restricted(u64),
}

impl Default for RewriteStrategy {
    fn default() -> Self { RewriteStrategy::FullL3 }
}

/// Where may Stabilize effects travel from this node?
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum StabilizationDomain {
    /// Stabilize cannot cross partition boundary.
    LocalOnly,
    /// May involve remote partitions if capability permits.
    Federated,
    /// Defer to partition's declared default.
    #[default]
    Inherit,
}

/// Semantic e-graph iteration budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RewriteBudget {
    pub max_iterations: usize,
}

impl Default for RewriteBudget {
    fn default() -> Self { RewriteBudget { max_iterations: 256 } }
}

/// What constitutes convergence for this node's stabilization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ConvergencePolicy {
    /// Saturation to a single canonical form (default).
    #[default]
    Canonical,
    /// Any non-Zero fixpoint suffices.
    Witness,
    /// Stop at budget limit; emit `TypedZero(Ambiguous)` if unresolved.
    BudgetExhausted,
}

// ── ExecutionPolicy ───────────────────────────────────────────────────────────

/// Scheduling and resource allocation policy for a node.
/// Stored in Engine's `node_execution` map; id-layer metadata.
#[derive(Clone, Debug, Default)]
pub struct ExecutionPolicy {
    /// Propagation queue ordering.
    pub queue_priority: QueuePriority,
    /// Behavior on `StepLimitExceeded` or partial convergence.
    pub retry_policy:   RetryPolicy,
}

impl ExecutionPolicy {
    /// Static default for use in const/reference contexts.
    pub const DEFAULT: ExecutionPolicy = ExecutionPolicy {
        queue_priority: QueuePriority::Normal,
        retry_policy:   RetryPolicy::Fail,
    };
}

/// Priority of a node in the propagation queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QueuePriority {
    /// Default scheduling order.
    #[default]
    Normal,
    /// Process before Normal-priority nodes.
    High,
    /// Process after Normal-priority nodes.
    Low,
}

/// Retry behavior on propagation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RetryPolicy {
    /// Fail immediately on the first error.
    #[default]
    Fail,
    /// Retry up to `max_attempts` times.
    Retry { max_attempts: usize },
}
