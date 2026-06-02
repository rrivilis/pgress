//! Engine — the top-level ISA dispatcher.
//!
//! The Engine owns the three mutable substrates:
//!   graph  — structural attributed graph (nodes + edges)
//!   deps   — dependency/subscription registry (propagation view)
//!   prop   — propagation work queue + event log
//!
//! ## Layer contract (I8)
//!
//! Every write to the interpretation layer (`value`, `attrs`) must go through
//! `Node::set_value` or `Node::reflect`, which bump `version`. The identity
//! layer (`id`, `typ`, `kind`, `port`) is never mutated after `NodeCreate` /
//! `EdgeConnect`.
//!
//! ## Port-mode inference
//!
//! When `EdgeConnect` carries a typed `Port`, the engine infers the target
//! node's preferred `ExecMode` from `port.preferred_mode()`:
//!   Effort → Lazy, Flow → Eager, Bond → Stabilizing, Signal → no change.
//! The inferred mode is applied only if the target is still at the default
//! (`Eager`). An explicit `SetMode` always takes precedence.

use std::collections::HashMap;
use rustc_hash::FxHashMap;
use crate::{
    attr::Val,
    deps::{DepMeta, DepRegistry},
    egraph::{PortHistory, build_section_region, stabilize_section, PortCompliance, StabZeroKind},
    graph::Graph,
    isa::IsaOp,
    node::{Edge, ExecMode, ExecutionPolicy, Node, NodeKind, Port, StabilizationConfig},
    partition::{
        AuthorityMode, DepKind, PartitionDecl, PartitionId, PartitionRegistry,
        UnboundPolicy, ZeroKind,
    },
    propagate::{AuthContext, PropEngine, PropError, PropEvent, WorkItem},
    region::{
        AdjEpoch, CompilePolicy, CompiledRegion, RegionArtifactCache,
        RegionBoundary, build_region_inv, run_compiled_region,
    },
    ternary::T,
    time::{EventStamp, ProductTime, TimeDim},
    uid::Uid,
    value_store::ValueStore,
};

/// Default max propagation steps per `apply` call (safety against cycles).
const DEFAULT_MAX_STEPS: usize = 100_000;

/// The main engine: owns graph + deps + propagation queue.
#[derive(Default)]
pub struct Engine {
    pub graph:       Graph,
    pub deps:        DepRegistry,
    prop:            PropEngine,
    pub history:     Vec<EngineEvent>,
    pub max_steps:   usize,
    /// Dense value cache — Opt 1: O(1) dep-value reads in the propagation hot path.
    /// Kept in sync with `graph.node(id).value` on every write.
    value_store:     ValueStore,
    /// Causal clock — one dimension advances per ISA op.
    /// Answers "should this push propagate?" via delta-gated dep meta.
    causal_time:     ProductTime,
    /// Monotone arrival counter — total order for debugging and PortHistory.
    /// Never used for causal gating; purely a trace witness.
    arrival_counter: u64,
    /// Per-boundary-node port histories for oscillation detection.
    port_histories:  HashMap<Uid, PortHistory>,

    // ── Authority / partition ─────────────────────────────────────────────────

    /// Registry of declared partitions. Mutation-time only; never accessed during
    /// step_push or should_propagate.
    pub partition_registry: PartitionRegistry,
    /// Engine-level authority enforcement mode (default: Advisory = zero cost).
    pub authority_mode:     AuthorityMode,
    /// Policy for nodes not bound to any partition (default per authority_mode).
    pub unbound_policy:     UnboundPolicy,
    /// Node → partition binding (populated by PartitionBind ISA op).
    node_partitions:        FxHashMap<Uid, PartitionId>,
    /// Node → stabilization config (populated by SetStabilizationConfig ISA op).
    node_stabilization:     FxHashMap<Uid, StabilizationConfig>,
    /// Node → execution policy (populated by SetExecutionPolicy ISA op).
    node_execution:         FxHashMap<Uid, ExecutionPolicy>,
    /// Compiled sparse circuit cache — keyed by region root `Uid`.
    /// Artifacts are invalidated on topology mutations touching member nodes.
    pub region_cache:       RegionArtifactCache,
    /// Opt 9: active-lane dirty bitmask — one bit per dense `ValueStore` slot.
    ///
    /// Bit `j` is set when dense slot `j` received a new value during this
    /// `apply` cycle (via `SetValue` or `Reflect`).  `run_compiled_region` uses
    /// this to skip CSR rows whose dep columns have no dirty bit, and sets
    /// output bits when a row's result changes so downstream rows in the same
    /// topo-order pass see the update.  Cleared after the tier-2 dispatch in
    /// `drain_and_stabilize`.
    dirty_slots:            Vec<u64>,
    /// Opt 5: compiled-handled bitmask — one bit per dense `ValueStore` slot.
    ///
    /// Bit `j` is set for every member of a compiled region that ran in the
    /// current drain cycle.  `PropEngine::drain` uses this to skip
    /// `WorkItem::Push` items for nodes the compiled tier already evaluated —
    /// the warm-path re-evaluation would be a no-op because ValueStore already
    /// holds the correct result.  Cleared at the end of `drain_and_stabilize`
    /// alongside `dirty_slots`.  Same width as `dirty_slots` at all times.
    compiled_handled:       Vec<u64>,
}

/// A logged engine event (for observability / time-travel).
#[derive(Debug, Clone)]
pub struct EngineEvent {
    pub op:    String,
    pub node:  Option<Uid>,
    /// Bitemporal stamp: causal ordering + arrival ordering.
    pub stamp: EventStamp,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            max_steps: DEFAULT_MAX_STEPS,
            causal_time: ProductTime::ZERO,
            arrival_counter: 0,
            port_histories: HashMap::new(),
            authority_mode: AuthorityMode::Advisory,
            unbound_policy: UnboundPolicy::Allow,
            ..Default::default()
        }
    }

    /// Apply one ISA operation and drain the resulting propagation queue.
    /// Returns all `PropEvent`s generated during this apply cycle.
    pub fn apply(&mut self, op: IsaOp) -> Result<Vec<PropEvent>, EngineError> {
        self.prop.events.clear();

        match op {
            // ── Extension ────────────────────────────────────────────────────

            IsaOp::NodeCreate { id, typ, rule, attrs } => {
                // Identity layer: initialise node (version=0)
                let node = match &rule {
                    NodeKind::Input       => Node::input(id, &typ),
                    NodeKind::Computed(r) => Node::computed(id, &typ, r.clone()),
                };
                let node = Node { attrs, ..node };
                self.graph.add_node(node);
                self.deps.set_mode(id, ExecMode::Eager);
                // Opt 1: allocate a dense cache slot (initial value = T::Neg).
                self.value_store.alloc(id);
                // Opt 9: keep dirty_slots wide enough to cover the new slot.
                // Opt 5: keep compiled_handled the same width as dirty_slots.
                let needed_words = (self.value_store.cache_slice().len() + 63) / 64;
                if self.dirty_slots.len() < needed_words {
                    self.dirty_slots.push(0u64);
                    self.compiled_handled.push(0u64);
                }
                self.log("NodeCreate", Some(id), TimeDim::Identity);
            }

            IsaOp::EdgeConnect { id, typ, src, tgt, dep } => {
                if self.graph.node(src).is_none() {
                    return Err(EngineError::NodeNotFound(src));
                }
                if self.graph.node(tgt).is_none() {
                    return Err(EngineError::NodeNotFound(tgt));
                }

                // DPO admissibility note: EdgeConnect always extends the id-layer;
                // no pushout complement check is needed for addition (only deletion).
                // Remote edges are registered for later deletion guard.

                // Port-mode inference
                if let DepKind::Local(Some(ref p)) = dep {
                    if let Some(preferred) = p.preferred_mode() {
                        if self.deps.mode_of(tgt) == ExecMode::Eager {
                            self.deps.set_mode(tgt, preferred);
                        }
                    }
                } else if let DepKind::Remote(ref r) = dep {
                    let synthetic_port = Port::new("remote", r.port_kind);
                    if let Some(preferred) = synthetic_port.preferred_mode() {
                        if self.deps.mode_of(tgt) == ExecMode::Eager {
                            self.deps.set_mode(tgt, preferred);
                        }
                    }
                }

                let edge = Edge { id, typ, src, tgt, dep, attrs: crate::attr::Attrs::new() };
                self.graph.add_edge(edge);
                // Opt 1: use indexed subscribe so step_push can use dense cache reads.
                if let Some(src_dense) = self.value_store.dense_index(src) {
                    self.deps.subscribe_with_meta_indexed(src, tgt, src_dense, DepMeta::default());
                } else {
                    self.deps.subscribe(src, tgt);
                }
                // Opt 2: initialise dep counter for tgt with src's actual current value.
                self.deps.add_dep_to_counter(tgt, self.value_store.get(src));
                // Invalidate compiled regions covering src or tgt.
                self.region_cache.invalidate_for_node(src);
                self.region_cache.invalidate_for_node(tgt);
                self.log("EdgeConnect", Some(id), TimeDim::Dependency);
            }

            IsaOp::Subscribe { source, subscriber } => {
                // Opt 1: indexed subscribe when source has a dense slot.
                if let Some(src_dense) = self.value_store.dense_index(source) {
                    self.deps.subscribe_with_meta_indexed(source, subscriber, src_dense, DepMeta::default());
                } else {
                    self.deps.subscribe(source, subscriber);
                }
                // Opt 2: initialise dep counter with source's actual current value.
                self.deps.add_dep_to_counter(subscriber, self.value_store.get(source));
                self.log("Subscribe", Some(subscriber), TimeDim::Dependency);
            }

            IsaOp::SetValue { node, val } => {
                let n = self.graph.node_mut(node)
                    .ok_or(EngineError::NodeNotFound(node))?;
                let old = n.value;
                // Opt 7: same-value no-op — skip subscriber loop, causal tick,
                // and drain entirely.  Cost: one node lookup + one T comparison.
                if val == old {
                    return Ok(vec![]);
                }
                n.set_value(val);
                // Opt 1: keep dense cache in sync on every value write.
                self.value_store.set(node, val);
                if let Some(dense) = self.value_store.dense_index(node) {
                    // Opt 8: mark compiled regions that read this node as a boundary input.
                    self.region_cache.mark_dirty_for_input(dense);
                    // Opt 9: set active-lane dirty bit for intra-region row skipping.
                    let (word, bit) = (dense as usize / 64, dense as usize % 64);
                    if word < self.dirty_slots.len() {
                        self.dirty_slots[word] |= 1u64 << bit;
                    }
                }
                self.prop.events.push(PropEvent::ValueChanged { node, old, new: val });
                self.log("SetValue", Some(node), TimeDim::Attribute);
                let causal = self.causal_time;
                for sub in self.deps.subs_vec(node) {
                    // Opt 2: update dep counter for every subscriber unconditionally
                    // (counters track dep-value distribution, not push eligibility).
                    self.deps.update_dep_counter(sub, old, val);
                    let gate = self.deps.dep_meta(node, sub)
                        .map_or(true, |m| crate::deps::should_propagate(m, &causal));
                    if gate && self.deps.mode_of(sub) == ExecMode::Eager {
                        self.prop.enqueue(WorkItem::Push(sub));
                        self.deps.update_last_seen(node, sub, causal);
                    }
                }
                self.drain_and_stabilize()?;
            }

            IsaOp::Propagate { node } => {
                self.prop.enqueue(WorkItem::Push(node));
                self.log("Propagate", Some(node), TimeDim::Attribute);
                self.drain_and_stabilize()?;
            }

            // ── Inhibition ───────────────────────────────────────────────────

            IsaOp::DelNode { id } => {
                // DPO admissibility: reject if node has remote-dep edges
                // (removing a node that is an interface node of another partition
                // violates DPO interface preservation).
                let has_remote_interface = self.graph.dangling_edges(id)
                    .iter()
                    .any(|&eid| self.graph.edge(eid).map_or(false, |e| e.is_remote()));
                if has_remote_interface {
                    return Err(EngineError::DpoViolation(id,
                        "node has remote-dep edges; deletion would violate interface preservation"));
                }

                // Remove incident edges first (dangling edge check — DPO step 1)
                let dangling = self.graph.dangling_edges(id);
                for eid in dangling {
                    if let Some(edge) = self.graph.remove_edge(eid) {
                        // Opt 2: remove dep contribution before unsubscribing.
                        // Both nodes still exist in value_store at this point.
                        self.deps.remove_dep_from_counter(edge.tgt, self.value_store.get(edge.src));
                        self.deps.unsubscribe(edge.src, edge.tgt);
                    }
                }
                self.graph.remove_node(id);
                self.deps.remove_node(id);
                // Opt 1: tombstone the dense cache slot (slot lives on; idx entry kept).
                self.value_store.dealloc(id);
                // Invalidate compiled regions covering this node.
                self.region_cache.invalidate_for_node(id);
                self.log("DelNode", Some(id), TimeDim::Identity);
            }

            IsaOp::DelEdge { id } => {
                // DPO admissibility: reject deletion of a remote-dep edge
                if let Some(edge) = self.graph.edge(id) {
                    if edge.is_remote() {
                        return Err(EngineError::DpoViolation(id,
                            "remote-dep edge cannot be deleted locally; partition boundary is preserved"));
                    }
                }
                if let Some(edge) = self.graph.remove_edge(id) {
                    self.region_cache.invalidate_for_node(edge.src);
                    self.region_cache.invalidate_for_node(edge.tgt);
                    // Opt 2: remove dep contribution from tgt's counter before unsubscribing.
                    self.deps.remove_dep_from_counter(edge.tgt, self.value_store.get(edge.src));
                    self.deps.unsubscribe(edge.src, edge.tgt);
                    self.log("DelEdge", Some(id), TimeDim::Dependency);
                }
            }

            IsaOp::Demand { node } => {
                self.prop.enqueue(WorkItem::Demand(node));
                self.log("Demand", Some(node), TimeDim::Attribute);
                self.drain_and_stabilize()?;
            }

            IsaOp::SetMode { node, mode } => {
                if self.graph.node(node).is_none() {
                    return Err(EngineError::NodeNotFound(node));
                }
                self.deps.set_mode(node, mode);
                self.log("SetMode", Some(node), TimeDim::Identity);
            }

            // ── Reflection ───────────────────────────────────────────────────

            IsaOp::Reflect { node } => {
                self.log("Reflect", Some(node), TimeDim::Attribute);
                let n = self.graph.node_mut(node)
                    .ok_or(EngineError::NodeNotFound(node))?;
                let old = n.reflect();
                let new = n.value;
                // Opt 1: keep dense cache in sync after reflect (mv_neg applied in-place).
                self.value_store.set(node, new);
                if new != old {
                    if let Some(dense) = self.value_store.dense_index(node) {
                        // Opt 8: mark compiled regions that read this node as a boundary input.
                        self.region_cache.mark_dirty_for_input(dense);
                        // Opt 9: set active-lane dirty bit.
                        let (word, bit) = (dense as usize / 64, dense as usize % 64);
                        if word < self.dirty_slots.len() {
                            self.dirty_slots[word] |= 1u64 << bit;
                        }
                    }
                    self.prop.events.push(PropEvent::ValueChanged { node, old, new });
                }
                let causal = self.causal_time;
                for sub in self.deps.subs_vec(node) {
                    // Opt 2: update dep counter (update is no-op when old == new,
                    // e.g. Reflect on Zero leaves value unchanged).
                    self.deps.update_dep_counter(sub, old, new);
                    let gate = self.deps.dep_meta(node, sub)
                        .map_or(true, |m| crate::deps::should_propagate(m, &causal));
                    if gate && self.deps.mode_of(sub) == ExecMode::Eager {
                        self.prop.enqueue(WorkItem::Push(sub));
                        self.deps.update_last_seen(node, sub, causal);
                    }
                }
                self.drain_and_stabilize()?;
            }

            IsaOp::Stabilize { region } => {
                let mut seeds: Vec<Uid> = region.unwrap_or_else(|| {
                    self.graph.nodes.keys()
                        .filter(|&&id| {
                            self.graph.node(id).map(|n| n.value == T::Zero).unwrap_or(false)
                        })
                        .copied()
                        .collect()
                });
                // Sort for deterministic stabilisation order (im::HashMap keys
                // iterate in random seed order, differing across Engine instances).
                seeds.sort_unstable();
                self.run_stabilize(&seeds)?;
                self.log("Stabilize", None, TimeDim::Egraph);
            }

            // ── Authority / partition (id-layer only) ─────────────────────────

            IsaOp::PartitionCreate { id, authority_root, lattice_class, causal_domain } => {
                self.partition_registry.register(PartitionDecl {
                    id, authority_root, lattice_class, causal_domain,
                });
                self.log("PartitionCreate", None, TimeDim::Identity);
            }

            IsaOp::PartitionBind { node, partition } => {
                self.node_partitions.insert(node, partition);
                // Recompile all outgoing edge labels to incorporate partition's causal domain.
                self.recompile_edges_for_node(node);
                self.log("PartitionBind", Some(node), TimeDim::Identity);
            }

            IsaOp::SetPartitionAuthority { partition, lattice_class, causal_domain } => {
                if let Some(decl) = self.partition_registry.partitions.get_mut(&partition) {
                    decl.lattice_class = lattice_class;
                    decl.causal_domain = causal_domain;
                }
                self.log("SetPartitionAuthority", None, TimeDim::Identity);
            }

            IsaOp::SetEdgeLabel { source, target, label } => {
                let src_partition = self.node_partitions.get(&source).copied();
                let compiled = self.partition_registry.compile_label(&label, src_partition);
                self.deps.set_compiled_label(source, target, compiled);
                self.log("SetEdgeLabel", None, TimeDim::Identity);
            }

            IsaOp::SetStabilizationConfig { node, config } => {
                self.node_stabilization.insert(node, config);
                self.log("SetStabilizationConfig", Some(node), TimeDim::Identity);
            }

            IsaOp::SetExecutionPolicy { node, policy } => {
                self.node_execution.insert(node, policy);
                self.log("SetExecutionPolicy", Some(node), TimeDim::Identity);
            }

            // ── Region compilation ────────────────────────────────────────────

            IsaOp::RegionDeclare { root, boundary, stability, compile } => {
                // Resolve member set from boundary spec.
                let members: Vec<Uid> = match &boundary {
                    RegionBoundary::ExplicitSet(v) => v.clone(),
                    RegionBoundary::DepClosure { max_depth } => {
                        bfs_dep_closure(root, *max_depth, &self.deps)
                    }
                    RegionBoundary::PartitionScoped(pid) => {
                        self.node_partitions.iter()
                            .filter(|(_, p)| *p == pid)
                            .map(|(n, _)| *n)
                            .collect()
                    }
                };

                // Always register the stability contract so invalidation fires
                // even for Lazy/Never policies.
                self.region_cache.contracts.insert(root, stability);

                if compile == CompilePolicy::Eager {
                    let epoch = self.deps.max_epoch_for_nodes(&members);
                    if let Some(region) = compile_region(
                        &members, &self.graph, &self.deps, &self.value_store, epoch,
                    ) {
                        self.region_cache.insert(root, region, stability);
                    }
                }

                self.log("RegionDeclare", Some(root), TimeDim::Identity);
            }
        }

        Ok(std::mem::take(&mut self.prop.events))
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Drain the propagation queue, handling any `Stabilize` work items inline.
    ///
    /// Constructs `AuthContext` from engine fields when in Audit/Enforced mode
    /// (zero cost in Advisory mode — `None` is passed and no auth struct is built).
    fn drain_and_stabilize(&mut self) -> Result<(), EngineError> {
        let max = self.max_steps;

        // ── Tier 2: run dirty compiled regions (Opts 8 + 9 + 11 + 12) ──────────
        // Opt 8:  only execute regions whose boundary inputs changed this cycle.
        // Opt 9:  skip rows whose deps have no dirty bit (superseded by Opt 11).
        // Opt 11: column-push pending bitmask — O(dirty_inputs × fan_out) row
        //         selection via inverted index binary search.
        // Opt 12: epoch re-check removed — EpochTracked artifacts are evicted from
        //         both `regions` and `dirty` by `invalidate_for_node` at mutation
        //         time, so `take_dirty()` never returns a stale root.
        let region_roots = self.region_cache.take_dirty();
        for root in region_roots {
            // Guard: artifact may have been evicted between SetValue and drain
            // (e.g. topology mutation in the same apply batch).
            if !self.region_cache.regions.contains_key(&root) { continue; }
            // Split borrow: region_cache (immutable) + value_store + dirty_slots (mutable).
            // These are distinct Engine fields, so Rust allows simultaneous borrows.
            let changed = run_compiled_region(
                &self.region_cache.regions[&root],
                &mut self.value_store,
                &mut self.dirty_slots,
            );

            // Opt 5: mark ALL members of this compiled region as handled so the
            // warm path skips any Push items for them.  Even rows the column-push
            // skipped (no dirty deps) are safe to mark: the warm path only enqueued
            // direct subscribers of changed inputs, which the compiled circuit has
            // already evaluated via the inverted index.
            for &member_uid in self.region_cache.regions[&root].members.iter() {
                if let Some(dense) = self.value_store.dense_index(member_uid) {
                    let (word, bit) = (dense as usize / 64, dense as usize % 64);
                    if word < self.compiled_handled.len() {
                        self.compiled_handled[word] |= 1u64 << bit;
                    }
                }
            }

            if changed.is_empty() { continue; }

            // Account for compiled-region materializations in PropStats so
            // region-aware amplification benchmarks see consistent counts.
            self.prop.stats.nodes_materialized += changed.len() as u64;

            let causal = self.causal_time;
            for (node, old, new) in changed {
                // Sync graph node value (keeps graph authoritative).
                if let Some(n) = self.graph.node_mut(node) { n.set_value(new); }
                self.prop.events.push(PropEvent::ValueChanged { node, old, new });
                // Enqueue boundary subscribers (nodes outside the compiled region).
                let region_members = &self.region_cache.regions[&root].members;
                for sub in self.deps.subs_vec(node) {
                    if region_members.contains(&sub) { continue; } // intra-region: handled
                    // Opt 2: keep dep_counter in sync for boundary subscribers so that
                    // step_push's fast path sees the updated value if it later fires.
                    self.deps.update_dep_counter(sub, old, new);
                    let gate = self.deps.dep_meta(node, sub)
                        .map_or(true, |m| crate::deps::should_propagate(m, &causal));
                    if gate && self.deps.mode_of(sub) == ExecMode::Eager {
                        self.prop.enqueue(WorkItem::Push(sub));
                        self.deps.update_last_seen(node, sub, causal);
                    }
                }
            }
        }
        // Opt 9: clear dirty_slots after all compiled regions have run.
        // The warm path does not use dirty_slots, so it is safe to zero here.
        for word in &mut self.dirty_slots { *word = 0; }
        // NOTE: compiled_handled is NOT cleared here — it must remain set through
        // the entire warm drain so prop.drain can skip Push items for handled nodes.
        // It is cleared at the end of drain_and_stabilize (see below).

        loop {
            let causal = self.causal_time;

            // Build auth context only for Audit/Enforced — Advisory is zero cost.
            if self.authority_mode != AuthorityMode::Advisory {
                let auth_ctx = AuthContext {
                    mode:            self.authority_mode,
                    unbound_policy:  self.unbound_policy,
                    node_partitions: &self.node_partitions,
                    partition_decls: &self.partition_registry.partitions,
                };
                self.prop.drain(&mut self.graph, &mut self.deps, &mut self.value_store, &causal, Some(&auth_ctx), &self.compiled_handled, max)
                    .map_err(EngineError::Prop)?;
            } else {
                self.prop.drain(&mut self.graph, &mut self.deps, &mut self.value_store, &causal, None, &self.compiled_handled, max)
                    .map_err(EngineError::Prop)?;
            }

            let stabilize_seeds: Vec<Vec<Uid>> = self.prop.events
                .iter()
                .filter_map(|e| {
                    if let PropEvent::StabilizeQueued { region } = e { Some(region.clone()) } else { None }
                })
                .collect();

            if stabilize_seeds.is_empty() { break; }

            self.prop.events.retain(|e| !matches!(e, PropEvent::StabilizeQueued { .. }));

            for seeds in stabilize_seeds {
                self.run_stabilize(&seeds)?;
            }
        }
        // Opt 5: clear compiled_handled at end of drain cycle.
        // Bits were valid for all warm-drain iterations above; zero for next cycle.
        for word in &mut self.compiled_handled { *word = 0; }
        Ok(())
    }

    /// Recompile compiled edge labels for all outgoing edges of `node`.
    ///
    /// Called after `PartitionBind` — the node's partition is now known, so the
    /// partition's causal domain can be intersected into existing compiled labels.
    fn recompile_edges_for_node(&mut self, node: Uid) {
        let src_partition = self.node_partitions.get(&node).copied();
        if let Some(pid) = src_partition {
            if let Some(decl) = self.partition_registry.partitions.get(&pid) {
                let domain_bits = decl.causal_domain.scope_bits;
                let subs: Vec<Uid> = self.deps.subs_of(node).collect();
                for sub in subs {
                    let mut label = self.deps.compiled_label(node, sub);
                    // Narrow causal scope to the intersection with partition domain.
                    label.causal_scope_bits &= domain_bits;
                    self.deps.set_compiled_label(node, sub, label);
                }
            }
        }
    }

    /// Run structural e-graph stabilization over a seeded region.
    ///
    /// Builds a `SectionRegion` from the seeds (BFS expansion, boundary-pinned),
    /// runs equivalence saturation on the structural expressions, checks port
    /// compliance, and applies any resolved values back to the graph (I8: via
    /// `set_value` to bump version).
    fn run_stabilize(&mut self, seeds: &[Uid]) -> Result<(), EngineError> {
        if seeds.is_empty() { return Ok(()); }

        let region = build_section_region(seeds, &self.graph, &self.deps, 64);

        let (resolved, compliance) = stabilize_section(
            &region, &self.graph, &self.deps, &self.port_histories,
        );

        match compliance {
            PortCompliance::Pos | PortCompliance::Neg => {
                if let Some(val) = resolved {
                    let causal = self.causal_time;
                    // Sort members for deterministic value-apply + push-enqueue order.
                    // std::HashSet iterates in random seed order across instances.
                    let mut sorted_members: Vec<Uid> = region.members.iter().copied().collect();
                    sorted_members.sort_unstable();
                    for &id in &sorted_members {
                        if let Some(n) = self.graph.node_mut(id) {
                            if n.value == T::Zero {
                                let old = n.set_value(val);
                                // Opt 1: sync dense cache after stabilize resolves a value.
                                self.value_store.set(id, val);
                                self.prop.events.push(PropEvent::ValueChanged {
                                    node: id, old, new: val,
                                });
                                for sub in self.deps.subs_vec(id) {
                                    let gate = self.deps.dep_meta(id, sub)
                                        .map_or(true, |m| crate::deps::should_propagate(m, &causal));
                                    if gate && self.deps.mode_of(sub) == ExecMode::Eager {
                                        self.prop.enqueue(WorkItem::Push(sub));
                                        self.deps.update_last_seen(id, sub, causal);
                                    }
                                }
                            }
                        }
                    }
                    // Advance Egraph dimension: equivalence saturation resolved
                    self.causal_time = self.causal_time.advance(TimeDim::Egraph);
                    self.arrival_counter += 1;
                }
            }
            PortCompliance::Zero(StabZeroKind::Boundary) => {
                // Emit typed zero to boundary — leave nodes as Zero
                // (ZeroKind::Conflict is the natural mapping from a boundary obstruction)
                for &id in &region.members {
                    self.prop.events.push(PropEvent::BochvarInfected {
                        node: id,
                        kind: Some(ZeroKind::Conflict),
                    });
                }
            }
            PortCompliance::Zero(StabZeroKind::Informational) => {
                // Upstream lazy dep not evaluated yet — demand it
                for &id in &region.members {
                    for dep_id in self.deps.deps_of(id).collect::<Vec<_>>() {
                        if self.graph.node(dep_id).map_or(false, |n| n.value == T::Neg) {
                            self.prop.enqueue(WorkItem::Demand(dep_id));
                        }
                    }
                }
            }
            PortCompliance::Zero(StabZeroKind::Structural) => {
                // Internal ambiguity — nodes stay Zero, no further action this cycle
            }
        }
        Ok(())
    }

    /// Advance the causal clock by one tick in `dim`, increment arrival counter,
    /// and append a stamped event to history.
    fn log(&mut self, op: &str, node: Option<Uid>, dim: TimeDim) {
        self.causal_time = self.causal_time.advance(dim);
        self.arrival_counter += 1;
        self.history.push(EngineEvent {
            op:    op.to_string(),
            node,
            stamp: EventStamp { causal: self.causal_time, arrival: self.arrival_counter },
        });
    }

    // ── Inspection ────────────────────────────────────────────────────────────

    /// O(1) snapshot of the current graph (structural sharing via `im`).
    pub fn snapshot(&self) -> Graph { self.graph.snapshot() }

    /// Current ternary value of a node.
    pub fn value_of(&self, id: Uid) -> Option<T> {
        self.graph.node(id).map(|n| n.value)
    }

    /// Current interpretation version of a node.
    /// Increments on every write to `value` or `attrs`.
    pub fn version_of(&self, id: Uid) -> Option<u64> {
        self.graph.node(id).map(|n| n.version)
    }

    /// Current attrs of a node.
    pub fn attrs_of(&self, id: Uid) -> Option<&crate::attr::Attrs> {
        self.graph.node(id).map(|n| &n.attrs)
    }

    /// Set an attribute directly (bumps version — interpretation write).
    pub fn set_attr(&mut self, node_id: Uid, key: &str, val: Val) -> Result<(), EngineError> {
        let n = self.graph.node_mut(node_id)
            .ok_or(EngineError::NodeNotFound(node_id))?;
        n.attrs.set(key, val);
        n.version += 1;
        Ok(())
    }

    pub fn node_count(&self) -> usize { self.graph.node_count() }
    pub fn edge_count(&self) -> usize { self.graph.edge_count() }

    // ── Statistics ────────────────────────────────────────────────────────────

    /// Reference to cumulative propagation statistics.
    pub fn stats(&self) -> &crate::propagate::PropStats { &self.prop.stats }

    /// Reset all accumulated propagation statistics to zero.
    pub fn reset_stats(&mut self) { self.prop.reset_stats(); }

    // ── Authority accessors ───────────────────────────────────────────────────

    /// Set the authority enforcement mode. Also updates `unbound_policy` to its
    /// mode-appropriate default (can be overridden afterwards).
    pub fn set_authority_mode(&mut self, mode: AuthorityMode) {
        self.authority_mode = mode;
        self.unbound_policy = UnboundPolicy::default_for(mode);
    }

    /// Returns the partition a node is bound to, if any.
    pub fn partition_of(&self, node: Uid) -> Option<PartitionId> {
        self.node_partitions.get(&node).copied()
    }

    /// Returns the stabilization config for a node (or the default if not set).
    pub fn stabilization_config(&self, node: Uid) -> &StabilizationConfig {
        self.node_stabilization.get(&node)
            .unwrap_or(&StabilizationConfig::DEFAULT)
    }

    /// Returns the execution policy for a node (or the default if not set).
    pub fn execution_policy(&self, node: Uid) -> &ExecutionPolicy {
        self.node_execution.get(&node)
            .unwrap_or(&ExecutionPolicy::DEFAULT)
    }
}

// ── Region helpers ────────────────────────────────────────────────────────────

/// BFS expansion of the dep-closure rooted at `root`, up to `max_depth` hops.
fn bfs_dep_closure(root: Uid, max_depth: usize, deps: &DepRegistry) -> Vec<Uid> {
    let mut visited = rustc_hash::FxHashSet::default();
    let mut queue   = std::collections::VecDeque::new();
    queue.push_back((root, 0usize));
    visited.insert(root);
    let mut result = Vec::new();
    while let Some((node, depth)) = queue.pop_front() {
        result.push(node);
        if depth < max_depth {
            // Sort for deterministic BFS order — im::HashSet seed varies per instance.
            let mut dep_ids: Vec<Uid> = deps.deps_of(node).collect();
            dep_ids.sort_unstable();
            for dep in dep_ids {
                if visited.insert(dep) {
                    queue.push_back((dep, depth + 1));
                }
            }
        }
    }
    result
}

/// Compile a frozen CSR circuit for the given member set.
///
/// Performs a topological sort (Kahn's algorithm), assigns CSR row pointers
/// from `ordered_dep_indices`, and stores the `ComputeRule` per node.
/// Returns `None` if the member set contains a cycle or an unmapped node.
fn compile_region(
    members:     &[Uid],
    graph:       &Graph,
    deps:        &DepRegistry,
    value_store: &ValueStore,
    epoch:       AdjEpoch,
) -> Option<CompiledRegion> {
    let member_set: rustc_hash::FxHashSet<Uid> = members.iter().copied().collect();

    // Kahn's topological sort over dep edges within the member set.
    let mut in_degree: rustc_hash::FxHashMap<Uid, usize> = members.iter()
        .map(|&n| (n, deps.deps_of(n).filter(|d| member_set.contains(d)).count()))
        .collect();
    let mut queue: std::collections::VecDeque<Uid> = in_degree.iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&n, _)| n)
        .collect();
    // Sort initial queue for determinism.
    let mut init: Vec<Uid> = queue.drain(..).collect();
    init.sort_unstable();
    queue.extend(init);

    let mut topo: Vec<Uid> = Vec::with_capacity(members.len());
    while let Some(n) = queue.pop_front() {
        topo.push(n);
        let mut next: Vec<Uid> = deps.subs_of(n)
            .filter(|s| member_set.contains(s))
            .collect();
        next.sort_unstable();
        for sub in next {
            let d = in_degree.get_mut(&sub)?;
            *d -= 1;
            if *d == 0 { queue.push_back(sub); }
        }
    }
    if topo.len() != members.len() { return None; } // cycle

    // Build CSR adjacency + rules + outputs.
    let mut dep_offsets: Vec<u32> = vec![0u32];
    let mut dep_cols:    Vec<u32> = Vec::new();
    let mut rules:       Vec<crate::node::ComputeRule> = Vec::new();
    let mut outputs:     Vec<u32> = Vec::new();
    let mut inputs_seen: rustc_hash::FxHashSet<u32> = Default::default();
    let mut inputs:      Vec<u32> = Vec::new();

    for &node in &topo {
        let out_dense = value_store.dense_index(node)?;
        outputs.push(out_dense);

        let rule = graph.node(node)
            .and_then(|n| n.compute_rule().cloned())
            .unwrap_or(crate::node::ComputeRule::Identity);
        rules.push(rule);

        // Use pre-baked dense indices when available; fall back to on-the-fly lookup.
        let dep_indices = deps.ordered_dep_indices_of(node);
        if !dep_indices.is_empty() {
            for &di in dep_indices {
                dep_cols.push(di);
                if inputs_seen.insert(di) && !outputs.contains(&di) {
                    inputs.push(di);
                }
            }
        } else {
            // Fallback: resolve dep Uids → dense indices now.
            let dep_uids: Vec<Uid> = deps.deps_of(node).collect();
            for dep_uid in dep_uids {
                let di = value_store.dense_index(dep_uid)?;
                dep_cols.push(di);
                if inputs_seen.insert(di) && !outputs.contains(&di) {
                    inputs.push(di);
                }
            }
        }
        dep_offsets.push(dep_cols.len() as u32);
    }

    // Build column-push inverted index + SWAR contiguity hints (Opts 10 + 11).
    let n_rows = topo.len();
    let (col_inv_keys, col_inv_offsets, col_inv_rows, contig_start) =
        build_region_inv(&dep_offsets, &dep_cols, n_rows);

    Some(CompiledRegion {
        members:         topo.into_boxed_slice(),
        rules:           rules.into_boxed_slice(),
        outputs:         outputs.into_boxed_slice(),
        inputs:          inputs.into_boxed_slice(),
        dep_offsets:     dep_offsets.into_boxed_slice(),
        dep_cols:        dep_cols.into_boxed_slice(),
        compiled_at:     epoch,
        col_inv_keys:    col_inv_keys.into_boxed_slice(),
        col_inv_offsets: col_inv_offsets.into_boxed_slice(),
        col_inv_rows:    col_inv_rows.into_boxed_slice(),
        contig_start:    contig_start.into_boxed_slice(),
    })
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("node {0} not found")]
    NodeNotFound(Uid),
    #[error("edge {0} not found")]
    EdgeNotFound(Uid),
    #[error("propagation error: {0}")]
    Prop(#[from] PropError),
    #[error("stabilization failed: {0}")]
    Stabilize(String),
    /// DPO interface preservation violation — the deletion or addition would
    /// alter a remote partition's interface without its consent.
    #[error("DPO admissibility violation at {0}: {1}")]
    DpoViolation(Uid, &'static str),
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        node::ComputeRule,
        uid,
    };

    fn make_input(e: &mut Engine, id: Uid) {
        e.apply(IsaOp::input_node(id, "input")).unwrap();
    }

    fn make_computed(e: &mut Engine, id: Uid, rule: ComputeRule) {
        e.apply(IsaOp::computed_node(id, "computed", rule)).unwrap();
    }

    fn connect(e: &mut Engine, src: Uid, tgt: Uid) {
        e.apply(IsaOp::dep_edge(uid::fresh(), src, tgt)).unwrap();
    }

    fn set(e: &mut Engine, id: Uid, val: T) {
        e.apply(IsaOp::SetValue { node: id, val }).unwrap();
    }

    // ── Existing behaviour ────────────────────────────────────────────────────

    #[test]
    fn test_single_input_node() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);
        assert_eq!(e.value_of(id), Some(T::Neg));
        set(&mut e, id, T::Pos);
        assert_eq!(e.value_of(id), Some(T::Pos));
    }

    #[test]
    fn test_and_gate_propagation() {
        let mut e = Engine::new();
        let (a, b, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_computed(&mut e, out, ComputeRule::MvMul);
        connect(&mut e, a, out);
        connect(&mut e, b, out);

        set(&mut e, a, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Neg)); // b still Neg → Pending

        set(&mut e, b, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Pos)); // both Pos → MvMul = Pos
    }

    #[test]
    fn test_bochvar_infection() {
        let mut e = Engine::new();
        let (a, b, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_computed(&mut e, out, ComputeRule::MvAdd);
        connect(&mut e, a, out);
        connect(&mut e, b, out);

        set(&mut e, b, T::Pos);
        set(&mut e, a, T::Zero);
        assert_eq!(e.value_of(out), Some(T::Zero)); // Bochvar infected
    }

    #[test]
    fn test_reflect_op() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);
        set(&mut e, id, T::Pos);
        e.apply(IsaOp::Reflect { node: id }).unwrap();
        assert_eq!(e.value_of(id), Some(T::Neg)); // mv_neg(Pos) = Neg
    }

    #[test]
    fn test_del_node_removes_edges() {
        let mut e = Engine::new();
        let (a, b, eid) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        e.apply(IsaOp::dep_edge(eid, a, b)).unwrap();

        assert_eq!(e.edge_count(), 1);
        e.apply(IsaOp::DelNode { id: a }).unwrap();
        assert_eq!(e.node_count(), 1);
        assert_eq!(e.edge_count(), 0);
    }

    #[test]
    fn test_lazy_mode_no_push() {
        let mut e = Engine::new();
        let (a, out) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::Identity);
        connect(&mut e, a, out);
        e.apply(IsaOp::SetMode { node: out, mode: ExecMode::Lazy }).unwrap();

        set(&mut e, a, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Neg)); // Lazy: not pushed
    }

    #[test]
    fn test_demand_triggers_pull() {
        let mut e = Engine::new();
        let (a, out) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::Identity);
        connect(&mut e, a, out);
        e.apply(IsaOp::SetMode { node: out, mode: ExecMode::Lazy }).unwrap();

        set(&mut e, a, T::Pos);
        e.apply(IsaOp::Demand { node: out }).unwrap();
        assert_eq!(e.value_of(out), Some(T::Pos));
    }

    #[test]
    fn test_snapshot_is_independent() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);
        set(&mut e, id, T::Pos);

        let snap = e.snapshot();
        set(&mut e, id, T::Neg);

        assert_eq!(e.value_of(id), Some(T::Neg));
        assert_eq!(snap.node(id).map(|n| n.value), Some(T::Pos));
    }

    // ── Layer separation: version tracking (I8) ───────────────────────────────

    #[test]
    fn test_version_starts_at_zero() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);
        assert_eq!(e.version_of(id), Some(0));
    }

    #[test]
    fn test_version_increments_on_set_value() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);

        set(&mut e, id, T::Pos);
        assert_eq!(e.version_of(id), Some(1));

        set(&mut e, id, T::Neg);
        assert_eq!(e.version_of(id), Some(2));
    }

    #[test]
    fn test_version_not_incremented_when_value_unchanged() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);

        set(&mut e, id, T::Pos);
        assert_eq!(e.version_of(id), Some(1));

        // Set to same value — no change, no version bump
        set(&mut e, id, T::Pos);
        assert_eq!(e.version_of(id), Some(1));
    }

    #[test]
    fn test_version_increments_on_reflect() {
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);
        set(&mut e, id, T::Pos);
        assert_eq!(e.version_of(id), Some(1));

        e.apply(IsaOp::Reflect { node: id }).unwrap();
        assert_eq!(e.version_of(id), Some(2));
        assert_eq!(e.value_of(id), Some(T::Neg));
    }

    #[test]
    fn test_version_increments_on_propagation() {
        let mut e = Engine::new();
        let (a, out) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::Identity);
        connect(&mut e, a, out);

        let v_before = e.version_of(out).unwrap();
        set(&mut e, a, T::Pos);
        // out's value changed via propagation → version should have bumped
        assert!(e.version_of(out).unwrap() > v_before);
    }

    #[test]
    fn test_identity_layer_unchanged_by_set_value() {
        // id, typ, kind must not change when interpretation is written
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);

        let typ_before = e.graph.node(id).unwrap().typ.clone();
        set(&mut e, id, T::Pos);
        let typ_after = e.graph.node(id).unwrap().typ.clone();
        assert_eq!(typ_before, typ_after);
    }

    // ── Port semantics ────────────────────────────────────────────────────────

    #[test]
    fn test_effort_port_infers_lazy_mode() {
        let mut e = Engine::new();
        let (src, tgt) = (uid::fresh(), uid::fresh());
        make_input(&mut e, src);
        make_computed(&mut e, tgt, ComputeRule::Identity);

        // Effort port: tgt should be inferred as Lazy
        e.apply(IsaOp::effort_edge(uid::fresh(), src, tgt, "e")).unwrap();
        assert_eq!(e.deps.mode_of(tgt), ExecMode::Lazy);
    }

    #[test]
    fn test_flow_port_keeps_eager_mode() {
        let mut e = Engine::new();
        let (src, tgt) = (uid::fresh(), uid::fresh());
        make_input(&mut e, src);
        make_computed(&mut e, tgt, ComputeRule::Identity);

        // Flow port: tgt stays Eager
        e.apply(IsaOp::flow_edge(uid::fresh(), src, tgt, "f")).unwrap();
        assert_eq!(e.deps.mode_of(tgt), ExecMode::Eager);
    }

    #[test]
    fn test_bond_port_infers_stabilizing_mode() {
        let mut e = Engine::new();
        let (src, tgt) = (uid::fresh(), uid::fresh());
        make_input(&mut e, src);
        make_computed(&mut e, tgt, ComputeRule::PowerProduct);

        // Bond port: tgt should be inferred as Stabilizing
        e.apply(IsaOp::bond_edge(uid::fresh(), src, tgt, "b")).unwrap();
        assert_eq!(e.deps.mode_of(tgt), ExecMode::Stabilizing);
    }

    #[test]
    fn test_explicit_set_mode_overrides_port_inference() {
        // If we set mode explicitly first, a later port inference should not override it
        let mut e = Engine::new();
        let (src, tgt) = (uid::fresh(), uid::fresh());
        make_input(&mut e, src);
        make_computed(&mut e, tgt, ComputeRule::Identity);

        // Explicitly set to Stabilizing before connecting
        e.apply(IsaOp::SetMode { node: tgt, mode: ExecMode::Stabilizing }).unwrap();

        // Flow port would prefer Eager, but Stabilizing ≠ Eager, so no override
        e.apply(IsaOp::flow_edge(uid::fresh(), src, tgt, "f")).unwrap();
        assert_eq!(e.deps.mode_of(tgt), ExecMode::Stabilizing); // unchanged
    }

    #[test]
    fn test_signal_port_stored_on_edge() {
        let mut e = Engine::new();
        let (src, tgt, eid) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, src);
        make_computed(&mut e, tgt, ComputeRule::Identity);

        e.apply(IsaOp::named_dep_edge(eid, src, tgt, "x")).unwrap();

        let edge = e.graph.edge(eid).unwrap();
        assert_eq!(edge.port_name(), Some("x"));
        assert_eq!(edge.port_kind(), crate::node::PortKind::Signal);
    }

    #[test]
    fn test_effort_port_kind_stored_on_edge() {
        let mut e = Engine::new();
        let (src, tgt, eid) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, src);
        make_computed(&mut e, tgt, ComputeRule::Identity);

        e.apply(IsaOp::effort_edge(eid, src, tgt, "e_in")).unwrap();

        let edge = e.graph.edge(eid).unwrap();
        assert_eq!(edge.port_kind(), crate::node::PortKind::Effort);
        assert_eq!(edge.port_name(), Some("e_in"));
    }

    // ── RegionDeclare + compiled circuit ──────────────────────────────────────

    #[test]
    fn region_declare_never_does_not_error() {
        use crate::region::{RegionBoundary, StabilityContract, CompilePolicy};
        let mut e = Engine::new();
        let id = uid::fresh();
        make_input(&mut e, id);
        e.apply(IsaOp::RegionDeclare {
            root: id,
            boundary: RegionBoundary::ExplicitSet(vec![id]),
            stability: StabilityContract::EpochTracked,
            compile: CompilePolicy::Never,
        }).unwrap();
    }

    #[test]
    fn region_declare_eager_compiles_and_propagates_correctly() {
        use crate::region::{RegionBoundary, StabilityContract, CompilePolicy};
        let mut e = Engine::new();
        let (a, b, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        connect(&mut e, a, out);
        connect(&mut e, b, out);

        // Declare the output node as an eagerly compiled region.
        e.apply(IsaOp::RegionDeclare {
            root: out,
            boundary: RegionBoundary::ExplicitSet(vec![out]),
            stability: StabilityContract::EpochTracked,
            compile: CompilePolicy::Eager,
        }).unwrap();

        set(&mut e, a, T::Pos);
        set(&mut e, b, T::Pos);
        // meet(Pos, Pos) = Pos — compiled circuit should have run
        assert_eq!(e.value_of(out), Some(T::Pos));

        // Bochvar: one Zero input forces output to Zero
        set(&mut e, a, T::Zero);
        assert_eq!(e.value_of(out), Some(T::Zero));

        // Recovery: back to Pos
        set(&mut e, a, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Pos));
    }

    #[test]
    fn region_epoch_invalidated_on_new_edge() {
        use crate::region::{RegionBoundary, StabilityContract, CompilePolicy};
        let mut e = Engine::new();
        let (a, out) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::Identity);
        connect(&mut e, a, out);

        e.apply(IsaOp::RegionDeclare {
            root: out,
            boundary: RegionBoundary::ExplicitSet(vec![out]),
            stability: StabilityContract::EpochTracked,
            compile: CompilePolicy::Eager,
        }).unwrap();
        assert!(e.region_cache.regions.contains_key(&out));

        // Adding a new edge touching `out` should invalidate the compiled artifact.
        let b = uid::fresh();
        make_input(&mut e, b);
        connect(&mut e, b, out);
        assert!(!e.region_cache.regions.contains_key(&out));
    }

    // ── Opt 5: compiled_handled suppression ───────────────────────────────────

    #[test]
    fn compiled_handled_suppresses_warm_reprocessing() {
        // Setup: 2-input MeetAll in a compiled region. After SetValue on one input,
        // the compiled tier handles `out`. The warm path should skip `out` entirely,
        // which shows up as pushes_suppressed_by_compiled_handled > 0.
        use crate::region::{RegionBoundary, StabilityContract, CompilePolicy};
        let mut e = Engine::new();
        let (a, b, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        connect(&mut e, a, out);
        connect(&mut e, b, out);

        e.apply(IsaOp::RegionDeclare {
            root: out,
            boundary: RegionBoundary::ExplicitSet(vec![out]),
            stability: StabilityContract::Pinned,
            compile: CompilePolicy::Eager,
        }).unwrap();

        e.reset_stats();
        set(&mut e, a, T::Pos);
        set(&mut e, b, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Pos));
        assert!(
            e.stats().pushes_suppressed_by_compiled_handled > 0,
            "expected warm-path re-evaluation of `out` to be suppressed (got {})",
            e.stats().pushes_suppressed_by_compiled_handled
        );
    }

    #[test]
    fn compiled_handled_cleared_between_drain_cycles() {
        // After the first drain, compiled_handled is zeroed. A second independent
        // SetValue (no compiled region triggered) must NOT suppress the warm push.
        let mut e = Engine::new();
        let (a, relay) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, relay, ComputeRule::Identity);
        connect(&mut e, a, relay);

        // relay is NOT in any compiled region.
        e.reset_stats();
        set(&mut e, a, T::Pos);
        assert_eq!(e.value_of(relay), Some(T::Pos)); // warm path must fire
        // relay is not compiled-handled, so its push must not be suppressed.
        assert_eq!(e.stats().pushes_suppressed_by_compiled_handled, 0);
    }

    // ── Opt 2: dep counter maintenance ───────────────────────────────────────

    #[test]
    fn dep_counter_initialized_on_edge_connect() {
        // After connecting a → out, out's counter should reflect a's current value.
        let mut e = Engine::new();
        let (a, out) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        // a is still T::Neg at this point
        connect(&mut e, a, out);
        let ctrs = e.deps.dep_counter(out).copied().unwrap_or_default();
        assert_eq!(ctrs.neg_count, 1, "expected a's T::Neg to be counted");
        assert_eq!(ctrs.total(), 1);
    }

    #[test]
    fn dep_counter_updated_on_set_value() {
        let mut e = Engine::new();
        let (a, b, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        connect(&mut e, a, out);
        connect(&mut e, b, out);

        // Both start Neg: neg=2, zero=0, pos=0
        {
            let c = e.deps.dep_counter(out).copied().unwrap_or_default();
            assert_eq!(c.neg_count, 2); assert_eq!(c.zero_count, 0); assert_eq!(c.pos_count, 0);
        }

        set(&mut e, a, T::Pos);
        {
            let c = e.deps.dep_counter(out).copied().unwrap_or_default();
            assert_eq!(c.neg_count, 1); assert_eq!(c.pos_count, 1);
        }

        set(&mut e, b, T::Zero);
        {
            let c = e.deps.dep_counter(out).copied().unwrap_or_default();
            assert_eq!(c.pos_count, 1); assert_eq!(c.zero_count, 1); assert_eq!(c.neg_count, 0);
        }
    }

    #[test]
    fn dep_counter_removed_on_del_edge() {
        let mut e = Engine::new();
        let (a, out, eid) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        e.apply(IsaOp::dep_edge(eid, a, out)).unwrap();
        // a is T::Neg → neg_count = 1
        assert_eq!(e.deps.dep_counter(out).map(|c| c.neg_count), Some(1));

        e.apply(IsaOp::DelEdge { id: eid }).unwrap();
        // Counter entry may still exist but total must be 0
        assert_eq!(e.deps.dep_counter(out).map(|c| c.total()), Some(0).or(None));
    }

    #[test]
    fn dep_counter_drives_meetall_correctly() {
        // MeetAll with 3 inputs: verify counter path produces correct outputs.
        let mut e = Engine::new();
        let (a, b, c, out) = (uid::fresh(), uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_input(&mut e, c);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        connect(&mut e, a, out);
        connect(&mut e, b, out);
        connect(&mut e, c, out);

        // All Neg → Pending → out stays Neg
        assert_eq!(e.value_of(out), Some(T::Neg));

        // Two Pos, one Neg → still Pending
        set(&mut e, a, T::Pos);
        set(&mut e, b, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Neg));

        // All Pos → Ready → out = Pos
        set(&mut e, c, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Pos));

        // One Zero introduced → Conflicted → out = Zero
        set(&mut e, b, T::Zero);
        assert_eq!(e.value_of(out), Some(T::Zero));

        // Zero removed → back to Ready
        set(&mut e, b, T::Pos);
        assert_eq!(e.value_of(out), Some(T::Pos));
    }

    #[test]
    fn region_pinned_not_invalidated_on_rewrite() {
        use crate::region::{RegionBoundary, StabilityContract, CompilePolicy};
        let mut e = Engine::new();
        let (a, out) = (uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, out, ComputeRule::Identity);
        connect(&mut e, a, out);

        e.apply(IsaOp::RegionDeclare {
            root: out,
            boundary: RegionBoundary::ExplicitSet(vec![out]),
            stability: StabilityContract::Pinned,
            compile: CompilePolicy::Eager,
        }).unwrap();
        assert!(e.region_cache.regions.contains_key(&out));

        // Pinned: adding an edge should NOT remove the artifact.
        let b = uid::fresh();
        make_input(&mut e, b);
        connect(&mut e, b, out);
        assert!(e.region_cache.regions.contains_key(&out));
    }

    // ── Dep-counter regression: computed → computed chains (Opt 2) ────────────
    //
    // Before the fix in propagate.rs, step_push never called update_dep_counter
    // for downstream subscribers when a *computed* node's value changed.
    // SetValue/Reflect updated counters for their direct subs, but derived
    // changes via step_push did not.  So a JoinAny/MeetAll node whose deps are
    // themselves computed would see a stale counter (neg_count never cleared)
    // and short-circuit as Pending indefinitely.

    #[test]
    fn dep_counter_updated_through_direct_computed_chain() {
        // Simplest case: a → mid(MeetAll) → out(MeetAll).
        // Counter for out must be updated when mid transitions Neg→Pos.
        let mut e = Engine::new();
        let (a, mid, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, mid, ComputeRule::MeetAll);
        make_computed(&mut e, out, ComputeRule::MeetAll);
        connect(&mut e, a,   mid);
        connect(&mut e, mid, out);

        set(&mut e, a, T::Pos);
        assert_eq!(e.value_of(mid), Some(T::Pos), "mid should propagate");
        assert_eq!(e.value_of(out), Some(T::Pos), "out counter must update when mid changes");
    }

    #[test]
    fn dep_counter_propagates_through_computed_fan_in() {
        // Two MeetAll gates feed a JoinAny root — the topology that exposed the bug.
        //
        //   a ─┬─ ab(and) ─┬─ root(or)
        //   b ─┘           │
        //   b ─┬─ bc(and) ─┘
        //   c ─┘
        //
        // root.counter starts at neg_count=2.  When ab→Pos, step_push must call
        // update_dep_counter(root, Neg, Pos) so root.neg_count drops to 1.
        // When bc→Pos, another update drops it to 0, and root evaluates to Pos.
        let mut e = Engine::new();
        let (a, b, c)        = (uid::fresh(), uid::fresh(), uid::fresh());
        let (ab, bc, root)   = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_input(&mut e, b);
        make_input(&mut e, c);
        make_computed(&mut e, ab,   ComputeRule::MeetAll);
        make_computed(&mut e, bc,   ComputeRule::MeetAll);
        make_computed(&mut e, root, ComputeRule::JoinAny);
        connect(&mut e, a,  ab);
        connect(&mut e, b,  ab);
        connect(&mut e, b,  bc);
        connect(&mut e, c,  bc);
        connect(&mut e, ab,   root);
        connect(&mut e, bc,   root);

        // Intermediate state after a, b: ab resolves but bc is still Pending.
        set(&mut e, a, T::Pos);
        set(&mut e, b, T::Pos);
        assert_eq!(e.value_of(ab),   Some(T::Pos), "ab: a=T b=T → Pos");
        assert_eq!(e.value_of(bc),   Some(T::Neg), "bc: c still Neg → Pending");
        assert_eq!(e.value_of(root), Some(T::Neg), "root: bc still Pending");

        // After c: bc resolves; root's counter should now be neg_count=0 → Pos.
        set(&mut e, c, T::Pos);
        assert_eq!(e.value_of(bc),   Some(T::Pos), "bc: b=T c=T → Pos");
        assert_eq!(e.value_of(root), Some(T::Pos),
            "root must be Pos — dep counter must be updated through computed chain");
    }

    #[test]
    fn dep_counter_bochvar_through_computed_chain() {
        // Zero (conflict) must also update downstream counters.
        // a(Zero) → mid(MeetAll) → out(JoinAny): out should see Zero.
        let mut e = Engine::new();
        let (a, mid, out) = (uid::fresh(), uid::fresh(), uid::fresh());
        make_input(&mut e, a);
        make_computed(&mut e, mid, ComputeRule::MeetAll);
        make_computed(&mut e, out, ComputeRule::JoinAny);
        connect(&mut e, a,   mid);
        connect(&mut e, mid, out);

        set(&mut e, a, T::Zero);
        assert_eq!(e.value_of(mid), Some(T::Zero), "Bochvar: Zero infects mid");
        assert_eq!(e.value_of(out), Some(T::Zero),
            "out counter must register mid's Zero → out infected too");
    }
}
