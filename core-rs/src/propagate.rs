//! Propagation engine — the three execution modes.
//!
//! Architecture (from spec):
//!
//!   SET_VALUE(node, +1)
//!        │
//!        ▼
//!   Monotone forward pass (Extension / Eager mode)
//!   MV-algebra ops on Pos-valued deps
//!        │
//!        ├── all deps Pos → compute, push Pos downstream
//!        │
//!        └── any dep Zero → Bochvar infection → Zero downstream
//!                              │
//!                              ▼
//!                    Effect handler dispatch
//!                    ┌─────────────────────────┐
//!                    │ Lazy: defer until DEMAND │  (Inhibition)
//!                    │ Stabilizing: e-graph run │  (Reflection)
//!                    └─────────────────────────┘
//!
//! The propagation engine does NOT own the graph or registry — it receives
//! mutable references and returns a list of side effects (events) for the
//! engine to record.

use std::collections::VecDeque;
use smallvec::SmallVec;
use rustc_hash::{FxHashMap, FxHashSet};
use crate::{
    deps::{should_propagate, DepRegistry},
    graph::Graph,
    node::{ComputeRule, ExecMode, NodeKind},
    partition::{
        authority_gate, AuthorityMode, CompiledEdgeLabel, EmittedAuth,
        GateReason, PartitionDecl, PartitionId, UnboundPolicy, ZeroKind,
    },
    ternary::{PropState, T},
    time::ProductTime,
    uid::Uid,
    value_store::ValueStore,
};

/// Propagation statistics accumulated during drain cycles.
///
/// Tracks suppression, materialization, and work-queue geometry.
/// Populated by `PropEngine::drain`; reset via `PropEngine::reset_stats`.
/// All counters are additive — they accumulate across multiple `drain` calls
/// until explicitly reset.
#[derive(Clone, Debug, Default)]
pub struct PropStats {
    /// Nodes whose value changed during propagation (interpretation version bumped).
    pub nodes_materialized:          u64,
    /// Subscriber push-enqueues skipped because the subscriber is Lazy or Stabilizing.
    pub pushes_suppressed_by_mode:   u64,
    /// Subscriber push-enqueues skipped by the delta-gate (`should_propagate` = false).
    pub pushes_suppressed_by_delta:  u64,
    /// Total Demand work items processed from the queue.
    pub demands_fired:               u64,
    /// Peak work-queue length observed at the start of any drain step.
    pub max_queue_depth:             usize,
    /// Peak number of concurrent Demand items in the queue (demand frontier width).
    pub max_demand_frontier:         usize,
    /// Cumulative sum of stabilization region sizes routed via `WorkItem::Stabilize`.
    pub stabilize_region_size_total: usize,
    /// Push work items skipped because the compiled tier already handled the node (Opt 5).
    pub pushes_suppressed_by_compiled_handled: u64,
}

impl PropStats {
    /// Reset all counters to zero.
    pub fn reset(&mut self) { *self = Self::default(); }

    /// Ratio of materialized nodes to demand events processed.
    ///
    /// Measures how much internal computation a single external DEMAND triggers.
    /// For a fully-Lazy chain of depth D with one tail DEMAND: returns D.
    /// Compare to Eager mode where every write materializes depth nodes unprompted.
    /// Returns `f64::NAN` if no demands have fired.
    pub fn materialization_per_demand(&self) -> f64 {
        if self.demands_fired == 0 { return f64::NAN; }
        self.nodes_materialized as f64 / self.demands_fired as f64
    }
}

/// A work item in the propagation queue.
#[derive(Debug, Clone)]
pub enum WorkItem {
    /// Push: recompute node and propagate to its subs.
    Push(Uid),
    /// Demand: pull-evaluate node (Inhibition mode).
    Demand(Uid),
    /// Stabilize: run e-graph saturation over a region.
    Stabilize(Vec<Uid>),
}

/// Output event from a propagation step.
#[derive(Debug, Clone)]
pub enum PropEvent {
    ValueChanged { node: Uid, old: T, new: T },
    /// A node was infected by a Zero value.
    BochvarInfected { node: Uid, kind: Option<ZeroKind> },
    DemandFired { node: Uid },
    StabilizeQueued { region: Vec<Uid> },
    /// Authority gate violation (Audit mode only — Enforced mode suppresses silently).
    AuthViolation { src: Uid, tgt: Uid, reason: GateReason },
}

/// Authority context passed into `step_push` for Audit/Enforced modes.
/// None when `AuthorityMode::Advisory` — zero construction cost.
pub struct AuthContext<'a> {
    pub mode:             AuthorityMode,
    pub unbound_policy:   UnboundPolicy,
    pub node_partitions:  &'a FxHashMap<Uid, PartitionId>,
    pub partition_decls:  &'a FxHashMap<PartitionId, PartitionDecl>,
}

impl<'a> AuthContext<'a> {
    /// Construct `EmittedAuth` for a node, using its partition binding if available.
    fn emitted_auth(&self, node_id: Uid, compiled: &CompiledEdgeLabel) -> (EmittedAuth, bool) {
        match self.node_partitions.get(&node_id) {
            Some(pid) => match self.partition_decls.get(pid) {
                Some(decl) => (EmittedAuth::from_local(compiled, decl), false),
                None => (EmittedAuth::unbound(), true),
            },
            None => (EmittedAuth::unbound(), true),
        }
    }
}

/// Check authority for a (src → tgt) push. Returns true if the push should proceed.
///
/// - Advisory (`auth_ctx` is `None`): always returns `true`, zero cost.
/// - Audit: evaluates gate, emits `AuthViolation` if denied, still returns `true`.
/// - Enforced: evaluates gate, returns `false` if denied (caller skips enqueue).
fn try_auth(
    src:      Uid,
    tgt:      Uid,
    deps:     &DepRegistry,
    auth_ctx: Option<&AuthContext<'_>>,
    events:   &mut Vec<PropEvent>,
) -> bool {
    let Some(ctx) = auth_ctx else { return true; };

    let compiled = deps.compiled_label(src, tgt);
    let (emitted, is_unbound) = ctx.emitted_auth(src, &compiled);

    if is_unbound {
        return match ctx.unbound_policy {
            UnboundPolicy::Allow => true,
            UnboundPolicy::AuditOnly => {
                events.push(PropEvent::AuthViolation { src, tgt, reason: GateReason::UnboundSource });
                true
            }
            UnboundPolicy::Deny => {
                // In Audit mode, Deny means log but still propagate (Audit never suppresses).
                // In Enforced mode, Deny means suppress.
                if ctx.mode == AuthorityMode::Audit {
                    events.push(PropEvent::AuthViolation { src, tgt, reason: GateReason::UnboundSource });
                    true
                } else {
                    false // Enforced + Deny: suppress
                }
            }
        };
    }

    match authority_gate(&compiled, &emitted) {
        Ok(()) => true,
        Err(reason) => match ctx.mode {
            AuthorityMode::Advisory => true, // defensive; Advisory path skips try_auth entirely
            AuthorityMode::Audit => {
                events.push(PropEvent::AuthViolation { src, tgt, reason });
                true // Audit: log but do not suppress
            }
            AuthorityMode::Enforced => false, // suppress unauthorized enqueue
        }
    }
}

/// Run one propagation step for a single Push work item.
///
/// `causal_time` is the engine's current `ProductTime` — used to gate which
/// subscribers actually receive this push via `should_propagate`. After a
/// push is decided, `deps.update_last_seen` stamps the dep pair so subsequent
/// pushes at the same causal time are skipped.
///
/// ## Hot-path optimizations
///
/// **Opt 1** — Dense value cache: dep values are read from `value_store.cache_slice()`
/// via pre-computed `u32` indices (no HAMT lookup per dep). Falls back to
/// `graph.node()` when indexed path is unavailable.
///
/// **Opt 4** — SmallVec for dep_vals: avoids heap allocation for the common case
/// of ≤8 deps (stack-allocated `SmallVec<[T; 8]>`).
///
/// **Opt 5** — Software prefetch: on x86_64, issues `PREFETCHT0` for all dep
/// cache lines before the value-collection loop, hiding DRAM latency.
pub fn step_push(
    node_id: Uid,
    graph: &mut Graph,
    deps: &mut DepRegistry,
    value_store: &mut ValueStore,
    causal_time: &ProductTime,
    auth_ctx: Option<&AuthContext<'_>>,
    demanded: &FxHashSet<Uid>,
    stats: &mut PropStats,
) -> (Option<T>, Vec<PropEvent>, Vec<WorkItem>) {
    let Some(node) = graph.node(node_id) else { return (None, vec![], vec![]) };

    // Input nodes don't self-compute — only change via SET_VALUE.
    if matches!(node.kind, NodeKind::Input) { return (None, vec![], vec![]); }

    let NodeKind::Computed(ref rule) = node.kind.clone() else { unreachable!() };
    let old_val = node.value;

    // ── Opt 2: counter-based shortcut for aggregate rules ─────────────────────
    //
    // For MeetAll and JoinAny, the aggregate result is fully determined by the
    // dep-value distribution counters: Zero wins (Bochvar), Neg second (Pending),
    // Pos last (Ready).  This is exact under this engine's propagation model —
    // rule.eval is only called when all deps are Pos (gives Pos) or any dep is Zero
    // (hardcoded to T::Zero regardless of rule).
    //
    // Excluded for Stabilizing mode: the Conflicted path needs dep_ids to build the
    // e-graph region; fall through to full dep_vals collection in that case.
    let node_mode = deps.mode_of(node_id);
    if matches!(rule, ComputeRule::MeetAll | ComputeRule::JoinAny)
        && node_mode != ExecMode::Stabilizing
    {
        if let Some(ctrs) = deps.dep_counter(node_id) {
            let n_deps = {
                let di = deps.ordered_dep_indices_of(node_id);
                if !di.is_empty() { di.len() } else { deps.ordered_deps_of(node_id).len() }
            };
            // Guard: counter is only valid when total matches dep count.
            if n_deps > 0 && ctrs.total() as usize == n_deps {
                let new_val = ctrs.eval_aggregate();
                if new_val == T::Neg {
                    // Pending — nothing to do.
                    return (None, vec![], vec![]);
                }
                let mut events = vec![];
                let mut work   = vec![];
                if new_val == T::Zero {
                    // Conflicted — Bochvar infection.
                    events.push(PropEvent::BochvarInfected { node: node_id, kind: None });
                    if new_val != old_val {
                        graph.node_mut(node_id).unwrap().set_value(new_val);
                        value_store.set(node_id, new_val);
                        events.push(PropEvent::ValueChanged { node: node_id, old: old_val, new: new_val });
                        stats.nodes_materialized += 1;
                    }
                    // Opt 2: always update dep counters for all downstream subscribers
                    // (even when this node is Lazy/Stabilizing — downstream counters must
                    // stay accurate). Push only when node_mode is Eager.
                    for sub in deps.subs_vec(node_id) {
                        if new_val != old_val {
                            deps.update_dep_counter(sub, old_val, new_val);
                        }
                        if node_mode != ExecMode::Eager { continue; }
                        let gate = deps.dep_meta(node_id, sub)
                            .map_or(true, |m| should_propagate(m, causal_time));
                        if !gate { stats.pushes_suppressed_by_delta += 1; continue; }
                        let sub_mode = deps.mode_of(sub);
                        if sub_mode != ExecMode::Eager && !demanded.contains(&sub) {
                            stats.pushes_suppressed_by_mode += 1; continue;
                        }
                        if try_auth(node_id, sub, deps, auth_ctx, &mut events) {
                            work.push(WorkItem::Push(sub));
                            deps.update_last_seen(node_id, sub, *causal_time);
                        }
                    }
                } else {
                    // Ready — new_val is T::Pos.
                    if new_val != old_val {
                        graph.node_mut(node_id).unwrap().set_value(new_val);
                        value_store.set(node_id, new_val);
                        events.push(PropEvent::ValueChanged { node: node_id, old: old_val, new: new_val });
                        stats.nodes_materialized += 1;
                        for sub in deps.subs_vec(node_id) {
                            // Opt 2: keep dep counters accurate for all downstream subscribers,
                            // regardless of push eligibility (mirrors SetValue in engine.rs).
                            deps.update_dep_counter(sub, old_val, new_val);
                            let gate = deps.dep_meta(node_id, sub)
                                .map_or(true, |m| should_propagate(m, causal_time));
                            if !gate { stats.pushes_suppressed_by_delta += 1; continue; }
                            let sub_mode = deps.mode_of(sub);
                            if sub_mode != ExecMode::Eager && !demanded.contains(&sub) {
                                stats.pushes_suppressed_by_mode += 1; continue;
                            }
                            if try_auth(node_id, sub, deps, auth_ctx, &mut events) {
                                work.push(WorkItem::Push(sub));
                                deps.update_last_seen(node_id, sub, *causal_time);
                            }
                        }
                    }
                }
                return (Some(new_val), events, work);
            }
        }
    }

    // ── Collect dep values (Opt 1 + Opt 4 + Opt 5) ───────────────────────────
    //
    // Prefer dense-index path (O(1) cache slice lookup) over graph.node() (HAMT).
    // dep_ids kept alive for Stabilize region construction in Conflicted path.
    let dep_indices = deps.ordered_dep_indices_of(node_id);
    // Collect into an owned Vec so `deps` can be mutably borrowed later
    // (e.g. update_dep_counter in the Conflicted path's counter-update loop).
    let dep_ids: Vec<Uid> = deps.ordered_deps_of(node_id).to_vec();

    let dep_vals: SmallVec<[T; 8]> = if !dep_indices.is_empty() {
        // Fast path: dense cache lookup — no hash, just pointer + index.
        let cache = value_store.cache_slice();

        // Opt 5: software prefetch — issue all loads before the read loop so
        // DRAM latency (~200 cycles) overlaps with the loop setup overhead.
        #[cfg(target_arch = "x86_64")]
        {
            for &idx in dep_indices {
                // SAFETY: `idx` was assigned by ValueStore::alloc; cache[idx] is in bounds.
                unsafe {
                    std::arch::x86_64::_mm_prefetch(
                        cache.as_ptr().add(idx as usize) as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                }
            }
        }

        dep_indices.iter().map(|&idx| cache[idx as usize]).collect()
    } else {
        // Fallback: HAMT lookup (nodes registered before ValueStore or in tests).
        dep_ids.iter()
            .map(|&d| graph.node(d).map(|n| n.value).unwrap_or(T::Neg))
            .collect()
    };

    let prop_state = T::propagation_state(&dep_vals);

    match (prop_state, deps.mode_of(node_id)) {
        // ── Ready: all deps Pos → compute, push downstream ────────────────
        (PropState::Ready, _) => {
            let new_val = rule.eval(&dep_vals).unwrap_or(T::Neg);
            let mut events = vec![];
            let mut work = vec![];

            if new_val != old_val {
                // Use set_value to ensure version is bumped (I8).
                graph.node_mut(node_id).unwrap().set_value(new_val);
                // Mirror write to dense cache (Opt 1 invariant).
                value_store.set(node_id, new_val);
                events.push(PropEvent::ValueChanged { node: node_id, old: old_val, new: new_val });
                stats.nodes_materialized += 1;

                // Delta-gated downstream push: enqueue Eager subs, and also
                // Lazy/Stabilizing subs that have an outstanding DEMAND on them
                // (explicit observation unlocks one propagation step).
                for sub in deps.subs_vec(node_id) {
                    // Opt 2: keep dep counters accurate for all downstream subscribers,
                    // regardless of push eligibility (mirrors SetValue in engine.rs).
                    deps.update_dep_counter(sub, old_val, new_val);
                    let gate = deps.dep_meta(node_id, sub)
                        .map_or(true, |m| should_propagate(m, causal_time));
                    if !gate {
                        stats.pushes_suppressed_by_delta += 1;
                        continue;
                    }
                    let mode = deps.mode_of(sub);
                    if mode != ExecMode::Eager && !demanded.contains(&sub) {
                        stats.pushes_suppressed_by_mode += 1;
                        continue;
                    }
                    if try_auth(node_id, sub, deps, auth_ctx, &mut events) {
                        work.push(WorkItem::Push(sub));
                        deps.update_last_seen(node_id, sub, *causal_time);
                    }
                }
            }
            (Some(new_val), events, work)
        }

        // ── Pending: some dep Neg → wait (Inhibition: do nothing yet) ─────
        (PropState::Pending, ExecMode::Lazy) | (PropState::Pending, ExecMode::Eager) => {
            (None, vec![], vec![])
        }

        // ── Conflicted: some dep Zero → Bochvar infection ─────────────────
        (PropState::Conflicted, mode) => {
            let new_val = T::Zero; // Bochvar: infect output
            let mut events = vec![PropEvent::BochvarInfected { node: node_id, kind: None }];
            let mut work = vec![];

            if new_val != old_val {
                graph.node_mut(node_id).unwrap().set_value(new_val);
                // Mirror write to dense cache (Opt 1 invariant).
                value_store.set(node_id, new_val);
                events.push(PropEvent::ValueChanged { node: node_id, old: old_val, new: new_val });
                stats.nodes_materialized += 1;
                // Opt 2: update dep counters for all downstream subscribers unconditionally.
                // (Eager-push loop below only fires in ExecMode::Eager; the counter update
                // must happen for all modes so that downstream MeetAll/JoinAny nodes see the
                // correct distribution when they are eventually pushed.)
                for sub in deps.subs_vec(node_id) {
                    deps.update_dep_counter(sub, old_val, new_val);
                }
            }

            match mode {
                ExecMode::Stabilizing => {
                    // Route to structural e-graph stabilizer (region-bounded).
                    let region: Vec<Uid> = dep_ids.iter().copied()
                        .chain(std::iter::once(node_id))
                        .collect();
                    work.push(WorkItem::Stabilize(region.clone()));
                    events.push(PropEvent::StabilizeQueued { region });
                }
                ExecMode::Lazy => {
                    // Inhibition: mark as pending demand; do not push.
                }
                ExecMode::Eager => {
                    // Propagate Zero infection downstream (delta-gated).
                    for sub in deps.subs_vec(node_id) {
                        let gate = deps.dep_meta(node_id, sub)
                            .map_or(true, |m| should_propagate(m, causal_time));
                        if !gate {
                            stats.pushes_suppressed_by_delta += 1;
                            continue;
                        }
                        let mode = deps.mode_of(sub);
                        if mode != ExecMode::Eager && !demanded.contains(&sub) {
                            stats.pushes_suppressed_by_mode += 1;
                            continue;
                        }
                        if try_auth(node_id, sub, deps, auth_ctx, &mut events) {
                            work.push(WorkItem::Push(sub));
                            deps.update_last_seen(node_id, sub, *causal_time);
                        }
                    }
                }
            }
            (Some(new_val), events, work)
        }

        // ── Pending + Stabilizing: conflicted dep in stabilizing mode ─────
        (PropState::Pending, ExecMode::Stabilizing) => {
            (None, vec![], vec![])
        }
    }
}

/// Run one Demand step: pull-evaluate a node and its deps recursively.
/// Returns work items for any deps that need to be pushed first.
pub fn step_demand(
    node_id: Uid,
    graph: &mut Graph,
    deps: &DepRegistry,
) -> (Vec<PropEvent>, Vec<WorkItem>) {
    let mut work = vec![];
    let events = vec![PropEvent::DemandFired { node: node_id }];

    // Pull all deps first (sorted for deterministic queue ordering).
    let mut dep_ids: Vec<Uid> = deps.deps_of(node_id).collect();
    dep_ids.sort_unstable();
    for dep_id in dep_ids {
        let dep_val = graph.node(dep_id).map(|n| n.value).unwrap_or(T::Neg);
        if dep_val.is_pending() {
            // Recursively demand the dep
            work.push(WorkItem::Demand(dep_id));
        }
    }

    // After deps resolve, push this node
    work.push(WorkItem::Push(node_id));
    (events, work)
}

/// Propagation engine: wraps a work queue and drives the three modes.
pub struct PropEngine {
    queue: VecDeque<WorkItem>,
    pub events: Vec<PropEvent>,
    /// Cumulative propagation statistics. Reset via `reset_stats`.
    pub stats: PropStats,
    /// Set of nodes that have an outstanding DEMAND in this drain cycle.
    /// Lazy/Stabilizing subscribers in this set accept pushes from their deps,
    /// allowing demand to correctly traverse multi-hop Lazy chains.
    /// Cleared at the end of each `drain` call.
    demanded: FxHashSet<Uid>,
}

impl PropEngine {
    pub fn new() -> Self {
        PropEngine {
            // Opt 4: pre-allocate common capacities to avoid first-push reallocs.
            // 64 work items covers most single-op propagation waves.
            // 256 events is generous for the typical sub-10-node demo graphs.
            queue:    VecDeque::with_capacity(64),
            events:   Vec::with_capacity(256),
            stats:    PropStats::default(),
            demanded: FxHashSet::default(),
        }
    }

    pub fn enqueue(&mut self, item: WorkItem) {
        self.queue.push_back(item);
    }

    /// Reset all accumulated statistics to zero.
    pub fn reset_stats(&mut self) { self.stats.reset(); }

    /// Drain the work queue, applying each step against the graph and registry.
    ///
    /// `causal_time` is the engine's current `ProductTime` — threaded into
    /// `step_push` for delta-gated propagation decisions.
    /// `auth_ctx` is `Some` in Audit/Enforced modes, `None` in Advisory (zero cost).
    /// `value_store` is the dense value cache; step_push uses it for O(1) dep reads
    /// and writes back whenever a node's value changes.
    /// Runs until the queue is empty (or `max_steps` exceeded for safety).
    ///
    /// Statistics are accumulated into `self.stats` and persist across calls
    /// until `reset_stats` is called.
    pub fn drain(
        &mut self,
        graph: &mut Graph,
        deps: &mut DepRegistry,
        value_store: &mut ValueStore,
        causal_time: &ProductTime,
        auth_ctx: Option<&AuthContext<'_>>,
        // Opt 5: dense-slot bitset of nodes already evaluated by compiled regions
        // this drain cycle.  Push items for matching nodes are skipped.
        // Pass &[] when no compiled regions ran (zero-cost: slice is empty).
        compiled_handled: &[u64],
        max_steps: usize,
    ) -> Result<(), PropError> {
        let mut steps = 0;

        // Track the concurrent demand frontier as a running count so we avoid
        // O(n) queue scans. Initialise from items already in the queue before
        // this drain call (e.g. a Demand enqueued by engine.apply).
        let mut demand_frontier: usize = self.queue
            .iter()
            .filter(|w| matches!(w, WorkItem::Demand(_)))
            .count();
        if demand_frontier > self.stats.max_demand_frontier {
            self.stats.max_demand_frontier = demand_frontier;
        }

        while let Some(item) = self.queue.pop_front() {
            if steps >= max_steps {
                return Err(PropError::StepLimitExceeded(max_steps));
            }
            steps += 1;

            // Queue depth at the moment we dequeue (before adding new work).
            let depth = self.queue.len() + 1; // +1 for the item we just popped
            if depth > self.stats.max_queue_depth {
                self.stats.max_queue_depth = depth;
            }

            match item {
                WorkItem::Push(id) => {
                    // Opt 5: skip nodes already evaluated by the compiled tier.
                    // The compiled circuit wrote the correct value into ValueStore;
                    // a warm re-evaluation would compute the same result (no-op).
                    if !compiled_handled.is_empty() {
                        if let Some(dense) = value_store.dense_index(id) {
                            let (word, bit) = (dense as usize / 64, dense as usize % 64);
                            if word < compiled_handled.len()
                                && compiled_handled[word] & (1u64 << bit) != 0
                            {
                                self.stats.pushes_suppressed_by_compiled_handled += 1;
                                continue;
                            }
                        }
                    }
                    let (_, mut evs, work) = step_push(
                        id, graph, deps, value_store, causal_time, auth_ctx, &self.demanded, &mut self.stats,
                    );
                    self.events.append(&mut evs);
                    for w in work {
                        if matches!(w, WorkItem::Demand(_)) {
                            demand_frontier += 1;
                            if demand_frontier > self.stats.max_demand_frontier {
                                self.stats.max_demand_frontier = demand_frontier;
                            }
                        }
                        self.queue.push_back(w);
                    }
                }
                WorkItem::Demand(id) => {
                    if demand_frontier > 0 { demand_frontier -= 1; }
                    self.stats.demands_fired += 1;
                    // Mark this node as explicitly demanded so its Lazy/Stabilizing
                    // subscribers accept pushes in this drain cycle.
                    self.demanded.insert(id);
                    let (mut evs, work) = step_demand(id, graph, deps);
                    self.events.append(&mut evs);
                    for w in work {
                        match &w {
                            WorkItem::Demand(_) => {
                                // DFS ordering: sub-demands to the front so deps are
                                // evaluated before the Push(id) that follows them.
                                demand_frontier += 1;
                                if demand_frontier > self.stats.max_demand_frontier {
                                    self.stats.max_demand_frontier = demand_frontier;
                                }
                                self.queue.push_front(w);
                            }
                            _ => {
                                self.queue.push_back(w);
                            }
                        }
                    }
                }
                WorkItem::Stabilize(region) => {
                    // Structural e-graph stabilization handled externally (egraph.rs).
                    // The engine queues it here; the caller drains the stabilize queue.
                    self.stats.stabilize_region_size_total += region.len();
                    self.events.push(PropEvent::StabilizeQueued { region });
                }
            }
        }
        // Clear the demand set — it's only valid within a single drain cycle.
        self.demanded.clear();
        Ok(())
    }

    pub fn is_idle(&self) -> bool { self.queue.is_empty() }
}

impl Default for PropEngine {
    fn default() -> Self { Self::new() }
}

#[derive(Debug, thiserror::Error)]
pub enum PropError {
    #[error("propagation step limit {0} exceeded — possible cycle in dependency graph")]
    StepLimitExceeded(usize),
}
