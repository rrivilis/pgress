//! Dependency registry — the subscription layer.
//!
//! For each node tracks:
//!   deps(n)  — the set of upstream nodes n reads from
//!   subs(n)  — the set of downstream nodes that read from n
//!   mode(n)  — ExecMode: Eager | Lazy | Stabilizing
//!
//! The registry is the live operational view of the graph edges.
//! The Graph owns the *structural* edge records; DepRegistry owns the
//! *propagation* view (which subs to push to, in what mode).
//!
//! ## Dep metadata (DepMeta)
//!
//! Every (source, subscriber) pair may carry a `DepMeta` declaring the
//! observability window for that specific dep:
//!
//! - `since / until`: bounded time window — dep is only active within this range
//! - `projection`:    `ProjectionMask` — which changes are visible at this dep
//! - `sensitivity`:   `Sensitivity` — how to react to visible changes
//!
//! Default meta (used by plain `subscribe`) is wildcard projection + exact
//! sensitivity with no expiry, which preserves existing eager behaviour.

use im::{HashMap as ImMap, HashSet as ImSet};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use crate::{
    node::{DepList, ExecMode},
    partition::{CompiledEdgeLabel, EdgeLabel},
    region::AdjEpoch,
    sensitivity::Sensitivity,
    ternary::T,
    time::{Frontier, ProductTime, Time, TimeDim},
    uid::Uid,
};

// ── DepCounters (Opt 2) ───────────────────────────────────────────────────────

/// Per-subscriber distribution of upstream dep values over {Neg, Zero, Pos}.
///
/// Maintained incrementally: one counter update per dep-value change instead of
/// O(k) dep_vals scan in step_push. `eval_aggregate` answers `T::propagation_state`
/// semantics in O(1): Zero wins (Bochvar), then Neg (Pending), then Pos (Ready).
///
/// Valid for MeetAll and JoinAny under this engine's propagation model, where
/// `rule.eval` is called only when all deps are Pos (Ready) — giving Pos — or
/// any dep is Zero (Conflicted) — hardcoded to Zero. Both rules share the same
/// aggregate function under these semantics.
///
/// Consistency guard: `total()` must equal the subscriber's dep count. If they
/// disagree (e.g. counter was never initialized for an indirect subscription),
/// `step_push` falls through to the full dep_vals scan.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct DepCounters {
    pub neg_count:  u32,
    pub zero_count: u32,
    pub pos_count:  u32,
}

impl DepCounters {
    /// neg + zero + pos.
    #[inline] pub fn total(&self) -> u32 {
        self.neg_count + self.zero_count + self.pos_count
    }

    /// Aggregate result matching propagation_state semantics.
    /// Zero takes priority (Bochvar), Neg second (Pending), Pos last (Ready).
    #[inline] pub fn eval_aggregate(&self) -> T {
        if self.zero_count > 0 { T::Zero }
        else if self.neg_count > 0 { T::Neg }
        else { T::Pos }
    }

    #[inline] pub fn add(&mut self, v: T) {
        match v {
            T::Neg  => self.neg_count  += 1,
            T::Zero => self.zero_count += 1,
            T::Pos  => self.pos_count  += 1,
        }
    }

    #[inline] pub fn remove(&mut self, v: T) {
        match v {
            T::Neg  => self.neg_count  = self.neg_count.saturating_sub(1),
            T::Zero => self.zero_count = self.zero_count.saturating_sub(1),
            T::Pos  => self.pos_count  = self.pos_count.saturating_sub(1),
        }
    }

    /// Decrement old bucket, increment new bucket. No-op if old == new.
    #[inline] pub fn update(&mut self, old: T, new: T) {
        if old == new { return; }
        self.remove(old);
        self.add(new);
    }
}

// ── DepMeta ───────────────────────────────────────────────────────────────────

/// Per-dep observability metadata. Controls the recompute window and filter.
///
/// `label` replaces the former `projection: ProjectionMask` field.
/// `should_propagate` reads `meta.label.projection_mask`; the hot path is
/// otherwise unchanged. Authority checks (`authority_gate`) read `label`
/// in Audit/Enforced mode only — Advisory is zero-cost.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepMeta {
    /// The dep became active at this time.
    pub since:       Time,
    /// The dep expires when the frontier advances past this point (None = no expiry).
    pub until:       Option<Frontier>,
    /// Compiled edge-label: projection mask + authority metadata.
    /// Replaces the former standalone `projection: ProjectionMask`.
    pub label:       CompiledEdgeLabel,
    /// How to interpret visible changes.
    pub sensitivity: Sensitivity,
    /// The causal time at which this subscriber last processed a push from source.
    pub last_seen:   ProductTime,
}

impl Default for DepMeta {
    fn default() -> Self {
        DepMeta {
            since:       Time::ZERO,
            until:       None,
            label:       CompiledEdgeLabel::permissive_default(),
            sensitivity: Sensitivity::Exact,
            last_seen:   ProductTime::ZERO,
        }
    }
}

impl DepMeta {
    pub fn new(label: CompiledEdgeLabel, sensitivity: Sensitivity) -> Self {
        DepMeta { label, sensitivity, ..Default::default() }
    }

    /// Convenience: construct with a declared EdgeLabel (compiles immediately).
    pub fn from_label(label: &EdgeLabel, sensitivity: Sensitivity) -> Self {
        DepMeta {
            label: CompiledEdgeLabel::compile(label),
            sensitivity,
            ..Default::default()
        }
    }

    /// Whether this dep is active at the given time.
    pub fn is_active_at(&self, t: &Time) -> bool {
        t.dominates(&self.since)
            && self.until.as_ref().map_or(true, |u| !u.dominates(t))
    }
}

// ── Propagation gate ──────────────────────────────────────────────────────────

/// Returns true if source→subscriber should propagate at `current_time`.
///
/// Reads `meta.label.projection_mask` (formerly `meta.projection`).
/// A push is warranted when `current_time` has advanced in at least one
/// dimension that the projection mask observes, relative to `meta.last_seen`.
pub fn should_propagate(meta: &DepMeta, current_time: &ProductTime) -> bool {
    for dim in [
        TimeDim::Identity,
        TimeDim::Attribute,
        TimeDim::Dependency,
        TimeDim::Egraph,
        TimeDim::Boundary,
    ] {
        if meta.label.projection_mask.dims.contains(dim)
            && current_time.get(dim) > meta.last_seen.get(dim)
        {
            return true;
        }
    }
    false
}

// ── DepRegistry ───────────────────────────────────────────────────────────────

/// Dense index parallel to `ordered_deps`.
/// `ordered_dep_indices[subscriber][i]` is the `ValueStore` index for
/// `ordered_deps[subscriber][i]`, enabling O(1) dep-value reads in step_push.
type DenseIdxList = SmallVec<[u32; 4]>;

/// The dependency/subscription registry.
///
/// Cloning is O(1) (structural sharing via `im`).
#[derive(Clone, Debug, Default)]
pub struct DepRegistry {
    /// Upstream deps: node → set of nodes it reads.
    deps: ImMap<Uid, ImSet<Uid>>,
    /// Downstream subs: node → set of nodes that read it.
    subs: ImMap<Uid, ImSet<Uid>>,
    /// Execution mode per node.
    modes: ImMap<Uid, ExecMode>,
    /// Ordered dep list per node (preserves port order for ComputeRule eval).
    ordered_deps: ImMap<Uid, DepList>,
    /// Parallel to `ordered_deps`: dense ValueStore indices for O(1) dep-value reads.
    /// Populated by `subscribe_with_meta_indexed`; empty for subs registered without index.
    ordered_dep_indices: ImMap<Uid, DenseIdxList>,
    /// Per-dep observability metadata: (source, subscriber) → DepMeta.
    dep_meta: ImMap<(Uid, Uid), DepMeta>,
    /// Adjacency epoch per node — bumped on subscribe / unsubscribe / remove_node.
    /// Used by `RegionArtifactCache` to detect topology staleness.
    adj_epochs: FxHashMap<Uid, AdjEpoch>,
    /// Monotone counter; next epoch value to assign.
    adj_epoch_counter: AdjEpoch,
    /// Opt 2: per-subscriber dep-value distribution counters.
    /// Key = subscriber node Uid. Populated by Engine at EdgeConnect/Subscribe time
    /// using the dep's actual current value. Updated on every SetValue/Reflect.
    /// Not part of im-backed structural snapshot state (derived from live values).
    dep_counters: FxHashMap<Uid, DepCounters>,
}

impl DepRegistry {
    pub fn new() -> Self { Self::default() }

    // ── Registration ──────────────────────────────────────────────────────────

    /// Register that `subscriber` depends on `source` with default metadata.
    /// Does NOT populate `ordered_dep_indices` — call `subscribe_with_meta_indexed`
    /// from the Engine (which has access to ValueStore) to enable fast dep reads.
    pub fn subscribe(&mut self, source: Uid, subscriber: Uid) {
        self.subscribe_with_meta(source, subscriber, DepMeta::default());
    }

    /// Register that `subscriber` depends on `source` with explicit metadata.
    pub fn subscribe_with_meta(&mut self, source: Uid, subscriber: Uid, meta: DepMeta) {
        self.deps.entry(subscriber).or_default().insert(source);
        self.subs.entry(source).or_default().insert(subscriber);
        self.ordered_deps.entry(subscriber).or_default().push(source);
        self.dep_meta.insert((source, subscriber), meta);
        self.bump_epoch(source);
        self.bump_epoch(subscriber);
    }

    /// Like `subscribe_with_meta` but also records `source_dense` — the
    /// ValueStore dense index for `source` — into `ordered_dep_indices`.
    /// This enables O(1) dep-value reads via `ValueStore::cache_slice()` in step_push.
    pub fn subscribe_with_meta_indexed(
        &mut self,
        source: Uid,
        subscriber: Uid,
        source_dense: u32,
        meta: DepMeta,
    ) {
        self.deps.entry(subscriber).or_default().insert(source);
        self.subs.entry(source).or_default().insert(subscriber);
        self.ordered_deps.entry(subscriber).or_default().push(source);
        self.ordered_dep_indices.entry(subscriber).or_default().push(source_dense);
        self.dep_meta.insert((source, subscriber), meta);
        self.bump_epoch(source);
        self.bump_epoch(subscriber);
    }

    /// Deregister a source → subscriber dependency.
    /// Removes from `ordered_deps` and `ordered_dep_indices` by scanning for
    /// the first occurrence of `source` (O(n) for n deps, acceptable since n is small).
    pub fn unsubscribe(&mut self, source: Uid, subscriber: Uid) {
        if let Some(d) = self.deps.get_mut(&subscriber) { d.remove(&source); }
        if let Some(s) = self.subs.get_mut(&source)     { s.remove(&subscriber); }

        // Remove from ordered_deps and parallel ordered_dep_indices by position.
        if let Some(od) = self.ordered_deps.get_mut(&subscriber) {
            let pos = od.iter().position(|id| *id == source);
            if let Some(i) = pos {
                od.remove(i);
                // Mirror removal in ordered_dep_indices (same position).
                if let Some(odi) = self.ordered_dep_indices.get_mut(&subscriber) {
                    if i < odi.len() { odi.remove(i); }
                }
            }
        }
        self.dep_meta.remove(&(source, subscriber));
        self.bump_epoch(source);
        self.bump_epoch(subscriber);
    }

    /// Remove all dep/sub entries for a node (used on node deletion).
    pub fn remove_node(&mut self, id: Uid) {
        let upstream: Vec<Uid> = self.deps.get(&id)
            .map(|s| s.iter().copied().collect()).unwrap_or_default();
        for src in upstream { self.unsubscribe(src, id); }

        let downstream: Vec<Uid> = self.subs.get(&id)
            .map(|s| s.iter().copied().collect()).unwrap_or_default();
        for sub in &downstream {
            if let Some(d) = self.deps.get_mut(sub) { d.remove(&id); }
            if let Some(od) = self.ordered_deps.get_mut(sub) {
                let pos = od.iter().position(|x| *x == id);
                if let Some(i) = pos {
                    od.remove(i);
                    if let Some(odi) = self.ordered_dep_indices.get_mut(sub) {
                        if i < odi.len() { odi.remove(i); }
                    }
                }
            }
            self.dep_meta.remove(&(id, *sub));
        }
        self.deps.remove(&id);
        self.subs.remove(&id);
        self.modes.remove(&id);
        self.ordered_deps.remove(&id);
        self.ordered_dep_indices.remove(&id);
        // Opt 2: drop dep counter for this node (Engine removes dep contributions
        // from downstream counters before calling remove_node via DelEdge loops).
        self.dep_counters.remove(&id);
        self.bump_epoch(id);
    }

    // ── Adjacency epoch ───────────────────────────────────────────────────────

    /// Current adjacency epoch for `node`.
    /// Returns 0 if the node has never been touched by a topology mutation.
    pub fn adj_epoch(&self, node: Uid) -> AdjEpoch {
        self.adj_epochs.get(&node).copied().unwrap_or(0)
    }

    /// Maximum adjacency epoch across a set of nodes.
    /// Used by `RegionArtifactCache` to validate compiled artifacts.
    pub fn max_epoch_for_nodes(&self, nodes: &[Uid]) -> AdjEpoch {
        nodes.iter().map(|&n| self.adj_epoch(n)).max().unwrap_or(0)
    }

    /// Bump the adjacency epoch for `node` and return the new value.
    fn bump_epoch(&mut self, node: Uid) -> AdjEpoch {
        self.adj_epoch_counter += 1;
        let e = self.adj_epoch_counter;
        self.adj_epochs.insert(node, e);
        e
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    pub fn deps_of(&self, id: Uid) -> impl Iterator<Item = Uid> + '_ {
        self.deps.get(&id).into_iter().flat_map(|s| s.iter().copied())
    }

    pub fn ordered_deps_of(&self, id: Uid) -> &[Uid] {
        self.ordered_deps.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Dense ValueStore indices parallel to `ordered_deps_of`.
    /// Returns an empty slice if this node was subscribed without an index
    /// (i.e., via `subscribe` / `subscribe_with_meta` rather than `subscribe_with_meta_indexed`).
    /// step_push falls back to graph lookup when this is empty.
    #[inline(always)]
    pub fn ordered_dep_indices_of(&self, id: Uid) -> &[u32] {
        self.ordered_dep_indices.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn subs_of(&self, id: Uid) -> impl Iterator<Item = Uid> + '_ {
        self.subs.get(&id).into_iter().flat_map(|s| s.iter().copied())
    }

    pub fn mode_of(&self, id: Uid) -> ExecMode {
        self.modes.get(&id).copied().unwrap_or_default()
    }

    pub fn set_mode(&mut self, id: Uid, mode: ExecMode) {
        self.modes.insert(id, mode);
    }

    pub fn has_deps(&self, id: Uid) -> bool {
        self.deps.get(&id).map(|s| !s.is_empty()).unwrap_or(false)
    }

    pub fn has_subs(&self, id: Uid) -> bool {
        self.subs.get(&id).map(|s| !s.is_empty()).unwrap_or(false)
    }

    pub fn subs_vec(&self, id: Uid) -> Vec<Uid> {
        // Sort for deterministic propagation order across re-runs.
        // im::HashSet uses a per-instance random seed (RandomState) so iteration
        // order differs between two Engine instances in the same process.
        let mut v: Vec<Uid> = self.subs.get(&id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        v.sort_unstable();
        v
    }

    /// Retrieve the dep metadata for a (source, subscriber) pair.
    pub fn dep_meta(&self, source: Uid, subscriber: Uid) -> Option<&DepMeta> {
        self.dep_meta.get(&(source, subscriber))
    }

    /// Retrieve the compiled edge label for a (source, subscriber) pair.
    /// Returns the permissive default if no explicit label has been set.
    pub fn compiled_label(&self, source: Uid, subscriber: Uid) -> CompiledEdgeLabel {
        self.dep_meta
            .get(&(source, subscriber))
            .map(|m| m.label.clone())
            .unwrap_or_default()
    }

    /// Update the compiled edge label for a (source, subscriber) pair.
    pub fn set_compiled_label(&mut self, source: Uid, subscriber: Uid, label: CompiledEdgeLabel) {
        if let Some(meta) = self.dep_meta.get_mut(&(source, subscriber)) {
            meta.label = label;
        }
    }

    /// Update `last_seen` for a (source, subscriber) pair after a successful push.
    /// Called by the propagation engine after deciding to enqueue a sub.
    pub fn update_last_seen(&mut self, source: Uid, subscriber: Uid, time: ProductTime) {
        if let Some(meta) = self.dep_meta.get_mut(&(source, subscriber)) {
            meta.last_seen = meta.last_seen.join(&time);
        }
    }

    /// Subs that are active at time `t` and whose projection observes `delta`.
    /// Used by the propagation engine to gate downstream push notifications.
    pub fn active_subs_for(&self, source: Uid, t: &Time) -> Vec<Uid> {
        self.subs_vec(source)
            .into_iter()
            .filter(|&sub| {
                self.dep_meta
                    .get(&(source, sub))
                    .map_or(true, |m| m.is_active_at(t))
            })
            .collect()
    }

    // ── Dep counter access (Opt 2) ────────────────────────────────────────────

    /// Current dep-value distribution for a subscriber node.
    /// Returns `None` if no deps have been registered via `add_dep_to_counter`.
    pub fn dep_counter(&self, id: Uid) -> Option<&DepCounters> {
        self.dep_counters.get(&id)
    }

    /// Record that `subscriber` gained a dep whose current value is `dep_val`.
    /// Called by Engine at subscribe time with the dep's actual current value
    /// (not assumed T::Neg — the dep may already have a live value).
    pub fn add_dep_to_counter(&mut self, subscriber: Uid, dep_val: T) {
        self.dep_counters.entry(subscriber).or_default().add(dep_val);
    }

    /// Record that `subscriber` lost a dep that had value `dep_val`.
    /// Called by Engine before unsubscribe, with the dep's current value.
    pub fn remove_dep_from_counter(&mut self, subscriber: Uid, dep_val: T) {
        if let Some(c) = self.dep_counters.get_mut(&subscriber) {
            c.remove(dep_val);
        }
    }

    /// Record a dep-value transition `old → new` for `subscriber`.
    /// Called by Engine on every SetValue / Reflect that changes a dep's value.
    pub fn update_dep_counter(&mut self, subscriber: Uid, old: T, new: T) {
        if let Some(c) = self.dep_counters.get_mut(&subscriber) {
            c.update(old, new);
        }
    }
}
