//! E-graph equivalence saturation — the Reflection / Stabilize mode.
//!
//! When a node reaches Zero (Bochvar-infected), the stabilizer runs egg's
//! equivalence saturation over the conflicting e-class region. After saturation,
//! if a canonical form is found, the node's value is updated and propagation
//! continues. If the e-class remains conflicted, the node stays Zero.
//!
//! The rewrite rules here encode the algebraic theorems proved in Lean
//! (Pgrs.Algebra) — they are correct by construction.

use egg::{rewrite as rw, *};
use std::collections::{HashMap, HashSet, VecDeque};
use crate::{
    deps::DepRegistry,
    graph::Graph,
    node::{ComputeRule, NodeKind},
    ternary::T,
    uid::Uid,
};

// ── Language ──────────────────────────────────────────────────────────────────

// The e-graph expression language for ternary computations.
define_language! {
    pub enum PgrsLang {
        // Literal ternary values
        "neg" = LitNeg,
        "zero" = LitZero,
        "pos" = LitPos,
        // MV-algebra operations (unary)
        "mv-neg" = MvNeg([Id; 1]),
        // MV-algebra operations (binary)
        "mv-add" = MvAdd([Id; 2]),
        "mv-mul" = MvMul([Id; 2]),
        "mv-sub" = MvSub([Id; 2]),
        // Lattice
        "meet"   = Meet([Id; 2]),
        "join"   = Join([Id; 2]),
        // Merge (ternary)
        "merge"  = Merge([Id; 3]),
        // Bochvar (binary)
        "b-add"  = BAdd([Id; 2]),
        // Symbolic node reference (by UID string)
        Symbol(Symbol),
    }
}

// ── Analysis ──────────────────────────────────────────────────────────────────

/// E-class analysis: track the constant ternary value if all nodes in the
/// e-class are constant (used for constant folding / value resolution).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TernaryData {
    /// Constant value of this e-class, if known.
    pub constant: Option<T>,
}

impl Analysis<PgrsLang> for TernaryData {
    type Data = TernaryData;

    fn make(egraph: &EGraph<PgrsLang, Self>, enode: &PgrsLang) -> TernaryData {
        let c = |id: &Id| egraph[*id].data.constant;
        let constant = match enode {
            PgrsLang::LitNeg  => Some(T::Neg),
            PgrsLang::LitZero => Some(T::Zero),
            PgrsLang::LitPos  => Some(T::Pos),
            PgrsLang::MvNeg([x])    => c(x).map(|v| v.mv_neg()),
            PgrsLang::MvAdd([a, b]) => c(a).zip(c(b)).map(|(a,b)| a.mv_add(b)),
            PgrsLang::MvMul([a, b]) => c(a).zip(c(b)).map(|(a,b)| a.mv_mul(b)),
            PgrsLang::MvSub([a, b]) => c(a).zip(c(b)).map(|(a,b)| a.mv_sub(b)),
            PgrsLang::Meet  ([a, b]) => c(a).zip(c(b)).map(|(a,b)| a.meet(b)),
            PgrsLang::Join  ([a, b]) => c(a).zip(c(b)).map(|(a,b)| a.join(b)),
            PgrsLang::Merge([b,l,r]) => c(b).zip(c(l)).zip(c(r)).map(|((b,l),r)| T::merge(b,l,r)),
            PgrsLang::BAdd  ([a, b]) => c(a).zip(c(b)).map(|(a,b)| a.bochvar_add(b)),
            _ => None,
        };
        TernaryData { constant }
    }

    fn merge(&mut self, to: &mut TernaryData, from: TernaryData) -> DidMerge {
        match (to.constant, from.constant) {
            (None, Some(v)) => { to.constant = Some(v); DidMerge(true, false) }
            _ => DidMerge(false, false)
        }
    }
}

// ── Rewrite rules (from Lean proofs in Pgrs.Algebra) ─────────────────────────

pub fn pgrs_rules() -> Vec<Rewrite<PgrsLang, TernaryData>> {
    vec![
        // I1: Reflection is involutive
        rw!("neg-invol";     "(mv-neg (mv-neg ?x))" => "?x"),
        // I1: Zero is the fixed point of mv-neg
        rw!("neg-zero";      "(mv-neg zero)" => "zero"),
        // MV-add commutativity
        rw!("add-comm";      "(mv-add ?a ?b)" => "(mv-add ?b ?a)"),
        // MV-add left identity (Neg is the identity)
        rw!("add-id-l";      "(mv-add neg ?x)" => "?x"),
        rw!("add-id-r";      "(mv-add ?x neg)" => "?x"),
        // MV-mul commutativity
        rw!("mul-comm";      "(mv-mul ?a ?b)" => "(mv-mul ?b ?a)"),
        // MV-mul left identity (Pos is the identity)
        rw!("mul-id-l";      "(mv-mul pos ?x)" => "?x"),
        rw!("mul-id-r";      "(mv-mul ?x pos)" => "?x"),
        // Annihilator: x ⊕ ¬x = Pos
        rw!("annihilator";   "(mv-add ?x (mv-neg ?x))" => "pos"),
        // merge(b, x, x) = x
        rw!("merge-same";    "(merge ?b ?x ?x)" => "?x"),
        // merge commutativity in branches
        rw!("merge-comm";    "(merge ?b ?l ?r)" => "(merge ?b ?r ?l)"),
        // Bochvar: zero is infectious
        rw!("bochvar-l";     "(b-add zero ?x)" => "zero"),
        rw!("bochvar-r";     "(b-add ?x zero)" => "zero"),
        // Double negation of add (De Morgan)
        rw!("de-morgan-add"; "(mv-neg (mv-add ?a ?b))"
                           => "(mv-mul (mv-neg ?a) (mv-neg ?b))"),
        // meet/join constant folding
        rw!("meet-neg-l";    "(meet neg ?x)" => "neg"),
        rw!("meet-pos-l";    "(meet pos ?x)" => "?x"),
        rw!("join-pos-l";    "(join pos ?x)" => "pos"),
        rw!("join-neg-l";    "(join neg ?x)" => "?x"),
    ]
}

// ── Runner ────────────────────────────────────────────────────────────────────

/// Run equivalence saturation over a set of expressions and return the
/// constant value if the e-class resolves to one, or None if still conflicted.
pub fn stabilize(exprs: &[RecExpr<PgrsLang>]) -> Option<T> {
    let runner = Runner::default()
        .with_iter_limit(20)
        .with_node_limit(10_000);

    let mut runner = runner;
    for expr in exprs {
        runner = runner.with_expr(expr);
    }

    let runner = runner.run(&pgrs_rules());

    // Check if any root e-class has a known constant value
    for root in &runner.roots {
        if let Some(constant) = runner.egraph[*root].data.constant {
            return Some(constant);
        }
    }
    None
}

// ── EClassSummary ─────────────────────────────────────────────────────────────

/// Observable summary of an e-class. Only bumps `version` when the summary
/// actually changes — internal e-class growth that doesn't affect the
/// canonical form, cost, type signature, or boundary shape is invisible to
/// downstream deps. This prevents e-graph equivalence creep from causing
/// unnecessary fan-out recompute.
#[derive(Clone, Debug, PartialEq)]
pub struct EClassSummary {
    /// The canonical node (cheapest representative) in this e-class.
    pub canonical: egg::Id,
    /// Extraction cost of the canonical representative.
    pub cost: f64,
    /// Type signature of the e-class (coarse structural tag).
    pub type_sig: String,
    /// Hash of the canonical form's shape — stable under internal rewrites
    /// that don't change the representative.
    pub boundary_shape: u64,
    /// Version counter: increments only when canonical/cost/type_sig/shape changes.
    pub version: u64,
}

impl EClassSummary {
    /// Create a new summary. Version starts at 0.
    pub fn new(canonical: egg::Id, cost: f64, type_sig: impl Into<String>, boundary_shape: u64) -> Self {
        EClassSummary { canonical, cost, type_sig: type_sig.into(), boundary_shape, version: 0 }
    }

    /// Update the summary if any observable field changed.
    /// Returns true if the version was bumped.
    pub fn update(
        &mut self,
        canonical: egg::Id,
        cost: f64,
        type_sig: &str,
        boundary_shape: u64,
    ) -> bool {
        let changed = self.canonical != canonical
            || (self.cost - cost).abs() > f64::EPSILON
            || self.type_sig != type_sig
            || self.boundary_shape != boundary_shape;
        if changed {
            self.canonical      = canonical;
            self.cost           = cost;
            self.type_sig       = type_sig.to_string();
            self.boundary_shape = boundary_shape;
            self.version       += 1;
        }
        changed
    }
}

/// Compute a coarse shape hash for a `RecExpr` — stable under internal
/// rewrites that preserve the canonical form's structure.
pub fn shape_hash(expr: &RecExpr<PgrsLang>) -> u64 {
    use std::hash::{Hash, Hasher};
    use rustc_hash::FxHasher;
    let mut h = FxHasher::default();
    // Hash the string representation — fast and sufficient for a shape tag
    expr.to_string().hash(&mut h);
    h.finish()
}

/// Build a literal expression for a ternary value.
pub fn lit(t: T) -> RecExpr<PgrsLang> {
    let s = match t {
        T::Neg  => "neg",
        T::Zero => "zero",
        T::Pos  => "pos",
    };
    s.parse().unwrap()
}

// ── Port compliance ───────────────────────────────────────────────────────────

/// Three-way classification of a stabilization Zero outcome.
///
/// Replaces the old scalar cost extraction with a port-law acceptance test.
/// The question is not "what is the cheapest expression?" but
/// "does the rewritten section satisfy port laws on its boundary?"
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StabZeroKind {
    /// Internal structural ambiguity — e-graph should keep rewriting.
    Structural,
    /// Incompatible or underdetermined boundary values — emit `TypedZero`.
    Boundary,
    /// Lazy dep not yet evaluated — demand upstream before retrying.
    Informational,
}

/// Outcome of a port compliance check on a rewritten section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortCompliance {
    /// Section satisfies port laws on all boundary edges — accept.
    Pos,
    /// Section definitively violates port laws — reject.
    Neg,
    /// Port laws not yet satisfiable; see `StabZeroKind` for next action.
    Zero(StabZeroKind),
}

/// Compressed port history for a single boundary interface.
///
/// Tracks oscillation (rapid Pos↔Neg alternation) using arrival-time counters.
/// `alternating_count` increments on a direction reversal; resets on stability.
#[derive(Clone, Debug, Default)]
pub struct PortHistory {
    pub last_state:          Option<T>,
    pub last_transition:     Option<(T, T)>,
    pub alternating_count:   u8,
    /// Arrival counter value at the last observed state change.
    pub last_change_arrival: u64,
}

impl PortHistory {
    /// Update history given the new port state and current arrival counter.
    /// Returns whether the state changed.
    pub fn update(&mut self, new_state: T, arrival: u64) -> bool {
        let changed = self.last_state != Some(new_state);
        if changed {
            if let (Some(last), Some((_, prev_to))) = (self.last_state, self.last_transition) {
                // Detect direction reversal: Pos→Neg followed by Neg→Pos (or vice versa)
                if last != prev_to {
                    self.alternating_count = self.alternating_count.saturating_add(1);
                } else {
                    self.alternating_count = 0;
                }
            }
            self.last_transition = self.last_state.map(|old| (old, new_state));
            self.last_state = Some(new_state);
            self.last_change_arrival = arrival;
        }
        changed
    }

    /// True if this port appears to be oscillating (rapid direction reversals).
    pub fn is_oscillating(&self) -> bool { self.alternating_count >= 3 }
}

// ── Section region ────────────────────────────────────────────────────────────

/// A pinned boundary node — an external dep whose value is treated as a
/// symbolic constant inside the section. Never rewritten across.
#[derive(Clone, Debug)]
pub struct BoundaryPin {
    pub uid:     Uid,
    pub version: u64,
    pub value:   T,
}

impl BoundaryPin {
    /// Stable symbol name for this boundary pin (UID + version = unique constant).
    pub fn symbol_name(&self) -> String {
        format!("bnd_{}_{}", self.uid, self.version)
    }
}

/// The region submitted to structural e-graph stabilization.
///
/// - `members`: nodes inside the section U — get full structural expressions
/// - `boundary`: external deps — pinned as symbolic constants; never rewritten
///
/// Construction: seed from Zero nodes, expand inward along dep edges until
/// hitting a stable boundary (non-Zero value, partition boundary, or budget cap).
#[derive(Clone, Debug, Default)]
pub struct SectionRegion {
    pub members:  HashSet<Uid>,
    pub boundary: HashMap<Uid, BoundaryPin>,
}

/// Build a `SectionRegion` by BFS expansion from seed nodes.
///
/// Seeds: nodes that are currently Zero or feed a Stabilizing node.
/// Expansion stops at:
/// - stable (non-Zero) nodes → added to boundary
/// - nodes already in members
/// - nodes beyond `max_members` budget
///
/// Any dep of a member that is not itself a member becomes a boundary pin.
pub fn build_section_region(
    seeds: &[Uid],
    graph: &Graph,
    deps: &DepRegistry,
    max_members: usize,
) -> SectionRegion {
    let mut members: HashSet<Uid> = HashSet::new();
    let mut boundary: HashMap<Uid, BoundaryPin> = HashMap::new();
    let mut queue: VecDeque<(Uid, bool)> = seeds.iter().map(|&id| (id, true)).collect();

    while let Some((nid, is_seed)) = queue.pop_front() {
        if members.contains(&nid) || boundary.contains_key(&nid) { continue; }

        let Some(node) = graph.node(nid) else { continue };

        // Non-seed stable nodes become boundary pins
        if !is_seed && node.value != T::Zero {
            boundary.insert(nid, BoundaryPin { uid: nid, version: node.version, value: node.value });
            continue;
        }

        if members.len() >= max_members { break; }
        members.insert(nid);

        // Expand into upstream deps — sort for deterministic BFS order.
        // im::HashSet uses a per-instance random seed so deps_of iteration
        // order differs between Engine instances; sort before enqueuing so
        // the member set is identical when max_members truncation hits.
        let mut dep_ids: Vec<Uid> = deps.deps_of(nid).collect();
        dep_ids.sort_unstable();
        for dep_id in dep_ids {
            if !members.contains(&dep_id) && !boundary.contains_key(&dep_id) {
                queue.push_back((dep_id, false));
            }
        }
    }

    // Any dep of a member not yet classified → boundary pin
    for &mid in &members {
        let mut dep_ids: Vec<Uid> = deps.deps_of(mid).collect();
        dep_ids.sort_unstable();
        for dep_id in dep_ids {
            if !members.contains(&dep_id) && !boundary.contains_key(&dep_id) {
                if let Some(node) = graph.node(dep_id) {
                    boundary.insert(dep_id, BoundaryPin {
                        uid: dep_id, version: node.version, value: node.value,
                    });
                }
            }
        }
    }

    SectionRegion { members, boundary }
}

// ── Expression builder ────────────────────────────────────────────────────────

/// Add a ternary literal to an e-graph; return its e-class Id.
fn add_lit(t: T, egraph: &mut EGraph<PgrsLang, TernaryData>) -> Id {
    match t {
        T::Neg  => egraph.add(PgrsLang::LitNeg),
        T::Zero => egraph.add(PgrsLang::LitZero),
        T::Pos  => egraph.add(PgrsLang::LitPos),
    }
}

/// Map a `ComputeRule` + dep e-class Ids into a `PgrsLang` e-graph node.
///
/// If a rule requires more deps than are available (e.g. `MvAdd` with only one
/// dep wired up), missing slots are filled with `LitNeg` (the pending/absent
/// sentinel). This avoids panics when the e-graph is seeded with a partially-
/// connected node during fuzz / proptest runs.
fn rule_to_node(
    rule: &ComputeRule,
    dep_exprs: &[Id],
    egraph: &mut EGraph<PgrsLang, TernaryData>,
) -> Id {
    use ComputeRule::*;

    // Return dep_exprs[i] or a fresh LitNeg if the index is out of range.
    let dep = |i: usize, eg: &mut EGraph<PgrsLang, TernaryData>| -> Id {
        dep_exprs.get(i).copied().unwrap_or_else(|| eg.add(PgrsLang::LitNeg))
    };

    match rule {
        Identity     => dep(0, egraph),
        MvNeg        => { let d0 = dep(0, egraph); egraph.add(PgrsLang::MvNeg([d0])) }
        MvAdd        => { let d0 = dep(0, egraph); let d1 = dep(1, egraph); egraph.add(PgrsLang::MvAdd([d0, d1])) }
        MvMul        => { let d0 = dep(0, egraph); let d1 = dep(1, egraph); egraph.add(PgrsLang::MvMul([d0, d1])) }
        MvSub        => { let d0 = dep(0, egraph); let d1 = dep(1, egraph); egraph.add(PgrsLang::MvSub([d0, d1])) }
        Merge        => { let d0 = dep(0, egraph); let d1 = dep(1, egraph); let d2 = dep(2, egraph); egraph.add(PgrsLang::Merge([d0, d1, d2])) }
        MeetAll      => dep_exprs.iter().copied()
                            .reduce(|a, b| egraph.add(PgrsLang::Meet([a, b])))
                            .unwrap_or_else(|| egraph.add(PgrsLang::LitNeg)),
        JoinAny      => dep_exprs.iter().copied()
                            .reduce(|a, b| egraph.add(PgrsLang::Join([a, b])))
                            .unwrap_or_else(|| egraph.add(PgrsLang::LitNeg)),
        BochvarFold  => dep_exprs.iter().copied()
                            .reduce(|a, b| egraph.add(PgrsLang::BAdd([a, b])))
                            .unwrap_or_else(|| egraph.add(PgrsLang::LitNeg)),
        PowerProduct => { let d0 = dep(0, egraph); let d1 = dep(1, egraph); egraph.add(PgrsLang::MvMul([d0, d1])) }
    }
}

/// Recursively build the structural expression for `node_id` in the e-graph.
///
/// - Region members: recurse into their ComputeRule + deps (DAG-shared via memo).
/// - Boundary pins: add as a named Symbol constant — never rewritten across.
/// - Cycles: insert a fresh μ-symbol to break the cycle; the e-class representative
///   is consistent within a single stabilization run.
fn build_node_expr(
    id: Uid,
    region: &SectionRegion,
    graph: &Graph,
    deps: &DepRegistry,
    egraph: &mut EGraph<PgrsLang, TernaryData>,
    memo: &mut HashMap<Uid, Id>,
    on_stack: &mut HashSet<Uid>,
) -> Id {
    // Memoization: DAG sharing — same subterm not rebuilt twice
    if let Some(&cached) = memo.get(&id) { return cached; }

    // Cycle detection: insert a μ-symbol as a fresh e-class representative
    if on_stack.contains(&id) {
        let sym = Symbol::new(format!("mu_{}", id));
        let cycle_id = egraph.add(PgrsLang::Symbol(sym));
        memo.insert(id, cycle_id);
        return cycle_id;
    }

    // Boundary node: symbolic constant tagged with uid+version
    if let Some(pin) = region.boundary.get(&id) {
        let sym = Symbol::new(pin.symbol_name());
        let pin_id = egraph.add(PgrsLang::Symbol(sym));
        memo.insert(id, pin_id);
        return pin_id;
    }

    let Some(node) = graph.node(id) else {
        let fallback = egraph.add(PgrsLang::LitNeg);
        memo.insert(id, fallback);
        return fallback;
    };

    on_stack.insert(id);

    let expr_id = match &node.kind {
        NodeKind::Input => {
            // Input inside the region: treat current value as literal
            // (boundary inputs are handled above via the BoundaryPin path)
            add_lit(node.value, egraph)
        }
        NodeKind::Computed(rule) => {
            let dep_ids = deps.ordered_deps_of(id).to_vec();
            let dep_exprs: Vec<Id> = dep_ids.iter().map(|&dep_id| {
                if region.members.contains(&dep_id) {
                    build_node_expr(dep_id, region, graph, deps, egraph, memo, on_stack)
                } else {
                    // External dep: boundary pin or literal
                    if let Some(pin) = region.boundary.get(&dep_id) {
                        let sym = Symbol::new(pin.symbol_name());
                        egraph.add(PgrsLang::Symbol(sym))
                    } else if let Some(dep_node) = graph.node(dep_id) {
                        add_lit(dep_node.value, egraph)
                    } else {
                        egraph.add(PgrsLang::LitNeg)
                    }
                }
            }).collect();
            rule_to_node(rule, &dep_exprs, egraph)
        }
    };

    on_stack.remove(&id);
    memo.insert(id, expr_id);
    expr_id
}

/// Build e-graph expressions for all members of a `SectionRegion`.
///
/// Returns the e-graph and a map from member `Uid` to their root e-class `Id`.
pub fn build_region_exprs(
    region: &SectionRegion,
    graph: &Graph,
    deps: &DepRegistry,
) -> (EGraph<PgrsLang, TernaryData>, HashMap<Uid, Id>) {
    let mut egraph: EGraph<PgrsLang, TernaryData> = EGraph::default();
    let mut memo:     HashMap<Uid, Id> = HashMap::new();
    let mut on_stack: HashSet<Uid>     = HashSet::new();

    // Sort members for deterministic expression-building order.
    let mut sorted_members: Vec<Uid> = region.members.iter().copied().collect();
    sorted_members.sort_unstable();
    for &nid in &sorted_members {
        build_node_expr(nid, region, graph, deps, &mut egraph, &mut memo, &mut on_stack);
    }

    (egraph, memo)
}

// ── Structural stabilization ──────────────────────────────────────────────────

/// Check port compliance for a rewritten section.
///
/// Acceptance condition: all member roots resolved to the same non-Zero constant.
/// If any root is Zero or unresolved, classify the obstruction type:
/// - Boundary pin with Zero → `Boundary` (emit TypedZero)
/// - Port history oscillating → `Boundary` (instability)
/// - Otherwise → `Structural` (keep rewriting)
///
/// `Informational` is returned by the caller when it detects that a dep of the
/// region is in `ExecMode::Lazy` and has not yet been evaluated.
pub fn check_port_compliance(
    region: &SectionRegion,
    runner_egraph: &EGraph<PgrsLang, TernaryData>,
    roots: &[Id],
    histories: &HashMap<Uid, PortHistory>,
) -> PortCompliance {
    // Try to find a single agreed constant across all roots
    let mut agreed: Option<T> = None;
    let mut all_resolved = true;

    for &root in roots {
        match runner_egraph[root].data.constant {
            Some(T::Zero) | None => { all_resolved = false; }
            Some(v) => {
                match agreed {
                    None => agreed = Some(v),
                    Some(existing) if existing == v => {}
                    Some(_) => {
                        // Two roots disagree — structural conflict
                        return PortCompliance::Zero(StabZeroKind::Structural);
                    }
                }
            }
        }
    }

    if all_resolved && agreed.is_some() {
        return match agreed.unwrap() {
            T::Pos => PortCompliance::Pos,
            T::Neg => PortCompliance::Neg,
            T::Zero => PortCompliance::Zero(StabZeroKind::Structural),
        };
    }

    // Check boundary pins for Zero (boundary-induced metastability)
    if region.boundary.values().any(|pin| pin.value == T::Zero) {
        return PortCompliance::Zero(StabZeroKind::Boundary);
    }

    // Check port histories for oscillation (also boundary-level instability)
    if histories.values().any(|h| h.is_oscillating()) {
        return PortCompliance::Zero(StabZeroKind::Boundary);
    }

    // Default: internal structural ambiguity — keep rewriting
    PortCompliance::Zero(StabZeroKind::Structural)
}

/// Structural stabilization: run equivalence saturation over the expressions
/// in a `SectionRegion` and evaluate port compliance.
///
/// Returns:
/// - `Some(T)` + `PortCompliance::Pos/Neg`: all roots resolved, section accepted
/// - `None`   + `PortCompliance::Zero(k)`: section still conflicted; `k` says what to do
pub fn stabilize_section(
    region: &SectionRegion,
    graph: &Graph,
    deps: &DepRegistry,
    histories: &HashMap<Uid, PortHistory>,
) -> (Option<T>, PortCompliance) {
    if region.members.is_empty() {
        return (None, PortCompliance::Zero(StabZeroKind::Structural));
    }

    let (init_egraph, node_roots) = build_region_exprs(region, graph, deps);

    // Seed the runner with all member expressions
    let mut runner = Runner::<PgrsLang, TernaryData, ()>::default()
        .with_iter_limit(20)
        .with_node_limit(10_000);

    // Collect root e-class ids in sorted member order for determinism.
    // std::HashSet iterates in random seed order — sort to ensure consistent
    // runner.roots ordering across Engine instances.
    let mut sorted_members: Vec<Uid> = region.members.iter().copied().collect();
    sorted_members.sort_unstable();
    let member_roots: Vec<Id> = sorted_members.iter()
        .filter_map(|id| node_roots.get(id).copied())
        .collect();

    // Seed runner from the initial e-graph's expressions.
    // Use Extractor rather than id_to_expr — the latter requires explanations
    // enabled on the e-graph and will panic without them. Since init_egraph
    // has no rules applied yet (all e-classes are singletons), Extractor with
    // AstSize is equivalent and avoids the explanations requirement.
    let extractor = Extractor::new(&init_egraph, AstSize);
    for &root in &member_roots {
        let (_, expr) = extractor.find_best(root);
        runner = runner.with_expr(&expr);
    }

    let runner = runner.run(&pgrs_rules());

    let compliance = check_port_compliance(
        region, &runner.egraph, &runner.roots, histories,
    );

    match compliance {
        PortCompliance::Pos | PortCompliance::Neg => {
            // Extract the agreed constant
            let resolved = runner.roots.iter().find_map(|&root| {
                runner.egraph[root].data.constant
            });
            (resolved, compliance)
        }
        other => (None, other),
    }
}

/// Convenience: stabilize a bare list of node UIDs (backwards-compatible path).
/// Seeds the region from the provided nodes, uses empty port histories.
pub fn stabilize_nodes(
    node_ids: &[Uid],
    graph: &Graph,
    deps: &DepRegistry,
) -> Option<T> {
    let region = build_section_region(node_ids, graph, deps, 64);
    let histories = HashMap::new();
    let (val, compliance) = stabilize_section(&region, graph, deps, &histories);
    match compliance {
        PortCompliance::Pos | PortCompliance::Neg => val,
        _ => None,
    }
}
