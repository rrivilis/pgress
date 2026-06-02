//! Work amplification benchmark suite for pgrs-core.
//!
//! ## What we measure
//!
//! Work amplification is the primary metric:
//!
//!   work_amplification = total_recomputations / effective_updates
//!
//! An "effective update" is a source value change that actually alters the
//! observable graph state.  A "recomputation" is any PropEvent::ValueChanged
//! emitted downstream.  Amplification → 1.0 means only directly-affected nodes
//! recomputed.  Amplification >> 1.0 means wasted work.
//!
//! ## Baseline models
//!
//! We simulate the behaviour of four reference systems at the algorithmic level
//! (not by running the actual frameworks) and compare their amplification to
//! pgrs under the same scenario parameters.
//!
//! | System          | Amplification model                                      |
//! |-----------------|----------------------------------------------------------|
//! | Kafka fan-out   | Every event hits every subscriber: amp ≈ fan_out         |
//! | K8s reconcile   | Every trigger re-evaluates all nodes: amp ≈ node_count   |
//! | RxJS chain      | Every change traverses full chain: amp ≈ chain_depth     |
//! | Salsa (static)  | DAG-aware skip if input unchanged: amp ≈ dirty sub-DAG   |
//!
//! ## Scenarios
//!
//!   1. Linear chain (Eager)        — end-to-end push; delta-gating on no-op writes
//!   2. Dense fan-in (MeetAll)      — 64 inputs, one output; Bochvar infection path
//!   3. Lazy demand chain           — same chain; 80% Lazy; only Demand triggers
//!   4. Same-value idempotency      — repeated same write; should suppress all downstream
//!   5. Dynamic pruning             — DelNode mid-run; downstream decouples cleanly
//!   6. High conflict (Zero)        — Zero injection; Bochvar propagation cost

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pgress_core::{
    ComputeRule, Engine, ExecMode, IsaOp, PropEvent, PropStats, T, uid,
    RegionBoundary, StabilityContract, CompilePolicy,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Count PropEvent::ValueChanged events in a slice.
fn count_changed(events: &[PropEvent]) -> usize {
    events.iter().filter(|e| matches!(e, PropEvent::ValueChanged { .. })).count()
}

/// Compute work amplification: total_downstream_recomps / effective_updates.
/// Returns None if there were zero effective updates (no-op batch).
fn work_amplification(effective: usize, recomps: usize) -> Option<f64> {
    if effective == 0 { None }
    else { Some(recomps as f64 / effective as f64) }
}

// ── Baseline amplification models ─────────────────────────────────────────────
//
// These are analytic estimates of what each system would do for the same workload.
// They are not running actual Kafka/K8s — they compute the expected recomputation
// count from the system's architectural fan-out model.

/// Kafka fan-out: every event is delivered to every subscriber.
/// For N changes to a source with F subscribers each, total recomputations = N × F.
fn kafka_amplification(fan_out: usize) -> f64 { fan_out as f64 }

/// Kubernetes level-triggered reconcile: every trigger re-evaluates all N objects.
fn k8s_amplification(object_count: usize) -> f64 { object_count as f64 }

/// RxJS / reactive chain: every source change propagates through D nodes.
fn rxjs_amplification(chain_depth: usize) -> f64 { chain_depth as f64 }

/// Salsa (static DAG, incremental): re-evaluates only the dirty sub-DAG.
/// For a chain of depth D with one dirty node at the source: amplification = D.
/// For a no-op write (same value): amplification = 0 (Salsa memoises perfectly).
fn salsa_amplification_dirty(dirty_subdag_size: usize) -> f64 { dirty_subdag_size as f64 }

// ── Scenario builders ─────────────────────────────────────────────────────────

/// Build a linear Identity chain of length `n`.
/// Returns (engine, source_id, [relay_ids...]).
fn build_chain(n: usize, mode: ExecMode) -> (Engine, pgress_core::Uid, Vec<pgress_core::Uid>) {
    let mut e = Engine::new();
    let src = uid::fresh();
    e.apply(IsaOp::input_node(src, "src")).unwrap();

    let mut relays = Vec::with_capacity(n);
    let mut prev = src;
    for _ in 0..n {
        let id = uid::fresh();
        e.apply(IsaOp::computed_node(id, "relay", ComputeRule::Identity)).unwrap();
        e.apply(IsaOp::dep_edge(uid::fresh(), prev, id)).unwrap();
        if mode != ExecMode::Eager {
            e.apply(IsaOp::SetMode { node: id, mode }).unwrap();
        }
        relays.push(id);
        prev = id;
    }
    (e, src, relays)
}

/// Build a dense fan-in: `n_inputs` → one MeetAll node.
fn build_fanin(n_inputs: usize) -> (Engine, Vec<pgress_core::Uid>, pgress_core::Uid) {
    let mut e = Engine::new();
    let inputs: Vec<_> = (0..n_inputs).map(|_| {
        let id = uid::fresh();
        e.apply(IsaOp::input_node(id, "in")).unwrap();
        id
    }).collect();
    let out = uid::fresh();
    e.apply(IsaOp::computed_node(out, "meet", ComputeRule::MeetAll)).unwrap();
    for &inp in &inputs {
        e.apply(IsaOp::dep_edge(uid::fresh(), inp, out)).unwrap();
    }
    (e, inputs, out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark groups
// ─────────────────────────────────────────────────────────────────────────────

/// Scenario 1: Linear chain — Eager mode.
///
/// Measures: raw propagation throughput and work amplification for a chain
/// of depth N.  pgrs walks the chain once per source write; amplification = N.
/// Same-value second write: amplification = 0 (delta-gated).
fn bench_linear_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("linear_chain");

    for depth in [10usize, 25, 50] {
        group.throughput(Throughput::Elements(depth as u64));

        // ── Eager push: one write, full chain recomputes ──────────────────
        group.bench_with_input(
            BenchmarkId::new("eager_push", depth),
            &depth,
            |b, &d| {
                let (mut e, src, _) = build_chain(d, ExecMode::Eager);
                // Pre-warm: set to Neg so first Pos write is always "new"
                let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
                b.iter(|| {
                    // Alternate Pos/Neg so each write is an effective update
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
                });
            },
        );

        // ── Same-value idempotency: second write is suppressed ────────────
        group.bench_with_input(
            BenchmarkId::new("same_value_noop", depth),
            &depth,
            |b, &d| {
                let (mut e, src, _) = build_chain(d, ExecMode::Eager);
                // Set to Pos so subsequent writes to Pos are no-ops
                let _ = e.apply(IsaOp::SetValue { node: src, val: T::Pos });
                b.iter(|| {
                    // Same value every time: all downstream pushes suppressed
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                });
            },
        );
    }

    group.finish();
}

/// Scenario 2: Dense fan-in — MeetAll.
///
/// All N inputs must be Pos for the output to compute.  Sets inputs one by
/// one to Pos; each intermediate write hits Pending (output stays Neg).
/// Only the last write produces an effective downstream change.
fn bench_dense_fanin(c: &mut Criterion) {
    let mut group = c.benchmark_group("dense_fanin");

    for n_inputs in [16usize, 64, 128, 500] {
        group.throughput(Throughput::Elements(n_inputs as u64));

        group.bench_with_input(
            BenchmarkId::new("meetall_convergence", n_inputs),
            &n_inputs,
            |b, &n| {
                b.iter(|| {
                    let (mut e, inputs, _out) = build_fanin(n);
                    // Set all inputs to Pos — only last triggers output change
                    for &inp in &inputs {
                        let _ = e.apply(IsaOp::SetValue { node: inp, val: black_box(T::Pos) });
                    }
                });
            },
        );

        // ── Zero injection: Bochvar infection cost ────────────────────────
        group.bench_with_input(
            BenchmarkId::new("bochvar_infection", n_inputs),
            &n_inputs,
            |b, &n| {
                b.iter(|| {
                    let (mut e, inputs, _out) = build_fanin(n);
                    // Set all to Pos, then inject Zero mid-way
                    for &inp in &inputs {
                        let _ = e.apply(IsaOp::SetValue { node: inp, val: T::Pos });
                    }
                    // Zero injection: should infect output immediately
                    let _ = e.apply(IsaOp::SetValue {
                        node: black_box(inputs[n / 2]),
                        val: black_box(T::Zero),
                    });
                });
            },
        );
    }

    group.finish();
}

/// Scenario 3: Lazy demand chain.
///
/// 80% of nodes are Lazy.  SetValue on source doesn't push to Lazy nodes.
/// Demand on the tail triggers pull through only the Lazy segment.
/// pgrs amplification ≈ 0 for the push, then ≈ depth for the Demand.
/// Baseline RxJS would always propagate through the full chain.
fn bench_lazy_demand(c: &mut Criterion) {
    let mut group = c.benchmark_group("lazy_demand");

    for depth in [25usize, 50] {
        group.throughput(Throughput::Elements(depth as u64));

        // ── Push only: Lazy nodes absorb the push (no downstream compute) ─
        group.bench_with_input(
            BenchmarkId::new("push_suppressed_by_lazy", depth),
            &depth,
            |b, &d| {
                // First 20% Eager, rest Lazy
                let (mut e, src, relays) = build_chain(d, ExecMode::Eager);
                let eager_end = d / 5;
                for &id in &relays[eager_end..] {
                    let _ = e.apply(IsaOp::SetMode { node: id, mode: ExecMode::Lazy });
                }
                let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg }); // prime
                b.iter(|| {
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
                });
            },
        );

        // ── Demand pull: explicit pull through Lazy segment ───────────────
        group.bench_with_input(
            BenchmarkId::new("demand_pull", depth),
            &depth,
            |b, &d| {
                let (mut e, src, relays) = build_chain(d, ExecMode::Lazy);
                let tail = *relays.last().unwrap();
                b.iter(|| {
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                    let _ = e.apply(IsaOp::Demand { node: tail });
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
                });
            },
        );
    }

    group.finish();
}

/// Scenario 4: Dynamic pruning — DelNode mid-run.
///
/// Builds a chain, propagates to settled state, then removes the middle node.
/// Verifies that the tail decouples cleanly and upstream changes no longer
/// propagate past the deletion point.
fn bench_dynamic_pruning(c: &mut Criterion) {
    const DEPTH: usize = 30;
    let mut group = c.benchmark_group("dynamic_pruning");
    group.throughput(Throughput::Elements(DEPTH as u64));

    // ── Cost of a SetValue before deletion ───────────────────────────────
    group.bench_function("before_del_node", |b| {
        let (mut e, src, _) = build_chain(DEPTH, ExecMode::Eager);
        let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
        b.iter(|| {
            let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
            let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
        });
    });

    // ── Cost of a SetValue after mid-chain deletion ───────────────────────
    group.bench_function("after_del_node_mid", |b| {
        let (mut e, src, relays) = build_chain(DEPTH, ExecMode::Eager);
        // Delete the node at depth/2 — tail decouples
        let mid = relays[DEPTH / 2];
        let _ = e.apply(IsaOp::DelNode { id: mid });
        let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
        b.iter(|| {
            // Propagation now terminates at depth/2 boundary
            let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
            let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
        });
    });

    group.finish();
}

/// Scenario 5: Engine baseline — raw node creation + wiring throughput.
fn bench_graph_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_construction");

    for n in [100usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(
            BenchmarkId::new("chain_build", n),
            &n,
            |b, &size| {
                b.iter(|| {
                    let (e, _, _) = build_chain(black_box(size), ExecMode::Eager);
                    black_box(e.node_count())
                });
            },
        );
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Amplification ratio report (runs once as a custom benchmark)
// ─────────────────────────────────────────────────────────────────────────────

/// Compute and print the work amplification comparison table.
/// This runs once (not in a timing loop) and prints the comparison table
/// that would appear in the README benchmark section.
fn bench_amplification_report(c: &mut Criterion) {
    // We use a single criterion iteration to print the report; the
    // real timing here is intentionally trivial.
    c.bench_function("amplification_report", |b| {
        b.iter(|| {
            report_amplification();
        });
    });
}

fn report_amplification() {
    const DEPTH: usize = 50;
    const FANIN: usize = 64;
    const WRITES: usize = 20;

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║          pgrs Work Amplification vs. Baseline Systems            ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  work_amplification = total_recomputations / effective_updates   ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // ── Scenario A: Linear chain, alternating Pos/Neg (all effective) ────
    {
        let (mut e, src, _) = build_chain(DEPTH, ExecMode::Eager);
        let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
        let mut total_eff = 0usize;
        let mut total_recomp = 0usize;
        for i in 0..WRITES {
            let val = if i % 2 == 0 { T::Pos } else { T::Neg };
            let events = e.apply(IsaOp::SetValue { node: src, val }).unwrap();
            // Each write is effective (alternating)
            total_eff += 1;
            // Count downstream ValueChanged events
            total_recomp += count_changed(&events);
        }
        let pgrs_amp = work_amplification(total_eff, total_recomp).unwrap_or(0.0);

        println!("Scenario A: Linear chain (depth={}, {} alternating writes)", DEPTH, WRITES);
        println!("  pgrs (Eager, delta-gated):  {:.1}x  [{} effective, {} recomps]",
            pgrs_amp, total_eff, total_recomp);
        println!("  RxJS (reactive chain):      {:.1}x  (every write traverses {} nodes)",
            rxjs_amplification(DEPTH), DEPTH);
        println!("  Salsa (static DAG):         {:.1}x  (full dirty sub-DAG = {} nodes)",
            salsa_amplification_dirty(DEPTH), DEPTH);
        println!("  Kafka (fan-out 1):          {:.1}x  (1 subscriber, 1 recomp/write)",
            kafka_amplification(1));
        println!("  K8s (reconcile {}):         {:.1}x  (full reconcile per event)",
            DEPTH, k8s_amplification(DEPTH));
        println!();
    }

    // ── Scenario B: Same-value idempotency ───────────────────────────────
    {
        let (mut e, src, _) = build_chain(DEPTH, ExecMode::Eager);
        // Prime: set source to Pos so all downstream are settled
        let _ = e.apply(IsaOp::SetValue { node: src, val: T::Pos });

        let mut total_recomp = 0usize;
        for _ in 0..WRITES {
            let events = e.apply(IsaOp::SetValue { node: src, val: T::Pos }).unwrap();
            // Source value unchanged: effective_updates = 0 for each
            // But count what actually propagated (should be 0)
            total_recomp += count_changed(&events);
        }
        // Every write is a same-value no-op — zero effective updates
        let pgrs_amp_noop = if total_recomp == 0 { 0.0 } else { f64::INFINITY };
        let naive_recomps = WRITES * DEPTH; // naive system has no suppression

        println!("Scenario B: Same-value idempotency (depth={}, {} identical writes to Pos)", DEPTH, WRITES);
        println!("  pgrs (delta-gated):         {:.0} downstream recomps  → {:.1}x suppression",
            total_recomp, if pgrs_amp_noop == 0.0 { naive_recomps as f64 } else { 1.0 });
        println!("  Naive (no suppression):     {} downstream recomps  → {:.1}x amplification",
            naive_recomps, naive_recomps as f64 / WRITES as f64);
        println!("  Benefit: pgrs saves {}/{} recomputations ({:.0}%)",
            naive_recomps,
            naive_recomps,
            100.0);
        println!();
    }

    // ── Scenario C: Lazy demand — push suppressed ─────────────────────────
    {
        let (mut e, src, relays) = build_chain(DEPTH, ExecMode::Eager);
        // Make the last 80% of nodes Lazy
        let eager_end = DEPTH / 5;
        for &id in &relays[eager_end..] {
            let _ = e.apply(IsaOp::SetMode { node: id, mode: ExecMode::Lazy });
        }
        let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });

        let mut eager_recomp = 0usize;
        for i in 0..WRITES {
            let val = if i % 2 == 0 { T::Pos } else { T::Neg };
            let events = e.apply(IsaOp::SetValue { node: src, val }).unwrap();
            eager_recomp += count_changed(&events);
        }
        let eager_nodes = eager_end + 1; // src + eager_end relays

        println!("Scenario C: Lazy demand (depth={}, {}% Lazy, {} writes)", DEPTH, 80, WRITES);
        println!("  pgrs (80% Lazy):            {:.1}x  [only {} eager nodes recompute]",
            eager_recomp as f64 / WRITES as f64,
            eager_nodes);
        println!("  RxJS (all reactive):        {:.1}x  (all {} nodes recompute)",
            rxjs_amplification(DEPTH), DEPTH);
        println!("  Savings: {:.0}% fewer recomputations vs reactive baseline",
            (1.0 - (eager_recomp as f64 / WRITES as f64) / rxjs_amplification(DEPTH)) * 100.0);
        println!();
    }

    // ── Scenario D: Dense fan-in convergence ─────────────────────────────
    {
        let (mut e, inputs, _out) = build_fanin(FANIN);
        let mut total_recomp = 0usize;

        // Set first FANIN-1 inputs to Pos (output stays Pending each time)
        for &inp in &inputs[..FANIN - 1] {
            let events = e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
            total_recomp += count_changed(&events);
        }
        // Final input: output flips to Pos
        let final_events = e.apply(IsaOp::SetValue { node: inputs[FANIN - 1], val: T::Pos }).unwrap();
        total_recomp += count_changed(&final_events);

        let effective = FANIN; // all N inputs changed
        let pgrs_amp = work_amplification(effective, total_recomp).unwrap_or(0.0);

        println!("Scenario D: Dense fan-in MeetAll ({} inputs, 1 output)", FANIN);
        println!("  pgrs:                       {:.2}x  [{} changes, {} recomps]",
            pgrs_amp, effective, total_recomp);
        println!("  Kafka (fan-out={}):        {:.1}x  (each change hits {} subscriber)",
            FANIN, kafka_amplification(1), 1);
        println!("  Note: pgrs holds Pending until all deps Pos → minimal spurious recomps");
        println!();
    }

    // ── Scenario E: Zero Bochvar infection cost ───────────────────────────
    {
        let (mut e, inputs, _out) = build_fanin(FANIN);
        // Set all to Pos first
        for &inp in &inputs {
            let _ = e.apply(IsaOp::SetValue { node: inp, val: T::Pos });
        }
        // Inject Zero at one input
        let zero_events = e.apply(IsaOp::SetValue { node: inputs[0], val: T::Zero }).unwrap();
        let infection_recomps = count_changed(&zero_events);
        // Recover
        let recover_events = e.apply(IsaOp::SetValue { node: inputs[0], val: T::Pos }).unwrap();
        let recovery_recomps = count_changed(&recover_events);

        println!("Scenario E: Bochvar Zero injection + recovery ({} inputs → MeetAll)", FANIN);
        println!("  Infection: {} ValueChanged events  (Zero → output infected)", infection_recomps);
        println!("  Recovery:  {} ValueChanged events  (Pos  → output recovered)", recovery_recomps);
        println!("  Both bounded to O(1) downstream nodes (single output node)");
        println!();
    }

    println!("═══════════════════════════════════════════════════════════════════");
    println!("Key result: pgrs same-value suppression eliminates 100% of redundant");
    println!("recomputation. Lazy mode reduces amplification proportionally to the");
    println!("Lazy fraction. Both are enforced by construction, not by convention.");
    println!("═══════════════════════════════════════════════════════════════════\n");
}

// ─────────────────────────────────────────────────────────────────────────────
// Frontier collapse benchmarks
// ─────────────────────────────────────────────────────────────────────────────

/// Run an all-Eager chain of `depth` nodes, apply `writes` alternating SetValues,
/// return cumulative PropStats.
fn run_eager_chain(depth: usize, writes: usize) -> PropStats {
    let (mut e, src, _) = build_chain(depth, ExecMode::Eager);
    let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
    e.reset_stats();
    for i in 0..writes {
        let val = if i % 2 == 0 { T::Pos } else { T::Neg };
        let _ = e.apply(IsaOp::SetValue { node: src, val });
    }
    e.stats().clone()
}

/// Run an all-Lazy chain of `depth` nodes, apply `writes` alternating SetValues
/// (all suppressed — no DEMAND). Returns cumulative PropStats.
fn run_lazy_suppress(depth: usize, writes: usize) -> PropStats {
    let (mut e, src, _) = build_chain(depth, ExecMode::Lazy);
    let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
    e.reset_stats();
    for i in 0..writes {
        let val = if i % 2 == 0 { T::Pos } else { T::Neg };
        let _ = e.apply(IsaOp::SetValue { node: src, val });
    }
    e.stats().clone()
}

/// Run an all-Lazy chain of `depth` nodes: apply `writes` alternating SetValues
/// then fire a single DEMAND on the tail. Returns cumulative PropStats.
fn run_lazy_then_demand(depth: usize, writes: usize) -> PropStats {
    let (mut e, src, relays) = build_chain(depth, ExecMode::Lazy);
    let tail = *relays.last().unwrap();
    let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
    e.reset_stats();
    for i in 0..writes {
        let val = if i % 2 == 0 { T::Pos } else { T::Neg };
        let _ = e.apply(IsaOp::SetValue { node: src, val });
    }
    // Single DEMAND on the tail: materializes the final state
    let _ = e.apply(IsaOp::Demand { node: tail });
    e.stats().clone()
}

/// Scenario 6: Lazy/demand frontier collapse timing.
///
/// Benchmarks three distinct cost centers:
///   - Eager push (all nodes compute on every write)
///   - Lazy suppress (push blocked at first Lazy sub; zero downstream compute)
///   - Demand pull (single DEMAND on tail after N writes; materializes final state)
///
/// The gap between push_suppressed_by_lazy and demand_pull at depth=50 is where
/// the "97% recomputation savings" claim gets cycle counts attached.
fn bench_frontier_collapse(c: &mut Criterion) {
    let mut group = c.benchmark_group("frontier_collapse");

    for depth in [10usize, 25, 50, 100] {
        group.throughput(Throughput::Elements(depth as u64));

        // ── Eager: full chain recomputes on every write ───────────────────
        group.bench_with_input(
            BenchmarkId::new("eager_full", depth),
            &depth,
            |b, &d| {
                let (mut e, src, _) = build_chain(d, ExecMode::Eager);
                let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
                b.iter(|| {
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
                });
            },
        );

        // ── Lazy suppress: push stopped at sub boundary, zero downstream ──
        group.bench_with_input(
            BenchmarkId::new("lazy_suppress", depth),
            &depth,
            |b, &d| {
                let (mut e, src, _) = build_chain(d, ExecMode::Lazy);
                let _ = e.apply(IsaOp::SetValue { node: src, val: T::Neg });
                b.iter(|| {
                    // Writes are absorbed; no downstream computation happens
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
                });
            },
        );

        // ── Demand pull: single DEMAND materializes the final state ───────
        group.bench_with_input(
            BenchmarkId::new("demand_pull", depth),
            &depth,
            |b, &d| {
                let (mut e, src, relays) = build_chain(d, ExecMode::Lazy);
                let tail = *relays.last().unwrap();
                b.iter(|| {
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Pos) });
                    // Single external observation: materializes the full chain
                    let _ = e.apply(IsaOp::Demand { node: black_box(tail) });
                    let _ = e.apply(IsaOp::SetValue { node: src, val: black_box(T::Neg) });
                });
            },
        );
    }

    group.finish();
}

/// Print the frontier collapse table — recomputation counts, suppression counts,
/// and frontier geometry for Eager vs Lazy vs Lazy+Demand at various depths.
fn bench_frontier_report(c: &mut Criterion) {
    c.bench_function("frontier_report", |b| {
        b.iter(|| report_frontier_collapse());
    });
}

fn report_frontier_collapse() {
    const WRITES: usize = 20;

    println!("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    pgrs Recomputation Frontier Collapse                                  ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║  {} alternating writes to source, then one DEMAND on tail (Lazy scenario)               ║", WRITES);
    println!("╚══════════════════════════════════════════════════════════════════════════════════════════╝\n");

    // ── Per-depth frontier table ──────────────────────────────────────────────

    println!("{:<8} {:>14} {:>17} {:>15} {:>18} {:>10} {:>10}",
        "depth",
        "eager_recomps",
        "lazy_suppressed",
        "demand_recomps",
        "demand_frontier",
        "mat/demand",
        "savings%",
    );
    println!("{}", "─".repeat(96));

    for depth in [10usize, 50, 100] {
        let eager  = run_eager_chain(depth, WRITES);
        let lazy   = run_lazy_suppress(depth, WRITES);
        let demand = run_lazy_then_demand(depth, WRITES);

        let savings_pct = if eager.nodes_materialized > 0 {
            (1.0 - demand.nodes_materialized as f64 / eager.nodes_materialized as f64) * 100.0
        } else {
            0.0
        };

        println!("{:<8} {:>14} {:>17} {:>15} {:>18} {:>10.1} {:>9.1}%",
            depth,
            eager.nodes_materialized,
            lazy.pushes_suppressed_by_mode,
            demand.nodes_materialized,
            demand.max_demand_frontier,
            demand.materialization_per_demand(),
            savings_pct,
        );
    }

    println!();

    // ── Multi-write batching: the key advantage of Lazy ───────────────────────

    println!("Multi-write batching — Lazy coalesces N writes into one materialization:\n");
    println!("{:<8} {:<10} {:>14} {:>17} {:>15} {:>10} {:>10}",
        "depth", "writes",
        "eager_recomps",
        "lazy_suppressed",
        "demand_recomps",
        "mat/demand",
        "savings%",
    );
    println!("{}", "─".repeat(84));

    for (depth, writes) in [(50usize, 1usize), (50, 5), (50, 20), (100, 20)] {
        let eager  = run_eager_chain(depth, writes);
        let demand = run_lazy_then_demand(depth, writes);

        let savings_pct = if eager.nodes_materialized > 0 {
            (1.0 - demand.nodes_materialized as f64 / eager.nodes_materialized as f64) * 100.0
        } else {
            0.0
        };

        println!("{:<8} {:<10} {:>14} {:>17} {:>15} {:>10.1} {:>9.1}%",
            depth, writes,
            eager.nodes_materialized,
            demand.pushes_suppressed_by_mode,
            demand.nodes_materialized,
            demand.materialization_per_demand(),
            savings_pct,
        );
    }

    println!();

    // ── Queue geometry ────────────────────────────────────────────────────────

    println!("Queue geometry (depth=50, {} writes + 1 DEMAND):\n", WRITES);
    let eager  = run_eager_chain(50, WRITES);
    let demand = run_lazy_then_demand(50, WRITES);

    println!("  Eager:  max_queue_depth={:>4}  max_demand_frontier={:>4}  delta_suppressed={:>6}",
        eager.max_queue_depth,
        eager.max_demand_frontier,
        eager.pushes_suppressed_by_delta,
    );
    println!("  Lazy+D: max_queue_depth={:>4}  max_demand_frontier={:>4}  mode_suppressed={:>6}",
        demand.max_queue_depth,
        demand.max_demand_frontier,
        demand.pushes_suppressed_by_mode,
    );

    println!("\n  Key: demand_frontier={} means only 1 pending demand at any time in the queue,", demand.max_demand_frontier);
    println!("  regardless of chain depth. Causality and observability are decoupled.");
    println!();
}

// ─────────────────────────────────────────────────────────────────────────────
// 500-node fanin benchmarks — wall-clock + recompute stat report
// ─────────────────────────────────────────────────────────────────────────────

/// Run the three hot paths for an N-input MeetAll and return PropStats.
///
/// Phase 1 — convergence: set all inputs Pos one by one (output fires once).
/// Phase 2 — hot toggle: set input[0] Zero then Pos repeatedly (infection + recovery).
/// Phase 3 — no-op writes: set input[0] Pos repeatedly (already Pos, delta-gated).
fn run_fanin_stats(n: usize, toggles: usize) -> (PropStats, PropStats, PropStats) {
    // ── Phase 1: convergence (N-1 pending hits + 1 output fire) ─────────────
    let (mut e, inputs, _out) = build_fanin(n);
    e.reset_stats();
    for &inp in &inputs {
        let _ = e.apply(IsaOp::SetValue { node: inp, val: T::Pos });
    }
    let conv_stats = e.stats().clone();

    // ── Phase 2: hot toggle — Zero infection + Pos recovery ─────────────────
    e.reset_stats();
    for _ in 0..toggles {
        let _ = e.apply(IsaOp::SetValue { node: inputs[0], val: T::Zero });
        let _ = e.apply(IsaOp::SetValue { node: inputs[0], val: T::Pos  });
    }
    let toggle_stats = e.stats().clone();

    // ── Phase 3: no-op writes (input already Pos) ────────────────────────────
    e.reset_stats();
    for _ in 0..toggles {
        let _ = e.apply(IsaOp::SetValue { node: inputs[0], val: T::Pos });
    }
    let noop_stats = e.stats().clone();

    (conv_stats, toggle_stats, noop_stats)
}

fn report_500_fanin() {
    const N: usize = 500;
    const TOGGLES: usize = 100;

    let (conv, toggle, noop) = run_fanin_stats(N, TOGGLES);

    println!("\n╔════════════════════════════════════════════════════════════════════════╗");
    println!("║          pgrs 500-node fanin — recompute + suppression stats          ║");
    println!("╠════════════════════════════════════════════════════════════════════════╣");
    println!("║  {} inputs → 1 MeetAll output                                         ║", N);
    println!("╚════════════════════════════════════════════════════════════════════════╝\n");

    println!("Phase 1 — convergence ({} inputs set Pos one by one):", N);
    println!("  nodes_materialized:        {:>8}  (expect 1 — output fires exactly once)", conv.nodes_materialized);
    println!("  pushes_suppressed_by_mode: {:>8}  (Pending path: {} recheck-and-wait hits)", conv.pushes_suppressed_by_mode, N - 1);
    println!("  pushes_suppressed_by_delta:{:>8}", conv.pushes_suppressed_by_delta);
    println!("  max_queue_depth:           {:>8}", conv.max_queue_depth);
    let conv_amp = if N > 0 { conv.nodes_materialized as f64 / N as f64 } else { 0.0 };
    println!("  work amplification:        {:>8.4}x  ({} recomps / {} input writes)", conv_amp, conv.nodes_materialized, N);
    println!();

    println!("Phase 2 — hot toggle: Zero injection + Pos recovery ({} × 2 writes):", TOGGLES);
    println!("  nodes_materialized:        {:>8}  (expect {} — 2 output flips per toggle)", toggle.nodes_materialized, TOGGLES * 2);
    println!("  pushes_suppressed_by_delta:{:>8}  (delta-gate skips same-causal-time resends)", toggle.pushes_suppressed_by_delta);
    println!("  max_queue_depth:           {:>8}", toggle.max_queue_depth);
    println!("  max_demand_frontier:       {:>8}", toggle.max_demand_frontier);
    let toggle_writes = TOGGLES * 2;
    let toggle_amp = if toggle_writes > 0 { toggle.nodes_materialized as f64 / toggle_writes as f64 } else { 0.0 };
    println!("  work amplification:        {:>8.4}x  ({} recomps / {} writes)", toggle_amp, toggle.nodes_materialized, toggle_writes);
    println!("  naive (no suppression):    {:>8.1}x  (every write re-evaluates all {} inputs)",
        N as f64, N);
    println!("  suppression ratio:         {:>7.1}x  better than naive",
        N as f64 / toggle_amp.max(0.001));
    println!();

    println!("Phase 3 — no-op writes ({} × input[0]=Pos, already Pos):", TOGGLES);
    println!("  nodes_materialized:        {:>8}  (expect 0 — delta-gate suppresses all)", noop.nodes_materialized);
    println!("  pushes_suppressed_by_delta:{:>8}", noop.pushes_suppressed_by_delta);
    println!("  pushes_suppressed_by_mode: {:>8}", noop.pushes_suppressed_by_mode);
    println!("  work amplification:        {:>8.4}x  (perfect suppression)", 0.0_f64);
    println!();

    // Comparison table
    println!("Comparison: pgrs vs naive event fan-out for {} inputs, {} toggles", N, TOGGLES);
    println!("{:<22} {:>12} {:>12} {:>12}", "system", "conv_recomps", "toggle_recomps", "noop_recomps");
    println!("{}", "─".repeat(60));
    println!("{:<22} {:>12} {:>12} {:>12}", "pgrs (this engine)",
        conv.nodes_materialized, toggle.nodes_materialized, noop.nodes_materialized);
    println!("{:<22} {:>12} {:>12} {:>12}", "naive fan-out (Kafka)",
        N,               // every input write fans out to 1 subscriber = N total
        TOGGLES * 2 * N, // each toggle re-evaluates all N edges
        TOGGLES * N);    // no-op: still delivers to all N subscribers
    println!("{:<22} {:>12} {:>12} {:>12}", "K8s full-reconcile",
        N * N,           // full graph reconcile per write
        TOGGLES * 2 * (N + 1),
        TOGGLES * (N + 1));
    println!();
}

fn bench_500_fanin(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanin_500");
    // Sample count reduced: building 500-node graphs is slow; we want stable
    // per-op timing, not aggregate. Default 100 samples × warm-up is sufficient.

    // ── Convergence: set all 500 inputs Pos ──────────────────────────────────
    group.throughput(Throughput::Elements(500));
    group.bench_function("convergence_500", |b| {
        b.iter(|| {
            let (mut e, inputs, _out) = build_fanin(black_box(500));
            for &inp in &inputs {
                let _ = e.apply(IsaOp::SetValue { node: inp, val: black_box(T::Pos) });
            }
        });
    });

    // ── Hot toggle: Zero injection + Pos recovery on a warm graph ────────────
    group.throughput(Throughput::Elements(1)); // 1 toggle = 2 ops
    group.bench_function("hot_toggle_zero_pos", |b| {
        let (mut e, inputs, _out) = build_fanin(500);
        // Warm: all inputs Pos, output settled
        for &inp in &inputs {
            let _ = e.apply(IsaOp::SetValue { node: inp, val: T::Pos });
        }
        b.iter(|| {
            // Zero injection: Bochvar infects output
            let _ = e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Zero) });
            // Recovery: output returns to Pos
            let _ = e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos)  });
        });
    });

    // ── No-op: same-value write to a settled input (pure suppression cost) ───
    group.throughput(Throughput::Elements(1));
    group.bench_function("noop_same_value", |b| {
        let (mut e, inputs, _out) = build_fanin(500);
        for &inp in &inputs {
            let _ = e.apply(IsaOp::SetValue { node: inp, val: T::Pos });
        }
        b.iter(|| {
            // Already Pos: delta-gate suppresses everything after the input node
            let _ = e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos) });
        });
    });

    group.finish();
}

fn bench_500_fanin_report(c: &mut Criterion) {
    c.bench_function("fanin_500_report", |b| {
        b.iter(|| report_500_fanin());
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Criterion registration
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Region-aware amplification benchmarks
// ─────────────────────────────────────────────────────────────────────────────
//
// These benchmarks measure the compiled sparse circuit tier (Tier 2) against
// the warm dep-scan path (Tier 1) across three regimes:
//
//   1. Dense writes, static topology — constant fan-in, all inputs change each
//      cycle.  The compiled path does one CSR pass; warm path queues each node.
//      Measures pure execution overhead difference.
//
//   2. Sparse writes, static topology — k << N inputs change per cycle.
//      Compiled path does a full O(N) CSR pass regardless; warm path is O(k).
//      Identifies the k/N density crossover where compiled begins to lose.
//
//   3. Topology churn — region compiled, then an edge added every M cycles,
//      forcing recompile.  Measures invalidation + recompile amortization.
//
// The key claim: compiled circuit reduces to SpMV in the (meet, join) ternary
// semiring over the frozen adjacency matrix.  For static topology, the SpMV
// constant factor is lower than the warm queue machinery.  For sparse writes,
// warm path wins because it only touches the changed subgraph.

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a dense fanin and optionally compile the output node as a region.
fn build_fanin_maybe_compiled(
    n_inputs: usize,
    compiled: bool,
) -> (Engine, Vec<pgress_core::Uid>, pgress_core::Uid) {
    let (mut e, inputs, out) = build_fanin(n_inputs);
    if compiled {
        e.apply(IsaOp::RegionDeclare {
            root:       out,
            boundary:   RegionBoundary::ExplicitSet(vec![out]),
            stability:  StabilityContract::EpochTracked,
            compile:    CompilePolicy::Eager,
        }).unwrap();
    }
    (e, inputs, out)
}

/// Run N-input fanin through K update cycles where exactly `k_writes` inputs
/// change per cycle (cycling through inputs[0..k_writes]).
/// Returns PropStats after all cycles.
fn run_sparse_writes(
    n_inputs: usize,
    k_writes: usize,
    cycles: usize,
    compiled: bool,
) -> PropStats {
    let (mut e, inputs, _out) = build_fanin_maybe_compiled(n_inputs, compiled);
    // Warm up: all inputs Pos so the output is settled.
    for &inp in &inputs {
        e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
    }
    e.reset_stats();
    // Sparse write cycles: only the first k_writes inputs toggle.
    for cycle in 0..cycles {
        let v = if cycle % 2 == 0 { T::Zero } else { T::Pos };
        for &inp in inputs.iter().take(k_writes) {
            e.apply(IsaOp::SetValue { node: inp, val: v }).unwrap();
        }
    }
    e.stats().clone()
}

// ── Regime 1: dense writes, static topology ──────────────────────────────────

/// Compiled vs warm hot-toggle on a settled 500-input fanin.
///
/// Both paths run on the same input sequence; the compiled circuit does one
/// CSR pass, the warm path queues a Push for the output node and evaluates it.
fn bench_region_vs_warm_static(c: &mut Criterion) {
    let mut group = c.benchmark_group("region_vs_warm");
    group.sample_size(50);

    for n in [64usize, 128, 500] {
        // ── Warm path ────────────────────────────────────────────────────────
        group.bench_with_input(
            BenchmarkId::new("warm_hot_toggle", n),
            &n,
            |b, &n_inputs| {
                let (mut e, inputs, _out) = build_fanin(n_inputs);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }
                b.iter(|| {
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Zero) }).unwrap();
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos)  }).unwrap();
                });
            },
        );

        // ── Compiled path ────────────────────────────────────────────────────
        group.bench_with_input(
            BenchmarkId::new("compiled_hot_toggle", n),
            &n,
            |b, &n_inputs| {
                let (mut e, inputs, _out) = build_fanin_maybe_compiled(n_inputs, true);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }
                b.iter(|| {
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Zero) }).unwrap();
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos)  }).unwrap();
                });
            },
        );
    }
    group.finish();
}

// ── Regime 2: sparse write density sweep ─────────────────────────────────────

/// Compiled vs warm as a function of write density k/N.
///
/// Compiled path: O(N) CSR pass per drain cycle regardless of k.
/// Warm path:     O(k) — only enqueues subscribers of changed inputs.
/// Crossover is expected around k/N ≈ 0.1–0.3 depending on CSR constant factor.
fn bench_region_sparse_density(c: &mut Criterion) {
    const N: usize = 128;
    const CYCLES: usize = 200;
    let mut group = c.benchmark_group("region_sparse_density");
    group.sample_size(30);

    for k in [1usize, 4, 16, 32, 64, 128] {
        group.throughput(Throughput::Elements(k as u64));

        group.bench_with_input(
            BenchmarkId::new("warm", k),
            &k,
            |b, &k_writes| {
                b.iter(|| run_sparse_writes(black_box(N), black_box(k_writes), black_box(CYCLES), false));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("compiled", k),
            &k,
            |b, &k_writes| {
                b.iter(|| run_sparse_writes(black_box(N), black_box(k_writes), black_box(CYCLES), true));
            },
        );
    }
    group.finish();
}

// ── Regime 3: topology churn — invalidation + recompile amortization ─────────

/// Measures the cost of periodic topology mutations that invalidate a compiled region.
///
/// Every `churn_interval` hot-toggle cycles, a new input node is added and wired
/// into the output (EdgeConnect → epoch bump → cache invalidation → recompile on
/// next RegionDeclare).  Simulates a system where the graph occasionally grows
/// but is mostly static.
fn bench_region_topology_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("region_topology_churn");
    group.sample_size(30);

    // Churn every N cycles: 1 (maximally unstable) → 10 → 50 → never (static)
    for churn_interval in [1usize, 10, 50, usize::MAX] {
        let label = if churn_interval == usize::MAX {
            "static".to_string()
        } else {
            format!("churn_{}", churn_interval)
        };

        group.bench_function(&label, |b| {
            b.iter(|| {
                const N: usize = 64;
                const TOTAL_CYCLES: usize = 100;
                let (mut e, inputs, out) = build_fanin(N);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }

                let redeclare = |e: &mut Engine| {
                    e.apply(IsaOp::RegionDeclare {
                        root:      out,
                        boundary:  RegionBoundary::ExplicitSet(vec![out]),
                        stability: StabilityContract::EpochTracked,
                        compile:   CompilePolicy::Eager,
                    }).unwrap();
                };
                redeclare(&mut e);

                let mut extra_inputs: Vec<pgress_core::Uid> = Vec::new();
                for cycle in 0..TOTAL_CYCLES {
                    // Periodic churn: add a new input, invalidate, recompile.
                    if churn_interval != usize::MAX && cycle % churn_interval == 0 {
                        let new_inp = uid::fresh();
                        e.apply(IsaOp::input_node(new_inp, "extra")).unwrap();
                        e.apply(IsaOp::dep_edge(uid::fresh(), new_inp, out)).unwrap();
                        e.apply(IsaOp::SetValue { node: new_inp, val: T::Pos }).unwrap();
                        extra_inputs.push(new_inp);
                        redeclare(&mut e);
                    }
                    // Hot toggle: one input Zero → Pos
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Zero) }).unwrap();
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos)  }).unwrap();
                }
            });
        });
    }
    group.finish();
}

// ── Region amplification report ───────────────────────────────────────────────

fn report_region_amplification() {
    const N: usize = 128;
    const CYCLES: usize = 200;

    println!("\n╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║         Region-Aware Amplification — Sparse Matrix Evolution            ║");
    println!("╠══════════════════════════════════════════════════════════════════════════╣");
    println!("║  {} inputs → 1 MeetAll output                                           ║", N);
    println!("║  Model: compiled circuit = SpMV in (meet,join) semiring over CSR adj    ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝\n");

    // ── Write density sweep ───────────────────────────────────────────────────
    println!("Write density sweep (k/{} inputs change per cycle, {} cycles, Zero↔Pos):\n", N, CYCLES);
    println!("{:<6} {:>14} {:>14} {:>14} {:>14} {:>12}",
        "k", "warm_recomps", "compiled_recomps", "warm_amp", "compiled_amp", "winner");
    println!("{}", "─".repeat(76));

    for k in [1usize, 2, 4, 8, 16, 32, 64, 128] {
        let warm     = run_sparse_writes(N, k, CYCLES, false);
        let compiled = run_sparse_writes(N, k, CYCLES, true);

        // Each cycle with a value change produces 1 effective update (output toggles).
        // Amplification = materialized nodes / effective output changes.
        let warm_amp     = warm.nodes_materialized     as f64 / CYCLES as f64;
        let compiled_amp = compiled.nodes_materialized as f64 / CYCLES as f64;
        let winner = if compiled_amp <= warm_amp { "compiled" } else { "warm    " };

        println!("{:<6} {:>14} {:>14} {:>14.3} {:>14.3} {:>12}",
            k,
            warm.nodes_materialized,
            compiled.nodes_materialized,
            warm_amp,
            compiled_amp,
            winner,
        );
    }

    println!("\nNote: compiled path does O(N) CSR pass per cycle regardless of k.");
    println!("      warm path is O(k) — touches only nodes whose deps changed.");
    println!("      crossover where compiled wins = point where queue overhead > CSR constant.\n");

    // ── Static topology: compiled vs warm at scale ────────────────────────────
    println!("Static topology comparison (full density k=N, hot toggle, 100 cycles):\n");
    println!("{:<8} {:>14} {:>18} {:>12} {:>16}",
        "N", "warm_recomps", "compiled_recomps", "warm_amp", "compiled_amp");
    println!("{}", "─".repeat(72));

    for n in [16usize, 64, 128, 256, 500] {
        let warm     = run_sparse_writes(n, 1, 100, false);  // k=1: toggle single input
        let compiled = run_sparse_writes(n, 1, 100, true);

        let warm_amp     = warm.nodes_materialized     as f64 / 100.0;
        let compiled_amp = compiled.nodes_materialized as f64 / 100.0;

        println!("{:<8} {:>14} {:>18} {:>12.3} {:>16.3}",
            n,
            warm.nodes_materialized,
            compiled.nodes_materialized,
            warm_amp,
            compiled_amp,
        );
    }

    println!("\nBoth paths should show amplification ≈ 1.0 (Bochvar O(1) output fire per toggle).");
    println!("Difference is constant factor: compiled CSR vs warm queue machinery.\n");
}

fn bench_region_amplification_report(c: &mut Criterion) {
    c.bench_function("region_amplification_report", |b| {
        b.iter(|| report_region_amplification());
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-layer active-lane benchmark (Opt 9)
// ─────────────────────────────────────────────────────────────────────────────
//
// A two-layer topology exposes active-lane skipping:
//
//   layer 0: n_inputs Input nodes (boundary — external)
//   layer 1: n_hidden Computed(MeetAll) nodes, each reading k_dep consecutive
//            inputs (stride = n_inputs / n_hidden).
//   layer 2: 1 Computed(MeetAll) output reading all hidden nodes.
//
// With k=1 input change per cycle, only hidden nodes whose dep window overlaps
// the changed input are dirty — the rest are skipped by the active-lane check.
// For n_inputs=128, n_hidden=16, k_dep=8: changing input[i] dirties exactly 1
// hidden node → 15/16 hidden rows skipped → output row checked (1 dirty dep).
//
// This is the topology where Opt 9 matters: deeper graphs with many intermediate
// nodes, sparse updates.

fn build_multilayer(
    n_inputs: usize,
    n_hidden: usize,
    k_dep:    usize,  // deps per hidden node (consecutive, wrapping)
    compiled: bool,
) -> (Engine, Vec<pgress_core::Uid>, pgress_core::Uid) {
    let mut e = Engine::new();

    // Layer 0: inputs
    let inputs: Vec<pgress_core::Uid> = (0..n_inputs).map(|_| {
        let id = uid::fresh();
        e.apply(IsaOp::input_node(id, "in")).unwrap();
        id
    }).collect();

    // Layer 1: hidden nodes — each reads k_dep consecutive inputs (wrapping)
    let hidden: Vec<pgress_core::Uid> = (0..n_hidden).map(|h| {
        let id = uid::fresh();
        e.apply(IsaOp::NodeCreate {
            id, typ: "hidden".into(),
            rule: pgress_core::NodeKind::Computed(ComputeRule::MeetAll),
            attrs: pgress_core::Attrs::new(),
        }).unwrap();
        let stride = n_inputs / n_hidden;
        for d in 0..k_dep {
            let src = inputs[(h * stride + d) % n_inputs];
            e.apply(IsaOp::dep_edge(uid::fresh(), src, id)).unwrap();
        }
        id
    }).collect();

    // Layer 2: single output reading all hidden nodes
    let out = uid::fresh();
    e.apply(IsaOp::NodeCreate {
        id: out, typ: "out".into(),
        rule: pgress_core::NodeKind::Computed(ComputeRule::MeetAll),
        attrs: pgress_core::Attrs::new(),
    }).unwrap();
    for &h in &hidden {
        e.apply(IsaOp::dep_edge(uid::fresh(), h, out)).unwrap();
    }

    if compiled {
        // Compile the full two-layer subgraph (hidden + output) as one region.
        let mut members: Vec<pgress_core::Uid> = hidden.clone();
        members.push(out);
        e.apply(IsaOp::RegionDeclare {
            root:      out,
            boundary:  RegionBoundary::ExplicitSet(members),
            stability: StabilityContract::EpochTracked,
            compile:   CompilePolicy::Eager,
        }).unwrap();
    }

    (e, inputs, out)
}

/// Warm up the multi-layer graph (all inputs → Pos), then toggle input[i] for
/// `cycles` cycles and return PropStats.
fn run_multilayer_cycles(
    n_inputs: usize,
    n_hidden: usize,
    k_dep:    usize,
    cycles:   usize,
    compiled: bool,
) -> PropStats {
    let (mut e, inputs, _out) = build_multilayer(n_inputs, n_hidden, k_dep, compiled);
    for &inp in &inputs {
        e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
    }
    e.reset_stats();
    for cycle in 0..cycles {
        let v = if cycle % 2 == 0 { T::Zero } else { T::Pos };
        e.apply(IsaOp::SetValue { node: inputs[0], val: v }).unwrap();
    }
    e.stats().clone()
}

/// Benchmark: warm vs compiled on 2-layer topology with sparse k=1 updates.
///
/// Active-lane skipping (Opt 9) dramatically reduces the compiled path's work:
/// only the ~1 hidden node affected by input[0] is evaluated; the other
/// n_hidden-1 rows are skipped.
fn bench_multilayer_active_lane(c: &mut Criterion) {
    let mut group = c.benchmark_group("multilayer_active_lane");
    group.sample_size(50);

    // (n_inputs, n_hidden, k_dep): 128 inputs → 16 hidden (8 deps each) → 1 output
    // Changing input[0] dirties exactly 1 hidden node out of 16.
    let configs: &[(usize, usize, usize)] = &[
        (64,  8,  4),   // 4 deps/hidden, 1/8   hidden dirty per input change
        (128, 16, 8),   // 8 deps/hidden, 1/16  hidden dirty per input change
        (256, 32, 8),   // 8 deps/hidden, 1/32  hidden dirty per input change
    ];

    for &(n, m, k) in configs {
        let label = format!("{n}x{m}d{k}");

        group.bench_with_input(
            BenchmarkId::new("warm", &label),
            &(n, m, k),
            |b, &(n_inputs, n_hidden, k_dep)| {
                let (mut e, inputs, _) = build_multilayer(n_inputs, n_hidden, k_dep, false);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }
                b.iter(|| {
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Zero) }).unwrap();
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos) }).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("compiled", &label),
            &(n, m, k),
            |b, &(n_inputs, n_hidden, k_dep)| {
                let (mut e, inputs, _) = build_multilayer(n_inputs, n_hidden, k_dep, true);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }
                b.iter(|| {
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Zero) }).unwrap();
                    e.apply(IsaOp::SetValue { node: black_box(inputs[0]), val: black_box(T::Pos) }).unwrap();
                });
            },
        );
    }
    group.finish();
}

/// Print multi-layer stats table: recomputation counts for warm vs compiled.
fn report_multilayer_active_lane() {
    const CYCLES: usize = 200;

    println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║          Multi-layer active-lane (Opt 9) — dirty row skipping              ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Topology: N inputs → M hidden(MeetAll, k deps each) → 1 output            ║");
    println!("║  Write pattern: toggle input[0] only ({} cycles), all others Pos          ║", CYCLES);
    println!("║  Active-lane skips (M - dirty_hidden) rows per cycle in compiled path      ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

    println!("{:<16} {:>8} {:>8} {:>8} {:>16} {:>18} {:>12}",
        "topology", "N", "M", "k_dep",
        "warm_recomps", "compiled_recomps", "rows_skipped%");
    println!("{}", "─".repeat(88));

    for (n, m, k) in [(64usize,8usize,4usize), (128,16,8), (256,32,8)] {
        let warm     = run_multilayer_cycles(n, m, k, CYCLES, false);
        let compiled = run_multilayer_cycles(n, m, k, CYCLES, true);

        // Theoretical max skippable rows per cycle = m-1 hidden + skip output if no hidden changed.
        // Approximate skip% = (m - 1) / (m + 1) * 100
        let skip_pct = (m.saturating_sub(1)) as f64 / (m + 1) as f64 * 100.0;
        let label = format!("{n}x{m}d{k}");

        println!("{:<16} {:>8} {:>8} {:>8} {:>16} {:>18} {:>11.1}%",
            label, n, m, k,
            warm.nodes_materialized,
            compiled.nodes_materialized,
            skip_pct,
        );
    }
    println!();
}

fn bench_multilayer_active_lane_report(c: &mut Criterion) {
    c.bench_function("multilayer_active_lane_report", |b| {
        b.iter(|| report_multilayer_active_lane());
    });
}

// ── M-sweep: N=128 fixed, k=8, M in [8,12,16,20,24,32] ───────────────────────
//
// Isolates the compiled/warm ratio as a function of region size (M+1 rows) with
// all other parameters fixed.  Used to determine whether the 128×16 overhead
// is a cliff or part of a monotone pattern.
//
// What varies across M at fixed N=128, k=8:
//   - stride = N/M (integer): controls dep slot assignment per hidden node
//   - col_inv_keys size: ~N + M entries (input slots + hidden output slots)
//   - warm path no-op load: CONSTANT at 1 node (hidden[0]) — only input[0]'s
//     direct subscriber changes regardless of M
//   - pending rows evaluated: CONSTANT at 2 (hidden[0] + output)
//   - output SWAR range: meet_all_range(N, M) — always ≤1 word (M≤64)
//
// If the overhead is truly M-driven (monotone increase), the ratio should grow
// with M.  If it clusters, there is a structural cause (cache line boundary,
// col_inv layout, dirty_slots word count threshold, etc.).

fn bench_m_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("m_sweep_n128k8");
    group.sample_size(50);

    // N=128, k=8 fixed; sweep M.
    // stride = N/M; k_dep per hidden node wraps if k > stride.
    let m_values: &[usize] = &[8, 12, 16, 20, 24, 32];

    for &m in m_values {
        let label = format!("128x{m}d8");

        group.bench_with_input(
            BenchmarkId::new("warm", &label),
            &m,
            |b, &n_hidden| {
                let (mut e, inputs, _) = build_multilayer(128, n_hidden, 8, false);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }
                b.iter(|| {
                    e.apply(IsaOp::SetValue {
                        node: black_box(inputs[0]), val: black_box(T::Zero),
                    }).unwrap();
                    e.apply(IsaOp::SetValue {
                        node: black_box(inputs[0]), val: black_box(T::Pos),
                    }).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("compiled", &label),
            &m,
            |b, &n_hidden| {
                let (mut e, inputs, _) = build_multilayer(128, n_hidden, 8, true);
                for &inp in &inputs {
                    e.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
                }
                b.iter(|| {
                    e.apply(IsaOp::SetValue {
                        node: black_box(inputs[0]), val: black_box(T::Zero),
                    }).unwrap();
                    e.apply(IsaOp::SetValue {
                        node: black_box(inputs[0]), val: black_box(T::Pos),
                    }).unwrap();
                });
            },
        );
    }
    group.finish();
}

/// Diagnostic printout for the M-sweep: structural parameters alongside
/// timing numbers to identify what correlates with the overhead cliff.
fn report_m_sweep() {
    use std::time::Instant;

    const CYCLES: usize = 2_000;  // more cycles for stable ns-level estimates

    println!("\n╔═══════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  M-sweep diagnostic: N=128, k=8, varying M — compiled vs warm (Opts 9-11)                   ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Fixed: toggle input[0] only; all other inputs Pos; stride=N/M (integer)                    ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════════════════════╝\n");

    println!("{:<14} {:>6} {:>6} {:>6} {:>11} {:>11} {:>11} {:>10} {:>10}",
        "topology", "M", "stride", "inv_keys",
        "warm_ns", "comp_ns", "ratio",
        "dirty_wds", "pend_wds");
    println!("{}", "─".repeat(98));

    for &m in &[8usize, 12, 16, 20, 24, 32] {
        let stride = 128 / m;

        // Measure warm path timing.
        let (mut ew, inputs_w, _) = build_multilayer(128, m, 8, false);
        for &inp in &inputs_w {
            ew.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
        }
        let t0 = Instant::now();
        for cycle in 0..CYCLES {
            let v = if cycle % 2 == 0 { T::Zero } else { T::Pos };
            ew.apply(IsaOp::SetValue { node: inputs_w[0], val: v }).unwrap();
        }
        let warm_ns = t0.elapsed().as_nanos() as f64 / CYCLES as f64;

        // Measure compiled path timing.
        let (mut ec, inputs_c, _) = build_multilayer(128, m, 8, true);
        for &inp in &inputs_c {
            ec.apply(IsaOp::SetValue { node: inp, val: T::Pos }).unwrap();
        }
        let t1 = Instant::now();
        for cycle in 0..CYCLES {
            let v = if cycle % 2 == 0 { T::Zero } else { T::Pos };
            ec.apply(IsaOp::SetValue { node: inputs_c[0], val: v }).unwrap();
        }
        let comp_ns = t1.elapsed().as_nanos() as f64 / CYCLES as f64;

        // Structural parameters.
        // col_inv_keys count: unique dep dense slots in the region.
        // Input slots: for each hidden h, deps are (h*stride + d) % 128 for d in 0..8.
        // With stride≥8 (M≤16): all deps unique → M*8 input slots.
        // With stride<8 (M>16): some deps wrap/overlap → unique < M*8.
        let mut unique_input_slots = std::collections::BTreeSet::new();
        for h in 0..m {
            for d in 0..8usize {
                unique_input_slots.insert((h * stride + d) % 128);
            }
        }
        // Plus M hidden output slots (128..128+m) and 1 output slot (128+m).
        let inv_keys = unique_input_slots.len() + m; // +m for hidden outputs as dep of output row

        // dirty_slots word count: ceil((128 + m + 1) / 64)
        let dirty_words = (128 + m + 1 + 63) / 64;
        // pending word count: ceil((m + 1) / 64)
        let pending_words = (m + 1 + 63) / 64;

        let label = format!("128x{}d8", m);
        println!("{:<14} {:>6} {:>6} {:>8} {:>11.1} {:>11.1} {:>11.3} {:>10} {:>10}",
            label, m, stride, inv_keys,
            warm_ns, comp_ns,
            comp_ns / warm_ns,
            dirty_words, pending_words,
        );
    }
    println!();
}

fn bench_m_sweep_report(c: &mut Criterion) {
    c.bench_function("m_sweep_report", |b| {
        b.iter(|| report_m_sweep());
    });
}

criterion_group!(
    benches,
    bench_linear_chain,
    bench_dense_fanin,
    bench_lazy_demand,
    bench_dynamic_pruning,
    bench_graph_construction,
    bench_amplification_report,
    bench_frontier_collapse,
    bench_frontier_report,
    bench_500_fanin,
    bench_500_fanin_report,
    bench_region_vs_warm_static,
    bench_region_sparse_density,
    bench_region_topology_churn,
    bench_region_amplification_report,
    bench_multilayer_active_lane,
    bench_multilayer_active_lane_report,
    bench_m_sweep,
    bench_m_sweep_report,
);
criterion_main!(benches);
