# pgress

**pgress is a reactive runtime designed to actively suppress redundant computational work by tracking causal dependencies between observable state changes. Instead of recomputing everything when data changes, it tracks what actually depends on what and only propagates meaningful updates.**

- Same-value writes cost ~90 ns and trigger nothing downstream — enforced by construction, not convention.
- A 500-input convergence fires the output exactly once (1.0× amplification vs 500× for Kafka).
- Contested state is a stable first-class value, not an exception or a null.

---

## The problem

Every system that maintains derived state faces the same tension: when an upstream value changes, which downstream computations actually need to run?

```
work_amplification = total_recomputations / effective_updates
```

| System | Amplification |
|---|---|
| Kafka fan-out | ≈ F — every event hits every subscriber |
| Kubernetes reconcile | ≈ N — full reconcile on every trigger |
| RxJS / reactive chain | ≈ D — every change traverses the full chain |
| pgress | **≈ 1.0×** — output fires iff value changes; Zero/Pending suppress downstream |

The amplification gap is not just a performance issue. In any system that maintains derived state, recomputation cost determines whether the system stays tractable as it grows. Most systems pay it indirectly, reconstructing dependencies they threw away rather than tracking them up front.

Naive answers are expensive at scale for any pipeline with derived state that has to reconcile multiple sources that can legitimately disagree: policy and config systems, multi-source data validation, anywhere last-write-wins silently destroys a conflict a human needed to see. pgress approaches the problem by making the dependency graph structure the primary computational artifact: there is no separate "change detection" layer. Every recomputation is gated by a formal test of whether it would produce a new observable value.

---

## The mental model

**Ports define truth, switches transform it, graphs transport it.**

```
port ↔ switch ↔ graph ↔ switch ↔ port
```

- **Port** — a source of truth. Input nodes, external events, sensor readings. A port produces a signed value in `{−1, 0, +1}`.
- **Switch** — a computation. A node that derives its value from its dependencies via a rule (`MeetAll`, `JoinAny`, `Identity`, etc.).
- **Graph** — the transport. Typed dependency edges carry values between ports and switches, across partition boundaries if necessary.

The system stores which ports feed which switches, which switches feed which other switches, and what mode each switch runs in. It then maintains all derived values incrementally as ports change.

---

## The ternary logic model

Every value lives in `T = {Neg, Zero, Pos}`:

| Value | Python | Meaning |
|---|---|---|
| `Pos` | `True` | Present / satisfied / active |
| `Zero` | `False` | Infected / conflicted — self-negating fixed point |
| `Neg` | `None` | Absent / inhibited / pending |

`Zero` is not a null state. It is the stable evaluation of genuine structural disagreement, operationally presenting as a first-class contested system state that is its own negation (`¬Zero = Zero`). By surfacing `Zero` as a stable value, the runtime can reason about contradiction rather than collapsing it into logs, retries, or last-write-wins resolution.

---

## Execution modes

Every ISA operation is one of three primitive classes:

```
SET_VALUE(node, Pos)
     │
     ▼
Extension / Eager ─── all deps Pos → compute → push Pos downstream
     │
     └── any dep Zero →   infection → Zero propagates
                              │
               ┌──────────────┴───────────────┐
               │ Lazy (Inhibition)             │ Stabilizing (Reflection)
               │ defer until DEMAND fires      │ route region to e-graph
               └───────────────────────────────┘
```

**Extension (Eager)**: the normal productive path. When all dependencies are ready (`Pos`), compute the rule and push downstream. Delta-gated: a push is skipped if the output value is unchanged, or if the subscriber's observability window hasn't advanced since the last push.

**Inhibition (Lazy)**: demand-driven. A `Lazy`-mode node does not recompute until `DEMAND` fires explicitly. Correct for effort-causal variables where pulling is semantically correct and pushing would be premature.

**Reflection (Stabilizing)**: when `Zero` reaches a `Stabilizing`-mode node, the conflicted region is routed to the egg e-graph equivalence saturator. If saturation resolves the conflict to a canonical value, propagation continues. If not, `Zero` remains — the obstruction is real.

**Inhibition rule**: when any dependency goes to `Neg` (pending), downstream nodes do not re-evaluate. They retain their current value. This prevents spurious clearing of derived state when an upstream signal goes temporarily unresolved.

---

## Key highlights

**Same-value suppression (~90 ns, 42× faster than baseline)**

A no-op write into a 500-node dependency chain costs approximately the same as a no-op write into a 10-node chain — ~90 ns — because the propagation graph is never activated once semantic equivalence is established:

```rust
if val == old { return Ok(vec![]); }  // single u8 comparison; no clock tick, no queue, no drain
```

**500-input convergence (1.0× amplification)**

A graph with 500 inputs feeding a single `MeetAll` output fires the output exactly once, after the 500th input arrives and state transitions Pending → Ready. Intermediate pushes are held in Pending state with zero downstream materializations.

**Infectious state and recovery (O(1))**

When any dependency is `Zero`, the output is forced to `Zero` regardless of the other 499. Recovery is O(1); only the toggling dep changes. The output fires on every toggle because `Zero → Pos` is a value change.

**Lazy coalescing**

An 80%-Lazy graph produces 97% fewer recomputations than a fully reactive baseline. Multiple upstream writes to a lazy node accumulate silently; the lazy node evaluates at most once per `DEMAND`.

---

## What distinguishes this from existing models

| Model | What it gets right | Where it differs |
|---|---|---|
| **Kafka** | Durable message ordering | No dependency graph; amplification = fan-out; recomputes everything |
| **Kubernetes** | Declarative reconciliation | Level-triggered; full reconcile per event; no ternary; no ISA |
| **RxJS / reactive streams** | Compositional data flow | No persistent graph; amplification = chain depth; no Bochvar boundary |
| **Salsa** | Incremental computation, DAG-aware | Compiler-centric; no distributed model; no ternary; no graph rewriting |
| **Differential Dataflow** | Change propagation, lattice-based | No graph rewriting at runtime; no Stabilize mode; no port semantics |
| **Datalog / Datomic** | Derived facts, declarative | No causal ordering; no DPO; no ternary; Neg ≠ absent |
| **E-graphs (egg)** | Equivalence saturation | Not a propagation runtime; no causal structure; no ISA |
| **Actor model** | Concurrency isolation | Process-centric; no persistent substrate; no shared graph |

The closest competitor is **Salsa** (rust-analyzer): also incremental, also DAG-aware, also skips recomputation when inputs are unchanged. The difference: Salsa is a compiler cache, modeling a static DAG over pure functions. pgress is a dynamic attributed graph that can be rewritten at runtime, supports non-monotone operations (Lazy, Reflect), handles structural conflicts via e-graph saturation, distributes across partition boundaries with causal consistency, and carries formal information-flow authority on every dependency edge.

## When not to use pgress

- Every event is independently meaningful. If downstream consumers need to process each individual message (e.g. financial tick data, audit logs, ordered command streams) then suppressing same-value writes is wrong. Use Kafka or a message queue; pgress is designed to discard redundancy, not preserve it.

- No derived state. If your system is mostly reads and writes with no computed dependencies between values, you don't need a dependency graph. A database with indexes will do less work with less overhead.

- Static DAG, pure functions, no runtime rewrites. If your dependency graph is fixed at compile time and never mutated, Salsa or a memoization cache is simpler and has lower operational surface area. pgress is designed for graphs that change shape at runtime. if yours doesn't, that design is overhead you'll never recover.

- You need a clean boolean false. pgress has no value meaning plain "false/off." Absence is None (`Neg`), which suspends downstream evaluation rather than propagating; False (`Zero`) means contested (infectious, self-negating), and is structurally unlike "no." Code that reaches for False expecting "off" triggers conflict propagation instead. If your domain needs an asserted negative that behaves like an ordinary boolean, the ternary model fights you at every turn.

- Small graphs under light load. The ISA dispatch, partition registry, and authority model carry constant-factor overhead. A 5-node graph with one writer will be faster in a HashMap. The amplification advantage emerges at scale in dense fan-in, high update rates, mixed eager/lazy topologies.

- Concurrent access. Each engine shard assumes serialized mutation; `Graph` has no internal locking. Multi-shard parallelism (via EnginePool + pick_shard_for) is the concurrency model: shards run independently, each serialized within. Lock-free reads and multi-writer access within a single shard are not supported.

---

## Quick start (Python)

```python
import pygress

g = pygress.Graph()

price  = g.input("price")
volume = g.input("volume")
signal = g.computed("signal", rule="and")
g.connect(price,  signal)
g.connect(volume, signal)

g.set(price,  True)
g.set(volume, True)
print(g.get(signal))    # True

g.set(volume, False)    # Bochvar: False short-circuits AND immediately
print(g.get(signal))    # False

# Lazy node — defers until observation
summary = g.computed("summary", rule="identity", lazy=True)
g.connect(signal, summary)

g.set(price, True)
g.set(volume, True)
print(g.get(summary))    # None  — not yet evaluated
print(g.demand(summary)) # True  — computed on demand
```

Build: `maturin develop --release` inside `pgress/py/` with an active virtualenv, or `maturin build --release` + `pip install` for a wheel. See `py/python.md` for the full SDK reference.

---

## Implementation

`core-rs` is the computation substrate: 18-opcode ISA, ternary propagation, delta-gated dependency tracking, e-graph equivalence saturation, two-layer node decomposition (identity / interpretation), and a partition authority model with `LatticeClass` + `CapabilityBits` bitmask gates. `session-rs` is the session manager dataplane that sits above the engine: wire decoding, path-to-session routing, four-dimensional `AuthorityPolicy` enforcement, admission control, shard placement, session expiry, and telemetry. The two crates share no mutable state; `session-rs` never imports engine internals.

For design details such as the ISA reference, two-layer decomposition, delta-gate mechanics, quiescent state model, RTL clock-gating coupling, session manager architecture, trust boundary, and distributed consistency model, see [`spec/architecture.md`](spec/architecture.md).

---

## Test suite

**Current results: 107/107 passing** (`pgress-core`, 73 unit + 20 proptest + 14 amplification assertion tests) **+ 198/198 passing** (`pgress-session`). **49/49 Python tests passing.**

### Deterministic simulation results (11/11 passing)

The `sim` feature gate enables a TigerBeetle-style in-process cluster simulation: seeded `Lcg64` deterministic RNG, explicit tick driver, and a simulated message bus with configurable drop rate, delay, and bidirectional partition injection. Key results:

| Test | Property verified |
|---|---|
| `no_false_pos_after_shard_crash` | **Safety**: a crashed shard's topology node never reads `Pos` on surviving shards |
| `crash_emits_topology_health_changed_event` | Crash event appears on telemetry partition |
| `healing_converges_to_pos` | **Liveness**: after heal, all peers converge to `Pos` within `max_delay + 5` ticks |
| `heal_emits_pos_topology_event_on_all_shards` | Heal event appears on all shard telemetry streams |
| `placement_skips_crashed_shard` | Placement routing avoids `Neg` shards |
| `placement_prefers_lowest_id_on_tie` | Tie-breaking is deterministic |
| `network_partition_isolates_gossip` | Partitioned shards do not receive cross-partition gossip |
| `deterministic_replay_same_seed_same_health_sequence` | Full replay determinism from seed |
| `convergence_under_different_gossip_orderings` | **AP structural test**: `delay=0` and `delay=3` orderings converge to the same final health class — the non-canonical paths collapse to the same observable quotient |
| `reaper_fires_only_on_stale_sessions` | Session expiry fires on inactive sessions, not active ones |
| `congested_shard_shows_zero_not_neg` | Congestion maps to `Zero` (contested/prefer-others), not `Neg` (absent) |

The AP structural test (`convergence_under_different_gossip_orderings`) is the key distributed systems result: it directly validates that different delivery orderings of the same events — corresponding to different non-canonical rewrite paths through the e-graph — produce the same converged observable health class. Non-confluence at the rewrite level; convergence at the quotient level.

### Property-based invariants (proptest) — 256 random ISA op sequences each

| Invariant | What it catches |
|---|---|
| Snapshot/replay equivalence | Non-determinism, hidden mutable state |
| Version monotonicity | Version rollback, stale cache bugs |
| No dangling edges | DPO admissibility failures post-delete |
| Identity layer stability | I8 violations — typ/kind mutated after creation |
| Causal history monotonicity | Clock regression in `arrival` counter |
| Propagation termination | Unexpected engine errors under arbitrary topologies |

### Adversarial topology tests

| Test | What it stresses |
|---|---|
| 64 inputs → 1 MeetAll | Fan-in propagation, Zero infection, recovery |
| 50-node Identity chain | End-to-end push from single `SetValue` |
| Same-value delta gate | Second `SetValue` same value: zero downstream version bump |
| `del_node` mid-chain | Downstream retains last value; upstream changes don't leak |
| Mode switch mid-stream | `set_mode_to_lazy` stops push; re-Eager restores it |

Bugs caught by proptest during development: e-graph index OOB on underfed rules; egg explanations requirement panic; non-deterministic HashSet iteration across `Engine` instances. All fixed.

---

## Binary ABI

The canonical wire encoding is specified in `spec/abi.md`. Key properties:

- **`IsaStreamHeader`** — 32-byte stream preamble: `magic[4]="PGRS", abi_version:u16=0x0003, flags:u16, feature_flags:u64, path_id:u64, session_id:u64`
- **`IsaHeader`** — 48-byte fixed prefix on every record (fits in one 64-byte cache line): `opcode:u16, flags:u16, length:u32 | tenant_id:u64, session_id:u64, partition_id:u64 | causal_epoch:u64, stream_seq:u64`
- **17 stable core opcodes** — discriminants `0x0001`–`0x0011` are frozen; `SessionProfile` (`0x0100`) is the first standard extension; append-only evolution
- **Handles** — all node/edge identity via opaque `u64` Uid; Uid 0 reserved (invalid)
- **Little-endian, no alignment padding, UTF-8 strings with u16 length prefix**

---

## Spec directory

| Document | Contents |
|---|---|
| `spec/architecture.md` | ISA reference; quiescent state model; RTL clock-gating; session manager; trust boundary; distributed consistency model |
| `spec/abi.md` | Binary wire encoding v0x0003; IsaHeader; IsaStreamHeader; opcode table; SessionProfile; evolution policy |
| `spec/integration.md` | Integration reference for host application or network service |
| `spec/benchmarks.md` | Work amplification results; hot-path optimization notes |
| `py/python.md` | Python SDK: installation, quickstart, walkthrough, API reference |

---

## Repository structure

```
pgress/
├── README.md            — this file
├── Cargo.toml           — workspace root (core-rs, session-rs, py)
├── spec/
│   ├── architecture.md  — ISA; quiescence/RTL; session manager; trust boundary; consistency model
│   ├── abi.md           — binary wire encoding (v0x0003)
│   ├── integration.md   — reference for session-rs integrations
│   ├── benchmarks.md    — amplification results, hot-path optimization notes
├── core-rs/             — Rust — computation substrate (pgress-core)
│   ├── Cargo.toml
│   ├── src/             — ISA, engine, propagation, e-graph, dep registry
│   └── tests/
│       └── proptest_invariants.rs  — invariant + adversarial tests
├── session-rs/          — Rust — session manager dataplane (pgress-session)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs       — wire ID newtypes; module declarations
│       ├── opcode.rs    — OpcodeClass; classify from opcode u16
│       ├── domain.rs    — tenant/session/partition/shard hierarchy; capability inheritance
│       ├── session.rs   — SessionTable, PathTable; stream_seq / path migration
│       ├── auth.rs      — AuthorityPolicy (assertion/delegation/observability/disclosure); PartitionAuthTable; lattice + policy gate
│       ├── profile.rs   — SessionProfile wire format; TrustStore; Advisory/Asserted/Attested; Ed25519 verification
│       ├── admission.rs — BackpressurePolicy; ShardPressure; AdmissionDecision
│       ├── recovery.rs  — WorkCursor; EngineOutcome; FailureAction
│       ├── telemetry.rs — TelemetryPartition; TelemetryEvent; health transitions
│       ├── topology.rs  — TopologyPartition; placement; ShardFabricAddr locality
│       ├── runtime.rs   — SessionRuntime; ParsedHeader; route_record pipeline
│       └── sim/         — deterministic simulation harness (feature = "sim")
│           ├── mod.rs   — SimCluster; Lcg64 RNG; tick driver; 11 sim tests
│           ├── network.rs — SimNetwork; PendingMsg; drain_due
│           ├── faults.rs — FaultPolicy; drop_rate, delay, bidirectional partitions
│           └── workload.rs — WorkloadBuilder; CrossShardWorkload (bench helpers)
├── py/                  — Python SDK — PyO3/maturin binding
│   ├── Cargo.toml       — cdylib crate depending on core-rs + pyo3
│   ├── pyproject.toml   — maturin build config; abi3-py39 wheel
│   ├── src/lib.rs       — PyO3 bindings (Graph, NodeHandle)
│   ├── pygress/__init__.py
│   ├── python.md        — SDK reference (this is what you want to read)
│   └── tests/test_pygress.py
├── rtl/                 — Hardware implementation
│   ├── sv/              — SystemVerilog RTL
│   │   ├── ternary_cell.sv      — single ternary cell; {p1,p0} two-bit encoding
│   │   ├── ternary_cell_syn.sv  — synthesis-ready variant
│   │   ├── ternary_chain.sv     — chain topology
│   │   ├── ternary_tree.sv      — tree topology
│   │   ├── ternary_region.sv    — region-level ICG; quiescence hierarchy
│   │   ├── meetall_500.sv       — 500-input MeetAll benchmark module
│   │   ├── icg_model.sv         — ICG behavioral model
│   │   ├── tb_meetall.sv        — MeetAll testbench
│   │   └── tb_topology.sv       — topology testbench
│   ├── hls/             — HLS kernel (Vitis HLS)
│   │   ├── ternary_meetall.h/cpp — MeetAll kernel
│   │   ├── tb_meetall.cpp       — HLS testbench
│   │   └── directives.tcl       — synthesis directives
│   ├── syn/             — Synthesis (Yosys)
│   │   ├── synth_ternary_cell.ys
│   │   └── run_synth.sh
│   ├── spice/           — SPICE netlists and simulation outputs
│   │   ├── t_cell.spice / t_dff.spice / t_zero.spice / t_zero_icg.spice
│   │   ├── t_region_icg.spice
│   │   └── run_spice.sh
│   ├── sim/             — Verilator simulation
│   │   ├── run_verilator.sh
│   │   ├── run_topology.sh
│   │   └── build/               — generated Verilator output (not checked in)
│   ├── pnr/             — OpenLane P&R configs and setup (sky130_fd_sc_hd)
│   │   ├── setup_openlane.sh        — stage RTL + configs into ~/OpenLane/designs/
│   │   ├── ternary_cell/            — N=8 MeetAll standalone; 80×80 µm die
│   │   ├── ternary_region/          — K=8 region with ICG; headline P&R target
│   │   └── meetall_500/             — N=500 flat MeetAll; 1000×1000 µm die
│   └── pdn/             — OpenLane P&R GDSII visual certificates (sky130_fd_sc_hd)
│       ├── ternary_cell_top.png   — N=8 MeetAll; 80×80 µm die; physical locality cert
│       ├── meetall_500_1.png      — N=500 MeetAll; 1000×1000 µm die; IO ring + OR reduction tree
│       ├── meetall_500_2.png      — N=500 MeetAll; cell-level view; dfxtp_4 output register
│       ├── ternary-region3.png    — K=8 region; ICG latch (dlxtn_1) at 0.4 µm; quiescence net to GATE_N
│       └── ternary-region4.png    — K=8 region; semantic workflow cluster: dlxtn_1 + a21oi_1 + xnor2_1 + dlygate4sd3_1
├── asm/                 — x86-64 SSE2 bitplane kernels (machine-level semantics witness)
│   ├── ternary_propkernel.S — meetall_sse2, joinany_sse2, region_quiescent
│   ├── test_propkernel.c    — 26 witness tests; chain propagation quiescence proof
│   └── Makefile             — gcc only (GAS .S; no nasm required)
├── demo/                — React + FastAPI interactive demo
│   ├── backend/         — Python FastAPI server; 6 scenarios
│   └── frontend/        — React Flow canvas; ternary state visualization
```
