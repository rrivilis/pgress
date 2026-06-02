# pgress

**pgress is a distributed runtime designed to actively suppress redundant computational work by tracking causal dependencies between observable state changes. Instead of recomputing everything when data changes, it tracks what actually depends on what and only propagates meaningful updates.**

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

The amplification gap is not just a performance issue. At scale, whether dealing with AI inference pipelines or real-time collaborative systems, recomputation determines whether the system is tractable at all. Existing systems still incur the cost of recomputation once dependencies are reconstructed indirectly. Data centers running LLM query throughput pay this cost continuously. 

Naive answers are expensive at scale, whether you are running AI inference pipelines, real-time collaborative systems, or network routing. pgress approaches the problem by making the dependency graph structure the primary computational artifact: there is no separate "change detection" layer. Every recomputation is gated by a formal test of whether it would produce a new observable value.

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

A graph with 500 inputs feeding a single `MeetAll` output fires the output exactly once — after the 500th input arrives and state transitions Pending → Ready. Intermediate pushes are held in Pending state with zero downstream materializations.

**Bochvar infection and recovery (O(1))**

When any dependency is `Zero`, the output is forced to `Zero` regardless of the other 499. Recovery is O(1) — only the toggling dep changes. The output fires on every toggle because `Zero → Pos` is a value change.

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

- Concurrent access. `Graph` is not thread-safe. If you need multiple writers or parallel reads without external serialization, pgress is not the right primitive in its current form.

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

## Implementation: `core-rs`

The active implementation is `pgress/core-rs`, a Rust crate. It implements the full computation substrate: ISA dispatch, ternary propagation, dependency tracking, delta-gated subscription, e-graph equivalence saturation, and an authority discipline over partition boundaries.

### ISA — the instruction set

All graph mutation goes through 18 ISA operations:

**Extension** — grow the graph / set values:

| Operation | Effect |
|---|---|
| `NodeCreate` | Add a node (Input or Computed with a rule) |
| `EdgeConnect` | Add a typed dependency edge; registers subscription; infers `ExecMode` from port kind |
| `Subscribe` | Register a push subscription directly |
| `SetValue` | Set an input node's value; delta-gates downstream push |
| `Propagate` | Eagerly push a node's current value to its subscribers |

**Inhibition** — retract / demand:

| Operation | Effect |
|---|---|
| `DelNode` | Remove a node + incident edges; DPO admissibility enforced |
| `DelEdge` | Remove a dependency edge |
| `Demand` | Pull-evaluate a Lazy node and its transitive pending deps |
| `SetMode` | Set execution mode: Eager / Lazy / Stabilizing |

**Reflection** — flip polarity / stabilize:

| Operation | Effect |
|---|---|
| `Reflect` | Apply `mv_neg` to a node's value and all ternary attrs; bumps version |
| `Stabilize` | Run e-graph saturation over a region; resolve or leave as `Zero` |

**Authority / partition** — id-layer only, no propagation enqueued:

| Operation | Effect |
|---|---|
| `PartitionCreate` | Declare a partition with its `AuthorityRoot`, `LatticeClass`, and `CausalScope` |
| `PartitionBind` | Bind a node to a partition; triggers compiled edge-label recompilation |
| `SetPartitionAuthority` | Mutate a partition's lattice class or causal domain |
| `SetEdgeLabel` | Compile and store an `EdgeLabel` on a `(source, target)` dep |
| `SetStabilizationConfig` | Set node-local e-graph semantics (strategy, domain, budget, convergence policy) |
| `SetExecutionPolicy` | Set node scheduling policy (queue priority, retry behavior) |

**Region compilation** — freeze topology for sparse circuit execution:

| Operation | Effect |
|---|---|
| `RegionDeclare` | Declare a subgraph region; compiles a CSR sparse circuit artifact if `CompilePolicy::Eager` |

### Two-layer decomposition (I8)

Every node carries two strictly separated layers:

```
id_layer(n)     = (uid, typ, kind)            — persistent, set once at NodeCreate
interp_layer(n) = (value, attrs, version)      — versioned, mutated by SetValue / Reflect / Stabilize
```

Every write to the interpretation layer that changes `value` or `attrs` increments `version`. A `(uid, version)` pair addresses a unique historical interpretation. Snapshots capture both layers independently via structural sharing — O(1) clone.

### Delta-gated propagation

Every `(source, subscriber)` dependency pair carries a `DepMeta` with:
- `label: CompiledEdgeLabel` — compiled flat form containing `projection_mask`, `capability`, `lattice_class`, and `causal_scope_bits`
- `last_seen: ProductTime` — the causal timestamp at which this subscriber last processed a push

A push is gated by `should_propagate(meta, current_time)`:
```rust
for dim in meta.label.projection_mask.dims {
    if current_time[dim] > meta.last_seen[dim] { return true; }
}
false
```

Setting the same value twice produces zero downstream version bumps on the second set. `CompiledEdgeLabel` is pre-baked into `DepMeta` so the hot path never touches the `PartitionRegistry`.

### Partition authority

`LatticeClass(u64)` encodes the transitive closure of the partition class hierarchy as a bitmask. The partial-order check is a single bit operation:
```rust
fn flows_to(self, other: LatticeClass) -> bool { (self.0 & other.0) == self.0 }
```

`CapabilityBits(u64)` is an unforgeable bitfield of permitted operations on a dep edge (`READ`, `PROPAGATE`, `STABILIZE`).

The **`authority_gate`** hot-path check is exactly three bit operations, no allocation, no registry access:
```rust
fn authority_gate(compiled: &CompiledEdgeLabel, emitted: &EmittedAuth) -> Result<(), GateReason> {
    compiled.capability.allows(emitted.capability)?;   // bit AND
    compiled_scope.contains(emitted.scope_bits)?;      // bit AND
    emitted.class.flows_to(compiled.lattice_class)?;   // bit AND
    Ok(())
}
```

Three `AuthorityMode` levels: `Advisory` (zero cost — no struct construction, no gate evaluated), `Audit` (one HashMap lookup + 3 bit ops, violations logged), `Enforced` (unauthorized subscriber enqueue suppressed).

---

## Session manager: `session-rs`

`session-rs` is the dataplane layer above the engine. It sits between the wire and the engine shard, applying admission checks from the `IsaHeader` alone — without parsing record bodies — and routing admitted records to the correct engine shard.

```
wire record
  ↓ IsaStreamHeader   path_id → session_id (PathTable)
  ↓ IsaHeader         session lookup, tenant check, stream_seq replay gate
  ↓ opcode classify   OpcodeClass from opcode u16 alone
  ↓ partition auth    PartitionAuthTable[(partition_id, opcode_class)] lattice gate
  ↓ admission         BackpressurePolicy — per opcode class treatment under load
  → engine shard      RouteOutcome::Admitted { opcode_class }
```

### Domain hierarchy

Authority is structurally bounded downward through four levels. A child domain can never hold capabilities its parent does not hold:

```
tenant      capability ceiling + aggregate quota
  └─ session    stream continuity + auth mode  (stable across path migration)
       └─ partition   causal isolation + PartitionAuthTable scope
            └─ shard      engine instance + quiescence guarantee + WorkCursor budget
```

### Session / path split (QUIC-style)

`session_id` is stable logical identity — survives transport interruptions and path migrations. `path_id` is ephemeral transport identity — lives in `IsaStreamHeader` only; the engine never sees it. `stream_seq` is session-scoped: increments continuously across path migrations, resets only on session termination.

### Execution geometry: topology as a partition

The topology partition (`PARTITION_TOPOLOGY = 0xFFFF_FFFF_FFFF_FFFD`) materialises the network diameter as a pgress graph using existing primitives — no separate routing protocol, no separate topology manager.

`ShardPressure` events are dual-written as ternary health state on shard nodes: `Pos` = accept new work, `Zero` = congested (prefer others), `Neg` = hot or unknown (do not route here). Placement decisions are resolved by reading the converged ternary — the propagation through the topology graph has already computed the routing. No BFS, no routing table update protocol, no separate placement service.

---

## Test suite

**Current results: 107/107 passing** (`pgress-core`, 73 unit + 20 proptest + 14 amplification assertion tests) **+ 151/151 passing** (`pgress-session`). **49/49 Python tests passing.**

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
│       ├── auth.rs      — PartitionAuthTable; lattice gate
│       ├── profile.rs   — SessionProfile; TrustLevel; Advisory/Asserted/Attested
│       ├── admission.rs — BackpressurePolicy; ShardPressure; AdmissionDecision
│       ├── recovery.rs  — WorkCursor; EngineOutcome; FailureAction
│       ├── telemetry.rs — TelemetryPartition; TelemetryEvent; health transitions
│       ├── topology.rs  — TopologyPartition; placement; ShardFabricAddr locality
│       └── runtime.rs   — SessionRuntime; ParsedHeader; route_record pipeline
├── py/                  — Python SDK — PyO3/maturin binding
│   ├── Cargo.toml       — cdylib crate depending on core-rs + pyo3
│   ├── pyproject.toml   — maturin build config; abi3-py39 wheel
│   ├── src/lib.rs       — PyO3 bindings (Graph, NodeHandle)
│   ├── pygress/__init__.py
│   ├── python.md        — SDK reference (this is what you want to read)
│   └── tests/test_pygress.py
├── demo/                — React + FastAPI interactive demo
│   ├── backend/         — Python FastAPI server; 6 scenarios
│   └── frontend/        — React Flow canvas; ternary state visualization
```
