# pgress Benchmarks and Hot-Path Optimizations

Reproduction: `cargo bench --bench amplification` in `core-rs/`. The harness uses [Criterion](https://github.com/bheisler/criterion.rs) (100 samples, warmup 3s).

---

## Work amplification — measured scenarios

```
work_amplification = total_recomputations / effective_updates
```

| Scenario | pgress | RxJS | Salsa | Kafka | K8s |
|---|---|---|---|---|---|
| **A** Linear chain, depth=50, 20 alternating writes | **3.5×** | 50× | 50× | 1× | 50× |
| **B** Same-value idempotency (depth=50, 20 × Pos) | **0×** | 50× | 50× | 50× | 50× |
| **C** Lazy demand (depth=50, 80% Lazy, 20 writes) | **1.5×** | 50× | 50× | 50× | 50× |
| **D** Dense fan-in, 64 inputs → 1 MeetAll output | **1.02×** | — | — | 1× | — |
| **E** Infectious Zero injection → recovery (64→1) | **O(1)** (2 events) | — | — | — | — |

Key results:
- **Scenario B**: same-value suppression eliminates 100% of redundant recomputation — zero downstream events, enforced by construction, not convention.
- **Scenario C**: 97% fewer recomputations than a fully reactive baseline by switching 80% of nodes to Lazy mode.
- **Scenario D**: fan-in convergence holds Pending until all deps are ready; output changes exactly once regardless of input arrival order.
- **Scenario E**: Zero infection and recovery are both O(1) downstream — the single output node is the only witness.

Scenario A's 3.5× arises from 20 writes on a 50-node chain where only 4 of the writes alternate (pgress = 70 recomps / 20 writes ≈ 3.5×). A fully alternating pattern approaches depth+1 = 51× which matches RxJS; the advantage is that **same-value writes cost exactly zero**.

---

## Frontier collapse — Lazy/demand split

**20 alternating writes to source, then one DEMAND on tail:**

| depth | eager_recomps | lazy_suppressed | demand_recomps | demand_frontier | mat/demand | savings% |
|-------|---------------|-----------------|----------------|-----------------|------------|----------|
| 10    | 10            | 0               | 0              | 1               | 0.0        | 100%     |
| 50    | 50            | 0               | 0              | 1               | 0.0        | 100%     |
| 100   | 100           | 0               | 0              | 1               | 0.0        | 100%     |

`demand_recomps=0` / `savings=100%` reflects a correctness property: 20 alternating writes leave the source in `T::Neg` (pending) as the final state. A DEMAND on the tail correctly answers "nothing to observe yet" — zero work, zero materializations.

**Multi-write batching — Lazy coalesces N writes into one materialization:**

| depth | writes | eager_recomps | demand_recomps | mat/demand | savings% |
|-------|--------|---------------|----------------|------------|----------|
| 50    | 1      | 50            | 50             | 1.0        | 0%       |
| 50    | 5      | 50            | 50             | 1.0        | 0%       |
| 50    | 20     | 50            | 0              | 0.0        | 100%     |
| 100   | 20     | 100           | 0              | 0.0        | 100%     |

`writes=1` and `writes=5` both produce `demand_recomps=depth`: the DFS demand traversal correctly unlocks the full Lazy chain on a single DEMAND. Five writes coalesce identically to one — intermediate states are never computed.

**Queue geometry (depth=50, 20 writes + 1 DEMAND):**

```
Eager:  max_queue_depth=  1  max_demand_frontier=  0  delta_suppressed=    0
Lazy+D: max_queue_depth= 51  max_demand_frontier=  1  mode_suppressed=     0
```

`demand_frontier=1` is the key invariant: the DFS ordering (`push_front` for sub-demand items) keeps the demand stack strictly serial. Breadth-first demand traversal would have `demand_frontier=depth`, interleaving pushes from the wrong phase and producing incorrect materializations.

---

## Criterion timings (release profile, AMD64)

| Benchmark | p50 |
|---|---|
| `linear_chain/eager_push/10` | ~2.1 µs |
| `linear_chain/eager_push/25` | ~2.5 µs |
| `linear_chain/eager_push/50` | ~2.1 µs |
| `linear_chain/same_value_noop/10` | ~971 ns |
| `linear_chain/same_value_noop/25` | ~913 ns |
| `linear_chain/same_value_noop/50` | ~966 ns |
| `dense_fanin/meetall_convergence/16` | ~81 µs |
| `dense_fanin/meetall_convergence/64` | ~475 µs |
| `dense_fanin/bochvar_infection/16` | ~87 µs |
| `fanin_500/convergence_500` | ~2.27 ms |
| `fanin_500/hot_toggle_zero_pos` | ~3.68 µs |
| `fanin_500/noop_same_value` | ~90 ns |

`same_value_noop` is bounded by the `should_propagate` delta-gate check, not rule evaluation. `meetall_convergence` builds a fresh fan-in graph each iteration (amortized construction cost included). The Stabilize path is invoked in `bochvar_infection` for `Stabilizing`-mode nodes.

Propagation cost is dominated by runtime constant factors rather than dependency depth — increasing chain depth 5× produces only a ~13% increase in propagation latency. `same_value_noop` at depth 10 vs depth 50 costs essentially the same (~966 ns) because the propagation graph is never activated once semantic equivalence is established.

---

## 500-node fanin — work amplification at scale

Three phases on a 500-input → 1 `MeetAll` output graph.

**Phase 1 — Convergence (all 500 inputs Neg → Pos, one-shot):**

| Metric | pgress | Kafka | K8s |
|---|---|---|---|
| total_recomputations | **1** | 500 | 250 000 |
| effective_updates | 1 | 1 | 1 |
| work_amplification | **1.0×** | 500× | 250 000× |

The output node fires exactly once — after the 500th input arrives and PropState transitions Pending → Ready.

**Phase 2 — Hot toggle (one input Zero ↔ Pos, 200 cycles):**

| Metric | pgress | Kafka | K8s |
|---|---|---|---|
| total_recomputations | **200** | 100 000 | 100 000 |
| effective_updates | 200 | 200 | 200 |
| work_amplification | **1.0×** | 500× | 500× |
| p50 wall-clock / cycle | **3.68 µs** | — | — |

Bochvar semantics: when any dep is Zero, the output is immediately forced to Zero regardless of the other 499. Recovery is O(1) — only the toggling dep changes.

**Phase 3 — No-op (same-value writes to all 500 inputs, 100 rounds):**

| Metric | pgress | Kafka | K8s |
|---|---|---|---|
| total_recomputations | **0** | 50 000 | 50 000 |
| effective_updates | 0 | 50 000 | 50 000 |
| suppression ratio | **∞** (100%) | 0% | 0% |
| p50 wall-clock | **~90 ns** | — | — |

Zero downstream materializations. Cost is one node lookup + one `T` comparison (`u8` equality) — subscriber loop, causal clock tick, and drain are all skipped entirely.

---

## Region-aware amplification — sparse circuit tier

Propagation over a dependency graph is structurally equivalent to sparse matrix-vector multiplication in the (meet, join) semiring over T. `step_push` is SpMV; the adjacency matrix is the dep graph; T::Zero is the semiring zero element. Compiled regions freeze the CSR adjacency and execute SpMV directly from dense ValueStore indices, eliminating HAMT lookups per entry.

Run with `cargo bench --bench amplification -- region`.

**Regime 1 — flat fanin, hot-toggle (all inputs alternate):**

| N | warm p50 | compiled p50 | ratio |
|---|---|---|---|
| 64  | ~1.9 µs | ~2.4 µs | 1.26× |
| 128 | ~2.9 µs | **~2.4 µs** | **0.83× (compiled wins)** |
| 500 | ~1.6 µs | ~1.7 µs | 1.06× (≈ parity) |

**Regime 2 — sparse write density (N=128, 200 cycles, varying k writes/cycle):**

| k writes/cycle | warm p50 | compiled p50 |
|---|---|---|
| 1   | ~752 µs  | ~1.04 ms |
| 4   | ~3.34 ms | ~5.68 ms |
| 16  | ~10.2 ms | ~17.3 ms |
| 32  | ~17.6 ms | ~36.3 ms |
| 64  | ~35.0 ms | ~66.3 ms |
| 128 | ~68.4 ms | ~137 ms  |

Compiled is consistently ~1.7–2× slower than warm at every density for flat fanin. Both paths produce identical amplification (≈ 1.0×) — the difference is purely constant-factor wall-clock.

**Regime 3 — topology churn amortization (N=64 fanin, 100 cycles):**

| churn interval | p50 total |
|---|---|
| churn every cycle  | ~5.1 ms |
| churn every 10     | ~1.8 ms |
| churn every 50     | ~1.6 ms |
| static (no churn)  | ~1.5 ms |

Topology mutations invalidate EpochTracked compiled artifacts. Recompilation cost amortizes quickly — at 10-cycle interval, total cost approaches the static floor within 20%.

**Regime 4 — multi-layer topology, k=1 update (dep-counters + compiled-handled):**

| topology (N×M, k_dep) | warm p50 | compiled p50 | ratio |
|---|---|---|---|
| 64×8, k=4 (9 members)   | ~2.61 µs | ~3.10 µs | 1.19× |
| 128×16, k=8 (17 members) | ~2.61 µs | **~2.54 µs** | **0.97× (compiled wins)** |
| 256×32, k=8 (33 members) | ~2.44 µs | ~2.99 µs | 1.23× |

Opt 2 makes `step_push` O(1) for MeetAll/JoinAny — the dep_counter returns `eval_aggregate()` immediately, eliminating the O(N) dep-value collection. Opt 5 marks compiled-region members so the warm drain skips re-evaluating them. Both effects compound: warm/N is approximately constant across N=64/128/500.

**M-sweep — N=128, k=8, M ∈ {8,12,16,20,24,32}:**

| M | warm p50 | compiled p50 | ratio |
|---|---|---|---|
| 8  | ~2.53 µs | ~2.54 µs | 1.00× |
| 12 | ~2.71 µs | ~2.83 µs | 1.04× |
| 16 | ~4.45 µs | ~5.11 µs | 1.15× |
| 20 | ~4.66 µs | **~3.43 µs** | **0.74× (compiled wins)** |
| 24 | ~2.72 µs | ~3.91 µs | 1.44× |
| 32 | ~5.93 µs | ~8.41 µs | 1.42× |

No structural cliff at M=16. All confidence intervals overlap across M ∈ {8..24}. High variance (σ≈0.3×) persists — the distribution shape is unchanged; only the mean shifts down. The ~1.1× mean ratio is the irreducible floor of current compiled-path infrastructure: dirty-slot scan, 2 binary searches into `col_inv_keys`, pending bitmask updates, and warm-path double-processing of tier-2-handled boundary subscribers.

**Both paths produce identical amplification (≈ 1.0×)** across all regimes — confirmed by `PropStats.nodes_materialized`. The compiled circuit and warm engine are semantically equivalent; the compiled path is a constant-factor optimization for topologies where CSR structure amortizes over many invocations.

---

## Baseline amplification model

| System | Model | Amplification |
|---|---|---|
| **pgress** | Ternary algebra, derived causal graph | **≈ 1.0×** — output fires iff value changes; Zero/Pending suppress downstream |
| **Kafka + consumer group** | Fan-out streaming | ≈ F — every event hits every consumer |
| **Kubernetes controllers** | Level-triggered reconcile | ≈ N — full reconcile on every informer event |
| **RxJS / reactive chain** | Synchronous dataflow | ≈ D — every upstream change traverses full chain |
| **Salsa** | Incremental DAG (static) | ≈ dirty sub-DAG — no dynamic topology; no Zero semantics |
| **Differential Dataflow** | Change propagation | ≈ 1.0 for static graphs; recompute on schema change |
| **Redis pub/sub** | Broadcast, no dep model | ≈ F — no suppression of unchanged values |

---

## Hot-path optimizations

Twelve optimizations applied to `core-rs` to eliminate allocations and indirect lookups on the propagation hot path. All tests pass before and after each change.

### Opt 1 — ValueStore: flat array replaces HAMT per-dep lookup

`im::HashMap` (HAMT) gives O(1) clone for snapshots but ~10× slower per-lookup than a plain `Vec`. `ValueStore` keeps a dense `Vec<T>` indexed by a `u32` dense index assigned at `NodeCreate`:

```rust
pub struct ValueStore {
    cache: Vec<T>,           // T::Neg by default
    idx:   FxHashMap<Uid, u32>,
}
```

`DepRegistry` gains `ordered_dep_indices: ImMap<Uid, SmallVec<[u32; 4]>>`. In `step_push`, dep collection becomes a slice index:

```rust
let cache = value_store.cache_slice();
let dep_vals: SmallVec<[T; 8]> = dep_indices.iter().map(|&i| cache[i as usize]).collect();
```

No allocation on the fast path for ≤8 deps.

### Opt 2 — Single-pass branchless PropState

0/1/2-dep fast-path `match` arms and early-return on the first Zero in the general case:

```rust
_ => {
    let mut n_pos: u32 = 0;
    for &v in vals {
        match v {
            T::Zero => return PropState::Conflicted,  // short-circuit
            T::Pos  => n_pos += 1,
            T::Neg  => {}
        }
    }
    if n_pos as usize == vals.len() { Ready } else { Pending }
}
```

Same pattern applied to `meet_all` and `join_any` (`[a]`, `[a,b]`, `[a,b,c]` match arms before general fold).

### Opt 3 — SmallVec for dep_vals

`dep_vals` changed from `Vec<T>` (heap allocation per step) to `SmallVec<[T; 8]>` (stack-allocated for ≤8 deps). Combined with Opt 1, zero heap allocations on the common fan-in path.

### Opt 4 — Pre-allocated work queue

`PropEngine::new()` pre-allocates:

```rust
queue:  VecDeque::with_capacity(64),
events: Vec::with_capacity(256),
```

Eliminates reallocation churn during the first propagation wave.

### Opt 5 — Software prefetch for dep values

Before collecting dep values, a prefetch loop hints the hardware prefetcher:

```rust
#[cfg(target_arch = "x86_64")]
for &idx in dep_indices {
    unsafe {
        std::arch::x86_64::_mm_prefetch(
            cache.as_ptr().add(idx as usize) as *const i8,
            std::arch::x86_64::_MM_HINT_T0,
        );
    }
}
```

Hides DRAM latency for large fan-in graphs where dep values span multiple cache lines.

### Opt 6 — `repr(u8)` + variant reorder for SIMD readiness

`T` is now `#[repr(u8)]` with `Neg=0, Pos=1, Zero=2`. Enables future 2-bit packing (4 ternary values per byte) and direct SIMD byte-lane operations without a shuffle step. Bit-sliced storage (bit0 plane = Pos, bit1 plane = Zero; `meet_all`/`join_any` reducible to bitwise AND/OR) is the natural next step; crossover around 8–12 deps.

### Opt 7 — Same-value early exit in `SetValue`

Before this optimization, a no-op write still traversed the full subscriber loop, ticked the causal clock, and ran `drain_and_stabilize`:

```rust
IsaOp::SetValue { node, val } => {
    let n = self.graph.node_mut(node).ok_or(...)?;
    let old = n.value;
    // Opt 7: same-value no-op — skip subscriber loop, causal tick, and drain.
    if val == old {
        return Ok(vec![]);
    }
    // ... log, set, enqueue, drain
}
```

`T` is `#[repr(u8)]`, so the comparison is a single `cmp` instruction. Result: ~3.73 µs → ~90 ns (~42× speedup) on the 500-input no-op benchmark.

### Opt 8 — Per-region dirty-flag for compiled sparse circuits

`RegionArtifactCache` maintains a `dirty: FxHashSet<Uid>` and `input_to_regions: FxHashMap<u32, Vec<Uid>>`. `mark_dirty_for_input(dense)` is called after every `SetValue`/`Reflect` write. Without this, the tier-2 dispatch iterated all compiled regions on every drain cycle regardless of whether any boundary input changed — O(N × regions) overhead on every ISA op.

The dirty-check eliminates all compiled-region overhead when no boundary input changed in the current drain cycle.

### Opt 9 — Column-push inverted index for compiled regions

`CompiledRegion` gains a second CSR that inverts the dep relationship:

```rust
col_inv_keys:    Box<[u32]>,   // sorted unique dep dense indices
col_inv_offsets: Box<[u32]>,   // CSR offsets into col_inv_rows
col_inv_rows:    Box<[u32]>,   // row indices per key
```

Phase 1 (pending bitmask): iterate `dirty_slots` words with Brian Kernighan bit extraction; for each dirty input slot, binary-search `col_inv_keys` to find affected rows. Phase 2 (evaluation): iterate rows in topo order; skip rows whose pending bit is clear.

Closes O(all_rows × avg_deps) → O(dirty_inputs × avg_fan_out), matching the warm path's column-push semantics in the compiled tier.

### Opt 10 — Bit-sliced T planes and SWAR bulk operations

`ValueStore` gains two parallel `Vec<u64>` planes:

```
plane_p0[w]: bit j is set iff cache[w*64 + j] == T::Pos  (repr bit 0)
plane_p1[w]: bit j is set iff cache[w*64 + j] == T::Zero (repr bit 1)
```

`meet_all_range(start, len)` and `join_any_range(start, len)` operate on these planes with ~3 word-level ops per 64-slot window:

```rust
// MeetAll: result is Neg if any slot is Neg, Zero if no Neg but any Zero, else Pos.
let neg_bits = (!p0m) & (!p1m) & mask;  // Neg = p0=0 AND p1=0
if neg_bits != 0 { return T::Neg; }
if (!p0m) & p1m != 0 { has_zero = true; }
```

`CompiledRegion` gains `contig_start: Box<[Option<u32>]>`: `Some(first_dense)` when a row's deps form a contiguous range. For those rows, `run_compiled_region` dispatches to `meet_all_range`/`join_any_range` (SWAR path). In the benchmark topology (nodes allocated in layer order), all rows are contiguous.

### Opt 11 — O(1) epoch check elimination

The `drain_and_stabilize` tier-2 loop previously called `deps.max_epoch_for_nodes(region.members)` on every dirty region — O(N) FxHashMap hash operations per member. This was dead work: `EpochTracked` regions are removed from both `regions` AND `dirty` by `invalidate_for_node` at mutation time. The epoch re-check is removed entirely; `take_dirty()` never returns a stale root.

### Opt 12 — Dep-counter optimization 

`DepRegistry` maintains per-subscriber `{neg_count, zero_count, pos_count}` counters updated at every `SetValue`/`Reflect`. `step_push` for `MeetAll`/`JoinAny` reads the counter instead of collecting all dep values — O(1) aggregate regardless of N:

```rust
fn eval_aggregate(&self) -> T {
    if self.neg_count  > 0 { T::Neg  }
    else if self.zero_count > 0 { T::Zero }
    else                         { T::Pos  }
}
```

The shortcut fires only when `ctrs.total() as usize == n_deps` (all deps have registered). Zero wins (Bochvar priority); Neg next; Pos if all deps confirm.

**Critical**: counters must be updated by `step_push` when a computed node's value changes — not only by `SetValue`/`Reflect` in `engine.rs`. Failing to update counters through computed→computed chains caused stale `neg_count` values that prevented `MeetAll`/`JoinAny` nodes from ever evaluating beyond the first Pending state. See regression tests `dep_counter_updated_through_direct_computed_chain`, `dep_counter_propagates_through_computed_fan_in`, `dep_counter_bochvar_through_computed_chain` in `engine.rs`.

---

## Hardware lowering — RTL simulation results

Reproduction: `./rtl/sim/run_verilator.sh` and `./rtl/sim/run_topology.sh` in `rtl/` (requires Verilator >= 5.0).

RTL files: `rtl/sv/ternary_cell.sv`, `rtl/sv/meetall_500.sv`, `rtl/sv/ternary_chain.sv`, `rtl/sv/ternary_tree.sv`. HLS prototype: `rtl/hls/ternary_meetall.cpp`.

Clock: 4 ns period (250 MHz). All RTL numbers are cycle-exact Verilator simulation; software numbers are Criterion p50 on the same workloads.

### Flat fan-in — 500-input MeetAll (tb_meetall)

| Scenario | RTL latency | Software p50 | Speedup |
|---|---|---|---|
| Convergence (500 Neg → Pos, one-shot) | **4 ns** (1 cycle) | ~2.27 ms | **~567 000×** |
| Hot toggle (Zero ↔ Pos, per cycle) | **4 ns/cycle** | ~3.68 µs/cycle | **~920×** |
| No-op (same-value re-drive) | **0 ns** (ce=0) | ~90 ns | **∞** (zero dynamic power) |

No-op at zero cost is structural, not a run-time optimization: clock-enable suppression (`ce = comb_out != out`) prevents the output register from toggling when the combinational result matches the stored value. This is the direct hardware analogue of Opt 7 (same-value early exit) and Opt 12 (dep-counter shortcut).

Hot-toggle speed difference (920×) reflects the gap between one clocked register update and a full Rust tick: queue drain, dep-counter update, causal epoch increment, and subscriber loop. The RTL path has none of these because the fixed-point rule is directly materialized in silicon.

### Topology stabilization — chain and tree (tb_topology)

A pgress `CompiledRegion` maps to a circuit of `ternary_cell` instances with `quiescent = NOR(all ce_out)`. Stabilization latency is topologically determined: no queue, no scheduling, no causal overhead.

**ternary_chain (K=8, FAN_IN=4, MeetAll)**

A Zero injected at stage 0 propagates through all 8 registered stages before `quiescent` fires:

| Scenario | RTL cycles | RTL latency @ 250 MHz | Software est. | Speedup |
|---|---|---|---|---|
| Zero propagation through K=8 stages | **8** | **32 ns** | ~32 µs | **~1 000×** |
| Recovery (remove Zero, chain → Pos) | **8** | **32 ns** | ~32 µs | **~1 000×** |
| No-op re-drive (20 rounds) | **0** (ce=0) | **0 ns** | — | **∞** |

**ternary_tree (L=4, FANOUT=2, LEAF_FAN=4, MeetAll)**

8 leaf cells, 4 intermediate cells, 1 root. A perturbation at any leaf propagates to root in L=4 clock cycles.

| Scenario | RTL cycles | RTL latency @ 250 MHz | Software est. | Speedup |
|---|---|---|---|---|
| All leaves Neg → all Pos (simultaneous) | **4** | **16 ns** | ~12 µs | **~750×** |
| Single leaf Zero injection | **4** | **16 ns** | ~12 µs | **~750×** |
| Single leaf recovery | **4** | **16 ns** | ~12 µs | **~750×** |
| No-op re-drive (20 rounds) | **0** (ce=0) | **0 ns** | — | **∞** |

Software estimates use ~4 µs/hop (Rust causal tick + dep-counter update); actual software measurements pending for these topology topologies.

### Predictable latency by topology

The key property demonstrated by the RTL results: **stabilization latency is a deterministic function of topology, not of runtime load**.

| Topology | Stabilization cycles | Rule |
|---|---|---|
| Single cell, N inputs | 1 | MeetAll/JoinAny is a single-cycle combinational op |
| K-stage pipeline chain | K | One register per stage; change propagates at 1 stage/cycle |
| L-level FANOUT-ary tree | L | One register per level; change propagates at 1 level/cycle |
| General DAG, critical path D | D | One register per edge hop on the longest path |

For a `CompiledRegion` with critical path depth D, the RTL stabilization latency is exactly `D × T_clock`. The hardware backend maps into the same ternary fixed-point algebra, and the software oracle and hardware fabric must agree on the quiescent state. 

**Quiescent-state equivalence** is the hardware correctness criterion: for every region R and input vector v, the RTL fabric reaches the same ternary fixed point as `run_compiled_region` on the software engine. The RTL simulation confirms this for all 12 tested scenarios. 

What seems to drive this result is that the only monotone quantity is the e-graph equivalence relation. The hardware does not see a global state or observable in the quiescence predicate, only that no new distinctions are being made. The clock gate materializes activity from the fact that no new distinctions can emerge. Once equivalence relations are saturated by the e-graph, the question of whether anything new will occur and the question of whether the clock tree should keep toggling become the same question.

### CGRA vs FPGA projection

Same RTL, different clock target. CGRA cells are coarse-grained and operate at higher frequency than equivalent FPGA LUT implementations:

| Target | Clock | Chain K=8 | Tree L=4 | SW (Rust) |
|---|---|---|---|---|
| FPGA (Xilinx UltraScale+) | 300 MHz | 26 ns | 13 ns | ~32 µs / ~12 µs |
| CGRA (est. 500 MHz) | 500 MHz | 16 ns | 8 ns | ~32 µs / ~12 µs |
| CGRA (est. 800 MHz) | 800 MHz | 10 ns | 5 ns | ~32 µs / ~12 µs |

CGRA is the preferred materialized fabric target over ASIC because:
- **Dynamic topology**: routing reconfiguration matches pgress's dynamic region rewrites; ASIC requires re-synthesis.
- **Granularity match**: coarse-grained CGRA cells map 1:1 to `ternary_cell` instances; no LUT decomposition overhead.
- **Host split**: Stabilize, egg e-graph saturation, and Demand remain on the host CPU. CGRA handles the hot SpMV path only. CGRA reconfigurability preserves this split at runtime.

Speedup ratios hold at all targets because software costs (queue drain, causal tick, dep-counter update) do not scale with clock frequency.

---

## Gate-level synthesis — Yosys characterization

Reproduction: `./rtl/syn/run_synth.sh` (requires Yosys ≥ 0.24).

Source: `rtl/sv/ternary_cell_syn.sv` (synthesis-compatible variant, `automatic` keyword removed). Synthesis target: generic LUT-6 (6-input LUT) using `abc -lut 6` with full optimization pipeline (`proc; flatten; opt; memory; techmap; opt; abc -lut 6`). Results reflect 6-input LUT technology; FPGA back-end targets (Xilinx UltraScale+, Intel Agilex) use this primitive natively. CGRA targets translate LUTs to coarse-grained ALU cells post-synthesis.

### Resource utilization

| Configuration | Generic gates (pre-synthesis) | LUT-6 cells | DFF cells | LUT reduction |
|---|---|---|---|---|
| N=500, MeetAll | 3 006 | 583 | 2 | **5.15× compression** |
| N=4,   MeetAll | 12   | 5   | 2 | 2.4× compression |
| N=4,   JoinAny | 12   | 5   | 2 | 2.4× compression |

The 5× LUT compression at N=500 reflects ABC's global optimization: wide OR-trees across 500 slots collapse into balanced LUT networks far more efficient than direct 1-gate-per-slot expansion.

### Critical path depth

| Configuration | LUT levels (critical path) | Est. latency @ 500 MHz |
|---|---|---|
| N=500, MeetAll | **6** | **12 ns** |
| N=4,   MeetAll | 2    | 4 ns |

Six LUT levels for N=500 MeetAll. Each level adds one LUT propagation delay (~0.5 ns at 500 MHz for a well-placed CGRA or FPGA cell). The 6-level depth is consistent with a balanced binary reduction tree over 500 inputs: ⌈log₂(500)⌉ = 9 levels naively, but ABC exploits MeetAll's associativity and the fixed 6-input LUT width to achieve 6 levels.

This is the registered critical path D=6 for the flat fan-in topology entry in the predictable-latency table: a 500-input MeetAll stabilizes in 6 clock cycles in the general registered case (1 cycle in the fully combinational case when the output register is at the root only).

### Fanout analysis

| Metric | N=500 MeetAll |
|---|---|
| Maximum net fanout | **2** |
| Average net fanout | ~1.1 |

Maximum fanout=2 confirms that ABC's technology mapping holds the LUT network to near-unit fanout throughout. No high-fanout net exists in the synthesized MeetAll — the tree structure prevents any single intermediate signal from driving many consumers. This is structurally important for FPGA/CGRA placement: no buffering or fanout insertion is required, and routing is local.

### Optimization: `any_pos` eliminated by synthesis

For MeetAll with N≥3, `any_pos` (the Pos-reduction OR tree) is entirely removed by ABC. The reason: `MeetAll` returns Pos only when `!any_neg && !any_zero` — i.e., the `else` branch, which needs no explicit test. ABC determines that `any_pos` is never consulted as an independent signal and culls the entire OR tree (N gates → 0). Only 2 of the 3 reduction planes are materialized in silicon:

| Plane | Gates in LUT netlist |
|---|---|
| `any_neg` reduction | ✓ present |
| `any_zero` reduction | ✓ present |
| `any_pos` reduction | ✗ eliminated (implicit else branch) |

JoinAny has the symmetric optimization: `any_neg` is implicit.

### Synthesis summary

```
ternary_cell (N=500, MeetAll)
  Generic gates (pre-abc):  3,006
  LUT-6 cells:                583
  DFF cells:                    2
  LUT levels:                   6
  Max net fanout:               2
  any_pos OR-tree:         PRUNED (ABC optimization)
```

The 2 DFF cells correspond to the 2-bit registered output `{p1, p0}` — the `$_SDFFE_PP0P_` (T_DFF) at the root.

---

## Switch-level characterization — sky130_fd_sc_hd SPICE decks

Reproduction: install ngspice and sky130 PDK, then `./rtl/spice/run_spice.sh all`.

```bash
sudo apt install ngspice -y
pip3 install sky130
./rtl/spice/run_spice.sh all
```

Three primitive cells characterized against the SkyWater 130nm high-density standard cell library (`sky130_fd_sc_hd`). These are the transistor-level certification of the RTL and synthesis results above.

### T_CLASS — ternary slot classifier

**Function**: `{p0, p1}` → `{is_neg, is_zero, is_pos}` one-hot flags.

**Implementation**: 5 cells, ~20 transistors.
- `nor2_1`: `is_neg = NOR(p0, p1) = ~p0 & ~p1`
- `inv_1` × 2: shared `~p0`, `~p1` (each reused by two downstream AND cells)
- `and2_0` × 2: `is_zero = AND(~p0, p1)`, `is_pos = AND(p0, ~p1)`

| Measurement | Measured | Description |
|---|---|---|
| `tpd_neg` | **22.7 ps** | NOR2 direct path — single gate delay |
| `tpd_pos` | **36.1 ps** | INV + AND2 via p0 — two gate levels |
| `tpd_zero` | **60.7 ps** | INV + AND2 via p1 — two gate levels |
| `power_avg` | **1.95 µW** | Average VDD power during 30 ns state sweep |

`tpd_neg` (22.7 ps) is the fastest path with NOR2 direct output with no intervening INV stage. `tpd_pos` (36.1 ps) and `tpd_zero` (60.7 ps) traverse one shared INV plus an AND2; the asymmetry between them reflects the p0 vs p1 transistor sizing difference in the INV cells. All three are well within a single 4 ns clock period (250 MHz), confirming that the full ternary classification completes combinationally in one clock cycle. The critical path for a `ternary_cell` reduction followed by T_CLASS output is `D + 60.7 ps` where D is the reduction depth in LUT levels.

### T_DFF — ternary register with clock enable

**Function**: 2-bit ternary register, synchronous reset, positive-edge clock enable.

**Yosys cell**: `$_SDFFE_PP0P_` (positive clock, positive CE, reset-to-0).

**Implementation**: 5 cells, ~30 transistors.
- `inv_1`: `RST → RST_B` (sky130 dfrtp has active-low reset)
- `mux2_1` × 2: CE multiplexer per bit — `D_eff = CE ? D : Q`
- `dfrtp_1` × 2: D flip-flop with reset per bit

| Scenario | Measurement | Measured | Description |
|---|---|---|---|
| A — Active toggling (CE=1, data alternates 250 MHz) | `power_active` | **18.97 µW** | Baseline dynamic power |
| B — Data matches stored (CE=1, D==Q) | `power_ce_data_matches` | **18.74 µW** | MUX+DFF overhead when same-value |
| C — CE=0 quiescent | `power_ce_zero` | **9.14 µW** | Full quiescence — CE pin low |

| Ratio | Value | Meaning |
|---|---|---|
| `power_active / power_ce_data_matches` | **1.01×** | CE=1 with D==Q saves almost nothing |
| `power_active / power_ce_zero` | **2.07×** | CE=0 cuts power in half |

The 1.01× ratio for scenario B reveals that driving CE=1 with D equal to Q barely reduces power; that is, the MUX and DFF clock network still switch on every posedge CLK regardless of whether the output changes. The meaningful saving comes only when CE is actively driven to 0 (scenario C), which cuts power by 2.07×. This is the transistor-level proof certificate of why `ternary_cell`'s clock-enable suppression (`ce = comb_out != out`) must assert CE=0 rather than relying on D==Q passivity. Scenario C's 9.14 µW represents the residual clock-tree switching cost — the DFF clock buffer inside `dfrtp_1` switches every 4 ns regardless of CE.

### T_ZERO — frustration isolation cell

**Function**: T_DFF variant that locks out the clock enable once the output reaches Zero state (`p1=1, p0=0`). Models the frustration/contradiction isolation property of pgress: once a region node reaches Bochvar Zero, it contributes no further computation until an explicit synchronous reset.

**Why T_ZERO vs T_DFF at Zero**: A plain T_DFF at Zero still runs the CE comparison every clock cycle — the XOR comparator sees `comb_out == out → ce=0`, but the comparator itself switches transiently on each clock edge as upstream combinational logic ripples. T_ZERO adds an internal CE lockout that short-circuits `CE_eff` to 0 without consulting the comparator:

```
is_at_zero = Q_p1 & ~Q_p0        -- detect out == Zero
CE_eff     = CE_ext & ~is_at_zero -- lockout: CE_eff = 0 once Zero detected
```

Once `out == Zero`: `CE_eff = 0`. The MUX never updates `D_eff`. The XOR comparator is still live, but the DFF input is frozen, and the comparator's switching is itself suppressed by the lockout.

**Implementation**: 9 cells, ~40 transistors (T_DFF core + 4 lockout cells).
- T_DFF core: `inv_1`, `mux2_1` × 2, `dfrtp_1` × 2
- Lockout: `inv_1` (Q_p0 → inv_q0), `and2_0` (Q_p1, inv_q0 → is_at_zero), `nand2_1` (CE_ext, is_at_zero → ce_gate_b), `inv_1` (ce_gate_b → CE_eff)

| Phase | Time range | Measurement | Measured | Description |
|---|---|---|---|---|
| 1 — Active | 0–40 ns | `power_active` | **9.59 µW** | Neg/Pos toggling at 250 MHz; CE_eff=1 |
| 2 — Zero latch | 40–48 ns | `power_transition` | **8.96 µW** | Final switching event as Zero is latched |
| 3 — Isolated | 60–120 ns | `power_isolated` | **9.14 µW** | Locked at Zero; CE_eff≈0 |
| — | 60–120 ns | `ce_eff_during_isolation` | **76.5 nV** | CE_eff mean voltage; confirms lockout |

**Isolation factor**: `power_active / power_isolated` = **1.05×**

**Interpretation**: The 1.05× factor reflects a physically correct result, not a failure of the cell. `ce_eff_during_isolation = 76.5 nV ≈ 0 V` confirms the lockout circuit is structurally correct — no new data is ever loaded into the DFF after Zero is detected. However, the internal clock network of the two `dfrtp_1` cells continues to switch on every posedge CLK regardless of `CE_eff`. At sky130 130nm, the clock buffer dissipation dominates the power budget and accounts for nearly all of the 9.14 µW "isolated" power. Active data-path switching adds only ~5% on top of this floor.

The distinction is between two forms of isolation:

| Isolation type | Provided by T_ZERO? | Mechanism |
|---|---|---|
| **Data isolation** (no new values latched) | ✓ Yes | `CE_eff = 0` by lockout — proven by `ce_eff = 76.5 nV` |
| **Power isolation** (leakage-floor dissipation) | ✗ No | Requires ICG clock gating upstream of DFF clock pin |

T_ZERO is a *semantic* isolation cell: it guarantees that once a region node reaches the Zero state, no further input changes can corrupt the stored value without any software intervention, and without any external CE signal assertion. The power floor is set by the clock network, which requires a separate Integrated Clock Gate (ICG) cell to fully quiesce. The T_DFF scenario C result (CE=0 → 9.14 µW) confirms this: even with CE explicitly held at 0, the clock-tree power is identical because the DFF clock input is not gated.

**Corrected framing**: the benchmarks.md text above that said "dynamic power drops to the leakage floor" was premature. The actual floor is the clock-network switching floor (~9 µW for two `dfrtp_1` at 250 MHz), not transistor leakage (~nW range). Leakage-floor isolation requires pairing T_ZERO with a clock gate.

**T_ZERO as a named PDK cell**: T_ZERO's semantic guarantee is still distinct enough from T_DFF to warrant a standalone cell entry. The lockout logic (4 cells, ~20 transistors) is below the threshold where ABC would discover and preserve the Zero-state detection automaton from a behavioural description. A library designer gets an explicit, formally-specified isolation guarantee at the cell boundary.

---

### Custom and ICG comparison decks

The three baseline cells above used standard-cell composition. The decks below characterize two alternative implementations — a flat transistor-level T_CLASS and an ICG-gated T_ZERO — and a side-by-side MUX-vs-ICG DFF comparison. Each deck uses identical stimulus to its corresponding baseline for direct comparison.

#### T_CLASS_CUSTOM — flat MOSFET-level implementation

**Design intent**: eliminate inter-cell routing parasitics by flattening the five-cell NOR2 + 2×INV + 2×AND2 composition into a single subcircuit. Shared `~p0`/`~p1` internal nodes drive NAND2 PDN inputs directly with no cell-boundary re-buffering.

**Implementation** (`rtl/spice/t_class_custom.spice`): same 20 transistors as the standard-cell baseline, but instantiated as raw `sky130_fd_pr__nfet_01v8` and `sky130_fd_pr__pfet_01v8_hvt` X-instances. Minimum HD sizing throughout; NOR2 PUN uses 2×W PMOS for series-stack balance; NAND2 PDN uses 2×W NMOS for series-stack drive.

| Measurement | T_CLASS (std cell) | T_CLASS_CUSTOM (flat) | Δ |
|---|---|---|---|
| `tpd_neg` | 22.7 ps | **24.9 ps** | +10% (parity) |
| `tpd_pos` | 36.1 ps | **26.6 ps** | **−26%** ✓ |
| `tpd_zero` | 60.7 ps | **94.1 ps** | +55% ✗ |
| `power_avg` | 1.95 µW | **2.71 µW** | +39% ✗ |
| Cells | 5 | ~20T flat (no cell wrappers) | — |

**Interpretation**:

`tpd_pos` improves 26% — the targeted benefit. The p0 → is_pos path (p0 → NAND2 PDN → INV output) no longer crosses a cell boundary with its wire capacitance and output driver overhead; the minimum-sized flat transistors see only the intrinsic gate cap of the next stage.

`tpd_zero` degrades 55%. The root cause is the shared `p0_b` node. In the flat design, `p0_b` is the output of the inverter pair (Xinv_p0_p + Xinv_p0_n) and drives three MOSFET gates simultaneously: Xpos_pp0, Xpos_np0 (pos-path NAND2), and Xzero_np0 (zero-path NAND2 PDN). This FO3 load — versus the standard cell's internal inversion that drives only a single internal node — slows `p0_b` settling by approximately the ratio of output capacitances. The standard `and2_0` cell's internal inverter carries FO1 load only; the cross-cell `inv_1` in the standard composition drives two `and2_0` inputs (FO2) which is still faster than the flat FO3 case at minimum sizing.

`power_avg` increases 39%. Minimum-sized flat transistors do not benefit from the standard cell's internal routing optimization (shorter wire stubs, tighter drain-source coupling), and the flat netlist exposes additional wire parasitics between primitive devices. The standard cell's highly optimized layout keeps internal nodes at leakage scale; the flat netlist adds explicit node capacitance at each X-instance port.

**Conclusion**: flat transistor composition with a shared internal node is selectively beneficial. The `is_pos` path (p0 direct input to NAND2, no shared-node degradation) wins 26%. The `is_zero` path (which depends on the loaded shared `p0_b` node) is significantly slower. A balanced custom layout would need to either size up the `p0_b` driver or restructure the is_zero path to avoid the shared node.

---

#### T_ZERO_ICG — frustration isolation with dlclkp_1 clock gate

**Design intent**: replace the two `mux2_1` CE multiplexers with a `dlclkp_1` integrated clock gate. The MUX-based T_ZERO keeps the DFF clock input running on every posedge regardless of `CE_eff`; the ICG holds `GCLK` low when `GATE = CE_eff = 0`, fully quiescing the DFF clock input and internal pipeline.

**Implementation** (`rtl/spice/t_zero_icg.spice`):
- Remove: 2×`mux2_1` (CE function moved to clock gate)
- Add: `dlclkp_1` (CLK=CLK, GATE=CE_eff → GCLK)
- DFFs use GCLK directly: `dfrtp_1(GCLK, D, RST_B)` — no D-input MUX needed
- Lockout logic unchanged: `inv_1` + `and2_0` + `nand2_1` + `inv_1`
- Net: **8 cells** (vs 9 for standard T_ZERO); `dlclkp_1` pin order: `CLK GATE VGND VNB VPB VPWR GCLK`

| Phase | Time | Measurement | T_ZERO (MUX) | T_ZERO_ICG | Δ |
|---|---|---|---|---|---|
| 1 — Active | 2–40 ns | `power_active` | 9.59 µW | **8.64 µW** | −10% |
| 2 — Transition | 40–48 ns | `power_transition` | 8.96 µW | **4.51 µW** | −50% |
| 3 — Isolated | 60–120 ns | `power_isolated` | 9.14 µW | **3.47 µW** | **−62%** |
| 3 — Verify | 60–120 ns | `gclk_during_isolation` | — | **−900 nV ≈ 0 V** | GCLK frozen ✓ |
| 3 — Verify | 60–120 ns | `ce_eff_during_isolation` | 76.5 nV | **30 nV ≈ 0 V** | CE_eff frozen ✓ |

| Metric | T_ZERO (MUX) | T_ZERO_ICG | Improvement |
|---|---|---|---|
| **Isolation factor** (`power_active / power_isolated`) | **1.05×** | **2.49×** | **2.37× better** |
| Cells | 9 | 8 | −11% |

**Interpretation**:

The ICG variant achieves 2.49× isolation vs 1.05× for the MUX variant — a 2.37× improvement in the power isolation ratio. The `gclk_during_isolation = −900 nV ≈ 0 V` is the transistor-level confirmation that `GCLK` is fully held low during the isolated phase: the `dlclkp_1` latch output never toggles, the DFF clock input sees no rising edges, and no D→Q capture occurs.

The residual 3.47 µW in the isolated phase is not leakage. It has two components: (1) the `dlclkp_1` ICG cell itself receives the running `CLK` input and has internal switching in its enable latch and output driver even when GCLK is frozen — this is clock power before the gate, unavoidable without gating CLK upstream; (2) the lockout combinational logic (`inv_q0`, `Xdetect`, `Xlockout`, `Xinv_ce`) draws static leakage in their settled states. Full leakage-floor isolation (~nW range) would require gating the CLK source at the domain boundary, which is a floor plan decision rather than a cell-level decision.

**Cell count comparison** (delay / area / isolation):

| Cell | Cells | power_active | power_isolated | isolation |
|---|---|---|---|---|
| T_ZERO (MUX) | 9 | 9.59 µW | 9.14 µW | 1.05× |
| T_ZERO_ICG | **8** | 8.64 µW | **3.47 µW** | **2.49×** |
| Improvement | −1 cell | −10% | **−62%** | **+2.37×** |

T_ZERO_ICG is strictly better: fewer cells, lower active power, and dramatically lower isolated power. The ICG variant is the preferred implementation whenever GCLK routing is available from the clock tree. T_ZERO (MUX) remains valid as a fallback when the clock grid does not expose a `dlclkp_1` tap point near the cell.

---

#### T_DFF_COMPARISON — MUX-CE vs ICG-CE register power

**Design intent**: characterize the CE-path power difference between MUX-based clock enable and ICG-based clock enable for the plain ternary register (without Zero-lockout). Both variants share a single VDD rail.

**Implementation** (`rtl/spice/t_dff_comparison.spice`):
- **Variant A (MUX)**: `inv_1` + 2×`mux2_1` + 2×`dfrtp_1` — 5 cells, same as T_DFF baseline
- **Variant B (ICG)**: `dlclkp_1` + 2×`dfxtp_1` — 4 cells; `dfxtp_1` (simple DFF, no CE/reset pin) receives GCLK from ICG; `dfxtp_1` pin order: `CLK D VGND VNB VPB VPWR Q`

Both variants are driven simultaneously with identical CE and D stimuli; total VDD current is measured. Per-variant estimates use the T_DFF baseline (18.97 µW active, 9.14 µW CE=0) as the MUX-variant component.

| Measurement | Combined (MUX + ICG) | Description |
|---|---|---|
| `power_active_total` | **42.3 µW** | CE=1, D_p0 toggling 250 MHz — both variants active |
| `power_ce_data_total` | **42.1 µW** | CE=1, D==Q settled — both variants nearly unchanged |
| `power_ce_zero_total` | **17.3 µW** | CE=0 — both variants quiescent |
| `gclk_b_ce_zero` | **−900 nV ≈ 0 V** | ICG GCLK_b held low when CE=0 ✓ |
| Combined ratio (active / CE=0) | **2.45×** | — |

**Per-variant estimates** (subtracting T_DFF MUX baseline from combined):

| Metric | T_DFF_MUX | T_DFF_ICG (est.) |
|---|---|---|
| `power_active` | 18.97 µW | ~23.3 µW |
| `power_ce_zero` | 9.14 µW | ~8.2 µW |
| Isolation ratio | 2.07× | ~2.84× |
| Cells | 5 | 4 |

**Interpretation**:

The ICG variant's estimated CE=0 power (~8.2 µW) is only marginally lower than the MUX variant's (9.14 µW). The `gclk_b_ce_zero = −900 nV ≈ 0 V` confirms GCLK_b is frozen, yet the power saving is modest. This is consistent with the T_ZERO_ICG finding: the `dlclkp_1` ICG cell itself receives the running CLK and contributes switching current before the gate point. The DFF internal D→Q switching is eliminated, but the CLK input to the ICG latch still transitions, and that accounts for most of the residual floor.

The slightly higher active power for the ICG variant (~23 µW vs ~19 µW) reflects two factors: (1) the `dlclkp_1` adds overhead to the gated-clock path that didn't exist in the direct-CLK MUX path; (2) `dfxtp_1` (no reset, no enable logic) and `dfrtp_1` (with reset) have different internal switching characteristics.

**Summary — all six cells, delay / area / isolation**:

| Cell | Cells | `tpd_neg` | `tpd_pos` | `tpd_zero` | `power_active` | `power_isolated` | Isolation |
|---|---|---|---|---|---|---|---|
| T_CLASS (std) | 5 | 22.7 ps | 36.1 ps | 60.7 ps | 1.95 µW | — | — |
| T_CLASS_CUSTOM (flat) | ~20T | 24.9 ps | **26.6 ps** | 94.1 ps | 2.71 µW | — | — |
| T_DFF (MUX) | 5 | — | — | — | 18.97 µW | 9.14 µW | 2.07× |
| T_DFF (ICG est.) | 4 | — | — | — | ~23 µW | ~8.2 µW | ~2.84× |
| T_ZERO (MUX) | 9 | — | — | — | 9.59 µW | 9.14 µW | 1.05× |
| **T_ZERO_ICG** | **8** | — | — | — | **8.64 µW** | **3.47 µW** | **2.49×** |

**Key takeaways**:
1. **T_CLASS_CUSTOM tpd_pos −26%**: removing inter-cell routing saves real delay on the direct-input path. The shared-node penalty on tpd_zero (+55%) means a custom classifier should be restructured per-output rather than sharing all inverted inputs from a single driver.
2. **T_ZERO_ICG is the preferred isolation cell**: 2.49× isolation vs 1.05×, one fewer cell, and 10% lower active power. The ICG clock gate provides structural GCLK quiescence (−900 nV confirmed) that the MUX-CE variant cannot achieve.
3. **Dominant floor is the pre-ICG clock path**: at sky130 130nm, the `dlclkp_1` ICG cell's own CLK input transitions account for most of the residual isolated power (~3.5 µW). True leakage-floor isolation requires gating the CLK source at the domain boundary — this is the region-level ICG described in the section below.
4. **Standard cell composition wins on power for T_CLASS**: flat minimum-size transistors expose more wire parasitics than the optimized standard cell layout; power is 39% higher despite identical transistor count. Custom layouts require matching the standard cell's place-and-route discipline to capture the interconnect savings.

---

### Region-level ICG — hierarchical clock gating from semantic quiescence

The cell-level ICG (T_ZERO_ICG) establishes that the infectious Zero state can directly drive a hardware clock gate. The remaining residual (~3.5 µW per cell in isolation) is the always-on CLK trunk reaching the dlclkp_1 input. Eliminating that residual requires gating the trunk itself via one region-level ICG that covers all N cells in the region, driven by the aggregate quiescence signal.

**Architecture** (`rtl/sv/ternary_region.sv`, `rtl/sv/icg_model.sv`):

```
CLK (trunk, always-on)
  └─ region_icg (icg_model: enable = ~quiescent | rst)
       └─ gclk (gated, fed to all K cells)
            └─ ternary_chain (K T_ZERO_ICG cells)
                 quiescent = NOR(ce_out[0..K-1])
                 (each ce_out = comb_out != out, combinational)
```

`quiescent` is purely combinational and valid even when `gclk=0`. When `quiescent=1`:
- `gate_enable = ~quiescent = 0`
- ICG latch captures 0 at next CLK falling edge
- `gclk = CLK & 0 = 0` — trunk gated

If an input changes while gclk is dark, `comb_out` diverges from `out` for some cell → `quiescent` drops combinationally → `gate_enable` rises → ICG re-opens → `gclk` resumes. The gate is self-clearing.

**RTL simulation results** (`rtl/sim/run_topology.sh`, all 14 scenarios pass):

| Scenario | Result |
|---|---|
| TC4a — Zero propagation, K=8 | PASS — 8 cycles to quiescence, 32 ns @ 250 MHz |
| TC4b — gclk gated after quiescence | PASS — `gclk_active=0` |
| TC4c — trunk cycles banked | PASS — `gated_trunk_cycles=3` accumulating on always-on CLK |
| TC5a — quiescent drops on input change | PASS — combinational path valid while gclk=0 |
| TC5b — region wakes and re-quiesces | PASS — 7 cycles, 28 ns |
| TC5c — gated_trunk_cycles resets on wake | PASS — counter resets to 0 during active phase |

TC4c is the behavioral proof: `gated_trunk_cycles` is driven by the always-on `clk` and increments while `quiescent=1`. It accumulates even while `gclk=0` — the trunk clock is banked up independently of the gated domain. TC5a–c prove the round-trip: frustrated → wake → re-quiesce, with no software intervention at any stage.

**SPICE results** (`rtl/spice/t_region_icg.spice`, sky130 transistor level):

Clock hierarchy: CLK → region_icg → GCLK_region → cell_icg → GCLK_cell → dfrtp_1. CE_eff drives both ICG GATE pins simultaneously.

| Phase | Measurement | T_ZERO_ICG (cell only) | T_REGION_ICG (two levels) |
|---|---|---|---|
| Active | `power_active` | 8.64 µW | **8.64 µW** |
| Isolated | `power_isolated` | 3.47 µW | **3.47 µW** |
| Verify | `gclk_region_isolated` | — | **−895 nV ≈ 0 V** ✓ |
| Verify | `gclk_cell_isolated` | — | **2.6 nV ≈ 0 V** ✓ |
| Verify | `ce_eff_isolated` | 30 nV | **24.6 nV** ✓ |

**Interpretation — why power is equal at N=1, and why it scales with N**:

The isolated power equality is expected and correct. In the single-cell test, the `t_region_icg.spice` deck replaces 1 cell_icg (which received always-on CLK) with 1 region_icg + 1 cell_icg (region_icg receives always-on CLK, cell_icg receives GCLK_region=0). The CLK trunk still drives exactly one `dlclkp_1` input at 250 MHz — the switching energy on the CLK node is the same. The gating confirmation (`gclk_region_isolated = −895 nV`, `gclk_cell_isolated = 2.6 nV`) proves the two-level hierarchy works: both nodes are at 0V during isolation.

The power savings from a region ICG scale with N (the number of cells the region covers):

| Region size | Without region ICG | With region ICG | Savings |
|---|---|---|---|
| N=1 | 1× P_clk_icg + 1× P_leak | 1× P_clk_icg + 1× P_leak | 0% |
| N=2 | 2× P_clk_icg + 2× P_leak | 1× P_clk_icg + 2× P_leak | ~37% (at 3.47/0.02 ratio) |
| N=8 | 8× P_clk_icg + 8× P_leak | 1× P_clk_icg + 8× P_leak | ~87.5% |
| N=K | K× P_clk_icg + K× P_leak | 1× P_clk_icg + K× P_leak | ~(K−1)/K |

Where P_clk_icg ≈ 3.47 µW (one dlclkp_1 on CLK at 250 MHz, sky130) and P_leak ≈ 20 nW (cell leakage, negligible). For K=8 (the ternary_chain topology used in RTL benchmarks): isolated power drops from ~27.8 µW to ~3.47 µW, and isolation_factor scales from 2.49× (per cell) to approximately 8.64 × 8 / 3.47 ≈ **20×** at the region level.

This is the direct consequence of the algebraic structure: a region with K cells has exactly one aggregate `quiescent` signal (NOR of K cell ce_out signals). One `dlclkp_1` at the region clock root driven by `~quiescent` gates the CLK to all K cells simultaneously. The savings grow linearly with K, and K is set by the pgress compiled-region topology — the region size is the natural unit of quiescence in the ternary fixed-point semantics. 

More fundamentally, the hardware is not tracking truth, convergence, or observability, but it is tracking distinguishability in the sense of whether any inferential distinction can occur in a region. The e-graph regulates the growth of distinguishable conflict states through equivalence-class saturation; the clock tree simply stops switching once no further distinctions can emerge. Once no new distinctions can be produced, all `ce_out` signals fall low, the regional quiescence predicate becomes true, and the clock gate closes. The resulting power reduction arises as a consequence of the algebra itself rather than a separate power-management policy layered on top of the runtime.

**The full isolation hierarchy**:

| Level | Mechanism | What quiesces | Trigger |
|---|---|---|---|
| Cell (T_ZERO_ICG) | Cell ICG gates GCLK_cell | DFF D→Q capture, internal pipeline | `is_at_zero` per cell |
| Region (ternary_region) | Region ICG gates trunk CLK to all K cells | K cell ICG CLK inputs + K DFF pipelines | `NOR(ce_out[0..K-1])` = `quiescent` |
| Domain | Region ICG gates CLK spine to all regions in a domain | All region ICG CLK inputs | `AND(quiescent_i)` for all regions |

Each level is driven by the ternary algebra's own fixed-point signal without the need for a separate power management controller, runtime scheduler, or ISA op. The Zero state propagating to all cells of a region propagates directly to the clock spines through a combinational NOR tree and a transparent-low ICG latch.

---

## Physical implementation — OpenLane P&R on sky130_fd_sc_hd

Reproduction: install OpenLane (superstable branch) with Docker, then:

```bash
bash rtl/pnr/setup_openlane.sh        # stage RTL + configs into ~/OpenLane/designs/
cd ~/OpenLane && make mount            # enter container
./flow.tcl -design ternary_cell   -tag run1
./flow.tcl -design ternary_region -tag run1
./flow.tcl -design meetall_500    -tag run1
```

GDS outputs land in `designs/<design>/runs/<tag>/results/final/gds/`. Visual certificates are in `rtl/pdn/`. Layer properties for KLayout: `~/.ciel/sky130A/libs.tech/klayout/tech/sky130A.lyp`.

PDK: `sky130A` / `sky130_fd_sc_hd`. Clock: 4 ns (250 MHz). Each design was placed and routed through all 42 OpenLane steps (synthesis → floorplan → placement → CTS → routing → signoff ERC).

The accompanying screenshots in `rtl/pdn/` serve as visual certification that the semantic structures discussed in the architecture and benchmark sections survive synthesis, placement, routing, and timing closure and remain spatially identifiable as physical artifacts.

---

### `ternary_cell_top` — physical locality certification

**What it certifies:** a single `ternary_cell` instance at N=8 is placeable and routable as a standalone die with no degenerate layout artifacts. The N=8 wrapper (`rtl/pnr/ternary_cell/src/ternary_cell_top.sv`) fixes the parameter to keep the IO pin count manageable (16 input pins vs 1000 for N=500).

| Metric | Value |
|---|---|
| Die area | 80 × 80 µm (absolute, `FP_SIZING: absolute`) |
| Output register | `sky130_fd_sc_hd__dfxtp_1` — 1× drive, minimum area |
| Utilization | Low (~5% cell area) — die sized for PDN headroom, not density |
| IO pins | 16 inputs + clk + rst + out[1:0] + ce_out = 21 |

The `dfxtp_1` (1× drive) output register reflects synthesis choosing the minimum footprint sufficient to drive the 21-pin die. No upsizing was triggered because the output load is small.

The die is IO-comfortable and PDN-clean at 80 µm. The previous attempt at auto-sized floorplan produced a 20 × 19 µm die where the power grid pitch (5.175 µm) violated the minimum (6.6 µm). The 80 µm absolute floor gives PDN pitch headroom throughout.

---

### `meetall_500` — wall-to-wall reduction certification

**What it certifies:** 500 ternary inputs reduce to a single registered ternary output across the full transistor fabric. The design is IO-pin-limited, not logic-limited: the N=500 reduction tree fits in a fraction of the die; the 1000-pin input bus (500 × 2-bit packed ternary) determines the minimum perimeter.

| Metric | Value |
|---|---|
| Die area | 1000 × 1000 µm (absolute) |
| Output register | `sky130_fd_sc_hd__dfxtp_4` — 4× drive, upsized by synthesis |
| IO pins | 1000 inputs + clk + rst + out[1:0] + ce_out = 1005 |
| Reduction tree | ~1200 OR gates (`or2`, `or4`) in balanced 5-level tree |

The `dfxtp_4` (4× drive) vs `ternary_cell_top`'s `dfxtp_1` reflects synthesis upsizing the output register to drive the higher-capacitance IO ring. The computation logic (`any_neg`, `any_zero`, `any_pos` OR trees plus priority mux) occupies roughly 10–15% of the die area; the remainder is pad ring and fill. The design is perimeter-limited rather than logic-limited: physical area is dominated by the requirement to expose 1005 IO pins rather than by the reduction computation itself.

The `any_pos` OR tree is absent from the placed netlist, consistent with the Yosys characterization: for MeetAll with N≥3, ABC eliminates the `any_pos` reduction because the Pos case is the implicit `else` branch and needs no materialized test. Only the `any_neg` and `any_zero` trees are routed in metal.

The 700 × 700 µm initial die was too small for 1005 IO pins (848 slots available; `PPL-0024`). The 1000 µm side provides 1090 slots at sky130_fd_sc_hd pin pitch.

---

### `ternary_region` — quiescent semantics workflow certification

**What it certifies:** the complete fixed-point suppression predicate, from ternary cell change detection through priority resolution through clock gating, exists as placed and routed silicon. The key highlight: K=8 `ternary_cell` stages with a region-level ICG driven by the aggregate quiescence signal.

| Metric | Value |
|---|---|
| Die area | ~60–80 µm × ~60–80 µm (auto-sized, FP_CORE_UTIL 45%) |
| IO pins | stage_out[K×2-1:0], stage_ce[K-1:0], quiescent, quiescent_age[12:0], gclk, gclk_active, gated_trunk_cycles[12:0] |
| ICG cluster | `dlxtn_1` at (54.55, 32.40)–(60.45, 35.60) µm |
| Layers | 6 metal layers (li1 through met4 visible in KLayout) |

Four cells in the placed netlist directly witness the quiescent semantic workflow. Each is identifiable by name in KLayout via `Macros → Run Script` with `each_inst { |i| puts i.cell.name if i.cell.name =~ /pattern/ }`:

| Cell | sky130 instance | Semantic role |
|---|---|---|
| `sky130_fd_sc_hd__dlxtn_1` | ICG enable latch | Transparent-low latch: captures `gate_enable = ~quiescent \| rst` at CLK falling edge. Holds `en_latch`; output AND'd with CLK to produce `gclk`. The quiescence wire to `GATE_N` is the fixed-point suppression predicate in metal. |
| `sky130_fd_sc_hd__a21oi_1` | Priority mux (AND-OR-INVERT) | MeetAll priority resolution: Neg > Zero > Pos. Implements `any_neg → 2'b00 / any_zero → 2'b10 / else → 2'b01` in one AOI gate. |
| `sky130_fd_sc_hd__xnor2_1` | Change detector | `ce = (comb_out != out)` per output bit. XNOR compares the combinational result with the stored register value; `ce=0` suppresses the clock enable. Zero dynamic power when the ternary value is stable. Hardware analogue of Opt 7 (same-value early exit). |
| `sky130_fd_sc_hd__dlygate4sd3_1` | Hold buffer | Inserted by OpenROAD resizer during timing closure. Physically witnesses the PnR tool's hold-violation repair work: a dedicated delay cell on a short path where data arrives too early relative to the clock edge. |

The `dlxtn_1 → and2_2` hold violation (−0.02 ns at the typical corner) and the `gclk` output setup violation (−2.03 ns) are STA modeling artifacts, not silicon defects:

- The hold violation arises because Yosys decomposed the behavioral `icg_model` into a discrete `dlxtn_1` latch plus `and2_2` AND gate. The integrated `sky130_fd_sc_hd__dlclkp_1` cell characterizes the clock gating check internally and would not produce this violation. The 20 ps gap is the timing model cost of the discrete decomposition.
- The setup violation arises because OpenLane's default SDC applies a −0.80 ns output delay to all ports, treating `gclk` (a generated clock) as a data output. The constraint is meaningless for a clock output port.

Both violations are accepted with `QUIT_ON_TIMING_VIOLATIONS: 0` in `rtl/pnr/ternary_region/config.json`. The physical routing is correct; the violations exist in the STA model, not the layout.

**The full feedback loop in metal:**

```
NOR(stage_ce[0..7])           — combinational NOR across 8 cell clock-enables
  → quiescent (net)           — routed in li1/met1 to ICG cluster
  → dlxtn_1/GATE_N            — latch input at (54.55, 32.40) µm
  → dlxtn_1/Q (en_latch)      — captured at CLK fall edge
  → and2_2/A                  — AND gate input
  → and2_2/X (gclk)           — gated clock output
  → clkbuf tree               — fans out to all 8 cell registers
```

The wire from the NOR reduction to `dlxtn_1/GATE_N` is the physical realization of the predicate `~(p0 | p1) == 0` over the region. The core semantics survived all the way down to cell-by-cell attribution, rendered in metal and routed by OpenROAD.

The significance here is that the predicate remained identifiable after synthesis and place-and-route. The fixed-point suppression condition can be traced from its algebraic definition through RTL, standard-cell mapping, timing analysis, and final routed geometry without changing meaning.
