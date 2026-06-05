# pgress Architecture

> **Audience:** engineers implementing a new runtime, building a federation layer, debugging propagation semantics, or mapping the software model to RTL. For first-time evaluation, read the [README](../README.md) first.

---

## Layer overview

```
┌──────────────────────────────────────────────────────────┐
│  Python SDK  (py/)          PyO3/maturin binding          │
├──────────────────────────────────────────────────────────┤
│  Session manager  (session-rs)                            │
│  wire → path resolution → session gate → shard dispatch  │
├──────────────────────────────────────────────────────────┤
│  Computation substrate  (core-rs)                         │
│  ISA dispatch · propagation · e-graph · dep registry      │
├──────────────────────────────────────────────────────────┤
│  RTL  (ternary_region.sv)                                 │
│  cell encoding · ICG enable · trunk clock gating          │
└──────────────────────────────────────────────────────────┘
```

Each layer is independently testable. The session manager never imports engine internals; the engine never sees `path_id`, `session_id`, or wire bytes.

---

## Computation substrate: `core-rs`

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

The separation is enforced structurally: operations on the id layer (`NodeCreate`, `EdgeConnect`, `PartitionBind`) never touch the interpretation layer, and vice versa. This means identity-layer stability is checkable by proptest without coordinating with propagation state.

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

`CapabilityBits(u64)` is an unforgeable bitfield of permitted operations on a dep edge (`READ`, `PROPAGATE`, `STABILIZE`). The session-layer equivalent is `AuthorityPolicy` — four `u64` bitmasks enforced at the session manager gate before a record reaches the engine (see [Trust boundary and authority model](#trust-boundary-and-authority-model) below).

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

## Quiescent state model and hardware clock gating

pgress defines a three-level quiescence hierarchy that couples the semantic stabilization criterion directly to the hardware clock-activity criterion. This is non-trivial: in most compute systems, power-saving clock gating is driven by a separate activity detector (e.g. "idle for N cycles") that is logically independent of the computation's semantic state. In pgress they are the same predicate evaluated at different levels of the physical hierarchy.

### The three levels

```
Cell      is_at_zero:     Q_p1 & ~Q_p0              ← cell is in ternary Zero state
Region    quiescent:      NOR(ce_out[0..K-1])        ← all cells have no pending clock-enables
Domain    quiescent:      propagation queue empty    ← partition has no pending work items
```

A **cell** is at its fixed point when its ternary value is `Zero` — `Q_p1 = 1, Q_p0 = 0` in the `{p1,p0}` two-bit encoding. The cell's clock-enable output `ce_out[i]` is low exactly when the cell is at its fixed point and no downstream push is pending.

A **region** is quiescent when `NOR(ce_out[0..K-1]) = 1` — that is, every cell in the region has its clock-enable output suppressed. The region-level ICG gates the trunk clock dark at this point: `gclk = trunk_clk AND (NOT quiescent)`.

The **domain** (partition) reaches quiescence when the propagation engine's work queue is empty. This is the condition `DomainQuiescent` tracks via the telemetry partition.

### RTL implementation: `ternary_region.sv`

> Physical P&R results including die dimensions, placed cell identities, ICG cluster coordinates, and STA closure notes are all in [benchmarks.md § Physical implementation](benchmarks.md#physical-implementation--openlane-pr-on-sky130_fd_sc_hd). Visual certificates are in `rtl/pdn/`.

The two-bit `{p1, p0}` cell encoding maps directly onto standard-cell synthesis:

| `{p1, p0}` | Value | Semantic |
|---|---|---|
| `{0, 0}` | `Neg` | Pending / absent — cell has not yet resolved |
| `{1, 0}` | `Zero` | Conflict fixed point — self-negating, ICG suppressed |
| `{1, 1}` | `Pos` | Present / active — downstream pushes may fire |

`ce_out[i]` for cell `i` is derived combinatorially from the propagation queue state:

```verilog
assign ce_out[i] = ~(is_at_zero[i]) & ~queue_empty[i];
// is_at_zero[i]  = Q_p1[i] & ~Q_p0[i]
// queue_empty[i] = no pending downstream push entries for cell i
```

The region ICG enable is the NOR reduction across all cells:

```verilog
assign region_quiescent = ~(|ce_out);            // NOR
assign gclk             = trunk_clk & ~region_quiescent;  // ICG
```

`gated_trunk_cycles` is the hardware counter that increments each cycle `region_quiescent` holds — the RTL analog of `quiescent_epochs` in the session manager. When a `SetValue` or `Propagate` write reaches any cell in the region, `ce_out` for that cell goes high, `region_quiescent` falls, and the ICG ungates the trunk clock within one cycle.

The `Zero` state is architecturally privileged: it is the only value that suppresses `ce_out` regardless of queue depth. A `Zero` cell can have pending downstream pushes and still hold `ce_out` low, because `Zero` infection propagates in zero cycles — the downstream cells receive `Zero` by the time they would evaluate, so no computation needs to be scheduled.

### Why the coupling is non-trivial

The stabilization criterion (e-graph saturation has reached its fixed-point quotient, no further equivalence classes can be merged) and the hardware activity criterion (ICG enable goes low, trunk clock dark) are derived control signals at different abstraction levels directly from the semantics of the algebra. The RTL certifies this by construction: `ce_out[i]` is driven by the propagation queue state of cell `i`, which is empty exactly when cell `i` is at its ternary fixed point, which is exactly when e-graph saturation has nothing left to do for that cell.

Consequence: if a region is semantically quiescent, the hardware is guaranteed dark. If the hardware is dark, the computation is guaranteed complete. No external arbiter, no polling, no "N idle cycles" heuristic. The physical activity signal conditioning of the clock is derived directly from the interpretive stabilization encoded in the semantics of the algebra.

### Session manager wiring

The `Stabilize` ISA op triggers `run_stabilize` synchronously in the engine. When `apply` returns, the region is at its fixed point. The session manager (`Dispatcher::handle_admitted`) emits `TelemetryEvent::RegionQuiescent` at that moment — recording both the event and the running `quiescent_epochs` counter (the software analog of `gated_trunk_cycles`). A subsequent `SetValue` or `Propagate` op resets the counter (clock re-enables, region wakes).

```
Stabilize op applied
  ↓ engine.run_stabilize() — synchronous; returns at e-graph fixed point
  ↓ RegionQuiescent { region_root, partition_id, quiescent: true, quiescent_epochs: n }
  ↓ emitted to TelemetryPartition (PARTITION_TELEMETRY = 0xFFFF_FFFF_FFFF_FFFE)

SetValue / Propagate op applied
  ↓ shards.record_region_wake(partition_id) — resets epoch counter
  (region clock re-enables; next Stabilize restarts from epoch 1)
```

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

`PathTable` maps `path_id → session_id` + last ack state. `SessionTable` tracks `active_path_id` and `prev_path_id` (draining). `stream_seq_floor` provides the replay suppression floor that survives path migration.

### Execution geometry: topology as a partition

The topology partition (`PARTITION_TOPOLOGY = 0xFFFF_FFFF_FFFF_FFFD`) materialises the network diameter as a pgress graph using existing primitives — no separate routing protocol, no separate topology manager.

`ShardPressure` events are dual-written as ternary health state on shard nodes: `Pos` = accept new work, `Zero` = congested (prefer others), `Neg` = hot or unknown (do not route here). Placement decisions are resolved by reading the converged ternary — the propagation through the topology graph has already computed the routing. No BFS, no routing table update protocol, no separate placement service.

`pick_shard_for(session_id)` in `Dispatcher` follows: locality hint from the session's existing partitions → `best_shard_for` → `any_healthy_shard` (lowest-id `HEALTH_POS` shard) → `ShardId(0)` bootstrap fallback.

### Trust boundary and authority model

`session-rs` enforces a **two-lattice MAC model** that separates integrity (who can create or transfer distinctions) from confidentiality (who can witness or reveal them). The single-axis `CapabilityBits` model conflates these; the four-dimensional `AuthorityPolicy` makes the separation explicit and enforceable at every gate point.

#### The four axes

| Axis | Question answered | Opcodes gated |
|---|---|---|
| `assertion` | Can mutate graph state in this partition? | `Control`, `SetValue`, `Propagate`, `Stabilize`, `Data` |
| `delegation` | Can transfer authority to a child session or profile? | `Profile` (opcode `0x0100`) |
| `observability` | Can subscribe to state and demand lazy evaluation? | `Subscribe`, `Demand` |
| `disclosure` | Can reveal distinctions across partition or session boundaries? | `EdgeConnect(Remote)`, `SetEdgeLabel` with non-zero `causal_scope_bits` |

**Integrity axis** (`assertion` + `delegation`) corresponds to a Biba-style "no write up" discipline — you cannot assert into a higher-integrity region, and you cannot delegate bits you do not hold.

**Confidentiality axis** (`observability` + `disclosure`) corresponds to a BLP-style "no read up" discipline — you can only witness and disclose at your permitted coarseness of the e-graph quotient. Coarser observability = fewer equivalence classes are distinguishable.

The two axes are orthogonal. A session may hold full `assertion` and zero `disclosure` (can mutate local state but cannot install cross-partition edges), or zero `assertion` and full `observability` (read-only subscriber). `AuthorityPolicy::READ_ONLY` is the canonical constant for the latter case.

#### Capability inheritance

Authority is strictly bounded downward through the domain hierarchy. A child domain can never hold bits its parent does not hold on any axis:

```
tenant.policy
  ⊇ session.effective_policy   (= claimed.intersect(tenant.policy))
    ⊇ partition.row.required   (NONE = permissive, ALL = maximally restrictive)
      ⊇ edge.label.capability  (CompiledEdgeLabel in DepMeta — engine layer)
```

`effective_policy` is computed on every gate evaluation: `session.claimed_policy.intersect(tenant.policy)`. A `SessionProfile` that claims `ALL` on a tenant whose `policy.delegation = 0` produces `effective_policy.delegation = 0` — the claim is silently clipped, not rejected.

#### Three trust levels

| Level | Mechanism | When to use |
|---|---|---|
| **Advisory** | No signature | Single-tenant / intra-cluster / transport-secured |
| **Asserted** | Ed25519 signed by `issuer_domain` key in `TrustStore` | Cross-tenant federation |
| **Attested** | Signed + per-record stream MAC under derived session key | Hostile transit / compliance audit (not yet implemented) |

`Advisory` is zero-crypto: claimed policy is intersected with the parent ceiling and installed immediately.

`Asserted` proves the authority claim is backed by a key the host explicitly registered. The issuer signs a 64-byte canonical payload; the `session_id` binding prevents a valid profile from session A being replayed onto session B:

```
bytes  0– 7   assertion_mask
bytes  8–15   delegation_mask
bytes 16–23   observability_mask
bytes 24–31   disclosure_mask
bytes 32–39   issuer_domain
bytes 40–47   session_id        ← splice-attack prevention
bytes 48–55   generation
bytes 56–63   expiry_epoch
```

#### SessionProfile wire format

Opcode `0x0100` (first standard extension). Appears after `IsaStreamHeader`, before any data ops. 73-byte fixed prefix + `sig_len` signature bytes:

```
profile_id:u32  profile_version:u16  trust_level:u8
assertion_mask:u64  delegation_mask:u64  observability_mask:u64  disclosure_mask:u64
issuer_domain:u64  session_id:u64  generation:u64  expiry_epoch:u64
sig_len:u16  signature:u8[sig_len]
```

#### Enforcement points

Two distinct gates enforce the model at different pipeline stages, so body-parsing overhead is never incurred for header-level rejections:

**1. Header-only gate** (`PartitionAuthTable` — no body parsing):

After session lookup, tenant check, and stream_seq replay suppression, `auth.gate` checks the effective session policy against the installed `PartitionAuthRow` for `(partition_id, opcode_class)`. The per-row check is two bitwise operations:

```
(session_eff.for_opcode_class(oc) & row.required.for_opcode_class(oc))
    == row.required.for_opcode_class(oc)
```

`PartitionAuthRow::permissive()` (`required = NONE`) is installed automatically on `PartitionCreate`; tighter rows are installed by the host after creation. Rejection here returns `RejectionReason::AuthGateDenied` and the record never reaches `parse_payload`.

**2. Body-parsed gate** (disclosure check — after `parse_payload`, before `engine.apply`):

Cross-partition authority requires body inspection because `causal_scope_bits` lives in the record payload, not the header. The check runs in `Dispatcher::handle_admitted`:

```
session.effective_policy.disclosure & edge.causal_scope_bits == edge.causal_scope_bits
```

Applied to `EdgeConnect(Remote)` and `SetEdgeLabel` with non-zero `causal_scope_bits`. Failure returns `DispatchError::DisclosureViolation { required, held }` before any engine mutation occurs. Sessions absent from `DomainRegistry` (local bootstrap) fall back to `disclosure = u64::MAX` (permissive).

#### Configuring the TrustStore

`SessionRuntime` holds `trust_store: TrustStore` (`FxHashMap<TenantId, ed25519_dalek::VerifyingKey>`). Pre-register an issuer key for each tenant that may present `Asserted` profiles:

```rust
use ed25519_dalek::VerifyingKey;
runtime.trust_store.insert(TenantId(99), verifying_key);
```

An `Asserted` profile whose `issuer_domain` is absent from the store returns `ProfileError::UnknownIssuer`. `Advisory` profiles do not consult the store. There is no default trust — the store starts empty.

#### Revocation

`generation` is a monotone counter maintained by the issuer. Revocation is O(1) — no certificate chain, no network lookup, no revocation list. The causal `expiry_epoch` field provides time-bounded validity without requiring wall-clock synchronization.

---

## Distributed consistency model

pgress is **AP** — available and partition-tolerant, not linearizable. Understanding its consistency properties requires situating the monotonicity claim correctly.

### What is and isn't monotone

pgress is **not** CALM-monotone over values or event history. The ternary value sequence `Neg → Pos → Zero` is valid and frequent; no monotone order over `{Neg, Zero, Pos}` is preserved under propagation. Attempting to treat pgress as a monotone-lattice system (e.g. CRDTs, Bloom) will produce incorrect predictions about convergence.

The monotonicity is one level higher: **over the filtered interpretation order**. Each observable region induces a projection and a distinguishability rank over the observables accessible at that scope. This defines a quotient of the e-graph: two nodes are equivalent if no observation in the region can distinguish them. The set of such quotients, partially ordered by coarseness, is the object that is monotone. Stabilization pushes the region toward a coarser quotient such that equivalence classes can merge, but never split. The epoch counter on `RegionQuiescent` tracks movement through this order.

### Global non-confluence

There is no canonical global rewrite sequence. Two disjoint regions can reach different locally stable quotients that are not reachable from a common fine-grained starting point, even starting from the same initial graph. This is not a bug: it reflects genuine observational non-equivalence between regions with different projection or sensitivity parameters.

For purely monotone subgraphs (no Reflection mode, no `Zero`-generating conflicts), the system degenerates to CALM-equivalent behavior. The e-graph saturation has no non-trivial equivalence classes to merge, and value propagation alone determines the fixed point.

### Partition semantics

Under network partition, a shard that cannot receive gossip from a peer continues to route normally within its observable window. The peer's topology node degrades: `Pos → Zero → Neg` as gossip ages. Placement decisions reflect this in ternary: `Zero` = prefer others, `Neg` = avoid. No shard halts, no error is returned to the client. This is the **available** property.

When the partition heals, gossip converges and the peer's topology node recovers to `Pos`. The topology partition's propagation graph computes the healed state exactly once. This is the **partition-tolerant** property.

The critical safety invariant: a shard that lost contact with peers never emits a false `Pos` on topology nodes it cannot observe. Unknown state is `Neg` (pending/absent), not `Pos`. The only way a topology node reads `Pos` is via confirmed gossip from the source shard.

### Distributed convergence criterion

Distributed convergence in pgress arises from the e-graph saturation fixed point, not from a consensus round. The condition for convergence of a region spanning multiple shards is:

1. All shards holding nodes in the region have processed all in-flight ops on those nodes (domain quiescence per shard).
2. No cross-shard gossip messages are pending delivery that would update a topology node feeding a placement-sensitive routing decision.

When both hold, the region's observable quotient is stable — no further equivalence-class merges can occur without a new external `SetValue`. This is the semantic analogue of the RTL condition `NOR(ce_out[0..K-1]) = 1` across all participating shards.
