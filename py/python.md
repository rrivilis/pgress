# pygress — Python SDK

pygress is the Python binding for the pgress reactive computation substrate. It exposes the full ternary dependency graph through a simple `Graph` API built on PyO3/maturin over `core-rs`.

---

## Installation

### Build from source (recommended)

Requires Rust (stable) and Python ≥ 3.9.

```bash
cd pgress/py

# Option A: with an active virtualenv or conda env
maturin develop --release

# Option B: build a wheel and install it
python -m maturin build --release
pip install --force-reinstall target/wheels/pygress-*.whl
```

The wheel is `abi3-py39` — compatible with Python 3.9 and later without rebuilding.

### Verify

```python
import pygress
g = pygress.Graph()
print(g)  # pygress.Graph(nodes=0, edges=0)
```

---

## Quick start

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
print(g.get(signal))   # True

g.set(volume, False)   # False is Bochvar-infectious — short-circuits AND immediately
print(g.get(signal))   # False
```

---

## Value model

pgress uses three values. In Python they map to:

| Python | pgress | Meaning |
|--------|--------|---------|
| `True` | `T::Pos` | Signal active / present / satisfied |
| `False` | `T::Zero` | Contested / infected — Bochvar short-circuit |
| `None` | `T::Neg` | Pending — not yet evaluated (default) |

`False` is not "falsy" in the conventional sense. It is the stable result of genuine structural disagreement or an explicit Bochvar infection. Once a `False` enters a graph, it propagates downstream through most compute rules until resolved — it cannot be quietly papered over.

`None` means no signal has arrived yet, or the node is lazy and has not been demanded. Most freshly created nodes start as `None`.

---

## Compute rules

Pass `rule=` when creating a computed node.

### `"and"` / `"all"` — MV-multiplication (MeetAll)

Evaluates to `True` when all inputs are `True`. `False` in any input infects the output (Bochvar). `None` in any input keeps the output pending.

```python
a = g.input("a"); b = g.input("b")
out = g.computed("out", rule="and")
g.connect(a, out); g.connect(b, out)

g.set(a, True);  g.set(b, True)   → out = True
g.set(b, False)                   → out = False  (Bochvar infection)
g.set(b, None)                    → out = False  (inhibition: Neg dep suppresses re-eval)
g.set(b, True)                    → out = True
```

### `"or"` / `"any"` — MV-addition (JoinAny)

Evaluates to `True` when all inputs are `True`. Evaluates to `False` if any input is `False` (Bochvar), same as `"and"`. `None` in any input keeps the output pending.

Note: `"or"` does **not** short-circuit on `True` the way Python's `or` does. Both inputs must arrive (`True` or `False`) before the output resolves to `True`. To get short-circuit OR behavior you need upstream demand management.

```python
g.set(a, True);  g.set(b, None)  → out = None  (b still pending)
g.set(b, True)                   → out = True
g.set(a, False)                  → out = False  (Bochvar)
```

### `"not"` / `"neg"` — MV-negation

L₃ negation: `True → None`, `None → True`, `False → False` (fixed point).

```python
na = g.computed("na", rule="not")
g.connect(a, na)

g.set(a, True)   → na = None   (negation of Pos is Neg)
g.set(a, False)  → na = False  (Zero is its own negation — fixed point)
g.set(a, None)   → na = None   (inhibition: Neg dep suppresses re-eval; na retains last value)
```

**Inhibition rule**: when any dependency goes to `None`, the downstream node does **not** re-evaluate. It retains its current value. This is by design — `None` means "signal withdrawn," not "recalculate."

### `"identity"` / `"id"` — pass-through

Single-input, passes the value through unchanged.

```python
relay = g.computed("relay", rule="identity")
g.connect(src, relay)
g.set(src, True)   → relay = True
```

### `"fold"` / `"bochvar"` — Bochvar fold

Multi-input fold. Any `False` in any input infects the output; all `True` → `True`; otherwise `None`.

### `"majority"` — three-input majority vote

Requires exactly three inputs connected. Two or more `True` → `True`. Two or more `False` → `False`. One of each → `None` (contested).

---

## Lazy nodes and demand

By default, computed nodes are **eager**: they recompute and push downstream whenever any dependency changes. A **lazy** node defers evaluation until explicitly demanded.

```python
src = g.input("src")

# Lazy node — will not recompute on push
summary = g.computed("summary", rule="identity", lazy=True)
g.connect(src, summary)

g.set(src, True)
print(g.get(summary))    # None — not evaluated yet

val = g.demand(summary)
print(val)               # True — computed on demand
print(g.get(summary))    # True — now cached
```

Lazy nodes are correct for variables that are expensive to evaluate or semantically should not be computed until observed.

### Switching modes

```python
g.make_lazy(node)    # stop push-evaluation; node becomes lazy
g.make_eager(node)   # resume push-evaluation; fires on next upstream change
```

`make_eager` does not retroactively push — it takes effect on the next upstream change.

---

## End-to-end walkthrough

Build a signal pipeline with a lazy summary node that coalesces multiple upstream changes into a single observation.

```python
import pygress

g = pygress.Graph()

# Two input signals
price  = g.input("price")
volume = g.input("volume")

# Eager AND gate: fires immediately when both inputs arrive
signal = g.computed("signal", rule="and")
g.connect(price,  signal)
g.connect(volume, signal)

# Lazy summary: accumulates upstream changes, computed only on demand
summary = g.computed("summary", rule="identity", lazy=True)
g.connect(signal, summary)

# Simulate a burst of upstream changes
g.set(price,  True)
g.set(volume, False)   # Bochvar: signal → False immediately
g.set(volume, True)    # recovery: signal → True

# Summary has accumulated three changes but done no work
print(g.get(summary))   # None — lazy, never evaluated

# Demand forces evaluation at the current stable state
print(g.demand(summary))  # True — one evaluation, not three

# Verify signal chain
print(g.get(signal))   # True
print(g.get(summary))  # True (now cached)

# Inject a conflict
g.set(price, False)
print(g.get(signal))   # False (Bochvar: price=False infects AND)
print(g.get(summary))  # True (lazy: not re-evaluated; retains cached value)

# Force re-observation of the contested state
print(g.demand(summary))  # False
```

### Graph introspection

```python
print(g.node_count())   # 4
print(g.edge_count())   # 3
print(repr(g))          # pygress.Graph(nodes=4, edges=4)  (edges include subscriptions)
```

---

## Structural operations

### Connect and disconnect

```python
g.connect(src, dst)      # add a dependency edge src → dst
g.disconnect(src, dst)   # remove it; raises RuntimeError if edge doesn't exist
```

After `disconnect`, `dst` no longer receives pushes from `src`. Its current value is retained (stale).

### Delete a node

```python
g.delete_node(n)   # removes node and all its incident edges
```

Downstream nodes retain their last value.

### Stabilize

```python
g.stabilize()   # run e-graph saturation over any Stabilizing-mode nodes; no-op if none
```

Rarely needed directly from Python — the engine calls stabilize automatically during propagation when `Zero` reaches a `Stabilizing`-mode node.

### Region hints

```python
g.declare_region(root_node)
g.declare_region(root_node, members=[a, b, c], max_depth=4)
g.declare_region(root_node, stability="epoch_tracked", compile="lazy")
```

`declare_region` is a hint to the engine about subgraph structure. It does not change evaluation semantics. Useful for large graphs where you want to control compilation of sparse circuit artifacts.

- `stability`: `"default"` or `"epoch_tracked"` (recompile when topology changes)
- `compile`: `"lazy"` (compile on first use) or `"never"` (always use warm path)

---

## API reference

### `pygress.Graph`

| Method | Description |
|--------|-------------|
| `Graph()` | Create a new empty graph |
| `input(name: str) → NodeHandle` | Create an input node (starts `None`) |
| `computed(name: str, rule: str, lazy: bool = False) → NodeHandle` | Create a computed node |
| `connect(src: NodeHandle, dst: NodeHandle)` | Add a dependency edge |
| `disconnect(src: NodeHandle, dst: NodeHandle)` | Remove a dependency edge |
| `set(node: NodeHandle, value: bool \| None)` | Set a node's value and propagate |
| `get(node: NodeHandle) → bool \| None` | Read a node's current value |
| `demand(node: NodeHandle) → bool \| None` | Pull-evaluate a node and its pending deps |
| `make_lazy(node: NodeHandle)` | Switch node to lazy mode |
| `make_eager(node: NodeHandle)` | Switch node to eager mode |
| `delete_node(node: NodeHandle)` | Remove a node and its edges |
| `stabilize()` | Run e-graph saturation pass |
| `declare_region(root, *, members=None, max_depth=None, stability="default", compile="lazy")` | Register a region hint |
| `node_count() → int` | Number of nodes in the graph |
| `edge_count() → int` | Number of dependency edges |

### `pygress.NodeHandle`

Opaque handle to a node. Supports `==`, `!=`, `hash()`, and `repr()`.

```python
a == a     # True
a == b     # False (different nodes)
{a, b}     # works — NodeHandle is hashable
repr(a)    # "pygress.NodeHandle(id=...)"
```

---

## Common patterns

### Fan-in convergence

```python
inputs = [g.input(f"i{n}") for n in range(10)]
gate = g.computed("gate", rule="and")
for i in inputs:
    g.connect(i, gate)

for i in inputs:
    g.set(i, True)
print(g.get(gate))  # True — fires exactly once after last input arrives
```

### Bochvar infection and recovery

```python
g.set(inputs[3], False)   # gate → False immediately (Bochvar)
g.set(inputs[3], True)    # gate → True (recovery once all inputs re-confirm)
```

### Lazy coalescing

Multiple upstream writes to a lazy node's deps are coalesced — the lazy node evaluates at most once per demand, regardless of how many upstream changes arrived since the last demand.

```python
for _ in range(100):
    g.set(src, True)
    g.set(src, False)
    g.set(src, True)

# All 300 writes above cost ~90 ns each (same-value suppression).
# The lazy node has done zero work.
print(g.demand(lazy_node))   # one evaluation
```

### Using NodeHandle in collections

```python
node_map = {a: "price", b: "volume"}
node_set = {a, b, signal}
seen = set()
seen.add(signal)
```

---

## Notes on semantics

**Inhibition rule**: when a dependency is set to `None`, its downstream computed nodes do **not** re-evaluate. They retain their current value. This prevents spurious clearing of derived state when an upstream signal goes temporarily unresolved.

**`"or"` is not classical OR**: both inputs must arrive before `"or"` evaluates. If you want a node that resolves as soon as *either* input is `True`, you need to manage that logic at the source level (e.g., set the other input to `False` explicitly to trigger evaluation).

**`False` vs `None`**: `False` (`T::Zero`) is a stable, infectious first-class value. `None` (`T::Neg`) is the ground state. Don't conflate them. A `False` output that you clear by setting the source to `None` will **not** recompute — the inhibition rule retains it. Set the source to `True` or `False` to force re-evaluation.

**Thread safety**: `Graph` is not thread-safe. If you need concurrent access, serialize calls externally or use separate `Graph` instances.
