"""
pgress scenario engine — manages a single active Graph + scenario.

Each scenario exposes input nodes (driveable via set_value) and computed
nodes (eager or lazy). Graph state is polled by the frontend via GET /graph.

Thread-safety: a threading.Lock protects every public call so FastAPI's
thread-pool workers can call concurrently without racing on the Graph.
"""

import threading
from dataclasses import dataclass
from typing import Optional

import pygress


# ── Node metadata ─────────────────────────────────────────────────────────────

@dataclass
class NodeMeta:
    name:   str
    kind:   str             # "input" | "computed"
    rule:   Optional[str]   # None for input nodes
    lazy:   bool
    handle: pygress.NodeHandle
    x:      float
    y:      float


# ── Engine ────────────────────────────────────────────────────────────────────

class ScenarioEngine:
    def __init__(self):
        self._lock     = threading.Lock()
        self._graph: Optional[pygress.Graph] = None
        self._nodes: dict[str, NodeMeta]     = {}
        self._edges: list[tuple[str, str]]   = []
        self._scenario: str                  = ""

        # ── Propagation stats ─────────────────────────────────────────────────
        self._ops:                 int = 0
        self._propagation_count:   int = 0   # downstream computed nodes changed
        self._recomputes_suppressed: int = 0 # ops where no computed node changed
        self._frontier_size:       int = 0   # computed nodes currently Pending
        self._last_op:             str = "none"

        self.load_scenario("and_gate")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _snapshot_computed(self) -> dict:
        """Snapshot {name: value} for all computed nodes (lock must be held)."""
        return {
            n: self._graph.get(m.handle)
            for n, m in self._nodes.items()
            if m.kind == "computed"
        }

    def _record_propagation(self, before: dict, after: dict) -> None:
        """Update stats after an op (lock must be held)."""
        changed = sum(1 for n in before if before[n] != after[n])
        if changed:
            self._propagation_count += changed
        else:
            self._recomputes_suppressed += 1
        self._frontier_size = sum(
            1 for n, m in self._nodes.items()
            if m.kind == "computed" and self._graph.get(m.handle) is None
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def load_scenario(self, name: str) -> None:
        builder = _SCENARIOS.get(name)
        if builder is None:
            raise ValueError(f"unknown scenario {name!r}")
        with self._lock:
            self._graph, self._nodes, self._edges = builder()
            self._scenario  = name
            self._ops       = 0
            self._propagation_count    = 0
            self._recomputes_suppressed = 0
            self._frontier_size        = 0
            self._last_op              = "none"

    def set_value(self, node_name: str, value) -> None:
        """Set an input node to True, False, or None (pending)."""
        with self._lock:
            meta = self._nodes.get(node_name)
            if meta is None or meta.kind != "input":
                raise ValueError(f"no input node {node_name!r}")
            before = self._snapshot_computed()
            self._graph.set(meta.handle, value)
            after  = self._snapshot_computed()
            self._record_propagation(before, after)
            v = "T" if value is True else "F" if value is False else "?"
            self._last_op = f"set {node_name} -> {v}"
            self._ops += 1

    def demand(self, node_name: str) -> None:
        """Force evaluation of a lazy node."""
        with self._lock:
            meta = self._nodes.get(node_name)
            if meta is None:
                raise ValueError(f"no node {node_name!r}")
            before = self._snapshot_computed()
            self._graph.demand(meta.handle)
            after  = self._snapshot_computed()
            self._record_propagation(before, after)
            self._last_op = f"demand {node_name}"
            self._ops += 1

    def get_graph_state(self) -> dict:
        """Return a React Flow-compatible node/edge snapshot."""
        with self._lock:
            nodes = []
            for meta in self._nodes.values():
                val = self._graph.get(meta.handle)
                nodes.append({
                    "id":       meta.name,
                    "position": {"x": meta.x, "y": meta.y},
                    "type":     "ternary",
                    "data": {
                        "label": meta.name,
                        "kind":  meta.kind,
                        "rule":  meta.rule,
                        "lazy":  meta.lazy,
                        "value": val,        # True | False | None
                    },
                })
            edges = [
                {"id": f"e_{src}_{tgt}", "source": src, "target": tgt}
                for src, tgt in self._edges
            ]
            return {
                "scenario": self._scenario,
                "nodes":    nodes,
                "edges":    edges,
            }

    def get_metrics(self) -> dict:
        with self._lock:
            return {
                "scenario":              self._scenario,
                "node_count":            self._graph.node_count() if self._graph else 0,
                "edge_count":            self._graph.edge_count() if self._graph else 0,
                "ops_applied":           self._ops,
                "propagation_count":     self._propagation_count,
                "recomputes_suppressed": self._recomputes_suppressed,
                "frontier_size":         self._frontier_size,
                "last_op":               self._last_op,
                "scenarios":             list(_SCENARIOS.keys()),
            }


# ── Scenario builders ─────────────────────────────────────────────────────────

def _make_nodes(g: pygress.Graph, specs: list) -> dict[str, NodeMeta]:
    """
    specs: list of (name, kind, rule_or_None, lazy, x, y)
    Registers each node on the graph and returns name → NodeMeta.
    """
    nodes: dict[str, NodeMeta] = {}
    for name, kind, rule, lazy, x, y in specs:
        if kind == "input":
            handle = g.input(name)
        else:
            handle = g.computed(name, rule=rule, lazy=lazy)
        nodes[name] = NodeMeta(
            name=name, kind=kind, rule=rule, lazy=lazy,
            handle=handle, x=x, y=y,
        )
    return nodes


def _wire(g: pygress.Graph, nodes: dict[str, NodeMeta],
          edges: list[tuple[str, str]]) -> None:
    for src, tgt in edges:
        g.connect(nodes[src].handle, nodes[tgt].handle)


# ── Greatest hits ─────────────────────────────────────────────────────────────

def _scenario_and_gate():
    """price AND volume → signal. Classic two-input gate."""
    g = pygress.Graph()
    nodes = _make_nodes(g, [
        ("price",  "input",    None,  False, 120, 80),
        ("volume", "input",    None,  False, 120, 240),
        ("signal", "computed", "and", False, 420, 160),
    ])
    edges = [("price", "signal"), ("volume", "signal")]
    _wire(g, nodes, edges)
    return g, nodes, edges


def _scenario_bochvar_chain():
    """
    Three inputs → fold → downstream (identity).
    Key: any False (contested) input makes fold_out False, infecting downstream.
    True+True+True → True. Any None with no False → None.
    """
    g = pygress.Graph()
    nodes = _make_nodes(g, [
        ("a",          "input",    None,       False, 80,  80),
        ("b",          "input",    None,       False, 80,  220),
        ("c",          "input",    None,       False, 80,  360),
        ("fold_out",   "computed", "fold",     False, 360, 220),
        ("downstream", "computed", "identity", False, 620, 220),
    ])
    edges = [
        ("a", "fold_out"), ("b", "fold_out"), ("c", "fold_out"),
        ("fold_out", "downstream"),
    ]
    _wire(g, nodes, edges)
    return g, nodes, edges


def _scenario_lazy_demand():
    """
    price AND volume → signal → summary (lazy identity).
    summary stays Pending until DEMAND is explicitly called,
    even after signal changes.
    """
    g = pygress.Graph()
    nodes = _make_nodes(g, [
        ("price",   "input",    None,       False, 80,  80),
        ("volume",  "input",    None,       False, 80,  240),
        ("signal",  "computed", "and",      False, 360, 160),
        ("summary", "computed", "identity", True,  640, 160),
    ])
    edges = [
        ("price",  "signal"),
        ("volume", "signal"),
        ("signal", "summary"),
    ]
    _wire(g, nodes, edges)
    return g, nodes, edges


# ── All computation rules ─────────────────────────────────────────────────────

def _scenario_or_gate():
    """Any True input → alert True. Both None → None."""
    g = pygress.Graph()
    nodes = _make_nodes(g, [
        ("source_a", "input",    None, False, 120, 80),
        ("source_b", "input",    None, False, 120, 240),
        ("alert",    "computed", "or", False, 420, 160),
    ])
    edges = [("source_a", "alert"), ("source_b", "alert")]
    _wire(g, nodes, edges)
    return g, nodes, edges


def _scenario_not_gate():
    """
    L3 negation: True→Pending, Pending→True, False→False (self-complementary).
    False is the fixed point of negation in Łukasiewicz L3.
    """
    g = pygress.Graph()
    nodes = _make_nodes(g, [
        ("condition", "input",    None,  False, 120, 160),
        ("negated",   "computed", "not", False, 420, 160),
    ])
    edges = [("condition", "negated")]
    _wire(g, nodes, edges)
    return g, nodes, edges


def _scenario_majority():
    """
    Three-input majority vote. Two or more True → True.
    Tie (one True, one False, one Pending) → reflects the contested middle.
    """
    g = pygress.Graph()
    nodes = _make_nodes(g, [
        ("voter_a",  "input",    None,       False, 120, 80),
        ("voter_b",  "input",    None,       False, 120, 230),
        ("voter_c",  "input",    None,       False, 120, 380),
        ("decision", "computed", "majority", False, 420, 230),
    ])
    edges = [
        ("voter_a", "decision"),
        ("voter_b", "decision"),
        ("voter_c", "decision"),
    ]
    _wire(g, nodes, edges)
    return g, nodes, edges


# ── Registry (ordered: greatest hits first) ───────────────────────────────────

_SCENARIOS: dict[str, callable] = {
    "and_gate":      _scenario_and_gate,
    "bochvar_chain": _scenario_bochvar_chain,
    "lazy_demand":   _scenario_lazy_demand,
    "or_gate":       _scenario_or_gate,
    "not_gate":      _scenario_not_gate,
    "majority":      _scenario_majority,
}

# Module-level singleton consumed by main.py
engine = ScenarioEngine()
