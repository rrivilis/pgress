"""
Tests for the pygress Python SDK.

Values:
    True  — Pos (signal present, condition holds)
    False — Zero (conflict / contested; Bochvar-infected)
    None  — Neg (pending / not yet evaluated)
"""

import pytest
import pygress
from pygress import Graph, NodeHandle


# ── Smoke test ────────────────────────────────────────────────────────────────

class TestSmoke:
    def test_import(self):
        assert hasattr(pygress, "Graph")
        assert hasattr(pygress, "NodeHandle")

    def test_graph_repr(self):
        g = Graph()
        r = repr(g)
        assert "pygress.Graph" in r
        assert "nodes=0" in r

    def test_node_handle_repr(self):
        g = Graph()
        n = g.input("x")
        assert "pygress.NodeHandle" in repr(n)

    def test_node_handle_equality(self):
        g = Graph()
        a = g.input("a")
        b = g.input("b")
        assert a == a
        assert a != b

    def test_node_handle_hashable(self):
        g = Graph()
        a = g.input("a")
        b = g.input("b")
        s = {a, b}
        assert len(s) == 2
        s.add(a)
        assert len(s) == 2


# ── Node creation ─────────────────────────────────────────────────────────────

class TestNodeCreation:
    def test_input_node_starts_pending(self):
        g = Graph()
        n = g.input("x")
        assert g.get(n) is None

    def test_computed_node_starts_pending(self):
        g = Graph()
        n = g.computed("c", rule="and")
        assert g.get(n) is None

    def test_node_count_increases(self):
        g = Graph()
        assert g.node_count() == 0
        g.input("a")
        assert g.node_count() == 1
        g.input("b")
        assert g.node_count() == 2

    def test_edge_count_increases_on_connect(self):
        g = Graph()
        a = g.input("a")
        c = g.computed("c", rule="and")
        assert g.edge_count() == 0
        g.connect(a, c)
        assert g.edge_count() == 1

    def test_all_compute_rules_accepted(self):
        rules = ["and", "or", "not", "majority", "fold", "identity",
                 "all", "any", "neg", "bochvar", "id"]
        g = Graph()
        for rule in rules:
            g.computed(f"node_{rule}", rule=rule)

    def test_unknown_rule_raises_value_error(self):
        g = Graph()
        with pytest.raises(ValueError):
            g.computed("bad", rule="xor")


# ── Value setting and getting ─────────────────────────────────────────────────

class TestValues:
    def test_set_true(self):
        g = Graph()
        n = g.input("x")
        g.set(n, True)
        assert g.get(n) is True

    def test_set_false(self):
        g = Graph()
        n = g.input("x")
        g.set(n, False)
        assert g.get(n) is False

    def test_set_none(self):
        g = Graph()
        n = g.input("x")
        g.set(n, True)
        g.set(n, None)
        assert g.get(n) is None

    def test_unknown_node_raises(self):
        g1 = Graph()
        g2 = Graph()
        a = g1.input("a")
        with pytest.raises(Exception):
            g2.get(a)


# ── Eager propagation ─────────────────────────────────────────────────────────

class TestEagerPropagation:
    def test_identity_propagates(self):
        g = Graph()
        src = g.input("src")
        dst = g.computed("dst", rule="identity")
        g.connect(src, dst)
        g.set(src, True)
        assert g.get(dst) is True

    def test_and_gate_both_true(self):
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="and")
        g.connect(a, out); g.connect(b, out)
        g.set(a, True); g.set(b, True)
        assert g.get(out) is True

    def test_and_gate_one_false(self):
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="and")
        g.connect(a, out); g.connect(b, out)
        g.set(a, True); g.set(b, False)
        # False (conflict) infects output — Bochvar strict
        assert g.get(out) is False

    def test_and_gate_one_pending(self):
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="and")
        g.connect(a, out); g.connect(b, out)
        g.set(a, True)
        # b is still None → output stays pending (Neg suspends propagation)
        assert g.get(out) is None

    def test_or_gate_any_true_wins(self):
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="or")
        g.connect(a, out); g.connect(b, out)
        g.set(a, True); g.set(b, None)
        # One input True, one pending — join_any returns True only when all Pos;
        # with one pending it is still pending
        assert g.get(out) is None
        g.set(b, True)
        assert g.get(out) is True

    def test_not_gate_negates(self):
        g = Graph()
        a  = g.input("a")
        na = g.computed("na", rule="not")
        g.connect(a, na)
        g.set(a, True)
        # MV-negation: Pos → Neg (pending) — na goes None
        assert g.get(na) is None
        g.set(a, None)
        # Inhibition rule: when dep is Neg, the computed node does NOT re-evaluate.
        # na retains its last computed value (None).
        assert g.get(na) is None
        g.set(a, False)
        # Zero → Zero (conflict is a fixed point under MV-negation)
        assert g.get(na) is False

    def test_chain_propagation(self):
        g = Graph()
        a = g.input("a")
        b = g.computed("b", rule="identity")
        c = g.computed("c", rule="identity")
        g.connect(a, b); g.connect(b, c)
        g.set(a, True)
        assert g.get(b) is True
        assert g.get(c) is True

    def test_bochvar_fold_conflict_infects(self):
        g = Graph()
        a = g.input("a"); b = g.input("b"); c = g.input("c")
        out = g.computed("out", rule="fold")
        g.connect(a, out); g.connect(b, out); g.connect(c, out)
        g.set(a, True); g.set(b, False); g.set(c, True)
        # Any False → output is False (Bochvar strict)
        assert g.get(out) is False

    def test_value_change_propagates_downstream(self):
        g = Graph()
        a = g.input("a")
        out = g.computed("out", rule="identity")
        g.connect(a, out)
        g.set(a, True)
        assert g.get(out) is True
        g.set(a, False)
        assert g.get(out) is False
        g.set(a, None)
        # Inhibition rule: setting dep to Neg suppresses re-evaluation.
        # out retains its last computed value (False).
        assert g.get(out) is False


# ── Lazy evaluation ───────────────────────────────────────────────────────────

class TestLazy:
    def test_lazy_node_does_not_push(self):
        g = Graph()
        src = g.input("src")
        lazy = g.computed("lazy", rule="identity", lazy=True)
        g.connect(src, lazy)
        g.set(src, True)
        # Lazy node should NOT have recomputed — still pending
        assert g.get(lazy) is None

    def test_demand_forces_evaluation(self):
        g = Graph()
        src = g.input("src")
        lazy = g.computed("lazy", rule="identity", lazy=True)
        g.connect(src, lazy)
        g.set(src, True)
        val = g.demand(lazy)
        assert val is True
        assert g.get(lazy) is True

    def test_demand_eager_is_noop(self):
        g = Graph()
        src = g.input("src")
        eager = g.computed("eager", rule="identity")
        g.connect(src, eager)
        g.set(src, True)
        # Already evaluated eagerly — demand returns the current value
        val = g.demand(eager)
        assert val is True

    def test_make_lazy_stops_push(self):
        g = Graph()
        src = g.input("src")
        node = g.computed("node", rule="identity")
        g.connect(src, node)
        g.set(src, True)
        assert g.get(node) is True  # eagerly evaluated
        g.make_lazy(node)
        g.set(src, None)
        # After becoming lazy, no push — value is stale (True)
        assert g.get(node) is True

    def test_make_eager_resumes_push(self):
        g = Graph()
        src = g.input("src")
        node = g.computed("node", rule="identity", lazy=True)
        g.connect(src, node)
        g.set(src, True)
        assert g.get(node) is None  # lazy, no push
        g.make_eager(node)
        # Becoming eager doesn't retroactively push, but the next change does
        g.set(src, False)
        assert g.get(node) is False


# ── Structural operations ─────────────────────────────────────────────────────

class TestStructural:
    def test_delete_node(self):
        g = Graph()
        a = g.input("a")
        assert g.node_count() == 1
        g.delete_node(a)
        assert g.node_count() == 0

    def test_delete_node_removes_edges(self):
        g = Graph()
        a = g.input("a")
        b = g.computed("b", rule="identity")
        g.connect(a, b)
        assert g.edge_count() == 1
        g.delete_node(a)
        assert g.edge_count() == 0

    def test_disconnect_removes_edge(self):
        g = Graph()
        a = g.input("a")
        b = g.computed("b", rule="identity")
        g.connect(a, b)
        assert g.edge_count() == 1
        g.disconnect(a, b)
        assert g.edge_count() == 0

    def test_disconnect_stops_propagation(self):
        g = Graph()
        a = g.input("a")
        b = g.computed("b", rule="identity")
        g.connect(a, b)
        g.set(a, True)
        assert g.get(b) is True
        g.disconnect(a, b)
        g.set(a, None)
        # b is no longer wired to a — value stays stale
        assert g.get(b) is True

    def test_disconnect_nonexistent_edge_raises(self):
        g = Graph()
        a = g.input("a")
        b = g.computed("b", rule="identity")
        with pytest.raises(RuntimeError):
            g.disconnect(a, b)


# ── Stabilize ─────────────────────────────────────────────────────────────────

class TestStabilize:
    def test_stabilize_no_conflict_is_noop(self):
        g = Graph()
        a = g.input("a")
        g.set(a, True)
        g.stabilize()
        assert g.get(a) is True

    def test_stabilize_runs_without_error_on_empty_graph(self):
        g = Graph()
        g.stabilize()


# ── RegionDeclare ─────────────────────────────────────────────────────────────

class TestRegionDeclare:
    def test_declare_region_dep_closure(self):
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="and")
        g.connect(a, out); g.connect(b, out)
        # Should not raise
        g.declare_region(out)

    def test_declare_region_explicit_members(self):
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="and")
        g.connect(a, out); g.connect(b, out)
        g.declare_region(out, members=[a, b, out])

    def test_declare_region_custom_depth(self):
        g = Graph()
        a = g.input("a")
        out = g.computed("out", rule="identity")
        g.connect(a, out)
        g.declare_region(out, max_depth=3)

    def test_declare_region_stability_epoch_tracked(self):
        g = Graph()
        n = g.input("n")
        out = g.computed("out", rule="identity")
        g.connect(n, out)
        g.declare_region(out, stability="epoch_tracked")

    def test_declare_region_compile_lazy(self):
        g = Graph()
        n = g.input("n")
        out = g.computed("out", rule="identity")
        g.connect(n, out)
        g.declare_region(out, compile="lazy")

    def test_declare_region_compile_never(self):
        g = Graph()
        n = g.input("n")
        out = g.computed("out", rule="identity")
        g.connect(n, out)
        g.declare_region(out, compile="never")

    def test_declare_region_still_propagates(self):
        """RegionDeclare is a hint — values still propagate correctly after."""
        g = Graph()
        a = g.input("a"); b = g.input("b")
        out = g.computed("out", rule="and")
        g.connect(a, out); g.connect(b, out)
        g.declare_region(out, max_depth=4, stability="epoch_tracked")
        g.set(a, True); g.set(b, True)
        assert g.get(out) is True
        g.set(b, None)
        # Inhibition rule: setting b to Neg suppresses re-evaluation of out.
        # out retains its last computed value (True).
        assert g.get(out) is True

    def test_declare_region_unknown_stability_raises(self):
        g = Graph()
        n = g.input("n")
        with pytest.raises(ValueError, match="stability"):
            g.declare_region(n, stability="unknown")

    def test_declare_region_unknown_compile_raises(self):
        g = Graph()
        n = g.input("n")
        with pytest.raises(ValueError, match="compile"):
            g.declare_region(n, compile="instant")


# ── Fan-in / fan-out scenarios ────────────────────────────────────────────────

class TestTopology:
    def test_fan_in_and_gate_three_inputs(self):
        g = Graph()
        inputs = [g.input(f"i{n}") for n in range(3)]
        gate = g.computed("gate", rule="and")
        for i in inputs:
            g.connect(i, gate)
        for i in inputs:
            g.set(i, True)
        assert g.get(gate) is True

    def test_fan_out_one_input_drives_many(self):
        g = Graph()
        src = g.input("src")
        consumers = [g.computed(f"c{n}", rule="identity") for n in range(5)]
        for c in consumers:
            g.connect(src, c)
        g.set(src, True)
        for c in consumers:
            assert g.get(c) is True, f"consumer {c} should be True"

    def test_dag_depth_three(self):
        g = Graph()
        a = g.input("a"); b = g.input("b"); c = g.input("c")
        ab = g.computed("ab", rule="and"); bc = g.computed("bc", rule="and")
        root = g.computed("root", rule="or")
        g.connect(a, ab); g.connect(b, ab)
        g.connect(b, bc); g.connect(c, bc)
        g.connect(ab, root); g.connect(bc, root)
        g.set(a, True); g.set(b, True); g.set(c, True)
        assert g.get(ab)   is True
        assert g.get(bc)   is True
        assert g.get(root) is True

    def test_conflict_propagates_transitively(self):
        g = Graph()
        src = g.input("src")
        mid = g.computed("mid", rule="identity")
        dst = g.computed("dst", rule="identity")
        g.connect(src, mid); g.connect(mid, dst)
        g.set(src, False)
        # Bochvar infection: False propagates through the chain
        assert g.get(mid) is False
        assert g.get(dst) is False
