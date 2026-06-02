//! pygress Python extension — PyO3 bindings.
//!
//! Exposes a single `Graph` class. Ternary internals are fully hidden:
//!   T::Pos  ↔  True
//!   T::Zero ↔  False (contested / conflict)
//!   T::Neg  ↔  None  (pending / not yet evaluated)
//!
//! Node handles are opaque `NodeHandle` objects — stable Python references
//! backed by the engine's `Uid`. The engine owns all graph state.

use pgress_core::{
    node::ComputeRule,
    region::{CompilePolicy, RegionBoundary, StabilityContract},
    uid,
    Engine, IsaOp, T,
};
use pyo3::prelude::*;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};

// ── Value conversion ──────────────────────────────────────────────────────────

fn py_to_t(val: &Bound<'_, PyAny>) -> PyResult<T> {
    if val.is_none() {
        return Ok(T::Neg);
    }
    let b: bool = val.extract()?;
    Ok(if b { T::Pos } else { T::Zero })
}

fn t_to_py(py: Python<'_>, val: T) -> PyObject {
    match val {
        T::Pos  => true.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        T::Zero => false.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        T::Neg  => py.None(),
    }
}

// ── NodeHandle ────────────────────────────────────────────────────────────────

/// Opaque handle to a node in a `Graph`. Not constructable from Python directly.
#[pyclass(frozen)]
pub struct NodeHandle {
    id: pgress_core::Uid,
}

#[pymethods]
impl NodeHandle {
    fn __repr__(&self) -> String {
        format!("<pygress.NodeHandle {}>", self.id)
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let mut h = DefaultHasher::new();
        self.id.hash(&mut h);
        h.finish()
    }

    fn __eq__(&self, other: &NodeHandle) -> bool {
        self.id == other.id
    }
}

// ── Graph ─────────────────────────────────────────────────────────────────────

/// A reactive computation graph.
///
/// Nodes are either **inputs** (driven externally via `set`) or **computed**
/// (derived from their dependencies via a rule). Computed nodes can be eager
/// (recompute immediately on any input change) or lazy (recompute only when
/// explicitly observed via `demand`).
///
/// Values are Python `True`, `False`, or `None` (pending / not yet evaluated).
///
/// Example::
///
///     import pgrs
///
///     g = pgrs.Graph()
///     a = g.input("a")
///     b = g.input("b")
///     out = g.computed("out", rule="and")
///     g.connect(a, out)
///     g.connect(b, out)
///
///     g.set(a, True)
///     g.set(b, True)
///     print(g.get(out))   # True
///
///     g.set(b, False)
///     print(g.get(out))   # False
#[pyclass]
pub struct Graph {
    engine: Engine,
}

#[pymethods]
impl Graph {
    #[new]
    pub fn new() -> Self {
        Graph { engine: Engine::new() }
    }

    // ── Node creation ─────────────────────────────────────────────────────────

    /// Create an input node. Input nodes are driven externally via `set`.
    pub fn input(&mut self, name: &str) -> NodeHandle {
        let id = uid::fresh();
        self.engine.apply(IsaOp::input_node(id, name)).unwrap();
        NodeHandle { id }
    }

    /// Create a computed node that derives its value from its connected inputs.
    ///
    /// `rule` controls how dependency values are combined:
    ///   - ``"and"``      — True only when **all** inputs are True (MV-min / Łukasiewicz product)
    ///   - ``"or"``       — True when **any** input is True (MV-max / Łukasiewicz sum)
    ///   - ``"not"``      — negate a single input (MV-negation; ``True→None, None→True, False→False``)
    ///   - ``"majority"`` — Three-input majority vote (``merge``)
    ///   - ``"fold"``     — Bochvar-fold: ``False`` (conflict) is infectious across all inputs
    ///   - ``"identity"`` — pass-through a single input unchanged
    ///
    /// Aliases: ``"all"`` = ``"and"``, ``"any"`` = ``"or"``, ``"id"`` = ``"identity"``.
    ///
    /// By default the node is eager (recomputes immediately on any input change).
    /// Pass ``lazy=True`` to defer evaluation until `demand` is called.
    #[pyo3(signature = (name, rule="and", lazy=false))]
    pub fn computed(
        &mut self,
        name: &str,
        rule: &str,
        lazy: bool,
    ) -> PyResult<NodeHandle> {
        let compute_rule = match rule {
            "and" | "all"        => ComputeRule::MeetAll,
            "or"  | "any"        => ComputeRule::JoinAny,
            "not" | "neg"        => ComputeRule::MvNeg,
            "majority"           => ComputeRule::Merge,
            "fold" | "bochvar"   => ComputeRule::BochvarFold,
            "identity" | "id"    => ComputeRule::Identity,
            other => return Err(PyValueError::new_err(
                format!(
                    "unknown rule {:?}; use 'and', 'or', 'not', 'majority', 'fold', or 'identity'",
                    other
                )
            )),
        };
        let id = uid::fresh();
        self.engine.apply(IsaOp::computed_node(id, name, compute_rule)).unwrap();
        if lazy {
            self.engine.apply(IsaOp::SetMode {
                node: id,
                mode: pgress_core::node::ExecMode::Lazy,
            }).unwrap();
        }
        Ok(NodeHandle { id })
    }

    // ── Wiring ────────────────────────────────────────────────────────────────

    /// Connect `src` as a dependency of `tgt`.
    ///
    /// After connecting, `tgt` will recompute whenever `src` changes (if eager)
    /// or when `demand(tgt)` is called (if lazy).
    pub fn connect(&mut self, src: &NodeHandle, tgt: &NodeHandle) -> PyResult<()> {
        let edge_id = uid::fresh();
        self.engine.apply(IsaOp::dep_edge(edge_id, src.id, tgt.id))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // ── Driving values ────────────────────────────────────────────────────────

    /// Set the value of an input node.
    ///
    /// `value` must be ``True``, ``False``, or ``None`` (clears back to pending).
    pub fn set(&mut self, node: &NodeHandle, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let t = py_to_t(value)?;
        self.engine.apply(IsaOp::SetValue { node: node.id, val: t })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // ── Reading values ────────────────────────────────────────────────────────

    /// Return the current value of a node: ``True``, ``False``, or ``None``.
    ///
    /// ``None`` means the node has not yet been evaluated (pending). For lazy
    /// nodes, use `demand` to force evaluation first.
    pub fn get(&self, py: Python<'_>, node: &NodeHandle) -> PyResult<PyObject> {
        let val = self.engine.value_of(node.id)
            .ok_or_else(|| PyKeyError::new_err("node not found"))?;
        Ok(t_to_py(py, val))
    }

    /// Force evaluation of a lazy node and return its value.
    ///
    /// Recursively evaluates any pending dependencies before computing `node`.
    /// Equivalent to `get` for eager nodes (demand is a no-op on already-current
    /// values).
    pub fn demand(&mut self, py: Python<'_>, node: &NodeHandle) -> PyResult<PyObject> {
        self.engine.apply(IsaOp::Demand { node: node.id })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let val = self.engine.value_of(node.id)
            .ok_or_else(|| PyKeyError::new_err("node not found"))?;
        Ok(t_to_py(py, val))
    }

    // ── Mode control ──────────────────────────────────────────────────────────

    /// Mark a node as lazy. Lazy nodes do not recompute on input changes;
    /// they only evaluate when `demand` is called.
    pub fn make_lazy(&mut self, node: &NodeHandle) -> PyResult<()> {
        self.engine.apply(IsaOp::SetMode {
            node: node.id,
            mode: pgress_core::node::ExecMode::Lazy,
        }).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Mark a node as eager (default). Eager nodes recompute immediately when
    /// any dependency changes.
    pub fn make_eager(&mut self, node: &NodeHandle) -> PyResult<()> {
        self.engine.apply(IsaOp::SetMode {
            node: node.id,
            mode: pgress_core::node::ExecMode::Eager,
        }).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // ── Structural operations ─────────────────────────────────────────────────

    /// Delete a node and all its incident edges from the graph.
    ///
    /// After deletion, the `NodeHandle` is invalid and must not be reused.
    /// Raises `RuntimeError` if the node has remote-partition edges (DPO rule).
    pub fn delete_node(&mut self, node: &NodeHandle) -> PyResult<()> {
        self.engine.apply(IsaOp::DelNode { id: node.id })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Delete an edge by its source and target nodes.
    ///
    /// This is a convenience wrapper: it scans incident edges of `src` for the
    /// first edge to `tgt` and removes it. Use the lower-level ISA `DelEdge`
    /// if you need to delete a specific edge by ID.
    /// Raises `RuntimeError` if no such edge exists.
    pub fn disconnect(&mut self, src: &NodeHandle, tgt: &NodeHandle) -> PyResult<()> {
        // Walk graph edges for src→tgt; take the first match.
        let edge_id = self.engine.graph
            .edges_from(src.id)
            .find(|e| e.tgt == tgt.id)
            .map(|e| e.id)
            .ok_or_else(|| PyRuntimeError::new_err(
                format!("no edge from {} to {}", src.id, tgt.id)
            ))?;
        self.engine.apply(IsaOp::DelEdge { id: edge_id })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // ── Region compilation hints ──────────────────────────────────────────────

    /// Declare a compilation region rooted at `root`.
    ///
    /// A region hint tells the engine to pre-compile a circuit for a stable
    /// subgraph so that value propagation inside it uses fast column-push
    /// (vectorised SWAR) rather than per-node evaluation.
    ///
    /// **Boundary selection** — choose one of:
    /// - ``max_depth=N`` (default 8): include all dependency-reachable nodes
    ///   within ``N`` hops of ``root`` (DepClosure boundary).
    /// - ``members=[n1, n2, ...]``: include exactly these nodes (ExplicitSet
    ///   boundary). Use this when you know the exact circuit.
    ///
    /// **stability** — ``"pinned"`` (default): the compiled circuit is kept
    /// indefinitely; ``"epoch_tracked"``: recompiled when the adjacency epoch
    /// changes (new edges added).
    ///
    /// **compile** — ``"eager"`` (default): compile immediately when the op
    /// is applied; ``"lazy"``: defer until first Stabilize; ``"never"``: only
    /// register the boundary, do not compile.
    ///
    /// Example — pre-compile a 3-node AND gate::
    ///
    ///     a = g.input("a"); b = g.input("b"); c = g.input("c")
    ///     out = g.computed("out", rule="and")
    ///     g.connect(a, out); g.connect(b, out); g.connect(c, out)
    ///     g.declare_region(out)   # DepClosure(depth=8) by default
    #[pyo3(signature = (root, max_depth=None, members=None, stability="pinned", compile="eager"))]
    pub fn declare_region(
        &mut self,
        root:      &NodeHandle,
        max_depth: Option<usize>,
        members:   Option<Vec<PyRef<NodeHandle>>>,
        stability: &str,
        compile:   &str,
    ) -> PyResult<()> {
        let boundary = match (max_depth, members) {
            (_, Some(ms)) => {
                // ExplicitSet takes priority if both provided.
                let ids = ms.iter().map(|h| h.id).collect();
                RegionBoundary::ExplicitSet(ids)
            }
            (Some(d), None) => RegionBoundary::DepClosure { max_depth: d },
            (None,    None) => RegionBoundary::DepClosure { max_depth: 8 },
        };

        let stab = match stability {
            "pinned"        => StabilityContract::Pinned,
            "epoch_tracked" => StabilityContract::EpochTracked,
            other => return Err(PyValueError::new_err(
                format!("unknown stability {:?}; use 'pinned' or 'epoch_tracked'", other)
            )),
        };

        let comp = match compile {
            "eager" => CompilePolicy::Eager,
            "lazy"  => CompilePolicy::Lazy,
            "never" => CompilePolicy::Never,
            other => return Err(PyValueError::new_err(
                format!("unknown compile {:?}; use 'eager', 'lazy', or 'never'", other)
            )),
        };

        self.engine.apply(IsaOp::RegionDeclare {
            root:      root.id,
            boundary,
            stability: stab,
            compile:   comp,
        }).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    /// Run global stabilization over all conflicted (False) nodes.
    ///
    /// Equivalent to sending a `Stabilize` ISA op with no explicit region.
    /// Useful after a batch of `set` calls that may have introduced conflicts.
    pub fn stabilize(&mut self) -> PyResult<()> {
        self.engine.apply(IsaOp::Stabilize { region: None })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(())
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    /// Number of nodes currently in the graph.
    pub fn node_count(&self) -> usize { self.engine.node_count() }

    /// Number of edges currently in the graph.
    pub fn edge_count(&self) -> usize { self.engine.edge_count() }

    fn __repr__(&self) -> String {
        format!(
            "<pygress.Graph nodes={} edges={}>",
            self.engine.node_count(),
            self.engine.edge_count(),
        )
    }
}

// ── Module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn _pygress(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Graph>()?;
    m.add_class::<NodeHandle>()?;
    Ok(())
}
