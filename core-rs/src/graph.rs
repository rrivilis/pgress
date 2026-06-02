//! Persistent attributed graph — the substrate store.
//!
//! Uses `im::HashMap` for structural sharing: cloning a snapshot is O(1)
//! and shares memory with the original. History is free.

use im::HashMap as ImMap;
use crate::{
    node::{Edge, Node},
    uid::Uid,
};

/// The persistent attributed graph.
///
/// Cloning this struct is O(1) due to structural sharing in `im::HashMap`.
/// Each clone is an independent snapshot that can diverge from its parent.
#[derive(Clone, Debug, Default)]
pub struct Graph {
    pub nodes: ImMap<Uid, Node>,
    pub edges: ImMap<Uid, Edge>,
}

impl Graph {
    pub fn new() -> Self { Self::default() }

    // ── Node operations ───────────────────────────────────────────────────

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.id, node);
    }

    pub fn remove_node(&mut self, id: Uid) -> Option<Node> {
        self.nodes.remove(&id)
    }

    pub fn node(&self, id: Uid) -> Option<&Node> {
        self.nodes.get(&id)
    }

    pub fn node_mut(&mut self, id: Uid) -> Option<&mut Node> {
        self.nodes.get_mut(&id)
    }

    // ── Edge operations ───────────────────────────────────────────────────

    pub fn add_edge(&mut self, edge: Edge) {
        self.edges.insert(edge.id, edge);
    }

    pub fn remove_edge(&mut self, id: Uid) -> Option<Edge> {
        self.edges.remove(&id)
    }

    pub fn edge(&self, id: Uid) -> Option<&Edge> {
        self.edges.get(&id)
    }

    /// All edges whose source is `src`.
    pub fn edges_from(&self, src: Uid) -> impl Iterator<Item = &Edge> {
        self.edges.values().filter(move |e| e.src == src)
    }

    /// All edges whose target is `tgt`.
    pub fn edges_to(&self, tgt: Uid) -> impl Iterator<Item = &Edge> {
        self.edges.values().filter(move |e| e.tgt == tgt)
    }

    // ── Dangling edge check (I6 — DPO admissibility) ─────────────────────

    /// Returns edge IDs that reference a node not in the graph.
    /// Used to enforce the dangling edge condition before node removal.
    pub fn dangling_edges(&self, node_id: Uid) -> Vec<Uid> {
        let mut v: Vec<Uid> = self.edges
            .values()
            .filter(|e| e.src == node_id || e.tgt == node_id)
            .map(|e| e.id)
            .collect();
        // Sort for deterministic edge-removal order across Engine instances.
        // im::HashMap iterates in per-instance random seed order.
        v.sort_unstable();
        v
    }

    // ── Snapshot ──────────────────────────────────────────────────────────

    /// Take an O(1) immutable snapshot. The snapshot shares memory with self.
    pub fn snapshot(&self) -> Graph { self.clone() }

    pub fn node_count(&self) -> usize { self.nodes.len() }
    pub fn edge_count(&self) -> usize { self.edges.len() }
}
