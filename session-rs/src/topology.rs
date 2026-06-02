//! Topology — fabric-aware placement via a pgress-compatible stream interface.
//!
//! In v1, topology is backed by an in-memory index associated with the reserved
//! partition ID `PARTITION_TOPOLOGY`. The interface is forward-compatible with a
//! future first-class engine partition: one node per shard, edges encoding
//! physical adjacency via `ShardFabricAddr`, `ShardPressure` dual-written as
//! ternary health state so the graph reflects both fabric geometry and runtime
//! load (thermodynamics) in the same structure.
//!
//! When materialised as a first-class partition, placement subscribers will use
//! standard `Subscribe` / `Demand` ops; the node naming and ternary encoding
//! defined here remain stable.
//!
//! ## Reserved partition ID
//!
//! `PARTITION_TOPOLOGY = 0xFFFF_FFFF_FFFF_FFFD` — one below `PARTITION_TELEMETRY`.
//!
//! ## Ternary health encoding
//!
//! | Value | Meaning                                        |
//! |-------|------------------------------------------------|
//! | Pos   | Healthy — accept new partition assignments     |
//! | Zero  | Congested — use with caution, prefer others   |
//! | Neg   | Hot / unknown — do not route new work here    |
//!
//! ## Shard node naming
//!
//! Shard nodes in the topology partition are named `shard::{id}::fabric`
//! (via `TopologyNodeName::shard`), allowing the session manager to locate
//! its own node without a directory lookup.
//!
//! ## Placement scoring
//!
//! `TopologyPartition::best_shard_for` returns the `Pos`-health shard with
//! the smallest hop distance to a given requester address. This is a structural
//! estimate only — no BFS, no global state — and is suitable for the MVP.
//! Full graph-traversal placement (incorporating causal scope and propagation
//! frontier data) is a future enhancement.

use rustc_hash::FxHashMap;
use crate::{
    ShardId,
    admission::ShardPressure,
    domain::{DomainRegistry, ShardFabricAddr},
};

/// Reserved partition ID for the topology graph.
pub const PARTITION_TOPOLOGY: u64 = 0xFFFF_FFFF_FFFF_FFFD;

// Thresholds for health classification.
// Conservative relative to admission policy thresholds: the topology partition
// should react earlier than the admission controller drops traffic.
const CONGESTED_QUEUE_DEPTH: u32 = 500;   // half the default shed threshold (5_000)
const HOT_CURSOR_COUNT:      u32 = 1;     // any parked WorkCursor → Neg

// ── Ternary health constants ──────────────────────────────────────────────────

pub const HEALTH_POS:  i8 =  1;   // healthy
pub const HEALTH_ZERO: i8 =  0;   // congested
pub const HEALTH_NEG:  i8 = -1;   // hot / unknown

fn health_from_pressure(p: &ShardPressure) -> i8 {
    if p.cursor_count  >= HOT_CURSOR_COUNT      { return HEALTH_NEG;  }
    if p.queue_depth   >= CONGESTED_QUEUE_DEPTH { return HEALTH_ZERO; }
    HEALTH_POS
}

// ── TopologyNode ──────────────────────────────────────────────────────────────

/// Snapshot of a shard's topology state.
#[derive(Clone, Debug)]
pub struct TopologyNode {
    /// Current ternary health value: Pos / Zero / Neg.
    pub health:          i8,
    pub queue_depth:     u32,
    pub cursor_count:    u32,
    pub budget_consumed: u64,
}

impl TopologyNode {
    fn from_pressure(pressure: &ShardPressure) -> Self {
        TopologyNode {
            health:          health_from_pressure(pressure),
            queue_depth:     pressure.queue_depth,
            cursor_count:    pressure.cursor_count,
            budget_consumed: pressure.budget_consumed,
        }
    }

    /// True if this shard is a suitable target for new partition assignments.
    pub fn is_healthy(&self) -> bool { self.health == HEALTH_POS }
}

// ── TopologyPartition ─────────────────────────────────────────────────────────

/// Fabric-topology-aware shard health map.
///
/// Updated by `SessionRuntime::update_pressure`. Consulted for placement hints.
/// The health values correspond to `SetValue` ops that would be emitted on the
/// topology partition when engine integration is complete.
#[derive(Clone, Debug, Default)]
pub struct TopologyPartition {
    nodes: FxHashMap<ShardId, TopologyNode>,
}

impl TopologyPartition {
    pub fn new() -> Self { Self::default() }

    /// Update a shard's topology state from a pressure snapshot.
    ///
    /// Returns `(old_health, new_health)` so the caller can detect transitions
    /// and emit telemetry only when health changes.
    pub fn update(&mut self, shard_id: ShardId, pressure: &ShardPressure) -> (i8, i8) {
        let old = self.nodes.get(&shard_id)
            .map(|n| n.health)
            .unwrap_or(HEALTH_NEG);
        let node = TopologyNode::from_pressure(pressure);
        let new  = node.health;
        self.nodes.insert(shard_id, node);
        (old, new)
    }

    /// Current health of a shard. Returns `Neg` if not yet registered.
    pub fn health(&self, shard_id: ShardId) -> i8 {
        self.nodes.get(&shard_id).map(|n| n.health).unwrap_or(HEALTH_NEG)
    }

    /// Snapshot of a shard's node, if registered.
    pub fn node(&self, shard_id: ShardId) -> Option<&TopologyNode> {
        self.nodes.get(&shard_id)
    }

    /// Placement hint: the healthy shard nearest to `requester_addr`.
    ///
    /// Considers only shards registered in both the topology partition (have
    /// received at least one pressure update) and the domain registry (have a
    /// known `fabric_addr`). Returns `None` if no healthy candidate exists.
    pub fn best_shard_for(
        &self,
        requester_addr: &ShardFabricAddr,
        registry:       &DomainRegistry,
    ) -> Option<ShardId> {
        self.nodes.iter()
            .filter(|(_, node)| node.is_healthy())
            .filter_map(|(shard_id, _)| {
                let domain = registry.shards.get(shard_id)?;
                let addr   = domain.fabric_addr.as_ref()?;
                let dist   = requester_addr.hop_distance(addr);
                Some((*shard_id, dist))
            })
            .min_by_key(|(_, dist)| *dist)
            .map(|(shard_id, _)| shard_id)
    }

    pub fn len(&self)      -> usize { self.nodes.len()     }
    pub fn is_empty(&self) -> bool  { self.nodes.is_empty() }
}

// ── TopologyNodeName ──────────────────────────────────────────────────────────

/// Canonical node name builders for the topology partition.
pub struct TopologyNodeName;

impl TopologyNodeName {
    /// `shard::{id}::fabric` — primary shard node in the topology graph.
    pub fn shard(shard_id: ShardId) -> String {
        format!("shard::{}::fabric", shard_id.0)
    }

    /// `shard::{id}::locality_radius` — observable hop radius telemetry node.
    pub fn locality_radius(shard_id: ShardId) -> String {
        format!("shard::{}::locality_radius", shard_id.0)
    }

    /// `shard::{id}::placement_coords` — fabric address as observable telemetry node.
    pub fn placement_coords(shard_id: ShardId) -> String {
        format!("shard::{}::placement_coords", shard_id.0)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admission::ShardPressure,
        domain::{ShardDomain, ShardFabricAddr},
    };

    fn pressure(depth: u32, cursors: u32) -> ShardPressure {
        ShardPressure { queue_depth: depth, cursor_count: cursors, budget_consumed: 0 }
    }

    fn addr(region: u16, pod: u16, rack: u16, leaf: u16) -> ShardFabricAddr {
        ShardFabricAddr { region, pod, rack, fabric_leaf: leaf }
    }

    #[test]
    fn unknown_shard_is_neg() {
        let tp = TopologyPartition::new();
        assert_eq!(tp.health(ShardId(99)), HEALTH_NEG);
    }

    #[test]
    fn healthy_pressure_yields_pos() {
        let mut tp = TopologyPartition::new();
        let (_, new) = tp.update(ShardId(1), &pressure(0, 0));
        assert_eq!(new, HEALTH_POS);
        assert!(tp.node(ShardId(1)).unwrap().is_healthy());
    }

    #[test]
    fn congested_queue_yields_zero() {
        let mut tp = TopologyPartition::new();
        let (_, new) = tp.update(ShardId(1), &pressure(CONGESTED_QUEUE_DEPTH, 0));
        assert_eq!(new, HEALTH_ZERO);
    }

    #[test]
    fn parked_cursor_yields_neg() {
        let mut tp = TopologyPartition::new();
        let (_, new) = tp.update(ShardId(1), &pressure(0, 1));
        assert_eq!(new, HEALTH_NEG);
    }

    #[test]
    fn cursor_takes_priority_over_queue() {
        // A shard with both high queue depth AND parked cursors → Neg (not Zero).
        let mut tp = TopologyPartition::new();
        let (_, new) = tp.update(ShardId(1), &pressure(CONGESTED_QUEUE_DEPTH, 1));
        assert_eq!(new, HEALTH_NEG);
    }

    #[test]
    fn update_returns_old_and_new_health() {
        let mut tp = TopologyPartition::new();
        // First update: old = Neg (unknown), new = Pos
        let (old, new) = tp.update(ShardId(1), &pressure(0, 0));
        assert_eq!(old, HEALTH_NEG);
        assert_eq!(new, HEALTH_POS);
        // Second update: old = Pos, new = Zero
        let (old2, new2) = tp.update(ShardId(1), &pressure(CONGESTED_QUEUE_DEPTH, 0));
        assert_eq!(old2, HEALTH_POS);
        assert_eq!(new2, HEALTH_ZERO);
    }

    #[test]
    fn best_shard_for_prefers_nearest_healthy() {
        let mut tp  = TopologyPartition::new();
        let mut reg = DomainRegistry::new();

        // Shard 1: healthy, same rack as requester (hop=2)
        tp.update(ShardId(1), &pressure(0, 0));
        reg.shards.insert(ShardId(1), ShardDomain {
            id: ShardId(1), fabric_addr: Some(addr(0, 0, 0, 1)),
        });

        // Shard 2: healthy, same region (hop=8)
        tp.update(ShardId(2), &pressure(0, 0));
        reg.shards.insert(ShardId(2), ShardDomain {
            id: ShardId(2), fabric_addr: Some(addr(0, 1, 0, 0)),
        });

        // Shard 3: congested (Zero) — should not be selected
        tp.update(ShardId(3), &pressure(CONGESTED_QUEUE_DEPTH, 0));
        reg.shards.insert(ShardId(3), ShardDomain {
            id: ShardId(3), fabric_addr: Some(addr(0, 0, 0, 2)),
        });

        let requester = addr(0, 0, 0, 0);
        assert_eq!(tp.best_shard_for(&requester, &reg), Some(ShardId(1)));
    }

    #[test]
    fn best_shard_for_no_healthy_returns_none() {
        let mut tp  = TopologyPartition::new();
        let mut reg = DomainRegistry::new();
        tp.update(ShardId(1), &pressure(0, 1)); // Neg
        reg.shards.insert(ShardId(1), ShardDomain {
            id: ShardId(1), fabric_addr: Some(addr(0, 0, 0, 1)),
        });
        assert_eq!(tp.best_shard_for(&addr(0, 0, 0, 0), &reg), None);
    }

    #[test]
    fn node_names_have_correct_prefixes() {
        let name = TopologyNodeName::shard(ShardId(7));
        assert!(name.starts_with("shard::7::fabric"), "{}", name);

        let name = TopologyNodeName::locality_radius(ShardId(3));
        assert!(name.contains("locality_radius"), "{}", name);

        let name = TopologyNodeName::placement_coords(ShardId(5));
        assert!(name.contains("placement_coords"), "{}", name);
    }
}
