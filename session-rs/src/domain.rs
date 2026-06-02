//! Domain hierarchy: tenant → session → partition → shard.
//!
//! Each level in the hierarchy owns distinct guarantees:
//!
//! | Level     | Owns                                                        |
//! |-----------|-------------------------------------------------------------|
//! | Tenant    | Capability ceiling, aggregate quota, replay audit root      |
//! | Session   | Stream continuity, auth mode, backpressure scope            |
//! | Partition | Causal isolation, PartitionAuthTable scope, shard assignment|
//! | Shard     | Engine instance, quiescence guarantee, WorkCursor budget    |
//!
//! ## Capability inheritance
//!
//! Authority is strictly bounded downward through the hierarchy:
//! ```text
//! tenant.capabilities
//!   ⊇ session.effective_caps   (= min(claimed, tenant.capabilities))
//!     ⊇ partition.capability_mask
//!       ⊇ edge.label.capability_bits
//! ```
//!
//! A child domain cannot hold capabilities its parent does not have.
//! `DomainRegistry::effective_caps` enforces this at the session level.

use rustc_hash::FxHashMap;
use pgress_core::partition::{AuthorityMode, CapabilityBits, LatticeClass};
use crate::{SessionId, ShardId, TenantId, WirePartitionId};

// ── ShardFabricAddr ───────────────────────────────────────────────────────────

/// Physical fabric address for an engine shard.
///
/// Encodes position in the network hierarchy (coarsest → finest):
/// `region → pod → rack → fabric_leaf`.
///
/// All fields are opaque administrator-assigned identifiers within their level.
/// Two shards with equal values at a given level are co-located at that boundary.
/// The address is used to:
/// - Compute `locality_class` for `SetEdgeLabel` on the topology partition
/// - Compute `hop_distance` for placement scoring
/// - Derive `FabricScope` for telemetry observability
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShardFabricAddr {
    pub region:      u16,
    pub pod:         u16,
    pub rack:        u16,
    pub fabric_leaf: u16,
}

/// Scope level describing how two fabric addresses relate in the hierarchy.
///
/// Ordered from finest (cheapest) to coarsest (most expensive):
/// `Leaf < Rack < Pod < Region < Remote`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FabricScope {
    /// Same fabric leaf — adjacent tiles, minimal hop cost.
    Leaf   = 0,
    /// Same rack, different leaf.
    Rack   = 1,
    /// Same pod, different rack.
    Pod    = 2,
    /// Same region, different pod.
    Region = 3,
    /// Different region — cross-package or off-wafer.
    Remote = 4,
}

impl ShardFabricAddr {
    /// Scope level relative to `other`.
    pub fn scope(&self, other: &ShardFabricAddr) -> FabricScope {
        if self.region      != other.region      { return FabricScope::Remote; }
        if self.pod         != other.pod         { return FabricScope::Region; }
        if self.rack        != other.rack        { return FabricScope::Pod;    }
        if self.fabric_leaf != other.fabric_leaf { return FabricScope::Rack;   }
        FabricScope::Leaf
    }

    /// Abstract hop count to `other`.
    ///
    /// Doubles at each hierarchy boundary:
    /// `Leaf=1, Rack=2, Pod=4, Region=8, Remote=16`.
    pub fn hop_distance(&self, other: &ShardFabricAddr) -> u32 {
        match self.scope(other) {
            FabricScope::Leaf   => 1,
            FabricScope::Rack   => 2,
            FabricScope::Pod    => 4,
            FabricScope::Region => 8,
            FabricScope::Remote => 16,
        }
    }

    /// `LatticeClass` for a topology edge between `self` and `other`.
    ///
    /// Encodes physical proximity for use with `SetEdgeLabel` on the topology
    /// partition. Lower class = cheaper / closer link.
    pub fn locality_class(&self, other: &ShardFabricAddr) -> LatticeClass {
        match self.scope(other) {
            FabricScope::Leaf   => LatticeClass(0b0001),
            FabricScope::Rack   => LatticeClass(0b0011),
            FabricScope::Pod    => LatticeClass(0b0111),
            FabricScope::Region => LatticeClass(0b1111),
            FabricScope::Remote => LatticeClass::TOP,
        }
    }
}

// ── Quota types ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct TenantQuota {
    /// Maximum concurrent sessions under this tenant.
    pub max_sessions: u32,
    /// Aggregate record admission rate (records/sec) across all sessions.
    pub max_records_per_sec: u32,
}

impl Default for TenantQuota {
    fn default() -> Self {
        TenantQuota { max_sessions: 1_024, max_records_per_sec: 100_000 }
    }
}

#[derive(Clone, Debug)]
pub struct SessionQuota {
    /// Maximum records in-flight (admitted but not yet engine-applied).
    pub max_inflight: u32,
    /// Maximum partitions this session may address.
    pub max_partitions: u32,
}

impl Default for SessionQuota {
    fn default() -> Self {
        SessionQuota { max_inflight: 4_096, max_partitions: 256 }
    }
}

// ── Domain types ──────────────────────────────────────────────────────────────

/// Outermost isolation boundary. Sets the capability ceiling for all child sessions.
#[derive(Clone, Debug)]
pub struct TenantDomain {
    pub id:           TenantId,
    /// Hard ceiling on capabilities for all sessions under this tenant.
    /// Child sessions cannot claim capabilities the tenant does not hold.
    pub capabilities: CapabilityBits,
    pub quota:        TenantQuota,
}

impl TenantDomain {
    /// Default single-tenant domain: all capabilities, generous quota.
    pub fn single_tenant() -> Self {
        TenantDomain {
            id:           TenantId(0),
            capabilities: CapabilityBits::ALL,
            quota:        TenantQuota::default(),
        }
    }
}

/// Transient connection context. Stable logical identity across path migrations.
#[derive(Clone, Debug)]
pub struct SessionDomain {
    pub id:           SessionId,
    pub tenant_id:    TenantId,
    /// Capability claimed by this session (e.g. from SessionProfile).
    /// Effective capability = min(claimed_caps, tenant.capabilities).
    pub claimed_caps: CapabilityBits,
    pub auth_mode:    AuthorityMode,
    pub quota:        SessionQuota,
}

/// Causal isolation unit. Owns PartitionAuthTable scope and shard assignment.
#[derive(Clone, Debug)]
pub struct PartitionDomain {
    pub id:            WirePartitionId,
    pub session_id:    SessionId,
    pub lattice_class: LatticeClass,
    pub causal_scope:  u64,
    pub shard_id:      ShardId,
}

/// Physical execution unit. Owns engine instance and quiescence guarantee.
#[derive(Clone, Debug)]
pub struct ShardDomain {
    pub id:          ShardId,
    /// Physical fabric address. `None` if the shard has no topology-aware placement
    /// (e.g. single-node deployments or shards added before topology registration).
    pub fabric_addr: Option<ShardFabricAddr>,
}

// ── DomainRegistry ────────────────────────────────────────────────────────────

/// Registry of all domains in the hierarchy.
///
/// The registry enforces capability inheritance: `effective_caps` returns
/// `min(session.claimed_caps, tenant.capabilities)`, never the raw claimed value.
#[derive(Clone, Debug, Default)]
pub struct DomainRegistry {
    pub tenants:    FxHashMap<TenantId, TenantDomain>,
    pub sessions:   FxHashMap<SessionId, SessionDomain>,
    pub partitions: FxHashMap<WirePartitionId, PartitionDomain>,
    pub shards:     FxHashMap<ShardId, ShardDomain>,
}

impl DomainRegistry {
    pub fn new() -> Self { Self::default() }

    pub fn register_tenant(&mut self, t: TenantDomain) {
        self.tenants.insert(t.id, t);
    }

    pub fn register_session(&mut self, s: SessionDomain) {
        self.sessions.insert(s.id, s);
    }

    pub fn register_partition(&mut self, p: PartitionDomain) {
        self.partitions.insert(p.id, p);
    }

    pub fn register_shard(&mut self, s: ShardDomain) {
        self.shards.insert(s.id, s);
    }

    /// Effective capabilities for a session.
    ///
    /// Returns `min(session.claimed_caps, tenant.capabilities)` — the child
    /// can never exceed the parent ceiling.
    /// Returns `None` if the session or its parent tenant is not registered.
    pub fn effective_caps(&self, session_id: SessionId) -> Option<CapabilityBits> {
        let session = self.sessions.get(&session_id)?;
        let tenant  = self.tenants.get(&session.tenant_id)?;
        Some(CapabilityBits(session.claimed_caps.0 & tenant.capabilities.0))
    }

    /// True if the session holds `required` capabilities after applying the
    /// parent ceiling. False if session or tenant not found.
    pub fn session_allows(&self, session_id: SessionId, required: CapabilityBits) -> bool {
        self.effective_caps(session_id)
            .map(|eff| eff.allows(required))
            .unwrap_or(false)
    }

    /// `LatticeClass` for a topology edge between two shards.
    ///
    /// Returns `None` if either shard is not registered or has no `fabric_addr`.
    pub fn locality_class(&self, a: ShardId, b: ShardId) -> Option<LatticeClass> {
        let addr_a = self.shards.get(&a)?.fabric_addr.as_ref()?;
        let addr_b = self.shards.get(&b)?.fabric_addr.as_ref()?;
        Some(addr_a.locality_class(addr_b))
    }

    /// Abstract hop distance between two shards.
    ///
    /// Returns `None` if either shard is not registered or has no `fabric_addr`.
    pub fn hop_distance(&self, a: ShardId, b: ShardId) -> Option<u32> {
        let addr_a = self.shards.get(&a)?.fabric_addr.as_ref()?;
        let addr_b = self.shards.get(&b)?.fabric_addr.as_ref()?;
        Some(addr_a.hop_distance(addr_b))
    }

    /// Locality radius for a shard: the maximum hop distance to any other
    /// registered shard that has a known `fabric_addr`.
    ///
    /// Returns `None` if the shard is not registered or has no `fabric_addr`.
    /// Returns `Some(0)` if no other addressed shards are registered.
    pub fn locality_radius(&self, shard: ShardId) -> Option<u32> {
        let addr = self.shards.get(&shard)?.fabric_addr.as_ref()?;
        let radius = self.shards.values()
            .filter(|s| s.id != shard)
            .filter_map(|s| s.fabric_addr.as_ref())
            .map(|other| addr.hop_distance(other))
            .max()
            .unwrap_or(0);
        Some(radius)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_registry() -> DomainRegistry {
        let mut r = DomainRegistry::new();
        r.register_tenant(TenantDomain {
            id:           TenantId(1),
            capabilities: CapabilityBits::READ | CapabilityBits::PROPAGATE,
            quota:        TenantQuota::default(),
        });
        r.register_session(SessionDomain {
            id:           SessionId(10),
            tenant_id:    TenantId(1),
            claimed_caps: CapabilityBits::ALL,   // claims everything
            auth_mode:    AuthorityMode::Advisory,
            quota:        SessionQuota::default(),
        });
        r
    }

    #[test]
    fn effective_caps_clips_to_parent() {
        let r = make_registry();
        // Session claims ALL but tenant only grants READ | PROPAGATE
        let eff = r.effective_caps(SessionId(10)).unwrap();
        let expected = CapabilityBits::READ | CapabilityBits::PROPAGATE;
        assert_eq!(eff, expected);
    }

    #[test]
    fn effective_caps_missing_session_returns_none() {
        let r = make_registry();
        assert!(r.effective_caps(SessionId(999)).is_none());
    }

    #[test]
    fn effective_caps_missing_tenant_returns_none() {
        let mut r = DomainRegistry::new();
        // Register session whose tenant does not exist
        r.register_session(SessionDomain {
            id:           SessionId(5),
            tenant_id:    TenantId(99),  // no such tenant
            claimed_caps: CapabilityBits::ALL,
            auth_mode:    AuthorityMode::Advisory,
            quota:        SessionQuota::default(),
        });
        assert!(r.effective_caps(SessionId(5)).is_none());
    }

    #[test]
    fn session_allows_read_within_parent_ceiling() {
        let r = make_registry();
        assert!(r.session_allows(SessionId(10), CapabilityBits::READ));
    }

    #[test]
    fn session_disallows_stabilize_outside_parent_ceiling() {
        let r = make_registry();
        // Tenant does not grant STABILIZE
        assert!(!r.session_allows(SessionId(10), CapabilityBits::STABILIZE));
    }

    // ── ShardFabricAddr tests ─────────────────────────────────────────────────

    fn addr(region: u16, pod: u16, rack: u16, leaf: u16) -> ShardFabricAddr {
        ShardFabricAddr { region, pod, rack, fabric_leaf: leaf }
    }

    #[test]
    fn fabric_scope_same_leaf() {
        let a = addr(1, 1, 1, 1);
        assert_eq!(a.scope(&addr(1, 1, 1, 1)), FabricScope::Leaf);
    }

    #[test]
    fn fabric_scope_same_rack_different_leaf() {
        let a = addr(1, 1, 1, 1);
        assert_eq!(a.scope(&addr(1, 1, 1, 2)), FabricScope::Rack);
    }

    #[test]
    fn fabric_scope_same_pod_different_rack() {
        assert_eq!(addr(1,1,1,1).scope(&addr(1,1,2,1)), FabricScope::Pod);
    }

    #[test]
    fn fabric_scope_same_region_different_pod() {
        assert_eq!(addr(1,1,1,1).scope(&addr(1,2,1,1)), FabricScope::Region);
    }

    #[test]
    fn fabric_scope_remote() {
        assert_eq!(addr(1,1,1,1).scope(&addr(2,1,1,1)), FabricScope::Remote);
    }

    #[test]
    fn hop_distance_doubles_at_each_boundary() {
        let origin = addr(0, 0, 0, 0);
        assert_eq!(origin.hop_distance(&addr(0, 0, 0, 1)),  2);  // Rack
        assert_eq!(origin.hop_distance(&addr(0, 0, 1, 0)),  4);  // Pod
        assert_eq!(origin.hop_distance(&addr(0, 1, 0, 0)),  8);  // Region
        assert_eq!(origin.hop_distance(&addr(1, 0, 0, 0)), 16);  // Remote
    }

    #[test]
    fn locality_class_encodes_proximity() {
        let origin = addr(0, 0, 0, 0);
        assert_eq!(origin.locality_class(&addr(0, 0, 0, 1)), LatticeClass(0b0011)); // Rack
        assert_eq!(origin.locality_class(&addr(0, 0, 1, 0)), LatticeClass(0b0111)); // Pod
        assert_eq!(origin.locality_class(&addr(0, 1, 0, 0)), LatticeClass(0b1111)); // Region
        assert_eq!(origin.locality_class(&addr(1, 0, 0, 0)), LatticeClass::TOP);    // Remote
    }

    #[test]
    fn locality_radius_is_max_hop_distance() {
        let mut r = DomainRegistry::new();
        r.shards.insert(ShardId(1), ShardDomain { id: ShardId(1), fabric_addr: Some(addr(0,0,0,0)) });
        r.shards.insert(ShardId(2), ShardDomain { id: ShardId(2), fabric_addr: Some(addr(0,0,0,1)) }); // Rack
        r.shards.insert(ShardId(3), ShardDomain { id: ShardId(3), fabric_addr: Some(addr(0,1,0,0)) }); // Region
        // Radius from shard 1: max hop to shard 2 (Rack=2) or shard 3 (Region=8) → 8
        assert_eq!(r.locality_radius(ShardId(1)), Some(8));
    }

    #[test]
    fn locality_radius_no_fabric_addr_returns_none() {
        let mut r = DomainRegistry::new();
        r.shards.insert(ShardId(1), ShardDomain { id: ShardId(1), fabric_addr: None });
        assert_eq!(r.locality_radius(ShardId(1)), None);
    }

    #[test]
    fn single_tenant_default_has_all_caps() {
        let mut r = DomainRegistry::new();
        let tenant = TenantDomain::single_tenant();
        r.register_tenant(tenant);
        r.register_session(SessionDomain {
            id:           SessionId(1),
            tenant_id:    TenantId(0),
            claimed_caps: CapabilityBits::READ,
            auth_mode:    AuthorityMode::Advisory,
            quota:        SessionQuota::default(),
        });
        // Effective = min(READ, ALL) = READ
        assert_eq!(r.effective_caps(SessionId(1)), Some(CapabilityBits::READ));
    }
}
