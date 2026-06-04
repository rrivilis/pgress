//! Deterministic in-process simulation harness for multi-shard session manager testing.
//!
//! Inspired by TigerBeetle's Simulator: all "nodes" (shards) run in a single process,
//! network I/O is replaced by an explicit message bus with fault injection, and
//! execution is driven by deterministic `tick()` calls seeded by a u64 value.
//! The same seed produces identical telemetry sequences, making failures
//! fully reproducible.
//!
//! ## Architecture
//!
//! ```text
//! SimCluster
//!   ├── shards:  FxHashMap<ShardId, SessionRuntime>   (one runtime per shard)
//!   ├── network: SimNetwork                           (topology gossip + remote dep msgs)
//!   └── rng:     Lcg64                                (seeded, deterministic)
//! ```
//!
//! ## What the sim models
//!
//! - **Topology gossip**: each `tick()` delivers pending pressure updates to all
//!   shards' topology views, simulating background gossip propagation.
//! - **Shard crash / heal**: `crash_shard` marks a shard as offline and injects
//!   `HEALTH_NEG` pressure; `heal_shard` re-registers with `HEALTH_POS` pressure.
//! - **Network partition**: `partition_shards` drops gossip between two shard sets.
//! - **Message delay**: messages can be held back for N ticks.
//!
//! ## What the sim does NOT model
//!
//! - Engine execution (no actual ternary propagation — this tests the session manager
//!   layer only: routing, placement, health tracking, expiry).
//! - ASSERTED/ATTESTED signature verification (Advisory trust throughout).
//!
//! ## Consistency model: AP with interpretation-monotone quotient
//!
//! pgress is **not** monotone in the CALM sense over event history or values.
//! A cell may go Neg → Pos → Zero; the value lattice is not information-monotone.
//! The invariant operates one level higher, over the **filtered interpretation order**:
//!
//! ### Why not CALM
//!
//! CALM's monotonicity is a statement about the *content of the store* — facts are
//! added but never retracted, so reads are coordination-free. pgress values do not
//! satisfy this. The relevant monotone structure is instead:
//!
//! 1. **Region lowering + topology** fix an observable region — the set of shards in
//!    scope, with their fabric addresses and health states establishing the physical
//!    boundary within which propagation is evaluated.
//!
//! 2. **Projection + sensitivity** define a **quotient hierarchy by distinguishability
//!    rank**: two states are equivalent iff no observer at that sensitivity level can
//!    distinguish them (Leibniz equality over observables, not over values). Coarser
//!    projections induce coarser quotients; finer sensitivity induces finer classes.
//!
//! 3. **Stabilization** (`Stabilize` op; hardware ICG) drives the region toward the
//!    *coarsest e-graph quotient* consistent with its observable constraints. The region
//!    quiesces when no new equivalence classes can be merged — the ICG is the physical
//!    fixed-point detector for this saturation process.
//!
//! 4. **Conflict between local monotone propagation and non-monotone distinction** is
//!    *lifted*: when two propagation paths produce incompatible results, the contradiction
//!    is not resolved by choosing one — it becomes `Zero` (Bochvar indeterminate), the
//!    lattice element that represents frustration at this distinguishability rank. `Zero`
//!    is the fixed point for that cell; it is the e-class containing all expressions that
//!    collide under the current quotient.
//!
//! ### AP, not CAP
//!
//! The system is **Available** (always returns a response, possibly `Zero`) and
//! **Partition-tolerant** (partitions surface as `Zero`, not halts). Consistency in the
//! CAP sense would require a canonical global normal form — but the global structure is
//! **non-confluent**: different regions can independently stabilize to different equivalence
//! classes, and there is no canonical global rewrite that resolves them. Church-Rosser
//! fails globally; it holds only locally per region.
//!
//! The "at least AP" bound is exact: for purely monotone subgraphs (no `Zero` states),
//! coordination-free consistency follows directly — reads are CALM-equivalent. For
//! non-monotone subgraphs, `Zero` is the available answer under partition; it is the
//! interpretation-class assigned when distinguishability collapses.
//!
//! ### Non-confluence implication for tests
//!
//! Non-confluence means different event orderings (gossip delivery sequences, op arrival
//! orders) produce different intermediate states. The correct assertion is not that two
//! runs with different orderings produce identical value sequences — it is that they
//! converge to the same **observable equivalence class**. The test
//! `convergence_under_different_gossip_orderings` demonstrates this: two clusters with
//! different delay policies traverse non-canonical intermediate paths, but their final
//! health classifications (which shards are Pos/Zero/Neg as seen by which observers)
//! agree. The intermediate states may differ; the interpretation class does not.

pub mod network;
pub mod faults;
pub mod workload;

use rustc_hash::FxHashMap;
use crate::{
    ShardId, SessionId, PathId, TenantId,
    admission::ShardPressure,
    domain::{ShardDomain, ShardFabricAddr, SessionDomain, TenantDomain, SessionQuota},
    session::{ExpiryPolicy, PathEntry, PathState, ReapResult, SessionEntry},
    telemetry::TelemetryEvent,
    topology::{HEALTH_POS, HEALTH_ZERO},
    runtime::SessionRuntime,
};
use network::{SimNetwork, PendingMsg};
use pgress_core::partition::AuthorityMode;
use crate::auth::AuthorityPolicy;

// ── Lcg64 — minimal seeded deterministic RNG ──────────────────────────────────

/// 64-bit linear congruential generator.
/// Sufficient for fault injection ordering — not cryptographic.
#[derive(Clone, Debug)]
pub struct Lcg64 {
    state: u64,
}

impl Lcg64 {
    pub fn new(seed: u64) -> Self { Lcg64 { state: seed.wrapping_add(1) } }

    pub fn next_u64(&mut self) -> u64 {
        // Knuth's LCG constants
        self.state = self.state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    /// Uniform float in [0, 1).
    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 33) as f32 / (1u64 << 31) as f32
    }

    /// Uniform usize in [0, n).
    pub fn next_usize_below(&mut self, n: usize) -> usize {
        (self.next_u64() as usize) % n
    }
}

// ── ShardState — per-shard lifecycle ─────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardLifecycle {
    /// Fully operational.
    Alive,
    /// Simulated crash — not accepting new records; topology marks it Neg.
    Crashed,
    /// Recovering — restarted but pressure not yet propagated; topology still Neg.
    Recovering,
}

// ── SimCluster ────────────────────────────────────────────────────────────────

/// Multi-shard simulation cluster.
///
/// Each shard is a full `SessionRuntime`. The cluster drives topology gossip,
/// fault injection, and expiry via explicit `tick()` calls.
pub struct SimCluster {
    /// One `SessionRuntime` per shard, keyed by `ShardId`.
    pub shards:    FxHashMap<ShardId, SessionRuntime>,
    pub lifecycle: FxHashMap<ShardId, ShardLifecycle>,
    pub network:   SimNetwork,
    pub rng:       Lcg64,
    /// Monotonically increasing simulated causal epoch.
    pub epoch:     u64,
    /// Tick count since cluster creation.
    pub tick:      u64,
}

impl SimCluster {
    /// Create a cluster with `n` shards pre-registered, all `Alive` and `HEALTH_POS`.
    ///
    /// Shards are assigned fabric addresses within the same rack for minimal hop distance.
    /// The single-tenant sentinel (`TenantId(0)`) is registered on every shard.
    pub fn new(n: usize, seed: u64) -> Self {
        let mut shards    = FxHashMap::default();
        let mut lifecycle = FxHashMap::default();

        // First pass: create all runtimes with self-pressure only
        for i in 0..n {
            let shard_id = ShardId(i as u64);
            let mut rt   = SessionRuntime::new();

            // Register single-tenant sentinel
            rt.domains.register_tenant(TenantDomain::single_tenant());

            // Register shard with fabric address (all same rack, different leaves)
            let addr = ShardFabricAddr { region: 0, pod: 0, rack: 0, fabric_leaf: i as u16 };
            rt.domains.shards.insert(shard_id, ShardDomain {
                id: shard_id, fabric_addr: Some(addr),
            });

            // Bootstrap own pressure as healthy
            let healthy = ShardPressure { queue_depth: 0, cursor_count: 0, budget_consumed: 0 };
            rt.update_pressure(shard_id, healthy);

            shards.insert(shard_id, rt);
            lifecycle.insert(shard_id, ShardLifecycle::Alive);
        }

        // Second pass: cross-populate healthy pressure so every shard sees every
        // other shard as HEALTH_POS from the start (simulates completed initial gossip).
        let healthy = ShardPressure { queue_depth: 0, cursor_count: 0, budget_consumed: 0 };
        let ids: Vec<ShardId> = shards.keys().copied().collect();
        for &dst in &ids {
            for &src in &ids {
                if src == dst { continue; }
                shards.get_mut(&dst).unwrap().update_pressure(src, healthy);
            }
        }
        // Drain the bootstrap telemetry so tests start with a clean event stream
        for rt in shards.values_mut() {
            rt.telemetry.drain();
        }

        SimCluster {
            shards,
            lifecycle,
            network: SimNetwork::new(),
            rng:     Lcg64::new(seed),
            epoch:   1,
            tick:    0,
        }
    }

    // ── Tick ──────────────────────────────────────────────────────────────────

    /// Advance the simulation by one step.
    ///
    /// Per tick:
    /// 1. Deliver any pending topology gossip messages (respecting fault policy).
    /// 2. Advance the causal epoch.
    /// 3. Process any scheduled fault events (crashes, heals).
    pub fn tick_once(&mut self) {
        self.tick  += 1;
        self.epoch += 1;

        // Deliver pending gossip messages
        let msgs = self.network.drain_due(self.tick);
        for msg in msgs {
            self.deliver(msg);
        }

        // Gossip each alive shard's pressure to all other alive shards
        let alive_shards: Vec<ShardId> = self.shards.keys()
            .copied()
            .filter(|id| self.lifecycle.get(id) == Some(&ShardLifecycle::Alive))
            .collect();

        for &src in &alive_shards {
            if let Some(pressure) = self.shards[&src].pressure.get(&src).copied() {
                for &dst in &alive_shards {
                    if dst == src { continue; }
                    if self.network.faults.is_partitioned(src, dst) { continue; }
                    // Probabilistic drop based on fault policy
                    let drop_rate = self.network.faults.drop_rate;
                    if drop_rate > 0.0 && self.rng.next_f32() < drop_rate { continue; }
                    let delay = self.network.faults.sample_delay_ticks(&mut self.rng);
                    let deliver_at = self.tick + delay as u64;
                    self.network.enqueue(PendingMsg::TopologyGossip {
                        src, dst, pressure, deliver_at,
                    });
                }
            }
        }
    }

    /// Advance by `n` ticks.
    pub fn tick(&mut self, n: u64) {
        for _ in 0..n { self.tick_once(); }
    }

    fn deliver(&mut self, msg: PendingMsg) {
        match msg {
            PendingMsg::TopologyGossip { src, dst, pressure, .. } => {
                if let Some(rt) = self.shards.get_mut(&dst) {
                    rt.update_pressure(src, pressure);
                }
            }
        }
    }

    // ── Fault injection ───────────────────────────────────────────────────────

    /// Simulate a shard crash.
    ///
    /// The crashed shard stops gossiping its pressure. All other shards will see
    /// stale Pos until the gossip timeout — to fast-forward, call
    /// `inject_neg_pressure_everywhere(shard_id)` after crashing.
    pub fn crash_shard(&mut self, shard_id: ShardId) {
        self.lifecycle.insert(shard_id, ShardLifecycle::Crashed);
        // Immediately inject Neg into all other shards' topology views
        self.inject_neg_everywhere(shard_id);
    }

    /// Inject `HEALTH_NEG` pressure for `shard_id` into every shard's topology.
    ///
    /// Models a crash detection signal propagating instantly (e.g. lease expiry,
    /// heartbeat timeout). In production this would take O(gossip_interval) ticks.
    pub fn inject_neg_everywhere(&mut self, shard_id: ShardId) {
        let neg_pressure = ShardPressure { queue_depth: 0, cursor_count: 1, budget_consumed: 0 };
        let shard_ids: Vec<ShardId> = self.shards.keys().copied().collect();
        for id in shard_ids {
            if id == shard_id { continue; }
            if let Some(rt) = self.shards.get_mut(&id) {
                rt.update_pressure(shard_id, neg_pressure);
            }
        }
        // Update the crashed shard's own view of itself too
        if let Some(rt) = self.shards.get_mut(&shard_id) {
            rt.update_pressure(shard_id, neg_pressure);
        }
    }

    /// Simulate shard recovery — re-registers healthy pressure on all shards.
    pub fn heal_shard(&mut self, shard_id: ShardId) {
        self.lifecycle.insert(shard_id, ShardLifecycle::Alive);
        let healthy = ShardPressure { queue_depth: 0, cursor_count: 0, budget_consumed: 0 };
        let shard_ids: Vec<ShardId> = self.shards.keys().copied().collect();
        for &id in &shard_ids {
            if let Some(rt) = self.shards.get_mut(&id) {
                rt.update_pressure(shard_id, healthy);
            }
        }
    }

    /// Partition network between two sets of shards (bidirectional drop).
    pub fn partition_network(&mut self, set_a: &[ShardId], set_b: &[ShardId]) {
        for &a in set_a {
            for &b in set_b {
                self.network.faults.add_partition(a, b);
            }
        }
    }

    /// Heal a network partition between two sets.
    pub fn heal_network_partition(&mut self, set_a: &[ShardId], set_b: &[ShardId]) {
        for &a in set_a {
            for &b in set_b {
                self.network.faults.remove_partition(a, b);
            }
        }
    }

    // ── Session helpers ───────────────────────────────────────────────────────

    /// Register a session on all shards (so any shard can route it).
    pub fn register_session_everywhere(
        &mut self,
        session_id: SessionId,
        path_id:    PathId,
    ) {
        let shard_ids: Vec<ShardId> = self.shards.keys().copied().collect();
        for id in shard_ids {
            let rt = self.shards.get_mut(&id).unwrap();
            rt.domains.register_session(SessionDomain {
                id:             session_id,
                tenant_id:      TenantId(0),
                claimed_policy: AuthorityPolicy::ALL,
                auth_mode:      AuthorityMode::Advisory,
                quota:          SessionQuota::default(),
            });
            rt.sessions.create(SessionEntry {
                session_id,
                tenant_id:                    TenantId(0),
                active_path_id:               Some(path_id),
                prev_path_id:                 None,
                stream_seq_floor:             0,
                auth_mode:                    AuthorityMode::Advisory,
                last_active_causal_epoch:     0,
                consecutive_quiescent_epochs: 0,
            });
            rt.paths.create(PathEntry {
                path_id,
                session_id,
                last_ack_stream_seq: 0,
                state: PathState::Active,
            });
        }
    }

    // ── Observability queries ─────────────────────────────────────────────────

    /// Health of `target_shard` as seen by `observer_shard`.
    pub fn health_as_seen_by(&self, observer: ShardId, target: ShardId) -> i8 {
        self.shards[&observer].topology.health(target)
    }

    /// True if every alive shard sees `target` as `HEALTH_POS`.
    pub fn all_see_healthy(&self, target: ShardId) -> bool {
        self.shards.iter()
            .filter(|(&id, _)| id != target)
            .all(|(_, rt)| rt.topology.health(target) == HEALTH_POS)
    }

    /// True if no alive shard (other than target itself) has `HEALTH_POS` for target.
    pub fn none_see_pos(&self, target: ShardId) -> bool {
        self.shards.iter()
            .filter(|(&id, _)| id != target)
            .all(|(_, rt)| rt.topology.health(target) != HEALTH_POS)
    }

    /// Drain all telemetry events from a shard.
    pub fn drain_telemetry(&mut self, shard_id: ShardId) -> Vec<TelemetryEvent> {
        self.shards.get_mut(&shard_id)
            .map(|rt| rt.telemetry.drain())
            .unwrap_or_default()
    }

    /// Drain and collect only `TopologyHealthChanged` events from a shard.
    pub fn drain_topology_events(
        &mut self,
        shard_id: ShardId,
    ) -> Vec<(ShardId, i8, i8)> {
        self.drain_telemetry(shard_id)
            .into_iter()
            .filter_map(|ev| match ev {
                TelemetryEvent::TopologyHealthChanged { shard_id, old_health, new_health } =>
                    Some((shard_id, old_health, new_health)),
                _ => None,
            })
            .collect()
    }

    /// Run the session reaper across all shards with a given policy.
    pub fn reap_all(&mut self, policy: &ExpiryPolicy) -> FxHashMap<ShardId, ReapResult> {
        let epoch = self.epoch;
        self.shards.iter_mut()
            .map(|(&id, rt)| (id, rt.run_reaper(policy, epoch)))
            .collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{HEALTH_NEG, HEALTH_POS};

    // ── Safety: no false Pos after shard death ────────────────────────────────

    #[test]
    fn no_false_pos_after_shard_crash() {
        // 3-shard cluster. After shard 2 crashes, shards 0 and 1 must NOT
        // report HEALTH_POS for it.
        let mut cluster = SimCluster::new(3, 0xDEAD_BEEF);

        // All shards start healthy — verify baseline
        assert_eq!(cluster.health_as_seen_by(ShardId(0), ShardId(2)), HEALTH_POS);
        assert_eq!(cluster.health_as_seen_by(ShardId(1), ShardId(2)), HEALTH_POS);

        cluster.crash_shard(ShardId(2));

        // After crash, no other shard should see Pos for shard 2
        assert!(cluster.none_see_pos(ShardId(2)),
            "crashed shard 2 must not be visible as Pos to any peer");

        // The neg health must be precisely HEALTH_NEG
        assert_eq!(cluster.health_as_seen_by(ShardId(0), ShardId(2)), HEALTH_NEG);
        assert_eq!(cluster.health_as_seen_by(ShardId(1), ShardId(2)), HEALTH_NEG);
    }

    #[test]
    fn crash_emits_topology_health_changed_event() {
        let mut cluster = SimCluster::new(2, 0xCAFE);

        // Drain the initial bootstrap events so we start clean
        cluster.drain_telemetry(ShardId(0));

        // Now crash shard 1 — shard 0 should emit a TopologyHealthChanged event
        cluster.crash_shard(ShardId(1));
        let events = cluster.drain_topology_events(ShardId(0));

        assert!(!events.is_empty(), "expected topology health changed event");
        let (target, old_h, new_h) = events[0];
        assert_eq!(target, ShardId(1));
        assert_eq!(old_h, HEALTH_POS);
        assert_eq!(new_h, HEALTH_NEG);
    }

    // ── Liveness: healing converges to Pos ───────────────────────────────────

    #[test]
    fn healing_converges_to_pos() {
        let mut cluster = SimCluster::new(3, 0xBEEF_CAFE);

        cluster.crash_shard(ShardId(1));
        assert!(cluster.none_see_pos(ShardId(1)));

        cluster.heal_shard(ShardId(1));

        // After heal, all shards should see shard 1 as Pos again
        assert!(cluster.all_see_healthy(ShardId(1)),
            "healed shard 1 should be Pos everywhere");
    }

    #[test]
    fn heal_emits_pos_topology_event_on_all_shards() {
        let mut cluster = SimCluster::new(3, 0xF00D);

        cluster.crash_shard(ShardId(2));
        // Drain crash events
        for i in 0..3u64 { cluster.drain_telemetry(ShardId(i)); }

        cluster.heal_shard(ShardId(2));

        // All shards (other than shard 2 itself) should see a Neg→Pos transition
        for i in 0..2u64 {
            let events = cluster.drain_topology_events(ShardId(i));
            let healed = events.iter().any(|&(t, old_h, new_h)| {
                t == ShardId(2) && old_h == HEALTH_NEG && new_h == HEALTH_POS
            });
            assert!(healed, "shard {} should see shard 2 transition Neg→Pos", i);
        }
    }

    // ── Placement: pick_shard avoids crashed shards ──────────────────────────

    #[test]
    fn placement_skips_crashed_shard() {
        // 2-shard cluster. Crash shard 1. Shard 0 must route new partitions
        // only to itself (the last healthy shard).
        let mut cluster = SimCluster::new(2, 0x1234);

        cluster.crash_shard(ShardId(1));

        // any_healthy_shard on shard 0 should only return shard 0
        let healthy = cluster.shards[&ShardId(0)].topology.any_healthy_shard();
        assert_eq!(healthy, Some(ShardId(0)),
            "only shard 0 is healthy; shard 1 (crashed) must be excluded");
    }

    #[test]
    fn placement_prefers_lowest_id_on_tie() {
        // 3 equally healthy shards, no locality bias — should pick ShardId(0).
        let cluster = SimCluster::new(3, 0xABCD);
        let healthy = cluster.shards[&ShardId(0)].topology.any_healthy_shard();
        assert_eq!(healthy, Some(ShardId(0)));
    }

    // ── Network partition semantics ───────────────────────────────────────────

    #[test]
    fn network_partition_isolates_gossip() {
        // 3-shard cluster. Partition {shard 0} from {shard 1, shard 2}.
        // After ticking, shards 1 and 2 should NOT receive shard 0's gossip.
        let mut cluster = SimCluster::new(3, 0x5678);

        // First: crash shard 0 from within the partition so we have a health signal to test
        cluster.partition_network(&[ShardId(0)], &[ShardId(1), ShardId(2)]);

        // Inject a "healed" pressure from shard 0 on itself — normally this would
        // gossip out, but the network partition should prevent it reaching shards 1/2.
        // We test the inverse: crash shard 0 everywhere first, then heal and tick.
        cluster.inject_neg_everywhere(ShardId(0));
        assert_eq!(cluster.health_as_seen_by(ShardId(1), ShardId(0)), HEALTH_NEG);

        // Now heal shard 0 and tick — BUT the gossip should be dropped by the fault policy
        cluster.lifecycle.insert(ShardId(0), ShardLifecycle::Alive);
        let healthy = ShardPressure { queue_depth: 0, cursor_count: 0, budget_consumed: 0 };
        // Update shard 0's own pressure (would normally be gossiped out on tick)
        if let Some(rt) = cluster.shards.get_mut(&ShardId(0)) {
            rt.update_pressure(ShardId(0), healthy);
        }
        // Tick several times — gossip from shard 0 should be dropped by the partition
        cluster.tick(5);

        // Shards 1 and 2 should still see shard 0 as Neg (gossip never arrived)
        assert_eq!(cluster.health_as_seen_by(ShardId(1), ShardId(0)), HEALTH_NEG,
            "gossip from shard 0 should not reach shard 1 through the network partition");
        assert_eq!(cluster.health_as_seen_by(ShardId(2), ShardId(0)), HEALTH_NEG,
            "gossip from shard 0 should not reach shard 2 through the network partition");

        // After healing the network partition, shard 0's next gossip tick should reach peers
        cluster.heal_network_partition(&[ShardId(0)], &[ShardId(1), ShardId(2)]);
        cluster.tick(2);

        assert_eq!(cluster.health_as_seen_by(ShardId(1), ShardId(0)), HEALTH_POS,
            "after network heal, shard 0 gossip should propagate to shard 1");
    }

    // ── Determinism ──────────────────────────────────────────────────────────

    #[test]
    fn deterministic_replay_same_seed_same_health_sequence() {
        // Run cluster A and cluster B with the same seed through the same scenario.
        // They must produce identical health sequences.
        let run = |seed: u64| -> Vec<i8> {
            let mut cluster = SimCluster::new(4, seed);
            cluster.crash_shard(ShardId(2));
            cluster.tick(3);
            cluster.heal_shard(ShardId(2));
            cluster.tick(3);
            // Collect health of shard 2 as seen by shard 0 after each step
            (0..4u64)
                .map(|id| cluster.health_as_seen_by(ShardId(0), ShardId(id)))
                .collect()
        };

        assert_eq!(run(0xDEAD), run(0xDEAD),
            "same seed must produce identical health snapshot");
        // Different seeds may or may not differ (fault injection varies), but
        // the determinism property must hold within a seed.
    }

    // ── Session expiry in multi-shard context ─────────────────────────────────

    #[test]
    fn reaper_fires_only_on_stale_sessions() {
        let mut cluster = SimCluster::new(2, 0xAAAA);
        let active_sid  = SessionId(1);
        let stale_sid   = SessionId(2);

        cluster.register_session_everywhere(active_sid, PathId(10));
        cluster.register_session_everywhere(stale_sid,  PathId(20));

        // Advance epoch significantly, then record activity only for active session
        cluster.epoch = 100_000;
        for (_, rt) in cluster.shards.iter_mut() {
            rt.sessions.record_activity(active_sid, 99_999);
            // stale_sid: last_active stays at 0 (from construction)
        }

        let policy = ExpiryPolicy {
            inactivity_epoch_threshold: Some(1_000),
            epoch_lag_threshold:        Some(50_000),
            zero_frustration_threshold: None,
        };

        let results = cluster.reap_all(&policy);
        for (shard_id, result) in &results {
            let tombstoned_ids: Vec<SessionId> = result.tombstoned.iter().map(|(s, _)| *s).collect();
            assert!(tombstoned_ids.contains(&stale_sid),
                "shard {:?}: stale session should be tombstoned", shard_id);
            assert!(!tombstoned_ids.contains(&active_sid),
                "shard {:?}: active session should NOT be tombstoned", shard_id);
        }
    }

    // ── Interpretation-monotone convergence (AP, non-confluent paths) ────────────

    #[test]
    fn convergence_under_different_gossip_orderings() {
        // Non-confluence structural test.
        //
        // Two clusters run the same fault scenario (crash shard 2, tick, heal, tick)
        // but with different gossip delay policies, causing different event orderings.
        // The intermediate states are non-canonical — different delivery orderings
        // will produce different health readings mid-scenario.
        //
        // The invariant: the *final observable equivalence class* (which shards are
        // Pos/Zero/Neg after convergence) must agree across both runs, even though
        // the paths through the intermediate state space differ.
        //
        // This is the AP property: availability (the system continues to operate,
        // possibly at Zero) + eventual interpretation-class convergence. It is NOT
        // a claim that intermediate states are identical (that would require global
        // confluence, which does not hold).

        let run = |max_delay: u32, seed: u64| -> [i8; 3] {
            let mut cluster = SimCluster::new(4, seed);
            cluster.network.faults.max_delay_ticks = max_delay;

            // Crash shard 2 via gossip: update only shard 2's own pressure.
            // Peers learn about it through the gossip tick (delayed by delay policy).
            // We do NOT call inject_neg_everywhere — that would bypass the gossip path
            // and remove the ordering non-determinism we are testing.
            let neg = ShardPressure { queue_depth: 0, cursor_count: 1, budget_consumed: 0 };
            cluster.lifecycle.insert(ShardId(2), ShardLifecycle::Crashed);
            cluster.shards.get_mut(&ShardId(2)).unwrap().update_pressure(ShardId(2), neg);

            // Tick enough for even the maximum delay to expire
            cluster.tick(max_delay as u64 + 5);

            // Intermediate check: we intentionally do NOT assert anything about
            // intermediate states here — that would be a non-confluence violation.

            // Heal: restore shard 2's own pressure. Peers learn through gossip ticks.
            let healthy = ShardPressure { queue_depth: 0, cursor_count: 0, budget_consumed: 0 };
            cluster.lifecycle.insert(ShardId(2), ShardLifecycle::Alive);
            cluster.shards.get_mut(&ShardId(2)).unwrap().update_pressure(ShardId(2), healthy);

            // Tick enough for heal gossip to reach all peers regardless of delay
            cluster.tick(max_delay as u64 + 5);

            // Collect final health of shard 2 as seen by shards 0, 1, 3
            // (the three observers that are not shard 2 itself)
            [
                cluster.health_as_seen_by(ShardId(0), ShardId(2)),
                cluster.health_as_seen_by(ShardId(1), ShardId(2)),
                cluster.health_as_seen_by(ShardId(3), ShardId(2)),
            ]
        };

        // Fast gossip (no delay) and slow gossip (delay up to 3 ticks) must
        // produce the same final health classification.
        let fast_class = run(0, 0xC0FFEE);
        let slow_class = run(3, 0xC0FFEE);
        assert_eq!(fast_class, slow_class,
            "different gossip delay orderings must converge to the same interpretation class");
        assert!(fast_class.iter().all(|&h| h == HEALTH_POS),
            "after heal + convergence, all observers must see shard 2 as Pos");

        // Different seeds (different random delay sequences), same delay bound —
        // still the same final class.
        let class_a = run(2, 0x1111_AAAA);
        let class_b = run(2, 0x2222_BBBB);
        assert_eq!(class_a, class_b,
            "different RNG seeds (different delay sequences) must converge to same class");
    }

    // ── Congested-but-not-dead shard uses Zero health ─────────────────────────

    #[test]
    fn congested_shard_shows_zero_not_neg() {
        let mut cluster = SimCluster::new(2, 0xBBBB);

        // Inject congested (but not dead) pressure for shard 1
        let congested = ShardPressure {
            queue_depth:     600,  // above CONGESTED_QUEUE_DEPTH (500)
            cursor_count:    0,    // no parked WorkCursor → not Neg
            budget_consumed: 0,
        };
        if let Some(rt) = cluster.shards.get_mut(&ShardId(0)) {
            rt.update_pressure(ShardId(1), congested);
        }

        let health = cluster.health_as_seen_by(ShardId(0), ShardId(1));
        assert_eq!(health, HEALTH_ZERO,
            "congested shard should be HEALTH_ZERO (not Neg, not Pos)");

        // Placement must exclude the congested shard
        let best = cluster.shards[&ShardId(0)].topology.any_healthy_shard();
        assert_eq!(best, Some(ShardId(0)),
            "congested shard 1 (Zero) must be excluded from placement; only shard 0 qualifies");
    }
}
