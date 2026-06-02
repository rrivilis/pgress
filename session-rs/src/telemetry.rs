//! Telemetry — observability via a pgress-compatible stream interface.
//!
//! In v1, telemetry is backed by an in-process ring buffer associated with the
//! reserved partition ID `PARTITION_TELEMETRY`. The event semantics, node naming
//! convention, and ternary encoding are stable and forward-compatible with a
//! future first-class engine partition. When that migration happens, the ring
//! buffer is replaced by a real engine partition and subscribers use standard
//! `Subscribe` / `Demand` ops directly; callers see no API change.
//!
//! Telemetry nodes are ternary:
//!
//! | Value | Meaning                     |
//! |-------|-----------------------------|
//! | Pos   | Healthy / changed / active  |
//! | Zero  | Conflict / degraded / error |
//! | Neg   | Pending / unknown           |
//!
//! ## Node naming convention
//!
//! Telemetry nodes follow the pattern `{subject}::{id}::{metric}`.
//! See `TelemetryNodeName` for the canonical name builders.
//!
//! ## Pre-subscription buffering
//!
//! Events emitted before any subscriber connects are stored in a bounded ring
//! buffer. When the first subscriber connects, the caller should call `drain`
//! to flush the backlog. If the buffer overflows, the oldest events are dropped
//! and the `dropped` counter is incremented.
//!
//! ## Auth
//!
//! `PARTITION_TELEMETRY` is registered in `PartitionAuthTable` and obeys the
//! same authority gate as any other partition. Unauthorised observers cannot
//! subscribe.

use std::collections::VecDeque;
use pgress_core::uid::Uid;
use crate::{SessionId, ShardId, WirePartitionId};
use crate::domain::ShardFabricAddr;

/// Reserved partition ID for the telemetry stream.
/// Chosen to be outside any plausible user-allocated space.
pub const PARTITION_TELEMETRY: u64 = 0xFFFF_FFFF_FFFF_FFFE;

/// Default ring buffer capacity before any subscriber is registered.
pub const DEFAULT_RING_CAPACITY: usize = 4_096;

// ── TelemetryEvent ────────────────────────────────────────────────────────────

/// Events emitted to the telemetry partition by the session manager and engine.
#[derive(Clone, Debug)]
pub enum TelemetryEvent {
    /// A node's value changed (version bumped).
    ValueChanged {
        node:    Uid,
        version: u64,
    },

    /// An authority gate violation was detected (either at the dataplane or engine level).
    AuthViolation {
        node:   Uid,
        reason: String,
    },

    /// A session's stream health changed (gap detected, reconnect, etc.).
    SessionHealthChanged {
        session_id: SessionId,
        /// True = stream is healthy; False = degraded (gap, auth failure, etc.).
        healthy:    bool,
    },

    /// A shard's queue depth crossed a pressure threshold.
    ShardPressureChanged {
        shard_id: ShardId,
        depth:    u32,
    },

    /// A partition reached quiescence (empty propagation queue).
    DomainQuiescent {
        partition_id: WirePartitionId,
    },

    /// An engine step budget was exhausted; a WorkCursor was parked.
    BudgetExhausted {
        node:            Uid,
        steps_consumed:  u32,
        budget:          u32,
    },

    /// A shard's ternary health in the topology partition changed.
    ///
    /// Emitted by `SessionRuntime::update_pressure` when the health value
    /// transitions (Pos → Zero, Zero → Neg, etc.). Subscribers watching the
    /// topology partition receive this as placement-relevant signal.
    TopologyHealthChanged {
        shard_id:   ShardId,
        /// Old ternary value: 1=Pos, 0=Zero, -1=Neg.
        old_health: i8,
        /// New ternary value.
        new_health: i8,
    },

    /// A shard's fabric address was registered or updated.
    ///
    /// Carries the full `ShardFabricAddr` (placement coords + implicit scope
    /// hierarchy). The locality radius is derivable from this and the set of
    /// other registered shards via `DomainRegistry::locality_radius`.
    FabricPlacementChanged {
        shard_id: ShardId,
        addr:     ShardFabricAddr,
    },
}

// ── TelemetryNodeName ─────────────────────────────────────────────────────────

/// Canonical node name builders for the telemetry partition.
///
/// Names follow the convention `{subject}::{id}::{metric}`.
pub struct TelemetryNodeName;

impl TelemetryNodeName {
    pub fn node_value(uid: Uid) -> String {
        format!("node::{}::value", uid)
    }

    pub fn session_health(sid: SessionId) -> String {
        format!("session::{}::stream_health", sid.0)
    }

    pub fn shard_pressure(shard: ShardId) -> String {
        format!("shard::{}::pressure", shard.0)
    }

    pub fn auth_violations() -> String {
        "auth::violations".to_owned()
    }

    pub fn domain_quiescent(pid: WirePartitionId) -> String {
        format!("domain::{}::quiescent", pid.0)
    }

    /// `shard::{id}::topology_health` — ternary health value in the topology partition.
    pub fn topology_health(shard: ShardId) -> String {
        format!("shard::{}::topology_health", shard.0)
    }

    /// `shard::{id}::locality_radius` — max hop distance to any peer shard.
    pub fn locality_radius(shard: ShardId) -> String {
        format!("shard::{}::locality_radius", shard.0)
    }

    /// `shard::{id}::placement_coords` — fabric address (region/pod/rack/leaf).
    pub fn placement_coords(shard: ShardId) -> String {
        format!("shard::{}::placement_coords", shard.0)
    }
}

// ── TelemetryPartition ────────────────────────────────────────────────────────

/// Ring buffer for pre-subscription telemetry events.
///
/// Events are pushed via `emit`. When a subscriber connects, the caller
/// should call `drain` to flush the backlog. If the buffer is full on
/// `emit`, the oldest event is silently dropped and `dropped` is incremented.
#[derive(Debug)]
pub struct TelemetryPartition {
    buffer:      VecDeque<TelemetryEvent>,
    capacity:    usize,
    /// Count of events dropped due to buffer overflow since last drain.
    pub dropped: u64,
}

impl Default for TelemetryPartition {
    fn default() -> Self {
        TelemetryPartition::new(DEFAULT_RING_CAPACITY)
    }
}

impl TelemetryPartition {
    pub fn new(capacity: usize) -> Self {
        TelemetryPartition {
            buffer:   VecDeque::with_capacity(capacity.min(capacity)),
            capacity,
            dropped: 0,
        }
    }

    /// Emit an event. Drops the oldest event if the buffer is at capacity.
    pub fn emit(&mut self, event: TelemetryEvent) {
        if self.buffer.len() >= self.capacity {
            self.buffer.pop_front();
            self.dropped += 1;
        }
        self.buffer.push_back(event);
    }

    /// Drain all buffered events in emission order.
    /// Resets the `dropped` counter.
    pub fn drain(&mut self) -> Vec<TelemetryEvent> {
        self.dropped = 0;
        self.buffer.drain(..).collect()
    }

    /// Peek at buffered events without draining.
    pub fn peek(&self) -> impl Iterator<Item = &TelemetryEvent> {
        self.buffer.iter()
    }

    pub fn len(&self) -> usize { self.buffer.len() }
    pub fn is_empty(&self) -> bool { self.buffer.is_empty() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgress_core::uid;

    #[test]
    fn emit_and_drain_in_order() {
        let mut tp = TelemetryPartition::new(8);
        let uid_a  = uid::fresh();
        let uid_b  = uid::fresh();

        tp.emit(TelemetryEvent::ValueChanged { node: uid_a, version: 1 });
        tp.emit(TelemetryEvent::ValueChanged { node: uid_b, version: 2 });

        let events = tp.drain();
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], TelemetryEvent::ValueChanged { node, .. } if node == uid_a));
        assert!(matches!(events[1], TelemetryEvent::ValueChanged { node, .. } if node == uid_b));
        assert!(tp.is_empty());
    }

    #[test]
    fn overflow_drops_oldest_event() {
        let mut tp = TelemetryPartition::new(2);
        let uid_a = uid::fresh();
        let uid_b = uid::fresh();
        let uid_c = uid::fresh();

        tp.emit(TelemetryEvent::ValueChanged { node: uid_a, version: 1 });
        tp.emit(TelemetryEvent::ValueChanged { node: uid_b, version: 2 });
        tp.emit(TelemetryEvent::ValueChanged { node: uid_c, version: 3 }); // drops uid_a

        assert_eq!(tp.dropped, 1);
        let events = tp.drain();
        assert_eq!(events.len(), 2);
        // uid_a was dropped; uid_b and uid_c remain
        assert!(matches!(events[0], TelemetryEvent::ValueChanged { node, .. } if node == uid_b));
        assert!(matches!(events[1], TelemetryEvent::ValueChanged { node, .. } if node == uid_c));
    }

    #[test]
    fn drain_resets_dropped_counter() {
        let mut tp = TelemetryPartition::new(1);
        tp.emit(TelemetryEvent::ValueChanged { node: uid::fresh(), version: 1 });
        tp.emit(TelemetryEvent::ValueChanged { node: uid::fresh(), version: 2 });
        assert_eq!(tp.dropped, 1);
        tp.drain();
        assert_eq!(tp.dropped, 0);
    }

    #[test]
    fn node_name_format() {
        let uid  = uid::fresh();
        let name = TelemetryNodeName::node_value(uid);
        assert!(name.starts_with("node::"));
        assert!(name.ends_with("::value"));

        let name = TelemetryNodeName::auth_violations();
        assert_eq!(name, "auth::violations");
    }

    #[test]
    fn session_health_name_contains_session_id() {
        let name = TelemetryNodeName::session_health(SessionId(42));
        assert!(name.contains("42"));
        assert!(name.contains("stream_health"));
    }
}
