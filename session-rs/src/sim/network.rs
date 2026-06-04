//! SimNetwork — deterministic message bus with fault injection.
//!
//! Messages are queued with a `deliver_at` tick and drained by the cluster
//! on each `tick_once()`. The fault policy controls drop rate, delay range,
//! and bidirectional network partitions between shard pairs.

use crate::{ShardId, admission::ShardPressure};
use super::faults::FaultPolicy;

// ── PendingMsg ────────────────────────────────────────────────────────────────

/// In-flight messages on the simulation network.
#[derive(Clone, Debug)]
pub enum PendingMsg {
    /// Topology gossip: shard `src` advertising its pressure to shard `dst`.
    TopologyGossip {
        src:        ShardId,
        dst:        ShardId,
        pressure:   ShardPressure,
        deliver_at: u64,
    },
}

impl PendingMsg {
    pub fn deliver_at(&self) -> u64 {
        match self { PendingMsg::TopologyGossip { deliver_at, .. } => *deliver_at }
    }
}

// ── SimNetwork ────────────────────────────────────────────────────────────────

/// Simulated network bus for in-process cluster messaging.
pub struct SimNetwork {
    /// Pending messages not yet delivered, sorted loosely by `deliver_at`.
    pending: Vec<PendingMsg>,
    /// Fault injection policy.
    pub faults: FaultPolicy,
}

impl SimNetwork {
    pub fn new() -> Self {
        SimNetwork { pending: Vec::new(), faults: FaultPolicy::default() }
    }

    /// Enqueue a message for future delivery.
    pub fn enqueue(&mut self, msg: PendingMsg) {
        self.pending.push(msg);
    }

    /// Drain all messages with `deliver_at <= current_tick`.
    pub fn drain_due(&mut self, current_tick: u64) -> Vec<PendingMsg> {
        let mut due    = Vec::new();
        let mut remain = Vec::new();
        for msg in self.pending.drain(..) {
            if msg.deliver_at() <= current_tick {
                due.push(msg);
            } else {
                remain.push(msg);
            }
        }
        self.pending = remain;
        due
    }

    pub fn pending_count(&self) -> usize { self.pending.len() }
}
