//! Fault injection policy for the simulation network.
//!
//! Controls:
//! - **Drop rate**: probabilistic message loss (0.0 = no drops, 1.0 = all dropped).
//! - **Delay ticks**: uniform-random delay added to each message's `deliver_at`.
//! - **Network partitions**: bidirectional complete message loss between shard pairs.

use rustc_hash::FxHashSet;
use crate::ShardId;
use super::Lcg64;

/// Fault injection configuration for the simulated network.
#[derive(Debug, Default)]
pub struct FaultPolicy {
    /// Fraction of messages to drop uniformly at random. Range: [0.0, 1.0].
    pub drop_rate: f32,
    /// When non-zero, each message is delayed by a uniform random number of ticks
    /// in [0, max_delay_ticks]. Delay = 0 means immediate delivery on next tick.
    pub max_delay_ticks: u32,
    /// Set of (src, dst) pairs where messages are silently dropped.
    /// A partition (a, b) implies both (a→b) and (b→a) are partitioned.
    partitioned: FxHashSet<(u64, u64)>,
}

impl FaultPolicy {
    pub fn new() -> Self { Self::default() }

    /// A perfectly lossy, zero-delay network (no faults).
    pub fn perfect() -> Self { Self::default() }

    /// Network with a given uniform drop rate.
    pub fn with_drop_rate(mut self, rate: f32) -> Self {
        self.drop_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Network with bounded message delay.
    pub fn with_max_delay(mut self, ticks: u32) -> Self {
        self.max_delay_ticks = ticks;
        self
    }

    /// Add a bidirectional partition between two shards.
    pub fn add_partition(&mut self, a: ShardId, b: ShardId) {
        self.partitioned.insert((a.0.min(b.0), a.0.max(b.0)));
    }

    /// Remove a bidirectional partition.
    pub fn remove_partition(&mut self, a: ShardId, b: ShardId) {
        self.partitioned.remove(&(a.0.min(b.0), a.0.max(b.0)));
    }

    /// Returns true if messages between `a` and `b` are being dropped.
    pub fn is_partitioned(&self, a: ShardId, b: ShardId) -> bool {
        self.partitioned.contains(&(a.0.min(b.0), a.0.max(b.0)))
    }

    /// Sample a delay in [0, max_delay_ticks] using the provided RNG.
    /// Returns 1 (deliver next tick) when `max_delay_ticks == 0`.
    pub fn sample_delay_ticks(&self, rng: &mut Lcg64) -> u32 {
        if self.max_delay_ticks == 0 {
            1   // immediate delivery = next tick
        } else {
            1 + (rng.next_u64() as u32 % self.max_delay_ticks)
        }
    }
}
