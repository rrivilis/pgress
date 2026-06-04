//! Synthetic workload generators for simulation tests.
//!
//! Builders produce `ParsedHeader` streams that the caller routes through
//! `SessionRuntime::route_record`. Designed for throughput benchmarks and
//! property-based load tests.

use crate::{
    SessionId, TenantId, WirePartitionId,
    runtime::ParsedHeader,
};

// ── WorkloadBuilder ───────────────────────────────────────────────────────────

/// Generates a stream of `ParsedHeader` values for a given session + partition.
///
/// Use in benchmarks: call `next()` in a tight loop, feed each header to
/// `SessionRuntime::route_record`.
pub struct WorkloadBuilder {
    session_id:   SessionId,
    partition_id: WirePartitionId,
    tenant_id:    TenantId,
    stream_seq:   u64,
    epoch:        u64,
    opcode:       u16,
}

impl WorkloadBuilder {
    /// Builder for a `SetValue` (opcode 0x0001) workload on the given session/partition.
    pub fn set_value(session_id: SessionId, partition_id: WirePartitionId) -> Self {
        WorkloadBuilder {
            session_id,
            partition_id,
            tenant_id:  TenantId(0),
            stream_seq: 1,
            epoch:      1,
            opcode:     0x0001,  // SetValue
        }
    }

    /// Builder for a `Propagate` (opcode 0x0004) workload.
    pub fn propagate(session_id: SessionId, partition_id: WirePartitionId) -> Self {
        WorkloadBuilder { opcode: 0x0004, ..Self::set_value(session_id, partition_id) }
    }

    /// Emit the next header, advancing stream_seq and epoch.
    pub fn next(&mut self) -> ParsedHeader {
        let h = ParsedHeader {
            opcode:       self.opcode,
            flags:        0,
            length:       16,
            tenant_id:    self.tenant_id,
            session_id:   self.session_id,
            partition_id: self.partition_id,
            causal_epoch: self.epoch,
            stream_seq:   self.stream_seq,
        };
        self.stream_seq += 1;
        self.epoch      += 1;
        h
    }

    /// Emit `n` headers in a batch.
    pub fn batch(&mut self, n: usize) -> Vec<ParsedHeader> {
        (0..n).map(|_| self.next()).collect()
    }
}

// ── CrossShardWorkload ────────────────────────────────────────────────────────

/// Workload that alternates between two partitions on different shards.
///
/// Used to measure the overhead of the encoding boundary: the caller routes
/// partition A's records on shard 0, then partition B's on shard 1.
pub struct CrossShardWorkload {
    pub local:  WorkloadBuilder,
    pub remote: WorkloadBuilder,
    toggle:     bool,
}

impl CrossShardWorkload {
    pub fn new(
        session_id: SessionId,
        local_partition:  WirePartitionId,
        remote_partition: WirePartitionId,
    ) -> Self {
        CrossShardWorkload {
            local:  WorkloadBuilder::set_value(session_id, local_partition),
            remote: WorkloadBuilder::set_value(session_id, remote_partition),
            toggle: false,
        }
    }

    /// Returns the next header and whether it targets the remote partition.
    pub fn next(&mut self) -> (ParsedHeader, bool) {
        let is_remote = self.toggle;
        self.toggle   = !self.toggle;
        if is_remote {
            (self.remote.next(), true)
        } else {
            (self.local.next(), false)
        }
    }
}
