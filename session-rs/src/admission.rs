//! Admission control and backpressure policy.
//!
//! The session manager applies per-opcode-class admission treatment under load.
//! Each opcode class has different semantics:
//!
//! | Class      | Treatment under pressure                               |
//! |------------|--------------------------------------------------------|
//! | Control    | Serialised off hot path; subject to control-plane quota|
//! | Profile    | Same as Control                                        |
//! | SetValue   | Coalesce: last writer wins per node uid                |
//! | Propagate  | Sheddable when a newer SetValue for root is queued     |
//! | Demand     | Prioritised; MUST NOT be shed while caller is blocked  |
//! | Stabilize  | Slow-lane budget; preemptable via WorkCursor           |
//! | Subscribe  | Control-plane; lightweight                             |
//! | Data       | Standard FIFO within session quota                     |
//! | Unknown    | Skip-forward via length field; no further action       |

use crate::OpcodeClass;

// ── ShardPressure ─────────────────────────────────────────────────────────────

/// Pressure counters for a single engine shard.
/// Updated by the shard; read by the admission controller.
#[derive(Clone, Copy, Debug, Default)]
pub struct ShardPressure {
    /// Current propagation queue depth.
    pub queue_depth: u32,
    /// Rolling budget consumed in the current scheduling window.
    pub budget_consumed: u64,
    /// Number of parked `WorkCursor` instances (StepLimitExceeded).
    pub cursor_count: u32,
}

// ── AdmissionDecision ─────────────────────────────────────────────────────────

/// The session manager's verdict for an incoming record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Forward to engine shard immediately.
    Allow,
    /// Buffer briefly; re-evaluate when pressure drops. Not yet implemented
    /// (treated as Allow in the current scheduler).
    Delay,
    /// Discard. Caller receives no indication. Used for sheddable ops.
    Drop,
    /// Merge with a pending record for the same node uid (last writer wins).
    /// Caller is responsible for deduplication before engine dispatch.
    Coalesce,
    /// Place at the front of the engine queue. Used for Demand.
    Prioritize,
    /// Route to the slow-lane budget. Used for Stabilize.
    SlowLane,
    /// Route to the serialised control pipeline (off hot path).
    ControlPlane,
}

impl AdmissionDecision {
    /// True if the record should be forwarded to the engine (after coalescing
    /// and routing decisions are applied).
    pub fn reaches_engine(self) -> bool {
        !matches!(self, Self::Drop)
    }
}

// ── BackpressurePolicy ────────────────────────────────────────────────────────

/// Per-opcode-class admission policy thresholds.
#[derive(Clone, Debug)]
pub struct BackpressurePolicy {
    /// Queue depth above which SetValue coalescing kicks in.
    pub coalesce_threshold: u32,
    /// Queue depth above which Propagate records are shed.
    pub shed_threshold: u32,
}

impl Default for BackpressurePolicy {
    fn default() -> Self {
        BackpressurePolicy {
            coalesce_threshold: 1_000,
            shed_threshold:     5_000,
        }
    }
}

impl BackpressurePolicy {
    pub fn new(coalesce_threshold: u32, shed_threshold: u32) -> Self {
        BackpressurePolicy { coalesce_threshold, shed_threshold }
    }

    /// Decide admission treatment for an incoming record.
    ///
    /// Control-plane ops are always routed to the control pipeline regardless
    /// of pressure. Demand is never shed. All other ops are subject to thresholds.
    pub fn decide(&self, opcode_class: OpcodeClass, pressure: &ShardPressure) -> AdmissionDecision {
        use OpcodeClass::*;
        use AdmissionDecision::*;

        match opcode_class {
            Control | Profile | Subscribe => ControlPlane,

            // Demand is highest priority; never shed
            Demand => Prioritize,

            // Stabilize goes to the slow-lane budget regardless of pressure
            Stabilize => SlowLane,

            // SetValue: coalesce under pressure (last-writer-wins per node uid)
            SetValue => {
                if pressure.queue_depth >= self.coalesce_threshold {
                    Coalesce
                } else {
                    Allow
                }
            }

            // Propagate: sheddable when a newer SetValue for the same root is queued
            Propagate => {
                if pressure.queue_depth >= self.shed_threshold {
                    Drop
                } else {
                    Allow
                }
            }

            // Known data ops: forward under normal conditions, delay under pressure
            Data => {
                if pressure.queue_depth >= self.coalesce_threshold {
                    Delay
                } else {
                    Allow
                }
            }

            // Unknown ops are not forwarded to the engine
            Unknown => Drop,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> BackpressurePolicy {
        BackpressurePolicy::new(100, 500)
    }

    fn pressure(depth: u32) -> ShardPressure {
        ShardPressure { queue_depth: depth, ..Default::default() }
    }

    #[test]
    fn demand_is_never_shed() {
        let p = policy();
        // Even at maximum conceivable depth, Demand is prioritised
        assert_eq!(p.decide(OpcodeClass::Demand, &pressure(u32::MAX)), AdmissionDecision::Prioritize);
        assert_eq!(p.decide(OpcodeClass::Demand, &pressure(0)),        AdmissionDecision::Prioritize);
    }

    #[test]
    fn propagate_shed_above_threshold() {
        let p = policy();
        assert_eq!(p.decide(OpcodeClass::Propagate, &pressure(500)), AdmissionDecision::Drop);
        assert_eq!(p.decide(OpcodeClass::Propagate, &pressure(501)), AdmissionDecision::Drop);
    }

    #[test]
    fn propagate_allowed_below_threshold() {
        let p = policy();
        assert_eq!(p.decide(OpcodeClass::Propagate, &pressure(0)),   AdmissionDecision::Allow);
        assert_eq!(p.decide(OpcodeClass::Propagate, &pressure(499)), AdmissionDecision::Allow);
    }

    #[test]
    fn set_value_coalesced_at_threshold() {
        let p = policy();
        assert_eq!(p.decide(OpcodeClass::SetValue, &pressure(100)), AdmissionDecision::Coalesce);
        assert_eq!(p.decide(OpcodeClass::SetValue, &pressure(99)),  AdmissionDecision::Allow);
    }

    #[test]
    fn stabilize_always_slow_lane() {
        let p = policy();
        assert_eq!(p.decide(OpcodeClass::Stabilize, &pressure(0)),        AdmissionDecision::SlowLane);
        assert_eq!(p.decide(OpcodeClass::Stabilize, &pressure(u32::MAX)), AdmissionDecision::SlowLane);
    }

    #[test]
    fn control_plane_ops_always_routed_off_hot_path() {
        let p = policy();
        for &cls in &[OpcodeClass::Control, OpcodeClass::Profile, OpcodeClass::Subscribe] {
            assert_eq!(p.decide(cls, &pressure(u32::MAX)), AdmissionDecision::ControlPlane,
                "{:?} should always be ControlPlane", cls);
        }
    }

    #[test]
    fn unknown_opcode_dropped() {
        let p = policy();
        assert_eq!(p.decide(OpcodeClass::Unknown, &pressure(0)), AdmissionDecision::Drop);
    }

    #[test]
    fn drop_does_not_reach_engine() {
        assert!(!AdmissionDecision::Drop.reaches_engine());
        assert!(AdmissionDecision::Allow.reaches_engine());
        assert!(AdmissionDecision::Prioritize.reaches_engine());
        assert!(AdmissionDecision::Coalesce.reaches_engine());
    }
}
