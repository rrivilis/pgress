//! Recovery types: typed engine outcomes and failure policy.
//!
//! The engine currently returns `Result<(), EngineError>` (a stringly-typed error).
//! This module defines the typed surface the session manager needs: a richer
//! `EngineOutcome` that carries structured payloads for each failure mode, and
//! a `WorkCursor` that makes `StepLimitExceeded` resumable.
//!
//! ## WorkCursor
//!
//! When the engine hits its step budget, it should return a `WorkCursor` rather
//! than discarding in-progress work. The session manager can then:
//! - **Requeue**: park the cursor and retry on the next scheduling cycle
//! - **Shed**: discard (acceptable for Propagate/Stabilize under load)
//! - **Escalate**: report to monitoring (for CycleDetected / CapabilityViolation)
//!
//! Full cursor resumability (serialising the propagation queue) requires engine
//! support not yet implemented. For now, `WorkCursor` is an opaque token carrying
//! enough metadata for the session manager to decide a `FailureAction`.

use pgress_core::uid::Uid;

// ── WorkCursor ────────────────────────────────────────────────────────────────

/// Opaque token representing parked propagation work.
///
/// Returned inside `EngineOutcome::StepLimitExceeded`. The session manager
/// inspects `steps_consumed` and `budget` to decide whether to requeue or shed.
///
/// Full queue serialisation (pausing mid-drain and restoring the `VecDeque`) is
/// a future enhancement requiring engine-level support.
#[derive(Debug)]
pub struct WorkCursor {
    /// Root node that triggered the work.
    pub root_node:       Uid,
    /// Steps consumed before the limit was hit.
    pub steps_consumed:  u32,
    /// Budget that was in force.
    pub budget:          u32,
}

impl WorkCursor {
    pub fn new(root_node: Uid, steps_consumed: u32, budget: u32) -> Self {
        WorkCursor { root_node, steps_consumed, budget }
    }

    /// True if the work was near the budget limit (>= 90% consumed).
    /// Used by the session manager to decide whether to escalate or just requeue.
    pub fn near_limit(&self) -> bool {
        self.budget > 0 && self.steps_consumed * 10 >= self.budget * 9
    }
}

// ── EngineOutcome ─────────────────────────────────────────────────────────────

/// Typed result from an engine `apply` call, as seen by the session manager.
///
/// Replaces the stringly-typed `Result<(), EngineError>` for session-manager
/// routing decisions.
#[derive(Debug)]
pub enum EngineOutcome {
    /// Engine applied the op and propagated to quiescence (or empty queue).
    Ok,
    /// Engine hit the step budget before reaching quiescence.
    /// The session manager MAY requeue the cursor or shed the work.
    StepLimitExceeded { cursor: WorkCursor },
    /// Engine detected a dependency cycle.
    /// The session manager SHOULD escalate (log, alert, isolate the partition).
    CycleDetected { node: Uid },
    /// Engine rejected the op due to a capability violation.
    /// The session manager SHOULD shed and emit a telemetry AuthViolation event.
    CapabilityViolation { node: Uid },
}

// ── FailureAction ─────────────────────────────────────────────────────────────

/// What the session manager does with a non-Ok `EngineOutcome`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureAction {
    /// Park the cursor and retry on the next scheduling cycle.
    Requeue,
    /// Discard the pending work. Acceptable for Propagate/Stabilize under load.
    Shed,
    /// Report to operator / monitoring. Required for CycleDetected.
    Escalate,
}

impl FailureAction {
    /// Default action for each outcome type.
    pub fn default_for(outcome: &EngineOutcome) -> Self {
        match outcome {
            EngineOutcome::Ok                       => Self::Requeue, // shouldn't call this
            EngineOutcome::StepLimitExceeded { .. } => Self::Requeue,
            EngineOutcome::CycleDetected { .. }     => Self::Escalate,
            EngineOutcome::CapabilityViolation { .. }=> Self::Shed,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pgress_core::uid;

    #[test]
    fn near_limit_true_at_90_percent() {
        let c = WorkCursor::new(uid::fresh(), 90, 100);
        assert!(c.near_limit());
    }

    #[test]
    fn near_limit_false_below_90_percent() {
        let c = WorkCursor::new(uid::fresh(), 89, 100);
        assert!(!c.near_limit());
    }

    #[test]
    fn near_limit_false_with_zero_budget() {
        let c = WorkCursor::new(uid::fresh(), 0, 0);
        assert!(!c.near_limit());
    }

    #[test]
    fn default_action_for_step_limit_is_requeue() {
        let cursor  = WorkCursor::new(uid::fresh(), 50, 100);
        let outcome = EngineOutcome::StepLimitExceeded { cursor };
        assert_eq!(FailureAction::default_for(&outcome), FailureAction::Requeue);
    }

    #[test]
    fn default_action_for_cycle_is_escalate() {
        let outcome = EngineOutcome::CycleDetected { node: uid::fresh() };
        assert_eq!(FailureAction::default_for(&outcome), FailureAction::Escalate);
    }

    #[test]
    fn default_action_for_capability_violation_is_shed() {
        let outcome = EngineOutcome::CapabilityViolation { node: uid::fresh() };
        assert_eq!(FailureAction::default_for(&outcome), FailureAction::Shed);
    }
}
