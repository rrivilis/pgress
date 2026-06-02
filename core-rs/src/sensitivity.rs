//! Sensitivity — the interpretation layer of dep observability.
//!
//! `ProjectionMask` answers: **what can be seen** (structural filter).
//! `Sensitivity` answers: **how to react** to what is seen (semantic filter).
//!
//! Together they form the full dep recompute predicate:
//!
//! ```text
//! triggers_recompute(dep, delta, before, after) =
//!     dep.mask.affects(delta)          // observable?
//!     && sensitivity_check(dep.sensitivity, before, after)  // warrants recompute?
//! ```
//!
//! ## VersionedState partial order
//!
//! `VersionedState<T>` is an element of the information lattice. A state A
//! dominates state B (`A.dominates(B)`) when A is at least as recent and at
//! least as informative in every dimension. Non-comparable states form
//! antichains — the frontier of unprocessed knowledge.

use crate::{
    delta::{Delta, ProjectionMask},
    partition::ZeroKind,
    time::Time,
};

// ── Sensitivity ───────────────────────────────────────────────────────────────

/// How a dep reacts to an observable change.
///
/// The interaction between `ProjectionMask` and `Sensitivity`:
/// - `ProjectionMask` is the *structural* filter — it says which deltas are
///   even visible at this dep boundary.
/// - `Sensitivity` is the *semantic* filter — it says, given a visible delta,
///   whether it warrants recompute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Sensitivity {
    /// React to any observable change. No filtering on value direction or type.
    /// This is the default — equivalent to the current eager propagation.
    #[default]
    Exact,

    /// React only to monotone increases (Neg→Val). Ignores retractions
    /// (Val→Neg) and Zero signals. Used for deps that only care about
    /// whether something became available, not whether it was retracted.
    Monotone,

    /// React only after the emitting node has been through `Stabilize` —
    /// i.e., only when the state is definite (val is Some, zero is None).
    /// Suppresses all Zero signals and partial states.
    StableOnly,

    /// React only on state-class *transitions*: Neg→Val, Val→Zero, Zero→Val,
    /// Val→Neg. Does not recompute on value changes within the same class.
    /// Appropriate for reactive / event-driven deps where the transition is
    /// the signal, not the value.
    Edge,
}

// ── StateClass — for Edge sensitivity ────────────────────────────────────────

/// Coarse classification of a `VersionedState` for `Edge` sensitivity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateClass {
    Pending,    // val = None, zero = None  (Neg / not yet computed)
    Definite,   // val = Some(_), zero = None
    Uncertain,  // zero = Some(_) (typed Zero — regardless of val)
}

// ── VersionedState ────────────────────────────────────────────────────────────

/// A node's interpretation at a point in time, with partial knowledge.
///
/// Carries:
/// - `val`: the definite value, if known (`Some(T)` = Pos or Neg, `None` = pending)
/// - `zero`: a typed uncertainty signal, if emitted (`Some(ZeroKind)`)
/// - `mask`: which aspects of the node's state are captured here
/// - `time`: the product timestamp when this state was observed
///
/// ## Partial order
///
/// `A.dominates(B)` iff A is at least as recent AND at least as informative:
/// - `A.time.dominates(B.time)` (component-wise ≥ on all 5 dimensions)
/// - `A.mask.covers(B.mask)` (A observed at least everything B observed)
/// - A is not less informative than B:
///   - `Some(_)` > `None` (more specific)
///   - `val` and `zero` are incomparable — a definite value doesn't dominate
///     a typed Zero or vice versa; they're different kinds of knowledge
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedState<T: Clone + PartialEq> {
    /// Definite value (Pos or Neg), if known.
    pub val:  Option<T>,
    /// Typed uncertainty signal, if emitted.
    pub zero: Option<ZeroKind>,
    /// Which aspects of the node's state are captured by this observation.
    pub mask: ProjectionMask,
    /// When this state was observed.
    pub time: Time,
}

impl<T: Clone + PartialEq> VersionedState<T> {
    pub fn pending(time: Time, mask: ProjectionMask) -> Self {
        VersionedState { val: None, zero: None, mask, time }
    }

    pub fn definite(val: T, time: Time, mask: ProjectionMask) -> Self {
        VersionedState { val: Some(val), zero: None, mask, time }
    }

    pub fn uncertain(zero: ZeroKind, time: Time, mask: ProjectionMask) -> Self {
        VersionedState { val: None, zero: Some(zero), mask, time }
    }

    /// The coarse state class of this versioned state.
    pub fn class(&self) -> StateClass {
        if self.zero.is_some() { StateClass::Uncertain }
        else if self.val.is_some() { StateClass::Definite }
        else { StateClass::Pending }
    }

    /// Whether this state is less informative than `other` in terms of
    /// val/zero knowledge (ignoring time and mask).
    fn is_less_informative_than(&self, other: &VersionedState<T>) -> bool {
        match (&self.val, &self.zero, &other.val, &other.zero) {
            // self has nothing, other has something → self is less informative
            (None, None, Some(_), _) | (None, None, _, Some(_)) => true,
            _ => false,
        }
    }

    /// `A.dominates(B)` — A is at least as recent, covers at least as much,
    /// and is not less informative than B.
    ///
    /// `val` (definite value) and `zero` (typed uncertainty) are incomparable
    /// epistemic kinds — neither dominates the other. A Pending state can be
    /// dominated by either.
    pub fn dominates(&self, other: &VersionedState<T>) -> bool {
        // Definite ↔ Uncertain: orthogonal knowledge kinds; incomparable.
        let epistemic_ok = match (&self.val, &self.zero, &other.val, &other.zero) {
            (Some(_), None, None, Some(_)) => false,  // definite does not dominate uncertain
            (None, Some(_), Some(_), None) => false,  // uncertain does not dominate definite
            _ => true,
        };
        epistemic_ok
            && self.time.dominates(&other.time)
            && self.mask.covers(&other.mask)
            && !self.is_less_informative_than(other)
    }
}

// ── triggers_recompute ────────────────────────────────────────────────────────

/// Full dep recompute predicate: observable AND semantically significant.
///
/// - `mask`:        the dep's `ProjectionMask` (what can be seen)
/// - `sensitivity`: the dep's `Sensitivity` (how to react)
/// - `delta`:       what changed
/// - `before`:      the state before the change
/// - `after`:       the state after the change
pub fn triggers_recompute<T: Clone + PartialEq>(
    mask:        &ProjectionMask,
    sensitivity: Sensitivity,
    delta:       &Delta,
    before:      &VersionedState<T>,
    after:       &VersionedState<T>,
) -> bool {
    // Gate 1: is this delta even observable through this dep's mask?
    if !mask.affects(delta) { return false; }

    // Gate 2: does this sensitivity react to the observed change?
    match sensitivity {
        Sensitivity::Exact => true,

        Sensitivity::Monotone => {
            // Only react to Pending → Definite transitions.
            // Ignore retractions (Definite → Pending) and Zero signals.
            before.class() == StateClass::Pending
                && after.class() == StateClass::Definite
        }

        Sensitivity::StableOnly => {
            // Only react when the result is fully stable (definite, no Zero).
            after.class() == StateClass::Definite
        }

        Sensitivity::Edge => {
            // React only on class *transitions*.
            before.class() != after.class()
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{delta::Delta, time::ProductTime, uid};

    fn t0() -> Time { ProductTime::ZERO }
    fn t1() -> Time { ProductTime { attribute: 1, ..Default::default() } }

    fn pending() -> VersionedState<i32> {
        VersionedState::pending(t0(), ProjectionMask::wildcard())
    }
    fn definite(v: i32) -> VersionedState<i32> {
        VersionedState::definite(v, t1(), ProjectionMask::wildcard())
    }
    fn uncertain() -> VersionedState<i32> {
        VersionedState::uncertain(ZeroKind::Conflict, t1(), ProjectionMask::wildcard())
    }

    #[test]
    fn test_state_class() {
        assert_eq!(pending().class(),    StateClass::Pending);
        assert_eq!(definite(1).class(),  StateClass::Definite);
        assert_eq!(uncertain().class(),  StateClass::Uncertain);
    }

    #[test]
    fn test_versioned_state_dominates() {
        let a = VersionedState::definite(1i32, t1(), ProjectionMask::wildcard());
        let b = VersionedState::pending(t0(), ProjectionMask::wildcard());
        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn test_val_and_zero_incomparable() {
        // Val and Zero are incomparable — different kinds of knowledge
        let val_state = definite(1i32);
        let zero_state = uncertain();
        assert!(!val_state.dominates(&zero_state));
        assert!(!zero_state.dominates(&val_state));
    }

    #[test]
    fn test_exact_sensitivity() {
        let id = uid::fresh();
        let d = Delta::attribute(id);
        let m = ProjectionMask::wildcard();
        assert!(triggers_recompute(&m, Sensitivity::Exact, &d, &pending(), &definite(1)));
        assert!(triggers_recompute(&m, Sensitivity::Exact, &d, &definite(1), &pending()));
    }

    #[test]
    fn test_monotone_sensitivity() {
        let id = uid::fresh();
        let d = Delta::attribute(id);
        let m = ProjectionMask::wildcard();
        // Pending → Definite: triggers
        assert!(triggers_recompute(&m, Sensitivity::Monotone, &d, &pending(), &definite(1)));
        // Definite → Pending (retraction): does NOT trigger
        assert!(!triggers_recompute(&m, Sensitivity::Monotone, &d, &definite(1), &pending()));
        // Pending → Zero: does NOT trigger
        assert!(!triggers_recompute(&m, Sensitivity::Monotone, &d, &pending(), &uncertain()));
    }

    #[test]
    fn test_stable_only_sensitivity() {
        let id = uid::fresh();
        let d = Delta::attribute(id);
        let m = ProjectionMask::wildcard();
        // Only fires when after is Definite
        assert!(triggers_recompute(&m, Sensitivity::StableOnly, &d, &pending(), &definite(1)));
        assert!(!triggers_recompute(&m, Sensitivity::StableOnly, &d, &pending(), &uncertain()));
        assert!(!triggers_recompute(&m, Sensitivity::StableOnly, &d, &definite(1), &pending()));
    }

    #[test]
    fn test_edge_sensitivity() {
        let id = uid::fresh();
        let d = Delta::attribute(id);
        let m = ProjectionMask::wildcard();
        // Transition: fires
        assert!(triggers_recompute(&m, Sensitivity::Edge, &d, &pending(), &definite(1)));
        assert!(triggers_recompute(&m, Sensitivity::Edge, &d, &definite(1), &uncertain()));
        // Same class (Definite → Definite): does NOT fire
        assert!(!triggers_recompute(&m, Sensitivity::Edge, &d, &definite(1), &definite(2)));
    }

    #[test]
    fn test_mask_gates_sensitivity() {
        let id = uid::fresh();
        let structural_delta = Delta::structural(id);
        let attr_only_mask = ProjectionMask::attribute_only();
        // Structural delta does not pass an attr-only mask, regardless of sensitivity
        assert!(!triggers_recompute(
            &attr_only_mask, Sensitivity::Exact, &structural_delta, &pending(), &definite(1)
        ));
    }
}
