//! Product timestamp — the five-dimensional causal clock.
//!
//! Each dimension tracks a distinct kind of change independently.
//! The product order gives a partial order: `t ≤ t'` iff every component
//! is ≤. This means changes along orthogonal axes are incomparable, which
//! is exactly right — an attribute change and a dependency change are not
//! causally ordered unless both clocks advance.
//!
//! ## Dimensions
//!
//! | Dim        | Advances when                                      |
//! |------------|----------------------------------------------------|
//! | identity   | NodeCreate / DelNode / EdgeConnect / DelEdge        |
//! | attribute  | SetValue / Reflect / Stabilize (== Node.version)   |
//! | dependency | EdgeConnect / DelEdge / Subscribe                   |
//! | egraph     | EClassSummary.version bumps                         |
//! | boundary   | emit() fires (value crosses partition boundary)     |

use rustc_hash::FxHashMap;
use crate::partition::PartitionId;

// ── Time dimensions ───────────────────────────────────────────────────────────

/// A single named time dimension.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeDim {
    Identity   = 0,
    Attribute  = 1,
    Dependency = 2,
    Egraph     = 3,
    Boundary   = 4,
}

/// Bitmask of time dimensions. Used in `ProjectionMask` and `Delta`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct TimeDimSet(u8);

impl TimeDimSet {
    pub const NONE:       Self = Self(0b00000);
    pub const IDENTITY:   Self = Self(0b00001);
    pub const ATTRIBUTE:  Self = Self(0b00010);
    pub const DEPENDENCY: Self = Self(0b00100);
    pub const EGRAPH:     Self = Self(0b01000);
    pub const BOUNDARY:   Self = Self(0b10000);
    pub const ALL:        Self = Self(0b11111);

    pub fn contains(self, dim: TimeDim) -> bool {
        (self.0 >> (dim as u8)) & 1 == 1
    }

    pub fn intersects(self, other: Self) -> bool {
        (self.0 & other.0) != 0
    }

    pub fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub fn insert(self, dim: TimeDim) -> Self {
        Self(self.0 | (1 << dim as u8))
    }

    pub fn is_empty(self) -> bool { self.0 == 0 }

    /// Raw bitmask — for superset checks in ProjectionMask::covers.
    pub fn bits(self) -> u8 { self.0 }
}

// ── Product timestamp ─────────────────────────────────────────────────────────

/// Five-dimensional product timestamp. Partially ordered component-wise.
///
/// `a.dominates(b)` iff every component of `a` ≥ the corresponding component
/// of `b`. Non-comparable timestamps form antichains.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct ProductTime {
    pub identity:   u64,
    pub attribute:  u64,
    pub dependency: u64,
    pub egraph:     u64,
    pub boundary:   u64,
}

pub type Time = ProductTime;

impl ProductTime {
    pub const ZERO: Self = Self { identity: 0, attribute: 0, dependency: 0, egraph: 0, boundary: 0 };

    /// Component-wise ≥ — this is the partial order dominance relation.
    pub fn dominates(&self, other: &Self) -> bool {
        self.identity   >= other.identity
            && self.attribute  >= other.attribute
            && self.dependency >= other.dependency
            && self.egraph     >= other.egraph
            && self.boundary   >= other.boundary
    }

    /// Component-wise minimum — greatest lower bound (meet).
    pub fn meet(&self, other: &Self) -> Self {
        Self {
            identity:   self.identity.min(other.identity),
            attribute:  self.attribute.min(other.attribute),
            dependency: self.dependency.min(other.dependency),
            egraph:     self.egraph.min(other.egraph),
            boundary:   self.boundary.min(other.boundary),
        }
    }

    /// Component-wise maximum — least upper bound (join).
    pub fn join(&self, other: &Self) -> Self {
        Self {
            identity:   self.identity.max(other.identity),
            attribute:  self.attribute.max(other.attribute),
            dependency: self.dependency.max(other.dependency),
            egraph:     self.egraph.max(other.egraph),
            boundary:   self.boundary.max(other.boundary),
        }
    }

    /// Advance a single dimension by 1.
    pub fn advance(&self, dim: TimeDim) -> Self {
        let mut t = *self;
        match dim {
            TimeDim::Identity   => t.identity   += 1,
            TimeDim::Attribute  => t.attribute  += 1,
            TimeDim::Dependency => t.dependency += 1,
            TimeDim::Egraph     => t.egraph     += 1,
            TimeDim::Boundary   => t.boundary   += 1,
        }
        t
    }

    /// Retrieve a single dimension's value.
    pub fn get(&self, dim: TimeDim) -> u64 {
        match dim {
            TimeDim::Identity   => self.identity,
            TimeDim::Attribute  => self.attribute,
            TimeDim::Dependency => self.dependency,
            TimeDim::Egraph     => self.egraph,
            TimeDim::Boundary   => self.boundary,
        }
    }
}

// ── Bitemporal event stamping ─────────────────────────────────────────────────

/// Which causal dimension advanced in a single ISA event, and to what counter.
///
/// Derivable from two consecutive `ProductTime` snapshots via `delta()`.
/// Exactly one dimension should advance per ISA op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaTime {
    pub dim:     TimeDim,
    pub counter: u64,
}

/// Bitemporal stamp attached to every engine event.
///
/// - `causal`:  the `ProductTime` after the op — answers "should this propagate?"
///              (gating mask-delta comparisons)
/// - `arrival`: monotone scalar — answers "in what order did events occur?"
///              (trace witness, oscillation detection, `PortHistory`)
///
/// The two orderings are intentionally separate. `arrival` is a total order
/// for debugging; `causal` is a partial order for correctness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventStamp {
    pub causal:  ProductTime,
    pub arrival: u64,
}

/// Derive the `DeltaTime` from two consecutive causal timestamps.
///
/// Inspects which single dimension advanced. If multiple dimensions advanced
/// (should not happen under the one-dim-per-op invariant) the first one found
/// in declaration order is returned.
pub fn delta(prev: &ProductTime, curr: &ProductTime) -> DeltaTime {
    if curr.identity   > prev.identity   { return DeltaTime { dim: TimeDim::Identity,   counter: curr.identity   }; }
    if curr.dependency > prev.dependency { return DeltaTime { dim: TimeDim::Dependency, counter: curr.dependency }; }
    if curr.attribute  > prev.attribute  { return DeltaTime { dim: TimeDim::Attribute,  counter: curr.attribute  }; }
    if curr.egraph     > prev.egraph     { return DeltaTime { dim: TimeDim::Egraph,     counter: curr.egraph     }; }
    DeltaTime { dim: TimeDim::Boundary, counter: curr.boundary }
}

// ── Frontier (antichain of Times) ─────────────────────────────────────────────

/// An antichain of `Time` values — the minimal set of incomparable elements
/// representing the "frontier" of processed work.
///
/// Invariant: no element dominates any other in the set.
///
/// Used for:
/// - GC: causal_frontier in `RemoteDep` — what the remote partition has processed
/// - Dep windows: `Dep.until` — when this dep expires
/// - Stream acknowledgment: what the receiver has consumed
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Frontier(Vec<Time>);

impl Frontier {
    pub fn new() -> Self { Self::default() }

    /// Returns true if this frontier dominates `t` — i.e., `t` is "behind" the frontier.
    pub fn dominates(&self, t: &Time) -> bool {
        self.0.iter().any(|f| f.dominates(t))
    }

    /// Advance the frontier to include `t`, maintaining the antichain invariant.
    /// Removes any existing elements dominated by `t`, then inserts `t` if not
    /// already dominated.
    pub fn advance(&mut self, t: Time) {
        if self.dominates(&t) { return; }
        self.0.retain(|existing| !t.dominates(existing));
        self.0.push(t);
    }

    /// Merge two frontiers into their join (least upper bound).
    pub fn join(&self, other: &Frontier) -> Frontier {
        let mut result = self.clone();
        for t in &other.0 { result.advance(*t); }
        result
    }

    /// The meet of two frontiers (greatest lower bound) — element-wise min.
    pub fn meet(&self, other: &Frontier) -> Frontier {
        // Meet of antichains: all meets of pairs from the two antichains,
        // then reduced to an antichain.
        let mut result = Frontier::new();
        for a in &self.0 {
            for b in &other.0 {
                result.advance(a.meet(b));
            }
        }
        result
    }

    pub fn elements(&self) -> &[Time] { &self.0 }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

impl From<Time> for Frontier {
    fn from(t: Time) -> Self {
        let mut f = Frontier::new();
        f.advance(t);
        f
    }
}

// ── Vector clock ──────────────────────────────────────────────────────────────

/// Per-partition logical clock for causal ordering (I5).
///
/// Records the latest known time for each partition. Used in `RemoteDep`
/// to annotate the causal state of the emitting partition at emission time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VectorClock(FxHashMap<PartitionId, u64>);

impl VectorClock {
    pub fn new() -> Self { Self::default() }

    pub fn get(&self, partition: PartitionId) -> u64 {
        self.0.get(&partition).copied().unwrap_or(0)
    }

    pub fn tick(&mut self, partition: PartitionId) {
        *self.0.entry(partition).or_insert(0) += 1;
    }

    pub fn set(&mut self, partition: PartitionId, v: u64) {
        self.0.insert(partition, v);
    }

    /// Merge (component-wise max). Used to advance a local clock upon receiving a message.
    pub fn merge(&mut self, other: &VectorClock) {
        for (&partition, &v) in &other.0 {
            let entry = self.0.entry(partition).or_insert(0);
            *entry = (*entry).max(v);
        }
    }

    /// Returns true if self causally dominates other (component-wise ≥).
    pub fn dominates(&self, other: &VectorClock) -> bool {
        other.0.iter().all(|(&p, &v)| self.get(p) >= v)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_dominates() {
        let a = ProductTime { identity: 2, attribute: 3, dependency: 1, egraph: 0, boundary: 0 };
        let b = ProductTime { identity: 1, attribute: 2, dependency: 1, egraph: 0, boundary: 0 };
        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn test_time_incomparable() {
        let a = ProductTime { attribute: 2, ..Default::default() };
        let b = ProductTime { dependency: 1, ..Default::default() };
        assert!(!a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn test_frontier_antichain_invariant() {
        let mut f = Frontier::new();
        let t1 = ProductTime { attribute: 1, ..Default::default() };
        let t2 = ProductTime { dependency: 1, ..Default::default() };
        let t3 = ProductTime { attribute: 2, ..Default::default() }; // dominates t1

        f.advance(t1);
        f.advance(t2);
        assert_eq!(f.elements().len(), 2); // incomparable

        f.advance(t3); // dominates t1, so t1 is removed
        assert_eq!(f.elements().len(), 2); // t3 + t2
        assert!(!f.elements().contains(&t1));
    }

    #[test]
    fn test_frontier_dominates() {
        let mut f = Frontier::new();
        f.advance(ProductTime { attribute: 5, ..Default::default() });
        assert!(f.dominates(&ProductTime { attribute: 3, ..Default::default() }));
        assert!(!f.dominates(&ProductTime { attribute: 6, ..Default::default() }));
    }

    #[test]
    fn test_vector_clock_dominates() {
        let p1 = uuid::Uuid::new_v4();
        let p2 = uuid::Uuid::new_v4();
        let mut a = VectorClock::new();
        let mut b = VectorClock::new();
        a.set(p1, 3); a.set(p2, 2);
        b.set(p1, 2); b.set(p2, 2);
        assert!(a.dominates(&b));
        assert!(!b.dominates(&a));
    }

    #[test]
    fn test_timedimset_ops() {
        let s = TimeDimSet::ATTRIBUTE.union(TimeDimSet::DEPENDENCY);
        assert!(s.intersects(TimeDimSet::ATTRIBUTE));
        assert!(s.intersects(TimeDimSet::DEPENDENCY));
        assert!(!s.intersects(TimeDimSet::EGRAPH));
        assert!(s.contains(TimeDim::Attribute));
        assert!(!s.contains(TimeDim::Egraph));
    }
}
