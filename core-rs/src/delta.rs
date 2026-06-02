//! Delta, ProjectionMask, ShapeMask — the observability layer.
//!
//! Every ISA mutation produces a `Delta` annotating what changed and along
//! which dimensions. Every `Dep` carries a `ProjectionMask` declaring which
//! deltas it cares about. The `affects` predicate is the O(1) gate that
//! prevents fan-out recompute from propagating through deps that don't
//! observe the change.
//!
//! ## Predicate structure
//!
//! ```text
//! affects(dep, delta) =
//!     dep.mask.dims.intersects(delta.dims)          // time axis visible?
//!     && dep.mask.attrs.matches(delta.attrs)         // attr path visible?
//!     && dep.mask.shape.matches(delta.shape)         // structural aspect visible?
//! ```
//!
//! `ProjectionMask` answers: **what can be seen** at this dep.
//! `Sensitivity` (see sensitivity.rs) answers: **how to react** to what is seen.

use std::collections::BTreeSet;
use crate::{time::TimeDimSet, uid::Uid};

// ── ShapeMask — structural observability ─────────────────────────────────────

/// Bitmask of structural aspects of a node that are observable at a dep boundary.
///
/// | Bit | Aspect       | Advances when                              |
/// |-----|-------------|---------------------------------------------|
/// | 0   | eclass       | E-class membership changes (coarse)        |
/// | 1   | persistence  | Node/edge created or deleted (id_layer)    |
/// | 2   | emitted      | Value crosses a partition boundary         |
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct ShapeMask(u8);

impl ShapeMask {
    pub const NONE:        Self = Self(0b000);
    pub const ECLASS:      Self = Self(0b001);
    pub const PERSISTENCE: Self = Self(0b010);
    pub const EMITTED:     Self = Self(0b100);
    pub const ALL:         Self = Self(0b111);

    pub fn intersects(self, other: Self) -> bool { (self.0 & other.0) != 0 }
    pub fn contains(self, other: Self) -> bool   { (self.0 & other.0) == other.0 }
    pub fn union(self, other: Self) -> Self       { Self(self.0 | other.0) }
    pub fn is_empty(self) -> bool                 { self.0 == 0 }

    pub fn eclass(self) -> bool      { self.intersects(Self::ECLASS) }
    pub fn persistence(self) -> bool { self.intersects(Self::PERSISTENCE) }
    pub fn emitted(self) -> bool     { self.intersects(Self::EMITTED) }
}

// ── AttrPath / AttrPathSet ────────────────────────────────────────────────────

/// A path identifying a specific attribute (e.g. "weight", "label.color").
pub type AttrPath = String;

/// A sorted set of attribute paths. Sorted for deterministic hashing and
/// equality. Empty = wildcard (matches all attributes).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct AttrPathSet(BTreeSet<String>);

impl AttrPathSet {
    pub fn new() -> Self { Self::default() }

    /// Wildcard: empty set matches any attribute path.
    pub fn wildcard() -> Self { Self::new() }

    pub fn insert(&mut self, path: impl Into<String>) {
        self.0.insert(path.into());
    }

    pub fn from_paths(paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut s = Self::new();
        for p in paths { s.insert(p); }
        s
    }

    /// True if this set is a wildcard (empty = matches everything).
    pub fn is_wildcard(&self) -> bool { self.0.is_empty() }

    /// True if the dep's attr filter matches the delta's changed attrs.
    /// An empty dep filter matches any delta (wildcard).
    /// A non-empty dep filter requires at least one common path.
    pub fn matches(&self, delta_attrs: &AttrPathSet) -> bool {
        if self.is_wildcard() { return true; }
        if delta_attrs.is_wildcard() { return true; }
        self.0.iter().any(|p| delta_attrs.0.contains(p))
    }

    pub fn contains(&self, path: &str) -> bool { self.0.contains(path) }
    pub fn len(&self) -> usize { self.0.len() }
}

// ── ProjectionMask ────────────────────────────────────────────────────────────

/// Declares which changes are **observable** at a dep boundary.
///
/// - `dims`:  which time dimensions this dep cares about
/// - `attrs`: which attribute paths this dep cares about (empty = all)
/// - `shape`: which structural aspects this dep cares about (None = all)
///
/// Intended to be **interned** via `MaskRegistry` — two deps with the same mask
/// share an ID and a single subscription entry at the emitting partition.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct ProjectionMask {
    pub dims:  TimeDimSet,
    pub attrs: AttrPathSet,
    pub shape: Option<ShapeMask>,
}

impl ProjectionMask {
    /// Wildcard: observes everything.
    pub fn wildcard() -> Self {
        Self {
            dims:  TimeDimSet::ALL,
            attrs: AttrPathSet::wildcard(),
            shape: None,
        }
    }

    /// Observe only attribute changes (the common case for value deps).
    pub fn attribute_only() -> Self {
        Self {
            dims:  TimeDimSet::ATTRIBUTE,
            attrs: AttrPathSet::wildcard(),
            shape: None,
        }
    }

    /// Observe only id-layer changes (node/edge persistence).
    pub fn structural() -> Self {
        Self {
            dims:  TimeDimSet::IDENTITY.union(TimeDimSet::DEPENDENCY),
            attrs: AttrPathSet::wildcard(),
            shape: Some(ShapeMask::PERSISTENCE),
        }
    }

    /// Observe only boundary emissions.
    pub fn boundary() -> Self {
        Self {
            dims:  TimeDimSet::BOUNDARY,
            attrs: AttrPathSet::wildcard(),
            shape: Some(ShapeMask::EMITTED),
        }
    }

    /// O(1) gate: does this mask observe the given delta?
    pub fn affects(&self, delta: &Delta) -> bool {
        // 1. At least one time dimension must match
        if !self.dims.intersects(delta.dims) { return false; }
        // 2. Attribute filter (wildcard passes through)
        if !self.attrs.matches(&delta.attrs) { return false; }
        // 3. Shape filter (None = wildcard)
        if let Some(mask) = self.shape {
            if !mask.intersects(delta.shape) { return false; }
        }
        true
    }

    /// Whether this mask is a superset of another (covers at least as much).
    pub fn covers(&self, other: &ProjectionMask) -> bool {
        self.dims.bits() >= other.dims.bits()  // superset of dim bits
            && (self.attrs.is_wildcard()
                || other.attrs.0.iter().all(|p| self.attrs.contains(p)))
            && match (self.shape, other.shape) {
                (None, _)                   => true,
                (Some(_), None)             => false,
                (Some(a), Some(b))          => a.contains(b),
            }
    }
}

// ── Delta — what changed ─────────────────────────────────────────────────────

/// Annotates a single mutation: which node changed, along which dimensions,
/// on which attrs, with which structural aspects.
///
/// Produced by every ISA operation that mutates state. Consumed by
/// `ProjectionMask::affects` to gate downstream dep notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delta {
    /// The node or edge whose state changed.
    pub uid:   Uid,
    /// Which time dimensions advanced in this mutation.
    pub dims:  TimeDimSet,
    /// Which attribute paths changed (empty = none / not attr-driven).
    pub attrs: AttrPathSet,
    /// Which structural aspects changed.
    pub shape: ShapeMask,
}

impl Delta {
    /// A pure attribute/value change (the common case from SetValue/Reflect/Stabilize).
    pub fn attribute(uid: Uid) -> Self {
        Delta {
            uid,
            dims:  TimeDimSet::ATTRIBUTE,
            attrs: AttrPathSet::wildcard(),
            shape: ShapeMask::NONE,
        }
    }

    /// A specific attribute change (narrow projection).
    pub fn attr_path(uid: Uid, path: impl Into<String>) -> Self {
        let mut attrs = AttrPathSet::new();
        attrs.insert(path);
        Delta {
            uid,
            dims:  TimeDimSet::ATTRIBUTE,
            attrs,
            shape: ShapeMask::NONE,
        }
    }

    /// A structural change (node/edge created or deleted).
    pub fn structural(uid: Uid) -> Self {
        Delta {
            uid,
            dims:  TimeDimSet::IDENTITY,
            attrs: AttrPathSet::new(),
            shape: ShapeMask::PERSISTENCE,
        }
    }

    /// A dependency structure change (edge connected/disconnected).
    /// Carries `ShapeMask::PERSISTENCE` because the dep-graph topology changed.
    pub fn dependency(uid: Uid) -> Self {
        Delta {
            uid,
            dims:  TimeDimSet::DEPENDENCY,
            attrs: AttrPathSet::new(),
            shape: ShapeMask::PERSISTENCE,
        }
    }

    /// An e-graph equivalence class change.
    pub fn egraph(uid: Uid) -> Self {
        Delta {
            uid,
            dims:  TimeDimSet::EGRAPH,
            attrs: AttrPathSet::new(),
            shape: ShapeMask::ECLASS,
        }
    }

    /// A boundary emission (value crossed a partition boundary).
    pub fn boundary(uid: Uid) -> Self {
        Delta {
            uid,
            dims:  TimeDimSet::BOUNDARY,
            attrs: AttrPathSet::new(),
            shape: ShapeMask::EMITTED,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uid;

    #[test]
    fn test_shape_mask_bitwise() {
        assert!(ShapeMask::ALL.contains(ShapeMask::ECLASS));
        assert!(ShapeMask::ALL.contains(ShapeMask::PERSISTENCE));
        assert!(!ShapeMask::ECLASS.contains(ShapeMask::PERSISTENCE));
        assert!(ShapeMask::ECLASS.union(ShapeMask::PERSISTENCE).intersects(ShapeMask::PERSISTENCE));
    }

    #[test]
    fn test_attr_path_wildcard_matches_all() {
        let wildcard = AttrPathSet::wildcard();
        let specific = AttrPathSet::from_paths(["weight"]);
        assert!(wildcard.matches(&specific));
        assert!(specific.matches(&wildcard));
    }

    #[test]
    fn test_attr_path_specific_matches() {
        let a = AttrPathSet::from_paths(["weight", "color"]);
        let b = AttrPathSet::from_paths(["color", "opacity"]);
        let c = AttrPathSet::from_paths(["opacity"]);
        assert!(a.matches(&b));  // "color" in common
        assert!(!a.matches(&c)); // no intersection
    }

    #[test]
    fn test_affects_attribute_only_mask() {
        let id = uid::fresh();
        let mask = ProjectionMask::attribute_only();
        assert!(mask.affects(&Delta::attribute(id)));
        assert!(!mask.affects(&Delta::structural(id)));
        assert!(!mask.affects(&Delta::dependency(id)));
    }

    #[test]
    fn test_affects_wildcard_matches_all() {
        let id = uid::fresh();
        let mask = ProjectionMask::wildcard();
        assert!(mask.affects(&Delta::attribute(id)));
        assert!(mask.affects(&Delta::structural(id)));
        assert!(mask.affects(&Delta::egraph(id)));
        assert!(mask.affects(&Delta::boundary(id)));
    }

    #[test]
    fn test_affects_structural_mask() {
        let id = uid::fresh();
        let mask = ProjectionMask::structural();
        assert!(mask.affects(&Delta::structural(id)));
        assert!(mask.affects(&Delta::dependency(id)));
        assert!(!mask.affects(&Delta::attribute(id)));
        assert!(!mask.affects(&Delta::egraph(id)));
    }

    #[test]
    fn test_affects_shape_filter() {
        let id = uid::fresh();
        // Mask cares only about eclass changes
        let mask = ProjectionMask {
            dims:  TimeDimSet::EGRAPH,
            attrs: AttrPathSet::wildcard(),
            shape: Some(ShapeMask::ECLASS),
        };
        assert!(mask.affects(&Delta::egraph(id)));
        assert!(!mask.affects(&Delta::structural(id))); // PERSISTENCE not in shape mask
    }
}
