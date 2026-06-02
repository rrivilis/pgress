//! MaskRegistry — interned ProjectionMasks.
//!
//! Two deps with the same `ProjectionMask` share a `MaskId`. The emitting
//! partition groups active subscriptions by `MaskId` and runs the `affects`
//! check once per unique mask rather than once per subscriber.
//!
//! This is the mechanism that keeps subscription fan-out proportional to
//! unique mask count, not to total subscriber count.

use rustc_hash::FxHashMap;
use crate::delta::ProjectionMask;

/// A cheap, copy-able handle to an interned `ProjectionMask`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct MaskId(pub u32);

/// Registry that interns `ProjectionMask` values to `MaskId` handles.
///
/// `intern` is idempotent: calling it twice with the same mask returns the
/// same `MaskId`. `resolve` retrieves the mask for a given ID.
#[derive(Debug, Default)]
pub struct MaskRegistry {
    by_mask: FxHashMap<ProjectionMask, MaskId>,
    by_id:   Vec<ProjectionMask>,
}

impl MaskRegistry {
    pub fn new() -> Self { Self::default() }

    /// Intern a mask, returning a stable `MaskId`.
    /// If the mask has been seen before, returns the existing ID.
    pub fn intern(&mut self, mask: ProjectionMask) -> MaskId {
        if let Some(&id) = self.by_mask.get(&mask) {
            return id;
        }
        let id = MaskId(self.by_id.len() as u32);
        self.by_id.push(mask.clone());
        self.by_mask.insert(mask, id);
        id
    }

    /// Retrieve the mask for a given `MaskId`.
    /// Panics if the ID was not issued by this registry.
    pub fn resolve(&self, id: MaskId) -> &ProjectionMask {
        &self.by_id[id.0 as usize]
    }

    /// Number of distinct interned masks.
    pub fn len(&self) -> usize { self.by_id.len() }
    pub fn is_empty(&self) -> bool { self.by_id.is_empty() }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::AttrPathSet;
    use crate::time::TimeDimSet;

    #[test]
    fn test_intern_same_mask_same_id() {
        let mut reg = MaskRegistry::new();
        let m1 = ProjectionMask::wildcard();
        let m2 = ProjectionMask::wildcard();
        assert_eq!(reg.intern(m1), reg.intern(m2));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_intern_different_masks_different_ids() {
        let mut reg = MaskRegistry::new();
        let m1 = ProjectionMask::wildcard();
        let m2 = ProjectionMask::attribute_only();
        let id1 = reg.intern(m1);
        let id2 = reg.intern(m2);
        assert_ne!(id1, id2);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn test_resolve_roundtrip() {
        let mut reg = MaskRegistry::new();
        let mask = ProjectionMask {
            dims:  TimeDimSet::ATTRIBUTE.union(TimeDimSet::EGRAPH),
            attrs: AttrPathSet::from_paths(["weight"]),
            shape: None,
        };
        let id = reg.intern(mask.clone());
        assert_eq!(reg.resolve(id), &mask);
    }

    #[test]
    fn test_intern_idempotent() {
        let mut reg = MaskRegistry::new();
        let m = ProjectionMask::boundary();
        let id1 = reg.intern(m.clone());
        let id2 = reg.intern(m.clone());
        let id3 = reg.intern(m);
        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
        assert_eq!(reg.len(), 1);
    }
}
