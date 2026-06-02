//! Dense value cache — Opt 1: eliminate per-dep im::HashMap lookups in the hot path.
//!
//! The propagation hot path (`step_push`) historically called `graph.node(dep_id)`
//! for every upstream dep, which hits the HAMT (O(1) amortized but ~10× slower than
//! a direct array index due to hash + pointer chase).
//!
//! `ValueStore` assigns each node a compact `u32` index on creation and maintains a
//! flat `Vec<T>`. Dep-value reads in step_push become `cache[idx]` — a single load
//! from a hot cache line instead of a hash lookup.
//!
//! ## Invariant
//!
//! `ValueStore::cache[idx]` and `Graph::node(id).value` are always identical for
//! live nodes. Every code path that calls `Node::set_value` or `Node::reflect` MUST
//! also call `ValueStore::set` with the same new value.
//!
//! ## Tombstones
//!
//! Deleted nodes leave a `T::Neg` slot. The slot is never reused. Because
//! `DepRegistry::unsubscribe` removes the dep index from all subscriber
//! `ordered_dep_indices` lists before the node is dropped, no live dep list
//! will ever reference a tombstone slot.
//!
//! ## Bit-sliced planes (Opt 10)
//!
//! Two parallel `Vec<u64>` planes over the dense cache enable SWAR (SIMD Within A
//! Register) bulk operations for compiled regions:
//!
//! * `plane_p0[w]` — bit `j` is set iff `cache[w*64 + j] == T::Pos`  (repr bit 0)
//! * `plane_p1[w]` — bit `j` is set iff `cache[w*64 + j] == T::Zero` (repr bit 1)
//!
//! Invariant: `cache[d] as u8 == (p0_bit(d) | (p1_bit(d) << 1))`.
//!
//! `meet_all_range` and `join_any_range` operate on these planes with ~3 word-level
//! ops per 64-slot window instead of 64 byte loads.

use rustc_hash::FxHashMap;
use crate::{ternary::T, uid::Uid};

/// Dense ternary value cache: `Uid → u32 → T`.
#[derive(Debug, Default)]
pub struct ValueStore {
    /// Flat cache: dense index → current T value. Default slot = T::Neg.
    cache: Vec<T>,
    /// Uid → dense index (stable for the node's lifetime).
    idx:   FxHashMap<Uid, u32>,
    /// Bit-sliced Pos plane: bit j of word w is set iff cache[w*64+j] == T::Pos.
    plane_p0: Vec<u64>,
    /// Bit-sliced Zero plane: bit j of word w is set iff cache[w*64+j] == T::Zero.
    plane_p1: Vec<u64>,
}

impl ValueStore {
    pub fn new() -> Self { Self::default() }

    /// Allocate a new slot for `id`, initialised to `T::Neg`.
    /// Returns the dense index — callers should pass this to
    /// `DepRegistry::subscribe_with_meta_indexed` so subscriptions record it.
    #[inline]
    pub fn alloc(&mut self, id: Uid) -> u32 {
        let dense = self.cache.len() as u32;
        self.cache.push(T::Neg);
        self.idx.insert(id, dense);
        // Grow planes lazily (T::Neg has repr 0, so new words start at 0 — correct).
        let needed_words = (self.cache.len() + 63) / 64;
        while self.plane_p0.len() < needed_words {
            self.plane_p0.push(0u64);
            self.plane_p1.push(0u64);
        }
        dense
    }

    /// Update the cached value for `id`.
    ///
    /// Called after every `Node::set_value` / `Node::reflect` / propagation write.
    /// O(1) hash lookup then direct array write + plane update.
    #[inline]
    pub fn set(&mut self, id: Uid, val: T) {
        if let Some(&dense) = self.idx.get(&id) {
            self.set_by_dense(dense, val);
        }
    }

    /// Update the cached value by dense index (bypasses the Uid→dense lookup).
    ///
    /// Used by `run_compiled_region` for output writes during the hot path.
    #[inline]
    pub fn set_by_dense(&mut self, dense: u32, val: T) {
        let d = dense as usize;
        let (word, bit) = (d / 64, d % 64);
        let mask = 1u64 << bit;
        let repr = val as u8; // Neg=0b00, Pos=0b01, Zero=0b10
        // p0 = repr & 1 → 1 iff Pos;  p1 = repr >> 1 → 1 iff Zero
        if repr & 1 != 0 {
            self.plane_p0[word] |= mask;
        } else {
            self.plane_p0[word] &= !mask;
        }
        if repr >> 1 != 0 {
            self.plane_p1[word] |= mask;
        } else {
            self.plane_p1[word] &= !mask;
        }
        // SAFETY: dense was assigned as `cache.len()` at alloc time; cache only grows.
        *unsafe { self.cache.get_unchecked_mut(d) } = val;
    }

    /// Read the cached value for `id`. Returns `T::Neg` if unknown.
    #[inline(always)]
    pub fn get(&self, id: Uid) -> T {
        match self.idx.get(&id) {
            Some(&d) => unsafe { *self.cache.get_unchecked(d as usize) },
            None     => T::Neg,
        }
    }

    /// Read the cached value by dense index.
    ///
    /// Caller must ensure `dense < self.len()`.
    #[inline(always)]
    pub fn get_by_dense(&self, dense: u32) -> T {
        // SAFETY: dense indices are assigned sequentially and never exceed cache length.
        unsafe { *self.cache.get_unchecked(dense as usize) }
    }

    /// Dense index for `id`, or `None` if not registered.
    #[inline(always)]
    pub fn dense_index(&self, id: Uid) -> Option<u32> {
        self.idx.get(&id).copied()
    }

    /// Number of allocated slots.
    #[inline(always)]
    pub fn len(&self) -> usize { self.cache.len() }

    /// Direct access to the raw cache slice.
    ///
    /// Used by `step_push` for direct index reads (dep_indices → cache[idx])
    /// and by the software prefetch path.
    #[inline(always)]
    pub fn cache_slice(&self) -> &[T] { &self.cache }

    /// Mutable slice over the dense cache — used by legacy callers that write
    /// directly into the cache without going through `set_by_dense`.
    ///
    /// **Note:** writes via this slice do NOT update the bit-sliced planes.
    /// Use `set_by_dense` for writes that must keep planes in sync.
    #[inline(always)]
    pub fn cache_slice_mut(&mut self) -> &mut Vec<T> { &mut self.cache }

    /// Mark a slot as tombstone on node deletion (sets to `T::Neg`).
    ///
    /// The `idx` entry is left intact so that any stale dep-index references
    /// read `T::Neg` (Pending) rather than out-of-bounds. Those stale entries
    /// are cleared by `DepRegistry::unsubscribe` / `remove_node` before this
    /// method is called.
    #[inline]
    pub fn dealloc(&mut self, id: Uid) {
        if let Some(&dense) = self.idx.get(&id) {
            self.set_by_dense(dense, T::Neg);
        }
    }

    // ── SWAR bulk operations (Opt 10) ─────────────────────────────────────────

    /// Meet-all (lattice min) over dense slots `[start, start+len)`.
    ///
    /// Operates on bit-sliced planes: O(len/64) word-level AND/OR operations.
    /// Returns `T::Pos` for an empty range (identity element for meet).
    ///
    /// Semantics match `T::meet_all` on the equivalent byte slice.
    pub fn meet_all_range(&self, start: u32, len: u32) -> T {
        if len == 0 { return T::Pos; }
        let s = start as usize;
        let e = s + len as usize;
        let w0 = s / 64;
        let w1 = (e + 63) / 64;
        let mut has_zero = false;
        for w in w0..w1.min(self.plane_p0.len()) {
            let mask = range_mask(s, e, w);
            if mask == 0 { continue; }
            let p0m = self.plane_p0[w] & mask;
            let p1m = self.plane_p1[w] & mask;
            // Neg slot within range: p0=0 AND p1=0.
            // In the masked region, a slot is Neg if it contributes 0 to BOTH planes.
            // neg_bits = bits that are 0 in p0 AND 0 in p1 (but within mask).
            let neg_bits = (!p0m) & (!p1m) & mask;
            if neg_bits != 0 { return T::Neg; } // any Neg → meet = Neg
            // Zero slot: p0=0 AND p1=1
            if (!p0m) & p1m != 0 { has_zero = true; }
        }
        if has_zero { T::Zero } else { T::Pos }
    }

    /// Join-any (lattice max) over dense slots `[start, start+len)`.
    ///
    /// Operates on bit-sliced planes: O(len/64) word-level ops.
    /// Returns `T::Neg` for an empty range (identity element for join).
    ///
    /// Semantics match `T::join_any` on the equivalent byte slice.
    pub fn join_any_range(&self, start: u32, len: u32) -> T {
        if len == 0 { return T::Neg; }
        let s = start as usize;
        let e = s + len as usize;
        let w0 = s / 64;
        let w1 = (e + 63) / 64;
        let mut has_zero = false;
        for w in w0..w1.min(self.plane_p0.len()) {
            let mask = range_mask(s, e, w);
            if mask == 0 { continue; }
            let p0m = self.plane_p0[w] & mask;
            let p1m = self.plane_p1[w] & mask;
            // Pos slot: p0=1 AND p1=0
            let pos_bits = p0m & (!p1m);
            if pos_bits != 0 { return T::Pos; } // any Pos → join = Pos
            // Zero slot: p0=0 AND p1=1
            if (!p0m) & p1m != 0 { has_zero = true; }
        }
        if has_zero { T::Zero } else { T::Neg }
    }

    /// Construct a `ValueStore` directly from a raw cache vec.
    ///
    /// The `idx` map is left empty — only dense-index accessors (`get_by_dense`,
    /// `set_by_dense`, `meet_all_range`, `join_any_range`) work on the result.
    /// Used in tests and benchmarks that operate on pre-allocated dense slots.
    #[cfg(test)]
    pub fn from_raw_cache(cache: Vec<T>) -> Self {
        let n = cache.len();
        let words = (n + 63) / 64;
        let mut plane_p0 = vec![0u64; words];
        let mut plane_p1 = vec![0u64; words];
        for (i, &val) in cache.iter().enumerate() {
            let repr = val as u8;
            let (word, bit) = (i / 64, i % 64);
            if repr & 1 != 0 { plane_p0[word] |= 1u64 << bit; }
            if repr >> 1 != 0 { plane_p1[word] |= 1u64 << bit; }
        }
        Self { cache, idx: Default::default(), plane_p0, plane_p1 }
    }
}

/// Bitmask for the bits within word `w` (covering dense slots `[w*64, (w+1)*64)`)
/// that fall in the range `[start, end)`.
///
/// Returns 0 when the word's range and [start, end) do not overlap.
#[inline]
fn range_mask(start: usize, end: usize, w: usize) -> u64 {
    let w_lo = w * 64;
    let w_hi = w_lo + 64;
    if start >= w_hi || end <= w_lo { return 0; }
    let lo = start.max(w_lo) - w_lo; // in [0, 64)
    let hi = end.min(w_hi) - w_lo;   // in (0, 64]
    // Bits [lo, hi) within this 64-bit word.
    let lo_mask: u64 = if lo == 0 { u64::MAX } else { u64::MAX << lo };
    let hi_mask: u64 = if hi == 64 { u64::MAX } else { (1u64 << hi) - 1 };
    lo_mask & hi_mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_mask_full_word() {
        assert_eq!(range_mask(0, 64, 0), u64::MAX);
        assert_eq!(range_mask(64, 128, 1), u64::MAX);
    }

    #[test]
    fn range_mask_partial() {
        // Bits 1..3 of word 0 → 0b110 = 6
        assert_eq!(range_mask(1, 3, 0), 0b110);
        // Out of range
        assert_eq!(range_mask(0, 64, 1), 0);
        assert_eq!(range_mask(64, 128, 0), 0);
    }

    #[test]
    fn meet_all_range_basic() {
        // All Pos → Pos
        let vs = ValueStore::from_raw_cache(vec![T::Pos, T::Pos, T::Pos]);
        assert_eq!(vs.meet_all_range(0, 3), T::Pos);
        // Any Neg → Neg
        let vs = ValueStore::from_raw_cache(vec![T::Pos, T::Neg, T::Pos]);
        assert_eq!(vs.meet_all_range(0, 3), T::Neg);
        // No Neg but has Zero → Zero
        let vs = ValueStore::from_raw_cache(vec![T::Pos, T::Zero, T::Pos]);
        assert_eq!(vs.meet_all_range(0, 3), T::Zero);
        // Empty range → Pos (identity)
        let vs = ValueStore::from_raw_cache(vec![T::Neg]);
        assert_eq!(vs.meet_all_range(0, 0), T::Pos);
        // Single Neg
        let vs = ValueStore::from_raw_cache(vec![T::Neg]);
        assert_eq!(vs.meet_all_range(0, 1), T::Neg);
    }

    #[test]
    fn join_any_range_basic() {
        // All Neg → Neg
        let vs = ValueStore::from_raw_cache(vec![T::Neg, T::Neg, T::Neg]);
        assert_eq!(vs.join_any_range(0, 3), T::Neg);
        // Any Pos → Pos
        let vs = ValueStore::from_raw_cache(vec![T::Neg, T::Pos, T::Neg]);
        assert_eq!(vs.join_any_range(0, 3), T::Pos);
        // No Pos but has Zero → Zero
        let vs = ValueStore::from_raw_cache(vec![T::Neg, T::Zero, T::Neg]);
        assert_eq!(vs.join_any_range(0, 3), T::Zero);
        // Empty range → Neg (identity)
        let vs = ValueStore::from_raw_cache(vec![T::Pos]);
        assert_eq!(vs.join_any_range(0, 0), T::Neg);
    }

    #[test]
    fn meet_all_range_subrange() {
        // cache: [Pos, Neg, Pos, Pos]; meet_all_range(2, 2) → meet(Pos, Pos) = Pos
        let vs = ValueStore::from_raw_cache(vec![T::Pos, T::Neg, T::Pos, T::Pos]);
        assert_eq!(vs.meet_all_range(2, 2), T::Pos);
        // meet_all_range(1, 2) → meet(Neg, Pos) = Neg
        assert_eq!(vs.meet_all_range(1, 2), T::Neg);
    }

    #[test]
    fn set_by_dense_updates_planes() {
        let mut vs = ValueStore::from_raw_cache(vec![T::Neg, T::Neg]);
        vs.set_by_dense(0, T::Pos);
        assert_eq!(vs.get_by_dense(0), T::Pos);
        assert_eq!(vs.meet_all_range(0, 2), T::Neg); // slot 1 still Neg
        vs.set_by_dense(1, T::Pos);
        assert_eq!(vs.meet_all_range(0, 2), T::Pos);
        // Now write Zero to slot 0 — meet should become Zero (no Neg, has Zero)
        vs.set_by_dense(0, T::Zero);
        assert_eq!(vs.meet_all_range(0, 2), T::Zero);
    }

    #[test]
    fn planes_consistent_with_cache() {
        let vals = [T::Neg, T::Pos, T::Zero, T::Pos, T::Neg, T::Zero];
        let vs = ValueStore::from_raw_cache(vals.to_vec());
        for (i, &expected) in vals.iter().enumerate() {
            assert_eq!(vs.get_by_dense(i as u32), expected, "slot {i}");
        }
        // Verify planes agree
        assert_eq!(vs.meet_all_range(1, 1), T::Pos); // [Pos]
        assert_eq!(vs.meet_all_range(2, 1), T::Zero); // [Zero]
        assert_eq!(vs.meet_all_range(0, 1), T::Neg); // [Neg]
        assert_eq!(vs.meet_all_range(1, 2), T::Zero); // meet(Pos, Zero) = Zero
        assert_eq!(vs.join_any_range(0, 2), T::Pos);  // join(Neg, Pos) = Pos
    }
}
