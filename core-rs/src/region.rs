//! Region compilation — explicit sparse circuit tier.
//!
//! A `CompiledRegion` freezes the topology of a subgraph at a given
//! `AdjEpoch` and represents the computation as a CSR sparse circuit:
//!
//!   - `members` / `rules`    : topologically-sorted nodes + their compute rules
//!   - `dep_offsets/dep_cols` : CSR adjacency (dense ValueStore indices)
//!   - `outputs`              : dense ValueStore index per member node
//!
//! Values are NOT frozen: `run_compiled_region` reads/writes live into
//! the `ValueStore`.
//!
//! ## Column-push inverted index (Opt 11)
//!
//! `col_inv_keys / col_inv_offsets / col_inv_rows` form a second CSR that
//! inverts the dep relationship: given a dirty input dense index, a binary
//! search on `col_inv_keys` yields the set of rows (in topo order) that must
//! be re-evaluated.  This replaces the O(N × avg_deps) row-scan in Opt 9 with
//! O(|dirty_inputs| × avg_fan_out) row selection — matching the warm path.
//!
//! ## SWAR evaluation (Opt 10)
//!
//! For rows whose deps are contiguous in the ValueStore (`contig_start` is
//! `Some(first_dense)`), `MeetAll` and `JoinAny` rules are evaluated via
//! `ValueStore::meet_all_range` / `join_any_range` (bit-sliced planes,
//! O(len/64) word ops) instead of gathering individual dep bytes.
//!
//! ## O(1) epoch check (Opt 12)
//!
//! `EpochTracked` regions are removed from `RegionArtifactCache::regions` by
//! `invalidate_for_node` on every topology mutation *and* removed from
//! `RegionArtifactCache::dirty` at the same time.  Therefore `take_dirty()`
//! never returns a root that lacks a valid artifact — the per-drain epoch
//! re-check that was O(N) hashes is dead work and has been removed.

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use crate::{
    node::ComputeRule,
    ternary::T,
    uid::Uid,
    value_store::ValueStore,
};

// ── Region declaration types ──────────────────────────────────────────────────

/// How to expand the region from its root anchor.
#[derive(Debug, Clone)]
pub enum RegionBoundary {
    /// BFS dep-closure up to `max_depth` hops from root.
    DepClosure { max_depth: usize },
    /// Exact set of nodes — user-specified, no expansion.
    ExplicitSet(Vec<Uid>),
    /// All nodes bound to a given partition.
    PartitionScoped(crate::partition::PartitionId),
}

impl RegionBoundary {
    /// Returns the explicit member list if this is an `ExplicitSet`.
    pub fn explicit_members(&self) -> Option<&[Uid]> {
        if let RegionBoundary::ExplicitSet(v) = self { Some(v) } else { None }
    }
}

/// Whether the engine epoch-tracks the region and invalidates on topology change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StabilityContract {
    /// User guarantees no rewrites; epoch tracking is skipped.
    Pinned,
    /// Engine invalidates the compiled artifact on any EdgeConnect / DelEdge /
    /// DelNode that touches a member node.
    EpochTracked,
}

/// When to produce the compiled circuit artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilePolicy {
    /// Compile immediately when `RegionDeclare` is processed.
    Eager,
    /// Compile on first hot invocation (placeholder — currently falls through
    /// to warm path on every drain until explicitly recompiled).
    Lazy,
    /// Epoch tracking only — no compiled artifact is produced.
    Never,
}

// ── Compiled artifact ─────────────────────────────────────────────────────────

/// Monotone adjacency-epoch counter.  Stored per node in `DepRegistry`.
pub type AdjEpoch = u64;

/// A compiled sparse circuit for a frozen subgraph region.
///
/// The topology (dep graph) is baked in at compile time.  Values are read
/// live from `ValueStore` at execution time.
///
/// Layout invariant: `members`, `rules`, `outputs`, and the row dimension of
/// the CSR (`dep_offsets` has `members.len() + 1` entries) are all parallel.
/// `contig_start` is also parallel to `members`.
#[derive(Debug)]
pub struct CompiledRegion {
    /// Member nodes in topological order (sources before sinks).
    pub members:     Box<[Uid]>,
    /// Compute rule for each member node (parallel to `members`).
    pub rules:       Box<[ComputeRule]>,
    /// Dense `ValueStore` index for each member node's output slot (parallel
    /// to `members`).
    pub outputs:     Box<[u32]>,
    /// Dense `ValueStore` indices for boundary input nodes (deps outside the
    /// region that feed into member nodes).
    pub inputs:      Box<[u32]>,
    /// CSR row pointers: deps of `members[i]` are at
    /// `dep_cols[dep_offsets[i]..dep_offsets[i+1]]`.
    /// Length = `members.len() + 1`.
    pub dep_offsets: Box<[u32]>,
    /// CSR column data: dense `ValueStore` indices of each dep.
    pub dep_cols:    Box<[u32]>,
    /// `AdjEpoch` at compile time — stored for reference; validity is
    /// enforced by `invalidate_for_node` removing stale artifacts before
    /// `drain_and_stabilize` runs.
    pub compiled_at: AdjEpoch,

    // ── Opt 11: column-push inverted index ───────────────────────────────────

    /// Sorted unique dep dense indices that appear in any row's dep list.
    /// Binary-searched to map a dirty input slot → affected rows.
    pub col_inv_keys:    Box<[u32]>,
    /// CSR offsets into `col_inv_rows`.  Length = `col_inv_keys.len() + 1`.
    pub col_inv_offsets: Box<[u32]>,
    /// Row indices (in topo order) per `col_inv_keys` entry.
    pub col_inv_rows:    Box<[u32]>,

    // ── Opt 10: SWAR contiguity hint ─────────────────────────────────────────

    /// For each member row: `Some(first_dense)` if the row's deps form a
    /// contiguous range `[first_dense, first_dense + dep_count)` in the
    /// ValueStore.  `None` for scattered deps or 0-dep rows.
    /// Enables `meet_all_range` / `join_any_range` SWAR dispatch.
    pub contig_start: Box<[Option<u32>]>,
}

// ── Artifact cache ────────────────────────────────────────────────────────────

/// Per-engine store of compiled region artifacts, keyed by region root `Uid`.
///
/// ## Dirty-flag mechanism (Opt 8)
///
/// `run_compiled_region` is O(N) over the member set.  Running it on every
/// `drain_and_stabilize` call is wasteful when the boundary inputs have not
/// changed — this was the ~2× slowdown vs. the warm path in benchmarks.
///
/// `RegionArtifactCache` maintains two complementary structures:
///
/// * `input_to_regions` — reverse map from a boundary input's dense
///   `ValueStore` index to the set of region roots that read it.
/// * `dirty` — set of region roots whose boundary inputs have changed since
///   the last execution.
///
/// `mark_dirty_for_input(dense)` is called by the engine after every explicit
/// `SetValue` or `Reflect` write to a node that is a boundary input for at
/// least one compiled region.  `take_dirty()` drains the set and returns
/// roots for the tier-2 dispatch in `drain_and_stabilize`.
///
/// ### Warm-path limitation
///
/// The dirty flag is set only for explicit ISA writes (`SetValue`, `Reflect`).
/// If the warm propagation engine updates a boundary-input node as a side
/// effect of a push, that update does NOT currently mark the region dirty —
/// the compiled region will pick up the change on the next explicit write.
/// This is acceptable for the primary use case (all inputs set explicitly).
#[derive(Debug, Default)]
pub struct RegionArtifactCache {
    /// Compiled artifacts.
    pub regions:        FxHashMap<Uid, CompiledRegion>,
    /// Stability contracts per root (needed for invalidation checks).
    pub contracts:      FxHashMap<Uid, StabilityContract>,
    /// Regions with at least one stale boundary input — need re-execution.
    dirty:              FxHashSet<Uid>,
    /// Reverse index: dense `ValueStore` input slot → region roots that read it.
    input_to_regions:   FxHashMap<u32, Vec<Uid>>,
}

impl RegionArtifactCache {
    pub fn new() -> Self { Self::default() }

    /// Store a compiled artifact under `root` with the given contract.
    ///
    /// Populates the reverse `input_to_regions` map so that future calls to
    /// `mark_dirty_for_input` can efficiently find which regions to mark.
    pub fn insert(&mut self, root: Uid, region: CompiledRegion, contract: StabilityContract) {
        // Build reverse map: boundary input → this root.
        for &dense in region.inputs.iter() {
            self.input_to_regions.entry(dense).or_default().push(root);
        }
        self.regions.insert(root, region);
        self.contracts.insert(root, contract);
    }

    /// Returns `true` if `root` has a compiled artifact valid at `current_epoch`.
    ///
    /// `Pinned` regions are always considered valid (user guarantees stability).
    pub fn is_valid(&self, root: Uid, current_epoch: AdjEpoch) -> bool {
        match self.contracts.get(&root) {
            Some(StabilityContract::Pinned) => self.regions.contains_key(&root),
            Some(StabilityContract::EpochTracked) => {
                self.regions.get(&root)
                    .map(|r| r.compiled_at == current_epoch)
                    .unwrap_or(false)
            }
            None => false,
        }
    }

    /// Invalidate all `EpochTracked` regions whose member set contains `node`.
    ///
    /// Called by the engine on every topology mutation (EdgeConnect, DelEdge,
    /// DelNode) that touches `node`.
    pub fn invalidate_for_node(&mut self, node: Uid) {
        let to_remove: Vec<Uid> = self.regions.iter()
            .filter(|(&root, r)| {
                matches!(self.contracts.get(&root), Some(StabilityContract::EpochTracked))
                    && r.members.contains(&node)
            })
            .map(|(&root, _)| root)
            .collect();
        for root in to_remove {
            // Clean up reverse map before removing the artifact.
            self.remove_reverse_map_for(root);
            self.regions.remove(&root);
            self.dirty.remove(&root);
            // Keep contract entry so re-declaration can reuse the root Uid.
        }
    }

    /// Remove `root`'s entries from the `input_to_regions` reverse map.
    fn remove_reverse_map_for(&mut self, root: Uid) {
        if let Some(region) = self.regions.get(&root) {
            for &dense in region.inputs.iter() {
                if let Some(roots) = self.input_to_regions.get_mut(&dense) {
                    roots.retain(|&r| r != root);
                }
            }
        }
    }

    /// Mark all regions that read `dense` as a boundary input as dirty.
    ///
    /// Call this after every explicit `SetValue` or `Reflect` write to a node
    /// whose dense `ValueStore` index is `dense`.  O(regions that use this input),
    /// which is typically 0–1.
    #[inline]
    pub fn mark_dirty_for_input(&mut self, dense: u32) {
        if let Some(roots) = self.input_to_regions.get(&dense) {
            for &root in roots {
                self.dirty.insert(root);
            }
        }
    }

    /// Drain the dirty set and return the region roots that need re-execution.
    ///
    /// Called once at the top of `drain_and_stabilize`.  The set is cleared here;
    /// the caller must not re-add roots unless a new boundary write occurs.
    #[inline]
    pub fn take_dirty(&mut self) -> Vec<Uid> {
        self.dirty.drain().collect()
    }
}

// ── Inverted index builder ────────────────────────────────────────────────────

/// Build the column-push inverted index and SWAR contiguity hints from
/// the CSR dep representation.
///
/// Returns `(col_inv_keys, col_inv_offsets, col_inv_rows, contig_start)`.
///
/// Used by both `compile_region` (in the engine) and test helpers.
pub fn build_region_inv(
    dep_offsets: &[u32],
    dep_cols:    &[u32],
    n_rows:      usize,
) -> (Vec<u32>, Vec<u32>, Vec<u32>, Vec<Option<u32>>) {
    // Build inverted index: dep_dense → [row indices].
    let mut inv: FxHashMap<u32, Vec<u32>> = Default::default();
    for row in 0..n_rows {
        let rs = dep_offsets[row] as usize;
        let re = dep_offsets[row + 1] as usize;
        for &col in &dep_cols[rs..re] {
            inv.entry(col).or_default().push(row as u32);
        }
    }

    let mut col_inv_keys: Vec<u32> = inv.keys().copied().collect();
    col_inv_keys.sort_unstable();

    let mut col_inv_offsets = Vec::with_capacity(col_inv_keys.len() + 1);
    let mut col_inv_rows    = Vec::new();
    col_inv_offsets.push(0u32);
    for &key in &col_inv_keys {
        col_inv_rows.extend_from_slice(&inv[&key]);
        col_inv_offsets.push(col_inv_rows.len() as u32);
    }

    // Build contiguity hints: row is contiguous iff dep_cols[rs..re] = [first, first+1, ..., first+k].
    let contig_start: Vec<Option<u32>> = (0..n_rows)
        .map(|row| {
            let rs = dep_offsets[row] as usize;
            let re = dep_offsets[row + 1] as usize;
            if rs >= re { return None; } // 0-dep row — no contiguous range
            let first = dep_cols[rs];
            if dep_cols[rs..re]
                .iter()
                .enumerate()
                .all(|(k, &c)| c == first + k as u32)
            {
                Some(first)
            } else {
                None
            }
        })
        .collect();

    (col_inv_keys, col_inv_offsets, col_inv_rows, contig_start)
}

// ── Execution ─────────────────────────────────────────────────────────────────

/// Execute a compiled region over the live `ValueStore`.
///
/// ## Algorithm (Opts 10 + 11 + 12)
///
/// 1. **Column-push pending set (Opt 11):** Iterate `dirty_slots` words using
///    Brian Kernighan bit-iteration; for each dirty dense slot, binary-search
///    `col_inv_keys` to find affected rows and set their bits in a local
///    `pending` bitmask.  Cost: O(dirty_inputs × avg_fan_out) — mirrors the
///    warm push path.
///
/// 2. **Topo-order evaluation:** Iterate rows 0..N; skip rows whose bit is
///    clear in `pending`.  For rows with contiguous deps and a `MeetAll` or
///    `JoinAny` rule, dispatch to `value_store.meet_all_range` /
///    `join_any_range` (SWAR, Opt 10).  Otherwise fall back to scatter-gather
///    + `rule.eval`.
///
/// 3. **Forward propagate dirty:** On output change, set the output slot's bit
///    in `dirty_slots` (for cross-region propagation) and add downstream rows
///    to `pending` via the inverted index.
///
/// Returns `(uid, old_val, new_val)` for each changed output.
pub fn run_compiled_region(
    region:      &CompiledRegion,
    value_store: &mut ValueStore,
    dirty_slots: &mut [u64],
) -> Vec<(Uid, T, T)> {
    let n_rows = region.members.len();
    if n_rows == 0 { return Vec::new(); }

    // ── Phase 1: build pending bitmask via column-push ────────────────────────
    let pending_words = (n_rows + 63) / 64;
    let mut pending = SmallVec::<[u64; 4]>::from_elem(0, pending_words);

    for (wi, &word) in dirty_slots.iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros() as usize;
            let col = (wi * 64 + bit) as u32;
            // Binary search the sorted col_inv_keys for this dirty column.
            if let Ok(k) = region.col_inv_keys.binary_search(&col) {
                let rs = region.col_inv_offsets[k] as usize;
                let re = region.col_inv_offsets[k + 1] as usize;
                for &row in &region.col_inv_rows[rs..re] {
                    let r = row as usize;
                    pending[r / 64] |= 1u64 << (r % 64);
                }
            }
            w &= w - 1; // clear lowest set bit (Brian Kernighan)
        }
    }

    // ── Phase 2: topo-order evaluation ───────────────────────────────────────
    let mut changed: Vec<(Uid, T, T)> = Vec::new();
    let mut dep_buf: SmallVec<[T; 8]> = SmallVec::new();

    for i in 0..n_rows {
        // Skip rows not in the pending set.
        if (pending[i / 64] >> (i % 64)) & 1 == 0 { continue; }

        let out_dense  = region.outputs[i];
        let row_start  = region.dep_offsets[i] as usize;
        let row_end    = region.dep_offsets[i + 1] as usize;
        let dep_count  = (row_end - row_start) as u32;

        // Evaluate using SWAR fast path or scalar fallback.
        let new_val_opt: Option<T> =
            if let Some(first) = region.contig_start[i] {
                match region.rules[i] {
                    ComputeRule::MeetAll =>
                        Some(value_store.meet_all_range(first, dep_count)),
                    ComputeRule::JoinAny =>
                        Some(value_store.join_any_range(first, dep_count)),
                    _ => {
                        // Contiguous deps but non-SWAR rule — scatter-gather fallback.
                        dep_buf.clear();
                        for &col in &region.dep_cols[row_start..row_end] {
                            dep_buf.push(value_store.get_by_dense(col));
                        }
                        region.rules[i].eval(&dep_buf)
                    }
                }
            } else {
                // Scattered deps — scatter-gather.
                dep_buf.clear();
                for &col in &region.dep_cols[row_start..row_end] {
                    dep_buf.push(value_store.get_by_dense(col));
                }
                region.rules[i].eval(&dep_buf)
            };

        let new_val = match new_val_opt { Some(v) => v, None => continue };
        let old_val = value_store.get_by_dense(out_dense);

        if old_val != new_val {
            value_store.set_by_dense(out_dense, new_val);
            changed.push((region.members[i], old_val, new_val));

            // ── Forward propagate dirty ───────────────────────────────────────
            // 1. Update dirty_slots so cross-region deps and boundary subs see this.
            let (dw, db) = (out_dense as usize / 64, out_dense as usize % 64);
            if dw < dirty_slots.len() {
                dirty_slots[dw] |= 1u64 << db;
            }
            // 2. Add downstream rows (intra-region) to pending via col_inv.
            if let Ok(k) = region.col_inv_keys.binary_search(&out_dense) {
                let rs = region.col_inv_offsets[k] as usize;
                let re = region.col_inv_offsets[k + 1] as usize;
                for &row in &region.col_inv_rows[rs..re] {
                    let r = row as usize;
                    if r > i { // only downstream rows (topo-sorted)
                        pending[r / 64] |= 1u64 << (r % 64);
                    }
                }
            }
        }
    }

    changed
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uid;

    // ── Test helpers ─────────────────────────────────────────────────────────

    /// Build a `CompiledRegion` from the minimal fields, populating the
    /// inverted index and contiguity hints automatically via `build_region_inv`.
    fn make_region(
        members:     Vec<Uid>,
        rules:       Vec<ComputeRule>,
        outputs:     Vec<u32>,
        inputs:      Vec<u32>,
        dep_offsets: Vec<u32>,
        dep_cols:    Vec<u32>,
    ) -> CompiledRegion {
        let n = members.len();
        let (col_inv_keys, col_inv_offsets, col_inv_rows, contig_start) =
            build_region_inv(&dep_offsets, &dep_cols, n);
        CompiledRegion {
            members:         members.into_boxed_slice(),
            rules:           rules.into_boxed_slice(),
            outputs:         outputs.into_boxed_slice(),
            inputs:          inputs.into_boxed_slice(),
            dep_offsets:     dep_offsets.into_boxed_slice(),
            dep_cols:        dep_cols.into_boxed_slice(),
            compiled_at:     0,
            col_inv_keys:    col_inv_keys.into_boxed_slice(),
            col_inv_offsets: col_inv_offsets.into_boxed_slice(),
            col_inv_rows:    col_inv_rows.into_boxed_slice(),
            contig_start:    contig_start.into_boxed_slice(),
        }
    }

    /// Single-input → single-output MeetAll region (common test fixture).
    fn make_meetall_region(input_dense: u32, output_dense: u32) -> CompiledRegion {
        make_region(
            vec![uid::fresh()],
            vec![ComputeRule::MeetAll],
            vec![output_dense],
            vec![input_dense],
            vec![0u32, 1u32],
            vec![input_dense],
        )
    }

    /// Run a compiled region with all slots marked dirty (full evaluation).
    ///
    /// Convenience wrapper for tests that don't care about lane selection.
    fn run_full(region: &CompiledRegion, vs: &mut ValueStore) -> Vec<(Uid, T, T)> {
        let words = (vs.len() + 63) / 64;
        let mut dirty = vec![u64::MAX; words];
        run_compiled_region(region, vs, &mut dirty)
    }

    // ── Basic correctness ─────────────────────────────────────────────────────

    #[test]
    fn run_single_node_pos_input() {
        let region = make_meetall_region(0, 1);
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Neg]);
        let changed = run_full(&region, &mut vs);
        assert_eq!(vs.get_by_dense(1), T::Pos);
        assert_eq!(changed.len(), 1);
        let (_, old, new) = changed[0];
        assert_eq!(old, T::Neg);
        assert_eq!(new, T::Pos);
    }

    #[test]
    fn run_no_change_returns_empty() {
        let region = make_meetall_region(0, 1);
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Pos]); // already converged
        let changed = run_full(&region, &mut vs);
        assert!(changed.is_empty());
    }

    #[test]
    fn run_bochvar_zero_propagates() {
        let region = make_meetall_region(0, 1);
        let mut vs = ValueStore::from_raw_cache(vec![T::Zero, T::Pos]);
        let changed = run_full(&region, &mut vs);
        // MeetAll([Zero]) = Zero; output was Pos → should change to Zero
        assert_eq!(vs.get_by_dense(1), T::Zero);
        assert_eq!(changed.len(), 1);
    }

    #[test]
    fn run_two_dep_meetall() {
        // Two inputs (dense 0, 1) → one output (dense 2)
        let region = make_region(
            vec![uid::fresh()],
            vec![ComputeRule::MeetAll],
            vec![2u32],
            vec![0u32, 1u32],
            vec![0u32, 2u32],
            vec![0u32, 1u32],
        );
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Pos, T::Neg]);
        let changed = run_full(&region, &mut vs);
        assert_eq!(vs.get_by_dense(2), T::Pos); // meet(Pos, Pos) = Pos
        assert_eq!(changed.len(), 1);

        // Now with one Neg input
        vs.set_by_dense(1, T::Neg);
        let changed2 = run_full(&region, &mut vs);
        assert_eq!(vs.get_by_dense(2), T::Neg); // meet(Pos, Neg) = Neg
        assert_eq!(changed2.len(), 1);
    }

    #[test]
    fn region_boundary_explicit_members() {
        let a = uid::fresh();
        let b = uid::fresh();
        let boundary = RegionBoundary::ExplicitSet(vec![a, b]);
        assert_eq!(boundary.explicit_members(), Some(&[a, b][..]));
    }

    // ── Active-lane (Opt 9) / column-push (Opt 11) tests ─────────────────────

    #[test]
    fn active_lane_skips_clean_row() {
        // input(dense=0) → output(dense=1); dirty_slots has no bit set → row skipped.
        let region = make_meetall_region(0, 1);
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Neg]);
        let mut dirty = vec![0u64];
        let changed = run_compiled_region(&region, &mut vs, &mut dirty);
        // Row was skipped: output stays Neg even though input is Pos.
        assert!(changed.is_empty());
        assert_eq!(vs.get_by_dense(1), T::Neg);
    }

    #[test]
    fn active_lane_evaluates_dirty_row() {
        // Same setup but with the input slot marked dirty.
        let region = make_meetall_region(0, 1);
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Neg]);
        let mut dirty = vec![0b1u64]; // dense slot 0 is dirty
        let changed = run_compiled_region(&region, &mut vs, &mut dirty);
        assert_eq!(changed.len(), 1);
        assert_eq!(vs.get_by_dense(1), T::Pos);
    }

    #[test]
    fn active_lane_chain_propagates_dirty() {
        // Chain: input(0) → A(1) → B(2).
        // Only input is initially dirty.  After A's row fires and A changes,
        // A's output slot (1) must be added to pending so B's row also fires.
        //
        // CSR layout:
        //   row 0 (A, dense=1): dep_cols=[0]
        //   row 1 (B, dense=2): dep_cols=[1]
        let a = uid::fresh();
        let b = uid::fresh();
        let region = make_region(
            vec![a, b],
            vec![ComputeRule::Identity, ComputeRule::Identity],
            vec![1u32, 2u32],
            vec![0u32],
            vec![0u32, 1u32, 2u32],
            vec![0u32, 1u32],
        );
        // cache: input=Pos(0), A=Neg(1), B=Neg(2)
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Neg, T::Neg]);
        let mut dirty = vec![0b001u64]; // only slot 0 dirty
        let changed = run_compiled_region(&region, &mut vs, &mut dirty);
        // A fires (dep 0 dirty → Identity(Pos) = Pos, A changes Neg→Pos, slot 1 added to pending).
        // B fires (dep 1 now pending → Identity(Pos) = Pos, B changes Neg→Pos).
        assert_eq!(changed.len(), 2);
        assert_eq!(vs.get_by_dense(1), T::Pos);
        assert_eq!(vs.get_by_dense(2), T::Pos);
    }

    #[test]
    fn active_lane_chain_stops_at_stable_node() {
        // Same chain, but A's input matches its current output (Pos→Pos).
        // A does not change → A's slot stays clean → B's row is NOT added to pending.
        let a = uid::fresh();
        let b = uid::fresh();
        let region = make_region(
            vec![a, b],
            vec![ComputeRule::Identity, ComputeRule::Identity],
            vec![1u32, 2u32],
            vec![0u32],
            vec![0u32, 1u32, 2u32],
            vec![0u32, 1u32],
        );
        // cache: input=Pos(0), A=Pos(1) (already converged), B=Neg(2)
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Pos, T::Neg]);
        let mut dirty = vec![0b001u64]; // slot 0 dirty
        let changed = run_compiled_region(&region, &mut vs, &mut dirty);
        // A fires (dep 0 dirty), evaluates Identity(Pos)=Pos, old=Pos → no change.
        // B is never added to pending.
        assert!(changed.is_empty());
        assert_eq!(vs.get_by_dense(2), T::Neg); // B unchanged
    }

    // ── SWAR dispatch tests ───────────────────────────────────────────────────

    #[test]
    fn swar_meetall_fan_in() {
        // 4 contiguous inputs (dense 0-3) → 1 output (dense 4), MeetAll rule.
        // Contiguity hint should be Some(0), triggering SWAR path.
        let region = make_region(
            vec![uid::fresh()],
            vec![ComputeRule::MeetAll],
            vec![4u32],
            vec![0u32, 1u32, 2u32, 3u32],
            vec![0u32, 4u32],
            vec![0u32, 1u32, 2u32, 3u32],
        );
        // All inputs Pos → MeetAll = Pos
        let mut vs = ValueStore::from_raw_cache(vec![T::Pos, T::Pos, T::Pos, T::Pos, T::Neg]);
        let changed = run_full(&region, &mut vs);
        assert_eq!(vs.get_by_dense(4), T::Pos);
        assert_eq!(changed.len(), 1);

        // One Neg input → MeetAll = Neg
        vs.set_by_dense(2, T::Neg);
        let changed2 = run_full(&region, &mut vs);
        assert_eq!(vs.get_by_dense(4), T::Neg);
        assert_eq!(changed2.len(), 1);
    }

    #[test]
    fn swar_joinany_fan_in() {
        // 3 contiguous inputs → 1 output, JoinAny rule.
        let region = make_region(
            vec![uid::fresh()],
            vec![ComputeRule::JoinAny],
            vec![3u32],
            vec![0u32, 1u32, 2u32],
            vec![0u32, 3u32],
            vec![0u32, 1u32, 2u32],
        );
        // All Neg → JoinAny = Neg
        let mut vs = ValueStore::from_raw_cache(vec![T::Neg, T::Neg, T::Neg, T::Neg]);
        let changed = run_full(&region, &mut vs);
        assert!(changed.is_empty()); // Neg → Neg, no change

        // One Pos → JoinAny = Pos
        vs.set_by_dense(1, T::Pos);
        let changed2 = run_full(&region, &mut vs);
        assert_eq!(vs.get_by_dense(3), T::Pos);
        assert_eq!(changed2.len(), 1);
    }

    // ── RegionArtifactCache tests ─────────────────────────────────────────────

    #[test]
    fn cache_invalidate_removes_epoch_tracked() {
        let mut cache = RegionArtifactCache::new();
        let root = uid::fresh();
        let member = uid::fresh();
        let region = make_region(
            vec![member],
            vec![ComputeRule::Identity],
            vec![0u32],
            vec![],
            vec![0u32, 0u32],
            vec![],
        );
        cache.insert(root, region, StabilityContract::EpochTracked);
        assert!(cache.regions.contains_key(&root));
        cache.invalidate_for_node(member);
        assert!(!cache.regions.contains_key(&root));
        // Contract entry survives invalidation
        assert!(cache.contracts.contains_key(&root));
    }

    #[test]
    fn cache_pinned_not_invalidated() {
        let mut cache = RegionArtifactCache::new();
        let root = uid::fresh();
        let member = uid::fresh();
        let region = make_region(
            vec![member],
            vec![ComputeRule::Identity],
            vec![0u32],
            vec![],
            vec![0u32, 0u32],
            vec![],
        );
        cache.insert(root, region, StabilityContract::Pinned);
        cache.invalidate_for_node(member); // Pinned → no removal
        assert!(cache.regions.contains_key(&root));
    }

    #[test]
    fn build_region_inv_basic() {
        // Two rows: row 0 depends on [col 5], row 1 depends on [col 5, col 7]
        let dep_offsets = vec![0u32, 1u32, 3u32];
        let dep_cols    = vec![5u32, 5u32, 7u32];
        let (keys, offsets, rows, contig) = build_region_inv(&dep_offsets, &dep_cols, 2);
        // keys should be sorted unique: [5, 7]
        assert_eq!(&*keys, &[5u32, 7u32]);
        // offsets[0]=0, offsets[1]=2 (rows 0 and 1 both use col 5), offsets[2]=3 (row 1 uses col 7)
        assert_eq!(&*offsets, &[0u32, 2u32, 3u32]);
        // rows for col 5: [0, 1]; rows for col 7: [1]
        assert_eq!(rows[0..2], [0u32, 1u32]);
        assert_eq!(rows[2], 1u32);
        // contig: row 0 has [5] → Some(5); row 1 has [5, 7] → not sequential → None
        assert_eq!(contig[0], Some(5u32));
        assert_eq!(contig[1], None);
    }
}
