//! Ternary truth domain T = {Neg, Zero, Pos} — the corrected L₃ MV-algebra.
//!
//! Encoding: {-1, 0, +1} as a centered subset of ℤ, via f: {0,½,1} → {-1,0,+1}.
//! All operations use the shifted arithmetic (see spec/algebra.md).
//!
//! Propagation semantics (see spec/algebra.md §Propagation):
//!   Pos  — clean value; propagate forward (Extension / push mode)
//!   Neg  — absent / pending; wait or demand (Inhibition / pull mode)
//!   Zero — conflicted; Bochvar-infectious; route to effect handler (Reflection)

/// `repr(u8)` with ordering Neg=0, Pos=1, Zero=2 enables:
///   • direct cast `v as u8` for SIMD byte-comparison lanes
///   • 2-bit packing (Zero's high bit = 0b10 is always distinct from Pos = 0b01 / Neg = 0b00)
///   • early-exit bitwise Zero detection without per-element branching
///
/// All match arms in this file use named variants — discriminant reordering is safe.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub enum T {
    #[default]
    Neg  = 0, // −1 = 0 in {0,½,1}: absent / inhibited / pending
    Pos  = 1, // +1 = 1 in {0,½,1}: confirmed / active / computed
    Zero = 2, //  0 = ½ in {0,½,1}: irresolvable; fixed point of ¬
}

impl T {
    // ── Core MV-algebra operations ─────────────────────────────────────────

    /// MV-negation ¬x = −x. Reflection primitive (I1). Fixed point: ¬Zero = Zero.
    #[inline]
    pub const fn mv_neg(self) -> T {
        match self {
            T::Neg  => T::Pos,
            T::Zero => T::Zero,
            T::Pos  => T::Neg,
        }
    }

    /// MV-addition: min(+1, x+y+1) in {−1,0,+1}. Additive identity: Neg.
    /// Note: Zero ⊕ Zero = Pos (½+½=1). Not the same as saturating integer add.
    #[inline]
    pub const fn mv_add(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Neg,  T::Neg)  => T::Neg,
            (T::Neg,  T::Zero) => T::Zero,
            (T::Neg,  T::Pos)  => T::Pos,
            (T::Zero, T::Neg)  => T::Zero,
            (T::Zero, T::Zero) => T::Pos,  // ½+½=1, clamped
            (T::Zero, T::Pos)  => T::Pos,
            (T::Pos,  T::Neg)  => T::Pos,
            (T::Pos,  T::Zero) => T::Pos,
            (T::Pos,  T::Pos)  => T::Pos,
        }
    }

    /// MV-multiplication: max(−1, x+y−1). Multiplicative identity: Pos.
    /// Note: Zero ⊗ Zero = Neg (½·½=0, truncated to bottom).
    #[inline]
    pub const fn mv_mul(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Pos,  T::Pos)  => T::Pos,
            (T::Pos,  T::Zero) => T::Zero,
            (T::Pos,  T::Neg)  => T::Neg,
            (T::Zero, T::Pos)  => T::Zero,
            (T::Zero, T::Zero) => T::Neg,  // ½·½=0, truncated
            (T::Zero, T::Neg)  => T::Neg,
            (T::Neg,  T::Pos)  => T::Neg,
            (T::Neg,  T::Zero) => T::Neg,
            (T::Neg,  T::Neg)  => T::Neg,
        }
    }

    /// MV bounded difference: x ⊖ y = ¬(¬x ⊕ y). Satisfies x ⊖ x = Neg.
    /// Used as the Čech coboundary: sections agree ↔ coboundary = Neg.
    #[inline]
    pub const fn mv_sub(self, rhs: T) -> T {
        self.mv_neg().mv_add(rhs).mv_neg()
    }

    /// Łukasiewicz implication: x → y = ¬x ⊕ y.
    #[inline]
    pub const fn mv_impl(self, rhs: T) -> T {
        self.mv_neg().mv_add(rhs)
    }

    // ── Lattice operations ────────────────────────────────────────────────

    /// Meet (min) over the chain Neg < Zero < Pos.
    #[inline]
    pub const fn meet(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Neg, _) | (_, T::Neg) => T::Neg,
            (T::Zero, _) | (_, T::Zero) => T::Zero,
            _ => T::Pos,
        }
    }

    /// Join (max) over the chain Neg < Zero < Pos.
    #[inline]
    pub const fn join(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Pos, _) | (_, T::Pos) => T::Pos,
            (T::Zero, _) | (_, T::Zero) => T::Zero,
            _ => T::Neg,
        }
    }

    // ── Median and merge ─────────────────────────────────────────────────

    /// Median: middle value of three under the chain order.
    /// merge(base, l, r) = median(base, l, r).
    #[inline]
    pub const fn median(a: T, b: T, c: T) -> T {
        a.meet(b).join(b.meet(c).join(a.meet(c)))
    }

    /// Merge: conflict resolution. merge(base, x, x) = x; merge(b, l, r) = merge(b, r, l).
    #[inline]
    pub const fn merge(base: T, left: T, right: T) -> T {
        T::median(base, left, right)
    }

    // ── Bochvar propagation ───────────────────────────────────────────────
    //
    // Zero is infectious: any computation touching a Zero dep immediately
    // returns Zero without firing. This is the Bochvar boundary between
    // the monotone forward pass (Pos-valued) and the effect handler layer.

    /// Bochvar-add: returns Zero if either operand is Zero; else MV-add.
    #[inline]
    pub const fn bochvar_add(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Zero, _) | (_, T::Zero) => T::Zero,
            _ => self.mv_add(rhs),
        }
    }

    /// Bochvar-mul: returns Zero if either operand is Zero; else MV-mul.
    #[inline]
    pub const fn bochvar_mul(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Zero, _) | (_, T::Zero) => T::Zero,
            _ => self.mv_mul(rhs),
        }
    }

    /// Bochvar-meet: returns Zero if either operand is Zero; else meet.
    #[inline]
    pub const fn bochvar_meet(self, rhs: T) -> T {
        match (self, rhs) {
            (T::Zero, _) | (_, T::Zero) => T::Zero,
            _ => self.meet(rhs),
        }
    }

    // ── Propagation state queries ─────────────────────────────────────────

    /// Pos: dep is ready, include in forward computation.
    #[inline]
    pub const fn is_ready(self) -> bool { matches!(self, T::Pos) }

    /// Zero: dep is conflicted. Bochvar-infects the output; route to handler.
    #[inline]
    pub const fn is_conflicted(self) -> bool { matches!(self, T::Zero) }

    /// Neg: dep is absent or pending. Computation must wait (or demand).
    #[inline]
    pub const fn is_pending(self) -> bool { matches!(self, T::Neg) }

    /// Fold a slice of dep values under Bochvar-add.
    /// Returns Neg (identity) for empty slice.
    /// Opt 3: 0/1/2/3-dep fast paths avoid iterator overhead for the common case.
    #[inline]
    pub fn bochvar_fold_add(vals: &[T]) -> T {
        match vals {
            []        => T::Neg,
            [a]       => *a,
            [a, b]    => a.bochvar_add(*b),
            [a, b, c] => a.bochvar_add(*b).bochvar_add(*c),
            _         => vals.iter().copied().fold(T::Neg, T::bochvar_add),
        }
    }

    /// Fold a slice of dep values under meet (conjunction of all deps).
    /// Opt 3: 0/1/2/3-dep fast paths.
    #[inline]
    pub fn meet_all(vals: &[T]) -> T {
        match vals {
            []        => T::Pos,
            [a]       => *a,
            [a, b]    => a.meet(*b),
            [a, b, c] => a.meet(*b).meet(*c),
            _         => vals.iter().copied().fold(T::Pos, T::meet),
        }
    }

    /// Fold a slice of dep values under join (disjunction of any dep).
    /// Opt 3: 0/1/2/3-dep fast paths.
    #[inline]
    pub fn join_any(vals: &[T]) -> T {
        match vals {
            []        => T::Neg,
            [a]       => *a,
            [a, b]    => a.join(*b),
            [a, b, c] => a.join(*b).join(*c),
            _         => vals.iter().copied().fold(T::Neg, T::join),
        }
    }

    /// Check propagation state of a dep slice:
    /// - Conflicted if any are Zero
    /// - Ready if all are Pos
    /// - Pending otherwise (some Neg, no Zero)
    ///
    /// Opt 2: single-pass counter with early exit on Zero eliminates the
    /// two-pass (any + all) pattern and its redundant iterations.
    /// Opt 3: 0/1/2-dep inlined fast paths with no loop overhead.
    #[inline]
    pub fn propagation_state(vals: &[T]) -> PropState {
        match vals {
            // 0-dep: vacuously all Pos → Ready (unit for meet)
            [] => PropState::Ready,
            // 1-dep: direct dispatch — no branch misprediction
            [a] => match a {
                T::Zero => PropState::Conflicted,
                T::Pos  => PropState::Ready,
                T::Neg  => PropState::Pending,
            },
            // 2-dep: four comparisons, no loop
            [a, b] => {
                if matches!(a, T::Zero) || matches!(b, T::Zero) {
                    PropState::Conflicted
                } else if matches!(a, T::Pos) && matches!(b, T::Pos) {
                    PropState::Ready
                } else {
                    PropState::Pending
                }
            }
            // ≥3 deps: single-pass with early exit on Zero
            _ => {
                let mut n_pos: u32 = 0;
                for &v in vals {
                    match v {
                        T::Zero => return PropState::Conflicted, // Bochvar: exit immediately
                        T::Pos  => n_pos += 1,
                        T::Neg  => {}
                    }
                }
                if n_pos as usize == vals.len() { PropState::Ready } else { PropState::Pending }
            }
        }
    }
}

/// Propagation state of a node's dependency set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PropState {
    Ready,      // all deps Pos → compute and push
    Pending,    // some deps Neg → wait or demand
    Conflicted, // some deps Zero → Bochvar-infected; route to effect handler
}

impl std::fmt::Display for T {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            T::Neg  => write!(f, "-1"),
            T::Zero => write!(f, " 0"),
            T::Pos  => write!(f, "+1"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::T::{self, *};

    fn all() -> [T; 3] { [Neg, Zero, Pos] }

    #[test] fn neg_involutive() {
        for x in all() { assert_eq!(x.mv_neg().mv_neg(), x, "{x}"); }
    }

    #[test] fn zero_fixpoint() { assert_eq!(Zero.mv_neg(), Zero); }

    #[test] fn add_identity() {
        for x in all() {
            assert_eq!(Neg.mv_add(x), x, "Neg⊕{x}");
            assert_eq!(x.mv_add(Neg), x, "{x}⊕Neg");
        }
    }

    #[test] fn mul_identity() {
        for x in all() {
            assert_eq!(Pos.mv_mul(x), x, "Pos⊗{x}");
            assert_eq!(x.mv_mul(Pos), x, "{x}⊗Pos");
        }
    }

    #[test] fn zero_add_zero_top() { assert_eq!(Zero.mv_add(Zero), Pos); }
    #[test] fn zero_mul_zero_bot() { assert_eq!(Zero.mv_mul(Zero), Neg); }

    #[test] fn annihilator() {
        for x in all() { assert_eq!(x.mv_add(x.mv_neg()), Pos, "{x}⊕¬{x}"); }
    }

    #[test] fn add_comm() {
        for a in all() { for b in all() {
            assert_eq!(a.mv_add(b), b.mv_add(a), "{a}⊕{b}");
        }}
    }

    #[test] fn add_assoc() {
        for a in all() { for b in all() { for c in all() {
            assert_eq!(a.mv_add(b).mv_add(c), a.mv_add(b.mv_add(c)));
        }}}
    }

    #[test] fn mul_comm() {
        for a in all() { for b in all() {
            assert_eq!(a.mv_mul(b), b.mv_mul(a));
        }}
    }

    #[test] fn sub_self_is_zero() {
        for x in all() { assert_eq!(x.mv_sub(x), Neg, "{x}⊖{x}"); }
    }

    #[test] fn bochvar_infection() {
        for x in all() {
            assert_eq!(Zero.bochvar_add(x), Zero);
            assert_eq!(x.bochvar_add(Zero), Zero);
        }
    }

    #[test] fn merge_same() {
        for b in all() { for x in all() {
            assert_eq!(T::merge(b, x, x), x, "merge({b},{x},{x})");
        }}
    }

    #[test] fn merge_comm() {
        for b in all() { for l in all() { for r in all() {
            assert_eq!(T::merge(b, l, r), T::merge(b, r, l));
        }}}
    }
}
