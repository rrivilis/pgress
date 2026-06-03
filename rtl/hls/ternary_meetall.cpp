// rtl/hls/ternary_meetall.cpp
//
// Vitis HLS kernel: meetall_500
//
// Direct port of pgress ValueStore::meet_all_range(0, 500) from
// core-rs/src/value_store.rs (Opt 10 bitplane SWAR path).
//
// Input:  two arrays of 8 × u64 words representing the bit-sliced ternary
//         planes over 500 input values.
//   plane_p0[w]: bit j is set iff input[w*64+j] == T_POS  (repr bit 0)
//   plane_p1[w]: bit j is set iff input[w*64+j] == T_ZERO (repr bit 1)
//
// Output: 2-bit T_val:
//   T_NEG  (0b00) if any input is Neg  (p0=0 AND p1=0)
//   T_ZERO (0b10) if no Neg but any input is Zero (p0=0 AND p1=1)
//   T_POS  (0b01) if all inputs are Pos
//
// Don't-care state (p0=1, p1=1): never triggers Neg or Zero detection,
// so it is treated as Pos-like. Synthesis tool may exploit it freely.
//
// Synthesis target: Xilinx UltraScale+, 250 MHz (4 ns clock).
// With ARRAY_PARTITION complete + PIPELINE II=1 + UNROLL, all 8 word
// comparisons are evaluated in a single clock cycle.

#include "ternary_meetall.h"

T_val meetall_500(u64 plane_p0[N_WORDS], u64 plane_p1[N_WORDS]) {
#pragma HLS PIPELINE II=1
#pragma HLS ARRAY_PARTITION variable=plane_p0 complete dim=1
#pragma HLS ARRAY_PARTITION variable=plane_p1 complete dim=1

    bool has_zero = false;

    // Words 0..6: full 64-bit words, all bits valid, no masking needed.
    for (int w = 0; w < N_WORDS - 1; w++) {
#pragma HLS UNROLL
        u64 p0m = plane_p0[w];
        u64 p1m = plane_p1[w];

        // Neg: p0=0 AND p1=0 → (~p0m & ~p1m) has a set bit
        if ((~p0m & ~p1m) != 0) return T_NEG;

        // Zero: p0=0 AND p1=1 → (~p0m & p1m) has a set bit
        if ((~p0m & p1m) != 0) has_zero = true;
    }

    // Word 7: only bits [0..51] are valid (slots 448..499, 52 bits).
    // Mask out the upper 12 bits so tombstone/unallocated slots (which
    // default to T_Neg = 0b00) don't incorrectly trigger Neg detection.
    {
        const u64 mask = (u64)LAST_MASK;
        u64 p0m = plane_p0[N_WORDS - 1] & mask;
        u64 p1m = plane_p1[N_WORDS - 1] & mask;

        // neg_bits: positions that are 0 in both planes AND within mask
        u64 neg_bits = (~p0m) & (~p1m) & mask;
        if (neg_bits != 0) return T_NEG;
        if ((~p0m & p1m) != 0) has_zero = true;
    }

    return has_zero ? T_ZERO : T_POS;
}
