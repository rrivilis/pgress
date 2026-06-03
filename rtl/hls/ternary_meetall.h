// rtl/hls/ternary_meetall.h
#pragma once
#include "ap_int.h"

// ── Ternary encoding (matches pgress T repr) ────────────────────────────────
// T::Neg  = 0b00  (Neg  = 0u8 in Rust repr)
// T::Pos  = 0b01  (Pos  = 1u8)
// T::Zero = 0b10  (Zero = 2u8)
// 0b11 is don't-care (unused; synthesis tool may exploit for LUT reduction)

typedef ap_uint<2>  T_val;   // one ternary value
typedef ap_uint<64> u64;     // one bitplane word

static const T_val T_NEG  = 0b00;
static const T_val T_POS  = 0b01;
static const T_val T_ZERO = 0b10;

// Fixed topology: 500 inputs, 8 bitplane words.
//   ceil(500 / 64) = 8 words
//   Word 7 covers dense slots 448..511; valid slots are 448..499 → 52 bits.
//   LAST_MASK = (1 << 52) - 1 = 0x000FFFFFFFFFFFFF
#define N_INPUTS  500
#define N_WORDS   8
#define LAST_MASK ((1ULL << 52) - 1ULL)

// Top-level kernel (defined in ternary_meetall.cpp)
T_val meetall_500(u64 plane_p0[N_WORDS], u64 plane_p1[N_WORDS]);
