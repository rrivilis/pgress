// rtl/hls/tb_meetall.cpp
//
// HLS C-simulation testbench for meetall_500.
// Validates semantic equivalence with pgress ValueStore::meet_all_range(0, 500).
//
// Scenarios mirror the Criterion benchmarks in spec/benchmarks.md:
//   A — Convergence:  all 500 inputs Neg → Pos; output fires exactly once (on last)
//   B — Hot toggle:   input[0] cycles Zero ↔ Pos, rest stable Pos; 200 cycles
//   C — No-op:        all inputs already Pos; re-drive same values; output unchanged
//   D — Zero poison:  one input Zero in a sea of Pos; output must be Zero

#include <cstdio>
#include <cstring>
#include <cassert>
#include "ternary_meetall.h"

// ── Helpers ──────────────────────────────────────────────────────────────────

// Build plane_p0 / plane_p1 from a flat array of N_INPUTS T_val values.
static void build_planes(const T_val vals[N_INPUTS],
                         u64 p0[N_WORDS], u64 p1[N_WORDS]) {
    memset(p0, 0, N_WORDS * sizeof(u64));
    memset(p1, 0, N_WORDS * sizeof(u64));
    for (int i = 0; i < N_INPUTS; i++) {
        int w = i / 64, b = i % 64;
        u64 mask = (u64)1 << b;
        // repr bit 0 → Pos plane; repr bit 1 → Zero plane
        if ((vals[i] & 1) != 0) p0[w] |= mask;
        if ((vals[i] & 2) != 0) p1[w] |= mask;
    }
}

// Set all N_INPUTS values to the same T_val.
static void set_all(T_val vals[N_INPUTS], T_val val) {
    for (int i = 0; i < N_INPUTS; i++) vals[i] = val;
}

// ── Main ─────────────────────────────────────────────────────────────────────

int main() {
    T_val inputs[N_INPUTS];
    u64 p0[N_WORDS], p1[N_WORDS];
    int failures = 0;
    T_val out;

    // ── Scenario A: Convergence (all Neg → all Pos) ──────────────────────────
    // Output must remain Neg until the final (500th) input arrives as Pos.

    set_all(inputs, T_NEG);
    build_planes(inputs, p0, p1);
    out = meetall_500(p0, p1);
    if (out != T_NEG) {
        printf("FAIL A-init: initial state out=0x%x (expected T_NEG)\n", (int)out);
        failures++;
    }

    // Drive inputs Pos one at a time; output must stay Neg through input 498.
    for (int i = 0; i < N_INPUTS - 1; i++) {
        inputs[i] = T_POS;
        build_planes(inputs, p0, p1);
        out = meetall_500(p0, p1);
        if (out != T_NEG) {
            printf("FAIL A: output changed at i=%d (got 0x%x, expected T_NEG)\n",
                   i, (int)out);
            failures++;
            break; // don't spam 500 failures
        }
    }

    // Final input → all Pos; output must flip to Pos exactly here.
    inputs[N_INPUTS - 1] = T_POS;
    build_planes(inputs, p0, p1);
    out = meetall_500(p0, p1);
    if (out != T_POS) {
        printf("FAIL A-final: out=0x%x after all inputs Pos (expected T_POS)\n", (int)out);
        failures++;
    } else {
        printf("Scenario A (convergence): PASS\n");
    }

    // ── Scenario B: Hot toggle (input[0] Zero ↔ Pos, rest Pos) ──────────────
    // 200 cycles. Each cycle: set input[0]=Zero → check Zero; set input[0]=Pos → check Pos.

    set_all(inputs, T_POS);
    build_planes(inputs, p0, p1);
    assert(meetall_500(p0, p1) == T_POS);

    int toggle_failures = 0;
    for (int cycle = 0; cycle < 200; cycle++) {
        inputs[0] = T_ZERO;
        build_planes(inputs, p0, p1);
        out = meetall_500(p0, p1);
        if (out != T_ZERO) toggle_failures++;

        inputs[0] = T_POS;
        build_planes(inputs, p0, p1);
        out = meetall_500(p0, p1);
        if (out != T_POS) toggle_failures++;
    }
    if (toggle_failures > 0) {
        printf("FAIL B: %d mismatches in 200 hot-toggle cycles\n", toggle_failures);
        failures += toggle_failures;
    } else {
        printf("Scenario B (hot toggle, 200 cycles): PASS\n");
    }

    // ── Scenario C: No-op (all Pos, re-drive same values 100 rounds) ─────────
    // Output must remain T_POS on every round — no state change, no fire.

    set_all(inputs, T_POS);
    int noop_failures = 0;
    for (int round = 0; round < 100; round++) {
        build_planes(inputs, p0, p1);   // same planes every time
        out = meetall_500(p0, p1);
        if (out != T_POS) noop_failures++;
    }
    if (noop_failures > 0) {
        printf("FAIL C: output changed on no-op in %d/100 rounds\n", noop_failures);
        failures += noop_failures;
    } else {
        printf("Scenario C (no-op, 100 rounds): PASS\n");
    }

    // ── Scenario D: Zero poison (one Zero, rest Pos) ─────────────────────────
    // input[250] = Zero; all others Pos; output must be T_ZERO.

    set_all(inputs, T_POS);
    inputs[250] = T_ZERO;
    build_planes(inputs, p0, p1);
    out = meetall_500(p0, p1);
    if (out != T_ZERO) {
        printf("FAIL D: Zero poison — out=0x%x (expected T_ZERO)\n", (int)out);
        failures++;
    } else {
        printf("Scenario D (Zero poison, input[250]): PASS\n");
    }

    // ── Scenario E: Neg in last word (slot 498, word 7) ──────────────────────
    // Regression: verify the LAST_MASK in word 7 doesn't hide a valid Neg slot.

    set_all(inputs, T_POS);
    inputs[498] = T_NEG;
    build_planes(inputs, p0, p1);
    out = meetall_500(p0, p1);
    if (out != T_NEG) {
        printf("FAIL E: Neg at slot 498 (word 7, bit 50) not detected — out=0x%x\n",
               (int)out);
        failures++;
    } else {
        printf("Scenario E (Neg in last word, slot 498): PASS\n");
    }

    // ── Summary ──────────────────────────────────────────────────────────────
    printf("\n%s (%d total failures)\n",
           failures == 0 ? "ALL PASS" : "SOME FAILURES", failures);
    return failures;
}
