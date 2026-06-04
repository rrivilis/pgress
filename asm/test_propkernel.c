/*
 * test_propkernel.c — witness tests for the SSE2 ternary propagation kernel.
 *
 * Each test initialises a region as packed bitplanes, invokes the asm kernels,
 * and asserts the result.  The chain-propagation witness at the end shows a
 * region transitioning from non-quiescent to quiescent through successive
 * MeetAll-driven Pos propagation steps — no Rust, no dep graph, no ownership.
 *
 * Build:  see Makefile  (nasm + gcc, Linux / WSL)
 * Run:    ./test_propkernel
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

/* ── T repr values (must match core-rs ternary.rs) ──────────────────────── */
#define T_NEG  0   /* Neg  = 0b00: absent / pending */
#define T_POS  1   /* Pos  = 0b01: confirmed / active */
#define T_ZERO 2   /* Zero = 0b10: conflict fixed point */

/* ── Region size ────────────────────────────────────────────────────────── */
/* 512 cells = 8 qwords exactly, no padding bits needed.                    */
#define N_CELLS  512
#define N_QWORDS (N_CELLS / 64)   /* = 8 */

/* ── Kernel declarations (SysV AMD64 ABI) ────────────────────────────────── */
extern uint8_t meetall_sse2    (const uint64_t *p0, const uint64_t *p1, size_t n_qwords);
extern uint8_t joinany_sse2    (const uint64_t *p0, const uint64_t *p1, size_t n_qwords);
extern int     region_quiescent(const uint64_t *p0, const uint64_t *p1, size_t n_qwords);

/* ── Helpers ─────────────────────────────────────────────────────────────── */

static const char *t_name(uint8_t v) {
    switch (v) {
        case T_NEG:  return "Neg";
        case T_POS:  return "Pos";
        case T_ZERO: return "Zero";
        default:     return "???";
    }
}

/* Fill entire region with a single ternary value. */
static void region_fill(uint64_t *p0, uint64_t *p1, uint8_t val) {
    uint64_t fill_p0 = (val == T_POS)  ? UINT64_MAX : 0ULL;
    uint64_t fill_p1 = (val == T_ZERO) ? UINT64_MAX : 0ULL;
    for (size_t w = 0; w < N_QWORDS; w++) {
        p0[w] = fill_p0;
        p1[w] = fill_p1;
    }
}

/* Set a single cell (by dense index) to val. */
static void cell_set(uint64_t *p0, uint64_t *p1, size_t idx, uint8_t val) {
    size_t  word = idx / 64;
    int     bit  = (int)(idx % 64);
    uint64_t mask = 1ULL << bit;
    /* clear both planes for this slot, then set the right one */
    p0[word] &= ~mask;
    p1[word] &= ~mask;
    if (val == T_POS)  p0[word] |= mask;
    if (val == T_ZERO) p1[word] |= mask;
}

/* Read a single cell value. */
static uint8_t cell_get(const uint64_t *p0, const uint64_t *p1, size_t idx) {
    size_t   word = idx / 64;
    int      bit  = (int)(idx % 64);
    uint64_t mask = 1ULL << bit;
    int is_p0 = (p0[word] & mask) != 0;
    int is_p1 = (p1[word] & mask) != 0;
    if (is_p0) return T_POS;
    if (is_p1) return T_ZERO;
    return T_NEG;
}

/* Count cells with a given value. */
static size_t cell_count(const uint64_t *p0, const uint64_t *p1, uint8_t val) {
    size_t n = 0;
    for (size_t i = 0; i < N_CELLS; i++)
        if (cell_get(p0, p1, i) == val) n++;
    return n;
}

/*
 * Propagation step: each cell inherits Pos from its left neighbour.
 *
 * Bitplane interpretation: shift p0 left by 1 bit (toward higher cell indices)
 * and OR into p0.  Carry bit between qwords propagates across the 64-bit boundary.
 *
 *   new_p0[i] = p0[i] | p0[i-1]    (with i=0 having no left neighbour)
 *
 * p1 (Zero plane) is untouched — this scenario has no Zero cells.
 *
 * After k steps starting from cell[0]=Pos: cells 0..k are all Pos.
 * After N_CELLS-1 = 511 steps: all 512 cells are Pos → region quiescent.
 */
static void propagate_step(uint64_t *p0, size_t n_qwords) {
    uint64_t carry = 0;
    for (size_t w = 0; w < n_qwords; w++) {
        uint64_t new_carry = p0[w] >> 63;       /* bit 63 → carry to word w+1 */
        p0[w] = p0[w] | (p0[w] << 1) | carry;  /* extend each run rightward  */
        carry = new_carry;
    }
}

/* ── Test harness ─────────────────────────────────────────────────────────── */

static int pass_count = 0;
static int fail_count = 0;

#define CHECK(cond, label) do {                                         \
    if (cond) { printf("  PASS  %s\n", label); pass_count++; }         \
    else       { printf("  FAIL  %s\n", label); fail_count++; }        \
} while (0)

int main(void) {
    uint64_t p0[N_QWORDS];
    uint64_t p1[N_QWORDS];

    printf("=== ternary propkernel SSE2 witness tests  (N=%d cells, %d qwords) ===\n\n",
           N_CELLS, N_QWORDS);

    /* ── meetall tests ─────────────────────────────────────────────────── */
    printf("--- meetall_sse2 ---\n");

    region_fill(p0, p1, T_POS);
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_POS,
          "all-Pos  → Pos");
    CHECK(region_quiescent(p0, p1, N_QWORDS) == 1,
          "all-Pos  → quiescent");

    region_fill(p0, p1, T_NEG);
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_NEG,
          "all-Neg  → Neg");
    CHECK(region_quiescent(p0, p1, N_QWORDS) == 0,
          "all-Neg  → not quiescent");

    region_fill(p0, p1, T_ZERO);
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_ZERO,
          "all-Zero → Zero  (Zero is a fixed point, not Neg)");
    CHECK(region_quiescent(p0, p1, N_QWORDS) == 1,
          "all-Zero → quiescent  (no Neg cells)");

    /* One Neg cell poisons the meet regardless of position */
    region_fill(p0, p1, T_POS);
    cell_set(p0, p1, 0,          T_NEG);    /* first cell */
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_NEG,
          "511 Pos + 1 Neg at [0]   → Neg");

    region_fill(p0, p1, T_POS);
    cell_set(p0, p1, N_CELLS-1,  T_NEG);    /* last cell */
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_NEG,
          "511 Pos + 1 Neg at [511] → Neg");

    region_fill(p0, p1, T_POS);
    cell_set(p0, p1, N_CELLS/2,  T_NEG);    /* middle cell */
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_NEG,
          "511 Pos + 1 Neg at [256] → Neg");

    /* Zero poisons in the absence of Neg */
    region_fill(p0, p1, T_POS);
    cell_set(p0, p1, 0,         T_ZERO);
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_ZERO,
          "511 Pos + 1 Zero at [0]   → Zero");
    CHECK(region_quiescent(p0, p1, N_QWORDS) == 1,
          "511 Pos + 1 Zero at [0]   → quiescent (no Neg)");

    region_fill(p0, p1, T_POS);
    cell_set(p0, p1, N_CELLS-1, T_ZERO);
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_ZERO,
          "511 Pos + 1 Zero at [511] → Zero");

    /* Neg beats Zero: Neg wins the meet even if Zero is also present */
    region_fill(p0, p1, T_POS);
    cell_set(p0, p1, 0,   T_ZERO);
    cell_set(p0, p1, 255, T_NEG);
    CHECK(meetall_sse2(p0, p1, N_QWORDS) == T_NEG,
          "510 Pos + 1 Zero + 1 Neg → Neg  (Neg < Zero)");

    /* ── joinany tests ─────────────────────────────────────────────────── */
    printf("\n--- joinany_sse2 ---\n");

    region_fill(p0, p1, T_POS);
    CHECK(joinany_sse2(p0, p1, N_QWORDS) == T_POS,
          "all-Pos  → Pos");

    region_fill(p0, p1, T_NEG);
    CHECK(joinany_sse2(p0, p1, N_QWORDS) == T_NEG,
          "all-Neg  → Neg");

    region_fill(p0, p1, T_ZERO);
    CHECK(joinany_sse2(p0, p1, N_QWORDS) == T_ZERO,
          "all-Zero → Zero");

    region_fill(p0, p1, T_NEG);
    cell_set(p0, p1, N_CELLS/2, T_POS);
    CHECK(joinany_sse2(p0, p1, N_QWORDS) == T_POS,
          "511 Neg + 1 Pos at [256] → Pos");

    region_fill(p0, p1, T_NEG);
    cell_set(p0, p1, N_CELLS-1, T_ZERO);
    CHECK(joinany_sse2(p0, p1, N_QWORDS) == T_ZERO,
          "511 Neg + 1 Zero at [511] → Zero  (no Pos)");

    /* Zero loses to Pos in the join */
    region_fill(p0, p1, T_NEG);
    cell_set(p0, p1, 0,   T_ZERO);
    cell_set(p0, p1, 511, T_POS);
    CHECK(joinany_sse2(p0, p1, N_QWORDS) == T_POS,
          "510 Neg + 1 Zero + 1 Pos → Pos  (Pos > Zero)");

    /* Empty call (n_qwords = 0): identity elements */
    printf("\n--- identity elements (n_qwords=0) ---\n");
    CHECK(meetall_sse2(p0, p1, 0) == T_POS,
          "meetall(∅) = Pos  (identity for meet)");
    CHECK(joinany_sse2(p0, p1, 0) == T_NEG,
          "joinany(∅) = Neg  (identity for join)");
    CHECK(region_quiescent(p0, p1, 0) == 1,
          "region_quiescent(∅) = 1  (vacuously quiescent)");

    /* ── Chain propagation witness ─────────────────────────────────────── *
     *
     * Scenario: a single Pos seed at cell[0] propagates rightward through
     * 511 steps until all 512 cells have stabilised to Pos.
     *
     * This is the SSE2 witness of the quiescence theorem:
     *   "a ternary region stabilises in at most D propagation steps, where D
     *    is the critical path depth of the dependency graph."
     *
     * For a linear chain of depth 511, D = 511.  After exactly 511 steps the
     * region is quiescent.  No Rust runtime, no dep counters, no e-graph —
     * just packed bitplanes and eldritch arithmetic.
     */
    printf("\n--- chain propagation witness ---\n");
    printf("    seed: cell[0] = Pos, cells[1..511] = Neg\n");

    region_fill(p0, p1, T_NEG);
    cell_set(p0, p1, 0, T_POS);

    /* Confirm non-quiescent at step 0 */
    int initial_q = region_quiescent(p0, p1, N_QWORDS);
    printf("    step %4d: quiescent=%d  neg_cells=%zu\n",
           0, initial_q, cell_count(p0, p1, T_NEG));
    CHECK(initial_q == 0, "step 0: not yet quiescent");

    /* Run propagation; sample every 128 steps */
    for (int step = 1; step <= N_CELLS - 1; step++) {
        propagate_step(p0, N_QWORDS);
        if (step % 128 == 0 || step == N_CELLS - 1) {
            size_t n_neg = cell_count(p0, p1, T_NEG);
            size_t n_pos = cell_count(p0, p1, T_POS);
            printf("    step %4d: quiescent=%d  pos_cells=%zu  neg_cells=%zu\n",
                   step, region_quiescent(p0, p1, N_QWORDS), n_pos, n_neg);
        }
    }

    int final_q = region_quiescent(p0, p1, N_QWORDS);
    uint8_t final_meet = meetall_sse2(p0, p1, N_QWORDS);

    CHECK(final_q    == 1,     "step 511: region quiescent");
    CHECK(final_meet == T_POS, "step 511: meetall = Pos");
    CHECK(cell_count(p0, p1, T_NEG) == 0, "step 511: zero Neg cells remain");

    /* ── Results ────────────────────────────────────────────────────────── */
    printf("\n=== %d passed, %d failed ===\n", pass_count, fail_count);
    printf("final meet: %s\n", t_name(final_meet));
    return fail_count == 0 ? 0 : 1;
}
