/* ternary_propkernel.S — SSE2 bitplane kernels for the pgress ternary substrate.
 *
 * Assembles with GAS (GNU Assembler), which ships with gcc/binutils.
 * No NASM required:  gcc -c ternary_propkernel.S -o ternary_propkernel.o
 *
 * Bitplane encoding mirrors ValueStore in core-rs/src/value_store.rs exactly:
 *
 *   plane_p0[w] bit j = 1  iff  cell[w*64+j] == T::Pos   (repr 0b01)
 *   plane_p1[w] bit j = 1  iff  cell[w*64+j] == T::Zero  (repr 0b10)
 *   T::Neg: p0=0, p1=0     (repr 0b00, the implicit third state)
 *
 * Ternary chain ordering:  Neg < Zero < Pos
 *
 *   meet(a,b) = min:  any Neg  → Neg;  no Neg, any Zero → Zero;  all Pos → Pos
 *   join(a,b) = max:  any Pos  → Pos;  no Pos, any Zero → Zero;  all Neg → Neg
 *
 * Per 128-cell SSE2 chunk (one XMM load from each plane):
 *
 *   neg_bits  = ~(p0 | p1)      p0=0 AND p1=0 → Neg
 *   zero_bits = ~p0  &  p1      p0=0 AND p1=1 → Zero     [AT&T: pandn %p1,%p0_copy]
 *   pos_bits  = ~p1  &  p0      p1=0 AND p0=1 → Pos
 *
 * AT&T operand order throughout: src, dst  (opposite of Intel syntax)
 * PANDN src, dst  ≡  dst = ~dst & src     (destructive on dst — copies before use)
 *
 * Calling convention: System V AMD64 (Linux / WSL)
 *   args:    %rdi, %rsi, %rdx, %rcx
 *   return:  %rax
 *   XMM0–XMM15: caller-saved (no preservation needed within these functions)
 *
 * T repr (core-rs ternary.rs):  T::Neg=0  T::Pos=1  T::Zero=2
 *
 * Exported:
 *   uint8_t meetall_sse2    (const uint64_t *p0, const uint64_t *p1, size_t n_qwords)
 *   uint8_t joinany_sse2    (const uint64_t *p0, const uint64_t *p1, size_t n_qwords)
 *   int     region_quiescent(const uint64_t *p0, const uint64_t *p1, size_t n_qwords)
 *
 * n_qwords: number of 64-bit words per plane.  N cells → ceil(N/64) qwords.
 * Cells beyond N within the last qword MUST be zeroed by the caller.
 */

    .intel_syntax noprefix          /* Use Intel syntax in GAS — cleaner, fewer % sigils */

    /* Mark stack as non-executable (suppresses linker warning on Linux ELF). */
    .section .note.GNU-stack,"",@progbits

    .text

/* ── Reduction helper ──────────────────────────────────────────────────────
 *
 * test_nonzero_xmm xmm_reg, xmm_zero
 *   Sets ZF=1 if xmm_reg is all-zeros, ZF=0 if any byte is nonzero.
 *   Clobbers xmm_reg and eax.
 *
 * Strategy: PCMPEQB against 0 produces 0xFF per zero-byte, 0x00 per nonzero.
 * PMOVMSKB extracts the MSBs.  If eax == 0xFFFF, all original bytes were 0.
 * We then CMP with 0xFFFF; JE means "was all-zero" (no bits set).
 */

/* ═══════════════════════════════════════════════════════════════════════════
 * meetall_sse2 — lattice meet (min) over all N cells
 *
 * %rdi = p0 plane   %rsi = p1 plane   %rdx = n_qwords
 * Returns: T::Neg(0) | T::Pos(1) | T::Zero(2)
 *
 * Accumulators:
 *   xmm4 = OR of all neg_bits  chunks  (nonzero ↔ ≥1 Neg cell)
 *   xmm5 = OR of all zero_bits chunks  (nonzero ↔ ≥1 Zero cell)
 * ═══════════════════════════════════════════════════════════════════════════ */
    .global meetall_sse2
meetall_sse2:
    pxor    xmm4, xmm4              /* any_neg  accumulator = 0               */
    pxor    xmm5, xmm5              /* any_zero accumulator = 0               */
    pcmpeqb xmm6, xmm6              /* xmm6 = 0xFF…FF  (for bitwise NOT)      */
    xor     eax, eax                /* byte offset = 0                        */

    mov     rcx, rdx
    shr     rcx, 1                  /* rcx = n_qwords / 2  (XMM iterations)   */
    jz      .ma_tail

.ma_loop:
    movdqu  xmm0, [rdi + rax]       /* p0 chunk (128 bits = 128 cells)        */
    movdqu  xmm1, [rsi + rax]       /* p1 chunk                               */

    /* neg_bits = ~(p0 | p1) */
    movdqa  xmm2, xmm0
    por     xmm2, xmm1              /* xmm2 = p0 | p1                        */
    pxor    xmm2, xmm6              /* xmm2 = ~(p0 | p1) = neg_bits          */
    por     xmm4, xmm2              /* any_neg |= neg_bits                   */

    /* zero_bits = ~p0 & p1  via  PANDN(p0_copy, p1)  ≡  ~p0_copy & p1 */
    movdqa  xmm2, xmm0
    pandn   xmm2, xmm1              /* xmm2 = ~p0 & p1 = zero_bits           */
    por     xmm5, xmm2              /* any_zero |= zero_bits                 */

    add     eax, 16
    dec     rcx
    jnz     .ma_loop

.ma_tail:
    test    rdx, 1                  /* odd trailing qword?                    */
    jz      .ma_reduce

    movq    xmm0, [rdi + rax]       /* MOVQ: zero-extends upper 64 bits       */
    movq    xmm1, [rsi + rax]

    movdqa  xmm2, xmm0
    por     xmm2, xmm1
    pxor    xmm2, xmm6
    por     xmm4, xmm2

    movdqa  xmm2, xmm0
    pandn   xmm2, xmm1
    por     xmm5, xmm2

.ma_reduce:
    /* Test any_neg: PCMPEQB xmm4, 0 → 0xFF where byte was 0.
     * PMOVMSKB → eax; 0xFFFF means all bytes were 0 (no Neg bits). */
    pxor    xmm7, xmm7
    pcmpeqb xmm4, xmm7
    pmovmskb eax, xmm4
    cmp     eax, 0xFFFF
    jne     .ma_ret_neg             /* eax ≠ 0xFFFF → some byte nonzero → Neg */

    pcmpeqb xmm5, xmm7             /* xmm7 still = 0                        */
    pmovmskb eax, xmm5
    cmp     eax, 0xFFFF
    jne     .ma_ret_zero

    mov     eax, 1                  /* T::Pos = 1                             */
    ret
.ma_ret_neg:
    xor     eax, eax                /* T::Neg = 0                             */
    ret
.ma_ret_zero:
    mov     eax, 2                  /* T::Zero = 2                            */
    ret


/* ═══════════════════════════════════════════════════════════════════════════
 * joinany_sse2 — lattice join (max) over all N cells
 *
 * %rdi = p0 plane   %rsi = p1 plane   %rdx = n_qwords
 * Returns: T::Neg(0) | T::Pos(1) | T::Zero(2)
 *
 *   any Pos  → Pos
 *   any Zero (no Pos) → Zero
 *   all Neg  → Neg
 * ═══════════════════════════════════════════════════════════════════════════ */
    .global joinany_sse2
joinany_sse2:
    pxor    xmm4, xmm4              /* any_pos  accumulator = 0               */
    pxor    xmm5, xmm5              /* any_zero accumulator = 0               */
    xor     eax, eax

    mov     rcx, rdx
    shr     rcx, 1
    jz      .ja_tail

.ja_loop:
    movdqu  xmm0, [rdi + rax]
    movdqu  xmm1, [rsi + rax]

    /* pos_bits = ~p1 & p0  via  PANDN(p1_copy, p0) */
    movdqa  xmm2, xmm1
    pandn   xmm2, xmm0              /* ~p1 & p0 = pos_bits                   */
    por     xmm4, xmm2

    /* zero_bits = ~p0 & p1  via  PANDN(p0_copy, p1) */
    movdqa  xmm2, xmm0
    pandn   xmm2, xmm1              /* ~p0 & p1 = zero_bits                  */
    por     xmm5, xmm2

    add     eax, 16
    dec     rcx
    jnz     .ja_loop

.ja_tail:
    test    rdx, 1
    jz      .ja_reduce

    movq    xmm0, [rdi + rax]
    movq    xmm1, [rsi + rax]

    movdqa  xmm2, xmm1
    pandn   xmm2, xmm0
    por     xmm4, xmm2

    movdqa  xmm2, xmm0
    pandn   xmm2, xmm1
    por     xmm5, xmm2

.ja_reduce:
    pxor    xmm7, xmm7
    pcmpeqb xmm4, xmm7
    pmovmskb eax, xmm4
    cmp     eax, 0xFFFF
    jne     .ja_ret_pos             /* had Pos bits */

    pcmpeqb xmm5, xmm7
    pmovmskb eax, xmm5
    cmp     eax, 0xFFFF
    jne     .ja_ret_zero            /* no Pos, had Zero bits */

    xor     eax, eax                /* T::Neg = 0                             */
    ret
.ja_ret_pos:
    mov     eax, 1                  /* T::Pos = 1                             */
    ret
.ja_ret_zero:
    mov     eax, 2                  /* T::Zero = 2                            */
    ret


/* ═══════════════════════════════════════════════════════════════════════════
 * region_quiescent — 1 if no cell is Neg, 0 otherwise
 *
 * %rdi = p0 plane   %rsi = p1 plane   %rdx = n_qwords
 * Returns: 1 (quiescent) | 0 (pending)
 *
 * A region is quiescent when neg_bits = ~(p0|p1) = 0 everywhere.
 * Directly mirrors the RTL:
 *   assign region_quiescent = ~(|ce_out)
 *   assign ce_out[i] = ~is_at_zero[i] & ~queue_empty[i]
 * ═══════════════════════════════════════════════════════════════════════════ */
    .global region_quiescent
region_quiescent:
    pxor    xmm4, xmm4              /* any_neg acc = 0                        */
    pcmpeqb xmm6, xmm6              /* all-ones (for ~(p0|p1))                */
    xor     eax, eax

    mov     rcx, rdx
    shr     rcx, 1
    jz      .rq_tail

.rq_loop:
    movdqu  xmm0, [rdi + rax]
    movdqu  xmm1, [rsi + rax]
    por     xmm0, xmm1              /* p0 | p1                                */
    pxor    xmm0, xmm6              /* ~(p0 | p1) = neg_bits                  */
    por     xmm4, xmm0              /* any_neg |= neg_bits                    */
    add     eax, 16
    dec     rcx
    jnz     .rq_loop

.rq_tail:
    test    rdx, 1
    jz      .rq_reduce

    movq    xmm0, [rdi + rax]
    movq    xmm1, [rsi + rax]
    por     xmm0, xmm1
    pxor    xmm0, xmm6
    por     xmm4, xmm0

.rq_reduce:
    pxor    xmm7, xmm7
    pcmpeqb xmm4, xmm7              /* 0xFF where byte was 0 (no Neg bits)    */
    pmovmskb eax, xmm4
    cmp     eax, 0xFFFF
    je      .rq_yes                 /* all bytes 0xFF → accumulator was 0     */

    xor     eax, eax                /* 0: not quiescent                       */
    ret
.rq_yes:
    mov     eax, 1                  /* 1: quiescent                           */
    ret
