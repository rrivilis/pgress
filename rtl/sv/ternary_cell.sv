// rtl/sv/ternary_cell.sv
//
// Parameterized ternary compute cell -- MeetAll or JoinAny over N inputs.
//
// Encoding: {p1, p0} = 2'b00=Neg, 2'b01=Pos, 2'b10=Zero, 2'b11=DC (don't-care)
//
//   inputs[2*i]   = p0 bit of input i  (set iff Pos or DC)
//   inputs[2*i+1] = p1 bit of input i  (set iff Zero or DC)
//
// RULE parameter:
//   0 = MeetAll  -- Neg dominates, then Zero, then Pos  (lattice meet / AND)
//   1 = JoinAny  -- Pos dominates, then Zero, then Neg  (lattice join / OR)
//
// Clock-enable (same-value suppression):
//   The output register only latches when the combinational result differs
//   from the currently stored value. ce_out pulses high on every output change.
//
// Wide bitwise-NOT padding hazard:
//   Simulation tools store N-bit vectors in ceil(N/64)*64-bit words.
//   A bitwise ~ on a 500-bit wire flips the 12 padding bits (500-511) to 1,
//   making |(~p0 & ~p1) permanently 1 and any_neg always true.
//   Fix: always_comb loop over individual input slots -- no wide NOT needed.

`timescale 1ns/1ps

module ternary_cell #(
    parameter integer N    = 500,  // number of ternary inputs
    parameter integer RULE = 0     // 0 = MeetAll, 1 = JoinAny
) (
    input  wire           clk,
    input  wire           rst,
    input  wire [N*2-1:0] inputs,  // packed 2-bit values: inputs[2*i+1:2*i] = {p1_i, p0_i}
    output reg  [1:0]     out,     // registered ternary output {p1, p0}
    output wire           ce_out   // pulses high when output changes
);

    // ── Per-input reduction ───────────────────────────────────────────────────
    // Iterate each of the N input slots, classify each 2-bit value, fold into
    // three boolean flags. Avoids wide bitwise-NOT and the padding-bit hazard.

    logic any_neg, any_zero, any_pos;

    always_comb begin : p_reduce
        automatic logic p0_i, p1_i;
        any_neg  = 1'b0;
        any_zero = 1'b0;
        any_pos  = 1'b0;
        for (int k = 0; k < N; k++) begin
            p0_i = inputs[2*k];
            p1_i = inputs[2*k+1];
            any_neg  = any_neg  | (~p0_i & ~p1_i);  // Neg:  p0=0, p1=0
            any_zero = any_zero | (~p0_i &  p1_i);  // Zero: p0=0, p1=1
            any_pos  = any_pos  | ( p0_i & ~p1_i);  // Pos:  p0=1, p1=0
        end
    end

    // ── Priority mux (combinational) ─────────────────────────────────────────
    reg [1:0] comb_out;
    always_comb begin : p_mux
        if (RULE == 0) begin
            // MeetAll: Neg > Zero > Pos
            if      (any_neg)  comb_out = 2'b00;  // Neg
            else if (any_zero) comb_out = 2'b10;  // Zero
            else               comb_out = 2'b01;  // Pos
        end else begin
            // JoinAny: Pos > Zero > Neg
            if      (any_pos)  comb_out = 2'b01;  // Pos
            else if (any_zero) comb_out = 2'b10;  // Zero
            else               comb_out = 2'b00;  // Neg
        end
    end

    // ── Clock-enable (same-value suppression) ────────────────────────────────
    // ce fires when comb_out differs from stored out. When ce=0 the flop does
    // not toggle -- zero dynamic power, direct hardware equivalent of Opt 7.
    wire ce = (comb_out != out);
    assign ce_out = ce;

    // ── Output register ──────────────────────────────────────────────────────
    always @(posedge clk) begin
        if (rst)
            out <= 2'b00;       // reset to Neg
        else if (ce)
            out <= comb_out;    // latch only on value change
    end

endmodule
