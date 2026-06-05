// rtl/pnr/ternary_cell/src/ternary_cell.sv
//
// PnR override of rtl/sv/ternary_cell.sv.
//
// The `automatic` keyword inside always_comb is not supported by the Yosys
// version bundled in the OpenLane container.  This override is semantically
// identical to the canonical RTL but inlines the p0/p1 bit expressions
// directly, eliminating the automatic local variable declaration.
//
// DO NOT modify this file for simulation — edit rtl/sv/ternary_cell.sv instead.

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
    // NOTE: automatic local vars removed for Yosys compatibility.

    logic any_neg, any_zero, any_pos;

    always_comb begin : p_reduce
        any_neg  = 1'b0;
        any_zero = 1'b0;
        any_pos  = 1'b0;
        for (int k = 0; k < N; k++) begin
            any_neg  = any_neg  | (~inputs[2*k] & ~inputs[2*k+1]);  // Neg:  p0=0, p1=0
            any_zero = any_zero | (~inputs[2*k] &  inputs[2*k+1]);  // Zero: p0=0, p1=1
            any_pos  = any_pos  | ( inputs[2*k] & ~inputs[2*k+1]);  // Pos:  p0=1, p1=0
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
