// rtl/sv/ternary_cell_syn.sv
//
// Synthesis-compatible variant of ternary_cell for Yosys.
// Identical logic; `automatic` removed (synthesis-irrelevant, Yosys unsupported).
// NOT used in simulation — tb_meetall and tb_topology use ternary_cell.sv.

`timescale 1ns/1ps

module ternary_cell #(
    parameter integer N    = 500,
    parameter integer RULE = 0
) (
    input  wire           clk,
    input  wire           rst,
    input  wire [N*2-1:0] inputs,
    output reg  [1:0]     out,
    output wire           ce_out
);

    logic any_neg, any_zero, any_pos;
    logic p0_i, p1_i;

    always_comb begin : p_reduce
        any_neg  = 1'b0;
        any_zero = 1'b0;
        any_pos  = 1'b0;
        p0_i     = 1'b0;
        p1_i     = 1'b0;
        for (int k = 0; k < N; k++) begin
            p0_i = inputs[2*k];
            p1_i = inputs[2*k+1];
            any_neg  = any_neg  | (~p0_i & ~p1_i);
            any_zero = any_zero | (~p0_i &  p1_i);
            any_pos  = any_pos  | ( p0_i & ~p1_i);
        end
    end

    reg [1:0] comb_out;
    always_comb begin : p_mux
        if (RULE == 0) begin
            if      (any_neg)  comb_out = 2'b00;
            else if (any_zero) comb_out = 2'b10;
            else               comb_out = 2'b01;
        end else begin
            if      (any_pos)  comb_out = 2'b01;
            else if (any_zero) comb_out = 2'b10;
            else               comb_out = 2'b00;
        end
    end

    wire ce = (comb_out != out);
    assign ce_out = ce;

    always @(posedge clk) begin
        if (rst)
            out <= 2'b00;
        else if (ce)
            out <= comb_out;
    end

endmodule
