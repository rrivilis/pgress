// ternary_cell_top.sv — N=8 synthesis wrapper for standalone PnR.
//
// Fixes the N parameter to keep the input bus manageable for standalone layout
// (N=500 would require 1000 IO pins; N=8 = 16 input pins).
// The gate structure and same-value suppression semantics are identical at any N.
//
// RULE=0: MeetAll  (Neg dominates → Zero → Pos)
// Change RULE to 1 for JoinAny variant.

module ternary_cell_top (
    input  wire        clk,
    input  wire        rst,
    input  wire [15:0] inputs,   // 8 × 2-bit packed ternary inputs
    output wire [1:0]  out,      // registered 2-bit ternary result
    output wire        ce_out    // pulses high on output value change
);

    ternary_cell #(
        .N   (8),
        .RULE(0)
    ) u_cell (
        .clk   (clk),
        .rst   (rst),
        .inputs(inputs),
        .out   (out),
        .ce_out(ce_out)
    );

endmodule
