// rtl/sv/meetall_500.sv
//
// 500-input MeetAll top module.
// Instantiates ternary_cell with N=500, RULE=0 (MeetAll).
//
// This is the hardware target for the pgress hot path:
//   500 ternary inputs → 1 registered ternary output
//   Clock-enable suppression: output register only toggles on value change.
//
// Port:
//   inputs[999:0]  — 500 × 2-bit packed ternary values
//                    inputs[2*i+1:2*i] = {p1_i, p0_i} for i in [0, 499]
//   out[1:0]       — registered 2-bit ternary result
//   ce_out         — pulses high for one cycle on output change

`timescale 1ns/1ps

module meetall_500 (
    input  wire          clk,
    input  wire          rst,
    input  wire [999:0]  inputs,   // 500 × 2-bit packed inputs
    output wire [1:0]    out,      // 2-bit ternary result (registered)
    output wire          ce_out    // output-change strobe
);

    ternary_cell #(
        .N    (500),
        .RULE (0)     // 0 = MeetAll
    ) u_meetall (
        .clk    (clk),
        .rst    (rst),
        .inputs (inputs),
        .out    (out),
        .ce_out (ce_out)
    );

endmodule
