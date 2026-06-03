// rtl/sv/ternary_chain.sv
//
// K-stage ternary pipeline chain.
//
// Layout:
//   Stage 0 : ternary_cell, inputs = chain_in[FAN_IN*2-1:0]
//   Stage k : ternary_cell, inputs[0] = out of stage k-1,
//                           inputs[1..FAN_IN-1] = side_in[k][FAN_IN-1:1] (packed)
//
// The first input slot of every stage (except stage 0) is wired to the
// previous stage output.  This creates a dependency chain: a change at
// stage 0 must propagate forward one cycle per stage before the fabric
// reaches quiescence.
//
// quiescent: high when no cell fired a ce_out this cycle.
// quiescent_age: saturating count of cycles since last global change.
//
// Parameters
//   K       : number of stages (depth)
//   FAN_IN  : ternary inputs per cell (must be >= 2 for the chain wire)
//   RULE    : 0=MeetAll, 1=JoinAny (same for all stages)

`timescale 1ns/1ps

module ternary_chain #(
    parameter integer K      = 8,    // pipeline depth
    parameter integer FAN_IN = 4,    // inputs per cell (>= 2)
    parameter integer RULE   = 0     // 0=MeetAll, 1=JoinAny
) (
    input  wire                         clk,
    input  wire                         rst,

    // Stage 0 primary inputs (all FAN_IN slots)
    input  wire [FAN_IN*2-1:0]          chain_in,

    // Side inputs for stages 1..K-1: slot 0 is the chain wire;
    // slots 1..FAN_IN-1 come from side_in[k-1].
    // side_in is packed: [k*(FAN_IN-1)*2 +: (FAN_IN-1)*2] for stage k.
    input  wire [K*(FAN_IN-1)*2-1:0]   side_in,

    // Registered output of each stage
    output wire [K*2-1:0]              stage_out,

    // ce_out per stage (packed)
    output wire [K-1:0]                stage_ce,

    // Global quiescence: all ce_out == 0 this cycle
    output wire                        quiescent,

    // Saturating count of cycles since last global change (13-bit, max 8191)
    output reg  [12:0]                 quiescent_age
);

    // ── Per-stage wires ───────────────────────────────────────────────────────
    wire [1:0] cell_out [0:K-1];
    wire       cell_ce  [0:K-1];

    // ── Stage 0 ───────────────────────────────────────────────────────────────
    ternary_cell #(.N(FAN_IN), .RULE(RULE)) stage0 (
        .clk    (clk),
        .rst    (rst),
        .inputs (chain_in),
        .out    (cell_out[0]),
        .ce_out (cell_ce[0])
    );

    // ── Stages 1..K-1 ────────────────────────────────────────────────────────
    // Input word = {chain_wire, side_in_slots} packed as 2*(FAN_IN) bits.
    // Slot 0 = chain wire (previous stage output, 2 bits).
    // Slots 1..FAN_IN-1 = side_in[(k-1)*(FAN_IN-1)*2 +: (FAN_IN-1)*2].

    genvar k;
    generate
        for (k = 1; k < K; k++) begin : gen_stages
            wire [FAN_IN*2-1:0] cell_inputs;
            assign cell_inputs = {
                side_in[(k-1)*(FAN_IN-1)*2 +: (FAN_IN-1)*2],  // slots FAN_IN-1 downto 1
                cell_out[k-1]                                    // slot 0
            };

            ternary_cell #(.N(FAN_IN), .RULE(RULE)) stagek (
                .clk    (clk),
                .rst    (rst),
                .inputs (cell_inputs),
                .out    (cell_out[k]),
                .ce_out (cell_ce[k])
            );
        end
    endgenerate

    // ── Output buses ─────────────────────────────────────────────────────────
    genvar s;
    generate
        for (s = 0; s < K; s++) begin : gen_out
            assign stage_out[s*2 +: 2] = cell_out[s];
            assign stage_ce[s]         = cell_ce[s];
        end
    endgenerate

    // ── Quiescence ───────────────────────────────────────────────────────────
    // quiescent = NOR of all ce_out signals.
    // Any cell firing resets the age counter.
    wire any_ce = |stage_ce;
    assign quiescent = ~any_ce;

    always @(posedge clk) begin
        if (rst)
            quiescent_age <= 13'd0;
        else if (any_ce)
            quiescent_age <= 13'd0;
        else if (quiescent_age != 13'h1FFF)
            quiescent_age <= quiescent_age + 13'd1;
    end

endmodule
