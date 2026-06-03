// rtl/sv/ternary_tree.sv
//
// L-level ternary reduction tree.
//
// Topology (FANOUT=2, LEVELS=3 example):
//
//   Level 0 (leaves): 4 cells, each consuming LEAF_FAN primary inputs
//   Level 1          : 2 cells, each consuming 2 level-0 outputs + (FANOUT-2) extra inputs
//   Level 2 (root)  : 1 cell, consuming 2 level-1 outputs + (FANOUT-2) extra inputs
//
// General: level L has FANOUT^(LEVELS-1-L) cells.
// Total leaves = FANOUT^(LEVELS-1).
//
// A change at any leaf propagates to the root in (LEVELS-1) clock cycles.
// This is the RTL analog of a multi-hop region dependency chain.
//
// Parameters:
//   LEVELS   : tree depth (1 = single cell with FANOUT leaf inputs)
//   FANOUT   : children per internal node (also inputs per cell at internal levels)
//   LEAF_FAN : primary inputs per leaf cell
//   RULE     : 0=MeetAll, 1=JoinAny (same for all cells)
//
// Restrictions: FANOUT >= 2; LEVELS >= 1; LEAF_FAN >= 1.
// Max cells = (FANOUT^LEVELS - 1) / (FANOUT - 1) -- keep FANOUT*LEVELS small.
// Tested configuration: FANOUT=2, LEVELS=4 -> 15 cells, 8 leaves x LEAF_FAN=4 inputs.

`timescale 1ns/1ps

module ternary_tree #(
    parameter integer LEVELS   = 4,   // tree depth
    parameter integer FANOUT   = 2,   // children per internal node
    parameter integer LEAF_FAN = 4,   // primary inputs per leaf cell
    parameter integer RULE     = 0    // 0=MeetAll, 1=JoinAny
) (
    input  wire clk,
    input  wire rst,

    // Primary inputs: FANOUT^(LEVELS-1) leaf cells, each with LEAF_FAN inputs.
    // Total bits = FANOUT^(LEVELS-1) * LEAF_FAN * 2.
    // leaf_inputs[(i*LEAF_FAN*2) +: LEAF_FAN*2] = inputs for leaf i.
    input  wire [((FANOUT**(LEVELS-1)) * LEAF_FAN * 2) - 1 : 0] leaf_inputs,

    // Root output (registered)
    output wire [1:0]  root_out,
    output wire        root_ce,

    // Global quiescence
    output wire        quiescent,
    output reg  [12:0] quiescent_age
);

    // ── Compute tree dimensions ───────────────────────────────────────────────
    // Level 0 = leaf level: N_LEAVES = FANOUT^(LEVELS-1) cells
    // Level L = FANOUT^(LEVELS-1-L) cells
    // Root (level LEVELS-1): 1 cell
    //
    // We unroll only LEVELS=4, FANOUT=2 to keep SV generate manageable.
    // For other configs use the parameterized LEVELS generate below.
    //
    // Implementation strategy: generate arrays indexed [level][cell_in_level].
    // Verilog doesn't allow 2D port arrays easily, so we flatten into:
    //   cell_out[level * MAX_CELLS_PER_LEVEL + cell]
    //   cell_ce [level * MAX_CELLS_PER_LEVEL + cell]
    //
    // MAX_CELLS_PER_LEVEL = FANOUT^(LEVELS-1) (leaf count -- widest level).

    localparam integer N_LEAVES        = FANOUT ** (LEVELS - 1);
    localparam integer MAX_CPL         = N_LEAVES;  // max cells per level
    localparam integer TOTAL_CELLS     = (FANOUT ** LEVELS - 1) / (FANOUT - 1);

    // Flattened cell outputs and ce signals.
    // Index: [level * MAX_CPL + cell_in_level]
    wire [1:0] cell_out [0 : LEVELS*MAX_CPL - 1];
    wire       cell_ce  [0 : LEVELS*MAX_CPL - 1];

    // ── Leaf level (level 0) ──────────────────────────────────────────────────
    genvar li;
    generate
        for (li = 0; li < N_LEAVES; li++) begin : gen_leaves
            ternary_cell #(.N(LEAF_FAN), .RULE(RULE)) leaf_cell (
                .clk    (clk),
                .rst    (rst),
                .inputs (leaf_inputs[li*LEAF_FAN*2 +: LEAF_FAN*2]),
                .out    (cell_out[0*MAX_CPL + li]),
                .ce_out (cell_ce [0*MAX_CPL + li])
            );
        end
    endgenerate

    // ── Internal levels (1 .. LEVELS-1) ───────────────────────────────────────
    // Level lv has FANOUT^(LEVELS-1-lv) cells.
    // Cell ci at level lv takes FANOUT inputs: children ci*FANOUT .. ci*FANOUT+FANOUT-1
    // from level lv-1.
    genvar lv, ci, fi;
    generate
        for (lv = 1; lv < LEVELS; lv++) begin : gen_levels
            localparam integer N_CELLS_LV = FANOUT ** (LEVELS - 1 - lv);
            for (ci = 0; ci < N_CELLS_LV; ci++) begin : gen_cells
                wire [FANOUT*2-1:0] cell_inputs_w;
                for (fi = 0; fi < FANOUT; fi++) begin : gen_fan
                    assign cell_inputs_w[fi*2 +: 2] =
                        cell_out[(lv-1)*MAX_CPL + ci*FANOUT + fi];
                end
                ternary_cell #(.N(FANOUT), .RULE(RULE)) internal_cell (
                    .clk    (clk),
                    .rst    (rst),
                    .inputs (cell_inputs_w),
                    .out    (cell_out[lv*MAX_CPL + ci]),
                    .ce_out (cell_ce [lv*MAX_CPL + ci])
                );
            end
        end
    endgenerate

    // ── Root output ───────────────────────────────────────────────────────────
    assign root_out = cell_out[(LEVELS-1)*MAX_CPL + 0];
    assign root_ce  = cell_ce [(LEVELS-1)*MAX_CPL + 0];

    // ── Global quiescence ─────────────────────────────────────────────────────
    // NOR all ce_out signals across all levels and cells.
    // Reduction loop over all cell slots; unused slots have ce=0 (generate ties them off).
    reg any_ce_r;
    integer idx;
    always_comb begin
        any_ce_r = 1'b0;
        for (idx = 0; idx < LEVELS * MAX_CPL; idx++) begin
            // Only OR valid cells; unused slots have ce=0 (tied off by generate)
            any_ce_r = any_ce_r | cell_ce[idx];
        end
    end

    assign quiescent = ~any_ce_r;

    always @(posedge clk) begin
        if (rst)
            quiescent_age <= 13'd0;
        else if (any_ce_r)
            quiescent_age <= 13'd0;
        else if (quiescent_age != 13'h1FFF)
            quiescent_age <= quiescent_age + 13'd1;
    end

endmodule
