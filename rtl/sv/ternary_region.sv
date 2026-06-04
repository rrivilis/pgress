// rtl/sv/ternary_region.sv
//
// Ternary compute region with region-level integrated clock gate.
//
// Wraps ternary_chain with an icg_model that gates the trunk clock when the
// entire region has reached its ternary fixed point (quiescent=1).
//
// Clock hierarchy:
//   clk (trunk, always-on)
//     └─ region_icg (icg_model)
//          └─ gclk (gated, fed to all cells)
//               └─ ternary_chain (K stages, FAN_IN inputs per cell)
//
// Quiescence signal path:
//   ternary_chain.ce_out[i]  (per-cell, combinational: comb_out != out)
//     → any_ce = |ce_out      (NOR reduction inside ternary_chain)
//     → quiescent = ~any_ce   (exposed as output)
//     → gate_enable = ~quiescent | rst  (keep clock active during reset)
//     → icg_model.enable
//     → gclk = clk & en_latch (latch captured at clk falling edge)
//
// The feedback quiescent→gate_enable→ICG→gclk is broken by the ICG latch:
// en_latch only updates when clk=0, so there is no combinational loop.
//
// Quiescence properties:
//   - quiescent=1 when all cells have comb_out == out (fixed point reached)
//   - gclk darkens one CLK cycle after quiescent first asserts
//     (ICG captures gate_enable=0 at the next CLK falling edge)
//   - Once gclk=0: cell registers are frozen; combinational quiescent
//     remains valid (dependent only on frozen out and current inputs)
//   - If any input changes while gclk=0: comb_out may diverge from out
//     → quiescent drops → gate_enable rises → ICG re-opens → gclk resumes
//
// Gated trunk cycles counter (gated_trunk_cycles):
//   Uses always-on clk. Counts trunk clock edges while quiescent=1.
//   Resets to 0 whenever quiescent=0. This is the observable evidence that
//   the trunk clock is "banked" while the region is at fixed point.
//
// quiescent_age: forwarded from ternary_chain. Clocked by gclk; counts
//   gated-domain cycles since last cell fired. Freezes when gclk=0.
//
// Parameters:
//   K       : chain depth (number of ternary_cell stages)
//   FAN_IN  : inputs per cell (must be >= 2)
//   RULE    : 0=MeetAll, 1=JoinAny

`timescale 1ns/1ps

module ternary_region #(
    parameter integer K      = 8,
    parameter integer FAN_IN = 4,
    parameter integer RULE   = 0
) (
    input  wire clk,   // always-on trunk clock
    input  wire rst,

    // Chain inputs (same layout as ternary_chain)
    input  wire [FAN_IN*2-1:0]        chain_in,
    input  wire [K*(FAN_IN-1)*2-1:0]  side_in,

    // Outputs
    output wire [K*2-1:0]  stage_out,         // registered output per stage
    output wire [K-1:0]    stage_ce,          // ce pulse per stage (gated clock domain)
    output wire            quiescent,         // 1 when region at fixed point (combinational)
    output wire [12:0]     quiescent_age,     // cycles since last change (gated domain, from chain)
    output wire            gclk,              // gated clock fed to chain cells
    output wire            gclk_active,       // gate_enable: 1 when ICG is open
    output reg  [12:0]     gated_trunk_cycles // trunk CLK cycles while quiescent (always-on domain)
);

    // ── Region ICG ───────────────────────────────────────────────────────────
    // gate_enable=1: region active (or in reset) → gclk follows clk
    // gate_enable=0: region quiescent → gclk held low
    wire gate_enable = (~quiescent) | rst;

    icg_model region_icg (
        .clk    (clk),
        .enable (gate_enable),
        .gclk   (gclk)
    );

    // gclk_active is the combinational gate_enable signal.
    // There is a one-cycle lag between gate_enable dropping and gclk actually
    // darkening (ICG latch captures at the next CLK falling edge). The output
    // reflects intent; check gclk waveform for the precise gating edge.
    assign gclk_active = gate_enable;

    // ── Chain (receives gated clock) ─────────────────────────────────────────
    // ternary_chain uses its clk input for both cell registers and quiescent_age.
    // Feeding gclk here means:
    //   - Cells latch only when gclk pulses (region active)
    //   - quiescent_age counts gated-domain cycles (freezes when gclk=0)
    wire       ch_quiescent;
    wire [12:0] ch_age;

    ternary_chain #(.K(K), .FAN_IN(FAN_IN), .RULE(RULE)) chain (
        .clk          (gclk),
        .rst          (rst),
        .chain_in     (chain_in),
        .side_in      (side_in),
        .stage_out    (stage_out),
        .stage_ce     (stage_ce),
        .quiescent    (ch_quiescent),
        .quiescent_age(ch_age)
    );

    assign quiescent     = ch_quiescent;
    assign quiescent_age = ch_age;

    // ── Gated trunk cycle counter (always-on CLK domain) ─────────────────────
    // Increments each trunk clock edge while quiescent=1.
    // Resets to 0 on the first trunk edge when quiescent=0 (region wakes).
    // This is the observable evidence that the trunk clock banks up while the
    // region is at fixed point -- power proportional to productive work.
    always @(posedge clk) begin
        if (rst)
            gated_trunk_cycles <= 13'd0;
        else if (quiescent)
            gated_trunk_cycles <= (gated_trunk_cycles < 13'h1FFF) ?
                                   gated_trunk_cycles + 13'd1 :
                                   13'h1FFF;
        else
            gated_trunk_cycles <= 13'd0;
    end

endmodule
