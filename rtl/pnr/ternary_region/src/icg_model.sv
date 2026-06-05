// rtl/pnr/ternary_region/src/icg_model.sv
//
// Behavioral ICG model for OpenLane synthesis.
//
// Direct instantiation of sky130_fd_sc_hd__dlclkp_1 in RTL fails Yosys
// hierarchy analysis: the cell lives in the liberty files, which are read
// after hierarchy resolution — it is not a module in the source tree.
//
// This behavioral model is the correct synthesis input:
//   - Yosys elaborates and optimizes it cleanly
//   - The tech-mapper maps the latch+AND to sky130_fd_sc_hd__dlclkp_1
//     (or equivalent ICG cell) during cell mapping
//   - The resulting post-synthesis netlist contains the real PDK cell
//
// Semantics: level-sensitive transparent latch on low clk, AND gate output.
// Exact behavioral equivalent of sky130_fd_sc_hd__dlclkp_1:
//   enable=1, clk=0  → en_latch captures 1
//   clk rises         → gclk follows clk while en_latch=1
//   enable=0, clk=0  → en_latch captures 0 → gclk held low
//
// In ternary_region: gate_enable = (~quiescent) | rst
//   quiescent=0 → enable=1 → clock runs → cells can fire
//   quiescent=1 → enable=0 → gclk dark  → fixed point, zero dynamic power
//
// The quiescence net driving enable is the physical manifestation of the
// Zero fixed-point suppression predicate. In GDSII this appears as a routed
// net between the NOR reduction and the ICG cell's GATE pin on the clock trunk.

`timescale 1ns/1ps

module icg_model (
    input  wire clk,
    input  wire enable,   // 1 = clock passes; 0 = gclk held low (quiescent)
    output wire gclk
);
    reg en_latch;

    // Level-sensitive latch: transparent when clk is low.
    always_latch
        if (!clk) en_latch = enable;

    assign gclk = clk & en_latch;

endmodule
