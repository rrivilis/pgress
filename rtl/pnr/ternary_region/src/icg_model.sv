// icg_model.sv — sky130 ASIC synthesis target.
//
// Replaces the behavioral latch model (rtl/sv/icg_model.sv) with a direct
// instantiation of sky130_fd_sc_hd__dlclkp_1.
//
// Interface is identical: clk / enable / gclk.
// ternary_region.sv requires no modification; this file overrides the model
// by being placed in the design src/ directory ahead of the generic version.
//
// dlclkp_1 semantics (matches behavioral model exactly):
//   - Latch is transparent when CLK = 0 (low phase); samples GATE.
//   - GCLK = CLK AND en_latch
//   - GATE=1 → gclk follows clk  (region active, propagation running)
//   - GATE=0 → gclk held low     (region quiescent, clock dark)
//
// In ternary_region: gate_enable = (~quiescent) | rst
//   quiescent=0 → GATE=1 → clock runs → cells can fire
//   quiescent=1 → GATE=0 → clock dark → fixed point, zero dynamic power
//
// The quiescence wire driving GATE is the physical manifestation of the
// Zero fixed-point suppression predicate: ~(p0|p1) == 0 over the region.
// In GDSII this appears as a routed net between the NOR reduction and the
// ICG cell's GATE pin on the clock trunk.

module icg_model (
    input  wire clk,
    input  wire enable,   // 1 = clock passes; 0 = gclk held low (quiescent)
    output wire gclk
);

    sky130_fd_sc_hd__dlclkp_1 u_icg (
        .CLK (clk),
        .GATE(enable),
        .GCLK(gclk)
    );

endmodule
