// rtl/sv/icg_model.sv
//
// Behavioral model of dlclkp_1 integrated clock gate (sky130_fd_sc_hd).
//
// Structure: transparent-low latch + AND gate.
//   - enable is sampled when clk is LOW (latch is transparent during low phase)
//   - gclk = clk AND latched_enable
//
// When enable=1 (region active): latch holds 1 → gclk follows clk.
// When enable=0 (region quiescent): latch holds 0 → gclk stays low.
//
// The latch prevents glitches on gclk: enable transitions that arrive while
// clk=1 are not visible on gclk until the next low phase captures them.
//
// This is the RTL analog of sky130_fd_sc_hd__dlclkp_1 (pin: CLK GATE → GCLK).
// The key property: once enable=0 is captured at the falling edge, every
// subsequent rising edge of clk sees en_latch=0 and produces gclk=0.

/* verilator lint_off LATCH */
/* verilator lint_off COMBDLY */
module icg_model (
    input  wire clk,
    input  wire enable,   // 1 = clock passes through; 0 = gclk held low
    output wire gclk
);
    logic en_latch;

    // Level-sensitive latch: transparent when clk=0, holds when clk=1.
    // Captures enable at the falling edge of clk.
    // Blocking assignment used (= not <=) to satisfy the simulator's
    // always_latch combinational-process requirement.
    always_latch
        if (!clk) en_latch = enable;

    assign gclk = clk & en_latch;

endmodule
/* verilator lint_on COMBDLY */
/* verilator lint_on LATCH */
