// rtl/sv/tb_meetall.sv
//
// SystemVerilog testbench for meetall_500.
//
// Scenarios match spec/benchmarks.md Criterion scenarios and the HLS testbench:
//   A — Convergence:   500 inputs Neg → Pos one at a time; output fires once (on last)
//   B — Hot toggle:    input[0] cycles Zero ↔ Pos, rest Pos; 200 cycles
//   C — No-op:         all inputs Pos, re-drive same values; ce_out must stay 0
//   D — Zero poison:   one input Zero, rest Pos; output must be Zero
//   E — Neg last word: Neg at slot 498 (word 7, bit 50); regression for LAST_MASK
//
// Clock: 4 ns period (250 MHz target). Each clock edge is at t=2, 6, 10, ...
// Stimulus is driven immediately after posedge (+#1 setup margin).
//
// Timing benchmark:
//   $time is recorded around Scenarios B and C to report ns/cycle and ns/round.
//   Compare against Criterion p50: hot_toggle_zero_pos ~3.68 µs, noop ~90 ns.
//   RTL numbers reflect pure combinational + register latency; Criterion numbers
//   include Rust queue drain, dep-counter update, and causal clock tick.

`timescale 1ns/1ps

module tb_meetall;

    // ── DUT interface ────────────────────────────────────────────────────────
    reg          clk    = 0;
    reg          rst    = 1;
    reg  [999:0] inputs = '0;   // all Neg initially (2'b00 per slot)
    wire [1:0]   out;
    wire         ce_out;

    // 4 ns period → posedge at t = 2, 6, 10, ...
    always #2 clk = ~clk;

    meetall_500 dut (
        .clk    (clk),
        .rst    (rst),
        .inputs (inputs),
        .out    (out),
        .ce_out (ce_out)
    );

    // ── Helpers ──────────────────────────────────────────────────────────────

    // Set input i to a 2-bit ternary value.
    task automatic set_input(input integer idx, input [1:0] val);
        inputs[2*idx +: 2] = val;
    endtask

    // Set all 500 inputs to the same ternary value.
    // NOTE: must use whole-vector replication, NOT a per-slot loop.
    // With --timing, always_comb sensitivity does not reliably fire when a
    // 1000-bit packed array receives 500 individual slice assignments in a
    // tight loop without an intervening clock edge. A single whole-vector
    // assign triggers re-evaluation in one delta cycle.
    task automatic set_all(input [1:0] val);
        inputs = {500{val}};
    endtask

    // Advance one clock, check output, optionally assert ce_out.
    task automatic tick_check(
        input [1:0]  expected_out,
        input        check_ce,
        input        expected_ce,
        input string label
    );
        @(posedge clk); #1;
        if (out !== expected_out) begin
            $display("FAIL [%s]: out=%02b (expected %02b) at t=%0t",
                     label, out, expected_out, $time);
            tb_failures = tb_failures + 1;
        end
        if (check_ce && ce_out !== expected_ce) begin
            $display("FAIL [%s]: ce_out=%b (expected %b) at t=%0t",
                     label, ce_out, expected_ce, $time);
            tb_failures = tb_failures + 1;
        end
    endtask

    // ── State ────────────────────────────────────────────────────────────────
    integer tb_failures = 0;
    integer i, cycle;
    longint t_start, t_end;

    // ── Stimulus ─────────────────────────────────────────────────────────────
    initial begin
        // Reset for 2 cycles
        rst = 1;
        repeat (2) @(posedge clk);
        #1; rst = 0;

        // ── Scenario A: Convergence ──────────────────────────────────────────
        // All inputs start Neg (already set). Output must remain Neg through
        // input 498, then flip to Pos when input 499 arrives.

        // Check initial state after reset settles.
        @(posedge clk); #1;
        if (out !== 2'b00) begin
            $display("FAIL A-init: out=%02b after reset (expected Neg)", out);
            tb_failures++;
        end

        for (i = 0; i < 499; i++) begin
            set_input(i, 2'b01);  // Pos
            @(posedge clk); #1;
            if (out !== 2'b00) begin
                $display("FAIL A: output changed at i=%0d (got %02b, expected Neg)", i, out);
                tb_failures++;
                i = 499; // break: don't flood with 500 failures
            end
        end

        set_input(499, 2'b01);    // last input → all 500 Pos
        @(posedge clk); #1;
        if (out !== 2'b01) begin
            $display("FAIL A-final: out=%02b (expected Pos)", out);
            tb_failures++;
        end else begin
            $display("Scenario A (convergence, 500 inputs): PASS");
        end

        // ── Scenario B: Hot toggle ────────────────────────────────────────────
        // input[0] alternates Zero ↔ Pos. All other inputs stable Pos.
        // Records wall-ns for 200 cycles.

        set_all(2'b01);       // all Pos baseline
        @(posedge clk); #1;
        assert (out === 2'b01) else $display("B setup: expected Pos baseline");

        t_start = $time;
        for (cycle = 0; cycle < 200; cycle++) begin
            // Zero injection → output must become Zero (Bochvar)
            set_input(0, 2'b10);
            @(posedge clk); #1;
            if (out !== 2'b10) begin
                $display("FAIL B cycle %0d: Zero injection — out=%02b", cycle, out);
                tb_failures++;
            end

            // Recovery → output must return to Pos
            set_input(0, 2'b01);
            @(posedge clk); #1;
            if (out !== 2'b01) begin
                $display("FAIL B cycle %0d: recovery — out=%02b", cycle, out);
                tb_failures++;
            end
        end
        t_end = $time;

        $display("Scenario B (hot toggle, 200 cycles): %s  |  %0d ns total, %0d ns/cycle (RTL sim)",
                 tb_failures == 0 ? "PASS" : "FAIL",
                 t_end - t_start, (t_end - t_start) / 200);
        // Criterion p50 comparison: ~3.68 µs/cycle (Rust software, with queue + dep-counter)
        // RTL: 1 clock cycle per toggle = 4 ns at 250 MHz

        // ── Scenario C: No-op ─────────────────────────────────────────────────
        // All inputs Pos. Re-drive same values 100 times.
        // ce_out must remain 0: the output register does not toggle.

        set_all(2'b01);
        @(posedge clk); #1;
        assert (out === 2'b01);

        t_start = $time;
        for (cycle = 0; cycle < 100; cycle++) begin
            set_all(2'b01);         // same values
            @(posedge clk); #1;
            if (ce_out !== 1'b0) begin
                $display("FAIL C round %0d: ce_out asserted on no-op (out=%02b)", cycle, out);
                tb_failures++;
            end
            if (out !== 2'b01) begin
                $display("FAIL C round %0d: output changed on no-op (out=%02b)", cycle, out);
                tb_failures++;
            end
        end
        t_end = $time;

        $display("Scenario C (no-op, 100 rounds): %s  |  ce_out=0 throughout  |  %0d ns total",
                 tb_failures == 0 ? "PASS" : "FAIL", t_end - t_start);
        // Criterion p50: ~90 ns total (same-value suppression, no downstream work)
        // RTL: ce=0 throughout → output register never toggles → zero dynamic power

        // ── Scenario D: Zero poison ───────────────────────────────────────────
        set_all(2'b01);
        set_input(250, 2'b10);    // middle input = Zero
        @(posedge clk); #1;
        if (out !== 2'b10) begin
            $display("FAIL D: Zero poison (input[250]) — out=%02b (expected Zero)", out);
            tb_failures++;
        end else begin
            $display("Scenario D (Zero poison, input[250]): PASS");
        end

        // ── Scenario E: Neg in last bitplane word (slot 498) ─────────────────
        // Regression: LAST_MASK in HLS and the SV bitplane reduction must
        // correctly detect Neg at the boundary of the last 64-slot word.
        set_all(2'b01);
        set_input(498, 2'b00);    // Neg at slot 498 (word 7, bit 50)
        @(posedge clk); #1;
        if (out !== 2'b00) begin
            $display("FAIL E: Neg at slot 498 not detected — out=%02b", out);
            tb_failures++;
        end else begin
            $display("Scenario E (Neg in slot 498, last word): PASS");
        end

        // ── Summary ───────────────────────────────────────────────────────────
        $display("");
        if (tb_failures == 0)
            $display("ALL PASS");
        else
            $display("FAILURES: %0d", tb_failures);

        $finish;
    end

    // Waveform dump (no-op without --trace; harmless to leave in)
    initial begin
        $dumpfile("tb_meetall.vcd");
        $dumpvars(0, tb_meetall);
    end

endmodule
