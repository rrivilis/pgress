# rtl/hls/directives.tcl
#
# Vitis HLS project setup and synthesis directives for meetall_500.
#
# Usage:
#   vitis_hls -f directives.tcl
#   (Vivado HLS 2019.2+ also works: vivado_hls -f directives.tcl)
#
# Run from the rtl/hls/ directory so relative file paths resolve correctly.
#
# Phases:
#   1. csim_design  — C simulation against tb_meetall.cpp (no license needed)
#   2. csynth_design — RTL generation + resource/timing estimates
#   3. cosim_design  — RTL co-simulation vs C++ reference (Vivado license needed;
#                      commented out by default)
#
# Part selection:
#   ZCU104  (Zynq UltraScale+):  xczu7ev-ffvc1156-2-e   (PS+PL split; recommended)
#   Arty A7-100T:                xc7a100tcsg324-1        (low-cost dev board)
#   Change set_part to match your board.

open_project meetall_500_proj
set_top meetall_500

add_files ternary_meetall.h
add_files ternary_meetall.cpp
add_files -tb tb_meetall.cpp

open_solution "solution1" -flow_target vivado
set_part {xczu7ev-ffvc1156-2-e}

# 250 MHz target (4 ns period).
# Adjust if targeting Arty A7 (100–200 MHz is more typical for 7-series).
create_clock -period 4 -name default

# ── Synthesis directives ─────────────────────────────────────────────────────
# These mirror the #pragma HLS directives in ternary_meetall.cpp; listed here
# as a reference and to override from the TCL flow if needed.

# Pipeline the top function with initiation interval = 1.
set_directive_pipeline -II 1 "meetall_500"

# Partition both input arrays completely (8 elements each) so all words are
# accessed in parallel within the single-cycle pipeline body.
set_directive_array_partition -type complete -dim 1 "meetall_500" plane_p0
set_directive_array_partition -type complete -dim 1 "meetall_500" plane_p1

# ── Run phases ───────────────────────────────────────────────────────────────

# Phase 1: C simulation. Validates testbench against the C++ reference model.
# Expected output: ALL PASS (0 total failures). No license required.
csim_design

# Phase 2: RTL synthesis. Generates Verilog/VHDL and resource/timing estimates.
# Check meetall_500_proj/solution1/syn/report/meetall_500_csynth.rpt for:
#   - Estimated clock period (should meet 4 ns)
#   - LUT / FF / DSP utilization
#   - Latency: 0 cycles (II=1 fully pipelined, combinational output)
csynth_design

# Phase 3: RTL co-simulation vs C++ golden reference.
# Requires Vivado simulator license. Uncomment when available.
# cosim_design -rtl verilog -trace_level all

# Phase 4: Export as IP catalog for Vivado block design integration (optional).
# export_design -format ip_catalog -description "pgress ternary MeetAll N=500"

close_project
