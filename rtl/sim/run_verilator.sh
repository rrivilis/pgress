#!/usr/bin/env bash
# rtl/sim/run_verilator.sh
#
# Compile meetall_500 + tb_meetall with Verilator and run simulation.
#
# Requirements:
#   - Verilator >= 5.0  (for --timing support; needed for #delay and @(posedge) in
#                        initial blocks within the pure-SV testbench)
#   - g++ or clang++
#
# On Windows: run via WSL or Git Bash with Verilator installed in the Linux env.
#
# Usage:
#   chmod +x rtl/sim/run_verilator.sh
#   ./rtl/sim/run_verilator.sh              # run from pgress/ root
#   ./rtl/sim/run_verilator.sh --trace      # also emit tb_meetall.vcd waveform
#
# Expected output:
#   Scenario A (convergence, 500 inputs): PASS
#   Scenario B (hot toggle, 200 cycles):  PASS  |  ... ns total, 4 ns/cycle
#   Scenario C (no-op, 100 rounds):       PASS  |  ce_out=0 throughout
#   Scenario D (Zero poison, input[250]): PASS
#   Scenario E (Neg in slot 498):         PASS
#   ALL PASS

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SV_DIR="$REPO_ROOT/rtl/sv"
OUT_DIR="$SCRIPT_DIR/build"

mkdir -p "$OUT_DIR"

# ── Check Verilator version ───────────────────────────────────────────────────
if ! command -v verilator &>/dev/null; then
    echo "ERROR: verilator not found. Install Verilator >= 5.0."
    echo "  Ubuntu/Debian: sudo apt install verilator"
    echo "  macOS:         brew install verilator"
    exit 1
fi

VER=$(verilator --version 2>&1 | awk '/Verilator/{print $2}')
MAJOR=$(echo "$VER" | cut -d. -f1)
if [ "$MAJOR" -lt 5 ]; then
    echo "WARNING: Verilator $VER detected; >= 5.0 required for --timing."
    echo "  Upgrade: https://verilator.org/guide/latest/install.html"
    echo "  Continuing — simulation may fail if --timing is unsupported."
fi

# ── Build flags ───────────────────────────────────────────────────────────────
TRACE_FLAGS=""
if [[ "${1:-}" == "--trace" ]]; then
    TRACE_FLAGS="--trace"
    echo "Waveform tracing enabled: tb_meetall.vcd will be written."
fi

# ── Compile ───────────────────────────────────────────────────────────────────
echo "=== Compiling with Verilator $VER ==="

verilator \
    --cc \
    --exe \
    --main \
    --build \
    --timing \
    -Wno-WIDTHTRUNC \
    -Wno-UNUSEDSIGNAL \
    --top-module tb_meetall \
    --Mdir "$OUT_DIR" \
    $TRACE_FLAGS \
    "$SV_DIR/ternary_cell.sv" \
    "$SV_DIR/meetall_500.sv" \
    "$SV_DIR/tb_meetall.sv"

echo ""
echo "=== Running simulation ==="
"$OUT_DIR/Vtb_meetall"
