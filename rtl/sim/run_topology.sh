#!/usr/bin/env bash
# rtl/sim/run_topology.sh
#
# Compile ternary_chain + ternary_tree + tb_topology with Verilator and run.
#
# Usage:
#   chmod +x rtl/sim/run_topology.sh
#   ./rtl/sim/run_topology.sh          # from pgress/ root
#   ./rtl/sim/run_topology.sh --trace  # also emit tb_topology.vcd
#
# Expected output:
#   TC1 (chain Zero propagation, K=8): PASS  |  8 cycles  |  32 ns
#   TC2 (chain Zero recovery,    K=8): PASS  |  8 cycles  |  32 ns
#   TC3 (chain no-op, 20 rounds):      PASS  |  ce=0 throughout
#   TT1 (tree all-Neg->all-Pos, depth=4): PASS  |  3 cycles  |  12 ns
#   TT2 (tree single-leaf Zero,  depth=4): PASS  |  3 cycles  |  12 ns
#   TT2b (tree single-leaf recovery):  PASS  |  3 cycles  |  12 ns
#   TT3 (tree no-op, 20 rounds):       PASS  |  ce=0 throughout
#   ALL PASS

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SV_DIR="$REPO_ROOT/rtl/sv"
OUT_DIR="$HOME/pgress_topo_sim/build"

mkdir -p "$OUT_DIR"

if ! command -v verilator &>/dev/null; then
    echo "ERROR: verilator not found. Install Verilator >= 5.0."
    exit 1
fi

VER=$(verilator --version 2>&1 | awk '/Verilator/{print $2}')
MAJOR=$(echo "$VER" | cut -d. -f1)
if [ "$MAJOR" -lt 5 ]; then
    echo "WARNING: Verilator $VER detected; >= 5.0 required for --timing."
fi

TRACE_FLAGS=""
if [[ "${1:-}" == "--trace" ]]; then
    TRACE_FLAGS="--trace"
    echo "Waveform tracing enabled: tb_topology.vcd will be written."
fi

echo "=== Compiling topology bench with Verilator $VER ==="

verilator \
    --cc \
    --exe \
    --main \
    --build \
    --timing \
    -Wno-WIDTHTRUNC \
    -Wno-UNUSEDSIGNAL \
    -Wno-WIDTHEXPAND \
    --top-module tb_topology \
    --Mdir "$OUT_DIR" \
    $TRACE_FLAGS \
    "$SV_DIR/ternary_cell.sv" \
    "$SV_DIR/ternary_chain.sv" \
    "$SV_DIR/ternary_tree.sv" \
    "$SV_DIR/tb_topology.sv"

echo ""
echo "=== Running topology benchmark ==="
"$OUT_DIR/Vtb_topology"
