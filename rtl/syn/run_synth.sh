#!/usr/bin/env bash
# rtl/syn/run_synth.sh
#
# Run Yosys gate-level synthesis for ternary_cell and report key metrics.
#
# Usage (from pgress/ root):
#   chmod +x rtl/syn/run_synth.sh
#   ./rtl/syn/run_synth.sh
#
# Requires: yosys >= 0.9 (sudo apt install yosys on Ubuntu)
#
# Output files written to rtl/syn/:
#   stat_n500_meetall.txt  — cell/wire counts, N=500 MeetAll
#   abc_n500_meetall.txt   — LUT depth and LUT count after 6-LUT mapping
#   netlist_n500_meetall.v — synthesized gate netlist
#   stat_n4_meetall.txt    — same for N=4 (minimal cell for comparison)
#   stat_n500_joinany.txt  — same for N=500 JoinAny (should be symmetric)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if ! command -v yosys &>/dev/null; then
    echo "ERROR: yosys not found."
    echo "  Ubuntu/Debian: sudo apt install yosys"
    exit 1
fi

VER=$(yosys --version 2>&1 | head -1)
echo "=== Yosys synthesis: ternary_cell === ($VER)"
echo ""

mkdir -p "$SCRIPT_DIR"
cd "$REPO_ROOT"
yosys "$SCRIPT_DIR/synth_ternary_cell.ys" 2>&1 | grep -v "^$" | grep -E \
    "Executing|Number of|=== |cells:|wires:|DFF|LUT|abc|stat|Warning|Error|Level" \
    || true

echo ""
echo "=== Results ==="
echo ""

for f in stat_n500_meetall abc_n500_meetall stat_n4_meetall stat_n500_joinany; do
    if [[ -f "$SCRIPT_DIR/${f}.txt" ]]; then
        echo "── ${f}.txt ──────────────────────────────────────"
        cat "$SCRIPT_DIR/${f}.txt"
        echo ""
    fi
done
