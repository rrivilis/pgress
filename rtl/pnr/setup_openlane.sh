#!/usr/bin/env bash
# setup_openlane.sh — stage pgress ternary designs into OpenLane.
#
# Run from WSL *outside* the container (not from inside `make mount`).
# Copies RTL from rtl/sv/ and PnR-specific overrides from rtl/pnr/*/src/
# into ~/OpenLane/designs/. Override files take precedence over common RTL.
#
# Usage:
#   bash rtl/pnr/setup_openlane.sh [OPENLANE_ROOT]
#   OPENLANE_ROOT defaults to ~/OpenLane

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RTL_SV="$SCRIPT_DIR/../sv"
OPENLANE_ROOT="${1:-$HOME/OpenLane}"
DESIGNS="$OPENLANE_ROOT/designs"

if [ ! -d "$OPENLANE_ROOT" ]; then
    echo "ERROR: OpenLane root not found at $OPENLANE_ROOT"
    echo "Pass the correct path as the first argument."
    exit 1
fi

echo "pgress RTL:     $RTL_SV"
echo "OpenLane root:  $OPENLANE_ROOT"
echo ""

# Common RTL files each design may need (copied only if not already overridden)
COMMON_SV=(
    ternary_cell.sv
    ternary_chain.sv
    ternary_region.sv
    icg_model.sv
    meetall_500.sv
)

for design in ternary_cell ternary_region meetall_500; do
    pnr_dir="$SCRIPT_DIR/$design"
    dest_dir="$DESIGNS/$design"
    src_dir="$dest_dir/src"

    echo "── $design ──────────────────────────────────────"
    mkdir -p "$src_dir"

    # 1. Config
    cp "$pnr_dir/config.json" "$dest_dir/config.json"
    echo "  config.json"

    # 2. PnR-specific src overrides (these win over common RTL)
    if [ -d "$pnr_dir/src" ]; then
        for f in "$pnr_dir/src/"*.sv; do
            [ -f "$f" ] || continue
            cp "$f" "$src_dir/$(basename "$f")"
            echo "  src/$(basename "$f")  [pnr override]"
        done
    fi

    # 3. Common RTL — only if not already present (preserves overrides)
    for sv in "${COMMON_SV[@]}"; do
        if [ -f "$RTL_SV/$sv" ] && [ ! -f "$src_dir/$sv" ]; then
            cp "$RTL_SV/$sv" "$src_dir/$sv"
            echo "  src/$sv"
        fi
    done
done

echo ""
echo "Done. Enter the container and run the flows:"
echo "  cd $OPENLANE_ROOT && make mount"
echo "  ./flow.tcl -design ternary_cell   -tag run1   # N=8  MeetAll cell"
echo "  ./flow.tcl -design ternary_region -tag run1   # K=8  ICG region (headline)"
echo "  ./flow.tcl -design meetall_500    -tag run1   # N=500 flat MeetAll"
