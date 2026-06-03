#!/usr/bin/env bash
# rtl/spice/run_spice.sh
#
# Run ngspice characterization for T_CLASS, T_DFF, T_ZERO against sky130.
#
# Requirements:
#   - ngspice >= 36  (sudo apt install ngspice)
#   - sky130 PDK SPICE models, one of:
#       pip install sky130   (sets SKY130_PDK env or provides path via Python)
#       volare               ($HOME/.volare/sky130A/...)
#       open_pdks install    (/usr/local/share/pdk/sky130A/...)
#
# Usage:
#   ./rtl/spice/run_spice.sh [t_class | t_dff | t_zero | all]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Locate sky130 SPICE models ────────────────────────────────────────────────
find_pdk() {
    local candidates=(
        "${PDK_ROOT:-}"
        "${HOME}/.volare/sky130A/libs.ref/sky130_fd_sc_hd/spice"
        "/usr/local/share/pdk/sky130A/libs.ref/sky130_fd_sc_hd/spice"
        "/usr/share/pdk/sky130A/libs.ref/sky130_fd_sc_hd/spice"
    )
    # Try pip-installed sky130
    local pip_path
    pip_path=$(python3 -c "import sky130; import os; \
        print(os.path.join(sky130.PDK_ROOT,'sky130A','libs.ref','sky130_fd_sc_hd','spice'))" \
        2>/dev/null || true)
    [[ -n "$pip_path" ]] && candidates+=("$pip_path")

    for c in "${candidates[@]}"; do
        local f="$c/sky130_fd_sc_hd.spice"
        if [[ -f "$f" ]]; then
            echo "$c"
            return 0
        fi
    done
    return 1
}

PDK_SPICE_DIR=$(find_pdk) || {
    echo "ERROR: sky130_fd_sc_hd.spice not found."
    echo "  Install via:  pip3 install sky130"
    echo "  or volare:    pip3 install volare && volare enable sky130 \$(volare ls-remote sky130 | head -1)"
    echo "  or open_pdks: https://github.com/RTimothyEdwards/open_pdks"
    exit 1
}
echo "PDK: $PDK_SPICE_DIR"

# ── Check ngspice ─────────────────────────────────────────────────────────────
if ! command -v ngspice &>/dev/null; then
    echo "ERROR: ngspice not found. sudo apt install ngspice"
    exit 1
fi
echo "ngspice: $(ngspice --version 2>&1 | head -1)"

# ── Run a deck ────────────────────────────────────────────────────────────────
run_deck() {
    local name=$1
    local deck="$SCRIPT_DIR/${name}.spice"
    local out="$SCRIPT_DIR/${name}.out"
    echo ""
    echo "=== Running $name ==="
    # Inject PDK path by preprocessing the .include line
    sed "s|\$PDK_ROOT|${PDK_SPICE_DIR}|g" "$deck" > /tmp/${name}_resolved.spice
    ngspice -b -o "$out" /tmp/${name}_resolved.spice 2>&1 | \
        grep -E "tpd_|power_|ratio_|isolation_|ce_eff_|measure|Error|Warning" || true
    echo "Full output: $out"
}

TARGET="${1:-all}"
case "$TARGET" in
    t_class) run_deck t_class ;;
    t_dff)   run_deck t_dff ;;
    t_zero)  run_deck t_zero ;;
    all)
        run_deck t_class
        run_deck t_dff
        run_deck t_zero
        ;;
    *) echo "Usage: $0 [t_class | t_dff | t_zero | all]"; exit 1 ;;
esac

echo ""
echo "=== Done ==="
