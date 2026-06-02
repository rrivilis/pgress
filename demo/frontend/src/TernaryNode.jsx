/**
 * Custom React Flow node for pgress ternary values.
 *
 * Input nodes   — show [T] [F] [?] toggle buttons to drive the graph.
 * Computed nodes — show rule label + current value; lazy nodes get a DEMAND button.
 *
 * Value encoding (matches the Python SDK and JSON wire):
 *   true  → Pos  (definite positive)
 *   false → Zero (contested / conflict)
 *   null  → Neg  (pending / not yet evaluated)
 */

import { Handle, Position } from 'reactflow';

// ── Ternary palette ───────────────────────────────────────────────────────────

const COLOR = {
  true:  '#22c55e',   // green-500   — True / Pos
  false: '#f97316',   // orange-500  — False / Zero (contested, not absent)
  null:  '#64748b',   // slate-500   — Pending / Neg
};

const LABEL = {
  true:  'True',
  false: 'False',  // contested
  null:  'Pending',
};

/** Stable key for a ternary value (handles the JS false/null ambiguity). */
function vk(v) {
  if (v === true)  return 'true';
  if (v === false) return 'false';
  return 'null';
}

// ── Component ─────────────────────────────────────────────────────────────────

const TOGGLE_BTNS = [
  { v: true,  label: 'T', title: 'Set True (Pos)' },
  { v: false, label: 'F', title: 'Set False — contested (Zero)' },
  { v: null,  label: '?', title: 'Clear to Pending (Neg)' },
];

export default function TernaryNode({ id, data }) {
  const { label, kind, rule, lazy, value, onSetValue, onDemand } = data;
  const key   = vk(value);
  const color = COLOR[key];
  const vLabel = LABEL[key];

  // Fire animation: brief scale-pop + expanding ring glow in the node's own color.
  // The CSS custom property --pc is read by the @keyframes node-fire rule in index.html.
  const fireStyle = data.pulse ? {
    '--pc':     color,
    animation:  'node-fire 0.55s ease-out both',
  } : {};

  return (
    <div style={{
      background:   '#1e293b',
      border:       `2px solid ${color}`,
      borderRadius: 10,
      padding:      '10px 14px',
      minWidth:     158,
      color:        '#f1f5f9',
      fontFamily:   'monospace',
      fontSize:     13,
      userSelect:   'none',
      transition:   'border-color 0.15s',
      ...fireStyle,
    }}>
      {/* Inbound edge handle */}
      <Handle type="target" position={Position.Left}
        style={{ background: '#475569', border: 'none' }} />

      {/* ── Header row ── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 7 }}>
        {/* Value indicator dot */}
        <span style={{
          width: 11, height: 11, borderRadius: '50%',
          background: color, flexShrink: 0, display: 'inline-block',
        }} />

        {/* Node label */}
        <span style={{ fontWeight: 700, flex: 1 }}>{label}</span>

        {/* Badges */}
        {kind === 'computed' && (
          <span style={{
            fontSize: 10, background: '#334155', borderRadius: 4,
            padding: '1px 5px', color: '#7dd3fc',
          }}>
            {rule}
          </span>
        )}
        {lazy && (
          <span style={{
            fontSize: 10, background: '#1d4ed8', borderRadius: 4,
            padding: '1px 5px', color: '#bfdbfe',
          }}>
            lazy
          </span>
        )}
      </div>

      {/* ── Value display ── */}
      <div style={{ fontSize: 12, color, fontWeight: 600, marginBottom: 9 }}>
        {vLabel}
      </div>

      {/* ── Input toggles ── */}
      {kind === 'input' && (
        <div style={{ display: 'flex', gap: 4 }}>
          {TOGGLE_BTNS.map(({ v, label: btnLabel, title }) => {
            const active = vk(v) === key;
            return (
              <button
                key={btnLabel}
                title={title}
                onClick={() => onSetValue(id, v)}
                style={{
                  flex:         1,
                  padding:      '4px 0',
                  fontSize:     12,
                  fontWeight:   700,
                  border:       'none',
                  borderRadius: 5,
                  cursor:       'pointer',
                  background:   active ? COLOR[vk(v)] : '#334155',
                  color:        '#f1f5f9',
                  opacity:      active ? 1 : 0.7,
                  transition:   'background 0.12s',
                }}
              >
                {btnLabel}
              </button>
            );
          })}
        </div>
      )}

      {/* ── Demand button (lazy computed only) ── */}
      {kind === 'computed' && lazy && (
        <button
          title="Force evaluation of this lazy node"
          onClick={() => onDemand(id)}
          style={{
            width:        '100%',
            padding:      '4px 0',
            fontSize:     12,
            fontWeight:   700,
            border:       '1px solid #3b82f6',
            borderRadius: 5,
            cursor:       'pointer',
            background:   'transparent',
            color:        '#93c5fd',
            letterSpacing: 1,
          }}
        >
          DEMAND
        </button>
      )}

      {/* Outbound edge handle */}
      <Handle type="source" position={Position.Right}
        style={{ background: '#475569', border: 'none' }} />
    </div>
  );
}
