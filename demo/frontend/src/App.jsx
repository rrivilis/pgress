/**
 * pgress demo — root component.
 *
 * Layout:
 *   [top bar: scenario tabs | metrics]
 *   [React Flow canvas — full remaining height]
 *     [propagation panel — absolute overlay, top-right]
 *   [bottom legend]
 *
 * Polling: GET /graph + GET /metrics every 200ms.
 * Ops:     POST /op  on button clicks inside TernaryNode.
 * Pulse:   nodes that changed value flash; active edges animate.
 */

import { useEffect, useState, useCallback, useMemo, useRef } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap, MarkerType,
} from 'reactflow';
import 'reactflow/dist/style.css';
import TernaryNode from './TernaryNode';

// ── Constants ─────────────────────────────────────────────────────────────────

const NODE_TYPES = { ternary: TernaryNode };

const POLL_MS    = 200;
const PULSE_MS   = 550;   // how long the fire animation lasts

// Human-readable scenario names (ordered to match backend registry).
const SCENARIO_LABELS = {
  and_gate:      'AND Gate',
  bochvar_chain: 'Bochvar Chain',
  lazy_demand:   'Lazy Demand',
  or_gate:       'OR Gate',
  not_gate:      'NOT Gate',
  majority:      'Majority Vote',
};

// Short descriptions shown as hover tooltips on each scenario tab.
const SCENARIO_NOTES = {
  and_gate:
    'price AND volume → signal. Both must be True; any False (contested) infects the output.',
  bochvar_chain:
    'Three inputs through a Bochvar fold. Any False is infectious — even one contested input forces all downstream nodes to False.',
  lazy_demand:
    'signal is eager; summary is lazy. Summary stays Pending until you press DEMAND, even after signal resolves.',
  or_gate:
    'Either source True → alert True. Both must be Pending for the output to stay Pending.',
  not_gate:
    'L₃ negation: True→Pending, False→False (fixed point).\n\n' +
    'Inhibition note: if you set the input to Pending (?) after True, the output does NOT change to True — ' +
    'the engine suppresses re-evaluation when a dependency goes Pending. ' +
    'Set to False to see the False→False fixed point.',
  majority:
    'Three-input majority vote. Two or more True → True. Tie (one True, one False, one Pending) → contested.',
};

const EDGE_BASE = { stroke: '#475569', strokeWidth: 2 };
const EDGE_LIVE = { stroke: '#38bdf8', strokeWidth: 2.5 };

// ── Helpers ───────────────────────────────────────────────────────────────────

async function postOp(body) {
  await fetch('/op', {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(body),
  });
}

// ── PropPanel ─────────────────────────────────────────────────────────────────

function PropPanel({ metrics }) {
  if (!metrics) return null;

  const rows = [
    { label: 'last op',     value: metrics.last_op,               mono: true  },
    { label: 'propagations', value: metrics.propagation_count,    mono: false },
    { label: 'suppressed',  value: metrics.recomputes_suppressed, mono: false },
    { label: 'frontier',    value: `${metrics.frontier_size} node${metrics.frontier_size === 1 ? '' : 's'}`, mono: false },
    { label: 'total ops',   value: metrics.ops_applied,           mono: false },
  ];

  return (
    <div style={{
      position:     'absolute',
      top:          12,
      right:        12,
      zIndex:       10,
      background:   'rgba(15, 23, 42, 0.88)',
      border:       '1px solid #1e3a5f',
      borderRadius: 8,
      padding:      '10px 14px',
      fontFamily:   'monospace',
      fontSize:     11,
      color:        '#64748b',
      minWidth:     200,
      backdropFilter: 'blur(4px)',
      pointerEvents: 'none',   // don't steal canvas interactions
    }}>
      <div style={{
        color: '#38bdf8', fontWeight: 700, fontSize: 10,
        letterSpacing: 1, marginBottom: 8, textTransform: 'uppercase',
      }}>
        propagation metrics
      </div>
      {rows.map(({ label, value, mono }) => (
        <div key={label} style={{
          display: 'flex', justifyContent: 'space-between',
          gap: 16, marginBottom: 4,
        }}>
          <span style={{ color: '#475569' }}>{label}</span>
          <span style={{
            color:      '#94a3b8',
            fontFamily: mono ? 'monospace' : undefined,
            textAlign:  'right',
            maxWidth:   120,
            overflow:   'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}>
            {value}
          </span>
        </div>
      ))}
    </div>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────

export default function App() {
  const [graphData,   setGraphData]   = useState(null);
  const [metrics,     setMetrics]     = useState(null);
  const [error,       setError]       = useState(null);
  const [pulsedNodes, setPulsedNodes] = useState(() => new Set());

  const prevGraphRef  = useRef(null);   // previous graph snapshot for diffing
  const pulseTimerRef = useRef(null);   // outstanding clear-pulse timeout

  // ── Stable callbacks ──────────────────────────────────────────────────────

  const handleSetValue = useCallback((nodeId, value) => {
    postOp({ type: 'set_value', node: nodeId, value });
  }, []);

  const handleDemand = useCallback((nodeId) => {
    postOp({ type: 'demand', node: nodeId });
  }, []);

  const handleLoadScenario = useCallback((scenario) => {
    postOp({ type: 'load_scenario', scenario });
  }, []);

  // ── Polling + pulse detection ─────────────────────────────────────────────

  useEffect(() => {
    let alive = true;

    async function poll() {
      try {
        const [gr, mr] = await Promise.all([
          fetch('/graph').then(r => r.json()),
          fetch('/metrics').then(r => r.json()),
        ]);
        if (!alive) return;

        // Diff against previous snapshot to find changed nodes.
        if (prevGraphRef.current) {
          const prevMap = new Map(
            prevGraphRef.current.nodes.map(n => [n.id, n.data.value])
          );
          const changed = new Set(
            gr.nodes
              .filter(n => prevMap.get(n.id) !== n.data.value)
              .map(n => n.id)
          );
          if (changed.size > 0) {
            if (pulseTimerRef.current) clearTimeout(pulseTimerRef.current);
            setPulsedNodes(changed);
            pulseTimerRef.current = setTimeout(() => {
              setPulsedNodes(new Set());
              pulseTimerRef.current = null;
            }, PULSE_MS);
          }
        }

        prevGraphRef.current = gr;
        setGraphData(gr);
        setMetrics(mr);
        setError(null);
      } catch (e) {
        if (alive) setError(e.message);
      }
    }

    poll();
    const id = setInterval(poll, POLL_MS);
    return () => {
      alive = false;
      clearInterval(id);
      if (pulseTimerRef.current) clearTimeout(pulseTimerRef.current);
    };
  }, []);

  // ── Build React Flow arrays ───────────────────────────────────────────────

  const rfNodes = useMemo(() => {
    if (!graphData) return [];
    return graphData.nodes.map(n => ({
      ...n,
      data: {
        ...n.data,
        onSetValue: handleSetValue,
        onDemand:   handleDemand,
        pulse:      pulsedNodes.has(n.id),
      },
    }));
  }, [graphData, handleSetValue, handleDemand, pulsedNodes]);

  // Edges whose *source* just fired get the animated treatment.
  const rfEdges = useMemo(() => {
    if (!graphData) return [];
    return graphData.edges.map(e => {
      const live = pulsedNodes.has(e.source);
      return {
        ...e,
        animated: live,
        style:     live ? EDGE_LIVE : EDGE_BASE,
        markerEnd: {
          type:  MarkerType.ArrowClosed,
          color: live ? '#38bdf8' : '#475569',
        },
      };
    });
  }, [graphData, pulsedNodes]);

  const currentScenario = graphData?.scenario ?? '';
  const allScenarios    = metrics?.scenarios  ?? [];

  // ── Render ────────────────────────────────────────────────────────────────

  return (
    <div style={{
      height:        '100vh',
      display:       'flex',
      flexDirection: 'column',
      background:    '#0f172a',
      color:         '#f1f5f9',
    }}>

      {/* ── Top bar ── */}
      <div style={{
        display:      'flex',
        alignItems:   'center',
        gap:          10,
        padding:      '8px 16px',
        background:   '#1e293b',
        borderBottom: '1px solid #334155',
        flexShrink:   0,
        flexWrap:     'wrap',
      }}>
        {/* Logo */}
        <span style={{
          fontWeight: 800, fontSize: 15, color: '#7dd3fc', letterSpacing: 1,
        }}>
          pgress
        </span>
        <span style={{ color: '#334155' }}>|</span>

        {/* Scenario tabs */}
        {allScenarios.map(s => (
          <button
            key={s}
            title={SCENARIO_NOTES[s]}
            onClick={() => handleLoadScenario(s)}
            style={{
              padding:      '4px 12px',
              borderRadius: 6,
              border:       'none',
              cursor:       'pointer',
              fontFamily:   'monospace',
              fontSize:     12,
              background:   currentScenario === s ? '#3b82f6' : '#334155',
              color:        currentScenario === s ? '#fff'    : '#94a3b8',
              fontWeight:   currentScenario === s ? 700       : 400,
              transition:   'background 0.12s',
            }}
          >
            {SCENARIO_LABELS[s] ?? s}
          </button>
        ))}

        {/* Spacer */}
        <div style={{ flex: 1 }} />

        {/* Compact top-bar metrics */}
        {metrics && (
          <div style={{
            fontFamily: 'monospace', fontSize: 12,
            color: '#64748b', display: 'flex', gap: 18,
          }}>
            <span>nodes <b style={{ color: '#94a3b8' }}>{metrics.node_count}</b></span>
            <span>edges <b style={{ color: '#94a3b8' }}>{metrics.edge_count}</b></span>
            <span>ops   <b style={{ color: '#94a3b8' }}>{metrics.ops_applied}</b></span>
          </div>
        )}

        {/* Error indicator */}
        {error && (
          <span style={{ color: '#ef4444', fontSize: 12 }}>⚠ {error}</span>
        )}
      </div>

      {/* ── Graph canvas + overlay panel ── */}
      <div style={{ flex: 1, position: 'relative' }}>
        {graphData ? (
          <ReactFlow
            nodes={rfNodes}
            edges={rfEdges}
            nodeTypes={NODE_TYPES}
            fitView
            fitViewOptions={{ padding: 0.35 }}
            nodesDraggable={false}
            nodesConnectable={false}
            deleteKeyCode={null}
          >
            <Background color="#1e3a5f" gap={28} size={1} />
            <Controls
              style={{ background: '#1e293b', border: '1px solid #334155' }}
            />
            <MiniMap
              nodeColor={n => {
                const v = n.data?.value;
                if (v === true)  return '#22c55e';
                if (v === false) return '#f97316';
                return '#64748b';
              }}
              style={{ background: '#1e293b', border: '1px solid #334155' }}
            />
          </ReactFlow>
        ) : (
          <div style={{
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            height: '100%', color: '#475569', fontFamily: 'monospace',
          }}>
            connecting to backend…
          </div>
        )}

        {/* Propagation metrics overlay */}
        <PropPanel metrics={metrics} />
      </div>

      {/* ── Bottom legend ── */}
      <div style={{
        display:    'flex',
        alignItems: 'center',
        gap:        20,
        padding:    '6px 16px',
        background: '#1e293b',
        borderTop:  '1px solid #334155',
        flexShrink: 0,
        fontFamily: 'monospace',
        fontSize:   11,
        color:      '#64748b',
      }}>
        <span>ternary values:</span>
        {[
          { color: '#22c55e', label: 'True  (Pos)'              },
          { color: '#f97316', label: 'False (Zero / contested)' },
          { color: '#64748b', label: 'Pending (Neg)'            },
        ].map(({ color, label }) => (
          <span key={label} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
            <span style={{
              width: 9, height: 9, borderRadius: '50%',
              background: color, display: 'inline-block', flexShrink: 0,
            }} />
            {label}
          </span>
        ))}
        <div style={{ flex: 1 }} />
        <span style={{ color: '#334155' }}>polling {POLL_MS}ms · L₃ Łukasiewicz</span>
      </div>
    </div>
  );
}
