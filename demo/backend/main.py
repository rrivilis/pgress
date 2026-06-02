"""
pgress demo backend — FastAPI.

Three endpoints:

  GET  /graph    → React Flow-compatible node/edge snapshot (polled at 200ms)
  GET  /metrics  → node/edge counts, ops applied, scenario list
  POST /op       → drive the graph: set_value | demand | load_scenario

Run:
    cd demo/backend
    uvicorn main:app --reload --port 8000
"""

from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from engine import engine

app = FastAPI(title="pgress demo", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/graph")
def get_graph():
    """
    Return a React Flow-compatible snapshot of the current graph state.

    Each node carries:
      data.value  — True | False | null  (ternary: Pos / Zero / Neg)
      data.kind   — "input" | "computed"
      data.rule   — compute rule name, null for inputs
      data.lazy   — bool
    """
    return engine.get_graph_state()


@app.get("/metrics")
def get_metrics():
    """
    Graph-level counters and the ordered scenario list (for tab rendering).
    """
    return engine.get_metrics()


class Op(BaseModel):
    type:     str               # "set_value" | "demand" | "load_scenario"
    node:     Optional[str] = None
    value:    Optional[Any] = None   # true / false / null  (ternary)
    scenario: Optional[str] = None


@app.post("/op")
def post_op(op: Op):
    """
    Drive the graph or switch scenarios.

    set_value:
        {"type": "set_value", "node": "price", "value": true}
        {"type": "set_value", "node": "price", "value": false}   // contested
        {"type": "set_value", "node": "price", "value": null}    // clear

    demand:
        {"type": "demand", "node": "summary"}

    load_scenario:
        {"type": "load_scenario", "scenario": "bochvar_chain"}
    """
    try:
        match op.type:
            case "set_value":
                if op.node is None:
                    raise HTTPException(400, "node required")
                engine.set_value(op.node, op.value)
            case "demand":
                if op.node is None:
                    raise HTTPException(400, "node required")
                engine.demand(op.node)
            case "load_scenario":
                if op.scenario is None:
                    raise HTTPException(400, "scenario required")
                engine.load_scenario(op.scenario)
            case _:
                raise HTTPException(400, f"unknown op type {op.type!r}")
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc

    return {"ok": True}
