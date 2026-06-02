"""
pygress — self-adjusting reactive computation graph.

Quick start::

    import pygress

    g = pygress.Graph()

    # inputs
    price   = g.input("price")
    volume  = g.input("volume")

    # derived signal: both price AND volume must be True
    signal  = g.computed("signal", rule="and")
    g.connect(price,  signal)
    g.connect(volume, signal)

    # drive inputs
    g.set(price,  True)
    g.set(volume, True)

    print(g.get(signal))   # True

    g.set(volume, False)
    print(g.get(signal))   # False

Lazy evaluation::

    summary = g.computed("summary", rule="identity", lazy=True)
    g.connect(signal, summary)

    g.set(price, True)
    # summary has NOT recomputed yet

    print(g.demand(summary))  # True  — evaluated on demand

Values
------
``True``   — positive (signal present / condition holds)
``False``  — conflict (two inputs disagree; contested state)
``None``   — pending (not yet evaluated; default for new nodes)

The three-valued algebra is Łukasiewicz L₃. ``False`` here means *contested*,
not *absent* — both ``True`` and ``False`` are definite states; ``None`` is the
indeterminate / not-yet-computed state.

Conflict semantics (Bochvar)
---------------------------
The engine applies Bochvar strict three-valued logic at the propagation layer:
if **any** upstream input is ``False`` (contested), the output is forced to
``False`` regardless of the computation rule.  If any input is ``None``
(pending) and none are ``False``, propagation is inhibited — the node keeps
its current value.  A rule fires (via ``meet_all`` / ``join_any`` / etc.) only
when **all** upstream inputs are ``True``.

Practical consequence: for binary ``"and"`` and ``"or"`` nodes you will only
observe ``True`` output when every predecessor is ``True``.  Use ``"or"`` for
semantics where *any* positive signal should dominate once all conflict is
resolved; use ``"and"`` for conjunctive gates.

Computation rules
-----------------
- ``"and"`` / ``"all"``      — meet (min) over all inputs; variable arity
- ``"or"``  / ``"any"``      — join (max) over all inputs; variable arity
- ``"not"`` / ``"neg"``      — negate a single input (True→None, None→True, False→False)
- ``"majority"``             — positive majority vote (>½ inputs True → True)
- ``"fold"`` / ``"bochvar"`` — explicit Bochvar fold: any False input → False
- ``"identity"`` / ``"id"``  — pass single input through unchanged
"""

from pygress._pygress import Graph, NodeHandle

__all__ = ["Graph", "NodeHandle"]
