"""Shared audit computation for the sync endpoint and async job workers.

The synchronous endpoint (``POST /api/audit``) and the asynchronous job
runner must return exactly the same result document for the same payload,
so the document is built in exactly one place: here.
"""

from __future__ import annotations

from typing import Any

from .solver import build_adjacency, solve
from .validation import parse_problem


def solve_payload(payload: Any) -> dict[str, Any]:
    """Validate ``payload`` and return the audit result document.

    Raises :class:`ValidationError` (HTTP 400 class) for malformed input and
    :class:`TopologyError` (HTTP 422 class) when the endpoints cannot be
    joined; callers map those to their transport-specific error shape.
    """
    problem = parse_problem(payload)
    cost, selected, edge_ids = solve(problem)
    return {
        "cost": cost,
        "edge_set": list(edge_ids),
        "edges": [
            {
                "id": e.id,
                "source": problem.nodes[e.source],
                "target": problem.nodes[e.target],
                "cost": e.cost,
            }
            for e in sorted(selected, key=lambda e: e.id)
        ],
        "adjacency": build_adjacency(problem.nodes, selected),
    }
