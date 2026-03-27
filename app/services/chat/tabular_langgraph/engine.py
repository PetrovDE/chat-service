from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.services.chat.tabular_langgraph.graph_builder import build_tabular_langgraph
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


logger = logging.getLogger(__name__)
_COMPILED_GRAPH: Any = None


def _get_compiled_graph() -> Any:
    global _COMPILED_GRAPH
    if _COMPILED_GRAPH is None:
        _COMPILED_GRAPH = build_tabular_langgraph()
    return _COMPILED_GRAPH


async def execute_tabular_langgraph_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    graph = _get_compiled_graph()
    initial_state: TabularLangGraphState = {
        "query": str(query or ""),
        "files": list(files or []),
        "node_trace": [],
        "next_step": "detect_intent",
    }
    try:
        final_state = await graph.ainvoke(initial_state)
    except Exception:  # pragma: no cover - defensive runtime guard
        logger.exception("tabular_langgraph_execution_failed")
        return None
    if not isinstance(final_state, dict):
        return None
    payload = final_state.get("payload")
    return payload if isinstance(payload, dict) else None
