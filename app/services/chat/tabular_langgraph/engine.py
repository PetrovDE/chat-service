from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.services.chat.tabular_langgraph.graph_builder import build_tabular_langgraph
from app.services.chat.tabular_langgraph.node_utils import GRAPH_VERSION
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


logger = logging.getLogger(__name__)
_COMPILED_GRAPH: Any = None


def _get_compiled_graph() -> Any:
    global _COMPILED_GRAPH
    if _COMPILED_GRAPH is None:
        _COMPILED_GRAPH = build_tabular_langgraph()
    return _COMPILED_GRAPH


def _build_invoke_config(*, graph_run_id: str) -> Dict[str, Any]:
    if not bool(getattr(settings, "LANGSMITH_TRACING_ENABLED", False)):
        return {}
    tags = [
        tag.strip()
        for tag in str(getattr(settings, "LANGSMITH_TAGS", "tabular-langgraph") or "").split(",")
        if tag.strip()
    ]
    metadata = {
        "service": "llama-service",
        "graph_version": GRAPH_VERSION,
        "graph_run_id": graph_run_id,
        "project": str(getattr(settings, "LANGSMITH_PROJECT", "llama-service") or "llama-service"),
    }
    return {
        "run_name": "tabular_langgraph",
        "tags": tags or ["tabular-langgraph"],
        "metadata": metadata,
    }


async def execute_tabular_langgraph_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    graph = _get_compiled_graph()
    graph_run_id = str(uuid.uuid4())
    initial_state: TabularLangGraphState = {
        "query": str(query or ""),
        "files": list(files or []),
        "graph_run_id": graph_run_id,
        "graph_started_at_ms": int(time.time() * 1000),
        "graph_stop_reason": "not_started",
        "node_trace": [],
        "next_step": "detect_intent",
    }
    try:
        invoke_config = _build_invoke_config(graph_run_id=graph_run_id)
        if invoke_config:
            final_state = await graph.ainvoke(initial_state, config=invoke_config)
        else:
            final_state = await graph.ainvoke(initial_state)
    except Exception:  # pragma: no cover - defensive runtime guard
        logger.exception("tabular_langgraph_execution_failed graph_run_id=%s", graph_run_id)
        return None
    if not isinstance(final_state, dict):
        return None
    payload = final_state.get("payload")
    return payload if isinstance(payload, dict) else None
