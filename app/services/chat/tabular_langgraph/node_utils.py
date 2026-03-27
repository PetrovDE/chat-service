from __future__ import annotations

from typing import Any, Dict, List

from app.services.chat.tabular_langgraph.state import TabularLangGraphState


GRAPH_VERSION = "tabular_langgraph_v1"


def append_trace(state: TabularLangGraphState, *, node: str, status: str, reason: str = "none") -> List[Dict[str, Any]]:
    trace = list(state.get("node_trace") or [])
    trace.append({"node": node, "status": status, "reason": reason})
    return trace[-24:]
