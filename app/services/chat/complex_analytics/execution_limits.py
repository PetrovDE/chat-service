from __future__ import annotations

from typing import Any, Dict, Optional

from app.core.config import settings

from .report_quality import is_broad_full_analysis_query


def resolve_max_artifacts_limit(
    *,
    query: str,
    codegen_meta: Optional[Dict[str, Any]],
    primary_frame: Any,
) -> int:
    base_limit = int(getattr(settings, "COMPLEX_ANALYTICS_MAX_ARTIFACTS", 16) or 16)
    hard_cap = int(getattr(settings, "COMPLEX_ANALYTICS_MAX_ARTIFACTS_HARD_CAP", 48) or 48)
    effective_limit = base_limit

    contract = (
        dict((codegen_meta or {}).get("plan_contract"))
        if isinstance((codegen_meta or {}).get("plan_contract"), dict)
        else {}
    )
    if bool(contract.get("expects_visualization")):
        effective_limit = max(effective_limit, base_limit + 6)
    if bool(contract.get("expects_dependency")):
        effective_limit = max(effective_limit, base_limit + 8)
    if is_broad_full_analysis_query(query):
        effective_limit = max(effective_limit, base_limit + 10)

    columns_obj = getattr(primary_frame, "columns", None)
    columns_total = int(len(columns_obj)) if columns_obj is not None else 0
    if columns_total >= 20:
        effective_limit += 2
    if columns_total >= 40:
        effective_limit += 2

    return max(1, min(effective_limit, hard_cap))


# Compatibility alias.
_resolve_max_artifacts_limit = resolve_max_artifacts_limit
