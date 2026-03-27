from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.chat.tabular_query_parser import ParsedTabularQuery, parse_tabular_query
from app.services.chat.tabular_schema_resolver import resolve_requested_field
from app.services.chat.tabular_temporal_planner import (
    resolve_temporal_grouping,
    resolve_temporal_measure_column,
)


_ALLOWED_TASK_TYPES = {"aggregate", "chart", "comparison"}
_ALLOWED_OUTPUT_TYPES = {"table", "chart", "both"}
_ALLOWED_AGGREGATIONS = {"count", "sum", "avg", "min", "max"}
_ALLOWED_CHART_TYPES = {"none", "line", "bar", "area", "scatter"}
_COUNT_SIGNALS = ("count", "how many", "\u0441\u043a\u043e\u043b\u044c\u043a\u043e", "\u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432", "\u0447\u0438\u0441\u043b")


def _normalize_identifier(value: Any) -> str:
    return str(value or "").strip().lower()


def _canonical_route(*, parsed_route: str, selected_route: str) -> str:
    selected = _normalize_identifier(selected_route)
    parsed = _normalize_identifier(parsed_route)
    if selected == "comparison":
        return "comparison"
    if selected in {"chart", "trend"}:
        return "chart"
    if selected == "aggregation":
        return "aggregation"
    if parsed == "comparison":
        return "comparison"
    if parsed in {"chart", "trend"}:
        return "chart"
    return "aggregation"


def _task_type_for_route(route: str) -> str:
    if route == "comparison":
        return "comparison"
    if route == "chart":
        return "chart"
    return "aggregate"


def _output_type_for_route(route: str) -> str:
    if route in {"chart", "comparison"}:
        return "chart"
    return "table"


def _chart_type_for_plan(*, output_type: str, derived_time_grain: str) -> str:
    if output_type not in {"chart", "both"}:
        return "none"
    if derived_time_grain not in {"", "none"}:
        return "line"
    return "bar"


def _coerce_aggregation(raw_value: Any) -> str:
    value = _normalize_identifier(raw_value)
    if value in _ALLOWED_AGGREGATIONS:
        return value
    return "count"


def _has_count_signal(query: str) -> bool:
    normalized = str(query or "").strip().lower()
    if not normalized:
        return False
    return any(token in normalized for token in _COUNT_SIGNALS)


def _is_likely_numeric_column(*, table: Any, column: str) -> bool:
    metadata = getattr(table, "column_metadata", None)
    column_meta = metadata.get(column) if isinstance(metadata, dict) else {}
    dtype = str((column_meta or {}).get("dtype") or "").lower()
    if any(token in dtype for token in ("int", "float", "double", "decimal", "numeric", "number")):
        return True
    lowered = str(column or "").lower()
    return any(token in lowered for token in ("amount", "total", "cost", "price", "value", "revenue"))


def _build_dimension_payload(
    *,
    parsed: ParsedTabularQuery,
    table: Any,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    group_text = str(parsed.group_by_field_text or "").strip()
    if not group_text:
        return [], None
    resolution = resolve_requested_field(
        requested_field_text=group_text,
        table=table,
        expected_dtype_family=None,
    )
    if resolution.status == "matched" and resolution.matched_column:
        column = str(resolution.matched_column)
        return [{"requested": group_text, "field": column}], None
    return [], f"dimension_{str(resolution.status or 'no_match')}"


def _build_measure_payload(
    *,
    query: str,
    parsed: ParsedTabularQuery,
    table: Any,
    aggregation: str,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    requested = str(parsed.requested_field_text or "").strip() or "metric"
    if aggregation == "count":
        return [{"requested": requested, "field": None, "aggregation": "count"}], None

    requested_text = str(parsed.requested_field_text or "").strip() or None
    if requested_text:
        explicit = resolve_requested_field(
            requested_field_text=requested_text,
            table=table,
            expected_dtype_family="numeric",
        )
        if explicit.status == "matched" and explicit.matched_column:
            explicit_column = str(explicit.matched_column)
            if not _is_likely_numeric_column(table=table, column=explicit_column):
                explicit_column = ""
            if explicit_column:
                return [
                    {
                        "requested": requested,
                        "field": explicit_column,
                        "aggregation": aggregation,
                    }
                ], None
        if explicit.status == "matched" and explicit.matched_column:
            # matched a non-numeric candidate under weak lexical similarity; continue semantic recovery.
            pass
        if explicit.status == "ambiguous":
            return [{"requested": requested, "field": None, "aggregation": aggregation}], "ambiguous_measure"

    semantic = resolve_temporal_measure_column(
        query=query,
        table=table,
        requested_metric_text=requested_text,
    )
    if semantic.status == "resolved" and semantic.measure_column:
        semantic_column = str(semantic.measure_column)
        if not _is_likely_numeric_column(table=table, column=semantic_column):
            semantic_column = ""
        if semantic_column:
            return [
                {
                    "requested": requested,
                    "field": semantic_column,
                    "aggregation": aggregation,
                }
            ], None

    reason = str(semantic.fallback_reason or semantic.status or "missing_measure")
    return [{"requested": requested, "field": None, "aggregation": aggregation}], reason


def _build_temporal_payload(
    *,
    query: str,
    parsed: ParsedTabularQuery,
    table: Any,
) -> Tuple[str, Optional[str], Optional[str]]:
    grain = str(parsed.requested_time_grain or "").strip().lower()
    if not grain:
        return "none", None, None
    temporal = resolve_temporal_grouping(
        query=query,
        table=table,
        requested_time_grain=parsed.requested_time_grain,
        source_datetime_hint=parsed.source_datetime_field_hint,
    )
    source_datetime = str(temporal.source_datetime_field or "").strip() or None
    if source_datetime:
        return grain, source_datetime, None
    return grain, None, str(temporal.fallback_reason or temporal.temporal_plan_status or "missing_source_datetime_field")


def _confidence_from_hints(
    *,
    measure_field: Optional[str],
    dimension_field: Optional[str],
    derived_time_grain: str,
    source_datetime_field: Optional[str],
    ambiguity_reasons: Sequence[str],
) -> float:
    score = 0.24
    if measure_field is not None:
        score += 0.34
    if dimension_field:
        score += 0.16
    if derived_time_grain not in {"", "none"}:
        score += 0.1
    if source_datetime_field:
        score += 0.1
    if ambiguity_reasons:
        score = min(score, 0.28)
    return max(0.0, min(1.0, score))


def build_semantic_plan_hint(
    *,
    query: str,
    table: Any,
    selected_route: str,
) -> Dict[str, Any]:
    parsed = parse_tabular_query(query)
    route = _canonical_route(parsed_route=str(parsed.route or ""), selected_route=selected_route)
    task_type = _task_type_for_route(route)
    output_type = _output_type_for_route(route)
    parsed_operation = str(parsed.operation or "").strip().lower()
    if not parsed_operation and _has_count_signal(query):
        parsed_operation = "count"
    aggregation = _coerce_aggregation(parsed_operation or "count")

    derived_time_grain, source_datetime_field, temporal_issue = _build_temporal_payload(
        query=query,
        parsed=parsed,
        table=table,
    )
    measures, measure_issue = _build_measure_payload(
        query=query,
        parsed=parsed,
        table=table,
        aggregation=aggregation,
    )
    dimensions, dimension_issue = _build_dimension_payload(parsed=parsed, table=table)

    if derived_time_grain not in {"", "none"}:
        dimensions = []

    first_measure = dict(measures[0]) if measures and isinstance(measures[0], dict) else {}
    first_dimension = dict(dimensions[0]) if dimensions and isinstance(dimensions[0], dict) else {}
    measure_field = str(first_measure.get("field") or "").strip() or None
    dimension_field = str(first_dimension.get("field") or "").strip() or None
    ambiguity_flags = [
        reason
        for reason in (measure_issue, dimension_issue, temporal_issue)
        if str(reason or "").strip()
    ]

    return {
        "task_type": task_type if task_type in _ALLOWED_TASK_TYPES else "aggregate",
        "requested_output_type": output_type if output_type in _ALLOWED_OUTPUT_TYPES else "table",
        "source_scope": {
            "table_name": str(getattr(table, "table_name", "") or ""),
            "sheet_name": str(getattr(table, "sheet_name", "") or ""),
        },
        "measures": measures[:1],
        "dimensions": dimensions[:1],
        "derived_time_grain": derived_time_grain if derived_time_grain else "none",
        "source_datetime_field": source_datetime_field,
        "filters": [],
        "chart_type": _chart_type_for_plan(output_type=output_type, derived_time_grain=derived_time_grain),
        "confidence": _confidence_from_hints(
            measure_field=measure_field,
            dimension_field=dimension_field,
            derived_time_grain=derived_time_grain,
            source_datetime_field=source_datetime_field,
            ambiguity_reasons=ambiguity_flags,
        ),
        "ambiguity_flags": ambiguity_flags or ["none"],
        "semantic_operation_explicit": bool(parsed_operation),
    }


def merge_plan_with_semantic_hint(
    *,
    raw_plan: Dict[str, Any],
    semantic_hint: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(raw_plan or {})
    hint = dict(semantic_hint or {})
    if not hint:
        return merged

    merged["task_type"] = str(hint.get("task_type") or merged.get("task_type") or "aggregate")
    merged["requested_output_type"] = str(
        hint.get("requested_output_type") or merged.get("requested_output_type") or "table"
    )
    merged["source_scope"] = dict(hint.get("source_scope") or merged.get("source_scope") or {})
    merged["derived_time_grain"] = str(hint.get("derived_time_grain") or merged.get("derived_time_grain") or "none")
    merged["source_datetime_field"] = hint.get("source_datetime_field")
    merged["chart_type"] = str(hint.get("chart_type") or merged.get("chart_type") or "none")
    merged["confidence"] = float(hint.get("confidence") or merged.get("confidence") or 0.0)

    hint_measures = list(hint.get("measures") or [])
    if hint_measures:
        hint_measure = dict(hint_measures[0]) if isinstance(hint_measures[0], dict) else {}
        hint_field = str(hint_measure.get("field") or "").strip()
        hint_aggregation = _coerce_aggregation(hint_measure.get("aggregation"))
        hint_requested = str(hint_measure.get("requested") or "").strip().lower()
        hint_operation_explicit = bool(hint.get("semantic_operation_explicit"))

        raw_measures = list(merged.get("measures") or [])
        raw_measure = dict(raw_measures[0]) if raw_measures and isinstance(raw_measures[0], dict) else {}
        raw_field = str(raw_measure.get("field") or "").strip()
        keep_raw_measure = bool(
            raw_field
            and not hint_field
            and hint_aggregation == "count"
            and hint_requested in {"", "metric"}
            and not hint_operation_explicit
        )
        if keep_raw_measure:
            merged["measures"] = [raw_measure]
        else:
            merged["measures"] = [hint_measure]

    hint_dimensions = list(hint.get("dimensions") or [])
    if hint_dimensions:
        merged["dimensions"] = hint_dimensions[:1]
    elif str(merged.get("derived_time_grain") or "none").strip().lower() not in {"", "none"}:
        merged["dimensions"] = []
    elif "dimensions" not in merged:
        merged["dimensions"] = []

    if not isinstance(merged.get("filters"), list):
        merged["filters"] = list(hint.get("filters") or [])
    if not isinstance(merged.get("ambiguity_flags"), list):
        merged["ambiguity_flags"] = list(hint.get("ambiguity_flags") or ["none"])
    return merged


def build_execution_spec_hint(*, validated_plan: Dict[str, Any]) -> Dict[str, Any]:
    task_type = _normalize_identifier(validated_plan.get("task_type") or "aggregate")
    output_type = _normalize_identifier(validated_plan.get("requested_output_type") or "table")
    if task_type == "comparison":
        route = "comparison"
    elif task_type == "chart" or output_type in {"chart", "both"}:
        route = "chart"
    else:
        route = "aggregation"

    measure = (list(validated_plan.get("measures") or [{}]) or [{}])[0]
    aggregation = _coerce_aggregation(measure.get("aggregation"))
    measure_field = str(measure.get("field") or "").strip() or None
    if aggregation == "count":
        measure_field = None

    dimension = (list(validated_plan.get("dimensions") or [{}]) or [{}])[0]
    dimension_field = str(dimension.get("field") or "").strip() or None
    derived_time_grain = str(validated_plan.get("derived_time_grain") or "none").strip().lower() or "none"
    source_datetime_field = str(validated_plan.get("source_datetime_field") or "").strip() or None
    chart_type = _normalize_identifier(validated_plan.get("chart_type") or "none")
    if chart_type not in _ALLOWED_CHART_TYPES:
        chart_type = "line" if derived_time_grain not in {"", "none"} else ("bar" if route != "aggregation" else "none")

    if derived_time_grain not in {"", "none"}:
        output_columns = ["bucket", "value"]
    elif dimension_field:
        output_columns = ["group_key", "value"]
    else:
        output_columns = ["value"]

    return {
        "selected_route": route,
        "requested_output_type": output_type if output_type in _ALLOWED_OUTPUT_TYPES else ("chart" if route != "aggregation" else "table"),
        "measure": {"field": measure_field, "aggregation": aggregation},
        "dimension": {"field": dimension_field},
        "derived_time_grain": derived_time_grain,
        "source_datetime_field": source_datetime_field,
        "filters": list(validated_plan.get("filters") or []),
        "chart_type": chart_type,
        "output_columns": output_columns,
    }


def merge_execution_spec_with_hint(
    *,
    raw_execution_spec: Dict[str, Any],
    spec_hint: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(raw_execution_spec or {})
    hint = dict(spec_hint or {})
    if not hint:
        return merged

    merged["selected_route"] = str(hint.get("selected_route") or "aggregation")
    merged["requested_output_type"] = str(hint.get("requested_output_type") or "table")
    merged["measure"] = dict(hint.get("measure") or {})
    merged["dimension"] = dict(hint.get("dimension") or {})
    merged["derived_time_grain"] = str(hint.get("derived_time_grain") or "none")
    merged["source_datetime_field"] = hint.get("source_datetime_field")
    merged["filters"] = list(hint.get("filters") or [])
    merged["chart_type"] = str(hint.get("chart_type") or "none")
    merged["output_columns"] = list(hint.get("output_columns") or ["value"])
    return merged
