from __future__ import annotations

from typing import Any, Dict, Optional

from app.services.chat.tabular_query_parser import parse_tabular_query


_TASK_TYPE_ALIASES = {
    "aggregate": "aggregate",
    "aggregation": "aggregate",
    "summary": "aggregate",
    "table": "aggregate",
    "chart": "chart",
    "graph": "chart",
    "plot": "chart",
    "diagram": "chart",
    "visualization": "chart",
    "visualisation": "chart",
    "visual": "chart",
    "trend": "chart",
    "timeseries": "chart",
    "time_series": "chart",
    "time series": "chart",
    "temporal": "chart",
    "comparison": "comparison",
    "compare": "comparison",
    "comparative": "comparison",
}

_OUTPUT_TYPE_ALIASES = {
    "table": "table",
    "tables": "table",
    "text": "table",
    "result_table": "table",
    "chart": "chart",
    "graph": "chart",
    "plot": "chart",
    "diagram": "chart",
    "visualization": "chart",
    "visualisation": "chart",
    "visual": "chart",
    "figure": "chart",
    "both": "both",
    "table_and_chart": "both",
    "chart_and_table": "both",
    "table+chart": "both",
    "chart+table": "both",
}

_CHART_TYPE_ALIASES = {
    "none": "none",
    "line": "line",
    "bar": "bar",
    "area": "area",
    "scatter": "scatter",
    "column": "bar",
    "columns": "bar",
    "hist": "bar",
    "histogram": "bar",
    "time_series": "line",
    "timeseries": "line",
}

_ROUTE_TO_TASK_TYPE = {
    "aggregation": "aggregate",
    "chart": "chart",
    "trend": "chart",
    "comparison": "comparison",
}

_ROUTE_TO_OUTPUT_TYPE = {
    "aggregation": "table",
    "chart": "chart",
    "trend": "chart",
    "comparison": "chart",
}


def _normalized_alias(raw_value: Any, aliases: Dict[str, str]) -> Optional[str]:
    value = str(raw_value or "").strip().lower()
    if not value:
        return None
    return aliases.get(value)


def _coerce_confidence(raw_value: Any) -> Optional[float]:
    if raw_value is None:
        return None
    try:
        value = float(raw_value)
    except Exception:
        return None
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _plan_selected_route(validated_plan: Dict[str, Any]) -> str:
    task_type = str(validated_plan.get("task_type") or "aggregate").strip().lower()
    output_type = str(validated_plan.get("requested_output_type") or "table").strip().lower()
    if task_type == "comparison":
        return "comparison"
    if task_type == "aggregate":
        return "aggregation"
    if output_type in {"chart", "both"}:
        return "chart"
    return "aggregation"


def _canonical_execution_route(raw_route: Optional[str]) -> Optional[str]:
    route = str(raw_route or "").strip().lower()
    if not route:
        return None
    if route in {"aggregate", "aggregation"}:
        return "aggregation"
    if route in {"chart", "trend"}:
        return "chart"
    if route == "comparison":
        return "comparison"
    return None


def normalize_plan_payload(
    *,
    raw_plan: Dict[str, Any],
    query: str,
) -> Dict[str, Any]:
    plan = dict(raw_plan or {})
    parsed = parse_tabular_query(str(query or ""))
    route = str(parsed.route or "").strip().lower()

    task_type = _normalized_alias(plan.get("task_type"), _TASK_TYPE_ALIASES)
    if not task_type:
        task_type = _ROUTE_TO_TASK_TYPE.get(route)
    route_task_type = _ROUTE_TO_TASK_TYPE.get(route)
    if route_task_type and task_type and route in {"aggregation", "chart", "trend", "comparison"}:
        task_type = route_task_type
    if task_type:
        plan["task_type"] = task_type

    output_type = _normalized_alias(plan.get("requested_output_type"), _OUTPUT_TYPE_ALIASES)
    if not output_type:
        output_type = _ROUTE_TO_OUTPUT_TYPE.get(route)
    route_output_type = _ROUTE_TO_OUTPUT_TYPE.get(route)
    if route_output_type and output_type and route in {"aggregation", "chart", "trend", "comparison"}:
        output_type = route_output_type
    if not output_type and task_type in {"chart", "trend", "comparison"}:
        output_type = "chart"
    if output_type:
        plan["requested_output_type"] = output_type

    derived_time_grain = str(plan.get("derived_time_grain") or "").strip().lower()
    if not derived_time_grain and parsed.requested_time_grain:
        derived_time_grain = str(parsed.requested_time_grain)
        plan["derived_time_grain"] = derived_time_grain

    source_datetime = str(plan.get("source_datetime_field") or "").strip()
    if not source_datetime and parsed.source_datetime_field_hint:
        plan["source_datetime_field"] = str(parsed.source_datetime_field_hint)

    chart_type = _normalized_alias(plan.get("chart_type"), _CHART_TYPE_ALIASES)
    if not chart_type:
        if output_type in {"chart", "both"}:
            chart_type = "line" if derived_time_grain not in {"", "none"} else "bar"
        else:
            chart_type = "none"
    plan["chart_type"] = chart_type

    confidence = _coerce_confidence(plan.get("confidence"))
    if confidence is not None:
        plan["confidence"] = confidence

    if not isinstance(plan.get("measures"), list):
        plan["measures"] = []
    if not isinstance(plan.get("dimensions"), list):
        plan["dimensions"] = []
    if not isinstance(plan.get("filters"), list):
        plan["filters"] = []
    if not isinstance(plan.get("ambiguity_flags"), list):
        plan["ambiguity_flags"] = ["none"]

    if task_type in {"chart", "comparison"} and output_type is None:
        plan["requested_output_type"] = "chart"

    if not plan["measures"] and parsed.operation:
        measure_requested = str(parsed.requested_field_text or "").strip() or "metric"
        measure_field = str(parsed.requested_field_text or "").strip() or None
        plan["measures"] = [
            {
                "requested": measure_requested,
                "field": measure_field,
                "aggregation": str(parsed.operation),
            }
        ]

    if (
        not plan["dimensions"]
        and parsed.group_by_field_text
        and not str(plan.get("derived_time_grain") or "").strip()
    ):
        requested = str(parsed.group_by_field_text).strip()
        if requested:
            plan["dimensions"] = [{"requested": requested, "field": requested}]

    return plan


def normalize_execution_spec_payload(
    *,
    raw_execution_spec: Dict[str, Any],
    validated_plan: Dict[str, Any],
) -> Dict[str, Any]:
    payload = dict(raw_execution_spec or {})
    plan_route = _plan_selected_route(validated_plan)

    route = _canonical_execution_route(_normalized_alias(payload.get("selected_route"), _TASK_TYPE_ALIASES))
    if not route:
        route = _canonical_execution_route(_normalized_alias(payload.get("route"), _TASK_TYPE_ALIASES))
    if not route:
        route = _canonical_execution_route(_normalized_alias(payload.get("task_type"), _TASK_TYPE_ALIASES))
    if route != plan_route:
        route = plan_route
    payload["selected_route"] = route

    output_type = _normalized_alias(payload.get("requested_output_type"), _OUTPUT_TYPE_ALIASES)
    plan_output_type = str(validated_plan.get("requested_output_type") or "").strip().lower() or None
    if output_type != plan_output_type:
        output_type = plan_output_type
    if not output_type:
        output_type = "chart" if route in {"chart", "comparison"} else "table"
    if output_type:
        payload["requested_output_type"] = output_type

    if not str(payload.get("derived_time_grain") or "").strip():
        payload["derived_time_grain"] = str(validated_plan.get("derived_time_grain") or "none")
    if payload.get("source_datetime_field") in {None, ""}:
        payload["source_datetime_field"] = validated_plan.get("source_datetime_field")

    plan_measure = (list(validated_plan.get("measures") or [{}]) or [{}])[0]
    plan_dimension = (list(validated_plan.get("dimensions") or [{}]) or [{}])[0]
    measure = payload.get("measure") if isinstance(payload.get("measure"), dict) else {}
    plan_aggregation = str(plan_measure.get("aggregation") or "count").strip().lower() or "count"
    measure["aggregation"] = plan_aggregation
    if plan_aggregation == "count":
        measure["field"] = None
    else:
        measure["field"] = plan_measure.get("field")
    payload["measure"] = measure
    dimension = payload.get("dimension") if isinstance(payload.get("dimension"), dict) else {}
    dimension["field"] = plan_dimension.get("field")
    payload["dimension"] = dimension

    chart_type = _normalized_alias(payload.get("chart_type"), _CHART_TYPE_ALIASES)
    if not chart_type:
        if output_type in {"chart", "both"}:
            derived_time_grain = str(payload.get("derived_time_grain") or "none").strip().lower()
            chart_type = "line" if derived_time_grain not in {"", "none"} else "bar"
        else:
            chart_type = "none"
    payload["chart_type"] = chart_type

    if not isinstance(payload.get("output_columns"), list):
        payload["output_columns"] = []
    if not isinstance(payload.get("filters"), list):
        payload["filters"] = []
    payload["filters"] = list(validated_plan.get("filters") or [])

    derived_time_grain = str(payload.get("derived_time_grain") or "none").strip().lower()
    dimension_field = str((payload.get("dimension") or {}).get("field") or "").strip()
    canonical_output_columns = ["value"]
    if derived_time_grain not in {"", "none"}:
        canonical_output_columns = ["bucket", "value"]
    elif dimension_field:
        canonical_output_columns = ["group_key", "value"]
    payload["output_columns"] = canonical_output_columns

    return payload
