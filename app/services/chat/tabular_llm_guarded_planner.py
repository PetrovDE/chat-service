from __future__ import annotations

import asyncio
import json
import re
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.services.chat.language import detect_preferred_response_language, localized_text
from app.services.chat.tabular_chart_delivery import render_chart_artifact
from app.services.chat.tabular_llm_guarded_contract_alignment import (
    normalize_execution_spec_payload,
    normalize_plan_payload,
)
from app.services.chat.tabular_debug_contract import (
    apply_tabular_debug_fields,
    build_dataset_debug_fields,
)
from app.services.chat.tabular_response_composer import build_chart_response_text
from app.services.chat.tabular_schema_resolver import resolve_requested_field
from app.services.chat.tabular_temporal_planner import (
    build_temporal_bucket_expression,
    resolve_temporal_measure_column,
)
from app.services.llm.manager import llm_manager
from app.services.tabular import (
    GuardrailsConfig,
    SQLExecutionLimits,
    SQLGuardrails,
    TabularExecutionSession,
    rows_to_result_text,
    to_tabular_error_payload,
)
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


ANALYTIC_PLAN_VERSION = "tabular_analytic_plan_v1"
EXECUTION_SPEC_VERSION = "tabular_execution_spec_v1"
ANALYTIC_ROUTES = {"aggregation", "chart", "trend", "comparison"}
EXECUTABLE_ANALYTIC_ROUTES = {"aggregation", "chart", "comparison"}
ALLOWED_TASK_TYPES = {"aggregate", "chart", "trend", "comparison"}
ALLOWED_OUTPUT_TYPES = {"table", "chart", "both"}
ALLOWED_AGGREGATIONS = {"count", "sum", "avg", "min", "max"}
ALLOWED_TIME_GRAINS = {"day", "week", "month", "quarter", "year", "none", ""}
ALLOWED_FILTER_OPERATORS = {"eq", "contains", "gt", "gte", "lt", "lte"}
ALLOWED_CHART_TYPES = {"none", "line", "bar", "area", "scatter"}
JSON_CODE_BLOCK_RE = re.compile(r"```(?:json)?\s*(.*?)```", flags=re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class StageValidation:
    status: str
    reason: str
    payload: Optional[Dict[str, Any]] = None
    errors: Tuple[str, ...] = ()


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", " ", str(value or "").lower()).strip()


def _dtype_family(dtype_value: str) -> str:
    lowered = _normalize_text(dtype_value)
    if not lowered:
        return "unknown"
    if any(token in lowered for token in ("date", "time", "timestamp")):
        return "datetime"
    if any(token in lowered for token in ("int", "float", "double", "decimal", "numeric", "number")):
        return "numeric"
    if any(token in lowered for token in ("bool", "boolean")):
        return "boolean"
    if any(token in lowered for token in ("string", "str", "text", "object", "category")):
        return "categorical"
    return "unknown"


def _table_column_metadata(table: ResolvedTabularTable, column: str) -> Dict[str, Any]:
    raw = getattr(table, "column_metadata", None)
    if not isinstance(raw, dict):
        return {}
    payload = raw.get(column)
    return payload if isinstance(payload, dict) else {}


def _is_datetime_column(table: ResolvedTabularTable, column: str) -> bool:
    metadata = _table_column_metadata(table, column)
    family = _dtype_family(str(metadata.get("dtype") or ""))
    if family == "datetime":
        return True
    descriptor = " ".join(
        [
            str(column),
            str(table.column_aliases.get(column, "")),
            str(metadata.get("display_name") or ""),
        ]
    ).lower()
    return any(token in descriptor for token in ("date", "time", "timestamp", "created", "updated"))


def _is_numeric_column(table: ResolvedTabularTable, column: str) -> bool:
    metadata = _table_column_metadata(table, column)
    family = _dtype_family(str(metadata.get("dtype") or ""))
    return family == "numeric"


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    code_blocks = [str(item or "").strip() for item in JSON_CODE_BLOCK_RE.findall(raw) if str(item or "").strip()]
    candidates = [*code_blocks, raw]
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    brace_depth = 0
    in_string = False
    escaped = False
    start = None
    for idx, ch in enumerate(raw):
        if escaped:
            escaped = False
            continue
        if in_string:
            if ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if brace_depth == 0:
                start = idx
            brace_depth += 1
            continue
        if ch == "}" and brace_depth > 0:
            brace_depth -= 1
            if brace_depth == 0 and start is not None:
                snippet = raw[start : idx + 1]
                try:
                    parsed = json.loads(snippet)
                except Exception:
                    start = None
                    continue
                if isinstance(parsed, dict):
                    return parsed
                start = None
    return None


def _build_plan_prompt(
    *,
    query: str,
    table: ResolvedTabularTable,
    feedback: Sequence[str],
) -> str:
    columns_payload: List[Dict[str, Any]] = []
    for column in list(table.columns):
        metadata = _table_column_metadata(table, column)
        columns_payload.append(
            {
                "name": str(column),
                "alias": str(table.column_aliases.get(column, "")),
                "dtype": str(metadata.get("dtype") or "unknown"),
                "sample_values": list(metadata.get("sample_values") or [])[:4],
            }
        )
    feedback_block = "\n".join([f"- {item}" for item in feedback if str(item).strip()]) or "- none"
    schema_json = json.dumps(
        {
            "table_name": table.table_name,
            "sheet_name": table.sheet_name,
            "row_count": int(table.row_count or 0),
            "columns": columns_payload,
        },
        ensure_ascii=False,
    )
    return textwrap.dedent(
        f"""
You are a strict analytics planner.
Return ONLY JSON with this schema:
{{
  "task_type": "aggregate|chart|comparison",
  "requested_output_type": "table|chart|both",
  "source_scope": {{"table_name":"string","sheet_name":"string"}},
  "measures": [{{"requested":"string","field":"string|null","aggregation":"count|sum|avg|min|max"}}],
  "dimensions": [{{"requested":"string","field":"string"}}],
  "derived_time_grain": "none|day|week|month|quarter|year",
  "source_datetime_field": "string|null",
  "filters": [{{"field":"string","operator":"eq|contains|gt|gte|lt|lte","value":"string"}}],
  "chart_type": "none|line|bar|area|scatter",
  "confidence": 0.0,
  "ambiguity_flags": ["none or explicit ambiguity labels"]
}}

Rules:
- Use only fields from schema.
- Do not invent columns.
- If query asks for time grouping, set derived_time_grain and source_datetime_field.
- If ambiguity exists, include explicit ambiguity flag.
- Keep values concise and machine-readable.

Previous validator feedback:
{feedback_block}

User query:
{query}

Schema JSON:
{schema_json}
        """
    ).strip()


def _build_execution_spec_prompt(
    *,
    query: str,
    validated_plan: Dict[str, Any],
    feedback: Sequence[str],
) -> str:
    feedback_block = "\n".join([f"- {item}" for item in feedback if str(item).strip()]) or "- none"
    plan_json = json.dumps(validated_plan, ensure_ascii=False)
    return textwrap.dedent(
        f"""
You are a strict execution planner.
Return ONLY JSON with this schema:
{{
  "selected_route": "aggregation|chart|comparison",
  "requested_output_type": "table|chart|both",
  "measure": {{"field":"string|null","aggregation":"count|sum|avg|min|max"}},
  "dimension": {{"field":"string|null"}},
  "derived_time_grain": "none|day|week|month|quarter|year",
  "source_datetime_field": "string|null",
  "filters": [{{"field":"string","operator":"eq|contains|gt|gte|lt|lte","value":"string"}}],
  "chart_type": "none|line|bar|area|scatter",
  "output_columns": ["column aliases in output order"]
}}

Rules:
- Keep measure/dimension/time/filter values aligned with validated plan.
- Do not invent new fields.
- Keep selected_route compatible with validated plan intent.

Previous validator feedback:
{feedback_block}

User query:
{query}

Validated plan JSON:
{plan_json}
        """
    ).strip()


async def _call_llm_json(
    *,
    prompt: str,
    max_tokens: int,
    timeout_seconds: float,
    policy_class: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    try:
        response = await asyncio.wait_for(
            llm_manager.generate_response(
                prompt=prompt,
                temperature=0.0,
                max_tokens=max_tokens,
                conversation_history=None,
                cannot_wait=True,
                sla_critical=False,
                policy_class=policy_class,
            ),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        return None, "llm_timeout"
    except Exception:
        return None, "llm_runtime_error"
    parsed = _extract_json_object(str((response or {}).get("response") or ""))
    if not isinstance(parsed, dict):
        return None, "invalid_json"
    return parsed, "success"


def _resolve_column(
    *,
    requested_field: Optional[str],
    table: ResolvedTabularTable,
    expected_dtype_family: Optional[str],
) -> Tuple[Optional[str], str]:
    field = str(requested_field or "").strip()
    if not field:
        return None, "empty"
    if field in set(table.columns):
        if expected_dtype_family == "numeric" and not _is_numeric_column(table, field):
            return None, "dtype_mismatch_numeric"
        if expected_dtype_family == "datetime" and not _is_datetime_column(table, field):
            return None, "dtype_mismatch_datetime"
        return field, "exact"
    resolution = resolve_requested_field(
        requested_field_text=field,
        table=table,
        expected_dtype_family=expected_dtype_family,
    )
    if resolution.status == "matched" and resolution.matched_column:
        return str(resolution.matched_column), "resolved"
    return None, str(resolution.status or "no_match")


def _normalize_filters(raw_filters: Any) -> List[Dict[str, str]]:
    if not isinstance(raw_filters, list):
        return []
    normalized: List[Dict[str, str]] = []
    for item in raw_filters:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        operator = str(item.get("operator") or "eq").strip().lower()
        value = str(item.get("value") or "").strip()
        if not field or not operator:
            continue
        normalized.append({"field": field, "operator": operator, "value": value})
    return normalized


def _effective_plan_confidence(
    *,
    provided_confidence: Optional[float],
    task_type: str,
    output_type: str,
    validated_measures: Sequence[Dict[str, Any]],
    validated_dimensions: Sequence[Dict[str, Any]],
    derived_time_grain: str,
    source_datetime_field: Optional[str],
    explicit_ambiguity: Sequence[str],
) -> float:
    base_confidence = float(provided_confidence if provided_confidence is not None else 0.0)
    heuristic_confidence = 0.0
    if task_type in ALLOWED_TASK_TYPES:
        heuristic_confidence += 0.16
    if output_type in ALLOWED_OUTPUT_TYPES:
        heuristic_confidence += 0.14
    if validated_measures:
        heuristic_confidence += 0.36
    if validated_dimensions:
        heuristic_confidence += 0.18
    if derived_time_grain not in {"none", ""}:
        heuristic_confidence += 0.08
    if source_datetime_field:
        heuristic_confidence += 0.12
    if output_type in {"chart", "both"}:
        heuristic_confidence += 0.08
    if explicit_ambiguity:
        heuristic_confidence = min(heuristic_confidence, 0.19)
    return max(0.0, min(1.0, max(base_confidence, heuristic_confidence)))


def _validate_plan(
    *,
    plan: Dict[str, Any],
    table: ResolvedTabularTable,
    query: str = "",
) -> StageValidation:
    errors: List[str] = []
    task_type = str(plan.get("task_type") or "").strip().lower()
    output_type = str(plan.get("requested_output_type") or "").strip().lower()
    derived_time_grain = str(plan.get("derived_time_grain") or "none").strip().lower()
    source_datetime_field_raw = str(plan.get("source_datetime_field") or "").strip() or None
    chart_type = str(plan.get("chart_type") or "none").strip().lower()
    confidence_raw = plan.get("confidence")
    confidence: Optional[float] = None
    try:
        if confidence_raw is not None:
            confidence = max(0.0, min(1.0, float(confidence_raw)))
    except Exception:
        confidence = None
    ambiguity_flags = [str(item).strip().lower() for item in list(plan.get("ambiguity_flags") or []) if str(item).strip()]

    if task_type not in ALLOWED_TASK_TYPES:
        errors.append("invalid_task_type")
    if output_type not in ALLOWED_OUTPUT_TYPES:
        errors.append("invalid_requested_output_type")
    if derived_time_grain not in ALLOWED_TIME_GRAINS:
        errors.append("invalid_derived_time_grain")
    if chart_type not in ALLOWED_CHART_TYPES:
        errors.append("invalid_chart_type")
    explicit_ambiguity = [flag for flag in ambiguity_flags if flag not in {"none", ""}]
    if explicit_ambiguity:
        errors.append("explicit_plan_ambiguity")

    source_scope = plan.get("source_scope")
    if isinstance(source_scope, dict):
        scope_table = str(source_scope.get("table_name") or "").strip()
        if scope_table and scope_table != str(table.table_name):
            errors.append("source_scope_table_mismatch")

    raw_measures = list(plan.get("measures") or [])
    if not raw_measures:
        errors.append("missing_measure")
    validated_measures: List[Dict[str, Any]] = []
    for measure in raw_measures:
        if not isinstance(measure, dict):
            continue
        aggregation = str(measure.get("aggregation") or "").strip().lower()
        requested = str(measure.get("requested") or "").strip()
        field_raw = str(measure.get("field") or "").strip() or None
        if aggregation not in ALLOWED_AGGREGATIONS:
            errors.append("invalid_measure_aggregation")
            continue
        if aggregation == "count":
            validated_measures.append({"requested": requested, "field": None, "aggregation": aggregation})
            continue
        fallback_reason = "none"
        resolved_field, resolution_status = _resolve_column(
            requested_field=field_raw,
            table=table,
            expected_dtype_family="numeric",
        )
        allow_semantic_fallback = True
        normalized_raw_field = str(field_raw or "").strip().lower()
        if normalized_raw_field and re.fullmatch(r"[a-z][a-z0-9_]*", normalized_raw_field):
            if "_" in normalized_raw_field or normalized_raw_field.endswith("id") or normalized_raw_field.endswith("_id"):
                allow_semantic_fallback = False

        if not resolved_field and allow_semantic_fallback:
            semantic_resolution = resolve_temporal_measure_column(
                query=str(query or ""),
                table=table,
                requested_metric_text=requested or field_raw,
            )
            if semantic_resolution.status == "resolved" and semantic_resolution.measure_column:
                resolved_field = str(semantic_resolution.measure_column)
            fallback_reason = str(semantic_resolution.fallback_reason or semantic_resolution.status or "no_match")
        if not resolved_field:
            errors.append(f"measure_field_invalid:{fallback_reason if fallback_reason != 'none' else resolution_status}")
            continue
        validated_measures.append({"requested": requested, "field": resolved_field, "aggregation": aggregation})

    raw_dimensions = list(plan.get("dimensions") or [])
    validated_dimensions: List[Dict[str, str]] = []
    for dimension in raw_dimensions:
        if not isinstance(dimension, dict):
            continue
        requested = str(dimension.get("requested") or "").strip()
        field_raw = str(dimension.get("field") or "").strip() or None
        if not field_raw:
            continue
        expected_family = "datetime" if derived_time_grain not in {"none", ""} else None
        resolved_field, resolution_status = _resolve_column(
            requested_field=field_raw,
            table=table,
            expected_dtype_family=expected_family,
        )
        if not resolved_field:
            errors.append(f"dimension_field_invalid:{resolution_status}")
            continue
        validated_dimensions.append({"requested": requested, "field": resolved_field})

    normalized_filters = _normalize_filters(plan.get("filters"))
    validated_filters: List[Dict[str, str]] = []
    for item in normalized_filters:
        operator = str(item.get("operator") or "").strip().lower()
        if operator not in ALLOWED_FILTER_OPERATORS:
            errors.append(f"invalid_filter_operator:{operator or 'none'}")
            continue
        resolved_field, resolution_status = _resolve_column(
            requested_field=item.get("field"),
            table=table,
            expected_dtype_family=None,
        )
        if not resolved_field:
            errors.append(f"filter_field_invalid:{resolution_status}")
            continue
        validated_filters.append(
            {"field": resolved_field, "operator": operator, "value": str(item.get("value") or "").strip()}
        )

    source_datetime_field = source_datetime_field_raw
    if derived_time_grain not in {"none", ""}:
        if source_datetime_field:
            resolved_datetime, resolution_status = _resolve_column(
                requested_field=source_datetime_field,
                table=table,
                expected_dtype_family="datetime",
            )
            if not resolved_datetime:
                errors.append(f"source_datetime_invalid:{resolution_status}")
            else:
                source_datetime_field = resolved_datetime
        elif validated_dimensions:
            candidate = str(validated_dimensions[0].get("field") or "")
            if candidate and _is_datetime_column(table, candidate):
                source_datetime_field = candidate
        if not source_datetime_field:
            errors.append("missing_source_datetime_field")

    effective_confidence = _effective_plan_confidence(
        provided_confidence=confidence,
        task_type=task_type,
        output_type=output_type,
        validated_measures=validated_measures,
        validated_dimensions=validated_dimensions,
        derived_time_grain=derived_time_grain,
        source_datetime_field=source_datetime_field,
        explicit_ambiguity=explicit_ambiguity,
    )
    if effective_confidence < 0.2:
        errors.append("low_plan_confidence")

    if output_type in {"chart", "both"} and not validated_dimensions and derived_time_grain in {"none", ""}:
        errors.append("chart_requires_dimension")
    if task_type in {"trend", "chart", "comparison"} and not validated_dimensions and derived_time_grain in {"none", ""}:
        errors.append("task_requires_dimension")

    if errors:
        return StageValidation(status="failed", reason=str(errors[0]), errors=tuple(errors))

    validated_plan = {
        "task_type": task_type,
        "requested_output_type": output_type,
        "source_scope": {"table_name": str(table.table_name), "sheet_name": str(table.sheet_name)},
        "measures": validated_measures[:1],
        "dimensions": validated_dimensions[:1],
        "derived_time_grain": "none" if derived_time_grain in {"", "none"} else derived_time_grain,
        "source_datetime_field": source_datetime_field,
        "filters": validated_filters,
        "chart_type": chart_type,
        "confidence": effective_confidence,
        "ambiguity_flags": explicit_ambiguity or ["none"],
    }
    return StageValidation(status="success", reason="none", payload=validated_plan)


def _route_from_validated_plan(validated_plan: Dict[str, Any]) -> str:
    output_type = str(validated_plan.get("requested_output_type") or "table")
    task_type = str(validated_plan.get("task_type") or "aggregate")
    if task_type == "comparison":
        return "comparison"
    if task_type in {"chart", "trend"} or output_type in {"chart", "both"}:
        return "chart"
    return "aggregation"


def _filters_signature(filters: Sequence[Dict[str, str]]) -> List[Tuple[str, str, str]]:
    return sorted(
        [
            (
                str(item.get("field") or ""),
                str(item.get("operator") or ""),
                str(item.get("value") or ""),
            )
            for item in list(filters or [])
            if isinstance(item, dict)
        ]
    )


def _validate_execution_spec(
    *,
    execution_spec: Dict[str, Any],
    validated_plan: Dict[str, Any],
) -> StageValidation:
    errors: List[str] = []
    selected_route = str(execution_spec.get("selected_route") or "").strip().lower()
    output_type = str(execution_spec.get("requested_output_type") or "").strip().lower()
    derived_time_grain = str(execution_spec.get("derived_time_grain") or "none").strip().lower()
    source_datetime_field = str(execution_spec.get("source_datetime_field") or "").strip() or None
    chart_type = str(execution_spec.get("chart_type") or "none").strip().lower()
    output_columns = [str(item).strip() for item in list(execution_spec.get("output_columns") or []) if str(item).strip()]

    plan_route = _route_from_validated_plan(validated_plan)
    plan_output_type = str(validated_plan.get("requested_output_type") or "table")
    plan_measure = (list(validated_plan.get("measures") or [{}]) or [{}])[0]
    plan_dimension = (list(validated_plan.get("dimensions") or [{}]) or [{}])[0]

    measure_payload = execution_spec.get("measure")
    if not isinstance(measure_payload, dict):
        errors.append("missing_measure_payload")
        measure_payload = {}
    measure_field = str(measure_payload.get("field") or "").strip() or None
    measure_aggregation = str(measure_payload.get("aggregation") or "").strip().lower()

    dimension_payload = execution_spec.get("dimension")
    if not isinstance(dimension_payload, dict):
        dimension_payload = {}
    dimension_field = str(dimension_payload.get("field") or "").strip() or None

    normalized_filters = _normalize_filters(execution_spec.get("filters"))

    if selected_route not in EXECUTABLE_ANALYTIC_ROUTES:
        errors.append("invalid_selected_route")
    if selected_route != plan_route:
        errors.append("selected_route_plan_mismatch")
    if output_type not in ALLOWED_OUTPUT_TYPES:
        errors.append("invalid_output_type")
    if output_type != plan_output_type:
        errors.append("output_type_plan_mismatch")
    if measure_aggregation not in ALLOWED_AGGREGATIONS:
        errors.append("invalid_measure_aggregation")
    if measure_aggregation != str(plan_measure.get("aggregation") or ""):
        errors.append("measure_aggregation_plan_mismatch")
    plan_measure_field = str(plan_measure.get("field") or "").strip() or None
    if measure_field != plan_measure_field:
        errors.append("measure_field_plan_mismatch")
    plan_dimension_field = str(plan_dimension.get("field") or "").strip() or None
    if dimension_field != plan_dimension_field:
        errors.append("dimension_field_plan_mismatch")
    if derived_time_grain != str(validated_plan.get("derived_time_grain") or "none"):
        errors.append("time_grain_plan_mismatch")
    if source_datetime_field != (str(validated_plan.get("source_datetime_field") or "").strip() or None):
        errors.append("source_datetime_plan_mismatch")
    if chart_type not in ALLOWED_CHART_TYPES:
        errors.append("invalid_chart_type")
    if _filters_signature(normalized_filters) != _filters_signature(validated_plan.get("filters") or []):
        errors.append("filters_plan_mismatch")
    if "value" not in [item.lower() for item in output_columns]:
        errors.append("missing_value_output_column")
    if plan_dimension_field and not output_columns:
        errors.append("missing_dimension_output_column")

    if errors:
        return StageValidation(status="failed", reason=str(errors[0]), errors=tuple(errors))

    validated_spec = {
        "selected_route": selected_route,
        "requested_output_type": output_type,
        "measure": {"field": measure_field, "aggregation": measure_aggregation},
        "dimension": {"field": dimension_field},
        "derived_time_grain": derived_time_grain,
        "source_datetime_field": source_datetime_field,
        "filters": normalized_filters,
        "chart_type": chart_type,
        "output_columns": output_columns,
    }
    return StageValidation(status="success", reason="none", payload=validated_spec)


def _build_sql_from_execution_spec(
    *,
    table: ResolvedTabularTable,
    execution_spec: Dict[str, Any],
) -> Dict[str, Any]:
    table_q = _quote_ident(table.table_name)
    measure = execution_spec.get("measure") if isinstance(execution_spec.get("measure"), dict) else {}
    aggregation = str(measure.get("aggregation") or "count").strip().lower()
    measure_field = str(measure.get("field") or "").strip() or None
    dimension = execution_spec.get("dimension") if isinstance(execution_spec.get("dimension"), dict) else {}
    dimension_field = str(dimension.get("field") or "").strip() or None
    derived_time_grain = str(execution_spec.get("derived_time_grain") or "none").strip().lower()
    source_datetime_field = str(execution_spec.get("source_datetime_field") or "").strip() or None
    filters = _normalize_filters(execution_spec.get("filters"))

    where_parts: List[str] = []
    dimension_expr = None
    dimension_alias = None

    if derived_time_grain not in {"none", ""} and source_datetime_field:
        datetime_q = _quote_ident(source_datetime_field)
        bucket = build_temporal_bucket_expression(
            datetime_sql_expr=datetime_q,
            requested_time_grain=derived_time_grain,
        )
        dimension_expr = str(bucket.get("bucket_expr") or "")
        dimension_alias = "bucket"
        where_clause = str(bucket.get("where_clause") or "").strip()
        if where_clause.lower().startswith("where "):
            where_parts.append(where_clause[6:].strip())
    elif dimension_field:
        dimension_expr = _quote_ident(dimension_field)
        dimension_alias = "group_key"

    for item in filters:
        field = str(item.get("field") or "").strip()
        operator = str(item.get("operator") or "eq").strip().lower()
        value = str(item.get("value") or "").strip()
        field_q = _quote_ident(field)
        normalized_value = value.lower()
        if operator == "contains":
            where_parts.append(
                f"LOWER(TRIM(COALESCE(CAST({field_q} AS VARCHAR), ''))) LIKE {_sql_literal('%' + normalized_value + '%')}"
            )
        elif operator == "eq":
            where_parts.append(
                f"LOWER(TRIM(COALESCE(CAST({field_q} AS VARCHAR), ''))) = {_sql_literal(normalized_value)}"
            )
        elif operator in {"gt", "gte", "lt", "lte"}:
            cmp = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[operator]
            where_parts.append(
                f"TRY_CAST(REPLACE(NULLIF(TRIM(CAST({field_q} AS VARCHAR)), ''), ',', '.') AS DOUBLE) {cmp} "
                f"TRY_CAST({_sql_literal(value)} AS DOUBLE)"
            )

    if aggregation != "count" and measure_field:
        metric_q = _quote_ident(measure_field)
        where_parts.append(f"TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    if aggregation == "count":
        value_expr = "COUNT(*)"
    else:
        metric_q = _quote_ident(str(measure_field or ""))
        numeric_expr = f"CAST(REPLACE(NULLIF(TRIM(CAST({metric_q} AS VARCHAR)), ''), ',', '.') AS DOUBLE)"
        operation = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX"}[aggregation]
        value_expr = f"ROUND({operation}({numeric_expr}), 6)"

    if dimension_expr and dimension_alias:
        sql = (
            f"SELECT {dimension_expr} AS {dimension_alias}, {value_expr} AS value "
            f"FROM {table_q} "
            f"{where_sql} "
            f"GROUP BY {dimension_alias} "
            f"ORDER BY {dimension_alias} ASC"
        ).strip()
        output_columns = [dimension_alias, "value"]
    else:
        sql = f"SELECT {value_expr} AS value FROM {table_q} {where_sql}".strip()
        output_columns = ["value"]

    count_sql = f"SELECT COUNT(*) AS value FROM {table_q} {where_sql}".strip()
    return {
        "sql": sql,
        "count_sql": count_sql,
        "where_sql": where_sql,
        "output_columns": output_columns,
    }


def _build_guardrails() -> SQLGuardrails:
    return SQLGuardrails(
        GuardrailsConfig(
            max_sql_chars=int(settings.TABULAR_SQL_MAX_CHARS),
            max_result_rows=int(settings.TABULAR_SQL_MAX_RESULT_ROWS),
            max_scanned_rows=int(settings.TABULAR_SQL_MAX_SCANNED_ROWS),
            max_result_bytes=int(settings.TABULAR_SQL_MAX_RESULT_BYTES),
        )
    )


def _build_execution_limits() -> SQLExecutionLimits:
    return SQLExecutionLimits(
        max_result_rows=int(settings.TABULAR_SQL_MAX_RESULT_ROWS),
        max_result_bytes=int(settings.TABULAR_SQL_MAX_RESULT_BYTES),
    )


def _validate_sql(
    *,
    sql: str,
    table: ResolvedTabularTable,
    execution_spec: Dict[str, Any],
) -> StageValidation:
    guardrails = _build_guardrails()
    try:
        guarded_sql, guard_debug = guardrails.enforce(sql, estimated_scan_rows=int(table.row_count or 0))
    except Exception as exc:
        payload = to_tabular_error_payload(exc)
        reason = str(payload.get("code") or "sql_guardrail_blocked")
        return StageValidation(status="failed", reason=reason, payload={"error": payload})

    quoted_identifiers = [
        str(item or "").replace('""', '"')
        for item in re.findall(r'"([^"]+)"', str(guarded_sql))
    ]
    allowed_identifiers = set([str(table.table_name), *[str(col) for col in list(table.columns)]])
    for identifier in quoted_identifiers:
        if identifier not in allowed_identifiers:
            return StageValidation(status="failed", reason="sql_identifier_not_allowed", errors=("sql_identifier_not_allowed",))

    aggregation = str(((execution_spec.get("measure") or {}).get("aggregation") or "count")).lower()
    lowered_sql = str(guarded_sql).lower()
    if aggregation != "count" and f"{aggregation}(" not in lowered_sql:
        return StageValidation(status="failed", reason="sql_aggregation_mismatch", errors=("sql_aggregation_mismatch",))
    if aggregation == "count" and "count(" not in lowered_sql:
        return StageValidation(status="failed", reason="sql_aggregation_mismatch", errors=("sql_aggregation_mismatch",))

    dimension_field = str(((execution_spec.get("dimension") or {}).get("field") or "")).strip()
    derived_time_grain = str(execution_spec.get("derived_time_grain") or "none").strip().lower()
    if (dimension_field or derived_time_grain not in {"none", ""}) and "group by" not in lowered_sql:
        return StageValidation(status="failed", reason="sql_group_by_missing", errors=("sql_group_by_missing",))
    if derived_time_grain not in {"none", ""} and "strftime" not in lowered_sql:
        return StageValidation(status="failed", reason="sql_time_grain_mismatch", errors=("sql_time_grain_mismatch",))

    return StageValidation(
        status="success",
        reason="none",
        payload={"guarded_sql": guarded_sql, "guard_debug": guard_debug},
    )


def _execute_sql(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    guarded_sql: str,
    count_sql: str,
) -> Dict[str, Any]:
    timeout_seconds = float(settings.TABULAR_SQL_TIMEOUT_SECONDS)
    limits = _build_execution_limits()
    guardrails = _build_guardrails()
    with TabularExecutionSession(dataset=dataset, table=table, limits=limits) as session:
        rows = session.execute(guarded_sql, timeout_seconds=timeout_seconds)
        guarded_count_sql, _ = guardrails.enforce(count_sql, estimated_scan_rows=int(table.row_count or 0))
        count_rows = session.execute(guarded_count_sql, timeout_seconds=timeout_seconds)
    rows_effective = int(count_rows[0][0]) if count_rows and count_rows[0] else 0
    return {"rows": [tuple(row) for row in rows], "rows_effective": rows_effective}


def _validate_post_execution(
    *,
    rows: Sequence[Tuple[Any, ...]],
    execution_spec: Dict[str, Any],
) -> StageValidation:
    dimension_field = str(((execution_spec.get("dimension") or {}).get("field") or "")).strip()
    derived_time_grain = str(execution_spec.get("derived_time_grain") or "none").strip().lower()
    aggregation = str(((execution_spec.get("measure") or {}).get("aggregation") or "count")).strip().lower()
    if (dimension_field or derived_time_grain not in {"none", ""}) and any(len(row) < 2 for row in rows):
        return StageValidation(status="failed", reason="post_validation_dimension_shape_mismatch")
    if not dimension_field and derived_time_grain in {"none", ""} and any(len(row) < 1 for row in rows):
        return StageValidation(status="failed", reason="post_validation_scalar_shape_mismatch")
    if aggregation in {"sum", "avg", "min", "max"}:
        for row in list(rows)[:5]:
            if len(row) < 1:
                return StageValidation(status="failed", reason="post_validation_missing_value")
            value = row[-1]
            try:
                float(value)
            except Exception:
                return StageValidation(status="failed", reason="post_validation_non_numeric_value")
    return StageValidation(status="success", reason="none")


def _build_retry_exhausted_clarification(*, preferred_lang: str) -> str:
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "Please clarify the metric column and grouping scope. "
            "Example: `sum of amount by month using created_at`."
        ),
        en=(
            "Please clarify the metric column and grouping scope. "
            "Example: `sum of amount by month using created_at`."
        ),
    )


def _base_debug_fields(
    *,
    selected_route: str,
    planner_mode: str,
    analytic_plan_json: Dict[str, Any],
    plan_validation_status: str,
    sql_generation_mode: str,
    sql_validation_status: str,
    post_execution_validation_status: str,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_failure_reason: str,
    clarification_triggered_after_retries: bool,
    final_execution_mode: str,
) -> Dict[str, Any]:
    return {
        "planner_mode": planner_mode,
        "analytic_plan_version": ANALYTIC_PLAN_VERSION,
        "analytic_plan_json": analytic_plan_json,
        "plan_validation_status": plan_validation_status,
        "sql_generation_mode": sql_generation_mode,
        "sql_validation_status": sql_validation_status,
        "post_execution_validation_status": post_execution_validation_status,
        "repair_iteration_index": int(repair_iteration_index),
        "repair_iteration_count": int(repair_iteration_count),
        "repair_failure_reason": repair_failure_reason,
        "clarification_triggered_after_retries": bool(clarification_triggered_after_retries),
        "final_execution_mode": final_execution_mode,
        "final_selected_route": selected_route,
    }


def _build_success_payload(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    validated_plan: Dict[str, Any],
    execution_spec: Dict[str, Any],
    guarded_sql: str,
    guard_debug: Dict[str, Any],
    rows: Sequence[Tuple[Any, ...]],
    rows_effective: int,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_iteration_trace: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    selected_route = str(execution_spec.get("selected_route") or "aggregation")
    preferred_lang = detect_preferred_response_language(query)
    result_text = rows_to_result_text(rows)
    coverage_ratio = float(rows_effective / int(table.row_count or 1)) if int(table.row_count or 0) > 0 else 0.0

    payload: Dict[str, Any] = {
        "status": "ok",
        "prompt_context": (
            "LLM-guarded tabular execution (validated source of truth):\n"
            f"route={selected_route}\n"
            f"plan={json.dumps(validated_plan, ensure_ascii=False)}\n"
            f"sql={guarded_sql}\n"
            f"result={result_text}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": f"tabular_{selected_route}",
            "deterministic_path": False,
            "execution_route": "tabular_sql",
            "detected_intent": "llm_guarded_tabular",
            "selected_route": selected_route,
            "fallback_type": "none",
            "fallback_reason": "none",
            "tabular_sql": {
                **build_dataset_debug_fields(dataset=dataset, table=table),
                "table_row_count": int(table.row_count or 0),
                "executed_sql": guarded_sql,
                "policy_decision": guard_debug.get("policy_decision"),
                "guardrail_flags": guard_debug.get("guardrail_flags", []),
                "sql": guarded_sql,
                "result": result_text,
                "sql_guardrails": guard_debug,
                "measure": execution_spec.get("measure"),
                "dimension": execution_spec.get("dimension"),
                "filters": execution_spec.get("filters"),
                "output_columns": execution_spec.get("output_columns"),
                "repair_iteration_trace": list(repair_iteration_trace),
            },
        },
        "artifacts": [],
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | llm_guarded_sql"
            )
        ],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": int(rows_effective),
        "rows_used_map_total": int(rows_effective),
        "rows_used_reduce_total": int(rows_effective),
        "row_coverage_ratio": coverage_ratio,
    }
    base_fields = _base_debug_fields(
        selected_route=selected_route,
        planner_mode="llm_guarded",
        analytic_plan_json=validated_plan,
        plan_validation_status="success",
        sql_generation_mode="llm_guarded_execution_spec",
        sql_validation_status="success",
        post_execution_validation_status="success",
        repair_iteration_index=repair_iteration_index,
        repair_iteration_count=repair_iteration_count,
        repair_failure_reason="none",
        clarification_triggered_after_retries=False,
        final_execution_mode="llm_guarded",
    )
    payload = apply_tabular_debug_fields(payload, fields=base_fields)

    matched_columns: List[str] = []
    measure_field = str(((execution_spec.get("measure") or {}).get("field") or "")).strip()
    if measure_field:
        matched_columns.append(measure_field)
    dimension_field = str(((execution_spec.get("dimension") or {}).get("field") or "")).strip()
    if dimension_field:
        matched_columns.append(dimension_field)
    source_datetime = str(execution_spec.get("source_datetime_field") or "").strip()
    if source_datetime:
        matched_columns.append(source_datetime)
    for item in list(execution_spec.get("filters") or []):
        if isinstance(item, dict):
            field = str(item.get("field") or "").strip()
            if field:
                matched_columns.append(field)
    deduped_columns = list(dict.fromkeys(matched_columns))
    payload = apply_tabular_debug_fields(
        payload,
        fields={
            "requested_output_type": str(execution_spec.get("requested_output_type") or "table"),
            "matched_columns": deduped_columns,
            "unmatched_requested_fields": [],
            "requested_time_grain": None
            if str(execution_spec.get("derived_time_grain") or "none") in {"none", ""}
            else str(execution_spec.get("derived_time_grain")),
            "source_datetime_field": source_datetime or None,
            "derived_temporal_dimension": (
                f"{execution_spec.get('derived_time_grain')}({source_datetime})"
                if source_datetime and str(execution_spec.get("derived_time_grain") or "none") not in {"none", ""}
                else None
            ),
            "temporal_plan_status": (
                "resolved" if str(execution_spec.get("derived_time_grain") or "none") not in {"none", ""} else "not_requested"
            ),
            "temporal_aggregation_plan": {
                "requested_time_grain": None
                if str(execution_spec.get("derived_time_grain") or "none") in {"none", ""}
                else str(execution_spec.get("derived_time_grain")),
                "source_datetime_field": source_datetime or None,
                "operation": str(((execution_spec.get("measure") or {}).get("aggregation") or "count")),
                "measure_column": measure_field or None,
                "status": "resolved",
                "fallback_reason": "none",
            },
        },
    )

    should_render_chart = (
        str(execution_spec.get("requested_output_type") or "table") in {"chart", "both"}
        or selected_route in {"chart", "trend", "comparison"}
    )
    if should_render_chart:
        chart_dimension = source_datetime or dimension_field or "dimension"
        chart_delivery = render_chart_artifact(
            rows=rows,
            chart_spec={
                "matched_chart_field": chart_dimension,
                "requested_dimension_column": chart_dimension,
                "title": f"{selected_route.title()} chart",
                "x_title": chart_dimension,
                "y_title": str(((execution_spec.get("measure") or {}).get("aggregation") or "value")),
            },
        )
        artifact = chart_delivery.get("artifact") if isinstance(chart_delivery.get("artifact"), dict) else None
        artifacts = [artifact] if artifact else []
        payload["artifacts"] = artifacts
        chart_response = build_chart_response_text(
            preferred_lang=preferred_lang,
            column_label=chart_dimension,
            chart_rendered=bool(chart_delivery.get("chart_rendered", False)),
            chart_artifact_available=bool(chart_delivery.get("chart_artifact_available", False)),
            chart_fallback_reason=str(chart_delivery.get("chart_fallback_reason") or "none"),
            result_text=result_text,
            source_scope=f"{getattr(target_file, 'original_filename', 'unknown')} | table={table.table_name}",
        )
        payload["chart_response_text"] = chart_response
        payload = apply_tabular_debug_fields(
            payload,
            fields={
                "requested_chart_field": chart_dimension,
                "matched_chart_field": chart_dimension,
                "chart_spec_generated": True,
                "chart_rendered": bool(chart_delivery.get("chart_rendered", False)),
                "chart_artifact_path": chart_delivery.get("chart_artifact_path"),
                "chart_artifact_id": chart_delivery.get("chart_artifact_id"),
                "chart_artifact_available": bool(chart_delivery.get("chart_artifact_available", False)),
                "chart_artifact_exists": bool(chart_delivery.get("chart_artifact_exists", False)),
                "chart_fallback_reason": str(chart_delivery.get("chart_fallback_reason") or "none"),
                "controlled_response_state": (
                    "chart_render_success"
                    if bool(chart_delivery.get("chart_artifact_available", False))
                    else "chart_render_failed"
                ),
                "fallback_type": (
                    "none"
                    if bool(chart_delivery.get("chart_artifact_available", False))
                    else "tabular_chart_render_failed"
                ),
                "fallback_reason": (
                    "none"
                    if bool(chart_delivery.get("chart_artifact_available", False))
                    else str(chart_delivery.get("chart_fallback_reason") or "chart_render_failed")
                ),
            },
        )

    return payload


def _build_retry_exhausted_payload(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    selected_route: str,
    last_plan: Dict[str, Any],
    plan_validation_status: str,
    sql_validation_status: str,
    post_execution_validation_status: str,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_failure_reason: str,
    repair_iteration_trace: Sequence[Dict[str, Any]],
    clarification_prompt_override: Optional[str] = None,
    clarification_reason_code: str = "planner_validation_failed",
) -> Dict[str, Any]:
    preferred_lang = detect_preferred_response_language(query)
    clarification_prompt = (
        str(clarification_prompt_override).strip()
        if str(clarification_prompt_override or "").strip()
        else _build_retry_exhausted_clarification(preferred_lang=preferred_lang)
    )
    payload = {
        "status": "error",
        "clarification_prompt": clarification_prompt,
        "prompt_context": (
            "LLM-guarded tabular loop exhausted bounded retries.\n"
            f"repair_failure_reason={repair_failure_reason}\n"
            f"repair_iteration_trace={json.dumps(list(repair_iteration_trace), ensure_ascii=False)}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_llm_guarded_clarification",
            "deterministic_path": False,
            "execution_route": "tabular_sql",
            "detected_intent": "llm_guarded_tabular",
            "selected_route": selected_route,
            "fallback_type": "clarification",
            "fallback_reason": repair_failure_reason,
            "tabular_sql": {
                **build_dataset_debug_fields(dataset=dataset, table=table),
                "table_row_count": int(table.row_count or 0),
                "executed_sql": None,
                "sql": None,
                "result": None,
                "policy_decision": {"allowed": False, "reason": repair_failure_reason},
                "guardrail_flags": [],
                "sql_guardrails": {"valid": False, "reason": repair_failure_reason},
                "repair_iteration_trace": list(repair_iteration_trace),
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | llm_guarded_retries_exhausted"
            )
        ],
        "artifacts": [],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": 0,
        "rows_used_map_total": 0,
        "rows_used_reduce_total": 0,
        "row_coverage_ratio": 0.0,
    }
    base_fields = _base_debug_fields(
        selected_route=selected_route,
        planner_mode="llm_guarded",
        analytic_plan_json=last_plan,
        plan_validation_status=plan_validation_status,
        sql_generation_mode="llm_guarded_execution_spec",
        sql_validation_status=sql_validation_status,
        post_execution_validation_status=post_execution_validation_status,
        repair_iteration_index=repair_iteration_index,
        repair_iteration_count=repair_iteration_count,
        repair_failure_reason=repair_failure_reason,
        clarification_triggered_after_retries=True,
        final_execution_mode="clarification_after_retries",
    )
    payload = apply_tabular_debug_fields(payload, fields=base_fields)
    payload = apply_tabular_debug_fields(
        payload,
        fields={
            "requested_output_type": str(last_plan.get("requested_output_type") or "table"),
            "matched_columns": [],
            "unmatched_requested_fields": [],
            "clarification_reason_code": str(clarification_reason_code or "planner_validation_failed"),
        },
    )
    return payload


def _is_guarded_mode_candidate(*, parsed_query_route: str, selected_route: str) -> bool:
    normalized_parsed = str(parsed_query_route or "").strip().lower()
    normalized_selected = str(selected_route or "").strip().lower()
    if normalized_selected in ANALYTIC_ROUTES:
        return True
    if normalized_selected == "unsupported_missing_column" and normalized_parsed in ANALYTIC_ROUTES:
        return True
    return normalized_parsed in ANALYTIC_ROUTES


def build_plan_prompt(*, query: str, table: ResolvedTabularTable, feedback: Sequence[str]) -> str:
    return _build_plan_prompt(query=query, table=table, feedback=feedback)


async def call_llm_json(
    *,
    prompt: str,
    max_tokens: int,
    timeout_seconds: float,
    policy_class: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    return await _call_llm_json(
        prompt=prompt,
        max_tokens=max_tokens,
        timeout_seconds=timeout_seconds,
        policy_class=policy_class,
    )


def validate_plan(*, plan: Dict[str, Any], table: ResolvedTabularTable, query: str = "") -> StageValidation:
    return _validate_plan(plan=plan, table=table, query=query)


def route_from_validated_plan(validated_plan: Dict[str, Any]) -> str:
    return _route_from_validated_plan(validated_plan)


def build_execution_spec_prompt(
    *,
    query: str,
    validated_plan: Dict[str, Any],
    feedback: Sequence[str],
) -> str:
    return _build_execution_spec_prompt(
        query=query,
        validated_plan=validated_plan,
        feedback=feedback,
    )


def validate_execution_spec(*, execution_spec: Dict[str, Any], validated_plan: Dict[str, Any]) -> StageValidation:
    return _validate_execution_spec(
        execution_spec=execution_spec,
        validated_plan=validated_plan,
    )


def build_sql_from_execution_spec(*, table: ResolvedTabularTable, execution_spec: Dict[str, Any]) -> Dict[str, Any]:
    return _build_sql_from_execution_spec(table=table, execution_spec=execution_spec)


def validate_sql(*, sql: str, table: ResolvedTabularTable, execution_spec: Dict[str, Any]) -> StageValidation:
    return _validate_sql(sql=sql, table=table, execution_spec=execution_spec)


def validate_post_execution(*, rows: Sequence[Tuple[Any, ...]], execution_spec: Dict[str, Any]) -> StageValidation:
    return _validate_post_execution(rows=rows, execution_spec=execution_spec)


def execute_sql(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    guarded_sql: str,
    count_sql: str,
) -> Dict[str, Any]:
    return _execute_sql(
        dataset=dataset,
        table=table,
        guarded_sql=guarded_sql,
        count_sql=count_sql,
    )


def build_success_payload(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    validated_plan: Dict[str, Any],
    execution_spec: Dict[str, Any],
    guarded_sql: str,
    guard_debug: Dict[str, Any],
    rows: Sequence[Tuple[Any, ...]],
    rows_effective: int,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_iteration_trace: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    return _build_success_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        validated_plan=validated_plan,
        execution_spec=execution_spec,
        guarded_sql=guarded_sql,
        guard_debug=guard_debug,
        rows=rows,
        rows_effective=rows_effective,
        repair_iteration_index=repair_iteration_index,
        repair_iteration_count=repair_iteration_count,
        repair_iteration_trace=repair_iteration_trace,
    )


def build_retry_exhausted_payload(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    selected_route: str,
    last_plan: Dict[str, Any],
    plan_validation_status: str,
    sql_validation_status: str,
    post_execution_validation_status: str,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_failure_reason: str,
    repair_iteration_trace: Sequence[Dict[str, Any]],
    clarification_prompt_override: Optional[str] = None,
    clarification_reason_code: str = "planner_validation_failed",
) -> Dict[str, Any]:
    return _build_retry_exhausted_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        selected_route=selected_route,
        last_plan=last_plan,
        plan_validation_status=plan_validation_status,
        sql_validation_status=sql_validation_status,
        post_execution_validation_status=post_execution_validation_status,
        repair_iteration_index=repair_iteration_index,
        repair_iteration_count=repair_iteration_count,
        repair_failure_reason=repair_failure_reason,
        repair_iteration_trace=repair_iteration_trace,
        clarification_prompt_override=clarification_prompt_override,
        clarification_reason_code=clarification_reason_code,
    )


def is_guarded_mode_candidate(*, parsed_query_route: str, selected_route: str) -> bool:
    return _is_guarded_mode_candidate(
        parsed_query_route=parsed_query_route,
        selected_route=selected_route,
    )


async def maybe_execute_llm_guarded_tabular(
    *,
    query: str,
    parsed_query_route: str,
    selected_route: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
) -> Optional[Dict[str, Any]]:
    if not bool(getattr(settings, "TABULAR_LLM_GUARDED_PLANNER_ENABLED", False)):
        return None
    if not _is_guarded_mode_candidate(
        parsed_query_route=parsed_query_route,
        selected_route=selected_route,
    ):
        return None

    max_attempts = int(getattr(settings, "TABULAR_LLM_GUARDED_MAX_ATTEMPTS", 3) or 3)
    max_attempts = max(1, min(5, max_attempts))
    plan_max_tokens = int(getattr(settings, "TABULAR_LLM_GUARDED_PLAN_MAX_TOKENS", 800) or 800)
    exec_max_tokens = int(getattr(settings, "TABULAR_LLM_GUARDED_EXECUTION_MAX_TOKENS", 700) or 700)
    plan_timeout_seconds = float(getattr(settings, "TABULAR_LLM_GUARDED_PLAN_TIMEOUT_SECONDS", 5.0) or 5.0)
    exec_timeout_seconds = float(getattr(settings, "TABULAR_LLM_GUARDED_EXECUTION_TIMEOUT_SECONDS", 5.0) or 5.0)

    plan_feedback: List[str] = []
    execution_feedback: List[str] = []
    repair_iteration_trace: List[Dict[str, Any]] = []

    last_validated_plan: Dict[str, Any] = {}
    plan_validation_status = "not_attempted"
    sql_validation_status = "not_attempted"
    post_execution_validation_status = "not_attempted"
    repair_failure_reason = "none"

    for attempt_index in range(1, max_attempts + 1):
        plan_prompt = _build_plan_prompt(
            query=query,
            table=table,
            feedback=plan_feedback,
        )
        raw_plan, plan_call_status = await _call_llm_json(
            prompt=plan_prompt,
            max_tokens=plan_max_tokens,
            timeout_seconds=plan_timeout_seconds,
            policy_class="tabular_llm_guarded_plan",
        )
        if plan_call_status in {"llm_timeout", "llm_runtime_error"}:
            return None
        if plan_call_status != "success" or not isinstance(raw_plan, dict):
            plan_validation_status = "failed"
            repair_failure_reason = "plan_invalid_json"
            plan_feedback = [f"plan parse failed: {plan_call_status}"]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "plan_generation",
                    "status": "failed",
                    "reason": repair_failure_reason,
                }
            )
            continue

        normalized_plan = normalize_plan_payload(raw_plan=raw_plan, query=query)
        plan_validation = _validate_plan(plan=normalized_plan, table=table, query=query)
        plan_validation_status = plan_validation.status
        if plan_validation.status != "success" or not isinstance(plan_validation.payload, dict):
            repair_failure_reason = plan_validation.reason
            errors = list(plan_validation.errors) or [plan_validation.reason]
            plan_feedback = [f"plan validation failed: {item}" for item in errors[:6]]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "plan_validation",
                    "status": "failed",
                    "reason": repair_failure_reason,
                    "errors": errors[:6],
                }
            )
            continue

        last_validated_plan = dict(plan_validation.payload)
        execution_prompt = _build_execution_spec_prompt(
            query=query,
            validated_plan=last_validated_plan,
            feedback=execution_feedback,
        )
        execution_spec_raw, execution_call_status = await _call_llm_json(
            prompt=execution_prompt,
            max_tokens=exec_max_tokens,
            timeout_seconds=exec_timeout_seconds,
            policy_class="tabular_llm_guarded_execution",
        )
        if execution_call_status in {"llm_timeout", "llm_runtime_error"}:
            return None
        if execution_call_status != "success" or not isinstance(execution_spec_raw, dict):
            sql_validation_status = "failed"
            repair_failure_reason = "execution_spec_invalid_json"
            execution_feedback = [f"execution spec parse failed: {execution_call_status}"]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "execution_spec_generation",
                    "status": "failed",
                    "reason": repair_failure_reason,
                }
            )
            continue

        normalized_execution_spec = normalize_execution_spec_payload(
            raw_execution_spec=execution_spec_raw,
            validated_plan=last_validated_plan,
        )
        execution_spec_validation = _validate_execution_spec(
            execution_spec=normalized_execution_spec,
            validated_plan=last_validated_plan,
        )
        if execution_spec_validation.status != "success" or not isinstance(execution_spec_validation.payload, dict):
            sql_validation_status = "failed"
            repair_failure_reason = execution_spec_validation.reason
            errors = list(execution_spec_validation.errors) or [execution_spec_validation.reason]
            execution_feedback = [f"execution spec validation failed: {item}" for item in errors[:6]]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "execution_spec_validation",
                    "status": "failed",
                    "reason": repair_failure_reason,
                    "errors": errors[:6],
                }
            )
            continue

        execution_spec = dict(execution_spec_validation.payload)
        sql_bundle = _build_sql_from_execution_spec(table=table, execution_spec=execution_spec)
        sql_validation = _validate_sql(
            sql=str(sql_bundle.get("sql") or ""),
            table=table,
            execution_spec=execution_spec,
        )
        sql_validation_status = sql_validation.status
        if sql_validation.status != "success" or not isinstance(sql_validation.payload, dict):
            repair_failure_reason = sql_validation.reason
            execution_feedback = [f"sql validation failed: {repair_failure_reason}"]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "sql_validation",
                    "status": "failed",
                    "reason": repair_failure_reason,
                }
            )
            continue

        guarded_sql = str(sql_validation.payload.get("guarded_sql") or "")
        guard_debug = (
            sql_validation.payload.get("guard_debug")
            if isinstance(sql_validation.payload.get("guard_debug"), dict)
            else {}
        )
        try:
            execution_output = await asyncio.to_thread(
                _execute_sql,
                dataset=dataset,
                table=table,
                guarded_sql=guarded_sql,
                count_sql=str(sql_bundle.get("count_sql") or ""),
            )
        except Exception as exc:
            sql_validation_status = "failed"
            post_execution_validation_status = "failed"
            error_payload = to_tabular_error_payload(exc)
            repair_failure_reason = str(error_payload.get("code") or "sql_execution_failed")
            execution_feedback = [f"execution failed: {repair_failure_reason}"]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "execution",
                    "status": "failed",
                    "reason": repair_failure_reason,
                }
            )
            continue

        rows = list(execution_output.get("rows") or [])
        rows_effective = int(execution_output.get("rows_effective", 0) or 0)
        post_validation = _validate_post_execution(rows=rows, execution_spec=execution_spec)
        post_execution_validation_status = post_validation.status
        if post_validation.status != "success":
            repair_failure_reason = post_validation.reason
            execution_feedback = [f"post execution validation failed: {repair_failure_reason}"]
            repair_iteration_trace.append(
                {
                    "iteration": attempt_index,
                    "stage": "post_execution_validation",
                    "status": "failed",
                    "reason": repair_failure_reason,
                }
            )
            continue

        repair_iteration_trace.append(
            {
                "iteration": attempt_index,
                "stage": "completed",
                "status": "success",
                "reason": "none",
            }
        )
        return _build_success_payload(
            query=query,
            dataset=dataset,
            table=table,
            target_file=target_file,
            validated_plan=last_validated_plan,
            execution_spec=execution_spec,
            guarded_sql=guarded_sql,
            guard_debug=guard_debug,
            rows=rows,
            rows_effective=rows_effective,
            repair_iteration_index=attempt_index,
            repair_iteration_count=max_attempts,
            repair_iteration_trace=repair_iteration_trace,
        )

    return _build_retry_exhausted_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        selected_route=_route_from_validated_plan(last_validated_plan) if last_validated_plan else "aggregation",
        last_plan=last_validated_plan,
        plan_validation_status=plan_validation_status,
        sql_validation_status=sql_validation_status,
        post_execution_validation_status=post_execution_validation_status,
        repair_iteration_index=max_attempts,
        repair_iteration_count=max_attempts,
        repair_failure_reason=repair_failure_reason if repair_failure_reason != "none" else "retries_exhausted",
        repair_iteration_trace=repair_iteration_trace,
    )
