from __future__ import annotations

from typing import Sequence

from app.services.chat.controlled_response_composer import (
    build_chart_response_text as _build_chart_response_text,
    build_chart_unmatched_field_message as _build_chart_unmatched_field_message,
    build_execution_error_message as _build_execution_error_message,
    build_missing_column_message as _build_missing_column_message,
    build_no_context_message as _build_no_context_file_message,
    build_scope_clarification_message as _build_scope_clarification_message,
    build_timeout_message as _build_timeout_message,
)
from app.services.chat.tabular_aggregation_response_composer import (
    build_aggregation_response_text as _build_aggregation_response_text,
)


def build_missing_column_message(
    *,
    preferred_lang: str,
    requested_fields: Sequence[str],
    alternatives: Sequence[str],
    ambiguous: bool = False,
) -> str:
    return _build_missing_column_message(
        preferred_lang=preferred_lang,
        requested_fields=requested_fields,
        alternatives=alternatives,
        ambiguous=ambiguous,
    )


def build_chart_unmatched_field_message(
    *,
    preferred_lang: str,
    requested_field: str,
    alternatives: Sequence[str] | None = None,
) -> str:
    return _build_chart_unmatched_field_message(
        preferred_lang=preferred_lang,
        requested_field=requested_field,
        alternatives=alternatives,
    )


def build_timeout_message(*, preferred_lang: str) -> str:
    return _build_timeout_message(preferred_lang=preferred_lang)


def build_execution_error_message(*, preferred_lang: str) -> str:
    return _build_execution_error_message(preferred_lang=preferred_lang)


def build_chart_response_text(
    *,
    preferred_lang: str,
    column_label: str,
    chart_rendered: bool,
    chart_artifact_available: bool,
    chart_fallback_reason: str,
    result_text: str,
    source_scope: str | None = None,
) -> str:
    return _build_chart_response_text(
        preferred_lang=preferred_lang,
        column_label=column_label,
        chart_rendered=chart_rendered,
        chart_artifact_available=chart_artifact_available,
        chart_fallback_reason=chart_fallback_reason,
        result_text=result_text,
        source_scope=source_scope,
    )


def build_aggregation_response_text(
    *,
    preferred_lang: str,
    result_text: str,
    operation: str,
    metric_column: str | None = None,
    group_by_column: str | None = None,
    source_scope: str | None = None,
    max_rows: int = 8,
) -> str:
    return _build_aggregation_response_text(
        preferred_lang=preferred_lang,
        result_text=result_text,
        operation=operation,
        metric_column=metric_column,
        group_by_column=group_by_column,
        source_scope=source_scope,
        max_rows=max_rows,
    )


def build_no_context_tabular_message(*, preferred_lang: str) -> str:
    return _build_no_context_file_message(preferred_lang=preferred_lang)


def build_scope_clarification_message(
    *,
    preferred_lang: str,
    scope_kind: str,
    scope_options: Sequence[str],
) -> str:
    return _build_scope_clarification_message(
        preferred_lang=preferred_lang,
        scope_kind=scope_kind,
        scope_options=scope_options,
    )
