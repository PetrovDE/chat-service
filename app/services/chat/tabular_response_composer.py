from __future__ import annotations

from typing import Sequence

from app.services.chat.controlled_response_composer import (
    build_chart_response_text as _build_chart_response_text,
    build_chart_unmatched_field_message as _build_chart_unmatched_field_message,
    build_execution_error_message as _build_execution_error_message,
    build_missing_column_message as _build_missing_column_message,
    build_no_context_message as _build_no_context_file_message,
    build_timeout_message as _build_timeout_message,
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


def build_chart_unmatched_field_message(*, preferred_lang: str, requested_field: str) -> str:
    return _build_chart_unmatched_field_message(
        preferred_lang=preferred_lang,
        requested_field=requested_field,
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
) -> str:
    return _build_chart_response_text(
        preferred_lang=preferred_lang,
        column_label=column_label,
        chart_rendered=chart_rendered,
        chart_artifact_available=chart_artifact_available,
        chart_fallback_reason=chart_fallback_reason,
        result_text=result_text,
    )


def build_no_context_tabular_message(*, preferred_lang: str) -> str:
    return _build_no_context_file_message(preferred_lang=preferred_lang)
