from __future__ import annotations

import json
from typing import Mapping, Sequence

from app.services.chat.language import localized_text


STATE_AMBIGUOUS_COLUMN = "ambiguous_column"
STATE_AMBIGUOUS_FILE = "ambiguous_file"
STATE_CHART_RENDER_FAILED = "chart_render_failed"
STATE_CHART_RENDER_SUCCESS = "chart_render_success"
STATE_CHART_UNMATCHED_FIELD = "chart_unmatched_field"
STATE_FILE_NOT_FOUND = "file_not_found"
STATE_MISSING_COLUMN = "missing_column"
STATE_NO_CONTEXT = "no_context"
STATE_NO_RETRIEVAL = "no_retrieval"
STATE_RUNTIME_ERROR = "runtime_error"
STATE_SCOPE_AMBIGUITY = "scope_ambiguity"
STATE_TABULAR_EXECUTION_ERROR = "tabular_execution_error"
STATE_TABULAR_TIMEOUT = "tabular_timeout"


def _to_preview(items: Sequence[str], *, fallback_en: str, fallback_ru: str, preferred_lang: str) -> str:
    preview = ", ".join([f"`{str(item).strip()}`" for item in items if str(item).strip()])
    if preview:
        return preview
    return fallback_ru if str(preferred_lang or "").lower().startswith("ru") else fallback_en


def _summarize_distribution(result_text: str) -> str:
    text = str(result_text or "").strip()
    if not text:
        return ""
    try:
        parsed = json.loads(text)
    except Exception:
        return ""
    if not isinstance(parsed, list) or not parsed:
        return ""
    first = parsed[0]
    if not isinstance(first, list) or len(first) < 2:
        return ""
    bucket = str(first[0]).strip()
    value = str(first[1]).strip()
    if not bucket:
        return ""
    if value:
        return f"Top bucket: `{bucket}` ({value})."
    return f"Top bucket: `{bucket}`."


def compose_controlled_response(
    *,
    state: str,
    preferred_lang: str,
    requested_fields: Sequence[str] | None = None,
    alternatives: Sequence[str] | None = None,
    missing_candidates: Sequence[str] | None = None,
    ambiguous_options: Mapping[str, Sequence[str]] | None = None,
    requested_field: str | None = None,
    chart_alternatives: Sequence[str] | None = None,
    column_label: str | None = None,
    source_scope: str | None = None,
    chart_fallback_reason: str | None = None,
    result_text: str | None = None,
    scope_kind: str | None = None,
    scope_options: Sequence[str] | None = None,
) -> str:
    requested_fields = list(requested_fields or [])
    alternatives = list(alternatives or [])
    missing_candidates = list(missing_candidates or [])
    ambiguous_options = dict(ambiguous_options or {})
    chart_alternatives = list(chart_alternatives or [])
    scope_options = list(scope_options or [])
    normalized_state = str(state or "").strip().lower()

    if normalized_state == STATE_NO_CONTEXT:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0412 \u044d\u0442\u043e\u043c \u0447\u0430\u0442\u0435 \u043d\u0435\u0442 \u0433\u043e\u0442\u043e\u0432\u044b\u0445 "
                "\u0444\u0430\u0439\u043b\u043e\u0432 \u0434\u043b\u044f \u043e\u0442\u0432\u0435\u0442\u0430 \u043f\u043e \u0434\u0430\u043d\u043d\u044b\u043c. "
                "\u041f\u0440\u0438\u043a\u0440\u0435\u043f\u0438\u0442\u0435 \u0444\u0430\u0439\u043b \u043a \u0447\u0430\u0442\u0443 "
                "\u0438\u043b\u0438 \u0443\u043a\u0430\u0436\u0438\u0442\u0435 \u0444\u0430\u0439\u043b \u043f\u043e \u0438\u043c\u0435\u043d\u0438, "
                "\u0438 \u044f \u043f\u0440\u043e\u0434\u043e\u043b\u0436\u0443."
            ),
            en=(
                "There are no ready files in this chat for file-based answering. "
                "Attach a file to this chat or reference a filename, and I will continue."
            ),
        )

    if normalized_state == STATE_FILE_NOT_FOUND:
        listed = ", ".join([f"`{item}`" for item in missing_candidates[:5]])
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"\u041d\u0435 \u043d\u0430\u0448\u0451\u043b \u0444\u0430\u0439\u043b(\u044b) {listed} "
                "\u0441\u0440\u0435\u0434\u0438 \u0432\u0430\u0448\u0438\u0445 \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u043d\u043d\u044b\u0445 "
                "\u0444\u0430\u0439\u043b\u043e\u0432. \u041f\u0440\u043e\u0432\u0435\u0440\u044c\u0442\u0435 \u0438\u043c\u044f "
                "\u0444\u0430\u0439\u043b\u0430 \u0438\u043b\u0438 \u0434\u043e\u0436\u0434\u0438\u0442\u0435\u0441\u044c "
                "\u0437\u0430\u0432\u0435\u0440\u0448\u0435\u043d\u0438\u044f \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0438."
            ),
            en=(
                f"I could not find file(s) {listed} among your processed files. "
                "Please verify the filename or wait until processing is complete."
            ),
        )

    if normalized_state == STATE_AMBIGUOUS_FILE:
        header = localized_text(
            preferred_lang=preferred_lang,
            ru="\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435, \u043a\u0430\u043a\u043e\u0439 \u0444\u0430\u0439\u043b \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u0442\u044c:",
            en="Please clarify which file should be used:",
        )
        lines = [header]
        for candidate, options in list(ambiguous_options.items())[:3]:
            lines.append(
                localized_text(
                    preferred_lang=preferred_lang,
                    ru=(
                        f"\u0414\u043b\u044f `{candidate}` \u043d\u0430\u0439\u0434\u0435\u043d\u043e "
                        "\u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u043e\u0432:"
                    ),
                    en=f"Multiple matches were found for `{candidate}`:",
                )
            )
            for idx, option in enumerate(list(options)[:5], start=1):
                lines.append(f"{idx}. {str(option)}")
        return "\n".join(lines).strip()

    if normalized_state == STATE_NO_RETRIEVAL:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043d\u0430\u0439\u0442\u0438 "
                "\u0440\u0435\u043b\u0435\u0432\u0430\u043d\u0442\u043d\u044b\u0435 \u0444\u0440\u0430\u0433\u043c\u0435\u043d\u0442\u044b "
                "\u0432 \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0445 \u0444\u0430\u0439\u043b\u0430\u0445 \u0434\u043b\u044f "
                "\u044d\u0442\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u0430. "
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435 \u0444\u043e\u0440\u043c\u0443\u043b\u0438\u0440\u043e\u0432\u043a\u0443, "
                "\u0444\u0438\u043b\u044c\u0442\u0440\u044b \u0438\u043b\u0438 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 "
                "\u043b\u0438\u0441\u0442\u0430/\u043a\u043e\u043b\u043e\u043d\u043a\u0438."
            ),
            en=(
                "No relevant chunks were found in the available files for this query. "
                "Please clarify wording, filters, or sheet/column name."
            ),
        )

    if normalized_state == STATE_SCOPE_AMBIGUITY:
        options_preview = _to_preview(
            scope_options,
            fallback_en="`no options available`",
            fallback_ru="`no options available`",
            preferred_lang=preferred_lang,
        )
        scope_label = str(scope_kind or "dataset scope").strip() or "dataset scope"
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"I found multiple possible {scope_label} matches. "
                f"Please pick one so I can run deterministic analysis: {options_preview}."
            ),
            en=(
                f"I found multiple possible {scope_label} matches. "
                f"Please pick one so I can run deterministic analysis: {options_preview}."
            ),
        )

    if normalized_state == STATE_MISSING_COLUMN or normalized_state == STATE_AMBIGUOUS_COLUMN:
        requested_preview = _to_preview(
            requested_fields,
            fallback_en="`required field`",
            fallback_ru="`\u043d\u0443\u0436\u043d\u043e\u0435 \u043f\u043e\u043b\u0435`",
            preferred_lang=preferred_lang,
        )
        alternatives_preview = _to_preview(
            alternatives,
            fallback_en="no suitable columns were detected",
            fallback_ru="\u043f\u043e\u0434\u0445\u043e\u0434\u044f\u0449\u0438\u0435 \u043a\u043e\u043b\u043e\u043d\u043a\u0438 \u043d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u044b",
            preferred_lang=preferred_lang,
        )
        if normalized_state == STATE_AMBIGUOUS_COLUMN:
            return localized_text(
                preferred_lang=preferred_lang,
                ru=(
                    "\u041d\u0430\u0439\u0434\u0435\u043d\u043e \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e "
                    f"\u0440\u0430\u0432\u043d\u043e\u0432\u0435\u0440\u043e\u044f\u0442\u043d\u044b\u0445 \u043a\u043e\u043b\u043e\u043d\u043e\u043a "
                    f"\u0434\u043b\u044f \u0437\u0430\u043f\u0440\u043e\u0441\u0430 ({requested_preview}). "
                    "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435 \u0442\u043e\u0447\u043d\u043e\u0435 \u043f\u043e\u043b\u0435. "
                    f"\u0414\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0435 \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u044b: {alternatives_preview}."
                ),
                en=(
                    f"Multiple columns matched the request ({requested_preview}) with similar confidence. "
                    f"Please clarify the exact field. Available options: {alternatives_preview}. "
                    f"Try one directly in your next query."
                ),
            )
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0412 \u0442\u0430\u0431\u043b\u0438\u0446\u0435 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e "
                f"\u0443\u0432\u0435\u0440\u0435\u043d\u043d\u043e\u0433\u043e \u0441\u043e\u043e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0438\u044f "
                f"\u0434\u043b\u044f \u043f\u043e\u043b\u044f ({requested_preview}). "
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043a\u043e\u043b\u043e\u043d\u043a\u0438. "
                f"\u0414\u043e\u0441\u0442\u0443\u043f\u043d\u044b\u0435 \u0432\u0430\u0440\u0438\u0430\u043d\u0442\u044b: {alternatives_preview}."
            ),
            en=(
                f"No confident schema match was found for field ({requested_preview}). "
                f"Please clarify the column name. Available options: {alternatives_preview}. "
                f"Try one directly in your next query."
            ),
        )

    if normalized_state == STATE_CHART_UNMATCHED_FIELD:
        requested = str(requested_field or "").strip() or "requested field"
        alternatives_preview = _to_preview(
            chart_alternatives,
            fallback_en="no close column matches",
            fallback_ru="no close column matches",
            preferred_lang=preferred_lang,
        )
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"The chart field '{requested}' was not matched to table columns. "
                f"Closest options: {alternatives_preview}. Please name one explicitly."
            ),
            en=(
                f"The chart field '{requested}' was not matched to table columns. "
                f"Closest options: {alternatives_preview}. Please name one explicitly."
            ),
        )

    if normalized_state == STATE_TABULAR_TIMEOUT:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0414\u0435\u0442\u0435\u0440\u043c\u0438\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0439 SQL-\u0437\u0430\u043f\u0440\u043e\u0441 "
                "\u043f\u0440\u0435\u0432\u044b\u0441\u0438\u043b \u043b\u0438\u043c\u0438\u0442 \u0432\u0440\u0435\u043c\u0435\u043d\u0438. "
                "\u0421\u0443\u0437\u044c\u0442\u0435 \u0444\u0438\u043b\u044c\u0442\u0440 \u0438\u043b\u0438 "
                "\u0443\u043c\u0435\u043d\u044c\u0448\u0438\u0442\u0435 scope \u0430\u043d\u0430\u043b\u0438\u0437\u0430 "
                "\u0438 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0437\u0430\u043f\u0440\u043e\u0441."
            ),
            en=(
                "Deterministic SQL execution timed out. "
                "Please narrow filters or reduce analysis scope and retry."
            ),
        )

    if normalized_state == STATE_TABULAR_EXECUTION_ERROR:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0414\u0435\u0442\u0435\u0440\u043c\u0438\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0439 SQL-\u0437\u0430\u043f\u0440\u043e\u0441 "
                "\u0431\u044b\u043b \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d \u043f\u043e\u043b\u0438\u0442\u0438\u043a\u043e\u0439 "
                "\u0438\u043b\u0438 \u043e\u0448\u0438\u0431\u043a\u043e\u0439 \u0432\u044b\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u044f. "
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435 \u043c\u0435\u0442\u0440\u0438\u043a\u0443/\u0444\u0438\u043b\u044c\u0442\u0440 "
                "\u0438 \u043f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0437\u0430\u043f\u0440\u043e\u0441."
            ),
            en=(
                "Deterministic SQL execution was blocked by policy or execution error. "
                "Please clarify metric/filter and retry."
            ),
        )

    if normalized_state == STATE_CHART_RENDER_SUCCESS:
        label = str(column_label or "field").strip() or "field"
        values = str(result_text or "").strip()
        top_bucket = _summarize_distribution(values)
        source_prefix = f"Source: {str(source_scope).strip()}. " if str(source_scope or "").strip() else ""
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"{source_prefix}\u0413\u0440\u0430\u0444\u0438\u043a \u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u044f "
                f"\u043f\u043e \u00ab{label}\u00bb \u0443\u0441\u043f\u0435\u0448\u043d\u043e \u043f\u043e\u0441\u0442\u0440\u043e\u0435\u043d "
                "\u0438 \u0434\u043e\u0441\u0442\u0443\u043f\u0435\u043d \u0432 \u0431\u043b\u043e\u043a\u0435 Charts. "
                f"{top_bucket} \u0414\u0430\u043d\u043d\u044b\u0435 \u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u044f: {values}"
            ),
            en=(
                f"{source_prefix}The distribution chart for '{label}' was generated and is available in Charts. "
                f"{top_bucket} Distribution data: {values}"
            ),
        )

    if normalized_state == STATE_CHART_RENDER_FAILED:
        reason = str(chart_fallback_reason or "chart_render_failed")
        values = str(result_text or "").strip()
        top_bucket = _summarize_distribution(values)
        source_prefix = f"Source: {str(source_scope).strip()}. " if str(source_scope or "").strip() else ""
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"{source_prefix}\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0434\u043e\u0441\u0442\u0430\u0432\u0438\u0442\u044c "
                "\u0438\u0437\u043e\u0431\u0440\u0430\u0436\u0435\u043d\u0438\u0435 \u0433\u0440\u0430\u0444\u0438\u043a\u0430, "
                f"\u043d\u043e \u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u0435 "
                f"\u0440\u0430\u0441\u0441\u0447\u0438\u0442\u0430\u043d\u043e \u043f\u043e \u0434\u0430\u043d\u043d\u044b\u043c "
                f"\u0442\u0430\u0431\u043b\u0438\u0446\u044b (reason={reason}). {top_bucket} "
                f"\u0414\u0430\u043d\u043d\u044b\u0435 \u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u044f: {values}"
            ),
            en=(
                f"{source_prefix}The chart image could not be delivered, but distribution data was computed "
                f"from the table (reason={reason}). {top_bucket} Distribution data: {values}"
            ),
        )

    if normalized_state == STATE_RUNTIME_ERROR:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u041e\u0448\u0438\u0431\u043a\u0430 \u0432\u043d\u0443\u0442\u0440\u0435\u043d\u043d\u0435\u0433\u043e runtime "
                "\u043f\u0440\u0438 \u0444\u043e\u0440\u043c\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0438 \u043e\u0442\u0432\u0435\u0442\u0430 "
                "\u043f\u043e \u0442\u0435\u043a\u0443\u0449\u0435\u043c\u0443 \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442\u0443 "
                "\u0444\u0430\u0439\u043b\u043e\u0432. \u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u0437\u0430\u043f\u0440\u043e\u0441."
            ),
            en=(
                "Internal runtime error while building an answer from the current file context. "
                "Please retry the request."
            ),
        )

    raise ValueError(f"Unsupported controlled response state: {state}")


def build_missing_column_message(
    *,
    preferred_lang: str,
    requested_fields: Sequence[str],
    alternatives: Sequence[str],
    ambiguous: bool = False,
) -> str:
    state = STATE_AMBIGUOUS_COLUMN if ambiguous else STATE_MISSING_COLUMN
    return compose_controlled_response(
        state=state,
        preferred_lang=preferred_lang,
        requested_fields=requested_fields,
        alternatives=alternatives,
    )


def build_chart_unmatched_field_message(
    *,
    preferred_lang: str,
    requested_field: str,
    alternatives: Sequence[str] | None = None,
) -> str:
    return compose_controlled_response(
        state=STATE_CHART_UNMATCHED_FIELD,
        preferred_lang=preferred_lang,
        requested_field=requested_field,
        chart_alternatives=list(alternatives or []),
    )


def build_timeout_message(*, preferred_lang: str) -> str:
    return compose_controlled_response(
        state=STATE_TABULAR_TIMEOUT,
        preferred_lang=preferred_lang,
    )


def build_execution_error_message(*, preferred_lang: str) -> str:
    return compose_controlled_response(
        state=STATE_TABULAR_EXECUTION_ERROR,
        preferred_lang=preferred_lang,
    )


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
    state = (
        STATE_CHART_RENDER_SUCCESS
        if bool(chart_rendered and chart_artifact_available)
        else STATE_CHART_RENDER_FAILED
    )
    return compose_controlled_response(
        state=state,
        preferred_lang=preferred_lang,
        column_label=column_label,
        source_scope=source_scope,
        chart_fallback_reason=chart_fallback_reason,
        result_text=result_text,
    )


def build_scope_clarification_message(
    *,
    preferred_lang: str,
    scope_kind: str,
    scope_options: Sequence[str],
) -> str:
    return compose_controlled_response(
        state=STATE_SCOPE_AMBIGUITY,
        preferred_lang=preferred_lang,
        scope_kind=scope_kind,
        scope_options=scope_options,
    )


def build_file_not_found_message(*, preferred_lang: str, missing_candidates: Sequence[str]) -> str:
    return compose_controlled_response(
        state=STATE_FILE_NOT_FOUND,
        preferred_lang=preferred_lang,
        missing_candidates=missing_candidates,
    )


def build_ambiguous_file_message(
    *,
    preferred_lang: str,
    ambiguous_options: Mapping[str, Sequence[str]],
) -> str:
    return compose_controlled_response(
        state=STATE_AMBIGUOUS_FILE,
        preferred_lang=preferred_lang,
        ambiguous_options=ambiguous_options,
    )


def build_no_context_message(*, preferred_lang: str) -> str:
    return compose_controlled_response(
        state=STATE_NO_CONTEXT,
        preferred_lang=preferred_lang,
    )


def build_no_retrieval_message(*, preferred_lang: str) -> str:
    return compose_controlled_response(
        state=STATE_NO_RETRIEVAL,
        preferred_lang=preferred_lang,
    )


def build_runtime_error_message(*, preferred_lang: str) -> str:
    return compose_controlled_response(
        state=STATE_RUNTIME_ERROR,
        preferred_lang=preferred_lang,
    )
