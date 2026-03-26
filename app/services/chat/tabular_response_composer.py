from __future__ import annotations

from typing import Sequence

from app.services.chat.language import localized_text


def build_missing_column_message(
    *,
    preferred_lang: str,
    requested_fields: Sequence[str],
    alternatives: Sequence[str],
    ambiguous: bool = False,
) -> str:
    requested_preview = ", ".join([f"`{str(item).strip()}`" for item in requested_fields if str(item).strip()])
    alternatives_preview = ", ".join([f"`{str(item).strip()}`" for item in alternatives if str(item).strip()])
    if not requested_preview:
        requested_preview = "`required field`" if preferred_lang == "en" else "`нужное поле`"
    if not alternatives_preview:
        alternatives_preview = (
            "no suitable columns were detected" if preferred_lang == "en" else "подходящие колонки не определены"
        )

    if ambiguous:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"Найдено несколько равновероятных колонок для запроса ({requested_preview}). "
                f"Уточните точное поле. Доступные варианты: {alternatives_preview}."
            ),
            en=(
                f"Multiple columns matched the request ({requested_preview}) with similar confidence. "
                f"Please clarify the exact field. Available options: {alternatives_preview}."
            ),
        )

    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            f"В таблице не найдено уверенного соответствия для поля ({requested_preview}). "
            f"Уточните название колонки. Доступные варианты: {alternatives_preview}."
        ),
        en=(
            f"No confident schema match was found for field ({requested_preview}). "
            f"Please clarify the column name. Available options: {alternatives_preview}."
        ),
    )


def build_chart_unmatched_field_message(*, preferred_lang: str, requested_field: str) -> str:
    requested = str(requested_field or "").strip() or "requested field"
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            f"Не удалось сопоставить поле графика '{requested}' с колонками таблицы. "
            "Уточните точное название колонки из схемы файла."
        ),
        en=(
            f"The chart field '{requested}' was not matched to table columns. "
            "Please clarify the exact column name from the file schema."
        ),
    )


def build_timeout_message(*, preferred_lang: str) -> str:
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "Детерминированный SQL-запрос превысил лимит времени. "
            "Сузьте фильтр или уменьшите scope анализа и повторите запрос."
        ),
        en=(
            "Deterministic SQL execution timed out. "
            "Please narrow filters or reduce analysis scope and retry."
        ),
    )


def build_execution_error_message(*, preferred_lang: str) -> str:
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "Детерминированный SQL-запрос был остановлен политикой или ошибкой выполнения. "
            "Уточните метрику/фильтр и повторите запрос."
        ),
        en=(
            "Deterministic SQL execution was blocked by policy or execution error. "
            "Please clarify metric/filter and retry."
        ),
    )


def build_chart_response_text(
    *,
    preferred_lang: str,
    column_label: str,
    chart_rendered: bool,
    chart_artifact_available: bool,
    chart_fallback_reason: str,
    result_text: str,
) -> str:
    rendered = bool(chart_rendered and chart_artifact_available)
    if rendered:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"График распределения по «{column_label}» успешно построен и доступен в блоке Charts. "
                f"Данные распределения: {result_text}"
            ),
            en=(
                f"The distribution chart for '{column_label}' was generated and is available in Charts. "
                f"Distribution data: {result_text}"
            ),
        )

    reason = str(chart_fallback_reason or "chart_render_failed")
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "Не удалось сформировать изображение графика, "
            f"но распределение рассчитано по данным таблицы (reason={reason}). "
            f"Данные распределения: {result_text}"
        ),
        en=(
            "The chart image could not be generated, "
            f"but distribution data was computed from the table (reason={reason}). "
            f"Distribution data: {result_text}"
        ),
    )


def build_no_context_tabular_message(*, preferred_lang: str) -> str:
    return localized_text(
        preferred_lang=preferred_lang,
        ru="В этом чате нет готового табличного файла для детерминированной аналитики.",
        en="There is no ready tabular file in this chat for deterministic analytics.",
    )
