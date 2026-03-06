from __future__ import annotations

import re
from typing import Any, Dict, Sequence


BROAD_ANALYSIS_HINTS = (
    "senior analytic",
    "senior analyst",
    "analyze file fully",
    "analyze this file fully",
    "full analysis",
    "complete analysis",
    "feature relationship",
    "feature relation",
    "distribution",
    "statistics",
    "полный анализ",
    "полностью",
    "проанализируй файл полностью",
    "отнош",
    "связ",
    "распредел",
    "статистик",
    "график",
)

LOW_QUALITY_PHRASES = (
    "запрос был обработан",
    "сообщение об обработке запроса",
    "request was processed",
    "message about request processing",
    "прactical",
    "deeper анализа",
)


def is_russian_text(text: str) -> bool:
    return bool(re.search(r"[\u0400-\u04FF]", text or ""))


def is_broad_full_analysis_query(query: str) -> bool:
    q = str(query or "").strip().lower()
    if not q:
        return False
    return any(hint in q for hint in BROAD_ANALYSIS_HINTS)


def _cyrillic_ratio(text: str) -> float:
    letters = [ch for ch in str(text or "") if ch.isalpha()]
    if not letters:
        return 0.0
    cyrillic = [ch for ch in letters if re.match(r"[\u0400-\u04FF]", ch)]
    return float(len(cyrillic)) / float(len(letters))


def _has_structured_sections(text: str) -> bool:
    candidate = str(text or "")
    markdown_headers = re.findall(r"^\s{0,3}#{1,4}\s+\S+", candidate, flags=re.MULTILINE)
    if len(markdown_headers) >= 4:
        return True
    colon_headers = re.findall(r"^[A-Za-zА-Яа-я][^:\n]{3,80}:\s*$", candidate, flags=re.MULTILINE)
    return len(colon_headers) >= 4


def _contains_relationship_signal(text: str, *, is_ru: bool) -> bool:
    lower = str(text or "").lower()
    if is_ru:
        return any(token in lower for token in ("связ", "завис", "коррел", "взаимосвяз"))
    return any(token in lower for token in ("relationship", "dependency", "correlation", "feature interaction"))


def _contains_metric_signal(text: str, *, is_ru: bool) -> bool:
    lower = str(text or "").lower()
    if is_ru:
        return any(token in lower for token in ("строк", "колон", "метрик", "статист"))
    return any(token in lower for token in ("rows", "columns", "metric", "statistic"))


def _contains_visual_signal(text: str, *, artifacts_present: bool) -> bool:
    lower = str(text or "").lower()
    if not artifacts_present:
        return any(token in lower for token in ("visual", "chart", "plot", "граф", "диаграм", "визуал"))
    # If we already have artifacts, enforce explicit link/image references in final response.
    has_artifact_link = "/uploads/" in lower or "http://" in lower or "https://" in lower
    has_markdown_image = "![" in text and "](" in text
    return bool(has_artifact_link and has_markdown_image)


def is_compose_response_sufficient(
    *,
    text: str,
    query: str,
    execution_context: Dict[str, Any],
) -> bool:
    candidate = str(text or "").strip()
    if len(candidate) < 260:
        return False

    lower = candidate.lower()
    if any(phrase in lower for phrase in LOW_QUALITY_PHRASES):
        return False
    if not _has_structured_sections(candidate):
        return False

    is_ru = is_russian_text(query)
    if is_ru and _cyrillic_ratio(candidate) < 0.45:
        return False
    if not _contains_metric_signal(candidate, is_ru=is_ru):
        return False

    artifacts = execution_context.get("artifacts")
    artifacts_present = isinstance(artifacts, list) and len(artifacts) > 0
    if not _contains_visual_signal(candidate, artifacts_present=artifacts_present):
        return False

    if is_broad_full_analysis_query(query):
        if not _contains_relationship_signal(candidate, is_ru=is_ru):
            return False

    return True


def build_local_formatter_meta(reason: str) -> Dict[str, str]:
    return {
        "response_status": "fallback",
        "response_error_code": str(reason or "local_formatter"),
    }

