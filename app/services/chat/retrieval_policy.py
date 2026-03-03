from __future__ import annotations

import math
import re
from typing import Any, Dict, Optional

from app.core.config import settings

TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_]+")
BROAD_HINTS = (
    "проанализ",
    "анализ",
    "свод",
    "итог",
    "обзор",
    "сравн",
    "тренд",
    "динам",
    "весь",
    "все строки",
    "по всему",
    "analyze",
    "analysis",
    "summary",
    "overview",
    "compare",
    "trend",
    "entire",
    "whole",
)


def _token_count(text: str) -> int:
    return len(TOKEN_RE.findall((text or "").lower()))


def classify_query_profile(query: str) -> str:
    q = (query or "").strip().lower()
    if not q:
        return "fact"

    if any(hint in q for hint in BROAD_HINTS):
        return "broad"

    if _token_count(q) <= int(settings.RAG_DYNAMIC_SHORT_QUERY_MAX_TOKENS):
        return "short"

    return "fact"


def build_retrieval_budget_plan(
    *,
    query: str,
    rag_mode: Optional[str],
    requested_top_k: int,
    expected_chunks_total: int,
) -> Dict[str, Any]:
    mode = (rag_mode or "auto").strip().lower() or "auto"
    expected = max(0, int(expected_chunks_total or 0))
    requested = max(1, int(requested_top_k or 1))
    profile = classify_query_profile(query)

    if mode == "full_file":
        effective = min(max(expected, requested), int(settings.RAG_FULL_FILE_MAX_CHUNKS))
        return {
            "mode": mode,
            "query_profile": profile,
            "requested_top_k": requested,
            "effective_top_k": effective,
            "ratio": 1.0,
            "dynamic_enabled": bool(settings.RAG_DYNAMIC_TOPK_ENABLED),
        }

    ratio = float(settings.RAG_DYNAMIC_TOPK_FACT_RATIO)
    if profile == "short":
        ratio = float(settings.RAG_DYNAMIC_TOPK_SHORT_RATIO)
    elif profile == "broad":
        ratio = float(settings.RAG_DYNAMIC_TOPK_BROAD_RATIO)

    if not bool(settings.RAG_DYNAMIC_TOPK_ENABLED):
        effective = requested
    elif expected > 0:
        effective = int(math.ceil(expected * ratio))
    else:
        effective = requested

    if expected > 0:
        effective = min(effective, expected)
    effective = max(effective, int(settings.RAG_DYNAMIC_TOPK_MIN), requested)
    effective = min(effective, int(settings.RAG_DYNAMIC_TOPK_MAX))

    return {
        "mode": mode,
        "query_profile": profile,
        "requested_top_k": requested,
        "effective_top_k": int(effective),
        "ratio": float(ratio),
        "dynamic_enabled": bool(settings.RAG_DYNAMIC_TOPK_ENABLED),
    }


def choose_escalation_plan(
    *,
    rag_mode: Optional[str],
    expected_chunks_total: int,
    current_top_k: int,
    coverage_ratio: float,
) -> Optional[Dict[str, Any]]:
    if not bool(settings.RAG_DYNAMIC_ESCALATION_ENABLED):
        return None

    mode = (rag_mode or "auto").strip().lower() or "auto"
    if mode == "full_file":
        return None

    expected = max(0, int(expected_chunks_total or 0))
    if expected <= 0:
        return None

    threshold = float(settings.RAG_DYNAMIC_COVERAGE_MIN_RATIO)
    if float(coverage_ratio) >= threshold:
        return None

    if mode == "auto" and expected <= int(settings.RAG_DYNAMIC_ESCALATE_TO_FULL_FILE_MAX_CHUNKS):
        next_top_k = min(expected, int(settings.RAG_FULL_FILE_MAX_CHUNKS))
        return {
            "enabled": True,
            "reason": "low_coverage_small_doc_switch_full_file",
            "next_mode": "full_file",
            "next_top_k": int(next_top_k),
            "coverage_ratio": float(coverage_ratio),
            "coverage_threshold": threshold,
        }

    multiplier = float(settings.RAG_DYNAMIC_ESCALATION_MULTIPLIER)
    next_top_k = int(math.ceil(max(1, current_top_k) * multiplier))
    next_top_k = min(next_top_k, int(settings.RAG_DYNAMIC_ESCALATION_MAX_TOPK), expected)
    if next_top_k <= int(current_top_k):
        return None

    return {
        "enabled": True,
        "reason": "low_coverage_increase_top_k",
        "next_mode": mode,
        "next_top_k": int(next_top_k),
        "coverage_ratio": float(coverage_ratio),
        "coverage_threshold": threshold,
    }
