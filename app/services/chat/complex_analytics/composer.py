from __future__ import annotations

import asyncio
import json
import logging
import re
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.services.chat.language import apply_language_policy_to_prompt, detect_preferred_response_language
from app.services.llm.manager import llm_manager

from .localization import (
    RU_ARTIFACT_KIND_MAP,
    RU_NOTE_MAP,
    RU_PROCESS_CONTEXT,
    RU_PURPOSE_HINTS,
    localize_en_to_ru,
)
from .planner import resolve_complex_analytics_routing
from .report_quality import is_compose_response_sufficient as evaluate_compose_response_sufficiency

logger = logging.getLogger(__name__)


@dataclass
class ComposeMeta:
    provider_source: str
    provider_mode: str
    response_status: str
    response_error_code: Optional[str]
    model_route: Optional[str]
    provider_effective: Optional[str]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "provider_source": self.provider_source,
            "provider_mode": self.provider_mode,
            "response_status": self.response_status,
            "response_error_code": self.response_error_code,
            "model_route": self.model_route,
            "provider_effective": self.provider_effective,
            "provider_source_selected": self.provider_source,
        }


def wants_python_code(query: str) -> bool:
    q = (query or "").lower()
    return any(token in q for token in ("python", "код", "script", "notebook", "пайтон", "питон"))


def is_russian_text(text: str) -> bool:
    return bool(re.search(r"[\u0400-\u04FF]", text or ""))


def format_complex_analytics_answer(
    *,
    query: str,
    table_name: str,
    metrics: Dict[str, Any],
    notes: Sequence[Any],
    artifacts: Sequence[Dict[str, Any]],
    executed_code: str,
    include_code: bool,
    insights: Optional[Sequence[Any]] = None,
) -> str:
    is_ru = is_russian_text(query)
    rows_total = int(metrics.get("rows_total", 0) or 0)
    columns_total = int(metrics.get("columns_total", 0) or 0)
    columns = metrics.get("columns") if isinstance(metrics.get("columns"), list) else []
    process_context = str(metrics.get("potential_process") or "").strip()
    if is_ru:
        process_context = localize_en_to_ru(
            process_context or "Likely an operational process dataset.",
            RU_PROCESS_CONTEXT,
        )
    elif not process_context:
        process_context = "Likely an operational process dataset."

    column_profile = metrics.get("column_profile") if isinstance(metrics.get("column_profile"), list) else []
    numeric_summary = metrics.get("numeric_summary") if isinstance(metrics.get("numeric_summary"), list) else []
    datetime_summary = metrics.get("datetime_summary") if isinstance(metrics.get("datetime_summary"), list) else []
    categorical_summary = metrics.get("categorical_summary") if isinstance(metrics.get("categorical_summary"), list) else []
    relationship_findings = metrics.get("relationship_findings") if isinstance(metrics.get("relationship_findings"), list) else []
    metric_insights = metrics.get("insights") if isinstance(metrics.get("insights"), list) else []
    all_insights = list(insights or []) + list(metric_insights)

    lines: List[str] = []
    if is_ru:
        lines.append("## Полный аналитический отчет")
        lines.append("### 1) Сводка")
        lines.append(f"- Таблица: `{table_name}`")
        lines.append(f"- Строк: **{rows_total}**")
        lines.append(f"- Колонок: **{columns_total}**")
        if columns:
            lines.append("- Список колонок: " + ", ".join([f"`{c}`" for c in columns]))
        lines.append("### 2) Контекст процесса")
        lines.append(f"- {process_context}")

        lines.append("### 3) Колонки и назначение")
        if column_profile:
            for item in column_profile[:24]:
                col = str(item.get("column") or "")
                purpose = localize_en_to_ru(str(item.get("purpose_hint") or ""), RU_PURPOSE_HINTS) or "не определено"
                non_null = int(item.get("non_null", 0) or 0)
                null_count = int(item.get("null_count", 0) or 0)
                unique_count = int(item.get("unique_count", 0) or 0)
                sample_values = item.get("sample_values") if isinstance(item.get("sample_values"), list) else []
                sample_text = ", ".join([str(x) for x in sample_values[:3]]) if sample_values else "-"
                lines.append(
                    f"- `{col}`: {purpose}; non_null={non_null}, null={null_count}, unique={unique_count}, sample={sample_text}"
                )
        else:
            lines.append("- Профили колонок не были возвращены исполнителем.")

        lines.append("### 4) Метрики и статистика")
        lines.append(f"- Числовых метрик: {len(numeric_summary)}")
        lines.append(f"- Дата/время метрик: {len(datetime_summary)}")
        lines.append(f"- Категориальных сводок: {len(categorical_summary)}")
        for item in numeric_summary[:8]:
            col = str(item.get("column") or "")
            min_v = item.get("min")
            max_v = item.get("max")
            mean_v = item.get("mean")
            median_v = item.get("median")
            lines.append(
                f"- Numeric `{col}`: min={min_v}, max={max_v}, mean={mean_v}, median={median_v}"
            )
        for item in datetime_summary[:6]:
            col = str(item.get("column") or "")
            lines.append(f"- Datetime `{col}`: {item.get('min')} .. {item.get('max')}")
        for item in categorical_summary[:6]:
            col = str(item.get("column") or "")
            top_values = item.get("top_values") if isinstance(item.get("top_values"), dict) else {}
            if top_values:
                preview = ", ".join([f"{k}={v}" for k, v in list(top_values.items())[:6]])
                lines.append(f"- Categorical `{col}` top values: {preview}")
        if relationship_findings:
            lines.append("### 5) Связи между признаками")
            for item in relationship_findings[:8]:
                a = str(item.get("feature_a") or "")
                b = str(item.get("feature_b") or "")
                corr = item.get("correlation")
                lines.append(f"- `{a}` <-> `{b}`: correlation={corr}")
        if all_insights:
            lines.append("### 6) Ключевые выводы")
            for insight in all_insights[:10]:
                insight_text = str(insight).strip()
                if insight_text:
                    lines.append(f"- {insight_text}")

        lines.append("### 7) Визуализации")
        if artifacts:
            for artifact in artifacts:
                kind = str(artifact.get("kind", "chart") or "chart")
                kind_ru = localize_en_to_ru(kind, RU_ARTIFACT_KIND_MAP)
                name = str(artifact.get("name", "") or "")
                path = str(artifact.get("path", "") or "")
                url = str(artifact.get("url", "") or "")
                ref = url or path or name
                lines.append(f"- {kind_ru}: `{name}` -> `{ref}`")
                if url:
                    lines.append(f"![{kind_ru}]({url})")
        else:
            lines.append("- Артефакты графиков не были созданы в этом запуске.")

        if notes:
            lines.append("### 8) Ограничения / заметки")
            for note in notes[:6]:
                note_text = str(note).strip()
                if note_text:
                    lines.append(f"- {localize_en_to_ru(note_text, RU_NOTE_MAP)}")

        if include_code:
            lines.append("### 9) Исполненный Python-код")
            lines.append("```python")
            lines.append(executed_code.strip())
            lines.append("```")
    else:
        lines.append("## Full Analytics Report")
        lines.append("### 1) Summary")
        lines.append(f"- Table: `{table_name}`")
        lines.append(f"- Rows: **{rows_total}**")
        lines.append(f"- Columns: **{columns_total}**")
        if columns:
            lines.append("- Column list: " + ", ".join([f"`{c}`" for c in columns]))
        lines.append("### 2) Likely Process Context")
        lines.append(f"- {process_context}")
        lines.append("### 3) Columns and Purpose")
        for item in column_profile[:24]:
            lines.append(f"- `{item.get('column')}`: {item.get('purpose_hint')}")
        lines.append("### 4) Metrics and Statistics")
        lines.append(f"- Numeric metrics count: {len(numeric_summary)}")
        lines.append(f"- Datetime metrics count: {len(datetime_summary)}")
        lines.append(f"- Categorical summaries count: {len(categorical_summary)}")
        for item in numeric_summary[:8]:
            col = str(item.get("column") or "")
            min_v = item.get("min")
            max_v = item.get("max")
            mean_v = item.get("mean")
            median_v = item.get("median")
            lines.append(f"- Numeric `{col}`: min={min_v}, max={max_v}, mean={mean_v}, median={median_v}")
        for item in datetime_summary[:6]:
            col = str(item.get("column") or "")
            lines.append(f"- Datetime `{col}`: {item.get('min')} .. {item.get('max')}")
        for item in categorical_summary[:6]:
            col = str(item.get("column") or "")
            top_values = item.get("top_values") if isinstance(item.get("top_values"), dict) else {}
            if top_values:
                preview = ", ".join([f"{k}={v}" for k, v in list(top_values.items())[:6]])
                lines.append(f"- Categorical `{col}` top values: {preview}")
        if relationship_findings:
            lines.append("### 5) Relationships Between Features")
            for item in relationship_findings[:8]:
                a = str(item.get("feature_a") or "")
                b = str(item.get("feature_b") or "")
                corr = item.get("correlation")
                lines.append(f"- `{a}` <-> `{b}`: correlation={corr}")
        if all_insights:
            lines.append("### 6) Key Insights")
            for insight in all_insights[:10]:
                insight_text = str(insight).strip()
                if insight_text:
                    lines.append(f"- {insight_text}")

        lines.append("### 7) Visualizations")
        if artifacts:
            for artifact in artifacts:
                kind = str(artifact.get("kind", "chart") or "chart")
                name = str(artifact.get("name", "") or "")
                path = str(artifact.get("path", "") or "")
                url = str(artifact.get("url", "") or "")
                ref = url or path or name
                lines.append(f"- {kind}: `{name}` -> `{ref}`")
                if url:
                    lines.append(f"![{kind}]({url})")
        else:
            lines.append("- No chart artifacts were created in this execution.")
        if notes:
            lines.append("### 8) Notes / Limitations")
            for note in notes[:6]:
                note_text = str(note).strip()
                if note_text:
                    lines.append(f"- {note_text}")
        if include_code:
            lines.append("### 9) Executed Python Code")
            lines.append("```python")
            lines.append(executed_code.strip())
            lines.append("```")

    return "\n".join(lines)


def truncate_for_prompt(value: Any, max_chars: int = 1000) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def build_complex_analytics_execution_context(
    *,
    query: str,
    table_name: str,
    metrics: Dict[str, Any],
    notes: Sequence[Any],
    artifacts: Sequence[Dict[str, Any]],
    executed_code: str,
    execution_stdout: str = "",
) -> Dict[str, Any]:
    column_profiles = metrics.get("column_profile") if isinstance(metrics.get("column_profile"), list) else []
    numeric_summary = metrics.get("numeric_summary") if isinstance(metrics.get("numeric_summary"), list) else []
    datetime_summary = metrics.get("datetime_summary") if isinstance(metrics.get("datetime_summary"), list) else []
    categorical_summary = metrics.get("categorical_summary") if isinstance(metrics.get("categorical_summary"), list) else []
    relationship_findings = metrics.get("relationship_findings") if isinstance(metrics.get("relationship_findings"), list) else []
    metric_insights = metrics.get("insights") if isinstance(metrics.get("insights"), list) else []
    return {
        "query": query,
        "table_name": table_name,
        "rows_total": int(metrics.get("rows_total", 0) or 0),
        "columns_total": int(metrics.get("columns_total", 0) or 0),
        "columns": [str(c) for c in (metrics.get("columns") or [])][:80],
        "process_context": metrics.get("potential_process"),
        "column_profile": column_profiles[:24],
        "numeric_summary": numeric_summary[:16],
        "datetime_summary": datetime_summary[:12],
        "categorical_summary": categorical_summary[:12],
        "relationship_findings": relationship_findings[:12],
        "insights": [str(x) for x in metric_insights[:12] if str(x).strip()],
        "notes": [str(n) for n in list(notes)[:12] if str(n).strip()],
        "raw_output": truncate_for_prompt(execution_stdout, max_chars=4000),
        "artifacts": [
            {
                "kind": artifact.get("kind"),
                "name": artifact.get("name"),
                "path": artifact.get("path"),
                "url": artifact.get("url"),
            }
            for artifact in artifacts
            if isinstance(artifact, dict)
        ],
        "code_preview": truncate_for_prompt(executed_code, max_chars=4200),
    }


def build_complex_analytics_response_prompt(
    *,
    execution_query: str,
    execution_context: Dict[str, Any],
) -> str:
    include_code = wants_python_code(execution_query)
    language = detect_preferred_response_language(execution_query)
    payload = json.dumps(execution_context, ensure_ascii=False, indent=2)
    include_code_clause = (
        "- Include the executed Python code in full at the end."
        if include_code
        else "- Do not include Python code unless explicitly asked."
    )
    prompt = textwrap.dedent(
        f"""
You are a senior data analyst assistant.
Given the executed sandbox output for a user request, generate the final user-facing report.

Requirements:
- Be practical, concise, and evidence-based.
- Mention only what is directly supported by the executed output.
- If visual artifacts are present, list them and include markdown image links.
- Use the stdout/diagnostics to explain caveats and potential limitations.
- Do not expose internal security sandbox details.
{include_code_clause}
- If the data does not support a requested analysis, state that clearly.
- Use markdown section headers for each report section.
- Include a dedicated "feature relationships" section when numeric/correlation signals are available.
- Avoid generic filler phrases such as "request processed" without analytical detail.

User request:
{execution_query}

Execution output (JSON):
{payload}

Return:
Return in this order:
1) Short confirmation that request was processed.
2) Data profile summary + key metrics.
3) Analytical interpretation / conclusions.
4) Visualizations (name, path/url, what they represent) + markdown image links.
5) Practical recommendations / next steps for deeper analysis.
""".strip()
    ).strip()
    return apply_language_policy_to_prompt(prompt=prompt, preferred_lang=language)


def is_compose_response_sufficient(
    *,
    text: str,
    query: str,
    execution_context: Dict[str, Any],
) -> bool:
    return evaluate_compose_response_sufficiency(
        text=text,
        query=query,
        execution_context=execution_context,
    )


async def compose_complex_analytics_response(
    *,
    query: str,
    table_name: str,
    metrics: Dict[str, Any],
    notes: Sequence[Any],
    artifacts: Sequence[Dict[str, Any]],
    executed_code: str,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    context = build_complex_analytics_execution_context(
        query=query,
        table_name=table_name,
        metrics=metrics,
        notes=notes,
        artifacts=artifacts,
        executed_code=executed_code,
        execution_stdout=metrics.get("stdout", ""),
    )
    prompt = build_complex_analytics_response_prompt(
        execution_query=query,
        execution_context=context,
    )
    timeout_seconds = float(getattr(settings, "COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS", 10.0) or 10.0)
    max_tokens = int(getattr(settings, "COMPLEX_ANALYTICS_RESPONSE_MAX_TOKENS", 1800) or 1800)
    routing = resolve_complex_analytics_routing(
        model_source=model_source,
        provider_mode=provider_mode,
    )
    selected_source = str(routing.get("model_source") or "local")
    selected_mode = str(routing.get("provider_mode") or "explicit")
    meta = ComposeMeta(
        provider_source=selected_source,
        provider_mode=selected_mode,
        response_status="not_attempted",
        response_error_code=None,
        model_route=None,
        provider_effective=None,
    )
    try:
        response = await asyncio.wait_for(
            llm_manager.generate_response(
                prompt=prompt,
                model_source=selected_source,
                provider_mode=selected_mode,
                model_name=model_name,
                temperature=0.25,
                max_tokens=max_tokens,
                conversation_history=None,
                cannot_wait=True,
                sla_critical=False,
                policy_class="complex_analytics_response",
            ),
            timeout=timeout_seconds,
        )
        text = str(response.get("response") or "").strip()
        if not text:
            meta.response_status = "fallback"
            meta.response_error_code = "empty_response"
            logger.info(
                "complex_analytics.compose status=fallback reason=empty_response provider=%s mode=%s",
                selected_source,
                selected_mode,
            )
            return "", meta.as_dict()
        if not is_compose_response_sufficient(text=text, query=query, execution_context=context):
            meta.response_status = "fallback"
            meta.response_error_code = "low_content_quality"
            logger.info(
                "complex_analytics.compose status=fallback reason=low_content_quality provider=%s mode=%s",
                selected_source,
                selected_mode,
            )
            return "", meta.as_dict()
        meta.response_status = "success"
        meta.model_route = response.get("model_route")
        meta.provider_effective = response.get("provider_effective")
        logger.info(
            "complex_analytics.compose status=success provider=%s model_route=%s",
            meta.provider_effective or selected_source,
            meta.model_route,
        )
        return text, meta.as_dict()
    except TimeoutError:
        meta.response_status = "error"
        meta.response_error_code = "timeout"
        logger.info(
            "complex_analytics.compose status=error reason=timeout provider=%s mode=%s",
            selected_source,
            selected_mode,
        )
        return "", meta.as_dict()
    except Exception as exc:  # pragma: no cover - provider/runtime dependent
        meta.response_status = "error"
        meta.response_error_code = f"runtime:{type(exc).__name__}"
        logger.warning("Complex analytics response composition failed: %s", exc)
        return "", meta.as_dict()


# Compatibility aliases.
_wants_python_code = wants_python_code
_is_russian_text = is_russian_text
_localize_en_to_ru = localize_en_to_ru
_format_complex_analytics_answer = format_complex_analytics_answer
_truncate_for_prompt = truncate_for_prompt
_build_complex_analytics_execution_context = build_complex_analytics_execution_context
_build_complex_analytics_response_prompt = build_complex_analytics_response_prompt
_is_compose_response_sufficient = is_compose_response_sufficient
_compose_complex_analytics_response = compose_complex_analytics_response
