from __future__ import annotations

import ast
import asyncio
import builtins
import json
import logging
import re
import shutil
import sqlite3
import textwrap
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter, time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.services.llm.manager import llm_manager
from app.services.tabular.sql_execution import (
    ResolvedTabularDataset,
    ResolvedTabularTable,
    resolve_tabular_dataset,
)

logger = logging.getLogger(__name__)

COMPLEX_ANALYTICS_ERROR_SECURITY = "security_violation"
COMPLEX_ANALYTICS_ERROR_TIMEOUT = "timeout"
COMPLEX_ANALYTICS_ERROR_RUNTIME = "runtime_error"
COMPLEX_ANALYTICS_ERROR_DATASET = "dataset_unavailable"
COMPLEX_ANALYTICS_ERROR_DEPENDENCY = "dependency_missing"
COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT = "output_limit_exceeded"

_COMPLEX_ANALYTICS_HINTS = (
    "python",
    "pandas",
    "numpy",
    "duckdb",
    "seaborn",
    "matplotlib",
    "heatmap",
    "plot",
    "chart",
    "visualization",
    "visualise",
    "visualize",
    "nlp",
    "comment_text",
    "tokenize",
    "lemmat",
    "sentiment",
    "dataframe",
    "multi-step",
    "multi step",
    "пайтон",
    "питон",
    "визуализац",
    "график",
    "хитмап",
    "теплов",
    "npl",
)
_ALLOWED_IMPORT_ROOTS = {
    "pandas",
    "numpy",
    "duckdb",
    "matplotlib",
    "seaborn",
    "datetime",
    "re",
    "warnings",
}
_BLOCKED_IMPORT_ROOTS = {
    "os",
    "subprocess",
    "socket",
    "requests",
    "httpx",
    "urllib",
    "pathlib",
    "shutil",
    "ftplib",
    "paramiko",
}
_BLOCKED_CALL_NAMES = {
    "open",
    "eval",
    "exec",
    "compile",
    "input",
    "__import__",
    "breakpoint",
    "getattr",
    "setattr",
    "delattr",
    "globals",
    "locals",
    "vars",
}
_BLOCKED_ATTRIBUTE_ROOTS = {
    "os",
    "subprocess",
    "socket",
    "requests",
    "httpx",
    "urllib",
    "pathlib",
    "shutil",
}
_BLOCKED_ATTRIBUTE_CHAINS = {
    "os.system",
    "os.popen",
    "subprocess.run",
    "subprocess.Popen",
    "subprocess.call",
    "socket.socket",
    "requests.get",
    "requests.post",
    "httpx.get",
    "httpx.post",
}
_NETWORK_LITERAL_PATTERN = re.compile(r"https?://|ftp://", flags=re.IGNORECASE)
_CODE_BLOCK_PATTERN = re.compile(r"```(?:python|py)?\s*(.*?)```", flags=re.IGNORECASE | re.DOTALL)
_MAX_CODE_LINES = 520
_CODEGEN_TABLE_SAMPLE_ROWS = 6
_CODEGEN_COLUMN_SAMPLE_VALUES = 4


class ComplexAnalyticsSecurityError(Exception):
    pass


class ComplexAnalyticsOutputLimitError(Exception):
    pass


@dataclass
class SandboxExecutionResult:
    result: Dict[str, Any]
    stdout: str
    artifacts: List[Dict[str, Any]]


def is_complex_analytics_query(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    if any(hint in q for hint in _COMPLEX_ANALYTICS_HINTS):
        return True
    multi_step_signals = ("then", "after that", "step 1", "step 2", "сначала", "затем", "после этого")
    if any(signal in q for signal in multi_step_signals):
        return True
    return False


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _to_safe_filename(name: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(name or "artifact")).strip("._")
    if not stem:
        stem = "artifact"
    if not stem.lower().endswith(".png"):
        stem = f"{stem}.png"
    return stem


def _artifact_public_url(path_value: str) -> Optional[str]:
    if not path_value:
        return None
    try:
        artifact_path = Path(str(path_value)).expanduser().resolve()
        uploads_root = Path("uploads").resolve()
        relative = artifact_path.relative_to(uploads_root)
        return "/uploads/" + "/".join(relative.parts)
    except Exception:
        return None


def _artifact_relative_path(path_value: str) -> Optional[str]:
    if not path_value:
        return None
    try:
        artifact_path = Path(str(path_value)).expanduser().resolve()
        uploads_root = Path("uploads").resolve()
        relative = artifact_path.relative_to(uploads_root)
        return "uploads/" + "/".join(relative.parts)
    except Exception:
        return None


def _sanitize_artifact_for_response(raw_artifact: Dict[str, Any]) -> Dict[str, Any]:
    artifact = dict(raw_artifact or {})
    path_value = str(artifact.get("path") or "")
    if path_value:
        relative_path = _artifact_relative_path(path_value)
        if relative_path:
            artifact["path"] = relative_path
        else:
            artifact.pop("path", None)
    url_value = str(artifact.get("url") or "")
    if not url_value and path_value:
        public_url = _artifact_public_url(path_value)
        if public_url:
            artifact["url"] = public_url
    return artifact


def _cleanup_complex_analytics_artifacts(*, artifacts_root: Path) -> Dict[str, int]:
    deleted = 0
    failed = 0
    ttl_hours = int(getattr(settings, "COMPLEX_ANALYTICS_ARTIFACT_TTL_HOURS", 168) or 168)
    max_run_dirs = int(getattr(settings, "COMPLEX_ANALYTICS_ARTIFACT_MAX_RUN_DIRS", 2000) or 2000)
    try:
        artifacts_root.mkdir(parents=True, exist_ok=True)
        now_ts = time()
        ttl_seconds = max(3600, ttl_hours * 3600)
        directories = [p for p in artifacts_root.iterdir() if p.is_dir()]
        directories.sort(key=lambda p: p.stat().st_mtime)

        stale = [p for p in directories if (now_ts - p.stat().st_mtime) > ttl_seconds]
        for path in stale:
            try:
                shutil.rmtree(path, ignore_errors=False)
                deleted += 1
            except Exception:
                failed += 1

        remaining = [p for p in artifacts_root.iterdir() if p.is_dir()]
        remaining.sort(key=lambda p: p.stat().st_mtime)
        overflow = max(0, len(remaining) - max_run_dirs)
        for path in remaining[:overflow]:
            try:
                shutil.rmtree(path, ignore_errors=False)
                deleted += 1
            except Exception:
                failed += 1
    except Exception:
        failed += 1

    if deleted:
        inc_counter("complex_analytics_artifacts_cleanup_total", value=deleted, status="deleted")
    if failed:
        inc_counter("complex_analytics_artifacts_cleanup_total", value=failed, status="failed")
    return {"deleted": deleted, "failed": failed}


def _resolve_table_for_query(*, query: str, dataset: ResolvedTabularDataset) -> Optional[ResolvedTabularTable]:
    if not dataset.tables:
        return None
    q = (query or "").lower()
    for table in dataset.tables:
        if table.table_name.lower() in q or table.sheet_name.lower() in q:
            return table
    return max(dataset.tables, key=lambda t: int(t.row_count or 0))


def _validate_python_security(code: str) -> None:
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:  # pragma: no cover - syntax errors are rare in our generated templates
        raise ComplexAnalyticsSecurityError(f"Invalid python syntax: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = str(alias.name or "").split(".", 1)[0]
                if root in _BLOCKED_IMPORT_ROOTS:
                    raise ComplexAnalyticsSecurityError(f"Import blocked: {alias.name}")
                if root not in _ALLOWED_IMPORT_ROOTS:
                    raise ComplexAnalyticsSecurityError(f"Import not allowed: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module_root = str(node.module or "").split(".", 1)[0]
            if module_root in _BLOCKED_IMPORT_ROOTS:
                raise ComplexAnalyticsSecurityError(f"Import blocked: {node.module}")
            if module_root and module_root not in _ALLOWED_IMPORT_ROOTS:
                raise ComplexAnalyticsSecurityError(f"Import not allowed: {node.module}")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if str(node.func.id or "") in _BLOCKED_CALL_NAMES:
                    raise ComplexAnalyticsSecurityError(f"Call blocked: {node.func.id}")
            elif isinstance(node.func, ast.Attribute):
                chain = _attribute_chain(node.func)
                if any(part.startswith("__") for part in chain.split(".")):
                    raise ComplexAnalyticsSecurityError(f"Dunder attribute access blocked: {chain}")
                if chain in _BLOCKED_ATTRIBUTE_CHAINS:
                    raise ComplexAnalyticsSecurityError(f"Call blocked: {chain}")
                root = chain.split(".", 1)[0]
                if root in _BLOCKED_ATTRIBUTE_ROOTS:
                    raise ComplexAnalyticsSecurityError(f"Call blocked: {chain}")
        elif isinstance(node, ast.Attribute):
            if str(node.attr or "").startswith("__"):
                raise ComplexAnalyticsSecurityError(f"Dunder attribute access blocked: {node.attr}")
        elif isinstance(node, ast.Name):
            if str(node.id or "").startswith("__"):
                raise ComplexAnalyticsSecurityError(f"Name blocked: {node.id}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if _NETWORK_LITERAL_PATTERN.search(node.value):
                raise ComplexAnalyticsSecurityError("Network URL literals are not allowed in sandbox code")


def _attribute_chain(node: ast.Attribute) -> str:
    chunks = [str(node.attr or "")]
    value = node.value
    while isinstance(value, ast.Attribute):
        chunks.append(str(value.attr or ""))
        value = value.value
    if isinstance(value, ast.Name):
        chunks.append(str(value.id or ""))
    chunks.reverse()
    return ".".join([item for item in chunks if item])


def execute_sandboxed_python(
    *,
    code: str,
    datasets: Dict[str, Any],
    artifacts_dir: Path,
    max_output_chars: int,
    max_artifacts: int,
) -> SandboxExecutionResult:
    _validate_python_security(code)

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    artifacts: List[Dict[str, Any]] = []
    output_parts: List[str] = []
    output_size = 0

    def _safe_print(*args: Any, **kwargs: Any) -> None:
        nonlocal output_size
        _ = kwargs
        text = " ".join(str(arg) for arg in args).strip()
        if not text:
            return
        remaining = int(max_output_chars) - output_size
        if remaining <= 0:
            raise ComplexAnalyticsOutputLimitError("stdout limit exceeded")
        chunk = text[:remaining]
        output_parts.append(chunk)
        output_size += len(chunk)
        if len(text) > len(chunk):
            raise ComplexAnalyticsOutputLimitError("stdout limit exceeded")

    def _save_plot(*, fig=None, name: str = "chart.png") -> str:  # noqa: ANN001
        if len(artifacts) >= int(max_artifacts):
            raise ComplexAnalyticsOutputLimitError("artifact limit exceeded")
        target_name = _to_safe_filename(name)
        target_path = (artifacts_dir / target_name).resolve()
        if not str(target_path).startswith(str(artifacts_dir.resolve())):
            raise ComplexAnalyticsSecurityError("Artifact path escape detected")
        if fig is None:
            try:
                import matplotlib.pyplot as plt  # noqa: PLC0415

                fig = plt.gcf()
            except Exception as exc:  # pragma: no cover - runtime environment dependent
                raise ComplexAnalyticsSecurityError("matplotlib is required to save plots") from exc
        fig.savefig(target_path, dpi=150, bbox_inches="tight")
        artifacts.append(
            {
                "name": target_name,
                "path": str(target_path),
                "content_type": "image/png",
            }
        )
        return str(target_path)

    safe_builtins = {
        # Keep builtin importer for internal dependency imports (e.g. pandas -> stdlib modules).
        # Explicit user imports are restricted by AST checks in _validate_python_security.
        "__import__": builtins.__import__,
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "dict": dict,
        "enumerate": enumerate,
        "float": float,
        "int": int,
        "len": len,
        "list": list,
        "max": max,
        "min": min,
        "print": _safe_print,
        "range": range,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "zip": zip,
    }

    exec_namespace: Dict[str, Any] = {
        "__builtins__": safe_builtins,
        "datasets": datasets,
        "save_plot": _save_plot,
    }
    compiled = compile(code, "<complex_analytics_sandbox>", "exec")
    # Use a single namespace to keep variable resolution stable inside comprehensions.
    exec(compiled, exec_namespace, exec_namespace)

    raw_result = exec_namespace.get("result")
    if raw_result is None:
        raw_result = {"status": "ok", "notes": "No explicit result payload returned by script"}
    if not isinstance(raw_result, dict):
        raw_result = {"status": "ok", "value": str(raw_result)}

    serialized = json.dumps(raw_result, ensure_ascii=False, default=str)
    if len(serialized) > int(max_output_chars):
        raise ComplexAnalyticsOutputLimitError("result payload too large")
    return SandboxExecutionResult(result=raw_result, stdout="\n".join(output_parts), artifacts=artifacts)


def _wants_python_code(query: str) -> bool:
    q = (query or "").lower()
    return any(token in q for token in ("python", "код", "script", "notebook", "пайтон", "питон"))


def _is_russian_text(text: str) -> bool:
    return bool(re.search(r"[\u0400-\u04FF]", text or ""))

_RU_PURPOSE_HINTS = {
    "identifier/key column": "идентификатор/ключ",
    "time/event timestamp": "временная метка события",
    "free-text/narrative field": "текстовое поле (комментарии/описание)",
    "organizational or location dimension": "измерение локации/офиса",
    "process state dimension": "измерение статуса процесса",
    "financial metric field": "финансовая метрика",
    "volume/count metric field": "количественная метрика",
    "attribute used for segmentation/analysis": "атрибут для сегментации/аналитики",
}

_RU_PROCESS_CONTEXT = {
    "Application review / processing workflow with distributed offices and analyst comments.": (
        "Процесс обработки заявок с распределением по офисам и комментариями аналитиков."
    ),
    "Order-to-cash / document processing workflow.": "Операционный процесс order-to-cash / обработки документов.",
    "Support or incident management workflow.": "Процесс поддержки или управления инцидентами.",
    "Likely an operational process dataset with records, dimensions, and process indicators.": (
        "Вероятно, это операционный процессный датасет с фактами, измерениями и индикаторами процесса."
    ),
    "Likely an operational process dataset.": "Вероятно, это операционный процессный датасет.",
}

_RU_NOTE_MAP = {
    "comment_time exists but could not be parsed to datetime": (
        "Колонка comment_time найдена, но не удалось корректно распознать формат даты/времени."
    ),
    "NLP requested but no tokens extracted from comment_text": (
        "Запрошен NLP-анализ, но из comment_text не удалось извлечь токены."
    ),
    "NLP requested but comment_text column was not found": (
        "Запрошен NLP-анализ, но колонка comment_text не найдена."
    ),
    "Visualization requested but no suitable columns were found for charting.": (
        "Запрошена визуализация, но не найдены подходящие колонки для построения графика."
    ),
}

_RU_ARTIFACT_KIND_MAP = {
    "heatmap": "тепловая карта",
    "histogram": "гистограмма",
    "categorical_bar": "категориальная диаграмма",
    "dependency_bar": "диаграмма зависимости",
    "correlation_heatmap": "корреляционная тепловая карта",
    "scatter": "диаграмма рассеяния",
    "chart": "график",
}


def _localize_en_to_ru(value: str, mapping: Dict[str, str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return mapping.get(text, text)


def _extract_python_from_llm_text(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    matches = _CODE_BLOCK_PATTERN.findall(raw)
    candidate = max(matches, key=lambda item: len(str(item or ""))) if matches else raw
    code = str(candidate or "").replace("\r\n", "\n").strip()
    if code.lower().startswith("python\n"):
        code = code.split("\n", 1)[1]
    return code.strip()


def _is_dependency_query(query: str) -> bool:
    q = (query or "").lower()
    dependency_hints = (
        "dependenc",
        "relationship",
        "correlation",
        "correl",
        "heatmap",
        "pairplot",
        "scatter",
        "зависим",
        "связ",
        "коррел",
        "теплов",
        "диаграмм",
    )
    return any(hint in q for hint in dependency_hints)


def _build_dataframe_profile_for_codegen(df: Any) -> Dict[str, Any]:
    sample_rows: List[Dict[str, Any]] = []
    try:
        sample_rows = [
            {str(k): str(v) for k, v in row.items()}
            for row in df.head(_CODEGEN_TABLE_SAMPLE_ROWS).to_dict(orient="records")
        ]
    except Exception:
        sample_rows = []
    profile: Dict[str, Any] = {
        "rows_total": int(len(df)),
        "columns_total": int(len(getattr(df, "columns", []))),
        "columns": [],
        "numeric_columns": [],
        "datetime_like_columns": [],
        "categorical_columns": [],
        "sample_rows": sample_rows,
    }
    columns = list(getattr(df, "columns", []))
    for column in columns[:80]:
        series = df[column]
        clean = series.dropna()
        sampled = clean.head(2500)
        sample_values = [str(v) for v in clean.astype(str).head(_CODEGEN_COLUMN_SAMPLE_VALUES).tolist()]
        dtype_text = str(getattr(series, "dtype", ""))
        entry: Dict[str, Any] = {
            "name": str(column),
            "dtype": dtype_text,
            "non_null": int(clean.shape[0]),
            "sample_values": sample_values,
            "sample_unique": int(sampled.nunique(dropna=True)),
        }
        column_lower = str(column).lower()
        dtype_lower = dtype_text.lower()
        if any(token in dtype_lower for token in ("int", "float", "double", "decimal")):
            profile["numeric_columns"].append(str(column))
        elif any(token in dtype_lower for token in ("datetime", "date", "timestamp")):
            profile["datetime_like_columns"].append(str(column))
        elif any(token in column_lower for token in ("date", "time", "created", "updated", "comment_time")):
            profile["datetime_like_columns"].append(str(column))
        elif entry["sample_unique"] <= 80:
            profile["categorical_columns"].append(str(column))
        profile["columns"].append(entry)
    return profile


def _build_codegen_prompt(
    *,
    query: str,
    primary_table_name: str,
    dataframe_profile: Dict[str, Any],
) -> str:
    profile_json = json.dumps(dataframe_profile, ensure_ascii=False)
    profile_snippet = profile_json[:14000]
    chart_required = _is_dependency_query(query) or any(
        token in (query or "").lower()
        for token in ("plot", "chart", "visual", "граф", "визуал", "heatmap", "теплов")
    )
    return textwrap.dedent(
        f"""
        You are generating offline Python code for a secure analytics sandbox.
        Return ONLY Python code (no markdown, no explanations).

        Runtime contract:
        - Input dataframe: df = datasets[{json.dumps(primary_table_name)}].copy()
        - Available libs: pandas, numpy, duckdb, matplotlib, seaborn, datetime, re, warnings
        - Forbidden behavior: subprocess, network, file reads/writes, OS/system calls, eval/exec/open
        - Use save_plot(fig=..., name="...png") to persist chart artifacts.
        - Create variable `result` as dict with keys:
          status, table_name, metrics (dict), notes (list), artifacts (list)
        - Always fill:
          result["metrics"]["rows_total"], ["columns_total"], ["columns"]
        - If charts are requested, generate 1-3 meaningful charts and append to:
          result["artifacts"].append({{"kind":"<type>", "path": plot_path, "title":"..."}})
        - For dependency analysis requests:
          Prefer correlation heatmap (if >=2 numeric columns),
          else numeric-vs-category dependency chart,
          else categorical dependency heatmap from crosstab.
        - Chart requirement: {"required" if chart_required else "optional"}.
        - Keep script under {_MAX_CODE_LINES} lines.

        User request:
        {query}

        Data profile (JSON):
        {profile_snippet}
        """
    ).strip()


def _validate_generated_code_contract(code: str) -> Optional[str]:
    candidate = str(code or "").strip()
    if not candidate:
        return "empty_code"
    if len(candidate.splitlines()) > _MAX_CODE_LINES:
        return "code_too_long"
    try:
        tree = ast.parse(candidate)
    except SyntaxError:
        return "syntax_error"
    has_result_assign = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and str(target.id or "") == "result":
                    has_result_assign = True
                    break
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if str(node.target.id or "") == "result":
                has_result_assign = True
    if not has_result_assign:
        return "missing_result_assignment"
    if "datasets" not in candidate:
        return "missing_datasets_access"
    try:
        _validate_python_security(candidate)
    except ComplexAnalyticsSecurityError:
        return "security_precheck_failed"
    return None


async def _generate_complex_analysis_code(
    *,
    query: str,
    primary_table_name: str,
    primary_frame: Any,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    fallback_code = _build_complex_analysis_code(query=query, primary_table_name=primary_table_name)
    meta: Dict[str, Any] = {
        "codegen_enabled": bool(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_ENABLED", True)),
        "codegen_attempted": False,
        "codegen_status": "disabled",
        "code_source": "template",
        "codegen_error": None,
        "provider_selected": str(model_source or ""),
        "provider_mode": str(provider_mode or ""),
        "model_name": str(model_name or ""),
    }

    if not meta["codegen_enabled"]:
        return fallback_code, meta

    profile = _build_dataframe_profile_for_codegen(primary_frame)
    prompt = _build_codegen_prompt(
        query=query,
        primary_table_name=primary_table_name,
        dataframe_profile=profile,
    )
    timeout_seconds = float(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS", 8.0) or 8.0)
    max_tokens = int(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_MAX_TOKENS", 2200) or 2200)
    force_local = bool(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL", True))
    codegen_source = str(model_source or "local").strip().lower() or "local"
    if force_local:
        codegen_source = "local"

    meta.update(
        {
            "codegen_attempted": True,
            "codegen_status": "attempted",
            "provider_effective": codegen_source,
            "provider_overridden": bool(force_local),
        }
    )

    try:
        llm_result = await asyncio.wait_for(
            llm_manager.generate_response(
                prompt=prompt,
                model_source=codegen_source,
                provider_mode="explicit" if force_local else provider_mode,
                model_name=model_name,
                temperature=0.05,
                max_tokens=max_tokens,
                conversation_history=None,
                cannot_wait=True,
                sla_critical=False,
                policy_class="complex_analytics_codegen",
            ),
            timeout=timeout_seconds,
        )
        candidate = _extract_python_from_llm_text(str(llm_result.get("response") or ""))
        contract_error = _validate_generated_code_contract(candidate)
        if contract_error:
            meta["codegen_status"] = "fallback"
            meta["codegen_error"] = contract_error
            inc_counter("complex_analytics_codegen_total", status="fallback", reason=contract_error)
            return fallback_code, meta

        meta["codegen_status"] = "success"
        meta["code_source"] = "llm"
        meta["model_route"] = llm_result.get("model_route")
        meta["provider_effective_runtime"] = llm_result.get("provider_effective")
        inc_counter("complex_analytics_codegen_total", status="success", reason="none")
        return candidate, meta
    except TimeoutError:
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = "timeout"
        inc_counter("complex_analytics_codegen_total", status="fallback", reason="timeout")
        return fallback_code, meta
    except Exception as exc:  # pragma: no cover - provider/runtime dependent
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = f"runtime_error:{type(exc).__name__}"
        logger.warning("Complex analytics codegen failed: %s", exc)
        inc_counter("complex_analytics_codegen_total", status="fallback", reason="runtime_error")
        return fallback_code, meta


def _build_complex_analysis_code(*, query: str, primary_table_name: str) -> str:
    q_lower = (query or "").lower()
    needs_visual = True
    needs_nlp = any(
        token in q_lower
        for token in (
            "nlp",
            "comment_text",
            "text",
            "\u043a\u043e\u043c\u043c\u0435\u043d\u0442",
            "\u0442\u0435\u043a\u0441\u0442",
            "token",
        )
    )
    needs_dependency = _is_dependency_query(q_lower)

    return f"""
import pandas as pd
import numpy as np
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import re
import warnings
from datetime import datetime
try:
    import seaborn as sns
except Exception:
    sns = None

df = datasets[{json.dumps(primary_table_name)}].copy()
result = {{
    "status": "ok",
    "table_name": {json.dumps(primary_table_name)},
    "metrics": {{}},
    "notes": [],
    "artifacts": [],
    "insights": []
}}

result["metrics"]["rows_total"] = int(len(df))
result["metrics"]["columns_total"] = int(len(df.columns))
result["metrics"]["columns"] = [str(c) for c in df.columns]
result["metrics"]["insights"] = []

con = duckdb.connect(database=":memory:")
con.register("df", df)
duck_rows = con.execute("SELECT COUNT(*) AS cnt FROM df").fetchone()[0]
result["metrics"]["rows_total_duckdb"] = int(duck_rows or 0)
con.close()

def to_datetime_silent(series):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"Could not infer format, so each element will be parsed individually.*",
        )
        try:
            return pd.to_datetime(series, errors="coerce", format="mixed")
        except TypeError:
            return pd.to_datetime(series, errors="coerce")

def infer_column_purpose(column_name):
    c = str(column_name).lower()
    if "id" in c:
        return "identifier/key column"
    if "date" in c or "time" in c:
        return "time/event timestamp"
    if "comment" in c or "text" in c or "descr" in c:
        return "free-text/narrative field"
    if "office" in c or "region" in c or "city" in c or "branch" in c:
        return "organizational or location dimension"
    if "status" in c or "stage" in c:
        return "process state dimension"
    if "amount" in c or "sum" in c or "price" in c or "cost" in c or "total" in c:
        return "financial metric field"
    if "count" in c or "qty" in c:
        return "volume/count metric field"
    return "attribute used for segmentation/analysis"

def infer_process_context(columns):
    cols = set([str(c).lower() for c in columns])
    if set(["application_id", "comment_time", "comment_text", "office"]).issubset(cols):
        return "Application review / processing workflow with distributed offices and analyst comments."
    if ("order_id" in cols or "invoice_id" in cols) and ("status" in cols or "stage" in cols):
        return "Order-to-cash / document processing workflow."
    if ("ticket_id" in cols or "incident_id" in cols) and ("status" in cols or "comment_text" in cols):
        return "Support or incident management workflow."
    return "Likely an operational process dataset with records, dimensions, and process indicators."

column_profiles = []
numeric_summaries = []
datetime_summaries = []
categorical_summaries = []

rows_total = int(len(df))
for col in df.columns:
    series = df[col]
    non_null = int(series.notna().sum())
    null_count = int(rows_total - non_null)
    unique_count = int(series.nunique(dropna=True))
    sample_values = [str(v) for v in series.dropna().astype(str).head(3).tolist()]
    purpose_hint = infer_column_purpose(col)

    profile = {{
        "column": str(col),
        "dtype": str(series.dtype),
        "purpose_hint": purpose_hint,
        "non_null": non_null,
        "null_count": null_count,
        "unique_count": unique_count,
        "sample_values": sample_values,
    }}

    numeric_series = pd.to_numeric(series, errors="coerce")
    numeric_non_null = int(numeric_series.notna().sum())
    if numeric_non_null > 0 and numeric_non_null >= max(3, int(rows_total * 0.1)):
        profile["numeric_detected"] = True
        num_clean = numeric_series.dropna()
        if len(num_clean) > 0:
            numeric_summaries.append(
                {{
                    "column": str(col),
                    "count": int(len(num_clean)),
                    "min": float(num_clean.min()),
                    "max": float(num_clean.max()),
                    "mean": float(num_clean.mean()),
                    "median": float(num_clean.median()),
                }}
            )

    dt_series = to_datetime_silent(series)
    dt_non_null = int(dt_series.notna().sum())
    if dt_non_null > 0 and dt_non_null >= max(3, int(rows_total * 0.1)):
        profile["datetime_detected"] = True
        dt_clean = dt_series.dropna()
        if len(dt_clean) > 0:
            datetime_summaries.append(
                {{
                    "column": str(col),
                    "min": str(dt_clean.min()),
                    "max": str(dt_clean.max()),
                }}
            )

    if unique_count > 1 and unique_count <= 40:
        ser = series.dropna().astype(str).str.strip()
        ser = ser[ser != ""]
        if len(ser) > 0:
            top_counts = ser.value_counts().head(8)
            categorical_summaries.append(
                {{
                    "column": str(col),
                    "top_values": {{str(k): int(v) for k, v in top_counts.items()}},
                }}
            )

    column_profiles.append(profile)

result["metrics"]["column_profile"] = column_profiles
result["metrics"]["numeric_summary"] = numeric_summaries
result["metrics"]["datetime_summary"] = datetime_summaries
result["metrics"]["categorical_summary"] = categorical_summaries
result["metrics"]["potential_process"] = infer_process_context(df.columns)
result["metrics"]["insights"].append(f"Dataset rows={{int(len(df))}}, columns={{int(len(df.columns))}}")
result["metrics"]["insights"].append(f"Detected numeric columns={{len(numeric_summaries)}}, datetime columns={{len(datetime_summaries)}}")

if "application_id" in df.columns:
    result["metrics"]["application_id_unique"] = int(df["application_id"].nunique(dropna=True))

if "office" in df.columns:
    office = df["office"].astype(str).str.strip()
    office = office[office != ""]
    office_counts = office.value_counts().head(10)
    if len(office_counts) > 0:
        result["metrics"]["office_top"] = {{str(k): int(v) for k, v in office_counts.items()}}

if "comment_time" in df.columns:
    ts = to_datetime_silent(df["comment_time"])
    valid_ts = ts.dropna()
    if len(valid_ts) > 0:
        result["metrics"]["comment_time_min"] = str(valid_ts.min())
        result["metrics"]["comment_time_max"] = str(valid_ts.max())
    else:
        result["notes"].append("comment_time exists but could not be parsed to datetime")

if {str(needs_nlp)} and "comment_text" in df.columns:
    txt = df["comment_text"].astype(str).str.lower()
    txt = txt.str.replace(r"[^a-z\u0430-\u044f0-9\\s]", " ", regex=True)
    tokens = txt.str.split().explode()
    if tokens is not None:
        tokens = tokens[tokens.str.len() > 2]
    if tokens is not None and len(tokens) > 0:
        top_tokens = tokens.value_counts().head(20)
        result["metrics"]["comment_top_tokens"] = {{str(k): int(v) for k, v in top_tokens.items()}}
    else:
        result["notes"].append("NLP requested but no tokens extracted from comment_text")
elif {str(needs_nlp)}:
    result["notes"].append("NLP requested but comment_text column was not found")

if {str(needs_visual)}:
    figure_saved = False
    if {str(needs_dependency)}:
        numeric_cols = [str(item["column"]) for item in numeric_summaries]
        if len(numeric_cols) >= 2:
            numeric_df = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
            corr = numeric_df.corr(numeric_only=True)
            if corr is not None and not corr.empty:
                fig, ax = plt.subplots(figsize=(8, 6))
                if sns is not None:
                    sns.heatmap(corr, annot=True, cmap="RdBu_r", center=0, ax=ax)
                else:
                    image = ax.imshow(corr.to_numpy(), cmap="RdBu_r")
                    ax.figure.colorbar(image, ax=ax)
                    ax.set_xticks(range(len(corr.columns)))
                    ax.set_xticklabels([str(x) for x in corr.columns], rotation=45, ha="right")
                    ax.set_yticks(range(len(corr.index)))
                    ax.set_yticklabels([str(x) for x in corr.index])
                ax.set_title("Numeric dependency heatmap")
                plot_path = save_plot(fig=fig, name="numeric_dependency_heatmap.png")
                plt.close(fig)
                result["artifacts"].append({{"kind": "heatmap", "path": plot_path}})
                figure_saved = True
                result["metrics"]["insights"].append("Built numeric dependency heatmap from correlation matrix.")

        if (not figure_saved) and len(numeric_summaries) >= 1:
            metric_col = str(numeric_summaries[0]["column"])
            for candidate_col in df.columns:
                ser = df[candidate_col].dropna().astype(str).str.strip()
                ser = ser[ser != ""]
                if len(ser) == 0:
                    continue
                if int(ser.nunique()) > 12:
                    continue
                plot_df = df[[candidate_col, metric_col]].copy()
                plot_df[metric_col] = pd.to_numeric(plot_df[metric_col], errors="coerce")
                plot_df = plot_df.dropna()
                if len(plot_df) < 3:
                    continue
                grouped = plot_df.groupby(str(candidate_col))[str(metric_col)].mean().sort_values(ascending=False).head(10)
                if grouped.empty:
                    continue
                fig, ax = plt.subplots(figsize=(10, 4))
                ax.bar([str(x) for x in grouped.index], [float(v) for v in grouped.values], color="#4c78a8")
                ax.set_title(f"Mean {{metric_col}} by {{candidate_col}}")
                ax.set_ylabel("Mean value")
                ax.tick_params(axis="x", labelrotation=45)
                plot_path = save_plot(fig=fig, name=f"dependency_mean_{{str(candidate_col)}}_{{metric_col}}.png")
                plt.close(fig)
                result["artifacts"].append({{"kind": "dependency_bar", "column": str(candidate_col), "path": plot_path}})
                figure_saved = True
                result["metrics"]["insights"].append(f"Built dependency chart: mean {{metric_col}} by {{candidate_col}}.")
                break

    if "office" in df.columns and "comment_time" in df.columns:
        tmp = df[["office", "comment_time"]].copy()
        tmp["comment_time"] = to_datetime_silent(tmp["comment_time"])
        tmp = tmp.dropna(subset=["comment_time"])
        if len(tmp) > 0:
            tmp["weekday"] = tmp["comment_time"].dt.day_name()
            pivot = tmp.pivot_table(index="office", columns="weekday", values="comment_time", aggfunc="count", fill_value=0)
            if not pivot.empty:
                weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                existing = [w for w in weekdays if w in pivot.columns]
                pivot = pivot[existing] if existing else pivot
                fig, ax = plt.subplots(figsize=(10, 4))
                if sns is not None:
                    sns.heatmap(pivot, cmap="Blues", annot=True, fmt=".0f", linewidths=0.3, ax=ax)
                else:
                    matrix = pivot.to_numpy()
                    image = ax.imshow(matrix, aspect="auto", cmap="Blues")
                    ax.figure.colorbar(image, ax=ax)
                    ax.set_xticks(range(len(pivot.columns)))
                    ax.set_xticklabels([str(x) for x in pivot.columns], rotation=45, ha="right")
                    ax.set_yticks(range(len(pivot.index)))
                    ax.set_yticklabels([str(x) for x in pivot.index])
                ax.set_title("Office vs weekday comment activity")
                plot_path = save_plot(fig=fig, name="office_weekday_heatmap.png")
                plt.close(fig)
                result["artifacts"].append({{"kind": "heatmap", "path": plot_path}})
                figure_saved = True

    if not figure_saved and len(numeric_summaries) > 0:
        col = str(numeric_summaries[0]["column"])
        numeric_values = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(numeric_values) > 0:
            fig, ax = plt.subplots(figsize=(8, 4))
            numeric_values.astype(float).plot(kind="hist", bins=20, ax=ax, title=f"Distribution of {{col}}")
            plot_path = save_plot(fig=fig, name="numeric_distribution.png")
            plt.close(fig)
            result["artifacts"].append({{"kind": "histogram", "column": str(col), "path": plot_path}})
            figure_saved = True

    if not figure_saved:
        for col in df.columns:
            ser = df[col].dropna().astype(str).str.strip()
            ser = ser[ser != ""]
            if len(ser) == 0:
                continue
            value_counts = ser.value_counts().head(10)
            if len(value_counts) <= 1:
                continue
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar([str(x) for x in value_counts.index], [int(v) for v in value_counts.values], color="#4c78a8")
            ax.set_title(f"Top values in {{col}}")
            ax.set_ylabel("Count")
            ax.tick_params(axis="x", labelrotation=45)
            plot_path = save_plot(fig=fig, name=f"top_values_{{str(col)}}.png")
            plt.close(fig)
            result["artifacts"].append({{"kind": "categorical_bar", "column": str(col), "path": plot_path}})
            figure_saved = True
            break

    if not figure_saved:
        result["notes"].append("Visualization requested but no suitable columns were found for charting.")
"""

def _format_complex_analytics_answer(
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
    is_ru = _is_russian_text(query)
    rows_total = int(metrics.get("rows_total", 0) or 0)
    columns_total = int(metrics.get("columns_total", 0) or 0)
    columns = metrics.get("columns") if isinstance(metrics.get("columns"), list) else []
    process_context = str(metrics.get("potential_process") or "").strip()
    if is_ru:
        process_context = _localize_en_to_ru(
            process_context or "Likely an operational process dataset.",
            _RU_PROCESS_CONTEXT,
        )
    elif not process_context:
        process_context = "Likely an operational process dataset."

    column_profile = metrics.get("column_profile") if isinstance(metrics.get("column_profile"), list) else []
    numeric_summary = metrics.get("numeric_summary") if isinstance(metrics.get("numeric_summary"), list) else []
    datetime_summary = metrics.get("datetime_summary") if isinstance(metrics.get("datetime_summary"), list) else []
    categorical_summary = metrics.get("categorical_summary") if isinstance(metrics.get("categorical_summary"), list) else []
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
                purpose = _localize_en_to_ru(str(item.get("purpose_hint") or ""), _RU_PURPOSE_HINTS) or "не определено"
                non_null = int(item.get("non_null", 0) or 0)
                null_count = int(item.get("null_count", 0) or 0)
                unique_count = int(item.get("unique_count", 0) or 0)
                sample_values = item.get("sample_values") if isinstance(item.get("sample_values"), list) else []
                sample_text = ", ".join([str(x) for x in sample_values[:3]]) if sample_values else "-"
                lines.append(
                    f"- `{col}`: {purpose}; непустых={non_null}, пустых={null_count}, уникальных={unique_count}, примеры={sample_text}"
                )

        lines.append("### 4) Метрики и статистика")
        if numeric_summary:
            lines.append("- Числовые метрики:")
            for item in numeric_summary[:12]:
                lines.append(
                    f"  - `{item.get('column')}`: кол-во={int(item.get('count', 0) or 0)}, "
                    f"мин={item.get('min')}, макс={item.get('max')}, среднее={item.get('mean')}, медиана={item.get('median')}"
                )
        else:
            lines.append("- Числовые метрики для расчета не найдены.")

        if datetime_summary:
            lines.append("- Временные метрики:")
            for item in datetime_summary[:8]:
                lines.append(f"  - `{item.get('column')}`: мин={item.get('min')}, макс={item.get('max')}")

        if categorical_summary:
            lines.append("- Категориальные распределения (top):")
            for item in categorical_summary[:8]:
                top_values = item.get("top_values") if isinstance(item.get("top_values"), dict) else {}
                rendered = ", ".join([f"{k}:{v}" for k, v in list(top_values.items())[:6]])
                lines.append(f"  - `{item.get('column')}`: {rendered}")

        if all_insights:
            lines.append("### 5) Аналитические выводы")
            for insight in all_insights[:10]:
                insight_text = str(insight).strip()
                if insight_text:
                    lines.append(f"- {insight_text}")

        lines.append("### 6) Визуализации")
        if artifacts:
            for artifact in artifacts:
                kind = str(artifact.get("kind", "chart") or "chart")
                kind_label = _localize_en_to_ru(kind, _RU_ARTIFACT_KIND_MAP) or kind
                name = str(artifact.get("name", "") or "")
                path = str(artifact.get("path", "") or "")
                url = str(artifact.get("url", "") or "")
                ref = url or path or name
                lines.append(f"- {kind_label}: `{name}` -> `{ref}`")
                if url:
                    lines.append(f"![{kind_label}]({url})")
        else:
            lines.append("- В этом запуске графики не были построены.")

        if notes:
            lines.append("### 7) Ограничения / заметки")
            for note in notes[:6]:
                note_text = _localize_en_to_ru(str(note).strip(), _RU_NOTE_MAP)
                if note_text:
                    lines.append(f"- {note_text}")

        if include_code:
            lines.append("### 8) Выполненный Python-код")
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
        if all_insights:
            lines.append("### 5) Key Insights")
            for insight in all_insights[:10]:
                insight_text = str(insight).strip()
                if insight_text:
                    lines.append(f"- {insight_text}")

        lines.append("### 6) Visualizations")
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
            lines.append("### 7) Notes / Limitations")
            for note in notes[:6]:
                note_text = str(note).strip()
                if note_text:
                    lines.append(f"- {note_text}")
        if include_code:
            lines.append("### 8) Executed Python Code")
            lines.append("```python")
            lines.append(executed_code.strip())
            lines.append("```")

    return "\n".join(lines)

def _load_table_dataframe(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    max_rows: int,
) -> Any:
    try:
        import duckdb  # noqa: PLC0415
        import pandas as pd  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - dependency check path
        raise RuntimeError("pandas and duckdb are required for complex analytics executor") from exc

    table_q = _quote_ident(table.table_name)
    limit_clause = f" LIMIT {int(max_rows)}" if int(max_rows) > 0 else ""

    if dataset.engine == "duckdb_parquet":
        if table.parquet_path is None:
            raise RuntimeError(f"Missing parquet path for table {table.table_name}")
        conn = duckdb.connect(database=":memory:")
        try:
            sql = f"SELECT * FROM read_parquet({_sql_literal(str(table.parquet_path))}){limit_clause}"
            return conn.execute(sql).df()
        finally:
            conn.close()

    if dataset.engine == "sqlite_legacy":
        if dataset.sqlite_path is None:
            raise RuntimeError("Missing SQLite path for legacy tabular dataset")
        sqlite_conn = sqlite3.connect(str(dataset.sqlite_path))
        try:
            sql = f"SELECT * FROM {table_q}{limit_clause}"
            return pd.read_sql_query(sql, sqlite_conn)
        finally:
            sqlite_conn.close()

    raise RuntimeError(f"Unsupported tabular dataset engine: {dataset.engine}")


def _collect_datasets_for_file(file_obj: Any) -> Optional[Tuple[ResolvedTabularDataset, Dict[str, Any]]]:
    dataset = resolve_tabular_dataset(file_obj)
    if dataset is None or not dataset.tables:
        return None

    frames: Dict[str, Any] = {}
    max_rows = int(settings.COMPLEX_ANALYTICS_MAX_ROWS)
    for table in dataset.tables:
        frames[table.table_name] = _load_table_dataframe(dataset=dataset, table=table, max_rows=max_rows)
    return dataset, frames


def _clarification_for_error(code: str, details: Optional[str] = None) -> str:
    detail_tail = f" ({details})" if details else ""
    if code == COMPLEX_ANALYTICS_ERROR_SECURITY:
        return "Complex analytics sandbox blocked unsafe operations. Remove network/subprocess/system operations and retry."
    if code == COMPLEX_ANALYTICS_ERROR_TIMEOUT:
        return "Complex analytics sandbox timed out. Reduce dataframe scope or simplify the analysis steps and retry."
    if code == COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT:
        return "Complex analytics sandbox output exceeded limits. Reduce printed output or chart count and retry."
    if code == COMPLEX_ANALYTICS_ERROR_DATASET:
        return "Complex analytics requires a tabular dataset in this conversation."
    if code == COMPLEX_ANALYTICS_ERROR_DEPENDENCY:
        return "Complex analytics runtime dependency is missing. Install pandas/numpy/duckdb/matplotlib/seaborn in offline environment."
    return f"Complex analytics sandbox failed{detail_tail}. Please clarify or simplify the request."


def _build_error_payload(
    *,
    query: str,
    target_file: Optional[Any],
    dataset: Optional[ResolvedTabularDataset],
    code: str,
    message: str,
    debug_details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source_label = getattr(target_file, "original_filename", "unknown")
    dataset_version = getattr(dataset, "dataset_version", None)
    dataset_id = getattr(dataset, "dataset_id", None)

    return {
        "status": "error",
        "final_response": _clarification_for_error(code, details=message),
        "clarification_prompt": _clarification_for_error(code, details=message),
        "sources": [f"{source_label} | complex_analytics_error={code}"] if target_file is not None else [],
        "artifacts": [],
        "debug": {
            "retrieval_mode": "complex_analytics",
            "intent": "complex_analytics",
            "execution_route": "complex_analytics",
            "executor_attempted": True,
            "executor_status": "error",
            "executor_error_code": code,
            "artifacts_count": 0,
            "complex_analytics": {
                "query": query,
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "message": message,
                "details": debug_details or {},
            },
        },
    }


def _execute_complex_analytics_sync(
    *,
    query: str,
    target_file: Any,
    dataset: ResolvedTabularDataset,
    datasets: Dict[str, Any],
    primary_table: ResolvedTabularTable,
    code: str,
    codegen_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_id = uuid.uuid4().hex
    artifacts_root = Path(settings.COMPLEX_ANALYTICS_ARTIFACT_DIR).expanduser().resolve()
    _cleanup_complex_analytics_artifacts(artifacts_root=artifacts_root)
    artifacts_dir = artifacts_root / run_id

    effective_code = code
    effective_codegen_meta = dict(codegen_meta or {})
    try:
        sandbox_result = execute_sandboxed_python(
            code=effective_code,
            datasets=datasets,
            artifacts_dir=artifacts_dir,
            max_output_chars=int(settings.COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS),
            max_artifacts=int(settings.COMPLEX_ANALYTICS_MAX_ARTIFACTS),
        )
    except Exception as exec_error:
        if str((effective_codegen_meta or {}).get("code_source") or "") == "llm":
            fallback_code = _build_complex_analysis_code(
                query=query,
                primary_table_name=primary_table.table_name,
            )
            effective_code = fallback_code
            effective_codegen_meta["code_source"] = "template_runtime_fallback"
            effective_codegen_meta["codegen_status"] = "runtime_fallback"
            effective_codegen_meta["codegen_error"] = f"runtime_exec:{type(exec_error).__name__}"
            inc_counter("complex_analytics_codegen_total", status="fallback", reason="runtime_exec_error")
            sandbox_result = execute_sandboxed_python(
                code=effective_code,
                datasets=datasets,
                artifacts_dir=artifacts_dir,
                max_output_chars=int(settings.COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS),
                max_artifacts=int(settings.COMPLEX_ANALYTICS_MAX_ARTIFACTS),
            )
        else:
            raise

    metrics = sandbox_result.result.get("metrics")
    notes = sandbox_result.result.get("notes")
    insights = sandbox_result.result.get("insights")
    artifacts = sandbox_result.artifacts
    result_artifacts = sandbox_result.result.get("artifacts")
    if isinstance(result_artifacts, list):
        for index, item in enumerate(result_artifacts):
            if index >= len(artifacts):
                break
            if isinstance(item, dict):
                artifacts[index].update({k: v for k, v in item.items() if k != "path"})
    response_artifacts: List[Dict[str, Any]] = []
    for artifact in artifacts:
        path_value = str(artifact.get("path") or "")
        if path_value:
            public_url = _artifact_public_url(path_value)
            if public_url:
                artifact["url"] = public_url
        response_artifacts.append(_sanitize_artifact_for_response(artifact))

    if response_artifacts:
        inc_counter("complex_analytics_artifacts_generated_total", value=len(response_artifacts))
        for artifact in response_artifacts:
            inc_counter(
                "complex_analytics_artifact_kind_total",
                kind=str(artifact.get("kind") or "unknown"),
            )
    inc_counter("complex_analytics_executor_success_total", engine=str(dataset.engine))

    answer_text = _format_complex_analytics_answer(
        query=query,
        table_name=primary_table.table_name,
        metrics=metrics if isinstance(metrics, dict) else {},
        notes=notes if isinstance(notes, list) else [],
        artifacts=response_artifacts,
        executed_code=effective_code,
        include_code=_wants_python_code(query),
        insights=insights if isinstance(insights, list) else [],
    )
    source_label = getattr(target_file, "original_filename", "unknown")
    return {
        "status": "ok",
        "final_response": answer_text,
        "sources": [
            (
                f"{source_label} | table={primary_table.table_name} "
                f"| dataset_v={dataset.dataset_version} | complex_analytics"
            )
        ],
        "artifacts": response_artifacts,
        "debug": {
            "retrieval_mode": "complex_analytics",
            "intent": "complex_analytics",
            "execution_route": "complex_analytics",
            "executor_attempted": True,
            "executor_status": "success",
            "executor_error_code": None,
            "artifacts_count": len(response_artifacts),
            "complex_analytics": {
                "query": query,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": primary_table.table_name,
                "table_version": primary_table.table_version,
                "table_provenance_id": primary_table.provenance_id,
                "stdout": sandbox_result.stdout,
                "code_preview": effective_code[:1200],
                "code_source": str((effective_codegen_meta or {}).get("code_source") or "template"),
                "codegen": dict(effective_codegen_meta or {}),
                "metrics": metrics if isinstance(metrics, dict) else {},
                "notes": notes if isinstance(notes, list) else [],
                "artifact_dir": _artifact_relative_path(str(artifacts_dir)) or run_id,
            },
        },
    }


async def execute_complex_analytics_path(
    *,
    query: str,
    files: List[Any],
    model_source: Optional[str] = None,
    provider_mode: Optional[str] = None,
    model_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not is_complex_analytics_query(query):
        return None

    target_file = None
    dataset = None
    datasets = None
    for file_obj in files:
        file_type = str(getattr(file_obj, "file_type", "") or "").lower()
        if file_type not in {"xlsx", "xls", "csv"}:
            continue
        try:
            resolved = _collect_datasets_for_file(file_obj)
        except RuntimeError as exc:
            message = str(exc)
            code = COMPLEX_ANALYTICS_ERROR_RUNTIME
            if "required for complex analytics executor" in message:
                code = COMPLEX_ANALYTICS_ERROR_DEPENDENCY
            return _build_error_payload(
                query=query,
                target_file=file_obj,
                dataset=None,
                code=code,
                message=message,
            )
        if resolved is None:
            continue
        target_file = file_obj
        dataset, datasets = resolved
        break

    if target_file is None or dataset is None or datasets is None:
        return _build_error_payload(
            query=query,
            target_file=None,
            dataset=None,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message="No tabular dataset attached to current conversation",
        )

    primary_table = _resolve_table_for_query(query=query, dataset=dataset)
    if primary_table is None:
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message="No tables available in tabular dataset",
        )

    primary_frame = datasets.get(primary_table.table_name)
    if primary_frame is None:
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message=f"Primary table frame not found: {primary_table.table_name}",
        )

    generated_code, codegen_meta = await _generate_complex_analysis_code(
        query=query,
        primary_table_name=primary_table.table_name,
        primary_frame=primary_frame,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
    )

    timeout_seconds = float(settings.COMPLEX_ANALYTICS_TIMEOUT_SECONDS)
    started = perf_counter()
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(
                _execute_complex_analytics_sync,
                query=query,
                target_file=target_file,
                dataset=dataset,
                datasets=datasets,
                primary_table=primary_table,
                code=generated_code,
                codegen_meta=codegen_meta,
            ),
            timeout=timeout_seconds,
        )
        observe_ms("complex_analytics_executor_ms", (perf_counter() - started) * 1000.0)
        status = str(payload.get("status") or "ok")
        inc_counter(
            "complex_analytics_executor_total",
            status=status,
            engine=str(dataset.engine),
        )
        return payload
    except TimeoutError:
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_TIMEOUT)
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_TIMEOUT,
            message=f"Execution exceeded timeout={timeout_seconds}s",
        )
    except ComplexAnalyticsSecurityError as exc:
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_SECURITY)
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_SECURITY,
            message=str(exc),
        )
    except ComplexAnalyticsOutputLimitError as exc:
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT)
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT,
            message=str(exc),
        )
    except RuntimeError as exc:
        message = str(exc)
        code = COMPLEX_ANALYTICS_ERROR_RUNTIME
        if "required for complex analytics executor" in message:
            code = COMPLEX_ANALYTICS_ERROR_DEPENDENCY
        inc_counter("complex_analytics_executor_error_total", error_code=code)
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=code,
            message=message,
        )
    except Exception as exc:  # pragma: no cover - fallback protection path
        logger.warning("Complex analytics executor failed: %s", exc, exc_info=True)
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_RUNTIME)
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_RUNTIME,
            message=str(exc),
            debug_details={"traceback": traceback.format_exc(limit=20)},
        )






