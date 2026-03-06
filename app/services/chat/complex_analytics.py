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
from app.services.chat.language import apply_language_policy_to_prompt, detect_preferred_response_language
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
COMPLEX_ANALYTICS_RESPONSE_ERROR = "response_generation_error"
COMPLEX_ANALYTICS_ERROR_CODEGEN = "codegen_failed"
COMPLEX_ANALYTICS_ERROR_VALIDATION = "validation_failed"
COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS = "missing_required_artifacts"

_COMPLEX_ANALYTICS_HINTS = (
    "python",
    "pandas",
    "numpy",
    "duckdb",
    "seaborn",
    "matplotlib",
    "heatmap",
    "dependency",
    "dependencies",
    "correlation",
    "relationship",
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
    "зависим",
    "коррел",
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
    "sys",
    "importlib",
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
    "sys",
    "importlib",
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


def _resolve_complex_analytics_routing(
    *,
    model_source: Optional[str],
    provider_mode: Optional[str],
) -> Dict[str, str]:
    """Resolve provider + provider-mode for complex analytics LLM calls.

    This is a strict execution helper; explicit provider selection (`local`/`ollama`,
    `openai`, `corporate`) must not be downgraded by policy mode.
    `aihub` keeps policy mode unless explicit mode is required by caller.
    """
    configured_default = str(getattr(settings, "DEFAULT_MODEL_SOURCE", "aihub")).strip().lower() or "aihub"
    requested_source = str(model_source or configured_default).strip().lower() or "aihub"
    normalized_source = requested_source
    if normalized_source == "local":
        normalized_source = "ollama"
    elif normalized_source == "corporate":
        normalized_source = "aihub"
    elif normalized_source not in {"aihub", "ollama", "openai"}:
        normalized_source = "aihub"

    force_local = bool(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL", False))
    if force_local:
        normalized_source = "ollama"
        resolved_mode = "explicit"
    elif normalized_source == "aihub":
        resolved_mode = str(provider_mode or "policy").strip().lower() or "policy"
    else:
        # Local/Ollama/OpenAI providers are explicit by nature in this architecture.
        resolved_mode = "explicit"

    return {"model_source": normalized_source, "provider_mode": resolved_mode}
_NETWORK_LITERAL_PATTERN = re.compile(r"https?://|ftp://", flags=re.IGNORECASE)
_CODE_BLOCK_PATTERN = re.compile(r"```(?:python|py)?\s*(.*?)```", flags=re.IGNORECASE | re.DOTALL)
_MAX_CODE_LINES = 520
_CODEGEN_TABLE_SAMPLE_ROWS = 6
_CODEGEN_COLUMN_SAMPLE_VALUES = 4
_CODEGEN_PLAN_ATTEMPTS = 2
_CODEGEN_REPAIR_ATTEMPTS = 3


class ComplexAnalyticsSecurityError(Exception):
    pass


class ComplexAnalyticsOutputLimitError(Exception):
    pass


class ComplexAnalyticsValidationError(Exception):
    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = str(error_code or COMPLEX_ANALYTICS_ERROR_VALIDATION)


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


def _intent_flags_from_query(query: str) -> Dict[str, bool]:
    q = (query or "").lower()
    return {
        "requires_visualization": any(
            token in q
            for token in (
                "visual",
                "vis",
                "chart",
                "plot",
                "heatmap",
                "график",
                "графики",
                "диаграм",
                "визуализа",
                "хитмап",
                "теплов",
            )
        ),
        "requires_dependency": _is_dependency_query(q),
        "requires_nlp": any(
            token in q
            for token in (
                "nlp",
                "comment_text",
                "comment",
                "коммент",
                "текст",
                "token",
                "леммат",
                "sentiment",
            )
        ),
    }


def _contract_from_plan(plan: Dict[str, Any]) -> Dict[str, bool]:
    required_outputs = [str(item or "").lower() for item in plan.get("required_outputs", []) if str(item or "").strip()]
    output_blob = " ".join(required_outputs)
    return {
        "expects_visualization": any(
            token in output_blob
            for token in ("chart", "plot", "hist", "histogram", "heatmap", "bar", "scatter", "visual", "dependency", "correlation")
        ),
        "expects_dependency": any(
            token in output_blob for token in ("dependency", "correlation", "relationship", "heatmap", "cross-tab", "covariance")
        ),
        "expects_nlp": any(token in output_blob for token in ("nlp", "token", "keyword", "comment", "text")),
    }


def _normalize_text_block(text: str, *, max_length: int = 2000) -> str:
    value = (text or "").strip()
    if len(value) <= max_length:
        return value
    return value[: max_length - 3].rstrip() + "..."


def _parse_truthy_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


def _compute_plan_contract(
    *,
    plan: Dict[str, Any],
    query: str,
) -> Dict[str, Any]:
    query_flags = _intent_flags_from_query(query)
    plan_outputs = [str(item or "").strip().lower() for item in plan.get("required_outputs", []) if str(item or "").strip()]
    plan_blob = " ".join(plan_outputs)
    explicit_contract = plan.get("required_contract")
    explicit_requires_visualization = bool(
        explicit_contract
        and isinstance(explicit_contract, dict)
        and _parse_truthy_bool(explicit_contract.get("expects_visualization"))
    )
    explicit_requires_dependency = bool(
        explicit_contract
        and isinstance(explicit_contract, dict)
        and _parse_truthy_bool(explicit_contract.get("expects_dependency"))
    )
    explicit_requires_nlp = bool(
        explicit_contract
        and isinstance(explicit_contract, dict)
        and _parse_truthy_bool(explicit_contract.get("expects_nlp"))
    )

    contract = _contract_from_plan(plan)
    if explicit_requires_visualization:
        contract["expects_visualization"] = True
    if explicit_requires_dependency:
        contract["expects_dependency"] = True
    if explicit_requires_nlp:
        contract["expects_nlp"] = True

    # keep safe fallback flags from query-level detection
    if query_flags["requires_visualization"]:
        contract["expects_visualization"] = True
    if query_flags["requires_dependency"]:
        contract["expects_dependency"] = True
    if query_flags["requires_nlp"]:
        contract["expects_nlp"] = True

    if "depends" in plan_blob:
        contract["expects_dependency"] = True
    if "dependency" in plan_blob:
        contract["expects_dependency"] = True
    if "heatmap" in plan_blob:
        contract["expects_visualization"] = True

    required_outputs = plan_outputs
    return {
        "analysis_goal": str(plan.get("analysis_goal") or query),
        "required_outputs": required_outputs,
        "expects_visualization": bool(contract["expects_visualization"]),
        "expects_dependency": bool(contract["expects_dependency"]),
        "expects_nlp": bool(contract["expects_nlp"]),
        "plan_blob": plan_blob,
    }


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

    import warnings as _sandbox_warnings

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
        "Exception": Exception,
        "TypeError": TypeError,
        "ValueError": ValueError,
        "RuntimeError": RuntimeError,
        "UserWarning": UserWarning,
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
    try:
        import matplotlib  # noqa: PLC0415

        matplotlib.use("Agg", force=True)
    except Exception:
        pass
    with _sandbox_warnings.catch_warnings():
        _sandbox_warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message=r"Could not infer format, so each element will be parsed individually.*",
        )
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


def _extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    json_candidates = re.findall(
        r"```(?:json)?\s*(.*?)```",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    candidates: List[str] = [str(item or "").strip() for item in json_candidates if str(item or "").strip()]
    candidates.append(raw)
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    # fallback: scan plain text for the first JSON object (supports prefix/suffix around JSON)
    brace_depth = 0
    string_mode = False
    escaped = False
    start_pos: Optional[int] = None
    for index, char in enumerate(raw):
        if escaped:
            escaped = False
            continue
        if string_mode:
            if char == "\\":
                escaped = True
            elif char == '"':
                string_mode = False
            continue
        if char == '"':
            string_mode = True
            continue
        if char == "{":
            if brace_depth == 0:
                start_pos = index
            brace_depth += 1
            continue
        if char == "}" and brace_depth > 0:
            brace_depth -= 1
            if brace_depth == 0 and start_pos is not None:
                candidate = raw[start_pos : index + 1]
                try:
                    parsed = json.loads(candidate)
                except Exception:
                    start_pos = None
                    continue
                if isinstance(parsed, dict):
                    return parsed
                start_pos = None
    return None


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


def _build_complex_analysis_plan_prompt(
    *,
    query: str,
    primary_table_name: str,
    dataframe_profile: Dict[str, Any],
) -> str:
    profile_json = json.dumps(dataframe_profile, ensure_ascii=False)
    profile_snippet = profile_json[:14000]
    return textwrap.dedent(
        f"""
You are a planning analyst for offline tabular data execution.
Return ONLY strict JSON (no markdown, no prose outside JSON) with this schema:
{{
  "analysis_goal": "string",
  "required_artifacts": ["plots"|"tables"|"metrics"|"nlp"|...],
  "required_outputs": ["summary","metrics","insights","tables","artifacts"],
  "data_contract": {{
    "required_inputs": ["datasets","dataset_name","file_ids"],
    "required_outputs": ["result","artifacts","insights","metrics","tables"]
  }},
  "required_contract": {{
    "expects_visualization": true|false,
    "expects_dependency": true|false,
    "expects_nlp": true|false
  }},
  "python_generation_prompt": "string",
  "should_generate_code": true
}}

Hard requirements for the plan:
- The generated code must load data using: df = datasets[{json.dumps(primary_table_name)}].copy()
- The analysis is offline only; no network or file reads.
- The generated code must produce variable `result` with keys: status, table_name, metrics, notes, artifacts.
- For dependency/correlation requests, plan must ask for relationship analysis and at least one dependency artifact when feasible.
- If heatmap is impossible due dtypes, plan must ask for a meaningful fallback chart.
- Keep target script under {_MAX_CODE_LINES} lines.

User request:
{query}

Data profile (JSON):
{profile_snippet}
        """
    ).strip()


def _build_codegen_prompt(
    *,
    analysis_plan: str,
    primary_table_name: str,
    dataframe_profile: Dict[str, Any],
    plan_contract: Dict[str, Any],
) -> str:
    safe_plan = str(analysis_plan or "").strip()
    profile_snippet = json.dumps(dataframe_profile, ensure_ascii=False)[:16000]
    contract_snippet = json.dumps(plan_contract, ensure_ascii=False)
    return textwrap.dedent(
        f"""
You are generating offline Python code for a secure analytics sandbox.
Return Python code only. No markdown. No comments outside code.

Execution plan:
{safe_plan}

Contract summary:
{contract_snippet}

Dataset profile JSON:
{profile_snippet}

Mandatory runtime rules:
- Start with: df = datasets[{json.dumps(primary_table_name)}].copy()
- Use only: pandas, numpy, duckdb, matplotlib, seaborn, datetime, re, warnings.
- Never use network/system/subprocess/file IO/eval/exec/open.
- Save charts only via save_plot(fig=..., name="...png").
- Define `result` as dict with keys:
  status, table_name, metrics (dict), notes (list), artifacts (list), insights (list optional).
- metrics must include rows_total, columns_total, columns and analytical findings.
- If visualization/dependency requested and feasible, generate at least one chart artifact.
- If impossible, add explicit reason to `result["notes"]`.
- Keep script under {_MAX_CODE_LINES} lines.
        """.strip()
    ).strip()


def _validate_generated_code_contract(code: str, *, plan_contract: Optional[Dict[str, Any]] = None) -> Optional[str]:
    effective_contract = plan_contract if isinstance(plan_contract, dict) else {}
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
    if str(effective_contract.get("expects_visualization")).lower() == "true" and "save_plot" not in candidate:
        return "missing_visualization_contract"
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
        "code_source": "none",
        "codegen_error": None,
        "provider_selected": str(model_source or ""),
        "provider_mode": str(provider_mode or ""),
        "model_name": str(model_name or ""),
        "codegen_plan_status": "disabled",
        "codegen_plan_error": None,
        "analysis_goal": None,
        "analysis_plan": None,
        "plan_contract": {},
        "complex_analytics_code_generation_prompt_status": "disabled",
        "complex_analytics_code_generation_source": "none",
        "complex_analytics_codegen": {"provider": None, "model_route": None},
        "complex_analytics_sandbox": {"secure_eval": True},
    }

    if not meta["codegen_enabled"]:
        meta["code_source"] = "template"
        meta["complex_analytics_code_generation_source"] = "template"
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = "codegen_disabled"
        return fallback_code, meta

    profile = _build_dataframe_profile_for_codegen(primary_frame)
    plan_prompt = _build_complex_analysis_plan_prompt(
        query=query,
        primary_table_name=primary_table_name,
        dataframe_profile=profile,
    )
    routing = _resolve_complex_analytics_routing(
        model_source=model_source,
        provider_mode=provider_mode,
    )
    routing_source = str(routing.get("model_source") or "local")
    routing_mode = str(routing.get("provider_mode") or "explicit")
    plan_timeout_seconds = float(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS", 6.0) or 6.0)
    plan_max_tokens = int(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_PLAN_MAX_TOKENS", 900) or 900)
    code_timeout_seconds = float(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS", 8.0) or 8.0)
    code_max_tokens = int(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_MAX_TOKENS", 2200) or 2200)
    force_local = bool(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL", False))

    meta.update(
        {
            "codegen_attempted": True,
            "codegen_status": "attempted",
            "provider_effective": routing_source,
            "provider_effective_plan": None,
            "provider_effective_codegen": None,
            "provider_overridden": bool(force_local),
        }
    )

    try:
        async def _invoke_plan(prompt: str) -> Dict[str, Any]:
            return await asyncio.wait_for(
                llm_manager.generate_response(
                    prompt=prompt,
                    model_source=routing_source,
                    provider_mode=routing_mode,
                    model_name=model_name,
                    temperature=0.05,
                    max_tokens=plan_max_tokens,
                    conversation_history=None,
                    cannot_wait=True,
                    sla_critical=False,
                    policy_class="complex_analytics_plan",
                ),
                timeout=plan_timeout_seconds,
            )

        async def _invoke_codegen(prompt: str) -> Dict[str, Any]:
            return await asyncio.wait_for(
                llm_manager.generate_response(
                    prompt=prompt,
                    model_source=routing_source,
                    provider_mode=routing_mode,
                    model_name=model_name,
                    temperature=0.05,
                    max_tokens=code_max_tokens,
                    conversation_history=None,
                    cannot_wait=True,
                    sla_critical=False,
                    policy_class="complex_analytics_codegen",
                ),
                timeout=code_timeout_seconds,
            )

        current_plan_prompt = (
            f"{plan_prompt}\nReturn STRICT JSON only and do not include markdown or explanations."
        )

        parsed_plan: Optional[Dict[str, Any]] = None
        plan_error: Optional[str] = None
        plan_analysis_prompt = ""
        plan_contract: Dict[str, Any] = _compute_plan_contract(plan={}, query=query)

        for attempt in range(_CODEGEN_PLAN_ATTEMPTS):
            analysis_plan_result = await _invoke_plan(current_plan_prompt)
            parsed_plan = _extract_json_from_text(str(analysis_plan_result.get("response") or ""))
            meta["provider_effective_plan"] = analysis_plan_result.get("provider_effective")
            meta["provider_effective_codegen"] = analysis_plan_result.get("provider_effective")
            meta["model_route_plan"] = analysis_plan_result.get("model_route")
            meta["provider_selected_plan"] = routing_source
            meta["provider_mode_plan"] = routing_mode

            plan_error = None
            if not isinstance(parsed_plan, dict):
                plan_error = "invalid_plan_json"
            else:
                plan_analysis_prompt = str(parsed_plan.get("python_generation_prompt") or "").strip()
                if not _parse_truthy_bool(parsed_plan.get("should_generate_code")):
                    plan_error = "plan_should_not_generate_code"
                if not plan_analysis_prompt:
                    plan_error = "plan_missing_python_generation_prompt"
                plan_contract = _compute_plan_contract(plan=parsed_plan, query=query)
            if not plan_error:
                break
            if attempt >= (_CODEGEN_PLAN_ATTEMPTS - 1):
                break
            current_plan_prompt = (
                "Return ONLY strict JSON with keys: analysis_goal, required_artifacts, required_outputs, "
                "data_contract, required_contract, python_generation_prompt, should_generate_code.\n"
                f"User: {query!r}\n"
                f"Data profile: {json.dumps(profile, ensure_ascii=False)}\n"
                "This prompt is hard for machine parsing; enforce strict JSON only."
            )

        if plan_error or not isinstance(parsed_plan, dict):
            logger.info(
                "complex_analytics.codegen_plan status=fallback reason=%s provider=%s mode=%s",
                plan_error,
                routing_source,
                routing_mode,
            )
            meta["codegen_plan_status"] = "fallback"
            meta["codegen_plan_error"] = plan_error
            meta["codegen_status"] = "fallback"
            meta["codegen_error"] = plan_error
            meta["code_source"] = "template"
            meta["complex_analytics_code_generation_prompt_status"] = "fallback"
            meta["complex_analytics_code_generation_source"] = "template"
            meta["complex_analytics_codegen"] = {"provider": routing_source, "model_route": None}
            inc_counter("complex_analytics_codegen_total", status="fallback", reason=plan_error)
            return fallback_code, meta

        meta["codegen_plan_status"] = "success"
        meta["complex_analytics_code_generation_prompt_status"] = "success"
        meta["analysis_plan"] = parsed_plan
        meta["analysis_goal"] = str(parsed_plan.get("analysis_goal") or query)
        meta["plan_contract"] = dict(plan_contract)
        logger.info(
            "complex_analytics.codegen_plan status=success provider=%s mode=%s expects_visualization=%s expects_dependency=%s expects_nlp=%s",
            routing_source,
            routing_mode,
            bool(plan_contract.get("expects_visualization")),
            bool(plan_contract.get("expects_dependency")),
            bool(plan_contract.get("expects_nlp")),
        )
        codegen_prompt = _build_codegen_prompt(
            analysis_plan=plan_analysis_prompt,
            primary_table_name=primary_table_name,
            dataframe_profile=profile,
            plan_contract=plan_contract,
        )
        codegen_result: Optional[Dict[str, Any]] = None
        candidate = ""
        contract_error: Optional[str] = "not_attempted"
        prompt = codegen_prompt
        for attempt in range(_CODEGEN_REPAIR_ATTEMPTS):
            codegen_result = await _invoke_codegen(prompt)
            meta["provider_effective_codegen"] = codegen_result.get("provider_effective")
            candidate = _extract_python_from_llm_text(str(codegen_result.get("response") or ""))
            contract_error = _validate_generated_code_contract(candidate, plan_contract=plan_contract)
            if not contract_error:
                break
            prompt = (
                "Return strict Python only. Previous candidate violated contract: "
                f"{contract_error}. Fix and regenerate.\n\n{codegen_prompt}"
            )
            logger.debug(
                "Complex analytics codegen contract validation failed (attempt=%s): %s",
                attempt + 1,
                contract_error,
            )

        if contract_error:
            logger.info(
                "complex_analytics.codegen_execute status=fallback reason=%s provider=%s mode=%s",
                contract_error,
                routing_source,
                routing_mode,
            )
            meta["codegen_status"] = "fallback"
            meta["codegen_error"] = contract_error
            meta["code_source"] = "template"
            meta["complex_analytics_code_generation_source"] = "template"
            meta["complex_analytics_codegen"] = {
                "provider": routing_source,
                "model_route": codegen_result.get("model_route") if isinstance(codegen_result, dict) else None,
            }
            inc_counter("complex_analytics_codegen_total", status="fallback", reason=contract_error)
            return fallback_code, meta

        meta["code_source"] = "llm"
        meta["complex_analytics_code_generation_source"] = "llm"
        meta["model_route"] = codegen_result.get("model_route")
        meta["provider_effective_runtime"] = codegen_result.get("provider_effective")
        meta["provider_selected_runtime"] = routing_source
        meta["provider_mode_runtime"] = routing_mode
        meta["complex_analytics_codegen"] = {
            "provider": codegen_result.get("provider_effective") or routing_source,
            "model_route": codegen_result.get("model_route"),
        }
        meta["codegen_status"] = "success"
        logger.info(
            "complex_analytics.codegen_execute status=success provider=%s model_route=%s",
            codegen_result.get("provider_effective") or routing_source,
            codegen_result.get("model_route"),
        )
        inc_counter("complex_analytics_codegen_total", status="success", reason="none")
        return candidate, meta
    except TimeoutError:
        logger.info(
            "complex_analytics.codegen_execute status=fallback reason=timeout provider=%s mode=%s",
            routing_source,
            routing_mode,
        )
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = "timeout"
        meta["code_source"] = "template"
        meta["complex_analytics_code_generation_prompt_status"] = (
            "fallback" if meta.get("codegen_plan_status") != "success" else "success"
        )
        meta["complex_analytics_code_generation_source"] = "template"
        meta["complex_analytics_codegen"] = {"provider": routing_source, "model_route": None}
        inc_counter("complex_analytics_codegen_total", status="fallback", reason="timeout")
        return fallback_code, meta
    except Exception as exc:  # pragma: no cover - provider/runtime dependent
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = f"runtime_error:{type(exc).__name__}"
        meta["code_source"] = "template"
        meta["complex_analytics_code_generation_prompt_status"] = (
            "fallback" if meta.get("codegen_plan_status") != "success" else "success"
        )
        meta["complex_analytics_code_generation_source"] = "template"
        meta["complex_analytics_codegen"] = {"provider": routing_source, "model_route": None}
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
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.filterwarnings(
            "ignore",
            message=r"Could not infer format, so each element will be parsed individually.*",
        )
        try:
            return pd.to_datetime(series, errors="coerce")
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


def _truncate_for_prompt(value: Any, max_chars: int = 1000) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _build_complex_analytics_execution_context(
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
        "insights": [str(x) for x in metric_insights[:12] if str(x).strip()],
        "notes": [str(n) for n in list(notes)[:12] if str(n).strip()],
        "raw_output": _truncate_for_prompt(execution_stdout, max_chars=4000),
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
        "code_preview": _truncate_for_prompt(executed_code, max_chars=4200),
    }


def _build_complex_analytics_response_prompt(
    *,
    execution_query: str,
    execution_context: Dict[str, Any],
) -> str:
    include_code = _wants_python_code(execution_query)
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


async def _compose_complex_analytics_response(
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
    context = _build_complex_analytics_execution_context(
        query=query,
        table_name=table_name,
        metrics=metrics,
        notes=notes,
        artifacts=artifacts,
        executed_code=executed_code,
        execution_stdout=metrics.get("stdout", ""),
    )
    prompt = _build_complex_analytics_response_prompt(
        execution_query=query,
        execution_context=context,
    )
    timeout_seconds = float(getattr(settings, "COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS", 10.0) or 10.0)
    max_tokens = int(getattr(settings, "COMPLEX_ANALYTICS_RESPONSE_MAX_TOKENS", 1800) or 1800)
    routing = _resolve_complex_analytics_routing(
        model_source=model_source,
        provider_mode=provider_mode,
    )
    selected_source = str(routing.get("model_source") or "local")
    selected_mode = str(routing.get("provider_mode") or "explicit")
    meta: Dict[str, Any] = {
        "provider_source": selected_source,
        "provider_mode": selected_mode,
        "response_status": "not_attempted",
        "response_error_code": None,
        "model_route": None,
        "provider_effective": None,
        "provider_source_selected": selected_source,
    }
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
            meta["response_status"] = "fallback"
            meta["response_error_code"] = "empty_response"
            logger.info(
                "complex_analytics.compose status=fallback reason=empty_response provider=%s mode=%s",
                selected_source,
                selected_mode,
            )
            return "", meta
        meta["response_status"] = "success"
        meta["model_route"] = response.get("model_route")
        meta["provider_effective"] = response.get("provider_effective")
        logger.info(
            "complex_analytics.compose status=success provider=%s model_route=%s",
            meta["provider_effective"] or selected_source,
            meta["model_route"],
        )
        return text, meta
    except TimeoutError:
        meta["response_status"] = "error"
        meta["response_error_code"] = "timeout"
        logger.info(
            "complex_analytics.compose status=error reason=timeout provider=%s mode=%s",
            selected_source,
            selected_mode,
        )
        return "", meta
    except Exception as exc:  # pragma: no cover - provider/runtime dependent
        meta["response_status"] = "error"
        meta["response_error_code"] = f"runtime:{type(exc).__name__}"
        logger.warning("Complex analytics response composition failed: %s", exc)
        return "", meta

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
    if code == COMPLEX_ANALYTICS_ERROR_CODEGEN:
        return (
            "Complex analytics code generation failed for this request. "
            "Please specify target columns/metrics/charts explicitly and retry."
        )
    if code == COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS:
        return (
            "Visualization was requested but no valid chart artifacts were produced. "
            "Specify concrete columns for dependency analysis (for example: x, y, grouping) and retry."
        )
    if code == COMPLEX_ANALYTICS_ERROR_VALIDATION:
        return "Complex analytics result validation failed. Clarify requested outputs and retry."
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
                "complex_analytics_code_generation_prompt_status": "unknown",
                "complex_analytics_code_generation_source": "unknown",
                "complex_analytics_codegen": {"provider": None, "model_route": None},
                "sandbox": {"secure_eval": True},
                "response_status": "not_attempted",
                "response_error_code": None,
                "response_meta": None,
            },
        },
    }


def _validate_executor_result(
    *,
    sandbox_result: SandboxExecutionResult,
    plan_contract: Optional[Dict[str, Any]],
) -> None:
    result_payload = sandbox_result.result if isinstance(sandbox_result.result, dict) else {}
    if not isinstance(result_payload.get("metrics"), dict):
        raise ComplexAnalyticsValidationError(
            COMPLEX_ANALYTICS_ERROR_VALIDATION,
            "Sandbox result must contain metrics dictionary",
        )

    contract = plan_contract if isinstance(plan_contract, dict) else {}
    expects_visualization = bool(contract.get("expects_visualization"))
    if expects_visualization and not sandbox_result.artifacts:
        raise ComplexAnalyticsValidationError(
            COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS,
            "Visualization requested but no chart artifacts produced",
        )


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
    plan_contract = (
        dict(effective_codegen_meta.get("plan_contract"))
        if isinstance(effective_codegen_meta.get("plan_contract"), dict)
        else {}
    )
    try:
        sandbox_result = execute_sandboxed_python(
            code=effective_code,
            datasets=datasets,
            artifacts_dir=artifacts_dir,
            max_output_chars=int(settings.COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS),
            max_artifacts=int(settings.COMPLEX_ANALYTICS_MAX_ARTIFACTS),
        )
    except Exception as exec_error:
        if (
            str((effective_codegen_meta or {}).get("code_source") or "") == "llm"
            and bool(getattr(settings, "COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK", False))
        ):
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
            raise ComplexAnalyticsValidationError(
                COMPLEX_ANALYTICS_ERROR_VALIDATION,
                f"Generated code runtime failure: {type(exec_error).__name__}: {exec_error}",
            ) from exec_error

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

    sandbox_result = SandboxExecutionResult(
        result=sandbox_result.result,
        stdout=sandbox_result.stdout,
        artifacts=response_artifacts,
    )
    _validate_executor_result(
        sandbox_result=sandbox_result,
        plan_contract=plan_contract,
    )

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
                "complex_analytics_code_generation_prompt_status": str(
                    (effective_codegen_meta or {}).get("complex_analytics_code_generation_prompt_status") or "unknown"
                ),
                "complex_analytics_code_generation_source": str(
                    (effective_codegen_meta or {}).get("complex_analytics_code_generation_source") or "unknown"
                ),
                "complex_analytics_codegen": dict((effective_codegen_meta or {}).get("complex_analytics_codegen") or {}),
                "sandbox": {
                    "secure_eval": True,
                    "artifacts_limit": int(settings.COMPLEX_ANALYTICS_MAX_ARTIFACTS),
                    "output_limit_chars": int(settings.COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS),
                },
                "plan_contract": dict((effective_codegen_meta or {}).get("plan_contract") or {}),
                "metrics": metrics if isinstance(metrics, dict) else {},
                "notes": notes if isinstance(notes, list) else [],
                "artifact_dir": _artifact_relative_path(str(artifacts_dir)) or run_id,
                "response_status": "not_attempted",
                "response_error_code": None,
                "response_meta": None,
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
    code_source = str((codegen_meta or {}).get("code_source") or "none")
    if code_source != "llm" and not bool(getattr(settings, "COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK", False)):
        reason = str((codegen_meta or {}).get("codegen_error") or "codegen_unavailable")
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_CODEGEN,
            message=f"Template fallback disabled; reason={reason}",
            debug_details={"codegen": dict(codegen_meta or {})},
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
        if status == "ok":
            fallback_response = str(payload.get("final_response") or "").strip()
            execution_metrics = payload.get("debug", {}).get("complex_analytics", {}).get("metrics", {})
            execution_notes = payload.get("debug", {}).get("complex_analytics", {}).get("notes", [])
            execution_stdout = payload.get("debug", {}).get("complex_analytics", {}).get("stdout", "")
            execution_code = payload.get("debug", {}).get("complex_analytics", {}).get("code_preview") or ""
            if isinstance(execution_metrics, dict) and execution_stdout:
                execution_metrics = dict(execution_metrics)
                execution_metrics["stdout"] = execution_stdout
            generated_response, response_meta = await _compose_complex_analytics_response(
                query=query,
                table_name=primary_table.table_name,
                metrics=execution_metrics if isinstance(execution_metrics, dict) else {},
                notes=execution_notes if isinstance(execution_notes, list) else [],
                artifacts=payload.get("artifacts") or [],
                executed_code=execution_code if isinstance(execution_code, str) else "",
                model_source=model_source,
                provider_mode=provider_mode,
                model_name=model_name,
            )
            if generated_response:
                payload["final_response"] = generated_response
                payload["debug"]["complex_analytics"]["response_meta"] = response_meta
                payload["debug"]["complex_analytics"]["response_status"] = response_meta.get("response_status")
                payload["debug"]["complex_analytics"]["response_error_code"] = response_meta.get("response_error_code")
            else:
                payload["debug"]["complex_analytics"]["response_status"] = "fallback"
                if response_meta:
                    payload["debug"]["complex_analytics"]["response_error_code"] = (
                        response_meta.get("response_error_code") or "unknown"
                    )
                    payload["debug"]["complex_analytics"]["response_meta"] = response_meta
            if not isinstance(payload.get("final_response"), str) or not payload["final_response"].strip():
                payload["final_response"] = fallback_response or _format_complex_analytics_answer(
                    query=query,
                    table_name=primary_table.table_name,
                    metrics=payload.get("debug", {}).get("complex_analytics", {}).get("metrics", {}),
                    notes=payload.get("debug", {}).get("complex_analytics", {}).get("notes", []),
                    artifacts=payload.get("artifacts") or [],
                    executed_code=payload.get("debug", {}).get("complex_analytics", {}).get("code_preview") or "",
                    include_code=_wants_python_code(query),
                    insights=payload.get("debug", {}).get("complex_analytics", {}).get("metrics", {}).get("insights", []),
                )
        if payload["debug"]["complex_analytics"].get("response_status") is None:
            payload["debug"]["complex_analytics"]["response_status"] = "not_attempted"
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
    except ComplexAnalyticsValidationError as exc:
        error_code = str(exc.error_code or COMPLEX_ANALYTICS_ERROR_VALIDATION)
        inc_counter("complex_analytics_executor_error_total", error_code=error_code)
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=error_code,
            message=str(exc),
            debug_details={"codegen": dict(codegen_meta or {})},
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






