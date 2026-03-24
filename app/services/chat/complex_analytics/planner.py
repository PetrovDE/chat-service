from __future__ import annotations

import json
import re
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.core.config import settings

COMPLEX_ANALYTICS_HINTS = (
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
    "nlp",
    "comment_text",
    "tokenize",
    "lemmat",
    "sentiment",
    "dataframe",
    "multi-step",
    "multi step",
    "full analysis",
    "analyze this file fully",
    "analyze file fully",
    "analyze dataset fully",
    "полный анализ",
    "проанализируй файл полностью",
    "проанализировать файл полностью",
    "пайтон",
    "питон",
    "хитмап",
    "теплов",
    "зависим",
    "коррел",
    "npl",
)

CODE_BLOCK_PATTERN = re.compile(r"```(?:python|py)?\s*(.*?)```", flags=re.IGNORECASE | re.DOTALL)
MAX_CODE_LINES = 520
CODEGEN_TABLE_SAMPLE_ROWS = 6
CODEGEN_COLUMN_SAMPLE_VALUES = 4


@dataclass
class PlanResult:
    analysis_goal: str
    required_outputs: List[str]
    expects_visualization: bool
    expects_dependency: bool
    expects_nlp: bool
    plan_blob: str
    python_generation_prompt: str
    raw_plan: Dict[str, Any]


def resolve_complex_analytics_routing(
    *,
    model_source: Optional[str],
    provider_mode: Optional[str],
) -> Dict[str, str]:
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
        resolved_mode = "explicit"

    return {"model_source": normalized_source, "provider_mode": resolved_mode}


def is_complex_analytics_query(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    if any(hint in q for hint in COMPLEX_ANALYTICS_HINTS):
        return True
    multi_step_signals = ("then", "after that", "step 1", "step 2", "сначала", "затем", "после этого")
    if any(signal in q for signal in multi_step_signals):
        return True
    return False


def is_dependency_query(query: str) -> bool:
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


def intent_flags_from_query(query: str) -> Dict[str, bool]:
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
        "requires_dependency": is_dependency_query(q),
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


def parse_truthy_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


def contract_from_plan(plan: Dict[str, Any]) -> Dict[str, bool]:
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


def compute_plan_contract(
    *,
    plan: Dict[str, Any],
    query: str,
) -> Dict[str, Any]:
    query_flags = intent_flags_from_query(query)
    plan_outputs = [str(item or "").strip().lower() for item in plan.get("required_outputs", []) if str(item or "").strip()]
    plan_blob = " ".join(plan_outputs)
    explicit_contract = plan.get("required_contract")
    explicit_requires_visualization = bool(
        explicit_contract
        and isinstance(explicit_contract, dict)
        and parse_truthy_bool(explicit_contract.get("expects_visualization"))
    )
    explicit_requires_dependency = bool(
        explicit_contract
        and isinstance(explicit_contract, dict)
        and parse_truthy_bool(explicit_contract.get("expects_dependency"))
    )
    explicit_requires_nlp = bool(
        explicit_contract
        and isinstance(explicit_contract, dict)
        and parse_truthy_bool(explicit_contract.get("expects_nlp"))
    )

    contract = contract_from_plan(plan)
    if explicit_requires_visualization:
        contract["expects_visualization"] = True
    if explicit_requires_dependency:
        contract["expects_dependency"] = True
    if explicit_requires_nlp:
        contract["expects_nlp"] = True

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


def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
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


def extract_python_from_llm_text(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    matches = CODE_BLOCK_PATTERN.findall(raw)
    candidate = max(matches, key=lambda item: len(str(item or ""))) if matches else raw
    code = str(candidate or "").replace("\r\n", "\n").strip()
    if code.lower().startswith("python\n"):
        code = code.split("\n", 1)[1]
    return code.strip()


def build_dataframe_profile_for_codegen(df: Any) -> Dict[str, Any]:
    sample_rows: List[Dict[str, Any]] = []
    try:
        sample_rows = [
            {str(k): str(v) for k, v in row.items()}
            for row in df.head(CODEGEN_TABLE_SAMPLE_ROWS).to_dict(orient="records")
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
        sample_values = [str(v) for v in clean.astype(str).head(CODEGEN_COLUMN_SAMPLE_VALUES).tolist()]
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


def build_complex_analysis_plan_prompt(
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
- Keep target script under {MAX_CODE_LINES} lines.

User request:
{query}

Data profile (JSON):
{profile_snippet}
        """
    ).strip()


# Compatibility aliases for previous private names.
_resolve_complex_analytics_routing = resolve_complex_analytics_routing
_is_dependency_query = is_dependency_query
_intent_flags_from_query = intent_flags_from_query
_contract_from_plan = contract_from_plan
_parse_truthy_bool = parse_truthy_bool
_compute_plan_contract = compute_plan_contract
_extract_json_from_text = extract_json_from_text
_extract_python_from_llm_text = extract_python_from_llm_text
_build_dataframe_profile_for_codegen = build_dataframe_profile_for_codegen
_build_complex_analysis_plan_prompt = build_complex_analysis_plan_prompt
