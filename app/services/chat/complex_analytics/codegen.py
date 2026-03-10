from __future__ import annotations

import ast
import asyncio
import json
import logging
import textwrap
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from app.core.config import settings
from app.observability.metrics import inc_counter
from app.services.llm.manager import llm_manager

from .auto_visual_patch import inject_visualization_fallback
from .errors import ComplexAnalyticsSecurityError
from .planner import (
    MAX_CODE_LINES,
    PlanResult,
    build_complex_analysis_plan_prompt,
    build_dataframe_profile_for_codegen,
    compute_plan_contract,
    extract_json_from_text,
    extract_python_from_llm_text,
    parse_truthy_bool,
    resolve_complex_analytics_routing,
)
from .sandbox import validate_python_security
from .template_codegen import build_complex_analysis_code
from .report_quality import is_broad_full_analysis_query

logger = logging.getLogger(__name__)

CODEGEN_PLAN_ATTEMPTS = 2
CODEGEN_REPAIR_ATTEMPTS = 3


@dataclass
class CodegenMeta:
    payload: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.payload)


def _is_aihub_policy_route(*, model_source: Optional[str], provider_mode: Optional[str]) -> bool:
    source = str(model_source or "").strip().lower()
    mode = str(provider_mode or "").strip().lower()
    return source in {"aihub", "ai_hub", "ai-hub"} and mode == "policy"


def _resolve_timeout_seconds(
    *,
    base_attr: str,
    policy_override_attr: str,
    model_source: Optional[str],
    provider_mode: Optional[str],
    default_value: float,
) -> float:
    base_timeout = float(getattr(settings, base_attr, default_value) or default_value)
    if not _is_aihub_policy_route(model_source=model_source, provider_mode=provider_mode):
        return base_timeout
    policy_timeout = float(getattr(settings, policy_override_attr, base_timeout) or base_timeout)
    return max(base_timeout, policy_timeout)


def _resolve_codegen_timeouts(*, model_source: Optional[str], provider_mode: Optional[str]) -> Tuple[float, float]:
    plan_timeout = _resolve_timeout_seconds(
        base_attr="COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS",
        policy_override_attr="COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS_AIHUB_POLICY",
        model_source=model_source,
        provider_mode=provider_mode,
        default_value=6.0,
    )
    code_timeout = _resolve_timeout_seconds(
        base_attr="COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS",
        policy_override_attr="COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS_AIHUB_POLICY",
        model_source=model_source,
        provider_mode=provider_mode,
        default_value=8.0,
    )
    return plan_timeout, code_timeout


def build_codegen_prompt(
    *,
    query: str,
    analysis_plan: str,
    primary_table_name: str,
    dataframe_profile: Dict[str, Any],
    plan_contract: Dict[str, Any],
) -> str:
    safe_plan = str(analysis_plan or "").strip()
    broad_analysis = is_broad_full_analysis_query(query)
    profile_snippet = json.dumps(dataframe_profile, ensure_ascii=False)[:16000]
    contract_snippet = json.dumps(plan_contract, ensure_ascii=False)
    quality_requirements = (
        "- Build detailed column profiles for all relevant columns and include as metrics['column_profile'].\n"
        "- Compute numeric_summary, datetime_summary, categorical_summary when data supports it.\n"
        "- Compute relationship_findings for numeric feature pairs when >=2 numeric columns are available.\n"
        "- Add concrete insights that reference actual column names from the dataset.\n"
    )
    broad_query_requirements = (
        "- This is a broad/full analysis request: provide comprehensive EDA, not a minimal template.\n"
        "- When visualization is feasible, generate multiple chart artifacts (distribution + categorical + relationship view).\n"
    )
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
- Always prefer context-specific analytics based on dataset profile and user request.
{quality_requirements}{broad_query_requirements if broad_analysis else ""}
- Keep script under {MAX_CODE_LINES} lines.
        """.strip()
    ).strip()


def validate_generated_code_contract(code: str, *, plan_contract: Optional[Dict[str, Any]] = None) -> Optional[str]:
    effective_contract = plan_contract if isinstance(plan_contract, dict) else {}
    candidate = str(code or "").strip()
    if not candidate:
        return "empty_code"
    if len(candidate.splitlines()) > MAX_CODE_LINES:
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
        validate_python_security(candidate)
    except ComplexAnalyticsSecurityError:
        return "security_precheck_failed"
    return None

async def generate_complex_analysis_code(
    *,
    query: str,
    primary_table_name: str,
    primary_frame: Any,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
    llm_client: Optional[Any] = None,
) -> Tuple[str, Dict[str, Any]]:
    client = llm_client or llm_manager
    fallback_code = build_complex_analysis_code(query=query, primary_table_name=primary_table_name)
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
        "codegen_auto_visual_patch_applied": False,
        "complex_analytics_code_generation_prompt_status": "disabled",
        "complex_analytics_code_generation_source": "none",
        "complex_analytics_codegen": {"provider": None, "model_route": None, "auto_visual_patch_applied": False},
        "complex_analytics_sandbox": {"secure_eval": True},
    }

    if not meta["codegen_enabled"]:
        meta["code_source"] = "template"
        meta["complex_analytics_code_generation_source"] = "template"
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = "codegen_disabled"
        return fallback_code, CodegenMeta(meta).as_dict()

    profile = build_dataframe_profile_for_codegen(primary_frame)
    plan_prompt = build_complex_analysis_plan_prompt(
        query=query,
        primary_table_name=primary_table_name,
        dataframe_profile=profile,
    )
    routing = resolve_complex_analytics_routing(
        model_source=model_source,
        provider_mode=provider_mode,
    )
    routing_source = str(routing.get("model_source") or "local")
    routing_mode = str(routing.get("provider_mode") or "explicit")
    plan_timeout_seconds, code_timeout_seconds = _resolve_codegen_timeouts(
        model_source=routing_source,
        provider_mode=routing_mode,
    )
    plan_max_tokens = int(getattr(settings, "COMPLEX_ANALYTICS_CODEGEN_PLAN_MAX_TOKENS", 900) or 900)
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
            "codegen_plan_timeout_seconds": plan_timeout_seconds,
            "codegen_timeout_seconds": code_timeout_seconds,
        }
    )

    try:
        async def _invoke_plan(prompt: str) -> Dict[str, Any]:
            return await asyncio.wait_for(
                client.generate_response(
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
                client.generate_response(
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
        plan_result: Optional[PlanResult] = None
        plan_contract: Dict[str, Any] = compute_plan_contract(plan={}, query=query)

        for attempt in range(CODEGEN_PLAN_ATTEMPTS):
            analysis_plan_result = await _invoke_plan(current_plan_prompt)
            parsed_plan = extract_json_from_text(str(analysis_plan_result.get("response") or ""))
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
                if not parse_truthy_bool(parsed_plan.get("should_generate_code")):
                    plan_error = "plan_should_not_generate_code"
                if not plan_analysis_prompt:
                    plan_error = "plan_missing_python_generation_prompt"
                plan_contract = compute_plan_contract(plan=parsed_plan, query=query)
                plan_result = PlanResult(
                    analysis_goal=str(parsed_plan.get("analysis_goal") or query),
                    required_outputs=list(plan_contract.get("required_outputs") or []),
                    expects_visualization=bool(plan_contract.get("expects_visualization")),
                    expects_dependency=bool(plan_contract.get("expects_dependency")),
                    expects_nlp=bool(plan_contract.get("expects_nlp")),
                    plan_blob=str(plan_contract.get("plan_blob") or ""),
                    python_generation_prompt=plan_analysis_prompt,
                    raw_plan=dict(parsed_plan),
                )
            if not plan_error:
                break
            if attempt >= (CODEGEN_PLAN_ATTEMPTS - 1):
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
            meta["complex_analytics_codegen"] = {
                "provider": routing_source,
                "model_route": None,
                "auto_visual_patch_applied": bool(meta.get("codegen_auto_visual_patch_applied", False)),
            }
            inc_counter("complex_analytics_codegen_total", status="fallback", reason=plan_error)
            return fallback_code, CodegenMeta(meta).as_dict()

        meta["codegen_plan_status"] = "success"
        meta["complex_analytics_code_generation_prompt_status"] = "success"
        meta["analysis_plan"] = dict((plan_result.raw_plan if plan_result else parsed_plan) or {})
        meta["analysis_goal"] = str((plan_result.analysis_goal if plan_result else parsed_plan.get("analysis_goal")) or query)
        meta["plan_contract"] = dict(plan_contract)
        logger.info(
            "complex_analytics.codegen_plan status=success provider=%s mode=%s expects_visualization=%s expects_dependency=%s expects_nlp=%s timeout_plan=%ss timeout_codegen=%ss",
            routing_source,
            routing_mode,
            bool(plan_contract.get("expects_visualization")),
            bool(plan_contract.get("expects_dependency")),
            bool(plan_contract.get("expects_nlp")),
            plan_timeout_seconds,
            code_timeout_seconds,
        )
        codegen_prompt = build_codegen_prompt(
            query=query,
            analysis_plan=(plan_result.python_generation_prompt if plan_result else plan_analysis_prompt),
            primary_table_name=primary_table_name,
            dataframe_profile=profile,
            plan_contract=plan_contract,
        )

        codegen_result: Optional[Dict[str, Any]] = None
        candidate = ""
        contract_error: Optional[str] = "not_attempted"
        prompt = codegen_prompt
        for attempt in range(CODEGEN_REPAIR_ATTEMPTS):
            codegen_result = await _invoke_codegen(prompt)
            meta["provider_effective_codegen"] = codegen_result.get("provider_effective")
            candidate = extract_python_from_llm_text(str(codegen_result.get("response") or ""))
            contract_error = validate_generated_code_contract(candidate, plan_contract=plan_contract)
            if contract_error == "missing_visualization_contract":
                patched_candidate = _inject_visualization_fallback(candidate)
                patched_error = validate_generated_code_contract(patched_candidate, plan_contract=plan_contract)
                if not patched_error:
                    logger.info(
                        "complex_analytics.codegen_execute status=success_via_auto_visual_patch provider=%s mode=%s",
                        routing_source,
                        routing_mode,
                    )
                    candidate = patched_candidate
                    contract_error = None
                    meta["codegen_auto_visual_patch_applied"] = True
                    break
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
                "auto_visual_patch_applied": bool(meta.get("codegen_auto_visual_patch_applied", False)),
            }
            inc_counter("complex_analytics_codegen_total", status="fallback", reason=contract_error)
            return fallback_code, CodegenMeta(meta).as_dict()

        meta["code_source"] = "llm"
        meta["complex_analytics_code_generation_source"] = "llm"
        meta["model_route"] = codegen_result.get("model_route")
        meta["provider_effective_runtime"] = codegen_result.get("provider_effective")
        meta["provider_selected_runtime"] = routing_source
        meta["provider_mode_runtime"] = routing_mode
        meta["complex_analytics_codegen"] = {
            "provider": codegen_result.get("provider_effective") or routing_source,
            "model_route": codegen_result.get("model_route"),
            "auto_visual_patch_applied": bool(meta.get("codegen_auto_visual_patch_applied", False)),
        }
        meta["codegen_status"] = "success"
        logger.info(
            "complex_analytics.codegen_execute status=success provider=%s model_route=%s",
            codegen_result.get("provider_effective") or routing_source,
            codegen_result.get("model_route"),
        )
        inc_counter("complex_analytics_codegen_total", status="success", reason="none")
        return candidate, CodegenMeta(meta).as_dict()
    except TimeoutError:
        logger.info(
            "complex_analytics.codegen_execute status=fallback reason=timeout provider=%s mode=%s timeout_plan=%ss timeout_codegen=%ss",
            routing_source,
            routing_mode,
            plan_timeout_seconds,
            code_timeout_seconds,
        )
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = "timeout"
        meta["code_source"] = "template"
        meta["complex_analytics_code_generation_prompt_status"] = (
            "fallback" if meta.get("codegen_plan_status") != "success" else "success"
        )
        meta["complex_analytics_code_generation_source"] = "template"
        meta["complex_analytics_codegen"] = {
            "provider": routing_source,
            "model_route": None,
            "auto_visual_patch_applied": bool(meta.get("codegen_auto_visual_patch_applied", False)),
        }
        inc_counter("complex_analytics_codegen_total", status="fallback", reason="timeout")
        return fallback_code, CodegenMeta(meta).as_dict()
    except Exception as exc:  # pragma: no cover - provider/runtime dependent
        meta["codegen_status"] = "fallback"
        meta["codegen_error"] = f"runtime_error:{type(exc).__name__}"
        meta["code_source"] = "template"
        meta["complex_analytics_code_generation_prompt_status"] = (
            "fallback" if meta.get("codegen_plan_status") != "success" else "success"
        )
        meta["complex_analytics_code_generation_source"] = "template"
        meta["complex_analytics_codegen"] = {
            "provider": routing_source,
            "model_route": None,
            "auto_visual_patch_applied": bool(meta.get("codegen_auto_visual_patch_applied", False)),
        }
        logger.warning("Complex analytics codegen failed: %s", exc)
        inc_counter("complex_analytics_codegen_total", status="fallback", reason="runtime_error")
        return fallback_code, CodegenMeta(meta).as_dict()

# Compatibility aliases.
_build_codegen_prompt = build_codegen_prompt
_validate_generated_code_contract = validate_generated_code_contract
_inject_visualization_fallback = inject_visualization_fallback
_generate_complex_analysis_code = generate_complex_analysis_code
_build_complex_analysis_code = build_complex_analysis_code
