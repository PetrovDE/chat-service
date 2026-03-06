from __future__ import annotations

from typing import Any, Dict, Optional


def build_error_debug_payload(
    *,
    query: str,
    dataset_id: Optional[str],
    dataset_version: Optional[int],
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    details_payload = details if isinstance(details, dict) else {}
    codegen_details = details_payload.get("codegen")
    if isinstance(codegen_details, dict):
        auto_visual_patch_applied = bool(
            codegen_details.get("codegen_auto_visual_patch_applied")
            or (codegen_details.get("complex_analytics_codegen") or {}).get("auto_visual_patch_applied")
        )
    else:
        auto_visual_patch_applied = False

    return {
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
            "details": details_payload,
            "codegen_auto_visual_patch_applied": auto_visual_patch_applied,
            "complex_analytics_code_generation_prompt_status": "unknown",
            "complex_analytics_code_generation_source": "unknown",
            "complex_analytics_codegen": {
                "provider": None,
                "model_route": None,
                "auto_visual_patch_applied": auto_visual_patch_applied,
            },
            "sandbox": {"secure_eval": True},
            "response_status": "not_attempted",
            "response_error_code": None,
            "response_meta": None,
        },
    }


def apply_response_meta(debug_payload: Dict[str, Any], response_meta: Optional[Dict[str, Any]]) -> None:
    complex_debug = debug_payload.setdefault("complex_analytics", {})
    if not isinstance(complex_debug, dict):
        return
    if response_meta:
        complex_debug["response_meta"] = response_meta
        complex_debug["response_status"] = response_meta.get("response_status")
        complex_debug["response_error_code"] = response_meta.get("response_error_code")
    elif complex_debug.get("response_status") is None:
        complex_debug["response_status"] = "not_attempted"
