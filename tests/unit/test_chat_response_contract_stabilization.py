import uuid

from app.services.chat.orchestrator_runtime import (
    _build_chat_response,
)
from app.services.chat.orchestrator_stream_payloads import build_stream_contract_fields


def _build_response(
    *,
    rag_debug_ctx,
    artifacts_payload,
    execution_route,
    artifacts_count,
    rag_debug_payload=None,
    debug_enabled=False,
):
    return _build_chat_response(
        response_text="ok",
        conversation_id=uuid.uuid4(),
        message_id=uuid.uuid4(),
        model_used="model-a",
        route_telemetry={
            "model_route": "aihub",
            "route_mode": "policy",
            "provider_selected": "aihub",
            "provider_effective": "aihub",
            "fallback_reason": "none",
            "fallback_allowed": False,
            "fallback_attempted": False,
            "fallback_policy_version": "p1",
            "aihub_attempted": True,
        },
        execution_telemetry={
            "execution_route": execution_route,
            "executor_attempted": False,
            "executor_status": "not_attempted",
            "executor_error_code": None,
            "artifacts_count": artifacts_count,
        },
        generation_time=0.1,
        rag_caveats=[],
        rag_sources=[],
        artifacts_payload=artifacts_payload,
        rag_debug_ctx=rag_debug_ctx,
        rag_debug_payload=rag_debug_payload,
        debug_enabled=debug_enabled,
        tokens_used=None,
        summary=None,
        default_execution_route=execution_route,
        default_executor_status="not_attempted",
    )


def test_general_chat_response_contract():
    result = _build_response(
        rag_debug_ctx={
            "selected_route": "general_chat",
            "retrieval_mode": "assistant_direct",
            "execution_route": "narrative",
            "file_resolution_status": "not_requested",
            "fallback_type": "none",
            "fallback_reason": "none",
            "requires_clarification": False,
        },
        artifacts_payload=[],
        execution_route="narrative",
        artifacts_count=0,
    )

    contract = result.response_contract
    assert contract.response_mode == "general_chat"
    assert contract.clarification_required is False
    assert contract.controlled_fallback is False
    assert contract.artifacts_available is False
    assert contract.chart_artifact_available is False


def test_file_aware_response_contract():
    result = _build_response(
        rag_debug_ctx={
            "selected_route": "narrative",
            "retrieval_mode": "hybrid",
            "execution_route": "narrative",
            "file_resolution_status": "resolved_unique",
            "fallback_type": "none",
            "fallback_reason": "none",
            "requires_clarification": False,
        },
        artifacts_payload=[],
        execution_route="narrative",
        artifacts_count=0,
    )

    contract = result.response_contract
    assert contract.response_mode == "file_aware"
    assert contract.file_resolution_status == "resolved_unique"
    assert contract.controlled_fallback is False


def test_controlled_fallback_response_contract():
    result = _build_response(
        rag_debug_ctx={
            "selected_route": "no_context",
            "retrieval_mode": "no_context_files",
            "execution_route": "clarification",
            "file_resolution_status": "no_context_files",
            "fallback_type": "no_context",
            "fallback_reason": "no_ready_files_in_chat",
            "controlled_response_state": "no_context",
            "requires_clarification": True,
        },
        artifacts_payload=[],
        execution_route="clarification",
        artifacts_count=0,
    )

    contract = result.response_contract
    assert contract.response_mode == "clarification"
    assert contract.clarification_required is True
    assert contract.controlled_fallback is True
    assert contract.fallback_type == "no_context"


def test_chart_response_contract_with_artifact_available():
    artifacts = [{"name": "chart.png", "url": "/uploads/chart.png", "kind": "bar"}]
    result = _build_response(
        rag_debug_ctx={
            "selected_route": "chart",
            "retrieval_mode": "tabular_sql",
            "execution_route": "tabular_sql",
            "chart_spec_generated": True,
            "chart_rendered": True,
            "chart_artifact_available": True,
            "fallback_type": "none",
            "fallback_reason": "none",
            "requires_clarification": False,
        },
        artifacts_payload=artifacts,
        execution_route="tabular_sql",
        artifacts_count=1,
    )

    contract = result.response_contract
    assert contract.response_mode == "chart"
    assert contract.artifacts_available is True
    assert contract.artifacts_count == 1
    assert contract.chart_artifact_available is True
    assert contract.controlled_fallback is False


def test_chart_response_contract_with_artifact_unavailable():
    result = _build_response(
        rag_debug_ctx={
            "selected_route": "chart",
            "retrieval_mode": "tabular_sql",
            "execution_route": "tabular_sql",
            "chart_spec_generated": True,
            "chart_rendered": False,
            "chart_artifact_available": False,
            "fallback_type": "tabular_chart_render_failed",
            "fallback_reason": "chart_render_failed",
            "controlled_response_state": "chart_render_failed",
            "requires_clarification": False,
        },
        artifacts_payload=[],
        execution_route="tabular_sql",
        artifacts_count=0,
    )

    contract = result.response_contract
    assert contract.response_mode == "chart"
    assert contract.artifacts_available is False
    assert contract.chart_artifact_available is False
    assert contract.controlled_fallback is True
    assert contract.fallback_type == "tabular_chart_render_failed"


def test_debug_true_response_contract():
    result = _build_response(
        rag_debug_ctx={
            "selected_route": "general_chat",
            "retrieval_mode": "assistant_direct",
            "execution_route": "narrative",
            "fallback_type": "none",
            "fallback_reason": "none",
            "requires_clarification": False,
        },
        artifacts_payload=[],
        execution_route="narrative",
        artifacts_count=0,
        rag_debug_payload={"debug_contract_version": "rag_debug_v1"},
        debug_enabled=True,
    )

    contract = result.response_contract
    assert contract.debug_enabled is True
    assert contract.debug_included is True


def test_stream_nonstream_contract_parity_for_stabilized_fields():
    rag_debug_ctx = {
        "selected_route": "chart",
        "retrieval_mode": "tabular_sql",
        "execution_route": "tabular_sql",
        "file_resolution_status": "conversation_match",
        "chart_spec_generated": True,
        "chart_artifact_available": True,
        "fallback_type": "none",
        "fallback_reason": "none",
        "requires_clarification": False,
    }
    artifacts_payload = [{"name": "chart.png", "url": "/uploads/chart.png", "kind": "bar"}]
    route_telemetry = {
        "model_route": "aihub_primary",
        "route_mode": "policy",
        "provider_selected": "aihub",
        "provider_effective": "aihub",
        "fallback_reason": "none",
        "fallback_allowed": False,
        "fallback_attempted": False,
        "fallback_policy_version": "p1",
        "aihub_attempted": True,
    }
    execution_telemetry = {
        "execution_route": "tabular_sql",
        "executor_attempted": False,
        "executor_status": "not_attempted",
        "executor_error_code": None,
        "artifacts_count": 1,
    }

    nonstream = _build_response(
        rag_debug_ctx=rag_debug_ctx,
        artifacts_payload=artifacts_payload,
        execution_route="tabular_sql",
        artifacts_count=1,
        rag_debug_payload={"debug_contract_version": "rag_debug_v1"},
        debug_enabled=True,
    )
    stream_fields = build_stream_contract_fields(
        rag_debug_ctx=rag_debug_ctx,
        route_telemetry=route_telemetry,
        execution_telemetry=execution_telemetry,
        artifacts_payload=artifacts_payload,
        debug_enabled=True,
        debug_included=True,
        default_execution_route="tabular_sql",
        default_executor_status="not_attempted",
    )

    assert nonstream.response_contract.model_dump() == stream_fields["response_contract"]
