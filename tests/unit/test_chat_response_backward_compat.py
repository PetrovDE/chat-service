import uuid

from app.schemas.chat import ChatResponse


def test_chat_response_backward_compatible_defaults_for_route_telemetry():
    payload = {
        "response": "ok",
        "conversation_id": str(uuid.uuid4()),
        "message_id": str(uuid.uuid4()),
        "model_used": "vikhr",
        "tokens_used": 12,
        "generation_time": 0.5,
        "summary": None,
        "caveats": [],
        "sources": [],
        "rag_debug": None,
    }

    parsed = ChatResponse.model_validate(payload)
    assert parsed.model_route == "aihub_primary"
    assert parsed.route_mode == "policy"
    assert parsed.provider_selected is None
    assert parsed.provider_effective == "aihub"
    assert parsed.fallback_reason == "none"
    assert parsed.fallback_allowed is False
    assert parsed.fallback_attempted is False
    assert parsed.aihub_attempted is False
    assert parsed.fallback_policy_version == "p1-aihub-first-v1"
