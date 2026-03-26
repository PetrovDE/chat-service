import asyncio
import uuid

from app.services.chat import rag_prompt_builder as rag_builder


def test_build_rag_prompt_delegates_orchestration_helpers(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    calls = {
        "load_conversation_files": False,
        "resolve_file_references": False,
        "classify_top_level_intent": False,
        "inject_file_resolution_debug": False,
        "log_fallback_cache_event": False,
    }

    async def fake_load_conversation_files(
        *,
        crud_file_module,
        db,  # noqa: ARG001
        conversation_id,
        user_id,
        file_ids,  # noqa: ARG001
    ):
        calls["load_conversation_files"] = True
        assert crud_file_module is rag_builder.crud_file
        assert str(conversation_id)
        assert str(user_id)
        return []

    async def fake_resolve_file_references(
        *,
        crud_file_module,
        db,  # noqa: ARG001
        user_id,
        conversation_id,
        query,  # noqa: ARG001
        files,
        file_ids,  # noqa: ARG001
        preferred_lang,  # noqa: ARG001
        rag_mode,  # noqa: ARG001
        top_k,  # noqa: ARG001
    ):
        calls["resolve_file_references"] = True
        assert crud_file_module is rag_builder.crud_file
        assert str(user_id)
        assert str(conversation_id)
        assert files == []
        return [], {"file_resolution_status": "not_requested"}, None

    def fake_classify_top_level_intent(*, query, resolution_meta):
        calls["classify_top_level_intent"] = True
        assert query == "hello"
        assert resolution_meta["file_resolution_status"] == "not_requested"
        return "general_chat"

    def fake_inject_file_resolution_debug(
        *,
        rag_debug,
        resolution_meta,  # noqa: ARG001
        preferred_lang,  # noqa: ARG001
        query,  # noqa: ARG001
        user_id,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
    ):
        calls["inject_file_resolution_debug"] = True
        payload = dict(rag_debug or {})
        payload["delegated_debug_helper"] = True
        return payload

    def fake_log_fallback_cache_event(*, user_id, conversation_id, rag_debug):  # noqa: ARG001
        calls["log_fallback_cache_event"] = True
        assert rag_debug["delegated_debug_helper"] is True

    monkeypatch.setattr(rag_builder.file_resolution, "load_conversation_files", fake_load_conversation_files)
    monkeypatch.setattr(rag_builder.file_resolution, "resolve_file_references", fake_resolve_file_references)
    monkeypatch.setattr(rag_builder.intent_classifier, "classify_top_level_intent", fake_classify_top_level_intent)
    monkeypatch.setattr(rag_builder.prompt_debug, "inject_file_resolution_debug", fake_inject_file_resolution_debug)
    monkeypatch.setattr(rag_builder.prompt_debug, "log_fallback_cache_event", fake_log_fallback_cache_event)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="hello",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert isinstance(final_prompt, str) and final_prompt
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["delegated_debug_helper"] is True
    assert all(calls.values())
