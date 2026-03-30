import asyncio
import json
import uuid
from types import SimpleNamespace

from app.schemas import ChatMessage
from app.services.chat_orchestrator import ChatOrchestrator


def _build_context(
    conversation_id: uuid.UUID,
    *,
    selected_route: str = "schema_question",
    retrieval_mode: str = "hybrid",
    execution_route: str = "narrative",
    file_resolution_status: str = "conversation_match",
    followup_context_used: bool = True,
    prior_tabular_intent_reused: bool = True,
    context_docs: list | None = None,
    rag_sources: list[str] | None = None,
    retrieval_hits: int = 2,
    unmatched_fields: list[str] | None = None,
    matched_columns: list[str] | None = None,
    history_for_generation: list[dict[str, str]] | None = None,
) -> dict:
    docs = (
        context_docs
        if context_docs is not None
        else [
            {
                "content": "status values: new, approved, archived; city values include London and Berlin.",
                "metadata": {"filename": "dataset.xlsx", "chunk_index": 0, "file_id": "file-1"},
            }
        ]
    )
    return {
        "conversation_id": conversation_id,
        "provider_source_selected_raw": "local",
        "provider_source_effective": "ollama",
        "provider_model_effective": "llama-test",
        "provider_mode": "explicit",
        "final_prompt": "Answer from available runtime evidence.",
        "rag_used": False,
        "rag_debug": {
            "execution_route": execution_route,
            "retrieval_mode": retrieval_mode,
            "selected_route": selected_route,
            "detected_intent": "file_question" if selected_route != "general_chat" else "general_chat",
            "file_resolution_status": file_resolution_status,
            "resolved_file_ids": ["file-1"],
            "file_ids": ["file-1"],
            "retrieval_hits": retrieval_hits,
            "retrieved_chunks_total": retrieval_hits,
            "followup_context_used": followup_context_used,
            "prior_tabular_intent_reused": prior_tabular_intent_reused,
            "fallback_type": "none",
            "fallback_reason": "none",
            "matched_columns": list(matched_columns or ["status", "city"]),
            "unmatched_requested_fields": list(unmatched_fields or []),
        },
        "context_docs": docs,
        "rag_caveats": [],
        "rag_sources": list(rag_sources or ["dataset.xlsx | sheet=Sheet1 | chunk=0"]),
        "history_for_generation": list(
            history_for_generation
            or [
                {"role": "user", "content": "what columns are in the file"},
                {"role": "assistant", "content": "Columns include city, status, amount."},
            ]
        ),
        "preferred_lang": "en",
        "primary_file_id": "file-1",
    }


def _llm_payload(response: str) -> dict:
    return {
        "response": response,
        "model": "llama-test",
        "model_route": "ollama",
        "route_mode": "explicit",
        "provider_selected": "local",
        "provider_effective": "ollama",
        "fallback_reason": "none",
        "fallback_allowed": False,
        "fallback_attempted": False,
        "fallback_policy_version": "p1-aihub-first-v1",
        "aihub_attempted": False,
        "tokens_used": 42,
        "provider_debug": {},
    }


def _parse_sse_events(sse_text: str) -> list[dict]:
    events: list[dict] = []
    for line in sse_text.splitlines():
        if not line.startswith("data: "):
            continue
        data = line[6:]
        if data == "[DONE]":
            continue
        try:
            payload = json.loads(data)
        except Exception:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def test_stream_file_aware_output_is_pre_emission_grounded(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    assistant_messages = []
    ctx = _build_context(conversation_id)

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        assistant_messages.append(content)
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        if "Evidence-grounded answer composition task." in prompt:
            return _llm_payload("`status` has three categories: new, approved, archived.")
        return _llm_payload("raw fallback text")

    class FakeTelemetry:
        def as_dict(self) -> dict:
            return {
                "model_route": "ollama",
                "route_mode": "explicit",
                "provider_selected": "local",
                "provider_effective": "ollama",
                "fallback_reason": "none",
                "fallback_allowed": False,
                "fallback_attempted": False,
                "fallback_policy_version": "p1-aihub-first-v1",
                "aihub_attempted": False,
            }

    class FakeRoutedStream:
        telemetry = FakeTelemetry()

        @property
        def stream(self):
            async def _iter():
                yield "I cannot access attached files in this environment."

            return _iter()

    async def fake_create_routed_stream(**kwargs):  # noqa: ANN003
        return FakeRoutedStream()

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.create_routed_stream", fake_create_routed_stream)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    async def _run_stream() -> str:
        response = await orchestrator.chat_stream(
            chat_data=ChatMessage(
                message="what does status show in the file?",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
        chunks = []
        async for item in response.body_iterator:
            chunks.append(item.decode("utf-8") if isinstance(item, (bytes, bytearray)) else str(item))
        return "".join(chunks)

    sse_text = asyncio.run(_run_stream())
    events = _parse_sse_events(sse_text)
    chunk_contents = [str(event.get("content") or "") for event in events if event.get("type") == "chunk"]

    assert any(event.get("type") == "start" for event in events)
    assert any(event.get("type") == "done" for event in events)
    assert chunk_contents
    assert all("cannot access attached files" not in content.lower() for content in chunk_contents)
    assert any("status" in content.lower() and "three categories" in content.lower() for content in chunk_contents)
    assert not any(event.get("type") == "final_refinement" for event in events)
    assert assistant_messages
    assert "cannot access attached files" not in assistant_messages[-1].lower()
    assert "three categories" in assistant_messages[-1].lower()


def test_nonstream_contradictory_file_access_denial_blocked(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    persisted_messages = []
    ctx = _build_context(conversation_id)

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        persisted_messages.append(content)
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        if "Evidence-grounded answer composition task." in prompt:
            return _llm_payload("`status` contains new, approved, and archived.")
        return _llm_payload("I do not have access to the attached file in this chat.")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="what does status show?",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert "do not have access" not in result.response.lower()
    assert "status" in result.response.lower()
    assert persisted_messages and persisted_messages[-1] == result.response
    assert isinstance(result.rag_debug, dict)
    assert result.rag_debug["evidence_gate_applied"] is True
    assert result.rag_debug["evidence_gate_mode"] == "llm_compose"


def test_schema_followup_detail_uses_continuity_and_grounded_compose(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    ctx = _build_context(
        conversation_id,
        selected_route="schema_question",
        retrieval_mode="hybrid",
        history_for_generation=[
            {"role": "user", "content": "what columns are in the file"},
            {"role": "assistant", "content": "city, status, amount"},
            {"role": "user", "content": "show full description for each column"},
        ],
    )
    compose_prompts = []

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,  # noqa: ARG001
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        if "Evidence-grounded answer composition task." in prompt:
            compose_prompts.append(prompt)
            return _llm_payload("city: text location; status: workflow state; amount: numeric transaction value.")
        return _llm_payload("Columns are city, status, amount.")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="show full description for each column",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert "city:" in result.response.lower()
    assert "status:" in result.response.lower()
    assert compose_prompts
    assert "recent_user_turns" in compose_prompts[0]
    assert "what columns are in the file" in compose_prompts[0]
    assert "selected_route: schema_question" in compose_prompts[0]
    assert "file_resolution_status: conversation_match" in compose_prompts[0]


def test_schema_to_analytics_followup_does_not_repeat_schema(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    ctx = _build_context(
        conversation_id,
        selected_route="aggregation",
        retrieval_mode="tabular_sql",
        execution_route="narrative",
    )

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,  # noqa: ARG001
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        if "Evidence-grounded answer composition task." in prompt:
            return _llm_payload("city has the highest record count in the current data.")
        return _llm_payload("Columns: city, status, amount.")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="which column has most records?",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert "highest record count" in result.response.lower()
    assert "columns:" not in result.response.lower()


def test_attached_file_unrelated_coding_question_keeps_general_answer(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    ctx = _build_context(
        conversation_id,
        selected_route="general_chat",
        retrieval_mode="assistant_direct",
        followup_context_used=False,
        prior_tabular_intent_reused=False,
        retrieval_hits=0,
        context_docs=[],
        rag_sources=[],
    )

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,  # noqa: ARG001
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        assert "Evidence-grounded answer composition task." not in prompt
        return _llm_payload("Use matplotlib: plt.plot(x, y); plt.show().")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="write python code for chart rendering",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert result.response.strip() == "Use matplotlib: plt.plot(x, y); plt.show()."
    assert isinstance(result.rag_debug, dict)
    assert "evidence_gate_applied" not in result.rag_debug


def test_arbitrary_column_question_uses_grounded_file_context(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    ctx = _build_context(conversation_id, selected_route="filtering", retrieval_mode="hybrid")

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,  # noqa: ARG001
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        if "Evidence-grounded answer composition task." in prompt:
            return _llm_payload(
                "Column `calc_need_spravka` indicates whether supporting paperwork is required for the record."
            )
        return _llm_payload("I am not sure.")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="explain column calc_need_spravka",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert "calc_need_spravka" in result.response
    assert "required" in result.response.lower()
    assert result.rag_debug["evidence_gate_mode"] == "llm_compose"


def test_insufficient_evidence_returns_precise_missing_details(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()
    ctx = _build_context(
        conversation_id,
        selected_route="unsupported_missing_column",
        retrieval_mode="tabular_sql",
        context_docs=[],
        rag_sources=[],
        retrieval_hits=0,
        unmatched_fields=["calc_need_spravka"],
        matched_columns=["status", "city"],
    )
    compose_calls = {"count": 0}

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return ctx

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,  # noqa: ARG001
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        prompt = str(kwargs.get("prompt") or "")
        if "Evidence-grounded answer composition task." in prompt:
            compose_calls["count"] += 1
        return _llm_payload("I cannot access attached files in this environment.")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.evidence_answer_gate.llm_manager.generate_response", fake_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="what does calc_need_spravka mean?",
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert compose_calls["count"] == 0
    assert "missing:" in result.response.lower()
    assert "calc_need_spravka" in result.response
    assert "closest matched columns" in result.response.lower()
    assert "cannot access attached files" not in result.response.lower()
    assert result.rag_debug["evidence_gate_mode"] == "clarification"
