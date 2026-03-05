import json

from fastapi.testclient import TestClient
from fastapi.responses import StreamingResponse

from app.api.v1.endpoints import chat as chat_endpoint
from app.main import app


def test_app_starts():
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200


def test_health_endpoint():
    with TestClient(app) as client:
        response = client.get("/health")
        data = response.json()
        assert response.status_code == 200
        assert data["status"] == "healthy"
        assert "timestamp" in data


def test_health_response_has_request_id_header():
    with TestClient(app) as client:
        response = client.get("/health")
        rid = response.headers.get("x-request-id")
        assert response.status_code == 200
        assert isinstance(rid, str)
        assert rid.strip() != ""


def test_health_echoes_incoming_request_id_header():
    with TestClient(app) as client:
        response = client.get("/health", headers={"x-request-id": "rid-smoke-123"})
        assert response.status_code == 200
        assert response.headers.get("x-request-id") == "rid-smoke-123"


def test_access_log_contains_non_empty_request_id(caplog):
    with TestClient(app) as client:
        with caplog.at_level("INFO", logger="app.main"):
            response = client.get("/health", headers={"x-request-id": "rid-log-check"})
    assert response.status_code == 200
    records = [r for r in caplog.records if r.name == "app.main" and "GET /health -> 200" in str(r.getMessage())]
    assert records
    assert all(getattr(r, "request_id", "-") != "-" for r in records)


def test_chat_stream_echoes_incoming_request_id(monkeypatch):
    async def fake_get_db():
        yield None

    async def fake_chat_stream(*, chat_data, db, current_user):  # noqa: ARG001
        async def event_stream():
            yield f"data: {json.dumps({'type': 'start', 'conversation_id': 'c1', 'message_id': 'm1'})}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'content': 'ok'})}\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    app.dependency_overrides[chat_endpoint.get_db] = fake_get_db
    app.dependency_overrides[chat_endpoint.get_current_user_optional] = lambda: None
    monkeypatch.setattr(chat_endpoint.chat_orchestrator, "chat_stream", fake_chat_stream)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/chat/stream",
                headers={"x-request-id": "rid-stream-123"},
                json={"message": "hello"},
            )
            assert response.status_code == 200
            assert "text/event-stream" in str(response.headers.get("content-type", ""))
            assert response.headers.get("x-request-id") == "rid-stream-123"
    finally:
        app.dependency_overrides.pop(chat_endpoint.get_db, None)
        app.dependency_overrides.pop(chat_endpoint.get_current_user_optional, None)


def test_chat_stream_generates_request_id_when_missing(monkeypatch):
    async def fake_get_db():
        yield None

    async def fake_chat_stream(*, chat_data, db, current_user):  # noqa: ARG001
        async def event_stream():
            yield f"data: {json.dumps({'type': 'done', 'content': 'ok'})}\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    app.dependency_overrides[chat_endpoint.get_db] = fake_get_db
    app.dependency_overrides[chat_endpoint.get_current_user_optional] = lambda: None
    monkeypatch.setattr(chat_endpoint.chat_orchestrator, "chat_stream", fake_chat_stream)

    try:
        with TestClient(app) as client:
            response = client.post("/api/v1/chat/stream", json={"message": "hello"})
            assert response.status_code == 200
            rid = response.headers.get("x-request-id")
            assert isinstance(rid, str)
            assert rid.strip() != ""
    finally:
        app.dependency_overrides.pop(chat_endpoint.get_db, None)
        app.dependency_overrides.pop(chat_endpoint.get_current_user_optional, None)


def test_models_status_endpoint():
    with TestClient(app) as client:
        response = client.get("/api/v1/models/status")
        data = response.json()
        assert response.status_code == 200
        assert "ollama" in data
        assert "aihub" in data
        assert "openai" in data
