from fastapi.testclient import TestClient

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


def test_models_status_endpoint():
    with TestClient(app) as client:
        response = client.get("/api/v1/models/status")
        data = response.json()
        assert response.status_code == 200
        assert "ollama" in data
        assert "aihub" in data
        assert "openai" in data
