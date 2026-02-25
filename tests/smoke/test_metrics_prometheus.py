import uuid

from fastapi.testclient import TestClient

from app.main import app
from app.observability.metrics import inc_counter, observe_ms, render_prometheus_metrics


def test_prometheus_render_contains_counter_and_timer():
    suffix = uuid.uuid4().hex[:8]
    counter_name = f"test_counter_{suffix}"
    timer_name = f"test_timer_{suffix}"

    inc_counter(counter_name, endpoint="/test")
    observe_ms(timer_name, 12.5, endpoint="/test")

    body = render_prometheus_metrics()
    assert f"# TYPE {counter_name} counter" in body
    assert f'{counter_name}{{endpoint="/test"}} 1' in body
    assert f"# TYPE {timer_name}_count counter" in body
    assert f'{timer_name}_sum{{endpoint="/test"}} 12.5' in body


def test_metrics_endpoint_exposed():
    with TestClient(app) as client:
        client.get("/health")
        response = client.get("/metrics")
        assert response.status_code == 200
        assert "text/plain" in response.headers.get("content-type", "")
        assert "http_requests_total" in response.text
