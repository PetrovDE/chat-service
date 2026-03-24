import asyncio

from app.services.chat.complex_analytics import composer


def test_composer_formats_english_report():
    text = composer.format_complex_analytics_answer(
        query="Build dependency chart",
        table_name="sheet_1",
        metrics={"rows_total": 3, "columns_total": 2, "columns": ["a", "b"], "column_profile": []},
        notes=[],
        artifacts=[],
        executed_code="result = {}",
        include_code=False,
        insights=[],
    )
    assert "Analysis Result" in text
    assert "Full Analytics Report" not in text


def test_composer_formats_russian_report():
    text = composer.format_complex_analytics_answer(
        query="\u0421\u0434\u0435\u043b\u0430\u0439 \u043f\u043e\u043b\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437 \u0438 \u0433\u0440\u0430\u0444\u0438\u043a\u0438",
        table_name="sheet_1",
        metrics={"rows_total": 3, "columns_total": 2, "columns": ["a", "b"], "column_profile": []},
        notes=[],
        artifacts=[],
        executed_code="result = {}",
        include_code=False,
        insights=[],
    )
    assert "\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u0430\u043d\u0430\u043b\u0438\u0437\u0430" in text
    assert "\u041f\u043e\u043b\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0442\u0438\u0447\u0435\u0441\u043a\u0438\u0439 \u043e\u0442\u0447\u0435\u0442" not in text


def test_compose_quality_gate_rejects_too_short_llm_output(monkeypatch):
    async def fake_generate_response(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {"response": "Done.", "model_route": "ollama", "provider_effective": "ollama"}

    monkeypatch.setattr(composer.llm_manager, "generate_response", fake_generate_response)

    text, meta = asyncio.run(
        composer.compose_complex_analytics_response(
            query="Analyze the dataset fully and add charts",
            table_name="sheet_1",
            metrics={"rows_total": 10, "columns_total": 3, "columns": ["a", "b", "c"]},
            notes=[],
            artifacts=[{"kind": "histogram", "name": "x.png", "path": "uploads/x.png", "url": "/uploads/x.png"}],
            executed_code="result = {}",
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert text == ""
    assert meta.get("response_status") == "fallback"
    assert meta.get("response_error_code") == "low_content_quality"


def test_compose_quality_gate_accepts_structured_output():
    response = """
## Full Analytics Report
### 1) Summary
Rows and columns metrics are calculated.
### 2) Key Insights
Distribution and dependency patterns detected.
### 3) Relationships Between Features
Strong correlation observed between amount and duration.
### 4) Visualizations
Histogram chart available at /uploads/x.png with interpretation.
![histogram](/uploads/x.png)
### 5) Recommendations
Review outliers and segment by category for next analysis iteration.
"""
    ok = composer.is_compose_response_sufficient(
        text=response,
        query="Analyze dataset and build charts",
        execution_context={"artifacts": [{"url": "/uploads/x.png"}]},
    )
    assert ok is True


def test_compose_quality_gate_rejects_generic_processed_message_even_with_sections():
    response = """
## Сообщение об обработке запроса
### 1) Профиль данных и ключевые метрики
Запрос был обработан.
### 2) Аналитический анализ и выводы
Рекомендуется deeper analysis.
### 3) Визуализации
График доступен по ссылке /uploads/x.png
![histogram](/uploads/x.png)
### 4) Practical рекомендации
Нужно сделать проверку.
"""
    ok = composer.is_compose_response_sufficient(
        text=response,
        query="Проанализируй файл полностью, статистики и связи",
        execution_context={
            "columns": ["amount_rub", "status", "priority"],
            "artifacts": [{"url": "/uploads/x.png"}],
        },
    )
    assert ok is False


def test_compose_aihub_policy_timeout_override_allows_slow_provider(monkeypatch):
    async def fake_generate_response(**kwargs):  # noqa: ANN003
        _ = kwargs
        await asyncio.sleep(0.03)
        return {
            "response": """
## Full Analytics Report
### 1) Summary
Rows and columns metrics are calculated.
### 2) Key Insights
Distribution and dependency patterns detected.
### 3) Relationships Between Features
Strong correlation observed between amount and duration.
### 4) Visualizations
Histogram chart available at /uploads/x.png with interpretation.
![histogram](/uploads/x.png)
### 5) Recommendations
Review outliers and segment by category for next analysis iteration.
""",
            "model_route": "aihub",
            "provider_effective": "aihub",
        }

    monkeypatch.setattr(composer.llm_manager, "generate_response", fake_generate_response)
    monkeypatch.setattr(composer.settings, "COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(composer.settings, "COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS_AIHUB_POLICY", 0.2)

    text, meta = asyncio.run(
        composer.compose_complex_analytics_response(
            query="Analyze dataset and build charts",
            table_name="sheet_1",
            metrics={"rows_total": 10, "columns_total": 3, "columns": ["amount", "duration", "status"]},
            notes=[],
            artifacts=[{"kind": "histogram", "name": "x.png", "path": "uploads/x.png", "url": "/uploads/x.png"}],
            executed_code="result = {}",
            model_source="aihub",
            provider_mode="policy",
            model_name="gpt-4.1-mini",
        )
    )

    assert "Full Analytics Report" in text
    assert meta.get("response_status") == "success"
    assert float(meta.get("response_timeout_seconds") or 0.0) >= 0.2
