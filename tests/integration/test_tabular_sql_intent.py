from app.services.chat.tabular_sql import detect_tabular_intent, is_tabular_aggregate_intent


def test_tabular_profile_intent_has_priority_over_aggregate_keywords():
    query = (
        "отвечай как аналитик, посмотри файл и дай информацию, какие данные там лежат и сколько их. "
        "По каждой колонке дай статистики и метрики. Проведи общий анализ всего файла."
    )
    assert detect_tabular_intent(query) == "profile"
    assert is_tabular_aggregate_intent(query) is False


def test_tabular_aggregate_intent_detects_simple_count_query():
    query = "Сколько строк в файле по всем строкам?"
    assert detect_tabular_intent(query) == "aggregate"
    assert is_tabular_aggregate_intent(query) is True
