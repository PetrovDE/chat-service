from app.services.chat.tabular_sql import detect_tabular_intent, is_tabular_aggregate_intent


def test_tabular_profile_intent_has_priority_over_aggregate_keywords():
    query = "Show full analysis per column and overall dataset profile."
    assert detect_tabular_intent(query) == "profile"
    assert is_tabular_aggregate_intent(query) is False


def test_tabular_aggregate_intent_detects_simple_count_query():
    query = "How many rows are in the whole file?"
    assert detect_tabular_intent(query) == "aggregate"
    assert is_tabular_aggregate_intent(query) is True


def test_tabular_lookup_intent_detects_row_filter_queries():
    query = "Find rows where client equals acme and show records"
    assert detect_tabular_intent(query) == "lookup"
