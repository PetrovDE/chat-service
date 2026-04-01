from app.services.chat.tabular_followup_context import apply_tabular_followup_context
from app.services.chat.tabular_query_parser import parse_tabular_query


def test_short_temporal_followup_reuses_prior_tabular_intent():
    history = [
        {"role": "user", "content": "show spending by month"},
        {"role": "assistant", "content": "I need a datetime source for month grouping."},
    ]

    result = apply_tabular_followup_context(
        query="take month from dates",
        conversation_history=history,
    )

    assert result.followup_context_used is True
    assert result.prior_tabular_intent_reused is True
    assert "show spending by month" in result.effective_query
    assert "take month from dates" in result.effective_query


def test_non_tabular_followup_does_not_reuse_context():
    history = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "Hi there."},
    ]

    result = apply_tabular_followup_context(
        query="yes, from the date",
        conversation_history=history,
    )

    assert result.followup_context_used is False
    assert result.prior_tabular_intent_reused is False
    assert result.effective_query == "yes, from the date"


def test_russian_temporal_followup_reuses_prior_tabular_context():
    history = [
        {
            "role": "user",
            "content": (
                "\u041f\u043e\u043a\u0430\u0436\u0438 \u0433\u0440\u0430\u0444\u0438\u043a "
                "\u043e\u0431\u044a\u0435\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442 "
                "\u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c"
            ),
        },
        {"role": "assistant", "content": "clarify"},
    ]
    followup = (
        "\u043c\u0435\u0441\u044f\u0446\u0430 \u043c\u043e\u0436\u043d\u043e "
        "\u0432\u0437\u044f\u0442\u044c \u0438\u0437 \u0434\u0430\u0442"
    )
    result = apply_tabular_followup_context(
        query=followup,
        conversation_history=history,
    )

    assert result.followup_context_used is True
    assert result.prior_tabular_intent_reused is True
    assert "Follow-up refinement" in result.effective_query
    assert followup in result.effective_query


def test_column_description_followup_reuses_prior_schema_intent():
    history = [
        {"role": "user", "content": "what columns are in the file"},
        {"role": "assistant", "content": "Here is the schema."},
    ]

    result = apply_tabular_followup_context(
        query="show full description for each column",
        conversation_history=history,
    )

    assert result.followup_context_used is True
    assert result.prior_tabular_intent_reused is True
    assert "Follow-up refinement" in result.effective_query
    assert "what columns are in the file" in result.effective_query


def test_column_description_followup_parses_as_overview_not_schema():
    history = [
        {"role": "user", "content": "what columns are in the file"},
        {"role": "assistant", "content": "Here is the schema."},
    ]

    result = apply_tabular_followup_context(
        query="show full description for each column",
        conversation_history=history,
    )
    parsed = parse_tabular_query(result.effective_query)

    assert result.followup_context_used is True
    assert parsed.route == "overview"
    assert parsed.legacy_intent == "profile"


def test_analytics_followup_keeps_group_dimension_in_effective_query():
    history = [
        {"role": "user", "content": "what columns are in the file"},
        {"role": "assistant", "content": "Columns include office and status."},
    ]

    result = apply_tabular_followup_context(
        query="\u043f\u043e \u043a\u0430\u043a\u043e\u043c\u0443 office \u0431\u043e\u043b\u044c\u0448\u0435 \u0432\u0441\u0435\u0433\u043e \u0437\u0430\u043f\u0438\u0441\u0435\u0439?",
        conversation_history=history,
    )
    parsed = parse_tabular_query(result.effective_query)

    assert result.followup_context_used is True
    assert parsed.route == "aggregation"
    assert parsed.operation == "count"
    assert parsed.group_by_field_text == "office"


def test_coding_followup_does_not_hijack_tabular_context():
    history = [
        {"role": "user", "content": "what columns are in the file"},
        {"role": "assistant", "content": "Here is the schema."},
    ]

    result = apply_tabular_followup_context(
        query="write python code for chart rendering",
        conversation_history=history,
    )

    assert result.followup_context_used is False
    assert result.prior_tabular_intent_reused is False
    assert result.effective_query == "write python code for chart rendering"
