from app.services.chat.tabular_followup_context import apply_tabular_followup_context


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
