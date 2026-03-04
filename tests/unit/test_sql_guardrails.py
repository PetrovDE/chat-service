import pytest

from app.services.tabular.sql_errors import (
    SQL_ERROR_GUARDRAIL_BLOCKED,
    SQL_ERROR_RESULT_LIMIT_EXCEEDED,
    TabularSQLException,
)
from app.services.tabular.sql_guardrails import GuardrailsConfig, SQLGuardrails


def test_sql_guardrails_rejects_non_select_statement():
    guardrails = SQLGuardrails(GuardrailsConfig(max_result_rows=50))

    with pytest.raises(TabularSQLException) as err:
        guardrails.enforce("DELETE FROM sheet_1")

    assert err.value.code == SQL_ERROR_GUARDRAIL_BLOCKED
    payload = err.value.as_payload()
    assert payload["policy_decision"]["allowed"] is False


def test_sql_guardrails_rejects_limit_over_configured_max():
    guardrails = SQLGuardrails(GuardrailsConfig(max_result_rows=10))

    with pytest.raises(TabularSQLException) as err:
        guardrails.enforce("SELECT city FROM sheet_1 LIMIT 50")

    assert err.value.code == SQL_ERROR_RESULT_LIMIT_EXCEEDED


def test_sql_guardrails_happy_path_sets_policy_and_flags():
    guardrails = SQLGuardrails(GuardrailsConfig(max_result_rows=10))

    sql, debug = guardrails.enforce("SELECT city FROM sheet_1 ORDER BY city")

    assert sql.endswith(" LIMIT 10")
    assert debug["valid"] is True
    assert debug["policy_decision"]["allowed"] is True
    assert isinstance(debug["guardrail_flags"], list)
