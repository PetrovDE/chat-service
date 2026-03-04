from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence


SQL_ERROR_GUARDRAIL_BLOCKED = "sql_guardrail_blocked"
SQL_ERROR_SCAN_LIMIT_EXCEEDED = "sql_scan_limit_exceeded"
SQL_ERROR_RESULT_LIMIT_EXCEEDED = "sql_result_limit_exceeded"
SQL_ERROR_RESULT_SIZE_EXCEEDED = "sql_result_size_exceeded"
SQL_ERROR_TIMEOUT = "sql_timeout"
SQL_ERROR_EXECUTION_FAILED = "sql_execution_failed"


@dataclass(frozen=True)
class _ErrorSpec:
    category: str
    retryable: bool


_ERROR_SPECS: Dict[str, _ErrorSpec] = {
    SQL_ERROR_GUARDRAIL_BLOCKED: _ErrorSpec(category="guardrail", retryable=False),
    SQL_ERROR_SCAN_LIMIT_EXCEEDED: _ErrorSpec(category="limit", retryable=False),
    SQL_ERROR_RESULT_LIMIT_EXCEEDED: _ErrorSpec(category="limit", retryable=False),
    SQL_ERROR_RESULT_SIZE_EXCEEDED: _ErrorSpec(category="limit", retryable=False),
    SQL_ERROR_TIMEOUT: _ErrorSpec(category="timeout", retryable=True),
    SQL_ERROR_EXECUTION_FAILED: _ErrorSpec(category="execution", retryable=True),
}


def _spec_for(code: str) -> _ErrorSpec:
    return _ERROR_SPECS.get(str(code), _ErrorSpec(category="execution", retryable=True))


class TabularSQLException(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        executed_sql: Optional[str] = None,
        policy_decision: Optional[Dict[str, Any]] = None,
        guardrail_flags: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)
        self.details = dict(details or {})
        self.executed_sql = executed_sql
        self.policy_decision = dict(policy_decision or {})
        self.guardrail_flags = [str(flag) for flag in (guardrail_flags or []) if str(flag).strip()]

    def as_payload(self) -> Dict[str, Any]:
        spec = _spec_for(self.code)
        payload: Dict[str, Any] = {
            "code": self.code,
            "category": spec.category,
            "message": self.message,
            "retryable": bool(spec.retryable),
            "details": dict(self.details),
            "executed_sql": self.executed_sql,
            "policy_decision": dict(self.policy_decision),
            "guardrail_flags": list(self.guardrail_flags),
        }
        return payload


def to_tabular_error_payload(
    exc: Exception,
    *,
    default_code: str = SQL_ERROR_EXECUTION_FAILED,
    default_message: str = "Tabular SQL execution failed",
) -> Dict[str, Any]:
    if isinstance(exc, TabularSQLException):
        return exc.as_payload()

    fallback = TabularSQLException(
        code=default_code,
        message=default_message,
        details={"exception_type": type(exc).__name__},
    )
    return fallback.as_payload()
