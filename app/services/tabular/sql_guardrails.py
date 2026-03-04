from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Dict, Optional, Tuple

from app.services.tabular.sql_errors import (
    SQL_ERROR_GUARDRAIL_BLOCKED,
    SQL_ERROR_RESULT_LIMIT_EXCEEDED,
    SQL_ERROR_SCAN_LIMIT_EXCEEDED,
    TabularSQLException,
)


_BLOCKED_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "attach",
    "detach",
    "copy",
    "pragma",
    "vacuum",
    "call",
)

_BLOCKED_PATTERNS = (
    r"\bread_csv(_auto)?\s*\(",
    r"\bread_json(_auto)?\s*\(",
    r"\bread_parquet\s*\(",
    r"\bhttpfs\b",
    r"\binstall\b",
    r"\bload\b",
)


@dataclass(frozen=True)
class GuardrailsConfig:
    max_sql_chars: int = 4000
    max_result_rows: int = 200
    max_scanned_rows: int = 1_000_000
    max_result_bytes: int = 200_000


class SQLGuardrails:
    def __init__(self, config: GuardrailsConfig) -> None:
        self.config = config

    def _raise_blocked(
        self,
        *,
        payload: Dict[str, object],
        reason: str,
        message: str,
        code: str = SQL_ERROR_GUARDRAIL_BLOCKED,
    ) -> None:
        flags = payload.setdefault("guardrail_flags", [])
        if isinstance(flags, list) and reason not in flags:
            flags.append(reason)
        payload["reason"] = reason
        policy_decision = payload.setdefault("policy_decision", {})
        if isinstance(policy_decision, dict):
            policy_decision["allowed"] = False
            policy_decision["reason"] = reason
        raise TabularSQLException(
            code=code,
            message=message,
            details={
                "reason": reason,
                "sql_original": payload.get("sql_original"),
                "sql_final": payload.get("sql_final"),
            },
            executed_sql=str(payload.get("sql_final") or payload.get("sql_original") or ""),
            policy_decision=policy_decision if isinstance(policy_decision, dict) else {},
            guardrail_flags=flags if isinstance(flags, list) else [],
        )

    def enforce(self, sql: str, *, estimated_scan_rows: Optional[int] = None) -> Tuple[str, Dict[str, object]]:
        normalized = str(sql or "").strip()
        payload: Dict[str, object] = {
            "valid": False,
            "reason": None,
            "sql_original": sql,
            "sql_final": None,
            "guardrail_flags": [],
            "policy_decision": {
                "allowed": False,
                "reason": None,
                "operation": None,
                "limits": {
                    "max_sql_chars": int(self.config.max_sql_chars),
                    "max_result_rows": int(self.config.max_result_rows),
                    "max_scanned_rows": int(self.config.max_scanned_rows),
                    "max_result_bytes": int(self.config.max_result_bytes),
                },
            },
        }

        if not normalized:
            self._raise_blocked(payload=payload, reason="empty_sql", message="SQL guardrails: empty query")
        if len(normalized) > int(self.config.max_sql_chars):
            self._raise_blocked(
                payload=payload,
                reason="sql_too_long",
                message="SQL guardrails: query exceeds max length",
            )
        if "--" in normalized or "/*" in normalized or "*/" in normalized:
            self._raise_blocked(
                payload=payload,
                reason="comments_not_allowed",
                message="SQL guardrails: comments are not allowed",
            )

        trimmed = normalized.rstrip(";").strip()
        if ";" in trimmed:
            self._raise_blocked(
                payload=payload,
                reason="multiple_statements_not_allowed",
                message="SQL guardrails: multiple statements are not allowed",
            )

        lower = trimmed.lower()
        operation = "with" if lower.startswith("with ") else "select" if lower.startswith("select ") else None
        if operation is None:
            self._raise_blocked(
                payload=payload,
                reason="statement_not_allowed",
                message="SQL guardrails: only SELECT/WITH queries are allowed",
            )
        policy_decision = payload.get("policy_decision")
        if isinstance(policy_decision, dict):
            policy_decision["operation"] = operation

        for keyword in _BLOCKED_KEYWORDS:
            if re.search(rf"\b{re.escape(keyword)}\b", lower):
                self._raise_blocked(
                    payload=payload,
                    reason=f"blocked_keyword:{keyword}",
                    message=f"SQL guardrails: blocked keyword '{keyword}'",
                )

        for pattern in _BLOCKED_PATTERNS:
            if re.search(pattern, lower):
                self._raise_blocked(
                    payload=payload,
                    reason="blocked_pattern",
                    message="SQL guardrails: blocked SQL pattern",
                )

        if estimated_scan_rows is not None:
            scan_rows = max(0, int(estimated_scan_rows))
            payload["estimated_scan_rows"] = scan_rows
            if scan_rows > int(self.config.max_scanned_rows):
                self._raise_blocked(
                    payload=payload,
                    reason="scan_limit_exceeded",
                    message="SQL guardrails: estimated scanned rows exceed configured limit",
                    code=SQL_ERROR_SCAN_LIMIT_EXCEEDED,
                )

        final_sql = self._enforce_limit(trimmed, payload=payload)
        payload["valid"] = True
        payload["sql_final"] = final_sql
        payload["limit_enforced"] = final_sql != trimmed
        if isinstance(payload.get("guardrail_flags"), list):
            flags = payload["guardrail_flags"]
            if payload["limit_enforced"] and "result_limit_appended" not in flags:
                flags.append("result_limit_appended")
        policy_decision = payload.get("policy_decision")
        if isinstance(policy_decision, dict):
            policy_decision["allowed"] = True
            policy_decision["reason"] = "allowed"
        return final_sql, payload

    def _enforce_limit(self, sql: str, *, payload: Dict[str, object]) -> str:
        lower = sql.lower()
        existing_limit_match = re.search(r"\blimit\s+(\d+)\b", lower)
        if existing_limit_match:
            requested_limit = int(existing_limit_match.group(1))
            if requested_limit > int(self.config.max_result_rows):
                self._raise_blocked(
                    payload=payload,
                    reason="result_limit_exceeded",
                    message="SQL guardrails: requested LIMIT exceeds max_result_rows",
                    code=SQL_ERROR_RESULT_LIMIT_EXCEEDED,
                )
            return sql

        has_group_by = " group by " in f" {lower} "
        is_single_aggregate = bool(
            re.search(r"\b(count|sum|avg|min|max)\s*\(", lower) and not has_group_by
        )
        if is_single_aggregate:
            return sql

        return f"{sql} LIMIT {int(self.config.max_result_rows)}"
