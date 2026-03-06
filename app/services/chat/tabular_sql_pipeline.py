from __future__ import annotations

import json
from time import perf_counter
from typing import Any, Callable, Dict, List, Tuple

from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable, TabularExecutionSession


def execute_aggregate_sync_pipeline(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
    build_guardrails_fn,
    build_execution_limits_fn,
    build_sql_fn: Callable[[str, ResolvedTabularTable], Tuple[str, Dict[str, Any]]],
    run_guarded_query_fn,
    quote_ident_fn: Callable[[str], str],
    first_int_fn: Callable[[List[Tuple[Any, ...]], int], int],
    observe_ms_fn,
    rows_to_result_text_fn,
) -> Dict[str, Any]:
    t0 = perf_counter()
    guardrails = build_guardrails_fn()
    execution_limits = build_execution_limits_fn()
    sql, plan = build_sql_fn(query, table)
    with TabularExecutionSession(dataset=dataset, table=table, limits=execution_limits) as session:
        rows, guarded_sql, guard_debug = run_guarded_query_fn(
            session=session,
            guardrails=guardrails,
            sql=sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        rows_total = int(table.row_count or 0)
        rows_effective = rows_total
        if plan.get("metric_column"):
            metric_q = quote_ident_fn(str(plan["metric_column"]))
            rows_effective_sql = (
                f"SELECT COUNT(*) AS value FROM {quote_ident_fn(table.table_name)} "
                f"WHERE TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
            )
            rows_effective_rows, _, _ = run_guarded_query_fn(
                session=session,
                guardrails=guardrails,
                sql=rows_effective_sql,
                estimated_scan_rows=int(table.row_count or 0),
                timeout_seconds=timeout_seconds,
            )
            rows_effective = first_int_fn(rows_effective_rows, rows_total)

        coverage_ratio = float(rows_effective / rows_total) if rows_total > 0 else 0.0

    observe_ms_fn("tabular_sql_execution_ms", (perf_counter() - t0) * 1000.0, intent="aggregate")
    result_text = rows_to_result_text_fn(rows)
    return {
        "status": "ok",
        "prompt_context": (
            "Deterministic tabular SQL result (source of truth):\n"
            f"table={table.table_name}\n"
            f"sql={guarded_sql}\n"
            f"result={result_text}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_aggregate",
            "deterministic_path": True,
            "tabular_sql": {
                "storage_engine": dataset.engine,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": table.table_name,
                "table_version": table.table_version,
                "table_provenance_id": table.provenance_id,
                "table_row_count": rows_total,
                "executed_sql": guarded_sql,
                "policy_decision": guard_debug.get("policy_decision"),
                "guardrail_flags": guard_debug.get("guardrail_flags", []),
                "sql": guarded_sql,
                "result": result_text,
                "sql_guardrails": guard_debug,
                **plan,
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql"
            )
        ],
        "rows_expected_total": rows_total,
        "rows_retrieved_total": rows_effective,
        "rows_used_map_total": rows_effective,
        "rows_used_reduce_total": rows_effective,
        "row_coverage_ratio": coverage_ratio,
    }


def build_profile_payload_pipeline(
    *,
    session: TabularExecutionSession,
    guardrails,
    table: ResolvedTabularTable,
    max_columns: int,
    timeout_seconds: float,
    run_guarded_query_fn,
    quote_ident_fn: Callable[[str], str],
    first_int_fn: Callable[[List[Tuple[Any, ...]], int], int],
    extract_row_tuples_fn,
) -> Tuple[str, Dict[str, Any], int]:
    table_q = quote_ident_fn(table.table_name)
    row_count_sql = f"SELECT COUNT(*) AS value FROM {table_q}"
    row_count_rows, row_count_sql_final, row_guard = run_guarded_query_fn(
        session=session,
        guardrails=guardrails,
        sql=row_count_sql,
        estimated_scan_rows=int(table.row_count or 0),
        timeout_seconds=timeout_seconds,
    )
    row_count = first_int_fn(row_count_rows, int(table.row_count or 0))

    sample_rows_sql = f"SELECT * FROM {table_q} LIMIT 5"
    sample_rows_raw, sample_rows_sql_final, sample_guard = run_guarded_query_fn(
        session=session,
        guardrails=guardrails,
        sql=sample_rows_sql,
        estimated_scan_rows=int(table.row_count or 0),
        timeout_seconds=timeout_seconds,
    )
    sample_rows = extract_row_tuples_fn(sample_rows_raw)

    column_stats: List[Dict[str, Any]] = []
    profiled_columns = min(len(table.columns), int(max_columns))
    executed_sql_list: List[str] = [row_count_sql_final, sample_rows_sql_final]
    policy_decisions: List[Dict[str, Any]] = []
    guardrail_flags: List[str] = []
    for debug_payload in (row_guard, sample_guard):
        policy_payload = debug_payload.get("policy_decision")
        if isinstance(policy_payload, dict):
            policy_decisions.append(dict(policy_payload))
        flags_payload = debug_payload.get("guardrail_flags")
        if isinstance(flags_payload, list):
            for flag in flags_payload:
                flag_str = str(flag)
                if flag_str and flag_str not in guardrail_flags:
                    guardrail_flags.append(flag_str)

    for col in table.columns[:profiled_columns]:
        cq = quote_ident_fn(col)
        non_empty_sql = f"SELECT COUNT(*) AS value FROM {table_q} WHERE TRIM(COALESCE(CAST({cq} AS VARCHAR), '')) <> ''"
        distinct_sql = (
            f"SELECT COUNT(DISTINCT {cq}) AS value "
            f"FROM {table_q} "
            f"WHERE TRIM(COALESCE(CAST({cq} AS VARCHAR), '')) <> ''"
        )
        top_values_sql = (
            f"SELECT {cq} AS value, COUNT(*) AS cnt "
            f"FROM {table_q} "
            f"WHERE TRIM(COALESCE(CAST({cq} AS VARCHAR), '')) <> '' "
            f"GROUP BY {cq} "
            f"ORDER BY cnt DESC LIMIT 3"
        )
        non_empty_rows, non_empty_sql_final, non_empty_guard = run_guarded_query_fn(
            session=session,
            guardrails=guardrails,
            sql=non_empty_sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        distinct_rows, distinct_sql_final, distinct_guard = run_guarded_query_fn(
            session=session,
            guardrails=guardrails,
            sql=distinct_sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        top_values_rows, top_values_sql_final, top_values_guard = run_guarded_query_fn(
            session=session,
            guardrails=guardrails,
            sql=top_values_sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        executed_sql_list.extend([non_empty_sql_final, distinct_sql_final, top_values_sql_final])
        for debug_payload in (non_empty_guard, distinct_guard, top_values_guard):
            policy_payload = debug_payload.get("policy_decision")
            if isinstance(policy_payload, dict):
                policy_decisions.append(dict(policy_payload))
            flags_payload = debug_payload.get("guardrail_flags")
            if isinstance(flags_payload, list):
                for flag in flags_payload:
                    flag_str = str(flag)
                    if flag_str and flag_str not in guardrail_flags:
                        guardrail_flags.append(flag_str)
        column_stats.append(
            {
                "column": col,
                "column_original_name": table.column_aliases.get(col),
                "non_empty_count": first_int_fn(non_empty_rows, 0),
                "distinct_non_empty_count": first_int_fn(distinct_rows, 0),
                "top_values": extract_row_tuples_fn(top_values_rows),
            }
        )

    profile_payload = {
        "table_name": table.table_name,
        "table_version": table.table_version,
        "table_provenance_id": table.provenance_id,
        "row_count": row_count,
        "columns_total": len(table.columns),
        "profiled_columns": profiled_columns,
        "column_stats": column_stats,
        "sample_rows": sample_rows,
    }
    prompt_context = "Deterministic tabular profile (source of truth):\n" + json.dumps(
        profile_payload,
        ensure_ascii=False,
        indent=2,
    )
    debug = {
        "profile_kind": "per_column",
        "row_count_sql": row_count_sql_final,
        "sample_rows_sql": sample_rows_sql_final,
        "executed_sql": executed_sql_list,
        "policy_decision": {
            "allowed": True,
            "reason": "allowed",
            "queries_executed": len(executed_sql_list),
            "checks": policy_decisions,
        },
        "guardrail_flags": guardrail_flags,
        "sql_guardrails_checks": [row_guard, sample_guard],
        "profiled_columns": profiled_columns,
    }
    return prompt_context, debug, row_count


def execute_profile_sync_pipeline(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
    max_columns: int,
    build_guardrails_fn,
    build_execution_limits_fn,
    build_profile_payload_fn,
    quote_ident_fn: Callable[[str], str],
) -> Dict[str, Any]:
    guardrails = build_guardrails_fn()
    execution_limits = build_execution_limits_fn()
    row_count_sql = f"SELECT COUNT(*) AS value FROM {quote_ident_fn(table.table_name)}"
    _, guard_debug = guardrails.enforce(row_count_sql, estimated_scan_rows=int(table.row_count or 0))

    with TabularExecutionSession(dataset=dataset, table=table, limits=execution_limits) as session:
        prompt_context, profile_debug, rows_total = build_profile_payload_fn(
            session=session,
            guardrails=guardrails,
            table=table,
            max_columns=max_columns,
            timeout_seconds=timeout_seconds,
        )

    return {
        "status": "ok",
        "prompt_context": prompt_context,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_profile",
            "deterministic_path": True,
            "tabular_sql": {
                "storage_engine": dataset.engine,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": table.table_name,
                "table_version": table.table_version,
                "table_provenance_id": table.provenance_id,
                "table_row_count": rows_total,
                "executed_sql": profile_debug.get("executed_sql", []),
                "policy_decision": profile_debug.get("policy_decision"),
                "guardrail_flags": profile_debug.get("guardrail_flags", []),
                "sql_guardrails": guard_debug,
                **profile_debug,
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql_profile"
            )
        ],
        "rows_expected_total": rows_total,
        "rows_retrieved_total": rows_total,
        "rows_used_map_total": rows_total,
        "rows_used_reduce_total": rows_total,
        "row_coverage_ratio": 1.0 if rows_total > 0 else 0.0,
    }


def build_tabular_error_result_pipeline(
    *,
    intent_kind: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    error_payload: Dict[str, Any],
    sql_error_execution_failed: str,
    sql_error_timeout: str,
) -> Dict[str, Any]:
    error_code = str(error_payload.get("code") or sql_error_execution_failed)
    error_message = str(error_payload.get("message") or "Deterministic SQL execution failed")
    executed_sql = str(error_payload.get("executed_sql") or "")
    policy_decision = error_payload.get("policy_decision")
    if not isinstance(policy_decision, dict):
        policy_decision = {"allowed": False, "reason": error_code}
    guardrail_flags = error_payload.get("guardrail_flags")
    if not isinstance(guardrail_flags, list):
        guardrail_flags = []

    clarification_prompt = (
        "Deterministic SQL execution was blocked by safety policy. "
        "Please narrow the metric/filter scope and retry."
    )
    if error_code == sql_error_timeout:
        clarification_prompt = (
            "Deterministic SQL execution timed out. "
            "Please narrow the filter or reduce the analysis scope and retry."
        )

    return {
        "status": "error",
        "clarification_prompt": clarification_prompt,
        "prompt_context": (
            "Deterministic tabular SQL execution failed.\n"
            f"error_code={error_code}\n"
            f"error_message={error_message}\n"
            f"executed_sql={executed_sql or 'n/a'}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": f"tabular_{intent_kind}",
            "deterministic_path": True,
            "deterministic_error": error_payload,
            "tabular_sql": {
                "storage_engine": dataset.engine,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": table.table_name,
                "table_version": table.table_version,
                "table_provenance_id": table.provenance_id,
                "table_row_count": int(table.row_count or 0),
                "executed_sql": executed_sql or None,
                "policy_decision": policy_decision,
                "guardrail_flags": guardrail_flags,
                "sql": executed_sql or None,
                "result": None,
                "sql_guardrails": {
                    "valid": False,
                    "reason": policy_decision.get("reason"),
                    "policy_decision": policy_decision,
                    "guardrail_flags": guardrail_flags,
                },
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql_error={error_code}"
            )
        ],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": 0,
        "rows_used_map_total": 0,
        "rows_used_reduce_total": 0,
        "row_coverage_ratio": 0.0,
    }
