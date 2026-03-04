from app.observability.metrics import reset_metrics, snapshot_metrics
from app.observability.slo_metrics import (
    observe_ingestion_enqueue,
    observe_ingestion_retry,
    observe_llm_route_decision,
    observe_planner_decision,
    observe_retrieval_coverage,
    observe_tabular_row_coverage,
    set_ingestion_queue_snapshot,
)


def _has_metric(metrics: dict, name: str, *contains: str) -> bool:
    for key in metrics:
        if name not in key:
            continue
        if all(token in key for token in contains):
            return True
    return False


def test_route_and_planner_slo_metrics_are_recorded():
    reset_metrics()
    observe_llm_route_decision(
        route="ollama_fallback",
        fallback_reason="timeout",
        fallback_allowed=True,
        fallback_policy_version="test-v1",
    )
    observe_planner_decision(
        route="deterministic_analytics",
        intent="tabular_aggregate",
        requires_clarification=False,
        metric_critical=True,
    )
    counters = snapshot_metrics()["counters"]

    assert _has_metric(
        counters,
        "llama_service_llm_route_decisions_total",
        "route=ollama_fallback",
        "fallback_reason=timeout",
    )
    assert _has_metric(counters, "llama_service_llm_fallback_total", "fallback_reason=timeout")
    assert _has_metric(
        counters,
        "llama_service_query_planner_route_total",
        "route_class=deterministic",
        "metric_critical=true",
    )


def test_ingestion_and_coverage_slo_metrics_are_recorded():
    reset_metrics()
    observe_ingestion_enqueue(mode="local", deduplicated=False)
    observe_ingestion_retry()
    set_ingestion_queue_snapshot(
        depth=3,
        processing=1,
        dead_letter_depth=0,
        lag_seconds=4.5,
        heartbeat_age_seconds=1.0,
    )
    observe_retrieval_coverage(
        coverage_ratio=0.81,
        retrieval_mode="full_file",
        expected_chunks=100,
        retrieved_chunks=81,
    )
    observe_tabular_row_coverage(
        coverage_ratio=0.97,
        retrieval_mode="tabular_sql",
        rows_expected_total=200,
        rows_retrieved_total=194,
    )

    snap = snapshot_metrics()
    counters = snap["counters"]
    gauges = snap["gauges"]

    assert _has_metric(counters, "llama_service_ingestion_enqueue_total", "result=enqueued")
    assert _has_metric(counters, "llama_service_ingestion_retries_total")
    assert _has_metric(
        counters,
        "llama_service_retrieval_coverage_events_total",
        "retrieval_mode=full_file",
    )
    assert _has_metric(
        counters,
        "llama_service_tabular_row_coverage_events_total",
        "retrieval_mode=tabular_sql",
    )

    assert _has_metric(gauges, "llama_service_ingestion_queue_depth")
    assert _has_metric(gauges, "llama_service_ingestion_queue_lag_seconds")
    assert _has_metric(gauges, "llama_service_retrieval_coverage_ratio", "retrieval_mode=full_file")
    assert _has_metric(gauges, "llama_service_tabular_row_coverage_ratio", "retrieval_mode=tabular_sql")
