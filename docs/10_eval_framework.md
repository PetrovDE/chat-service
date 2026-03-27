# 10. Eval Framework

## Scope

Eval runtime is in `scripts/evals/*` with datasets in `tests/evals/datasets/*`.

The Stage 6 objective is explicit: provide objective, repeatable evidence that the
new analytics/RAG path is at least non-regressive on correctness, better on
explainability, and operationally safe to promote.

## Dataset Inventory

### Offline Datasets (repo-native, deterministic)

Offline Stage 6 datasets are intentionally execution-light contract checks.
They verify routing/debug/fallback/shape expectations from canonical fixtures
without requiring external providers or heavy runtime dependencies.

- `tabular_aggregate_golden`
- `tabular_profile_golden`
- `narrative_rag_golden`
- `fallback_route_golden`
- `complex_analytics_quality_golden`
- `tabular_langgraph_eval_slice_golden` (Stage 6)

### Online Datasets (controlled runtime validation)

- `complex_analytics_quality_online`
- `rag_retrieval_quality_online` (Stage 6)
- `rag_failure_explainability_online` (Stage 6)
- `tabular_followup_continuity_online` (Stage 6)

## Stage 6 Representative Coverage Matrix

The suite covers the required scenario families as follows:

1. Schema discovery questions
- Offline: `tabular_langgraph_eval_slice_golden` (`langgraph_schema_discovery`)

2. Simple counts and aggregations
- Offline: `tabular_aggregate_golden`, `tabular_langgraph_eval_slice_golden` (`langgraph_count_rows`)

3. Temporal grouping
- Offline: `tabular_langgraph_eval_slice_golden` (`langgraph_temporal_grouping`)

4. Chart requests
- Offline: `tabular_langgraph_eval_slice_golden` (`langgraph_chart_request`)
- Online: `tabular_followup_continuity_online` (`tabular_chart_request`)

5. Follow-up analytical refinements
- Offline: `tabular_langgraph_eval_slice_golden` (`langgraph_followup_refinement`)
- Online: `tabular_followup_continuity_online` (`tabular_followup_monthly_refinement`)

6. Multi-document retrieval
- Online: `rag_retrieval_quality_online` (`rag_multidoc_retrieval`)

7. Retrieval-backed follow-up questions
- Online: `rag_retrieval_quality_online` (`rag_followup_refinement`)

8. Failure and clarification cases
- Online: `rag_failure_explainability_online` (`tabular_clarification_explainability`)

9. Indexing-not-ready / no-context explainability
- Online: `rag_failure_explainability_online` (`rag_indexing_not_ready_explainability`)

## Golden Fixtures

Stage 6 golden fixture proposals are maintained in:

- `tests/evals/datasets/golden_fixture_catalog.json`

Required categories included in the catalog:

- text-heavy docs
- tabular files
- mixed-format uploads
- noisy or poorly structured files
- large files
- multiple documents in one conversation
- short but valid files
- weak-pdf and near-empty pdf edge cases

## Runner

Main entry:

- `scripts/evals/run_eval_suite.py`

Core orchestration:

- `scripts/evals/runner.py`

Modes:

- `offline`
- `online`
- `hybrid`

## Evaluation Dimensions

Stage 6 minimum dimensions and metric mapping:

1. Correctness
- `numeric_exact_match`
- `route_correctness`
- `langgraph_eval_correctness`

2. Grounding in uploaded content
- `citation_faithfulness` (offline proxy)
- `online_metric::grounding_uploaded_content`

3. Retrieval relevance
- `online_metric::retrieval_relevance`

4. Chart/execution correctness
- `complex_analytics_report_quality`
- `online_metric::chart_execution_correctness`

5. Follow-up continuity
- `online_metric::followup_continuity`

6. Latency
- `latency_regression_violations`
- `p95_latency::*`
- `online_latency_regression_violations`
- `online_p95_latency::*`

7. Fallback rate control
- `online_metric::fallback_rate_control`

8. Explainability/debug usefulness
- `langgraph_explainability_gain`
- `online_metric::explainability_debug_usefulness`

## Online Dataset Controls

Online cases can be conditionally enabled via `enabled_if_env`.
If that environment variable is missing, the case is skipped and reported in:

- `online_report.skipped_cases`
- `online_report.skipped_cases_count`

This keeps the suite reproducible while allowing preprod-only slices that need
specific prepared conversations/files.

## CI Gates

`run_ci_gates.py` evaluates thresholds from gate config (`tests/evals/gates.json`
or `tests/evals/gates.preprod.json`):

- baseline offline metrics (`numeric_exact_match`, `citation_faithfulness`, `route_correctness`, `complex_analytics_report_quality`)
- optional offline metric map: `offline_metric_min_scores`
- optional online metric map: `online_metric_min_scores`
- latency violations and p95 budgets (offline and online)

Exit code:

- `0` when all gates pass
- `1` when any gate fails

## Typical Commands

```bash
py -3 scripts/evals/run_eval_suite.py --mode offline --datasets-root tests/evals/datasets
py -3 scripts/evals/run_ci_gates.py --mode offline --datasets-root tests/evals/datasets --gate-config tests/evals/gates.json
py -3 scripts/evals/run_eval_suite.py --mode hybrid --datasets-root tests/evals/datasets --online-base-url http://localhost:8000
```

## Stage 6 Promotion Gates (Explicit Pass/Fail)

### 1) Legacy -> LangGraph Promotion Readiness

Pass when all are true:

- `offline_metric::langgraph_eval_correctness >= 1.0`
- `offline_metric::langgraph_vs_legacy_correctness_delta >= 0.0`
- `offline_metric::langgraph_explainability_gain >= 1.0`

Fail when any condition is false.

Note: offline LangGraph checks are contract-level parity/explainability checks.
Real runtime superiority must still be confirmed by online/preprod gates below.

### 2) Retrieval Quality Readiness

Pass when configured online retrieval metrics meet `online_metric_min_scores`
in `tests/evals/gates.preprod.json`.

Fail when any retrieval or grounding score is below threshold.

### 3) Observability Readiness

Pass when explainability slices validate required debug/no-context diagnostics:

- `online_metric::explainability_debug_usefulness` meets threshold

Fail when no-context or clarification responses are not diagnosable via debug
contract fields.

### 4) Debug Contract Readiness

Pass when online payload checks for Stage 4/5 debug contract fields succeed
(`debug_contract_version`, retrieval counters, indexing-state diagnostics,
fallback diagnostics).

Fail on missing or mismatched required fields.

### 5) Rollback Readiness

Pass when fallback visibility remains explicit (`fallback_reason`, mode served,
engine fallback diagnostics) and no silent route downgrade is observed in evals.

Fail if fallback path is opaque or unavailable.

## Definition of Done (Stage 6)

Stage 6 is done only if all are true:

1. Representative suite includes all required scenario families listed above.
2. Golden fixture catalog covers required file categories.
3. Offline hooks are runnable without uncontrolled external dependencies.
4. Online slices are controlled by explicit environment gating and report skipped cases.
5. Promotion gate thresholds are codified in `tests/evals/gates.preprod.json`.
6. Rollback criteria are documented and testable.
7. Eval tests validate dataset contract, runner behavior, online check operators, and CI gate logic.

## Rollback Triggers (Operational)

Rollback from LangGraph-preferred mode to legacy-preferred mode is triggered when
any of the following happens during staged rollout:

1. `langgraph_vs_legacy_correctness_delta < 0.0` in controlled eval runs.
2. `online_metric::retrieval_relevance` or `online_metric::grounding_uploaded_content`
   drops below gate threshold for two consecutive runs.
3. Explainability metric fails (`online_metric::explainability_debug_usefulness`).
4. Latency violations exceed configured budgets.
5. Unexpected surge in fallback behavior for non-fallback control cases.

## Runtime Validation Still Required

The following require real environment execution and cannot be fully proven by
offline checks alone:

1. Multi-document retrieval quality against production-like corpora.
2. End-to-end indexing-not-ready diagnostics with actual ingestion lag.
3. Real artifact delivery paths and storage permissions for chart URLs.
4. Production traffic latency/variance and fallback-rate behavior.
