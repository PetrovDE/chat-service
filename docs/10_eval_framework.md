# 10. Eval Framework

## Scope
Eval runtime is in `scripts/evals/*` with datasets in `tests/evals/datasets/*`.

## Datasets
- `tabular_aggregate_golden`
- `tabular_profile_golden`
- `narrative_rag_golden`
- `fallback_route_golden`
- `complex_analytics_quality_golden`
- `complex_analytics_quality_online`

## Runner
Main entry:
- `scripts/evals/run_eval_suite.py`

Core orchestration:
- `scripts/evals/runner.py`

Modes:
- `offline`
- `online`
- `hybrid`

## CI Gates
`run_ci_gates.py` evaluates thresholds from `tests/evals/gates.json`:
- numeric exact match
- citation faithfulness
- route correctness
- complex analytics report quality (broad prompts, structured final response)
- latency violations
- p95 latency by dataset

Exit code:
- `0` when all gates pass
- `1` when any gate fails

## Typical Commands
```bash
py -3 scripts/evals/run_eval_suite.py --mode offline --datasets-root tests/evals/datasets
py -3 scripts/evals/run_ci_gates.py --mode offline --datasets-root tests/evals/datasets --gate-config tests/evals/gates.json
py -3 scripts/evals/run_eval_suite.py --mode hybrid --datasets-root tests/evals/datasets --online-base-url http://localhost:8000
```

## Complex Analytics Quality Eval (2026-03-06)
- New offline dataset validates broad complex analytics prompts where compose stage returns weak text and backend must fallback to deterministic structured formatter.
- Required checks per case:
  - `status=ok`
  - minimum artifacts count
  - required report section substrings in final response
  - debug status contract (`response_status`, `response_error_code`)
- New metric in summary:
  - `complex_analytics_report_quality.score`

## Online/Hybrid Quality Gate (Preprod)
- Online dataset `complex_analytics_quality_online` validates real `/api/v1/chat` behavior for broad complex analytics prompts.
- New online metric:
  - `online_report.metrics.complex_analytics_report_quality.score`
- New preprod gate config:
  - `tests/evals/gates.preprod.json`
  - `online_complex_analytics_report_quality_min`
  - `online_max_latency_violations`
  - `online_p95_latency_ms.complex_analytics_quality_online`

Preprod command example:
```bash
set EVAL_COMPLEX_ANALYTICS_CONVERSATION_ID=<conversation-with-tabular-file>
py -3 scripts/evals/run_ci_gates.py --mode online --datasets-root tests/evals/datasets --online-base-url http://<preprod-host> --gate-config tests/evals/gates.preprod.json
```

Dataset supports `${ENV_VAR}` placeholders in `online_request` values (for secure preprod runtime injection).
