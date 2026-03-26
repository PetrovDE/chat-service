# Architecture Documentation Reconciliation

Date: 2026-03-24

## 1) What Was Found

- Documentation was split between active contracts and transitional/historical plans in the same top-level folder.
- Multiple files contained mixed-language or non-English content in engineering artifacts.
- Canonical boundaries for architecture contracts vs investigations vs prompts were not explicit.
- Cursor rules existed as a single mixed-language file with overlapping concerns.
- Historical runbooks and architecture plans were not isolated from current source-of-truth docs.

## 2) Canonical Documentation Set (Source of Truth)

### Core Contracts
- `docs/README.md`
- `docs/architecture_guardrails.md`
- `docs/module_boundaries.md`
- `docs/00_project_map.md`
- `docs/00_system_overview.md`
- `docs/01_architecture_overview.md`
- `docs/01_service_spec.md`
- `docs/02_api_contracts.md`
- `docs/02_service_structure.md`
- `docs/03_rag_pipeline.md`
- `docs/04_frontend_architecture.md`
- `docs/04_ingestion_pipeline.md`
- `docs/05_query_planner.md`
- `docs/06_tabular_runtime.md`
- `docs/07_llm_routing.md`
- `docs/08_sql_guardrails.md`
- `docs/09_observability.md`
- `docs/10_eval_framework.md`
- `docs/12_architecture_decisions.md`
- `docs/13_persistent_user_files_architecture.md`
- `docs/19_big_file_refactor_complex_analytics.md`
- `docs/20_alembic_clean_slate.md`

### ADRs (Canonical)
- `docs/adr/ADR-005-model-router-aihub-first.md`
- `docs/adr/ADR-006-durable-ingestion-sqlite-queue.md`
- `docs/adr/ADR-007-tabular-runtime-duckdb-parquet.md`
- `docs/adr/ADR-008-query-planner-deterministic-vs-narrative.md`
- `docs/adr/ADR-009-sql-guardrails-bounded-execution.md`
- `docs/adr/ADR-010-preprod-hardening-verification-strategy.md`
- `docs/adr/ADR-011-architecture-doc-reconciliation.md`
- `docs/adr/ADR-012-rag-debug-observability-contract.md`
- `docs/adr/ADR-013-provider-selection-precedence-explicit-mode.md`
- `docs/adr/ADR-014-complex-analytics-sandbox-executor.md`

### Canonical Runbooks
- `docs/runbooks/aihub_outage.md`
- `docs/runbooks/fallback_surge.md`
- `docs/runbooks/queue_backlog.md`
- `docs/runbooks/rag_degradation.md`

## 3) Classification and Decisions

| Path | Category | Relevance | Decision | Notes |
|---|---|---|---|---|
| `docs/00_project_map.md` | Canonical architecture contract | High | Keep as canonical | Entry map to code locations |
| `docs/00_system_overview.md` | Canonical architecture contract | High | Keep as canonical | System-level scope and subsystem map |
| `docs/01_architecture_overview.md` | Canonical architecture contract | High | Keep as canonical | Architectural style and dependency graph |
| `docs/01_service_spec.md` | Canonical contract | High | Keep as canonical | Lifecycle and status contract |
| `docs/02_api_contracts.md` | Canonical contract | High | Keep as canonical | API envelope and endpoint contracts |
| `docs/02_service_structure.md` | Canonical architecture contract | High | Keep as canonical | Directory responsibility map |
| `docs/03_rag_pipeline.md` | Canonical architecture contract | High | Keep as canonical | Retrieval and route flow |
| `docs/04_frontend_architecture.md` | Canonical architecture contract | Medium | Keep as canonical | Frontend integration boundaries |
| `docs/04_ingestion_pipeline.md` | Canonical architecture contract | High | Keep as canonical | Durable ingestion flow |
| `docs/05_query_planner.md` | Canonical architecture contract | High | Keep as canonical | Planner decision contract |
| `docs/06_tabular_runtime.md` | Canonical architecture contract | High | Keep as canonical | Deterministic tabular runtime contract |
| `docs/07_llm_routing.md` | Canonical architecture contract | High | Keep as canonical | Routing modes and precedence |
| `docs/08_sql_guardrails.md` | Canonical architecture contract | High | Keep as canonical | SQL safety bounds |
| `docs/09_observability.md` | Canonical architecture contract | High | Keep as canonical | Current observability contract |
| `docs/10_eval_framework.md` | Canonical quality contract | High | Keep as canonical | Current eval and quality gates |
| `docs/12_architecture_decisions.md` | ADR index | High | Keep as canonical | Decision register index |
| `docs/13_persistent_user_files_architecture.md` | Canonical architecture contract | High | Keep as canonical | File lifecycle architecture |
| `docs/19_big_file_refactor_complex_analytics.md` | Canonical architecture guardrail context | High | Keep as canonical | Oversized-file decomposition baseline |
| `docs/20_alembic_clean_slate.md` | Canonical runbook/contract | Medium | Keep as canonical | Migration source-of-truth workflow |
| `docs/rag_debugging.md` | Debug reference | Medium | Keep as non-canonical reference | Operational debug quick guide |
| `docs/archive/legacy_architecture/03_backend_architecture.md` | Historical architecture | Medium | Keep but move to archive | Superseded by current architecture contracts |
| `docs/archive/legacy_architecture/05_observability.md` | Historical architecture | Low | Keep but move to archive | Duplicates/superseded by `docs/09_observability.md` |
| `docs/archive/legacy_architecture/06_testing_and_dod.md` | Historical quality plan | Low | Keep but move to archive | Superseded by `docs/10_eval_framework.md` |
| `docs/archive/legacy_architecture/07_rag_full_file_excel.md` | Historical RAG plan | Low | Keep but move to archive | Superseded by `docs/03_rag_pipeline.md` |
| `docs/archive/legacy_architecture/11_llm_file_chat_best_practices_architecture.md` | Historical baseline | Medium | Keep but move to archive | Legacy baseline preserved for history |
| `docs/archive/legacy_architecture/13_offline_refactor_gap_analysis.md` | Historical analysis | Low | Keep but move to archive | Transitional gap analysis |
| `docs/archive/legacy_architecture/14_observability_slo_offline.md` | Historical observability plan | Low | Keep but move to archive | Superseded by canonical observability docs |
| `docs/archive/legacy_architecture/15_eval_framework_offline.md` | Historical eval plan | Low | Keep but move to archive | Superseded by canonical eval docs |
| `docs/archive/transition_plans/08_chat_orchestrator_refactor_plan.md` | Transitional plan | Medium | Keep but move to archive | Historical refactor planning notes |
| `docs/archive/transition_plans/09_dynamic_rag_budget_plan.md` | Transitional plan | Medium | Keep but move to archive | Not a current runtime contract |
| `docs/archive/transition_plans/10_langchain_xlsx_rag_implementation_plan.md` | Transitional plan | Medium | Keep but move to archive | Proposal-level implementation plan |
| `docs/archive/transition_plans/17_transition_plan_chatgpt_claude_style_execution.md` | Transitional plan | Low | Keep but move to archive | Process transition notes |
| `docs/archive/transition_plans/100_planing_to_rq.md` | Transitional/legacy operations | Low | Keep but move to archive | Legacy queue migration plan |
| `docs/archive/transition_plans/101_comands_to_up_rq.md` | Transitional/legacy operations | Low | Keep but move to archive | Legacy deployment commands |
| `docs/archive/dev_prompts/12_codex_cursor_prompts_offline_architecture.md` | Dev prompt note | Low | Keep but move to archive | Not a runtime contract |
| `docs/archive/dev_prompts/18_codex_super_prompt_transition_execution_plan.md` | Dev prompt note | Low | Keep but move to archive | Not a runtime contract |
| `docs/archive/reports/16_preprod_readiness_report.md` | Historical report | Medium | Keep but move to archive | Snapshot report, not contract |
| `docs/archive/runbooks_legacy/aihub_incident.md` | Historical runbook | Medium | Keep but move to archive | Legacy/mixed-language duplicate |
| `docs/archive/runbooks_legacy/degraded_mode.md` | Historical runbook | Medium | Keep but move to archive | Legacy/mixed-language duplicate |
| `docs/adr/ADR-005-model-router-aihub-first.md` | ADR | High | Keep as canonical and normalize | Rewritten to English |
| `docs/adr/ADR-006-durable-ingestion-sqlite-queue.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-007-tabular-runtime-duckdb-parquet.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-008-query-planner-deterministic-vs-narrative.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-009-sql-guardrails-bounded-execution.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-010-preprod-hardening-verification-strategy.md` | ADR | High | Keep as canonical and normalize | Rewritten to English |
| `docs/adr/ADR-011-architecture-doc-reconciliation.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-012-rag-debug-observability-contract.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-013-provider-selection-precedence-explicit-mode.md` | ADR | High | Keep as canonical | No move |
| `docs/adr/ADR-014-complex-analytics-sandbox-executor.md` | ADR | High | Keep as canonical | No move |
| `docs/runbooks/aihub_outage.md` | Runbook | High | Keep as canonical | Active outage workflow |
| `docs/runbooks/fallback_surge.md` | Runbook | High | Keep as canonical | Active fallback workflow |
| `docs/runbooks/queue_backlog.md` | Runbook | High | Keep as canonical | Active ingestion backlog workflow |
| `docs/runbooks/rag_degradation.md` | Runbook | High | Keep as canonical | Active retrieval degradation workflow |
| `docs/rag_debug/01_system_diagnosis.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/02_file_lifecycle_logging.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/03_cross_chat_file_resolution_and_language.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/04_table_intent_routing.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/05_fallback_and_cache_cleanup.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/06_tests_and_debug_mode.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/07_general_chat_route_hotfix.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/08_chart_rendering_and_artifact_delivery.md` | Debug investigation | Medium | Keep as non-canonical debug | Investigation artifact |
| `docs/rag_debug/10_runtime_architecture_cleanup.md` | Debug investigation | High | Keep as non-canonical debug | Useful anti-pattern audit |
| `docs/ux/chat_states.md` | UX reference | Medium | Keep as reference | Not architecture source-of-truth |
| `docs/ux/file_ingestion_progress.md` | UX reference | Medium | Keep as reference | Not architecture source-of-truth |
| `docs/examples/*` | API examples | Medium | Keep as reference | Example payloads/responses |

## 4) New Guardrails Added

- `docs/architecture_guardrails.md` defines strict architecture rules, forbidden patterns, oversized-file policy, mandatory tests, and architecture-level DoD.
- `docs/module_boundaries.md` defines layer responsibilities, allowed/forbidden dependencies, and anti-pattern examples.
- `AGENTS.md` added as plain fallback instruction layer for AI agents and contributors.
- Cursor rules normalized to:
  - `.cursor/rules/00-architecture-guardrails.mdc`
  - `.cursor/rules/10-doc-reading-order.mdc`
  - `.cursor/rules/20-runtime-boundaries.mdc`

## 5) Required Reading Order Now

1. `docs/README.md`
2. `docs/architecture_guardrails.md`
3. `docs/module_boundaries.md`
4. `docs/01_service_spec.md`
5. `docs/02_api_contracts.md`
6. Runtime-specific canonical contracts for the changed area
7. Relevant ADRs
8. Runbooks/debug docs only if needed

## 6) Remaining Risks

- Some non-canonical debug/history artifacts may still include mixed-language content; they are archived or explicitly non-canonical, but full historical translation is still pending.
- Architecture boundaries are now enforced by lightweight static gates (`scripts/run_architecture_checks.py` + `tests/architecture/test_architecture_enforcement.py`), but coverage is intentionally incremental and includes warning-only debt categories.
- Large legacy runtime modules still exist and require incremental extraction under the new guardrails.
- Canonical scope may still need one follow-up decision for frontend-only docs (`docs/04_frontend_architecture.md`) if the UI architecture diverges rapidly from backend release cadence.

## 7) Next Practical Coding Step

Tighten the current gates incrementally:
- Promote warning-only domain/service coupling checks to strict for changed domain files.
- Add AST-level silent-fallback anti-pattern checks (first-column/table guesses).
- Add import cycle checks for chat/tabular/rag/llm slices.
- Ratchet line/function budgets downward after extraction refactors.

## 8) Patch List

### Added
- `docs/README.md`
- `docs/architecture_guardrails.md`
- `docs/module_boundaries.md`
- `docs/architecture_docs_reconciliation.md`
- `AGENTS.md`
- `.cursor/rules/00-architecture-guardrails.mdc`
- `.cursor/rules/10-doc-reading-order.mdc`
- `.cursor/rules/20-runtime-boundaries.mdc`

### Updated
- `.gitignore`
- `README.md`
- `docs/adr/ADR-005-model-router-aihub-first.md`
- `docs/adr/ADR-010-preprod-hardening-verification-strategy.md`

### Moved to Archive
- `docs/03_backend_architecture.md` -> `docs/archive/legacy_architecture/03_backend_architecture.md`
- `docs/05_observability.md` -> `docs/archive/legacy_architecture/05_observability.md`
- `docs/06_testing_and_dod.md` -> `docs/archive/legacy_architecture/06_testing_and_dod.md`
- `docs/07_rag_full_file_excel.md` -> `docs/archive/legacy_architecture/07_rag_full_file_excel.md`
- `docs/08_chat_orchestrator_refactor_plan.md` -> `docs/archive/transition_plans/08_chat_orchestrator_refactor_plan.md`
- `docs/09_dynamic_rag_budget_plan.md` -> `docs/archive/transition_plans/09_dynamic_rag_budget_plan.md`
- `docs/10_langchain_xlsx_rag_implementation_plan.md` -> `docs/archive/transition_plans/10_langchain_xlsx_rag_implementation_plan.md`
- `docs/11_llm_file_chat_best_practices_architecture.md` -> `docs/archive/legacy_architecture/11_llm_file_chat_best_practices_architecture.md`
- `docs/12_codex_cursor_prompts_offline_architecture.md` -> `docs/archive/dev_prompts/12_codex_cursor_prompts_offline_architecture.md`
- `docs/13_offline_refactor_gap_analysis.md` -> `docs/archive/legacy_architecture/13_offline_refactor_gap_analysis.md`
- `docs/14_observability_slo_offline.md` -> `docs/archive/legacy_architecture/14_observability_slo_offline.md`
- `docs/15_eval_framework_offline.md` -> `docs/archive/legacy_architecture/15_eval_framework_offline.md`
- `docs/16_preprod_readiness_report.md` -> `docs/archive/reports/16_preprod_readiness_report.md`
- `docs/17_transition_plan_chatgpt_claude_style_execution.md` -> `docs/archive/transition_plans/17_transition_plan_chatgpt_claude_style_execution.md`
- `docs/18_codex_super_prompt_transition_execution_plan.md` -> `docs/archive/dev_prompts/18_codex_super_prompt_transition_execution_plan.md`
- `docs/100_planing_to_rq.md` -> `docs/archive/transition_plans/100_planing_to_rq.md`
- `docs/101_comands_to_up_rq.md` -> `docs/archive/transition_plans/101_comands_to_up_rq.md`
- `docs/runbooks/aihub_incident.md` -> `docs/archive/runbooks_legacy/aihub_incident.md`
- `docs/runbooks/degraded_mode.md` -> `docs/archive/runbooks_legacy/degraded_mode.md`

## 9) English-Only Normalization Confirmation

- All newly created governance artifacts in this change are English-only.
- Updated canonical ADR files (`ADR-005`, `ADR-010`) were normalized to English-only content.
- The prior mixed-language Cursor rule was replaced with English-only rules.

## 10) Cyrillic Scan Confirmation

Modified governance artifacts were scanned for Cyrillic and cleaned.
Historical archived material may still contain non-English text by design, but it is now explicitly non-canonical.
