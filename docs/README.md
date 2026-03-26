# Documentation Index

This file is the source of truth for documentation ownership, categories, and reading order.

## Canonical Source-of-Truth Docs

Read in this order unless a task is narrowly scoped:

1. `docs/architecture_guardrails.md`
2. `docs/module_boundaries.md`
3. `docs/dev_quality_gates.md`
4. `docs/00_project_map.md`
5. `docs/01_service_spec.md`
6. `docs/02_api_contracts.md`
7. `docs/01_architecture_overview.md`
8. `docs/03_rag_pipeline.md`
9. `docs/04_ingestion_pipeline.md`
10. `docs/05_query_planner.md`
11. `docs/06_tabular_runtime.md`
12. `docs/07_llm_routing.md`
13. `docs/08_sql_guardrails.md`
14. `docs/09_observability.md`
15. `docs/10_eval_framework.md`
16. `docs/13_persistent_user_files_architecture.md`
17. `docs/19_big_file_refactor_complex_analytics.md`
18. `docs/20_alembic_clean_slate.md`
19. `docs/12_architecture_decisions.md` and all `docs/adr/*`

## Reading Order for Codex / AI Agents / New Contributors

1. Read `docs/architecture_guardrails.md` and `docs/module_boundaries.md` first.
2. Read the API and lifecycle contracts (`docs/01_service_spec.md`, `docs/02_api_contracts.md`).
3. Read the runtime path docs relevant to the task (`docs/03` to `docs/10`).
4. Read related ADRs before changing architecture behavior.
5. Read runbooks only for operational tasks.
6. Treat debug investigations and archived plans as context only, not contracts.

## Documentation Categories

### Canonical Contracts
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
- `docs/dev_quality_gates.md`
- `docs/12_architecture_decisions.md`
- `docs/13_persistent_user_files_architecture.md`
- `docs/19_big_file_refactor_complex_analytics.md`
- `docs/20_alembic_clean_slate.md`
- `docs/architecture_guardrails.md`
- `docs/module_boundaries.md`

### ADRs
- `docs/adr/ADR-005` to `docs/adr/ADR-014`

### Runbooks
- `docs/runbooks/aihub_outage.md`
- `docs/runbooks/fallback_surge.md`
- `docs/runbooks/queue_backlog.md`
- `docs/runbooks/rag_degradation.md`

### Debug Investigations (Non-Canonical)
- `docs/rag_debug/*`
- `docs/rag_debugging.md`

### Examples and Reference Material
- `docs/examples/*`
- `docs/ux/*`

### Implementation Reports
- `docs/architecture_enforcement_report.md`

### Historical / Archived Material
- `docs/archive/legacy_architecture/*`
- `docs/archive/transition_plans/*`
- `docs/archive/dev_prompts/*`
- `docs/archive/reports/*`
- `docs/archive/runbooks_legacy/*`

## Maintenance Rules

- When architecture boundaries change, update canonical docs in the same change.
- Do not use archived docs as implementation contracts.
- Keep engineering docs, rules, and comments in English only.
- Do not introduce Cyrillic into code, config, rules, or engineering documentation.
