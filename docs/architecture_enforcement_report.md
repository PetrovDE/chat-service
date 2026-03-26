# Architecture Enforcement Report

Date: 2026-03-26

## 1) Enforcement Added

Added a lightweight architecture enforcement layer with:

- Static boundary/anti-pattern checks in `scripts/architecture/enforcement_checks.py`.
- Architecture pytest gate in `tests/architecture/test_architecture_enforcement.py`.
- Developer/CI entrypoint in `scripts/run_architecture_checks.py`.
- Canonical gate documentation in `docs/dev_quality_gates.md`.

## 2) Active Checks Now

Active strict checks:

1. Runtime import boundary checks for:
   - `app/services/tabular`
   - `app/services/llm/routing`
   - `app/rag` (chat-localization/composer dependency restrictions)
2. Domain import boundary checks (`app/domain` disallowing infra/transport dependencies).
3. API-to-RAG direct import allowlist checks at endpoint layer.
4. Shared debug builder ownership check (`debug_sections` assembly location).
5. Low-level localization marker checks.
6. Forbidden matching-hint token checks in tabular matching/runtime files.
7. Module line-budget checks on known high-risk files.
8. Function line-budget checks on known high-risk functions.

Active warning checks:

1. Global oversized module watchlist (`>500` LOC).
2. Domain-to-service coupling watchlist.
3. Cyrillic watchlist in `app/rag`.
4. Stale budget-entry watchlist for moved/extracted files/functions.

Contract subset (optional via `--with-contract-tests`, executed after strict checks):

- `tests/unit/test_tabular_sql_no_silent_fallback.py`
- `tests/unit/test_tabular_schema_resolver.py`
- `tests/integration/test_rag_debug_contract.py`

## 3) Warning-Only vs Strict

Strict (fail-fast):

- Import boundary violations.
- Debug builder ownership violations.
- Low-level localization marker violations.
- Forbidden matching-hint token violations.
- Module/function budget overruns.

Warning-only (report mode):

- Existing oversized module inventory.
- Existing domain-service coupling.
- Existing Cyrillic presence in `app/rag`.
- Budget configuration drift (moved/extracted monitored files/functions).

## 4) Boundaries Enforced

1. Low-level runtime and routing layers cannot import chat rendering/localization concerns.
2. Domain layer cannot depend on transport/infrastructure frameworks.
3. Endpoint layer cannot add new direct RAG coupling without explicit allowlist update.
4. Debug contract section assembly is centralized in shared builder code path.
5. Hotspot growth is budget-constrained to avoid silent monolith expansion.

## 5) Anti-Patterns Covered

1. Direct boundary-crossing imports across runtime/domain/API slices.
2. Ad-hoc `debug_sections` assembly outside shared debug builder.
3. Reintroduction of hardcoded dataset-domain hint tokens in tabular matching logic.
4. Silent architecture drift through unchecked growth of known bloated modules/functions.
5. Silent fallback/matching/debug contract regressions via contract-test subset execution.

## 6) Remaining Risky Modules

Current hotspots still above the global 500-line watch threshold are warning-tracked:

- `app/services/chat/tabular_sql.py`
- `app/services/chat/rag_prompt_builder.py`
- `app/services/file.py`
- `app/services/chat/orchestrator_runtime.py`
- `app/rag/retriever.py`

These are constrained by strict growth budgets but still need incremental decomposition.

## 7) Next Realistic Tightening Step

Promote one warning category at a time to strict:

1. Start with domain-to-service coupling for changed domain files only.
2. Add AST-level detection for silent fallback-first-column patterns.
3. Add import cycle checks between `chat`, `tabular`, `rag`, `llm` slices.
4. Ratchet line/function budgets downward after extraction PRs.
