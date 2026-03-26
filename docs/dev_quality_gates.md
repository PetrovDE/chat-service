# Dev Quality Gates

Date: 2026-03-26

## Purpose

This document defines the active architecture enforcement gates for `chat-service / LLM file chat`.
It turns architecture guardrails into runnable checks.

## Entrypoint

Run strict architecture gates (default fail-fast path):

```bash
py -3 scripts/run_architecture_checks.py
```

Run strict checks and also execute contract subset:

```bash
py -3 scripts/run_architecture_checks.py --with-contract-tests
```

`--with-contract-tests` expects project dependencies in the active interpreter
(at minimum `pytest` and `pydantic`, usually via `pip install -r requirements.txt`).
If these optional dependencies are missing, the command degrades gracefully:
- strict architecture checks still run and enforce fail-fast behavior,
- contract subset is skipped with an explicit message,
- command exits successfully (`0`) for the strict-only outcome.

Run architecture pytest gate directly:

```bash
py -3 -m pytest -q tests/architecture/test_architecture_enforcement.py
```

## Enforcement Matrix

### Strict (Fail-Fast)

The following checks fail the command with non-zero exit:

1. Runtime import boundaries (`scripts/architecture/enforcement_checks.py`)
   - `app/services/tabular/*` must not import API/schemas/chat-layer modules.
   - `app/services/llm/routing/*` must not import API/schemas/chat-layer modules.
   - `app/rag/*` must not import chat localization/response-composer modules.
2. Domain import boundaries
   - `app/domain/*` must not import infrastructure/transport packages (`fastapi`, `sqlalchemy`, `httpx`, `app.api`, `app.db`).
3. API-to-RAG direct dependency allowlist
   - only explicitly allowlisted endpoint-level `app.rag.*` imports are allowed.
4. Debug contract assembly ownership
   - `debug_sections` assembly is restricted to `app/services/chat/sources_debug.py`.
5. Low-level localization guard
   - low-level runtime modules (`tabular`, `llm/routing`, `rag`) cannot introduce `localized_text(...)` or `clarification_prompt` markers.
6. Forbidden matching-hint tokens
   - tabular matching/runtime files are scanned for domain-specific hardcoded hint tokens.
7. Module growth budgets
   - strict line-count ceilings on known high-risk files:
     - `app/services/chat/tabular_sql.py`
     - `app/services/chat/rag_prompt_builder.py`
     - `app/services/file.py`
     - `app/services/chat/orchestrator_runtime.py`
     - `app/rag/retriever.py`
8. Function growth budgets
   - strict line-count ceilings on high-risk functions in the same hotspots.

### Warning-Only (Report)

The following checks are reported but do not fail yet:

1. Global oversized module watchlist (`app/**/*.py`, threshold `>500` lines).
2. Domain-to-service coupling watchlist (`app/domain/*` imports from `app.services.*`).
3. Cyrillic watchlist in `app/rag/*` for incremental cleanup planning.
4. Stale budget configuration warnings when monitored files/functions are moved or extracted.

## Contract Test Subset

`scripts/run_architecture_checks.py --with-contract-tests` executes a targeted contract subset after strict static gates pass:

- `tests/unit/test_tabular_sql_no_silent_fallback.py`
- `tests/unit/test_tabular_schema_resolver.py`
- `tests/integration/test_rag_debug_contract.py`

This keeps fallback, schema matching, and debug contract behavior under active enforcement.

## Known Gaps

1. Coarse import checks are static and path-based; they do not yet enforce full dependency graphs or cycle detection.
2. Function complexity (cyclomatic) is not yet enforced; line-budget is used as a lightweight proxy.
3. Domain-to-service coupling and RAG Cyrillic cleanup are warning-only to avoid disruptive big-bang refactors.
4. API-to-runtime boundaries still include a narrow allowlisted exception that should be extracted behind service boundaries.

## Tightening Plan (Next Realistic Step)

1. Move warning-only domain-to-service coupling to strict for newly changed domain files.
2. Replace token-based matching-hint checks with AST-level pattern checks for silent fallback constructs.
3. Add import cycle checks between `chat`, `tabular`, `rag`, and `llm` slices.
4. Ratchet line/function budgets downward only after extractions land, not by forcing immediate breakage.
