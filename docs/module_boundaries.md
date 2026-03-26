# Module Boundaries

## Scope

This file defines intended responsibilities and dependency boundaries for major backend layers.

## Layer Contract Matrix

| Layer | Responsibility | Allowed Dependencies | Forbidden Dependencies | Anti-Pattern Examples |
|---|---|---|---|---|
| `app/api` | HTTP parsing, validation, auth wiring, response mapping | `app/schemas`, `app/core`, orchestrator entrypoints in `app/services` | Direct calls into `app/rag` internals, SQL/ORM queries in endpoints | Endpoint builds SQL or vector filters directly |
| `app/domain` | Pure decision logic and contracts (planner, intent decisions) | Standard library and typed contracts | `fastapi`, ORM, provider SDKs | Domain module imports provider client or HTTP exception |
| `app/services/chat` | Chat orchestration and route coordination | `app/domain`, `app/services/llm`, `app/services/tabular`, `app/rag`, shared contracts | Direct HTTP exception shaping, UI copy literals in low-level helpers | Single function handles planning, execution, localization, and debug assembly |
| `app/services/tabular` | Deterministic tabular runtime and SQL safety/execution | `app/core`, storage/runtime adapters, typed errors | API transport concerns and user-facing copy composition | Runtime silently guesses fields or mixes SQL and chat rendering |
| `app/services/llm` | Provider routing, policy, retries, reliability, model execution | `app/core`, provider adapters, telemetry contracts | API response rendering and business workflow branching | Router returns user-facing localized text |
| `app/services/ingestion` | Durable ingestion queue, worker lifecycle, processing orchestration | `app/core`, storage adapters, models/crud via service boundary | Chat response composition or endpoint concerns | Worker module mutates API response contract |
| `app/rag` | Retrieval/indexing primitives and vector adapters | `app/core`, embeddings/vector integrations | Endpoint concerns, conversation orchestration policy | Retriever decides user message fallback copy |
| `app/observability` | Metrics, request context, logging helpers | Any layer via stable helper APIs | Business policy branching | Metrics helper embeds domain fallback rules |
| `app/crud` and `app/db` | Persistence models, queries, database session boundaries | SQLAlchemy models and session utilities | HTTP layer imports, LLM provider logic | CRUD module calls LLM or retrieval pipeline |
| `app/schemas` | Transport DTOs only | Pydantic and typing helpers | Runtime business logic | Schema module computes planner routes |
| `app/core` | Shared config, exceptions, logging bootstrap, security primitives | Base utilities | Feature-specific orchestration logic | Config module includes route-specific fallback text |

## Shared Contracts (Current Equivalent)

The project currently uses these shared contract points:
- Exceptions and error primitives: `app/core/exceptions.py`
- Error envelope mapping: `app/core/error_handlers.py`
- Transport contracts: `app/schemas/*`
- Domain planner contracts: `app/domain/chat/query_planner.py`
- Controlled user-facing fallback/clarification composition: `app/services/chat/controlled_response_composer.py`

If a dedicated shared contracts package is introduced later, it must preserve the same dependency direction (inward-only for business logic).

## Dependency Direction Rules

1. API depends on orchestration contracts, not runtime internals.
2. Domain logic must not depend on transport or infrastructure SDKs.
3. Runtime adapters (`rag`, provider clients, tabular storage) must not emit user-facing copy.
4. Observability helpers are cross-cutting and must stay policy-neutral.

## Boundary Violation Checklist

Before merging:
- Verify no new reverse dependencies were introduced.
- Verify no low-level module now owns user-facing text or localization decisions.
- Verify orchestration modules delegate debug payload construction to shared builders.
- Verify matching/routing logic stays schema-first with explicit failure paths.
