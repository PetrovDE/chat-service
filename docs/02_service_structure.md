# 02. Service Structure

## Directory Responsibilities
- `app/main.py`: FastAPI bootstrap, middleware, `/health`, `/metrics`, startup recovery.
- `app/api/v1/endpoints/*`: HTTP contracts.
- `app/api/dependencies.py`: auth dependencies.
- `app/domain/chat/query_planner.py`: query route/intent decision.
- `app/services/chat_orchestrator.py`: chat lifecycle orchestration.
- `app/services/chat/*`: context build, prompt assembly, retrieval policy, RAG debug shaping, postprocess.
- `app/services/llm/*`: provider registry, model router, fallback policy, circuit breaker.
- `app/services/file.py`: ingestion scheduling/processing/finalization.
- `app/services/ingestion/*`: durable queue contracts, SQLite adapter, worker runtime.
- `app/services/tabular/*`: tabular dataset storage/runtime, SQL guardrails/execution/errors.
- `app/rag/*`: loaders, splitter, embeddings adapter, vector store, retriever.
- `app/observability/*`: request context, metrics, SLO wrappers, middleware.
- `scripts/evals/*`: offline/online eval runner and CI gates.

## API Entry Points
- `POST /api/v1/chat/`
- `POST /api/v1/chat/stream`
- `POST /api/v1/files/upload`
- `GET /api/v1/files/`
- `GET /api/v1/files/quota`
- `GET /api/v1/files/{file_id}/status`
- `POST /api/v1/files/{file_id}/attach`
- `POST /api/v1/files/{file_id}/detach`
- `POST /api/v1/files/{file_id}/reprocess`
- `POST /api/v1/files/{file_id}/reindex`
- `GET /api/v1/files/{file_id}/processing`
- `GET /api/v1/files/{file_id}/processing/active`
- `DELETE /api/v1/files/{file_id}`
- `GET /api/v1/stats/observability`
- `GET /health`
- `GET /metrics`

## Contracts
- `ChatResponse`: includes `model_route`, `fallback_reason`, `fallback_allowed`, `fallback_policy_version`.
- `FileProcessingStatus`: ingestion counters and stage fields.
- `rag_debug` payload: includes retrieval diagnostics, top chunks, and coverage fields.
