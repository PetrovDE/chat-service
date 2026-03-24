# llama-service

FastAPI-based AI chat service with:
- chat and SSE chat endpoints,
- persistent user-owned file library,
- async file ingestion to RAG vector store,
- table-aware retrieval and deterministic tabular analytics.

## Core Capabilities
- Chat API: `/api/v1/chat`, `/api/v1/chat/stream`
- Providers: `aihub`, `local/ollama`, `openai`
- Persistent file lifecycle: upload, attach/detach to chat, reprocess, delete
- RAG constrained to files linked to the current chat
- Versioned file processing profiles (`processing_id`, versions, embedding metadata)

## Quick Start
1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Start PostgreSQL:
```bash
docker compose -f docker-compose.db.yml up -d
```
3. Configure `.env` (minimum):
```env
DATABASE_URL=postgresql+asyncpg://llama_chat_user:1306@localhost:5432/llama_chat_db
ALEMBIC_DATABASE_URL=postgresql://llama_chat_user:1306@localhost:5432/llama_chat_db
JWT_SECRET_KEY=change-me
```
4. Run migrations:
```bash
alembic upgrade head
alembic current
alembic check
```
5. Start API:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open:
- App: `http://localhost:8000/`
- Swagger: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Metrics: `http://localhost:8000/metrics`

## Architecture (High Level)
```text
Browser SPA (frontend/*)
  -> FastAPI (/api/v1/*)
    -> Chat orchestrator
      -> LLM providers
      -> RAG retriever
      -> Tabular deterministic route
    -> File ingestion queue/worker
      -> parse/chunk/embed/upsert
    -> CRUD/SQLAlchemy
      -> PostgreSQL
```

## File Lifecycle Contract
`files.status` values:
- `uploaded`
- `processing`
- `ready`
- `failed`
- `deleting`
- `deleted`

Processing stage details are exposed through `ingestion_progress.stage`
(for example `queued`, `parsing`, `chunking`, `embedding`, `indexing`, `completed`, `partial_failed`, `failed`).

Attach contract:
- Attach allowed only for `uploaded|processing|ready`
- Non-attachable states return `409`

Delete contract:
- Removes chat links, vectors, derived artifacts, and raw file
- Marks file as `deleted` and releases quota

## API Notes (Files)
- Removed legacy endpoint: `POST /api/v1/files/process/{file_id}`
- Use: `POST /api/v1/files/{file_id}/reprocess`
- Status: `GET /api/v1/files/{file_id}/status`
- File API is user-owned + chat-link based (not attachment-temporary)

## Runtime Storage
Service-local runtime paths:
- `runtime/raw_files/`
- `runtime/temp_uploads/`
- `runtime/file_artifacts/`
- `runtime/public/uploads/`
- `runtime/vector/chromadb/`
- `runtime/queue/.ingestion_jobs.sqlite3`
- `runtime/tabular_runtime/`

Runtime data is git-ignored.

## Clean-Slate DB Reset (Test/Dev)
When DB architecture changes heavily, reset flow is:
1. Drop and recreate test DB schema.
2. Remove old files from `alembic/versions/*.py`.
3. Generate new initial migration with autogenerate.
4. Inspect generated migration (ensure no legacy tables / no `drop_*` in upgrade).
5. Run `alembic upgrade head`.
6. Verify with `alembic current` and `alembic check`.

Do not use `scripts/init_db.py` as migration source of truth for this flow.

Detailed runbook: [docs/20_alembic_clean_slate.md](docs/20_alembic_clean_slate.md)

## Canonical Documentation
- [docs/00_project_map.md](docs/00_project_map.md)
- [docs/01_service_spec.md](docs/01_service_spec.md)
- [docs/02_api_contracts.md](docs/02_api_contracts.md)
- [docs/01_architecture_overview.md](docs/01_architecture_overview.md)
- [docs/05_query_planner.md](docs/05_query_planner.md)
- [docs/07_llm_routing.md](docs/07_llm_routing.md)
- [docs/09_observability.md](docs/09_observability.md)
- [docs/13_persistent_user_files_architecture.md](docs/13_persistent_user_files_architecture.md)
- [docs/19_big_file_refactor_complex_analytics.md](docs/19_big_file_refactor_complex_analytics.md)
- [docs/20_alembic_clean_slate.md](docs/20_alembic_clean_slate.md)
- [docs/adr/ADR-014-complex-analytics-sandbox-executor.md](docs/adr/ADR-014-complex-analytics-sandbox-executor.md)
