# llama-service

FastAPI backend for chat with multi-provider LLM, RAG, JWT auth, and SSE streaming.

## Features
- Async API under `/api/v1/*` plus static frontend serving.
- Multi-provider chat: `ollama`/`local`, `openai`, `aihub` (`corporate` alias supported).
- RAG over uploaded files (ChromaDB) with mixed-embedding retrieval.
- Chat orchestration moved to service layer: `app/services/chat_orchestrator.py`.
- File ingestion worker with queue, retries, graceful shutdown, and stats.
- Unified error handling and request observability middleware.
- In-memory metrics snapshot and Prometheus-compatible metrics export.

## Architecture

```mermaid
flowchart LR
    A[Client / Frontend] --> B[FastAPI app.main]
    B --> C[API Routers /api/v1]
    C --> D[Dependencies JWT/Auth/DB]
    C --> E[CRUD]
    C --> F[Chat Orchestrator]
    F --> G[LLM Manager]
    F --> H[RAG Retriever]
    H --> I[Embeddings + Vector Store]
    E --> J[(PostgreSQL)]
    I --> K[(ChromaDB)]
```

## Key Modules
- `app/main.py`: bootstrap, CORS, middleware, `/health`, `/metrics`, static mount.
- `app/api/v1/endpoints/chat.py`: thin HTTP layer for chat APIs.
- `app/services/chat_orchestrator.py`: chat flow orchestration (conversation, RAG, generation, persistence).
- `app/api/v1/endpoints/stats.py`: user/system/admin stats and observability snapshot.
- `app/observability/metrics.py`: in-memory counters/timers + Prometheus text renderer.
- `app/services/file.py`: async file processing worker, queue, retries, worker stats.
- `app/services/llm/*`: provider abstraction and retries.
- `app/rag/*`: retrieval, vector store, chunking, indexing.

## Repository Structure
```text
.
|-- app/
|   |-- api/
|   |-- core/
|   |-- crud/
|   |-- db/
|   |-- observability/
|   |-- rag/
|   |-- schemas/
|   `-- services/
|-- alembic/
|-- config/
|-- frontend/
|-- scripts/
|-- tests/
|-- docker-compose.db.yml
`-- requirements.txt
```

## Quick Start (Local)

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- Optional: Ollama for local model mode

### Install
```bash
pip install -r requirements.txt
```

### Configure `.env`
Minimal required:
```env
DATABASE_URL=postgresql+asyncpg://user:password@localhost/llama_chat_db
ALEMBIC_DATABASE_URL=postgresql://user:password@localhost/llama_chat_db
JWT_SECRET_KEY=change-me
```

### Init DB
```bash
alembic upgrade head
python scripts/create_admin.py
```

### Run API
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open:
- Swagger: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Prometheus metrics: `http://localhost:8000/metrics`

## API Overview

Base prefix: `/api/v1`

- Auth: `POST /auth/register`, `POST /auth/login`, `GET /auth/me`
- Chat: `POST /chat`, `POST /chat/stream`
- Conversations: `GET /conversations`, `PATCH /conversations/{id}`, `DELETE /conversations/{id}`
- Files: `POST /files/upload`, `GET /files`, `GET /files/status/{file_id}`, `DELETE /files/{file_id}`
- Models: `GET /models/list?mode=local|ollama|aihub|openai|corporate`, `GET /models/status`
- Stats: `GET /stats/user`, `GET /stats/system`, `GET /stats/observability` (admin)

Public root endpoints:
- `GET /health`
- `GET /metrics` (Prometheus text format)

## Metrics and Observability
- Middleware records:
  - `http_requests_total` counter (labels: method, path, status)
  - `http_request_duration_ms` timer aggregates
- Admin snapshot endpoint:
  - `GET /api/v1/stats/observability` returns in-memory metrics + file worker stats.
- Prometheus scrape endpoint:
  - `GET /metrics` returns text format `version=0.0.4`.

## Development

Run tests:
```bash
pytest -q
```

Run smoke tests only:
```bash
pytest tests/smoke -q
```

## Troubleshooting
- `422 validation_error`: request body does not match schema.
- `401 Not authenticated`: missing/invalid Bearer token.
- Empty model list in `/api/v1/models/list`: check provider URL/credentials.
- File processing `failed`: inspect logs and worker stats in `/api/v1/stats/observability`.
- Slow responses: verify provider availability and model readiness.
