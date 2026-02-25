# llama-service

AI HUB chat backend on FastAPI with:
- multi-provider LLM (`ollama`, `openai`, `aihub`)
- RAG over uploaded files (ChromaDB)
- JWT auth and multi-user conversations
- streaming responses via SSE

## Features
- FastAPI async API (`/api/v1/*`) + static frontend serving
- Chat with optional RAG context from conversation files
- File upload, processing, indexing, status tracking
- Provider abstraction for LLM and embeddings
- PostgreSQL persistence + Alembic migrations
- Request-id aware logging and unified error payloads

## Architecture

```mermaid
flowchart LR
    A[Client Web UI / API Consumer] --> B[FastAPI app.main]
    B --> C[API Routers /api/v1]
    C --> D[Dependencies\nJWT/Auth/DB session]
    C --> E[CRUD Layer]
    C --> F[LLM Manager]
    C --> G[RAG Retriever]

    E --> H[(PostgreSQL)]
    G --> I[Embeddings Manager]
    G --> J[VectorStore Manager]
    I --> F
    J --> K[(ChromaDB)]

    F --> L[Ollama]
    F --> M[OpenAI]
    F --> N[AI HUB]

    B --> O[Static frontend/]
```

## Layered Structure
- `app/main.py`: app bootstrap, middleware, CORS, health, static mount.
- `app/api`: HTTP layer (routers/endpoints/dependencies).
- `app/crud`: data access for DB models.
- `app/services/llm`: provider abstraction and model routing.
- `app/rag`: loaders, splitter, embeddings, retrieval, vector store.
- `app/core`: typed settings, security, logging, error handlers.
- `app/db`: SQLAlchemy models and async session.
- `alembic`: DB migration scripts.

## Repository Structure
```text
.
├── app/
│   ├── api/
│   ├── core/
│   ├── crud/
│   ├── db/
│   ├── observability/
│   ├── rag/
│   ├── schemas/
│   └── services/
├── alembic/
├── config/
├── frontend/
├── scripts/
├── tests/
├── docker-compose.db.yml
└── requirements.txt
```

## Quick Start (Local)

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- Ollama (optional but recommended for local mode)

### Install
```bash
pip install -r requirements.txt
```

### Configure environment
Create `.env` in project root and set required values.

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

## Environment Variables

| Name | Required | Default | Description | Example |
|---|---|---|---|---|
| `DATABASE_URL` | yes | - | async SQLAlchemy DB URL | `postgresql+asyncpg://u:p@localhost/db` |
| `ALEMBIC_DATABASE_URL` | yes | - | sync URL for migrations/tools | `postgresql://u:p@localhost/db` |
| `JWT_SECRET_KEY` | yes | - | JWT signing key | `super-secret` |
| `JWT_ALGORITHM` | no | `HS256` | JWT algorithm | `HS256` |
| `JWT_ACCESS_TOKEN_EXPIRE_MINUTES` | no | `10080` | token TTL minutes | `10080` |
| `allowed_origins` | no | localhost list | CORS origins CSV or `*` | `http://localhost:8000` |
| `LOG_LEVEL` | no | `INFO` | app log level | `DEBUG` |
| `DEFAULT_MODEL_SOURCE` | no | `ollama` | default LLM source | `aihub` |
| `EMBEDDINGS_BASEURL` | no | `http://localhost:11434` | Ollama base URL | `http://localhost:11434` |
| `OLLAMA_CHAT_MODEL` | no | `llama3.2:latest` | local chat model | `llama3.2:latest` |
| `OLLAMA_EMBED_MODEL` | no | `nomic-embed-text:latest` | local embedding model | `nomic-embed-text:latest` |
| `OPENAI_API_KEY` | no | empty | OpenAI token | `sk-...` |
| `OPENAI_MODEL` | no | `gpt-4` | OpenAI chat model | `gpt-4o` |
| `AIHUB_URL` | no | empty | AI HUB base URL | `https://...` |
| `AIHUB_KEYCLOAK_HOST` | no | empty | Keycloak token endpoint | `https://.../token` |
| `AIHUB_USERNAME` | no | empty | AI HUB username | `svc_user` |
| `AIHUB_PASSWORD` | no | empty | AI HUB password | `***` |
| `AIHUB_CLIENT_ID` | no | empty | OAuth client id | `client-id` |
| `AIHUB_CLIENT_SECRET` | no | empty | OAuth client secret | `***` |
| `AIHUB_VERIFY_SSL` | no | `false` | TLS verify for AI HUB | `true` |
| `MAX_FILESIZE_MB` | no | `50` | upload size limit | `50` |
| `CHUNK_SIZE` | no | `2000` | text split chunk size | `2000` |
| `CHUNK_OVERLAP` | no | `400` | chunk overlap | `400` |
| `VECTORDB_PATH` | no | `.chromadb` | Chroma persistence path | `.chromadb` |
| `COLLECTION_NAME` | no | `documents` | base Chroma collection name | `documents` |

## API Overview

Base prefix: `/api/v1`

- `POST /auth/register`
- `POST /auth/login`
- `GET /auth/me`
- `POST /chat`
- `POST /chat/stream`
- `GET /conversations`
- `PATCH /conversations/{conversation_id}`
- `DELETE /conversations/{conversation_id}`
- `POST /files/upload`
- `GET /files`
- `GET /files/status/{file_id}`
- `DELETE /files/{file_id}`
- `GET /models/list?mode=local|ollama|aihub|openai`
- `GET /models/status`
- `GET /stats/user`
- `GET /stats/system`

### curl examples

Login:
```bash
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123456"}'
```

Chat:
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello","model_source":"ollama"}'
```

Streaming chat (SSE):
```bash
curl -N -X POST http://localhost:8000/api/v1/chat/stream \
  -H "Content-Type: application/json" \
  -d '{"message":"Summarize attached file","model_source":"aihub"}'
```

Health:
```bash
curl http://localhost:8000/health
```

## Development

Run smoke tests:
```bash
pytest tests/smoke -q
```

Recommended local quality checks:
```bash
ruff check .
ruff format .
pytest -q
```

If you add pre-commit:
```bash
pre-commit install
pre-commit run --all-files
```

## Troubleshooting
- `422 / validation_error`: request payload does not match schema.
- `401 / Not authenticated`: missing/invalid Bearer token.
- Empty model list in `GET /models/list`: check provider URL/credentials.
- File processing stuck at `failed`: inspect server logs and file content quality.
- Slow responses: verify provider availability and model load state.

## How To Verify Changes
- Start app: `uvicorn app.main:app --reload`
- Check health: `GET /health` should return `{"status":"healthy", ...}`
- Check models status: `GET /api/v1/models/status` returns `ollama/aihub/openai` keys
- Run smoke tests: `pytest tests/smoke -q`

## Roadmap
- Move chat endpoint orchestration into dedicated service layer.
- Add integration tests with mocked LLM providers and DB fixtures.
- Add retry/timeout policy abstraction for all provider calls.
- Harden deployment templates (`nginx` and systemd) to current route layout.
