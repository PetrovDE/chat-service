# 00 Project Map

Date: 2026-03-23

## Backend Entry Points

- App bootstrap: `app/main.py`
- API router: `app/api/v1/router.py`
- File endpoints: `app/api/v1/endpoints/files.py`
- Conversation endpoints: `app/api/v1/endpoints/conversations.py`
- Chat endpoints: `app/api/v1/endpoints/chat.py`

## File Architecture Map

`endpoint -> service -> storage -> model -> processing -> chat link`

1. `POST /api/v1/files/upload`
2. `files.upload_file` (`app/api/v1/endpoints/files.py`)
3. raw persistence to `settings.get_raw_files_dir() / <user_id> / <file_id>_<safe_name>`
4. DB write to `files` (owner + storage metadata)
5. optional async ingestion queue (`app/services/file.py`)
6. version row in `file_processing_profiles`
7. optional chat link row in `chat_file_links`

## Core File Modules

- DB models:
  - `app/db/models/file.py`
  - `app/db/models/file_processing.py`
  - `app/db/models/conversation_file.py` (`chat_file_links`)
- CRUD:
  - `app/crud/file.py`
- Ingestion:
  - `app/services/file.py`
  - `app/services/file_pipeline.py`
  - `app/services/ingestion/*`
- Retrieval:
  - `app/rag/retriever.py`
  - `app/rag/retriever_helpers.py`
  - `app/services/chat/rag_prompt_builder.py` (active processing gating)

## Storage and Runtime Paths

Configured in `app/core/config.py`, rooted in service folder:

- `runtime/raw_files`
- `runtime/temp_uploads`
- `runtime/file_artifacts`
- `runtime/public/uploads`
- `runtime/vector/chromadb`
- `runtime/queue/.ingestion_jobs.sqlite3`
- `runtime/tabular_runtime/*`

## Migration

- Alembic config:
  - `alembic.ini`
  - `alembic/env.py`
- Current clean-slate initial migration:
  - `alembic/versions/*_initial_clean_schema.py`
- Clean reset runbook:
  - `docs/20_alembic_clean_slate.md`
