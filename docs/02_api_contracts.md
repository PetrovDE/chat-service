# 02 API Contracts

Date: 2026-03-23

Source of truth:
- backend routes under `app/api/v1/endpoints/*`
- generated OpenAPI from FastAPI app

## Error Envelope

HTTP errors are returned by global handlers in a normalized envelope:

```json
{
  "error": {
    "code": "http_404",
    "message": "File not found"
  }
}
```

Validation errors (`422`) include `details`.

Current error envelope shape:
- HTTP/Domain errors: `{"error":{"code":"http_<status>","message":"..."}}`
- Validation (`422`): `{"error":{"code":"validation_error","message":"Request validation failed","details":[...]}}`

## File API (persistent user file model)

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/files/upload` | Persist raw file, create file entity, optional auto-process, optional attach to chat |
| GET | `/api/v1/files/` | List my non-deleted files |
| GET | `/api/v1/files/quota` | Get quota usage/limit |
| GET | `/api/v1/files/processed` | List ready files, optional `chat_id` filter |
| GET | `/api/v1/files/{file_id}` | File metadata |
| GET | `/api/v1/files/{file_id}/status` | File processing status summary |
| POST | `/api/v1/files/{file_id}/attach` | Attach existing file to chat |
| POST | `/api/v1/files/{file_id}/detach` | Detach file from chat |
| POST | `/api/v1/files/{file_id}/reprocess` | Start new processing version |
| POST | `/api/v1/files/{file_id}/reindex` | Alias to reprocess intent |
| GET | `/api/v1/files/{file_id}/processing` | List processing versions |
| GET | `/api/v1/files/{file_id}/processing/active` | Active processing profile |
| GET | `/api/v1/files/{file_id}/debug` | Debug payload for file/runtime/processing |
| DELETE | `/api/v1/files/{file_id}` | Delete file lifecycle (links, vectors, artifacts, raw) |

## Models API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/models/list` | List provider models for requested `mode` and `capability` |
| GET | `/api/v1/models/status` | Provider connectivity/status snapshot |

`GET /api/v1/models/list` request params:
- `mode`: `local|ollama|aihub|openai` (provider aliases are normalized server-side)
- `capability`: `chat|embedding` (defaults to `chat`)

Response shape:
- `mode`
- `capability`
- `default_model`: nullable, set only when resolved provider default is currently present in returned `models`
- `models`: provider-available model rows (`name`, `size`, `capability`, `is_default`)
- `count`
- optional `error`

Selector correctness policy (2026-03-29):
- visible model rows are derived from provider-discovered availability for active provider/mode;
- unavailable catalog/default entries are not injected into selector options;
- when provider default is unavailable, `default_model` is `null` and clients must choose from returned `models` only.

## Removed Legacy Contract

- Removed endpoint:
  - `POST /api/v1/files/process/{file_id}`
- Removed attachment-first backend semantics:
  - files are no longer chat-owned temporary entities
  - files are persistent user-owned entities with chat links

## Retrieval Contract (file processing-aware)

RAG metadata and filters now include:
- `user_id`
- `file_id`
- `chat_id`
- `processing_id`
- `pipeline_version`
- `parser_version`
- `artifact_version`
- `embedding_model`
- `embedding_dimension`
- `retrieval_profile`
- `is_active_processing`

Retrieval uses chat-linked files and active processing IDs for those files.
Files without active ready processing are excluded from retrieval/analysis paths.

Notes:
- `GET /api/v1/files/{file_id}/status` returns file lifecycle status (`uploaded|processing|ready|failed|deleting|deleted`) in `status`.
- Detailed ingestion phase (`queued|parsing|...`) is exposed via `stage` and counters from active processing progress.
- `POST /api/v1/files/{file_id}/attach` accepts only file states `uploaded|processing|ready`; otherwise returns `409`.

## Frontend Integration Notes (2026-03-23)

Frontend file UX must follow this split:

1. User file library (global, persistent)
   - source: `GET /api/v1/files/`
   - actions:
     - upload: `POST /api/v1/files/upload`
     - delete: `DELETE /api/v1/files/{file_id}`
     - reprocess: `POST /api/v1/files/{file_id}/reprocess`
     - details/debug: `GET /api/v1/files/{file_id}/debug`
   - status/progress:
     - `GET /api/v1/files/{file_id}/status`

2. Files attached to current chat (chat-scoped links)
   - attach existing:
     - `POST /api/v1/files/{file_id}/attach`
   - detach from chat:
     - `POST /api/v1/files/{file_id}/detach`
   - optional list filter for ready-only view:
     - `GET /api/v1/files/processed?chat_id=<chat_id>`

Upload action in chat composer should upload into file library first and can auto-attach via `chat_id` in upload form payload.
One-shot in-memory attachment lifecycle is not part of the active contract.

### UX cleanup / hardening (frontend)

- File library and current-chat links are separate UI sections and separate frontend state slices.
- Attach/detach is optimistic in UI, then reconciled with backend refresh (`GET /api/v1/files/`).
- Reprocess/delete/update flows must invalidate file library state and current chat file panel state.
- Debug IDs (`file_id`, `processing_id`) are shown only in debug mode (`debug_ui=1` or `localStorage.debug_ui=1`).
- Removed frontend legacy modules:
  - `frontend/static/js/file-manager.js`
  - `frontend/static/js/auth-ui.js`
  - `frontend/static/js/conversations-ui.js`
  - `frontend/static/js/utils.js`

## Chat API Response Contract (2026-03-26)

The chat API now exposes a stabilized outward contract for both non-stream and stream variants.

### Non-Stream (`POST /api/v1/chat/`)

Response model remains `ChatResponse` and includes:

- Existing top-level fields (backward compatible):
  - `response`
  - `conversation_id`
  - `message_id`
  - `model_used`
  - route telemetry: `model_route`, `route_mode`, `provider_selected`, `provider_effective`, `fallback_reason`, `fallback_allowed`, `fallback_attempted`, `fallback_policy_version`, `aihub_attempted`
  - execution telemetry: `execution_route`, `executor_attempted`, `executor_status`, `executor_error_code`, `artifacts_count`
  - `tokens_used`, `generation_time`, `summary`, `caveats`, `sources`, `artifacts`, optional `rag_debug`

- Stabilized contract block:
  - `response_contract.contract_version` (`chat_response_v1`)
  - `response_contract.response_mode` (`general_chat|file_aware|tabular|chart|complex_analytics|narrative|clarification|runtime_error|unknown`)
  - `response_contract.execution_route`
  - `response_contract.selected_route`
  - `response_contract.retrieval_mode`
  - `response_contract.file_resolution_status`
  - `response_contract.clarification_required`
  - `response_contract.controlled_fallback`
  - `response_contract.controlled_response_state`
  - `response_contract.fallback_type`
  - `response_contract.fallback_reason`
  - `response_contract.artifacts_available`
  - `response_contract.artifacts_count`
  - `response_contract.chart_artifact_available`
  - `response_contract.debug_enabled`
  - `response_contract.debug_included`

### Stream (`POST /api/v1/chat/stream`)

SSE transport is unchanged (`start`, `chunk`, `done`, `error`), but stabilized fields now have consistent meaning with non-stream:

- `start`, `done`, and `error` include normalized route/execution telemetry fields.
- `start`, `done`, and `error` include `response_contract` with the same shape and semantics as non-stream.
- `rag_debug` is included only when debug is explicitly enabled (`chat_data.rag_debug=true` or query `debug=true`).
- `debug_enabled` / `debug_included` are explicit in `response_contract`.

### Contract Semantics

- `response_mode` is the canonical mode for UI rendering and mode-specific handling.
- `clarification_required` and `controlled_fallback` are explicit and no longer inferred from mixed fields.
- Chart delivery state is explicit via `chart_artifact_available`; chart success is not inferred from chart spec generation.
- `fallback_reason` in route telemetry is normalized (`none|timeout|network|hub_5xx|circuit_open`) for stable transport-level interpretation.
