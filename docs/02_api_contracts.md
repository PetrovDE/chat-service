> Deprecated: this document is historical. Use `docs/02_service_structure.md` for current architecture contract.

# 02 API Contracts

## OpenAPI Status (FastAPI)

`app.openapi()` должен корректно отражать фактические backend-контракты для `/api/v1/*` и `/health`.

Что зафиксировано:
- Для ключевых endpoint'ов задан явный `response_model`, чтобы в OpenAPI не было пустых `{}`.
- Ошибки возвращаются в едином envelope-формате из `app/core/error_handlers.py`.
- Админские endpoint'ы статистики возвращают `403` при недостатке прав, а не `200` с ошибкой в payload.

## Error Envelope

Любая ошибка, выброшенная как `HTTPException`, должна приходить в формате:

```json
{
  "error": {
    "code": "http_404",
    "message": "File not found"
  }
}
```

Ошибки валидации (`422`) возвращаются в этом же формате и содержат `details`.

```json
{
  "error": {
    "code": "validation_error",
    "message": "Request validation failed",
    "details": [
      {
        "loc": ["body", "message"],
        "msg": "Field required",
        "type": "missing"
      }
    ]
  }
}
```

## Endpoints Table

| Method | Path | Purpose | Request (body/query/path) | Success response | Errors (same envelope) |
|---|---|---|---|---|---|
| GET | `/health` | Проверка живости сервиса | body: none | `200 { status, timestamp }` | - |
| POST | `/api/v1/auth/register` | Регистрация пользователя | body `UserCreate` | `200 UserResponse` | `400`, `422` |
| POST | `/api/v1/auth/login` | Получение JWT | body `UserLogin` | `200 Token` | `401`, `422` |
| GET | `/api/v1/auth/me` | Профиль текущего пользователя | auth Bearer | `200 UserResponse` | `401`, `403` |
| POST | `/api/v1/auth/change-password` | Смена пароля | auth Bearer, body `PasswordChange` | `200 PasswordChangeResponse` | `400`, `401`, `403`, `422` |
| POST | `/api/v1/chat/stream` | Потоковый чат (SSE) | body `ChatMessage`, auth optional | `200 text/event-stream` (`start/chunk/final_refinement/summary/critic/done/error`) | `403`, `404`, `422`, `500` |
| POST | `/api/v1/chat/` | Непотоковый чат | body `ChatMessage`, auth optional | `200 ChatResponse` | `403`, `404`, `422`, `500` |
| GET | `/api/v1/conversations/` | Список диалогов | auth Bearer, query `skip/limit/include_archived` | `200 ConversationResponse[]` | `401`, `403`, `422` |
| GET | `/api/v1/conversations/{conversation_id}/messages` | Сообщения диалога | auth Bearer, path `conversation_id` | `200 ConversationMessageItem[]` | `401`, `403`, `404`, `422` |
| PATCH | `/api/v1/conversations/{conversation_id}` | Обновление диалога | auth Bearer, path `conversation_id`, body `ConversationUpdate` | `200 ConversationResponse` | `401`, `403`, `404`, `422` |
| DELETE | `/api/v1/conversations/{conversation_id}` | Удаление диалога | auth Bearer, path `conversation_id` | `200 ConversationDeleteResponse` | `401`, `403`, `404`, `422` |
| POST | `/api/v1/files/upload` | Загрузка файла + запуск ingestion | auth Bearer, multipart: `file`, `conversation_id`, `embedding_mode?`, `embedding_model?` | `200 FileUploadResponse` | `400`, `401`, `403`, `413`, `422`, `500` |
| POST | `/api/v1/files/process/{file_id}` | Повторный запуск ingestion | auth Bearer, path `file_id`, form `embedding_mode?`, `embedding_model?` | `200 FileReprocessResponse` | `401`, `403`, `404`, `422` |
| GET | `/api/v1/files/` | Список файлов пользователя | auth Bearer, query `skip/limit` | `200 FileInfo[]` | `401`, `403`, `422` |
| GET | `/api/v1/files/processed` | Список обработанных файлов | auth Bearer, query `conversation_id?` | `200 FileInfo[]` | `401`, `403`, `422` |
| GET | `/api/v1/files/{file_id}` | Детали файла | auth Bearer, path `file_id` | `200 FileInfo` | `401`, `403`, `404`, `422` |
| GET | `/api/v1/files/status/{file_id}` | Статус ingestion + counters | auth Bearer, path `file_id` | `200 FileProcessingStatus` | `401`, `403`, `404`, `422` |
| DELETE | `/api/v1/files/{file_id}` | Удаление файла + cleanup индекса | auth Bearer, path `file_id` | `200 FileDeleteResponse` | `401`, `403`, `404`, `422`, `500` |
| GET | `/api/v1/models/list` | Список моделей по mode | query `mode` (`local|ollama|aihub|corporate|openai`) | `200 ModelsListResponse` | `422` |
| GET | `/api/v1/models/status` | Доступность провайдеров/моделей | body: none | `200 ModelsStatusResponse` | - |
| GET | `/api/v1/stats/user` | Пользовательская статистика | auth Bearer | `200 UserStatsResponse` | `401`, `403` |
| GET | `/api/v1/stats/system` | Системная статистика (admin) | auth Bearer (admin) | `200 SystemStatsResponse` | `401`, `403` |
| GET | `/api/v1/stats/observability` | In-memory метрики + file worker stats (admin) | auth Bearer (admin) | `200 ObservabilityStatsResponse` | `401`, `403` |

## Notes

- `/metrics` не включается в OpenAPI (`include_in_schema=False`), но доступен как endpoint Prometheus.
- `FileProcessingStatus.status`: `pending | processing | completed | partial_success | failed`.
- `FileProcessingStatus.stage`: `queued | extract | chunk | embed_upsert | finalized | failed`.
- В статусе файла возвращаются counters: `total_chunks_expected`, `chunks_processed`, `chunks_failed`, `chunks_indexed`.

Chat routing fields:
- Request:
  - `model_source`: `local|ollama|aihub|corporate|openai`
  - `provider_mode` (optional): `explicit|policy`
- Response/SSE telemetry:
  - `model_route`
  - `route_mode`
  - `provider_selected`
  - `provider_effective`
  - `fallback_attempted`
  - `fallback_reason`
  - `fallback_allowed`
  - `fallback_policy_version`
  - `aihub_attempted`
  - `execution_route`
  - `executor_attempted`
  - `executor_status`
  - `executor_error_code`
  - `artifacts_count`
  - `artifacts` (optional list in `ChatResponse`, populated for `complex_analytics`)
  - `rag_debug.complex_analytics.complex_analytics_code_generation_prompt_status` (optional)
  - `rag_debug.complex_analytics.complex_analytics_code_generation_source` (optional)
  - `rag_debug.complex_analytics.complex_analytics_codegen` (optional)
  - `rag_debug.complex_analytics.codegen_auto_visual_patch_applied` (optional)
  - `rag_debug.complex_analytics.complex_analytics_codegen.auto_visual_patch_applied` (optional)
  - `rag_debug.complex_analytics.codegen_plan_timeout_seconds` (optional)
  - `rag_debug.complex_analytics.codegen_timeout_seconds` (optional)
  - `rag_debug.complex_analytics.sandbox.secure_eval` (optional)
  - `rag_debug.complex_analytics.sandbox.artifacts_limit` (optional)
  - `rag_debug.complex_analytics.sandbox.artifacts_limit_base` (optional)
  - `rag_debug.complex_analytics.response_timeout_seconds` (optional)

Chat payload examples:
- Local explicit (no AI HUB attempts expected):
```json
{
  "message": "Summarize attached table",
  "model_source": "local",
  "provider_mode": "explicit",
  "model_name": "llama3.2:latest"
}
```
- AI HUB policy mode:
```json
{
  "message": "Summarize attached table",
  "model_source": "aihub",
  "provider_mode": "policy",
  "model_name": "vikhr",
  "cannot_wait": true,
  "sla_tier": "critical"
}
```
- Complex analytics sandbox request:
```json
{
  "message": "Run Python/pandas NLP on comment_text and build a heatmap by office",
  "model_source": "local",
  "provider_mode": "explicit",
  "rag_debug": true
}
```
Reference examples:
- `docs/examples/chat.request.complex_analytics.json`
- `docs/examples/chat.response.complex_analytics.json`
- `docs/examples/chat.request.complex_analytics.ru.json`
- `docs/examples/chat.response.complex_analytics.ru.json`

RAG debug (для `POST /chat` и SSE `start`):
- `retrieved_chunks_total`
- `coverage.{expected_chunks,retrieved_chunks,ratio,complete}`
- `rows_expected_total`, `rows_retrieved_total`, `rows_used_map_total`, `rows_used_reduce_total`, `row_coverage_ratio`
- `silent_row_loss_detected` (optional, when chunk coverage is high but row coverage is low)
- `top_chunks_limit`, `top_chunks_total`
- `top_chunks[].row_start`, `top_chunks[].row_end`, `top_chunks[].total_rows` для табличных документов
- `retrieval_policy.{mode,query_profile,requested_top_k,effective_top_k,ratio,expected_chunks_total}`
- `retrieval_policy.escalation.{attempted,applied,reason,next_mode,next_top_k,coverage_ratio,coverage_threshold}`
- `retrieval_policy.row_escalation.{attempted,applied,reason,coverage_ratio,coverage_threshold,next_full_file_max_chunks,retried_coverage_ratio}`
- `full_file_map_reduce.batch_diagnostics[].{batch_rows_start_end,batch_input_chars,batch_output_chars}`
- `prompt_chars_before`, `prompt_chars_after`, `prompt_truncated` (если провайдер вернул telemetry)
- `planner_decision.{route,intent,confidence,requires_clarification,reason_codes}` (P4 planner contract)
- `clarification_prompt` (optional, when planner requires clarification)
- Семантика идентификаторов в `top_chunks`:
  - `doc_id` = идентификатор документа (`file_id`)
  - `chunk_id` = идентификатор чанка (`<file_id>_<chunk_index>`)
- `retrieval_mode` может быть: `hybrid | full_file | tabular_sql | complex_analytics | clarification`.
- Для `retrieval_mode=complex_analytics` debug включает:
  - `execution_route=complex_analytics`,
  - `executor_status`, `executor_error_code`,
  - `artifacts_count`,
  - `artifacts[]` (`path` relative to project root + `url` + metadata),
  - `complex_analytics.metrics` и `complex_analytics.notes`,
  - `complex_analytics.code_source` (`llm | template | none`),
  - `complex_analytics.codegen` (status/error/provider metadata for code generation stage),
  - `complex_analytics.codegen_auto_visual_patch_applied` (bool, optional),
  - `complex_analytics.complex_analytics_code_generation_prompt_status` (`success | fallback | disabled`),
  - `complex_analytics.complex_analytics_code_generation_source` (`llm | template | none`),
  - `complex_analytics.complex_analytics_codegen.provider` (effective provider for codegen),
  - `complex_analytics.complex_analytics_codegen.auto_visual_patch_applied` (bool, optional),
  - `complex_analytics.sandbox.secure_eval` (sandbox guard flag).
  - absolute filesystem paths are not returned in API payloads.
- Для `retrieval_mode=tabular_sql` intent может быть:
  - `tabular_aggregate` (single SQL aggregate/group query),
  - `tabular_profile` (per-column deterministic profiling).
- Для `retrieval_mode=clarification` backend возвращает только уточняющий вопрос (без retrieval и без numeric guess).
- В debug присутствует `tabular_sql.{storage_engine,dataset_version,dataset_provenance_id,table_name,table_version,table_provenance_id,...}`:
  - aggregate path: `sql`, `result`, `operation`, `sql_guardrails`, ...
  - profile path: `row_count_sql`, `sample_rows_sql`, `profiled_columns`.
- For deterministic SQL tracing, `tabular_sql` now includes:
  - `executed_sql`,
  - `policy_decision`,
  - `guardrail_flags`.
- For deterministic SQL failures, debug includes classified payload:
  - `deterministic_error.{code,category,message,retryable,details}`.
  - expected codes: `sql_guardrail_blocked | sql_scan_limit_exceeded | sql_result_limit_exceeded | sql_result_size_exceeded | sql_timeout | sql_execution_failed`.
- Для migration compatibility может встречаться `storage_engine=sqlite_legacy` (legacy `tabular_sidecar`).

Sources:
- Базовый формат: `... | rows=<start>-<end>`
- Для `full_file` возможен агрегированный формат: `... | sheet=<sheet> | rows=<merged_ranges>`

Language behavior:
- Язык ответа должен соответствовать языку запроса (RU/EN).
- Если генерация ушла в другой язык, применяется post-processing language rewrite без изменения фактов.
- For `complex_analytics` short-circuit responses, report formatting is language-aware:
  - RU query -> RU report section titles and localized field semantics.
  - EN query -> EN report section titles.

## Update 2026-03-06 (Complex Analytics)
- Backend execution contract now uses two-stage generation (`plan`, `codegen`) before sandbox execution.
- `executor_status` domain is extended with `fallback` (backward compatible).
- Default behavior: safe template fallback is enabled for complex analytics when LLM codegen contract is not satisfied (internal-only behavior).
- When visualization is required and generated code omits chart save contract, backend may auto-repair script in a safe sandboxed way before fallback (internal-only behavior).
- Compose stage may fallback to local structured formatter when LLM output quality is too low (`response_error_code=low_content_quality`), without API contract changes.
- For broad full-analysis prompts, compose stage may be intentionally bypassed in favor of deterministic local formatter output (`response_error_code=broad_query_local_formatter`).
- New optional debug fields for complex analytics pipeline are documented above and remain non-breaking.

## Update 2026-03-10 (Complex Analytics AI HUB Timeout Tuning)

- Internal behavior update for `model_source=aihub` and `provider_mode=policy`:
  - codegen plan/codegen and compose stages now use provider-aware timeout overrides.
  - effective timeout is `max(base, aihub_policy_override)` for each stage.
- New settings:
  - `COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS_AIHUB_POLICY`
  - `COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS_AIHUB_POLICY`
  - `COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS_AIHUB_POLICY`
- API compatibility:
  - HTTP/SSE contract is unchanged.
  - Added optional debug fields only (`codegen_plan_timeout_seconds`, `codegen_timeout_seconds`, `response_timeout_seconds`).

## Update 2026-03-10 (Complex Analytics Quality and Artifact Budget)

- Internal runtime quality hardening:
  - adaptive artifact budget per request/plan complexity (`max_artifacts` effective per run),
  - metrics backfill in executor for incomplete script output (`column_profile`, statistical summaries, relationship findings),
  - stricter compose quality gate against generic "request processed" style responses.
- API compatibility:
  - HTTP/SSE contract unchanged,
  - additional debug-only optional fields (`sandbox.artifacts_limit`, `sandbox.artifacts_limit_base`).

## Internal Refactor Note (2026-03-03)

`chat_orchestrator.py` was decomposed into `app/services/chat/*` modules.
This change is internal and does not modify HTTP/SSE contracts listed above.

## Update 2026-03-06 (Complex Analytics Modular Refactor)

- Internal implementation of `complex_analytics` moved from monolithic module to package:
  - `app/services/chat/complex_analytics/`
  - modules: `planner`, `codegen`, `sandbox`, `executor`, `composer`, `artifacts`, `errors`, `telemetry`,
    `dataset_context`, `template_codegen`, `report_quality`, `localization`, `auto_visual_patch`, `executor_support`
- Public contract is unchanged:
  - `app.services.chat.complex_analytics.execute_complex_analytics_path`
  - `app.services.chat.complex_analytics.is_complex_analytics_query`
- HTTP/SSE payload shape is unchanged (no external API migration required).
- Additional internal split in chat execution layer:
  - `app/services/chat/orchestrator_helpers.py`
  - `app/services/chat/rag_prompt_routes.py`
  - `app/services/chat/orchestrator_runtime.py`
  - `app/services/chat/rag_retrieval_helpers.py`
  - `app/services/chat/rag_prompt_narrative.py`
  External contract remains unchanged.
- Additional internal split in ingestion execution layer:
  - `app/services/file_pipeline.py` (processing/finalization pipeline),
  - `app/services/file.py` (runtime wiring + stable service entrypoints).
  External contract remains unchanged.
- Additional internal split in deterministic SQL execution layer:
  - `app/services/chat/tabular_sql_pipeline.py` (aggregate/profile/error internals),
  - `app/services/chat/tabular_sql.py` (stable route entrypoint and compatibility wrappers).
  External contract remains unchanged.
- Additional internal split in retrieval execution layer:
  - `app/rag/retriever_helpers.py` (intent/filter/scoring/rerank/prompt helpers),
  - `app/rag/retriever.py` (stable retriever class and route-facing entrypoints).
  External contract remains unchanged.
- Additional internal split in full-file analysis prompt builder:
  - `app/services/chat/full_file_analysis_runtime.py` (map-reduce runtime),
  - `app/services/chat/full_file_analysis_helpers.py` (range/merge helpers),
  - `app/services/chat/full_file_analysis.py` (stable facade used by RAG builder).
  External contract remains unchanged.
- Additional internal split in durable ingestion queue:
  - `app/services/ingestion/sqlite_queue_runtime.py` (sync SQL operations),
  - `app/services/ingestion/sqlite_queue.py` (async adapter facade).
  External contract remains unchanged.
- Additional internal split in complex analytics execution path:
  - `app/services/chat/complex_analytics/executor_compose.py` (compose-stage runtime),
  - `app/services/chat/complex_analytics/executor.py` (stable executor entrypoint/orchestration).
  External contract remains unchanged.





