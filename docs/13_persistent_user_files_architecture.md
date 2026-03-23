# 13 Persistent User Files Architecture

Date: 2026-03-23

## Goal

This document defines the new file architecture:

`user-owned persistent file -> raw storage -> processing versions -> chat link -> retrieval by active processing -> delete/reprocess lifecycle`

Legacy attachment-first flow is removed from the backend contract.

## Core Principles

1. Files are persistent user artifacts.
2. Raw file is stored first, then processing starts.
3. One upload can be reused across multiple chats.
4. Access is private per owner user.
5. Raw storage quota is enforced per user: `1 GB`.
6. Processing is explicitly versioned and switchable (`is_active`).
7. Reprocess/reindex and delete are first-class lifecycle operations.
8. Runtime storage stays inside service folder (`runtime/*`) and is ignored by git.

## Data Model

### `files`

- `id` (`file_id`)
- `user_id` (`owner_user_id`)
- `original_filename`
- `stored_filename`
- `storage_key`
- `storage_path`
- `mime_type`
- `extension`
- `size_bytes`
- `checksum`
- `visibility` (`private`)
- `status` (`uploaded|processing|ready|failed|deleting|deleted`)
- `source_kind`
- `created_at`
- `updated_at`
- `deleted_at`

### `chat_file_links`

- `id`
- `chat_id`
- `file_id`
- `attached_by_user_id`
- `attached_at`
- unique (`chat_id`, `file_id`)

### `file_processing_profiles`

- `id` (`processing_id`)
- `file_id`
- `pipeline_version`
- `parser_version`
- `artifact_version`
- `embedding_provider`
- `embedding_model`
- `embedding_dimension`
- `chunking_strategy`
- `retrieval_profile`
- `status`
- `started_at`
- `finished_at`
- `error_message`
- `is_active`
- `ingestion_progress` (JSON)
- `artifact_metadata` (JSON)
- `created_at`
- `updated_at`

## Runtime Storage Layout

All runtime paths are configured in `app/core/config.py` and resolved relative to service root:

- `runtime/raw_files/` raw uploaded user files
- `runtime/temp_uploads/` temporary upload staging
- `runtime/file_artifacts/` file-scoped derived artifacts
- `runtime/public/uploads/` public static artifacts (for UI links)
- `runtime/exports/` debug/export outputs
- `runtime/local_index/` local index/cache data
- `runtime/vector/chromadb/` vector storage
- `runtime/queue/.ingestion_jobs.sqlite3` durable ingestion queue
- `runtime/tabular_runtime/` tabular runtime datasets/catalog

## Quota Model

- Limit: `USER_FILE_QUOTA_BYTES = 1_073_741_824` (1 GB).
- Accounted bytes: sum of `files.size_bytes` for non-deleted file records.
- Enforced during streaming upload write (hard-fail with `413` when exceeded).
- Quota is released when file is deleted (`status=deleted` and raw removed).

## Ownership and Access

- Every file is bound to `files.user_id`.
- All file endpoints resolve file by `(file_id, current_user.id)`.
- Cross-user access returns `404`/`403` depending on endpoint context.
- Chat attach/detach requires both:
  - chat owned by current user,
  - file owned by current user.

## Processing and Active Selection

- Upload creates persistent file metadata first.
- Processing creates a new `file_processing_profiles` row.
- Terminal ready-like processing result activates one profile (`is_active=true`) and deactivates others.
- Retrieval uses only chat-linked files with active ready processing profile.
- Retrieval filters vectors by active `processing_id` per file (no cross-version mixing).
- Vector metadata includes:
  - `owner user_id`, `file_id`, `chat_id`, `processing_id`,
  - `pipeline_version`, `parser_version`, `artifact_version`,
  - `embedding_model`, `embedding_dimension`,
  - `retrieval_profile`, `is_active_processing` marker.

## Reprocess / Reindex Lifecycle

1. Validate owner and raw file presence.
2. Create new processing profile with requested versions/models.
3. Queue durable ingestion job with explicit processing/version payload.
4. Build new artifacts/vectors under new `processing_id`.
5. On successful finalize (`ready`/partial-ready), switch active profile.
6. Retrieval reads only active processing IDs.

## Delete Lifecycle

1. Mark file as `deleting`.
2. Remove chat links (`chat_file_links`).
3. Remove vectors by `file_id`.
4. Remove derived artifacts (tabular + file artifact directory).
5. Remove raw file from `runtime/raw_files/...`.
6. Mark processing profiles inactive/deleted.
7. Mark file as `deleted` (`deleted_at` set), quota recalculated.

Deleted files are excluded from normal file listings and ready retrieval.

## File API Contract (UI-ready)

- `POST /api/v1/files/upload`
- `GET /api/v1/files/`
- `GET /api/v1/files/quota`
- `GET /api/v1/files/processed?chat_id=<uuid>`
- `GET /api/v1/files/{file_id}`
- `GET /api/v1/files/{file_id}/status`
- `POST /api/v1/files/{file_id}/attach`
- `POST /api/v1/files/{file_id}/detach`
- `POST /api/v1/files/{file_id}/reprocess`
- `POST /api/v1/files/{file_id}/reindex`
- `GET /api/v1/files/{file_id}/processing`
- `GET /api/v1/files/{file_id}/processing/active`
- `GET /api/v1/files/{file_id}/debug`
- `DELETE /api/v1/files/{file_id}`

## Observability and Structured Logging

`file_lifecycle` log events include fields:

- `rid`
- `uid`
- `chat_id`
- `file_id`
- `upload_id`
- `processing_id`
- `pipeline_version`
- `embedding_model`
- `embedding_dimension`
- `storage_key`
- `quota_used_bytes`
- `quota_limit_bytes`
- `status`
- `is_active_processing`

### Journalctl examples

- all file lifecycle:
  - `journalctl -u llama-service -o cat | rg "file_lifecycle"`
- specific user:
  - `journalctl -u llama-service -o cat | rg "\"uid\":\"<user-uuid>\""`
- specific file:
  - `journalctl -u llama-service -o cat | rg "\"file_id\":\"<file-uuid>\""`
- specific processing:
  - `journalctl -u llama-service -o cat | rg "\"processing_id\":\"<processing-uuid>\""`

## Breaking Changes

1. Legacy endpoint removed:
   - removed `POST /api/v1/files/process/{file_id}`
   - use `POST /api/v1/files/{file_id}/reprocess`
2. File upload/query now uses `chat_id` for chat linkage in file API.
3. File DTO switched to persistent shape (`file_id`, `status`, `chat_ids`, storage metadata).
4. Legacy attachment-only semantics are removed from backend file lifecycle.
5. Storage paths moved to `runtime/*` layout under service root.

## Legacy Removed

- Old one-shot attachment model as source of truth for file lifecycle.
- Old DB relation/table naming for conversation-file attachment (`conversation_files`).
- Legacy alias processing route (`/files/process/{file_id}`).
- Legacy SQLite tabular sidecar execution path (`sqlite_legacy`, `tabular_sidecar` metadata).
