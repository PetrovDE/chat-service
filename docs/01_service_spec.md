# 01 Service Specification

Date: 2026-03-23

## File Lifecycle States

File entity status (`files.status`):
- `uploaded`
- `processing`
- `ready`
- `failed`
- `deleting`
- `deleted`

Processing profile status (`file_processing_profiles.status`):
- queue/running stages: `queued`, `parsing`, `parsed`, `chunking`, `embedding`, `indexing`
- terminal stages: `ready`, `partial_success`, `partial_failed`, `failed`, `deleted`

## Ownership and Access

- Files are owned by user (`files.user_id`).
- Chats only link files via `chat_file_links`.
- Access to file endpoints is owner-only.

## Quota

- `USER_FILE_QUOTA_BYTES = 1_073_741_824` (1 GB).
- Enforced during upload stream write.
- Applied to active (non-deleted) raw file bytes.

## Processing Versioning

Each processing attempt creates `file_processing_profiles` row with:
- pipeline/parser/artifact versions
- embedding provider/model/dimension
- chunking strategy and retrieval profile
- status + timing + active flag

Exactly one active processing profile per file (`is_active=true`).

## Delete

Delete flow removes:
- chat links
- vectors
- derived artifacts
- raw file
- marks processing versions inactive/deleted
- marks file deleted and releases quota

## Reprocess/Reindex

Reprocess keeps raw file, creates a new processing profile, builds artifacts/vectors under new `processing_id`, then switches active profile on terminal success.

Embedding preflight validation errors in upload/reprocess are returned as `422` with normalized error envelope.

## Attach/Detach Contract

- Attach is allowed only for file states: `uploaded`, `processing`, `ready`.
- Attach for non-attachable states (for example `failed`, `deleting`) returns `409`.
- Detach is idempotent by chat link and returns removed link count.

## Tabular Runtime

- Deterministic tabular execution uses shared `duckdb_parquet` runtime metadata under `custom_metadata.tabular_dataset`.
- Legacy SQLite sidecar metadata is not used in active backend flow.
