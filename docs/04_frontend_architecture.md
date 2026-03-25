# 04 Frontend Architecture

Date: 2026-03-23

## Scope

This document describes the current frontend architecture under the persistent user file model:

`upload -> user file library -> attach/detach per chat -> RAG over chat-linked files`

Legacy one-shot attachment state is removed.

## Entry Points

- UI shell: `frontend/index.html`
- App bootstrap/composition root: `frontend/static/js/app.js`
- Chat runtime: `frontend/static/js/chat-manager.js`
- Conversations UI/state: `frontend/static/js/conversations-manager.js`
- Files UI/state (library + current chat panel): `frontend/static/js/files-sidebar-manager.js`
- API layer: `frontend/static/js/api-service.js`
- Settings/auth/theme helpers:
  - `frontend/static/js/settings-manager.js`
  - `frontend/static/js/auth-manager.js`
  - `frontend/static/js/theme-manager.js`

## Frontend Flow Map

`screen -> components -> manager -> api client -> backend endpoint`

1. Chat screen
   - `#chatMessages`, composer, chat file panel (`#chatFilesPanel`)
   - `ChatManager.sendMessage()`
   - `ApiService.streamChat()`
   - `POST /api/v1/chat/stream`

2. File library sidebar (`My files`)
   - `#filesSidebarList`, `#filesSidebarQuota`, `#fileDetailsPanel`
   - `FilesSidebarManager.loadFiles()`
   - `ApiService.getFiles()`, `getFileQuota()`, `getFileStatus()`
   - `GET /api/v1/files/`, `GET /api/v1/files/quota`, `GET /api/v1/files/{file_id}/status`

3. Upload from composer or library button
   - hidden input `#fileInput`, buttons `Upload`
   - `FilesSidebarManager.handleUpload()`
   - `ApiService.uploadFile()`
   - `POST /api/v1/files/upload` (optional `chat_id` for auto-attach)

4. Attach existing file to current chat
   - `#chatFileSelect`, `#attachExistingFileBtn`, library action `Attach`
   - `FilesSidebarManager.handleAttachToCurrentChat()`
   - `ApiService.attachFileToChat()`
   - `POST /api/v1/files/{file_id}/attach`

5. Detach file from current chat
   - chat/library action `Detach`
   - `FilesSidebarManager.handleDetachFromCurrentChat()`
   - `ApiService.detachFileFromChat()`
   - `POST /api/v1/files/{file_id}/detach`

6. Reprocess and delete lifecycle
   - actions `Reprocess`, `Delete`
   - `FilesSidebarManager.handleReprocessFile()`, `handleDeleteFile()`
   - `ApiService.reprocessFile()`, `deleteFile()`
   - `POST /api/v1/files/{file_id}/reprocess`, `DELETE /api/v1/files/{file_id}`

7. Processing/debug details
   - action `Details`
   - `FilesSidebarManager.handleDetails()`
   - `ApiService.getFileDebugInfo()`
   - `GET /api/v1/files/{file_id}/debug`

## State Model

Frontend keeps lightweight in-memory state in manager classes.

File-related state is explicitly split:

1. Global file library state (`FilesSidebarManager`)
   - `files`
   - `quota`
   - `fileStatusDetails` (status/progress/errors)
   - `fileDebugDetails` (debug/details payload)

2. Current chat linked files
   - derived from `files` by `currentConversationId`
   - exposed via `getCurrentChatFileIds()` for chat payload

No local ephemeral `attachedFiles[]` array is used anymore.

## UX Model

1. Upload always writes into user file library.
2. If a chat is active, upload can auto-attach to that chat.
3. User can attach the same file to another chat without re-upload.
4. User can detach from current chat independently.
5. User can delete and reprocess from both library and chat panel.
6. User sees processing progression and readable failure messages.

## Status and Error Rendering

Rendered lifecycle statuses:

- `uploaded`
- `processing`
- `ready`
- `failed`
- `deleting`
- `deleted`

Additional frontend behavior:

- per-file polling for active processing after upload/reprocess
- tabular readiness hint (`csv/tsv/xlsx/xls`)
- user-friendly error mapping for:
  - quota (`413`)
  - access (`401/403`)
  - invalid request (`422`)
  - conflicts (`409`)

Raw backend tracebacks are not shown in UI.

## Cache Invalidation and Refresh

After file mutations frontend triggers reload:

- upload
- attach
- detach
- reprocess
- delete

`FilesSidebarManager.loadFiles(true)` is called after each mutation, plus periodic background refresh.

## Legacy Cleanup

Removed from active frontend flow:

- `frontend/static/js/file-manager.js`
- global clear/remove attachment handlers
- ephemeral one-shot attachment list in composer
- attachment clearing after message send

Chat now references persisted chat-linked files via `filesSidebarManager.getCurrentChatFileIds()`.
