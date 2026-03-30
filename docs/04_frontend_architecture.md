# 04 Frontend Architecture

Date: 2026-03-29

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
   - `#chatMessages`, composer
   - `ChatManager.sendMessage()`
   - `ApiService.streamChat()`
   - `POST /api/v1/chat/stream`

2. File library sidebar (`My files`)
   - `#filesSidebarList`, `#filesSidebarQuota`, `#fileDetailsPanel`
   - `FilesSidebarManager.loadFiles()`
   - `ApiService.getFiles()`, `getFileQuota()`, `getFileStatus()`
   - `GET /api/v1/files/`, `GET /api/v1/files/quota`, `GET /api/v1/files/{file_id}/status`

3. Upload from composer controls row or file drawer
   - hidden input `#fileInput`, composer attach action `#composerAttachBtn`, drawer upload action `#filesDrawerUploadBtn`
   - `FilesSidebarManager.handleUpload()`
   - `ApiService.uploadFile()`
   - `POST /api/v1/files/upload` (optional `chat_id` for auto-attach)

4. Attach existing file to current chat
   - file drawer library action `Attach`
   - `FilesSidebarManager.handleAttachToCurrentChat()`
   - `ApiService.attachFileToChat()`
   - `POST /api/v1/files/{file_id}/attach`

5. Detach file from current chat
   - file drawer/library action `Detach`
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
5. User can delete and reprocess from the file drawer/library actions.
6. User sees processing progression and readable failure messages.

## Chat Screen Layout (2026-03-29)

Main chat screen keeps conversation as the primary focus:

1. Left sidebar:
   - chats list
   - chat search
   - new chat action
2. Center column:
   - current chat header
   - message feed (`#chatMessages`)
   - sticky composer
3. Composer visible controls:
   - primary row: message input (`#messageInput`)
   - compact secondary row: attach/upload (`#composerAttachBtn`), provider selector (`#mode-selector`), chat model selector (`#model-selector`), advanced toggle (`#composerAdvancedToggle`), send/stop
4. Composer advanced controls (floating anchored panel `#composerAdvancedPanel`):
   - embedding model selector (`#embedding-model-selector`)
   - RAG mode selector (`#ragModeSelector`)
5. File management details:
   - no in-flow file-management block in center chat flow
   - detailed file management in right-side drawer (`#filesSidebarList`, `#fileDetailsPanel`) for upload/attach/list/details/reprocess/delete actions

## Model Selector Data Contract (Frontend)

Frontend model selectors consume `GET /api/v1/models/list?mode=<provider>&capability=<chat|embedding>` and apply these rules:

1. Visible options are sourced only from provider-available models returned by backend.
2. `default_model` is applied only when that model is present in the returned option list.
3. If previously selected model is unavailable after provider switch:
   - selector moves to provider default if available;
   - otherwise selector moves to first available option;
   - if no options are available, selection is cleared.
4. Frontend does not reinsert unavailable defaults into dropdown options.
5. Frontend guards against stale async provider responses:
   - outdated model-list responses are ignored if request order or selected provider changed;
   - provider-scoped previous selections are preserved so an empty model list on one provider does not wipe valid selections for another provider.

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
