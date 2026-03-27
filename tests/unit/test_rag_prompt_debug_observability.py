from __future__ import annotations

import uuid

from app.observability.context import request_id_ctx
from app.services.chat.rag_prompt_debug import inject_file_resolution_debug


def test_inject_file_resolution_debug_propagates_correlation_and_source_ids():
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    token = request_id_ctx.set("rid-stage4-test")
    try:
        payload = inject_file_resolution_debug(
            rag_debug={
                "retrieval_mode": "tabular_sql",
                "selected_route": "aggregation",
                "fallback_reason": "none",
            },
            resolution_meta={
                "file_resolution_status": "resolved_unique",
                "requested_file_names": ["sales.xlsx"],
                "resolved_file_names": ["sales.xlsx"],
                "resolved_file_ids": ["file-1"],
                "resolved_upload_ids": ["upload-1"],
                "resolved_document_ids": ["document-1"],
            },
            preferred_lang="en",
            query="count rows",
            user_id=user_id,
            conversation_id=conversation_id,
        )
    finally:
        request_id_ctx.reset(token)

    assert payload["request_id"] == "rid-stage4-test"
    assert payload["conversation_id"] == str(conversation_id)
    assert payload["user_id"] == str(user_id)
    assert payload["file_id"] == "file-1"
    assert payload["upload_id"] == "upload-1"
    assert payload["document_id"] == "document-1"
