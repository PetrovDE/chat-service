from app.observability.file_lifecycle import log_file_lifecycle_event


class _CaptureLogger:
    def __init__(self) -> None:
        self.info_calls = []
        self.warning_calls = []

    def info(self, message, *args):  # noqa: ANN001
        self.info_calls.append((message, args))

    def warning(self, message, *args):  # noqa: ANN001
        self.warning_calls.append((message, args))


def test_file_lifecycle_contract_logs_normalized_payload_without_placeholders():
    logger = _CaptureLogger()
    payload = log_file_lifecycle_event(
        logger,
        "processing_created",
        user_id="user-1",
        chat_id="chat-1",
        file_id="file-1",
        processing_id="proc-1",
        pipeline_version="v2",
        embedding_provider="local",
        embedding_model="nomic-embed-text",
        status="processing",
    )

    assert payload["lifecycle_schema_version"] == 2
    assert payload["user_id"] == "user-1"
    assert payload["chat_id"] == "chat-1"
    assert payload["conversation_id"] == "chat-1"
    assert payload["file_id"] == "file-1"
    assert payload["processing_id"] == "proc-1"
    assert payload["embedding_provider"] == "local"
    assert payload["embedding_model"] == "nomic-embed-text"
    assert payload["status"] == "processing"
    assert payload["upload_id"] is None
    assert payload["embedding_dimension"] is None
    assert logger.info_calls
    assert logger.warning_calls == []


def test_file_lifecycle_contract_warns_when_required_fields_are_missing():
    logger = _CaptureLogger()
    payload = log_file_lifecycle_event(
        logger,
        "file_uploaded",
        user_id="user-1",
        file_id="file-1",
        storage_key="raw/user-1/file-1.xlsx",
        status="uploaded",
    )

    assert payload["event"] == "file_uploaded"
    assert logger.warning_calls
    warning_message, warning_args = logger.warning_calls[0]
    assert "file_lifecycle_contract_violation" in warning_message
    assert warning_args[0] == "file_uploaded"
    assert "upload_id" in warning_args[1]
