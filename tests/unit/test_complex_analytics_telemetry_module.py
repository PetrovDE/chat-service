from app.services.chat.complex_analytics import telemetry


def test_error_debug_payload_contains_codegen_auto_visual_patch_flag_default():
    payload = telemetry.build_error_debug_payload(
        query="analyze",
        dataset_id="ds-1",
        dataset_version=1,
        code="codegen_failed",
        message="failed",
        details=None,
    )
    ca = payload["complex_analytics"]
    assert ca["codegen_auto_visual_patch_applied"] is False
    assert ca["complex_analytics_codegen"]["auto_visual_patch_applied"] is False


def test_error_debug_payload_propagates_codegen_auto_visual_patch_flag():
    payload = telemetry.build_error_debug_payload(
        query="analyze",
        dataset_id="ds-1",
        dataset_version=1,
        code="codegen_failed",
        message="failed",
        details={
            "codegen": {
                "codegen_auto_visual_patch_applied": True,
                "complex_analytics_codegen": {"auto_visual_patch_applied": True},
            }
        },
    )
    ca = payload["complex_analytics"]
    assert ca["codegen_auto_visual_patch_applied"] is True
    assert ca["complex_analytics_codegen"]["auto_visual_patch_applied"] is True
