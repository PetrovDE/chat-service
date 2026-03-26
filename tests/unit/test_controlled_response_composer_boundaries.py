from __future__ import annotations

from pathlib import Path

from app.services.chat import tabular_response_composer as tabular_composer
from app.services.chat import controlled_response_composer as controlled_composer


def test_controlled_fallback_templates_cover_file_and_tabular_states() -> None:
    file_missing = controlled_composer.build_file_not_found_message(
        preferred_lang="en",
        missing_candidates=["report.xlsx"],
    )
    file_ambiguous = controlled_composer.build_ambiguous_file_message(
        preferred_lang="en",
        ambiguous_options={"report.xlsx": ["`report.xlsx` (file_id=f1)", "`report.xlsx` (file_id=f2)"]},
    )
    missing_column = tabular_composer.build_missing_column_message(
        preferred_lang="en",
        requested_fields=["month of birth"],
        alternatives=["birth_month", "age"],
        ambiguous=False,
    )
    chart_failed = tabular_composer.build_chart_response_text(
        preferred_lang="en",
        column_label="Status Code",
        chart_rendered=True,
        chart_artifact_available=False,
        chart_fallback_reason="artifact_not_accessible",
        result_text="[[\"200\", 10]]",
    )

    assert "could not find file" in file_missing.lower()
    assert "please clarify" in file_ambiguous.lower()
    assert "available options" in missing_column.lower()
    assert "could not be delivered" in chart_failed.lower()
    assert "reason=artifact_not_accessible" in chart_failed


def test_localization_is_preserved_in_controlled_responses() -> None:
    en_runtime = controlled_composer.build_runtime_error_message(preferred_lang="en")
    ru_runtime = controlled_composer.build_runtime_error_message(preferred_lang="ru")
    en_no_retrieval = controlled_composer.build_no_retrieval_message(preferred_lang="en")
    ru_no_retrieval = controlled_composer.build_no_retrieval_message(preferred_lang="ru")

    assert "Internal runtime error" in en_runtime
    assert "Please retry the request." in en_runtime
    assert "Please retry the request." not in ru_runtime
    assert "No relevant chunks were found" in en_no_retrieval
    assert "No relevant chunks were found" not in ru_no_retrieval


def test_cleaned_modules_no_longer_format_controlled_copy_directly() -> None:
    files = {
        "app/services/chat/rag_prompt_file_resolution.py": "build_file_not_found_message",
        "app/services/chat/rag_prompt_narrative.py": "build_no_retrieval_message",
        "app/services/chat/orchestrator_runtime.py": "build_runtime_error_message",
        "app/services/chat/tabular_deterministic_result.py": "build_chart_response_text",
    }

    for path, expected_symbol in files.items():
        source = Path(path).read_text(encoding="utf-8")
        assert "localized_text(" not in source
        assert expected_symbol in source


def test_tabular_composer_wrappers_match_centralized_templates() -> None:
    wrapped = tabular_composer.build_timeout_message(preferred_lang="en")
    central = controlled_composer.build_timeout_message(preferred_lang="en")

    assert wrapped == central


def test_chart_response_text_includes_source_and_top_bucket_hint() -> None:
    message = tabular_composer.build_chart_response_text(
        preferred_lang="en",
        column_label="Status Code",
        chart_rendered=True,
        chart_artifact_available=True,
        chart_fallback_reason="none",
        result_text='[["200", 10], ["500", 2]]',
        source_scope="orders.xlsx | sheet=Orders | table=orders_sheet",
    )

    assert "Status Code" in message
    assert "Used data: orders.xlsx | sheet=Orders | table=orders_sheet." in message
    assert "Top bucket: `200` (10)." in message
    assert "Top 2 buckets cover 100.0% of counted rows." in message
