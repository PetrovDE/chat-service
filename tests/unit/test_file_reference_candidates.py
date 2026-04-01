from types import SimpleNamespace

from app.services.chat.file_reference_candidates import extract_filename_candidates


def test_dotted_python_identifiers_are_not_treated_as_filenames_without_file_context():
    query = (
        "what does this Python error mean: "
        "'numpy.ndarray' object has no attribute 'itertuples' "
        "and how is it different from pandas.DataFrame?"
    )
    candidates = extract_filename_candidates(query=query, conversation_files=[])
    assert candidates == []


def test_glob_like_symbol_is_not_treated_as_filename_candidate():
    query = "why does '*.itertuples' fail in this traceback?"
    candidates = extract_filename_candidates(query=query, conversation_files=[])
    assert candidates == []


def test_stacktrace_path_is_ignored_without_explicit_uploaded_file_context():
    query = (
        "Traceback ... File '/usr/lib/python3.11/site-packages/pandas/core/frame.py', line 1, "
        "AttributeError: object has no attribute"
    )
    candidates = extract_filename_candidates(query=query, conversation_files=[])
    assert candidates == []


def test_spreadsheet_filename_without_file_keyword_is_still_detected():
    query = "show summary for monthly_report.xlsx"
    candidates = extract_filename_candidates(query=query, conversation_files=[])
    assert candidates == ["monthly_report.xlsx"]


def test_explicit_file_context_allows_uncommon_extension():
    query = "please check file report.customext"
    candidates = extract_filename_candidates(query=query, conversation_files=[])
    assert candidates == ["report.customext"]


def test_attached_exact_alias_allows_uncommon_extension_without_file_keywords():
    attached = [
        SimpleNamespace(
            id="f-1",
            original_filename="foo.bar.baz",
            stored_filename="f-1_foo.bar.baz",
            custom_metadata={},
        )
    ]
    query = "foo.bar.baz"
    candidates = extract_filename_candidates(query=query, conversation_files=attached)
    assert candidates == ["foo.bar.baz"]

