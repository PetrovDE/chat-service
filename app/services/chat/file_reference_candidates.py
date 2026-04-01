from __future__ import annotations

import re
from typing import Any, Iterable, List, Sequence, Set

_QUOTED_FILENAME_TOKEN_RE = re.compile(
    r"[\"'`\u00ab]([^\"'`\u00bb]{1,220}\.[A-Za-z0-9]{1,16})[\"'`\u00bb]"
)
_BARE_FILENAME_TOKEN_RE = re.compile(
    r"(?<![A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u04510-9._\-])([A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u04510-9._\-\[\]()]{1,220}\.[A-Za-z0-9]{1,16})(?![A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u04510-9._\-])"
)
_STORED_PREFIX_RE = re.compile(r"^[0-9a-fA-F\-]{8,}_")
_IDENTIFIER_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_EXPLICIT_FILE_CONTEXT_RE = re.compile(
    r"(?:"
    r"\b(?:file|files|filename|document|documents|dataset|spreadsheet|workbook|sheet|csv|tsv|xlsx|xls|parquet|pdf)\b|"
    r"\b(?:in|from|inside|within|about)\s+(?:the\s+)?(?:file|files|document|dataset|spreadsheet|sheet|table)\b|"
    r"(?:\b(?:\u0432|\u0438\u0437|\u043f\u0440\u043e)\s+(?:\u0444\u0430\u0439\u043b|\u0434\u043e\u043a\u0443\u043c\u0435\u043d\u0442|\u0442\u0430\u0431\u043b\u0438\u0446|\u0434\u0430\u0442\u0430\u0441\u0435\u0442|\u043b\u0438\u0441\u0442)[\u0430-\u044f\u0451]*)"
    r")",
    flags=re.IGNORECASE,
)
_TECHNICAL_CONTEXT_RE = re.compile(
    r"(?:"
    r"\b(?:python|pandas|numpy|matplotlib|seaborn|plotly|sklearn|scipy)\b|"
    r"\b(?:traceback|stack\s*trace|attributeerror|typeerror|nameerror|syntaxerror|importerror|exception|error)\b|"
    r"\b(?:object\s+has\s+no\s+attribute|module|class|method|function)\b|"
    r"(?:\u043e\u0448\u0438\u0431\u043a[\u0430-\u044f\u0451]*|\u0442\u0440\u0435\u0439\u0441\u0431\u0435\u043a|\u0438\u0441\u043a\u043b\u044e\u0447\u0435\u043d\u0438[\u0430-\u044f\u0451]*)"
    r")",
    flags=re.IGNORECASE,
)

_HIGH_CONFIDENCE_FILE_EXTENSIONS = {
    "csv",
    "tsv",
    "xlsx",
    "xls",
    "xlsm",
    "xlsb",
    "ods",
    "parquet",
    "txt",
    "md",
    "rst",
    "pdf",
    "doc",
    "docx",
    "rtf",
    "ppt",
    "pptx",
    "json",
    "jsonl",
    "yaml",
    "yml",
    "xml",
    "html",
    "htm",
    "log",
    "zip",
    "gz",
    "bz2",
    "7z",
    "tar",
    "png",
    "jpg",
    "jpeg",
    "gif",
    "bmp",
    "webp",
    "svg",
}
_CODE_LIKE_EXTENSIONS = {
    "py",
    "pyw",
    "ipynb",
    "js",
    "jsx",
    "ts",
    "tsx",
    "java",
    "kt",
    "scala",
    "go",
    "rs",
    "c",
    "h",
    "hpp",
    "cc",
    "cpp",
    "cs",
    "php",
    "rb",
    "pl",
    "r",
    "swift",
    "m",
    "mm",
    "sh",
    "bash",
    "zsh",
    "ps1",
    "bat",
    "cmd",
    "sql",
}


def _normalize_filename_token(value: str) -> str:
    text = str(value or "").strip().strip("`'\"")
    text = text.replace("\\", "/")
    if "/" in text:
        text = text.split("/")[-1]
    return re.sub(r"\s+", " ", text).strip().lower()


def _strip_candidate_token(value: str) -> str:
    return str(value or "").strip().strip(".,;:!?)]}\u00bb")


def _extract_extension(value: str) -> str:
    token = _normalize_filename_token(value)
    if "." not in token:
        return ""
    return token.rsplit(".", 1)[-1].strip().lower()


def _looks_like_path(token: str) -> bool:
    value = str(token or "")
    return "/" in value or "\\" in value


def _looks_like_technical_dotted_identifier(token: str) -> bool:
    normalized = _normalize_filename_token(token)
    if "." not in normalized:
        return False
    segments = [segment for segment in normalized.split(".") if segment]
    if len(segments) < 2:
        return False
    return all(_IDENTIFIER_SEGMENT_RE.fullmatch(segment) for segment in segments)


def _has_glob_wildcard(token: str) -> bool:
    normalized = _normalize_filename_token(token)
    return "*" in normalized or "?" in normalized


def _iter_alias_values(file_obj: Any) -> Iterable[str]:
    original_filename = str(getattr(file_obj, "original_filename", "") or "").strip()
    stored_filename = str(getattr(file_obj, "stored_filename", "") or "").strip()
    if original_filename:
        yield original_filename
    if stored_filename:
        yield stored_filename
        yield _STORED_PREFIX_RE.sub("", stored_filename)

    custom_metadata = getattr(file_obj, "custom_metadata", None)
    if isinstance(custom_metadata, dict):
        for key in ("display_name", "filename", "original_filename", "source_filename"):
            value = str(custom_metadata.get(key) or "").strip()
            if value:
                yield value


def _collect_attached_aliases(files: Sequence[Any]) -> Set[str]:
    aliases: Set[str] = set()
    for file_obj in files:
        for alias in _iter_alias_values(file_obj):
            normalized = _normalize_filename_token(alias)
            if normalized:
                aliases.add(normalized)
    return aliases


def _extract_raw_filename_tokens(query: str) -> List[str]:
    text = str(query or "")
    raw_hits = list(_QUOTED_FILENAME_TOKEN_RE.findall(text))
    raw_hits.extend(_BARE_FILENAME_TOKEN_RE.findall(text))
    out: List[str] = []
    seen = set()
    for raw in raw_hits:
        token = _strip_candidate_token(raw)
        normalized = _normalize_filename_token(token)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(token)
    return out


def _should_keep_candidate(
    *,
    candidate: str,
    explicit_file_context: bool,
    technical_context: bool,
    attached_aliases: Set[str],
) -> bool:
    normalized = _normalize_filename_token(candidate)
    if not normalized:
        return False
    if _has_glob_wildcard(candidate):
        return False

    extension = _extract_extension(candidate)
    if not extension:
        return False

    attached_match = normalized in attached_aliases
    if attached_match:
        return True

    if extension.isdigit():
        return False

    path_like = _looks_like_path(candidate)
    if path_like and not explicit_file_context:
        return False

    extension_is_high_confidence = extension in _HIGH_CONFIDENCE_FILE_EXTENSIONS
    extension_is_code_like = extension in _CODE_LIKE_EXTENSIONS
    dotted_identifier = _looks_like_technical_dotted_identifier(candidate)

    if explicit_file_context:
        if dotted_identifier and not extension_is_high_confidence and technical_context:
            return False
        return True

    if extension_is_code_like:
        return False

    if dotted_identifier and not extension_is_high_confidence:
        return False

    if technical_context and not extension_is_high_confidence:
        return False

    return extension_is_high_confidence


def extract_filename_candidates(
    *,
    query: str,
    conversation_files: Sequence[Any],
) -> List[str]:
    raw_candidates = _extract_raw_filename_tokens(query)
    if not raw_candidates:
        return []

    text = str(query or "")
    explicit_file_context = bool(_EXPLICIT_FILE_CONTEXT_RE.search(text))
    technical_context = bool(_TECHNICAL_CONTEXT_RE.search(text))
    attached_aliases = _collect_attached_aliases(conversation_files)

    out: List[str] = []
    seen = set()
    for candidate in raw_candidates:
        normalized = _normalize_filename_token(candidate)
        if not normalized or normalized in seen:
            continue
        if not _should_keep_candidate(
            candidate=candidate,
            explicit_file_context=explicit_file_context,
            technical_context=technical_context,
            attached_aliases=attached_aliases,
        ):
            continue
        seen.add(normalized)
        out.append(candidate)
    return out
