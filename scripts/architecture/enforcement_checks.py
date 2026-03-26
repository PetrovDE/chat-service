from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")


@dataclass(frozen=True)
class CheckIssue:
    check: str
    message: str
    severity: str = "strict"


RUNTIME_IMPORT_RULES: Dict[str, Tuple[str, ...]] = {
    "app/services/tabular": (
        "fastapi",
        "app.api",
        "app.schemas",
        "app.services.chat",
    ),
    "app/services/llm/routing": (
        "fastapi",
        "app.api",
        "app.schemas",
        "app.services.chat",
    ),
    "app/rag": (
        "fastapi",
        "app.api",
        "app.schemas",
        "app.services.chat.language",
        "app.services.chat.tabular_response_composer",
    ),
}

DOMAIN_FORBIDDEN_IMPORT_PREFIXES: Tuple[str, ...] = (
    "fastapi",
    "sqlalchemy",
    "httpx",
    "app.api",
    "app.db",
)

API_RAG_IMPORT_ALLOWLIST: Dict[str, Tuple[str, ...]] = {
    "app/api/v1/endpoints/files.py": ("app.rag.vector_store",),
}

DEBUG_SECTIONS_OWNER = "app/services/chat/sources_debug.py"

MATCHING_HINT_FILES: Tuple[str, ...] = (
    "app/services/chat/tabular_schema_resolver.py",
    "app/services/chat/tabular_intent_router.py",
    "app/services/chat/tabular_sql.py",
)

FORBIDDEN_MATCHING_HINTS: Dict[str, str] = {
    "birth_date": r"\bbirth[_\s]?date\b",
    "amount_rub": r"\bamount[_\s]?rub\b",
    "customer_name": r"\bcustomer[_\s]?name\b",
    "customer_mood": r"\bcustomer[_\s]?mood\b",
    "invoice_total": r"\binvoice[_\s]?total\b",
    "sales_region": r"\bsales[_\s]?region\b",
}

MODULE_LINE_BUDGETS: Dict[str, int] = {
    "app/services/chat/tabular_sql.py": 1360,
    "app/services/chat/rag_prompt_builder.py": 960,
    "app/services/file.py": 920,
    "app/services/chat/orchestrator_runtime.py": 640,
    "app/rag/retriever.py": 620,
}

FUNCTION_LINE_BUDGETS: Dict[str, Dict[str, int]] = {
    "app/services/chat/tabular_sql.py": {
        "execute_tabular_sql_path": 220,
        "_execute_chart_sync": 150,
    },
    "app/services/chat/rag_prompt_builder.py": {
        "build_rag_prompt": 250,
    },
    "app/services/file.py": {
        "process_file_async": 200,
    },
    "app/services/chat/orchestrator_runtime.py": {
        "stream_chat_events": 280,
        "run_nonstream_chat": 280,
    },
    "app/rag/retriever.py": {
        "retrieve": 190,
        "query_rag": 130,
    },
}


def _to_repo_relative(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _iter_python_files(relative_root: str) -> Iterable[Path]:
    root = REPO_ROOT / relative_root
    if not root.exists():
        return []
    return sorted(
        p
        for p in root.rglob("*.py")
        if "__pycache__" not in p.parts
    )


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _matches_prefix(module_name: str, prefix: str) -> bool:
    return module_name == prefix or module_name.startswith(f"{prefix}.")


def _parse_imports(path: Path) -> Tuple[set[str], List[CheckIssue]]:
    text = _read_text(path)
    rel = _to_repo_relative(path)
    issues: List[CheckIssue] = []
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        issues.append(
            CheckIssue(
                check="parseable_python",
                message=f"{rel}: failed to parse ({exc.msg})",
            )
        )
        return set(), issues

    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                imports.add(node.module)
    return imports, issues


def _collect_runtime_import_violations() -> List[CheckIssue]:
    issues: List[CheckIssue] = []
    for relative_root, forbidden_prefixes in RUNTIME_IMPORT_RULES.items():
        for path in _iter_python_files(relative_root):
            rel = _to_repo_relative(path)
            imports, parse_issues = _parse_imports(path)
            issues.extend(parse_issues)
            for imported in sorted(imports):
                for forbidden_prefix in forbidden_prefixes:
                    if _matches_prefix(imported, forbidden_prefix):
                        issues.append(
                            CheckIssue(
                                check="runtime_import_boundaries",
                                message=(
                                    f"{rel}: forbidden runtime dependency '{imported}' "
                                    f"(matches '{forbidden_prefix}')"
                                ),
                            )
                        )
    return issues


def _collect_domain_import_violations() -> List[CheckIssue]:
    issues: List[CheckIssue] = []
    for path in _iter_python_files("app/domain"):
        rel = _to_repo_relative(path)
        imports, parse_issues = _parse_imports(path)
        issues.extend(parse_issues)
        for imported in sorted(imports):
            for forbidden_prefix in DOMAIN_FORBIDDEN_IMPORT_PREFIXES:
                if _matches_prefix(imported, forbidden_prefix):
                    issues.append(
                        CheckIssue(
                            check="domain_import_boundaries",
                            message=f"{rel}: forbidden domain dependency '{imported}'",
                        )
                    )
    return issues


def _collect_api_rag_allowlist_violations() -> List[CheckIssue]:
    issues: List[CheckIssue] = []
    for path in _iter_python_files("app/api/v1/endpoints"):
        rel = _to_repo_relative(path)
        imports, parse_issues = _parse_imports(path)
        issues.extend(parse_issues)
        allowed_for_file = set(API_RAG_IMPORT_ALLOWLIST.get(rel, ()))
        for imported in sorted(imports):
            if not _matches_prefix(imported, "app.rag"):
                continue
            if imported not in allowed_for_file:
                allowed_preview = ", ".join(sorted(allowed_for_file)) or "<none>"
                issues.append(
                    CheckIssue(
                        check="api_rag_import_allowlist",
                        message=(
                            f"{rel}: direct app.rag import '{imported}' is not allowlisted "
                            f"(allowed: {allowed_preview})"
                        ),
                    )
                )
    return issues


def _collect_debug_sections_owner_violations() -> List[CheckIssue]:
    issues: List[CheckIssue] = []
    for path in _iter_python_files("app"):
        rel = _to_repo_relative(path)
        if rel == DEBUG_SECTIONS_OWNER:
            continue
        if "debug_sections" in _read_text(path):
            issues.append(
                CheckIssue(
                    check="debug_sections_builder_ownership",
                    message=(
                        f"{rel}: 'debug_sections' assembly must stay in {DEBUG_SECTIONS_OWNER}"
                    ),
                )
            )
    return issues


def _collect_low_level_localization_violations() -> List[CheckIssue]:
    issues: List[CheckIssue] = []
    monitored_roots = (
        "app/services/tabular",
        "app/services/llm/routing",
        "app/rag",
    )
    banned_literals = ("localized_text(", "clarification_prompt")
    for monitored_root in monitored_roots:
        for path in _iter_python_files(monitored_root):
            rel = _to_repo_relative(path)
            payload = _read_text(path)
            for banned in banned_literals:
                if banned in payload:
                    issues.append(
                        CheckIssue(
                            check="low_level_localization_guard",
                            message=f"{rel}: contains forbidden low-level marker '{banned}'",
                        )
                    )
    return issues


def _collect_matching_hint_violations() -> List[CheckIssue]:
    issues: List[CheckIssue] = []
    compiled = {
        name: re.compile(pattern, flags=re.IGNORECASE)
        for name, pattern in FORBIDDEN_MATCHING_HINTS.items()
    }
    for relative_path in MATCHING_HINT_FILES:
        path = REPO_ROOT / relative_path
        if not path.exists():
            continue
        text = _read_text(path)
        for token_name, pattern in compiled.items():
            for match in pattern.finditer(text):
                line_no = text.count("\n", 0, match.start()) + 1
                issues.append(
                    CheckIssue(
                        check="forbidden_matching_hints",
                        message=(
                            f"{relative_path}:{line_no}: detected forbidden hardcoded hint "
                            f"token '{token_name}'"
                        ),
                    )
                )
    return issues


def _collect_module_line_budget_violations() -> Tuple[List[CheckIssue], List[CheckIssue]]:
    strict_issues: List[CheckIssue] = []
    warning_issues: List[CheckIssue] = []
    for relative_path, max_lines in MODULE_LINE_BUDGETS.items():
        path = REPO_ROOT / relative_path
        if not path.exists():
            warning_issues.append(
                CheckIssue(
                    check="module_line_budgets",
                    message=f"{relative_path}: budget exists but file was moved/removed",
                    severity="warning",
                )
            )
            continue
        line_count = len(_read_text(path).splitlines())
        if line_count > max_lines:
            strict_issues.append(
                CheckIssue(
                    check="module_line_budgets",
                    message=(
                        f"{relative_path}: {line_count} lines exceeds budget {max_lines}; "
                        "extract logic before adding more"
                    ),
                )
            )
    return strict_issues, warning_issues


def _build_function_length_index(path: Path) -> Tuple[Dict[str, int], List[CheckIssue]]:
    text = _read_text(path)
    rel = _to_repo_relative(path)
    issues: List[CheckIssue] = []
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        issues.append(
            CheckIssue(
                check="function_line_budgets",
                message=f"{rel}: failed to parse for function budgets ({exc.msg})",
            )
        )
        return {}, issues
    lengths: Dict[str, int] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end_line = getattr(node, "end_lineno", node.lineno)
            size = int(end_line) - int(node.lineno) + 1
            previous = lengths.get(node.name)
            if previous is None or size > previous:
                lengths[node.name] = size
    return lengths, issues


def _collect_function_line_budget_violations() -> Tuple[List[CheckIssue], List[CheckIssue]]:
    strict_issues: List[CheckIssue] = []
    warning_issues: List[CheckIssue] = []
    for relative_path, function_budgets in FUNCTION_LINE_BUDGETS.items():
        path = REPO_ROOT / relative_path
        if not path.exists():
            warning_issues.append(
                CheckIssue(
                    check="function_line_budgets",
                    message=f"{relative_path}: function budgets exist but file was moved/removed",
                    severity="warning",
                )
            )
            continue
        lengths, parse_issues = _build_function_length_index(path)
        strict_issues.extend(parse_issues)
        for function_name, max_lines in function_budgets.items():
            measured = lengths.get(function_name)
            if measured is None:
                warning_issues.append(
                    CheckIssue(
                        check="function_line_budgets",
                        message=(
                            f"{relative_path}:{function_name}: function not found; "
                            "budget entry may need refresh after extraction"
                        ),
                        severity="warning",
                    )
                )
                continue
            if measured > max_lines:
                strict_issues.append(
                    CheckIssue(
                        check="function_line_budgets",
                        message=(
                            f"{relative_path}:{function_name}: {measured} lines exceeds budget {max_lines}"
                        ),
                    )
                )
    return strict_issues, warning_issues


def _collect_warning_oversized_modules(threshold: int = 500) -> List[CheckIssue]:
    warning_issues: List[CheckIssue] = []
    oversized: List[Tuple[int, str]] = []
    for path in _iter_python_files("app"):
        rel = _to_repo_relative(path)
        line_count = len(_read_text(path).splitlines())
        if line_count > threshold:
            oversized.append((line_count, rel))
    for line_count, rel in sorted(oversized, reverse=True):
        warning_issues.append(
            CheckIssue(
                check="oversized_module_watchlist",
                message=f"{rel}: {line_count} lines (>{threshold})",
                severity="warning",
            )
        )
    return warning_issues


def _collect_warning_domain_service_coupling() -> List[CheckIssue]:
    warning_issues: List[CheckIssue] = []
    for path in _iter_python_files("app/domain"):
        rel = _to_repo_relative(path)
        imports, parse_issues = _parse_imports(path)
        warning_issues.extend(
            CheckIssue(check=issue.check, message=issue.message, severity="warning")
            for issue in parse_issues
        )
        for imported in sorted(imports):
            if _matches_prefix(imported, "app.services"):
                warning_issues.append(
                    CheckIssue(
                        check="domain_service_coupling_watchlist",
                        message=f"{rel}: currently depends on service layer import '{imported}'",
                        severity="warning",
                    )
                )
    return warning_issues


def _collect_warning_cyrillic_in_rag() -> List[CheckIssue]:
    warning_issues: List[CheckIssue] = []
    for path in _iter_python_files("app/rag"):
        rel = _to_repo_relative(path)
        text = _read_text(path)
        if CYRILLIC_RE.search(text):
            warning_issues.append(
                CheckIssue(
                    check="rag_cyrillic_watchlist",
                    message=(
                        f"{rel}: contains Cyrillic characters; keep localization/user copy "
                        "out of low-level runtime"
                    ),
                    severity="warning",
                )
            )
    return warning_issues


def run_architecture_checks() -> Tuple[List[CheckIssue], List[CheckIssue]]:
    strict_issues: List[CheckIssue] = []
    warning_issues: List[CheckIssue] = []

    strict_issues.extend(_collect_runtime_import_violations())
    strict_issues.extend(_collect_domain_import_violations())
    strict_issues.extend(_collect_api_rag_allowlist_violations())
    strict_issues.extend(_collect_debug_sections_owner_violations())
    strict_issues.extend(_collect_low_level_localization_violations())
    strict_issues.extend(_collect_matching_hint_violations())

    strict_line_issues, warning_line_issues = _collect_module_line_budget_violations()
    strict_issues.extend(strict_line_issues)
    warning_issues.extend(warning_line_issues)

    strict_fn_issues, warning_fn_issues = _collect_function_line_budget_violations()
    strict_issues.extend(strict_fn_issues)
    warning_issues.extend(warning_fn_issues)

    warning_issues.extend(_collect_warning_oversized_modules())
    warning_issues.extend(_collect_warning_domain_service_coupling())
    warning_issues.extend(_collect_warning_cyrillic_in_rag())

    return strict_issues, warning_issues


def format_issues(issues: Sequence[CheckIssue]) -> str:
    if not issues:
        return "none"
    ordered = sorted(issues, key=lambda item: (item.severity, item.check, item.message))
    return "\n".join(
        f"- [{issue.severity}] {issue.check}: {issue.message}"
        for issue in ordered
    )

