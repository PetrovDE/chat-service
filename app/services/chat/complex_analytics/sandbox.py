from __future__ import annotations

import ast
import builtins
import json
import re
from pathlib import Path
from typing import Any, Dict, List

from .artifacts import to_safe_filename
from .errors import (
    ComplexAnalyticsOutputLimitError,
    ComplexAnalyticsSecurityError,
    SandboxResult,
)

_ALLOWED_IMPORT_ROOTS = {
    "pandas",
    "numpy",
    "duckdb",
    "matplotlib",
    "seaborn",
    "datetime",
    "re",
    "warnings",
}
_BLOCKED_IMPORT_ROOTS = {
    "os",
    "subprocess",
    "socket",
    "requests",
    "httpx",
    "urllib",
    "pathlib",
    "shutil",
    "ftplib",
    "paramiko",
    "sys",
    "importlib",
}
_BLOCKED_CALL_NAMES = {
    "open",
    "eval",
    "exec",
    "compile",
    "input",
    "__import__",
    "breakpoint",
    "getattr",
    "setattr",
    "delattr",
    "globals",
    "locals",
    "vars",
}
_BLOCKED_ATTRIBUTE_ROOTS = {
    "os",
    "subprocess",
    "socket",
    "requests",
    "httpx",
    "urllib",
    "pathlib",
    "shutil",
    "sys",
    "importlib",
}
_BLOCKED_ATTRIBUTE_CHAINS = {
    "os.system",
    "os.popen",
    "subprocess.run",
    "subprocess.Popen",
    "subprocess.call",
    "socket.socket",
    "requests.get",
    "requests.post",
    "httpx.get",
    "httpx.post",
}

_NETWORK_LITERAL_PATTERN = re.compile(r"https?://|ftp://", flags=re.IGNORECASE)


def attribute_chain(node: ast.Attribute) -> str:
    chunks = [str(node.attr or "")]
    value = node.value
    while isinstance(value, ast.Attribute):
        chunks.append(str(value.attr or ""))
        value = value.value
    if isinstance(value, ast.Name):
        chunks.append(str(value.id or ""))
    chunks.reverse()
    return ".".join([item for item in chunks if item])


def validate_python_security(code: str) -> None:
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:  # pragma: no cover - syntax errors are rare in generated templates
        raise ComplexAnalyticsSecurityError(f"Invalid python syntax: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = str(alias.name or "").split(".", 1)[0]
                if root in _BLOCKED_IMPORT_ROOTS:
                    raise ComplexAnalyticsSecurityError(f"Import blocked: {alias.name}")
                if root not in _ALLOWED_IMPORT_ROOTS:
                    raise ComplexAnalyticsSecurityError(f"Import not allowed: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module_root = str(node.module or "").split(".", 1)[0]
            if module_root in _BLOCKED_IMPORT_ROOTS:
                raise ComplexAnalyticsSecurityError(f"Import blocked: {node.module}")
            if module_root and module_root not in _ALLOWED_IMPORT_ROOTS:
                raise ComplexAnalyticsSecurityError(f"Import not allowed: {node.module}")
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if str(node.func.id or "") in _BLOCKED_CALL_NAMES:
                    raise ComplexAnalyticsSecurityError(f"Call blocked: {node.func.id}")
            elif isinstance(node.func, ast.Attribute):
                chain = attribute_chain(node.func)
                if any(part.startswith("__") for part in chain.split(".")):
                    raise ComplexAnalyticsSecurityError(f"Dunder attribute access blocked: {chain}")
                if chain in _BLOCKED_ATTRIBUTE_CHAINS:
                    raise ComplexAnalyticsSecurityError(f"Call blocked: {chain}")
                root = chain.split(".", 1)[0]
                if root in _BLOCKED_ATTRIBUTE_ROOTS:
                    raise ComplexAnalyticsSecurityError(f"Call blocked: {chain}")
        elif isinstance(node, ast.Attribute):
            if str(node.attr or "").startswith("__"):
                raise ComplexAnalyticsSecurityError(f"Dunder attribute access blocked: {node.attr}")
        elif isinstance(node, ast.Name):
            if str(node.id or "").startswith("__"):
                raise ComplexAnalyticsSecurityError(f"Name blocked: {node.id}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if _NETWORK_LITERAL_PATTERN.search(node.value):
                raise ComplexAnalyticsSecurityError("Network URL literals are not allowed in sandbox code")


def execute_sandboxed_python(
    *,
    code: str,
    datasets: Dict[str, Any],
    artifacts_dir: Path,
    max_output_chars: int,
    max_artifacts: int,
) -> SandboxResult:
    validate_python_security(code)

    artifacts_dir.mkdir(parents=True, exist_ok=True)
    artifacts: List[Dict[str, Any]] = []
    output_parts: List[str] = []
    output_size = 0

    def _safe_print(*args: Any, **kwargs: Any) -> None:
        nonlocal output_size
        _ = kwargs
        text = " ".join(str(arg) for arg in args).strip()
        if not text:
            return
        remaining = int(max_output_chars) - output_size
        if remaining <= 0:
            raise ComplexAnalyticsOutputLimitError("stdout limit exceeded")
        chunk = text[:remaining]
        output_parts.append(chunk)
        output_size += len(chunk)
        if len(text) > len(chunk):
            raise ComplexAnalyticsOutputLimitError("stdout limit exceeded")

    import warnings as _sandbox_warnings

    def _save_plot(*, fig=None, name: str = "chart.png") -> str:  # noqa: ANN001
        if len(artifacts) >= int(max_artifacts):
            raise ComplexAnalyticsOutputLimitError("artifact limit exceeded")
        target_name = to_safe_filename(name)
        target_path = (artifacts_dir / target_name).resolve()
        if not str(target_path).startswith(str(artifacts_dir.resolve())):
            raise ComplexAnalyticsSecurityError("Artifact path escape detected")
        if fig is None:
            try:
                import matplotlib.pyplot as plt  # noqa: PLC0415

                fig = plt.gcf()
            except Exception as exc:  # pragma: no cover - runtime environment dependent
                raise ComplexAnalyticsSecurityError("matplotlib is required to save plots") from exc
        fig.savefig(target_path, dpi=150, bbox_inches="tight")
        artifacts.append(
            {
                "name": target_name,
                "path": str(target_path),
                "content_type": "image/png",
            }
        )
        return str(target_path)

    safe_builtins = {
        "__import__": builtins.__import__,
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "Exception": Exception,
        "TypeError": TypeError,
        "ValueError": ValueError,
        "RuntimeError": RuntimeError,
        "UserWarning": UserWarning,
        "dict": dict,
        "enumerate": enumerate,
        "float": float,
        "int": int,
        "len": len,
        "list": list,
        "max": max,
        "min": min,
        "print": _safe_print,
        "range": range,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "zip": zip,
    }

    exec_namespace: Dict[str, Any] = {
        "__builtins__": safe_builtins,
        "datasets": datasets,
        "save_plot": _save_plot,
    }
    try:
        import matplotlib  # noqa: PLC0415

        matplotlib.use("Agg", force=True)
    except Exception:
        pass
    with _sandbox_warnings.catch_warnings():
        _sandbox_warnings.filterwarnings(
            "ignore",
            category=UserWarning,
            message=r"Could not infer format, so each element will be parsed individually.*",
        )
        compiled = compile(code, "<complex_analytics_sandbox>", "exec")
        exec(compiled, exec_namespace, exec_namespace)

    raw_result = exec_namespace.get("result")
    if raw_result is None:
        raw_result = {"status": "ok", "notes": "No explicit result payload returned by script"}
    if not isinstance(raw_result, dict):
        raw_result = {"status": "ok", "value": str(raw_result)}

    serialized = json.dumps(raw_result, ensure_ascii=False, default=str)
    if len(serialized) > int(max_output_chars):
        raise ComplexAnalyticsOutputLimitError("result payload too large")
    return SandboxResult(result=raw_result, stdout="\n".join(output_parts), artifacts=artifacts)


# Compatibility aliases.
SandboxExecutionResult = SandboxResult
_attribute_chain = attribute_chain
_validate_python_security = validate_python_security
