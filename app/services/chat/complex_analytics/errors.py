from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

COMPLEX_ANALYTICS_ERROR_SECURITY = "security_violation"
COMPLEX_ANALYTICS_ERROR_TIMEOUT = "timeout"
COMPLEX_ANALYTICS_ERROR_RUNTIME = "runtime_error"
COMPLEX_ANALYTICS_ERROR_DATASET = "dataset_unavailable"
COMPLEX_ANALYTICS_ERROR_DEPENDENCY = "dependency_missing"
COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT = "output_limit_exceeded"
COMPLEX_ANALYTICS_RESPONSE_ERROR = "response_generation_error"
COMPLEX_ANALYTICS_ERROR_CODEGEN = "codegen_failed"
COMPLEX_ANALYTICS_ERROR_VALIDATION = "validation_failed"
COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS = "missing_required_artifacts"


class ComplexAnalyticsSecurityError(Exception):
    pass


class ComplexAnalyticsOutputLimitError(Exception):
    pass


class ComplexAnalyticsValidationError(Exception):
    def __init__(self, error_code: str, message: str) -> None:
        super().__init__(message)
        self.error_code = str(error_code or COMPLEX_ANALYTICS_ERROR_VALIDATION)


@dataclass
class SandboxResult:
    result: Dict[str, Any]
    stdout: str
    artifacts: List[Dict[str, Any]]
