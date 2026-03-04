from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncGenerator, Dict, Optional


@dataclass(frozen=True)
class RoutingPolicyContext:
    cannot_wait: bool = False
    sla_critical: bool = False
    policy_class: Optional[str] = None


@dataclass
class RouteTelemetry:
    model_route: str
    fallback_reason: str
    fallback_allowed: bool
    fallback_policy_version: str

    def as_dict(self) -> Dict[str, object]:
        return {
            "model_route": self.model_route,
            "fallback_reason": self.fallback_reason,
            "fallback_allowed": self.fallback_allowed,
            "fallback_policy_version": self.fallback_policy_version,
        }


@dataclass(frozen=True)
class RoutedStream:
    stream: AsyncGenerator[str, None]
    telemetry: RouteTelemetry

