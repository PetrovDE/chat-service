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
    route_mode: str
    provider_selected: Optional[str]
    provider_effective: str
    fallback_reason: Optional[str]
    fallback_allowed: bool
    fallback_attempted: bool
    fallback_policy_version: str
    aihub_attempted: bool

    def as_dict(self) -> Dict[str, object]:
        return {
            "model_route": self.model_route,
            "route_mode": self.route_mode,
            "provider_selected": self.provider_selected,
            "provider_effective": self.provider_effective,
            "fallback_reason": self.fallback_reason,
            "fallback_allowed": self.fallback_allowed,
            "fallback_attempted": self.fallback_attempted,
            "fallback_policy_version": self.fallback_policy_version,
            "aihub_attempted": self.aihub_attempted,
        }


@dataclass(frozen=True)
class RoutedStream:
    stream: AsyncGenerator[str, None]
    telemetry: RouteTelemetry
