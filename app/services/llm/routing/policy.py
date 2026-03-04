from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Set

from app.services.llm.routing.types import RoutingPolicyContext


@dataclass(frozen=True)
class FallbackDecision:
    allowed: bool
    outage: bool
    urgent: bool
    restricted: bool
    outage_reason: str


class FallbackPolicy:
    OUTAGE_REASONS = {"timeout", "network", "hub_5xx", "circuit_open"}

    def __init__(self, *, policy_version: str, restricted_classes: Set[str], enabled: bool = True):
        self.policy_version = str(policy_version or "v1")
        self.enabled = bool(enabled)
        self.restricted_classes = {str(item).strip().lower() for item in restricted_classes if str(item).strip()}

    def evaluate(self, *, context: RoutingPolicyContext, outage_reason: Optional[str]) -> FallbackDecision:
        normalized_reason = str(outage_reason or "none")
        policy_class = str(context.policy_class or "").strip().lower()
        outage = normalized_reason in self.OUTAGE_REASONS
        urgent = bool(context.cannot_wait or context.sla_critical)
        restricted = bool(policy_class and policy_class in self.restricted_classes)
        allowed = bool(self.enabled and outage and urgent and not restricted)
        return FallbackDecision(
            allowed=allowed,
            outage=outage,
            urgent=urgent,
            restricted=restricted,
            outage_reason=normalized_reason,
        )

