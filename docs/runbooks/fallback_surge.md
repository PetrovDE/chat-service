# Runbook: Fallback Surge

## Triggers
- Rapid increase in `llama_service_llm_fallback_total`.
- Fallback share above operational threshold.

## Diagnostics
1. Group by `fallback_reason` and `fallback_policy_version`.
2. Validate urgency flags (`cannot_wait`, `sla_tier=critical`).
3. Check if surge is due to AI HUB outages or policy misuse.

## Actions
1. Confirm no policy regression in fallback gate logic.
2. If surge is outage-driven, execute `aihub_outage.md`.
3. If surge is traffic-pattern driven, tune client urgency usage.
4. Verify restricted classes are not incorrectly falling back.

## Recovery
- Fallback rate normalizes.
- Route mix returns to expected AI HUB-first baseline.
