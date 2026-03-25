# Runbook: AI HUB Outage

## Triggers
- Spike in `llama_service_llm_fallback_total`.
- `llm_model_route_total{route="ollama_fallback"}` dominates.
- Repeated `fallback_reason` in `{timeout, network, hub_5xx, circuit_open}`.

## Immediate Actions (0-15 min)
1. Confirm AI HUB endpoint and auth availability.
2. Check circuit state via logs/metrics.
3. Confirm fallback policy behavior for critical traffic.
4. Announce degraded mode and expected impact.

## Diagnostics
- Provider metrics: `llm_provider_error_total{provider="aihub"}`.
- Router metrics: `llama_service_llm_route_decisions_total`.
- Check `fallback_allowed=false` errors for restricted traffic.

## Mitigation
1. Keep fallback enabled for urgent classes only.
2. Temporarily reduce load: cap long requests, reduce `max_tokens`.
3. If AI HUB auth issue, rotate/reload credentials.

## Recovery
1. Validate AI HUB health with synthetic request.
2. Observe router return to `aihub_primary`.
3. Ensure circuit transitions `open -> half_open -> closed`.

## Exit Criteria
- Fallback rate returns to baseline.
- No new AI HUB outage-class errors for agreed period.
